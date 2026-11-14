//! JSON path functions of our own, under the names and operators of
//! datafusion-functions-json and with its results, so SQL written for it
//! works here: a getter of each type, `json_get`'s value of any type, and
//! `->`, `->>`, `?`, `#>` and `#>>`.
//!
//! The JSON is a string: a JSON column, which the engine keeps as a string,
//! or any other. A path is keys and array indexes, as arguments or as the
//! elements of a list. A negative index counts from the end, and a key that
//! reaches an array is an index if it spells one. A path that isn't there,
//! a null step and a null document give null, and `json_contains` false.
//! Each call reads a document once, as a stream, and builds no tree; a
//! document that isn't JSON throughout has nothing in it. Where an object
//! has a key twice the last one counts, as in Postgres; the library takes
//! the first.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, LazyLock};

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array,
    UnionArray,
};
use arrow_schema::{DataType, Field, FieldRef, UnionFields, UnionMode};
use datafusion_common::arrow::buffer::ScalarBuffer;
use datafusion_common::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Result, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TryCast, TypeSignature, Volatility,
};
use datafusion_sql::sqlparser::ast::BinaryOperator;
use serde::Deserialize;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

/// What each function gives. The order is `NAMES`'s.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    /// The value, of any type, as the JSON union.
    Get,
    /// A boolean, or a string that spells one.
    GetBool,
    /// An integer, a float with no fraction that i64 holds, or a string that
    /// spells an integer.
    GetInt,
    /// A number, or a string that spells one.
    GetFloat,
    /// A string's value.
    GetStr,
    /// The value's JSON text, as it's written; a JSON null is `null`.
    GetJson,
    /// An array's elements, each as its JSON text.
    GetArray,
    /// A string's value, any other value's JSON text, and null for a JSON
    /// null.
    AsText,
    /// Whether the path reaches a value, null included. Never null.
    Contains,
    /// The number of an array's elements or an object's keys.
    Length,
    ObjectKeys,
    /// The JSON union's value as JSON text, and null for a JSON null. Given
    /// JSON and a path instead of the union, the value there as it's written,
    /// which is what `json_get`'s becomes in an output column.
    UnionToText,
    /// A boolean, number or string as the JSON union.
    FromScalar,
}

const NAMES: [(Kind, &str, &[&str]); 13] = [
    (Kind::Get, "json_get", &[]),
    (Kind::GetBool, "json_get_bool", &[]),
    (Kind::GetInt, "json_get_int", &[]),
    (Kind::GetFloat, "json_get_float", &[]),
    (Kind::GetStr, "json_get_str", &[]),
    (Kind::GetJson, "json_get_json", &[]),
    (Kind::GetArray, "json_get_array", &[]),
    (Kind::AsText, "json_as_text", &[]),
    (Kind::Contains, "json_contains", &[]),
    (Kind::Length, "json_length", &["json_len"]),
    (Kind::ObjectKeys, "json_object_keys", &["json_keys"]),
    (Kind::UnionToText, "json_union_to_text", &[]),
    (Kind::FromScalar, "json_from_scalar", &["scalar_to_json"]),
];

static UDFS: LazyLock<Vec<Arc<ScalarUDF>>> = LazyLock::new(|| {
    NAMES
        .iter()
        .map(|&(kind, name, aliases)| {
            Arc::new(ScalarUDF::new_from_impl(Json {
                kind,
                name,
                aliases: aliases.iter().map(|a| (*a).to_owned()).collect(),
                // Nullary too, so that a call with no arguments reaches
                // `check`, whose refusal says what the function takes.
                signature: Signature::one_of(
                    vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                    Volatility::Immutable,
                ),
            }))
        })
        .collect()
});

fn udf(kind: Kind) -> Arc<ScalarUDF> {
    Arc::clone(&UDFS[kind as usize])
}

fn call(kind: Kind, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf(kind), args))
}

/// The functions, for the registry.
pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
    UDFS.clone()
}

/// A call of one of these functions, through aliases: what it is and its
/// arguments.
fn ours(e: &Expr) -> Option<(Kind, &[Expr])> {
    match e {
        Expr::Alias(a) => ours(&a.expr),
        Expr::ScalarFunction(f) => f
            .func
            .inner()
            .downcast_ref::<Json>()
            .map(|j| (j.kind, f.args.as_slice())),
        _ => None,
    }
}

/// `->` and `#>` as `json_get`, `->>` and `#>>` as `json_as_text`, and `?`
/// as `json_contains`. `#>` and `#>>` take a list as the path.
#[derive(Debug)]
pub(crate) struct Operators;

impl ExprPlanner for Operators {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let kind = match expr.op {
            BinaryOperator::Arrow | BinaryOperator::HashArrow => Kind::Get,
            BinaryOperator::LongArrow | BinaryOperator::HashLongArrow => Kind::AsText,
            BinaryOperator::Question => Kind::Contains,
            _ => return Ok(PlannerResult::Original(expr)),
        };
        Ok(PlannerResult::Planned(call(
            kind,
            vec![expr.left, expr.right],
        )))
    }
}

/// Rewrites before type coercion. A call on `json_get`'s value is one call
/// with the whole path, so `doc->'a'->>'b'` is `json_as_text(doc, 'a',
/// 'b')`. A cast of `json_get`'s value reads it with the getter of that
/// type.
#[derive(Debug)]
pub(crate) struct Rewrites;

impl FunctionRewrite for Rewrites {
    fn name(&self) -> &str {
        "json"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        Ok(match expr {
            Expr::Cast(Cast { expr, field }) => match read_as(&expr, field.data_type()) {
                Some((getter, true)) => Transformed::yes(getter),
                Some((getter, false)) => Transformed::yes(Expr::Cast(Cast {
                    expr: Box::new(getter),
                    field,
                })),
                None => Transformed::no(Expr::Cast(Cast { expr, field })),
            },
            Expr::TryCast(TryCast { expr, field }) => match read_as(&expr, field.data_type()) {
                Some((getter, true)) => Transformed::yes(getter),
                Some((getter, false)) => Transformed::yes(Expr::TryCast(TryCast {
                    expr: Box::new(getter),
                    field,
                })),
                None => Transformed::no(Expr::TryCast(TryCast { expr, field })),
            },
            e => match flattened(&e) {
                Some(f) => Transformed::yes(f),
                None => Transformed::no(e),
            },
        })
    }
}

/// A call whose JSON is `json_get`'s value, as one call with both paths.
/// The library does this only where every step is a literal; here a path
/// can differ by row anywhere in it.
fn flattened(e: &Expr) -> Option<Expr> {
    let (kind, args) = ours(e)?;
    if kind == Kind::FromScalar {
        return None;
    }
    let (first, rest) = args.split_first()?;
    let (Kind::Get, inner) = ours(first)? else {
        return None;
    };
    let mut all = inner.to_vec();
    all.extend_from_slice(rest);
    Some(call(kind, all))
}

/// The getter that reads `json_get`'s value as `to`, so that a value of
/// another type is null rather than an error, and a string type takes
/// `json_as_text`'s text; and whether the getter gives `to` itself, so the
/// cast can go. DECIMAL reads a float first, as the library does: for exact
/// digits, cast `->>`'s text instead.
fn read_as(e: &Expr, to: &DataType) -> Option<(Expr, bool)> {
    let (Kind::Get, args) = ours(e)? else {
        return None;
    };
    let (kind, gives) = match to {
        DataType::Boolean => (Kind::GetBool, DataType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (Kind::GetInt, DataType::Int64)
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => (Kind::GetFloat, DataType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (Kind::AsText, DataType::Utf8),
        _ => return None,
    };
    Some((call(kind, args.to_vec()), gives == *to))
}

/// The JSON union as JSON text, for an output column, which can't hold a
/// union. `json_get`'s value simplifies to the path form, which gives the
/// value as the document writes it, so a number keeps its digits.
pub(crate) fn text(e: Expr) -> Expr {
    call(Kind::UnionToText, vec![e])
}

const NULL: i8 = 0;
const BOOL: i8 = 1;
const INT: i8 = 2;
const FLOAT: i8 = 3;
const STR: i8 = 4;
const ARRAY: i8 = 5;
const OBJECT: i8 = 6;

/// The JSON union: a JSON value of any type, by type id, as the library has
/// it. An array or an object is its JSON text. A sparse union, whose
/// members are null where the row is another's.
static UNION: LazyLock<UnionFields> = LazyLock::new(|| {
    UnionFields::from_iter([
        (NULL, Arc::new(Field::new("null", DataType::Null, true))),
        (BOOL, Arc::new(Field::new("bool", DataType::Boolean, true))),
        (INT, Arc::new(Field::new("int", DataType::Int64, true))),
        (
            FLOAT,
            Arc::new(Field::new("float", DataType::Float64, true)),
        ),
        (STR, Arc::new(Field::new("str", DataType::Utf8, true))),
        (ARRAY, Arc::new(Field::new("array", DataType::Utf8, true))),
        (OBJECT, Arc::new(Field::new("object", DataType::Utf8, true))),
    ])
});

fn union_type() -> DataType {
    DataType::Union(UNION.clone(), UnionMode::Sparse)
}

pub(crate) fn is_union(t: &DataType) -> bool {
    matches!(t, DataType::Union(f, UnionMode::Sparse) if *f == *UNION)
}

/// A string field the engine reads as JSON.
fn json_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([
        ("ARROW:extension:name".to_owned(), "arrow.json".to_owned()),
        ("ARROW:extension:metadata".to_owned(), String::new()),
    ]))
}

fn is_str(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn is_int(t: &DataType) -> bool {
    t.is_integer()
}

fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Json {
    kind: Kind,
    name: &'static str,
    aliases: Vec<String>,
    signature: Signature,
}

impl Json {
    /// Refuses arguments of types the function doesn't take. It takes more
    /// than the library: integers of any width as indexes, and a null where
    /// the library wants a string.
    fn check(&self, args: &[DataType]) -> Result<()> {
        let name = self.name;
        if self.kind == Kind::FromScalar {
            return match args {
                [t] if *t == DataType::Null
                    || *t == DataType::Boolean
                    || is_int(t)
                    || is_float(t)
                    || is_str(t) =>
                {
                    Ok(())
                }
                [t] => plan_err!("{name} takes a boolean, a number or a string, not {t}"),
                _ => plan_err!("{name} takes one argument, not {}", args.len()),
            };
        }
        if self.kind == Kind::UnionToText && matches!(args, [t] if is_union(t)) {
            return Ok(());
        }
        let (who, take) = match self.kind {
            Kind::Get => ("json_get, -> and #>", "take"),
            Kind::AsText => ("json_as_text, ->> and #>>", "take"),
            Kind::Contains => ("json_contains and ?", "take"),
            _ => (name, "takes"),
        };
        let Some((json, path)) = args.split_first() else {
            return plan_err!("{who} {take} JSON, then the keys and indexes of a path");
        };
        if !(is_str(json) || is_union(json) || *json == DataType::Null) {
            return plan_err!("{who} {take} JSON as a string, not {json}");
        }
        let step = |t: &DataType| is_str(t) || is_int(t) || *t == DataType::Null;
        match path
            .iter()
            .find(|t| !(step(t) || matches!(t, DataType::List(f) if step(f.data_type()))))
        {
            Some(t) => plan_err!("{who} {take} a path of keys and indexes, not {t}"),
            None => Ok(()),
        }
    }

    fn field(&self) -> Field {
        let t = match self.kind {
            Kind::Get | Kind::FromScalar => union_type(),
            Kind::GetBool | Kind::Contains => DataType::Boolean,
            Kind::GetInt => DataType::Int64,
            Kind::GetFloat => DataType::Float64,
            Kind::GetStr | Kind::AsText => DataType::Utf8,
            Kind::GetJson | Kind::UnionToText => return json_field(self.name),
            Kind::GetArray => DataType::List(Arc::new(json_field("item"))),
            Kind::Length => DataType::UInt64,
            Kind::ObjectKeys => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        };
        Field::new(self.name, t, self.kind != Kind::Contains)
    }
}

impl ScalarUDFImpl for Json {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.check(args)?;
        Ok(self.field().data_type().clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        self.check(&types)?;
        Ok(Arc::new(self.field()))
    }

    /// `json_union_to_text(json_get(…))` as the path form, which an output
    /// column's text becomes once projections merge, after the rewrites.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if self.kind == Kind::UnionToText
            && let [a] = args.as_slice()
            && let Some((Kind::Get, inner)) = ours(a)
        {
            return Ok(ExprSimplifyResult::Simplified(call(
                Kind::UnionToText,
                inner.to_vec(),
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let mut values = args.args.into_iter();
        let Some(first) = values.next() else {
            return exec_err!("{} takes an argument", self.name);
        };
        let first = first.into_array(rows)?;
        let path = values
            .map(|v| path_arg(&v))
            .collect::<Result<Vec<PathArg>>>()?;
        let out: ArrayRef = match self.kind {
            Kind::FromScalar => from_scalars(&first)?,
            Kind::UnionToText if path.is_empty() && is_union(first.data_type()) => {
                Arc::new(texts(&first)?)
            }
            kind => eval(kind, &documents(&first)?, &path)?,
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// A step of a path.
#[derive(Clone, Copy, Debug)]
enum Step<'a> {
    Key(&'a str),
    Index(i64),
}

/// A step, held for a batch.
#[derive(Clone, Debug)]
enum Owned {
    Key(String),
    Index(i64),
}

impl Owned {
    fn step(&self) -> Step<'_> {
        match self {
            Owned::Key(k) => Step::Key(k),
            Owned::Index(i) => Step::Index(*i),
        }
    }
}

/// A path argument's steps: the same for every row, or one row's each.
/// `None` where it, or an element of its list, is null.
enum PathArg {
    Fixed(Option<Vec<Owned>>),
    Each(Vec<Option<Vec<Owned>>>),
}

fn path_arg(v: &ColumnarValue) -> Result<PathArg> {
    Ok(match v {
        ColumnarValue::Scalar(s) => {
            PathArg::Fixed(steps(&s.to_array()?)?.into_iter().next().flatten())
        }
        ColumnarValue::Array(a) => PathArg::Each(steps(a)?),
    })
}

/// Each row's steps: a key, an index, or a list's elements.
fn steps(a: &ArrayRef) -> Result<Vec<Option<Vec<Owned>>>> {
    let Some(list) = a.as_list_opt::<i32>() else {
        return Ok(single(a)?.into_iter().map(|s| s.map(|s| vec![s])).collect());
    };
    let elements = single(list.values())?;
    let mut out = Vec::with_capacity(list.len());
    for (row, w) in list.offsets().windows(2).enumerate() {
        let (start, end) = (
            usize::try_from(w[0]).unwrap_or(0),
            usize::try_from(w[1]).unwrap_or(0),
        );
        out.push(match elements.get(start..end) {
            Some(e) if list.is_valid(row) => e.iter().cloned().collect(),
            _ => None,
        });
    }
    Ok(out)
}

/// A step per row, from keys or indexes.
fn single(a: &ArrayRef) -> Result<Vec<Option<Owned>>> {
    let t = a.data_type();
    Ok(if *t == DataType::Null {
        vec![None; a.len()]
    } else if is_str(t) {
        cast(a, &DataType::Utf8)?
            .as_string::<i32>()
            .iter()
            .map(|k| k.map(|k| Owned::Key(k.to_owned())))
            .collect()
    } else if is_int(t) {
        // An unsigned index past i64 is null, which reaches nothing, as an
        // index that far would.
        cast(a, &DataType::Int64)?
            .as_primitive::<Int64Type>()
            .iter()
            .map(|i| i.map(Owned::Index))
            .collect()
    } else {
        return exec_err!("a JSON path takes keys and indexes, not {t}");
    })
}

/// Puts a row's path in `into`; false where a step of it is null.
fn path_at<'a>(args: &'a [PathArg], row: usize, into: &mut Vec<Step<'a>>) -> bool {
    into.clear();
    for a in args {
        let steps = match a {
            PathArg::Fixed(s) => s.as_ref(),
            PathArg::Each(rows) => rows.get(row).and_then(Option::as_ref),
        };
        let Some(steps) = steps else {
            return false;
        };
        into.extend(steps.iter().map(Owned::step));
    }
    true
}

/// The documents, one per row: JSON text, or the JSON union's values as
/// their JSON text.
fn documents(json: &ArrayRef) -> Result<StringArray> {
    let t = json.data_type();
    if is_union(t) {
        return texts(json);
    }
    if *t == DataType::Null {
        return Ok(StringArray::new_null(json.len()));
    }
    Ok(cast(json, &DataType::Utf8)?.as_string::<i32>().clone())
}

fn eval(kind: Kind, docs: &StringArray, args: &[PathArg]) -> Result<ArrayRef> {
    let mut path = Vec::with_capacity(args.len());
    let found: Vec<Option<&str>> = docs
        .iter()
        .enumerate()
        .map(|(row, doc)| match doc {
            Some(d) if path_at(args, row, &mut path) => find(d, &path),
            _ => None,
        })
        .collect();
    let values = found.iter().copied();
    Ok(match kind {
        Kind::Get => union_of(values.map(|r| r.map_or(Member::Null, member)))?,
        Kind::GetBool => Arc::new(
            values
                .map(|r| r.and_then(boolean))
                .collect::<BooleanArray>(),
        ),
        Kind::GetInt => Arc::new(values.map(|r| r.and_then(int)).collect::<Int64Array>()),
        Kind::GetFloat => Arc::new(values.map(|r| r.and_then(float)).collect::<Float64Array>()),
        Kind::GetStr => Arc::new(values.map(|r| r.and_then(string)).collect::<StringArray>()),
        Kind::GetJson => Arc::new(values.collect::<StringArray>()),
        Kind::AsText => Arc::new(values.map(|r| r.and_then(as_text)).collect::<StringArray>()),
        Kind::UnionToText => Arc::new(
            values
                .map(|r| r.filter(|r| !matches!(classify(r), Value::Null)))
                .collect::<StringArray>(),
        ),
        Kind::Contains => Arc::new(BooleanArray::from(
            values.map(|r| r.is_some()).collect::<Vec<bool>>(),
        )),
        Kind::Length => Arc::new(values.map(|r| r.and_then(length)).collect::<UInt64Array>()),
        Kind::GetArray => lists(values.map(|r| r.and_then(elements)), json_field("item")),
        Kind::ObjectKeys => lists(
            values.map(|r| r.and_then(keys)),
            Field::new("item", DataType::Utf8, true),
        ),
        Kind::FromScalar => return exec_err!("json_from_scalar takes no path"),
    })
}

fn lists<'a>(values: impl Iterator<Item = Option<Vec<Cow<'a, str>>>>, item: Field) -> ArrayRef {
    let mut b = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(item));
    for v in values {
        b.append_option(v.map(|items| items.into_iter().map(Some)));
    }
    Arc::new(b.finish())
}

/// The JSON text of the value at `path` in `json`, as it's written, if
/// `json` is a JSON document and has a value there.
fn find<'j>(json: &'j str, path: &[Step<'_>]) -> Option<&'j str> {
    let mut d = serde_json::Deserializer::from_str(json);
    let found = Walk { path }.deserialize(&mut d).ok()?;
    d.end().ok()?;
    found
}

/// Walks a document to the value at `path`, and reads past the rest.
struct Walk<'p, 'k> {
    path: &'p [Step<'k>],
}

impl<'de> DeserializeSeed<'de> for Walk<'_, '_> {
    type Value = Option<&'de str>;

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        match self.path.split_first() {
            None => <&'de RawValue>::deserialize(d).map(|r| Some(r.get())),
            Some((step, rest)) => d.deserialize_any(Enter { step: *step, rest }),
        }
    }
}

/// One step of a walk, into the object or array it's at. Any other value
/// has nothing in it.
struct Enter<'p, 'k> {
    step: Step<'k>,
    rest: &'p [Step<'k>],
}

impl<'de> Visitor<'de> for Enter<'_, '_> {
    type Value = Option<&'de str>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    // An object; or a number, which serde_json's arbitrary precision gives
    // as a map whose one key is its own. A path that names that key goes
    // into a string or fails, and finds nothing either way.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let want = match self.step {
            Step::Key(k) => Some(k),
            Step::Index(_) => None,
        };
        let mut found = None;
        while let Some(key) = map.next_key::<Text<'de>>()? {
            if want.is_some_and(|w| key.0 == w) {
                found = map.next_value_seed(Walk { path: self.rest })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(found)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let index = match self.step {
            Step::Index(i) => Some(i),
            Step::Key(k) => k.parse::<i64>().ok(),
        };
        let mut found = None;
        match index.map(usize::try_from) {
            Some(Ok(i)) => {
                let mut at = 0usize;
                loop {
                    if at == i {
                        match seq.next_element_seed(Walk { path: self.rest })? {
                            Some(f) => found = f,
                            None => break,
                        }
                    } else if seq.next_element::<IgnoredAny>()?.is_none() {
                        break;
                    }
                    at = at.saturating_add(1);
                }
            }
            Some(Err(_)) => {
                // From the end: keep the last |i| elements, then walk the
                // rest of the path in the first of them.
                let keep = index
                    .and_then(|i| usize::try_from(i.unsigned_abs()).ok())
                    .unwrap_or(usize::MAX);
                let mut last: VecDeque<&'de RawValue> = VecDeque::new();
                while let Some(e) = seq.next_element::<&'de RawValue>()? {
                    if last.len() == keep {
                        last.pop_front();
                    }
                    last.push_back(e);
                }
                if last.len() == keep {
                    found = last.front().and_then(|e| find(e.get(), self.rest));
                }
            }
            None => while seq.next_element::<IgnoredAny>()?.is_some() {},
        }
        Ok(found)
    }
}

/// A string as it's written, borrowed where it has no escapes.
struct Text<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Text<'de> {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Text<'de>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string")
            }

            fn visit_borrowed_str<E: de::Error>(self, s: &'de str) -> Result<Self::Value, E> {
                Ok(Text(Cow::Borrowed(s)))
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Text(Cow::Owned(s.to_owned())))
            }
        }
        d.deserialize_str(V)
    }
}

/// A value's JSON text, by its type.
enum Value<'a> {
    Null,
    Bool(bool),
    Number(&'a str),
    String(&'a str),
    Array(&'a str),
    Object(&'a str),
}

/// What a value is, from its first character; `raw` is a found value, which
/// is JSON.
fn classify(raw: &str) -> Value<'_> {
    match raw.as_bytes().first() {
        Some(b'n') => Value::Null,
        Some(b't') => Value::Bool(true),
        Some(b'f') => Value::Bool(false),
        Some(b'"') => Value::String(raw),
        Some(b'[') => Value::Array(raw),
        Some(b'{') => Value::Object(raw),
        _ => Value::Number(raw),
    }
}

/// A JSON string's value.
fn unquote(raw: &str) -> Option<Cow<'_, str>> {
    let inner = raw.strip_prefix('"')?.strip_suffix('"')?;
    if inner.contains('\\') {
        serde_json::from_str::<String>(raw).ok().map(Cow::Owned)
    } else {
        Some(Cow::Borrowed(inner))
    }
}

fn string(raw: &str) -> Option<Cow<'_, str>> {
    match classify(raw) {
        Value::String(s) => unquote(s),
        _ => None,
    }
}

fn boolean(raw: &str) -> Option<bool> {
    match classify(raw) {
        Value::Bool(b) => Some(b),
        Value::String(s) => unquote(s)?.parse().ok(),
        _ => None,
    }
}

fn int(raw: &str) -> Option<i64> {
    match classify(raw) {
        Value::Number(n) if n.contains(['.', 'e', 'E']) => integral(n.parse().ok()?),
        Value::Number(n) => n.parse().ok(),
        Value::String(s) => unquote(s)?.parse().ok(),
        _ => None,
    }
}

/// A float with no fraction as the integer, where i64 holds it.
fn integral(f: f64) -> Option<i64> {
    // -2^63 and 2^63 are exact as floats: the first is i64::MIN, and the
    // second the first value past i64::MAX.
    let bound = -(i64::MIN as f64);
    (f.fract() == 0.0 && f >= -bound && f < bound).then_some(f as i64)
}

fn float(raw: &str) -> Option<f64> {
    match classify(raw) {
        Value::Number(n) => n.parse().ok(),
        Value::String(s) => unquote(s)?.parse().ok(),
        _ => None,
    }
}

fn as_text(raw: &str) -> Option<Cow<'_, str>> {
    match classify(raw) {
        Value::Null => None,
        Value::String(s) => unquote(s),
        _ => Some(Cow::Borrowed(raw)),
    }
}

fn elements(raw: &str) -> Option<Vec<Cow<'_, str>>> {
    match classify(raw) {
        Value::Array(a) => serde_json::from_str::<Vec<&RawValue>>(a)
            .ok()
            .map(|v| v.into_iter().map(|e| Cow::Borrowed(e.get())).collect()),
        _ => None,
    }
}

fn keys(raw: &str) -> Option<Vec<Cow<'_, str>>> {
    struct Keys;
    impl<'de> Visitor<'de> for Keys {
        type Value = Vec<Cow<'de, str>>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an object")
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut keys = Vec::new();
            while let Some(k) = map.next_key::<Text<'de>>()? {
                map.next_value::<IgnoredAny>()?;
                keys.push(k.0);
            }
            Ok(keys)
        }
    }
    match classify(raw) {
        Value::Object(o) => {
            let mut d = serde_json::Deserializer::from_str(o);
            de::Deserializer::deserialize_map(&mut d, Keys).ok()
        }
        _ => None,
    }
}

fn length(raw: &str) -> Option<u64> {
    struct Count;
    impl<'de> Visitor<'de> for Count {
        type Value = u64;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an array or an object")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<u64, A::Error> {
            let mut n = 0u64;
            while seq.next_element::<IgnoredAny>()?.is_some() {
                n += 1;
            }
            Ok(n)
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
            let mut n = 0u64;
            while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
                n += 1;
            }
            Ok(n)
        }
    }
    let mut d = serde_json::Deserializer::from_str(raw);
    match classify(raw) {
        Value::Array(_) => de::Deserializer::deserialize_seq(&mut d, Count).ok(),
        Value::Object(_) => de::Deserializer::deserialize_map(&mut d, Count).ok(),
        _ => None,
    }
}

/// A value of the JSON union.
enum Member<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Cow<'a, str>),
    Array(&'a str),
    Object(&'a str),
}

/// A found value as the union's member. An integer past what i64 holds has
/// none, and is null, as in the library.
fn member(raw: &str) -> Member<'_> {
    match classify(raw) {
        Value::Null => Member::Null,
        Value::Bool(b) => Member::Bool(b),
        Value::Number(n) if n.contains(['.', 'e', 'E']) => {
            n.parse().map_or(Member::Null, Member::Float)
        }
        Value::Number(n) => n.parse().map_or(Member::Null, Member::Int),
        Value::String(s) => unquote(s).map_or(Member::Null, Member::Str),
        Value::Array(a) => Member::Array(a),
        Value::Object(o) => Member::Object(o),
    }
}

fn union_of<'a>(values: impl Iterator<Item = Member<'a>>) -> Result<ArrayRef> {
    let mut ids = Vec::new();
    let mut bools = BooleanBuilder::new();
    let mut ints = Int64Builder::new();
    let mut floats = Float64Builder::new();
    let mut strs = StringBuilder::new();
    let mut arrays = StringBuilder::new();
    let mut objects = StringBuilder::new();
    for v in values {
        ids.push(match &v {
            Member::Null => NULL,
            Member::Bool(_) => BOOL,
            Member::Int(_) => INT,
            Member::Float(_) => FLOAT,
            Member::Str(_) => STR,
            Member::Array(_) => ARRAY,
            Member::Object(_) => OBJECT,
        });
        match v {
            Member::Null => {}
            Member::Bool(b) => bools.append_value(b),
            Member::Int(i) => ints.append_value(i),
            Member::Float(f) => floats.append_value(f),
            Member::Str(s) => strs.append_value(s),
            Member::Array(a) => arrays.append_value(a),
            Member::Object(o) => objects.append_value(o),
        }
        let n = ids.len();
        if bools.len() < n {
            bools.append_null();
        }
        if ints.len() < n {
            ints.append_null();
        }
        if floats.len() < n {
            floats.append_null();
        }
        for b in [&mut strs, &mut arrays, &mut objects] {
            if b.len() < n {
                b.append_null();
            }
        }
    }
    let n = ids.len();
    Ok(Arc::new(UnionArray::try_new(
        UNION.clone(),
        ScalarBuffer::from(ids),
        None,
        vec![
            Arc::new(NullArray::new(n)),
            Arc::new(bools.finish()),
            Arc::new(ints.finish()),
            Arc::new(floats.finish()),
            Arc::new(strs.finish()),
            Arc::new(arrays.finish()),
            Arc::new(objects.finish()),
        ],
    )?))
}

/// The JSON union's values as JSON text, written again from the members; a
/// JSON null is null.
fn texts(union: &ArrayRef) -> Result<StringArray> {
    let Some(u) = union
        .as_any()
        .downcast_ref::<UnionArray>()
        .filter(|u| is_union(u.data_type()))
    else {
        return exec_err!("expected json_get's value, not {}", union.data_type());
    };
    let bools: Vec<Option<bool>> = u.child(BOOL).as_boolean().iter().collect();
    let ints: Vec<Option<i64>> = u.child(INT).as_primitive::<Int64Type>().iter().collect();
    let floats: Vec<Option<f64>> = u
        .child(FLOAT)
        .as_primitive::<Float64Type>()
        .iter()
        .collect();
    let strs: Vec<Option<&str>> = u.child(STR).as_string::<i32>().iter().collect();
    let arrays: Vec<Option<&str>> = u.child(ARRAY).as_string::<i32>().iter().collect();
    let objects: Vec<Option<&str>> = u.child(OBJECT).as_string::<i32>().iter().collect();
    Ok(u.type_ids()
        .iter()
        .enumerate()
        .map(|(row, id)| match *id {
            BOOL => at(&bools, row).map(|b| b.to_string()),
            INT => at(&ints, row).map(|i| i.to_string()),
            // A float that isn't finite is `null`, as serde_json and the
            // library write it.
            FLOAT => at(&floats, row).and_then(|f| serde_json::to_string(&f).ok()),
            STR => at(&strs, row).and_then(|s| serde_json::to_string(s).ok()),
            ARRAY => at(&arrays, row).map(str::to_owned),
            OBJECT => at(&objects, row).map(str::to_owned),
            _ => None,
        })
        .collect())
}

fn at<T: Copy>(values: &[Option<T>], row: usize) -> Option<T> {
    values.get(row).copied().flatten()
}

/// SQL values as the JSON union: null, a boolean, an integer, a float or a
/// string.
fn from_scalars(a: &ArrayRef) -> Result<ArrayRef> {
    let t = a.data_type();
    if *t == DataType::Null {
        return union_of((0..a.len()).map(|_| Member::Null));
    }
    if *t == DataType::Boolean {
        return union_of(
            a.as_boolean()
                .iter()
                .map(|b| b.map_or(Member::Null, Member::Bool)),
        );
    }
    if is_int(t) {
        let strict = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        let i = cast_with_options(a, &DataType::Int64, &strict).map_err(|_| {
            exec_datafusion_err!(
                "json_from_scalar: an integer past {} is out of range",
                i64::MAX
            )
        })?;
        return union_of(
            i.as_primitive::<Int64Type>()
                .iter()
                .map(|i| i.map_or(Member::Null, Member::Int)),
        );
    }
    if is_float(t) {
        let f = cast(a, &DataType::Float64)?;
        return union_of(
            f.as_primitive::<Float64Type>()
                .iter()
                .map(|f| f.map_or(Member::Null, Member::Float)),
        );
    }
    if is_str(t) {
        let s = cast(a, &DataType::Utf8)?;
        return union_of(
            s.as_string::<i32>()
                .iter()
                .map(|s| s.map_or(Member::Null, |s| Member::Str(Cow::Borrowed(s)))),
        );
    }
    exec_err!("json_from_scalar takes a boolean, a number or a string, not {t}")
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn the_functions_are_in_their_kinds_order() {
        for (i, (kind, name, _)) in NAMES.iter().enumerate() {
            assert_eq!(*kind as usize, i, "{name}");
            assert_eq!(udf(*kind).name(), *name);
        }
    }

    #[test]
    fn a_path_finds_keys_and_indexes() {
        use Step::{Index as I, Key as K};
        let doc =
            r#" {"a": {"b": [10, "x", {"c": true}]}, "k": 1, "k": 2, "0": "zero", "e\"q": 3} "#;
        let cases: &[(&[Step], Option<&str>)] = &[
            (&[K("a"), K("b"), I(1)], Some(r#""x""#)),
            (&[K("a"), K("b"), K("2"), K("c")], Some("true")),
            (&[K("a"), K("b"), I(-1), K("c")], Some("true")),
            (&[K("a"), K("b"), I(-3)], Some("10")),
            (&[K("a"), K("b"), I(-4)], None),
            (&[K("a"), K("b"), I(3)], None),
            (&[K("a"), K("b"), K("x")], None),
            (&[K("a"), K("b"), K("-1")], Some(r#"{"c": true}"#)),
            // The last of a key that's there twice.
            (&[K("k")], Some("2")),
            // An index doesn't reach into an object; a key that spells one
            // is a key there.
            (&[I(0)], None),
            (&[K("0")], Some(r#""zero""#)),
            (&[K("e\"q")], Some("3")),
            (&[K("a"), K("b"), I(0), K("x")], None),
            (&[K("a"), K("b"), I(1), I(0)], None),
            (&[K("A")], None),
            (
                &[],
                Some(
                    r#"{"a": {"b": [10, "x", {"c": true}]}, "k": 1, "k": 2, "0": "zero", "e\"q": 3}"#,
                ),
            ),
        ];
        for (path, want) in cases {
            assert_eq!(find(doc, path), *want, "{path:?}");
        }
    }

    #[test]
    fn a_document_that_isnt_json_throughout_has_nothing() {
        use Step::Key as K;
        for doc in [
            "",
            "not json",
            r#"{"a": 1"#,
            r#"{"a": 1} x"#,
            r#"{"a": 1, "b": }"#,
            r#"{"a": 1, "b": [1, 2}"#,
            r#"{'a': 1}"#,
            "[1] [2]",
            "NaN",
        ] {
            assert_eq!(find(doc, &[K("a")]), None, "{doc}");
            assert_eq!(find(doc, &[]), None, "{doc}");
        }
        // What a path doesn't enter is read past without recursion, so a
        // document can nest deeper than serde_json's limit of 128.
        let deep = format!(
            r#"{{"a": {}{}, "b": 1}}"#,
            "[".repeat(1000),
            "]".repeat(1000)
        );
        assert_eq!(find(&deep, &[K("b")]), Some("1"));
        assert_eq!(find(r#"{"a": 1, "b": 2}"#, &[K("a")]), Some("1"));
    }

    #[test]
    fn values_read_as_each_type() {
        assert_eq!(int("9223372036854775807"), Some(i64::MAX));
        assert_eq!(int("9223372036854775808"), None);
        assert_eq!(int("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(int("1.5e3"), Some(1500));
        assert_eq!(int("7.0"), Some(7));
        assert_eq!(int("1.5"), None);
        assert_eq!(int("-0.0"), Some(0));
        assert_eq!(int("9.223372036854776e18"), None);
        assert_eq!(int("1e400"), None);
        assert_eq!(int(r#""42""#), Some(42));
        assert_eq!(int(r#""1.0""#), None);
        assert_eq!(int("true"), None);
        assert_eq!(int("[1]"), None);
        assert_eq!(float("1e400"), Some(f64::INFINITY));
        assert_eq!(float("18446744073709551615"), Some(1.8446744073709552e19));
        assert_eq!(float(r#""1.5""#), Some(1.5));
        assert_eq!(float("null"), None);
        assert_eq!(boolean("false"), Some(false));
        assert_eq!(boolean(r#""true""#), Some(true));
        assert_eq!(boolean(r#""True""#), None);
        assert_eq!(boolean("1"), None);
        assert_eq!(string(r#""a\nb""#).as_deref(), Some("a\nb"));
        assert_eq!(string(r#""é""#).as_deref(), Some("é"));
        assert_eq!(string("1"), None);
        assert_eq!(as_text(r#""a""#).as_deref(), Some("a"));
        assert_eq!(as_text("4.2e-1").as_deref(), Some("4.2e-1"));
        assert_eq!(as_text(r#"{"a": [1]}"#).as_deref(), Some(r#"{"a": [1]}"#));
        assert_eq!(as_text("null"), None);
        assert_eq!(length("[1, [2, 3], {}]"), Some(3));
        assert_eq!(length(r#"{"a": 1, "a": 2}"#), Some(2));
        assert_eq!(length(r#""abc""#), None);
        assert_eq!(
            elements(r#"[1, "a", {"b": null}, null]"#),
            Some(vec![
                Cow::Borrowed("1"),
                Cow::Borrowed(r#""a""#),
                Cow::Borrowed(r#"{"b": null}"#),
                Cow::Borrowed("null")
            ])
        );
        assert_eq!(
            keys(r#"{"a": 1, "b!": {"c": 2}}"#),
            Some(vec![Cow::Borrowed("a"), Cow::Owned("b!".to_owned())])
        );
        assert_eq!(keys("[1]"), None);
    }

    fn invoke(kind: Kind, args: Vec<ColumnarValue>, rows: usize) -> ArrayRef {
        let f = udf(kind);
        let arg_fields: Vec<FieldRef> = args
            .iter()
            .map(|a| Arc::new(Field::new("a", a.data_type(), true)))
            .collect();
        let return_field = f
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &vec![None; args.len()],
            })
            .unwrap_or_else(|e| panic!("{e}"));
        let out = f
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: rows,
                return_field: return_field.clone(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .and_then(|v| v.into_array(rows))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(out.data_type(), return_field.data_type());
        out
    }

    fn docs(rows: &[Option<&str>]) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(rows.to_vec())))
    }

    fn key(k: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::from(k))
    }

    fn strs(a: &ArrayRef) -> Vec<Option<String>> {
        a.as_string::<i32>()
            .iter()
            .map(|s| s.map(str::to_owned))
            .collect()
    }

    #[test]
    fn paths_differ_by_row_and_come_as_lists() {
        let d = docs(&[
            Some(r#"{"a": {"b": 1}, "c": [5, 6]}"#),
            Some(r#"{"a": {"b": 2}, "c": [7, 8]}"#),
            Some(r#"{"a": {"b": 3}, "c": [9]}"#),
            None,
        ]);
        let keys = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("a"),
            Some("c"),
            None,
            Some("a"),
        ])));
        let indexes = ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(0),
            Some(-1),
            Some(0),
            Some(0),
        ])));
        let out = invoke(Kind::GetJson, vec![d.clone(), keys, indexes], 4);
        assert_eq!(
            strs(&out),
            [None, Some("8".to_owned()), None, None],
            "an index reaches no object's key, and a null step nothing"
        );
        let ints = |a: ArrayRef| a.as_primitive::<Int64Type>().iter().collect::<Vec<_>>();
        let out = invoke(
            Kind::GetInt,
            vec![d.clone(), list(vec![Some("a"), Some("b")])],
            4,
        );
        assert_eq!(ints(out), [Some(1), Some(2), Some(3), None]);
        let out = invoke(
            Kind::GetInt,
            vec![
                d.clone(),
                list(vec![Some("c")]),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            4,
        );
        assert_eq!(ints(out), [Some(6), Some(8), Some(9), None]);
        let out = invoke(
            Kind::Contains,
            vec![d.clone(), list(vec![Some("a"), None])],
            4,
        );
        assert_eq!(
            out.as_boolean().iter().collect::<Vec<_>>(),
            [Some(false); 4]
        );
        // A list per row.
        let mut b = ListBuilder::new(StringBuilder::new());
        b.append_value([Some("a"), Some("b")]);
        b.append_value([Some("c"), Some("1")]);
        b.append_null();
        b.append_value([Some("a")]);
        let per_row = ColumnarValue::Array(Arc::new(b.finish()));
        let out = invoke(Kind::GetJson, vec![d, per_row], 4);
        assert_eq!(
            strs(&out),
            [Some("1".to_owned()), Some("8".to_owned()), None, None]
        );
    }

    fn list(items: Vec<Option<&str>>) -> ColumnarValue {
        let mut b = ListBuilder::new(StringBuilder::new());
        b.append_value(items);
        ColumnarValue::Scalar(ScalarValue::List(Arc::new(b.finish())))
    }

    #[test]
    fn json_get_gives_the_union_and_its_text_round_trips() {
        let d = docs(&[
            Some(r#"{"v": "a\"b"}"#),
            Some(r#"{"v": 42}"#),
            Some(r#"{"v": 4.2e-1}"#),
            Some(r#"{"v": true}"#),
            Some(r#"{"v": [1, {"x": null}]}"#),
            Some(r#"{"v": {"y": 1}}"#),
            Some(r#"{"v": null}"#),
            Some(r#"{"w": 1}"#),
            Some(r#"{"v": 18446744073709551615}"#),
            Some("not json"),
            None,
        ]);
        let u = invoke(Kind::Get, vec![d.clone(), key("v")], 11);
        assert!(is_union(u.data_type()));
        let nulls: Vec<bool> = u
            .logical_nulls()
            .map(|n| n.iter().map(|valid| !valid).collect())
            .unwrap_or_default();
        assert_eq!(
            nulls,
            [
                false, false, false, false, false, false, true, true, true, true, true
            ]
        );
        let text = invoke(Kind::UnionToText, vec![ColumnarValue::Array(u)], 11);
        let want = [
            Some(r#""a\"b""#),
            Some("42"),
            Some("0.42"),
            Some("true"),
            Some(r#"[1, {"x": null}]"#),
            Some(r#"{"y": 1}"#),
            None,
            None,
            None,
            None,
            None,
        ];
        assert_eq!(
            strs(&text),
            want.map(|s| s.map(str::to_owned)),
            "the union's text is written again"
        );
        let exact = invoke(Kind::UnionToText, vec![d, key("v")], 11);
        let mut want = want;
        want[2] = Some("4.2e-1");
        want[8] = Some("18446744073709551615");
        assert_eq!(
            strs(&exact),
            want.map(|s| s.map(str::to_owned)),
            "the path form's is as it's written"
        );
    }

    #[test]
    fn the_union_is_json_for_the_other_functions() {
        let d = docs(&[Some(r#"{"a": {"b": "x"}}"#), Some(r#"{"a": "{\"b\": 1}"}"#)]);
        let u = invoke(Kind::Get, vec![d, key("a")], 2);
        let out = invoke(
            Kind::GetStr,
            vec![ColumnarValue::Array(u.clone()), key("b")],
            2,
        );
        assert_eq!(strs(&out), [Some("x".to_owned()), None]);
        let out = invoke(Kind::GetJson, vec![ColumnarValue::Array(u)], 2);
        assert_eq!(
            strs(&out),
            [
                Some(r#"{"b": "x"}"#.to_owned()),
                Some(r#""{\"b\": 1}""#.to_owned())
            ]
        );
    }

    #[test]
    fn scalars_become_the_union() {
        let cases: Vec<(ArrayRef, Vec<Option<&str>>)> = vec![
            (
                Arc::new(Int32Array::from(vec![Some(1), None])),
                vec![Some("1"), None],
            ),
            (
                Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN)])),
                vec![Some("1.5"), Some("null")],
            ),
            (
                Arc::new(StringArray::from(vec![Some("a\""), Some("")])),
                vec![Some(r#""a\"""#), Some(r#""""#)],
            ),
            (
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                vec![Some("true"), None],
            ),
            (Arc::new(NullArray::new(2)), vec![None, None]),
        ];
        for (a, want) in cases {
            let u = invoke(Kind::FromScalar, vec![ColumnarValue::Array(a)], 2);
            let text = invoke(Kind::UnionToText, vec![ColumnarValue::Array(u)], 2);
            assert_eq!(
                strs(&text),
                want.iter()
                    .map(|s| s.map(str::to_owned))
                    .collect::<Vec<_>>()
            );
        }
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        let e = UDFS[Kind::FromScalar as usize]
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(big)],
                arg_fields: vec![Arc::new(Field::new("a", DataType::UInt64, true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", union_type(), true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .err()
            .map(|e| e.message().into_owned());
        assert!(
            e.as_deref().is_some_and(|e| e.contains("out of range")),
            "{e:?}"
        );
    }

    /// A small differential test: values found by the streaming walk match
    /// serde_json's tree, for documents and paths made from a seed.
    #[test]
    fn the_walk_agrees_with_serde_json() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        fn value(next: &mut dyn FnMut() -> u64, depth: u32) -> serde_json::Value {
            use serde_json::Value as V;
            match next() % if depth > 2 { 5 } else { 7 } {
                0 => V::Null,
                1 => V::Bool(next().is_multiple_of(2)),
                2 => V::from(next() as i64 >> (next() % 64)),
                3 => V::from((next() % 10_000) as f64 / 7.0),
                4 => V::from(["a", "é", "q\"t", "\\", "\u{1F600}", ""][(next() % 6) as usize]),
                5 => V::Array((0..next() % 4).map(|_| value(next, depth + 1)).collect()),
                _ => V::Object(
                    (0..next() % 4)
                        .map(|_| {
                            let k = ["a", "b", "c", "é"][(next() % 4) as usize].to_owned();
                            (k, value(next, depth + 1))
                        })
                        .collect(),
                ),
            }
        }
        for _ in 0..2000 {
            let doc = value(&mut next, 0);
            let text = serde_json::to_string(&doc).unwrap_or_default();
            let mut path: Vec<String> = Vec::new();
            let mut at = Some(&doc);
            let mut steps = Vec::new();
            for _ in 0..(next() % 4) {
                match at {
                    Some(serde_json::Value::Object(o))
                        if !o.is_empty() && !next().is_multiple_of(4) =>
                    {
                        let Some((k, v)) = o.iter().nth((next() as usize) % o.len()) else {
                            break;
                        };
                        path.push(k.clone());
                        at = Some(v);
                    }
                    Some(serde_json::Value::Array(a))
                        if !a.is_empty() && !next().is_multiple_of(4) =>
                    {
                        let i = (next() as usize) % a.len();
                        // From the start or from the end.
                        let step = if next().is_multiple_of(2) {
                            format!("#{i}")
                        } else {
                            format!("#-{}", a.len() - i)
                        };
                        path.push(step);
                        at = a.get(i);
                    }
                    _ => {
                        path.push("zz".into());
                        at = None;
                    }
                }
            }
            for p in &path {
                steps.push(match p.strip_prefix('#') {
                    Some(i) => Step::Index(i.parse().unwrap_or(0)),
                    None => Step::Key(p),
                });
            }
            let want = at.map(|v| serde_json::to_string(v).unwrap_or_default());
            let got = find(&text, &steps).map(str::to_owned);
            assert_eq!(got, want, "{text} at {path:?}");
            if let Some(v) = at {
                let found = find(&text, &steps);
                assert_eq!(
                    found.and_then(int),
                    v.as_i64()
                        .or_else(|| v.as_f64().and_then(integral))
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                );
                assert_eq!(
                    found.and_then(string).map(Cow::into_owned),
                    v.as_str().map(str::to_owned)
                );
                assert_eq!(
                    found.and_then(boolean),
                    v.as_bool()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                );
            }
        }
    }
}

//! Expressions written over a stream's own columns, as text of ours, which a
//! stateful operator's ID hashes. Aliases, views, the `FROM` alias, how
//! DataFusion shapes a plan and how it prints an expression don't enter it.

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DataFusionError, NullHandling};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr as DfExpr, LogicalPlan};

/// `e`, an expression over `node`'s output, as one over the columns of the
/// stream the plan scans: each column it reads replaced by what computes it.
pub(crate) fn unfold(e: &DfExpr, node: &LogicalPlan) -> Result<DfExpr, String> {
    e.clone()
        .unalias_nested()
        .data
        .transform_up(|x| match &x {
            DfExpr::Column(c) => column(c, node)
                .map(Transformed::yes)
                .map_err(DataFusionError::Plan),
            _ => Ok(Transformed::no(x)),
        })
        .map(|t| t.data)
        .map_err(|e| e.to_string())
}

/// Column `c` of `node`'s output, over the stream's columns.
fn column(c: &Column, node: &LogicalPlan) -> Result<DfExpr, String> {
    let i = node
        .schema()
        .index_of_column(c)
        .map_err(|e| e.to_string())?;
    let below = |input: &LogicalPlan, j: usize| {
        let (q, f) = input.schema().qualified_field(j);
        column(&Column::new(q.cloned(), f.name()), input)
    };
    match node {
        LogicalPlan::TableScan(s) => Ok(DfExpr::Column(Column::new_unqualified(
            s.projected_schema.field(i).name(),
        ))),
        LogicalPlan::Projection(p) => unfold(&p.expr[i], &p.input),
        LogicalPlan::Filter(f) => below(&f.input, i),
        LogicalPlan::SubqueryAlias(a) => below(&a.input, i),
        LogicalPlan::Unnest(u) => {
            let src = u.dependency_indices[i];
            let value = below(&u.input, src)?;
            match u.list_type_columns.iter().any(|(j, _)| *j == src) {
                true => Ok(DfExpr::Column(Column::new_unqualified(unnested(
                    &value,
                    u.options.null_handling,
                )))),
                false => Ok(value),
            }
        }
        other => Err(format!("a {} step", other.display())),
    }
}

fn unnested(list: &DfExpr, nulls: NullHandling) -> String {
    let outer = match nulls {
        NullHandling::Drop => "",
        _ => "_outer",
    };
    format!("unnest{outer}({})", text(list))
}

/// What the steps from `input` down to its scan do to the rows, other than
/// computing columns: each conjunct of each filter, and each list unnested,
/// over the stream's columns, sorted.
pub(crate) fn rows(input: &LogicalPlan) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut node = input;
    loop {
        match node {
            LogicalPlan::Filter(f) => {
                for c in split_conjunction(&f.predicate) {
                    if !matches!(
                        c,
                        DfExpr::Literal(datafusion_common::ScalarValue::Boolean(Some(true)), _)
                    ) {
                        out.push(format!("where {}", text(&unfold(c, &f.input)?)));
                    }
                }
            }
            LogicalPlan::Unnest(u) => {
                for (j, _) in &u.list_type_columns {
                    let (q, f) = u.input.schema().qualified_field(*j);
                    let list = column(&Column::new(q.cloned(), f.name()), &u.input)?;
                    out.push(unnested(&list, u.options.null_handling));
                }
            }
            LogicalPlan::TableScan(_) => break,
            _ => {}
        }
        let inputs = node.inputs();
        let [next] = inputs.as_slice() else {
            return Err(format!("a {} step", node.display()));
        };
        node = next;
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// `e` as text of ours. Columns are by name, quoted where a name needs it,
/// constants carry their type, and every operation is bracketed.
pub(crate) fn text(e: &DfExpr) -> String {
    use DfExpr as E;
    let list = |es: &[DfExpr]| es.iter().map(text).collect::<Vec<_>>().join(", ");
    let not = |negated: bool| if negated { "not " } else { "" };
    match e {
        E::Alias(a) => text(&a.expr),
        E::Column(c) => crate::ident(&c.name),
        E::Literal(v, _) => match v.is_null() {
            true => format!("null::{}", type_text(&v.data_type())),
            false => format!("{:?}::{}", v.to_string(), type_text(&v.data_type())),
        },
        E::BinaryExpr(b) => format!("({} {} {})", text(&b.left), b.op, text(&b.right)),
        E::Like(l) | E::SimilarTo(l) => format!(
            "{}{}({}, {}, {:?}, {})",
            not(l.negated),
            match e {
                E::Like(_) => "like",
                _ => "similar_to",
            },
            text(&l.expr),
            text(&l.pattern),
            l.escape_char,
            l.case_insensitive
        ),
        E::Not(x) => format!("not({})", text(x)),
        E::IsNotNull(x) => format!("is_not_null({})", text(x)),
        E::IsNull(x) => format!("is_null({})", text(x)),
        E::IsTrue(x) => format!("is_true({})", text(x)),
        E::IsFalse(x) => format!("is_false({})", text(x)),
        E::IsUnknown(x) => format!("is_unknown({})", text(x)),
        E::IsNotTrue(x) => format!("is_not_true({})", text(x)),
        E::IsNotFalse(x) => format!("is_not_false({})", text(x)),
        E::IsNotUnknown(x) => format!("is_not_unknown({})", text(x)),
        E::Negative(x) => format!("negative({})", text(x)),
        E::Between(b) => format!(
            "{}between({}, {}, {})",
            not(b.negated),
            text(&b.expr),
            text(&b.low),
            text(&b.high)
        ),
        E::Case(c) => {
            let mut s = String::from("case(");
            if let Some(x) = &c.expr {
                s.push_str(&text(x));
            }
            for (w, t) in &c.when_then_expr {
                s.push_str(&format!("; when {} then {}", text(w), text(t)));
            }
            if let Some(x) = &c.else_expr {
                s.push_str(&format!("; else {}", text(x)));
            }
            s.push(')');
            s
        }
        E::Cast(c) => format!(
            "cast({} as {})",
            text(&c.expr),
            type_text(c.field.data_type())
        ),
        E::TryCast(c) => format!(
            "try_cast({} as {})",
            text(&c.expr),
            type_text(c.field.data_type())
        ),
        E::ScalarFunction(f) => format!("{}({})", f.func.name(), list(&f.args)),
        E::InList(l) => format!(
            "{}in({}, [{}])",
            not(l.negated),
            text(&l.expr),
            list(&l.list)
        ),
        other => other.to_string(),
    }
}

/// A type's name of ours.
pub(crate) fn type_text(t: &DataType) -> String {
    use DataType as T;
    let unit = |u: &TimeUnit| match u {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    };
    match t {
        T::Null => "null".into(),
        T::Boolean => "bool".into(),
        T::Int8 => "int8".into(),
        T::Int16 => "int16".into(),
        T::Int32 => "int32".into(),
        T::Int64 => "int64".into(),
        T::UInt8 => "uint8".into(),
        T::UInt16 => "uint16".into(),
        T::UInt32 => "uint32".into(),
        T::UInt64 => "uint64".into(),
        T::Float16 => "float16".into(),
        T::Float32 => "float32".into(),
        T::Float64 => "float64".into(),
        T::Utf8 => "utf8".into(),
        T::LargeUtf8 => "large_utf8".into(),
        T::Utf8View => "utf8_view".into(),
        T::Binary => "binary".into(),
        T::LargeBinary => "large_binary".into(),
        T::BinaryView => "binary_view".into(),
        T::FixedSizeBinary(n) => format!("fixed_size_binary({n})"),
        T::Date32 => "date32".into(),
        T::Date64 => "date64".into(),
        T::Time32(u) => format!("time32[{}]", unit(u)),
        T::Time64(u) => format!("time64[{}]", unit(u)),
        T::Timestamp(u, None) => format!("timestamp[{}]", unit(u)),
        T::Timestamp(u, Some(tz)) => format!("timestamp[{}, tz={tz}]", unit(u)),
        T::Duration(u) => format!("duration[{}]", unit(u)),
        T::Decimal128(p, s) => format!("decimal128({p}, {s})"),
        T::Decimal256(p, s) => format!("decimal256({p}, {s})"),
        T::List(f) => format!("list<{}>", type_text(f.data_type())),
        T::LargeList(f) => format!("large_list<{}>", type_text(f.data_type())),
        T::FixedSizeList(f, n) => format!("fixed_size_list<{}, {n}>", type_text(f.data_type())),
        T::Struct(fs) => {
            let fields: Vec<String> = fs
                .iter()
                .map(|f| format!("{}: {}", crate::ident(f.name()), type_text(f.data_type())))
                .collect();
            format!("struct<{}>", fields.join(", "))
        }
        other => format!("{other:?}"),
    }
}

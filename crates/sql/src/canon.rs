//! Expressions written over a stream's own columns, as text of ours, which a
//! stateful operator's ID hashes. Aliases, views, the `FROM` alias, how
//! DataFusion shapes a plan and how it prints an expression don't enter it.

use std::collections::HashMap;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::{Column, NullHandling};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr as DfExpr, LogicalPlan};
use twox_hash::XxHash3_64;

/// The longest text a column of a step is written as. A longer one is
/// written as `$` and the hash of its text, so the text of a chain of steps
/// that each read a column several times grows with the steps, not with
/// the columns they expand to.
const LONGEST: usize = 256;

/// Writes expressions over a plan's steps as text over the stream's
/// columns. It keeps each column of each step it has written, so a column
/// read many times is written once.
#[derive(Default)]
pub(crate) struct Canon {
    columns: HashMap<(usize, usize), String>,
}

impl Canon {
    /// `e`, an expression over `node`'s output, as text over the stream's
    /// columns: each column it reads written as what computes it.
    pub(crate) fn over(&mut self, e: &DfExpr, node: &LogicalPlan) -> Result<String, String> {
        use DfExpr as E;
        let not = |negated: bool| if negated { "not " } else { "" };
        Ok(match e {
            E::Alias(a) => self.over(&a.expr, node)?,
            E::Column(c) => self.column(c, node)?,
            E::Literal(v, _) => match v.is_null() {
                true => format!("null::{}", type_text(&v.data_type())),
                false => format!("{:?}::{}", v.to_string(), type_text(&v.data_type())),
            },
            E::BinaryExpr(b) => format!(
                "({} {} {})",
                self.over(&b.left, node)?,
                b.op,
                self.over(&b.right, node)?
            ),
            E::Like(l) | E::SimilarTo(l) => format!(
                "{}{}({}, {}, {:?}, {})",
                not(l.negated),
                match e {
                    E::Like(_) => "like",
                    _ => "similar_to",
                },
                self.over(&l.expr, node)?,
                self.over(&l.pattern, node)?,
                l.escape_char,
                l.case_insensitive
            ),
            E::Not(x) => format!("not({})", self.over(x, node)?),
            E::IsNotNull(x) => format!("is_not_null({})", self.over(x, node)?),
            E::IsNull(x) => format!("is_null({})", self.over(x, node)?),
            E::IsTrue(x) => format!("is_true({})", self.over(x, node)?),
            E::IsFalse(x) => format!("is_false({})", self.over(x, node)?),
            E::IsUnknown(x) => format!("is_unknown({})", self.over(x, node)?),
            E::IsNotTrue(x) => format!("is_not_true({})", self.over(x, node)?),
            E::IsNotFalse(x) => format!("is_not_false({})", self.over(x, node)?),
            E::IsNotUnknown(x) => format!("is_not_unknown({})", self.over(x, node)?),
            E::Negative(x) => format!("negative({})", self.over(x, node)?),
            E::Between(b) => format!(
                "{}between({}, {}, {})",
                not(b.negated),
                self.over(&b.expr, node)?,
                self.over(&b.low, node)?,
                self.over(&b.high, node)?
            ),
            E::Case(c) => {
                let mut s = String::from("case(");
                if let Some(x) = &c.expr {
                    s.push_str(&self.over(x, node)?);
                }
                for (w, t) in &c.when_then_expr {
                    s.push_str(&format!(
                        "; when {} then {}",
                        self.over(w, node)?,
                        self.over(t, node)?
                    ));
                }
                if let Some(x) = &c.else_expr {
                    s.push_str(&format!("; else {}", self.over(x, node)?));
                }
                s.push(')');
                s
            }
            E::Cast(c) => format!(
                "cast({} as {})",
                self.over(&c.expr, node)?,
                type_text(c.field.data_type())
            ),
            E::TryCast(c) => format!(
                "try_cast({} as {})",
                self.over(&c.expr, node)?,
                type_text(c.field.data_type())
            ),
            E::ScalarFunction(f) => format!("{}({})", f.func.name(), self.list(&f.args, node)?),
            E::InList(l) => format!(
                "{}in({}, [{}])",
                not(l.negated),
                self.over(&l.expr, node)?,
                self.list(&l.list, node)?
            ),
            other => {
                let mut cols = Vec::new();
                for c in other.column_refs() {
                    cols.push(self.column(c, node)?);
                }
                format!("{other} over [{}]", cols.join(", "))
            }
        })
    }

    fn list(&mut self, es: &[DfExpr], node: &LogicalPlan) -> Result<String, String> {
        let mut out = Vec::with_capacity(es.len());
        for e in es {
            out.push(self.over(e, node)?);
        }
        Ok(out.join(", "))
    }

    /// Column `c` of `node`'s output, over the stream's columns: a stream's
    /// column by its name, quoted where it needs it, so no name reads as an
    /// expression or as an unnest.
    fn column(&mut self, c: &Column, node: &LogicalPlan) -> Result<String, String> {
        let i = node
            .schema()
            .index_of_column(c)
            .map_err(|e| e.to_string())?;
        let key = (std::ptr::from_ref(node) as usize, i);
        if let Some(t) = self.columns.get(&key) {
            return Ok(t.clone());
        }
        let text = match node {
            LogicalPlan::TableScan(s) => crate::ident(s.projected_schema.field(i).name()),
            LogicalPlan::Projection(p) => self.over(&p.expr[i], &p.input)?,
            LogicalPlan::Filter(f) => self.below(&f.input, i)?,
            LogicalPlan::SubqueryAlias(a) => self.below(&a.input, i)?,
            LogicalPlan::Unnest(u) => {
                let src = u.dependency_indices[i];
                let value = self.below(&u.input, src)?;
                match u.list_type_columns.iter().any(|(j, _)| *j == src) {
                    true => unnested(&value, u.options.null_handling),
                    false => value,
                }
            }
            other => return Err(format!("a {} step", other.display())),
        };
        let text = match text.len() > LONGEST {
            true => format!("${:016x}", XxHash3_64::oneshot(text.as_bytes())),
            false => text,
        };
        self.columns.insert(key, text.clone());
        Ok(text)
    }

    /// Column `j` of `input`'s output.
    fn below(&mut self, input: &LogicalPlan, j: usize) -> Result<String, String> {
        let (q, f) = input.schema().qualified_field(j);
        self.column(&Column::new(q.cloned(), f.name()), input)
    }

    /// What the steps from `input` down to its scan do to the rows, other
    /// than computing columns: each conjunct of each filter, and each unnest
    /// step with the lists it unnests together, over the stream's columns,
    /// sorted. A conjunct written twice filters as once does; an unnest step
    /// written twice multiplies the rows again, so each stays.
    pub(crate) fn rows(&mut self, input: &LogicalPlan) -> Result<Vec<String>, String> {
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
                            out.push(format!("where {}", self.over(c, &f.input)?));
                        }
                    }
                }
                LogicalPlan::Unnest(u) => {
                    // One entry per step: lists unnested together give a row
                    // per element of the longest, and steps one after
                    // another multiply.
                    let mut lists = Vec::with_capacity(u.list_type_columns.len());
                    for (j, _) in &u.list_type_columns {
                        lists.push(self.below(&u.input, *j)?);
                    }
                    out.push(format!(
                        "unnest step {}",
                        unnested(&lists.join(", "), u.options.null_handling)
                    ));
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
        out.dedup_by(|a, b| a == b && a.starts_with("where "));
        Ok(out)
    }
}

/// An unnest of a list, which no stream column's name can read as: a name
/// with brackets is quoted.
fn unnested(list: &str, nulls: NullHandling) -> String {
    let outer = match nulls {
        NullHandling::Drop => "",
        _ => "_outer",
    };
    format!("unnest{outer}({list})")
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

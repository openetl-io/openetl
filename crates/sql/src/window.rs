//! Group windows: `tumble`, `hop` and `session` in a statement's `GROUP BY`,
//! with `window_start` and `window_end` for their bounds, the aggregates
//! they run, and the lowering of a bound query that groups by one.
//!
//! A window's call is never evaluated: binding types it as its bounds, a
//! struct of `start` and `end`, and lowering takes its time column and its
//! sizes off it. The aggregates are DataFusion's for binding and typing;
//! the engine's accumulators run them.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::logical_plan::Aggregate as DfAggregate;
use datafusion_expr::{
    AggregateUDF, ColumnarValue, Expr as DfExpr, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_sql::sqlparser::ast::{self, Spanned};
use datafusion_sql::sqlparser::tokenizer::Span;
use twox_hash::XxHash3_64;

use crate::plan::{
    Aggregate, AggregateKind, Column, GroupKey, GroupWindow, Projection, RowWise, Step, Window,
};
use crate::{Catalog, Class, Error, ErrorKind, Expr, Position};

pub(crate) const WINDOWS: [&str; 3] = ["tumble", "hop", "session"];

/// The aggregates a group window runs, by name and by alias.
pub(crate) const AGGREGATES: &[&str] = &[
    "approx_distinct",
    "avg",
    "count",
    "max",
    "mean",
    "median",
    "min",
    "percentile_cont",
    "quantile_cont",
    "sum",
];

/// The aggregates a group window runs, for messages.
pub(crate) const NAMED: &str =
    "count, sum, min, max, avg, approx_distinct, median and percentile_cont";

/// The grouped rows' columns that hold each window's bounds. A key or an
/// aggregate can't have these names.
const START: &str = "__openetl_window_start";
const END: &str = "__openetl_window_end";

/// The layout of a group window's state, which its operator's ID covers.
const STATE: u32 = 1;

/// The most windows of a `hop` a row is in.
const MAX_HOPS: i64 = 1000;

pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
    [("tumble", 1), ("hop", 2), ("session", 1)]
        .into_iter()
        .map(|(name, intervals)| {
            Arc::new(ScalarUDF::new_from_impl(WindowFn {
                name,
                intervals,
                signature: Signature::user_defined(Volatility::Immutable),
            }))
        })
        .collect()
}

pub(crate) fn aggregates() -> Vec<Arc<AggregateUDF>> {
    use datafusion_functions_aggregate as a;
    vec![
        a::count::count_udaf(),
        a::sum::sum_udaf(),
        a::min_max::min_udaf(),
        a::min_max::max_udaf(),
        a::average::avg_udaf(),
        a::approx_distinct::approx_distinct_udaf(),
        a::median::median_udaf(),
        a::percentile_cont::percentile_cont_udaf(),
    ]
}

/// A window's call, as binding types it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WindowFn {
    name: &'static str,
    /// How many intervals follow its time.
    intervals: usize,
    signature: Signature,
}

impl WindowFn {
    fn usage(&self) -> &'static str {
        match self.name {
            "hop" => {
                "hop(time, slide, size), as hop(created_at, INTERVAL '1 minute', INTERVAL '5 minutes')"
            }
            "session" => "session(time, gap), as session(created_at, INTERVAL '30 minutes')",
            _ => "tumble(time, size), as tumble(created_at, INTERVAL '1 minute')",
        }
    }
}

impl ScalarUDFImpl for WindowFn {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args
            .iter()
            .enumerate()
            .map(|(i, t)| match (i, t) {
                (1.., DataType::Interval(_)) => DataType::Interval(IntervalUnit::MonthDayNano),
                _ => t.clone(),
            })
            .collect())
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() != 1 + self.intervals {
            return plan_err!("{} takes {}", self.name, self.usage());
        }
        let DataType::Timestamp(..) = &args[0] else {
            return plan_err!(
                "{}'s first argument is the stream's event time, a timestamp, not {}",
                self.name,
                args[0]
            );
        };
        if let Some(t) = args[1..]
            .iter()
            .find(|t| !matches!(t, DataType::Interval(IntervalUnit::MonthDayNano)))
        {
            return plan_err!(
                "{} takes an INTERVAL after its time, such as INTERVAL '1 minute', not {t}: {}",
                self.name,
                self.usage()
            );
        }
        Ok(bounds(&args[0]))
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("{} is a group window, which only GROUP BY takes", self.name)
    }
}

/// What a window's call gives: its bounds, of its time's type.
fn bounds(time: &DataType) -> DataType {
    DataType::Struct(
        vec![
            Field::new("start", time.clone(), true),
            Field::new("end", time.clone(), true),
        ]
        .into(),
    )
}

fn window_name(f: &ast::Function) -> Option<&'static str> {
    let name = f.name.0.last()?.as_ident()?;
    let folded = match name.quote_style {
        Some(_) => name.value.clone(),
        None => name.value.to_lowercase(),
    };
    WINDOWS.into_iter().find(|w| *w == folded)
}

/// The first window's call in a SELECT's list, WHERE or HAVING, which
/// only GROUP BY takes, and its name.
pub(crate) fn stray(s: &ast::Select) -> Option<(Span, String)> {
    let mut found = None;
    let mut look = |e: &ast::Expr| {
        if let ast::Expr::Function(f) = e
            && let Some(name) = window_name(f)
        {
            found = Some((e.span(), name.to_owned()));
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    };
    let _ = ast::visit_expressions(&s.projection, &mut look);
    let _ = ast::visit_expressions(&s.selection, &mut look);
    let _ = ast::visit_expressions(&s.having, &mut look);
    found
}

/// Which bound an identifier names: an unquoted `window_start` or
/// `window_end`.
fn bound(i: &ast::Ident) -> Option<&'static str> {
    if i.quote_style.is_some() {
        return None;
    }
    if i.value.eq_ignore_ascii_case("window_start") {
        Some("start")
    } else if i.value.eq_ignore_ascii_case("window_end") {
        Some("end")
    } else {
        None
    }
}

/// `get_field(window, 'bound')`.
fn field(window: &ast::Expr, bound: &str) -> ast::Expr {
    let arg = |e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e));
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
            "get_field",
        ))]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                arg(window.clone()),
                arg(ast::Expr::value(ast::Value::SingleQuotedString(
                    bound.to_owned(),
                ))),
            ],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// Where a SELECT groups by exactly one window, makes each `window_start`
/// and `window_end` in its list and HAVING that window's bound, named as
/// written where it stands alone; gives where WHERE reads one, which it
/// can't, as it runs before rows are grouped.
pub(crate) fn bounds_of(s: &mut ast::Select) -> Vec<(Span, String)> {
    let ast::GroupByExpr::Expressions(exprs, _) = &s.group_by else {
        return Vec::new();
    };
    let windows: Vec<&ast::Expr> = exprs
        .iter()
        .filter(|e| matches!(e, ast::Expr::Function(f) if window_name(f).is_some()))
        .collect();
    let [window] = windows.as_slice() else {
        return Vec::new();
    };
    let window = (*window).clone();
    for item in &mut s.projection {
        if let ast::SelectItem::UnnamedExpr(ast::Expr::Identifier(i)) = item
            && let Some(b) = bound(i)
        {
            *item = ast::SelectItem::ExprWithAlias {
                expr: field(&window, b),
                alias: i.clone(),
            };
        }
    }
    let mut replace = |e: &mut ast::Expr| {
        if let ast::Expr::Identifier(i) = e
            && let Some(b) = bound(i)
        {
            *e = field(&window, b);
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = ast::visit_expressions_mut(&mut s.projection, &mut replace);
    let _ = ast::visit_expressions_mut(&mut s.having, &mut replace);
    let mut refused = Vec::new();
    let _ = ast::visit_expressions(&s.selection, |e| {
        if let ast::Expr::Identifier(i) = e
            && bound(i).is_some()
        {
            refused.push((
                e.span(),
                format!(
                    "{i} is a window's bound, and WHERE runs before rows are grouped: compare it in HAVING"
                ),
            ));
        }
        ControlFlow::<()>::Continue(())
    });
    refused
}

/// Where a group window's errors are.
pub(crate) struct At<'a> {
    /// The statement's INSERT.
    pub(crate) statement: Position,
    /// The window in its GROUP BY.
    pub(crate) window: Position,
    /// Where the outermost SELECT gives a column, by name.
    pub(crate) column: &'a dyn Fn(&str) -> Position,
}

/// A statement that groups by a window, lowered.
pub(crate) struct Lowered {
    pub(crate) operator: GroupWindow,
    pub(crate) schema: arrow_schema::SchemaRef,
    /// `window_start` and the group keys, as the output names them.
    pub(crate) key: Vec<String>,
}

fn refused(at: Position, message: impl Into<String>) -> Error {
    Error::at(ErrorKind::Unsupported, at, message)
}

fn internal(at: Position, what: &str) -> Error {
    Error::at(ErrorKind::Internal, at, format!("planning gave {what}"))
}

/// Whether a group window groups by, or takes `min`, `max` and
/// `approx_distinct` of, a column of type `t`: one its state can keep.
pub(crate) fn groupable(t: &DataType) -> bool {
    use DataType as T;
    matches!(
        t,
        T::Boolean
            | T::Int16
            | T::Int32
            | T::Int64
            | T::Float32
            | T::Float64
            | T::Decimal128(..)
            | T::Utf8
            | T::Binary
            | T::Date32
            | T::Time64(TimeUnit::Microsecond)
            | T::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
            | T::FixedSizeBinary(16)
    )
}

/// Lowers a bound statement whose SELECT groups by a window: the row-wise
/// steps that give the rows grouped, the window, keys and aggregates, then
/// the steps on the grouped rows. Its output's key is `window_start` and
/// the group keys, which must all reach it.
pub(crate) fn lower(
    plan: &LogicalPlan,
    catalog: &Catalog,
    at: &At,
    output: &str,
) -> Result<Lowered, Error> {
    // From the top down to the aggregate.
    let mut above = Vec::new();
    let mut node = plan;
    let agg = loop {
        match node {
            LogicalPlan::Aggregate(a) => break a,
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => above.push(node),
            LogicalPlan::Unnest(_) => {
                return Err(refused(
                    at.statement,
                    "UNNEST of a group window's rows isn't supported: unnest the stream's rows before they're grouped, in a view",
                ));
            }
            other => return Err(crate::lower::refusal(other, at.statement)),
        }
        let inputs = node.inputs();
        let [input] = inputs.as_slice() else {
            return Err(internal(at.statement, "a step with other than one input"));
        };
        node = input;
    };
    let rows = crate::lower::row_wise(&agg.input, catalog, at.statement)?;
    let input = rows.operator.input.clone();
    let Some(stream) = catalog.get(&input) else {
        return Err(internal(
            at.statement,
            "a scan of a stream that isn't in the catalog",
        ));
    };
    if rows.operator.changelog {
        return Err(Error::at(
            ErrorKind::LaterTier(Class::Updating),
            at.window,
            format!(
                "{input} is a changelog, and a group window groups appended rows: transforms don't run updating statements yet (tier 5)"
            ),
        ));
    }
    let schema = agg.input.schema();
    let (window_index, call) = window_call(agg, at)?;
    let time = time_column(call, &rows.operator, stream, &input, at)?;
    let time_index = schema
        .index_of_column(&time)
        .map_err(|e| Error::datafusion(ErrorKind::Bind, at.window, &e))?;
    let time_type = schema.field(time_index).data_type().clone();
    let window = sizes(call, &time_type, at)?;

    let mut keys = Vec::new();
    for (i, e) in agg.group_expr.iter().enumerate() {
        if i == window_index {
            continue;
        }
        let DfExpr::Column(c) = e else {
            return Err(refused(
                at.window,
                format!(
                    "a group window groups by columns of {input}, renamed or not, beside its window: {e} is computed"
                ),
            ));
        };
        let column = schema
            .index_of_column(c)
            .map_err(|e| Error::datafusion(ErrorKind::Bind, at.window, &e))?;
        let Some(from) = rows.operator.origin(&c.name) else {
            return Err(refused(
                at.window,
                format!(
                    "a group window groups by columns of {input}, renamed or not, beside its window: {} is computed",
                    c.name
                ),
            ));
        };
        let t = schema.field(column).data_type();
        if !groupable(t) {
            return Err(refused(
                at.window,
                format!("a group window can't group by {}, of type {t}", c.name),
            ));
        }
        keys.push((
            i,
            GroupKey {
                column,
                stream: from,
            },
        ));
    }
    // The output's key follows GROUP BY. The operator takes its keys by the
    // stream's names, and its aggregates by what they compute, so writing
    // either in another order keeps its key groups, its state and its ID.
    let written: Vec<(usize, String)> = keys.iter().map(|(i, k)| (*i, k.stream.clone())).collect();
    keys.sort_by(|(i, a), (j, b)| a.stream.cmp(&b.stream).then(i.cmp(j)));

    let mut canon = crate::canon::Canon::default();
    let mut aggregates = Vec::with_capacity(agg.aggr_expr.len());
    for (j, e) in agg.aggr_expr.iter().enumerate() {
        let data_type = agg
            .schema
            .field(agg.group_expr.len() + j)
            .data_type()
            .clone();
        let a = aggregate(e, schema, data_type, at)?;
        let computes = computes(&mut canon, e, &a, &agg.input, at)?;
        aggregates.push((j, a, computes));
    }
    aggregates.sort_by(|(i, _, a), (j, _, b)| a.cmp(b).then(i.cmp(j)));

    // The grouped rows: the window's bounds in place of its call, then the
    // keys and the aggregates, as the aggregate names them.
    let mut fields = vec![
        (None, Arc::new(Field::new(START, time_type.clone(), true))),
        (None, Arc::new(Field::new(END, time_type.clone(), true))),
    ];
    for (i, _) in &keys {
        let (q, f) = agg.schema.qualified_field(*i);
        fields.push((q.cloned(), f.clone()));
    }
    for (j, _, _) in &aggregates {
        let (q, f) = agg.schema.qualified_field(agg.group_expr.len() + j);
        fields.push((q.cloned(), f.clone()));
    }
    let grouped = DFSchema::new_with_metadata(fields, HashMap::new()).map_err(|e| {
        refused(
            at.statement,
            format!("a group window's keys and aggregates need names of their own: {e}"),
        )
    })?;
    let call_column = {
        let (q, f) = agg.schema.qualified_field(window_index);
        datafusion_common::Column::new(q.cloned(), f.name())
    };
    let steps = after(&above, &grouped, &call_column, at)?;

    // Its key: window_start and each group key, through the steps.
    let mut key = Vec::with_capacity(1 + keys.len());
    let mut passes = Vec::with_capacity(keys.len());
    match through(START, &steps) {
        Some(name) => key.push(name),
        None => {
            return Err(refused(
                at.statement,
                "a group window's output keeps window_start, which with its group keys is its primary key: select window_start",
            ));
        }
    }
    for (i, stream) in &written {
        let name = agg.schema.field(*i).name();
        match through(name, &steps) {
            Some(out) => {
                passes.push((stream.clone(), out.clone()));
                key.push(out);
            }
            None => {
                return Err(refused(
                    at.statement,
                    format!(
                        "a group window's output keeps its group keys, which with window_start are its primary key: select {name}"
                    ),
                ));
            }
        }
    }
    let keys: Vec<GroupKey> = keys.into_iter().map(|(_, k)| k).collect();
    let need = keys.iter().map(|k| k.stream.clone()).collect();
    let (aggregates, computes): (Vec<Aggregate>, Vec<String>) =
        aggregates.into_iter().map(|(_, a, c)| (a, c)).unzip();
    let event_time = stream.event_time.clone().unwrap_or_default();
    let before = canon
        .rows(&agg.input)
        .map_err(|e| internal(at.statement, &format!("{e} before the window")))?;
    let id = operator_id(&Kept {
        output,
        input: &input,
        window: &window,
        time: (&event_time, &time_type),
        keys: &keys
            .iter()
            .map(|k| (k.stream.as_str(), schema.field(k.column).data_type()))
            .collect::<Vec<_>>(),
        aggregates: &aggregates,
        computes: &computes,
        before: &before,
    });
    Ok(Lowered {
        operator: GroupWindow {
            input: rows.operator,
            rows: schema.inner().clone(),
            window,
            time: time_index,
            event_time,
            keys,
            aggregates,
            computes,
            grouped: grouped.inner().clone(),
            steps,
            need,
            passes,
            id,
        },
        schema: plan.schema().inner().clone(),
        key,
    })
}

/// The window's call among the aggregate's group expressions, and its
/// place among them.
fn window_call<'a>(
    agg: &'a DfAggregate,
    at: &At,
) -> Result<(usize, &'a datafusion_expr::expr::ScalarFunction), Error> {
    agg.group_expr
        .iter()
        .enumerate()
        .find_map(|(i, e)| match e {
            DfExpr::ScalarFunction(f) if WINDOWS.contains(&f.func.name()) => Some((i, f)),
            _ => None,
        })
        .ok_or_else(|| internal(at.window, "a group window without its window"))
}

/// The window's time: the stream's event-time column, passed through.
fn time_column(
    call: &datafusion_expr::expr::ScalarFunction,
    rows: &RowWise,
    stream: &crate::Stream,
    input: &str,
    at: &At,
) -> Result<datafusion_common::Column, Error> {
    let name = call.func.name();
    let Some(event_time) = &stream.event_time else {
        return Err(refused(
            at.window,
            format!(
                "{input} has no event time, which a group window goes by: set eventTime on the stream in source.streams"
            ),
        ));
    };
    let wrong = || {
        refused(
            at.window,
            format!(
                "{name} goes by the event time of {input}, {event_time}: write {name}({}, …)",
                crate::ident(event_time)
            ),
        )
    };
    match call.args.first() {
        Some(DfExpr::Column(c)) if rows.origin(&c.name).as_deref() == Some(event_time) => {
            Ok(c.clone())
        }
        _ => Err(wrong()),
    }
}

/// The window's sizes in its time's unit.
fn sizes(
    call: &datafusion_expr::expr::ScalarFunction,
    time: &DataType,
    at: &At,
) -> Result<Window, Error> {
    let name = call.func.name();
    let per_micro: i64 = match time {
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1000,
        other => {
            return Err(refused(
                at.window,
                format!("{name} goes by a timestamp, not {other}"),
            ));
        }
    };
    let mut sizes = Vec::with_capacity(2);
    for a in &call.args[1..] {
        let DfExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(v)), _) = a else {
            return Err(refused(
                at.window,
                format!("{name}'s sizes are constant INTERVALs, such as INTERVAL '1 minute'"),
            ));
        };
        if v.months != 0 {
            return Err(refused(
                at.window,
                format!(
                    "{name}'s sizes are in days, hours, minutes or seconds, as a month's length varies"
                ),
            ));
        }
        if v.nanoseconds % 1000 != 0 {
            return Err(refused(
                at.window,
                format!("{name}'s sizes are whole microseconds"),
            ));
        }
        let micros = i64::from(v.days)
            .checked_mul(86_400_000_000)
            .and_then(|d| d.checked_add(v.nanoseconds / 1000))
            .and_then(|m| m.checked_mul(per_micro));
        match micros {
            Some(m) if m > 0 => sizes.push(m),
            _ => {
                return Err(refused(
                    at.window,
                    format!("{name}'s sizes are more than zero, and under 290,000 years"),
                ));
            }
        }
    }
    Ok(match (name, sizes.as_slice()) {
        ("tumble", [size]) => Window::Tumble { size: *size },
        ("hop", [slide, size]) => {
            if slide > size {
                return Err(refused(
                    at.window,
                    "hop's slide is at most its size, so every row is in a window: hop(time, slide, size)",
                ));
            }
            if size / slide + i64::from(size % slide != 0) > MAX_HOPS {
                return Err(refused(
                    at.window,
                    format!(
                        "hop puts each row in at most {MAX_HOPS} windows: its size over its slide is more"
                    ),
                ));
            }
            Window::Hop {
                slide: *slide,
                size: *size,
            }
        }
        ("session", [gap]) => Window::Session { gap: *gap },
        _ => return Err(internal(at.window, "a window with other sizes")),
    })
}

/// An aggregate of the grouped rows.
fn aggregate(
    e: &DfExpr,
    rows: &DFSchema,
    data_type: DataType,
    at: &At,
) -> Result<Aggregate, Error> {
    let inner = e.clone().unalias_nested().data;
    let DfExpr::AggregateFunction(f) = &inner else {
        return Err(internal(at.statement, &format!("an aggregate {e}")));
    };
    let name = f.func.name().to_owned();
    let p = &f.params;
    let compile = |e: &DfExpr| Expr::compile(e, rows, at.statement);
    if p.distinct && !matches!(name.as_str(), "min" | "max") {
        return Err(refused(
            at.statement,
            format!(
                "{name}(DISTINCT …) isn't supported: approx_distinct(x) estimates how many distinct values x has"
            ),
        ));
    }
    if !p.order_by.is_empty() && name != "percentile_cont" {
        return Err(refused(at.statement, format!("{name} takes no ORDER BY")));
    }
    let filter = p.filter.as_deref().map(compile).transpose()?;
    let one_arg = || match p.args.as_slice() {
        [a] => compile(a),
        _ => Err(internal(
            at.statement,
            &format!("{name} with other than one argument"),
        )),
    };
    let check = |arg: &Expr, ok: fn(&DataType) -> bool, takes: &str| match ok(arg.data_type()) {
        true => Ok(()),
        false => Err(refused(
            at.statement,
            format!("{name} takes {takes}, not {}", arg.data_type()),
        )),
    };
    let numbers = |t: &DataType| {
        matches!(
            t,
            DataType::Int64 | DataType::Float64 | DataType::Decimal128(..)
        )
    };
    let (kind, arg) = match name.as_str() {
        "count" => {
            let arg = match p.args.as_slice() {
                [] => None,
                [DfExpr::Literal(v, _)] if !v.is_null() => None,
                #[allow(deprecated)]
                [DfExpr::Wildcard { .. }] => None,
                [a] => Some(compile(a)?),
                _ => return Err(internal(at.statement, "count with several arguments")),
            };
            (AggregateKind::Count, arg)
        }
        "sum" => {
            let a = one_arg()?;
            check(&a, numbers, "numbers")?;
            (AggregateKind::Sum, Some(a))
        }
        "avg" => {
            let a = one_arg()?;
            check(
                &a,
                |t| matches!(t, DataType::Float64 | DataType::Decimal128(..)),
                "numbers",
            )?;
            (AggregateKind::Avg, Some(a))
        }
        "min" | "max" => {
            let a = one_arg()?;
            check(
                &a,
                groupable,
                "a number, text, binary, a boolean, a date, a time or a timestamp",
            )?;
            let kind = match name.as_str() {
                "min" => AggregateKind::Min,
                _ => AggregateKind::Max,
            };
            (kind, Some(a))
        }
        "approx_distinct" => {
            let a = one_arg()?;
            check(
                &a,
                groupable,
                "a number, text, binary, a boolean, a date, a time or a timestamp",
            )?;
            (AggregateKind::ApproxDistinct, Some(a))
        }
        "median" => {
            let a = one_arg()?;
            check(
                &a,
                |t| {
                    matches!(
                        t,
                        DataType::Float32 | DataType::Float64 | DataType::Decimal128(..)
                    )
                },
                "numbers",
            )?;
            (AggregateKind::Percentile(0.5), Some(a))
        }
        "percentile_cont" => {
            let [value, fraction] = p.args.as_slice() else {
                return Err(refused(
                    at.statement,
                    "percentile_cont takes a fraction: percentile_cont(0.9) WITHIN GROUP (ORDER BY x)",
                ));
            };
            let fraction = match fraction {
                DfExpr::Literal(ScalarValue::Float64(Some(f)), _) if (0.0..=1.0).contains(f) => *f,
                other => {
                    return Err(refused(
                        at.statement,
                        format!(
                            "percentile_cont's fraction is a constant from 0 to 1, not {other}"
                        ),
                    ));
                }
            };
            let descending = p.order_by.first().is_some_and(|s| !s.asc);
            let a = compile(value)?;
            check(&a, |t| matches!(t, DataType::Float64), "numbers")?;
            let fraction = if descending { 1.0 - fraction } else { fraction };
            (AggregateKind::Percentile(fraction), Some(a))
        }
        other => {
            return Err(refused(
                at.statement,
                format!("{other} isn't an aggregate transforms run: a group window takes {NAMED}"),
            ));
        }
    };
    Ok(Aggregate {
        kind,
        arg,
        filter,
        data_type,
    })
}

/// The steps over the grouped rows, from the bottom up: HAVING's filter,
/// and the projections. Each reads the window's bounds where binding gave
/// a field of its call.
fn after(
    above: &[&LogicalPlan],
    grouped: &DFSchema,
    call: &datafusion_common::Column,
    at: &At,
) -> Result<Vec<Step>, Error> {
    let mut steps = Vec::with_capacity(above.len());
    let mut input = grouped.clone();
    for node in above.iter().rev() {
        match node {
            LogicalPlan::Filter(f) => {
                let predicate = bounds_read(&f.predicate, call, at)?;
                steps.push(Step::Filter(Expr::compile(
                    &predicate,
                    &input,
                    at.statement,
                )?));
            }
            LogicalPlan::Projection(p) => {
                let mut columns = Vec::with_capacity(p.expr.len());
                for (e, f) in p.expr.iter().zip(p.schema.fields()) {
                    if matches!(f.data_type(), DataType::Struct(_)) && reads(e, call) {
                        return Err(refused(
                            (at.column)(f.name()),
                            "select a window's bounds as window_start and window_end, not the window",
                        ));
                    }
                    let e = bounds_read(e, call, at)?;
                    columns.push(Column {
                        name: f.name().clone(),
                        expr: Expr::compile(&e, &input, at.statement)?,
                    });
                }
                steps.push(Step::Project(Projection {
                    columns,
                    schema: p.schema.inner().clone(),
                }));
                input = p.schema.as_ref().clone();
            }
            _ => return Err(internal(at.statement, "a step above the window")),
        }
    }
    Ok(steps)
}

/// Whether `e` reads the column `c`.
fn reads(e: &DfExpr, c: &datafusion_common::Column) -> bool {
    let mut found = false;
    let _ = e.apply(|x| {
        if let DfExpr::Column(x) = x
            && x == c
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// `e` with each field of the window's call read from the grouped rows'
/// bounds. Refuses the call read otherwise.
fn bounds_read(e: &DfExpr, call: &datafusion_common::Column, at: &At) -> Result<DfExpr, Error> {
    let out = e
        .clone()
        .transform_up(|x| {
            if let DfExpr::ScalarFunction(f) = &x
                && f.func.name() == "get_field"
                && let [
                    DfExpr::Column(c),
                    DfExpr::Literal(ScalarValue::Utf8(Some(name)), _),
                ] = f.args.as_slice()
                && c == call
            {
                let to = match name.as_str() {
                    "start" => START,
                    "end" => END,
                    _ => return Ok(Transformed::no(x)),
                };
                return Ok(Transformed::yes(DfExpr::Column(
                    datafusion_common::Column::new_unqualified(to),
                )));
            }
            Ok(Transformed::no(x))
        })
        .map_err(|e| Error::datafusion(ErrorKind::Internal, at.statement, &e))?
        .data;
    if reads(&out, call) {
        return Err(refused(
            at.statement,
            "a window is read by its bounds, window_start and window_end",
        ));
    }
    Ok(out)
}

/// The name a column of the grouped rows has once it passes the steps
/// unchanged; `None` if it doesn't.
fn through(name: &str, steps: &[Step]) -> Option<String> {
    let mut name = name.to_owned();
    for s in steps {
        match s {
            Step::Filter(_) => {}
            Step::Unnest(_) => return None,
            Step::Project(p) => {
                name = p
                    .columns
                    .iter()
                    .find(|c| c.expr.column() == Some(name.as_str()))?
                    .name
                    .clone();
            }
        }
    }
    Some(name)
}

/// What a group window computes and keeps, by the stream's names.
struct Kept<'a> {
    output: &'a str,
    input: &'a str,
    window: &'a Window,
    /// The event-time column, and its type.
    time: (&'a str, &'a DataType),
    /// The keys, and their types, in the operator's order.
    keys: &'a [(&'a str, &'a DataType)],
    aggregates: &'a [Aggregate],
    /// Each aggregate, over the stream's columns.
    computes: &'a [String],
    /// What the steps before the window do to the rows: filters and
    /// unnests.
    before: &'a [String],
}

/// A hash of what the operator computes and keeps: the output it feeds, the
/// stream it reads, its window, its time and keys with their types, its
/// aggregates over the stream's columns with their types, the filters and
/// unnests before it, and its state's layout. A projection, a name, a
/// view, the order of GROUP BY or of the aggregates, and HAVING or anything
/// else after the window don't enter it.
fn operator_id(k: &Kept) -> String {
    use crate::canon::type_text;
    let window = match k.window {
        Window::Tumble { size } => format!("tumble size {size}"),
        Window::Hop { slide, size } => format!("hop slide {slide} size {size}"),
        Window::Session { gap } => format!("session gap {gap}"),
    };
    let mut d = format!(
        "group_window {STATE}\noutput {}\ninput {}\nwindow {window}\ntime {} {}\n",
        crate::ident(k.output),
        crate::ident(k.input),
        crate::ident(k.time.0),
        type_text(k.time.1)
    );
    for (name, t) in k.keys {
        d.push_str(&format!("key {} {}\n", crate::ident(name), type_text(t)));
    }
    for (a, c) in k.aggregates.iter().zip(k.computes) {
        let arg = a
            .arg
            .as_ref()
            .map(|e| type_text(e.data_type()))
            .unwrap_or_default();
        d.push_str(&format!(
            "aggregate {c} of {arg} as {}\n",
            type_text(&a.data_type)
        ));
    }
    // Quoted, so no entry's text can read as two.
    for b in k.before {
        d.push_str(&format!("{b:?}\n"));
    }
    format!("window-{:016x}", XxHash3_64::oneshot(d.as_bytes()))
}

/// An aggregate as the stream's columns compute it, such as `sum(amount)`
/// or `count(*) FILTER (WHERE (amount > …))`: `e` is over `input`'s
/// output.
fn computes(
    canon: &mut crate::canon::Canon,
    e: &DfExpr,
    a: &Aggregate,
    input: &LogicalPlan,
    at: &At,
) -> Result<String, Error> {
    let inner = e.clone().unalias_nested().data;
    let DfExpr::AggregateFunction(f) = &inner else {
        return Err(internal(at.statement, &format!("an aggregate {e}")));
    };
    let mut over = |e: &DfExpr| {
        canon
            .over(e, input)
            .map_err(|m| internal(at.statement, &format!("{m} under an aggregate")))
    };
    let arg = match (&a.arg, f.params.args.first()) {
        (Some(_), Some(x)) => over(x)?,
        _ => "*".to_owned(),
    };
    let mut out = match a.kind {
        AggregateKind::Percentile(p) => format!("percentile_cont({p}, {arg})"),
        k => format!("{}({arg})", k.name()),
    };
    if let Some(filter) = &f.params.filter {
        out.push_str(&format!(" FILTER (WHERE {})", over(filter)?));
    }
    Ok(out)
}

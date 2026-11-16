//! `OVER`: aggregates, `lag` and `first_value` over a frame of each key's
//! rows that ends at the current row, in event-time order, and dedupe by
//! `ROW_NUMBER() … = 1`; the window functions binding types, and the
//! lowering of a bound query that computes them.
//!
//! A window function is never evaluated here: binding types it, and
//! lowering takes its arguments, keys, order and frame off it. The engine's
//! operators run them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, SchemaRef, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, ScalarValue, TableReference, exec_err, plan_err};
use datafusion_expr::expr::{NullTreatment, WindowFunction};
use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion_expr::logical_plan::Window as DfWindow;
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr as DfExpr, LogicalPlan, Operator as DfOperator, PartitionEvaluator, Signature, Volatility,
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowUDF, WindowUDFImpl, lit,
};
use twox_hash::XxHash3_64;

use crate::plan::{
    Column, Dedupe, Frame, GroupKey, Keep, Operator, Over, OverFunction, OverKind, Projection,
    RowWise, Step,
};
use crate::{Catalog, Class, Error, ErrorKind, Expr, Position};

/// The functions `OVER` takes, by name and by alias.
pub(crate) const FUNCTIONS: &[&str] = &[
    "avg",
    "count",
    "first_value",
    "lag",
    "max",
    "mean",
    "min",
    "row_number",
    "sum",
];

/// The functions `OVER` takes, for messages.
pub(crate) const NAMED: &str =
    "sum, count, min, max, avg, lag and first_value, and ROW_NUMBER() … = 1 to deduplicate";

/// Why OVER over what another OVER computed is refused.
pub(crate) const NESTED: &str = "OVER over the values of another OVER isn't supported: compute a statement's OVER over the stream's rows";

/// The layout of an `OVER` operator's state, which its ID covers, and which
/// the engine writes it in.
pub const OVER_STATE: u8 = 1;

/// The layout of a dedupe's state, which its ID covers, and which the
/// engine writes it in.
pub const DEDUPE_STATE: u8 = 1;

/// `row_number`, `lag` and `first_value`, as binding types them. The
/// aggregates are the group windows'.
pub(crate) fn functions() -> Vec<Arc<WindowUDF>> {
    [
        ("row_number", Signature::nullary(Volatility::Immutable)),
        ("lag", Signature::user_defined(Volatility::Immutable)),
        ("first_value", Signature::any(1, Volatility::Immutable)),
    ]
    .into_iter()
    .map(|(name, signature)| Arc::new(WindowUDF::new_from_impl(OverFn { name, signature })))
    .collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OverFn {
    name: &'static str,
    signature: Signature,
}

impl WindowUDFImpl for OverFn {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// `lag(value, offset, default)`: the offset a BIGINT, and the default
    /// of the value's type.
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [value] => Ok(vec![value.clone()]),
            [value, _] => Ok(vec![value.clone(), DataType::Int64]),
            [value, _, _] => Ok(vec![value.clone(), DataType::Int64, value.clone()]),
            _ => {
                plan_err!("lag takes lag(value), lag(value, offset) or lag(value, offset, default)")
            }
        }
    }

    fn partition_evaluator(
        &self,
        _: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        exec_err!("{} runs in the engine's operator", self.name)
    }

    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        if self.name == "row_number" {
            return Ok(Arc::new(Field::new(args.name(), DataType::UInt64, false)));
        }
        match args.input_fields().first() {
            Some(f) => Ok(Arc::new(
                f.as_ref()
                    .clone()
                    .with_name(args.name())
                    .with_nullable(true),
            )),
            None => plan_err!("{} takes a value", self.name),
        }
    }
}

/// Where `OVER`'s errors are.
pub(crate) struct At {
    /// The statement's INSERT.
    pub(crate) statement: Position,
    /// Its first OVER.
    pub(crate) over: Position,
}

/// A statement that computes `OVER`, lowered.
pub(crate) struct Lowered {
    pub(crate) operator: Operator,
    pub(crate) schema: SchemaRef,
    pub(crate) key: Vec<String>,
}

fn refused(at: Position, message: impl Into<String>) -> Error {
    Error::at(ErrorKind::Unsupported, at, message)
}

fn internal(at: Position, what: &str) -> Error {
    Error::at(ErrorKind::Internal, at, format!("planning gave {what}"))
}

fn only_input<'a>(node: &'a LogicalPlan, at: &At) -> Result<&'a LogicalPlan, Error> {
    match node.inputs().as_slice() {
        [input] => Ok(*input),
        _ => Err(internal(at.statement, "a step with other than one input")),
    }
}

/// Lowers a bound statement whose window functions are computed `OVER`: the
/// row-wise steps that give the rows, the operator, then the steps after
/// it. `None` where binding left no window function, as where nothing
/// reads one.
pub(crate) fn lower(
    plan: &LogicalPlan,
    catalog: &Catalog,
    at: &At,
    output: &str,
) -> Result<Option<Lowered>, Error> {
    let windowed = plan
        .exists(|n| Ok(matches!(n, LogicalPlan::Window(_))))
        .map_err(|e| Error::datafusion(ErrorKind::Internal, at.statement, &e))?;
    if !windowed {
        return Ok(None);
    }
    // From the top down to the windows.
    let mut above = Vec::new();
    let mut node = plan;
    loop {
        match node {
            LogicalPlan::Window(_) => break,
            LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
                above.push(node)
            }
            LogicalPlan::Unnest(_) => {
                return Err(refused(
                    at.statement,
                    "UNNEST of OVER's rows isn't supported: unnest the stream's rows before OVER, in a view",
                ));
            }
            other => return Err(crate::lower::refusal(other, at.statement)),
        }
        node = only_input(node, at)?;
    }
    // A window for each PARTITION BY and ORDER BY, one on another, with
    // projections between them that only pass columns.
    let mut windows: Vec<&DfWindow> = Vec::new();
    loop {
        match node {
            LogicalPlan::Window(w) => {
                windows.push(w);
                node = &w.input;
            }
            LogicalPlan::Projection(p)
                if matches!(p.input.as_ref(), LogicalPlan::Window(_))
                    && p.expr.iter().all(|e| matches!(e, DfExpr::Column(_))) =>
            {
                node = &p.input
            }
            _ => break,
        }
    }
    let below = node;
    let rows = crate::lower::row_wise(below, catalog, at.statement)?;
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
            at.over,
            format!(
                "{input} is a changelog, and OVER reads appended rows: transforms don't run updating statements yet (tier 5)"
            ),
        ));
    }
    let Some(event_time) = stream.event_time.clone() else {
        return Err(refused(
            at.over,
            format!(
                "{input} has no event time, which OVER goes by: set eventTime on the stream in source.streams"
            ),
        ));
    };
    let schema = below.schema();

    // Each window function, with its column of the windows' output.
    let mut calls = Vec::new();
    for w in &windows {
        let n = w.input.schema().fields().len();
        for (j, e) in w.window_expr.iter().enumerate() {
            let (q, f) = w.schema.qualified_field(n + j);
            let DfExpr::WindowFunction(call) = e.clone().unalias_nested().data else {
                return Err(internal(at.over, &format!("a window expression {e}")));
            };
            if e.column_refs().iter().any(|c| !schema.has_column(c)) {
                return Err(refused(at.statement, NESTED));
            }
            calls.push((call, q.cloned(), f.clone()));
        }
    }
    let mut spec: Option<Spec> = None;
    for (call, ..) in &calls {
        let s = spec_of(call, schema, &rows.operator, &event_time, &input, at)?;
        match &spec {
            None => spec = Some(s),
            Some(first) if first.same(&s) => {}
            Some(_) => {
                return Err(refused(
                    at.over,
                    "a statement's OVER clauses have one PARTITION BY and ORDER BY, by which one operator keeps their state: compute the others in a statement of their own",
                ));
            }
        }
    }
    let Some(spec) = spec else {
        return Err(internal(at.over, "windows without functions"));
    };
    let time_type = schema.field(spec.time).data_type().clone();
    let per_micro: i64 = match &time_type {
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1000,
        other => {
            return Err(refused(
                at.over,
                format!("OVER goes by a timestamp, not {other}"),
            ));
        }
    };
    let mut keys: Vec<GroupKey> = spec
        .written
        .iter()
        .map(|(column, stream)| GroupKey {
            column: *column,
            stream: stream.clone(),
        })
        .collect();
    keys.sort_by(|a, b| a.stream.cmp(&b.stream));
    let need: Vec<String> = keys.iter().map(|k| k.stream.clone()).collect();
    let key_types: Vec<(&str, &DataType)> = keys
        .iter()
        .map(|k| (k.stream.as_str(), schema.field(k.column).data_type()))
        .collect();
    let mut canon = crate::canon::Canon::default();
    let before = canon
        .rows(below)
        .map_err(|e| internal(at.statement, &format!("{e} before OVER")))?;
    let kept = Kept {
        output,
        input: &input,
        time: (&event_time, &time_type),
        keys: &key_types,
        before: &before,
    };

    if calls.iter().any(|(c, ..)| c.fun.name() == "row_number") {
        let [(call, q, f)] = calls.as_slice() else {
            return Err(refused(
                at.over,
                "ROW_NUMBER() … = 1 deduplicates on its own: compute other functions OVER in a statement of their own",
            ));
        };
        let p = &call.params;
        if !p.args.is_empty() || p.filter.is_some() || p.distinct {
            return Err(refused(at.over, "ROW_NUMBER() takes nothing but OVER (…)"));
        }
        if spec.written.is_empty() {
            return Err(refused(
                at.over,
                "ROW_NUMBER() … = 1 keeps a row for each key of its PARTITION BY: partition by the columns that identify a row",
            ));
        }
        let top = windows.first().map_or(schema, |w| &w.schema);
        let number = top
            .index_of_column(&datafusion_common::Column::new(q.clone(), f.name()))
            .map_err(|e| Error::datafusion(ErrorKind::Internal, at.over, &e))?;
        let (steps, first) = after(
            &above,
            schema.as_ref().clone(),
            Some(HashSet::from([number])),
            at,
        )?;
        if !first {
            return Err(refused(
                at.over,
                "ROW_NUMBER() deduplicates, keeping the rows where it's 1: compare it = 1 in QUALIFY, or in the WHERE of a SELECT that reads it; count(*) OVER (… ROWS UNBOUNDED PRECEDING) numbers every row",
            ));
        }
        let keep = match spec.descending {
            true => Keep::Latest,
            false => Keep::First,
        };
        let mut key = Vec::with_capacity(spec.written.len());
        for (column, _) in &spec.written {
            let name = schema.field(*column).name();
            match crate::window::through(name, &steps) {
                Some(out) => key.push(out),
                None => {
                    return Err(refused(
                        at.statement,
                        format!(
                            "a deduplicated output keeps its PARTITION BY columns, which are its primary key: select {name}"
                        ),
                    ));
                }
            }
        }
        let id = dedupe_id(&kept, keep);
        return Ok(Some(Lowered {
            operator: Operator::Dedupe(Box::new(Dedupe {
                passes: passes(&rows.operator, &steps),
                input: rows.operator,
                rows: schema.inner().clone(),
                time: spec.time,
                event_time,
                keys,
                keep,
                steps,
                need,
                id,
                at: at.over,
            })),
            schema: plan.schema().inner().clone(),
            key,
        }));
    }

    if spec.descending {
        return Err(refused(
            at.over,
            format!(
                "OVER's frames end at the current row in event-time order: write ORDER BY {} without DESC",
                crate::ident(&event_time)
            ),
        ));
    }
    let mut functions = Vec::with_capacity(calls.len());
    for (j, (call, q, f)) in calls.iter().enumerate() {
        let function = function(call, schema, f.data_type().clone(), per_micro, at)?;
        let computes = computes(&mut canon, call, &function, below, at)?;
        functions.push((j, function, computes, q.clone(), f.clone()));
    }
    // The operator takes its keys by the stream's names, and its functions
    // by what they compute, so writing either in another order keeps its
    // state and its ID.
    functions.sort_by(|(i, _, a, ..), (j, _, b, ..)| a.cmp(b).then(i.cmp(j)));
    let mut fields: Vec<(Option<TableReference>, FieldRef)> = schema
        .iter()
        .map(|(q, f)| (q.cloned(), f.clone()))
        .collect();
    for (_, _, _, q, f) in &functions {
        fields.push((q.clone(), f.clone()));
    }
    let framed = DFSchema::new_with_metadata(fields, HashMap::new()).map_err(|e| {
        refused(
            at.statement,
            format!("OVER's values need names of their own: {e}"),
        )
    })?;
    let (steps, _) = after(&above, framed.clone(), None, at)?;
    let (functions, computes): (Vec<OverFunction>, Vec<String>) =
        functions.into_iter().map(|(_, f, c, ..)| (f, c)).unzip();
    let id = over_id(&kept, &functions, &computes);
    Ok(Some(Lowered {
        key: crate::lower::follow(&rows.key, &steps),
        operator: Operator::Over(Box::new(Over {
            passes: passes(&rows.operator, &steps),
            input: rows.operator,
            rows: schema.inner().clone(),
            time: spec.time,
            event_time,
            keys,
            functions,
            computes,
            framed: framed.inner().clone(),
            steps,
            need,
            id,
            at: at.over,
        })),
        schema: plan.schema().inner().clone(),
    }))
}

/// What a window function's `OVER` says: its keys and order.
struct Spec {
    /// Each column of the rows that `PARTITION BY` names, as written, once,
    /// with the stream's column it is.
    written: Vec<(usize, String)>,
    /// The column of the rows with the event time.
    time: usize,
    descending: bool,
}

impl Spec {
    /// Whether one operator can run both: the same keys, in any order, and
    /// the same order.
    fn same(&self, other: &Spec) -> bool {
        let streams = |s: &Spec| {
            let mut v: Vec<String> = s.written.iter().map(|(_, s)| s.clone()).collect();
            v.sort();
            v
        };
        streams(self) == streams(other)
            && self.time == other.time
            && self.descending == other.descending
    }
}

fn spec_of(
    call: &WindowFunction,
    rows: &DFSchema,
    steps: &RowWise,
    event_time: &str,
    input: &str,
    at: &At,
) -> Result<Spec, Error> {
    let p = &call.params;
    let order = || {
        refused(
            at.over,
            format!(
                "OVER goes by the event time of {input}, {event_time}: write ORDER BY {}",
                crate::ident(event_time)
            ),
        )
    };
    let [sort] = p.order_by.as_slice() else {
        return Err(match p.order_by.len() {
            0 => order(),
            _ => refused(
                at.over,
                format!(
                    "OVER orders by the event time alone, which ties don't change: write ORDER BY {}",
                    crate::ident(event_time)
                ),
            ),
        });
    };
    let DfExpr::Column(c) = &sort.expr else {
        return Err(order());
    };
    if steps.origin(&c.name).as_deref() != Some(event_time) {
        return Err(order());
    }
    let time = rows
        .index_of_column(c)
        .map_err(|e| Error::datafusion(ErrorKind::Bind, at.over, &e))?;
    let mut written: Vec<(usize, String)> = Vec::with_capacity(p.partition_by.len());
    for e in &p.partition_by {
        let computed = |what: &dyn std::fmt::Display| {
            refused(
                at.over,
                format!(
                    "OVER partitions by columns of {input}, renamed or not: {what} is computed"
                ),
            )
        };
        let DfExpr::Column(c) = e else {
            return Err(computed(e));
        };
        let column = rows
            .index_of_column(c)
            .map_err(|e| Error::datafusion(ErrorKind::Bind, at.over, &e))?;
        let Some(from) = steps.origin(&c.name) else {
            return Err(computed(&c.name));
        };
        let t = rows.field(column).data_type();
        if !crate::window::groupable(t) {
            return Err(refused(
                at.over,
                format!("OVER can't partition by {}, of type {t}", c.name),
            ));
        }
        if !written.iter().any(|(_, s)| *s == from) {
            written.push((column, from));
        }
    }
    Ok(Spec {
        written,
        time,
        descending: !sort.asc,
    })
}

/// A function the operator computes, over the rows.
fn function(
    call: &WindowFunction,
    rows: &DFSchema,
    data_type: DataType,
    per_micro: i64,
    at: &At,
) -> Result<OverFunction, Error> {
    let name = call.fun.name().to_owned();
    let p = &call.params;
    let compile = |e: &DfExpr| Expr::compile(e, rows, at.statement);
    if p.distinct && !matches!(name.as_str(), "min" | "max") {
        return Err(refused(
            at.over,
            format!("{name}(DISTINCT …) isn't supported over a frame"),
        ));
    }
    if p.null_treatment == Some(NullTreatment::IgnoreNulls) {
        return Err(refused(
            at.over,
            format!("{name}(…) IGNORE NULLS isn't supported"),
        ));
    }
    let aggregate = matches!(name.as_str(), "count" | "sum" | "min" | "max" | "avg");
    let filter = match &p.filter {
        None => None,
        Some(f) if aggregate => Some(compile(f)?),
        Some(_) => {
            return Err(refused(
                at.over,
                format!("{name} takes no FILTER, which is for aggregates"),
            ));
        }
    };
    let one_arg = || match p.args.as_slice() {
        [a] => compile(a),
        _ => Err(internal(
            at.over,
            &format!("{name} with other than one argument"),
        )),
    };
    let check = |arg: &Expr, ok: fn(&DataType) -> bool, takes: &str| match ok(arg.data_type()) {
        true => Ok(()),
        false => Err(refused(
            at.over,
            format!("{name} takes {takes}, not {}", arg.data_type()),
        )),
    };
    let numbers = |t: &DataType| {
        matches!(
            t,
            DataType::Int64 | DataType::Float64 | DataType::Decimal128(..)
        )
    };
    const KEPT: &str = "a number, text, binary, a boolean, a date, a time or a timestamp";
    let (kind, arg) = match name.as_str() {
        "count" => {
            let arg = match p.args.as_slice() {
                [] => None,
                [DfExpr::Literal(v, _)] if !v.is_null() => None,
                [a] => Some(compile(a)?),
                _ => return Err(internal(at.over, "count with several arguments")),
            };
            (OverKind::Count, arg)
        }
        "sum" => {
            let a = one_arg()?;
            check(&a, numbers, "numbers")?;
            (OverKind::Sum, Some(a))
        }
        "avg" => {
            let a = one_arg()?;
            check(
                &a,
                |t| matches!(t, DataType::Float64 | DataType::Decimal128(..)),
                "numbers",
            )?;
            (OverKind::Avg, Some(a))
        }
        "min" | "max" => {
            let a = one_arg()?;
            check(&a, crate::window::groupable, KEPT)?;
            let kind = match name.as_str() {
                "min" => OverKind::Min,
                _ => OverKind::Max,
            };
            (kind, Some(a))
        }
        "first_value" => {
            let a = one_arg()?;
            check(&a, crate::window::groupable, KEPT)?;
            (OverKind::FirstValue, Some(a))
        }
        "lag" => {
            let (value, offset, default) = match p.args.as_slice() {
                [v] => (v, None, None),
                [v, o] => (v, Some(o), None),
                [v, o, d] => (v, Some(o), Some(d)),
                _ => return Err(internal(at.over, "lag with other arguments")),
            };
            let a = compile(value)?;
            check(&a, crate::window::groupable, KEPT)?;
            let offset = match offset {
                None => 1,
                Some(o) => match constant(o).and_then(|v| v.cast_to(&DataType::Int64).ok()) {
                    Some(ScalarValue::Int64(Some(n))) if n >= 1 => n.unsigned_abs(),
                    _ => {
                        return Err(refused(
                            at.over,
                            "lag's offset is a constant whole number, 1 or more",
                        ));
                    }
                },
            };
            let default = match default {
                None => None,
                Some(d) if constant(d).is_some_and(|v| v.is_null()) => None,
                Some(d) => {
                    let e = compile(d)?;
                    if !e.inputs().is_empty() || !e.is_deterministic() {
                        return Err(refused(
                            at.over,
                            "lag's default is a constant, of the value's type",
                        ));
                    }
                    Some(e)
                }
            };
            (OverKind::Lag { offset, default }, Some(a))
        }
        other => {
            return Err(refused(
                at.over,
                format!("{other} isn't a function OVER takes: OVER takes {NAMED}"),
            ));
        }
    };
    let frame = match kind {
        OverKind::Lag { .. } => None,
        _ => Some(frame(&p.window_frame, per_micro, at)?),
    };
    Ok(OverFunction {
        kind,
        arg,
        filter,
        frame,
        data_type,
    })
}

/// A literal's value, through any casts of it that binding left.
fn constant(e: &DfExpr) -> Option<ScalarValue> {
    match e {
        DfExpr::Literal(v, _) => Some(v.clone()),
        DfExpr::Cast(c) => constant(&c.expr)?.cast_to(c.field.data_type()).ok(),
        DfExpr::Alias(a) => constant(&a.expr),
        _ => None,
    }
}

/// A function's frame, which ends at the current row.
fn frame(f: &WindowFrame, per_micro: i64, at: &At) -> Result<Frame, Error> {
    if f.units == WindowFrameUnits::Groups {
        return Err(refused(
            at.over,
            "GROUPS frames aren't supported: write ROWS or RANGE",
        ));
    }
    if f.end_bound != WindowFrameBound::CurrentRow {
        return Err(refused(
            at.over,
            "OVER's frames end at the current row: write ROWS or RANGE BETWEEN … PRECEDING AND CURRENT ROW",
        ));
    }
    enum Start {
        Unbounded,
        Current,
        Preceding(ScalarValue),
    }
    let start = match &f.start_bound {
        WindowFrameBound::Preceding(v) if v.is_null() => Start::Unbounded,
        WindowFrameBound::Preceding(v) => Start::Preceding(v.clone()),
        WindowFrameBound::CurrentRow => Start::Current,
        WindowFrameBound::Following(_) => {
            return Err(refused(
                at.over,
                "a frame starts before the current row, or at it",
            ));
        }
    };
    Ok(match f.units {
        WindowFrameUnits::Rows => Frame::Rows {
            preceding: match start {
                Start::Unbounded => None,
                Start::Current => Some(0),
                Start::Preceding(ScalarValue::UInt64(Some(n))) => Some(n),
                Start::Preceding(other) => {
                    return Err(internal(at.over, &format!("a ROWS frame of {other}")));
                }
            },
        },
        _ => Frame::Range {
            preceding: match start {
                Start::Unbounded => None,
                Start::Current => Some(0),
                Start::Preceding(v) => Some(range(&v, per_micro, at)?),
            },
        },
    })
}

/// A RANGE frame's offset in the event time's unit.
fn range(v: &ScalarValue, per_micro: i64, at: &At) -> Result<i64, Error> {
    let ScalarValue::IntervalMonthDayNano(Some(v)) = v else {
        return Err(refused(
            at.over,
            "a RANGE frame's offset is an INTERVAL, such as INTERVAL '1 hour'",
        ));
    };
    if v.months != 0 {
        return Err(refused(
            at.over,
            "a RANGE frame's offset is in days, hours, minutes or seconds, as a month's length varies",
        ));
    }
    if v.nanoseconds % 1000 != 0 {
        return Err(refused(
            at.over,
            "a RANGE frame's offset is whole microseconds",
        ));
    }
    let micros = i64::from(v.days)
        .checked_mul(86_400_000_000)
        .and_then(|d| d.checked_add(v.nanoseconds / 1000))
        .and_then(|m| m.checked_mul(per_micro));
    match micros {
        Some(m) if m >= 0 => Ok(m),
        _ => Err(refused(
            at.over,
            "a RANGE frame's offset is zero or more, and under 290,000 years",
        )),
    }
}

/// The steps over the operator's output, from the bottom up: filters and
/// projections, the first over `input`, each one after over what the one
/// before gives. With `number`, the columns of each step's input that hold
/// the row number: they read as 1, and a filter that compares one = 1 is
/// dropped, as the operator keeps only those rows; it says whether there
/// was one. A filter that compares it otherwise is refused.
fn after(
    above: &[&LogicalPlan],
    mut input: DFSchema,
    mut number: Option<HashSet<usize>>,
    at: &At,
) -> Result<(Vec<Step>, bool), Error> {
    let mut steps = Vec::with_capacity(above.len());
    let mut first = false;
    for node in above.iter().rev() {
        let below = only_input(node, at)?.schema();
        match node {
            LogicalPlan::Filter(f) => {
                let mut kept = Vec::new();
                for c in split_conjunction(&f.predicate) {
                    match &number {
                        Some(n) if reads(c, below, n) => match is_first(c, below, n) {
                            true => first = true,
                            false => {
                                return Err(Error::later_tier(
                                    Class::Updating,
                                    at.over,
                                    "ROW_NUMBER() compared other than = 1, as in a top-N",
                                ));
                            }
                        },
                        _ => kept.push(c.clone()),
                    }
                }
                if let Some(p) = conjunction(kept) {
                    steps.push(Step::Filter(Expr::compile(&p, &input, at.statement)?));
                }
            }
            LogicalPlan::Projection(p) => {
                let mut columns = Vec::with_capacity(p.expr.len());
                let mut next = HashSet::new();
                for (i, (e, f)) in p.expr.iter().zip(p.schema.fields()).enumerate() {
                    let e = match &number {
                        Some(n) => {
                            if let DfExpr::Column(c) = e.clone().unalias_nested().data
                                && below.index_of_column(&c).is_ok_and(|j| n.contains(&j))
                            {
                                next.insert(i);
                            }
                            one(e, below, n, at)?
                        }
                        None => e.clone(),
                    };
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
                if let Some(n) = &mut number {
                    *n = next;
                }
            }
            LogicalPlan::SubqueryAlias(a) => input = input.replace_qualifier(a.alias.clone()),
            _ => return Err(internal(at.statement, "a step above OVER")),
        }
    }
    Ok((steps, first))
}

/// Whether `e`, over `schema`, reads one of the columns `of`.
fn reads(e: &DfExpr, schema: &DFSchema, of: &HashSet<usize>) -> bool {
    e.column_refs()
        .into_iter()
        .any(|c| schema.index_of_column(c).is_ok_and(|i| of.contains(&i)))
}

/// Whether `e` is `n = 1` for a column `n` of the columns `of`: `n`, or a
/// cast of it that gives each number a value of its own, equals the value
/// that 1 has there.
fn is_first(e: &DfExpr, schema: &DFSchema, of: &HashSet<usize>) -> bool {
    let DfExpr::BinaryExpr(b) = e else {
        return false;
    };
    if b.op != DfOperator::Eq {
        return false;
    }
    let first = |n: &DfExpr, value: &DfExpr| {
        let (Some(n), Some(value)) = (at_one(n, schema, of), constant(value)) else {
            return false;
        };
        n == value
    };
    first(&b.left, &b.right) || first(&b.right, &b.left)
}

/// What `e` gives where the row number, one of the columns `of`, is 1:
/// through casts that keep numbers apart, to numbers or their text; `None`
/// for anything else.
fn at_one(e: &DfExpr, schema: &DFSchema, of: &HashSet<usize>) -> Option<ScalarValue> {
    match e {
        DfExpr::Column(c) => schema
            .index_of_column(c)
            .is_ok_and(|i| of.contains(&i))
            .then_some(ScalarValue::UInt64(Some(1))),
        DfExpr::Cast(c) => apart(c.field.data_type(), at_one(&c.expr, schema, of)?),
        DfExpr::TryCast(c) => apart(c.field.data_type(), at_one(&c.expr, schema, of)?),
        _ => None,
    }
}

/// `v` cast to `t`, where the cast gives each whole number a value of its
/// own.
fn apart(t: &DataType, v: ScalarValue) -> Option<ScalarValue> {
    let keeps = t.is_integer()
        || t.is_floating()
        || matches!(
            t,
            DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
        );
    match keeps {
        true => v.cast_to(t).ok(),
        false => None,
    }
}

/// `e` with each of the columns `of` read as 1.
fn one(e: &DfExpr, schema: &DFSchema, of: &HashSet<usize>, at: &At) -> Result<DfExpr, Error> {
    e.clone()
        .transform_up(|x| {
            if let DfExpr::Column(c) = &x
                && schema.index_of_column(c).is_ok_and(|i| of.contains(&i))
            {
                return Ok(Transformed::new(lit(1u64), true, TreeNodeRecursion::Jump));
            }
            Ok(Transformed::no(x))
        })
        .map(|t| t.data)
        .map_err(|e| Error::datafusion(ErrorKind::Internal, at.statement, &e))
}

/// The stream's columns that reach the output unchanged, with their names
/// there: those that pass the steps before the operator, the operator, and
/// the steps after it.
fn passes(rows: &RowWise, steps: &[Step]) -> Vec<(String, String)> {
    rows.passes()
        .into_iter()
        .filter_map(|(from, name)| crate::window::through(&name, steps).map(|out| (from, out)))
        .collect()
}

/// A function as the stream's columns compute it, with its frame, such as
/// `sum(amount) rows unbounded` or `lag(amount, 1)`: `call` is over
/// `below`'s output.
fn computes(
    canon: &mut crate::canon::Canon,
    call: &WindowFunction,
    f: &OverFunction,
    below: &LogicalPlan,
    at: &At,
) -> Result<String, Error> {
    let p = &call.params;
    let mut over = |e: &DfExpr| {
        canon
            .over(e, below)
            .map_err(|m| internal(at.over, &format!("{m} under OVER")))
    };
    let arg = match (&f.arg, p.args.first()) {
        (Some(_), Some(x)) => over(x)?,
        _ => "*".to_owned(),
    };
    let mut out = match &f.kind {
        OverKind::Lag {
            offset,
            default: Some(_),
        } => match p.args.get(2) {
            Some(d) => format!("lag({arg}, {offset}, {})", over(d)?),
            None => return Err(internal(at.over, "lag's default without its argument")),
        },
        OverKind::Lag { offset, .. } => format!("lag({arg}, {offset})"),
        k => format!("{}({arg})", k.name()),
    };
    if let Some(filter) = &p.filter {
        out.push_str(&format!(" FILTER (WHERE {})", over(filter)?));
    }
    match f.frame {
        Some(Frame::Rows { preceding: None }) => out.push_str(" rows unbounded"),
        Some(Frame::Rows { preceding: Some(n) }) => out.push_str(&format!(" rows {n}")),
        Some(Frame::Range { preceding: None }) => out.push_str(" range unbounded"),
        Some(Frame::Range { preceding: Some(n) }) => out.push_str(&format!(" range {n}")),
        None => {}
    }
    Ok(out)
}

/// What an `OVER` operator or a dedupe keeps, by the stream's names, but for
/// its functions or which row it keeps.
struct Kept<'a> {
    output: &'a str,
    input: &'a str,
    /// The event-time column, and its type.
    time: (&'a str, &'a DataType),
    /// The keys, and their types, in the operator's order.
    keys: &'a [(&'a str, &'a DataType)],
    /// What the steps before the operator do to the rows: filters and
    /// unnests.
    before: &'a [String],
}

impl Kept<'_> {
    fn description(&self, kind: &str, layout: u8) -> String {
        use crate::canon::type_text;
        let mut d = format!(
            "{kind} {layout}\noutput {}\ninput {}\ntime {} {}\n",
            crate::ident(self.output),
            crate::ident(self.input),
            crate::ident(self.time.0),
            type_text(self.time.1)
        );
        for (name, t) in self.keys {
            d.push_str(&format!("key {} {}\n", crate::ident(name), type_text(t)));
        }
        d
    }
}

/// A hash of what an `OVER` operator computes and keeps: the output it
/// feeds, the stream it reads, its time and keys with their types, its
/// functions over the stream's columns with their frames and types, the
/// filters and unnests before it, and its state's layout.
fn over_id(k: &Kept, functions: &[OverFunction], computes: &[String]) -> String {
    use crate::canon::type_text;
    let mut d = k.description("over", OVER_STATE);
    for (f, c) in functions.iter().zip(computes) {
        let arg = f
            .arg
            .as_ref()
            .map(|e| type_text(e.data_type()))
            .unwrap_or_default();
        d.push_str(&format!(
            "function {c} of {arg} as {}\n",
            type_text(&f.data_type)
        ));
    }
    // Quoted, so no entry's text can read as two.
    for b in k.before {
        d.push_str(&format!("{b:?}\n"));
    }
    format!("over-{:016x}", XxHash3_64::oneshot(d.as_bytes()))
}

/// A hash of what a dedupe keeps: the output it feeds, the stream it reads,
/// its time and keys with their types, which row it keeps, the filters and
/// unnests before it, and its state's layout.
fn dedupe_id(k: &Kept, keep: Keep) -> String {
    let mut d = k.description("dedupe", DEDUPE_STATE);
    d.push_str(match keep {
        Keep::First => "keep first\n",
        Keep::Latest => "keep latest\n",
    });
    for b in k.before {
        d.push_str(&format!("{b:?}\n"));
    }
    format!("dedupe-{:016x}", XxHash3_64::oneshot(d.as_bytes()))
}

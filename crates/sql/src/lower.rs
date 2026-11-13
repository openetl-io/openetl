//! DataFusion's logical plan of a statement, classified and lowered to ours.

use datafusion_expr::{Expr as DfExpr, LogicalPlan, TableScan};

use crate::window::{TUMBLE, width};
use crate::{
    Aggregate, Catalog, Class, Column, Error, ErrorKind, Expr, GroupWindow, Operator, Position,
    RowWise, Statement, Step, Window,
};

pub(crate) fn statement(
    output: &str,
    at: Position,
    plan: &LogicalPlan,
    catalog: &Catalog,
) -> Result<Statement, Error> {
    // From the top down to the scan.
    let mut chain = Vec::new();
    let mut node = plan;
    let scan = loop {
        match node {
            LogicalPlan::TableScan(s) => break s,
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Aggregate(_) => chain.push(node),
            other => return Err(Error::refused(at, refusal(other))),
        }
        let inputs = node.inputs();
        let [input] = inputs.as_slice() else {
            return Err(Error::refused(at, "a statement reads one stream"));
        };
        node = input;
    };
    if !scan.filters.is_empty() {
        return Err(Error::new_internal(at, "a filter was pushed into a scan"));
    }
    if scan.fetch.is_some() {
        return Err(Error::refused(
            at,
            "a stream has no end, so LIMIT is refused",
        ));
    }
    let input = scan.table_name.table().to_owned();
    let Some(stream) = catalog.get(&input) else {
        return Err(Error::new_internal(
            at,
            "the scan's stream isn't in the catalog",
        ));
    };
    let schema = plan.schema().inner().clone();
    chain.reverse();
    let aggregates: Vec<_> = chain
        .iter()
        .enumerate()
        .filter(|(_, n)| matches!(n, LogicalPlan::Aggregate(_)))
        .map(|(i, _)| i)
        .collect();
    match aggregates.as_slice() {
        [] => {
            let op = row_wise(&chain, scan, at)?;
            let key = follow(&stream.key, &op.steps);
            if stream.changelog && key.is_empty() && !stream.key.is_empty() {
                return Err(Error::refused(
                    at,
                    format!(
                        "{input} is a changelog, so a statement on it keeps its key ({}) as it is",
                        stream.key.join(", ")
                    ),
                ));
            }
            Ok(Statement {
                output: output.to_owned(),
                at,
                class: Class::RowWise,
                schema,
                key,
                operator: Operator::RowWise(op),
            })
        }
        [i] => {
            if stream.changelog {
                return Err(Error::refused(
                    at,
                    format!("{input} is a changelog; a group window needs an append-only stream"),
                ));
            }
            let LogicalPlan::Aggregate(agg) = chain[*i] else {
                return Err(Error::new_internal(at, "not an aggregate"));
            };
            let pre = row_wise(&chain[..*i], scan, at)?;
            let (window, keys, aggregates) = group_window(agg, at)?;
            let post = steps(&chain[i + 1..], at)?;
            // The window's start and the keys, as the aggregate names them.
            let grouped: Vec<String> = agg
                .schema
                .fields()
                .iter()
                .take(agg.group_expr.len())
                .map(|f| f.name().clone())
                .collect();
            let key = follow(&grouped, &post);
            Ok(Statement {
                output: output.to_owned(),
                at,
                class: Class::GroupWindow,
                schema,
                key,
                operator: Operator::GroupWindow(GroupWindow {
                    pre,
                    window,
                    keys,
                    aggregates,
                    post,
                }),
            })
        }
        _ => Err(Error::refused(at, "a statement aggregates once")),
    }
}

impl Error {
    fn new_internal(at: Position, message: &str) -> Self {
        Error {
            kind: ErrorKind::Bind,
            at: Some(at),
            message: format!("internal: {message}"),
        }
    }
}

fn refusal(node: &LogicalPlan) -> String {
    match node {
        LogicalPlan::Join(_) => "joins aren't supported yet".into(),
        LogicalPlan::Sort(_) => "a stream has no order, so ORDER BY is refused".into(),
        LogicalPlan::Limit(_) => "a stream has no end, so LIMIT is refused".into(),
        LogicalPlan::Distinct(_) => "DISTINCT keeps state per row: not supported yet".into(),
        LogicalPlan::Window(_) => "OVER isn't supported yet".into(),
        LogicalPlan::Unnest(_) => "UNNEST isn't supported yet".into(),
        LogicalPlan::Union(_) => "UNION isn't supported yet".into(),
        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
            "a statement reads a stream".into()
        }
        other => format!("{} isn't supported", other.display()),
    }
}

/// Filters and projections, in the order they run.
fn steps(chain: &[&LogicalPlan], at: Position) -> Result<Vec<Step>, Error> {
    let mut steps = Vec::new();
    for node in chain {
        match node {
            LogicalPlan::Filter(f) => {
                steps.push(Step::Filter(Expr::compile(
                    &f.predicate,
                    f.input.schema(),
                    at,
                )?));
            }
            LogicalPlan::Projection(p) => {
                let mut columns = Vec::with_capacity(p.expr.len());
                for (e, f) in p.expr.iter().zip(p.schema.fields()) {
                    columns.push(Column {
                        name: f.name().clone(),
                        expr: Expr::compile(e, p.input.schema(), at)?,
                    });
                }
                steps.push(Step::Project {
                    columns,
                    schema: p.schema.inner().clone(),
                });
            }
            LogicalPlan::SubqueryAlias(_) => {}
            other => return Err(Error::refused(at, refusal(other))),
        }
    }
    Ok(steps)
}

fn row_wise(chain: &[&LogicalPlan], scan: &TableScan, at: Position) -> Result<RowWise, Error> {
    let reads = scan.projected_schema.inner().clone();
    let schema = match chain.last() {
        Some(n) => n.schema().inner().clone(),
        None => reads.clone(),
    };
    Ok(RowWise {
        input: scan.table_name.table().to_owned(),
        reads,
        steps: steps(chain, at)?,
        schema,
    })
}

/// Where columns that pass through unchanged end up: their names after the
/// steps, or nothing if any of them doesn't.
fn follow(columns: &[String], steps: &[Step]) -> Vec<String> {
    let mut names = columns.to_vec();
    for s in steps {
        let Step::Project { columns, .. } = s else {
            continue;
        };
        let mut next = Vec::with_capacity(names.len());
        for n in &names {
            match columns.iter().find(|c| c.expr.column() == Some(n.as_str())) {
                Some(c) => next.push(c.name.clone()),
                None => return Vec::new(),
            }
        }
        names = next;
    }
    names
}

type Grouped = (Window, Vec<Column>, Vec<Aggregate>);

fn group_window(agg: &datafusion_expr::Aggregate, at: Position) -> Result<Grouped, Error> {
    let input = agg.input.schema();
    let names = agg.schema.fields();
    let mut window = None;
    let mut keys = Vec::new();
    for (e, f) in agg.group_expr.iter().zip(names.iter()) {
        if let DfExpr::ScalarFunction(call) = e
            && call.func.name() == TUMBLE
        {
            let w = match call.args.get(1) {
                Some(DfExpr::Literal(v, _)) => {
                    width(v).map_err(|e| Error::datafusion(ErrorKind::Refused, at, e))?
                }
                _ => return Err(Error::refused(at, "a window's width is a constant")),
            };
            window = Some(Window::Tumble {
                start: Expr::compile(e, input, at)?,
                width: w,
            });
            continue;
        }
        if matches!(e, DfExpr::GroupingSet(_)) {
            return Err(Error::refused(at, "grouping sets aren't supported"));
        }
        keys.push(Column {
            name: f.name().clone(),
            expr: Expr::compile(e, input, at)?,
        });
    }
    let Some(window) = window else {
        return Err(Error::refused(
            at,
            "an aggregate without a window changes rows it has given: not supported yet",
        ));
    };
    let mut aggregates = Vec::with_capacity(agg.aggr_expr.len());
    for (e, f) in agg
        .aggr_expr
        .iter()
        .zip(names.iter().skip(agg.group_expr.len()))
    {
        let DfExpr::AggregateFunction(a) = e.clone().unalias_nested().data else {
            return Err(Error::new_internal(
                at,
                "an aggregate that isn't a function",
            ));
        };
        if a.params.filter.is_some() || !a.params.order_by.is_empty() {
            return Err(Error::refused(
                at,
                "FILTER and ORDER BY in an aggregate aren't supported yet",
            ));
        }
        let mut args = Vec::with_capacity(a.params.args.len());
        for arg in &a.params.args {
            args.push(Expr::compile(arg, input, at)?);
        }
        aggregates.push(Aggregate {
            name: f.name().clone(),
            function: a.func.name().to_owned(),
            args,
            distinct: a.params.distinct,
            data_type: f.data_type().clone(),
        });
    }
    Ok((window, keys, aggregates))
}

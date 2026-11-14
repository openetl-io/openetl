//! DataFusion's logical plan of a row-wise query, lowered to steps of ours.

use arrow_schema::SchemaRef;
use datafusion_common::{NullHandling, ScalarValue};
use datafusion_expr::{Expr as DfExpr, LogicalPlan};

use crate::plan::{Column, Projection, RowWise, Step, Unnest};
use crate::{Catalog, Class, Error, ErrorKind, Expr, Position};

pub(crate) struct Lowered {
    pub(crate) operator: RowWise,
    pub(crate) schema: SchemaRef,
    /// The input's key where it passes through unchanged; empty otherwise.
    pub(crate) key: Vec<String>,
    /// The input is a changelog, and the statement loses its key or
    /// multiplies its rows.
    pub(crate) loses_changelog_key: bool,
}

pub(crate) fn row_wise(
    plan: &LogicalPlan,
    catalog: &Catalog,
    at: Position,
) -> Result<Lowered, Error> {
    // From the top down to the scan.
    let mut chain = Vec::new();
    let mut node = plan;
    let scan = loop {
        match node {
            LogicalPlan::TableScan(s) => break s,
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Unnest(_) => chain.push(node),
            other => return Err(refusal(other, at)),
        }
        let inputs = node.inputs();
        let [input] = inputs.as_slice() else {
            return Err(internal(at, "a step with other than one input"));
        };
        node = input;
    };
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return Err(internal(at, "a scan took a filter or a limit"));
    }
    let input = scan.table_name.table().to_owned();
    let Some(stream) = catalog.get(&input) else {
        return Err(internal(at, "a scan of a stream that isn't in the catalog"));
    };
    chain.reverse();
    let mut steps = Vec::with_capacity(chain.len());
    for node in chain {
        match node {
            LogicalPlan::Filter(f) => {
                if matches!(
                    f.predicate,
                    DfExpr::Literal(ScalarValue::Boolean(Some(true)), _)
                ) {
                    continue;
                }
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
                steps.push(Step::Project(Projection {
                    columns,
                    schema: p.schema.inner().clone(),
                }));
            }
            LogicalPlan::Unnest(u) => steps.push(Step::Unnest(unnest(u, at)?)),
            _ => {}
        }
    }
    let reads = scan.projected_schema.inner().clone();
    let key = if stream.key.iter().all(|k| reads.field_with_name(k).is_ok()) {
        follow(&stream.key, &steps)
    } else {
        Vec::new()
    };
    let unnests = steps.iter().any(|s| matches!(s, Step::Unnest(_)));
    Ok(Lowered {
        loses_changelog_key: stream.changelog
            && (unnests || !stream.key.is_empty() && key.is_empty()),
        operator: RowWise {
            input,
            changelog: stream.changelog,
            reads,
            steps,
        },
        schema: plan.schema().inner().clone(),
        key,
    })
}

fn unnest(u: &datafusion_expr::logical_plan::Unnest, at: Position) -> Result<Unnest, Error> {
    if !u.struct_type_columns.is_empty() {
        return Err(Error::at(
            ErrorKind::Unsupported,
            at,
            "UNNEST takes a list; a struct's fields are s['field']",
        ));
    }
    if u.list_type_columns.iter().any(|(_, l)| l.depth != 1)
        || u.options.recursions.iter().any(|r| r.depth != 1)
    {
        return Err(Error::at(
            ErrorKind::Unsupported,
            at,
            "UNNEST flattens one level of a list: use flatten() for more",
        ));
    }
    let outer = match u.options.null_handling {
        NullHandling::Drop => false,
        NullHandling::PreserveAndExpandEmpty => true,
        NullHandling::Preserve => return Err(internal(at, "an UNNEST that keeps nulls only")),
    };
    Ok(Unnest {
        lists: u.list_type_columns.iter().map(|(i, _)| *i).collect(),
        sources: u.dependency_indices.clone(),
        outer,
        schema: u.schema.inner().clone(),
    })
}

/// Where the key's columns end up, by name, when each passes through every
/// projection unchanged; nothing if any of them doesn't, or rows multiply.
/// A column passed through under several names is followed under each, and
/// ends up under the first the output has.
fn follow(key: &[String], steps: &[Step]) -> Vec<String> {
    let mut names: Vec<Vec<String>> = key.iter().map(|k| vec![k.clone()]).collect();
    for s in steps {
        match s {
            Step::Filter(_) => {}
            Step::Unnest(_) => return Vec::new(),
            Step::Project(p) => {
                for n in &mut names {
                    let next: Vec<String> = p
                        .columns
                        .iter()
                        .filter(|c| {
                            c.expr
                                .column()
                                .is_some_and(|from| n.iter().any(|m| m == from))
                        })
                        .map(|c| c.name.clone())
                        .collect();
                    if next.is_empty() {
                        return Vec::new();
                    }
                    *n = next;
                }
            }
        }
    }
    names
        .into_iter()
        .filter_map(|n| n.into_iter().next())
        .collect()
}

fn internal(at: Position, what: &str) -> Error {
    Error::at(ErrorKind::Internal, at, format!("planning gave {what}"))
}

/// What a node no row-wise statement has means. The checks before binding
/// refuse each of these first, where they can say where.
fn refusal(node: &LogicalPlan, at: Position) -> Error {
    let later = |class, what: &str| Error::at(ErrorKind::LaterTier(class), at, what);
    let unsupported = |what: &str| Error::at(ErrorKind::Unsupported, at, what);
    match node {
        LogicalPlan::Aggregate(_) => later(Class::Updating, "an aggregate"),
        LogicalPlan::Distinct(_) => later(Class::Updating, "DISTINCT"),
        LogicalPlan::Window(_) => later(Class::Over, "OVER"),
        LogicalPlan::Join(_) | LogicalPlan::Subquery(_) => later(Class::Join, "a join"),
        LogicalPlan::Sort(_) => unsupported("ORDER BY"),
        LogicalPlan::Limit(_) => unsupported("LIMIT"),
        LogicalPlan::Union(_) => unsupported("UNION"),
        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
            unsupported("a statement that reads no stream")
        }
        other => internal(at, &format!("a {} step", other.display())),
    }
}

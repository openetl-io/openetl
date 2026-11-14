//! Binding a query, whose stream and view names are already resolved,
//! against the catalog and the views planned before it, with our functions;
//! then type coercion and a few of DataFusion's logical rewrites.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result, TableReference, plan_err};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, Cast, Expr, HigherOrderUDF, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
    TableSource, WindowUDF,
};
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerContext, OptimizerRule};
use datafusion_physical_expr::create_physical_expr;
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::ast;

use crate::functions::FUNCTIONS;
use crate::{Catalog, Error, ErrorKind, Position};

pub(crate) fn options() -> ConfigOptions {
    let mut o = ConfigOptions::default();
    // Strings stay Utf8, as streams have them.
    o.sql_parser.map_string_types_to_utf8view = false;
    o.sql_parser.collect_spans = true;
    o
}

/// A stream as DataFusion sees it. It takes no filters, so they stay in the
/// plan, where the operator evaluates them.
struct Source(SchemaRef);

impl TableSource for Source {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
}

/// A view as DataFusion sees it: its plan, which binding inlines into the
/// plan that reads it.
struct ViewSource(LogicalPlan);

impl TableSource for ViewSource {
    fn schema(&self) -> SchemaRef {
        self.0.schema().inner().clone()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Borrowed(&self.0))
    }
}

struct Provider<'a> {
    catalog: &'a Catalog,
    views: &'a [(&'a str, &'a LogicalPlan)],
    options: ConfigOptions,
}

impl ContextProvider for Provider<'_> {
    /// Names reach here resolved: a view's name, or a stream's whole name. No
    /// view has a stream's name.
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if !matches!(name, TableReference::Bare { .. }) {
            return plan_err!("no stream {name}");
        }
        if let Some((_, plan)) = self.views.iter().find(|(v, _)| *v == name.table()) {
            return Ok(Arc::new(ViewSource((*plan).clone())));
        }
        match self.catalog.get(name.table()) {
            Some(s) => Ok(Arc::new(Source(s.schema.clone()))),
            None => plan_err!("no stream {name}"),
        }
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &FUNCTIONS.planners
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        FUNCTIONS.get(name).cloned()
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        FUNCTIONS.scalars.keys().cloned().collect()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Binds a query, coerces its types and simplifies it. Columns a statement
/// doesn't use are pruned from its scan. Filters stay where they are written,
/// and nothing is folded that isn't deterministic. Its columns are cast to
/// logical types where that loses nothing; `column` says where the
/// statement gives a column, by name, for a column that can't be.
pub(crate) fn plan(
    query: &ast::Query,
    catalog: &Catalog,
    views: &[(&str, &LogicalPlan)],
    at: Position,
    column: &dyn Fn(&str) -> Position,
) -> Result<LogicalPlan, Error> {
    let p = Provider {
        catalog,
        views,
        options: options(),
    };
    let bind = |e| Error::datafusion(ErrorKind::Bind, at, &e);
    let plan = SqlToRel::new(&p)
        .sql_statement_to_plan(ast::Statement::Query(Box::new(query.clone())))
        .map_err(bind)?;
    let mut analyzer = Analyzer::new();
    for r in &FUNCTIONS.rewrites {
        analyzer.add_function_rewrite(r.clone());
    }
    let plan = analyzer
        .execute_and_check(plan, &p.options, |_, _| {})
        .map_err(bind)?;
    unexposed(&plan, at)?;
    let plan = logical(plan, at, column)?;
    overflows(&plan, at, column)?;
    let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(SimplifyExpressions::new()),
        Arc::new(OptimizeProjections::new()),
    ];
    let ctx = OptimizerContext::new_with_config_options(Arc::new(p.options))
        .without_query_execution_start_time();
    Optimizer::with_rules(rules)
        .optimize(plan, &ctx, |_, _| {})
        .map_err(bind)
}

/// Casts the plan's columns to logical types where they aren't, and refuses
/// a column no logical type holds. `json_get`'s value of any type becomes
/// its JSON text, in a JSON column.
fn logical(
    plan: LogicalPlan,
    at: Position,
    column: &dyn Fn(&str) -> Position,
) -> Result<LogicalPlan, Error> {
    let mut exprs = Vec::with_capacity(plan.schema().fields().len());
    let mut cast = false;
    for (qualifier, f) in plan.schema().iter() {
        let c = Expr::Column(datafusion_common::Column::new(qualifier.cloned(), f.name()));
        if crate::json::is_union(f.data_type()) {
            cast = true;
            exprs.push(crate::json::text(c).alias(f.name()));
            continue;
        }
        match crate::types::output(f.data_type()) {
            Some(t) if t == *f.data_type() => exprs.push(c),
            Some(t) => {
                cast = true;
                exprs.push(Expr::Cast(Cast::new(Box::new(c), t)).alias(f.name()));
            }
            None => {
                return Err(Error::at(
                    ErrorKind::Bind,
                    column(f.name()),
                    format!(
                        "column {} is {}, which an output can't hold: CAST it to a type it can",
                        f.name(),
                        f.data_type()
                    ),
                ));
            }
        }
    }
    if !cast {
        return Ok(plan);
    }
    LogicalPlanBuilder::from(plan)
        .project(exprs)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|e| Error::datafusion(ErrorKind::Bind, at, &e))
}

/// Refuses constant arithmetic whose result overflows its type.
/// SimplifyExpressions folds a constant with DataFusion's arithmetic, which
/// wraps, and every row would be written with the wrapped value; checked, as
/// the operator evaluates it, the same expression fails. Each largest
/// constant part of an expression is evaluated both ways, and refused where
/// only the checked one fails.
fn overflows(
    plan: &LogicalPlan,
    at: Position,
    column: &dyn Fn(&str) -> Position,
) -> Result<(), Error> {
    let mut found = None;
    let _ = plan.apply(|node| {
        let projection = matches!(node, LogicalPlan::Projection(_));
        node.apply_expressions(|e| {
            let at = match e {
                Expr::Alias(a) if projection => column(&a.name),
                _ => at,
            };
            e.apply(|part| {
                if !constant_arithmetic(part) {
                    return Ok(TreeNodeRecursion::Continue);
                }
                if let Some(why) = overflow(part) {
                    let part = match part {
                        Expr::Alias(a) => a.expr.as_ref(),
                        p => p,
                    };
                    found = Some(Error::at(
                        ErrorKind::Bind,
                        at,
                        format!("{part} overflows its type ({why})"),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Jump)
            })
        })
    });
    match found {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Whether an expression reads nothing but constants, the same each time,
/// and does arithmetic that can overflow.
fn constant_arithmetic(e: &Expr) -> bool {
    use datafusion_expr::Operator as Op;
    let mut arithmetic = false;
    let mut variable = false;
    let _ = e.apply(|x| {
        match x {
            Expr::Column(_)
            | Expr::OuterReferenceColumn(..)
            | Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::Placeholder(_)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Unnest(_) => variable = true,
            x if x.is_volatile_node() => variable = true,
            Expr::BinaryExpr(b) if matches!(b.op, Op::Plus | Op::Minus | Op::Multiply) => {
                arithmetic = true
            }
            Expr::Negative(_) => arithmetic = true,
            _ => {}
        }
        Ok(match variable {
            true => TreeNodeRecursion::Stop,
            false => TreeNodeRecursion::Continue,
        })
    });
    arithmetic && !variable
}

/// Why a constant overflows: its checked value fails where DataFusion's
/// doesn't. None where both give a value, or both fail, as for 1 / 0, which
/// then fails each row as it's evaluated.
fn overflow(e: &Expr) -> Option<String> {
    let schema = DFSchema::empty();
    let physical = create_physical_expr(
        e,
        &schema,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
    .ok()?;
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .ok()?;
    physical.evaluate(&batch).ok()?;
    let checked = crate::expr::checked(physical, schema.as_arrow()).ok()?;
    checked
        .evaluate(&batch)
        .err()
        .map(|e| e.message().into_owned())
}

/// Refuses a function that syntax planned but that isn't one we expose, such
/// as `map` from `MAP {…}`.
fn unexposed(plan: &LogicalPlan, at: Position) -> Result<(), Error> {
    let mut found = None;
    let _ = plan.apply(|node| {
        node.apply_expressions(|e| {
            e.apply(|e| {
                if let Expr::ScalarFunction(f) = e
                    && FUNCTIONS.get(f.func.name()).is_none()
                {
                    found = Some(f.func.name().to_owned());
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    });
    match found {
        Some(name) => Err(Error::at(
            ErrorKind::Bind,
            at,
            format!("{name} isn't a function transforms can call"),
        )),
        None => Ok(()),
    }
}

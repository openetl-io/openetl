//! Binding a query, whose stream and view names are already resolved,
//! against the catalog and the views planned before it, with our functions;
//! then type coercion and a few of DataFusion's logical rewrites.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, TableReference, plan_err};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, Cast, Expr, HigherOrderUDF, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
    TableSource, WindowUDF,
};
use datafusion_optimizer::optimize_projections::OptimizeProjections;
use datafusion_optimizer::simplify_expressions::SimplifyExpressions;
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerContext, OptimizerRule};
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
    let plan = Analyzer::new()
        .execute_and_check(plan, &p.options, |_, _| {})
        .map_err(bind)?;
    unexposed(&plan, at)?;
    let plan = logical(plan, at, column)?;
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
/// a column no logical type holds.
fn logical(
    plan: LogicalPlan,
    at: Position,
    column: &dyn Fn(&str) -> Position,
) -> Result<LogicalPlan, Error> {
    let mut exprs = Vec::with_capacity(plan.schema().fields().len());
    let mut cast = false;
    for (qualifier, f) in plan.schema().iter() {
        let c = Expr::Column(datafusion_common::Column::new(qualifier.cloned(), f.name()));
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

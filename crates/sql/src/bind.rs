//! Binding against our catalog of streams, with DataFusion's functions and
//! ours, then type coercion and the logical optimizer.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, TableReference, plan_err};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_optimizer::{Analyzer, Optimizer, OptimizerContext};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::ast;

use crate::{Catalog, Error, ErrorKind, Position};

/// The functions statements can call, by name and alias.
struct Functions {
    scalars: HashMap<String, Arc<ScalarUDF>>,
    aggregates: HashMap<String, Arc<AggregateUDF>>,
    planners: Vec<Arc<dyn ExprPlanner>>,
}

static FUNCTIONS: LazyLock<Functions> = LazyLock::new(|| {
    let mut scalars = HashMap::new();
    let all = datafusion_functions::all_default_functions()
        .into_iter()
        .chain(datafusion_functions_nested::all_default_nested_functions())
        .chain([crate::window::tumble()]);
    for f in all {
        for a in f.aliases() {
            scalars.insert(a.clone(), f.clone());
        }
        scalars.insert(f.name().to_owned(), f);
    }
    let mut aggregates = HashMap::new();
    for f in datafusion_functions_aggregate::all_default_aggregate_functions() {
        for a in f.aliases() {
            aggregates.insert(a.clone(), f.clone());
        }
        aggregates.insert(f.name().to_owned(), f);
    }
    let planners: Vec<Arc<dyn ExprPlanner>> = vec![
        Arc::new(datafusion_functions::core::planner::CoreFunctionPlanner::default()),
        Arc::new(datafusion_functions_nested::planner::NestedFunctionPlanner),
        Arc::new(datafusion_functions_nested::planner::FieldAccessPlanner),
        Arc::new(datafusion_functions::datetime::planner::DatetimeFunctionPlanner),
        Arc::new(datafusion_functions::unicode::planner::UnicodeFunctionPlanner),
        Arc::new(datafusion_functions_aggregate::planner::AggregateFunctionPlanner),
    ];
    Functions {
        scalars,
        aggregates,
        planners,
    }
});

pub(crate) fn options() -> ConfigOptions {
    let mut o = ConfigOptions::default();
    // Strings stay Utf8, as streams have them.
    o.sql_parser.map_string_types_to_utf8view = false;
    o.sql_parser.collect_spans = true;
    o
}

/// A stream as DataFusion sees it. Filters stay in the plan, where the
/// operator evaluates them.
struct Source(SchemaRef);

impl TableSource for Source {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
}

struct Provider<'a> {
    catalog: &'a Catalog,
    options: ConfigOptions,
}

impl ContextProvider for Provider<'_> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if !matches!(name, TableReference::Bare { .. }) {
            return plan_err!("streams have one-part names, not {name}");
        }
        match self.catalog.get(name.table()) {
            Some(s) => Ok(Arc::new(Source(s.schema.clone()))),
            None => plan_err!("no stream {}", name.table()),
        }
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &FUNCTIONS.planners
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        FUNCTIONS.scalars.get(name).cloned()
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        FUNCTIONS.aggregates.get(name).cloned()
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
        FUNCTIONS.aggregates.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Binds a query, coerces its types and optimizes it. Time functions such as
/// `now()` aren't folded to a constant.
pub(crate) fn plan(
    query: &ast::Query,
    catalog: &Catalog,
    at: Position,
) -> Result<LogicalPlan, Error> {
    let p = Provider {
        catalog,
        options: options(),
    };
    let bind = |e| Error::datafusion(ErrorKind::Bind, at, e);
    let plan = SqlToRel::new(&p)
        .sql_statement_to_plan(ast::Statement::Query(Box::new(query.clone())))
        .map_err(bind)?;
    let plan = Analyzer::new()
        .execute_and_check(plan, &p.options, |_, _| {})
        .map_err(bind)?;
    let ctx = OptimizerContext::new_with_config_options(Arc::new(p.options))
        .without_query_execution_start_time();
    Optimizer::new()
        .optimize(plan, &ctx, |_, _| {})
        .map_err(bind)
}

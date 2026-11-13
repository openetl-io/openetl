use std::fmt;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::DataType;
use datafusion_common::DFSchema;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr as DfExpr, Volatility};
use datafusion_physical_expr::PhysicalExpr;

use crate::{Error, ErrorKind, Position};

/// An expression over a step's input, ready to evaluate on its batches.
pub struct Expr {
    display: String,
    data_type: DataType,
    column: Option<String>,
    inputs: Vec<usize>,
    deterministic: bool,
    physical: Arc<dyn PhysicalExpr>,
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

/// How the expression reads; for people, not to parse.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl Expr {
    pub(crate) fn compile(e: &DfExpr, input: &DFSchema, at: Position) -> Result<Self, Error> {
        let bind = |e| Error::datafusion(ErrorKind::Bind, at, &e);
        let inner = e.clone().unalias_nested().data;
        let column = match &inner {
            DfExpr::Column(c) => Some(c.name.clone()),
            _ => None,
        };
        let mut deterministic = true;
        let _ = inner.apply(|e| {
            if let DfExpr::ScalarFunction(f) = e
                && f.func.signature().volatility != Volatility::Immutable
            {
                deterministic = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        let mut inputs = Vec::new();
        for c in inner.column_refs() {
            inputs.push(input.index_of_column(c).map_err(bind)?);
        }
        inputs.sort_unstable();
        inputs.dedup();
        let physical = datafusion_physical_expr::create_physical_expr(
            &inner,
            input,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .map_err(bind)?;
        let data_type = physical.data_type(input.as_arrow()).map_err(bind)?;
        Ok(Expr {
            display: inner.to_string(),
            data_type,
            column,
            inputs,
            deterministic,
            physical,
        })
    }

    /// The input column it passes through unchanged, if that's all it does.
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    /// The columns of its step's input it reads, by index, in order.
    pub fn inputs(&self) -> &[usize] {
        &self.inputs
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Whether it gives the same value for the same input, every time.
    /// `now()` and `random()` don't.
    pub fn is_deterministic(&self) -> bool {
        self.deterministic
    }

    /// Its value per row of a batch of its step's input.
    pub fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef, Error> {
        self.physical
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .map_err(|e| Error::eval(e.message().into_owned()))
    }

    /// A predicate's value per row, where null is false.
    pub fn eval_predicate(&self, batch: &RecordBatch) -> Result<BooleanArray, Error> {
        let a = self.eval(batch)?;
        let Some(b) = a.as_boolean_opt() else {
            return Err(Error::eval(format!(
                "a predicate gave {}, not booleans",
                a.data_type()
            )));
        };
        Ok(match b.nulls() {
            Some(nulls) => BooleanArray::new(b.values() & nulls.inner(), None),
            None => b.clone(),
        })
    }
}

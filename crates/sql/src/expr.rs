use std::fmt;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::DataType;
use datafusion_common::DFSchema;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_physical_expr::PhysicalExpr;

use crate::{Error, ErrorKind, Position};

/// An expression over a step's input, ready to evaluate on its batches.
pub struct Expr {
    display: String,
    data_type: DataType,
    column: Option<String>,
    physical: Arc<dyn PhysicalExpr>,
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display)
    }
}

impl Expr {
    pub(crate) fn compile(
        e: &datafusion_expr::Expr,
        input: &DFSchema,
        at: Position,
    ) -> Result<Self, Error> {
        let bind = |e| Error::datafusion(ErrorKind::Bind, at, e);
        let inner = e.clone().unalias_nested().data;
        let column = match &inner {
            datafusion_expr::Expr::Column(c) => Some(c.name.clone()),
            _ => None,
        };
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
            physical,
        })
    }

    /// The input column it passes through unchanged, if that's all it does.
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef, Error> {
        self.physical
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .map_err(|e| Error::eval(e.strip_backtrace()))
    }

    /// A predicate's value per row; null is false.
    pub(crate) fn eval_mask(&self, batch: &RecordBatch) -> Result<BooleanArray, Error> {
        let a = self.eval(batch)?;
        match a.as_boolean_opt() {
            Some(b) => Ok(b.clone()),
            None => Err(Error::eval(format!(
                "a filter gave {}, not booleans",
                a.data_type()
            ))),
        }
    }
}

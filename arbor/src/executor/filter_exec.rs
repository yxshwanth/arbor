//! Boolean filter operator.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;

use crate::error::{ArborError, Result};
use crate::executor::{evaluate_expr, PhysicalPlan};
use crate::planner::Expr;

/// Keeps rows where the predicate evaluates to true.
pub struct FilterExec {
    child: Box<dyn PhysicalPlan>,
    predicate: Expr,
    schema: SchemaRef,
}

impl FilterExec {
    /// Wraps `child` with boolean `predicate`.
    pub fn new(child: Box<dyn PhysicalPlan>, predicate: Expr) -> Result<Self> {
        let schema = child.schema();
        Ok(Self {
            child,
            predicate,
            schema,
        })
    }
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let Some(batch) = self.child.next_batch()? else {
                return Ok(None);
            };
            let mask_arr = evaluate_expr(&self.predicate, &batch)?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ArborError::Execution("filter mask must be boolean".into()))?;
            let out = filter_record_batch(&batch, mask)?;
            if out.num_rows() == 0 {
                continue;
            }
            return Ok(Some(out));
        }
    }
}

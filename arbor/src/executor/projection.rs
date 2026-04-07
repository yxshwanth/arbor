//! Evaluate expressions into a new batch.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::executor::{evaluate_expr, PhysicalPlan};
use crate::planner::Expr;

/// Projects a set of expressions with explicit output names and schema.
pub struct ProjectionExec {
    child: Box<dyn PhysicalPlan>,
    pairs: Vec<(Expr, String)>,
    schema: SchemaRef,
}

impl ProjectionExec {
    /// Wraps `child` with evaluated expressions. `schema` is the logical output Arrow schema.
    pub fn new(
        child: Box<dyn PhysicalPlan>,
        pairs: Vec<(Expr, String)>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            child,
            pairs,
            schema,
        }
    }
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let mut columns = Vec::new();
        for (expr, _) in &self.pairs {
            columns.push(evaluate_expr(expr, &batch)?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(crate::error::ArborError::from)?;
        Ok(Some(batch))
    }
}

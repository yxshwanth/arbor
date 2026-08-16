//! LIMIT rows.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::executor::PhysicalPlan;

/// Returns at most `n` rows from its child.
pub struct LimitExec {
    child: Box<dyn PhysicalPlan>,
    remaining: usize,
    schema: SchemaRef,
}

impl LimitExec {
    /// Caps output from `child` to `n` rows across batches.
    pub fn new(child: Box<dyn PhysicalPlan>, n: usize) -> Self {
        let schema = child.schema();
        Self {
            child,
            remaining: n,
            schema,
        }
    }
}

impl PhysicalPlan for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        let n = batch.num_rows();
        if n <= self.remaining {
            self.remaining -= n;
            Ok(Some(batch))
        } else {
            let out = batch.slice(0, self.remaining);
            self.remaining = 0;
            Ok(Some(out))
        }
    }
}

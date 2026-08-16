//! Sort rows by one or more key expressions (in-memory).

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::kernels::sort::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;

use crate::error::{ArborError, Result};
use crate::executor::{evaluate_expr, PhysicalPlan};
use crate::planner::SortExpr;

/// Sorts all input in memory then returns the sorted result (single batch).
pub struct SortExec {
    child: Box<dyn PhysicalPlan>,
    sort_exprs: Vec<SortExpr>,
    schema: SchemaRef,
    pending: Option<Vec<RecordBatch>>,
    next_idx: usize,
}

impl SortExec {
    /// Creates a sort; buffers full input on first `next_batch` call.
    pub fn new(child: Box<dyn PhysicalPlan>, sort_exprs: Vec<SortExpr>) -> Result<Self> {
        let schema = child.schema();
        Ok(Self {
            child,
            sort_exprs,
            schema,
            pending: None,
            next_idx: 0,
        })
    }
}

impl PhysicalPlan for SortExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_none() {
            let mut batches = Vec::new();
            while let Some(b) = self.child.next_batch()? {
                batches.push(b);
            }
            if batches.is_empty() {
                self.pending = Some(vec![]);
            } else {
                let schema = batches[0].schema();
                let all = concat_batches(&schema, &batches).map_err(ArborError::from)?;
                let sort_cols: Vec<SortColumn> = self
                    .sort_exprs
                    .iter()
                    .map(|se| {
                        let arr = evaluate_expr(&se.expr, &all)?;
                        Ok(SortColumn {
                            values: arr,
                            options: Some(SortOptions {
                                descending: !se.asc,
                                nulls_first: false,
                            }),
                        })
                    })
                    .collect::<Result<_>>()?;
                let idx = lexsort_to_indices(&sort_cols, None).map_err(ArborError::from)?;
                let mut cols: Vec<ArrayRef> = Vec::new();
                for i in 0..all.num_columns() {
                    let c = all.column(i);
                    cols.push(take(c, &idx, None).map_err(ArborError::from)?);
                }
                let sorted =
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(ArborError::from)?;
                self.pending = Some(vec![sorted]);
            }
        }
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| ArborError::Execution("sort state".into()))?;
        if self.next_idx >= pending.len() {
            return Ok(None);
        }
        let b = pending[self.next_idx].clone();
        self.next_idx += 1;
        Ok(Some(b))
    }
}

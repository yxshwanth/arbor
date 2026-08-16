//! Hash join (inner).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};

use crate::error::{ArborError, Result};
use crate::executor::{evaluate_expr, PhysicalPlan};
use crate::planner::JoinType;
use crate::types::ScalarValue;

/// Hash join on equi keys.
pub struct HashJoinExec {
    out_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    next_i: usize,
}

fn concat_child_schemas(left: SchemaRef, right: SchemaRef) -> SchemaRef {
    let mut fields: Vec<ArrowField> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
    Arc::new(ArrowSchema::new(fields))
}

impl HashJoinExec {
    /// Builds inner hash join; output schema is `left.schema() || right.schema()`.
    #[allow(clippy::new_ret_no_self)] // returns boxed trait object
    pub fn new(
        mut left: Box<dyn PhysicalPlan>,
        mut right: Box<dyn PhysicalPlan>,
        on: Vec<(crate::planner::Expr, crate::planner::Expr)>,
        join_type: JoinType,
    ) -> Result<Box<dyn PhysicalPlan>> {
        if !matches!(join_type, JoinType::Inner) {
            return Err(ArborError::Execution(
                "only INNER hash join is implemented".into(),
            ));
        }
        let out_schema = concat_child_schemas(left.schema(), right.schema());
        let mut rbatches = Vec::new();
        while let Some(b) = right.next_batch()? {
            rbatches.push(b);
        }
        if rbatches.is_empty() {
            return Ok(Box::new(HashJoinExec {
                out_schema,
                batches: vec![],
                next_i: 0,
            }));
        }
        let rschema = rbatches[0].schema();
        let r_all = concat_batches(&rschema, &rbatches).map_err(ArborError::from)?;
        let right_keys = eval_join_keys(on.iter().map(|(_, rk)| rk), &r_all)?;
        let mut map: HashMap<Vec<ScalarValue>, Vec<u32>> = HashMap::new();
        for row in 0..r_all.num_rows() {
            let mut key = Vec::with_capacity(right_keys.len());
            for c in &right_keys {
                key.push(scalar_at_join(c, row)?);
            }
            map.entry(key).or_default().push(row as u32);
        }
        let mut lbatches = Vec::new();
        while let Some(b) = left.next_batch()? {
            lbatches.push(b);
        }
        if lbatches.is_empty() {
            return Ok(Box::new(HashJoinExec {
                out_schema,
                batches: vec![],
                next_i: 0,
            }));
        }
        let lschema = lbatches[0].schema();
        let l_all = concat_batches(&lschema, &lbatches).map_err(ArborError::from)?;
        let batches = probe_inner(&l_all, &r_all, &on, &map, &out_schema)?;
        Ok(Box::new(HashJoinExec {
            out_schema,
            batches,
            next_i: 0,
        }))
    }
}

impl PhysicalPlan for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        self.out_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.next_i >= self.batches.len() {
            return Ok(None);
        }
        let b = self.batches[self.next_i].clone();
        self.next_i += 1;
        Ok(Some(b))
    }
}

/// Evaluates each join-key expression once over the full batch.
fn eval_join_keys<'a, I>(keys: I, batch: &RecordBatch) -> Result<Vec<ArrayRef>>
where
    I: IntoIterator<Item = &'a crate::planner::Expr>,
{
    keys.into_iter().map(|k| evaluate_expr(k, batch)).collect()
}

fn scalar_at_join(arr: &ArrayRef, row: usize) -> Result<ScalarValue> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    if !arr.is_valid(row) {
        return Ok(ScalarValue::Null);
    }
    Ok(if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        ScalarValue::Int64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        ScalarValue::Float64(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        ScalarValue::Utf8(a.value(row).to_string())
    } else if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        ScalarValue::Boolean(a.value(row))
    } else {
        return Err(ArborError::Execution("join key type".into()));
    })
}

fn probe_inner(
    l_all: &RecordBatch,
    r_all: &RecordBatch,
    on: &[(crate::planner::Expr, crate::planner::Expr)],
    map: &HashMap<Vec<ScalarValue>, Vec<u32>>,
    out_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let left_keys = eval_join_keys(on.iter().map(|(lk, _)| lk), l_all)?;
    let mut li: Vec<u32> = Vec::new();
    let mut rj: Vec<u32> = Vec::new();
    for row in 0..l_all.num_rows() {
        let mut key = Vec::with_capacity(left_keys.len());
        for c in &left_keys {
            key.push(scalar_at_join(c, row)?);
        }
        if let Some(rows) = map.get(&key) {
            for &rr in rows {
                li.push(row as u32);
                rj.push(rr);
            }
        }
    }
    if li.is_empty() {
        return Ok(vec![]);
    }
    let idx_l = UInt32Array::from(li);
    let idx_r = UInt32Array::from(rj);
    let mut cols: Vec<ArrayRef> = Vec::new();
    for i in 0..l_all.num_columns() {
        cols.push(take(l_all.column(i), &idx_l, None).map_err(ArborError::from)?);
    }
    for i in 0..r_all.num_columns() {
        cols.push(take(r_all.column(i), &idx_r, None).map_err(ArborError::from)?);
    }
    Ok(vec![
        RecordBatch::try_new(out_schema.clone(), cols).map_err(ArborError::from)?
    ])
}

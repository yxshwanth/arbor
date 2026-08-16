//! Physical execution: pull-based operators over Arrow record batches.

mod aggregate;
mod expr_eval;
mod filter_exec;
mod join;
mod limit_exec;
mod projection;
mod scan;
mod sort;

pub use expr_eval::evaluate_expr;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::{ArborError, Result};
use crate::optimizer::column_key;
use crate::planner::{Expr, LogicalPlan};

pub use aggregate::HashAggregateExec;
pub use filter_exec::FilterExec;
pub use join::HashJoinExec;
pub use limit_exec::LimitExec;
pub use projection::ProjectionExec;
pub use scan::{prune_row_groups, RowGroupPruneStats, ScanExec};
pub use sort::SortExec;

/// Standard batch size (rows) for readers and operators.
pub const BATCH_SIZE: usize = 8192;

/// Iterator-style physical operator producing Arrow batches.
pub trait PhysicalPlan: Send {
    /// Arrow schema of output batches.
    fn schema(&self) -> SchemaRef;
    /// Next batch, or `None` if exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Drains all batches from `plan` into a vector.
pub fn collect(plan: &mut dyn PhysicalPlan) -> Result<Vec<RecordBatch>> {
    let mut v = Vec::new();
    while let Some(b) = plan.next_batch()? {
        v.push(b);
    }
    Ok(v)
}

/// Builds a physical plan from a logical plan and Parquet `data_dir`.
pub fn create_physical_plan(
    logical: &LogicalPlan,
    data_dir: &Path,
) -> Result<Box<dyn PhysicalPlan>> {
    use crate::planner::LogicalPlan::*;
    match logical {
        Empty { schema } => Ok(Box::new(EmptyExec {
            schema: std::sync::Arc::new(schema.clone().into()),
        })),
        Scan {
            table_name,
            schema,
            projection,
        } => scan_exec(data_dir, table_name, schema, projection, None),
        Filter { predicate, input } => match input.as_ref() {
            Scan {
                table_name,
                schema,
                projection,
            } => scan_exec(
                data_dir,
                table_name,
                schema,
                projection,
                Some(predicate.clone()),
            ),
            _ => {
                let child = create_physical_plan(input, data_dir)?;
                Ok(Box::new(FilterExec::new(child, predicate.clone())?))
            }
        },
        Projection {
            exprs,
            schema,
            input,
        } => {
            let child = create_physical_plan(input, data_dir)?;
            let pairs: Vec<(crate::planner::Expr, String)> = exprs
                .iter()
                .zip(schema.fields.iter())
                .map(|(e, f)| (e.clone(), f.name.clone()))
                .collect();
            let out_schema: SchemaRef = std::sync::Arc::new(schema.clone().into());
            Ok(Box::new(ProjectionExec::new(child, pairs, out_schema)))
        }
        Aggregate {
            group_by,
            aggr_exprs,
            schema,
            input,
        } => {
            let child = create_physical_plan(input, data_dir)?;
            HashAggregateExec::new(child, group_by.clone(), aggr_exprs.clone(), schema.clone())
        }
        Sort { exprs, input } => {
            let child = create_physical_plan(input, data_dir)?;
            Ok(Box::new(SortExec::new(child, exprs.clone())?))
        }
        Join {
            left,
            right,
            on,
            join_type,
            schema: _,
        } => {
            let l = create_physical_plan(left, data_dir)?;
            let r = create_physical_plan(right, data_dir)?;
            HashJoinExec::new(l, r, on.clone(), *join_type)
        }
        Limit { n, input } => {
            let child = create_physical_plan(input, data_dir)?;
            Ok(Box::new(LimitExec::new(child, *n)))
        }
    }
}

/// Builds a [`ScanExec`], optionally fusing a filter predicate into the Parquet reader.
fn scan_exec(
    data_dir: &Path,
    table_name: &str,
    schema: &crate::types::Schema,
    projection: &Option<Vec<usize>>,
    predicate: Option<Expr>,
) -> Result<Box<dyn PhysicalPlan>> {
    let path: PathBuf = data_dir.join(format!("{table_name}.parquet"));
    let logical_subset: crate::types::Schema = match projection {
        None => schema.clone(),
        Some(idxs) => crate::types::Schema {
            fields: idxs
                .iter()
                .filter_map(|&i| schema.fields.get(i).cloned())
                .collect(),
        },
    };
    let output_schema: SchemaRef = Arc::new(logical_subset.into());
    let full_logical: SchemaRef = Arc::new(schema.clone().into());
    ScanExec::new(
        path,
        output_schema,
        full_logical,
        projection.clone(),
        BATCH_SIZE,
        predicate,
    )
}

/// Resolves a logical column reference to a batch column index.
pub(crate) fn batch_column_index(
    schema: &arrow::datatypes::Schema,
    name: &str,
    relation: Option<&str>,
) -> Result<usize> {
    let rel_owned = relation.map(|s| s.to_string());
    let key = column_key(name, rel_owned.as_ref());
    for (i, f) in schema.fields().iter().enumerate() {
        if f.name() == key.as_str() {
            return Ok(i);
        }
    }
    if relation.is_some() {
        for (i, f) in schema.fields().iter().enumerate() {
            if f.name() == name {
                return Ok(i);
            }
        }
    }
    Err(ArborError::Execution(format!("column not in batch: {key}")))
}

struct EmptyExec {
    schema: SchemaRef,
}

impl PhysicalPlan for EmptyExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

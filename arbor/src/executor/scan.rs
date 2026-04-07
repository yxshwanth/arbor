//! Parquet table scan.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::path::PathBuf;

use crate::error::{ArborError, Result};
use crate::executor::PhysicalPlan;

/// Reads a Parquet file into Arrow batches with optional column projection.
pub struct ScanExec {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    /// Output schema (logical column names, aligned with projected file order).
    schema: SchemaRef,
}

impl ScanExec {
    /// Opens `path` with optional per-file column indices (leaf indices in file order).
    #[allow(clippy::new_ret_no_self)] // returns boxed trait object
    pub fn new(
        path: PathBuf,
        logical_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Box<dyn PhysicalPlan>> {
        let file = File::open(&path)
            .map_err(|e| ArborError::Storage(format!("open {}: {e}", path.display())))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ArborError::Storage(e.to_string()))?;
        let reader = if let Some(indices) = projection {
            let mask = ProjectionMask::leaves(builder.parquet_schema(), indices);
            builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .build()
                .map_err(|e| ArborError::Storage(e.to_string()))?
        } else {
            builder
                .with_batch_size(batch_size)
                .build()
                .map_err(|e| ArborError::Storage(e.to_string()))?
        };
        Ok(Box::new(ScanExec {
            reader,
            schema: logical_schema,
        }))
    }
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => remap_batch_schema(&batch, &self.schema),
            Some(Err(e)) => Err(ArborError::from(e)),
        }
    }
}

fn remap_batch_schema(batch: &RecordBatch, logical: &SchemaRef) -> Result<Option<RecordBatch>> {
    if batch.num_columns() != logical.fields().len() {
        return Err(ArborError::Execution(format!(
            "column mismatch: batch {} logical {}",
            batch.num_columns(),
            logical.fields().len()
        )));
    }
    let cols: Vec<_> = (0..batch.num_columns())
        .map(|i| batch.column(i).clone())
        .collect();
    RecordBatch::try_new(logical.clone(), cols)
        .map(Some)
        .map_err(ArborError::from)
}

//! Parquet table scan with column projection, row-group pruning, and predicate pushdown.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::path::PathBuf;

use crate::error::{ArborError, Result};
use crate::executor::{batch_column_index, evaluate_expr, PhysicalPlan};
use crate::optimizer::collect_columns;
use crate::planner::{BinaryOp, Expr};
use crate::types::ScalarValue;

/// Reads a Parquet file into Arrow batches with optional column projection and filter pushdown.
pub struct ScanExec {
    reader: Option<ParquetRecordBatchReader>,
    /// Output schema (logical column names, aligned with projected file order).
    schema: SchemaRef,
}

/// Summary of min/max row-group pruning against a scan predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowGroupPruneStats {
    /// Total row groups in the file.
    pub total: usize,
    /// Indexes of row groups that may contain matching rows.
    pub kept: Vec<usize>,
}

impl RowGroupPruneStats {
    /// Number of row groups skipped because their statistics cannot satisfy the predicate.
    pub fn skipped(&self) -> usize {
        self.total.saturating_sub(self.kept.len())
    }

    /// Fraction of row groups skipped, in `0.0..=1.0`.
    pub fn skip_fraction(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.skipped() as f64 / self.total as f64
        }
    }
}

impl ScanExec {
    /// Opens `path` with optional per-file column indices (leaf indices in file order).
    ///
    /// `output_schema` is the projected logical schema. `full_logical_schema` is the scan
    /// schema in file column order (names may be join-qualified). When `predicate` is set,
    /// row groups whose min/max statistics cannot satisfy it are skipped, and the predicate
    /// is pushed into the reader as a [`RowFilter`].
    #[allow(clippy::new_ret_no_self)] // returns boxed trait object
    pub fn new(
        path: PathBuf,
        output_schema: SchemaRef,
        full_logical_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        predicate: Option<Expr>,
    ) -> Result<Box<dyn PhysicalPlan>> {
        let file = File::open(&path)
            .map_err(|e| ArborError::Storage(format!("open {}: {e}", path.display())))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ArborError::Storage(e.to_string()))?;

        if let Some(pred) = predicate.as_ref() {
            let prune = prune_row_groups(builder.metadata(), full_logical_schema.as_ref(), pred)?;
            scan_trace(&path, &prune);
            if prune.kept.is_empty() {
                return Ok(Box::new(ScanExec {
                    reader: None,
                    schema: output_schema,
                }));
            }
            builder = builder.with_row_groups(prune.kept);
            if let Some(filter) = build_row_filter(
                builder.parquet_schema(),
                builder.schema(),
                &full_logical_schema,
                pred,
            )? {
                builder = builder.with_row_filter(filter);
            }
        }

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
            reader: Some(reader),
            schema: output_schema,
        }))
    }
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        match reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => remap_batch_schema(&batch, &self.schema),
            Some(Err(e)) => Err(ArborError::from(e)),
        }
    }
}

/// Selects row groups whose column statistics can satisfy `predicate`.
///
/// Conservative: if a conjunct cannot be interpreted against statistics, the row group is kept.
pub fn prune_row_groups(
    metadata: &ParquetMetaData,
    logical_schema: &arrow::datatypes::Schema,
    predicate: &Expr,
) -> Result<RowGroupPruneStats> {
    let total = metadata.num_row_groups();
    let mut kept = Vec::new();
    for i in 0..total {
        if row_group_can_match(metadata.row_group(i), logical_schema, predicate)? {
            kept.push(i);
        }
    }
    Ok(RowGroupPruneStats { total, kept })
}

fn scan_trace(path: &std::path::Path, prune: &RowGroupPruneStats) {
    if std::env::var_os("ARBOR_SCAN_TRACE").is_some() {
        eprintln!(
            "[scan] {} kept {}/{} row groups ({:.0}% skipped)",
            path.display(),
            prune.kept.len(),
            prune.total,
            prune.skip_fraction() * 100.0
        );
    }
}

fn row_group_can_match(
    rg: &RowGroupMetaData,
    logical: &arrow::datatypes::Schema,
    expr: &Expr,
) -> Result<bool> {
    match expr {
        Expr::Alias { expr, .. } => row_group_can_match(rg, logical, expr),
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            Ok(row_group_can_match(rg, logical, left)? && row_group_can_match(rg, logical, right)?)
        }
        Expr::BinaryExpr {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            Ok(row_group_can_match(rg, logical, left)? || row_group_can_match(rg, logical, right)?)
        }
        other => {
            if let Some((idx, op, lit)) = extract_col_cmp(other, logical)? {
                if idx >= rg.num_columns() {
                    return Ok(true);
                }
                Ok(stats_can_match(rg.column(idx).statistics(), op, &lit))
            } else {
                Ok(true)
            }
        }
    }
}

fn extract_col_cmp(
    expr: &Expr,
    logical: &arrow::datatypes::Schema,
) -> Result<Option<(usize, BinaryOp, ScalarValue)>> {
    let expr = match expr {
        Expr::Alias { expr, .. } => expr.as_ref(),
        other => other,
    };
    let Expr::BinaryExpr { left, op, right } = expr else {
        return Ok(None);
    };
    if matches!(
        op,
        BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Plus
            | BinaryOp::Minus
            | BinaryOp::Mul
            | BinaryOp::Div
    ) {
        return Ok(None);
    }
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column { name, relation }, Expr::Literal(v)) => {
            let idx = batch_column_index(logical, name, relation.as_deref())?;
            Ok(Some((idx, *op, v.clone())))
        }
        (Expr::Literal(v), Expr::Column { name, relation }) => {
            let idx = batch_column_index(logical, name, relation.as_deref())?;
            Ok(Some((idx, flip_cmp_op(*op), v.clone())))
        }
        _ => Ok(None),
    }
}

fn flip_cmp_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

fn stats_can_match(stats: Option<&Statistics>, op: BinaryOp, lit: &ScalarValue) -> bool {
    let Some(stats) = stats else {
        return true;
    };
    if !stats.has_min_max_set() {
        return true;
    }
    match (stats, lit) {
        (Statistics::Int64(s), ScalarValue::Int64(v)) => cmp_range(s.min(), s.max(), v, op),
        (Statistics::Double(s), ScalarValue::Float64(v)) => cmp_range(s.min(), s.max(), v, op),
        (Statistics::Boolean(s), ScalarValue::Boolean(v)) => cmp_range(s.min(), s.max(), v, op),
        (Statistics::ByteArray(s), ScalarValue::Utf8(v)) => {
            let Ok(min) = s.min().as_utf8() else {
                return true;
            };
            let Ok(max) = s.max().as_utf8() else {
                return true;
            };
            cmp_range(&min, &max, &v.as_str(), op)
        }
        _ => true,
    }
}

fn cmp_range<T: PartialOrd + PartialEq>(min: &T, max: &T, v: &T, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => v >= min && v <= max,
        BinaryOp::Neq => !(min == max && min == v),
        BinaryOp::Lt => min < v,
        BinaryOp::LtEq => min <= v,
        BinaryOp::Gt => max > v,
        BinaryOp::GtEq => max >= v,
        _ => true,
    }
}

fn build_row_filter(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    file_schema: &arrow::datatypes::Schema,
    logical_schema: &SchemaRef,
    predicate: &Expr,
) -> Result<Option<RowFilter>> {
    let mut leaf_idxs: Vec<usize> = Vec::new();
    for name in collect_columns(predicate) {
        let (base, relation) = split_column_key(&name);
        let idx = batch_column_index(logical_schema, base, relation)?;
        leaf_idxs.push(idx);
    }
    leaf_idxs.sort_unstable();
    leaf_idxs.dedup();
    if leaf_idxs.is_empty() {
        return Ok(None);
    }
    let rewritten = rewrite_expr_file_names(predicate, logical_schema, file_schema)?;
    let mask = ProjectionMask::leaves(parquet_schema, leaf_idxs);
    let pred_fn = ArrowPredicateFn::new(mask, move |batch| {
        let arr = evaluate_expr(&rewritten, &batch)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| ArrowError::ComputeError("scan predicate must be boolean".into()))
    });
    Ok(Some(RowFilter::new(vec![Box::new(pred_fn)])))
}

fn split_column_key(key: &str) -> (&str, Option<&str>) {
    match key.rsplit_once('.') {
        Some((rel, name)) => (name, Some(rel)),
        None => (key, None),
    }
}

fn rewrite_expr_file_names(
    expr: &Expr,
    logical: &arrow::datatypes::Schema,
    file: &arrow::datatypes::Schema,
) -> Result<Expr> {
    match expr {
        Expr::Column { name, relation } => {
            let idx = batch_column_index(logical, name, relation.as_deref())?;
            let file_name = file
                .fields
                .get(idx)
                .ok_or_else(|| {
                    ArborError::Execution(format!("column index {idx} out of file schema"))
                })?
                .name()
                .to_string();
            Ok(Expr::Column {
                name: file_name,
                relation: None,
            })
        }
        Expr::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
            left: Box::new(rewrite_expr_file_names(left, logical, file)?),
            op: *op,
            right: Box::new(rewrite_expr_file_names(right, logical, file)?),
        }),
        Expr::Alias { expr, name } => Ok(Expr::Alias {
            expr: Box::new(rewrite_expr_file_names(expr, logical, file)?),
            name: name.clone(),
        }),
        Expr::AggregateFunc { func, arg } => Ok(Expr::AggregateFunc {
            func: *func,
            arg: Box::new(rewrite_expr_file_names(arg, logical, file)?),
        }),
        other => Ok(other.clone()),
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

#[cfg(test)]
mod tests {
    use super::prune_row_groups;
    use crate::planner::{BinaryOp, Expr};
    use crate::types::ScalarValue;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;

    fn col(name: &str) -> Expr {
        Expr::Column {
            name: name.into(),
            relation: None,
        }
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }

    fn cmp(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    #[test]
    fn prunes_row_groups_from_int_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let mut xs = Vec::with_capacity(400);
        for i in 0..400i64 {
            xs.push(i);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(xs))]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 4);

        // Values 250..280 live only in row group 2 (200..299).
        let pred = cmp(
            cmp(col("x"), BinaryOp::GtEq, lit_i64(250)),
            BinaryOp::And,
            cmp(col("x"), BinaryOp::Lt, lit_i64(280)),
        );
        let stats = prune_row_groups(builder.metadata(), builder.schema().as_ref(), &pred).unwrap();
        assert_eq!(stats.total, 4);
        assert_eq!(stats.kept, vec![2]);
        assert_eq!(stats.skipped(), 3);
    }

    #[test]
    fn selective_date_predicate_skips_most_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lineitem.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_shipdate",
            DataType::Int64,
            false,
        )]));
        let total = 10_000u64;
        let props = WriterProperties::builder()
            .set_max_row_group_size(1_000)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let dates: Vec<i64> = (0..total)
            .map(|i| 5000 + (i * 6000 / total) as i64)
            .collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(dates))]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file = File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 10);
        let pred = cmp(
            cmp(col("l_shipdate"), BinaryOp::GtEq, lit_i64(8000)),
            BinaryOp::And,
            cmp(col("l_shipdate"), BinaryOp::Lt, lit_i64(8600)),
        );
        let stats = prune_row_groups(builder.metadata(), builder.schema().as_ref(), &pred).unwrap();
        assert!(
            stats.skip_fraction() >= 0.7,
            "expected >=70% skip, got {:.0}% (kept {:?})",
            stats.skip_fraction() * 100.0,
            stats.kept
        );
    }

    #[test]
    fn fused_scan_predicate_returns_matching_rows() {
        use super::ScanExec;
        use crate::executor::{collect, BATCH_SIZE};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_max_row_group_size(100)
            .build();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        let xs: Vec<i64> = (0..400).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(xs))]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let pred = cmp(
            cmp(col("x"), BinaryOp::GtEq, lit_i64(250)),
            BinaryOp::And,
            cmp(col("x"), BinaryOp::Lt, lit_i64(280)),
        );
        let mut plan =
            ScanExec::new(path, schema.clone(), schema, None, BATCH_SIZE, Some(pred)).unwrap();
        let batches = collect(&mut *plan).unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 30);
    }
}

//! Shared Parquet generation and pipeline helpers for TPC-H–style Criterion benches.
#![allow(dead_code)]
// `tpch_q6` and `tpch_q1` each `#[path]`-include this file; only a subset of `pub` items is used per binary.

use std::path::{Path, PathBuf};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::black_box;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

use arbor::error::Result;
use arbor::executor;
use arbor::optimizer;
use arbor::parser;
use arbor::planner;
use arbor::types::Catalog;

/// Row count for Q6 (scan + filter + global aggregate); still ~1M-class but slightly below 1M for laptop runs.
pub const ROWS_Q6: u64 = 900_000;

/// Row count for Q1 (grouped aggregates + sort). Kept small so each run is ~1–3s on a laptop; Criterion still needs many iterations for stable stats.
pub const ROWS_Q1: u64 = 5_000;

const BATCH_SIZE: usize = arbor::executor::BATCH_SIZE;

/// Writes a synthetic `lineitem`-shaped Parquet table as `{table_stem}.parquet` under `data_dir`.
pub fn write_lineitem_parquet(path: &Path, total_rows: u64) -> std::io::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Int64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
    ]));
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let mut written: u64 = 0;
    while written < total_rows {
        let n = (total_rows - written).min(BATCH_SIZE as u64) as usize;
        let base = written as i64;
        let mut l_quantity = Vec::with_capacity(n);
        let mut l_extendedprice = Vec::with_capacity(n);
        let mut l_discount = Vec::with_capacity(n);
        let mut l_tax = Vec::with_capacity(n);
        let mut l_shipdate = Vec::with_capacity(n);
        let mut l_returnflag = Vec::with_capacity(n);
        let mut l_linestatus = Vec::with_capacity(n);
        let flags = ["A", "R", "N"];
        let statuses = ["O", "F"];
        for i in 0..n {
            let i64_i = base + i as i64;
            let u = i64_i as u64;
            l_quantity.push(((u % 50) + 1) as f64);
            l_extendedprice.push(((u % 10000) + 1) as f64 * 0.01);
            l_discount.push(((u % 20) as f64) * 0.01);
            l_tax.push(((u % 15) as f64) * 0.01);
            l_shipdate.push(5000 + (i64_i % 6000));
            l_returnflag.push(flags[(u % 3) as usize].to_string());
            l_linestatus.push(statuses[(u % 2) as usize].to_string());
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(l_quantity)),
                Arc::new(Float64Array::from(l_extendedprice)),
                Arc::new(Float64Array::from(l_discount)),
                Arc::new(Float64Array::from(l_tax)),
                Arc::new(Int64Array::from(l_shipdate)),
                Arc::new(StringArray::from(l_returnflag)),
                Arc::new(StringArray::from(l_linestatus)),
            ],
        )
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        written += n as u64;
    }
    writer
        .close()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}

/// Temp dir (guard), data directory path, and catalog with one table named `table_stem`.
pub fn setup_lineitem(rows: u64, table_stem: &str) -> (tempfile::TempDir, PathBuf, Catalog) {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let path = data_dir.join(format!("{table_stem}.parquet"));
    write_lineitem_parquet(&path, rows).expect("write lineitem parquet");
    let catalog = arbor::storage::build_catalog(&data_dir).expect("catalog");
    (dir, data_dir, catalog)
}

/// Parse → plan → optional optimize → execute; discard result shape via `black_box`.
pub fn run_query(sql: &str, catalog: &Catalog, data_dir: &Path, optimize: bool) -> Result<()> {
    let stmt = parser::parse_sql(sql)?;
    let plan = planner::plan_query(&stmt, catalog)?;
    let plan = if optimize {
        optimizer::optimize(plan)?
    } else {
        plan
    };
    let mut phys = executor::create_physical_plan(&plan, data_dir)?;
    let batches = executor::collect(&mut *phys)?;
    black_box(batches);
    Ok(())
}

/// Q6-shaped scan + selective filter + global `SUM` of an expression.
pub const SQL_Q6: &str = r#"SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= 8000 AND l_shipdate < 8600
  AND l_discount >= 0.05 AND l_discount <= 0.07
  AND l_quantity < 24.0"#;

/// Q1-shaped grouped aggregates + filter + sort (uses `lineitem_q1` table; smaller Parquet than Q6).
pub const SQL_Q1: &str = r#"SELECT l_returnflag, l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem_q1
WHERE l_shipdate <= 10500
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus"#;

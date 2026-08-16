//! End-to-end pipeline tests over Parquet fixtures.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;

use arbor::error::Result;
use arbor::executor;
use arbor::optimizer;
use arbor::parser;
use arbor::planner;
use arbor::types::Catalog;

fn write_parquet(path: &Path, schema: Arc<Schema>, batches: &[RecordBatch]) {
    let file = File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
}

fn mk_fixture_dir() -> (tempfile::TempDir, PathBuf, Catalog) {
    let dir = tempfile::tempdir().unwrap();
    let dp = dir.path().to_path_buf();

    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut ages = Vec::new();
    let mut cities = Vec::new();
    let city_choices = ["NYC", "LA", "CHI", "HOU", "PHX"];
    for i in 0..100i64 {
        ids.push(i + 1);
        names.push(format!("user{i}"));
        ages.push(18 + (i % 50));
        cities.push(city_choices[(i as usize) % city_choices.len()].to_string());
    }
    let ub = RecordBatch::try_new(
        users_schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(ages)),
            Arc::new(StringArray::from(cities)),
        ],
    )
    .unwrap();
    write_parquet(
        &dp.join("users.parquet"),
        users_schema,
        std::slice::from_ref(&ub),
    );

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let mut oids = Vec::new();
    let mut uids = Vec::new();
    let mut amounts = Vec::new();
    let mut statuses = Vec::new();
    for i in 0..500i64 {
        oids.push(i + 1);
        uids.push((i % 100) + 1);
        amounts.push(10.0 + ((i * 7) % 200) as f64);
        statuses.push(if i % 2 == 0 { "ok" } else { "pending" }.to_string());
    }
    let ob = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(Int64Array::from(uids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(statuses)),
        ],
    )
    .unwrap();
    write_parquet(
        &dp.join("orders.parquet"),
        orders_schema,
        std::slice::from_ref(&ob),
    );

    let catalog = arbor::storage::build_catalog(&dp).unwrap();
    (dir, dp, catalog)
}

fn run_query(sql: &str, catalog: &Catalog, data_dir: &Path, opt: bool) -> Result<Vec<RecordBatch>> {
    let stmt = parser::parse_sql(sql)?;
    let plan = planner::plan_query(&stmt, catalog)?;
    let plan = if opt {
        optimizer::optimize(plan)?
    } else {
        plan
    };
    let mut p = executor::create_physical_plan(&plan, data_dir)?;
    executor::collect(&mut *p)
}

#[test]
fn integration_scan_star() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query("SELECT * FROM users", &cat, &dp, true).unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 100);
    assert_eq!(batches[0].num_columns(), 4);
}

#[test]
fn integration_filter_project() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query(
        "SELECT name, age FROM users WHERE age > 50",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    let age_idx = 1;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(n > 0);
    for b in &batches {
        let ages = b
            .column(age_idx)
            .as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..b.num_rows() {
            assert!(ages.value(i) > 50);
        }
    }
}

#[test]
fn integration_group_aggregate() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query(
        "SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 5);
}

#[test]
fn integration_full_agg() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query("SELECT SUM(amount), COUNT(*) FROM orders", &cat, &dp, true).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[test]
fn integration_join_filter() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100.0",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(n > 0);
    let amt = batches[0]
        .column(1)
        .as_primitive::<arrow::datatypes::Float64Type>();
    for i in 0..batches[0].num_rows() {
        assert!(amt.value(i) > 100.0);
    }
}

#[test]
fn integration_sort_limit() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query(
        "SELECT name, age FROM users ORDER BY age DESC LIMIT 10",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 10);
    let ages = batches[0]
        .column(1)
        .as_primitive::<arrow::datatypes::Int64Type>();
    for i in 1..ages.len() {
        assert!(ages.value(i - 1) >= ages.value(i));
    }
}

#[test]
fn integration_complex_query() {
    let (_d, dp, cat) = mk_fixture_dir();
    let batches = run_query(
        "SELECT u.city, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.city ORDER BY total DESC LIMIT 5",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 5);
}

#[test]
fn optimizer_equivalence_filter_project() {
    let (_d, dp, cat) = mk_fixture_dir();
    let a = run_query(
        "SELECT name, age FROM users WHERE age > 50",
        &cat,
        &dp,
        false,
    )
    .unwrap();
    let b = run_query(
        "SELECT name, age FROM users WHERE age > 50",
        &cat,
        &dp,
        true,
    )
    .unwrap();
    assert_eq!(
        a.iter().map(|x| x.num_rows()).sum::<usize>(),
        b.iter().map(|x| x.num_rows()).sum::<usize>()
    );
}

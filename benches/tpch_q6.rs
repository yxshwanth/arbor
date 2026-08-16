//! Q6-style benchmark: large `lineitem` scan + filter + global aggregate (optimizer on vs off).
//!
//! Daily smoke: `cargo bench --bench tpch_q6 -- --quick`

#[path = "tpch_shared.rs"]
mod shared;

use std::fs::File;
use std::time::Duration;

use arbor::executor::prune_row_groups;
use arbor::planner::{BinaryOp, Expr};
use arbor::types::ScalarValue;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn col(name: &str) -> Expr {
    Expr::Column {
        name: name.into(),
        relation: None,
    }
}

fn q6_scan_predicate() -> Expr {
    let ship = Expr::BinaryExpr {
        left: Box::new(Expr::BinaryExpr {
            left: Box::new(col("l_shipdate")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(8000))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryExpr {
            left: Box::new(col("l_shipdate")),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(8600))),
        }),
    };
    let disc = Expr::BinaryExpr {
        left: Box::new(Expr::BinaryExpr {
            left: Box::new(col("l_discount")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Float64(0.05))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryExpr {
            left: Box::new(col("l_discount")),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::Literal(ScalarValue::Float64(0.07))),
        }),
    };
    let qty = Expr::BinaryExpr {
        left: Box::new(col("l_quantity")),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Literal(ScalarValue::Float64(24.0))),
    };
    Expr::BinaryExpr {
        left: Box::new(Expr::BinaryExpr {
            left: Box::new(ship),
            op: BinaryOp::And,
            right: Box::new(disc),
        }),
        op: BinaryOp::And,
        right: Box::new(qty),
    }
}

fn bench_tpch_q6(c: &mut Criterion) {
    let (_guard, data_dir, catalog) = shared::setup_lineitem(shared::ROWS_Q6, "lineitem");
    let parquet_path = data_dir.join("lineitem.parquet");
    let file = File::open(&parquet_path).expect("open lineitem");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
    let prune = prune_row_groups(
        builder.metadata(),
        builder.schema().as_ref(),
        &q6_scan_predicate(),
    )
    .expect("prune");
    eprintln!(
        "Q6 row-group pruning: kept {}/{} ({:.1}% skipped)",
        prune.kept.len(),
        prune.total,
        prune.skip_fraction() * 100.0
    );

    let mut g = c.benchmark_group("tpch_q6_lineitem");
    g.throughput(Throughput::Elements(shared::ROWS_Q6));
    g.sample_size(12);
    g.measurement_time(Duration::from_secs(5));
    g.warm_up_time(Duration::from_secs(1));

    for optimize in [false, true] {
        g.bench_with_input(
            BenchmarkId::new(
                "pipeline",
                if optimize {
                    "optimize_on"
                } else {
                    "optimize_off"
                },
            ),
            &optimize,
            |b, &optimize| {
                b.iter(|| {
                    shared::run_query(shared::SQL_Q6, &catalog, &data_dir, optimize).expect("q6");
                });
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_tpch_q6);
criterion_main!(benches);

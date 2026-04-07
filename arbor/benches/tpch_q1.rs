//! Q1-style benchmark: smaller `lineitem_q1` table, grouped aggregates + sort (optimizer on vs off).
//!
//! Lighter Criterion settings for laptops. Daily: `cargo bench --bench tpch_q1 -- --quick`

#[path = "tpch_shared.rs"]
mod shared;

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn criterion_q1() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(25))
        .warm_up_time(Duration::from_secs(1))
}

fn bench_tpch_q1(c: &mut Criterion) {
    let (_guard, data_dir, catalog) = shared::setup_lineitem(shared::ROWS_Q1, "lineitem_q1");

    let mut g = c.benchmark_group("tpch_q1_lineitem_scaled");
    g.throughput(Throughput::Elements(shared::ROWS_Q1));

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
                    shared::run_query(shared::SQL_Q1, &catalog, &data_dir, optimize).expect("q1");
                });
            },
        );
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = criterion_q1();
    targets = bench_tpch_q1
}
criterion_main!(benches);

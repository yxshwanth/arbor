//! Q6-style benchmark: large `lineitem` scan + filter + global aggregate (optimizer on vs off).
//!
//! Daily smoke: `cargo bench --bench tpch_q6 -- --quick`

#[path = "tpch_shared.rs"]
mod shared;

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn bench_tpch_q6(c: &mut Criterion) {
    let (_guard, data_dir, catalog) = shared::setup_lineitem(shared::ROWS_Q6, "lineitem");

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

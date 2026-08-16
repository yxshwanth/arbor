# Arbor

Columnar SQL engine over Parquet in Rust — hand-written planner, rule-based optimizer, Volcano executor, **no DataFusion**. Row-group pruning from Parquet statistics skips **89%** of I/O on selective scans; hoisting per-row expression evaluation out of the join and aggregate key paths cut TPC-H Q1 from **52 ms to 1.2 ms**.

Arbor parses a SQL subset, builds a **logical plan**, runs a **rule-based optimizer**, executes with **pull-based physical operators**, and materializes **Arrow record batches**.

**Non-goal:** Arbor does **not** use DataFusion, Polars SQL, or other query-engine crates for plan execution.

---

## Table of contents

1. [Features & SQL subset](#features--sql-subset)
2. [Project constraints](#project-constraints)
3. [Repository layout](#repository-layout)
4. [Requirements](#requirements)
5. [Quick start (CLI)](#quick-start-cli)
6. [Using the library](#using-the-library)
7. [Architecture](#architecture)
8. [Optimizer](#optimizer)
9. [Testing, linting, and docs](#testing-linting-and-docs)
10. [Benchmarks](#benchmarks)
11. [Toolchain notes](#toolchain-notes)
12. [Continuous integration](#continuous-integration)
13. [License](#license)

---

## Features & SQL subset

Supported today:

| Area | Support |
|------|---------|
| Queries | `SELECT` only (no `INSERT`, `WITH`, etc.) |
| `FROM` | Single join tree: `INNER JOIN … ON` with **equi** predicates (AND of equalities) |
| Aliases | Table aliases; qualified columns (`u.id`) |
| Filters | `WHERE` with boolean expressions |
| Aggregates | `SUM`, `COUNT`, `COUNT(*)`, `AVG`, `MIN`, `MAX` with `GROUP BY` |
| Ordering / limit | `ORDER BY`, `LIMIT` |
| Types in plans | Int64, Float64, Utf8, Boolean, NULL literals (as used by Parquet / planner) |
| Scan pushdown | Column projection, min/max **row-group pruning**, and Parquet `RowFilter` |

**Not supported** (non-exhaustive): outer joins at execution, subqueries, `HAVING`, `DISTINCT`, set operations, DDL, transactions, indexes, cost-based optimization.

**Type tip for SQL literals:** comparisons and arithmetic must match Arrow types (e.g. compare a float column to `24.0`, not `24`; use `1.0 - discount`, not `1 - discount` when `discount` is float).

---

## Project constraints

These rules match the intended design of the codebase:

| Topic | Rule |
|--------|------|
| Query engine | Built in-house; **no** DataFusion (or similar) as a dependency |
| Runtime crates | `arrow`, `parquet`, `sqlparser`, `thiserror` (plus pinned `chrono` for Arrow 51) |
| Dev crates | `criterion`, `tempfile` |
| Batch size | **8192** rows standard for scan / operator batching (`executor::BATCH_SIZE`) |
| Errors | Library code in `src/` returns `Result<T, ArborError>`; avoid `unwrap` / `expect` in library code |
| Module boundaries | Parser must not depend on execution/storage internals; planner must not depend on Parquet I/O; executor must not depend on raw SQL strings / parser AST |
| Public API | Public items carry `///` documentation; `cargo doc` is checked with `RUSTDOCFLAGS='-D warnings'` in CI |

---

## Repository layout

```
Arbor/
├── Cargo.toml
├── LICENSE                      # Apache-2.0
├── README.md
├── .github/workflows/ci.yml
├── src/
│   ├── lib.rs
│   ├── main.rs                  # CLI binary
│   ├── parser/
│   ├── planner/
│   ├── optimizer/
│   ├── executor/
│   └── storage/
├── tests/
├── benches/
└── data/                        # CLI Parquet files (gitignored contents)
```

---

## Requirements

- **Rust**: stable toolchain (2021 edition), with `rustfmt` and `clippy` for local/CI checks.
- **Data**: Parquet files for the CLI live under `data/` (see below).

---

## Quick start (CLI)

1. Place `*.parquet` files in `data/`. The file **stem** is the table name (e.g. `data/users.parquet` → table `users`).

2. Build and run from the repository root:

```bash
cargo build --release
echo "SELECT * FROM users LIMIT 5" | cargo run --release
```

3. **Inline SQL** (non-REPL):

```bash
cargo run --release -- "SELECT count(*) FROM users"
```

4. **Explain** logical plans (parse + plan + optimize), no execution:

```bash
cargo run --release -- --explain "SELECT a FROM t"
```

5. **REPL**: run with no SQL arguments; type queries at the `>` prompt; `exit` / `quit` to leave.

The CLI runs the **optimizer** on the logical plan before execution for normal queries (not for `--explain`, which prints before/after optimize).

---

## Using the library

Typical pipeline (see `src/main.rs` for a full example):

1. `storage::build_catalog(data_dir)` — discover `*.parquet` and load schemas.
2. `parser::parse_sql(sql)` — SELECT-only.
3. `planner::plan_query(&stmt, &catalog)` — AST → `LogicalPlan`.
4. `optimizer::optimize(plan)` — optional but recommended.
5. `executor::create_physical_plan(&plan, data_dir)` then `executor::collect(&mut *phys)` — run query.

A `Filter` sitting directly on a `Scan` is fused into `ScanExec`: Parquet row-group statistics skip groups that cannot match, and the predicate is pushed into the reader as a `RowFilter`.

Public modules: `error`, `parser`, `planner`, `optimizer`, `executor`, `storage`, `types`. Run `cargo doc --no-deps --open` for API details.

---

## Architecture

```
  +------+     +--------+     +--------------+     +----------+     +----------------+     +---------+
  | SQL  | --> | Parser | --> | Logical plan | --> | Optimize | --> | Physical plan  | --> | Batches |
  +------+     +--------+     +--------------+     +----------+     +----------------+     +---------+
```

- **Volcano / iterator model:** each operator implements `PhysicalPlan::next_batch()` and pulls from its child.
- **Arrow:** columnar batches; expression evaluation and aggregates use Arrow compute kernels where applicable.
- **Parquet:** table scans read via `parquet`’s Arrow reader; column projection, row-group pruning, and `RowFilter` are pushed into the reader.

---

## Optimizer

Default rule pipeline (order matters):

1. **Predicate pushdown** — move filters toward scans and join inputs when safe.
2. **Projection pruning** — narrow scan columns and simplify projections.
3. **Constant folding** — fold constant expressions; replace always-false filters with empty plans.

**Debug traces:** set `ARBOR_OPTIMIZER_TRACE` to print rule activity on stderr; set `ARBOR_SCAN_TRACE` to print row-group keep/skip counts.

---

## Testing, linting, and docs

From the repository root:

| Command | Purpose |
|---------|---------|
| `cargo test` | Unit, integration, and optimizer tests |
| `cargo fmt` | Format (use `cargo fmt --check` in CI) |
| `cargo clippy --all-targets -- -D warnings` | Lint with warnings denied |
| `RUSTDOCFLAGS='-D warnings' cargo doc --no-deps` | API docs; fails on broken links / doc warnings |

---

## Benchmarks

Synthetic TPC-H–**style** workloads (not official TPC-H data). Criterion compares **optimizer on vs off**. Measured on an Apple silicon laptop; `--quick` Criterion settings.

| Bench | Command | Result |
|-------|---------|--------|
| Q6-shaped (scan + filter + global aggregate, 900k rows) | `cargo bench --bench tpch_q6` | **1.85 ms** with optimizer on; row-group pruning keeps **12 / 110** groups (**89%** skipped) |
| Q1-shaped (grouped aggregates + sort, 900k rows) | `cargo bench --bench tpch_q1` | **179 ms** with optimizer on |

Q1 at 5k rows was **52 ms** before hoisting expression evaluation out of the per-row aggregate loop, and **1.2 ms** after (~44×). Both benches now use 900k rows.

**Daily / laptop:** append `-- --quick` to either command. Shared setup: `benches/tpch_shared.rs`.

---

## Toolchain notes

- **`chrono = "=0.4.38"`** is pinned in `Cargo.toml` so **Arrow 51** / `arrow-arith` compiles cleanly (avoids `Datelike::quarter` ambiguity with newer `chrono`).

---

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs:

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `RUSTDOCFLAGS='-D warnings' cargo doc --no-deps`

**Benchmarks are not run in CI** by default (time and determinism).

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

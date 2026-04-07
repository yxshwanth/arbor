# Arbor

Arbor is a small **from-scratch** SQL query engine over **Apache Arrow** and **Apache Parquet**: parse a SQL subset, build a **logical plan**, run a **rule-based optimizer**, execute with **pull-based physical operators**, and materialize **record batches**. It is intended for learning and for owning the full planner → executor path without pulling in a full query engine.

**Non-goal:** Arbor does **not** use DataFusion, Polars SQL, or other “batteries-included” query-engine crates for plan execution.

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
├── .github/workflows/ci.yml   # optional CI (fmt, clippy -D warnings, test, doc)
└── arbor/                     # Rust crate root
    ├── Cargo.toml
    ├── README.md                # this file
    ├── src/
    │   ├── lib.rs               # crate entry, module exports
    │   ├── main.rs              # CLI binary
    │   ├── error.rs
    │   ├── types.rs
    │   ├── parser/
    │   ├── planner/
    │   ├── optimizer/
    │   ├── executor/
    │   └── storage/
    ├── tests/                   # integration + optimizer tests
    └── benches/
        ├── tpch_shared.rs
        ├── tpch_q6.rs
        └── tpch_q1.rs
```

---

## Requirements

- **Rust**: stable toolchain (2021 edition), with `rustfmt` and `clippy` for local/CI checks.
- **Data**: Parquet files for the CLI live under `data/` (see below).

---

## Quick start (CLI)

1. Place `*.parquet` files in `arbor/data/`. The file **stem** is the table name (e.g. `data/users.parquet` → table `users`).

2. Build and run from the **`arbor`** directory:

```bash
cd arbor
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

Public modules: `error`, `parser`, `planner`, `optimizer`, `executor`, `storage`, `types`. Run `cargo doc --no-deps --open` for API details.

---

## Architecture

```
  +------+     +--------+     +-------------+     +----------+     +----------------+     +--------+
  | SQL  | --> | Parser | --> | Logical plan | --> | Optimize | --> | Physical plan  | --> | Batches |
  +------+     +--------+     +-------------+     +----------+     +----------------+     +--------+
```

- **Volcano / iterator model:** each operator implements `PhysicalPlan::next_batch()` and pulls from its child.
- **Arrow:** columnar batches; expression evaluation and aggregates use Arrow compute kernels where applicable.
- **Parquet:** table scans read via `parquet`’s Arrow reader; optional column projection is pushed into the reader.

---

## Optimizer

Default rule pipeline (order matters):

1. **Predicate pushdown** — move filters toward scans and join inputs when safe.
2. **Projection pruning** — narrow scan columns and simplify projections.
3. **Constant folding** — fold constant expressions; replace always-false filters with empty plans.

**Debug trace:** set environment variable `ARBOR_OPTIMIZER_TRACE` (any value) to print rule activity on stderr.

---

## Testing, linting, and docs

From the `arbor` directory:

| Command | Purpose |
|---------|---------|
| `cargo test` | Unit, integration, and optimizer tests |
| `cargo fmt` | Format (use `cargo fmt --check` in CI) |
| `cargo clippy --all-targets -- -D warnings` | Lint with warnings denied |
| `RUSTDOCFLAGS='-D warnings' cargo doc --no-deps` | API docs; fails on broken links / doc warnings |

---

## Benchmarks

Synthetic TPC-H–**style** workloads (not official TPC-H data). Criterion compares **optimizer on vs off**.

| Bench | Command | Notes |
|-------|---------|--------|
| Q6-shaped (scan + filter + global aggregate) | `cargo bench --bench tpch_q6` | ~900k rows in generated Parquet |
| Q1-shaped (grouped aggregates + sort) | `cargo bench --bench tpch_q1` | Default **5k** rows for fast laptops; raise `ROWS_Q1` in `benches/tpch_shared.rs` for heavier runs |

**Daily / laptop:** append `-- --quick` to either command. Shared setup: `benches/tpch_shared.rs`.

---

## Toolchain notes

- **`chrono = "=0.4.38"`** is pinned in `Cargo.toml` so **Arrow 51** / `arrow-arith` compiles cleanly (avoids `Datelike::quarter` ambiguity with newer `chrono`).

---

## Continuous integration

If this repo is hosted on GitHub, `.github/workflows/ci.yml` runs (from the **`arbor`** subdirectory):

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `RUSTDOCFLAGS='-D warnings' cargo doc --no-deps`

**Benchmarks are not run in CI** by default (time and determinism).

Adjust branch names in `on:` if your default branch is not `main` / `master`.

# Arbor

From-scratch SQL query engine over **Apache Arrow** and **Apache Parquet** (no DataFusion).

**Full documentation** (features, layout, CLI, library API, optimizer, tests, benchmarks, CI): **[arbor/README.md](arbor/README.md)**.

Quick start:

```bash
cd arbor
cargo test
cargo build --release
```

GitHub Actions workflow (format, clippy `-D warnings`, test, doc): [.github/workflows/ci.yml](.github/workflows/ci.yml).

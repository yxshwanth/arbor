//! Arbor: small SQL query engine over Arrow/Parquet (no external query engine crates).
//!
//! Pipeline: parse SQL → logical plan → optimize → physical operators → record batches.
#![warn(missing_docs)]

/// Error types and the [`crate::error::Result`] alias.
pub mod error;
/// Physical execution and expression evaluation.
pub mod executor;
/// Logical plan rewrites.
pub mod optimizer;
/// SQL text → sqlparser AST (SELECT-only).
pub mod parser;
/// AST → logical plan.
pub mod planner;
/// Parquet schema and catalog helpers.
pub mod storage;
/// Shared logical types (schema, scalars).
pub mod types;

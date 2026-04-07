//! Central error type for the Arbor query engine.

use thiserror::Error;

/// Errors that can occur while parsing, planning, executing, or reading storage.
#[derive(Debug, Error)]
pub enum ArborError {
    /// SQL parsing failed.
    #[error("parse error: {0}")]
    Parse(String),
    /// Logical planning failed (e.g. unknown relation or invalid types).
    #[error("plan error: {0}")]
    Plan(String),
    /// Runtime execution failed.
    #[error("execution error: {0}")]
    Execution(String),
    /// Parquet or catalog I/O failed.
    #[error("storage error: {0}")]
    Storage(String),
    /// Type coercion or mismatch.
    #[error("type error: {0}")]
    Type(String),
}

impl From<sqlparser::parser::ParserError> for ArborError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        ArborError::Parse(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for ArborError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ArborError::Execution(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for ArborError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        ArborError::Storage(e.to_string())
    }
}

/// Convenient [`Result`] alias using [`ArborError`] as the error type.
pub type Result<T> = std::result::Result<T, ArborError>;

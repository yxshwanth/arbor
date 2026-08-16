//! Logical schema and scalar value types shared across planner and execution.

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

use crate::error::{ArborError, Result};

/// A single scalar value used in expressions and aggregation keys.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit float; [`Hash`] uses [`f64::to_bits`].
    Float64(f64),
    /// UTF-8 string.
    Utf8(String),
    /// Boolean.
    Boolean(bool),
    /// SQL NULL.
    Null,
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a == b,
            (ScalarValue::Float64(a), ScalarValue::Float64(b)) => a.to_bits() == b.to_bits(),
            (ScalarValue::Utf8(a), ScalarValue::Utf8(b)) => a == b,
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Null, ScalarValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Int64(v) => v.hash(state),
            ScalarValue::Float64(v) => v.to_bits().hash(state),
            ScalarValue::Utf8(v) => v.hash(state),
            ScalarValue::Boolean(v) => v.hash(state),
            ScalarValue::Null => {
                0u8.hash(state);
            }
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Int64(v) => write!(f, "{v}"),
            ScalarValue::Float64(v) => write!(f, "{v}"),
            ScalarValue::Utf8(v) => write!(f, "'{v}'"),
            ScalarValue::Boolean(v) => write!(f, "{v}"),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

/// Named field with Arrow logical type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// Column name.
    pub name: String,
    /// Arrow [`ArrowDataType`] for this field.
    pub data_type: ArrowDataType,
}

/// Ordered list of fields describing a relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Fields in column order.
    pub fields: Vec<Field>,
}

impl Schema {
    /// Looks up a field by name (case-sensitive); returns index and field if found.
    pub fn field_by_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields.iter().enumerate().find(|(_, f)| f.name == name)
    }

    /// Returns the index of column `name` or a plan error if not found.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        self.field_by_name(name)
            .map(|(i, _)| i)
            .ok_or_else(|| ArborError::Plan(format!("unknown column '{name}' in schema")))
    }
}

impl From<&ArrowSchema> for Schema {
    fn from(arrow: &ArrowSchema) -> Self {
        let fields = arrow
            .fields()
            .iter()
            .map(|f| Field {
                name: f.name().to_string(),
                data_type: f.data_type().clone(),
            })
            .collect();
        Schema { fields }
    }
}

impl From<Schema> for ArrowSchema {
    fn from(schema: Schema) -> Self {
        let fields: Vec<ArrowField> = schema
            .fields
            .into_iter()
            .map(|f| ArrowField::new(f.name, f.data_type, true))
            .collect();
        ArrowSchema::new(fields)
    }
}

/// Table name → relation schema (e.g. built from Parquet files in a directory).
pub type Catalog = std::collections::HashMap<String, Schema>;

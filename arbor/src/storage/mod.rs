//! Parquet-backed catalog helpers.

use std::fs::File;
use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{ArborError, Result};
use crate::types::{Catalog, Schema};

/// Reads the Arrow schema embedded in a Parquet file's footer.
pub fn read_schema(path: &Path) -> Result<Schema> {
    let file = File::open(path).map_err(|e| ArborError::Storage(format!("open: {e}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ArborError::Storage(e.to_string()))?;
    Ok(Schema::from(builder.schema().as_ref()))
}

/// Builds a table catalog: each `*.parquet` stem maps to that file's schema.
pub fn build_catalog(data_dir: &Path) -> Result<Catalog> {
    let mut catalog = Catalog::new();
    let entries = std::fs::read_dir(data_dir)
        .map_err(|e| ArborError::Storage(format!("read_dir {}: {e}", data_dir.display())))?;
    for ent in entries {
        let ent = ent.map_err(|e| ArborError::Storage(e.to_string()))?;
        let path: PathBuf = ent.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| ArborError::Storage("bad file name".into()))?;
        let schema = read_schema(&path)?;
        catalog.insert(stem.to_string(), schema);
    }
    Ok(catalog)
}

#[cfg(test)]
mod tests {
    use super::{build_catalog, read_schema};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    #[test]
    fn roundtrip_schema_from_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, arrow.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let s = read_schema(&path).unwrap();
        assert_eq!(s.fields.len(), 1);
        assert_eq!(s.fields[0].name, "id");
        let cat = build_catalog(dir.path()).unwrap();
        assert!(cat.contains_key("t"));
    }
}

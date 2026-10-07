//! Hive-partitioned Parquet on disk: a directory of `key=value` directories read as
//! one scan, with the keys as columns.

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use polars::io::HiveOptions;
use polars::prelude::*;

/// Build a LazyFrame for hive-partitioned Parquet only (no schema collection, no partition discovery).
/// Use this for phased loading so "Scanning input" is instant; schema and partition handling are the schema phase's.
pub fn scan_parquet_hive(path: &Path) -> Result<LazyFrame> {
    let is_glob = crate::source::expands_as_glob(path);
    let pl_path = PlRefPath::try_from_path(path)?;
    let args = ScanArgsParquet {
        hive_options: HiveOptions::new_enabled(),
        glob: is_glob,
        ..Default::default()
    };
    LazyFrame::scan_parquet(pl_path, args).map_err(Into::into)
}

/// Build a LazyFrame for hive-partitioned Parquet with a pre-computed schema (avoids slow collect_schema across all files).
pub fn scan_parquet_hive_with_schema(path: &Path, schema: Arc<Schema>) -> Result<LazyFrame> {
    let is_glob = crate::source::expands_as_glob(path);
    let pl_path = PlRefPath::try_from_path(path)?;
    let args = ScanArgsParquet {
        schema: Some(schema),
        hive_options: HiveOptions::new_enabled(),
        glob: is_glob,
        ..Default::default()
    };
    LazyFrame::scan_parquet(pl_path, args).map_err(Into::into)
}

/// The first Parquet file along one spine of a hive directory (as partition discovery
/// walks); `None` if there is none.
fn first_parquet_file_in_hive_dir(path: &Path) -> Option<std::path::PathBuf> {
    const MAX_DEPTH: usize = 64;
    first_parquet_file_spine(path, 0, MAX_DEPTH)
}

fn first_parquet_file_spine(
    path: &Path,
    depth: usize,
    max_depth: usize,
) -> Option<std::path::PathBuf> {
    if depth >= max_depth {
        return None;
    }
    let entries = fs::read_dir(path).ok()?;
    let mut first_partition_child: Option<std::path::PathBuf> = None;
    for entry in entries.flatten() {
        let child = entry.path();
        if child.is_file() {
            if crate::discover::is_parquet_path(&child) {
                return Some(child);
            }
        } else if child.is_dir()
            && let Some(name) = child.file_name().and_then(|n| n.to_str())
            && name.contains('=')
            && first_partition_child.is_none()
        {
            first_partition_child = Some(child);
        }
    }
    first_partition_child.and_then(|p| first_parquet_file_spine(&p, depth + 1, max_depth))
}

/// Read schema from a single parquet file (metadata only, no data scan). Used to avoid collect_schema() over many files.
fn read_schema_from_single_parquet(path: &Path) -> Result<Arc<Schema>> {
    let file = File::open(path)?;
    let mut reader = ParquetReader::new(file);
    let arrow_schema = reader.schema()?;
    let schema = Schema::from_arrow_schema(arrow_schema.as_ref());
    Ok(Arc::new(schema))
}

/// The schema of one Parquet file in a hive directory (not a glob) merged with the
/// partition columns, returned with them; spares `collect_schema()` when used with
/// `scan_parquet_hive_with_schema`. Errors if no file is found or it fails to read.
pub fn schema_from_one_hive_parquet(path: &Path) -> Result<(Arc<Schema>, Vec<String>)> {
    let partition_columns = discover_hive_partition_columns(path);
    let one_file = first_parquet_file_in_hive_dir(path)
        .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in hive directory"))?;
    let file_schema = read_schema_from_single_parquet(&one_file)?;
    let values = hive_partition_values(path, &one_file);
    let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut merged = Schema::with_capacity(partition_columns.len() + file_schema.len());
    for name in &partition_columns {
        merged.with_column(
            name.clone().into(),
            partition_dtype(name, &file_schema, &values),
        );
    }
    for (name, dtype) in file_schema.iter() {
        if !part_set.contains(name.as_str()) {
            merged.with_column(name.clone(), dtype.clone());
        }
    }
    Ok((Arc::new(merged), partition_columns))
}

/// `key=value` names of the partition directories beside each one on the path to `file`.
fn hive_partition_values(root: &Path, file: &Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(rel) = file.strip_prefix(root).ok().and_then(Path::parent) else {
        return out;
    };
    let mut dir = root.to_path_buf();
    for component in rel.components() {
        let Some(segment) = component.as_os_str().to_str() else {
            break;
        };
        if let Some((key, _)) = segment.split_once('=') {
            for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
                let name = entry.file_name();
                let Some((k, v)) = name.to_str().and_then(|n| n.split_once('=')) else {
                    continue;
                };
                if k == key && entry.path().is_dir() {
                    out.push((k.to_string(), v.to_string()));
                }
            }
        }
        dir.push(segment);
    }
    out
}

/// Discover hive partition column names (public for phased loading). Directory: single-spine walk; glob: parse pattern.
pub fn discover_hive_partition_columns(path: &Path) -> Vec<String> {
    if path.is_dir() {
        discover_partition_columns_from_path(path)
    } else {
        discover_partition_columns_from_glob_pattern(path)
    }
}

/// Discover hive partition column names from a directory path by walking a single
/// "spine" (one branch) of key=value directories. Partition keys are uniform across
/// the tree, so we only need one path to infer [year, month, day] etc. Returns columns
/// in path order. Stops after max_depth levels to avoid runaway on malformed trees.
fn discover_partition_columns_from_path(path: &Path) -> Vec<String> {
    const MAX_PARTITION_DEPTH: usize = 64;
    let mut columns = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    discover_partition_columns_spine(path, &mut columns, &mut seen, 0, MAX_PARTITION_DEPTH);
    columns
}

/// Walk one branch: at this directory, find the first child that is a key=value dir,
/// record the key (if not already seen), then recurse into that one child only.
/// This does O(depth) read_dir calls instead of walking the entire tree.
fn discover_partition_columns_spine(
    path: &Path,
    columns: &mut Vec<String>,
    seen: &mut HashSet<String>,
    depth: usize,
    max_depth: usize,
) {
    if depth >= max_depth {
        return;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    let mut first_partition_child: Option<std::path::PathBuf> = None;
    for entry in entries.flatten() {
        let child = entry.path();
        if child.is_dir()
            && let Some(name) = child.file_name().and_then(|n| n.to_str())
            && let Some((key, _)) = name.split_once('=')
        {
            if !key.is_empty() && seen.insert(key.to_string()) {
                columns.push(key.to_string());
            }
            if first_partition_child.is_none() {
                first_partition_child = Some(child);
            }
            break;
        }
    }
    if let Some(one) = first_partition_child {
        discover_partition_columns_spine(&one, columns, seen, depth + 1, max_depth);
    }
}

/// Infer partition column names from a glob pattern path (e.g. "data/year=*/month=*/*.parquet").
fn discover_partition_columns_from_glob_pattern(path: &Path) -> Vec<String> {
    let path_str = path.as_os_str().to_string_lossy();
    let mut columns = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for segment in path_str.split('/') {
        if let Some((key, rest)) = segment.split_once('=')
            && !key.is_empty()
            && (rest == "*" || !rest.contains('*'))
            && seen.insert(key.to_string())
        {
            columns.push(key.to_string());
        }
    }
    columns
}

/// A partition column's type: the file's own if stored there, else inferred from its
/// values the way Polars does for a full scan.
pub(crate) fn partition_dtype(
    name: &str,
    file_schema: &Schema,
    values: &[(String, String)],
) -> DataType {
    use polars::io::csv::read::schema_inference::{finish_infer_field_schema, infer_field_schema};
    if let Some(dtype) = file_schema.get(name) {
        return dtype.clone();
    }
    let seen: PlIndexSet<DataType> = values
        .iter()
        .filter(|(k, v)| k == name && !v.is_empty() && v != "__HIVE_DEFAULT_PARTITION__")
        .map(|(_, v)| infer_field_schema(v, true, false))
        .collect();
    if seen.is_empty() {
        DataType::String
    } else {
        finish_infer_field_schema(&seen)
    }
}

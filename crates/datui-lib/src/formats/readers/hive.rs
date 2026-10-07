//! Hive-partitioned Parquet on disk: a directory of `key=value` directories read as
//! one scan, with the keys as columns.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

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

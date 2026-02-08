//! Python bindings for datui. Exposes `view_from_bytes` (binary-serialized LazyFrame),
//! `view_from_json` (JSON, deprecated by Polars), `view_paths` (open by path strings),
//! and `run_cli`. The Python package provides `view()` which accepts LazyFrame/DataFrame
//! or path string(s) and dispatches accordingly.
//!
//! Error classification lives in datui-lib; the binding only maps lib result to Python exceptions.

use std::panic;
use std::path::{Path, PathBuf};

use ::datui::{error_for_python, ErrorKindForPython, OpenOptions, RunInput, run};
use bincode::config::legacy;
use polars::prelude::LazyFrame;
use polars_plan::dsl::DslPlan;
use pyo3::exceptions::{PyFileNotFoundError, PyPermissionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::{self, Value};

/// Rewrite path objects from newer Polars JSON format to Rust 0.52 format.
/// Newer Polars emits `"path": {"inner": "/foo"}`; polars-plan 0.52 expects `"path": {"Local": "/foo"}`.
/// We only rewrite when the value of key "path" is an object with single key "inner" and string value.
fn normalize_lazyframe_json(value: Value) -> Value {
    match value {
        Value::Object(mut map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let v = map.get_mut(&k).expect("key exists");
                if k == "path" {
                    if let Some(normalized) = normalize_path_value(v.clone()) {
                        *v = normalized;
                    }
                } else {
                    *v = normalize_lazyframe_json(std::mem::take(v));
                }
            }
            Value::Object(map)
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(normalize_lazyframe_json)
                .collect(),
        ),
        other => other,
    }
}

/// If `value` is `{"inner": "<string>"}`, return `{"Local": "<string>"}`; else return None.
fn normalize_path_value(value: Value) -> Option<Value> {
    let obj = value.as_object()?;
    if obj.len() != 1 {
        return None;
    }
    let (key, val) = obj.iter().next()?;
    if key != "inner" {
        return None;
    }
    let path_str = val.as_str()?;
    let mut m = serde_json::Map::new();
    m.insert("Local".to_string(), Value::String(path_str.to_string()));
    Some(Value::Object(m))
}

fn run_tui(plan: DslPlan, debug: bool) -> PyResult<()> {
    let lf = LazyFrame::from(plan);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let opts = OpenOptions::default();
        let input = RunInput::LazyFrame(Box::new(lf), opts);
        run(input, None, debug)
    }));
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            let (kind, msg) = error_for_python(&e);
            Err(match kind {
                ErrorKindForPython::FileNotFound => PyFileNotFoundError::new_err(msg),
                ErrorKindForPython::PermissionDenied => PyPermissionError::new_err(msg),
                ErrorKindForPython::Other => PyRuntimeError::new_err(msg),
            }
            .into())
        }
        Err(panic_payload) => {
            let msg: String = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "datui panicked".to_string()
            };
            Err(PyRuntimeError::new_err(format!("datui panicked: {}", msg)).into())
        }
    }
}

/// Launch the datui TUI with a LazyFrame logical plan given as binary (default Polars format).
///
/// The bytes must be the output of Polars Python `LazyFrame.serialize()` or
/// `DataFrame.lazy().serialize()` (binary format, the default). This avoids passing
/// LazyFrame objects across the Python/Rust boundary.
///
/// When the user exits the TUI (e.g. presses `q`), control returns to Python.
/// Uses the same config as the CLI (~/.config/datui/config.toml).
///
/// Args:
///     data: Bytes from LazyFrame.serialize() or df.lazy().serialize() (binary).
///     debug: If True, enable debug overlay (default False).
///
/// Raises:
///     ValueError: If the bytes are not valid LazyFrame binary.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics.
#[pyfunction]
#[pyo3(signature = (data, *, debug=false))]
fn view_from_bytes(
    _py: Python<'_>,
    data: &[u8],
    debug: bool,
) -> PyResult<()> {
    let (plan, _): (DslPlan, usize) = bincode::serde::decode_from_slice(data, legacy())
        .map_err(|e| {
            PyValueError::new_err(format!(
                "invalid LazyFrame binary (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
                e
            ))
        })?;
    run_tui(plan, debug)
}

/// Launch the datui TUI with a LazyFrame logical plan given as JSON.
///
/// The JSON must be the output of Polars Python `LazyFrame.serialize(format="json")`
/// (deprecated in Polars). Prefer `view_from_bytes()` with the default binary format.
///
/// When the user exits the TUI (e.g. presses `q`), control returns to Python.
///
/// Args:
///     json_str: JSON string from LazyFrame.serialize(format="json").
///     debug: If True, enable debug overlay (default False).
///
/// Raises:
///     ValueError: If the string is not valid LazyFrame JSON.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics.
#[pyfunction]
#[pyo3(signature = (json_str, *, debug=false))]
fn view_from_json(
    _py: Python<'_>,
    json_str: &str,
    debug: bool,
) -> PyResult<()> {
    let value: Value = serde_json::from_str(json_str).map_err(|e| {
        PyValueError::new_err(format!(
            "invalid LazyFrame JSON (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
            e
        ))
    })?;
    let normalized = normalize_lazyframe_json(value);
    let plan: DslPlan = serde_json::from_value(normalized).map_err(|e| {
        PyValueError::new_err(format!(
            "invalid LazyFrame JSON (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
            e
        ))
    })?;
    run_tui(plan, debug)
}

/// Launch the datui TUI with one or more paths (local files, S3, GCS, or HTTP/HTTPS URLs).
///
/// Paths are passed to the same loading logic as the CLI: local files, `s3://`, `gs://`,
/// and `http(s)://` are supported. Glob patterns (e.g. `"data/**/*.parquet"`) are supported
/// for Parquet; the loader passes them to Polars for expansion. Non-Parquet remote files
/// are downloaded to a temp file then loaded. Multiple paths are allowed; the same rule
/// as the CLI applies (e.g. only one remote URL when the first path is remote).
///
/// Args:
///     paths: A single path string or a list of path strings (e.g. `"file.csv"`,
///            `"s3://bucket/file.csv"`, `["a.csv", "b.csv"]`, or `"data/**/*.parquet"`).
///     debug: If True, enable debug overlay (default False).
///
/// Raises:
///     ValueError: If paths is empty.
///     FileNotFoundError: If a path does not exist (globs are not checked for existence).
///     PermissionError: If read access to a path is denied.
///     RuntimeError: If the TUI fails or an uncategorized error occurs.
#[pyfunction]
#[pyo3(signature = (paths, *, debug=false))]
fn view_paths(
    _py: Python<'_>,
    paths: Vec<String>,
    debug: bool,
) -> PyResult<()> {
    if paths.is_empty() {
        return Err(PyValueError::new_err("paths must not be empty"));
    }
    let path_bufs: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    let opts = OpenOptions::default();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        run(RunInput::Paths(path_bufs, opts), None, debug)
    }));
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            let (kind, msg) = error_for_python(&e);
            Err(match kind {
                ErrorKindForPython::FileNotFound => PyFileNotFoundError::new_err(msg),
                ErrorKindForPython::PermissionDenied => PyPermissionError::new_err(msg),
                ErrorKindForPython::Other => PyRuntimeError::new_err(msg),
            }
            .into())
        }
        Err(panic_payload) => {
            let msg: String = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "datui panicked".to_string()
            };
            Err(PyRuntimeError::new_err(format!("datui panicked: {}", msg)).into())
        }
    }
}

/// Run the datui CLI with the current process arguments (e.g. from `datui file.csv`).
///
/// Looks for a bundled binary next to the extension module (`datui_bin/datui`). Does not
/// fall back to `datui` on PATH, because that may be this same Python script (infinite loop).
/// Exits the process with the CLI's exit code (via sys.exit).
#[pyfunction]
fn run_cli(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let argv: Vec<String> = sys.getattr("argv")?.extract()?;
    let modules = sys.getattr("modules")?;
    let datui_module = modules.get_item("datui")?;
    let file: Option<String> = datui_module.getattr("__file__")?.extract().ok();
    let bin_name = {
        #[cfg(windows)]
        {
            "datui.exe"
        }
        #[cfg(not(windows))]
        {
            "datui"
        }
    };
    // Prefer datui package __file__ (__init__.py); fallback to this extension's __file__ (_datui.so) for package dir.
    let package_dir = file
        .as_ref()
        .map(|f| Path::new(f).parent().unwrap_or(Path::new(".")).to_path_buf());
    let package_dir = match package_dir {
        Some(d) => d,
        None => {
            // Fallback: use this extension module's path (we're in datui/_datui.*.so, so parent = package dir).
            let mod_datui = modules.get_item("datui")?.getattr("_datui")?;
            let ext_file: Option<String> = mod_datui.getattr("__file__")?.extract().ok();
            ext_file
                .as_ref()
                .and_then(|f| Path::new(f).parent().map(|p| p.to_path_buf()))
                .ok_or_else(|| {
                    PyRuntimeError::new_err(
                        "datui CLI: cannot find package location. Install a wheel that bundles the binary.",
                    )
                })?
        }
    };
    let binary = {
        // Wheel layout: datui/ and datui_bin/ are siblings under site-packages (include = ["datui_bin/*"]).
        let bundled_sibling = package_dir.parent().unwrap_or(&package_dir).join("datui_bin").join(bin_name);
        // Editable/dev layout: datui/datui_bin/ next to __init__.py (or _datui.so).
        let bundled_inside = package_dir.join("datui_bin").join(bin_name);
        if bundled_sibling.exists() {
            bundled_sibling
        } else if bundled_inside.exists() {
            bundled_inside
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "datui CLI binary not found (looked for {} and {}). \
                 For local dev, run: cp target/debug/datui python/datui_bin/ then maturin develop. \
                 Or install a wheel that bundles the binary.",
                bundled_sibling.display(),
                bundled_inside.display()
            )));
        }
    };
    // Refuse to run if the path is a script (e.g. venv bin/datui wrapper); prevents infinite loop.
    if let Ok(prefix) = std::fs::read(&binary).and_then(|b| Ok(b.get(0..2).unwrap_or_default().to_vec())) {
        if prefix == b"#!" {
            return Err(PyRuntimeError::new_err(format!(
                "datui CLI: {} is a script, not the datui binary. \
                 Do not use the Python wrapper to run itself. \
                 Copy the real binary to datui_bin/ or run the standalone datui from PATH.",
                binary.display()
            )));
        }
    }
    let status = std::process::Command::new(&binary)
        .args(&argv[1..])
        .status()
        .map_err(|e| PyRuntimeError::new_err(format!("failed to run datui CLI: {}", e)))?;
    let code = status.code().unwrap_or(-1);
    let _ = py.import("sys")?.getattr("exit")?.call1((code,));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_path_value_inner_to_local() {
        let v = serde_json::json!({"inner": "/tmp/foo.parquet"});
        let out = normalize_path_value(v).expect("normalized");
        let obj = out.as_object().expect("object");
        assert_eq!(obj.len(), 1);
        assert_eq!(obj.get("Local").and_then(Value::as_str), Some("/tmp/foo.parquet"));
    }

    #[test]
    fn normalize_path_value_non_inner_unchanged() {
        let v = serde_json::json!({"Local": "/already/local"});
        assert!(normalize_path_value(v).is_none());
    }

    #[test]
    fn normalize_lazyframe_json_rewrites_nested_path() {
        let json = serde_json::json!({
            "DataFrameScan": {
                "path": {"inner": "/data/file.parquet"},
                "other": "unchanged"
            }
        });
        let out = normalize_lazyframe_json(json);
        let scan = out.get("DataFrameScan").expect("DataFrameScan").as_object().expect("obj");
        let path = scan.get("path").expect("path").as_object().expect("path obj");
        assert_eq!(path.get("Local").and_then(Value::as_str), Some("/data/file.parquet"));
        assert!(!path.contains_key("inner"));
        assert_eq!(scan.get("other").and_then(Value::as_str), Some("unchanged"));
    }
}

/// Native extension module. The public `datui` package is provided by Python code
/// (datui/__init__.py) which imports this as _datui and exposes view(), view_from_bytes(),
/// view_from_json(), view_paths(), run_cli.
#[pymodule]
fn _datui(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(view_from_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(view_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(view_paths, m)?)?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}

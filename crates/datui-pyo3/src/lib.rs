//! Python bindings for datui. Exposes `view_from_bytes` (binary-serialized LazyFrame),
//! `view_from_json` (JSON, deprecated by Polars), and `run_cli`.
//! The Python package provides `view()` which serializes a LazyFrame and calls `view_from_bytes`.

use std::panic;
use std::path::Path;

use ::datui::{OpenOptions, RunInput, run};
use bincode::config::legacy;
use polars::prelude::LazyFrame;
use polars_plan::dsl::DslPlan;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde_json;

fn run_tui(plan: DslPlan, debug: bool, row_numbers: bool) -> PyResult<()> {
    let lf = LazyFrame::from(plan);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let opts = OpenOptions {
            row_numbers,
            ..OpenOptions::default()
        };
        let input = RunInput::LazyFrame(Box::new(lf), opts);
        run(input, None, debug)
    }));
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(PyRuntimeError::new_err(format!("datui error: {}", e))),
        Err(panic_payload) => {
            let msg: String = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "datui panicked".to_string()
            };
            Err(PyRuntimeError::new_err(format!("datui panicked: {}", msg)))
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
///     row_numbers: If True, show row numbers (default False).
///
/// Raises:
///     RuntimeError: If the bytes are invalid or the TUI fails/panics.
#[pyfunction]
#[pyo3(signature = (data, *, debug=false, row_numbers=false))]
fn view_from_bytes(
    _py: Python<'_>,
    data: &[u8],
    debug: bool,
    row_numbers: bool,
) -> PyResult<()> {
    let (plan, _): (DslPlan, usize) = bincode::serde::decode_from_slice(data, legacy())
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "invalid LazyFrame binary (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
                e
            ))
        })?;
    run_tui(plan, debug, row_numbers)
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
///     row_numbers: If True, show row numbers (default False).
///
/// Raises:
///     RuntimeError: If the JSON is invalid or the TUI fails/panics.
#[pyfunction]
#[pyo3(signature = (json_str, *, debug=false, row_numbers=false))]
fn view_from_json(
    _py: Python<'_>,
    json_str: &str,
    debug: bool,
    row_numbers: bool,
) -> PyResult<()> {
    let plan: DslPlan = serde_json::from_str(json_str).map_err(|e| {
        PyRuntimeError::new_err(format!(
            "invalid LazyFrame JSON (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
            e
        ))
    })?;
    run_tui(plan, debug, row_numbers)
}

/// Run the datui CLI with the current process arguments (e.g. from `datui file.csv`).
///
/// Looks for a bundled binary next to the extension module (`datui_bin/datui`), then falls
/// back to `datui` on PATH. Exits the process with the CLI's exit code (via sys.exit).
#[pyfunction]
fn run_cli(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let argv: Vec<String> = sys.getattr("argv")?.extract()?;
    let modules = sys.getattr("modules")?;
    let datui_module = modules.get_item("datui")?;
    let file: Option<String> = datui_module.getattr("__file__")?.extract().ok();
    let binary = if let Some(ref f) = file {
        let parent = Path::new(f).parent().unwrap_or(Path::new("."));
        let bundled = parent.join("datui_bin").join({
            #[cfg(windows)]
            {
                "datui.exe"
            }
            #[cfg(not(windows))]
            {
                "datui"
            }
        });
        if bundled.exists() {
            bundled
        } else {
            Path::new({
                #[cfg(windows)]
                {
                    "datui.exe"
                }
                #[cfg(not(windows))]
                {
                    "datui"
                }
            })
            .to_path_buf()
        }
    } else {
        Path::new({
            #[cfg(windows)]
            {
                "datui.exe"
            }
            #[cfg(not(windows))]
            {
                "datui"
            }
        })
        .to_path_buf()
    };
    let status = std::process::Command::new(&binary)
        .args(&argv[1..])
        .status()
        .map_err(|e| PyRuntimeError::new_err(format!("failed to run datui CLI: {}", e)))?;
    let code = status.code().unwrap_or(-1);
    let _ = py.import("sys")?.getattr("exit")?.call1((code,));
    Ok(())
}

/// Native extension module. The public `datui` package is provided by Python code
/// (datui/__init__.py) which imports this as _datui and exposes view(), view_from_bytes(), view_from_json(), run_cli.
#[pymodule]
fn _datui(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(view_from_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(view_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}

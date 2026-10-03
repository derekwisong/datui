//! Python bindings for datui. Exposes `view_from_bytes` (binary-serialized LazyFrame),
//! `view_from_json` (JSON, deprecated by Polars), `view_paths` (open by path strings),
//! `DatuiOptions`, `CompressionFormat`, and `run_cli`. The Python package provides
//! `view()` which accepts LazyFrame/DataFrame or path string(s) and dispatches accordingly.
//!
//! Error classification lives in datui-lib; the binding only maps lib result to Python exceptions.

use std::panic;
use std::path::{Path, PathBuf};

use ::datui::cli::{Args, parse_args, settings};
use ::datui::{ErrorKindForPython, RunInput, error_for_python, run, run_captured};
use polars::prelude::LazyFrame;
use polars_plan::dsl::DslPlan;
use pyo3::exceptions::{
    PyFileNotFoundError, PyPermissionError, PyRuntimeError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json;

/// Every keyword `datui.view()` and `DatuiOptions` take: the open's own options and
/// the config keys' keywords from the option registry, and `config`, a dict of any
/// config key to its value, as `-c` takes them.
fn option_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = settings::OPEN.iter().map(|o| o.kwarg).collect();
    names.extend(settings::SETTINGS.iter().filter_map(|s| s.kwarg));
    names.push("config");
    names
}

/// A Python value as `-c` or a flag spells it: booleans as `true`/`false`, a list as
/// a TOML array, anything else as its text.
fn option_text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(b) = value.extract::<bool>() {
        return Ok(b.to_string());
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    if let Ok(items) = value.extract::<Vec<String>>() {
        return serde_json::to_string(&items)
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }
    if let Ok(n) = value.extract::<i64>() {
        return Ok(n.to_string());
    }
    // A path-like, such as pathlib.Path.
    let os = PyModule::import(value.py(), "os")?;
    os.getattr("fspath")?.call1((value,))?.extract::<String>()
}

/// The items of a value that may be one or a list.
fn option_items(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if value.extract::<String>().is_err()
        && let Ok(list) = value.cast::<pyo3::types::PyList>()
    {
        return list.iter().map(|item| option_text(&item)).collect();
    }
    if let Ok(tuple) = value.cast::<pyo3::types::PyTuple>() {
        return tuple.iter().map(|item| option_text(&item)).collect();
    }
    Ok(vec![option_text(value)?])
}

/// The command line `kwargs` stand for: the open's options as their flags, the config
/// keys' keywords and `config` as `-c`. Read by the same parser as the binary's, so a
/// value means what it means there.
fn args_from_kwargs(kwargs: &Bound<'_, PyDict>) -> PyResult<Args> {
    let mut argv: Vec<String> = vec!["datui".into()];
    for (key, value) in kwargs.iter() {
        let key: String = key.extract()?;
        if value.is_none() {
            continue;
        }
        if key == "config" {
            let table = value
                .cast::<PyDict>()
                .map_err(|_| PyTypeError::new_err("config must be a dict of key to value"))?;
            for (name, value) in table.iter() {
                argv.push("-c".into());
                argv.push(format!("{}={}", name.extract::<String>()?, option_text(&value)?));
            }
        } else if let Some(setting) = settings::by_kwarg(&key) {
            argv.push("-c".into());
            argv.push(format!("{}={}", setting.key, option_text(&value)?));
        } else if let Some(open) = settings::OPEN.iter().find(|o| o.kwarg == key) {
            let flag = format!("--{}", open.flag);
            match open.kind {
                settings::Kind::Bool => {
                    if value.is_truthy()? {
                        argv.push(flag);
                    }
                }
                settings::Kind::List => {
                    for item in option_items(&value)? {
                        argv.push(flag.clone());
                        argv.push(item);
                    }
                }
                _ => {
                    // A delimiter given as its code.
                    let text = match value.extract::<u8>() {
                        Ok(code) if open.flag == "delimiter" => format!("0x{code:02x}"),
                        _ => option_text(&value)?,
                    };
                    argv.push(flag);
                    argv.push(text);
                }
            }
        } else {
            let mut names = option_names();
            names.sort_unstable();
            return Err(PyTypeError::new_err(format!(
                "{key:?} is not a datui option; options: {}",
                names.join(", ")
            )));
        }
    }
    parse_args(argv).map_err(PyValueError::new_err)
}

/// Options for opening data, as keywords: the open's own (`format`, `table`,
/// `delimiter`, `header_rows`, ...) and a config key's (`comment`, `row_numbers`,
/// `infer_types`, ...), named in `datui.OPTION_NAMES`. `config` sets any config key
/// for this view, as `-c` does: `config={"display.row_numbers": True}`. Values mean
/// what the same flag or key means on the command line.
#[pyclass(name = "DatuiOptions")]
struct DatuiOptionsPy {
    args: Args,
    kwargs: Py<PyDict>,
}

#[pymethods]
impl DatuiOptionsPy {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(py: Python<'_>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let kwargs = match kwargs {
            Some(kwargs) => kwargs.copy()?,
            None => PyDict::new(py),
        };
        let args = args_from_kwargs(&kwargs)?;
        Ok(Self {
            args,
            kwargs: kwargs.unbind(),
        })
    }

    /// The keywords these options were made with, for merging with more. Internal use.
    fn _as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.kwargs.bind(py).copy()
    }
}

/// The command line `opts` stand for, or none's.
fn datui_options_to_args(opts: Option<&Bound<'_, DatuiOptionsPy>>) -> PyResult<Args> {
    match opts {
        Some(opts) => Ok(opts.borrow().args.clone()),
        None => parse_args(["datui"]).map_err(PyValueError::new_err),
    }
}

/// Compression format for data files (e.g. for use with DatuiOptions).
#[pyclass(name = "CompressionFormat")]
#[derive(Clone, Copy)]
enum CompressionFormatPy {
    Gzip,
    Zstd,
    Bzip2,
    Xz,
}

/// Rewrite path-like objects from newer Polars JSON format to Rust 0.52 format.
/// Newer Polars emits `{"inner": "/foo"}` (under "path" or other keys); polars-plan 0.52
/// expects `{"Local": "/foo"}` or `{"Cloud": "..."}`. We recursively rewrite any object
/// that is exactly `{"inner": "<string>"}` to `{"Local": "<string>"}`.
fn run_tui(plan: DslPlan, args: Args, capture: bool) -> PyResult<Option<Vec<u8>>> {
    let lf = LazyFrame::from(plan);
    let input = RunInput::Host(Box::new(args), Some(Box::new(lf)));
    run_input(input, capture)
}

/// Run the TUI on `input` and hand back the captured view's plan bytes, if one was
/// asked for and a dataset was open at quit.
fn run_input(input: RunInput, capture: bool) -> PyResult<Option<Vec<u8>>> {
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        if capture {
            run_captured(input, None)
        } else {
            run(input, None).map(|()| None)
        }
    }));
    match result {
        Ok(Ok(None)) => Ok(None),
        Ok(Ok(Some(lf))) => serialize_captured(lf).map(Some),
        Ok(Err(e)) => {
            let (kind, msg) = error_for_python(&e);
            Err(match kind {
                ErrorKindForPython::FileNotFound => PyFileNotFoundError::new_err(msg),
                ErrorKindForPython::PermissionDenied => PyPermissionError::new_err(msg),
                ErrorKindForPython::Other => PyRuntimeError::new_err(msg),
            })
        }
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

/// Serialize a captured view's plan for Python to deserialize. Always RuntimeError on
/// failure, never ValueError: the wrapper retries a ValueError through the JSON input
/// path, and a failure on the way *out* must not launch the TUI a second time.
fn serialize_captured(lf: LazyFrame) -> PyResult<Vec<u8>> {
    let mut buf = Vec::new();
    lf.logical_plan
        .serialize_versioned(&mut buf, Default::default())
        .map_err(|e| {
            PyRuntimeError::new_err(format!(
                "datui could not serialize the captured view: {}",
                e
            ))
        })?;
    Ok(buf)
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
///     options: Optional DatuiOptions; default when None.
///     capture: When True, return the final view's plan as bytes on normal quit
///         (None when no dataset was open); the wrapper deserializes them.
///
/// Raises:
///     ValueError: If the bytes are not valid LazyFrame binary.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics, or a captured view cannot be
///         returned (temporary source files, serialization failure).
/// Where the schema hash sits in a versioned plan: after the `DSL_VERSION` magic bytes and
/// the u16 major and minor version.
const DSL_HASH_OFFSET: usize = b"DSL_VERSION".len() + 4;
const DSL_HASH_LEN: usize = 64;

/// This build's DSL schema hash, taken from the header of a plan it serializes itself.
/// Empty if that fails, in which case plans are passed through unchanged.
fn own_dsl_hash() -> &'static [u8] {
    static HASH: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    HASH.get_or_init(|| {
        use polars::prelude::IntoLazy;
        let mut header = Vec::new();
        let plan = polars::prelude::DataFrame::empty().lazy().logical_plan;
        if plan
            .serialize_versioned(&mut header, Default::default())
            .is_err()
        {
            return Vec::new();
        }
        header
            .get(DSL_HASH_OFFSET..DSL_HASH_OFFSET + DSL_HASH_LEN)
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
    })
}

/// A reader over `data` with its schema hash replaced by this build's own, without copying
/// the plan (which can carry a whole DataFrame).
fn with_own_dsl_hash(data: &[u8]) -> Box<dyn std::io::Read + '_> {
    use std::io::Read;
    let own = own_dsl_hash();
    if own.len() != DSL_HASH_LEN || data.len() < DSL_HASH_OFFSET + DSL_HASH_LEN {
        return Box::new(data);
    }
    Box::new(
        data[..DSL_HASH_OFFSET]
            .chain(own)
            .chain(&data[DSL_HASH_OFFSET + DSL_HASH_LEN..]),
    )
}

#[pyfunction]
#[pyo3(signature = (data, *, options=None, capture=false))]
fn view_from_bytes(
    _py: Python<'_>,
    data: &[u8],
    options: Option<Bound<'_, DatuiOptionsPy>>,
    capture: bool,
) -> PyResult<Option<Vec<u8>>> {
    // Python `LazyFrame.serialize()` writes a DSL version and a schema hash ahead of the
    // plan. The version is checked. The hash is not comparable: it is the digest of a file
    // in the polars repository at the commit each release was cut from, and no PyPI wheel
    // is cut from the commit of a crates.io release, so it never matches even within one
    // release train. This build's own hash is spliced in its place, so the check passes
    // without setting POLARS_SKIP_DSL_HASH_VERIFICATION, which would mean writing the
    // environment of a process already running threads. The plan itself is MessagePack
    // with field names, so a plan from a different DSL still fails on a missing field
    // instead of being misread.
    let decoded = DslPlan::deserialize_versioned(with_own_dsl_hash(data));
    let plan = decoded.map_err(|e| {
        PyValueError::new_err(format!(
            "invalid LazyFrame binary (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
            e
        ))
    })?;
    let args = datui_options_to_args(options.as_ref())?;
    run_tui(plan, args, capture)
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
///     options: Optional DatuiOptions; default when None.
///
/// Raises:
///     ValueError: If the string is not valid LazyFrame JSON.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics.
#[pyfunction]
#[pyo3(signature = (json_str, *, options=None, capture=false))]
fn view_from_json(
    _py: Python<'_>,
    json_str: &str,
    options: Option<Bound<'_, DatuiOptionsPy>>,
    capture: bool,
) -> PyResult<Option<Vec<u8>>> {
    let plan: DslPlan = serde_json::from_str(json_str).map_err(|e| {
        PyValueError::new_err(format!(
            "invalid LazyFrame JSON (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
            e
        ))
    })?;
    let args = datui_options_to_args(options.as_ref())?;
    run_tui(plan, args, capture)
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
///     options: Optional DatuiOptions; default when None.
///
/// Raises:
///     ValueError: If paths is empty.
///     FileNotFoundError: If a path does not exist (globs are not checked for existence).
///     PermissionError: If read access to a path is denied.
///     RuntimeError: If the TUI fails or an uncategorized error occurs.
#[pyfunction]
#[pyo3(signature = (paths, *, options=None, capture=false))]
fn view_paths(
    _py: Python<'_>,
    paths: Vec<String>,
    options: Option<Bound<'_, DatuiOptionsPy>>,
    capture: bool,
) -> PyResult<Option<Vec<u8>>> {
    if paths.is_empty() {
        return Err(PyValueError::new_err("paths must not be empty"));
    }
    let mut args = datui_options_to_args(options.as_ref())?;
    args.paths = paths.into_iter().map(PathBuf::from).collect();
    run_input(RunInput::Host(Box::new(args), None), capture)
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
    let package_dir = file.as_ref().map(|f| {
        Path::new(f)
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf()
    });
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
        let bundled_sibling = package_dir
            .parent()
            .unwrap_or(&package_dir)
            .join("datui_bin")
            .join(bin_name);
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
    if let Ok(prefix) =
        std::fs::read(&binary).and_then(|b| Ok(b.get(0..2).unwrap_or_default().to_vec()))
    {
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

/// Native extension module. The public `datui` package is provided by Python code
/// (datui/__init__.py) which imports this as _datui and exposes view(), DatuiOptions,
/// CompressionFormat, view_from_bytes(), view_from_json(), view_paths(), run_cli.
#[pymodule]
fn _datui(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatuiOptionsPy>()?;
    m.add("OPTION_NAMES", option_names())?;
    m.add_class::<CompressionFormatPy>()?;
    m.add_function(wrap_pyfunction!(view_from_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(view_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(view_paths, m)?)?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}

//! Python bindings for datui. Exposes `view_from_bytes` (binary-serialized LazyFrame),
//! `view_from_json` (JSON, deprecated by Polars), `view_paths` (open by path strings),
//! `DatuiOptions`, `CompressionFormat`, and `run_cli`. The Python package provides
//! `view()` which accepts LazyFrame/DataFrame or path string(s) and dispatches accordingly.
//!
//! Error classification lives in datui-lib; the binding only maps lib result to Python exceptions.

use std::panic;
use std::path::{Path, PathBuf};

use ::datui::{
    error_for_python, CompressionFormat, ErrorKindForPython, FileFormat, OpenOptions, ParseStringsTarget,
    RunInput, run,
};
use bincode::config::legacy;
use polars::prelude::LazyFrame;
use polars_plan::dsl::DslPlan;
use pyo3::exceptions::{PyFileNotFoundError, PyPermissionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use serde_json::{self, Value};

fn parse_compression(s: &str) -> PyResult<CompressionFormat> {
    match s.to_lowercase().as_str() {
        "gzip" => Ok(CompressionFormat::Gzip),
        "zstd" | "zstandard" => Ok(CompressionFormat::Zstd),
        "bzip2" | "bz2" => Ok(CompressionFormat::Bzip2),
        "xz" => Ok(CompressionFormat::Xz),
        _ => Err(PyValueError::new_err(format!(
            "compression must be one of: gzip, zstd, bzip2, xz (got {:?})",
            s
        ))),
    }
}

fn parse_format(s: &str) -> PyResult<FileFormat> {
    FileFormat::from_extension(s).ok_or_else(|| {
        PyValueError::new_err(format!(
            "format must be one of: parquet, csv, tsv, psv, json, jsonl, arrow, avro, orc, excel (got {:?})",
            s
        ))
    })
}

fn format_to_str(f: FileFormat) -> &'static str {
    match f {
        FileFormat::Parquet => "parquet",
        FileFormat::Csv => "csv",
        FileFormat::Tsv => "tsv",
        FileFormat::Psv => "psv",
        FileFormat::Json => "json",
        FileFormat::Jsonl => "jsonl",
        FileFormat::Arrow => "arrow",
        FileFormat::Avro => "avro",
        FileFormat::Orc => "orc",
        FileFormat::Excel => "excel",
    }
}

fn delimiter_from_py(any: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Option<u8>> {
    if any.is_none() {
        return Ok(None);
    }
    if let Ok(n) = any.extract::<i64>() {
        let b = u8::try_from(n).map_err(|_| {
            PyValueError::new_err(format!("delimiter must be 0-255 (got {})", n))
        })?;
        return Ok(Some(b));
    }
    if let Ok(s) = any.extract::<String>() {
        let ch: Vec<char> = s.chars().collect();
        if ch.len() != 1 {
            return Err(PyValueError::new_err(
                "delimiter as str must be a single character",
            ));
        }
        let b = ch[0] as u32;
        if b > 255 {
            return Err(PyValueError::new_err(
                "delimiter character code must be 0-255",
            ));
        }
        return Ok(Some(b as u8));
    }
    Err(PyTypeError::new_err(
        "delimiter must be int (0-255) or single-character str",
    ))
}

fn opt_path_from_py(any: Option<&Bound<'_, pyo3::types::PyAny>>) -> PyResult<Option<PathBuf>> {
    let Some(any) = any else { return Ok(None) };
    if any.is_none() {
        return Ok(None);
    }
    let s: String = any.extract().map_err(|_| {
        PyTypeError::new_err("temp_dir must be str or path-like")
    })?;
    Ok(Some(PathBuf::from(s)))
}

/// Convert Python value to Option<ParseStringsTarget>. None/omitted → All (default). False → disabled. True or [] → All. [str, ...] → Columns.
fn parse_strings_from_py(any: Option<&Bound<'_, pyo3::types::PyAny>>) -> PyResult<Option<ParseStringsTarget>> {
    let Some(any) = any else {
        return Ok(Some(ParseStringsTarget::All));
    };
    if any.is_none() {
        return Ok(Some(ParseStringsTarget::All));
    }
    if let Ok(false) = any.extract::<bool>() {
        return Ok(None);
    }
    if let Ok(true) = any.extract::<bool>() {
        return Ok(Some(ParseStringsTarget::All));
    }
    if let Ok(list) = any.extract::<Vec<String>>() {
        if list.is_empty() {
            return Ok(Some(ParseStringsTarget::All));
        }
        return Ok(Some(ParseStringsTarget::Columns(
            list.into_iter().collect::<std::collections::HashSet<_>>().into_iter().collect(),
        )));
    }
    Err(PyTypeError::new_err(
        "parse_strings must be None, False, True, or a list of column name strings",
    ))
}

/// Options for loading and displaying data in the TUI (Python name for OpenOptions).
///
/// **parse_strings**: Default is all CSV string columns (trim + type inference). Use `False` to
/// disable; `True` or `[]` for all; or a list of column names to limit to those columns.
/// **parse_strings_sample_rows**: Rows to sample for type inference when parse_strings is enabled (default 1000).
#[pyclass(name = "DatuiOptions")]
struct DatuiOptionsPy {
    inner: OpenOptions,
}

#[pymethods]
impl DatuiOptionsPy {
    #[new]
    #[pyo3(signature = (
        delimiter=None,
        has_header=None,
        skip_lines=None,
        skip_rows=None,
        skip_tail_rows=None,
        compression=None,
        format=None,
        pages_lookahead=None,
        pages_lookback=None,
        max_buffered_rows=None,
        max_buffered_mb=None,
        row_numbers=false,
        row_start_index=1,
        hive=false,
        single_spine_schema=true,
        parse_dates=true,
        decompress_in_memory=false,
        temp_dir=None,
        excel_sheet=None,
        s3_endpoint_url=None,
        s3_access_key_id=None,
        s3_secret_access_key=None,
        s3_region=None,
        polars_streaming=true,
        workaround_pivot_date_index=true,
        null_values=None,
        debug=false,
        parse_strings=None,
        parse_strings_sample_rows=1000
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        delimiter: Option<Bound<'_, pyo3::types::PyAny>>,
        has_header: Option<Bound<'_, pyo3::types::PyAny>>,
        skip_lines: Option<Bound<'_, pyo3::types::PyAny>>,
        skip_rows: Option<Bound<'_, pyo3::types::PyAny>>,
        skip_tail_rows: Option<Bound<'_, pyo3::types::PyAny>>,
        compression: Option<Bound<'_, pyo3::types::PyAny>>,
        format: Option<Bound<'_, pyo3::types::PyAny>>,
        pages_lookahead: Option<Bound<'_, pyo3::types::PyAny>>,
        pages_lookback: Option<Bound<'_, pyo3::types::PyAny>>,
        max_buffered_rows: Option<Bound<'_, pyo3::types::PyAny>>,
        max_buffered_mb: Option<Bound<'_, pyo3::types::PyAny>>,
        row_numbers: bool,
        row_start_index: usize,
        hive: bool,
        single_spine_schema: bool,
        parse_dates: bool,
        decompress_in_memory: bool,
        temp_dir: Option<Bound<'_, pyo3::types::PyAny>>,
        excel_sheet: Option<Bound<'_, pyo3::types::PyAny>>,
        s3_endpoint_url: Option<Bound<'_, pyo3::types::PyAny>>,
        s3_access_key_id: Option<Bound<'_, pyo3::types::PyAny>>,
        s3_secret_access_key: Option<Bound<'_, pyo3::types::PyAny>>,
        s3_region: Option<Bound<'_, pyo3::types::PyAny>>,
        polars_streaming: bool,
        workaround_pivot_date_index: bool,
        null_values: Option<Bound<'_, pyo3::types::PyAny>>,
        debug: bool,
        parse_strings: Option<Bound<'_, pyo3::types::PyAny>>,
        parse_strings_sample_rows: usize,
    ) -> PyResult<Self> {
        let mut opts = OpenOptions::new();
        opts.row_numbers = row_numbers;
        opts.row_start_index = row_start_index;
        opts.hive = hive;
        opts.single_spine_schema = single_spine_schema;
        opts.parse_dates = parse_dates;
        opts.decompress_in_memory = decompress_in_memory;
        opts.polars_streaming = polars_streaming;
        opts.workaround_pivot_date_index = workaround_pivot_date_index;

        if let Some(ref a) = delimiter {
            opts.delimiter = delimiter_from_py(a)?;
        }
        if let Some(ref a) = has_header {
            if !a.is_none() {
                opts.has_header = Some(a.extract()?);
            }
        }
        if let Some(ref a) = skip_lines {
            if !a.is_none() {
                opts.skip_lines = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = skip_rows {
            if !a.is_none() {
                opts.skip_rows = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = skip_tail_rows {
            if !a.is_none() {
                opts.skip_tail_rows = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = compression {
            if !a.is_none() {
                let s: String = a.extract().map_err(|_| {
                    PyTypeError::new_err("compression must be str (e.g. 'gzip', 'zstd')")
                })?;
                opts.compression = Some(parse_compression(&s)?);
            }
        }
        if let Some(ref a) = format {
            if !a.is_none() {
                let s: String = a.extract().map_err(|_| {
                    PyTypeError::new_err("format must be str (e.g. 'csv', 'parquet')")
                })?;
                opts.format = Some(parse_format(&s)?);
            }
        }
        if let Some(ref a) = pages_lookahead {
            if !a.is_none() {
                opts.pages_lookahead = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = pages_lookback {
            if !a.is_none() {
                opts.pages_lookback = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = max_buffered_rows {
            if !a.is_none() {
                opts.max_buffered_rows = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = max_buffered_mb {
            if !a.is_none() {
                opts.max_buffered_mb = Some(a.extract::<usize>()?);
            }
        }
        if let Some(ref a) = temp_dir {
            opts.temp_dir = opt_path_from_py(Some(a))?;
        }
        if let Some(ref a) = excel_sheet {
            if !a.is_none() {
                opts.excel_sheet = Some(a.extract::<String>()?);
            }
        }
        if let Some(ref a) = s3_endpoint_url {
            if !a.is_none() {
                opts.s3_endpoint_url_override = Some(a.extract::<String>()?);
            }
        }
        if let Some(ref a) = s3_access_key_id {
            if !a.is_none() {
                opts.s3_access_key_id_override = Some(a.extract::<String>()?);
            }
        }
        if let Some(ref a) = s3_secret_access_key {
            if !a.is_none() {
                opts.s3_secret_access_key_override = Some(a.extract::<String>()?);
            }
        }
        if let Some(ref a) = s3_region {
            if !a.is_none() {
                opts.s3_region_override = Some(a.extract::<String>()?);
            }
        }
        if let Some(ref a) = null_values {
            if !a.is_none() {
                opts.null_values = Some(a.extract::<Vec<String>>()?);
            }
        }
        opts.parse_strings = parse_strings_from_py(parse_strings.as_ref().map(|b| b.as_ref()))?;
        opts.parse_strings_sample_rows = parse_strings_sample_rows;
        opts.debug = debug;
        Ok(Self { inner: opts })
    }

    /// Return options as a dict of Python values (for merging with kwargs). Internal use.
    fn _as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        use pyo3::types::PyDict;
        let d = PyDict::new(py);
        let o = &self.inner;
        if let Some(v) = o.delimiter {
            d.set_item("delimiter", v)?;
        }
        if let Some(v) = o.has_header {
            d.set_item("has_header", v)?;
        }
        if let Some(v) = o.skip_lines {
            d.set_item("skip_lines", v)?;
        }
        if let Some(v) = o.skip_rows {
            d.set_item("skip_rows", v)?;
        }
        if let Some(v) = o.skip_tail_rows {
            d.set_item("skip_tail_rows", v)?;
        }
        if let Some(ref v) = o.compression {
            let s = match v {
                CompressionFormat::Gzip => "gzip",
                CompressionFormat::Zstd => "zstd",
                CompressionFormat::Bzip2 => "bzip2",
                CompressionFormat::Xz => "xz",
            };
            d.set_item("compression", s)?;
        }
        if let Some(ref v) = o.format {
            d.set_item("format", format_to_str(*v))?;
        }
        if let Some(v) = o.pages_lookahead {
            d.set_item("pages_lookahead", v)?;
        }
        if let Some(v) = o.pages_lookback {
            d.set_item("pages_lookback", v)?;
        }
        if let Some(v) = o.max_buffered_rows {
            d.set_item("max_buffered_rows", v)?;
        }
        if let Some(v) = o.max_buffered_mb {
            d.set_item("max_buffered_mb", v)?;
        }
        d.set_item("row_numbers", o.row_numbers)?;
        d.set_item("row_start_index", o.row_start_index)?;
        d.set_item("hive", o.hive)?;
        d.set_item("single_spine_schema", o.single_spine_schema)?;
        d.set_item("parse_dates", o.parse_dates)?;
        match &o.parse_strings {
            None => d.set_item("parse_strings", false)?,
            Some(ParseStringsTarget::All) => d.set_item("parse_strings", true)?,
            Some(ParseStringsTarget::Columns(c)) => d.set_item("parse_strings", c.clone())?,
        }
        d.set_item("parse_strings_sample_rows", o.parse_strings_sample_rows)?;
        d.set_item("decompress_in_memory", o.decompress_in_memory)?;
        if let Some(ref v) = o.temp_dir {
            d.set_item("temp_dir", v.to_string_lossy().as_ref())?;
        }
        if let Some(ref v) = o.excel_sheet {
            d.set_item("excel_sheet", v.as_str())?;
        }
        if let Some(ref v) = o.s3_endpoint_url_override {
            d.set_item("s3_endpoint_url", v.as_str())?;
        }
        if let Some(ref v) = o.s3_access_key_id_override {
            d.set_item("s3_access_key_id", v.as_str())?;
        }
        if let Some(ref v) = o.s3_secret_access_key_override {
            d.set_item("s3_secret_access_key", v.as_str())?;
        }
        if let Some(ref v) = o.s3_region_override {
            d.set_item("s3_region", v.as_str())?;
        }
        d.set_item("polars_streaming", o.polars_streaming)?;
        d.set_item("workaround_pivot_date_index", o.workaround_pivot_date_index)?;
        if let Some(ref v) = o.null_values {
            d.set_item("null_values", v.as_slice())?;
        }
        d.set_item("debug", o.debug)?;
        Ok(d)
    }
}

fn datui_options_to_rust(opts: Option<&Bound<'_, DatuiOptionsPy>>) -> OpenOptions {
    opts.map(|o| o.borrow().inner.clone())
        .unwrap_or_else(OpenOptions::default)
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
fn normalize_lazyframe_json(value: Value) -> Value {
    match value {
        Value::Object(mut map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let v = map.get_mut(&k).expect("key exists");
                *v = normalize_lazyframe_json(std::mem::take(v));
                if let Some(normalized) = normalize_path_value(v.clone()) {
                    *v = normalized;
                }
            }
            // Rewrite this object if it is itself {"inner": "<string>"} (e.g. nested path enum).
            let as_value = Value::Object(map);
            normalize_path_value(as_value.clone()).unwrap_or(as_value)
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

fn run_tui(plan: DslPlan, opts: OpenOptions) -> PyResult<()> {
    let lf = LazyFrame::from(plan);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let input = RunInput::LazyFrame(Box::new(lf), opts);
        run(input, None)
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
///     options: Optional DatuiOptions (includes debug); default when None.
///
/// Raises:
///     ValueError: If the bytes are not valid LazyFrame binary.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics.
#[pyfunction]
#[pyo3(signature = (data, *, options=None))]
fn view_from_bytes(
    _py: Python<'_>,
    data: &[u8],
    options: Option<Bound<'_, DatuiOptionsPy>>,
) -> PyResult<()> {
    let (plan, _): (DslPlan, usize) = bincode::serde::decode_from_slice(data, legacy())
        .map_err(|e| {
            PyValueError::new_err(format!(
                "invalid LazyFrame binary (use LazyFrame.serialize() or DataFrame.lazy().serialize()): {}",
                e
            ))
        })?;
    let opts = datui_options_to_rust(options.as_ref());
    run_tui(plan, opts)
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
///     options: Optional DatuiOptions (includes debug); default when None.
///
/// Raises:
///     ValueError: If the string is not valid LazyFrame JSON.
///     FileNotFoundError: If a path is used and the file is not found (internal).
///     PermissionError: If read access is denied (internal).
///     RuntimeError: If the TUI fails or panics.
#[pyfunction]
#[pyo3(signature = (json_str, *, options=None))]
fn view_from_json(
    _py: Python<'_>,
    json_str: &str,
    options: Option<Bound<'_, DatuiOptionsPy>>,
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
    let opts = datui_options_to_rust(options.as_ref());
    run_tui(plan, opts)
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
///     options: Optional DatuiOptions (includes debug); default when None.
///
/// Raises:
///     ValueError: If paths is empty.
///     FileNotFoundError: If a path does not exist (globs are not checked for existence).
///     PermissionError: If read access to a path is denied.
///     RuntimeError: If the TUI fails or an uncategorized error occurs.
#[pyfunction]
#[pyo3(signature = (paths, *, options=None))]
fn view_paths(
    _py: Python<'_>,
    paths: Vec<String>,
    options: Option<Bound<'_, DatuiOptionsPy>>,
) -> PyResult<()> {
    if paths.is_empty() {
        return Err(PyValueError::new_err("paths must not be empty"));
    }
    let path_bufs: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    let opts = datui_options_to_rust(options.as_ref());
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        run(RunInput::Paths(path_bufs, opts), None)
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
/// (datui/__init__.py) which imports this as _datui and exposes view(), DatuiOptions,
/// CompressionFormat, view_from_bytes(), view_from_json(), view_paths(), run_cli.
#[pymodule]
fn _datui(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatuiOptionsPy>()?;
    m.add_class::<CompressionFormatPy>()?;
    m.add_function(wrap_pyfunction!(view_from_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(view_from_json, m)?)?;
    m.add_function(wrap_pyfunction!(view_paths, m)?)?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}

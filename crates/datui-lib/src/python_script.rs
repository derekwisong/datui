//! Copy as Python: the view as a Python Polars script.
//!
//! Built from datui's own record of how the view was made — the open's reader and
//! its options, then each step in the order it ran ([`Step`]) — never by printing a
//! Polars plan, so the script reads like code a person writes. A step Python
//! cannot repeat is said in a comment, and what follows it is commented out with
//! it: the script never computes something other than what the screen shows.

use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::pivot_melt_modal::PivotAggregation;
use crate::{CompressionFormat, FileFormat, OpenOptions};

/// `s` as a Python string literal.
pub(crate) fn py_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `f` as a Python float literal: always with a point or an exponent, so Python
/// reads a float, as Polars was given one.
pub(crate) fn py_float(f: f64) -> String {
    if f.is_nan() {
        "float(\"nan\")".to_string()
    } else if f.is_infinite() {
        if f > 0.0 {
            "float(\"inf\")".to_string()
        } else {
            "float(\"-inf\")".to_string()
        }
    } else {
        // Debug writes `5.0`, `0.1` and `1e20`, all of which Python reads as floats.
        format!("{f:?}")
    }
}

pub(crate) fn py_bool(b: bool) -> &'static str {
    if b { "True" } else { "False" }
}

/// Names as a Python list: `["a", "b"]`.
pub(crate) fn py_names(names: &[String]) -> String {
    let items: Vec<String> = names.iter().map(|n| py_str(n)).collect();
    format!("[{}]", items.join(", "))
}

/// A sort as datui runs every one: nulls last, ties in the order they came.
pub(crate) fn sort_call(columns: &[String], descending: &[bool]) -> String {
    let by = match columns {
        [one] => py_str(one),
        _ => py_names(columns),
    };
    let descending = if descending.iter().all(|d| !d) {
        String::new()
    } else if descending.iter().all(|d| *d) {
        "descending=True, ".to_string()
    } else {
        let flags: Vec<&str> = descending.iter().map(|d| py_bool(*d)).collect();
        format!("descending=[{}], ", flags.join(", "))
    };
    format!(".sort({by}, {descending}nulls_last=True, maintain_order=True)")
}

/// The value a sidebar filter compares with, typed as the column is: a number for a
/// numeric column when the text reads as one, text otherwise.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(String),
}

impl FilterValue {
    fn lit(&self) -> Expr {
        match self {
            FilterValue::Float(f) => lit(*f),
            FilterValue::Int(i) => lit(*i),
            FilterValue::UInt(u) => lit(*u),
            FilterValue::Bool(b) => lit(*b),
            FilterValue::Str(s) => lit(s.as_str()),
        }
    }

    fn python(&self) -> String {
        match self {
            FilterValue::Float(f) => py_float(*f),
            FilterValue::Int(i) => i.to_string(),
            FilterValue::UInt(u) => u.to_string(),
            FilterValue::Bool(b) => py_bool(*b).to_string(),
            FilterValue::Str(s) => py_str(s),
        }
    }
}

/// One sidebar filter statement with its value typed against the column. The one
/// place a statement becomes a predicate, for the table and for the script alike.
#[derive(Debug, Clone, PartialEq)]
pub struct SidebarFilter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
    /// The value as typed, which `contains` matches as text whatever the column.
    pub text: String,
    pub logical_op: LogicalOperator,
}

impl SidebarFilter {
    pub fn typed(statement: &FilterStatement, dtype: Option<&DataType>) -> Self {
        let text = statement.value.as_str();
        let as_text = || FilterValue::Str(text.to_string());
        let value = match dtype {
            Some(DataType::Float32 | DataType::Float64) => text
                .parse()
                .map(FilterValue::Float)
                .unwrap_or_else(|_| as_text()),
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => text
                .parse()
                .map(FilterValue::Int)
                .unwrap_or_else(|_| as_text()),
            Some(DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => text
                .parse()
                .map(FilterValue::UInt)
                .unwrap_or_else(|_| as_text()),
            Some(DataType::Boolean) => text
                .parse()
                .map(FilterValue::Bool)
                .unwrap_or_else(|_| as_text()),
            _ => as_text(),
        };
        Self {
            column: statement.column.clone(),
            operator: statement.operator,
            value,
            text: statement.value.clone(),
            logical_op: statement.logical_op,
        }
    }

    fn expr(&self) -> Expr {
        let column = col(&self.column);
        let contains = || {
            col(&self.column)
                .str()
                .contains_literal(lit(self.text.as_str()))
        };
        match self.operator {
            FilterOperator::Eq => column.eq(self.value.lit()),
            FilterOperator::NotEq => column.neq(self.value.lit()),
            FilterOperator::Gt => column.gt(self.value.lit()),
            FilterOperator::Lt => column.lt(self.value.lit()),
            FilterOperator::GtEq => column.gt_eq(self.value.lit()),
            FilterOperator::LtEq => column.lt_eq(self.value.lit()),
            FilterOperator::Contains => contains(),
            FilterOperator::NotContains => contains().not(),
        }
    }

    fn python(&self) -> String {
        let column = format!("pl.col({})", py_str(&self.column));
        let op = match self.operator {
            FilterOperator::Eq => "==",
            FilterOperator::NotEq => "!=",
            FilterOperator::Gt => ">",
            FilterOperator::Lt => "<",
            FilterOperator::GtEq => ">=",
            FilterOperator::LtEq => "<=",
            FilterOperator::Contains | FilterOperator::NotContains => {
                let not = if self.operator == FilterOperator::NotContains {
                    "~"
                } else {
                    ""
                };
                return format!(
                    "{not}{column}.str.contains({}, literal=True)",
                    py_str(&self.text)
                );
            }
        };
        format!("{column} {op} {}", self.value.python())
    }
}

/// The statements joined left to right, each with the operator before it, as the
/// sidebar reads them: `a AND b OR c` is `(a AND b) OR c`.
pub fn filters_expr(filters: &[SidebarFilter]) -> Option<Expr> {
    filters.iter().fold(None, |all, f| {
        Some(match all {
            None => f.expr(),
            Some(all) => match f.logical_op {
                LogicalOperator::And => all.and(f.expr()),
                LogicalOperator::Or => all.or(f.expr()),
            },
        })
    })
}

fn filters_python(filters: &[SidebarFilter]) -> String {
    let mut out = String::new();
    let mut last: Option<LogicalOperator> = None;
    for (i, f) in filters.iter().enumerate() {
        let term = format!("({})", f.python());
        if i == 0 {
            // Alone, a statement needs no parentheses.
            out = if filters.len() == 1 { f.python() } else { term };
            continue;
        }
        // Python's `&` binds tighter than `|`: a change of operator closes what
        // came before, so it is evaluated first, as the sidebar does.
        if last.is_some_and(|l| l != f.logical_op) {
            out = format!("({out})");
        }
        let op = match f.logical_op {
            LogicalOperator::And => "&",
            LogicalOperator::Or => "|",
        };
        out = format!("{out} {op} {term}");
        last = Some(f.logical_op);
    }
    out
}

/// One step datui took to build the view, in the order it took them.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// A q-style query over data of the `input` schema. `keys` names a grouping's
    /// keys in its result, which orders its rows.
    Query {
        query: String,
        input: SchemaRef,
        keys: Vec<String>,
    },
    /// The rows a grouped q-style query grouped: its where clause alone.
    QueryRows {
        query: String,
        input: SchemaRef,
    },
    /// A SQL statement over the data so far, registered as `df`. `ordered_by` names
    /// the keys datui orders a grouping's rows by when the statement does not.
    Sql {
        sql: String,
        ordered_by: Vec<String>,
    },
    /// The search (Fuzzy) tab: each pattern must match some text column.
    Search {
        patterns: Vec<String>,
        columns: Vec<String>,
    },
    Filter(Vec<SidebarFilter>),
    Sort {
        columns: Vec<String>,
        descending: Vec<bool>,
    },
    Reverse,
    Select(Vec<String>),
    Drop(Vec<String>),
    Pivot {
        index: Vec<String>,
        on: String,
        values: String,
        aggregation: PivotAggregation,
    },
    Melt {
        index: Vec<String>,
        on: Vec<String>,
        variable_name: String,
        value_name: String,
    },
    /// The rows whose keys equal the values, a null matching nulls: a drill. Each
    /// key and value is Python code already.
    Matching(Vec<(String, String)>),
    /// Something datui did that Python cannot repeat, said plainly.
    Unreproducible(String),
}

impl Step {
    /// The method calls the step is, one per line, without indentation.
    fn python(&self) -> Vec<String> {
        match self {
            Step::Query { query, input, keys } => match crate::query::parse_nodes(query) {
                Ok(mut nodes) => {
                    nodes.resolve_division(input);
                    nodes.python_steps(keys)
                }
                Err(e) => vec![format!("# the query did not parse: {e}")],
            },
            Step::QueryRows { query, input } => match crate::query::parse_nodes(query) {
                Ok(mut nodes) => {
                    nodes.resolve_division(input);
                    nodes.python_filter().into_iter().collect()
                }
                Err(e) => vec![format!("# the query did not parse: {e}")],
            },
            Step::Sql { sql, ordered_by } => {
                let sql = sql.trim();
                let mut lines =
                    if sql.contains('\n') && !sql.contains("\"\"\"") && !sql.contains('\\') {
                        vec![
                            ".sql(".to_string(),
                            format!("    \"\"\"{sql}\"\"\","),
                            "    table_name=\"df\",".to_string(),
                            ")".to_string(),
                        ]
                    } else {
                        vec![format!(".sql({}, table_name=\"df\")", py_str(sql))]
                    };
                if !ordered_by.is_empty() {
                    lines.push(sort_call(ordered_by, &vec![false; ordered_by.len()]));
                }
                lines
            }
            Step::Search { patterns, columns } => {
                let terms: Vec<String> = patterns
                    .iter()
                    .map(|p| {
                        let any: Vec<String> = columns
                            .iter()
                            .map(|c| {
                                format!(
                                    "pl.col({}).str.contains({}, strict=False)",
                                    py_str(c),
                                    py_str(p)
                                )
                            })
                            .collect();
                        if any.len() == 1 || patterns.len() == 1 {
                            any.join(" | ")
                        } else {
                            format!("({})", any.join(" | "))
                        }
                    })
                    .collect();
                vec![format!(".filter({})", terms.join(" & "))]
            }
            Step::Filter(filters) => vec![format!(".filter({})", filters_python(filters))],
            Step::Sort {
                columns,
                descending,
            } => vec![sort_call(columns, descending)],
            Step::Reverse => vec![".reverse()".to_string()],
            Step::Select(columns) => vec![format!(".select({})", py_names(columns))],
            Step::Drop(columns) => vec![format!(".drop({})", py_names(columns))],
            Step::Pivot {
                index,
                on,
                values,
                aggregation,
            } => {
                // As datui pivots: each cell aggregated by a grouping in one pass,
                // then the new columns in sorted order, a null one last.
                let agg = match aggregation {
                    PivotAggregation::Last => "last()",
                    PivotAggregation::First => "first()",
                    PivotAggregation::Min => "min()",
                    PivotAggregation::Max => "max()",
                    PivotAggregation::Avg => "mean()",
                    PivotAggregation::Med => "median()",
                    PivotAggregation::Std => "std()",
                    PivotAggregation::Count => "len()",
                };
                let cell = match aggregation {
                    PivotAggregation::Count => "sum",
                    _ => "first",
                };
                let keys: Vec<String> = index.iter().chain([on]).cloned().collect();
                vec![
                    format!(".group_by({}, maintain_order=True)", py_names(&keys)),
                    format!(".agg(pl.col({}).{agg})", py_str(values)),
                    ".collect()".to_string(),
                    ".pipe(".to_string(),
                    "    lambda cells: cells.pivot(".to_string(),
                    format!("        on={},", py_str(on)),
                    format!(
                        "        on_columns=cells[{}].unique().sort(nulls_last=True),",
                        py_str(on)
                    ),
                    format!("        index={},", py_names(index)),
                    format!("        values={},", py_str(values)),
                    format!("        aggregate_function={},", py_str(cell)),
                    "    )".to_string(),
                    ")".to_string(),
                    ".lazy()".to_string(),
                ]
            }
            Step::Melt {
                index,
                on,
                variable_name,
                value_name,
            } => vec![format!(
                ".unpivot(on={}, index={}, variable_name={}, value_name={})",
                py_names(on),
                py_names(index),
                py_str(variable_name),
                py_str(value_name)
            )],
            Step::Matching(keys) => {
                let terms: Vec<String> = keys
                    .iter()
                    .map(|(key, value)| format!("{key}.eq_missing({value})"))
                    .collect();
                let terms = if terms.len() == 1 {
                    terms
                } else {
                    terms.into_iter().map(|t| format!("({t})")).collect()
                };
                vec![format!(".filter({})", terms.join(" & "))]
            }
            Step::Unreproducible(what) => vec![format!("# {what}")],
        }
    }
}

/// Where the script's data comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    /// A reader call that gives a LazyFrame, with any calls right after it that the
    /// open made part of reading (a footer dropped), and what the reader cannot do
    /// as datui did, as comment lines.
    Read {
        call: String,
        after: Vec<String>,
        notes: Vec<String>,
    },
    /// Data no reader call can name — standard input, a frame handed over, a format
    /// Polars does not read — left to the user as `df = ...`, with why.
    Placeholder { what: String },
}

/// What the open was asked for and what it found, for naming the reader.
pub struct OpenRecord<'a> {
    /// The paths asked for; `None` for a frame handed over.
    pub paths: Option<&'a [PathBuf]>,
    pub options: &'a OpenOptions,
    /// The data as loaded.
    pub schema: &'a Schema,
    /// Each object a remote dataset reads, for the format of a prefix.
    pub remote_objects: Vec<String>,
    /// S3 endpoint and region in effect, for a bucket that is not AWS's.
    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    /// Columns read as text from every file.
    pub read_as_text: Vec<String>,
}

fn is_url(path: &Path) -> bool {
    crate::source::is_remote_url(path)
}

/// The format a file is read as: `--format`, else its extension, looking through a
/// compression extension (`.csv.gz` is CSV).
fn file_format(path: &Path, options: &OpenOptions) -> Option<FileFormat> {
    options.format.or_else(|| {
        FileFormat::from_path(path).or_else(|| {
            CompressionFormat::from_extension(path)
                .and_then(|_| path.file_stem())
                .and_then(|stem| FileFormat::from_path(Path::new(stem)))
        })
    })
}

/// The commonest format among `names`, by extension.
fn commonest_format<'a>(names: impl Iterator<Item = &'a str>) -> Option<FileFormat> {
    let mut counts: Vec<(FileFormat, usize)> = Vec::new();
    for name in names {
        if let Some(format) = FileFormat::from_path(Path::new(name)) {
            match counts.iter_mut().find(|(f, _)| *f == format) {
                Some((_, n)) => *n += 1,
                None => counts.push((format, 1)),
            }
        }
    }
    counts.into_iter().max_by_key(|(_, n)| *n).map(|(f, _)| f)
}

/// What a path names for the reader: the path itself for a file, or a glob over
/// the files of the format datui read in a directory or a prefix.
fn reader_target(path: &Path, record: &OpenRecord) -> Option<(String, FileFormat, bool)> {
    let text = path.to_string_lossy().to_string();
    if is_url(path) {
        if let Some(format) = file_format(path, record.options) {
            return Some((text, format, false));
        }
        // A prefix: scanned whole, in the format of what it holds.
        let format = record
            .options
            .format
            .or_else(|| commonest_format(record.remote_objects.iter().map(String::as_str)))?;
        let base = text.trim_end_matches('/');
        let ext = format_extension(format)?;
        return Some((format!("{base}/**/*.{ext}"), format, true));
    }
    if path.is_dir() {
        let entries: Vec<std::fs::DirEntry> = std::fs::read_dir(path).ok()?.flatten().collect();
        let mut names: Vec<String> = entries
            .iter()
            .filter(|e| e.path().is_file())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        // Hugging Face's metadata beside Arrow shards is not data.
        if names
            .iter()
            .any(|n| FileFormat::from_path(Path::new(n)) == Some(FileFormat::Arrow))
        {
            names.retain(|n| !crate::discover::is_hugging_face_metadata(n));
        }
        let has_dirs = entries.iter().any(|e| e.path().is_dir());
        let format = record
            .options
            .format
            .or_else(|| commonest_format(names.iter().map(String::as_str)));
        let base = text.trim_end_matches(['/', '\\']);
        // A directory of Parquet with subdirectories, or with no files of its own, is
        // scanned whole for Parquet; otherwise the files of its commonest format.
        return match format {
            Some(FileFormat::Parquet) if has_dirs => {
                Some((format!("{base}/**/*.parquet"), FileFormat::Parquet, true))
            }
            None if has_dirs => Some((format!("{base}/**/*.parquet"), FileFormat::Parquet, true)),
            Some(format) => {
                let ext = format_extension(format)?;
                Some((format!("{base}/*.{ext}"), format, false))
            }
            None => None,
        };
    }
    Some((text, file_format(path, record.options)?, false))
}

/// The Arrow files the paths name, local and in order, when they are IPC streams
/// rather than IPC files.
fn ipc_streams(paths: &[PathBuf]) -> Option<Vec<String>> {
    let mut files = Vec::new();
    for path in paths {
        if is_url(path) {
            return None;
        }
        if path.is_dir() {
            let mut inside: Vec<PathBuf> = std::fs::read_dir(path)
                .ok()?
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_file() && FileFormat::from_path(p) == Some(FileFormat::Arrow))
                .collect();
            inside.sort();
            files.extend(inside);
        } else {
            files.push(path.clone());
        }
    }
    let streams = crate::ipc_stream::streams_among(&files)?.ok()?;
    Some(
        streams
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect(),
    )
}

/// The extension a glob matches for `format`, for the formats datui reads as many
/// files.
fn format_extension(format: FileFormat) -> Option<&'static str> {
    Some(match format {
        FileFormat::Parquet => "parquet",
        FileFormat::Csv => "csv",
        FileFormat::Tsv => "tsv",
        FileFormat::Psv => "psv",
        FileFormat::Jsonl => "jsonl",
        FileFormat::Arrow => "arrow",
        FileFormat::Json => "json",
        FileFormat::Avro => "avro",
        _ => return None,
    })
}

/// The reader for the open, with the options datui gave its own.
pub fn source(record: &OpenRecord) -> Source {
    let Some(paths) = record.paths else {
        return Source::Placeholder {
            what: "The data datui was handed: load it here as a DataFrame or LazyFrame."
                .to_string(),
        };
    };
    if paths.iter().any(|p| crate::stdin::is_stdin(p)) {
        return Source::Placeholder {
            what: "The data datui read from standard input: load it here.".to_string(),
        };
    }
    let targets: Option<Vec<(String, FileFormat, bool)>> =
        paths.iter().map(|p| reader_target(p, record)).collect();
    let Some(targets) = targets.filter(|t| !t.is_empty()) else {
        return Source::Placeholder {
            what: "datui could not name a Polars reader for this data: load it here.".to_string(),
        };
    };
    let format = targets[0].1;
    if targets.iter().any(|t| t.1 != format) {
        return Source::Placeholder {
            what: "The files are of more than one format: load them here.".to_string(),
        };
    }
    let below = targets.iter().any(|t| t.2);
    let names: Vec<String> = targets.iter().map(|t| t.0.clone()).collect();
    let target = match names.as_slice() {
        [one] => py_str(one),
        many => py_names(many),
    };
    let options = record.options;
    let mut args: Vec<String> = vec![target];
    // What the open did to the rows read, as datui recorded it, then the footer.
    let mut after = options.read_python.clone();
    let mut skip_tail = None;
    let mut notes = Vec::new();
    let remote_s3 = names.iter().any(|n| n.starts_with("s3://"));
    let storage = || {
        let mut pairs = Vec::new();
        if let Some(endpoint) = &record.s3_endpoint {
            pairs.push(format!("\"aws_endpoint_url\": {}", py_str(endpoint)));
        }
        if let Some(region) = &record.s3_region {
            pairs.push(format!("\"aws_region\": {}", py_str(region)));
        }
        (!pairs.is_empty()).then(|| format!("storage_options={{{}}}", pairs.join(", ")))
    };
    let call = match format {
        FileFormat::Parquet => {
            if options.hive || below {
                args.push("hive_partitioning=True".to_string());
            }
            if remote_s3 && let Some(s) = storage() {
                args.push(s);
            }
            "pl.scan_parquet"
        }
        FileFormat::Csv | FileFormat::Tsv | FileFormat::Psv => {
            let separator = options
                .delimiter
                .or_else(|| format.separator())
                .unwrap_or(b',');
            if separator != b',' {
                args.push(format!(
                    "separator={}",
                    py_str(&(separator as char).to_string())
                ));
            }
            if options.has_header == Some(false) {
                args.push("has_header=False".to_string());
            }
            if let Some(n) = options.skip_lines {
                args.push(format!("skip_lines={n}"));
            }
            if let Some(n) = options.skip_rows {
                args.push(format!("skip_rows={n}"));
            }
            if let Some(n) = options.infer_schema_length {
                args.push(format!("infer_schema_length={n}"));
            }
            if options.ignore_errors {
                args.push("ignore_errors=True".to_string());
            }
            // A prefix in a bucket is read without it; see `build_lazyframe_from_paths`.
            let bucket_prefix = below && paths.iter().any(|p| is_url(p));
            if options.csv_try_parse_dates() && !bucket_prefix {
                args.push("try_parse_dates=True".to_string());
            }
            if let Some(nulls) = csv_null_values(options, record.schema) {
                args.push(format!("null_values={nulls}"));
            }
            if remote_s3 && let Some(s) = storage() {
                args.push(s);
            }
            if let Some(n) = options.skip_tail_rows.filter(|n| *n > 0) {
                skip_tail = Some(format!(".filter(pl.int_range(pl.len()) < pl.len() - {n})"));
            }
            match options.compression.or_else(|| {
                paths
                    .first()
                    .and_then(|p| CompressionFormat::from_extension(p))
            }) {
                Some(CompressionFormat::Bzip2 | CompressionFormat::Xz) => {
                    return Source::Placeholder {
                        what: format!(
                            "{}: Polars cannot read bzip2 or xz; decompress it and read it with pl.scan_csv.",
                            names.join(", ")
                        ),
                    };
                }
                _ => "pl.scan_csv",
            }
        }
        FileFormat::Jsonl => "pl.scan_ndjson",
        FileFormat::Json => "pl.read_json",
        FileFormat::Arrow => match ipc_streams(paths) {
            // A stream has no footer to scan: read it whole, as datui converts it.
            Some(streams) => {
                let call = match streams.as_slice() {
                    [one] => format!("pl.read_ipc_stream({}).lazy()", py_str(one)),
                    many => format!(
                        "pl.concat([pl.read_ipc_stream(f) for f in {}]).lazy()",
                        py_names(many)
                    ),
                };
                after.extend(skip_tail);
                return Source::Read { call, after, notes };
            }
            None => "pl.scan_ipc",
        },
        FileFormat::Avro => "pl.read_avro",
        FileFormat::Excel => {
            if let Some(sheet) = &options.excel_sheet {
                match sheet.parse::<usize>() {
                    // datui counts sheets from 0, Polars from 1.
                    Ok(i) => args.push(format!("sheet_id={}", i + 1)),
                    Err(_) => args.push(format!("sheet_name={}", py_str(sheet))),
                }
            }
            notes.push(
                "datui types a sheet's columns itself; Polars may read some differently."
                    .to_string(),
            );
            "pl.read_excel"
        }
        FileFormat::Orc | FileFormat::Safetensors | FileFormat::Gguf => {
            return Source::Placeholder {
                what: format!(
                    "{}: Polars has no reader for this format; load it here.",
                    names.join(", ")
                ),
            };
        }
    };
    after.extend(skip_tail);
    if !record.read_as_text.is_empty() {
        notes.push(format!(
            "datui read these columns as text from every file: {}.",
            record.read_as_text.join(", ")
        ));
    }
    let eager = matches!(
        format,
        FileFormat::Json | FileFormat::Avro | FileFormat::Excel
    );
    let mut call = format!("{call}({})", args.join(", "));
    if eager {
        call.push_str(".lazy()");
    }
    Source::Read { call, after, notes }
}

/// `--null-values` as `scan_csv` takes them: one value or a list for every column,
/// or a dict by column. With both, every column gets its own or the first global
/// one, as datui builds them.
fn csv_null_values(options: &OpenOptions, schema: &Schema) -> Option<String> {
    let specs = options.null_values.as_ref().filter(|s| !s.is_empty())?;
    let mut global = Vec::new();
    let mut per_column: Vec<(String, String)> = Vec::new();
    for spec in specs {
        match spec.find('=') {
            Some(i) => per_column.push((spec[..i].to_string(), spec[i + 1..].to_string())),
            None => global.push(spec.clone()),
        }
    }
    let dict = |pairs: Vec<(String, String)>| {
        let items: Vec<String> = pairs
            .iter()
            .map(|(c, v)| format!("{}: {}", py_str(c), py_str(v)))
            .collect();
        format!("{{{}}}", items.join(", "))
    };
    Some(match (global.as_slice(), per_column.is_empty()) {
        ([one], true) => py_str(one),
        (_, true) => py_names(&global),
        ([], false) => dict(per_column),
        (_, false) => dict(
            schema
                .iter_names()
                .map(|name| {
                    let value = per_column
                        .iter()
                        .rev()
                        .find(|(c, _)| c == name.as_str())
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| global[0].clone());
                    (name.to_string(), value)
                })
                .collect(),
        ),
    })
}

/// A value as a Python literal, for a drill's match; None for a type the script
/// cannot spell as one.
pub fn py_value(value: &AnyValue) -> Option<String> {
    Some(match value {
        AnyValue::Null => "None".to_string(),
        AnyValue::Boolean(b) => py_bool(*b).to_string(),
        AnyValue::String(s) => py_str(s),
        AnyValue::StringOwned(s) => py_str(s),
        AnyValue::Int8(v) => v.to_string(),
        AnyValue::Int16(v) => v.to_string(),
        AnyValue::Int32(v) => v.to_string(),
        AnyValue::Int64(v) => v.to_string(),
        AnyValue::UInt8(v) => v.to_string(),
        AnyValue::UInt16(v) => v.to_string(),
        AnyValue::UInt32(v) => v.to_string(),
        AnyValue::UInt64(v) => v.to_string(),
        AnyValue::Float32(v) => py_float(f64::from(*v)),
        AnyValue::Float64(v) => py_float(*v),
        AnyValue::Date(days) => {
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?
                .checked_add_signed(chrono::Duration::days(i64::from(*days)))?;
            use chrono::Datelike;
            format!("pl.date({}, {}, {})", date.year(), date.month(), date.day())
        }
        _ => return None,
    })
}

/// The whole view as a script: the reader, then each step.
#[derive(Debug, Clone, PartialEq)]
pub struct Script {
    pub source: Source,
    pub steps: Vec<Step>,
}

impl Script {
    pub fn render(&self) -> String {
        let mut out = String::from("import polars as pl\n\n");
        let (head, mut lines) = match &self.source {
            Source::Read { call, after, notes } => {
                for note in notes {
                    out.push_str(&format!("# {note}\n"));
                }
                (call.clone(), after.clone())
            }
            Source::Placeholder { what } => {
                out.push_str(&format!("# {what}\ndf = ...\n\n"));
                ("df.lazy()".to_string(), Vec::new())
            }
        };
        // After a step Python cannot repeat, what follows is still said, as
        // comments: run, it would compute something the screen never showed.
        let mut stopped = false;
        for step in &self.steps {
            let calls = step.python();
            if stopped {
                lines.extend(calls.into_iter().map(|c| {
                    if c.starts_with('#') {
                        c
                    } else {
                        format!("# {c}")
                    }
                }));
            } else {
                stopped = matches!(step, Step::Unreproducible(_));
                lines.extend(calls);
            }
        }
        if lines.is_empty() {
            out.push_str(&format!("df = {head}\n"));
        } else {
            out.push_str("df = (\n");
            out.push_str(&format!("    {head}\n"));
            for line in lines {
                out.push_str(&format!("    {line}\n"));
            }
            out.push_str(")\n");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement(column: &str, operator: FilterOperator, value: &str) -> FilterStatement {
        FilterStatement {
            column: column.to_string(),
            operator,
            value: value.to_string(),
            logical_op: LogicalOperator::And,
        }
    }

    fn script(steps: Vec<Step>) -> String {
        Script {
            source: Source::Read {
                call: "pl.scan_parquet(\"sales.parquet\")".to_string(),
                after: Vec::new(),
                notes: Vec::new(),
            },
            steps,
        }
        .render()
    }

    #[test]
    fn strings_and_floats_read_back_in_python() {
        assert_eq!(py_str("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
        assert_eq!(py_str("\u{1}"), "\"\\u0001\"");
        assert_eq!(py_float(5.0), "5.0");
        assert_eq!(py_float(0.1), "0.1");
        assert_eq!(py_float(1e20), "1e20");
        assert_eq!(py_float(f64::NAN), "float(\"nan\")");
    }

    #[test]
    fn a_view_with_nothing_applied_is_the_reader() {
        assert_eq!(
            script(Vec::new()),
            "import polars as pl\n\ndf = pl.scan_parquet(\"sales.parquet\")\n"
        );
    }

    #[test]
    fn filters_typed_by_column_then_a_multi_column_sort_then_a_projection() {
        let schema = Schema::from_iter([
            Field::new("region".into(), DataType::String),
            Field::new("amount".into(), DataType::Float64),
            Field::new("qty".into(), DataType::Int64),
        ]);
        let mut or = statement("qty", FilterOperator::GtEq, "3");
        or.logical_op = LogicalOperator::Or;
        let filters: Vec<SidebarFilter> = [
            statement("region", FilterOperator::Eq, "north"),
            statement("amount", FilterOperator::Gt, "10"),
            or,
        ]
        .iter()
        .map(|s| SidebarFilter::typed(s, schema.get(&s.column)))
        .collect();
        let text = script(vec![
            Step::Filter(filters),
            Step::Sort {
                columns: vec!["amount".into(), "region".into()],
                descending: vec![true, false],
            },
            Step::Select(vec!["order_id".into(), "customer".into(), "amount".into()]),
        ]);
        assert_eq!(
            text,
            "import polars as pl\n\n\
             df = (\n    \
             pl.scan_parquet(\"sales.parquet\")\n    \
             .filter(((pl.col(\"region\") == \"north\") & (pl.col(\"amount\") > 10.0)) | (pl.col(\"qty\") >= 3))\n    \
             .sort([\"amount\", \"region\"], descending=[True, False], nulls_last=True, maintain_order=True)\n    \
             .select([\"order_id\", \"customer\", \"amount\"])\n\
             )\n"
        );
    }

    #[test]
    fn contains_filters_are_literal_and_a_number_that_does_not_parse_stays_text() {
        let s = SidebarFilter::typed(
            &statement("name", FilterOperator::NotContains, "a.b"),
            Some(&DataType::String),
        );
        assert_eq!(
            s.python(),
            "~pl.col(\"name\").str.contains(\"a.b\", literal=True)"
        );
        let s = SidebarFilter::typed(
            &statement("n", FilterOperator::Eq, "n/a"),
            Some(&DataType::Int64),
        );
        assert_eq!(s.value, FilterValue::Str("n/a".into()));
    }

    #[test]
    fn one_sort_column_reads_plainly() {
        assert_eq!(
            sort_call(&["amount".into()], &[true]),
            ".sort(\"amount\", descending=True, nulls_last=True, maintain_order=True)"
        );
    }

    #[test]
    fn steps_after_one_python_cannot_repeat_are_commented_out() {
        let text = script(vec![
            Step::Unreproducible("drilled into a group held as lists".into()),
            Step::Reverse,
        ]);
        assert!(
            text.contains("    # drilled into a group held as lists\n    # .reverse()\n"),
            "{text}"
        );
    }

    #[test]
    fn a_placeholder_source_leaves_df_to_the_user() {
        let text = Script {
            source: Source::Placeholder {
                what: "The data datui read from standard input: load it here.".into(),
            },
            steps: vec![Step::Reverse],
        }
        .render();
        assert_eq!(
            text,
            "import polars as pl\n\n\
             # The data datui read from standard input: load it here.\n\
             df = ...\n\n\
             df = (\n    df.lazy()\n    .reverse()\n)\n"
        );
    }

    #[test]
    fn a_grouped_query_groups_then_orders_by_its_keys() {
        let input = Schema::from_iter([
            Field::new("dept".into(), DataType::String),
            Field::new("salary".into(), DataType::Float64),
            Field::new("id".into(), DataType::Int64),
            Field::new("age".into(), DataType::Int64),
        ]);
        let text = script(vec![Step::Query {
            query: "select avg salary, n: count id by dept where age > 30".into(),
            input: Arc::new(input),
            keys: vec!["dept".into()],
        }]);
        assert!(
            text.contains(
                "    .filter(pl.col(\"age\") > 30.0)\n    \
                 .group_by(\"dept\")\n    \
                 .agg(pl.col(\"salary\").mean().alias(\"avg_salary\"), pl.col(\"id\").count().alias(\"n\"))\n    \
                 .sort(\"dept\", nulls_last=True, maintain_order=True)\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn csv_options_become_reader_arguments() {
        let mut options = OpenOptions::new();
        options.delimiter = Some(b';');
        options.has_header = Some(false);
        options.skip_rows = Some(2);
        options.null_values = Some(vec!["NA".into()]);
        options.skip_tail_rows = Some(1);
        let paths = vec![PathBuf::from("data/x.csv")];
        let schema = Schema::default();
        let record = OpenRecord {
            paths: Some(&paths),
            options: &options,
            schema: &schema,
            remote_objects: Vec::new(),
            s3_endpoint: None,
            s3_region: None,
            read_as_text: Vec::new(),
        };
        let Source::Read { call, after, .. } = source(&record) else {
            panic!("a CSV has a reader");
        };
        assert_eq!(
            call,
            "pl.scan_csv(\"data/x.csv\", separator=\";\", has_header=False, skip_rows=2, \
             try_parse_dates=True, null_values=\"NA\")"
        );
        assert_eq!(
            after,
            vec![".filter(pl.int_range(pl.len()) < pl.len() - 1)"]
        );
    }

    #[test]
    fn stdin_and_bucket_prefixes() {
        let options = OpenOptions::new();
        let schema = Schema::default();
        let stdin = vec![PathBuf::from("-")];
        let record = |paths: &'static [PathBuf]| OpenRecord {
            paths: Some(paths),
            options: &options,
            schema: &schema,
            remote_objects: vec!["s3://b/p/year=2024/a.parquet".into()],
            s3_endpoint: Some("http://localhost:9000".into()),
            s3_region: None,
            read_as_text: Vec::new(),
        };
        let stdin: &'static [PathBuf] = Box::leak(stdin.into_boxed_slice());
        assert!(matches!(source(&record(stdin)), Source::Placeholder { .. }));
        let prefix: &'static [PathBuf] =
            Box::leak(vec![PathBuf::from("s3://b/p/")].into_boxed_slice());
        let Source::Read { call, .. } = source(&record(prefix)) else {
            panic!("a Parquet prefix has a reader");
        };
        assert_eq!(
            call,
            "pl.scan_parquet(\"s3://b/p/**/*.parquet\", hive_partitioning=True, \
             storage_options={\"aws_endpoint_url\": \"http://localhost:9000\"})"
        );
    }
}

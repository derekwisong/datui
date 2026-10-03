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

/// `text` as a Python comment line. A line break or other control character in
/// it (a column name, a value, a path) is written escaped, so the text cannot end
/// the comment and run as code.
pub(crate) fn py_comment(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push_str("# ");
    for c in text.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push('\t'),
            c if c.is_control() || c == '\u{2028}' || c == '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c => out.push(c),
        }
    }
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
                Err(e) => vec![py_comment(&format!("the query did not parse: {e}"))],
            },
            Step::QueryRows { query, input } => match crate::query::parse_nodes(query) {
                Ok(mut nodes) => {
                    nodes.resolve_division(input);
                    nodes.python_filter().into_iter().collect()
                }
                Err(e) => vec![py_comment(&format!("the query did not parse: {e}"))],
            },
            Step::Sql { sql, ordered_by } => {
                let sql = sql.trim();
                // Triple quotes keep a statement's lines, where nothing in it could end
                // or change the string early.
                let verbatim = sql.contains('\n')
                    && !sql.contains("\"\"\"")
                    && !sql.ends_with('"')
                    && sql
                        .chars()
                        .all(|c| c == '\n' || c == '\t' || (c != '\\' && !c.is_control()));
                let mut lines = if verbatim {
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
            Step::Unreproducible(what) => vec![py_comment(what)],
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
        /// Imports the call needs besides Polars: `import sqlite3`.
        imports: Vec<&'static str>,
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
    /// The format the open read, after sniffing and spec matching: what the scan
    /// chose, which the name may not say (a `.bin` DataFlash log, a part file with no
    /// extension). Before `--format` and the extension.
    pub format: Option<FileFormat>,
    /// How datui read the data ([`crate::ReadMode`]), as the Info panel's `Read:` says.
    pub read_mode: Option<crate::ReadMode>,
    /// The data as loaded.
    pub schema: &'a Schema,
    /// Each object a remote dataset reads, for the format of a prefix.
    pub remote_objects: Vec<String>,
    /// S3 endpoint and region in effect, for a bucket that is not AWS's: the source's
    /// own for an `s3://<id>@bucket` URL.
    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    /// The object store was read with no signature: a public bucket.
    pub unsigned: bool,
    /// Columns read as text from every file.
    pub read_as_text: Vec<String>,
    /// The binary format spec the data was read through, by name.
    pub spec: Option<String>,
}

fn is_url(path: &Path) -> bool {
    crate::source::is_remote_url(path)
}

/// `url` without what may be a secret: a user and password before the host, and
/// for HTTP the query string and fragment, where a signed URL keeps its signature
/// or a token. The second value says whether anything was taken out.
fn without_secrets(url: &str) -> (String, bool) {
    // An S3 source ID before the bucket is datui's name for the source, not a user.
    let url = &*crate::source::split_source_id(url).1;
    let Some(scheme_end) = url.find("://").map(|i| i + 3) else {
        return (url.to_string(), false);
    };
    let (scheme, rest) = url.split_at(scheme_end);
    let host_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, path) = rest.split_at(host_end);
    // `abfss://container@account...` names the container there.
    let azure = ["abfs://", "abfss://"]
        .iter()
        .any(|s| scheme.eq_ignore_ascii_case(s));
    let host = match authority.rsplit_once('@') {
        Some((_, host)) if !azure => host,
        _ => authority,
    };
    let http = scheme.eq_ignore_ascii_case("http://") || scheme.eq_ignore_ascii_case("https://");
    let path = match path.find(['?', '#']) {
        Some(i) if http => &path[..i],
        _ => path,
    };
    let kept = format!("{scheme}{host}{path}");
    let cut = kept != url;
    (kept, cut)
}

/// The format a file is read as: what the open read, else `--format`, else its
/// extension, looking through a compression extension (`.csv.gz` is CSV).
fn file_format(path: &Path, record: &OpenRecord) -> Option<FileFormat> {
    record.format.or(record.options.format).or_else(|| {
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

/// What a path names for the reader.
struct Target {
    /// The path itself for a file, or a glob over the files of the format datui read
    /// in a directory or a prefix.
    text: String,
    format: FileFormat,
    /// Read from below a prefix or directory, as Hive partitions may be.
    below: bool,
    /// The text is a pattern for the scan to expand.
    pattern: bool,
    /// The text names one file, with a glob character the scan must not expand.
    literal: bool,
}

/// What `path` names for the reader.
fn reader_target(path: &Path, record: &OpenRecord) -> Option<Target> {
    let text = path.to_string_lossy().to_string();
    if is_url(path) {
        let (text, _) = without_secrets(&text);
        if let Some(format) = file_format(Path::new(&text), record) {
            let pattern = crate::source::has_glob_chars(Path::new(&text));
            return Some(Target {
                text,
                format,
                below: false,
                pattern,
                literal: false,
            });
        }
        // A prefix: scanned whole, in the format of what it holds.
        let format = record
            .format
            .or(record.options.format)
            .or_else(|| commonest_format(record.remote_objects.iter().map(String::as_str)))?;
        let base = text.trim_end_matches('/');
        let ext = format_extension(format)?;
        return Some(Target {
            text: format!("{base}/**/*.{ext}"),
            format,
            below: true,
            pattern: true,
            literal: false,
        });
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
            .format
            .or(record.options.format)
            .or_else(|| commonest_format(names.iter().map(String::as_str)));
        // The directory is there, so its name is no pattern, `[` and all (#625).
        let base = crate::source::escape_glob(text.trim_end_matches(['/', '\\']));
        // A directory of Parquet with subdirectories, or with no files of its own, is
        // scanned whole for Parquet; otherwise the files of its commonest format.
        let (text, format, below) = match format {
            Some(FileFormat::Parquet) | None if has_dirs => {
                (format!("{base}/**/*.parquet"), FileFormat::Parquet, true)
            }
            Some(format) => (
                format!("{base}/*.{}", format_extension(format)?),
                format,
                false,
            ),
            None => return None,
        };
        return Some(Target {
            text,
            format,
            below,
            pattern: true,
            literal: false,
        });
    }
    let format = file_format(path, record)?;
    let pattern = crate::source::expands_as_glob(path);
    Some(Target {
        // Polars' scans read every name as a pattern: an existing `d[1].csv` is that
        // file alone (#625).
        literal: !pattern && scans_by_pattern(format) && crate::source::has_glob_chars(path),
        text,
        format,
        below: false,
        pattern,
    })
}

/// Whether the script reads `format` with a Polars scan, which expands globs.
fn scans_by_pattern(format: FileFormat) -> bool {
    python_of(format).is_some_and(|python| !python.eager)
}

/// The Arrow files of a Hugging Face directory the paths name, local and in order:
/// the split the open read. `None` for any other paths.
fn hugging_face_files(paths: &[PathBuf], table: Option<&str>) -> Option<Vec<String>> {
    let [path] = paths else {
        return None;
    };
    if is_url(path) || !path.is_dir() {
        return None;
    }
    // A DatasetDict's split is a directory of its own.
    let dict_split = crate::hf_splits::dataset_dict(path).and_then(|splits| {
        let listed: Vec<&str> = splits.iter().map(String::as_str).collect();
        crate::hf_splits::pick(&listed, table).ok()?.split
    });
    let dict = dict_split.is_some();
    let table = if dict { None } else { table };
    let path = &dict_split.map_or_else(|| path.clone(), |split| path.join(split));
    let mut inside: Vec<PathBuf> = std::fs::read_dir(path)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file() && FileFormat::from_path(p) == Some(FileFormat::Arrow))
        .collect();
    inside.sort();
    let cache = ["dataset_info.json", "state.json"]
        .iter()
        .any(|name| path.join(name).is_file());
    if !cache && !dict {
        return None;
    }
    if cache {
        let names: Vec<&str> = inside
            .iter()
            .map(|f| f.file_name().and_then(|n| n.to_str()).unwrap_or_default())
            .collect();
        let (chosen, _) = crate::hf_splits::choose(&names, table).ok()?;
        inside = chosen.into_iter().map(|i| inside[i].clone()).collect();
    }
    Some(
        inside
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect(),
    )
}

/// The read of Arrow `inputs`, each a file or URL and whether it is a stream, in
/// order: a stream has no footer to scan, so it is read whole, as datui converts it,
/// and the inputs are stacked as datui stacks them. `extra` goes to every call.
fn arrow_read(inputs: &[(String, bool)], extra: Option<&str>) -> String {
    let extra = extra.map(|e| format!(", {e}")).unwrap_or_default();
    let names: Vec<String> = inputs.iter().map(|(name, _)| name.clone()).collect();
    if inputs.iter().all(|(_, stream)| *stream) {
        return match names.as_slice() {
            [one] => format!("pl.read_ipc_stream({}{extra}).lazy()", py_str(one)),
            many => format!(
                "pl.concat([pl.read_ipc_stream(f{extra}) for f in {}]).lazy()",
                py_names(many)
            ),
        };
    }
    if inputs.iter().all(|(_, stream)| !*stream) {
        return match names.as_slice() {
            [one] => format!("pl.scan_ipc({}{extra})", py_str(one)),
            many => format!("pl.scan_ipc({}{extra})", py_names(many)),
        };
    }
    let reads: Vec<String> = inputs
        .iter()
        .map(|(name, stream)| match stream {
            true => format!("pl.read_ipc_stream({}{extra}).lazy()", py_str(name)),
            false => format!("pl.scan_ipc({}{extra})", py_str(name)),
        })
        .collect();
    format!(
        "pl.concat([{}], how=\"diagonal_relaxed\")",
        reads.join(", ")
    )
}

/// The extension a glob matches for `format`, for the formats datui reads as many
/// files and Polars reads: the first its descriptor lists.
fn format_extension(format: FileFormat) -> Option<&'static str> {
    python_of(format)?;
    let d = format.descriptor();
    (d.many_files || format.separator().is_some())
        .then(|| d.extensions.first().copied())
        .flatten()
}

/// How Copy as Python reads a format with Polars: part of its reader
/// ([`crate::readers::Reader::python`]).
pub(crate) struct Python {
    /// The Polars function: `pl.scan_parquet`.
    pub call: &'static str,
    /// A read into memory rather than a scan: the frame is made lazy after it.
    pub eager: bool,
    /// The scan takes `glob=False`, for a file whose name holds a glob character.
    pub glob_flag: bool,
    /// What the format adds to the call, as datui read it: its arguments, what follows
    /// the call and notes; or the whole source, when the call is not one call.
    pub arguments: Option<fn(&mut Call<'_>) -> Option<Source>>,
}

/// A reader call being written, for a format's [`Python::arguments`].
pub(crate) struct Call<'a> {
    pub record: &'a OpenRecord<'a>,
    pub paths: &'a [PathBuf],
    pub format: FileFormat,
    /// The names read, as the call's first argument says them.
    pub names: &'a [String],
    /// Read from below a prefix or directory, as Hive partitions may be.
    pub below: bool,
    /// The `storage_options` argument the object store read needs, when one is read.
    pub storage: Option<String>,
    pub args: Vec<String>,
    pub after: Vec<String>,
    /// The footer dropped, after what the open did to the rows.
    pub skip_tail: Option<String>,
    pub notes: Vec<String>,
}

impl Call<'_> {
    /// The store's settings, for a call that reads from an object store.
    fn storage_for(&self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Option<String> {
        names
            .into_iter()
            .any(|n| store_scheme(n.as_ref()).is_some())
            .then(|| self.storage.clone())
            .flatten()
    }
}

/// The object store `name` is in: `s3`, `gs` or `azure`; `None` for a local path or
/// an HTTP(S) URL.
fn store_scheme(name: &str) -> Option<&'static str> {
    let (scheme, _) = name.split_once("://")?;
    match scheme.to_ascii_lowercase().as_str() {
        "s3" | "s3a" => Some("s3"),
        "gs" | "gcs" => Some("gs"),
        "az" | "adl" | "azure" | "abfs" | "abfss" => Some("azure"),
        _ => None,
    }
}

/// `storage_options` for reading `name`, with what the open read it with that is not a
/// secret: S3's endpoint and region, an Azure account, and no signature for a public
/// place. Credentials stay where Polars finds them, as datui found them.
fn storage_options(name: &str, record: &OpenRecord, endpoint: Option<&str>) -> Option<String> {
    let mut pairs: Vec<(&str, String)> = Vec::new();
    match store_scheme(name)? {
        "s3" => {
            pairs.extend(endpoint.map(|e| ("aws_endpoint_url", e.to_string())));
            pairs.extend(record.s3_region.clone().map(|r| ("aws_region", r)));
        }
        "azure" => {
            pairs.extend(
                crate::source::azure_parts(name).map(|(account, ..)| ("account_name", account)),
            );
        }
        _ => {}
    }
    if record.unsigned {
        pairs.push(("skip_signature", "true".to_string()));
    }
    let pairs: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{}: {}", py_str(k), py_str(v)))
        .collect();
    (!pairs.is_empty()).then(|| format!("storage_options={{{}}}", pairs.join(", ")))
}

/// NDJSON: the store's settings.
pub(crate) fn ndjson_arguments(call: &mut Call<'_>) -> Option<Source> {
    if let Some(s) = call.storage_for(call.names) {
        call.args.push(s);
    }
    None
}

fn python_of(format: FileFormat) -> Option<&'static Python> {
    crate::readers::of(format).python.as_ref()
}

/// Parquet: hive partitions under a directory or prefix, and S3 settings.
pub(crate) fn parquet_arguments(call: &mut Call<'_>) -> Option<Source> {
    if call.record.options.hive || call.below {
        call.args.push("hive_partitioning=True".to_string());
    }
    if let Some(s) = call.storage_for(call.names) {
        call.args.push(s);
    }
    None
}

/// CSV, TSV and PSV: the dialect datui read with, where Polars has it.
pub(crate) fn csv_arguments(call: &mut Call<'_>) -> Option<Source> {
    let options = call.record.options;
    let names = call.names.join(", ");
    // Polars takes a comment prefix of up to five characters.
    let comment = options.comment_char.as_deref().filter(|c| !c.is_empty());
    let dialect: Vec<&str> = [
        (comment.is_some_and(|c| c.len() > 5), "--comment-char"),
        (options.header_rows().is_some(), "--header-rows"),
        (options.skip_initial_space, "--skip-initial-space"),
    ]
    .into_iter()
    .filter_map(|(set, flag)| set.then_some(flag))
    .collect();
    if !dialect.is_empty() {
        return Some(Source::Placeholder {
            what: format!(
                "{names}: datui read it with {}, which it cannot write as Python: load it here.",
                dialect.join(", ")
            ),
        });
    }
    let separator = options
        .delimiter
        .or_else(|| call.format.separator())
        .unwrap_or(b',');
    let args = &mut call.args;
    if separator != b',' {
        args.push(format!(
            "separator={}",
            py_str(&(separator as char).to_string())
        ));
    }
    if let Some(prefix) = comment {
        args.push(format!("comment_prefix={}", py_str(prefix)));
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
    let bucket_prefix = call.below && call.paths.iter().any(|p| is_url(p));
    if options.csv_try_parse_dates() && !bucket_prefix {
        call.args.push("try_parse_dates=True".to_string());
    }
    if let Some(nulls) = csv_null_values(options, call.record.schema) {
        call.args.push(format!("null_values={nulls}"));
    }
    if let Some(s) = call.storage_for(call.names) {
        call.args.push(s);
    }
    if let Some(n) = options.skip_tail_rows.filter(|n| *n > 0) {
        call.skip_tail = Some(format!(".filter(pl.int_range(pl.len()) < pl.len() - {n})"));
    }
    match options.compression.or_else(|| {
        call.paths
            .first()
            .and_then(|p| CompressionFormat::from_extension(p))
    }) {
        Some(CompressionFormat::Bzip2 | CompressionFormat::Xz) => Some(Source::Placeholder {
            what: format!(
                "{names}: Polars cannot read bzip2 or xz; decompress it and read it with pl.scan_csv."
            ),
        }),
        _ => None,
    }
}

/// Arrow: what the open read, each input's kind, after a conversion or a bucket's
/// listing, or the split of a Hugging Face directory of IPC files.
pub(crate) fn arrow_arguments(call: &mut Call<'_>) -> Option<Source> {
    let options = call.record.options;
    let inputs: Option<Vec<(String, bool)>> = match &options.arrow_parts {
        Some(parts) => Some(
            parts
                .iter()
                .map(|part| match part {
                    crate::ipc_stream::Part::InPlace(p) => (p, false),
                    crate::ipc_stream::Part::Converted { source, .. } => (source, true),
                })
                .map(|(p, stream)| (without_secrets(&p.to_string_lossy()).0, stream))
                .collect(),
        ),
        None => hugging_face_files(call.paths, options.table.as_deref())
            .map(|files| files.into_iter().map(|f| (f, false)).collect()),
    };
    match inputs {
        Some(inputs) => {
            let extra = call.storage_for(inputs.iter().map(|(name, _)| name));
            let read = arrow_read(&inputs, extra.as_deref());
            let mut after = std::mem::take(&mut call.after);
            after.extend(call.skip_tail.take());
            Some(Source::Read {
                call: read,
                after,
                notes: std::mem::take(&mut call.notes),
                imports: Vec::new(),
            })
        }
        None => {
            if let Some(s) = call.storage_for(call.names) {
                call.args.push(s);
            }
            None
        }
    }
}

/// Excel: the sheet, counted as Polars counts it.
pub(crate) fn excel_arguments(call: &mut Call<'_>) -> Option<Source> {
    if let Some(sheet) = &call.record.options.excel_sheet {
        match sheet.parse::<usize>() {
            // datui counts sheets from 0, Polars from 1.
            Ok(i) => call.args.push(format!("sheet_id={}", i + 1)),
            Err(_) => call.args.push(format!("sheet_name={}", py_str(sheet))),
        }
    }
    call.notes.push(
        "datui types a sheet's columns itself; Polars may read some differently.".to_string(),
    );
    None
}

/// `name` as an SQL identifier, quoted.
fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The source a format read whole by a call of its own, with the steps the open
/// recorded after it.
fn whole(call: &mut Call<'_>, read: String, imports: Vec<&'static str>) -> Source {
    call.notes.extend(read_whole_note(call.record, &read));
    let mut after = std::mem::take(&mut call.after);
    after.extend(call.skip_tail.take());
    Source::Read {
        call: read,
        after,
        notes: std::mem::take(&mut call.notes),
        imports,
    }
}

/// What the script's read costs where datui's did not: the `Read:` fact the Info
/// panel states, beside the call that reads the file whole.
fn read_whole_note(record: &OpenRecord, call: &str) -> Option<String> {
    let name = call.split('(').next().unwrap_or(call);
    (record.read_mode == Some(crate::ReadMode::Lazy)).then(|| {
        format!(
            "Read: {} in datui; {name} reads the file whole into memory.",
            crate::ReadMode::Lazy.label()
        )
    })
}

/// SQLite: the table on screen, read through Python's own `sqlite3`.
pub(crate) fn sqlite_arguments(call: &mut Call<'_>) -> Option<Source> {
    let [file] = call.names else {
        return None;
    };
    let Some(table) = call.record.options.table.as_deref() else {
        return Some(Source::Placeholder {
            what: format!("{file}: datui could not tell which table it read; load it here."),
        });
    };
    if call.paths.iter().any(|p| is_url(p)) {
        return Some(Source::Placeholder {
            what: format!(
                "{file} --table {table}: sqlite3 opens a local file; download it and read it \
                 with pl.read_database."
            ),
        });
    }
    call.notes.push(
        "datui types a table's columns from their declared types; Polars infers them from \
         the values."
            .to_string(),
    );
    let query = format!("SELECT * FROM {}", sql_ident(table));
    let read = format!(
        "pl.read_database({}, sqlite3.connect({})).lazy()",
        py_str(&query),
        py_str(file)
    );
    Some(whole(call, read, vec!["import sqlite3"]))
}

/// NumPy: the array on screen, loaded with NumPy and named as datui names its columns.
pub(crate) fn numpy_arguments(call: &mut Call<'_>) -> Option<Source> {
    let [file] = call.names else {
        return None;
    };
    let table = call.record.options.table.as_deref();
    let archive = table.is_some() || file.to_ascii_lowercase().ends_with(".npz");
    let array = match table {
        Some(name) => format!("np.load({})[{}]", py_str(file), py_str(name)),
        // An archive of one array opens it.
        None if archive => format!("next(iter(np.load({}).values()))", py_str(file)),
        None => format!("np.load({})", py_str(file)),
    };
    let names: Vec<String> = call
        .record
        .schema
        .iter_names()
        .map(|n| n.to_string())
        .collect();
    // A nested field is a struct in Polars and `outer.inner` columns in datui.
    let schema = if names.iter().any(|n| n.contains('.')) {
        call.notes.push(
            "datui names a nested field's columns outer.inner; Polars keeps the field as a struct."
                .to_string(),
        );
        String::new()
    } else {
        format!(", schema={}", py_names(&names))
    };
    let read = format!("pl.from_numpy({array}{schema}, orient=\"row\").lazy()");
    Some(whole(call, read, vec!["import numpy as np"]))
}

/// `names`, and the table picked inside them where the open named one, as the
/// placeholders say what to load: `log.bin --table GPS`.
fn named_with_table(names: &[String], record: &OpenRecord) -> String {
    let names = names.join(", ");
    match record.options.table.as_deref() {
        Some(table) => format!("{names} --table {table}"),
        None => names,
    }
}

/// Text read as lines: the file split at its newlines as datui splits it, numbered
/// from 1.
pub(crate) fn lines_arguments(call: &mut Call<'_>) -> Option<Source> {
    let names = call.names.join(", ");
    let path = match call.paths {
        [one]
            if !is_url(one)
                && !one.is_dir()
                && CompressionFormat::from_extension(one).is_none() =>
        {
            one
        }
        _ => {
            return Some(Source::Placeholder {
                what: format!("{names}: datui read it as lines; load it here."),
            });
        }
    };
    let read = format!(
        "pl.LazyFrame({{\"line\": open({}, encoding=\"utf-8\", errors=\"replace\", newline=\"\").read().removesuffix(\"\\n\").split(\"\\n\")}})",
        py_str(&path.to_string_lossy())
    );
    let mut after = vec![
        ".with_columns(pl.col(\"line\").str.strip_suffix(\"\\r\"))".to_string(),
        ".with_row_index(\"line_no\", offset=1)".to_string(),
    ];
    after.append(&mut call.after);
    Some(Source::Read {
        call: read,
        after,
        notes: std::mem::take(&mut call.notes),
        imports: Vec::new(),
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
    // Standard input recorded with `--tee` is read again from its file.
    let teed;
    let paths = match (&record.options.tee, paths) {
        (Some(tee), [one]) if crate::stdin::is_stdin(one) => {
            teed = [tee.clone()];
            &teed[..]
        }
        _ => paths,
    };
    if paths.iter().any(|p| crate::stdin::is_stdin(p)) {
        return Source::Placeholder {
            what: "The data datui read from standard input: load it here.".to_string(),
        };
    }
    let spec = record.spec.clone().or_else(|| {
        let options = record.options;
        options
            .spec_name
            .clone()
            .or_else(|| options.spec_file.as_ref().map(|f| f.display().to_string()))
    });
    if let Some(spec) = spec {
        return Source::Placeholder {
            what: format!(
                "datui read this through the format spec {spec}, which it cannot write as \
                 Python: load it here."
            ),
        };
    }
    let targets: Option<Vec<Target>> = paths.iter().map(|p| reader_target(p, record)).collect();
    let Some(targets) = targets.filter(|t| !t.is_empty()) else {
        let names: Vec<String> = paths
            .iter()
            .map(|p| without_secrets(&p.to_string_lossy()).0)
            .collect();
        return Source::Placeholder {
            what: format!(
                "{}: datui could not name a Polars reader for this data; load it here.",
                named_with_table(&names, record)
            ),
        };
    };
    let format = targets[0].format;
    if targets.iter().any(|t| t.format != format) {
        return Source::Placeholder {
            what: "The files are of more than one format: load them here.".to_string(),
        };
    }
    let below = targets.iter().any(|t| t.below);
    let python = python_of(format);
    // A file named like a glob is read with `glob=False`, unless the scan has no such
    // flag (NDJSON) or another name is a pattern; then its name is escaped instead.
    let literal = targets.iter().any(|t| t.literal);
    let no_glob = literal
        && python.is_some_and(|python| python.glob_flag)
        && !targets.iter().any(|t| t.pattern);
    let names: Vec<String> = targets
        .into_iter()
        .map(|t| {
            if t.literal && !no_glob {
                crate::source::escape_glob(&t.text)
            } else {
                t.text
            }
        })
        .collect();
    let Some(python) = python else {
        return Source::Placeholder {
            what: format!(
                "{}: Polars has no reader for {} files; load it here.",
                named_with_table(&names, record),
                format.title()
            ),
        };
    };
    let target = match names.as_slice() {
        [one] => py_str(one),
        many => py_names(many),
    };
    let options = record.options;
    let mut args: Vec<String> = vec![target];
    if no_glob {
        args.push("glob=False".to_string());
    }
    let mut notes = Vec::new();
    let endpoint = record.s3_endpoint.as_deref().map(without_secrets);
    if paths
        .iter()
        .any(|p| is_url(p) && without_secrets(&p.to_string_lossy()).1)
        || endpoint.as_ref().is_some_and(|(_, cut)| *cut)
    {
        notes.push(
            "datui left a user, password or query string out of the URL, as it may be a \
             credential: add it back if the server needs it."
                .to_string(),
        );
    }
    let in_store = names.iter().find(|n| store_scheme(n).is_some());
    let storage = in_store
        .and_then(|name| storage_options(name, record, endpoint.as_ref().map(|(e, _)| e.as_str())));
    // The readers that read a file whole take no `storage_options`.
    if let Some(name) = in_store
        && python.eager
    {
        notes.push(format!(
            "{} reads no object store: download {name} and read it from disk.",
            python.call
        ));
    }
    let mut call = Call {
        record,
        paths,
        format,
        names: &names,
        below,
        storage,
        args,
        // What the open did to the rows read, as datui recorded it, then the footer.
        after: options.read_python.clone(),
        skip_tail: None,
        notes,
    };
    if let Some(arguments) = python.arguments
        && let Some(source) = arguments(&mut call)
    {
        return source;
    }
    let Call {
        args,
        mut after,
        skip_tail,
        mut notes,
        ..
    } = call;
    after.extend(skip_tail);
    if !record.read_as_text.is_empty() {
        notes.push(format!(
            "datui read these columns as text from every file: {}.",
            record.read_as_text.join(", ")
        ));
    }
    let mut call = format!("{}({})", python.call, args.join(", "));
    if python.eager {
        notes.extend(read_whole_note(record, &call));
        call.push_str(".lazy()");
    }
    Source::Read {
        call,
        after,
        notes,
        imports: Vec::new(),
    }
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
        let mut out = String::from("import polars as pl\n");
        if let Source::Read { imports, .. } = &self.source {
            for import in imports {
                out.push_str(import);
                out.push('\n');
            }
        }
        out.push('\n');
        let (head, mut lines) = match &self.source {
            Source::Read {
                call, after, notes, ..
            } => {
                for note in notes {
                    out.push_str(&py_comment(note));
                    out.push('\n');
                }
                (call.clone(), after.clone())
            }
            Source::Placeholder { what } => {
                out.push_str(&py_comment(what));
                out.push_str("\ndf = ...\n\n");
                ("df.lazy()".to_string(), Vec::new())
            }
        };
        // After a step Python cannot repeat, what follows is still said, as
        // comments: run, it would compute something the screen never showed.
        let mut stopped = false;
        for step in &self.steps {
            let calls = step.python();
            if stopped {
                // Line by line: a call can span lines, as a triple-quoted SQL does.
                for call in &calls {
                    lines.extend(call.lines().map(|c| {
                        if c.starts_with('#') {
                            c.to_string()
                        } else {
                            format!("# {c}")
                        }
                    }));
                }
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
                imports: Vec::new(),
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
    fn text_in_a_comment_cannot_end_it() {
        assert_eq!(
            py_comment("a\nimport os\r\u{2028}x"),
            "# a\\nimport os\\r\\u2028x"
        );
        let text = script(vec![Step::Unreproducible("where k is \"\nboom()".into())]);
        assert!(text.contains("    # where k is \"\\nboom()\n"), "{text}");
    }

    #[test]
    fn sql_is_triple_quoted_only_where_nothing_in_it_ends_the_string() {
        let sql = |sql: &str| {
            Step::Sql {
                sql: sql.into(),
                ordered_by: Vec::new(),
            }
            .python()
        };
        assert_eq!(
            sql("SELECT *\nFROM df"),
            vec![
                ".sql(",
                "    \"\"\"SELECT *\nFROM df\"\"\",",
                "    table_name=\"df\",",
                ")"
            ]
        );
        assert_eq!(
            sql("SELECT *\nFROM df ORDER BY \"a\""),
            vec![".sql(\"SELECT *\\nFROM df ORDER BY \\\"a\\\"\", table_name=\"df\")"]
        );
        // Commented out, every line of it is a comment.
        let text = script(vec![
            Step::Unreproducible("drilled into a group held as lists".into()),
            Step::Sql {
                sql: "SELECT *\nFROM df".into(),
                ordered_by: Vec::new(),
            },
        ]);
        assert!(text.contains("    # FROM df\"\"\",\n"), "{text}");
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
            unsigned: false,
            format: None,
            read_mode: None,
            read_as_text: Vec::new(),
            spec: None,
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

    /// Reads datui does its own way are not written as a Polars reader that would
    /// give other rows: a format spec, a GPS log, and the CSV dialect flags.
    #[test]
    fn reads_python_cannot_repeat_leave_a_placeholder() {
        let schema = Schema::default();
        let placeholder = |paths: &[PathBuf], options: &OpenOptions, spec: Option<&str>| {
            let record = OpenRecord {
                paths: Some(paths),
                options,
                schema: &schema,
                remote_objects: Vec::new(),
                s3_endpoint: None,
                s3_region: None,
                unsigned: false,
                format: None,
                read_mode: None,
                read_as_text: Vec::new(),
                spec: spec.map(str::to_string),
            };
            match source(&record) {
                Source::Placeholder { what } => what,
                Source::Read { call, .. } => panic!("a reader was written: {call}"),
            }
        };
        let plain = OpenOptions::new();
        let what = placeholder(&[PathBuf::from("a.l2")], &plain, Some("acme.l2feed"));
        assert!(what.contains("acme.l2feed"), "{what}");
        let mut named = OpenOptions::new();
        named.spec_name = Some("acme.l2feed".into());
        placeholder(&[PathBuf::from("a.bin")], &named, None);
        placeholder(&[PathBuf::from("track.gpx")], &plain, None);
        placeholder(&[PathBuf::from("drive.nmea")], &plain, None);
        let csv = [PathBuf::from("log.csv")];
        let mut comment = OpenOptions::new();
        comment.comment_char = Some("######".into());
        assert!(placeholder(&csv, &comment, None).contains("--comment-char"));
        let mut rows = OpenOptions::new();
        rows.header_rows = vec![3, 2];
        assert!(placeholder(&csv, &rows, None).contains("--header-rows"));
        let mut space = OpenOptions::new();
        space.skip_initial_space = true;
        assert!(placeholder(&csv, &space, None).contains("--skip-initial-space"));
    }

    fn record_for<'a>(
        paths: &'a [PathBuf],
        options: &'a OpenOptions,
        schema: &'a Schema,
    ) -> OpenRecord<'a> {
        OpenRecord {
            paths: Some(paths),
            options,
            format: None,
            read_mode: None,
            schema,
            remote_objects: Vec::new(),
            s3_endpoint: None,
            s3_region: None,
            unsigned: false,
            read_as_text: Vec::new(),
            spec: None,
        }
    }

    fn call_of(source: Source) -> (String, Vec<String>) {
        match source {
            Source::Read { call, notes, .. } => (call, notes),
            Source::Placeholder { what } => panic!("a placeholder: {what}"),
        }
    }

    /// Every reader of an object store gets its settings: S3's endpoint and region
    /// for NDJSON as for Parquet, an Azure account, and no signature for a public
    /// place. A reader that reads a file whole says it reads no store.
    #[test]
    fn every_store_reader_gets_its_storage_options() {
        let schema = Schema::default();
        let options = OpenOptions::new();
        let s3 = [PathBuf::from("s3://b/logs/a.jsonl")];
        let mut record = record_for(&s3, &options, &schema);
        record.s3_endpoint = Some("http://localhost:9000".into());
        record.s3_region = Some("us-east-1".into());
        assert_eq!(
            call_of(source(&record)).0,
            "pl.scan_ndjson(\"s3://b/logs/a.jsonl\", storage_options={\"aws_endpoint_url\": \
             \"http://localhost:9000\", \"aws_region\": \"us-east-1\"})"
        );
        let gcs = [PathBuf::from("gs://public/x.parquet")];
        let mut record = record_for(&gcs, &options, &schema);
        record.unsigned = true;
        assert_eq!(
            call_of(source(&record)).0,
            "pl.scan_parquet(\"gs://public/x.parquet\", storage_options={\"skip_signature\": \"true\"})"
        );
        let azure = [PathBuf::from(
            "abfss://data@acct.dfs.core.windows.net/t/x.csv",
        )];
        let (call, notes) = call_of(source(&record_for(&azure, &options, &schema)));
        assert!(
            call.starts_with("pl.scan_csv(\"abfss://data@acct.dfs.core.windows.net/t/x.csv\", ")
                && call.contains("storage_options={\"account_name\": \"acct\"}"),
            "{call}"
        );
        assert!(
            notes.is_empty(),
            "the container is no credential: {notes:?}"
        );
        let json = [PathBuf::from("s3://b/x.json")];
        let (call, notes) = call_of(source(&record_for(&json, &options, &schema)));
        assert_eq!(call, "pl.read_json(\"s3://b/x.json\").lazy()");
        assert!(notes[0].contains("reads no object store"), "{notes:?}");
    }

    /// An `s3://<id>@bucket` URL is read as the plain URL, with no word of a
    /// credential left out: the ID is datui's name for the source.
    #[test]
    fn a_source_id_is_no_credential() {
        assert_eq!(
            without_secrets("s3://minio@bucket/x.parquet"),
            ("s3://bucket/x.parquet".to_string(), false)
        );
        assert_eq!(
            without_secrets("abfss://c@a.dfs.core.windows.net/x"),
            ("abfss://c@a.dfs.core.windows.net/x".to_string(), false)
        );
    }

    /// Standard input recorded with `--tee` is read again from the file.
    #[test]
    fn a_teed_pipe_reads_its_file() {
        let schema = Schema::default();
        let mut options = OpenOptions::new();
        options.tee = Some(PathBuf::from("rec.csv"));
        let stdin = [PathBuf::from("-")];
        let mut record = record_for(&stdin, &options, &schema);
        record.format = Some(FileFormat::Csv);
        assert_eq!(
            call_of(source(&record)).0,
            "pl.scan_csv(\"rec.csv\", try_parse_dates=True)"
        );
    }

    /// A table datui read lazily that the script reads whole says so, as the Info
    /// panel's `Read:` line does; one datui read in memory too says nothing more.
    #[test]
    fn a_whole_read_of_a_lazy_table_says_so() {
        let schema = Schema::default();
        let mut options = OpenOptions::new();
        options.table = Some("orders".into());
        let db = [PathBuf::from("shop.db")];
        let mut record = record_for(&db, &options, &schema);
        record.format = Some(FileFormat::Sqlite);
        record.read_mode = Some(crate::ReadMode::Lazy);
        let (call, notes) = call_of(source(&record));
        assert!(call.starts_with("pl.read_database("), "{call}");
        assert!(
            notes.contains(
                &"Read: lazy in datui; pl.read_database reads the file whole into memory."
                    .to_string()
            ),
            "{notes:?}"
        );
        let json = [PathBuf::from("a.json")];
        let mut record = record_for(&json, &options, &schema);
        record.read_mode = Some(crate::ReadMode::InMemory);
        assert!(call_of(source(&record)).1.is_empty());
    }

    /// `--comment-char` is Polars' `comment_prefix`.
    #[test]
    fn a_comment_character_is_the_comment_prefix() {
        let schema = Schema::default();
        let mut options = OpenOptions::new();
        options.comment_char = Some("#".into());
        let csv = [PathBuf::from("log.csv")];
        assert_eq!(
            call_of(source(&record_for(&csv, &options, &schema))).0,
            "pl.scan_csv(\"log.csv\", comment_prefix=\"#\", try_parse_dates=True)"
        );
    }

    /// A file known by its bytes, read as a format Polars has no reader for: the
    /// placeholder names the format and the table on screen.
    #[test]
    fn a_placeholder_names_the_format_read_and_the_table() {
        let schema = Schema::default();
        let paths = vec![PathBuf::from("flight.bin")];
        let mut options = OpenOptions::new();
        options.table = Some("GPS".into());
        let record = OpenRecord {
            paths: Some(&paths),
            options: &options,
            format: Some(FileFormat::Dataflash),
            read_mode: None,
            schema: &schema,
            remote_objects: Vec::new(),
            s3_endpoint: None,
            s3_region: None,
            unsigned: false,
            read_as_text: Vec::new(),
            spec: None,
        };
        let Source::Placeholder { what } = source(&record) else {
            panic!("Polars reads no DataFlash");
        };
        assert_eq!(
            what,
            "flight.bin --table GPS: Polars has no reader for DataFlash files; load it here."
        );
    }

    #[test]
    fn credentials_in_a_url_stay_out_of_the_script() {
        assert_eq!(
            without_secrets("https://u:p@host.example/d/x.parquet?X-Amz-Signature=abc#f"),
            ("https://host.example/d/x.parquet".to_string(), true)
        );
        assert_eq!(
            without_secrets("s3://bucket/data-?.parquet"),
            ("s3://bucket/data-?.parquet".to_string(), false)
        );
        let options = OpenOptions::new();
        let schema = Schema::default();
        let paths = vec![PathBuf::from(
            "https://user:secret@host.example/d/x.csv?token=s3cr3t",
        )];
        let record = OpenRecord {
            paths: Some(&paths),
            options: &options,
            schema: &schema,
            remote_objects: Vec::new(),
            s3_endpoint: Some("http://key:secret@localhost:9000".into()),
            s3_region: None,
            unsigned: false,
            format: None,
            read_mode: None,
            read_as_text: Vec::new(),
            spec: None,
        };
        let text = Script {
            source: source(&record),
            steps: Vec::new(),
        }
        .render();
        assert!(
            !text.contains("secret") && !text.contains("s3cr3t"),
            "{text}"
        );
        assert!(text.contains("\"https://host.example/d/x.csv\""), "{text}");
        assert!(text.contains("# datui left a user"), "{text}");
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
            unsigned: false,
            format: None,
            read_mode: None,
            read_as_text: Vec::new(),
            spec: None,
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

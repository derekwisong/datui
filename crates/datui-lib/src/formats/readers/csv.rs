//! Delimited text through Polars' CSV reader: CSV, TSV and PSV, compressed or not, and
//! the typing of string columns that CSV and JSON reads share.

use std::fs::File;
use std::io::{BufReader, Read as _};
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use polars::io::csv::read::NullValues;
use polars::prelude::*;
use tempfile::NamedTempFile;

use super::{Read, Typing};
use crate::analysis::statistics::collect_lazy;
use crate::loading::unfinished::{Claim, Writer};
use crate::python_script::py_str;
use crate::{CompressionFormat, OpenOptions, ParseStringsTarget};

/// Decompress `path` into a new file in `temp_dir`, claimed through `writer` (see
/// [`crate::loading::unfinished`]) and given up, removed, once its open is stopped.
fn decompress_compressed_csv_to_temp(
    path: &Path,
    compression: CompressionFormat,
    temp_dir: &Path,
    writer: &Writer,
) -> Result<Decompressed> {
    let stopped = || color_eyre::eyre::eyre!("Decompressing was stopped.");
    let Some((file, claim)) = writer.create(|| NamedTempFile::new_in(temp_dir))? else {
        return Err(stopped());
    };
    // Held from here, so a failure drops the file before the claim.
    let mut temp = Decompressed {
        file,
        _claim: claim,
    };
    let out = temp.file.as_file_mut();
    let mut reader: Box<dyn std::io::Read> = match compression {
        CompressionFormat::Gzip => {
            let f = File::open(path)?;
            Box::new(flate2::read::GzDecoder::new(BufReader::new(f)))
        }
        CompressionFormat::Zstd => {
            let f = File::open(path)?;
            Box::new(zstd::Decoder::new(BufReader::new(f))?)
        }
        CompressionFormat::Bzip2 => {
            let f = File::open(path)?;
            Box::new(bzip2::read::BzDecoder::new(BufReader::new(f)))
        }
        CompressionFormat::Xz => {
            let f = File::open(path)?;
            Box::new(xz2::read::XzDecoder::new(BufReader::new(f)))
        }
    };
    // A chunk at a time, so a stopped open stops writing rather than finishing a
    // file nobody will read.
    let mut chunk = vec![0u8; 1 << 20];
    loop {
        if writer.stopped() {
            return Err(stopped());
        }
        let read = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        std::io::Write::write_all(out, &chunk[..read])?;
    }
    out.sync_all()?;
    Ok(temp)
}

/// Parse null value specs: "VAL" -> global, "COL=VAL" -> per-column (first '=' separates).
fn parse_null_value_specs(specs: &[String]) -> (Vec<String>, Vec<(String, String)>) {
    let mut global = Vec::new();
    let mut per_column = Vec::new();
    for s in specs {
        if let Some(i) = s.find('=') {
            let (col, val) = (s[..i].to_string(), s[i + 1..].to_string());
            per_column.push((col, val));
        } else {
            global.push(s.clone());
        }
    }
    (global, per_column)
}

/// Build Polars NullValues from parsed specs. When both global and per_column are set, schema is required (caller does schema scan).
fn build_polars_null_values(
    global: &[String],
    per_column: &[(String, String)],
    schema: Option<&Schema>,
) -> Option<NullValues> {
    if global.is_empty() && per_column.is_empty() {
        return None;
    }
    if per_column.is_empty() {
        let vals: Vec<PlSmallStr> = global
            .iter()
            .map(|s| PlSmallStr::from(s.as_str()))
            .collect();
        return Some(if vals.len() == 1 {
            NullValues::AllColumnsSingle(vals[0].clone())
        } else {
            NullValues::AllColumns(vals)
        });
    }
    if global.is_empty() {
        let pairs: Vec<(PlSmallStr, PlSmallStr)> = per_column
            .iter()
            .map(|(c, v)| (PlSmallStr::from(c.as_str()), PlSmallStr::from(v.as_str())))
            .collect();
        return Some(NullValues::Named(pairs));
    }
    let schema = schema?;
    let mut pairs: Vec<(PlSmallStr, PlSmallStr)> = Vec::new();
    let first_global = PlSmallStr::from(global[0].as_str());
    for (name, _) in schema.iter() {
        let col_name = name.as_str();
        let val = per_column
            .iter()
            .rev()
            .find(|(c, _)| c == col_name)
            .map(|(_, v)| PlSmallStr::from(v.as_str()))
            .unwrap_or_else(|| first_global.clone());
        pairs.push((PlSmallStr::from(col_name), val));
    }
    Some(NullValues::Named(pairs))
}

/// Every reader-level CSV option, set one way on every route that scans lazily: a
/// file, several files, a decompressed temp file, and a prefix in a bucket.
pub(crate) fn configure_csv_reader(
    mut reader: LazyCsvReader,
    options: &OpenOptions,
    null_values: Option<&NullValues>,
) -> LazyCsvReader {
    reader = reader
        .with_separator(options.separator_or(b','))
        .with_comment_prefix(options.comment_char.as_deref().map(PlSmallStr::from));
    if let Some(rows) = options.header_rows() {
        // The header lines are read apart (`csv_header_names`); Polars starts
        // after the last of them, with `--skip-lines` counted from the same top
        // and `--skip-rows` counted after.
        let last = rows.iter().copied().max().unwrap_or(0);
        reader = reader
            .with_has_header(false)
            .with_skip_lines(last.max(options.skip_lines.unwrap_or(0)))
            .with_skip_rows_after_header(options.skip_rows.unwrap_or(0));
    } else {
        if let Some(skip_lines) = options.skip_lines {
            reader = reader.with_skip_lines(skip_lines);
        }
        if let Some(skip_rows) = options.skip_rows {
            reader = reader.with_skip_rows(skip_rows);
        }
        if let Some(has_header) = options.has_header {
            reader = reader.with_has_header(has_header);
        }
    }
    if let Some(n) = options.infer_schema_length {
        reader = reader.with_infer_schema_length(Some(n));
    }
    reader
        .with_ignore_errors(options.ignore_errors)
        // A followed file's later rows may have a field too many; they are counted
        // as not fitting rather than failing the read.
        .with_truncate_ragged_lines(options.follow)
        .with_try_parse_dates(options.csv_try_parse_dates())
        .with_null_values(null_values.cloned())
        // One byte that is not UTF-8 is a U+FFFD where it stands, not a file that
        // cannot be read past it.
        .with_encoding(CsvEncoding::LossyUtf8)
}

/// [`configure_csv_reader`] for the in-memory readers, which take options
/// rather than a builder.
fn eager_csv_read_options(
    options: &OpenOptions,
    null_values: Option<&NullValues>,
) -> CsvReadOptions {
    let mut read_options = CsvReadOptions::default();
    if let Some(rows) = options.header_rows() {
        let last = rows.iter().copied().max().unwrap_or(0);
        read_options.has_header = false;
        read_options.skip_lines = last.max(options.skip_lines.unwrap_or(0));
        read_options.skip_rows_after_header = options.skip_rows.unwrap_or(0);
    } else {
        if let Some(skip_lines) = options.skip_lines {
            read_options.skip_lines = skip_lines;
        }
        if let Some(skip_rows) = options.skip_rows {
            read_options.skip_rows = skip_rows;
        }
        if let Some(has_header) = options.has_header {
            read_options.has_header = has_header;
        }
    }
    if let Some(n) = options.infer_schema_length {
        read_options.infer_schema_length = Some(n);
    }
    read_options.ignore_errors = options.ignore_errors;
    read_options.map_parse_options(|opts| {
        opts.with_separator(options.separator_or(b','))
            .with_comment_prefix(
                options
                    .comment_char
                    .as_deref()
                    .map(polars::io::csv::read::CommentPrefix::new_from_str),
            )
            .with_try_parse_dates(options.csv_try_parse_dates())
            .with_null_values(null_values.cloned())
            .with_encoding(CsvEncoding::LossyUtf8)
    })
}

/// The columns a CSV reader will produce, from one row, for building null_values
/// when both global and per-column are set.
pub(crate) fn csv_schema_for_null_values(
    reader: LazyCsvReader,
    options: &OpenOptions,
) -> Result<Arc<Schema>> {
    let mut lf = configure_csv_reader(reader.with_n_rows(Some(1)), options, None).finish()?;
    lf.collect_schema().map_err(color_eyre::eyre::Report::from)
}

/// Build Polars NullValues from options, for the CSV at `path` with the header
/// lines `header` read from it.
fn build_null_values_for_csv(
    options: &OpenOptions,
    path: &Path,
    header: Option<&[String]>,
) -> Result<Option<NullValues>> {
    build_null_values_with(options, header, || {
        csv_schema_for_null_values(csv_reader_of(path)?, options)
    })
}

/// Build Polars NullValues from options. `schema` is the reader's own columns, read
/// only when a spec names a column: the user names it as it is shown (trimmed, or
/// from `--header-rows`), and the reader knows it by what it parsed.
pub(crate) fn build_null_values_with(
    options: &OpenOptions,
    header: Option<&[String]>,
    schema: impl FnOnce() -> Result<Arc<Schema>>,
) -> Result<Option<NullValues>> {
    let specs = match &options.null_values {
        None => return Ok(None),
        Some(s) if s.is_empty() => return Ok(None),
        Some(s) => s.as_slice(),
    };
    let (global, mut per_column) = parse_null_value_specs(specs);
    if per_column.is_empty() {
        return Ok(build_polars_null_values(&global, &per_column, None));
    }
    let schema = match schema() {
        Ok(schema) => schema,
        // Nothing follows the header lines: no value to read as null.
        Err(e)
            if header.is_some()
                && matches!(
                    e.downcast_ref::<PolarsError>(),
                    Some(PolarsError::NoData(_))
                ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let raw: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
    let shown = crate::formats::csv_dialect::shown_names(&raw, header);
    for (column, _) in per_column.iter_mut() {
        if let Some(i) = shown.iter().position(|s| s == column) {
            *column = raw[i].to_string();
        }
    }
    Ok(build_polars_null_values(
        &global,
        &per_column,
        Some(schema.as_ref()),
    ))
}

/// The null values `--null` gives the column shown as `column`.
pub(crate) fn csv_null_values_for(options: &OpenOptions, column: &str) -> Vec<String> {
    let (global, per_column) =
        parse_null_value_specs(options.null_values.as_deref().unwrap_or_default());
    let mut values: Vec<String> = per_column
        .into_iter()
        .filter(|(c, _)| c == column)
        .map(|(_, v)| v)
        .collect();
    values.extend(global);
    values
}

/// The names `--header-rows` gives the columns of the CSV `source` holds, or
/// `None` when it is not in effect.
fn csv_header_names<R: std::io::BufRead>(
    options: &OpenOptions,
    source: impl FnOnce() -> std::io::Result<R>,
) -> Result<Option<Vec<String>>> {
    let Some(rows) = options.header_rows() else {
        return Ok(None);
    };
    Ok(Some(crate::formats::csv_dialect::header_names(
        source()?,
        rows,
        &options.header_join,
        options.separator_or(b','),
        options.comment_char.as_deref(),
    )?))
}

/// A lazy CSV reader of the file at `path`, by its path; or, when the file ends in
/// a run of NULs, of its text before them, mapped and read in place.
pub(crate) fn csv_reader_of(path: &Path) -> Result<LazyCsvReader> {
    let glob = crate::cloud::source::expands_as_glob(path);
    if !glob
        && path.is_file()
        && let Ok(Some(text)) = crate::formats::nul_tail::text_buffer(path)
    {
        return Ok(LazyCsvReader::new_with_sources(
            polars::lazy::dsl::ScanSources::Buffers(Arc::from([text])),
        ));
    }
    Ok(LazyCsvReader::new(PlRefPath::try_from_path(path)?).with_glob(glob))
}

/// [`csv_header_names`] for a file on disk, compressed with `compression`
/// or not.
pub(crate) fn csv_header_names_of(
    options: &OpenOptions,
    path: &Path,
    compression: Option<CompressionFormat>,
) -> Result<Option<Vec<String>>> {
    csv_header_names(options, || text_source(path, compression))
}

/// The text of the file at `path`, through its decompressor when it has one.
pub(crate) fn text_source(
    path: &Path,
    compression: Option<CompressionFormat>,
) -> std::io::Result<Box<dyn std::io::BufRead>> {
    let file = File::open(path)?;
    if compression.is_none()
        && let Some(len) = crate::formats::nul_tail::text_len(&file)?
    {
        return Ok(Box::new(BufReader::new(file.take(len))));
    }
    let file = BufReader::new(file);
    Ok(match compression {
        None => Box::new(file),
        Some(CompressionFormat::Gzip) => {
            Box::new(BufReader::new(flate2::read::GzDecoder::new(file)))
        }
        Some(CompressionFormat::Zstd) => {
            Box::new(BufReader::new(zstd::Decoder::with_buffer(file)?))
        }
        Some(CompressionFormat::Bzip2) => {
            Box::new(BufReader::new(bzip2::read::BzDecoder::new(file)))
        }
        Some(CompressionFormat::Xz) => Box::new(BufReader::new(xz2::read::XzDecoder::new(file))),
    })
}

/// What every CSV read does after Polars parses it: name the columns (trimmed, or from
/// `--header-rows`), skip post-delimiter padding, type text columns, drop the footer.
/// `read` gets the steps Python can repeat.
fn finish_csv_frame(
    lf: LazyFrame,
    options: &OpenOptions,
    header: Option<&[String]>,
    read: &mut Vec<String>,
    typing: &mut Typing,
) -> Result<LazyFrame> {
    let lf = name_csv_columns(lf, header, Some(read))?;
    finish_csv_values(lf, options, read, typing)
}

/// [`crate::formats::csv_dialect::name_columns`], with the renames as Python in `read`.
/// Names from `--header-rows` are not recorded: Copy as Python does not write that
/// read.
fn name_csv_columns(
    mut lf: LazyFrame,
    header: Option<&[String]>,
    read: Option<&mut Vec<String>>,
) -> Result<LazyFrame> {
    if let (None, Some(read)) = (header, read) {
        let raw: Vec<PlSmallStr> = lf.collect_schema()?.iter_names().cloned().collect();
        let shown = crate::formats::csv_dialect::shown_names(&raw, None);
        let renames: Vec<String> = raw
            .iter()
            .zip(&shown)
            .filter(|(raw, shown)| raw.as_str() != shown.as_str())
            .map(|(raw, shown)| format!("{}: {}", py_str(raw), py_str(shown)))
            .collect();
        if !renames.is_empty() {
            read.push(format!(".rename({{{}}})", renames.join(", ")));
        }
    }
    Ok(crate::formats::csv_dialect::name_columns(lf, header)?)
}

/// [`finish_csv_frame`] after the names, for frames already named: several
/// files are named one at a time and stacked first.
fn finish_csv_values(
    mut lf: LazyFrame,
    options: &OpenOptions,
    read: &mut Vec<String>,
    typing: &mut Typing,
) -> Result<LazyFrame> {
    if options.skip_initial_space {
        lf = crate::formats::csv_dialect::skip_initial_space(lf, |column| {
            csv_null_values_for(options, column)
        })?;
    }
    // Read without a header (`H`), the columns have no names to derive from, or to
    // type by. Derived columns read the file's text, before any column is typed.
    let spec = options
        .delimited
        .as_ref()
        .filter(|_| options.has_header != Some(false))
        .map(|read| read.delimited());
    if let Some(spec) = spec {
        lf = spec.derive(lf)?;
        lf = declare_types(lf, &spec.types, typing)?;
    }
    let typed: Vec<String> = typing.typed.iter().map(|t| t.column.clone()).collect();
    lf = apply_parse_strings_to_csv_lazyframe(lf, options, read, &typed, typing)?;
    apply_skip_tail_rows_csv(lf, options)
}

/// `lf` with each column `types` names read as its type, lazily; `typing` records
/// them and the frame before, for the count of the values that did not fit, and a
/// note names the ones the frame does not have.
fn declare_types(
    mut lf: LazyFrame,
    types: &[(String, crate::formats::column_types::ColumnType)],
    typing: &mut Typing,
) -> Result<LazyFrame> {
    if types.is_empty() {
        return Ok(lf);
    }
    let schema = lf.collect_schema()?;
    let mut exprs = Vec::with_capacity(types.len());
    let mut missing = Vec::new();
    for (name, ty) in types {
        match schema.get(name.as_str()) {
            Some(from) => {
                exprs.push(ty.expr(name, from).alias(name.as_str()));
                typing.typed.push(crate::formats::column_types::Typed {
                    column: name.clone(),
                    ty: ty.clone(),
                    from: from.clone(),
                });
            }
            None => missing.push(name.as_str()),
        }
    }
    if !missing.is_empty() {
        typing.notes.push(crate::notes::Note {
            summary: format!(
                "typed in the spec, not in the file: {}",
                crate::notes::some_names(&missing)
            ),
            scope: "the spec's [columns]".to_string(),
            read_as_text: None,
            passed_over: None,
        });
    }
    if exprs.is_empty() {
        return Ok(lf);
    }
    typing.source = Some(lf.clone());
    Ok(lf.with_columns(exprs))
}

/// `reader` (the scan of `path`) reading some columns as text: those a spec types (via
/// [`declare_types`], misfits null rather than failing), and, while `read.infer_types`
/// types text, those with a leading-zero number in their first rows (`02134`, which an
/// integer read would lose). `window` is those rows if already read.
pub(crate) fn scan_some_as_text(
    reader: LazyCsvReader,
    options: &OpenOptions,
    header: Option<&[String]>,
    path: &Path,
    window: Option<&[Vec<String>]>,
    text: &mut Vec<String>,
) -> Result<LazyCsvReader> {
    if options.has_header == Some(false) {
        return Ok(reader);
    }
    let names: Vec<String> = options
        .delimited
        .as_ref()
        .map(|read| {
            read.delimited()
                .types
                .iter()
                .map(|(name, _)| name.clone())
                .collect()
        })
        .unwrap_or_default();
    let zeros: Vec<usize> = match &options.parse_strings {
        None => Vec::new(),
        Some(_) => {
            let read;
            let window = match window {
                Some(window) => window,
                None => {
                    read =
                        crate::formats::spec_union::head_window(path, options).unwrap_or_default();
                    &read
                }
            };
            let width = window.iter().map(Vec::len).max().unwrap_or(0);
            (0..width)
                .filter(|&at| {
                    window.iter().any(|row| {
                        row.get(at)
                            .is_some_and(|v| crate::formats::column_types::has_leading_zero(v))
                    })
                })
                .collect()
        }
    };
    if names.is_empty() && zeros.is_empty() {
        return Ok(reader);
    }
    let header = header.map(<[String]>::to_vec);
    let target = options.parse_strings.clone();
    let read_as_text = Arc::new(std::sync::Mutex::new(Vec::new()));
    let said = read_as_text.clone();
    let reader = reader.with_schema_modify(move |mut schema| {
        let raw: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
        let shown = crate::formats::csv_dialect::shown_names(&raw, header.as_deref());
        for (at, (raw, shown)) in raw.iter().zip(&shown).enumerate() {
            let inferred = match &target {
                Some(ParseStringsTarget::All) => true,
                Some(ParseStringsTarget::Columns(columns)) => columns.contains(shown),
                None => false,
            };
            if names.contains(shown) || (inferred && zeros.contains(&at)) {
                schema.with_column(raw.clone(), DataType::String);
                if let Ok(mut said) = said.lock() {
                    said.push(raw.to_string());
                }
            }
        }
        Ok(schema)
    })?;
    if let Ok(mut read) = read_as_text.lock() {
        text.append(&mut read);
    }
    Ok(reader)
}

/// If options.skip_tail_rows is set, run a count query and slice the LazyFrame to drop that many rows from the end. Used for CSV with trailing garbage/footer.
pub(crate) fn apply_skip_tail_rows_csv(lf: LazyFrame, options: &OpenOptions) -> Result<LazyFrame> {
    let n = match options.skip_tail_rows {
        None | Some(0) => return Ok(lf),
        Some(n) => n,
    };
    let count_df = collect_lazy(lf.clone().select([len()]), options.polars_streaming)
        .map_err(color_eyre::eyre::Report::from)?;
    let total: u32 = match count_df.get(0) {
        Some(col) => match col.first() {
            Some(AnyValue::UInt32(v)) => *v,
            _ => return Ok(lf),
        },
        _ => {
            return Ok(lf);
        }
    };
    let keep = total.saturating_sub(n as u32);
    Ok(lf.slice(0, keep))
}

/// The first date format `sample` reads in. `None` when none does, so Polars is
/// never handed `format: None`, which can fail.
fn infer_date_format_from_sample(sample: &str) -> Option<&'static str> {
    crate::formats::column_types::formats_reading(&DataType::Date, sample)
        .first()
        .copied()
}

fn infer_datetime_format_from_sample(sample: &str) -> Option<&'static str> {
    crate::formats::column_types::formats_reading(
        &DataType::Datetime(TimeUnit::Microseconds, None),
        sample,
    )
    .first()
    .copied()
}

/// Parse a string array into nanosecond durations in Polars' format (`1d`, `2h30m`,
/// `-1w2d`); invalid or null inputs become null.
fn string_chunked_to_duration_ns(str_ca: &StringChunked) -> DurationChunked {
    let name = str_ca.name().clone();
    let vals: Vec<Option<i64>> = str_ca
        .iter()
        .map(|opt_s| {
            opt_s.and_then(|s| {
                polars::time::Duration::try_parse(s)
                    .ok()
                    .map(|d| d.duration_ns())
            })
        })
        .collect();
    let int_ca = Int64Chunked::from_iter_options(name, vals.into_iter());
    int_ca.into_duration(TimeUnit::Nanoseconds)
}

fn infer_time_format_from_sample(sample: &str) -> Option<&'static str> {
    crate::formats::column_types::formats_reading(&DataType::Time, sample)
        .first()
        .copied()
}

/// With `--infer-types`, trim and type CSV string columns: sample up to
/// `options.parse_strings_sample_rows` rows, then overlay lazy trim-and-cast exprs.
fn apply_parse_strings_to_csv_lazyframe(
    lf: LazyFrame,
    options: &OpenOptions,
    read: &mut Vec<String>,
    except: &[String],
    typing: &mut Typing,
) -> Result<LazyFrame> {
    let Some(target) = &options.parse_strings else {
        return Ok(lf);
    };
    let before = lf.clone();
    let mut typed = Vec::new();
    let lf = type_string_columns(
        lf,
        target,
        options.parse_strings_sample_rows,
        StringTypes {
            dates: options.parse_dates,
            numbers: true,
        },
        read,
        except,
        &mut typed,
    )?;
    // The columns it typed are counted as the spec's are, over the frame before
    // either: the spec's typing leaves these columns as they were read.
    if !typed.is_empty() {
        typing.source.get_or_insert(before);
        typing.typed.extend(typed);
    }
    Ok(lf)
}

/// A string column read as a microsecond Datetime with `format`. Exact, so a naive
/// format cannot match the front of a value that carries an offset and drop it; not
/// strict, so a value past the sample that does not parse is null.
fn datetime_from_str(expr: Expr, format: &str) -> Expr {
    expr.str().to_datetime(
        Some(TimeUnit::Microseconds),
        None,
        StrptimeOptions {
            format: Some(PlSmallStr::from(format)),
            strict: false,
            exact: true,
            cache: true,
        },
        lit(PlSmallStr::from_static("raise")),
    )
}

/// The rows string inference reads: the first `sample_rows` of the `targets` only,
/// trimmed so inference sees "1" not " 1 ", with blanks as null so "all null" and
/// the accept test see normalized values. Only the targets are selected, so the
/// other columns are neither decoded nor held; the frame the table shows keeps them.
pub(super) fn string_inference_sample(
    lf: LazyFrame,
    targets: &[String],
    sample_rows: usize,
) -> PolarsResult<DataFrame> {
    let whitespace_pat = lit(PlSmallStr::from_static(" \t\n\r"));
    let trimmed: Vec<Expr> = targets
        .iter()
        .map(|c| {
            let name = PlSmallStr::from(c.as_str());
            col(name.clone())
                .str()
                .strip_chars(whitespace_pat.clone())
                .alias(name)
        })
        .collect();
    let blank_to_null: Vec<Expr> = targets
        .iter()
        .map(|c| {
            let name = PlSmallStr::from(c.as_str());
            when(col(name.clone()).eq(lit(PlSmallStr::from_static(""))))
                .then(Null {}.lit())
                .otherwise(col(name.clone()))
                .alias(name)
        })
        .collect();
    lf.limit(sample_rows as u32)
        .select(trimmed)
        .with_columns(blank_to_null)
        .collect()
}

/// Type string columns from the first `sample_rows` rows: trim, then keep the first
/// of Date, Datetime, Time, Duration, Int64 and Float64 (as `types` allows) that
/// parses every sampled value, as lazy expressions over `lf`.
pub(crate) fn type_string_columns(
    lf: LazyFrame,
    target: &ParseStringsTarget,
    sample_rows: usize,
    types: StringTypes,
    read: &mut Vec<String>,
    except: &[String],
    typed: &mut Vec<crate::formats::column_types::Typed>,
) -> Result<LazyFrame> {
    // The scan already inferred the schema; the sample below is the one read.
    let schema = lf.clone().collect_schema()?;
    let string_cols: Vec<String> = schema
        .iter()
        // A column given a type keeps it.
        .filter(|(name, _)| !except.iter().any(|e| e == name.as_str()))
        .filter(|(_name, dtype)| **dtype == DataType::String)
        .map(|(name, _)| name.to_string())
        .collect();
    let target_cols: Vec<String> = match target {
        ParseStringsTarget::All => string_cols,
        ParseStringsTarget::Columns(c) => c
            .iter()
            .filter(|name| string_cols.contains(name))
            .cloned()
            .collect(),
    };
    if target_cols.is_empty() {
        return Ok(lf);
    }
    use polars::datatypes::TimeUnit;
    let whitespace_pat = lit(PlSmallStr::from_static(" \t\n\r"));
    let sample_df = string_inference_sample(lf.clone(), &target_cols, sample_rows)?;
    log::debug!(
        target: "datui",
        "string inference sample: {} rows x {} columns for {} targets, {} bytes",
        sample_df.height(),
        sample_df.width(),
        target_cols.len(),
        sample_df.estimated_size()
    );
    let mut exprs = Vec::with_capacity(target_cols.len());
    // The same typing as Python, for Copy as Python.
    let mut python = Vec::with_capacity(target_cols.len());
    for col_name in &target_cols {
        let name = PlSmallStr::from(col_name.as_str());
        let s = sample_df.column(col_name.as_str())?;
        let null_before = s.null_count();
        let len = s.len();
        // Accept type if we didn't introduce new nulls (null_after <= null_before).
        let accept_type = |null_after: usize| null_after <= null_before;
        // Inference order: Date → Datetime → Time → Duration → Int64 → Float64 → String.
        enum InferredType {
            Date,
            Datetime,
            Time,
            Duration,
            Int64,
            Float64,
            String,
        }
        let (inferred, date_fmt, datetime_fmt, time_fmt) = if null_before == len {
            // Column is all null (including blanks treated as null): leave as string.
            (InferredType::String, None, None, None)
        } else {
            match s.str() {
                Err(_) => (InferredType::String, None, None, None),
                Ok(str_ca) => {
                    let first_val: Option<&str> = str_ca
                        .iter()
                        .find_map(|o: Option<&str>| o.filter(|s: &&str| !s.is_empty()));
                    // `02134`, `007`: a ZIP code or an ID, not a number.
                    let zeros = str_ca
                        .iter()
                        .flatten()
                        .any(crate::formats::column_types::has_leading_zero);
                    let (mut t, mut date_fmt, mut datetime_fmt, mut time_fmt) =
                        match str_ca.as_date(None, true) {
                            Ok(as_date) if types.dates && accept_type(as_date.null_count()) => {
                                let fmt = first_val.and_then(infer_date_format_from_sample);
                                if fmt.is_some() {
                                    (InferredType::Date, fmt.map(String::from), None, None)
                                } else {
                                    (InferredType::String, None, None, None)
                                }
                            }
                            _ => (InferredType::String, None, None, None),
                        };
                    if matches!(t, InferredType::String)
                        && types.dates
                        && let Some(fmt) = first_val.and_then(infer_datetime_format_from_sample)
                    {
                        // Judged by the expression the table will run, so a column
                        // whose values disagree (an offset on some, none on others)
                        // fails here and stays text.
                        let parsed = sample_df
                            .clone()
                            .lazy()
                            .select([datetime_from_str(col(name.clone()), fmt)])
                            .collect()?;
                        if accept_type(parsed.column(col_name.as_str())?.null_count()) {
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                (InferredType::Datetime, None, Some(fmt.to_string()), None);
                        }
                    }
                    if matches!(t, InferredType::String) {
                        (t, date_fmt, datetime_fmt, time_fmt) = match str_ca.as_time(None, true) {
                            Ok(as_time) if accept_type(as_time.null_count()) => {
                                let fmt = first_val.and_then(infer_time_format_from_sample);
                                if fmt.is_some() {
                                    (InferredType::Time, None, None, fmt.map(String::from))
                                } else {
                                    (InferredType::String, None, None, None)
                                }
                            }
                            _ => (InferredType::String, None, None, None),
                        };
                    }
                    if matches!(t, InferredType::String) && types.numbers {
                        let duration_ca = string_chunked_to_duration_ns(str_ca);
                        (t, date_fmt, datetime_fmt, time_fmt) =
                            if accept_type(duration_ca.null_count()) {
                                (InferredType::Duration, None, None, None)
                            } else {
                                (InferredType::String, None, None, None)
                            };
                    }
                    if matches!(t, InferredType::String) && types.numbers && !zeros {
                        (t, date_fmt, datetime_fmt, time_fmt) =
                            match s.strict_cast(&DataType::Int64) {
                                Ok(as_int) if accept_type(as_int.null_count()) => {
                                    (InferredType::Int64, None, None, None)
                                }
                                _ => (InferredType::String, None, None, None),
                            };
                    }
                    if matches!(t, InferredType::String) && types.numbers && !zeros {
                        (t, date_fmt, datetime_fmt, time_fmt) =
                            match s.strict_cast(&DataType::Float64) {
                                Ok(as_float) if accept_type(as_float.null_count()) => {
                                    (InferredType::Float64, None, None, None)
                                }
                                _ => (InferredType::String, None, None, None),
                            };
                    }
                    (t, date_fmt, datetime_fmt, time_fmt)
                }
            }
        };
        let base = col(PlSmallStr::from(col_name.as_str()))
            .str()
            .strip_chars(whitespace_pat.clone());
        let trimmed = format!(
            "pl.col({}).str.strip_chars(\" \\t\\n\\r\")",
            py_str(col_name)
        );
        let blank_null = format!("{trimmed}.replace(\"\", None)");
        let format_arg = |f: &Option<String>| match f {
            Some(f) => format!("{}, ", py_str(f)),
            None => String::new(),
        };
        python.push(match &inferred {
            InferredType::Date => format!(
                "{blank_null}.str.to_date({}strict=False)",
                format_arg(&date_fmt)
            ),
            InferredType::Datetime => format!(
                "{blank_null}.str.to_datetime({}time_unit=\"us\", strict=False)",
                format_arg(&datetime_fmt)
            ),
            InferredType::Time => format!(
                "{blank_null}.str.to_time({}strict=False)",
                format_arg(&time_fmt)
            ),
            InferredType::Duration => crate::python_script::py_comment(&format!(
                "{col_name}: datui reads these as durations (\"1d2h\"); Polars has no parser for them"
            )),
            InferredType::Int64 => {
                format!("{blank_null}.cast(pl.Int64, strict=False)")
            }
            InferredType::Float64 => {
                format!("{blank_null}.cast(pl.Float64, strict=False)")
            }
            InferredType::String if types.numbers => trimmed.clone(),
            InferredType::String => String::new(),
        });
        // The one way a column is given a type: the spec's and the table's too.
        let ty = |dtype: DataType, format: Option<String>| {
            crate::formats::column_types::ColumnType { dtype, format }
        };
        let ty = match inferred {
            InferredType::Date => ty(DataType::Date, date_fmt),
            InferredType::Datetime => ty(
                DataType::Datetime(TimeUnit::Microseconds, None),
                datetime_fmt,
            ),
            InferredType::Time => ty(DataType::Time, time_fmt),
            InferredType::Duration => ty(DataType::Duration(TimeUnit::Nanoseconds), None),
            InferredType::Int64 => ty(DataType::Int64, None),
            InferredType::Float64 => ty(DataType::Float64, None),
            // Trimmed where every column is text; left as read where the
            // writer chose a string.
            InferredType::String if types.numbers => {
                exprs.push(base.alias(name));
                continue;
            }
            InferredType::String => continue,
        };
        let expr = ty.expr(col_name, &DataType::String).alias(name);
        typed.push(crate::formats::column_types::Typed {
            column: col_name.clone(),
            ty,
            from: DataType::String,
        });
        exprs.push(expr);
    }
    let python: Vec<String> = python.into_iter().filter(|p| !p.is_empty()).collect();
    if !python.is_empty() {
        read.push(".with_columns(".to_string());
        read.extend(python.into_iter().map(|p| {
            if p.starts_with('#') {
                format!("    {p}")
            } else {
                format!("    {p},")
            }
        }));
        read.push(")".to_string());
    }
    Ok(lf.with_columns(exprs))
}

/// `path` decompressed to a temporary copy in `temp_dir`, written through `writer`:
/// a stopped open stops the copy, and quitting removes it.
pub(crate) fn decompress_to_copy(
    path: &Path,
    compression: CompressionFormat,
    temp_dir: &Path,
    writer: &Writer,
) -> Result<crate::cloud::download::TempDownload> {
    let Decompressed { file, _claim } =
        decompress_compressed_csv_to_temp(path, compression, temp_dir, writer)?;
    Ok(crate::cloud::download::TempDownload::held(
        file,
        Some(_claim),
    ))
}

/// A delimited text file, split on `delimiter` (its format's separator) unless
/// `--delimiter` says otherwise. CSV, TSV and PSV are one reader, so every CSV
/// option means the same thing for all three. A compressed file is decompressed
/// through `writer`, so an open's stop and quitting reach the copy.
pub(crate) fn read_delimited(
    path: &Path,
    delimiter: u8,
    options: &OpenOptions,
    writer: &Writer,
) -> Result<Read> {
    // Settled once here: every reader below asks `options.separator_or(b',')`.
    let options = &OpenOptions {
        delimiter: Some(options.separator_or(delimiter)),
        ..options.clone()
    };

    let compression = options
        .compression
        .or_else(|| CompressionFormat::from_extension(path));

    if let Some(compression) = compression {
        if options.decompress_in_memory {
            // Eager read: decompress into memory, then CSV read
            let (df, header) = match compression {
                CompressionFormat::Gzip | CompressionFormat::Zstd => {
                    let header = csv_header_names_of(options, path, Some(compression))?;
                    let nv = build_null_values_for_csv(options, path, header.as_deref())?;
                    let read_options = eager_csv_read_options(options, nv.as_ref());
                    let df = crate::formats::csv_dialect::read_after_header(
                        read_options
                            .try_into_reader_with_file_path(Some(path.into()))?
                            .finish(),
                        header.as_deref(),
                    )?;
                    (df, header)
                }
                CompressionFormat::Bzip2 | CompressionFormat::Xz => {
                    let file = BufReader::new(File::open(path)?);
                    let mut decompressed = Vec::new();
                    if compression == CompressionFormat::Bzip2 {
                        bzip2::read::BzDecoder::new(file).read_to_end(&mut decompressed)?;
                    } else {
                        xz2::read::XzDecoder::new(file).read_to_end(&mut decompressed)?;
                    }
                    crate::formats::nul_tail::trim(&mut decompressed);
                    let header = csv_header_names(options, || {
                        Ok(std::io::Cursor::new(decompressed.as_slice()))
                    })?;
                    // Column names for per-column null values come from the bytes: the file on
                    // disk is still compressed.
                    let nv = build_null_values_with(options, header.as_deref(), || {
                        let one_row = eager_csv_read_options(options, None).with_n_rows(Some(1));
                        let df = CsvReader::new(std::io::Cursor::new(decompressed.as_slice()))
                            .with_options(one_row)
                            .finish()?;
                        Ok(df.schema().clone())
                    })?;
                    let read_options = eager_csv_read_options(options, nv.as_ref());
                    let df = crate::formats::csv_dialect::read_after_header(
                        CsvReader::new(std::io::Cursor::new(decompressed))
                            .with_options(read_options)
                            .finish(),
                        header.as_deref(),
                    )?;
                    (df, header)
                }
            };
            let mut read = Vec::new();
            let mut typing = Typing::default();
            let lf = finish_csv_frame(
                df.lazy(),
                options,
                header.as_deref(),
                &mut read,
                &mut typing,
            )?;
            Ok(Read {
                python: read,
                ..lf.into()
            }
            .typed(typing))
        } else {
            // Decompress to temp file, then lazy scan
            let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
            let temp = decompress_compressed_csv_to_temp(path, compression, &temp_dir, writer)?;
            let read = scan_csv_file(temp.path(), options)?;
            Ok(Read {
                temp: Some(Arc::new(temp)),
                ..read
            })
        }
    } else {
        // For uncompressed files, use lazy scanning (more efficient)
        scan_csv_file(path, options)
    }
}

/// A compressed file read as lines: decompressed once to a file in `--temp-dir`, or
/// into memory with `[read] decompress_in_memory`, then indexed.
pub(crate) fn from_lines_decompressed(
    path: &Path,
    options: &OpenOptions,
    writer: &Writer,
) -> Result<(Read, crate::formats::members::Opened)> {
    let compression = options
        .compression
        .or_else(|| CompressionFormat::from_extension(path))
        .ok_or_else(|| color_eyre::eyre::eyre!("{} is not compressed", path.display()))?;
    let (lines, temp) = if options.decompress_in_memory {
        let mut bytes = Vec::new();
        let read = std::sync::atomic::AtomicU64::new(0);
        crate::formats::text_formats::open_reader(path, options, &read)?.read_to_end(&mut bytes)?;
        let name = path
            .file_stem()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        let bytes = Arc::new(crate::formats::fixed_records::Bytes::Owned(bytes));
        (
            crate::formats::lines::Lines::from_bytes(vec![(name, bytes)]),
            None,
        )
    } else {
        let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
        let temp = decompress_compressed_csv_to_temp(path, compression, &temp_dir, writer)?;
        let lines = crate::formats::lines::Lines::open(&[temp.path().to_path_buf()], false)?;
        (lines, Some(Arc::new(temp)))
    };
    let lines = Arc::new(lines);
    let opened = crate::formats::lines::opened(&lines, options);
    let read = Read {
        temp,
        ..lines.lazy().into()
    };
    Ok((read, opened))
}

/// One uncompressed delimited file, scanned lazily. The frame is finished before
/// the state is made from it, so the column order is of the names shown.
fn scan_csv_file(path: &Path, options: &OpenOptions) -> Result<Read> {
    let header = csv_header_names_of(options, path, None)?;
    let nv = build_null_values_for_csv(options, path, header.as_deref())?;
    let reader = csv_reader_of(path)?;
    let reader = configure_csv_reader(reader, options, nv.as_ref());
    let mut typing = Typing::default();
    let lf = scan_some_as_text(
        reader,
        options,
        header.as_deref(),
        path,
        None,
        &mut typing.text,
    )?
    .finish()?;
    let mut read = Vec::new();
    let lf = finish_csv_frame(lf, options, header.as_deref(), &mut read, &mut typing)?;
    Ok(Read {
        python: read,
        ..lf.into()
    }
    .typed(typing))
}

/// Load multiple CSV files (uncompressed) and concatenate into one LazyFrame.
pub(crate) fn from_csv_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Read> {
    if paths.is_empty() {
        return Err(color_eyre::eyre::eyre!("No paths provided"));
    }
    if paths.len() == 1 {
        return read_delimited(paths[0].as_ref(), b',', options, &Writer::default());
    }
    // Each file is named from its own header, so files whose names are padded
    // differently, or whose header lines say the same thing, stack by name.
    let mut lazy_frames = Vec::with_capacity(paths.len());
    // Python reads the files as one scan: the first file's renames stand for all.
    let mut read = Vec::new();
    // Files with nothing in them: no header, so no columns to stack.
    let mut no_header: Vec<&Path> = Vec::new();
    // Read through a spec, each file's header pass reads its units and the lines its
    // types are inferred from too, for lining the files up by name.
    let spec = options.delimited.as_ref().map(|read| read.delimited());
    let mut heads = Vec::new();
    let mut read_text = Vec::new();
    for p in paths {
        let p = p.as_ref();
        let in_file = |e: color_eyre::Report| crate::error_display::in_file(p, e);
        let head_read = match spec {
            Some(spec) => crate::formats::spec_union::read_head(p, options, spec).map(Some),
            None => Ok(None),
        };
        let head = match head_read {
            Err(e) if crate::formats::csv_dialect::is_blank_file(&e) => {
                no_header.push(p);
                continue;
            }
            head => head.map_err(in_file)?,
        };
        let header = match &head {
            Some(head) => head.names.clone(),
            None => match csv_header_names_of(options, p, None) {
                Err(e) if crate::formats::csv_dialect::is_blank_file(&e) => {
                    no_header.push(p);
                    continue;
                }
                header => header.map_err(in_file)?,
            },
        };
        let nv = build_null_values_for_csv(options, p, header.as_deref()).map_err(in_file)?;
        let reader = csv_reader_of(p).map_err(in_file)?;
        let reader = configure_csv_reader(reader, options, nv.as_ref());
        let window = head.as_ref().map(|head| head.window.as_slice());
        // Python reads the files as one scan: the first file's columns stand for all.
        let mut text = Vec::new();
        let lf = scan_some_as_text(reader, options, header.as_deref(), p, window, &mut text)
            .map_err(in_file)?
            .finish()
            .map_err(|e| in_file(e.into()))?;
        if lazy_frames.is_empty() {
            read_text = text;
        }
        let record = lazy_frames.is_empty().then_some(&mut read);
        // Polars reads the header line itself: a file with none has no columns, or
        // one with a blank name.
        if header.is_none() {
            let raw = lf.clone().collect_schema();
            let headless = match &raw {
                Err(PolarsError::NoData(_)) => true,
                Ok(schema) => {
                    schema.is_empty()
                        || (schema.len() == 1 && schema.iter_names().all(|n| n.trim().is_empty()))
                }
                Err(_) => false,
            };
            if headless && is_blank_text(p) {
                no_header.push(p);
                continue;
            }
        }
        let named = name_csv_columns(lf, header.as_deref(), record).map_err(in_file)?;
        lazy_frames.push(named);
        heads.extend(head);
    }
    if lazy_frames.is_empty() {
        return Err(color_eyre::eyre::eyre!(
            "none of these {} files has a header: each is empty, or blank",
            paths.len()
        ));
    }
    let mut notes: Vec<crate::notes::Note> =
        crate::notes::no_header(&no_header).into_iter().collect();
    let mut units = None;
    if spec.is_some() && lazy_frames.len() > 1 {
        let lined = crate::formats::spec_union::line_up(lazy_frames, &heads, options)?;
        lazy_frames = lined.frames;
        notes.extend(lined.notes);
        units = Some(lined.units);
    }
    let mut typing = Typing {
        text: read_text,
        ..Typing::default()
    };
    let lf = finish_csv_values(
        polars::prelude::concat(lazy_frames.as_slice(), super::polars::union_of_files())?,
        options,
        &mut read,
        &mut typing,
    )?;
    Ok(Read {
        lf,
        python: read,
        notes,
        units,
        ..Read::default()
    }
    .typed(typing))
}

/// Whether the text of the file at `path`, to its NUL padding, is no more than
/// white space. Read up to a bound: past it, the file holds something.
fn is_blank_text(path: &Path) -> bool {
    const MOST: u64 = 64 << 10;
    let mut text = Vec::new();
    text_source(path, None)
        .and_then(|source| source.take(MOST + 1).read_to_end(&mut text))
        .is_ok_and(|n| n as u64 <= MOST && text.iter().all(u8::is_ascii_whitespace))
}

/// What string-column inference may turn a column into, besides Time.
#[derive(Clone, Copy)]
pub(crate) struct StringTypes {
    /// Date and Datetime.
    pub(crate) dates: bool,
    /// Duration, Int64 and Float64, and trimming the columns that stay text.
    pub(crate) numbers: bool,
}

/// A compressed CSV's decompressed copy, then the open's claim on it: dropped in that
/// order, so the claim goes only once the file has. See [`crate::loading::unfinished`].
pub(crate) struct Decompressed {
    file: NamedTempFile,
    _claim: Claim,
}

impl Decompressed {
    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }
}

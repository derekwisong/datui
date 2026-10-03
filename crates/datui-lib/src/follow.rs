//! Following a file that is still being written, as `tail -f` does: `--follow` and
//! `t` at the table.
//!
//! The frame scans the file as any delimited or NDJSON file is scanned, with a slice
//! right above the scan that bounds it to the rows whose records are complete
//! ([`bound`]). A watcher thread ([`Follow`]) checks the file's size every interval,
//! reads only the bytes that arrived, counts the records they complete ([`Tail`]), and
//! sends what it found as [`AppEvent::Followed`]. The app then moves the slice in every
//! frame the view holds, so the query, filters and sort run over the new rows with no
//! frame rebuilt, and reads the window on screen. A partial last line is never in the
//! bound: it waits for its newline.
//!
//! Standard input followed (`datui -f -`) is spooled to a file by a [`Spool`] that goes
//! on copying after the first rows show; the file is followed like any other.
//!
//! An Arrow IPC stream is followed the same way, its record batches counted in place
//! of records and read by a scan of its own ([`stream`]).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use polars::prelude::*;

use crate::download::TempDownload;
use crate::unfinished::Writer;
use crate::{AppEvent, CompressionFormat, FileFormat, OpenOptions};

#[cfg(target_os = "linux")]
mod notify;
pub(crate) mod stream;
#[doc(hidden)]
pub use stream::stream_messages;

/// How often the watcher checks the file, or where it hears of changes (Linux) the least
/// time between two reads, unless `[file_loading] follow_interval_ms` says otherwise. A
/// burst of appends inside one interval is one refresh.
pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);

/// Bytes read from the file per step while counting records.
const CHUNK: usize = 1 << 20;

/// A record longer than this is kept only in part: enough to classify it.
const LONGEST_RECORD: usize = 16 << 20;

/// A row's start is marked once this many rows, or this many bytes, have passed since
/// the last mark: a window is read from the mark before it, so it costs at most this
/// much beyond its own rows at any file size.
pub(crate) const MARK_ROWS: u64 = 8192;
const MARK_BYTES: u64 = 1 << 20;

/// The most bytes a read from a mark takes in. A view that needs more (a filtered
/// window far behind the last count, a count after a long pause) is read by Polars
/// from the start of the file as before.
const MOST_FROM_A_MARK: u64 = 64 << 20;

/// Why `format` cannot be followed, or `None` when it can. Only text read line by
/// line, and an Arrow IPC stream (see [`followed_stream`]), can: a file whose footer is
/// written last (Parquet, an Arrow IPC file, Excel) cannot be read before it is
/// finished, and a compressed one cannot be read from the middle.
pub fn refusal(format: Option<FileFormat>, options: &OpenOptions) -> Option<String> {
    let format = format.unwrap_or(FileFormat::TEXT);
    if format == FileFormat::Arrow {
        return Some(
            "An Arrow IPC file is read once it is finished; an Arrow IPC stream can be followed."
                .to_string(),
        );
    }
    if !format.follows() {
        let followed: Vec<&str> = FileFormat::ALL
            .into_iter()
            .filter(|f| f.follows())
            .map(FileFormat::title)
            .collect();
        let followed = match followed.split_last() {
            Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
            _ => followed.join(""),
        };
        return Some(format!(
            "Only {followed} and Arrow IPC streams can be followed as they grow; {} is read \
             once it is finished.",
            format.title()
        ));
    }
    if options.compression.is_some() {
        return Some("A compressed file cannot be followed as it grows.".to_string());
    }
    if options.header_rows().is_some() || options.skip_tail_rows.is_some() {
        return Some(
            "A file read with --header-rows or --skip-tail-rows cannot be followed.".to_string(),
        );
    }
    if options.spec_name.is_some() || options.spec_file.is_some() || options.delimited.is_some() {
        return Some("A file read through a format spec cannot be followed.".to_string());
    }
    None
}

/// Whether `path`'s format, as `options` say or its name does, is one `--follow` reads.
pub fn followable_path(path: &Path, options: &OpenOptions) -> bool {
    let format = options.format.or_else(|| {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(FileFormat::from_extension)
    });
    let compression = options
        .compression
        .or_else(|| CompressionFormat::from_extension(path));
    compression.is_none()
        && (refusal(format, options).is_none() || followed_stream(path, format, options))
}

/// Whether `path`, read as `format`, is an Arrow IPC stream `--follow` reads as it grows,
/// by its contents.
pub(crate) fn followed_stream(
    path: &Path,
    format: Option<FileFormat>,
    options: &OpenOptions,
) -> bool {
    format == Some(FileFormat::Arrow)
        && options.compression.is_none()
        && options.spec_name.is_none()
        && options.spec_file.is_none()
        && crate::ipc_stream::is_stream_file(path)
}

/// Why `paths` cannot be followed as `options` ask, before anything is read: only one
/// local file, or standard input, can be.
pub fn refuse_paths(paths: &[PathBuf], options: &OpenOptions) -> Option<String> {
    let [path] = paths else {
        return Some("Only one file can be followed at a time.".to_string());
    };
    if crate::stdin::is_stdin(path) {
        // What it holds is known once its first bytes are in.
        return None;
    }
    if !matches!(
        crate::source::input_source(path),
        crate::source::InputSource::Local(_)
    ) {
        return Some("Only a local file or standard input can be followed.".to_string());
    }
    if path.is_dir() {
        return Some("A directory cannot be followed: name a file in it.".to_string());
    }
    let format = options.format.or_else(|| {
        path.extension()
            .and_then(|e| e.to_str())
            .and_then(FileFormat::from_extension)
    });
    let options = OpenOptions {
        compression: options
            .compression
            .or_else(|| CompressionFormat::from_extension(path)),
        ..options.clone()
    };
    if followed_stream(path, format, &options) {
        return None;
    }
    refusal(format, &options)
}

/// The format a followed `path` is read as.
pub(crate) fn format_of(path: &Path, found: Option<FileFormat>) -> FileFormat {
    found
        .or_else(|| {
            path.extension()
                .and_then(|e| e.to_str())
                .and_then(FileFormat::from_extension)
        })
        .unwrap_or(FileFormat::TEXT)
}

/// An NDJSON file followed, scanned lazily rather than read whole as an unfollowed one
/// is: the frame reads more of it as it grows.
pub(crate) fn scan_lines(
    path: &Path,
    options: &OpenOptions,
    read_python: &mut Vec<String>,
) -> color_eyre::Result<LazyFrame> {
    let mut reader =
        LazyJsonLineReader::new(PlRefPath::try_from_path(path)?).with_ignore_errors(true);
    if let Some(n) = options
        .infer_schema_length
        .and_then(std::num::NonZeroUsize::new)
    {
        reader = reader.with_infer_schema_length(Some(n));
    }
    let lf = reader.finish()?;
    crate::widgets::datatable::DataTableState::apply_parse_dates_to_json_lazyframe(
        lf,
        options,
        read_python,
    )
}

/// `lf`, the scan of `path` read as `format`, bounded to the rows of the file's complete
/// records, and the count its watcher reads on from.
pub(crate) fn bound_to_complete(
    mut lf: LazyFrame,
    path: &Path,
    format: FileFormat,
    options: &OpenOptions,
) -> color_eyre::Result<(LazyFrame, Tail)> {
    let schema = lf.collect_schema()?;
    let mut tail = Tail::new(format, options, &schema);
    tail.path = path.to_path_buf();
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    tail.read_on(&mut file, len, false)?;
    bound(&mut lf, path, tail.rows());
    Ok((lf, tail))
}

/// What a field of a row has to read as, for a row to fit the schema inferred from the
/// first rows.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Fits {
    Anything,
    Integer,
    Number,
    Boolean,
    Text,
}

impl Fits {
    fn of(dtype: &DataType) -> Fits {
        if dtype.is_integer() {
            Fits::Integer
        } else if dtype.is_float() {
            Fits::Number
        } else if matches!(dtype, DataType::Boolean) {
            Fits::Boolean
        } else if matches!(dtype, DataType::String) {
            Fits::Text
        } else {
            Fits::Anything
        }
    }

    /// Whether a CSV cell fits. An empty cell or a null value fits anything.
    fn cell(self, cell: &str, nulls: &[String]) -> bool {
        let cell = cell.trim();
        if cell.is_empty() || nulls.iter().any(|n| n == cell) {
            return true;
        }
        match self {
            Fits::Integer => cell.parse::<i64>().is_ok() || cell.parse::<u64>().is_ok(),
            Fits::Number => cell.parse::<f64>().is_ok(),
            Fits::Boolean => {
                cell.eq_ignore_ascii_case("true") || cell.eq_ignore_ascii_case("false")
            }
            Fits::Text | Fits::Anything => true,
        }
    }

    /// Whether a JSON value fits.
    fn value(self, value: &serde_json::Value) -> bool {
        match self {
            _ if value.is_null() => true,
            Fits::Integer => value.is_i64() || value.is_u64(),
            Fits::Number => value.is_number(),
            Fits::Boolean => value.is_boolean(),
            // Polars reads an array into a text column as its JSON text: journalctl's
            // bytes for a message that is not UTF-8.
            Fits::Text => value.is_string() || value.is_array(),
            Fits::Anything => true,
        }
    }
}

/// How the file's records become rows.
#[derive(Clone, Debug)]
enum Layout {
    Delimited {
        separator: u8,
        /// Records before the header that are not rows: `--skip-lines` and
        /// `--skip-rows`.
        skip: u64,
        header: bool,
        comment: Option<Vec<u8>>,
        nulls: Vec<String>,
    },
    Lines,
    /// An Arrow IPC stream: its record batch messages.
    Stream,
    /// Text read as lines: every line a row, blank ones too.
    Text,
}

/// Rows a [`Tail`] marked that its follow's [`Marks`] does not have yet, as (row, byte
/// where its record starts), and the last mark made.
#[derive(Clone, Debug, Default)]
struct NewMarks {
    new: Vec<(u64, u64)>,
    last: Option<(u64, u64)>,
}

/// The complete records of a growing delimited or NDJSON file: where they end, and
/// how many rows they hold. Extended with the bytes that arrive; a partial last record
/// is not counted until its newline lands.
#[derive(Clone, Debug)]
pub struct Tail {
    /// The file counted.
    path: PathBuf,
    layout: Layout,
    /// Each column's kind, by position for delimited text and by name for NDJSON.
    columns: Vec<(String, Fits)>,
    /// Bytes through the end of the last complete record.
    complete: u64,
    /// Records counted, rows among them, and the rows that did not fit.
    records: u64,
    rows: u64,
    misfits: u64,
    /// Fields in the header, which every row should have.
    fields: Option<usize>,
    /// Rows marked since the marks were last handed to the follow's [`Marks`].
    marks: NewMarks,
    /// How far apart marks are: rows, bytes. Small in tests.
    mark_every: (u64, u64),
}

impl Tail {
    /// The tail of a file read as `format` with `options`, whose frame has `schema`,
    /// before anything is counted.
    pub fn new(format: FileFormat, options: &OpenOptions, schema: &Schema) -> Tail {
        let layout = match format.separator() {
            _ if format == FileFormat::Arrow => Layout::Stream,
            Some(separator) => Layout::Delimited {
                separator: options.separator_or(separator),
                skip: options.skip_lines.unwrap_or(0) as u64
                    + options.skip_rows.unwrap_or(0) as u64,
                header: options.has_header != Some(false),
                comment: options
                    .comment_char
                    .as_ref()
                    .filter(|c| !c.is_empty())
                    .map(|c| c.as_bytes().to_vec()),
                nulls: options
                    .null_values
                    .iter()
                    .flatten()
                    .filter(|spec| !spec.contains('='))
                    .cloned()
                    .collect(),
            },
            None if format.is_lines() => Layout::Text,
            None => Layout::Lines,
        };
        let columns = schema
            .iter()
            .map(|(name, dtype)| (name.to_string(), Fits::of(dtype)))
            .collect();
        Tail {
            path: PathBuf::new(),
            layout,
            columns,
            complete: 0,
            records: 0,
            rows: 0,
            misfits: 0,
            fields: None,
            marks: NewMarks::default(),
            mark_every: (MARK_ROWS, MARK_BYTES),
        }
    }

    /// The file counted.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rows in the complete records.
    pub fn rows(&self) -> usize {
        self.rows as usize
    }

    /// Rows that arrived after the first count and did not fit the schema.
    pub fn misfits(&self) -> usize {
        self.misfits as usize
    }

    /// Bytes through the end of the last complete record.
    pub fn complete(&self) -> u64 {
        self.complete
    }

    /// Forget what was counted, to count the file again from its start.
    fn restart(&mut self) {
        self.complete = 0;
        self.records = 0;
        self.rows = 0;
        self.misfits = 0;
        self.fields = None;
        self.marks = NewMarks::default();
    }

    /// Mark where row `row`, whose record starts at byte `start`, is: the first row, and
    /// then once enough has passed since the last mark. The first is marked so that no
    /// page is read through a Polars slice with an offset, which counts an NDJSON
    /// file's blank lines as rows (#672). A blank record is never marked: read first
    /// from a mark, it could be taken for no row at all.
    fn mark(marks: &mut NewMarks, every: (u64, u64), row: u64, start: u64, blank: bool) {
        let (rows, bytes) = every;
        if blank
            || marks.last.is_some_and(|(last_row, last_start)| {
                row - last_row < rows && start - last_start < bytes
            })
        {
            return;
        }
        marks.last = Some((row, start));
        marks.new.push((row, start));
    }

    /// Count the records `file` completes between what was counted and `len`, checking
    /// each new row against the schema when `check`.
    pub fn read_on(&mut self, file: &mut File, len: u64, check: bool) -> std::io::Result<()> {
        if len <= self.complete {
            return Ok(());
        }
        if matches!(self.layout, Layout::Stream) {
            return self.read_messages(file, len);
        }
        file.seek(SeekFrom::Start(self.complete))?;
        let mut reader = file.take(len - self.complete);
        let quoted = matches!(self.layout, Layout::Delimited { .. });
        let mut buf = vec![0u8; CHUNK];
        let mut record: Vec<u8> = Vec::new();
        let mut oversized = false;
        let mut in_quotes = false;
        let mut at = self.complete;
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            let chunk = &buf[..n];
            let mut i = 0;
            while i < n {
                let rest = &chunk[i..];
                let next = if quoted {
                    rest.iter().position(|&b| b == b'\n' || b == b'"')
                } else {
                    rest.iter().position(|&b| b == b'\n')
                };
                let Some(k) = next else {
                    keep(&mut record, rest, &mut oversized);
                    break;
                };
                keep(&mut record, &rest[..k], &mut oversized);
                let byte = rest[k];
                i += k + 1;
                if byte == b'"' {
                    in_quotes = !in_quotes;
                    keep(&mut record, b"\"", &mut oversized);
                } else if in_quotes {
                    keep(&mut record, b"\n", &mut oversized);
                } else {
                    self.end_record(&record, oversized, check);
                    record.clear();
                    oversized = false;
                    self.complete = at + i as u64;
                }
            }
            at += n as u64;
        }
        Ok(())
    }

    /// Count the record batches of a stream whose messages `file` completes between what
    /// was counted and `len`. Only their headers are read.
    fn read_messages(&mut self, file: &mut File, len: u64) -> std::io::Result<()> {
        file.seek(SeekFrom::Start(self.complete))?;
        let mut reader = std::io::BufReader::with_capacity(CHUNK, file);
        while let Some((message, size)) = stream::next_message(&mut reader, len - self.complete)? {
            match message {
                stream::Message::Batch { rows } => {
                    Self::mark(
                        &mut self.marks,
                        self.mark_every,
                        self.rows,
                        self.complete,
                        false,
                    );
                    self.rows += rows;
                }
                stream::Message::Dictionary => {
                    return Err(std::io::Error::other(
                        "a dictionary batch arrived, which a followed stream cannot read",
                    ));
                }
                stream::Message::Schema | stream::Message::End | stream::Message::Other => {}
            }
            self.records += 1;
            self.complete += size;
        }
        Ok(())
    }

    /// One complete record, without its newline. It starts where the records counted
    /// before it end.
    fn end_record(&mut self, record: &[u8], oversized: bool, check: bool) {
        let record = record.strip_suffix(b"\r").unwrap_or(record);
        let start = self.complete;
        let index = self.records;
        self.records += 1;
        match &self.layout {
            Layout::Delimited {
                separator,
                skip,
                header,
                comment,
                nulls,
            } => {
                if index < *skip {
                    return;
                }
                if comment.as_ref().is_some_and(|c| record.starts_with(c)) {
                    return;
                }
                if *header && self.fields.is_none() {
                    self.fields = Some(split_fields(record, *separator).len());
                    return;
                }
                Self::mark(
                    &mut self.marks,
                    self.mark_every,
                    self.rows,
                    start,
                    record.is_empty(),
                );
                self.rows += 1;
                if check && (oversized || !self.cells_fit(record, *separator, nulls)) {
                    self.misfits += 1;
                }
            }
            Layout::Stream => {}
            Layout::Text => self.rows += 1,
            Layout::Lines => {
                if record.iter().all(u8::is_ascii_whitespace) {
                    return;
                }
                Self::mark(&mut self.marks, self.mark_every, self.rows, start, false);
                self.rows += 1;
                if check && (oversized || !self.object_fits(record)) {
                    self.misfits += 1;
                }
            }
        }
    }

    fn cells_fit(&self, record: &[u8], separator: u8, nulls: &[String]) -> bool {
        let cells = split_fields(record, separator);
        let expected = self.fields.unwrap_or(self.columns.len());
        // An empty line is a row of nulls, as Polars reads it.
        if record.is_empty() {
            return true;
        }
        cells.len() == expected
            && cells.iter().zip(&self.columns).all(|(cell, (_, fits))| {
                let text = String::from_utf8_lossy(cell);
                fits.cell(unquote(&text), nulls)
            })
    }

    fn object_fits(&self, record: &[u8]) -> bool {
        let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(record) else {
            return false;
        };
        object.iter().all(|(key, value)| {
            self.columns
                .iter()
                .find(|(name, _)| name == key)
                .is_some_and(|(_, fits)| fits.value(value))
        })
    }
}

/// Add `bytes` to the record being read, up to [`LONGEST_RECORD`].
fn keep(record: &mut Vec<u8>, bytes: &[u8], oversized: &mut bool) {
    if record.len() + bytes.len() > LONGEST_RECORD {
        *oversized = true;
        return;
    }
    record.extend_from_slice(bytes);
}

/// A delimited record's fields, quotes respected.
fn split_fields(record: &[u8], separator: u8) -> Vec<&[u8]> {
    let mut fields = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, &b) in record.iter().enumerate() {
        if b == b'"' {
            in_quotes = !in_quotes;
        } else if b == separator && !in_quotes {
            fields.push(&record[start..i]);
            start = i + 1;
        }
    }
    fields.push(&record[start..]);
    fields
}

fn unquote(cell: &str) -> &str {
    let trimmed = cell.trim();
    trimmed
        .strip_prefix('"')
        .and_then(|c| c.strip_suffix('"'))
        .unwrap_or(trimmed)
}

/// Whether two spellings of a path name one file. Polars keeps its own spelling of a
/// scan's path, which on Windows need not match ours character for character.
fn same_file(a: &str, b: &str) -> bool {
    if a == b || Path::new(a) == Path::new(b) {
        return true;
    }
    matches!(
        (std::fs::canonicalize(a), std::fs::canonicalize(b)),
        (Ok(a), Ok(b)) if a == b
    )
}

/// Whether `plan` is a scan of `path` and nothing else.
fn scans(plan: &polars::lazy::dsl::DslPlan, path: &str) -> bool {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::Scan {
            sources: ScanSources::Paths(paths),
            ..
        } if paths.len() == 1 => same_file(paths[0].as_str(), path),
        DslPlan::Scan { .. } => stream::StreamScan::of(plan, path).is_some(),
        DslPlan::IR { dsl, .. } => scans(dsl, path),
        _ => false,
    }
}

/// `lf` reading the file at `path` only up to its first `rows` rows. A scan of it with
/// no bound gets one right above it; one bounded already has its bound moved. The
/// frames built on a scan carry the scan in their plans, so moving its bound moves
/// what every one of them reads.
pub fn bound(lf: &mut LazyFrame, path: &Path, rows: usize) {
    let path = path.to_string_lossy();
    let rows = IdxSize::try_from(rows).unwrap_or(IdxSize::MAX);
    bound_plan(&mut lf.logical_plan, &path, rows);
}

fn bound_plan(plan: &mut polars::lazy::dsl::DslPlan, path: &str, rows: IdxSize) {
    use polars::lazy::dsl::DslPlan;
    if crate::lines::bound(plan, rows) {
        return;
    }
    match plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // bound the plan it came from, and leave the IR behind.
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            bound_plan(&mut inner, path, rows);
            *plan = inner;
            return;
        }
        DslPlan::Slice {
            input,
            offset: 0,
            len,
        } if scans(input, path) => {
            *len = rows;
            return;
        }
        DslPlan::Scan { .. } if scans(plan, path) => {
            let scan = std::mem::take(plan);
            *plan = DslPlan::Slice {
                input: Arc::new(scan),
                offset: 0,
                len: rows,
            };
            return;
        }
        _ => {}
    }
    crate::widgets::datatable::for_each_input(plan, &mut |input| bound_plan(input, path, rows));
}

/// `lf` reading the file at `path` through `file`, an open handle on it, rather than by
/// its name: the file was deleted, and the handle still reads what it held.
pub fn read_through(lf: &mut LazyFrame, path: &Path, file: &File) {
    let path = path.to_string_lossy();
    read_through_plan(&mut lf.logical_plan, &path, file);
}

fn read_through_plan(plan: &mut polars::lazy::dsl::DslPlan, path: &str, file: &File) {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            read_through_plan(&mut inner, path, file);
            *plan = inner;
            return;
        }
        DslPlan::Scan { .. } if scans(plan, path) => {
            let held = stream::StreamScan::of(plan, path).and_then(|scan| scan.held(file));
            if let DslPlan::Scan {
                sources,
                scan_type,
                cached_ir,
                ..
            } = plan
                && let Ok(handle) = file.try_clone()
            {
                match (held, &mut **scan_type) {
                    (Some(held), polars::lazy::dsl::FileScanDsl::Anonymous { function, .. }) => {
                        *function = Arc::new(held);
                    }
                    _ => *sources = ScanSources::Files(Arc::from([handle])),
                }
                // The conversion cached for the path would read the path.
                *cached_ir = Default::default();
            }
            return;
        }
        _ => {}
    }
    crate::widgets::datatable::for_each_input(plan, &mut |input| {
        read_through_plan(input, path, file)
    });
}

/// Where rows of a followed file start, every so many rows ([`MARK_ROWS`],
/// [`MARK_BYTES`]), and where its complete records end: a window deep in the file is
/// read from the mark before it rather than from the file's start. The watcher makes
/// the marks in the pass that counts the new records, so they cost no read of their own.
#[derive(Default)]
pub struct Marks {
    inner: Mutex<MarksInner>,
}

#[derive(Default)]
struct MarksInner {
    /// (row, byte where its record starts), rows ascending.
    at: Vec<(u64, u64)>,
    complete: u64,
}

/// The bytes holding a run of rows: from the start of the record of `row`, the mark at or
/// before the run, to `end`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Span {
    row: u64,
    start: u64,
    end: u64,
}

impl Marks {
    fn lock(&self) -> std::sync::MutexGuard<'_, MarksInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take the marks `tail` made since the last call, and where its records end.
    fn take_from(&self, tail: &mut Tail) {
        let mut inner = self.lock();
        inner.at.append(&mut tail.marks.new);
        inner.complete = tail.complete;
    }

    /// The file is read again from its start: the marks so far are of another file.
    fn clear(&self) {
        let mut inner = self.lock();
        inner.at.clear();
        inner.complete = 0;
    }

    /// The bytes holding rows `[from, to)`: from the last mark at or before `from` to
    /// the first at or after `to`, or to the end of the complete records. `None` before
    /// the first mark (a blank first row) and when the bytes are more than
    /// [`MOST_FROM_A_MARK`].
    fn span(&self, from: u64, to: u64) -> Option<Span> {
        let inner = self.lock();
        let before = inner.at.partition_point(|&(row, _)| row <= from);
        let (row, start) = *inner.at.get(before.checked_sub(1)?)?;
        let after = inner.at.partition_point(|&(row, _)| row < to);
        let end = inner.at.get(after).map_or(inner.complete, |&(_, at)| at);
        (end >= start && end - start <= MOST_FROM_A_MARK).then_some(Span { row, start, end })
    }
}

/// How a run of a followed file's bytes, starting at a record, becomes rows: as the
/// file's scan reads them, with no header and nothing skipped.
#[derive(Clone)]
enum Parse {
    Csv(Box<CsvReadOptions>),
    Lines { ignore_errors: bool },
    Stream(Arc<stream::StreamSchema>),
}

/// The scan under `plan`, seen through the wrapper a schema request leaves.
fn scan_node(plan: &polars::lazy::dsl::DslPlan) -> &polars::lazy::dsl::DslPlan {
    match plan {
        polars::lazy::dsl::DslPlan::IR { dsl, .. } => scan_node(dsl),
        plan => plan,
    }
}

impl Parse {
    /// How `scan` reads its rows, when a run of them can be read the same way: a CSV
    /// or NDJSON scan of every column, with no row index or path column.
    fn of(scan: &polars::lazy::dsl::DslPlan, schema: &SchemaRef) -> Option<Parse> {
        use polars::lazy::dsl::{DslPlan, FileScanDsl};
        if let Some(stream) = stream::StreamScan::in_plan(scan_node(scan)) {
            return Some(Parse::Stream(stream.schema().clone()));
        }
        let DslPlan::Scan {
            scan_type,
            unified_scan_args,
            ..
        } = scan_node(scan)
        else {
            return None;
        };
        if unified_scan_args.row_index.is_some() || unified_scan_args.include_file_paths.is_some() {
            return None;
        }
        match &**scan_type {
            FileScanDsl::Csv { options } => {
                if options.columns.is_some()
                    || options.projection.is_some()
                    || options.row_index.is_some()
                {
                    return None;
                }
                let mut options = (**options).clone();
                options.path = None;
                options.has_header = false;
                options.skip_rows = 0;
                options.skip_lines = 0;
                options.skip_rows_after_header = 0;
                options.n_rows = None;
                // The names and types the scan settled on, by position.
                options.schema = Some(schema.clone());
                options.schema_overwrite = None;
                options.dtype_overwrite = None;
                options.column_names_overwrite = None;
                options.raise_if_empty = false;
                Some(Parse::Csv(Box::new(options)))
            }
            FileScanDsl::NDJson { options } => Some(Parse::Lines {
                ignore_errors: options.ignore_errors,
            }),
            _ => None,
        }
    }
}

/// Rows `[skip, skip + take)` of the records in `span` of a followed file, read when
/// the frame is collected.
struct Piece {
    path: PathBuf,
    span: Span,
    skip: usize,
    take: usize,
    parse: Parse,
    schema: SchemaRef,
}

/// The name a read from a mark carries in a plan.
const PIECE_NAME: &str = "FOLLOWED";

impl polars::prelude::AnonymousScan for Piece {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn scan(&self, args: polars::prelude::AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let take = args.n_rows.map_or(self.take, |n| n.min(self.take));
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(self.span.start))?;
        let mut bytes = Vec::with_capacity((self.span.end - self.span.start) as usize);
        file.take(self.span.end - self.span.start)
            .read_to_end(&mut bytes)?;
        let df = match &self.parse {
            Parse::Csv(options) => {
                let mut options = (**options).clone();
                options.n_rows = Some(self.skip + take);
                options
                    .into_reader_with_file_handle(std::io::Cursor::new(bytes))
                    .finish()?
            }
            Parse::Lines { ignore_errors } => {
                polars::io::ndjson::core::parse_ndjson(&bytes, None, &self.schema, *ignore_errors)?
            }
            Parse::Stream(schema) => stream::decode_run(bytes, schema, self.skip + take)?,
        };
        Ok(df.slice(self.skip as i64, take))
    }
}

/// `lf` with the bounded scan of the followed file at `path` reading only its rows
/// `[from, to)` (`to` at most the bound, the bound when `None`), from the mark before
/// them: whatever the view does above the scan is done to those rows alone. `None` when
/// the marks do not reach them or the plan has no bounded scan of the file.
pub(crate) fn from_marks(
    lf: &LazyFrame,
    path: &Path,
    marks: &Marks,
    from: usize,
    to: Option<usize>,
) -> Option<LazyFrame> {
    let path_text = path.to_string_lossy();
    let mut plan = lf.logical_plan.clone();
    let mut replaced = false;
    let mut failed = false;
    let piece = |scan: &polars::lazy::dsl::DslPlan, bound: usize| {
        let to = to.map_or(bound, |to| to.min(bound));
        let from = from.min(to);
        let span = marks.span(from as u64, to as u64)?;
        let schema = LazyFrame::from(scan.clone()).collect_schema().ok()?;
        let parse = Parse::of(scan, &schema)?;
        let piece = Piece {
            path: path.to_path_buf(),
            span,
            skip: from - span.row as usize,
            take: to - from,
            parse,
            schema: schema.clone(),
        };
        LazyFrame::anonymous_scan(
            Arc::new(piece),
            ScanArgsAnonymous {
                schema: Some(schema),
                name: PIECE_NAME,
                ..Default::default()
            },
        )
        .ok()
        .map(|lf| lf.logical_plan)
    };
    replace_bound(
        &mut plan,
        &path_text,
        &mut |scan, bound| match piece(scan, bound) {
            Some(plan) => {
                replaced = true;
                Some(plan)
            }
            None => {
                failed = true;
                None
            }
        },
    );
    (replaced && !failed).then(|| {
        let mut out = lf.clone();
        out.logical_plan = plan;
        out
    })
}

/// Put `with(scan, bound)` where `plan` reads the file at `path` through its bound.
fn replace_bound(
    plan: &mut polars::lazy::dsl::DslPlan,
    path: &str,
    with: &mut dyn FnMut(&polars::lazy::dsl::DslPlan, usize) -> Option<polars::lazy::dsl::DslPlan>,
) {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            replace_bound(&mut inner, path, with);
            *plan = inner;
            return;
        }
        DslPlan::Slice {
            input,
            offset: 0,
            len,
        } if scans(input, path) => {
            if let Some(piece) = with(input, *len as usize) {
                *plan = piece;
            }
            return;
        }
        _ => {}
    }
    crate::widgets::datatable::for_each_input(plan, &mut |input| replace_bound(input, path, with));
}

/// How many rows the frame `lf` reads of the followed file at `path`: its bound.
pub(crate) fn bound_of(lf: &LazyFrame, path: &Path) -> Option<usize> {
    use polars::lazy::dsl::DslPlan;
    let path = path.to_string_lossy();
    (&lf.logical_plan).into_iter().find_map(|node| match node {
        DslPlan::Slice {
            input,
            offset: 0,
            len,
        } if scans(input, &path) => Some(*len as usize),
        _ => None,
    })
}

/// The windows of a followed file's view, each read from the mark before it. A view
/// of the rows as they are reads its rows straight; one that only filters them reads
/// on from `known`, a point where the rows of the view before it are known (view row,
/// file row), and slices.
pub(crate) struct Window {
    pub(crate) lf: LazyFrame,
    pub(crate) path: PathBuf,
    pub(crate) marks: Arc<Marks>,
    pub(crate) known: Option<Vec<(usize, usize)>>,
}

impl crate::pushdown::Windowed for Window {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let read = match &self.known {
            None => from_marks(&self.lf, &self.path, &self.marks, start, Some(start + len)),
            Some(known) => {
                let at = known.partition_point(|&(view, _)| view <= start);
                known.get(at.wrapping_sub(1)).and_then(|&(view, row)| {
                    from_marks(&self.lf, &self.path, &self.marks, row, None)
                        .map(|lf| lf.slice((start - view) as i64, len as IdxSize))
                })
            }
        };
        // Short of marks, Polars reads from the start of the file.
        Ok(read.unwrap_or_else(|| self.lf.clone().slice(start as i64, len as IdxSize)))
    }
}

/// What the watcher found.
#[derive(Clone)]
pub enum Change {
    /// More complete rows: how many there are now, and how many of the rows that came
    /// in since the open do not fit the schema.
    Grew { rows: usize, misfits: usize },
    /// The file shrank or was replaced (truncated, rotated): it is read again from its
    /// start, which holds `rows` rows.
    Restarted { rows: usize, misfits: usize },
    /// The file is gone. `handle` still reads what it held.
    Gone { handle: Option<Arc<File>> },
    /// Standard input ended: with the reason when it ended in an error.
    Ended(Option<String>),
    /// The file could not be read.
    Failed(String),
}

/// One report from a watcher, for the follow named `id`.
#[derive(Clone)]
pub struct News {
    pub(crate) id: u64,
    pub(crate) change: Change,
}

/// What stops the watcher, and wakes it to look now.
#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    poke: Mutex<bool>,
    woken: Condvar,
    /// Wakes a watcher waiting on inotify rather than on `woken`.
    #[cfg(target_os = "linux")]
    bell: notify::Bell,
}

impl Shared {
    /// Wait out `interval`, or until poked or stopped. Whether to go on.
    fn wait(&self, interval: Duration) -> bool {
        let mut poked = self.poke.lock().unwrap_or_else(|e| e.into_inner());
        if !*poked && !self.stop.load(Ordering::Relaxed) {
            poked = self
                .woken
                .wait_timeout(poked, interval)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *poked = false;
        !self.stop.load(Ordering::Relaxed)
    }

    fn wake(&self) {
        *self.poke.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.woken.notify_all();
        #[cfg(target_os = "linux")]
        self.bell.ring();
    }

    /// Wait until the file changes, as `notify` hears, or until poked or stopped. A
    /// change is looked at no sooner than `interval` after the last look, `last`, so a
    /// burst of appends is one look. Whether to go on.
    #[cfg(target_os = "linux")]
    fn wait_for_change(
        &self,
        notify: &notify::Notify,
        interval: Duration,
        last: &mut Option<Instant>,
    ) -> bool {
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return false;
            }
            if std::mem::take(&mut *self.poke.lock().unwrap_or_else(|e| e.into_inner())) {
                break;
            }
            if notify.wait(&self.bell, None) == notify::Woke::Changed {
                let left = last
                    .map(|at| at + interval)
                    .and_then(|due| due.checked_duration_since(Instant::now()));
                // Returns early when poked.
                if let Some(left) = left
                    && !self.wait(left)
                {
                    return false;
                }
                break;
            }
        }
        // What changed before this look is read by it.
        notify.drain();
        *last = Some(Instant::now());
        !self.stop.load(Ordering::Relaxed)
    }
}

/// How long ago, `elapsed`, as the follow chip says it: in the largest whole unit,
/// padded so the chip keeps its width as the number grows.
pub fn age(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    match secs {
        0..60 => format!("{secs:>2}s ago"),
        60..3_600 => format!("{:>2}m ago", secs / 60),
        3_600..86_400 => format!("{:>2}h ago", secs / 3_600),
        _ => format!("{:>2}d ago", secs / 86_400),
    }
}

/// When the age of an append made at `at` next reads differently.
pub fn next_tick(at: Instant) -> Instant {
    let secs = at.elapsed().as_secs();
    let unit = match secs {
        0..60 => 1,
        60..3_600 => 60,
        3_600..86_400 => 3_600,
        _ => 86_400,
    };
    at + Duration::from_secs((secs / unit + 1) * unit)
}

/// Where the follow stands, as the control bar says it.
#[derive(Clone, Debug, PartialEq)]
pub enum Standing {
    Following,
    Paused,
    /// Standard input ended, or the file went: what is on screen is all there is.
    Ended,
}

/// A file being followed: its watcher, and what the view has taken of it. Belongs to
/// the dataset; dropping it stops the watcher.
pub struct Follow {
    id: u64,
    /// The file the frame scans.
    path: PathBuf,
    shared: Arc<Shared>,
    spool: Option<Arc<SpoolHandle>>,
    /// Rows the watcher has counted, and the misfits among them.
    counted: usize,
    misfits: usize,
    /// The file was read again from its start, and the view has not caught up.
    restarted: bool,
    /// Rows the frame reads now.
    shown: usize,
    /// Rows that arrived below the cursor while it was not on the last row.
    pub(crate) new_below: usize,
    pub(crate) standing: Standing,
    pub(crate) last_append: Option<Instant>,
    /// The view has not gone to the last row yet: a follow starts there, as `tail -f`
    /// does, once the first page is drawn.
    pub(crate) settle_at_end: bool,
    /// The cursor was on the last row when rows arrived: it goes to the new last row
    /// once they are read.
    pub(crate) end_pending: bool,
    /// Rows were taken while the table was not on screen: it reads its rows again
    /// when it is.
    pub(crate) stale_view: bool,
    /// The handle a deleted file is read through from now on.
    held: Option<Arc<File>>,
    /// Where its rows start, every so many.
    marks: Arc<Marks>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl Follow {
    /// Follow the file `tail` counted, whose first `tail.rows()` rows the frame reads,
    /// checking every `interval` and telling `events`. `spool` is standard input being
    /// copied to it.
    pub fn start(
        mut tail: Tail,
        interval: Duration,
        events: Sender<AppEvent>,
        spool: Option<Arc<SpoolHandle>>,
    ) -> Follow {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = tail.path.clone();
        let shared = Arc::new(Shared::default());
        let shown = tail.rows();
        if let Some(handle) = &spool {
            handle.spool.wake_on_end(shared.clone());
        }
        let marks = Arc::new(Marks::default());
        marks.take_from(&mut tail);
        let watcher = Watcher {
            marks: marks.clone(),
            id,
            path: path.clone(),
            tail,
            shared: shared.clone(),
            events,
            spool: spool.as_ref().map(|handle| handle.spool.clone()),
            interval,
        };
        let _ = std::thread::Builder::new()
            .name("datui-follow".to_string())
            .spawn(move || watcher.run());
        Follow {
            id,
            path,
            shared,
            spool,
            counted: shown,
            misfits: 0,
            restarted: false,
            shown,
            new_below: 0,
            standing: Standing::Following,
            last_append: None,
            settle_at_end: true,
            end_pending: false,
            stale_view: false,
            held: None,
            marks,
        }
    }

    /// Where the file's rows start, every so many.
    pub(crate) fn marks(&self) -> &Arc<Marks> {
        &self.marks
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rows the frame reads.
    pub fn shown(&self) -> usize {
        self.shown
    }

    /// Rows counted that the view has not taken yet.
    pub fn waiting(&self) -> usize {
        self.counted.saturating_sub(self.shown)
    }

    pub fn misfits(&self) -> usize {
        self.misfits
    }

    pub fn standing(&self) -> &Standing {
        &self.standing
    }

    /// Rows that arrived below the cursor while it was off the last row.
    pub fn new_below(&self) -> usize {
        self.new_below
    }

    /// Whether the view has rows or a restart to take: not while paused. The rows
    /// counted before standard input ended are taken after it did.
    pub fn behind(&self) -> bool {
        self.standing != Standing::Paused && (self.restarted || self.counted != self.shown)
    }

    /// Standard input being copied, if this follows it.
    pub fn spool(&self) -> Option<&Arc<Spool>> {
        self.spool.as_ref().map(|handle| &handle.spool)
    }

    /// Look now rather than at the end of the interval.
    pub fn check_now(&self) {
        self.shared.wake();
    }

    /// Take a report from this follow's watcher. Returns what the user is told, if
    /// anything.
    pub fn take(&mut self, change: &Change) -> Option<String> {
        match change {
            Change::Grew { rows, misfits } => {
                if *rows > self.counted {
                    self.last_append = Some(Instant::now());
                }
                self.counted = *rows;
                self.misfits = *misfits;
                None
            }
            Change::Restarted { rows, misfits } => {
                self.counted = *rows;
                self.misfits = *misfits;
                self.restarted = true;
                self.last_append = Some(Instant::now());
                Some("The file was truncated or replaced: reading it from the start".to_string())
            }
            Change::Gone { handle } => {
                self.held = handle.clone();
                self.end();
                Some("The file was deleted: following stopped, the rows read stay".to_string())
            }
            // A recording's end is said by the recording's own mark.
            Change::Ended(_) if self.spool().is_some_and(|s| s.tee().is_some()) => {
                self.end();
                None
            }
            Change::Ended(None) => {
                self.end();
                Some("Standard input ended".to_string())
            }
            Change::Ended(Some(reason)) | Change::Failed(reason) => {
                self.end();
                Some(reason.clone())
            }
        }
    }

    /// The view takes what was counted: the rows its frame reads from now on, and
    /// whether the file was read again from its start.
    pub fn catch_up(&mut self) -> (usize, bool) {
        self.shown = self.counted;
        (self.shown, std::mem::take(&mut self.restarted))
    }

    /// The handle a deleted file is read through, once, for the frames to take.
    pub fn take_held(&mut self) -> Option<Arc<File>> {
        self.held.take()
    }

    pub fn pause(&mut self) {
        if self.standing == Standing::Following {
            self.standing = Standing::Paused;
        }
    }

    pub fn resume(&mut self) {
        if self.standing == Standing::Paused {
            self.standing = Standing::Following;
        }
    }

    /// Stop watching. What the frame reads stays. Standard input stops being read,
    /// unless it is being recorded (`--tee`): that is stopped only when asked.
    pub fn end(&mut self) {
        self.standing = Standing::Ended;
        self.shared.stop.store(true, Ordering::Relaxed);
        self.shared.wake();
        if let Some(spool) = self.spool.as_ref().filter(|s| s.spool.tee().is_none()) {
            spool.spool.stop();
        }
    }
}

impl Drop for Follow {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        self.shared.wake();
    }
}

/// The thread that watches the file.
struct Watcher {
    id: u64,
    path: PathBuf,
    tail: Tail,
    shared: Arc<Shared>,
    events: Sender<AppEvent>,
    spool: Option<Arc<Spool>>,
    interval: Duration,
    marks: Arc<Marks>,
}

/// Which file this is, so a replaced file is told from a grown one: (device, inode) on
/// Unix, (volume serial, file index) on Windows.
type Identity = (u64, u64);

#[cfg(unix)]
fn identity_of(file: &File) -> Option<Identity> {
    use std::os::unix::fs::MetadataExt;
    file.metadata().ok().map(|meta| (meta.dev(), meta.ino()))
}

#[cfg(windows)]
fn identity_of(file: &File) -> Option<Identity> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    // SAFETY: the handle is `file`'s, open while this runs, and `info` is plain data
    // the call fills in; zeroed is a valid value of it.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) };
    (ok != 0).then(|| {
        (
            u64::from(info.dwVolumeSerialNumber),
            u64::from(info.nFileIndexHigh) << 32 | u64::from(info.nFileIndexLow),
        )
    })
}

#[cfg(not(any(unix, windows)))]
fn identity_of(_file: &File) -> Option<Identity> {
    None
}

/// Which file `path` names now, `meta` its metadata. Unix reads it from the metadata;
/// Windows has to open the file to ask.
#[cfg(unix)]
fn identity_at(_path: &Path, meta: &std::fs::Metadata) -> Option<Identity> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn identity_at(path: &Path, _meta: &std::fs::Metadata) -> Option<Identity> {
    File::open(path).ok().as_ref().and_then(identity_of)
}

/// Whether the file a path names is another one than the file followed. Unknown on
/// either side is not a replacement: a shrink still tells a truncation.
fn replaced(known: Option<Identity>, now: Option<Identity>) -> bool {
    matches!((known, now), (Some(known), Some(now)) if known != now)
}

impl Watcher {
    fn run(mut self) {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(e) => {
                self.send(Change::Failed(format!("Could not follow the file: {e}")));
                return;
            }
        };
        let mut known = identity_of(&file);
        let mut sent = (self.tail.rows(), 0usize);
        #[cfg(target_os = "linux")]
        let notify = notify::Notify::new(&self.path);
        #[cfg(target_os = "linux")]
        let mut last = None;
        loop {
            #[cfg(target_os = "linux")]
            let go_on = match &notify {
                Some(notify) => self
                    .shared
                    .wait_for_change(notify, self.interval, &mut last),
                None => self.shared.wait(self.interval),
            };
            #[cfg(not(target_os = "linux"))]
            let go_on = self.shared.wait(self.interval);
            if !go_on {
                return;
            }
            // Read before the file, so nothing the spool wrote before it ended is missed.
            let spool_ended = self.spool.as_ref().and_then(|spool| spool.ended());
            let meta = match std::fs::metadata(&self.path) {
                Ok(meta) => meta,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.send(Change::Gone {
                        handle: Some(Arc::new(file)),
                    });
                    return;
                }
                Err(e) => {
                    self.send(Change::Failed(format!("Could not follow the file: {e}")));
                    return;
                }
            };
            let now = identity_at(&self.path, &meta);
            let len = meta.len();
            if replaced(known, now) || len < self.tail.complete() {
                match File::open(&self.path) {
                    Ok(reopened) => file = reopened,
                    Err(e) => {
                        self.send(Change::Failed(format!("Could not follow the file: {e}")));
                        return;
                    }
                }
                known = identity_of(&file);
                #[cfg(target_os = "linux")]
                if let Some(notify) = &notify {
                    notify.rewatch(&self.path);
                }
                self.tail.restart();
                self.marks.clear();
                if let Err(e) = self.tail.read_on(&mut file, len, false) {
                    self.send(Change::Failed(format!("Could not read the file: {e}")));
                    return;
                }
                self.marks.take_from(&mut self.tail);
                sent = (self.tail.rows(), self.tail.misfits());
                self.send(Change::Restarted {
                    rows: sent.0,
                    misfits: sent.1,
                });
                continue;
            }
            if let Err(e) = self.tail.read_on(&mut file, len, true) {
                self.send(Change::Failed(format!("Could not read the file: {e}")));
                return;
            }
            // Before the rows are reported, so the view's reads of them find marks.
            self.marks.take_from(&mut self.tail);
            let now_counted = (self.tail.rows(), self.tail.misfits());
            if now_counted != sent {
                sent = now_counted;
                if !self.send(Change::Grew {
                    rows: sent.0,
                    misfits: sent.1,
                }) {
                    return;
                }
            }
            if let Some(reason) = spool_ended {
                self.send(Change::Ended(reason));
                return;
            }
        }
    }

    /// Whether the app is still there to hear it.
    fn send(&self, change: Change) -> bool {
        self.events
            .send(AppEvent::Followed(News {
                id: self.id,
                change,
            }))
            .is_ok()
    }
}

/// Standard input being copied to a file while the file is read: the copy goes on
/// after the first rows show, until the stream ends or the copy is stopped. The file is
/// a temporary one, or the one `--tee` names, which the user keeps; with `--tee -`, a
/// temporary one, and the stream is passed on to standard output too.
pub struct Spool {
    stop: AtomicBool,
    bytes: AtomicU64,
    state: Mutex<SpoolState>,
    changed: Condvar,
    /// The file being written. Taken when the copy finishes, so a read still waiting on
    /// the producer writes nothing after it.
    sink: Mutex<Option<File>>,
    /// The file `--tee` named, when it is the one written.
    tee: Option<Tee>,
    /// Standard output, for `--tee -`. Taken when the copy finishes, which closes it.
    pass: Mutex<Option<Box<dyn Write + Send>>>,
    started: Instant,
}

/// The file `--tee` named.
#[derive(Clone, Debug)]
pub struct Tee {
    pub path: PathBuf,
    /// `--tee-raw`: the bytes exactly as they came, a WAV header's sizes included.
    pub raw: bool,
}

impl Tee {
    /// `--tee -`: the stream is passed on to standard output rather than kept in a file.
    pub fn to_stdout(&self) -> bool {
        crate::stdin::is_stdin(&self.path)
    }

    /// Where the stream goes, as a message names it.
    pub fn name(&self) -> String {
        if self.to_stdout() {
            return "standard output".to_string();
        }
        self.path.file_name().map_or_else(
            || self.path.display().to_string(),
            |name| name.to_string_lossy().into_owned(),
        )
    }
}

#[derive(Default)]
struct SpoolState {
    /// Newlines copied so far, up to what the open waits for.
    lines: usize,
    /// The last read took all the producer had: it is slower than the copy.
    drained: bool,
    /// The stream ended: `Some(reason)` when in an error.
    ended: Option<Option<String>>,
    /// When the copy finished, and what finishing the file said.
    finished: Option<Instant>,
    /// Bytes copied by when, a few seconds of them, for the rate.
    samples: std::collections::VecDeque<(Instant, u64)>,
    /// The watcher following the file, woken when the copy ends: a watcher waiting
    /// for the file to change would not hear an end that writes nothing.
    watcher: Option<Arc<Shared>>,
}

/// How far back the rate looks.
const RATE_WINDOW: Duration = Duration::from_secs(2);

impl Spool {
    fn new(sink: File, tee: Option<Tee>, pass: Option<Box<dyn Write + Send>>) -> Spool {
        Spool {
            stop: AtomicBool::new(false),
            bytes: AtomicU64::new(0),
            state: Mutex::new(SpoolState::default()),
            changed: Condvar::new(),
            sink: Mutex::new(Some(sink)),
            tee,
            pass: Mutex::new(pass),
            started: Instant::now(),
        }
    }

    /// Bytes copied so far.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Bytes a second over the last few seconds.
    pub fn rate(&self) -> f64 {
        let state = self.lock();
        match (state.samples.front(), state.samples.back()) {
            (Some((t0, b0)), Some((t1, b1))) if t1 > t0 => {
                (b1 - b0) as f64 / t1.duration_since(*t0).as_secs_f64()
            }
            _ => 0.0,
        }
    }

    /// How long the copy ran, or has run.
    pub fn duration(&self) -> Duration {
        let finished = self.lock().finished;
        finished
            .unwrap_or_else(Instant::now)
            .duration_since(self.started)
    }

    /// The file `--tee` named, when the copy goes there.
    pub fn tee(&self) -> Option<&Tee> {
        self.tee.as_ref()
    }

    /// Stop copying and finish the file: a read waiting on the producer writes
    /// nothing more when it returns.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.finish(None);
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Whether the copy ended, and the reason when in an error.
    pub fn ended(&self) -> Option<Option<String>> {
        self.lock().ended.clone()
    }

    /// Whether bytes may still arrive: the producer is still sending.
    pub fn live(&self) -> bool {
        self.lock().ended.is_none()
    }

    /// Wait until the copy has ended.
    pub fn wait(&self) {
        let mut state = self.lock();
        while state.ended.is_none() {
            state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SpoolState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Write `bytes` as they came. False once the copy is finished.
    fn write(&self, bytes: &[u8]) -> Result<bool, String> {
        let mut sink = self.sink.lock().unwrap_or_else(|e| e.into_inner());
        let Some(file) = sink.as_mut() else {
            return Ok(false);
        };
        file.write_all(bytes).map_err(|e| {
            format!(
                "Could not write {}: {e}",
                self.tee
                    .as_ref()
                    .filter(|t| !t.to_stdout())
                    .map_or("what came in".to_string(), |t| t.path.display().to_string())
            )
        })?;
        drop(sink);
        // Outside the file's lock: a reader downstream that stops reading holds up this
        // write, and must not hold up a stop.
        if let Some(out) = self.pass.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            out.write_all(bytes)
                .and_then(|()| out.flush())
                .map_err(|e| format!("Could not write standard output: {e}"))?;
        }
        let total =
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed) + bytes.len() as u64;
        let now = Instant::now();
        let mut state = self.lock();
        if state.lines < WANTED_LINES {
            state.lines += bytes.iter().filter(|&&b| b == b'\n').count();
        }
        state.samples.push_back((now, total));
        while state
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > RATE_WINDOW)
            && state.samples.len() > 2
        {
            state.samples.pop_front();
        }
        drop(state);
        self.changed.notify_all();
        Ok(true)
    }

    /// End the copy, `reason` when it ended in an error, and finish the file: a WAV
    /// header's sizes filled in for `--tee` (unless `--tee-raw`), and the file synced so
    /// that saved means safe to copy. Once; later calls change nothing.
    fn finish(&self, reason: Option<String>) {
        let file = self.sink.lock().unwrap_or_else(|e| e.into_inner()).take();
        // Closed, so the reader downstream sees the stream end. Held by a write a reader
        // downstream is not taking, it is closed once that write returns: the copy then
        // finds the file finished and finishes again.
        if let Ok(mut pass) = self.pass.try_lock() {
            pass.take();
        }
        let mut reason = reason;
        if let (Some(mut file), Some(tee)) = (file, self.tee.as_ref().filter(|t| !t.to_stdout())) {
            let finished = (if tee.raw {
                Ok(())
            } else {
                crate::tee::fix_wav_sizes(&mut file).map(|_| ())
            })
            .and_then(|()| file.sync_all());
            if let Err(e) = finished
                && reason.is_none()
            {
                reason = Some(format!("Could not finish {}: {e}", tee.path.display()));
            }
        }
        let mut state = self.lock();
        if state.ended.is_none() {
            state.ended = Some(reason);
            state.finished = Some(Instant::now());
        }
        let watcher = state.watcher.take();
        drop(state);
        self.changed.notify_all();
        if let Some(watcher) = watcher {
            watcher.wake();
        }
    }

    /// Wake the watcher `shared` once the copy ends, or now if it has.
    fn wake_on_end(&self, shared: Arc<Shared>) {
        let mut state = self.lock();
        if state.ended.is_some() {
            drop(state);
            shared.wake();
        } else {
            state.watcher = Some(shared);
        }
    }
}

/// The open's hold on a [`Spool`]: the copy stops when the last holder lets go, so a
/// follow put down before its dataset arrived does not go on copying, and quitting
/// finishes the file.
pub struct SpoolHandle {
    spool: Arc<Spool>,
}

impl SpoolHandle {
    pub fn spool(&self) -> &Arc<Spool> {
        &self.spool
    }
}

impl Drop for SpoolHandle {
    fn drop(&mut self) {
        self.spool.stop();
    }
}

/// Copy what `reader` sends into `spool` on a thread of its own, until it ends or the
/// spool stops. Each chunk is written whole as it arrives into one buffer, reused:
/// nothing is held back, and however long the stream runs the copy holds a megabyte.
/// A producer faster than the disk waits on the pipe, not on datui's memory.
fn copy_on(mut reader: impl Read + Send + 'static, spool: Arc<Spool>) {
    let _ = std::thread::Builder::new()
        .name("datui-spool".to_string())
        .spawn(move || {
            let mut buf = vec![0u8; CHUNK];
            let reason = loop {
                if spool.stopped() {
                    break None;
                }
                let n = match reader.read(&mut buf) {
                    Ok(0) => break None,
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => break Some(format!("Standard input failed: {e}")),
                };
                match spool.write(&buf[..n]) {
                    Ok(true) => {}
                    Ok(false) => break None,
                    Err(reason) => break Some(reason),
                }
                spool.lock().drained = n < buf.len();
            };
            spool.finish(reason);
        });
}

/// Lines the open waits for before it reads the spooled file, unless the producer is
/// slower than the copy: then two, a header and a row, are enough to show.
const WANTED_LINES: usize = 1000;

/// What standard input was copied to.
pub enum Spooled {
    /// A temporary file, removed when its holders let go.
    Temp(TempDownload),
    /// The file `--tee` named, the user's.
    Kept(PathBuf),
}

/// Copy standard input, from `open`, to the file `--tee` names or else a temporary
/// file in `--temp-dir` claimed through `writer`: until enough has arrived to show when
/// following, until it ends when not. A followed copy goes on behind the answer. Says
/// what the file holds, as [`crate::stdin::spool`] does, with the copy carried in the
/// options for the dataset to hold.
pub(crate) fn spool<R: Read + Send + 'static>(
    open: impl FnOnce() -> crate::download::Opened<R>,
    options: OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
    stdout: Option<Box<dyn Write + Send>>,
) -> Result<(Spooled, OpenOptions), String> {
    let tee = options.tee.clone().map(|path| Tee {
        path,
        raw: options.tee_raw,
    });
    // A pipe cannot be sought back to, to fill in a WAV header.
    let tee = tee.map(|tee| Tee {
        raw: tee.raw || tee.to_stdout(),
        ..tee
    });
    let pass = match &tee {
        Some(tee) if tee.to_stdout() => Some(stdout.ok_or_else(|| {
            "--tee - passes the stream on to standard output, which only the datui command has."
                .to_string()
        })?),
        _ => None,
    };
    let (reader, _) = open().map_err(|e| format!("Could not read standard input: {e}"))?;
    let (spooled, file) = match &tee {
        Some(tee) if !tee.to_stdout() => {
            let file = crate::tee::create(&tee.path, options.force)?;
            (Spooled::Kept(tee.path.clone()), file)
        }
        _ => {
            let Some((named, claim)) = writer
                .create(|| TempDownload::create(options.temp_dir.as_deref(), None))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None))?
            else {
                return Err("Reading standard input was stopped.".to_string());
            };
            let file = named
                .as_file()
                .try_clone()
                .map_err(|e| format!("Could not write what came in: {e}"))?;
            (Spooled::Temp(TempDownload::held(named, Some(claim))), file)
        }
    };
    let spooled_path = match &spooled {
        Spooled::Temp(download) => download.path().to_path_buf(),
        Spooled::Kept(path) => path.clone(),
    };
    let spool = Arc::new(Spool::new(file, tee, pass));
    let handle = Arc::new(SpoolHandle {
        spool: spool.clone(),
    });
    copy_on(reader, spool.clone());
    // A header and a row, at least, before anything is read.
    let wanted = if options.has_header == Some(false) {
        1
    } else {
        2
    };
    let mut state = spool.lock();
    loop {
        read.store(spool.bytes(), Ordering::Relaxed);
        if writer.stopped() {
            drop(state);
            spool.stop();
            return Err("Reading standard input was stopped.".to_string());
        }
        // An Arrow stream has no lines to count: its schema message is enough.
        let enough = options.follow
            && (state.lines >= WANTED_LINES
                || (state.drained
                    && (state.lines >= wanted || stream::begins_with_schema(&spooled_path))));
        if enough || state.ended.is_some() {
            break;
        }
        state = spool
            .changed
            .wait_timeout(state, Duration::from_millis(100))
            .unwrap_or_else(|e| e.into_inner())
            .0;
    }
    if let Some(Some(reason)) = &state.ended {
        return Err(reason.clone());
    }
    drop(state);
    read.store(spool.bytes(), Ordering::Relaxed);
    let path = spooled_path;
    let mut head = Vec::new();
    File::open(&path)
        .and_then(|f| f.take(4096).read_to_end(&mut head))
        .map_err(|e| format!("Could not read standard input back: {e}"))?;
    if head.is_empty() {
        return Err("Nothing came in on standard input.".to_string());
    }
    let (format, compression, guessed) = crate::stdin::sniff_for(&head, &options);
    let options = OpenOptions {
        format_guessed: options.format.is_none() && guessed,
        format: options.format.or(Some(format)),
        compression: options.compression.or(compression),
        ..options
    };
    // A recording goes on whatever it holds; only the view is not followed then.
    if options.follow
        && options.tee.is_none()
        && !followed_stream(&path, options.format, &options)
        && let Some(refusal) = refusal(options.format, &options)
    {
        return Err(refusal);
    }
    Ok((
        spooled,
        OpenOptions {
            spool: Some(handle),
            // The file is read from here on, as any file is.
            tee: None,
            ..options
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tail_of(text: &[u8], format: FileFormat, options: &OpenOptions) -> Tail {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        std::fs::write(&path, text).unwrap();
        let mut tail = Tail::new(format, options, &Schema::default());
        let mut file = File::open(&path).unwrap();
        tail.read_on(&mut file, text.len() as u64, false).unwrap();
        tail
    }

    /// Polars' own count of the complete records of `text` read as CSV.
    fn polars_rows(text: &[u8], options: &OpenOptions) -> usize {
        let complete = text.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        let mut read = CsvReadOptions::default().with_has_header(options.has_header != Some(false));
        read = read.map_parse_options(|p| {
            p.with_comment_prefix(
                options
                    .comment_char
                    .as_deref()
                    .map(polars::io::csv::read::CommentPrefix::new_from_str),
            )
        });
        if let Some(skip) = options.skip_lines {
            read.skip_lines = skip;
        }
        CsvReader::new(std::io::Cursor::new(text[..complete].to_vec()))
            .with_options(read)
            .finish()
            .map(|df| df.height())
            .unwrap_or(0)
    }

    /// Rows counted as Polars counts them: blank lines are rows of nulls, a quoted
    /// newline is not a record's end, comments and skipped lines are not rows, and a
    /// partial last line waits.
    #[test]
    fn records_are_counted_as_polars_reads_them() {
        let cases: [(&[u8], OpenOptions); 6] = [
            (b"a,b\n1,2\n3,4\n", OpenOptions::default()),
            (b"a,b\n1,2\n\n3,4\n5,", OpenOptions::default()),
            (b"a,b\n1,\"x\ny\"\n3,4\n", OpenOptions::default()),
            (b"a,b\r\n1,2\r\n3,4\r\n", OpenOptions::default()),
            (
                b"#c\na,b\n#x\n1,2\n3,4\n",
                OpenOptions {
                    comment_char: Some("#".to_string()),
                    ..Default::default()
                },
            ),
            (
                b"junk\na,b\n1,2\n",
                OpenOptions::default().with_skip_lines(1),
            ),
        ];
        for (text, options) in cases {
            let tail = tail_of(text, FileFormat::Csv, &options);
            assert_eq!(
                tail.rows(),
                polars_rows(text, &options),
                "{}",
                String::from_utf8_lossy(text)
            );
        }
        let lines = tail_of(
            b"{\"a\":1}\n\n{\"a\":2}\n{\"a\":",
            FileFormat::Jsonl,
            &Default::default(),
        );
        assert_eq!(lines.rows(), 2);
        assert_eq!(lines.complete(), 17);
    }

    /// On Linux the watcher hears an append through inotify: with an interval of an
    /// hour, no size check would see it, and nothing pokes it.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_append_is_heard_of_without_a_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.csv");
        std::fs::write(&path, "t\n1\n").unwrap();
        let scan = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        let (_, tail) =
            bound_to_complete(scan, &path, FileFormat::Csv, &OpenOptions::default()).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let follow = Follow::start(tail, Duration::from_secs(3_600), tx, None);
        let guard = Duration::from_secs(30);
        // The watcher may not be waiting yet: append until it reports, each append a
        // change it hears once it is.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        let mut rows = 1;
        let deadline = Instant::now() + guard;
        let news = loop {
            assert!(Instant::now() < deadline, "the watcher never heard");
            file.write_all(format!("{}\n", rows + 1).as_bytes())
                .unwrap();
            rows += 1;
            if let Ok(AppEvent::Followed(news)) = rx.recv_timeout(Duration::from_millis(50)) {
                break news;
            }
        };
        assert!(matches!(news.change, Change::Grew { rows: 2.., .. }));
        drop(follow);
    }

    /// The bytes that arrive later are read from where the count stopped, a partial
    /// line among them once it completes; a row that does not fit is counted.
    #[test]
    fn a_tail_reads_on_from_where_it_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grow.csv");
        let mut out = File::create(&path).unwrap();
        out.write_all(b"t,n\n1.5,2\n2.5,").unwrap();
        let schema = Schema::from_iter([
            Field::new("t".into(), DataType::Float64),
            Field::new("n".into(), DataType::Int64),
        ]);
        let mut tail = Tail::new(FileFormat::Csv, &OpenOptions::default(), &schema);
        let mut file = File::open(&path).unwrap();
        let len = |p: &Path| std::fs::metadata(p).unwrap().len();
        tail.read_on(&mut file, len(&path), true).unwrap();
        assert_eq!((tail.rows(), tail.complete()), (1, 10));
        out.write_all(b"3\nx,4\n4.5,5,6\n").unwrap();
        tail.read_on(&mut file, len(&path), true).unwrap();
        assert_eq!(tail.rows(), 4);
        assert_eq!(
            tail.misfits(),
            2,
            "a word for a number, and a field too many"
        );
    }

    /// A scan bounded to its complete rows reads more once the bound moves, through
    /// the filters built on it, and only as many as the bound says.
    #[test]
    fn moving_the_bound_reads_the_new_rows_through_the_view() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grow.csv");
        std::fs::write(&path, "a,b\n1,x\n2,y\n3,").unwrap();
        let scan = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .with_truncate_ragged_lines(true)
            .with_ignore_errors(true)
            .finish()
            .unwrap();
        let mut root = scan.clone();
        bound(&mut root, &path, 2);
        let mut view = root.clone().filter(col("a").gt(lit(1)));
        view.collect_schema().unwrap();
        assert_eq!(view.clone().collect().unwrap().height(), 1);
        let mut out = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        out.write_all(b"z\n4,w\n5").unwrap();
        bound(&mut view, &path, 4);
        let df = view.clone().collect().unwrap();
        assert_eq!(df.height(), 3, "{df}");
        bound(&mut root, &path, 4);
        assert_eq!(root.collect().unwrap().height(), 4, "the partial row waits");
    }

    /// `text` written to a file and scanned as `scan` does, bounded to its complete
    /// rows, with a mark every `every` rows.
    fn marked(
        text: &[u8],
        format: FileFormat,
        options: &OpenOptions,
        every: u64,
        scan: impl Fn(&Path) -> LazyFrame,
    ) -> (tempfile::TempDir, PathBuf, LazyFrame, Arc<Marks>, usize) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("marked");
        std::fs::write(&path, text).unwrap();
        let mut lf = scan(&path);
        let schema = lf.collect_schema().unwrap();
        let mut tail = Tail::new(format, options, &schema);
        tail.mark_every = (every, u64::MAX);
        tail.read_on(&mut File::open(&path).unwrap(), text.len() as u64, false)
            .unwrap();
        let marks = Arc::new(Marks::default());
        marks.take_from(&mut tail);
        bound(&mut lf, &path, tail.rows());
        (dir, path, lf, marks, tail.rows())
    }

    fn csv_scan(path: &Path, options: &OpenOptions) -> LazyFrame {
        let mut reader = LazyCsvReader::new(PlRefPath::try_from_path(path).unwrap())
            .with_ignore_errors(true)
            .with_truncate_ragged_lines(true)
            .with_has_header(options.has_header != Some(false))
            .with_comment_prefix(options.comment_char.as_deref().map(PlSmallStr::from_str));
        if let Some(skip) = options.skip_lines {
            reader = reader.with_skip_lines(skip);
        }
        reader.finish().unwrap()
    }

    /// Every window read from the marks holds the rows a read from the start of the
    /// file gives: quoted newlines, blank lines, comments, skipped lines, carriage
    /// returns and NDJSON, at every offset.
    #[test]
    fn a_window_from_a_mark_reads_what_a_read_from_the_start_does() {
        let mut csv = b"skipped\nt,s,n\n".to_vec();
        let mut crlf = b"t,s,n\r\n".to_vec();
        let mut lines = Vec::new();
        for i in 0..120 {
            let row = match i % 9 {
                0 => format!("{i},\"two\nlines\",{}\n", i * 2),
                3 => "\n".to_string(),
                5 => "# a comment\n".to_string(),
                7 => format!("{i},x,oops\n"),
                _ => format!("{i},s{i},{}\n", i * 2),
            };
            csv.extend(row.as_bytes());
            crlf.extend(format!("{i},s{i},{}\r\n", i * 2).as_bytes());
            lines.extend(format!("{{\"t\":{i},\"s\":\"s{i}\"}}\n").as_bytes());
            if i % 4 == 1 {
                lines.extend(b"\n  \n");
            }
        }
        csv.extend(b"999,partial");
        let commented = OpenOptions {
            comment_char: Some("#".to_string()),
            ..OpenOptions::default().with_skip_lines(1)
        };
        // An Arrow stream of batches of three rows, the last message cut short.
        let (schema, batches) = stream_messages(&arrow_rows(0, 100), 3);
        let mut arrows = schema;
        batches.iter().for_each(|batch| arrows.extend(batch));
        arrows.extend(&batches[0][..20]);
        let cases: Vec<(&[u8], FileFormat, OpenOptions)> = vec![
            (&csv, FileFormat::Csv, commented),
            (&crlf, FileFormat::Csv, OpenOptions::default()),
            (&lines, FileFormat::Jsonl, OpenOptions::default()),
            (&arrows, FileFormat::Arrow, OpenOptions::default()),
        ];
        for (text, format, options) in cases {
            let scan = |path: &Path| match format {
                FileFormat::Jsonl => scan_lines(path, &options, &mut Vec::new()).unwrap(),
                FileFormat::Arrow => stream::scan(path).unwrap(),
                _ => csv_scan(path, &options),
            };
            let (_dir, path, lf, marks, rows) = marked(text, format, &options, 7, scan);
            let window = Window {
                lf: lf.clone(),
                path: path.clone(),
                marks: marks.clone(),
                known: None,
            };
            let whole = lf.clone().collect().unwrap();
            assert_eq!(whole.height(), rows);
            for start in (0..rows + 3).step_by(5) {
                for len in [1, 6, 40] {
                    let read = crate::pushdown::Windowed::window(&window, start, len)
                        .unwrap()
                        .collect()
                        .unwrap();
                    let expected = whole.slice(start as i64, len);
                    assert!(
                        read.equals_missing(&expected),
                        "{format:?} rows {start}+{len}:\n{read:?}\n{expected:?}"
                    );
                }
                if start < rows {
                    assert!(
                        from_marks(&lf, &path, &marks, start, Some(start + 1)).is_some(),
                        "{format:?} row {start} is read from a mark"
                    );
                }
            }
        }
    }

    /// `n` rows from `from`: a number, its text, and a float.
    fn arrow_rows(from: i64, n: i64) -> DataFrame {
        df!(
            "t" => (from..from + n).collect::<Vec<_>>(),
            "s" => (from..from + n).map(|i| format!("s{i}")).collect::<Vec<_>>(),
            "x" => (from..from + n).map(|i| i as f64 / 2.0).collect::<Vec<_>>(),
        )
        .unwrap()
    }

    /// An Arrow stream's batches are counted as their messages complete, a batch cut
    /// short waiting for the rest; the stream's own scan reads them, filtered and
    /// projected a batch at a time; and a stream with dictionaries is refused.
    #[test]
    fn an_arrow_stream_is_counted_and_read_by_its_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.arrows");
        let (schema, batches) = stream_messages(&arrow_rows(0, 50), 4);
        let mut head = schema.clone();
        head.extend(&batches[0]);
        head.extend(&batches[1][..batches[1].len() - 3]);
        std::fs::write(&path, &head).unwrap();
        let lf = stream::scan(&path).unwrap();
        let (mut lf, mut tail) =
            bound_to_complete(lf, &path, FileFormat::Arrow, &OpenOptions::default()).unwrap();
        assert_eq!(tail.rows(), 4, "the second batch is not all there");
        assert_eq!(lf.clone().collect().unwrap(), arrow_rows(0, 4));

        let mut rest = batches[1][batches[1].len() - 3..].to_vec();
        batches[2..].iter().for_each(|batch| rest.extend(batch));
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&rest).unwrap();
        let size = file.metadata().unwrap().len();
        tail.read_on(&mut File::open(&path).unwrap(), size, true)
            .unwrap();
        assert_eq!((tail.rows(), tail.misfits()), (50, 0));
        bound(&mut lf, &path, tail.rows());
        assert_eq!(lf.clone().collect().unwrap(), arrow_rows(0, 50));
        let kept = lf
            .clone()
            .filter(col("t").gt_eq(lit(45)))
            .select([col("s")])
            .collect()
            .unwrap();
        assert_eq!(kept, arrow_rows(45, 5).select(["s"]).unwrap());
        let count = lf.select([len()]).collect().unwrap();
        assert_eq!(count.column("len").unwrap().u32().unwrap().get(0), Some(50));

        // Written before Arrow 0.15, with no continuation markers, and ended.
        let legacy = dir.path().join("legacy.arrows");
        std::fs::write(
            &legacy,
            crate::ipc_stream::tests::stream(&arrow_rows(0, 10), None, true),
        )
        .unwrap();
        let (lf, tail) = bound_to_complete(
            stream::scan(&legacy).unwrap(),
            &legacy,
            FileFormat::Arrow,
            &OpenOptions::default(),
        )
        .unwrap();
        assert_eq!(tail.rows(), 10);
        assert_eq!(lf.collect().unwrap(), arrow_rows(0, 10));

        // Dictionary-encoded columns.
        let cats = df!("c" => ["a", "b", "a"])
            .unwrap()
            .lazy()
            .with_column(col("c").cast(DataType::from_categories(Categories::global())))
            .collect()
            .unwrap();
        let (schema, batches) = stream_messages(&cats, 3);
        let dictionary = dir.path().join("dict.arrows");
        std::fs::write(&dictionary, [schema, batches.concat()].concat()).unwrap();
        assert!(
            stream::scan(&dictionary).is_err_and(|e| e.contains("dictionary")),
            "refused"
        );
    }

    /// A filtered view reads on from a point where its rows are known, and counts the
    /// rows after it alone.
    #[test]
    fn a_filtered_view_reads_and_counts_on_from_what_is_known() {
        let mut text = b"t,n\n".to_vec();
        for i in 0..300 {
            text.extend(format!("{i},{}\n", i % 5).as_bytes());
        }
        let options = OpenOptions::default();
        let (_dir, path, lf, marks, rows) = marked(&text, FileFormat::Csv, &options, 16, |p| {
            csv_scan(p, &options)
        });
        let view = lf.filter(col("n").eq(lit(3)));
        let whole = view.clone().collect().unwrap();
        // The view's rows among the first 200 of the file.
        let known = (whole.column("t").unwrap().i64().unwrap().to_vec())
            .into_iter()
            .filter(|t| t.unwrap() < 200)
            .count();
        let rest = from_marks(&view, &path, &marks, 200, None).unwrap();
        let after = rest.collect().unwrap().height();
        assert_eq!(known + after, whole.height());
        assert_eq!(rows, 300);
        let window = Window {
            lf: view.clone(),
            path,
            marks,
            known: Some(vec![(0, 0), (known, 200)]),
        };
        for start in [0, 10, known - 1, known, known + 5, whole.height() - 3] {
            let read = crate::pushdown::Windowed::window(&window, start, 4)
                .unwrap()
                .collect()
                .unwrap();
            assert!(
                read.equals_missing(&whole.slice(start as i64, 4)),
                "{start}"
            );
        }
    }

    /// A file put in place of the followed one, as big or bigger, is another file;
    /// the file grown in place is the same one. Windows reads this from the volume and
    /// file index, Unix from the device and inode.
    #[test]
    fn a_replaced_file_is_told_from_a_grown_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.csv");
        std::fs::write(&path, "t\n1\n").unwrap();
        let held = File::open(&path).unwrap();
        let known = identity_of(&held);
        assert!(cfg!(not(any(unix, windows))) || known.is_some());
        let now = |path: &Path| identity_at(path, &std::fs::metadata(path).unwrap());
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"2\n")
            .unwrap();
        assert!(!replaced(known, now(&path)), "grown in place");
        let other = dir.path().join("next.csv");
        std::fs::write(&other, "t\n1\n2\n3\n").unwrap();
        std::fs::rename(&other, &path).unwrap();
        assert_eq!(
            replaced(known, now(&path)),
            known.is_some(),
            "renamed over it"
        );
        assert!(!replaced(None, now(&path)), "unknown is no replacement");
        drop(held);
    }

    /// A deleted file is read through the handle held on it.
    #[cfg(unix)]
    #[test]
    fn a_deleted_file_reads_through_its_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gone.csv");
        std::fs::write(&path, "a\n1\n2\n").unwrap();
        let mut lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        bound(&mut lf, &path, 2);
        let mut view = lf.filter(col("a").gt(lit(0)));
        view.collect_schema().unwrap();
        let handle = File::open(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        read_through(&mut view, &path, &handle);
        assert_eq!(view.collect().unwrap().height(), 2);
    }
}

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

/// How often the watcher checks the file, unless `[file_loading] follow_interval_ms`
/// says otherwise. A burst of appends inside one interval is one refresh.
pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);

/// Bytes read from the file per step while counting records.
const CHUNK: usize = 1 << 20;

/// A record longer than this is kept only in part: enough to classify it.
const LONGEST_RECORD: usize = 16 << 20;

/// Why `format` cannot be followed, or `None` when it can. Only text read line by
/// line can: a file whose footer is written last (Parquet, Arrow IPC, Excel) cannot be
/// read before it is finished, and a compressed one cannot be read from the middle.
pub fn refusal(format: Option<FileFormat>, options: &OpenOptions) -> Option<String> {
    let format = format.unwrap_or(FileFormat::Csv);
    if !matches!(
        format,
        FileFormat::Csv | FileFormat::Tsv | FileFormat::Psv | FileFormat::Jsonl
    ) {
        return Some(format!(
            "Only CSV, TSV, PSV and NDJSON can be followed as they grow; {} is read once it is finished.",
            format_name(format)
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

fn format_name(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Parquet => "Parquet",
        FileFormat::Arrow => "Arrow IPC",
        FileFormat::Excel => "Excel",
        FileFormat::Json => "a JSON document",
        FileFormat::Avro => "Avro",
        FileFormat::Orc => "ORC",
        FileFormat::Sqlite => "SQLite",
        _ => "this format",
    }
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
    compression.is_none() && refusal(format, options).is_none()
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
        .unwrap_or(FileFormat::Csv)
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
            Fits::Text => value.is_string(),
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
}

impl Tail {
    /// The tail of a file read as `format` with `options`, whose frame has `schema`,
    /// before anything is counted.
    pub fn new(format: FileFormat, options: &OpenOptions, schema: &Schema) -> Tail {
        let layout = match format.separator() {
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
    }

    /// Count the records `file` completes between what was counted and `len`, checking
    /// each new row against the schema when `check`.
    pub fn read_on(&mut self, file: &mut File, len: u64, check: bool) -> std::io::Result<()> {
        if len <= self.complete {
            return Ok(());
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

    /// One complete record, without its newline.
    fn end_record(&mut self, record: &[u8], oversized: bool, check: bool) {
        let record = record.strip_suffix(b"\r").unwrap_or(record);
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
                self.rows += 1;
                if check && (oversized || !self.cells_fit(record, *separator, nulls)) {
                    self.misfits += 1;
                }
            }
            Layout::Lines => {
                if record.iter().all(u8::is_ascii_whitespace) {
                    return;
                }
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

/// Whether `plan` is a scan of `path` and nothing else.
fn scans(plan: &polars::lazy::dsl::DslPlan, path: &str) -> bool {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::Scan {
            sources: ScanSources::Paths(paths),
            ..
        } => paths.len() == 1 && paths[0].as_str() == path,
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
            if let DslPlan::Scan {
                sources, cached_ir, ..
            } = plan
                && let Ok(handle) = file.try_clone()
            {
                *sources = ScanSources::Files(Arc::from([handle]));
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
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl Follow {
    /// Follow the file `tail` counted, whose first `tail.rows()` rows the frame reads,
    /// checking every `interval` and telling `events`. `spool` is standard input being
    /// copied to it.
    pub fn start(
        tail: Tail,
        interval: Duration,
        events: Sender<AppEvent>,
        spool: Option<Arc<SpoolHandle>>,
    ) -> Follow {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = tail.path.clone();
        let shared = Arc::new(Shared::default());
        let shown = tail.rows();
        let watcher = Watcher {
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
        }
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
}

/// Which file a path names, so a replaced file is told from a grown one.
#[cfg(unix)]
fn identity(meta: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn identity(_meta: &std::fs::Metadata) -> Option<(u64, u64)> {
    None
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
        let mut known = file.metadata().ok().and_then(|m| identity(&m));
        let mut sent = (self.tail.rows(), 0usize);
        while self.shared.wait(self.interval) {
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
            let now = identity(&meta);
            let len = meta.len();
            let replaced = now.is_some() && known.is_some() && now != known;
            if replaced || len < self.tail.complete() {
                match File::open(&self.path) {
                    Ok(reopened) => file = reopened,
                    Err(e) => {
                        self.send(Change::Failed(format!("Could not follow the file: {e}")));
                        return;
                    }
                }
                known = now;
                self.tail.restart();
                if let Err(e) = self.tail.read_on(&mut file, len, false) {
                    self.send(Change::Failed(format!("Could not read the file: {e}")));
                    return;
                }
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
/// a temporary one, or the one `--tee` names, which the user keeps.
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
    started: Instant,
}

/// The file `--tee` named.
#[derive(Clone, Debug)]
pub struct Tee {
    pub path: PathBuf,
    /// `--tee-raw`: the bytes exactly as they came, a WAV header's sizes included.
    pub raw: bool,
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
}

/// How far back the rate looks.
const RATE_WINDOW: Duration = Duration::from_secs(2);

impl Spool {
    fn new(sink: File, tee: Option<Tee>) -> Spool {
        Spool {
            stop: AtomicBool::new(false),
            bytes: AtomicU64::new(0),
            state: Mutex::new(SpoolState::default()),
            changed: Condvar::new(),
            sink: Mutex::new(Some(sink)),
            tee,
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
                    .map_or("what came in".to_string(), |t| t.path.display().to_string())
            )
        })?;
        drop(sink);
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
        let mut reason = reason;
        if let (Some(mut file), Some(tee)) = (file, self.tee.as_ref()) {
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
        drop(state);
        self.changed.notify_all();
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
) -> Result<(Spooled, OpenOptions), String> {
    let (reader, _) = open().map_err(|e| format!("Could not read standard input: {e}"))?;
    let tee = options.tee.clone().map(|path| Tee {
        path,
        raw: options.tee_raw,
    });
    let (spooled, file) = match &tee {
        Some(tee) => {
            let file = crate::tee::create(&tee.path, options.force)?;
            (Spooled::Kept(tee.path.clone()), file)
        }
        None => {
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
    let spool = Arc::new(Spool::new(file, tee));
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
        let enough = options.follow
            && (state.lines >= WANTED_LINES || (state.drained && state.lines >= wanted));
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
    let path = match &spooled {
        Spooled::Temp(download) => download.path().to_path_buf(),
        Spooled::Kept(path) => path.clone(),
    };
    let mut head = Vec::new();
    File::open(&path)
        .and_then(|f| f.take(4096).read_to_end(&mut head))
        .map_err(|e| format!("Could not read standard input back: {e}"))?;
    if head.is_empty() {
        return Err("Nothing came in on standard input.".to_string());
    }
    let (format, compression) = crate::stdin::sniff(&head);
    let options = OpenOptions {
        format: options.format.or(Some(format)),
        compression: options.compression.or(compression),
        ..options
    };
    // A recording goes on whatever it holds; only the view is not followed then.
    if options.follow
        && options.tee.is_none()
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

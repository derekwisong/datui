//! Windows deep in a CSV file, read without the rows before them.
//!
//! Polars keeps no row offsets for CSV: a window at row N counts every row before it,
//! so a page deep in a large file costs a pass over most of the file. [`CsvMarks`]
//! records where rows start as windows find them: a mark every chunk counted, and each
//! window's ends. A window reads on from the mark at or before it, so paging costs the
//! rows moved, and no byte is counted twice.
//!
//! A wrong row is never shown for a faster one: only rows provably Polars' own reading.
//! Each row is counted twice, as Polars' parser splits rows (a quote opens a field only
//! at its start) and as Polars' chunker does ([`CountLines`], a quote anywhere), and the
//! two must agree; every window checks that the rows parsed are the rows counted and
//! that its marks agree with the ones before. Any doubt, and the file's windows go
//! back to Polars' own slice for good, erring where it errs. A file read with
//! `--ignore-errors` has no marks: Polars' slice decides what it shows.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use polars::io::csv::read::_csv_read_internal::CountLines;
use polars::io::csv::read::CommentPrefix;
use polars::lazy::dsl::{DslPlan, FileScanDsl, FunctionExpr, ScanSources, StringFunction};
use polars::prelude::*;
use polars_buffer::Buffer;

/// Bytes between the marks a long seek leaves.
#[cfg(not(test))]
const CHUNK: usize = 1 << 20;
/// Small in tests, so a test file is marked many times.
#[cfg(test)]
const CHUNK: usize = 64;

#[cfg(test)]
thread_local! {
    /// Bytes this thread's seeks have counted.
    static COUNTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The name a window read from a mark carries in a plan.
const RUN_NAME: &str = "CSV_RUN";

/// Where the rows of one CSV scan start, as far as windows have counted.
pub(crate) struct CsvMarks {
    /// The scan the marks are of, kept whole: a window replaces this node, found by its
    /// cached conversion, which every clone of the plan shares.
    scan: DslPlan,
    source: Source,
    /// How the scan finds its first row: its own options, with the schema it settled on.
    head: CsvReadOptions,
    /// How a run of rows from a mark is read: see [`crate::loading::follow::run_options`].
    run: CsvReadOptions,
    schema: SchemaRef,
    known: Mutex<Known>,
}

/// What the scan reads.
enum Source {
    Path(PathBuf),
    Buffer(Buffer<u8>),
}

/// What tells one version of a file from another: its length, modification time and
/// inode, and a hash of its first and last few KB.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    modified: Option<SystemTime>,
    inode: u64,
    ends: u64,
}

/// Bytes of each end of a file its [`Stamp`] hashes.
const STAMP_BYTES: usize = 4096;

/// Marks found so far, for the file as it was when they were found.
#[derive(Default)]
struct Known {
    /// The file when marked: another means the marks are of other bytes.
    stamp: Option<Stamp>,
    /// (row, byte where it starts), rows ascending; the first is row 0.
    at: Vec<(usize, usize)>,
    /// A window disagreed with the marks: this version of the file reads through
    /// Polars' slice from now on.
    broken: bool,
}

impl Known {
    /// The last mark at or before `row`.
    fn floor(&self, row: usize) -> Option<(usize, usize)> {
        let i = self.at.partition_point(|&(r, _)| r <= row);
        self.at.get(i.checked_sub(1)?).copied()
    }

    /// Record that `row` starts at `byte`. A mark of the row at another byte, or out of
    /// order with its neighbors, means the count went wrong: the marks break.
    fn mark(&mut self, row: usize, byte: usize) {
        let i = self.at.partition_point(|&(r, _)| r < row);
        match self.at.get(i) {
            Some(&(r, b)) if r == row => self.broken |= b != byte,
            next => {
                let before = i.checked_sub(1).map(|j| self.at[j]);
                let in_order =
                    before.is_none_or(|(_, b)| b < byte) && next.is_none_or(|&(_, b)| byte < b);
                if in_order {
                    self.at.insert(i, (row, byte));
                } else {
                    self.broken = true;
                }
            }
        }
    }
}

impl CsvMarks {
    /// Marks for the one CSV scan `lf` reads: a local file or a buffer, every column,
    /// rows unnumbered. `None` for any other frame.
    pub(crate) fn of(lf: &LazyFrame) -> Option<Arc<CsvMarks>> {
        let mut scans = (&lf.logical_plan)
            .into_iter()
            .filter(|node| matches!(node, DslPlan::Scan { .. }));
        let scan = scans.next()?.clone();
        if scans.next().is_some() {
            return None;
        }
        let DslPlan::Scan {
            sources,
            unified_scan_args: args,
            scan_type,
            ..
        } = &scan
        else {
            return None;
        };
        let FileScanDsl::Csv { options } = &**scan_type else {
            return None;
        };
        // With errors ignored, the rows Polars shows depend on how it reads the file
        // around them; only its own slice gives them.
        if args.row_index.is_some()
            || args.include_file_paths.is_some()
            || args.pre_slice.is_some()
            || options.n_rows.is_some()
            || options.ignore_errors
        {
            return None;
        }
        let source = match sources {
            ScanSources::Paths(paths) if paths.len() == 1 && !args.glob => {
                let path = PathBuf::from(paths[0].as_str());
                if crate::cloud::source::is_remote_url(&path) {
                    return None;
                }
                Source::Path(path)
            }
            ScanSources::Buffers(buffers) if buffers.len() == 1 => {
                Source::Buffer(buffers[0].clone())
            }
            _ => return None,
        };
        // Settled when the dataset opened, and cached in the plan: without it the
        // schema would be inferred from the file here, on the UI thread.
        let DslPlan::Scan { cached_ir, .. } = &scan else {
            return None;
        };
        if !cached_ir.lock().is_ok_and(|ir| ir.is_some()) {
            return None;
        }
        let schema = LazyFrame::from(scan.clone()).collect_schema().ok()?;
        let run = crate::loading::follow::run_options(options, &schema)?;
        let mut head = (**options).clone();
        head.schema = Some(schema.clone());
        Some(Arc::new(CsvMarks {
            scan,
            source,
            head,
            run,
            schema,
            known: Mutex::default(),
        }))
    }

    /// Rows `[start, start + len)` of `lf`, read on from the mark at or before them.
    /// `None` unless `lf` is this scan under steps that keep every row in place: see
    /// [`Self::replace`].
    pub(crate) fn window(
        self: &Arc<Self>,
        lf: &LazyFrame,
        start: usize,
        len: usize,
    ) -> Option<LazyFrame> {
        let run = LazyFrame::anonymous_scan(
            Arc::new(Run {
                marks: self.clone(),
                start,
                len,
            }),
            ScanArgsAnonymous {
                schema: Some(self.schema.clone()),
                name: RUN_NAME,
                ..Default::default()
            },
        )
        .ok()?;
        let mut plan = lf.logical_plan.clone();
        self.replace(&mut plan, &run.logical_plan).then(|| {
            let mut out = lf.clone();
            out.logical_plan = plan;
            out
        })
    }

    /// Put `with` where `plan` reads this scan through row-keeping steps alone: columns
    /// selected, renamed or computed row by row from expressions known to be per-row.
    fn replace(&self, plan: &mut DslPlan, with: &DslPlan) -> bool {
        match plan {
            DslPlan::IR { dsl, .. } => {
                let mut inner = Arc::unwrap_or_clone(dsl.clone());
                let replaced = self.replace(&mut inner, with);
                *plan = inner;
                replaced
            }
            DslPlan::Scan { cached_ir, .. } => {
                let DslPlan::Scan {
                    cached_ir: ours, ..
                } = &self.scan
                else {
                    return false;
                };
                if !Arc::ptr_eq(cached_ir, ours) {
                    return false;
                }
                *plan = with.clone();
                true
            }
            DslPlan::Select { input, expr, .. } if expr.iter().all(per_row) => {
                self.replace(Arc::make_mut(input), with)
            }
            DslPlan::HStack { input, exprs, .. } if exprs.iter().all(per_row) => {
                self.replace(Arc::make_mut(input), with)
            }
            // Polars names its functions by these; it does not export their type.
            DslPlan::MapFunction { input, function }
                if matches!(function.to_string().as_str(), "RENAME" | "FILL_NAN") =>
            {
                self.replace(Arc::make_mut(input), with)
            }
            _ => false,
        }
    }

    /// The bytes the scan reads now, with the marks for them: marks of other bytes (the
    /// file changed) are dropped.
    fn bytes(&self) -> PolarsResult<(Buffer<u8>, std::sync::MutexGuard<'_, Known>)> {
        let (bytes, stamp) = match &self.source {
            Source::Buffer(buffer) => (buffer.clone(), None),
            Source::Path(path) => {
                let file = std::fs::File::open(path)?;
                let meta = file.metadata()?;
                let bytes = if meta.len() == 0 {
                    Buffer::new()
                } else {
                    // SAFETY: read-only, held for the read, as Polars maps the files it
                    // scans; see `crate::formats::fixed_records::Bytes::map`.
                    Buffer::from_owner(unsafe { memmap2::Mmap::map(&file)? })
                };
                let stamp = Stamp {
                    len: meta.len(),
                    modified: meta.modified().ok(),
                    inode: inode(&meta),
                    ends: ends_hash(&bytes),
                };
                (bytes, Some(stamp))
            }
        };
        let mut known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        if known.stamp != stamp || (known.at.is_empty() && !known.broken) {
            *known = Known {
                stamp,
                ..Known::default()
            };
            // Where Polars cannot say, every window reads through it as before.
            if let Ok(Some(first)) = self.first_row(&bytes) {
                known.at.push((0, first));
            }
        }
        Ok((bytes, known))
    }

    /// Where the first row starts, past what the scan skips (header, skipped lines and
    /// rows), as Polars finds it; `None` when Polars says from no place in `bytes`.
    fn first_row(&self, bytes: &Buffer<u8>) -> PolarsResult<Option<usize>> {
        let mut reader =
            polars::io::utils::compression::ByteSourceReader::from_memory(bytes.clone())?;
        let (_, rest) = polars::io::csv::read::streaming::read_until_start_and_infer_schema(
            &self.head,
            None,
            Some(bytes.len()),
            None,
            &mut reader,
        )?;
        if rest.is_empty() {
            return Ok(Some(bytes.len()));
        }
        let at = (rest.as_ptr() as usize).checked_sub(bytes.as_ptr() as usize);
        Ok(at.filter(|&at| at + rest.len() <= bytes.len()))
    }

    /// Rows `[start, start + len)` of `columns` (every column when `None`), parsed as
    /// the scan parses them: from a mark when the marks hold, else through Polars.
    fn read(
        &self,
        start: usize,
        len: usize,
        columns: Option<&[PlSmallStr]>,
    ) -> PolarsResult<DataFrame> {
        match self.read_from_marks(start, len, columns) {
            Some(df) => Ok(df),
            None => self.through_polars(start, len, columns),
        }
    }

    /// The window as Polars reads it without marks: every row before it counted.
    fn through_polars(
        &self,
        start: usize,
        len: usize,
        columns: Option<&[PlSmallStr]>,
    ) -> PolarsResult<DataFrame> {
        let mut lf = LazyFrame::from(self.scan.clone());
        if let Some(columns) = columns {
            lf = lf.select(
                columns
                    .iter()
                    .map(|name| col(name.clone()))
                    .collect::<Vec<_>>(),
            );
        }
        lf.slice(start as i64, len as IdxSize).collect()
    }

    /// The window read from the mark before it, or `None` when the marks cannot be
    /// trusted for it: then they are not trusted again for this version of the file.
    fn read_from_marks(
        &self,
        start: usize,
        len: usize,
        columns: Option<&[PlSmallStr]>,
    ) -> Option<DataFrame> {
        let (bytes, mut known) = self.bytes().ok()?;
        if known.broken {
            return None;
        }
        let begin = known.floor(start)?;
        let counter = Counter::of(&self.run);
        let (row, from) = counter.seek(&bytes, &mut known, begin, start);
        let (end_row, to) = if row < start {
            (row, from)
        } else {
            counter.seek(&bytes, &mut known, (row, from), start + len)
        };
        if known.broken {
            return None;
        }
        let counted = end_row.saturating_sub(start);
        if counted == 0 {
            // Past the last row by the count: an empty read would say where the data
            // ends, which only Polars can say for sure.
            return None;
        }
        // Parsed as Polars parses the file, and checked: rows that do not come out as
        // counted mean the count is not Polars', here or before. A window of no columns
        // (a count) parses the first, to check against.
        let first = self.schema.iter_names().next().cloned();
        let parse_columns = match columns {
            Some([]) => first.as_ref().map(std::slice::from_ref),
            columns => columns,
        };
        match self.parse(&bytes[from..to], parse_columns) {
            Ok(df) if df.height() == counted => match columns {
                Some([]) => Some(DataFrame::empty_with_height(counted)),
                _ => Some(df),
            },
            _ => {
                known.broken = true;
                None
            }
        }
    }

    /// `run`, rows from its first byte, as the scan as loaded parses them: Polars' own
    /// reader with the scan's options. Only `columns` are parsed.
    fn parse(&self, run: &[u8], columns: Option<&[PlSmallStr]>) -> PolarsResult<DataFrame> {
        let mut options = self.run.clone();
        if let Some(columns) = columns {
            options.columns = Some(columns.iter().cloned().collect());
        }
        let df = options
            .into_reader_with_file_handle(std::io::Cursor::new(run))
            .finish()?;
        project(df, columns)
    }
}

/// `df` as `columns`, in their order.
fn project(df: DataFrame, columns: Option<&[PlSmallStr]>) -> PolarsResult<DataFrame> {
    match columns {
        Some(columns) => df.select(columns.iter().cloned()),
        None => Ok(df),
    }
}

/// Whether `expr` computes each row from that row alone: columns, scalar literals,
/// casts, arithmetic and comparisons, conditions, and the functions datui's reads
/// apply to text. Anything else (a shift, a rank, an aggregation, a window) reads
/// other rows, which a window does not hold.
fn per_row(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Literal(value) => value.is_scalar(),
        Expr::Alias(inner, _) | Expr::KeepName(inner) => per_row(inner),
        Expr::Cast { expr, .. } => per_row(expr),
        Expr::BinaryExpr { left, right, .. } => per_row(left) && per_row(right),
        Expr::Ternary {
            predicate,
            truthy,
            falsy,
        } => per_row(predicate) && per_row(truthy) && per_row(falsy),
        // A date parse with no format infers one from the values it is given: from a
        // window's alone, it may infer another than the column's.
        Expr::Function {
            function: FunctionExpr::StringExpr(StringFunction::Strptime(_, options)),
            input,
        } => options.format.is_some() && input.iter().all(per_row),
        Expr::Function { input, function } => {
            PER_ROW_FUNCTIONS.contains(&function.to_string().as_str()) && input.iter().all(per_row)
        }
        _ => false,
    }
}

/// Functions known to compute a row from that row alone, by the names Polars gives
/// them.
const PER_ROW_FUNCTIONS: &[&str] = &[
    "str.strip_chars",
    "str.strip_chars_start",
    "str.strip_chars_end",
    "str.to_integer",
    "str.replace",
    "str.replace_all",
    "str.to_lowercase",
    "str.to_uppercase",
    "str.len_bytes",
    "str.len_chars",
    "str.contains",
    "str.starts_with",
    "str.ends_with",
    "is_null",
    "is_not_null",
    "fill_null",
    "coalesce",
    "abs",
    "round",
    "not",
];

#[cfg(unix)]
fn inode(meta: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(meta)
}

#[cfg(not(unix))]
fn inode(_meta: &std::fs::Metadata) -> u64 {
    0
}

/// A hash of the first and last [`STAMP_BYTES`] of `bytes`.
fn ends_hash(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes[..bytes.len().min(STAMP_BYTES)].hash(&mut hasher);
    bytes[bytes.len().saturating_sub(STAMP_BYTES)..].hash(&mut hasher);
    hasher.finish()
}

/// Counts rows as Polars' parser splits them: a quote opens a field only at the
/// field's start, and closes at the next quote not doubled; a line end outside a
/// quoted field ends the row; with a comment prefix, a line starting with it at a
/// row's start is no row. A blank line is a row.
struct Counter {
    /// Polars' chunker's count, which every row's must match.
    lines: CountLines,
    quote: Option<u8>,
    separator: u8,
    eol: u8,
    comment: Option<Vec<u8>>,
}

impl Counter {
    fn of(options: &CsvReadOptions) -> Counter {
        let parse = &options.parse_options;
        Counter {
            lines: CountLines::new(
                parse.quote_char,
                parse.eol_char,
                parse.comment_prefix.clone(),
            ),
            quote: parse.quote_char,
            separator: parse.separator,
            eol: parse.eol_char,
            comment: parse.comment_prefix.as_ref().map(|prefix| match prefix {
                CommentPrefix::Single(c) => vec![*c],
                CommentPrefix::Multi(s) => s.as_bytes().to_vec(),
            }),
        }
    }

    /// From row `at.0`, which starts at byte `at.1`, on to row `target`: that row and
    /// where it starts, or the last row and the end when the bytes end first. A mark
    /// every [`CHUNK`] bytes passed, and at the end.
    fn seek(
        &self,
        bytes: &[u8],
        known: &mut Known,
        at: (usize, usize),
        target: usize,
    ) -> (usize, usize) {
        let (mut row, mut pos) = at;
        // Where rows were marked, each span between to be counted by Polars' chunker too.
        let mut spans = vec![(row, pos)];
        while row < target && pos < bytes.len() {
            let end = match self.row_end(bytes, pos) {
                RowEnd::At(end) => end,
                // A last row with no line end is a row, unless only comments are left.
                RowEnd::Open if self.only_comments(&bytes[pos..]) => {
                    pos = bytes.len();
                    break;
                }
                RowEnd::Open => bytes.len(),
                // Where the parser and the chunker part, which rows Polars means is not
                // provable: its slice decides.
                RowEnd::Stray => {
                    known.broken = true;
                    return (row, pos);
                }
            };
            #[cfg(test)]
            COUNTED.with(|counted| counted.set(counted.get() + end - pos));
            pos = end;
            row += 1;
            if spans.last().is_some_and(|&(_, at)| pos - at >= CHUNK) && pos < bytes.len() {
                known.mark(row, pos);
                spans.push((row, pos));
            }
        }
        spans.push((row, pos));
        if !self.chunker_agrees_on(bytes, &spans) {
            known.broken = true;
        } else if row == target && pos < bytes.len() {
            known.mark(row, pos);
        }
        (row, pos)
    }

    /// Whether Polars' chunker counts the rows from `from` to `to` (row, byte) as this
    /// count does: as many, the last ending where the count says.
    fn chunker_agrees(&self, bytes: &[u8], from: (usize, usize), to: (usize, usize)) -> bool {
        if to.1 == from.1 {
            return to.0 == from.0;
        }
        let last = to.1 == bytes.len();
        self.lines.count_rows(&bytes[from.1..to.1], last) == (to.0 - from.0, to.1 - from.1)
    }

    /// Whether Polars' chunker agrees on every span between `points` (row, byte). Spans
    /// start at rows, so each is counted alone: a long seek's on several threads.
    fn chunker_agrees_on(&self, bytes: &[u8], points: &[(usize, usize)]) -> bool {
        let spans: Vec<_> = points.windows(2).map(|w| (w[0], w[1])).collect();
        let threads = std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(8);
        if spans.len() < 4 || threads < 2 {
            return spans
                .iter()
                .all(|&(from, to)| self.chunker_agrees(bytes, from, to));
        }
        let per = spans.len().div_ceil(threads);
        std::thread::scope(|scope| {
            let parts: Vec<_> = spans
                .chunks(per)
                .map(|part| {
                    scope.spawn(move || {
                        part.iter()
                            .all(|&(from, to)| self.chunker_agrees(bytes, from, to))
                    })
                })
                .collect();
            parts.into_iter().all(|part| part.join().unwrap_or(false))
        })
    }

    /// Whether `rest` holds comment lines and nothing else.
    fn only_comments(&self, mut rest: &[u8]) -> bool {
        let Some(prefix) = &self.comment else {
            return false;
        };
        while rest.starts_with(prefix) {
            match memchr::memchr(self.eol, rest) {
                Some(n) => rest = &rest[n + 1..],
                None => return true,
            }
        }
        rest.is_empty()
    }

    /// Where the row starting at `pos` ends, past its line end, comment lines before it
    /// passed over.
    fn row_end(&self, bytes: &[u8], pos: usize) -> RowEnd {
        self.find_row_end(bytes, pos).unwrap_or(RowEnd::Open)
    }

    /// [`Self::row_end`]; `None` when the bytes end first.
    fn find_row_end(&self, bytes: &[u8], mut pos: usize) -> Option<RowEnd> {
        if let Some(prefix) = &self.comment {
            while bytes[pos..].starts_with(prefix) {
                pos += memchr::memchr(self.eol, &bytes[pos..])? + 1;
            }
        }
        let quote = self.quote;
        let mut field_start = pos;
        let mut i = pos;
        loop {
            if quote.is_some_and(|q| bytes.get(i) == Some(&q)) && i == field_start {
                // A quoted field: on to its closing quote; a doubled quote is one quote.
                let q = quote.unwrap_or_default();
                i += 1;
                loop {
                    i += memchr::memchr(q, bytes.get(i..)?)?;
                    if bytes.get(i + 1) == Some(&q) {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                }
                continue;
            }
            let rest = bytes.get(i..)?;
            let n = match quote {
                Some(q) => memchr::memchr3(self.separator, self.eol, q, rest)?,
                None => memchr::memchr2(self.separator, self.eol, rest)?,
            };
            i += n;
            let c = bytes[i];
            if c == self.eol {
                return Some(RowEnd::At(i + 1));
            }
            // A quote inside a field opens nothing for the parser, but the chunker
            // takes it to open a quoted run.
            if Some(c) == quote {
                return Some(RowEnd::Stray);
            }
            i += 1;
            if c == self.separator {
                field_start = i;
            }
        }
    }
}

/// Where a row ends, for [`Counter::row_end`].
enum RowEnd {
    /// Past its line end, here.
    At(usize),
    /// The bytes end first.
    Open,
    /// A quote inside a field, where Polars' parser and chunker part.
    Stray,
}

/// A window of rows from a mark, read when the frame is collected.
struct Run {
    marks: Arc<CsvMarks>,
    start: usize,
    len: usize,
}

impl AnonymousScan for Run {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.marks.schema.clone())
    }

    /// The columns a view shows: hidden ones are not parsed, as Polars' scan skips them.
    fn allows_projection_pushdown(&self) -> bool {
        true
    }

    fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let len = args.n_rows.map_or(self.len, |n| n.min(self.len));
        self.marks
            .read(self.start, len, args.with_columns.as_deref())
    }
}

#[cfg(test)]
#[path = "csv_marks_tests.rs"]
mod tests;

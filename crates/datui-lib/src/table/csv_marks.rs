//! Windows deep in a CSV file, read without the rows before them.
//!
//! Polars keeps no row offsets for CSV: a window at row N counts every row before it,
//! so a page deep in a large file costs a pass over most of the file. [`CsvMarks`]
//! records where rows start as windows find them, counted as Polars counts them
//! ([`CountLines`]): a mark every chunk counted, and each window's ends. A window reads
//! on from the mark at or before it, so paging costs the rows moved, and no byte is
//! counted twice.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use polars::io::csv::read::_csv_read_internal::CountLines;
use polars::io::csv::read::CommentPrefix;
use polars::lazy::dsl::{DslPlan, FileScanDsl, ScanSources};
use polars::prelude::*;
use polars_buffer::Buffer;

/// Bytes counted at a step, and between marks: the first step of a seek counts the
/// least, so a page's move counts about a page; each step after counts twice the last,
/// up to the most.
#[cfg(not(test))]
const CHUNK: (usize, usize) = (64 << 10, 1 << 20);
/// Small in tests, so a test file is counted in many chunks.
#[cfg(test)]
const CHUNK: (usize, usize) = (16, 64);

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

/// Marks found so far, for the file as it was when they were found.
#[derive(Default)]
struct Known {
    /// The file's length and modification time when marked: another means the marks
    /// are of other bytes.
    stamp: Option<(u64, Option<SystemTime>)>,
    /// (row, byte where it starts), rows ascending; the first is row 0.
    at: Vec<(usize, usize)>,
}

impl Known {
    /// The last mark at or before `row`.
    fn floor(&self, row: usize) -> Option<(usize, usize)> {
        let i = self.at.partition_point(|&(r, _)| r <= row);
        self.at.get(i.checked_sub(1)?).copied()
    }

    fn mark(&mut self, row: usize, byte: usize) {
        let i = self.at.partition_point(|&(r, _)| r < row);
        if self.at.get(i).is_none_or(|&(r, _)| r != row) {
            self.at.insert(i, (row, byte));
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
        if args.row_index.is_some()
            || args.include_file_paths.is_some()
            || args.pre_slice.is_some()
            || options.n_rows.is_some()
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
        // Settled when the dataset opened: the plan caches it.
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
    /// `None` unless `lf` is this scan under steps that keep every row in place
    /// (selections, new columns, renames), which a pristine view's are.
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

    /// Put `with` where `plan` reads this scan through row-keeping steps alone.
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
            DslPlan::Select { input, .. } | DslPlan::HStack { input, .. } => {
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
                let stamp = Some((meta.len(), meta.modified().ok()));
                let bytes = if meta.len() == 0 {
                    Buffer::new()
                } else {
                    // SAFETY: read-only, held for the read, as Polars maps the files it
                    // scans; see `crate::formats::fixed_records::Bytes::map`.
                    Buffer::from_owner(unsafe { memmap2::Mmap::map(&file)? })
                };
                (bytes, stamp)
            }
        };
        let mut known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        if known.stamp != stamp || known.at.is_empty() {
            known.at.clear();
            known.stamp = stamp;
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

    /// Rows `[start, start + len)`, parsed as the scan parses them.
    fn read(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        let (bytes, mut known) = self.bytes()?;
        let Some(begin) = known.floor(start) else {
            drop(known);
            // The first row could not be placed: Polars counts from the start.
            return LazyFrame::from(self.scan.clone())
                .slice(start as i64, len as IdxSize)
                .collect();
        };
        let counter = Counter::of(&self.run);
        let (row, from) = counter.seek(&bytes, &mut known, begin, start);
        let to = if row < start {
            from
        } else {
            counter.seek(&bytes, &mut known, (row, from), start + len).1
        };
        drop(known);
        if from >= to {
            return Ok(DataFrame::empty_with_schema(&self.schema));
        }
        Ok(self
            .scan_of(bytes.sliced(from..to))?
            .collect()?
            .slice(0, len))
    }

    /// The scan, reading `run` as rows from its first byte: Polars' own reader, as the
    /// scan as loaded reads.
    fn scan_of(&self, run: Buffer<u8>) -> PolarsResult<LazyFrame> {
        let DslPlan::Scan {
            unified_scan_args, ..
        } = &self.scan
        else {
            polars_bail!(ComputeError: "a CSV window without its scan");
        };
        Ok(LazyFrame::from(DslPlan::Scan {
            sources: ScanSources::Buffers(Arc::from([run])),
            unified_scan_args: unified_scan_args.clone(),
            scan_type: Box::new(FileScanDsl::Csv {
                options: Arc::new(self.run.clone()),
            }),
            cached_ir: Default::default(),
        }))
    }
}

/// Counts rows as Polars does: quoted line ends are not row ends, and with a comment
/// prefix, comment lines are not rows.
struct Counter {
    lines: CountLines,
    quote: Option<u8>,
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
            eol: parse.eol_char,
            comment: parse.comment_prefix.as_ref().map(|prefix| match prefix {
                CommentPrefix::Single(c) => vec![*c],
                CommentPrefix::Multi(s) => s.as_bytes().to_vec(),
            }),
        }
    }

    /// From row `at.0`, which starts at byte `at.1`, on to row `target`: that row and
    /// where it starts, or the last row and the end when the bytes end first. Whole
    /// chunks are counted at once and marked; the rest a row at a time.
    fn seek(
        &self,
        bytes: &[u8],
        known: &mut Known,
        at: (usize, usize),
        target: usize,
    ) -> (usize, usize) {
        let (mut row, mut pos) = at;
        let mut size = CHUNK.0;
        while row < target && pos < bytes.len() {
            let end = (pos + size).min(bytes.len());
            let last = end == bytes.len();
            #[cfg(test)]
            COUNTED.with(|counted| counted.set(counted.get() + end - pos));
            let (rows, used) = self.lines.count_rows(&bytes[pos..end], last);
            if rows == 0 {
                if last {
                    break;
                }
                // A row longer than the chunk.
                size = size.saturating_mul(2);
                continue;
            }
            if row + rows > target {
                break;
            }
            row += rows;
            pos += used;
            if pos < bytes.len() {
                known.mark(row, pos);
            }
            size = (size * 2).min(CHUNK.1.max(size));
        }
        while row < target && pos < bytes.len() {
            // A last row with no line end is a row too.
            let end = self.row_end(bytes, pos).unwrap_or(bytes.len());
            #[cfg(test)]
            COUNTED.with(|counted| counted.set(counted.get() + end - pos));
            pos = end;
            row += 1;
        }
        if row == target && pos < bytes.len() {
            known.mark(row, pos);
        }
        (row, pos)
    }

    /// Where the row starting at `pos` ends, past its line end, comment lines before it
    /// passed over as [`CountLines`] passes them; `None` when the bytes end first.
    fn row_end(&self, bytes: &[u8], mut pos: usize) -> Option<usize> {
        if let Some(prefix) = &self.comment {
            while bytes[pos..].starts_with(prefix) {
                pos += prefix.len() + memchr::memchr(self.eol, &bytes[pos + prefix.len()..])? + 1;
            }
        }
        let mut quoted = false;
        for (i, &c) in bytes[pos..].iter().enumerate() {
            if Some(c) == self.quote {
                quoted = !quoted;
            } else if c == self.eol && !quoted {
                return Some(pos + i + 1);
            }
        }
        None
    }
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

    fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let len = args.n_rows.map_or(self.len, |n| n.min(self.len));
        self.marks.read(self.start, len)
    }
}

#[cfg(test)]
#[path = "csv_marks_tests.rs"]
mod tests;

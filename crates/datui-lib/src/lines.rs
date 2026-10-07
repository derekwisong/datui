//! Text read as it stands: a row per line, in a `line` column.
//!
//! One pass records where each line starts ([`LineIndex`]); a line is then read from a
//! map of the file where it is shown, so only the rows a view reaches are decoded.
//! Every line is a row, blank ones included, and each row carries its place in the
//! file ([`crate::row_index::INDEX`]), so `#` numbers it as `less -N` does through any
//! sort or filter. A `\r\n` ending is a line ending; bytes that are not UTF-8 are shown
//! as `�` and counted in a note. Control characters stay in the value: the screen
//! escapes them when it draws ([`crate::sanitize`]).
//!
//! A large file shows its first rows once its first [`FIRST_BYTES`] are indexed; the
//! rest is indexed behind them ([`Lines::index_more`]), and the frame's height moves
//! as it goes (`bound`).
//!
//! A followed file's lines are counted by the watcher ([`crate::follow`]), which moves
//! the frame's height (`bound`); the index reads on from its last whole line when a
//! row past it is asked for.
//!
//! [`guess`] says what text no format's signature claims is: JSON, CSV or TSV only on
//! real evidence, lines otherwise.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use color_eyre::Result;
use polars::prelude::*;

use crate::FileFormat;
use crate::fixed_records::Bytes;
use crate::indexed::Offsets;

/// The columns: the file a line is from (several files only), and the line.
pub const FILE: &str = "file";
pub const LINE: &str = "line";

/// Bytes an open indexes before it shows the first rows. Past this the rest of a file
/// is indexed in the background.
pub const FIRST_BYTES: usize = 8 << 20;

/// Bytes checked as UTF-8 at once, as the newlines in them are found: a chunk is
/// still in cache when its lines are indexed, and the file is read once.
const CHUNK: usize = 4 << 20;

/// What datui does with text read as lines: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    preview: Some(crate::readers::Preview::Scan),
    python: Some(crate::python_script::Python {
        call: "pl.LazyFrame",
        eager: false,
        glob_flag: false,
        arguments: Some(crate::python_script::lines_arguments),
    }),
    export: Some(crate::export_modal::ExportFormat::Csv),
    ..crate::readers::BASE
};

/// The columns of one file's lines, or several files' with the file named.
pub fn schema(several: bool) -> Schema {
    let mut fields = Vec::with_capacity(2);
    if several {
        fields.push(Field::new(FILE.into(), DataType::String));
    }
    fields.push(Field::new(LINE.into(), DataType::String));
    Schema::from_iter(fields)
}

/// Where each line of some bytes starts, from one pass. Read on from where it left
/// off as the bytes grow: a last line with no newline yet is read again.
#[derive(Debug, Clone, Default)]
pub struct LineIndex {
    offsets: Offsets,
    /// Bytes through the last newline.
    end: usize,
    /// The last line has no newline: whether it was indexed, and whether it was
    /// counted as not UTF-8, so a read on from `end` takes it back first.
    partial: Option<(bool, bool)>,
    /// Lines that are not valid UTF-8.
    pub invalid: usize,
    /// Lines past `limits.indexed_records`, left out.
    pub past_limit: usize,
    /// Lines indexed before this index begins: a step taken apart from the index it
    /// goes on ([`Self::step_after`]) counts them toward the limit.
    before: usize,
}

impl LineIndex {
    /// The lines of `bytes`, all of them, the last whether or not it ends in a newline.
    pub fn of(bytes: &[u8]) -> Self {
        let mut index = Self {
            offsets: Offsets::for_file(bytes.len()),
            ..Default::default()
        };
        index.extend(bytes);
        index
    }

    /// Lines indexed.
    pub fn lines(&self) -> usize {
        self.offsets.len()
    }

    /// Lines that end in a newline.
    pub fn complete(&self) -> usize {
        self.lines() - usize::from(self.partial.is_some_and(|(indexed, _)| indexed))
    }

    /// Bytes through the last newline.
    pub fn end(&self) -> usize {
        self.end
    }

    /// Whether every byte of `bytes` is indexed.
    pub fn whole(&self, bytes: &[u8]) -> bool {
        self.end >= bytes.len() || self.partial.is_some()
    }

    /// Index what `bytes`, the same bytes as before and maybe more, hold past the
    /// last newline.
    pub fn extend(&mut self, bytes: &[u8]) {
        self.extend_by(bytes, usize::MAX);
    }

    /// [`Self::extend`] through `budget` more bytes at least: to the end of the line
    /// that passes it. Returns whether every byte is indexed.
    pub fn extend_by(&mut self, bytes: &[u8], budget: usize) -> bool {
        if let Some((indexed, invalid)) = self.partial.take() {
            if indexed {
                self.offsets.truncate(self.offsets.len() - 1);
            } else {
                self.past_limit -= 1;
            }
            self.invalid -= usize::from(invalid);
        }
        if bytes.len() <= self.end {
            return true;
        }
        self.offsets.widen_for(bytes.len());
        let stop = self.end.saturating_add(budget).min(bytes.len());
        while self.end < stop && self.partial.is_none() {
            // A chunk ends at a line's end, so each line is checked whole.
            let cut = (self.end + CHUNK).min(stop);
            let to = if cut < bytes.len() {
                memchr::memchr(b'\n', &bytes[cut - 1..]).map_or(bytes.len(), |i| cut + i)
            } else {
                cut
            };
            self.index_chunk(bytes, to);
        }
        self.whole(bytes)
    }

    /// Index the lines from `end` to `to`: the end of `bytes`, or just past a newline.
    fn index_chunk(&mut self, bytes: &[u8], to: usize) {
        // Checked once for the chunk; line by line only when that fails.
        let all_utf8 = std::str::from_utf8(&bytes[self.end..to]).is_ok();
        let limit = crate::limits::get().indexed_records;
        let mut at = self.end;
        while at < to {
            let newline = memchr::memchr(b'\n', &bytes[at..to]).map(|i| at + i);
            let end = newline.unwrap_or(to);
            let indexed = self.before + self.offsets.len() < limit;
            if indexed {
                self.offsets.push(at);
            } else {
                self.past_limit += 1;
            }
            let invalid = !all_utf8 && std::str::from_utf8(&bytes[at..end]).is_err();
            self.invalid += usize::from(invalid);
            match newline {
                Some(newline) => {
                    at = newline + 1;
                    self.end = at;
                }
                None => {
                    self.partial = Some((indexed, invalid));
                    at = to;
                }
            }
        }
    }

    /// An empty index that goes on from this one's last whole line, for a step taken
    /// without holding this one: [`Self::take_step`] appends it. Not for an index with
    /// a last line still open, which only a whole read leaves.
    fn step_after(&self, len: usize) -> Self {
        Self {
            offsets: Offsets::for_file(len),
            end: self.end,
            before: self.before + self.offsets.len(),
            ..Default::default()
        }
    }

    /// Append `step`, taken from [`Self::step_after`] over the same bytes.
    fn take_step(&mut self, step: LineIndex) {
        debug_assert_eq!(step.before, self.offsets.len());
        self.offsets.append(&step.offsets);
        self.end = step.end;
        self.partial = step.partial;
        self.invalid += step.invalid;
        self.past_limit += step.past_limit;
    }

    /// Line `i` of `bytes`, without its newline or a `\r` before it.
    fn line<'a>(&self, bytes: &'a [u8], i: usize) -> &'a [u8] {
        let start = self.offsets.get(i);
        let end = if i + 1 < self.offsets.len() {
            self.offsets.get(i + 1) - 1
        } else {
            memchr::memchr(b'\n', &bytes[start..]).map_or(bytes.len(), |n| start + n)
        };
        let line = &bytes[start..end.min(bytes.len())];
        line.strip_suffix(b"\r").unwrap_or(line)
    }
}

/// One file's bytes and their lines.
struct Mapped {
    bytes: Arc<Bytes>,
    index: LineIndex,
}

struct LineFile {
    /// The name shown in the `file` column.
    name: String,
    /// The file, when it is followed and its lines are read on as it grows.
    grows: Option<PathBuf>,
    mapped: RwLock<Mapped>,
}

impl LineFile {
    fn rows(&self) -> usize {
        self.mapped
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .index
            .lines()
    }

    /// Map the file again and index on from the last newline, or from the start when
    /// it is shorter than what was indexed: it was truncated or replaced.
    fn grow(&self) -> PolarsResult<()> {
        let Some(path) = &self.grows else {
            return Ok(());
        };
        let mut mapped = self.mapped.write().unwrap_or_else(|e| e.into_inner());
        // By name while there is one; a deleted file is read through the handle on it.
        let bytes = match Bytes::map(path) {
            Ok(bytes) => bytes,
            Err(_) => match mapped.bytes.as_ref() {
                Bytes::Mapped(_, file) => remap(file)?,
                Bytes::Owned(_) => return Ok(()),
            },
        };
        if bytes.len() < mapped.index.end() {
            mapped.index = LineIndex::default();
        }
        mapped.index.extend(bytes.as_slice());
        mapped.bytes = Arc::new(bytes);
        Ok(())
    }
}

fn remap(file: &std::fs::File) -> PolarsResult<Bytes> {
    let file = file.try_clone()?;
    if file.metadata()?.len() == 0 {
        return Ok(Bytes::Owned(Vec::new()));
    }
    // SAFETY: as `Bytes::map`: read-only, and a shorter file is indexed again.
    let map = unsafe { memmap2::Mmap::map(&file)? };
    Ok(Bytes::Mapped(map, file))
}

/// The lines of one file or several, read where they are shown.
pub struct Lines {
    files: Vec<LineFile>,
    schema: SchemaRef,
    /// The one file's lines are still being indexed, behind the first rows, and what
    /// a read of every line waits on until they are.
    indexing: (std::sync::Mutex<bool>, std::sync::Condvar),
    /// One indexing step at a time: a paused indexing taken up again may start its
    /// thread while the last one finishes its step.
    stepping: std::sync::Mutex<()>,
    /// The file came back shorter than it was mapped while it was indexed.
    shrank: std::sync::atomic::AtomicBool,
}

/// What a read of every line says when the file shrank as it was indexed.
pub const SHRANK: &str = "the file shrank while it was indexed; open it again to read it";

impl Lines {
    /// `bytes` read as lines; `name` is what the `file` column says when there are
    /// several.
    pub fn from_bytes(parts: Vec<(String, Arc<Bytes>)>) -> Self {
        Self::from_bytes_first(parts, usize::MAX)
    }

    /// [`Self::from_bytes`], one file's first `first` bytes indexed and the rest left
    /// to [`Self::index_more`]. Several files are indexed whole: a row's place in one
    /// would move as the file before it was indexed.
    pub fn from_bytes_first(parts: Vec<(String, Arc<Bytes>)>, first: usize) -> Self {
        let several = parts.len() > 1;
        let mut indexing = false;
        let files = parts
            .into_iter()
            .map(|(name, bytes)| {
                let mut index = LineIndex {
                    offsets: Offsets::for_file(bytes.len()),
                    ..Default::default()
                };
                let budget = if several { usize::MAX } else { first };
                indexing |= !index.extend_by(bytes.as_slice(), budget);
                LineFile {
                    name,
                    grows: None,
                    mapped: RwLock::new(Mapped { index, bytes }),
                }
            })
            .collect();
        Self {
            files,
            schema: Arc::new(schema(several)),
            indexing: (indexing.into(), Default::default()),
            stepping: Default::default(),
            shrank: Default::default(),
        }
    }

    /// Whether every line of every file is indexed.
    pub fn whole(&self) -> bool {
        self.files.iter().all(|f| {
            let m = f.mapped.read().unwrap_or_else(|e| e.into_inner());
            m.index.whole(m.bytes.as_slice())
        })
    }

    /// Whether the file came back shorter than it was mapped while it was indexed.
    pub fn shrank(&self) -> bool {
        self.shrank.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Indexing set aside ([`Self::stop_indexing`] was not called: a pause leaves
    /// the reads waiting) is taken up again: whether there is more to index.
    pub fn resume_indexing(&self) -> bool {
        let more = !self.whole() && !self.shrank();
        *self.indexing.0.lock().unwrap_or_else(|e| e.into_inner()) = more;
        more
    }

    /// Which line of its own file the row at `place` is, from 0: the row's place in
    /// the table, for one file.
    pub fn line_in_file(&self, place: usize) -> Option<usize> {
        let mut start = 0;
        for f in &self.files {
            let rows = f.rows();
            if place < start + rows {
                return Some(place - start);
            }
            start += rows;
        }
        None
    }

    /// Whether the rows are of several files, each numbered on its own.
    pub fn several(&self) -> bool {
        self.files.len() > 1
    }

    /// Whether lines are still being indexed behind the first rows.
    pub fn indexing(&self) -> bool {
        *self.indexing.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// No more lines will be indexed: all of them are, or the indexing stopped (the
    /// dataset went). Whatever waits on them goes on with what there is.
    pub fn stop_indexing(&self) {
        *self.indexing.0.lock().unwrap_or_else(|e| e.into_inner()) = false;
        self.indexing.1.notify_all();
    }

    /// Wait until every line is indexed, on a worker, never the UI thread: `Err` when
    /// they will not all be (the indexing stopped, or the file shrank), or the job
    /// waiting was superseded and its answer is not wanted.
    fn wait_indexed(&self) -> PolarsResult<()> {
        let mut indexing = self.indexing.0.lock().unwrap_or_else(|e| e.into_inner());
        while *indexing {
            polars_ensure!(
                !crate::jobs::superseded(),
                ComputeError: "the read is no longer wanted"
            );
            indexing = self
                .indexing
                .1
                .wait_timeout(indexing, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        drop(indexing);
        polars_ensure!(!self.shrank(), ComputeError: "{SHRANK}");
        polars_ensure!(
            self.whole(),
            ComputeError: "the file's lines were not all indexed; open it again"
        );
        Ok(())
    }

    /// Index at least `budget` more bytes of a file indexed in part. Returns whether
    /// every line is indexed. Holds the lines' write lock for the step only, so rows
    /// are read between steps.
    pub fn index_more(&self, budget: usize) -> bool {
        if !self.indexing() {
            return true;
        }
        let _step = self.stepping.lock().unwrap_or_else(|e| e.into_inner());
        let whole = self.files.iter().all(|f| {
            // Read with no lock held, so rows go on being read meanwhile, then taken
            // in under the write lock: this is the only writer of a file not followed.
            let (bytes, mut step) = {
                let mapped = f.mapped.read().unwrap_or_else(|e| e.into_inner());
                if mapped.index.whole(mapped.bytes.as_slice()) {
                    return true;
                }
                // A file cut short meanwhile is not read past its end, and the lines
                // so far are not taken for all of them. Checked before every step: a
                // truncation inside one step can still fault the map (SIGBUS), which
                // only a copy of the file would rule out.
                if mapped.bytes.still_whole().is_err() {
                    self.shrank
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return true;
                }
                let bytes = mapped.bytes.clone();
                let step = mapped.index.step_after(bytes.len());
                (bytes, step)
            };
            let whole = step.extend_by(bytes.as_slice(), budget);
            f.mapped
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .index
                .take_step(step);
            whole
        });
        if whole {
            self.stop_indexing();
        }
        whole
    }

    /// Bytes indexed, and the bytes there are.
    pub fn indexed_bytes(&self) -> (u64, u64) {
        self.files.iter().fold((0, 0), |(done, all), f| {
            let m = f.mapped.read().unwrap_or_else(|e| e.into_inner());
            let len = m.bytes.as_slice().len();
            let indexed = if m.index.whole(m.bytes.as_slice()) {
                len
            } else {
                m.index.end()
            };
            (done + indexed as u64, all + len as u64)
        })
    }

    /// The files at `paths`, mapped and indexed. `follow` reads one on as it grows.
    pub fn open(paths: &[PathBuf], follow: bool) -> Result<Self> {
        Self::open_first(paths, follow, usize::MAX)
    }

    /// [`Self::open`], a file not followed indexed through its first `first` bytes.
    pub fn open_first(paths: &[PathBuf], follow: bool, first: usize) -> Result<Self> {
        let parts = paths
            .iter()
            .map(|path| {
                let bytes =
                    Bytes::map(path).map_err(|e| crate::error_display::in_file(path, e.into()))?;
                Ok((file_name(path), Arc::new(bytes)))
            })
            .collect::<Result<Vec<_>>>()?;
        let first = if follow { usize::MAX } else { first };
        let mut lines = Self::from_bytes_first(parts, first);
        if follow && let ([file], [path]) = (lines.files.as_mut_slice(), paths) {
            file.grows = Some(path.clone());
        }
        Ok(lines)
    }

    /// Whether lines are read on as the file grows.
    pub fn grows(&self) -> bool {
        self.files.iter().any(|f| f.grows.is_some())
    }

    /// Rows on hand.
    pub fn rows(&self) -> usize {
        self.files
            .iter()
            .map(LineFile::rows)
            .sum::<usize>()
            .min(crate::row_index::MAX_ROWS)
    }

    /// Rows of lines that end in a newline.
    fn complete_rows(&self) -> usize {
        self.files
            .iter()
            .map(|f| {
                f.mapped
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .index
                    .complete()
            })
            .sum()
    }

    /// What the files hold that the rows do not say as they are: lines not UTF-8,
    /// lines left out.
    fn counts(&self) -> (usize, usize) {
        self.files.iter().fold((0, 0), |(invalid, past), f| {
            let m = f.mapped.read().unwrap_or_else(|e| e.into_inner());
            (invalid + m.index.invalid, past + m.index.past_limit)
        })
    }

    /// The frame: a row index decoded a column at a time. A followed file's frame has
    /// the rows of its complete lines, moved as it grows (`bound`).
    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        if self.indexing() {
            // Every line, once they are all indexed: the frame's height is taken when
            // it runs, on a worker, which waits for the indexing. The first rows come
            // from the window ([`opened`]), which does not wait.
            let lines = self.clone();
            let height = DataFrame::empty_with_height(0).lazy().map(
                move |_| {
                    lines.wait_indexed()?;
                    Ok(DataFrame::empty_with_height(lines.rows()))
                },
                AllowedOptimizations::empty(),
                None,
                Some("every line"),
            );
            return crate::row_index::lazy_numbered_over(self, height);
        }
        let height = if self.grows() {
            self.complete_rows()
        } else {
            self.rows()
        };
        crate::row_index::lazy_numbered(self, height)
    }

    /// For each row, its file and the line in it, in order.
    fn place(&self, rows: &[IdxSize]) -> PolarsResult<Vec<(usize, usize)>> {
        let mut starts = Vec::with_capacity(self.files.len());
        let mut total = 0usize;
        for f in &self.files {
            starts.push(total);
            total += f.rows();
        }
        rows.iter()
            .map(|&r| {
                let r = r as usize;
                polars_ensure!(r < total, OutOfBounds: "row {r} is past the {total} lines on hand");
                let file = starts.partition_point(|&s| s <= r) - 1;
                Ok((file, r - starts[file]))
            })
            .collect()
    }

    fn column(&self, column: usize, rows: &[IdxSize]) -> PolarsResult<Column> {
        // A followed file is read on when a row is past its last whole line.
        if self.grows()
            && let Some(max) = rows.iter().max()
            && *max as usize >= self.complete_rows()
        {
            for f in &self.files {
                f.grow()?;
            }
        }
        let placed = self.place(rows)?;
        let several = self.files.len() > 1;
        let name = self
            .schema
            .get_at_index(column)
            .expect("a column")
            .0
            .clone();
        let column = if several { column } else { column + 1 };
        let series = match column {
            0 => placed
                .iter()
                .map(|&(f, _)| Some(self.files[f].name.as_str()))
                .collect::<StringChunked>()
                .into_series(),
            _ => {
                let held: Vec<_> = self
                    .files
                    .iter()
                    .map(|f| f.mapped.read().unwrap_or_else(|e| e.into_inner()))
                    .collect();
                if !self.grows() {
                    for m in &held {
                        m.bytes.still_whole()?;
                    }
                }
                let mut builder = StringChunkedBuilder::new(name.clone(), placed.len());
                for &(f, i) in &placed {
                    let m = &held[f];
                    let bytes = m.bytes.as_slice();
                    match std::str::from_utf8(m.index.line(bytes, i)) {
                        Ok(text) => builder.append_value(text),
                        Err(_) => builder
                            .append_value(String::from_utf8_lossy(m.index.line(bytes, i)).as_ref()),
                    }
                }
                builder.finish().into_series()
            }
        };
        Ok(series.with_name(name).into_column())
    }

    /// Rows `[start, start + len)`, decoded now.
    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        let rows = self.rows();
        let start = start.min(rows);
        let len = len.min(rows - start);
        let index: Vec<IdxSize> = (start..start + len).map(|r| r as IdxSize).collect();
        let mut columns = (0..self.schema.len())
            .map(|c| self.column(c, &index))
            .collect::<PolarsResult<Vec<_>>>()?;
        // Each row's place, as the frame carries it.
        columns.push(IdxCa::from_vec(crate::row_index::INDEX.into(), index).into_column());
        DataFrame::new(len, columns)
    }
}

impl crate::row_index::RowSource for Lines {
    fn height(&self) -> usize {
        self.rows()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        polars_ensure!(
            index.null_count() == 0,
            ComputeError: "a row index has a missing row"
        );
        let rows: Vec<IdxSize> = index.into_no_null_iter().collect();
        self.column(column, &rows)
    }
}

impl crate::pushdown::Windowed for Lines {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(self.collect_window(start, len)?.lazy())
    }
}

fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}

/// Moves the height of a followed file's lines in `plan` to `rows`, and says whether
/// it found them. The frame is a row index over a frame of no columns
/// ([`crate::row_index::lazy_with_height`]), so its height is the rows it has.
pub(crate) fn bound(plan: &mut polars::lazy::dsl::DslPlan, rows: IdxSize) -> bool {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::DataFrameScan { df, .. } if df.width() == 0 => {
            *df = Arc::new(DataFrame::empty_with_height(rows as usize));
            true
        }
        _ => false,
    }
}

/// The lines of `input`, for its reader.
fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let lines = Arc::new(Lines::open_first(
        input.paths,
        input.options.follow,
        FIRST_BYTES,
    )?);
    let lf = lines.lazy();
    input.report.opened = Some(Arc::new(opened(&lines, input.options)));
    Ok(lf.into())
}

/// What the Info panel says of lines read, the window a view reads straight from them
/// when it can, and the lines themselves while they are still being indexed.
pub(crate) fn opened(lines: &Arc<Lines>, options: &crate::OpenOptions) -> crate::members::Opened {
    crate::members::Opened {
        // A followed file's rows are the watcher's to count.
        window: (!lines.grows()).then(|| {
            (
                lines.clone() as Arc<dyn crate::pushdown::Windowed>,
                lines.rows(),
            )
        }),
        notes: notes(lines, options.format_guessed),
        indexing: lines.indexing().then(|| lines.clone()),
        numbering: lines.several().then(|| lines.clone()),
        ..Default::default()
    }
}

/// How the note that the format was guessed begins.
pub(crate) const GUESSED: &str = "no format detected";

/// What the Info panel says of lines read, as far as they are indexed.
pub(crate) fn notes(lines: &Lines, format_guessed: bool) -> Vec<crate::notes::Note> {
    let (invalid, past_limit) = lines.counts();
    let scope = match lines.files.len() {
        1 => "the file".to_string(),
        n => format!("the {n} files"),
    };
    let middot = crate::glyphs::get().middot;
    let mut notes = vec![format!(
        "read as lines {middot} a row per line, blank lines included"
    )];
    if format_guessed {
        notes.push(format!("{GUESSED} {middot} --format csv reads it as CSV"));
    }
    if invalid > 0 {
        notes.push(format!(
            "{} with invalid UTF-8 {middot} shown as \u{fffd}",
            crate::text_formats::count(invalid as u64, "line", "lines")
        ));
    }
    if past_limit > 0 {
        notes.push(crate::limits::left_out(
            &crate::text_formats::count(past_limit as u64, "line", "lines"),
            crate::limits::get().indexed_records,
            "indexed_records",
        ));
    }
    notes
        .into_iter()
        .map(|n| crate::text_formats::note(n, scope.clone()))
        .collect()
}

// --- What text is -----------------------------------------------------------------

/// Complete records looked at for a field count.
const MOST_RECORDS: usize = 50;

/// What text no format's signature claims is, from its first bytes `head`; `whole`
/// when they are all there is, so a last line without a newline is complete. `None`
/// for bytes that are not text, which a local file shows in the hex view.
///
/// JSON only when the bytes parse as JSON so far; NDJSON when the first line is an
/// object. CSV or TSV only when several complete records have one field count, two or
/// more, with quotes where CSV allows them. Anything else is lines.
pub fn guess(head: &[u8], whole: bool) -> Option<FileFormat> {
    let text = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    if !is_text(text, whole) {
        return None;
    }
    let start = text
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(text.len());
    let trimmed = &text[start..];
    match trimmed.first() {
        Some(b'[') if json_so_far(trimmed, whole) => return Some(FileFormat::Json),
        Some(b'{') => {
            let first = trimmed.split(|&b| b == b'\n').next().unwrap_or_default();
            let line_whole = first.len() < trimmed.len() || whole;
            if line_whole
                && matches!(
                    serde_json::from_slice::<serde_json::Value>(first),
                    Ok(serde_json::Value::Object(_))
                )
            {
                return Some(FileFormat::Jsonl);
            }
            if json_so_far(trimmed, whole) {
                return Some(FileFormat::Json);
            }
        }
        _ => {}
    }
    let mut best: Option<(FileFormat, usize)> = None;
    for (separator, format) in [(b',', FileFormat::Csv), (b'\t', FileFormat::Tsv)] {
        if let Some(fields) = consistent_fields(text, separator, whole)
            && best.is_none_or(|(_, most)| fields > most)
        {
            best = Some((format, fields));
        }
    }
    Some(best.map_or(FileFormat::Text, |(format, _)| format))
}

/// A guess of lines turned to CSV when the user gave a delimited dialect
/// (`--delimiter`, `--comment`, `--header-rows`, `--skip-initial-space`): they
/// said the text is delimited, in a shape the guess does not try.
pub fn as_asked(format: FileFormat, options: &crate::OpenOptions) -> FileFormat {
    let delimited = options.delimiter.is_some()
        || options.comment_char.is_some()
        || options.header_rows().is_some()
        || options.skip_initial_space;
    match format {
        FileFormat::Text if delimited => FileFormat::Csv,
        other => other,
    }
}

/// [`guess`] for the local file at `path`, through its compression. What is inside a
/// compressed file is read through it only as delimited text or lines.
pub fn guess_file(
    path: &Path,
    compression: Option<crate::CompressionFormat>,
) -> Option<FileFormat> {
    let compression = compression.or_else(|| crate::CompressionFormat::from_extension(path));
    let reach = crate::readers::HEAD as u64;
    let head = crate::formats::head_of(path, compression, reach)?;
    // An empty file holds no lines to show; the hex view says it is empty.
    if head.is_empty() {
        return None;
    }
    let whole = match compression {
        Some(_) => (head.len() as u64) < reach,
        None => std::fs::metadata(path).is_ok_and(|m| m.len() <= head.len() as u64),
    };
    guess(&head, whole).map(|f| match compression {
        Some(_) if !f.decompressed_once() => FileFormat::Text,
        _ => f,
    })
}

/// Whether `bytes` read as text: no NUL, and at most one byte in ten a control
/// character or not UTF-8. A character cut off at the end of a head is not counted.
fn is_text(bytes: &[u8], whole: bool) -> bool {
    if bytes.contains(&0) {
        return false;
    }
    let mut odd = 0usize;
    let mut chunks = bytes.utf8_chunks().peekable();
    while let Some(chunk) = chunks.next() {
        odd += chunk
            .valid()
            .chars()
            .filter(|c| {
                c.is_control() && !matches!(c, '\t' | '\n' | '\r' | '\x0c' | '\x1b' | '\x08')
            })
            .count();
        let cut_off = !whole && chunks.peek().is_none();
        if !cut_off {
            odd += chunk.invalid().len();
        }
    }
    odd * 10 <= bytes.len()
}

/// Whether `bytes` are JSON, or the start of it when they are not `whole`.
fn json_so_far(bytes: &[u8], whole: bool) -> bool {
    match serde_json::from_slice::<serde::de::IgnoredAny>(bytes) {
        Ok(_) => true,
        Err(e) => !whole && e.is_eof(),
    }
}

/// The field count every complete record of `text` has when split on `separator`,
/// quotes respected, if they agree, have two fields or more, and are enough records to
/// say so: three, or two when that is all there is. Two fields split at a comma that
/// is always followed by a space is prose (`ok, then`), not evidence.
fn consistent_fields(text: &[u8], separator: u8, whole: bool) -> Option<usize> {
    let (mut counts, last, spaced) = field_counts(text, separator)?;
    // A last line with no newline is a record of a whole input when it agrees: a
    // stream caught mid-line is not refused for it.
    if whole
        && let Some(last) = last
        && counts.first().is_none_or(|&first| first == last)
    {
        counts.push(last);
    }
    let first = *counts.first()?;
    let enough = if whole {
        counts.len() >= 2
    } else {
        counts.len() >= 3
    };
    let prose = separator == b',' && first == 2 && spaced;
    (enough && first >= 2 && !prose && counts.iter().all(|&n| n == first)).then_some(first)
}

/// Each complete, non-blank record's field count, the last record's when it has no
/// newline, and whether every separator is followed by a space; `None` when a quote is
/// where CSV does not allow one.
fn field_counts(text: &[u8], separator: u8) -> Option<(Vec<usize>, Option<usize>, bool)> {
    let mut counts = Vec::new();
    let mut spaced = true;
    let mut fields = 1usize;
    let mut blank = true;
    let mut field_start = true;
    let mut in_quotes = false;
    let mut i = 0;
    while i < text.len() && counts.len() < MOST_RECORDS {
        let b = text[i];
        i += 1;
        if in_quotes {
            if b == b'"' {
                if text.get(i) == Some(&b'"') {
                    i += 1;
                } else {
                    in_quotes = false;
                    // A closing quote ends its field.
                    match text.get(i) {
                        Some(&next) if next == separator || next == b'\n' || next == b'\r' => {}
                        None => {}
                        Some(_) => return None,
                    }
                }
            }
            continue;
        }
        match b {
            b'"' if field_start => {
                in_quotes = true;
                field_start = false;
                blank = false;
            }
            b'\n' => {
                if !blank {
                    counts.push(fields);
                }
                fields = 1;
                blank = true;
                field_start = true;
            }
            b'\r' if text.get(i) == Some(&b'\n') => {}
            _ if b == separator => {
                spaced &= text.get(i) == Some(&b' ');
                fields += 1;
                blank = false;
                field_start = true;
            }
            _ => {
                if !b.is_ascii_whitespace() {
                    blank = false;
                }
                field_start = false;
            }
        }
    }
    // A quoted field still open is cut off by the head, or never closed: not a record.
    let last = (!in_quotes && !blank && counts.len() < MOST_RECORDS).then_some(fields);
    Some((counts, last, spaced))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines_of(bytes: &[u8]) -> Vec<String> {
        let lines = Arc::new(Lines::from_bytes(vec![(
            "a.log".to_string(),
            Arc::new(Bytes::Owned(bytes.to_vec())),
        )]));
        let df = lines.lazy().collect().unwrap();
        let places: Vec<u32> = df
            .column(crate::row_index::INDEX)
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(places, (0..df.height() as u32).collect::<Vec<_>>());
        df.column(LINE)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect()
    }

    /// Every line as written: blank lines kept, `\r\n` an ending, a lone `\r` and
    /// separators, quotes and control characters kept, bytes not UTF-8 shown lossily.
    #[test]
    fn every_line_as_written() {
        assert_eq!(lines_of(b"a\n\nb\n\n\nc\n"), ["a", "", "b", "", "", "c"]);
        assert_eq!(lines_of(b"a\r\nb\r\n\r\nc"), ["a", "b", "", "c"]);
        assert_eq!(
            lines_of(b"x\x1fy,\"z\tw\n#c\x00\x1b[1m\r\nq\rr\n"),
            ["x\x1fy,\"z\tw", "#c\0\x1b[1m", "q\rr"]
        );
        assert_eq!(
            lines_of(b"ok\n\xff\xfe bad\n"),
            ["ok", "\u{fffd}\u{fffd} bad"]
        );
        assert_eq!(lines_of(b"\n\n"), ["", ""]);
        assert!(lines_of(b"").is_empty());
    }

    /// An index read on as its bytes grow matches one read whole, the partial last
    /// line taken back first.
    #[test]
    fn an_index_reads_on_as_bytes_grow() {
        let all = b"one\ntw\xffo\npartial line\n\nlast";
        for cut in 0..all.len() {
            let mut index = LineIndex::of(&all[..cut]);
            index.extend(all);
            let whole = LineIndex::of(all);
            assert_eq!(index.lines(), whole.lines(), "{cut}");
            assert_eq!(index.invalid, whole.invalid, "{cut}");
            assert_eq!(index.complete(), 4);
            for i in 0..whole.lines() {
                assert_eq!(index.line(all, i), whole.line(all, i));
            }
        }
    }

    #[test]
    fn several_files_name_theirs() {
        let lines = Arc::new(Lines::from_bytes(vec![
            ("a.log".into(), Arc::new(Bytes::Owned(b"1\n2\n".to_vec()))),
            ("b.log".into(), Arc::new(Bytes::Owned(b"3\n".to_vec()))),
        ]));
        let df = lines.lazy().collect().unwrap();
        assert_eq!(df.get_column_names(), [FILE, LINE, crate::row_index::INDEX]);
        let file: Vec<Option<&str>> = df.column(FILE).unwrap().str().unwrap().iter().collect();
        assert_eq!(file, [Some("a.log"), Some("a.log"), Some("b.log")]);
        let w = lines.collect_window(1, 5).unwrap();
        assert_eq!(w.height(), 2);
        assert_eq!(w.get_column_names(), df.get_column_names());
        let places: Vec<u32> = w
            .column(crate::row_index::INDEX)
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(places, [1, 2]);
    }

    /// An index taken a chunk at a time, in steps of any size, is the one taken whole:
    /// the same lines, the same count not UTF-8, the last line with no newline kept.
    #[test]
    fn an_index_in_steps_is_the_index_whole() {
        let mut bytes = Vec::new();
        let mut bad = 0;
        for i in 0..200_000u32 {
            match i % 7 {
                0 => bytes.extend_from_slice(b"\r\n"),
                3 => {
                    bytes.extend_from_slice(b"bad \xff\xfe line\n");
                    bad += 1;
                }
                _ => bytes.extend_from_slice(
                    format!("line {i} {}\n", "x".repeat((i % 50) as usize)).as_bytes(),
                ),
            }
        }
        bytes.extend_from_slice(b"no newline at the end \xff");
        let whole = LineIndex::of(&bytes);
        assert_eq!(whole.lines(), 200_001);
        assert_eq!(whole.invalid, bad + 1);
        for step in [1, 4096, CHUNK - 3, CHUNK * 2 + 17] {
            let mut index = LineIndex {
                offsets: Offsets::for_file(bytes.len()),
                ..Default::default()
            };
            let mut steps = 0;
            while !index.extend_by(&bytes, step) {
                steps += 1;
                assert!(index.lines() < whole.lines(), "{step}");
            }
            assert!(steps > 0 || step > bytes.len(), "{step}");
            assert_eq!(index.lines(), whole.lines(), "{step}");
            assert_eq!(index.invalid, whole.invalid, "{step}");
            assert_eq!(index.complete(), whole.complete(), "{step}");
            for i in [0, 1, 3, 1000, whole.lines() - 1] {
                assert_eq!(index.line(&bytes, i), whole.line(&bytes, i), "{step} {i}");
            }
        }
    }

    /// Indexing stopped before every line is in (the dataset went) gives a read
    /// waiting on it an error, not the lines so far as all of them; paused and taken up
    /// again, it reads on.
    #[test]
    fn a_stopped_index_is_no_count_and_a_paused_one_reads_on() {
        let bytes: Vec<u8> = (0..50_000u32)
            .flat_map(|i| format!("{i}\n").into_bytes())
            .collect();
        let lines = Arc::new(Lines::from_bytes_first(
            vec![("a.log".into(), Arc::new(Bytes::Owned(bytes)))],
            1000,
        ));
        let lf = lines.lazy();
        let waiting = {
            let lf = lf.clone();
            std::thread::spawn(move || lf.collect())
        };
        // Paused: the flag stays, the read waits; taken up again, it reads on.
        assert!(!lines.index_more(1000));
        assert!(lines.resume_indexing());
        while !lines.index_more(100_000) {}
        assert_eq!(waiting.join().unwrap().unwrap().height(), 50_000);

        let bytes: Vec<u8> = (0..50_000u32)
            .flat_map(|i| format!("{i}\n").into_bytes())
            .collect();
        let lines = Arc::new(Lines::from_bytes_first(
            vec![("a.log".into(), Arc::new(Bytes::Owned(bytes)))],
            1000,
        ));
        let lf = lines.lazy();
        let waiting = std::thread::spawn(move || lf.collect());
        lines.stop_indexing();
        let error = waiting.join().unwrap().unwrap_err().to_string();
        assert!(error.contains("not all indexed"), "{error}");
        assert!(!lines.resume_indexing() || !lines.whole());
    }

    /// A file that shrinks while it is indexed stops the indexing where it is, and a
    /// read of every line says so rather than taking the lines so far for all. Unix
    /// only: Windows refuses to shorten a file another handle has mapped (os error
    /// 1224), so a rotation there fails in the rotating process, not here.
    #[test]
    #[cfg(unix)]
    fn a_file_that_shrinks_while_indexed_has_no_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotated.log");
        let bytes: Vec<u8> = (0..50_000u32)
            .flat_map(|i| format!("{i}\n").into_bytes())
            .collect();
        std::fs::write(&path, &bytes).unwrap();
        let lines = Arc::new(Lines::open_first(std::slice::from_ref(&path), false, 1000).unwrap());
        assert!(lines.indexing());
        // copytruncate, between two steps.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(10)
            .unwrap();
        assert!(lines.index_more(100_000), "the indexing stops");
        assert!(lines.shrank());
        let error = lines.lazy().collect().unwrap_err().to_string();
        // Whichever reads first says the file is shorter than it was.
        assert!(
            error.contains(SHRANK) || error.contains("shorter"),
            "{error}"
        );
    }

    /// Several files' rows are numbered by their line in their own file.
    #[test]
    fn several_files_number_their_own_lines() {
        let lines = Lines::from_bytes(vec![
            ("a.log".into(), Arc::new(Bytes::Owned(b"1\n2\n".to_vec()))),
            (
                "b.log".into(),
                Arc::new(Bytes::Owned(b"3\n4\n5\n".to_vec())),
            ),
        ]);
        assert!(lines.several());
        let numbered: Vec<Option<usize>> = (0..6).map(|p| lines.line_in_file(p)).collect();
        assert_eq!(
            numbered,
            [Some(0), Some(1), Some(0), Some(1), Some(2), None]
        );
    }

    /// A large file shows its first lines indexed, and indexes the rest in steps; its
    /// frame reads every line once they are in, numbered by their place, on either
    /// engine, and a read of it started meanwhile waits for them.
    #[test]
    fn a_large_file_indexes_behind_its_first_rows() {
        let mut bytes = Vec::new();
        for i in 0..100_000u32 {
            bytes.extend_from_slice(format!("{i}\n").as_bytes());
        }
        let lines = Arc::new(Lines::from_bytes_first(
            vec![("a.log".into(), Arc::new(Bytes::Owned(bytes)))],
            1000,
        ));
        assert!(lines.indexing());
        let first = lines.rows();
        assert!(first > 0 && first < 100_000, "{first}");
        assert_eq!(lines.collect_window(0, 200_000).unwrap().height(), first);
        let lf = lines.lazy();
        let waiting = {
            let lf = lf.clone().filter(col(LINE).eq(lit("99999")));
            std::thread::spawn(move || lf.collect().unwrap())
        };
        while !lines.index_more(50_000) {}
        assert!(!lines.indexing());
        assert_eq!(lines.rows(), 100_000);
        let (done, all) = lines.indexed_bytes();
        assert_eq!(done, all);
        let df = waiting.join().unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column(crate::row_index::INDEX)
                .unwrap()
                .u32()
                .unwrap()
                .get(0),
            Some(99_999)
        );
        for streaming in [false, true] {
            let df = crate::statistics::collect_lazy(lf.clone(), streaming).unwrap();
            assert_eq!(df.height(), 100_000, "streaming {streaming}");
        }
    }

    #[test]
    fn delimited_only_on_evidence() {
        let cases: [(&[u8], bool, Option<FileFormat>); 18] = [
            (b"a,b,c\n1,2,3\n4,5,6\n", true, Some(FileFormat::Csv)),
            (b"a,b\n1,2\n", true, Some(FileFormat::Csv)),
            (b"a\tb\n1\t2\n3\t4\n", true, Some(FileFormat::Tsv)),
            (
                b"name,note\n\"x\",\"a, b\nc\"\ny,z\n",
                true,
                Some(FileFormat::Csv),
            ),
            // A cut line is not a record.
            (b"a,b,c\n1,2,3\n4,5,6\n7,8", false, Some(FileFormat::Csv)),
            (b"a,b,c\n1,2,3\n", false, Some(FileFormat::Text)),
            // Logs: commas and tabs that do not line up.
            (
                b"2024-01-01 INFO started, ok\n2024-01-01 WARN slow, very, slow\nINFO done\n",
                true,
                Some(FileFormat::Text),
            ),
            (
                b"[2024-01-01 12:00] INFO hi\n[2024-01-01 12:01] INFO there\n",
                true,
                Some(FileFormat::Text),
            ),
            (b"[INFO] a\n", true, Some(FileFormat::Text)),
            (
                b"say \"hi\", she said\nok, then\nno, way\n",
                true,
                Some(FileFormat::Text),
            ),
            (b"one line, with a comma\n", true, Some(FileFormat::Text)),
            (b"[1, 2,", false, Some(FileFormat::Json)),
            (b"[1, 2]", true, Some(FileFormat::Json)),
            (b"{\"a\": 1}\n{\"a\": 2}\n", true, Some(FileFormat::Jsonl)),
            (b"{\n  \"a\": 1\n}\n", true, Some(FileFormat::Json)),
            (b"{not json}\n", true, Some(FileFormat::Text)),
            (b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR", false, None),
            (b"\xef\xbb\xbfid,v\n1,2\n", true, Some(FileFormat::Csv)),
        ];
        for (head, whole, format) in cases {
            assert_eq!(
                guess(head, whole),
                format,
                "{}",
                String::from_utf8_lossy(head)
            );
        }
        // Latin-1 is still text; a run of random bytes is not.
        assert_eq!(guess(b"caf\xe9 au lait\n", true), Some(FileFormat::Text));
        let noise: Vec<u8> = (0..512u32).map(|i| (i * 97 % 255 + 1) as u8).collect();
        assert_eq!(guess(&noise, true), None);
    }
}

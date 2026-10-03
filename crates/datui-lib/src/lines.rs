//! Text read as it stands: a row per line, `line_no` and `line`.
//!
//! One pass records where each line starts ([`LineIndex`]); a line is then read from a
//! map of the file where it is shown, so only the rows a view reaches are decoded.
//! Every line is a row, blank ones included, so `line_no` is the number `less -N`
//! gives it. A `\r\n` ending is a line ending; bytes that are not UTF-8 are shown as
//! `�` and counted in a note. Control characters stay in the value: the screen escapes
//! them when it draws ([`crate::sanitize`]).
//!
//! A followed file's lines are counted by the watcher ([`crate::follow`]), which moves
//! the frame's height ([`bound`]); the index reads on from its last whole line when a
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

/// The columns: the file a line is from (several files only), its number and itself.
pub const FILE: &str = "file";
pub const LINE_NO: &str = "line_no";
pub const LINE: &str = "line";

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
    let mut fields = Vec::with_capacity(3);
    if several {
        fields.push(Field::new(FILE.into(), DataType::String));
    }
    fields.push(Field::new(LINE_NO.into(), DataType::UInt32));
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
    /// Lines past [`crate::indexed::MAX_RECORDS`], left out.
    pub past_limit: usize,
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

    /// Index what `bytes`, the same bytes as before and maybe more, hold past the
    /// last newline.
    pub fn extend(&mut self, bytes: &[u8]) {
        if let Some((indexed, invalid)) = self.partial.take() {
            if indexed {
                self.offsets.truncate(self.offsets.len() - 1);
            } else {
                self.past_limit -= 1;
            }
            self.invalid -= usize::from(invalid);
        }
        if bytes.len() <= self.end {
            return;
        }
        self.offsets.widen_for(bytes.len());
        // Checked once for the whole of it; line by line only when that fails.
        let all_utf8 = std::str::from_utf8(&bytes[self.end..]).is_ok();
        let mut at = self.end;
        while at < bytes.len() {
            let newline = memchr::memchr(b'\n', &bytes[at..]).map(|i| at + i);
            let end = newline.unwrap_or(bytes.len());
            let indexed = self.offsets.len() < crate::indexed::MAX_RECORDS;
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
                    at = bytes.len();
                }
            }
        }
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
}

impl Lines {
    /// `bytes` read as lines; `name` is what the `file` column says when there are
    /// several.
    pub fn from_bytes(parts: Vec<(String, Arc<Bytes>)>) -> Self {
        let several = parts.len() > 1;
        let files = parts
            .into_iter()
            .map(|(name, bytes)| LineFile {
                name,
                grows: None,
                mapped: RwLock::new(Mapped {
                    index: LineIndex::of(bytes.as_slice()),
                    bytes,
                }),
            })
            .collect();
        Self {
            files,
            schema: Arc::new(schema(several)),
        }
    }

    /// The files at `paths`, mapped and indexed. `follow` reads one on as it grows.
    pub fn open(paths: &[PathBuf], follow: bool) -> Result<Self> {
        let parts = paths
            .iter()
            .map(|path| {
                let bytes =
                    Bytes::map(path).map_err(|e| crate::error_display::in_file(path, e.into()))?;
                Ok((file_name(path), Arc::new(bytes)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut lines = Self::from_bytes(parts);
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
    /// the rows of its complete lines, moved as it grows ([`bound`]).
    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        if self.grows() {
            crate::row_index::lazy_with_height(self, self.complete_rows())
        } else {
            crate::row_index::lazy(self)
        }
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
            1 => placed
                .iter()
                .map(|&(_, i)| Some(i as u32 + 1))
                .collect::<UInt32Chunked>()
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
        let columns = (0..self.schema.len())
            .map(|c| self.column(c, &index))
            .collect::<PolarsResult<Vec<_>>>()?;
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
    let lines = Arc::new(Lines::open(input.paths, input.options.follow)?);
    let lf = lines.lazy();
    input.report.opened = Some(Arc::new(opened(&lines, input.options)));
    Ok(lf.into())
}

/// What the Info panel says of lines read, and the window a view reads straight from
/// them when it can.
pub(crate) fn opened(lines: &Arc<Lines>, options: &crate::OpenOptions) -> crate::members::Opened {
    let (invalid, past_limit) = lines.counts();
    let scope = match lines.files.len() {
        1 => "the file".to_string(),
        n => format!("the {n} files"),
    };
    let mut notes = vec!["Read as lines: a row per line, blank lines included.".to_string()];
    if options.format_guessed {
        notes.push(
            "Nothing in its first lines says CSV or another format; --format csv reads it as CSV."
                .to_string(),
        );
    }
    if invalid > 0 {
        notes.push(format!(
            "{} not valid UTF-8; the bytes that are not are shown as \u{fffd}.",
            crate::text_formats::count(invalid as u64, "line is", "lines are")
        ));
    }
    if past_limit > 0 {
        notes.push(format!(
            "{} past the first {} are left out.",
            crate::text_formats::count(past_limit as u64, "line", "lines"),
            crate::numfmt::group_chrome(crate::indexed::MAX_RECORDS)
        ));
    }
    crate::members::Opened {
        // A followed file's rows are the watcher's to count.
        window: (!lines.grows()).then(|| {
            (
                lines.clone() as Arc<dyn crate::pushdown::Windowed>,
                lines.rows(),
            )
        }),
        notes: notes
            .into_iter()
            .map(|n| crate::text_formats::note(n, scope.clone()))
            .collect(),
        ..Default::default()
    }
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
/// (`--delimiter`, `--comment-char`, `--header-rows`, `--skip-initial-space`): they
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
        let numbers: Vec<u32> = df
            .column(LINE_NO)
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(numbers, (1..=df.height() as u32).collect::<Vec<_>>());
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
        assert_eq!(df.get_column_names(), [FILE, LINE_NO, LINE]);
        let file: Vec<Option<&str>> = df.column(FILE).unwrap().str().unwrap().iter().collect();
        assert_eq!(file, [Some("a.log"), Some("a.log"), Some("b.log")]);
        let no: Vec<u32> = df
            .column(LINE_NO)
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(no, [1, 2, 1]);
        let w = lines.collect_window(1, 5).unwrap();
        assert_eq!(w.height(), 2);
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

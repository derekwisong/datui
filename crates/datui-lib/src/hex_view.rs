//! The hex view: any local file as its bytes, a row of them at a time.
//!
//! The file is memory-mapped by a worker ([`crate::fixed_records::Bytes::map`]) and
//! never read whole: drawing slices the map for the rows on screen, and a find reads it
//! on a worker, a window at a time, so a stop is seen between windows. How many bytes a
//! row holds is decided at draw time from the width (8, 16, 32 or 64), unless a record
//! size fixes it, so that records line up.
//!
//! Everything here is pure: the layout, the cursor's moves, the parsers for an offset
//! and a pattern, the search, and the byte inspector's readings. The App drives it.

use crate::fixed_records::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The widths a row takes on its own, smallest first.
pub const WIDTHS: [usize; 4] = [8, 16, 32, 64];

/// The longest record size a row may be fixed to.
pub const MAX_RECORD_SIZE: usize = 4096;

/// The longest pattern a find takes, in bytes.
pub const MAX_PATTERN: usize = 4096;

/// Matches on screen are marked for patterns up to this long; a longer one marks only
/// the match the cursor is on.
const MAX_MARKED_PATTERN: usize = 256;

/// Bytes from the cursor the inspector reads: enough for its text.
pub const INSPECTED: usize = 64;

/// Columns the inspector panel takes beside the bytes, not counting its rule.
pub const PANEL_WIDTH: u16 = 46;

/// Below this many columns the ASCII gutter is left out.
pub const ASCII_MIN_WIDTH: u16 = 50;

/// Start positions a find reads between looks at its stop flag.
const WINDOW: usize = 8 << 20;

/// How far past a match a find looks for the next ones, to guess a record size.
const STRIDE_REACH: usize = 4 << 20;

/// Matches a stride guess is made from.
const STRIDE_MATCHES: usize = 8;

/// Columns the hex bytes of a row of `n` take: a space between bytes, one more between
/// groups of four, and another between groups of eight.
pub fn hex_width(n: usize) -> usize {
    if n == 0 { 0 } else { hex_x(n - 1) + 2 }
}

/// The column, from the start of the hex bytes, where byte `i` of a row starts.
pub fn hex_x(i: usize) -> usize {
    i * 3 + i / 4 + i / 8
}

/// Digits an offset into a file of `len` bytes takes: at least eight.
pub fn offset_digits(len: u64, decimal: bool) -> usize {
    let last = len.saturating_sub(1);
    let digits = if decimal {
        last.checked_ilog10().map_or(1, |d| d as usize + 1)
    } else {
        last.checked_ilog2().map_or(1, |b| b as usize / 4 + 1)
    };
    digits.max(8)
}

/// Columns a row of `n` bytes takes: the offset, the hex bytes, and the ASCII gutter.
pub fn row_width(n: usize, digits: usize, ascii: bool) -> usize {
    digits + 2 + hex_width(n) + if ascii { 2 + n } else { 0 }
}

/// The most bytes a row of `width` columns shows, at least one.
pub fn fit(width: usize, digits: usize, ascii: bool) -> usize {
    let mut n = 1;
    while row_width(n + 1, digits, ascii) <= width {
        n += 1;
    }
    n
}

/// Bytes a row holds when nothing fixes it: the widest of [`WIDTHS`] that fits, or as
/// many as fit when not even eight do.
pub fn auto_per_row(width: usize, digits: usize, ascii: bool) -> usize {
    WIDTHS
        .iter()
        .rev()
        .copied()
        .find(|&n| row_width(n, digits, ascii) <= width)
        .unwrap_or_else(|| fit(width, digits, ascii))
}

/// How the view is laid out at one width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    /// Bytes a row holds.
    pub per_row: usize,
    /// Of those, the bytes that fit on screen: fewer than `per_row` only when a record
    /// size is wider than the screen.
    pub shown: usize,
    /// The row's first byte on screen, when not all of it fits.
    pub first_col: usize,
    pub ascii: bool,
    /// Whether the inspector sits beside the bytes.
    pub panel: bool,
    /// Whether there is room for it there.
    pub room_for_panel: bool,
    pub digits: usize,
    /// Rows of bytes on screen.
    pub rows: usize,
}

impl Default for Geometry {
    fn default() -> Self {
        Self {
            per_row: 16,
            shown: 16,
            first_col: 0,
            ascii: true,
            panel: false,
            room_for_panel: false,
            digits: 8,
            rows: 16,
        }
    }
}

/// What a byte is, which picks its color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteClass {
    Null,
    Printable,
    Whitespace,
    Control,
    /// 0x80 and above, but not 0xFF.
    High,
    Ff,
}

pub fn class(b: u8) -> ByteClass {
    match b {
        0 => ByteClass::Null,
        0xff => ByteClass::Ff,
        b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | b' ' => ByteClass::Whitespace,
        0x21..=0x7e => ByteClass::Printable,
        0x80..=0xfe => ByteClass::High,
        _ => ByteClass::Control,
    }
}

/// Where a hex view was opened from, which decides where Esc and `q` go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The home screen: Esc and `q` go back there.
    Home,
    /// The Info panel over a table: Esc goes back to the table.
    Table,
    /// The command line: `q` quits.
    Launch,
}

/// The prompt open at the foot of the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptKind {
    GoTo,
    Find,
    RecordSize,
}

/// A find's pattern: each byte, or `None` for `??`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    pub bytes: Vec<Option<u8>>,
    /// What was typed, for the status line.
    pub label: String,
}

/// The last find: its pattern, where it landed, and what it says about a record size.
#[derive(Debug, Clone)]
pub struct Found {
    pub pattern: Pattern,
    pub hit: Option<u64>,
    /// The distance between matches when it is the same for every one read.
    pub stride: Option<u64>,
}

/// A find under way on a worker.
#[derive(Debug, Clone)]
pub struct HexFindRun {
    pub stop: Arc<AtomicBool>,
    /// Which hex view it reads: the view's `serial`.
    pub view: u64,
    pub pattern: Pattern,
}

/// A find's answer.
#[derive(Debug, Clone)]
pub struct HexHit {
    pub at: Option<u64>,
    pub wrapped: bool,
    pub stride: Option<u64>,
}

/// A local file, mapped.
pub struct HexSource {
    pub path: PathBuf,
    pub bytes: Arc<Bytes>,
}

impl std::fmt::Debug for HexSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HexSource")
            .field("path", &self.path)
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl HexSource {
    /// Map `path`. A worker's job: the map is a system call against a file that may be
    /// on a slow disk or a network share.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if path.is_dir() {
            return Err(std::io::Error::other(format!(
                "{} is a directory",
                path.display()
            )));
        }
        let bytes = Arc::new(Bytes::map(&path)?);
        Ok(Self { path, bytes })
    }
}

/// The hex view's state.
pub struct HexView {
    pub path: PathBuf,
    pub bytes: Arc<Bytes>,
    pub origin: Origin,
    /// Opened because no reader and no spec took the file.
    pub fallback: bool,
    /// Bumped per view, so a find's answer for another file is dropped.
    pub serial: u64,
    pub cursor: u64,
    /// The offset of the first row on screen.
    pub top: u64,
    /// Bytes a row holds, when fixed (`--hex-width`, `r`, `R`).
    pub record_size: Option<usize>,
    pub decimal: bool,
    /// The other end of a marked range, the cursor being one end.
    pub mark: Option<u64>,
    /// Whether the inspector is shown: beside the bytes when there is room, over them
    /// when there is not.
    pub inspector: bool,
    pub found: Option<Found>,
    /// The open prompt, and what it says went wrong.
    pub prompt: Option<PromptKind>,
    pub prompt_error: Option<String>,
    /// A find's text as UTF-16 (little-endian) rather than UTF-8.
    pub utf16: bool,
    /// Where there is no room for the inspector beside the bytes, it opens over them.
    pub inspector_open: bool,
    /// The prompt's text field.
    pub input: crate::widgets::text_input::TextInput,
    /// The spec picker (`b`), when open.
    pub picker: Option<crate::widgets::ui::PickerState>,
    /// The layout of the last frame drawn, which the moves page by.
    pub geometry: Geometry,
}

impl HexView {
    pub fn new(source: HexSource, origin: Origin, fallback: bool, serial: u64) -> Self {
        Self {
            path: source.path,
            bytes: source.bytes,
            origin,
            fallback,
            serial,
            cursor: 0,
            top: 0,
            record_size: None,
            decimal: false,
            mark: None,
            inspector: true,
            found: None,
            prompt: None,
            prompt_error: None,
            utf16: false,
            inspector_open: false,
            input: crate::widgets::text_input::TextInput::new(),
            picker: None,
            geometry: Geometry::default(),
        }
    }

    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The file's name, for the header.
    pub fn name(&self) -> String {
        self.path.file_name().map_or_else(
            || self.path.display().to_string(),
            |n| n.to_string_lossy().into_owned(),
        )
    }

    /// Whether the inspector fits beside the bytes at `width` columns.
    pub fn panel_fits(&self, width: u16) -> bool {
        let digits = offset_digits(self.len(), self.decimal);
        width as usize > row_width(16, digits, true) + PANEL_WIDTH as usize
    }

    /// The layout at `width` columns and `rows` rows of bytes, with the cursor kept on
    /// screen. Kept for the moves that page.
    pub fn lay_out(&mut self, width: u16, rows: usize) -> Geometry {
        let digits = offset_digits(self.len(), self.decimal);
        let ascii = width >= ASCII_MIN_WIDTH;
        let room_for_panel = self.panel_fits(width);
        let panel = self.inspector && room_for_panel;
        let avail = if panel {
            width as usize - PANEL_WIDTH as usize - 1
        } else {
            width as usize
        };
        let per_row = self
            .record_size
            .unwrap_or_else(|| auto_per_row(avail, digits, ascii))
            .max(1);
        let shown = per_row.min(fit(avail, digits, ascii));
        let rows = rows.max(1);
        // The top row, on a row boundary for this width, and the cursor on screen.
        let per = per_row as u64;
        let cursor_row = self.cursor / per;
        let mut top_row = self.top / per;
        if cursor_row < top_row {
            top_row = cursor_row;
        } else if cursor_row >= top_row + rows as u64 {
            top_row = cursor_row + 1 - rows as u64;
        }
        self.top = top_row * per;
        let col = (self.cursor % per) as usize;
        let mut first_col = self.geometry.first_col.min(per_row.saturating_sub(shown));
        if col < first_col {
            first_col = col;
        } else if col >= first_col + shown {
            first_col = col + 1 - shown;
        }
        self.geometry = Geometry {
            per_row,
            shown,
            first_col,
            ascii,
            panel,
            room_for_panel,
            digits,
            rows,
        };
        self.geometry
    }

    fn last(&self) -> u64 {
        self.len().saturating_sub(1)
    }

    fn per(&self) -> u64 {
        self.geometry.per_row.max(1) as u64
    }

    /// Put the cursor at `at`, inside the file.
    pub fn go(&mut self, at: u64) {
        self.cursor = at.min(self.last());
    }

    /// Move the cursor `delta` bytes.
    pub fn step(&mut self, delta: i64) {
        let at = if delta < 0 {
            self.cursor.saturating_sub(delta.unsigned_abs())
        } else {
            self.cursor.saturating_add(delta as u64)
        };
        self.go(at);
    }

    /// Move the cursor `rows` rows, staying in its column while there is a row there,
    /// and onto the last byte from a row above the last.
    pub fn step_rows(&mut self, rows: i64) {
        let per = self.per();
        let delta = per.saturating_mul(rows.unsigned_abs());
        if rows < 0 {
            if self.cursor >= delta {
                self.cursor -= delta;
            } else {
                self.cursor %= per;
            }
        } else {
            let row = self.cursor / per;
            let last_row = self.last() / per;
            if row < last_row {
                self.go(self.cursor.saturating_add(delta));
            }
        }
    }

    /// The start of the next group of four in the row, or of the next row.
    pub fn next_group(&mut self) {
        let per = self.per();
        let row_start = self.cursor / per * per;
        let col = self.cursor - row_start;
        let next = (col / 4 + 1) * 4;
        let at = if next >= per {
            row_start + per
        } else {
            row_start + next
        };
        if at <= self.last() {
            self.cursor = at;
        }
    }

    /// The start of this group of four, or of the one before it.
    pub fn previous_group(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let per = self.per();
        let row_start = self.cursor / per * per;
        let col = self.cursor - row_start;
        self.cursor = if col == 0 {
            // The last group of the row above.
            let above = row_start - per;
            above + (per - 1) / 4 * 4
        } else if !col.is_multiple_of(4) {
            row_start + col / 4 * 4
        } else {
            row_start + col - 4
        };
    }

    pub fn row_start(&mut self) {
        let per = self.per();
        self.cursor = self.cursor / per * per;
    }

    pub fn row_end(&mut self) {
        let per = self.per();
        self.go(self.cursor / per * per + per - 1);
    }

    /// The marked range, first and last byte, when a mark is set.
    pub fn selection(&self) -> Option<(u64, u64)> {
        let mark = self.mark?.min(self.last());
        Some((mark.min(self.cursor), mark.max(self.cursor)))
    }

    /// The bytes from `at`, at most `n`.
    pub fn slice(&self, at: u64, n: usize) -> &[u8] {
        let data = self.bytes.as_slice();
        let start = (at as usize).min(data.len());
        &data[start..(start + n).min(data.len())]
    }

    /// The matches of the last find that touch `[lo, hi)`, each as its first byte and
    /// the byte past it.
    pub fn matches_on_screen(&self, lo: u64, hi: u64) -> Vec<(u64, u64)> {
        let Some(found) = &self.found else {
            return Vec::new();
        };
        let m = found.pattern.bytes.len() as u64;
        if found.pattern.bytes.len() > MAX_MARKED_PATTERN {
            return found
                .hit
                .filter(|&at| at < hi && at + m > lo)
                .map(|at| vec![(at, at + m)])
                .unwrap_or_default();
        }
        let start = lo.saturating_sub(m - 1);
        let data = self.bytes.as_slice();
        let end = (hi as usize).min(data.len());
        if start as usize >= end {
            return Vec::new();
        }
        let window = &data[start as usize..end];
        let mut out = Vec::new();
        let anchor = Anchor::of(&found.pattern);
        let mut at = 0;
        while let Some(p) = anchor.next_in(window, &found.pattern, at, window.len()) {
            out.push((start + p as u64, start + p as u64 + m));
            at = p + 1;
        }
        out
    }
}

/// Parse a number: decimal, or hexadecimal after `0x`, with `_` allowed between digits.
fn number(text: &str) -> Option<u64> {
    let text = text.trim().replace('_', "");
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        u64::from_str_radix(hex, 16).ok()
    } else {
        text.parse().ok()
    }
}

/// Where a typed offset points: decimal, `0x` hex, `+N` and `-N` from the cursor, and
/// `e-N` from the end (`e-1` is the last byte). Past either end is an error.
pub fn parse_offset(text: &str, cursor: u64, len: u64) -> Result<u64, String> {
    let text = text.trim();
    if len == 0 {
        return Err("The file is empty".to_string());
    }
    let bad = || format!("{text} is not an offset: a number, 0x..., +N, -N or e-N");
    let at = if let Some(n) = text.strip_prefix("e-").or_else(|| text.strip_prefix("E-")) {
        let n = number(n).ok_or_else(bad)?;
        len.checked_sub(n)
            .ok_or_else(|| format!("{text} is before the start of the file"))?
    } else if let Some(n) = text.strip_prefix('+') {
        cursor
            .checked_add(number(n).ok_or_else(bad)?)
            .ok_or_else(bad)?
    } else if let Some(n) = text.strip_prefix('-') {
        cursor
            .checked_sub(number(n).ok_or_else(bad)?)
            .ok_or_else(|| format!("{text} is before the start of the file"))?
    } else {
        number(text).ok_or_else(bad)?
    };
    if at >= len {
        return Err(format!(
            "{text} is past the end of the file ({} bytes)",
            crate::numfmt::group_chrome(len as usize)
        ));
    }
    Ok(at)
}

/// Whether `token` is one byte in hex, or `??`.
fn hex_pair(token: &str) -> Option<Option<u8>> {
    if token == "??" {
        return Some(None);
    }
    if token.len() != 2 {
        return None;
    }
    u8::from_str_radix(token, 16).ok().map(Some)
}

/// A find's pattern from what was typed:
///
/// - `0x` and hex digits, or two or more space-separated hex pairs: bytes, where `??`
///   matches any byte (`de ad ?? ef`);
/// - anything else, or text in double quotes: the text's bytes, UTF-8, or UTF-16
///   little-endian when `utf16` is set.
pub fn parse_pattern(text: &str, utf16: bool) -> Result<Pattern, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("Type text, 0x... or hex pairs to find".to_string());
    }
    let label = trimmed.to_string();
    let as_text = |s: &str| -> Vec<Option<u8>> {
        if utf16 {
            s.encode_utf16()
                .flat_map(|u| u.to_le_bytes())
                .map(Some)
                .collect()
        } else {
            s.bytes().map(Some).collect()
        }
    };
    let quoted = trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"');
    let bytes = if quoted {
        as_text(&trimmed[1..trimmed.len() - 1])
    } else if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        let digits: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        if digits.is_empty() || !digits.len().is_multiple_of(2) || !digits.is_ascii() {
            return Err(format!(
                "{trimmed}: after 0x, pairs of hex digits (?? for any byte)"
            ));
        }
        (0..digits.len() / 2)
            .map(|i| hex_pair(&digits[i * 2..i * 2 + 2]))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| format!("{trimmed}: after 0x, pairs of hex digits (?? for any byte)"))?
    } else {
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        let pairs: Option<Vec<Option<u8>>> = tokens.iter().map(|t| hex_pair(t)).collect();
        match pairs {
            Some(pairs) if tokens.len() >= 2 || pairs.contains(&None) => pairs,
            _ => as_text(trimmed),
        }
    };
    if bytes.is_empty() {
        return Err("Type text, 0x... or hex pairs to find".to_string());
    }
    if bytes.len() > MAX_PATTERN {
        return Err(format!("A pattern is at most {MAX_PATTERN} bytes"));
    }
    if bytes.iter().all(Option::is_none) {
        return Err("A pattern needs a byte that is not ??".to_string());
    }
    Ok(Pattern { bytes, label })
}

/// The longest run of known bytes in a pattern: what is searched for, the rest checked
/// around each place it is found.
struct Anchor {
    offset: usize,
    literal: Vec<u8>,
}

impl Anchor {
    fn of(pattern: &Pattern) -> Self {
        let (mut best, mut best_len) = (0, 0);
        let mut i = 0;
        while i < pattern.bytes.len() {
            if pattern.bytes[i].is_none() {
                i += 1;
                continue;
            }
            let start = i;
            while i < pattern.bytes.len() && pattern.bytes[i].is_some() {
                i += 1;
            }
            if i - start > best_len {
                best = start;
                best_len = i - start;
            }
        }
        Self {
            offset: best,
            literal: pattern.bytes[best..best + best_len]
                .iter()
                .map(|b| b.expect("a run of known bytes"))
                .collect(),
        }
    }

    /// The first match of `pattern` in `hay` starting in `[lo, hi)`.
    fn next_in(&self, hay: &[u8], pattern: &Pattern, lo: usize, hi: usize) -> Option<usize> {
        let m = pattern.bytes.len();
        if m > hay.len() || lo >= hi {
            return None;
        }
        let hi = hi.min(hay.len() - m + 1);
        if lo >= hi {
            return None;
        }
        let from = lo + self.offset;
        let to = (hi - 1 + self.offset + self.literal.len()).min(hay.len());
        let finder = memchr::memmem::Finder::new(&self.literal);
        let mut at = from;
        while at < to {
            let q = at + finder.find(&hay[at..to])?;
            let p = q - self.offset;
            if verify(hay, pattern, p) {
                return Some(p);
            }
            at = q + 1;
        }
        None
    }

    /// The last match of `pattern` in `hay` starting in `[lo, hi)`.
    fn prev_in(&self, hay: &[u8], pattern: &Pattern, lo: usize, hi: usize) -> Option<usize> {
        let m = pattern.bytes.len();
        if m > hay.len() || lo >= hi {
            return None;
        }
        let hi = hi.min(hay.len() - m + 1);
        if lo >= hi {
            return None;
        }
        let from = lo + self.offset;
        let mut to = (hi - 1 + self.offset + self.literal.len()).min(hay.len());
        let finder = memchr::memmem::FinderRev::new(&self.literal);
        while from < to {
            let q = from + finder.rfind(&hay[from..to])?;
            let p = q - self.offset;
            if verify(hay, pattern, p) {
                return Some(p);
            }
            to = q + self.literal.len() - 1;
        }
        None
    }
}

fn verify(hay: &[u8], pattern: &Pattern, at: usize) -> bool {
    hay.get(at..at + pattern.bytes.len()).is_some_and(|window| {
        window
            .iter()
            .zip(&pattern.bytes)
            .all(|(b, p)| p.is_none_or(|p| p == *b))
    })
}

/// The find was stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped;

/// Find `pattern` in `hay`: forward from `from` (a match there counts), round to the
/// start; or backward from `from` (inclusive), round to the end. Reads a window at a
/// time, looking at `stop` between, and tells `progress` how many bytes it has read.
pub fn find(
    hay: &[u8],
    pattern: &Pattern,
    from: u64,
    forward: bool,
    stop: &AtomicBool,
    mut progress: impl FnMut(u64),
) -> Result<HexHit, Stopped> {
    let anchor = Anchor::of(pattern);
    let len = hay.len();
    let from = (from as usize).min(len);
    let mut read = 0u64;
    let mut scan = |lo: usize, hi: usize, read: &mut u64| -> Result<Option<usize>, Stopped> {
        if forward {
            let mut at = lo;
            while at < hi {
                if stop.load(Ordering::Relaxed) {
                    return Err(Stopped);
                }
                let end = (at + WINDOW).min(hi);
                if let Some(p) = anchor.next_in(hay, pattern, at, end) {
                    return Ok(Some(p));
                }
                *read += (end - at) as u64;
                progress(*read);
                at = end;
            }
        } else {
            let mut at = hi;
            while at > lo {
                if stop.load(Ordering::Relaxed) {
                    return Err(Stopped);
                }
                let start = at.saturating_sub(WINDOW).max(lo);
                if let Some(p) = anchor.prev_in(hay, pattern, start, at) {
                    return Ok(Some(p));
                }
                *read += (at - start) as u64;
                progress(*read);
                at = start;
            }
        }
        Ok(None)
    };
    let (first, second) = if forward {
        ((from, len), (0, from))
    } else {
        ((0, from + 1), (from + 1, len))
    };
    if let Some(p) = scan(first.0, first.1, &mut read)? {
        return Ok(HexHit {
            at: Some(p as u64),
            wrapped: false,
            stride: None,
        });
    }
    let at = scan(second.0, second.1, &mut read)?;
    Ok(HexHit {
        at: at.map(|p| p as u64),
        wrapped: at.is_some(),
        stride: None,
    })
}

/// The distance between the matches after `hit`, when the next few are all the same
/// distance apart: a sync word or a magic that starts every record.
pub fn stride(hay: &[u8], pattern: &Pattern, hit: u64, stop: &AtomicBool) -> Option<u64> {
    let anchor = Anchor::of(pattern);
    let hit = hit as usize;
    let reach = (hit + STRIDE_REACH).min(hay.len());
    let mut at = hit;
    let mut hits = vec![hit];
    while hits.len() < STRIDE_MATCHES {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        match anchor.next_in(hay, pattern, at + 1, reach) {
            Some(p) => {
                hits.push(p);
                at = p;
            }
            None => break,
        }
    }
    if hits.len() < 3 {
        return None;
    }
    let distance = hits[1] - hits[0];
    hits.windows(2)
        .all(|w| w[1] - w[0] == distance)
        .then_some(distance as u64)
}

/// One line of the inspector: what the bytes read as, little-endian and, where the
/// order matters, big-endian.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reading {
    pub label: &'static str,
    pub le: String,
    pub be: Option<String>,
}

fn unsigned(bytes: &[u8], big: bool) -> u64 {
    crate::fixed_records::read_unsigned(bytes, big)
}

fn signed(bytes: &[u8], big: bool) -> i64 {
    crate::fixed_records::read_signed(bytes, big)
}

/// A LEB128 varint at the front of `bytes`: its value and the bytes it took.
pub fn varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, b) in bytes.iter().take(10).enumerate() {
        let part = u64::from(b & 0x7f);
        let shift = 7 * i as u32;
        if shift >= 64 || (shift == 63 && part > 1) {
            return None;
        }
        value |= part << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// Seconds since 1970 in a year from 1980 to 2100, as text.
fn plausible_time(ns: i128) -> Option<String> {
    const LO: i128 = 315_532_800 * 1_000_000_000; // 1980-01-01
    const HI: i128 = 4_133_980_800 * 1_000_000_000; // 2101-01-01
    if !(LO..HI).contains(&ns) {
        return None;
    }
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    let at = chrono::DateTime::from_timestamp(secs, nanos)?;
    Some(if nanos == 0 {
        at.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        at.format("%Y-%m-%d %H:%M:%S%.f").to_string()
    })
}

fn date_from_days(days: i64, epoch: chrono::NaiveDate) -> Option<String> {
    // 1900 to 2100: past that a count of days is more likely something else.
    let date = epoch.checked_add_signed(chrono::Duration::days(days))?;
    let year = chrono::Datelike::year(&date);
    (1900..=2100)
        .contains(&year)
        .then(|| date.format("%Y-%m-%d").to_string())
}

fn yyyymmdd(v: u64) -> Option<String> {
    let (y, m, d) = (v / 10_000, (v / 100) % 100, v % 100);
    if !(1900..=2100).contains(&y) {
        return None;
    }
    chrono::NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32)
        .map(|date| date.format("%Y-%m-%d").to_string())
}

fn float_text(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "inf" } else { "-inf" }.to_string()
    } else if v != 0.0 && (v.abs() >= 1e15 || v.abs() < 1e-6) {
        format!("{v:.6e}")
    } else {
        let text = format!("{v}");
        if text.len() > 20 {
            format!("{v:.6e}")
        } else {
            text
        }
    }
}

/// A 32-bit float as its own shortest text, not a 64-bit one's.
fn float32_text(v: f32) -> String {
    if v.is_finite() && v != 0.0 && (v.abs() >= 1e15 || v.abs() < 1e-6) {
        format!("{v:.6e}")
    } else if v.is_finite() {
        let text = format!("{v}");
        if text.len() > 20 {
            format!("{v:.6e}")
        } else {
            text
        }
    } else {
        float_text(f64::from(v))
    }
}

/// Bits of `bytes`, most significant first, a space between bytes.
pub fn bits(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:08b}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Everything the bytes at the cursor read as. `bytes` is the cursor's byte and what
/// follows it, as far as the file goes (sixteen is enough).
pub fn readings(bytes: &[u8]) -> Vec<Reading> {
    let mut out = Vec::new();
    let both = |label: &'static str, le: String, be: String| Reading {
        label,
        le,
        be: Some(be),
    };
    let one = |label: &'static str, value: String| Reading {
        label,
        le: value,
        be: None,
    };
    let Some(&b0) = bytes.first() else {
        return out;
    };
    out.push(one("u8", b0.to_string()));
    out.push(one("i8", (b0 as i8).to_string()));
    out.push(one("bits", bits(&bytes[..1])));
    for (width, u, s) in [
        (2, "u16", "i16"),
        (3, "u24", "i24"),
        (4, "u32", "i32"),
        (5, "u40", "i40"),
        (6, "u48", "i48"),
        (8, "u64", "i64"),
    ] {
        let Some(b) = bytes.get(..width) else {
            break;
        };
        out.push(both(
            u,
            unsigned(b, false).to_string(),
            unsigned(b, true).to_string(),
        ));
        out.push(both(
            s,
            signed(b, false).to_string(),
            signed(b, true).to_string(),
        ));
    }
    if let Some(b) = bytes.get(..2) {
        out.push(both(
            "f16",
            float_text(f64::from(half::f16::from_le_bytes([b[0], b[1]]))),
            float_text(f64::from(half::f16::from_be_bytes([b[0], b[1]]))),
        ));
    }
    if let Some(b) = bytes.get(..4) {
        let raw: [u8; 4] = b.try_into().expect("four bytes");
        out.push(both(
            "f32",
            float32_text(f32::from_le_bytes(raw)),
            float32_text(f32::from_be_bytes(raw)),
        ));
    }
    if let Some(b) = bytes.get(..8) {
        let raw: [u8; 8] = b.try_into().expect("eight bytes");
        out.push(both(
            "f64",
            float_text(f64::from_le_bytes(raw)),
            float_text(f64::from_be_bytes(raw)),
        ));
    }
    if let Some((value, n)) = varint(bytes) {
        out.push(one("varint", format!("{value} ({n} B)")));
        let zigzag = (value >> 1) as i64 ^ -((value & 1) as i64);
        out.push(one("zigzag", zigzag.to_string()));
    }
    // Times, little-endian then big-endian, only where they land in a plausible year.
    let time = |label: &'static str, width: usize, per: i128| -> Option<Reading> {
        let b = bytes.get(..width)?;
        let at = |big: bool| plausible_time(i128::from(signed(b, big)) * per);
        let (le, be) = (at(false), at(true));
        (le.is_some() || be.is_some()).then(|| Reading {
            label,
            le: le.unwrap_or_default(),
            be: Some(be.unwrap_or_default()),
        })
    };
    out.extend(time("unix s", 4, 1_000_000_000));
    out.extend(time("unix ms", 8, 1_000_000));
    out.extend(time("unix us", 8, 1_000));
    out.extend(time("unix ns", 8, 1));
    if let Some(b) = bytes.get(..4) {
        let (le, be) = (yyyymmdd(unsigned(b, false)), yyyymmdd(unsigned(b, true)));
        if le.is_some() || be.is_some() {
            out.push(both(
                "yyyymmdd",
                le.unwrap_or_default(),
                be.unwrap_or_default(),
            ));
        }
        let epoch_1970 = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a date");
        let epoch_2000 = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).expect("a date");
        for (label, epoch) in [("days 1970", epoch_1970), ("days 2000", epoch_2000)] {
            let le = date_from_days(signed(b, false), epoch);
            let be = date_from_days(signed(b, true), epoch);
            if le.is_some() || be.is_some() {
                out.push(both(label, le.unwrap_or_default(), be.unwrap_or_default()));
            }
        }
    }
    let text = text_at(bytes);
    if !text.is_empty() {
        out.push(one("text", text));
    }
    let sentinels = sentinels(bytes);
    if !sentinels.is_empty() {
        out.push(one("null?", sentinels.join(", ")));
    }
    out
}

/// The text at the front of `bytes`, up to the first NUL: printable ASCII and UTF-8,
/// anything else ending it.
pub fn text_at(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let valid = match std::str::from_utf8(&bytes[..end]) {
        Ok(s) => s,
        Err(e) => std::str::from_utf8(&bytes[..e.valid_up_to()]).unwrap_or_default(),
    };
    valid
        .chars()
        .take_while(|c| !c.is_control())
        .take(48)
        .collect()
}

/// The null sentinels the bytes at the cursor hold: the smallest signed integer, the
/// largest unsigned one, a NaN.
pub fn sentinels(bytes: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for width in [2usize, 4, 8] {
        let Some(b) = bytes.get(..width) else {
            break;
        };
        let bits = width as u32 * 8;
        if b.iter().all(|&x| x == 0xff) {
            out.push(format!("u{bits} max"));
        }
        let min = 1u64 << (bits - 1);
        if unsigned(b, false) == min {
            out.push(format!("i{bits} min LE"));
        }
        if unsigned(b, true) == min {
            out.push(format!("i{bits} min BE"));
        }
    }
    if let Some(b) = bytes.get(..4) {
        let raw: [u8; 4] = b.try_into().expect("four bytes");
        if f32::from_le_bytes(raw).is_nan() || f32::from_be_bytes(raw).is_nan() {
            out.push("f32 NaN".to_string());
        }
    }
    if let Some(b) = bytes.get(..8) {
        let raw: [u8; 8] = b.try_into().expect("eight bytes");
        if f64::from_le_bytes(raw).is_nan() || f64::from_be_bytes(raw).is_nan() {
            out.push("f64 NaN".to_string());
        }
    }
    // An all-ones word is every width's max and a NaN; say it once.
    if out.iter().any(|s| s == "u64 max") {
        out.retain(|s| !s.ends_with("NaN") && !s.ends_with(" max") || s == "u64 max");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(text: &str) -> Pattern {
        parse_pattern(text, false).unwrap()
    }

    fn never() -> AtomicBool {
        AtomicBool::new(false)
    }

    fn view(bytes: Vec<u8>) -> HexView {
        HexView::new(
            HexSource {
                path: PathBuf::from("x.bin"),
                bytes: Arc::new(Bytes::Owned(bytes)),
            },
            Origin::Launch,
            false,
            1,
        )
    }

    #[test]
    fn widths_step_from_eight_to_sixty_four() {
        let auto = |w| auto_per_row(w, 8, w >= ASCII_MIN_WIDTH as usize);
        assert_eq!(row_width(16, 8, true), 79);
        assert_eq!(auto(60), 8);
        assert_eq!(auto(80), 16);
        assert_eq!(auto(150), 32);
        assert_eq!(auto(300), 64);
        // Too narrow even for eight: as many as fit, without the gutter.
        assert_eq!(auto(30), 6);
        assert_eq!(hex_x(4), 13);
        assert_eq!(hex_x(8), 27);
    }

    #[test]
    fn offsets_take_at_least_eight_digits() {
        assert_eq!(offset_digits(0, false), 8);
        assert_eq!(offset_digits(1 << 40, false), 10);
        assert_eq!(offset_digits(1 << 32, false), 8);
        assert_eq!(offset_digits(1 << 32, true), 10);
    }

    #[test]
    fn the_panel_needs_room_for_sixteen_bytes_beside_it() {
        let mut v = view(vec![0; 1000]);
        assert!(!v.lay_out(100, 10).panel);
        let g = v.lay_out(140, 10);
        assert!(g.panel);
        assert_eq!(g.per_row, 16);
        let g = v.lay_out(200, 10);
        assert_eq!(g.per_row, 32);
        v.inspector = false;
        let g = v.lay_out(200, 10);
        assert!(!g.panel);
        assert_eq!(g.per_row, 32);
    }

    #[test]
    fn a_record_size_wider_than_the_screen_scrolls_to_the_cursor() {
        let mut v = view(vec![0; 10_000]);
        v.record_size = Some(100);
        let g = v.lay_out(80, 10);
        assert_eq!((g.per_row, g.first_col), (100, 0));
        assert!(g.shown < 100);
        v.row_end();
        let g = v.lay_out(80, 10);
        assert_eq!(g.first_col + g.shown, 100);
        assert_eq!(v.cursor, 99);
    }

    #[test]
    fn moves_stay_in_the_file() {
        let mut v = view((0..100u8).collect());
        v.lay_out(80, 4);
        v.step_rows(1);
        assert_eq!(v.cursor, 16);
        v.next_group();
        assert_eq!(v.cursor, 20);
        v.previous_group();
        assert_eq!(v.cursor, 16);
        v.previous_group();
        assert_eq!(v.cursor, 12, "the last group of the row above");
        v.row_end();
        assert_eq!(v.cursor, 15);
        v.step_rows(100);
        assert_eq!(v.cursor, 99, "past the last row lands on the last byte");
        v.step_rows(-100);
        assert_eq!(
            v.cursor, 3,
            "past the first row lands in the cursor's column"
        );
        v.go(1_000);
        assert_eq!(v.cursor, 99);
        // The screen follows the cursor.
        assert_eq!(v.lay_out(80, 4).per_row, 16);
        assert_eq!(v.top, 48);
    }

    #[test]
    fn offsets_parse_every_way() {
        assert_eq!(parse_offset("100", 0, 1000), Ok(100));
        assert_eq!(parse_offset("0x1f", 0, 1000), Ok(31));
        assert_eq!(parse_offset("+10", 50, 1000), Ok(60));
        assert_eq!(parse_offset("-10", 50, 1000), Ok(40));
        assert_eq!(parse_offset("e-1", 0, 1000), Ok(999));
        assert_eq!(parse_offset("e-0x10", 0, 1000), Ok(984));
        assert_eq!(parse_offset("1_000", 0, 2000), Ok(1000));
        assert!(
            parse_offset("1000", 0, 1000)
                .unwrap_err()
                .contains("past the end")
        );
        assert!(
            parse_offset("-60", 50, 1000)
                .unwrap_err()
                .contains("before")
        );
        assert!(parse_offset("e-2000", 0, 1000).is_err());
        assert!(parse_offset("zz", 0, 1000).is_err());
        assert!(parse_offset("0", 0, 0).is_err());
    }

    #[test]
    fn patterns_are_text_hex_or_wildcards() {
        assert_eq!(
            pattern("abc").bytes,
            vec![Some(b'a'), Some(b'b'), Some(b'c')]
        );
        assert_eq!(pattern("0xdead").bytes, vec![Some(0xde), Some(0xad)]);
        assert_eq!(
            pattern("de ?? ef").bytes,
            vec![Some(0xde), None, Some(0xef)]
        );
        // One pair alone is text; quotes make hex-looking text text.
        assert_eq!(pattern("de").bytes, vec![Some(b'd'), Some(b'e')]);
        assert_eq!(pattern("\"de ad\"").bytes.len(), 5);
        assert_eq!(
            parse_pattern("hi", true).unwrap().bytes,
            vec![Some(b'h'), Some(0), Some(b'i'), Some(0)]
        );
        assert!(parse_pattern("?? ??", false).is_err());
        assert!(parse_pattern("0xabc", false).is_err());
        assert!(parse_pattern("   ", false).is_err());
    }

    #[test]
    fn a_find_spans_rows_wraps_and_goes_back() {
        let mut hay = vec![0u8; 100];
        hay[14..18].copy_from_slice(b"WXYZ"); // across the row boundary at 16
        hay[70..74].copy_from_slice(b"WXYZ");
        let p = pattern("WXYZ");
        let hit = find(&hay, &p, 0, true, &never(), |_| {}).unwrap();
        assert_eq!((hit.at, hit.wrapped), (Some(14), false));
        let hit = find(&hay, &p, 15, true, &never(), |_| {}).unwrap();
        assert_eq!(hit.at, Some(70));
        let hit = find(&hay, &p, 71, true, &never(), |_| {}).unwrap();
        assert_eq!((hit.at, hit.wrapped), (Some(14), true));
        let hit = find(&hay, &p, 69, false, &never(), |_| {}).unwrap();
        assert_eq!(hit.at, Some(14));
        let hit = find(&hay, &p, 13, false, &never(), |_| {}).unwrap();
        assert_eq!((hit.at, hit.wrapped), (Some(70), true));
        let none = find(&hay, &pattern("nope"), 0, true, &never(), |_| {}).unwrap();
        assert_eq!(none.at, None);
    }

    #[test]
    fn a_wildcard_matches_any_byte() {
        let hay = b"..\xde\x01\xef..\xde\x02\xee..\xde\x03\xef".to_vec();
        let p = pattern("de ?? ef");
        let hit = find(&hay, &p, 0, true, &never(), |_| {}).unwrap();
        assert_eq!(hit.at, Some(2));
        let hit = find(&hay, &p, 3, true, &never(), |_| {}).unwrap();
        assert_eq!(hit.at, Some(12));
        // A wildcard at the front.
        let p = pattern("?? ef");
        let hit = find(&hay, &p, 0, true, &never(), |_| {}).unwrap();
        assert_eq!(hit.at, Some(3));
    }

    #[test]
    fn a_stopped_find_says_so() {
        let hay = vec![0u8; WINDOW * 3];
        let stop = AtomicBool::new(true);
        assert_eq!(
            find(&hay, &pattern("x"), 0, true, &stop, |_| {}).unwrap_err(),
            Stopped
        );
    }

    #[test]
    fn evenly_spaced_matches_give_a_stride() {
        let mut hay = vec![0u8; 21 * 20];
        for i in 0..20 {
            hay[i * 21..i * 21 + 2].copy_from_slice(b"SY");
        }
        assert_eq!(stride(&hay, &pattern("SY"), 0, &never()), Some(21));
        hay[21 * 5 + 7] = b'S';
        hay[21 * 5 + 8] = b'Y';
        assert_eq!(stride(&hay, &pattern("SY"), 0, &never()), None);
    }

    #[test]
    fn matches_on_screen_include_ones_that_start_above_it() {
        let mut bytes = vec![0u8; 64];
        bytes[14..18].copy_from_slice(b"WXYZ");
        bytes[40..44].copy_from_slice(b"WXYZ");
        let mut v = view(bytes);
        v.found = Some(Found {
            pattern: pattern("WXYZ"),
            hit: Some(14),
            stride: None,
        });
        assert_eq!(v.matches_on_screen(16, 48), vec![(14, 18), (40, 44)]);
        assert_eq!(v.matches_on_screen(18, 40), vec![]);
    }

    #[test]
    fn the_inspector_reads_every_width_both_ways() {
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let r = readings(&bytes);
        let get = |label: &str| r.iter().find(|x| x.label == label).unwrap().clone();
        assert_eq!(get("u16").le, "513");
        assert_eq!(get("u16").be.unwrap(), "258");
        assert_eq!(get("u24").le, "197121");
        assert_eq!(get("u48").be.unwrap(), "1108152157446");
        assert_eq!(get("u64").le, "578437695752307201");
        assert_eq!(get("bits").le, "00000001");
        assert_eq!(get("varint").le, "1 (1 B)");
        // One byte: no wider readings.
        let r = readings(&[0xff]);
        assert_eq!(r.iter().find(|x| x.label == "i8").unwrap().le, "-1");
        assert!(!r.iter().any(|x| x.label == "u16"));
        assert!(readings(&[]).is_empty());
    }

    #[test]
    fn the_inspector_reads_times_dates_and_text() {
        // 2024-01-02 00:00:00 UTC.
        let r = readings(&1_704_153_600u32.to_le_bytes());
        let get = |label: &str| r.iter().find(|x| x.label == label).cloned();
        assert_eq!(get("unix s").unwrap().le, "2024-01-02 00:00:00");
        let r = readings(&20240102u32.to_le_bytes());
        let get = |label: &str| r.iter().find(|x| x.label == label).cloned();
        assert_eq!(get("yyyymmdd").unwrap().le, "2024-01-02");
        let r = readings(&19_724i32.to_le_bytes());
        let get = |label: &str| r.iter().find(|x| x.label == label).cloned();
        assert_eq!(get("days 1970").unwrap().le, "2024-01-02");
        let r = readings(b"PAR1\0xyz");
        assert_eq!(r.iter().find(|x| x.label == "text").unwrap().le, "PAR1");
        assert_eq!(varint(&[0xac, 0x02]), Some((300, 2)));
        let r = readings(&[0xac, 0x02]);
        assert_eq!(r.iter().find(|x| x.label == "zigzag").unwrap().le, "150");
        assert_eq!(f64::from(half::f16::from_le_bytes([0x00, 0x3c])), 1.0);
    }

    #[test]
    fn sentinels_are_flagged() {
        assert!(sentinels(&[0, 0, 0, 0x80]).contains(&"i32 min LE".to_string()));
        assert_eq!(sentinels(&[0xff; 8]), vec!["u64 max".to_string()]);
        let nan = f64::NAN.to_le_bytes();
        assert!(sentinels(&nan).contains(&"f64 NaN".to_string()));
        assert!(sentinels(&[1, 2, 3, 4]).is_empty());
    }

    #[test]
    fn bytes_have_classes() {
        assert_eq!(class(0), ByteClass::Null);
        assert_eq!(class(b'A'), ByteClass::Printable);
        assert_eq!(class(b' '), ByteClass::Whitespace);
        assert_eq!(class(0x01), ByteClass::Control);
        assert_eq!(class(0x7f), ByteClass::Control);
        assert_eq!(class(0x80), ByteClass::High);
        assert_eq!(class(0xff), ByteClass::Ff);
    }
}

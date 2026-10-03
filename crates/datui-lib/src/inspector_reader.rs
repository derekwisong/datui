//! The inspector's value reader: a long value read a screen at a time.
//!
//! Text is cut into units — a line, or a segment of [`SEG`] bytes of a longer
//! line — and only the units on screen are wrapped. Scrolling walks units from
//! the top of the pane; End finds the last unit by searching back for a line
//! break and fills the pane upward. So the end of a 2 MiB value is as near as
//! its start, and no key wraps more than a screen and a unit of it.
//!
//! A unit's bounds depend only on the text, never on how it was reached, so a
//! unit wrapped walking down is the unit found walking up.

use crate::copy_modal::thousands;
use std::collections::HashMap;
use std::sync::Arc;
use unicode_width::UnicodeWidthChar;

/// The most bytes of a long line wrapped as one unit.
pub const SEG: usize = 2048;
/// How far back from a segment's end a space is looked for, to end it there.
const BREAK_LOOKBACK: usize = 256;
/// A unit with no space and longer than this is a token run (base64, hex):
/// word wrap would cut it at its few slashes, so it fills each row instead.
const TOKEN_RUN: usize = 512;
/// Units kept wrapped; past this the cache starts over.
const CACHE_UNITS: usize = 256;
/// The most matches a search in a value lists.
pub const MAX_HITS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Plain,
    Dim,
    Warn,
}

/// How a line wider than the pane breaks: at spaces and after `/ & ? , ; | -`,
/// or at the pane's edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Wrap {
    #[default]
    Word,
    Hard,
}

/// Text as itself (lines broken, tabs spaced, controls marked) or as an escaped
/// literal on one line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextForm {
    Raw,
    Escaped,
}

/// What the value pane reads.
#[derive(Debug, Clone)]
pub enum Content {
    /// A few lines, wrapped whole: a scalar, a null, a message.
    Lines(Vec<(String, Tone)>),
    /// Text of any length, wrapped as it comes on screen. `lines` is its count
    /// of lines, counted once.
    Text {
        text: Arc<str>,
        form: TextForm,
        lines: usize,
    },
    /// Bytes as a hex dump, `per_line` to a row: any row is found by its offset.
    Hex { bytes: Arc<[u8]>, per_line: usize },
}

impl Content {
    pub fn text(text: Arc<str>, form: TextForm) -> Self {
        let lines = match form {
            TextForm::Raw => count_lines(&text),
            TextForm::Escaped => 1,
        };
        Content::Text { text, form, lines }
    }

    /// Whether `w` changes how this reads: only text wraps.
    pub fn wraps(&self) -> bool {
        matches!(self, Content::Text { .. })
    }
}

/// Lines in `text`: its line breaks, plus one.
pub fn count_lines(text: &str) -> usize {
    bytecount(text.as_bytes(), b'\n') + 1
}

fn bytecount(bytes: &[u8], needle: u8) -> usize {
    bytes.iter().filter(|&&b| b == needle).count()
}

/// One row of wrapped text, and the bytes of the value it shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub text: String,
    pub start: usize,
    pub end: usize,
}

/// A row on screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shown {
    pub text: String,
    pub tone: Tone,
}

/// The rows on screen, and whether any are above or below them.
#[derive(Debug, Clone, Default)]
pub struct Window {
    pub rows: Vec<Shown>,
    pub above: bool,
    pub below: bool,
    /// Where the window starts and ends in the value: bytes for text and hex,
    /// rows for lines.
    pub from: usize,
    pub to: usize,
}

/// Where a unit of text ends, and where the next starts: past its line break,
/// or at its end when the line is cut into segments.
fn unit_bounds(text: &str, start: usize, lines: bool) -> (usize, usize) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let lim = len.min(start.saturating_add(SEG));
    if lines && let Some(i) = bytes[start..lim].iter().position(|&b| b == b'\n') {
        return (start + i, start + i + 1);
    }
    if lim == len {
        return (len, len);
    }
    let mut cut = lim;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    // End the segment after a space where there is one near: a row then breaks
    // where it would have anyway.
    let from = cut.saturating_sub(BREAK_LOOKBACK).max(start + 1);
    if from < cut
        && let Some(p) = bytes[from..cut].iter().rposition(|&b| b == b' ')
    {
        cut = from + p + 1;
    }
    (cut, cut)
}

fn next_unit(text: &str, start: usize, lines: bool) -> Option<usize> {
    let (end, next) = unit_bounds(text, start, lines);
    (next > end || next < text.len()).then_some(next)
}

/// Where the line holding byte `pos` starts.
fn line_start(text: &str, pos: usize, lines: bool) -> usize {
    if !lines {
        return 0;
    }
    text.as_bytes()[..pos]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |i| i + 1)
}

/// The unit holding byte `pos`: found from its line's start, as walking down finds it.
fn unit_of(text: &str, pos: usize, lines: bool) -> usize {
    let mut at = line_start(text, pos, lines);
    while let Some(next) = next_unit(text, at, lines) {
        if next > pos {
            break;
        }
        at = next;
    }
    at
}

fn prev_unit(text: &str, start: usize, lines: bool) -> Option<usize> {
    if start == 0 {
        return None;
    }
    Some(unit_of(text, start - 1, lines))
}

fn last_unit(text: &str, lines: bool) -> usize {
    let len = text.len();
    if lines && text.ends_with('\n') {
        return len;
    }
    unit_of(text, len.saturating_sub(1), lines)
}

/// Whether a row may end after `c` under word wrap.
fn breaks_after(c: char) -> bool {
    c.is_whitespace() || matches!(c, '-' | '/' | '&' | '?' | ',' | ';' | '|')
}

/// Rows being filled, a piece of text at a time.
struct Filler {
    rows: Vec<Row>,
    cur: String,
    cur_w: usize,
    cur_start: usize,
    /// Where the row may end under word wrap: bytes into `cur`, its cells, and
    /// the value's byte after it.
    last_break: Option<(usize, usize, usize)>,
    width: usize,
    hard: bool,
}

impl Filler {
    fn end_row(&mut self, end: usize) {
        self.rows.push(Row {
            text: std::mem::take(&mut self.cur),
            start: self.cur_start,
            end,
        });
        self.cur_w = 0;
        self.cur_start = end;
        self.last_break = None;
    }

    /// Add `piece`, `w` cells wide, for the value's bytes `at..after`; `brk` when a
    /// row may end after it.
    fn push(&mut self, piece: &str, w: usize, at: usize, after: usize, brk: bool) {
        if w > 0 && self.cur_w + w > self.width && !self.cur.is_empty() {
            if !self.hard && piece.chars().all(char::is_whitespace) {
                // A space at the edge hangs past it: the row ends after it.
                self.cur.push_str(piece);
                self.end_row(after);
                return;
            }
            match self.last_break.take() {
                Some((byte, cells, src)) if !self.hard && byte < self.cur.len() => {
                    let carry = self.cur.split_off(byte);
                    let carried = self.cur_w - cells;
                    self.end_row(src);
                    self.cur = carry;
                    self.cur_w = carried;
                    if self.cur_w + w > self.width && !self.cur.is_empty() {
                        self.end_row(at);
                    }
                }
                _ => self.end_row(at),
            }
        }
        self.cur.push_str(piece);
        self.cur_w += w;
        if brk && !self.hard {
            self.last_break = Some((self.cur.len(), self.cur_w, after));
        }
    }
}

/// Wrap bytes `start..end` of `text` to rows of at most `width` cells. Raw text
/// spaces its tabs to stops of four and marks controls; escaped text is the
/// literal, quoted at the value's two ends.
pub fn wrap_unit(
    text: &str,
    start: usize,
    end: usize,
    form: TextForm,
    wrap: Wrap,
    width: usize,
) -> Vec<Row> {
    let g = crate::glyphs::get();
    let unit = &text[start..end];
    let mut fill = Filler {
        rows: Vec::new(),
        cur: String::new(),
        cur_w: 0,
        cur_start: start,
        last_break: None,
        width: width.max(1),
        hard: wrap == Wrap::Hard
            || (unit.len() > TOKEN_RUN && !unit.bytes().any(|b| b == b' ' || b == b'\t')),
    };
    if form == TextForm::Escaped && start == 0 {
        fill.push("\"", 1, start, start, false);
    }
    let ends_line = end < text.len() && text.as_bytes()[end] == b'\n';
    let mut col = 0usize;
    let mut piece = String::new();
    for (i, c) in unit.char_indices() {
        let at = start + i;
        let after = at + c.len_utf8();
        piece.clear();
        let w = match form {
            TextForm::Raw => match c {
                // A Windows line end is one break.
                '\r' if ends_line && after == end => continue,
                '\t' => {
                    let stop = 4 - col % 4;
                    piece.extend(std::iter::repeat_n(' ', stop));
                    stop
                }
                c if crate::exact::marked(c) => {
                    piece.push_str(g.control_mark);
                    crate::glyphs::display_width(g.control_mark)
                }
                c => {
                    piece.push(c);
                    c.width().unwrap_or(0)
                }
            },
            TextForm::Escaped => {
                crate::exact::escape_char(c, &mut piece);
                crate::glyphs::display_width(&piece)
            }
        };
        col += w;
        fill.push(&piece, w, at, after, breaks_after(c));
    }
    if form == TextForm::Escaped && end == text.len() {
        fill.push("\"", 1, end, end, false);
    }
    if !fill.cur.is_empty() || fill.rows.is_empty() {
        fill.end_row(end);
    }
    fill.rows
}

/// A hex dump row: offset, the bytes, and their printable ASCII.
pub fn hex_row(bytes: &[u8], offset: usize, per_line: usize) -> String {
    use std::fmt::Write;
    let chunk = &bytes[offset..bytes.len().min(offset + per_line)];
    let mut line = format!("{offset:08x} ");
    for b in chunk {
        let _ = write!(line, " {b:02x}");
    }
    for _ in chunk.len()..per_line {
        line.push_str("   ");
    }
    line.push_str("  ");
    line.extend(chunk.iter().map(|&b| {
        if (0x20..0x7f).contains(&b) {
            b as char
        } else {
            '.'
        }
    }));
    line
}

/// Bytes to a hex row at `width` cells: 32 where a row of them fits, then 16,
/// 8, 4. A row of `n` is the offset and a space, three cells a byte, two
/// spaces, and a cell a byte.
pub fn hex_per_line(width: usize) -> usize {
    [32, 16, 8]
        .into_iter()
        .find(|n| width >= 8 + 1 + n * 3 + 2 + n)
        .unwrap_or(4)
}

/// Where a pane is in its value, and the units of text wrapped so far.
#[derive(Debug, Clone, Default)]
pub struct Reader {
    /// What the reader reads: the pane's identity, its width and wrap. A new one
    /// starts at the top; the same one at another width keeps its unit.
    key: Option<(u64, usize, Wrap)>,
    /// The unit at the top (its first byte; 0 for lines and hex) and the row of it.
    top: (usize, usize),
    cache: HashMap<usize, Vec<Row>>,
    /// Bytes of text wrapped since [`Reader::take_formatted`] last asked.
    formatted: usize,
    /// A byte, and the line it is on, so a line number is counted from near it.
    line_at: Option<(usize, usize)>,
}

impl Reader {
    /// Read the pane `id` at `width`, wrapped as `wrap`.
    pub fn prepare(&mut self, id: u64, width: usize, wrap: Wrap) {
        let key = (id, width.max(1), wrap);
        match self.key {
            Some(k) if k == key => {}
            Some((same, _, _)) if same == id => {
                // Rewrapped: the same unit stays at the top.
                self.key = Some(key);
                self.cache.clear();
                self.top.1 = 0;
            }
            _ => {
                self.key = Some(key);
                self.cache.clear();
                self.top = (0, 0);
                self.line_at = None;
            }
        }
    }

    fn width(&self) -> usize {
        self.key.map_or(1, |k| k.1)
    }

    fn wrap(&self) -> Wrap {
        self.key.map_or(Wrap::Word, |k| k.2)
    }

    /// Bytes wrapped since the last ask: what a key cost.
    pub fn take_formatted(&mut self) -> usize {
        std::mem::take(&mut self.formatted)
    }

    fn rows_of(&mut self, text: &str, form: TextForm, unit: usize) -> &Vec<Row> {
        if !self.cache.contains_key(&unit) {
            if self.cache.len() >= CACHE_UNITS {
                self.cache.clear();
            }
            let lines = form == TextForm::Raw;
            let (end, _) = unit_bounds(text, unit, lines);
            self.formatted += end - unit;
            let rows = wrap_unit(text, unit, end, form, self.wrap(), self.width());
            self.cache.insert(unit, rows);
        }
        &self.cache[&unit]
    }

    fn count_rows(&mut self, text: &str, form: TextForm, unit: usize) -> usize {
        self.rows_of(text, form, unit).len()
    }

    /// Rows of a lines or hex value.
    fn total(c: &Content) -> usize {
        match c {
            Content::Lines(lines) => lines.len(),
            Content::Hex { bytes, per_line } => bytes.len().div_ceil(*per_line).max(1),
            Content::Text { .. } => 0,
        }
    }

    pub fn home(&mut self) {
        self.top = (0, 0);
    }

    /// The last rows of the value, filling a pane of `h` rows.
    pub fn end(&mut self, c: &Content, h: usize) {
        match c {
            Content::Text { text, form, .. } => {
                let last = last_unit(text, *form == TextForm::Raw);
                let rows = self.count_rows(text, *form, last);
                self.top = (last, rows);
                self.up(c, h);
            }
            _ => self.top = (0, Self::total(c).saturating_sub(h)),
        }
    }

    fn up(&mut self, c: &Content, mut n: usize) {
        let Content::Text { text, form, .. } = c else {
            self.top.1 = self.top.1.saturating_sub(n);
            return;
        };
        let lines = *form == TextForm::Raw;
        while n > 0 {
            if self.top.1 > 0 {
                let k = self.top.1.min(n);
                self.top.1 -= k;
                n -= k;
            } else if let Some(prev) = prev_unit(text, self.top.0, lines) {
                let rows = self.count_rows(text, *form, prev);
                self.top = (prev, rows);
            } else {
                break;
            }
        }
    }

    fn down(&mut self, c: &Content, mut n: usize) {
        let Content::Text { text, form, .. } = c else {
            self.top.1 += n;
            return;
        };
        let lines = *form == TextForm::Raw;
        while n > 0 {
            let rows = self.count_rows(text, *form, self.top.0);
            if self.top.1 + n < rows {
                self.top.1 += n;
                break;
            }
            n -= rows - self.top.1;
            match next_unit(text, self.top.0, lines) {
                Some(next) => self.top = (next, 0),
                None => {
                    self.top.1 = rows.saturating_sub(1);
                    break;
                }
            }
        }
    }

    /// Move `delta` rows, never past the top or past where the last row is at the
    /// bottom of a pane of `h` rows.
    pub fn scroll(&mut self, c: &Content, h: usize, delta: isize) {
        if delta < 0 {
            self.up(c, delta.unsigned_abs());
        } else {
            self.down(c, delta as usize);
        }
        self.clamp(c, h);
    }

    /// Keep the pane full: past the end, the top comes back up.
    fn clamp(&mut self, c: &Content, h: usize) {
        match c {
            Content::Text { .. } => {
                let shown = self.window(c, h).rows.len();
                if shown < h {
                    self.up(c, h - shown);
                }
            }
            _ => self.top.1 = self.top.1.min(Self::total(c).saturating_sub(h)),
        }
    }

    /// Put byte `pos` (a row, for lines) near the top of a pane of `h` rows.
    pub fn jump(&mut self, c: &Content, h: usize, pos: usize) {
        match c {
            Content::Text { text, form, .. } => {
                let pos = pos.min(text.len());
                let unit = unit_of(text, pos, *form == TextForm::Raw);
                let row = self
                    .rows_of(text, *form, unit)
                    .iter()
                    .rposition(|r| r.start <= pos)
                    .unwrap_or(0);
                self.top = (unit, row);
            }
            Content::Hex { per_line, .. } => self.top = (0, pos / per_line),
            Content::Lines(_) => self.top = (0, pos),
        }
        self.up(c, (h / 4).min(2));
        self.clamp(c, h);
    }

    /// The rows of a pane of `h` rows from the top.
    pub fn window(&mut self, c: &Content, h: usize) -> Window {
        let mut win = Window::default();
        match c {
            Content::Lines(lines) => {
                let start = self.top.1.min(lines.len());
                win.rows = lines[start..]
                    .iter()
                    .take(h)
                    .map(|(text, tone)| Shown {
                        text: text.clone(),
                        tone: *tone,
                    })
                    .collect();
                win.above = start > 0;
                win.below = start + h < lines.len();
                win.from = start;
                win.to = start + win.rows.len();
            }
            Content::Hex { bytes, per_line } => {
                let total = Self::total(c);
                let start = self.top.1.min(total.saturating_sub(1));
                for i in start..total.min(start + h) {
                    win.rows.push(Shown {
                        text: hex_row(bytes, i * per_line, *per_line),
                        tone: Tone::Plain,
                    });
                }
                win.above = start > 0;
                win.below = start + h < total;
                win.from = start * per_line;
                win.to = ((start + win.rows.len()) * per_line).min(bytes.len());
            }
            Content::Text { text, form, .. } => {
                let lines = *form == TextForm::Raw;
                let (mut unit, mut skip) = self.top;
                win.above = unit > 0 || skip > 0;
                win.from = unit;
                'units: loop {
                    let rows = self.rows_of(text, *form, unit).clone();
                    for row in rows.into_iter().skip(skip) {
                        if win.rows.len() == h {
                            win.below = true;
                            break 'units;
                        }
                        if win.rows.is_empty() {
                            win.from = row.start;
                        }
                        win.to = row.end;
                        win.rows.push(Shown {
                            text: row.text,
                            tone: Tone::Plain,
                        });
                    }
                    skip = 0;
                    match next_unit(text, unit, lines) {
                        Some(next) => unit = next,
                        None => break,
                    }
                }
            }
        }
        win
    }

    /// Rows the value takes, counted up to `cap`: how much of the pane it needs.
    pub fn rows_needed(&mut self, c: &Content, cap: usize) -> usize {
        match c {
            Content::Text { text, form, .. } => {
                let lines = *form == TextForm::Raw;
                let mut unit = 0;
                let mut n = 0;
                loop {
                    n += self.count_rows(text, *form, unit);
                    if n >= cap {
                        return cap;
                    }
                    match next_unit(text, unit, lines) {
                        Some(next) => unit = next,
                        None => return n,
                    }
                }
            }
            _ => Self::total(c).min(cap),
        }
    }

    /// The line byte `pos` of `text` is on, counted from the nearest line known.
    fn line_of(&mut self, text: &str, pos: usize) -> usize {
        let bytes = text.as_bytes();
        let line = match self.line_at {
            Some((at, line)) if at <= pos => line + bytecount(&bytes[at..pos], b'\n'),
            Some((at, line)) if at - pos < pos => line - bytecount(&bytes[pos..at], b'\n'),
            _ => 1 + bytecount(&bytes[..pos], b'\n'),
        };
        self.line_at = Some((pos, line));
        line
    }

    /// The widest [`Self::position`] can be for `c`, so what sits beside it on the
    /// rule does not move as the pane scrolls.
    pub fn position_width(c: &Content) -> usize {
        let m = crate::glyphs::get().middot;
        let widest = match c {
            Content::Lines(lines) => {
                let n = thousands(lines.len());
                format!("lines {n}-{n} of {n}")
            }
            Content::Hex { bytes, .. } => {
                let n = format!("0x{:x}", bytes.len());
                format!("{n}-{n} of {n}")
            }
            Content::Text { lines, .. } if *lines > 1 => {
                let n = thousands(*lines);
                format!("lines {n}-{n} of {n} {m} 100%")
            }
            Content::Text { .. } => "100%".to_string(),
        };
        crate::glyphs::cell_width(&widest)
    }

    /// Where the pane is, for its rule: `lines 41-73 of 4,000 · 1%`, an offset
    /// range for bytes.
    pub fn position(&mut self, c: &Content, win: &Window) -> String {
        let m = crate::glyphs::get().middot;
        match c {
            Content::Lines(lines) => format!(
                "lines {}-{} of {}",
                thousands(win.from + 1),
                thousands(win.to),
                thousands(lines.len())
            ),
            Content::Hex { bytes, .. } => format!(
                "0x{:x}-0x{:x} of 0x{:x}",
                win.from,
                win.to.saturating_sub(1),
                bytes.len()
            ),
            Content::Text { text, lines, .. } => {
                let percent = if text.is_empty() {
                    100
                } else {
                    win.to * 100 / text.len()
                };
                if *lines > 1 {
                    let first = self.line_of(text, win.from);
                    let last = first + bytecount(&text.as_bytes()[win.from..win.to], b'\n');
                    format!(
                        "lines {}-{} of {} {m} {percent}%",
                        thousands(first),
                        thousands(last.min(*lines)),
                        thousands(*lines)
                    )
                } else {
                    format!("{percent}%")
                }
            }
        }
    }
}

/// Whether a search for `needle` ignores case: only when it has no capitals.
pub fn ignores_case(needle: &str) -> bool {
    !needle.chars().any(char::is_uppercase)
}

/// Every place `needle` is in the value, up to [`MAX_HITS`]: bytes into text or
/// hex, rows of lines.
pub fn find_hits(c: &Content, needle: &str) -> Vec<usize> {
    if needle.is_empty() {
        return Vec::new();
    }
    let fold = ignores_case(needle);
    let needle_cmp = if fold {
        needle.to_ascii_lowercase()
    } else {
        needle.to_string()
    };
    let search = |hay: &[u8]| -> Vec<usize> {
        let n = needle_cmp.as_bytes();
        if n.len() > hay.len() {
            return Vec::new();
        }
        let first = n[0];
        let mut hits = Vec::new();
        let mut i = 0;
        while i + n.len() <= hay.len() && hits.len() < MAX_HITS {
            let b = if fold {
                hay[i].to_ascii_lowercase()
            } else {
                hay[i]
            };
            if b == first {
                let matched = hay[i..i + n.len()].iter().zip(n).all(|(&h, &w)| {
                    if fold {
                        h.to_ascii_lowercase() == w
                    } else {
                        h == w
                    }
                });
                if matched {
                    hits.push(i);
                    i += n.len();
                    continue;
                }
            }
            i += 1;
        }
        hits
    };
    match c {
        Content::Text { text, .. } => search(text.as_bytes()),
        Content::Hex { bytes, .. } => search(bytes),
        Content::Lines(lines) => lines
            .iter()
            .enumerate()
            .filter(|(_, (text, _))| !search(text.as_bytes()).is_empty())
            .map(|(i, _)| i)
            .take(MAX_HITS)
            .collect(),
    }
}

/// Where `needle` is in a row on screen, as byte ranges of its text.
pub fn hits_in_row(row: &str, needle: &str) -> Vec<(usize, usize)> {
    if needle.is_empty() {
        return Vec::new();
    }
    let fold = ignores_case(needle);
    let (hay, n) = if fold {
        (row.to_ascii_lowercase(), needle.to_ascii_lowercase())
    } else {
        (row.to_string(), needle.to_string())
    };
    hay.match_indices(&n)
        .map(|(i, m)| (i, i + m.len()))
        .filter(|(a, b)| row.is_char_boundary(*a) && row.is_char_boundary(*b))
        .collect()
}

/// Lines of a short text, wrapped to `width` at word breaks, for the pane's own
/// sentences and short values.
pub fn wrap_lines(text: &str, width: usize, tone: Tone, out: &mut Vec<(String, Tone)>) {
    let mut at = 0;
    for line in text.split('\n') {
        let end = at + line.len();
        for row in wrap_unit(text, at, end, TextForm::Raw, Wrap::Word, width) {
            out.push((row.text, tone));
        }
        at = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(text: &str, width: usize, wrap: Wrap) -> Vec<String> {
        wrap_unit(text, 0, text.len(), TextForm::Raw, wrap, width)
            .into_iter()
            .map(|r| r.text)
            .collect()
    }

    /// D7: word wrap never splits a word that fits a row; a URL breaks after its
    /// slashes and ampersands.
    #[test]
    fn word_wrap_breaks_between_words_and_after_url_separators() {
        let url = "https://shop.example.com/orders/segment0/segment1/segment2/segment3/segment4/segment5/segment6/segment7/segment8/segment9/segment10/segment11?id=0&utm_source=newsletter&utm_campaign=spring";
        let out = rows(url, 76, Wrap::Word);
        assert_eq!(out.concat(), url, "rows put back together are the value");
        for r in &out {
            assert!(crate::glyphs::display_width(r) <= 76, "{r}");
        }
        assert!(
            out.iter().all(|r| !r.ends_with("utm_s")),
            "no word split: {out:?}"
        );
        for r in &out[..out.len() - 1] {
            assert!(
                r.ends_with('/') || r.ends_with('&') || r.ends_with('?'),
                "{out:?}"
            );
        }

        let log = "2024-03-02T10:00:01Z INFO request_id=000001 path=/api/v1/orders/1 status=200 latency_ms=1 user_agent=Mozilla/5.0";
        let out = rows(log, 60, Wrap::Word);
        assert_eq!(out.concat(), log);
        let words: Vec<&str> = log.split(' ').collect();
        for r in &out {
            // Every row starts at a word, or after a slash inside one.
            let first = r.split(' ').next().unwrap();
            assert!(
                words.iter().any(|w| w.starts_with(first))
                    || words.iter().any(|w| w.contains(&format!("/{first}"))),
                "{first:?} in {out:?}"
            );
            assert!(!r.starts_with("ms="), "{out:?}");
        }
        // Hard wrap fills every row.
        let hard = rows(log, 60, Wrap::Hard);
        assert_eq!(hard[0].len(), 60);
    }

    #[test]
    fn a_word_wider_than_the_row_is_cut_and_wide_characters_stay_whole() {
        assert_eq!(rows("abcdefghij", 4, Wrap::Word), ["abcd", "efgh", "ij"]);
        assert_eq!(
            rows("東京大阪京都", 5, Wrap::Word),
            ["東京", "大阪", "京都"]
        );
        // A token run (base64) fills its rows rather than breaking at slashes.
        let b64 = "AbC/".repeat(200);
        let out = rows(&b64, 50, Wrap::Word);
        assert!(out[..out.len() - 1].iter().all(|r| r.len() == 50));
    }

    #[test]
    fn tabs_space_to_stops_and_controls_are_marked() {
        let g = crate::glyphs::get();
        assert_eq!(rows("a\tb", 40, Wrap::Word), ["a   b"]);
        assert_eq!(
            rows("x\u{7}y", 40, Wrap::Word),
            [format!("x{}y", g.control_mark)]
        );
        let esc = wrap_unit("a\nb", 0, 3, TextForm::Escaped, Wrap::Word, 40);
        assert_eq!(esc[0].text, r#""a\nb""#);
    }

    /// Units walk the same way down and up, and End lands on the last rows.
    #[test]
    fn units_are_found_alike_from_either_direction() {
        let text: String = (0..300)
            .map(|i| format!("line {i} {}\n", "word ".repeat(i % 7)))
            .collect::<String>()
            + &"x".repeat(SEG * 3 + 17)
            + "\ntail";
        let mut down = vec![0];
        while let Some(n) = next_unit(&text, *down.last().unwrap(), true) {
            down.push(n);
        }
        let mut up = vec![last_unit(&text, true)];
        while let Some(p) = prev_unit(&text, *up.last().unwrap(), true) {
            up.push(p);
        }
        up.reverse();
        assert_eq!(down, up);
        // A trailing line break has its empty last line.
        assert_eq!(last_unit("a\n", true), 2);
        assert_eq!(next_unit("a\n", 0, true), Some(2));
        assert_eq!(next_unit("a\n", 2, true), None);
    }

    fn reader(text: &str, width: usize) -> (Reader, Content) {
        let mut r = Reader::default();
        r.prepare(1, width, Wrap::Word);
        (r, Content::text(Arc::from(text), TextForm::Raw))
    }

    /// The end of a 2 MiB value is one key away, and no key wraps more than a
    /// chunk of it: the pane's rows and a unit.
    #[test]
    fn end_and_home_wrap_only_what_is_on_screen() {
        let prose: String = (0..30_000)
            .map(|i| format!("paragraph {i} of words that wrap at the pane edge.\n"))
            .collect::<String>()
            + &"y".repeat(2 << 20);
        let (mut r, c) = reader(&prose, 100);
        let first = r.window(&c, 40);
        assert_eq!(
            first.rows[0].text,
            "paragraph 0 of words that wrap at the pane edge."
        );
        assert!(r.take_formatted() <= crate::inspector_modal::CHUNK_BYTES);
        r.end(&c, 40);
        let last = r.window(&c, 40);
        assert!(!last.below && last.above);
        assert_eq!(last.rows.len(), 40);
        assert!(last.rows.iter().all(|row| row.text.starts_with('y')));
        assert_eq!(last.to, prose.len());
        assert!(r.take_formatted() <= crate::inspector_modal::CHUNK_BYTES);
        for _ in 0..50 {
            r.scroll(&c, 40, -39);
            r.window(&c, 40);
            assert!(r.take_formatted() <= crate::inspector_modal::CHUNK_BYTES);
        }
        r.home();
        assert_eq!(r.window(&c, 40).from, 0);
    }

    #[test]
    fn scrolling_stops_with_the_last_row_at_the_bottom() {
        let text = (1..=10)
            .map(|i| format!("{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let (mut r, c) = reader(&text, 20);
        r.scroll(&c, 4, 100);
        let w = r.window(&c, 4);
        let shown: Vec<&str> = w.rows.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(shown, ["7", "8", "9", "10"]);
        assert_eq!(
            r.position(&c, &w),
            format!("lines 7-10 of 10 {} 100%", crate::glyphs::get().middot)
        );
        r.scroll(&c, 4, -2);
        assert_eq!(r.window(&c, 4).rows[0].text, "5");
        // A value shorter than the pane stays at its top.
        let (mut r, c) = reader("a\nb", 20);
        r.scroll(&c, 4, 3);
        assert_eq!(r.window(&c, 4).rows.len(), 2);
    }

    #[test]
    fn a_search_finds_every_place_and_jumps_there() {
        let text: String = (0..4000)
            .map(|i| format!("row {i} status=200\n"))
            .collect::<String>()
            + "row 4000 status=500";
        let c = Content::text(Arc::from(text.as_str()), TextForm::Raw);
        assert!(
            find_hits(&c, "STATUS=500").is_empty(),
            "capitals match case"
        );
        let hits = find_hits(&c, "status=500");
        assert_eq!(hits, vec![text.find("status=500").unwrap()]);
        let mut r = Reader::default();
        r.prepare(1, 80, Wrap::Word);
        r.jump(&c, 10, hits[0]);
        let w = r.window(&c, 10);
        assert!(w.rows.iter().any(|row| row.text.contains("status=500")));
        assert_eq!(
            hits_in_row("a Status x status", "status"),
            [(2, 8), (11, 17)]
        );
    }

    #[test]
    fn hex_rows_are_found_by_offset() {
        let bytes: Arc<[u8]> = Arc::from(vec![0x41u8; 1 << 20]);
        let c = Content::Hex {
            bytes,
            per_line: 16,
        };
        let mut r = Reader::default();
        r.prepare(1, 80, Wrap::Word);
        r.end(&c, 20);
        let w = r.window(&c, 20);
        assert!(w.rows.last().unwrap().text.starts_with("000ffff0"));
        assert_eq!(r.position(&c, &w), "0xffec0-0xfffff of 0x100000");
        assert_eq!(hex_per_line(150), 32);
        assert_eq!(hex_per_line(100), 16);
    }
}

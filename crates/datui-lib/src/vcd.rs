//! VCD (value change dump) files from HDL simulators and logic analyzers, read into a
//! long table: one row per value change of each signal.
//!
//! A VCD file is whitespace-separated tokens: a header of `$keyword ... $end` sections
//! (`$timescale`, `$scope`, `$var`), then `#time` markers and value changes. The reader
//! takes the file a piece at a time and keeps only the token being read, the header
//! and one batch of rows. Every length is bounded: a token, a header text, the scope
//! depth and the number of signals.

use std::collections::HashMap;

use color_eyre::Result;
use polars::prelude::*;

use crate::model_files::MetaValue;
use crate::notes::Note;
use crate::text_formats::{Detail, capped_list, count, note};

/// What datui does with a VCD dump: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    convert: Some(|input| {
        crate::text_formats::convert_with(input, VcdReader::new(), |reader, lf| {
            Ok((lf, notes(reader), detail(reader)))
        })
    }),
    scan: crate::readers::read_into,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Text,
        trusted: crate::readers::Trusted {
            listing: false,
            ..crate::readers::EVERYWHERE
        },
    }],
    ..crate::readers::BASE
};

/// The longest token read; a longer one is passed over.
pub const MAX_TOKEN: usize = 1 << 20;
/// The most text kept of one header section (`$date`, `$version`, `$comment`).
pub const MAX_TEXT: usize = 4096;
/// The deepest scope nesting.
pub const MAX_DEPTH: usize = 256;
/// The most signals declared.
pub const MAX_VARS: usize = 1 << 20;
/// The most bytes of signal paths held, all signals together.
pub const MAX_PATHS: usize = 64 << 20;
/// The most tokens one `$var` or `$scope` section may hold.
const MAX_SECTION_TOKENS: usize = 16;
/// The widest vector a short value is extended to: VCD leaves leading zeros out.
pub const MAX_EXTEND: u32 = 4096;
/// Rows held before a batch is handed over.
pub const BATCH_ROWS: usize = 65_536;
/// Text held before a batch is handed over, whatever its rows.
pub const BATCH_TEXT: usize = 32 << 20;

/// What the file's `$timescale` makes of a `#time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scale {
    /// A Duration: each tick is this many nanoseconds.
    Nanos(i64),
    /// Finer than a nanosecond, or not given: `time` counts `unit`s, this many a tick.
    Count { per_tick: i64, unit: &'static str },
}

impl Scale {
    fn dtype(self) -> DataType {
        match self {
            Scale::Nanos(_) => DataType::Duration(TimeUnit::Nanoseconds),
            Scale::Count { .. } => DataType::Int64,
        }
    }

    fn per_tick(self) -> i64 {
        match self {
            Scale::Nanos(n) => n,
            Scale::Count { per_tick, .. } => per_tick,
        }
    }
}

/// `1 ns`, `10ps`, `100 fs`: the timescale's text as a [`Scale`].
pub fn parse_timescale(text: &str) -> Option<Scale> {
    let text: String = text.split_whitespace().collect();
    let digits = text.find(|c: char| !c.is_ascii_digit())?;
    let (number, unit) = text.split_at(digits);
    let number: i64 = number.parse().ok()?;
    if !matches!(number, 1 | 10 | 100) {
        return None;
    }
    Some(match unit {
        "s" => Scale::Nanos(number * 1_000_000_000),
        "ms" => Scale::Nanos(number * 1_000_000),
        "us" => Scale::Nanos(number * 1_000),
        "ns" => Scale::Nanos(number),
        "ps" => Scale::Count {
            per_tick: number,
            unit: "ps",
        },
        "fs" => Scale::Count {
            per_tick: number,
            unit: "fs",
        },
        _ => return None,
    })
}

/// Whether the first bytes are a VCD header: a `$` section that VCD writers start with,
/// closed by `$end`.
pub fn looks_like(head: &[u8]) -> bool {
    let text = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let start = text
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(text.len());
    let text = &text[start..];
    let first = text
        .split(|b| b.is_ascii_whitespace())
        .next()
        .unwrap_or_default();
    matches!(
        first,
        b"$date" | b"$version" | b"$timescale" | b"$comment" | b"$scope" | b"$var"
    ) && text.windows(4).any(|w| w == b"$end")
}

/// One declared signal.
#[derive(Debug, Clone, PartialEq)]
pub struct Var {
    /// The dotted scope path and name, with its bit range: `top.cpu.data[7:0]`.
    pub path: String,
    /// `wire`, `reg`, `integer`, `real`, ...
    pub kind: String,
    pub width: u32,
    /// The identifier code its changes are written with.
    pub id: String,
}

/// The file's header.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Header {
    pub date: Option<String>,
    pub version: Option<String>,
    pub timescale: Option<String>,
    pub comments: Vec<String>,
    pub scopes: u64,
    pub vars: Vec<Var>,
}

/// What reading noticed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    /// Rows: value changes, one per signal an identifier names.
    pub rows: u64,
    /// `#time` markers.
    pub times: u64,
    pub first_time: Option<i64>,
    pub last_time: Option<i64>,
    /// Tokens that are not VCD, passed over.
    pub unreadable: u64,
    /// Value changes for an identifier no `$var` declared.
    pub undeclared: u64,
    /// Signals past [`MAX_VARS`], or scopes past [`MAX_DEPTH`], left out.
    pub vars_dropped: u64,
    /// Times that do not fit a Duration in nanoseconds, made null.
    pub overflowed: u64,
    /// `#time` markers earlier than the one before.
    pub backwards: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Section {
    Date,
    Version,
    Timescale,
    Comment,
    Scope,
    Upscope,
    Var,
    EndDefinitions,
    Other,
}

/// A value read, waiting for its identifier.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    /// `b1010`, without the `b`.
    Vector(String),
    /// `r1.5`, without the `r`.
    Real(String),
    /// `sHello`, a string change some tools write.
    Text(String),
    /// A scalar written apart from its identifier: `1 !`.
    Scalar(char),
}

#[derive(Debug)]
enum Pending {
    Nothing,
    Section {
        what: Section,
        tokens: Vec<String>,
        text: usize,
    },
    Id(Value),
}

/// The rows of one batch.
#[derive(Debug, Default)]
struct Rows {
    time: Vec<Option<i64>>,
    var: Vec<u32>,
    value: Vec<String>,
    int: Vec<Option<u64>>,
}

/// Reads a VCD file a piece at a time.
#[derive(Debug)]
pub struct VcdReader {
    buf: Vec<u8>,
    /// Passing over a token longer than [`MAX_TOKEN`] until whitespace ends it.
    skipping: bool,
    in_body: bool,
    pending: Pending,
    scope: Vec<String>,
    /// Scopes opened past [`MAX_DEPTH`], kept as a count only.
    too_deep: usize,
    /// Bytes of signal paths held, bounded by [`MAX_PATHS`].
    path_bytes: usize,
    header: Header,
    ids: HashMap<String, Vec<u32>>,
    scale: Option<Scale>,
    time: i64,
    rows: Rows,
    held: usize,
    stats: Stats,
    /// Whether anything VCD was read: a section or a time.
    seen: bool,
}

impl Default for VcdReader {
    fn default() -> Self {
        Self::new()
    }
}

impl VcdReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            skipping: false,
            in_body: false,
            pending: Pending::Nothing,
            scope: Vec::new(),
            too_deep: 0,
            path_bytes: 0,
            header: Header::default(),
            ids: HashMap::new(),
            scale: None,
            time: 0,
            rows: Rows::default(),
            held: 0,
            stats: Stats::default(),
            seen: false,
        }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// What `time` is, once the header is read; `None` before.
    pub fn scale(&self) -> Option<Scale> {
        self.scale
    }

    /// The table's schema, once the header is read.
    pub fn schema(&self) -> Option<Schema> {
        let scale = self.scale?;
        Some(Schema::from_iter([
            Field::new("time".into(), scale.dtype()),
            Field::new("signal".into(), DataType::String),
            Field::new("value".into(), DataType::String),
            Field::new("int".into(), DataType::UInt64),
            Field::new("width".into(), DataType::UInt32),
        ]))
    }

    /// Read `bytes`, the next piece of the file.
    pub fn push(&mut self, bytes: &[u8]) {
        let mut start = 0;
        for (i, &b) in bytes.iter().enumerate() {
            if !b.is_ascii_whitespace() {
                continue;
            }
            if self.skipping {
                self.skipping = false;
                self.buf.clear();
            } else if self.buf.len() + (i - start) <= MAX_TOKEN {
                self.buf.extend_from_slice(&bytes[start..i]);
                if !self.buf.is_empty() {
                    let token = std::mem::take(&mut self.buf);
                    self.token(&String::from_utf8_lossy(&token));
                }
            } else {
                self.buf.clear();
                self.stats.unreadable += 1;
            }
            start = i + 1;
        }
        let rest = &bytes[start..];
        if self.skipping {
            return;
        }
        if self.buf.len() + rest.len() > MAX_TOKEN {
            self.buf.clear();
            self.skipping = true;
            self.stats.unreadable += 1;
        } else {
            self.buf.extend_from_slice(rest);
        }
    }

    /// A full batch, once one is held: [`BATCH_ROWS`] rows, or [`BATCH_TEXT`] of text.
    pub fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.rows.var.len() < BATCH_ROWS && self.held < BATCH_TEXT {
            return Ok(None);
        }
        self.batch().map(Some)
    }

    /// The end of the file: the last token, and the rows not yet taken.
    pub fn finish(&mut self) -> Result<DataFrame, String> {
        if !self.skipping && !self.buf.is_empty() {
            let token = std::mem::take(&mut self.buf);
            self.token(&String::from_utf8_lossy(&token));
        }
        if !self.seen {
            return Err("Not a VCD file: it has no $var, $timescale or #time.".into());
        }
        self.start_body();
        self.batch().map_err(|e| e.to_string())
    }

    fn batch(&mut self) -> PolarsResult<DataFrame> {
        self.held = 0;
        let scale = self.scale.unwrap_or(Scale::Count {
            per_tick: 1,
            unit: "ticks",
        });
        let rows = std::mem::take(&mut self.rows);
        let height = rows.var.len();
        let time = Int64Chunked::from_iter_options("time".into(), rows.time.into_iter());
        let time = match scale {
            Scale::Nanos(_) => time.into_duration(TimeUnit::Nanoseconds).into_column(),
            Scale::Count { .. } => time.into_column(),
        };
        let vars = &self.header.vars;
        let signal = StringChunked::from_iter_values(
            "signal".into(),
            rows.var.iter().map(|&i| vars[i as usize].path.as_str()),
        );
        let width = UInt32Chunked::from_iter_values(
            "width".into(),
            rows.var.iter().map(|&i| vars[i as usize].width),
        );
        let value = StringChunked::from_iter_values("value".into(), rows.value.iter());
        let int = UInt64Chunked::from_iter_options("int".into(), rows.int.into_iter());
        DataFrame::new(
            height,
            vec![
                time,
                signal.into_column(),
                value.into_column(),
                int.into_column(),
                width.into_column(),
            ],
        )
    }

    fn start_body(&mut self) {
        if self.in_body {
            return;
        }
        self.in_body = true;
        self.scale = Some(
            self.header
                .timescale
                .as_deref()
                .and_then(parse_timescale)
                .unwrap_or(Scale::Count {
                    per_tick: 1,
                    unit: "ticks",
                }),
        );
    }

    fn token(&mut self, token: &str) {
        match std::mem::replace(&mut self.pending, Pending::Nothing) {
            Pending::Section {
                what,
                mut tokens,
                text,
            } => {
                if token == "$end" {
                    self.section(what, tokens);
                } else {
                    let keep = match what {
                        Section::Date | Section::Version | Section::Comment => {
                            text + token.len() <= MAX_TEXT
                        }
                        Section::Timescale | Section::Scope | Section::Var => {
                            tokens.len() < MAX_SECTION_TOKENS && text + token.len() <= MAX_TEXT
                        }
                        _ => false,
                    };
                    let text = if keep {
                        tokens.push(token.to_string());
                        text + token.len()
                    } else {
                        text
                    };
                    self.pending = Pending::Section { what, tokens, text };
                }
                return;
            }
            Pending::Id(value) => {
                self.change(value, token);
                return;
            }
            Pending::Nothing => {}
        }
        if let Some(keyword) = token.strip_prefix('$') {
            let what = match keyword {
                "date" => Section::Date,
                "version" => Section::Version,
                "timescale" => Section::Timescale,
                "comment" => Section::Comment,
                "scope" => Section::Scope,
                "upscope" => Section::Upscope,
                "var" => Section::Var,
                "enddefinitions" => Section::EndDefinitions,
                // A stray `$end` closes nothing.
                "end" => return,
                // The body's dump commands frame value changes; they hold nothing.
                "dumpvars" | "dumpall" | "dumpon" | "dumpoff" if self.in_body => return,
                _ if self.in_body => {
                    self.stats.unreadable += 1;
                    return;
                }
                _ => Section::Other,
            };
            if self.in_body && what != Section::Comment {
                // A header section after the header is not read as one.
                self.stats.unreadable += 1;
                return;
            }
            self.seen = true;
            self.pending = Pending::Section {
                what,
                tokens: Vec::new(),
                text: 0,
            };
            return;
        }
        if let Some(digits) = token.strip_prefix('#') {
            if let Ok(time) = digits.parse::<u64>()
                && let Ok(time) = i64::try_from(time)
            {
                self.seen = true;
                self.start_body();
                if time < self.time && self.stats.times > 0 {
                    self.stats.backwards += 1;
                }
                self.time = time;
                self.stats.times += 1;
                self.stats.first_time.get_or_insert(time);
                self.stats.last_time = Some(time);
            } else {
                self.stats.unreadable += 1;
            }
            return;
        }
        if !self.in_body {
            self.stats.unreadable += 1;
            return;
        }
        let mut chars = token.chars();
        let Some(first) = chars.next() else {
            return;
        };
        let rest = chars.as_str();
        match first {
            'b' | 'B' if !rest.is_empty() => self.pending = Pending::Id(Value::Vector(rest.into())),
            'r' | 'R' if !rest.is_empty() => self.pending = Pending::Id(Value::Real(rest.into())),
            's' | 'S' => self.pending = Pending::Id(Value::Text(rest.into())),
            '0' | '1' | 'x' | 'X' | 'z' | 'Z' | 'u' | 'U' | 'w' | 'W' | 'l' | 'L' | 'h' | 'H'
            | '-' => {
                if rest.is_empty() {
                    self.pending = Pending::Id(Value::Scalar(first));
                } else {
                    self.change(Value::Scalar(first), rest);
                }
            }
            _ => self.stats.unreadable += 1,
        }
    }

    fn section(&mut self, what: Section, tokens: Vec<String>) {
        match what {
            Section::Date => self.header.date = Some(tokens.join(" ")),
            Section::Version => self.header.version = Some(tokens.join(" ")),
            Section::Timescale => self.header.timescale = Some(tokens.join(" ")),
            Section::Comment => {
                if !self.in_body && self.header.comments.len() < 16 {
                    self.header.comments.push(tokens.join(" "));
                }
            }
            Section::Scope => {
                // `$scope module top $end`: the name is the last token.
                let name = tokens.last().cloned().unwrap_or_default();
                if self.scope.len() < MAX_DEPTH && self.too_deep == 0 {
                    self.header.scopes += 1;
                    self.scope.push(name);
                } else {
                    self.stats.vars_dropped += 1;
                    self.too_deep += 1;
                }
            }
            Section::Upscope => {
                if self.too_deep > 0 {
                    self.too_deep -= 1;
                } else {
                    self.scope.pop();
                }
            }
            Section::Var => self.var(tokens),
            Section::EndDefinitions => self.start_body(),
            Section::Other => {}
        }
    }

    /// `$var wire 8 # data [7:0] $end`.
    fn var(&mut self, tokens: Vec<String>) {
        let [kind, width, id, name, range @ ..] = tokens.as_slice() else {
            self.stats.unreadable += 1;
            return;
        };
        let len = self.scope.iter().map(|s| s.len() + 1).sum::<usize>()
            + name.len()
            + range.iter().map(String::len).sum::<usize>();
        if self.header.vars.len() >= MAX_VARS
            || self.too_deep > 0
            || len > MAX_TEXT
            || self.path_bytes + len > MAX_PATHS
        {
            self.stats.vars_dropped += 1;
            return;
        }
        let Ok(width) = width.parse::<u32>() else {
            self.stats.unreadable += 1;
            return;
        };
        let mut path = String::new();
        for scope in &self.scope {
            path.push_str(scope);
            path.push('.');
        }
        path.push_str(name);
        for part in range {
            path.push_str(part);
        }
        self.path_bytes += path.len();
        let index = self.header.vars.len() as u32;
        self.header.vars.push(Var {
            path,
            kind: kind.clone(),
            width,
            id: id.clone(),
        });
        self.ids.entry(id.clone()).or_default().push(index);
    }

    fn change(&mut self, value: Value, id: &str) {
        let Some(vars) = self.ids.get(id) else {
            self.stats.undeclared += 1;
            return;
        };
        let time = match self.scale.unwrap_or(Scale::Nanos(1)).per_tick() {
            1 => Some(self.time),
            per => self.time.checked_mul(per),
        };
        if time.is_none() {
            self.stats.overflowed += vars.len() as u64;
        }
        for &var in vars {
            let width = self.header.vars[var as usize].width;
            let (text, int) = match &value {
                Value::Scalar(c) => (
                    c.to_string(),
                    match c {
                        '0' => Some(0),
                        '1' => Some(1),
                        _ => None,
                    },
                ),
                Value::Vector(bits) => {
                    let text = extend(bits, width);
                    let int = to_int(&text);
                    (text, int)
                }
                Value::Real(text) | Value::Text(text) => (text.clone(), None),
            };
            self.held += text.len();
            self.rows.time.push(time);
            self.rows.var.push(var);
            self.rows.value.push(text);
            self.rows.int.push(int);
            self.stats.rows += 1;
        }
    }
}

/// A vector value as wide as its signal: VCD leaves out leading zeros, and a leading
/// `x` or `z` stands for as many as are missing.
fn extend(bits: &str, width: u32) -> String {
    let bits = bits.to_ascii_lowercase();
    let len = bits.chars().count();
    let width = width.min(MAX_EXTEND) as usize;
    if len >= width {
        return bits;
    }
    let fill = bits
        .chars()
        .next()
        .filter(|c| matches!(c, 'x' | 'z'))
        .unwrap_or('0');
    let mut out = String::with_capacity(width);
    out.extend(std::iter::repeat_n(fill, width - len));
    out.push_str(&bits);
    out
}

/// A binary value of 0s and 1s as an integer, when it fits 64 bits.
fn to_int(bits: &str) -> Option<u64> {
    if bits.is_empty() || !bits.bytes().all(|b| b == b'0' || b == b'1') {
        return None;
    }
    let significant = bits.trim_start_matches('0');
    if significant.len() > 64 {
        return None;
    }
    if significant.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(significant, 2).ok()
}

/// What the reader noticed, as the dataset's notes.
fn notes(reader: &VcdReader) -> Vec<Note> {
    let stats = reader.stats();
    let mut notes = Vec::new();
    let of_rows = format!("of {}", count(stats.rows, "value change", "value changes"));
    if stats.unreadable > 0 {
        notes.push(note(
            format!(
                "{} skipped: not VCD",
                count(stats.unreadable, "token", "tokens")
            ),
            "in the whole file".to_string(),
        ));
    }
    if stats.undeclared > 0 {
        notes.push(note(
            format!(
                "{} left out: identifier not declared by a $var",
                count(stats.undeclared, "value change", "value changes")
            ),
            of_rows.clone(),
        ));
    }
    if stats.vars_dropped > 0 {
        notes.push(note(
            format!(
                "{} left out: past limits ({} signals, {} scopes deep, {} bytes a name)",
                count(stats.vars_dropped, "declaration", "declarations"),
                group_u64(MAX_VARS as u64),
                MAX_DEPTH,
                group_u64(MAX_TEXT as u64)
            ),
            "in the header".to_string(),
        ));
    }
    if stats.overflowed > 0 {
        notes.push(note(
            format!(
                "{} past the Duration range {} time null",
                count(stats.overflowed, "value change", "value changes"),
                crate::glyphs::get().middot
            ),
            of_rows.clone(),
        ));
    }
    if stats.backwards > 0 {
        notes.push(note(
            format!(
                "{} earlier than the one before",
                count(stats.backwards, "#time", "#times")
            ),
            format!("of {}", count(stats.times, "#time", "#times")),
        ));
    }
    match reader.scale() {
        Some(Scale::Count { unit: "ticks", .. }) => notes.push(note(
            format!(
                "no $timescale {} time in ticks",
                crate::glyphs::get().middot
            ),
            "in the header".to_string(),
        )),
        Some(Scale::Count { unit, .. }) => notes.push(note(
            format!(
                "timescale finer than a Duration {} time in {unit}",
                crate::glyphs::get().middot
            ),
            format!(
                "from $timescale {}",
                reader.header().timescale.as_deref().unwrap_or_default()
            ),
        )),
        _ => {}
    }
    notes
}

fn group_u64(n: u64) -> String {
    crate::numfmt::group_chrome(usize::try_from(n).unwrap_or(usize::MAX))
}

/// The span from `first` to `last` ticks as text, in the coarsest unit both are whole
/// numbers of: `0 to 800 s`, `5 to 12,500 ns`, `40 to 80 ps`.
fn span_text(first: i64, last: i64, scale: Scale) -> String {
    let (per, units): (i64, &[(&str, i64)]) = match scale {
        Scale::Nanos(ns) => (
            ns,
            &[
                ("s", 1_000_000_000),
                ("ms", 1_000_000),
                ("us", 1_000),
                ("ns", 1),
            ],
        ),
        Scale::Count { per_tick, unit } => (
            per_tick,
            if unit == "ps" {
                &[("ps", 1)]
            } else if unit == "fs" {
                &[("fs", 1)]
            } else {
                &[("ticks", 1)]
            },
        ),
    };
    let (Some(a), Some(b)) = (first.checked_mul(per), last.checked_mul(per)) else {
        return format!("#{first} to #{last}");
    };
    let (unit, div) = units
        .iter()
        .find(|(_, d)| a % d == 0 && b % d == 0)
        .copied()
        .unwrap_or(("ns", 1));
    let show = |n: i64| {
        let text = group_u64((n / div).unsigned_abs());
        if n < 0 { format!("-{text}") } else { text }
    };
    format!("{} to {} {unit}", show(a), show(b))
}

/// The VCD tab of the Info panel: the header and the signals.
pub fn detail(reader: &VcdReader) -> Detail {
    let header = reader.header();
    let stats = reader.stats();
    let sep = format!(" {} ", crate::glyphs::get().middot);
    let mut head = String::from("VCD");
    if let Some(timescale) = &header.timescale {
        head.push_str(&sep);
        head.push_str(&format!("timescale {timescale}"));
    }
    head.push_str(&sep);
    head.push_str(&count(header.vars.len() as u64, "signal", "signals"));
    head.push_str(&format!(" in {}", count(header.scopes, "scope", "scopes")));
    let mut lines = vec![head];
    let mut body = count(stats.rows, "value change", "value changes");
    if let (Some(first), Some(last), Some(scale)) =
        (stats.first_time, stats.last_time, reader.scale())
    {
        body.push_str(&sep);
        body.push_str(&span_text(first, last, scale));
    }
    lines.push(body);
    if let Some(date) = header.date.as_deref().filter(|d| !d.is_empty()) {
        lines.push(format!("Date: {date}"));
    }
    if let Some(version) = header.version.as_deref().filter(|v| !v.is_empty()) {
        lines.push(format!("Version: {version}"));
    }
    for comment in header.comments.iter().filter(|c| !c.is_empty()) {
        lines.push(format!("Comment: {comment}"));
    }
    let list = capped_list(
        header.vars.iter().map(|v| {
            (
                v.path.clone(),
                MetaValue::Text(format!(
                    "{}{sep}{}{sep}id {}",
                    v.kind,
                    count(u64::from(v.width), "bit", "bits"),
                    v.id
                )),
            )
        }),
        header.vars.len(),
    );
    Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Vcd),
        lines,
        list_title: "Signals",
        list,
        first: true,
        ..Default::default()
    }
}

impl crate::text_formats::BatchReader for VcdReader {
    fn push(&mut self, piece: &[u8]) -> Result<()> {
        self.push(piece);
        Ok(())
    }

    fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        self.take_batch()
    }

    fn finish(&mut self) -> Result<DataFrame> {
        self.finish().map_err(|e| color_eyre::eyre::eyre!(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file that is not one names itself, in the one shape.
    #[test]
    fn errors_name_the_file() {
        crate::readers::bad_input::each_names_its_file(
            crate::FileFormat::Vcd,
            &[("words.vcd", b"hello there\n", "Not a VCD file")],
        );
    }

    const SAMPLE: &str = "$date Mon Oct  2 2026 $end
$version Icarus Verilog $end
$timescale 1ns $end
$scope module top $end
$var wire 1 ! clk $end
$scope module cpu $end
$var wire 8 \" data [7:0] $end
$var real 64 # temp $end
$upscope $end
$upscope $end
$enddefinitions $end
#0
$dumpvars
0!
bx \"
r20.5 #
$end
#5
1!
b101 \"
#10
0!
b11111111 \"
";

    fn read(text: &[u8], piece: usize) -> (DataFrame, VcdReader) {
        let mut reader = VcdReader::new();
        let mut frames = Vec::new();
        for chunk in text.chunks(piece) {
            reader.push(chunk);
            if let Some(df) = reader.take_batch().unwrap() {
                frames.push(df);
            }
        }
        frames.push(reader.finish().unwrap());
        let mut df = frames.remove(0);
        for f in frames {
            df.vstack_mut(&f).unwrap();
        }
        (df, reader)
    }

    fn strings(df: &DataFrame, name: &str) -> Vec<String> {
        df.column(name)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect()
    }

    #[test]
    fn a_dump_reads_as_its_value_changes() {
        for piece in [1, 3, 7, 4096] {
            let (df, reader) = read(SAMPLE.as_bytes(), piece);
            assert_eq!(df.height(), 7, "piece {piece}");
            assert_eq!(
                strings(&df, "signal"),
                [
                    "top.clk",
                    "top.cpu.data[7:0]",
                    "top.cpu.temp",
                    "top.clk",
                    "top.cpu.data[7:0]",
                    "top.clk",
                    "top.cpu.data[7:0]"
                ]
            );
            assert_eq!(
                strings(&df, "value"),
                ["0", "xxxxxxxx", "20.5", "1", "00000101", "0", "11111111"]
            );
            let int: Vec<Option<u64>> = df.column("int").unwrap().u64().unwrap().iter().collect();
            assert_eq!(
                int,
                [Some(0), None, None, Some(1), Some(5), Some(0), Some(255)]
            );
            let time = df.column("time").unwrap();
            assert_eq!(time.dtype(), &DataType::Duration(TimeUnit::Nanoseconds));
            let ns: Vec<Option<i64>> = time.duration().unwrap().physical().iter().collect();
            assert_eq!(ns[3], Some(5));
            assert_eq!(ns[6], Some(10));
            let width: Vec<Option<u32>> =
                df.column("width").unwrap().u32().unwrap().iter().collect();
            assert_eq!(width[1], Some(8));
            assert_eq!(reader.header().scopes, 2);
            assert_eq!(reader.header().date.as_deref(), Some("Mon Oct 2 2026"));
            assert_eq!(reader.stats().unreadable, 0);
            assert_eq!(df.schema().as_ref(), &reader.schema().unwrap());
        }
    }

    #[test]
    fn timescales() {
        assert_eq!(parse_timescale("1ns"), Some(Scale::Nanos(1)));
        assert_eq!(parse_timescale("10 us"), Some(Scale::Nanos(10_000)));
        assert_eq!(parse_timescale("100 ms"), Some(Scale::Nanos(100_000_000)));
        assert_eq!(
            parse_timescale("1ps"),
            Some(Scale::Count {
                per_tick: 1,
                unit: "ps"
            })
        );
        assert_eq!(parse_timescale("3ns"), None);
        assert_eq!(parse_timescale("1 parsec"), None);
    }

    #[test]
    fn a_picosecond_timescale_counts_picoseconds() {
        let text = "$timescale 10ps $end $var wire 1 ! a $end $enddefinitions $end #3 1!";
        let (df, reader) = read(text.as_bytes(), 5);
        let time = df.column("time").unwrap();
        assert_eq!(time.dtype(), &DataType::Int64);
        assert_eq!(time.i64().unwrap().get(0), Some(30));
        assert!(
            notes(&reader)
                .iter()
                .any(|n| n.summary.contains("time in ps"))
        );
    }

    #[test]
    fn aliases_undeclared_ids_and_garbage() {
        let text = "$timescale 1 us $end
$scope module a $end $var wire 1 ! x $end $upscope $end
$scope module b $end $var wire 1 ! y $end $upscope $end
$enddefinitions $end
#1 1! 0? %%% #2 z!";
        let (df, reader) = read(text.as_bytes(), 2);
        assert_eq!(strings(&df, "signal"), ["a.x", "b.y", "a.x", "b.y"]);
        assert_eq!(strings(&df, "value"), ["1", "1", "z", "z"]);
        assert_eq!(reader.stats().undeclared, 1);
        assert_eq!(reader.stats().unreadable, 1);
        let ns: Vec<Option<i64>> = df
            .column("time")
            .unwrap()
            .duration()
            .unwrap()
            .physical()
            .iter()
            .collect();
        assert_eq!(ns, [Some(1000), Some(1000), Some(2000), Some(2000)]);
    }

    #[test]
    fn a_long_token_is_passed_over_and_a_wide_value_kept_whole() {
        let mut text = b"$var wire 100 ! w $end $enddefinitions $end #0 b".to_vec();
        text.extend(std::iter::repeat_n(b'1', 100));
        text.extend_from_slice(b" ! #1 b");
        text.extend(std::iter::repeat_n(b'0', MAX_TOKEN + 10));
        text.extend_from_slice(b" ! #2 b1 !");
        let (df, reader) = read(&text, 65536);
        assert_eq!(df.height(), 2);
        assert_eq!(strings(&df, "value")[0].len(), 100);
        assert_eq!(df.column("int").unwrap().u64().unwrap().get(0), None);
        assert_eq!(df.column("int").unwrap().u64().unwrap().get(1), Some(1));
        // The long token, then the `!` it left waiting for nothing.
        assert!(reader.stats().unreadable >= 1);
    }

    #[test]
    fn not_vcd_is_an_error() {
        let mut reader = VcdReader::new();
        reader.push(b"hello world");
        assert!(reader.finish().is_err());
        assert!(!looks_like(b"$foo $end"));
        assert!(looks_like(b"\n$date\n today\n$end\n"));
    }

    #[test]
    fn the_detail_names_the_header_and_signals() {
        let (_, reader) = read(SAMPLE.as_bytes(), 4096);
        let detail = detail(&reader);
        assert_eq!(detail.tab, "VCD");
        assert!(
            detail.lines[0].contains("timescale 1ns"),
            "{:?}",
            detail.lines
        );
        assert!(detail.lines[0].contains("3 signals in 2 scopes"));
        assert!(detail.lines[1].contains("0 to 10 ns"), "{:?}", detail.lines);
        assert_eq!(span_text(0, 800, Scale::Nanos(1_000_000_000)), "0 to 800 s");
        assert_eq!(span_text(5, 2_500, Scale::Nanos(1)), "5 to 2,500 ns");
        assert_eq!(
            span_text(
                4,
                8,
                Scale::Count {
                    per_tick: 10,
                    unit: "ps"
                }
            ),
            "40 to 80 ps"
        );
        assert_eq!(detail.list.len(), 3);
        assert_eq!(detail.list[1].0, "top.cpu.data[7:0]");
    }
}

//! FIX logs: `tag=value` messages, SOH or `|` delimited, read into a table of one row
//! per message.
//!
//! Each tag in the file is a column, named from the FIX dictionaries ([`dict`]): `35`
//! is `MsgType`, `55` is `Symbol`. An enumerated tag shows its names (`54=1` is `Buy`)
//! with the code beside it in `<name>_code`; a tag repeated within a message (a
//! repeating group) keeps its first value and lists the rest in `<name>_rest`. Prices
//! and quantities become numbers and `52`/`60` timestamps datetimes when every value
//! reads as one. A line's text before `8=FIX` is kept as `prefix`, with the direction
//! and session it names. `body_length_ok` and `checksum_ok` check tags 9 and 10.
//!
//! The log is read once, a piece at a time, into temporary Arrow IPC segments; a
//! message, a value and the number of columns are bounded.

pub mod dict;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error_display::FileError;
use color_eyre::Result;
use polars::prelude::*;

use crate::OpenOptions;
use crate::model_files::MetaValue;
use crate::notes::Note;
use crate::segments::{Converted, Segments};
use crate::text_formats::{Detail, Pieces, capped_list, count, note};
use crate::unfinished::Writer;
use dict::{FixType, Layers, Resolved};

/// What datui does with a FIX log: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    // The dictionaries are read before the file, so one that does not parse says so at
    // once.
    convert: Some(|input| {
        let layers = layers(input.formats, &input.options.dicts)?;
        crate::text_formats::read_one(input, |pieces| {
            convert(input.display, input.options, layers, input.writer, pieces)
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

/// The longest message read; a longer one is cut there.
pub const MAX_MESSAGE: usize = 1 << 20;
/// The most fields one message may hold; the rest are left out.
pub const MAX_FIELDS: usize = 4096;
/// The most tag columns; tags past this are left out.
pub const MAX_TAGS: usize = 4096;
/// The longest prefix kept.
pub const MAX_PREFIX: usize = 4096;
/// The most distinct names one tag is shown with in the Info panel.
const MAX_NAMES: usize = 8;
/// The most distinct counterparties whose dictionaries are remembered.
const MAX_VIEWS: usize = 1024;
/// Rows held before a batch is handed over.
pub const BATCH_ROWS: usize = 32_768;
/// Text held before a batch is handed over, whatever its rows. Each cell counts
/// too, empty or not: every tag seen is a column in every row, so a log of
/// thousands of tags would otherwise hold gigabytes of empty cells in one batch.
pub const BATCH_TEXT: usize = 32 << 20;

/// The columns that are not tags, in front of them and after them.
pub const FRONT: [&str; 3] = ["prefix", "direction", "session"];
pub const BACK: [&str; 2] = ["body_length_ok", "checksum_ok"];

/// Where `8=FIX` starts in `line`, where it starts a tag: not after a digit.
fn find_begin(line: &[u8]) -> Option<usize> {
    let mut from = 0;
    while let Some(at) = line[from..]
        .windows(5)
        .position(|w| w == b"8=FIX")
        .map(|i| from + i)
    {
        if at == 0 || !line[at - 1].is_ascii_digit() {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// Whether a value byte can be part of a BeginString: `FIX.4.4`, `FIXT.1.1`.
fn begin_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.'
}

/// The delimiter after a BeginString's value: SOH, `|`, `^A`, or another byte.
fn delimiter_at(rest: &[u8]) -> Option<&'static [u8]> {
    match rest {
        [b'^', b'A', ..] => Some(b"^A"),
        [0x01, ..] => Some(b"\x01"),
        [b'|', ..] => Some(b"|"),
        [b';', ..] => Some(b";"),
        [b',', ..] => Some(b","),
        [b'\t', ..] => Some(b"\t"),
        [b' ', ..] => Some(b" "),
        _ => None,
    }
}

/// Whether the first bytes hold a FIX message: `8=FIX...`, a delimiter and `9=`.
pub fn looks_like(head: &[u8]) -> bool {
    head.split(|&b| b == b'\n').any(|line| {
        let Some(at) = find_begin(line) else {
            return false;
        };
        let value = &line[at + 2..];
        let len = value.iter().take_while(|&&b| begin_byte(b)).count();
        let after = &value[len..];
        delimiter_at(after).is_some_and(|d| after[d.len()..].starts_with(b"9="))
    })
}

/// One message as read.
#[derive(Debug, Default)]
struct Message {
    fields: Vec<(u32, String)>,
    /// Bytes read, from `8=` on.
    len: usize,
    /// Whether it ended with tag 10.
    complete: bool,
    body_length_ok: Option<bool>,
    checksum_ok: Option<bool>,
    /// Fields past [`MAX_FIELDS`].
    dropped: u64,
}

/// Read one message from `bytes`, which start at `8=`. `None` when more is needed.
fn parse_message(bytes: &[u8], eof: bool, layers: &Layers) -> Option<Message> {
    let mut m = Message::default();
    let mut i = 0;
    let mut delim: Option<&'static [u8]> = None;
    let mut sized: Option<(u32, usize)> = None;
    let (mut sum, mut body) = (0u64, 0usize);
    let (mut declared_len, mut declared_sum) = (None::<usize>, None::<u64>);
    let mut after_length = false;
    loop {
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() && i - start < 10 {
            i += 1;
        }
        if i == bytes.len() {
            if !eof {
                return None;
            }
            i = start;
            break;
        }
        if i == start || bytes[i] != b'=' {
            i = start;
            break;
        }
        let tag = std::str::from_utf8(&bytes[start..i])
            .ok()
            .and_then(|t| t.parse::<u32>().ok())
            .filter(|t| (1..=dict::MAX_TAG).contains(t));
        let Some(tag) = tag else {
            i = start;
            break;
        };
        let value_start = i + 1;
        let value_end = match sized.take().filter(|(data, _)| *data == tag) {
            Some((_, n)) => match value_start.checked_add(n) {
                Some(end) if end <= bytes.len() => end,
                _ if !eof => return None,
                _ => bytes.len(),
            },
            None => {
                if delim.is_none() {
                    let len = bytes[value_start..]
                        .iter()
                        .take_while(|&&b| begin_byte(b))
                        .count();
                    let after = &bytes[value_start + len..];
                    if after.len() < 2 && !eof {
                        return None;
                    }
                    delim = delimiter_at(after);
                }
                let stop = bytes[value_start..].iter().enumerate().position(|(k, &b)| {
                    b == b'\n'
                        || b == b'\r'
                        || delim.is_some_and(|d| bytes[value_start + k..].starts_with(d))
                });
                match stop {
                    Some(k) => value_start + k,
                    None if !eof => return None,
                    None => bytes.len(),
                }
            }
        };
        let value = &bytes[value_start..value_end];
        if tag == 10 {
            declared_sum = std::str::from_utf8(value).ok().and_then(|v| v.parse().ok());
        } else {
            sum += bytes[start..value_end]
                .iter()
                .map(|&b| u64::from(b))
                .sum::<u64>()
                + 1;
            if after_length {
                body += value_end - start + 1;
            }
        }
        if tag == 9 && declared_len.is_none() {
            declared_len = std::str::from_utf8(value).ok().and_then(|v| v.parse().ok());
            after_length = true;
        }
        if let Some(data) = layers.data_tag(tag)
            && let Some(n) = std::str::from_utf8(value)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n <= MAX_MESSAGE)
        {
            sized = Some((data, n));
        }
        if m.fields.len() < MAX_FIELDS {
            m.fields
                .push((tag, String::from_utf8_lossy(value).into_owned()));
        } else {
            m.dropped += 1;
        }
        i = value_end;
        let delimited = delim.is_some_and(|d| bytes[i..].starts_with(d));
        if delimited {
            i += delim.map_or(0, <[u8]>::len);
        }
        if tag == 10 {
            m.complete = true;
            break;
        }
        if !delimited {
            break;
        }
    }
    m.len = i;
    if m.complete {
        m.body_length_ok = declared_len.map(|d| d == body);
        m.checksum_ok = declared_sum.map(|d| d == sum % 256);
    }
    Some(m)
}

/// The lines of `text` (newline-separated) that are not blank.
fn blank_free_lines(text: &[u8]) -> u64 {
    text.split(|&b| b == b'\n')
        .filter(|line| !line.trim_ascii().is_empty())
        .count() as u64
}

/// `in` or `out`, from the words of a log line's prefix.
fn direction(prefix: &str) -> Option<&'static str> {
    prefix
        .split(|c: char| c.is_whitespace() || matches!(c, '[' | ']' | '(' | ')' | ':' | ','))
        .find_map(|word| match word.to_ascii_uppercase().as_str() {
            "IN" | "INCOMING" | "INBOUND" | "RECV" | "RECEIVED" | "RCV" | "RX" | "<" | "<<"
            | "<-" | "<<<" => Some("in"),
            "OUT" | "OUTGOING" | "OUTBOUND" | "SENT" | "SEND" | "SND" | "TX" | ">" | ">>"
            | "->" | ">>>" => Some("out"),
            _ => None,
        })
}

/// A session a prefix names: `FIX.4.4:SENDER->TARGET`, or any `A->B`.
fn session(prefix: &str) -> Option<String> {
    prefix
        .split(|c: char| c.is_whitespace() || matches!(c, '[' | ']' | '(' | ')' | ','))
        .map(|w| w.trim_end_matches(':'))
        .find(|w| {
            w.split_once("->")
                .is_some_and(|(a, b)| !a.is_empty() && !b.is_empty() && !b.contains("->"))
        })
        .map(str::to_string)
}

/// What reading noticed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    pub messages: u64,
    /// Lines with no FIX message, passed over.
    pub skipped_lines: u64,
    /// Messages that end before tag 10.
    pub incomplete: u64,
    pub body_length_failed: u64,
    pub checksum_failed: u64,
    /// Values of tags past [`MAX_TAGS`].
    pub tags_dropped: u64,
    /// Fields past [`MAX_FIELDS`] in a message.
    pub fields_dropped: u64,
    /// Messages cut at [`MAX_MESSAGE`].
    pub cut: u64,
    /// BeginStrings and how many messages each.
    pub versions: Vec<(String, u64)>,
    /// Per dictionary, the messages it applied to.
    pub applied: Vec<u64>,
}

/// One tag's columns and what its values were.
#[derive(Debug)]
struct Tag {
    tag: u32,
    values: Vec<Option<String>>,
    codes: Option<Vec<Option<String>>>,
    rest: Option<Vec<Option<Vec<String>>>>,
    /// The names the dictionaries gave it, each with the dictionary.
    names: Vec<(String, usize)>,
    ty: Option<FixType>,
    /// The dictionaries gave it two types.
    types_differ: bool,
    /// Every value read as `ty`.
    all_read: bool,
}

impl Tag {
    fn new(tag: u32, rows: usize) -> Self {
        Self {
            tag,
            values: vec![None; rows],
            codes: None,
            rest: None,
            names: Vec::new(),
            ty: None,
            types_differ: false,
            all_read: true,
        }
    }

    /// The type its column is cast to, if any: a tag with enums keeps its names.
    fn cast(&self) -> Option<FixType> {
        if self.codes.is_some() || self.types_differ || !self.all_read {
            return None;
        }
        self.ty
            .filter(|t| !matches!(t, FixType::Text | FixType::Data))
    }
}

/// The dictionaries one counterparty's messages are read with, and what they said.
#[derive(Debug, Default)]
struct View {
    applying: Vec<usize>,
    resolved: HashMap<u32, Arc<Resolved>>,
}

/// Reads a FIX log a piece at a time.
#[derive(Debug)]
pub struct FixReader {
    layers: Layers,
    buf: Vec<u8>,
    pos: usize,
    tags: Vec<Tag>,
    by_tag: HashMap<u32, usize>,
    prefix: Vec<Option<String>>,
    direction: Vec<Option<&'static str>>,
    session: Vec<Option<String>>,
    body_ok: Vec<Option<bool>>,
    checksum_ok: Vec<Option<bool>>,
    rows: usize,
    held: usize,
    views: HashMap<[Option<String>; 3], View>,
    stats: Stats,
    seen_prefix: bool,
    seen_direction: bool,
    seen_session: bool,
}

impl FixReader {
    pub fn new(layers: Layers) -> Self {
        let applied = vec![0; layers.dicts.len()];
        Self {
            layers,
            buf: Vec::new(),
            pos: 0,
            tags: Vec::new(),
            by_tag: HashMap::new(),
            prefix: Vec::new(),
            direction: Vec::new(),
            session: Vec::new(),
            body_ok: Vec::new(),
            checksum_ok: Vec::new(),
            rows: 0,
            held: 0,
            views: HashMap::new(),
            stats: Stats {
                applied,
                ..Stats::default()
            },
            seen_prefix: false,
            seen_direction: false,
            seen_session: false,
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn layers(&self) -> &Layers {
        &self.layers
    }

    /// Read `bytes`, the next piece of the file.
    pub fn push(&mut self, bytes: &[u8]) {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(bytes);
        self.scan(false);
    }

    /// A full batch, once one is held: [`BATCH_ROWS`] rows, or [`BATCH_TEXT`] of text.
    pub fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.rows < BATCH_ROWS && self.held < BATCH_TEXT {
            return Ok(None);
        }
        self.batch().map(Some)
    }

    /// The end of the file: what is left, and the rows not yet taken.
    pub fn finish(&mut self) -> PolarsResult<DataFrame> {
        self.scan(true);
        self.buf.clear();
        self.pos = 0;
        self.batch()
    }

    fn scan(&mut self, eof: bool) {
        while self.pos < self.buf.len() {
            let rest = &self.buf[self.pos..];
            // The next message, and the whole lines before it that hold none. Found
            // before any newline is looked for, so a capture of messages back to back,
            // with no newlines at all, is read in one pass.
            let Some(at) = find_begin(rest) else {
                let last = rest.iter().rposition(|&b| b == b'\n');
                match last {
                    Some(end) => {
                        self.stats.skipped_lines += blank_free_lines(&rest[..end]);
                        self.pos += end + 1;
                    }
                    None if eof => {
                        self.stats.skipped_lines += blank_free_lines(rest);
                        self.pos = self.buf.len();
                    }
                    // A line with no `8=FIX` in its first MiB: passed over, keeping
                    // what could be the start of one.
                    None if rest.len() > MAX_MESSAGE => {
                        self.stats.skipped_lines += 1;
                        self.pos += rest.len() - 4;
                    }
                    None => return,
                }
                continue;
            };
            let line_start = rest[..at]
                .iter()
                .rposition(|&b| b == b'\n')
                .map_or(0, |i| i + 1);
            if line_start > 0 {
                self.stats.skipped_lines += blank_free_lines(&rest[..line_start - 1]);
                self.pos += line_start;
                continue;
            }
            let long = rest.len() - at > MAX_MESSAGE;
            let window = &rest[at..rest.len().min(at + MAX_MESSAGE)];
            let Some(message) = parse_message(window, eof || long, &self.layers) else {
                return;
            };
            if long && message.len == window.len() {
                self.stats.cut += 1;
            }
            let prefix = String::from_utf8_lossy(&rest[..at]).into_owned();
            let len = at + message.len.max(2);
            self.message(prefix, message);
            self.pos += len;
        }
    }

    fn message(&mut self, prefix: String, m: Message) {
        let prefix = prefix.trim().trim_end_matches(':').trim_end();
        let mut cut = prefix.len().min(MAX_PREFIX);
        while !prefix.is_char_boundary(cut) {
            cut -= 1;
        }
        let prefix = &prefix[..cut];
        let dir = direction(prefix);
        let sess = session(prefix);
        self.seen_prefix |= !prefix.is_empty();
        self.seen_direction |= dir.is_some();
        self.seen_session |= sess.is_some();
        self.held += prefix.len();
        self.prefix
            .push(Some(prefix.to_string()).filter(|p| !p.is_empty()));
        self.direction.push(dir);
        self.session.push(sess);
        self.body_ok.push(m.body_length_ok);
        self.checksum_ok.push(m.checksum_ok);
        self.stats.messages += 1;
        self.stats.fields_dropped += m.dropped;
        if !m.complete {
            self.stats.incomplete += 1;
        }
        if m.body_length_ok == Some(false) {
            self.stats.body_length_failed += 1;
        }
        if m.checksum_ok == Some(false) {
            self.stats.checksum_failed += 1;
        }

        let first = |tag: u32| {
            m.fields
                .iter()
                .find(|(t, _)| *t == tag)
                .map(|(_, v)| v.clone())
        };
        let key = [first(49), first(56), first(8)];
        if let Some(begin) = &key[2] {
            let versions = &mut self.stats.versions;
            match versions.iter().position(|(b, _)| b == begin) {
                Some(i) => versions[i].1 += 1,
                None if versions.len() < 16 => versions.push((begin.clone(), 1)),
                None => {}
            }
        }
        let mut view = match self.views.remove(&key) {
            Some(view) => view,
            None => View {
                applying: self.layers.applying(
                    key[0].as_deref(),
                    key[1].as_deref(),
                    key[2].as_deref(),
                ),
                resolved: HashMap::new(),
            },
        };
        for &i in &view.applying {
            self.stats.applied[i] += 1;
        }

        // Each tag's first value, and the rest when it repeats.
        let mut order: Vec<(u32, String, Vec<String>)> = Vec::new();
        let mut at: HashMap<u32, usize> = HashMap::new();
        for (tag, value) in m.fields {
            match at.get(&tag) {
                Some(&i) => order[i].2.push(value),
                None => {
                    at.insert(tag, order.len());
                    order.push((tag, value, Vec::new()));
                }
            }
        }
        let row = self.rows;
        for (tag, value, rest) in order {
            let index = match self.by_tag.get(&tag) {
                Some(&i) => i,
                None if self.tags.len() >= MAX_TAGS => {
                    self.stats.tags_dropped += 1;
                    continue;
                }
                None => {
                    self.by_tag.insert(tag, self.tags.len());
                    self.tags.push(Tag::new(tag, row));
                    self.tags.len() - 1
                }
            };
            let resolved = view
                .resolved
                .entry(tag)
                .or_insert_with(|| Arc::new(self.layers.resolve(&view.applying, tag)))
                .clone();
            let t = &mut self.tags[index];
            if let (Some(name), Some(by)) = (&resolved.name, resolved.named_by)
                && !t.names.iter().any(|(n, _)| n == name)
                && t.names.len() < MAX_NAMES
            {
                t.names.push((name.clone(), by));
            }
            if let Some(ty) = resolved.ty {
                match t.ty {
                    None => t.ty = Some(ty),
                    Some(had) if had != ty => t.types_differ = true,
                    _ => {}
                }
            }
            if let Some(ty) = t.ty {
                t.all_read &= ty.reads(&value) && rest.iter().all(|v| ty.reads(v));
            }
            let named = |v: &String| resolved.enums.get(v).cloned().unwrap_or_else(|| v.clone());
            if !resolved.enums.is_empty() && t.codes.is_none() {
                t.codes = Some(vec![None; row]);
            }
            let shown = if resolved.enums.is_empty() {
                value.clone()
            } else {
                named(&value)
            };
            self.held += shown.len() + rest.iter().map(String::len).sum::<usize>();
            if let Some(codes) = t.codes.as_mut() {
                codes.push(Some(value));
            }
            if !rest.is_empty() {
                let rest: Vec<String> = if resolved.enums.is_empty() {
                    rest
                } else {
                    rest.iter().map(named).collect()
                };
                t.rest
                    .get_or_insert_with(|| vec![None; row])
                    .push(Some(rest));
            }
            t.values.push(Some(shown));
        }
        self.rows += 1;
        for t in &mut self.tags {
            self.held += size_of::<Option<String>>()
                * (1 + usize::from(t.codes.is_some()) + usize::from(t.rest.is_some()));
            if t.values.len() < self.rows {
                t.values.push(None);
            }
            if let Some(codes) = t.codes.as_mut()
                && codes.len() < self.rows
            {
                codes.push(None);
            }
            if let Some(rest) = t.rest.as_mut()
                && rest.len() < self.rows
            {
                rest.push(None);
            }
        }
        if self.views.len() < MAX_VIEWS {
            self.views.insert(key, view);
        }
    }

    fn batch(&mut self) -> PolarsResult<DataFrame> {
        self.held = 0;
        let height = std::mem::take(&mut self.rows);
        let mut columns = vec![
            StringChunked::from_iter_options(
                "prefix".into(),
                std::mem::take(&mut self.prefix).into_iter(),
            )
            .into_column(),
            StringChunked::from_iter_options(
                "direction".into(),
                std::mem::take(&mut self.direction).into_iter(),
            )
            .into_column(),
            StringChunked::from_iter_options(
                "session".into(),
                std::mem::take(&mut self.session).into_iter(),
            )
            .into_column(),
        ];
        for t in &mut self.tags {
            let name = t.tag.to_string();
            columns.push(
                StringChunked::from_iter_options(
                    name.as_str().into(),
                    std::mem::take(&mut t.values).into_iter(),
                )
                .into_column(),
            );
            if let Some(codes) = t.codes.as_mut() {
                columns.push(
                    StringChunked::from_iter_options(
                        format!("{name}#code").into(),
                        std::mem::take(codes).into_iter(),
                    )
                    .into_column(),
                );
            }
            if let Some(rest) = t.rest.as_mut() {
                let list: ListChunked = std::mem::take(rest)
                    .into_iter()
                    .map(|items| items.map(|items| Series::new(PlSmallStr::EMPTY, items)))
                    .collect();
                // A batch of nulls only is a list of nulls; every batch's type must match.
                columns.push(
                    list.with_name(format!("{name}#rest").into())
                        .into_series()
                        .cast(&DataType::List(Box::new(DataType::String)))?
                        .into_column(),
                );
            }
        }
        columns.push(
            BooleanChunked::from_iter_options(
                "body_length_ok".into(),
                std::mem::take(&mut self.body_ok).into_iter(),
            )
            .into_column(),
        );
        columns.push(
            BooleanChunked::from_iter_options(
                "checksum_ok".into(),
                std::mem::take(&mut self.checksum_ok).into_iter(),
            )
            .into_column(),
        );
        DataFrame::new(height, columns)
    }

    /// Each tag and the names the dictionaries gave it, each with the dictionary's index
    /// in [`Self::layers`].
    pub fn tag_names(&self) -> Vec<(u32, String, usize)> {
        self.tags
            .iter()
            .flat_map(|t| t.names.iter().map(|(n, by)| (t.tag, n.clone(), *by)))
            .collect()
    }

    /// Each tag's column name: the name every dictionary that applied gave it, or its
    /// number when none did, when they disagree, or when the name is taken.
    pub fn column_names(&self) -> Vec<(u32, String)> {
        let mut taken: Vec<String> = FRONT
            .iter()
            .chain(BACK.iter())
            .map(|s| s.to_string())
            .collect();
        let mut out = Vec::with_capacity(self.tags.len());
        for t in &self.tags {
            // A name of digits or with `#` could meet another column's raw name.
            let usable = |name: &String| {
                !taken.contains(name)
                    && !taken.contains(&format!("{name}_code"))
                    && !name.contains('#')
                    && !name.bytes().all(|b| b.is_ascii_digit())
            };
            let name = match t.names.as_slice() {
                [(name, _)] if usable(name) => name.clone(),
                _ => t.tag.to_string(),
            };
            taken.push(name.clone());
            taken.push(format!("{name}_code"));
            taken.push(format!("{name}_rest"));
            out.push((t.tag, name));
        }
        out
    }

    /// `batch`, the last, which has every column, as the dataset shows it: for checks.
    pub fn finished(&self, batch: DataFrame) -> PolarsResult<DataFrame> {
        self.finish_frame(batch.lazy()).collect()
    }

    /// The segments' columns renamed, typed and, for those that never held a value,
    /// dropped.
    pub fn finish_frame(&self, lf: LazyFrame) -> LazyFrame {
        let mut drop: Vec<&str> = Vec::new();
        if !self.seen_prefix {
            drop.push("prefix");
        }
        if !self.seen_direction {
            drop.push("direction");
        }
        if !self.seen_session {
            drop.push("session");
        }
        let mut casts = Vec::new();
        let (mut from, mut to) = (Vec::new(), Vec::new());
        for (t, (_, name)) in self.tags.iter().zip(self.column_names()) {
            let raw = t.tag.to_string();
            if let Some(ty) = t.cast() {
                casts.push(cast(col(raw.as_str()), ty).alias(raw.as_str()));
                if t.rest.is_some()
                    && let Some(inner) = list_type(ty)
                {
                    let rest = format!("{raw}#rest");
                    casts.push(
                        col(rest.as_str())
                            .cast(DataType::List(Box::new(inner)))
                            .alias(rest.as_str()),
                    );
                }
            }
            from.push(raw.clone());
            to.push(name.clone());
            if t.codes.is_some() {
                from.push(format!("{raw}#code"));
                to.push(format!("{name}_code"));
            }
            if t.rest.is_some() {
                from.push(format!("{raw}#rest"));
                to.push(format!("{name}_rest"));
            }
        }
        let mut lf = lf;
        if !drop.is_empty() {
            lf = lf.drop(cols(drop));
        }
        if !casts.is_empty() {
            lf = lf.with_columns(casts);
        }
        lf.rename(from, to, true)
    }
}

/// A string column as `ty`.
fn cast(expr: Expr, ty: FixType) -> Expr {
    match ty {
        FixType::Int | FixType::Length => expr.cast(DataType::Int64),
        FixType::Float => expr.cast(DataType::Float64),
        FixType::Bool => expr.eq(lit("Y")),
        FixType::Timestamp => expr.str().to_datetime(
            Some(TimeUnit::Nanoseconds),
            Some(TimeZone::UTC),
            StrptimeOptions {
                format: Some("%Y%m%d-%H:%M:%S%.f".into()),
                ..Default::default()
            },
            lit("raise"),
        ),
        FixType::Date => expr.str().to_date(StrptimeOptions {
            format: Some("%Y%m%d".into()),
            ..Default::default()
        }),
        FixType::Data | FixType::Text => expr,
    }
}

/// The element type a repeated tag's list is cast to: numbers only.
fn list_type(ty: FixType) -> Option<DataType> {
    match ty {
        FixType::Int | FixType::Length => Some(DataType::Int64),
        FixType::Float => Some(DataType::Float64),
        _ => None,
    }
}

fn notes(reader: &FixReader) -> Vec<Note> {
    let stats = reader.stats();
    let mut notes = Vec::new();
    let of_messages = format!("of {}", count(stats.messages, "message", "messages"));
    if stats.skipped_lines > 0 {
        notes.push(note(
            format!(
                "{} no FIX message and left out",
                count(stats.skipped_lines, "line holds", "lines hold")
            ),
            "in the whole file".to_string(),
        ));
    }
    if stats.incomplete > 0 {
        notes.push(note(
            format!(
                "{} before tag 10; its checks are null",
                count(stats.incomplete, "message ends", "messages end")
            ),
            of_messages.clone(),
        ));
    }
    if stats.body_length_failed > 0 {
        notes.push(note(
            format!(
                "{} BodyLength (9); body_length_ok is false",
                count(
                    stats.body_length_failed,
                    "message fails its",
                    "messages fail their"
                )
            ),
            of_messages.clone(),
        ));
    }
    if stats.checksum_failed > 0 {
        notes.push(note(
            format!(
                "{} CheckSum (10); checksum_ok is false",
                count(
                    stats.checksum_failed,
                    "message fails its",
                    "messages fail their"
                )
            ),
            of_messages.clone(),
        ));
    }
    if stats.tags_dropped > 0 || stats.fields_dropped > 0 {
        notes.push(note(
            format!(
                "{} past datui's limits ({MAX_TAGS} tags, {MAX_FIELDS} fields a message) left out",
                count(stats.tags_dropped + stats.fields_dropped, "value", "values")
            ),
            of_messages.clone(),
        ));
    }
    if stats.cut > 0 {
        notes.push(note(
            format!(
                "{} longer than {} MiB, cut there",
                count(stats.cut, "message is", "messages are"),
                MAX_MESSAGE >> 20
            ),
            of_messages.clone(),
        ));
    }
    let differ: Vec<String> = reader
        .tags
        .iter()
        .filter(|t| t.names.len() > 1)
        .map(|t| t.tag.to_string())
        .collect();
    if !differ.is_empty() {
        notes.push(note(
            format!(
                "Dictionaries name {} differently; {} by number (the Info panel's FIX tab has the names)",
                count(differ.len() as u64, "tag", "tags"),
                if differ.len() == 1 { "its column goes" } else { "their columns go" }
            ),
            format!("tags {}", differ.join(", ")),
        ));
    }
    notes
}

/// The FIX tab of the Info panel: versions, dictionaries and each tag's number.
pub fn detail(reader: &FixReader) -> Detail {
    let stats = reader.stats();
    let sep = format!(" {} ", crate::glyphs::get().middot);
    let mut head = format!("FIX{sep}{}", count(stats.messages, "message", "messages"));
    for (begin, n) in &stats.versions {
        head.push_str(&sep);
        head.push_str(&format!(
            "{begin} ({})",
            crate::numfmt::group_chrome(*n as usize)
        ));
    }
    let mut lines = vec![head];
    let dicts = &reader.layers().dicts;
    let mut used = vec!["built-in FIX 4.2, 4.4, 5.0 SP2".to_string()];
    for (i, d) in dicts.iter().enumerate().skip(1) {
        let mut said = d.name.clone();
        let summary = d.matcher.summary();
        if !summary.is_empty() {
            said.push_str(&format!(" ({summary})"));
        }
        said.push_str(&format!(
            ", {}",
            count(
                stats.applied.get(i).copied().unwrap_or(0),
                "message",
                "messages"
            )
        ));
        used.push(said);
    }
    lines.push(format!("Dictionaries: {}", used.join(&sep)));
    let names = reader.column_names();
    let list = capped_list(
        reader.tags.iter().zip(names).map(|(t, (_, column))| {
            let said = match t.names.as_slice() {
                [] => format!("tag {}{sep}in no dictionary", t.tag),
                [(name, _)] => format!("tag {}{sep}{name}", t.tag),
                many => {
                    let each: Vec<String> = many
                        .iter()
                        .map(|(name, by)| {
                            let dict = dicts.get(*by).map_or("?", |d| d.name.as_str());
                            format!("{name} ({dict})")
                        })
                        .collect();
                    format!("tag {}{sep}{}", t.tag, each.join(&sep))
                }
            };
            (column, MetaValue::Text(said))
        }),
        reader.tags.len(),
    );
    Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Fix),
        lines,
        list_title: "Tags",
        list,
        first: false,
        ..Default::default()
    }
}

/// The dictionaries a log is read with: the search path's, then each `--dict`.
pub fn layers(registry: &crate::formats::Registry, dicts: &[PathBuf]) -> Result<Layers> {
    let mut custom: Vec<Arc<dict::Dictionary>> =
        registry.fix.iter().map(|f| f.dict.clone()).collect();
    for path in dicts {
        match dict::Dictionary::load(path) {
            Ok(Some(d)) => custom.push(Arc::new(d)),
            Ok(None) => {
                return Err(FileError::new(
                    path,
                    "not a FIX dictionary. --dict takes a QuickFIX XML file, or TOML with kind = \"fix\".",
                )
                .into());
            }
            Err(e) => {
                let at = match e.line {
                    0 => String::new(),
                    line => format!("line {line}, column {}: ", e.column),
                };
                return Err(FileError::new(path, format!("{at}{}", e.message)).into());
            }
        }
    }
    Ok(Layers::new(custom))
}

/// Read a FIX log, given a piece at a time by `pieces`, into segments written through
/// `writer`.
pub(crate) fn convert(
    display: &Path,
    options: &OpenOptions,
    layers: Layers,
    writer: &Writer,
    pieces: &mut Pieces<'_>,
) -> Result<(Converted, Detail)> {
    let mut reader = FixReader::new(layers);
    let mut segments = Segments::new(options, writer);
    pieces(&mut |piece| {
        reader.push(piece);
        if let Some(df) = reader.take_batch()? {
            segments.write(&df)?;
        }
        Ok(())
    })?;
    let last = reader.finish()?;
    if reader.stats().messages == 0 {
        return Err(FileError::new(display, "no FIX messages: no line holds 8=FIX.").into());
    }
    segments.write(&last)?;
    let (lf, files) = segments.finish()?;
    let lf = reader.finish_frame(lf);
    Ok((
        Converted {
            lf,
            files,
            notes: notes(&reader),
            other_tables: Vec::new(),
        },
        detail(&reader),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A log with no messages names itself; a dictionary that is not one names the
    /// dictionary and the flag that took it.
    #[test]
    fn errors_name_the_file() {
        use crate::readers::bad_input::{assert_shape, each_names_its_file, opening};
        each_names_its_file(
            crate::FileFormat::Fix,
            &[("words.fix", b"hello there\n", "No FIX messages")],
        );
        let dir = tempfile::tempdir().unwrap();
        for (name, text, says) in [
            ("plain.toml", "a = 1\n", "--dict takes"),
            ("broken.xml", "<fix><fields><field", "Line "),
        ] {
            let dict = dir.path().join(name);
            std::fs::write(&dict, text).unwrap();
            let options = OpenOptions {
                dicts: vec![dict.clone()],
                ..Default::default()
            };
            let message = opening(
                dir.path(),
                "a.fix",
                b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01\n",
                crate::FileFormat::Fix,
                &options,
            )
            .expect("the dictionary is refused");
            eprintln!("{message}");
            assert_shape(&message, &dict);
            assert!(message.contains(says), "{message}");
        }
    }

    /// A message of `body` (fields after 9, before 10) with a correct BodyLength and
    /// CheckSum, joined by `delim`.
    pub(crate) fn message(begin: &str, body: &[(u32, &str)], delim: &str) -> String {
        let body: String = body.iter().map(|(t, v)| format!("{t}={v}\x01")).collect();
        let head = format!("8={begin}\x019={}\x01", body.len());
        let sum: u32 = head.bytes().chain(body.bytes()).map(u32::from).sum();
        format!("{head}{body}10={:03}\x01", sum % 256).replace('\x01', delim)
    }

    fn read(text: &[u8], piece: usize, layers: Layers) -> (DataFrame, FixReader) {
        let mut reader = FixReader::new(layers);
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
        let df = reader.finish_frame(df.lazy()).collect().unwrap();
        (df, reader)
    }

    fn strings(df: &DataFrame, name: &str) -> Vec<Option<String>> {
        df.column(name)
            .unwrap_or_else(|_| panic!("{name} in {:?}", df.get_column_names()))
            .str()
            .unwrap()
            .iter()
            .map(|s| s.map(String::from))
            .collect()
    }

    /// Empty cells count toward a batch: one message of 2,000 tags makes every later
    /// row 2,000 cells wide, so the batch is handed over long before `BATCH_ROWS`.
    #[test]
    fn a_wide_log_hands_over_small_batches() {
        let mut reader = FixReader::new(Layers::default());
        let tags: Vec<(u32, String)> = (20_000..22_000).map(|t| (t, "x".to_string())).collect();
        let tags: Vec<(u32, &str)> = tags.iter().map(|(t, v)| (*t, v.as_str())).collect();
        reader.push(format!("{}\n", message("FIX.4.4", &tags, "\x01")).as_bytes());
        let narrow = format!("{}\n", message("FIX.4.4", &[(35, "0")], "\x01"));
        let mut rows_at_batch = None;
        for row in 1..BATCH_ROWS {
            reader.push(narrow.as_bytes());
            if let Some(df) = reader.take_batch().unwrap() {
                rows_at_batch = Some((row, df.height()));
                break;
            }
        }
        let (row, height) = rows_at_batch.expect("a batch is handed over");
        assert!(height < BATCH_ROWS / 8, "{height} rows of 2,000 tags");
        assert_eq!(height, row + 1);
    }

    fn log() -> String {
        let order = message(
            "FIX.4.4",
            &[
                (35, "D"),
                (49, "BUYSIDE"),
                (56, "BROKERX"),
                (52, "20260102-03:04:05.123"),
                (55, "ACME"),
                (54, "1"),
                (44, "101.25"),
                (38, "500"),
            ],
            "\x01",
        );
        let fill = message(
            "FIX.4.4",
            &[
                (35, "8"),
                (49, "BROKERX"),
                (56, "BUYSIDE"),
                (52, "20260102-03:04:06"),
                (55, "ACME"),
                (54, "2"),
                (44, "101.5"),
                (453, "2"),
                (448, "PARTY1"),
                (448, "PARTY2"),
            ],
            "|",
        );
        format!("{order}\n2026-01-02 03:04:06 FIX.4.4:BROKERX->BUYSIDE IN : {fill}\nnot fix\n")
    }

    #[test]
    fn messages_read_as_rows_named_by_the_dictionary() {
        for piece in [1, 7, 4096] {
            let (df, reader) = read(log().as_bytes(), piece, Layers::default());
            assert_eq!(df.height(), 2, "piece {piece}");
            assert_eq!(reader.stats().skipped_lines, 1);
            assert_eq!(
                strings(&df, "MsgType"),
                [
                    Some("NewOrderSingle".into()),
                    Some("ExecutionReport".into())
                ]
            );
            assert_eq!(
                strings(&df, "MsgType_code"),
                [Some("D".into()), Some("8".into())]
            );
            assert_eq!(
                strings(&df, "Side"),
                [Some("Buy".into()), Some("Sell".into())]
            );
            let price: Vec<_> = df.column("Price").unwrap().f64().unwrap().iter().collect();
            assert_eq!(price, [Some(101.25), Some(101.5)]);
            let qty = df.column("OrderQty").unwrap();
            assert_eq!(qty.dtype(), &DataType::Float64);
            let sent = df.column("SendingTime").unwrap();
            assert_eq!(
                sent.dtype(),
                &DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC))
            );
            let ns: Vec<_> = sent.datetime().unwrap().physical().iter().collect();
            assert_eq!(ns[0], dict::parse_timestamp("20260102-03:04:05.123"));
            assert_eq!(ns[1], dict::parse_timestamp("20260102-03:04:06"));
            assert_eq!(strings(&df, "PartyID"), [None, Some("PARTY1".into())]);
            let rest = df.column("PartyID_rest").unwrap().list().unwrap();
            assert!(rest.get_as_series(0).is_none());
            assert_eq!(rest.get_as_series(1).unwrap().len(), 1);
            assert_eq!(
                strings(&df, "prefix"),
                [
                    None,
                    Some("2026-01-02 03:04:06 FIX.4.4:BROKERX->BUYSIDE IN".into())
                ]
            );
            assert_eq!(strings(&df, "direction"), [None, Some("in".into())]);
            assert_eq!(
                strings(&df, "session"),
                [None, Some("FIX.4.4:BROKERX->BUYSIDE".into())]
            );
            for check in ["body_length_ok", "checksum_ok"] {
                let ok: Vec<_> = df.column(check).unwrap().bool().unwrap().iter().collect();
                assert_eq!(ok, [Some(true), Some(true)], "{check}");
            }
        }
    }

    #[test]
    fn bad_checks_are_false_and_a_cut_message_null() {
        let good = message("FIX.4.2", &[(35, "0")], "|");
        let bad_sum = good.replace("10=", "10=9");
        let bad_len = good.replace("9=5", "9=6");
        let text = format!("{bad_sum}\n{bad_len}\n8=FIX.4.2|9=5|35=0|\n");
        let (df, reader) = read(text.as_bytes(), 3, Layers::default());
        let sum: Vec<_> = df
            .column("checksum_ok")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect();
        let len: Vec<_> = df
            .column("body_length_ok")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(sum, [Some(false), Some(false), None]);
        assert_eq!(len, [Some(true), Some(false), None]);
        assert_eq!(reader.stats().incomplete, 1);
        assert!(df.get_column_names().iter().all(|c| *c != "prefix"));
    }

    #[test]
    fn a_length_tagged_value_may_hold_the_delimiter() {
        let text = message(
            "FIX.4.4",
            &[(35, "A"), (95, "7"), (96, "a\x01b|c\nd"), (58, "hi")],
            "\x01",
        );
        let (df, _) = read(text.as_bytes(), 2, Layers::default());
        assert_eq!(df.height(), 1);
        assert_eq!(strings(&df, "RawData"), [Some("a\x01b|c\nd".into())]);
        assert_eq!(strings(&df, "Text"), [Some("hi".into())]);
        let ok: Vec<_> = df
            .column("checksum_ok")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(ok, [Some(true)]);
    }

    #[test]
    fn counterparties_name_custom_tags_their_own_way() {
        let x = dict::Dictionary::parse_toml(
            "name = \"acme.x\"\nkind = \"fix\"\nmatch = { sender = \"X\" }\ntags = { 9001 = \"AlgoName\", 9002 = { name = \"Urgency\", type = \"int\", enum = { 1 = \"Low\" } } }",
            None,
        )
        .unwrap();
        let y = dict::Dictionary::parse_toml(
            "name = \"acme.y\"\nkind = \"fix\"\nmatch = { sender = \"Y\" }\ntags = { 9001 = \"Strategy\" }",
            None,
        )
        .unwrap();
        let layers = Layers::new(vec![Arc::new(x), Arc::new(y)]);
        let text = [
            message(
                "FIX.4.4",
                &[(35, "D"), (49, "X"), (9001, "VWAP"), (9002, "1")],
                "|",
            ),
            message(
                "FIX.4.4",
                &[(35, "D"), (49, "Y"), (9001, "TWAP"), (9003, "z")],
                "|",
            ),
        ]
        .join("\n");
        let (df, reader) = read(text.as_bytes(), 5, layers);
        assert_eq!(
            strings(&df, "9001"),
            [Some("VWAP".into()), Some("TWAP".into())]
        );
        assert_eq!(strings(&df, "Urgency"), [Some("Low".into()), None]);
        assert_eq!(strings(&df, "Urgency_code"), [Some("1".into()), None]);
        assert_eq!(strings(&df, "9003"), [None, Some("z".into())]);
        assert_eq!(reader.stats().applied, [2, 1, 1]);
        let detail = detail(&reader);
        let row = detail.list.iter().find(|(k, _)| k == "9001").unwrap();
        let MetaValue::Text(said) = &row.1 else {
            panic!()
        };
        assert!(
            said.contains("AlgoName (acme.x)") && said.contains("Strategy (acme.y)"),
            "{said}"
        );
        assert!(
            notes(&reader)
                .iter()
                .any(|n| n.summary.contains("differently"))
        );
    }

    #[test]
    fn sniffing() {
        assert!(looks_like(log().as_bytes()));
        assert!(looks_like(b"in: 8=FIX.4.2^A9=12^A35=0^A10=000^A"));
        assert!(!looks_like(b"the 8=FIX tag starts a message"));
        assert!(!looks_like(b"a,b\n18=FIX.4.4|9=1\n"));
        assert_eq!(direction("12:00 OUT >"), Some("out"));
        assert_eq!(direction("received"), Some("in"));
        assert_eq!(session("[FIX.4.4:A->B]"), Some("FIX.4.4:A->B".into()));
        assert_eq!(session("a -> b"), None);
    }
}

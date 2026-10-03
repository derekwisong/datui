//! CAN logs written by `candump`, decoded with DBC files.
//!
//! Each line is one frame: `(1436509052.249713) can0 123#DEADBEEF` as `candump -l` and
//! `-L` write it (`##` for CAN FD, `#R` for a remote request), or the default
//! `can0  123   [4]  DE AD BE EF`, with or without a `(timestamp)` in front. One pass
//! records where each frame's line starts and its id and interface; the raw table is
//! then read line by line where it is shown, from a map of the file.
//!
//! With a DBC file ([`crate::dbc`]) that names the log's messages, each message is a
//! table of its own: `ts` and a column per signal, with units, value names as text,
//! and a multiplexed signal null in the frames its multiplexer does not select. The
//! home screen lists them, with the raw `frames` and a long `signals` table of every
//! decoded value (`ts`, `message`, `signal`, `value`, `unit`).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::eyre;

use crate::error_display::{FileError, in_file};
use polars::prelude::*;

use crate::dbc::{Dbc, Message, Mux, Signal};
use crate::fixed_records::{Bytes, ColumnLayout, Logical, Physical};
use crate::indexed::Offsets;
use crate::model_files::MetaValue;
use crate::sqlite::Table;
use crate::text_formats::Detail;

/// What datui does with a candump log: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Text,
        trusted: crate::readers::Trusted {
            tables: true,
            ..crate::readers::EVERYWHERE
        },
    }],
    tables: Some(|path| {
        listed(path).ok_or_else(|| color_eyre::eyre::eyre!("Open the log to list its tables."))
    }),
    ..crate::readers::BASE
};

/// The longest line read as a frame; a longer one is not one.
const MAX_LINE: usize = 4096;
/// Interfaces told apart; past this many, the rest share the last.
const MAX_INTERFACES: usize = 255;
/// Long-table parts: message signals unpivoted.
const MAX_LONG_PARTS: usize = 10_000;

/// The raw table, and the long table of decoded values.
pub const FRAMES: &str = "frames";
pub const SIGNALS: &str = "signals";

/// One frame, as a line gives it.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame<'a> {
    /// Microseconds, from the epoch or from the start of the capture.
    pub ts: Option<i64>,
    pub iface: &'a str,
    pub id: u32,
    pub extended: bool,
    pub fd: bool,
    /// CAN FD flags (BRS, ESI).
    pub flags: Option<u8>,
    pub remote: bool,
    pub error: bool,
    pub dlc: u8,
    pub data: Vec<u8>,
}

fn hex_bytes(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || text.len() > 128 {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

/// An id as written: three hex digits standard, eight extended.
fn parse_id(text: &str) -> Option<(u32, bool)> {
    if !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    match text.len() {
        1..=3 => Some((u32::from_str_radix(text, 16).ok()?, false)),
        8 => Some((u32::from_str_radix(text, 16).ok()?, true)),
        _ => None,
    }
}

/// A timestamp in parentheses: seconds with a fraction, or a date and time.
fn parse_ts(text: &str) -> Option<i64> {
    if let Some((s, frac)) = text.split_once('.')
        && !s.is_empty()
        && s.bytes().all(|b| b.is_ascii_digit())
        && !frac.is_empty()
        && frac.bytes().all(|b| b.is_ascii_digit())
    {
        let secs: i64 = s.parse().ok()?;
        let digits = &frac[..frac.len().min(6)];
        let micros: i64 = digits.parse::<i64>().ok()? * 10i64.pow(6 - digits.len() as u32);
        return secs.checked_mul(1_000_000)?.checked_add(micros);
    }
    // `candump -ta` writes a local date and time: `2024-01-31 08:15:00.123456`.
    let parsed = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").ok()?;
    Some(parsed.and_utc().timestamp_micros())
}

/// Parse one line, as `candump -l` writes it or as `candump` prints it.
pub fn parse_line(line: &str) -> Option<Frame<'_>> {
    let line = line.trim();
    if line.is_empty() || line.len() > MAX_LINE {
        return None;
    }
    let (ts, rest) = match line.strip_prefix('(') {
        Some(after) => {
            let (inside, rest) = after.split_once(')')?;
            (Some(parse_ts(inside.trim())?), rest.trim_start())
        }
        None => (None, line),
    };
    let mut words = rest.split_whitespace();
    let iface = words.next()?;
    let next = words.next()?;
    if let Some((id, frame)) = next.split_once('#') {
        return parse_compact(ts, iface, id, frame);
    }
    // The printed form: an id, then `[n]` and the bytes, maybe after `-x` fields.
    let mut id_word = next;
    let mut dlc_word = words.next()?;
    let mut guard = 0;
    while !(dlc_word.starts_with('[') && dlc_word.ends_with(']')) {
        id_word = dlc_word;
        dlc_word = words.next()?;
        guard += 1;
        if guard > 4 {
            return None;
        }
    }
    let (id, extended) = parse_id(id_word)?;
    let dlc: u8 = dlc_word[1..dlc_word.len() - 1].parse().ok()?;
    let rest: Vec<&str> = words.collect();
    if rest.first() == Some(&"remote") {
        return Some(Frame {
            ts,
            iface,
            id,
            extended,
            fd: false,
            flags: None,
            remote: true,
            error: false,
            dlc,
            data: Vec::new(),
        });
    }
    let data: Vec<u8> = rest
        .iter()
        .take(dlc as usize)
        .map(|b| {
            (b.len() == 2)
                .then(|| u8::from_str_radix(b, 16).ok())
                .flatten()
        })
        .collect::<Option<_>>()?;
    if data.len() != dlc as usize || dlc > 64 {
        return None;
    }
    Some(Frame {
        ts,
        iface,
        id,
        extended,
        fd: dlc > 8,
        flags: None,
        remote: false,
        error: extended && id & 0x2000_0000 != 0,
        dlc,
        data,
    })
}

fn parse_compact<'a>(ts: Option<i64>, iface: &'a str, id: &str, frame: &str) -> Option<Frame<'a>> {
    let (id, extended) = parse_id(id)?;
    let error = extended && id & 0x2000_0000 != 0;
    let mut out = Frame {
        ts,
        iface,
        id,
        extended,
        fd: false,
        flags: None,
        remote: false,
        error,
        dlc: 0,
        data: Vec::new(),
    };
    if let Some(fd) = frame.strip_prefix('#') {
        // CAN FD: a flags digit, then the data.
        if fd.starts_with('#') {
            return None;
        }
        let mut chars = fd.chars();
        let flags = chars.next()?.to_digit(16)? as u8;
        let data = hex_bytes(chars.as_str())?;
        out.fd = true;
        out.flags = Some(flags);
        out.dlc = data.len() as u8;
        out.data = data;
        return Some(out);
    }
    if let Some(remote) = frame.strip_prefix('R').or_else(|| frame.strip_prefix('r')) {
        out.remote = true;
        out.dlc = if remote.is_empty() {
            0
        } else {
            remote.parse().ok().filter(|d| *d <= 8)?
        };
        return Some(out);
    }
    // `_X` after the data: a length code past 8 for eight bytes of data.
    let (data, code) = match frame.split_once('_') {
        Some((data, code)) => (data, u8::from_str_radix(code, 16).ok()),
        None => (frame, None),
    };
    let data = hex_bytes(data)?;
    if data.len() > 8 {
        return None;
    }
    out.dlc = code.unwrap_or(data.len() as u8);
    out.data = data;
    Some(out)
}

/// Whether `head` begins a candump log: its first line that is not blank is a frame.
pub fn looks_like(head: &[u8]) -> bool {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(first) = lines.next() else {
        return false;
    };
    // A head cut mid-line is judged on the frame so far: a byte cut in half is
    // left off.
    let cut = !text.contains('\n');
    parse_line(first).is_some()
        || (cut
            && first
                .char_indices()
                .last()
                .is_some_and(|(at, _)| parse_line(&first[..at]).is_some()))
}

/// What one pass over a candump log found.
#[derive(Debug, Default)]
pub struct Index {
    /// Where each frame's line starts.
    pub offsets: Arc<Offsets>,
    /// Each frame's id, with bit 31 for an extended one.
    pub keys: Vec<u32>,
    /// Each frame's interface, as an index into `interfaces`.
    pub ifaces: Vec<u8>,
    pub interfaces: Vec<String>,
    /// Whether the timestamps are from the epoch (`-l`), not from the capture's start.
    pub absolute: bool,
    /// Lines that are not frames, passed over.
    pub skipped: usize,
    pub past_limit: usize,
}

/// Index the candump log in `data`: one pass, start to end.
pub fn index(data: &[u8]) -> std::result::Result<Index, String> {
    let mut offsets = Offsets::for_file(data.len());
    let mut index = Index::default();
    let mut ifaces: HashMap<String, u8> = HashMap::new();
    let mut first_ts = None;
    let mut at = 0usize;
    while at < data.len() {
        let end = memchr::memchr(b'\n', &data[at..]).map_or(data.len(), |i| at + i);
        let line = &data[at..end];
        let parsed = (line.len() <= MAX_LINE)
            .then(|| std::str::from_utf8(line).ok())
            .flatten()
            .and_then(parse_line);
        match parsed {
            Some(frame) if offsets.len() < crate::indexed::MAX_RECORDS => {
                if first_ts.is_none() {
                    first_ts = frame.ts;
                }
                let iface = match ifaces.get(frame.iface) {
                    Some(&i) => i,
                    None if index.interfaces.len() < MAX_INTERFACES => {
                        let i = index.interfaces.len() as u8;
                        index.interfaces.push(frame.iface.to_string());
                        ifaces.insert(frame.iface.to_string(), i);
                        i
                    }
                    None => (MAX_INTERFACES - 1) as u8,
                };
                offsets.push(at);
                index
                    .keys
                    .push(frame.id | (u32::from(frame.extended) << 31));
                index.ifaces.push(iface);
            }
            Some(_) => index.past_limit += 1,
            None if line.iter().all(|b| b.is_ascii_whitespace()) => {}
            None => index.skipped += 1,
        }
        at = end + 1;
    }
    if offsets.is_empty() {
        return Err("no line is a CAN frame as candump writes them".into());
    }
    offsets.shrink();
    index.keys.shrink_to_fit();
    index.ifaces.shrink_to_fit();
    index.offsets = Arc::new(offsets);
    // Seconds since 2001 and later are wall-clock time; less is time since the start.
    index.absolute = first_ts.is_some_and(|ts| ts >= 978_307_200_000_000);
    Ok(index)
}

/// The line of frame `row`.
fn line_of(bytes: &[u8], at: usize) -> &str {
    let end = memchr::memchr(b'\n', &bytes[at..]).map_or(bytes.len(), |i| at + i);
    std::str::from_utf8(&bytes[at..end.min(at + MAX_LINE)]).unwrap_or_default()
}

fn ts_dtype(absolute: bool) -> DataType {
    if absolute {
        DataType::Datetime(TimeUnit::Microseconds, None)
    } else {
        DataType::Duration(TimeUnit::Microseconds)
    }
}

fn ts_series(values: Vec<Option<i64>>, absolute: bool) -> Series {
    let ca: Int64Chunked = values.into_iter().collect();
    if absolute {
        ca.into_datetime(TimeUnit::Microseconds, None).into_series()
    } else {
        ca.into_duration(TimeUnit::Microseconds).into_series()
    }
}

/// The raw table: a row per frame, read from its line where it is shown.
pub struct RawFrames {
    bytes: Arc<Bytes>,
    offsets: Arc<Offsets>,
    absolute: bool,
    schema: SchemaRef,
}

impl RawFrames {
    pub fn new(bytes: Arc<Bytes>, index: &Index) -> Self {
        let schema = Schema::from_iter([
            Field::new("ts".into(), ts_dtype(index.absolute)),
            Field::new("iface".into(), DataType::String),
            Field::new("id".into(), DataType::String),
            Field::new("ext".into(), DataType::Boolean),
            Field::new("dlc".into(), DataType::UInt8),
            Field::new("data".into(), DataType::Binary),
            Field::new("fd".into(), DataType::Boolean),
            Field::new("flags".into(), DataType::UInt8),
            Field::new("kind".into(), DataType::String),
        ]);
        Self {
            bytes,
            offsets: index.offsets.clone(),
            absolute: index.absolute,
            schema: Arc::new(schema),
        }
    }

    pub fn rows(&self) -> usize {
        self.offsets.len().min(crate::row_index::MAX_ROWS)
    }

    fn column(&self, column: usize, rows: impl Iterator<Item = usize>) -> PolarsResult<Column> {
        self.bytes.still_whole()?;
        let bytes = self.bytes.as_slice();
        let frames: Vec<Option<Frame<'_>>> = rows
            .map(|r| parse_line(line_of(bytes, self.offsets.get(r))))
            .collect();
        let name: PlSmallStr = self
            .schema
            .get_at_index(column)
            .expect("a column")
            .0
            .clone();
        let series = match column {
            0 => ts_series(
                frames
                    .iter()
                    .map(|f| f.as_ref().and_then(|f| f.ts))
                    .collect(),
                self.absolute,
            ),
            1 => frames
                .iter()
                .map(|f| f.as_ref().map(|f| f.iface))
                .collect::<StringChunked>()
                .into_series(),
            2 => frames
                .iter()
                .map(|f| {
                    f.as_ref().map(|f| {
                        if f.extended {
                            format!("{:08X}", f.id)
                        } else {
                            format!("{:03X}", f.id)
                        }
                    })
                })
                .collect::<StringChunked>()
                .into_series(),
            3 => frames
                .iter()
                .map(|f| f.as_ref().map(|f| f.extended))
                .collect::<BooleanChunked>()
                .into_series(),
            4 => frames
                .iter()
                .map(|f| f.as_ref().map(|f| f.dlc))
                .collect::<UInt8Chunked>()
                .into_series(),
            5 => frames
                .iter()
                .map(|f| f.as_ref().map(|f| f.data.as_slice()))
                .collect::<BinaryChunked>()
                .into_series(),
            6 => frames
                .iter()
                .map(|f| f.as_ref().map(|f| f.fd))
                .collect::<BooleanChunked>()
                .into_series(),
            7 => frames
                .iter()
                .map(|f| f.as_ref().and_then(|f| f.flags))
                .collect::<UInt8Chunked>()
                .into_series(),
            _ => frames
                .iter()
                .map(|f| {
                    f.as_ref().map(|f| {
                        if f.error {
                            "error"
                        } else if f.remote {
                            "remote"
                        } else {
                            "data"
                        }
                    })
                })
                .collect::<StringChunked>()
                .into_series(),
        };
        Ok(series.with_name(name).into_column())
    }

    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        let start = start.min(self.rows());
        let len = len.min(self.rows() - start);
        let columns = (0..self.schema.len())
            .map(|c| self.column(c, start..start + len))
            .collect::<PolarsResult<Vec<_>>>()?;
        DataFrame::new(len, columns)
    }
}

impl crate::row_index::RowSource for RawFrames {
    fn height(&self) -> usize {
        self.rows()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        let rows = crate::row_index::checked(index, self.rows())?;
        self.column(column, rows.iter().map(|&r| r as usize))
    }
}

impl crate::pushdown::Windowed for RawFrames {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(self.collect_window(start, len)?.lazy())
    }
}

/// One message's frames, decoded by its DBC entry.
pub struct Decoded {
    bytes: Arc<Bytes>,
    offsets: Arc<Offsets>,
    /// The raw rows of this message's frames.
    rows: Arc<Vec<u32>>,
    message: Arc<Message>,
    absolute: bool,
    /// Value names as text, or every value as a number (for the long table).
    named: bool,
    schema: SchemaRef,
}

/// How a signal's values are typed: an integer when factor and offset keep it one, a
/// float otherwise, its names as text when it has them.
fn signal_layout(signal: &Signal, named: bool) -> ColumnLayout {
    let whole = signal.float == 0 && signal.factor == 1.0 && signal.offset.fract() == 0.0;
    let physical = if signal.signed || signal.offset < 0.0 {
        Physical::Signed(8)
    } else {
        Physical::Unsigned(8)
    };
    let mut layout = ColumnLayout::new(&signal.name, 0, 8, physical, 8);
    layout.logical = if named && !signal.values.is_empty() && whole && signal.offset == 0.0 {
        Logical::Enum(Arc::new(
            signal.values.iter().map(|(k, v)| (*k, v.clone())).collect(),
        ))
    } else if whole && signal.offset == 0.0 {
        Logical::Plain
    } else {
        Logical::Linear {
            factor: signal.factor,
            offset: signal.offset,
        }
    };
    layout
}

impl Decoded {
    pub fn new(
        bytes: Arc<Bytes>,
        index: &Index,
        rows: Arc<Vec<u32>>,
        message: Arc<Message>,
        named: bool,
    ) -> PolarsResult<Self> {
        let mut fields = vec![Field::new("ts".into(), ts_dtype(index.absolute))];
        for s in &message.signals {
            let dtype = if s.float != 0 {
                DataType::Float64
            } else {
                signal_layout(s, named).dtype()
            };
            fields.push(Field::new(s.name.as_str().into(), dtype));
        }
        let schema: Schema = fields.into_iter().collect();
        polars_ensure!(
            schema.len() == message.signals.len() + 1,
            Duplicate: "{} has two signals of one name, or one named ts", message.name
        );
        Ok(Self {
            bytes,
            offsets: index.offsets.clone(),
            rows,
            message,
            absolute: index.absolute,
            named,
            schema: Arc::new(schema),
        })
    }

    pub fn height(&self) -> usize {
        self.rows.len().min(crate::row_index::MAX_ROWS)
    }

    fn column(&self, column: usize, rows: impl Iterator<Item = usize>) -> PolarsResult<Column> {
        self.bytes.still_whole()?;
        let bytes = self.bytes.as_slice();
        let frames: Vec<Option<Frame<'_>>> = rows
            .map(|r| parse_line(line_of(bytes, self.offsets.get(self.rows[r] as usize))))
            .collect();
        let name = self
            .schema
            .get_at_index(column)
            .expect("a column")
            .0
            .clone();
        if column == 0 {
            let ts = frames
                .iter()
                .map(|f| f.as_ref().and_then(|f| f.ts))
                .collect();
            return Ok(ts_series(ts, self.absolute).with_name(name).into_column());
        }
        let signal = &self.message.signals[column - 1];
        let multiplexer = self
            .message
            .signals
            .iter()
            .find(|s| s.mux == Mux::Multiplexer);
        let present = |f: &Frame<'_>| {
            let mux = multiplexer.and_then(|m| crate::dbc::raw(m, &f.data));
            crate::dbc::present(signal, mux)
        };
        let series = if signal.float != 0 {
            frames
                .iter()
                .map(|f| {
                    let f = f.as_ref()?;
                    present(f).then(|| crate::dbc::physical(signal, &f.data))?
                })
                .collect::<Float64Chunked>()
                .into_series()
        } else {
            let ints: Vec<Option<i128>> = frames
                .iter()
                .map(|f| {
                    let f = f.as_ref()?;
                    present(f).then(|| crate::dbc::integer(signal, &f.data))?
                })
                .collect();
            crate::fixed_records::integers(&signal_layout(signal, self.named), ints)?
        };
        Ok(series.with_name(name).into_column())
    }

    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        let start = start.min(self.height());
        let len = len.min(self.height() - start);
        let columns = (0..self.schema.len())
            .map(|c| self.column(c, start..start + len))
            .collect::<PolarsResult<Vec<_>>>()?;
        DataFrame::new(len, columns)
    }
}

impl crate::row_index::RowSource for Decoded {
    fn height(&self) -> usize {
        Decoded::height(self)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        let rows = crate::row_index::checked(index, self.height())?;
        self.column(column, rows.iter().map(|&r| r as usize))
    }
}

impl crate::pushdown::Windowed for Decoded {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(self.collect_window(start, len)?.lazy())
    }
}

// --- DBC layers ----------------------------------------------------------------------

/// The DBC files a log is read with, in order: the search path's, then each `--dict`.
#[derive(Debug, Clone, Default)]
pub struct Layers {
    pub dbcs: Vec<Arc<Dbc>>,
}

impl Layers {
    /// The search path's DBC files and each `--dict`.
    pub fn new(registry: &crate::formats::Registry, dicts: &[PathBuf]) -> Result<Self> {
        let mut dbcs: Vec<Arc<Dbc>> = registry.dbc.iter().map(|f| f.dbc.clone()).collect();
        for path in dicts {
            match crate::dbc::load(path) {
                Ok(Some(d)) => dbcs.push(Arc::new(d)),
                Ok(None) => {
                    return Err(FileError::new(
                        path,
                        "not a DBC dictionary. --dict takes a .dbc file, or TOML with kind = \"dbc\".",
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
        Ok(Self { dbcs })
    }

    fn files(&self) -> Vec<PathBuf> {
        self.dbcs.iter().filter_map(|d| d.path.clone()).collect()
    }
}

/// What a log's frames resolve to under its layers: each message with frames, the
/// rows of its frames.
#[derive(Debug, Default)]
pub struct Listing {
    pub layers: Layers,
    /// Message tables by name: the message and its frames' raw rows.
    pub messages: BTreeMap<String, (Arc<Message>, Arc<Vec<u32>>)>,
    /// Frames no DBC names.
    pub unknown: usize,
}

impl Listing {
    pub fn resolve(index: &Index, layers: Layers) -> Self {
        // For each interface and id, the last layer that applies and names it.
        let mut known: HashMap<(u8, u32), Option<(usize, usize)>> = HashMap::new();
        let lookup = |iface: u8, key: u32| -> Option<(usize, usize)> {
            let name = index.interfaces.get(iface as usize)?;
            let (id, extended) = (key & 0x7FFF_FFFF, key >> 31 == 1);
            layers.dbcs.iter().enumerate().rev().find_map(|(l, dbc)| {
                if !dbc.applies(name) {
                    return None;
                }
                dbc.messages
                    .iter()
                    .position(|m| m.id == id && m.extended == extended)
                    .map(|m| (l, m))
            })
        };
        let mut rows: HashMap<(usize, usize), Vec<u32>> = HashMap::new();
        let mut unknown = 0;
        for (row, (&key, &iface)) in index.keys.iter().zip(&index.ifaces).enumerate() {
            let found = *known
                .entry((iface, key))
                .or_insert_with(|| lookup(iface, key));
            match found {
                Some(at) => rows.entry(at).or_default().push(row as u32),
                None => unknown += 1,
            }
        }
        let mut messages = BTreeMap::new();
        for ((l, m), rows) in rows {
            let message = &layers.dbcs[l].messages[m];
            let mut name = message.name.clone();
            if name == FRAMES || name == SIGNALS || messages.contains_key(&name) {
                name = format!("{name}.{}", layers.dbcs[l].name);
            }
            messages.insert(name, (Arc::new(message.clone()), Arc::new(rows)));
        }
        Self {
            layers,
            messages,
            unknown,
        }
    }

    pub fn tables(&self) -> Vec<Table> {
        let mut tables = vec![Table {
            name: FRAMES.to_string(),
            kind: "frames".to_string(),
            internal: false,
            columns: [
                "ts", "iface", "id", "ext", "dlc", "data", "fd", "flags", "kind",
            ]
            .iter()
            .map(|c| (c.to_string(), String::new()))
            .collect(),
        }];
        if self.messages.is_empty() {
            return tables;
        }
        tables.push(Table {
            name: SIGNALS.to_string(),
            kind: "signals".to_string(),
            internal: false,
            columns: ["ts", "message", "signal", "value", "unit"]
                .iter()
                .map(|c| (c.to_string(), String::new()))
                .collect(),
        });
        for (name, (message, _)) in &self.messages {
            tables.push(Table {
                name: name.clone(),
                kind: "message".to_string(),
                internal: false,
                columns: std::iter::once("ts".to_string())
                    .chain(message.signals.iter().map(|s| s.name.clone()))
                    .map(|c| (c, String::new()))
                    .collect(),
            });
        }
        tables
    }
}

/// The index of the candump log at `path`, made by one pass or kept from one.
pub fn indexed(path: &Path) -> Result<(Arc<Bytes>, Arc<Index>)> {
    let bytes = Arc::new(Bytes::map(path).map_err(|e| in_file(path, e.into()))?);
    let index = crate::indexed::cached(path, || index(bytes.as_slice()))
        .map_err(|e| FileError::new(path, e))?;
    Ok((bytes, index))
}

/// The tables the log at `path` was last listed with, for the home screen.
pub fn listed(path: &Path) -> Option<Vec<Table>> {
    crate::indexed::peek::<Listing>(path).map(|l| l.tables())
}

/// What the Info panel's CAN tab says.
fn detail(index: &Index, listing: &Listing) -> Detail {
    let group = crate::numfmt::group_chrome;
    let mut lines = vec![
        format!("Frames: {}", group(index.keys.len())),
        format!("Interfaces: {}", index.interfaces.join(", ")),
        format!(
            "Timestamps: {}",
            if index.absolute {
                "wall clock"
            } else {
                "from the start of the capture"
            }
        ),
    ];
    if listing.layers.dbcs.is_empty() {
        lines.push("DBC: none; --dict FILE or the format search path decodes signals".into());
    } else {
        for dbc in &listing.layers.dbcs {
            lines.push(format!(
                "DBC: {}{}, {} messages",
                dbc.name,
                dbc.interface
                    .as_ref()
                    .map(|i| format!(" on {i}"))
                    .unwrap_or_default(),
                dbc.messages.len()
            ));
        }
        lines.push(format!("Frames no DBC names: {}", group(listing.unknown)));
    }
    let list = listing.messages.iter().map(|(name, (message, rows))| {
        (
            name.clone(),
            MetaValue::Text(format!(
                "id {}, {} frames, {} signals{}",
                if message.extended {
                    format!("{:08X}", message.id)
                } else {
                    format!("{:03X}", message.id)
                },
                group(rows.len()),
                message.signals.len(),
                message
                    .comment
                    .as_ref()
                    .map(|c| format!(": {c}"))
                    .unwrap_or_default()
            )),
        )
    });
    Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Candump),
        lines,
        list_title: "Messages",
        list: crate::text_formats::capped_list(list, listing.messages.len()),
        first: false,
        ..Default::default()
    }
}

/// What opening a candump log finds.
pub enum Open {
    Table {
        lf: Box<LazyFrame>,
        opened: Box<crate::members::Opened>,
    },
    Several(Vec<String>),
}

/// Open the candump log at `path` with `layers`: the table `wanted` names, its frames
/// when no DBC names its messages, or the list.
pub fn open(path: &Path, wanted: Option<&str>, layers: Layers) -> Result<Open> {
    let (bytes, index) = indexed(path)?;
    // A message opened from the home screen's list is read with the DBC files the list
    // was made with, `--dict` among them.
    let layers = match crate::indexed::peek::<Listing>(path) {
        Some(last) if layers.files().is_empty() || last.layers.files() == layers.files() => {
            last.layers.clone()
        }
        _ => layers,
    };
    let listing = crate::indexed::cached::<Listing, std::convert::Infallible>(path, || {
        Ok(Listing::resolve(&index, layers.clone()))
    })
    .unwrap_or_else(|never| match never {});
    let listing = if listing.layers.files() == layers.files() {
        listing
    } else {
        // The DBC files changed: resolve again and keep that.
        crate::indexed::forget::<Listing>(path);
        crate::indexed::cached::<Listing, std::convert::Infallible>(path, || {
            Ok(Listing::resolve(&index, layers))
        })
        .unwrap_or_else(|never| match never {})
    };
    let tables = listing.tables();
    // Without a DBC file only the frames are read: a message or the signals asked for
    // by name wants one.
    if let Some(wanted) = wanted
        && listing.layers.dbcs.is_empty()
        && !tables.iter().any(|t| t.name.eq_ignore_ascii_case(wanted))
    {
        return Err(FileError::new(
            path,
            format!(
                "no table \"{wanted}\": with no dictionary only its {FRAMES} are read. --dict names one that decodes its messages."
            ),
        )
        .into());
    }
    let picked = match crate::members::pick(tables.clone(), wanted, path, "")? {
        crate::sqlite::Pick::One(table) => table.name,
        crate::sqlite::Pick::Several(tables) => {
            return Ok(Open::Several(tables.into_iter().map(|t| t.name).collect()));
        }
    };
    let mut notes: Vec<String> = Vec::new();
    if index.skipped > 0 {
        notes.push(format!(
            "{} lines that are not frames were passed over.",
            crate::numfmt::group_chrome(index.skipped)
        ));
    }
    if index.past_limit > 0 {
        notes.push(format!(
            "{} frames past the first {} are left out.",
            crate::numfmt::group_chrome(index.past_limit),
            crate::numfmt::group_chrome(crate::indexed::MAX_RECORDS)
        ));
    }
    for dbc in &listing.layers.dbcs {
        notes.extend(dbc.notes.iter().cloned());
    }
    let mut opened = crate::members::Opened {
        detail: Some(Arc::new(detail(&index, &listing))),
        other_tables: crate::members::others(&tables, &picked),
        notes: notes
            .into_iter()
            .map(|n| crate::text_formats::note(n, "the log".to_string()))
            .collect(),
        ..Default::default()
    };
    let lf = if picked == FRAMES {
        let raw = Arc::new(RawFrames::new(bytes, &index));
        opened.window = Some((raw.clone(), raw.rows()));
        crate::row_index::lazy(&raw)
    } else if picked == SIGNALS {
        long_table(&bytes, &index, &listing, &mut opened)?
    } else {
        let (message, rows) = listing
            .messages
            .get(&picked)
            .ok_or_else(|| FileError::new(path, format!("no message \"{picked}\"")))?;
        let decoded = Arc::new(
            Decoded::new(bytes, &index, rows.clone(), message.clone(), true)
                .map_err(|e| eyre!("{e}"))?,
        );
        opened.units = message
            .signals
            .iter()
            .filter(|s| !s.unit.is_empty())
            .map(|s| (s.name.clone(), s.unit.clone()))
            .collect();
        opened.window = Some((decoded.clone(), decoded.height()));
        crate::row_index::lazy(&decoded)
    };
    Ok(Open::Table {
        lf: Box::new(lf),
        opened: Box::new(opened),
    })
}

/// Every decoded value, one row each: each message's signals unpivoted, in time order.
fn long_table(
    bytes: &Arc<Bytes>,
    index: &Index,
    listing: &Listing,
    opened: &mut crate::members::Opened,
) -> Result<LazyFrame> {
    let mut parts = Vec::new();
    let mut left_out = 0usize;
    for (name, (message, rows)) in &listing.messages {
        let decoded = Arc::new(
            Decoded::new(bytes.clone(), index, rows.clone(), message.clone(), false)
                .map_err(|e| eyre!("{e}"))?,
        );
        let lf = crate::row_index::lazy(&decoded);
        for signal in &message.signals {
            if parts.len() >= MAX_LONG_PARTS {
                left_out += 1;
                continue;
            }
            parts.push(
                lf.clone()
                    .select([
                        col("ts"),
                        lit(name.as_str()).alias("message"),
                        lit(signal.name.as_str()).alias("signal"),
                        col(signal.name.as_str())
                            .cast(DataType::Float64)
                            .alias("value"),
                        lit(signal.unit.as_str()).alias("unit"),
                    ])
                    .filter(col("value").is_not_null()),
            );
        }
    }
    if left_out > 0 {
        opened.notes.push(crate::text_formats::note(
            format!(
                "{} signals past the first {MAX_LONG_PARTS} are left out of this table; each message's own table has them.",
                crate::numfmt::group_chrome(left_out)
            ),
            "the dictionaries".to_string(),
        ));
    }
    if parts.is_empty() {
        return Err(eyre!(
            "no signals to decode: no frame of the log is a message of the dictionaries"
        ));
    }
    let all = concat(parts, UnionArgs::default())?;
    Ok(all.sort(
        ["ts"],
        SortMultipleOptions::default()
            .with_maintain_order(true)
            .with_nulls_last(true),
    ))
}

/// The scan of a candump log: its frames, or with DBC files that name its messages,
/// the table `--table` names or the list of them. The pass that indexes the log is
/// kept, as a flight log's is.
fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let file = input.path();
    let layers = Layers::new(input.formats, &input.options.dicts)?;
    Ok(match open(file, input.options.table.as_deref(), layers)? {
        Open::Table { lf, opened } => {
            input.report.opened = Some(Arc::new(*opened));
            (*lf).into()
        }
        Open::Several(tables) => crate::scan::Scan::Tables {
            file: file.to_path_buf(),
            tables,
            format: input.format,
        },
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A log of no frames names itself; a table that wants a DBC file says the flag
    /// that gives one, and a DBC file that is not one names the DBC file.
    #[test]
    fn errors_name_the_file() {
        use crate::readers::bad_input::{assert_shape, each_names_its_file, opening};
        each_names_its_file(
            crate::FileFormat::Candump,
            &[("text.log", b"hello there\n", "No line is a CAN frame")],
        );
        let dir = tempfile::tempdir().unwrap();
        let log = b"(1436509052.249713) can0 123#DEADBEEF\n";
        let dbc = dir.path().join("plain.toml");
        std::fs::write(&dbc, "a = 1\n").unwrap();
        for (options, named, says) in [
            (
                crate::OpenOptions {
                    table: Some("Engine".into()),
                    ..Default::default()
                },
                dir.path().join("a.log"),
                "--dict names one",
            ),
            (
                crate::OpenOptions {
                    dicts: vec![dbc.clone()],
                    ..Default::default()
                },
                dbc.clone(),
                "--dict takes",
            ),
        ] {
            let message = opening(
                dir.path(),
                "a.log",
                log,
                crate::FileFormat::Candump,
                &options,
            )
            .expect("refused");
            eprintln!("{message}");
            assert_shape(&message, &named);
            assert!(message.contains(says), "{message}");
        }
    }

    pub(crate) const LOG: &str = "(1700000000.000100) can0 123#401F7602\n\
(1700000000.000200) can1 18FEF1FE#1234FFF000000000\n\
# a comment\n\
(1700000000.000300) can0 200#01102700\n\
(1700000000.000400) can0 200#02F6FF0000000000\n\
(1700000000.000500) can0 7DF#R\n\
(1700000000.000600) can0 123##3112233445566778899AABBCC\n\
(1700000000.000700) can0 456#DEAD\n";

    #[test]
    fn each_line_form() {
        let f = parse_line("(1436509052.249713) vcan0 044#2A366C2BBA").unwrap();
        assert_eq!(f.ts, Some(1_436_509_052_249_713));
        assert_eq!((f.id, f.extended, f.dlc), (0x44, false, 5));
        let f = parse_line("(0.5) can0 12345678#R").unwrap();
        assert!(f.remote && f.extended);
        let f = parse_line("(1.0) can0 123##1DEADBEEF").unwrap();
        assert!(f.fd);
        assert_eq!(f.flags, Some(1));
        assert_eq!(f.data, [0xDE, 0xAD, 0xBE, 0xEF]);
        let f = parse_line("  can0  123   [4]  DE AD BE EF").unwrap();
        assert_eq!((f.ts, f.dlc), (None, 4));
        let f = parse_line(" (1436509052.249713)  can0  1F334455   [2]  01 02").unwrap();
        assert!(f.extended);
        let f = parse_line("(2024-01-31 08:15:00.123456)  can0  123   [1]  FF").unwrap();
        assert!(f.ts.is_some());
        let f = parse_line("  can0  321   [8]  remote request").unwrap();
        assert!(f.remote);
        assert_eq!(parse_line("can0 123#ABC"), None);
        assert_eq!(parse_line("hello world"), None);
        assert!(looks_like(LOG.as_bytes()));
        assert!(!looks_like(b"a,b,c\n1,2,3\n"));
    }

    #[test]
    fn frames_and_decoded_messages() {
        let index = index(LOG.as_bytes()).unwrap();
        assert_eq!(index.keys.len(), 7);
        assert_eq!(index.skipped, 1);
        assert!(index.absolute);
        let bytes = Arc::new(Bytes::Owned(LOG.as_bytes().to_vec()));
        let raw = Arc::new(RawFrames::new(bytes.clone(), &index));
        let df = crate::row_index::lazy(&raw).collect().unwrap();
        assert_eq!(df.height(), 7);
        assert_eq!(
            df.column("id").unwrap().str().unwrap().get(1),
            Some("18FEF1FE")
        );
        assert_eq!(
            df.column("kind").unwrap().str().unwrap().get(4),
            Some("remote")
        );

        let dbc = crate::dbc::parse(crate::dbc::tests::SAMPLE, "car", None).unwrap();
        let layers = Layers {
            dbcs: vec![Arc::new(dbc)],
        };
        let listing = Listing::resolve(&index, layers);
        let names: Vec<&String> = listing.messages.keys().collect();
        assert_eq!(names, ["BODY", "ENGINE", "MUXED"]);
        assert_eq!(listing.unknown, 2);
        let (message, rows) = &listing.messages["ENGINE"];
        let decoded = Arc::new(
            Decoded::new(bytes.clone(), &index, rows.clone(), message.clone(), true).unwrap(),
        );
        let df = crate::row_index::lazy(&decoded).collect().unwrap();
        // The classic frame and the FD one.
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.column("Speed").unwrap().f64().unwrap().get(0),
            Some(1000.0)
        );
        assert_eq!(df.column("Temp").unwrap().f64().unwrap().get(0), Some(78.0));
        assert_eq!(
            df.column("Gear").unwrap().str().unwrap().get(0),
            Some("Second")
        );
        let (message, rows) = &listing.messages["MUXED"];
        let decoded = Arc::new(
            Decoded::new(bytes.clone(), &index, rows.clone(), message.clone(), true).unwrap(),
        );
        let df = crate::row_index::lazy(&decoded).collect().unwrap();
        let volts = df.column("Volts").unwrap().f64().unwrap();
        let amps = df.column("Amps").unwrap().f64().unwrap();
        assert_eq!(volts.get(0), Some(100.0));
        assert_eq!(amps.get(0), None);
        assert_eq!(volts.get(1), None);
        assert!((amps.get(1).unwrap() - -1.0).abs() < 1e-9);
    }
}

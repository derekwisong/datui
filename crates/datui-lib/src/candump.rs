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
use std::sync::{Arc, Mutex};

use color_eyre::Result;
use color_eyre::eyre::eyre;

use crate::error_display::FileError;
use polars::prelude::*;

use crate::columns::{Cell, Kind};
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
    let limit = crate::limits::get().indexed_records;
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
            Some(frame) if offsets.len() < limit => {
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

/// The `ts` column's kind: wall clock, or time since the capture started.
fn ts_kind(absolute: bool) -> Kind {
    if absolute {
        Kind::DatetimeUs
    } else {
        Kind::DurationUs
    }
}

fn ts_cell(absolute: bool, ts: Option<i64>) -> Cell {
    if absolute {
        Cell::DatetimeUs(ts)
    } else {
        Cell::DurationUs(ts)
    }
}

/// A window's frames, each line parsed once, holding nothing of the file: the columns
/// are built from it one at a time, as Polars asks for them.
#[derive(Debug, Default)]
struct Lines {
    frames: Vec<Option<Parsed>>,
    /// The interfaces the window names; a frame holds its place here.
    ifaces: Vec<String>,
    /// Every frame's data end to end; a frame holds its range.
    data: Vec<u8>,
}

/// One parsed frame of a [`Lines`].
#[derive(Debug)]
struct Parsed {
    ts: Option<i64>,
    iface: u32,
    id: u32,
    extended: bool,
    fd: bool,
    flags: Option<u8>,
    remote: bool,
    error: bool,
    dlc: u8,
    data: std::ops::Range<usize>,
}

impl Lines {
    /// The frames whose lines start at `starts` in `bytes`.
    fn parse(bytes: &[u8], starts: impl ExactSizeIterator<Item = usize>) -> Self {
        let mut lines = Lines {
            frames: Vec::with_capacity(starts.len()),
            ..Default::default()
        };
        for at in starts {
            let parsed = parse_line(line_of(bytes, at)).map(|f| {
                let iface = match lines.ifaces.iter().position(|i| i == f.iface) {
                    Some(i) => i,
                    None => {
                        lines.ifaces.push(f.iface.to_string());
                        lines.ifaces.len() - 1
                    }
                } as u32;
                let start = lines.data.len();
                lines.data.extend_from_slice(&f.data);
                Parsed {
                    ts: f.ts,
                    iface,
                    id: f.id,
                    extended: f.extended,
                    fd: f.fd,
                    flags: f.flags,
                    remote: f.remote,
                    error: f.error,
                    dlc: f.dlc,
                    data: start..lines.data.len(),
                }
            });
            lines.frames.push(parsed);
        }
        lines
    }

    fn data(&self, frame: &Parsed) -> &[u8] {
        &self.data[frame.data.clone()]
    }
}

/// Which rows a decode asked for: a run exactly, any other set by a hash of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowKey {
    Run {
        first: IdxSize,
        len: usize,
    },
    Rows {
        first: IdxSize,
        len: usize,
        hash: u64,
    },
}

impl WindowKey {
    fn of(rows: &[IdxSize]) -> Self {
        let first = rows.first().copied().unwrap_or(0);
        let run = rows
            .iter()
            .enumerate()
            .all(|(i, &r)| r as usize == first as usize + i);
        if run {
            return Self::Run {
                first,
                len: rows.len(),
            };
        }
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        rows.hash(&mut hasher);
        Self::Rows {
            first,
            len: rows.len(),
            hash: hasher.finish(),
        }
    }
}

/// Rows of windows kept at once: a window this long or a few streaming morsels.
const KEPT_ROWS: usize = 1 << 20;

/// The windows decoded lately, parsed, so that the columns Polars decodes one at a
/// time share one parse of each line. The lock covers only the lookup: a parse runs
/// outside it, so windows decode in parallel, and a caller wanting a window being
/// parsed waits for that window alone. A window leaves once every column has taken
/// it, or when newer windows pass the slots or [`KEPT_ROWS`].
struct Windows<T> {
    /// Columns a window serves before it leaves.
    width: usize,
    slots: usize,
    kept: Mutex<std::collections::VecDeque<Slot<T>>>,
    #[cfg(test)]
    parses: std::sync::atomic::AtomicUsize,
}

struct Slot<T> {
    key: WindowKey,
    rows: usize,
    taken: usize,
    parsed: Arc<std::sync::OnceLock<T>>,
}

impl<T> Windows<T> {
    fn new(width: usize) -> Self {
        let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
        Self {
            width,
            slots: (2 * threads).max(4),
            kept: Default::default(),
            #[cfg(test)]
            parses: Default::default(),
        }
    }

    /// `then` of the window of `rows`, parsed by `parse` unless a column parsed it.
    fn with<R>(
        &self,
        rows: &[IdxSize],
        parse: impl FnOnce(&[IdxSize]) -> T,
        then: impl FnOnce(&T) -> R,
    ) -> R {
        let key = WindowKey::of(rows);
        let parsed = {
            let mut kept = self.kept.lock().unwrap_or_else(|e| e.into_inner());
            match kept.iter().position(|s| s.key == key) {
                Some(at) => {
                    let slot = &mut kept[at];
                    slot.taken += 1;
                    let parsed = slot.parsed.clone();
                    if slot.taken >= self.width {
                        kept.remove(at);
                    }
                    parsed
                }
                None => {
                    let parsed = Arc::new(std::sync::OnceLock::new());
                    if self.width > 1 {
                        kept.push_back(Slot {
                            key,
                            rows: rows.len(),
                            taken: 1,
                            parsed: parsed.clone(),
                        });
                        let mut total: usize = kept.iter().map(|s| s.rows).sum();
                        while kept.len() > 1 && (kept.len() > self.slots || total > KEPT_ROWS) {
                            total -= kept.pop_front().map_or(0, |s| s.rows);
                        }
                    }
                    parsed
                }
            }
        };
        then(parsed.get_or_init(|| {
            #[cfg(test)]
            self.parses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            parse(rows)
        }))
    }
}

/// The raw table's columns, `ts` of the log's kind.
fn raw_columns(absolute: bool) -> [(&'static str, Kind); 9] {
    [
        ("ts", ts_kind(absolute)),
        ("iface", Kind::Str),
        ("id", Kind::Str),
        ("ext", Kind::Bool),
        ("dlc", Kind::U8),
        ("data", Kind::Binary),
        ("fd", Kind::Bool),
        ("flags", Kind::U8),
        ("kind", Kind::Label),
    ]
}

/// The raw table: a row per frame, read from its line where it is shown.
pub struct RawFrames {
    bytes: Arc<Bytes>,
    offsets: Arc<Offsets>,
    absolute: bool,
    schema: SchemaRef,
    windows: Windows<Lines>,
}

impl RawFrames {
    pub fn new(bytes: Arc<Bytes>, index: &Index) -> Self {
        let schema = raw_columns(index.absolute)
            .into_iter()
            .map(|(name, kind)| Field::new(name.into(), kind.dtype()))
            .collect::<Schema>();
        Self {
            bytes,
            offsets: index.offsets.clone(),
            absolute: index.absolute,
            windows: Windows::new(schema.len()),
            schema: Arc::new(schema),
        }
    }

    pub fn rows(&self) -> usize {
        self.offsets.len().min(crate::row_index::MAX_ROWS)
    }

    fn lines(&self, rows: impl ExactSizeIterator<Item = usize>) -> Lines {
        Lines::parse(self.bytes.as_slice(), rows.map(|r| self.offsets.get(r)))
    }

    /// Column `column` of the frames `lines` holds.
    fn column(&self, column: usize, lines: &Lines) -> PolarsResult<Column> {
        let (name, kind) = raw_columns(self.absolute)[column];
        let cells = lines.frames.iter().map(|f| {
            let f = f.as_ref();
            match column {
                0 => ts_cell(self.absolute, f.and_then(|f| f.ts)),
                1 => Cell::Str(f.map(|f| lines.ifaces[f.iface as usize].clone())),
                2 => Cell::Str(f.map(|f| {
                    if f.extended {
                        format!("{:08X}", f.id)
                    } else {
                        format!("{:03X}", f.id)
                    }
                })),
                3 => Cell::Bool(f.map(|f| f.extended)),
                4 => Cell::U8(f.map(|f| f.dlc)),
                5 => Cell::Binary(f.map(|f| lines.data(f).to_vec())),
                6 => Cell::Bool(f.map(|f| f.fd)),
                7 => Cell::U8(f.and_then(|f| f.flags)),
                _ => Cell::Label(f.map(|f| {
                    if f.error {
                        "error"
                    } else if f.remote {
                        "remote"
                    } else {
                        "data"
                    }
                })),
            }
        });
        Ok(crate::columns::series(name, kind, cells)?.into_column())
    }

    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        self.bytes.still_whole()?;
        let start = start.min(self.rows());
        let len = len.min(self.rows() - start);
        let lines = self.lines(start..start + len);
        let columns = (0..self.schema.len())
            .map(|c| self.column(c, &lines))
            .collect::<PolarsResult<_>>()?;
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
        self.bytes.still_whole()?;
        self.windows.with(
            &rows,
            |rows| self.lines(rows.iter().map(|&r| r as usize)),
            |lines| self.column(column, lines),
        )
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
    /// The message's multiplexer signal, if it has one.
    multiplexer: Option<usize>,
    absolute: bool,
    /// Value names as text, or every value as a number (for the long table).
    named: bool,
    schema: SchemaRef,
    windows: Windows<Lines>,
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
        let mut fields = vec![Field::new("ts".into(), ts_kind(index.absolute).dtype())];
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
            multiplexer: message
                .signals
                .iter()
                .position(|s| s.mux == Mux::Multiplexer),
            message,
            absolute: index.absolute,
            named,
            windows: Windows::new(schema.len()),
            schema: Arc::new(schema),
        })
    }

    pub fn height(&self) -> usize {
        self.rows.len().min(crate::row_index::MAX_ROWS)
    }

    fn lines(&self, rows: impl ExactSizeIterator<Item = usize>) -> Lines {
        Lines::parse(
            self.bytes.as_slice(),
            rows.map(|r| self.offsets.get(self.rows[r] as usize)),
        )
    }

    /// Column `column` of the frames `lines` holds: `ts`, then a signal's values.
    fn column(&self, column: usize, lines: &Lines) -> PolarsResult<Column> {
        let name = self.schema.get_at_index(column).map(|(n, _)| n.clone());
        let name = name.unwrap_or_default();
        let Some(signal) = column.checked_sub(1).map(|s| &self.message.signals[s]) else {
            let ts = lines
                .frames
                .iter()
                .map(|f| ts_cell(self.absolute, f.as_ref().and_then(|f| f.ts)));
            let ts = crate::columns::series(&name, ts_kind(self.absolute), ts)?;
            return Ok(ts.into_column());
        };
        let multiplexer = self.multiplexer.map(|m| &self.message.signals[m]);
        let data = |f: &Option<Parsed>| {
            let data = lines.data(f.as_ref()?);
            let mux = multiplexer.and_then(|m| crate::dbc::raw(m, data));
            crate::dbc::present(signal, mux).then_some(data)
        };
        let series = if signal.float != 0 {
            lines
                .frames
                .iter()
                .map(|f| crate::dbc::physical(signal, data(f)?))
                .collect::<Float64Chunked>()
                .into_series()
        } else {
            let ints: Vec<Option<i128>> = lines
                .frames
                .iter()
                .map(|f| crate::dbc::integer(signal, data(f)?))
                .collect();
            crate::fixed_records::integers(&signal_layout(signal, self.named), ints)?
        };
        Ok(series.with_name(name).into_column())
    }

    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        self.bytes.still_whole()?;
        let start = start.min(self.height());
        let len = len.min(self.height() - start);
        let lines = self.lines(start..start + len);
        let columns = (0..self.schema.len())
            .map(|c| self.column(c, &lines))
            .collect::<PolarsResult<_>>()?;
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
        self.bytes.still_whole()?;
        self.windows.with(
            &rows,
            |rows| self.lines(rows.iter().map(|&r| r as usize)),
            |lines| self.column(column, lines),
        )
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
                    // A TOML dictionary's error may be in the file it names.
                    let at = e.path.as_deref().unwrap_or(path);
                    return Err(FileError::at(at, e.line, e.column, e.message).into());
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
        let frames = [
            "ts", "iface", "id", "ext", "dlc", "data", "fd", "flags", "kind",
        ];
        let mut tables = vec![Table::plain(FRAMES, "frames", frames)];
        if self.messages.is_empty() {
            return tables;
        }
        let signals = ["ts", "message", "signal", "value", "unit"];
        tables.push(Table::plain(SIGNALS, "signals", signals));
        for (name, (message, _)) in &self.messages {
            let columns =
                std::iter::once("ts").chain(message.signals.iter().map(|s| s.name.as_str()));
            tables.push(Table::plain(name, "message", columns));
        }
        tables
    }
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
                "{} signals left out: past the first {MAX_LONG_PARTS} {} each message's table has them",
                crate::numfmt::group_chrome(left_out),
                crate::glyphs::get().middot
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
    let path = &input.path().to_path_buf();
    let wanted = input.options.table.as_deref();
    let layers = Layers::new(input.formats, &input.options.dicts)?;
    let (bytes, index) = crate::indexed::indexed(path, index)?;
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
            return Ok(crate::members::several(&input, tables));
        }
    };
    let mut notes: Vec<String> = Vec::new();
    if index.skipped > 0 {
        notes.push(format!(
            "{} non-frame lines skipped",
            crate::numfmt::group_chrome(index.skipped)
        ));
    }
    if index.past_limit > 0 {
        notes.push(crate::limits::left_out(
            &format!("{} frames", crate::numfmt::group_chrome(index.past_limit)),
            crate::limits::get().indexed_records,
            "indexed_records",
        ));
    }
    for dbc in &listing.layers.dbcs {
        notes.extend(dbc.notes.iter().cloned());
    }
    let mut opened = crate::members::Opened::for_table(
        detail(&index, &listing),
        &tables,
        &picked,
        notes,
        "the log",
    );
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
    Ok(opened.scan(input, lf))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Every column of one window comes from one parse of its rows; other rows parse
    /// again, and a window every column has taken is let go.
    #[test]
    fn a_window_is_parsed_once_for_all_its_columns() {
        let windows = Windows::new(2);
        let parse = |rows: &[IdxSize]| rows.to_vec();
        let parses = || windows.parses.load(std::sync::atomic::Ordering::Relaxed);
        let first = windows.with(&[1, 2], parse, |rows| rows.clone());
        let second = windows.with(&[1, 2], parse, |rows| rows.clone());
        assert_eq!((first, second, parses()), (vec![1, 2], vec![1, 2], 1));
        assert!(windows.kept.lock().unwrap().is_empty());
        windows.with(&[1, 3], parse, |_| ());
        windows.with(&[3], parse, |_| ());
        windows.with(&[1, 3], parse, |_| ());
        assert_eq!(parses(), 3);
    }

    /// One window's parse does not hold up another's.
    #[test]
    fn windows_parse_in_parallel() {
        let windows = &Windows::new(9);
        let (parsed_b, b_done) = std::sync::mpsc::channel();
        std::thread::scope(|s| {
            s.spawn(move || {
                windows.with(
                    &[0, 1],
                    |_| {
                        b_done
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .expect("the second window waited on the first");
                    },
                    |_| (),
                );
            });
            s.spawn(move || {
                // Let the first parse begin, then parse another window while it runs.
                std::thread::sleep(std::time::Duration::from_millis(50));
                windows.with(&[5, 6], |_| (), |_| ());
                parsed_b.send(()).unwrap();
            });
        });
    }

    /// The raw table and a message table build each column of a window from one
    /// parse of its lines.
    #[test]
    fn a_decoded_window_parses_each_line_once() {
        use crate::row_index::RowSource;
        let log = b"(1.000001) can0 123#0102\n(1.000002) can1 1FFFFFFF#R\nnot a frame\n(1.000003) can0 456#03\n";
        let index = index(log).unwrap();
        let raw = RawFrames::new(Arc::new(Bytes::Owned(log.to_vec())), &index);
        let rows = IdxCa::from_vec("".into(), vec![0, 2]);
        let columns: Vec<Column> = (0..9).map(|c| raw.decode(c, &rows).unwrap()).collect();
        assert_eq!(
            raw.windows
                .parses
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        let df = DataFrame::new(2, columns).unwrap();
        let window = raw.collect_window(0, 3).unwrap();
        let picked = window.take(&rows).unwrap();
        assert!(df.equals_missing(&picked), "{df}\n{picked}");
        assert_eq!(
            df.column("iface").unwrap().str().unwrap().get(1),
            Some("can0")
        );
    }

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

        let dbc = crate::dbc::parse(crate::tests::fixtures::DBC, "car", None).unwrap();
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

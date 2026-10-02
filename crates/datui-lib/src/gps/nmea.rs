//! NMEA 0183 logs: the sentences a GPS receiver writes, one per line.
//!
//! [`NmeaReader`] takes the log as bytes in whatever pieces they arrive and hands rows
//! back a batch at a time, keeping only the line in progress and the fix being merged.
//! Nothing about it needs the whole file, so a log being written can be read on as it
//! grows (`push` the new bytes, take what is ready); only [`NmeaReader::finish`] says
//! there will be no more.
//!
//! Every length comes from the reader, not the file: a line is at most [`MAX_LINE`]
//! bytes (the standard's is 82), so a hostile file costs one line of memory.

use chrono::NaiveDate;
use polars::prelude::*;

use super::table::{Builder, Cell, Kind};

/// The longest line read as a sentence. The standard allows 82 characters; vendors'
/// own sentences run longer. A longer line is not NMEA and is skipped.
pub const MAX_LINE: usize = 1024;
/// Rows held before they are handed over as a batch.
pub const BATCH_ROWS: usize = 65_536;
/// How many distinct sentence types are counted by name; the rest are counted together.
const MAX_TYPES: usize = 64;
/// Knots to meters per second.
const KNOTS: f64 = 1852.0 / 3600.0;
const DAY_MS: i64 = 86_400_000;
const HALF_DAY_MS: i64 = DAY_MS / 2;

/// The tables an NMEA log opens as: the fixes merged from its sentences, each sentence
/// type alone, or every sentence as it stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Fixes,
    Gga,
    Rmc,
    Vtg,
    Gsa,
    Gsv,
    Gll,
    Zda,
    Sentences,
}

impl Table {
    pub const ALL: [Table; 9] = [
        Table::Fixes,
        Table::Gga,
        Table::Rmc,
        Table::Vtg,
        Table::Gsa,
        Table::Gsv,
        Table::Gll,
        Table::Zda,
        Table::Sentences,
    ];

    /// The name `--table` takes, as the help and errors spell it.
    pub fn name(self) -> &'static str {
        match self {
            Table::Fixes => "fixes",
            Table::Gga => "GGA",
            Table::Rmc => "RMC",
            Table::Vtg => "VTG",
            Table::Gsa => "GSA",
            Table::Gsv => "GSV",
            Table::Gll => "GLL",
            Table::Zda => "ZDA",
            Table::Sentences => "sentences",
        }
    }

    /// The table `name` names, in any case.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|t| t.name().eq_ignore_ascii_case(name.trim()))
    }

    /// The sentence type this table is the rows of, for the tables that are one.
    fn sentence(self) -> Option<&'static str> {
        match self {
            Table::Fixes | Table::Sentences => None,
            other => Some(other.name()),
        }
    }

    /// The table's columns. `time` is first in every one.
    pub fn columns(self) -> Vec<(&'static str, Kind)> {
        use Kind::*;
        match self {
            Table::Fixes => vec![
                ("time", Time),
                ("lat", F64),
                ("lon", F64),
                ("alt", F64),
                ("speed", F64),
                ("course", F64),
                ("sats", U32),
                ("hdop", F64),
                ("fix", Str),
                ("gap", F64),
                ("checksum_ok", Bool),
            ],
            Table::Gga => vec![
                ("time", Time),
                ("talker", Str),
                ("lat", F64),
                ("lon", F64),
                ("quality", U32),
                ("sats", U32),
                ("hdop", F64),
                ("alt", F64),
                ("geoid_sep", F64),
                ("dgps_age", F64),
                ("dgps_station", Str),
                ("checksum_ok", Bool),
            ],
            Table::Rmc => vec![
                ("time", Time),
                ("talker", Str),
                ("status", Str),
                ("lat", F64),
                ("lon", F64),
                ("speed", F64),
                ("course", F64),
                ("mag_var", F64),
                ("mode", Str),
                ("checksum_ok", Bool),
            ],
            Table::Vtg => vec![
                ("time", Time),
                ("talker", Str),
                ("course", F64),
                ("course_mag", F64),
                ("speed", F64),
                ("mode", Str),
                ("checksum_ok", Bool),
            ],
            Table::Gsa => vec![
                ("time", Time),
                ("talker", Str),
                ("mode", Str),
                ("fix_type", U32),
                ("sats", ListU32),
                ("pdop", F64),
                ("hdop", F64),
                ("vdop", F64),
                ("system", U32),
                ("checksum_ok", Bool),
            ],
            Table::Gsv => vec![
                ("time", Time),
                ("talker", Str),
                ("messages", U32),
                ("message", U32),
                ("in_view", U32),
                ("prn", U32),
                ("elevation", F64),
                ("azimuth", F64),
                ("snr", F64),
                ("signal", Str),
                ("checksum_ok", Bool),
            ],
            Table::Gll => vec![
                ("time", Time),
                ("talker", Str),
                ("lat", F64),
                ("lon", F64),
                ("status", Str),
                ("mode", Str),
                ("checksum_ok", Bool),
            ],
            Table::Zda => vec![
                ("time", Time),
                ("talker", Str),
                ("zone_hours", I32),
                ("zone_minutes", I32),
                ("checksum_ok", Bool),
            ],
            Table::Sentences => vec![
                ("time", Time),
                ("line", U64),
                ("talker", Str),
                ("type", Str),
                ("fields", Str),
                ("checksum_ok", Bool),
            ],
        }
    }

    /// The schema of the table's frames.
    pub fn schema(self) -> Schema {
        self.columns()
            .into_iter()
            .map(|(name, kind)| Field::new(name.into(), kind.dtype()))
            .collect()
    }
}

/// One sentence, borrowed from its line.
#[derive(Debug)]
struct Sentence<'a> {
    /// `GP`, `GN`, `GL`…; `P` for a vendor's own sentence.
    talker: &'a str,
    /// `GGA`, `RMC`…; for a vendor sentence, what follows the `P`.
    kind: &'a str,
    /// Everything between the address and the checksum, as written.
    body: &'a str,
    fields: Vec<&'a str>,
    /// Whether the checksum matches; `None` when the sentence has none.
    checksum_ok: Option<bool>,
}

impl Sentence<'_> {
    fn field(&self, i: usize) -> &str {
        self.fields.get(i).copied().unwrap_or("")
    }

    /// The type as counted: `GGA`, or `PUBX` for a vendor sentence.
    fn type_name(&self) -> String {
        if self.talker == "P" {
            format!("P{}", self.kind)
        } else {
            self.kind.to_string()
        }
    }
}

/// The sentence on `line`, if it holds one. It starts at the first `$` or `!`, so a
/// logger's own prefix before it (a timestamp) is passed over.
fn parse_sentence(line: &[u8]) -> Option<Sentence<'_>> {
    let start = line.iter().position(|&b| b == b'$' || b == b'!')?;
    let text = std::str::from_utf8(&line[start + 1..]).ok()?;
    let text = text.trim_end();
    let (data, checksum_ok) = match text.rfind('*') {
        Some(star) => {
            let given = &text[star + 1..];
            let data = &text[..star];
            let ok = given.len() == 2
                && u8::from_str_radix(given, 16)
                    .is_ok_and(|sum| data.bytes().fold(0u8, |acc, b| acc ^ b) == sum);
            (data, Some(ok))
        }
        None => (text, None),
    };
    let (address, body) = data.split_once(',').unwrap_or((data, ""));
    let valid = (3..=10).contains(&address.len())
        && address.as_bytes()[0].is_ascii_uppercase()
        && address
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit());
    if !valid {
        return None;
    }
    let (talker, kind) = if let Some(rest) = address.strip_prefix('P') {
        ("P", rest)
    } else {
        address.split_at(2)
    };
    Some(Sentence {
        talker,
        kind,
        body,
        fields: if data.contains(',') {
            body.split(',').collect()
        } else {
            Vec::new()
        },
        checksum_ok,
    })
}

/// Sentence types a receiver writes, by which a sentence with no checksum is known.
const KNOWN: [&str; 14] = [
    "GGA", "RMC", "VTG", "GSA", "GSV", "GLL", "ZDA", "GNS", "GST", "GBS", "TXT", "HDT", "DTM",
    "GRS",
];

/// Whether the first bytes of a file are an NMEA log: its first line that is not blank
/// is a sentence at the start of the line, either with a checksum that matches or of a
/// type receivers write. A CSV header such as `$USD,$EUR` has the shape of a sentence
/// and is neither. A capture from a serial port starts wherever the port was in its
/// output, so a first line that is the tail of a sentence (`...,M,,*47`) is passed over.
pub fn looks_like(head: &[u8]) -> bool {
    let head = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let mut lines = head
        .split(|&b| b == b'\n')
        .map(|l| l.trim_ascii())
        .filter(|l| !l.is_empty());
    let sentence = |line: &[u8]| {
        line.first() == Some(&b'$')
            && parse_sentence(line).is_some_and(|s| {
                s.checksum_ok == Some(true) || (s.talker.len() == 2 && KNOWN.contains(&s.kind))
            })
    };
    let tail = |line: &[u8]| {
        line.len() >= 3
            && line[line.len() - 3] == b'*'
            && line[line.len() - 2..].iter().all(u8::is_ascii_hexdigit)
    };
    match lines.next() {
        Some(first) if sentence(first) => true,
        Some(first) if first.first() != Some(&b'$') && tail(first) => {
            lines.next().is_some_and(sentence)
        }
        _ => false,
    }
}

fn number(s: &str) -> Option<f64> {
    let s = s.trim();
    // `inf` and `NaN` parse as floats; they are not readings.
    if s.is_empty()
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || b"+-.eE".contains(&b))
    {
        return None;
    }
    s.parse::<f64>().ok().filter(|v| v.is_finite())
}

fn unsigned(s: &str) -> Option<u32> {
    s.trim().parse().ok()
}

fn text(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// A latitude or longitude written `ddmm.mmmm` (`dddmm.mmmm`), in decimal degrees,
/// negative south and west. `None` for a value that is not one.
fn degrees(value: &str, hemisphere: &str, limit: f64) -> Option<f64> {
    let v = number(value).filter(|v| *v >= 0.0)?;
    let whole = (v / 100.0).floor();
    let minutes = v - whole * 100.0;
    if minutes >= 60.0 {
        return None;
    }
    let deg = whole + minutes / 60.0;
    if deg > limit {
        return None;
    }
    match hemisphere.trim() {
        "N" | "E" => Some(deg),
        "S" | "W" => Some(-deg),
        _ => None,
    }
}

fn latitude(value: &str, hemisphere: &str) -> Option<f64> {
    degrees(value, hemisphere, 90.0).filter(|_| matches!(hemisphere.trim(), "N" | "S"))
}

fn longitude(value: &str, hemisphere: &str) -> Option<f64> {
    degrees(value, hemisphere, 180.0).filter(|_| matches!(hemisphere.trim(), "E" | "W"))
}

/// `hhmmss` or `hhmmss.sss`, as milliseconds into the day.
fn time_of_day(s: &str) -> Option<u32> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 6 || !b[..6].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let two = |i: usize| u32::from(b[i] - b'0') * 10 + u32::from(b[i + 1] - b'0');
    let (h, m, sec) = (two(0), two(2), two(4));
    if h > 23 || m > 59 || sec > 60 {
        return None;
    }
    let mut ms = 0;
    if b.len() > 6 {
        let frac = s[6..].strip_prefix('.')?;
        if !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        // Milliseconds: the first three digits, padded.
        for (i, c) in frac.bytes().take(3).enumerate() {
            ms += u32::from(c - b'0') * [100, 10, 1][i];
        }
    }
    Some(((h * 60 + m) * 60 + sec) * 1000 + ms)
}

/// Days since 1970-01-01 of a calendar date.
fn days(year: i32, month: u32, day: u32) -> Option<i64> {
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    Some(
        date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1)?)
            .num_days(),
    )
}

/// An RMC date, `ddmmyy`. Two-digit years from 80 on are the 1900s.
fn rmc_date(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.len() != 6 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = |i: usize| s[i..i + 2].parse::<u32>().ok();
    let yy = n(4)? as i32;
    days(if yy >= 80 { 1900 + yy } else { 2000 + yy }, n(2)?, n(0)?)
}

/// The day the log is on. NMEA puts the date in RMC and ZDA only; every other
/// sentence carries the time of day, and is dated by the last date seen, a day
/// later when its time has gone back past midnight.
#[derive(Debug, Default)]
struct Clock {
    /// The last day and time of day known.
    anchor: Option<(i64, u32)>,
}

impl Clock {
    /// The day a time of day `tod` falls on, near `anchor`.
    fn near(anchor: (i64, u32), tod: u32) -> i64 {
        let (day, last) = (anchor.0, i64::from(anchor.1));
        let tod = i64::from(tod);
        if tod + HALF_DAY_MS < last {
            day + 1
        } else if tod > last + HALF_DAY_MS {
            day - 1
        } else {
            day
        }
    }

    fn date(&mut self, day: i64, tod: u32) {
        self.anchor = Some((day, tod));
    }

    /// Milliseconds since the epoch of `tod`; the clock moves on with it.
    fn resolve(&mut self, tod: u32) -> Option<i64> {
        let anchor = self.anchor?;
        let day = Self::near(anchor, tod);
        if day > anchor.0 || (day == anchor.0 && tod > anchor.1) {
            self.anchor = Some((day, tod));
        }
        Some(day * DAY_MS + i64::from(tod))
    }
}

/// The fix being merged: one epoch's GGA, RMC, VTG and GLL.
#[derive(Debug, Default)]
struct Fix {
    tod: Option<u32>,
    /// The sentence types merged, as bits: GGA, RMC, VTG, GLL.
    has: u8,
    lat: Option<f64>,
    lon: Option<f64>,
    alt: Option<f64>,
    speed: Option<f64>,
    course: Option<f64>,
    sats: Option<u32>,
    hdop: Option<f64>,
    fix: Option<&'static str>,
    checksum_ok: Option<bool>,
}

fn gga_fix(quality: Option<u32>) -> Option<&'static str> {
    Some(match quality? {
        0 => "none",
        1 => "gps",
        2 => "dgps",
        3 => "pps",
        4 => "rtk",
        5 => "rtk float",
        6 => "estimated",
        7 => "manual",
        8 => "simulated",
        _ => return None,
    })
}

/// What RMC says of the fix: its status, and its mode where it has one.
fn rmc_fix(status: &str, mode: &str) -> Option<&'static str> {
    if status.trim() == "V" {
        return Some("none");
    }
    Some(match mode.trim() {
        "A" => "gps",
        "D" => "dgps",
        "E" => "estimated",
        "F" => "rtk float",
        "R" => "rtk",
        "M" => "manual",
        "S" => "simulated",
        "N" => "none",
        "" if status.trim() == "A" => "gps",
        _ => return None,
    })
}

/// VTG's course and speed: the field letters (`T`, `M`, `N`, `K`) of NMEA 2.3, or the
/// four bare values before it.
fn vtg_values(s: &Sentence) -> (Option<f64>, Option<f64>, Option<f64>, Option<String>) {
    let speed = |knots: &str, kmh: &str| {
        number(knots)
            .map(|v| v * KNOTS)
            .or_else(|| number(kmh).map(|v| v / 3.6))
    };
    if s.field(1).trim() == "T" || s.fields.len() >= 8 {
        (
            number(s.field(0)),
            number(s.field(2)),
            speed(s.field(4), s.field(6)),
            text(s.field(8)),
        )
    } else {
        (
            number(s.field(0)),
            number(s.field(1)),
            speed(s.field(2), s.field(3)),
            None,
        )
    }
}

fn fix_bit(kind: &str) -> u8 {
    match kind {
        "GGA" => 1,
        "RMC" => 2,
        "VTG" => 4,
        "GLL" => 8,
        _ => 0,
    }
}

fn and_checksum(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => Some(x && y),
    }
}

/// What a read counted, for the notes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub lines: u64,
    pub sentences: u64,
    /// Lines that are not blank and hold no sentence.
    pub skipped: u64,
    pub bad_checksums: u64,
    /// Sentences of each type, in the order first seen.
    pub types: Vec<(String, u64)>,
    /// Sentences past the [`MAX_TYPES`] types counted by name.
    pub other_types: u64,
    /// Rows the table holds.
    pub rows: u64,
    /// Whether the log said what day it is (RMC or ZDA).
    pub dated: bool,
}

impl Stats {
    fn count(&mut self, name: String) {
        if let Some((_, n)) = self.types.iter_mut().find(|(t, _)| *t == name) {
            *n += 1;
        } else if self.types.len() < MAX_TYPES {
            self.types.push((name, 1));
        } else {
            self.other_types += 1;
        }
    }

    /// How many sentences of `kind` were read.
    pub fn of(&self, kind: &str) -> u64 {
        self.types
            .iter()
            .find(|(t, _)| t == kind)
            .map_or(0, |(_, n)| *n)
    }
}

/// An NMEA log read into one [`Table`], a piece at a time.
pub struct NmeaReader {
    table: Table,
    rows: Builder,
    line: Vec<u8>,
    /// The line in progress is past [`MAX_LINE`]: the rest of it is passed over.
    overlong: bool,
    clock: Clock,
    /// Rows of this batch read before the log gave a date, and their times of day.
    undated: Vec<(usize, u32)>,
    /// The time of day of the epoch the last sentences belong to.
    epoch: Option<u32>,
    /// The time of day of the last fix that had one, for the next fix's `gap`.
    last_fix: Option<u32>,
    fix: Option<Fix>,
    stats: Stats,
}

impl NmeaReader {
    pub fn new(table: Table) -> Self {
        Self {
            table,
            rows: Builder::new(&table.columns()),
            line: Vec::new(),
            overlong: false,
            clock: Clock::default(),
            undated: Vec::new(),
            epoch: None,
            last_fix: None,
            fix: None,
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Read `bytes`, the next piece of the log. A line cut off at the end waits for
    /// the rest.
    pub fn push(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            match bytes.iter().position(|&b| b == b'\n') {
                Some(end) => {
                    self.extend_line(&bytes[..end]);
                    self.end_line();
                    bytes = &bytes[end + 1..];
                }
                None => {
                    self.extend_line(bytes);
                    break;
                }
            }
        }
    }

    fn extend_line(&mut self, piece: &[u8]) {
        if self.overlong {
            return;
        }
        if self.line.len() + piece.len() > MAX_LINE {
            self.overlong = true;
            self.line.clear();
        } else {
            self.line.extend_from_slice(piece);
        }
    }

    fn end_line(&mut self) {
        let line = std::mem::take(&mut self.line);
        self.stats.lines += 1;
        if std::mem::take(&mut self.overlong) {
            self.stats.skipped += 1;
        } else if !line.trim_ascii().is_empty() {
            match parse_sentence(&line) {
                Some(sentence) => self.sentence(&sentence),
                None => self.stats.skipped += 1,
            }
        }
        // The buffer is kept for the next line.
        self.line = line;
        self.line.clear();
    }

    /// A full batch, once one is held. Rows of it still waiting for a date are let go
    /// undated.
    pub fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.rows.len() < BATCH_ROWS {
            return Ok(None);
        }
        self.take().map(Some)
    }

    fn take(&mut self) -> PolarsResult<DataFrame> {
        self.undated.clear();
        self.rows.take()
    }

    /// The end of the log: the last line and fix, and the rows not yet taken.
    pub fn finish(&mut self) -> PolarsResult<DataFrame> {
        if !self.line.is_empty() || self.overlong {
            self.end_line();
        }
        self.flush_fix();
        self.take()
    }

    /// The time of a row: its own time of day when it has one, else its epoch's.
    fn row_time(&mut self, tod: Option<u32>) -> Cell {
        let Some(tod) = tod.or(self.epoch) else {
            return Cell::Time(None);
        };
        match self.clock.resolve(tod) {
            Some(ms) => Cell::Time(Some(ms)),
            None => {
                // Dated once the log says what day it is.
                self.undated.push((self.rows.len(), tod));
                Cell::Time(None)
            }
        }
    }

    /// The log gave a date: the rows read before it are dated from it.
    fn dated(&mut self, day: i64, tod: u32) {
        let first = self.clock.anchor.is_none();
        self.clock.date(day, tod);
        self.stats.dated = true;
        if first {
            let anchor = (day, tod);
            for (row, tod) in std::mem::take(&mut self.undated) {
                let ms = Clock::near(anchor, tod) * DAY_MS + i64::from(tod);
                self.rows.set_time(0, row, ms);
            }
        }
    }

    fn emit(&mut self, row: Vec<Cell>) {
        self.rows.push(row);
        self.stats.rows += 1;
    }

    fn sentence(&mut self, s: &Sentence) {
        self.stats.sentences += 1;
        if s.checksum_ok == Some(false) {
            self.stats.bad_checksums += 1;
        }
        self.stats.count(s.type_name());
        let standard = s.talker != "P";
        let kind = if standard { s.kind } else { "" };
        let tod = match kind {
            "GGA" | "RMC" | "ZDA" => time_of_day(s.field(0)),
            "GLL" => time_of_day(s.field(4)),
            _ => None,
        };
        // A new epoch starts with a sentence of another time, so the fix of the last
        // one is complete; flushed before the date below moves the clock on.
        let bit = fix_bit(kind);
        if self.table == Table::Fixes && bit != 0 {
            let starts_new = self.fix.as_ref().is_some_and(|fix| {
                fix.has & bit != 0 || matches!((fix.tod, tod), (Some(a), Some(b)) if a != b)
            });
            if starts_new {
                self.flush_fix();
            }
        }
        match kind {
            "RMC" => {
                if let (Some(day), Some(tod)) = (rmc_date(s.field(8)), tod) {
                    self.dated(day, tod);
                }
            }
            "ZDA" => {
                let n = |i: usize| s.field(i).trim().parse::<u32>().ok();
                if let (Some(d), Some(m), Some(y), Some(tod)) = (n(1), n(2), n(3), tod)
                    && let Some(day) = i32::try_from(y).ok().and_then(|y| days(y, m, d))
                {
                    self.dated(day, tod);
                }
            }
            _ => {}
        }
        if tod.is_some() {
            self.epoch = tod;
        }
        match self.table {
            Table::Fixes => {
                if bit != 0 {
                    self.merge(s, kind, bit, tod);
                }
            }
            Table::Sentences => {
                let time = self.row_time(tod);
                let row = vec![
                    time,
                    Cell::U64(Some(self.stats.lines)),
                    Cell::Str(Some(s.talker.to_string())),
                    Cell::Str(Some(s.kind.to_string())),
                    Cell::Str(Some(s.body.to_string())),
                    Cell::Bool(s.checksum_ok),
                ];
                self.emit(row);
            }
            table if table.sentence() == Some(kind) => self.sentence_rows(s, kind, tod),
            _ => {}
        }
    }

    /// Fold a GGA, RMC, VTG or GLL into the fix of its epoch.
    fn merge(&mut self, s: &Sentence, kind: &str, bit: u8, tod: Option<u32>) {
        let fix = self.fix.get_or_insert_with(Fix::default);
        fix.has |= bit;
        fix.tod = fix.tod.or(tod);
        fix.checksum_ok = and_checksum(fix.checksum_ok, s.checksum_ok);
        let (lat, lon) = match kind {
            "GGA" => (
                latitude(s.field(1), s.field(2)),
                longitude(s.field(3), s.field(4)),
            ),
            "RMC" => (
                latitude(s.field(2), s.field(3)),
                longitude(s.field(4), s.field(5)),
            ),
            "GLL" => (
                latitude(s.field(0), s.field(1)),
                longitude(s.field(2), s.field(3)),
            ),
            _ => (None, None),
        };
        fix.lat = fix.lat.or(lat);
        fix.lon = fix.lon.or(lon);
        match kind {
            "GGA" => {
                let quality = unsigned(s.field(5));
                // GGA's quality is the fullest account of the fix; it wins over RMC's.
                fix.fix = gga_fix(quality).or(fix.fix);
                fix.sats = unsigned(s.field(6));
                fix.hdop = number(s.field(7));
                fix.alt = number(s.field(8));
            }
            "RMC" => {
                fix.speed = fix.speed.or(number(s.field(6)).map(|v| v * KNOTS));
                fix.course = fix.course.or(number(s.field(7)));
                if fix.has & 1 == 0 {
                    fix.fix = rmc_fix(s.field(1), s.field(11)).or(fix.fix);
                }
            }
            "VTG" => {
                let (course, _, speed, _) = vtg_values(s);
                fix.speed = fix.speed.or(speed);
                fix.course = fix.course.or(course);
            }
            _ => {}
        }
    }

    fn flush_fix(&mut self) {
        let Some(fix) = self.fix.take() else {
            return;
        };
        let time = match fix.tod {
            Some(tod) => self.row_time(Some(tod)),
            None => Cell::Time(None),
        };
        // Seconds since the fix before, across midnight too; a log's fixes are never
        // a day apart.
        let gap = fix.tod.and_then(|tod| {
            let last = self.last_fix.replace(tod)?;
            let ms = (i64::from(tod) - i64::from(last)).rem_euclid(DAY_MS);
            Some(ms as f64 / 1000.0)
        });
        self.emit(vec![
            time,
            Cell::F64(fix.lat),
            Cell::F64(fix.lon),
            Cell::F64(fix.alt),
            Cell::F64(fix.speed),
            Cell::F64(fix.course),
            Cell::U32(fix.sats),
            Cell::F64(fix.hdop),
            Cell::Str(fix.fix.map(str::to_string)),
            Cell::F64(gap),
            Cell::Bool(fix.checksum_ok),
        ]);
    }

    /// The rows one sentence of the table's own type gives: one, or for GSV one per
    /// satellite it lists.
    fn sentence_rows(&mut self, s: &Sentence, kind: &str, tod: Option<u32>) {
        let (start, waiting) = (self.rows.len(), self.undated.len());
        let time = self.row_time(tod);
        let talker = Cell::Str(Some(s.talker.to_string()));
        let ok = Cell::Bool(s.checksum_ok);
        let f = |i: usize| s.field(i);
        match kind {
            "GGA" => self.emit(vec![
                time,
                talker,
                Cell::F64(latitude(f(1), f(2))),
                Cell::F64(longitude(f(3), f(4))),
                Cell::U32(unsigned(f(5))),
                Cell::U32(unsigned(f(6))),
                Cell::F64(number(f(7))),
                Cell::F64(number(f(8))),
                Cell::F64(number(f(10))),
                Cell::F64(number(f(12))),
                Cell::Str(text(f(13))),
                ok,
            ]),
            "RMC" => {
                let mag_var = number(f(9)).map(|v| if f(10).trim() == "W" { -v } else { v });
                self.emit(vec![
                    time,
                    talker,
                    Cell::Str(text(f(1))),
                    Cell::F64(latitude(f(2), f(3))),
                    Cell::F64(longitude(f(4), f(5))),
                    Cell::F64(number(f(6)).map(|v| v * KNOTS)),
                    Cell::F64(number(f(7))),
                    Cell::F64(mag_var),
                    Cell::Str(text(f(11))),
                    ok,
                ]);
            }
            "VTG" => {
                let (course, course_mag, speed, mode) = vtg_values(s);
                self.emit(vec![
                    time,
                    talker,
                    Cell::F64(course),
                    Cell::F64(course_mag),
                    Cell::F64(speed),
                    Cell::Str(mode),
                    ok,
                ]);
            }
            "GSA" => {
                let sats: Vec<u32> = (2..14).filter_map(|i| unsigned(f(i))).collect();
                self.emit(vec![
                    time,
                    talker,
                    Cell::Str(text(f(0))),
                    Cell::U32(unsigned(f(1))),
                    Cell::ListU32(Some(sats)),
                    Cell::F64(number(f(14))),
                    Cell::F64(number(f(15))),
                    Cell::F64(number(f(16))),
                    Cell::U32(unsigned(f(17))),
                    ok,
                ]);
            }
            "GSV" => {
                // Up to four satellites of four fields each, then NMEA 4.10's signal.
                let blocks = s.fields.len().saturating_sub(3);
                let signal = (blocks % 4 == 1)
                    .then(|| text(f(s.fields.len() - 1)))
                    .flatten();
                for block in 0..blocks / 4 {
                    let at = 3 + block * 4;
                    let Some(prn) = unsigned(f(at)) else {
                        continue;
                    };
                    self.emit(vec![
                        time.clone(),
                        talker.clone(),
                        Cell::U32(unsigned(f(0))),
                        Cell::U32(unsigned(f(1))),
                        Cell::U32(unsigned(f(2))),
                        Cell::U32(Some(prn)),
                        Cell::F64(number(f(at + 1))),
                        Cell::F64(number(f(at + 2))),
                        Cell::F64(number(f(at + 3))),
                        Cell::Str(signal.clone()),
                        ok.clone(),
                    ]);
                }
            }
            "GLL" => self.emit(vec![
                time,
                talker,
                Cell::F64(latitude(f(0), f(1))),
                Cell::F64(longitude(f(2), f(3))),
                Cell::Str(text(f(5))),
                Cell::Str(text(f(6))),
                ok,
            ]),
            "ZDA" => {
                let zone = |i: usize| f(i).trim().parse::<i32>().ok();
                self.emit(vec![
                    time,
                    talker,
                    Cell::I32(zone(4)),
                    Cell::I32(zone(5)),
                    ok,
                ]);
            }
            _ => {}
        }
        // `row_time` recorded an undated row as the next one pushed. A GSV pushes one
        // per satellite, or none, and each waits for the date alike.
        if let Some(&(_, tod)) = self.undated.get(waiting) {
            self.undated.truncate(waiting);
            for row in start..self.rows.len() {
                self.undated.push((row, tod));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(table: Table, text: &str) -> (DataFrame, Stats) {
        let mut reader = NmeaReader::new(table);
        // In small pieces, so lines arrive cut.
        for piece in text.as_bytes().chunks(7) {
            reader.push(piece);
        }
        let df = reader.finish().unwrap();
        (df, reader.stats().clone())
    }

    fn with_checksum(body: &str) -> String {
        let sum = body.bytes().fold(0u8, |a, b| a ^ b);
        format!("${body}*{sum:02X}")
    }

    fn f64s(df: &DataFrame, name: &str) -> Vec<Option<f64>> {
        df.column(name).unwrap().f64().unwrap().iter().collect()
    }

    fn times(df: &DataFrame) -> Vec<Option<i64>> {
        df.column("time")
            .unwrap()
            .datetime()
            .unwrap()
            .physical()
            .iter()
            .collect()
    }

    fn ms(date: &str, tod: &str) -> i64 {
        let day = rmc_date(date).unwrap();
        day * DAY_MS + i64::from(time_of_day(tod).unwrap())
    }

    #[test]
    fn coordinates_are_decimal_degrees() {
        let lat = latitude("4807.038", "N").unwrap();
        assert!((lat - (48.0 + 7.038 / 60.0)).abs() < 1e-12);
        assert!(longitude("01131.000", "W").unwrap() < 0.0);
        assert_eq!(latitude("4867.000", "N"), None, "60 minutes or more");
        assert_eq!(latitude("9100.000", "N"), None);
        assert_eq!(latitude("4807.038", "E"), None, "a longitude's hemisphere");
        assert_eq!(latitude("", "N"), None);
        assert_eq!(latitude("inf", "N"), None);
        assert_eq!(time_of_day("123519.5"), Some(45_319_500));
        assert_eq!(time_of_day("246000"), None);
    }

    #[test]
    fn checksums_and_addresses() {
        let good = with_checksum("GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,");
        let s = parse_sentence(good.as_bytes()).unwrap();
        assert_eq!((s.talker, s.kind, s.checksum_ok), ("GP", "GGA", Some(true)));
        let bad = good.replace("545.4", "545.5");
        assert_eq!(
            parse_sentence(bad.as_bytes()).unwrap().checksum_ok,
            Some(false)
        );
        let none = parse_sentence(b"$GPVTG,054.7,T,034.4,M,005.5,N,010.2,K").unwrap();
        assert_eq!(none.checksum_ok, None);
        let vendor = parse_sentence(b"$PUBX,00,1*00").unwrap();
        assert_eq!((vendor.talker, vendor.kind), ("P", "UBX"));
        assert_eq!(vendor.type_name(), "PUBX");
        let prefixed = parse_sentence(b"12:00:01 $GNRMC,,V,,,,,,,,,,N*4D").unwrap();
        assert_eq!(prefixed.kind, "RMC");
        assert!(parse_sentence(b"$gpgga,1").is_none());
        assert!(parse_sentence(b"hello").is_none());
        assert!(looks_like(b"\n$GPGGA,123519,48"));
        assert!(!looks_like(b"time,lat\n$GPGGA,1"));
        let vendor = with_checksum("PUBX,00,1");
        assert!(looks_like(vendor.as_bytes()), "a vendor's, by its checksum");
        let wrong = vendor.replace("PUBX,00", "PUBX,01");
        assert!(
            !looks_like(wrong.as_bytes()),
            "a vendor's, its checksum wrong"
        );
        assert!(!looks_like(b"$USD,$EUR\n1,2\n"), "a CSV header");
        assert!(!looks_like(b"$AMOUNT,QTY\n1,2\n"));
        assert!(
            looks_like(b"5.4,M,46.9,M,,*47\r\n$GNRMC,120000,A,4807.038,N"),
            "a capture that starts mid-sentence"
        );
        assert!(!looks_like(b"a,b*47\n$USD,$EUR\n"));
    }

    #[test]
    fn an_epoch_s_sentences_merge_into_one_fix() {
        let log = [
            with_checksum("GPRMC,235959.00,A,4807.038,N,01131.000,E,10.0,84.4,311223,003.1,W,A"),
            with_checksum("GPGGA,235959.00,4807.038,N,01131.000,E,4,12,0.8,545.4,M,46.9,M,,"),
            with_checksum("GPVTG,84.4,T,,M,10.0,N,18.5,K,A"),
            "not a sentence".to_string(),
            // Past midnight, without a date of its own.
            with_checksum("GPGGA,000000.00,4807.040,N,01131.000,E,1,07,1.0,546.0,M,46.9,M,,"),
            "$GPRMC,000000.00,A,4807.040,N,01131.000,E,9.0,84.0,010124,,,A*00".to_string(),
        ]
        .join("\r\n");
        let (df, stats) = read(Table::Fixes, &log);
        assert_eq!(df.height(), 2, "{df}");
        assert_eq!(
            times(&df),
            [Some(ms("311223", "235959")), Some(ms("010124", "000000"))]
        );
        let speed = f64s(&df, "speed");
        assert!(
            (speed[0].unwrap() - 10.0 * KNOTS).abs() < 1e-12,
            "knots to m/s"
        );
        assert_eq!(f64s(&df, "alt"), [Some(545.4), Some(546.0)]);
        assert_eq!(f64s(&df, "gap"), [None, Some(1.0)], "across midnight");
        let fix: Vec<_> = df.column("fix").unwrap().str().unwrap().iter().collect();
        assert_eq!(fix, [Some("rtk"), Some("gps")], "GGA's quality wins");
        let ok: Vec<_> = df
            .column("checksum_ok")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(
            ok,
            [Some(true), Some(false)],
            "the second RMC's checksum is wrong"
        );
        assert_eq!(
            (stats.skipped, stats.bad_checksums, stats.sentences),
            (1, 1, 5)
        );
        assert_eq!(stats.of("GGA"), 2);
        assert!(stats.dated);
    }

    #[test]
    fn rows_before_the_first_date_are_dated_by_it() {
        let log = [
            with_checksum("GPGGA,235958,4807.038,N,01131.000,E,1,08,0.9,545.4,M,,M,,"),
            with_checksum("GPGGA,000001,4807.038,N,01131.000,E,1,08,0.9,545.4,M,,M,,"),
            with_checksum("GPRMC,000001,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A"),
        ]
        .join("\n");
        let (df, _) = read(Table::Fixes, &log);
        assert_eq!(
            times(&df),
            [Some(ms("311223", "235958")), Some(ms("010124", "000001"))],
            "the day before, across midnight"
        );
        // A log with no date at all keeps its times empty.
        let (df, stats) = read(Table::Gga, log.lines().next().unwrap());
        assert_eq!(times(&df), [None]);
        assert!(!stats.dated);
    }

    #[test]
    fn each_sentence_type_is_a_table() {
        let log = [
            with_checksum("GPRMC,120000,A,4807.038,N,01131.000,E,1.0,0.0,010124,,,A"),
            with_checksum("GPGSV,2,1,08,01,40,083,46,02,17,308,41,12,07,344,39,14,22,228,45"),
            with_checksum("GPGSV,2,2,08,15,,,,16,10,100,,,,,,,,,,1"),
            with_checksum("GPGSA,A,3,04,05,,09,12,,,24,,,,,2.5,1.3,2.1"),
            with_checksum("GPVTG,054.7,034.4,005.5,010.2"),
            with_checksum("PGRME,15.0,M,45.0,M,25.0,M"),
        ]
        .join("\n");
        let (gsv, _) = read(Table::Gsv, &log);
        assert_eq!(gsv.height(), 6, "a row per satellite listed: {gsv}");
        let snr = f64s(&gsv, "snr");
        assert_eq!(snr[..4], [Some(46.0), Some(41.0), Some(39.0), Some(45.0)]);
        let signal: Vec<_> = gsv
            .column("signal")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(signal[4..], [Some("1"), Some("1")]);
        assert!(
            times(&gsv)
                .iter()
                .all(|t| *t == Some(ms("010124", "120000")))
        );
        let (gsa, _) = read(Table::Gsa, &log);
        let sats = gsa
            .column("sats")
            .unwrap()
            .list()
            .unwrap()
            .get_as_series(0)
            .unwrap();
        assert_eq!(
            sats.u32().unwrap().into_no_null_iter().collect::<Vec<_>>(),
            [4, 5, 9, 12, 24]
        );
        let (vtg, _) = read(Table::Vtg, &log);
        assert_eq!(
            f64s(&vtg, "course_mag"),
            [Some(34.4)],
            "the bare pre-2.3 form"
        );
        let (all, stats) = read(Table::Sentences, &log);
        assert_eq!(all.height(), 6);
        assert_eq!(stats.of("PGRME"), 1);
        assert_eq!(Table::from_name("gsv"), Some(Table::Gsv));
        for table in Table::ALL {
            let (df, _) = read(table, &log);
            assert_eq!(df.schema().as_ref(), &table.schema(), "{}", table.name());
        }
    }

    #[test]
    fn an_overlong_line_is_skipped_and_costs_one_line() {
        let mut reader = NmeaReader::new(Table::Sentences);
        let long = vec![b'x'; MAX_LINE * 4];
        reader.push(&long);
        reader.push(&long);
        assert!(reader.line.len() <= MAX_LINE);
        reader.push(b"\n$GPGGA,1*00\n");
        let df = reader.finish().unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(reader.stats().skipped, 1);
    }
}

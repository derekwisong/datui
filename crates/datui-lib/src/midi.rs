//! Standard MIDI Files, read as a table of their events.
//!
//! The parser is written by hand over the file's bytes: `MThd` and `MTrk` chunks,
//! variable-length deltas, running status, sysex and meta events. Every length the
//! file states is checked against the bytes that remain before it is used, so a
//! corrupt or hostile file is an error rather than a panic or an allocation sized by
//! a number the file made up.
//!
//! The rows are an eager `DataFrame` (one per event) made lazy, as ORC and Excel are:
//! MIDI files are kilobytes. What is not a row — the header, the tracks' names, the
//! tempo range — is a [`MidiSummary`], carried to the dataset for the Info panel's
//! MIDI tab.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

/// The largest file read. MIDI files are kilobytes; a song with a dense controller
/// stream is a few megabytes.
pub const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// The most events read, across every file of one open. Each is a row of a dozen
/// columns held in memory.
pub const MAX_EVENTS: usize = 10_000_000;
/// The most bytes of a sysex or unknown meta event written out as hex.
const HEX_SHOWN: usize = 256;
/// The tempo until a file sets one: 120 beats per minute.
const DEFAULT_TEMPO: u32 = 500_000;

/// How a file counts time: ticks per quarter note, or SMPTE frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Division {
    /// Ticks per quarter note; the tempo map turns ticks into seconds.
    Ppq(u16),
    /// Frames per second (24, 25, 29 for 29.97 drop-frame, or 30) and ticks per
    /// frame. Time is absolute; tempo events do not change it.
    Smpte { fps: u8, ticks_per_frame: u8 },
}

impl Division {
    /// `480 ticks per quarter`, `25 fps, 40 ticks per frame`.
    pub fn label(self) -> String {
        match self {
            Division::Ppq(n) => format!("{n} ticks per quarter"),
            Division::Smpte {
                fps,
                ticks_per_frame,
            } => {
                let fps = if fps == 29 {
                    "29.97".to_string()
                } else {
                    fps.to_string()
                };
                format!("{fps} fps, {ticks_per_frame} ticks per frame")
            }
        }
    }
}

/// One event as the file has it, its bytes borrowed from the file.
#[derive(Debug, Clone, Copy)]
pub struct Event<'a> {
    /// Ticks from the start of its track.
    pub tick: u64,
    pub body: Body<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Body<'a> {
    /// A channel message: the status byte (kind in the high nibble, channel 0-15 in
    /// the low) and its one or two data bytes. `b` is 0 for a one-byte message.
    Channel { status: u8, a: u8, b: u8 },
    /// `F0` (a whole or first sysex packet) or `F7` (a continuation or escape), with
    /// the bytes after its length.
    Sysex { escape: bool, data: &'a [u8] },
    /// `FF`: the meta type and its data.
    Meta { kind: u8, data: &'a [u8] },
    /// A system common (`F1`-`F6`) or real-time (`F8`-`FE`) message, which a file
    /// should not hold but some do, and its data bytes: 0 where it has none.
    System { status: u8, a: u8, b: u8 },
}

/// A Standard MIDI File, parsed.
#[derive(Debug, Clone)]
pub struct Smf<'a> {
    /// 0 (one track), 1 (tracks played together) or 2 (independent sequences).
    pub format: u16,
    pub division: Division,
    /// Each `MTrk` chunk's events, in file order.
    pub tracks: Vec<Vec<Event<'a>>>,
}

/// Whether `head` starts like a MIDI file: `MThd` with its length of 6, or a RIFF
/// `RMID` wrapper.
pub fn looks_like_midi(head: &[u8]) -> bool {
    head.starts_with(b"MThd\0\0\0\x06")
        || (head.len() >= 12 && head.starts_with(b"RIFF") && &head[8..12] == b"RMID")
}

/// A cursor over the bytes of one chunk that never reads past its end.
struct Bytes<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Bytes<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }

    fn left(&self) -> usize {
        self.data.len() - self.at
    }

    fn u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.at)?;
        self.at += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.left() {
            return None;
        }
        let out = &self.data[self.at..self.at + n];
        self.at += n;
        Some(out)
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A variable-length quantity: seven bits a byte, high bit set on all but the
    /// last, at most four bytes (28 bits), as the specification bounds it.
    fn vlq(&mut self) -> Result<u32, VlqError> {
        let mut value = 0u32;
        for _ in 0..4 {
            let b = self.u8().ok_or(VlqError::CutShort)?;
            value = (value << 7) | u32::from(b & 0x7f);
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(VlqError::TooLong)
    }
}

enum VlqError {
    CutShort,
    TooLong,
}

/// Where in a track an error was found, for its message.
fn at(track: usize, offset: usize) -> String {
    format!("track {}, byte {offset}", track + 1)
}

/// The SMF inside a RIFF `RMID` wrapper: the contents of its `data` chunk.
fn unwrap_rmid(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.get(8..12) != Some(b"RMID".as_slice()) {
        return Err(eyre!("Not a MIDI file: a RIFF file that is not RIFF MIDI"));
    }
    let mut r = Bytes::new(&bytes[12..]);
    while r.left() >= 8 {
        let id = r.take(4).unwrap_or_default();
        let len = r
            .take(4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));
        let len = len.unwrap_or(0) as usize;
        let Some(body) = r.take(len) else {
            return Err(eyre!("Not a MIDI file: a RIFF chunk runs past the end"));
        };
        if id == b"data" {
            return Ok(body);
        }
        // Chunks are padded to an even length.
        if len % 2 == 1 {
            r.take(1);
        }
    }
    Err(eyre!(
        "Not a MIDI file: the RIFF MIDI wrapper has no data chunk"
    ))
}

/// Parse a Standard MIDI File (or one in a RIFF `RMID` wrapper).
///
/// Strict where being lenient would show a table that looks whole and is not: a track
/// that runs past the end of the file, an event cut short, a data byte with no status
/// before it, or fewer tracks than the header says are errors. Chunks other than
/// `MTrk` are skipped, as the specification says, and anything after the last track
/// the header promises is ignored. A track may end without its End of Track event.
pub fn parse(bytes: &[u8]) -> Result<Smf<'_>> {
    let bytes = if bytes.starts_with(b"RIFF") {
        unwrap_rmid(bytes)?
    } else {
        bytes
    };
    let mut r = Bytes::new(bytes);
    if r.take(4) != Some(b"MThd".as_slice()) {
        return Err(eyre!("Not a MIDI file: it does not start with MThd"));
    }
    let header_len = r.u32().ok_or_else(|| eyre!("MIDI header is cut short"))? as usize;
    if header_len < 6 {
        return Err(eyre!("MIDI header is {header_len} bytes; it needs 6"));
    }
    let header = r
        .take(header_len)
        .ok_or_else(|| eyre!("MIDI header is cut short"))?;
    let format = u16::from_be_bytes([header[0], header[1]]);
    let declared = u16::from_be_bytes([header[2], header[3]]) as usize;
    let raw_division = u16::from_be_bytes([header[4], header[5]]);
    if format > 2 {
        return Err(eyre!("MIDI format {format} is not one of 0, 1 or 2"));
    }
    if declared == 0 {
        return Err(eyre!("MIDI header says the file has no tracks"));
    }
    let division = if raw_division & 0x8000 != 0 {
        // The high byte is the frame rate, negated as a signed byte.
        let fps = (-i16::from((raw_division >> 8) as u8 as i8)) as u8;
        let ticks_per_frame = (raw_division & 0xff) as u8;
        if !matches!(fps, 24 | 25 | 29 | 30) || ticks_per_frame == 0 {
            return Err(eyre!(
                "MIDI header's SMPTE timing ({fps} fps, {ticks_per_frame} ticks per frame) is not usable"
            ));
        }
        Division::Smpte {
            fps,
            ticks_per_frame,
        }
    } else if raw_division == 0 {
        return Err(eyre!("MIDI header says 0 ticks per quarter note"));
    } else {
        Division::Ppq(raw_division)
    };

    // Not sized by `declared`: a header may claim 65,535 tracks in a file of 20 bytes.
    let mut tracks = Vec::new();
    let mut events = 0usize;
    while tracks.len() < declared {
        if r.left() < 8 {
            return Err(eyre!(
                "MIDI header says {declared} {}; the file holds {}",
                if declared == 1 { "track" } else { "tracks" },
                tracks.len()
            ));
        }
        let id = r.take(4).unwrap_or_default();
        let len = r.u32().unwrap_or(0) as usize;
        let left = r.left();
        let Some(body) = r.take(len) else {
            if id == b"MTrk" {
                return Err(eyre!(
                    "MIDI track {} is cut short: it says {len} bytes and {left} remain",
                    tracks.len() + 1
                ));
            }
            return Err(eyre!(
                "MIDI file is cut short: a chunk says {len} bytes and {left} remain"
            ));
        };
        if id != b"MTrk" {
            continue;
        }
        let track = parse_track(body, tracks.len(), &mut events)?;
        tracks.push(track);
    }
    Ok(Smf {
        format,
        division,
        tracks,
    })
}

/// One `MTrk` chunk's events. `events` counts across tracks, for [`MAX_EVENTS`].
fn parse_track<'a>(body: &'a [u8], track: usize, events: &mut usize) -> Result<Vec<Event<'a>>> {
    let mut r = Bytes::new(body);
    let mut out = Vec::new();
    let mut tick = 0u64;
    // The last channel status, which a data byte in its place repeats. Sysex and meta
    // events cancel it.
    let mut running: Option<u8> = None;
    while r.left() > 0 {
        let start = r.at;
        let delta = r.vlq().map_err(|e| match e {
            VlqError::CutShort => eyre!("MIDI {}: a delta time is cut short", at(track, start)),
            VlqError::TooLong => eyre!(
                "MIDI {}: a delta time is longer than four bytes",
                at(track, start)
            ),
        })?;
        tick += u64::from(delta);
        let cut = || eyre!("MIDI {}: an event is cut short", at(track, start));
        let first = r.u8().ok_or_else(cut)?;
        let body = match first {
            0xff => {
                running = None;
                let kind = r.u8().ok_or_else(cut)?;
                let len = vlq_len(&mut r, track, start)?;
                let data = r.take(len).ok_or_else(cut)?;
                Body::Meta { kind, data }
            }
            0xf0 | 0xf7 => {
                running = None;
                let len = vlq_len(&mut r, track, start)?;
                let data = r.take(len).ok_or_else(cut)?;
                Body::Sysex {
                    escape: first == 0xf7,
                    data,
                }
            }
            // System real-time: one byte, and running status survives it, as on the
            // wire. A file should not hold one, but its length is never in doubt.
            0xf8..=0xfe => Body::System {
                status: first,
                a: 0,
                b: 0,
            },
            // System common: its data bytes as the wire defines them. It cancels
            // running status. 0xF4 and 0xF5 have no definition, so no length.
            0xf1 | 0xf2 | 0xf3 | 0xf6 => {
                running = None;
                let (a, b) = match first {
                    0xf1 | 0xf3 => (r.u8().ok_or_else(cut)?, 0),
                    0xf2 => (r.u8().ok_or_else(cut)?, r.u8().ok_or_else(cut)?),
                    _ => (0, 0),
                };
                if a & 0x80 != 0 || b & 0x80 != 0 {
                    return Err(eyre!(
                        "MIDI {}: a data byte has its high bit set",
                        at(track, start)
                    ));
                }
                Body::System {
                    status: first,
                    a,
                    b,
                }
            }
            0xf4 | 0xf5 => {
                return Err(eyre!(
                    "MIDI {}: status {first:#04X} is undefined",
                    at(track, start)
                ));
            }
            _ => {
                let (status, a) = if first & 0x80 != 0 {
                    running = Some(first);
                    (first, r.u8().ok_or_else(cut)?)
                } else {
                    let status = running.ok_or_else(|| {
                        eyre!(
                            "MIDI {}: a data byte with no status before it",
                            at(track, start)
                        )
                    })?;
                    (status, first)
                };
                let b = if matches!(status & 0xf0, 0xc0 | 0xd0) {
                    0
                } else {
                    r.u8().ok_or_else(cut)?
                };
                if a & 0x80 != 0 || b & 0x80 != 0 {
                    return Err(eyre!(
                        "MIDI {}: a data byte has its high bit set",
                        at(track, start)
                    ));
                }
                Body::Channel { status, a, b }
            }
        };
        *events += 1;
        if *events > MAX_EVENTS {
            return Err(eyre!(
                "MIDI has more than {MAX_EVENTS} events; datui reads up to that many"
            ));
        }
        out.push(Event { tick, body });
        if matches!(body, Body::Meta { kind: 0x2f, .. }) {
            // End of Track: whatever follows in the chunk is not events.
            break;
        }
    }
    Ok(out)
}

/// A length for a sysex or meta event.
fn vlq_len(r: &mut Bytes<'_>, track: usize, start: usize) -> Result<usize> {
    r.vlq().map(|n| n as usize).map_err(|e| match e {
        VlqError::CutShort => eyre!("MIDI {}: an event is cut short", at(track, start)),
        VlqError::TooLong => eyre!(
            "MIDI {}: a length is longer than four bytes",
            at(track, start)
        ),
    })
}

/// Every note's name, made once: a song has thousands of notes and a name each.
static NOTE_NAMES: std::sync::LazyLock<Vec<String>> =
    std::sync::LazyLock::new(|| (0..=127).map(note_name).collect());

/// [`note_name`], borrowed from [`NOTE_NAMES`].
fn note_name_of(note: u8) -> &'static str {
    NOTE_NAMES[usize::from(note & 0x7f)].as_str()
}

/// A note number as a name, middle C (60) as `C4`.
pub fn note_name(note: u8) -> String {
    const NAMES: [&str; 12] = [
        "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B",
    ];
    format!(
        "{}{}",
        NAMES[(note % 12) as usize],
        i32::from(note / 12) - 1
    )
}

/// A key signature as written: sharps (positive) or flats (negative), and minor.
fn key_name(sf: i8, minor: bool) -> Option<String> {
    const MAJOR: [&str; 15] = [
        "Cb", "Gb", "Db", "Ab", "Eb", "Bb", "F", "C", "G", "D", "A", "E", "B", "F#", "C#",
    ];
    const MINOR: [&str; 15] = [
        "Ab", "Eb", "Bb", "F", "C", "G", "D", "A", "E", "B", "F#", "C#", "G#", "D#", "A#",
    ];
    let i = usize::try_from(i16::from(sf) + 7)
        .ok()
        .filter(|i| *i < 15)?;
    Some(if minor {
        format!("{} minor", MINOR[i])
    } else {
        format!("{} major", MAJOR[i])
    })
}

/// Beats per minute for a tempo in microseconds per quarter note: `120`, `92.31`.
pub(crate) fn bpm(tempo: u32) -> String {
    if tempo == 0 {
        return "-".to_string();
    }
    let s = format!("{:.2}", 60_000_000.0 / f64::from(tempo));
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// Text from a meta event: UTF-8 when it is, otherwise Latin-1, which older files
/// mostly are.
fn meta_text(data: &[u8]) -> String {
    match std::str::from_utf8(data) {
        Ok(s) => s.to_string(),
        Err(_) => data.iter().map(|&b| char::from(b)).collect(),
    }
}

/// Bytes as spaced hex, cut at [`HEX_SHOWN`] with how many more there are.
fn hex(prefix: Option<u8>, data: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    if let Some(p) = prefix {
        let _ = write!(out, "{p:02X}");
    }
    for b in data.iter().take(HEX_SHOWN) {
        if !out.is_empty() {
            out.push(' ');
        }
        let _ = write!(out, "{b:02X}");
    }
    if data.len() > HEX_SHOWN {
        let _ = write!(out, " ... ({} more bytes)", data.len() - HEX_SHOWN);
    }
    out
}

/// The tempo changes that apply to a track, as ticks to microseconds.
struct TempoMap {
    /// Tick, microseconds × ticks-per-quarter up to it, tempo from it. Sorted by tick,
    /// starting at 0.
    segments: Vec<(u64, u128, u32)>,
    ppq: u128,
}

impl TempoMap {
    /// From `(tick, tempo)` changes in file order; the last change at a tick wins.
    fn new(mut changes: Vec<(u64, u32)>, ppq: u16) -> Self {
        changes.sort_by_key(|(tick, _)| *tick);
        let mut segments: Vec<(u64, u128, u32)> = vec![(0, 0, DEFAULT_TEMPO)];
        for (tick, tempo) in changes {
            let &(last_tick, last_base, last_tempo) = segments.last().expect("starts with one");
            if tick == last_tick {
                segments.last_mut().expect("starts with one").2 = tempo;
            } else {
                let base = last_base + u128::from(tick - last_tick) * u128::from(last_tempo);
                segments.push((tick, base, tempo));
            }
        }
        Self {
            segments,
            ppq: u128::from(ppq),
        }
    }

    fn micros(&self, tick: u64) -> i64 {
        let i = self.segments.partition_point(|(t, _, _)| *t <= tick) - 1;
        let (t, base, tempo) = self.segments[i];
        let num = base + u128::from(tick - t) * u128::from(tempo);
        i64::try_from(num / self.ppq).unwrap_or(i64::MAX)
    }
}

/// Ticks to microseconds for one track.
enum Clock<'m> {
    Tempo(&'m TempoMap),
    /// Microseconds per tick as a fraction: SMPTE time ignores tempo.
    Smpte {
        num: u128,
        den: u128,
    },
}

impl Clock<'_> {
    fn micros(&self, tick: u64) -> i64 {
        match self {
            Clock::Tempo(map) => map.micros(tick),
            Clock::Smpte { num, den } => {
                i64::try_from(u128::from(tick) * num / den).unwrap_or(i64::MAX)
            }
        }
    }
}

/// The tempo changes in some tracks: a tempo meta event with its three bytes.
fn tempo_events<'a>(tracks: impl IntoIterator<Item = &'a Vec<Event<'a>>>) -> Vec<(u64, u32)> {
    tracks
        .into_iter()
        .flatten()
        .filter_map(|e| match e.body {
            Body::Meta {
                kind: 0x51,
                data: [a, b, c],
            } => Some((e.tick, u32::from_be_bytes([0, *a, *b, *c]))),
            _ => None,
        })
        .collect()
}

/// One track, as the Info panel lists it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackSummary {
    /// Its first track name event.
    pub name: Option<String>,
    /// Its first instrument name event.
    pub instrument: Option<String>,
    pub events: usize,
    /// Note-on events with a velocity above zero.
    pub notes: usize,
    /// The channels its channel messages use, 1-16, ascending.
    pub channels: Vec<u8>,
}

/// What a MIDI file says besides its events, for the Info panel's MIDI tab.
#[derive(Debug, Clone, Default)]
pub struct MidiSummary {
    /// How many files the table holds.
    pub files: usize,
    /// The SMF format, when every file has the same one.
    pub format: Option<u16>,
    /// The timing, when every file has the same one.
    pub division: Option<Division>,
    /// Each track of a single file; empty for many files.
    pub tracks: Vec<TrackSummary>,
    /// Tracks across every file.
    pub track_count: usize,
    pub events: usize,
    /// Note-on events with a velocity above zero.
    pub notes: usize,
    /// Notes that start and never end: no note-off for them in their track.
    pub unended: usize,
    /// The time of the last event, the longest file's.
    pub length_micros: i64,
    /// The first tempo, then the fewest and most microseconds per quarter: the fastest
    /// and the slowest.
    pub tempo: Option<(u32, u32, u32)>,
    /// Tempo events after the first of each file.
    pub tempo_changes: usize,
    /// The first time signature, as `6/8`.
    pub time_signature: Option<String>,
    /// The first key signature, as `D major`.
    pub key: Option<String>,
    /// The first copyright notice.
    pub copyright: Option<String>,
    /// Files of a directory that could not be read, with why. They are not in the
    /// table.
    pub unreadable: Vec<(String, String)>,
}

/// The columns a MIDI file's events fill, one value a row.
#[derive(Default)]
struct Columns<'a> {
    file: Vec<&'a str>,
    track: Vec<u16>,
    tick: Vec<u64>,
    time: Vec<i64>,
    kind: Vec<&'static str>,
    channel: Vec<Option<u8>>,
    note: Vec<Option<u8>>,
    note_name: Vec<Option<&'static str>>,
    velocity: Vec<Option<u8>>,
    controller: Vec<Option<u8>>,
    value: Vec<Option<i32>>,
    length: Vec<Option<i64>>,
    text: Vec<Option<String>>,
}

impl Columns<'_> {
    fn push(&mut self, track: u16, tick: u64, time: i64, kind: &'static str) {
        self.track.push(track);
        self.tick.push(tick);
        self.time.push(time);
        self.kind.push(kind);
        self.channel.push(None);
        self.note.push(None);
        self.note_name.push(None);
        self.velocity.push(None);
        self.controller.push(None);
        self.value.push(None);
        self.length.push(None);
        self.text.push(None);
    }

    fn rows(&self) -> usize {
        self.track.len()
    }
}

/// The summary of one file's events, added into `summary`; its rows into `cols`.
fn add_file<'a>(
    smf: &Smf<'_>,
    file: Option<&'a str>,
    cols: &mut Columns<'a>,
    summary: &mut MidiSummary,
) {
    let tempo_map = match smf.division {
        Division::Ppq(ppq) if smf.format != 2 => {
            Some(TempoMap::new(tempo_events(&smf.tracks), ppq))
        }
        _ => None,
    };
    let mut tracks = Vec::with_capacity(smf.tracks.len());
    let mut tempos: Vec<u32> = Vec::new();
    for (index, events) in smf.tracks.iter().enumerate() {
        // Format 2's tracks are independent sequences, each with its own tempo.
        let own_map;
        let clock = match (smf.division, &tempo_map) {
            (
                Division::Smpte {
                    fps,
                    ticks_per_frame,
                },
                _,
            ) => {
                let (fps_num, fps_den) = if fps == 29 {
                    (30_000u128, 1001u128)
                } else {
                    (u128::from(fps), 1)
                };
                Clock::Smpte {
                    num: 1_000_000 * fps_den,
                    den: fps_num * u128::from(ticks_per_frame),
                }
            }
            (_, Some(map)) => Clock::Tempo(map),
            (Division::Ppq(ppq), None) => {
                own_map = TempoMap::new(tempo_events([events]), ppq);
                Clock::Tempo(&own_map)
            }
        };
        let track_no = u16::try_from(index + 1).unwrap_or(u16::MAX);
        let mut track = TrackSummary {
            events: events.len(),
            ..Default::default()
        };
        let mut channels = [false; 16];
        // Notes sounding, by channel and note, oldest first: a note-off ends the
        // earliest of its note's starts.
        let mut sounding: HashMap<(u8, u8), VecDeque<usize>> = HashMap::new();
        for event in events {
            let time = clock.micros(event.tick);
            summary.length_micros = summary.length_micros.max(time);
            let row = cols.rows();
            match event.body {
                Body::Channel { status, a, b } => {
                    let ch = status & 0x0f;
                    channels[ch as usize] = true;
                    let kind = match status & 0xf0 {
                        0x90 if b > 0 => "note_on",
                        0x80 | 0x90 => "note_off",
                        0xa0 => "poly_aftertouch",
                        0xb0 => "cc",
                        0xc0 => "program",
                        0xd0 => "channel_aftertouch",
                        _ => "pitch_bend",
                    };
                    cols.push(track_no, event.tick, time, kind);
                    cols.channel[row] = Some(ch + 1);
                    match kind {
                        "note_on" | "note_off" | "poly_aftertouch" => {
                            cols.note[row] = Some(a);
                            cols.note_name[row] = Some(note_name_of(a));
                            if kind == "poly_aftertouch" {
                                cols.value[row] = Some(i32::from(b));
                            } else {
                                cols.velocity[row] = Some(b);
                            }
                        }
                        "cc" => {
                            cols.controller[row] = Some(a);
                            cols.value[row] = Some(i32::from(b));
                        }
                        "program" | "channel_aftertouch" => cols.value[row] = Some(i32::from(a)),
                        _ => cols.value[row] = Some(((i32::from(b) << 7) | i32::from(a)) - 8192),
                    }
                    if kind == "note_on" {
                        track.notes += 1;
                        sounding.entry((ch, a)).or_default().push_back(row);
                    } else if kind == "note_off"
                        && let Some(start) = sounding.get_mut(&(ch, a)).and_then(|q| q.pop_front())
                    {
                        cols.length[start] = Some(time.saturating_sub(cols.time[start]));
                    }
                }
                Body::Sysex { escape, data } => {
                    cols.push(
                        track_no,
                        event.tick,
                        time,
                        if escape { "sysex_escape" } else { "sysex" },
                    );
                    cols.value[row] = Some(i32::try_from(data.len()).unwrap_or(i32::MAX));
                    cols.text[row] = Some(hex((!escape).then_some(0xf0), data));
                }
                Body::System { status, a, b } => {
                    let kind = match status {
                        0xf1 => "mtc_quarter_frame",
                        0xf2 => "song_position",
                        0xf3 => "song_select",
                        0xf6 => "tune_request",
                        0xf8 => "clock",
                        0xfa => "start",
                        0xfb => "continue",
                        0xfc => "stop",
                        0xfe => "active_sensing",
                        _ => "realtime",
                    };
                    cols.push(track_no, event.tick, time, kind);
                    cols.value[row] = match status {
                        0xf1 | 0xf3 => Some(i32::from(a)),
                        0xf2 => Some((i32::from(b) << 7) | i32::from(a)),
                        _ => None,
                    };
                }
                Body::Meta { kind, data } => {
                    let name = meta_kind(kind);
                    cols.push(track_no, event.tick, time, name);
                    let (value, text) = meta_value(kind, data);
                    cols.value[row] = value;
                    cols.text[row] = text;
                    if kind == 0x20
                        && let [ch] = data
                    {
                        cols.channel[row] = Some((ch & 0x0f) + 1);
                    }
                    match (kind, data) {
                        (0x51, [a, b, c]) => tempos.push(u32::from_be_bytes([0, *a, *b, *c])),
                        (0x02, _) if summary.copyright.is_none() => {
                            summary.copyright = cols.text[row].clone();
                        }
                        (0x03, _) if track.name.is_none() => track.name = cols.text[row].clone(),
                        (0x04, _) if track.instrument.is_none() => {
                            track.instrument = cols.text[row].clone();
                        }
                        (0x58, _) if summary.time_signature.is_none() => {
                            summary.time_signature = cols.text[row].clone();
                        }
                        (0x59, _) if summary.key.is_none() => summary.key = cols.text[row].clone(),
                        _ => {}
                    }
                }
            }
            if let Some(file) = file {
                cols.file.push(file);
            }
        }
        track.channels = (1..=16u8).filter(|c| channels[(c - 1) as usize]).collect();
        summary.unended += sounding.values().map(VecDeque::len).sum::<usize>();
        summary.notes += track.notes;
        tracks.push(track);
    }
    summary.events += smf.tracks.iter().map(Vec::len).sum::<usize>();
    summary.track_count += smf.tracks.len();
    // The first tempo sets it; each after that changes it.
    summary.tempo_changes += tempos.len().saturating_sub(1);
    if let Some(&first) = tempos.first() {
        let (lo, hi) = tempos
            .iter()
            .fold((u32::MAX, 0), |(lo, hi), &t| (lo.min(t), hi.max(t)));
        summary.tempo = Some(match summary.tempo {
            Some((f, l, h)) => (f, l.min(lo), h.max(hi)),
            None => (first, lo, hi),
        });
    }
    summary.format = match (summary.files, summary.format) {
        (0, _) => Some(smf.format),
        (_, Some(f)) if f == smf.format => Some(f),
        _ => None,
    };
    summary.division = match (summary.files, summary.division) {
        (0, _) => Some(smf.division),
        (_, Some(d)) if d == smf.division => Some(d),
        _ => None,
    };
    summary.tracks = if summary.files == 0 {
        tracks
    } else {
        Vec::new()
    };
    summary.files += 1;
}

/// A meta event's column name.
fn meta_kind(kind: u8) -> &'static str {
    match kind {
        0x00 => "sequence_number",
        0x01 | 0x0a..=0x0f => "text",
        0x02 => "copyright",
        0x03 => "track_name",
        0x04 => "instrument",
        0x05 => "lyric",
        0x06 => "marker",
        0x07 => "cue",
        0x08 => "program_name",
        0x09 => "device_name",
        0x20 => "channel_prefix",
        0x21 => "port",
        0x2f => "end_of_track",
        0x51 => "tempo",
        0x54 => "smpte_offset",
        0x58 => "time_signature",
        0x59 => "key_signature",
        0x7f => "sequencer_specific",
        _ => "meta",
    }
}

/// A meta event's `value` and `text`. Data of the wrong length for its type is shown
/// as hex rather than read.
fn meta_value(kind: u8, data: &[u8]) -> (Option<i32>, Option<String>) {
    match (kind, data) {
        (0x01..=0x0f, _) => (None, Some(meta_text(data))),
        (0x00, [a, b]) => (Some(i32::from(u16::from_be_bytes([*a, *b]))), None),
        (0x20 | 0x21, [a]) => (Some(i32::from(*a)), None),
        (0x2f, []) => (None, None),
        (0x51, [a, b, c]) => {
            let tempo = u32::from_be_bytes([0, *a, *b, *c]);
            (
                Some(i32::try_from(tempo).unwrap_or(i32::MAX)),
                Some(format!("{} bpm", bpm(tempo))),
            )
        }
        (0x54, [hr, mn, se, fr, ff]) => (
            None,
            Some(format!(
                "{:02}:{:02}:{:02}:{:02}.{:02}",
                hr & 0x1f,
                mn,
                se,
                fr,
                ff
            )),
        ),
        (0x58, [nn, dd, _, _]) => {
            let denominator = 1u64.checked_shl(u32::from(*dd)).unwrap_or(0);
            (None, Some(format!("{nn}/{denominator}")))
        }
        (0x59, [sf, mi]) => {
            let sf = *sf as i8;
            match key_name(sf, *mi == 1) {
                Some(key) => (Some(i32::from(sf)), Some(key)),
                None => (Some(i32::from(sf)), Some(hex(None, data))),
            }
        }
        (0x7f, _) => (None, Some(hex(None, data))),
        _ => (None, Some(format!("type {kind:02X}: {}", hex(None, data)))),
    }
}

/// The table and the summary for parsed files; `names` are their files, written to a
/// `file` column when there is more than one.
pub fn build(files: &[(String, Smf<'_>)]) -> Result<(LazyFrame, MidiSummary)> {
    let many = files.len() > 1;
    let mut cols = Columns::default();
    let mut summary = MidiSummary::default();
    for (name, smf) in files {
        add_file(smf, many.then_some(name.as_str()), &mut cols, &mut summary);
    }
    Ok((frame(cols, many)?, summary))
}

/// The columns as a table, with the `file` column first when there is one.
fn frame(cols: Columns<'_>, many: bool) -> Result<LazyFrame> {
    let rows = cols.rows();
    let mut columns: Vec<Column> = Vec::new();
    if many {
        columns.push(Series::new("file".into(), cols.file).into());
    }
    let micros = |name: &str, v: Series| -> Result<Column> {
        Ok(v.cast(&DataType::Duration(TimeUnit::Microseconds))?
            .with_name(name.into())
            .into())
    };
    columns.push(Series::new("track".into(), cols.track).into());
    columns.push(Series::new("tick".into(), cols.tick).into());
    columns.push(micros("time", Series::new("time".into(), cols.time))?);
    columns.push(Series::new("kind".into(), cols.kind).into());
    columns.push(Series::new("channel".into(), cols.channel).into());
    columns.push(Series::new("note".into(), cols.note).into());
    columns.push(Series::new("note_name".into(), cols.note_name).into());
    columns.push(Series::new("velocity".into(), cols.velocity).into());
    columns.push(Series::new("controller".into(), cols.controller).into());
    columns.push(Series::new("value".into(), cols.value).into());
    columns.push(micros("length", Series::new("length".into(), cols.length))?);
    columns.push(Series::new("text".into(), cols.text).into());
    Ok(DataFrame::new(rows, columns)?.lazy())
}

/// Read one file's bytes, refusing one past [`MAX_FILE_BYTES`].
fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len > MAX_FILE_BYTES {
        let size = crate::widgets::info::format_bytes;
        return Err(eyre!(
            "MIDI file is {}; datui reads MIDI files up to {}",
            size(len),
            size(MAX_FILE_BYTES)
        ));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.take(MAX_FILE_BYTES).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Read `paths` as one table of events, with a `file` column when there is more than
/// one.
///
/// One file that cannot be read is an error. Of several — a directory of songs — a
/// file that cannot be read is left out and named in the summary, unless none can be.
/// Files are read one at a time and each one's bytes let go once its rows are taken,
/// so a directory never holds more than one file's bytes.
pub fn read_midi(paths: &[PathBuf]) -> Result<(LazyFrame, MidiSummary)> {
    if paths.is_empty() {
        return Err(eyre!("No MIDI files to read"));
    }
    let names: Vec<String> = paths
        .iter()
        .map(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| p.display().to_string())
        })
        .collect();
    let many = paths.len() > 1;
    let mut cols = Columns::default();
    let mut summary = MidiSummary::default();
    let mut unreadable = Vec::new();
    for (path, name) in paths.iter().zip(&names) {
        let read = read_bytes(path);
        let parsed = read.as_deref().map_err(|e| eyre!("{e}")).and_then(parse);
        let smf = match parsed {
            Ok(smf) => smf,
            Err(e) if !many => return Err(e),
            Err(e) => {
                unreadable.push((name.clone(), e.to_string()));
                continue;
            }
        };
        let events = smf.tracks.iter().map(Vec::len).sum::<usize>();
        if summary.events + events > MAX_EVENTS {
            return Err(eyre!(
                "These MIDI files have more than {MAX_EVENTS} events; datui reads up to that many"
            ));
        }
        add_file(&smf, many.then_some(name.as_str()), &mut cols, &mut summary);
    }
    if summary.files == 0 {
        let (name, why) = unreadable
            .first()
            .cloned()
            .unwrap_or_else(|| (String::new(), "no files".to_string()));
        return Err(eyre!("No MIDI file could be read; {name}: {why}"));
    }
    summary.unreadable = unreadable;
    Ok((frame(cols, many)?, summary))
}

/// What the open has to say about the events: notes that never end, and files that
/// could not be read.
pub fn notes(summary: &MidiSummary) -> Vec<crate::notes::Note> {
    let mut out = Vec::new();
    if summary.unended > 0 {
        let n = summary.unended;
        out.push(crate::notes::Note {
            summary: format!(
                "{} {} never {} — a note_on with no note_off after it; its length is null",
                crate::widgets::info::group_u64(n as u64),
                if n == 1 {
                    "note starts and"
                } else {
                    "notes start and"
                },
                if n == 1 { "ends" } else { "end" }
            ),
            scope: "from every event".to_string(),
            read_as_text: None,
            passed_over: None,
        });
    }
    if !summary.unreadable.is_empty() {
        let n = summary.unreadable.len();
        let (name, why) = &summary.unreadable[0];
        out.push(crate::notes::Note {
            summary: format!(
                "{} could not be read and {} left out — {name}: {why}",
                if n == 1 {
                    "1 file".to_string()
                } else {
                    format!("{} files", crate::widgets::info::group_u64(n as u64))
                },
                if n == 1 { "is" } else { "are" },
            ),
            scope: "from every file".to_string(),
            read_as_text: None,
            passed_over: None,
        });
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A variable-length quantity.
    pub(crate) fn vlq(mut n: u32) -> Vec<u8> {
        let mut out = vec![(n & 0x7f) as u8];
        n >>= 7;
        while n > 0 {
            out.insert(0, (n & 0x7f) as u8 | 0x80);
            n >>= 7;
        }
        out
    }

    /// A file from its header fields and each track's event bytes.
    pub(crate) fn smf(format: u16, division: u16, tracks: &[&[u8]]) -> Vec<u8> {
        let mut out = b"MThd\0\0\0\x06".to_vec();
        out.extend_from_slice(&format.to_be_bytes());
        out.extend_from_slice(&(tracks.len() as u16).to_be_bytes());
        out.extend_from_slice(&division.to_be_bytes());
        for t in tracks {
            out.extend_from_slice(b"MTrk");
            out.extend_from_slice(&(t.len() as u32).to_be_bytes());
            out.extend_from_slice(t);
        }
        out
    }

    fn table(bytes: &[u8]) -> (DataFrame, MidiSummary) {
        let smf = parse(bytes).unwrap();
        let (lf, summary) = build(&[("a.mid".to_string(), smf)]).unwrap();
        (lf.collect().unwrap(), summary)
    }

    fn col<'a>(df: &'a DataFrame, name: &str) -> &'a Series {
        df.column(name).unwrap().as_materialized_series()
    }

    #[test]
    fn vlq_reads_the_specification_examples() {
        for (bytes, value) in [
            (&[0x00][..], 0u32),
            (&[0x40], 0x40),
            (&[0x7f], 0x7f),
            (&[0x81, 0x00], 0x80),
            (&[0xc0, 0x00], 0x2000),
            (&[0xff, 0x7f], 0x3fff),
            (&[0x81, 0x80, 0x00], 0x4000),
            (&[0xff, 0xff, 0x7f], 0x1f_ffff),
            (&[0x81, 0x80, 0x80, 0x00], 0x20_0000),
            (&[0xff, 0xff, 0xff, 0x7f], 0x0fff_ffff),
        ] {
            assert_eq!(Bytes::new(bytes).vlq().ok(), Some(value), "{bytes:02x?}");
            assert_eq!(vlq(value), bytes);
        }
        assert!(matches!(
            Bytes::new(&[0x80, 0x80, 0x80, 0x80, 0x00]).vlq(),
            Err(VlqError::TooLong)
        ));
        assert!(matches!(Bytes::new(&[0x81]).vlq(), Err(VlqError::CutShort)));
    }

    #[test]
    fn running_status_repeats_the_last_channel_status() {
        // Note on C4, then E4 and G4 by running status, then a note on at velocity 0,
        // which is a note off.
        let track = [
            0x00, 0x90, 60, 100, 0x00, 64, 90, 0x00, 67, 80, 0x60, 60, 0, 0x00, 0xff, 0x2f, 0x00,
        ];
        let (df, summary) = table(&smf(0, 96, &[&track]));
        let kinds: Vec<_> = col(&df, "kind").str().unwrap().iter().flatten().collect();
        assert_eq!(
            kinds,
            ["note_on", "note_on", "note_on", "note_off", "end_of_track"]
        );
        let names: Vec<_> = col(&df, "note_name").str().unwrap().iter().collect();
        assert_eq!(names[..4], [Some("C4"), Some("E4"), Some("G4"), Some("C4")]);
        assert_eq!(col(&df, "channel").u8().unwrap().get(0), Some(1));
        // The narrowest type each holds: a track number past 255 is rare but legal.
        for (name, dtype) in [
            ("track", DataType::UInt16),
            ("channel", DataType::UInt8),
            ("note", DataType::UInt8),
            ("velocity", DataType::UInt8),
            ("controller", DataType::UInt8),
            ("value", DataType::Int32),
        ] {
            assert_eq!(df.column(name).unwrap().dtype(), &dtype, "{name}");
        }
        assert_eq!(summary.notes, 3);
        assert_eq!(summary.unended, 2, "E4 and G4 never end");
        // 96 ticks at 120 bpm and 96 per quarter is half a second.
        let length = col(&df, "length").duration().unwrap().physical().get(0);
        assert_eq!(length, Some(500_000));
        assert_eq!(col(&df, "length").null_count(), 4);
    }

    #[test]
    fn a_data_byte_with_no_status_is_an_error() {
        let err = parse(&smf(0, 96, &[&[0x00, 60, 100]])).unwrap_err();
        assert!(err.to_string().contains("no status"), "{err}");
        // Meta events cancel running status.
        let track = [0x00, 0x90, 60, 100, 0x00, 0xff, 0x01, 0x00, 0x00, 60, 0];
        assert!(parse(&smf(0, 96, &[&track])).is_err());
        // So do sysex and escape packets.
        for sysex in [0xf0, 0xf7] {
            let track = [0x00, 0x90, 60, 100, 0x00, sysex, 0x01, 0xf7, 0x00, 60, 0];
            assert!(parse(&smf(0, 96, &[&track])).is_err(), "{sysex:02X}");
        }
        // And system common messages; 0xF4 has no definition, so no length to skip.
        let track = [0x00, 0x90, 60, 100, 0x00, 0xf3, 0x02, 0x00, 60, 0];
        assert!(parse(&smf(0, 96, &[&track])).is_err());
        let err = parse(&smf(0, 96, &[&[0x00, 0xf4]])).unwrap_err();
        assert!(err.to_string().contains("undefined"), "{err}");
    }

    /// A real-time byte a file should not hold is read as itself, and the note after
    /// it still has the status before it, as on the wire.
    #[test]
    fn real_time_bytes_keep_running_status() {
        let track = [
            0x00, 0x90, 60, 100, 0x00, 0xf8, 0x00, 60, 0, 0x00, 0xf2, 0x10, 0x01, 0x00, 0xff, 0x2f,
            0x00,
        ];
        let (df, _) = table(&smf(0, 96, &[&track]));
        let kinds: Vec<_> = col(&df, "kind").str().unwrap().iter().flatten().collect();
        assert_eq!(
            kinds,
            [
                "note_on",
                "clock",
                "note_off",
                "song_position",
                "end_of_track"
            ]
        );
        assert_eq!(col(&df, "value").i32().unwrap().get(3), Some(0x90));
    }

    /// Hostile lengths: past the end, overlong quantities, more tracks than are there.
    #[test]
    fn lengths_are_checked_before_use() {
        // A meta event claiming 2^28 - 1 bytes.
        let track = [0x00, 0xff, 0x01, 0xff, 0xff, 0xff, 0x7f, b'a'];
        assert!(parse(&smf(0, 96, &[&track])).is_err());
        // A track chunk longer than the file.
        let mut bytes = smf(0, 96, &[&[0x00, 0xff, 0x2f, 0x00]]);
        bytes[18..22].copy_from_slice(&u32::MAX.to_be_bytes());
        let err = parse(&bytes).unwrap_err().to_string();
        assert!(err.contains("cut short"), "{err}");
        // 65,535 tracks declared, one present.
        let mut bytes = smf(1, 96, &[&[0x00, 0xff, 0x2f, 0x00]]);
        bytes[10..12].copy_from_slice(&u16::MAX.to_be_bytes());
        let err = parse(&bytes).unwrap_err().to_string();
        assert!(err.contains("65535 tracks"), "{err}");
        // A delta of five bytes.
        let track = [0x80, 0x80, 0x80, 0x80, 0x00, 0xff, 0x2f, 0x00];
        assert!(parse(&smf(0, 96, &[&track])).is_err());
        // An event cut off by the end of its chunk.
        assert!(parse(&smf(0, 96, &[&[0x00, 0x90, 60]])).is_err());
        // Not MIDI, or a header that says nothing usable.
        assert!(parse(b"MThd").is_err());
        assert!(parse(&smf(3, 96, &[&[]])).is_err());
        assert!(parse(&smf(0, 0, &[&[]])).is_err());
        assert!(parse(&smf(0, 96, &[])).is_err());
    }

    #[test]
    fn the_tempo_map_turns_ticks_into_time() {
        // Format 1: the tempo track sets 120 bpm, then 60 bpm at tick 480. The other
        // track has notes at 0, 480 and 960 ticks of 480 per quarter.
        let mut tempo = vec![0x00, 0xff, 0x51, 0x03, 0x07, 0xa1, 0x20];
        tempo.extend(vlq(480));
        tempo.extend([0xff, 0x51, 0x03, 0x0f, 0x42, 0x40, 0x00, 0xff, 0x2f, 0x00]);
        let mut notes = vec![0x00, 0x90, 60, 100];
        notes.extend(vlq(480));
        notes.extend([62, 100]);
        notes.extend(vlq(480));
        notes.extend([64, 100]);
        let (df, summary) = table(&smf(1, 480, &[&tempo, &notes]));
        let times: Vec<_> = col(&df, "time")
            .duration()
            .unwrap()
            .physical()
            .into_no_null_iter()
            .collect();
        // Tempo track: 0, 0.5 s, 0.5 s (end of track); notes: 0, 0.5 s, 1.5 s.
        assert_eq!(times, [0, 500_000, 500_000, 0, 500_000, 1_500_000]);
        assert_eq!(summary.tempo, Some((500_000, 500_000, 1_000_000)));
        assert_eq!(summary.length_micros, 1_500_000);
        let text = col(&df, "text").str().unwrap();
        assert_eq!(text.get(0), Some("120 bpm"));
        assert_eq!(text.get(1), Some("60 bpm"));
        assert_eq!(summary.tracks.len(), 2);
        assert_eq!(summary.tracks[1].channels, [1]);
    }

    #[test]
    fn smpte_time_ignores_tempo() {
        // 25 fps, 40 ticks per frame: 1000 ticks a second.
        let division = (((-25i8) as u8 as u16) << 8) | 40;
        let mut track = vec![0x00, 0xff, 0x51, 0x03, 0x0f, 0x42, 0x40];
        track.extend(vlq(1500));
        track.extend([0x90, 60, 1]);
        let (df, summary) = table(&smf(0, division, &[&track]));
        let time = col(&df, "time").duration().unwrap().physical().get(1);
        assert_eq!(time, Some(1_500_000));
        assert_eq!(
            summary.division.map(Division::label).as_deref(),
            Some("25 fps, 40 ticks per frame")
        );
    }

    #[test]
    fn format_2_tracks_keep_their_own_tempo() {
        let mut slow = vec![0x00, 0xff, 0x51, 0x03, 0x0f, 0x42, 0x40];
        slow.extend(vlq(96));
        slow.extend([0x90, 60, 1]);
        let mut plain = vlq(96);
        plain.extend([0x90, 60, 1]);
        let (df, _) = table(&smf(2, 96, &[&slow, &plain]));
        let times: Vec<_> = col(&df, "time")
            .duration()
            .unwrap()
            .physical()
            .into_no_null_iter()
            .collect();
        assert_eq!(times, [0, 1_000_000, 500_000]);
    }

    #[test]
    fn meta_and_channel_events_fill_their_columns() {
        let mut t = vec![];
        t.extend([0x00, 0xff, 0x03, 0x05]);
        t.extend(b"Piano");
        t.extend([0x00, 0xff, 0x58, 0x04, 6, 3, 24, 8]);
        t.extend([0x00, 0xff, 0x59, 0x02, 0xfd, 0x01]); // 3 flats, minor
        t.extend([0x00, 0xff, 0x05, 0x02, 0xe9, b'a']); // Latin-1 lyric
        t.extend([0x00, 0xb3, 64, 127]); // sustain on, channel 4
        t.extend([0x00, 0xc3, 5]);
        t.extend([0x00, 0xe3, 0x00, 0x00]); // pitch bend all the way down
        t.extend([0x00, 0xf0, 0x03, 0x7e, 0x7f, 0xf7]);
        let (df, summary) = table(&smf(0, 96, &[&t]));
        let kind = col(&df, "kind").str().unwrap();
        let text = col(&df, "text").str().unwrap();
        let value = col(&df, "value").i32().unwrap();
        assert_eq!(kind.get(1), Some("time_signature"));
        assert_eq!(text.get(1), Some("6/8"));
        assert_eq!(text.get(2), Some("C minor"));
        assert_eq!(value.get(2), Some(-3));
        assert_eq!(text.get(3), Some("éa"));
        assert_eq!(kind.get(4), Some("cc"));
        assert_eq!(col(&df, "controller").u8().unwrap().get(4), Some(64));
        assert_eq!(value.get(4), Some(127));
        assert_eq!(col(&df, "channel").u8().unwrap().get(4), Some(4));
        assert_eq!(value.get(5), Some(5));
        assert_eq!(value.get(6), Some(-8192));
        assert_eq!(kind.get(7), Some("sysex"));
        assert_eq!(text.get(7), Some("F0 7E 7F F7"));
        assert_eq!(summary.tracks[0].name.as_deref(), Some("Piano"));
        assert_eq!(summary.time_signature.as_deref(), Some("6/8"));
        assert_eq!(summary.key.as_deref(), Some("C minor"));
    }

    #[test]
    fn note_names_put_middle_c_in_octave_4() {
        assert_eq!(note_name(60), "C4");
        assert_eq!(note_name(0), "C-1");
        assert_eq!(note_name(69), "A4");
        assert_eq!(note_name(127), "G9");
        assert_eq!(key_name(0, false).as_deref(), Some("C major"));
        assert_eq!(key_name(7, false).as_deref(), Some("C# major"));
        assert_eq!(key_name(-7, true).as_deref(), Some("Ab minor"));
        assert_eq!(key_name(8, false), None);
        assert_eq!(bpm(500_000), "120");
        assert_eq!(bpm(650_000), "92.31");
    }

    #[test]
    fn midi_is_known_by_its_first_bytes_and_unwrapped_from_riff() {
        let inner = smf(0, 96, &[&[0x00, 0x90, 60, 100]]);
        assert!(looks_like_midi(&inner));
        let mut riff = b"RIFF".to_vec();
        riff.extend(((inner.len() + 12) as u32).to_le_bytes());
        riff.extend(b"RMIDdata");
        riff.extend((inner.len() as u32).to_le_bytes());
        riff.extend(&inner);
        assert!(looks_like_midi(&riff));
        assert_eq!(parse(&riff).unwrap().tracks[0].len(), 1);
        assert!(!looks_like_midi(b"RIFF\0\0\0\0WAVEfmt "));
        assert!(!looks_like_midi(b"MThd\0\0\0\x07"));
    }

    /// Of a directory, a file too large or broken is left out and named; alone, it is
    /// the error. The `file` column stays when only one of the files is read.
    #[test]
    fn a_directory_leaves_out_what_it_cannot_read() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.mid");
        std::fs::write(&good, smf(0, 96, &[&[0x00, 0x90, 60, 100]])).unwrap();
        let huge = dir.path().join("huge.mid");
        // Sparse: the size is all that is read before the refusal.
        std::fs::File::create(&huge)
            .unwrap()
            .set_len(MAX_FILE_BYTES + 1)
            .unwrap();
        let err = read_midi(std::slice::from_ref(&huge))
            .err()
            .expect("a file too large is refused")
            .to_string();
        assert!(err.contains("up to 64.0 MiB"), "{err}");
        let (lf, summary) = read_midi(&[huge, good]).unwrap();
        let df = lf.collect().unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(col(&df, "file").str().unwrap().get(0), Some("good.mid"));
        assert_eq!(summary.unreadable.len(), 1);
        assert_eq!(summary.unreadable[0].0, "huge.mid");
    }

    #[test]
    fn unknown_chunks_are_skipped_and_trailing_bytes_ignored() {
        let mut bytes = smf(0, 96, &[]);
        bytes[11] = 1;
        bytes.extend(b"XFIH\0\0\0\x02ab");
        bytes.extend(b"MTrk\0\0\0\x04\x00\xff\x2f\x00");
        bytes.extend(b"junk");
        assert_eq!(parse(&bytes).unwrap().tracks.len(), 1);
    }
}

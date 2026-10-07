//! GPX tracks, routes and waypoints: one row per `trkpt`, `rtept` or `wpt`.
//!
//! GPX is XML, read here by a small scanner of its own rather than an XML crate. The
//! one in the tree is optional (it comes with the cloud feature), and its streaming
//! reader buffers each event whole, so a text node of gigabytes is gigabytes. GPX
//! needs a sliver of XML: elements, attributes, text, the five entities and character
//! references. Everything else (comments, CDATA, processing instructions, a DOCTYPE)
//! is passed over, and entities a DOCTYPE declares are never expanded.
//!
//! Every length is bounded by the reader: markup (a tag with its attributes, a
//! comment) is at most [`MAX_MARKUP`] bytes, text is kept to [`MAX_TEXT`] per value,
//! elements nest at most [`MAX_DEPTH`] deep, and a file adds at most
//! `limits.gpx_fields` columns. Text between tags is never buffered past what is
//! kept, however long.

use polars::prelude::*;

use crate::columns::{Builder, Cell, Kind};

/// The longest tag, comment or other markup read whole.
pub const MAX_MARKUP: usize = 1 << 20;
/// The most of one value kept. A waypoint's description is a sentence.
pub const MAX_TEXT: usize = 4096;
/// How deep elements may nest. GPX's own structure is five deep.
pub const MAX_DEPTH: usize = 64;
/// The longest field name made a column.
const MAX_NAME: usize = 64;
/// Rows held before they are handed over as a batch.
pub const BATCH_ROWS: usize = 65_536;
/// Text held before rows are handed over, however few: a point may carry a few
/// hundred fields of [`MAX_TEXT`] each.
pub const BATCH_TEXT: usize = 32 << 20;

/// The columns every GPX table has, in order. Fields found in the file follow.
pub const CORE: [(&str, Kind); 9] = [
    ("time", Kind::Time),
    ("lat", Kind::F64),
    ("lon", Kind::F64),
    ("ele", Kind::F64),
    ("kind", Kind::Str),
    ("track", Kind::U32),
    ("track_name", Kind::Str),
    ("segment", Kind::U32),
    ("gap", Kind::F64),
];

/// What an element is to the reader, by where it sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Gpx,
    Track,
    Route,
    Segment,
    /// A track's or route's `name`.
    TrackName,
    Point,
    /// A child of a point: `ele`, `time`, `name`, `sat`, `link`…
    PointField,
    Extensions,
    /// Anything inside `extensions`; a leaf is a column.
    ExtField,
    Other,
}

#[derive(Debug)]
struct Frame {
    name: String,
    role: Role,
    /// Its text, raw (entities still escaped), up to [`MAX_TEXT`].
    text: Vec<u8>,
    children: bool,
    /// A `link`'s `href`.
    href: Option<String>,
}

#[derive(Debug, Default)]
struct Point {
    kind: &'static str,
    lat: Option<f64>,
    lon: Option<f64>,
    ele: Option<f64>,
    time: Option<i64>,
    fields: Vec<(usize, String)>,
}

/// What one field column held, to type it once the file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldColumn {
    pub name: String,
    /// Every value seen parses as an integer.
    pub integers: bool,
    /// Every value seen parses as a number.
    pub numbers: bool,
}

/// What a read counted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub points: u64,
    pub tracks: u64,
    pub routes: u64,
    pub waypoints: u64,
    /// Values of fields not made columns, past `limits.gpx_fields`.
    pub fields_dropped: u64,
    /// Values of fields not made columns, their names too long for one.
    pub long_names: u64,
    /// Times that are not ISO 8601, left empty.
    pub bad_times: u64,
    /// The file ended inside an element.
    pub truncated: bool,
}

/// A GPX file read a piece at a time.
pub struct GpxReader {
    buf: Vec<u8>,
    pos: usize,
    stack: Vec<Frame>,
    root_seen: bool,
    rows: Builder,
    fields: Vec<FieldColumn>,
    point: Option<Point>,
    track: Option<u32>,
    track_name: Option<String>,
    segment: Option<u32>,
    segments: u32,
    stats: Stats,
    /// The time of the last point of the segment being read, for the next one's `gap`.
    last_time: Option<i64>,
    /// Bytes of text in the rows not yet taken.
    held: usize,
    /// `limits.gpx_fields`, read once for the file.
    max_fields: usize,
}

impl Default for GpxReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether the first bytes of a file are GPX: XML whose first element is `gpx`.
pub fn looks_like(head: &[u8]) -> bool {
    let head = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let mut rest = head.trim_ascii_start();
    // The declaration, comments and processing instructions before the root.
    loop {
        if let Some(after) = rest.strip_prefix(b"<?") {
            let Some(end) = find(after, b"?>") else {
                return false;
            };
            rest = after[end + 2..].trim_ascii_start();
        } else if let Some(after) = rest.strip_prefix(b"<!--") {
            let Some(end) = find(after, b"-->") else {
                return false;
            };
            rest = after[end + 3..].trim_ascii_start();
        } else {
            break;
        }
    }
    rest.strip_prefix(b"<gpx")
        .and_then(|r| r.first())
        .is_some_and(|b| b.is_ascii_whitespace() || *b == b'>' || *b == b'/')
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Text with its entities and character references replaced. An entity that is not
/// one of XML's five is left as written.
fn unescape(raw: &[u8]) -> String {
    let text = String::from_utf8_lossy(raw);
    if !text.contains('&') {
        return text.into_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = &*text;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        // A reference is short; the window ends on a character, not inside one.
        let Some(semi) = rest[..rest.floor_char_boundary(12)].find(';') else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let entity = &rest[1..semi];
        let ch = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .map(|hex| u32::from_str_radix(hex, 16))
                .or_else(|| entity.strip_prefix('#').map(str::parse::<u32>))
                .and_then(|n| n.ok())
                .and_then(char::from_u32),
        };
        match ch {
            Some(ch) => {
                out.push(ch);
                rest = &rest[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// A time as GPX writes it, ISO 8601 (`2024-05-01T12:00:00Z`, with fractions or an
/// offset), as milliseconds since the epoch. One with no offset is UTC.
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(t.timestamp_millis());
    }
    // ISO 8601's basic offset, `+0200`, which RFC 3339 does not allow.
    if let Ok(t) = chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%z") {
        return Some(t.timestamp_millis());
    }
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|f| chrono::NaiveDateTime::parse_from_str(s, f).ok())
        .map(|t| t.and_utc().timestamp_millis())
}

fn coordinate(s: Option<&str>, limit: f64) -> Option<f64> {
    s?.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && v.abs() <= limit)
}

/// The name without its namespace prefix: `gpxtpx:hr` is `hr`.
fn local(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, l)| l)
}

/// A tag's name and attributes.
struct Tag<'a> {
    name: &'a str,
    attrs: Vec<(&'a str, String)>,
    closing: bool,
    empty: bool,
}

/// Parse the markup between `<` and `>`, both excluded.
fn parse_tag(inner: &[u8]) -> Option<Tag<'_>> {
    let text = std::str::from_utf8(inner).ok()?;
    let (closing, text) = match text.strip_prefix('/') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (empty, text) = match text.strip_suffix('/') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let end = text
        .find(|c: char| c.is_ascii_whitespace())
        .unwrap_or(text.len());
    let name = &text[..end];
    if name.is_empty() {
        return None;
    }
    let mut attrs = Vec::new();
    let mut rest = &text[end..];
    while let Some(eq) = rest.find('=') {
        let key = rest[..eq].trim();
        let after = rest[eq + 1..].trim_start();
        let quote = after.chars().next()?;
        if quote != '"' && quote != '\'' {
            return None;
        }
        let close = after[1..].find(quote)?;
        attrs.push((local(key), unescape(&after.as_bytes()[1..1 + close])));
        rest = &after[close + 2..];
    }
    Some(Tag {
        name: local(name),
        attrs,
        closing,
        empty,
    })
}

/// The end of the markup starting at `bytes[0] == b'<'`, just past its `>`; `None`
/// when it has not all arrived.
fn markup_end(bytes: &[u8]) -> Option<usize> {
    let after = |start: usize, close: &[u8]| {
        bytes
            .get(start..)
            .and_then(|b| find(b, close))
            .map(|at| start + at + close.len())
    };
    if bytes.starts_with(b"<!--") {
        after(4, b"-->")
    } else if bytes.starts_with(b"<![CDATA[") {
        after(9, b"]]>")
    } else if bytes.starts_with(b"<?") {
        after(2, b"?>")
    } else if bytes.starts_with(b"<!") {
        // A DOCTYPE, whose internal subset holds `>` of its own.
        let gt = bytes.iter().position(|&b| b == b'>')?;
        let Some(open) = bytes[..gt].iter().position(|&b| b == b'[') else {
            return Some(gt + 1);
        };
        // The subset ends at a `]` followed, past any whitespace, by the `>`.
        let mut at = open;
        loop {
            at += 1 + bytes.get(at + 1..)?.iter().position(|&b| b == b']')?;
            let rest = &bytes[at + 1..];
            let space = rest.iter().take_while(|b| b.is_ascii_whitespace()).count();
            match rest.get(space) {
                Some(b'>') => return Some(at + 1 + space + 1),
                Some(_) => {}
                None => return None,
            }
        }
    } else {
        // A tag: its `>`, outside quoted attribute values.
        let mut quote = None;
        for (i, &b) in bytes.iter().enumerate().skip(1) {
            match (quote, b) {
                (None, b'"' | b'\'') => quote = Some(b),
                (Some(q), b) if b == q => quote = None,
                (None, b'>') => return Some(i + 1),
                _ => {}
            }
        }
        None
    }
}

/// Whether `bytes` could still become one of the longer markup openings once more
/// arrives: a `<!` or `<!-` that is not yet a comment, CDATA or DOCTYPE.
fn undecided(bytes: &[u8]) -> bool {
    bytes.len() < 9 && (b"<![CDATA[".starts_with(bytes) || b"<!--".starts_with(bytes))
}

impl GpxReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
            stack: Vec::new(),
            root_seen: false,
            rows: Builder::new(&CORE),
            fields: Vec::new(),
            point: None,
            track: None,
            track_name: None,
            segment: None,
            segments: 0,
            stats: Stats::default(),
            held: 0,
            last_time: None,
            max_fields: crate::limits::get().gpx_fields,
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The columns the file's fields made, in order after [`CORE`].
    pub fn fields(&self) -> &[FieldColumn] {
        &self.fields
    }

    /// Read `bytes`, the next piece of the file.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), String> {
        // What was read is let go before the buffer grows.
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        self.buf.extend_from_slice(bytes);
        self.scan(false)
    }

    /// A full batch, once one is held: [`BATCH_ROWS`] rows, or [`BATCH_TEXT`] of text.
    pub fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.rows.len() < BATCH_ROWS && self.held < BATCH_TEXT {
            return Ok(None);
        }
        self.held = 0;
        self.rows.take().map(Some)
    }

    /// The end of the file: what is left, and the rows not yet taken.
    pub fn finish(&mut self) -> Result<DataFrame, String> {
        self.scan(true)?;
        if !self.root_seen {
            return Err("Not a GPX file: it has no <gpx> element.".to_string());
        }
        if !self.stack.is_empty() || self.pos < self.buf.len() {
            self.stats.truncated = true;
        }
        self.rows.take().map_err(|e| e.to_string())
    }

    fn scan(&mut self, at_end: bool) -> Result<(), String> {
        while self.pos < self.buf.len() {
            let rest = &self.buf[self.pos..];
            if rest[0] != b'<' {
                let end = rest.iter().position(|&b| b == b'<').unwrap_or(rest.len());
                let (start, stop) = (self.pos, self.pos + end);
                self.text(start, stop, false);
                self.pos = stop;
                continue;
            }
            if !at_end && undecided(rest) {
                return Ok(());
            }
            let Some(len) = markup_end(rest) else {
                if rest.len() > MAX_MARKUP {
                    return Err(format!(
                        "The GPX file has a tag or comment longer than {} MiB.",
                        MAX_MARKUP >> 20
                    ));
                }
                // The rest has not arrived, or never will: `finish` says so.
                return Ok(());
            };
            if len > MAX_MARKUP {
                return Err(format!(
                    "The GPX file has a tag or comment longer than {} MiB.",
                    MAX_MARKUP >> 20
                ));
            }
            let (start, stop) = (self.pos, self.pos + len);
            self.pos = stop;
            if self.buf[start..stop].starts_with(b"<![CDATA[") {
                self.text(start + 9, stop - 3, true);
            } else if !(self.buf[start + 1] == b'!' || self.buf[start + 1] == b'?') {
                let inner = self.buf[start + 1..stop - 1].to_vec();
                if let Some(tag) = parse_tag(&inner) {
                    if tag.closing {
                        self.end(tag.name);
                    } else {
                        self.start(&tag)?;
                        if tag.empty {
                            self.end(tag.name);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Text from `buf[start..stop]`, kept by the element it is in if that element
    /// is a value. CDATA is escaped as it is kept, so unescaping gives it back.
    fn text(&mut self, start: usize, stop: usize, cdata: bool) {
        let Some(frame) = self.stack.last_mut() else {
            return;
        };
        if !matches!(
            frame.role,
            Role::TrackName | Role::PointField | Role::ExtField
        ) {
            return;
        }
        let room = MAX_TEXT.saturating_sub(frame.text.len());
        if room == 0 {
            return;
        }
        let piece = &self.buf[start..stop];
        if cdata {
            for &b in piece {
                if frame.text.len() >= MAX_TEXT {
                    break;
                }
                match b {
                    b'&' => frame.text.extend_from_slice(b"&amp;"),
                    b => frame.text.push(b),
                }
            }
        } else {
            frame
                .text
                .extend_from_slice(&piece[..piece.len().min(room)]);
        }
    }

    fn start(&mut self, tag: &Tag) -> Result<(), String> {
        if self.stack.len() >= MAX_DEPTH {
            return Err(format!(
                "The GPX file nests elements more than {MAX_DEPTH} deep."
            ));
        }
        if !self.root_seen {
            if tag.name != "gpx" {
                return Err(format!(
                    "Not a GPX file: its first element is <{}>.",
                    tag.name.chars().take(40).collect::<String>()
                ));
            }
            self.root_seen = true;
        } else if self.stack.is_empty() {
            // A second root, after the first closed: not GPX's, passed over.
            self.stack.push(Frame::new(tag.name, Role::Other));
            return Ok(());
        }
        let parent = self.stack.last_mut().map(|frame| {
            frame.children = true;
            frame.role
        });
        let role = match (parent, tag.name) {
            (None, _) => Role::Gpx,
            (Some(Role::Gpx), "trk") => {
                self.track = Some(self.stats.tracks as u32);
                self.stats.tracks += 1;
                self.track_name = None;
                self.segments = 0;
                Role::Track
            }
            (Some(Role::Gpx), "rte") => {
                self.track = Some(self.stats.routes as u32);
                self.stats.routes += 1;
                self.track_name = None;
                Role::Route
            }
            (Some(Role::Gpx), "wpt") => self.point_starts("waypoint", tag),
            (Some(Role::Track), "trkseg") => {
                self.segment = Some(self.segments);
                self.last_time = None;
                self.segments = self.segments.saturating_add(1);
                Role::Segment
            }
            (Some(Role::Track | Role::Route), "name") => Role::TrackName,
            (Some(Role::Route), "rtept") => self.point_starts("route", tag),
            (Some(Role::Segment), "trkpt") => self.point_starts("track", tag),
            (Some(Role::Point), "extensions") => Role::Extensions,
            (Some(Role::Point), _) => Role::PointField,
            (Some(Role::Extensions | Role::ExtField), _) => Role::ExtField,
            _ => Role::Other,
        };
        let mut frame = Frame::new(tag.name, role);
        if role == Role::PointField && tag.name == "link" {
            frame.href = tag
                .attrs
                .iter()
                .find(|(k, _)| *k == "href")
                .map(|(_, v)| v.clone());
        }
        self.stack.push(frame);
        Ok(())
    }

    fn point_starts(&mut self, kind: &'static str, tag: &Tag) -> Role {
        let attr = |name: &str| {
            tag.attrs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.as_str())
        };
        self.point = Some(Point {
            kind,
            lat: coordinate(attr("lat"), 90.0),
            lon: coordinate(attr("lon"), 180.0),
            ..Point::default()
        });
        Role::Point
    }

    fn end(&mut self, name: &str) {
        // An end tag closes its element and any left open inside it; one that closes
        // nothing open is passed over.
        let Some(at) = self.stack.iter().rposition(|frame| frame.name == name) else {
            return;
        };
        while self.stack.len() > at {
            let frame = self.stack.pop().expect("above `at`");
            self.close(frame);
        }
    }

    fn close(&mut self, frame: Frame) {
        match frame.role {
            Role::Track | Role::Route => {
                self.track = None;
                self.track_name = None;
                self.segment = None;
            }
            Role::Segment => self.segment = None,
            Role::TrackName => {
                let name = unescape(&frame.text).trim().to_string();
                self.track_name = (!name.is_empty()).then_some(name);
            }
            Role::Point => self.point_ends(),
            Role::PointField => {
                let value = match frame.href {
                    Some(href) => href,
                    None => unescape(&frame.text).trim().to_string(),
                };
                let Some(point) = self.point.as_mut() else {
                    return;
                };
                match frame.name.as_str() {
                    "ele" => point.ele = value.parse().ok().filter(|v: &f64| v.is_finite()),
                    "time" => {
                        point.time = parse_time(&value);
                        if point.time.is_none() && !value.is_empty() {
                            self.stats.bad_times += 1;
                        }
                    }
                    name => self.field(name, value),
                }
            }
            Role::ExtField if !frame.children => {
                let value = unescape(&frame.text).trim().to_string();
                self.field(&frame.name, value);
            }
            _ => {}
        }
    }

    /// A value for the point being read, in the column its name makes.
    fn field(&mut self, name: &str, value: String) {
        if value.is_empty() || self.point.is_none() {
            return;
        }
        // A field named like a core column (an extension's own `time`) is told apart.
        let name = if CORE.iter().any(|(core, _)| *core == name) {
            format!("ext_{name}")
        } else {
            name.to_string()
        };
        let column = match self.fields.iter().position(|f| f.name == name) {
            Some(at) => at,
            None if name.len() > MAX_NAME => {
                self.stats.long_names += 1;
                return;
            }
            None if self.fields.len() < self.max_fields => {
                self.rows.add_column(&name, Kind::Str);
                self.fields.push(FieldColumn {
                    name,
                    integers: true,
                    numbers: true,
                });
                self.fields.len() - 1
            }
            None => {
                self.stats.fields_dropped += 1;
                return;
            }
        };
        let info = &mut self.fields[column];
        info.integers &= value.parse::<i64>().is_ok();
        info.numbers &= value.parse::<f64>().is_ok_and(f64::is_finite);
        let point = self.point.as_mut().expect("checked above");
        if !point.fields.iter().any(|(at, _)| *at == column) {
            point.fields.push((column, value));
        }
    }

    fn point_ends(&mut self) {
        let Some(point) = self.point.take() else {
            return;
        };
        self.stats.points += 1;
        if point.kind == "waypoint" {
            self.stats.waypoints += 1;
        }
        let in_track = point.kind != "waypoint";
        // Seconds since the point before, in a track segment only: routes and
        // waypoints are places, not a recording. None when time steps back further
        // than a clock settling, as NMEA's.
        let gap = match (point.kind, point.time) {
            ("track", Some(time)) => self
                .last_time
                .replace(time)
                .map(|last| time - last)
                .filter(|ms| *ms >= -super::nmea::BACK_MS)
                .map(|ms| ms as f64 / 1000.0),
            _ => None,
        };
        let cells = [
            Cell::Time(point.time),
            Cell::F64(point.lat),
            Cell::F64(point.lon),
            Cell::F64(point.ele),
            Cell::Str(Some(point.kind.to_string())),
            Cell::U32(self.track.filter(|_| in_track)),
            Cell::Str(self.track_name.clone().filter(|_| in_track)),
            Cell::U32(self.segment.filter(|_| point.kind == "track")),
            Cell::F64(gap),
        ];
        self.held += point.fields.iter().map(|(_, v)| v.len()).sum::<usize>()
            + self.track_name.as_ref().map_or(0, String::len);
        let core = cells.into_iter().enumerate();
        let fields = point
            .fields
            .into_iter()
            .map(|(at, value)| (CORE.len() + at, Cell::Str(Some(value))));
        self.rows.push_sparse(core.chain(fields));
    }
}

impl Frame {
    fn new(name: &str, role: Role) -> Self {
        Self {
            name: name.chars().take(MAX_NAME * 2).collect(),
            role,
            text: Vec::new(),
            children: false,
            href: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!-- written by hand -->
<gpx version="1.1" creator="test" xmlns:gpxtpx="http://example.com/tpx">
  <metadata><name>Morning</name><time>2024-05-01T06:00:00Z</time></metadata>
  <wpt lat="47.1" lon="8.5"><name>Start &amp; finish</name><sym>Flag</sym></wpt>
  <trk>
    <name><![CDATA[Ride & run]]></name>
    <trkseg>
      <trkpt lat="47.2" lon="8.6"><ele>410.5</ele><time>2024-05-01T06:00:01Z</time>
        <extensions><gpxtpx:TrackPointExtension><gpxtpx:hr>141</gpxtpx:hr><gpxtpx:cad>80</gpxtpx:cad></gpxtpx:TrackPointExtension></extensions>
      </trkpt>
      <trkpt lat='47.3' lon='8.7'><ele>411</ele><time>2024-05-01T08:00:02.500+02:00</time>
        <extensions><gpxtpx:TrackPointExtension><gpxtpx:hr>142</gpxtpx:hr><gpxtpx:cad>n/a</gpxtpx:cad></gpxtpx:TrackPointExtension></extensions>
      </trkpt>
    </trkseg>
    <trkseg><trkpt lat="47.4" lon="8.8"/></trkseg>
  </trk>
  <rte><name>Way</name><rtept lat="1" lon="2"><link href="http://x/?a=1&amp;b=2"><text>t</text></link></rtept></rte>
</gpx>"#;

    fn read_in(text: &[u8], piece: usize) -> (DataFrame, GpxReader) {
        let mut reader = GpxReader::new();
        for chunk in text.chunks(piece) {
            reader.push(chunk).unwrap();
        }
        let df = reader.finish().unwrap();
        (df, reader)
    }

    fn strs(df: &DataFrame, name: &str) -> Vec<Option<String>> {
        df.column(name)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn every_point_is_a_row() {
        // Whole, and a byte at a time: the pieces cut every token somewhere.
        for piece in [SAMPLE.len(), 1, 5] {
            let (df, reader) = read_in(SAMPLE.as_bytes(), piece);
            assert_eq!(df.height(), 5, "{df}");
            assert_eq!(
                strs(&df, "kind"),
                ["waypoint", "track", "track", "track", "route"].map(|s| Some(s.to_string()))
            );
            assert_eq!(
                strs(&df, "track_name"),
                [
                    None,
                    Some("Ride & run"),
                    Some("Ride & run"),
                    Some("Ride & run"),
                    Some("Way")
                ]
                .map(|s| s.map(str::to_string))
            );
            let segment: Vec<_> = df
                .column("segment")
                .unwrap()
                .u32()
                .unwrap()
                .iter()
                .collect();
            assert_eq!(segment, [None, Some(0), Some(0), Some(1), None]);
            let gap: Vec<_> = df.column("gap").unwrap().f64().unwrap().iter().collect();
            assert_eq!(gap, [None, None, Some(1.5), None, None], "within a segment");
            let time: Vec<_> = df
                .column("time")
                .unwrap()
                .datetime()
                .unwrap()
                .physical()
                .iter()
                .collect();
            assert_eq!(time[1], Some(1_714_543_201_000));
            assert_eq!(time[2], Some(1_714_543_202_500), "the offset is applied");
            assert_eq!(
                parse_time("2024-05-01T08:00:02.500+0200"),
                time[2],
                "an offset without its colon"
            );
            assert_eq!(
                strs(&df, "hr")[1..3],
                [Some("141".into()), Some("142".into())]
            );
            assert_eq!(strs(&df, "name")[0], Some("Start & finish".into()));
            assert_eq!(strs(&df, "link")[4], Some("http://x/?a=1&b=2".into()));
            let fields = reader.fields();
            let hr = fields.iter().find(|f| f.name == "hr").unwrap();
            assert!(hr.integers);
            let cad = fields.iter().find(|f| f.name == "cad").unwrap();
            assert!(!cad.numbers, "n/a is not a number");
            assert_eq!(reader.stats().points, 5);
            assert!(!reader.stats().truncated);
        }
    }

    #[test]
    fn not_gpx_and_cut_short() {
        let mut reader = GpxReader::new();
        assert!(reader.push(b"<kml><Document/></kml>").is_err());
        let mut reader = GpxReader::new();
        assert!(reader.finish().is_err(), "nothing at all");
        let cut = &SAMPLE.as_bytes()[..SAMPLE.find("</trkseg>").unwrap()];
        let (df, reader) = read_in(cut, 64);
        assert_eq!(df.height(), 3, "the points before the cut");
        assert!(reader.stats().truncated);
        assert!(looks_like(SAMPLE.as_bytes()));
        assert!(looks_like(b"<gpx>"));
        assert!(!looks_like(b"<gpxx>"));
        assert!(!looks_like(b"<?xml version='1.0'?><kml>"));
    }

    #[test]
    fn hostile_input_is_bounded() {
        // Points of long fields are handed over before their text passes BATCH_TEXT.
        let mut reader = GpxReader::new();
        reader.push(b"<gpx>").unwrap();
        let wide: String = (0..200)
            .map(|i| {
                let text = "z".repeat(MAX_TEXT);
                format!("<wpt lat=\"1\" lon=\"1\"><f{i}>{text}</f{i}></wpt>")
            })
            .collect();
        let mut batches = 0;
        for _ in 0..BATCH_TEXT / (200 * MAX_TEXT) + 2 {
            reader.push(wide.as_bytes()).unwrap();
            assert!(reader.rows.len() * MAX_TEXT <= BATCH_TEXT + 200 * MAX_TEXT);
            batches += usize::from(reader.take_batch().unwrap().is_some());
        }
        assert!(batches > 0);
        // A text node of megabytes keeps MAX_TEXT of it.
        let mut text = b"<gpx><wpt lat=\"1\" lon=\"2\"><desc>".to_vec();
        text.extend(std::iter::repeat_n(b'x', 3 * MAX_MARKUP));
        text.extend_from_slice(b"</desc></wpt></gpx>");
        let (df, _) = read_in(&text, 1 << 16);
        assert_eq!(strs(&df, "desc")[0].as_ref().unwrap().len(), MAX_TEXT);
        // A tag that never ends is refused once it passes MAX_MARKUP.
        let mut reader = GpxReader::new();
        reader.push(b"<gpx><wpt a=\"").unwrap();
        let junk = vec![b'y'; 1 << 16];
        let refused = (0..(MAX_MARKUP >> 16) + 2).any(|_| reader.push(&junk).is_err());
        assert!(refused);
        // Nesting past MAX_DEPTH is refused.
        let deep = "<a>".repeat(MAX_DEPTH + 1);
        let mut reader = GpxReader::new();
        assert!(reader.push(format!("<gpx>{deep}").as_bytes()).is_err());
        assert_eq!(unescape(b"&#x41;&#66;&bogus;&"), "AB&bogus;&");
        // A DOCTYPE's entities are never expanded, wherever its subset closes.
        let laughs = br#"<?xml version="1.0"?>
<!DOCTYPE gpx [
  <!ENTITY lol "lol">
  <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
] >
<gpx><wpt lat="1" lon="2"><name>&lol2;</name></wpt></gpx>"#;
        let (df, reader) = read_in(laughs, 7);
        assert_eq!(strs(&df, "name"), [Some("&lol2;".into())]);
        assert!(!reader.stats().truncated);
        // Not a reference, with a character of several bytes where its end would be.
        assert_eq!(
            unescape("a &\u{fffd}\u{fffd}\u{fffd}\u{fffd};".as_bytes()),
            "a &\u{fffd}\u{fffd}\u{fffd}\u{fffd};"
        );
    }
}

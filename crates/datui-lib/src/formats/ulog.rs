//! PX4 ULog flight logs (`.ulg`).
//!
//! A ULog file describes its own messages: format definitions (`F`) give each topic's
//! fields, subscriptions (`A`) give a topic and instance a message id, and data
//! messages (`D`) carry that id and the fields packed end to end. One pass over the
//! file reads the definitions and records where each subscription's data messages
//! start ([`crate::formats::indexed`]); each topic and instance is then a table decoded from a
//! map of the file. Parameters, logged text and info messages are read in the same
//! pass: the first two as tables of their own, info and parameters on the Info tab.
//!
//! Every length is bounded: a message by its 16-bit size and the file's end, a field
//! list by [`MAX_COLUMNS`], nesting by `MAX_DEPTH`, the text kept by its counts.
//! A corrupt stretch is passed over to the next sync marker and counted.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use polars::prelude::*;

use crate::formats::columns::{Builder, Cell, Kind};
use crate::formats::fixed_records::{Bytes, ColumnLayout, Logical, Physical};
use crate::formats::indexed::{IndexedRecords, Offsets};
use crate::formats::model_files::MetaValue;
use crate::formats::sqlite::Table;
use crate::formats::text_formats::Detail;

/// What datui does with a ULog flight log: see [`crate::formats::readers`].
pub(crate) const READER: crate::formats::readers::Reader = crate::formats::readers::Reader {
    scan: crate::formats::indexed::scan::<Index>,
    signatures: &[crate::formats::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::formats::readers::Kind::Magic,
        trusted: crate::formats::readers::Trusted {
            tables: true,
            ..crate::formats::readers::EVERYWHERE
        },
    }],
    tables: Some(crate::formats::indexed::listed::<Index>),
    ..crate::formats::readers::BASE
};

/// The first seven bytes of every ULog file; the eighth is its version.
pub const MAGIC: &[u8; 7] = b"ULog\x01\x12\x35";

/// The sync marker a writer puts between messages, to find the next one after damage.
pub(crate) const SYNC: [u8; 8] = [0x2F, 0x73, 0x13, 0x20, 0x25, 0x0C, 0xBB, 0x12];

/// Columns one topic may make, nested types and arrays of them flattened.
pub const MAX_COLUMNS: usize = 4096;
/// Nesting of one type inside another.
const MAX_DEPTH: usize = 16;
/// Format definitions, subscriptions, and info and parameter messages kept.
const MAX_DEFINITIONS: usize = 65_536;
/// Logged text messages kept.
const MAX_LOGGED: usize = 1_000_000;

/// The table of the log's text messages, and of its parameters.
pub const LOGGED: &str = "logged_messages";
pub const PARAMETERS: &str = "parameters";

/// Whether `head`, the first bytes of a file, begins a ULog file.
pub fn looks_like(head: &[u8]) -> bool {
    head.starts_with(MAGIC)
}

/// One field of a format definition.
#[derive(Debug, Clone, PartialEq)]
struct FieldDef {
    kind: String,
    /// The array length, for `float[3]`.
    count: Option<usize>,
    name: String,
}

/// A subscription: a topic's instance and where its data messages start.
#[derive(Debug)]
pub struct Topic {
    /// Where each message's fields start: after its header and message id.
    pub offsets: Arc<Offsets>,
    pub columns: Vec<ColumnLayout>,
}

/// What one pass over a ULog file found.
#[derive(Debug, Default)]
pub struct Index {
    pub version: u8,
    /// Each topic and instance with data, by message id.
    pub topics: BTreeMap<u16, Topic>,
    /// The table name of each topic with data, in the listing's order.
    pub names: Vec<(String, u16)>,
    pub info: Vec<(String, String)>,
    /// Name, type, value, and the time it was set past the header (`None` for the
    /// initial value).
    pub parameters: Vec<(String, &'static str, f64, Option<u64>)>,
    /// Timestamp, level, tag, text.
    pub logged: Vec<(u64, &'static str, Option<u16>, String)>,
    pub logged_left_out: usize,
    pub dropouts: usize,
    pub dropout_ms: u64,
    /// Bytes passed over as damaged, and how many stretches.
    pub skipped: usize,
    pub damaged: usize,
    /// Data messages too short for their topic's fields, left out.
    pub short: usize,
    /// Data messages with an id no subscription gave.
    pub unsubscribed: usize,
    /// Records past `limits.indexed_records`, left out.
    pub past_limit: usize,
    /// Why a topic's fields could not be read, by topic.
    pub unread: Vec<(String, String)>,
    /// A data section that runs past what the file holds: written while logging.
    pub cut_short: bool,
}

/// A primitive type's bytes and how it is stored.
fn primitive(kind: &str) -> Option<(Physical, usize)> {
    Some(match kind {
        "int8_t" => (Physical::Signed(1), 1),
        "uint8_t" => (Physical::Unsigned(1), 1),
        "int16_t" => (Physical::Signed(2), 2),
        "uint16_t" => (Physical::Unsigned(2), 2),
        "int32_t" => (Physical::Signed(4), 4),
        "uint32_t" => (Physical::Unsigned(4), 4),
        "int64_t" => (Physical::Signed(8), 8),
        "uint64_t" => (Physical::Unsigned(8), 8),
        "float" => (Physical::Float(4), 4),
        "double" => (Physical::Float(8), 8),
        "bool" => (Physical::Bool, 1),
        "char" => (Physical::Text, 1),
        _ => return None,
    })
}

/// Parse `name:type field;type[n] field;...`.
fn parse_format(text: &str) -> Option<(String, Vec<FieldDef>)> {
    let (name, rest) = text.split_once(':')?;
    let mut fields = Vec::new();
    for part in rest.split(';').filter(|p| !p.trim().is_empty()) {
        let (kind, field) = part.trim().split_once(' ')?;
        let (kind, count) = match kind.split_once('[') {
            Some((kind, n)) => (kind, Some(n.strip_suffix(']')?.parse().ok()?)),
            None => (kind, None),
        };
        fields.push(FieldDef {
            kind: kind.to_string(),
            count,
            name: field.trim().to_string(),
        });
        if fields.len() > MAX_COLUMNS {
            return None;
        }
    }
    Some((name.to_string(), fields))
}

/// The size of format `name`, nested types included.
fn size_of(
    name: &str,
    formats: &HashMap<String, Vec<FieldDef>>,
    depth: usize,
) -> Result<usize, String> {
    if depth > MAX_DEPTH {
        return Err("types nest too deep".into());
    }
    let fields = formats
        .get(name)
        .ok_or_else(|| format!("no format for {name}"))?;
    let mut size = 0usize;
    for f in fields {
        let one = match primitive(&f.kind) {
            Some((_, n)) => n,
            None => size_of(&f.kind, formats, depth + 1)?,
        };
        size = one
            .checked_mul(f.count.unwrap_or(1))
            .and_then(|n| size.checked_add(n))
            .ok_or("a type is too large")?;
    }
    Ok(size)
}

/// The columns of format `name` at `offset`, names under `prefix`; returns the end of
/// the last field that is not padding.
fn columns_of(
    name: &str,
    formats: &HashMap<String, Vec<FieldDef>>,
    prefix: &str,
    offset: usize,
    out: &mut Vec<ColumnLayout>,
    depth: usize,
) -> Result<usize, String> {
    if depth > MAX_DEPTH {
        return Err("types nest too deep".into());
    }
    let fields = formats
        .get(name)
        .ok_or_else(|| format!("no format for {name}"))?;
    let mut at = offset;
    let mut needs = offset;
    for f in fields {
        let count = f.count.unwrap_or(1);
        let full = format!("{prefix}{}", f.name);
        match primitive(&f.kind) {
            Some((physical, width)) => {
                let bytes = width.checked_mul(count).ok_or("a field is too large")?;
                // Padding, and an array of no elements, take no column.
                if !f.name.starts_with("_padding") && bytes > 0 {
                    let mut column = match physical {
                        // A char array is text.
                        Physical::Text => ColumnLayout::new(&full, at, 0, physical, bytes),
                        _ => {
                            let mut c = ColumnLayout::new(&full, at, 0, physical, width);
                            c.count = count;
                            c
                        }
                    };
                    // Microseconds since boot, as a duration.
                    if depth == 0 && f.name == "timestamp" && f.kind == "uint64_t" && count == 1 {
                        column.logical = Logical::Duration {
                            unit: TimeUnit::Microseconds,
                            multiplier: 1,
                        };
                    }
                    out.push(column);
                    needs = at + bytes;
                }
                at = at.checked_add(bytes).ok_or("a type is too large")?;
            }
            None => {
                let size = size_of(&f.kind, formats, depth + 1)?;
                for i in 0..count {
                    let inner = match f.count {
                        Some(_) => format!("{full}[{i}]."),
                        None => format!("{full}."),
                    };
                    let end = columns_of(&f.kind, formats, &inner, at, out, depth + 1)?;
                    if end > at {
                        needs = needs.max(end);
                    }
                    at = at.checked_add(size).ok_or("a type is too large")?;
                    if out.len() > MAX_COLUMNS {
                        return Err(format!("its fields make more than {MAX_COLUMNS} columns"));
                    }
                }
            }
        }
        if out.len() > MAX_COLUMNS {
            return Err(format!("its fields make more than {MAX_COLUMNS} columns"));
        }
    }
    Ok(needs)
}

fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// An info or parameter message's key (`type name`) and value, as text and as a number.
fn key_value(payload: &[u8]) -> Option<(String, String, String, Option<f64>)> {
    let len = *payload.first()? as usize;
    let key = std::str::from_utf8(payload.get(1..1 + len)?).ok()?;
    let value = payload.get(1 + len..)?;
    let (kind, name) = key.split_once(' ')?;
    let (base, count) = match kind.split_once('[') {
        Some((base, n)) => (base, n.trim_end_matches(']').parse::<usize>().ok()),
        None => (kind, None),
    };
    let number = |v: Option<f64>| v.map(|n| (format_number(n), Some(n)));
    let (text, number) = match (base, count) {
        ("char", _) => (
            String::from_utf8_lossy(value)
                .trim_end_matches('\0')
                .to_string(),
            None,
        ),
        (_, Some(_)) => (crate::formats::fixed_records::hex(value), None),
        _ => {
            let (physical, width) = primitive(base)?;
            let raw = value.get(..width)?;
            let v = match physical {
                Physical::Signed(_) => {
                    crate::formats::fixed_records::read_signed(raw, false) as f64
                }
                Physical::Unsigned(_) | Physical::Bool => {
                    crate::formats::fixed_records::read_unsigned(raw, false) as f64
                }
                Physical::Float(4) => f32::from_le_bytes(raw.try_into().ok()?) as f64,
                Physical::Float(_) => f64::from_le_bytes(raw.try_into().ok()?),
                _ => return None,
            };
            number(Some(v))?
        }
    };
    Some((kind.to_string(), name.to_string(), text, number))
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

fn level(byte: u8) -> &'static str {
    match byte {
        b'0' => "emergency",
        b'1' => "alert",
        b'2' => "critical",
        b'3' => "error",
        b'4' => "warning",
        b'5' => "notice",
        b'6' => "info",
        b'7' => "debug",
        _ => "unknown",
    }
}

/// Index the ULog file in `data`: one pass, start to end.
pub fn index(data: &[u8]) -> Result<Index, String> {
    if !looks_like(data) {
        return Err("not a ULog file: no ULog magic at the start".into());
    }
    if data.len() < 16 {
        return Err("the header is cut short".into());
    }
    let mut index = Index {
        version: data[7],
        ..Index::default()
    };
    let mut formats: HashMap<String, Vec<FieldDef>> = HashMap::new();
    // Subscriptions by message id: topic, instance, offsets.
    let mut subs: HashMap<u16, (String, u8, Offsets)> = HashMap::new();
    let mut multi: Vec<(String, String)> = Vec::new();
    let mut records = 0usize;
    let limit = crate::limits::get().indexed_records;
    let mut last_time: Option<u64> = None;
    // Appended data, at offsets the flag bits give: the main data ends at the first.
    let mut appended: Vec<usize> = Vec::new();
    let mut end = data.len();
    let mut at = 16usize;
    loop {
        if at >= end {
            match appended.first().copied() {
                Some(next) if next >= at.min(end) && next < data.len() => {
                    appended.remove(0);
                    at = next;
                    end = appended.first().copied().unwrap_or(data.len()).max(next);
                    continue;
                }
                _ => break,
            }
        }
        let Some(size) = u16_at(data, at) else {
            index.cut_short = at < end;
            break;
        };
        let size = size as usize;
        let Some(&kind) = data.get(at + 2) else {
            index.cut_short = true;
            break;
        };
        let body = at + 3;
        if body + size > end {
            // Logging stopped mid-message, or damage: look for the next sync marker.
            if let Some(next) = find_sync(data, at + 1, end) {
                index.skipped += next - at;
                index.damaged += 1;
                at = next;
                continue;
            }
            index.cut_short = true;
            break;
        }
        let payload = &data[body..body + size];
        match kind {
            b'B' if size >= 40 => {
                // Incompatible flag bit 0: appended data at up to three offsets.
                if payload[8] & 1 != 0 {
                    for i in 0..3 {
                        if let Some(o) = u64_at(payload, 16 + i * 8)
                            && o > 0
                            && let Ok(o) = usize::try_from(o)
                            && o > body + size
                            && o < data.len()
                        {
                            appended.push(o);
                        }
                    }
                    appended.sort_unstable();
                    if let Some(&first) = appended.first() {
                        end = first;
                    }
                }
            }
            b'F' => {
                if formats.len() < MAX_DEFINITIONS
                    && let Some((name, fields)) = parse_format(&String::from_utf8_lossy(payload))
                {
                    formats.insert(name, fields);
                }
            }
            b'A' if size >= 3 => {
                let multi_id = payload[0];
                let id = u16_at(payload, 1).unwrap_or_default();
                let name = String::from_utf8_lossy(&payload[3..])
                    .trim_end_matches('\0')
                    .to_string();
                if subs.len() < MAX_DEFINITIONS {
                    subs.insert(id, (name, multi_id, Offsets::for_file(data.len())));
                }
            }
            b'D' if size >= 2 => {
                let id = u16_at(payload, 0).unwrap_or_default();
                match subs.get_mut(&id) {
                    Some((_, _, offsets)) if records < limit => {
                        offsets.push(body + 2);
                        records += 1;
                    }
                    Some(_) => index.past_limit += 1,
                    None => index.unsubscribed += 1,
                }
            }
            b'I' => {
                if let Some((_, name, text, _)) = key_value(payload)
                    && index.info.len() < MAX_DEFINITIONS
                {
                    index.info.push((name, text));
                }
            }
            b'M' if size >= 1 => {
                // A value continued over several messages, joined.
                if let Some((_, name, text, _)) = key_value(&payload[1..]) {
                    let continued = payload[0] != 0;
                    match multi.iter().position(|(n, _)| *n == name) {
                        Some(i) if continued => {
                            let value = &mut multi[i].1;
                            if value.len() < 1 << 20 {
                                value.push_str(&text);
                            }
                        }
                        _ if multi.len() < MAX_DEFINITIONS => multi.push((name, text)),
                        _ => {}
                    }
                }
            }
            b'P' => {
                if let Some((kind, name, _, Some(value))) = key_value(payload)
                    && index.parameters.len() < MAX_DEFINITIONS
                {
                    let kind = if kind == "float" { "float" } else { "int32" };
                    index.parameters.push((name, kind, value, last_time));
                }
            }
            b'L' if size >= 9 => {
                let time = u64_at(payload, 1).unwrap_or_default();
                last_time = Some(time);
                push_logged(&mut index, time, payload[0], None, &payload[9..]);
            }
            b'C' if size >= 11 => {
                let tag = u16_at(payload, 1);
                let time = u64_at(payload, 3).unwrap_or_default();
                last_time = Some(time);
                push_logged(&mut index, time, payload[0], tag, &payload[11..]);
            }
            b'O' if size >= 2 => {
                index.dropouts += 1;
                index.dropout_ms += u64::from(u16_at(payload, 0).unwrap_or_default());
            }
            b'R' | b'S' | b'Q' | b'B' | b'A' | b'D' | b'L' | b'C' | b'O' | b'M' => {}
            _ => {
                // Not a message type: damage. Resume at the next sync marker.
                match find_sync(data, at + 1, end) {
                    Some(next) => {
                        index.skipped += next - at;
                        index.damaged += 1;
                        at = next;
                        continue;
                    }
                    None => {
                        index.skipped += end - at;
                        index.damaged += 1;
                        at = end;
                        continue;
                    }
                }
            }
        }
        at = body + size;
    }
    for (name, value) in multi {
        index.info.push((name, value));
    }

    // Each subscription with data, its columns from its format.
    let mut counts: HashMap<String, usize> = HashMap::new();
    for (name, _, offsets) in subs.values() {
        if !offsets.is_empty() {
            *counts.entry(name.clone()).or_default() += 1;
        }
    }
    let mut ids: Vec<u16> = subs.keys().copied().collect();
    ids.sort_unstable_by_key(|id| (subs[id].0.clone(), subs[id].1));
    for id in ids {
        let (name, multi_id, mut offsets) = subs.remove(&id).expect("listed above");
        if offsets.is_empty() {
            continue;
        }
        let mut columns = Vec::new();
        let needs = match columns_of(&name, &formats, "", 0, &mut columns, 0).and_then(|needs| {
            // A table has one column of a name; a format that repeats one is refused.
            let mut seen = std::collections::HashSet::new();
            match columns.iter().find(|c| !seen.insert(c.name.clone())) {
                Some(c) => Err(format!("it has two fields named {}", c.name)),
                None => Ok(needs),
            }
        }) {
            Ok(needs) => needs,
            Err(why) => {
                index.unread.push((name, why));
                continue;
            }
        };
        // A message too short for its fields is left out: the index has to promise
        // every row holds every column.
        let before = offsets.len();
        offsets = keep_long_enough(offsets, data, needs);
        index.short += before - offsets.len();
        if offsets.is_empty() {
            continue;
        }
        offsets.shrink();
        let table = if counts.get(&name).copied().unwrap_or(0) > 1 {
            format!("{name}.{multi_id}")
        } else {
            name
        };
        index.names.push((table, id));
        index.topics.insert(
            id,
            Topic {
                offsets: Arc::new(offsets),
                columns,
            },
        );
    }
    Ok(index)
}

/// The offsets whose message holds `needs` bytes of fields; a message's size is the
/// two bytes before its header's type, five before its fields.
fn keep_long_enough(offsets: Offsets, data: &[u8], needs: usize) -> Offsets {
    let long_enough = |at: usize| {
        u16_at(data, at - 5).is_some_and(|size| (size as usize).saturating_sub(2) >= needs)
    };
    let mut kept = Offsets::for_file(data.len());
    for i in 0..offsets.len() {
        let at = offsets.get(i);
        if long_enough(at) {
            kept.push(at);
        }
    }
    kept
}

fn push_logged(index: &mut Index, time: u64, level_byte: u8, tag: Option<u16>, text: &[u8]) {
    if index.logged.len() >= MAX_LOGGED {
        index.logged_left_out += 1;
        return;
    }
    let text = String::from_utf8_lossy(text)
        .trim_end_matches('\0')
        .to_string();
    index.logged.push((time, level(level_byte), tag, text));
}

fn find_sync(data: &[u8], from: usize, end: usize) -> Option<usize> {
    let hay = data.get(from..end)?;
    memchr::memmem::find(hay, &SYNC).map(|i| from + i + SYNC.len())
}

impl crate::formats::indexed::Log for Index {
    const EMPTY: &'static str = " The log has no data messages.";

    fn index(data: &[u8]) -> Result<Self, String> {
        index(data)
    }

    fn tables(&self) -> Vec<Table> {
        let mut tables: Vec<Table> = self
            .names
            .iter()
            .map(|(name, id)| {
                let columns = self.topics[id].columns.iter().map(|c| c.name.as_str());
                Table::plain(name, "topic", columns)
            })
            .collect();
        let taken = |name: &str| self.names.iter().any(|(n, _)| n == name);
        if !self.logged.is_empty() && !taken(LOGGED) {
            let columns = ["timestamp", "level", "tag", "message"];
            tables.push(Table::plain(LOGGED, "messages", columns));
        }
        if !self.parameters.is_empty() && !taken(PARAMETERS) {
            let columns = ["name", "type", "value", "timestamp"];
            tables.push(Table::plain(PARAMETERS, "parameters", columns));
        }
        tables
    }

    fn detail(&self) -> Detail {
        let count = crate::formats::text_formats::count;
        let mut lines = vec![
            format!("Version: {}", self.version),
            format!(
                "Topics: {}",
                count(self.names.len() as u64, "table", "tables")
            ),
        ];
        if self.dropouts > 0 {
            lines.push(format!(
                "Dropouts: {}, {} ms in all",
                self.dropouts, self.dropout_ms
            ));
        }
        let mut list: Vec<(String, MetaValue)> = self
            .info
            .iter()
            .map(|(k, v)| (k.clone(), MetaValue::Text(v.clone())))
            .collect();
        let mut seen = std::collections::HashSet::new();
        for (name, _, value, at) in &self.parameters {
            // The value the log started with; later changes are in the parameters table.
            if at.is_none() && seen.insert(name.clone()) {
                list.push((
                    format!("param {name}"),
                    MetaValue::Text(format_number(*value)),
                ));
            }
        }
        let total = list.len();
        Detail {
            tab: crate::formats::text_formats::tab(crate::FileFormat::Ulog),
            lines,
            list_title: "Info and parameters",
            list: crate::formats::text_formats::capped_list(list.into_iter(), total),
            first: false,
            ..Default::default()
        }
    }

    fn notes(&self) -> Vec<String> {
        let group = |n: usize| crate::numfmt::group_chrome(n);
        let mut notes = Vec::new();
        if self.damaged > 0 {
            notes.push(format!(
                "{} damaged stretches skipped ({} bytes)",
                group(self.damaged),
                group(self.skipped)
            ));
        }
        if self.cut_short {
            notes.push("log cut short mid-message".to_string());
        }
        if self.short > 0 {
            notes.push(format!(
                "{} data messages left out: too short for their topic",
                group(self.short)
            ));
        }
        if self.unsubscribed > 0 {
            notes.push(format!(
                "{} data messages left out: no subscription",
                group(self.unsubscribed)
            ));
        }
        if self.past_limit > 0 {
            notes.push(crate::limits::left_out(
                &format!("{} messages", group(self.past_limit)),
                crate::limits::get().indexed_records,
                "indexed_records",
            ));
        }
        if self.logged_left_out > 0 {
            notes.push(format!(
                "{} logged messages left out: past the first {}",
                group(self.logged_left_out),
                group(MAX_LOGGED)
            ));
        }
        for (topic, why) in &self.unread {
            notes.push(format!("topic {topic} not read: {why}"));
        }
        notes
    }

    fn table(
        &self,
        bytes: Arc<Bytes>,
        name: &str,
        opened: &mut crate::formats::members::Opened,
    ) -> Result<LazyFrame, String> {
        if let Some((_, id)) = self.names.iter().find(|(n, _)| *n == name) {
            let topic = &self.topics[id];
            let records = Arc::new(
                IndexedRecords::new(bytes, topic.offsets.clone(), topic.columns.clone())
                    .map_err(|e| format!("table \"{name}\": {e}"))?,
            );
            opened.window = Some((records.clone(), records.rows()));
            return Ok(records.lazy());
        }
        let mut table = if name == LOGGED {
            let mut table = Builder::new(&[
                ("timestamp", Kind::DurationUs),
                ("level", Kind::Label),
                ("tag", Kind::U16),
                ("message", Kind::Str),
            ]);
            for (t, level, tag, text) in &self.logged {
                table.push([
                    Cell::DurationUs(Some(*t as i64)),
                    Cell::Label(Some(level)),
                    Cell::U16(*tag),
                    Cell::Str(Some(text.clone())),
                ]);
            }
            table
        } else {
            let mut table = Builder::new(&[
                ("name", Kind::Str),
                ("type", Kind::Label),
                ("value", Kind::F64),
                ("timestamp", Kind::DurationUs),
            ]);
            for (name, kind, value, t) in &self.parameters {
                table.push([
                    Cell::Str(Some(name.clone())),
                    Cell::Label(Some(kind)),
                    Cell::F64(Some(*value)),
                    Cell::DurationUs(t.map(|t| t as i64)),
                ]);
            }
            table
        };
        Ok(table.take().map_err(|e| e.to_string())?.lazy())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file that is not a ULog names itself, in the one shape.
    #[test]
    fn errors_name_the_file() {
        crate::formats::readers::bad_input::each_names_its_file(
            crate::FileFormat::Ulog,
            &[
                ("text.ulg", b"hello there, this is text", "Not a ULog file"),
                ("cut.ulg", b"ULog\x01\x12\x35\x01", "cut short"),
            ],
        );
    }

    #[test]
    fn topics_instances_and_text() {
        let index = index(&crate::tests::fixtures::ulog()).unwrap();
        let names: Vec<&str> = index.names.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["sensor.0", "sensor.1", "status"]);
        let sensor = &index.topics[&1];
        assert_eq!(sensor.offsets.len(), 2);
        let columns: Vec<&str> = sensor.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            columns,
            ["timestamp", "id", "v.x", "v.y", "v.z", "tag", "raw"]
        );
        assert_eq!(index.info, [("sys_name".to_string(), "PX4".to_string())]);
        assert_eq!(index.parameters[0].0, "MPC_XY_VEL");
        assert_eq!(index.logged[0].3, "low battery");
        assert_eq!(index.damaged, 1);
        assert!(index.cut_short);
    }

    #[test]
    fn a_topic_decodes() {
        let data = crate::tests::fixtures::ulog();
        let index = index(&data).unwrap();
        let topic = &index.topics[&1];
        let records = Arc::new(
            IndexedRecords::new(
                Arc::new(Bytes::Owned(data.clone())),
                topic.offsets.clone(),
                topic.columns.clone(),
            )
            .unwrap(),
        );
        let df = records.lazy().collect().unwrap();
        assert_eq!(
            df.column("v.y").unwrap().f32().unwrap().to_vec(),
            [Some(2.0), Some(4.0)]
        );
        assert_eq!(
            df.column("timestamp").unwrap().dtype(),
            &DataType::Duration(TimeUnit::Microseconds)
        );
        assert_eq!(df.column("tag").unwrap().str().unwrap().get(0), Some("ab"));
        assert_eq!(
            df.column("raw").unwrap().dtype(),
            &DataType::Array(Box::new(DataType::Int16), 2)
        );
    }

    #[test]
    fn garbage_is_bounded() {
        assert!(index(b"nope").is_err());
        let mut data = MAGIC.to_vec();
        data.extend([1, 0, 0, 0, 0, 0, 0, 0, 0]);
        data.extend([0xFF; 64]);
        let index = index(&data).unwrap();
        assert!(index.names.is_empty());
    }
}

//! ArduPilot DataFlash logs (`.bin`).
//!
//! A DataFlash log describes its own messages: each `FMT` record gives a message
//! type's id, length, name, format characters and field labels; every other record is
//! `A3 95`, its type's id, and its fields packed end to end. One pass records where
//! each type's records start ([`crate::indexed`]) and reads the units and multipliers
//! of `FMTU`, `UNIT` and `MULT` when the log has them; each type is then a table
//! decoded from a map of the file, its columns typed by their format characters.
//!
//! Every length is bounded: a record by its type's length and the file's end. A byte
//! that does not start a record is passed over to the next `A3 95` and counted.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use polars::prelude::*;

use crate::fixed_records::{Bytes, ColumnLayout, Logical, Physical};
use crate::indexed::{IndexedRecords, Offsets};
use crate::model_files::MetaValue;
use crate::sqlite::Table;
use crate::text_formats::Detail;

/// What datui does with a DataFlash log: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan: crate::indexed::scan::<Index>,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Magic,
        trusted: crate::readers::Trusted {
            tables: true,
            ..crate::readers::EVERYWHERE
        },
    }],
    tables: Some(crate::indexed::listed::<Index>),
    ..crate::readers::BASE
};

const HEAD: [u8; 2] = [0xA3, 0x95];
/// The type id of `FMT`, the record that defines the others.
pub(crate) const FMT: u8 = 0x80;
/// An `FMT` record's length: header, type, length, name, format, labels.
pub(crate) const FMT_LEN: usize = 89;

/// Whether `head`, the first bytes of a file, begins a DataFlash log: an `FMT` record
/// that defines `FMT` itself.
pub fn looks_like(head: &[u8]) -> bool {
    head.len() >= 8
        && head[..3] == [0xA3, 0x95, FMT]
        && head[3] == FMT
        && head[4] as usize == FMT_LEN
        && &head[5..8] == b"FMT"
}

/// A format character's storage, width, values, and the scale its value carries.
fn format_char(c: u8) -> Option<(Physical, usize, usize, Option<f64>)> {
    Some(match c {
        b'b' => (Physical::Signed(1), 1, 1, None),
        b'B' | b'M' => (Physical::Unsigned(1), 1, 1, None),
        b'h' => (Physical::Signed(2), 2, 1, None),
        b'H' => (Physical::Unsigned(2), 2, 1, None),
        b'i' => (Physical::Signed(4), 4, 1, None),
        b'I' => (Physical::Unsigned(4), 4, 1, None),
        b'q' => (Physical::Signed(8), 8, 1, None),
        b'Q' => (Physical::Unsigned(8), 8, 1, None),
        b'f' => (Physical::Float(4), 4, 1, None),
        b'd' => (Physical::Float(8), 8, 1, None),
        b'g' => (Physical::Float(2), 2, 1, None),
        b'n' => (Physical::Text, 4, 1, None),
        b'N' => (Physical::Text, 16, 1, None),
        b'Z' => (Physical::Text, 64, 1, None),
        b'c' => (Physical::Signed(2), 2, 1, Some(0.01)),
        b'C' => (Physical::Unsigned(2), 2, 1, Some(0.01)),
        b'e' => (Physical::Signed(4), 4, 1, Some(0.01)),
        b'E' => (Physical::Unsigned(4), 4, 1, Some(0.01)),
        // Latitude and longitude in 1e-7 degrees.
        b'L' => (Physical::Signed(4), 4, 1, Some(1e-7)),
        b'a' => (Physical::Signed(2), 2, 32, None),
        _ => return None,
    })
}

/// One message type, as its `FMT` record defines it.
#[derive(Debug)]
pub struct MessageType {
    pub id: u8,
    pub name: String,
    /// The whole record's length, header included.
    pub length: usize,
    pub format: String,
    pub labels: Vec<String>,
    pub offsets: Arc<Offsets>,
    /// Unit and multiplier ids per field, from `FMTU`.
    pub unit_ids: Option<String>,
    pub mult_ids: Option<String>,
}

/// What one pass over a DataFlash log found.
#[derive(Debug, Default)]
pub struct Index {
    pub types: BTreeMap<u8, MessageType>,
    pub units: HashMap<u8, String>,
    pub mults: HashMap<u8, f64>,
    pub skipped: usize,
    pub damaged: usize,
    pub past_limit: usize,
    /// `FMT` records whose format does not add up to their length, or has a character
    /// datui does not know.
    pub bad_formats: Vec<String>,
    pub cut_short: bool,
}

impl Index {
    /// The tables with records, by name.
    pub fn names(&self) -> Vec<(String, u8)> {
        let mut names: Vec<(String, u8)> = self
            .types
            .values()
            .filter(|t| !t.offsets.is_empty() && t.id != FMT)
            .map(|t| (t.name.clone(), t.id))
            .collect();
        names.sort();
        // Two types of one name (a log that redefines one) keep their ids apart.
        let mut seen: HashMap<String, usize> = HashMap::new();
        for (name, _) in &names {
            *seen.entry(name.clone()).or_default() += 1;
        }
        for (name, id) in &mut names {
            if seen[name.as_str()] > 1 {
                *name = format!("{name}.{id}");
            }
        }
        names
    }
}

/// Text from a fixed-width field.
fn text(bytes: &[u8]) -> String {
    crate::fixed_records::text(bytes)
}

/// The size a format's characters add up to, `None` for one datui does not know.
pub(crate) fn format_size(format: &str) -> Option<usize> {
    format.bytes().try_fold(0usize, |n, c| {
        let (_, width, count, _) = format_char(c)?;
        n.checked_add(width * count)
    })
}

/// The fields of a record of `t` read as plain values, for `FMTU`, `UNIT` and `MULT`.
fn fields<'a>(t: &MessageType, record: &'a [u8]) -> Vec<(&'a [u8], u8)> {
    let mut at = 3;
    let mut out = Vec::new();
    for c in t.format.bytes() {
        let Some((_, width, count, _)) = format_char(c) else {
            break;
        };
        let len = width * count;
        let Some(bytes) = record.get(at..at + len) else {
            break;
        };
        out.push((bytes, c));
        at += len;
    }
    out
}

/// Index the DataFlash log in `data`: one pass, start to end.
pub fn index(data: &[u8]) -> Result<Index, String> {
    if !looks_like(data) {
        return Err("not a DataFlash log: it does not start with an FMT record".into());
    }
    let mut index = Index::default();
    let mut records = 0usize;
    let limit = crate::limits::get().indexed_records;
    let mut fmtu: Vec<(u8, String, String)> = Vec::new();
    let mut building: HashMap<u8, Offsets> = HashMap::new();
    let mut at = 0usize;
    while at < data.len() {
        if data.get(at..at + 2) != Some(&HEAD[..]) {
            // Not a record: the next header, if there is one.
            match memchr::memmem::find(&data[at + 1..], &HEAD) {
                Some(i) => {
                    index.skipped += i + 1;
                    index.damaged += 1;
                    at += i + 1;
                    continue;
                }
                None => {
                    index.skipped += data.len() - at;
                    index.damaged += 1;
                    break;
                }
            }
        }
        let Some(&id) = data.get(at + 2) else {
            index.cut_short = true;
            break;
        };
        if id == FMT {
            let Some(record) = data.get(at..at + FMT_LEN) else {
                index.cut_short = true;
                break;
            };
            let defined = record[3];
            let length = record[4] as usize;
            let name = text(&record[5..9]);
            let format = text(&record[9..25]);
            let labels: Vec<String> = text(&record[25..89])
                .split(',')
                .map(str::to_string)
                .collect();
            match format_size(&format) {
                Some(size) if size + 3 == length && length >= 3 => {
                    // A type redefined alike keeps the records it had; one redefined
                    // otherwise starts again, as its records are read differently.
                    let same = index
                        .types
                        .get(&defined)
                        .is_some_and(|t| t.length == length && t.format == format);
                    if !same {
                        building.insert(defined, Offsets::for_file(data.len()));
                    }
                    index.types.insert(
                        defined,
                        MessageType {
                            id: defined,
                            name,
                            length,
                            format,
                            labels,
                            offsets: Arc::new(Offsets::Narrow(Vec::new())),
                            unit_ids: None,
                            mult_ids: None,
                        },
                    );
                }
                _ => {
                    if index.bad_formats.len() < 1000 {
                        index.bad_formats.push(format!("{name} ({format})"));
                    }
                }
            }
            at += FMT_LEN;
            continue;
        }
        let Some(t) = index.types.get_mut(&id) else {
            // A type no FMT defined: its length is unknown, so look for the next header.
            match memchr::memmem::find(&data[at + 2..], &HEAD) {
                Some(i) => {
                    index.skipped += i + 2;
                    index.damaged += 1;
                    at += i + 2;
                    continue;
                }
                None => {
                    index.skipped += data.len() - at;
                    index.damaged += 1;
                    break;
                }
            }
        };
        let Some(record) = data.get(at..at + t.length) else {
            index.cut_short = true;
            break;
        };
        match t.name.as_str() {
            "FMTU" => {
                let f = fields(t, record);
                if let [_, (ty, _), (units, _), (mults, _), ..] = f.as_slice()
                    && let Some(&ty) = ty.first()
                    && fmtu.len() < 1000
                {
                    fmtu.push((ty, text(units), text(mults)));
                }
            }
            "UNIT" => {
                let f = fields(t, record);
                if let [_, (unit_id, _), (label, _), ..] = f.as_slice()
                    && let Some(&unit_id) = unit_id.first()
                {
                    index.units.insert(unit_id, text(label));
                }
            }
            "MULT" => {
                let f = fields(t, record);
                if let [_, (mult_id, _), (mult, b'd'), ..] = f.as_slice()
                    && let Some(&mult_id) = mult_id.first()
                    && let Ok(raw) = <[u8; 8]>::try_from(*mult)
                {
                    index.mults.insert(mult_id, f64::from_le_bytes(raw));
                }
            }
            _ => {}
        }
        if records < limit {
            building
                .entry(id)
                .or_insert_with(|| Offsets::for_file(data.len()))
                .push(at);
            records += 1;
        } else {
            index.past_limit += 1;
        }
        at += t.length;
    }
    for (ty, units, mults) in fmtu {
        if let Some(t) = index.types.get_mut(&ty) {
            t.unit_ids = Some(units);
            t.mult_ids = Some(mults);
        }
    }
    for (id, mut offsets) in building {
        if let Some(t) = index.types.get_mut(&id) {
            offsets.shrink();
            t.offsets = Arc::new(offsets);
        }
    }
    Ok(index)
}

/// The columns of a message type, and each one's unit.
pub fn columns(index: &Index, t: &MessageType) -> (Vec<ColumnLayout>, Vec<(String, String)>) {
    let mut columns = Vec::new();
    let mut units = Vec::new();
    let mut at = 3;
    for (i, c) in t.format.bytes().enumerate() {
        let Some((physical, width, count, scale)) = format_char(c) else {
            break;
        };
        let mut name = t
            .labels
            .get(i)
            .filter(|l| !l.is_empty())
            .cloned()
            .unwrap_or_else(|| format!("field{i}"));
        // A label a log repeats keeps both columns, the later by its place.
        if columns
            .iter()
            .any(|c: &ColumnLayout| c.name == name.as_str())
        {
            name = format!("{name}_{i}");
        }
        let mut column = match physical {
            Physical::Text => ColumnLayout::new(&name, at, 0, physical, width),
            _ => {
                let mut c = ColumnLayout::new(&name, at, 0, physical, width);
                c.count = count;
                c
            }
        };
        let mult = t
            .mult_ids
            .as_ref()
            .and_then(|m| m.as_bytes().get(i))
            .and_then(|id| index.mults.get(id))
            .copied();
        if (name == "TimeUS" && c == b'Q') || (name == "TimeMS" && c == b'I') {
            column.logical = Logical::Duration {
                unit: if c == b'Q' {
                    TimeUnit::Microseconds
                } else {
                    TimeUnit::Milliseconds
                },
                multiplier: 1,
            };
        } else if let Some(scale) = scale {
            column.logical = Logical::Linear {
                factor: scale,
                offset: 0.0,
            };
        } else if physical.is_integer()
            && count == 1
            && let Some(mult) = mult.filter(|m| *m != 1.0 && *m != 0.0 && m.is_finite())
        {
            // An integer the log scales: FMTU says by what.
            column.logical = Logical::Linear {
                factor: mult,
                offset: 0.0,
            };
        }
        if let Some(unit) = t
            .unit_ids
            .as_ref()
            .and_then(|u| u.as_bytes().get(i))
            .and_then(|id| index.units.get(id))
            .filter(|u| !u.is_empty())
        {
            units.push((name.clone(), unit.clone()));
        }
        columns.push(column);
        at += width * count;
    }
    (columns, units)
}

impl crate::indexed::Log for Index {
    const EMPTY: &'static str = " The log has no records but its formats.";

    fn index(data: &[u8]) -> Result<Self, String> {
        index(data)
    }

    fn tables(&self) -> Vec<Table> {
        self.names()
            .into_iter()
            .map(|(name, id)| {
                let columns = columns(self, &self.types[&id]).0;
                Table::plain(name, "message", columns.iter().map(|c| c.name.as_str()))
            })
            .collect()
    }

    fn detail(&self) -> Detail {
        let names = self.names();
        let records: usize = names
            .iter()
            .map(|(_, id)| self.types[id].offsets.len())
            .sum();
        let lines = vec![
            format!(
                "Message types: {} with records, {} defined",
                names.len(),
                self.types.len()
            ),
            format!("Records: {}", crate::numfmt::group_chrome(records)),
            format!(
                "Units: {}",
                if self.units.is_empty() {
                    "none in the log".to_string()
                } else {
                    format!("{} (FMTU, UNIT, MULT)", self.units.len())
                }
            ),
        ];
        let list = names.iter().map(|(name, id)| {
            let t = &self.types[id];
            (
                name.clone(),
                MetaValue::Text(format!(
                    "{} records, format {}, {} bytes",
                    crate::numfmt::group_chrome(t.offsets.len()),
                    t.format,
                    t.length
                )),
            )
        });
        Detail {
            tab: crate::text_formats::tab(crate::FileFormat::Dataflash),
            lines,
            list_title: "Messages",
            list: crate::text_formats::capped_list(list, names.len()),
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
            notes.push("log cut short mid-record".to_string());
        }
        if self.past_limit > 0 {
            notes.push(crate::limits::left_out(
                &format!("{} records", group(self.past_limit)),
                crate::limits::get().indexed_records,
                "indexed_records",
            ));
        }
        if !self.bad_formats.is_empty() {
            notes.push(format!(
                "{} message types not read, format unreadable: {}",
                self.bad_formats.len(),
                self.bad_formats
                    .iter()
                    .take(10)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        notes
    }

    fn table(
        &self,
        bytes: Arc<Bytes>,
        name: &str,
        opened: &mut crate::members::Opened,
    ) -> Result<LazyFrame, String> {
        let id = self
            .names()
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, id)| id)
            .expect("picked from the names");
        let t = &self.types[&id];
        let (columns, units) = columns(self, t);
        let records = Arc::new(
            IndexedRecords::new(bytes, t.offsets.clone(), columns)
                .map_err(|e| format!("table \"{name}\": {e}"))?,
        );
        opened.window = Some((records.clone(), records.rows()));
        opened.units = units;
        Ok(records.lazy())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file that is not a DataFlash log names itself, in the one shape.
    #[test]
    fn errors_name_the_file() {
        crate::readers::bad_input::each_names_its_file(
            crate::FileFormat::Dataflash,
            &[(
                "text.bin",
                b"hello there, this is text",
                "Not a DataFlash log",
            )],
        );
    }

    #[test]
    fn types_records_units_and_damage() {
        let data = crate::tests::fixtures::dataflash();
        assert!(looks_like(&data));
        let index = index(&data).unwrap();
        let names: Vec<String> = index.names().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["ATT", "BARO", "FMTU", "MULT", "UNIT"]);
        assert_eq!(index.types[&140].offsets.len(), 3);
        assert_eq!(index.damaged, 1);
        assert!(index.cut_short);
        let (columns, units) = columns(&index, &index.types[&141]);
        assert_eq!(
            units,
            [
                ("Alt".to_string(), "m".to_string()),
                ("Press".to_string(), "Pa".to_string()),
                ("Lat".to_string(), "deg".to_string())
            ]
        );
        let records = Arc::new(
            IndexedRecords::new(
                Arc::new(Bytes::Owned(data)),
                index.types[&141].offsets.clone(),
                columns,
            )
            .unwrap(),
        );
        let df = records.lazy().collect().unwrap();
        assert_eq!(
            df.column("TimeUS").unwrap().dtype(),
            &DataType::Duration(TimeUnit::Microseconds)
        );
        // The format scales latitude; FMTU's multiplier scales the pressure.
        let lat = df.column("Lat").unwrap().f64().unwrap().get(0).unwrap();
        assert!((lat - 47.3977418).abs() < 1e-9);
        assert_eq!(
            df.column("Press").unwrap().f64().unwrap().get(0),
            Some(10_132_500.0)
        );
    }

    #[test]
    fn scaled_characters() {
        let data = crate::tests::fixtures::dataflash();
        let index = index(&data).unwrap();
        let (columns, _) = columns(&index, &index.types[&140]);
        let records = Arc::new(
            IndexedRecords::new(
                Arc::new(Bytes::Owned(data)),
                index.types[&140].offsets.clone(),
                columns,
            )
            .unwrap(),
        );
        let df = records.lazy().collect().unwrap();
        assert_eq!(
            df.column("Roll").unwrap().f64().unwrap().to_vec(),
            [Some(1.5), Some(1.51), Some(1.52)]
        );
        assert_eq!(df.column("Yaw").unwrap().u16().unwrap().get(0), Some(9000));
    }

    #[test]
    fn garbage_is_bounded() {
        assert!(index(b"nope").is_err());
        let mut data = crate::tests::fixtures::dataflash_fmt(
            FMT,
            "FMT",
            "BBnNZ",
            "Type,Length,Name,Format,Columns",
        );
        data.extend([0xA3, 0x95, 0x80, 7, 200]);
        data.extend([0xA3; 50]);
        let index = index(&data).unwrap();
        assert!(index.names().is_empty());
    }
}

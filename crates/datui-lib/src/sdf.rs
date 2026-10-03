//! SDF (structure-data format) compound files, read into a table: one row per record,
//! with the molecule's name, its atom and bond counts, and every data item
//! (`> <FIELD>`) as a column.
//!
//! A record is a molfile header (name, program, comment, counts line), the connection
//! table up to `M  END`, then data items, and ends at `$$$$`. The connection table is
//! passed over line by line and never held. The reader takes the file a piece at a
//! time; a line, a value and the number of fields are bounded.

use std::collections::HashMap;
use std::path::Path;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::OpenOptions;
use crate::model_files::MetaValue;
use crate::notes::Note;
use crate::segments::{Converted, Segments};
use crate::text_formats::{Detail, Pieces, capped_list, count, note};
use crate::unfinished::Writer;

/// The longest line kept; the rest of a longer one is cut off.
pub const MAX_LINE: usize = 1 << 20;
/// The longest value kept, its lines together; the rest is cut off.
pub const MAX_VALUE: usize = 1 << 20;
/// The most data fields; one past this is left out.
pub const MAX_FIELDS: usize = 4096;
/// The longest field name.
pub const MAX_NAME: usize = 256;
/// Rows held before a batch is handed over.
pub const BATCH_ROWS: usize = 16_384;
/// Text held before a batch is handed over, whatever its rows.
pub const BATCH_TEXT: usize = 32 << 20;

/// The columns every SDF table starts with.
pub const CORE: [&str; 3] = ["name", "atoms", "bonds"];

/// Whether the first bytes look like an SDF file: a counts line (`V2000` or `V3000`)
/// on the fourth line, or `M  END` followed by a data item or `$$$$`.
pub fn looks_like(head: &[u8]) -> bool {
    let text = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let mut lines = text.split(|&b| b == b'\n');
    let counts = lines.nth(3).unwrap_or_default();
    let counts_line = counts.windows(5).any(|w| w == b"V2000" || w == b"V3000");
    let has = |needle: &[u8]| text.windows(needle.len()).any(|w| w == needle);
    counts_line || (has(b"M  END") && (has(b"$$$$") || has(b"> <")))
}

/// What one data field's values were, for its type.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldColumn {
    pub name: String,
    /// Records holding a value.
    pub values: u64,
    /// Every value is an integer.
    pub integers: bool,
    /// Every value is a number.
    pub numbers: bool,
}

impl FieldColumn {
    /// The type the column is cast to.
    pub fn dtype(&self) -> DataType {
        if self.values == 0 {
            DataType::String
        } else if self.integers {
            DataType::Int64
        } else if self.numbers {
            DataType::Float64
        } else {
            DataType::String
        }
    }
}

/// What reading noticed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    pub records: u64,
    /// Lines cut at [`MAX_LINE`].
    pub long_lines: u64,
    /// Values cut at [`MAX_VALUE`].
    pub long_values: u64,
    /// Data items in a record that already had the field; the first is kept.
    pub repeated: u64,
    /// Data items of fields past [`MAX_FIELDS`], left out.
    pub fields_dropped: u64,
    /// Records in the V3000 format.
    pub v3000: u64,
    /// Whether the last record had no `$$$$`.
    pub unterminated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// The molfile header: line 0 is the name, 3 the counts line.
    Header(u8),
    /// The connection table, up to `M  END`.
    Ctab,
    /// Between data items.
    Data,
    /// A data item's value lines, up to a blank line.
    Value,
}

/// One record being read.
#[derive(Debug, Default)]
struct Record {
    name: Option<String>,
    atoms: Option<u32>,
    bonds: Option<u32>,
    values: HashMap<usize, String>,
    /// The field whose value is being read, if it is kept.
    field: Option<usize>,
    value: String,
    /// The value was cut at [`MAX_VALUE`]; its other lines are passed over.
    cut: bool,
    /// Whether anything but blank lines was read: blank lines alone are no record.
    started: bool,
}

/// Reads an SDF file a piece at a time.
#[derive(Debug)]
pub struct SdfReader {
    buf: Vec<u8>,
    /// The rest of a line longer than [`MAX_LINE`] is being passed over.
    cutting: bool,
    place: Place,
    record: Record,
    fields: Vec<FieldColumn>,
    by_name: HashMap<String, usize>,
    names: Vec<Option<String>>,
    atoms: Vec<Option<u32>>,
    bonds: Vec<Option<u32>>,
    /// Per field, a value per row.
    columns: Vec<Vec<Option<String>>>,
    rows: usize,
    held: usize,
    stats: Stats,
}

impl Default for SdfReader {
    fn default() -> Self {
        Self::new()
    }
}

impl SdfReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            cutting: false,
            place: Place::Header(0),
            record: Record::default(),
            fields: Vec::new(),
            by_name: HashMap::new(),
            names: Vec::new(),
            atoms: Vec::new(),
            bonds: Vec::new(),
            columns: Vec::new(),
            rows: 0,
            held: 0,
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The data fields, in the order they were first seen.
    pub fn fields(&self) -> &[FieldColumn] {
        &self.fields
    }

    /// Read `bytes`, the next piece of the file.
    pub fn push(&mut self, bytes: &[u8]) {
        let mut rest = bytes;
        while let Some(end) = rest.iter().position(|&b| b == b'\n') {
            self.take(&rest[..end]);
            if !self.cutting {
                let line = std::mem::take(&mut self.buf);
                self.line(&line);
            }
            self.cutting = false;
            self.buf.clear();
            rest = &rest[end + 1..];
        }
        self.take(rest);
    }

    /// Hold `part` of the line being read, up to [`MAX_LINE`].
    fn take(&mut self, part: &[u8]) {
        if self.cutting {
            return;
        }
        let room = MAX_LINE.saturating_sub(self.buf.len());
        if part.len() > room {
            self.buf.extend_from_slice(&part[..room]);
            self.stats.long_lines += 1;
            let line = std::mem::take(&mut self.buf);
            self.line(&line);
            self.cutting = true;
        } else {
            self.buf.extend_from_slice(part);
        }
    }

    /// A full batch, once one is held: [`BATCH_ROWS`] rows, or [`BATCH_TEXT`] of text.
    pub fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        if self.rows < BATCH_ROWS && self.held < BATCH_TEXT {
            return Ok(None);
        }
        self.batch().map(Some)
    }

    /// The end of the file: the last line and record, and the rows not yet taken.
    pub fn finish(&mut self) -> PolarsResult<DataFrame> {
        if !self.cutting && !self.buf.is_empty() {
            let line = std::mem::take(&mut self.buf);
            self.line(&line);
        }
        self.buf.clear();
        self.cutting = false;
        if self.record.started {
            self.stats.unterminated = true;
            self.end_record();
        }
        self.batch()
    }

    fn batch(&mut self) -> PolarsResult<DataFrame> {
        self.held = 0;
        let height = std::mem::take(&mut self.rows);
        let mut columns = vec![
            StringChunked::from_iter_options(
                "name".into(),
                std::mem::take(&mut self.names).into_iter(),
            )
            .into_column(),
            UInt32Chunked::from_iter_options(
                "atoms".into(),
                std::mem::take(&mut self.atoms).into_iter(),
            )
            .into_column(),
            UInt32Chunked::from_iter_options(
                "bonds".into(),
                std::mem::take(&mut self.bonds).into_iter(),
            )
            .into_column(),
        ];
        for (field, values) in self.fields.iter().zip(self.columns.iter_mut()) {
            columns.push(
                StringChunked::from_iter_options(
                    field.name.as_str().into(),
                    std::mem::take(values).into_iter(),
                )
                .into_column(),
            );
        }
        DataFrame::new(height, columns)
    }

    fn line(&mut self, raw: &[u8]) {
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        let line = String::from_utf8_lossy(raw);
        let line = line.as_ref();
        if line.starts_with("$$$$") {
            self.end_value();
            if self.record.started {
                self.end_record();
            } else {
                // Blank lines and a `$$$$` hold no record.
                self.record = Record::default();
                self.place = Place::Header(0);
            }
            return;
        }
        if !line.trim().is_empty() {
            self.record.started = true;
        }
        match self.place {
            Place::Header(n) => {
                // The name line may be blank; lines after it say what made the file.
                if n == 0 {
                    self.record.name = Some(line.trim().to_string()).filter(|s| !s.is_empty());
                } else if n == 3 {
                    self.counts(line);
                }
                self.place = if n >= 3 {
                    Place::Ctab
                } else {
                    Place::Header(n + 1)
                };
            }
            Place::Ctab => {
                if line.starts_with("M  END") {
                    self.place = Place::Data;
                } else if let Some(counts) = line.strip_prefix("M  V30 COUNTS") {
                    let mut numbers = counts.split_whitespace();
                    self.record.atoms = numbers.next().and_then(|n| n.parse().ok());
                    self.record.bonds = numbers.next().and_then(|n| n.parse().ok());
                } else if is_item_header(line) {
                    // A connection table cut short of `M  END`.
                    self.place = Place::Data;
                    self.item(line);
                }
            }
            Place::Data => {
                if is_item_header(line) {
                    self.item(line);
                }
            }
            Place::Value => {
                if line.trim().is_empty() {
                    self.end_value();
                    self.place = Place::Data;
                } else if is_item_header(line) && line.contains('<') {
                    // A writer that leaves out the blank line.
                    self.end_value();
                    self.item(line);
                } else if self.record.field.is_some() && !self.record.cut {
                    let value = &mut self.record.value;
                    let sep = usize::from(!value.is_empty());
                    let room = MAX_VALUE.saturating_sub(value.len() + sep);
                    let mut take = line.len().min(room);
                    while !line.is_char_boundary(take) {
                        take -= 1;
                    }
                    if take > 0 {
                        if sep == 1 {
                            value.push('\n');
                        }
                        value.push_str(&line[..take]);
                    }
                    if take < line.len() {
                        self.stats.long_values += 1;
                        self.record.cut = true;
                    }
                }
            }
        }
    }

    /// The counts line: atoms in columns 1-3, bonds in 4-6; V3000 gives them later.
    fn counts(&mut self, line: &str) {
        if line.contains("V3000") {
            self.stats.v3000 += 1;
            return;
        }
        let number = |range: std::ops::Range<usize>| {
            line.get(range).and_then(|s| s.trim().parse::<u32>().ok())
        };
        self.record.atoms = number(0..3);
        self.record.bonds = number(3..6);
    }

    /// A data item's header line: `> <FIELD>`, `>  <FIELD> (ID)`, `> 25 <FIELD>`.
    fn item(&mut self, line: &str) {
        self.place = Place::Value;
        self.record.value.clear();
        self.record.field = None;
        self.record.cut = false;
        let Some(name) = item_name(line) else {
            return;
        };
        let index = match self.by_name.get(&name) {
            Some(&i) => i,
            None if self.fields.len() >= MAX_FIELDS => {
                self.stats.fields_dropped += 1;
                return;
            }
            None => {
                let i = self.fields.len();
                self.by_name.insert(name.clone(), i);
                self.fields.push(FieldColumn {
                    name,
                    values: 0,
                    integers: true,
                    numbers: true,
                });
                self.columns.push(vec![None; self.rows]);
                i
            }
        };
        if self.record.values.contains_key(&index) {
            self.stats.repeated += 1;
            return;
        }
        self.record.field = Some(index);
    }

    fn end_value(&mut self) {
        if self.place != Place::Value {
            return;
        }
        let value = std::mem::take(&mut self.record.value);
        if let Some(field) = self.record.field.take() {
            self.keep(field, value);
        }
    }

    fn keep(&mut self, field: usize, value: String) {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return;
        }
        let column = &mut self.fields[field];
        column.values += 1;
        column.integers &= trimmed.parse::<i64>().is_ok();
        column.numbers &= trimmed.parse::<f64>().is_ok_and(f64::is_finite);
        let value = if trimmed.len() == value.len() {
            value
        } else {
            trimmed.to_string()
        };
        self.record.values.insert(field, value);
    }

    fn end_record(&mut self) {
        let record = std::mem::take(&mut self.record);
        self.place = Place::Header(0);
        self.names.push(record.name);
        self.atoms.push(record.atoms);
        self.bonds.push(record.bonds);
        let mut values = record.values;
        for (i, column) in self.columns.iter_mut().enumerate() {
            let value = values.remove(&i);
            self.held += value.as_ref().map_or(0, String::len);
            column.push(value);
        }
        self.rows += 1;
        self.stats.records += 1;
    }
}

/// Whether `line` starts a data item.
fn is_item_header(line: &str) -> bool {
    line.starts_with('>')
}

/// The field a data item header names: what is between `<` and `>`, or else the first
/// word after the `>` (`> DT12`).
pub fn item_name(line: &str) -> Option<String> {
    let rest = line.strip_prefix('>')?;
    let name = match rest.find('<') {
        Some(open) => {
            let after = &rest[open + 1..];
            let close = after.find('>')?;
            &after[..close]
        }
        None => rest.split_whitespace().next()?,
    };
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    let mut cut = name.len().min(MAX_NAME);
    while !name.is_char_boundary(cut) {
        cut -= 1;
    }
    Some(name[..cut].to_string())
}

/// The fields as numbers where every value was one.
fn type_fields(lf: LazyFrame, fields: &[FieldColumn]) -> LazyFrame {
    let casts: Vec<Expr> = fields
        .iter()
        .filter(|f| f.dtype() != DataType::String)
        .map(|f| col(f.name.as_str()).cast(f.dtype()))
        .collect();
    if casts.is_empty() {
        lf
    } else {
        lf.with_columns(casts)
    }
}

fn notes(stats: &Stats) -> Vec<Note> {
    let mut notes = Vec::new();
    let of_records = format!("of {}", count(stats.records, "record", "records"));
    if stats.long_lines > 0 {
        notes.push(note(
            format!(
                "{} longer than {} MiB, cut there",
                count(stats.long_lines, "line is", "lines are"),
                MAX_LINE >> 20
            ),
            "in the whole file".to_string(),
        ));
    }
    if stats.long_values > 0 {
        notes.push(note(
            format!(
                "{} longer than {} MiB, cut there",
                count(stats.long_values, "value is", "values are"),
                MAX_VALUE >> 20
            ),
            of_records.clone(),
        ));
    }
    if stats.repeated > 0 {
        notes.push(note(
            format!(
                "{} a field its record already had; the first is kept",
                count(stats.repeated, "data item repeats", "data items repeat")
            ),
            of_records.clone(),
        ));
    }
    if stats.fields_dropped > 0 {
        notes.push(note(
            format!(
                "{} of fields past the first {MAX_FIELDS} left out",
                count(stats.fields_dropped, "data item", "data items")
            ),
            of_records.clone(),
        ));
    }
    if stats.unterminated {
        notes.push(note(
            "The last record has no $$$$; it is read as it stands".to_string(),
            of_records,
        ));
    }
    notes
}

/// The SDF tab of the Info panel: the counts and each field's type and coverage.
pub fn detail(reader: &SdfReader) -> Detail {
    let stats = reader.stats();
    let sep = format!(" {} ", crate::glyphs::get().middot);
    let mut head = format!(
        "SDF{sep}{}{sep}{}",
        count(stats.records, "record", "records"),
        count(reader.fields().len() as u64, "field", "fields")
    );
    if stats.v3000 > 0 {
        head.push_str(&sep);
        head.push_str(&format!(
            "{} V3000",
            count(stats.v3000, "record", "records")
        ));
    }
    let list = capped_list(
        reader.fields().iter().map(|f| {
            (
                f.name.clone(),
                MetaValue::Text(format!(
                    "{}{sep}in {} of {}",
                    f.dtype(),
                    crate::numfmt::group_chrome(f.values as usize),
                    count(stats.records, "record", "records"),
                )),
            )
        }),
        reader.fields().len(),
    );
    Detail {
        tab: "SDF",
        lines: vec![head],
        list_title: "Fields",
        list,
        first: false,
    }
}

/// Read an SDF file, given a piece at a time by `pieces`, into segments written through
/// `writer`.
pub(crate) fn convert(
    display: &Path,
    options: &OpenOptions,
    writer: &Writer,
    pieces: &mut Pieces<'_>,
) -> Result<(Converted, Detail)> {
    let mut reader = SdfReader::new();
    let mut segments = Segments::new(options, writer);
    pieces(&mut |piece| {
        reader.push(piece);
        if let Some(df) = reader.take_batch()? {
            segments.write(&df)?;
        }
        Ok(())
    })?;
    let last = reader.finish()?;
    if reader.stats().records == 0 {
        return Err(eyre!("No SDF records in {}.", display.display()));
    }
    segments.write(&last)?;
    let (lf, files) = segments.finish()?;
    let lf = type_fields(lf, reader.fields());
    Ok((
        Converted {
            lf,
            files,
            notes: notes(reader.stats()),
            other_tables: Vec::new(),
        },
        detail(&reader),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "aspirin
  RDKit          2D

  3  2  0  0  0  0  0  0  0  0999 V2000
    0.0000    0.0000    0.0000 C   0  0  0  0  0  0  0  0  0  0  0  0
    1.2990    0.7500    0.0000 C   0  0
    2.5981   -0.0000    0.0000 O   0  0
  1  2  1  0
  2  3  2  0
M  END
> <ID>
101

> <LogP>  (MD-1)
1.19

> <SMILES>
CC(=O)O

$$$$

  RDKit          2D

  1  0  0  0  0  0  0  0  0  0999 V2000
    0.0000    0.0000    0.0000 N   0  0
M  END
>  <ID>
102

> 25 <Notes>
first line
second line

> <LogP>
n/a

$$$$
";

    fn read(text: &[u8], piece: usize) -> (DataFrame, SdfReader) {
        let mut reader = SdfReader::new();
        let mut frames = Vec::new();
        for chunk in text.chunks(piece) {
            reader.push(chunk);
            if let Some(df) = reader.take_batch().unwrap() {
                frames.push(df);
            }
        }
        frames.push(reader.finish().unwrap());
        let width = frames.last().unwrap().width();
        let mut df = frames.pop().unwrap();
        for f in frames.into_iter().rev() {
            assert!(f.width() <= width);
            df = f.vstack(&df).unwrap_or(df);
        }
        (df, reader)
    }

    fn strings(df: &DataFrame, name: &str) -> Vec<Option<String>> {
        df.column(name)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|s| s.map(String::from))
            .collect()
    }

    #[test]
    fn records_read_as_rows_with_their_fields() {
        for piece in [1, 5, 64, 4096] {
            let (df, reader) = read(SAMPLE.as_bytes(), piece);
            assert_eq!(df.height(), 2, "piece {piece}");
            assert_eq!(
                df.get_column_names(),
                ["name", "atoms", "bonds", "ID", "LogP", "SMILES", "Notes"]
            );
            assert_eq!(strings(&df, "name"), [Some("aspirin".into()), None]);
            let atoms: Vec<_> = df.column("atoms").unwrap().u32().unwrap().iter().collect();
            assert_eq!(atoms, [Some(3), Some(1)]);
            let bonds: Vec<_> = df.column("bonds").unwrap().u32().unwrap().iter().collect();
            assert_eq!(bonds, [Some(2), Some(0)]);
            assert_eq!(
                strings(&df, "Notes"),
                [None, Some("first line\nsecond line".into())]
            );
            assert_eq!(strings(&df, "SMILES"), [Some("CC(=O)O".into()), None]);
            let fields = reader.fields();
            assert_eq!(fields[0].dtype(), DataType::Int64);
            assert_eq!(fields[1].dtype(), DataType::String, "n/a is not a number");
            assert_eq!(reader.stats().records, 2);
            assert!(!reader.stats().unterminated);
        }
    }

    #[test]
    fn field_types_are_cast_on_the_frame() {
        let (df, reader) = read(b"m\n\n\n  0  0  0  0  0  0  0  0  0  0999 V2000\nM  END\n> <MW>\n180.16\n\n> <N>\n3\n\n$$$$\n", 7);
        let lf = type_fields(df.lazy(), reader.fields());
        let df = lf.collect().unwrap();
        assert_eq!(df.column("MW").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("N").unwrap().i64().unwrap().get(0), Some(3));
    }

    #[test]
    fn v3000_counts_and_an_unterminated_record() {
        let text = "x\n\n\n  0  0  0     0  0            999 V3000\nM  V30 BEGIN CTAB\nM  V30 COUNTS 12 11 0 0 0\nM  V30 END CTAB\nM  END\n> <A>\n1\n";
        let (df, reader) = read(text.as_bytes(), 3);
        assert_eq!(df.column("atoms").unwrap().u32().unwrap().get(0), Some(12));
        assert_eq!(df.column("bonds").unwrap().u32().unwrap().get(0), Some(11));
        assert!(reader.stats().unterminated);
        assert_eq!(reader.stats().v3000, 1);
    }

    #[test]
    fn item_names() {
        assert_eq!(item_name("> <KEY>").as_deref(), Some("KEY"));
        assert_eq!(item_name(">  <KEY> (ID)").as_deref(), Some("KEY"));
        assert_eq!(item_name("> 25 <a b>").as_deref(), Some("a b"));
        assert_eq!(item_name("> DT12 (MD-08974)").as_deref(), Some("DT12"));
        assert_eq!(item_name("> <>"), None);
        assert_eq!(item_name(">"), None);
    }

    #[test]
    fn long_lines_and_values_are_cut() {
        let mut text = b"m\n\n\n  0  0\nM  END\n> <Big>\n".to_vec();
        text.extend(std::iter::repeat_n(b'a', MAX_LINE + 5));
        text.extend_from_slice(b"\nmore\n\n$$$$\n");
        let (df, reader) = read(&text, 1 << 16);
        let big = df
            .column("Big")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .len();
        assert!(big <= MAX_VALUE, "{big}");
        assert_eq!(reader.stats().long_lines, 1);
        assert_eq!(reader.stats().long_values, 1);
    }

    #[test]
    fn sniffing() {
        assert!(looks_like(SAMPLE.as_bytes()));
        assert!(looks_like(b"\n\n\nM  END\n> <A>\n1\n\n$$$$\n"));
        assert!(!looks_like(b"a,b\n1,2\n"));
    }
}

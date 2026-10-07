//! A file's header, footer and lookups read, and where each record field sits.

use super::*;

/// A header, read: each named field's value, and how many bytes the header takes;
/// and the footer's values, and the symbol lists fields index into.
#[derive(Debug, Default, Clone)]
pub struct HeaderValues {
    pub values: Vec<(String, AnyValue<'static>)>,
    pub size: u64,
    pub footer: Vec<(String, AnyValue<'static>)>,
    pub lookups: BTreeMap<String, Arc<Vec<String>>>,
}

fn int_value(value: &AnyValue<'_>) -> Option<i128> {
    match value {
        AnyValue::UInt8(v) => Some(i128::from(*v)),
        AnyValue::UInt16(v) => Some(i128::from(*v)),
        AnyValue::UInt32(v) => Some(i128::from(*v)),
        AnyValue::UInt64(v) => Some(i128::from(*v)),
        AnyValue::Int8(v) => Some(i128::from(*v)),
        AnyValue::Int16(v) => Some(i128::from(*v)),
        AnyValue::Int32(v) => Some(i128::from(*v)),
        AnyValue::Int64(v) => Some(i128::from(*v)),
        _ => None,
    }
}

impl HeaderValues {
    fn get(&self, name: &str) -> Option<&AnyValue<'static>> {
        self.values.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    fn footer_int(&self, name: &str) -> Option<i128> {
        self.footer
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, v)| int_value(v))
    }

    /// `amount`'s value with no bound but its sign: an offset or a count, which the
    /// file's length bounds where it is used.
    pub(crate) fn resolve_any(&self, amount: &Amount, what: &str) -> Result<u64, String> {
        let (value, field) = match amount {
            Amount::Given(n) => return Ok(*n),
            Amount::Header { field, adjust } => (
                self.int(field).map(|v| v + i128::from(*adjust)),
                format!("header's `{field}`"),
            ),
            Amount::Footer { field, adjust } => (
                self.footer_int(field).map(|v| v + i128::from(*adjust)),
                format!("footer's `{field}`"),
            ),
            Amount::Record { .. } | Amount::Rest => {
                return Err(format!("{what}: comes from each record"));
            }
        };
        let value = value.ok_or_else(|| format!("{what}: the {field} has no value"))?;
        u64::try_from(value).map_err(|_| format!("{what}: the {field} gives {value}, below 0"))
    }

    pub(crate) fn int(&self, name: &str) -> Option<i128> {
        int_value(self.get(name)?)
    }

    pub(crate) fn text(&self, name: &str) -> Option<String> {
        match self.get(name)? {
            AnyValue::String(s) => Some(s.to_string()),
            AnyValue::StringOwned(s) => Some(s.to_string()),
            _ => None,
        }
    }

    /// Nanoseconds from the Unix epoch to midnight of the date header field `name`
    /// holds: a date, a datetime, or text such as 2024-01-02 or 20240102.
    fn midnight_ns(&self, name: &str) -> Option<i64> {
        let days = match self.get(name)? {
            AnyValue::Date(days) => i64::from(*days),
            AnyValue::Datetime(v, unit, _) | AnyValue::DatetimeOwned(v, unit, _) => {
                let per_day = DAY_NS
                    / match unit {
                        TimeUnit::Nanoseconds => 1,
                        TimeUnit::Microseconds => 1_000,
                        TimeUnit::Milliseconds => 1_000_000,
                    };
                v.div_euclid(per_day)
            }
            other => {
                let text = match other {
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::StringOwned(s) => s.to_string(),
                    _ => return None,
                };
                let date = chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                    .or_else(|_| chrono::NaiveDate::parse_from_str(&text, "%Y%m%d"))
                    .ok()?;
                (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days()
            }
        };
        days.checked_mul(DAY_NS)
    }

    /// `amount`'s value: given, or read from a header or footer field and bounded.
    pub(crate) fn resolve(&self, amount: &Amount, what: &str) -> Result<u64, String> {
        match amount {
            Amount::Given(n) => Ok(*n),
            Amount::Record { .. } | Amount::Rest => Err(format!(
                "{what}: comes from each record, which needs the records walked"
            )),
            Amount::Header { field, adjust } | Amount::Footer { field, adjust } => {
                let (part, value) = match amount {
                    Amount::Footer { .. } => ("footer", self.footer_int(field)),
                    _ => ("header", self.int(field)),
                };
                let value = value
                    .ok_or_else(|| format!("{what}: the {part} has no value for `{field}`"))?
                    + i128::from(*adjust);
                // A record count is bounded by the file instead.
                let bound = if what == "count" {
                    i128::from(u64::MAX)
                } else {
                    i128::from(MAX_SIZE)
                };
                if !(0..=bound).contains(&value) {
                    return Err(format!(
                        "{what}: the header's `{field}` gives {value}, outside 0 to {bound}"
                    ));
                }
                Ok(value as u64)
            }
        }
    }
}

/// Where a column's cells are: the first at `start`, `stride` apart (a cell's own
/// width when `None`), each `count` values of `width` bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Place {
    pub start: usize,
    pub stride: Option<usize>,
    pub width: usize,
    pub count: usize,
}

/// The decoder's view of `field`, named `name`, its cells at `place`.
pub(crate) fn layout_of(
    spec: &Spec,
    field: &Field,
    name: &str,
    place: Place,
    header: &HeaderValues,
) -> Result<ColumnLayout, String> {
    let Place {
        start,
        stride,
        width,
        count,
    } = place;
    let logical = match &field.meaning {
        Meaning::Plain if field.lookup.is_some() => Logical::Lookup(
            field
                .name
                .as_ref()
                .and_then(|n| header.lookups.get(n))
                .cloned()
                .ok_or_else(|| format!("{name}: its symbol list was not read"))?,
        ),
        Meaning::Plain => Logical::Plain,
        Meaning::Scale(scale) => Logical::Decimal {
            scale: *scale as usize,
        },
        Meaning::Linear { factor, offset } => Logical::Linear {
            factor: *factor,
            offset: *offset,
        },
        Meaning::Enum(labels) => Logical::Enum(labels.clone()),
        Meaning::Yyyymmdd => Logical::Yyyymmdd,
        Meaning::Time { unit, epoch_ns } => match (field.ty, unit) {
            (Type::Float(_), unit) => Logical::FloatTimestamp {
                ns_per_unit: unit.nanos() as f64,
                epoch_ns: *epoch_ns,
            },
            (_, TimeUnitSpec::Days) => Logical::Days {
                epoch_days: i32::try_from(epoch_ns.div_euclid(DAY_NS))
                    .map_err(|_| format!("{name}: the epoch is out of range"))?,
            },
            (_, unit) => {
                let (unit, multiplier, per) = match unit {
                    TimeUnitSpec::Seconds => (TimeUnit::Milliseconds, 1000, 1_000_000),
                    TimeUnitSpec::Millis => (TimeUnit::Milliseconds, 1, 1_000_000),
                    TimeUnitSpec::Micros => (TimeUnit::Microseconds, 1, 1_000),
                    _ => (TimeUnit::Nanoseconds, 1, 1),
                };
                Logical::Timestamp {
                    unit,
                    multiplier,
                    epoch: epoch_ns / per,
                }
            }
        },
        Meaning::TimeOfDay { unit, date } => Logical::TimeOfDay {
            ns_per_unit: unit.nanos(),
            date_ns: match date {
                None => None,
                Some(field) => Some(
                    header
                        .midnight_ns(field)
                        .ok_or_else(|| format!("{name}: the header's `{field}` holds no date"))?,
                ),
            },
        },
    };
    Ok(ColumnLayout {
        name: PlSmallStr::from(name),
        source: 0,
        start,
        stride: stride.unwrap_or(width * count),
        width,
        count,
        physical: if field.ty == Type::Str {
            field.encoding.physical()
        } else {
            field.ty.physical()
        },
        big_endian: field.endian.unwrap_or(spec.endian) == Endian::Big,
        null: field.null,
        logical,
    })
}

/// How many bytes one value of `field` takes and how many values it holds, now that
/// the header is read.
pub(crate) fn sized(field: &Field, header: &HeaderValues) -> Result<(u64, u64), String> {
    let width = match (field.ty.width(), &field.size) {
        (Some(w), _) => w,
        (None, Some(amount)) => header.resolve(amount, "size")?,
        (None, None) => 0,
    };
    let count = match &field.count {
        None => 1,
        Some(amount) => header.resolve(amount, "count")?,
    };
    if count > MAX_SIZE || width.saturating_mul(count) > MAX_SIZE {
        return Err(format!(
            "field `{}` would take {count} values of {width} bytes, more than {MAX_SIZE}",
            field.name.as_deref().unwrap_or("pad")
        ));
    }
    Ok((width, count))
}

/// Read `fields` from `bytes` at `at`, sizing each as it goes, into `read`'s header
/// values or (for `footer`) its footer values. Gives where they end.
fn read_fields(
    spec: &Spec,
    fields: &[Field],
    bytes: &[u8],
    mut at: u64,
    read: &mut HeaderValues,
    footer: bool,
) -> Result<u64, String> {
    for field in fields {
        let (width, count) = sized(field, read)?;
        let end = at + width * count;
        if end > bytes.len() as u64 {
            return Err(format!(
                "the file is {} bytes, too short for its {} (field `{}` ends at byte {end})",
                bytes.len(),
                if footer { "footer" } else { "header" },
                field.name.as_deref().unwrap_or("pad")
            ));
        }
        if let Some(name) = &field.name
            && width > 0
        {
            let place = Place {
                start: at as usize,
                stride: None,
                width: width as usize,
                count: count as usize,
            };
            let layout = layout_of(spec, field, name, place, read)?;
            let column = crate::formats::fixed_records::decode(bytes, &layout, 1)
                .map_err(|e| e.to_string())?;
            let value = column.get(0).map_err(|e| e.to_string())?.into_static();
            if footer {
                read.footer.push((name.clone(), value));
            } else {
                read.values.push((name.clone(), value));
            }
        }
        at = end;
    }
    Ok(at)
}

/// Read the header from the front of `bytes`, sizing each field as it goes.
pub(crate) fn read_header(spec: &Spec, bytes: &[u8]) -> Result<HeaderValues, String> {
    let mut read = HeaderValues::default();
    let at = read_fields(spec, &spec.header.fields, bytes, 0, &mut read, false)?;
    read.size = match &spec.header.size {
        None => at,
        Some(amount) => {
            let size = read.resolve(amount, "header size")?;
            if size < at {
                return Err(format!(
                    "the header's fields take {at} bytes, more than its size of {size}"
                ));
            }
            size
        }
    };
    Ok(read)
}

/// Read the footer at the end of `bytes` into `read`: where it starts, and a note on
/// its checksum.
pub(crate) fn read_footer(
    spec: &Spec,
    bytes: &[u8],
    read: &mut HeaderValues,
) -> Result<(u64, Option<String>), String> {
    let len = bytes.len() as u64;
    let Some(footer) = &spec.footer else {
        return Ok((len, None));
    };
    let size = footer
        .size
        .or_else(|| given_width(&footer.fields))
        .unwrap_or(0);
    let start = len
        .checked_sub(size)
        .filter(|s| *s >= read.size)
        .ok_or_else(|| {
            format!(
                "the file is {len} bytes, too short for its {}-byte header and {size}-byte footer",
                read.size
            )
        })?;
    read_fields(spec, &footer.fields, bytes, start, read, true)?;
    // Notes are warnings: a checksum that matches says nothing.
    let note = footer.checksum.as_ref().and_then(|(algo, field)| {
        let stored = read.footer_int(field);
        let computed = algo.compute(&bytes[..start as usize]);
        match stored {
            Some(v) if v == i128::from(computed) => None,
            Some(v) => Some(format!(
                "the footer's checksum `{field}` is {v:#x}; the file's is {computed:#x}"
            )),
            None => Some(format!(
                "the footer has no value for its checksum `{field}`"
            )),
        }
    });
    Ok((start, note))
}

/// The symbol lists `fields` index into, read from beside `dir`.
pub(crate) fn read_lookups(
    fields: &[Field],
    dir: Option<&Path>,
    read: &mut HeaderValues,
) -> Result<(), String> {
    for field in fields {
        let (Some(name), Some(lookup)) = (&field.name, &field.lookup) else {
            continue;
        };
        let dir = dir.ok_or_else(|| {
            format!("{name}: its symbol list {} is beside the data, which was not opened from a directory", lookup.file)
        })?;
        let path = dir.join(&lookup.file);
        let size = std::fs::metadata(&path)
            .map_err(|e| format!("{name}: the symbol list {}: {e}", path.display()))?
            .len();
        if size > MAX_SIZE {
            return Err(format!(
                "{name}: the symbol list {} is {size} bytes, more than {MAX_SIZE}",
                path.display()
            ));
        }
        let bytes = std::fs::read(&path)
            .map_err(|e| format!("{name}: the symbol list {}: {e}", path.display()))?;
        read.lookups
            .insert(name.clone(), Arc::new(symbols(&bytes, lookup.format)));
    }
    Ok(())
}

/// The entries of a symbol list.
pub fn symbols(bytes: &[u8], format: LookupFormat) -> Vec<String> {
    match format {
        LookupFormat::Lines => String::from_utf8_lossy(bytes)
            .lines()
            .map(|l| l.trim_end_matches('\r').to_string())
            .collect(),
        LookupFormat::Nul => {
            let mut out: Vec<String> = bytes
                .split(|b| *b == 0)
                .map(|s| String::from_utf8_lossy(s).into_owned())
                .collect();
            // The list ends with a NUL, which leaves an empty piece after it.
            if bytes.last() == Some(&0) {
                out.pop();
            }
            out
        }
        LookupFormat::Fixed(n) => bytes
            .chunks(n as usize)
            .map(crate::formats::fixed_records::text)
            .collect(),
    }
}

/// The rows a spec reads, however they are framed: what the table, a window of it,
/// `formats check` and the fuzz target read through.
pub trait SpecRecords: crate::formats::pushdown::Windowed + std::fmt::Debug {
    fn rows(&self) -> usize;
    fn schema(&self) -> SchemaRef;
    /// The frame: a scan that decodes only what a query asks for.
    fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame>;
    /// The first `rows` rows, decoded now.
    fn collect(&self, rows: usize) -> PolarsResult<DataFrame>;
    /// The bytes the rows are read from, for a reader of the raw bytes.
    fn sources(&self) -> &[Arc<Bytes>];
}

impl std::fmt::Debug for FixedRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedRecords")
            .field("rows", &self.rows())
            .finish()
    }
}

impl SpecRecords for FixedRecords {
    fn rows(&self) -> usize {
        FixedRecords::rows(self)
    }
    fn schema(&self) -> SchemaRef {
        FixedRecords::schema(self)
    }
    fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
        Ok(FixedRecords::lazy(&self))
    }
    fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        FixedRecords::collect(self, rows)
    }
    fn sources(&self) -> &[Arc<Bytes>] {
        FixedRecords::sources(self)
    }
}

impl SpecRecords for crate::formats::framed_records::FramedRecords {
    fn rows(&self) -> usize {
        crate::formats::framed_records::FramedRecords::rows(self)
    }
    fn schema(&self) -> SchemaRef {
        crate::formats::framed_records::FramedRecords::schema(self)
    }
    fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
        Ok(crate::formats::framed_records::FramedRecords::lazy(&self))
    }
    fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        crate::formats::framed_records::FramedRecords::collect(self, rows)
    }
    fn sources(&self) -> &[Arc<Bytes>] {
        crate::formats::framed_records::FramedRecords::sources(self)
    }
}

/// A file read through a spec: its columns, and what the read had to say.
pub struct Opened {
    pub records: Arc<dyn SpecRecords>,
    /// Warnings for the dataset's notes, one sentence each.
    pub notes: Vec<String>,
    pub header: HeaderValues,
}

/// A note for the records of `rows` past the most a table holds, which are not shown.
pub(crate) fn past_limit(notes: &mut Vec<String>, rows: u64, records: &FixedRecords) {
    let past = rows.saturating_sub(records.rows() as u64);
    if past > 0 {
        notes.push(format!(
            "last {past} records not shown: past the table limit"
        ));
    }
}

/// Up to this many trailing bytes are shown in the warning about them.
const TRAILING_SHOWN: usize = 32;

pub(crate) fn trailing_note(what: &str, bytes: &[u8]) -> String {
    let shown = &bytes[..bytes.len().min(TRAILING_SHOWN)];
    let more = if bytes.len() > shown.len() {
        " ..."
    } else {
        ""
    };
    format!(
        "{what}: {} trailing {} left out, not a whole record: {}{more}",
        bytes.len(),
        if bytes.len() == 1 { "byte" } else { "bytes" },
        crate::formats::fixed_records::hex(shown),
    )
}

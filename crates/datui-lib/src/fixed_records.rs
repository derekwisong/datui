//! Fixed-width values read straight out of a memory map.
//!
//! A column is a run of values `width` bytes wide, `stride` bytes apart, starting
//! `start` bytes into one of the reader's sources; `count` values side by side make one
//! Array cell. Rows of records (each field a column with the record size as its stride)
//! and one file per column (stride equal to width) are both that shape, so one strided
//! decoder serves the format specs and NumPy arrays, and [`decode`] is public for other
//! readers of packed values (audio samples, CAN signals). The sources are kept, so a
//! viewer of the raw bytes can read them too.
//!
//! The frame is a Polars anonymous scan: only the projected columns are decoded, and
//! only the first `n_rows` of them. Polars hands an anonymous scan no row offset, so a
//! window deeper in the file is read through [`FixedRecords::window`], which starts
//! the columns further in rather than decoding from row 0.
//!
//! Polars 0.55's streaming engine cannot run an anonymous scan (it stops at a
//! `todo!`), so [`in_plan`] tells the places that pick an engine to use the in-memory
//! one.

use polars::prelude::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// The name the scan carries in a plan, and what [`in_plan`] looks for.
pub const SCAN_NAME: &str = "FIXED RECORDS";

/// Nanoseconds in a day.
const DAY_NS: i64 = 86_400_000_000_000;

/// The bytes a column reads.
pub enum Bytes {
    /// A file, mapped.
    Mapped(memmap2::Mmap),
    /// Bytes in memory, for tests and the fuzz target.
    Owned(Vec<u8>),
}

impl Bytes {
    /// Map `path` read-only.
    pub fn map(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        // An empty file cannot be mapped on every platform, and has nothing to read.
        if file.metadata()?.len() == 0 {
            return Ok(Self::Owned(Vec::new()));
        }
        // SAFETY: the map is read-only and lives as long as the scan. A file truncated
        // by another process while it is mapped faults on access, as it does for every
        // reader that maps (Polars' own IPC reader included); datui does not write to
        // the files it reads.
        let map = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self::Mapped(map))
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(map) => map,
            Self::Owned(bytes) => bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// How a value's bytes are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Physical {
    /// An unsigned integer of 1 to 8 bytes.
    Unsigned(u8),
    /// A two's-complement integer of 1 to 8 bytes.
    Signed(u8),
    /// An IEEE float of 4 or 8 bytes.
    Float(u8),
    /// One byte, nonzero for true.
    Bool,
    /// Text, its padding (NUL and spaces) trimmed from the right.
    Text,
    /// Raw bytes.
    Raw,
}

impl Physical {
    /// Bytes the type itself takes, or `None` for text and raw bytes.
    pub fn width(self) -> Option<usize> {
        match self {
            Self::Unsigned(n) | Self::Signed(n) | Self::Float(n) => Some(n as usize),
            Self::Bool => Some(1),
            Self::Text | Self::Raw => None,
        }
    }

    pub fn is_integer(self) -> bool {
        matches!(self, Self::Unsigned(_) | Self::Signed(_))
    }

    fn plain_dtype(self) -> DataType {
        match self {
            Self::Unsigned(1) => DataType::UInt8,
            Self::Unsigned(2) => DataType::UInt16,
            Self::Unsigned(3 | 4) => DataType::UInt32,
            Self::Unsigned(_) => DataType::UInt64,
            Self::Signed(1) => DataType::Int8,
            Self::Signed(2) => DataType::Int16,
            Self::Signed(3 | 4) => DataType::Int32,
            Self::Signed(_) => DataType::Int64,
            Self::Float(4) => DataType::Float32,
            Self::Float(_) => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Text => DataType::String,
            Self::Raw => DataType::Binary,
        }
    }
}

/// A stored value that means "no value".
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Null {
    /// The type's smallest value: `i32::MIN` for an `s4`, 0 for a `u4`.
    Min,
    /// The type's largest value: `u32::MAX` for a `u4` (all ones), `i32::MAX` for an `s4`.
    Max,
    /// A float NaN.
    NaN,
    /// This value, as stored.
    Value(i128),
}

/// What a value means beyond how it is stored.
#[derive(Debug, Clone, PartialEq)]
pub enum Logical {
    /// The value as stored.
    Plain,
    /// An integer count of a unit since an epoch, as a datetime: `value * multiplier +
    /// epoch`, in `unit`.
    Timestamp {
        unit: TimeUnit,
        multiplier: i64,
        epoch: i64,
    },
    /// A float count of a unit since an epoch, as a nanosecond datetime: serial dates.
    FloatTimestamp { ns_per_unit: f64, epoch_ns: i64 },
    /// A count of days since `epoch_days` days past 1970-01-01, as a date.
    Days { epoch_days: i32 },
    /// An integer written as `YYYYMMDD`, as a date; anything else is null.
    Yyyymmdd,
    /// A count of a unit since midnight: a time of day, or a datetime on `date_ns`
    /// (nanoseconds from the Unix epoch to that day's midnight).
    TimeOfDay {
        ns_per_unit: i64,
        date_ns: Option<i64>,
    },
    /// An integer with `scale` implied decimal places.
    Decimal { scale: usize },
    /// `value * factor + offset`, as a float.
    Linear { factor: f64, offset: f64 },
    /// A code and its label; a code with no label reads as its number.
    Enum(Arc<BTreeMap<i64, String>>),
}

/// One column: where its values are and how to read them.
#[derive(Debug, Clone)]
pub struct ColumnLayout {
    pub name: PlSmallStr,
    /// Index into the reader's sources.
    pub source: usize,
    /// Byte offset of row 0's value.
    pub start: usize,
    /// Bytes from one row's value to the next.
    pub stride: usize,
    /// Bytes one value takes.
    pub width: usize,
    /// Values side by side in each row: more than one makes the cell an Array.
    pub count: usize,
    pub physical: Physical,
    pub big_endian: bool,
    pub null: Option<Null>,
    pub logical: Logical,
}

impl ColumnLayout {
    /// A plain column of one `physical` value a row, little-endian.
    pub fn new(name: &str, start: usize, stride: usize, physical: Physical, width: usize) -> Self {
        Self {
            name: name.into(),
            source: 0,
            start,
            stride,
            width,
            count: 1,
            physical,
            big_endian: false,
            null: None,
            logical: Logical::Plain,
        }
    }

    /// The type of one value.
    pub fn value_dtype(&self) -> DataType {
        match &self.logical {
            Logical::Timestamp { unit, .. } => DataType::Datetime(*unit, None),
            Logical::FloatTimestamp { .. } => DataType::Datetime(TimeUnit::Nanoseconds, None),
            Logical::Days { .. } | Logical::Yyyymmdd => DataType::Date,
            Logical::TimeOfDay { date_ns: None, .. } => DataType::Time,
            Logical::TimeOfDay {
                date_ns: Some(_), ..
            } => DataType::Datetime(TimeUnit::Nanoseconds, None),
            Logical::Decimal { scale } => DataType::Decimal(38, *scale),
            Logical::Linear { .. } => DataType::Float64,
            Logical::Enum(_) => DataType::String,
            Logical::Plain => self.physical.plain_dtype(),
        }
    }

    /// The column's type: the value's, or an Array of `count` of them.
    pub fn dtype(&self) -> DataType {
        if self.count > 1 {
            DataType::Array(Box::new(self.value_dtype()), self.count)
        } else {
            self.value_dtype()
        }
    }

    /// Bytes one row's cell takes.
    fn cell_width(&self) -> usize {
        self.width * self.count.max(1)
    }

    /// Rows this column can give from a source of `len` bytes.
    fn rows_in(&self, len: usize) -> usize {
        let cell = self.cell_width();
        if self.stride == 0 || len <= self.start {
            return 0;
        }
        let room = len - self.start;
        if room < cell {
            0
        } else {
            (room - cell) / self.stride + 1
        }
    }

    /// Check the layout reads what it says: widths that fit the type, sensible counts.
    pub fn validate(&self) -> PolarsResult<()> {
        polars_ensure!(
            self.stride > 0 && self.width > 0 && self.count > 0,
            ComputeError: "column {} has no width", self.name
        );
        if let Some(width) = self.physical.width() {
            polars_ensure!(
                width == self.width,
                ComputeError: "column {} is {} bytes wide, not {width}", self.name, self.width
            );
        }
        if let Physical::Unsigned(n) | Physical::Signed(n) = self.physical {
            polars_ensure!(
                (1..=8).contains(&n),
                ComputeError: "column {}: an integer is 1 to 8 bytes", self.name
            );
        }
        if let Physical::Float(n) = self.physical {
            polars_ensure!(
                n == 4 || n == 8,
                ComputeError: "column {}: a float is 4 or 8 bytes", self.name
            );
        }
        Ok(())
    }
}

/// An integer of `bytes.len()` bytes, unsigned.
pub fn read_unsigned(bytes: &[u8], big_endian: bool) -> u64 {
    let mut value = 0u64;
    if big_endian {
        for b in bytes {
            value = (value << 8) | u64::from(*b);
        }
    } else {
        for b in bytes.iter().rev() {
            value = (value << 8) | u64::from(*b);
        }
    }
    value
}

/// An integer of `bytes.len()` bytes, two's complement.
pub fn read_signed(bytes: &[u8], big_endian: bool) -> i64 {
    let raw = read_unsigned(bytes, big_endian);
    let bits = bytes.len() * 8;
    if bits == 0 || bits >= 64 {
        raw as i64
    } else {
        // Sign-extend from the value's own width.
        ((raw << (64 - bits)) as i64) >> (64 - bits)
    }
}

/// The value `null` stands for in a `physical` integer, as stored.
fn null_integer(null: Null, physical: Physical) -> Option<i128> {
    let bits = u32::from(match physical {
        Physical::Unsigned(n) | Physical::Signed(n) => n,
        Physical::Bool => 1,
        _ => return None,
    }) * 8;
    let signed = matches!(physical, Physical::Signed(_));
    match null {
        Null::Min if signed => Some(-(1i128 << (bits - 1))),
        Null::Min => Some(0),
        Null::Max if signed => Some((1i128 << (bits - 1)) - 1),
        Null::Max => Some((1i128 << bits) - 1),
        Null::Value(v) => Some(v),
        Null::NaN => None,
    }
}

/// The first `rows` values of `column` from `bytes`: one cell a row, an Array of
/// `column.count` values each when it holds more than one. The caller makes sure
/// every value it asks for lies inside `bytes` ([`FixedRecords::new`] does).
pub fn decode(bytes: &[u8], column: &ColumnLayout, rows: usize) -> PolarsResult<Column> {
    let count = column.count.max(1);
    let values = rows * count;
    // Each value's bytes, row by row and in a row left to right.
    let at = move |i: usize| {
        let start = column.start + (i / count) * column.stride + (i % count) * column.width;
        &bytes[start..start + column.width]
    };
    let name = column.name.clone();
    let flat = decode_values(column, values, at)?.with_name(name.clone());
    if count == 1 {
        return Ok(flat.into_column());
    }
    flat.reshape_array(&[
        ReshapeDimension::new(rows as i64),
        ReshapeDimension::new(count as i64),
    ])
    .map(|s| s.with_name(name).into_column())
}

/// `n` values, the `i`th of them in `at(i)`.
fn decode_values<'a>(
    column: &ColumnLayout,
    n: usize,
    at: impl Fn(usize) -> &'a [u8],
) -> PolarsResult<Series> {
    let big = column.big_endian;
    let name = PlSmallStr::EMPTY;
    macro_rules! native {
        ($ty:ty) => {{
            const N: usize = std::mem::size_of::<$ty>();
            (0..n)
                .map(|i| {
                    let raw: [u8; N] = at(i).try_into().expect("width checked at build");
                    if big {
                        <$ty>::from_be_bytes(raw)
                    } else {
                        <$ty>::from_le_bytes(raw)
                    }
                })
                .collect::<Vec<$ty>>()
        }};
    }
    // The common case at memory speed: a native width, as stored, no sentinel.
    if column.logical == Logical::Plain && column.null.is_none() {
        let fast = match column.physical {
            Physical::Unsigned(1) => Some(Series::new(name.clone(), native!(u8))),
            Physical::Unsigned(2) => Some(Series::new(name.clone(), native!(u16))),
            Physical::Unsigned(4) => Some(Series::new(name.clone(), native!(u32))),
            Physical::Unsigned(8) => Some(Series::new(name.clone(), native!(u64))),
            Physical::Signed(1) => Some(Series::new(name.clone(), native!(i8))),
            Physical::Signed(2) => Some(Series::new(name.clone(), native!(i16))),
            Physical::Signed(4) => Some(Series::new(name.clone(), native!(i32))),
            Physical::Signed(8) => Some(Series::new(name.clone(), native!(i64))),
            Physical::Float(4) => Some(Series::new(name.clone(), native!(f32))),
            Physical::Float(8) => Some(Series::new(name.clone(), native!(f64))),
            _ => None,
        };
        if let Some(series) = fast {
            return Ok(series);
        }
    }
    match column.physical {
        Physical::Unsigned(_) | Physical::Signed(_) | Physical::Bool => {
            let signed = matches!(column.physical, Physical::Signed(_));
            let sentinel = column
                .null
                .and_then(|null| null_integer(null, column.physical));
            let ints: Vec<Option<i128>> = (0..n)
                .map(|i| {
                    let bytes = at(i);
                    let v = if signed {
                        i128::from(read_signed(bytes, big))
                    } else {
                        i128::from(read_unsigned(bytes, big))
                    };
                    (Some(v) != sentinel).then_some(v)
                })
                .collect();
            integers(column, ints)
        }
        Physical::Float(width) => {
            let floats: Vec<Option<f64>> = (0..n)
                .map(|i| {
                    let raw = read_unsigned(at(i), big);
                    let v = if width == 4 {
                        f64::from(f32::from_bits(raw as u32))
                    } else {
                        f64::from_bits(raw)
                    };
                    let null = match column.null {
                        Some(Null::NaN) => v.is_nan(),
                        Some(Null::Value(sentinel)) => v == sentinel as f64,
                        _ => false,
                    };
                    (!null).then_some(v)
                })
                .collect();
            floats_of(column, width, floats)
        }
        Physical::Text => {
            let values: StringChunked = (0..n).map(|i| Some(text(at(i)))).collect();
            Ok(values.into_series())
        }
        Physical::Raw => {
            let values: BinaryChunked = (0..n).map(|i| Some(at(i))).collect();
            Ok(values.into_series())
        }
    }
}

/// Integers as their column means them.
fn integers(column: &ColumnLayout, ints: Vec<Option<i128>>) -> PolarsResult<Series> {
    let as_i64 = |v: Option<i128>| v.and_then(|v| i64::try_from(v).ok());
    Ok(match &column.logical {
        Logical::Plain => match column.physical {
            Physical::Bool => ints
                .into_iter()
                .map(|v| v.map(|v| v != 0))
                .collect::<BooleanChunked>()
                .into_series(),
            physical => {
                let wide: Int128Chunked = ints.into_iter().collect();
                // Each value fits the type `plain_dtype` names for its width.
                wide.into_series().strict_cast(&physical.plain_dtype())?
            }
        },
        Logical::Timestamp {
            unit,
            multiplier,
            epoch,
        } => ints
            .into_iter()
            .map(|v| as_i64(v)?.checked_mul(*multiplier)?.checked_add(*epoch))
            .collect::<Int64Chunked>()
            .into_datetime(*unit, None)
            .into_series(),
        Logical::FloatTimestamp {
            ns_per_unit,
            epoch_ns,
        } => float_timestamps(
            ints.into_iter().map(|v| v.map(|v| v as f64)),
            *ns_per_unit,
            *epoch_ns,
        ),
        Logical::Days { epoch_days } => ints
            .into_iter()
            .map(|v| i32::try_from(as_i64(v)?).ok()?.checked_add(*epoch_days))
            .collect::<Int32Chunked>()
            .into_date()
            .into_series(),
        Logical::Yyyymmdd => ints
            .into_iter()
            .map(|v| yyyymmdd_days(as_i64(v)?))
            .collect::<Int32Chunked>()
            .into_date()
            .into_series(),
        Logical::TimeOfDay {
            ns_per_unit,
            date_ns,
        } => {
            let of_day = ints.into_iter().map(|v| {
                let ns = as_i64(v)?.checked_mul(*ns_per_unit)?;
                (0..DAY_NS).contains(&ns).then_some(ns)
            });
            match date_ns {
                None => of_day.collect::<Int64Chunked>().into_time().into_series(),
                Some(day) => of_day
                    .map(|ns| ns?.checked_add(*day))
                    .collect::<Int64Chunked>()
                    .into_datetime(TimeUnit::Nanoseconds, None)
                    .into_series(),
            }
        }
        Logical::Decimal { scale } => ints
            .into_iter()
            .collect::<Int128Chunked>()
            .into_decimal_unchecked(38, *scale)
            .into_series(),
        Logical::Linear { factor, offset } => ints
            .into_iter()
            .map(|v| v.map(|v| v as f64 * factor + offset))
            .collect::<Float64Chunked>()
            .into_series(),
        Logical::Enum(labels) => ints
            .into_iter()
            .map(|v| {
                v.map(|code| {
                    i64::try_from(code)
                        .ok()
                        .and_then(|code| labels.get(&code).cloned())
                        .unwrap_or_else(|| code.to_string())
                })
            })
            .collect::<StringChunked>()
            .into_series(),
    })
}

/// Floats as their column means them.
fn floats_of(column: &ColumnLayout, width: u8, floats: Vec<Option<f64>>) -> PolarsResult<Series> {
    Ok(match &column.logical {
        Logical::Linear { factor, offset } => floats
            .into_iter()
            .map(|v| v.map(|v| v * factor + offset))
            .collect::<Float64Chunked>()
            .into_series(),
        Logical::FloatTimestamp {
            ns_per_unit,
            epoch_ns,
        } => float_timestamps(floats.into_iter(), *ns_per_unit, *epoch_ns),
        _ if width == 4 => floats
            .into_iter()
            .map(|v| v.map(|v| v as f32))
            .collect::<Float32Chunked>()
            .into_series(),
        _ => floats.into_iter().collect::<Float64Chunked>().into_series(),
    })
}

fn float_timestamps(
    values: impl Iterator<Item = Option<f64>>,
    ns_per_unit: f64,
    epoch_ns: i64,
) -> Series {
    values
        .map(|v| {
            let ns = (v? * ns_per_unit).round();
            // Out of range or not a number reads as null rather than wrapping.
            (ns.is_finite() && ns.abs() < 9.0e18).then(|| (ns as i64).checked_add(epoch_ns))?
        })
        .collect::<Int64Chunked>()
        .into_datetime(TimeUnit::Nanoseconds, None)
        .into_series()
}

/// Days since 1970-01-01 of a date written as the integer `YYYYMMDD`.
fn yyyymmdd_days(v: i64) -> Option<i32> {
    let (year, month, day) = (v / 10_000, (v / 100) % 100, v % 100);
    let date = chrono::NaiveDate::from_ymd_opt(
        i32::try_from(year).ok()?,
        u32::try_from(month).ok()?,
        u32::try_from(day).ok()?,
    )?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((date - epoch).num_days()).ok()
}

/// Text from a fixed-width field: UTF-8 where it is, padding trimmed from the right.
pub fn text(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map_or(0, |i| i + 1);
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

/// Bytes as space-separated hex pairs.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

/// Columns of fixed-width values over one or more sources, `rows` long.
pub struct FixedRecords {
    sources: Vec<Arc<Bytes>>,
    columns: Vec<ColumnLayout>,
    rows: usize,
    schema: SchemaRef,
}

impl FixedRecords {
    /// The columns over `sources`, at most `rows` long: fewer when a source runs out
    /// first, so no read ever goes past the end of one, and a partial last record is
    /// left out rather than refused. The rows come from the sources' lengths as they
    /// are now: a file that has grown is read by building the records again, over a
    /// fresh map, with the same columns and `usize::MAX` rows.
    pub fn new(
        sources: Vec<Arc<Bytes>>,
        columns: Vec<ColumnLayout>,
        rows: usize,
    ) -> PolarsResult<Self> {
        let mut rows = rows;
        for column in &columns {
            column.validate()?;
            let source = sources
                .get(column.source)
                .ok_or_else(|| polars_err!(ComputeError: "column {} has no source", column.name))?;
            rows = rows.min(column.rows_in(source.len()));
        }
        let schema: Schema = columns
            .iter()
            .map(|c| Field::new(c.name.clone(), c.dtype()))
            .collect();
        Ok(Self {
            sources,
            columns,
            rows,
            schema: Arc::new(schema),
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn columns(&self) -> &[ColumnLayout] {
        &self.columns
    }

    /// The bytes the columns read, for a reader of the raw bytes.
    pub fn sources(&self) -> &[Arc<Bytes>] {
        &self.sources
    }

    /// The frame: a scan that decodes only what a query asks for.
    pub fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
        let schema = self.schema.clone();
        LazyFrame::anonymous_scan(
            self,
            ScanArgsAnonymous {
                schema: Some(schema),
                name: SCAN_NAME,
                ..Default::default()
            },
        )
    }

    /// Rows `[start, start + len)` as a frame of their own: the columns start further
    /// into the same sources, so nothing before `start` is decoded.
    pub fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let start = start.min(self.rows);
        let len = len.min(self.rows - start);
        let columns = self
            .columns
            .iter()
            .map(|c| ColumnLayout {
                start: c.start + start * c.stride,
                ..c.clone()
            })
            .collect();
        Arc::new(Self::new(self.sources.clone(), columns, len)?).into_lazy()
    }

    /// The first `rows` rows of every column, decoded now.
    pub fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        let rows = rows.min(self.rows);
        let columns = self
            .columns
            .iter()
            .map(|c| self.decode_column(c, rows))
            .collect::<PolarsResult<Vec<_>>>()?;
        DataFrame::new(rows, columns)
    }

    fn decode_column(&self, column: &ColumnLayout, rows: usize) -> PolarsResult<Column> {
        decode(self.sources[column.source].as_slice(), column, rows)
    }
}

impl AnonymousScan for FixedRecords {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }

    fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        let rows = args.n_rows.map_or(self.rows, |n| n.min(self.rows));
        let columns = match &args.with_columns {
            Some(names) => names
                .iter()
                .map(|name| {
                    let column = self
                        .columns
                        .iter()
                        .find(|c| c.name == *name)
                        .ok_or_else(|| polars_err!(ColumnNotFound: "{name}"))?;
                    self.decode_column(column, rows)
                })
                .collect::<PolarsResult<Vec<_>>>()?,
            None => self
                .columns
                .iter()
                .map(|c| self.decode_column(c, rows))
                .collect::<PolarsResult<Vec<_>>>()?,
        };
        DataFrame::new(rows, columns)
    }
}

/// Whether `lf` reads fixed records, and so has to run on the in-memory engine.
pub fn in_plan(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::{DslPlan, FileScanDsl};
    lf.logical_plan.into_iter().any(|node| match node {
        DslPlan::Scan { scan_type, .. } => matches!(
            scan_type.as_ref(),
            FileScanDsl::Anonymous { options, .. } if options.fmt_str == SCAN_NAME
        ),
        _ => false,
    })
}

/// Whether a query over `lf` may use the streaming engine: asked for, and possible.
pub fn may_stream(lf: &LazyFrame, wanted: bool) -> bool {
    wanted && !in_plan(lf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(bytes: Vec<u8>, columns: Vec<ColumnLayout>, rows: usize) -> Arc<FixedRecords> {
        Arc::new(FixedRecords::new(vec![Arc::new(Bytes::Owned(bytes))], columns, rows).unwrap())
    }

    fn column(name: &str, start: usize, stride: usize, physical: Physical) -> ColumnLayout {
        ColumnLayout::new(
            name,
            start,
            stride,
            physical,
            physical.width().unwrap_or(stride),
        )
    }

    #[test]
    fn strided_columns_decode_in_both_byte_orders() {
        // Two records of (u16, i32): 1, -2 then 3, -4, little endian.
        let mut bytes = Vec::new();
        for (a, b) in [(1u16, -2i32), (3, -4)] {
            bytes.extend(a.to_le_bytes());
            bytes.extend(b.to_le_bytes());
        }
        let lf = records(
            bytes.clone(),
            vec![
                column("a", 0, 6, Physical::Unsigned(2)),
                column("b", 2, 6, Physical::Signed(4)),
            ],
            usize::MAX,
        )
        .into_lazy()
        .unwrap();
        let df = lf.collect().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.column("b").unwrap().i32().unwrap().to_vec(),
            [Some(-2), Some(-4)]
        );
        let mut big = column("a", 0, 6, Physical::Unsigned(2));
        big.big_endian = true;
        let df = records(bytes, vec![big], usize::MAX).collect(9).unwrap();
        assert_eq!(
            df.column("a").unwrap().u16().unwrap().to_vec(),
            [Some(256), Some(768)]
        );
    }

    #[test]
    fn odd_widths_sign_extend_and_sentinels_read_null() {
        // Two 3-byte signed samples, -2 and 8,388,607, then the s3 minimum.
        let bytes = vec![0xfe, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x80];
        let mut s3 = column("s", 0, 3, Physical::Signed(3));
        let df = records(bytes.clone(), vec![s3.clone()], usize::MAX)
            .collect(9)
            .unwrap();
        assert_eq!(
            df.column("s").unwrap().i32().unwrap().to_vec(),
            [Some(-2), Some(8_388_607), Some(-8_388_608)]
        );
        s3.null = Some(Null::Min);
        let df = records(bytes, vec![s3], usize::MAX).collect(9).unwrap();
        assert_eq!(df.column("s").unwrap().null_count(), 1);
        let mut u2 = column("u", 0, 2, Physical::Unsigned(2));
        u2.null = Some(Null::Max);
        let df = records(vec![0xff, 0xff, 1, 0], vec![u2], usize::MAX)
            .collect(9)
            .unwrap();
        assert_eq!(
            df.column("u").unwrap().u16().unwrap().to_vec(),
            [None, Some(1)]
        );
    }

    #[test]
    fn a_count_makes_an_array_and_factor_offset_a_float() {
        // Two records of three u1 channels.
        let mut channels = column("ch", 0, 3, Physical::Unsigned(1));
        channels.count = 3;
        channels.logical = Logical::Linear {
            factor: 0.5,
            offset: -1.0,
        };
        let df = records(vec![0, 2, 4, 6, 8, 10], vec![channels], usize::MAX)
            .collect(9)
            .unwrap();
        let ch = df.column("ch").unwrap();
        assert_eq!(ch.dtype(), &DataType::Array(Box::new(DataType::Float64), 3));
        assert_eq!(ch.get(1).unwrap().to_string(), "[2.0, 3.0, 4.0]");
    }

    #[test]
    fn dates_and_times_of_day() {
        let mut ymd = column("d", 0, 4, Physical::Unsigned(4));
        ymd.logical = Logical::Yyyymmdd;
        let mut bytes = 20240229u32.to_le_bytes().to_vec();
        bytes.extend(20241301u32.to_le_bytes());
        let df = records(bytes, vec![ymd], usize::MAX).collect(9).unwrap();
        let d = df.column("d").unwrap();
        assert_eq!(d.get(0).unwrap().to_string(), "2024-02-29");
        assert_eq!(d.null_count(), 1, "month 13 is no date");
        let mut tod = column("t", 0, 6, Physical::Unsigned(6));
        tod.big_endian = true;
        tod.logical = Logical::TimeOfDay {
            ns_per_unit: 1,
            date_ns: None,
        };
        let ns: u64 = 34_200_000_000_000; // 09:30
        let df = records(ns.to_be_bytes()[2..].to_vec(), vec![tod], usize::MAX)
            .collect(9)
            .unwrap();
        assert_eq!(
            df.column("t").unwrap().get(0).unwrap().to_string(),
            "09:30:00"
        );
    }

    #[test]
    fn projection_and_n_rows_reach_the_scan() {
        let bytes: Vec<u8> = (0u8..40).collect();
        let lf = records(
            bytes,
            vec![
                column("a", 0, 4, Physical::Unsigned(1)),
                column("b", 1, 4, Physical::Unsigned(1)),
            ],
            usize::MAX,
        )
        .into_lazy()
        .unwrap();
        let df = lf.clone().select([col("b")]).limit(3).collect().unwrap();
        assert_eq!(df.get_column_names(), ["b"]);
        assert_eq!(df.height(), 3);
        let count = lf.select([len()]).collect().unwrap();
        assert_eq!(
            count.column("len").unwrap().get(0).unwrap(),
            AnyValue::UInt32(10)
        );
    }

    #[test]
    fn a_window_starts_where_it_is_asked() {
        let bytes: Vec<u8> = (0u8..40).collect();
        let records = records(
            bytes,
            vec![column("a", 0, 4, Physical::Unsigned(1))],
            usize::MAX,
        );
        let df = records.window(8, 5).unwrap().collect().unwrap();
        assert_eq!(
            df.column("a").unwrap().u8().unwrap().to_vec(),
            [Some(32), Some(36)]
        );
        assert_eq!(
            records.window(99, 5).unwrap().collect().unwrap().height(),
            0
        );
    }

    #[test]
    fn the_scan_is_found_in_a_plan_and_kept_off_the_streaming_engine() {
        let lf = records(
            vec![0; 8],
            vec![column("a", 0, 1, Physical::Unsigned(1))],
            usize::MAX,
        )
        .into_lazy()
        .unwrap();
        let view = lf.filter(col("a").eq(lit(0u8))).select([col("a")]);
        assert!(in_plan(&view));
        assert!(!may_stream(&view, true));
        assert!(!in_plan(&df!("a" => [1]).unwrap().lazy()));
        // The in-memory engine runs a callback sink over it, as the copy and the
        // chart counts ask.
        let got = Arc::new(std::sync::Mutex::new(0usize));
        let seen = got.clone();
        let sink = view
            .sink_batches(
                PlanCallback::new(move |batch: DataFrame| {
                    *seen.lock().unwrap() += batch.height();
                    Ok(false)
                }),
                true,
                None,
            )
            .unwrap();
        crate::statistics::collect_lazy(sink, true).unwrap();
        assert_eq!(*got.lock().unwrap(), 8);
    }
}

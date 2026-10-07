//! Fixed-width values read straight out of a memory map. A column is values `width`
//! bytes wide, `stride` apart, from `start` into a source; `count` side by side make an
//! Array cell. Records (stride = record size) and one file per column (stride = width)
//! share one strided decoder for format specs and NumPy; [`decode`] is public for other
//! packed readers (audio, CAN). Sources are kept for raw-byte viewers. Frames decode
//! over a row index ([`crate::formats::row_index`]) on the streaming engine; an untouched view's
//! window reads via [`FixedRecords::window`] without an index.

use crate::formats::row_index::RowSource;
use polars::prelude::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Nanoseconds in a day.
const DAY_NS: i64 = 86_400_000_000_000;

/// The bytes a column reads.
pub enum Bytes {
    /// A file, mapped, and the file, to ask its length again.
    Mapped(memmap2::Mmap, std::fs::File),
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
        // by another process while it is mapped faults (SIGBUS) on access past its new
        // end, as it does for every reader that maps (Polars' own readers included).
        // Datui does not write to the files it reads, and `still_whole` refuses a read
        // once the file is shorter, which leaves only a truncation during a read.
        let map = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self::Mapped(map, file))
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mapped(map, _) => map,
            Self::Owned(bytes) => bytes,
        }
    }

    /// Fails when the mapped file is now shorter than its map, so reading the map
    /// would go past the file's end.
    pub fn still_whole(&self) -> PolarsResult<()> {
        if let Self::Mapped(map, file) = self {
            let len = file.metadata()?.len();
            polars_ensure!(
                len >= map.len() as u64,
                ComputeError: "the file is now {len} bytes, shorter than the {} it had when it was opened; open it again",
                map.len()
            );
        }
        Ok(())
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
    /// An IEEE float of 2 (half), 4 or 8 bytes.
    Float(u8),
    /// A bfloat16: the top half of an `f4`.
    BFloat16,
    /// One byte, nonzero for true.
    Bool,
    /// Text, its padding (NUL and spaces) trimmed from the right.
    Text,
    /// Text in ISO 8859-1, one byte a character, trimmed as `Text` is.
    Latin1,
    /// Text in UTF-16, two bytes a unit, trimmed as `Text` is.
    Utf16 { big_endian: bool },
    /// Text in UTF-32, four bytes a character, NUL characters trimmed from the right:
    /// NumPy's `U` strings.
    Utf32 { big_endian: bool },
    /// Raw bytes.
    Raw,
}

impl Physical {
    /// Bytes the type itself takes, or `None` for text and raw bytes.
    pub fn width(self) -> Option<usize> {
        match self {
            Self::Unsigned(n) | Self::Signed(n) | Self::Float(n) => Some(n as usize),
            Self::Bool => Some(1),
            Self::BFloat16 => Some(2),
            Self::Text | Self::Latin1 | Self::Utf16 { .. } | Self::Utf32 { .. } | Self::Raw => None,
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
            Self::Float(2 | 4) | Self::BFloat16 => DataType::Float32,
            Self::Float(_) => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Text | Self::Latin1 | Self::Utf16 { .. } | Self::Utf32 { .. } => DataType::String,
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
    /// An integer count of `multiplier` of a unit, as a duration in `unit`.
    Duration { unit: TimeUnit, multiplier: i64 },
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
    /// An index into a list of symbols, as a categorical; one past the list is null.
    Lookup(Arc<Vec<String>>),
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
            Logical::Duration { unit, .. } => DataType::Duration(*unit),
            Logical::Days { .. } | Logical::Yyyymmdd => DataType::Date,
            Logical::TimeOfDay { date_ns: None, .. } => DataType::Time,
            Logical::TimeOfDay {
                date_ns: Some(_), ..
            } => DataType::Datetime(TimeUnit::Nanoseconds, None),
            Logical::Decimal { scale } => DataType::Decimal(38, *scale),
            Logical::Linear { .. } => DataType::Float64,
            Logical::Enum(_) => DataType::String,
            Logical::Lookup(_) => DataType::from_categories(Categories::global()),
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

    /// Bytes one row's cell takes, `None` past `usize`.
    fn cell_width(&self) -> Option<usize> {
        self.width.checked_mul(self.count.max(1))
    }

    /// Rows this column can give from a source of `len` bytes.
    fn rows_in(&self, len: usize) -> usize {
        let Some(cell) = self.cell_width() else {
            return 0;
        };
        match len
            .checked_sub(self.start)
            .and_then(|room| room.checked_sub(cell))
        {
            Some(after_first) if self.stride > 0 => after_first / self.stride + 1,
            _ => 0,
        }
    }

    /// Check the layout reads what it says: widths that fit the type, sensible counts.
    pub fn validate(&self) -> PolarsResult<()> {
        polars_ensure!(
            self.stride > 0 && self.width > 0 && self.count > 0,
            ComputeError: "column {} has no width", self.name
        );
        polars_ensure!(
            self.cell_width().is_some(),
            ComputeError: "column {}: {} values of {} bytes is too many", self.name, self.count, self.width
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
                n == 2 || n == 4 || n == 8,
                ComputeError: "column {}: a float is 2, 4 or 8 bytes", self.name
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
/// `column.count` values each when it holds more than one. Asking for more rows than
/// `bytes` holds is an error.
pub fn decode(bytes: &[u8], column: &ColumnLayout, rows: usize) -> PolarsResult<Column> {
    column.validate()?;
    let fits = column.rows_in(bytes.len());
    polars_ensure!(
        rows <= fits,
        ComputeError: "column {}: {rows} rows asked for, {fits} in {} bytes", column.name, bytes.len()
    );
    decode_strided(bytes, column, rows, |row| row)
}

/// The values of `column` from `bytes` for the rows `index` names, in that order. A
/// missing row, or one `bytes` does not hold, is an error.
pub fn decode_rows(bytes: &[u8], column: &ColumnLayout, index: &IdxCa) -> PolarsResult<Column> {
    column.validate()?;
    let rows = crate::formats::row_index::checked(index, column.rows_in(bytes.len()))?;
    decode_strided(bytes, column, rows.len(), |i| rows[i] as usize)
}

/// The values of `column` from `bytes` for records starting at `records`, in order
/// (`column.start` into each; `stride` unused): for unevenly spaced records like log
/// messages found by an index. A cell past the end is an error.
pub fn decode_at(bytes: &[u8], column: &ColumnLayout, records: &[usize]) -> PolarsResult<Column> {
    // The stride is not used here, so a layout may leave it 0.
    ColumnLayout {
        stride: column.stride.max(1),
        ..column.clone()
    }
    .validate()?;
    let cell = column.cell_width().unwrap_or(usize::MAX);
    for &record in records {
        let end = record
            .checked_add(column.start)
            .and_then(|start| start.checked_add(cell));
        polars_ensure!(
            end.is_some_and(|end| end <= bytes.len()),
            OutOfBounds: "column {}: a record at {record} runs past the {} bytes on hand", column.name, bytes.len()
        );
    }
    let start = column.start;
    decode_cells(bytes, column, records.len(), |i| records[i] + start)
}

/// `rows` cells of `column`, the `i`th of them its row `row(i)`; every row is one
/// `bytes` holds.
fn decode_strided(
    bytes: &[u8],
    column: &ColumnLayout,
    rows: usize,
    row: impl Fn(usize) -> usize,
) -> PolarsResult<Column> {
    let (start, stride) = (column.start, column.stride);
    decode_cells(bytes, column, rows, move |i| start + row(i) * stride)
}

/// `rows` cells of `column`, the `i`th of them starting at byte `cell(i)`; every cell
/// is one `bytes` holds.
fn decode_cells(
    bytes: &[u8],
    column: &ColumnLayout,
    rows: usize,
    cell: impl Fn(usize) -> usize,
) -> PolarsResult<Column> {
    let count = column.count.max(1);
    let values = rows
        .checked_mul(count)
        .ok_or_else(|| polars_err!(ComputeError: "column {}: too many values", column.name))?;
    // Each value's bytes, row by row and in a row left to right.
    let at = move |i: usize| {
        let start = cell(i / count) + (i % count) * column.width;
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
    // A native width with a sentinel: compared at that width, not through `i128`.
    if column.logical == Logical::Plain
        && let Some(null) = column.null
    {
        let sentinel = null_integer(null, column.physical);
        macro_rules! or_null {
            ($ty:ty) => {{
                let sentinel = sentinel.and_then(|s| <$ty>::try_from(s).ok());
                let values = native!($ty).into_iter();
                Some(Series::new(
                    name.clone(),
                    values
                        .map(|v| (Some(v) != sentinel).then_some(v))
                        .collect::<Vec<Option<$ty>>>(),
                ))
            }};
        }
        let fast = match column.physical {
            Physical::Unsigned(1) => or_null!(u8),
            Physical::Unsigned(2) => or_null!(u16),
            Physical::Unsigned(4) => or_null!(u32),
            Physical::Unsigned(8) => or_null!(u64),
            Physical::Signed(1) => or_null!(i8),
            Physical::Signed(2) => or_null!(i16),
            Physical::Signed(4) => or_null!(i32),
            Physical::Signed(8) => or_null!(i64),
            _ => None,
        };
        if let Some(series) = fast {
            return Ok(series);
        }
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
            // Odd widths straight into the type they widen to: through `i128` and a
            // cast they took four times as long as a native width (#662).
            Physical::Unsigned(3) => Some(Series::new(
                name.clone(),
                (0..n)
                    .map(|i| read_unsigned(at(i), big) as u32)
                    .collect::<Vec<u32>>(),
            )),
            Physical::Unsigned(5..=7) => Some(Series::new(
                name.clone(),
                (0..n)
                    .map(|i| read_unsigned(at(i), big))
                    .collect::<Vec<u64>>(),
            )),
            Physical::Signed(3) => Some(Series::new(
                name.clone(),
                (0..n)
                    .map(|i| read_signed(at(i), big) as i32)
                    .collect::<Vec<i32>>(),
            )),
            Physical::Signed(5..=7) => Some(Series::new(
                name.clone(),
                (0..n)
                    .map(|i| read_signed(at(i), big))
                    .collect::<Vec<i64>>(),
            )),
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
        Physical::Float(_) | Physical::BFloat16 => {
            let physical = column.physical;
            let floats: Vec<Option<f64>> = (0..n)
                .map(|i| {
                    let raw = read_unsigned(at(i), big);
                    let v = match physical {
                        Physical::Float(2) => f64::from(half::f16::from_bits(raw as u16)),
                        Physical::BFloat16 => f64::from(half::bf16::from_bits(raw as u16)),
                        Physical::Float(4) => f64::from(f32::from_bits(raw as u32)),
                        _ => f64::from_bits(raw),
                    };
                    let null = match column.null {
                        Some(Null::NaN) => v.is_nan(),
                        Some(Null::Value(sentinel)) => v == sentinel as f64,
                        _ => false,
                    };
                    (!null).then_some(v)
                })
                .collect();
            let width = physical.width().unwrap_or(8) as u8;
            floats_of(column, width, floats)
        }
        Physical::Text => {
            let values: StringChunked = (0..n).map(|i| Some(text(at(i)))).collect();
            Ok(values.into_series())
        }
        Physical::Latin1 => {
            let values: StringChunked = (0..n).map(|i| Some(latin1(at(i)))).collect();
            Ok(values.into_series())
        }
        Physical::Utf16 { big_endian } => {
            let values: StringChunked = (0..n).map(|i| Some(utf16(at(i), big_endian))).collect();
            Ok(values.into_series())
        }
        Physical::Utf32 { big_endian } => {
            let values: StringChunked = (0..n).map(|i| Some(utf32(at(i), big_endian))).collect();
            Ok(values.into_series())
        }
        Physical::Raw => {
            let values: BinaryChunked = (0..n).map(|i| Some(at(i))).collect();
            Ok(values.into_series())
        }
    }
}

/// Integers as their column means them: plain at its width, or as its `logical` says
/// (a datetime, a duration, a factor and offset, an enum's labels). Public for readers
/// that find their integers some other way, such as bit fields of CAN signals.
pub fn integers(column: &ColumnLayout, ints: Vec<Option<i128>>) -> PolarsResult<Series> {
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
        Logical::Duration { unit, multiplier } => ints
            .into_iter()
            .map(|v| as_i64(v)?.checked_mul(*multiplier))
            .collect::<Int64Chunked>()
            .into_duration(*unit)
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
        Logical::Lookup(symbols) => ints
            .into_iter()
            .map(|v| {
                let i = usize::try_from(v?).ok()?;
                symbols.get(i).map(String::as_str)
            })
            .collect::<StringChunked>()
            .into_series()
            .cast(&DataType::from_categories(Categories::global()))?,
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
        _ if width <= 4 => floats
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

/// Text from a fixed-width ISO 8859-1 field, padding trimmed as [`text`] trims it.
pub fn latin1(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map_or(0, |i| i + 1);
    bytes[..end].iter().map(|&b| char::from(b)).collect()
}

/// Text from a fixed-width UTF-16 field, NUL and space units trimmed from the right.
/// An odd last byte and unpaired surrogates read as the replacement character.
pub fn utf16(bytes: &[u8], big_endian: bool) -> String {
    let mut units: Vec<u16> = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|&pair| {
            if big_endian {
                u16::from_be_bytes(pair)
            } else {
                u16::from_le_bytes(pair)
            }
        })
        .collect();
    while units.last().is_some_and(|&u| u == 0 || u == 0x20) {
        units.pop();
    }
    String::from_utf16_lossy(&units)
}

/// Text from a fixed-width UTF-32 field, NUL characters trimmed from the right as NumPy
/// trims them. A unit that is no character, and an odd last few bytes, read as the
/// replacement character.
pub fn utf32(bytes: &[u8], big_endian: bool) -> String {
    let (units, rest) = bytes.as_chunks::<4>();
    let mut chars: Vec<char> = units
        .iter()
        .map(|&unit| {
            let code = if big_endian {
                u32::from_be_bytes(unit)
            } else {
                u32::from_le_bytes(unit)
            };
            char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER)
        })
        .collect();
    while chars.last() == Some(&'\0') {
        chars.pop();
    }
    let mut text: String = chars.into_iter().collect();
    if !rest.is_empty() {
        text.push(char::REPLACEMENT_CHARACTER);
    }
    text
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
    /// The columns over `sources`, at most `rows` long (fewer when a source runs out, never
    /// reading past one, dropping a partial last record), from the sources' current lengths.
    /// A grown file rebuilds records over a fresh map with `usize::MAX` rows. At most
    /// [`crate::formats::row_index::MAX_ROWS`] are shown.
    pub fn new(
        sources: Vec<Arc<Bytes>>,
        columns: Vec<ColumnLayout>,
        rows: usize,
    ) -> PolarsResult<Self> {
        let mut rows = rows.min(crate::formats::row_index::MAX_ROWS);
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
        // The frame decodes a column by its place in the schema.
        polars_ensure!(
            schema.len() == columns.len(),
            Duplicate: "two columns have the same name"
        );
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

    /// The bytes the columns read, for a reader of the raw bytes.
    pub fn sources(&self) -> &[Arc<Bytes>] {
        &self.sources
    }

    /// The frame: decoded over a row index, only what a query reaches.
    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        crate::formats::row_index::lazy(self)
    }

    /// Rows `[start, start + len)`, decoded now: the columns start further into the
    /// same sources, so nothing before `start` is decoded and no index is built.
    pub fn window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
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
        Self::new(self.sources.clone(), columns, len)?.collect(len)
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
        let source = &self.sources[column.source];
        source.still_whole()?;
        decode(source.as_slice(), column, rows)
    }
}

impl RowSource for FixedRecords {
    fn height(&self) -> usize {
        self.rows
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        let column = &self.columns[column];
        let source = &self.sources[column.source];
        source.still_whole()?;
        decode_rows(source.as_slice(), column, index)
    }
}

impl crate::formats::pushdown::Windowed for FixedRecords {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(FixedRecords::window(self, start, len)?.lazy())
    }
}

#[cfg(test)]
mod tests;

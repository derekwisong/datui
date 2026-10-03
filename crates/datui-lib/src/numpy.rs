//! NumPy arrays: `.npy` files, and `.npz` archives of them.
//!
//! An `.npy` file is a short header, a Python dict literal giving the element type
//! (`descr`), the order (`fortran_order`) and the `shape`, and then the elements packed
//! end to end. Every element is the same size, so the data is read where it is, through
//! the strided decoder of the binary format specs ([`crate::fixed_records`]): a map of
//! the file, decoded only where it is shown.
//!
//! A structured element type gives a column per field (nested fields as `outer.inner`,
//! a subarray field as an Array column); a 1-D array is one column, named for the
//! array; a 2-D array is a grid with a column per index, `0` to `n-1`. More dimensions
//! are refused with the shape. Python objects (`O`), which only unpickling could read,
//! are refused and never unpickled.
//!
//! An `.npz` archive is a zip of `.npy` files. Its arrays are listed on the home screen
//! like a directory's files; an archive of one opens it. An array stored as it is in the
//! archive (`np.savez`) is read in place from a map of the archive; a compressed one
//! (`np.savez_compressed`) is decompressed once to a temporary file first.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::*;

use crate::fixed_records::{Bytes, ColumnLayout, FixedRecords, Logical, Null, Physical};
use crate::model_files::MetaValue;
use crate::sqlite::Table;
use crate::text_formats::Detail;

/// The first six bytes of every `.npy` file.
pub const MAGIC: &[u8; 6] = b"\x93NUMPY";

/// The longest header read. NumPy's own reader refuses past 10,000 bytes unless told
/// otherwise; a structured type of many fields can be longer, and this is still small.
pub const MAX_HEADER: usize = 4 << 20;

/// Nesting of the header's literal, and of structured types inside structured types.
const MAX_DEPTH: usize = 32;

/// Columns one array may make, nested fields and repeated structs flattened.
pub const MAX_COLUMNS: usize = 4096;

/// A 2-D array of more columns than this is one Array column rather than a column per
/// index.
pub const MAX_GRID: usize = 1024;

/// Arrays of an archive listed or read.
const MAX_MEMBERS: usize = 100_000;

/// Whether `head`, the first bytes of a file, begins a `.npy` file.
pub fn looks_like(head: &[u8]) -> bool {
    head.starts_with(MAGIC)
}

/// Whether a file is a NumPy archive: a zip file named `.npz`.
pub fn is_archive(path: &Path, head: &[u8]) -> bool {
    is_archive_name(path) && (head.starts_with(b"PK\x03\x04") || head.starts_with(b"PK\x05\x06"))
}

fn is_archive_name(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("npz"))
}

// --- The header's Python literal -------------------------------------------------

/// A Python literal, as far as a `.npy` header uses them.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Int(i128),
    Bool(bool),
    None,
    List(Vec<Literal>),
    Tuple(Vec<Literal>),
    Dict(Vec<(Literal, Literal)>),
}

impl Literal {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    fn items(&self) -> Option<&[Literal]> {
        match self {
            Self::List(items) | Self::Tuple(items) => Some(items),
            _ => None,
        }
    }

    fn get(&self, key: &str) -> Option<&Literal> {
        match self {
            Self::Dict(pairs) => pairs
                .iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .map(|(_, v)| v),
            _ => None,
        }
    }
}

/// Parse the Python literal `text`: strings, integers, `True`, `False`, `None`, lists,
/// tuples and dicts, nested at most [`MAX_DEPTH`] deep.
pub fn parse_literal(text: &str) -> std::result::Result<Literal, String> {
    let mut parser = Parser {
        text: text.as_bytes(),
        at: 0,
    };
    let value = parser.value(0)?;
    parser.space();
    if parser.at != parser.text.len() {
        return Err(format!("unexpected text at byte {}", parser.at));
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn space(&mut self) {
        while self
            .text
            .get(self.at)
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            self.at += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.space();
        self.text.get(self.at).copied()
    }

    fn value(&mut self, depth: usize) -> std::result::Result<Literal, String> {
        if depth > MAX_DEPTH {
            return Err("nested too deep".into());
        }
        match self.peek() {
            Some(b'\'' | b'"') => self.string().map(Literal::Str),
            Some(b'[') => self
                .sequence(b']', depth)
                .map(|(items, _)| Literal::List(items)),
            Some(b'(') => {
                let (items, comma) = self.sequence(b')', depth)?;
                // `(x)` is x; `(x,)` is a tuple of one.
                Ok(match (items.len(), comma) {
                    (1, false) => items.into_iter().next().expect("one item"),
                    _ => Literal::Tuple(items),
                })
            }
            Some(b'{') => self.dict(depth),
            Some(b'-' | b'+' | b'0'..=b'9') => self.int(),
            Some(_) => self.word(),
            None => Err("the header ends early".into()),
        }
    }

    fn string(&mut self) -> std::result::Result<String, String> {
        let quote = self.text[self.at];
        self.at += 1;
        let mut out = Vec::new();
        loop {
            let Some(&b) = self.text.get(self.at) else {
                return Err("a string does not end".into());
            };
            self.at += 1;
            match b {
                b if b == quote => break,
                b'\\' => {
                    let Some(&e) = self.text.get(self.at) else {
                        return Err("a string does not end".into());
                    };
                    self.at += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'0' => out.push(0),
                        b'x' => {
                            let hex = self
                                .text
                                .get(self.at..self.at + 2)
                                .and_then(|h| std::str::from_utf8(h).ok())
                                .and_then(|h| u8::from_str_radix(h, 16).ok())
                                .ok_or("a bad \\x escape")?;
                            self.at += 2;
                            out.push(hex);
                        }
                        other => out.push(other),
                    }
                }
                b => out.push(b),
            }
        }
        Ok(String::from_utf8_lossy(&out).into_owned())
    }

    fn sequence(
        &mut self,
        close: u8,
        depth: usize,
    ) -> std::result::Result<(Vec<Literal>, bool), String> {
        self.at += 1;
        let mut items = Vec::new();
        let mut comma = false;
        loop {
            if self.peek() == Some(close) {
                self.at += 1;
                return Ok((items, comma));
            }
            items.push(self.value(depth + 1)?);
            comma = false;
            match self.peek() {
                Some(b',') => {
                    self.at += 1;
                    comma = true;
                }
                Some(b) if b == close => {}
                _ => {
                    return Err(format!(
                        "expected , or {} at byte {}",
                        close as char, self.at
                    ));
                }
            }
        }
    }

    fn dict(&mut self, depth: usize) -> std::result::Result<Literal, String> {
        self.at += 1;
        let mut pairs = Vec::new();
        loop {
            if self.peek() == Some(b'}') {
                self.at += 1;
                return Ok(Literal::Dict(pairs));
            }
            let key = self.value(depth + 1)?;
            if self.peek() != Some(b':') {
                return Err(format!("expected : at byte {}", self.at));
            }
            self.at += 1;
            let value = self.value(depth + 1)?;
            pairs.push((key, value));
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {}
                _ => return Err(format!("expected , or }} at byte {}", self.at)),
            }
        }
    }

    fn int(&mut self) -> std::result::Result<Literal, String> {
        let start = self.at;
        if matches!(self.text.get(self.at), Some(b'-' | b'+')) {
            self.at += 1;
        }
        while self.text.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
        let digits = std::str::from_utf8(&self.text[start..self.at]).unwrap_or_default();
        // Python 2 wrote long integers with an `L`: `(3L, 4L)`.
        if matches!(self.text.get(self.at), Some(b'L' | b'l')) {
            self.at += 1;
        }
        digits
            .parse::<i128>()
            .map(Literal::Int)
            .map_err(|_| format!("a bad number at byte {start}"))
    }

    fn word(&mut self) -> std::result::Result<Literal, String> {
        let start = self.at;
        while self.text.get(self.at).is_some_and(u8::is_ascii_alphabetic) {
            self.at += 1;
        }
        match &self.text[start..self.at] {
            b"True" => Ok(Literal::Bool(true)),
            b"False" => Ok(Literal::Bool(false)),
            b"None" => Ok(Literal::None),
            _ => Err(format!("unexpected text at byte {start}")),
        }
    }
}

// --- Element types -----------------------------------------------------------------

/// An element type, as `descr` gives it.
#[derive(Debug, Clone, PartialEq)]
pub enum Dtype {
    Simple(Simple),
    /// Fields at offsets in an element of `itemsize` bytes.
    Struct {
        fields: Vec<FieldDef>,
        itemsize: usize,
    },
}

/// One field of a structured type.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub dtype: Dtype,
    /// A subarray field's shape; empty for one value.
    pub shape: Vec<usize>,
    pub offset: usize,
}

/// A type with no fields: `<f8`, `|S10`, `<M8[ns]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Simple {
    /// The type string as written.
    pub text: String,
    pub kind: u8,
    /// Bytes an element takes.
    pub size: usize,
    pub big: bool,
    /// A datetime's or timedelta's unit and how many of it one step is.
    pub unit: Option<(String, i64)>,
}

impl Dtype {
    /// Bytes one element takes.
    pub fn itemsize(&self) -> usize {
        match self {
            Self::Simple(s) => s.size,
            Self::Struct { itemsize, .. } => *itemsize,
        }
    }

    /// The type as NumPy writes it, for the Info panel.
    pub fn describe(&self) -> String {
        match self {
            Self::Simple(s) => s.text.clone(),
            Self::Struct { fields, itemsize } => {
                format!("struct of {} fields, {itemsize} bytes", fields.len())
            }
        }
    }
}

/// Parse a `descr`.
pub fn parse_dtype(descr: &Literal) -> std::result::Result<Dtype, String> {
    dtype_of(descr, 0)
}

fn dtype_of(descr: &Literal, depth: usize) -> std::result::Result<Dtype, String> {
    if depth > MAX_DEPTH {
        return Err("a structured type nested too deep".into());
    }
    match descr {
        Literal::Str(text) => simple(text).map(Dtype::Simple),
        Literal::List(items) => {
            // Fields one after another; a field with no name is padding.
            let mut fields = Vec::new();
            let mut offset = 0usize;
            for item in items {
                let parts = item.items().ok_or("a field is not a tuple")?;
                let (name, field_descr, shape) = match parts {
                    [name, descr] => (name, descr, None),
                    [name, descr, shape] => (name, descr, Some(shape)),
                    _ => return Err("a field is not (name, type[, shape])".into()),
                };
                let name = field_name(name)?;
                let dtype = dtype_of(field_descr, depth + 1)?;
                let shape = shape.map(shape_of).transpose()?.unwrap_or_default();
                let count = count_of(&shape)?;
                let size = dtype
                    .itemsize()
                    .checked_mul(count)
                    .ok_or("a field is too large")?;
                if !name.is_empty() {
                    fields.push(FieldDef {
                        name,
                        dtype,
                        shape,
                        offset,
                    });
                }
                offset = offset.checked_add(size).ok_or("a type is too large")?;
            }
            Ok(Dtype::Struct {
                fields,
                itemsize: offset,
            })
        }
        Literal::Dict(_) => {
            // The dict form: names, formats, offsets and the element's size.
            let list = |key: &str| {
                descr
                    .get(key)
                    .and_then(Literal::items)
                    .ok_or_else(|| format!("a structured type without {key}"))
            };
            let names = list("names")?;
            let formats = list("formats")?;
            if names.len() != formats.len() {
                return Err("names and formats differ in length".into());
            }
            let offsets: Vec<usize> = match descr.get("offsets") {
                Some(offsets) => offsets
                    .items()
                    .ok_or("offsets is not a list")?
                    .iter()
                    .map(|o| match o {
                        Literal::Int(n) => usize::try_from(*n).map_err(|_| "a bad offset".into()),
                        _ => Err("a bad offset".to_string()),
                    })
                    .collect::<std::result::Result<_, _>>()?,
                None => Vec::new(),
            };
            let mut fields = Vec::new();
            let mut next = 0usize;
            for (i, (name, format)) in names.iter().zip(formats).enumerate() {
                let name = field_name(name)?;
                let (dtype, shape) = match format.items() {
                    // `('<f8', (3,))`: a subarray in the dict form.
                    Some([inner, shape]) => (dtype_of(inner, depth + 1)?, shape_of(shape)?),
                    _ => (dtype_of(format, depth + 1)?, Vec::new()),
                };
                let size = dtype
                    .itemsize()
                    .checked_mul(count_of(&shape)?)
                    .ok_or("a field is too large")?;
                let offset = offsets.get(i).copied().unwrap_or(next);
                next = offset.checked_add(size).ok_or("a type is too large")?;
                if !name.is_empty() {
                    fields.push(FieldDef {
                        name,
                        dtype,
                        shape,
                        offset,
                    });
                }
            }
            let end = fields
                .iter()
                .map(|f| f.offset + f.dtype.itemsize() * count_of(&f.shape).unwrap_or(1))
                .max()
                .unwrap_or(0);
            let itemsize = match descr.get("itemsize") {
                Some(Literal::Int(n)) => usize::try_from(*n).map_err(|_| "a bad itemsize")?,
                _ => end,
            };
            if itemsize < end {
                return Err(format!(
                    "a field ends at byte {end}, past the element's {itemsize}"
                ));
            }
            Ok(Dtype::Struct { fields, itemsize })
        }
        _ => Err("descr is not a type".into()),
    }
}

/// A field's name: a string, or `(title, name)`.
fn field_name(name: &Literal) -> std::result::Result<String, String> {
    match name {
        Literal::Str(s) => Ok(s.clone()),
        Literal::Tuple(pair) if pair.len() == 2 => pair[1]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| "a field's name is not text".into()),
        _ => Err("a field's name is not text".into()),
    }
}

fn shape_of(shape: &Literal) -> std::result::Result<Vec<usize>, String> {
    match shape {
        Literal::Int(n) => Ok(vec![usize::try_from(*n).map_err(|_| "a bad dimension")?]),
        Literal::Tuple(items) | Literal::List(items) => items
            .iter()
            .map(|d| match d {
                Literal::Int(n) => usize::try_from(*n).map_err(|_| "a bad dimension".into()),
                _ => Err("a dimension is not a number".to_string()),
            })
            .collect(),
        _ => Err("a shape is not a tuple".into()),
    }
}

fn count_of(shape: &[usize]) -> std::result::Result<usize, String> {
    shape
        .iter()
        .try_fold(1usize, |n, d| n.checked_mul(*d))
        .ok_or_else(|| "a shape is too large".into())
}

/// Parse a type string such as `<f8`, `|S10`, `>i4` or `<M8[ns]`.
fn simple(text: &str) -> std::result::Result<Simple, String> {
    let bytes = text.as_bytes();
    let (order, rest) = match bytes.first() {
        Some(b @ (b'<' | b'>' | b'|' | b'=' | b'!')) => (*b, &text[1..]),
        _ => (b'=', text),
    };
    let big = match order {
        b'>' | b'!' => true,
        b'=' => cfg!(target_endian = "big"),
        _ => false,
    };
    let mut chars = rest.chars();
    let kind = chars.next().ok_or("an empty type")?;
    if !kind.is_ascii() {
        return Err(format!("the type {text} is not one NumPy writes"));
    }
    let rest = chars.as_str();
    let (digits, unit) = match rest.split_once('[') {
        Some((digits, unit)) => (
            digits,
            Some(
                unit.strip_suffix(']')
                    .ok_or_else(|| format!("the type {text} has no ]"))?,
            ),
        ),
        None => (rest, None),
    };
    let n: usize = if digits.is_empty() {
        0
    } else {
        digits
            .parse()
            .map_err(|_| format!("the type {text} is not one NumPy writes"))?
    };
    let kind = kind as u8;
    let size = match kind {
        b'?' => 1,
        b'U' => n.checked_mul(4).ok_or("a string type is too long")?,
        b'O' => 8,
        _ => n,
    };
    let unit = match unit {
        Some(unit) => {
            let split = unit
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(unit.len());
            let multiplier = match &unit[..split] {
                "" => 1,
                m => m.parse().map_err(|_| format!("the unit of {text}"))?,
            };
            Some((unit[split..].to_string(), multiplier))
        }
        None => None,
    };
    Ok(Simple {
        text: text.to_string(),
        kind,
        size,
        big,
        unit,
    })
}

/// How one simple type is read: what it is stored as and what it means, and how many
/// values one element holds (two for a complex number).
fn read_as(s: &Simple) -> std::result::Result<(Physical, usize, Logical, Option<Null>), String> {
    let plain = Logical::Plain;
    let unsupported = || format!("the type {} is not read", s.text);
    Ok(match (s.kind, s.size) {
        (b'b' | b'?', 1) => (Physical::Bool, 1, plain, None),
        (b'i', 1 | 2 | 4 | 8) => (Physical::Signed(s.size as u8), 1, plain, None),
        (b'u', 1 | 2 | 4 | 8) => (Physical::Unsigned(s.size as u8), 1, plain, None),
        (b'f', 2 | 4 | 8) => (Physical::Float(s.size as u8), 1, plain, None),
        // Real and imaginary parts, side by side.
        (b'c', 8 | 16) => (Physical::Float((s.size / 2) as u8), 2, plain, None),
        (b'S' | b'a', 1..) => (Physical::Text, 1, plain, None),
        (b'U', 4..) => (Physical::Utf32 { big_endian: s.big }, 1, plain, None),
        (b'V', 1..) => (Physical::Raw, 1, plain, None),
        (b'O', _) => {
            return Err(
                "it holds Python objects (dtype O), which only unpickling could read; datui does not unpickle"
                    .into(),
            );
        }
        (b'M', 8) => {
            let logical = match time_unit(s.unit.as_ref()) {
                Some(Step::Days(1)) => Logical::Days { epoch_days: 0 },
                Some(Step::Days(n)) => Logical::Timestamp {
                    unit: TimeUnit::Milliseconds,
                    multiplier: n.checked_mul(86_400_000).ok_or_else(unsupported)?,
                    epoch: 0,
                },
                Some(Step::Unit(unit, multiplier)) => Logical::Timestamp {
                    unit,
                    multiplier,
                    epoch: 0,
                },
                // Months and years have no fixed length, and finer than nanoseconds has
                // no Polars type: their counts, as stored.
                None => plain,
            };
            (Physical::Signed(8), 1, logical, Some(Null::Min))
        }
        (b'm', 8) => {
            let logical = match time_unit(s.unit.as_ref()) {
                Some(Step::Days(n)) => Logical::Duration {
                    unit: TimeUnit::Milliseconds,
                    multiplier: n.checked_mul(86_400_000).ok_or_else(unsupported)?,
                },
                Some(Step::Unit(unit, multiplier)) => Logical::Duration { unit, multiplier },
                None => plain,
            };
            (Physical::Signed(8), 1, logical, Some(Null::Min))
        }
        _ => return Err(unsupported()),
    })
}

enum Step {
    /// A Polars unit, and how many of it one step is.
    Unit(TimeUnit, i64),
    /// Days, this many to a step.
    Days(i64),
}

/// The step of a datetime or timedelta unit, when Polars has a type for it.
fn time_unit(unit: Option<&(String, i64)>) -> Option<Step> {
    let (name, n) = unit?;
    let n = *n;
    let ms = |per: i64| Some(Step::Unit(TimeUnit::Milliseconds, per.checked_mul(n)?));
    match name.as_str() {
        "ns" => Some(Step::Unit(TimeUnit::Nanoseconds, n)),
        "us" | "\u{b5}s" => Some(Step::Unit(TimeUnit::Microseconds, n)),
        "ms" => Some(Step::Unit(TimeUnit::Milliseconds, n)),
        "s" => ms(1000),
        "m" => ms(60_000),
        "h" => ms(3_600_000),
        "D" => Some(Step::Days(n)),
        "W" => Some(Step::Days(n.checked_mul(7)?)),
        _ => None,
    }
}

// --- The header ---------------------------------------------------------------------

/// What a `.npy` header says.
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub version: (u8, u8),
    pub dtype: Dtype,
    pub fortran: bool,
    pub shape: Vec<usize>,
    /// Bytes from the start of the file to the first element.
    pub data_offset: usize,
}

/// Bytes before the header's text, and the header's length, from the file's first 12
/// bytes; `None` when they are not a `.npy` file's.
pub fn header_len(head: &[u8]) -> std::result::Result<(usize, usize), String> {
    if !looks_like(head) {
        return Err("not a NumPy .npy file: no \\x93NUMPY at the start".into());
    }
    let major = *head.get(6).ok_or("the header is cut short")?;
    let (prefix, len) = match major {
        1 => (
            10,
            u16::from_le_bytes(
                head.get(8..10)
                    .ok_or("the header is cut short")?
                    .try_into()
                    .expect("two bytes"),
            ) as usize,
        ),
        2 | 3 => (
            12,
            u32::from_le_bytes(
                head.get(8..12)
                    .ok_or("the header is cut short")?
                    .try_into()
                    .expect("four bytes"),
            ) as usize,
        ),
        v => {
            return Err(format!(
                "format version {v} is not one datui reads (1, 2 or 3)"
            ));
        }
    };
    if len > MAX_HEADER {
        return Err(format!(
            "its header is {len} bytes, more than the {} MiB datui reads",
            MAX_HEADER >> 20
        ));
    }
    Ok((prefix, len))
}

/// Parse the header at the start of `bytes`, which must hold all of it.
pub fn parse_header(bytes: &[u8]) -> std::result::Result<Header, String> {
    let (prefix, len) = header_len(bytes)?;
    let text = bytes
        .get(prefix..prefix + len)
        .ok_or("the header is cut short")?;
    // Version 3 headers are UTF-8, earlier ones Latin-1; the keys and types are ASCII.
    let text: String = match bytes[6] {
        3 => String::from_utf8_lossy(text).into_owned(),
        _ => text.iter().map(|&b| char::from(b)).collect(),
    };
    let dict = parse_literal(text.trim_end_matches(['\n', ' ', '\0']))
        .map_err(|e| format!("its header is not a Python dict: {e}"))?;
    if !matches!(dict, Literal::Dict(_)) {
        return Err("its header is not a Python dict".into());
    }
    let descr = dict.get("descr").ok_or("its header has no descr")?;
    let dtype = parse_dtype(descr)?;
    let fortran = match dict.get("fortran_order") {
        Some(Literal::Bool(b)) => *b,
        None => false,
        _ => return Err("fortran_order is not True or False".into()),
    };
    let shape = shape_of(dict.get("shape").ok_or("its header has no shape")?)?;
    count_of(&shape)?;
    Ok(Header {
        version: (bytes[6], bytes[7]),
        dtype,
        fortran,
        shape,
        data_offset: prefix + len,
    })
}

/// The shape as Python writes it: `(3,)`, `(2, 4)`, `()`.
pub fn shape_text(shape: &[usize]) -> String {
    match shape {
        [one] => format!("({one},)"),
        _ => format!(
            "({})",
            shape
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

// --- Columns ------------------------------------------------------------------------

/// The columns an array is read as, their starts counted from its first element, and
/// its rows.
#[derive(Debug)]
pub struct Plan {
    pub columns: Vec<ColumnLayout>,
    pub rows: usize,
    /// Bytes the elements take, as the shape says.
    pub bytes: u128,
}

/// The columns of an array of `header`, named `name` where it is one column.
pub fn plan(header: &Header, name: &str) -> std::result::Result<Plan, String> {
    let itemsize = header.dtype.itemsize();
    if itemsize == 0 {
        return Err("its elements take no bytes".into());
    }
    let elements = count_of(&header.shape)?;
    let bytes = elements as u128 * itemsize as u128;
    let refuse_shape = || {
        format!(
            "it has {} dimensions, shape {}; datui reads 1-D and 2-D arrays",
            header.shape.len(),
            shape_text(&header.shape)
        )
    };
    match (&header.dtype, header.shape.as_slice()) {
        (_, [] | [_]) => {
            let mut columns = Vec::new();
            // A structured array's fields are its columns, named as they are.
            let name = match header.dtype {
                Dtype::Struct { .. } => "",
                Dtype::Simple(_) => name,
            };
            leaves(&header.dtype, name, 0, itemsize, &mut columns, 0)?;
            Ok(Plan {
                columns,
                rows: header.shape.first().copied().unwrap_or(1),
                bytes,
            })
        }
        (Dtype::Simple(_), [rows, cols]) => {
            let (rows, cols) = (*rows, *cols);
            let mut columns = Vec::new();
            if cols > MAX_GRID {
                if header.fortran {
                    return Err(format!(
                        "it is {} in Fortran order, with more than {MAX_GRID} columns",
                        shape_text(&header.shape)
                    ));
                }
                let mut leaf = Vec::new();
                leaves(&header.dtype, "values", 0, itemsize, &mut leaf, 0)?;
                let mut column = leaf.remove(0);
                column.count = column.count.checked_mul(cols).ok_or("a row is too large")?;
                column.stride = itemsize.checked_mul(cols).ok_or("a row is too large")?;
                columns.push(column);
            } else {
                for c in 0..cols {
                    let mut leaf = Vec::new();
                    leaves(&header.dtype, &c.to_string(), 0, itemsize, &mut leaf, 0)?;
                    let mut column = leaf.remove(0);
                    if header.fortran {
                        // Column-major: a column's elements are side by side.
                        column.start = c * rows * itemsize;
                        column.stride = itemsize;
                    } else {
                        column.start = c * itemsize;
                        column.stride = itemsize * cols;
                    }
                    columns.push(column);
                }
            }
            Ok(Plan {
                columns,
                rows,
                bytes,
            })
        }
        (Dtype::Struct { .. }, [_, _]) => Err(format!(
            "it is a structured array of shape {}; datui reads structured arrays of one dimension",
            shape_text(&header.shape)
        )),
        _ => Err(refuse_shape()),
    }
}

/// The columns of an element of `dtype` at `offset` into a record of `stride` bytes,
/// named under `name`.
fn leaves(
    dtype: &Dtype,
    name: &str,
    offset: usize,
    stride: usize,
    out: &mut Vec<ColumnLayout>,
    depth: usize,
) -> std::result::Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("a structured type nested too deep".into());
    }
    match dtype {
        Dtype::Simple(s) => {
            let (physical, values, logical, null) =
                read_as(s).map_err(|e| format!("{name}: {e}"))?;
            let width = s.size / values;
            out.push(ColumnLayout {
                name: name.into(),
                source: 0,
                start: offset,
                stride,
                width,
                count: values,
                physical,
                big_endian: s.big,
                null,
                logical,
            });
        }
        Dtype::Struct { fields, .. } => {
            for field in fields {
                let full = match name {
                    "" => field.name.clone(),
                    _ => format!("{name}.{}", field.name),
                };
                let count = count_of(&field.shape)?;
                let at = offset
                    .checked_add(field.offset)
                    .ok_or("a field is too far in")?;
                match &field.dtype {
                    Dtype::Simple(_) => {
                        leaves(&field.dtype, &full, at, stride, out, depth + 1)?;
                        if count != 1
                            && let Some(column) = out.last_mut()
                        {
                            // A subarray field: its values in one Array cell.
                            column.count = column
                                .count
                                .checked_mul(count)
                                .ok_or("a subarray is too large")?;
                        }
                    }
                    Dtype::Struct { .. } if count == 1 => {
                        leaves(&field.dtype, &full, at, stride, out, depth + 1)?;
                    }
                    Dtype::Struct { itemsize, .. } => {
                        for k in 0..count {
                            let at = k
                                .checked_mul(*itemsize)
                                .and_then(|o| at.checked_add(o))
                                .ok_or("a field is too far in")?;
                            leaves(
                                &field.dtype,
                                &format!("{full}[{k}]"),
                                at,
                                stride,
                                out,
                                depth + 1,
                            )?;
                            if out.len() > MAX_COLUMNS {
                                break;
                            }
                        }
                    }
                }
                if out.len() > MAX_COLUMNS {
                    return Err(format!("its fields make more than {MAX_COLUMNS} columns"));
                }
            }
        }
    }
    Ok(())
}

// --- Reading ------------------------------------------------------------------------

/// An array read in place: its records and what to say about it.
pub struct Array {
    pub records: Arc<FixedRecords>,
    pub header: Header,
    pub notes: Vec<String>,
}

/// The array whose `.npy` bytes start `at` bytes into `bytes` (the whole of a file, or
/// a member stored in an archive) and run for `len` bytes, its one column named `name`.
pub fn open_in(
    bytes: Arc<Bytes>,
    at: usize,
    len: usize,
    name: &str,
) -> std::result::Result<Array, String> {
    let end = at.checked_add(len).filter(|&end| end <= bytes.len());
    let Some(end) = end else {
        return Err("the array runs past the end of the file".into());
    };
    let npy = &bytes.as_slice()[at..end];
    let (prefix, header_bytes) = header_len(npy)?;
    if npy.len() < prefix + header_bytes {
        return Err("the header is cut short".into());
    }
    let header = parse_header(npy)?;
    let plan = plan(&header, name)?;
    let data = header.data_offset;
    let on_hand = (npy.len() - data) as u128;
    let mut notes = Vec::new();
    if on_hand < plan.bytes {
        notes.push(format!(
            "The file holds {} of the {} bytes its shape {} says; the rows past the end are left out.",
            crate::numfmt::group_chrome(on_hand as usize),
            crate::numfmt::group_chrome(usize::try_from(plan.bytes).unwrap_or(usize::MAX)),
            shape_text(&header.shape)
        ));
    }
    if plan.rows > crate::row_index::MAX_ROWS {
        notes.push(format!(
            "The array has {} rows; the first {} are shown.",
            crate::numfmt::group_chrome(plan.rows),
            crate::numfmt::group_chrome(crate::row_index::MAX_ROWS)
        ));
    }
    // A cell past what is on hand would be read past the array's end in an archive,
    // into the next member: the rows are cut to the array's own bytes.
    let rows_on_hand = |c: &ColumnLayout| -> usize {
        let cell = c.width * c.count;
        let room = (end - at).saturating_sub(data + c.start);
        if room < cell || c.stride == 0 {
            0
        } else {
            (room - cell) / c.stride + 1
        }
    };
    let rows = plan
        .columns
        .iter()
        .map(rows_on_hand)
        .min()
        .unwrap_or(plan.rows)
        .min(plan.rows);
    let columns = plan
        .columns
        .into_iter()
        .map(|c| ColumnLayout {
            start: c.start + at + data,
            ..c
        })
        .collect();
    let records = FixedRecords::new(vec![bytes], columns, rows).map_err(|e| e.to_string())?;
    Ok(Array {
        records: Arc::new(records),
        header,
        notes,
    })
}

/// The array of the `.npy` file at `path`.
pub fn open_file(path: &Path) -> Result<Array> {
    let bytes = Bytes::map(path).map_err(|e| eyre!("{}: {e}", path.display()))?;
    let len = bytes.len();
    open_in(Arc::new(bytes), 0, len, &stem(path))
        .map_err(|e| eyre!("{} is not read: {e}", path.display()))
}

fn stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "values".to_string())
}

/// What the Info panel's NumPy tab says of an array.
pub fn detail(header: &Header, archive: Option<(&Path, usize)>, compressed: bool) -> Detail {
    let mut lines = vec![
        format!("Shape: {}", shape_text(&header.shape)),
        format!("Type: {}", header.dtype.describe()),
        format!(
            "Order: {}",
            if header.fortran {
                "Fortran (column-major)"
            } else {
                "C (row-major)"
            }
        ),
        format!("Version: {}.{}", header.version.0, header.version.1),
    ];
    if let Some((archive, arrays)) = archive {
        lines.push(format!(
            "Archive: {}, {}{}",
            archive.display(),
            crate::text_formats::count(arrays as u64, "array", "arrays"),
            if compressed {
                "; this one compressed, read from a copy"
            } else {
                ""
            }
        ));
    }
    let mut list = Vec::new();
    if let Dtype::Struct { fields, .. } = &header.dtype {
        let rows = fields.iter().map(|f| {
            let mut text = f.dtype.describe();
            if !f.shape.is_empty() {
                text.push_str(&format!(" {}", shape_text(&f.shape)));
            }
            text.push_str(&format!(" at byte {}", f.offset));
            (f.name.clone(), MetaValue::Text(text))
        });
        list = crate::text_formats::capped_list(rows, fields.len());
    }
    Detail {
        tab: "NumPy",
        lines,
        list_title: "Fields",
        list,
        first: false,
    }
}

// --- Archives -----------------------------------------------------------------------

/// One array of an archive.
#[derive(Debug, Clone)]
pub struct Member {
    /// The name `np.load` gives it: the file name in the archive, less `.npy`.
    pub name: String,
    /// The entry's index in the archive.
    pub index: usize,
    /// Stored as it is, at this offset in the archive; `None` when compressed.
    pub stored_at: Option<u64>,
    pub size: u64,
}

fn archive_of(path: &Path) -> Result<::zip::ZipArchive<std::fs::File>> {
    let file = std::fs::File::open(path).map_err(|e| eyre!("{}: {e}", path.display()))?;
    ::zip::ZipArchive::new(file)
        .map_err(|e| eyre!("{} is not a NumPy archive: {e}", path.display()))
}

/// The arrays of the archive at `path`, in the archive's order.
pub fn members(path: &Path) -> Result<Vec<Member>> {
    let mut archive = archive_of(path)?;
    let mut members = Vec::new();
    for index in 0..archive.len().min(MAX_MEMBERS) {
        let Ok(entry) = archive.by_index_raw(index) else {
            continue;
        };
        let Some(name) = entry.name().strip_suffix(".npy") else {
            continue;
        };
        if entry.is_dir() || name.is_empty() {
            continue;
        }
        let stored = entry.compression() == ::zip::CompressionMethod::Stored;
        members.push(Member {
            name: name.to_string(),
            index,
            stored_at: if stored { entry.data_start() } else { None },
            size: entry.size(),
        });
    }
    Ok(members)
}

/// The header of `member`, read from its first bytes, decompressed when it is
/// compressed: never more than the header.
fn member_header(path: &Path, member: &Member) -> std::result::Result<Header, String> {
    let mut archive = archive_of(path).map_err(|e| e.to_string())?;
    let entry = archive.by_index(member.index).map_err(|e| e.to_string())?;
    let mut reader = entry.take(12);
    let mut head = Vec::with_capacity(12);
    reader.read_to_end(&mut head).map_err(|e| e.to_string())?;
    let (prefix, len) = header_len(&head)?;
    let mut entry = reader.into_inner();
    let mut rest = Vec::with_capacity(len);
    (&mut entry)
        .take((prefix + len - head.len()) as u64)
        .read_to_end(&mut rest)
        .map_err(|e| e.to_string())?;
    head.extend(rest);
    parse_header(&head)
}

/// The arrays of an archive as tables, with their columns, for the home screen.
pub fn tables(path: &Path) -> Result<Vec<Table>> {
    let members = members(path)?;
    Ok(members
        .iter()
        .map(|member| {
            let columns = member_header(path, member)
                .ok()
                .and_then(|header| {
                    let plan = plan(&header, &member.name).ok()?;
                    Some(
                        plan.columns
                            .iter()
                            .map(|c| (c.name.to_string(), String::new()))
                            .collect(),
                    )
                })
                .unwrap_or_default();
            Table {
                name: member.name.clone(),
                kind: "array".to_string(),
                internal: false,
                columns,
            }
        })
        .collect())
}

/// The columns an array would open with, from its header: a `.npy` file, or an
/// archive's `member`. `None` for one that would not open.
pub fn schema_preview(path: &Path, member: Option<&str>) -> Option<crate::discover::SchemaPreview> {
    let (header, name) = match member {
        Some(name) => {
            let found = members(path).ok()?.into_iter().find(|m| m.name == name)?;
            (member_header(path, &found).ok()?, found.name)
        }
        None if is_archive_name(path) => {
            let all = members(path).ok()?;
            let [one] = all.as_slice() else {
                return None;
            };
            (member_header(path, one).ok()?, one.name.clone())
        }
        None => {
            let mut file = std::fs::File::open(path).ok()?;
            let mut head = [0u8; 12];
            file.read_exact(&mut head).ok()?;
            let (prefix, len) = header_len(&head).ok()?;
            let mut bytes = head.to_vec();
            file.take((prefix + len - 12) as u64)
                .read_to_end(&mut bytes)
                .ok()?;
            (parse_header(&bytes).ok()?, stem(path))
        }
    };
    let plan = plan(&header, &name).ok()?;
    Some(
        plan.columns
            .iter()
            .map(|c| (c.name.to_string(), c.dtype()))
            .collect(),
    )
}

/// What opening a NumPy file finds.
pub enum Open {
    /// An array read in place.
    Array {
        lf: Box<LazyFrame>,
        opened: Box<crate::members::Opened>,
    },
    /// An archive of several arrays, and no `--table` to say which.
    Several(Vec<String>),
    /// A compressed member, to be decompressed to a file first.
    Compressed { member: String },
}

/// Open `path`: a `.npy` file, or an archive's array `wanted` names, or its one array.
pub fn open(path: &Path, wanted: Option<&str>) -> Result<Open> {
    let mut head = [0u8; 8];
    let is_npy = std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut head))
        .is_ok()
        && looks_like(&head);
    if is_npy {
        if let Some(wanted) = wanted {
            return Err(eyre!(
                "{} is one array; --table {wanted:?} picks an array of an .npz archive.",
                path.display()
            ));
        }
        let array = open_file(path)?;
        return Ok(Open::Array {
            lf: Box::new(array.records.lazy()),
            opened: Box::new(opened(array, None, false)),
        });
    }
    if !is_archive_name(path) {
        return Err(eyre!(
            "{} is not a NumPy file: an .npy file starts with \\x93NUMPY, and an .npz is a zip of them.",
            path.display()
        ));
    }
    let members = members(path)?;
    let tables: Vec<Table> = members
        .iter()
        .map(|m| Table {
            name: m.name.clone(),
            kind: "array".to_string(),
            internal: false,
            columns: Vec::new(),
        })
        .collect();
    let picked =
        match crate::members::pick(tables.clone(), wanted, path, crate::FileFormat::Numpy, "")? {
            crate::sqlite::Pick::One(table) => table,
            crate::sqlite::Pick::Several(tables) => {
                return Ok(Open::Several(tables.into_iter().map(|t| t.name).collect()));
            }
        };
    let member = members
        .iter()
        .find(|m| m.name == picked.name)
        .expect("picked from the members");
    let Some(at) = member.stored_at else {
        return Ok(Open::Compressed {
            member: member.name.clone(),
        });
    };
    let bytes = Bytes::map(path).map_err(|e| eyre!("{}: {e}", path.display()))?;
    let at = usize::try_from(at).map_err(|_| eyre!("{} is too large", path.display()))?;
    let len = usize::try_from(member.size).unwrap_or(usize::MAX);
    let array = open_in(Arc::new(bytes), at, len, &member.name)
        .map_err(|e| eyre!("{} in {} is not read: {e}", member.name, path.display()))?;
    let lf = array.records.lazy();
    let mut opened = opened(array, Some((path, members.len())), false);
    opened.other_tables = crate::members::others(&tables, &member.name);
    Ok(Open::Array {
        lf: Box::new(lf),
        opened: Box::new(opened),
    })
}

/// What the open carries to the dataset for `array`.
fn opened(
    array: Array,
    archive: Option<(&Path, usize)>,
    compressed: bool,
) -> crate::members::Opened {
    let rows = array.records.rows();
    crate::members::Opened {
        window: Some((array.records.clone(), rows)),
        detail: Some(Arc::new(detail(&array.header, archive, compressed))),
        other_tables: Vec::new(),
        notes: array
            .notes
            .into_iter()
            .map(|summary| crate::text_formats::note(summary, "the array".to_string()))
            .collect(),
        units: Vec::new(),
    }
}

/// Decompress the archive's array `name` into a temporary `.npy` file written through
/// `writer`, counting the archive's bytes read in `read`, and open it.
pub(crate) fn convert(
    path: &Path,
    name: &str,
    options: &crate::OpenOptions,
    writer: &crate::unfinished::Writer,
    read: &std::sync::atomic::AtomicU64,
) -> Result<(
    crate::download::TempDownload,
    LazyFrame,
    crate::members::Opened,
)> {
    use std::io::Write;
    use std::sync::atomic::Ordering;
    let all = members(path)?;
    let member = all
        .iter()
        .find(|m| m.name == name)
        .ok_or_else(|| eyre!("No array {name:?} in {}.", path.display()))?;
    let mut archive = archive_of(path)?;
    let entry = archive
        .by_index(member.index)
        .map_err(|e| eyre!("{name} in {}: {e}", path.display()))?;
    let compressed = entry.compressed_size();
    let Some((mut file, claim)) = writer.create(|| {
        crate::download::TempDownload::create(options.temp_dir.as_deref(), Some("npy"))
    })?
    else {
        return Err(eyre!("Reading was stopped."));
    };
    // No more than the archive says the array is, so a member that inflates past its
    // stated size stops there rather than filling the disk.
    let mut reader = entry.take(member.size);
    let mut chunk = vec![0u8; 1 << 16];
    let mut written = 0u64;
    loop {
        if writer.stopped() {
            return Err(eyre!("Reading was stopped."));
        }
        let n = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(eyre!("{name} in {}: {e}", path.display())),
        };
        file.write_all(&chunk[..n])?;
        written += n as u64;
        if member.size > 0 {
            // The compressed bytes read so far, in proportion.
            let share = (compressed as u128 * n as u128 / member.size as u128) as u64;
            read.fetch_add(share, Ordering::Relaxed);
        }
    }
    file.flush()?;
    let held = crate::download::TempDownload::held(file, Some(claim));
    let bytes = Bytes::map(held.path())?;
    let array = open_in(Arc::new(bytes), 0, written as usize, name)
        .map_err(|e| eyre!("{name} in {} is not read: {e}", path.display()))?;
    let tables: Vec<Table> = all
        .iter()
        .map(|m| Table {
            name: m.name.clone(),
            kind: "array".to_string(),
            internal: false,
            columns: Vec::new(),
        })
        .collect();
    let lf = array.records.lazy();
    let mut opened = opened(array, Some((path, all.len())), true);
    opened.other_tables = crate::members::others(&tables, name);
    Ok((held, lf, opened))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn npy(descr: &str, fortran: bool, shape: &str, data: &[u8]) -> Vec<u8> {
        let mut header = format!(
            "{{'descr': {descr}, 'fortran_order': {}, 'shape': {shape}, }}",
            if fortran { "True" } else { "False" }
        );
        while (10 + header.len() + 1) % 64 != 0 {
            header.push(' ');
        }
        header.push('\n');
        let mut out = MAGIC.to_vec();
        out.extend([1, 0]);
        out.extend((header.len() as u16).to_le_bytes());
        out.extend(header.as_bytes());
        out.extend(data);
        out
    }

    fn read(bytes: Vec<u8>, name: &str) -> DataFrame {
        let len = bytes.len();
        let array = open_in(Arc::new(Bytes::Owned(bytes)), 0, len, name).unwrap();
        array.records.lazy().collect().unwrap()
    }

    #[test]
    fn literals_parse() {
        let parsed = parse_literal("{'a': [('x', '<f8', (3,)), (('t', 'y'), '|b1')], 'b': (2L,), 'c': True, 'd': None, 'e': -4}")
            .unwrap();
        assert_eq!(
            parsed.get("b"),
            Some(&Literal::Tuple(vec![Literal::Int(2)]))
        );
        assert_eq!(parsed.get("c"), Some(&Literal::Bool(true)));
        assert_eq!(parsed.get("e"), Some(&Literal::Int(-4)));
        assert_eq!(parse_literal("(1)").unwrap(), Literal::Int(1));
        assert_eq!(parse_literal("()").unwrap(), Literal::Tuple(Vec::new()));
        assert!(parse_literal("{'a': 1").is_err());
        assert!(parse_literal(&"[".repeat(100)).is_err());
        assert!(parse_literal("__import__('os')").is_err());
    }

    #[test]
    fn a_vector_is_one_column_named_for_the_array() {
        let data: Vec<u8> = [1.5f64, -2.0, 3.25]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let df = read(npy("'<f8'", false, "(3,)", &data), "prices");
        assert_eq!(df.get_column_names(), ["prices"]);
        assert_eq!(
            df.column("prices").unwrap().f64().unwrap().to_vec(),
            [Some(1.5), Some(-2.0), Some(3.25)]
        );
    }

    #[test]
    fn a_grid_in_either_order() {
        // [[1, 2, 3], [4, 5, 6]] as int32.
        let c: Vec<u8> = [1i32, 2, 3, 4, 5, 6]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let f: Vec<u8> = [1i32, 4, 2, 5, 3, 6]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        for df in [
            read(npy("'<i4'", false, "(2, 3)", &c), "g"),
            read(npy("'<i4'", true, "(2, 3)", &f), "g"),
        ] {
            assert_eq!(df.get_column_names(), ["0", "1", "2"]);
            assert_eq!(
                df.column("2").unwrap().i32().unwrap().to_vec(),
                [Some(3), Some(6)]
            );
        }
    }

    #[test]
    fn structured_fields_mixed_endian_subarrays_and_padding() {
        // ts <u8, px >f4 (big-endian), 3 bytes of padding, v <i2 (2,)
        let mut data = Vec::new();
        for (ts, px, v) in [(1u64, 0.5f32, [7i16, 8]), (2, 1.5, [9, 10])] {
            data.extend(ts.to_le_bytes());
            data.extend(px.to_be_bytes());
            data.extend([0u8; 3]);
            data.extend(v.iter().flat_map(|x| x.to_le_bytes()));
        }
        let descr = "[('ts', '<u8'), ('px', '>f4'), ('', '|V3'), ('v', '<i2', (2,))]";
        let df = read(npy(descr, false, "(2,)", &data), "t");
        assert_eq!(df.get_column_names(), ["ts", "px", "v"]);
        assert_eq!(
            df.column("px").unwrap().f32().unwrap().to_vec(),
            [Some(0.5), Some(1.5)]
        );
        assert_eq!(
            df.column("v").unwrap().dtype(),
            &DataType::Array(Box::new(DataType::Int16), 2)
        );
    }

    #[test]
    fn the_dict_form_honors_offsets_and_itemsize() {
        // a at 4, b at 0, in 12-byte elements.
        let mut data = Vec::new();
        for (a, b) in [(10i32, 20i32), (30, 40)] {
            data.extend(b.to_le_bytes());
            data.extend(a.to_le_bytes());
            data.extend([0u8; 4]);
        }
        let descr =
            "{'names': ['a', 'b'], 'formats': ['<i4', '<i4'], 'offsets': [4, 0], 'itemsize': 12}";
        let df = read(npy(descr, false, "(2,)", &data), "t");
        assert_eq!(
            df.column("a").unwrap().i32().unwrap().to_vec(),
            [Some(10), Some(30)]
        );
        assert_eq!(
            df.column("b").unwrap().i32().unwrap().to_vec(),
            [Some(20), Some(40)]
        );
    }

    #[test]
    fn strings_times_and_bools() {
        let mut data = Vec::new();
        // U3 "hé", S4 "ab", M8[s] 86400 (1970-01-02), m8[ms] 1500, b1 true.
        for c in "hé".chars().chain(std::iter::once('\0')) {
            data.extend((c as u32).to_le_bytes());
        }
        data.extend(b"ab\0\0");
        data.extend(86_400i64.to_le_bytes());
        data.extend(1500i64.to_le_bytes());
        data.push(1);
        let descr = "[('u', '<U3'), ('s', '|S4'), ('t', '<M8[s]'), ('d', '<m8[ms]'), ('b', '|b1')]";
        let df = read(npy(descr, false, "(1,)", &data), "t");
        assert_eq!(df.column("u").unwrap().str().unwrap().get(0), Some("hé"));
        assert_eq!(df.column("s").unwrap().str().unwrap().get(0), Some("ab"));
        assert_eq!(
            df.column("t").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(
            df.column("d").unwrap().dtype(),
            &DataType::Duration(TimeUnit::Milliseconds)
        );
        assert_eq!(df.column("b").unwrap().bool().unwrap().get(0), Some(true));
    }

    #[test]
    fn what_is_refused() {
        let refused = |descr: &str, shape: &str| {
            let bytes = npy(descr, false, shape, &[0u8; 64]);
            let len = bytes.len();
            open_in(Arc::new(Bytes::Owned(bytes)), 0, len, "x")
                .err()
                .unwrap()
        };
        assert!(refused("'|O'", "(1,)").contains("does not unpickle"));
        assert!(refused("'<f8'", "(1, 1, 1)").contains("shape (1, 1, 1)"));
        assert!(refused("'<f16'", "(1,)").contains("not read"));
    }

    #[test]
    fn a_short_file_keeps_the_rows_it_has() {
        let data: Vec<u8> = [1i16, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
        let bytes = npy("'<i2'", false, "(10,)", &data);
        let len = bytes.len();
        let array = open_in(Arc::new(Bytes::Owned(bytes)), 0, len, "x").unwrap();
        assert_eq!(array.records.rows(), 3);
        assert_eq!(array.notes.len(), 1);
    }
}

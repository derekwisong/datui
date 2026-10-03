//! Binary formats described by specs: one TOML file per format, found on a search path.
//!
//! A spec names its format (`acme.l2feed`), says which files are in it (`match`: a
//! glob, magic bytes, header values), and lays out its header and its records. Specs
//! are data: no expressions and no code. A field refers to an earlier one by name
//! instead, and every size read from a file is bounded.
//!
//! What a spec describes is decoded by [`crate::fixed_records`]; this module turns a
//! spec and a file into that reader's columns.

use crate::fixed_records::{Bytes, ColumnLayout, FixedRecords, Logical, Null, Physical};
use globset::{Glob, GlobSet, GlobSetBuilder};
use polars::prelude::{
    AnyValue, DataFrame, LazyFrame, PlSmallStr, PolarsResult, SchemaRef, TimeUnit,
};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use toml::de::{DeTable, DeValue};

/// The largest size a spec or a file may give one field, header or record, and the
/// most values one field may hold. A size read from a file past this is refused
/// rather than believed.
pub const MAX_SIZE: u64 = 64 << 20;

/// The most columns one field may be flattened into.
pub const MAX_FLATTEN: u64 = 1024;

/// The most bytes at the front of a file read to match it: its magic and the header
/// values `match.where` compares.
const MAX_MATCH_READ: u64 = 64 << 10;

/// Nanoseconds in a day.
const DAY_NS: i64 = 86_400_000_000_000;

/// The variable the search path is extended with, after the config directory.
pub const PATH_VAR: &str = "DATUI_FORMATS_PATH";

/// A problem with a spec: where it is (file, line, column) and what was expected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    pub path: Option<PathBuf>,
    /// One-based; 0 when the problem is not at one place in the text.
    pub line: usize,
    pub column: usize,
    pub message: String,
}

impl std::fmt::Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(f, "{}:", path.display())?;
        }
        if self.line > 0 {
            write!(f, "{}:{}: ", self.line, self.column)?;
        } else if self.path.is_some() {
            f.write_str(" ")?;
        }
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpecError {}

/// Byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Little,
    Big,
}

/// How the records sit in what is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// One file, a record after another.
    Rows,
    /// A directory holding one file of fixed-width values per field, as kdb+ splays a
    /// table; or, when every field gives its `offset`, one file holding each column's
    /// values in a run of their own.
    Columns,
}

/// How one record is told from the next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Framing {
    /// Each record takes the same bytes: `size`, or what its fields take.
    #[default]
    Fixed,
    /// A field of the record gives its size (`size = "len"`).
    LengthPrefixed,
    /// The variant the type field picks gives the record's size.
    Variant,
    /// Each record starts with the `sync` marker; bytes between records are skipped.
    Sync,
}

/// A size or a count: written in the spec, or read from a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Amount {
    Given(u64),
    /// The value of header field `field`, plus `adjust`.
    Header {
        field: String,
        adjust: i64,
    },
    /// The value of footer field `field`, plus `adjust`.
    Footer {
        field: String,
        adjust: i64,
    },
    /// The value of an earlier field of the same record (or block header, or group
    /// item), plus `adjust`.
    Record {
        field: String,
        adjust: i64,
    },
    /// What is left of the record: `size = "rest"`.
    Rest,
}

impl Amount {
    /// Whether the amount is known before a record is read.
    pub fn is_fixed(&self) -> bool {
        !matches!(self, Self::Record { .. } | Self::Rest)
    }
}

/// What a field's bytes hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// 1 to 8 bytes.
    Unsigned(u8),
    Signed(u8),
    /// 2 (half), 4 or 8 bytes.
    Float(u8),
    /// bfloat16.
    BFloat16,
    Bool,
    Str,
    /// Text up to its NUL, within `size` when one is given.
    Strz,
    Bytes,
    Pad,
    /// LEB128: unsigned, and zigzag signed.
    VarU,
    VarS,
    /// A counted run of items, each of the group's fields.
    Group,
}

impl Type {
    /// Bytes one value of the type takes, when the type says.
    pub fn width(self) -> Option<u64> {
        match self {
            Self::Strz | Self::VarU | Self::VarS | Self::Group => None,
            _ => self.physical().width().map(|w| w as u64),
        }
    }

    fn is_integer(self) -> bool {
        matches!(
            self,
            Self::Unsigned(_) | Self::Signed(_) | Self::VarU | Self::VarS
        )
    }

    fn is_number(self) -> bool {
        self.is_integer() || matches!(self, Self::Float(_) | Self::BFloat16)
    }

    pub fn is_text(self) -> bool {
        matches!(self, Self::Str | Self::Strz)
    }

    fn physical(self) -> Physical {
        match self {
            Self::Unsigned(n) => Physical::Unsigned(n),
            Self::Signed(n) => Physical::Signed(n),
            Self::Float(n) => Physical::Float(n),
            Self::BFloat16 => Physical::BFloat16,
            Self::Bool => Physical::Bool,
            Self::Str | Self::Strz => Physical::Text,
            // A varint is decoded to eight bytes before its meaning is applied.
            Self::VarU => Physical::Unsigned(8),
            Self::VarS => Physical::Signed(8),
            Self::Bytes | Self::Pad | Self::Group => Physical::Raw,
        }
    }
}

/// How text is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    #[default]
    Utf8,
    Latin1,
    Utf16Le,
    Utf16Be,
}

impl Encoding {
    pub fn physical(self) -> Physical {
        match self {
            Self::Utf8 => Physical::Text,
            Self::Latin1 => Physical::Latin1,
            Self::Utf16Le => Physical::Utf16 { big_endian: false },
            Self::Utf16Be => Physical::Utf16 { big_endian: true },
        }
    }

    /// Bytes in one code unit: where a NUL is looked for.
    pub fn unit(self) -> usize {
        match self {
            Self::Utf16Le | Self::Utf16Be => 2,
            _ => 1,
        }
    }
}

/// A value stored as the change from the previous record's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Delta {
    #[default]
    None,
    /// Summed from the first record.
    All,
    /// Summed from the first record of each block.
    Block,
}

/// One bit field of an integer: its own column.
#[derive(Debug, Clone, PartialEq)]
pub struct BitField {
    pub name: String,
    /// The lowest bit, 0 the least significant.
    pub bit: u32,
    pub width: u32,
    pub labels: Option<Arc<BTreeMap<i64, String>>>,
}

/// Where a symbol list is, for a field of indexes into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lookup {
    /// Relative to the directory the data is in.
    pub file: String,
    pub format: LookupFormat,
}

/// How a symbol list's entries are told apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupFormat {
    /// One a line.
    Lines,
    /// Each ended by a NUL, as kdb+ writes its `sym` file.
    Nul,
    /// Each `n` bytes, padded.
    Fixed(u64),
}

/// A checksum over part of each record, or of the file before its footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    pub algo: ChecksumAlgo,
    /// The field holding the stored checksum.
    pub field: String,
    /// The field the checksummed bytes start at; the record's start when `None`.
    pub from: Option<String>,
    /// The field they end before; the checksum field when `None`.
    pub to: Option<String>,
}

/// The checksums a spec can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgo {
    Crc16Ccitt,
    Crc16Xmodem,
    Crc16Modbus,
    Crc16Arc,
    Crc32,
    Crc32c,
    Sum8,
    Xor8,
}

impl ChecksumAlgo {
    const NAMES: &[(&str, Self)] = &[
        ("crc16-ccitt", Self::Crc16Ccitt),
        ("crc16-xmodem", Self::Crc16Xmodem),
        ("crc16-modbus", Self::Crc16Modbus),
        ("crc16-arc", Self::Crc16Arc),
        ("crc32", Self::Crc32),
        ("crc32c", Self::Crc32c),
        ("sum8", Self::Sum8),
        ("xor8", Self::Xor8),
    ];

    fn parse(name: &str) -> Option<Self> {
        Self::NAMES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, a)| *a)
    }

    /// The checksum of `bytes`.
    pub fn compute(self, bytes: &[u8]) -> u64 {
        use crc::{
            CRC_16_ARC, CRC_16_IBM_3740, CRC_16_MODBUS, CRC_16_XMODEM, CRC_32_ISCSI,
            CRC_32_ISO_HDLC, Crc,
        };
        match self {
            Self::Crc16Ccitt => u64::from(Crc::<u16>::new(&CRC_16_IBM_3740).checksum(bytes)),
            Self::Crc16Xmodem => u64::from(Crc::<u16>::new(&CRC_16_XMODEM).checksum(bytes)),
            Self::Crc16Modbus => u64::from(Crc::<u16>::new(&CRC_16_MODBUS).checksum(bytes)),
            Self::Crc16Arc => u64::from(Crc::<u16>::new(&CRC_16_ARC).checksum(bytes)),
            Self::Crc32 => u64::from(Crc::<u32>::new(&CRC_32_ISO_HDLC).checksum(bytes)),
            Self::Crc32c => u64::from(Crc::<u32>::new(&CRC_32_ISCSI).checksum(bytes)),
            Self::Sum8 => u64::from(bytes.iter().fold(0u8, |a, b| a.wrapping_add(*b))),
            Self::Xor8 => u64::from(bytes.iter().fold(0u8, |a, b| a ^ b)),
        }
    }
}

/// The unit a `time` field counts in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnitSpec {
    Days,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TimeUnitSpec {
    fn nanos(self) -> i64 {
        match self {
            Self::Days => DAY_NS,
            Self::Seconds => 1_000_000_000,
            Self::Millis => 1_000_000,
            Self::Micros => 1_000,
            Self::Nanos => 1,
        }
    }
}

/// What a field means beyond its stored value.
#[derive(Debug, Clone, PartialEq)]
pub enum Meaning {
    Plain,
    /// A count of `unit` since `epoch_ns` nanoseconds past the Unix epoch: a datetime,
    /// or a date for whole days.
    Time {
        unit: TimeUnitSpec,
        epoch_ns: i64,
    },
    /// A count of `unit` since midnight: a time of day, or a datetime on the date the
    /// header field `date` holds.
    TimeOfDay {
        unit: TimeUnitSpec,
        date: Option<String>,
    },
    /// An integer written as `YYYYMMDD`.
    Yyyymmdd,
    /// `scale` implied decimal places.
    Scale(u32),
    /// `value * factor + offset`, as a float.
    Linear {
        factor: f64,
        offset: f64,
    },
    /// Codes and their labels.
    Enum(Arc<BTreeMap<i64, String>>),
}

/// One field of a header or a record.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// `None` for `pad`.
    pub name: Option<String>,
    pub ty: Type,
    /// For `str`, `bytes` and `pad`.
    pub size: Option<Amount>,
    /// The type's own `le` or `be`, over the spec's.
    pub endian: Option<Endian>,
    pub meaning: Meaning,
    /// A stored value that means no value.
    pub null: Option<Null>,
    /// Values side by side: an Array column, or `flatten`ed into `name_0`, `name_1`, ...
    pub count: Option<Amount>,
    pub flatten: bool,
    /// In the columns layout, the file in the directory that holds it; its name by
    /// default.
    pub file: Option<String>,
    /// In the columns layout of one file, where the column's values start.
    pub at: Option<Amount>,
    /// For text.
    pub encoding: Encoding,
    pub delta: Delta,
    /// Columns of their own from the integer's bits.
    pub bits: Vec<BitField>,
    /// The fields of each item of a group; its count is `count`.
    pub group: Vec<Field>,
    /// An offset into this section of the file, where NUL-terminated text is.
    pub string_at: Option<String>,
    pub lookup: Option<Lookup>,
}

/// A spec's header: its fields, and its size when that is more than they take.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Header {
    pub fields: Vec<Field>,
    pub size: Option<Amount>,
}

/// A spec's records.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Records {
    pub framing: Framing,
    /// Every record's fields; with variants, the common ones before the variant's.
    pub fields: Vec<Field>,
    /// At least the fields' sizes; the rest of each record is skipped. Read from a
    /// field of the record for `length_prefixed`.
    pub size: Option<Amount>,
    /// How many records there are, when the file says.
    pub count: Option<Amount>,
    /// The size is written again after the record, as Fortran writes it.
    pub length_suffix: bool,
    /// Each record starts at a multiple of this, counted from the first.
    pub align: u64,
    /// The marker each record starts with, for `sync`.
    pub sync: Vec<u8>,
    /// The common field whose value picks the variant.
    pub type_field: Option<String>,
    pub variants: Vec<Variant>,
    pub checksum: Option<Checksum>,
    /// A ring buffer: the oldest record's index, from the header.
    pub ring: Option<Amount>,
}

/// One layout a record can take, picked by the type field.
#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    pub name: String,
    /// The type values that pick it.
    pub when: Vec<Expected>,
    /// The fields after the common ones.
    pub fields: Vec<Field>,
    /// The whole record's size, when more than its fields take.
    pub size: Option<Amount>,
}

/// Fields at the end of the file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Footer {
    pub fields: Vec<Field>,
    pub size: Option<u64>,
    /// A checksum of the bytes before the footer.
    pub checksum: Option<(ChecksumAlgo, String)>,
}

/// How a block's body is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Deflate,
    Zlib,
    Zstd,
    /// The LZ4 frame format.
    Lz4,
    /// A raw LZ4 block; its decompressed size comes from `uncompressed`.
    Lz4Block,
    /// Raw Snappy.
    Snappy,
    /// The Snappy frame format.
    SnappyFramed,
    Brotli,
    Bzip2,
    Xz,
}

impl Compression {
    const NAMES: &[(&str, Self)] = &[
        ("none", Self::None),
        ("gzip", Self::Gzip),
        ("deflate", Self::Deflate),
        ("zlib", Self::Zlib),
        ("zstd", Self::Zstd),
        ("lz4", Self::Lz4),
        ("lz4_block", Self::Lz4Block),
        ("snappy", Self::Snappy),
        ("snappy_framed", Self::SnappyFramed),
        ("brotli", Self::Brotli),
        ("bzip2", Self::Bzip2),
        ("xz", Self::Xz),
    ];

    fn parse(name: &str) -> Option<Self> {
        Self::NAMES
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, c)| *c)
    }

    pub fn name(self) -> &'static str {
        Self::NAMES
            .iter()
            .find(|(_, c)| *c == self)
            .map_or("none", |(n, _)| n)
    }
}

/// The codec of each block: one for all, or one a header field names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Codec {
    Fixed(Compression),
    ByField {
        field: String,
        values: BTreeMap<i64, Compression>,
    },
}

/// Blocks listed in the file, so their headers are not walked.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockIndex {
    /// Where the index starts.
    pub at: Amount,
    /// How many entries it has.
    pub count: Amount,
    /// One entry's fields: `offset` is required; `rows` is the records in the block.
    pub fields: Vec<Field>,
}

/// Records in blocks, each with a header and, often, compressed on its own.
#[derive(Debug, Clone, PartialEq)]
pub struct Blocks {
    pub header: Vec<Field>,
    /// The body's bytes after the header.
    pub size: Amount,
    pub codec: Codec,
    /// The header field counting the block's records.
    pub records: Option<String>,
    /// The header field giving the body's decompressed size.
    pub uncompressed: Option<String>,
    pub index: Option<BlockIndex>,
}

/// A packet capture whose payloads hold the records.
#[derive(Debug, Clone, PartialEq)]
pub struct Capture {
    /// Fields at the front of each payload, such as a MoldUDP64 header.
    pub header: Vec<Field>,
    /// The payload header field counting its records.
    pub count: Option<String>,
    /// The name of a column of each packet's capture time.
    pub time: Option<String>,
}

/// A part of a file's path that becomes a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathPart {
    pub name: String,
    /// A chrono format for a date, such as `%Y%m%d`; text when `None`.
    pub date: Option<String>,
}

/// A directory of one spec's files: each file's path is read by `pattern`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Files {
    pub pattern: String,
    pub parts: Vec<PathPart>,
}

/// A named run of the file that fields point into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    pub name: String,
    pub offset: Amount,
    pub size: Amount,
}

/// A header value a file must hold to match: `match.where`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expected {
    Int(i128),
    Text(String),
}

/// The most a spec file may hold, local or remote: far more than any spec needs, and
/// a bound on what `--spec` reads before it knows what it read.
pub const MAX_SPEC_BYTES: u64 = 1 << 20;
/// [`MAX_SPEC_BYTES`], as the user is told it.
pub const MAX_SPEC_SAID: &str = "1 MiB";

/// One format, as its spec describes it.
#[derive(Debug, Clone)]
pub struct Spec {
    pub name: String,
    pub description: Option<String>,
    /// The file it was read from.
    pub path: Option<PathBuf>,
    pub globs: Vec<String>,
    glob_set: Option<GlobSet>,
    pub magic: Vec<u8>,
    pub magic_offset: u64,
    /// Header fields and the values a file must hold in them to match.
    pub expect: Vec<(String, Expected)>,
    pub endian: Endian,
    /// The byte order is the one the magic reads in: as written, little-endian, and
    /// reversed, big-endian.
    pub endian_auto: bool,
    pub layout: Layout,
    pub header: Header,
    pub records: Records,
    /// For `kind = "delimited"`: the reading options of a CSV-like file. A delimited
    /// spec has no header or record fields.
    pub delimited: Option<Arc<crate::delimited_spec::Delimited>>,
    pub footer: Option<Footer>,
    pub blocks: Option<Blocks>,
    pub capture: Option<Capture>,
    pub files: Option<Files>,
    pub sections: Vec<Section>,
    /// The variant read alone (`--variant`), when one is.
    pub variant: Option<String>,
}

impl PartialEq for Spec {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.description == other.description
            && self.path == other.path
            && self.globs == other.globs
            && self.magic == other.magic
            && self.magic_offset == other.magic_offset
            && self.expect == other.expect
            && self.endian == other.endian
            && self.layout == other.layout
            && self.endian_auto == other.endian_auto
            && self.header == other.header
            && self.records == other.records
            && self.delimited == other.delimited
            && self.footer == other.footer
            && self.blocks == other.blocks
            && self.capture == other.capture
            && self.files == other.files
            && self.sections == other.sections
    }
}

/// The byte-order mark a text file may start with.
const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// What a spec's `match` says files of it look like.
#[derive(Default)]
struct MatchRules {
    globs: Vec<String>,
    glob_set: Option<GlobSet>,
    magic: Vec<u8>,
    magic_offset: u64,
    expect: Vec<(String, Expected)>,
}

/// Line and column (one-based) of byte `offset` in `text`.
fn line_column(text: &str, offset: usize) -> (usize, usize) {
    let mut offset = offset.min(text.len());
    while !text.is_char_boundary(offset) {
        offset -= 1;
    }
    let before = &text[..offset];
    let line = before.matches('\n').count() + 1;
    let column = before
        .rsplit('\n')
        .next()
        .map_or(0, |last| last.chars().count())
        + 1;
    (line, column)
}

/// Which part of a file a field belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Header,
    Footer,
    /// A record, a block header, a payload header or a group item: read one at a
    /// time, so a field may size a later one.
    Records,
}

/// The keys a field takes.
const FIELD_KEYS: &[&str] = &[
    "name",
    "type",
    "size",
    "size_adjust",
    "count",
    "flatten",
    "null",
    "time",
    "epoch",
    "of_day",
    "date",
    "scale",
    "factor",
    "offset",
    "enum",
    "file",
    "encoding",
    "delta",
    "bits",
    "group",
    "string_at",
    "lookup",
];

/// The parts a field can refer to by name: `header.NAME`, `footer.NAME`.
#[derive(Clone, Copy, Default)]
struct Scopes<'f> {
    header: &'f [Field],
    footer: &'f [Field],
}

/// Reads a spec's TOML, keeping where each value was so a problem can say.
struct Reader<'a> {
    text: &'a str,
    path: Option<&'a Path>,
}

type Value<'i> = toml::Spanned<DeValue<'i>>;

impl Reader<'_> {
    fn error(&self, span: &Range<usize>, message: impl Into<String>) -> SpecError {
        let (line, column) = line_column(self.text, span.start);
        SpecError {
            path: self.path.map(Path::to_path_buf),
            line,
            column,
            message: message.into(),
        }
    }

    /// The table's entries, after checking every key is one of `known`.
    fn entries<'t, 'i>(
        &self,
        table: &'t DeTable<'i>,
        what: &str,
        known: &[&str],
    ) -> Result<BTreeMap<&'t str, &'t Value<'i>>, SpecError> {
        let mut out = BTreeMap::new();
        for (key, value) in table {
            let name: &str = key.get_ref();
            if !known.contains(&name) {
                return Err(self.error(
                    &key.span(),
                    format!(
                        "unknown key `{name}` in {what}; expected one of {}",
                        known.join(", ")
                    ),
                ));
            }
            out.insert(name, value);
        }
        Ok(out)
    }

    fn string(&self, value: &Value<'_>, what: &str) -> Result<String, SpecError> {
        match value.get_ref() {
            DeValue::String(s) => Ok(s.to_string()),
            _ => Err(self.error(&value.span(), format!("{what}: expected a string"))),
        }
    }

    fn integer(&self, value: &Value<'_>, what: &str) -> Result<i64, SpecError> {
        match value.get_ref() {
            DeValue::Integer(i) => {
                let digits = i.as_str().replace('_', "");
                i64::from_str_radix(&digits, i.radix())
                    .map_err(|_| self.error(&value.span(), format!("{what}: too large")))
            }
            _ => Err(self.error(&value.span(), format!("{what}: expected an integer"))),
        }
    }

    fn number(&self, value: &Value<'_>, what: &str) -> Result<f64, SpecError> {
        match value.get_ref() {
            DeValue::Integer(_) => Ok(self.integer(value, what)? as f64),
            DeValue::Float(f) => f
                .as_str()
                .replace('_', "")
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite())
                .ok_or_else(|| {
                    self.error(&value.span(), format!("{what}: expected a finite number"))
                }),
            _ => Err(self.error(&value.span(), format!("{what}: expected a number"))),
        }
    }

    fn boolean(&self, value: &Value<'_>, what: &str) -> Result<bool, SpecError> {
        match value.get_ref() {
            DeValue::Boolean(b) => Ok(*b),
            _ => Err(self.error(&value.span(), format!("{what}: expected true or false"))),
        }
    }

    fn table<'t, 'i>(
        &self,
        value: &'t Value<'i>,
        what: &str,
    ) -> Result<&'t DeTable<'i>, SpecError> {
        match value.get_ref() {
            DeValue::Table(t) => Ok(t),
            _ => Err(self.error(&value.span(), format!("{what}: expected a table"))),
        }
    }

    /// The earlier field `reference` names: `header.NAME`, `footer.NAME`, or `NAME`
    /// for an earlier field of the same part.
    fn earlier<'f>(
        &self,
        value: &Value<'_>,
        reference: &str,
        what: &str,
        earlier: &'f [Field],
        scopes: &Scopes<'f>,
    ) -> Result<(&'f Field, Option<&'static str>), SpecError> {
        let (scope, field) = match reference.split_once('.') {
            Some((scope, field)) => (Some(scope), field),
            None => (None, reference),
        };
        let (fields, scope) = match scope {
            Some("header") => (scopes.header, Some("header")),
            Some("footer") => (scopes.footer, Some("footer")),
            None => (earlier, None),
            Some(other) => {
                return Err(self.error(
                    &value.span(),
                    format!(
                        "{what}: `{other}.` is not a part; expected `header.NAME` or `footer.NAME`"
                    ),
                ));
            }
        };
        fields
            .iter()
            .find(|f| f.name.as_deref() == Some(field))
            .map(|f| (f, scope))
            .ok_or_else(|| {
                let part = scope.unwrap_or("earlier");
                let hint = if scope.is_none()
                    && scopes
                        .header
                        .iter()
                        .any(|f| f.name.as_deref() == Some(field))
                {
                    format!("; a header field is named `header.{field}`")
                } else {
                    String::new()
                };
                self.error(
                    &value.span(),
                    format!("{what}: no {part} field named `{field}`{hint}"),
                )
            })
    }

    /// A size or a count: a whole number, `rest`, or the name of an earlier plain
    /// integer field, with an optional `adjust`.
    fn amount(
        &self,
        value: &Value<'_>,
        adjust: Option<&Value<'_>>,
        what: &str,
        part: Part,
        earlier: &[Field],
        scopes: &Scopes<'_>,
    ) -> Result<Amount, SpecError> {
        let adjust = adjust
            .map(|a| self.integer(a, &format!("{what}_adjust")))
            .transpose()?;
        match value.get_ref() {
            DeValue::Integer(_) => {
                let n = self.integer(value, what)?;
                if adjust.is_some() {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}_adjust goes with a {what} read from a field"),
                    ));
                }
                if n < 0 || n as u64 > MAX_SIZE {
                    return Err(
                        self.error(&value.span(), format!("{what}: expected 0 to {MAX_SIZE}"))
                    );
                }
                Ok(Amount::Given(n as u64))
            }
            DeValue::String(reference) if reference.as_ref() == "rest" && what == "size" => {
                if part != Part::Records {
                    return Err(
                        self.error(&value.span(), "size = \"rest\" is for a field of a record")
                    );
                }
                if adjust.is_some() {
                    return Err(self.error(
                        &value.span(),
                        "size_adjust goes with a size read from a field",
                    ));
                }
                Ok(Amount::Rest)
            }
            DeValue::String(reference) => {
                let (target, scope) = self.earlier(value, reference, what, earlier, scopes)?;
                if !matches!(
                    target.ty,
                    Type::Unsigned(_) | Type::Signed(_) | Type::VarU | Type::VarS
                ) || target.meaning != Meaning::Plain
                    || target.count.is_some()
                    || target.delta != Delta::None
                {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: `{reference}` is not a plain integer field"),
                    ));
                }
                let field = target.name.clone().expect("a referenced field is named");
                let adjust = adjust.unwrap_or(0);
                Ok(match (scope, part) {
                    (Some("footer"), _) | (None, Part::Footer) => Amount::Footer { field, adjust },
                    (Some(_), _) | (None, Part::Header) => Amount::Header { field, adjust },
                    (None, Part::Records) => Amount::Record { field, adjust },
                })
            }
            _ => Err(self.error(
                &value.span(),
                format!("{what}: expected a whole number or the name of an earlier field"),
            )),
        }
    }

    /// A spec's `name`: namespaced, such as `acme.l2feed`.
    fn spec_name(&self, top: &BTreeMap<&str, &Value<'_>>) -> Result<String, SpecError> {
        match top.get("name") {
            Some(v) => {
                let name = self.string(v, "name")?;
                if !is_spec_name(&name) {
                    return Err(self.error(
                        &v.span(),
                        "name: expected a namespaced name of letters, digits, `_` and `-`, such as acme.l2feed",
                    ));
                }
                Ok(name)
            }
            None => Err(self.error(&(0..0), "missing `name`, such as name = \"acme.l2feed\"")),
        }
    }

    /// A spec's `match`: its globs, magic and, given a binary header's fields, the
    /// header values a file must hold.
    fn match_rules(
        &self,
        v: &Value<'_>,
        header: Option<&[Field]>,
    ) -> Result<MatchRules, SpecError> {
        let (mut globs, mut magic, mut magic_offset) = (Vec::new(), Vec::new(), 0u64);
        let mut glob_set = None;
        let mut expect = Vec::new();
        let table = self.table(v, "match")?;
        let keys = self.entries(table, "match", &["glob", "magic", "magic_offset", "where"])?;
        if let Some(g) = keys.get("glob") {
            globs = match g.get_ref() {
                DeValue::String(s) => vec![s.to_string()],
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| self.string(item, "glob"))
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(self.error(&g.span(), "glob: expected a string or a list of them"));
                }
            };
            let mut builder = GlobSetBuilder::new();
            for glob in &globs {
                builder.add(
                    Glob::new(glob).map_err(|e| {
                        self.error(&g.span(), format!("glob `{glob}`: {}", e.kind()))
                    })?,
                );
            }
            glob_set = Some(
                builder
                    .build()
                    .map_err(|e| self.error(&g.span(), format!("glob: {e}")))?,
            );
        }
        if let Some(m) = keys.get("magic") {
            magic = match m.get_ref() {
                DeValue::String(s) => s.as_bytes().to_vec(),
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| {
                        let byte = self.integer(item, "magic")?;
                        u8::try_from(byte)
                            .map_err(|_| self.error(&item.span(), "magic: a byte is 0 to 255"))
                    })
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(
                        self.error(&m.span(), "magic: expected a string or a list of bytes")
                    );
                }
            };
            if magic.is_empty() || magic.len() as u64 > MAX_MATCH_READ {
                return Err(self.error(&m.span(), "magic: expected 1 to 65536 bytes"));
            }
        }
        if let Some(o) = keys.get("magic_offset") {
            let offset = self.integer(o, "magic_offset")?;
            let room = MAX_MATCH_READ - magic.len() as u64;
            if offset < 0 || offset as u64 > room {
                return Err(self.error(&o.span(), format!("magic_offset: expected 0 to {room}")));
            }
            magic_offset = offset as u64;
        }
        if let Some(w) = keys.get("where") {
            let Some(header_fields) = header else {
                return Err(self.error(
                    &w.span(),
                    "where compares a binary header's fields; a delimited spec matches by glob and magic",
                ));
            };
            let table = self.table(w, "where")?;
            for (key, value) in table {
                let reference: &str = key.get_ref();
                let Some(field) = reference.strip_prefix("header.") else {
                    return Err(self.error(
                        &key.span(),
                        "where: expected header fields, such as \"header.version\" = 3",
                    ));
                };
                let Some(target) = header_fields
                    .iter()
                    .find(|f| f.name.as_deref() == Some(field))
                else {
                    return Err(self.error(
                        &key.span(),
                        format!("where: no header field named `{field}`"),
                    ));
                };
                let wanted = self.expected(value, target, "where")?;
                expect.push((field.to_string(), wanted));
            }
        }
        Ok(MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        })
    }

    /// The reading options of a `kind = "delimited"` spec, from its top-level keys.
    fn delimited(
        &self,
        top: &BTreeMap<&str, &Value<'_>>,
    ) -> Result<crate::delimited_spec::Delimited, SpecError> {
        use crate::delimited_spec::{Delimited, Derived, DerivedKind, HeaderRows, MAX_HEAD_LINE};
        let line = |value: &Value<'_>, what: &str| -> Result<usize, SpecError> {
            let n = self.integer(value, what)?;
            if n < 1 || n as usize > MAX_HEAD_LINE {
                return Err(self.error(
                    &value.span(),
                    format!("{what}: expected a line from 1 to {MAX_HEAD_LINE}"),
                ));
            }
            Ok(n as usize)
        };
        let lines = |value: &Value<'_>, what: &str| -> Result<Vec<usize>, SpecError> {
            match value.get_ref() {
                DeValue::Integer(_) => Ok(vec![line(value, what)?]),
                DeValue::Array(items) if !items.is_empty() => {
                    let mut rows = Vec::with_capacity(items.len());
                    for item in items {
                        let n = line(item, what)?;
                        if rows.contains(&n) {
                            return Err(self
                                .error(&item.span(), format!("{what}: line {n} is named twice")));
                        }
                        rows.push(n);
                    }
                    Ok(rows)
                }
                _ => Err(self.error(
                    &value.span(),
                    format!("{what}: expected a line number or a list of them"),
                )),
            }
        };
        let mut spec = Delimited::default();
        if let Some(v) = top.get("delimiter") {
            let text = self.string(v, "delimiter")?;
            spec.delimiter = match text.as_bytes() {
                [b] if b.is_ascii() && !matches!(b, b'"' | b'\n' | b'\r') => Some(*b),
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "delimiter: expected one character, such as \",\", \";\" or \"\\t\"",
                    ));
                }
            };
        }
        if let Some(v) = top.get("comment_char") {
            let text = self.string(v, "comment_char")?;
            crate::csv_dialect::check_comment_char(&text)
                .map_err(|e| self.error(&v.span(), format!("comment_char: {e}")))?;
            spec.comment_char = Some(text);
        }
        if let Some(v) = top.get("skip_initial_space") {
            spec.skip_initial_space = Some(self.boolean(v, "skip_initial_space")?);
        }
        if let Some(v) = top.get("header_join") {
            spec.header_join = Some(self.string(v, "header_join")?);
        }
        if let Some(v) = top.get("skip_lines") {
            let n = self.integer(v, "skip_lines")?;
            if n < 0 || n > i64::from(u32::MAX) {
                return Err(self.error(&v.span(), "skip_lines: expected 0 or more"));
            }
            spec.skip_lines = Some(n as usize);
        }
        if let Some(v) = top.get("null_value") {
            spec.null_values = match v.get_ref() {
                DeValue::String(s) => vec![s.to_string()],
                DeValue::Array(items) => items
                    .iter()
                    .map(|item| self.string(item, "null_value"))
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "null_value: expected a string or a list of them, such as \"NA\" or \"COL=-999\"",
                    ));
                }
            };
        }
        if let Some(v) = top.get("header_rows") {
            let rows = match v.get_ref() {
                DeValue::Table(table) => {
                    let keys =
                        self.entries(table, "header_rows", &["name", "unit", "description"])?;
                    if let Some(d) = keys.get("description") {
                        return Err(
                            self.error(&d.span(), "header_rows.description is not yet supported")
                        );
                    }
                    let Some(name) = keys.get("name") else {
                        return Err(self.error(
                            &v.span(),
                            "header_rows: missing `name`, the line that names the columns",
                        ));
                    };
                    let name = lines(name, "header_rows.name")?;
                    let unit = keys
                        .get("unit")
                        .map(|u| {
                            let n = line(u, "header_rows.unit")?;
                            if name.contains(&n) {
                                return Err(self.error(
                                    &u.span(),
                                    format!("header_rows.unit: line {n} is also a name line"),
                                ));
                            }
                            Ok(n)
                        })
                        .transpose()?;
                    HeaderRows { name, unit }
                }
                _ => HeaderRows {
                    name: lines(v, "header_rows")?,
                    unit: None,
                },
            };
            spec.header_rows = Some(rows);
        }
        if let Some(v) = top.get("metadata_line") {
            let n = line(v, "metadata_line")?;
            let header = spec.header_rows.as_ref();
            if header.is_some_and(|h| h.name.contains(&n) || h.unit == Some(n)) {
                return Err(self.error(
                    &v.span(),
                    format!("metadata_line: line {n} is a header line"),
                ));
            }
            let before_data = header
                .map_or(0, HeaderRows::last)
                .max(spec.skip_lines.unwrap_or(0));
            if n > before_data && spec.comment_char.is_none() {
                return Err(self.error(
                    &v.span(),
                    format!(
                        "metadata_line: line {n} would be read as data; put it above header_rows, or set skip_lines or comment_char"
                    ),
                ));
            }
            spec.metadata_line = Some(n);
        }
        if let Some(v) = top.get("columns") {
            let table = self.table(v, "[columns]")?;
            for (key, value) in table {
                let name: &str = key.get_ref();
                let what = format!("columns.{name}");
                let entry = self.table(value, &what)?;
                let keys = self.entries(entry, &what, &["from", "as", "format"])?;
                let Some(from) = keys.get("from") else {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: missing `from`, the columns it is made from"),
                    ));
                };
                let from_columns: Vec<String> = match from.get_ref() {
                    DeValue::String(s) => vec![s.to_string()],
                    DeValue::Array(items) => items
                        .iter()
                        .map(|item| self.string(item, &format!("{what}.from")))
                        .collect::<Result<_, _>>()?,
                    _ => {
                        return Err(self.error(
                            &from.span(),
                            format!("{what}.from: expected a column name or a list of them"),
                        ));
                    }
                };
                let Some(kind) = keys.get("as") else {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: missing `as`; expected datetime, date or time"),
                    ));
                };
                let kind = match self.string(kind, &format!("{what}.as"))?.as_str() {
                    "datetime" => DerivedKind::Datetime,
                    "date" => DerivedKind::Date,
                    "time" => DerivedKind::Time,
                    _ => {
                        return Err(self.error(
                            &kind.span(),
                            format!("{what}.as: expected datetime, date or time"),
                        ));
                    }
                };
                let most = if kind == DerivedKind::Datetime { 3 } else { 1 };
                if from_columns.is_empty() || from_columns.len() > most {
                    let expected = if most == 3 {
                        "1 to 3 columns: a date, a time and a UTC offset"
                    } else {
                        "one column"
                    };
                    return Err(self.error(
                        &from.span(),
                        format!("{what}.from: as = \"{}\" takes {expected}", kind.name()),
                    ));
                }
                let format = keys
                    .get("format")
                    .map(|f| self.string(f, &format!("{what}.format")))
                    .transpose()?;
                if name.trim().is_empty() {
                    return Err(self.error(&key.span(), "columns: a column needs a name"));
                }
                spec.columns.push(Derived {
                    name: name.to_string(),
                    from: from_columns,
                    kind,
                    format,
                });
            }
        }
        Ok(spec)
    }

    fn fields(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        scopes: &Scopes<'_>,
        prefix: &[Field],
    ) -> Result<Vec<Field>, SpecError> {
        let DeValue::Array(items) = value.get_ref() else {
            return Err(self.error(
                &value.span(),
                "fields: expected an array of tables, such as [{ name = \"ts\", type = \"u8\" }]",
            ));
        };
        let mut fields: Vec<Field> = prefix.to_vec();
        let mut names: std::collections::HashSet<String> =
            prefix.iter().flat_map(output_names).collect();
        for item in items {
            let field = self.field(item, part, layout, &fields, scopes)?;
            for name in output_names(&field) {
                if !names.insert(name.clone()) {
                    return Err(self.error(&item.span(), format!("a second field named `{name}`")));
                }
            }
            fields.push(field);
        }
        Ok(fields.split_off(prefix.len()))
    }

    fn field(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        earlier: &[Field],
        scopes: &Scopes<'_>,
    ) -> Result<Field, SpecError> {
        let table = self.table(value, "field")?;
        let mut keys = self.entries(table, "a field", FIELD_KEYS)?;
        let at = value.span();
        let (ty, endian) = match (keys.get("type"), keys.get("group")) {
            (Some(_), Some(g)) => {
                return Err(self.error(&g.span(), "group: a group has no type of its own"));
            }
            (None, Some(_)) => (Type::Group, None),
            (None, None) => return Err(self.error(&at, "field: missing `type`")),
            (Some(ty_value), None) => parse_type(&self.string(ty_value, "type")?).ok_or_else(|| {
                self.error(
                    &ty_value.span(),
                    "type: expected u1 to u8, s1 to s8, f2, f4, f8 (each with an optional le or be), bf2, vu, vs, bool, str, strz, bytes or pad",
                )
            })?,
        };
        let name = keys
            .get("name")
            .map(|v| {
                let name = self.string(v, "name")?;
                if name.trim().is_empty() {
                    return Err(self.error(&v.span(), "name: must not be empty"));
                }
                if name.contains('.') {
                    return Err(self.error(&v.span(), "name: must not contain `.`"));
                }
                Ok(name)
            })
            .transpose()?;
        if ty == Type::Pad {
            if let Some(key) = keys
                .keys()
                .find(|k| !matches!(**k, "type" | "size" | "size_adjust"))
            {
                return Err(self.error(
                    &keys[key].span(),
                    format!("pad: skipped bytes take only a size, not `{key}`"),
                ));
            }
        } else if name.is_none() {
            return Err(self.error(&at, "field: missing `name` (only pad goes without)"));
        }
        // A string offset is where a column starts (`offset = "header.px_off"`); a
        // number is the linear conversion's.
        let column_at = match keys.get("offset") {
            Some(v)
                if matches!(v.get_ref(), DeValue::String(_))
                    && layout == Layout::Columns
                    && part == Part::Records =>
            {
                let v = keys.remove("offset").expect("just read");
                Some(self.amount(v, None, "offset", Part::Header, &[], scopes)?)
            }
            _ => None,
        };
        if part != Part::Records
            && let Some(key) = ["delta", "bits", "group", "string_at", "lookup"]
                .iter()
                .find(|k| keys.contains_key(**k))
        {
            return Err(self.error(
                &keys[*key].span(),
                format!("{key}: is for a field of the records"),
            ));
        }
        if part != Part::Records && matches!(ty, Type::VarU | Type::VarS | Type::Strz) {
            return Err(self.error(
                &at,
                format!("{}: is for a field of the records", type_name(ty)),
            ));
        }
        let size = match (ty.width(), keys.get("size")) {
            (Some(width), Some(v)) => {
                return Err(self.error(
                    &v.span(),
                    format!(
                        "size: a {} is {width} bytes; size is for str, strz, bytes and pad",
                        type_name(ty)
                    ),
                ));
            }
            (None, Some(v)) if matches!(ty, Type::VarU | Type::VarS | Type::Group) => {
                return Err(
                    self.error(&v.span(), format!("size: a {} sizes itself", type_name(ty)))
                );
            }
            (Some(_), None) => None,
            (None, Some(v)) => Some(self.amount(
                v,
                keys.get("size_adjust").copied(),
                "size",
                part,
                earlier,
                scopes,
            )?),
            (None, None) if matches!(ty, Type::Strz | Type::VarU | Type::VarS | Type::Group) => {
                None
            }
            (None, None) => {
                return Err(self.error(&at, format!("{}: missing `size`", type_name(ty))));
            }
        };
        if !size.as_ref().is_some_and(|s| {
            matches!(
                s,
                Amount::Header { .. } | Amount::Footer { .. } | Amount::Record { .. }
            )
        }) && let Some(v) = keys.get("size_adjust")
        {
            return Err(self.error(&v.span(), "size_adjust goes with a size read from a field"));
        }
        if layout == Layout::Columns
            && part == Part::Records
            && size.as_ref().is_some_and(|s| !s.is_fixed())
        {
            return Err(self.error(
                &keys["size"].span(),
                "size: a column file's values are all one size",
            ));
        }
        let mut group = Vec::new();
        let mut count = keys
            .get("count")
            .map(|v| {
                if ty == Type::Group {
                    return Err(self.error(
                        &v.span(),
                        "count: a group's count goes inside group = { count = ... }",
                    ));
                }
                let count = self.amount(v, None, "count", part, earlier, scopes)?;
                if count == Amount::Given(0) {
                    return Err(self.error(&v.span(), "count: expected at least 1"));
                }
                Ok(count)
            })
            .transpose()?;
        if ty == Type::Group {
            let v = keys["group"];
            if layout == Layout::Columns {
                return Err(self.error(&v.span(), "group: a column file holds one value a row"));
            }
            let table = self.table(v, "group")?;
            let inner = self.entries(table, "group", &["count", "fields"])?;
            let Some(c) = inner.get("count") else {
                return Err(self.error(
                    &v.span(),
                    "group: missing `count`, such as count = \"n_levels\"",
                ));
            };
            count = Some(self.amount(c, None, "count", part, earlier, scopes)?);
            let Some(f) = inner.get("fields") else {
                return Err(self.error(&v.span(), "group: missing `fields`"));
            };
            group = self.fields(f, Part::Records, Layout::Rows, scopes, &[])?;
            if !group.iter().any(|f| f.name.is_some()) {
                return Err(self.error(&f.span(), "fields: a group item needs a named field"));
            }
        }
        let flatten = keys
            .get("flatten")
            .map(|v| self.boolean(v, "flatten"))
            .transpose()?
            .unwrap_or(false);
        if flatten {
            let v = keys["flatten"];
            match &count {
                Some(Amount::Given(n)) if *n > MAX_FLATTEN => {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "flatten: at most {MAX_FLATTEN} columns; leave {n} values an Array"
                        ),
                    ));
                }
                Some(Amount::Given(_)) if ty != Type::Group => {}
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "flatten: goes with a count written in the spec, such as count = 10",
                    ));
                }
            }
        }
        if matches!(count, Some(Amount::Rest)) {
            return Err(self.error(&keys["count"].span(), "count: expected a number or a field"));
        }

        let null = keys
            .get("null")
            .map(|v| {
                let null = match v.get_ref() {
                    DeValue::String(s) => match s.as_ref() {
                        "min" => Null::Min,
                        "max" => Null::Max,
                        "nan" => Null::NaN,
                        _ => {
                            return Err(self.error(
                                &v.span(),
                                "null: expected \"min\", \"max\", \"nan\" or an integer",
                            ));
                        }
                    },
                    DeValue::Integer(_) => Null::Value(i128::from(self.integer(v, "null")?)),
                    _ => {
                        return Err(self.error(
                            &v.span(),
                            "null: expected \"min\", \"max\", \"nan\" or an integer",
                        ));
                    }
                };
                let fits = match (null, ty) {
                    (Null::Min | Null::Max, t) => matches!(t, Type::Unsigned(_) | Type::Signed(_)),
                    (Null::NaN, t) => matches!(t, Type::Float(_) | Type::BFloat16),
                    (Null::Value(_), t) => t.is_number() || t == Type::Bool,
                };
                if !fits {
                    return Err(
                        self.error(&v.span(), format!("null: does not fit a {}", type_name(ty)))
                    );
                }
                // A sentinel the type cannot hold would never match.
                if let (Null::Value(value), Some((low, high))) = (null, integer_range(ty))
                    && !(low..=high).contains(&value)
                {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "null: a {} holds {low} to {high}, not {value}",
                            type_name(ty)
                        ),
                    ));
                }
                Ok(null)
            })
            .transpose()?;

        let meaning = self.meaning(&keys, ty)?;
        let file = keys
            .get("file")
            .map(|v| {
                if part != Part::Records || layout == Layout::Rows {
                    return Err(self.error(
                        &v.span(),
                        "file: only a record field of layout = \"columns\" has a file of its own",
                    ));
                }
                let file = self.string(v, "file")?;
                if !is_file_name(&file) {
                    return Err(self.error(
                        &v.span(),
                        "file: expected the name of a file in the directory, such as px.dat",
                    ));
                }
                Ok(file)
            })
            .transpose()?;
        if let (Some(_), Some(v)) = (&column_at, keys.get("file")) {
            return Err(self.error(&v.span(), "file: a column at an offset is in the one file"));
        }
        // Its name names its file.
        if layout == Layout::Columns
            && part == Part::Records
            && file.is_none()
            && column_at.is_none()
            && let Some(name) = &name
            && !is_file_name(name)
        {
            return Err(self.error(
                &keys["name"].span(),
                "name: names the column's file, so it cannot hold a path; give the file with file = \"...\"",
            ));
        }
        let encoding = keys
            .get("encoding")
            .map(|v| {
                if !ty.is_text() {
                    return Err(self.error(&v.span(), "encoding: is for str and strz"));
                }
                match self.string(v, "encoding")?.as_str() {
                    "utf8" | "utf-8" | "ascii" => Ok(Encoding::Utf8),
                    "latin1" | "iso-8859-1" => Ok(Encoding::Latin1),
                    "utf16le" | "utf-16le" => Ok(Encoding::Utf16Le),
                    "utf16be" | "utf-16be" => Ok(Encoding::Utf16Be),
                    _ => Err(self.error(
                        &v.span(),
                        "encoding: expected utf8, latin1, utf16le or utf16be",
                    )),
                }
            })
            .transpose()?
            .unwrap_or_default();
        let delta = keys
            .get("delta")
            .map(|v| {
                if !ty.is_integer() || count.is_some() {
                    return Err(self.error(&v.span(), "delta: is for one integer a record"));
                }
                match v.get_ref() {
                    DeValue::Boolean(true) => Ok(Delta::All),
                    DeValue::Boolean(false) => Ok(Delta::None),
                    DeValue::String(s) if s.as_ref() == "block" => Ok(Delta::Block),
                    _ => Err(self.error(&v.span(), "delta: expected true or \"block\"")),
                }
            })
            .transpose()?
            .unwrap_or_default();
        if delta != Delta::None && layout == Layout::Columns {
            return Err(self.error(
                &keys["delta"].span(),
                "delta: is for records, not column files",
            ));
        }
        let bits = match keys.get("bits") {
            None => Vec::new(),
            Some(v) => self.bits(v, ty, count.is_some())?,
        };
        let string_at = keys
            .get("string_at")
            .map(|v| {
                if !matches!(ty, Type::Unsigned(_)) || meaning != Meaning::Plain || count.is_some()
                {
                    return Err(self.error(
                        &v.span(),
                        "string_at: is for an unsigned offset, one a record",
                    ));
                }
                self.string(v, "string_at")
            })
            .transpose()?;
        let lookup = keys
            .get("lookup")
            .map(|v| {
                if !matches!(ty, Type::Unsigned(_) | Type::Signed(_)) || meaning != Meaning::Plain {
                    return Err(self.error(&v.span(), "lookup: is for a plain integer index"));
                }
                let table = self.table(v, "lookup")?;
                let inner = self.entries(table, "lookup", &["file", "format"])?;
                let Some(f) = inner.get("file") else {
                    return Err(self.error(&v.span(), "lookup: missing `file`"));
                };
                let file = self.string(f, "file")?;
                if file.trim().is_empty() || Path::new(&file).is_absolute() {
                    return Err(self.error(
                        &f.span(),
                        "file: expected a path relative to the data, such as ../sym",
                    ));
                }
                let format = match inner.get("format") {
                    None => LookupFormat::Lines,
                    Some(fv) => {
                        let text = self.string(fv, "format")?;
                        match text.as_str() {
                            "lines" => LookupFormat::Lines,
                            "nul" => LookupFormat::Nul,
                            _ => match text
                                .strip_prefix("str:")
                                .and_then(|n| n.parse::<u64>().ok())
                            {
                                Some(n) if (1..=MAX_SIZE).contains(&n) => LookupFormat::Fixed(n),
                                _ => {
                                    return Err(self.error(
                                        &fv.span(),
                                        "format: expected lines, nul or str:N",
                                    ));
                                }
                            },
                        }
                    }
                };
                Ok(Lookup { file, format })
            })
            .transpose()?;
        if lookup.is_some() && string_at.is_some() {
            return Err(self.error(
                &keys["lookup"].span(),
                "lookup: a field takes one of lookup and string_at",
            ));
        }
        Ok(Field {
            name,
            ty,
            size,
            endian,
            meaning,
            null,
            count,
            flatten,
            file,
            at: column_at,
            encoding,
            delta,
            bits,
            group,
            string_at,
            lookup,
        })
    }

    /// A value a field must hold: an integer for an integer field, text for text.
    fn expected(
        &self,
        value: &Value<'_>,
        target: &Field,
        what: &str,
    ) -> Result<Expected, SpecError> {
        match value.get_ref() {
            DeValue::Integer(_) if target.ty.is_integer() && target.meaning == Meaning::Plain => {
                Ok(Expected::Int(i128::from(self.integer(value, what)?)))
            }
            DeValue::String(s) if target.ty.is_text() => Ok(Expected::Text(s.to_string())),
            _ => Err(self.error(
                &value.span(),
                format!(
                    "{what}: `{}` is a {}; expected a value of that type",
                    target.name.as_deref().unwrap_or("pad"),
                    type_name(target.ty)
                ),
            )),
        }
    }

    /// Bytes written as hex (`"1ACFFC1D"`, `"0x1a cf"`) or as a list of bytes.
    fn hex_bytes(&self, value: &Value<'_>, what: &str) -> Result<Vec<u8>, SpecError> {
        let bad = || {
            self.error(
                &value.span(),
                format!("{what}: expected hex such as \"1ACFFC1D\", or a list of bytes"),
            )
        };
        let bytes = match value.get_ref() {
            DeValue::String(s) => {
                let digits: String = s
                    .trim()
                    .trim_start_matches("0x")
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                // Hex digits only, so every pair is two bytes of the string.
                if digits.is_empty()
                    || !digits.len().is_multiple_of(2)
                    || !digits.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(bad());
                }
                (0..digits.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&digits[i..i + 2], 16).map_err(|_| bad()))
                    .collect::<Result<Vec<u8>, _>>()?
            }
            DeValue::Array(items) => items
                .iter()
                .map(|item| {
                    let byte = self.integer(item, what)?;
                    u8::try_from(byte).map_err(|_| {
                        self.error(&item.span(), format!("{what}: a byte is 0 to 255"))
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(bad()),
        };
        if bytes.is_empty() || bytes.len() > 64 {
            return Err(self.error(&value.span(), format!("{what}: expected 1 to 64 bytes")));
        }
        Ok(bytes)
    }

    fn footer(&self, value: &Value<'_>, header: &[Field]) -> Result<Footer, SpecError> {
        let table = self.table(value, "[footer]")?;
        let keys = self.entries(table, "[footer]", &["fields", "size", "checksum"])?;
        let scopes = Scopes {
            header,
            footer: &[],
        };
        let fields = match keys.get("fields") {
            Some(f) => self.fields(f, Part::Footer, Layout::Rows, &scopes, &[])?,
            None => Vec::new(),
        };
        let width = given_width(&fields);
        let size = match keys.get("size") {
            None => None,
            Some(v) => {
                let n = self.integer(v, "size")?;
                if n < 0 || n as u64 > MAX_SIZE {
                    return Err(self.error(&v.span(), format!("size: expected 0 to {MAX_SIZE}")));
                }
                if width.is_some_and(|w| w > n as u64) {
                    return Err(self.error(
                        &v.span(),
                        format!(
                            "size: the fields take {} bytes, more than {n}",
                            width.unwrap_or(0)
                        ),
                    ));
                }
                Some(n as u64)
            }
        };
        if size.is_none() && width.is_none() {
            return Err(self.error(
                &value.span(),
                "[footer]: read from the end of the file, so its fields' sizes are written in the spec, or give its size",
            ));
        }
        let checksum = keys
            .get("checksum")
            .map(|v| {
                let table = self.table(v, "checksum")?;
                let inner = self.entries(table, "checksum", &["algo", "field"])?;
                let algo = self.algo(inner.get("algo").copied(), v)?;
                let Some(f) = inner.get("field") else {
                    return Err(self.error(
                        &v.span(),
                        "checksum: missing `field`, the footer field holding it",
                    ));
                };
                let field = self.string(f, "field")?;
                let field = field.strip_prefix("footer.").unwrap_or(&field).to_string();
                if !fields
                    .iter()
                    .any(|x| x.name.as_deref() == Some(field.as_str()) && x.ty.is_integer())
                {
                    return Err(self.error(
                        &f.span(),
                        format!("field: no integer footer field named `{field}`"),
                    ));
                }
                Ok((algo, field))
            })
            .transpose()?;
        Ok(Footer {
            fields,
            size,
            checksum,
        })
    }

    fn algo(&self, value: Option<&Value<'_>>, at: &Value<'_>) -> Result<ChecksumAlgo, SpecError> {
        let Some(v) = value else {
            return Err(self.error(&at.span(), "checksum: missing `algo`"));
        };
        ChecksumAlgo::parse(&self.string(v, "algo")?).ok_or_else(|| {
            let names: Vec<&str> = ChecksumAlgo::NAMES.iter().map(|(n, _)| *n).collect();
            self.error(
                &v.span(),
                format!("algo: expected one of {}", names.join(", ")),
            )
        })
    }

    fn sections(&self, value: &Value<'_>, scopes: &Scopes<'_>) -> Result<Vec<Section>, SpecError> {
        let table = self.table(value, "[sections]")?;
        let mut out = Vec::new();
        for (key, v) in table {
            let name: &str = key.get_ref();
            let inner = self.table(v, "section")?;
            let keys = self.entries(inner, "a section", &["offset", "size"])?;
            let (Some(o), Some(s)) = (keys.get("offset"), keys.get("size")) else {
                return Err(self.error(
                    &v.span(),
                    format!("[sections.{name}]: needs offset and size"),
                ));
            };
            let offset = self.amount(o, None, "offset", Part::Header, &[], scopes)?;
            let size = self.amount(s, None, "size", Part::Header, &[], scopes)?;
            out.push(Section {
                name: name.to_string(),
                offset,
                size,
            });
        }
        Ok(out)
    }

    fn records(
        &self,
        value: &Value<'_>,
        variants_value: Option<&Value<'_>>,
        layout: Layout,
        scopes: &Scopes<'_>,
    ) -> Result<Records, SpecError> {
        let table = self.table(value, "[records]")?;
        let keys = self.entries(
            table,
            "[records]",
            &[
                "framing",
                "fields",
                "size",
                "size_adjust",
                "count",
                "length_suffix",
                "align",
                "sync",
                "type",
                "checksum",
                "ring",
                "common",
            ],
        )?;
        let framing = match keys.get("framing") {
            None => Framing::Fixed,
            Some(v) => match self.string(v, "framing")?.as_str() {
                "fixed" => Framing::Fixed,
                "length_prefixed" => Framing::LengthPrefixed,
                "variant" => Framing::Variant,
                "sync" => Framing::Sync,
                "blocks" => {
                    return Err(self.error(
                        &v.span(),
                        "framing: blocks are described under [blocks]; framing is how records sit inside each block",
                    ));
                }
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "framing: expected fixed, length_prefixed, variant or sync",
                    ));
                }
            },
        };
        let common_value = match (keys.get("fields"), keys.get("common")) {
            (Some(_), Some(c)) => {
                return Err(self.error(
                    &c.span(),
                    "[records.common]: give the shared fields here or as `fields`, not both",
                ));
            }
            (Some(f), None) => Some(*f),
            (None, Some(c)) => {
                let t = self.table(c, "[records.common]")?;
                let k = self.entries(t, "[records.common]", &["fields"])?;
                k.get("fields").copied()
            }
            (None, None) => None,
        };
        let mut fields = match common_value {
            Some(f) => self.fields(f, Part::Records, layout, scopes, &[])?,
            None => Vec::new(),
        };
        let mut type_field = None;
        if let Some(t) = keys.get("type") {
            let name =
                match t.get_ref() {
                    DeValue::String(s) => s.to_string(),
                    DeValue::Table(inner) => {
                        let k = self.entries(inner, "type", &["field", "type"])?;
                        let Some(f) = k.get("field") else {
                            return Err(self.error(&t.span(), "type: missing `field`"));
                        };
                        let name = self.string(f, "field")?;
                        if let Some(ty) = k.get("type") {
                            if fields
                                .iter()
                                .any(|x| x.name.as_deref() == Some(name.as_str()))
                            {
                                return Err(self.error(
                                    &ty.span(),
                                    format!("type: `{name}` is already a field; name it alone"),
                                ));
                            }
                            let (ty, endian) = parse_type(&self.string(ty, "type")?)
                                .filter(|(t, _)| {
                                    matches!(t, Type::Unsigned(_) | Type::Signed(_) | Type::Str)
                                })
                                .ok_or_else(|| {
                                    self.error(
                                        &ty.span(),
                                        "type: a type field is an integer, or str with a size",
                                    )
                                })?;
                            if ty == Type::Str {
                                return Err(self.error(
                                    &t.span(),
                                    "type: a str type field goes in the fields, with its size",
                                ));
                            }
                            fields.push(Field::plain(&name, ty, endian));
                        }
                        name
                    }
                    _ => return Err(self.error(
                        &t.span(),
                        "type: expected the name of a common field, or { field = ..., type = ... }",
                    )),
                };
            let Some(target) = fields
                .iter()
                .find(|f| f.name.as_deref() == Some(name.as_str()))
            else {
                return Err(self.error(&t.span(), format!("type: no common field named `{name}`")));
            };
            if !(target.ty.is_integer() || target.ty.is_text()) || target.count.is_some() {
                return Err(self.error(
                    &t.span(),
                    format!("type: `{name}` is not an integer or text field"),
                ));
            }
            type_field = Some(name);
        }
        let mut variants = Vec::new();
        if let Some(v) = variants_value {
            let Some(type_name) = &type_field else {
                return Err(self.error(
                    &v.span(),
                    "[[variants]]: needs [records] type, the field that picks one",
                ));
            };
            let target = fields
                .iter()
                .find(|f| f.name.as_deref() == Some(type_name.as_str()))
                .expect("checked above")
                .clone();
            let DeValue::Array(items) = v.get_ref() else {
                return Err(self.error(&v.span(), "variants: expected [[variants]] tables"));
            };
            let mut seen = std::collections::HashSet::new();
            for item in items {
                let t = self.table(item, "variant")?;
                let k = self.entries(
                    t,
                    "a variant",
                    &["name", "when", "fields", "size", "size_adjust"],
                )?;
                let Some(n) = k.get("name") else {
                    return Err(self.error(&item.span(), "variant: missing `name`"));
                };
                let name = self.string(n, "name")?;
                if name.trim().is_empty() || !seen.insert(name.clone()) {
                    return Err(self.error(
                        &n.span(),
                        format!("name: `{name}` is empty or a second variant's"),
                    ));
                }
                let Some(w) = k.get("when") else {
                    return Err(self.error(
                        &item.span(),
                        "variant: missing `when`, the type value that picks it",
                    ));
                };
                let when = match w.get_ref() {
                    DeValue::Array(values) => values
                        .iter()
                        .map(|x| self.expected(x, &target, "when"))
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => vec![self.expected(w, &target, "when")?],
                };
                let vfields = match k.get("fields") {
                    Some(f) => self.fields(f, Part::Records, layout, scopes, &fields)?,
                    None => Vec::new(),
                };
                let mut every = fields.clone();
                every.extend(vfields.iter().cloned());
                let size = k
                    .get("size")
                    .map(|s| {
                        self.amount(
                            s,
                            k.get("size_adjust").copied(),
                            "size",
                            Part::Records,
                            &every,
                            scopes,
                        )
                    })
                    .transpose()?;
                if let (Some(Amount::Given(size)), Some(sum)) = (&size, given_width(&every))
                    && *size < sum
                {
                    return Err(self.error(
                        &k["size"].span(),
                        format!("size: the fields take {sum} bytes, more than {size}"),
                    ));
                }
                variants.push(Variant {
                    name,
                    when,
                    fields: vfields,
                    size,
                });
            }
            // A name two variants share is one column, so it has one type.
            let mut by_name: BTreeMap<String, &Field> = BTreeMap::new();
            for variant in &variants {
                for f in &variant.fields {
                    let Some(n) = &f.name else { continue };
                    if let Some(other) = by_name.get(n)
                        && (other.ty != f.ty
                            || other.meaning != f.meaning
                            || other.count != f.count
                            || other.flatten != f.flatten
                            || other.bits != f.bits
                            || other.group != f.group
                            || other.encoding != f.encoding)
                    {
                        return Err(self.error(
                            &v.span(),
                            format!("variants: `{n}` is a different field in two variants; one column has one type, so name them apart"),
                        ));
                    }
                    by_name.insert(n.clone(), f);
                }
            }
            if fields
                .iter()
                .filter_map(|f| f.name.as_deref())
                .any(|n| n == "type")
                || by_name.contains_key("type")
            {
                return Err(self.error(&v.span(), "variants: the variant's name is shown in a column named `type`, so no field may be named that"));
            }
        } else if type_field.is_some() {
            return Err(self.error(
                &keys["type"].span(),
                "type: picks one of the [[variants]], and there are none",
            ));
        }
        if fields.is_empty() && variants.is_empty() {
            return Err(self.error(&value.span(), "[records]: missing `fields`"));
        }
        if !fields.iter().any(|f| f.name.is_some()) && variants.is_empty() {
            return Err(self.error(&value.span(), "fields: a record needs a named field"));
        }
        let size = keys
            .get("size")
            .map(|s| {
                self.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Records,
                    &fields,
                    scopes,
                )
            })
            .transpose()?;
        if matches!(size, Some(Amount::Rest)) {
            return Err(self.error(&keys["size"].span(), "size: expected a number or a field"));
        }
        let count = keys
            .get("count")
            .map(|c| self.amount(c, None, "count", Part::Header, &[], scopes))
            .transpose()?;
        let length_suffix = keys
            .get("length_suffix")
            .map(|v| self.boolean(v, "length_suffix"))
            .transpose()?
            .unwrap_or(false);
        let align = match keys.get("align") {
            None => 1,
            Some(v) => {
                let n = self.integer(v, "align")?;
                if !(1..=65536).contains(&n) {
                    return Err(self.error(&v.span(), "align: expected 1 to 65536"));
                }
                n as u64
            }
        };
        let sync = keys
            .get("sync")
            .map(|v| self.hex_bytes(v, "sync"))
            .transpose()?
            .unwrap_or_default();
        let ring = keys
            .get("ring")
            .map(|v| self.amount(v, None, "ring", Part::Header, &[], scopes))
            .transpose()?;
        let record_size = matches!(size, Some(Amount::Record { .. }));
        let at = |key: &str| keys.get(key).map_or(value.span(), |v| v.span());
        match framing {
            Framing::LengthPrefixed if !record_size => {
                return Err(self.error(&at("size"), "framing = \"length_prefixed\": size names the field holding each record's length, such as size = \"len\""));
            }
            Framing::Variant if variants.is_empty() => {
                return Err(self.error(
                    &at("framing"),
                    "framing = \"variant\": needs [[variants]] and a type field",
                ));
            }
            Framing::Sync if sync.is_empty() => {
                return Err(self.error(&at("framing"), "framing = \"sync\": needs sync, the marker each record starts with, such as sync = \"1ACFFC1D\""));
            }
            Framing::Fixed | Framing::Variant if record_size => {
                return Err(self.error(&at("size"), "size: a record whose size is in its own field needs framing = \"length_prefixed\""));
            }
            _ => {}
        }
        if framing != Framing::Sync && !sync.is_empty() {
            return Err(self.error(&at("sync"), "sync: goes with framing = \"sync\""));
        }
        if length_suffix && !record_size {
            return Err(self.error(
                &at("length_suffix"),
                "length_suffix: repeats a length read from the record; give size = \"len\"",
            ));
        }
        let checksum = keys
            .get("checksum")
            .map(|v| {
                let table = self.table(v, "checksum")?;
                let inner = self.entries(table, "checksum", &["algo", "field", "from", "to"])?;
                let algo = self.algo(inner.get("algo").copied(), v)?;
                let named = |key: &str| -> Result<Option<String>, SpecError> {
                    let Some(x) = inner.get(key) else {
                        return Ok(None);
                    };
                    let name = self.string(x, key)?;
                    let known = fields
                        .iter()
                        .chain(variants.iter().flat_map(|v| &v.fields))
                        .any(|f| f.name.as_deref() == Some(name.as_str()));
                    if !known {
                        return Err(
                            self.error(&x.span(), format!("{key}: no record field named `{name}`"))
                        );
                    }
                    Ok(Some(name))
                };
                let Some(field) = named("field")? else {
                    return Err(
                        self.error(&v.span(), "checksum: missing `field`, the field holding it")
                    );
                };
                let holder = fields
                    .iter()
                    .chain(variants.iter().flat_map(|v| &v.fields))
                    .find(|f| f.name.as_deref() == Some(field.as_str()))
                    .expect("checked");
                if !matches!(holder.ty, Type::Unsigned(_) | Type::Signed(_)) {
                    return Err(self.error(&v.span(), "checksum: its field is an integer"));
                }
                Ok(Checksum {
                    algo,
                    field,
                    from: named("from")?,
                    to: named("to")?,
                })
            })
            .transpose()?;
        if checksum.is_some()
            && fields
                .iter()
                .chain(variants.iter().flat_map(|v| &v.fields))
                .any(|f| f.name.as_deref() == Some("checksum_ok"))
        {
            return Err(self.error(&value.span(), "checksum: its result is a column named `checksum_ok`, so no field may be named that"));
        }
        Ok(Records {
            framing,
            fields,
            size,
            count,
            length_suffix,
            align,
            sync,
            type_field,
            variants,
            checksum,
            ring,
        })
    }

    fn blocks(&self, value: &Value<'_>, scopes: &Scopes<'_>) -> Result<Blocks, SpecError> {
        let table = self.table(value, "[blocks]")?;
        let keys = self.entries(
            table,
            "[blocks]",
            &[
                "header",
                "size",
                "size_adjust",
                "compression",
                "records",
                "uncompressed",
                "index",
            ],
        )?;
        let header = match keys.get("header") {
            Some(h) => self.fields(h, Part::Header, Layout::Rows, scopes, &[])?,
            None => Vec::new(),
        };
        let Some(s) = keys.get("size") else {
            return Err(self.error(&value.span(), "[blocks]: missing `size`, the bytes after each block's header, such as size = \"clen\""));
        };
        let size = self.amount(
            s,
            keys.get("size_adjust").copied(),
            "size",
            Part::Records,
            &header,
            scopes,
        )?;
        if matches!(size, Amount::Rest) {
            return Err(self.error(&s.span(), "size: expected a number or a block header field"));
        }
        let header_field = |key: &str| -> Result<Option<String>, SpecError> {
            let Some(v) = keys.get(key) else {
                return Ok(None);
            };
            let name = self.string(v, key)?;
            if !header.iter().any(|f| {
                f.name.as_deref() == Some(name.as_str())
                    && f.ty.is_integer()
                    && f.meaning == Meaning::Plain
            }) {
                return Err(self.error(
                    &v.span(),
                    format!("{key}: no plain integer block header field named `{name}`"),
                ));
            }
            Ok(Some(name))
        };
        let records = header_field("records")?;
        let uncompressed = header_field("uncompressed")?;
        let codec = match keys.get("compression") {
            None => Codec::Fixed(Compression::None),
            Some(v) => match v.get_ref() {
                DeValue::String(name) => Codec::Fixed(self.compression(v, name)?),
                DeValue::Table(inner) => {
                    let k = self.entries(inner, "compression", &["field", "values"])?;
                    let (Some(f), Some(vals)) = (k.get("field"), k.get("values")) else {
                        return Err(self.error(&v.span(), "compression: expected { field = \"codec\", values = { 0 = \"none\", 1 = \"zstd\" } }"));
                    };
                    let field = self.string(f, "field")?;
                    if !header
                        .iter()
                        .any(|x| x.name.as_deref() == Some(field.as_str()) && x.ty.is_integer())
                    {
                        return Err(self.error(
                            &f.span(),
                            format!("field: no integer block header field named `{field}`"),
                        ));
                    }
                    let mut values = BTreeMap::new();
                    for (code, name) in self.table(vals, "values")? {
                        let text: &str = code.get_ref();
                        let code_value: i64 = text.parse().map_err(|_| {
                            self.error(
                                &code.span(),
                                format!("values: `{text}` is not a whole number"),
                            )
                        })?;
                        let DeValue::String(n) = name.get_ref() else {
                            return Err(self.error(&name.span(), "values: expected a codec's name"));
                        };
                        values.insert(code_value, self.compression(name, n)?);
                    }
                    Codec::ByField { field, values }
                }
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "compression: expected a codec's name or { field, values }",
                    ));
                }
            },
        };
        let lz4_block = match &codec {
            Codec::Fixed(c) => *c == Compression::Lz4Block,
            Codec::ByField { values, .. } => values.values().any(|c| *c == Compression::Lz4Block),
        };
        if lz4_block && uncompressed.is_none() {
            return Err(self.error(&value.span(), "compression: lz4_block needs uncompressed, the header field with each block's decompressed size"));
        }
        let index = keys
            .get("index")
            .map(|v| {
                let t = self.table(v, "index")?;
                let k = self.entries(t, "index", &["at", "count", "fields"])?;
                let (Some(a), Some(c), Some(f)) = (k.get("at"), k.get("count"), k.get("fields"))
                else {
                    return Err(self.error(&v.span(), "index: needs at, count and fields"));
                };
                let at = self.amount(a, None, "at", Part::Header, &[], scopes)?;
                let count = self.amount(c, None, "count", Part::Header, &[], scopes)?;
                let fields = self.fields(f, Part::Header, Layout::Rows, scopes, &[])?;
                if given_width(&fields).is_none() {
                    return Err(self.error(
                        &f.span(),
                        "fields: an index entry's sizes are written in the spec",
                    ));
                }
                if !fields
                    .iter()
                    .any(|x| x.name.as_deref() == Some("offset") && x.ty.is_integer())
                {
                    return Err(self.error(
                        &f.span(),
                        "fields: an index entry needs an integer `offset`, where its block starts",
                    ));
                }
                Ok(BlockIndex { at, count, fields })
            })
            .transpose()?;
        Ok(Blocks {
            header,
            size,
            codec,
            records,
            uncompressed,
            index,
        })
    }

    fn compression(&self, at: &Value<'_>, name: &str) -> Result<Compression, SpecError> {
        Compression::parse(name).ok_or_else(|| {
            let names: Vec<&str> = Compression::NAMES.iter().map(|(n, _)| *n).collect();
            self.error(
                &at.span(),
                format!("compression: expected one of {}", names.join(", ")),
            )
        })
    }

    fn capture(&self, value: &Value<'_>) -> Result<Capture, SpecError> {
        let table = self.table(value, "[capture]")?;
        // pcap or pcapng is told by the file's magic, so there is no key for it.
        let keys = self.entries(table, "[capture]", &["header", "count", "time"])?;
        let header = match keys.get("header") {
            Some(h) => self.fields(h, Part::Records, Layout::Rows, &Scopes::default(), &[])?,
            None => Vec::new(),
        };
        let count = keys
            .get("count")
            .map(|v| {
                let name = self.string(v, "count")?;
                if !header
                    .iter()
                    .any(|f| f.name.as_deref() == Some(name.as_str()) && f.ty.is_integer())
                {
                    return Err(self.error(
                        &v.span(),
                        format!("count: no integer payload header field named `{name}`"),
                    ));
                }
                Ok(name)
            })
            .transpose()?;
        let time = keys
            .get("time")
            .map(|v| {
                let name = self.string(v, "time")?;
                if name.trim().is_empty() || name.contains('.') {
                    return Err(self.error(&v.span(), "time: expected a column name"));
                }
                Ok(name)
            })
            .transpose()?;
        Ok(Capture {
            header,
            count,
            time,
        })
    }

    fn files(&self, value: &Value<'_>) -> Result<Files, SpecError> {
        let table = self.table(value, "[files]")?;
        let keys = self.entries(table, "[files]", &["path"])?;
        let Some(p) = keys.get("path") else {
            return Err(self.error(
                &value.span(),
                "[files]: missing `path`, such as path = \"{date:%Y%m%d}/{venue}/trades.bin\"",
            ));
        };
        let pattern = self.string(p, "path")?;
        let parts =
            path_parts(&pattern).map_err(|e| self.error(&p.span(), format!("path: {e}")))?;
        Ok(Files { pattern, parts })
    }

    /// What the columns layout allows: fixed values, one file each or all at offsets in
    /// one file.
    fn check_columns(&self, value: &Value<'_>, records: &Records) -> Result<(), SpecError> {
        if records.size.is_some() {
            return Err(self.error(
                &value.span(),
                "size: each column holds one field, so layout = \"columns\" takes no record size",
            ));
        }
        if records.framing != Framing::Fixed || records.checksum.is_some() || records.ring.is_some()
        {
            return Err(self.error(
                &value.span(),
                "layout = \"columns\": values are fixed, with no framing, checksum or ring",
            ));
        }
        let at = records.fields.iter().filter(|f| f.at.is_some()).count();
        if at != 0 && at != records.fields.len() {
            return Err(self.error(
                &value.span(),
                "offset: in one file, every column gives where it starts",
            ));
        }
        for field in &records.fields {
            let said = if field.ty == Type::Pad {
                Some("pad: layout = \"columns\" has no bytes between fields to skip")
            } else if field.flatten {
                Some("flatten: a column file holds one column; leave the values an Array")
            } else if matches!(field.ty, Type::Strz | Type::VarU | Type::VarS | Type::Group)
                || !field.bits.is_empty()
                || field.string_at.is_some()
            {
                Some("layout = \"columns\": a column's values are fixed-width")
            } else if field.size.as_ref().is_some_and(|s| !s.is_fixed())
                || field.count.as_ref().is_some_and(|s| !s.is_fixed())
            {
                Some("layout = \"columns\": a column's values are all one size")
            } else {
                None
            };
            if let Some(said) = said {
                return Err(self.error(&value.span(), said));
            }
        }
        Ok(())
    }

    /// An integer's bit fields.
    fn bits(&self, value: &Value<'_>, ty: Type, counted: bool) -> Result<Vec<BitField>, SpecError> {
        let total = match ty {
            Type::Unsigned(n) | Type::Signed(n) => u32::from(n) * 8,
            Type::VarU | Type::VarS => 64,
            _ => return Err(self.error(&value.span(), "bits: are for an integer field")),
        };
        if counted {
            return Err(self.error(&value.span(), "bits: are for one integer a record"));
        }
        let DeValue::Array(items) = value.get_ref() else {
            return Err(self.error(
                &value.span(),
                "bits: expected a list, such as [{ name = \"valid\", bit = 0 }]",
            ));
        };
        let mut out = Vec::new();
        for item in items {
            let table = self.table(item, "bit")?;
            let keys = self.entries(table, "a bit field", &["name", "bit", "width", "enum"])?;
            let Some(n) = keys.get("name") else {
                return Err(self.error(&item.span(), "bit: missing `name`"));
            };
            let name = self.string(n, "name")?;
            if name.trim().is_empty() || name.contains('.') {
                return Err(self.error(&n.span(), "name: must not be empty or contain `.`"));
            }
            let Some(b) = keys.get("bit") else {
                return Err(self.error(
                    &item.span(),
                    "bit: missing `bit`, the lowest bit, 0 the least significant",
                ));
            };
            let bit = self.integer(b, "bit")?;
            let width = keys
                .get("width")
                .map(|w| self.integer(w, "width"))
                .transpose()?
                .unwrap_or(1);
            if bit < 0 || width < 1 || bit + width > i64::from(total) {
                return Err(self.error(
                    &item.span(),
                    format!(
                        "bit: bits {bit} to {} are outside the field's {total}",
                        bit + width - 1
                    ),
                ));
            }
            let labels = keys
                .get("enum")
                .map(|e| {
                    let table = self.table(e, "enum")?;
                    let mut labels = BTreeMap::new();
                    for (code, label) in table {
                        let text: &str = code.get_ref();
                        let code_value: i64 = text.parse().map_err(|_| {
                            self.error(
                                &code.span(),
                                format!("enum: `{text}` is not a whole number"),
                            )
                        })?;
                        labels.insert(code_value, self.string(label, "enum label")?);
                    }
                    Ok::<_, SpecError>(Arc::new(labels))
                })
                .transpose()?;
            out.push(BitField {
                name,
                bit: bit as u32,
                width: width as u32,
                labels,
            });
        }
        Ok(out)
    }

    /// What a field means: at most one of a time, a scale, a linear conversion and an
    /// enum.
    fn meaning(&self, keys: &BTreeMap<&str, &Value<'_>>, ty: Type) -> Result<Meaning, SpecError> {
        let groups: [(&[&str], &str); 4] = [
            (&["time", "of_day", "date", "epoch"], "time"),
            (&["scale"], "scale"),
            (&["factor", "offset"], "factor"),
            (&["enum"], "enum"),
        ];
        let used: Vec<(&str, Range<usize>)> = groups
            .iter()
            .filter_map(|(group, said)| {
                group
                    .iter()
                    .find_map(|k| keys.get(k))
                    .map(|v| (*said, v.span()))
            })
            .collect();
        if used.len() > 1 {
            return Err(self.error(
                &used[1].1,
                format!(
                    "a field takes one of time (or date), scale, factor and enum, not {} and {}",
                    used[0].0, used[1].0
                ),
            ));
        }
        let Some((kind, span)) = used.into_iter().next() else {
            return Ok(Meaning::Plain);
        };
        let integers_only = |what: &str| {
            if ty.is_integer() {
                Ok(())
            } else {
                Err(self.error(
                    &span,
                    format!("{what} is for integer types, not {}", type_name(ty)),
                ))
            }
        };
        match kind {
            "scale" => {
                integers_only("scale")?;
                let v = keys["scale"];
                let scale = self.integer(v, "scale")?;
                if !(0..=38).contains(&scale) {
                    return Err(self.error(&v.span(), "scale: expected 0 to 38"));
                }
                Ok(Meaning::Scale(scale as u32))
            }
            "factor" => {
                if !ty.is_number() {
                    return Err(self.error(
                        &span,
                        format!("factor and offset are for numbers, not {}", type_name(ty)),
                    ));
                }
                let factor = keys
                    .get("factor")
                    .map(|v| self.number(v, "factor"))
                    .transpose()?
                    .unwrap_or(1.0);
                let offset = keys
                    .get("offset")
                    .map(|v| self.number(v, "offset"))
                    .transpose()?
                    .unwrap_or(0.0);
                Ok(Meaning::Linear { factor, offset })
            }
            "enum" => {
                integers_only("enum")?;
                let v = keys["enum"];
                let table = self.table(v, "enum")?;
                let mut labels = BTreeMap::new();
                for (code, label) in table {
                    let text: &str = code.get_ref();
                    let code_value: i64 = text.parse().map_err(|_| {
                        self.error(
                            &code.span(),
                            format!("enum: `{text}` is not a whole number"),
                        )
                    })?;
                    labels.insert(code_value, self.string(label, "enum label")?);
                }
                Ok(Meaning::Enum(Arc::new(labels)))
            }
            _ => self.time_meaning(keys, ty, &span),
        }
    }

    fn time_meaning(
        &self,
        keys: &BTreeMap<&str, &Value<'_>>,
        ty: Type,
        span: &Range<usize>,
    ) -> Result<Meaning, SpecError> {
        let unit = keys
            .get("time")
            .map(|v| match self.string(v, "time")?.as_str() {
                "days" => Ok(TimeUnitSpec::Days),
                "s" => Ok(TimeUnitSpec::Seconds),
                "ms" => Ok(TimeUnitSpec::Millis),
                "us" => Ok(TimeUnitSpec::Micros),
                "ns" => Ok(TimeUnitSpec::Nanos),
                _ => Err(self.error(&v.span(), "time: expected days, s, ms, us or ns")),
            })
            .transpose()?;
        let of_day = keys
            .get("of_day")
            .map(|v| self.boolean(v, "of_day"))
            .transpose()?
            .unwrap_or(false);
        let date = keys
            .get("date")
            .map(|v| Ok::<_, SpecError>((self.string(v, "date")?, v.span())))
            .transpose()?;
        if let Some(v) = keys.get("epoch")
            && (unit.is_none() || of_day)
        {
            return Err(self.error(&v.span(), "epoch goes with time, and not with of_day"));
        }
        match (unit, of_day, date) {
            (None, false, Some((date, at))) => {
                if date != "yyyymmdd" {
                    return Err(self.error(
                        &at,
                        "date: expected \"yyyymmdd\" (or, with of_day, a header field)",
                    ));
                }
                if !ty.is_integer() {
                    return Err(self.error(
                        &at,
                        format!("date is for integer types, not {}", type_name(ty)),
                    ));
                }
                Ok(Meaning::Yyyymmdd)
            }
            (None, _, _) => Err(self.error(
                span,
                "of_day goes with time = \"s\", \"ms\", \"us\" or \"ns\"",
            )),
            (Some(unit), true, date) => {
                if !ty.is_integer() {
                    return Err(self.error(
                        span,
                        format!("of_day is for integer types, not {}", type_name(ty)),
                    ));
                }
                if unit == TimeUnitSpec::Days {
                    return Err(self.error(span, "of_day counts s, ms, us or ns since midnight"));
                }
                let date = date
                    .map(|(date, at)| {
                        let field = date.strip_prefix("header.").ok_or_else(|| {
                            self.error(&at, "date: with of_day, expected a header field such as header.trade_date")
                        })?;
                        Ok::<_, SpecError>(field.to_string())
                    })
                    .transpose()?;
                Ok(Meaning::TimeOfDay { unit, date })
            }
            (Some(unit), false, date) => {
                if let Some((_, at)) = date {
                    return Err(self.error(&at, "date: goes with of_day, or alone as \"yyyymmdd\""));
                }
                if !ty.is_number() {
                    return Err(
                        self.error(span, format!("time is for numbers, not {}", type_name(ty)))
                    );
                }
                let epoch_ns = keys
                    .get("epoch")
                    .map(|e| self.epoch(e))
                    .transpose()?
                    .unwrap_or(0);
                Ok(Meaning::Time { unit, epoch_ns })
            }
        }
    }

    /// An epoch: a TOML date or date-time, or one written as a string.
    fn epoch(&self, value: &Value<'_>) -> Result<i64, SpecError> {
        let text = match value.get_ref() {
            DeValue::Datetime(dt) => dt.to_string(),
            DeValue::String(s) => s.to_string(),
            _ => {
                return Err(self.error(&value.span(), "epoch: expected a date such as 2000-01-01"));
            }
        };
        parse_epoch(&text).ok_or_else(|| {
            self.error(
                &value.span(),
                "epoch: expected a date such as 2000-01-01 or a date-time such as 2000-01-01T00:00:00Z",
            )
        })
    }
}

impl Field {
    /// A named field of `ty`, as stored.
    pub fn plain(name: &str, ty: Type, endian: Option<Endian>) -> Self {
        Self {
            name: Some(name.to_string()),
            ty,
            size: None,
            endian,
            meaning: Meaning::Plain,
            null: None,
            count: None,
            flatten: false,
            file: None,
            at: None,
            encoding: Encoding::Utf8,
            delta: Delta::None,
            bits: Vec::new(),
            group: Vec::new(),
            string_at: None,
            lookup: None,
        }
    }

    /// Whether the reader of fixed records reads it as it is: one value or an Array
    /// of them, of a size known before a record is read, and nothing derived.
    pub fn is_fixed_width(&self) -> bool {
        self.ty.width().is_some() || matches!(self.ty, Type::Str | Type::Bytes | Type::Pad)
    }
}

/// Every field of the records: the common ones, then each variant's.
pub fn all_fields(records: &Records) -> impl Iterator<Item = &Field> {
    records
        .fields
        .iter()
        .chain(records.variants.iter().flat_map(|v| &v.fields))
}

/// The parts `{name}` and `{name:%Y%m%d}` of a `[files]` path, in order.
pub fn path_parts(pattern: &str) -> Result<Vec<PathPart>, String> {
    let mut parts: Vec<PathPart> = Vec::new();
    let mut rest = pattern;
    if pattern.trim().is_empty()
        || Path::new(pattern).is_absolute()
        || pattern.split('/').any(|c| c == ".." || c.is_empty())
    {
        return Err(
            "expected a path relative to the directory, such as {date:%Y%m%d}/trades.bin".into(),
        );
    }
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let close = after.find('}').ok_or("a `{` without its `}`")?;
        let inside = &after[..close];
        let (name, date) = match inside.split_once(':') {
            Some((name, format)) => (name, Some(format.to_string())),
            None => (inside, None),
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "`{{{inside}}}`: a part is named with letters, digits and `_`"
            ));
        }
        if inside.contains('/') {
            return Err(format!(
                "`{{{inside}}}`: a part stays inside one directory's name"
            ));
        }
        if let Some(format) = &date
            && (format.is_empty()
                || chrono::format::StrftimeItems::new(format)
                    .any(|i| matches!(i, chrono::format::Item::Error)))
        {
            return Err(format!(
                "`{{{inside}}}`: `{format}` is not a date format such as %Y%m%d"
            ));
        }
        if parts.iter().any(|p| p.name == name) {
            return Err(format!("`{name}` is named twice"));
        }
        parts.push(PathPart {
            name: name.to_string(),
            date,
        });
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        return Err("a `}` without its `{`".into());
    }
    Ok(parts)
}

/// The columns a field shows as: none for `pad`, `name_0`... when flattened.
fn output_names(field: &Field) -> Vec<String> {
    let Some(name) = &field.name else {
        return Vec::new();
    };
    let mut names = match (&field.count, field.flatten) {
        (Some(Amount::Given(n)), true) => (0..*n).map(|i| format!("{name}_{i}")).collect(),
        _ => vec![name.clone()],
    };
    names.extend(field.bits.iter().map(|b| b.name.clone()));
    names
}

/// The smallest and largest value an integer (or bool) type holds.
fn integer_range(ty: Type) -> Option<(i128, i128)> {
    let bits = match ty {
        Type::Unsigned(n) | Type::Signed(n) => u32::from(n) * 8,
        Type::Bool => 8,
        _ => return None,
    };
    Some(match ty {
        Type::Signed(_) => (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1),
        _ => (0, (1i128 << bits) - 1),
    })
}

/// Whether `name` is one file's name, with no directory in it.
fn is_file_name(name: &str) -> bool {
    let mut parts = Path::new(name).components();
    matches!(
        (parts.next(), parts.next()),
        (Some(std::path::Component::Normal(_)), None)
    ) && !name.contains(['/', '\\'])
}

/// A type name and the byte order its suffix asks for.
fn parse_type(text: &str) -> Option<(Type, Option<Endian>)> {
    match text {
        "str" => return Some((Type::Str, None)),
        "strz" => return Some((Type::Strz, None)),
        "vu" => return Some((Type::VarU, None)),
        "vs" => return Some((Type::VarS, None)),
        "bf2" => return Some((Type::BFloat16, None)),
        "bf2le" => return Some((Type::BFloat16, Some(Endian::Little))),
        "bf2be" => return Some((Type::BFloat16, Some(Endian::Big))),
        "bytes" => return Some((Type::Bytes, None)),
        "pad" => return Some((Type::Pad, None)),
        "bool" => return Some((Type::Bool, None)),
        _ => {}
    }
    let (base, endian) = if let Some(base) = text.strip_suffix("le") {
        (base, Some(Endian::Little))
    } else if let Some(base) = text.strip_suffix("be") {
        (base, Some(Endian::Big))
    } else {
        (text, None)
    };
    let mut chars = base.chars();
    let kind = chars.next()?;
    let digits = chars.as_str();
    if digits.len() != 1 {
        return None;
    }
    let width: u8 = digits.parse().ok()?;
    let ty = match (kind, width) {
        ('u', 1..=8) => Type::Unsigned(width),
        ('s', 1..=8) => Type::Signed(width),
        ('f', 2 | 4 | 8) => Type::Float(width),
        _ => return None,
    };
    Some((ty, endian))
}

fn type_name(ty: Type) -> String {
    match ty {
        Type::Unsigned(n) => format!("u{n}"),
        Type::Signed(n) => format!("s{n}"),
        Type::Float(n) => format!("f{n}"),
        Type::Bool => "bool".into(),
        Type::BFloat16 => "bf2".into(),
        Type::Str => "str".into(),
        Type::Strz => "strz".into(),
        Type::Bytes => "bytes".into(),
        Type::Pad => "pad".into(),
        Type::VarU => "vu".into(),
        Type::VarS => "vs".into(),
        Type::Group => "group".into(),
    }
}

/// Nanoseconds from the Unix epoch to `text`: a date, or an RFC 3339 date-time (a
/// date-time without an offset is UTC).
fn parse_epoch(text: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};
    let at = if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        dt.naive_utc()
    } else if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f") {
        dt
    } else if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
        dt
    } else {
        NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .ok()?
            .and_hms_opt(0, 0, 0)?
    };
    at.and_utc().timestamp_nanos_opt()
}

/// What a spec name may be: namespaced, as `acme.l2feed` is, so it cannot be taken for
/// a built-in format.
pub fn is_spec_name(name: &str) -> bool {
    name.contains('.')
        && !name.starts_with('.')
        && !name.ends_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

impl Spec {
    /// Read a spec from its text. `path` names it in errors.
    pub fn parse(text: &str, path: Option<&Path>) -> Result<Self, SpecError> {
        let reader = Reader { text, path };
        let document = DeTable::parse(text).map_err(|e| {
            let (line, column) = e
                .span()
                .map_or((0, 0), |span| line_column(text, span.start));
            SpecError {
                path: path.map(Path::to_path_buf),
                line,
                column,
                message: e.message().to_string(),
            }
        })?;
        let kind = document
            .get_ref()
            .iter()
            .find(|(key, _)| {
                let key: &str = key.get_ref();
                key == "kind"
            })
            .map(|(_, v)| v);
        if let Some(v) = kind {
            match reader.string(v, "kind")?.as_str() {
                "binary" => {}
                "delimited" => return Self::parse_delimited(&reader, document.get_ref(), path),
                _ => return Err(reader.error(&v.span(), "kind: expected binary or delimited")),
            }
        }
        let top = reader.entries(
            document.get_ref(),
            "the spec",
            &[
                "name",
                "description",
                "kind",
                "match",
                "endian",
                "layout",
                "header",
                "records",
                "footer",
                "variants",
                "blocks",
                "capture",
                "files",
                "sections",
            ],
        )?;
        let whole = 0..0;
        let name = reader.spec_name(&top)?;
        let description = top
            .get("description")
            .map(|v| reader.string(v, "description"))
            .transpose()?;
        let (endian, endian_auto) = match top.get("endian") {
            None => (Endian::Little, false),
            Some(v) => match reader.string(v, "endian")?.as_str() {
                "le" => (Endian::Little, false),
                "be" => (Endian::Big, false),
                "auto" => (Endian::Little, true),
                _ => return Err(reader.error(&v.span(), "endian: expected le, be or auto")),
            },
        };
        let layout = match top.get("layout") {
            None => Layout::Rows,
            Some(v) => match reader.string(v, "layout")?.as_str() {
                "rows" => Layout::Rows,
                "columns" => Layout::Columns,
                _ => return Err(reader.error(&v.span(), "layout: expected rows or columns")),
            },
        };

        let mut header = Header::default();
        if let Some(v) = top.get("header") {
            let table = reader.table(v, "[header]")?;
            let keys = reader.entries(table, "[header]", &["fields", "size", "size_adjust"])?;
            if let Some(f) = keys.get("fields") {
                header.fields = reader.fields(f, Part::Header, layout, &Scopes::default(), &[])?;
            }
            if let Some(s) = keys.get("size") {
                let scopes = Scopes {
                    header: &header.fields,
                    footer: &[],
                };
                header.size = Some(reader.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Header,
                    &header.fields,
                    &scopes,
                )?);
            }
        }
        let footer = top
            .get("footer")
            .map(|v| reader.footer(v, &header.fields))
            .transpose()?;
        let footer_fields: &[Field] = footer.as_ref().map_or(&[], |f| &f.fields);
        let scopes = Scopes {
            header: &header.fields,
            footer: footer_fields,
        };

        let MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        } = match top.get("match") {
            Some(v) => reader.match_rules(v, Some(&header.fields))?,
            None => MatchRules::default(),
        };

        if endian_auto && magic.len() < 2 {
            return Err(reader.error(
                &top["endian"].span(),
                "endian = \"auto\" reads the byte order from the magic, so it needs a magic of at least two bytes",
            ));
        }

        let sections = top
            .get("sections")
            .map(|v| reader.sections(v, &scopes))
            .transpose()?
            .unwrap_or_default();

        let Some(records_value) = top.get("records") else {
            return Err(reader.error(&whole, "missing [records], with the fields of one record"));
        };
        let records =
            reader.records(records_value, top.get("variants").copied(), layout, &scopes)?;
        for field in all_fields(&records) {
            if let Some(section) = &field.string_at
                && !sections.iter().any(|s| &s.name == section)
            {
                return Err(reader.error(
                    &records_value.span(),
                    format!("string_at: no section named `{section}` under [sections]"),
                ));
            }
        }
        let blocks = top
            .get("blocks")
            .map(|v| reader.blocks(v, &scopes))
            .transpose()?;
        let capture = top.get("capture").map(|v| reader.capture(v)).transpose()?;
        let files = top.get("files").map(|v| reader.files(v)).transpose()?;

        if let Some(v) = top.get("capture")
            && (blocks.is_some() || top.contains_key("header") || footer.is_some())
        {
            return Err(reader.error(
                &v.span(),
                "[capture]: the capture's own headers frame the payloads; leave out [header], [footer] and [blocks]",
            ));
        }
        let framed = blocks.is_some() || capture.is_some();
        if layout == Layout::Columns
            && let Some(v) = top.get("blocks").or(top.get("files"))
        {
            return Err(reader.error(
                &v.span(),
                "layout = \"columns\" reads values, not blocks or a tree of files",
            ));
        }
        if let Some(ring) = &records.ring {
            let ring_ok = records.framing == Framing::Fixed
                && !framed
                && records.variants.is_empty()
                && matches!(
                    ring,
                    Amount::Header { .. } | Amount::Footer { .. } | Amount::Given(_)
                );
            if !ring_ok {
                return Err(reader.error(
                    &records_value.span(),
                    "ring: is for fixed records, the oldest's index read from the header",
                ));
            }
        }
        if let Some(files) = &files {
            let columns: std::collections::HashSet<String> =
                all_fields(&records).flat_map(output_names).collect();
            if let Some(part) = files.parts.iter().find(|p| columns.contains(&p.name)) {
                return Err(reader.error(
                    &top["files"].span(),
                    format!("path: `{}` is also a field's name", part.name),
                ));
            }
        }

        if layout == Layout::Columns {
            reader.check_columns(records_value, &records)?;
            if let Some(v) = top
                .get("footer")
                .or(top.get("variants"))
                .or(top.get("capture"))
            {
                return Err(reader.error(
                    &v.span(),
                    "layout = \"columns\" holds fixed values: no footer, variants or capture",
                ));
            }
        }
        // Sizes written down are checked now; one that comes from the file is checked
        // when the file is read.
        if let (Some(Amount::Given(size)), Some(sum)) =
            (&records.size, given_width(&records.fields))
            && *size < sum
            && records.variants.is_empty()
        {
            return Err(reader.error(
                &records_value.span(),
                format!("size: the fields take {sum} bytes, more than {size}"),
            ));
        }
        if let (Some(Amount::Given(size)), Some(sum)) = (&header.size, given_width(&header.fields))
            && *size < sum
        {
            return Err(reader.error(
                &records_value.span(),
                format!("[header] size: the fields take {sum} bytes, more than {size}"),
            ));
        }
        if let Some(sum) = given_width(&records.fields) {
            if sum == 0 && records.variants.is_empty() && records.sync.is_empty() {
                return Err(reader.error(&records_value.span(), "fields: a record takes no bytes"));
            }
            if sum > MAX_SIZE {
                return Err(reader.error(
                    &records_value.span(),
                    format!("fields: a record of {sum} bytes is more than {MAX_SIZE}"),
                ));
            }
        }
        Ok(Self {
            name,
            description,
            path: path.map(Path::to_path_buf),
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
            endian,
            endian_auto,
            layout,
            header,
            records,
            delimited: None,
            footer,
            blocks,
            capture,
            files,
            sections,
            variant: None,
        })
    }

    /// A `kind = "delimited"` spec: a CSV-like file's reading options, with no
    /// header or record fields.
    fn parse_delimited(
        reader: &Reader<'_>,
        document: &DeTable<'_>,
        path: Option<&Path>,
    ) -> Result<Self, SpecError> {
        let top = reader.entries(
            document,
            "a delimited spec",
            &[
                "name",
                "description",
                "kind",
                "match",
                "delimiter",
                "comment_char",
                "skip_initial_space",
                "header_rows",
                "header_join",
                "metadata_line",
                "null_value",
                "skip_lines",
                "columns",
            ],
        )?;
        let name = reader.spec_name(&top)?;
        let description = top
            .get("description")
            .map(|v| reader.string(v, "description"))
            .transpose()?;
        let MatchRules {
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
        } = match top.get("match") {
            Some(v) => reader.match_rules(v, None)?,
            None => MatchRules::default(),
        };
        let delimited = reader.delimited(&top)?;
        Ok(Self {
            name,
            description,
            path: path.map(Path::to_path_buf),
            globs,
            glob_set,
            magic,
            magic_offset,
            expect,
            endian: Endian::Little,
            endian_auto: false,
            layout: Layout::Rows,
            header: Header::default(),
            records: Records::default(),
            delimited: Some(Arc::new(delimited)),
            footer: None,
            blocks: None,
            capture: None,
            files: None,
            sections: Vec::new(),
            variant: None,
        })
    }

    /// Whether the spec is `kind = "delimited"`.
    pub fn is_delimited(&self) -> bool {
        self.delimited.is_some()
    }

    /// Read the spec in `path`, which may be no more than [`MAX_SPEC_BYTES`].
    pub fn load(path: &Path) -> Result<Self, SpecError> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|f| f.take(MAX_SPEC_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|e| SpecError {
                path: Some(path.to_path_buf()),
                line: 0,
                column: 0,
                message: format!("could not read the spec: {e}"),
            })?;
        Self::from_bytes(&bytes, path)
    }

    /// The spec in `bytes`, read from `from` (a file or a URL), refused past
    /// [`MAX_SPEC_BYTES`].
    pub fn from_bytes(bytes: &[u8], from: &Path) -> Result<Self, SpecError> {
        let refused = |message: String| SpecError {
            path: Some(from.to_path_buf()),
            line: 0,
            column: 0,
            message,
        };
        if bytes.len() as u64 > MAX_SPEC_BYTES {
            return Err(refused(format!("a spec is at most {MAX_SPEC_SAID}")));
        }
        let text =
            std::str::from_utf8(bytes).map_err(|_| refused("a spec is UTF-8 text".to_string()))?;
        Self::parse(text, Some(from))
    }

    /// Whether `path`'s name matches one of the spec's globs. A glob with a `/` is
    /// matched against the whole path, one without against the name.
    pub fn glob_matches(&self, path: &Path) -> bool {
        let Some(set) = &self.glob_set else {
            return false;
        };
        let name = path.file_name().map(Path::new);
        name.is_some_and(|n| set.is_match(n)) || set.is_match(path)
    }

    /// Whether `head`, the first bytes of a file, carries the spec's magic. A delimited
    /// spec's magic is the start of the first line, after any byte-order mark.
    pub fn magic_matches(&self, head: &[u8]) -> bool {
        let head = if self.is_delimited() {
            head.strip_prefix(UTF8_BOM).unwrap_or(head)
        } else {
            head
        };
        let start = self.magic_offset as usize;
        let found = head.get(start..start + self.magic.len());
        !self.magic.is_empty()
            && (found == Some(&self.magic)
                || (self.endian_auto
                    && found.is_some_and(|f| f.iter().eq(self.magic.iter().rev()))))
    }

    /// Whether `head` holds the header values `match.where` asks for.
    pub fn header_matches(&self, head: &[u8]) -> bool {
        if self.expect.is_empty() {
            return true;
        }
        let Ok(header) = read_header(self, head) else {
            return false;
        };
        self.expect.iter().all(|(field, wanted)| match wanted {
            Expected::Int(v) => header.int(field) == Some(*v),
            Expected::Text(v) => header.text(field).as_deref() == Some(v.as_str()),
        })
    }

    /// Bytes from the front of a file that settle the spec's magic and `where`.
    pub fn match_reach(&self) -> u64 {
        let magic = if self.magic.is_empty() {
            0
        } else {
            let bom = if self.is_delimited() {
                UTF8_BOM.len() as u64
            } else {
                0
            };
            bom + self.magic_offset + self.magic.len() as u64
        };
        let header = if self.expect.is_empty() {
            0
        } else {
            given_width(&self.header.fields).unwrap_or(MAX_MATCH_READ)
        };
        magic.max(header).min(MAX_MATCH_READ)
    }

    /// What the spec says files of it look like, for listings: its globs, magic and
    /// header values.
    pub fn match_summary(&self) -> String {
        let mut said = Vec::new();
        if !self.globs.is_empty() {
            said.push(self.globs.join(" "));
        }
        if !self.magic.is_empty() {
            let magic = if self.magic.iter().all(|b| b.is_ascii_graphic()) {
                format!("\"{}\"", String::from_utf8_lossy(&self.magic))
            } else {
                crate::fixed_records::hex(&self.magic)
            };
            if self.magic_offset > 0 {
                said.push(format!("magic {magic} at {}", self.magic_offset));
            } else {
                said.push(format!("magic {magic}"));
            }
        }
        for (field, wanted) in &self.expect {
            said.push(match wanted {
                Expected::Int(v) => format!("header.{field} = {v}"),
                Expected::Text(v) => format!("header.{field} = \"{v}\""),
            });
        }
        said.join(", ")
    }
}

/// Bytes one field takes in each record, when nothing about it comes from the file.
fn field_width(field: &Field) -> Option<u64> {
    let width = match (&field.size, field.ty.width()) {
        (_, Some(w)) => w,
        (Some(Amount::Given(n)), None) => *n,
        _ => return None,
    };
    let count = match &field.count {
        None => 1,
        Some(Amount::Given(n)) => *n,
        Some(_) => return None,
    };
    width.checked_mul(count)
}

/// The bytes the fields take, when none of their sizes comes from the file.
fn given_width(fields: &[Field]) -> Option<u64> {
    fields
        .iter()
        .try_fold(0u64, |sum, f| sum.checked_add(field_width(f)?))
}

/// The bytes `fields` take, when none of their sizes comes from the file.
pub(crate) fn fields_width(fields: &[Field]) -> Option<u64> {
    given_width(fields)
}

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

    fn int(&self, name: &str) -> Option<i128> {
        int_value(self.get(name)?)
    }

    fn text(&self, name: &str) -> Option<String> {
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
fn sized(field: &Field, header: &HeaderValues) -> Result<(u64, u64), String> {
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
            let column =
                crate::fixed_records::decode(bytes, &layout, 1).map_err(|e| e.to_string())?;
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
fn read_header(spec: &Spec, bytes: &[u8]) -> Result<HeaderValues, String> {
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
fn read_footer(
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
fn read_lookups(
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
            .map(crate::fixed_records::text)
            .collect(),
    }
}

/// The rows a spec reads, however they are framed: what the table, a window of it,
/// `formats check` and the fuzz target read through.
pub trait SpecRecords: crate::pushdown::Windowed + std::fmt::Debug {
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

impl SpecRecords for crate::framed_records::FramedRecords {
    fn rows(&self) -> usize {
        crate::framed_records::FramedRecords::rows(self)
    }
    fn schema(&self) -> SchemaRef {
        crate::framed_records::FramedRecords::schema(self)
    }
    fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
        Ok(crate::framed_records::FramedRecords::lazy(&self))
    }
    fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        crate::framed_records::FramedRecords::collect(self, rows)
    }
    fn sources(&self) -> &[Arc<Bytes>] {
        crate::framed_records::FramedRecords::sources(self)
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
fn past_limit(notes: &mut Vec<String>, rows: u64, records: &FixedRecords) {
    let past = rows.saturating_sub(records.rows() as u64);
    if past > 0 {
        notes.push(format!(
            "the last {past} records are past the most a table holds and are not shown"
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
        "{what} ends with {} {} that are not a whole record, left out: {}{more}",
        bytes.len(),
        if bytes.len() == 1 { "byte" } else { "bytes" },
        crate::fixed_records::hex(shown),
    )
}

impl Spec {
    /// The columns the record fields become, a record `stride` bytes apart (or one
    /// file per field for the columns layout), and the bytes the fields take.
    fn record_columns(
        &self,
        header: &HeaderValues,
        header_size: usize,
        stride: Option<usize>,
    ) -> Result<(Vec<ColumnLayout>, u64), String> {
        let mut columns = Vec::new();
        let mut at = 0u64;
        for (i, field) in self.records.fields.iter().enumerate() {
            let (width, count) = sized(field, header)?;
            if width == 0 {
                return Err(format!(
                    "field `{}` takes no bytes",
                    field.name.as_deref().unwrap_or("pad")
                ));
            }
            let start = match self.layout {
                Layout::Rows => header_size + at as usize,
                Layout::Columns => header_size,
            };
            if let Some(name) = &field.name {
                let source = match self.layout {
                    Layout::Rows => 0,
                    Layout::Columns => i,
                };
                if field.flatten {
                    for j in 0..count as usize {
                        let place = Place {
                            start: start + j * width as usize,
                            stride,
                            width: width as usize,
                            count: 1,
                        };
                        let mut layout =
                            layout_of(self, field, &format!("{name}_{j}"), place, header)?;
                        layout.source = source;
                        columns.push(layout);
                    }
                } else {
                    let place = Place {
                        start,
                        stride,
                        width: width as usize,
                        count: count as usize,
                    };
                    let mut layout = layout_of(self, field, name, place, header)?;
                    layout.source = source;
                    columns.push(layout);
                }
            }
            at += width * count;
        }
        Ok((columns, at))
    }

    /// Check the magic of `bytes`, the front of one file.
    fn check_magic(&self, bytes: &[u8], named: &str) -> Result<(), String> {
        if self.magic.is_empty() || self.magic_matches(bytes) {
            return Ok(());
        }
        let start = (self.magic_offset as usize).min(bytes.len());
        let found = &bytes[start..(start + self.magic.len()).min(bytes.len())];
        Err(format!(
            "{named} is not {}: expected magic {} at byte {}, found {}",
            self.name,
            crate::fixed_records::hex(&self.magic),
            self.magic_offset,
            if found.is_empty() {
                "the end of the file".to_string()
            } else {
                crate::fixed_records::hex(found)
            }
        ))
    }

    /// Read `bytes`, one file of the rows layout, named `named` in what it says.
    pub fn open_rows(&self, bytes: Arc<Bytes>, named: &str) -> Result<Opened, String> {
        if self.is_delimited() {
            return Err(format!(
                "{} is a delimited spec; it reads text through the CSV reader",
                self.name
            ));
        }
        self.open_rows_in(bytes, named, None)
    }

    /// The spec with its byte order settled by `head`, for `endian = "auto"`: as the
    /// spec says when the magic reads as written, big-endian when it reads reversed.
    pub fn for_file(&self, head: &[u8]) -> std::borrow::Cow<'_, Spec> {
        if !self.endian_auto {
            return std::borrow::Cow::Borrowed(self);
        }
        let reversed: Vec<u8> = self.magic.iter().rev().copied().collect();
        let start = self.magic_offset as usize;
        if reversed != self.magic && head.get(start..start + reversed.len()) == Some(&reversed[..])
        {
            let mut spec = self.clone();
            spec.endian = Endian::Big;
            spec.endian_auto = false;
            spec.magic = reversed;
            return std::borrow::Cow::Owned(spec);
        }
        std::borrow::Cow::Borrowed(self)
    }

    /// Whether the spec reads a directory: column files, or a tree of its files.
    pub fn reads_directory(&self) -> bool {
        self.files.is_some()
            || (self.layout == Layout::Columns
                && !self.records.fields.iter().any(|f| f.at.is_some()))
    }

    /// The spec reading the variant `name` alone: only its records, and only its columns.
    pub fn with_variant(&self, name: &str) -> Result<Spec, String> {
        if !self.records.variants.iter().any(|v| v.name == name) {
            let names: Vec<&str> = self
                .records
                .variants
                .iter()
                .map(|v| v.name.as_str())
                .collect();
            return Err(if names.is_empty() {
                format!("{} has no variants", self.name)
            } else {
                format!(
                    "{} has no variant {name}; it has {}",
                    self.name,
                    names.join(", ")
                )
            });
        }
        let mut spec = self.clone();
        spec.variant = Some(name.to_string());
        Ok(spec)
    }

    /// Read `bytes`, one file, named `named` in what it says; a symbol list a field
    /// names is looked for beside `path`, the file the bytes are, which also keeps the
    /// walk of its records for the next open of it.
    pub fn open_rows_in(
        &self,
        bytes: Arc<Bytes>,
        named: &str,
        path: Option<&Path>,
    ) -> Result<Opened, String> {
        let dir = path.and_then(Path::parent);
        if self.reads_directory() {
            return Err(format!(
                "{} reads a directory ({}); open the directory",
                self.name,
                if self.files.is_some() {
                    "a tree of its files"
                } else {
                    "column files, layout = \"columns\""
                }
            ));
        }
        let spec = self.for_file(bytes.as_slice());
        let spec = spec.as_ref();
        if spec.layout == Layout::Columns {
            return spec.open_columns_file(bytes, named, dir);
        }
        let data = bytes.as_slice();
        let mut header = if spec.capture.is_some() {
            if !crate::framed_records::capture::is_capture(data) {
                return Err(format!("{named} is not a pcap or pcapng capture"));
            }
            HeaderValues::default()
        } else {
            spec.check_magic(data, named)?;
            read_header(spec, data)?
        };
        let mut notes = Vec::new();
        let full = data.len() as u64;
        if header.size > full {
            return Err(format!(
                "{named} is {full} bytes, shorter than its {}-byte header",
                header.size
            ));
        }
        let (len, footer_note) = read_footer(spec, data, &mut header)?;
        notes.extend(footer_note);
        let fields: Vec<Field> = all_fields(&spec.records).cloned().collect();
        read_lookups(&fields, dir, &mut header)?;
        if crate::framed_records::needed(spec) {
            let (records, more) = crate::framed_records::FramedRecords::open(
                spec,
                bytes.clone(),
                &header,
                header.size as usize..len as usize,
                named,
                path,
            )?;
            notes.extend(more);
            return Ok(Opened {
                records: Arc::new(records),
                notes,
                header,
            });
        }
        let data = &data[..len as usize];
        // The fields' own width first, to check the record size against it.
        let (_, fields_width) = spec.record_columns(&header, 0, Some(1))?;
        let record = match &spec.records.size {
            None => fields_width,
            Some(amount) => {
                let size = header.resolve(amount, "record size")?;
                if size < fields_width {
                    return Err(format!(
                        "the record's fields take {fields_width} bytes, more than its size of {size}"
                    ));
                }
                size
            }
        };
        if record == 0 || record > MAX_SIZE {
            return Err(format!(
                "a record of {record} bytes is outside 1 to {MAX_SIZE}"
            ));
        }
        let room = len - header.size;
        let whole = room / record;
        let rows = match &spec.records.count {
            None => {
                let trailing = room % record;
                if trailing > 0 {
                    notes.push(trailing_note(named, &data[(len - trailing) as usize..]));
                }
                whole
            }
            Some(amount) => {
                let count = header.resolve(amount, "count")?;
                if count > whole {
                    notes.push(format!(
                        "the header says {count} records; the file holds {whole} whole ones, which are shown"
                    ));
                    whole
                } else {
                    let end = header.size + count * record;
                    if end < len {
                        let after = len - end;
                        notes.push(format!(
                            "{named} has {after} {} after its {count} records, left out",
                            if after == 1 { "byte" } else { "bytes" }
                        ));
                    }
                    count
                }
            }
        };
        let (columns, _) =
            spec.record_columns(&header, header.size as usize, Some(record as usize))?;
        let records =
            FixedRecords::new(vec![bytes], columns, rows as usize).map_err(|e| e.to_string())?;
        past_limit(&mut notes, rows, &records);
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Read `dir`, a directory of one file per record field.
    pub fn open_columns(&self, dir: &Path) -> Result<Opened, String> {
        if self.is_delimited() {
            return Err(format!(
                "{} is a delimited spec; it reads text through the CSV reader",
                self.name
            ));
        }
        if self.layout == Layout::Rows {
            return Err(format!(
                "{} reads one file, and {} is a directory",
                self.name,
                dir.display()
            ));
        }
        let mut sources = Vec::new();
        let mut header: Option<HeaderValues> = None;
        let mut notes = Vec::new();
        let mut counts: Vec<(String, u64)> = Vec::new();
        // Each file's own header size: a size read from the header may differ by file.
        let mut starts = Vec::new();
        for field in &self.records.fields {
            let file_name = field
                .file
                .clone()
                .or_else(|| field.name.clone())
                .expect("a column field is named");
            let path = dir.join(&file_name);
            let bytes = Bytes::map(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            self.check_magic(bytes.as_slice(), &file_name)?;
            let read = read_header(self, bytes.as_slice())?;
            let (width, count) = sized(field, header.as_ref().unwrap_or(&read))?;
            let cell = width * count;
            let len = bytes.len() as u64;
            if read.size > len {
                return Err(format!(
                    "{file_name} is {len} bytes, shorter than its {}-byte header",
                    read.size
                ));
            }
            let room = len - read.size;
            if cell > 0 && !room.is_multiple_of(cell) {
                let trailing = room % cell;
                notes.push(trailing_note(
                    &file_name,
                    &bytes.as_slice()[(len - trailing) as usize..],
                ));
            }
            counts.push((file_name, room.checked_div(cell).unwrap_or(0)));
            starts.push(read.size as usize);
            if header.is_none() {
                header = Some(read);
            }
            sources.push(Arc::new(bytes));
        }
        let mut header = header.unwrap_or_default();
        read_lookups(&self.records.fields, Some(dir), &mut header)?;
        let fewest = counts.iter().map(|(_, n)| *n).min().unwrap_or(0);
        if counts.iter().any(|(_, n)| *n != fewest) {
            let said: Vec<String> = counts
                .iter()
                .map(|(name, n)| format!("{name} {n}"))
                .collect();
            notes.push(format!(
                "the column files hold different numbers of values ({}); the first {fewest} rows are shown",
                said.join(", ")
            ));
        }
        let mut rows = fewest;
        if let Some(amount) = &self.records.count {
            let count = header.resolve(amount, "count")?;
            if count > fewest {
                notes.push(format!(
                    "the header says {count} records; the files hold {fewest}, which are shown"
                ));
            }
            rows = rows.min(count);
        }
        let (mut columns, _) = self.record_columns(&header, 0, None)?;
        for column in &mut columns {
            column.start = starts[column.source];
        }
        let records =
            FixedRecords::new(sources, columns, rows as usize).map_err(|e| e.to_string())?;
        past_limit(&mut notes, rows, &records);
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Read `bytes`, one file holding each column's values in a run at its `offset`.
    fn open_columns_file(
        &self,
        bytes: Arc<Bytes>,
        named: &str,
        dir: Option<&Path>,
    ) -> Result<Opened, String> {
        let data = bytes.as_slice();
        self.check_magic(data, named)?;
        let mut header = read_header(self, data)?;
        let mut notes = Vec::new();
        let (len, footer_note) = read_footer(self, data, &mut header)?;
        notes.extend(footer_note);
        read_lookups(&self.records.fields, dir, &mut header)?;
        let (mut columns, _) = self.record_columns(&header, 0, None)?;
        let mut rows = u64::MAX;
        let mut starts = Vec::new();
        for field in &self.records.fields {
            let (width, count) = sized(field, &header)?;
            let at = field.at.as_ref().expect("checked at parse");
            let start = header.resolve_any(at, "offset")?;
            if start < header.size || start > len {
                return Err(format!(
                    "column `{}` starts at byte {start}, outside the data from {} to {len}",
                    field.name.as_deref().unwrap_or("pad"),
                    header.size
                ));
            }
            rows = rows.min((len - start) / (width * count).max(1));
            starts.push(start as usize);
        }
        if let Some(amount) = &self.records.count {
            let count = header.resolve_any(amount, "count")?;
            if count > rows {
                notes.push(format!(
                    "the header says {count} records; the columns have room for {rows}, which are shown"
                ));
            }
            rows = rows.min(count);
        } else {
            notes.push(format!(
                "no count is given, so the rows are the {rows} the shortest column has room for"
            ));
        }
        for column in &mut columns {
            column.start = starts[column.source];
        }
        let sources = vec![bytes; self.records.fields.len()];
        let records = FixedRecords::new(sources, columns, rows.min(usize::MAX as u64) as usize)
            .map_err(|e| e.to_string())?;
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Open `path`: a file, or a directory of column files or of the spec's files.
    pub fn open(&self, path: &Path, named: &str) -> Result<Opened, String> {
        if self.files.is_some() {
            return crate::formats::files::open(self, path);
        }
        if self.reads_directory() {
            return self.open_columns(path);
        }
        if path.is_dir() {
            return Err(format!(
                "{} reads one file, and {} is a directory",
                self.name,
                path.display()
            ));
        }
        let bytes = Bytes::map(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.open_rows_in(Arc::new(bytes), named, Some(path))
    }
}

/// A spec found on the search path, and the copies of the same name it hides.
#[derive(Debug, Clone)]
pub struct Found {
    pub spec: Arc<Spec>,
    pub overrides: Vec<PathBuf>,
}

/// Every spec on the search path, first of each name first.
#[derive(Debug, Clone, Default)]
pub struct Registry {
    pub specs: Vec<Found>,
    /// FIX dictionaries: QuickFIX XML files and `kind = "fix"` TOML files.
    pub fix: Vec<FixFound>,
    /// DBC files for CAN logs: `.dbc` files and `kind = "dbc"` TOML files, in the order
    /// they are read.
    pub dbc: Vec<DbcFound>,
    /// Spec files that could not be read, each with why.
    pub errors: Vec<SpecError>,
}

/// A DBC file found on the search path.
#[derive(Debug, Clone)]
pub struct DbcFound {
    pub dbc: Arc<crate::dbc::Dbc>,
    /// The file it was found as: the `.dbc`, or the TOML that names it.
    pub path: PathBuf,
}

/// A FIX dictionary found on the search path, and the copies of the same name it hides.
#[derive(Debug, Clone)]
pub struct FixFound {
    pub dict: Arc<crate::fix::dict::Dictionary>,
    pub overrides: Vec<PathBuf>,
}

/// How a spec was chosen for a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chosen {
    /// `--spec FILE`.
    SpecFile,
    /// `--format NAME`, or picked in the view.
    Named,
    Glob,
    Magic,
}

impl Chosen {
    pub fn words(self) -> &'static str {
        match self {
            Self::SpecFile => "--spec",
            Self::Named => "its name",
            Self::Glob => "its glob",
            Self::Magic => "its magic",
        }
    }
}

/// The specs a file matches, by the first rule that matched any.
#[derive(Debug, Clone)]
pub struct Matched {
    pub specs: Vec<Arc<Spec>>,
    pub by: Chosen,
}

/// The directories and files searched for specs, in order: the config directory's
/// `formats`, then `$DATUI_FORMATS_PATH`, then `formats_path` from the config.
pub fn search_path(
    config_dir: Option<&Path>,
    env: Option<std::ffi::OsString>,
    configured: &[String],
) -> Vec<PathBuf> {
    let mut path = Vec::new();
    if let Some(dir) = config_dir {
        path.push(dir.join("formats"));
    }
    if let Some(env) = env {
        path.extend(std::env::split_paths(&env).filter(|p| !p.as_os_str().is_empty()));
    }
    path.extend(
        configured
            .iter()
            .filter(|p| !p.trim().is_empty())
            .map(|p| crate::config::expand_path(p)),
    );
    path
}

/// The search path `config` asks for: the config directory's `formats`, then
/// `$DATUI_FORMATS_PATH`, then its `formats_path`.
pub fn search_path_for(config: &crate::config::AppConfig) -> Vec<PathBuf> {
    let config_dir = crate::config::ConfigManager::new(crate::APP_NAME)
        .ok()
        .map(|m| m.config_dir().to_path_buf());
    search_path(
        config_dir.as_deref(),
        std::env::var_os(PATH_VAR),
        &config.formats_path,
    )
}

impl Registry {
    /// Read every spec on `path`. A directory gives its `*.toml` files in name order; a
    /// file gives itself. What cannot be read is kept as an error, not fatal. A TOML
    /// file of `kind = "fix"`, or a QuickFIX XML file, is a FIX dictionary; a `.dbc`
    /// file, or a TOML file of `kind = "dbc"`, is a DBC file.
    pub fn load(path: &[PathBuf]) -> Self {
        let mut registry = Self::default();
        for entry in path {
            let files: Vec<PathBuf> = if entry.is_dir() {
                let Ok(listing) = std::fs::read_dir(entry) else {
                    continue;
                };
                let mut files: Vec<PathBuf> = listing
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| {
                        p.is_file()
                            && p.extension().is_some_and(|e| {
                                e.eq_ignore_ascii_case("toml")
                                    || e.eq_ignore_ascii_case("xml")
                                    || e.eq_ignore_ascii_case("dbc")
                            })
                    })
                    .collect();
                files.sort();
                files
            } else if entry.is_file() {
                vec![entry.clone()]
            } else {
                continue;
            };
            for file in files {
                // A DBC file, or a TOML file of `kind = "dbc"` that names one.
                let dbc_like = file.extension().is_some_and(|e| {
                    e.eq_ignore_ascii_case("dbc") || e.eq_ignore_ascii_case("toml")
                });
                if dbc_like {
                    match crate::dbc::load(&file) {
                        Ok(Some(dbc)) => {
                            registry.dbc.push(DbcFound {
                                dbc: Arc::new(dbc),
                                path: file,
                            });
                            continue;
                        }
                        Err(e) => {
                            registry.errors.push(e);
                            continue;
                        }
                        Ok(None) => {}
                    }
                }
                match crate::fix::dict::Dictionary::load(&file) {
                    Ok(Some(dict)) => {
                        registry.add_fix(dict, file);
                        continue;
                    }
                    Err(e) => {
                        registry.errors.push(e);
                        continue;
                    }
                    // An XML file that is not a FIX dictionary is not a spec either.
                    Ok(None)
                        if !file
                            .extension()
                            .is_some_and(|e| e.eq_ignore_ascii_case("toml")) =>
                    {
                        continue;
                    }
                    Ok(None) => {}
                }
                match Spec::load(&file) {
                    Ok(spec) => registry.add(spec, file),
                    Err(e) => registry.errors.push(e),
                }
            }
        }
        registry
    }

    fn add(&mut self, spec: Spec, file: PathBuf) {
        if let Some(found) = self.specs.iter_mut().find(|f| f.spec.name == spec.name) {
            found.overrides.push(file);
        } else {
            self.specs.push(Found {
                spec: Arc::new(spec),
                overrides: Vec::new(),
            });
        }
    }

    fn add_fix(&mut self, dict: crate::fix::dict::Dictionary, file: PathBuf) {
        if let Some(found) = self.fix.iter_mut().find(|f| f.dict.name == dict.name) {
            found.overrides.push(file);
        } else {
            self.fix.push(FixFound {
                dict: Arc::new(dict),
                overrides: Vec::new(),
            });
        }
    }

    /// The FIX dictionary named `name`.
    pub fn fix_dict(&self, name: &str) -> Option<&Arc<crate::fix::dict::Dictionary>> {
        self.fix
            .iter()
            .find(|f| f.dict.name == name)
            .map(|f| &f.dict)
    }

    /// The registry of `specs`, for tests and hosts that have their specs in hand.
    pub fn of(specs: Vec<Spec>) -> Self {
        let mut registry = Self::default();
        for spec in specs {
            let file = spec.path.clone().unwrap_or_default();
            registry.add(spec, file);
        }
        registry
    }

    pub fn get(&self, name: &str) -> Option<&Arc<Spec>> {
        self.specs
            .iter()
            .find(|f| f.spec.name == name)
            .map(|f| &f.spec)
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// The spec whose glob names the file `file` first, when it reads the file's
    /// records as several variants: the home screen lists them inside the file.
    pub fn variants_of(&self, file: &Path) -> Option<Arc<Spec>> {
        self.by_glob(file, false)
            .into_iter()
            .find(|s| !s.is_delimited())
            .filter(|s| s.records.variants.len() > 1 && s.variant.is_none())
    }

    /// The specs whose globs match `path`, a file or (for the columns layout) a
    /// directory, by name alone.
    pub fn by_glob(&self, path: &Path, is_dir: bool) -> Vec<Arc<Spec>> {
        self.specs
            .iter()
            .map(|f| &f.spec)
            .filter(|s| s.reads_directory() == is_dir && s.glob_matches(path))
            .cloned()
            .collect()
    }

    /// The specs `path` matches: by glob, else by magic. A spec with `match.where`
    /// matches only a file whose header holds those values. The front of the file is
    /// read through `head`, once, and only when a magic or a header is to be compared.
    pub fn matching(
        &self,
        path: &Path,
        is_dir: bool,
        head: impl FnOnce(u64) -> Option<Vec<u8>>,
    ) -> Option<Matched> {
        self.matching_among(path, is_dir, |_| true, head)
    }

    /// [`Self::matching`] among the specs `wanted` says may read `path`.
    pub fn matching_among(
        &self,
        path: &Path,
        is_dir: bool,
        wanted: impl Fn(&Spec) -> bool,
        head: impl FnOnce(u64) -> Option<Vec<u8>>,
    ) -> Option<Matched> {
        let mut globbed = self.by_glob(path, is_dir);
        globbed.retain(|s| wanted(s));
        let (candidates, by) = if !globbed.is_empty() {
            (globbed, Chosen::Glob)
        } else if is_dir {
            return None;
        } else {
            let magic: Vec<Arc<Spec>> = self
                .specs
                .iter()
                .map(|f| &f.spec)
                .filter(|s| !s.reads_directory() && !s.magic.is_empty() && wanted(s))
                .cloned()
                .collect();
            (magic, Chosen::Magic)
        };
        let reach = candidates
            .iter()
            .map(|s| match by {
                Chosen::Magic => s.match_reach(),
                _ if s.expect.is_empty() => 0,
                _ => s.match_reach(),
            })
            .max()
            .unwrap_or(0);
        let head = if reach > 0 && !is_dir {
            head(reach)
        } else {
            None
        };
        let specs: Vec<Arc<Spec>> = candidates
            .into_iter()
            .filter(|s| {
                let Some(head) = &head else {
                    // Nothing read: a glob match stands unless it asked about the header.
                    return by == Chosen::Glob && s.expect.is_empty();
                };
                (by != Chosen::Magic || s.magic_matches(head)) && s.header_matches(head)
            })
            .collect();
        (!specs.is_empty()).then_some(Matched { specs, by })
    }

    /// Text for `datui formats`: each spec, the file it came from, the copies it hides,
    /// and the spec files that could not be read.
    pub fn listing(&self, path: &[PathBuf]) -> String {
        let mut out = String::new();
        if self.specs.is_empty() {
            out.push_str("No format specs found.\n");
        }
        for found in &self.specs {
            let spec = &found.spec;
            out.push_str(&spec.name);
            let summary = spec.match_summary();
            match (spec.is_delimited(), summary.is_empty()) {
                (true, true) => out.push_str("  (delimited)"),
                (true, false) => out.push_str(&format!("  (delimited; {summary})")),
                (false, true) => {}
                (false, false) => out.push_str(&format!("  ({summary})")),
            }
            out.push('\n');
            if let Some(description) = &spec.description {
                out.push_str(&format!("  {description}\n"));
            }
            if let Some(file) = &spec.path {
                out.push_str(&format!("  {}\n", file.display()));
            }
            for hidden in &found.overrides {
                out.push_str(&format!("  overrides {}\n", hidden.display()));
            }
        }
        if !self.fix.is_empty() {
            out.push_str("\nFIX dictionaries:\n");
        }
        for found in &self.fix {
            let dict = &found.dict;
            out.push_str(&dict.name);
            let summary = dict.matcher.summary();
            if !summary.is_empty() {
                out.push_str(&format!("  ({summary})"));
            }
            out.push('\n');
            if let Some(file) = &dict.path {
                out.push_str(&format!("  {}\n", file.display()));
            }
            for hidden in &found.overrides {
                out.push_str(&format!("  overrides {}\n", hidden.display()));
            }
        }
        if !self.dbc.is_empty() {
            out.push_str("\nDBC files:\n");
        }
        for found in &self.dbc {
            let dbc = &found.dbc;
            out.push_str(&format!(
                "{}  ({}{})\n  {}\n",
                dbc.name,
                crate::text_formats::count(dbc.messages.len() as u64, "message", "messages"),
                dbc.interface
                    .as_ref()
                    .map(|i| format!(", interface {i}"))
                    .unwrap_or_default(),
                found.path.display()
            ));
        }
        if !self.errors.is_empty() {
            out.push_str("\nCould not read:\n");
            for e in &self.errors {
                out.push_str(&format!("  {e}\n"));
            }
        }
        out.push_str("\nSearched, in order:\n");
        for entry in path {
            out.push_str(&format!("  {}\n", entry.display()));
        }
        out
    }
}

/// The first `reach` bytes of `path`, through its decompressor when it has one.
pub fn head_of(
    path: &Path,
    compression: Option<crate::CompressionFormat>,
    reach: u64,
) -> Option<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let reader: Box<dyn Read> = match compression {
        None => Box::new(file),
        Some(crate::CompressionFormat::Gzip) => Box::new(flate2::read::GzDecoder::new(file)),
        Some(crate::CompressionFormat::Zstd) => Box::new(zstd::Decoder::new(file).ok()?),
        Some(crate::CompressionFormat::Bzip2) => Box::new(bzip2::read::BzDecoder::new(file)),
        Some(crate::CompressionFormat::Xz) => Box::new(xz2::read::XzDecoder::new(file)),
    };
    let mut head = Vec::new();
    reader
        .take(reach.min(MAX_MATCH_READ))
        .read_to_end(&mut head)
        .ok()?;
    Some(head)
}

/// A file read through a spec, as the open carries it to the dataset.
pub struct Read {
    pub spec: Arc<Spec>,
    pub by: Chosen,
    /// The other specs that matched as well as `spec`, by the same rule.
    pub also: Vec<String>,
    /// Warnings from the read: trailing bytes, a short count.
    pub notes: Vec<String>,
    pub header: HeaderValues,
    pub records: Arc<dyn SpecRecords>,
}

impl std::fmt::Debug for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("spec", &self.spec.name)
            .field("by", &self.by)
            .field("also", &self.also)
            .field("rows", &self.records.rows())
            .finish()
    }
}

/// What a request for a format says, besides the path.
#[derive(Debug, Clone, Default)]
pub struct Asked {
    /// `--spec FILE`.
    pub spec_file: Option<PathBuf>,
    /// `--format NAME`, or the spec picked in the view.
    pub spec_name: Option<String>,
    /// The spec `spec_file` names, read already: fetched, when it is remote.
    pub spec: Option<Arc<Spec>>,
    /// `--variant NAME`: one variant of the spec's records, read alone.
    pub variant: Option<String>,
    /// A built-in format from `--format`, which no spec overrides.
    pub builtin: bool,
    pub compression: Option<crate::CompressionFormat>,
    /// Several files are read as one: only a delimited spec reads them.
    pub text_only: bool,
}

/// Where a spec was chosen from, carried to a decompressed copy's read.
#[derive(Clone)]
pub struct Choice {
    pub spec: Arc<Spec>,
    pub by: Chosen,
    pub also: Vec<String>,
}

/// What [`route`] decided about one local path.
pub enum Route {
    /// Not a spec's: the path opens as it does without specs.
    Elsewhere,
    Read(Box<Read>),
    /// Compressed: decompress it, then read the copy with `choice.spec`.
    Decompress(Choice),
    /// A delimited spec's: read with the CSV reader, in the spec's dialect.
    Delimited(Choice),
}

/// Whether, and with which spec, `path` is read. In order: `--spec FILE`, then
/// `--format NAME`, then a glob, then magic. A file whose name or bytes say it is a
/// format datui reads already keeps opening that way.
pub fn route(path: &Path, asked: &Asked, registry: &Registry) -> Result<Route, String> {
    let compression = asked.compression.or_else(|| {
        path.is_file()
            .then(|| crate::CompressionFormat::from_extension(path))
            .flatten()
    });
    let named = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let explicit = if let Some(spec) = &asked.spec {
        Some(Choice {
            spec: spec.clone(),
            by: Chosen::SpecFile,
            also: Vec::new(),
        })
    } else if let Some(file) = &asked.spec_file {
        if crate::source::is_remote_url(file) {
            return Err(format!(
                "{}: a remote spec is fetched by the open, and was not",
                file.display()
            ));
        }
        let spec = Spec::load(file).map_err(|e| e.to_string())?;
        Some(Choice {
            spec: Arc::new(spec),
            by: Chosen::SpecFile,
            also: Vec::new(),
        })
    } else if let Some(name) = &asked.spec_name {
        let spec = registry.get(name).ok_or_else(|| {
            format!("no format named {name} on the search path; `datui formats` lists them")
        })?;
        Some(Choice {
            spec: spec.clone(),
            by: Chosen::Named,
            also: Vec::new(),
        })
    } else {
        None
    };
    if asked.text_only
        && let Some(choice) = &explicit
        && !choice.spec.is_delimited()
    {
        return Err(format!(
            "{} reads one file, or one directory of column files",
            choice.spec.name
        ));
    }
    let choice = match explicit {
        Some(choice) => choice,
        None => {
            if asked.builtin || registry.is_empty() {
                return no_spec(asked);
            }
            let is_dir = path.is_dir();
            // What the name already says is read as it says, compressed or not: a
            // spec of records takes only a name that says no format datui reads, and a
            // delimited spec also one that says delimited text.
            let said = (!is_dir)
                .then(|| crate::discover::data_format(path))
                .flatten()
                // Text by its name (`.log`, `.txt`) says no more than no name does.
                .filter(|f| !f.is_lines());
            let parquet_key =
                crate::discover::is_parquet_key(&crate::discover::directory_and_name(path));
            let records_may = said.is_none() && !parquet_key && !asked.text_only;
            let text_may = !is_dir
                && !parquet_key
                && said.is_none_or(|f| crate::FileFormat::separator(f).is_some());
            if !records_may && !text_may {
                return Ok(Route::Elsewhere);
            }
            // A glob names the file as it is stored uncompressed: `day.l2.zst` is an `*.l2`.
            let inner = match compression {
                Some(_) => path.with_extension(""),
                None => path.to_path_buf(),
            };
            let wanted = |s: &Spec| {
                if s.is_delimited() {
                    text_may
                } else {
                    records_may
                }
            };
            let matched = registry.matching_among(&inner, is_dir, wanted, |reach| {
                // A file with no extension may be Parquet, Arrow, Avro or ORC by its bytes,
                // which it stays.
                if compression.is_none() && crate::discover::sniff_format(path).is_some() {
                    return None;
                }
                head_of(path, compression, reach)
            });
            let Some(matched) = matched else {
                return no_spec(asked);
            };
            let mut specs = matched.specs.into_iter();
            let spec = specs.next().expect("a match has a spec");
            Choice {
                spec,
                by: matched.by,
                also: specs.map(|s| s.name.clone()).collect(),
            }
        }
    };
    // The CSV reader reads a delimited spec's files, compressed or not.
    if choice.spec.is_delimited() {
        return Ok(Route::Delimited(choice));
    }
    let choice = match &asked.variant {
        Some(variant) => Choice {
            spec: Arc::new(choice.spec.with_variant(variant)?),
            ..choice
        },
        None => choice,
    };
    if compression.is_some() && path.is_file() {
        return Ok(Route::Decompress(choice));
    }
    read(path, &named, choice).map(|r| Route::Read(Box::new(r)))
}

/// The route of a path no spec reads, unless `--variant` asked for one.
fn no_spec(asked: &Asked) -> Result<Route, String> {
    match &asked.variant {
        Some(variant) => Err(format!(
            "--variant {variant} picks a variant of a format spec's records, and no spec reads this file"
        )),
        None => Ok(Route::Elsewhere),
    }
}

/// Read `path` with the spec `choice` holds, naming it `named` in what it says.
pub fn read(path: &Path, named: &str, choice: Choice) -> Result<Read, String> {
    let opened = choice.spec.open(path, named)?;
    Ok(Read {
        spec: choice.spec,
        by: choice.by,
        also: choice.also,
        notes: opened.notes,
        header: opened.header,
        records: opened.records,
    })
}

impl Read {
    /// The dataset's notes about the read: which format, why, what else matched, the
    /// header's values, and the warnings.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        let note = |summary: String, scope: String| crate::notes::Note {
            summary,
            scope,
            read_as_text: None,
            passed_over: None,
        };
        let from = self
            .spec
            .path
            .as_ref()
            .map_or_else(|| "the spec".to_string(), |p| p.display().to_string());
        let mut notes = vec![note(
            format!("read as {}, chosen by {}", self.spec.name, self.by.words()),
            format!("from {from}"),
        )];
        if !self.also.is_empty() {
            notes.push(note(
                format!(
                    "{} also {} this file; press b to pick another",
                    self.also.join(", "),
                    if self.also.len() == 1 {
                        "matches"
                    } else {
                        "match"
                    }
                ),
                format!("by {}", self.by.words()),
            ));
        }
        if !self.header.values.is_empty() {
            let said: Vec<String> = self
                .header
                .values
                .iter()
                .map(|(name, value)| format!("{name} = {value}"))
                .collect();
            notes.push(note(
                format!("header: {}", said.join(", ")),
                "from the file's header".to_string(),
            ));
        }
        for warning in &self.notes {
            notes.push(note(
                warning.clone(),
                "from the file's length and the spec".to_string(),
            ));
        }
        notes
    }
}

/// `datui formats`, or `datui formats check SPEC [FILE]`: what to print, and the exit
/// code (non-zero when the check finds an error).
pub fn command(
    action: Option<&crate::cli::FormatsAction>,
    args: &crate::cli::Args,
    config: &crate::config::AppConfig,
) -> (String, i32) {
    let path = search_path_for(config);
    let registry = Registry::load(&path);
    match action {
        None => (registry.listing(&path), 0),
        Some(crate::cli::FormatsAction::Check { spec, file }) => {
            let options = crate::OpenOptions::from_args_and_config(args, config);
            match check(spec, file.as_deref(), &registry, &options) {
                Ok(text) => (text, 0),
                Err(text) => (text, 1),
            }
        }
    }
}

/// Rows `formats check` prints from a file.
const CHECK_ROWS: usize = 10;

/// Check the spec `named` (a file, or a name on the search path) and, given `file`,
/// read its first rows.
fn check(
    named: &str,
    file: Option<&Path>,
    registry: &Registry,
    options: &crate::OpenOptions,
) -> Result<String, String> {
    let as_file = Path::new(named);
    if let Some(dict) = fix_dict_named(named, registry)? {
        return check_fix(&dict, file);
    }
    let spec = if as_file.is_file() {
        Arc::new(Spec::load(as_file).map_err(|e| format!("error: {e}\n"))?)
    } else if let Some(spec) = registry.get(named) {
        spec.clone()
    } else {
        let mut said = format!("error: no spec file or format named {named}\n");
        if let Some(e) = registry.errors.iter().find(|e| {
            e.path
                .as_ref()
                .is_some_and(|p| p.file_stem() == as_file.file_stem())
        }) {
            said.push_str(&format!("error: {e}\n"));
        }
        return Err(said);
    };
    let mut out = format!("{}: ok\n", spec.name);
    if let Some(from) = &spec.path {
        out.push_str(&format!("  from {}\n", from.display()));
    }
    let summary = spec.match_summary();
    if !summary.is_empty() {
        out.push_str(&format!("  matches {summary}\n"));
    }
    if spec.is_delimited() {
        return crate::delimited_spec::check(&spec, file, CHECK_ROWS, options)
            .map(|rest| out.clone() + &rest)
            .map_err(|rest| out.clone() + &rest);
    }
    let named_fields = spec
        .records
        .fields
        .iter()
        .filter(|f| f.name.is_some())
        .count();
    out.push_str(&format!("  {named_fields} record fields"));
    if let Some(width) = given_width(&spec.records.fields) {
        let size = match spec.records.size {
            Some(Amount::Given(size)) => size,
            _ => width,
        };
        if spec.layout == Layout::Rows
            && spec
                .records
                .size
                .as_ref()
                .is_none_or(|s| matches!(s, Amount::Given(_)))
        {
            out.push_str(&format!(", {size} bytes a record"));
        }
    }
    out.push('\n');
    let Some(file) = file else {
        return Ok(out);
    };
    let compression = crate::CompressionFormat::from_extension(file).filter(|_| file.is_file());
    let shown = file.file_name().map_or_else(
        || file.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let choice = Choice {
        spec: spec.clone(),
        by: Chosen::SpecFile,
        also: Vec::new(),
    };
    // A compressed file is read from a copy, as the open reads it.
    let copy;
    let readable = match compression {
        None => file,
        Some(compression) => {
            copy = decompressed_copy(file, compression)
                .map_err(|e| format!("{out}error: {shown}: {e}\n"))?;
            copy.path()
        }
    };
    let read = read(readable, &shown, choice).map_err(|e| format!("{out}error: {e}\n"))?;
    for note in &read.notes {
        out.push_str(&format!("warning: {note}\n"));
    }
    if !read.header.values.is_empty() {
        let said: Vec<String> = read
            .header
            .values
            .iter()
            .map(|(name, value)| format!("{name} = {value}"))
            .collect();
        out.push_str(&format!("header: {}\n", said.join(", ")));
    }
    out.push_str(&format!("{} records\n", read.records.rows()));
    let df = read
        .records
        .collect(CHECK_ROWS)
        .map_err(|e| format!("{out}error: {e}\n"))?;
    out.push_str(&text_table(&df));
    Ok(out)
}

/// The FIX dictionary `named` names: a dictionary file, or one on the search path.
fn fix_dict_named(
    named: &str,
    registry: &Registry,
) -> Result<Option<Arc<crate::fix::dict::Dictionary>>, String> {
    let as_file = Path::new(named);
    if as_file.is_file() {
        return match crate::fix::dict::Dictionary::load(as_file) {
            Ok(dict) => Ok(dict.map(Arc::new)),
            Err(e) => Err(format!("error: {e}\n")),
        };
    }
    Ok(registry.fix_dict(named).cloned())
}

/// `formats check` of a FIX dictionary: what it names and matches; with `file`, how
/// many of the log's messages it applies to and the tags it names there.
fn check_fix(
    dict: &Arc<crate::fix::dict::Dictionary>,
    file: Option<&Path>,
) -> Result<String, String> {
    use std::io::Read;
    let mut out = format!("{}: ok\n", dict.name);
    if let Some(from) = &dict.path {
        out.push_str(&format!("  from {}\n", from.display()));
    }
    let summary = dict.matcher.summary();
    if !summary.is_empty() {
        out.push_str(&format!("  matches {summary}\n"));
    }
    let enums = dict.tags.values().filter(|t| !t.enums.is_empty()).count();
    out.push_str(&format!("  {} tags, {enums} with enums\n", dict.tags.len()));
    let Some(file) = file else {
        return Ok(out);
    };
    let read = std::sync::atomic::AtomicU64::new(0);
    let mut reader = crate::gps::open_reader(file, &crate::OpenOptions::default(), &read)
        .map_err(|e| format!("{out}error: {}: {e}\n", file.display()))?;
    let mut log = crate::fix::FixReader::new(crate::fix::dict::Layers::new(vec![dict.clone()]));
    let mut chunk = vec![0u8; 1 << 16];
    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|e| format!("{out}error: {}: {e}\n", file.display()))?;
        if n == 0 {
            break;
        }
        log.push(&chunk[..n]);
        let _ = log.take_batch();
    }
    let _ = log.finish();
    let stats = log.stats();
    out.push_str(&format!(
        "{} messages, {} of them matched by {}\n",
        stats.messages,
        stats.applied.get(1).copied().unwrap_or(0),
        dict.name
    ));
    let named: Vec<String> = log
        .tag_names()
        .into_iter()
        .filter(|(_, _, by)| *by == 1)
        .map(|(tag, name, _)| format!("{tag} {name}"))
        .collect();
    if !named.is_empty() {
        out.push_str(&format!("names in the log: {}\n", named.join(", ")));
    }
    Ok(out)
}

/// `df` as plain text: a row of names, then a row per record, columns aligned.
pub(crate) fn text_table(df: &polars::prelude::DataFrame) -> String {
    let mut rows: Vec<Vec<String>> = vec![
        df.get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect(),
    ];
    for i in 0..df.height() {
        rows.push(
            df.columns()
                .iter()
                .map(|c| match c.get(i) {
                    Ok(polars::prelude::AnyValue::String(s)) => s.to_string(),
                    Ok(polars::prelude::AnyValue::StringOwned(s)) => s.to_string(),
                    Ok(v) => v.to_string(),
                    Err(_) => String::new(),
                })
                .collect(),
        );
    }
    let widths: Vec<usize> = (0..rows[0].len())
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        out.push_str(cells.join("  ").trim_end());
        out.push('\n');
    }
    out
}

/// `path` decompressed into a temporary file.
fn decompressed_copy(
    path: &Path,
    compression: crate::CompressionFormat,
) -> std::io::Result<tempfile::NamedTempFile> {
    use std::io::Read as _;
    let file = std::fs::File::open(path)?;
    let mut reader: Box<dyn std::io::Read> = match compression {
        crate::CompressionFormat::Gzip => Box::new(flate2::read::GzDecoder::new(file)),
        crate::CompressionFormat::Zstd => Box::new(zstd::Decoder::new(file)?),
        crate::CompressionFormat::Bzip2 => Box::new(bzip2::read::BzDecoder::new(file)),
        crate::CompressionFormat::Xz => Box::new(xz2::read::XzDecoder::new(file)),
    };
    let mut copy = tempfile::NamedTempFile::new()?;
    std::io::copy(&mut reader.by_ref(), copy.as_file_mut())?;
    Ok(copy)
}

/// A directory of one spec's files, `{date}/{venue}/trades.bin`: one table, the parts
/// of each file's path as columns.
pub mod files {
    use super::{Bytes, Opened, Spec, SpecRecords};
    use polars::prelude::*;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    /// The most files one tree is read from.
    const MAX_FILES: usize = 100_000;

    /// Each file's records and the values of its path's parts.
    pub struct SpecFiles {
        parts: Vec<(Arc<dyn SpecRecords>, Vec<AnyValue<'static>>)>,
        part_fields: Vec<(PlSmallStr, DataType)>,
        /// The first row of each file.
        starts: Vec<usize>,
        rows: usize,
        schema: SchemaRef,
        sources: Vec<Arc<Bytes>>,
    }

    /// One part's pattern, compiled: its regex, and the part names it captures.
    fn component_regex(component: &str) -> Result<(regex::Regex, Vec<String>), String> {
        let mut out = String::from("^");
        let mut names = Vec::new();
        let mut rest = component;
        while let Some(open) = rest.find('{') {
            out.push_str(&regex::escape(&rest[..open]));
            let after = &rest[open + 1..];
            let close = after.find('}').ok_or("a `{` without its `}`")?;
            let inside = &after[..close];
            let name = inside.split_once(':').map_or(inside, |(n, _)| n);
            out.push_str("(.+?)");
            names.push(name.to_string());
            rest = &after[close + 1..];
        }
        out.push_str(&regex::escape(rest));
        out.push('$');
        Ok((regex::Regex::new(&out).map_err(|e| e.to_string())?, names))
    }

    /// A part's name and the text it matched in a path.
    type PartText = (String, String);

    /// The files under `dir` the pattern names, each with its parts' text.
    fn matching(dir: &Path, pattern: &str) -> Result<Vec<(PathBuf, Vec<PartText>)>, String> {
        let components: Vec<(regex::Regex, Vec<String>)> = pattern
            .split('/')
            .map(component_regex)
            .collect::<Result<_, _>>()?;
        let mut found = Vec::new();
        let mut stack = vec![(dir.to_path_buf(), 0usize, Vec::<PartText>::new())];
        while let Some((at, depth, parts)) = stack.pop() {
            let Ok(listing) = std::fs::read_dir(&at) else {
                continue;
            };
            let (re, names) = &components[depth];
            let last = depth + 1 == components.len();
            for entry in listing.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Some(caps) = re.captures(&name) else {
                    continue;
                };
                let mut parts = parts.clone();
                for (i, part) in names.iter().enumerate() {
                    parts.push((
                        part.clone(),
                        caps.get(i + 1).map_or("", |m| m.as_str()).to_string(),
                    ));
                }
                let path = entry.path();
                if last {
                    if path.is_file() {
                        found.push((path, parts));
                        if found.len() > MAX_FILES {
                            return Err(format!("more than {MAX_FILES} files match"));
                        }
                    }
                } else if path.is_dir() {
                    stack.push((path, depth + 1, parts));
                }
            }
        }
        found.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(found)
    }

    /// Open the tree of `spec`'s files under `dir`.
    pub fn open(spec: &Spec, dir: &Path) -> Result<Opened, String> {
        let files = spec.files.as_ref().expect("a spec of files");
        if !dir.is_dir() {
            return Err(format!(
                "{} reads a directory of its files ({}), and {} is not one",
                spec.name,
                files.pattern,
                dir.display()
            ));
        }
        let mut one = spec.clone();
        one.files = None;
        let mut parts = Vec::new();
        let mut notes = Vec::new();
        let mut header = None;
        let mut sources = Vec::new();
        let mut schema: Option<SchemaRef> = None;
        let found = matching(dir, &files.pattern)?;
        if found.is_empty() {
            return Err(format!(
                "no files under {} match {}",
                dir.display(),
                files.pattern
            ));
        }
        for (path, texts) in found {
            let shown = path
                .strip_prefix(dir)
                .unwrap_or(&path)
                .display()
                .to_string();
            let mut values = Vec::new();
            let mut ok = true;
            for part in &files.parts {
                let text = texts
                    .iter()
                    .find(|(n, _)| *n == part.name)
                    .map_or("", |(_, t)| t.as_str());
                match &part.date {
                    Some(format) => match chrono::NaiveDate::parse_from_str(text, format) {
                        Ok(date) => {
                            let days = (date
                                - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a date"))
                            .num_days();
                            values.push(AnyValue::Date(days as i32));
                        }
                        Err(_) => {
                            notes.push(format!(
                                "{shown}: `{text}` is not a date as {format}; left out"
                            ));
                            ok = false;
                            break;
                        }
                    },
                    None => values.push(AnyValue::StringOwned(text.into())),
                }
            }
            if !ok {
                continue;
            }
            let opened = match one.open(&path, &shown) {
                Ok(o) => o,
                Err(e) => {
                    notes.push(format!("{shown}: {e}; left out"));
                    continue;
                }
            };
            let theirs = opened.records.schema();
            match &schema {
                Some(s) if *s != theirs => {
                    notes.push(format!(
                        "{shown}: its columns differ from the first file's; left out"
                    ));
                    continue;
                }
                Some(_) => {}
                None => schema = Some(theirs),
            }
            notes.extend(opened.notes.into_iter().map(|n| format!("{shown}: {n}")));
            sources.extend(opened.records.sources().iter().cloned());
            if header.is_none() {
                header = Some(opened.header);
            }
            parts.push((opened.records, values));
        }
        let Some(record_schema) = schema else {
            return Err(format!(
                "none of the files under {} could be read",
                dir.display()
            ));
        };
        let part_fields: Vec<(PlSmallStr, DataType)> = files
            .parts
            .iter()
            .map(|p| {
                (
                    PlSmallStr::from(p.name.as_str()),
                    if p.date.is_some() {
                        DataType::Date
                    } else {
                        DataType::String
                    },
                )
            })
            .collect();
        let mut fields: Vec<Field> = part_fields
            .iter()
            .map(|(n, d)| Field::new(n.clone(), d.clone()))
            .collect();
        fields.extend(record_schema.iter_fields());
        let mut starts = Vec::with_capacity(parts.len());
        let mut rows = 0usize;
        for (records, _) in &parts {
            starts.push(rows);
            rows = rows.saturating_add(records.rows());
        }
        let records = SpecFiles {
            parts,
            part_fields,
            starts,
            rows: rows.min(IdxSize::MAX as usize),
            schema: Arc::new(Schema::from_iter(fields)),
            sources,
        };
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header: header.unwrap_or_default(),
        })
    }

    impl std::fmt::Debug for SpecFiles {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SpecFiles")
                .field("files", &self.parts.len())
                .field("rows", &self.rows)
                .finish()
        }
    }

    impl SpecFiles {
        /// `lf`, the rows of part `i`, with the part's path values in front.
        fn dressed(&self, i: usize, lf: LazyFrame) -> LazyFrame {
            let mut exprs: Vec<Expr> = self
                .part_fields
                .iter()
                .zip(&self.parts[i].1)
                .map(|((name, dtype), value)| {
                    let value = match value {
                        AnyValue::Date(d) => lit(*d).cast(DataType::Date),
                        AnyValue::StringOwned(s) => lit(s.as_str()),
                        _ => lit(NULL).cast(dtype.clone()),
                    };
                    value.alias(name.clone())
                })
                .collect();
            exprs.push(all().as_expr());
            lf.select(exprs)
        }
    }

    impl crate::pushdown::Windowed for SpecFiles {
        fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
            let end = start.saturating_add(len).min(self.rows);
            let mut frames = Vec::new();
            let first = self
                .starts
                .partition_point(|s| *s <= start)
                .saturating_sub(1);
            for i in first..self.parts.len() {
                let begin = self.starts[i];
                if begin >= end {
                    break;
                }
                let rows = self.parts[i].0.rows();
                let from = start.saturating_sub(begin).min(rows);
                let take = (end - begin).min(rows) - from;
                if take == 0 {
                    continue;
                }
                frames.push(self.dressed(i, self.parts[i].0.window(from, take)?));
            }
            if frames.is_empty() {
                return Ok(DataFrame::empty_with_schema(&self.schema).lazy());
            }
            concat(frames, UnionArgs::default())
        }
    }

    impl SpecRecords for SpecFiles {
        fn rows(&self) -> usize {
            self.rows
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
            let frames = (0..self.parts.len())
                .map(|i| Ok(self.dressed(i, self.parts[i].0.clone().into_lazy()?)))
                .collect::<PolarsResult<Vec<_>>>()?;
            concat(frames, UnionArgs::default())
        }
        fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
            crate::pushdown::Windowed::window(self, 0, rows)?.collect()
        }
        fn sources(&self) -> &[Arc<Bytes>] {
            &self.sources
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    const L2: &str = r#"
name = "acme.l2feed"
match = { glob = ["*.l2"], magic = "L2FD" }
endian = "le"

[header]
fields = [{ name = "magic", type = "str", size = 4 }, { name = "count", type = "u8" }]

[records]
count = "header.count"
fields = [
  { name = "ts",     type = "u8", time = "ns" },
  { name = "symbol", type = "str", size = 8 },
  { name = "side",   type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
  { name = "price",  type = "u4", scale = 4 },
]
"#;

    fn l2_file(records: &[(u64, &str, u8, u32)], count: u64, trailing: &[u8]) -> Vec<u8> {
        let mut out = b"L2FD".to_vec();
        out.extend(count.to_le_bytes());
        for (ts, symbol, side, price) in records {
            out.extend(ts.to_le_bytes());
            let mut sym = symbol.as_bytes().to_vec();
            sym.resize(8, b' ');
            out.extend(sym);
            out.push(*side);
            out.extend(price.to_le_bytes());
        }
        out.extend(trailing);
        out
    }

    fn open(spec: &Spec, bytes: Vec<u8>) -> Opened {
        spec.open_rows(Arc::new(Bytes::Owned(bytes)), "f").unwrap()
    }

    /// Records past the most a table holds are not shown, and a note says so.
    #[test]
    fn records_past_the_table_limit_are_noted() {
        let records = FixedRecords::new(
            vec![Arc::new(Bytes::Owned(vec![0; 4]))],
            vec![ColumnLayout::new("a", 0, 1, Physical::Unsigned(1), 1)],
            usize::MAX,
        )
        .unwrap();
        let mut notes = Vec::new();
        past_limit(&mut notes, 4, &records);
        assert!(notes.is_empty());
        past_limit(&mut notes, 9, &records);
        assert_eq!(
            notes,
            ["the last 5 records are past the most a table holds and are not shown"]
        );
    }

    fn collect(opened: &Opened) -> DataFrame {
        Arc::clone(&opened.records)
            .into_lazy()
            .unwrap()
            .collect()
            .unwrap()
    }

    fn cell(df: &DataFrame, column: &str, row: usize) -> String {
        df.column(column).unwrap().get(row).unwrap().to_string()
    }

    #[test]
    fn the_issue_s_example_reads() {
        let spec = Spec::parse(L2, None).unwrap();
        assert_eq!(spec.name, "acme.l2feed");
        assert!(spec.glob_matches(Path::new("/x/day.l2")));
        let bytes = l2_file(&[(5, "AAPL", 1, 1_234_500), (6, "MSFT", 2, 7)], 2, &[]);
        assert!(spec.magic_matches(&bytes));
        let opened = open(&spec, bytes);
        assert!(opened.notes.is_empty(), "{:?}", opened.notes);
        let df = collect(&opened);
        assert_eq!(df.height(), 2);
        assert_eq!(cell(&df, "symbol", 0), "\"AAPL\"");
        assert_eq!(cell(&df, "side", 1), "\"SELL\"");
        assert_eq!(
            df.column("price").unwrap().dtype(),
            &DataType::Decimal(38, 4)
        );
        assert_eq!(cell(&df, "price", 0), "123.4500");
        assert_eq!(
            df.column("ts").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Nanoseconds, None)
        );
    }

    #[test]
    fn errors_point_at_the_line_and_column() {
        let text = "name = \"a.b\"\n[records]\nfields = [{ name = \"x\", type = \"u9\" }]\n";
        let e = Spec::parse(text, Some(Path::new("a.toml"))).unwrap_err();
        assert_eq!((e.line, e.column), (3, 32), "{e}");
        assert!(
            e.to_string().starts_with("a.toml:3:32: type: expected u1"),
            "{e}"
        );
        let e = Spec::parse(
            "name = \"a.b\"\n[records]\nfields = [{ name = \"x\", type = \"u4\", colour = 1 }]",
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("unknown key `colour`"), "{e}");
        assert_eq!(e.line, 3);
        let e = Spec::parse("name = \"a.b\"\n[records\n", None).unwrap_err();
        assert_eq!(e.line, 2, "{e}");
    }

    #[test]
    fn bad_specs_are_refused_by_name() {
        let record = |fields: &str| format!("name = \"a.b\"\n[records]\nfields = [{fields}]");
        for (text, said) in [
            ("name = \"a.b\"\n[records]\nframing = \"length_prefixed\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "size names the field"),
            ("name = \"a.b\"\n[records]\nframing = \"sync\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "needs sync"),
            ("name = \"a.b\"\n[blocks]\nheader = [{ name = \"n\", type = \"u4\" }]\nsize = \"n\"\ncompression = \"lzma9\"\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "expected one of none"),
            ("name = \"a.b\"\n[footer]\nfields = [{ name = \"n\", type = \"u4\" }]\n[records]\ncount = \"footer.m\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "no footer field named `m`"),
            ("name = \"a.b\"\n[records]\ntype = \"k\"\nfields = [{ name = \"k\", type = \"u1\" }]\n[[variants]]\nname = \"a\"\nwhen = \"x\"\nfields = []".to_string(), "`k` is a u1"),
            ("name = \"a.b\"\n[records]\nframing = \"sync\"\nsync = \"\u{1bb}a\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "expected hex"),
            ("name = \"ab\"\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "namespaced"),
            ("name = \"a.b\"\n[records]\nsize = 2\nfields = [{ name = \"x\", type = \"u4\" }]".to_string(), "more than 2"),
            (record("{ name = \"x\", type = \"f4\", scale = 2 }"), "integer types"),
            (record("{ name = \"x\", type = \"u4\", scale = 2, enum = { 1 = \"a\" } }"), "one of time"),
            (record("{ name = \"x\", type = \"f4\", null = \"min\" }"), "does not fit"),
            (record("{ name = \"x\", type = \"u4\", flatten = true }"), "flatten"),
            (record("{ name = \"x\", type = \"u4\", of_day = true }"), "of_day goes with time"),
            (record("{ name = \"x\", type = \"u4\", date = \"ddmmyy\" }"), "yyyymmdd"),
            (record("{ name = \"x\", type = \"u4\", time = \"ns\", of_day = true, date = \"trade\" }"), "header field"),
            (record("{ type = \"pad\", size = 2, name = \"p\" }"), "only a size"),
            (record("{ name = \"x\", type = \"u1\", count = 67108864, flatten = true }"), "at most 1024"),
            (record("{ name = \"x\", type = \"u4\", null = -1 }"), "holds 0 to 4294967295"),
            (record("{ name = \"x\", type = \"s1\", null = 128 }"), "holds -128 to 127"),
            ("name = \"a.b\"\nlayout = \"columns\"\n[records]\nfields = [{ name = \"x\", type = \"u1\", file = \"../x\" }]".to_string(), "name of a file"),
            ("name = \"a.b\"\nlayout = \"columns\"\n[records]\nfields = [{ name = \"/etc/x\", type = \"u1\" }]".to_string(), "cannot hold a path"),
        ] {
            let e = Spec::parse(&text, None).unwrap_err();
            assert!(e.message.contains(said), "{text}: {e}");
        }
    }

    /// Every type and meaning, in both byte orders, from one record.
    #[test]
    fn every_field_type_in_both_byte_orders() {
        for (endian, big) in [("le", false), ("be", true)] {
            let text = format!(
                r#"name = "t.all"
endian = "{endian}"
[records]
fields = [
  {{ name = "u1", type = "u1" }}, {{ name = "u2", type = "u2" }}, {{ name = "u3", type = "u3" }},
  {{ name = "u4", type = "u4" }}, {{ name = "u5", type = "u5" }}, {{ name = "u8", type = "u8" }},
  {{ name = "s1", type = "s1" }}, {{ name = "s2", type = "s2" }}, {{ name = "s3", type = "s3" }},
  {{ name = "s4", type = "s4" }}, {{ name = "s6", type = "s6" }}, {{ name = "s8", type = "s8" }},
  {{ name = "f4", type = "f4" }}, {{ name = "f8", type = "f8" }},
  {{ name = "flag", type = "bool" }},
  {{ name = "s", type = "str", size = 3 }}, {{ name = "b", type = "bytes", size = 2 }},
  {{ type = "pad", size = 1 }},
  {{ name = "t", type = "s4", time = "s", epoch = 2000-01-01 }},
  {{ name = "day", type = "u2", time = "days", epoch = "2000-01-01" }},
  {{ name = "ymd", type = "u4", date = "yyyymmdd" }},
  {{ name = "tod", type = "u4", time = "ms", of_day = true }},
  {{ name = "serial", type = "f8", time = "days", epoch = "1899-12-30" }},
  {{ name = "d", type = "s2", scale = 2 }},
  {{ name = "c", type = "s2", factor = 0.5, offset = -40.0 }},
  {{ name = "e", type = "u1", enum = {{ 7 = "seven" }} }},
  {{ name = "n", type = "s4", null = "min" }},
  {{ name = "le", type = "u2le" }}, {{ name = "be", type = "u2be" }},
  {{ name = "arr", type = "u1", count = 3 }},
  {{ name = "lv", type = "u1", count = 2, flatten = true }},
]"#
            );
            let spec = Spec::parse(&text, None).unwrap();
            let mut bytes = Vec::new();
            let put = |bytes: &mut Vec<u8>, le: &[u8]| {
                if big {
                    bytes.extend(le.iter().rev());
                } else {
                    bytes.extend(le);
                }
            };
            bytes.push(200);
            put(&mut bytes, &60_000u16.to_le_bytes());
            put(&mut bytes, &16_000_000u32.to_le_bytes()[..3]);
            put(&mut bytes, &4_000_000_000u32.to_le_bytes());
            put(&mut bytes, &1_099_511_627_775u64.to_le_bytes()[..5]);
            put(&mut bytes, &u64::MAX.to_le_bytes());
            bytes.push(-5i8 as u8);
            put(&mut bytes, &(-300i16).to_le_bytes());
            put(&mut bytes, &(-70_000i32).to_le_bytes()[..3]);
            put(&mut bytes, &(-70_000i32).to_le_bytes());
            put(&mut bytes, &(-1i64).to_le_bytes()[..6]);
            put(&mut bytes, &i64::MIN.to_le_bytes());
            put(&mut bytes, &1.5f32.to_le_bytes());
            put(&mut bytes, &(-2.25f64).to_le_bytes());
            bytes.push(9);
            bytes.extend(b"hi\0");
            bytes.extend([0xde, 0xad]);
            bytes.push(0xff);
            put(&mut bytes, &86_400i32.to_le_bytes());
            put(&mut bytes, &2u16.to_le_bytes());
            put(&mut bytes, &20240229u32.to_le_bytes());
            put(&mut bytes, &34_200_000u32.to_le_bytes());
            put(&mut bytes, &2.5f64.to_le_bytes());
            put(&mut bytes, &(-1234i16).to_le_bytes());
            put(&mut bytes, &100i16.to_le_bytes());
            bytes.push(7);
            put(&mut bytes, &i32::MIN.to_le_bytes());
            bytes.extend(513u16.to_le_bytes());
            bytes.extend(513u16.to_be_bytes());
            bytes.extend([1, 2, 3]);
            bytes.extend([4, 5]);
            let df = collect(&open(&spec, bytes));
            let row: Vec<String> = df
                .columns()
                .iter()
                .map(|c| c.get(0).unwrap().to_string())
                .collect();
            assert_eq!(
                row,
                [
                    "200",
                    "60000",
                    "16000000",
                    "4000000000",
                    "1099511627775",
                    "18446744073709551615",
                    "-5",
                    "-300",
                    "-70000",
                    "-70000",
                    "-1",
                    "-9223372036854775808",
                    "1.5",
                    "-2.25",
                    "true",
                    "\"hi\"",
                    "b\"\\xde\\xad\"",
                    "2000-01-02 00:00:00",
                    "2000-01-03",
                    "2024-02-29",
                    "09:30:00",
                    "1900-01-01 12:00:00",
                    "-12.34",
                    "10.0",
                    "\"seven\"",
                    "null",
                    "513",
                    "513",
                    "[1, 2, 3]",
                    "4",
                    "5",
                ],
                "{endian}"
            );
            assert_eq!(
                df.get_column_names(),
                [
                    "u1", "u2", "u3", "u4", "u5", "u8", "s1", "s2", "s3", "s4", "s6", "s8", "f4",
                    "f8", "flag", "s", "b", "t", "day", "ymd", "tod", "serial", "d", "c", "e", "n",
                    "le", "be", "arr", "lv_0", "lv_1"
                ]
            );
        }
    }

    #[test]
    fn a_time_of_day_takes_its_date_from_the_header() {
        let text = r#"name = "t.tod"
[header]
fields = [{ name = "trade_date", type = "u4", date = "yyyymmdd" }]
[records]
fields = [{ name = "ts", type = "u6be", time = "ns", of_day = true, date = "header.trade_date" }]"#;
        let spec = Spec::parse(text, None).unwrap();
        let mut bytes = 20240102u32.to_le_bytes().to_vec();
        bytes.extend(&34_200_000_000_123u64.to_be_bytes()[2..]);
        let df = collect(&open(&spec, bytes));
        assert_eq!(cell(&df, "ts", 0), "2024-01-02 09:30:00.000000123");
    }

    #[test]
    fn a_bad_magic_is_refused_saying_what_was_found() {
        let spec = Spec::parse(L2, None).unwrap();
        let mut bytes = l2_file(&[(1, "A", 1, 1)], 1, &[]);
        bytes[..4].copy_from_slice(b"NOPE");
        let e = spec
            .open_rows(Arc::new(Bytes::Owned(bytes)), "x.l2")
            .err()
            .unwrap();
        assert!(
            e.contains("expected magic 4c 32 46 44 at byte 0, found 4e 4f 50 45"),
            "{e}"
        );
    }

    #[test]
    fn a_truncated_last_record_is_left_out_and_shown() {
        let text = "name = \"t.x\"\n[records]\nfields = [{ name = \"a\", type = \"u4\" }]";
        let spec = Spec::parse(text, None).unwrap();
        let bytes = vec![1, 0, 0, 0, 2, 0, 0, 0, 0xab, 0xcd];
        let opened = spec
            .open_rows(Arc::new(Bytes::Owned(bytes)), "x.bin")
            .unwrap();
        assert_eq!(collect(&opened).height(), 2);
        assert_eq!(
            opened.notes,
            ["x.bin ends with 2 bytes that are not a whole record, left out: ab cd"]
        );
        // The header's count says more than is there: the whole records are shown.
        let spec = Spec::parse(L2, None).unwrap();
        let opened = open(&spec, l2_file(&[(1, "A", 1, 1)], 3, &[9]));
        assert_eq!(collect(&opened).height(), 1);
        assert!(
            opened.notes[0].contains("says 3 records"),
            "{:?}",
            opened.notes
        );
    }

    #[test]
    fn sizes_come_from_the_header_and_are_bounded() {
        let text = r#"name = "t.h"
[header]
fields = [{ name = "len", type = "u1" }, { name = "title", type = "str", size = "len" }, { name = "rec", type = "u2" }]
[records]
size = "header.rec"
size_adjust = 1
fields = [{ name = "a", type = "u1" }, { name = "b", type = "bytes", size = "header.len" }]"#;
        let spec = Spec::parse(text, None).unwrap();
        let mut bytes = vec![2, b'h', b'i', 3, 0];
        bytes.extend([1, 0xa, 0xb, 0xff, 2, 0xc, 0xd, 0xff]);
        let opened = open(&spec, bytes);
        let df = collect(&opened);
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.column("a").unwrap().u8().unwrap().to_vec(),
            [Some(1), Some(2)]
        );
        assert_eq!(opened.header.text("title").as_deref(), Some("hi"));
        // A size the file gives past the bound is refused, not believed.
        let text = r#"name = "t.h"
[header]
fields = [{ name = "len", type = "u8" }]
[records]
fields = [{ name = "b", type = "bytes", size = "header.len" }]"#;
        let spec = Spec::parse(text, None).unwrap();
        let e = spec
            .open_rows(Arc::new(Bytes::Owned(u64::MAX.to_le_bytes().to_vec())), "h")
            .err()
            .unwrap();
        assert!(e.contains("outside 0 to"), "{e}");
    }

    #[test]
    fn a_columns_layout_reads_one_file_per_field() {
        let dir = tempfile::tempdir().unwrap();
        let text = r#"name = "kdb.trades"
layout = "columns"
match = { glob = "trades" }
[header]
size = 8
[records]
fields = [{ name = "price", type = "f8" }, { name = "size", type = "s4", file = "qty" }]"#;
        let spec = Spec::parse(text, None).unwrap();
        let mut price = vec![0u8; 8];
        for p in [1.5f64, 2.5, 3.5] {
            price.extend(p.to_le_bytes());
        }
        let mut qty = vec![0u8; 8];
        for q in [10i32, 20] {
            qty.extend(q.to_le_bytes());
        }
        std::fs::write(dir.path().join("price"), price).unwrap();
        std::fs::write(dir.path().join("qty"), qty).unwrap();
        let opened = spec.open(dir.path(), "trades").unwrap();
        let df = collect(&opened);
        assert_eq!(df.height(), 2);
        assert_eq!(
            df.column("size").unwrap().i32().unwrap().to_vec(),
            [Some(10), Some(20)]
        );
        assert!(
            opened.notes[0].contains("price 3, qty 2"),
            "{:?}",
            opened.notes
        );
        let registry = Registry::of(vec![spec]);
        assert_eq!(registry.by_glob(Path::new("/db/trades"), true).len(), 1);
        assert!(registry.by_glob(Path::new("/db/trades"), false).is_empty());
    }

    /// A header sized by its own field may differ from file to file; each column
    /// starts after its own file's header.
    #[test]
    fn each_column_file_starts_after_its_own_header() {
        let dir = tempfile::tempdir().unwrap();
        let text = r#"name = "t.cols"
layout = "columns"
[header]
fields = [{ name = "len", type = "u1" }]
size = "len"
[records]
fields = [{ name = "a", type = "u1" }, { name = "b", type = "u1" }]"#;
        let spec = Spec::parse(text, None).unwrap();
        std::fs::write(dir.path().join("a"), [2, 0, 7, 8]).unwrap();
        std::fs::write(dir.path().join("b"), [4, 0, 0, 0, 9, 10]).unwrap();
        let df = collect(&spec.open(dir.path(), "t").unwrap());
        assert_eq!(
            df.column("a").unwrap().u8().unwrap().to_vec(),
            [Some(7), Some(8)]
        );
        assert_eq!(
            df.column("b").unwrap().u8().unwrap().to_vec(),
            [Some(9), Some(10)]
        );
    }

    #[test]
    fn the_search_path_keeps_the_first_of_each_name() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        std::fs::write(a.path().join("l2.toml"), L2).unwrap();
        std::fs::write(b.path().join("l2.toml"), L2.replace("*.l2", "*.lvl2")).unwrap();
        std::fs::write(b.path().join("bad.toml"), "name = 3").unwrap();
        std::fs::write(b.path().join("notes.txt"), "not a spec").unwrap();
        let path = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        let registry = Registry::load(&path);
        assert_eq!(registry.specs.len(), 1);
        assert_eq!(registry.specs[0].spec.globs, ["*.l2"]);
        assert_eq!(registry.specs[0].overrides, [b.path().join("l2.toml")]);
        assert_eq!(registry.errors.len(), 1);
        let listing = registry.listing(&path);
        assert!(listing.contains("overrides"), "{listing}");
        assert!(
            listing.contains("bad.toml:1:8: name: expected a string"),
            "{listing}"
        );
    }

    #[test]
    fn glob_comes_before_magic_and_ties_are_kept() {
        let one = Spec::parse(L2, None).unwrap();
        let two = Spec::parse(&L2.replace("acme.l2feed", "acme.other"), None).unwrap();
        let magic_only = Spec::parse(
            &L2.replace("acme.l2feed", "acme.magic")
                .replace("glob = [\"*.l2\"], ", ""),
            None,
        )
        .unwrap();
        let registry = Registry::of(vec![one, two, magic_only]);
        let matched = registry
            .matching(Path::new("a.l2"), false, |_| {
                panic!("no read for a glob match")
            })
            .unwrap();
        assert_eq!(matched.by, Chosen::Glob);
        assert_eq!(matched.specs.len(), 2);
        let matched = registry
            .matching(Path::new("a.dat"), false, |n| {
                assert_eq!(n, 4);
                Some(b"L2FD".to_vec())
            })
            .unwrap();
        assert_eq!(matched.by, Chosen::Magic);
        assert_eq!(matched.specs.len(), 3);
        assert!(
            registry
                .matching(Path::new("a.dat"), false, |_| Some(b"nope".to_vec()))
                .is_none()
        );
    }

    /// One spec per version, told apart by a header field after the magic.
    #[test]
    fn a_header_version_picks_the_spec() {
        let version = |v: u8| {
            format!(
                r#"name = "acme.v{v}"
match = {{ glob = "*.l2", magic = "L2FD", where = {{ "header.version" = {v} }} }}
[header]
fields = [{{ type = "pad", size = 4 }}, {{ name = "version", type = "u1" }}]
[records]
fields = [{{ name = "x", type = "u1" }}]"#
            )
        };
        let registry = Registry::of(vec![
            Spec::parse(&version(2), None).unwrap(),
            Spec::parse(&version(3), None).unwrap(),
        ]);
        for v in [2u8, 3] {
            let matched = registry
                .matching(Path::new("a.l2"), false, |n| {
                    assert_eq!(n, 5);
                    Some([b"L2FD".as_slice(), &[v]].concat())
                })
                .unwrap();
            let names: Vec<&str> = matched.specs.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(names, [format!("acme.v{v}")]);
        }
        let e = Spec::parse(
            &version(2).replace("\"header.version\"", "\"header.nope\""),
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("no header field named `nope`"), "{e}");
    }

    #[test]
    fn the_search_path_is_config_dir_then_env_then_config() {
        let env = std::env::join_paths(["/org/a", "/org/b"]).unwrap();
        let path = search_path(Some(Path::new("/cfg")), Some(env), &["/c".to_string()]);
        assert_eq!(
            path,
            [
                PathBuf::from("/cfg/formats"),
                PathBuf::from("/org/a"),
                PathBuf::from("/org/b"),
                PathBuf::from("/c")
            ]
        );
    }

    #[test]
    fn formats_check_validates_and_prints_the_first_rows() {
        let dir = tempfile::tempdir().unwrap();
        let spec = dir.path().join("l2.toml");
        std::fs::write(&spec, L2).unwrap();
        let data = dir.path().join("day.l2");
        std::fs::write(&data, l2_file(&[(1, "AAPL", 1, 10_000)], 1, &[7])).unwrap();
        let registry = Registry::default();
        let text = check(
            &spec.to_string_lossy(),
            Some(&data),
            &registry,
            &crate::OpenOptions::default(),
        )
        .unwrap();
        assert!(text.starts_with("acme.l2feed: ok"), "{text}");
        assert!(text.contains("warning: day.l2 has 1 byte after"), "{text}");
        assert!(
            text.contains("AAPL") && text.contains("1 records"),
            "{text}"
        );
        std::fs::write(&spec, "name = \"a.b\"\n[records]\nfields = 1").unwrap();
        let e = check(
            &spec.to_string_lossy(),
            None,
            &registry,
            &crate::OpenOptions::default(),
        )
        .unwrap_err();
        assert!(e.contains("l2.toml:3:10: fields: expected an array"), "{e}");
        assert!(
            check(
                "acme.nothing",
                None,
                &registry,
                &crate::OpenOptions::default()
            )
            .is_err()
        );
    }

    const LOG: &str = r##"
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }

comment_char = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
"##;

    const LOG_TEXT: &str = "#device_info, log_version=\"1.03\", model=\"X\"\n#yyyy-mm-dd, hh:mm:ss, hh:mm, deg F\n  Lcl Date, Lcl Time, UTCOfst, E1 CHT1\n          ,         ,       ,   187.2\n2024-03-01, 10:00:00, -05:00,   180.0\n";

    #[test]
    fn the_delimited_example_parses() {
        use crate::delimited_spec::{DerivedKind, HeaderRows};
        let spec = Spec::parse(LOG, None).unwrap();
        assert!(spec.is_delimited());
        assert_eq!(spec.magic, b"#device_info");
        let d = spec.delimited.as_deref().unwrap();
        assert_eq!(d.comment_char.as_deref(), Some("#"));
        assert_eq!(d.skip_initial_space, Some(true));
        assert_eq!(
            d.header_rows,
            Some(HeaderRows {
                name: vec![3],
                unit: Some(2)
            })
        );
        assert_eq!(d.metadata_line, Some(1));
        assert_eq!(d.columns[0].name, "time");
        assert_eq!(d.columns[0].kind, DerivedKind::Datetime);
        assert_eq!(d.columns[0].from.len(), 3);
        // A list of name lines joins them, as Frictionless does.
        let joined = Spec::parse(
            "name = \"a.b\"\nkind = \"delimited\"\nheader_rows = [1, 2]\ndelimiter = \"\\t\"\nnull_value = [\"NA\", \"x=-1\"]",
            None,
        )
        .unwrap();
        let d = joined.delimited.as_deref().unwrap();
        assert_eq!(d.header_rows.as_ref().unwrap().name, [1, 2]);
        assert_eq!(d.delimiter, Some(b'\t'));
        assert_eq!(d.null_values, ["NA", "x=-1"]);
        // A binary spec is not one.
        assert!(!Spec::parse(L2, None).unwrap().is_delimited());
    }

    #[test]
    fn delimited_spec_errors_point_at_the_line_and_column() {
        let head = "name = \"a.b\"\nkind = \"delimited\"\n";
        for (rest, said) in [
            (
                "records = 1",
                "3:1: unknown key `records` in a delimited spec",
            ),
            ("header_rows = { unit = 2 }", "header_rows: missing `name`"),
            (
                "header_rows = { name = 2, unit = 2 }",
                "header_rows.unit: line 2 is also a name line",
            ),
            (
                "header_rows = { name = 2, description = 1 }",
                "header_rows.description is not yet supported",
            ),
            (
                "header_rows = 0",
                "header_rows: expected a line from 1 to 1000",
            ),
            ("header_rows = [2, 2]", "header_rows: line 2 is named twice"),
            (
                "header_rows = 2\nmetadata_line = 5",
                "4:17: metadata_line: line 5 would be read as data",
            ),
            (
                "header_rows = 2\nmetadata_line = 2",
                "metadata_line: line 2 is a header line",
            ),
            ("delimiter = \"ab\"", "delimiter: expected one character"),
            ("comment_char = \"\"", "comment_char: must not be empty"),
            (
                "match = { where = { \"header.v\" = 1 } }",
                "where compares a binary header's fields",
            ),
            (
                "[columns]\nt = { from = [\"a\", \"b\"], as = \"date\" }",
                "columns.t.from: as = \"date\" takes one column",
            ),
            (
                "[columns]\nt = { from = \"a\", as = \"instant\" }",
                "columns.t.as: expected datetime, date or time",
            ),
            (
                "[columns]\nt = { as = \"date\" }",
                "columns.t: missing `from`",
            ),
            ("kind = \"text\"", "kind: expected binary or delimited"),
        ] {
            let text = format!("{head}{rest}");
            let text = text.replacen(
                "kind = \"delimited\"\nkind = \"text\"",
                "kind = \"text\"",
                1,
            );
            let e = Spec::parse(&text, None).unwrap_err().to_string();
            assert!(e.contains(said), "{rest}: {e}");
        }
        // A metadata line below the header lines is fine when it is a comment line.
        let text = format!("{head}header_rows = 2\ncomment_char = \"#\"\nmetadata_line = 5");
        assert!(Spec::parse(&text, None).is_ok());
    }

    #[test]
    fn a_delimited_spec_matches_csv_names_and_a_binary_spec_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("flight.csv");
        std::fs::write(&csv, LOG_TEXT).unwrap();
        let binary = "name = \"acme.raw\"\nmatch = { glob = \"*.csv\", magic = \"#dev\" }\n[records]\nfields = [{ name = \"b\", type = \"u1\" }]";
        let registry = Registry::of(vec![
            Spec::parse(binary, None).unwrap(),
            Spec::parse(LOG, None).unwrap(),
        ]);
        let asked = Asked::default();
        match route(&csv, &asked, &registry).unwrap() {
            Route::Delimited(choice) => {
                assert_eq!(choice.spec.name, "acme.instrument-log");
                assert_eq!(choice.by, Chosen::Magic);
            }
            _ => panic!("the delimited spec reads the CSV"),
        }
        // Several files: only a delimited spec is asked about.
        let several = Asked {
            spec_name: Some("acme.raw".into()),
            text_only: true,
            ..Asked::default()
        };
        let Err(e) = route(&csv, &several, &registry) else {
            panic!("a binary spec does not read several files");
        };
        assert!(e.contains("reads one file"), "{e}");
        // The magic is the start of the first line, after a byte-order mark.
        let bom = dir.path().join("bom.csv");
        std::fs::write(&bom, format!("\u{feff}{LOG_TEXT}")).unwrap();
        assert!(matches!(
            route(&bom, &asked, &registry).unwrap(),
            Route::Delimited(_)
        ));
        // A CSV whose first line is not the magic is read as it always was.
        let plain = dir.path().join("plain.csv");
        std::fs::write(&plain, "a,b\n1,2\n").unwrap();
        assert!(matches!(
            route(&plain, &asked, &registry).unwrap(),
            Route::Elsewhere
        ));
    }

    #[test]
    fn formats_check_prints_a_delimited_file_s_metadata_units_and_rows() {
        let dir = tempfile::tempdir().unwrap();
        let spec = dir.path().join("log.toml");
        std::fs::write(&spec, LOG).unwrap();
        let data = dir.path().join("flight.csv");
        std::fs::write(&data, LOG_TEXT).unwrap();
        let registry = Registry::default();
        let text = check(
            &spec.to_string_lossy(),
            Some(&data),
            &registry,
            &crate::OpenOptions::default(),
        )
        .unwrap();
        assert!(text.starts_with("acme.instrument-log: ok"), "{text}");
        assert!(
            text.contains("delimited: names on line 3, units on line 2, metadata on line 1"),
            "{text}"
        );
        assert!(
            text.contains("time = datetime from Lcl Date, Lcl Time, UTCOfst"),
            "{text}"
        );
        assert!(
            text.contains("metadata: device_info: log_version = 1.03, model = X"),
            "{text}"
        );
        assert!(text.contains("E1 CHT1 = deg F"), "{text}");
        assert!(text.contains("2024-03-01 15:00:00 UTC"), "{text}");
        let listing = Registry::of(vec![Spec::parse(LOG, None).unwrap()]).listing(&[]);
        assert!(
            listing.contains("acme.instrument-log  (delimited; magic \"#device_info\")"),
            "{listing}"
        );
    }

    /// DBC files share the search path: a `.dbc` file for every interface, a
    /// `kind = "dbc"` TOML file that names one for an interface, and one that does not
    /// parse is an error with its line.
    #[test]
    fn dbc_files_on_the_search_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("l2.toml"), L2).unwrap();
        std::fs::write(
            dir.path().join("car.dbc"),
            "BO_ 291 ENGINE: 8 ECU\n SG_ Speed : 0|16@1+ (0.125,0) [0|8191] \"rpm\" GW\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("body.toml"),
            "kind = \"dbc\"\nfile = \"body/body.dbc\"\n[match]\ninterface = \"can1\"\n",
        )
        .unwrap();
        std::fs::create_dir(dir.path().join("body")).unwrap();
        std::fs::write(
            dir.path().join("body/body.dbc"),
            "BO_ 512 DOORS: 1 GW\n SG_ Open : 0|1@1+ (1,0) [0|1] \"\" ECU\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("bad.dbc"), "BO_ 1 A: 8 X\n SG_ nope\n").unwrap();
        let path = vec![dir.path().to_path_buf()];
        let registry = Registry::load(&path);
        assert_eq!(registry.specs.len(), 1);
        let names: Vec<(&str, Option<&str>)> = registry
            .dbc
            .iter()
            .map(|f| (f.dbc.name.as_str(), f.dbc.interface.as_deref()))
            .collect();
        assert_eq!(names, [("body", Some("can1")), ("car", None)]);
        assert_eq!(registry.errors.len(), 1, "{:?}", registry.errors);
        assert_eq!(registry.errors[0].line, 2);
        let listing = registry.listing(&path);
        assert!(listing.contains("DBC files:"), "{listing}");
        assert!(
            listing.contains("body  (1 message, interface can1)"),
            "{listing}"
        );
    }

    /// FIX dictionaries share the search path: a `kind = "fix"` TOML file and a
    /// QuickFIX XML file are listed apart from the specs, an XML file that is not one is
    /// passed over, and `formats check` reads a log with one.
    #[test]
    fn fix_dictionaries_on_the_search_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("l2.toml"), L2).unwrap();
        std::fs::write(
            dir.path().join("broker.toml"),
            "name = \"acme.fix.broker-x\"\nkind = \"fix\"\nmatch = { sender = \"BROKERX\" }\ntags = { 9001 = \"AlgoName\" }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("FIX44-custom.xml"),
            "<fix major='4' minor='4'><fields><field number='5001' name='Desk' type='STRING'/></fields></fix>",
        )
        .unwrap();
        std::fs::write(dir.path().join("other.xml"), "<gpx/>").unwrap();
        std::fs::write(
            dir.path().join("broken.toml"),
            "name = \"acme.fix.bad\"\nkind = \"fix\"\ntags = { nine = \"X\" }\n",
        )
        .unwrap();
        let path = vec![dir.path().to_path_buf()];
        let registry = Registry::load(&path);
        assert_eq!(registry.specs.len(), 1);
        let names: Vec<&str> = registry.fix.iter().map(|f| f.dict.name.as_str()).collect();
        assert_eq!(names, ["FIX44-custom", "acme.fix.broker-x"]);
        assert_eq!(registry.errors.len(), 1, "{:?}", registry.errors);
        assert!(registry.errors[0].to_string().contains("tags.nine"));
        let listing = registry.listing(&path);
        assert!(listing.contains("FIX dictionaries:"), "{listing}");
        assert!(
            listing.contains("acme.fix.broker-x  (sender BROKERX)"),
            "{listing}"
        );
        assert!(
            listing.contains("FIX44-custom  (begin string FIX.4.4)"),
            "{listing}"
        );

        let log = dir.path().join("session.log");
        std::fs::write(
            &log,
            "8=FIX.4.4|9=20|35=0|49=BROKERX|9001=x|10=000|\n8=FIX.4.4|9=5|35=0|49=OTHER|10=000|\n",
        )
        .unwrap();
        let text = check(
            "acme.fix.broker-x",
            Some(&log),
            &registry,
            &crate::OpenOptions::default(),
        )
        .unwrap();
        assert!(text.starts_with("acme.fix.broker-x: ok"), "{text}");
        assert!(text.contains("matches sender BROKERX"), "{text}");
        assert!(text.contains("2 messages, 1 of them matched"), "{text}");
        assert!(text.contains("names in the log: 9001 AlgoName"), "{text}");
        let e = check(
            &dir.path().join("broken.toml").to_string_lossy(),
            None,
            &registry,
            &crate::OpenOptions::default(),
        )
        .unwrap_err();
        assert!(e.contains("tags.nine"), "{e}");
    }
}

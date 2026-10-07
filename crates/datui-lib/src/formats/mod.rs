//! Binary formats described by specs: one TOML file per format, on a search path. A
//! spec names its format (`acme.l2feed`), says which files match (glob, magic, header
//! values), and lays out header and records. Specs are data (no expressions or code:
//! fields refer to earlier ones by name), and every size read from a file is bounded.
//! Decoding is [`crate::fixed_records`]' for fixed-size records and
//! [`crate::framed_records`]' otherwise; [`files`] reads a directory of one spec's
//! files. This module turns a spec and a file into that reader's columns.

use crate::catalog::ColumnNote;
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

/// Said as every reader error is, with the place in the text a compiler gives:
/// `"spec.toml":3:7: <what went wrong>.`
impl std::fmt::Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let at = (self.line > 0).then_some((self.line, self.column));
        f.write_str(&crate::error_display::located_message(
            self.path.as_deref(),
            at,
            &self.message,
        ))
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
    /// What the column means: documentation, never read.
    pub description: Option<String>,
    /// The column's unit: documentation, never read.
    pub unit: Option<String>,
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
    /// What a record of this type is: documentation, never read.
    pub description: Option<String>,
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

/// One condition of a spec's `match`, as home, Info and `datui formats` show it (`magic
/// MKTD`, `version 1`, `glob *.bin *.dat`). Chips must all hold, except that a file the
/// glob names is not asked its magic; alternatives live inside one chip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchChip {
    pub name: String,
    pub value: String,
    pub kind: ChipKind,
    /// Where the magic sits, when not at the start.
    pub offset: Option<u64>,
}

/// What a chip's value is, for its color and whether plain text quotes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChipKind {
    /// Printable magic bytes, as text. Never quoted: a magic is bytes either way.
    Magic,
    /// Magic bytes in hex.
    Hex,
    /// A header value that is text. Quoted where no color tells it from a number.
    Text,
    /// A header value that is a number.
    Int,
    /// File name patterns, any of which names the file.
    Glob,
}

/// The most of a magic a chip shows before it cuts it.
const CHIP_MAGIC_CHARS: usize = 16;
const CHIP_MAGIC_BYTES: usize = 8;

impl MatchChip {
    /// The value as plain text: text header values quoted when `quote`.
    pub fn value_text(&self, quote: bool) -> String {
        let value = match self.kind {
            ChipKind::Text if quote => format!("\"{}\"", self.value),
            _ => self.value.clone(),
        };
        match self.offset {
            Some(at) => format!("{value} @ {at}"),
            None => value,
        }
    }

    /// `name value`, as plain text.
    pub fn plain(&self, quote: bool) -> String {
        format!("{} {}", self.name, self.value_text(quote))
    }
}

/// Chips as one line of plain text, for the command line and the Info panel:
/// `magic MKTD · version 1 · glob *.bin *.dat`.
pub fn chips_plain(chips: &[MatchChip]) -> String {
    let sep = format!(" {} ", crate::glyphs::get().middot);
    chips
        .iter()
        .map(|c| c.plain(true))
        .collect::<Vec<_>>()
        .join(&sep)
}

/// The most a spec file may hold, local or remote: far more than any spec needs, and
/// a bound on what `--format FILE` reads before it knows what it read.
pub const MAX_SPEC_BYTES: u64 = 1 << 20;
/// [`MAX_SPEC_BYTES`], as the user is told it.
pub const MAX_SPEC_SAID: &str = "1 MiB";

/// One format, as its spec describes it.
#[derive(Debug, Clone)]
pub struct Spec {
    pub name: String,
    pub description: Option<String>,
    /// An `https://` link to the format's own documentation.
    pub documentation: Option<String>,
    /// A delimited spec's `[columns]` notes: what each column of the file means.
    pub notes: Vec<(String, ColumnNote)>,
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
    /// The variant read alone (`--table`), when one is.
    pub variant: Option<String>,
}

impl PartialEq for Spec {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.description == other.description
            && self.documentation == other.documentation
            && self.notes == other.notes
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

mod command;
pub mod files;
mod layout;
mod matching;
mod open;
mod parse;
mod registry;
mod types;

pub use command::*;
pub use layout::*;
pub use matching::*;
pub(crate) use parse::*;
pub use registry::*;
pub use types::*;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod docs_tests;

#[cfg(test)]
mod chip_tests;

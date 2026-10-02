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
use polars::prelude::{AnyValue, PlSmallStr, TimeUnit};
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
    /// table.
    Columns,
}

/// How one record is told from the next. Only `fixed` is read so far; the spec keeps
/// the name so the other framings arrive without changing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    Fixed,
}

/// A size or a count: written in the spec, or read from a header field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Amount {
    Given(u64),
    /// The value of header field `field`, plus `adjust`.
    Header {
        field: String,
        adjust: i64,
    },
}

/// What a field's bytes hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// 1 to 8 bytes.
    Unsigned(u8),
    Signed(u8),
    /// 4 or 8 bytes.
    Float(u8),
    Bool,
    Str,
    Bytes,
    Pad,
}

impl Type {
    /// Bytes one value of the type takes, when the type says.
    pub fn width(self) -> Option<u64> {
        self.physical().width().map(|w| w as u64)
    }

    fn is_integer(self) -> bool {
        matches!(self, Self::Unsigned(_) | Self::Signed(_))
    }

    fn is_number(self) -> bool {
        matches!(self, Self::Unsigned(_) | Self::Signed(_) | Self::Float(_))
    }

    fn physical(self) -> Physical {
        match self {
            Self::Unsigned(n) => Physical::Unsigned(n),
            Self::Signed(n) => Physical::Signed(n),
            Self::Float(n) => Physical::Float(n),
            Self::Bool => Physical::Bool,
            Self::Str => Physical::Text,
            Self::Bytes | Self::Pad => Physical::Raw,
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
}

/// A spec's header: its fields, and its size when that is more than they take.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Header {
    pub fields: Vec<Field>,
    pub size: Option<Amount>,
}

/// A spec's records.
#[derive(Debug, Clone, PartialEq)]
pub struct Records {
    pub framing: Framing,
    pub fields: Vec<Field>,
    /// At least the fields' sizes; the rest of each record is skipped.
    pub size: Option<Amount>,
    /// How many records there are, when the file says.
    pub count: Option<Amount>,
}

/// A header value a file must hold to match: `match.where`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expected {
    Int(i128),
    Text(String),
}

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
    pub layout: Layout,
    pub header: Header,
    pub records: Records,
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
            && self.header == other.header
            && self.records == other.records
    }
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
];

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

    /// The earlier field `reference` names: `header.NAME`, or `NAME` for an earlier
    /// field of the same part. A record's own fields cannot size it: that is
    /// `length_prefixed` framing.
    fn earlier<'f>(
        &self,
        value: &Value<'_>,
        reference: &str,
        what: &str,
        part: Part,
        earlier: &'f [Field],
        header: &'f [Field],
    ) -> Result<&'f Field, SpecError> {
        let (scope, field) = match reference.split_once('.') {
            Some((scope, field)) => (Some(scope), field),
            None => (None, reference),
        };
        let fields = match (scope, part) {
            (Some("header"), _) => header,
            (None, Part::Header) => earlier,
            (None, Part::Records) => {
                let message = if earlier.iter().any(|f| f.name.as_deref() == Some(field)) {
                    format!(
                        "{what} = \"{field}\" reads from the record itself, which needs framing = \"length_prefixed\"; that framing is not yet supported"
                    )
                } else {
                    format!(
                        "{what}: no earlier field named `{field}`; a header field is named `header.{field}`"
                    )
                };
                return Err(self.error(&value.span(), message));
            }
            (Some(other), _) => {
                return Err(self.error(
                    &value.span(),
                    format!("{what}: `{other}.` is not a part; expected `header.NAME`"),
                ));
            }
        };
        fields
            .iter()
            .find(|f| f.name.as_deref() == Some(field))
            .ok_or_else(|| {
                let part = match (scope, part) {
                    (None, Part::Records) | (Some(_), _) => "header",
                    (None, Part::Header) => "header",
                };
                self.error(
                    &value.span(),
                    format!("{what}: no earlier {part} field named `{field}`"),
                )
            })
    }

    /// A size or a count: a whole number, or the name of an earlier plain integer
    /// field, with an optional `adjust`.
    fn amount(
        &self,
        value: &Value<'_>,
        adjust: Option<&Value<'_>>,
        what: &str,
        part: Part,
        earlier: &[Field],
        header: &[Field],
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
            DeValue::String(reference) => {
                let target = self.earlier(value, reference, what, part, earlier, header)?;
                if !target.ty.is_integer()
                    || target.meaning != Meaning::Plain
                    || target.count.is_some()
                {
                    return Err(self.error(
                        &value.span(),
                        format!("{what}: `{reference}` is not a plain integer field"),
                    ));
                }
                Ok(Amount::Header {
                    field: target.name.clone().expect("a referenced field is named"),
                    adjust: adjust.unwrap_or(0),
                })
            }
            _ => Err(self.error(
                &value.span(),
                format!("{what}: expected a whole number or the name of an earlier field"),
            )),
        }
    }

    fn fields(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        header: &[Field],
    ) -> Result<Vec<Field>, SpecError> {
        let DeValue::Array(items) = value.get_ref() else {
            return Err(self.error(
                &value.span(),
                "fields: expected an array of tables, such as [{ name = \"ts\", type = \"u8\" }]",
            ));
        };
        let mut fields: Vec<Field> = Vec::new();
        let mut names = std::collections::HashSet::new();
        for item in items {
            let field = self.field(item, part, layout, &fields, header)?;
            for name in output_names(&field) {
                if !names.insert(name.clone()) {
                    return Err(self.error(&item.span(), format!("a second field named `{name}`")));
                }
            }
            fields.push(field);
        }
        Ok(fields)
    }

    fn field(
        &self,
        value: &Value<'_>,
        part: Part,
        layout: Layout,
        earlier: &[Field],
        header: &[Field],
    ) -> Result<Field, SpecError> {
        let table = self.table(value, "field")?;
        let keys = self.entries(table, "a field", FIELD_KEYS)?;
        let at = value.span();
        let Some(ty_value) = keys.get("type") else {
            return Err(self.error(&at, "field: missing `type`"));
        };
        let (ty, endian) = parse_type(&self.string(ty_value, "type")?).ok_or_else(|| {
            self.error(
                &ty_value.span(),
                "type: expected u1 to u8, s1 to s8, f4, f8 (each with an optional le or be), bool, str, bytes or pad",
            )
        })?;
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
        let size = match (ty.width(), keys.get("size")) {
            (Some(width), Some(v)) => {
                return Err(self.error(
                    &v.span(),
                    format!(
                        "size: a {} is {width} bytes; size is for str, bytes and pad",
                        type_name(ty)
                    ),
                ));
            }
            (Some(_), None) => None,
            (None, Some(v)) => Some(self.amount(
                v,
                keys.get("size_adjust").copied(),
                "size",
                part,
                earlier,
                header,
            )?),
            (None, None) => {
                return Err(self.error(&at, format!("{}: missing `size`", type_name(ty))));
            }
        };
        if size.is_none()
            && let Some(v) = keys.get("size_adjust")
        {
            return Err(self.error(&v.span(), "size_adjust goes with a size read from a field"));
        }
        let count = keys
            .get("count")
            .map(|v| {
                let count = self.amount(v, None, "count", part, earlier, header)?;
                if count == Amount::Given(0) {
                    return Err(self.error(&v.span(), "count: expected at least 1"));
                }
                Ok(count)
            })
            .transpose()?;
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
                Some(Amount::Given(_)) => {}
                _ => {
                    return Err(self.error(
                        &v.span(),
                        "flatten: goes with a count written in the spec, such as count = 10",
                    ));
                }
            }
        }

        let null =
            keys.get("null")
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
                        (Null::Min | Null::Max, t) => t.is_integer(),
                        (Null::NaN, t) => matches!(t, Type::Float(_)),
                        (Null::Value(_), t) => t.is_number() || t == Type::Bool,
                    };
                    if !fits {
                        return Err(self
                            .error(&v.span(), format!("null: does not fit a {}", type_name(ty))));
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
                if part == Part::Header || layout == Layout::Rows {
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
        // Its name names its file.
        if layout == Layout::Columns
            && part == Part::Records
            && file.is_none()
            && let Some(name) = &name
            && !is_file_name(name)
        {
            return Err(self.error(
                &keys["name"].span(),
                "name: names the column's file, so it cannot hold a path; give the file with file = \"...\"",
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
        })
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

/// The columns a field shows as: none for `pad`, `name_0`... when flattened.
fn output_names(field: &Field) -> Vec<String> {
    let Some(name) = &field.name else {
        return Vec::new();
    };
    match (&field.count, field.flatten) {
        (Some(Amount::Given(n)), true) => (0..*n).map(|i| format!("{name}_{i}")).collect(),
        _ => vec![name.clone()],
    }
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
        ('f', 4 | 8) => Type::Float(width),
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
        Type::Str => "str".into(),
        Type::Bytes => "bytes".into(),
        Type::Pad => "pad".into(),
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
        let top = reader.entries(
            document.get_ref(),
            "the spec",
            &[
                "name",
                "description",
                "match",
                "endian",
                "layout",
                "header",
                "records",
                "footer",
                "variants",
            ],
        )?;
        let whole = 0..0;
        for later in ["footer", "variants"] {
            if let Some(v) = top.get(later) {
                return Err(reader.error(&v.span(), format!("`{later}` is not yet supported")));
            }
        }
        let name = match top.get("name") {
            Some(v) => {
                let name = reader.string(v, "name")?;
                if !is_spec_name(&name) {
                    return Err(reader.error(
                        &v.span(),
                        "name: expected a namespaced name of letters, digits, `_` and `-`, such as acme.l2feed",
                    ));
                }
                name
            }
            None => {
                return Err(reader.error(&whole, "missing `name`, such as name = \"acme.l2feed\""));
            }
        };
        let description = top
            .get("description")
            .map(|v| reader.string(v, "description"))
            .transpose()?;
        let endian = match top.get("endian") {
            None => Endian::Little,
            Some(v) => match reader.string(v, "endian")?.as_str() {
                "le" => Endian::Little,
                "be" => Endian::Big,
                _ => return Err(reader.error(&v.span(), "endian: expected le or be")),
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
                header.fields = reader.fields(f, Part::Header, layout, &[])?;
            }
            if let Some(s) = keys.get("size") {
                header.size = Some(reader.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Header,
                    &header.fields,
                    &header.fields,
                )?);
            }
        }

        let (mut globs, mut magic, mut magic_offset) = (Vec::new(), Vec::new(), 0u64);
        let mut glob_set = None;
        let mut expect = Vec::new();
        if let Some(v) = top.get("match") {
            let table = reader.table(v, "match")?;
            let keys =
                reader.entries(table, "match", &["glob", "magic", "magic_offset", "where"])?;
            if let Some(g) = keys.get("glob") {
                globs = match g.get_ref() {
                    DeValue::String(s) => vec![s.to_string()],
                    DeValue::Array(items) => items
                        .iter()
                        .map(|item| reader.string(item, "glob"))
                        .collect::<Result<_, _>>()?,
                    _ => {
                        return Err(
                            reader.error(&g.span(), "glob: expected a string or a list of them")
                        );
                    }
                };
                let mut builder = GlobSetBuilder::new();
                for glob in &globs {
                    builder.add(Glob::new(glob).map_err(|e| {
                        reader.error(&g.span(), format!("glob `{glob}`: {}", e.kind()))
                    })?);
                }
                glob_set = Some(
                    builder
                        .build()
                        .map_err(|e| reader.error(&g.span(), format!("glob: {e}")))?,
                );
            }
            if let Some(m) = keys.get("magic") {
                magic = match m.get_ref() {
                    DeValue::String(s) => s.as_bytes().to_vec(),
                    DeValue::Array(items) => items
                        .iter()
                        .map(|item| {
                            let byte = reader.integer(item, "magic")?;
                            u8::try_from(byte).map_err(|_| {
                                reader.error(&item.span(), "magic: a byte is 0 to 255")
                            })
                        })
                        .collect::<Result<_, _>>()?,
                    _ => {
                        return Err(
                            reader.error(&m.span(), "magic: expected a string or a list of bytes")
                        );
                    }
                };
                if magic.is_empty() || magic.len() as u64 > MAX_MATCH_READ {
                    return Err(reader.error(&m.span(), "magic: expected 1 to 65536 bytes"));
                }
            }
            if let Some(o) = keys.get("magic_offset") {
                let offset = reader.integer(o, "magic_offset")?;
                let room = MAX_MATCH_READ - magic.len() as u64;
                if offset < 0 || offset as u64 > room {
                    return Err(
                        reader.error(&o.span(), format!("magic_offset: expected 0 to {room}"))
                    );
                }
                magic_offset = offset as u64;
            }
            if let Some(w) = keys.get("where") {
                let table = reader.table(w, "where")?;
                for (key, value) in table {
                    let reference: &str = key.get_ref();
                    let Some(field) = reference.strip_prefix("header.") else {
                        return Err(reader.error(
                            &key.span(),
                            "where: expected header fields, such as \"header.version\" = 3",
                        ));
                    };
                    let Some(target) = header
                        .fields
                        .iter()
                        .find(|f| f.name.as_deref() == Some(field))
                    else {
                        return Err(reader.error(
                            &key.span(),
                            format!("where: no header field named `{field}`"),
                        ));
                    };
                    let wanted = match value.get_ref() {
                        DeValue::Integer(_)
                            if target.ty.is_integer() && target.meaning == Meaning::Plain =>
                        {
                            Expected::Int(i128::from(reader.integer(value, "where")?))
                        }
                        DeValue::String(s) if target.ty == Type::Str => {
                            Expected::Text(s.to_string())
                        }
                        _ => {
                            return Err(reader.error(
                                &value.span(),
                                format!(
                                    "where: `{field}` is a {}; expected a value of that type",
                                    type_name(target.ty)
                                ),
                            ));
                        }
                    };
                    expect.push((field.to_string(), wanted));
                }
            }
        }

        let Some(records_value) = top.get("records") else {
            return Err(reader.error(&whole, "missing [records], with the fields of one record"));
        };
        let table = reader.table(records_value, "[records]")?;
        let keys = reader.entries(
            table,
            "[records]",
            &["framing", "fields", "size", "size_adjust", "count"],
        )?;
        let framing = match keys.get("framing") {
            None => Framing::Fixed,
            Some(v) => match reader.string(v, "framing")?.as_str() {
                "fixed" => Framing::Fixed,
                other @ ("length_prefixed" | "blocks" | "variant" | "sync") => {
                    return Err(reader.error(
                        &v.span(),
                        format!("framing = \"{other}\" is not yet supported; expected fixed"),
                    ));
                }
                _ => return Err(reader.error(&v.span(), "framing: expected fixed")),
            },
        };
        let Some(fields_value) = keys.get("fields") else {
            return Err(reader.error(&records_value.span(), "[records]: missing `fields`"));
        };
        let fields = reader.fields(fields_value, Part::Records, layout, &header.fields)?;
        if !fields.iter().any(|f| f.name.is_some()) {
            return Err(reader.error(&fields_value.span(), "fields: a record needs a named field"));
        }
        let size = keys
            .get("size")
            .map(|s| {
                reader.amount(
                    s,
                    keys.get("size_adjust").copied(),
                    "size",
                    Part::Records,
                    &[],
                    &header.fields,
                )
            })
            .transpose()?;
        let count = keys
            .get("count")
            .map(|c| reader.amount(c, None, "count", Part::Records, &[], &header.fields))
            .transpose()?;
        if layout == Layout::Columns {
            if let Some(s) = keys.get("size") {
                return Err(reader.error(
                    &s.span(),
                    "size: each column file holds one field, so layout = \"columns\" takes no record size",
                ));
            }
            let DeValue::Array(items) = fields_value.get_ref() else {
                unreachable!("read as an array above")
            };
            for (field, item) in fields.iter().zip(items) {
                if field.ty == Type::Pad {
                    return Err(reader.error(
                        &item.span(),
                        "pad: layout = \"columns\" has no bytes between fields to skip",
                    ));
                }
                if field.flatten {
                    return Err(reader.error(
                        &item.span(),
                        "flatten: a column file holds one column; leave the values an Array",
                    ));
                }
            }
        }
        // Sizes written down are checked now; one that comes from the file is checked
        // when the file is read.
        if let (Some(Amount::Given(size)), Some(sum)) = (&size, given_width(&fields))
            && *size < sum
        {
            let s = keys.get("size").expect("size was read");
            return Err(reader.error(
                &s.span(),
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
        if let Some(sum) = given_width(&fields) {
            if sum == 0 {
                return Err(reader.error(&fields_value.span(), "fields: a record takes no bytes"));
            }
            if sum > MAX_SIZE {
                return Err(reader.error(
                    &fields_value.span(),
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
            layout,
            header,
            records: Records {
                framing,
                fields,
                size,
                count,
            },
        })
    }

    /// Read the spec in `path`.
    pub fn load(path: &Path) -> Result<Self, SpecError> {
        let text = std::fs::read_to_string(path).map_err(|e| SpecError {
            path: Some(path.to_path_buf()),
            line: 0,
            column: 0,
            message: format!("could not read the spec: {e}"),
        })?;
        Self::parse(&text, Some(path))
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

    /// Whether `head`, the first bytes of a file, carries the spec's magic.
    pub fn magic_matches(&self, head: &[u8]) -> bool {
        let start = self.magic_offset as usize;
        !self.magic.is_empty() && head.get(start..start + self.magic.len()) == Some(&self.magic)
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
            self.magic_offset + self.magic.len() as u64
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

/// A header, read: each named field's value, and how many bytes the header takes.
#[derive(Debug, Default)]
pub struct HeaderValues {
    pub values: Vec<(String, AnyValue<'static>)>,
    pub size: u64,
}

impl HeaderValues {
    fn get(&self, name: &str) -> Option<&AnyValue<'static>> {
        self.values.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    fn int(&self, name: &str) -> Option<i128> {
        match self.get(name)? {
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

    /// `amount`'s value: given, or read from a header field and bounded.
    fn resolve(&self, amount: &Amount, what: &str) -> Result<u64, String> {
        match amount {
            Amount::Given(n) => Ok(*n),
            Amount::Header { field, adjust } => {
                let value = self
                    .int(field)
                    .ok_or_else(|| format!("{what}: the header has no value for `{field}`"))?
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
struct Place {
    start: usize,
    stride: Option<usize>,
    width: usize,
    count: usize,
}

/// The decoder's view of `field`, named `name`, its cells at `place`.
fn layout_of(
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
        physical: field.ty.physical(),
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

/// Read the header from the front of `bytes`, sizing each field as it goes.
fn read_header(spec: &Spec, bytes: &[u8]) -> Result<HeaderValues, String> {
    let mut read = HeaderValues::default();
    let mut at = 0u64;
    for field in &spec.header.fields {
        let (width, count) = sized(field, &read)?;
        let end = at + width * count;
        if end > bytes.len() as u64 {
            return Err(format!(
                "the file is {} bytes, too short for its header (field `{}` ends at byte {end})",
                bytes.len(),
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
            let layout = layout_of(spec, field, name, place, &read)?;
            let column =
                crate::fixed_records::decode(bytes, &layout, 1).map_err(|e| e.to_string())?;
            let value = column.get(0).map_err(|e| e.to_string())?.into_static();
            read.values.push((name.clone(), value));
        }
        at = end;
    }
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

/// A file read through a spec: its columns, and what the read had to say.
pub struct Opened {
    pub records: Arc<FixedRecords>,
    /// Warnings for the dataset's notes, one sentence each.
    pub notes: Vec<String>,
    pub header: HeaderValues,
}

/// Up to this many trailing bytes are shown in the warning about them.
const TRAILING_SHOWN: usize = 32;

fn trailing_note(what: &str, bytes: &[u8]) -> String {
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
        if self.layout == Layout::Columns {
            return Err(format!(
                "{} is a directory of column files (layout = \"columns\"); open the directory",
                self.name
            ));
        }
        let data = bytes.as_slice();
        self.check_magic(data, named)?;
        let header = read_header(self, data)?;
        let mut notes = Vec::new();
        let len = data.len() as u64;
        if header.size > len {
            return Err(format!(
                "{named} is {len} bytes, shorter than its {}-byte header",
                header.size
            ));
        }
        // The fields' own width first, to check the record size against it.
        let (_, fields_width) = self.record_columns(&header, 0, Some(1))?;
        let record = match &self.records.size {
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
        let rows = match &self.records.count {
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
            self.record_columns(&header, header.size as usize, Some(record as usize))?;
        let records =
            FixedRecords::new(vec![bytes], columns, rows as usize).map_err(|e| e.to_string())?;
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Read `dir`, a directory of one file per record field.
    pub fn open_columns(&self, dir: &Path) -> Result<Opened, String> {
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
        let header = header.unwrap_or_default();
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
        Ok(Opened {
            records: Arc::new(records),
            notes,
            header,
        })
    }

    /// Open `path`: a file for the rows layout, a directory for the columns one.
    pub fn open(&self, path: &Path, named: &str) -> Result<Opened, String> {
        match self.layout {
            Layout::Columns => self.open_columns(path),
            Layout::Rows => {
                if path.is_dir() {
                    return Err(format!(
                        "{} reads one file, and {} is a directory",
                        self.name,
                        path.display()
                    ));
                }
                let bytes = Bytes::map(path).map_err(|e| format!("{}: {e}", path.display()))?;
                self.open_rows(Arc::new(bytes), named)
            }
        }
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
    /// Spec files that could not be read, each with why.
    pub errors: Vec<SpecError>,
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
    /// file gives itself. What cannot be read is kept as an error, not fatal.
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
                            && p.extension()
                                .is_some_and(|e| e.eq_ignore_ascii_case("toml"))
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

    /// The specs whose globs match `path`, a file or (for the columns layout) a
    /// directory, by name alone.
    pub fn by_glob(&self, path: &Path, is_dir: bool) -> Vec<Arc<Spec>> {
        self.specs
            .iter()
            .map(|f| &f.spec)
            .filter(|s| (s.layout == Layout::Columns) == is_dir && s.glob_matches(path))
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
        let globbed = self.by_glob(path, is_dir);
        let (candidates, by) = if !globbed.is_empty() {
            (globbed, Chosen::Glob)
        } else if is_dir {
            return None;
        } else {
            let magic: Vec<Arc<Spec>> = self
                .specs
                .iter()
                .map(|f| &f.spec)
                .filter(|s| s.layout == Layout::Rows && !s.magic.is_empty())
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
            if !summary.is_empty() {
                out.push_str(&format!("  ({summary})"));
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
    pub records: Arc<FixedRecords>,
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
    /// A spec already chosen, for a file decompressed before it is read.
    pub spec: Option<Arc<Spec>>,
    /// A built-in format from `--format`, which no spec overrides.
    pub builtin: bool,
    pub compression: Option<crate::CompressionFormat>,
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
            by: Chosen::Named,
            also: Vec::new(),
        })
    } else if let Some(file) = &asked.spec_file {
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
    let choice = match explicit {
        Some(choice) => choice,
        None => {
            if asked.builtin || registry.is_empty() {
                return Ok(Route::Elsewhere);
            }
            let is_dir = path.is_dir();
            // What the name already says is read as it says, compressed or not.
            if !is_dir && crate::discover::data_format(path).is_some()
                || crate::discover::is_parquet_key(&crate::discover::directory_and_name(path))
            {
                return Ok(Route::Elsewhere);
            }
            // A glob names the file as it is stored uncompressed: `day.l2.zst` is an `*.l2`.
            let inner = match compression {
                Some(_) => path.with_extension(""),
                None => path.to_path_buf(),
            };
            let matched = registry.matching(&inner, is_dir, |reach| {
                // A file with no extension may be Parquet, Arrow, Avro or ORC by its bytes,
                // which it stays.
                if compression.is_none() && crate::discover::sniff_format(path).is_some() {
                    return None;
                }
                head_of(path, compression, reach)
            });
            let Some(matched) = matched else {
                return Ok(Route::Elsewhere);
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
    if compression.is_some() && path.is_file() {
        return Ok(Route::Decompress(choice));
    }
    read(path, &named, choice).map(|r| Route::Read(Box::new(r)))
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
    config: &crate::config::AppConfig,
) -> (String, i32) {
    let path = search_path_for(config);
    let registry = Registry::load(&path);
    match action {
        None => (registry.listing(&path), 0),
        Some(crate::cli::FormatsAction::Check { spec, file }) => {
            match check(spec, file.as_deref(), &registry) {
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
fn check(named: &str, file: Option<&Path>, registry: &Registry) -> Result<String, String> {
    let as_file = Path::new(named);
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

/// `df` as plain text: a row of names, then a row per record, columns aligned.
fn text_table(df: &polars::prelude::DataFrame) -> String {
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

    fn collect(opened: &Opened) -> DataFrame {
        opened
            .records
            .clone()
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
    fn later_framings_and_bad_combinations_are_refused_by_name() {
        let record = |fields: &str| format!("name = \"a.b\"\n[records]\nfields = [{fields}]");
        for (text, said) in [
            ("name = \"a.b\"\n[records]\nframing = \"length_prefixed\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "not yet supported"),
            ("name = \"a.b\"\n[footer]\nsize = 4\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "`footer` is not yet supported"),
            (record("{ name = \"n\", type = \"u1\" }, { name = \"s\", type = \"str\", size = \"n\" }"), "length_prefixed"),
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
        let text = check(&spec.to_string_lossy(), Some(&data), &registry).unwrap();
        assert!(text.starts_with("acme.l2feed: ok"), "{text}");
        assert!(text.contains("warning: day.l2 has 1 byte after"), "{text}");
        assert!(
            text.contains("AAPL") && text.contains("1 records"),
            "{text}"
        );
        std::fs::write(&spec, "name = \"a.b\"\n[records]\nfields = 1").unwrap();
        let e = check(&spec.to_string_lossy(), None, &registry).unwrap_err();
        assert!(e.contains("l2.toml:3:10: fields: expected an array"), "{e}");
        assert!(check("acme.nothing", None, &registry).is_err());
    }
}

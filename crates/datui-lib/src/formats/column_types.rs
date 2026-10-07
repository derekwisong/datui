//! A column's declared type: one vocabulary for the type row, a delimited spec's
//! `[columns]`, the typing of text columns, and a column retyped in the table.
//!
//! [`ColumnType`] is a type name and, for the temporal ones, a strftime `format`, read
//! from and written as the inline table a spec entry uses: `{ type = "date", format =
//! "%d/%m/%Y" }`. [`ColumnType::expr`] turns a column into it, lazily and never failing:
//! a value that does not fit is null. [`ColumnType::unfit`] counts those values, for
//! the note that says so ([`unfit_notes`]).

use polars::prelude::*;

/// The type names, as the type row shows them and a spec or a picker takes them.
pub const TYPE_NAMES: [&str; 16] = [
    "str", "bool", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "date",
    "time", "datetime", "duration",
];

/// The type a name in [`TYPE_NAMES`] stands for.
pub fn dtype_named(name: &str) -> Option<DataType> {
    Some(match name {
        "str" => DataType::String,
        "bool" => DataType::Boolean,
        "i8" => DataType::Int8,
        "i16" => DataType::Int16,
        "i32" => DataType::Int32,
        "i64" => DataType::Int64,
        "u8" => DataType::UInt8,
        "u16" => DataType::UInt16,
        "u32" => DataType::UInt32,
        "u64" => DataType::UInt64,
        "f32" => DataType::Float32,
        "f64" => DataType::Float64,
        "date" => DataType::Date,
        "time" => DataType::Time,
        "datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
        "duration" => DataType::Duration(TimeUnit::Nanoseconds),
        _ => return None,
    })
}

/// The short name of a column's type, as the type row and the schema pane spell it.
///
/// Polars' own `Display` says `Datetime(Microseconds, None)`; the row under the header
/// has room for one word.
pub fn dtype_label(dtype: &DataType) -> String {
    match dtype {
        DataType::String => "str".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "i8".to_string(),
        DataType::Int16 => "i16".to_string(),
        DataType::Int32 => "i32".to_string(),
        DataType::Int64 => "i64".to_string(),
        DataType::UInt8 => "u8".to_string(),
        DataType::UInt16 => "u16".to_string(),
        DataType::UInt32 => "u32".to_string(),
        DataType::UInt64 => "u64".to_string(),
        DataType::Float32 => "f32".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Datetime(_, _) => "datetime".to_string(),
        DataType::Time => "time".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::Null => "null".to_string(),
        DataType::List(inner) => format!("list[{}]", dtype_label(inner)),
        DataType::Struct(_) => "struct".to_string(),
        other if other.is_categorical() => "cat".to_string(),
        other if other.is_enum() => "enum".to_string(),
        other if other.is_decimal() => "decimal".to_string(),
        other => other.to_string().to_ascii_lowercase(),
    }
}

/// A column's declared type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    pub dtype: DataType,
    /// The strftime format of the text, for `date`, `time` and `datetime`; inferred
    /// from the values when not given.
    pub format: Option<String>,
}

/// Written as the inline table a spec entry is, in a saved view's JSON too: `{ "type":
/// "date", "format": "%d/%m/%Y" }`.
impl serde::Serialize for ColumnType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", &self.name())?;
        if let Some(format) = &self.format {
            map.serialize_entry("format", format)?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for ColumnType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Entry {
            #[serde(rename = "type")]
            name: String,
            #[serde(default)]
            format: Option<String>,
        }
        let entry = Entry::deserialize(deserializer)?;
        Self::named(&entry.name, entry.format).map_err(serde::de::Error::custom)
    }
}

/// A change to a view's columns, by name: a type, or a column made from others, as a
/// spec's `[columns]` entry says it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnChange {
    pub name: String,
    #[serde(flatten)]
    pub change: Change,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Change {
    /// `{ from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }`.
    Made {
        from: Vec<String>,
        #[serde(rename = "as")]
        kind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<String>,
    },
    /// `{ type = "i64" }`.
    Typed(ColumnType),
}

impl ColumnChange {
    /// The spec entry it is: `zip = { type = "str" }`.
    pub fn to_toml(&self) -> String {
        let value = match &self.change {
            Change::Typed(ty) => ty.to_toml(),
            Change::Made { from, kind, format } => {
                let mut table = toml_edit::InlineTable::new();
                let mut list = toml_edit::Array::new();
                for f in from {
                    list.push(f.as_str());
                }
                table.insert("from", toml_edit::Value::Array(list));
                table.insert("as", kind.as_str().into());
                if let Some(format) = format {
                    table.insert("format", format.as_str().into());
                }
                table.fmt();
                table.to_string()
            }
        };
        let mut key = toml_edit::Key::new(self.name.as_str());
        key.fmt();
        format!("{} = {value}", key.display_repr())
    }

    /// The derived column it makes, for a change that makes one.
    pub fn derived(&self) -> Option<Derived> {
        let Change::Made { from, kind, format } = &self.change else {
            return None;
        };
        Some(Derived {
            name: self.name.clone(),
            from: from.clone(),
            kind: DerivedKind::named(kind)?,
            format: format.clone(),
        })
    }
}

/// The characters trimmed from a value before it is read as a type.
const PADDING: &str = " \t\r\n";

impl ColumnType {
    /// The type `name` names, with `format` for a temporal one. The error says what
    /// was wrong in words a spec's error or a prompt can carry.
    pub fn named(name: &str, format: Option<String>) -> Result<Self, String> {
        let dtype = dtype_named(name).ok_or_else(|| {
            format!(
                "unknown type \"{name}\"; expected one of {}",
                TYPE_NAMES.join(", ")
            )
        })?;
        let ty = Self { dtype, format };
        if ty.format.is_some() && !ty.is_temporal() {
            return Err(format!(
                "format is for date, time and datetime, not {}",
                ty.name()
            ));
        }
        Ok(ty)
    }

    pub fn name(&self) -> String {
        dtype_label(&self.dtype)
    }

    pub fn is_temporal(&self) -> bool {
        matches!(
            self.dtype,
            DataType::Date | DataType::Time | DataType::Datetime(_, _)
        )
    }

    fn is_integer(&self) -> bool {
        self.dtype.is_integer()
    }

    /// The inline table a spec entry and a saved view write it as.
    pub fn to_toml(&self) -> String {
        let mut table = toml_edit::InlineTable::new();
        table.insert("type", self.name().into());
        if let Some(format) = &self.format {
            table.insert("format", format.as_str().into());
        }
        table.fmt();
        table.to_string()
    }

    /// The type an inline table such as `{ type = "date", format = "%d/%m/%Y" }` says.
    #[cfg(test)]
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let value: toml_edit::Value = text.parse().map_err(|e| format!("{e}"))?;
        let table = value
            .as_inline_table()
            .ok_or_else(|| "expected an inline table such as { type = \"i64\" }".to_string())?;
        for (key, _) in table.iter() {
            if key != "type" && key != "format" {
                return Err(format!("unknown key `{key}`; expected type or format"));
            }
        }
        let name = table
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing `type`".to_string())?;
        let format = match table.get("format") {
            Some(v) => Some(
                v.as_str()
                    .ok_or_else(|| "format: expected a string".to_string())?
                    .to_string(),
            ),
            None => None,
        };
        Self::named(name, format)
    }

    fn strptime(&self) -> StrptimeOptions {
        StrptimeOptions {
            format: self.format.as_deref().map(PlSmallStr::from),
            strict: false,
            exact: true,
            cache: true,
        }
    }

    /// `text`, a text expression, trimmed and with a blank value null.
    fn trimmed(text: Expr) -> Expr {
        let trimmed = text
            .str()
            .strip_chars(lit(PlSmallStr::from_static(PADDING)));
        when(trimmed.clone().eq(lit(PlSmallStr::from_static(""))))
            .then(Null {}.lit().cast(DataType::String))
            .otherwise(trimmed)
    }

    /// The column `name`, now of type `from`, as this type. Text is trimmed first, and a
    /// blank value is null. `bool` takes `true`/`false` and `1`/`0`, in any case. A value
    /// that does not fit, or is out of range, is null: the read never fails on one.
    pub fn expr(&self, name: &str, from: &DataType) -> Expr {
        self.typed(name, from).alias(PlSmallStr::from(name))
    }

    fn typed(&self, name: &str, from: &DataType) -> Expr {
        let column = col(PlSmallStr::from(name));
        if *from != DataType::String {
            return column.cast(self.dtype.clone());
        }
        if self.dtype == DataType::String {
            return column;
        }
        let text = Self::trimmed(column);
        match &self.dtype {
            DataType::Boolean => {
                let lower = text.str().to_lowercase();
                when(lower.clone().eq(lit("true")).or(lower.clone().eq(lit("1"))))
                    .then(lit(true))
                    .when(lower.clone().eq(lit("false")).or(lower.eq(lit("0"))))
                    .then(lit(false))
                    .otherwise(Null {}.lit().cast(DataType::Boolean))
            }
            DataType::Date => text.str().to_date(self.strptime()),
            DataType::Time => text.str().to_time(self.strptime()),
            DataType::Datetime(unit, zone) => text.str().to_datetime(
                Some(*unit),
                zone.clone(),
                self.strptime(),
                lit(PlSmallStr::from_static("raise")),
            ),
            DataType::Duration(_) => text.map(
                |c: Column| Ok(durations(c.str()?).into_column()),
                |_: &Schema, field: &Field| {
                    Ok(Field::new(
                        field.name().clone(),
                        DataType::Duration(TimeUnit::Nanoseconds),
                    ))
                },
            ),
            dtype => text.cast(dtype.clone()),
        }
    }

    /// The counts of the values of the column `name`, of type `from`, that [`Self::expr`]
    /// makes null: those that do not parse, and those out of range for an integer type.
    /// Two expressions summed over the column, named `{name}\0not` and `{name}\0range`.
    pub fn unfit(&self, name: &str, from: &DataType) -> [Expr; 2] {
        let column = col(PlSmallStr::from(name));
        let typed = self.expr(name, from);
        let (present, integral) = if *from == DataType::String {
            let text = Self::trimmed(column);
            let integral = text
                .clone()
                .str()
                .contains(lit(r"^[+-]?[0-9]+$"), false)
                .fill_null(lit(false));
            (text.is_not_null(), integral)
        } else {
            (column.is_not_null(), lit(from.is_primitive_numeric()))
        };
        let unfit = present.and(typed.is_null());
        let range = self.is_integer();
        let not = if range {
            unfit.clone().and(integral.clone().not())
        } else {
            unfit.clone()
        };
        let out = if range {
            unfit.and(integral)
        } else {
            lit(false)
        };
        [
            not.cast(DataType::UInt64)
                .sum()
                .alias(format!("{name}\u{0}not")),
            out.cast(DataType::UInt64)
                .sum()
                .alias(format!("{name}\u{0}range")),
        ]
    }
}

/// The date formats `read.infer_types` tries and a retype offers, most common first.
pub const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%m-%d-%Y",
    "%m/%d/%Y",
];

/// The datetime formats, as [`DATE_FORMATS`].
pub const DATETIME_FORMATS: &[&str] = &[
    // ISO 8601 with an offset. `%#z` takes `Z`, `+05:00`, `-0500` and `+05`; a format
    // with an offset makes Polars read the column into UTC.
    "%Y-%m-%dT%H:%M:%S%.f%#z",
    "%Y-%m-%d %H:%M:%S%.f%#z",
    "%Y-%m-%dT%H:%M%#z",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d-%m-%YT%H:%M:%S%.f",
    "%d-%m-%YT%H:%M:%S",
    "%d-%m-%Y %H:%M:%S%.f",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%YT%H:%M:%S%.f",
    "%d/%m/%YT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y%m%dT%H%M%S%.f",
    "%Y%m%d %H%M%S",
];

/// The time formats, as [`DATE_FORMATS`].
pub const TIME_FORMATS: &[&str] = &[
    "%H:%M:%S%.9f",
    "%H:%M:%S%.6f",
    "%H:%M:%S%.3f",
    "%H:%M:%S",
    "%H:%M",
];

/// The formats of a temporal `dtype` that read `sample`, in the order they are tried.
pub fn formats_reading(dtype: &DataType, sample: &str) -> Vec<&'static str> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    let sample = sample.trim();
    match dtype {
        DataType::Date => DATE_FORMATS
            .iter()
            .filter(|f| NaiveDate::parse_from_str(sample, f).is_ok())
            .copied()
            .collect(),
        DataType::Datetime(_, _) => DATETIME_FORMATS
            .iter()
            .filter(|f| NaiveDateTime::parse_from_str(sample, f).is_ok())
            .copied()
            .collect(),
        DataType::Time => TIME_FORMATS
            .iter()
            .filter(|f| NaiveTime::parse_from_str(sample, f).is_ok())
            .copied()
            .collect(),
        _ => Vec::new(),
    }
}

/// `value` read as `ty`, as the table would show it: `2024-03-04` for `03/04/2024` as a
/// date with `%d/%m/%Y`. `None` when it does not read.
pub fn preview(ty: &ColumnType, value: &str) -> Option<String> {
    let df = df!("v" => [value]).ok()?;
    let out = df
        .lazy()
        .select([ty.expr("v", &DataType::String)])
        .collect()
        .ok()?;
    let v = out.column("v").ok()?.get(0).ok()?;
    (!v.is_null()).then(|| match v {
        AnyValue::String(s) => s.to_string(),
        v => v.to_string(),
    })
}

/// Text in Polars' duration format (`1d`, `2h30m`, `-1w2d`) as nanoseconds. A value that
/// does not parse is null.
pub fn durations(text: &StringChunked) -> DurationChunked {
    let values: Vec<Option<i64>> = text
        .iter()
        .map(|v| {
            v.and_then(|s| {
                polars::time::Duration::try_parse(s)
                    .ok()
                    .map(|d| d.duration_ns())
            })
        })
        .collect();
    Int64Chunked::from_iter_options(text.name().clone(), values.into_iter())
        .into_duration(TimeUnit::Nanoseconds)
}

/// Whether `value` starts with a zero another digit follows, as `02134`, `007` and
/// `-01` do and `0`, `0.5`, `-0.5` and `0e3` do not: a code or an ID, kept as text
/// rather than read as a number.
pub fn has_leading_zero(value: &str) -> bool {
    let digits = value.trim().trim_start_matches(['+', '-']).as_bytes();
    digits.len() > 1 && digits[0] == b'0' && digits[1].is_ascii_digit()
}

/// A column given a type, and the type it had before: what [`unfit_frame`] counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Typed {
    pub column: String,
    pub ty: ColumnType,
    pub from: DataType,
}

/// One pass over `source`, the frame before the columns of `typed` were typed, that
/// counts the values each one made null.
pub fn unfit_frame(source: LazyFrame, typed: &[Typed]) -> LazyFrame {
    let exprs: Vec<Expr> = typed
        .iter()
        .flat_map(|t| t.ty.unfit(&t.column, &t.from))
        .collect();
    source.select(exprs)
}

/// The values a column's type made null: those that are not of it, and those out of
/// its range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unfit {
    pub column: String,
    pub ty: String,
    pub not_parsed: u64,
    pub out_of_range: u64,
}

/// The counts in `counted`, the one row [`unfit_frame`] collects, for `typed`.
pub fn unfit_counts(counted: &DataFrame, typed: &[Typed]) -> Vec<Unfit> {
    let count = |name: String| -> u64 {
        counted
            .column(&name)
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<u64>())
            .unwrap_or(0)
    };
    typed
        .iter()
        .map(|t| Unfit {
            column: t.column.clone(),
            ty: t.ty.name(),
            not_parsed: count(format!("{}\u{0}not", t.column)),
            out_of_range: count(format!("{}\u{0}range", t.column)),
        })
        .filter(|u| u.not_parsed > 0 || u.out_of_range > 0)
        .collect()
}

/// A note a column, for each column whose type made values null:
/// `LogIdx: 3 values not i64, read as null`, `RPM: 2 values out of range for u8, read
/// as null`.
pub fn unfit_notes(unfit: &[Unfit], scope: &str) -> Vec<crate::notes::Note> {
    let values = |n: u64| if n == 1 { "value" } else { "values" };
    unfit
        .iter()
        .map(|u| {
            let mut said = Vec::new();
            if u.not_parsed > 0 {
                said.push(format!(
                    "{} {} not {}",
                    u.not_parsed,
                    values(u.not_parsed),
                    u.ty
                ));
            }
            if u.out_of_range > 0 {
                said.push(format!(
                    "{} {} out of range for {}",
                    u.out_of_range,
                    values(u.out_of_range),
                    u.ty
                ));
            }
            crate::notes::Note {
                summary: format!("{}: {}, read as null", u.column, said.join(", ")),
                scope: scope.to_string(),
                read_as_text: None,
                passed_over: None,
            }
        })
        .collect()
}

/// What a derived column is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedKind {
    /// From a date and a time, or one text column, with an optional UTC offset.
    Datetime,
    Date,
    Time,
}

impl DerivedKind {
    pub const ALL: [Self; 3] = [Self::Datetime, Self::Date, Self::Time];

    /// The kind `as` names.
    pub fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.name() == name)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Datetime => "datetime",
            Self::Date => "date",
            Self::Time => "time",
        }
    }
}

/// A column built from others: `time = { from = ["Date", "Time", "Offset"], as = "datetime" }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub name: String,
    pub from: Vec<String>,
    pub kind: DerivedKind,
    /// A strftime format for the text the `from` columns make, joined with a space.
    /// Inferred from the values when not given.
    pub format: Option<String>,
}

/// An offset such as `-05:00`, `+0530`, `-5` or `05:00:00`: its sign, hours and minutes.
const OFFSET: &str = r"^\s*([+-])?(\d{1,2})(?::?(\d{2}))?(?::\d{2})?\s*$";

impl Derived {
    /// The column, lazily, from its sources' text.
    pub fn expr(&self) -> Expr {
        let text = |name: &str| col(name).cast(DataType::String);
        let options = StrptimeOptions {
            format: self.format.as_deref().map(PlSmallStr::from),
            strict: false,
            exact: true,
            cache: true,
        };
        match self.kind {
            DerivedKind::Date => text(&self.from[0]).str().to_date(options),
            DerivedKind::Time => text(&self.from[0]).str().to_time(options),
            DerivedKind::Datetime => {
                let stamp = match self.from.as_slice() {
                    [one] => text(one),
                    // A null in either is a null stamp.
                    [date, time, ..] => text(date) + lit(" ") + text(time),
                    [] => unreachable!("a derived column has a source"),
                };
                let local = stamp.str().to_datetime(
                    Some(TimeUnit::Microseconds),
                    None,
                    options,
                    lit("raise"),
                );
                let Some(offset) = self.from.get(2) else {
                    return local;
                };
                // Local time less its offset from UTC is UTC.
                let part = |group| text(offset).str().extract(lit(OFFSET), group);
                let sign = when(part(1).eq(lit("-")))
                    .then(lit(-1i64))
                    .otherwise(lit(1i64));
                let minutes = sign
                    * (part(2).cast(DataType::Int64) * lit(60i64)
                        + part(3).cast(DataType::Int64).fill_null(lit(0i64)));
                let shift =
                    (minutes * lit(60_000_000i64)).cast(DataType::Duration(TimeUnit::Microseconds));
                (local - shift).dt().replace_time_zone(
                    Some(polars::prelude::TimeZone::UTC),
                    lit("raise"),
                    NonExistent::Raise,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(name: &str, format: Option<&str>, values: &[Option<&str>]) -> (Series, Vec<Unfit>) {
        let ty = ColumnType::named(name, format.map(String::from)).unwrap();
        let df = df!("v" => values).unwrap();
        let out = df
            .clone()
            .lazy()
            .select([ty.expr("v", &DataType::String).alias("v")])
            .collect()
            .unwrap();
        let typed = [Typed {
            column: "v".into(),
            ty,
            from: DataType::String,
        }];
        let counted = unfit_frame(df.lazy(), &typed).collect().unwrap();
        (
            out.column("v").unwrap().as_materialized_series().clone(),
            unfit_counts(&counted, &typed),
        )
    }

    #[test]
    fn a_preview_shows_what_a_type_makes_of_a_value() {
        let date = ColumnType::named("date", Some("%d/%m/%Y".into())).unwrap();
        assert_eq!(preview(&date, "03/04/2024").as_deref(), Some("2024-04-03"));
        assert_eq!(preview(&date, "2024-04-03"), None);
    }

    #[test]
    fn a_leading_zero_is_a_code() {
        for code in ["02134", "007", "-01", " 0012 "] {
            assert!(has_leading_zero(code), "{code}");
        }
        for number in ["0", "0.5", "-0.5", "0e3", "10", "", "-"] {
            assert!(!has_leading_zero(number), "{number}");
        }
    }

    #[test]
    fn a_change_is_the_spec_entry_in_json_and_toml() {
        let typed = ColumnChange {
            name: "zip".into(),
            change: Change::Typed(ColumnType::named("date", Some("%d/%m/%Y".into())).unwrap()),
        };
        let json = serde_json::to_string(&typed).unwrap();
        assert_eq!(json, r#"{"name":"zip","type":"date","format":"%d/%m/%Y"}"#);
        assert_eq!(serde_json::from_str::<ColumnChange>(&json).unwrap(), typed);
        assert_eq!(
            typed.to_toml(),
            r#"zip = { type = "date", format = "%d/%m/%Y" }"#
        );
        let made = ColumnChange {
            name: "when".into(),
            change: Change::Made {
                from: vec!["Lcl Date".into(), "Lcl Time".into()],
                kind: "datetime".into(),
                format: None,
            },
        };
        let json = serde_json::to_string(&made).unwrap();
        assert_eq!(serde_json::from_str::<ColumnChange>(&json).unwrap(), made);
        assert_eq!(
            made.to_toml(),
            r#"when = { from = ["Lcl Date", "Lcl Time"], as = "datetime" }"#
        );
        assert!(serde_json::from_str::<ColumnChange>(r#"{"name":"x","type":"int"}"#).is_err());
    }

    #[test]
    fn every_name_is_a_type_and_back() {
        for name in TYPE_NAMES {
            let ty = ColumnType::named(name, None).unwrap();
            assert_eq!(ty.name(), name);
        }
        let err = ColumnType::named("int", None).unwrap_err();
        assert!(err.contains("expected one of str, bool"), "{err}");
        assert!(ColumnType::named("i64", Some("%Y".into())).is_err());
    }

    #[test]
    fn it_reads_and_writes_the_spec_inline_table() {
        let ty = ColumnType::named("date", Some("%d/%m/%Y".into())).unwrap();
        let text = ty.to_toml();
        assert_eq!(text, r#"{ type = "date", format = "%d/%m/%Y" }"#);
        assert_eq!(ColumnType::from_toml(&text).unwrap(), ty);
        assert_eq!(
            ColumnType::from_toml(r#"{ type = "i64" }"#)
                .unwrap()
                .to_toml(),
            r#"{ type = "i64" }"#
        );
        assert!(ColumnType::from_toml(r#"{ type = "i64", unit = "x" }"#).is_err());
    }

    #[test]
    fn integers_are_trimmed_and_unfit_values_are_counted() {
        let (s, unfit) = typed(
            "i64",
            None,
            &[Some(" 12 "), Some("x"), Some("1.5"), None, Some("  ")],
        );
        assert_eq!(s.dtype(), &DataType::Int64);
        let v: Vec<Option<i64>> = s.i64().unwrap().iter().collect();
        assert_eq!(v, [Some(12), None, None, None, None]);
        assert_eq!(unfit[0].not_parsed, 2);
        assert_eq!(unfit[0].out_of_range, 0);
    }

    #[test]
    fn a_narrow_integer_counts_what_is_out_of_its_range() {
        let (s, unfit) = typed(
            "u8",
            None,
            &[Some("255"), Some("300"), Some("-1"), Some("a")],
        );
        let v: Vec<Option<u8>> = s.u8().unwrap().iter().collect();
        assert_eq!(v, [Some(255), None, None, None]);
        assert_eq!((unfit[0].not_parsed, unfit[0].out_of_range), (1, 2));
        let notes = unfit_notes(&unfit, "");
        assert_eq!(
            notes[0].summary,
            "v: 1 value not u8, 2 values out of range for u8, read as null"
        );
        let (s, unfit) = typed("i8", None, &[Some("127"), Some("128")]);
        assert_eq!(s.null_count(), 1);
        assert_eq!(unfit[0].out_of_range, 1);
    }

    #[test]
    fn floats_and_f32_round_trip() {
        let (s, unfit) = typed("f32", None, &[Some("1.5"), Some(" -2.25"), Some("1e3")]);
        let v: Vec<Option<f32>> = s.f32().unwrap().iter().collect();
        assert_eq!(v, [Some(1.5), Some(-2.25), Some(1000.0)]);
        assert!(unfit.is_empty());
        let (s, _) = typed("f64", None, &[Some("25.1")]);
        assert_eq!(s.f64().unwrap().get(0), Some(25.1));
    }

    #[test]
    fn bool_takes_words_and_digits() {
        let (s, unfit) = typed(
            "bool",
            None,
            &[
                Some("1"),
                Some(" 0"),
                Some("TRUE"),
                Some("false"),
                Some("yes"),
            ],
        );
        let v: Vec<Option<bool>> = s.bool().unwrap().iter().collect();
        assert_eq!(v, [Some(true), Some(false), Some(true), Some(false), None]);
        assert_eq!(unfit[0].not_parsed, 1);
    }

    #[test]
    fn temporal_types_take_a_format() {
        let (s, unfit) = typed(
            "date",
            Some("%d/%m/%Y"),
            &[Some("03/04/2024"), Some("2024-04-03")],
        );
        assert_eq!(s.dtype(), &DataType::Date);
        assert_eq!(s.get(0).unwrap().to_string(), "2024-04-03");
        assert_eq!(unfit[0].not_parsed, 1);
        let (s, _) = typed("time", Some("%H:%M:%S"), &[Some("13:35:42")]);
        assert_eq!(s.dtype(), &DataType::Time);
        let (s, _) = typed("datetime", None, &[Some("2024-03-01 10:00:00")]);
        assert_eq!(s.get(0).unwrap().to_string(), "2024-03-01 10:00:00");
        let (s, _) = typed("duration", None, &[Some("2h30m"), Some("x")]);
        assert_eq!(s.null_count(), 1);
        let (s, _) = typed("str", None, &[Some(" a ")]);
        assert_eq!(
            s.str().unwrap().get(0),
            Some(" a "),
            "text is left as it is"
        );
    }
}

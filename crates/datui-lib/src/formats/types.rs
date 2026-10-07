//! Type names, field names and the other small helpers the parser and the spec share.

use super::*;

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
            description: None,
            unit: None,
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
pub(crate) fn output_names(field: &Field) -> Vec<String> {
    let mut names = own_names(field);
    names.extend(field.bits.iter().map(|b| b.name.clone()));
    names
}

/// The columns of a field's own value, without its bits: `name`, or `name_0`... when
/// flattened.
pub(crate) fn own_names(field: &Field) -> Vec<String> {
    let Some(name) = &field.name else {
        return Vec::new();
    };
    match (&field.count, field.flatten) {
        (Some(Amount::Given(n)), true) => (0..*n).map(|i| format!("{name}_{i}")).collect(),
        _ => vec![name.clone()],
    }
}

/// The smallest and largest value an integer (or bool) type holds.
pub(crate) fn integer_range(ty: Type) -> Option<(i128, i128)> {
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
pub(crate) fn is_file_name(name: &str) -> bool {
    let mut parts = Path::new(name).components();
    matches!(
        (parts.next(), parts.next()),
        (Some(std::path::Component::Normal(_)), None)
    ) && !name.contains(['/', '\\'])
}

/// A type name and the byte order its suffix asks for.
pub(crate) fn parse_type(text: &str) -> Option<(Type, Option<Endian>)> {
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

pub(crate) fn type_name(ty: Type) -> String {
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
pub(crate) fn parse_epoch(text: &str) -> Option<i64> {
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

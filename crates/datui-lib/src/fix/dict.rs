//! FIX data dictionaries: what a tag is called, what type its values are, and what its
//! enumerated codes mean.
//!
//! The built-in dictionary is FIX 4.2, 4.4 and 5.0 SP2 merged, generated from
//! QuickFIX's data dictionaries by `scripts/code/fix_dictionary.py`. Custom ones are
//! found on the format spec search path (`~/.config/datui/formats/`,
//! `$DATUI_FORMATS_PATH`, `formats_path`) or named by `--dict`, in two forms: a
//! QuickFIX XML data dictionary, read as it is, or a short TOML file of `kind = "fix"`.
//! A custom dictionary may apply only to the messages of one counterparty (`match` on
//! SenderCompID, TargetCompID or BeginString).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::formats::SpecError;

/// The largest dictionary file read.
pub const MAX_FILE: u64 = 64 << 20;
/// The deepest XML nesting followed.
const MAX_DEPTH: usize = 64;
/// The longest tag name or enum name kept.
pub const MAX_NAME: usize = 256;
/// The highest tag number: FIX tags are positive and fit in a signed 32-bit integer.
pub const MAX_TAG: u32 = i32::MAX as u32;

/// The built-in dictionary's table; see the generator for its columns.
const BUILT_IN: &str = include_str!("dictionary.tsv");

/// What a tag's values are, as far as datui types them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixType {
    Int,
    Float,
    /// UTCTIMESTAMP: `YYYYMMDD-HH:MM:SS[.sss]`.
    Timestamp,
    /// UTCDATEONLY, LOCALMKTDATE: `YYYYMMDD`.
    Date,
    /// `Y` or `N`.
    Bool,
    /// A LENGTH tag: the size of the DATA tag after it.
    Length,
    /// A DATA tag, sized by its LENGTH tag; may hold the delimiter.
    Data,
    Text,
}

impl FixType {
    /// A QuickFIX type name, or a TOML dictionary's.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_uppercase().as_str() {
            "INT" | "SEQNUM" | "NUMINGROUP" | "TAGNUM" | "DAYOFMONTH" => Self::Int,
            "LENGTH" => Self::Length,
            "FLOAT" | "PRICE" | "QTY" | "AMT" | "PRICEOFFSET" | "PERCENTAGE" | "QUANTITY" => {
                Self::Float
            }
            "UTCTIMESTAMP" | "TIMESTAMP" | "DATETIME" => Self::Timestamp,
            "UTCDATEONLY" | "UTCDATE" | "LOCALMKTDATE" | "DATE" => Self::Date,
            "BOOLEAN" | "BOOL" => Self::Bool,
            "DATA" | "XMLDATA" => Self::Data,
            "STRING"
            | "CHAR"
            | "CURRENCY"
            | "EXCHANGE"
            | "COUNTRY"
            | "MONTHYEAR"
            | "MULTIPLECHARVALUE"
            | "MULTIPLESTRINGVALUE"
            | "MULTIPLEVALUESTRING"
            | "XID"
            | "XIDREF"
            | "LANGUAGE"
            | "LOCALMKTTIME"
            | "UTCTIMEONLY"
            | "TZTIMEONLY"
            | "TZTIMESTAMP"
            | "TEXT"
            | "STR" => Self::Text,
            _ => return None,
        })
    }

    /// Whether `value` reads as this type.
    pub fn reads(self, value: &str) -> bool {
        match self {
            Self::Int | Self::Length => value.parse::<i64>().is_ok(),
            Self::Float => value.parse::<f64>().is_ok_and(f64::is_finite),
            Self::Timestamp => parse_timestamp(value).is_some(),
            Self::Date => parse_date(value).is_some(),
            Self::Bool => matches!(value, "Y" | "N"),
            Self::Data | Self::Text => true,
        }
    }
}

/// A UTCTIMESTAMP as nanoseconds since the epoch: `20260102-03:04:05`, with up to nine
/// digits of fraction.
pub fn parse_timestamp(text: &str) -> Option<i64> {
    let (date, time) = text.split_once('-')?;
    let days = parse_date(date)?;
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, f)) => (hms, Some(f)),
        None => (time, None),
    };
    let b = hms.as_bytes();
    if b.len() != 8 || b[2] != b':' || b[5] != b':' {
        return None;
    }
    let two = |i: usize| -> Option<i64> {
        let (a, b) = (b[i], b[i + 1]);
        (a.is_ascii_digit() && b.is_ascii_digit()).then(|| i64::from((a - b'0') * 10 + (b - b'0')))
    };
    let (h, m, s) = (two(0)?, two(3)?, two(6)?);
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    let nanos = match fraction {
        None => 0,
        Some(f) if !f.is_empty() && f.len() <= 9 && f.bytes().all(|c| c.is_ascii_digit()) => {
            f.parse::<i64>().ok()? * 10i64.pow(9 - f.len() as u32)
        }
        Some(_) => return None,
    };
    // Out of a nanosecond Datetime's range (1677 to 2262) is not a time datui types.
    days.checked_mul(86_400_000_000_000)?
        .checked_add((h * 3600 + m * 60 + s) * 1_000_000_000 + nanos)
}

/// A `YYYYMMDD` date as days since the epoch.
pub fn parse_date(text: &str) -> Option<i64> {
    if text.len() != 8 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let y: i32 = text[..4].parse().ok()?;
    let m: u32 = text[4..6].parse().ok()?;
    let d: u32 = text[6..].parse().ok()?;
    let date = chrono::NaiveDate::from_ymd_opt(y, m, d)?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((date - epoch).num_days())
}

/// What one dictionary says of one tag.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TagDef {
    pub name: Option<String>,
    pub ty: Option<FixType>,
    /// Code to name.
    pub enums: HashMap<String, String>,
    /// For a LENGTH tag, the DATA tag it sizes.
    pub data: Option<u32>,
}

/// Which messages a dictionary applies to; every field given must match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Match {
    /// SenderCompID (49).
    pub sender: Option<String>,
    /// TargetCompID (56).
    pub target: Option<String>,
    /// BeginString (8).
    pub begin_string: Option<String>,
}

impl Match {
    pub fn is_empty(&self) -> bool {
        self.sender.is_none() && self.target.is_none() && self.begin_string.is_none()
    }

    /// Whether a message of this sender, target and begin string is one of its.
    pub fn matches(&self, sender: Option<&str>, target: Option<&str>, begin: Option<&str>) -> bool {
        let one = |want: &Option<String>, have: Option<&str>| {
            want.as_deref().is_none_or(|want| Some(want) == have)
        };
        one(&self.sender, sender) && one(&self.target, target) && one(&self.begin_string, begin)
    }

    /// `sender BROKERX, begin string FIX.4.4`.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if let Some(s) = &self.sender {
            parts.push(format!("sender {s}"));
        }
        if let Some(t) = &self.target {
            parts.push(format!("target {t}"));
        }
        if let Some(b) = &self.begin_string {
            parts.push(format!("begin string {b}"));
        }
        parts.join(", ")
    }
}

/// A FIX dictionary.
#[derive(Debug, Clone, PartialEq)]
pub struct Dictionary {
    /// `fix` for the built-in one; a TOML dictionary's `name`; an XML file's stem.
    pub name: String,
    pub path: Option<PathBuf>,
    pub matcher: Match,
    pub tags: HashMap<u32, TagDef>,
}

/// The built-in dictionary, read once.
pub fn built_in() -> Arc<Dictionary> {
    static DICT: OnceLock<Arc<Dictionary>> = OnceLock::new();
    DICT.get_or_init(|| Arc::new(parse_table(BUILT_IN))).clone()
}

fn parse_table(text: &str) -> Dictionary {
    let mut tags = HashMap::new();
    for line in text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let mut cols = line.split('\t');
        let (Some(tag), Some(name), Some(ty), data, enums) = (
            cols.next().and_then(|t| t.parse::<u32>().ok()),
            cols.next(),
            cols.next(),
            cols.next().and_then(|d| d.parse::<u32>().ok()),
            cols.next().unwrap_or_default(),
        ) else {
            continue;
        };
        let enums = enums
            .split(';')
            .filter_map(|pair| pair.split_once('='))
            .map(|(code, name)| (code.to_string(), name.to_string()))
            .collect();
        tags.insert(
            tag,
            TagDef {
                name: Some(name.to_string()),
                ty: FixType::parse(ty),
                enums,
                data,
            },
        );
    }
    Dictionary {
        name: "fix".to_string(),
        path: None,
        matcher: Match::default(),
        tags,
    }
}

fn error(path: Option<&Path>, line: usize, column: usize, message: impl Into<String>) -> SpecError {
    SpecError {
        path: path.map(Path::to_path_buf),
        line,
        column,
        message: message.into(),
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
        .unwrap_or_default()
        .chars()
        .count()
        + 1;
    (line, column)
}

/// Whether a TOML file is a FIX dictionary (`kind = "fix"`) rather than a format spec.
pub fn is_fix_toml(text: &str) -> bool {
    text.parse::<toml::Table>()
        .is_ok_and(|t| t.get("kind").and_then(|k| k.as_str()) == Some("fix"))
}

fn short(name: &str) -> String {
    let mut cut = name.len().min(MAX_NAME);
    while !name.is_char_boundary(cut) {
        cut -= 1;
    }
    name[..cut].to_string()
}

impl Dictionary {
    /// Read the dictionary in `path`: QuickFIX XML (`.xml`) or TOML. `Ok(None)` for an
    /// XML file that is not a FIX dictionary or a TOML file that is not `kind = "fix"`.
    pub fn load(path: &Path) -> Result<Option<Self>, SpecError> {
        let size = std::fs::metadata(path)
            .map_err(|e| error(Some(path), 0, 0, format!("could not read it: {e}")))?
            .len();
        if size > MAX_FILE {
            return Err(error(
                Some(path),
                0,
                0,
                format!(
                    "{} MiB is past the {} MiB a dictionary may be",
                    size >> 20,
                    MAX_FILE >> 20
                ),
            ));
        }
        let text = std::fs::read_to_string(path)
            .map_err(|e| error(Some(path), 0, 0, format!("could not read it: {e}")))?;
        let xml = path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("xml"));
        if xml {
            let stem = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            Self::parse_xml(&text, &stem, Some(path))
        } else if is_fix_toml(&text) {
            Self::parse_toml(&text, Some(path)).map(Some)
        } else {
            Ok(None)
        }
    }

    /// A `kind = "fix"` TOML dictionary.
    pub fn parse_toml(text: &str, path: Option<&Path>) -> Result<Self, SpecError> {
        let table: toml::Table = text.parse().map_err(|e: toml::de::Error| {
            let (line, column) = e
                .span()
                .map_or((0, 0), |span| line_column(text, span.start));
            error(path, line, column, e.message().to_string())
        })?;
        let at = |key: &str| {
            // Where a top-level key starts, for errors about its value.
            text.lines()
                .position(|l| l.trim_start().starts_with(key))
                .map_or(0, |i| i + 1)
        };
        let fail = |key: &str, message: String| error(path, at(key), 1, message);
        for key in table.keys() {
            if !matches!(
                key.as_str(),
                "name" | "kind" | "description" | "match" | "tags"
            ) {
                return Err(fail(
                    key,
                    format!(
                        "unknown key `{key}`; a FIX dictionary has name, kind, description, match and tags"
                    ),
                ));
            }
        }
        let name = match table.get("name") {
            Some(toml::Value::String(name)) if crate::formats::is_spec_name(name) => name.clone(),
            Some(_) => {
                return Err(fail(
                    "name",
                    "name: expected a namespaced name such as acme.fix.broker-x".into(),
                ));
            }
            None => {
                return Err(error(
                    path,
                    0,
                    0,
                    "missing `name`, such as name = \"acme.fix.broker-x\"",
                ));
            }
        };
        let mut matcher = Match::default();
        if let Some(m) = table.get("match") {
            let Some(m) = m.as_table() else {
                return Err(fail(
                    "match",
                    "match: expected a table such as { sender = \"BROKERX\" }".into(),
                ));
            };
            for (key, value) in m {
                let Some(value) = value.as_str() else {
                    return Err(fail("match", format!("match.{key}: expected a string")));
                };
                let slot = match key.as_str() {
                    "sender" => &mut matcher.sender,
                    "target" => &mut matcher.target,
                    "begin_string" => &mut matcher.begin_string,
                    _ => {
                        return Err(fail(
                            "match",
                            format!("match.{key}: expected sender, target or begin_string"),
                        ));
                    }
                };
                *slot = Some(value.to_string());
            }
        }
        let mut tags = HashMap::new();
        if let Some(t) = table.get("tags") {
            let Some(t) = t.as_table() else {
                return Err(fail(
                    "tags",
                    "tags: expected a table such as { 9001 = \"AlgoName\" }".into(),
                ));
            };
            for (key, value) in t {
                let tag = key
                    .parse::<u32>()
                    .ok()
                    .filter(|t| (1..=MAX_TAG).contains(t))
                    .ok_or_else(|| fail("tags", format!("tags.{key}: expected a tag number")))?;
                let def = match value {
                    toml::Value::String(name) => TagDef {
                        name: Some(short(name)),
                        ..TagDef::default()
                    },
                    toml::Value::Table(def) => tag_def(def, key).map_err(|m| fail("tags", m))?,
                    _ => {
                        return Err(fail(
                            "tags",
                            format!(
                                "tags.{key}: expected a name or a table with name, type and enum"
                            ),
                        ));
                    }
                };
                tags.insert(tag, def);
            }
        }
        Ok(Self {
            name,
            path: path.map(Path::to_path_buf),
            matcher,
            tags,
        })
    }

    /// A QuickFIX XML data dictionary: its `<fields>`, each with its `<value>`s. It
    /// applies to the messages of its version's BeginString. `Ok(None)` when the root
    /// element is not `<fix>`.
    pub fn parse_xml(
        text: &str,
        name: &str,
        path: Option<&Path>,
    ) -> Result<Option<Self>, SpecError> {
        let mut stack: Vec<String> = Vec::new();
        let mut tags = HashMap::new();
        let mut field: Option<(u32, TagDef)> = None;
        let mut root = None;
        let mut pos = 0;
        let bytes = text.as_bytes();
        while let Some(open) = text[pos..].find('<').map(|i| pos + i) {
            let rest = &text[open..];
            let fail = |message: &str| {
                let (line, column) = line_column(text, open);
                error(path, line, column, message.to_string())
            };
            if rest.starts_with("<!--") {
                pos = match rest.find("-->") {
                    Some(end) => open + end + 3,
                    None => return Err(fail("a comment is not closed")),
                };
                continue;
            }
            if rest.starts_with("<?") || rest.starts_with("<!") {
                pos = match rest.find('>') {
                    Some(end) => open + end + 1,
                    None => return Err(fail("markup is not closed")),
                };
                continue;
            }
            let Some(end) = markup_end(&bytes[open..]) else {
                return Err(fail("a tag is not closed"));
            };
            let inner = &text[open + 1..open + end - 1];
            pos = open + end;
            if let Some(closing) = inner.strip_prefix('/') {
                let closing = closing.trim();
                if stack.last().map(String::as_str) != Some(closing) {
                    return Err(fail(&format!("</{closing}> closes no <{closing}>")));
                }
                stack.pop();
                if closing == "field"
                    && let Some((tag, def)) = field.take()
                {
                    tags.insert(tag, def);
                }
                continue;
            }
            let empty = inner.ends_with('/');
            let inner = inner.strip_suffix('/').unwrap_or(inner);
            let (element, attrs) = match inner.find(|c: char| c.is_ascii_whitespace()) {
                Some(i) => (&inner[..i], &inner[i..]),
                None => (inner, ""),
            };
            let attrs = attributes(attrs).map_err(|m| fail(&m))?;
            let get = |key: &str| {
                attrs
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.as_str())
            };
            if root.is_none() {
                if element != "fix" {
                    return Ok(None);
                }
                let begin = match (get("type"), get("major"), get("minor")) {
                    (_, Some("5"), _) | (Some("FIXT"), _, _) => Some("FIXT.1.1".to_string()),
                    (_, Some(major), Some(minor)) => Some(format!("FIX.{major}.{minor}")),
                    _ => None,
                };
                root = Some(begin);
            }
            let in_fields = stack.last().map(String::as_str) == Some("fields");
            if element == "field" && in_fields {
                let tag = get("number")
                    .and_then(|n| n.parse::<u32>().ok())
                    .filter(|t| (1..=MAX_TAG).contains(t))
                    .ok_or_else(|| fail("<field> needs a number"))?;
                let def = TagDef {
                    name: get("name").map(short),
                    ty: get("type").and_then(FixType::parse),
                    enums: HashMap::new(),
                    data: None,
                };
                if empty {
                    tags.insert(tag, def);
                } else {
                    field = Some((tag, def));
                }
            } else if element == "value"
                && stack.last().map(String::as_str) == Some("field")
                && let Some((_, def)) = field.as_mut()
                && let (Some(code), Some(description)) = (get("enum"), get("description"))
            {
                def.enums.insert(short(code), camel(description));
            }
            if !empty {
                if stack.len() >= MAX_DEPTH {
                    return Err(fail("elements nest too deep"));
                }
                stack.push(element.to_string());
            }
        }
        let Some(begin) = root else {
            return Ok(None);
        };
        // A LENGTH field sizes the DATA field named as it is without Len or Length.
        let by_name: HashMap<String, u32> = tags
            .iter()
            .filter(|(_, d)| d.ty == Some(FixType::Data))
            .filter_map(|(t, d)| Some((d.name.clone()?, *t)))
            .collect();
        for def in tags.values_mut() {
            if def.ty == Some(FixType::Length)
                && let Some(name) = &def.name
            {
                def.data = ["Length", "Len"]
                    .iter()
                    .find_map(|s| name.strip_suffix(s))
                    .and_then(|stem| by_name.get(stem).copied());
            }
        }
        Ok(Some(Self {
            name: name.to_string(),
            path: path.map(Path::to_path_buf),
            matcher: Match {
                begin_string: begin,
                ..Match::default()
            },
            tags,
        }))
    }
}

/// A TOML dictionary's table for one tag: `{ name = "Urgency", type = "int", enum =
/// { 1 = "Low" } }`.
fn tag_def(def: &toml::Table, key: &str) -> Result<TagDef, String> {
    let mut out = TagDef::default();
    for (k, v) in def {
        match k.as_str() {
            "name" => {
                out.name =
                    Some(short(v.as_str().ok_or_else(|| {
                        format!("tags.{key}.name: expected a string")
                    })?));
            }
            "type" => {
                let name = v
                    .as_str()
                    .ok_or_else(|| format!("tags.{key}.type: expected a string"))?;
                out.ty = Some(FixType::parse(name).ok_or_else(|| {
                    format!(
                        "tags.{key}.type: expected int, float, price, qty, string, char, timestamp, date, bool, length or data"
                    )
                })?);
            }
            "enum" => {
                let table = v.as_table().ok_or_else(|| {
                    format!("tags.{key}.enum: expected a table such as {{ 1 = \"Low\" }}")
                })?;
                for (code, name) in table {
                    let name = name
                        .as_str()
                        .ok_or_else(|| format!("tags.{key}.enum.{code}: expected a string"))?;
                    out.enums.insert(short(code), short(name));
                }
            }
            "data" => {
                let tag = v
                    .as_integer()
                    .and_then(|t| u32::try_from(t).ok())
                    .filter(|t| (1..=MAX_TAG).contains(t))
                    .ok_or_else(|| {
                        format!("tags.{key}.data: expected the tag number this length sizes")
                    })?;
                out.data = Some(tag);
                out.ty.get_or_insert(FixType::Length);
            }
            other => {
                return Err(format!(
                    "tags.{key}.{other}: unknown key; a tag has name, type, enum and data"
                ));
            }
        }
    }
    Ok(out)
}

/// The length of the tag at the front of `bytes`, `>` included, minding quotes.
fn markup_end(bytes: &[u8]) -> Option<usize> {
    let mut quote = None;
    for (i, &b) in bytes.iter().enumerate().skip(1) {
        match (quote, b) {
            (None, b'"' | b'\'') => quote = Some(b),
            (Some(q), b) if b == q => quote = None,
            (None, b'>') => return Some(i + 1),
            _ => {}
        }
    }
    None
}

/// `name='v' other="w"` as pairs, entities decoded.
fn attributes(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut rest = text.trim_start();
    while !rest.is_empty() {
        let eq = rest.find('=').ok_or("an attribute has no value")?;
        let key = rest[..eq].trim().to_string();
        let after = rest[eq + 1..].trim_start();
        let quote = after.chars().next().ok_or("an attribute has no value")?;
        if quote != '"' && quote != '\'' {
            return Err("an attribute's value is not quoted".into());
        }
        let close = after[1..]
            .find(quote)
            .ok_or("an attribute's quote is not closed")?;
        out.push((key, unescape(&after[1..1 + close])));
        rest = after[close + 2..].trim_start();
        if out.len() > 64 {
            return Err("too many attributes".into());
        }
    }
    Ok(out)
}

fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// `SELL_SHORT` as SellShort, as the built-in dictionary writes QuickFIX's names.
pub fn camel(words: &str) -> String {
    if words.chars().any(|c| c.is_lowercase()) {
        return short(
            &words
                .split('_')
                .map(|p| {
                    let mut c = p.chars();
                    c.next()
                        .map(|f| f.to_uppercase().chain(c).collect::<String>())
                        .unwrap_or_default()
                })
                .collect::<String>(),
        );
    }
    short(
        &words
            .split('_')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let lower = p.to_lowercase();
                let mut c = lower.chars();
                c.next()
                    .map(|f| f.to_uppercase().chain(c).collect::<String>())
                    .unwrap_or_default()
            })
            .collect::<String>(),
    )
}

/// The dictionaries a FIX log is read with, in order: the built-in one, then the custom
/// ones on the search path, then `--dict`. Later ones win.
#[derive(Debug, Clone)]
pub struct Layers {
    pub dicts: Vec<Arc<Dictionary>>,
    /// For each LENGTH tag in any dictionary, the DATA tag it sizes.
    data: HashMap<u32, u32>,
}

impl Default for Layers {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// What the dictionaries that apply to a message say of one tag.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resolved {
    pub name: Option<String>,
    /// The dictionary that named it.
    pub named_by: Option<usize>,
    pub ty: Option<FixType>,
    pub enums: HashMap<String, String>,
}

impl Layers {
    /// The built-in dictionary under `custom`, in order.
    pub fn new(custom: Vec<Arc<Dictionary>>) -> Self {
        let mut dicts = vec![built_in()];
        dicts.extend(custom);
        let mut data = HashMap::new();
        for dict in &dicts {
            for (tag, def) in &dict.tags {
                if let Some(d) = def.data {
                    data.insert(*tag, d);
                }
            }
        }
        Self { dicts, data }
    }

    /// The DATA tag a LENGTH tag sizes, in any dictionary.
    pub fn data_tag(&self, length: u32) -> Option<u32> {
        self.data.get(&length).copied()
    }

    /// The dictionaries, by index, that apply to a message of this sender, target and
    /// begin string.
    pub fn applying(
        &self,
        sender: Option<&str>,
        target: Option<&str>,
        begin: Option<&str>,
    ) -> Vec<usize> {
        (0..self.dicts.len())
            .filter(|&i| self.dicts[i].matcher.matches(sender, target, begin))
            .collect()
    }

    /// What the dictionaries `applying` say of `tag`, later ones first.
    pub fn resolve(&self, applying: &[usize], tag: u32) -> Resolved {
        let mut out = Resolved::default();
        for &i in applying {
            let Some(def) = self.dicts[i].tags.get(&tag) else {
                continue;
            };
            if let Some(name) = &def.name {
                out.name = Some(name.clone());
                out.named_by = Some(i);
            }
            if def.ty.is_some() {
                out.ty = def.ty;
            }
            for (code, name) in &def.enums {
                out.enums.insert(code.clone(), name.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_built_in_dictionary_names_tags_and_codes() {
        let dict = built_in();
        let msg_type = &dict.tags[&35];
        assert_eq!(msg_type.name.as_deref(), Some("MsgType"));
        assert_eq!(msg_type.enums["D"], "NewOrderSingle");
        assert_eq!(dict.tags[&54].enums["1"], "Buy");
        assert!(
            dict.tags[&8].enums.is_empty(),
            "FIX.4.4 is not renamed Fix44"
        );
        assert_eq!(dict.tags[&44].ty, Some(FixType::Float));
        assert_eq!(dict.tags[&52].ty, Some(FixType::Timestamp));
        assert_eq!(dict.tags[&95].data, Some(96));
        assert_eq!(dict.tags[&212].data, Some(213));
        assert_eq!(dict.tags[&93].data, Some(89));
    }

    #[test]
    fn timestamps_and_dates() {
        assert_eq!(parse_timestamp("19700101-00:00:01"), Some(1_000_000_000));
        assert_eq!(parse_timestamp("19700101-00:00:00.5"), Some(500_000_000));
        assert_eq!(
            parse_timestamp("19700102-00:00:00.000000001"),
            Some(86_400_000_000_001)
        );
        assert_eq!(parse_timestamp("19700101-24:00:00"), None);
        assert_eq!(parse_timestamp("19700101-00:00:00."), None);
        assert_eq!(parse_timestamp("19701301-00:00:00"), None);
        assert_eq!(parse_date("19700201"), Some(31));
        assert_eq!(parse_timestamp("99991231-23:59:59"), None, "past 2262");
        assert_eq!(parse_timestamp("00010101-00:00:00"), None, "before 1677");
    }

    const TOML: &str = r#"
name = "acme.fix.broker-x"
kind = "fix"
match = { sender = "BROKERX", begin_string = "FIX.4.4" }
tags = { 9001 = "AlgoName", 9002 = { name = "Urgency", type = "int", enum = { 1 = "Low", 2 = "High" } }, 54 = { enum = { Z = "Zap" } } }
"#;

    #[test]
    fn a_toml_dictionary() {
        assert!(is_fix_toml(TOML));
        let dict = Dictionary::parse_toml(TOML, None).unwrap();
        assert_eq!(dict.name, "acme.fix.broker-x");
        assert_eq!(dict.matcher.sender.as_deref(), Some("BROKERX"));
        assert_eq!(dict.tags[&9001].name.as_deref(), Some("AlgoName"));
        assert_eq!(dict.tags[&9002].ty, Some(FixType::Int));
        assert_eq!(dict.tags[&9002].enums["2"], "High");

        let layers = Layers::new(vec![Arc::new(dict)]);
        let broker = layers.applying(Some("BROKERX"), None, Some("FIX.4.4"));
        let other = layers.applying(Some("OTHER"), None, Some("FIX.4.4"));
        assert_eq!(broker, [0, 1]);
        assert_eq!(other, [0]);
        let side = layers.resolve(&broker, 54);
        assert_eq!(side.name.as_deref(), Some("Side"));
        assert_eq!(side.enums["Z"], "Zap");
        assert_eq!(side.enums["1"], "Buy");
        assert_eq!(layers.resolve(&other, 9001).name, None);
    }

    #[test]
    fn toml_errors_say_where() {
        for (text, said) in [
            (
                "name = \"a.b\"\nkind = \"fix\"\ncolour = 1",
                "unknown key `colour`",
            ),
            ("name = \"plain\"\nkind = \"fix\"", "namespaced"),
            (
                "name = \"a.b\"\nkind = \"fix\"\ntags = { x = \"A\" }",
                "tags.x: expected a tag number",
            ),
            (
                "name = \"a.b\"\nkind = \"fix\"\ntags = { 1 = { type = \"blob\" } }",
                "tags.1.type",
            ),
            (
                "name = \"a.b\"\nkind = \"fix\"\nmatch = { venue = \"X\" }",
                "match.venue",
            ),
            ("name = \"a.b\"\nkind = \"fix\"\n[[", ""),
        ] {
            let e = Dictionary::parse_toml(text, Some(Path::new("d.toml"))).unwrap_err();
            assert!(e.to_string().contains(said), "{e}");
            assert!(e.to_string().starts_with("d.toml:"), "{e}");
        }
    }

    const XML: &str = r#"<?xml version="1.0"?>
<!-- custom fields -->
<fix type='FIX' major='4' minor='4' servicepack='0'>
 <header><field name='BeginString' required='Y'/></header>
 <messages><message name='Heartbeat' msgtype='0' msgcat='admin'><field name='TestReqID' required='N'/></message></messages>
 <fields>
  <field number='5001' name='Strategy' type='STRING'>
   <value enum='V' description='VWAP_PLUS' />
   <value enum='T' description="TWAP &amp; MORE" />
  </field>
  <field number='5002' name='SpecialLen' type='LENGTH'/>
  <field number='5003' name='Special' type='DATA'/>
 </fields>
</fix>"#;

    #[test]
    fn a_quickfix_xml_dictionary() {
        let dict = Dictionary::parse_xml(XML, "custom44", None)
            .unwrap()
            .unwrap();
        assert_eq!(dict.matcher.begin_string.as_deref(), Some("FIX.4.4"));
        assert_eq!(dict.tags.len(), 3);
        assert_eq!(dict.tags[&5001].enums["V"], "VwapPlus");
        assert_eq!(dict.tags[&5001].enums["T"], "Twap & more");
        assert_eq!(dict.tags[&5002].data, Some(5003));
        assert!(
            Dictionary::parse_xml("<gpx></gpx>", "x", None)
                .unwrap()
                .is_none()
        );
        assert!(
            Dictionary::parse_xml("<fix><fields><field number='x'/></fields></fix>", "x", None)
                .is_err()
        );
        assert!(Dictionary::parse_xml("<fix><fields></field></fix>", "x", None).is_err());
    }
}

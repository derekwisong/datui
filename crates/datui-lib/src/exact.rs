//! A stored value as text, exactly, apart from how the table previews it.
//!
//! The table's preview rounds floats, groups digits and cuts long text to fit a
//! cell; a copy or the inspector gives back what is stored. A float is the
//! shortest decimal that reads back to the same bits, never Polars' compact
//! display; a datetime carries every digit of its unit and its zone's offset.
//! Exact means the stored value: a CSV's `1.50` was stored as `1.5`.

use base64::Engine as _;
use polars::prelude::*;
use std::borrow::Cow;
use std::fmt::Write as _;

/// The shortest decimal that parses back to `v`: plain notation over the
/// magnitudes people read as plain numbers, exponent notation outside them.
/// NaN, the infinities and negative zero are spelled out rather than lost.
pub fn f64_text(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    let a = v.abs();
    // Rust's Display and LowerExp are both shortest round-trip; Display alone
    // would write 1e300 as 301 digits.
    if a == 0.0 || (1e-5..1e16).contains(&a) {
        whole_reads_as_float(format!("{v}"))
    } else {
        format!("{v:e}")
    }
}

/// `1.0`, not `1`: a whole float keeps its point, as Polars writes it, so it
/// does not read as an integer. Parses back the same.
fn whole_reads_as_float(mut text: String) -> String {
    if !text.contains('.') {
        text.push_str(".0");
    }
    text
}

/// [`f64_text`] for an `f32`, shortest at the `f32`'s own precision: widened
/// to `f64` first, `0.1f32` would read `0.10000000149011612`.
pub fn f32_text(v: f32) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    let a = v.abs();
    if a == 0.0 || (1e-5..1e16).contains(&a) {
        whole_reads_as_float(format!("{v}"))
    } else {
        format!("{v:e}")
    }
}

/// A date, datetime or time outside the calendar's range, as its stored number:
/// Polars panics formatting one (a sentinel like `i64::MIN` microseconds is real
/// data). A day of margin either side leaves room for a zone's offset.
pub fn out_of_range(value: &AnyValue) -> Option<String> {
    use chrono::{DateTime, NaiveDate, TimeDelta};
    let fits = |dt: Option<DateTime<chrono::Utc>>| {
        dt.is_some_and(|dt| {
            dt.checked_add_signed(TimeDelta::days(1)).is_some()
                && dt.checked_sub_signed(TimeDelta::days(1)).is_some()
        })
    };
    match value {
        AnyValue::Date(days) => {
            // Days from 0001-01-01 to the epoch, as Polars counts them.
            let fits = days
                .checked_add(719_163)
                .and_then(NaiveDate::from_num_days_from_ce_opt)
                .is_some();
            (!fits).then(|| format!("{days} days since 1970-01-01"))
        }
        AnyValue::Datetime(v, unit, _) | AnyValue::DatetimeOwned(v, unit, _) => {
            let (dt, unit) = match unit {
                TimeUnit::Milliseconds => (DateTime::from_timestamp_millis(*v), "ms"),
                TimeUnit::Microseconds => (DateTime::from_timestamp_micros(*v), "us"),
                TimeUnit::Nanoseconds => (Some(DateTime::from_timestamp_nanos(*v)), "ns"),
            };
            (!fits(dt)).then(|| format!("{v} {unit} since 1970-01-01 UTC"))
        }
        AnyValue::Time(ns) => {
            (!(0..NANOS_PER_DAY).contains(ns)).then(|| format!("{ns} ns since midnight"))
        }
        _ => None,
    }
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// [`AnyValue::str_value`], which panics on a date past the calendar, with such a
/// value written as its stored number, and a list or struct holding one written
/// as [`nested_compact`] does.
pub fn str_value<'a>(value: &AnyValue<'a>) -> Cow<'a, str> {
    match past_calendar_text(value) {
        Some(text) => Cow::Owned(text),
        None => value.str_value(),
    }
}

/// How the table previews a list cell: its first ten items, and how many there
/// are when that is not all of them (`[a, b...] (12 items)`).
pub fn list_preview(items: &Series) -> String {
    const SHOWN: usize = 10;
    let mut text = String::from("[");
    for (i, item) in items.iter().take(SHOWN).enumerate() {
        if i > 0 {
            text.push_str(", ");
        }
        text.push_str(&str_value(&item));
    }
    if items.len() > SHOWN {
        let _ = write!(text, "...] ({} items)", items.len());
    } else {
        text.push(']');
    }
    text
}

/// The text [`str_value`] gives a value Polars panics formatting: a date past
/// the calendar, or a list or struct holding one. `None` for any other value,
/// which Polars formats as usual.
pub fn past_calendar_text(value: &AnyValue) -> Option<String> {
    if let Some(text) = out_of_range(value) {
        return Some(text);
    }
    nested_out_of_range(value).then(|| nested_compact(value, CELL_PREVIEW_BYTES).text)
}

/// Whether a list, array or struct holds a value [`out_of_range`] names. Only one
/// whose type holds a date, datetime or time is looked into.
fn nested_out_of_range(value: &AnyValue) -> bool {
    let holds = |fields: &[Field]| fields.iter().any(|f| holds_calendar(f.dtype()));
    match value {
        AnyValue::List(s) | AnyValue::Array(s, _) => series_out_of_range(s),
        AnyValue::Struct(_, _, fields) if !holds(fields) => false,
        AnyValue::StructOwned(payload) if !holds(&payload.1) => false,
        AnyValue::Struct(..) | AnyValue::StructOwned(_) => value
            ._iter_struct_av()
            .any(|field| out_of_range(&field).is_some() || nested_out_of_range(&field)),
        _ => false,
    }
}

/// Whether any value of `s` is one [`out_of_range`] names: a flat column by its
/// least and greatest stored number, a nested one item by item.
fn series_out_of_range(s: &Series) -> bool {
    if !holds_calendar(s.dtype()) {
        return false;
    }
    match s.dtype() {
        DataType::Date | DataType::Datetime(..) | DataType::Time => {
            let Ok(stored) = s.to_physical_repr().cast(&DataType::Int64) else {
                return false;
            };
            let Ok(stored) = stored.i64() else {
                return false;
            };
            [stored.min(), stored.max()]
                .into_iter()
                .flatten()
                .any(|v| stored_out_of_range(s.dtype(), v).is_some())
        }
        _ => s.iter().any(|item| nested_out_of_range(&item)),
    }
}

/// [`out_of_range`] for a value of `dtype` stored as the number `stored`.
pub fn stored_out_of_range(dtype: &DataType, stored: i64) -> Option<String> {
    let value = match dtype {
        DataType::Date => AnyValue::Date(i32::try_from(stored).ok()?),
        DataType::Datetime(unit, _) => AnyValue::Datetime(stored, *unit, None),
        DataType::Time => AnyValue::Time(stored),
        _ => return None,
    };
    out_of_range(&value)
}

/// A date, datetime or time column with each value [`out_of_range`] names as
/// null, for the Polars operations that overflow or panic on one. `None` when
/// it holds none, the common case, which costs a min and a max.
pub fn calendar_without_out_of_range(series: &Series) -> PolarsResult<Option<Series>> {
    let dtype = series.dtype();
    if !matches!(
        dtype,
        DataType::Date | DataType::Datetime(..) | DataType::Time
    ) {
        return Ok(None);
    }
    let stored = series.to_physical_repr().cast(&DataType::Int64)?;
    let stored = stored.i64()?;
    let past = |v: i64| stored_out_of_range(dtype, v).is_some();
    if ![stored.min(), stored.max()].into_iter().flatten().any(past) {
        return Ok(None);
    }
    let kept = stored.apply(|v| v.filter(|v| !past(*v)));
    Ok(Some(
        kept.into_series()
            .cast(dtype)?
            .with_name(series.name().clone()),
    ))
}

/// Whether `dtype` is, or holds, a date, datetime or time.
pub fn holds_calendar(dtype: &DataType) -> bool {
    match dtype {
        DataType::Date | DataType::Datetime(..) | DataType::Time => true,
        DataType::List(inner) | DataType::Array(inner, _) => holds_calendar(inner),
        DataType::Struct(fields) => fields.iter().any(|f| holds_calendar(f.dtype())),
        _ => false,
    }
}

/// A datetime with every digit its unit stores, and the zone's offset when it
/// has a zone. Polars' own display drops trailing zeros and names the zone by
/// abbreviation, which several zones share.
fn datetime_text(v: i64, unit: TimeUnit, zone: Option<&TimeZone>) -> String {
    let digits = match unit {
        TimeUnit::Milliseconds => "%.3f",
        TimeUnit::Microseconds => "%.6f",
        TimeUnit::Nanoseconds => "%.9f",
    };
    let format = match zone {
        Some(_) => format!("%Y-%m-%d %H:%M:%S{digits} %:z"),
        None => format!("%Y-%m-%d %H:%M:%S{digits}"),
    };
    let ca = Int64Chunked::from_slice(PlSmallStr::EMPTY, &[v]).into_datetime(unit, zone.cloned());
    ca.to_string(&format)
        .ok()
        .and_then(|s| s.get(0).map(str::to_string))
        .unwrap_or_else(|| AnyValue::Datetime(v, unit, zone).str_value().into_owned())
}

/// Bytes as base64, the spelling every copy and export of a binary uses.
pub fn base64_text(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// A value as exact text. A null is empty, as in an export; a list or struct
/// is compact JSON-like text over exact scalars; bytes are base64.
pub fn value_text(value: &AnyValue) -> String {
    if let Some(text) = out_of_range(value) {
        return text;
    }
    match value {
        AnyValue::Null => String::new(),
        AnyValue::Float64(v) => f64_text(*v),
        AnyValue::Float32(v) => f32_text(*v),
        AnyValue::Float16(v) => f32_text(f32::from(*v)),
        AnyValue::Datetime(v, unit, zone) => datetime_text(*v, *unit, *zone),
        AnyValue::DatetimeOwned(v, unit, zone) => datetime_text(*v, *unit, zone.as_deref()),
        // ISO 8601 seconds, as a CSV export and every other copy write it.
        AnyValue::Duration(v, unit) => {
            let mut out = String::new();
            crate::export::nested_json::duration_iso(*v, *unit, &mut out);
            out
        }
        AnyValue::Binary(bytes) => base64_text(bytes),
        AnyValue::BinaryOwned(bytes) => base64_text(bytes),
        AnyValue::List(_)
        | AnyValue::Array(..)
        | AnyValue::Struct(..)
        | AnyValue::StructOwned(_) => {
            let mut out = String::new();
            write_nested(value, &mut out, None, 0, usize::MAX);
            out
        }
        // Integers, booleans, strings, dates, times, decimals and
        // categories: Polars' text for these is already the whole value.
        v => v.str_value().into_owned(),
    }
}

/// Text cut to a byte budget, and whether anything was cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bounded {
    pub text: String,
    pub cut: bool,
}

/// A list, array or struct laid out one item per line, indented, stopping once
/// `budget` bytes are written. Scalars inside are exact; text is quoted and
/// escaped so an item's edges are visible.
pub fn nested_pretty(value: &AnyValue, budget: usize) -> Bounded {
    let mut text = String::new();
    let cut = !write_nested(value, &mut text, Some(0), 0, budget);
    Bounded { text, cut }
}

/// A list, array or struct on one line, stopping once `budget` bytes are
/// written: for a preview, which shows only the start.
pub fn nested_compact(value: &AnyValue, budget: usize) -> Bounded {
    let mut text = String::new();
    let cut = !write_nested(value, &mut text, None, 0, budget);
    Bounded { text, cut }
}

/// Whether `value` is a list, array or struct.
pub fn is_nested_value(value: &AnyValue) -> bool {
    matches!(
        value,
        AnyValue::List(_) | AnyValue::Array(..) | AnyValue::Struct(..) | AnyValue::StructOwned(_)
    )
}

/// How many items a list or array holds, or fields a struct has.
pub fn nested_len(value: &AnyValue) -> Option<usize> {
    match value {
        AnyValue::List(s) | AnyValue::Array(s, _) => Some(s.len()),
        AnyValue::Struct(_, _, fields) => Some(fields.len()),
        AnyValue::StructOwned(payload) => Some(payload.1.len()),
        _ => None,
    }
}

/// Write `value` into `out`. `indent` is the current depth when laid out one
/// item per line, `None` for compact. Returns false once the budget ran out.
fn write_nested(
    value: &AnyValue,
    out: &mut String,
    indent: Option<usize>,
    depth: usize,
    budget: usize,
) -> bool {
    if out.len() >= budget {
        return false;
    }
    // Items are taken one at a time: a million-item list stops at the budget
    // rather than being turned into a million values first.
    match value {
        AnyValue::List(s) | AnyValue::Array(s, _) => write_items(
            s.iter().map(|v| (None, v)),
            s.len(),
            ('[', ']'),
            out,
            indent,
            depth,
            budget,
        ),
        AnyValue::Struct(_, _, fields) => write_items(
            fields
                .iter()
                .map(|f| Some(f.name().as_str()))
                .zip(value._iter_struct_av()),
            fields.len(),
            ('{', '}'),
            out,
            indent,
            depth,
            budget,
        ),
        AnyValue::StructOwned(payload) => write_items(
            payload
                .1
                .iter()
                .map(|f| Some(f.name().as_str()))
                .zip(payload.0.iter().cloned()),
            payload.1.len(),
            ('{', '}'),
            out,
            indent,
            depth,
            budget,
        ),
        // Text and bytes are cut at the budget too: one huge string in a list is
        // as costly as a huge cell.
        AnyValue::String(s) => write_quoted(s, out, budget),
        AnyValue::StringOwned(s) => write_quoted(s, out, budget),
        AnyValue::Binary(b) => write_bytes(b, out, budget),
        AnyValue::BinaryOwned(b) => write_bytes(b, out, budget),
        v => {
            out.push_str(&json_scalar(v));
            true
        }
    }
}

/// The items of a list, array or struct between `open` and `close`, each named
/// when a struct's. False once the budget ran out.
fn write_items<'n, 'v>(
    items: impl Iterator<Item = (Option<&'n str>, AnyValue<'v>)>,
    len: usize,
    (open, close): (char, char),
    out: &mut String,
    indent: Option<usize>,
    depth: usize,
    budget: usize,
) -> bool {
    let pad = |out: &mut String, depth: usize| {
        if indent.is_some() {
            out.push('\n');
            out.extend(std::iter::repeat_n("  ", depth));
        }
    };
    out.push(open);
    if len == 0 {
        out.push(close);
        return true;
    }
    for (i, (name, item)) in items.enumerate() {
        if i > 0 {
            out.push(',');
            if indent.is_none() {
                out.push(' ');
            }
        }
        pad(out, depth + 1);
        if let Some(name) = name {
            out.push_str(&json_string(name));
            out.push_str(": ");
        }
        if !write_nested(&item, out, indent, depth + 1, budget) {
            return false;
        }
        if out.len() >= budget && i + 1 < len {
            return false;
        }
    }
    pad(out, depth);
    out.push(close);
    true
}

/// Text quoted and escaped, as much of it as fits the budget. False when cut,
/// and then with no closing quote, so it does not read as the whole value.
fn write_quoted(s: &str, out: &mut String, budget: usize) -> bool {
    out.push('"');
    let head = prefix(s, budget.saturating_sub(out.len()));
    escape_into(head, out);
    if head.len() < s.len() {
        return false;
    }
    out.push('"');
    true
}

/// Bytes as quoted base64, as much as fits the budget. False when cut.
fn write_bytes(bytes: &[u8], out: &mut String, budget: usize) -> bool {
    // Base64 writes four characters per three bytes; whole groups keep the
    // part that is shown decodable.
    let room = budget.saturating_sub(out.len() + 1) / 4 * 3;
    let head = &bytes[..bytes.len().min(room)];
    out.push('"');
    out.push_str(&base64_text(head));
    if head.len() < bytes.len() {
        return false;
    }
    out.push('"');
    true
}

/// A scalar as it reads inside a list or struct: text quoted and escaped,
/// numbers exact, a null `null`.
fn json_scalar(value: &AnyValue) -> String {
    match value {
        AnyValue::Null => "null".to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::String(s) => json_string(s),
        AnyValue::StringOwned(s) => json_string(s),
        v if v.is_primitive_numeric() || matches!(v, AnyValue::Decimal(..)) => value_text(v),
        v => json_string(&value_text(v)),
    }
}

/// `s` in double quotes with JSON's escapes, and the invisible characters
/// [`escaped`] spells out.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    escape_into(s, &mut out);
    out.push('"');
    out
}

/// Whether `c` draws nothing, or nothing a reader can tell from a space, so the
/// escaped view spells it out: controls, Unicode's default-ignorable characters
/// (zero-width and direction marks, the soft hyphen, variation selectors, tags,
/// the byte-order mark) and the spaces other than U+0020.
fn invisible(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{a0}'
                | '\u{ad}'
                | '\u{34f}'
                | '\u{61c}'
                | '\u{115f}'..='\u{1160}'
                | '\u{1680}'
                | '\u{17b4}'..='\u{17b5}'
                | '\u{180b}'..='\u{180f}'
                | '\u{2000}'..='\u{200f}'
                | '\u{2028}'..='\u{202f}'
                | '\u{205f}'..='\u{206f}'
                | '\u{3000}'
                | '\u{3164}'
                | '\u{fe00}'..='\u{fe0f}'
                | '\u{feff}'
                | '\u{ffa0}'
                | '\u{fff0}'..='\u{fffb}'
                | '\u{1bca0}'..='\u{1bca3}'
                | '\u{1d173}'..='\u{1d17a}'
                | '\u{e0000}'..='\u{e0fff}'
        )
}

/// Whether a one-line cell draws `c` as a mark rather than as itself: a control
/// character, which a cell cannot draw, or a direction control, which a terminal
/// that lays out bidirectional text would apply to the rest of the row.
pub fn marked(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{61c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
        )
}

fn escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
        escape_char(c, out);
    }
}

/// One character of [`escaped`]'s literal, without the quotes around it: the
/// inspector escapes a long value a piece at a time.
pub fn escape_char(c: char, out: &mut String) {
    match c {
        '\\' => out.push_str("\\\\"),
        '"' => out.push_str("\\\""),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '\0' => out.push_str("\\0"),
        c if invisible(c) => {
            let _ = write!(out, "\\u{{{:x}}}", c as u32);
        }
        c => out.push(c),
    }
}

/// `s` as a quoted literal: backslash, quote, line breaks, tabs and every
/// invisible character written as an escape, so a literal `\n` in the data
/// (`"\\n"`) never reads as a line break, and leading or trailing spaces and
/// an empty string show by the quotes around them.
pub fn escaped(s: &str) -> String {
    json_string(s)
}

/// Bytes as a quoted literal: printable ASCII as itself, the rest as `\xNN`.
pub fn escaped_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 3);
    out.push_str("b\"");
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            b => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
    }
    out.push('"');
    out
}

/// Whether one-line text holds a character [`marked`] in a cell. Bytes first:
/// C1 controls start with 0xc2, U+061C with 0xd8, the other direction controls
/// with 0xe2.
fn has_marked(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
        || (s.bytes().any(|b| matches!(b, 0xc2 | 0xd8 | 0xe2)) && s.chars().any(marked))
}

/// Bytes of a value a table cell previews: more than any terminal row draws.
pub const CELL_PREVIEW_BYTES: usize = 4096;

/// [`preview`] of the start of `s`, ending in the ellipsis when cut: a cell
/// draws only its start, and measuring a huge value whole every frame costs
/// what the value costs.
pub fn cell_preview(s: &str, g: &crate::glyphs::Glyphs) -> String {
    let head = prefix(s, CELL_PREVIEW_BYTES);
    let mut text = preview(head, g).into_owned();
    if head.len() < s.len() {
        text.push_str(g.ellipsis);
    }
    text
}

/// One-line preview of `s`: a line break, a tab or another [`marked`] character
/// becomes a mark from the glyph set, so `line1\nline2` does not read as
/// `line1line2`. A `\r\n` pair is one break.
pub fn preview<'a>(s: &'a str, g: &crate::glyphs::Glyphs) -> Cow<'a, str> {
    if !has_marked(s) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' if chars.peek() == Some(&'\n') => {
                chars.next();
                out.push_str(g.newline_mark);
            }
            '\n' => out.push_str(g.newline_mark),
            '\t' => out.push_str(g.tab_mark),
            c if marked(c) => out.push_str(g.control_mark),
            c => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// The facts about text that its look on screen hides: how long it is, how
/// many lines it runs to, and whitespace at either end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TextFacts {
    pub chars: usize,
    pub lines: usize,
    pub leading_spaces: usize,
    pub trailing_spaces: usize,
}

pub fn text_facts(s: &str) -> TextFacts {
    let lines = if s.is_empty() {
        0
    } else {
        s.split('\n').count()
    };
    TextFacts {
        chars: s.chars().count(),
        lines,
        leading_spaces: s.chars().take_while(|c| c.is_whitespace()).count(),
        trailing_spaces: if s.chars().all(char::is_whitespace) {
            0
        } else {
            s.chars().rev().take_while(|c| c.is_whitespace()).count()
        },
    }
}

/// The longest prefix of `s` no longer than `budget` bytes, cut at a
/// character boundary.
pub fn prefix(s: &str, budget: usize) -> &str {
    if s.len() <= budget {
        return s;
    }
    let mut end = budget;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// One cell of a one-row column as copy text: exact scalars, nested values as
/// the JSON an export writes, bytes as base64, a null as empty.
pub fn copy_text(column: &Column) -> PolarsResult<String> {
    let value = column.get(0)?;
    if is_nested_value(&value) {
        let json = crate::export::nested_json::column_as_json(column)?;
        return Ok(match json.get(0)? {
            AnyValue::Null => String::new(),
            v => v.str_value().into_owned(),
        });
    }
    Ok(value_text(&value))
}

/// At least how many bytes [`copy_text`] writes for `value`, counted only until
/// the count passes `stop`: a copy can be refused at a cap before it is
/// formatted, and a million-item list is not walked to learn that it is over.
/// Text is its length and bytes their base64; a list or struct is counted from
/// its punctuation and leaves, each at its shortest JSON.
pub fn copy_len_floor(value: &AnyValue, stop: usize) -> usize {
    match value {
        AnyValue::String(s) => s.len(),
        AnyValue::StringOwned(s) => s.len(),
        AnyValue::Binary(b) => crate::clipboard::base64_len(b.len()),
        AnyValue::BinaryOwned(b) => crate::clipboard::base64_len(b.len()),
        v if is_nested_value(v) => json_len_floor(v, 0, stop),
        // Any other scalar is a few bytes: its text is its measure.
        v => value_text(v).len(),
    }
}

/// `so_far` plus a floor on the JSON written for `value`, stopping once past `stop`.
fn json_len_floor(value: &AnyValue, so_far: usize, stop: usize) -> usize {
    if so_far > stop {
        return so_far;
    }
    // Brackets, and a comma between items.
    let punctuation = |len: usize| 2 + len.saturating_sub(1);
    match value {
        AnyValue::List(s) | AnyValue::Array(s, _) => {
            let mut n = so_far + punctuation(s.len());
            for item in s.iter() {
                if n > stop {
                    break;
                }
                n = json_len_floor(&item, n, stop);
            }
            n
        }
        AnyValue::Struct(_, _, fields) => {
            let mut n = so_far + punctuation(fields.len());
            for (field, item) in fields.iter().zip(value._iter_struct_av()) {
                if n > stop {
                    break;
                }
                // `"name":` before the value.
                n = json_len_floor(&item, n + field.name().len() + 3, stop);
            }
            n
        }
        AnyValue::StructOwned(payload) => {
            let mut n = so_far + punctuation(payload.1.len());
            for (field, item) in payload.1.iter().zip(&payload.0) {
                if n > stop {
                    break;
                }
                n = json_len_floor(item, n + field.name().len() + 3, stop);
            }
            n
        }
        AnyValue::String(s) => so_far + s.len() + 2,
        AnyValue::StringOwned(s) => so_far + s.len() + 2,
        AnyValue::Binary(b) => so_far + crate::clipboard::base64_len(b.len()) + 2,
        AnyValue::BinaryOwned(b) => so_far + crate::clipboard::base64_len(b.len()) + 2,
        // A number, a flag or a null is at least a character.
        _ => so_far + 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_read_back_to_the_same_bits() {
        for v in [
            1000000.125,
            0.1 + 0.2,
            -0.0,
            1e300,
            -2.5e-12,
            123456789012345.67,
            f64::MIN_POSITIVE,
            f64::MAX,
            1.0,
        ] {
            let text = f64_text(v);
            let back: f64 = text.parse().unwrap();
            assert_eq!(back.to_bits(), v.to_bits(), "{v} wrote {text}");
        }
        assert_eq!(f64_text(1000000.125), "1000000.125");
        assert_eq!(f64_text(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(f64_text(1e300), "1e300");
        assert_eq!(f64_text(1.0), "1.0");
    }

    #[test]
    fn negative_zero_nan_and_the_infinities_are_spelled_out() {
        assert_eq!(f64_text(-0.0), "-0.0");
        assert_eq!(f64_text(0.0), "0.0");
        assert_eq!(f64_text(f64::NAN), "NaN");
        assert_eq!(f64_text(f64::INFINITY), "inf");
        assert_eq!(f64_text(f64::NEG_INFINITY), "-inf");
        assert_eq!(f32_text(-0.0), "-0.0");
        assert_eq!(f32_text(f32::NAN), "NaN");
    }

    #[test]
    fn an_f32_is_shortest_at_its_own_precision() {
        assert_eq!(f32_text(0.1), "0.1");
        assert_eq!(value_text(&AnyValue::Float32(16777217.0)), "16777216.0");
        let v = 3.4028235e38f32;
        assert_eq!(f32_text(v).parse::<f32>().unwrap(), v);
    }

    /// Polars' compact display rounds what the table previews; the exact text
    /// is the stored value, with no grouping and no settings consulted.
    #[test]
    fn exact_is_not_the_compact_display() {
        let v = AnyValue::Float64(1000000.125);
        assert_eq!(v.str_value(), "1.0000e6");
        assert_eq!(value_text(&v), "1000000.125");
        assert_eq!(value_text(&AnyValue::Int64(-1234567)), "-1234567");
    }

    #[test]
    fn datetimes_keep_every_digit_of_their_unit_and_the_offset() {
        // 2024-01-02 03:04:05.000120 UTC
        let us = 1_704_164_645_000_120i64;
        assert_eq!(
            value_text(&AnyValue::Datetime(us, TimeUnit::Microseconds, None)),
            "2024-01-02 03:04:05.000120"
        );
        assert_eq!(
            value_text(&AnyValue::Datetime(
                us * 1000 + 7,
                TimeUnit::Nanoseconds,
                None
            )),
            "2024-01-02 03:04:05.000120007"
        );
        let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
            .unwrap()
            .unwrap();
        assert_eq!(
            value_text(&AnyValue::Datetime(
                us,
                TimeUnit::Microseconds,
                Some(&paris)
            )),
            "2024-01-02 04:04:05.000120 +01:00"
        );
        let ms = 1_704_164_645_000i64;
        assert_eq!(
            value_text(&AnyValue::Datetime(ms, TimeUnit::Milliseconds, None)),
            "2024-01-02 03:04:05.000"
        );
    }

    #[test]
    fn dates_times_durations_and_decimals_are_whole() {
        assert_eq!(value_text(&AnyValue::Date(19724)), "2024-01-02");
        assert_eq!(
            value_text(&AnyValue::Time(3_723_000_000_123)),
            "01:02:03.000000123"
        );
        assert_eq!(
            value_text(&AnyValue::Duration(90_061_000_001, TimeUnit::Microseconds)),
            "PT90061.000001S"
        );
        assert_eq!(value_text(&AnyValue::Decimal(-123450, 10, 4)), "-12.3450");
    }

    #[test]
    fn a_null_is_empty_and_bytes_are_base64() {
        assert_eq!(value_text(&AnyValue::Null), "");
        assert_eq!(value_text(&AnyValue::Binary(b"hi\x00")), "aGkA");
    }

    #[test]
    fn the_escaped_view_tells_a_break_from_a_literal_backslash() {
        assert_eq!(escaped("line1\nline2"), r#""line1\nline2""#);
        assert_eq!(escaped(r"line1\nline2"), r#""line1\\nline2""#);
        assert_eq!(escaped("a\tb\r\n"), r#""a\tb\r\n""#);
        assert_eq!(escaped(""), r#""""#);
        assert_eq!(escaped("  pad "), r#""  pad ""#);
        assert_eq!(escaped("say \"hi\""), r#""say \"hi\"""#);
        assert_eq!(escaped("\u{1b}[0m"), r#""\u{1b}[0m""#);
        assert_eq!(
            escaped("no\u{a0}break\u{200b}"),
            r#""no\u{a0}break\u{200b}""#
        );
        assert_eq!(escaped("東京"), "\"東京\"");
    }

    #[test]
    fn bytes_escape() {
        assert_eq!(escaped_bytes(b"ab\x00\xff\""), r#"b"ab\x00\xff\"""#);
    }

    #[test]
    fn the_preview_marks_breaks_tabs_and_controls() {
        let g = crate::glyphs::unicode();
        assert_eq!(preview("line1\nline2", g), "line1¶line2");
        assert_eq!(preview("tab\tseparated", g), "tab»separated");
        assert_eq!(preview("crlf\r\nend", g), "crlf¶end");
        assert_eq!(preview("bell\u{7}", g), "bell¤");
        assert_eq!(preview("c1\u{85}", g), "c1¤");
        assert!(matches!(preview("plain é 東京", g), Cow::Borrowed(_)));
        let a = crate::glyphs::ascii();
        assert_eq!(preview("a\nb\tc\u{1b}", a), "a$b>c?");
    }

    #[test]
    fn text_facts_count_what_the_screen_hides() {
        let f = text_facts("  two\nlines ");
        assert_eq!(
            f,
            TextFacts {
                chars: 12,
                lines: 2,
                leading_spaces: 2,
                trailing_spaces: 1
            }
        );
        assert_eq!(text_facts("").lines, 0);
        assert_eq!(text_facts("   ").trailing_spaces, 0);
        assert_eq!(text_facts("   ").leading_spaces, 3);
    }

    #[test]
    fn prefix_cuts_at_a_character_boundary() {
        assert_eq!(prefix("東京", 4), "東");
        assert_eq!(prefix("abc", 10), "abc");
    }

    fn list(values: &[f64]) -> AnyValue<'static> {
        AnyValue::List(Series::new("".into(), values))
    }

    #[test]
    fn nested_values_lay_out_one_item_per_line_with_exact_scalars() {
        let pretty = nested_pretty(&list(&[1000000.125, -0.0]), usize::MAX);
        assert_eq!(pretty.text, "[\n  1000000.125,\n  -0.0\n]");
        assert!(!pretty.cut);
        assert_eq!(value_text(&list(&[1.5, 2.0])), "[1.5, 2.0]");
        assert_eq!(nested_pretty(&list(&[]), 100).text, "[]");

        let df = df!("name" => ["a\"b"], "n" => [Some(1i64)]).unwrap();
        let s = df.into_struct("s".into()).into_series();
        let value = s.get(0).unwrap();
        assert_eq!(nested_len(&value), Some(2));
        assert_eq!(
            nested_pretty(&value, usize::MAX).text,
            "{\n  \"name\": \"a\\\"b\",\n  \"n\": 1\n}"
        );
    }

    #[test]
    fn a_long_nested_value_stops_at_the_budget() {
        let values: Vec<f64> = (0..10_000).map(f64::from).collect();
        let pretty = nested_pretty(&list(&values), 200);
        assert!(pretty.cut);
        assert!(pretty.text.len() < 260, "{}", pretty.text.len());
    }

    /// Polars panics formatting these; the stored number stands in.
    #[test]
    fn a_date_past_the_calendar_is_its_stored_number() {
        let us = AnyValue::Datetime(i64::MIN + 1, TimeUnit::Microseconds, None);
        assert_eq!(
            value_text(&us),
            "-9223372036854775807 us since 1970-01-01 UTC"
        );
        let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
            .unwrap()
            .unwrap();
        let ms = AnyValue::Datetime(i64::MAX, TimeUnit::Milliseconds, Some(&paris));
        assert!(value_text(&ms).starts_with("9223372036854775807 ms"));
        assert_eq!(
            value_text(&AnyValue::Date(i32::MAX)),
            "2147483647 days since 1970-01-01"
        );
        // Every nanosecond count is a date; the edges keep their digits.
        assert_eq!(
            value_text(&AnyValue::Datetime(i64::MAX, TimeUnit::Nanoseconds, None)),
            "2262-04-11 23:47:16.854775807"
        );
        assert_eq!(value_text(&AnyValue::Date(-800_000)), "-0221-09-04");
    }

    /// The table's text for a value is Polars' own, except where Polars panics:
    /// a date, datetime or time past the calendar is its stored number, alone or
    /// inside a list or struct. Durations never panic and keep Polars' text.
    #[test]
    fn str_value_never_panics_on_a_date_past_the_calendar() {
        let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
            .unwrap()
            .unwrap();
        for (unit, name) in [
            (TimeUnit::Milliseconds, "ms"),
            (TimeUnit::Microseconds, "us"),
        ] {
            for zone in [None, Some(&paris)] {
                for v in [i64::MIN + 1, i64::MAX] {
                    assert_eq!(
                        str_value(&AnyValue::Datetime(v, unit, zone)),
                        format!("{v} {name} since 1970-01-01 UTC"),
                        "{unit:?} {zone:?}"
                    );
                }
                let epoch = AnyValue::Datetime(0, unit, zone);
                assert_eq!(str_value(&epoch), epoch.str_value());
            }
        }
        // Every nanosecond count is a date, with or without a zone.
        for zone in [None, Some(&paris)] {
            for v in [i64::MIN + 1, i64::MAX] {
                let value = AnyValue::Datetime(v, TimeUnit::Nanoseconds, zone);
                assert_eq!(str_value(&value), value.str_value());
            }
        }
        assert_eq!(
            str_value(&AnyValue::Date(i32::MAX)),
            "2147483647 days since 1970-01-01"
        );
        assert_eq!(
            str_value(&AnyValue::Date(i32::MIN)),
            "-2147483648 days since 1970-01-01"
        );
        assert_eq!(str_value(&AnyValue::Date(0)), "1970-01-01");
        assert_eq!(str_value(&AnyValue::Time(-1)), "-1 ns since midnight");
        assert_eq!(
            str_value(&AnyValue::Time(NANOS_PER_DAY)),
            "86400000000000 ns since midnight"
        );
        assert_eq!(
            str_value(&AnyValue::Time(NANOS_PER_DAY - 1)),
            "23:59:59.999999999"
        );
        for unit in [
            TimeUnit::Milliseconds,
            TimeUnit::Microseconds,
            TimeUnit::Nanoseconds,
        ] {
            for v in [i64::MIN, i64::MIN + 1, i64::MAX] {
                let value = AnyValue::Duration(v, unit);
                assert_eq!(str_value(&value), value.str_value());
            }
        }

        let stamps = |values: &[i64]| {
            Series::new("".into(), values)
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .unwrap()
        };
        let past = stamps(&[0, i64::MIN + 1]);
        assert_eq!(
            str_value(&AnyValue::List(past.clone())),
            r#"["1970-01-01 00:00:00.000000", "-9223372036854775807 us since 1970-01-01 UTC"]"#
        );
        let fine = AnyValue::List(stamps(&[0]));
        assert_eq!(str_value(&fine), fine.str_value());
        let nested = AnyValue::List(Series::new("".into(), [past.clone()]));
        assert!(str_value(&nested).contains("-9223372036854775807 us"));
        let row = StructChunked::from_series(
            "".into(),
            2,
            [
                Series::new("id".into(), [1i64, 2]),
                past.with_name("at".into()),
            ]
            .iter(),
        )
        .unwrap()
        .into_series();
        assert_eq!(
            str_value(&row.get(1).unwrap()),
            r#"{"id": 2, "at": "-9223372036854775807 us since 1970-01-01 UTC"}"#
        );
        assert_eq!(
            str_value(&row.get(0).unwrap()),
            row.get(0).unwrap().str_value()
        );
    }

    /// Only the values past the calendar are taken out; a column with none is
    /// left alone.
    #[test]
    fn values_past_the_calendar_become_null() {
        let dates = Series::new("d".into(), [Some(i32::MIN), Some(0), None, Some(i32::MAX)])
            .cast(&DataType::Date)
            .unwrap();
        let kept = calendar_without_out_of_range(&dates).unwrap().unwrap();
        assert_eq!(kept.dtype(), &DataType::Date);
        assert_eq!(kept.name().as_str(), "d");
        assert_eq!(kept.null_count(), 3);
        assert_eq!(kept.get(1).unwrap(), AnyValue::Date(0));
        let fine = dates.slice(1, 2);
        assert!(calendar_without_out_of_range(&fine).unwrap().is_none());
        let numbers = Series::new("n".into(), [i64::MIN, i64::MAX]);
        assert!(calendar_without_out_of_range(&numbers).unwrap().is_none());
    }

    #[test]
    fn the_escaped_view_spells_out_every_invisible_character() {
        for c in [
            '\u{61c}',
            '\u{2000}',
            '\u{3000}',
            '\u{180e}',
            '\u{fe0f}',
            '\u{e0041}',
            '\u{9b}',
            '\u{2066}',
            '\u{2028}',
        ] {
            assert_eq!(
                escaped(&c.to_string()),
                format!("\"\\u{{{:x}}}\"", c as u32)
            );
        }
    }

    /// A direction control would turn the rest of a row around in a terminal
    /// that lays out bidirectional text.
    #[test]
    fn the_preview_marks_direction_controls() {
        let g = crate::glyphs::unicode();
        assert_eq!(preview("a\u{202e}b\u{202c}c\u{61c}", g), "a¤b¤c¤");
        assert!(matches!(preview("שלום مرحبا — x", g), Cow::Borrowed(_)));
    }

    #[test]
    fn a_cell_previews_only_the_start_of_a_huge_value() {
        let g = crate::glyphs::unicode();
        let huge = "x".repeat(CELL_PREVIEW_BYTES * 10);
        let cell = cell_preview(&huge, g);
        assert_eq!(cell.len(), CELL_PREVIEW_BYTES + g.ellipsis.len());
        assert!(cell.ends_with(g.ellipsis));
        assert_eq!(cell_preview("a\nb", g), "a¶b");
    }

    /// One huge string or a million items in a list stop at the budget too.
    #[test]
    fn a_nested_value_is_cut_inside_a_huge_item() {
        let huge = "y".repeat(1 << 20);
        let s = Series::new("".into(), ["a", huge.as_str()]);
        let pretty = nested_pretty(&AnyValue::List(s), 200);
        assert!(pretty.cut);
        assert!(pretty.text.len() <= 210, "{}", pretty.text.len());
        assert!(pretty.text.starts_with("[\n  \"a\",\n  \"yyy"));
        let bytes = Series::new("".into(), [vec![7u8; 1 << 20].as_slice()]);
        let compact = nested_compact(&AnyValue::List(bytes), 100);
        assert!(compact.cut && compact.text.len() <= 100, "{}", compact.text);
        let many = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
        let compact = nested_compact(&AnyValue::List(many), 50);
        assert!(compact.cut && compact.text.len() < 60, "{}", compact.text);
    }

    /// The floor a capped copy is checked against is never more than what the
    /// copy writes, and it stops counting past its stop.
    #[test]
    fn copy_len_floor_is_a_floor_and_stops_early() {
        let point = StructChunked::from_columns(
            "p".into(),
            1,
            &[
                Column::new("x".into(), [Some(1i64)]),
                Column::new("label".into(), [None::<&str>]),
            ],
        )
        .unwrap()
        .into_column();
        let columns = [
            Column::new("s".into(), ["tab\there \"quoted\" été"]),
            Column::new("b".into(), [b"Hi\x00".as_slice()]),
            Column::new("f".into(), [1000000.125f64]),
            Column::new("n".into(), [None::<i64>]),
            Column::new("l".into(), [list(&[1.5, 2.0])]),
            Column::new("t".into(), [Series::new("".into(), ["a\"b", "", "日本"])]),
            Column::new("e".into(), [Series::new_empty("".into(), &DataType::Int64)]),
            point,
        ];
        for column in columns {
            let value = column.get(0).unwrap();
            let text = copy_text(&column).unwrap();
            let floor = copy_len_floor(&value, usize::MAX);
            assert!(
                floor <= text.len(),
                "{}: {floor} over {text:?}",
                column.name()
            );
        }
        // Text and bytes are exact.
        assert_eq!(copy_len_floor(&AnyValue::String("abcdef"), 0), 6);
        assert_eq!(copy_len_floor(&AnyValue::Binary(b"abcd"), 0), 8);
        // A million items are past a 1 KB stop from the brackets and commas alone.
        let many = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
        assert!(copy_len_floor(&AnyValue::List(many), 1024) > 1024);
    }

    #[test]
    fn copy_text_is_exact_and_nested_is_json() {
        let floats = Column::new("f".into(), [1000000.125f64]);
        assert_eq!(copy_text(&floats).unwrap(), "1000000.125");
        let nulls = Column::new("s".into(), [None::<&str>]);
        assert_eq!(copy_text(&nulls).unwrap(), "");
        let lists = Column::new("l".into(), [list(&[1.5, 2.0])]);
        assert_eq!(copy_text(&lists).unwrap(), "[1.5,2.0]");
    }

    /// The table's list preview: ten items, then how many there were.
    #[test]
    fn a_list_previews_ten_items_and_counts_the_rest() {
        let few = Series::new("".into(), &["a", "b"]);
        assert_eq!(list_preview(&few), "[a, b]");
        let many = Series::new("".into(), (0..12).collect::<Vec<i32>>());
        assert_eq!(
            list_preview(&many),
            "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9...] (12 items)"
        );
        let ten = Series::new("".into(), (0..10).collect::<Vec<i32>>());
        assert_eq!(list_preview(&ten), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
    }
}

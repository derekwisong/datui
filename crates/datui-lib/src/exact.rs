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
mod tests;

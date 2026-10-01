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
            crate::nested_json::duration_iso(*v, *unit, &mut out);
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
    let pad = |out: &mut String, depth: usize| {
        if indent.is_some() {
            out.push('\n');
            out.extend(std::iter::repeat_n("  ", depth));
        }
    };
    let items: Option<(Vec<Option<String>>, Vec<AnyValue>)> = match value {
        AnyValue::List(s) | AnyValue::Array(s, _) => {
            Some((vec![None; s.len()], s.iter().collect()))
        }
        AnyValue::Struct(..) | AnyValue::StructOwned(_) => {
            let fields: Vec<Option<String>> = match value {
                AnyValue::Struct(_, _, fields) => {
                    fields.iter().map(|f| Some(f.name().to_string())).collect()
                }
                AnyValue::StructOwned(payload) => payload
                    .1
                    .iter()
                    .map(|f| Some(f.name().to_string()))
                    .collect(),
                _ => unreachable!(),
            };
            let values = match value {
                AnyValue::StructOwned(payload) => payload.0.clone(),
                v => v._iter_struct_av().collect(),
            };
            Some((fields, values))
        }
        _ => None,
    };
    let Some((names, values)) = items else {
        out.push_str(&json_scalar(value));
        return true;
    };
    let (open, close) = if names.first().is_some_and(Option::is_some) {
        ('{', '}')
    } else {
        ('[', ']')
    };
    out.push(open);
    if values.is_empty() {
        out.push(close);
        return true;
    }
    for (i, (name, item)) in names.iter().zip(values.iter()).enumerate() {
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
        if !write_nested(item, out, indent, depth + 1, budget) {
            return false;
        }
        if out.len() >= budget && i + 1 < values.len() {
            return false;
        }
    }
    pad(out, depth);
    out.push(close);
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
/// escaped view spells it out: controls, no-break and zero-width spaces, the
/// soft hyphen, direction marks and the byte-order mark.
fn invisible(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{a0}'
                | '\u{ad}'
                | '\u{200b}'..='\u{200f}'
                | '\u{2028}'..='\u{202f}'
                | '\u{2060}'..='\u{2064}'
                | '\u{2066}'..='\u{2069}'
                | '\u{feff}'
        )
}

fn escape_into(s: &str, out: &mut String) {
    for c in s.chars() {
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

/// Bytes as hex-dump lines, `width` bytes to a line: offset, hex, then the
/// printable ASCII with a dot for the rest.
pub fn hex_lines(bytes: &[u8], width: usize) -> Vec<String> {
    let width = width.max(1);
    bytes
        .chunks(width)
        .enumerate()
        .map(|(i, chunk)| {
            let mut line = format!("{:08x} ", i * width);
            for b in chunk {
                let _ = write!(line, " {b:02x}");
            }
            for _ in chunk.len()..width {
                line.push_str("   ");
            }
            line.push_str("  ");
            line.extend(chunk.iter().map(|&b| {
                if (0x20..0x7f).contains(&b) {
                    b as char
                } else {
                    '.'
                }
            }));
            line
        })
        .collect()
}

/// Whether one-line text holds a character a cell cannot draw.
fn has_control(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
        || (s.as_bytes().contains(&0xc2) && s.chars().any(char::is_control))
}

/// One-line preview of `s`: a line break, a tab or another control character
/// becomes a mark from the glyph set, so `line1\nline2` does not read as
/// `line1line2`. A `\r\n` pair is one break.
pub fn preview<'a>(s: &'a str, g: &crate::glyphs::Glyphs) -> Cow<'a, str> {
    if !has_control(s) {
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
            c if c.is_control() => out.push_str(g.control_mark),
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
        let json = crate::nested_json::column_as_json(column)?;
        return Ok(match json.get(0)? {
            AnyValue::Null => String::new(),
            v => v.str_value().into_owned(),
        });
    }
    Ok(value_text(&value))
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
    fn bytes_escape_and_dump() {
        assert_eq!(escaped_bytes(b"ab\x00\xff\""), r#"b"ab\x00\xff\"""#);
        let lines = hex_lines(b"Hello, world!\x00\x01", 8);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "00000000  48 65 6c 6c 6f 2c 20 77  Hello, w");
        assert_eq!(lines[1], "00000008  6f 72 6c 64 21 00 01     orld!..");
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

    #[test]
    fn copy_text_is_exact_and_nested_is_json() {
        let floats = Column::new("f".into(), [1000000.125f64]);
        assert_eq!(copy_text(&floats).unwrap(), "1000000.125");
        let nulls = Column::new("s".into(), [None::<&str>]);
        assert_eq!(copy_text(&nulls).unwrap(), "");
        let lists = Column::new("l".into(), [list(&[1.5, 2.0])]);
        assert_eq!(copy_text(&lists).unwrap(), "[1.5,2.0]");
    }
}

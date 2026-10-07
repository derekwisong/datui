//! Display-time number formatting: digit grouping, decimal separator, optional fixed
//! float precision. Display only: exports, queries, filters, views and group-by keys use
//! raw values. Runs per visible cell per frame, so it allocates nothing beyond the
//! caller's `String`: integers are written digit by digit with inline separators,
//! [`NumberFormat::width_i64`] measures arithmetically, and per-column decisions resolve
//! to a [`CellFormatter`] once per column per frame.

use std::borrow::Cow;
use std::fmt::Write as _;

use polars::prelude::{AnyValue, DataType};

/// Digit grouping style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Grouping {
    /// No grouping: `1234567`
    #[default]
    None,
    /// Western grouping in threes: `1,234,567`
    Thousands,
    /// Indian grouping — three, then twos: `12,34,567` (lakh / crore)
    Indian,
}

impl Grouping {
    /// Number of separators a value with `digits` integer digits will contain.
    fn separator_count(self, digits: usize) -> usize {
        match self {
            Grouping::None => 0,
            Grouping::Thousands => digits.saturating_sub(1) / 3,
            Grouping::Indian => {
                if digits <= 3 {
                    0
                } else {
                    1 + (digits - 4) / 2
                }
            }
        }
    }

    /// Whether a separator goes before the next digit, given how many digits
    /// have already been emitted (counting from the right).
    #[inline]
    fn breaks_after(self, digits_emitted: u32) -> bool {
        match self {
            Grouping::None => false,
            Grouping::Thousands => digits_emitted.is_multiple_of(3),
            Grouping::Indian => {
                digits_emitted == 3 || (digits_emitted > 3 && digits_emitted % 2 == 1)
            }
        }
    }
}

/// How numbers are rendered. Resolved from config once at load time.
#[derive(Debug, Clone, PartialEq)]
pub struct NumberFormat {
    pub grouping: Grouping,
    /// Character placed between digit groups.
    pub group_sep: char,
    /// Character used as the decimal point.
    pub decimal_sep: char,
    /// Apply grouping to float columns as well as integer columns.
    pub floats: bool,
    /// Fixed decimal places for floats. `None` keeps the default rendering.
    pub float_precision: Option<u8>,
}

impl Default for NumberFormat {
    fn default() -> Self {
        Self::PLAIN
    }
}

impl NumberFormat {
    /// Renders numbers exactly as Polars would — no grouping, no precision
    /// override. This is the default so an upgrade changes nothing.
    pub const PLAIN: Self = Self {
        grouping: Grouping::None,
        group_sep: ',',
        decimal_sep: '.',
        floats: true,
        float_precision: None,
    };

    /// A named preset covering common locale conventions without ICU/CLDR data (see
    /// `docs/user-guide/configuration.md` on why formatting is explicit).
    pub fn preset(name: &str) -> Option<Self> {
        let base = Self::PLAIN;
        Some(match name {
            // 1234567
            "none" | "plain" => base,
            // 1,234,567.89
            "thousands" => Self {
                grouping: Grouping::Thousands,
                group_sep: ',',
                decimal_sep: '.',
                ..base
            },
            // 1.234.567,89
            "european" => Self {
                grouping: Grouping::Thousands,
                group_sep: '.',
                decimal_sep: ',',
                ..base
            },
            // 1 234 567.89 (ISO 31-0 / SI)
            "si" => Self {
                grouping: Grouping::Thousands,
                group_sep: '\u{202f}', // narrow no-break space
                decimal_sep: '.',
                ..base
            },
            // 1'234'567.89
            "swiss" => Self {
                grouping: Grouping::Thousands,
                group_sep: '\'',
                decimal_sep: '.',
                ..base
            },
            // 12,34,567.89
            "indian" => Self {
                grouping: Grouping::Indian,
                group_sep: ',',
                decimal_sep: '.',
                ..base
            },
            // 1_234_567.89
            "underscore" => Self {
                grouping: Grouping::Thousands,
                group_sep: '_',
                decimal_sep: '.',
                ..base
            },
            _ => return None,
        })
    }

    /// Comma grouping with no digit threshold, used for the application's own
    /// labels rather than the user's data.
    pub const CHROME: Self = Self {
        grouping: Grouping::Thousands,
        group_sep: ',',
        decimal_sep: '.',
        floats: true,
        float_precision: None,
    };

    /// Every preset name, for CLI value parsing and error messages.
    pub const PRESET_NAMES: &'static [&'static str] = &[
        "none",
        "thousands",
        "european",
        "si",
        "swiss",
        "indian",
        "underscore",
    ];

    /// True when this format would render every value exactly as Polars does,
    /// so callers can take the zero-cost passthrough path.
    pub fn is_noop(&self) -> bool {
        self.grouping == Grouping::None && self.decimal_sep == '.' && self.float_precision.is_none()
    }

    /// Whether values are grouped: every value in a formatted column, without a magnitude
    /// threshold, for uniform columns; identifier columns belong in `exclude`.
    #[inline]
    fn groups(&self) -> bool {
        self.grouping != Grouping::None
    }

    /// Display width (in characters) of `v` as this format would render it,
    /// computed arithmetically — no string is built.
    pub fn width_i64(&self, v: i64) -> usize {
        self.width_u64(v.unsigned_abs()) + usize::from(v < 0)
    }

    /// Display width (in characters) of `v` as this format would render it.
    pub fn width_u64(&self, v: u64) -> usize {
        let digits = digit_count(v);
        let seps = if self.groups() {
            self.grouping.separator_count(digits)
        } else {
            0
        };
        digits + seps
    }

    /// Append `v` to `out`, returning the display width in characters.
    pub fn write_i64(&self, v: i64, out: &mut String) -> usize {
        self.write_magnitude(v.unsigned_abs(), v < 0, out)
    }

    /// Append `v` to `out`, returning the display width in characters.
    pub fn write_u64(&self, v: u64, out: &mut String) -> usize {
        self.write_magnitude(v, false, out)
    }

    /// Core integer path: writes digits back-to-front into a stack buffer,
    /// emitting separators inline, then appends the finished slice in one go.
    fn write_magnitude(&self, mag: u64, negative: bool, out: &mut String) -> usize {
        // u64::MAX is 20 digits; Indian grouping tops out at 9 separators, each
        // at most 4 UTF-8 bytes; plus a sign.
        let mut buf = [0u8; 64];
        let mut pos = buf.len();
        let mut width = 0usize;

        let group = self.groups();
        let mut sep_bytes = [0u8; 4];
        let sep = self.group_sep.encode_utf8(&mut sep_bytes);
        let sep = sep.as_bytes();

        let mut n = mag;
        let mut emitted: u32 = 0;
        loop {
            let d = (n % 10) as u8;
            n /= 10;
            pos -= 1;
            buf[pos] = b'0' + d;
            emitted += 1;
            width += 1;
            if n == 0 {
                break;
            }
            if group && self.grouping.breaks_after(emitted) {
                pos -= sep.len();
                buf[pos..pos + sep.len()].copy_from_slice(sep);
                width += 1;
            }
        }

        if negative {
            pos -= 1;
            buf[pos] = b'-';
            width += 1;
        }

        // Every byte written is either ASCII or a complete UTF-8 encoding of
        // `group_sep`, so the slice is valid UTF-8 by construction.
        debug_assert!(std::str::from_utf8(&buf[pos..]).is_ok());
        match std::str::from_utf8(&buf[pos..]) {
            Ok(s) => {
                out.push_str(s);
                width
            }
            // Unreachable; zero rather than `width`, since a width not matching what was pushed
            // would corrupt column sizing silently.
            Err(_) => 0,
        }
    }

    /// Append `v` to `out`, returning its display width; `scratch` is a reused buffer.
    pub fn write_f64(&self, v: f64, scratch: &mut String, out: &mut String) -> usize {
        scratch.clear();
        match self.float_precision {
            Some(p) => {
                let _ = write!(scratch, "{:.*}", p as usize, v);
            }
            None => {
                let _ = write!(scratch, "{}", v);
            }
        }
        self.regroup_decimal(scratch, out)
    }

    /// Group the integer part of an already-rendered decimal and apply `decimal_sep`,
    /// returning the width. Keeps Polars' rendering (decimal places never change with
    /// formatting); non-plain decimals (`NaN`, `inf`, scientific) pass through.
    pub fn regroup_decimal(&self, src: &str, out: &mut String) -> usize {
        let body = src.strip_prefix('-').unwrap_or(src);
        let negative = body.len() != src.len();
        let (int_part, frac_part) = match body.find('.') {
            Some(i) => (&body[..i], Some(&body[i + 1..])),
            None => (body, None),
        };

        let plain = !int_part.is_empty()
            && int_part.bytes().all(|b| b.is_ascii_digit())
            && frac_part.is_none_or(|f| f.bytes().all(|b| b.is_ascii_digit()));
        if !plain {
            // NaN, inf, 1e300 — not something to regroup.
            out.push_str(src);
            return src.chars().count();
        }

        let mut width = 0usize;
        if negative {
            out.push('-');
            width += 1;
        }

        let digits = int_part.len();
        if self.groups() {
            for (i, ch) in int_part.chars().enumerate() {
                // A separator precedes this digit when the digits still to come
                // (including this one) land on a group boundary.
                let remaining = (digits - i) as u32;
                if i > 0 && self.grouping.breaks_after(remaining) {
                    out.push(self.group_sep);
                    width += 1;
                }
                out.push(ch);
                width += 1;
            }
        } else {
            out.push_str(int_part);
            width += digits;
        }

        if let Some(frac) = frac_part {
            out.push(self.decimal_sep);
            width += 1 + frac.len();
            out.push_str(frac);
        }
        width
    }
}

/// A byte count in binary units: `512 B`, `1.2 MiB`, and whole from 100 up, `340 MiB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{n} B"),
        _ if value >= 100.0 => format!("{value:.0} {}", UNITS[unit]),
        _ => format!("{value:.1} {}", UNITS[unit]),
    }
}

/// A wait's clock, to the second while seconds matter: `12s`, `3m 05s`, `1h 02m`.
pub fn clock(elapsed: std::time::Duration) -> String {
    let s = elapsed.as_secs();
    match s {
        0..60 => format!("{s}s"),
        60..3_600 => format!("{}m {:02}s", s / 60, s % 60),
        _ => format!("{}h {:02}m", s / 3_600, s % 3_600 / 60),
    }
}

/// A length of time in its largest unit, to a tenth past seconds so it fits a
/// narrow column: `12s`, `2.5m`, `1.5h`, `-2.1d`.
pub fn duration(seconds: i64) -> String {
    let sign = if seconds < 0 { "-" } else { "" };
    let s = seconds.unsigned_abs();
    let (unit, name) = match s {
        0..60 => return format!("{sign}{s}s"),
        60..3_600 => (60.0, "m"),
        3_600..86_400 => (3_600.0, "h"),
        _ => (86_400.0, "d"),
    };
    format!("{sign}{:.1}{name}", s as f64 / unit)
}

/// [`duration`], or `-` for none.
pub fn duration_or_dash(seconds: Option<i64>) -> String {
    seconds.map_or_else(|| "-".to_string(), duration)
}

/// A share as a percentage: a tenth of a percent, two places below 1% so a small
/// share never reads as none, and `<0.01%` below that.
pub fn percent(share: f64) -> String {
    let pct = share * 100.0;
    if pct > 0.0 && pct < 0.01 {
        "<0.01%".to_string()
    } else if pct > 0.0 && pct < 1.0 {
        format!("{pct:.2}%")
    } else {
        format!("{pct:.1}%")
    }
}

/// [`percent`] of `count` in `of`, or a dash with nothing to take it over.
pub fn percent_of(count: usize, of: usize) -> String {
    if of == 0 {
        return "-".to_string();
    }
    percent(count as f64 / of as f64)
}

/// Comma-group a count for datui's own chrome (footer count, info totals), regardless
/// of `display.number_format` or `,`: turning formatting off for exact data never makes
/// the UI harder to read.
pub fn group_chrome(n: usize) -> String {
    let mut out = String::new();
    NumberFormat::CHROME.write_u64(n as u64, &mut out);
    out
}

/// Digits in the base-10 representation of `n` (`0` counts as one digit).
#[inline]
fn digit_count(n: u64) -> usize {
    n.checked_ilog10().map_or(0, |l| l as usize) + 1
}

/// Per-column formatting decision, resolved once per column per frame.
#[derive(Debug, Clone, PartialEq)]
pub enum CellFormatter {
    /// Render exactly as Polars does. Zero added cost.
    Passthrough,
    /// Apply `NumberFormat` to numeric values.
    Number(NumberFormat),
}

impl CellFormatter {
    #[inline]
    pub fn is_passthrough(&self) -> bool {
        matches!(self, CellFormatter::Passthrough)
    }
}

/// True for dtypes whose values are numbers we group.
pub fn is_numeric_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Dtypes rendered flush right; temporals excluded (fixed-width already).
pub fn is_right_aligned_dtype(dtype: &DataType) -> bool {
    is_numeric_dtype(dtype)
}

/// Fully resolved display-formatting settings, held in the render context.
#[derive(Debug, Clone)]
pub struct NumberFormatSettings {
    /// The configured format.
    pub format: NumberFormat,
    /// Runtime `,` toggle. When false every column is `Passthrough`.
    pub enabled: bool,
    /// Columns never formatted (precompiled globs).
    pub exclude: Vec<Glob>,
    /// Right-align numeric columns and their headers.
    pub align_numeric_right: bool,
}

impl Default for NumberFormatSettings {
    fn default() -> Self {
        Self {
            format: NumberFormat::PLAIN,
            enabled: true,
            exclude: Vec::new(),
            align_numeric_right: true,
        }
    }
}

impl NumberFormatSettings {
    /// Resolve the formatter for one column. Called once per column per frame —
    /// never per cell, so glob matching stays off the hot path.
    pub fn formatter_for(&self, col_name: &str, dtype: &DataType) -> CellFormatter {
        if !self.enabled || self.format.is_noop() || !is_numeric_dtype(dtype) {
            return CellFormatter::Passthrough;
        }
        if self.exclude.iter().any(|g| g.matches(col_name)) {
            return CellFormatter::Passthrough;
        }
        let mut fmt = self.format.clone();
        if !fmt.floats && matches!(dtype, DataType::Float32 | DataType::Float64) {
            // Grouping is off for floats, but a decimal separator or fixed
            // precision may still apply.
            fmt.grouping = Grouping::None;
            if fmt.is_noop() {
                return CellFormatter::Passthrough;
            }
        }
        CellFormatter::Number(fmt)
    }
}

/// Format one value for display; `Cow::Borrowed` when no formatting applies.
pub fn format_any_value<'v>(
    fmt: &CellFormatter,
    value: &'v AnyValue<'v>,
    scratch: &mut String,
) -> Cow<'v, str> {
    if matches!(value, AnyValue::Null) {
        return Cow::Borrowed("");
    }
    let nf = match fmt {
        CellFormatter::Passthrough => return crate::exact::str_value(value),
        CellFormatter::Number(nf) => nf,
    };
    let mut out = String::new();
    match *value {
        AnyValue::Int8(v) => nf.write_i64(v as i64, &mut out),
        AnyValue::Int16(v) => nf.write_i64(v as i64, &mut out),
        AnyValue::Int32(v) => nf.write_i64(v as i64, &mut out),
        AnyValue::Int64(v) => nf.write_i64(v, &mut out),
        AnyValue::UInt8(v) => nf.write_u64(v as u64, &mut out),
        AnyValue::UInt16(v) => nf.write_u64(v as u64, &mut out),
        AnyValue::UInt32(v) => nf.write_u64(v as u64, &mut out),
        AnyValue::UInt64(v) => nf.write_u64(v, &mut out),
        // With no explicit precision, floats route through Polars' own
        // rendering so toggling formatting never changes how many decimal
        // places a value shows — only the grouping is layered on.
        AnyValue::Float32(f) => match nf.float_precision {
            Some(_) => nf.write_f64(f as f64, scratch, &mut out),
            None => nf.regroup_decimal(&value.str_value(), &mut out),
        },
        AnyValue::Float64(f) => match nf.float_precision {
            Some(_) => nf.write_f64(f, scratch, &mut out),
            None => nf.regroup_decimal(&value.str_value(), &mut out),
        },
        _ => return crate::exact::str_value(value),
    };
    Cow::Owned(out)
}

/// A value's on-screen cells without building its string where possible (integers);
/// others are rendered and measured as the table measures them.
pub fn display_width(fmt: &CellFormatter, value: &AnyValue, scratch: &mut String) -> usize {
    if matches!(value, AnyValue::Null) {
        return 0;
    }
    if let CellFormatter::Number(nf) = fmt {
        match *value {
            AnyValue::Int8(v) => return nf.width_i64(v as i64),
            AnyValue::Int16(v) => return nf.width_i64(v as i64),
            AnyValue::Int32(v) => return nf.width_i64(v as i64),
            AnyValue::Int64(v) => return nf.width_i64(v),
            AnyValue::UInt8(v) => return nf.width_u64(v as u64),
            AnyValue::UInt16(v) => return nf.width_u64(v as u64),
            AnyValue::UInt32(v) => return nf.width_u64(v as u64),
            AnyValue::UInt64(v) => return nf.width_u64(v),
            _ => {}
        }
    }
    crate::glyphs::cell_width(&format_any_value(fmt, value, scratch))
}

/// Minimal glob matcher: `*` (any run) and `?` (one character), all column-name
/// matching needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Glob {
    pattern: String,
    has_wildcard: bool,
}

impl Glob {
    pub fn new(pattern: impl Into<String>) -> Self {
        let pattern = pattern.into();
        let has_wildcard = pattern.contains('*') || pattern.contains('?');
        Self {
            pattern,
            has_wildcard,
        }
    }

    pub fn matches(&self, name: &str) -> bool {
        if !self.has_wildcard {
            return self.pattern == name;
        }
        let p: Vec<char> = self.pattern.chars().collect();
        let n: Vec<char> = name.chars().collect();
        // Standard two-pointer wildcard match with backtracking on the last `*`.
        let (mut pi, mut ni) = (0usize, 0usize);
        let (mut star, mut mark) = (usize::MAX, 0usize);
        while ni < n.len() {
            // `*` before the literal comparison, or a literal `*` in the name would consume it as
            // a wildcard (found by the `glob_match` fuzz target).
            if pi < p.len() && p[pi] == '*' {
                star = pi;
                mark = ni;
                pi += 1;
            } else if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
                pi += 1;
                ni += 1;
            } else if star != usize::MAX {
                pi = star + 1;
                mark += 1;
                ni = mark;
            } else {
                return false;
            }
        }
        while pi < p.len() && p[pi] == '*' {
            pi += 1;
        }
        pi == p.len()
    }
}

/// Map a POSIX locale tag to its preset, only for an explicit `grouping = "system"`. A
/// small static table rather than megabytes of ICU/CLDR to pick a separator.
pub fn preset_for_locale_tag(tag: &str) -> &'static str {
    // Strip encoding/modifier suffixes: "de_DE.UTF-8@euro" -> "de_DE"
    let base = tag
        .split(['.', '@'])
        .next()
        .unwrap_or(tag)
        .replace('_', "-");
    let lower = base.to_ascii_lowercase();
    let lang = lower.split('-').next().unwrap_or(&lower);
    let region = lower.split('-').nth(1).unwrap_or("");

    // Swiss variants group with apostrophes regardless of language.
    if region == "ch" {
        return "swiss";
    }
    match lang {
        "de" | "es" | "it" | "pt" | "nl" | "id" | "tr" | "da" | "el" | "ro" | "ca" | "vi"
        | "sl" | "hr" | "sr" | "is" => "european",
        "fr" | "nb" | "no" | "sv" | "fi" | "cs" | "sk" | "pl" | "ru" | "uk" | "hu" | "lv"
        | "lt" | "et" | "bg" => "si",
        "hi" | "bn" | "ta" | "te" | "mr" | "gu" | "kn" | "ml" | "pa" | "or" | "as" | "ne" => {
            "indian"
        }
        // C / POSIX / unset and everything else: plain Western grouping.
        _ => "thousands",
    }
}

/// Read the environment's numeric locale, honouring POSIX precedence.
/// Returns `None` when unset or explicitly the C/POSIX locale.
pub fn system_locale_tag() -> Option<String> {
    for var in ["LC_ALL", "LC_NUMERIC", "LANG"] {
        if let Ok(v) = std::env::var(var) {
            let v = v.trim();
            if v.is_empty() {
                continue;
            }
            if v == "C" || v == "POSIX" || v.starts_with("C.") {
                return None;
            }
            return Some(v.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests;

//! Display-time number formatting: digit grouping (thousands separators),
//! decimal separator choice, and optional fixed float precision.
//!
//! **This is display-only.** Exports, queries, filter values, templates, and
//! group-by key strings always use raw values — see [`format_any_value`] call
//! sites. Nothing here is ever fed back into Polars.
//!
//! # Performance
//!
//! Formatting runs once per *visible* cell per frame (roughly
//! `visible_rows * visible_cols`), so the hot path is deliberately allocation
//! free beyond the destination `String` the caller already needs:
//!
//! - Integers are written digit-by-digit into a stack buffer with separators
//!   emitted inline — one pass, no intermediate string.
//! - [`NumberFormat::width_i64`] computes display width arithmetically, so the
//!   locked-column measurement pass never builds a string it throws away.
//! - Per-column decisions (dtype eligibility, exclude globs) resolve to a
//!   [`CellFormatter`] once per column per frame, never per cell.

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

    /// Look up a named preset. Presets cover the common locale conventions
    /// without pulling in ICU/CLDR data — see `docs/user-guide/configuration.md`
    /// for why formatting is explicit rather than auto-detected from the
    /// environment.
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

    /// Whether values get digit grouping at all.
    ///
    /// Every value in a formatted column is grouped, with no magnitude
    /// threshold: within a table, uniform treatment of a column reads better
    /// than the prose convention of leaving four-digit numbers alone. Columns
    /// holding identifiers rather than quantities are named in `exclude`.
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
            // Unreachable. Report zero rather than `width` anyway: callers size
            // table columns from this return value, so a width that does not
            // match what was actually pushed would corrupt the layout instead
            // of failing visibly.
            Err(_) => 0,
        }
    }

    /// Append `v` to `out`, returning the display width in characters.
    ///
    /// `scratch` is a caller-owned reusable buffer; after the first call in a
    /// render pass it has enough capacity and no longer allocates.
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

    /// Insert group separators into the integer part of an already-rendered
    /// decimal string and apply `decimal_sep`. Returns the display width.
    ///
    /// Used for floats so Polars' own rendering is preserved and only the
    /// grouping is layered on — toggling formatting never changes how many
    /// decimal places a value shows. Anything that is not a plain decimal
    /// (`NaN`, `inf`, scientific notation) is copied through unchanged.
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

/// Comma-group a count for the application's own chrome — the control bar's
/// row count, info-panel totals, and similar labels.
///
/// Deliberately unconditional: these are datui's labels, not the user's data,
/// so they stay readable regardless of `display.number_format` or the `F`
/// toggle. Keeping the distinction means turning formatting off to read exact
/// data values never makes the surrounding UI harder to read.
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

/// True for dtypes that should render flush-right in the data table.
///
/// Temporal types are deliberately excluded: they render fixed-width, so
/// alignment buys nothing, and moving them would be a change unrelated to
/// number formatting.
pub fn is_right_aligned_dtype(dtype: &DataType) -> bool {
    is_numeric_dtype(dtype)
}

/// Fully resolved display-formatting settings, held in the render context.
#[derive(Debug, Clone)]
pub struct NumberFormatSettings {
    /// The configured format.
    pub format: NumberFormat,
    /// Runtime `F` toggle. When false every column is `Passthrough`.
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

/// Format one Polars value for display.
///
/// Returns `Cow::Borrowed` whenever no formatting applies, so unformatted
/// columns cost exactly what they cost today.
pub fn format_any_value<'v>(
    fmt: &CellFormatter,
    value: &'v AnyValue<'v>,
    scratch: &mut String,
) -> Cow<'v, str> {
    if matches!(value, AnyValue::Null) {
        return Cow::Borrowed("");
    }
    let nf = match fmt {
        CellFormatter::Passthrough => return value.str_value(),
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
        _ => return value.str_value(),
    };
    Cow::Owned(out)
}

/// Display width of a value without building its string, where possible.
///
/// Integers take the arithmetic path; everything else falls back to rendering.
/// Used by the locked-column measurement pass, which previously built and threw
/// away a `String` for every locked cell.
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
    format_any_value(fmt, value, scratch).chars().count()
}

/// Minimal glob matcher supporting `*` (any run) and `?` (one character).
///
/// A dependency would be overkill for matching column names; this is the whole
/// feature surface the config documents.
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
            // `*` is tested before the literal comparison, not after. With the order
            // reversed a `*` in the pattern matched a literal `*` in the name and then
            // stopped being a wildcard, so `*` failed to match a name like "a*b".
            // Found by the `glob_match` fuzz target.
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

/// Map a POSIX locale / language tag to the preset whose conventions match.
///
/// Only consulted when the user explicitly opts in with `grouping = "system"`.
/// Deliberately a small static table rather than ICU/CLDR: locale data is
/// multiple megabytes, and a single-binary TUI should not carry it to choose a
/// separator character.
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
mod tests {
    use super::*;

    fn fmt_i64(nf: &NumberFormat, v: i64) -> String {
        let mut s = String::new();
        let w = nf.write_i64(v, &mut s);
        assert_eq!(w, s.chars().count(), "reported width disagrees with output");
        assert_eq!(w, nf.width_i64(v), "width_i64 disagrees with write_i64");
        s
    }

    fn thousands() -> NumberFormat {
        NumberFormat::preset("thousands").unwrap()
    }

    #[test]
    fn digit_count_basics() {
        assert_eq!(digit_count(0), 1);
        assert_eq!(digit_count(9), 1);
        assert_eq!(digit_count(10), 2);
        assert_eq!(digit_count(999), 3);
        assert_eq!(digit_count(1000), 4);
        assert_eq!(digit_count(u64::MAX), 20);
    }

    #[test]
    fn every_value_in_a_formatted_column_is_grouped() {
        // No magnitude threshold: a column must not mix "1000" and
        // "248,956,422". Columns holding identifiers are named in `exclude`
        // instead of being guessed at by size.
        let nf = thousands();
        assert_eq!(fmt_i64(&nf, 0), "0");
        assert_eq!(fmt_i64(&nf, 999), "999");
        assert_eq!(fmt_i64(&nf, 1000), "1,000");
        assert_eq!(fmt_i64(&nf, 2024), "2,024");
        assert_eq!(fmt_i64(&nf, 9999), "9,999");
        assert_eq!(fmt_i64(&nf, 10000), "10,000");
        assert_eq!(fmt_i64(&nf, 1234567), "1,234,567");
    }

    #[test]
    fn negatives_and_extremes() {
        let nf = thousands();
        assert_eq!(fmt_i64(&nf, -1234567), "-1,234,567");
        assert_eq!(fmt_i64(&nf, -999), "-999");
        assert_eq!(fmt_i64(&nf, i64::MIN), "-9,223,372,036,854,775,808");
        assert_eq!(fmt_i64(&nf, i64::MAX), "9,223,372,036,854,775,807");

        let mut s = String::new();
        let w = nf.write_u64(u64::MAX, &mut s);
        assert_eq!(s, "18,446,744,073,709,551,615");
        assert_eq!(w, s.chars().count());
        assert_eq!(w, nf.width_u64(u64::MAX));
    }

    #[test]
    fn bed_style_coordinates() {
        // The case from issue #51: genomic coordinates in the millions/billions.
        let nf = thousands();
        assert_eq!(fmt_i64(&nf, 248_956_422), "248,956,422");
        assert_eq!(fmt_i64(&nf, 3_088_269_832), "3,088,269,832");
    }

    #[test]
    fn indian_grouping() {
        let nf = NumberFormat::preset("indian").unwrap();
        assert_eq!(fmt_i64(&nf, 100), "100");
        assert_eq!(fmt_i64(&nf, 1000), "1,000");
        assert_eq!(fmt_i64(&nf, 12345), "12,345");
        assert_eq!(fmt_i64(&nf, 123456), "1,23,456");
        assert_eq!(fmt_i64(&nf, 1234567), "12,34,567");
        assert_eq!(fmt_i64(&nf, 12345678), "1,23,45,678");
        assert_eq!(fmt_i64(&nf, -12345678), "-1,23,45,678");
    }

    #[test]
    fn all_presets_render() {
        let cases = [
            ("none", "1234567"),
            ("thousands", "1,234,567"),
            ("european", "1.234.567"),
            ("si", "1\u{202f}234\u{202f}567"),
            ("swiss", "1'234'567"),
            ("indian", "12,34,567"),
            ("underscore", "1_234_567"),
        ];
        for (name, expected) in cases {
            let nf = NumberFormat::preset(name).unwrap();
            assert_eq!(fmt_i64(&nf, 1234567), expected, "preset {name}");
        }
        assert!(NumberFormat::preset("klingon").is_none());
        for name in NumberFormat::PRESET_NAMES {
            assert!(NumberFormat::preset(name).is_some(), "preset {name}");
        }
    }

    #[test]
    fn width_matches_rendered_length_across_range() {
        for nf in NumberFormat::PRESET_NAMES
            .iter()
            .map(|n| NumberFormat::preset(n).unwrap())
        {
            let mut v: i64 = 1;
            for _ in 0..19 {
                for probe in [v, v - 1, -v, v * 3 / 2] {
                    let mut s = String::new();
                    let w = nf.write_i64(probe, &mut s);
                    assert_eq!(w, s.chars().count(), "{:?} on {probe}", nf.grouping);
                    assert_eq!(w, nf.width_i64(probe), "{:?} on {probe}", nf.grouping);
                }
                v = v.saturating_mul(10);
            }
        }
    }

    #[test]
    fn floats_regroup_integer_part_only() {
        let nf = thousands();
        let mut s = String::new();
        let w = nf.regroup_decimal("1234567.891", &mut s);
        assert_eq!(s, "1,234,567.891");
        assert_eq!(w, s.chars().count());

        s.clear();
        nf.regroup_decimal("-1234.5", &mut s);
        assert_eq!(s, "-1,234.5");
    }

    #[test]
    fn european_swaps_decimal_separator() {
        let nf = NumberFormat::preset("european").unwrap();
        let mut s = String::new();
        let w = nf.regroup_decimal("1234567.89", &mut s);
        assert_eq!(s, "1.234.567,89");
        assert_eq!(w, s.chars().count());
    }

    #[test]
    fn non_decimal_strings_pass_through_untouched() {
        let nf = thousands();
        for src in ["NaN", "inf", "-inf", "1e300", "1.5e-8", ""] {
            let mut s = String::new();
            let w = nf.regroup_decimal(src, &mut s);
            assert_eq!(s, src, "{src} should pass through");
            assert_eq!(w, src.chars().count());
        }
    }

    #[test]
    fn float_precision_is_applied() {
        let nf = NumberFormat {
            float_precision: Some(2),
            ..thousands()
        };
        let (mut scratch, mut out) = (String::new(), String::new());
        let w = nf.write_f64(1234.5678, &mut scratch, &mut out);
        assert_eq!(out, "1,234.57");
        assert_eq!(w, out.chars().count());

        out.clear();
        nf.write_f64(-0.5, &mut scratch, &mut out);
        assert_eq!(out, "-0.50");
    }

    #[test]
    fn is_noop_detects_the_free_path() {
        assert!(NumberFormat::PLAIN.is_noop());
        assert!(!thousands().is_noop());
        assert!(!NumberFormat {
            float_precision: Some(2),
            ..NumberFormat::PLAIN
        }
        .is_noop());
        assert!(!NumberFormat {
            decimal_sep: ',',
            ..NumberFormat::PLAIN
        }
        .is_noop());
    }

    #[test]
    fn chrome_grouping_is_unconditional() {
        // The app's own labels group regardless of the user's data settings,
        // and with no digit threshold: "Rows: 1,234" not "Rows: 1234".
        assert_eq!(group_chrome(0), "0");
        assert_eq!(group_chrome(999), "999");
        assert_eq!(group_chrome(1234), "1,234");
        assert_eq!(group_chrome(1_234_567), "1,234,567");
        assert_eq!(group_chrome(usize::MAX), "18,446,744,073,709,551,615");
    }

    #[test]
    fn glob_matching() {
        assert!(Glob::new("year").matches("year"));
        assert!(!Glob::new("year").matches("years"));
        assert!(Glob::new("*_id").matches("sample_id"));
        assert!(Glob::new("*_id").matches("_id"));
        assert!(!Glob::new("*_id").matches("id_sample"));
        assert!(Glob::new("chrom*").matches("chromStart"));
        assert!(Glob::new("*").matches("anything"));
        assert!(Glob::new("c?rom").matches("chrom"));
        assert!(!Glob::new("c?rom").matches("chhrom"));
        assert!(Glob::new("a*b*c").matches("axxbyyc"));
        assert!(!Glob::new("a*b*c").matches("axxbyy"));

        // Wildcard characters appearing literally in the *name*. Found by the
        // `glob_match` fuzz target: the matcher compared for equality before testing
        // for `*`, so a `*` in the name consumed the pattern's wildcard and stopped it
        // wildcarding anything further.
        assert!(Glob::new("*").matches("*]"));
        assert!(Glob::new("*").matches("a*b"));
        assert!(Glob::new("*").matches("?"));
        assert!(Glob::new("a*c").matches("a*c"));
        assert!(Glob::new("a*c").matches("a*x*c"));
        assert!(Glob::new("?").matches("*"));
    }

    #[test]
    fn settings_resolve_per_column() {
        let settings = NumberFormatSettings {
            format: thousands(),
            enabled: true,
            exclude: vec![Glob::new("*_id"), Glob::new("year")],
            align_numeric_right: true,
        };
        // Numeric column: formatted.
        assert!(!settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough());
        // Non-numeric: never formatted.
        assert!(settings
            .formatter_for("chrom", &DataType::String)
            .is_passthrough());
        assert!(settings
            .formatter_for("when", &DataType::Date)
            .is_passthrough());
        // Excluded by glob.
        assert!(settings
            .formatter_for("sample_id", &DataType::Int64)
            .is_passthrough());
        assert!(settings
            .formatter_for("year", &DataType::Int32)
            .is_passthrough());
    }

    #[test]
    fn settings_disabled_is_all_passthrough() {
        let settings = NumberFormatSettings {
            format: thousands(),
            enabled: false,
            ..Default::default()
        };
        assert!(settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough());
    }

    #[test]
    fn plain_format_is_always_passthrough() {
        let settings = NumberFormatSettings::default();
        assert!(settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough());
    }

    #[test]
    fn floats_flag_disables_grouping_for_floats_only() {
        let settings = NumberFormatSettings {
            format: NumberFormat {
                floats: false,
                ..thousands()
            },
            ..Default::default()
        };
        assert!(!settings
            .formatter_for("count", &DataType::Int64)
            .is_passthrough());
        assert!(settings
            .formatter_for("ratio", &DataType::Float64)
            .is_passthrough());
    }

    #[test]
    fn any_value_formatting_and_width_agree() {
        let fmt = CellFormatter::Number(thousands());
        let mut scratch = String::new();
        let cases: Vec<AnyValue> = vec![
            AnyValue::Int32(1234567),
            AnyValue::Int64(-9876543),
            AnyValue::UInt32(4000000),
            AnyValue::UInt64(u64::MAX),
            AnyValue::Int8(-12),
        ];
        for v in cases {
            let s = format_any_value(&fmt, &v, &mut scratch).into_owned();
            assert_eq!(
                display_width(&fmt, &v, &mut scratch),
                s.chars().count(),
                "width mismatch for {v:?} -> {s}"
            );
        }
        assert_eq!(
            format_any_value(&fmt, &AnyValue::Int32(1234567), &mut scratch),
            "1,234,567"
        );
    }

    #[test]
    fn nulls_and_strings_are_untouched() {
        let fmt = CellFormatter::Number(thousands());
        let mut scratch = String::new();
        assert_eq!(format_any_value(&fmt, &AnyValue::Null, &mut scratch), "");
        // A string value in a "numeric" formatter still passes through.
        assert_eq!(
            format_any_value(&fmt, &AnyValue::String("chr1"), &mut scratch),
            "chr1"
        );
        assert_eq!(
            format_any_value(
                &CellFormatter::Passthrough,
                &AnyValue::Int64(1234567),
                &mut scratch
            ),
            "1234567"
        );
    }

    /// The CLI crate duplicates the preset list to give clap `--help` output and
    /// completion, since it cannot depend on this crate. Catch drift here.
    #[test]
    fn number_format_values_match_presets() {
        let mut cli: Vec<&str> = datui_cli::NUMBER_FORMAT_VALUES.to_vec();
        let mut expected: Vec<&str> = NumberFormat::PRESET_NAMES.to_vec();
        // "system" is CLI/config-only: it resolves to a preset, it is not one.
        expected.push("system");
        cli.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            cli, expected,
            "datui_cli::NUMBER_FORMAT_VALUES is out of sync with NumberFormat::PRESET_NAMES"
        );
    }

    #[test]
    fn locale_tags_map_to_presets() {
        assert_eq!(preset_for_locale_tag("en_US.UTF-8"), "thousands");
        assert_eq!(preset_for_locale_tag("de_DE.UTF-8"), "european");
        assert_eq!(preset_for_locale_tag("de_DE.UTF-8@euro"), "european");
        assert_eq!(preset_for_locale_tag("fr_FR"), "si");
        assert_eq!(preset_for_locale_tag("hi_IN"), "indian");
        assert_eq!(preset_for_locale_tag("de_CH"), "swiss");
        assert_eq!(preset_for_locale_tag("it-CH"), "swiss");
        assert_eq!(preset_for_locale_tag("ja_JP"), "thousands");
        // Unknown tags fall back rather than failing.
        assert_eq!(preset_for_locale_tag("xx_YY"), "thousands");
    }
}

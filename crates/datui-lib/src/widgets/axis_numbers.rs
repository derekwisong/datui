//! How a chart writes its numbers: the format an axis takes from its column, the one
//! notation and precision every tick of an axis shares, a bar's label and a readout's
//! value, all through one writer.

use crate::numfmt::{CellFormatter, NumberFormat, NumberFormatSettings};
use polars::prelude::{AnyValue, DataType, Schema};

/// Decimal places past which an axis writes its numbers in scientific notation.
const MAX_AXIS_PLACES: i32 = 6;
/// The most decimals a scientific mantissa takes to tell ticks apart.
const MAX_MANTISSA_PLACES: i32 = 12;
/// Magnitude from which an axis writes its numbers in scientific notation.
const SCIENTIFIC_FROM: f64 = 1e15;

/// What a numeric axis holds: the table's format for its numbers, and whether they are
/// whole (counts, an integer column), which ticks the axis only at whole numbers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AxisNumbers {
    pub format: NumberFormat,
    pub whole: bool,
}

impl AxisNumbers {
    /// `column`'s numbers as the table prints them; plain when the schema lacks it.
    pub fn column(settings: &NumberFormatSettings, schema: Option<&Schema>, column: &str) -> Self {
        match schema.and_then(|s| s.get(column)) {
            Some(dtype) => Self {
                format: table_number_format(settings, column, dtype),
                whole: dtype.is_integer(),
            },
            None => Self::default(),
        }
    }

    /// Several columns on one axis: the first one's format, whole when every one is.
    pub fn columns(
        settings: &NumberFormatSettings,
        schema: Option<&Schema>,
        columns: &[String],
    ) -> Self {
        let mut each = columns.iter().map(|c| Self::column(settings, schema, c));
        let Some(first) = each.next() else {
            return Self::default();
        };
        let whole = first.whole && each.all(|n| n.whole);
        Self { whole, ..first }
    }

    /// Counts, as the table prints a count.
    pub fn count(settings: &NumberFormatSettings) -> Self {
        Self {
            format: table_number_format(settings, "Count", &DataType::UInt64),
            whole: true,
        }
    }

    /// A measure of the data such as a density, as the table prints a float.
    pub fn measure(settings: &NumberFormatSettings, name: &str) -> Self {
        Self {
            format: table_number_format(settings, name, &DataType::Float64),
            whole: false,
        }
    }

    /// `v` as the table writes the column, for a readout.
    pub fn write(&self, v: f64) -> String {
        write(&self.format, v, Notation::Table, 0.0)
    }

    /// The same numbers, ticked anywhere: on a log scale, or spread by a density.
    pub fn fractional(self) -> Self {
        Self {
            whole: false,
            ..self
        }
    }
}

/// How every tick of one numeric axis is written: one notation and one precision for
/// all of them, chosen from the ticks, in the table's grouping and decimal separator.
/// Chosen per tick, an axis switched to scientific notation partway up.
#[derive(Clone, Debug)]
pub struct AxisFormat {
    format: NumberFormat,
    full: Notation,
    /// The shorter form a narrow axis steps down to, when there is one.
    short: Option<Notation>,
    /// Below this a tick is zero: a stepped tick lands a hair off it, which
    /// scientific notation would print as `1.32e-24`.
    zero_below: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Notation {
    /// The value in `unit`s to `places` decimals, then `suffix`: `1,234.5`, `12.3k`.
    Fixed {
        places: usize,
        unit: f64,
        suffix: &'static str,
    },
    /// The mantissa to `places` decimals: `1.23e-5`.
    Scientific { places: usize },
    /// Each value in the largest of k, M, G and T it reaches, to the fewest places
    /// up to two that write it exactly: `500`, `2k`, `10M`. A log axis's short form,
    /// whose ticks run across many powers of ten.
    Prefixed,
    /// As the table writes the column: its precision, or as Polars writes a float
    /// (`19.434783`), never every digit an aggregate's division left.
    Table,
}

impl AxisFormat {
    /// The format for an axis ticked at `ticks`, in order, holding `numbers`.
    pub fn new(ticks: &[f64], numbers: &AxisNumbers) -> Self {
        let ticks: Vec<f64> = ticks.iter().copied().filter(|v| v.is_finite()).collect();
        let top = ticks.iter().fold(0.0_f64, |top, v| top.max(v.abs()));
        // The closest two ticks, which the labels must still tell apart.
        let gap = ticks
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .filter(|gap| *gap > 0.0)
            .fold(f64::INFINITY, f64::min);
        let places = if top >= SCIENTIFIC_FROM {
            None
        } else if numbers.whole {
            Some(0)
        } else {
            fixed_places(&ticks, top, gap)
        };
        let (full, short) = match places {
            Some(places) => (
                Notation::Fixed {
                    places,
                    unit: 1.0,
                    suffix: "",
                },
                short_notation(&ticks, top, gap),
            ),
            None => {
                // Every mantissa to the places that tell the closest ticks apart.
                let apart = if gap.is_finite() && top > 0.0 {
                    (magnitude(top) - magnitude(gap)).clamp(0, MAX_MANTISSA_PLACES) as usize
                } else {
                    0
                };
                (
                    Notation::Scientific {
                        places: apart.max(2),
                    },
                    Some(Notation::Scientific { places: apart }),
                )
            }
        };
        Self {
            format: numbers.format.clone(),
            full,
            short,
            zero_below: if gap.is_finite() { gap * 1e-9 } else { 0.0 },
        }
    }

    /// The format for a log axis ticked at `ticks`, values before the log: places
    /// enough to write every tick exactly, the 0.1 and 0.25 of a short axis included,
    /// rather than enough to tell the closest two apart, which on a log axis are the
    /// small ones. A 1, 2 or 5 a power of ten up reads `1e18`; the short form names
    /// each tick's own k, M, G or T: `1  10  100  1k  10k`.
    pub fn log(ticks: &[f64], numbers: &AxisNumbers) -> Self {
        let ticks: Vec<f64> = ticks.iter().copied().filter(|v| v.is_finite()).collect();
        let top = ticks.iter().fold(0.0_f64, |top, v| top.max(v.abs()));
        let full = if top >= SCIENTIFIC_FROM {
            Notation::Scientific { places: 0 }
        } else {
            Notation::Fixed {
                places: fewest_places(&ticks, 1.0, 0, MAX_AXIS_PLACES),
                unit: 1.0,
                suffix: "",
            }
        };
        Self {
            format: numbers.format.clone(),
            full,
            short: (1e3..SCIENTIFIC_FROM)
                .contains(&top)
                .then_some(Notation::Prefixed),
            zero_below: 0.0,
        }
    }

    /// The format for an axis from `lo` to `hi` ticked at its ends and halfway, as
    /// [`crate::widgets::axes::AxisSpec::ends_and_middle`] ticks it.
    pub fn ends_and_middle([lo, hi]: [f64; 2], numbers: &AxisNumbers) -> Self {
        Self::new(&[lo, (lo + hi) / 2.0, hi], numbers)
    }

    /// A tick at `level` of detail: 0 the full form, 1 the short one, `None` past the
    /// shortest.
    pub fn label(&self, v: f64, level: usize) -> Option<String> {
        let notation = match level {
            0 => self.full,
            1 => self.short?,
            _ => return None,
        };
        Some(write(&self.format, v, notation, self.zero_below))
    }
}

/// The one writer of a chart's numbers: `v` in `notation`, with `format`'s grouping
/// and decimal separator. Below `zero_below` a scientific tick is zero.
fn write(format: &NumberFormat, v: f64, notation: Notation, zero_below: f64) -> String {
    let (places, unit, suffix) = match notation {
        Notation::Scientific { places } => {
            let v = if v.abs() < zero_below { 0.0 } else { v };
            return scientific(v, places, format.decimal_sep);
        }
        Notation::Table => {
            let mut out = String::new();
            if v.fract() == 0.0 || format.float_precision.is_some() || !v.is_finite() {
                format.write_f64(v, &mut String::new(), &mut out);
            } else {
                format.regroup_decimal(&AnyValue::Float64(v).str_value(), &mut out);
            }
            return out;
        }
        Notation::Fixed { .. } | Notation::Prefixed if !v.is_finite() => {
            return v.to_string();
        }
        Notation::Prefixed => {
            let (unit, suffix) = [(1e12, "T"), (1e9, "G"), (1e6, "M"), (1e3, "k")]
                .into_iter()
                .find(|(unit, _)| v.abs() >= *unit)
                .unwrap_or((1.0, ""));
            let places = fewest_places(&[v], unit, 0, 2);
            (places, unit, suffix)
        }
        Notation::Fixed {
            places,
            unit,
            suffix,
        } => (places, unit, suffix),
    };
    let fixed = NumberFormat {
        float_precision: Some(places as u8),
        ..format.clone()
    };
    let mut out = String::new();
    fixed.write_f64(v / unit, &mut String::new(), &mut out);
    // A value that rounds to zero is zero: no sign, and no unit to count it in.
    // Checked on the text, since formatting rounds -0.5 to `-0` and `round` to -1.
    if !out.chars().any(|c| matches!(c, '1'..='9')) {
        if unit > 1.0 {
            return "0".to_string();
        }
        out.retain(|c| c != '-');
    }
    out.push_str(suffix);
    out
}

/// Decimal places for `ticks`, whose largest is `top` and closest two `gap` apart:
/// three significant figures of the largest, and enough to tell the closest apart.
/// Fewer when they write every tick exactly, so round ticks read `20`, not `20.0`.
/// `None` for numbers too small to write that way, or ticks too close.
fn fixed_places(ticks: &[f64], top: f64, gap: f64) -> Option<usize> {
    let figures = if top > 0.0 { 2 - magnitude(top) } else { 0 };
    let apart = if gap.is_finite() { -magnitude(gap) } else { 0 };
    let places = figures.max(apart).max(0);
    (places <= MAX_AXIS_PLACES).then(|| fewest_places(ticks, 1.0, apart.max(0), places))
}

/// The fewest places from `least` to `most` that write every one of `ticks`, counted
/// in `unit`s, exactly; `most` when none do.
fn fewest_places(ticks: &[f64], unit: f64, least: i32, most: i32) -> usize {
    let exact = |places: i32| {
        ticks.iter().all(|v| {
            let scaled = v / unit * 10f64.powi(places);
            // Ticks are stepped in floating point: 0.1 * 3 is 0.30000000000000004.
            (scaled - scaled.round()).abs() <= 1e-9 * scaled.abs().max(1.0)
        })
    };
    (least..most).find(|&p| exact(p)).unwrap_or(most) as usize
}

/// The short form of `ticks`, whose largest is `top`: counted in the k, M, G or T of
/// the largest, to two significant figures of it and places enough to tell ticks
/// `gap` apart, at most two, or fewer as [`fixed_places`] takes them. `None` below a
/// thousand, where there is no shorter form.
fn short_notation(ticks: &[f64], top: f64, gap: f64) -> Option<Notation> {
    let (unit, suffix) = [(1e12, "T"), (1e9, "G"), (1e6, "M"), (1e3, "k")]
        .into_iter()
        .find(|(unit, _)| top >= *unit)?;
    let figures = 1 - magnitude(top / unit);
    let apart = if gap.is_finite() {
        -magnitude(gap / unit)
    } else {
        0
    };
    let most = figures.max(apart).clamp(0, 2);
    Some(Notation::Fixed {
        places: fewest_places(ticks, unit, apart.clamp(0, most), most),
        unit,
        suffix,
    })
}

/// The power of ten `v` is counted in: 1 for 12.3, -2 for 0.05. A hair under a power
/// of ten counts as it, since ticks come of floating-point arithmetic: 1000.3 less
/// 1000.2 is 0.09999... and not a tenth.
fn magnitude(v: f64) -> i32 {
    (v.log10() + 1e-9).floor() as i32
}

/// `v` in scientific notation, its mantissa to `places` decimals.
fn scientific(v: f64, places: usize, decimal_sep: char) -> String {
    // No `-0.00e0`.
    let v = if v == 0.0 { 0.0 } else { v };
    let text = format!("{v:.places$e}");
    if decimal_sep == '.' {
        text
    } else {
        text.replacen('.', decimal_sep.encode_utf8(&mut [0; 4]), 1)
    }
}

/// The format the table prints `column` in, plain where it prints it unformatted.
pub fn table_number_format(
    settings: &NumberFormatSettings,
    column: &str,
    dtype: &DataType,
) -> NumberFormat {
    match settings.formatter_for(column, dtype) {
        CellFormatter::Number(format) => format,
        CellFormatter::Passthrough => NumberFormat::PLAIN,
    }
}

/// Format a bar's value for the label beside it in `format`: an integer column's whole,
/// anything else to the format's decimal places or two, so every bar shows the same
/// number of them. Values too large, or too small to show in those places, go to
/// scientific notation.
pub fn format_bar_value(v: f64, integer: bool, format: &NumberFormat) -> String {
    let places = format.float_precision.unwrap_or(2);
    let smallest = 0.5 * 10f64.powi(-i32::from(places));
    let notation =
        if !v.is_finite() || v.abs() >= 1e15 || (!integer && v != 0.0 && v.abs() < smallest) {
            Notation::Scientific { places: 2 }
        } else {
            Notation::Fixed {
                places: if integer { 0 } else { places.into() },
                unit: 1.0,
                suffix: "",
            }
        };
    write(format, v, notation, 0.0)
}

#[cfg(test)]
mod tests;

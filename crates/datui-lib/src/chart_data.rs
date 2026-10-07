//! Prepare chart data from a LazyFrame: read the chart's columns, then turn them into
//! points, bins or statistics.
//!
//! Every chart reads its rows through [`read_columns`]: up to a row limit of them,
//! spread across the table by the sampler the analysis tools use, so a chart shows the
//! table and not its first rows. What was read comes back as [`RowsRead`], so the chart
//! can say when it shows a sample.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use color_eyre::Result;
use polars::chunked_array::cast::CastOptions;
use polars::datatypes::{DataType, TimeUnit};
use polars::prelude::*;
use std::f64::consts::PI;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Describes how x-axis numeric values map to temporal types for label formatting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum XAxisTemporalKind {
    #[default]
    Numeric,
    Date,       // x = days since Unix epoch (f64)
    DatetimeUs, // x = microseconds since epoch
    DatetimeMs,
    DatetimeNs,
    Time, // x = nanoseconds since midnight
}

fn x_axis_temporal_kind(dtype: &DataType) -> XAxisTemporalKind {
    match dtype {
        DataType::Date => XAxisTemporalKind::Date,
        DataType::Datetime(unit, _) => match unit {
            TimeUnit::Nanoseconds => XAxisTemporalKind::DatetimeNs,
            TimeUnit::Microseconds => XAxisTemporalKind::DatetimeUs,
            TimeUnit::Milliseconds => XAxisTemporalKind::DatetimeMs,
        },
        DataType::Time => XAxisTemporalKind::Time,
        _ => XAxisTemporalKind::Numeric,
    }
}

/// Returns the x-axis temporal kind for a column from the schema (for axis label formatting when no data is loaded yet).
pub fn x_axis_temporal_kind_for_column(schema: &Schema, x_column: &str) -> XAxisTemporalKind {
    schema
        .get(x_column)
        .map(x_axis_temporal_kind)
        .unwrap_or(XAxisTemporalKind::Numeric)
}

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
    pub format: crate::numfmt::NumberFormat,
    pub whole: bool,
}

impl AxisNumbers {
    /// `column`'s numbers as the table prints them; plain when the schema lacks it.
    pub fn column(
        settings: &crate::numfmt::NumberFormatSettings,
        schema: Option<&Schema>,
        column: &str,
    ) -> Self {
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
        settings: &crate::numfmt::NumberFormatSettings,
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
    pub fn count(settings: &crate::numfmt::NumberFormatSettings) -> Self {
        Self {
            format: table_number_format(settings, "Count", &DataType::UInt64),
            whole: true,
        }
    }

    /// A measure of the data such as a density, as the table prints a float.
    pub fn measure(settings: &crate::numfmt::NumberFormatSettings, name: &str) -> Self {
        Self {
            format: table_number_format(settings, name, &DataType::Float64),
            whole: false,
        }
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
    format: crate::numfmt::NumberFormat,
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
        Some(self.write(v, notation))
    }

    fn write(&self, v: f64, notation: Notation) -> String {
        let (places, unit, suffix) = match notation {
            Notation::Scientific { places } => {
                let v = if v.abs() < self.zero_below { 0.0 } else { v };
                return scientific(v, places, self.format.decimal_sep);
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
        let fixed = crate::numfmt::NumberFormat {
            float_precision: Some(places as u8),
            ..self.format.clone()
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
    settings: &crate::numfmt::NumberFormatSettings,
    column: &str,
    dtype: &DataType,
) -> crate::numfmt::NumberFormat {
    match settings.formatter_for(column, dtype) {
        crate::numfmt::CellFormatter::Number(format) => format,
        crate::numfmt::CellFormatter::Passthrough => crate::numfmt::NumberFormat::PLAIN,
    }
}

/// An x value as the date and time it stands for, when `kind` is a date or datetime.
pub(crate) fn x_datetime(v: f64, kind: XAxisTemporalKind) -> Option<NaiveDateTime> {
    const UNIX_EPOCH_CE_DAYS: i32 = 719_163;
    match kind {
        XAxisTemporalKind::Date => NaiveDate::from_num_days_from_ce_opt(
            UNIX_EPOCH_CE_DAYS.saturating_add(v.trunc() as i32),
        )
        .map(|d| d.and_time(NaiveTime::MIN)),
        XAxisTemporalKind::DatetimeUs => {
            DateTime::from_timestamp_micros(v.trunc() as i64).map(|dt| dt.naive_utc())
        }
        XAxisTemporalKind::DatetimeMs => {
            DateTime::from_timestamp_millis(v.trunc() as i64).map(|dt| dt.naive_utc())
        }
        XAxisTemporalKind::DatetimeNs => {
            DateTime::from_timestamp_millis((v.trunc() as i64) / 1_000_000).map(|dt| dt.naive_utc())
        }
        XAxisTemporalKind::Numeric | XAxisTemporalKind::Time => None,
    }
}

/// An x value as a time of day, when `kind` is a time.
pub(crate) fn x_time(v: f64) -> Option<NaiveTime> {
    let nsecs = v.trunc() as u64;
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nsecs / 1_000_000_000) as u32,
        (nsecs % 1_000_000_000) as u32,
    )
}

/// An x tick at `level` of detail, 0 the fullest, or `None` past the shortest form.
/// A narrow axis steps down until its labels fit: a date to year-month and then the
/// year, or to month-day when both ends of the axis, `bounds`, fall in one year; a
/// datetime first to its date, or to the minute when the axis spans one day; a time
/// to the minute. A number, or a time past what it can stand for, is written in
/// `numbers`.
pub fn x_axis_label_at(
    v: f64,
    kind: XAxisTemporalKind,
    bounds: (f64, f64),
    level: usize,
    numbers: &AxisFormat,
) -> Option<String> {
    if kind == XAxisTemporalKind::Numeric {
        return numbers.label(v, level);
    }
    if kind == XAxisTemporalKind::Time {
        let pattern = ["%H:%M:%S", "%H:%M"].get(level)?;
        return Some(match x_time(v) {
            Some(t) => t.format(pattern).to_string(),
            None => numbers.label(v, level)?,
        });
    }
    let Some(at) = x_datetime(v, kind) else {
        return numbers.label(v, level);
    };
    let ends = x_datetime(bounds.0, kind).zip(x_datetime(bounds.1, kind));
    let one_day = ends.is_some_and(|(a, b)| a.date() == b.date());
    let one_year = ends.is_some_and(|(a, b)| a.year() == b.year());
    let dates: &[&str] = if one_year {
        &["%Y-%m-%d", "%m-%d"]
    } else {
        &["%Y-%m-%d", "%Y-%m", "%Y"]
    };
    let patterns: Vec<&str> = if kind == XAxisTemporalKind::Date {
        dates.to_vec()
    } else if one_day {
        vec!["%Y-%m-%d %H:%M", "%H:%M"]
    } else {
        std::iter::once("%Y-%m-%d %H:%M")
            .chain(dates.iter().copied())
            .collect()
    };
    patterns.get(level).map(|p| at.format(p).to_string())
}

/// How a chart reads its rows.
#[derive(Clone, Debug)]
pub struct ChartSampling {
    /// Rows to read; `None` reads every row.
    pub limit: Option<usize>,
    /// The view's row count when the table already knows it, which saves a count.
    pub known_total: Option<usize>,
    /// The shared analysis seed, so a chart and Describe draw alike.
    pub seed: u64,
    pub streaming: bool,
    /// Whether the view may be read whole, twice, for a line's envelope: not a scan
    /// of an object store in place, where the sample reads a few row groups and the
    /// envelope would download everything twice. See [`prepare_chart_data`].
    pub full_passes: bool,
    /// The rows already read from this view.
    pub held: HeldRows,
    /// Set once nobody wants the result: a streamed count stops at its next batch.
    pub cancel: Arc<AtomicBool>,
}

impl ChartSampling {
    /// Up to `limit` rows, with the analysis tools' default seed.
    pub fn rows(limit: Option<usize>) -> Self {
        Self {
            limit,
            known_total: None,
            seed: crate::sampling::Sample::default().seed,
            streaming: false,
            full_passes: true,
            held: HeldRows::default(),
            cancel: Arc::default(),
        }
    }
}

/// The rows a chart last read from one view. Another bin count, range, bandwidth or
/// chart over columns already read draws from them instead of reading the table
/// again, and every chart of the view describes the same sample. Shared with the
/// worker that reads; whoever owns the view starts a new one when the view changes.
#[derive(Clone, Default)]
pub struct HeldRows(Arc<Mutex<Holding>>);

#[derive(Default)]
struct Holding {
    rows: Option<Held>,
    /// Rows per category, from a count of the whole view: exact whatever the sample
    /// size, so another order or size draws from them rather than counting again.
    counts: Vec<HeldCounts>,
}

struct Held {
    limit: Option<usize>,
    seed: u64,
    df: DataFrame,
    rows: RowsRead,
}

struct HeldCounts {
    category: String,
    counted: Counted,
}

impl std::fmt::Debug for HeldRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeldRows")
    }
}

/// What a chart read: the rows the table has, and how many of them were sampled when
/// that was fewer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowsRead {
    pub total_rows: usize,
    pub sample_size: Option<usize>,
    /// A line chart over more rows than its sample size draws each of this many steps
    /// along X as its lowest and highest value instead of sampling: see
    /// [`prepare_chart_data`].
    pub envelope_steps: Option<usize>,
    /// The seed the sample was drawn with, when it is a sample.
    pub seed: Option<u64>,
}

/// Which values a histogram, box plot or KDE draws. Outliers far from the body squash
/// it into a bin or two; a percentile range leaves them out and says how many.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueRange {
    #[default]
    All,
    /// The 1st to the 99th percentile.
    Percentile1To99,
}

impl ValueRange {
    pub const ALL: [Self; 2] = [Self::All, Self::Percentile1To99];

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Percentile1To99 => "p1-p99",
        }
    }

    fn quantiles(self) -> Option<(f64, f64)> {
        match self {
            Self::All => None,
            Self::Percentile1To99 => Some((0.01, 0.99)),
        }
    }
}

/// A range that left values out: which, and how many.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clipped {
    pub range: ValueRange,
    pub outside: usize,
}

/// What a chart says under the plot about its input, one line each: that it is a
/// sample, with the seed that draws it again, and how many values a range left out.
/// Empty when it shows every row and every value. `middot` joins the seed on.
pub fn chart_notes(rows: &RowsRead, clipped: Option<&Clipped>, middot: &str) -> Vec<String> {
    let mut notes = Vec::new();
    if let Some(steps) = rows.envelope_steps {
        notes.push(format!(
            "min and max of {} rows in {} steps",
            crate::discover::format_rows(rows.total_rows),
            crate::numfmt::group_chrome(steps)
        ));
    }
    if let Some(n) = rows.sample_size {
        let mut note = format!(
            "sample of {} of {} rows",
            crate::numfmt::group_chrome(n),
            crate::discover::format_rows(rows.total_rows)
        );
        if let Some(seed) = rows.seed {
            note.push_str(&format!(" {middot} seed {seed}"));
        }
        notes.push(note);
    }
    if let Some(clipped) = clipped {
        let noun = if clipped.outside == 1 {
            "value"
        } else {
            "values"
        };
        notes.push(format!(
            "{} {noun} outside {}",
            crate::numfmt::group_chrome(clipped.outside),
            clipped.range.label()
        ));
    }
    notes
}

/// Read `columns` (each once, however often named) through the analysis sampler: every
/// row up to the limit, and past it a seeded sample spread across the table — runs of
/// one Parquet or IPC file, or one streamed pass over anything else — never its head.
///
/// Rows already held for the same size and seed are used as they are when they have
/// the columns. Otherwise the read takes the held columns along, so going back to one
/// does not read again.
fn read_columns(
    lf: &LazyFrame,
    columns: &[&str],
    sampling: &ChartSampling,
) -> Result<(DataFrame, RowsRead)> {
    let mut unique: Vec<PlSmallStr> = Vec::with_capacity(columns.len());
    for c in columns {
        if !unique.iter().any(|u| u == c) {
            unique.push((*c).into());
        }
    }
    let mut holding = sampling.held.0.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(h) = holding
        .rows
        .as_ref()
        .filter(|h| h.limit == sampling.limit && h.seed == sampling.seed)
    {
        if unique.iter().all(|c| h.df.column(c).is_ok()) {
            return Ok((h.df.select(unique.iter().cloned())?, h.rows));
        }
        for c in h.df.get_column_names() {
            if !unique.contains(c) {
                unique.push(c.clone());
            }
        }
    }
    let lf = lf
        .clone()
        .select(unique.iter().map(|c| col(c.clone())).collect::<Vec<_>>());
    let read = crate::sampling::analysis_rows(
        &lf,
        sampling.limit,
        sampling.known_total,
        sampling.seed,
        sampling.streaming,
    )?;
    let rows = RowsRead {
        total_rows: read.total_rows,
        sample_size: read.sample_size,
        envelope_steps: None,
        seed: read.sample_size.map(|_| sampling.seed),
    };
    holding.rows = Some(Held {
        limit: sampling.limit,
        seed: sampling.seed,
        df: read.df.clone(),
        rows,
    });
    Ok((read.df, rows))
}

/// A column's values as `f64`, one per row; null, NaN and infinities are `None`.
fn f64_values(df: &DataFrame, column: &str) -> Result<Vec<Option<f64>>> {
    let cast = df.column(column)?.cast(&DataType::Float64)?;
    Ok(cast
        .f64()?
        .iter()
        .map(|v| v.filter(|v| v.is_finite()))
        .collect())
}

/// X as `f64`, one per row: numbers as they are, temporal types as their ordinal (see
/// [`XAxisTemporalKind`]).
fn x_values(df: &DataFrame, column: &str, dtype: &DataType) -> Result<Vec<Option<f64>>> {
    match dtype {
        DataType::Datetime(_, _) | DataType::Date | DataType::Time => {
            let ordinal = df.column(column)?.cast(&DataType::Int64)?;
            Ok(ordinal.i64()?.iter().map(|v| v.map(|v| v as f64)).collect())
        }
        _ => f64_values(df, column),
    }
}

/// Result of loading only the x column: min/max for axis bounds and temporal kind.
pub struct ChartXRangeResult {
    pub x_min: f64,
    pub x_max: f64,
    pub x_axis_kind: XAxisTemporalKind,
    pub rows: RowsRead,
}

/// Loads only the x column and returns its min/max (for axis display when no y is selected).
pub fn prepare_chart_x_range(
    lf: &LazyFrame,
    schema: &Schema,
    x_column: &str,
    sampling: &ChartSampling,
) -> Result<ChartXRangeResult> {
    let x_dtype = schema
        .get(x_column)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", x_column))?;
    let x_axis_kind = x_axis_temporal_kind(x_dtype);
    let (df, rows) = read_columns(lf, &[x_column], sampling)?;
    let (x_min, x_max) = x_values(&df, x_column, x_dtype)?
        .into_iter()
        .flatten()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), x| {
            (lo.min(x), hi.max(x))
        });
    let (x_min, x_max) = if x_max >= x_min {
        (x_min, x_max)
    } else {
        (0.0, 1.0)
    };
    Ok(ChartXRangeResult {
        x_min,
        x_max,
        x_axis_kind,
        rows,
    })
}

/// Result of preparing chart data: series points and x-axis kind for label formatting.
pub struct ChartDataResult {
    /// One per y column, in X order.
    pub series: Vec<Vec<(f64, f64)>>,
    /// Per series, the indices of `series` where a line starts again after a gap: a row
    /// whose y is null is left out of that series alone, and a line does not bridge it.
    pub breaks: Vec<Vec<usize>>,
    pub x_axis_kind: XAxisTemporalKind,
    pub rows: RowsRead,
}

/// A series split at its breaks: the runs a line joins.
pub fn segments<'a>(points: &'a [(f64, f64)], breaks: &[usize]) -> Vec<&'a [(f64, f64)]> {
    let mut out = Vec::with_capacity(breaks.len() + 1);
    let mut start = 0;
    for &b in breaks {
        if b > start && b <= points.len() {
            out.push(&points[start..b]);
            start = b;
        }
    }
    if start < points.len() {
        out.push(&points[start..]);
    }
    out
}

/// Histogram bin (center and count).
#[derive(Clone, Debug)]
pub struct HistogramBin {
    pub center: f64,
    pub count: f64,
}

/// One group's bins of a histogram split by a color: its count (or share) per bin,
/// on the bins of the whole.
#[derive(Clone, Debug)]
pub struct HistogramGroup {
    pub name: String,
    pub counts: Vec<f64>,
}

/// Histogram data for a single column.
#[derive(Clone, Debug)]
pub struct HistogramData {
    pub column: String,
    /// Every row's bins; with `share`, each bin's share of the rows.
    pub bins: Vec<HistogramBin>,
    /// Per color group, on the same bins: drawn as outlines over one another.
    pub groups: Vec<HistogramGroup>,
    /// The last group is Other: every value of the color without a group of its own.
    pub other: bool,
    /// Each bin is a share of its group's rows (or of all rows), not a count.
    pub share: bool,
    pub x_min: f64,
    pub x_max: f64,
    pub max_count: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
}

/// KDE series and bounds.
#[derive(Clone, Debug)]
pub struct KdeSeries {
    pub name: String,
    pub points: Vec<(f64, f64)>,
}

#[derive(Clone, Debug)]
pub struct KdeData {
    pub series: Vec<KdeSeries>,
    /// The last series is Other: every value of the color without a series of its own.
    pub other: bool,
    pub x_min: f64,
    pub x_max: f64,
    pub y_max: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
}

/// Box plot stats for a column.
#[derive(Clone, Debug)]
pub struct BoxPlotStats {
    pub name: String,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
}

/// A box and its whiskers as line segments, the box `half` either side of `center`
/// and the caps `cap` either side; Y in data values, X in whatever unit `center` is.
pub struct BoxMarks {
    /// The box, corner to corner and back to the first.
    pub outline: [(f64, f64); 5],
    pub median: [(f64, f64); 2],
    /// Minimum to the first quartile, and the third quartile to the maximum.
    pub low: [(f64, f64); 2],
    pub high: [(f64, f64); 2],
    pub low_cap: [(f64, f64); 2],
    pub high_cap: [(f64, f64); 2],
}

impl BoxPlotStats {
    pub fn marks(&self, center: f64, half: f64, cap: f64) -> BoxMarks {
        let (left, right) = (center - half, center + half);
        BoxMarks {
            outline: [
                (left, self.q1),
                (right, self.q1),
                (right, self.q3),
                (left, self.q3),
                (left, self.q1),
            ],
            median: [(left, self.median), (right, self.median)],
            low: [(center, self.min), (center, self.q1)],
            high: [(center, self.q3), (center, self.max)],
            low_cap: [(center - cap, self.min), (center + cap, self.min)],
            high_cap: [(center - cap, self.max), (center + cap, self.max)],
        }
    }
}

#[derive(Clone, Debug)]
pub struct BoxPlotData {
    pub stats: Vec<BoxPlotStats>,
    pub y_min: f64,
    pub y_max: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
    /// One box per category: how many categories there are, of which the largest
    /// have a box. 0 for a box per column.
    pub of: usize,
}

impl HistogramData {
    /// Each group's bins as the outline of its bars: up the left edge of each bin,
    /// across its top, and down at the end.
    pub fn step_outlines(&self) -> Vec<Vec<(f64, f64)>> {
        let n = self.bins.len().max(1);
        let width = (self.x_max - self.x_min) / n as f64;
        self.groups
            .iter()
            .map(|group| {
                let mut points = vec![(self.x_min, 0.0)];
                for (i, &count) in group.counts.iter().enumerate() {
                    let x0 = self.x_min + i as f64 * width;
                    points.push((x0, count));
                    points.push((x0 + width, count));
                }
                points.push((self.x_max, 0.0));
                points
            })
            .collect()
    }
}

/// Where Other is among `n` series: the last, when there is one.
pub fn other_at(other: bool, n: usize) -> Option<usize> {
    (other && n > 0).then(|| n - 1)
}

/// The order `n` series are drawn in: Other first, under the series drawn over it.
pub fn drawing_order(n: usize, other: Option<usize>) -> impl Iterator<Item = usize> {
    other
        .filter(|&o| o < n)
        .into_iter()
        .chain((0..n).filter(move |&i| Some(i) != other))
}

/// Heatmap data for two numeric columns.
#[derive(Clone, Debug)]
pub struct HeatmapData {
    pub x_column: String,
    pub y_column: String,
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
    pub x_bins: usize,
    pub y_bins: usize,
    pub counts: Vec<Vec<f64>>,
    pub max_count: f64,
    pub rows: RowsRead,
}

/// Prepares XY series from the current LazyFrame. X is cast to f64 (temporal types as
/// ordinal). Nulls are dropped per series: a null X drops the row, a null Y drops that
/// series' point and breaks its line. Points come in X order, so a line runs left to
/// right whatever order the rows are in; ties keep table order.
///
/// With `envelope`, a view of more rows than the sample size is not sampled: X is cut
/// into half that many steps and each step draws its lowest and highest Y. A random
/// sample of a waveform or any long series joins points far apart and misses its peaks;
/// the envelope keeps every peak, as many steps as a plot has columns. It reads the
/// view twice, so only where [`ChartSampling::full_passes`] allows; both passes stop
/// when [`ChartSampling::cancel`] is set.
pub fn prepare_chart_data(
    lf: &LazyFrame,
    schema: &Schema,
    x_column: &str,
    y_columns: &[String],
    sampling: &ChartSampling,
    envelope: bool,
) -> Result<ChartDataResult> {
    if y_columns.is_empty() {
        return Ok(ChartDataResult {
            series: Vec::new(),
            breaks: Vec::new(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            rows: RowsRead::default(),
        });
    }

    let x_dtype = schema
        .get(x_column)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", x_column))?;
    let x_axis_kind = x_axis_temporal_kind(x_dtype);

    let mut counted = None;
    if envelope
        && sampling.full_passes
        && let Some(limit) = sampling.limit.filter(|&n| n > 0)
        && sampling.known_total.is_none_or(|n| n > limit)
    {
        match envelope_series(lf, x_column, x_dtype, y_columns, limit, sampling)? {
            Envelope::Drawn {
                series,
                breaks,
                rows,
                steps,
            } => {
                return Ok(ChartDataResult {
                    series,
                    breaks,
                    x_axis_kind,
                    rows: RowsRead {
                        total_rows: rows,
                        sample_size: None,
                        envelope_steps: Some(steps),
                        seed: None,
                    },
                });
            }
            // The first pass counted the view: the sample takes it whole.
            Envelope::Fits(rows) => counted = Some(rows),
        }
    }
    let counted_sampling;
    let sampling = match counted {
        Some(rows) => {
            counted_sampling = ChartSampling {
                known_total: Some(rows),
                ..sampling.clone()
            };
            &counted_sampling
        }
        None => sampling,
    };

    let mut columns = vec![x_column];
    columns.extend(y_columns.iter().map(String::as_str));
    let (df, rows) = read_columns(lf, &columns, sampling)?;

    let mut order: Vec<(f64, usize)> = x_values(&df, x_column, x_dtype)?
        .into_iter()
        .enumerate()
        .filter_map(|(i, x)| x.map(|x| (x, i)))
        .collect();
    // Stable, so rows sharing an X keep their table order.
    order.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut series = Vec::with_capacity(y_columns.len());
    let mut breaks = Vec::with_capacity(y_columns.len());
    for y_column in y_columns {
        let ys = f64_values(&df, y_column)?;
        let mut points = Vec::with_capacity(order.len());
        let mut starts = Vec::new();
        let mut gap = false;
        for &(x, i) in &order {
            match ys[i] {
                Some(y) => {
                    if gap && !points.is_empty() {
                        starts.push(points.len());
                    }
                    gap = false;
                    points.push((x, y));
                }
                None => gap = true,
            }
        }
        series.push(points);
        breaks.push(starts);
    }

    Ok(ChartDataResult {
        series,
        breaks,
        x_axis_kind,
        rows,
    })
}

/// What [`envelope_series`] found.
enum Envelope {
    /// Per Y column, its points and where its line breaks, from `rows` rows in
    /// `steps` steps.
    Drawn {
        series: Vec<Vec<(f64, f64)>>,
        breaks: Vec<Vec<usize>>,
        rows: usize,
        steps: usize,
    },
    /// The view is no more rows than the sample size, this many: no envelope.
    Fits(usize),
}

/// What a pass stopped by [`until_cancelled`] fails with.
const ENVELOPE_CANCELLED: &str = "chart cancelled";

/// `e`, failing the query once `cancel` is set: a streamed pass stops at its next
/// morsel rather than reading on for a chart nobody waits for.
fn until_cancelled(e: Expr, cancel: &Arc<AtomicBool>) -> Expr {
    let cancel = Arc::clone(cancel);
    e.map(
        move |c: Column| {
            polars_ensure!(!cancel.load(Ordering::Relaxed), ComputeError: ENVELOPE_CANCELLED);
            Ok(c)
        },
        |_, field| Ok(field.clone()),
    )
}

/// Collect a pass of the envelope, streamed whatever the setting: it holds a few
/// numbers per step. A pass stopped by `cancel` is an error that says so.
fn envelope_pass(lf: LazyFrame, cancel: &Arc<AtomicBool>) -> Result<DataFrame> {
    crate::statistics::collect_lazy(lf, true).map_err(|e| {
        if cancel.load(Ordering::Relaxed) {
            color_eyre::eyre::eyre!(ENVELOPE_CANCELLED)
        } else {
            e.into()
        }
    })
}

/// Per Y column, half `limit` steps along X, each its lowest and highest finite Y at
/// the step's lowest X, in X order. Two streamed passes over the view: the rows and
/// X's bounds, then one group per step. A row with no X is left out whole; a step
/// with rows where a series has no value breaks its line.
fn envelope_series(
    lf: &LazyFrame,
    x_column: &str,
    x_dtype: &DataType,
    y_columns: &[String],
    limit: usize,
    sampling: &ChartSampling,
) -> Result<Envelope> {
    let cancel = &sampling.cancel;
    // Temporal X as its ordinal, as `x_values` reads it.
    let x = match x_dtype {
        DataType::Datetime(_, _) | DataType::Date | DataType::Time | DataType::Duration(_) => {
            col(x_column).cast(DataType::Int64).cast(DataType::Float64)
        }
        _ => col(x_column).cast(DataType::Float64),
    };
    // Not finite is null, so the aggregations below are a plain min and max, which
    // stream; a filter inside them does not.
    let finite = |e: Expr| {
        when(e.clone().is_finite())
            .then(e)
            .otherwise(lit(NULL).cast(DataType::Float64))
    };
    let x = finite(until_cancelled(x, cancel)).alias("__x");
    let bounds = envelope_pass(
        lf.clone().select([
            len().alias("rows"),
            x.clone().min().alias("lo"),
            x.clone().max().alias("hi"),
        ]),
        cancel,
    )?;
    let rows = bounds
        .column("rows")?
        .cast(&DataType::UInt64)?
        .u64()?
        .get(0)
        .unwrap_or(0) as usize;
    if rows <= limit {
        return Ok(Envelope::Fits(rows));
    }
    let steps = (limit / 2).max(1);
    let n = y_columns.len();
    let drawn = |series, breaks| Envelope::Drawn {
        series,
        breaks,
        rows,
        steps,
    };
    let bound = |name: &str| -> Result<Option<f64>> { Ok(bounds.column(name)?.f64()?.get(0)) };
    let (Some(lo), Some(hi)) = (bound("lo")?, bound("hi")?) else {
        return Ok(drawn(vec![Vec::new(); n], vec![Vec::new(); n]));
    };
    let per_x = if hi > lo {
        steps as f64 / (hi - lo)
    } else {
        0.0
    };
    let lf = lf
        .clone()
        .select(
            std::iter::once(x)
                .chain(y_columns.iter().enumerate().map(|(i, y)| {
                    finite(col(y.as_str()).cast(DataType::Float64)).alias(format!("__y{i}"))
                }))
                .collect::<Vec<_>>(),
        )
        // A row filter, not a filter inside the select: that would leave X shorter than
        // the Y columns beside it.
        .filter(col("__x").is_not_null());
    let step = ((col("__x") - lit(lo)) * lit(per_x))
        .floor()
        .cast(DataType::Int64)
        .clip(lit(0i64), lit(steps as i64 - 1))
        .alias("__step");
    let mut aggs = vec![col("__x").min()];
    for i in 0..n {
        let y = col(format!("__y{i}"));
        aggs.push(y.clone().min().alias(format!("__lo{i}")));
        aggs.push(y.max().alias(format!("__hi{i}")));
    }
    let df = envelope_pass(
        lf.group_by([step])
            .agg(aggs)
            .sort(["__step"], Default::default()),
        cancel,
    )?;
    let xs = df.column("__x")?.f64()?.clone();
    let mut series = Vec::with_capacity(n);
    let mut breaks = Vec::with_capacity(n);
    for i in 0..n {
        let lows = df.column(&format!("__lo{i}"))?.f64()?.clone();
        let highs = df.column(&format!("__hi{i}"))?.f64()?.clone();
        let mut points = Vec::with_capacity(xs.len() * 2);
        let mut starts = Vec::new();
        let mut gap = false;
        for ((x, low), high) in xs.iter().zip(lows.iter()).zip(highs.iter()) {
            let (Some(x), Some(low), Some(high)) = (x, low, high) else {
                gap = true;
                continue;
            };
            if gap && !points.is_empty() {
                starts.push(points.len());
            }
            gap = false;
            points.push((x, low));
            if high != low {
                points.push((x, high));
            }
        }
        series.push(points);
        breaks.push(starts);
    }
    Ok(drawn(series, breaks))
}

/// Each column's finite values, read in one pass; nulls are dropped per column.
fn read_values(
    lf: &LazyFrame,
    columns: &[&str],
    sampling: &ChartSampling,
) -> Result<(Vec<Vec<f64>>, RowsRead)> {
    let (df, rows) = read_columns(lf, columns, sampling)?;
    let values = columns
        .iter()
        .map(|c| Ok(f64_values(&df, c)?.into_iter().flatten().collect()))
        .collect::<Result<Vec<Vec<f64>>>>()?;
    Ok((values, rows))
}

/// Sort `values` and keep those inside `range`; returns how many were left out.
fn sort_and_clip(values: &mut Vec<f64>, range: ValueRange) -> usize {
    values.sort_by(f64::total_cmp);
    let Some((low, high)) = range.quantiles() else {
        return 0;
    };
    if values.is_empty() {
        return 0;
    }
    let (low, high) = (quantile(values, low), quantile(values, high));
    let before = values.len();
    values.retain(|v| (low..=high).contains(v));
    before - values.len()
}

fn clipped(range: ValueRange, outside: usize) -> Option<Clipped> {
    (range != ValueRange::All).then_some(Clipped { range, outside })
}

/// Prepare histogram data for a numeric column.
pub fn prepare_histogram_data(
    lf: &LazyFrame,
    column: &str,
    bins: usize,
    range: ValueRange,
    sampling: &ChartSampling,
) -> Result<HistogramData> {
    prepare_histogram_by(lf, column, bins, range, false, None, sampling)
}

/// Prepare a histogram of `column`, split by `color` into groups on the same bins
/// when given. With `share`, a bin is its share of its group's rows (of every row,
/// unsplit), so groups of different sizes compare. The range is the whole
/// column's, so every group is clipped alike.
pub fn prepare_histogram_by(
    lf: &LazyFrame,
    column: &str,
    bins: usize,
    range: ValueRange,
    share: bool,
    color: Option<ColorSplit<'_>>,
    sampling: &ChartSampling,
) -> Result<HistogramData> {
    let (values, rows) = read_split(lf, column, color, sampling)?;
    let mut all: Vec<f64> = values.iter().map(|(v, _)| *v).collect();
    let outside = sort_and_clip(&mut all, range);
    let clipped = clipped(range, outside);
    let mut data = HistogramData {
        column: column.to_string(),
        bins: Vec::new(),
        groups: Vec::new(),
        other: false,
        share,
        x_min: 0.0,
        x_max: 1.0,
        max_count: 0.0,
        rows,
        clipped,
    };
    let (Some(&lo), Some(&hi)) = (all.first(), all.last()) else {
        return Ok(data);
    };
    let span = hi - lo;
    let bin_count = if span <= f64::EPSILON { 1 } else { bins.max(1) };
    let bin_width = if span <= f64::EPSILON {
        1.0
    } else {
        span / bin_count as f64
    };
    (data.x_min, data.x_max) = if span <= f64::EPSILON {
        (lo - 0.5, hi + 0.5)
    } else {
        (lo, hi)
    };
    let bin_of = |v: f64| {
        if span <= f64::EPSILON {
            0
        } else {
            (((v - lo) / bin_width).floor().max(0.0) as usize).min(bin_count - 1)
        }
    };
    let groups = color.map_or(0, |c| c.series());
    let mut total = vec![0.0_f64; bin_count];
    let mut by_group = vec![vec![0.0_f64; bin_count]; groups];
    for (v, group) in values {
        // The range is the whole view's; the bins count the groups drawn.
        if !(lo..=hi).contains(&v) || (color.is_some() && group.is_none()) {
            continue;
        }
        let bin = bin_of(v);
        total[bin] += 1.0;
        if let Some(g) = group {
            by_group[g][bin] += 1.0;
        }
    }
    let as_share = |counts: &mut Vec<f64>| {
        let n: f64 = counts.iter().sum();
        if share && n > 0.0 {
            counts.iter_mut().for_each(|c| *c /= n);
        }
    };
    as_share(&mut total);
    by_group.iter_mut().for_each(as_share);
    let center = |i: usize| {
        if span <= f64::EPSILON {
            lo
        } else {
            lo + (i as f64 + 0.5) * bin_width
        }
    };
    data.bins = total
        .iter()
        .enumerate()
        .map(|(i, &count)| HistogramBin {
            center: center(i),
            count,
        })
        .collect();
    let max = |counts: &[f64]| counts.iter().copied().fold(0.0_f64, f64::max);
    if let Some(color) = color {
        data.groups = color
            .names()
            .into_iter()
            .zip(by_group)
            .map(|(name, counts)| HistogramGroup { name, counts })
            .collect();
        data.other = color.other;
        data.max_count = data
            .groups
            .iter()
            .map(|g| max(&g.counts))
            .fold(0.0, f64::max);
    } else {
        data.max_count = max(&total);
    }
    Ok(data)
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let pos = q.clamp(0.0, 1.0) * (n as f64 - 1.0);
    let idx = pos.floor() as usize;
    let next = pos.ceil() as usize;
    if idx == next {
        sorted[idx]
    } else {
        let lower = sorted[idx];
        let upper = sorted[next];
        let weight = pos - idx as f64;
        lower + (upper - lower) * weight
    }
}

/// The five numbers of sorted `values`, or `None` when there are none.
fn box_stats(name: String, values: &[f64]) -> Option<BoxPlotStats> {
    let (min, max) = (*values.first()?, *values.last()?);
    Some(BoxPlotStats {
        name,
        min,
        q1: quantile(values, 0.25),
        median: quantile(values, 0.5),
        q3: quantile(values, 0.75),
        max,
    })
}

/// Box plot data from stats, its bounds taken from them.
fn box_data(stats: Vec<BoxPlotStats>, rows: RowsRead, clipped: Option<Clipped>) -> BoxPlotData {
    let mut y_min = stats.iter().map(|s| s.min).fold(f64::INFINITY, f64::min);
    let mut y_max = stats
        .iter()
        .map(|s| s.max)
        .fold(f64::NEG_INFINITY, f64::max);
    if stats.is_empty() {
        (y_min, y_max) = (0.0, 1.0);
    } else if y_max <= y_min {
        y_max = y_min + 1.0;
    }
    BoxPlotData {
        stats,
        y_min,
        y_max,
        rows,
        clipped,
        of: 0,
    }
}

/// Prepare box plot stats for one or more numeric columns. Uses a single read for all columns.
pub fn prepare_box_plot_data<T: AsRef<str>>(
    lf: &LazyFrame,
    columns: &[T],
    range: ValueRange,
    sampling: &ChartSampling,
) -> Result<BoxPlotData> {
    let col_refs: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
    let (columns_values, rows) = if col_refs.is_empty() {
        (Vec::new(), RowsRead::default())
    } else {
        read_values(lf, &col_refs, sampling)?
    };
    let mut stats = Vec::new();
    let mut outside = 0;
    for (column, mut values) in col_refs.iter().zip(columns_values) {
        outside += sort_and_clip(&mut values, range);
        stats.extend(box_stats((*column).to_string(), &values));
    }
    Ok(box_data(stats, rows, clipped(range, outside)))
}

/// Prepare one box of `column` per group of `by`: a box per category. The range is
/// each group's own, as each box describes its group.
pub fn prepare_box_by(
    lf: &LazyFrame,
    column: &str,
    by: ColorSplit<'_>,
    range: ValueRange,
    sampling: &ChartSampling,
) -> Result<BoxPlotData> {
    let (values, rows) = read_split(lf, column, Some(by), sampling)?;
    let mut groups = vec![Vec::new(); by.groups.len()];
    for (v, group) in values {
        if let Some(g) = group {
            groups[g].push(v);
        }
    }
    let mut outside = 0;
    let mut stats = Vec::new();
    for (name, mut values) in by.groups.iter().zip(groups) {
        outside += sort_and_clip(&mut values, range);
        stats.extend(box_stats(group_label(name), &values));
    }
    Ok(box_data(stats, rows, clipped(range, outside)))
}

fn kde_bandwidth(values: &[f64]) -> f64 {
    if values.len() <= 1 {
        return 1.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    let std = var.sqrt();
    if std <= f64::EPSILON {
        return 1.0;
    }
    1.06 * std * n.powf(-0.2)
}

/// The density of sorted `values` at 200 points, from three bandwidths below the
/// least to three above the greatest; `None` when there are none.
fn kde_series(name: String, values: &[f64], bandwidth_factor: f64) -> Option<KdeSeries> {
    let (min, max) = (*values.first()?, *values.last()?);
    let bandwidth = (kde_bandwidth(values) * bandwidth_factor).max(f64::EPSILON);
    let x_start = min - 3.0 * bandwidth;
    let x_end = max + 3.0 * bandwidth;
    let samples = 200_usize;
    let step = (x_end - x_start) / (samples.saturating_sub(1).max(1) as f64);
    let inv = 1.0 / ((values.len() as f64) * bandwidth * (2.0 * PI).sqrt());
    let points = (0..samples)
        .map(|i| {
            let x = x_start + i as f64 * step;
            let sum: f64 = values
                .iter()
                .map(|&v| {
                    let u = (x - v) / bandwidth;
                    (-0.5 * u * u).exp()
                })
                .sum();
            (x, inv * sum)
        })
        .collect();
    Some(KdeSeries { name, points })
}

/// KDE data from its series, its bounds taken from them.
fn kde_data(series: Vec<KdeSeries>, rows: RowsRead, clipped: Option<Clipped>) -> KdeData {
    let points = || series.iter().flat_map(|s| s.points.iter());
    let mut x_min = points().map(|p| p.0).fold(f64::INFINITY, f64::min);
    let mut x_max = points().map(|p| p.0).fold(f64::NEG_INFINITY, f64::max);
    let mut y_max = points().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);
    if series.is_empty() {
        (x_min, x_max, y_max) = (0.0, 1.0, 1.0);
    }
    if x_max <= x_min {
        x_max = x_min + 1.0;
    }
    if y_max <= 0.0 {
        y_max = 1.0;
    }
    KdeData {
        series,
        other: false,
        x_min,
        x_max,
        y_max,
        rows,
        clipped,
    }
}

/// Prepare KDE data for one or more numeric columns. Uses a single read for all columns.
pub fn prepare_kde_data<T: AsRef<str>>(
    lf: &LazyFrame,
    columns: &[T],
    bandwidth_factor: f64,
    range: ValueRange,
    sampling: &ChartSampling,
) -> Result<KdeData> {
    let col_refs: Vec<&str> = columns.iter().map(|c| c.as_ref()).collect();
    let (columns_values, rows) = if col_refs.is_empty() {
        (Vec::new(), RowsRead::default())
    } else {
        read_values(lf, &col_refs, sampling)?
    };
    let mut series = Vec::new();
    let mut outside = 0;
    for (column, mut values) in col_refs.iter().zip(columns_values) {
        outside += sort_and_clip(&mut values, range);
        series.extend(kde_series((*column).to_string(), &values, bandwidth_factor));
    }
    Ok(kde_data(series, rows, clipped(range, outside)))
}

/// Prepare one density curve of `column` per group of `color`. The range is the
/// whole column's, so every curve is clipped alike.
pub fn prepare_kde_by(
    lf: &LazyFrame,
    column: &str,
    bandwidth_factor: f64,
    range: ValueRange,
    color: ColorSplit<'_>,
    sampling: &ChartSampling,
) -> Result<KdeData> {
    let (values, rows) = read_split(lf, column, Some(color), sampling)?;
    let mut all: Vec<f64> = values.iter().map(|(v, _)| *v).collect();
    let outside = sort_and_clip(&mut all, range);
    let (lo, hi) = match (all.first(), all.last()) {
        (Some(&lo), Some(&hi)) => (lo, hi),
        _ => (f64::INFINITY, f64::NEG_INFINITY),
    };
    let mut groups = vec![Vec::new(); color.series()];
    for (v, group) in values {
        if let Some(g) = group
            && (lo..=hi).contains(&v)
        {
            groups[g].push(v);
        }
    }
    let last = color.series().saturating_sub(1);
    let mut other = false;
    let series = color
        .names()
        .into_iter()
        .zip(groups)
        .enumerate()
        .filter_map(|(i, (name, mut values))| {
            values.sort_by(f64::total_cmp);
            let series = kde_series(name, &values, bandwidth_factor)?;
            other = color.other && i == last;
            Some(series)
        })
        .collect();
    Ok(KdeData {
        other,
        ..kde_data(series, rows, clipped(range, outside))
    })
}

/// Prepare heatmap data for two numeric columns. A row counts when both are present.
pub fn prepare_heatmap_data(
    lf: &LazyFrame,
    x_column: &str,
    y_column: &str,
    bins: usize,
    sampling: &ChartSampling,
) -> Result<HeatmapData> {
    let (df, rows) = read_columns(lf, &[x_column, y_column], sampling)?;
    let pairs: Vec<(f64, f64)> = f64_values(&df, x_column)?
        .into_iter()
        .zip(f64_values(&df, y_column)?)
        .filter_map(|(x, y)| Some((x?, y?)))
        .collect();
    let x_bins = bins.max(1);
    let y_bins = bins.max(1);
    if pairs.is_empty() {
        return Ok(HeatmapData {
            x_column: x_column.to_string(),
            y_column: y_column.to_string(),
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
            x_bins,
            y_bins,
            counts: vec![vec![0.0; x_bins]; y_bins],
            max_count: 0.0,
            rows,
        });
    }
    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    for (x, y) in &pairs {
        x_min = x_min.min(*x);
        x_max = x_max.max(*x);
        y_min = y_min.min(*y);
        y_max = y_max.max(*y);
    }
    if x_max <= x_min {
        x_max = x_min + 1.0;
    }
    if y_max <= y_min {
        y_max = y_min + 1.0;
    }
    let mut counts = vec![vec![0.0_f64; x_bins]; y_bins];
    let x_range = x_max - x_min;
    let y_range = y_max - y_min;
    for (x, y) in pairs {
        let xi =
            (((x - x_min) / x_range * x_bins as f64).floor().max(0.0) as usize).min(x_bins - 1);
        let yi =
            (((y - y_min) / y_range * y_bins as f64).floor().max(0.0) as usize).min(y_bins - 1);
        counts[yi][xi] += 1.0;
    }
    let max_count = counts
        .iter()
        .flat_map(|row| row.iter())
        .cloned()
        .fold(0.0_f64, f64::max);
    Ok(HeatmapData {
        x_column: x_column.to_string(),
        y_column: y_column.to_string(),
        x_min,
        x_max,
        y_min,
        y_max,
        x_bins,
        y_bins,
        counts,
        max_count,
        rows,
    })
}

/// The order of a bar chart's bars, top to bottom.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarOrder {
    /// Largest value first.
    #[default]
    Value,
    /// The category's own order: text A to Z, numbers ascending, false before true.
    Label,
}

impl BarOrder {
    pub const ALL: [Self; 2] = [Self::Value, Self::Label];

    pub fn label(self) -> &'static str {
        match self {
            Self::Value => "Value",
            Self::Label => "Label",
        }
    }
}

/// Most bars a bar chart prepares; the rest are counted, not drawn. More than a tall
/// terminal shows, few enough that an export stays legible.
pub const BAR_CAP: usize = 100;

/// Column types a bar chart takes as its category: text, categories, booleans and
/// integers. A float or a timestamp is a measure, not a category.
pub fn is_category_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) | DataType::Boolean
    ) || dtype.is_integer()
}

/// What sets a bar's length: a numeric column, one row per category, or how many rows
/// each category has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BarValue {
    Count,
    Column(String),
}

impl BarValue {
    pub fn label(&self) -> &str {
        match self {
            Self::Count => "Count",
            Self::Column(column) => column,
        }
    }
}

/// Categories a count keeps before it stops: past this the column is an identifier,
/// not a category, and a count per value would hold as much as the table.
pub const COUNT_CATEGORY_CAP: usize = 100_000;

/// One bar: its category (`None` for a null category) and its value. Split by a
/// color, a bar is a row of bars, one per group (`None` where a group has no rows),
/// and its value is their total.
#[derive(Clone, Debug, PartialEq)]
pub struct Bar {
    pub label: Option<String>,
    pub value: f64,
    pub by_group: Vec<Option<f64>>,
}

/// A bar chart: one bar per category, in order, up to [`BAR_CAP`].
#[derive(Clone, Debug)]
pub struct BarData {
    pub category: String,
    pub value_column: String,
    pub bars: Vec<Bar>,
    /// Categories past the cap, not in `bars`.
    pub more: usize,
    /// Categories left out because their value is null.
    pub no_value: usize,
    pub rows: RowsRead,
    /// The value column's type: an integer column prints whole numbers.
    pub value_dtype: DataType,
    /// Rows counted, when a count read past the sample size: the counts are of every
    /// row of the view, and the note says so.
    pub counted: Option<usize>,
    /// The color groups each bar is split into, in color order; empty when not split.
    pub groups: Vec<String>,
    /// The last group is Other: every value of the color without a group of its own.
    pub other: bool,
    /// What an aggregate read, said under the plot: `all 336,776 rows`.
    pub rows_note: Option<String>,
}

impl BarData {
    /// The format the table prints the value column in, or plain where it shows the
    /// column unformatted.
    pub fn value_format(
        &self,
        settings: &crate::numfmt::NumberFormatSettings,
    ) -> crate::numfmt::NumberFormat {
        table_number_format(settings, &self.value_column, &self.value_dtype)
    }

    /// Each bar's value as the table prints the value column.
    pub fn value_labels(&self, settings: &crate::numfmt::NumberFormatSettings) -> Vec<String> {
        self.labels_in(&self.value_format(settings))
    }

    /// Each bar's value in `format`: whole for an integer column or a count.
    pub fn labels_in(&self, format: &crate::numfmt::NumberFormat) -> Vec<String> {
        let integer = self.value_dtype.is_integer();
        self.bars
            .iter()
            .map(|b| format_bar_value(b.value, integer, format))
            .collect()
    }
}

/// Format a bar's value for the label beside it in `format`: an integer column's whole,
/// anything else to the format's decimal places or two, so every bar shows the same
/// number of them. Values too large, or too small to show in those places, go to
/// scientific notation.
pub fn format_bar_value(v: f64, integer: bool, format: &crate::numfmt::NumberFormat) -> String {
    let places = format.float_precision.unwrap_or(2);
    let smallest = 0.5 * 10f64.powi(-i32::from(places));
    if !v.is_finite() || v.abs() >= 1e15 || (!integer && v != 0.0 && v.abs() < smallest) {
        return scientific(v, 2, format.decimal_sep);
    }
    let mut out = String::new();
    if integer {
        format.write_i64(v as i64, &mut out);
    } else {
        let fixed = crate::numfmt::NumberFormat {
            float_precision: Some(places),
            ..format.clone()
        };
        fixed.write_f64(v, &mut String::new(), &mut out);
    }
    out
}

/// A column name as SQL reads it: bare when it is a plain lowercase identifier, quoted
/// otherwise.
fn sql_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Prepare a bar chart: one bar per row, the category from `category`, the length from
/// `value`. The chart takes a grouped result — one row per category — and refuses a
/// category that repeats rather than guess how to combine its rows. A null category is
/// a bar of its own; a null value leaves its category out, counted in `no_value`.
pub fn prepare_bar_data(
    lf: &LazyFrame,
    category: &str,
    value: &str,
    order: BarOrder,
    cap: usize,
    sampling: &ChartSampling,
) -> Result<BarData> {
    let (df, rows) = read_columns(lf, &[category, value], sampling)?;
    let categories = df.column(category)?.as_materialized_series().clone();
    let labels_series = crate::past_calendar::cast_text(&categories, CastOptions::NonStrict)?;
    let labels: Vec<Option<&str>> = labels_series.str()?.iter().collect();

    let mut seen: std::collections::HashMap<Option<&str>, usize> =
        std::collections::HashMap::with_capacity(labels.len());
    for label in &labels {
        *seen.entry(*label).or_default() += 1;
    }
    if seen.len() < labels.len() {
        let read = match rows.sample_size {
            Some(n) => format!("a sample of {} rows", crate::numfmt::group_chrome(n)),
            None => format!("{} rows", crate::numfmt::group_chrome(labels.len())),
        };
        let (c, v) = (sql_ident(category), sql_ident(value));
        // The q form only where it reads the names as they are.
        let q = if c == category && v == value {
            format!(" (or select avg {value} by {category})")
        } else {
            String::new()
        };
        return Err(color_eyre::eyre::eyre!(
            "{category} repeats: {} categories in {read}. A bar takes one row per category, \
             so group first: SELECT {c}, AVG({v}) FROM df GROUP BY {c}{q}, or choose Count \
             for the rows per category",
            crate::numfmt::group_chrome(seen.len()),
        ));
    }

    let values = f64_values(&df, value)?;
    let (bars, more, no_value) = order_bars(&categories, &labels, &values, order, cap);
    Ok(BarData {
        category: category.to_string(),
        value_column: value.to_string(),
        bars,
        more,
        no_value,
        rows,
        value_dtype: df.column(value)?.dtype().clone(),
        counted: None,
        groups: Vec::new(),
        other: false,
        rows_note: None,
    })
}

/// A bar per row in `order`, up to `cap`: the bars, how many are past the cap, and how
/// many rows had no value. Equal values keep row order.
fn order_bars(
    categories: &Series,
    labels: &[Option<&str>],
    values: &[Option<f64>],
    order: BarOrder,
    cap: usize,
) -> (Vec<Bar>, usize, usize) {
    let row_order: Vec<usize> = match order {
        BarOrder::Value => (0..labels.len()).collect(),
        BarOrder::Label => label_order(categories),
    };
    let mut no_value = 0;
    let mut bars: Vec<Bar> = row_order
        .into_iter()
        .filter_map(|i| match values[i] {
            Some(value) => Some(Bar {
                label: labels[i].map(str::to_string),
                value,
                by_group: Vec::new(),
            }),
            None => {
                no_value += 1;
                None
            }
        })
        .collect();
    if order == BarOrder::Value {
        // Stable: equal values keep row order.
        bars.sort_by(|a, b| b.value.total_cmp(&a.value));
    }
    let more = bars.len().saturating_sub(cap);
    bars.truncate(cap);
    (bars, more, no_value)
}

/// Rows in the category's own order: text A to Z, numbers ascending, an enum in its
/// order, nulls last.
fn label_order(categories: &Series) -> Vec<usize> {
    categories
        .arg_sort(
            SortOptions::default()
                .with_nulls_last(true)
                .with_maintain_order(true),
        )
        .iter()
        .flatten()
        .map(|i| i as usize)
        .collect()
}

/// Prepare a bar chart of how many rows each category has. The counts are exact: the
/// whole view is counted, whatever the sample size, in one streamed pass that keeps a
/// count per category and no rows. When the rows held are the whole view, those are
/// counted instead. Past [`COUNT_CATEGORY_CAP`] categories the count stops, and says
/// so rather than drawing part of the view as the whole.
pub fn prepare_bar_counts(
    lf: &LazyFrame,
    category: &str,
    order: BarOrder,
    cap: usize,
    sampling: &ChartSampling,
) -> Result<BarData> {
    count_bars(lf, category, order, cap, COUNT_CATEGORY_CAP, sampling)
}

fn count_bars(
    lf: &LazyFrame,
    category: &str,
    order: BarOrder,
    cap: usize,
    max_categories: usize,
    sampling: &ChartSampling,
) -> Result<BarData> {
    let counted = match held_counts(sampling, category, max_categories)? {
        Some(counted) => counted,
        None => {
            // A view the sample size takes whole is read as the other charts read it,
            // so they draw from the same rows after; a larger one is streamed.
            let fits = sampling
                .limit
                .zip(sampling.known_total)
                .is_some_and(|(n, total)| total <= n);
            let whole = if fits {
                let (df, rows) = read_columns(lf, &[category], sampling)?;
                rows.sample_size.is_none().then_some(df)
            } else {
                None
            };
            let counted = match whole {
                Some(df) => count_frame(&df, category, max_categories)?,
                None => stream_counts(lf, category, max_categories, &sampling.cancel)?,
            };
            hold_counts(sampling, category, &counted);
            counted
        }
    };
    let (counts, total) = match counted {
        Counted::All { counts, rows } => (counts, rows),
        Counted::TooMany => {
            return Err(color_eyre::eyre::eyre!(
                "more than {} categories of {category}: counting stopped. Count by a \
                 column with fewer values",
                crate::numfmt::group_chrome(max_categories)
            ));
        }
    };
    let data = |bars, more| BarData {
        category: category.to_string(),
        value_column: "count".to_string(),
        bars,
        more,
        no_value: 0,
        rows: RowsRead {
            total_rows: total,
            sample_size: None,
            envelope_steps: None,
            seed: None,
        },
        value_dtype: DataType::UInt64,
        counted: sampling.limit.is_some_and(|n| total > n).then_some(total),
        groups: Vec::new(),
        other: false,
        rows_note: None,
    };
    let Some(counts) = counts else {
        return Ok(data(Vec::new(), 0));
    };
    // Label order first, so categories with equal counts come A to Z.
    let by_label: Vec<IdxSize> = label_order(counts.column(category)?.as_materialized_series())
        .into_iter()
        .map(|i| i as IdxSize)
        .collect();
    let counts = counts.take(&IdxCa::from_vec("order".into(), by_label))?;
    let categories = counts.column(category)?.as_materialized_series().clone();
    let labels_series = crate::past_calendar::cast_text(&categories, CastOptions::NonStrict)?;
    let labels: Vec<Option<&str>> = labels_series.str()?.iter().collect();
    let values: Vec<Option<f64>> = counts
        .column(COUNT_COLUMN)?
        .u64()?
        .iter()
        .map(|n| n.map(|n| n as f64))
        .collect();
    let (bars, more, _) = order_bars(&categories, &labels, &values, order, cap);
    Ok(data(bars, more))
}

/// The view's counts of `category` without reading it: counted before, or counted now
/// from rows held that are the whole view.
fn held_counts(
    sampling: &ChartSampling,
    category: &str,
    max_categories: usize,
) -> Result<Option<Counted>> {
    let holding = sampling.held.0.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(held) = holding.counts.iter().find(|h| h.category == category) {
        return Ok(Some(held.counted.clone()));
    }
    let Some(whole) = holding
        .rows
        .as_ref()
        .filter(|h| h.rows.sample_size.is_none() && h.df.column(category).is_ok())
    else {
        return Ok(None);
    };
    let counted = count_frame(&whole.df, category, max_categories)?;
    drop(holding);
    hold_counts(sampling, category, &counted);
    Ok(Some(counted))
}

fn hold_counts(sampling: &ChartSampling, category: &str, counted: &Counted) {
    let mut holding = sampling.held.0.lock().unwrap_or_else(|e| e.into_inner());
    holding.counts.retain(|h| h.category != category);
    if holding.counts.len() >= HELD_COUNTS {
        holding.counts.remove(0);
    }
    holding.counts.push(HeldCounts {
        category: category.to_string(),
        counted: counted.clone(),
    });
}

/// Count the categories of rows already in memory.
pub(crate) fn count_frame(
    df: &DataFrame,
    category: &str,
    max_categories: usize,
) -> Result<Counted> {
    let mut tally = Tally::new(category, max_categories);
    tally.observe(&df.select([category])?)?;
    Ok(tally.finish()?)
}

/// What a count found: one row per category with its rows in [`COUNT_COLUMN`] (none
/// when the view has no rows), and the rows counted; or more categories than it keeps.
#[derive(Clone)]
pub(crate) enum Counted {
    All {
        counts: Option<DataFrame>,
        rows: usize,
    },
    TooMany,
}

pub(crate) const COUNT_COLUMN: &str = "__datui_bar_count";

/// Counts held per view: enough to go back and forth between a few categories, each
/// up to [`COUNT_CATEGORY_CAP`] rows.
const HELD_COUNTS: usize = 4;

/// Rows piled up unmerged before a merge, at least: a run of new categories costs a
/// merge now and then rather than one per batch.
const MERGE_AFTER: usize = 1 << 16;

/// Rows per category, added up batch by batch. Holds one row per category and the
/// batches since the last merge, never the rows themselves.
pub(crate) struct Tally {
    category: PlSmallStr,
    max: usize,
    counts: Option<DataFrame>,
    /// Rows of `counts` as last merged: one per category.
    merged: usize,
    rows: usize,
    too_many: bool,
    /// Stopped before the end of the view because nobody wants the count.
    cancelled: bool,
}

impl Tally {
    pub(crate) fn new(category: &str, max: usize) -> Self {
        Self {
            category: category.into(),
            max,
            counts: None,
            merged: 0,
            rows: 0,
            too_many: false,
            cancelled: false,
        }
    }

    /// Count a batch of the category column. True once there are more categories than
    /// the count keeps, which stops the read.
    pub(crate) fn observe(&mut self, batch: &DataFrame) -> PolarsResult<bool> {
        if self.too_many {
            return Ok(true);
        }
        self.rows += batch.height();
        let part = group_counts(batch, &self.category, false)?;
        let mut counts = match self.counts.take() {
            Some(mut counts) => {
                counts.vstack_mut(&part)?;
                counts
            }
            None => part,
        };
        if counts.height() - self.merged >= self.merged.max(MERGE_AFTER) {
            counts = group_counts(&counts, &self.category, true)?;
            self.merged = counts.height();
            self.too_many = self.merged > self.max;
        }
        self.counts = Some(counts);
        Ok(self.too_many)
    }

    pub(crate) fn finish(self) -> PolarsResult<Counted> {
        let counts = match self.counts {
            Some(counts) => Some(group_counts(&counts, &self.category, true)?),
            None => None,
        };
        if self.too_many || counts.as_ref().is_some_and(|c| c.height() > self.max) {
            return Ok(Counted::TooMany);
        }
        Ok(Counted::All {
            counts,
            rows: self.rows,
        })
    }
}

/// One row per category of `df` with how many rows it stands for: its rows, or when
/// `summed` the counts they already carry, added up. A null category is one of them.
fn group_counts(df: &DataFrame, category: &str, summed: bool) -> PolarsResult<DataFrame> {
    let by = df.group_by([category])?;
    let groups = by.get_groups();
    let counts: Vec<u64> = if summed {
        let carried: Vec<u64> = df
            .column(COUNT_COLUMN)?
            .u64()?
            .into_no_null_iter()
            .collect();
        groups
            .iter()
            .map(|group| match group {
                GroupsIndicator::Idx((_, rows)) => rows.iter().map(|&i| carried[i as usize]).sum(),
                GroupsIndicator::Slice([first, len]) => {
                    carried[first as usize..(first + len) as usize].iter().sum()
                }
            })
            .collect()
    } else {
        groups.iter().map(|group| group.len() as u64).collect()
    };
    let mut columns = by.keys();
    columns.push(Column::new(COUNT_COLUMN.into(), counts));
    DataFrame::new_infer_height(columns)
}

/// Count the view's categories in one streamed pass, stopping past `max` of them, or
/// as soon as `cancel` is set: a pass over a large table can take minutes, and the
/// next chart waits for it.
fn stream_counts(
    lf: &LazyFrame,
    category: &str,
    max: usize,
    cancel: &Arc<AtomicBool>,
) -> Result<Counted> {
    let state = Arc::new(Mutex::new(Tally::new(category, max)));
    let callback_state = Arc::clone(&state);
    let callback_cancel = Arc::clone(cancel);
    let sink = lf.clone().select([col(category)]).sink_batches(
        PlanCallback::new(move |batch: DataFrame| {
            let mut tally = callback_state
                .lock()
                .map_err(|_| PolarsError::ComputeError("count lock failed".into()))?;
            if callback_cancel.load(Ordering::Relaxed) {
                tally.cancelled = true;
                return Ok(true);
            }
            tally.observe(&batch)
        }),
        false,
        None,
    )?;
    // Streaming whatever the setting: a count per category is all this holds.
    crate::statistics::collect_lazy(sink, true)?;
    let tally = std::mem::replace(
        &mut *state.lock().unwrap_or_else(|e| e.into_inner()),
        Tally::new(category, max),
    );
    // Part of the view counted is not a count of it. A pass that finished before it
    // was told to stop is whole, and kept.
    if tally.cancelled {
        return Err(color_eyre::eyre::eyre!("count cancelled"));
    }
    Ok(tally.finish()?)
}

// ----- Color: one series per value -----

/// A column a chart is split by, and the values given a group each, in color order.
/// `None` is the rows with no value. With `other`, every other value's rows make one
/// more group after them, [`OTHER`].
#[derive(Clone, Copy, Debug)]
pub struct ColorSplit<'a> {
    pub column: &'a str,
    pub groups: &'a [Option<String>],
    pub other: bool,
}

/// The name of the group of every value not given one of its own.
pub const OTHER: &str = "Other";

impl ColorSplit<'_> {
    /// How many groups the rows fall in: the values', and Other.
    pub fn series(&self) -> usize {
        self.groups.len() + usize::from(self.other)
    }

    /// Each group's name, as a legend writes it: Other last.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.groups.iter().map(group_label).collect();
        if self.other {
            names.push(OTHER.to_string());
        }
        names
    }
}

/// A group's name as a legend writes it.
pub fn group_label(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "null".to_string())
}

/// Each row's group: its place among `split.groups`, Other's after them, or `None`
/// for a value that has none. Values are compared as text, as the value picker lists
/// them.
fn row_groups(df: &DataFrame, split: ColorSplit<'_>) -> Result<Vec<Option<usize>>> {
    let series = df.column(split.column)?.as_materialized_series();
    let text = crate::past_calendar::cast_text(series, CastOptions::NonStrict)?;
    let index: std::collections::HashMap<Option<&str>, usize> = split
        .groups
        .iter()
        .enumerate()
        .map(|(i, g)| (g.as_deref(), i))
        .collect();
    let other = split.other.then_some(split.groups.len());
    Ok(text
        .str()?
        .iter()
        .map(|v| index.get(&v).copied().or(other))
        .collect())
}

/// Values with the group each is in, and what was read for them.
type SplitValues = (Vec<(f64, Option<usize>)>, RowsRead);

/// `column`'s finite values as read for a chart, each with its group when split
/// (`None` for a row of a value no group has, which still counts toward a range).
fn read_split(
    lf: &LazyFrame,
    column: &str,
    split: Option<ColorSplit<'_>>,
    sampling: &ChartSampling,
) -> Result<SplitValues> {
    let mut columns = vec![column];
    if let Some(split) = split {
        columns.push(split.column);
    }
    let (df, rows) = read_columns(lf, &columns, sampling)?;
    let values = f64_values(&df, column)?;
    let groups = split.map(|s| row_groups(&df, s)).transpose()?;
    let out = values
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| {
            let v = v?;
            Some((v, groups.as_ref().and_then(|groups| groups[i])))
        })
        .collect();
    Ok((out, rows))
}

/// A column's values with the rows holding each, most rows first (equal counts in
/// the column's order), counted over the whole view.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ValueRows {
    pub values: Vec<(Option<String>, u64)>,
    /// Rows counted.
    pub rows: usize,
}

/// Count `column`'s values over the whole view, or take the count held for it: one
/// streamed pass that keeps a count per value, the one a bar chart of counts makes.
pub fn value_rows(lf: &LazyFrame, column: &str, sampling: &ChartSampling) -> Result<ValueRows> {
    let counted = match held_counts(sampling, column, COUNT_CATEGORY_CAP)? {
        Some(counted) => counted,
        None => {
            let counted = stream_counts(lf, column, COUNT_CATEGORY_CAP, &sampling.cancel)?;
            hold_counts(sampling, column, &counted);
            counted
        }
    };
    let (counts, rows) = match counted {
        Counted::All { counts, rows } => (counts, rows),
        Counted::TooMany => {
            return Err(color_eyre::eyre::eyre!(
                "more than {} values of {column}: choose a column with fewer",
                crate::numfmt::group_chrome(COUNT_CATEGORY_CAP)
            ));
        }
    };
    let Some(counts) = counts else {
        return Ok(ValueRows {
            values: Vec::new(),
            rows,
        });
    };
    let by_label: Vec<IdxSize> = label_order(counts.column(column)?.as_materialized_series())
        .into_iter()
        .map(|i| i as IdxSize)
        .collect();
    let counts = counts.take(&IdxCa::from_vec("order".into(), by_label))?;
    let labels = crate::past_calendar::cast_text(
        counts.column(column)?.as_materialized_series(),
        CastOptions::NonStrict,
    )?;
    let mut values: Vec<(Option<String>, u64)> = labels
        .str()?
        .iter()
        .zip(counts.column(COUNT_COLUMN)?.u64()?.iter())
        .map(|(label, n)| (label.map(str::to_string), n.unwrap_or(0)))
        .collect();
    // Stable, so equal counts keep the column's order.
    values.sort_by_key(|v| std::cmp::Reverse(v.1));
    Ok(ValueRows { values, rows })
}

/// The groups a color makes: the values picked, in the order picked, or else the
/// largest by rows; at most `most`, one per series color.
pub fn color_groups(
    rows: &ValueRows,
    picked: &[Option<String>],
    most: usize,
) -> Vec<Option<String>> {
    if !picked.is_empty() {
        return picked.iter().take(most).cloned().collect();
    }
    rows.values
        .iter()
        .take(most)
        .map(|(value, _)| value.clone())
        .collect()
}

/// In a plan: each row's group as its place among `split.groups` (UInt32), Other's
/// after them, null for a value that has none.
fn group_expr(split: ColorSplit<'_>) -> Expr {
    let text = crate::past_calendar::text_expr(col(split.column), CastOptions::NonStrict);
    let mut out = match split.other {
        true => lit(split.groups.len() as u32).cast(DataType::UInt32),
        false => lit(NULL).cast(DataType::UInt32),
    };
    for (i, group) in split.groups.iter().enumerate().rev() {
        let matches = match group {
            Some(value) => text.clone().eq(lit(value.clone())),
            None => col(split.column).is_null(),
        };
        out = when(matches).then(lit(i as u32)).otherwise(out);
    }
    out
}

/// Series of a line or scatter chart, each named: a Y column's or a color group's.
#[derive(Clone, Debug, Default)]
pub struct GroupedSeries {
    pub names: Vec<String>,
    pub series: Vec<Vec<(f64, f64)>>,
    /// Per series, where its line starts again after a gap.
    pub breaks: Vec<Vec<usize>>,
    pub x_axis_kind: XAxisTemporalKind,
    pub rows: RowsRead,
    /// The last series is Other: every value of the color without a series of its own.
    pub other: bool,
}

/// A line or scatter chart of `y` split by `color`, from the rows a chart samples:
/// one series per group, in X order, each breaking where its Y has no value.
pub fn prepare_xy_by(
    lf: &LazyFrame,
    schema: &Schema,
    x: &str,
    y: &str,
    color: ColorSplit<'_>,
    sampling: &ChartSampling,
) -> Result<GroupedSeries> {
    let x_dtype = schema
        .get(x)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", x))?;
    let (df, rows) = read_columns(lf, &[x, y, color.column], sampling)?;
    let xs = x_values(&df, x, x_dtype)?;
    let ys = f64_values(&df, y)?;
    let groups = row_groups(&df, color)?;
    let mut order: Vec<(f64, usize)> = xs
        .into_iter()
        .enumerate()
        .filter_map(|(i, x)| x.map(|x| (x, i)))
        .collect();
    order.sort_by(|a, b| a.0.total_cmp(&b.0));
    let n = color.series();
    let mut series = vec![Vec::new(); n];
    let mut breaks = vec![Vec::new(); n];
    let mut gap = vec![false; n];
    for (x, i) in order {
        let Some(g) = groups[i] else { continue };
        match ys[i] {
            Some(y) => {
                if gap[g] && !series[g].is_empty() {
                    breaks[g].push(series[g].len());
                }
                gap[g] = false;
                series[g].push((x, y));
            }
            None => gap[g] = true,
        }
    }
    Ok(GroupedSeries {
        names: color.names(),
        series,
        breaks,
        x_axis_kind: x_axis_temporal_kind(x_dtype),
        rows,
        other: color.other,
    })
}

/// Most points an aggregated chart keeps. Past this X is close to a value per row,
/// and a time bucket is what it needs.
pub const AGGREGATE_POINTS_MAX: usize = 200_000;

/// What an aggregated line or scatter chart groups by and makes of the rows.
#[derive(Clone, Copy, Debug)]
pub struct AggregateSpec<'a> {
    pub x: &'a str,
    pub time_unit: crate::chart_modal::TimeUnit,
    pub ys: &'a [String],
    pub aggregate: crate::chart_modal::Aggregate,
    /// The percentile a quantile takes.
    pub quantile: u8,
    pub cumulative: crate::chart_modal::Cumulative,
    pub color: Option<ColorSplit<'a>>,
}

/// Y as an aggregate reads it: as numbers, or as it is for a distinct count, which
/// counts strings and dates too.
fn y_values(y: Expr, aggregate: crate::chart_modal::Aggregate) -> Expr {
    if aggregate.takes_any_y() {
        y
    } else {
        y.cast(DataType::Float64)
    }
}

/// The row index first and last read the rows' order by.
const ROW_ORDER: &str = "__i";

/// `values`' aggregate in a plan. A quantile takes `quantile` percent; first and
/// last go by [`ROW_ORDER`], which the plan must carry, so a group's rows keep the
/// view's order whatever order the engine hands them over in.
fn aggregate_expr(values: Expr, aggregate: crate::chart_modal::Aggregate, quantile: u8) -> Expr {
    use crate::chart_modal::Aggregate;
    let in_order = || {
        values
            .clone()
            .sort_by([col(ROW_ORDER)], SortMultipleOptions::default())
            .drop_nulls()
    };
    match aggregate {
        // Nulls are no value: a group of only nulls has none, a gap.
        Aggregate::Distinct => values.drop_nulls().n_unique().cast(DataType::Float64),
        Aggregate::Sum => values.sum(),
        Aggregate::Mean => values.mean(),
        Aggregate::Median => values.median(),
        // The sample deviation: null for a group of one, which draws no point.
        Aggregate::Stdev => values.std(1),
        Aggregate::Quantile => {
            values.quantile(lit(f64::from(quantile) / 100.0), QuantileMethod::Linear)
        }
        Aggregate::Min => values.min(),
        Aggregate::Max => values.max(),
        Aggregate::First => in_order().first(),
        Aggregate::Last => in_order().last(),
        Aggregate::None | Aggregate::Count => len().cast(DataType::Float64),
    }
}

/// `lf` with [`ROW_ORDER`] when `aggregate` reads the rows' order.
fn with_row_order(lf: &LazyFrame, aggregate: crate::chart_modal::Aggregate) -> LazyFrame {
    if aggregate.follows_row_order() {
        lf.clone().with_row_index(ROW_ORDER, None)
    } else {
        lf.clone()
    }
}

/// Collect an aggregate's plan, streamed whatever the setting: a group-by holds a
/// row per group, and the streaming engine checks `cancel` between morsels. A plan
/// the streaming engine cannot take runs in memory, where the check runs once and
/// the pass goes to its end. A pass stopped by `cancel` is an error that says so.
fn aggregate_pass(lf: LazyFrame, sampling: &ChartSampling) -> Result<DataFrame> {
    crate::statistics::collect_lazy(lf, true).map_err(|e| {
        if sampling.cancel.load(Ordering::Relaxed) {
            color_eyre::eyre::eyre!(ENVELOPE_CANCELLED)
        } else {
            e.into()
        }
    })
}

/// Rows of X a sample reads to judge how many values it has.
const GROUPS_SAMPLE: usize = 20_000;

/// Refuse, before the group-by, an X with more values than a chart can draw: a
/// group per value of a column of nearly as many values as rows would hold the
/// table. Judged from a sample of X: its distinct share, times the rows.
fn refuse_too_many_groups(
    lf: &LazyFrame,
    x: &str,
    most: usize,
    sampling: &ChartSampling,
) -> Result<()> {
    let read = crate::sampling::analysis_rows(
        &lf.clone().select([col(x)]),
        Some(GROUPS_SAMPLE),
        sampling.known_total,
        sampling.seed,
        sampling.streaming,
    )?;
    let distinct = read.df.column(x)?.n_unique()?;
    let read_rows = read.df.height().max(1);
    let estimate = match read.sample_size {
        Some(_) => distinct as f64 / read_rows as f64 * read.total_rows as f64,
        None => distinct as f64,
    };
    if estimate > most as f64 {
        return Err(color_eyre::eyre::eyre!(
            "about {} values of {x}: more than a chart can draw. Bucket X by a time \
             unit, or choose a column with fewer values",
            crate::numfmt::group_chrome(estimate as usize)
        ));
    }
    Ok(())
}

/// A line or scatter chart of Y aggregated per X (per time bucket of a temporal X),
/// and per color group: one lazy group-by over every row of the view. A count needs
/// no Y column; any other aggregate draws a series per Y column, or per color group
/// of the first.
///
/// With cumulative on, each point is the running total of the rows up to the end
/// of its X (its bucket), per series, in X order: a running sum of Y, or Y's rates
/// compounded, `(1 + y1)(1 + y2)... - 1` over every row. Each bucket carries its
/// rows' sum (or the sum of `ln(1 + y)`, which compounds the same), and the totals
/// run across buckets; the aggregate is not used, but a count runs as a count of
/// rows.
pub fn prepare_aggregate_xy(
    lf: &LazyFrame,
    schema: &Schema,
    spec: &AggregateSpec<'_>,
    sampling: &ChartSampling,
) -> Result<GroupedSeries> {
    use crate::chart_modal::{Aggregate, Cumulative};
    let x_dtype = schema
        .get(spec.x)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", spec.x))?;
    let mut x = col(spec.x);
    let bucketed = spec.time_unit.every().is_some()
        && matches!(x_dtype, DataType::Date | DataType::Datetime(_, _));
    if let Some(every) = spec.time_unit.every().filter(|_| bucketed) {
        x = x.dt().truncate(lit(every));
    }
    if !bucketed {
        refuse_too_many_groups(lf, spec.x, AGGREGATE_POINTS_MAX, sampling)?;
    }
    let x = until_cancelled(x, &sampling.cancel).alias("__x");
    let count = spec.aggregate == Aggregate::Count;
    let ys: &[String] = match (count, spec.color) {
        (true, _) => &[],
        (false, Some(_)) => &spec.ys[..spec.ys.len().min(1)],
        (false, None) => spec.ys,
    };
    let mut select = vec![x];
    let mut keys = vec![col("__x")];
    for (i, y) in ys.iter().enumerate() {
        select.push(y_values(col(y.as_str()), spec.aggregate).alias(format!("__y{i}")));
    }
    let plan = with_row_order(lf, spec.aggregate);
    if spec.aggregate.follows_row_order() {
        select.push(col(ROW_ORDER));
    }
    if let Some(color) = spec.color {
        select.push(group_expr(color).alias("__g"));
        keys.push(col("__g"));
    }
    let mut plan = plan.select(select).filter(col("__x").is_not_null());
    if spec.color.is_some() {
        plan = plan.filter(col("__g").is_not_null());
    }
    let mut aggs = vec![len().alias("__n")];
    for i in 0..ys.len() {
        let y = col(format!("__y{i}"));
        let made = match spec.cumulative {
            Cumulative::Off => aggregate_expr(y.clone(), spec.aggregate, spec.quantile),
            Cumulative::Sum => y.clone().sum(),
            Cumulative::Compound => (lit(1.0) + y.clone()).log(lit(std::f64::consts::E)).sum(),
        };
        aggs.push(made.alias(format!("__a{i}")));
        // The values behind it: none is a gap, not the zero a sum of nothing is.
        aggs.push(y.count().alias(format!("__c{i}")));
    }
    let df = aggregate_pass(
        plan.group_by_stable(keys)
            .agg(aggs)
            .sort(["__x"], Default::default()),
        sampling,
    )?;
    if df.height() > AGGREGATE_POINTS_MAX {
        return Err(color_eyre::eyre::eyre!(
            "{} points: more than a chart can draw. Bucket X by a time unit, or \
             choose an X with fewer values",
            crate::numfmt::group_chrome(df.height())
        ));
    }
    let xs: Vec<Option<f64>> = x_values(&df, "__x", x_dtype)?;
    let counts: Vec<u64> = df
        .column("__n")?
        .cast(&DataType::UInt64)?
        .u64()?
        .iter()
        .map(|n| n.unwrap_or(0))
        .collect();
    let groups: Option<Vec<Option<u32>>> = match spec.color {
        Some(_) => Some(df.column("__g")?.u32()?.iter().collect()),
        None => None,
    };
    let values: Vec<Vec<Option<f64>>> = if count {
        vec![counts.iter().map(|&n| Some(n as f64)).collect()]
    } else {
        (0..ys.len())
            .map(|i| {
                let made = df.column(&format!("__a{i}"))?.f64()?.clone();
                let behind = df.column(&format!("__c{i}"))?.cast(&DataType::UInt64)?;
                let behind = behind.u64()?;
                Ok(made
                    .iter()
                    .zip(behind.iter())
                    .map(|(v, n)| {
                        let v = v.filter(|_| n.unwrap_or(0) > 0)?;
                        // A bucket's compound return, from its log sum.
                        Some(if spec.cumulative == Cumulative::Compound {
                            v.exp_m1()
                        } else {
                            v
                        })
                    })
                    .collect())
            })
            .collect::<Result<_>>()?
    };
    let names: Vec<String> = match spec.color {
        Some(color) => color.names(),
        None if count => vec!["count".to_string()],
        None => ys.to_vec(),
    };
    let n = names.len();
    let mut series = vec![Vec::new(); n];
    let mut breaks = vec![Vec::new(); n];
    let mut gap = vec![false; n];
    let mut push = |s: usize, x: f64, y: Option<f64>| match y.filter(|y| y.is_finite()) {
        Some(y) => {
            if gap[s] && !series[s].is_empty() {
                breaks[s].push(series[s].len());
            }
            gap[s] = false;
            series[s].push((x, y));
        }
        None => gap[s] = true,
    };
    for (row, x) in xs.iter().enumerate() {
        let Some(x) = *x else { continue };
        match &groups {
            Some(groups) => {
                if let Some(g) = groups[row] {
                    push(g as usize, x, values[0][row]);
                }
            }
            None => {
                for (s, column) in values.iter().enumerate() {
                    push(s, x, column[row]);
                }
            }
        }
    }
    // A count of rows runs as a count, whichever way the totals were asked to run.
    let how = match spec.cumulative {
        Cumulative::Compound if count => Cumulative::Sum,
        how => how,
    };
    for points in &mut series {
        accumulate(points, how);
    }
    Ok(GroupedSeries {
        names,
        series,
        breaks,
        x_axis_kind: x_axis_temporal_kind(x_dtype),
        rows: RowsRead {
            total_rows: counts.iter().sum::<u64>() as usize,
            sample_size: None,
            envelope_steps: None,
            seed: None,
        },
        other: spec.color.is_some_and(|c| c.other),
    })
}

/// Make `points` cumulative along X: a running sum, or returns compounded (each
/// value a rate; the point is what 1 grew to, less 1).
pub fn accumulate(points: &mut [(f64, f64)], how: crate::chart_modal::Cumulative) {
    use crate::chart_modal::Cumulative;
    let mut total = 0.0;
    for (_, y) in points.iter_mut() {
        total = match how {
            Cumulative::Off => return,
            Cumulative::Sum => total + *y,
            Cumulative::Compound => (1.0 + total) * (1.0 + *y) - 1.0,
        };
        *y = total;
    }
}

/// What an aggregated bar chart groups by and makes of the rows.
#[derive(Clone, Copy, Debug)]
pub struct BarAggregate<'a> {
    pub category: &'a str,
    /// The Y column; none for a count.
    pub value: Option<&'a str>,
    pub aggregate: crate::chart_modal::Aggregate,
    /// The percentile a quantile takes.
    pub quantile: u8,
    pub color: Option<ColorSplit<'a>>,
    pub order: BarOrder,
    pub cap: usize,
}

/// A bar chart of `value` aggregated per category (and per color group): one lazy
/// group-by over every row of the view. A count needs no value column; one without
/// a color is the exact count a bar chart of counts draws.
pub fn prepare_bar_aggregate(
    lf: &LazyFrame,
    spec: &BarAggregate<'_>,
    sampling: &ChartSampling,
) -> Result<BarData> {
    use crate::chart_modal::Aggregate;
    let BarAggregate {
        category,
        value,
        aggregate,
        quantile,
        color,
        order,
        cap,
    } = *spec;
    let count = aggregate == Aggregate::Count;
    if count && color.is_none() {
        return prepare_bar_counts(lf, category, order, cap, sampling);
    }
    let value = match value {
        Some(value) if !count => Some(value),
        None if !count => return Err(color_eyre::eyre::eyre!("Pick a Y column")),
        _ => None,
    };
    let schema = lf.clone().collect_schema()?;
    let value_dtype = match value {
        Some(v) => schema
            .get(v)
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("no column {v}"))?,
        None => DataType::UInt64,
    };
    let mut select = vec![until_cancelled(col(category), &sampling.cancel)];
    let mut keys = vec![col(category)];
    if let Some(value) = value {
        select.push(y_values(col(value), aggregate).alias("__v"));
    }
    if aggregate.follows_row_order() {
        select.push(col(ROW_ORDER));
    }
    if let Some(color) = color {
        select.push(group_expr(color).alias("__g"));
        keys.push(col("__g"));
    }
    let mut plan = with_row_order(lf, aggregate).select(select);
    if color.is_some() {
        plan = plan.filter(col("__g").is_not_null());
    }
    let measure = match value {
        Some(_) => aggregate_expr(col("__v"), aggregate, quantile),
        None => len().cast(DataType::Float64),
    };
    // The values behind each bar: none is no bar, not the zero a sum of nothing is.
    let behind = match value {
        Some(_) => col("__v").count(),
        None => len(),
    };
    refuse_too_many_groups(lf, category, COUNT_CATEGORY_CAP, sampling)?;
    let df = aggregate_pass(
        // Stable, so bars of equal value keep one order from run to run.
        plan.group_by_stable(keys).agg([
            len().alias("__n"),
            measure.alias("__a"),
            behind.alias("__c"),
        ]),
        sampling,
    )?;
    let rows: usize = df
        .column("__n")?
        .cast(&DataType::UInt64)?
        .u64()?
        .iter()
        .map(|n| n.unwrap_or(0) as usize)
        .sum();
    // A sum, least or greatest of whole numbers is whole; a count always is.
    let whole = aggregate.is_count()
        || (value_dtype.is_integer()
            && matches!(
                aggregate,
                Aggregate::Sum
                    | Aggregate::Min
                    | Aggregate::Max
                    | Aggregate::First
                    | Aggregate::Last
            ));
    let categories = df.column(category)?.as_materialized_series().clone();
    let labels_series = crate::past_calendar::cast_text(&categories, CastOptions::NonStrict)?;
    let labels: Vec<Option<&str>> = labels_series.str()?.iter().collect();
    let behind: Vec<u64> = df
        .column("__c")?
        .cast(&DataType::UInt64)?
        .u64()?
        .iter()
        .map(|n| n.unwrap_or(0))
        .collect();
    // NaN and infinities draw nothing true: no bar.
    let measures: Vec<Option<f64>> = df
        .column("__a")?
        .f64()?
        .iter()
        .zip(&behind)
        .map(|(v, &n)| v.filter(|v| v.is_finite() && n > 0))
        .collect();
    let value_column = match value {
        Some(value) => format!("{} {value}", aggregate.named(quantile)),
        None => "count".to_string(),
    };
    let mut data = BarData {
        category: category.to_string(),
        value_column,
        bars: Vec::new(),
        more: 0,
        no_value: 0,
        rows: RowsRead {
            total_rows: rows,
            sample_size: None,
            envelope_steps: None,
            seed: None,
        },
        value_dtype: if whole {
            DataType::Int64
        } else {
            DataType::Float64
        },
        counted: None,
        groups: Vec::new(),
        other: false,
        rows_note: None,
    };
    let too_many = || {
        color_eyre::eyre::eyre!(
            "more than {} categories of {category}: choose a column with fewer",
            crate::numfmt::group_chrome(COUNT_CATEGORY_CAP)
        )
    };
    let Some(color) = color else {
        if df.height() > COUNT_CATEGORY_CAP {
            return Err(too_many());
        }
        let (bars, more, no_value) = order_bars(&categories, &labels, &measures, order, cap);
        (data.bars, data.more, data.no_value) = (bars, more, no_value);
        return Ok(data);
    };
    // One bar row per category, a value per group: the category's first row stands
    // for it when ordering by label.
    let groups: Vec<Option<u32>> = df.column("__g")?.u32()?.iter().collect();
    let mut at: std::collections::HashMap<Option<&str>, usize> = Default::default();
    let mut firsts: Vec<IdxSize> = Vec::new();
    let mut rows_of: Vec<Vec<Option<f64>>> = Vec::new();
    for (row, label) in labels.iter().enumerate() {
        let i = *at.entry(*label).or_insert_with(|| {
            firsts.push(row as IdxSize);
            rows_of.push(vec![None; color.series()]);
            rows_of.len() - 1
        });
        if let Some(g) = groups[row] {
            rows_of[i][g as usize] = measures[row];
        }
    }
    if rows_of.len() > COUNT_CATEGORY_CAP {
        return Err(too_many());
    }
    let unique = categories.take(&IdxCa::from_vec("firsts".into(), firsts.clone()))?;
    let unique_labels: Vec<Option<&str>> = firsts.iter().map(|&r| labels[r as usize]).collect();
    // A count or a sum adds up across groups; any other measure orders by its
    // largest group.
    let totals: Vec<Option<f64>> = rows_of
        .iter()
        .map(|values| {
            let present = values.iter().flatten();
            if count || aggregate == Aggregate::Sum {
                Some(present.sum())
            } else {
                present.copied().reduce(f64::max)
            }
        })
        .collect();
    let (mut bars, more, no_value) = order_bars(&unique, &unique_labels, &totals, order, cap);
    // `order_bars` names each bar by its label; give each its groups back.
    let by_label: std::collections::HashMap<Option<&str>, usize> = unique_labels
        .iter()
        .enumerate()
        .map(|(i, l)| (*l, i))
        .collect();
    for bar in &mut bars {
        if let Some(&i) = by_label.get(&bar.label.as_deref()) {
            bar.by_group = rows_of[i].clone();
        }
    }
    data.bars = bars;
    data.more = more;
    data.no_value = no_value;
    data.groups = color.names();
    data.other = color.other;
    Ok(data)
}

#[cfg(test)]
mod tests;

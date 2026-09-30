//! Prepare chart data from a LazyFrame: read the chart's columns, then turn them into
//! points, bins or statistics.
//!
//! Every chart reads its rows through [`read_columns`]: up to a row limit of them,
//! spread across the table by the sampler the analysis tools use, so a chart shows the
//! table and not its first rows. What was read comes back as [`RowsRead`], so the chart
//! can say when it shows a sample.

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use color_eyre::Result;
use polars::datatypes::{DataType, TimeUnit};
use polars::prelude::*;
use std::f64::consts::PI;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Describes how x-axis numeric values map to temporal types for label formatting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XAxisTemporalKind {
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

/// Format a numeric axis tick (for y-axis or generic numeric).
pub fn format_axis_label(v: f64) -> String {
    if v.abs() >= 1e6 || (v.abs() < 1e-2 && v != 0.0) {
        format!("{:.2e}", v)
    } else {
        format!("{:.2}", v)
    }
}

/// A numeric tick at `level` of detail: 0 is [`format_axis_label`], 1 three
/// significant figures with a k/M/G/T suffix, for an axis too narrow for the first.
pub fn axis_label_at(v: f64, level: usize) -> Option<String> {
    match level {
        0 => Some(format_axis_label(v)),
        1 => Some(compact_number(v)),
        _ => None,
    }
}

/// Three significant figures at most, trailing zeros dropped: `12.3k`, `5`, `0.05`.
fn compact_number(v: f64) -> String {
    let a = v.abs();
    if !v.is_finite() || a >= 1e15 || (a < 1e-2 && v != 0.0) {
        return format!("{v:.0e}");
    }
    let (scaled, suffix) = [(1e12, "T"), (1e9, "G"), (1e6, "M"), (1e3, "k")]
        .into_iter()
        .find(|(unit, _)| a >= *unit)
        .map_or((v, ""), |(unit, suffix)| (v / unit, suffix));
    let places = match scaled.abs() {
        x if x >= 100.0 => 0,
        x if x >= 10.0 => 1,
        _ => 2,
    };
    let text = format!("{scaled:.places$}");
    let text = if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        &text
    };
    format!("{text}{suffix}")
}

/// A tick on a whole-number axis (counts, an integer column) at `level` of detail: 0
/// the whole number in the table's `format`, 1 as [`axis_label_at`] shortens it.
pub fn whole_axis_label_at(
    v: f64,
    level: usize,
    format: &crate::numfmt::NumberFormat,
) -> Option<String> {
    match level {
        0 => Some(format_bar_value(v.round(), true, format)),
        level => axis_label_at(v.round(), level),
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

/// The table's format for `column` when it holds whole numbers, for an axis's ticks;
/// `None` for any other column.
pub fn whole_number_format(
    settings: &crate::numfmt::NumberFormatSettings,
    schema: &Schema,
    column: &str,
) -> Option<crate::numfmt::NumberFormat> {
    let dtype = schema.get(column)?;
    dtype
        .is_integer()
        .then(|| table_number_format(settings, column, dtype))
}

/// The first column's format when every one of `columns` holds whole numbers.
pub fn whole_numbers_format(
    settings: &crate::numfmt::NumberFormatSettings,
    schema: &Schema,
    columns: &[String],
) -> Option<crate::numfmt::NumberFormat> {
    let formats: Option<Vec<_>> = columns
        .iter()
        .map(|c| whole_number_format(settings, schema, c))
        .collect();
    formats?.into_iter().next()
}

/// The format the table prints a count in.
pub fn count_format(settings: &crate::numfmt::NumberFormatSettings) -> crate::numfmt::NumberFormat {
    table_number_format(settings, "Count", &DataType::UInt64)
}

/// An x value as the date and time it stands for, when `kind` is a date or datetime.
fn x_datetime(v: f64, kind: XAxisTemporalKind) -> Option<NaiveDateTime> {
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
fn x_time(v: f64) -> Option<NaiveTime> {
    let nsecs = v.trunc() as u64;
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nsecs / 1_000_000_000) as u32,
        (nsecs % 1_000_000_000) as u32,
    )
}

/// Format x-axis tick: dates/datetimes/times when kind is temporal, else numeric. Used by chart widget and export.
pub fn format_x_axis_label(v: f64, kind: XAxisTemporalKind) -> String {
    x_axis_label_at(v, kind, (v, v), 0).unwrap_or_else(|| format_axis_label(v))
}

/// An x tick at `level` of detail, 0 the fullest, or `None` past the shortest form.
/// A narrow axis steps down until its labels fit: a date to year-month and then the
/// year, or to month-day when both ends of the axis, `bounds`, fall in one year; a
/// datetime first to its date, or to the minute when the axis spans one day; a time
/// to the minute.
pub fn x_axis_label_at(
    v: f64,
    kind: XAxisTemporalKind,
    bounds: (f64, f64),
    level: usize,
) -> Option<String> {
    if kind == XAxisTemporalKind::Numeric {
        return axis_label_at(v, level);
    }
    if kind == XAxisTemporalKind::Time {
        let pattern = ["%H:%M:%S", "%H:%M"].get(level)?;
        return Some(match x_time(v) {
            Some(t) => t.format(pattern).to_string(),
            None => axis_label_at(v, level)?,
        });
    }
    let Some(at) = x_datetime(v, kind) else {
        return axis_label_at(v, level);
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
}

/// Which values a histogram, box plot or KDE draws. Outliers far from the body squash
/// it into a bin or two; a percentile range leaves them out and says how many.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
/// sample, and how many values a range left out. Empty when it shows every row and
/// every value.
pub fn chart_notes(rows: &RowsRead, clipped: Option<&Clipped>) -> Vec<String> {
    let mut notes = Vec::new();
    if let Some(n) = rows.sample_size {
        notes.push(format!(
            "sample of {} of {} rows",
            crate::numfmt::group_chrome(n),
            crate::discover::format_rows(rows.total_rows)
        ));
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
    let read = crate::statistics::analysis_rows(
        &lf,
        sampling.limit,
        sampling.known_total,
        sampling.seed,
        sampling.streaming,
    )?;
    let rows = RowsRead {
        total_rows: read.total_rows,
        sample_size: read.sample_size,
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
#[derive(Clone)]
pub struct HistogramBin {
    pub center: f64,
    pub count: f64,
}

/// Histogram data for a single column.
#[derive(Clone)]
pub struct HistogramData {
    pub column: String,
    pub bins: Vec<HistogramBin>,
    pub x_min: f64,
    pub x_max: f64,
    pub max_count: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
}

/// KDE series and bounds.
#[derive(Clone)]
pub struct KdeSeries {
    pub name: String,
    pub points: Vec<(f64, f64)>,
}

#[derive(Clone)]
pub struct KdeData {
    pub series: Vec<KdeSeries>,
    pub x_min: f64,
    pub x_max: f64,
    pub y_max: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
}

/// Box plot stats for a column.
#[derive(Clone)]
pub struct BoxPlotStats {
    pub name: String,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
}

#[derive(Clone)]
pub struct BoxPlotData {
    pub stats: Vec<BoxPlotStats>,
    pub y_min: f64,
    pub y_max: f64,
    pub rows: RowsRead,
    pub clipped: Option<Clipped>,
}

/// Heatmap data for two numeric columns.
#[derive(Clone)]
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
pub fn prepare_chart_data(
    lf: &LazyFrame,
    schema: &Schema,
    x_column: &str,
    y_columns: &[String],
    sampling: &ChartSampling,
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
    let (columns_values, rows) = read_values(lf, &[column], sampling)?;
    let mut values = columns_values.into_iter().next().unwrap_or_default();
    let outside = sort_and_clip(&mut values, range);
    let clipped = clipped(range, outside);
    let data = |bins: Vec<HistogramBin>, x_min, x_max, max_count| HistogramData {
        column: column.to_string(),
        bins,
        x_min,
        x_max,
        max_count,
        rows,
        clipped,
    };
    let (x_min, x_max) = match (values.first(), values.last()) {
        (Some(a), Some(b)) => (*a, *b),
        _ => return Ok(data(Vec::new(), 0.0, 1.0, 0.0)),
    };
    let span = (x_max - x_min).abs();
    let bin_count = bins.max(1);
    if span <= f64::EPSILON {
        let count = values.len() as f64;
        return Ok(data(
            vec![HistogramBin {
                center: x_min,
                count,
            }],
            x_min - 0.5,
            x_max + 0.5,
            count,
        ));
    }
    let bin_width = span / bin_count as f64;
    let mut counts = vec![0.0_f64; bin_count];
    for v in values {
        let idx = (((v - x_min) / bin_width).floor().max(0.0) as usize).min(bin_count - 1);
        counts[idx] += 1.0;
    }
    let bins: Vec<HistogramBin> = counts
        .iter()
        .enumerate()
        .map(|(i, count)| HistogramBin {
            center: x_min + (i as f64 + 0.5) * bin_width,
            count: *count,
        })
        .collect();
    let max_count = counts.iter().cloned().fold(0.0_f64, f64::max);
    Ok(data(bins, x_min, x_max, max_count))
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
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    for (column, mut values) in col_refs.iter().zip(columns_values) {
        outside += sort_and_clip(&mut values, range);
        let (min, max) = match (values.first(), values.last()) {
            (Some(a), Some(b)) => (*a, *b),
            _ => continue,
        };
        y_min = y_min.min(min);
        y_max = y_max.max(max);
        stats.push(BoxPlotStats {
            name: (*column).to_string(),
            min,
            q1: quantile(&values, 0.25),
            median: quantile(&values, 0.5),
            q3: quantile(&values, 0.75),
            max,
        });
    }
    if stats.is_empty() {
        (y_min, y_max) = (0.0, 1.0);
    } else if y_max <= y_min {
        y_max = y_min + 1.0;
    }
    Ok(BoxPlotData {
        stats,
        y_min,
        y_max,
        rows,
        clipped: clipped(range, outside),
    })
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
    let mut all_x_min = f64::INFINITY;
    let mut all_x_max = f64::NEG_INFINITY;
    let mut all_y_max = f64::NEG_INFINITY;
    for (column, mut values) in col_refs.iter().zip(columns_values) {
        outside += sort_and_clip(&mut values, range);
        let (min, max) = match (values.first(), values.last()) {
            (Some(a), Some(b)) => (*a, *b),
            _ => continue,
        };
        let base_bw = kde_bandwidth(&values);
        let bandwidth = (base_bw * bandwidth_factor).max(f64::EPSILON);
        let x_start = min - 3.0 * bandwidth;
        let x_end = max + 3.0 * bandwidth;
        let samples = 200_usize;
        let step = (x_end - x_start) / (samples.saturating_sub(1).max(1) as f64);
        let inv = 1.0 / ((values.len() as f64) * bandwidth * (2.0 * PI).sqrt());
        let mut points = Vec::with_capacity(samples);
        for i in 0..samples {
            let x = x_start + i as f64 * step;
            let mut sum = 0.0;
            for &v in &values {
                let u = (x - v) / bandwidth;
                sum += (-0.5 * u * u).exp();
            }
            let y = inv * sum;
            all_y_max = all_y_max.max(y);
            points.push((x, y));
        }
        all_x_min = all_x_min.min(x_start);
        all_x_max = all_x_max.max(x_end);
        series.push(KdeSeries {
            name: (*column).to_string(),
            points,
        });
    }
    if series.is_empty() {
        (all_x_min, all_x_max, all_y_max) = (0.0, 1.0, 1.0);
    }
    if all_x_max <= all_x_min {
        all_x_max = all_x_min + 1.0;
    }
    if all_y_max <= 0.0 {
        all_y_max = 1.0;
    }
    Ok(KdeData {
        series,
        x_min: all_x_min,
        x_max: all_x_max,
        y_max: all_y_max,
        rows,
        clipped: clipped(range, outside),
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

/// One bar: its category (`None` for a null category) and its value.
#[derive(Clone, Debug, PartialEq)]
pub struct Bar {
    pub label: Option<String>,
    pub value: f64,
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
}

impl BarData {
    /// The format the table prints the value column in, or plain where it shows the
    /// column unformatted.
    pub fn value_format(
        &self,
        settings: &crate::numfmt::NumberFormatSettings,
    ) -> crate::numfmt::NumberFormat {
        match settings.formatter_for(&self.value_column, &self.value_dtype) {
            crate::numfmt::CellFormatter::Number(format) => format,
            crate::numfmt::CellFormatter::Passthrough => crate::numfmt::NumberFormat::PLAIN,
        }
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
        return format_axis_label(v);
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
    let labels_series = categories.cast(&DataType::String)?;
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
        },
        value_dtype: DataType::UInt64,
        counted: sampling.limit.is_some_and(|n| total > n).then_some(total),
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
    let labels_series = categories.cast(&DataType::String)?;
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
fn count_frame(df: &DataFrame, category: &str, max_categories: usize) -> Result<Counted> {
    let mut tally = Tally::new(category, max_categories);
    tally.observe(&df.select([category])?)?;
    Ok(tally.finish()?)
}

/// What a count found: one row per category with its rows in [`COUNT_COLUMN`] (none
/// when the view has no rows), and the rows counted; or more categories than it keeps.
#[derive(Clone)]
enum Counted {
    All {
        counts: Option<DataFrame>,
        rows: usize,
    },
    TooMany,
}

const COUNT_COLUMN: &str = "__datui_bar_count";

/// Counts held per view: enough to go back and forth between a few categories, each
/// up to [`COUNT_CATEGORY_CAP`] rows.
const HELD_COUNTS: usize = 4;

/// Rows piled up unmerged before a merge, at least: a run of new categories costs a
/// merge now and then rather than one per batch.
const MERGE_AFTER: usize = 1 << 16;

/// Rows per category, added up batch by batch. Holds one row per category and the
/// batches since the last merge, never the rows themselves.
struct Tally {
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
    fn new(category: &str, max: usize) -> Self {
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
    fn observe(&mut self, batch: &DataFrame) -> PolarsResult<bool> {
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

    fn finish(self) -> PolarsResult<Counted> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Each form a narrow axis steps down to, and the full form unchanged.
    #[test]
    fn axis_labels_step_down_to_shorter_forms() {
        let at = |v, level| axis_label_at(v, level);
        assert_eq!(at(12345.0, 0).as_deref(), Some("12345.00"));
        let compact: Vec<_> = [12345.0, -1500.0, 5.0, 0.05, 0.0, 2.5e9, 0.001]
            .map(|v| at(v, 1).unwrap())
            .into();
        assert_eq!(
            compact,
            ["12.3k", "-1.5k", "5", "0.05", "0", "2.5G", "1e-3"]
        );
        assert_eq!(at(1.0, 2), None);

        // 2020-01-01 and 2024-12-31 in days; the same instants in microseconds.
        let (lo, hi) = (18262.0, 20088.0);
        let date = |v, level| x_axis_label_at(v, XAxisTemporalKind::Date, (lo, hi), level);
        let forms: Vec<_> = (0..).map_while(|level| date(hi, level)).collect();
        assert_eq!(forms, ["2024-12-31", "2024-12", "2024"]);
        assert_eq!(
            format_x_axis_label(hi, XAxisTemporalKind::Date),
            "2024-12-31"
        );
        // Inside one year, month and day tell the ticks apart.
        let date = |v, level| x_axis_label_at(v, XAxisTemporalKind::Date, (lo, lo + 30.0), level);
        assert_eq!(date(lo, 1).as_deref(), Some("01-01"));

        let us = 86_400.0 * 1e6;
        let kind = XAxisTemporalKind::DatetimeUs;
        let at = |v, bounds, level| x_axis_label_at(v, kind, bounds, level);
        let forms: Vec<_> = (0..)
            .map_while(|level| at(lo * us, (lo * us, hi * us), level))
            .collect();
        assert_eq!(forms, ["2020-01-01 00:00", "2020-01-01", "2020-01", "2020"]);
        // Inside one day, the time of day.
        let day = (lo * us, lo * us + 3600e6);
        assert_eq!(at(lo * us + 3600e6, day, 1).as_deref(), Some("01:00"));
    }

    fn all_rows() -> ChartSampling {
        ChartSampling::rows(Some(10_000))
    }

    fn xy(lf: &LazyFrame, x: &str, ys: &[&str], sampling: &ChartSampling) -> ChartDataResult {
        let schema = lf.clone().collect_schema().unwrap();
        let ys: Vec<String> = ys.iter().map(|s| s.to_string()).collect();
        prepare_chart_data(lf, schema.as_ref(), x, &ys, sampling).unwrap()
    }

    #[test]
    fn prepare_empty_y_columns() {
        let lf = df!("x" => &[1.0_f64, 2.0], "y" => &[10.0, 20.0])
            .unwrap()
            .lazy();
        let result = xy(&lf, "x", &[], &all_rows());
        assert!(result.series.is_empty());
        assert_eq!(result.x_axis_kind, XAxisTemporalKind::Numeric);
    }

    #[test]
    fn prepare_small_data() {
        let lf = df!(
            "x" => &[1.0_f64, 2.0, 3.0],
            "a" => &[10.0_f64, 20.0, 30.0],
            "b" => &[100.0_f64, 200.0, 300.0]
        )
        .unwrap()
        .lazy();
        let result = xy(&lf, "x", &["a", "b"], &all_rows());
        assert_eq!(result.series.len(), 2);
        assert_eq!(
            result.series[0],
            vec![(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)]
        );
        assert_eq!(
            result.series[1],
            vec![(1.0, 100.0), (2.0, 200.0), (3.0, 300.0)]
        );
        assert_eq!(result.x_axis_kind, XAxisTemporalKind::Numeric);
        assert_eq!(
            result.rows,
            RowsRead {
                total_rows: 3,
                sample_size: None
            },
            "every row read: nothing to say"
        );
        assert!(chart_notes(&result.rows, None).is_empty());
    }

    #[test]
    fn prepare_skips_nan() {
        let lf = df!(
            "x" => &[1.0_f64, 2.0, 3.0],
            "y" => &[10.0_f64, f64::NAN, 30.0]
        )
        .unwrap()
        .lazy();
        let result = xy(&lf, "x", &["y"], &all_rows());
        assert_eq!(result.series[0], vec![(1.0, 10.0), (3.0, 30.0)]);
    }

    #[test]
    fn prepare_missing_x_column_errors() {
        let lf = df!("x" => &[1.0_f64], "y" => &[2.0_f64]).unwrap().lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let result =
            prepare_chart_data(&lf, schema.as_ref(), "missing", &["y".into()], &all_rows());
        assert!(result.is_err());
    }

    /// Over the limit, a chart reads a sample spread across the table, not its head,
    /// and says how many rows it read of how many.
    #[test]
    fn over_the_limit_a_chart_reads_a_spread_sample_and_says_so() {
        let n = 50_000_i64;
        let lf = df!(
            "x" => (0..n).collect::<Vec<_>>(),
            "y" => (0..n).map(|v| v * 2).collect::<Vec<_>>()
        )
        .unwrap()
        .lazy();
        let result = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(1_000)));
        let points = &result.series[0];
        assert_eq!(points.len(), 1_000);
        let last_x = points.last().unwrap().0;
        assert!(
            last_x > (n as f64) * 0.9,
            "the sample reaches the end of the table, got {last_x}"
        );
        assert_eq!(
            result.rows,
            RowsRead {
                total_rows: n as usize,
                sample_size: Some(1_000)
            }
        );
        assert_eq!(
            chart_notes(&result.rows, None),
            ["sample of 1,000 of 50k rows"]
        );

        // No limit reads every row.
        let every = xy(&lf, "x", &["y"], &ChartSampling::rows(None));
        assert_eq!(every.series[0].len(), n as usize);
        assert_eq!(every.rows.sample_size, None);
    }

    /// One Parquet file is sampled in runs across it: the chart's columns stay a plan
    /// the sampler can seek in, so the chart does not read the file to draw from it.
    #[test]
    fn a_parquet_file_is_sampled_in_runs() {
        let dir = tempfile::tempdir().unwrap();
        let n = 100_000_i64;
        let mut df = df!(
            "id" => (0..n).collect::<Vec<_>>(),
            "fare" => (0..n).map(|v| v as f64).collect::<Vec<_>>(),
            "other" => vec!["x"; n as usize]
        )
        .unwrap();
        let path = dir.path().join("trips.parquet");
        ParquetWriter::new(std::fs::File::create(&path).unwrap())
            .with_row_group_size(Some(1_000))
            .finish(&mut df)
            .unwrap();
        let lf =
            LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default())
                .unwrap();
        assert!(crate::statistics::slices_reach_into_the_scan(
            &lf.clone().select([col("id"), col("fare")])
        ));
        let data = prepare_histogram_data(
            &lf,
            "fare",
            10,
            ValueRange::All,
            &ChartSampling::rows(Some(2_000)),
        )
        .unwrap();
        assert_eq!(
            data.rows,
            RowsRead {
                total_rows: n as usize,
                sample_size: Some(2_000)
            }
        );
        assert!(data.x_max > 90_000.0, "reaches the end: {}", data.x_max);
    }

    /// The same seed and size draw the same rows; the chart and the analysis tools
    /// share the sampler, so they agree on what a sample is.
    #[test]
    fn the_sample_is_seeded() {
        let lf = df!("x" => (0..20_000_i64).collect::<Vec<_>>(), "y" => vec![1.0_f64; 20_000])
            .unwrap()
            .lazy();
        let a = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(500)));
        let b = xy(&lf, "x", &["y"], &ChartSampling::rows(Some(500)));
        assert_eq!(a.series, b.series);
        let other = ChartSampling {
            seed: 7,
            ..ChartSampling::rows(Some(500))
        };
        let c = xy(&lf, "x", &["y"], &other);
        assert_ne!(a.series, c.series);
    }

    /// Another option over columns already read draws from the rows held, without
    /// reading the file again; a new column is read with the held ones, and another
    /// sample size reads afresh.
    #[test]
    fn rows_already_read_are_not_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fares.csv");
        let write = |a: i64, b: i64| {
            let rows: String = (0..100).map(|i| format!("{},{}\n", i + a, i + b)).collect();
            std::fs::write(&path, format!("a,b\n{rows}")).unwrap();
        };
        write(0, 0);
        let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        let sampling = ChartSampling::rows(Some(10_000));
        let first = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &sampling).unwrap();
        assert_eq!(first.x_min, 0.0);

        write(1_000, 1_000);
        let held =
            prepare_histogram_data(&lf, "a", 5, ValueRange::Percentile1To99, &sampling).unwrap();
        assert!(
            held.x_max < 100.0,
            "drawn from the rows held: {}",
            held.x_max
        );
        let boxed = prepare_box_plot_data(&lf, &["a"], ValueRange::All, &sampling).unwrap();
        assert_eq!(boxed.stats[0].max, 99.0);

        let with_b = prepare_histogram_data(&lf, "b", 10, ValueRange::All, &sampling).unwrap();
        assert_eq!(with_b.x_min, 1_000.0, "b was not held: read");
        let a_again = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &sampling).unwrap();
        assert_eq!(a_again.x_min, 1_000.0, "read along with b");

        write(5_000, 5_000);
        let other_size = ChartSampling {
            limit: Some(50),
            ..sampling.clone()
        };
        let resampled = prepare_histogram_data(&lf, "a", 10, ValueRange::All, &other_size).unwrap();
        assert!(resampled.x_min >= 5_000.0, "another size reads afresh");
    }

    /// A line is drawn in X order whatever order the rows are in, as a pivot leaves
    /// them.
    #[test]
    fn points_come_in_x_order() {
        let lf = df!(
            "year" => &[2001_i64, 1999, 2003, 2000, 2002],
            "count" => &[1.0_f64, 2.0, 3.0, 4.0, 5.0]
        )
        .unwrap()
        .lazy();
        let result = xy(&lf, "year", &["count"], &all_rows());
        let xs: Vec<f64> = result.series[0].iter().map(|p| p.0).collect();
        assert_eq!(xs, [1999.0, 2000.0, 2001.0, 2002.0, 2003.0]);
        assert_eq!(result.series[0][0], (1999.0, 2.0));
        assert!(result.breaks[0].is_empty());
    }

    /// Picking the X column as a Y series charts it against itself rather than failing
    /// on a repeated column.
    #[test]
    fn x_as_a_y_series_charts_rather_than_failing() {
        let lf = df!("x" => &[1.0_f64, 2.0], "y" => &[3.0_f64, 4.0])
            .unwrap()
            .lazy();
        let result = xy(&lf, "x", &["x", "y"], &all_rows());
        assert_eq!(result.series[0], vec![(1.0, 1.0), (2.0, 2.0)]);
        assert_eq!(result.series[1], vec![(1.0, 3.0), (2.0, 4.0)]);
    }

    /// A Y column the frame does not have is an error to show, not an empty chart.
    #[test]
    fn a_missing_y_column_is_an_error() {
        let lf = df!("x" => &[1.0_f64], "y" => &[2.0_f64]).unwrap().lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let result = prepare_chart_data(&lf, schema.as_ref(), "x", &["gone".into()], &all_rows());
        assert!(result.is_err());
    }

    /// One series' nulls drop that series' points only; a null X drops the row; a
    /// line breaks at a gap instead of bridging it.
    #[test]
    fn nulls_drop_per_series_and_break_the_line() {
        let lf = df!(
            "year" => &[Some(1880_i64), Some(1881), Some(1882), None, Some(1883), Some(1884)],
            "emma" => &[Some(10.0_f64), Some(11.0), Some(12.0), Some(99.0), Some(13.0), Some(14.0)],
            "jennifer" => &[None, None, Some(5.0_f64), Some(99.0), None, Some(7.0)]
        )
        .unwrap()
        .lazy();
        let result = xy(&lf, "year", &["emma", "jennifer"], &all_rows());
        assert_eq!(
            result.series[0],
            vec![
                (1880.0, 10.0),
                (1881.0, 11.0),
                (1882.0, 12.0),
                (1883.0, 13.0),
                (1884.0, 14.0)
            ],
            "Emma keeps the years Jennifer is missing; the null year is gone"
        );
        assert!(result.breaks[0].is_empty());
        assert_eq!(result.series[1], vec![(1882.0, 5.0), (1884.0, 7.0)]);
        assert_eq!(result.breaks[1], [1], "1883 is missing: the line breaks");
        assert_eq!(
            segments(&result.series[1], &result.breaks[1]),
            vec![&[(1882.0, 5.0)][..], &[(1884.0, 7.0)][..]]
        );
    }

    #[test]
    fn segments_split_at_breaks() {
        let points = [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)];
        assert_eq!(segments(&points, &[]), vec![&points[..]]);
        assert_eq!(
            segments(&points, &[1, 3]),
            vec![&points[..1], &points[1..3], &points[3..]]
        );
        assert!(segments(&[], &[]).is_empty());
    }

    /// Temporal X charts as its ordinal, nulls dropped the same way.
    #[test]
    fn a_date_x_is_ordinal() {
        let lf = df!("d" => &[Some(1_i32), None, Some(0)], "y" => &[1.0_f64, 2.0, 3.0])
            .unwrap()
            .lazy()
            .with_column(col("d").cast(DataType::Date));
        let result = xy(&lf, "d", &["y"], &all_rows());
        assert_eq!(result.x_axis_kind, XAxisTemporalKind::Date);
        assert_eq!(result.series[0], vec![(0.0, 3.0), (1.0, 1.0)]);
    }

    fn with_outliers() -> LazyFrame {
        // 1..=100 and two far outliers.
        let mut v: Vec<f64> = (1..=100).map(f64::from).collect();
        v.push(-10_000.0);
        v.push(50_000.0);
        df!("fare" => v).unwrap().lazy()
    }

    /// The percentile range leaves the tails out of a histogram and counts them.
    #[test]
    fn a_histogram_range_clips_the_tails_and_counts_them() {
        let lf = with_outliers();
        let all = prepare_histogram_data(&lf, "fare", 10, ValueRange::All, &all_rows()).unwrap();
        assert_eq!(all.x_min, -10_000.0);
        assert!(all.clipped.is_none());

        let clipped =
            prepare_histogram_data(&lf, "fare", 10, ValueRange::Percentile1To99, &all_rows())
                .unwrap();
        // Of 102 values the 1st percentile falls at 1.01 and the 99th at 99.99: each
        // tail loses its outlier and the value next to it.
        assert_eq!((clipped.x_min, clipped.x_max), (2.0, 99.0));
        let outside = clipped.clipped.unwrap().outside;
        assert_eq!(outside, 4);
        let counted: f64 = clipped.bins.iter().map(|b| b.count).sum();
        assert_eq!(counted as usize + outside, 102);
        assert_eq!(
            chart_notes(&clipped.rows, clipped.clipped.as_ref()),
            ["4 values outside p1-p99"]
        );
    }

    #[test]
    fn box_plot_and_kde_take_the_range_too() {
        let lf = with_outliers();
        let boxed = prepare_box_plot_data(&lf, &["fare"], ValueRange::Percentile1To99, &all_rows())
            .unwrap();
        assert!(boxed.stats[0].min > 0.0 && boxed.stats[0].max <= 100.0);
        assert!(boxed.clipped.unwrap().outside >= 2);

        let kde = prepare_kde_data(
            &lf,
            &["fare"],
            1.0,
            ValueRange::Percentile1To99,
            &all_rows(),
        )
        .unwrap();
        assert!(kde.x_min > -1_000.0 && kde.x_max < 1_000.0);
        assert!(kde.clipped.unwrap().outside >= 2);

        let whole = prepare_box_plot_data(&lf, &["fare"], ValueRange::All, &all_rows()).unwrap();
        assert_eq!(whole.stats[0].min, -10_000.0);
    }

    /// A value column read on its own: another column's nulls do not remove its values.
    #[test]
    fn a_histogram_keeps_every_value_of_its_column() {
        let lf = df!("a" => &[Some(1.0_f64), Some(2.0), None, Some(4.0)])
            .unwrap()
            .lazy();
        let data = prepare_histogram_data(&lf, "a", 5, ValueRange::All, &all_rows()).unwrap();
        let counted: f64 = data.bins.iter().map(|b| b.count).sum();
        assert_eq!(counted, 3.0);
    }

    #[test]
    fn prepare_x_range_numeric() {
        let lf = df!("x" => &[10.0_f64, 20.0, 5.0, 30.0]).unwrap().lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let r = prepare_chart_x_range(&lf, schema.as_ref(), "x", &all_rows()).unwrap();
        assert_eq!(r.x_min, 5.0);
        assert_eq!(r.x_max, 30.0);
        assert_eq!(r.x_axis_kind, XAxisTemporalKind::Numeric);
    }

    #[test]
    fn prepare_x_range_empty_returns_placeholder() {
        let lf = df!("x" => &[1.0_f64]).unwrap().lazy().slice(0, 0);
        let schema = lf.clone().collect_schema().unwrap();
        let r = prepare_chart_x_range(&lf, schema.as_ref(), "x", &all_rows()).unwrap();
        assert_eq!(r.x_min, 0.0);
        assert_eq!(r.x_max, 1.0);
    }

    fn bars(lf: &LazyFrame, order: BarOrder, cap: usize) -> BarData {
        prepare_bar_data(lf, "carrier", "delay", order, cap, &all_rows()).unwrap()
    }

    fn labels(data: &BarData) -> Vec<Option<&str>> {
        data.bars.iter().map(|b| b.label.as_deref()).collect()
    }

    /// Bars come largest first, or in the category's own order; past the cap the rest
    /// are counted rather than kept.
    #[test]
    fn bars_order_by_value_or_label_and_cap_the_rest() {
        let lf = df!(
            "carrier" => &["UA", "AA", "DL", "B6", "AS"],
            "delay" => &[3.5_f64, 0.4, 1.6, 9.5, -9.9]
        )
        .unwrap()
        .lazy();
        let by_value = bars(&lf, BarOrder::Value, BAR_CAP);
        assert_eq!(
            labels(&by_value),
            [Some("B6"), Some("UA"), Some("DL"), Some("AA"), Some("AS")]
        );
        assert_eq!(by_value.bars[4].value, -9.9);
        assert_eq!(by_value.more, 0);

        let by_label = bars(&lf, BarOrder::Label, BAR_CAP);
        assert_eq!(
            labels(&by_label),
            [Some("AA"), Some("AS"), Some("B6"), Some("DL"), Some("UA")]
        );

        let capped = bars(&lf, BarOrder::Value, 2);
        assert_eq!(labels(&capped), [Some("B6"), Some("UA")]);
        assert_eq!(capped.more, 3, "the three smallest are counted, not drawn");
        let capped = bars(&lf, BarOrder::Label, 2);
        assert_eq!(labels(&capped), [Some("AA"), Some("AS")]);
        assert_eq!(capped.more, 3);
    }

    /// An integer category orders as numbers, not text; a null category is a bar of its
    /// own, last in label order; a null value leaves its category out and is counted.
    #[test]
    fn bar_categories_keep_their_type_and_nulls_are_counted() {
        let lf = df!(
            "carrier" => &[Some(10_i64), Some(9), None, Some(100), Some(2)],
            "delay" => &[Some(1.0_f64), Some(2.0), Some(3.0), None, Some(1.0)]
        )
        .unwrap()
        .lazy();
        let data = bars(&lf, BarOrder::Label, BAR_CAP);
        assert_eq!(labels(&data), [Some("2"), Some("9"), Some("10"), None]);
        assert_eq!(data.no_value, 1, "100 has no value");

        let data = bars(&lf, BarOrder::Value, BAR_CAP);
        assert_eq!(
            labels(&data),
            [None, Some("9"), Some("10"), Some("2")],
            "ties keep table order"
        );
    }

    /// A category that repeats is refused with the way out, not summed or averaged.
    #[test]
    fn a_repeated_category_is_refused() {
        let lf = df!(
            "species" => &["Adelie", "Adelie", "Gentoo"],
            "body_mass_g" => &[3750_i64, 3800, 5000]
        )
        .unwrap()
        .lazy();
        let err = prepare_bar_data(
            &lf,
            "species",
            "body_mass_g",
            BarOrder::Value,
            BAR_CAP,
            &all_rows(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("species repeats: 2 categories in 3 rows"),
            "{err}"
        );
        assert!(
            err.contains("SELECT species, AVG(body_mass_g) FROM df GROUP BY species"),
            "SQL first: {err}"
        );
        assert!(
            err.contains("(or select avg body_mass_g by species)"),
            "{err}"
        );

        // A name SQL cannot read bare is quoted, and the q form, which cannot, is left out.
        let lf = df!("Species" => &["a", "a"], "mass g" => &[1_i64, 2])
            .unwrap()
            .lazy();
        let err = prepare_bar_data(
            &lf,
            "Species",
            "mass g",
            BarOrder::Value,
            BAR_CAP,
            &all_rows(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.ends_with(
                r#"SELECT "Species", AVG("mass g") FROM df GROUP BY "Species", or choose Count for the rows per category"#
            ),
            "{err}"
        );
    }

    /// Another order draws from the rows already read: the file is not read again.
    #[test]
    fn a_new_bar_order_does_not_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delays.csv");
        std::fs::write(&path, "carrier,delay\nUA,3.5\nAA,0.4\n").unwrap();
        let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        let sampling = all_rows();
        let first =
            prepare_bar_data(&lf, "carrier", "delay", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(labels(&first), [Some("UA"), Some("AA")]);
        std::fs::write(&path, "carrier,delay\nZZ,1.0\n").unwrap();
        let again =
            prepare_bar_data(&lf, "carrier", "delay", BarOrder::Label, BAR_CAP, &sampling).unwrap();
        assert_eq!(
            labels(&again),
            [Some("AA"), Some("UA")],
            "from the rows held"
        );
    }

    /// Booleans and categoricals are categories too.
    #[test]
    fn booleans_and_categoricals_chart_as_categories() {
        let lf = df!("flag" => &[true, false], "n" => &[5_i64, 7])
            .unwrap()
            .lazy();
        let data =
            prepare_bar_data(&lf, "flag", "n", BarOrder::Label, BAR_CAP, &all_rows()).unwrap();
        assert_eq!(labels(&data), [Some("false"), Some("true")]);

        let lf = df!("kind" => &["b", "a"], "n" => &[5_i64, 7])
            .unwrap()
            .lazy()
            .with_column(col("kind").cast(DataType::from_categories(Categories::global())));
        let schema = lf.clone().collect_schema().unwrap();
        assert!(is_category_dtype(schema.get("kind").unwrap()));
        let data =
            prepare_bar_data(&lf, "kind", "n", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
        assert_eq!(labels(&data), [Some("a"), Some("b")]);
        assert!(!is_category_dtype(&DataType::Float64));
        assert!(is_category_dtype(&DataType::UInt8));
    }

    /// Bar values print in the table's number format: an integer column whole, any
    /// other to the format's places or two, the same for every bar.
    #[test]
    fn bar_values_follow_the_table_number_format() {
        use crate::numfmt::NumberFormat;
        let plain = NumberFormat::PLAIN;
        let thousands = NumberFormat::preset("thousands").unwrap();
        let european = NumberFormat::preset("european").unwrap();
        assert_eq!(format_bar_value(1_234_567.0, true, &plain), "1234567");
        assert_eq!(format_bar_value(1_234_567.0, true, &thousands), "1,234,567");
        assert_eq!(format_bar_value(22.0, false, &plain), "22.00");
        assert_eq!(format_bar_value(-9.9296, false, &plain), "-9.93");
        assert_eq!(format_bar_value(4213.7, false, &thousands), "4,213.70");
        assert_eq!(format_bar_value(4213.7, false, &european), "4.213,70");
        let one_place = NumberFormat {
            float_precision: Some(1),
            ..thousands
        };
        assert_eq!(format_bar_value(4213.74, false, &one_place), "4,213.7");
        assert_eq!(format_bar_value(0.0, false, &plain), "0.00");
        assert_eq!(format_bar_value(0.001, false, &plain), "1.00e-3");

        let lf = df!("carrier" => &["UA", "AA"], "delay" => &[1234.5_f64, 7.0])
            .unwrap()
            .lazy();
        let data = bars(&lf, BarOrder::Value, BAR_CAP);
        let mut settings = crate::numfmt::NumberFormatSettings {
            format: NumberFormat::preset("thousands").unwrap(),
            ..Default::default()
        };
        assert_eq!(data.value_labels(&settings), ["1,234.50", "7.00"]);
        settings.enabled = false;
        assert_eq!(
            data.value_labels(&settings),
            ["1234.50", "7.00"],
            "F turns it off"
        );
    }

    fn species(n_adelie: usize, n_gentoo: usize, n_chinstrap: usize, n_null: usize) -> LazyFrame {
        let mut species: Vec<Option<&str>> = Vec::new();
        // Interleaved, so no stretch of the table is one species.
        let mut left = [
            (Some("Adelie"), n_adelie),
            (Some("Gentoo"), n_gentoo),
            (Some("Chinstrap"), n_chinstrap),
            (None, n_null),
        ];
        while left.iter().any(|(_, n)| *n > 0) {
            for (name, n) in &mut left {
                if *n > 0 {
                    species.push(*name);
                    *n -= 1;
                }
            }
        }
        df!("species" => species).unwrap().lazy()
    }

    fn counts(data: &BarData) -> Vec<(Option<&str>, f64)> {
        data.bars
            .iter()
            .map(|b| (b.label.as_deref(), b.value))
            .collect()
    }

    /// Count is exact over the whole view, not a count of the sample: more rows than
    /// the sample size are all counted, a null category is a bar of its own, and the
    /// note says the counts are of every row.
    #[test]
    fn counts_are_exact_past_the_sample_size() {
        let lf = species(30_000, 15_000, 4_999, 1);
        let sampling = ChartSampling::rows(Some(1_000));
        let data = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(
            counts(&data),
            [
                (Some("Adelie"), 30_000.0),
                (Some("Gentoo"), 15_000.0),
                (Some("Chinstrap"), 4_999.0),
                (None, 1.0)
            ]
        );
        assert_eq!(data.counted, Some(50_000), "counted past the sample size");
        assert_eq!(data.rows.sample_size, None);
        assert_eq!(data.value_column, "count");
        assert_eq!(
            data.value_labels(&crate::numfmt::NumberFormatSettings {
                format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
                ..Default::default()
            }),
            ["30,000", "15,000", "4,999", "1"],
            "whole numbers"
        );

        let by_label =
            prepare_bar_counts(&lf, "species", BarOrder::Label, BAR_CAP, &sampling).unwrap();
        assert_eq!(
            counts(&by_label),
            [
                (Some("Adelie"), 30_000.0),
                (Some("Chinstrap"), 4_999.0),
                (Some("Gentoo"), 15_000.0),
                (None, 1.0)
            ],
            "the null category last"
        );

        // Every row read, or a view under the sample size: nothing to say.
        let every = prepare_bar_counts(
            &lf,
            "species",
            BarOrder::Value,
            BAR_CAP,
            &ChartSampling::rows(None),
        )
        .unwrap();
        assert_eq!(every.counted, None);
        let small = species(152, 124, 68, 0);
        let data =
            prepare_bar_counts(&small, "species", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
        assert_eq!(
            counts(&data),
            [
                (Some("Adelie"), 152.0),
                (Some("Gentoo"), 124.0),
                (Some("Chinstrap"), 68.0)
            ]
        );
        assert_eq!(data.counted, None);
    }

    /// Equal counts come A to Z; past the bar cap the rest are counted, not drawn; past
    /// the category cap the count stops and says so rather than drawing a part.
    #[test]
    fn counts_cap_their_bars_and_stop_past_the_category_cap() {
        let lf = df!("carrier" => &["UA", "B6", "AA", "AA", "DL", "B6", "AA"])
            .unwrap()
            .lazy();
        let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, 2, &all_rows()).unwrap();
        assert_eq!(counts(&data), [(Some("AA"), 3.0), (Some("B6"), 2.0)]);
        assert_eq!(data.more, 2, "DL and UA are counted, not drawn");
        let data =
            prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &all_rows()).unwrap();
        assert_eq!(
            counts(&data)[2..],
            [(Some("DL"), 1.0), (Some("UA"), 1.0)],
            "ties A to Z"
        );

        let err = count_bars(&lf, "carrier", BarOrder::Value, BAR_CAP, 3, &all_rows())
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "more than 3 categories of carrier: counting stopped. Count by a column with \
             fewer values"
        );
        let data = count_bars(&lf, "carrier", BarOrder::Value, BAR_CAP, 4, &all_rows()).unwrap();
        assert_eq!(data.bars.len(), 4, "four is not more than four");
    }

    /// Batches are added up category by category, merged as they pile up; the read is
    /// told to stop as soon as the categories pass the cap.
    #[test]
    fn a_tally_merges_batches_and_stops_past_its_cap() {
        let batch = |ids: std::ops::Range<i64>| df!("id" => ids.collect::<Vec<_>>()).unwrap();
        let mut tally = Tally::new("id", 200_000);
        assert!(!tally.observe(&batch(0..70_000)).unwrap());
        assert_eq!(tally.merged, 70_000, "merged once the batches pile up");
        assert!(!tally.observe(&batch(0..10)).unwrap());
        let Counted::All { counts, rows } = tally.finish().unwrap() else {
            panic!("under the cap");
        };
        assert_eq!(rows, 70_010);
        let counts = counts.unwrap();
        assert_eq!(counts.height(), 70_000);
        let total: u64 = counts
            .column(COUNT_COLUMN)
            .unwrap()
            .u64()
            .unwrap()
            .sum()
            .unwrap();
        assert_eq!(total, 70_010);

        let mut tally = Tally::new("id", 1_000);
        assert!(
            tally.observe(&batch(0..70_000)).unwrap(),
            "past the cap: stop reading"
        );
        assert!(matches!(tally.finish().unwrap(), Counted::TooMany));
    }

    /// Through the streamed pass: past the cap the read stops and the count says so; a
    /// cancelled count is an error and is not held as the view's counts.
    #[test]
    fn a_streamed_count_stops_past_its_cap_or_when_cancelled() {
        let ids = df!("id" => (0..200_000i64).collect::<Vec<_>>())
            .unwrap()
            .lazy();
        let cancel = Arc::default();
        assert!(matches!(
            stream_counts(&ids, "id", 1_000, &cancel).unwrap(),
            Counted::TooMany
        ));

        let lf = species(30_000, 15_000, 4_999, 1);
        let sampling = ChartSampling::rows(Some(1_000));
        sampling.cancel.store(true, Ordering::Relaxed);
        let err = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "count cancelled");
        assert!(sampling.held.0.lock().unwrap().counts.is_empty());
        sampling.cancel.store(false, Ordering::Relaxed);
        let data = prepare_bar_counts(&lf, "species", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(data.counted, Some(50_000));
    }

    /// When the rows held are the whole view, Count counts them rather than reading
    /// again; a count is held too, so another order does not count again.
    #[test]
    fn counts_come_from_the_rows_held_and_are_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flights.csv");
        std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
        let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        let sampling = all_rows();
        prepare_histogram_data(&lf, "delay", 10, ValueRange::All, &sampling).unwrap();
        std::fs::write(&path, "carrier,delay\nZZ,1\n").unwrap();
        // The rows held have no carrier: read, one count per category.
        let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(counts(&data), [(Some("ZZ"), 1.0)]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flights.csv");
        std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
        let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .finish()
            .unwrap();
        let sampling = all_rows();
        // Refused, carriers repeat; the rows it read stay held.
        assert!(
            prepare_bar_data(&lf, "carrier", "delay", BarOrder::Value, BAR_CAP, &sampling).is_err()
        );
        std::fs::write(&path, "carrier,delay\nZZ,1\n").unwrap();
        let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(
            counts(&data),
            [(Some("UA"), 2.0), (Some("AA"), 1.0)],
            "counted from the rows held"
        );
        // Without the rows, only the count held can say this.
        sampling.held.0.lock().unwrap().rows = None;
        let data = prepare_bar_counts(&lf, "carrier", BarOrder::Label, BAR_CAP, &sampling).unwrap();
        assert_eq!(
            counts(&data),
            [(Some("AA"), 1.0), (Some("UA"), 2.0)],
            "another order from the count held"
        );

        // A view the sample size takes whole is read as a chart reads it, and its rows
        // held for the next chart.
        let sampling = ChartSampling {
            known_total: Some(3),
            ..all_rows()
        };
        std::fs::write(&path, "carrier,delay\nUA,1\nUA,2\nAA,3\n").unwrap();
        let data = prepare_bar_counts(&lf, "carrier", BarOrder::Value, BAR_CAP, &sampling).unwrap();
        assert_eq!(counts(&data), [(Some("UA"), 2.0), (Some("AA"), 1.0)]);
        let holding = sampling.held.0.lock().unwrap();
        let held = holding.rows.as_ref().expect("the rows read are held");
        assert_eq!(held.df.column("carrier").unwrap().len(), 3);
    }
}

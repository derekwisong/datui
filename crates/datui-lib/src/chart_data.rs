//! Prepare chart data from a LazyFrame: read the chart's columns, then turn them into
//! points, bins or statistics.
//!
//! Every chart reads its rows through [`read_columns`]: up to a row limit of them,
//! spread across the table by the sampler the analysis tools use, so a chart shows the
//! table and not its first rows. What was read comes back as [`RowsRead`], so the chart
//! can say when it shows a sample.

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use color_eyre::Result;
use polars::datatypes::{DataType, TimeUnit};
use polars::prelude::*;
use std::f64::consts::PI;

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

/// Format x-axis tick: dates/datetimes/times when kind is temporal, else numeric. Used by chart widget and export.
pub fn format_x_axis_label(v: f64, kind: XAxisTemporalKind) -> String {
    match kind {
        XAxisTemporalKind::Numeric => format_axis_label(v),
        XAxisTemporalKind::Date => {
            const UNIX_EPOCH_CE_DAYS: i32 = 719_163;
            let days = v.trunc() as i32;
            match NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_CE_DAYS.saturating_add(days)) {
                Some(d) => d.format("%Y-%m-%d").to_string(),
                None => format_axis_label(v),
            }
        }
        XAxisTemporalKind::DatetimeUs => DateTime::from_timestamp_micros(v.trunc() as i64)
            .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| format_axis_label(v)),
        XAxisTemporalKind::DatetimeMs => DateTime::from_timestamp_millis(v.trunc() as i64)
            .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| format_axis_label(v)),
        XAxisTemporalKind::DatetimeNs => {
            let millis = (v.trunc() as i64) / 1_000_000;
            DateTime::from_timestamp_millis(millis)
                .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| format_axis_label(v))
        }
        XAxisTemporalKind::Time => {
            let nsecs = v.trunc() as u64;
            let secs = (nsecs / 1_000_000_000) as u32;
            let subsec = (nsecs % 1_000_000_000) as u32;
            match NaiveTime::from_num_seconds_from_midnight_opt(secs, subsec) {
                Some(t) => t.format("%H:%M:%S").to_string(),
                None => format_axis_label(v),
            }
        }
    }
}

/// How a chart reads its rows.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChartSampling {
    /// Rows to read; `None` reads every row.
    pub limit: Option<usize>,
    /// The view's row count when the table already knows it, which saves a count.
    pub known_total: Option<usize>,
    /// The shared analysis seed, so a chart and Describe draw alike.
    pub seed: u64,
    pub streaming: bool,
}

impl ChartSampling {
    /// Up to `limit` rows, with the analysis tools' default seed.
    pub fn rows(limit: Option<usize>) -> Self {
        Self {
            limit,
            known_total: None,
            seed: crate::sampling::Sample::default().seed,
            streaming: false,
        }
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
            Self::Percentile1To99 => "1st-99th pct",
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
fn read_columns(
    lf: &LazyFrame,
    columns: &[&str],
    sampling: &ChartSampling,
) -> Result<(DataFrame, RowsRead)> {
    let mut unique: Vec<&str> = Vec::with_capacity(columns.len());
    for c in columns {
        if !unique.contains(c) {
            unique.push(c);
        }
    }
    let lf = lf
        .clone()
        .select(unique.iter().map(|c| col(*c)).collect::<Vec<_>>());
    let rows = crate::statistics::analysis_rows(
        &lf,
        sampling.limit,
        sampling.known_total,
        sampling.seed,
        sampling.streaming,
    )?;
    Ok((
        rows.df,
        RowsRead {
            total_rows: rows.total_rows,
            sample_size: rows.sample_size,
        },
    ))
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(clipped.x_min > 0.0 && clipped.x_max <= 100.0);
        let outside = clipped.clipped.unwrap().outside;
        assert!(outside >= 2, "both outliers are out, got {outside}");
        let counted: f64 = clipped.bins.iter().map(|b| b.count).sum();
        assert_eq!(counted as usize + outside, 102);
        assert_eq!(
            chart_notes(&clipped.rows, clipped.clipped.as_ref()),
            [format!("{outside} values outside 1st-99th pct")]
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
}

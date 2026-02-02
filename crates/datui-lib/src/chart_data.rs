//! Prepare chart data from LazyFrame: select x/y columns, collect, and convert to (f64, f64) points.

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use color_eyre::Result;
use polars::datatypes::{DataType, TimeUnit};
use polars::prelude::*;
use std::f64::consts::PI;

const CHART_ROW_LIMIT: usize = 10_000;

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

/// Result of loading only the x column: min/max for axis bounds and temporal kind.
pub struct ChartXRangeResult {
    pub x_min: f64,
    pub x_max: f64,
    pub x_axis_kind: XAxisTemporalKind,
}

/// Loads only the x column and returns its min/max (for axis display when no y is selected).
/// Drops nulls and limits to `CHART_ROW_LIMIT` rows. If no valid values, returns (0.0, 1.0).
pub fn prepare_chart_x_range(
    lf: &LazyFrame,
    schema: &Schema,
    x_column: &str,
) -> Result<ChartXRangeResult> {
    let x_dtype = schema
        .get(x_column)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", x_column))?;

    let x_axis_kind = x_axis_temporal_kind(x_dtype);
    let x_expr: Expr = match x_dtype {
        DataType::Datetime(_, _) | DataType::Date | DataType::Time => {
            col(x_column).cast(DataType::Int64)
        }
        _ => col(x_column).cast(DataType::Float64),
    };

    let df = lf
        .clone()
        .select([x_expr])
        .drop_nulls(None)
        .slice(0, CHART_ROW_LIMIT as u32)
        .collect()?;

    let n_rows = df.height();
    if n_rows == 0 {
        return Ok(ChartXRangeResult {
            x_min: 0.0,
            x_max: 1.0,
            x_axis_kind,
        });
    }

    let x_series = df.column(x_column)?;
    let x_f64 = match x_series.dtype() {
        DataType::Int64 => x_series.cast(&DataType::Float64)?,
        _ => x_series.clone(),
    };
    let x_f64 = x_f64.f64()?;

    let mut x_min = f64::INFINITY;
    let mut x_max = f64::NEG_INFINITY;
    for i in 0..n_rows {
        if let Some(v) = x_f64.get(i) {
            if v.is_finite() {
                x_min = x_min.min(v);
                x_max = x_max.max(v);
            }
        }
    }

    let (x_min, x_max) = if x_max >= x_min {
        (x_min, x_max)
    } else {
        (0.0, 1.0)
    };

    Ok(ChartXRangeResult {
        x_min,
        x_max,
        x_axis_kind,
    })
}

/// Result of preparing chart data: series points and x-axis kind for label formatting.
pub struct ChartDataResult {
    pub series: Vec<Vec<(f64, f64)>>,
    pub x_axis_kind: XAxisTemporalKind,
}

/// Histogram bin (center and count).
pub struct HistogramBin {
    pub center: f64,
    pub count: f64,
}

/// Histogram data for a single column.
pub struct HistogramData {
    pub column: String,
    pub bins: Vec<HistogramBin>,
    pub x_min: f64,
    pub x_max: f64,
    pub max_count: f64,
}

/// KDE series and bounds.
pub struct KdeSeries {
    pub name: String,
    pub points: Vec<(f64, f64)>,
}

pub struct KdeData {
    pub series: Vec<KdeSeries>,
    pub x_min: f64,
    pub x_max: f64,
    pub y_max: f64,
}

/// Box plot stats for a column.
pub struct BoxPlotStats {
    pub name: String,
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
}

pub struct BoxPlotData {
    pub stats: Vec<BoxPlotStats>,
    pub y_min: f64,
    pub y_max: f64,
}

/// Heatmap data for two numeric columns.
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
}

/// Prepares chart data from the current LazyFrame.
/// Returns series data and x-axis kind. X is cast to f64 (temporal types as ordinal).
/// Drops nulls and limits to `CHART_ROW_LIMIT` rows.
pub fn prepare_chart_data(
    lf: &LazyFrame,
    schema: &Schema,
    x_column: &str,
    y_columns: &[String],
) -> Result<ChartDataResult> {
    if y_columns.is_empty() {
        return Ok(ChartDataResult {
            series: Vec::new(),
            x_axis_kind: XAxisTemporalKind::Numeric,
        });
    }

    let x_dtype = schema
        .get(x_column)
        .ok_or_else(|| color_eyre::eyre::eyre!("x column '{}' not in schema", x_column))?;

    let x_axis_kind = x_axis_temporal_kind(x_dtype);

    // X expr: cast to Float64; for Date/Datetime/Time cast to Int64 (ordinal), then cast to f64 after collect.
    let x_expr: Expr = match x_dtype {
        DataType::Datetime(_, _) | DataType::Date | DataType::Time => {
            col(x_column).cast(DataType::Int64)
        }
        _ => col(x_column).cast(DataType::Float64),
    };

    let mut select_exprs = vec![x_expr];
    for y in y_columns {
        select_exprs.push(col(y.as_str()).cast(DataType::Float64));
    }

    let df = lf
        .clone()
        .select(select_exprs)
        .drop_nulls(None)
        .slice(0, CHART_ROW_LIMIT as u32)
        .collect()?;

    let n_rows = df.height();
    if n_rows == 0 {
        return Ok(ChartDataResult {
            series: vec![vec![]; y_columns.len()],
            x_axis_kind,
        });
    }

    let x_series = df.column(x_column)?;
    let x_f64 = match x_series.dtype() {
        DataType::Int64 => x_series.cast(&DataType::Float64)?,
        _ => x_series.clone(),
    };
    let x_f64 = x_f64.f64()?;

    let mut series_per_y: Vec<Vec<(f64, f64)>> = vec![Vec::with_capacity(n_rows); y_columns.len()];

    for (yi, y_name) in y_columns.iter().enumerate() {
        let y_series = df.column(y_name.as_str())?.f64()?;
        for i in 0..n_rows {
            let x_val = x_f64.get(i).unwrap_or(0.0);
            let y_val = y_series.get(i).unwrap_or(0.0);
            if x_val.is_finite() && y_val.is_finite() {
                series_per_y[yi].push((x_val, y_val));
            }
        }
    }

    Ok(ChartDataResult {
        series: series_per_y,
        x_axis_kind,
    })
}

fn collect_numeric_values(lf: &LazyFrame, column: &str) -> Result<Vec<f64>> {
    let df = lf
        .clone()
        .select([col(column).cast(DataType::Float64)])
        .drop_nulls(None)
        .slice(0, CHART_ROW_LIMIT as u32)
        .collect()?;
    let series = df.column(column)?.f64()?;
    let mut values = Vec::with_capacity(series.len());
    for i in 0..series.len() {
        if let Some(v) = series.get(i) {
            if v.is_finite() {
                values.push(v);
            }
        }
    }
    Ok(values)
}

fn collect_numeric_pairs(lf: &LazyFrame, x_column: &str, y_column: &str) -> Result<Vec<(f64, f64)>> {
    let df = lf
        .clone()
        .select([
            col(x_column).cast(DataType::Float64),
            col(y_column).cast(DataType::Float64),
        ])
        .drop_nulls(None)
        .slice(0, CHART_ROW_LIMIT as u32)
        .collect()?;
    let x_series = df.column(x_column)?.f64()?;
    let y_series = df.column(y_column)?.f64()?;
    let mut values = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let x_val = x_series.get(i).unwrap_or(0.0);
        let y_val = y_series.get(i).unwrap_or(0.0);
        if x_val.is_finite() && y_val.is_finite() {
            values.push((x_val, y_val));
        }
    }
    Ok(values)
}

/// Prepare histogram data for a numeric column.
pub fn prepare_histogram_data(
    lf: &LazyFrame,
    column: &str,
    bins: usize,
) -> Result<HistogramData> {
    let mut values = collect_numeric_values(lf, column)?;
    if values.is_empty() {
        return Ok(HistogramData {
            column: column.to_string(),
            bins: Vec::new(),
            x_min: 0.0,
            x_max: 1.0,
            max_count: 0.0,
        });
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let x_min = *values.first().unwrap();
    let x_max = *values.last().unwrap();
    let range = (x_max - x_min).abs();
    let bin_count = bins.max(1);
    if range <= f64::EPSILON {
        return Ok(HistogramData {
            column: column.to_string(),
            bins: vec![HistogramBin {
                center: x_min,
                count: values.len() as f64,
            }],
            x_min: x_min - 0.5,
            x_max: x_max + 0.5,
            max_count: values.len() as f64,
        });
    }
    let bin_width = range / bin_count as f64;
    let mut counts = vec![0.0_f64; bin_count];
    for v in values {
        let mut idx = ((v - x_min) / bin_width).floor() as isize;
        if idx < 0 {
            idx = 0;
        }
        if idx as usize >= bin_count {
            idx = bin_count.saturating_sub(1) as isize;
        }
        counts[idx as usize] += 1.0;
    }
    let bins: Vec<HistogramBin> = counts
        .iter()
        .enumerate()
        .map(|(i, count)| HistogramBin {
            center: x_min + (i as f64 + 0.5) * bin_width,
            count: *count,
        })
        .collect();
    let max_count = counts
        .iter()
        .cloned()
        .fold(0.0_f64, |a, b| a.max(b));
    Ok(HistogramData {
        column: column.to_string(),
        bins,
        x_min,
        x_max,
        max_count,
    })
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

/// Prepare box plot stats for one or more numeric columns.
pub fn prepare_box_plot_data(lf: &LazyFrame, columns: &[String]) -> Result<BoxPlotData> {
    let mut stats = Vec::new();
    let mut y_min = f64::INFINITY;
    let mut y_max = f64::NEG_INFINITY;
    for column in columns {
        let mut values = collect_numeric_values(lf, column)?;
        if values.is_empty() {
            continue;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min = *values.first().unwrap();
        let max = *values.last().unwrap();
        let q1 = quantile(&values, 0.25);
        let median = quantile(&values, 0.5);
        let q3 = quantile(&values, 0.75);
        y_min = y_min.min(min);
        y_max = y_max.max(max);
        stats.push(BoxPlotStats {
            name: column.to_string(),
            min,
            q1,
            median,
            q3,
            max,
        });
    }
    if stats.is_empty() {
        return Ok(BoxPlotData {
            stats,
            y_min: 0.0,
            y_max: 1.0,
        });
    }
    if y_max <= y_min {
        y_max = y_min + 1.0;
    }
    Ok(BoxPlotData { stats, y_min, y_max })
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

/// Prepare KDE data for one or more numeric columns.
pub fn prepare_kde_data(
    lf: &LazyFrame,
    columns: &[String],
    bandwidth_factor: f64,
) -> Result<KdeData> {
    let mut series = Vec::new();
    let mut all_x_min = f64::INFINITY;
    let mut all_x_max = f64::NEG_INFINITY;
    let mut all_y_max = f64::NEG_INFINITY;
    for column in columns {
        let mut values = collect_numeric_values(lf, column)?;
        if values.is_empty() {
            continue;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let min = *values.first().unwrap();
        let max = *values.last().unwrap();
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
            name: column.to_string(),
            points,
        });
    }
    if series.is_empty() {
        return Ok(KdeData {
            series,
            x_min: 0.0,
            x_max: 1.0,
            y_max: 1.0,
        });
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
    })
}

/// Prepare heatmap data for two numeric columns.
pub fn prepare_heatmap_data(
    lf: &LazyFrame,
    x_column: &str,
    y_column: &str,
    bins: usize,
) -> Result<HeatmapData> {
    let pairs = collect_numeric_pairs(lf, x_column, y_column)?;
    if pairs.is_empty() {
        return Ok(HeatmapData {
            x_column: x_column.to_string(),
            y_column: y_column.to_string(),
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
            x_bins: bins,
            y_bins: bins,
            counts: vec![vec![0.0; bins.max(1)]; bins.max(1)],
            max_count: 0.0,
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
    let x_bins = bins.max(1);
    let y_bins = bins.max(1);
    let mut counts = vec![vec![0.0_f64; x_bins]; y_bins];
    let x_range = x_max - x_min;
    let y_range = y_max - y_min;
    for (x, y) in pairs {
        let mut xi = ((x - x_min) / x_range * x_bins as f64).floor() as isize;
        let mut yi = ((y - y_min) / y_range * y_bins as f64).floor() as isize;
        if xi < 0 {
            xi = 0;
        }
        if yi < 0 {
            yi = 0;
        }
        if xi as usize >= x_bins {
            xi = x_bins.saturating_sub(1) as isize;
        }
        if yi as usize >= y_bins {
            yi = y_bins.saturating_sub(1) as isize;
        }
        counts[yi as usize][xi as usize] += 1.0;
    }
    let max_count = counts
        .iter()
        .flat_map(|row| row.iter())
        .cloned()
        .fold(0.0_f64, |a, b| a.max(b));
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
    })
}

#[cfg(test)]
mod tests {
    use super::{prepare_chart_data, XAxisTemporalKind};
    use polars::prelude::*;

    #[test]
    fn prepare_empty_y_columns() {
        let lf = df!("x" => &[1.0_f64, 2.0], "y" => &[10.0, 20.0])
            .unwrap()
            .lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let result = prepare_chart_data(&lf, schema.as_ref(), "x", &[]).unwrap();
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
        let schema = lf.clone().collect_schema().unwrap();
        let result =
            prepare_chart_data(&lf, schema.as_ref(), "x", &["a".into(), "b".into()]).unwrap();
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
    }

    #[test]
    fn prepare_skips_nan() {
        let lf = df!(
            "x" => &[1.0_f64, 2.0, 3.0],
            "y" => &[10.0_f64, f64::NAN, 30.0]
        )
        .unwrap()
        .lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let result = prepare_chart_data(&lf, schema.as_ref(), "x", &["y".into()]).unwrap();
        assert_eq!(result.series[0].len(), 2);
        assert_eq!(result.series[0], vec![(1.0, 10.0), (3.0, 30.0)]);
    }

    #[test]
    fn prepare_missing_x_column_errors() {
        let lf = df!("x" => &[1.0_f64], "y" => &[2.0_f64]).unwrap().lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let result = prepare_chart_data(&lf, schema.as_ref(), "missing", &["y".into()]);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod x_range_tests {
    use super::{prepare_chart_x_range, XAxisTemporalKind};
    use polars::prelude::*;

    #[test]
    fn prepare_x_range_numeric() {
        let lf = df!("x" => &[10.0_f64, 20.0, 5.0, 30.0]).unwrap().lazy();
        let schema = lf.clone().collect_schema().unwrap();
        let r = prepare_chart_x_range(&lf, schema.as_ref(), "x").unwrap();
        assert_eq!(r.x_min, 5.0);
        assert_eq!(r.x_max, 30.0);
        assert_eq!(r.x_axis_kind, XAxisTemporalKind::Numeric);
    }

    #[test]
    fn prepare_x_range_empty_returns_placeholder() {
        let lf = df!("x" => &[1.0_f64]).unwrap().lazy().slice(0, 0);
        let schema = lf.clone().collect_schema().unwrap();
        let r = prepare_chart_x_range(&lf, schema.as_ref(), "x").unwrap();
        assert_eq!(r.x_min, 0.0);
        assert_eq!(r.x_max, 1.0);
    }
}

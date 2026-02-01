//! Prepare chart data from LazyFrame: select x/y columns, collect, and convert to (f64, f64) points.

use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use color_eyre::Result;
use polars::datatypes::{DataType, TimeUnit};
use polars::prelude::*;

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

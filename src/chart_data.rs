//! Prepare chart data from LazyFrame: select x/y columns, collect, and convert to (f64, f64) points.

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

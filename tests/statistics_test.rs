use color_eyre::Result;
use datui::statistics::{
    ComputeOptions, compute_correlation_matrix, compute_correlation_pair,
    compute_statistics_with_options,
};
use polars::prelude::*;

#[test]
fn test_distribution_detection_normal() -> Result<()> {
    // Create a normal distribution dataset using deterministic values
    // Using Box-Muller transform approximation
    let values: Vec<f64> = (0..1000)
        .map(|i| {
            // Deterministic normal-like distribution
            let u1 = ((i * 7) % 1000) as f64 / 1000.0;
            let u2 = ((i * 13) % 1000) as f64 / 1000.0;
            let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            z0 * 10.0 + 50.0
        })
        .collect();

    let df = DataFrame::new_infer_height(vec![Series::new("value".into(), values).into()])?;

    let lf = df.lazy();
    let options = ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
        polars_streaming: true,
    };
    let results = compute_statistics_with_options(&lf, Some(1000), 42, options)?;

    // Check that we have distribution analysis
    assert!(!results.distribution_analyses.is_empty());

    // Check that the distribution type is detected (should be Normal or at least have high confidence)
    let dist_analysis = &results.distribution_analyses[0];
    assert_eq!(dist_analysis.column_name, "value");
    assert!(dist_analysis.confidence > 0.0);
    assert!(dist_analysis.fit_quality > 0.0);

    Ok(())
}

#[test]
fn test_correlation_matrix_computation() -> Result<()> {
    // Create correlated data
    let n = 100;
    let x: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let y: Vec<f64> = x.iter().map(|&xi| xi * 2.0 + 5.0 + (xi * 0.1)).collect(); // Strong positive correlation
    let z: Vec<f64> = x.iter().map(|&xi| -xi * 1.5 + 10.0).collect(); // Strong negative correlation

    let df = DataFrame::new_infer_height(vec![
        Series::new("x".into(), x).into(),
        Series::new("y".into(), y).into(),
        Series::new("z".into(), z).into(),
    ])?;

    let corr_matrix = compute_correlation_matrix(&df)?;

    assert_eq!(corr_matrix.columns.len(), 3);
    assert_eq!(corr_matrix.correlations.len(), 3);

    // Check diagonal (self-correlation should be 1.0)
    assert!((corr_matrix.correlations[0][0] - 1.0).abs() < 0.01);
    assert!((corr_matrix.correlations[1][1] - 1.0).abs() < 0.01);
    assert!((corr_matrix.correlations[2][2] - 1.0).abs() < 0.01);

    // Check symmetry
    assert!((corr_matrix.correlations[0][1] - corr_matrix.correlations[1][0]).abs() < 0.01);
    assert!((corr_matrix.correlations[0][2] - corr_matrix.correlations[2][0]).abs() < 0.01);
    assert!((corr_matrix.correlations[1][2] - corr_matrix.correlations[2][1]).abs() < 0.01);

    // Check that x and y have strong positive correlation
    assert!(corr_matrix.correlations[0][1] > 0.8);

    // Check that x and z have strong negative correlation
    assert!(corr_matrix.correlations[0][2] < -0.8);

    Ok(())
}

#[test]
fn test_correlation_pair_computation() -> Result<()> {
    // Create correlated data
    let n = 100;
    let x: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let y: Vec<f64> = x.iter().map(|&xi| xi * 2.0 + 5.0).collect();

    let df = DataFrame::new_infer_height(vec![
        Series::new("x".into(), x).into(),
        Series::new("y".into(), y).into(),
    ])?;

    let pair = compute_correlation_pair(&df, "x", "y")?;

    assert_eq!(pair.column1, "x");
    assert_eq!(pair.column2, "y");
    assert!(pair.correlation > 0.9); // Should be very high
    assert!(pair.r_squared > 0.8);
    assert!(pair.sample_size == n);

    Ok(())
}

#[test]
fn test_outlier_detection() -> Result<()> {
    // Create data with outliers
    let mut values: Vec<f64> = (0..100).map(|i| i as f64).collect();
    values.push(1000.0); // Outlier
    values.push(-1000.0); // Outlier

    let df = DataFrame::new_infer_height(vec![Series::new("value".into(), values).into()])?;

    let lf = df.lazy();
    let options = ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
        polars_streaming: true,
    };
    let results = compute_statistics_with_options(&lf, Some(102), 42, options)?;

    // Check that outliers are detected
    if let Some(dist_analysis) = results.distribution_analyses.first() {
        assert!(dist_analysis.outliers.total_count > 0);
        assert!(dist_analysis.outliers.percentage > 0.0);
    }

    Ok(())
}

/// 30,000 rows, a null every seventh, of a long right tail: more than the 10,000 values
/// the fits look at, and more than 100 outliers.
fn long_tail() -> LazyFrame {
    let values: Vec<Option<f64>> = (0..30_000)
        .map(|i| (i % 7 != 0).then(|| 1000.0 / (1 + (i * 31) % 997) as f64))
        .collect();
    df!("x" => values).unwrap().lazy()
}

fn distribution_options() -> ComputeOptions {
    ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
        polars_streaming: true,
    }
}

fn close(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0)
}

/// Every outlier is counted, not the first hundred, and skewness and kurtosis are
/// over the 25,714 non-null values. Expected values are Polars' on the same column:
/// `skew(bias=False)`, `kurtosis(fisher=False, bias=False)` and nearest quantiles.
#[test]
fn distribution_statistics_cover_every_value() -> Result<()> {
    let results = compute_statistics_with_options(&long_tail(), None, 0, distribution_options())?;
    let dist = &results.distribution_analyses[0];

    let outliers = &dist.outliers;
    assert_eq!(outliers.total_count, 3_224);
    assert_eq!(outliers.iqr_count, 3_224);
    assert_eq!(outliers.zscore_count, 180);
    assert!(close(outliers.percentage, 12.537917087967642));
    assert_eq!(outliers.outlier_rows.len(), 100, "examples stay capped");
    assert_eq!(
        outliers.outlier_rows[0].column_value, 1000.0,
        "most extreme first"
    );

    let shape = &dist.characteristics;
    assert!(
        close(shape.skewness, 18.449491291574184),
        "{}",
        shape.skewness
    );
    assert!(
        close(shape.kurtosis, 415.7554868960841),
        "{}",
        shape.kurtosis
    );
    assert!(close(shape.mean, 7.498365763834674));
    assert!(close(shape.std_dev, 39.92556614072846));
    assert!(close(shape.median, 2.004008016032064));
    assert!(close(dist.percentiles.p25, 1.3368983957219251));
    assert!(close(dist.percentiles.p75, 4.0));
    assert!(close(dist.percentiles.p99, 90.9090909090909));

    let numeric = results.column_statistics[0].numeric_stats.as_ref().unwrap();
    assert_eq!(
        (numeric.outliers_iqr, numeric.outliers_zscore),
        (3_224, 180)
    );
    Ok(())
}

/// Describe and Distribution read the same sample and give it one median.
#[test]
fn describe_and_distribution_agree_on_a_sample() -> Result<()> {
    let sample = datui::sampling::Sample {
        method: datui::sampling::SampleMethod::Spread,
        rows: 20_000,
        seed: 1,
        ..datui::sampling::Sample::default()
    };
    let lf = long_tail();
    let describe = datui::statistics::compute_describe_from_lazy(&lf, None, &sample, true)?;
    let distribution = datui::statistics::compute_statistics_for_sample(
        &lf,
        &sample,
        None,
        distribution_options(),
    )?;
    assert_eq!(describe.sample_size, Some(20_000));
    assert_eq!(distribution.sample_size, Some(20_000));

    let described = describe.column_statistics[0]
        .numeric_stats
        .as_ref()
        .unwrap();
    let dist = &distribution.distribution_analyses[0];
    assert_eq!(dist.characteristics.median, described.median);
    assert_eq!(dist.percentiles.p50, described.median);
    assert_eq!(dist.percentiles.p25, described.q25);
    assert_eq!(dist.percentiles.p75, described.q75);
    assert!(close(dist.characteristics.mean, described.mean));
    assert!(close(dist.characteristics.std_dev, described.std));
    Ok(())
}

/// A correlation is over every row where both columns are finite, the count beside
/// it is those rows, and a NaN in one column does not void the matrix. Expected values
/// are Polars' `corr` and `cov` on the same rows.
#[test]
fn correlation_covers_every_finite_pair() -> Result<()> {
    let a: Vec<f64> = (0..30_000).map(|i| (i % 1000) as f64).collect();
    let b: Vec<Option<f64>> = (0..30_000)
        .map(|i| {
            if i % 11 == 0 {
                Some(f64::NAN)
            } else if i % 13 == 0 {
                None
            } else {
                Some(((i % 1000) as f64).sqrt() + (i % 17) as f64)
            }
        })
        .collect();
    let df = df!("a" => a, "b" => b)?;

    let matrix = compute_correlation_matrix(&df)?;
    assert_eq!(matrix.sample_sizes[0][1], 25_174);
    assert!(close(matrix.correlations[0][1], 0.8191197645766106));

    let pair = compute_correlation_pair(&df, "a", "b")?;
    assert_eq!(pair.sample_size, 25_174);
    assert!(close(pair.correlation, 0.8191197645766106));
    assert!(close(pair.covariance, 2113.523780844375));
    Ok(())
}

/// NaN sorts above every number: two values in five of it made the median 84 and the
/// upper quartile NaN, and with it every fence. Describe and Distribution leave it out.
#[test]
fn quantiles_leave_nan_out() -> Result<()> {
    let values: Vec<f64> = (0..10_000)
        .map(|i| match i {
            _ if i % 5 < 2 => f64::NAN,
            _ if i % 1000 == 3 => 10_000.0,
            _ => (i % 100) as f64,
        })
        .collect();
    let lf = df!("x" => values)?.lazy();
    let every_row = datui::sampling::Sample {
        method: datui::sampling::SampleMethod::EveryRow,
        ..datui::sampling::Sample::default()
    };
    let describe = datui::statistics::compute_describe_from_lazy(&lf, None, &every_row, true)?;
    let described = describe.column_statistics[0]
        .numeric_stats
        .as_ref()
        .unwrap();
    assert_eq!(
        (described.q25, described.median, described.q75),
        (27.0, 52.0, 77.0)
    );

    let results = compute_statistics_with_options(&lf, None, 0, distribution_options())?;
    let dist = &results.distribution_analyses[0];
    assert_eq!(dist.percentiles.p50, 52.0);
    assert_eq!(dist.percentiles.p75, 77.0);
    assert_eq!(dist.outliers.total_count, 10);
    assert!(close(dist.outliers.percentage, 10.0 / 6_000.0 * 100.0));
    Ok(())
}

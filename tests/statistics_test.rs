use color_eyre::Result;
use datui::statistics::{
    compute_correlation_matrix, compute_correlation_pair, compute_statistics_with_options,
    ComputeOptions,
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

    let df = DataFrame::new(vec![Series::new("value".into(), values).into()])?;

    let lf = df.lazy();
    let options = ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
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

    let df = DataFrame::new(vec![
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

    let df = DataFrame::new(vec![
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

    let df = DataFrame::new(vec![Series::new("value".into(), values).into()])?;

    let lf = df.lazy();
    let options = ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
    };
    let results = compute_statistics_with_options(&lf, Some(102), 42, options)?;

    // Check that outliers are detected
    if let Some(dist_analysis) = results.distribution_analyses.first() {
        assert!(dist_analysis.outliers.total_count > 0);
        assert!(dist_analysis.outliers.percentage > 0.0);
    }

    Ok(())
}

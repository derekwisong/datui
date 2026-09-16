use color_eyre::Result;
use datui::statistics::{compute_statistics_with_options, ComputeOptions, DistributionType};
use polars::prelude::*;
use std::path::Path;

mod common;

/// Test distribution detection using the large dataset parquet file
/// Each test loads a specific distribution column and verifies the detection
fn load_large_dataset() -> Result<LazyFrame> {
    common::ensure_sample_data();
    let path = Path::new("tests/sample-data/large_dataset.parquet");
    let pl_path = PlRefPath::try_from_path(path)?;
    let lf = LazyFrame::scan_parquet(pl_path, Default::default())?;
    Ok(lf)
}

fn test_distribution_detection(column_name: &str, expected_type: DistributionType) -> Result<()> {
    let lf = load_large_dataset()?;

    // Select only the column we're testing
    let lf = lf.select([col(column_name)]);

    let options = ComputeOptions {
        include_distribution_info: true,
        include_distribution_analyses: true,
        include_correlation_matrix: false,
        include_skewness_kurtosis_outliers: true,
        polars_streaming: true,
    };

    let results = compute_statistics_with_options(&lf, Some(10000), 42, options)?;

    // Find the column statistics for our test column
    let col_stat = results
        .column_statistics
        .iter()
        .find(|cs| cs.name == column_name)
        .unwrap_or_else(|| panic!("Column {} not found in results", column_name));

    // Check that distribution info was computed
    let dist_info = col_stat
        .distribution_info
        .as_ref()
        .unwrap_or_else(|| panic!("Distribution info not computed for {}", column_name));

    // Check that the detected distribution type matches expected
    assert_eq!(
        dist_info.distribution_type,
        expected_type,
        "Distribution detection failed for {}: expected {:?}, got {:?} (confidence: {:.3}, fit_quality: {:.3})",
        column_name,
        expected_type,
        dist_info.distribution_type,
        dist_info.confidence,
        dist_info.fit_quality.unwrap_or(0.0)
    );

    // Also check that we have a distribution analysis
    let dist_analysis = results
        .distribution_analyses
        .iter()
        .find(|da| da.column_name == column_name)
        .unwrap_or_else(|| panic!("Distribution analysis not found for {}", column_name));

    assert_eq!(
        dist_analysis.distribution_type, expected_type,
        "Distribution analysis type mismatch for {}: expected {:?}, got {:?}",
        column_name, expected_type, dist_analysis.distribution_type
    );

    Ok(())
}

#[test]
fn test_dist_normal() -> Result<()> {
    test_distribution_detection("dist_normal", DistributionType::Normal)
}

#[test]
fn test_dist_lognormal() -> Result<()> {
    test_distribution_detection("dist_lognormal", DistributionType::LogNormal)
}

#[test]
fn test_dist_uniform() -> Result<()> {
    test_distribution_detection("dist_uniform", DistributionType::Uniform)
}

#[test]
fn test_dist_powerlaw() -> Result<()> {
    test_distribution_detection("dist_powerlaw", DistributionType::PowerLaw)
}

#[test]
fn test_dist_exponential() -> Result<()> {
    test_distribution_detection("dist_exponential", DistributionType::Exponential)
}

#[test]
fn test_dist_beta() -> Result<()> {
    test_distribution_detection("dist_beta", DistributionType::Beta)
}

#[test]
fn test_dist_gamma() -> Result<()> {
    test_distribution_detection("dist_gamma", DistributionType::Gamma)
}

#[test]
fn test_dist_chisquared() -> Result<()> {
    test_distribution_detection("dist_chisquared", DistributionType::ChiSquared)
}

#[test]
fn test_dist_students_t() -> Result<()> {
    test_distribution_detection("dist_students_t", DistributionType::StudentsT)
}

#[test]
fn test_dist_poisson() -> Result<()> {
    test_distribution_detection("dist_poisson", DistributionType::Poisson)
}

#[test]
fn test_dist_bernoulli() -> Result<()> {
    test_distribution_detection("dist_bernoulli", DistributionType::Bernoulli)
}

#[test]
fn test_dist_binomial() -> Result<()> {
    test_distribution_detection("dist_binomial", DistributionType::Binomial)
}

#[test]
fn test_dist_geometric() -> Result<()> {
    test_distribution_detection("dist_geometric", DistributionType::Geometric)
}

#[test]
fn test_dist_weibull() -> Result<()> {
    test_distribution_detection("dist_weibull", DistributionType::Weibull)
}

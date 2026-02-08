use color_eyre::eyre::Report;
use color_eyre::Result;
use polars::polars_compute::rolling::QuantileMethod;
use polars::prelude::*;
use std::collections::HashMap;

/// Collects a LazyFrame into a DataFrame.
///
/// When the `streaming` feature is enabled and `use_streaming` is true, uses the Polars
/// streaming engine (batch processing, lower memory). Otherwise collects normally.
/// Returns `PolarsError` so callers that need to display or store the error (e.g.
/// `DataTableState::error`) can do so without converting.
pub fn collect_lazy(
    lf: LazyFrame,
    use_streaming: bool,
) -> std::result::Result<DataFrame, PolarsError> {
    #[cfg(feature = "streaming")]
    {
        if use_streaming {
            lf.with_new_streaming(true).collect()
        } else {
            lf.collect()
        }
    }
    #[cfg(not(feature = "streaming"))]
    {
        let _ = use_streaming; // ignored when streaming feature is disabled
        lf.collect()
    }
}

/// Default sampling threshold: datasets >= this size are sampled.
/// Used as fallback when sample_size is None. App uses config value.
pub const SAMPLING_THRESHOLD: usize = 10_000;

#[derive(Clone)]
pub struct ColumnStatistics {
    pub name: String,
    pub dtype: DataType,
    pub count: usize,
    pub null_count: usize,
    pub numeric_stats: Option<NumericStatistics>,
    pub categorical_stats: Option<CategoricalStatistics>,
    pub distribution_info: Option<DistributionInfo>,
}

#[derive(Clone)]
pub struct NumericStatistics {
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub q25: f64,
    pub q75: f64,
    pub percentiles: HashMap<u8, f64>, // 1, 5, 25, 50, 75, 95, 99
    pub skewness: f64,
    pub kurtosis: f64,
    pub outliers_iqr: usize,
    pub outliers_zscore: usize,
}

#[derive(Clone)]
pub struct CategoricalStatistics {
    pub unique_count: usize,
    pub mode: Option<String>,
    pub top_values: Vec<(String, usize)>,
    pub min: Option<String>, // Lexicographically smallest string
    pub max: Option<String>, // Lexicographically largest string
}

#[derive(Clone)]
pub struct DistributionInfo {
    pub distribution_type: DistributionType,
    pub confidence: f64,
    pub sample_size: usize,
    pub is_sampled: bool,
    pub fit_quality: Option<f64>, // 0.0-1.0, how well data fits detected type
    pub all_distribution_pvalues: HashMap<DistributionType, f64>, // P-values for all tested distributions
}

#[derive(Clone)]
pub struct DistributionAnalysis {
    pub column_name: String,
    pub distribution_type: DistributionType,
    pub confidence: f64,  // 0.0-1.0
    pub fit_quality: f64, // 0.0-1.0, how well data fits detected type
    pub characteristics: DistributionCharacteristics,
    pub outliers: OutlierAnalysis,
    pub percentiles: PercentileBreakdown,
    pub sorted_sample_values: Vec<f64>, // Sorted data values for Q-Q plot (all data if < threshold, sampled if >= threshold)
    pub is_sampled: bool,               // Whether data was sampled
    pub sample_size: usize,             // Actual number of values used
    pub all_distribution_pvalues: HashMap<DistributionType, f64>, // P-values for all tested distributions
}

#[derive(Clone)]
pub struct DistributionCharacteristics {
    pub shapiro_wilk_stat: Option<f64>,
    pub shapiro_wilk_pvalue: Option<f64>,
    pub skewness: f64,
    pub kurtosis: f64,
    pub mean: f64,
    pub median: f64,
    pub std_dev: f64,
    pub variance: f64,
    pub coefficient_of_variation: f64,
    pub mode: Option<f64>, // For unimodal distributions
}

#[derive(Clone)]
pub struct OutlierAnalysis {
    pub total_count: usize,
    pub percentage: f64,
    pub iqr_count: usize,
    pub zscore_count: usize,
    pub outlier_rows: Vec<OutlierRow>, // Limited to top N for performance
}

#[derive(Clone)]
pub struct OutlierRow {
    pub row_index: usize,
    pub column_value: f64,
    pub context_data: HashMap<String, String>, // Other column values for context
    pub detection_method: OutlierMethod,
    pub z_score: Option<f64>,
    pub iqr_position: Option<IqrPosition>, // Below Q1-1.5*IQR or above Q3+1.5*IQR
}

#[derive(Clone, Debug)]
pub enum OutlierMethod {
    IQR,
    ZScore,
    Both,
}

#[derive(Clone, Debug)]
pub enum IqrPosition {
    BelowLowerFence,
    AboveUpperFence,
}

#[derive(Clone)]
pub struct PercentileBreakdown {
    pub p1: f64,
    pub p5: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
}

// Correlation matrix structures
#[derive(Clone)]
pub struct CorrelationMatrix {
    pub columns: Vec<String>,            // Numeric column names
    pub correlations: Vec<Vec<f64>>,     // Square matrix of correlations
    pub p_values: Option<Vec<Vec<f64>>>, // Statistical significance (optional)
    pub sample_sizes: Vec<Vec<usize>>,   // Sample size for each pair
}

#[derive(Clone)]
pub struct CorrelationPair {
    pub column1: String,
    pub column2: String,
    pub correlation: f64,
    pub p_value: Option<f64>,
    pub sample_size: usize,
    pub covariance: f64,
    pub r_squared: f64,
    pub stats1: ColumnStats,
    pub stats2: ColumnStats,
}

#[derive(Clone)]
pub struct ColumnStats {
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistributionType {
    #[default]
    Normal,
    LogNormal,
    Uniform,
    PowerLaw,
    Exponential,
    Beta,
    Gamma,
    ChiSquared,
    StudentsT,
    Poisson,
    Bernoulli,
    Binomial,
    Geometric,
    Weibull,
    Unknown,
}

impl std::fmt::Display for DistributionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionType::Normal => write!(f, "Normal"),
            DistributionType::LogNormal => write!(f, "Log-Normal"),
            DistributionType::Uniform => write!(f, "Uniform"),
            DistributionType::PowerLaw => write!(f, "Power Law"),
            DistributionType::Exponential => write!(f, "Exponential"),
            DistributionType::Beta => write!(f, "Beta"),
            DistributionType::Gamma => write!(f, "Gamma"),
            DistributionType::ChiSquared => write!(f, "Chi-Squared"),
            DistributionType::StudentsT => write!(f, "Student's t"),
            DistributionType::Poisson => write!(f, "Poisson"),
            DistributionType::Bernoulli => write!(f, "Bernoulli"),
            DistributionType::Binomial => write!(f, "Binomial"),
            DistributionType::Geometric => write!(f, "Geometric"),
            DistributionType::Weibull => write!(f, "Weibull"),
            DistributionType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Clone)]
pub struct AnalysisResults {
    pub column_statistics: Vec<ColumnStatistics>,
    pub total_rows: usize,
    pub sample_size: Option<usize>,
    pub sample_seed: u64,
    pub correlation_matrix: Option<CorrelationMatrix>,
    pub distribution_analyses: Vec<DistributionAnalysis>,
}

pub struct AnalysisContext {
    pub has_query: bool,
    pub query: String,
    pub has_filters: bool,
    pub filter_count: usize,
    pub is_drilled_down: bool,
    pub group_key: Option<Vec<String>>,
    pub group_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ComputeOptions {
    pub include_distribution_info: bool,
    pub include_distribution_analyses: bool,
    pub include_correlation_matrix: bool,
    pub include_skewness_kurtosis_outliers: bool,
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    pub polars_streaming: bool,
}

impl Default for ComputeOptions {
    fn default() -> Self {
        Self {
            include_distribution_info: false,
            include_distribution_analyses: false,
            include_correlation_matrix: false,
            include_skewness_kurtosis_outliers: false,
            polars_streaming: true,
        }
    }
}

/// Computes statistics for a LazyFrame with default options.
///
/// Convenience wrapper around `compute_statistics_with_options`.
pub fn compute_statistics(
    lf: &LazyFrame,
    sample_size: Option<usize>,
    seed: u64,
) -> Result<AnalysisResults> {
    compute_statistics_with_options(lf, sample_size, seed, ComputeOptions::default())
}

/// Computes comprehensive statistics for a LazyFrame.
///
/// Main entry point for statistical analysis. Computes:
/// - Basic statistics (count, nulls, min, max, mean) for all columns
/// - Numeric statistics (percentiles, skewness, kurtosis, outliers) for numeric columns
/// - Categorical statistics (unique count, mode, top values) for categorical columns
/// - Distribution detection and analysis for numeric columns (if enabled)
/// - Correlation matrix for numeric columns (if enabled)
///
/// Large datasets are automatically sampled when exceeding the sampling threshold.
pub fn compute_statistics_with_options(
    lf: &LazyFrame,
    sample_size: Option<usize>,
    seed: u64,
    options: ComputeOptions,
) -> Result<AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let use_streaming = options.polars_streaming;
    // Always count actual rows, regardless of sample_size parameter
    let total_rows = {
        let count_df =
            collect_lazy(lf.clone().select([len()]), use_streaming).map_err(Report::from)?;
        if let Some(col) = count_df.get(0) {
            if let Some(AnyValue::UInt32(n)) = col.first() {
                *n as usize
            } else {
                0
            }
        } else {
            0
        }
    };

    // sample_size: None = never sample (full data); Some(threshold) = sample when total_rows >= threshold
    let should_sample = sample_size.is_some_and(|t| total_rows >= t);
    let (sampling_threshold, actual_sample_size) = if should_sample {
        let t = sample_size.unwrap();
        (t, Some(t))
    } else {
        (0, None)
    };

    let df = if should_sample {
        sample_dataframe(lf, sampling_threshold, seed, use_streaming)?
    } else {
        collect_lazy(lf.clone(), use_streaming).map_err(Report::from)?
    };

    let mut column_statistics = Vec::new();

    for (name, dtype) in schema.iter() {
        let col = df.column(name)?;
        let series = col.as_materialized_series();
        let count = series.len();
        let null_count = series.null_count();

        let numeric_stats = if is_numeric_type(dtype) {
            Some(compute_numeric_stats(
                series,
                options.include_skewness_kurtosis_outliers,
            )?)
        } else {
            None
        };

        let categorical_stats = if is_categorical_type(dtype) {
            Some(compute_categorical_stats(series)?)
        } else {
            None
        };

        let distribution_info =
            if options.include_distribution_info && is_numeric_type(dtype) && null_count < count {
                // Get sample for distribution inference
                Some(infer_distribution(
                    series,
                    series,
                    actual_sample_size.unwrap_or(count),
                    should_sample,
                ))
            } else {
                None
            };

        column_statistics.push(ColumnStatistics {
            name: name.to_string(),
            dtype: dtype.clone(),
            count,
            null_count,
            numeric_stats,
            categorical_stats,
            distribution_info,
        });
    }

    let distribution_analyses = if options.include_distribution_analyses {
        column_statistics
            .iter()
            .filter_map(|col_stat| {
                if let (Some(numeric_stats), Some(dist_info)) =
                    (&col_stat.numeric_stats, &col_stat.distribution_info)
                {
                    if let Ok(series_col) = df.column(&col_stat.name) {
                        let series = series_col.as_materialized_series();
                        Some(compute_advanced_distribution_analysis(
                            &col_stat.name,
                            series,
                            numeric_stats,
                            dist_info,
                            actual_sample_size.unwrap_or(total_rows),
                            should_sample,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let correlation_matrix = if options.include_correlation_matrix {
        compute_correlation_matrix(&df).ok()
    } else {
        None
    };

    Ok(AnalysisResults {
        column_statistics,
        total_rows,
        sample_size: actual_sample_size,
        sample_seed: seed,
        correlation_matrix,
        distribution_analyses,
    })
}

/// Computes describe statistics for a single column of an already-collected DataFrame.
///
/// Used by the UI to compute stats per column and yield between columns for progress display.
/// No progress or chunk types; the library only exposes granularity.
pub fn compute_describe_column(
    df: &DataFrame,
    schema: &Schema,
    column_index: usize,
    options: &ComputeOptions,
    actual_sample_size: Option<usize>,
    is_sampled: bool,
) -> Result<ColumnStatistics> {
    let (name, dtype) = schema
        .iter()
        .nth(column_index)
        .ok_or_else(|| color_eyre::eyre::eyre!("column index {} out of range", column_index))?;
    let col = df.column(name)?;
    let series = col.as_materialized_series();
    let count = series.len();
    let null_count = series.null_count();

    let numeric_stats = if is_numeric_type(dtype) {
        Some(compute_numeric_stats(
            series,
            options.include_skewness_kurtosis_outliers,
        )?)
    } else {
        None
    };

    let categorical_stats = if is_categorical_type(dtype) {
        Some(compute_categorical_stats(series)?)
    } else {
        None
    };

    let distribution_info =
        if options.include_distribution_info && is_numeric_type(dtype) && null_count < count {
            Some(infer_distribution(
                series,
                series,
                actual_sample_size.unwrap_or(count),
                is_sampled,
            ))
        } else {
            None
        };

    Ok(ColumnStatistics {
        name: name.to_string(),
        dtype: dtype.clone(),
        count,
        null_count,
        numeric_stats,
        categorical_stats,
        distribution_info,
    })
}

/// Builds describe-only AnalysisResults from a list of column statistics.
///
/// Used when completing chunked describe; correlation and distribution analyses stay empty/None.
pub fn analysis_results_from_describe(
    column_statistics: Vec<ColumnStatistics>,
    total_rows: usize,
    sample_size: Option<usize>,
    sample_seed: u64,
) -> AnalysisResults {
    AnalysisResults {
        column_statistics,
        total_rows,
        sample_size,
        sample_seed,
        correlation_matrix: None,
        distribution_analyses: Vec::new(),
    }
}

/// Builds aggregation expressions for describe (count, null_count, mean, std, percentiles, min, max).
/// Used so we can run a single collect on a LazyFrame without materializing all rows.
fn build_describe_aggregation_exprs(schema: &Schema) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (name, dtype) in schema.iter() {
        let name = name.as_str();
        let prefix = format!("{}::", name);
        exprs.push(col(name).count().alias(format!("{}count", prefix)));
        exprs.push(
            col(name)
                .null_count()
                .alias(format!("{}null_count", prefix)),
        );
        if is_numeric_type(dtype) {
            let c = col(name).cast(DataType::Float64);
            exprs.push(c.clone().mean().alias(format!("{}mean", prefix)));
            exprs.push(c.clone().std(1).alias(format!("{}std", prefix)));
            exprs.push(c.clone().min().alias(format!("{}min", prefix)));
            exprs.push(
                c.clone()
                    .quantile(lit(0.25), QuantileMethod::Nearest)
                    .alias(format!("{}q25", prefix)),
            );
            exprs.push(
                c.clone()
                    .quantile(lit(0.5), QuantileMethod::Nearest)
                    .alias(format!("{}median", prefix)),
            );
            exprs.push(
                c.clone()
                    .quantile(lit(0.75), QuantileMethod::Nearest)
                    .alias(format!("{}q75", prefix)),
            );
            exprs.push(c.max().alias(format!("{}max", prefix)));
        } else if is_categorical_type(dtype) {
            exprs.push(col(name).min().alias(format!("{}min", prefix)));
            exprs.push(col(name).max().alias(format!("{}max", prefix)));
        }
    }
    exprs
}

/// Parses the single-row aggregation result from describe into column statistics.
fn parse_describe_agg_row(agg_df: &DataFrame, schema: &Schema) -> Vec<ColumnStatistics> {
    let row = 0usize;
    let mut column_statistics = Vec::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        let name_str = name.as_str();
        let prefix = format!("{}::", name_str);
        let count: usize = agg_df
            .column(&format!("{}count", prefix))
            .ok()
            .map(|s| match s.get(row) {
                Ok(AnyValue::UInt32(x)) => x as usize,
                _ => 0,
            })
            .unwrap_or(0);
        let null_count: usize = agg_df
            .column(&format!("{}null_count", prefix))
            .ok()
            .map(|s| match s.get(row) {
                Ok(AnyValue::UInt32(x)) => x as usize,
                _ => 0,
            })
            .unwrap_or(0);
        let numeric_stats = if is_numeric_type(dtype) {
            let mean = get_f64(agg_df, &format!("{}mean", prefix), row);
            let std = get_f64(agg_df, &format!("{}std", prefix), row);
            let min = get_f64(agg_df, &format!("{}min", prefix), row);
            let q25 = get_f64(agg_df, &format!("{}q25", prefix), row);
            let median = get_f64(agg_df, &format!("{}median", prefix), row);
            let q75 = get_f64(agg_df, &format!("{}q75", prefix), row);
            let max = get_f64(agg_df, &format!("{}max", prefix), row);
            let mut percentiles = HashMap::new();
            percentiles.insert(25u8, q25);
            percentiles.insert(50u8, median);
            percentiles.insert(75u8, q75);
            Some(NumericStatistics {
                mean,
                std,
                min,
                max,
                median,
                q25,
                q75,
                percentiles,
                skewness: 0.0,
                kurtosis: 3.0,
                outliers_iqr: 0,
                outliers_zscore: 0,
            })
        } else {
            None
        };
        let categorical_stats = if is_categorical_type(dtype) {
            let min = get_str(agg_df, &format!("{}min", prefix), row);
            let max = get_str(agg_df, &format!("{}max", prefix), row);
            Some(CategoricalStatistics {
                unique_count: 0,
                mode: None,
                top_values: Vec::new(),
                min,
                max,
            })
        } else {
            None
        };
        column_statistics.push(ColumnStatistics {
            name: name_str.to_string(),
            dtype: dtype.clone(),
            count,
            null_count,
            numeric_stats,
            categorical_stats,
            distribution_info: None,
        });
    }
    column_statistics
}

/// Computes describe statistics from a LazyFrame without materializing all rows.
/// When sampling is disabled, runs a single aggregation collect (like Polars describe) for similar performance.
/// When sampling is enabled, samples then runs describe on the sample.
pub fn compute_describe_from_lazy(
    lf: &LazyFrame,
    total_rows: usize,
    sample_size: Option<usize>,
    seed: u64,
    polars_streaming: bool,
) -> Result<AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let should_sample = sample_size.is_some_and(|t| total_rows >= t);
    if should_sample {
        let threshold = sample_size.unwrap();
        let df = sample_dataframe(lf, threshold, seed, polars_streaming)?;
        return compute_describe_single_aggregation(
            &df,
            &schema,
            total_rows,
            Some(threshold),
            seed,
            polars_streaming,
        );
    }
    let exprs = build_describe_aggregation_exprs(&schema);
    let agg_df = collect_lazy(lf.clone().select(exprs), polars_streaming).map_err(Report::from)?;
    let column_statistics = parse_describe_agg_row(&agg_df, &schema);
    Ok(analysis_results_from_describe(
        column_statistics,
        total_rows,
        None,
        seed,
    ))
}

/// Computes describe statistics in a single aggregation pass over the DataFrame.
/// Uses one collect() with aggregated expressions for all columns (count, null_count, mean, std, min, percentiles, max).
pub fn compute_describe_single_aggregation(
    df: &DataFrame,
    schema: &Schema,
    total_rows: usize,
    sample_size: Option<usize>,
    sample_seed: u64,
    polars_streaming: bool,
) -> Result<AnalysisResults> {
    let exprs = build_describe_aggregation_exprs(schema);
    let agg_df =
        collect_lazy(df.clone().lazy().select(exprs), polars_streaming).map_err(Report::from)?;
    let column_statistics = parse_describe_agg_row(&agg_df, schema);
    Ok(analysis_results_from_describe(
        column_statistics,
        total_rows,
        sample_size,
        sample_seed,
    ))
}

fn get_f64(df: &DataFrame, col_name: &str, row: usize) -> f64 {
    df.column(col_name)
        .ok()
        .and_then(|s| {
            let v = s.get(row).ok()?;
            match v {
                AnyValue::Float64(x) => Some(x),
                AnyValue::Float32(x) => Some(x as f64),
                AnyValue::Int32(x) => Some(x as f64),
                AnyValue::Int64(x) => Some(x as f64),
                AnyValue::UInt32(x) => Some(x as f64),
                AnyValue::Null => Some(f64::NAN),
                _ => None,
            }
        })
        .unwrap_or(f64::NAN)
}

fn get_str(df: &DataFrame, col_name: &str, row: usize) -> Option<String> {
    df.column(col_name)
        .ok()
        .and_then(|s| s.get(row).ok().map(|v| v.str_value().to_string()))
}

/// Computes distribution statistics for numeric columns.
///
/// - Infers distribution types for numeric columns missing distribution_info
/// - Computes advanced statistics (skewness, kurtosis, outliers) if missing
/// - Generates distribution_analyses for columns with detected distributions
pub fn compute_distribution_statistics(
    results: &mut AnalysisResults,
    lf: &LazyFrame,
    sample_size: Option<usize>,
    seed: u64,
    polars_streaming: bool,
) -> Result<()> {
    // sample_size: None = never sample; Some(threshold) = sample when total_rows >= threshold
    let should_sample = sample_size.is_some_and(|t| results.total_rows >= t);
    let sampling_threshold = sample_size.unwrap_or(0);
    let actual_sample_size = if should_sample {
        Some(sampling_threshold)
    } else {
        None
    };

    let df = if should_sample {
        sample_dataframe(lf, sampling_threshold, seed, polars_streaming)?
    } else {
        collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?
    };

    for col_stat in &mut results.column_statistics {
        if col_stat.distribution_info.is_none()
            && is_numeric_type(&col_stat.dtype)
            && col_stat.null_count < col_stat.count
        {
            let series = df.column(&col_stat.name)?.as_materialized_series();
            col_stat.distribution_info = Some(infer_distribution(
                series,
                series,
                actual_sample_size.unwrap_or(col_stat.count),
                should_sample,
            ));
        }

        if let Some(ref mut num_stats) = col_stat.numeric_stats {
            let needs_advanced_stats = num_stats.skewness == 0.0
                && num_stats.kurtosis == 3.0
                && num_stats.outliers_iqr == 0
                && num_stats.outliers_zscore == 0
                && col_stat.count > 0;
            if needs_advanced_stats {
                let series = df.column(&col_stat.name)?.as_materialized_series();
                num_stats.skewness = compute_skewness(series);
                num_stats.kurtosis = compute_kurtosis(series);
                let (out_iqr, out_zscore) = detect_outliers(
                    series,
                    num_stats.q25,
                    num_stats.q75,
                    num_stats.median,
                    num_stats.mean,
                    num_stats.std,
                );
                num_stats.outliers_iqr = out_iqr;
                num_stats.outliers_zscore = out_zscore;
            }
        }
    }

    if results.distribution_analyses.is_empty() {
        results.distribution_analyses = results
            .column_statistics
            .iter()
            .filter_map(|col_stat| {
                if let (Some(numeric_stats), Some(dist_info)) =
                    (&col_stat.numeric_stats, &col_stat.distribution_info)
                {
                    if let Ok(series_col) = df.column(&col_stat.name) {
                        let series = series_col.as_materialized_series();
                        Some(compute_advanced_distribution_analysis(
                            &col_stat.name,
                            series,
                            numeric_stats,
                            dist_info,
                            actual_sample_size.unwrap_or(results.total_rows),
                            should_sample,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
    }

    Ok(())
}

/// Computes correlation matrix if not already present in results.
pub fn compute_correlation_statistics(
    results: &mut AnalysisResults,
    lf: &LazyFrame,
    polars_streaming: bool,
) -> Result<()> {
    if results.correlation_matrix.is_none() {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        results.correlation_matrix = compute_correlation_matrix(&df).ok();
    }
    Ok(())
}

/// Uses Polars' definition so Int128, UInt128, Decimal, and future numeric types are included.
fn is_numeric_type(dtype: &DataType) -> bool {
    dtype.is_numeric()
}

fn is_categorical_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(..))
}

/// Samples a LazyFrame for analysis when row count exceeds threshold. Used by chunked describe.
pub fn sample_dataframe(
    lf: &LazyFrame,
    sample_size: usize,
    seed: u64,
    polars_streaming: bool,
) -> Result<DataFrame> {
    let collect_multiplier = if sample_size <= 1000 {
        5
    } else if sample_size <= 5000 {
        3
    } else {
        2
    };

    let collect_limit = (sample_size * collect_multiplier).min(50_000);
    let df = collect_lazy(lf.clone().limit(collect_limit as u32), polars_streaming)
        .map_err(Report::from)?;
    let total_collected = df.height();

    if total_collected <= sample_size {
        return Ok(df);
    }

    let step = total_collected / sample_size;
    let start_offset = (seed as usize) % step;

    let indices: Vec<u32> = (0..sample_size)
        .map(|i| {
            let idx = start_offset + i * step;
            (idx.min(total_collected - 1)) as u32
        })
        .collect();

    let indices_ca = UInt32Chunked::new("indices".into(), indices);
    df.take(&indices_ca)
        .map_err(|e| color_eyre::eyre::eyre!("Sampling error: {}", e))
}

fn get_numeric_values_as_f64(series: &Series) -> Vec<f64> {
    let max_len = 10000;
    let limited_series = if series.len() > max_len {
        series.slice(0, max_len)
    } else {
        series.clone()
    };

    if let Ok(f64_series) = limited_series.f64() {
        f64_series.iter().flatten().take(max_len).collect()
    } else if let Ok(i64_series) = limited_series.i64() {
        i64_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .take(max_len)
            .collect()
    } else if let Ok(i32_series) = limited_series.i32() {
        i32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .take(max_len)
            .collect()
    } else if let Ok(u64_series) = limited_series.u64() {
        u64_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .take(max_len)
            .collect()
    } else if let Ok(u32_series) = limited_series.u32() {
        u32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .take(max_len)
            .collect()
    } else if let Ok(f32_series) = limited_series.f32() {
        f32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .take(max_len)
            .collect()
    } else {
        match limited_series.cast(&DataType::Float64) {
            Ok(cast_series) => {
                if let Ok(f64_series) = cast_series.f64() {
                    f64_series.iter().flatten().take(max_len).collect()
                } else {
                    Vec::new()
                }
            }
            Err(_) => Vec::new(),
        }
    }
}

fn compute_numeric_stats(series: &Series, include_advanced: bool) -> Result<NumericStatistics> {
    let mean = series.mean().unwrap_or(f64::NAN);
    let std = series.std(1).unwrap_or(f64::NAN); // Sample std (ddof=1)

    let min = if let Ok(min_val) = series.min::<f64>() {
        min_val.unwrap_or(f64::NAN)
    } else if let Ok(min_val) = series.min::<i64>() {
        min_val.map(|v| v as f64).unwrap_or(f64::NAN)
    } else if let Ok(min_val) = series.min::<i32>() {
        min_val.map(|v| v as f64).unwrap_or(f64::NAN)
    } else {
        f64::NAN
    };

    let max = if let Ok(max_val) = series.max::<f64>() {
        max_val.unwrap_or(f64::NAN)
    } else if let Ok(max_val) = series.max::<i64>() {
        max_val.map(|v| v as f64).unwrap_or(f64::NAN)
    } else if let Ok(max_val) = series.max::<i32>() {
        max_val.map(|v| v as f64).unwrap_or(f64::NAN)
    } else {
        f64::NAN
    };

    let mut percentiles = HashMap::new();
    let values: Vec<f64> = get_numeric_values_as_f64(series);

    if !values.is_empty() {
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = sorted.len();

        for p in &[1, 5, 25, 50, 75, 95, 99] {
            let idx = ((*p as f64 / 100.0) * (n - 1) as f64).round() as usize;
            let idx = idx.min(n - 1);
            percentiles.insert(*p, sorted[idx]);
        }
    }

    let median = percentiles.get(&50).copied().unwrap_or(f64::NAN);
    let q25 = percentiles.get(&25).copied().unwrap_or(f64::NAN);
    let q75 = percentiles.get(&75).copied().unwrap_or(f64::NAN);

    let (skewness, kurtosis, outliers_iqr, outliers_zscore) = if include_advanced {
        let (out_iqr, out_zscore) = detect_outliers(series, q25, q75, median, mean, std);
        (
            compute_skewness(series),
            compute_kurtosis(series),
            out_iqr,
            out_zscore,
        )
    } else {
        (0.0, 3.0, 0, 0) // Default values when not computed
    };

    Ok(NumericStatistics {
        mean,
        std,
        min,
        max,
        median,
        q25,
        q75,
        percentiles,
        skewness,
        kurtosis,
        outliers_iqr,
        outliers_zscore,
    })
}

fn compute_skewness(series: &Series) -> f64 {
    let mean = series.mean().unwrap_or(0.0);
    let std = series.std(1).unwrap_or(1.0);
    let n = series.len() as f64;

    if std == 0.0 || n < 3.0 {
        return 0.0;
    }

    let values: Vec<f64> = get_numeric_values_as_f64(series);

    if values.is_empty() {
        return 0.0;
    }

    let sum_cubed_deviations: f64 = values
        .iter()
        .map(|v| {
            let deviation = (v - mean) / std;
            deviation * deviation * deviation
        })
        .sum();

    (n / ((n - 1.0) * (n - 2.0))) * sum_cubed_deviations
}

fn compute_kurtosis(series: &Series) -> f64 {
    let mean = series.mean().unwrap_or(0.0);
    let std = series.std(1).unwrap_or(1.0);
    let n = series.len() as f64;

    if std == 0.0 || n < 4.0 {
        return 3.0; // Normal distribution kurtosis
    }

    let values: Vec<f64> = get_numeric_values_as_f64(series);

    if values.is_empty() {
        return 3.0;
    }

    let sum_fourth_deviations: f64 = values
        .iter()
        .map(|v| {
            let deviation = (v - mean) / std;
            let d2 = deviation * deviation;
            d2 * d2
        })
        .sum();

    let k = (n * (n + 1.0) / ((n - 1.0) * (n - 2.0) * (n - 3.0))) * sum_fourth_deviations
        - 3.0 * (n - 1.0) * (n - 1.0) / ((n - 2.0) * (n - 3.0));

    k + 3.0 // Excess kurtosis -> kurtosis
}

fn detect_outliers(
    series: &Series,
    q25: f64,
    q75: f64,
    _median: f64,
    mean: f64,
    std: f64,
) -> (usize, usize) {
    if q25.is_nan() || q75.is_nan() {
        return (0, 0);
    }

    let iqr = q75 - q25;
    let lower_fence = q25 - 1.5 * iqr;
    let upper_fence = q75 + 1.5 * iqr;

    let mut outliers_iqr = 0;
    let mut outliers_zscore = 0;

    if std > 0.0 {
        // Get values as f64, handling both integer and float types
        let values: Vec<f64> = get_numeric_values_as_f64(series);

        for v in values {
            // IQR method
            if v < lower_fence || v > upper_fence {
                outliers_iqr += 1;
            }

            // Z-score method (3 sigma rule)
            let z = (v - mean).abs() / std;
            if z > 3.0 {
                outliers_zscore += 1;
            }
        }
    }

    (outliers_iqr, outliers_zscore)
}

fn compute_categorical_stats(series: &Series) -> Result<CategoricalStatistics> {
    let value_counts = series.value_counts(false, false, "counts".into(), false)?;
    let unique_count = value_counts.height();

    let mode = if unique_count > 0 {
        if let Some(col) = value_counts.get(0) {
            col.first().map(|v| v.str_value().to_string())
        } else {
            None
        }
    } else {
        None
    };

    let mut top_values = Vec::new();
    for i in 0..unique_count.min(10) {
        if let (Some(value_col), Some(count_col)) = (value_counts.get(0), value_counts.get(1)) {
            if let (Some(value), Some(count)) = (value_col.get(i), count_col.get(i)) {
                let value_str = value.str_value();
                if let Ok(count_u32) = count.try_extract::<u32>() {
                    top_values.push((value_str.to_string(), count_u32 as usize));
                }
            }
        }
    }

    let min = if let Ok(str_series) = series.str() {
        let mut min_val: Option<String> = None;
        for s in str_series.iter().flatten() {
            let s_str = s.to_string();
            min_val = match min_val {
                None => Some(s_str.clone()),
                Some(ref current) if s_str < *current => Some(s_str),
                Some(current) => Some(current),
            };
        }
        min_val
    } else {
        None
    };

    let max = if let Ok(str_series) = series.str() {
        let mut max_val: Option<String> = None;
        for s in str_series.iter().flatten() {
            let s_str = s.to_string();
            max_val = match max_val {
                None => Some(s_str.clone()),
                Some(ref current) if s_str > *current => Some(s_str),
                Some(current) => Some(current),
            };
        }
        max_val
    } else {
        None
    };

    Ok(CategoricalStatistics {
        unique_count,
        mode,
        top_values,
        min,
        max,
    })
}

fn infer_distribution(
    _series: &Series,
    sample: &Series,
    sample_size: usize,
    is_sampled: bool,
) -> DistributionInfo {
    if sample_size < 3 {
        return DistributionInfo {
            distribution_type: DistributionType::Unknown,
            confidence: 0.0,
            sample_size,
            is_sampled,
            fit_quality: None,
            all_distribution_pvalues: HashMap::new(),
        };
    }

    let max_convert = 10000.min(sample.len());
    let values: Vec<f64> = if sample.len() > max_convert {
        let all_values = get_numeric_values_as_f64(sample);
        all_values.into_iter().take(max_convert).collect()
    } else {
        get_numeric_values_as_f64(sample)
    };

    if values.is_empty() {
        return DistributionInfo {
            distribution_type: DistributionType::Unknown,
            confidence: 0.0,
            sample_size,
            is_sampled,
            fit_quality: None,
            all_distribution_pvalues: HashMap::new(),
        };
    }

    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    let std = variance.sqrt();

    let mut candidates: Vec<(DistributionType, f64, f64)> = Vec::new();

    let normal_fit = calculate_normal_fit_quality(&values, mean, std);
    let normal_confidence = normal_fit.min(0.95);
    candidates.push((DistributionType::Normal, normal_fit, normal_confidence));

    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() > 10 {
        let lognormal_fit = calculate_lognormal_fit_quality(&values);
        let lognormal_confidence = lognormal_fit.min(0.95);
        if lognormal_fit > 0.01 {
            candidates.push((
                DistributionType::LogNormal,
                lognormal_fit,
                lognormal_confidence,
            ));
        }
    }

    let uniformity_pvalue = chi_square_uniformity_test(&values);
    let uniform_fit = calculate_uniform_fit_quality(&values);
    let uniform_confidence = uniformity_pvalue.min(0.95);
    candidates.push((DistributionType::Uniform, uniform_fit, uniform_confidence));

    let powerlaw_pvalue = test_power_law(&values);
    let powerlaw_fit = calculate_power_law_fit_quality(&values);
    let powerlaw_confidence = powerlaw_pvalue.min(0.95);
    if powerlaw_pvalue > 0.0 {
        candidates.push((
            DistributionType::PowerLaw,
            powerlaw_fit,
            powerlaw_confidence,
        ));
    }

    let positive_exp: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_exp.len() > 10 {
        let exp_score = test_exponential(&values);
        let exp_fit = calculate_exponential_fit_quality(&values);
        let exp_confidence = exp_score.min(0.95);
        if exp_score > 0.0 {
            candidates.push((DistributionType::Exponential, exp_fit, exp_confidence));
        }
    }

    let beta_score = test_beta(&values);
    let beta_fit = calculate_beta_fit_quality(&values);
    let beta_confidence = beta_score.min(0.95);
    if beta_score > 0.0 {
        candidates.push((DistributionType::Beta, beta_fit, beta_confidence));
    }

    let gamma_score = test_gamma(&values);
    let gamma_fit = calculate_gamma_fit_quality(&values);
    let gamma_confidence = gamma_score.min(0.95);
    if gamma_score > 0.0 {
        candidates.push((DistributionType::Gamma, gamma_fit, gamma_confidence));
    }

    let chi2_score = test_chi_squared(&values);
    let chi2_fit = calculate_chi_squared_fit_quality(&values);
    let chi2_confidence = chi2_score.min(0.95);
    if chi2_score > 0.0 {
        candidates.push((DistributionType::ChiSquared, chi2_fit, chi2_confidence));
    }

    let t_score = test_students_t(&values);
    let t_fit = calculate_students_t_fit_quality(&values);
    let t_confidence = t_score.min(0.95);
    if t_score > 0.0 {
        candidates.push((DistributionType::StudentsT, t_fit, t_confidence));
    }

    let poisson_score = test_poisson(&values);
    let poisson_fit = calculate_poisson_fit_quality(&values);
    let poisson_confidence = poisson_score.min(0.95);
    if poisson_score > 0.0 {
        candidates.push((DistributionType::Poisson, poisson_fit, poisson_confidence));
    }

    let bernoulli_score = test_bernoulli(&values);
    let bernoulli_fit = calculate_bernoulli_fit_quality(&values);
    let bernoulli_confidence = bernoulli_score.min(0.95);
    let binary_count = values.iter().filter(|&&v| v == 0.0 || v == 1.0).count();
    if bernoulli_score > 0.01 && binary_count as f64 / values.len() as f64 > 0.9 {
        candidates.push((
            DistributionType::Bernoulli,
            bernoulli_fit,
            bernoulli_confidence,
        ));
    }

    let max_value = values.iter().fold(0.0f64, |a, &b| a.max(b));
    if max_value > 1.0 {
        let binomial_score = test_binomial(&values);
        let binomial_fit = calculate_binomial_fit_quality(&values);
        let binomial_confidence = binomial_score.min(0.95);
        let threshold = if values.len() > 5000 { 0.0 } else { 0.01 };
        if binomial_score > threshold {
            candidates.push((
                DistributionType::Binomial,
                binomial_fit,
                binomial_confidence,
            ));
        }
    }

    if values.len() <= 10000 {
        let non_negative_int_count = values
            .iter()
            .filter(|&&v| v >= 0.0 && v == v.floor() && v.is_finite())
            .count();

        if non_negative_int_count as f64 / values.len() as f64 > 0.9 {
            let geometric_score = test_geometric(&values);
            let geometric_fit = calculate_geometric_fit_quality(&values);
            let geometric_confidence = geometric_score.min(0.95);
            if geometric_score > 0.01 {
                candidates.push((
                    DistributionType::Geometric,
                    geometric_fit,
                    geometric_confidence,
                ));
            }
        }
    }

    let positive_weibull: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_weibull.len() > 10 {
        let weibull_score = test_weibull(&values);
        let weibull_fit = calculate_weibull_fit_quality(&values);
        let weibull_confidence = weibull_score.min(0.95);
        if weibull_score > 0.01 {
            candidates.push((DistributionType::Weibull, weibull_fit, weibull_confidence));
        }
    }

    // Build HashMap of all distribution p-values for reuse in detail page
    let mut all_pvalues = HashMap::new();
    for (dist_type, _, confidence) in &candidates {
        all_pvalues.insert(*dist_type, *confidence);
    }

    if let Some(best) = candidates.iter().max_by(|a, b| {
        // Primary comparison: p-value (confidence)
        let p_cmp = a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal);
        if p_cmp != std::cmp::Ordering::Equal {
            return p_cmp;
        }
        if (a.2 - b.2).abs() < 0.01 {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            p_cmp
        }
    }) {
        DistributionInfo {
            distribution_type: best.0,
            confidence: best.2,
            sample_size,
            is_sampled,
            fit_quality: Some(best.1),
            all_distribution_pvalues: all_pvalues,
        }
    } else {
        DistributionInfo {
            distribution_type: DistributionType::Unknown,
            confidence: 0.50,
            sample_size,
            is_sampled,
            fit_quality: Some(0.5),
            all_distribution_pvalues: HashMap::new(),
        }
    }
}

fn approximate_shapiro_wilk(values: &[f64]) -> (Option<f64>, Option<f64>) {
    let n = values.len();
    if n < 3 {
        return (None, None);
    }

    let mean: f64 = values.iter().sum::<f64>() / n as f64;
    let variance: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let std = variance.sqrt();

    if std == 0.0 {
        return (None, None);
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut sum_expected_sq = 0.0;
    let mut sum_data_sq = 0.0;
    let mut sum_product = 0.0;

    for (i, &value) in sorted.iter().enumerate() {
        let p = (i as f64 + 1.0 - 0.375) / (n as f64 + 0.25);
        let expected_quantile = normal_quantile(p);
        let standardized_value = (value - mean) / std;

        sum_expected_sq += expected_quantile * expected_quantile;
        sum_data_sq += standardized_value * standardized_value;
        sum_product += expected_quantile * standardized_value;
    }

    let sw_stat = if sum_expected_sq > 0.0 && sum_data_sq > 0.0 {
        (sum_product * sum_product) / (sum_expected_sq * sum_data_sq)
    } else {
        0.0
    };

    let sw_stat = sw_stat.clamp(0.0, 1.0);

    let skewness: f64 = values
        .iter()
        .map(|v| ((v - mean) / std).powi(3))
        .sum::<f64>()
        / n as f64;

    let kurtosis: f64 = values
        .iter()
        .map(|v| ((v - mean) / std).powi(4))
        .sum::<f64>()
        / n as f64;

    let skew_penalty = (skewness.abs() / 2.0).min(1.0);
    let kurt_penalty = ((kurtosis - 3.0).abs() / 2.0).min(1.0);

    let w_factor = sw_stat;
    let penalty_factor = 1.0 - (skew_penalty + kurt_penalty) / 2.0;
    let pvalue = (w_factor * 0.7 + penalty_factor * 0.3).clamp(0.0, 1.0);

    (Some(sw_stat), Some(pvalue))
}

/// Calculates chi-square statistic for uniformity test.
///
/// Divides values into 10 bins and compares observed vs expected frequencies.
/// Returns the raw chi-square value, or None if the test cannot be performed
/// (insufficient data or zero range).
fn calculate_chi_square_uniformity(values: &[f64]) -> Option<f64> {
    let n = values.len();
    if n < 10 {
        return None;
    }

    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let range = max - min;

    if range == 0.0 {
        return None;
    }

    let bins = 10;
    let mut counts = vec![0; bins];

    for &v in values {
        let bin = (((v - min) / range) * bins as f64) as usize;
        let bin = bin.min(bins - 1);
        counts[bin] += 1;
    }

    let expected = n as f64 / bins as f64;
    let chi_square: f64 = counts
        .iter()
        .map(|&count| {
            let diff = count as f64 - expected;
            diff * diff / expected
        })
        .sum();

    Some(chi_square)
}

fn kolmogorov_smirnov_test<F>(values: &[f64], theoretical_cdf: F) -> f64
where
    F: Fn(f64) -> f64,
{
    if values.is_empty() {
        return 1.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = sorted.len();
    let mut max_distance: f64 = 0.0;

    for (i, &x) in sorted.iter().enumerate() {
        let empirical_cdf = (i + 1) as f64 / n as f64;
        let theoretical_cdf_val = theoretical_cdf(x);
        let distance = (empirical_cdf - theoretical_cdf_val).abs();
        max_distance = max_distance.max(distance);
    }

    max_distance
}

fn chi_square_uniformity_test(values: &[f64]) -> f64 {
    if let Some(chi_square) = calculate_chi_square_uniformity(values) {
        (-chi_square / 20.0).exp().clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn estimate_power_law_mle(values: &[f64]) -> Option<(f64, f64)> {
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return None;
    }

    let mut sorted = positive_values.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let xmin = sorted[0];
    let sum_log_ratio: f64 = sorted.iter().map(|&x| (x / xmin).ln()).sum();

    if sum_log_ratio <= 0.0 {
        return None;
    }

    let n = sorted.len() as f64;
    let alpha = 1.0 + n / sum_log_ratio;
    if !(1.5..=4.0).contains(&alpha) {
        return None;
    }

    Some((xmin, alpha))
}

fn power_law_ks_test(values: &[f64], xmin: f64, alpha: f64) -> f64 {
    let mut filtered: Vec<f64> = values.iter().filter(|&&v| v >= xmin).copied().collect();
    if filtered.is_empty() {
        return 1.0;
    }
    filtered.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = filtered.len();
    let mut max_distance: f64 = 0.0;

    for (i, &x) in filtered.iter().enumerate() {
        let empirical_cdf = (i + 1) as f64 / n as f64;
        let theoretical_cdf = powerlaw_cdf(x, xmin, alpha);
        let distance = (empirical_cdf - theoretical_cdf).abs();
        max_distance = max_distance.max(distance);
    }

    max_distance
}

fn approximate_ks_pvalue(ks_stat: f64, n: usize) -> f64 {
    if n < 1 || ks_stat <= 0.0 {
        return 0.0;
    }
    if ks_stat >= 1.0 {
        return 0.0;
    }

    let sqrt_n = (n as f64).sqrt();
    let exponent = -2.0 * (sqrt_n * ks_stat).powi(2);
    let p_value = 2.0 * exponent.exp();

    p_value.clamp(0.0, 1.0)
}

fn chi_square_goodness_of_fit(observed: &[usize], expected: &[f64]) -> f64 {
    if observed.len() != expected.len() || observed.is_empty() {
        return 0.0;
    }

    let mut chi_square = 0.0;
    let mut total_observed = 0;
    let mut total_expected = 0.0;

    for (obs, exp) in observed.iter().zip(expected.iter()) {
        total_observed += obs;
        total_expected += exp;
        if *exp > 0.0 {
            let diff = *obs as f64 - exp;
            chi_square += (diff * diff) / exp;
        }
    }

    let df = (observed.len() as i32 - 2).max(1) as usize;
    // For smaller df, use a more conservative approximation
    let p_value = if df > 30 {
        // Large df: use normal approximation
        let z = ((2.0 * chi_square).sqrt() - (2.0 * df as f64 - 1.0).sqrt()).max(0.0);
        (-z * z / 2.0).exp()
    } else {
        // Small df: use exponential decay approximation
        (-chi_square / (2.0 * df as f64)).exp()
    };

    p_value.clamp(0.0, 1.0)
}

fn power_law_log_likelihood(values: &[f64], xmin: f64, alpha: f64) -> f64 {
    let filtered: Vec<f64> = values.iter().filter(|&&v| v >= xmin).copied().collect();
    if filtered.is_empty() {
        return f64::NEG_INFINITY;
    }

    let n = filtered.len() as f64;
    let sum_log = filtered.iter().map(|&x| (x / xmin).ln()).sum::<f64>();

    n * (alpha - 1.0).ln() - n * xmin.ln() - alpha * sum_log
}

fn weibull_log_likelihood(values: &[f64], shape: f64, scale: f64) -> f64 {
    let positive: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive.is_empty() || shape <= 0.0 || scale <= 0.0 {
        return f64::NEG_INFINITY;
    }

    let n = positive.len() as f64;
    let sum_power = positive
        .iter()
        .map(|&x| (x / scale).powf(shape))
        .sum::<f64>();
    let sum_log = positive.iter().map(|&x| x.ln()).sum::<f64>();

    n * (shape / scale).ln() + (shape - 1.0) * sum_log - sum_power
}

fn test_power_law(values: &[f64]) -> f64 {
    let Some((xmin, alpha)) = estimate_power_law_mle(values) else {
        return 0.0;
    };

    let ks_stat = power_law_ks_test(values, xmin, alpha);

    let positive_values: Vec<f64> = values.iter().filter(|&&v| v >= xmin).copied().collect();
    let n = positive_values.len();
    if n < 10 {
        return 0.0;
    }

    let mut p_value = approximate_ks_pvalue(ks_stat, n);

    let uniform_p = chi_square_uniformity_test(values);
    if uniform_p > 0.1 {
        p_value *= 0.3;
    }

    if p_value > 0.05 {
        let n_f64 = n as f64;
        let mean = positive_values.iter().sum::<f64>() / n_f64;
        let scale = mean / 1.0;
        let shape = 1.5;

        let pl_likelihood = power_law_log_likelihood(values, xmin, alpha);
        let wb_likelihood = weibull_log_likelihood(values, shape, scale);

        if wb_likelihood > pl_likelihood + 5.0 {
            p_value *= 0.5;
        }
    }

    p_value
}

fn test_exponential(values: &[f64]) -> f64 {
    // Use KS test for exponential distribution (same as fit quality for consistency)
    calculate_exponential_fit_quality(values)
}

fn test_beta(values: &[f64]) -> f64 {
    if values.is_empty() || values.len() < 10 {
        return 0.0;
    }

    let has_negatives = values.iter().any(|&v| v < 0.0);
    if has_negatives {
        return 0.0;
    }

    let values_in_range = values.iter().filter(|&&v| (0.0..=1.0).contains(&v)).count();
    let ratio_in_range = values_in_range as f64 / values.len() as f64;
    if ratio_in_range < 0.85 {
        return 0.0;
    }

    let valid_values: Vec<f64> = values
        .iter()
        .filter(|&&v| (0.0..=1.0).contains(&v))
        .copied()
        .collect();

    if valid_values.len() < 10 {
        return 0.0;
    }

    let mean = valid_values.iter().sum::<f64>() / valid_values.len() as f64;
    let variance: f64 = valid_values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
        / (valid_values.len() - 1) as f64;

    if variance <= 0.0 || mean <= 0.0 || mean >= 1.0 {
        return 0.0;
    }

    let temp = mean * (1.0 - mean) / variance - 1.0;
    if temp <= 0.0 {
        return 0.0; // Invalid parameters
    }
    let alpha = mean * temp;
    let beta = (1.0 - mean) * temp;

    if alpha <= 0.0 || beta <= 0.0 {
        return 0.0;
    }
    let alpha = alpha.min(1000.0);
    let beta = beta.min(1000.0);

    let ks_stat = kolmogorov_smirnov_test(&valid_values, |x| beta_cdf(x, alpha, beta));
    let n = valid_values.len();
    let ks_pvalue = approximate_ks_pvalue(ks_stat, n);

    let max_variance = mean * (1.0 - mean);
    let variance_score = if max_variance > 0.0 {
        (1.0 - (variance / max_variance).min(1.0)).max(0.0)
    } else {
        0.0
    };
    let confidence = if ks_pvalue > 0.1 {
        ks_pvalue.min(0.95)
    } else if ks_pvalue > 0.05 {
        (ks_pvalue * 7.0).min(0.7)
    } else if ks_pvalue > 0.01 {
        (ks_pvalue * 0.7 + variance_score * 0.3).min(0.5)
    } else {
        (ks_pvalue * 0.3 + variance_score * 0.7).min(0.4)
    };

    confidence.max(0.1)
}

// Test Gamma distribution
fn gamma_log_likelihood(values: &[f64], shape: f64, scale: f64) -> f64 {
    let positive: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive.is_empty() || shape <= 0.0 || scale <= 0.0 {
        return f64::NEG_INFINITY;
    }

    let n = positive.len() as f64;
    let sum_log = positive.iter().map(|&x| x.ln()).sum::<f64>();
    let sum_x = positive.iter().sum::<f64>();

    let gamma_term = ln_gamma_approx(shape);
    -n * gamma_term - n * shape * scale.ln() + (shape - 1.0) * sum_log - sum_x / scale
}

fn lognormal_log_likelihood(values: &[f64], mu: f64, sigma: f64) -> f64 {
    let positive: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive.is_empty() || sigma <= 0.0 {
        return f64::NEG_INFINITY;
    }

    let n = positive.len() as f64;
    let sum_log = positive.iter().map(|&x| x.ln()).sum::<f64>();
    let sum_log_sq = positive.iter().map(|&x| (x.ln() - mu).powi(2)).sum::<f64>();

    -n / 2.0 * (2.0 * std::f64::consts::PI).ln()
        - n * sigma.ln()
        - sum_log
        - sum_log_sq / (2.0 * sigma * sigma)
}

fn test_gamma(values: &[f64]) -> f64 {
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    let mean = positive_values.iter().sum::<f64>() / positive_values.len() as f64;
    let variance: f64 = positive_values
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / (positive_values.len() - 1) as f64;

    if mean <= 0.0 || variance <= 0.0 {
        return 0.0;
    }

    let shape = (mean * mean) / variance;
    let scale = variance / mean;

    if shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }

    let ks_stat = kolmogorov_smirnov_test(&positive_values, |x| gamma_cdf(x, shape, scale));
    let n = positive_values.len();
    let mut p_value = approximate_ks_pvalue(ks_stat, n);

    let cv = variance.sqrt() / mean;
    let p_value_threshold = if n > 5000 {
        0.001 // Very low p-value threshold for large samples
    } else {
        0.01 // Standard threshold
    };
    let ks_stat_threshold = if n > 5000 {
        0.15 // Slightly higher KS stat threshold for large samples
    } else {
        0.12 // Standard threshold
    };
    if n > 5000 {
        if ks_stat < 0.3
            && p_value >= 0.0
            && shape > 0.1
            && shape < 500.0
            && scale > 0.001
            && scale < 500.0
            && cv > 0.1
            && cv < 5.0
        {
            p_value = (ks_stat * 6.0).clamp(0.05, 0.30);
        } else if ks_stat < 0.5 && p_value >= 0.0 && shape > 0.1 && scale > 0.001 {
            p_value = (ks_stat * 3.0).clamp(0.01, 0.15);
        }
    } else if p_value < p_value_threshold
        && ks_stat < ks_stat_threshold
        && p_value > 0.0
        && shape > 0.5
        && shape < 100.0
        && scale > 0.01
        && scale < 100.0
        && cv > 0.2
        && cv < 2.0
    {
        p_value = (ks_stat * 2.0).min(0.10).max(p_value);
    }

    let gamma_likelihood = gamma_log_likelihood(values, shape, scale);

    let cv = variance.sqrt() / mean;
    let weibull_shape = if cv < 0.5 {
        2.0
    } else if cv < 1.0 {
        1.5
    } else {
        1.0
    };
    let gamma_approx = match weibull_shape {
        1.0 => 1.0,
        1.5 => 0.9,
        2.0 => 0.886,
        _ => 0.9,
    };
    let weibull_scale = mean / gamma_approx;

    if weibull_scale > 0.0 && weibull_shape > 0.0 && weibull_scale < 1000.0 && weibull_shape < 100.0
    {
        let weibull_likelihood = weibull_log_likelihood(values, weibull_shape, weibull_scale);

        let is_discrete = values.iter().all(|&v| v == v.floor());
        if is_discrete && p_value > 0.0 {
            p_value *= 0.1;
        } else if p_value > 0.1 && weibull_likelihood > gamma_likelihood + 5.0 {
            p_value *= 0.5;
        } else if p_value > 0.05 && weibull_likelihood > gamma_likelihood + 5.0 {
            p_value *= 0.7;
        } else if p_value > 0.05 && weibull_likelihood > gamma_likelihood + 2.0 {
            p_value *= 0.85;
        }
    }

    let lognormal_p = calculate_lognormal_fit_quality(values);
    if p_value > 0.05 && lognormal_p > 0.05 {
        let e_x = mean;
        let var_x = variance;
        let sigma_sq = (1.0 + var_x / (e_x * e_x)).ln();
        let mu = e_x.ln() - sigma_sq / 2.0;
        let sigma = sigma_sq.sqrt();

        let lognormal_likelihood = lognormal_log_likelihood(values, mu, sigma);
        if lognormal_likelihood > gamma_likelihood + 5.0 {
            p_value *= 0.5;
        }
    }

    p_value
}

// Test Chi-squared distribution
fn test_chi_squared(values: &[f64]) -> f64 {
    if values.is_empty() || values.len() < 10 {
        return 0.0;
    }

    let positive_values: Vec<f64> = values
        .iter()
        .filter(|&&v| v >= 0.0 && v.is_finite())
        .copied()
        .collect();

    let ratio_positive = positive_values.len() as f64 / values.len() as f64;
    if ratio_positive < 0.95 {
        return 0.0;
    }

    if positive_values.len() < 10 {
        return 0.0;
    }

    let mean = positive_values.iter().sum::<f64>() / positive_values.len() as f64;
    let variance: f64 = positive_values
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / (positive_values.len() - 1) as f64;

    if mean <= 0.0 {
        return 0.0;
    }

    let expected_var = 2.0 * mean;
    let variance_ratio = if expected_var > 0.0 {
        (variance / expected_var).min(expected_var / variance)
    } else {
        0.0
    };

    let variance_error = (variance - expected_var).abs() / expected_var;
    if variance_error > 0.1 {
        return 0.0;
    }

    let df = mean;
    if df <= 0.0 || df > 1000.0 {
        return 0.0;
    }

    let ks_stat = kolmogorov_smirnov_test(&positive_values, |x| chi_squared_cdf(x, df));
    let n = positive_values.len();
    let ks_pvalue = approximate_ks_pvalue(ks_stat, n);

    let confidence = if ks_pvalue > 0.1 {
        (ks_pvalue * 0.8 + variance_ratio * 0.2).min(0.95)
    } else if ks_pvalue > 0.05 {
        (ks_pvalue * 0.6 + variance_ratio * 0.4).min(0.7)
    } else if ks_pvalue > 0.01 {
        (ks_pvalue * 0.3 + variance_ratio * 0.7).min(0.5)
    } else {
        (variance_ratio * 0.5).min(0.3)
    };

    confidence.max(0.05)
}

// Test Student's t distribution
fn test_students_t(values: &[f64]) -> f64 {
    if values.len() < 10 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 =
        values.iter().map(|&v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    if variance <= 0.0 {
        return 0.0;
    }

    let df = if variance > 1.0 {
        2.0 * variance / (variance - 1.0)
    } else {
        3.0
    };
    let df = df.clamp(1.0, 100.0);

    // KS test against Student's t distribution
    let ks_stat = kolmogorov_smirnov_test(values, |x| students_t_cdf(x, df));
    let n = values.len();
    approximate_ks_pvalue(ks_stat, n)
}

fn test_poisson(values: &[f64]) -> f64 {
    let non_negative: Vec<f64> = values
        .iter()
        .filter(|&&v| v >= 0.0 && v == v.floor())
        .copied()
        .collect();
    if non_negative.len() < 10 {
        return 0.0;
    }

    let lambda = non_negative.iter().sum::<f64>() / non_negative.len() as f64;
    if lambda <= 0.0 {
        return 0.0;
    }

    let variance: f64 = non_negative
        .iter()
        .map(|&v| (v - lambda).powi(2))
        .sum::<f64>()
        / (non_negative.len() - 1) as f64;
    let variance_ratio = (variance / lambda).min(lambda / variance);
    if variance_ratio < 0.7 {
        return 0.0;
    }

    let max_val = non_negative.iter().fold(0.0f64, |a, &b| a.max(b)) as usize;
    let num_bins = (max_val.min(15) + 1).clamp(5, 20); // At least 5 bins, max 20

    let mut observed = vec![0; num_bins];
    for &v in &non_negative {
        let bin = (v as usize).min(num_bins - 1);
        observed[bin] += 1;
    }

    let mut expected = vec![0.0; num_bins];
    for (bin, exp) in expected.iter_mut().enumerate() {
        let k = bin;
        // Poisson PMF: lambda^k * exp(-lambda) / k!
        let mut factorial = 1.0;
        for i in 1..=k {
            factorial *= i as f64;
        }
        let pmf = lambda.powi(k as i32) * (-lambda).exp() / factorial;
        *exp = pmf * non_negative.len() as f64;
    }

    // Chi-square goodness-of-fit test
    chi_square_goodness_of_fit(&observed, &expected)
}

fn test_bernoulli(values: &[f64]) -> f64 {
    // Bernoulli: values should be 0 or 1
    let binary_values: Vec<f64> = values
        .iter()
        .filter(|&&v| v == 0.0 || v == 1.0)
        .copied()
        .collect();
    if binary_values.len() < 10 || binary_values.len() < values.len() / 2 {
        return 0.0;
    }

    // Count occurrences of 0 and 1
    let count_0 = binary_values.iter().filter(|&&v| v == 0.0).count();
    let count_1 = binary_values.iter().filter(|&&v| v == 1.0).count();
    let n = binary_values.len();

    let p = count_1 as f64 / n as f64;
    if p <= 0.0 || p >= 1.0 {
        return 0.0;
    }

    // Expected frequencies: n * (1-p) for 0, n * p for 1
    let expected = vec![n as f64 * (1.0 - p), n as f64 * p];
    let observed = vec![count_0, count_1];

    // Chi-square goodness-of-fit test
    chi_square_goodness_of_fit(&observed, &expected)
}

fn test_binomial(values: &[f64]) -> f64 {
    // Binomial: non-negative integer values
    let non_negative_int: Vec<f64> = values
        .iter()
        .filter(|&&v| v >= 0.0 && v == v.floor())
        .copied()
        .collect();
    if non_negative_int.len() < 10 || non_negative_int.len() < values.len() / 2 {
        return 0.0;
    }

    let max_val = non_negative_int.iter().fold(0.0f64, |a, &b| a.max(b));
    let mean = non_negative_int.iter().sum::<f64>() / non_negative_int.len() as f64;

    let variance: f64 = non_negative_int
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / (non_negative_int.len() - 1) as f64;

    if mean <= 0.0 || max_val <= 0.0 {
        return 0.0;
    }

    // Estimate n (number of trials) from max value
    let n = max_val as usize;
    if n == 0 {
        return 0.0;
    }

    let p = mean / n as f64;
    if p <= 0.0 || p >= 1.0 {
        return 0.0;
    }

    let expected_var = n as f64 * p * (1.0 - p);
    let variance_ratio = if expected_var > 0.0 {
        (variance / expected_var).min(expected_var / variance)
    } else {
        0.0
    };
    if variance_ratio < 0.7 {
        return 0.0; // Variance too different - not Binomial
    }

    let num_bins = (n + 1).min(20);
    let mut observed = vec![0; num_bins];
    for &v in &non_negative_int {
        let bin = (v as usize).min(num_bins - 1);
        observed[bin] += 1;
    }

    let mut expected = vec![0.0; num_bins];
    for (bin, exp) in expected.iter_mut().enumerate() {
        let k = bin;
        if k <= n {
            let coeff = binomial_coeff(n, k);
            let pmf = coeff * p.powi(k as i32) * (1.0 - p).powi((n - k) as i32);
            *exp = pmf * non_negative_int.len() as f64;
        }
    }

    // Chi-square goodness-of-fit test
    let mut score = chi_square_goodness_of_fit(&observed, &expected);

    let n_samples = non_negative_int.len();
    if n_samples > 5000 && score > 0.0 {
        if score < 0.01 {
            score = (score * 200.0).min(0.20);
        } else if score < 0.1 {
            score = (score * 2.0).min(0.25);
        }
        if score > 0.0 && variance_ratio >= 0.7 {
            score = score.max(0.05);
        }
    }

    score
}

fn test_geometric(values: &[f64]) -> f64 {
    if values.is_empty() || values.len() < 10 {
        return 0.0;
    }

    let process_limit = 5000.min(values.len());
    let sample: &[f64] = &values[..process_limit];

    let non_negative_int: Vec<f64> = sample
        .iter()
        .filter(|&&v| v >= 0.0 && v == v.floor() && v.is_finite())
        .copied()
        .collect();

    if non_negative_int.len() < 10 {
        return 0.0;
    }

    let mean = non_negative_int.iter().sum::<f64>() / non_negative_int.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }

    let max_seen = non_negative_int.iter().fold(0.0f64, |a, &b| a.max(b));
    if max_seen > 100.0 {
        return 0.0;
    }

    if mean > 30.0 {
        return 0.0;
    }

    // Variance = E[X^2] - (E[X])^2
    let variance: f64 = non_negative_int
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / (non_negative_int.len() - 1) as f64;
    if variance <= 0.0 {
        return 0.0;
    }

    let expected_var = mean * (mean + 1.0);
    if expected_var <= 0.0 {
        return 0.0;
    }

    let variance_ratio = (variance / expected_var).min(expected_var / variance);
    if variance_ratio < 0.5 {
        return 0.0;
    }

    let p = 1.0 / (mean + 1.0);
    if p <= 0.0 || p >= 1.0 {
        return 0.0;
    }

    // Bin values for chi-square test
    let max_bin = (max_seen as usize + 1).min(50);
    let mut observed = vec![0; max_bin];
    let mut expected = vec![0.0; max_bin];

    for &v in &non_negative_int {
        let bin = v as usize;
        if bin < max_bin {
            observed[bin] += 1;
        }
    }

    // Calculate expected frequencies using Geometric PMF: P(X=k) = p * (1-p)^k
    // For k = 0, 1, 2, ..., max_bin-1
    let n = non_negative_int.len() as f64;
    let mut total_pmf = 0.0;
    for (k, exp) in expected.iter_mut().enumerate().take(max_bin) {
        let pmf = p * (1.0 - p).powi(k as i32);
        *exp = pmf;
        total_pmf += pmf;
    }

    if total_pmf > 0.0 {
        for exp in &mut expected {
            *exp = (*exp / total_pmf) * n;
        }
    } else {
        let uniform_prob = 1.0 / max_bin as f64;
        expected.fill(uniform_prob * n);
    }

    let min_expected = expected.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let chi_square_score = if min_expected > 0.1 {
        chi_square_goodness_of_fit(&observed, &expected)
    } else {
        variance_ratio
    };

    let confidence = if chi_square_score > 0.1 {
        (chi_square_score * 0.8 + variance_ratio * 0.2).min(0.95)
    } else if chi_square_score > 0.05 {
        (chi_square_score * 0.6 + variance_ratio * 0.4).min(0.7)
    } else if chi_square_score > 0.01 {
        (chi_square_score * 0.3 + variance_ratio * 0.7).min(0.5)
    } else {
        (variance_ratio * 0.5).min(0.3)
    };

    confidence.max(0.05)
}

fn test_weibull(values: &[f64]) -> f64 {
    let has_negatives = values.iter().any(|&v| v < 0.0);
    if has_negatives {
        return 0.0;
    }

    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    let n = positive_values.len() as f64;
    let mean = positive_values.iter().sum::<f64>() / n;
    let variance: f64 = positive_values
        .iter()
        .map(|&v| (v - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);

    if mean <= 0.0 || variance <= 0.0 {
        return 0.0;
    }

    let cv = variance.sqrt() / mean;

    let candidate_shapes: Vec<f64> = if cv < 0.3 {
        vec![3.0, 2.5, 2.0, 1.8]
    } else if cv < 0.6 {
        vec![2.0, 1.8, 1.5, 1.3]
    } else if cv < 0.8 {
        vec![1.5, 1.3, 1.2, 1.1]
    } else if cv < 1.0 {
        vec![1.2, 1.1, 1.0, 0.9]
    } else {
        vec![1.0, 0.9, 0.8, 0.7]
    };

    let mut best_shape = candidate_shapes[0];
    let mut best_scale = 1.0;
    let mut best_ks_stat = 1.0;

    for &candidate_shape in &candidate_shapes {
        let k_inv = 1.0 / candidate_shape;
        let gamma_1_plus_1_over_shape = ln_gamma_approx(1.0 + k_inv).exp();
        let candidate_scale = mean / gamma_1_plus_1_over_shape;

        if candidate_scale > 0.0 && candidate_scale < 1000.0 {
            let ks_stat = kolmogorov_smirnov_test(&positive_values, |x| {
                weibull_cdf(x, candidate_shape, candidate_scale)
            });
            if ks_stat < best_ks_stat {
                best_ks_stat = ks_stat;
                best_shape = candidate_shape;
                best_scale = candidate_scale;
            }
        }
    }

    let shape = best_shape;
    let scale = best_scale;

    // Validate parameters
    if scale <= 0.0 || shape <= 0.0 || scale > 1000.0 || shape > 100.0 {
        return 0.0;
    }

    // KS test against Weibull distribution using best parameters
    let n_usize = positive_values.len();
    let mut p_value = approximate_ks_pvalue(best_ks_stat, n_usize);

    let gamma_shape = (mean * mean) / variance; // Method of moments for Gamma
    let gamma_scale = variance / mean;

    if gamma_shape > 0.0 && gamma_scale > 0.0 && gamma_shape < 1000.0 && gamma_scale < 1000.0 {
        let wb_likelihood = weibull_log_likelihood(values, shape, scale);
        let gamma_likelihood = gamma_log_likelihood(values, gamma_shape, gamma_scale);

        if p_value > 0.1 && gamma_likelihood > wb_likelihood + 5.0 {
            p_value *= 0.4;
        } else if p_value > 0.05 && gamma_likelihood > wb_likelihood + 5.0 {
            p_value *= 0.5;
        } else if p_value > 0.05 && gamma_likelihood > wb_likelihood + 2.0 {
            // Gamma fits somewhat better - moderate penalty
            p_value *= 0.7;
        } else if p_value > 0.02 && gamma_likelihood > wb_likelihood + 5.0 {
            // Gamma fits better but p-value is low - light penalty
            p_value *= 0.85;
        }
    }

    let shape_tolerance = if positive_values.len() > 5000 {
        0.5
    } else {
        0.3
    };
    if (shape - 1.0).abs() < shape_tolerance && p_value > 0.3 {
        let exp_lambda = 1.0 / mean;
        if exp_lambda > 0.0 {
            let exp_likelihood = -(positive_values.len() as f64) * exp_lambda.ln()
                - exp_lambda * positive_values.iter().sum::<f64>();
            let wb_likelihood = weibull_log_likelihood(values, shape, scale);

            if exp_likelihood > wb_likelihood + 0.5 {
                p_value *= 0.4;
            } else if exp_likelihood > wb_likelihood {
                p_value *= 0.6;
            } else if (shape - 1.0).abs() < 0.2 {
                p_value *= 0.7;
            }
        }
    }

    if let Some((xmin, alpha)) = estimate_power_law_mle(values) {
        let wb_likelihood = weibull_log_likelihood(values, shape, scale);
        let pl_likelihood = power_law_log_likelihood(values, xmin, alpha);

        if p_value > 0.05 && pl_likelihood > wb_likelihood + 5.0 {
            p_value *= 0.5;
        }
    }

    p_value
}

// Advanced distribution analysis computation
fn compute_advanced_distribution_analysis(
    column_name: &str,
    series: &Series,
    numeric_stats: &NumericStatistics,
    dist_info: &DistributionInfo,
    _sample_size: usize,
    is_sampled: bool,
) -> DistributionAnalysis {
    let max_values = 5000.min(series.len());
    let mut values: Vec<f64> = if series.len() > max_values {
        let limited_series = series.slice(0, max_values);
        get_numeric_values_as_f64(&limited_series)
    } else {
        get_numeric_values_as_f64(series)
    };

    // Sort values for Q-Q plot (all data if not sampled, or sampled data if >= threshold)
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sorted_sample_values = values.clone();
    let actual_sample_size = sorted_sample_values.len();

    // Compute distribution characteristics
    let (sw_stat, sw_pvalue) = if values.len() >= 3 {
        approximate_shapiro_wilk(&values)
    } else {
        (None, None)
    };
    let coefficient_of_variation = if numeric_stats.mean != 0.0 {
        numeric_stats.std / numeric_stats.mean.abs()
    } else {
        0.0
    };

    let mode = compute_mode(&values);

    let characteristics = DistributionCharacteristics {
        shapiro_wilk_stat: sw_stat,
        shapiro_wilk_pvalue: sw_pvalue,
        skewness: numeric_stats.skewness,
        kurtosis: numeric_stats.kurtosis,
        mean: numeric_stats.mean,
        median: numeric_stats.median,
        std_dev: numeric_stats.std,
        variance: numeric_stats.std * numeric_stats.std,
        coefficient_of_variation,
        mode,
    };

    let fit_quality = dist_info.fit_quality.unwrap_or_else(|| {
        calculate_fit_quality(
            &values,
            dist_info.distribution_type,
            numeric_stats.mean,
            numeric_stats.std,
        )
    });

    let outliers = compute_outlier_analysis(series, numeric_stats);

    let percentiles = PercentileBreakdown {
        p1: numeric_stats
            .percentiles
            .get(&1)
            .copied()
            .unwrap_or(f64::NAN),
        p5: numeric_stats
            .percentiles
            .get(&5)
            .copied()
            .unwrap_or(f64::NAN),
        p25: numeric_stats.q25,
        p50: numeric_stats.median,
        p75: numeric_stats.q75,
        p95: numeric_stats
            .percentiles
            .get(&95)
            .copied()
            .unwrap_or(f64::NAN),
        p99: numeric_stats
            .percentiles
            .get(&99)
            .copied()
            .unwrap_or(f64::NAN),
    };

    DistributionAnalysis {
        column_name: column_name.to_string(),
        distribution_type: dist_info.distribution_type,
        confidence: dist_info.confidence,
        fit_quality,
        characteristics,
        outliers,
        percentiles,
        sorted_sample_values,
        is_sampled,
        sample_size: actual_sample_size,
        all_distribution_pvalues: dist_info.all_distribution_pvalues.clone(),
    }
}

fn compute_mode(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    // Bin values and find most frequent bin
    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let range = max - min;

    if range == 0.0 {
        return Some(min);
    }

    let bins = 50.min(values.len());
    let mut bin_counts = vec![0; bins];
    let mut bin_sums = vec![0.0; bins];

    for &v in values {
        let bin = (((v - min) / range) * (bins - 1) as f64) as usize;
        let bin = bin.min(bins - 1);
        bin_counts[bin] += 1;
        bin_sums[bin] += v;
    }

    // Find bin with maximum count
    let max_bin = bin_counts
        .iter()
        .enumerate()
        .max_by_key(|(_, &count)| count)
        .map(|(idx, _)| idx);

    max_bin.map(|idx| bin_sums[idx] / bin_counts[idx] as f64)
}

/// Calculates fit quality (p-value) for a given distribution type.
///
/// Returns a value between 0.0 and 1.0, where higher values indicate better fit.
pub fn calculate_fit_quality(
    values: &[f64],
    dist_type: DistributionType,
    mean: f64,
    std: f64,
) -> f64 {
    match dist_type {
        DistributionType::Normal => calculate_normal_fit_quality(values, mean, std),
        DistributionType::LogNormal => calculate_lognormal_fit_quality(values),
        DistributionType::Uniform => calculate_uniform_fit_quality(values),
        DistributionType::PowerLaw => calculate_power_law_fit_quality(values),
        DistributionType::Exponential => calculate_exponential_fit_quality(values),
        DistributionType::Beta => calculate_beta_fit_quality(values),
        DistributionType::Gamma => calculate_gamma_fit_quality(values),
        DistributionType::ChiSquared => calculate_chi_squared_fit_quality(values),
        DistributionType::StudentsT => calculate_students_t_fit_quality(values),
        DistributionType::Poisson => calculate_poisson_fit_quality(values),
        DistributionType::Bernoulli => calculate_bernoulli_fit_quality(values),
        DistributionType::Binomial => calculate_binomial_fit_quality(values),
        DistributionType::Geometric => {
            if values.len() > 10000 {
                return 0.0;
            }
            calculate_geometric_fit_quality(values)
        }
        DistributionType::Weibull => calculate_weibull_fit_quality(values),
        DistributionType::Unknown => 0.5,
    }
}

fn calculate_normal_fit_quality(values: &[f64], mean: f64, std: f64) -> f64 {
    // Phase 2: Use KS test for proper statistical testing
    if values.is_empty() || std == 0.0 {
        return 0.0;
    }

    // KS test against normal distribution
    let ks_stat = kolmogorov_smirnov_test(values, |x| normal_cdf(x, mean, std));
    let n = values.len();
    approximate_ks_pvalue(ks_stat, n)
}

fn normal_quantile(p: f64) -> f64 {
    // Approximation of normal quantile function
    // Using Beasley-Springer-Moro algorithm approximation
    if p < 0.5 {
        -normal_quantile(1.0 - p)
    } else {
        let t = (-2.0 * (1.0 - p).ln()).sqrt();
        t - (2.515517 + 0.802853 * t + 0.010328 * t * t)
            / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t)
    }
}

fn calculate_lognormal_fit_quality(values: &[f64]) -> f64 {
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    // Estimate log-normal parameters from data
    let mean = positive_values.iter().sum::<f64>() / positive_values.len() as f64;
    let variance: f64 = positive_values
        .iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>()
        / (positive_values.len() - 1) as f64;

    // Log-normal parameters: mu and sigma from method of moments
    let e_x = mean;
    let var_x = variance;
    if e_x <= 0.0 || var_x <= 0.0 {
        return 0.0;
    }
    let sigma_sq = (1.0 + var_x / (e_x * e_x)).ln();
    let mu = e_x.ln() - sigma_sq / 2.0;
    let sigma = sigma_sq.sqrt();

    // KS test against log-normal distribution
    let ks_stat = kolmogorov_smirnov_test(&positive_values, |x| lognormal_cdf(x, mu, sigma));
    let n = positive_values.len();
    approximate_ks_pvalue(ks_stat, n)
}

fn calculate_uniform_fit_quality(values: &[f64]) -> f64 {
    if let Some(chi_square) = calculate_chi_square_uniformity(values) {
        let base_fit = 1.0 / (1.0 + chi_square / 17.0);
        base_fit.clamp(0.01, 1.0)
    } else {
        0.0
    }
}

fn calculate_power_law_fit_quality(values: &[f64]) -> f64 {
    test_power_law(values)
}

fn calculate_exponential_fit_quality(values: &[f64]) -> f64 {
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    // Estimate lambda (rate parameter) from mean
    let mean = positive_values.iter().sum::<f64>() / positive_values.len() as f64;
    if mean <= 0.0 {
        return 0.0;
    }
    let lambda = 1.0 / mean;

    // KS test against exponential distribution
    let ks_stat = kolmogorov_smirnov_test(&positive_values, |x| exponential_cdf(x, lambda));
    let n = positive_values.len();
    approximate_ks_pvalue(ks_stat, n)
}

fn calculate_beta_fit_quality(values: &[f64]) -> f64 {
    test_beta(values)
}

fn calculate_gamma_fit_quality(values: &[f64]) -> f64 {
    test_gamma(values) // Now returns p-value from KS test
}

fn calculate_chi_squared_fit_quality(values: &[f64]) -> f64 {
    test_chi_squared(values)
}

fn calculate_students_t_fit_quality(values: &[f64]) -> f64 {
    // Use same test as initial detection for consistency
    test_students_t(values)
}

fn calculate_poisson_fit_quality(values: &[f64]) -> f64 {
    test_poisson(values)
}

fn calculate_bernoulli_fit_quality(values: &[f64]) -> f64 {
    test_bernoulli(values)
}

fn calculate_binomial_fit_quality(values: &[f64]) -> f64 {
    test_binomial(values)
}

fn calculate_geometric_fit_quality(values: &[f64]) -> f64 {
    if values.len() > 10000 {
        return 0.0;
    }
    test_geometric(values)
}

fn calculate_weibull_fit_quality(values: &[f64]) -> f64 {
    test_weibull(values) // Now returns p-value from KS test
}

// CDF (Cumulative Distribution Function) implementations for histogram theoretical probabilities
fn normal_cdf(x: f64, mean: f64, std: f64) -> f64 {
    if std <= 0.0 {
        return if x < mean { 0.0 } else { 1.0 };
    }
    let z = (x - mean) / std;
    // Use error function approximation: CDF = 0.5 * (1 + erf(z/sqrt(2)))
    // erf approximation using Abramowitz and Stegun
    let z_normalized = z / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.3275911 * z_normalized.abs());
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let erf_approx = 1.0
        - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1)
            * t
            * (-z_normalized * z_normalized).exp();
    let erf_val = if z_normalized >= 0.0 {
        erf_approx
    } else {
        -erf_approx
    };
    0.5 * (1.0 + erf_val)
}

fn lognormal_cdf(x: f64, mu: f64, sigma: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if sigma <= 0.0 {
        return if x < mu.exp() { 0.0 } else { 1.0 };
    }
    // Lognormal: CDF(x) = Normal CDF of ln(x) with parameters mu, sigma
    normal_cdf(x.ln(), mu, sigma)
}

fn exponential_cdf(x: f64, lambda: f64) -> f64 {
    if x < 0.0 {
        return 0.0;
    }
    if lambda <= 0.0 {
        return if x < 0.0 { 0.0 } else { 1.0 };
    }
    // Exponential CDF: 1 - exp(-lambda * x)
    1.0 - (-lambda * x).exp()
}

fn powerlaw_cdf(x: f64, xmin: f64, alpha: f64) -> f64 {
    if x < xmin {
        return 0.0;
    }
    if alpha <= 1.0 {
        return if x >= xmin { 1.0 } else { 0.0 };
    }
    // Power law CDF: 1 - (x/xmin)^(-alpha + 1) for x >= xmin
    // Valid for alpha > 1
    if alpha <= 1.0 || xmin <= 0.0 {
        return if x >= xmin { 1.0 } else { 0.0 };
    }
    1.0 - (x / xmin).powf(-alpha + 1.0)
}

// Stirling's approximation for ln(gamma(z))
fn ln_gamma_approx(z: f64) -> f64 {
    if z <= 0.0 {
        return f64::NAN;
    }
    // Stirling's approximation: ln(Gamma(z)) ≈ (z - 0.5)*ln(z) - z + 0.5*ln(2π) + 1/(12z)
    if z > 50.0 {
        (z - 0.5) * z.ln() - z + 0.5 * (2.0 * std::f64::consts::PI).ln() + 1.0 / (12.0 * z)
    } else {
        // For smaller z, use iterative calculation (NO RECURSION)
        let mut result = 0.0;
        let mut z_val = z;
        // Cap iterations to prevent issues
        let max_iter = 100;
        let mut iter = 0;
        while z_val < 50.0 && iter < max_iter {
            result -= z_val.ln();
            z_val += 1.0;
            iter += 1;
        }
        // Use Stirling's approximation for final value (no recursion)
        if z_val >= 50.0 {
            result
                + ((z_val - 0.5) * z_val.ln() - z_val
                    + 0.5 * (2.0 * std::f64::consts::PI).ln()
                    + 1.0 / (12.0 * z_val))
        } else {
            result
        }
    }
}

// Beta distribution CDF (requires incomplete beta function approximation)
fn beta_cdf(x: f64, alpha: f64, beta: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    if alpha <= 0.0 || beta <= 0.0 {
        return 0.0;
    }
    // Approximation using normal approximation for large parameters
    // For small parameters, use simple approximation
    if alpha + beta > 50.0 {
        // Normal approximation
        let mean = alpha / (alpha + beta);
        let variance = (alpha * beta) / ((alpha + beta).powi(2) * (alpha + beta + 1.0));
        if variance > 0.0 {
            normal_cdf(x, mean, variance.sqrt())
        } else if x < mean {
            0.0
        } else {
            1.0
        }
    } else {
        // Simple polynomial approximation for small parameters
        // Beta CDF is related to incomplete beta function I_x(alpha, beta)
        // For small alpha, beta, use approximation: I_x(a,b) ≈ x^a * (1-x)^b / B(a,b) for small x
        // Simplified approximation using Stirling's approximation
        let ln_beta =
            ln_gamma_approx(alpha) + ln_gamma_approx(beta) - ln_gamma_approx(alpha + beta);
        let beta_const = ln_beta.exp();
        if beta_const > 0.0 {
            let integrand = x.powf(alpha) * (1.0 - x).powf(beta) / beta_const;
            integrand.clamp(0.0, 1.0)
        } else {
            // Fallback to normal approximation
            let mean = alpha / (alpha + beta);
            let variance = (alpha * beta) / ((alpha + beta).powi(2) * (alpha + beta + 1.0));
            if variance > 0.0 {
                normal_cdf(x, mean, variance.sqrt())
            } else if x < mean {
                0.0
            } else {
                1.0
            }
        }
    }
}

// Beta distribution PDF
pub(crate) fn beta_pdf(x: f64, alpha: f64, beta: f64) -> f64 {
    if x <= 0.0 || x >= 1.0 || alpha <= 0.0 || beta <= 0.0 {
        return 0.0;
    }
    // Beta PDF: x^(α-1) * (1-x)^(β-1) / B(α,β)
    // where B(α,β) = Γ(α) * Γ(β) / Γ(α+β)
    // Use log form to avoid overflow: ln(PDF) = (α-1)*ln(x) + (β-1)*ln(1-x) - ln(B(α,β))
    let ln_x = x.ln();
    let ln_one_minus_x = (1.0 - x).ln();
    let ln_beta = ln_gamma_approx(alpha) + ln_gamma_approx(beta) - ln_gamma_approx(alpha + beta);
    let ln_pdf = (alpha - 1.0) * ln_x + (beta - 1.0) * ln_one_minus_x - ln_beta;
    // Clamp to avoid overflow/underflow
    if ln_pdf < -700.0 {
        return 0.0;
    }
    if ln_pdf > 700.0 {
        return f64::INFINITY;
    }
    ln_pdf.exp()
}

// Gamma distribution CDF (requires incomplete gamma function approximation)
pub(crate) fn gamma_cdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    // Gamma CDF uses incomplete gamma function
    // For large shape, use normal approximation
    if shape > 30.0 {
        let mean = shape * scale;
        let variance = shape * scale * scale;
        if variance > 0.0 {
            normal_cdf(x, mean, variance.sqrt())
        } else if x < mean {
            0.0
        } else {
            1.0
        }
    } else {
        // Series approximation for incomplete gamma: P(x, k) = gamma(k, x) / Gamma(k)
        // Simplified approximation for small shape
        let z = x / scale;
        let sum: f64 = (0..(shape as usize * 10).min(100))
            .map(|n| {
                if (n as f64) < shape {
                    (-z).exp() * z.powi(n as i32) / (1..=n).map(|i| i as f64).product::<f64>()
                } else {
                    0.0
                }
            })
            .sum();
        (1.0 - sum).clamp(0.0, 1.0)
    }
}

// Gamma distribution PDF
pub(crate) fn gamma_pdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 || shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    // Gamma PDF: (x^(shape-1) * exp(-x/scale)) / (scale^shape * Gamma(shape))
    // Use log form to avoid overflow: ln(PDF) = (shape-1)*ln(x) - x/scale - shape*ln(scale) - ln(Gamma(shape))
    let ln_pdf = (shape - 1.0) * x.ln() - x / scale - shape * scale.ln() - ln_gamma_approx(shape);
    // Clamp to reasonable range to avoid overflow/underflow
    if ln_pdf < -700.0 {
        return 0.0;
    }
    if ln_pdf > 700.0 {
        return f64::INFINITY;
    }
    ln_pdf.exp()
}

// Gamma distribution quantile (inverse CDF) using binary search
pub(crate) fn gamma_quantile(p: f64, shape: f64, scale: f64) -> f64 {
    let p = p.clamp(0.0, 1.0);
    if p <= 0.0 {
        return 0.0;
    }
    if p >= 1.0 {
        // For p=1, return a large value (mean + 5*std as approximation)
        let mean = shape * scale;
        let std = (shape * scale * scale).sqrt();
        return mean + 5.0 * std;
    }
    if shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }

    let mean = shape * scale;
    let std = (shape * scale * scale).sqrt();

    // For large shape, use normal approximation
    if shape > 30.0 {
        let z = normal_quantile(p);
        return (mean + z * std).max(0.0);
    }

    // For very small shape (< 0.1), the series approximation in gamma_cdf may be unreliable
    // Use normal approximation or exponential approximation (shape=1) as fallback
    if shape < 0.1 {
        // For very small shape, approximate as exponential (shape=1) with adjusted scale
        // This avoids numerical issues with the series approximation
        let exp_scale = mean; // For exponential, mean = scale
        if exp_scale > 0.0 {
            // Exponential quantile: -scale * ln(1-p)
            return -exp_scale * (1.0 - p).ln();
        } else {
            // Fallback to normal approximation
            let z = normal_quantile(p);
            return (mean + z * std.max(mean * 0.1)).max(0.0);
        }
    }

    // Binary search for quantile
    // Initial bounds: [0, mean + 5*std]
    let mut low = 0.0;
    let mut high = mean + 5.0 * std;

    // Ensure high is large enough
    while gamma_cdf(high, shape, scale) < p {
        high *= 2.0;
        if high > 1e10 {
            // Fallback: use normal approximation if search fails
            let z = normal_quantile(p);
            return (mean + z * std).max(0.0);
        }
    }

    // Binary search with tolerance
    let tolerance = 1e-6;
    let mut mid = (low + high) / 2.0;
    for _ in 0..100 {
        let cdf_val = gamma_cdf(mid, shape, scale);
        if (cdf_val - p).abs() < tolerance {
            return mid;
        }
        if cdf_val < p {
            low = mid;
        } else {
            high = mid;
        }
        mid = (low + high) / 2.0;
    }

    mid
}

// Chi-squared distribution CDF (special case of Gamma with shape = df/2, scale = 2)
fn chi_squared_cdf(x: f64, df: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if df <= 0.0 {
        return 0.0;
    }
    gamma_cdf(x, df / 2.0, 2.0)
}

// Chi-squared PDF (special case of Gamma with shape = df/2, scale = 2)
pub(crate) fn chi_squared_pdf(x: f64, df: f64) -> f64 {
    if x <= 0.0 || df <= 0.0 {
        return 0.0;
    }
    gamma_pdf(x, df / 2.0, 2.0)
}

// Student's t distribution CDF (approximation)
fn students_t_cdf(x: f64, df: f64) -> f64 {
    if df <= 0.0 {
        return 0.5; // Invalid, return median
    }
    // For large df, approximate with normal
    if df > 30.0 {
        normal_cdf(x, 0.0, 1.0)
    } else {
        // Approximation using normal with correction
        let z = x * (1.0 - 1.0 / (4.0 * df));
        normal_cdf(z, 0.0, 1.0)
    }
}

// Student's t distribution PDF (approximation)
pub(crate) fn students_t_pdf(x: f64, df: f64) -> f64 {
    if df <= 0.0 {
        return 0.0;
    }
    // Student's t PDF: Γ((df+1)/2) / (sqrt(df*π) * Γ(df/2)) * (1 + x²/df)^(-(df+1)/2)
    // For large df, approximate with normal PDF
    if df > 30.0 {
        // Normal approximation: N(0, 1)
        let z = x;
        let pdf = (1.0 / (2.0 * std::f64::consts::PI).sqrt()) * (-0.5 * z * z).exp();
        return pdf;
    }
    // For small df, use approximation
    // Simplified: PDF ≈ (1 + x²/df)^(-(df+1)/2) * constant
    // Constant normalization factor approximated
    let x_sq_over_df = (x * x) / df;
    let exponent = -(df + 1.0) / 2.0;
    let power_term = (1.0 + x_sq_over_df).powf(exponent);
    // Approximate normalization constant
    let ln_gamma_half_df_plus_one = ln_gamma_approx((df + 1.0) / 2.0);
    let ln_gamma_half_df = ln_gamma_approx(df / 2.0);
    let ln_const =
        ln_gamma_half_df_plus_one - ln_gamma_half_df - 0.5 * (df * std::f64::consts::PI).ln();
    let const_term = ln_const.exp();
    let pdf = const_term * power_term;
    // Clamp to avoid overflow/underflow
    if pdf.is_finite() && pdf > 0.0 {
        pdf
    } else {
        0.0
    }
}

// Poisson CDF (discrete, but return as continuous approximation)
fn poisson_cdf(x: f64, lambda: f64) -> f64 {
    if x < 0.0 {
        return 0.0;
    }
    if lambda <= 0.0 {
        return if x >= 0.0 { 1.0 } else { 0.0 };
    }
    // For large lambda, use normal approximation
    if lambda > 20.0 {
        normal_cdf(x, lambda, lambda.sqrt())
    } else {
        // Sum Poisson PMF from 0 to floor(x)
        let k_max = x.floor() as usize;
        let mut cdf = 0.0;
        let mut factorial = 1.0;
        for k in 0..=k_max.min(100) {
            if k > 0 {
                factorial *= k as f64;
            }
            let ln_pmf = (k as f64) * lambda.ln() - lambda - factorial.ln();
            let pmf = ln_pmf.exp();
            cdf += pmf;
            if cdf > 1.0 {
                break;
            }
        }
        cdf.min(1.0)
    }
}

// Bernoulli CDF (discrete, p = probability of success)
fn bernoulli_cdf(x: f64, p: f64) -> f64 {
    if x < 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    if p < 0.0 {
        return 0.0;
    }
    if p > 1.0 {
        return 1.0;
    }
    1.0 - p // CDF(x) = 0 for x < 0, 1-p for 0 <= x < 1, 1 for x >= 1
}

// Binomial coefficient helper
fn binomial_coeff(n: usize, k: usize) -> f64 {
    if k > n {
        0.0
    } else if k == 0 || k == n {
        1.0
    } else {
        let k = k.min(n - k); // Use symmetry
        (1..=k).map(|i| (n - k + i) as f64 / i as f64).product()
    }
}

// Binomial CDF (discrete)
fn binomial_cdf(x: f64, n: usize, p: f64) -> f64 {
    if x < 0.0 {
        return 0.0;
    }
    if p <= 0.0 {
        return if x >= n as f64 { 1.0 } else { 0.0 };
    }
    if p >= 1.0 {
        return if x >= 0.0 { 1.0 } else { 0.0 };
    }
    // For large n, use normal approximation
    if n > 50 {
        let mean = n as f64 * p;
        let variance = n as f64 * p * (1.0 - p);
        if variance > 0.0 {
            normal_cdf(x + 0.5, mean, variance.sqrt()) // Continuity correction
        } else if x < mean {
            0.0
        } else {
            1.0
        }
    } else {
        // Sum binomial PMF
        let k_max = x.floor() as usize;
        let mut cdf = 0.0;
        for k in 0..=k_max.min(n) {
            let coeff = binomial_coeff(n, k);
            let pmf = coeff * p.powi(k as i32) * (1.0 - p).powi((n - k) as i32);
            cdf += pmf;
        }
        cdf.min(1.0)
    }
}

// Geometric CDF (discrete, number of failures before first success)
fn geometric_cdf(x: f64, p: f64) -> f64 {
    if x < 0.0 {
        return 0.0;
    }
    if p <= 0.0 || p >= 1.0 {
        return if x >= 0.0 && p >= 1.0 { 1.0 } else { 0.0 };
    }

    // Geometric CDF: 1 - (1-p)^(k+1) for k failures
    // Use log-space to avoid numerical underflow: (1-p)^(k+1) = exp((k+1) * ln(1-p))
    // But cap k aggressively: beyond k=50, CDF is essentially 1.0 for most p values
    let k = x.floor().min(50.0); // Aggressive cap at 50 (was 1000)

    // For very small (1-p)^(k+1), we can approximate as 0
    let log_one_minus_p = (1.0 - p).ln();
    if log_one_minus_p.is_nan() || log_one_minus_p.is_infinite() {
        return if x >= 0.0 { 1.0 } else { 0.0 };
    }

    // Calculate (k+1) * ln(1-p)
    let exponent = (k + 1.0) * log_one_minus_p;

    // If exponent is very negative, (1-p)^(k+1) is essentially 0, so CDF ≈ 1.0
    if exponent < -50.0 {
        return 1.0;
    }

    // Otherwise calculate normally using exp
    let one_minus_p_power = exponent.exp();
    let result = 1.0 - one_minus_p_power;
    result.clamp(0.0, 1.0)
}

// Geometric PMF (probability mass function) - for continuous approximation in histograms
// Geometric PMF: P(X=k) = p * (1-p)^k for k = 0, 1, 2, ...
// For continuous approximation, we use the PMF at the floor of x
pub(crate) fn geometric_pmf(x: f64, p: f64) -> f64 {
    if x < 0.0 || p <= 0.0 || p >= 1.0 {
        return 0.0;
    }
    // For continuous approximation, use floor(x) as the discrete value
    let k = x.floor().min(100.0); // Cap at reasonable value
                                  // PMF: p * (1-p)^k
                                  // Use log form: ln(PMF) = ln(p) + k * ln(1-p)
    let log_p = p.ln();
    let log_one_minus_p = (1.0 - p).ln();
    if log_p.is_nan()
        || log_p.is_infinite()
        || log_one_minus_p.is_nan()
        || log_one_minus_p.is_infinite()
    {
        return 0.0;
    }
    let ln_pmf = log_p + k * log_one_minus_p;
    // Clamp to avoid overflow/underflow
    if ln_pmf < -700.0 {
        return 0.0;
    }
    if ln_pmf > 700.0 {
        return f64::INFINITY;
    }
    ln_pmf.exp()
}

// Geometric quantile (inverse CDF) using binary search for better accuracy
pub(crate) fn geometric_quantile(p: f64, p_param: f64) -> f64 {
    let p = p.clamp(0.0, 1.0);
    if p <= 0.0 {
        return 0.0;
    }
    if p >= 1.0 {
        // For p=1, return a large value (cap at 100 for Geometric)
        return 100.0;
    }
    if p_param <= 0.0 || p_param >= 1.0 {
        return 0.0;
    }

    // Direct formula: q(p) = floor(ln(1-p) / ln(1-p_param))
    // But handle edge cases better
    let log_one_minus_p = (1.0 - p).ln();
    let log_one_minus_p_param = (1.0 - p_param).ln();

    // Check for numerical issues
    if log_one_minus_p.is_nan()
        || log_one_minus_p.is_infinite()
        || log_one_minus_p_param.is_nan()
        || log_one_minus_p_param.is_infinite()
        || log_one_minus_p_param.abs() < 1e-10
    {
        // Fallback: use binary search
        return geometric_quantile_binary_search(p, p_param);
    }

    let quantile = log_one_minus_p / log_one_minus_p_param;
    quantile.clamp(0.0, 100.0) // Cap at reasonable value
}

// Binary search fallback for Geometric quantile
fn geometric_quantile_binary_search(p: f64, p_param: f64) -> f64 {
    // Binary search for quantile
    let mut low = 0.0;
    let mut high = 100.0;

    // Ensure high is large enough
    while geometric_cdf(high, p_param) < p {
        high *= 2.0;
        if high > 1000.0 {
            return 100.0; // Cap at 100
        }
    }

    // Binary search with tolerance
    let tolerance = 1e-6;
    let mut mid = (low + high) / 2.0;
    for _ in 0..100 {
        let cdf_val = geometric_cdf(mid, p_param);
        if (cdf_val - p).abs() < tolerance {
            return mid;
        }
        if cdf_val < p {
            low = mid;
        } else {
            high = mid;
        }
        mid = (low + high) / 2.0;
    }

    mid
}

// Weibull distribution CDF
fn weibull_cdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    // Weibull CDF: 1 - exp(-(x/scale)^shape)
    1.0 - (-(x / scale).powf(shape)).exp()
}

// Weibull distribution PDF
pub(crate) fn weibull_pdf(x: f64, shape: f64, scale: f64) -> f64 {
    if x <= 0.0 || shape <= 0.0 || scale <= 0.0 {
        return 0.0;
    }
    // Weibull PDF: (k/λ) * (x/λ)^(k-1) * exp(-(x/λ)^k)
    // where k = shape, λ = scale
    let ratio = x / scale;
    let power = ratio.powf(shape);
    let pdf = (shape / scale) * ratio.powf(shape - 1.0) * (-power).exp();
    // Clamp to avoid overflow/underflow
    if pdf.is_finite() {
        pdf
    } else {
        0.0
    }
}

// Calculate theoretical probability in an interval [lower, upper] for a distribution
// Helper function for dense sampling of theoretical distribution
/// Calculates the probability that a value falls in [lower, upper] for the given distribution.
///
/// Uses the distribution's CDF to compute P(lower ≤ X < upper).
pub fn calculate_theoretical_probability_in_interval(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    lower: f64,
    upper: f64,
) -> f64 {
    let mean = dist.characteristics.mean;
    let std = dist.characteristics.std_dev;
    let sorted_data = &dist.sorted_sample_values;

    match dist_type {
        DistributionType::Normal => {
            let cdf_upper = normal_cdf(upper, mean, std);
            let cdf_lower = normal_cdf(lower, mean, std);
            cdf_upper - cdf_lower
        }
        DistributionType::LogNormal => {
            if sorted_data.is_empty() || !sorted_data.iter().all(|&v| v > 0.0) {
                0.0
            } else {
                let e_x = mean;
                let var_x = std * std;
                let sigma_sq = (1.0 + var_x / (e_x * e_x)).ln();
                let mu = e_x.ln() - sigma_sq / 2.0;
                let sigma = sigma_sq.sqrt();

                if lower > 0.0 && upper > 0.0 {
                    let cdf_upper = lognormal_cdf(upper, mu, sigma);
                    let cdf_lower = lognormal_cdf(lower, mu, sigma);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            }
        }
        DistributionType::Uniform => {
            if sorted_data.is_empty() {
                0.0
            } else {
                let data_min = sorted_data[0];
                let data_max = sorted_data[sorted_data.len() - 1];
                let data_range = data_max - data_min;
                if data_range > 0.0 {
                    (upper - lower) / data_range
                } else {
                    0.0
                }
            }
        }
        DistributionType::Exponential => {
            if mean > 0.0 {
                let lambda = 1.0 / mean;
                let cdf_upper = exponential_cdf(upper, lambda);
                let cdf_lower = exponential_cdf(lower, lambda);
                cdf_upper - cdf_lower
            } else {
                0.0
            }
        }
        DistributionType::PowerLaw => {
            if sorted_data.is_empty() || !sorted_data.iter().any(|&v| v > 0.0) {
                0.0
            } else {
                let positive_values: Vec<f64> =
                    sorted_data.iter().filter(|&&v| v > 0.0).copied().collect();
                if positive_values.is_empty() {
                    0.0
                } else {
                    let xmin = positive_values[0];
                    let n_pos = positive_values.len();
                    if n_pos < 2 || xmin <= 0.0 {
                        0.0
                    } else {
                        let sum_log = positive_values
                            .iter()
                            .map(|&x| (x / xmin).ln())
                            .sum::<f64>();
                        if sum_log > 0.0 {
                            let alpha = 1.0 + (n_pos as f64) / sum_log;
                            let cdf_upper = powerlaw_cdf(upper, xmin, alpha);
                            let cdf_lower = powerlaw_cdf(lower, xmin, alpha);
                            cdf_upper - cdf_lower
                        } else {
                            0.0
                        }
                    }
                }
            }
        }
        DistributionType::Beta => {
            // Estimate parameters from mean and variance
            let mean_val = mean;
            let variance = std * std;
            if mean_val > 0.0 && mean_val < 1.0 && variance > 0.0 {
                let max_var = mean_val * (1.0 - mean_val);
                if variance < max_var {
                    let sum = mean_val * (1.0 - mean_val) / variance - 1.0;
                    let alpha = mean_val * sum;
                    let beta = (1.0 - mean_val) * sum;
                    if alpha > 0.0 && beta > 0.0 {
                        let cdf_upper = beta_cdf(upper, alpha, beta);
                        let cdf_lower = beta_cdf(lower, alpha, beta);
                        cdf_upper - cdf_lower
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            } else {
                0.0
            }
        }
        DistributionType::Gamma => {
            if mean > 0.0 && std > 0.0 {
                let variance = std * std;
                let shape = (mean * mean) / variance;
                let scale = variance / mean;
                if shape > 0.0 && scale > 0.0 {
                    let cdf_upper = gamma_cdf(upper, shape, scale);
                    let cdf_lower = gamma_cdf(lower, shape, scale);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            } else {
                0.0
            }
        }
        DistributionType::ChiSquared => {
            // Chi-squared is gamma(df/2, 2)
            let df = mean; // For chi-squared, mean = df
            if df > 0.0 {
                let cdf_upper = chi_squared_cdf(upper, df);
                let cdf_lower = chi_squared_cdf(lower, df);
                cdf_upper - cdf_lower
            } else {
                0.0
            }
        }
        DistributionType::StudentsT => {
            // Estimate df from variance
            let variance = std * std;
            let df = if variance > 1.0 {
                2.0 * variance / (variance - 1.0)
            } else {
                30.0
            };
            let cdf_upper = students_t_cdf(upper, df);
            let cdf_lower = students_t_cdf(lower, df);
            cdf_upper - cdf_lower
        }
        DistributionType::Poisson => {
            let lambda = mean;
            if lambda > 0.0 {
                let cdf_upper = poisson_cdf(upper, lambda);
                let cdf_lower = poisson_cdf(lower, lambda);
                cdf_upper - cdf_lower
            } else {
                0.0
            }
        }
        DistributionType::Bernoulli => {
            let p = mean; // For Bernoulli, mean = p
            let cdf_upper = bernoulli_cdf(upper, p);
            let cdf_lower = bernoulli_cdf(lower, p);
            cdf_upper - cdf_lower
        }
        DistributionType::Binomial => {
            // Estimate n from data range
            let sorted_data = &dist.sorted_sample_values;
            if !sorted_data.is_empty() {
                let max_val = sorted_data[sorted_data.len() - 1];
                let n = max_val.floor() as usize;
                let p = if n > 0 { mean / n as f64 } else { 0.5 };
                if n > 0 && p > 0.0 && p < 1.0 {
                    let cdf_upper = binomial_cdf(upper, n, p);
                    let cdf_lower = binomial_cdf(lower, n, p);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            } else {
                0.0
            }
        }
        DistributionType::Geometric => {
            let mean_val = mean; // mean = (1-p)/p for geometric
            if mean_val > 0.0 {
                let p = 1.0 / (mean_val + 1.0);
                let cdf_upper = geometric_cdf(upper, p);
                let cdf_lower = geometric_cdf(lower, p);
                cdf_upper - cdf_lower
            } else {
                0.0
            }
        }
        DistributionType::Weibull => {
            if mean > 0.0 && std > 0.0 {
                // Approximate shape from CV
                let cv = std / mean;
                let shape = if cv < 1.0 { 1.0 / cv } else { 1.0 };
                // Scale from mean
                let gamma_1_over_shape = 1.0 + 1.0 / shape; // Approximation
                let scale = mean / gamma_1_over_shape;
                if shape > 0.0 && scale > 0.0 {
                    let cdf_upper = weibull_cdf(upper, shape, scale);
                    let cdf_lower = weibull_cdf(lower, shape, scale);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

// Calculate theoretical bin probabilities using CDF for histogram
/// Calculates probabilities for each bin defined by bin_boundaries.
///
/// Returns a vector where each element is P(lower ≤ X < upper) for the corresponding bin.
pub fn calculate_theoretical_bin_probabilities(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    bin_boundaries: &[f64],
) -> Vec<f64> {
    if bin_boundaries.len() < 2 {
        return vec![];
    }

    let mean = dist.characteristics.mean;
    let std = dist.characteristics.std_dev;
    let sorted_data = &dist.sorted_sample_values;

    // Calculate probability for each bin: P(lower <= X < upper) = CDF(upper) - CDF(lower)
    let mut probabilities = Vec::new();

    for i in 0..(bin_boundaries.len() - 1) {
        let lower = bin_boundaries[i];
        let upper = bin_boundaries[i + 1];

        let prob = match dist_type {
            DistributionType::Normal => {
                let cdf_upper = normal_cdf(upper, mean, std);
                let cdf_lower = normal_cdf(lower, mean, std);
                cdf_upper - cdf_lower
            }
            DistributionType::LogNormal => {
                // Estimate lognormal parameters from data characteristics
                // For lognormal: if X ~ LN(mu, sigma), then E[X] = exp(mu + sigma^2/2), Var[X] = (exp(sigma^2) - 1) * exp(2*mu + sigma^2)
                // Solving: sigma^2 = ln(1 + Var/E^2), mu = ln(E) - sigma^2/2
                if sorted_data.is_empty() || !sorted_data.iter().all(|&v| v > 0.0) {
                    0.0
                } else {
                    let e_x = mean;
                    let var_x = std * std;
                    let sigma_sq = (1.0 + var_x / (e_x * e_x)).ln();
                    let mu = e_x.ln() - sigma_sq / 2.0;
                    let sigma = sigma_sq.sqrt();

                    if lower > 0.0 && upper > 0.0 {
                        let cdf_upper = lognormal_cdf(upper, mu, sigma);
                        let cdf_lower = lognormal_cdf(lower, mu, sigma);
                        cdf_upper - cdf_lower
                    } else {
                        0.0
                    }
                }
            }
            DistributionType::Uniform => {
                // Uniform distribution: equal probability in each bin (if data range matches)
                // For uniform [a, b], probability in [lower, upper] = (upper - lower) / (b - a)
                if sorted_data.is_empty() {
                    0.0
                } else {
                    let data_min = sorted_data[0];
                    let data_max = sorted_data[sorted_data.len() - 1];
                    let data_range = data_max - data_min;
                    if data_range > 0.0 {
                        (upper - lower) / data_range
                    } else if i == 0 {
                        1.0 // All data in first bin if constant
                    } else {
                        0.0
                    }
                }
            }
            DistributionType::Exponential => {
                // Exponential distribution: parameter lambda = 1 / mean (for rate parameterization)
                if mean > 0.0 {
                    let lambda = 1.0 / mean;
                    let cdf_upper = exponential_cdf(upper, lambda);
                    let cdf_lower = exponential_cdf(lower, lambda);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            }
            DistributionType::PowerLaw => {
                // Power law: estimate parameters from data
                // Estimate xmin as minimum positive value, and alpha from data
                if sorted_data.is_empty() || !sorted_data.iter().any(|&v| v > 0.0) {
                    0.0
                } else {
                    let positive_values: Vec<f64> =
                        sorted_data.iter().filter(|&&v| v > 0.0).copied().collect();
                    if positive_values.is_empty() {
                        0.0
                    } else {
                        let xmin = positive_values[0];
                        // Estimate alpha using maximum likelihood or approximation
                        // For power law with continuous values: alpha ≈ 1 + n / sum(ln(x_i / xmin))
                        let n_pos = positive_values.len();
                        if n_pos < 2 || xmin <= 0.0 {
                            0.0
                        } else {
                            let sum_log = positive_values
                                .iter()
                                .map(|&x| (x / xmin).ln())
                                .sum::<f64>();
                            let alpha = if sum_log > 0.0 {
                                1.0 + (n_pos as f64) / sum_log
                            } else {
                                2.5 // Default fallback
                            };

                            if lower >= xmin && alpha > 1.0 {
                                let cdf_upper = powerlaw_cdf(upper, xmin, alpha);
                                let cdf_lower = powerlaw_cdf(lower, xmin, alpha);
                                cdf_upper - cdf_lower
                            } else {
                                0.0
                            }
                        }
                    }
                }
            }
            DistributionType::Beta => {
                // Estimate parameters from mean and variance
                let mean_val = mean;
                let variance = std * std;
                if mean_val > 0.0 && mean_val < 1.0 && variance > 0.0 {
                    let max_var = mean_val * (1.0 - mean_val);
                    if variance < max_var {
                        let sum = mean_val * (1.0 - mean_val) / variance - 1.0;
                        let alpha = mean_val * sum;
                        let beta = (1.0 - mean_val) * sum;
                        if alpha > 0.0 && beta > 0.0 {
                            let cdf_upper = beta_cdf(upper, alpha, beta);
                            let cdf_lower = beta_cdf(lower, alpha, beta);
                            cdf_upper - cdf_lower
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            DistributionType::Gamma => {
                if mean > 0.0 && std > 0.0 {
                    let variance = std * std;
                    let shape = (mean * mean) / variance;
                    let scale = variance / mean;
                    if shape > 0.0 && scale > 0.0 {
                        let cdf_upper = gamma_cdf(upper, shape, scale);
                        let cdf_lower = gamma_cdf(lower, shape, scale);
                        cdf_upper - cdf_lower
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            DistributionType::ChiSquared => {
                // Chi-squared is gamma(df/2, 2)
                let df = mean; // For chi-squared, mean = df
                if df > 0.0 {
                    let cdf_upper = chi_squared_cdf(upper, df);
                    let cdf_lower = chi_squared_cdf(lower, df);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            }
            DistributionType::StudentsT => {
                // Estimate df from variance
                let variance = std * std;
                let df = if variance > 1.0 {
                    2.0 * variance / (variance - 1.0)
                } else {
                    30.0
                };
                let cdf_upper = students_t_cdf(upper, df);
                let cdf_lower = students_t_cdf(lower, df);
                cdf_upper - cdf_lower
            }
            DistributionType::Poisson => {
                let lambda = mean;
                if lambda > 0.0 {
                    let cdf_upper = poisson_cdf(upper, lambda);
                    let cdf_lower = poisson_cdf(lower, lambda);
                    cdf_upper - cdf_lower
                } else {
                    0.0
                }
            }
            DistributionType::Bernoulli => {
                let p = mean; // For Bernoulli, mean = p
                let cdf_upper = bernoulli_cdf(upper, p);
                let cdf_lower = bernoulli_cdf(lower, p);
                cdf_upper - cdf_lower
            }
            DistributionType::Binomial => {
                // Estimate n from data range
                if !sorted_data.is_empty() {
                    let max_val = sorted_data[sorted_data.len() - 1];
                    let n = max_val.floor() as usize;
                    let p = if n > 0 { mean / n as f64 } else { 0.5 };
                    if n > 0 && p > 0.0 && p < 1.0 {
                        let cdf_upper = binomial_cdf(upper, n, p);
                        let cdf_lower = binomial_cdf(lower, n, p);
                        cdf_upper - cdf_lower
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            DistributionType::Geometric => {
                // Safe calculation with limits
                let mean_val = mean; // mean = (1-p)/p for geometric
                if mean_val > 0.0 && mean_val <= 20.0 {
                    let p = 1.0 / (mean_val + 1.0);
                    // Cap upper/lower to prevent issues with large values
                    let upper_capped = upper.min(50.0);
                    let lower_capped = lower.clamp(0.0, 50.0);
                    let cdf_upper = geometric_cdf(upper_capped, p);
                    let cdf_lower = geometric_cdf(lower_capped, p);
                    (cdf_upper - cdf_lower).max(0.0)
                } else {
                    0.0
                }
            }
            DistributionType::Weibull => {
                if mean > 0.0 && std > 0.0 {
                    // Approximate shape from CV
                    let cv = std / mean;
                    let shape = if cv < 1.0 { 1.0 / cv } else { 1.0 };
                    // Scale from mean
                    let gamma_1_over_shape = 1.0 + 1.0 / shape; // Approximation
                    let scale = mean / gamma_1_over_shape;
                    if shape > 0.0 && scale > 0.0 {
                        let cdf_upper = weibull_cdf(upper, shape, scale);
                        let cdf_lower = weibull_cdf(lower, shape, scale);
                        cdf_upper - cdf_lower
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
            DistributionType::Unknown => {
                // Fallback: uniform distribution
                if sorted_data.is_empty() {
                    0.0
                } else {
                    let data_min = sorted_data[0];
                    let data_max = sorted_data[sorted_data.len() - 1];
                    let data_range = data_max - data_min;
                    if data_range > 0.0 {
                        (upper - lower) / data_range
                    } else if i == 0 {
                        1.0
                    } else {
                        0.0
                    }
                }
            }
        };

        probabilities.push(prob.max(0.0)); // Ensure non-negative
    }

    probabilities
}

fn compute_outlier_analysis(series: &Series, numeric_stats: &NumericStatistics) -> OutlierAnalysis {
    let values: Vec<f64> = get_numeric_values_as_f64(series);
    let q25 = numeric_stats.q25;
    let q75 = numeric_stats.q75;
    let mean = numeric_stats.mean;
    let std = numeric_stats.std;

    if q25.is_nan() || q75.is_nan() || std == 0.0 {
        return OutlierAnalysis {
            total_count: 0,
            percentage: 0.0,
            iqr_count: 0,
            zscore_count: 0,
            outlier_rows: Vec::new(),
        };
    }

    let iqr = q75 - q25;
    let lower_fence = q25 - 1.5 * iqr;
    let upper_fence = q75 + 1.5 * iqr;
    let z_threshold = 3.0;

    let mut outlier_rows = Vec::new();

    for (idx, &value) in values.iter().enumerate() {
        let mut methods = Vec::new();
        let mut z_score = None;
        let mut iqr_position = None;

        // Check IQR
        if value < lower_fence {
            methods.push(OutlierMethod::IQR);
            iqr_position = Some(IqrPosition::BelowLowerFence);
        } else if value > upper_fence {
            methods.push(OutlierMethod::IQR);
            iqr_position = Some(IqrPosition::AboveUpperFence);
        }

        // Check Z-Score
        if std > 0.0 {
            let z = (value - mean).abs() / std;
            z_score = Some(z);
            if z > z_threshold {
                methods.push(OutlierMethod::ZScore);
            }
        }

        if !methods.is_empty() {
            outlier_rows.push(OutlierRow {
                row_index: idx,
                column_value: value,
                context_data: HashMap::new(), // Will be populated by caller if needed
                detection_method: if methods.len() == 2 {
                    OutlierMethod::Both
                } else {
                    methods[0].clone()
                },
                z_score,
                iqr_position,
            });
        }
    }

    // Sort by absolute deviation from mean (most extreme first)
    outlier_rows.sort_by(|a, b| {
        let a_dev = (a.column_value - mean).abs();
        let b_dev = (b.column_value - mean).abs();
        b_dev
            .partial_cmp(&a_dev)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Limit to top 100 for performance
    outlier_rows.truncate(100);

    let total_count = outlier_rows.len();
    let percentage = if values.is_empty() {
        0.0
    } else {
        (total_count as f64 / values.len() as f64) * 100.0
    };

    OutlierAnalysis {
        total_count,
        percentage,
        iqr_count: outlier_rows
            .iter()
            .filter(|r| matches!(r.detection_method, OutlierMethod::IQR | OutlierMethod::Both))
            .count(),
        zscore_count: outlier_rows
            .iter()
            .filter(|r| {
                matches!(
                    r.detection_method,
                    OutlierMethod::ZScore | OutlierMethod::Both
                )
            })
            .count(),
        outlier_rows,
    }
}

// Correlation matrix computation
/// Computes pairwise Pearson correlation matrix for all numeric columns.
///
/// Returns correlations, p-values, and sample sizes for each pair.
/// Requires at least 2 numeric columns.
pub fn compute_correlation_matrix(df: &DataFrame) -> Result<CorrelationMatrix> {
    // Get all numeric columns
    let schema = df.schema();
    let numeric_cols: Vec<String> = schema
        .iter()
        .filter(|(_, dtype)| is_numeric_type(dtype))
        .map(|(name, _)| name.to_string())
        .collect();

    if numeric_cols.len() < 2 {
        return Err(color_eyre::eyre::eyre!(
            "Need at least 2 numeric columns for correlation matrix"
        ));
    }

    let n = numeric_cols.len();
    let mut correlations = vec![vec![1.0; n]; n];
    let mut p_values = vec![vec![0.0; n]; n];
    let mut sample_sizes = vec![vec![0; n]; n];

    // Compute pairwise correlations
    for i in 0..n {
        for j in (i + 1)..n {
            let col1 = df.column(&numeric_cols[i])?;
            let col2 = df.column(&numeric_cols[j])?;

            // Remove nulls for this pair
            let mask = col1.is_not_null() & col2.is_not_null();
            let col1_clean = col1.filter(&mask)?;
            let col2_clean = col2.filter(&mask)?;

            let sample_size = col1_clean.len();
            sample_sizes[i][j] = sample_size;
            sample_sizes[j][i] = sample_size;

            if sample_size < 3 {
                // Not enough data for correlation
                correlations[i][j] = f64::NAN;
                correlations[j][i] = f64::NAN;
                continue;
            }

            // Compute Pearson correlation
            let col1_series = col1_clean.as_materialized_series();
            let col2_series = col2_clean.as_materialized_series();
            let correlation = compute_pearson_correlation(col1_series, col2_series)?;
            correlations[i][j] = correlation;
            correlations[j][i] = correlation; // Symmetric

            // Compute p-value (statistical significance)
            if sample_size >= 3 {
                let p_value = compute_correlation_p_value(correlation, sample_size);
                p_values[i][j] = p_value;
                p_values[j][i] = p_value;
            }
        }
    }

    Ok(CorrelationMatrix {
        columns: numeric_cols,
        correlations,
        p_values: Some(p_values),
        sample_sizes,
    })
}

fn compute_pearson_correlation(col1: &Series, col2: &Series) -> Result<f64> {
    // Compute Pearson correlation manually
    let values1: Vec<f64> = get_numeric_values_as_f64(col1);
    let values2: Vec<f64> = get_numeric_values_as_f64(col2);

    if values1.len() != values2.len() || values1.len() < 2 {
        return Err(color_eyre::eyre::eyre!("Invalid data for correlation"));
    }

    let mean1: f64 = values1.iter().sum::<f64>() / values1.len() as f64;
    let mean2: f64 = values2.iter().sum::<f64>() / values2.len() as f64;

    let numerator: f64 = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| (v1 - mean1) * (v2 - mean2))
        .sum();

    let var1: f64 = values1.iter().map(|v| (v - mean1).powi(2)).sum();
    let var2: f64 = values2.iter().map(|v| (v - mean2).powi(2)).sum();

    if var1 == 0.0 || var2 == 0.0 {
        return Ok(0.0);
    }

    let correlation = numerator / (var1.sqrt() * var2.sqrt());
    Ok(correlation)
}

fn compute_correlation_p_value(correlation: f64, n: usize) -> f64 {
    // t-test for correlation coefficient
    // t = r * sqrt((n-2) / (1-r^2))
    // Then use t-distribution to get p-value
    if correlation.abs() >= 1.0 || n < 3 {
        return 1.0;
    }

    let t_statistic = correlation * ((n - 2) as f64 / (1.0 - correlation * correlation)).sqrt();
    let _degrees_of_freedom = (n - 2) as f64;

    // Approximate p-value using t-distribution
    // Simplified approximation: p ≈ 2 * (1 - normal_cdf(|t|))
    let normal_cdf = |x: f64| -> f64 {
        // Approximation of normal CDF
        0.5 * (1.0 + (x / std::f64::consts::SQRT_2).tanh())
    };

    let p_value = 2.0 * (1.0 - normal_cdf(t_statistic.abs()));
    p_value.clamp(0.0, 1.0)
}

/// Computes correlation statistics for a pair of columns.
///
/// Returns Pearson correlation coefficient, p-value, covariance, and sample size.
/// Requires at least 3 non-null pairs of values.
pub fn compute_correlation_pair(
    df: &DataFrame,
    col1_name: &str,
    col2_name: &str,
) -> Result<CorrelationPair> {
    let col1 = df.column(col1_name)?;
    let col2 = df.column(col2_name)?;

    // Remove nulls
    let mask = col1.is_not_null() & col2.is_not_null();
    let col1_clean = col1.filter(&mask)?;
    let col2_clean = col2.filter(&mask)?;

    let sample_size = col1_clean.len();
    if sample_size < 3 {
        return Err(color_eyre::eyre::eyre!("Not enough data for correlation"));
    }

    let col1_series = col1_clean.as_materialized_series();
    let col2_series = col2_clean.as_materialized_series();
    let correlation = compute_pearson_correlation(col1_series, col2_series)?;
    let p_value = Some(compute_correlation_p_value(correlation, sample_size));

    // Compute covariance
    let mean1 = col1_series.mean().unwrap_or(0.0);
    let mean2 = col2_series.mean().unwrap_or(0.0);
    let values1: Vec<f64> = get_numeric_values_as_f64(col1_series);
    let values2: Vec<f64> = get_numeric_values_as_f64(col2_series);

    let covariance = if values1.len() == values2.len() {
        values1
            .iter()
            .zip(values2.iter())
            .map(|(v1, v2)| (v1 - mean1) * (v2 - mean2))
            .sum::<f64>()
            / (values1.len() - 1) as f64
    } else {
        0.0
    };

    let r_squared = correlation * correlation;

    // Compute stats for both columns
    let stats1 = ColumnStats {
        mean: mean1,
        std: col1_series.std(1).unwrap_or(0.0),
        min: col1_series
            .min::<f64>()
            .unwrap_or(Some(f64::NAN))
            .unwrap_or(f64::NAN),
        max: col1_series
            .max::<f64>()
            .unwrap_or(Some(f64::NAN))
            .unwrap_or(f64::NAN),
    };

    let stats2 = ColumnStats {
        mean: mean2,
        std: col2_series.std(1).unwrap_or(0.0),
        min: col2_series
            .min::<f64>()
            .unwrap_or(Some(f64::NAN))
            .unwrap_or(f64::NAN),
        max: col2_series
            .max::<f64>()
            .unwrap_or(Some(f64::NAN))
            .unwrap_or(f64::NAN),
    };

    Ok(CorrelationPair {
        column1: col1_name.to_string(),
        column2: col2_name.to_string(),
        correlation,
        p_value,
        sample_size,
        covariance,
        r_squared,
        stats1,
        stats2,
    })
}

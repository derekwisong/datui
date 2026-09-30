use color_eyre::Result;
use color_eyre::eyre::Report;
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
            // A plain collect is always one frame; `Multiple` only comes from sink_multiple.
            lf.collect_with_engine(Engine::Streaming)
                .map(|result| result.unwrap_single())
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
    pub temporal_stats: Option<TemporalStatistics>,
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

/// Describe for a Date, Datetime, Time or Duration column: each statistic a value of the
/// column's own type, written as the table writes it; `None` for a null.
#[derive(Clone, Default)]
pub struct TemporalStatistics {
    pub mean: Option<String>,
    pub min: Option<String>,
    pub q25: Option<String>,
    pub median: Option<String>,
    pub q75: Option<String>,
    pub max: Option<String>,
}

#[derive(Clone)]
pub struct DistributionInfo {
    pub distribution_type: DistributionType,
    pub confidence: f64,
    pub sample_size: usize,
    pub is_sampled: bool,
    pub fit_quality: Option<f64>, // 0.0-1.0, how well data fits detected type
    /// Every family's fit and test, or why it does not apply.
    pub fits: Vec<(DistributionType, crate::distribution_fit::FitOutcome)>,
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
    /// Every family's fit and test, or why it does not apply.
    pub fits: Vec<(DistributionType, crate::distribution_fit::FitOutcome)>,
    /// Each fitted family's quantiles at the plotting positions of
    /// `sorted_sample_values`, for its Q-Q plot: computed with the fit, not per frame.
    pub qq: Vec<(DistributionType, Vec<f64>)>,
}

impl DistributionAnalysis {
    pub fn fit(&self, family: DistributionType) -> Option<&crate::distribution_fit::FitOutcome> {
        self.fits
            .iter()
            .find(|(fitted, _)| *fitted == family)
            .map(|(_, outcome)| outcome)
    }

    pub fn qq(&self, family: DistributionType) -> Option<&[f64]> {
        self.qq
            .iter()
            .find(|(fitted, _)| *fitted == family)
            .map(|(_, quantiles)| quantiles.as_slice())
    }
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
    /// One value throughout: nothing to fit.
    Constant,
    /// Every candidate was rejected: shown as "No clear fit".
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
            DistributionType::Constant => write!(f, "Constant"),
            DistributionType::Unknown => write!(f, "No clear fit"),
        }
    }
}

#[derive(Clone)]
pub struct AnalysisResults {
    pub column_statistics: Vec<ColumnStatistics>,
    pub total_rows: usize,
    pub sample_size: Option<usize>,
    /// Rows an equal-per-value sample kept of each value. See [`crate::sampling::PerValue`].
    pub per_value: Option<usize>,
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
/// A table with more than `sample_size` rows is analyzed from a sample of that many;
/// see [`analysis_rows`]. `None` reads every row.
pub fn compute_statistics_with_options(
    lf: &LazyFrame,
    sample_size: Option<usize>,
    seed: u64,
    options: ComputeOptions,
) -> Result<AnalysisResults> {
    let sample = crate::sampling::Sample {
        method: if sample_size.is_some() {
            crate::sampling::SampleMethod::Spread
        } else {
            crate::sampling::SampleMethod::EveryRow
        },
        rows: sample_size.unwrap_or(0),
        seed,
        ..crate::sampling::Sample::default()
    };
    compute_statistics_for_sample(lf, &sample, None, options)
}

/// Describe's temporal statistics for one collected column, through the same
/// aggregation the lazy path runs.
fn temporal_stats_of(series: &Series) -> Result<Option<TemporalStatistics>> {
    if !is_temporal_type(series.dtype()) {
        return Ok(None);
    }
    let frame = DataFrame::new_infer_height(vec![series.clone().into()])?;
    let schema = frame.schema().clone();
    let agg_df = frame
        .lazy()
        .select(build_describe_aggregation_exprs(&schema))
        .collect()?;
    Ok(parse_describe_agg_row(&agg_df, &schema)
        .pop()
        .and_then(|stats| stats.temporal_stats))
}

/// A value as the table writes it; `None` for a null.
fn get_value_str(df: &DataFrame, col_name: &str, row: usize) -> Option<String> {
    match df.column(col_name).ok()?.get(row).ok()? {
        AnyValue::Null => None,
        v => Some(v.str_value().to_string()),
    }
}

/// [`compute_statistics_with_options`] over the rows a [`crate::sampling::Sample`]
/// picks from `lf`, which is already cut to the sample's scope.
pub fn compute_statistics_for_sample(
    lf: &LazyFrame,
    sample: &crate::sampling::Sample,
    known_total: Option<usize>,
    options: ComputeOptions,
) -> Result<AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let use_streaming = options.polars_streaming;
    let seed = sample.seed;
    let rows = crate::sampling::read(lf, sample, known_total, use_streaming)?;
    let total_rows = rows.total_rows;
    let actual_sample_size = rows.sample_size;
    let should_sample = actual_sample_size.is_some();
    let per_value = rows.per_value.as_ref().map(|per_value| per_value.kept);
    let df = rows.df;

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
            temporal_stats: temporal_stats_of(series)?,
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
        per_value,
        sample_seed: seed,
        correlation_matrix,
        distribution_analyses,
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
        per_value: None,
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
            // Without NaN, which sorts above every number and would be the upper
            // quantiles of a column holding enough of it.
            let numbers = c.clone().drop_nans();
            exprs.push(
                numbers
                    .clone()
                    .quantile(lit(0.25), QuantileMethod::Nearest)
                    .alias(format!("{}q25", prefix)),
            );
            exprs.push(
                numbers
                    .clone()
                    .quantile(lit(0.5), QuantileMethod::Nearest)
                    .alias(format!("{}median", prefix)),
            );
            exprs.push(
                numbers
                    .clone()
                    .quantile(lit(0.75), QuantileMethod::Nearest)
                    .alias(format!("{}q75", prefix)),
            );
            exprs.push(c.max().alias(format!("{}max", prefix)));
        } else if is_categorical_type(dtype) {
            exprs.push(col(name).min().alias(format!("{}min", prefix)));
            exprs.push(col(name).max().alias(format!("{}max", prefix)));
        } else if is_temporal_type(dtype) {
            // Mean and quantiles run on the physical integers and are cast back, so
            // each is a value of the column's own type, as in Polars' describe.
            let physical = dtype.to_physical();
            let back = |e: Expr| e.cast(physical.clone()).cast(dtype.clone());
            let p = col(name).cast(physical.clone());
            exprs.push(
                back(p.clone().cast(DataType::Float64).mean()).alias(format!("{}mean", prefix)),
            );
            exprs.push(col(name).min().alias(format!("{}min", prefix)));
            for (q, stat) in [(0.25, "q25"), (0.5, "median"), (0.75, "q75")] {
                exprs.push(
                    back(p.clone().quantile(lit(q), QuantileMethod::Nearest))
                        .alias(format!("{}{}", prefix, stat)),
                );
            }
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
        let temporal_stats = is_temporal_type(dtype).then(|| {
            let value = |stat: &str| get_value_str(agg_df, &format!("{}{}", prefix, stat), row);
            TemporalStatistics {
                mean: value("mean"),
                min: value("min"),
                q25: value("q25"),
                median: value("median"),
                q75: value("q75"),
                max: value("max"),
            }
        });
        column_statistics.push(ColumnStatistics {
            name: name_str.to_string(),
            dtype: dtype.clone(),
            count,
            null_count,
            numeric_stats,
            categorical_stats,
            temporal_stats,
            distribution_info: None,
        });
    }
    column_statistics
}

/// Computes describe statistics from a LazyFrame without materializing all rows.
/// When sampling is disabled, runs a single aggregation collect (like Polars describe) for similar performance.
/// When sampling is enabled, samples then runs describe on the sample.
/// Describe statistics for a frame. With `sample_size`, a table with more rows than
/// that is described from a sample (see [`analysis_rows`]); without it, every row is
/// aggregated in one streaming pass, never held. `known_total` saves a count.
pub fn compute_describe_from_lazy(
    lf: &LazyFrame,
    known_total: Option<usize>,
    sample: &crate::sampling::Sample,
    polars_streaming: bool,
) -> Result<AnalysisResults> {
    let schema = lf.clone().collect_schema()?;
    let seed = sample.seed;
    if sample.method != crate::sampling::SampleMethod::EveryRow {
        let rows = crate::sampling::read(lf, sample, known_total, polars_streaming)?;
        let mut results = compute_describe_single_aggregation(
            &rows.df,
            &schema,
            rows.total_rows,
            rows.sample_size,
            seed,
            polars_streaming,
        )?;
        results.per_value = rows.per_value.map(|per_value| per_value.kept);
        return Ok(results);
    }
    let total_rows = match known_total {
        Some(total) => total,
        None => count_rows(lf, polars_streaming)?,
    };
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

/// Uses Polars' definition so Int128, UInt128, Decimal, and future numeric types are included.
fn is_numeric_type(dtype: &DataType) -> bool {
    dtype.is_numeric()
}

fn is_categorical_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(..))
}

fn is_temporal_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Date | DataType::Datetime(..) | DataType::Time | DataType::Duration(_)
    )
}

/// The rows an analysis reads, and how many the table has.
pub struct AnalysisRows {
    pub df: DataFrame,
    pub total_rows: usize,
    /// How many rows were sampled, when the table had more than the analysis reads.
    pub sample_size: Option<usize>,
    /// What an equal-per-value sample kept and counted.
    pub per_value: Option<crate::sampling::PerValue>,
}

/// How many places across the table a block sample reads from. Enough that no one
/// stretch of it decides the answer, few enough that each is a row group or two.
const SAMPLE_BLOCKS: usize = 50;

/// How many runs of a block sample are read at once.
const SAMPLE_READERS: usize = 8;

/// The row index the streaming sampler ranks rows by, dropped before anyone sees it.
const SAMPLE_POSITION: &str = "__datui_sample_position";

/// Count a frame's rows.
pub fn count_rows(lf: &LazyFrame, polars_streaming: bool) -> Result<usize> {
    let count_df = collect_lazy(
        crate::widgets::datatable::row_count_lf(lf),
        polars_streaming,
    )
    .map_err(Report::from)?;
    Ok(match count_df.get(0).and_then(|row| row.first().cloned()) {
        Some(AnyValue::UInt64(n)) => n as usize,
        Some(AnyValue::UInt32(n)) => n as usize,
        _ => 0,
    })
}

/// Read the rows an analysis works on: all of them when the table has no more than
/// `sample_rows` (or `sample_rows` is `None`), and otherwise a seeded sample of that
/// many, spread across the whole table rather than taken from its head.
///
/// Two ways to spread it, chosen by what the plan can do cheaply:
///
/// - A plan whose slices reach into a single Parquet or IPC scan reads
///   [`SAMPLE_BLOCKS`] short runs at seeded places across the table. Each run is a
///   row group or two, so a sample of a 400-million-row hive table reads a few dozen
///   row groups, not the table. `known_total` saves the count; the footers give it
///   cheaply otherwise.
/// - Anything else — a filter, a query, a union of files, a CSV — is read once as a
///   stream, keeping the rows whose seeded rank is lowest. That is a uniform sample
///   in bounded memory, and the same pass counts the rows, so a filtered view is
///   read once rather than counted and then read.
pub fn analysis_rows(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
) -> Result<AnalysisRows> {
    analysis_rows_watched(lf, sample_rows, known_total, seed, polars_streaming, None)
}

/// [`analysis_rows`], stopping when `watch` says to: the streamed pass between
/// batches, the seeded runs between runs. A whole read is one collect, which runs to
/// its end.
pub(crate) fn analysis_rows_watched(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
    watch: Option<&crate::sampling::ReadWatch>,
) -> Result<AnalysisRows> {
    sample_rows_counting(
        lf,
        sample_rows,
        known_total,
        seed,
        polars_streaming,
        watch,
        None,
    )
    .map(|read| read.rows)
}

/// [`analysis_rows_watched`], keeping where each row sat, and counting every row by
/// `count` when the sample is one streamed pass. Seeded runs and a whole read count
/// nothing: the runs do not see every row, and a whole read is not a sample.
pub(crate) fn sample_rows_counting(
    lf: &LazyFrame,
    sample_rows: Option<usize>,
    known_total: Option<usize>,
    seed: u64,
    polars_streaming: bool,
    watch: Option<&crate::sampling::ReadWatch>,
    count: Option<&Expr>,
) -> Result<crate::sampling::SampledRows> {
    let whole = |df: DataFrame, total_rows: usize| crate::sampling::SampledRows {
        positions: (0..df.height() as IdxSize).collect(),
        rows: AnalysisRows {
            df,
            total_rows,
            sample_size: None,
            per_value: None,
        },
        counted: None,
    };
    let Some(n) = sample_rows.filter(|n| *n > 0) else {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        let total_rows = df.height();
        return Ok(whole(df, total_rows));
    };
    if !slices_reach_into_the_scan(lf) {
        let read = stream_sample(lf, n, seed, watch, count)?;
        let sample_size = (read.seen > n).then_some(read.df.height());
        return Ok(crate::sampling::SampledRows {
            rows: AnalysisRows {
                df: read.df,
                total_rows: read.seen,
                sample_size,
                per_value: None,
            },
            positions: read.positions,
            counted: read.counted,
        });
    }
    let total_rows = match known_total {
        Some(total) => total,
        None => count_rows(lf, polars_streaming)?,
    };
    if total_rows <= n {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        return Ok(whole(df, total_rows));
    }
    let (df, positions) = block_sample(lf, total_rows, n, seed, polars_streaming, watch)?;
    Ok(crate::sampling::SampledRows {
        rows: AnalysisRows {
            sample_size: Some(df.height()),
            df,
            total_rows,
            per_value: None,
        },
        positions,
        counted: None,
    })
}

/// Whether a slice of this plan is read by the scan of one file, skipping what comes
/// before it: true of a single Parquet or IPC file, which seeks by row group, with or
/// without columns stubbed above it. Not of a filter or a CSV, whose slice reads
/// everything ahead of it, nor of a scan of many files, where each slice opens the
/// footer of every file before it — measured on 135 files in S3, fifty slices took
/// longer than streaming all 37 million rows once.
///
/// Asked of the optimized plan because that is where the answer is, for every route a
/// frame can have been built by: pushed into the scan, the slice is a property of the
/// `SCAN` (`SLICE: Positive`); left above it, a node of its own (`SLICE[`). Should a
/// Polars upgrade change how the plan is described, this says no and the streaming
/// sampler takes over: slower, never wrong.
pub fn slices_reach_into_the_scan(lf: &LazyFrame) -> bool {
    let Ok(plan) = lf.clone().slice(1, 1).describe_optimized_plan() else {
        return false;
    };
    let scans: Vec<&str> = plan
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("Parquet SCAN") || l.starts_with("IPC SCAN"))
        .collect();
    let [scan] = scans.as_slice() else {
        return false;
    };
    let one_source = !scan.contains("other sources") && !scan.contains(", ");
    let total_scans = plan.matches(" SCAN").count();
    one_source && total_scans == 1 && plan.contains("SLICE: Positive") && !plan.contains("SLICE[")
}

/// `n` rows as [`SAMPLE_BLOCKS`] runs at seeded places across `total_rows`, in table
/// order. Each run is collected on its own: as one union the runs share a subplan, and
/// Polars caches a shared subplan whole. They are collected [`SAMPLE_READERS`] at a
/// time, because on an object store each is a round trip and fifty in a row is the
/// wait this exists to avoid.
fn block_sample(
    lf: &LazyFrame,
    total_rows: usize,
    n: usize,
    seed: u64,
    polars_streaming: bool,
    watch: Option<&crate::sampling::ReadWatch>,
) -> Result<(DataFrame, Vec<IdxSize>)> {
    // Under twice the sample, reading the table is about as cheap as reading runs of
    // it, and runs that must fit side by side would crowd or overlap. Read it and keep
    // a seeded uniform `n` of it instead.
    if total_rows < 2 * n {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        let mut ranked: Vec<(u64, IdxSize)> = (0..df.height())
            .map(|i| (sample_rank(seed, i as u64), i as IdxSize))
            .collect();
        ranked.sort_unstable();
        let mut keep: Vec<IdxSize> = ranked.into_iter().take(n).map(|(_, i)| i).collect();
        keep.sort_unstable();
        let df = df.take(&IdxCa::from_vec("sample".into(), keep.clone()))?;
        return Ok((df, keep));
    }
    let blocks = SAMPLE_BLOCKS.min(n).max(1);
    // Exactly `n` rows between the runs, so none is cut off the end, and each fits in
    // its own stretch of the table: a stretch is at least `2n / blocks` rows long.
    let stride = total_rows / blocks;
    let runs: Vec<(usize, usize)> = (0..blocks)
        .map(|block| {
            let run = (block + 1) * n / blocks - block * n / blocks;
            let room = stride.saturating_sub(run) as u64;
            let offset = block * stride + (sample_rank(seed, block as u64) % (room + 1)) as usize;
            (offset, run)
        })
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let read: Vec<Result<(usize, DataFrame)>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..SAMPLE_READERS.min(blocks))
            .map(|_| {
                scope.spawn(|| {
                    let mut read = Vec::new();
                    loop {
                        if watch.is_some_and(|watch| watch.stopped()) {
                            break;
                        }
                        let block = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let Some((offset, run)) = runs.get(block) else {
                            break;
                        };
                        let rows = collect_lazy(
                            lf.clone().slice(*offset as i64, *run as IdxSize),
                            polars_streaming,
                        )
                        .map(|df| {
                            if let Some(watch) = watch {
                                watch.saw(df.height());
                            }
                            (block, df)
                        })
                        .map_err(Report::from);
                        read.push(rows);
                    }
                    read
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| match worker.join() {
                Ok(read) => read,
                // A reader that died is an error, not a smaller sample.
                Err(_) => vec![Err(Report::msg("a sample reader failed"))],
            })
            .collect()
    });
    if let Some(watch) = watch {
        watch.check()?;
    }
    let mut read = read.into_iter().collect::<Result<Vec<_>>>()?;
    read.sort_by_key(|(block, _)| *block);
    let mut out: Option<DataFrame> = None;
    let mut positions = Vec::with_capacity(n);
    for (block, rows) in read {
        let offset = runs[block].0;
        positions.extend((0..rows.height()).map(|row| (offset + row) as IdxSize));
        out = Some(match out {
            Some(frame) => frame.vstack(&rows)?,
            None => rows,
        });
    }
    Ok((out.unwrap_or_default(), positions))
}

/// What [`stream_sample`] read.
struct StreamRead {
    df: DataFrame,
    /// Rows the pass saw: the scope's size.
    seen: usize,
    positions: Vec<IdxSize>,
    counted: Option<crate::sampling::Counted>,
}

/// A uniform sample of `n` rows from one streamed pass, and how many rows there were,
/// with every row counted by `count` on the way.
fn stream_sample(
    lf: &LazyFrame,
    n: usize,
    seed: u64,
    watch: Option<&crate::sampling::ReadWatch>,
    count: Option<&Expr>,
) -> Result<StreamRead> {
    let state = std::sync::Arc::new(std::sync::Mutex::new(Reservoir::new(n, seed)));
    let callback_state = std::sync::Arc::clone(&state);
    let callback_watch = watch.cloned();
    let sink = crate::sampling::with_count_key(lf.clone(), count)
        .with_row_index(SAMPLE_POSITION, None)
        .sink_batches(
            PlanCallback::new(move |batch: DataFrame| {
                // True stops the sink: a cancel ends the read at the next batch.
                if let Some(watch) = &callback_watch {
                    if watch.stopped() {
                        return Ok(true);
                    }
                    watch.saw(batch.height());
                }
                callback_state
                    .lock()
                    .map_err(|_| PolarsError::ComputeError("sampler lock failed".into()))?
                    .observe(batch)?;
                Ok(false)
            }),
            true,
            None,
        )?;
    // Streaming whatever the setting: holding the table is what this is here to avoid.
    collect_lazy(sink, true).map_err(Report::from)?;
    if let Some(watch) = watch {
        watch.check()?;
    }
    let mut reservoir = std::mem::take(
        &mut *state
            .lock()
            .map_err(|_| Report::msg("sampler lock failed"))?,
    );
    let seen = reservoir.seen;
    let counted = count
        .is_some()
        .then(|| std::mem::take(&mut reservoir.counter).finish());
    let (df, positions) = match reservoir.finish()? {
        Some(kept) => kept,
        // Nothing came through: an empty frame of the right shape.
        None => (
            collect_lazy(lf.clone().limit(0), true).map_err(Report::from)?,
            Vec::new(),
        ),
    };
    Ok(StreamRead {
        df,
        seen,
        positions,
        counted,
    })
}

/// The `n` rows with the lowest seeded rank seen so far. Held to at most twice `n`
/// between prunes, so memory is bounded by the sample and not by the table.
#[derive(Default)]
struct Reservoir {
    n: usize,
    seed: u64,
    seen: usize,
    kept: Option<DataFrame>,
    ranks: Vec<u64>,
    /// Rows ranked at or above this cannot make the sample: `n` lower ones are held.
    bar: u64,
    counter: crate::sampling::KeyCounter,
}

impl Reservoir {
    fn new(n: usize, seed: u64) -> Self {
        Self {
            n,
            seed,
            bar: u64::MAX,
            ..Default::default()
        }
    }

    fn observe(&mut self, mut batch: DataFrame) -> PolarsResult<()> {
        self.counter.observe(&mut batch)?;
        self.seen += batch.height();
        let positions = batch.column(SAMPLE_POSITION)?.idx()?;
        let mut picked = Vec::new();
        let mut ranks = Vec::new();
        for (index, position) in positions.into_no_null_iter().enumerate() {
            let rank = sample_rank(self.seed, position as u64);
            if rank < self.bar {
                picked.push(index as IdxSize);
                ranks.push(rank);
            }
        }
        if picked.is_empty() {
            return Ok(());
        }
        let rows = batch.take(&IdxCa::from_vec("picked".into(), picked))?;
        self.kept = Some(match self.kept.take() {
            Some(kept) => kept.vstack(&rows)?,
            None => rows,
        });
        self.ranks.extend(ranks);
        if self.ranks.len() > 2 * self.n {
            self.prune()?;
        }
        Ok(())
    }

    /// Keep the `n` lowest-ranked rows, and raise the bar to the highest of them.
    fn prune(&mut self) -> PolarsResult<()> {
        let Some(kept) = self.kept.take() else {
            return Ok(());
        };
        let mut order: Vec<usize> = (0..self.ranks.len()).collect();
        order.sort_unstable_by_key(|i| self.ranks[*i]);
        order.truncate(self.n);
        let take: Vec<IdxSize> = order.iter().map(|i| *i as IdxSize).collect();
        self.kept = Some(kept.take(&IdxCa::from_vec("kept".into(), take))?);
        self.ranks = order.iter().map(|i| self.ranks[*i]).collect();
        if self.ranks.len() == self.n {
            self.bar = self.ranks.iter().copied().max().unwrap_or(u64::MAX);
        }
        Ok(())
    }

    /// The sample, back in table order without the position column, and where each
    /// of its rows sat.
    fn finish(mut self) -> PolarsResult<Option<(DataFrame, Vec<IdxSize>)>> {
        self.prune()?;
        let Some(kept) = self.kept else {
            return Ok(None);
        };
        let sorted = kept.sort([SAMPLE_POSITION], SortMultipleOptions::default())?;
        let positions = sorted
            .column(SAMPLE_POSITION)?
            .idx()?
            .into_no_null_iter()
            .collect();
        Ok(Some((sorted.drop(SAMPLE_POSITION)?, positions)))
    }
}

/// A seeded, well-mixed rank for a row position (SplitMix64's finalizer). The same seed
/// and table give the same sample; another seed gives another.
pub(crate) fn sample_rank(seed: u64, position: u64) -> u64 {
    let mut value = seed ^ position.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Up to ten thousand of a column's values, spread across it, as `f64`. NaN and
/// infinities are left out: no distribution has them, and one NaN is enough to leave
/// a sort by `partial_cmp` out of order.
fn get_numeric_values_as_f64(series: &Series) -> Vec<f64> {
    let max_len = 10000;
    // Every k-th value, not the first ten thousand: a sample is spread across the
    // table, and its head is one stretch of it.
    let limited_series = if series.len() > max_len {
        let step = series.len().div_ceil(max_len);
        series
            .gather_every(step, 0)
            .unwrap_or_else(|_| series.slice(0, max_len))
    } else {
        series.clone()
    };

    if let Ok(f64_series) = limited_series.f64() {
        f64_series
            .iter()
            .flatten()
            .filter(|v| v.is_finite())
            .take(max_len)
            .collect()
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
            .flatten()
            .filter(|v| v.is_finite())
            .map(f64::from)
            .take(max_len)
            .collect()
    } else {
        match limited_series.cast(&DataType::Float64) {
            Ok(cast_series) => {
                if let Ok(f64_series) = cast_series.f64() {
                    f64_series
                        .iter()
                        .flatten()
                        .filter(|v| v.is_finite())
                        .take(max_len)
                        .collect()
                } else {
                    Vec::new()
                }
            }
            Err(_) => Vec::new(),
        }
    }
}

/// Every finite value of a numeric column as `f64`: nulls, NaN and infinities left out.
fn finite_values(series: &Series) -> Vec<f64> {
    let Ok(floats) = series.cast(&DataType::Float64) else {
        return Vec::new();
    };
    let Ok(floats) = floats.f64() else {
        return Vec::new();
    };
    floats.iter().flatten().filter(|v| v.is_finite()).collect()
}

fn compute_numeric_stats(series: &Series, include_advanced: bool) -> Result<NumericStatistics> {
    // Cast and aggregate as Describe does (`build_describe_aggregation_exprs`), so a
    // sample's median is one number wherever it is shown.
    let floats = series.cast(&DataType::Float64)?;
    let mean = floats.mean().unwrap_or(f64::NAN);
    let std = floats.std(1).unwrap_or(f64::NAN);
    let min = floats.min::<f64>()?.unwrap_or(f64::NAN);
    let max = floats.max::<f64>()?.unwrap_or(f64::NAN);

    // NaN sorts above every number, so a column with some would have them as its
    // upper percentiles and fences; Describe leaves them out too. One sort for all.
    let floats = floats.f64()?;
    let numbers = floats.filter(&floats.is_not_nan())?;
    const PERCENTILES: [u8; 7] = [1, 5, 25, 50, 75, 95, 99];
    let quantiles = PERCENTILES.map(|p| f64::from(p) / 100.0);
    let values = numbers.quantiles(&quantiles, QuantileMethod::Nearest)?;
    let percentiles: HashMap<u8, f64> = PERCENTILES
        .into_iter()
        .zip(values)
        .map(|(p, value)| (p, value.unwrap_or(f64::NAN)))
        .collect();

    let median = percentiles[&50];
    let q25 = percentiles[&25];
    let q75 = percentiles[&75];

    let (skewness, kurtosis, outliers_iqr, outliers_zscore) = if include_advanced {
        let values = finite_values(series);
        let (skewness, kurtosis) = skewness_and_kurtosis(&values);
        let (out_iqr, out_zscore) = detect_outliers(&values, q25, q75);
        (skewness, kurtosis, out_iqr, out_zscore)
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

/// Mean and sample standard deviation (ddof 1) of a set of values.
fn mean_and_std(values: &[f64]) -> (f64, f64) {
    let n = values.len() as f64;
    if values.len() < 2 {
        return (values.first().copied().unwrap_or(f64::NAN), f64::NAN);
    }
    let mean = values.iter().sum::<f64>() / n;
    let sum_squares: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
    (mean, (sum_squares / (n - 1.0)).sqrt())
}

/// Skewness and kurtosis of one set of values, `n` being their count: the
/// bias-corrected forms of Polars' `skew(bias=False)` and `kurtosis(bias=False)`,
/// kurtosis on the scale where a normal is 3. One value throughout, or too few to
/// say, is 0 and 3.
fn skewness_and_kurtosis(values: &[f64]) -> (f64, f64) {
    let count = values.len();
    if count < 3 || values.iter().all(|v| *v == values[0]) {
        return (0.0, 3.0);
    }
    let n = count as f64;
    let (mean, std) = mean_and_std(values);
    let (mut cubes, mut fourths) = (0.0, 0.0);
    for v in values {
        let z = (v - mean) / std;
        let z2 = z * z;
        cubes += z2 * z;
        fourths += z2 * z2;
    }
    let skewness = n / ((n - 1.0) * (n - 2.0)) * cubes;
    if count < 4 {
        return (skewness, 3.0);
    }
    let excess = n * (n + 1.0) / ((n - 1.0) * (n - 2.0) * (n - 3.0)) * fourths
        - 3.0 * (n - 1.0) * (n - 1.0) / ((n - 2.0) * (n - 3.0));
    (skewness, excess + 3.0)
}

/// Where a value falls against the IQR fences and three standard deviations.
struct OutlierTest {
    lower_fence: f64,
    upper_fence: f64,
    mean: f64,
    std: f64,
}

impl OutlierTest {
    /// Fences from the quartiles; the z-score from the values' own mean and std, so
    /// a NaN elsewhere in the column cannot void it.
    fn new(values: &[f64], q25: f64, q75: f64) -> Option<Self> {
        let (mean, std) = mean_and_std(values);
        if q25.is_nan() || q75.is_nan() || std.is_nan() || std == 0.0 {
            return None;
        }
        let iqr = q75 - q25;
        Some(Self {
            lower_fence: q25 - 1.5 * iqr,
            upper_fence: q75 + 1.5 * iqr,
            mean,
            std,
        })
    }

    fn iqr_position(&self, value: f64) -> Option<IqrPosition> {
        if value < self.lower_fence {
            Some(IqrPosition::BelowLowerFence)
        } else if value > self.upper_fence {
            Some(IqrPosition::AboveUpperFence)
        } else {
            None
        }
    }

    fn z_score(&self, value: f64) -> f64 {
        (value - self.mean).abs() / self.std
    }
}

const Z_THRESHOLD: f64 = 3.0;

/// IQR and z-score outliers among every value given.
fn detect_outliers(values: &[f64], q25: f64, q75: f64) -> (usize, usize) {
    let Some(test) = OutlierTest::new(values, q25, q75) else {
        return (0, 0);
    };
    values.iter().fold((0, 0), |(iqr, zscore), &v| {
        (
            iqr + usize::from(test.iqr_position(v).is_some()),
            zscore + usize::from(test.z_score(v) > Z_THRESHOLD),
        )
    })
}

fn compute_categorical_stats(series: &Series) -> Result<CategoricalStatistics> {
    let value_counts = series.value_counts(false, false, "counts".into(), false)?;
    let unique_count = value_counts.height();

    let mode = if unique_count > 0 {
        match value_counts.get(0) {
            Some(col) => col.first().map(|v| v.str_value().to_string()),
            _ => None,
        }
    } else {
        None
    };

    let mut top_values = Vec::new();
    for i in 0..unique_count.min(10) {
        if let (Some(value_col), Some(count_col)) = (value_counts.get(0), value_counts.get(1))
            && let (Some(value), Some(count)) = (value_col.get(i), count_col.get(i))
        {
            let value_str = value.str_value();
            if let Ok(count_u32) = count.try_extract::<u32>() {
                top_values.push((value_str.to_string(), count_u32 as usize));
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

/// Seeds the fit tests' simulations, so the same values get the same p-values.
const FIT_SEED: u64 = 0x5eed_d157;

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
            fits: Vec::new(),
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
            fits: Vec::new(),
        };
    }

    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    let std = variance.sqrt();

    // One value throughout fits every distribution's degenerate case and none of
    // them usefully; a year column in a partitioned table is the usual one.
    if std == 0.0 {
        return DistributionInfo {
            distribution_type: DistributionType::Constant,
            confidence: 1.0,
            sample_size,
            is_sampled,
            fit_quality: None,
            fits: Vec::new(),
        };
    }

    // Counts are described by a count distribution when one holds.
    let counts = values
        .iter()
        .all(|v| *v >= 0.0 && *v == v.floor() && v.is_finite());
    let fits = crate::distribution_fit::test_all(&values, FIT_SEED);
    let distribution_type = crate::distribution_fit::select(&fits, counts);
    // The figure beside the name is that family's p-value; with no clear fit, the best
    // any family managed, so the table can say how far from fitting it was.
    let confidence = fits
        .iter()
        .find(|(family, _)| *family == distribution_type)
        .and_then(|(_, outcome)| outcome.p_value())
        .or_else(|| {
            fits.iter()
                .filter_map(|(_, outcome)| outcome.p_value())
                .max_by(f64::total_cmp)
        })
        .unwrap_or(0.0);
    DistributionInfo {
        distribution_type,
        confidence,
        sample_size,
        is_sampled,
        fit_quality: Some(confidence),
        fits,
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
    (Some(sw_stat), shapiro_francia_pvalue(sw_stat, n))
}

/// The p-value of a normality statistic computed as above: the squared correlation of
/// the sorted values with normal scores, which is the Shapiro-Francia form of the
/// Shapiro-Wilk test. Royston's (1993) approximation, `ln(1 - W')` being close to
/// normal, for 5 to 5,000 values; `None` outside them.
///
/// It replaces a blend of W with skew and kurtosis penalties that was not a p-value:
/// 2,590 prices with W' = 0.929 read p = 0.855, "normal", where the test says p < 1e-20.
fn shapiro_francia_pvalue(w: f64, n: usize) -> Option<f64> {
    if !(5..=5_000).contains(&n) {
        return None;
    }
    if w >= 1.0 {
        return Some(1.0);
    }
    let u = (n as f64).ln();
    let v = u.ln();
    let mu = -1.2725 + 1.0521 * (v - u);
    let sigma = 1.0308 - 0.26758 * (v + 2.0 / u);
    let z = ((1.0 - w).ln() - mu) / sigma;
    Some((1.0 - normal_cdf(z, 0.0, 1.0)).clamp(0.0, 1.0))
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
    // At most five thousand, spread across the rows: the head of a table sorted by
    // date is its first few years.
    const MAX_VALUES: usize = 5_000;
    let mut values = get_numeric_values_as_f64(series);
    if values.len() > MAX_VALUES {
        let step = values.len().div_ceil(MAX_VALUES);
        values = values.into_iter().step_by(step).collect();
    }

    // Sort values for Q-Q plot (all data if not sampled, or sampled data if >= threshold)
    values.sort_by(f64::total_cmp);
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

    let fit_quality = dist_info.fit_quality.unwrap_or(dist_info.confidence);
    let qq = dist_info
        .fits
        .iter()
        .filter_map(|(family, outcome)| {
            let test = outcome.test()?;
            Some((
                *family,
                crate::distribution_fit::qq_quantiles(&test.fitted, sorted_sample_values.len()),
            ))
        })
        .collect();

    let outliers = compute_outlier_analysis(&finite_values(series), numeric_stats);

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
        fits: dist_info.fits.clone(),
        qq,
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
        .max_by_key(|&(_, &count)| count)
        .map(|(idx, _)| idx);

    max_bin.map(|idx| bin_sums[idx] / bin_counts[idx] as f64)
}

/// The standard normal quantile. See [`crate::distribution_fit::normal_quantile`].
pub(crate) fn normal_quantile(p: f64) -> f64 {
    crate::distribution_fit::normal_quantile(p)
}

// CDF (Cumulative Distribution Function) implementations for histogram theoretical probabilities
fn normal_cdf(x: f64, mean: f64, std: f64) -> f64 {
    if std <= 0.0 {
        return if x < mean { 0.0 } else { 1.0 };
    }
    crate::distribution_fit::normal_cdf((x - mean) / std)
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

/// The regularized incomplete beta function `I_x(a, b)`, by its continued fraction
/// (Lentz's method), flipped to the side where the fraction converges fast.
fn regularized_incomplete_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let ln_gamma = crate::distribution_fit::ln_gamma;
    let ln_front = ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln();
    let front = ln_front.exp();
    if x < (a + 1.0) / (a + b + 2.0) {
        front * beta_continued_fraction(x, a, b) / a
    } else {
        1.0 - front * beta_continued_fraction(1.0 - x, b, a) / b
    }
}

fn beta_continued_fraction(x: f64, a: f64, b: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let mut c = 1.0;
    let mut d = 1.0 - (a + b) * x / (a + 1.0);
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..=300 {
        let m = m as f64;
        let m2 = 2.0 * m;
        let even = m * (b - m) * x / ((a + m2 - 1.0) * (a + m2));
        d = 1.0 + even * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + even / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;
        let odd = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1.0));
        d = 1.0 + odd * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + odd / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let step = d * c;
        h *= step;
        if (step - 1.0).abs() < 1e-12 {
            break;
        }
    }
    h
}

// Beta distribution CDF: the regularized incomplete beta function.
fn beta_cdf(x: f64, alpha: f64, beta: f64) -> f64 {
    if alpha <= 0.0 || beta <= 0.0 {
        return 0.0;
    }
    regularized_incomplete_beta(x, alpha, beta)
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

// Student's t distribution CDF (approximation)
fn students_t_cdf(x: f64, df: f64) -> f64 {
    if df <= 0.0 {
        return 0.5; // Invalid, return median
    }
    // Exact, through the incomplete beta: the tails are what tell a t from a normal,
    // and a scaled normal standing in for it had none.
    let tail = 0.5 * regularized_incomplete_beta(df / (df + x * x), df / 2.0, 0.5);
    if x >= 0.0 { 1.0 - tail } else { tail }
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
        DistributionType::Exponential if mean > 0.0 => {
            let lambda = 1.0 / mean;
            let cdf_upper = exponential_cdf(upper, lambda);
            let cdf_lower = exponential_cdf(lower, lambda);
            cdf_upper - cdf_lower
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
        DistributionType::Gamma if mean > 0.0 && std > 0.0 => {
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
        DistributionType::Weibull if mean > 0.0 && std > 0.0 => {
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
        }
        _ => 0.0,
    }
}

/// How many outlier examples an analysis keeps, most extreme first.
const OUTLIER_EXAMPLES: usize = 100;

/// Outliers among every finite value of the column. The counts and the percentage
/// cover all of them; only the list of examples is cut to [`OUTLIER_EXAMPLES`].
fn compute_outlier_analysis(values: &[f64], numeric_stats: &NumericStatistics) -> OutlierAnalysis {
    let mut analysis = OutlierAnalysis {
        total_count: 0,
        percentage: 0.0,
        iqr_count: 0,
        zscore_count: 0,
        outlier_rows: Vec::new(),
    };
    let Some(test) = OutlierTest::new(values, numeric_stats.q25, numeric_stats.q75) else {
        return analysis;
    };

    for (idx, &value) in values.iter().enumerate() {
        let iqr_position = test.iqr_position(value);
        let z_score = test.z_score(value);
        let detection_method = match (iqr_position.is_some(), z_score > Z_THRESHOLD) {
            (true, true) => OutlierMethod::Both,
            (true, false) => OutlierMethod::IQR,
            (false, true) => OutlierMethod::ZScore,
            (false, false) => continue,
        };
        analysis.total_count += 1;
        analysis.iqr_count += usize::from(iqr_position.is_some());
        analysis.zscore_count += usize::from(z_score > Z_THRESHOLD);
        analysis.outlier_rows.push(OutlierRow {
            row_index: idx,
            column_value: value,
            context_data: HashMap::new(),
            detection_method,
            z_score: Some(z_score),
            iqr_position,
        });
    }

    analysis.percentage = analysis.total_count as f64 / values.len() as f64 * 100.0;
    // Most extreme first; a z-score is the distance from the mean in one scale. The
    // hundred are picked before sorting: a long tail has tens of thousands.
    let most_extreme = |a: &OutlierRow, b: &OutlierRow| {
        b.z_score
            .unwrap_or(0.0)
            .total_cmp(&a.z_score.unwrap_or(0.0))
    };
    if analysis.outlier_rows.len() > OUTLIER_EXAMPLES {
        analysis
            .outlier_rows
            .select_nth_unstable_by(OUTLIER_EXAMPLES, most_extreme);
        analysis.outlier_rows.truncate(OUTLIER_EXAMPLES);
    }
    analysis.outlier_rows.sort_by(most_extreme);
    analysis
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

    let columns = numeric_cols
        .iter()
        .map(|name| Ok(Centered::new(df.column(name)?.as_materialized_series())))
        .collect::<Result<Vec<_>>>()?;

    let n = numeric_cols.len();
    let mut correlations = vec![vec![1.0; n]; n];
    let mut p_values = vec![vec![0.0; n]; n];
    let mut sample_sizes = vec![vec![0; n]; n];

    // Every pair is one pass over two columns; fifty columns are 1,225 pairs, so the
    // rows of the matrix are shared out across threads, interleaved to even the load.
    let threads = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(n - 1);
    let pairs: Vec<(usize, usize, f64, usize)> = std::thread::scope(|scope| {
        let columns = &columns;
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                scope.spawn(move || {
                    let mut pairs = Vec::new();
                    for i in (thread..n).step_by(threads) {
                        for j in (i + 1)..n {
                            let (correlation, count) = pearson(&columns[i], &columns[j]);
                            pairs.push((i, j, correlation, count));
                        }
                    }
                    pairs
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap_or_default())
            .collect()
    });

    for (i, j, correlation, sample_size) in pairs {
        sample_sizes[i][j] = sample_size;
        sample_sizes[j][i] = sample_size;
        // Fewer than three pairs say nothing.
        let correlation = if sample_size < 3 {
            f64::NAN
        } else {
            correlation
        };
        correlations[i][j] = correlation;
        correlations[j][i] = correlation;
        if !correlation.is_nan() {
            let p_value = compute_correlation_p_value(correlation, sample_size);
            p_values[i][j] = p_value;
            p_values[j][i] = p_value;
        }
    }

    Ok(CorrelationMatrix {
        columns: numeric_cols,
        correlations,
        p_values: Some(p_values),
        sample_sizes,
    })
}

/// A numeric column ready to correlate: its finite values less their mean, NaN where
/// a value is null or not finite. Centering once keeps the one-pass sums below exact
/// enough, and each pair is then one pass with no copies.
struct Centered {
    values: Vec<f64>,
    /// No value is missing, so every row pairs.
    complete: bool,
}

impl Centered {
    fn new(series: &Series) -> Self {
        let floats = series.cast(&DataType::Float64).ok();
        let mut values: Vec<f64> = match floats.as_ref().and_then(|floats| floats.f64().ok()) {
            Some(floats) => floats
                .iter()
                .map(|v| v.filter(|v| v.is_finite()).unwrap_or(f64::NAN))
                .collect(),
            None => vec![f64::NAN; series.len()],
        };
        let (sum, count) = values
            .iter()
            .filter(|v| !v.is_nan())
            .fold((0.0, 0usize), |(sum, count), v| (sum + v, count + 1));
        if count > 0 {
            let mean = sum / count as f64;
            values.iter_mut().for_each(|v| *v -= mean);
        }
        let complete = count == values.len();
        Self { values, complete }
    }
}

/// Pearson r over the rows where both columns have a value, and how many rows those
/// are. NaN when either is one value throughout those rows.
fn pearson(a: &Centered, b: &Centered) -> (f64, usize) {
    let (mut x, mut y, mut xx, mut yy, mut xy) = (0.0, 0.0, 0.0, 0.0, 0.0);
    let mut count = 0usize;
    let pairs = a.values.iter().zip(&b.values);
    let both = a.complete && b.complete;
    for (&v1, &v2) in pairs {
        if !both && (v1.is_nan() || v2.is_nan()) {
            continue;
        }
        count += 1;
        x += v1;
        y += v2;
        xx += v1 * v1;
        yy += v2 * v2;
        xy += v1 * v2;
    }
    let n = count as f64;
    let (sxx, syy, sxy) = (xx - x * x / n, yy - y * y / n, xy - x * y / n);
    // Measured against the sums of squares: what a column with one value leaves
    // behind is rounding, not spread.
    if count < 2 || sxx <= xx * 1e-12 || syy <= yy * 1e-12 {
        return (f64::NAN, count);
    }
    ((sxy / (sxx * syy).sqrt()).clamp(-1.0, 1.0), count)
}

/// The rows where both columns hold a finite value, as two aligned lists: every pair,
/// so the correlation and the count beside it describe the same rows.
fn finite_pairs(col1: &Series, col2: &Series) -> (Vec<f64>, Vec<f64>) {
    let (Ok(floats1), Ok(floats2)) = (col1.cast(&DataType::Float64), col2.cast(&DataType::Float64))
    else {
        return (Vec::new(), Vec::new());
    };
    let (Ok(floats1), Ok(floats2)) = (floats1.f64(), floats2.f64()) else {
        return (Vec::new(), Vec::new());
    };
    floats1
        .iter()
        .zip(floats2.iter())
        .filter_map(|pair| match pair {
            (Some(v1), Some(v2)) if v1.is_finite() && v2.is_finite() => Some((v1, v2)),
            _ => None,
        })
        .unzip()
}

fn compute_pearson_correlation(values1: &[f64], values2: &[f64]) -> f64 {
    if values1.len() != values2.len() || values1.len() < 2 {
        return f64::NAN;
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

    // A column with one value has no correlation with anything: undefined, not 0,
    // which reads as a finding.
    if var1 == 0.0 || var2 == 0.0 {
        return f64::NAN;
    }

    numerator / (var1.sqrt() * var2.sqrt())
}

/// The two-sided p-value of Pearson's r over `n` pairs: Student's t with `n - 2`
/// degrees of freedom. With `t² = r²·df / (1 - r²)`, both tails together are
/// `I_x(df/2, 1/2)` at `x = df / (df + t²) = 1 - r²`, taken directly so that a small p
/// is not lost to `1 - cdf`.
fn compute_correlation_p_value(correlation: f64, n: usize) -> f64 {
    if n < 3 || correlation.is_nan() {
        return 1.0;
    }
    if correlation.abs() >= 1.0 {
        return 0.0;
    }
    let df = (n - 2) as f64;
    regularized_incomplete_beta(1.0 - correlation * correlation, df / 2.0, 0.5).clamp(0.0, 1.0)
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
    let (values1, values2) = finite_pairs(
        df.column(col1_name)?.as_materialized_series(),
        df.column(col2_name)?.as_materialized_series(),
    );

    let sample_size = values1.len();
    if sample_size < 3 {
        return Err(color_eyre::eyre::eyre!("Not enough data for correlation"));
    }

    let correlation = compute_pearson_correlation(&values1, &values2);
    let p_value = Some(compute_correlation_p_value(correlation, sample_size));

    let stats1 = column_stats(&values1);
    let stats2 = column_stats(&values2);
    let covariance = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| (v1 - stats1.mean) * (v2 - stats2.mean))
        .sum::<f64>()
        / (sample_size - 1) as f64;

    let r_squared = correlation * correlation;

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

fn column_stats(values: &[f64]) -> ColumnStats {
    let (mean, std) = mean_and_std(values);
    ColumnStats {
        mean,
        std,
        min: values.iter().copied().fold(f64::NAN, f64::min),
        max: values.iter().copied().fold(f64::NAN, f64::max),
    }
}

#[cfg(test)]
mod sampling_tests {
    use super::*;

    /// `n` rows of `id` and a `score` that climbs with it, as a Parquet file: a head
    /// sample of it is biased, and a spread one is not.
    fn climbing(dir: &std::path::Path, n: i64) -> LazyFrame {
        let ids: Vec<i64> = (0..n).collect();
        let scores: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
        let mut df = df!("id" => ids, "score" => scores).unwrap();
        let path = dir.join("climbing.parquet");
        ParquetWriter::new(std::fs::File::create(&path).unwrap())
            .with_row_group_size(Some(1_000))
            .finish(&mut df)
            .unwrap();
        LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default())
            .unwrap()
    }

    fn mean(df: &DataFrame) -> f64 {
        df.column("score")
            .unwrap()
            .as_materialized_series()
            .mean()
            .unwrap()
    }

    #[test]
    fn a_small_table_is_read_whole() {
        let dir = tempfile::tempdir().unwrap();
        let rows = analysis_rows(&climbing(dir.path(), 500), Some(1_000), None, 1, false).unwrap();
        assert_eq!(rows.df.height(), 500);
        assert_eq!(rows.total_rows, 500);
        assert_eq!(rows.sample_size, None);
    }

    #[test]
    fn a_parquet_scan_is_sampled_in_blocks_across_all_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let lf = climbing(dir.path(), 100_000);
        assert!(slices_reach_into_the_scan(&lf), "a Parquet scan seeks");
        let rows = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
        assert_eq!(rows.df.height(), 5_000);
        assert_eq!(rows.total_rows, 100_000);
        assert_eq!(rows.sample_size, Some(5_000));
        // The table's mean is 49,999.5; a head sample's would be 2,499.5.
        assert!(
            (mean(&rows.df) - 49_999.5).abs() < 2_500.0,
            "{}",
            mean(&rows.df)
        );
        // In table order, and reaching its last stretch.
        let ids = rows.df.column("id").unwrap().i64().unwrap();
        assert!(ids.into_no_null_iter().is_sorted());
        assert!(ids.max().unwrap() > 95_000);
        // The same seed, the same sample; another seed, another.
        let again = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
        assert!(rows.df.equals(&again.df));
        let other = analysis_rows(&lf, Some(5_000), None, 8, false).unwrap();
        assert!(!rows.df.equals(&other.df));
    }

    /// Seeded runs, and a table read whole because it is under twice the sample, say
    /// where each kept row sat: the row's id, in a table whose id is its position.
    #[test]
    fn a_block_sample_says_where_its_rows_sat() {
        for (rows, n) in [(100_000, 5_000), (10_000, 6_000)] {
            let dir = tempfile::tempdir().unwrap();
            let lf = climbing(dir.path(), rows);
            let read = sample_rows_counting(&lf, Some(n), None, 7, false, None, None).unwrap();
            let ids: Vec<IdxSize> = read
                .rows
                .df
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .map(|id| id as IdxSize)
                .collect();
            assert_eq!(ids.len(), n);
            assert_eq!(ids, read.positions, "{rows} rows, n={n}");
            assert_eq!(read.counted, None, "runs see too few rows to count");
        }
    }

    /// Odd and small sizes: exactly `n` distinct rows, reaching the end of the table.
    /// Runs rounded up and then cut to `n` once dropped the last blocks; runs wider
    /// than their stretch overlapped.
    #[test]
    fn a_block_sample_of_any_size_is_n_distinct_rows_across_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let lf = climbing(dir.path(), 10_000);
        for (n, seed) in [
            (60, 1),
            (101, 2),
            (1_234, 3),
            (4_999, 4),
            (5_001, 5),
            (9_999, 6),
        ] {
            let rows = analysis_rows(&lf, Some(n), None, seed, false).unwrap();
            let ids: Vec<i64> = rows
                .df
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect();
            assert_eq!(ids.len(), n, "n={n}");
            let unique: std::collections::HashSet<_> = ids.iter().collect();
            assert_eq!(unique.len(), n, "no duplicates at n={n}");
            assert!(ids.is_sorted(), "table order at n={n}");
            assert!(
                *ids.last().unwrap() > 9_000,
                "reaches the end at n={n}: {ids:?}"
            );
        }
    }

    #[test]
    fn only_a_single_file_scan_is_sampled_in_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let one = climbing(dir.path(), 1_000);
        // A column stubbed above the scan, as binary columns are: still seekable.
        let stubbed = one
            .clone()
            .select([col("id"), lit(NULL).cast(DataType::Binary).alias("blob")]);
        assert!(slices_reach_into_the_scan(&stubbed));
        // Two files: each slice would open the footers of those before it.
        let two = concat([one.clone(), one.clone()], UnionArgs::default()).unwrap();
        assert!(!slices_reach_into_the_scan(&two));
        let glob = LazyFrame::scan_parquet(
            PlRefPath::try_from_path(&dir.path().join("*.parquet")).unwrap(),
            Default::default(),
        )
        .unwrap();
        std::fs::copy(
            dir.path().join("climbing.parquet"),
            dir.path().join("again.parquet"),
        )
        .unwrap();
        assert!(!slices_reach_into_the_scan(&glob), "two files in one scan");
        assert!(!slices_reach_into_the_scan(
            &one.clone().sort(["id"], Default::default())
        ));
    }

    #[test]
    fn a_filtered_view_is_sampled_in_one_uniform_pass() {
        let dir = tempfile::tempdir().unwrap();
        let lf = climbing(dir.path(), 100_000).filter(col("id").gt_eq(lit(50_000)));
        assert!(
            !slices_reach_into_the_scan(&lf),
            "a slice of a filter reads what is ahead of it"
        );
        let rows = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
        assert_eq!(rows.df.height(), 5_000);
        assert_eq!(rows.total_rows, 50_000, "the pass counts as it goes");
        assert!(
            (mean(&rows.df) - 74_999.5).abs() < 1_500.0,
            "{}",
            mean(&rows.df)
        );
        assert!(
            rows.df.column(SAMPLE_POSITION).is_err(),
            "the position column does not leak"
        );
        let again = analysis_rows(&lf, Some(5_000), None, 7, false).unwrap();
        assert!(rows.df.equals(&again.df), "seeded");
    }

    #[test]
    fn no_sample_size_reads_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let rows = analysis_rows(&climbing(dir.path(), 20_000), None, None, 1, false).unwrap();
        assert_eq!(rows.df.height(), 20_000);
        assert_eq!(rows.sample_size, None);
    }

    #[test]
    fn a_constant_column_correlates_with_nothing() {
        let df = df!(
            "year" => vec![2020.0f64; 50],
            "value" => (0..50).map(|i| i as f64).collect::<Vec<_>>(),
            "double" => (0..50).map(|i| 2.0 * i as f64).collect::<Vec<_>>(),
            // Its mean is not exactly 0.1, so centering leaves rounding behind.
            "tenth" => vec![0.1f64; 50]
        )
        .unwrap();
        let matrix = compute_correlation_matrix(&df).unwrap();
        assert!(matrix.correlations[0][1].is_nan(), "undefined, not 0");
        assert!(
            matrix.correlations[3][1].is_nan(),
            "{}",
            matrix.correlations[3][1]
        );
        assert!((matrix.correlations[1][2] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn the_incomplete_beta_and_the_t_cdf_are_exact() {
        // I_0.5(2, 3) = 11/16.
        let i = regularized_incomplete_beta(0.5, 2.0, 3.0);
        assert!((i - 0.6875).abs() < 1e-6, "{i}");
        // The 97.5th percentile of t with 5 degrees of freedom is 2.5706.
        assert!((students_t_cdf(2.5706, 5.0) - 0.975).abs() < 1e-4);
        assert!((students_t_cdf(-2.5706, 5.0) - 0.025).abs() < 1e-4);
        assert!((students_t_cdf(0.0, 5.0) - 0.5).abs() < 1e-12);
        // Beta(1, 1) is the uniform.
        assert!((beta_cdf(0.3, 1.0, 1.0) - 0.3).abs() < 1e-6);
    }

    #[test]
    fn a_constant_column_is_constant_and_a_hopeless_one_has_no_clear_fit() {
        let constant = Series::new("c".into(), vec![2020.0f64; 500]);
        let info = infer_distribution(&constant, &constant, 500, false);
        assert_eq!(info.distribution_type, DistributionType::Constant);

        // Two far-apart clusters of non-integers: every candidate is rejected.
        let values: Vec<f64> = (0..2_000)
            .map(|i| {
                let jitter = (i % 97) as f64 * 0.013;
                if i % 2 == 0 {
                    -1_000.3 + jitter
                } else {
                    1_000.7 + jitter
                }
            })
            .collect();
        let bimodal = Series::new("b".into(), values);
        let info = infer_distribution(&bimodal, &bimodal, 2_000, false);
        assert_eq!(info.distribution_type, DistributionType::Unknown);
        assert_eq!(info.distribution_type.to_string(), "No clear fit");

        // Integers, half of them negative: no count distribution, whatever the
        // non-negative half looks like on its own.
        let values: Vec<f64> = (0..2_000)
            .map(|i| {
                if i % 2 == 0 {
                    -1_000.0 + (i % 7) as f64
                } else {
                    1_000.0 + (i % 5) as f64
                }
            })
            .collect();
        let integers = Series::new("i".into(), values);
        let info = infer_distribution(&integers, &integers, 2_000, false);
        assert!(
            !matches!(
                info.distribution_type,
                DistributionType::Binomial
                    | DistributionType::Poisson
                    | DistributionType::Geometric
            ),
            "{:?}",
            info.distribution_type
        );
    }
}

#[cfg(test)]
mod normality_tests {
    use super::*;

    /// Against SciPy's `2 * t.sf(|t|, n - 2)`. A normal CDF standing in for the t,
    /// and a tanh standing in for the normal, gave r = 0.01 over 100,000 pairs p = 0.023
    /// where it is 0.0016.
    #[test]
    fn correlation_p_values_are_students_t() {
        let cases = [
            (0.5, 10, 0.14111328125000006),
            (-0.2, 50, 0.16375308124541754),
            (0.3, 30, 0.10724594805795436),
            (0.9, 5, 0.03738607346849862),
            (0.1, 100, 0.32221736303061954),
            (0.02, 20_000, 0.004676184609440329),
            (0.01, 100_000, 0.0015651897452783157),
            (0.005, 1_000_000, 5.732288112893878e-7),
            (0.1, 10_000, 1.1970504236520445e-23),
        ];
        for (r, n, expected) in cases {
            let p = compute_correlation_p_value(r, n);
            assert!(
                (p - expected).abs() <= 1e-9 * expected,
                "r {r}, n {n}: {p} against {expected}"
            );
        }
        assert_eq!(compute_correlation_p_value(1.0, 10), 0.0);
        assert_eq!(compute_correlation_p_value(0.0, 10), 1.0);
    }

    /// Shapiro-Francia's p-value is a p-value: a normal sample passes, and a price
    /// series of two regimes, W' = 0.929 over 2,590 values, does not.
    #[test]
    fn the_normality_p_value_is_a_p_value() {
        assert!(shapiro_francia_pvalue(0.929, 2_590).unwrap() < 1e-10);
        assert!(shapiro_francia_pvalue(0.9995, 2_590).unwrap() > 0.05);
        assert_eq!(shapiro_francia_pvalue(0.99, 4), None);
        let normal: Vec<f64> = (1..=500)
            .map(|i| normal_quantile(i as f64 / 501.0))
            .collect();
        let (_, p) = approximate_shapiro_wilk(&normal);
        assert!(p.unwrap() > 0.5, "{p:?}");
    }

    /// Royston's approximation is calibrated: normal samples fall below 0.05 about one
    /// time in twenty, and skewed ones nearly always.
    #[test]
    fn the_normality_p_value_is_calibrated() {
        let mut rng = crate::distribution_fit::Rng::new(2_026);
        let mut sample = |skewed: bool| -> Vec<f64> {
            (0..100)
                .map(|_| {
                    let z = rng.normal();
                    if skewed { z.exp() } else { z }
                })
                .collect()
        };
        let below = |values: Vec<f64>| approximate_shapiro_wilk(&values).1.unwrap() < 0.05;
        let false_alarms = (0..400).filter(|_| below(sample(false))).count();
        assert!(
            (8..=36).contains(&false_alarms),
            "{false_alarms} of 400 normal samples below 0.05"
        );
        let caught = (0..100).filter(|_| below(sample(true))).count();
        assert!(caught >= 95, "{caught} of 100 log-normal samples caught");
    }

    /// NaN and infinities never reach a fit: one NaN left a `partial_cmp` sort out of
    /// order and folded the Q-Q plot.
    #[test]
    fn non_finite_values_are_left_out() {
        let series = Series::new("x".into(), &[3.0, f64::NAN, 1.0, f64::INFINITY, 2.0]);
        assert_eq!(get_numeric_values_as_f64(&series), vec![3.0, 1.0, 2.0]);
        assert_eq!(finite_values(&series), vec![3.0, 1.0, 2.0]);
        let integers = Series::new("i".into(), &[Some(4i16), None, Some(-2)]);
        assert_eq!(finite_values(&integers), vec![4.0, -2.0]);
    }

    /// One value throughout, even one a float cannot hold exactly, has no skew; and a
    /// symmetric set has none either.
    #[test]
    fn a_constant_has_no_shape() {
        assert_eq!(skewness_and_kurtosis(&[0.1; 50]), (0.0, 3.0));
        assert_eq!(skewness_and_kurtosis(&[1.0, 2.0]), (0.0, 3.0));
        let (skewness, _) = skewness_and_kurtosis(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!(skewness.abs() < 1e-12);
    }
}

#[cfg(test)]
pub(crate) mod describe_tests {
    use super::*;

    /// A frame with one column of each temporal type, a zoned datetime too, five
    /// values and a null each.
    pub(crate) fn temporal_frame() -> DataFrame {
        let day = 20_089i32; // 2025-01-01
        let dates = Series::new(
            "day".into(),
            &[
                Some(day + 4),
                Some(day),
                None,
                Some(day + 2),
                Some(day + 1),
                Some(day + 3),
            ],
        )
        .cast(&DataType::Date)
        .unwrap();
        let hour = 3_600_000_000i64; // microseconds
        let start = 1_735_678_075_000_000i64; // 2024-12-31 20:47:55
        let pickups = Series::new(
            "pickup".into(),
            &[
                Some(start + 4 * hour),
                Some(start),
                None,
                Some(start + 2 * hour),
                Some(start + hour),
                Some(start + 3 * hour),
            ],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let second = 1_000_000_000i64; // nanoseconds
        let times = Series::new(
            "at".into(),
            &[
                Some(9 * 3600 * second + 40 * second),
                Some(9 * 3600 * second),
                None,
                Some(9 * 3600 * second + 20 * second),
                Some(9 * 3600 * second + 10 * second),
                Some(9 * 3600 * second + 30 * second),
            ],
        )
        .cast(&DataType::Time)
        .unwrap();
        // The same instants in New York, in milliseconds: the zone survives the cast back.
        let local = Series::new(
            "local".into(),
            &[
                Some(start / 1000 + 4 * hour / 1000),
                Some(start / 1000),
                None,
                Some(start / 1000 + 2 * hour / 1000),
                Some(start / 1000 + hour / 1000),
                Some(start / 1000 + 3 * hour / 1000),
            ],
        )
        .cast(&DataType::Datetime(
            TimeUnit::Milliseconds,
            TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
        ))
        .unwrap();
        let minute = 60_000i64; // milliseconds
        let waits = Series::new(
            "wait".into(),
            &[
                Some(5 * minute),
                Some(minute),
                None,
                Some(3 * minute),
                Some(2 * minute),
                Some(4 * minute),
            ],
        )
        .cast(&DataType::Duration(TimeUnit::Milliseconds))
        .unwrap();
        DataFrame::new_infer_height(vec![
            dates.into(),
            pickups.into(),
            times.into(),
            local.into(),
            waits.into(),
        ])
        .unwrap()
    }

    #[test]
    fn describe_gives_dates_and_times_their_range_in_their_own_format() {
        let df = temporal_frame();
        let every_row = crate::sampling::Sample {
            method: crate::sampling::SampleMethod::EveryRow,
            ..crate::sampling::Sample::default()
        };
        let lazy = compute_describe_from_lazy(&df.clone().lazy(), Some(6), &every_row, false)
            .unwrap()
            .column_statistics;
        let schema = df.schema().clone();
        let sampled = compute_describe_single_aggregation(&df, &schema, 6, None, 0, false)
            .unwrap()
            .column_statistics;
        let expected = [
            [
                "2025-01-03",
                "2025-01-01",
                "2025-01-02",
                "2025-01-03",
                "2025-01-04",
                "2025-01-05",
            ],
            [
                "2024-12-31 22:47:55",
                "2024-12-31 20:47:55",
                "2024-12-31 21:47:55",
                "2024-12-31 22:47:55",
                "2024-12-31 23:47:55",
                "2025-01-01 00:47:55",
            ],
            [
                "09:00:20", "09:00:00", "09:00:10", "09:00:20", "09:00:30", "09:00:40",
            ],
            [
                "2024-12-31 17:47:55 EST",
                "2024-12-31 15:47:55 EST",
                "2024-12-31 16:47:55 EST",
                "2024-12-31 17:47:55 EST",
                "2024-12-31 18:47:55 EST",
                "2024-12-31 19:47:55 EST",
            ],
            ["3m", "1m", "2m", "3m", "4m", "5m"],
        ];
        for stats in [&lazy, &sampled] {
            assert_eq!(stats.len(), expected.len());
            for (column, want) in stats.iter().zip(expected) {
                assert!(column.numeric_stats.is_none(), "{}", column.name);
                assert_eq!(column.null_count, 1);
                let t = column.temporal_stats.as_ref().expect("temporal stats");
                let got = [&t.mean, &t.min, &t.q25, &t.median, &t.q75, &t.max]
                    .map(|v| v.clone().unwrap_or_default());
                assert_eq!(got, want.map(String::from), "{}", column.name);
            }
        }
    }

    #[test]
    fn describe_of_an_all_null_datetime_is_empty() {
        let empty = Series::new("never".into(), &[None::<i64>, None])
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap();
        let df = DataFrame::new_infer_height(vec![empty.into()]).unwrap();
        let schema = df.schema().clone();
        let stats = compute_describe_single_aggregation(&df, &schema, 2, None, 0, false)
            .unwrap()
            .column_statistics;
        let t = stats[0].temporal_stats.as_ref().expect("temporal stats");
        assert!(
            [&t.mean, &t.min, &t.q25, &t.median, &t.q75, &t.max]
                .iter()
                .all(|v| v.is_none())
        );
    }
}

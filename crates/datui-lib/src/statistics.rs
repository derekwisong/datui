use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::polars_compute::rolling::QuantileMethod;
use polars::prelude::*;
use std::collections::HashMap;
use std::ops::Range;

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
        if may_stream(&lf, use_streaming) && !sorts_by_one_wide_key(&lf) {
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

/// Whether a query over `lf` may use the streaming engine: asked for, and possible.
/// Polars 0.55's streaming engine cannot run an anonymous scan (a SQLite table): it
/// stops at a `todo!`.
pub fn may_stream(lf: &LazyFrame, wanted: bool) -> bool {
    use polars::lazy::dsl::{DslPlan, FileScanDsl};
    wanted
        && !lf.logical_plan.into_iter().any(|node| match node {
            DslPlan::Scan { scan_type, .. } => {
                matches!(scan_type.as_ref(), FileScanDsl::Anonymous { .. })
            }
            _ => false,
        })
}

/// Whether `lf` takes the first rows of an unstable sort by a single Decimal or Int128
/// key. Polars 0.55's streaming engine runs that as a top-k, which panics on those
/// dtypes ("not implemented for dtype Int128"); the in-memory engine sorts them. A
/// format spec's `scale` reads as Decimal, so a query's `by price`, whose group sort
/// is unstable, takes this for its first page. A stable sort (the table's own) carries
/// a row index as a second key, and a full sort or a slice further in is no top-k:
/// both stream.
#[cfg(feature = "streaming")]
fn sorts_by_one_wide_key(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::DslPlan;
    let wide_sort = |node: &DslPlan| match node {
        DslPlan::Sort {
            input,
            by_column,
            sort_options,
            ..
        } if by_column.len() == 1 && !sort_options.maintain_order => {
            LazyFrame::from((**input).clone())
                .select([by_column[0].clone()])
                .collect_schema()
                .ok()
                .and_then(|schema| schema.get_at_index(0).map(|(_, dtype)| dtype.clone()))
                .is_some_and(|dtype| dtype.is_decimal() || dtype == DataType::Int128)
        }
        _ => false,
    };
    lf.logical_plan.into_iter().any(|node| match node {
        DslPlan::Slice {
            input, offset: 0, ..
        } => input.into_iter().any(wide_sort),
        DslPlan::Sort { sort_options, .. } => sort_options.limit.is_some() && wide_sort(node),
        _ => false,
    })
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
pub struct DistributionAnalysis {
    pub column_name: String,
    pub distribution_type: DistributionType,
    /// The chosen family's p-value; with no clear fit, the best any family managed.
    pub confidence: f64,
    pub characteristics: DistributionCharacteristics,
    pub outliers: OutlierAnalysis,
    pub percentiles: PercentileBreakdown,
    /// At most five thousand of the column's finite values, spread across it, sorted.
    pub sorted_sample_values: Vec<f64>,
    /// Whether the rows analyzed were a sample of the table.
    pub is_sampled: bool,
    /// How many values `sorted_sample_values` holds.
    pub sample_size: usize,
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

/// Which coefficient the correlation matrix shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CorrelationMethod {
    /// Pearson's r: how close the pairs fall to a line.
    #[default]
    Pearson,
    /// Spearman's ρ: Pearson's r of the pairs' ranks, for any monotone relation.
    Spearman,
}

impl CorrelationMethod {
    pub fn toggled(self) -> Self {
        match self {
            Self::Pearson => Self::Spearman,
            Self::Spearman => Self::Pearson,
        }
    }
}

// Correlation matrix structures
#[derive(Clone)]
pub struct CorrelationMatrix {
    pub columns: Vec<String>,            // Numeric column names
    pub correlations: Vec<Vec<f64>>,     // Square matrix of Pearson correlations
    pub p_values: Option<Vec<Vec<f64>>>, // Statistical significance (optional)
    pub sample_sizes: Vec<Vec<usize>>,   // Sample size for each pair
    /// Spearman's ρ for each pair, over the same pairs as Pearson's r; `None` when
    /// the rows read hold more values than [`RANK_VALUES`].
    pub rank_correlations: Option<Vec<Vec<f64>>>,
    pub rank_p_values: Option<Vec<Vec<f64>>>,
}

/// The most values Spearman's ρ ranks: eight bytes each, so 512 MiB beside the rows
/// read. A sample's 100,000 rows rank up to 671 columns; a read of every row of a
/// large table can be past it, and the matrix then has Pearson's r only.
pub const RANK_VALUES: usize = 64 * 1024 * 1024;

impl CorrelationMatrix {
    /// The pair's coefficient by `method`; NaN where there is none.
    pub fn coefficient(&self, method: CorrelationMethod, row: usize, col: usize) -> f64 {
        let matrix = match method {
            CorrelationMethod::Pearson => Some(&self.correlations),
            CorrelationMethod::Spearman => self.rank_correlations.as_ref(),
        };
        matrix
            .and_then(|m| m.get(row))
            .and_then(|r| r.get(col))
            .copied()
            .unwrap_or(f64::NAN)
    }

    /// The pair's p-value by `method`, when the matrix has them.
    pub fn p_value(&self, method: CorrelationMethod, row: usize, col: usize) -> Option<f64> {
        let matrix = match method {
            CorrelationMethod::Pearson => self.p_values.as_ref(),
            CorrelationMethod::Spearman => self.rank_p_values.as_ref(),
        };
        matrix.and_then(|m| m.get(row)?.get(col).copied())
    }
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
    /// Fit distributions to the numeric columns. Analyses need this and
    /// `include_distribution_analyses` both.
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
        v => Some(crate::exact::str_value(&v).to_string()),
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
    let mut distribution_analyses = Vec::new();

    for (name, dtype) in schema.iter() {
        let col = df.column(name)?;
        let series = col.as_materialized_series();
        let count = series.len();
        let null_count = series.null_count();

        let numeric = if is_numeric_type(dtype) {
            Some(NumericColumn::of(series)?)
        } else {
            None
        };
        let numeric_stats = numeric
            .as_ref()
            .map(|column| compute_numeric_stats(column, options.include_skewness_kurtosis_outliers))
            .transpose()?;

        let categorical_stats = if is_categorical_type(dtype) {
            Some(compute_categorical_stats(series)?)
        } else {
            None
        };

        if options.include_distribution_info
            && options.include_distribution_analyses
            && null_count < count
            && let (Some(column), Some(stats)) = (&numeric, &numeric_stats)
        {
            distribution_analyses.push(distribution_analysis(
                name,
                column,
                stats,
                actual_sample_size.unwrap_or(count),
                should_sample,
            ));
        }

        column_statistics.push(ColumnStatistics {
            name: name.to_string(),
            dtype: dtype.clone(),
            count,
            null_count,
            numeric_stats,
            categorical_stats,
            temporal_stats: temporal_stats_of(series)?,
        });
    }

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
    df.column(col_name).ok().and_then(|s| {
        s.get(row)
            .ok()
            .map(|v| crate::exact::str_value(&v).to_string())
    })
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
/// `count` when the read sees every row: a streamed pass, or a table read whole
/// because it is under twice the sample. Seeded runs see too few rows to count, and
/// a read of the whole scope is not a sample, so neither counts.
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
    let along = Along {
        watch,
        count,
        on_run: None,
    };
    let read = block_sample(lf, total_rows, n, seed, polars_streaming, along)?;
    Ok(crate::sampling::SampledRows {
        rows: AnalysisRows {
            sample_size: Some(read.df.height()),
            df: read.df,
            total_rows,
            per_value: None,
        },
        positions: read.positions,
        counted: read.counted,
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

/// [`block_sample`] for a sample shown as it is drawn: each run goes to `on_run`, with
/// where it starts, as it lands. A table under twice the sample is read whole and cut,
/// and comes back as one frame instead. A stop ends the read with the runs so far
/// delivered.
pub(crate) fn block_sample_live(
    lf: &LazyFrame,
    total_rows: usize,
    n: usize,
    seed: u64,
    polars_streaming: bool,
    watch: &crate::sampling::ReadWatch,
    on_run: &OnRun<'_>,
) -> Result<Option<DataFrame>> {
    let along = Along {
        watch: Some(watch),
        count: None,
        on_run: Some(on_run),
    };
    let read = block_sample(lf, total_rows, n, seed, polars_streaming, along)?;
    Ok((total_rows < 2 * n).then_some(read.df))
}

/// What a block sample does beside reading its runs: stops when `watch` says to,
/// counts `count`'s key, and hands each run to `on_run` as it lands.
struct Along<'a> {
    watch: Option<&'a crate::sampling::ReadWatch>,
    count: Option<&'a Expr>,
    on_run: Option<&'a OnRun<'a>>,
}

/// Told of each run of a block sample as it lands, with where it starts.
pub(crate) type OnRun<'a> = dyn Fn(usize, &DataFrame) + Sync + 'a;

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
    along: Along<'_>,
) -> Result<StreamRead> {
    let Along {
        watch,
        count,
        on_run,
    } = along;
    // Under twice the sample, reading the table is about as cheap as reading runs of
    // it, and runs that must fit side by side would crowd or overlap. Read it and keep
    // a seeded uniform `n` of it instead, counting `count`'s key from the rows read.
    if total_rows < 2 * n {
        let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
        let counted = match count {
            Some(key) => {
                let mut keys = df
                    .clone()
                    .lazy()
                    .select([key.clone().alias(crate::sampling::COUNT_KEY)])
                    .collect()?;
                let mut counter = crate::sampling::KeyCounter::default();
                counter.observe(&mut keys)?;
                Some(counter.finish())
            }
            None => None,
        };
        let mut ranked: Vec<(u64, IdxSize)> = (0..df.height())
            .map(|i| (sample_rank(seed, i as u64), i as IdxSize))
            .collect();
        ranked.sort_unstable();
        let mut keep: Vec<IdxSize> = ranked.into_iter().take(n).map(|(_, i)| i).collect();
        keep.sort_unstable();
        let df = df.take(&IdxCa::from_vec("sample".into(), keep.clone()))?;
        return Ok(StreamRead {
            df,
            seen: total_rows,
            positions: keep,
            counted,
        });
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
                            if let Some(on_run) = on_run {
                                on_run(*offset, &df);
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
    Ok(StreamRead {
        df: out.unwrap_or_default(),
        seen: total_rows,
        positions,
        counted: None,
    })
}

/// What [`stream_sample`] or [`block_sample`] read.
struct StreamRead {
    df: DataFrame,
    /// Rows in the scope.
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
                let mut reservoir = callback_state
                    .lock()
                    .map_err(|_| PolarsError::ComputeError("sampler lock failed".into()))?;
                reservoir.observe(batch)?;
                if let Some(watch) = &callback_watch {
                    let held = reservoir.kept.as_ref();
                    watch.hold(
                        held.map_or(0, |kept| kept.estimated_size() as u64),
                        held.map_or(0, DataFrame::height),
                    );
                }
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

/// A numeric column cast to `f64` once, for everything the analysis computes from it.
struct NumericColumn {
    floats: Float64Chunked,
    /// Every finite value: nulls, NaN and infinities left out. No distribution has
    /// NaN, and one is enough to leave a sort by `partial_cmp` out of order.
    finite: Vec<f64>,
}

impl NumericColumn {
    fn of(series: &Series) -> Result<Self> {
        let floats = series.cast(&DataType::Float64)?.f64()?.clone();
        let finite = floats.iter().flatten().filter(|v| v.is_finite()).collect();
        Ok(Self { floats, finite })
    }

    /// Up to ten thousand finite values, every k-th row's rather than the first ten
    /// thousand: a sample is spread across the table, and its head is one stretch of it.
    fn spread(&self) -> Vec<f64> {
        const MAX_VALUES: usize = 10_000;
        let step = self.floats.len().div_ceil(MAX_VALUES).max(1);
        self.floats
            .iter()
            .step_by(step)
            .flatten()
            .filter(|v| v.is_finite())
            .collect()
    }
}

fn compute_numeric_stats(
    column: &NumericColumn,
    include_advanced: bool,
) -> Result<NumericStatistics> {
    // Cast and aggregate as Describe does (`build_describe_aggregation_exprs`), so a
    // sample's median is one number wherever it is shown.
    let floats = column.floats.clone().into_series();
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
        let (skewness, kurtosis) = skewness_and_kurtosis(&column.finite);
        let (out_iqr, out_zscore) = detect_outliers(&column.finite, q25, q75);
        (skewness, kurtosis, out_iqr, out_zscore)
    } else {
        (0.0, 3.0, 0, 0)
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
            Some(col) => col.first().map(|v| crate::exact::str_value(v).to_string()),
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
            let value_str = crate::exact::str_value(value);
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

/// The family a column's values follow, its p-value and every family's fit. `rows`
/// is how many rows the column was read from.
struct ColumnFit {
    distribution_type: DistributionType,
    confidence: f64,
    fits: Vec<(DistributionType, crate::distribution_fit::FitOutcome)>,
}

fn infer_distribution(values: &[f64], rows: usize) -> ColumnFit {
    let unknown = ColumnFit {
        distribution_type: DistributionType::Unknown,
        confidence: 0.0,
        fits: Vec::new(),
    };
    if rows < 3 || values.is_empty() {
        return unknown;
    }

    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    let std = variance.sqrt();

    // One value throughout fits every distribution's degenerate case and none of
    // them usefully; a year column in a partitioned table is the usual one.
    if std == 0.0 {
        return ColumnFit {
            distribution_type: DistributionType::Constant,
            confidence: 1.0,
            fits: Vec::new(),
        };
    }

    // Counts are described by a count distribution when one holds.
    let counts = values.iter().all(|v| *v >= 0.0 && *v == v.floor());
    let fits = crate::distribution_fit::test_all(values, FIT_SEED);
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
    ColumnFit {
        distribution_type,
        confidence,
        fits,
    }
}

/// The Shapiro-Francia statistic of `sorted` against normal scores at Blom's plotting
/// positions `(i + 1 - 3/8) / (n + 1/4)`, and its p-value.
fn approximate_shapiro_wilk(sorted: &[f64]) -> (Option<f64>, Option<f64>) {
    let n = sorted.len();
    if n < 3 {
        return (None, None);
    }

    let mean: f64 = sorted.iter().sum::<f64>() / n as f64;
    let variance: f64 = sorted.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let std = variance.sqrt();

    if std == 0.0 {
        return (None, None);
    }

    let mut sum_expected_sq = 0.0;
    let mut sum_data_sq = 0.0;
    let mut sum_product = 0.0;

    for (i, &value) in sorted.iter().enumerate() {
        let p = (i as f64 + 1.0 - 0.375) / (n as f64 + 0.25);
        let expected_quantile = crate::distribution_fit::normal_quantile(p);
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
    Some((1.0 - crate::distribution_fit::normal_cdf(z)).clamp(0.0, 1.0))
}

/// One numeric column's distribution: its fits, normality, outliers and the sorted
/// values its Q-Q plot draws. `rows` is how many rows the column was read from.
fn distribution_analysis(
    column_name: &str,
    column: &NumericColumn,
    numeric_stats: &NumericStatistics,
    rows: usize,
    is_sampled: bool,
) -> DistributionAnalysis {
    let spread = column.spread();
    let fit = infer_distribution(&spread, rows);
    // At most five thousand, spread across the rows: the head of a table sorted by
    // date is its first few years.
    const MAX_VALUES: usize = 5_000;
    let step = spread.len().div_ceil(MAX_VALUES).max(1);
    let mut sorted_sample_values: Vec<f64> = spread.into_iter().step_by(step).collect();
    sorted_sample_values.sort_by(f64::total_cmp);
    let actual_sample_size = sorted_sample_values.len();

    let (sw_stat, sw_pvalue) = approximate_shapiro_wilk(&sorted_sample_values);
    let coefficient_of_variation = if numeric_stats.mean != 0.0 {
        numeric_stats.std / numeric_stats.mean.abs()
    } else {
        0.0
    };

    let mode = compute_mode(&sorted_sample_values);

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

    let qq = fit
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

    let outliers = compute_outlier_analysis(&column.finite, numeric_stats);

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
        distribution_type: fit.distribution_type,
        confidence: fit.confidence,
        characteristics,
        outliers,
        percentiles,
        sorted_sample_values,
        is_sampled,
        sample_size: actual_sample_size,
        fits: fit.fits,
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
    let columns = df
        .schema()
        .iter()
        .filter(|(_, dtype)| is_numeric_type(dtype))
        .count()
        .max(1);
    let band = CORRELATION_SCRATCH_BYTES / (columns * std::mem::size_of::<f64>());
    correlation_matrix_in_bands(df, band.max(MIN_BAND_ROWS))
}

/// Bytes of converted values a correlation matrix holds at once, beyond the sample
/// itself and the matrix: one band of the 100,000-row sample is the whole of it up
/// to 83 columns.
const CORRELATION_SCRATCH_BYTES: usize = 64 * 1024 * 1024;

/// The fewest rows in a band, so a schema of thousands of columns is not read a few
/// rows at a time.
const MIN_BAND_ROWS: usize = 1024;

/// Rows cast to floats at a time, so no cast is the size of a column.
const CAST_ROWS: usize = 16 * 1024;

/// [`compute_correlation_matrix`] converting `band` rows of every column at a time.
fn correlation_matrix_in_bands(df: &DataFrame, band: usize) -> Result<CorrelationMatrix> {
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

    let series = numeric_cols
        .iter()
        .map(|name| Ok(df.column(name)?.as_materialized_series()))
        .collect::<Result<Vec<_>>>()?;

    let n = numeric_cols.len();
    let rows = df.height();
    let band = band.clamp(1, rows.max(1));

    // Each column's mean first, then a band of rows of every column at a time, less
    // those means. Each pair's sums run on from one band to the next in row order:
    // the same sums as one pass over whole columns, with every column converted once
    // and no more than a band of them held.
    let mut shifts = vec![Shift::default(); n];
    across_threads(
        series.iter().zip(shifts.iter_mut()).collect(),
        |(series, shift)| *shift = Shift::new(series),
    );
    let mut sums: Vec<Vec<PairSums>> = (0..n)
        .map(|i| vec![PairSums::default(); n - i - 1])
        .collect();
    let mut bands: Vec<Vec<f64>> = (0..n).map(|_| Vec::with_capacity(band)).collect();
    for start in (0..rows).step_by(band) {
        let within = start..(start + band).min(rows);
        across_threads(
            series.iter().zip(&shifts).zip(bands.iter_mut()).collect(),
            |((series, shift), values)| shift.fill(series, within.clone(), values),
        );
        // Every pair is one pass over two columns; fifty columns are 1,225 pairs, so
        // the rows of the matrix are shared out across threads, interleaved to even
        // the load.
        let (bands, shifts) = (&bands, &shifts);
        across_threads(sums.iter_mut().enumerate().collect(), |(i, row)| {
            for (k, sums) in row.iter_mut().enumerate() {
                let j = i + 1 + k;
                let both = shifts[i].complete && shifts[j].complete;
                sums.add_pairs(&bands[i], &bands[j], both);
            }
        });
    }

    let mut correlations = vec![vec![1.0; n]; n];
    let mut p_values = vec![vec![0.0; n]; n];
    let mut sample_sizes = vec![vec![0; n]; n];
    for (i, row) in sums.iter().enumerate() {
        for (k, sums) in row.iter().enumerate() {
            let j = i + 1 + k;
            let sample_size = sums.count;
            sample_sizes[i][j] = sample_size;
            sample_sizes[j][i] = sample_size;
            // Fewer than three pairs say nothing.
            let correlation = if sample_size < 3 {
                f64::NAN
            } else {
                sums.correlation()
            };
            correlations[i][j] = correlation;
            correlations[j][i] = correlation;
            if !correlation.is_nan() {
                let p_value = compute_correlation_p_value(correlation, sample_size);
                p_values[i][j] = p_value;
                p_values[j][i] = p_value;
            }
        }
    }

    let ranked = (rows.saturating_mul(n) <= RANK_VALUES)
        .then(|| rank_correlation_matrix(&series, &sample_sizes));
    let (rank_correlations, rank_p_values) = ranked.unzip();
    Ok(CorrelationMatrix {
        columns: numeric_cols,
        correlations,
        p_values: Some(p_values),
        sample_sizes,
        rank_correlations,
        rank_p_values,
    })
}

/// Marks a row with no finite value in [`Ranked::ranks`].
const NO_RANK: u32 = u32::MAX;

/// One column's ranks, for Spearman's ρ.
struct Ranked {
    /// Twice each finite value's average rank (from 1) among the column's finite
    /// values, so a tie's half rank stays whole; [`NO_RANK`] where there is none.
    /// Equal values share a rank, so the ranks also tell ties apart.
    ranks: Vec<u32>,
    /// The rows with a finite value, in order of value.
    order: Vec<u32>,
    /// Every row has a finite value.
    complete: bool,
}

impl Ranked {
    fn new(series: &Series) -> Option<Self> {
        let rows = series.len();
        // Doubled ranks reach twice the rows.
        if rows >= (NO_RANK / 2) as usize {
            return None;
        }
        let mut values = Vec::with_capacity(rows);
        let mut row = 0u32;
        for_each_float(series, 0..rows, |v| {
            if let Some(v) = v.filter(|v| v.is_finite()) {
                values.push((v, row));
            }
            row += 1;
        });
        values.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
        let mut ranks = vec![NO_RANK; rows];
        let mut start = 0;
        while start < values.len() {
            // `==` so that -0 and 0 tie, which total_cmp sorts side by side.
            let end = start
                + values[start..]
                    .iter()
                    .take_while(|(v, _)| *v == values[start].0)
                    .count();
            let doubled = (start + 1 + end) as u32;
            for &(_, row) in &values[start..end] {
                ranks[row as usize] = doubled;
            }
            start = end;
        }
        Some(Self {
            complete: values.len() == rows,
            order: values.into_iter().map(|(_, row)| row).collect(),
            ranks,
        })
    }

    /// `out` becomes this column's doubled ranks among the rows where `other` also
    /// has a value, [`NO_RANK`] elsewhere; the number of those rows is returned.
    /// Walking the rows in order of value, a run of equal global ranks is a run of
    /// equal values. `kept` is scratch, reused from pair to pair.
    fn ranks_beside(&self, other: &Ranked, out: &mut Vec<u32>, kept: &mut Vec<u32>) -> usize {
        out.clear();
        out.resize(self.ranks.len(), NO_RANK);
        kept.clear();
        kept.extend(
            self.order
                .iter()
                .copied()
                .filter(|&row| other.ranks[row as usize] != NO_RANK),
        );
        let mut start = 0;
        while start < kept.len() {
            let tie = self.ranks[kept[start] as usize];
            let end = start
                + kept[start..]
                    .iter()
                    .take_while(|&&row| self.ranks[row as usize] == tie)
                    .count();
            let doubled = (start + 1 + end) as u32;
            for &row in &kept[start..end] {
                out[row as usize] = doubled;
            }
            start = end;
        }
        kept.len()
    }
}

/// Spearman's ρ for every pair of `series`, with its p-values: Pearson's r of the
/// ranks, each pair ranked over the rows where both hold a finite value, as Pearson
/// pairs them. Where neither column misses a value the column's own ranks are the
/// pair's, and no pair needs ranking again. Holds four bytes of rank and four of
/// order per value, about what the sample's own values take.
fn rank_correlation_matrix(
    series: &[&Series],
    sample_sizes: &[Vec<usize>],
) -> (Vec<Vec<f64>>, Vec<Vec<f64>>) {
    let n = series.len();
    let mut ranked: Vec<Option<Ranked>> = (0..n).map(|_| None).collect();
    across_threads(
        series.iter().zip(ranked.iter_mut()).collect(),
        |(series, ranked)| *ranked = Ranked::new(series),
    );
    let mut rows: Vec<Vec<f64>> = (0..n).map(|i| vec![f64::NAN; n - i - 1]).collect();
    let ranked = &ranked;
    across_threads(rows.iter_mut().enumerate().collect(), |(i, row)| {
        // Ranks as doubled u32s, half the size of floats, held once per row of the
        // matrix rather than once per pair.
        let (mut a, mut b, mut kept) = (Vec::new(), Vec::new(), Vec::new());
        for (k, rho) in row.iter_mut().enumerate() {
            let (Some(x), Some(y)) = (&ranked[i], &ranked[i + 1 + k]) else {
                continue;
            };
            let mut sums = PairSums::default();
            if x.complete && y.complete {
                // Ranks centered on their mean, which is the row count plus one.
                let mean = (x.ranks.len() + 1) as f64;
                for (&rx, &ry) in x.ranks.iter().zip(&y.ranks) {
                    sums.add(rx as f64 - mean, ry as f64 - mean);
                }
            } else {
                let pairs = x.ranks_beside(y, &mut a, &mut kept);
                y.ranks_beside(x, &mut b, &mut kept);
                let mean = (pairs + 1) as f64;
                for (&rx, &ry) in a.iter().zip(&b) {
                    if rx != NO_RANK {
                        sums.add(rx as f64 - mean, ry as f64 - mean);
                    }
                }
            }
            if sums.count >= 3 {
                *rho = sums.correlation();
            }
        }
    });
    let mut rho = vec![vec![1.0; n]; n];
    let mut p_values = vec![vec![0.0; n]; n];
    for (i, row) in rows.iter().enumerate() {
        for (k, &r) in row.iter().enumerate() {
            let j = i + 1 + k;
            rho[i][j] = r;
            rho[j][i] = r;
            if !r.is_nan() {
                // The same t approximation as Pearson's, over the same pairs.
                let p = compute_correlation_p_value(r, sample_sizes[i][j]);
                p_values[i][j] = p;
                p_values[j][i] = p;
            }
        }
    }
    (rho, p_values)
}

/// Runs `work` on every item, the items dealt out across threads in turn. A worker's
/// panic is raised again here rather than leaving its items undone.
fn across_threads<T: Send>(items: Vec<T>, work: impl Fn(T) + Sync) {
    let threads = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(items.len())
        .max(1);
    let mut shares: Vec<Vec<T>> = (0..threads).map(|_| Vec::new()).collect();
    for (k, item) in items.into_iter().enumerate() {
        shares[k % threads].push(item);
    }
    let work = &work;
    std::thread::scope(|scope| {
        let handles: Vec<_> = shares
            .into_iter()
            .map(|share| scope.spawn(move || share.into_iter().for_each(work)))
            .collect();
        for handle in handles {
            if let Err(panic) = handle.join() {
                std::panic::resume_unwind(panic);
            }
        }
    });
}

/// `len` values of `series` from `start`, as floats.
fn float_piece(series: &Series, start: usize, len: usize) -> Option<Float64Chunked> {
    let piece = series
        .slice(start as i64, len)
        .cast(&DataType::Float64)
        .ok()?;
    piece.f64().ok().cloned()
}

/// The values of `series` in `rows` as floats, None where null, cast [`CAST_ROWS`]
/// at a time.
fn for_each_float(series: &Series, rows: Range<usize>, mut f: impl FnMut(Option<f64>)) {
    for start in rows.clone().step_by(CAST_ROWS) {
        let len = CAST_ROWS.min(rows.end - start);
        match float_piece(series, start, len) {
            Some(floats) => floats.iter().for_each(&mut f),
            None => (0..len).for_each(|_| f(None)),
        }
    }
}

/// A numeric column's mean over its finite values, taken off each value before it
/// is correlated: centered, the one-pass sums below stay exact enough.
#[derive(Clone, Copy, Default)]
struct Shift {
    mean: f64,
    /// No value is missing, so every row pairs.
    complete: bool,
}

impl Shift {
    fn new(series: &Series) -> Self {
        let (mut sum, mut count) = (0.0, 0usize);
        for_each_float(series, 0..series.len(), |v| {
            if let Some(v) = v.filter(|v| v.is_finite()) {
                sum += v;
                count += 1;
            }
        });
        Self {
            mean: if count > 0 { sum / count as f64 } else { 0.0 },
            complete: count == series.len(),
        }
    }

    /// `values` becomes the column's `rows` less the mean, NaN where a value is null
    /// or not finite.
    fn fill(&self, series: &Series, rows: Range<usize>, values: &mut Vec<f64>) {
        values.clear();
        for_each_float(series, rows, |v| {
            values.push(
                v.filter(|v| v.is_finite())
                    .map_or(f64::NAN, |v| v - self.mean),
            );
        });
    }
}

/// One pass of sums over paired values, each less a shift near its mean. The
/// spreads about the pairs' own means follow exactly whatever the shift; a shift
/// near the mean keeps them clear of rounding.
#[derive(Clone, Copy, Default)]
struct PairSums {
    count: usize,
    x: f64,
    y: f64,
    xx: f64,
    yy: f64,
    xy: f64,
}

impl PairSums {
    fn add(&mut self, v1: f64, v2: f64) {
        self.count += 1;
        self.x += v1;
        self.y += v2;
        self.xx += v1 * v1;
        self.yy += v2 * v2;
        self.xy += v1 * v2;
    }

    /// Adds the rows where both centered columns have a value: with `both` complete,
    /// every row.
    fn add_pairs(&mut self, a: &[f64], b: &[f64], both: bool) {
        // Summed in a copy, which the loop can keep in registers.
        let mut sums = *self;
        for (&v1, &v2) in a.iter().zip(b) {
            if !both && (v1.is_nan() || v2.is_nan()) {
                continue;
            }
            sums.add(v1, v2);
        }
        *self = sums;
    }

    /// The sums of squares and of products about the pairs' means.
    fn spreads(&self) -> (f64, f64, f64) {
        let n = self.count as f64;
        (
            self.xx - self.x * self.x / n,
            self.yy - self.y * self.y / n,
            self.xy - self.x * self.y / n,
        )
    }

    /// NaN for fewer than two pairs or a column of one value.
    fn correlation(&self) -> f64 {
        let (sxx, syy, sxy) = self.spreads();
        // Measured against the sums of squares: what a column with one value leaves
        // behind is rounding, not spread.
        if self.count < 2 || sxx <= self.xx * 1e-12 || syy <= self.yy * 1e-12 {
            return f64::NAN;
        }
        (sxy / (sxx * syy).sqrt()).clamp(-1.0, 1.0)
    }
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
    crate::distribution_fit::beta_inc(df / 2.0, 0.5, 1.0 - correlation * correlation)
        .clamp(0.0, 1.0)
}

/// Computes correlation statistics for a pair of columns.
///
/// Returns Pearson correlation coefficient, p-value, covariance, and sample size,
/// over the rows where both columns hold a finite value. Requires at least 3 such
/// rows. Two passes over the columns as they are, a rough mean first and then the
/// sums about it, as the matrix's: no list of the pairs is built.
pub fn compute_correlation_pair(
    df: &DataFrame,
    col1_name: &str,
    col2_name: &str,
) -> Result<CorrelationPair> {
    let series1 = df.column(col1_name)?.as_materialized_series();
    let series2 = df.column(col2_name)?.as_materialized_series();

    let mut sample_size = 0usize;
    let (mut sum1, mut sum2) = (0.0, 0.0);
    let (mut min1, mut max1, mut min2, mut max2) = (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
    for_each_finite_pair(series1, series2, |v1, v2| {
        sample_size += 1;
        sum1 += v1;
        sum2 += v2;
        (min1, max1) = (min1.min(v1), max1.max(v1));
        (min2, max2) = (min2.min(v2), max2.max(v2));
    });
    if sample_size < 3 {
        return Err(color_eyre::eyre::eyre!("Not enough data for correlation"));
    }
    let n = sample_size as f64;
    let (shift1, shift2) = (sum1 / n, sum2 / n);
    let mut sums = PairSums::default();
    for_each_finite_pair(series1, series2, |v1, v2| {
        sums.add(v1 - shift1, v2 - shift2)
    });
    let (sxx, syy, sxy) = sums.spreads();

    // A column with one value has no correlation with anything: undefined, not 0,
    // which reads as a finding.
    let correlation = sums.correlation();
    let p_value = Some(compute_correlation_p_value(correlation, sample_size));
    // Rounding can leave a constant's sum of squares a hair below zero.
    let stats = |mean: f64, squares: f64, min: f64, max: f64| ColumnStats {
        mean,
        std: (squares.max(0.0) / (n - 1.0)).sqrt(),
        min,
        max,
    };

    Ok(CorrelationPair {
        column1: col1_name.to_string(),
        column2: col2_name.to_string(),
        correlation,
        p_value,
        sample_size,
        covariance: sxy / (n - 1.0),
        r_squared: correlation * correlation,
        stats1: stats(shift1 + sums.x / n, sxx, min1, max1),
        stats2: stats(shift2 + sums.y / n, syy, min2, max2),
    })
}

/// The rows where both columns hold a finite value, in order, cast [`CAST_ROWS`] at
/// a time.
fn for_each_finite_pair(a: &Series, b: &Series, mut f: impl FnMut(f64, f64)) {
    let rows = a.len().min(b.len());
    for start in (0..rows).step_by(CAST_ROWS) {
        let len = CAST_ROWS.min(rows - start);
        let (Some(a), Some(b)) = (float_piece(a, start, len), float_piece(b, start, len)) else {
            continue;
        };
        for pair in a.iter().zip(b.iter()) {
            if let (Some(v1), Some(v2)) = pair
                && v1.is_finite()
                && v2.is_finite()
            {
                f(v1, v2);
            }
        }
    }
}

#[cfg(test)]
mod sampling_tests;

#[cfg(test)]
mod normality_tests;

#[cfg(test)]
pub(crate) mod describe_tests;

#[cfg(all(test, feature = "streaming"))]
mod streaming_guard_tests;

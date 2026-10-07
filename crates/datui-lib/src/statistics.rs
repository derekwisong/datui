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
    /// The histogram last drawn, kept for the next frame; see [`Self::histogram`].
    pub histogram: HistogramCache,
}

/// What a histogram of [`DistributionAnalysis::sorted_sample_values`] is drawn for:
/// one family's fit, a number of bins over a range of values, and the scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistogramKey {
    pub family: DistributionType,
    pub bins: usize,
    /// Bins equal in log space, the curve at each bin's geometric middle.
    pub log: bool,
    /// The values the bins span, positive on a log scale.
    pub range: (f64, f64),
    /// Points along a continuous family's density on linear bins.
    pub samples: usize,
}

/// A histogram and its family's expected counts, ready to draw.
#[derive(Debug)]
pub struct Histogram {
    /// Values in each bin.
    pub counts: Vec<usize>,
    /// The count axis's top: the tallest bar or expected count, rounded up to even
    /// so the middle label is a whole count.
    pub top: f64,
    /// The fit's expected counts as a curve, x where the axis puts each point and y
    /// on the 0-100 scale the bars stand on. Empty when the family does not apply.
    pub curve: Vec<(f64, f64)>,
}

/// The last [`Histogram`] built for an analysis. A copy starts empty.
#[derive(Debug, Default)]
pub struct HistogramCache(std::sync::Mutex<Option<(HistogramKey, std::sync::Arc<Histogram>)>>);

impl Clone for HistogramCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[cfg(test)]
thread_local! {
    /// Histograms this thread has built, for the tests that count them.
    pub(crate) static HISTOGRAMS_BUILT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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

    /// The histogram for `key`: the one last built when the key is the same, so a
    /// frame that changes nothing counts nothing and evaluates no CDF.
    pub fn histogram(&self, key: HistogramKey) -> std::sync::Arc<Histogram> {
        let mut cache = self
            .histogram
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((cached, histogram)) = cache.as_ref()
            && *cached == key
        {
            return std::sync::Arc::clone(histogram);
        }
        #[cfg(test)]
        HISTOGRAMS_BUILT.with(|built| built.set(built.get() + 1));
        let histogram = std::sync::Arc::new(self.build_histogram(key));
        *cache = Some((key, std::sync::Arc::clone(&histogram)));
        histogram
    }

    fn build_histogram(&self, key: HistogramKey) -> Histogram {
        let HistogramKey {
            family,
            bins,
            log,
            range: (low, high),
            samples,
        } = key;
        let sorted = &self.sorted_sample_values;
        let n = sorted.len() as f64;
        let edges: Vec<f64> = if log {
            let (log_low, log_high) = (low.ln(), high.ln());
            let width = (log_high - log_low) / bins as f64;
            (0..=bins)
                .map(|i| (log_low + i as f64 * width).exp())
                .collect()
        } else {
            let width = (high - low) / bins as f64;
            (0..=bins).map(|i| low + i as f64 * width).collect()
        };
        // Sorted, so a bin's count is the distance between where its edges fall:
        // each bin holds its lower edge, the last its upper edge too.
        let below = |edge: f64| sorted.partition_point(|value| *value < edge);
        let counts: Vec<usize> = (0..bins)
            .map(|i| {
                let end = if i + 1 == bins {
                    sorted.partition_point(|value| *value <= edges[i + 1])
                } else {
                    below(edges[i + 1])
                };
                end.saturating_sub(below(edges[i]))
            })
            .collect();

        // Expected counts from the fit every view of this family uses, by the CDF
        // across each bin: exact for log-scaled and whole-number bins, where a density
        // at the center is not.
        let fitted = self
            .fit(family)
            .and_then(|outcome| outcome.test())
            .map(|test| &test.fitted);
        let expected: Vec<f64> = match fitted {
            Some(fitted) => edges
                .windows(2)
                .enumerate()
                .map(|(i, edge)| {
                    let upper = if i + 1 == bins {
                        fitted.cdf(edge[1])
                    } else {
                        fitted.cdf_below(edge[1])
                    };
                    (upper - fitted.cdf_below(edge[0])).max(0.0) * n
                })
                .collect(),
            None => vec![0.0; bins],
        };
        let tallest = counts.iter().copied().max().unwrap_or(0);
        let expected_top = expected.iter().copied().fold(0.0, f64::max);
        let top = (tallest.max(expected_top.ceil() as usize).max(1) as f64 / 2.0).ceil() * 2.0;
        let height = |count: f64| count / top * 100.0;

        let curve = match fitted {
            // A continuous family on linear bins is drawn as its density, scaled to a
            // bin's count: a smooth curve rather than a staircase.
            Some(fitted) if !fitted.discrete() && !log && high > low => {
                let bin_width = (high - low) / bins as f64;
                (0..samples)
                    .map(|i| {
                        let x = low + i as f64 / (samples - 1) as f64 * (high - low);
                        (x, height(fitted.density(x) * bin_width * n))
                    })
                    .filter(|(_, y)| y.is_finite())
                    .collect()
            }
            // Counts, and log-scaled bins, by each bin's expected count at its center:
            // on Log a position is the log of the value, and a center the geometric
            // middle.
            Some(_) => edges
                .windows(2)
                .zip(&expected)
                .map(|(edge, count)| {
                    let center = if log {
                        (edge[0] * edge[1]).sqrt().ln()
                    } else {
                        (edge[0] + edge[1]) / 2.0
                    };
                    (center, height(*count))
                })
                .collect(),
            None => Vec::new(),
        };
        Histogram { counts, top, curve }
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
/// see [`crate::sampling::analysis_rows`]. `None` reads every row.
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

/// Describe statistics for a frame. With `sample_size`, a table with more rows than
/// that is described from a sample (see [`crate::sampling::analysis_rows`]); without it, every row is
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
        None => crate::sampling::count_rows(lf, polars_streaming)?,
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
        histogram: HistogramCache::default(),
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
mod tests;

#[cfg(test)]
mod normality_tests;

#[cfg(test)]
pub(crate) mod describe_tests;

#[cfg(all(test, feature = "streaming"))]
mod streaming_guard_tests;

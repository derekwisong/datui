use color_eyre::Result;
use polars::prelude::*;
use std::collections::HashMap;

// Sampling threshold: datasets >= this size will be sampled
pub const SAMPLING_THRESHOLD: usize = 10_000;

pub struct ColumnStatistics {
    pub name: String,
    pub dtype: DataType,
    pub count: usize,
    pub null_count: usize,
    pub numeric_stats: Option<NumericStatistics>,
    pub categorical_stats: Option<CategoricalStatistics>,
    pub distribution_info: Option<DistributionInfo>,
}

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

pub struct CategoricalStatistics {
    pub unique_count: usize,
    pub mode: Option<String>,
    pub top_values: Vec<(String, usize)>,
    pub min: Option<String>, // Lexicographically smallest string
    pub max: Option<String>, // Lexicographically largest string
}

pub struct DistributionInfo {
    pub distribution_type: DistributionType,
    pub confidence: f64,
    pub sample_size: usize,
    pub is_sampled: bool,
    pub fit_quality: Option<f64>, // 0.0-1.0, how well data fits detected type
}

// Advanced distribution analysis structures (Phase 6)
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DistributionType {
    #[default]
    Normal,
    LogNormal,
    Uniform,
    PowerLaw,
    Exponential,
    Unknown,
}

impl std::fmt::Display for DistributionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionType::Normal => write!(f, "Normal"),
            DistributionType::LogNormal => write!(f, "LogNormal"),
            DistributionType::Uniform => write!(f, "Uniform"),
            DistributionType::PowerLaw => write!(f, "PowerLaw"),
            DistributionType::Exponential => write!(f, "Exponential"),
            DistributionType::Unknown => write!(f, "Unknown"),
        }
    }
}

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

pub fn compute_statistics(
    lf: &LazyFrame,
    sample_size: Option<usize>,
    seed: u64,
) -> Result<AnalysisResults> {
    // Collect schema first
    let schema = lf.clone().collect_schema()?;
    let total_rows = {
        let count_df = lf.clone().select([len()]).collect()?;
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

    // Determine if we need to sample
    let sample_size = sample_size.unwrap_or(SAMPLING_THRESHOLD);
    let should_sample = total_rows > sample_size;
    let actual_sample_size = if should_sample {
        Some(sample_size)
    } else {
        None
    };

    // Collect data (with sampling if needed)
    let df = if should_sample {
        sample_dataframe(lf, sample_size, seed)?
    } else {
        lf.clone().collect()?
    };

    let mut column_statistics = Vec::new();

    for (name, dtype) in schema.iter() {
        // Get column and convert to Series using as_materialized_series()
        let col = df.column(name)?;
        let series = col.as_materialized_series();
        let count = series.len();
        let null_count = series.null_count();

        let numeric_stats = if is_numeric_type(dtype) {
            Some(compute_numeric_stats(series)?)
        } else {
            None
        };

        let categorical_stats = if is_categorical_type(dtype) {
            Some(compute_categorical_stats(series)?)
        } else {
            None
        };

        let distribution_info = if is_numeric_type(dtype) && null_count < count {
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

    // Compute advanced distribution analyses for numeric columns
    let distribution_analyses: Vec<DistributionAnalysis> = column_statistics
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
        .collect();

    // Compute correlation matrix for numeric columns
    let correlation_matrix = compute_correlation_matrix(&df).ok();

    Ok(AnalysisResults {
        column_statistics,
        total_rows,
        sample_size: actual_sample_size,
        sample_seed: seed,
        correlation_matrix,
        distribution_analyses,
    })
}

fn is_numeric_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn is_categorical_type(dtype: &DataType) -> bool {
    matches!(dtype, DataType::String | DataType::Categorical(..))
}

fn sample_dataframe(lf: &LazyFrame, sample_size: usize, seed: u64) -> Result<DataFrame> {
    // Use hash-based sampling for reproducibility
    let df = lf.clone().collect()?;
    let total_rows = df.height();

    if total_rows <= sample_size {
        return Ok(df);
    }

    // Use deterministic sampling based on seed
    // Take every nth row based on seed
    let step = total_rows / sample_size;
    let start = (seed as usize) % step;

    let indices: Vec<usize> = (start..total_rows)
        .step_by(step)
        .take(sample_size)
        .collect();

    // Use Polars' take operation with indices
    let indices_ca = UInt32Chunked::new(
        "indices".into(),
        indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
    );
    df.take(&indices_ca)
        .map_err(|e| color_eyre::eyre::eyre!("Sampling error: {}", e))
}

// Helper function to convert numeric series to Vec<f64>, handling both integer and float types
fn get_numeric_values_as_f64(series: &Series) -> Vec<f64> {
    if let Ok(f64_series) = series.f64() {
        f64_series.iter().flatten().collect()
    } else if let Ok(i64_series) = series.i64() {
        i64_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .collect()
    } else if let Ok(i32_series) = series.i32() {
        i32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .collect()
    } else if let Ok(u64_series) = series.u64() {
        u64_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .collect()
    } else if let Ok(u32_series) = series.u32() {
        u32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .collect()
    } else if let Ok(f32_series) = series.f32() {
        f32_series
            .iter()
            .filter_map(|v| v.map(|x| x as f64))
            .collect()
    } else {
        // Try to cast to f64 as last resort - collect into owned Series first
        match series.cast(&DataType::Float64) {
            Ok(cast_series) => {
                if let Ok(f64_series) = cast_series.f64() {
                    f64_series.iter().flatten().collect()
                } else {
                    Vec::new()
                }
            }
            Err(_) => Vec::new(),
        }
    }
}

fn compute_numeric_stats(series: &Series) -> Result<NumericStatistics> {
    let mean = series.mean().unwrap_or(f64::NAN);
    let std = series.std(1).unwrap_or(f64::NAN); // Sample std (ddof=1)

    // Get min/max - handle both integer and float types
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

    // Compute percentiles - handle both integer and float types
    let mut percentiles = HashMap::new();
    let values: Vec<f64> = get_numeric_values_as_f64(series);

    if !values.is_empty() {
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
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

    // Compute skewness and kurtosis
    let skewness = compute_skewness(series);
    let kurtosis = compute_kurtosis(series);

    // Detect outliers
    let (outliers_iqr, outliers_zscore) = detect_outliers(series, q25, q75, median, mean, std);

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

    // Get values as f64, handling both integer and float types
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

    // Get values as f64, handling both integer and float types
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
    // value_counts signature: (sort: bool, parallel: bool, name: PlSmallStr, multithreaded: bool)
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

    // Get top 10 values
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

    // Get min and max for string columns (lexicographic order)
    // Polars Rust doesn't have direct min/max on Utf8Chunked, so we iterate
    let min = if let Ok(str_series) = series.str() {
        // Iterate through non-null values and find minimum lexicographically
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
        // Iterate through non-null values and find maximum lexicographically
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
        };
    }

    // Convert to f64 for analysis - handle both integer and float types
    let values: Vec<f64> = get_numeric_values_as_f64(sample);

    if values.is_empty() {
        return DistributionInfo {
            distribution_type: DistributionType::Unknown,
            confidence: 0.0,
            sample_size,
            is_sampled,
            fit_quality: None,
        };
    }

    // Compute mean and std for fit quality calculations
    let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
    let variance: f64 =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    let std = variance.sqrt();

    // Test all distributions and calculate fit quality for each
    // Store candidates as (distribution_type, fit_quality, confidence)
    let mut candidates: Vec<(DistributionType, f64, f64)> = Vec::new();

    // Test Normal distribution
    let (_, sw_pvalue) = approximate_shapiro_wilk(&values);
    let skewness = compute_skewness(sample);
    let kurtosis = compute_kurtosis(sample);
    let sw_pvalue_val = sw_pvalue.unwrap_or(0.0);
    let normal_fit = calculate_normal_fit_quality(&values, mean, std);
    // Calculate confidence based on Shapiro-Wilk p-value, skewness, and kurtosis
    let normal_confidence =
        if sw_pvalue_val > 0.05 && skewness.abs() < 0.5 && (kurtosis - 3.0).abs() < 0.5 {
            sw_pvalue_val.min(0.95)
        } else {
            // Lower confidence if basic tests fail, but still consider fit quality
            // Use fit quality as a fallback to ensure confidence > 0 if fit > 0
            (sw_pvalue_val * 0.5 + normal_fit * 0.3)
                .min(0.70)
                .max(normal_fit * 0.1)
        };
    candidates.push((DistributionType::Normal, normal_fit, normal_confidence));

    // Test LogNormal distribution (requires positive values)
    let log_values: Vec<f64> = values
        .iter()
        .filter(|&&v| v > 0.0)
        .map(|v| v.ln())
        .collect();
    if log_values.len() > 3 {
        let (_log_sw_stat, log_sw_pvalue) = approximate_shapiro_wilk(&log_values);
        let lognormal_fit = calculate_lognormal_fit_quality(&values);
        let lognormal_confidence = if let Some(log_pval) = log_sw_pvalue {
            if log_pval > 0.05 {
                log_pval.min(0.90)
            } else {
                // Lower confidence but use fit quality as fallback
                (log_pval * 0.5 + lognormal_fit * 0.3)
                    .min(0.70)
                    .max(lognormal_fit * 0.1)
            }
        } else {
            // Fallback to fit quality scaled down, but ensure > 0 if fit > 0
            lognormal_fit * 0.8
        };
        candidates.push((
            DistributionType::LogNormal,
            lognormal_fit,
            lognormal_confidence,
        ));
    }

    // Test Uniform distribution
    let uniformity_score = chi_square_uniformity_test(&values);
    let uniform_fit = calculate_uniform_fit_quality(&values);
    let uniform_confidence = uniformity_score.min(0.90);
    candidates.push((DistributionType::Uniform, uniform_fit, uniform_confidence));

    // Test PowerLaw distribution (requires positive values)
    let power_law_score = test_power_law(&values);
    let powerlaw_fit = calculate_power_law_fit_quality(&values);
    let powerlaw_confidence = power_law_score.min(0.85);
    if power_law_score > 0.0 {
        // Only add if there are positive values to test
        candidates.push((
            DistributionType::PowerLaw,
            powerlaw_fit,
            powerlaw_confidence,
        ));
    }

    // Test Exponential distribution (requires positive values)
    let exp_score = test_exponential(&values);
    let exp_fit = calculate_exponential_fit_quality(&values);
    let exp_confidence = exp_score.min(0.85);
    if exp_score > 0.0 {
        // Only add if there are positive values to test
        candidates.push((DistributionType::Exponential, exp_fit, exp_confidence));
    }

    // Select the distribution with the best combined score
    // Use weighted combination: fit_quality * 0.6 + confidence * 0.4
    // This ensures confidence is always considered, not just as a tiebreaker
    if let Some(best) = candidates.iter().max_by(|a, b| {
        // Calculate combined score: fit quality weighted 60%, confidence weighted 40%
        let score_a = a.1 * 0.6 + a.2 * 0.4;
        let score_b = b.1 * 0.6 + b.2 * 0.4;
        let score_cmp = score_a
            .partial_cmp(&score_b)
            .unwrap_or(std::cmp::Ordering::Equal);

        // If scores are very close (within 0.01), use a two-level comparison:
        // First by fit quality, then by confidence
        if score_cmp == std::cmp::Ordering::Equal || (score_a - score_b).abs() < 0.01 {
            // Primary: fit quality
            let fit_cmp = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
            if fit_cmp != std::cmp::Ordering::Equal {
                return fit_cmp;
            }
            // Secondary: confidence (if fit qualities are very close or equal)
            a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            score_cmp
        }
    }) {
        DistributionInfo {
            distribution_type: best.0,
            confidence: best.2,
            sample_size,
            is_sampled,
            fit_quality: Some(best.1),
        }
    } else {
        // Fallback to Unknown if no candidates were found
        DistributionInfo {
            distribution_type: DistributionType::Unknown,
            confidence: 0.50,
            sample_size,
            is_sampled,
            fit_quality: Some(0.5),
        }
    }
}

fn approximate_shapiro_wilk(values: &[f64]) -> (Option<f64>, Option<f64>) {
    // Simplified Shapiro-Wilk approximation
    // For small samples, use a basic normality test
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

    // Compute skewness and kurtosis
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

    // Approximate p-value based on how close to normal
    let skew_penalty = skewness.abs() / 2.0;
    let kurt_penalty = (kurtosis - 3.0).abs() / 2.0;
    let pvalue = (1.0 - skew_penalty.min(1.0) - kurt_penalty.min(1.0)).max(0.0);

    // Approximate statistic (not used but included for completeness)
    let sw_stat = pvalue;

    (Some(sw_stat), Some(pvalue))
}

fn chi_square_uniformity_test(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 10 {
        return 0.0;
    }

    let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let range = max - min;

    if range == 0.0 {
        return 0.0;
    }

    // Divide into 10 bins
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

    // Approximate p-value (higher chi-square = lower p-value)
    // For 9 degrees of freedom, approximate
    (-chi_square / 20.0).exp().clamp(0.0, 1.0)
}

fn test_power_law(values: &[f64]) -> f64 {
    // Basic power law test: check if log-log plot is approximately linear
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    let log_values: Vec<f64> = positive_values.iter().map(|v| v.ln()).collect();
    let sorted_log: Vec<f64> = {
        let mut v = log_values.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v
    };

    // Compute correlation of log(x) vs log(rank)
    let n = sorted_log.len();
    let ranks: Vec<f64> = (1..=n).map(|i| (i as f64).ln()).collect();

    let mean_log = sorted_log.iter().sum::<f64>() / n as f64;
    let mean_rank = ranks.iter().sum::<f64>() / n as f64;

    let numerator: f64 = sorted_log
        .iter()
        .zip(ranks.iter())
        .map(|(x, r)| (x - mean_log) * (r - mean_rank))
        .sum();

    let denom_x: f64 = sorted_log.iter().map(|x| (x - mean_log).powi(2)).sum();
    let denom_r: f64 = ranks.iter().map(|r| (r - mean_rank).powi(2)).sum();

    if denom_x == 0.0 || denom_r == 0.0 {
        return 0.0;
    }

    let correlation = numerator / (denom_x.sqrt() * denom_r.sqrt());
    correlation.abs()
}

fn test_exponential(values: &[f64]) -> f64 {
    // Basic exponential test: check exponential decay pattern
    let positive_values: Vec<f64> = values.iter().filter(|&&v| v > 0.0).copied().collect();
    if positive_values.len() < 10 {
        return 0.0;
    }

    let sorted: Vec<f64> = {
        let mut v = positive_values.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v
    };

    // Check if log values are approximately linear
    let log_values: Vec<f64> = sorted.iter().map(|v| v.ln()).collect();
    let n = log_values.len();
    let x: Vec<f64> = (0..n).map(|i| i as f64).collect();

    let mean_x = x.iter().sum::<f64>() / n as f64;
    let mean_log = log_values.iter().sum::<f64>() / n as f64;

    let numerator: f64 = x
        .iter()
        .zip(log_values.iter())
        .map(|(xi, yi)| (xi - mean_x) * (yi - mean_log))
        .sum();

    let denom_x: f64 = x.iter().map(|xi| (xi - mean_x).powi(2)).sum();
    let denom_y: f64 = log_values.iter().map(|yi| (yi - mean_log).powi(2)).sum();

    if denom_x == 0.0 || denom_y == 0.0 {
        return 0.0;
    }

    let correlation = numerator / (denom_x.sqrt() * denom_y.sqrt());
    correlation.abs()
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
    let mut values: Vec<f64> = get_numeric_values_as_f64(series);

    // Sort values for Q-Q plot (all data if not sampled, or sampled data if >= threshold)
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sorted_sample_values = values.clone();
    let actual_sample_size = sorted_sample_values.len();

    // Compute distribution characteristics
    let (sw_stat, sw_pvalue) = if values.len() >= 3 {
        approximate_shapiro_wilk(&values)
    } else {
        (None, None)
    };

    let variance = numeric_stats.std * numeric_stats.std;
    let coefficient_of_variation = if numeric_stats.mean != 0.0 {
        numeric_stats.std / numeric_stats.mean.abs()
    } else {
        0.0
    };

    // Mode calculation (simplified - find most frequent value in binned data)
    let mode = compute_mode(&values);

    let characteristics = DistributionCharacteristics {
        shapiro_wilk_stat: sw_stat,
        shapiro_wilk_pvalue: sw_pvalue,
        skewness: numeric_stats.skewness,
        kurtosis: numeric_stats.kurtosis,
        mean: numeric_stats.mean,
        median: numeric_stats.median,
        std_dev: numeric_stats.std,
        variance,
        coefficient_of_variation,
        mode,
    };

    // Compute fit quality
    let fit_quality = dist_info.fit_quality.unwrap_or_else(|| {
        calculate_fit_quality(
            &values,
            dist_info.distribution_type,
            numeric_stats.mean,
            numeric_stats.std,
        )
    });

    // Compute outlier analysis with context
    let outliers = compute_outlier_analysis(series, numeric_stats);

    // Percentile breakdown
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
        DistributionType::Unknown => 0.5,
    }
}

fn calculate_normal_fit_quality(values: &[f64], mean: f64, std: f64) -> f64 {
    if values.is_empty() || std == 0.0 {
        return 0.0;
    }

    // Compare empirical percentiles to theoretical normal percentiles
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();

    let mut max_diff: f64 = 0.0;
    for (i, &val) in sorted.iter().enumerate() {
        let percentile = (i as f64) / (n - 1) as f64;
        // Theoretical normal percentile (using approximation)
        let theoretical = mean + std * normal_quantile(percentile);
        let diff = (val - theoretical).abs() / std;
        max_diff = max_diff.max(diff);
    }

    // Convert to quality score (0.0-1.0)
    (1.0 - (max_diff / 3.0).min(1.0)).max(0.0)
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
    let log_values: Vec<f64> = values
        .iter()
        .filter(|&&v| v > 0.0)
        .map(|v| v.ln())
        .collect();
    if log_values.len() < 3 {
        return 0.0;
    }

    let mean = log_values.iter().sum::<f64>() / log_values.len() as f64;
    let variance: f64 =
        log_values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (log_values.len() - 1) as f64;
    let std = variance.sqrt();

    calculate_normal_fit_quality(&log_values, mean, std)
}

fn calculate_uniform_fit_quality(values: &[f64]) -> f64 {
    chi_square_uniformity_test(values)
}

fn calculate_power_law_fit_quality(values: &[f64]) -> f64 {
    test_power_law(values)
}

fn calculate_exponential_fit_quality(values: &[f64]) -> f64 {
    test_exponential(values)
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

// Calculate theoretical bin probabilities using CDF for histogram
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

use color_eyre::Result;
use datui::distribution_fit::Fitted;
use datui::statistics::{
    ComputeOptions, DistributionType, compute_correlation_matrix, compute_correlation_pair,
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

/// Whole numbers spread over millions are counts to the Poisson and geometric fits,
/// whose draws once cost time in proportion to the values: 2,000 of them ran for
/// minutes. They finish, and the uniform they came from is the answer.
#[test]
fn distribution_of_a_wide_integer_range_finishes() -> Result<()> {
    let mut rng = datui::distribution_fit::Rng::new(442);
    let values: Vec<i64> = (0..2_000)
        .map(|_| (rng.next_u64() % 2_000_000) as i64)
        .collect();
    let lf = df!("x" => values)?.lazy();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(compute_statistics_with_options(
            &lf,
            None,
            0,
            distribution_options(),
        ));
    });
    // Seconds in a debug build; generous for a loaded CI machine.
    let results = receiver
        .recv_timeout(std::time::Duration::from_secs(120))
        .expect("distribution analysis of 2,000 integers did not finish")?;

    let dist = &results.distribution_analyses[0];
    assert_eq!(dist.distribution_type, DistributionType::Uniform);
    let uniform = dist.fit(DistributionType::Uniform).unwrap().test().unwrap();
    assert!(uniform.p_value >= 0.01, "{uniform:?}");
    let Fitted::Uniform { low, high } = uniform.fitted else {
        panic!("{uniform:?}");
    };
    assert!(low < 5_000.0 && high > 1_995_000.0, "{low} to {high}");
    for family in [DistributionType::Poisson, DistributionType::Geometric] {
        let test = dist.fit(family).unwrap().test().unwrap();
        assert!(test.p_value < 0.01, "{family:?} holds: {test:?}");
        let qq = dist.qq(family).unwrap();
        assert_eq!(qq.len(), 2_000);
        assert!(qq.windows(2).all(|pair| pair[0] <= pair[1]), "{family:?}");
    }
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

/// Counts the allocations as large as a column of `ROW_BYTES` floats, up to twice
/// that (a list grown by doubling), while `ROW_BYTES` is set.
struct RowSizedCount;

static ROW_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static ROW_SIZED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl RowSizedCount {
    fn note(size: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let row = ROW_BYTES.load(Relaxed);
        if row > 0 && (row..=2 * row + 64).contains(&size) {
            ROW_SIZED.fetch_add(1, Relaxed);
        }
    }

    /// The row-sized allocations `f` makes over `rows` rows, the fewest of three runs:
    /// the count is process-wide, and another test in this binary may allocate a
    /// block that size meanwhile.
    fn during(rows: usize, mut f: impl FnMut()) -> usize {
        use std::sync::atomic::Ordering::Relaxed;
        static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
        (0..3)
            .map(|_| {
                let before = ROW_SIZED.load(Relaxed);
                ROW_BYTES.store(rows * 8, Relaxed);
                f();
                ROW_BYTES.store(0, Relaxed);
                ROW_SIZED.load(Relaxed) - before
            })
            .min()
            .unwrap_or_default()
    }
}

unsafe impl std::alloc::GlobalAlloc for RowSizedCount {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::note(layout.size());
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::note(layout.size());
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new: usize) -> *mut u8 {
        Self::note(new);
        unsafe { std::alloc::System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: RowSizedCount = RowSizedCount;

/// Every pair of a correlation matrix is one pass over two columns converted once:
/// a column's floats are allocated once for Pearson and once for Spearman's ranks,
/// never once per pair, and no cast is the size of a column. 24 columns are 276
/// pairs; a filtered copy of each pair's rows, as the matrix once made, would be
/// hundreds of allocations the size of a column.
#[test]
fn correlation_allocates_per_column_not_per_pair() -> Result<()> {
    let rows = 50_003;
    let columns: Vec<Column> = (0..24)
        .map(|c| {
            let name = format!("c{c}");
            if c % 2 == 0 {
                let v: Vec<Option<i64>> = (0..rows)
                    .map(|r| ((r + c) % 37 != 0).then_some(((r * (c + 3)) % 1009) as i64))
                    .collect();
                Series::new(name.into(), v).into()
            } else {
                let v: Vec<Option<f64>> = (0..rows)
                    .map(|r| ((r * 7 + c) % 41 != 0).then_some(((r as f64) * 0.37).sin()))
                    .collect();
                Series::new(name.into(), v).into()
            }
        })
        .collect();
    let df = DataFrame::new(rows, columns)?;

    let mut matrix = None;
    let allocated = RowSizedCount::during(rows, || {
        matrix = Some(compute_correlation_matrix(&df));
    });
    let matrix = matrix.unwrap()?;
    assert_eq!(matrix.columns.len(), 24);
    assert!(allocated <= 2 * 24, "{allocated} column-sized allocations");

    // A pair reads its two columns where they are, cast a piece at a time.
    let mut pair = None;
    let allocated = RowSizedCount::during(rows, || {
        pair = Some(compute_correlation_pair(&df, "c0", "c1"));
    });
    let pair = pair.unwrap()?;
    assert_eq!(allocated, 0, "column-sized allocations");
    assert!((pair.correlation - matrix.correlations[0][1]).abs() < 1e-12);
    Ok(())
}

/// Pearson r over the rows where both values are finite, filtered first: the
/// corrected two-pass sums (Chan, Golub and LeVeque), deviations from the means
/// less what rounding left in their sums. A plain two-pass is not a reference over
/// a large offset: a mean on the offset's coarse grid adds n·d² to each sum of
/// squares.
fn reference_pearson(a: &[Option<f64>], b: &[Option<f64>]) -> (f64, usize) {
    let pairs: Vec<(f64, f64)> = a
        .iter()
        .zip(b)
        .filter_map(|p| match p {
            (Some(x), Some(y)) if x.is_finite() && y.is_finite() => Some((*x, *y)),
            _ => None,
        })
        .collect();
    let n = pairs.len();
    if n < 3 {
        return (f64::NAN, n);
    }
    let count = n as f64;
    let mx = pairs.iter().map(|p| p.0).sum::<f64>() / count;
    let my = pairs.iter().map(|p| p.1).sum::<f64>() / count;
    let (mut dx, mut dy, mut sxx, mut syy, mut sxy) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for (x, y) in &pairs {
        let (u, v) = (x - mx, y - my);
        dx += u;
        dy += v;
        sxx += u * u;
        syy += v * v;
        sxy += u * v;
    }
    let (sxx, syy, sxy) = (
        sxx - dx * dx / count,
        syy - dy * dy / count,
        sxy - dx * dy / count,
    );
    if sxx <= 0.0 || syy <= 0.0 {
        return (f64::NAN, n);
    }
    (sxy / (sxx * syy).sqrt(), n)
}

/// The matrix agrees with the reference over every pair of a frame of mixed
/// numeric types: nulls in different rows of each column, NaN and infinities, a
/// column of one value, one with two values, and a large offset with a small
/// spread. Each pair's count is its own, a pair of fewer than three rows or with
/// a constant is undefined, and the pair statistics read the same.
#[test]
fn correlation_agrees_with_the_reference_on_every_pair() -> Result<()> {
    let n = 600usize;
    let wave = |i: usize, k: f64| ((i as f64) * k).sin();
    let df = df!(
        "i8" => (0..n).map(|i| (i % 7 != 0).then_some((i % 100) as i8 - 50)).collect::<Vec<_>>(),
        "u32" => (0..n).map(|i| (i % 11 != 3).then_some((i * 13 % 1000) as u32)).collect::<Vec<_>>(),
        "i32" => (0..n).map(|i| (i * i % 977) as i32).collect::<Vec<_>>(),
        "u64" => (0..n).map(|i| 1_000_000_000_000_000u64 + (i as u64 * 7919 % 1000)).collect::<Vec<_>>(),
        "f32" => (0..n).map(|i| if i % 13 == 0 { f32::NAN } else { wave(i, 0.7) as f32 }).collect::<Vec<_>>(),
        "offset" => (0..n).map(|i| match i % 19 {
            0 => Some(f64::INFINITY),
            5 => None,
            _ => Some(1e9 + wave(i, 0.31) * 1e-3 + (i % 5) as f64 * 1e-4),
        }).collect::<Vec<_>>(),
        "trend" => (0..n).map(|i| (i % 23 != 4).then_some(i as f64 * 0.5 + wave(i, 1.3))).collect::<Vec<_>>(),
        "negative_inf" => (0..n).map(|i| if i % 17 == 0 { f64::NEG_INFINITY } else { -wave(i, 0.31) }).collect::<Vec<_>>(),
        "constant" => vec![42.0f64; n],
        "two_values" => (0..n).map(|i| (i == 10 || i == 20).then_some(i as f64)).collect::<Vec<_>>(),
    )?;

    let matrix = compute_correlation_matrix(&df)?;
    let floats: Vec<Vec<Option<f64>>> = matrix
        .columns
        .iter()
        .map(|name| {
            df.column(name)
                .unwrap()
                .cast(&DataType::Float64)
                .unwrap()
                .f64()
                .unwrap()
                .iter()
                .collect()
        })
        .collect();
    assert_eq!(matrix.columns.len(), 10, "every numeric type is correlated");
    let mut checked = 0;
    for i in 0..floats.len() {
        for j in (i + 1)..floats.len() {
            let (name_i, name_j) = (&matrix.columns[i], &matrix.columns[j]);
            let (expected, count) = reference_pearson(&floats[i], &floats[j]);
            let r = matrix.correlations[i][j];
            assert_eq!(matrix.sample_sizes[i][j], count, "{name_i} {name_j}");
            assert_eq!(r.to_bits(), matrix.correlations[j][i].to_bits());
            if expected.is_nan() {
                assert!(r.is_nan(), "{name_i} {name_j}: {r}");
                continue;
            }
            assert!(
                (r - expected).abs() <= 1e-9,
                "{name_i} {name_j}: {r} against {expected}"
            );
            let pair = compute_correlation_pair(&df, name_i, name_j)?;
            assert_eq!(pair.sample_size, count);
            assert!(
                (pair.correlation - expected).abs() <= 1e-12,
                "{name_i} {name_j}: {} against {expected}, matrix {r}",
                pair.correlation
            );
            let (p, pair_p) = (
                matrix.p_values.as_ref().unwrap()[i][j],
                pair.p_value.unwrap(),
            );
            assert!(
                (p - pair_p).abs() <= 1e-9 * pair_p.max(1e-300) || (p - pair_p).abs() < 1e-12,
                "{name_i} {name_j}: p {p} against {pair_p}"
            );
            checked += 1;
        }
    }
    assert!(checked >= 20, "{checked} pairs with a correlation");
    let index = |name: &str| matrix.columns.iter().position(|c| c == name).unwrap();
    assert!(matrix.correlations[index("constant")][index("trend")].is_nan());
    assert_eq!(matrix.sample_sizes[index("two_values")][index("trend")], 2);
    assert!(matrix.correlations[index("two_values")][index("trend")].is_nan());
    Ok(())
}

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
    LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default()).unwrap()
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
/// The runs see too few rows to count; the whole read counts what it read.
#[test]
fn a_block_sample_says_where_its_rows_sat() {
    let tens = (col("id") / lit(10_000i64)).alias("tens");
    for (rows, n) in [(100_000, 5_000), (10_000, 6_000)] {
        let dir = tempfile::tempdir().unwrap();
        let lf = climbing(dir.path(), rows);
        let read = sample_rows_counting(&lf, Some(n), None, 7, false, None, Some(&tens)).unwrap();
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
        if rows < 2 * n as i64 {
            assert_eq!(
                read.counted,
                Some(crate::sampling::Counted::Totals(
                    [(Some("0".to_string()), 10_000)].into()
                ))
            );
        } else {
            assert_eq!(read.counted, None, "runs see too few rows to count");
        }
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
fn a_matrix_in_bands_is_the_matrix_in_one() {
    // Columns with nulls in different rows, one with none and one of integers,
    // converted a band of rows at a time as well as all at once: the same matrix,
    // bit for bit, whether or not the bands divide the rows.
    let rows = 1_000;
    let mut columns: Vec<Column> = (0..6)
        .map(|c| {
            let v: Vec<Option<f64>> = (0..rows)
                .map(|r| {
                    (c == 0 || (r + c) % (5 + c) != 0)
                        .then(|| ((r * (c + 1)) as f64 * 0.37).sin() + (r % 13) as f64)
                })
                .collect();
            Series::new(format!("c{c}").into(), v).into()
        })
        .collect();
    let integers: Vec<Option<i64>> = (0..rows)
        .map(|r| (r % 9 != 4).then_some((r * r % 101) as i64))
        .collect();
    columns.push(Series::new("i".into(), integers).into());
    let df = DataFrame::new(rows, columns).unwrap();
    let whole = compute_correlation_matrix(&df).unwrap();
    let bits = |m: &CorrelationMatrix| {
        m.correlations
            .iter()
            .flatten()
            .chain(m.p_values.iter().flatten().flatten())
            .map(|v| v.to_bits())
            .collect::<Vec<_>>()
    };
    assert!(whole.correlations[0][6].is_finite());
    for band in [1, 2, 7, 333, 999, 1_000, 5_000] {
        let bands = correlation_matrix_in_bands(&df, band).unwrap();
        assert_eq!(bits(&bands), bits(&whole), "{band} rows at a time");
        assert_eq!(bands.sample_sizes, whole.sample_sizes);
    }
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

/// Spearman's ρ by the book: rank both columns over the pair's rows, average
/// ranks for ties, then Pearson's r of the ranks.
fn spearman_by_hand(x: &[Option<f64>], y: &[Option<f64>]) -> f64 {
    let pairs: Vec<(f64, f64)> = x
        .iter()
        .zip(y)
        .filter_map(|(a, b)| Some((a.filter(|v| v.is_finite())?, b.filter(|v| v.is_finite())?)))
        .collect();
    let rank = |values: Vec<f64>| -> Vec<f64> {
        values
            .iter()
            .map(|v| {
                let below = values.iter().filter(|w| *w < v).count() as f64;
                let equal = values.iter().filter(|w| *w == v).count() as f64;
                below + (equal + 1.0) / 2.0
            })
            .collect()
    };
    let a = rank(pairs.iter().map(|p| p.0).collect());
    let b = rank(pairs.iter().map(|p| p.1).collect());
    let n = a.len() as f64;
    let (ma, mb) = (a.iter().sum::<f64>() / n, b.iter().sum::<f64>() / n);
    let sab: f64 = a.iter().zip(&b).map(|(x, y)| (x - ma) * (y - mb)).sum();
    let saa: f64 = a.iter().map(|x| (x - ma).powi(2)).sum();
    let sbb: f64 = b.iter().map(|y| (y - mb).powi(2)).sum();
    sab / (saa * sbb).sqrt()
}

#[test]
fn spearman_ranks_each_pair_over_the_rows_both_hold() {
    let rows = 400;
    let x: Vec<Option<f64>> = (0..rows)
        .map(|r| (r % 7 != 3).then(|| ((r * 37 % 101) as f64 * 0.5).floor()))
        .collect();
    // Monotone in x but far from a line, with its own gaps and a NaN.
    let y: Vec<Option<f64>> = (0..rows)
        .map(|r| match r {
            _ if r % 11 == 5 => None,
            17 => Some(f64::NAN),
            _ => x[r].map(|v| v.powi(5)),
        })
        .collect();
    let z: Vec<Option<f64>> = (0..rows).map(|r| Some(((r * 13) % 29) as f64)).collect();
    let w: Vec<Option<f64>> = (0..rows)
        .map(|r| Some(((r * 7) % 31) as f64 - (r % 3) as f64))
        .collect();
    let df = DataFrame::new(
        rows,
        vec![
            Series::new("x".into(), &x).into(),
            Series::new("y".into(), &y).into(),
            Series::new("z".into(), &z).into(),
            Series::new("w".into(), &w).into(),
        ],
    )
    .unwrap();
    let m = compute_correlation_matrix(&df).unwrap();
    let columns = [&x, &y, &z, &w];
    for i in 0..4 {
        assert_eq!(m.coefficient(CorrelationMethod::Spearman, i, i), 1.0);
        for j in (0..4).filter(|&j| j != i) {
            let expected = spearman_by_hand(columns[i], columns[j]);
            let got = m.coefficient(CorrelationMethod::Spearman, i, j);
            assert!(
                (got - expected).abs() < 1e-12,
                "{i},{j}: {got} vs {expected}"
            );
        }
    }
    assert!((m.coefficient(CorrelationMethod::Spearman, 0, 1) - 1.0).abs() < 1e-12);
    assert!(m.coefficient(CorrelationMethod::Pearson, 0, 1) < 0.99);
    assert!(m.p_value(CorrelationMethod::Spearman, 2, 3).is_some());
}

#[test]
fn spearman_of_a_constant_column_is_undefined() {
    let df = df!(
        "year" => vec![2020.0f64; 50],
        "value" => (0..50).map(|i| i as f64).collect::<Vec<_>>()
    )
    .unwrap();
    let matrix = compute_correlation_matrix(&df).unwrap();
    assert!(
        matrix
            .coefficient(CorrelationMethod::Spearman, 0, 1)
            .is_nan()
    );
}

#[test]
fn a_constant_column_is_constant_and_a_hopeless_one_has_no_clear_fit() {
    let constant = Series::new("c".into(), vec![2020.0f64; 500]);
    let info = infer_distribution(&NumericColumn::of(&constant).unwrap().spread(), 500);
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
    let info = infer_distribution(&NumericColumn::of(&bimodal).unwrap().spread(), 2_000);
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
    let info = infer_distribution(&NumericColumn::of(&integers).unwrap().spread(), 2_000);
    assert!(
        !matches!(
            info.distribution_type,
            DistributionType::Binomial | DistributionType::Poisson | DistributionType::Geometric
        ),
        "{:?}",
        info.distribution_type
    );
}

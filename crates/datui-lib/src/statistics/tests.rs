use super::*;

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

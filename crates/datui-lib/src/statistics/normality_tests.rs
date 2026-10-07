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
        .map(|i| crate::distribution_fit::normal_quantile(i as f64 / 501.0))
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

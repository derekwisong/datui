//! Each estimator against reference values: scipy 1.18 (`linregress`,
//! `theilslopes(method="joint")`, `spearmanr`), numpy's `polyfit`, and statsmodels
//! 0.15's `lowess`, run on the same points.

use super::*;

/// The fits as a job makes them, never stopped.
fn fit_all(series: &[Vec<(f64, f64)>], other: bool, key: TrendKey, seed: u64) -> TrendFits {
    super::fit_all(series, other, key, seed, &|| false).unwrap()
}

/// Theil–Sen over one series, every point taken.
fn theil_sen(points: &[(f64, f64)], budget: usize, seed: u64) -> Option<(f64, Pairs)> {
    super::theil_sen(
        Source::new(&[points]),
        &takes(Trend::Linear),
        budget,
        seed,
        &|| false,
    )
}

/// LOWESS of `xs` and `ys`, never stopped.
fn lowess(xs: &[f64], ys: &[f64], frac: f64, iterations: usize, delta: f64) -> Vec<f64> {
    let points: Vec<(f64, f64)> = xs.iter().copied().zip(ys.iter().copied()).collect();
    super::lowess(&points, frac, iterations, delta, &|| false).unwrap()
}

fn close(a: f64, b: f64, tol: f64) {
    assert!((a - b).abs() <= tol, "{a} vs {b}");
}

/// x 1..=20, y = 3 + 2x and a repeating wobble of -2..=2.
fn wobble() -> Vec<f64> {
    (0..20).map(|i| f64::from((i * 7) % 5) - 2.0).collect()
}

fn line_points() -> Vec<(f64, f64)> {
    wobble()
        .into_iter()
        .enumerate()
        .map(|(i, n)| {
            let x = i as f64 + 1.0;
            (x, 3.0 + 2.0 * x + n)
        })
        .collect()
}

#[test]
fn least_squares_matches_linregress() {
    let fit = fit_one(&line_points(), Trend::Linear, 0).unwrap();
    let Model::Line { x0, y0, slope } = fit.model else {
        panic!("{:?}", fit.model)
    };
    close(slope, 2.030075187969925, 1e-12);
    close(y0 - slope * x0, 2.6842105263157876, 1e-12);
    close(fit.r, 0.9928886539034191, 1e-12);
    close(fit.resid_sd, 1.47946116768913, 1e-12);
    close(fit.rho, 0.9894736842105262, 1e-12);
    assert_eq!((fit.used, fit.dropped, fit.x_range), (20, 0, [1.0, 20.0]));
    assert!(!fit.ranks_disagree());
}

/// Centered on its mean X, a fit over nanosecond epochs keeps its precision.
#[test]
fn least_squares_keeps_its_precision_over_epochs() {
    let epoch = 1.7e18;
    let day = 86_400e9;
    let points: Vec<(f64, f64)> = line_points()
        .into_iter()
        .map(|(x, y)| (epoch + x * day, y))
        .collect();
    let fit = fit_one(&points, Trend::Linear, 0).unwrap();
    let Model::Line { slope, .. } = fit.model else {
        panic!()
    };
    close(slope * day, 2.030075187969925, 1e-6);
    close(
        fit.model.at(epoch + 10.0 * day),
        2.6842105263157876 + 20.30075187969925,
        1e-6,
    );
}

#[test]
fn exponential_and_power_fit_the_logs() {
    let wobble = wobble();
    let exp: Vec<(f64, f64)> = wobble
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let x = i as f64 + 1.0;
            (x, 5.0 * (0.1 * x).exp() * (n / 20.0).exp())
        })
        .collect();
    let fit = fit_one(&exp, Trend::Exponential, 0).unwrap();
    let Model::Exp { x0, ln_y0, rate } = fit.model else {
        panic!()
    };
    close(rate, 0.10150375939849628, 1e-12);
    close(ln_y0 - rate * x0, 1.5936484387498886, 1e-12);

    let power: Vec<(f64, f64)> = wobble
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let x = i as f64 + 1.0;
            (x, 2.0 * x.powf(1.5) * (n / 20.0).exp())
        })
        .collect();
    let fit = fit_one(&power, Trend::Power, 0).unwrap();
    let Model::Power {
        ln_x0,
        ln_y0,
        power,
    } = fit.model
    else {
        panic!()
    };
    close(power, 1.5151043536645974, 1e-12);
    close(ln_y0 - power * ln_x0, 0.6611745743783491, 1e-12);
    assert_eq!(fit.x_range, [1.0, 20.0]);
}

/// Exponential leaves out y of 0 or less, power x or y of 0 or less, NaN goes
/// unfitted, and a fit of fewer than ten points is none.
#[test]
fn values_a_log_cannot_take_are_left_out() {
    let mut points = line_points();
    points.push((21.0, 0.0));
    points.push((22.0, -3.0));
    points.push((-1.0, 5.0));
    points.push((23.0, f64::NAN));
    let fit = fit_one(&points, Trend::Exponential, 0).unwrap();
    assert_eq!((fit.used, fit.dropped), (21, 2));
    let fit = fit_one(&points, Trend::Power, 0).unwrap();
    assert_eq!((fit.used, fit.dropped), (20, 3));
    let fit = fit_one(&points, Trend::Linear, 0).unwrap();
    assert_eq!((fit.used, fit.dropped), (23, 0));
    assert!(fit_one(&points[..9], Trend::Linear, 0).is_none());
    let flat: Vec<(f64, f64)> = (0..12).map(|i| (1.0, f64::from(i))).collect();
    assert!(fit_one(&flat, Trend::Linear, 0).is_none(), "no spread in X");
}

#[test]
fn theil_sen_matches_scipy_and_shrugs_off_outliers() {
    let mut points = line_points();
    points[3].1 = 90.0;
    points[15].1 = -40.0;
    let fit = fit_one(&points, Trend::Robust, 0).unwrap();
    let Model::Line { x0, y0, slope } = fit.model else {
        panic!()
    };
    close(slope, 2.0, 1e-12);
    close(y0 - slope * x0, 3.0, 1e-12);
    assert_eq!(
        fit.pairs,
        Some(Pairs {
            used: 190,
            of: 190,
            sampled: false
        })
    );
    let least = fit_one(&points, Trend::Linear, 0).unwrap();
    let Model::Line { slope: pulled, .. } = least.model else {
        panic!()
    };
    assert!(
        (pulled - 2.0).abs() > 0.5,
        "least squares follows them: {pulled}"
    );

    let xs = [0.5, 1.7, 2.2, 3.9, 4.1, 5.5, 6.0, 7.3, 8.8, 9.1, 10.4, 11.0];
    let ys = [
        1.2, 2.9, 2.1, 5.5, 4.0, 7.9, 6.2, 30.0, 9.9, 8.1, 12.7, 11.5,
    ];
    let points: Vec<(f64, f64)> = xs.into_iter().zip(ys).collect();
    let fit = fit_one(&points, Trend::Robust, 0).unwrap();
    let Model::Line { x0, y0, slope } = fit.model else {
        panic!()
    };
    close(slope, 1.0540963855421688, 1e-12);
    // The intercept as `theilslopes(method="joint")`: the median of y - slope*x.
    close(y0 - slope * x0, 0.6484518072289152, 1e-12);
    // Its outlier pulls Pearson's r well below Spearman's ρ.
    let least = fit_one(&points, Trend::Linear, 0).unwrap();
    close(least.r, 0.5663298225406637, 1e-12);
    close(least.rho, 0.8951048951048951, 1e-12);
    assert!(least.ranks_disagree());
}

/// Past the budget a random draw of pairs is taken, the same for the same seed.
#[test]
fn theil_sen_draws_pairs_past_its_budget_by_seed() {
    let points = line_points();
    let (a, pairs) = theil_sen(&points, 50, 7).unwrap();
    assert_eq!(
        pairs,
        Pairs {
            used: 50,
            of: 190,
            sampled: true
        }
    );
    let (b, _) = theil_sen(&points, 50, 7).unwrap();
    assert_eq!(a.to_bits(), b.to_bits());
    let draws: Vec<u64> = (0..8)
        .map(|seed| theil_sen(&points, 50, seed).unwrap().0.to_bits())
        .collect();
    assert!(draws.iter().any(|&d| d != draws[0]), "seeds draw others");
    close(a, 2.0, 0.5);
}

fn lowess_points() -> (Vec<f64>, Vec<f64>) {
    let xs: Vec<f64> = (0..40).map(|i| 10.0 * f64::from(i) / 39.0).collect();
    let mut ys: Vec<f64> = xs
        .iter()
        .enumerate()
        .map(|(i, x)| x.sin() + f64::from(((i * 13) % 7) as i32 - 3) / 10.0)
        .collect();
    ys[10] = 4.0;
    (xs, ys)
}

#[test]
fn lowess_matches_statsmodels() {
    let (xs, ys) = lowess_points();
    let cases: [(f64, usize, f64, &[f64]); 3] = [
        (2.0 / 3.0, 3, 0.1, &STATSMODELS_DEFAULT),
        (0.3, 0, 0.0, &STATSMODELS_PLAIN),
        (0.5, 2, 1.0, &STATSMODELS_DELTA),
    ];
    for (frac, it, delta, want) in cases {
        let got = lowess(&xs, &ys, frac, it, delta);
        for (g, w) in got.iter().zip(want) {
            close(*g, *w, 1e-9);
        }
    }
    // The fit's model draws through those values.
    let points: Vec<(f64, f64)> = xs.iter().copied().zip(ys.iter().copied()).collect();
    let fit = fit_one(&points, Trend::Local, 0).unwrap();
    close(fit.model.at(xs[5]), STATSMODELS_DEFAULT[5], 1e-9);
}

/// `lowess(y, x, frac=2/3, it=3, delta=0.01 * 10)`.
const STATSMODELS_DEFAULT: [f64; 40] = [
    1.1534471513288973,
    1.0636489061046235,
    0.9713584423788316,
    0.8767077301434077,
    0.7800308569546879,
    0.6817874768889557,
    0.5824955458489202,
    0.4826807679342574,
    0.38284479844234137,
    0.2834233793581644,
    0.18459228958484342,
    0.08593050983601667,
    -0.014945193131126888,
    -0.10928223123657393,
    -0.19326324206088186,
    -0.2655564137759002,
    -0.32360559742787565,
    -0.36383261187837895,
    -0.3822649292113738,
    -0.3755568418675811,
    -0.34319736011803526,
    -0.28831106210801294,
    -0.21654170150489974,
    -0.13444802518300944,
    -0.04622632545222852,
    0.042939529111601496,
    0.128401634161045,
    0.20736316356937284,
    0.24173705872144238,
    0.27134167642704415,
    0.2996089681723157,
    0.3266321945428463,
    0.3520333252299664,
    0.37532576722525124,
    0.3959607419713713,
    0.41338887317868217,
    0.427159434446873,
    0.43698342028610615,
    0.4426570741112555,
    0.44415036208514,
];

/// `lowess(y, x, frac=0.3, it=0, delta=0)`.
const STATSMODELS_PLAIN: [f64; 40] = [
    0.203380893985411,
    0.3356878615043531,
    0.46313369254587283,
    0.5899809813161195,
    0.718829594748165,
    0.8436022441452813,
    1.0076550458176274,
    1.1417603637610274,
    1.1762002169864323,
    1.0978695200966393,
    0.9386771410660463,
    0.741267543794499,
    0.4881855012354709,
    0.16375381020768212,
    -0.19588987177228778,
    -0.5072898539336175,
    -0.6884515830632467,
    -0.7903547523451803,
    -0.8356455665921041,
    -0.8262961960628578,
    -0.7678515575391281,
    -0.6573273903849687,
    -0.5024552760019106,
    -0.31609663820008815,
    -0.11094163131464017,
    0.10640108461163363,
    0.31678657470438454,
    0.501524814783782,
    0.6553421317385871,
    0.7676761438863555,
    0.8284468575580162,
    0.8331759101809506,
    0.788359852357473,
    0.691995471028136,
    0.5454501327594348,
    0.36197614657051913,
    0.17489347317326925,
    -0.010384180680666544,
    -0.19487162888894674,
    -0.38082748573461483,
];

/// `lowess(y, x, frac=0.5, it=2, delta=1.0)`: most points joined, not fitted.
const STATSMODELS_DELTA: [f64; 40] = [
    0.9799745338742285,
    0.9253554160269128,
    0.870736298179597,
    0.8161171803322811,
    0.7470702203206072,
    0.6780232603089331,
    0.6089763002972591,
    0.5312493608609068,
    0.45352242142455435,
    0.3757954819882019,
    0.24999202552764013,
    0.1241885690670783,
    -0.001614887393483539,
    -0.13801325409201204,
    -0.27441162079054054,
    -0.4108099874890691,
    -0.4546320425514123,
    -0.4984540976137555,
    -0.5422761526760986,
    -0.49857728186355355,
    -0.45487841105100835,
    -0.41117954023846315,
    -0.297665328899379,
    -0.18415111756029487,
    -0.07063690622121072,
    0.06432118930338224,
    0.1992792848279752,
    0.33423738035256817,
    0.3903855965348375,
    0.4465338127171068,
    0.5026820288993761,
    0.46633721750063817,
    0.4299924061019002,
    0.3936475947031624,
    0.347957256529816,
    0.3022669183564697,
    0.2565765801831236,
    0.19761951721480092,
    0.13866245424647783,
    0.07100295107176713,
];

/// y = x is no fit: it counts the points above it and how far they typically fall.
#[test]
fn y_equals_x_measures_the_points_against_it() {
    let points = [(1.0, 2.0), (2.0, 1.0), (3.0, 5.0), (4.0, 4.0)];
    let fit = fit_one(&points, Trend::Identity, 0).unwrap();
    assert_eq!(fit.model, Model::Identity);
    close(fit.above, 0.5, 0.0);
    close(fit.resid_sd, (6.0f64 / 4.0).sqrt(), 1e-12);
    assert_eq!(fit.x_range, [1.0, 4.0]);
}

/// Per color: a fit per series of ten points or more, never Other; overall: every
/// point, Other's too.
#[test]
fn fits_follow_over_and_leave_other_and_small_series_out() {
    let a = line_points();
    let small: Vec<(f64, f64)> = a[..5].to_vec();
    let other: Vec<(f64, f64)> = a.iter().map(|&(x, y)| (x, y + 100.0)).collect();
    let series = vec![a.clone(), small, other];
    let per = TrendKey {
        trend: Trend::Linear,
        over: TrendOver::Color,
    };
    let fits = fit_all(&series, true, per, 0);
    assert_eq!(fits.fits.len(), 1);
    assert_eq!(fits.fits[0].series, Some(0));
    assert_eq!(fits.too_few, 1);
    let overall = fit_all(
        &series,
        true,
        TrendKey {
            over: TrendOver::Overall,
            ..per
        },
        0,
    );
    assert_eq!(overall.fits.len(), 1);
    assert_eq!((overall.fits[0].series, overall.fits[0].used), (None, 45));
    // y = x is one line over every point whatever Over says.
    let identity = fit_all(
        &series,
        true,
        TrendKey {
            trend: Trend::Identity,
            over: TrendOver::Color,
        },
        0,
    );
    assert_eq!(identity.fits.len(), 1);
    assert_eq!(identity.fits[0].series, None);
}

#[test]
fn medians_and_ranks() {
    assert!(median(&mut []).is_nan());
    close(median(&mut [3.0, 1.0, 2.0]), 2.0, 0.0);
    close(median(&mut [4.0, 1.0, 3.0, 2.0]), 2.5, 0.0);
    assert_eq!(
        ranks([10.0, 20.0, 10.0, 5.0].into_iter()),
        [2.5, 4.0, 2.5, 1.0]
    );
}

#[test]
fn significant_digits() {
    let plain = AxisNumbers::default();
    assert_eq!(significant(2.3456, 3, &plain), "2.35");
    assert_eq!(significant(0.0012345, 3, &plain), "0.00123");
    assert_eq!(significant(12345.0, 3, &plain), "12345");
    assert_eq!(significant(-41.26, 3, &plain), "-41.3");
}

fn read_as<'a>(x: &'a Axis, y: &'a Axis) -> Words<'a> {
    Words {
        x,
        y,
        x_name: "distance".to_string(),
        x_unit: Some("mi".to_string()),
        y_name: "air_time".to_string(),
        y_unit: Some("min".to_string()),
        points: None,
    }
}

fn fitted(points: &[(f64, f64)], trend: Trend) -> TrendFits {
    fit_all(
        &[points.to_vec()],
        false,
        TrendKey {
            trend,
            over: TrendOver::Color,
        },
        3,
    )
}

/// Each method's reading, term by term.
#[test]
fn readings_say_each_method() {
    let (x, y) = (Axis::default(), Axis::default());
    let words = read_as(&x, &y);
    let g = crate::glyphs::unicode();
    let read = |points: &[(f64, f64)], trend, words: &Words<'_>| {
        reading(&fitted(points, trend), words, g)
            .unwrap()
            .join(" · ")
    };
    let line = line_points();
    assert_eq!(
        read(&line, Trend::Linear, &words),
        "+2.03 min per mi · strong, R² 0.99 · typically ±1.48 min off the line"
    );
    // 10% more for each mile, give or take 2%.
    let grow: Vec<(f64, f64)> = line
        .iter()
        .map(|&(x, y)| {
            (
                x,
                10.0 * 1.1f64.powf(x) * (1.0 + (y - 3.0 - 2.0 * x) / 50.0),
            )
        })
        .collect();
    assert_eq!(
        read(&grow, Trend::Exponential, &words),
        "doubles every 7.23 mi · +10% per mi · strong, R² 1.00 · typically within ×1.03 of the line"
    );
    assert_eq!(
        read(&grow, Trend::Power, &words),
        "y ~ x^0.652 · 1% more distance, about 0.65% more air_time · strong, R² 0.87 · \
         typically within ×1.23 of the line"
    );
    assert_eq!(
        read(&line, Trend::Robust, &words),
        "+2.00 min per mi · half the points within ±1.00 min of it"
    );
    assert_eq!(
        read(&line, Trend::Local, &words),
        "local curve, each point from its nearest 2/3 of the data · typically ±1.49 min off the line"
    );
    assert_eq!(
        read(&line, Trend::Identity, &words),
        "100% of points above y = x · typically ±14.8 min off the line"
    );
    // Without units, X is a unit of its column.
    let bare = Words {
        x_unit: None,
        y_unit: None,
        ..read_as(&x, &y)
    };
    assert_eq!(
        read(&line, Trend::Linear, &bare),
        "+2.03 per unit of distance · strong, R² 0.99 · typically ±1.48 off the line"
    );
    assert!(
        read(&grow, Trend::Exponential, &bare)
            .starts_with("doubles every 7.23 units of distance · +10% per unit of distance")
    );
    // A constant Y neither doubles nor halves; points with no trend say so.
    let level: Vec<(f64, f64)> = (1..=20).map(|x| (f64::from(x), 5.0)).collect();
    assert!(read(&level, Trend::Exponential, &words).starts_with("flat · "));
    // A valley: as far up on each side, so no line leans either way.
    let scattered: Vec<(f64, f64)> = (1..=20)
        .map(|x| (f64::from(x), (f64::from(x) - 10.5).powi(2)))
        .collect();
    let fits = fitted(&scattered, Trend::Linear);
    assert!(fits.fits[0].r.abs() < 0.05, "{}", fits.fits[0].r);
    assert!(
        read(&scattered, Trend::Linear, &words).contains(" · no clear trend, R² 0.00 · "),
        "{}",
        read(&scattered, Trend::Linear, &words)
    );

    let key = TrendKey {
        trend: Trend::Linear,
        over: TrendOver::Color,
    };
    let failed = TrendFits::failed(key, "it panicked".to_string());
    assert_eq!(
        reading(&failed, &words, g).unwrap(),
        ["could not fit: it panicked"]
    );

    // A date X reads per a length of time its span holds three of, and its equation
    // starts where the line does.
    let days = Axis {
        kind: XAxisTemporalKind::Date,
        ..Axis::default()
    };
    let dated = Words {
        x: &days,
        ..read_as(&x, &y)
    };
    let points: Vec<(f64, f64)> = line.iter().map(|&(x, y)| (19_000.0 + x, y)).collect();
    let fits = fitted(&points, Trend::Linear);
    assert_eq!(reading(&fits, &dated, g).unwrap()[0], "+2.03 min per day");
    assert_eq!(
        equation(&fits.fits[0], &dated).unwrap(),
        "y = 4.71 min at 2022-01-09, +2.03 min per day"
    );
    let weeks: Vec<(f64, f64)> = (0..60)
        .map(|d| (19_000.0 + f64::from(d), f64::from(d)))
        .collect();
    assert_eq!(
        reading(&fitted(&weeks, Trend::Linear), &dated, g).unwrap()[0],
        "+7.00 min per week"
    );
    let growing: Vec<(f64, f64)> = grow.iter().map(|&(x, y)| (19_000.0 + 3.0 * x, y)).collect();
    let fits = fitted(&growing, Trend::Exponential);
    assert_eq!(slope_text(&fits.fits[0], &dated).unwrap(), "+25% per week");
    assert_eq!(
        equation(&fits.fits[0], &dated).unwrap(),
        "y = 10.9 min at 2022-01-11, +25% per week"
    );
}

/// Several fits: how many, and the range of what they say.
#[test]
fn several_fits_read_as_a_range() {
    let (x, y) = (Axis::default(), Axis::default());
    let words = read_as(&x, &y);
    let g = crate::glyphs::unicode();
    let line = line_points();
    let steeper: Vec<(f64, f64)> = line.iter().map(|&(x, y)| (x, 1.1 * y)).collect();
    let key = TrendKey {
        trend: Trend::Linear,
        over: TrendOver::Color,
    };
    let two = fit_all(&[line.clone(), steeper], false, key, 0);
    assert_eq!(
        reading(&two, &words, g).unwrap(),
        ["2 fits", "slopes +2.03 to +2.23 min per mi"]
    );
    let same = fit_all(&[line.clone(), line.clone()], false, key, 0);
    assert_eq!(
        reading(&same, &words, g).unwrap(),
        ["2 fits", "slopes +2.03 min per mi"]
    );
    let grow: Vec<(f64, f64)> = (1..=20).map(|x| (f64::from(x), 1.1f64.powi(x))).collect();
    let faster: Vec<(f64, f64)> = (1..=20).map(|x| (f64::from(x), 1.25f64.powi(x))).collect();
    let exp = fit_all(
        &[grow.clone(), faster],
        false,
        TrendKey {
            trend: Trend::Exponential,
            ..key
        },
        0,
    );
    assert_eq!(
        reading(&exp, &words, g).unwrap(),
        ["2 fits", "+10% to +25% per mi"]
    );
    let local = fit_all(
        &[line.clone(), line],
        false,
        TrendKey {
            trend: Trend::Local,
            ..key
        },
        0,
    );
    assert_eq!(reading(&local, &words, g).unwrap(), ["2 fits"]);
}

/// The notes say what was fitted: the points and nulls, values a log left out,
/// series too small, and Theil–Sen's pairs with their seed unless it is said.
#[test]
fn notes_say_what_was_fitted() {
    let g = crate::glyphs::unicode();
    let mut fits = fitted(&line_points(), Trend::Linear);
    assert_eq!(notes(&fits, 3, None, false, g), ["fit on 20 · 3 null"]);
    assert_eq!(
        notes(&fits, 3, Some("means"), false, g),
        ["fit on 20 means"]
    );
    fits.too_few = 2;
    fits.seed = Some(9);
    fits.fits[0].pairs = Some(Pairs {
        used: 990_000,
        of: 5_000_000,
        sampled: true,
    });
    assert_eq!(
        notes(&fits, 0, None, false, g),
        [
            "fit on 20",
            "2 series under 10 points not fitted",
            "slopes of 990k of 5.0M pairs · seed 9"
        ]
    );
    assert_eq!(
        notes(&fits, 0, None, true, g)[2],
        "slopes of 990k of 5.0M pairs"
    );
    let mut points = line_points();
    points[0].1 = 0.0;
    let exp = fitted(&points, Trend::Exponential);
    assert_eq!(
        notes(&exp, 0, None, false, g),
        ["fit on 19", "1 point with Y ≤ 0 left out"]
    );
    points[1].0 = -1.0;
    let power = fitted(&points, Trend::Power);
    assert_eq!(
        notes(&power, 0, None, false, g)[1],
        "2 points with X or Y ≤ 0 left out"
    );
    // y = x is drawn, not fitted.
    assert!(notes(&fitted(&points, Trend::Identity), 0, None, false, g).is_empty());
}

/// The mark: Pearson and Spearman apart by 0.2 or more on a linear fit, naming the
/// series when there are several.
#[test]
fn ranks_that_disagree_are_marked() {
    let g = crate::glyphs::unicode();
    let names = ["calm".to_string(), "curved".to_string()];
    let line = fitted(&line_points(), Trend::Linear);
    assert_eq!(rank_mark(&line, &names, g), None);
    // A curve that rises in rank throughout: ρ 1, r well short.
    let curve: Vec<(f64, f64)> = (1..=20)
        .map(|x| (f64::from(x), f64::from(x).powi(8)))
        .collect();
    let alone = fitted(&curve, Trend::Linear);
    assert_eq!(rank_mark(&alone, &names, g).unwrap(), "▲ r≠ρ: try robust");
    assert_eq!(
        rank_mark(&alone, &names, crate::glyphs::ascii()).unwrap(),
        "! r!=rho: try robust"
    );
    let key = TrendKey {
        trend: Trend::Linear,
        over: TrendOver::Color,
    };
    let both = fit_all(&[line_points(), curve.clone()], false, key, 0);
    assert_eq!(
        rank_mark(&both, &names, g).unwrap(),
        "▲ r≠ρ for curved: try robust"
    );
    assert_eq!(rank_mark(&fitted(&curve, Trend::Robust), &names, g), None);
}

/// Spearman ranks an even stride of the points past its limit, and says so.
#[test]
fn spearman_strides_past_its_limit() {
    let n = RANKED_MAX + 10;
    let points: Vec<(f64, f64)> = (0..n).map(|i| (i as f64, (i as f64).sqrt())).collect();
    let fit = fit_one(&points, Trend::Linear, 0).unwrap();
    assert_eq!(fit.rho_of, Some(n.div_ceil(2)));
    close(fit.rho, 1.0, 1e-12);
    assert!(
        fit_one(&line_points(), Trend::Linear, 0)
            .unwrap()
            .rho_of
            .is_none()
    );
}

/// A stop asked for ends a long fit with nothing.
#[test]
fn a_stop_ends_theil_sen_and_lowess() {
    let points: Vec<(f64, f64)> = (0..2_000)
        .map(|i| (f64::from(i), f64::from(i % 7)))
        .collect();
    let stopped = &|| true;
    assert!(
        super::theil_sen(
            Source::new(&[&points]),
            &takes(Trend::Linear),
            1_000,
            1,
            stopped
        )
        .is_none()
    );
    assert!(super::lowess(&points, LOWESS_FRAC, 3, 0.0, stopped).is_none());
    let key = TrendKey {
        trend: Trend::Robust,
        over: TrendOver::Color,
    };
    assert!(super::fit_all(&[points], false, key, 1, stopped).is_none());
}

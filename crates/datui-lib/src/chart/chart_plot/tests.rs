use super::*;

fn ys(points: &[(f64, f64)]) -> Vec<f64> {
    points.iter().map(|&(_, y)| y).collect()
}

/// A length of time takes the points whose X is in `(x - length, x]`, however they
/// are spaced, and at the start whatever there is.
#[test]
fn a_trailing_length_takes_the_points_inside_it() {
    let points = [(0.0, 1.0), (1.0, 3.0), (5.0, 5.0), (5.5, 7.0), (10.0, 9.0)];
    let smoothed = trailing_mean(&points, None, Window::Length(2.0));
    assert_eq!(
        smoothed.iter().map(|p| p.0).collect::<Vec<_>>(),
        [0.0, 1.0, 5.0, 5.5, 10.0]
    );
    assert_eq!(ys(&smoothed), [1.0, 2.0, 5.0, 6.0, 9.0]);
    // Exactly `length` back is out: (0, 2] at x = 2 leaves x = 0 out.
    let edge = trailing_mean(&[(0.0, 10.0), (2.0, 20.0)], None, Window::Length(2.0));
    assert_eq!(ys(&edge), [10.0, 20.0]);
}

/// A count takes the last N points, fewer at the start.
#[test]
fn a_trailing_count_takes_the_last_points() {
    let points: Vec<(f64, f64)> = (0..5).map(|i| (f64::from(i), f64::from(i))).collect();
    assert_eq!(
        ys(&trailing_mean(&points, None, Window::Points(3))),
        [0.0, 0.5, 1.0, 2.0, 3.0]
    );
    assert_eq!(
        ys(&trailing_mean(&points, None, Window::Points(1))),
        ys(&points)
    );
}

/// NaN is skipped: the mean is of the finite values in the window, and a window
/// with none is NaN.
#[test]
fn nan_is_skipped() {
    let points = [(0.0, f64::NAN), (1.0, 2.0), (2.0, f64::NAN), (3.0, 4.0)];
    let smoothed = ys(&trailing_mean(&points, None, Window::Points(2)));
    assert!(smoothed[0].is_nan());
    assert_eq!(&smoothed[1..], [2.0, 2.0, 4.0]);
    let timed = ys(&trailing_mean(&points, None, Window::Length(10.0)));
    assert_eq!(&timed[1..], [2.0, 2.0, 3.0]);
}

/// Points at one X are all in its window, none of them being after it; points out
/// of order are smoothed in X order and come back where they were.
#[test]
fn ties_and_order() {
    let ties = trailing_mean(
        &[(0.0, 1.0), (0.0, 3.0), (1.0, 5.0)],
        None,
        Window::Length(0.5),
    );
    assert_eq!(ys(&ties), [2.0, 2.0, 5.0]);
    let shuffled = trailing_mean(
        &[(2.0, 6.0), (0.0, 2.0), (1.0, 4.0)],
        None,
        Window::Points(2),
    );
    assert_eq!(shuffled, [(2.0, 5.0), (0.0, 2.0), (1.0, 3.0)]);
}

/// Points standing for rows (an envelope's steps) average their rows; a window of
/// points counts rows, the oldest step's taken in part at its mean.
#[test]
fn weighted_points_average_their_rows() {
    // Step one: 3 rows summing to 3; step two: 1 row of 9 (low and high points).
    let points = [(0.0, 0.0), (0.0, 2.0), (1.0, 9.0)];
    let weights = [(3.0, 3.0), (0.0, 0.0), (9.0, 1.0)];
    let timed = trailing_mean(&points, Some(&weights), Window::Length(5.0));
    assert_eq!(ys(&timed), [1.0, 1.0, 3.0]);
    // Two rows: the 9, and one of the first step's at its mean of 1.
    let counted = trailing_mean(&points, Some(&weights), Window::Points(2));
    assert_eq!(ys(&counted), [1.0, 1.0, 5.0]);
}

/// A window that took in a huge value and let it go leaves no rounding in the
/// means after it.
#[test]
fn a_spike_leaves_no_rounding_behind() {
    let mut points: Vec<(f64, f64)> = (0..50).map(|i| (f64::from(i), 0.1)).collect();
    points[10].1 = 1e17;
    let smoothed = ys(&trailing_mean(&points, None, Window::Points(3)));
    assert!(
        smoothed[40..].iter().all(|&y| (y - 0.1).abs() < 1e-12),
        "{:?}",
        &smoothed[40..]
    );
}

fn lines(series: Points, kind: XAxisTemporalKind) -> LinesData {
    let n = series.len();
    LinesData::new(
        chart_data::GroupedSeries {
            names: (0..n).map(|i| format!("s{i}")).collect(),
            series,
            breaks: vec![Vec::new(); n],
            x_axis_kind: kind,
            rows: RowsRead::default(),
            other: false,
        },
        None,
    )
}

fn smoothed(smooth: Smooth) -> Shown {
    Shown {
        smooth,
        ..Shown::default()
    }
}

/// Each series is smoothed on its own, and a duration X's window is in seconds.
#[test]
fn each_series_is_smoothed_on_its_own() {
    let mut data = lines(
        vec![
            vec![(0.0, 0.0), (1.0, 10.0), (2.0, 20.0)],
            vec![(0.0, 100.0), (2.0, 200.0)],
        ],
        XAxisTemporalKind::Duration,
    );
    let shown = smoothed(Smooth::Seconds(5));
    data.keep_shown(shown);
    let drawn: Vec<Vec<f64>> = data.drawn(shown).map(|s| ys(s.points)).collect();
    assert_eq!(drawn, [vec![0.0, 5.0, 10.0], vec![100.0, 150.0]]);
    // Two series: auto leaves the raw lines out.
    assert_eq!(data.raw_drawn(shown).count(), 0);
    let (values, raw) = data.values(shown);
    assert_eq!(values[0][1], (1.0, 5.0));
    assert_eq!(raw.unwrap()[0][1], (1.0, 10.0));
    // A count over a time X is not a window it takes.
    assert_eq!(data.wanted(smoothed(Smooth::Points(3))).smooth, Smooth::Off);
    // No clip or log: the drawn line is the smoothed one, not a copy of it.
    assert!(data.series_shown.as_ref().unwrap().series.is_none());
}

/// A window of time is as long in each kind of X: days, milli-, micro- and
/// nanoseconds since the epoch, nanoseconds since midnight.
#[test]
fn a_window_of_time_in_each_kind_of_x() {
    for (kind, per_second) in [
        (XAxisTemporalKind::Date, 1.0 / 86_400.0),
        (XAxisTemporalKind::DatetimeMs, 1e3),
        (XAxisTemporalKind::DatetimeUs, 1e6),
        (XAxisTemporalKind::DatetimeNs, 1e9),
        (XAxisTemporalKind::Time, 1e9),
        (XAxisTemporalKind::Duration, 1.0),
    ] {
        // A point a day; a 7-day window holds seven of them.
        let day = 86_400.0 * per_second;
        let series = vec![
            (0..10)
                .map(|i| (f64::from(i) * day, f64::from(i)))
                .collect(),
        ];
        let mut data = lines(series, kind);
        let shown = smoothed(Smooth::Seconds(604_800));
        data.keep_shown(shown);
        let (values, _) = data.values(shown);
        assert_eq!(values[0][9].1, 6.0, "{kind:?}: the mean of days 3 to 9");
    }
}

/// One series draws its raw line behind by itself; the axis spans both lines.
#[test]
fn one_series_draws_its_raw_line_behind() {
    let mut data = lines(
        vec![vec![(0.0, 0.0), (1.0, 10.0), (2.0, 0.0), (3.0, 10.0)]],
        XAxisTemporalKind::Numeric,
    );
    let shown = smoothed(Smooth::Points(2));
    data.keep_shown(shown);
    assert_eq!(data.raw_drawn(shown).count(), 1);
    assert_eq!(data.shown_bounds(shown).unwrap(), [0.0, 3.0, 0.0, 10.0]);
    let off = Shown {
        raw: RawLine::Off,
        ..shown
    };
    data.keep_shown(off);
    assert_eq!(data.raw_drawn(off).count(), 0);
    assert_eq!(data.shown_bounds(off).unwrap(), [0.0, 3.0, 0.0, 5.0]);
}

/// A trend with one spike, smoothed over two points.
fn spiked() -> LinesData {
    let mut ys: Vec<f64> = (0..200).map(f64::from).collect();
    ys[100] = 1e6;
    lines(
        vec![ys.iter().enumerate().map(|(x, &y)| (x as f64, y)).collect()],
        XAxisTemporalKind::Numeric,
    )
}

/// With the raw line behind, Range spans both lines without their outliers: the
/// raw spike and the two means it is in go to the edge, and are counted.
#[test]
fn range_with_the_raw_line_spans_both_lines() {
    let mut data = spiked();
    let shown = Shown {
        clip: true,
        ..smoothed(Smooth::Points(2))
    };
    data.keep_shown(shown);
    let clip = data.clip_drawn(shown).expect("the spike");
    assert_eq!((clip.lo, clip.hi), (0.0, 199.0), "the raw trend's ends");
    assert_eq!(clip.outside, 3, "the raw spike and its two means");
    assert_eq!(data.shown_bounds(shown).unwrap()[2..], [0.0, 199.0]);
    let raw = data.raw_drawn(shown).next().unwrap();
    assert_eq!(raw.points[100].1, 199.0, "at the edge");
}

/// Without the raw line, Range clips at the smoothed line's own outliers; a raw
/// spike a smoothed line has averaged away is no reason to clip.
#[test]
fn range_without_the_raw_line_clips_the_smoothed_one() {
    let mut data = spiked();
    let shown = Shown {
        clip: true,
        raw: RawLine::Off,
        ..smoothed(Smooth::Points(2))
    };
    data.keep_shown(shown);
    let clip = data
        .clip_drawn(shown)
        .expect("the two means the spike is in");
    assert_eq!((clip.lo, clip.hi), (0.0, 198.5));
    assert_eq!(clip.outside, 2);

    let mut flat = lines(
        vec![
            (0..100)
                .map(|i| (f64::from(i), f64::from(i % 2) * 100.0))
                .collect(),
        ],
        XAxisTemporalKind::Numeric,
    );
    let shown = Shown {
        clip: true,
        raw: RawLine::Off,
        ..smoothed(Smooth::Points(2))
    };
    flat.keep_shown(shown);
    assert_eq!(flat.clip_drawn(shown), None);
}

/// The windows over time are wider than the gap between points and at most half
/// the span; over numbers, at most half the longest series; three where that many
/// fit. Past that, the next ones up while wider than the gap and no wider than the
/// span; none when nothing is.
#[test]
fn the_ladder_follows_the_spacing_and_the_span() {
    let seconds = |s: &[u32]| s.iter().map(|&s| Smooth::Seconds(s)).collect::<Vec<_>>();
    // A point a second for an hour, as a duration X in seconds.
    let xs: Vec<f64> = (0..3_600).map(f64::from).collect();
    assert_eq!(
        smooth_ladder(&xs, xs.len(), 1.0, XAxisTemporalKind::Duration),
        seconds(&[5, 10, 30, 60, 300, 900])
    );
    // A day a point for a year, as dates.
    let days: Vec<f64> = (0..365).map(f64::from).collect();
    assert_eq!(
        smooth_ladder(&days, days.len(), 1.0, XAxisTemporalKind::Date),
        seconds(&[604_800, 2_592_000, 7_776_000])
    );
    // Ten days: a week, not a month that would only be the mean so far.
    assert_eq!(
        smooth_ladder(&days[..10], 10, 1.0, XAxisTemporalKind::Date),
        seconds(&[604_800])
    );
    // Monthly for two years: no 30d, not wider than the gap.
    let months: Vec<f64> = (0..24).map(|m| f64::from(m) * 30.44).collect();
    assert_eq!(
        smooth_ladder(&months, 24, 1.0, XAxisTemporalKind::Date),
        seconds(&[7_776_000, 31_536_000])
    );
    // A minute apart in milliseconds over ten minutes.
    let ms: Vec<f64> = (0..10).map(|i| f64::from(i) * 60_000.0).collect();
    assert_eq!(
        smooth_ladder(&ms, ms.len(), 1.0, XAxisTemporalKind::DatetimeMs),
        seconds(&[300])
    );
    // Two points a day apart: nothing to average.
    assert!(smooth_ladder(&[0.0, 1.0], 2, 1.0, XAxisTemporalKind::Date).is_empty());
    let points = |n: &[u32]| n.iter().map(|&n| Smooth::Points(n)).collect::<Vec<_>>();
    assert_eq!(
        smooth_ladder(&[], 50, 1.0, XAxisTemporalKind::Numeric),
        points(&[3, 5, 10, 20])
    );
    assert_eq!(
        smooth_ladder(&[], 5, 1.0, XAxisTemporalKind::Numeric),
        points(&[3, 5])
    );
    assert!(smooth_ladder(&[], 2, 1.0, XAxisTemporalKind::Numeric).is_empty());
}

/// An envelope offers only the counts of more rows than its biggest step holds,
/// since a window within one step draws that step's mean whatever its size.
#[test]
fn an_envelope_offers_windows_past_its_steps() {
    let points = |n: &[u32]| n.iter().map(|&n| Smooth::Points(n)).collect::<Vec<_>>();
    // 1,000 steps, each a low point carrying its rows and a high point carrying none.
    let envelope = |rows: f64| {
        let series = vec![(0..2_000).map(|i| (f64::from(i / 2), 0.0)).collect()];
        let weights = (0..2_000)
            .map(|i| if i % 2 == 0 { (0.0, rows) } else { (0.0, 0.0) })
            .collect();
        lines(series, XAxisTemporalKind::Numeric).weighed(Some(vec![weights]))
    };
    let small = envelope(30.0);
    assert_eq!(small.ladder, points(&[50, 100, 200]));
    assert!(!small.steps_too_big);
    let big = envelope(5_000.0);
    assert!(big.ladder.is_empty());
    assert!(
        big.steps_too_big,
        "dimmed for its steps, not for too few points"
    );
}

fn log_x() -> Shown {
    Shown {
        log_x: true,
        ..Shown::default()
    }
}

/// A log X draws `log10(x)`, leaves out the points at X of 0 or less and counts
/// them, and the crosshair stands only on the Xs drawn, reading their values.
#[test]
fn a_log_x_draws_log10_and_leaves_out_zero_and_below() {
    let mut data = lines(
        vec![
            vec![
                (-1.0, 5.0),
                (0.0, 6.0),
                (1.0, 7.0),
                (10.0, 8.0),
                (1000.0, 9.0),
            ],
            vec![(0.0, 1.0), (100.0, 2.0)],
        ],
        XAxisTemporalKind::Numeric,
    );
    let shown = log_x();
    data.keep_shown(shown);
    let drawn: Vec<Vec<(f64, f64)>> = data.drawn(shown).map(|s| s.points.to_vec()).collect();
    assert_eq!(
        drawn,
        [vec![(0.0, 7.0), (1.0, 8.0), (3.0, 9.0)], vec![(2.0, 2.0)]]
    );
    assert_eq!(data.left_out(shown).x, 3);
    assert_eq!(data.shown_bounds(shown).unwrap(), [0.0, 3.0, 2.0, 9.0]);
    assert_eq!(data.stops(shown), [1.0, 10.0, 100.0, 1000.0]);
    let (values, _) = data.values(shown);
    assert_eq!(
        crate::widgets::crosshair::values_at(values, 10.0),
        [Some(8.0), None]
    );
    // Off, every point and every X again.
    assert_eq!(data.left_out(Shown::default()).x, 0);
    assert_eq!(data.stops(Shown::default()).len(), 6);
    // A time X never takes it.
    let dates = lines(vec![vec![(1.0, 1.0)]], XAxisTemporalKind::Date);
    assert!(!dates.wanted(shown).log_x);
}

/// A run left out breaks the line where it was, and the gaps the series had stay
/// where they fall among the points kept.
#[test]
fn points_left_out_break_the_line() {
    let series = vec![vec![
        (1.0, 1.0),
        (2.0, 2.0),
        (-1.0, 0.0),
        (3.0, 3.0),
        (4.0, 4.0),
        (5.0, 5.0),
    ]];
    let positive = Drawable::of(&series, &[vec![4]], true, false);
    assert_eq!(positive.kept, [vec![0, 1, 3, 4, 5]]);
    assert_eq!(positive.breaks, [vec![2, 3]]);
    assert_eq!(positive.left_out.x, 1);
    // Left out at the start, as a sorted X leaves them: no break.
    let start = Drawable::of(
        &[vec![(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]],
        &[vec![]],
        true,
        false,
    );
    assert_eq!((start.kept[0].len(), start.breaks[0].len()), (2, 0));
}

/// Smoothing reads every point, the ones a log X leaves out included; the range
/// is found among the points drawn.
#[test]
fn a_log_x_smooths_in_data_x_and_clips_what_it_draws() {
    let mut data = lines(
        vec![vec![(0.0, 100.0), (1.0, 0.0), (10.0, 0.0)]],
        XAxisTemporalKind::Numeric,
    );
    let shown = Shown {
        log_x: true,
        smooth: Smooth::Points(2),
        raw: RawLine::Off,
        ..Shown::default()
    };
    data.keep_shown(shown);
    let drawn: Vec<f64> = data.drawn(shown).flat_map(|s| ys(s.points)).collect();
    assert_eq!(drawn, [50.0, 0.0]);
    assert_eq!(data.left_out(shown).x, 1);

    // An outlier at X = 0 is left out, so the drawn points have none.
    let mut ys_only: Vec<(f64, f64)> = (1..=40)
        .map(|i| (f64::from(i), 10.0 + f64::from(i % 3)))
        .collect();
    ys_only.push((0.0, 1e6));
    let mut data = lines(vec![ys_only], XAxisTemporalKind::Numeric);
    assert!(data.y_clip.is_some(), "the outlier counts with every point");
    let clipped = Shown {
        log_x: true,
        clip: true,
        ..Shown::default()
    };
    data.keep_shown(clipped);
    assert_eq!(data.clip_drawn(clipped), None);
}

fn log_y() -> Shown {
    Shown {
        log: true,
        ..Shown::default()
    }
}

/// A log Y draws `log10(y)`, leaves out the points at Y of 0 or less and counts
/// them, breaks the line where they were, and the crosshair stands only where a
/// point is drawn, reading the data's values.
#[test]
fn a_log_y_draws_log10_and_leaves_out_zero_and_below() {
    let mut data = lines(
        vec![vec![
            (1.0, 10.0),
            (2.0, 0.0),
            (3.0, 100.0),
            (4.0, -5.0),
            (5.0, 1000.0),
        ]],
        XAxisTemporalKind::Numeric,
    );
    let shown = log_y();
    data.keep_shown(shown);
    let drawn: Vec<_> = data.drawn(shown).collect();
    assert_eq!(drawn[0].points, [(1.0, 1.0), (3.0, 2.0), (5.0, 3.0)]);
    assert_eq!(
        drawn[0].breaks,
        [1, 2],
        "each point left out breaks the line"
    );
    assert_eq!(
        data.left_out(shown),
        LeftOut {
            y: 2,
            ..LeftOut::default()
        }
    );
    assert_eq!(data.shown_bounds(shown).unwrap(), [1.0, 5.0, 1.0, 3.0]);
    assert_eq!(data.stops(shown), [1.0, 3.0, 5.0]);
    let (values, _) = data.values(shown);
    assert_eq!(
        crate::widgets::crosshair::values_at(values, 3.0),
        [Some(100.0)]
    );
    assert_eq!(data.left_out(Shown::default()), LeftOut::default());
    assert_eq!(data.stops(Shown::default()).len(), 5);

    // A series with nothing above 0 is named; one with some is drawn.
    let mut two = lines(
        vec![vec![(1.0, 0.0), (2.0, -1.0)], vec![(1.0, 5.0)]],
        XAxisTemporalKind::Numeric,
    );
    two.names = vec!["a".to_string(), "b".to_string()];
    two.keep_shown(shown);
    assert_eq!(two.emptied(shown), ["a"]);
}

/// On both log axes a point is left out once, by X first: the counts stay apart
/// and the note says them as one.
#[test]
fn both_log_axes_count_each_point_once() {
    let mut data = lines(
        vec![vec![(0.0, 0.0), (-1.0, 5.0), (1.0, 0.0), (10.0, 10.0)]],
        XAxisTemporalKind::Numeric,
    );
    let shown = Shown {
        log: true,
        log_x: true,
        ..Shown::default()
    };
    data.keep_shown(shown);
    let left_out = data.left_out(shown);
    assert_eq!((left_out.x, left_out.y), (2, 1));
    assert_eq!(
        left_out.notes(),
        ["3 points with X or Y of 0 or less left out"]
    );
    let drawn: Vec<_> = data.drawn(shown).flat_map(|s| s.points.to_vec()).collect();
    assert_eq!(drawn, [(1.0, 1.0)]);
}

/// The note names the axis and what was counted: points, an envelope's rows, or
/// its values over several Y columns.
#[test]
fn left_out_notes_name_the_axis_and_the_unit() {
    let note = |x, y, rows, columns| {
        LeftOut {
            x,
            y,
            rows,
            columns,
        }
        .notes()
    };
    assert!(note(0, 0, false, 1).is_empty());
    assert_eq!(
        note(1, 0, false, 1),
        ["1 point with X of 0 or less left out"]
    );
    assert_eq!(
        note(0, 1200, false, 1),
        ["1,200 points with Y of 0 or less left out"]
    );
    assert_eq!(note(0, 3, true, 1), ["3 rows with Y of 0 or less left out"]);
    assert_eq!(
        note(2, 3, true, 1),
        ["5 rows with X or Y of 0 or less left out"]
    );
    assert_eq!(
        note(2, 3, true, 2),
        [
            "2 rows with X of 0 or less left out",
            "3 values with Y of 0 or less left out"
        ]
    );
}

/// Smoothing reads every point, those at 0 or less included: a mean above 0 is
/// drawn where the raw value is not. The raw line behind keeps its own points,
/// and the note counts the data's.
#[test]
fn a_log_y_smooths_the_data_and_each_line_keeps_its_own_points() {
    let mut data = lines(
        vec![vec![(1.0, 4.0), (2.0, 0.0), (3.0, -2.0), (4.0, 8.0)]],
        XAxisTemporalKind::Numeric,
    );
    let shown = Shown {
        log: true,
        smooth: Smooth::Points(2),
        raw: RawLine::On,
        ..Shown::default()
    };
    data.keep_shown(shown);
    // Means: 4, 2, -1, 3; the -1 is left out of the smoothed line.
    let line: Vec<_> = data.drawn(shown).collect();
    let expect = [
        (1.0, 4f64.log10()),
        (2.0, 2f64.log10()),
        (4.0, 3f64.log10()),
    ];
    assert_eq!(line[0].points, expect);
    assert_eq!(line[0].breaks, [2]);
    let raw: Vec<_> = data.raw_drawn(shown).collect();
    assert_eq!(raw[0].points, [(1.0, 4f64.log10()), (4.0, 8f64.log10())]);
    assert_eq!(raw[0].breaks, [1]);
    assert_eq!(data.left_out(shown).y, 2, "the data's points");
    // The crosshair stands where either line draws.
    assert_eq!(data.stops(shown), [1.0, 2.0, 4.0]);
}

/// On a log Y the range is found among the points drawn: a 0 among them is no
/// outlier to clip to.
#[test]
fn a_log_y_clips_what_it_draws() {
    let mut ys_only: Vec<(f64, f64)> = (1..=40)
        .map(|i| (f64::from(i), 10.0 + f64::from(i % 3)))
        .collect();
    ys_only.push((41.0, 0.0));
    let mut data = lines(vec![ys_only.clone()], XAxisTemporalKind::Numeric);
    assert!(
        data.y_clip.is_some(),
        "the 0 is an outlier among every point"
    );
    let clipped = Shown {
        log: true,
        clip: true,
        ..Shown::default()
    };
    data.keep_shown(clipped);
    assert_eq!(data.clip_drawn(clipped), None);

    // An outlier above 0 is clipped, and drawn at the edge on the log.
    ys_only.push((42.0, 1e6));
    let mut data = lines(vec![ys_only], XAxisTemporalKind::Numeric);
    data.keep_shown(clipped);
    let clip = data.clip_drawn(clipped).unwrap();
    assert_eq!(clip.outside, 1);
    let top = data.shown_bounds(clipped).unwrap()[3];
    assert_eq!(top, clip.hi.log10());
}

/// An envelope's points stand for rows: what a log scale leaves out of it is what
/// it counted as it read, said in rows.
#[test]
fn an_envelope_leaves_out_rows() {
    let data = lines(
        vec![vec![(1.0, 0.0), (10.0, 5.0)]],
        XAxisTemporalKind::Numeric,
    )
    .weighed(Some(vec![vec![(0.0, 4.0), (5.0, 1.0)]]))
    .leaving_out(2, 3);
    let both = Shown {
        log: true,
        log_x: true,
        ..Shown::default()
    };
    assert_eq!(
        data.left_out(both),
        LeftOut {
            x: 2,
            y: 3,
            rows: true,
            columns: 1
        }
    );
    assert_eq!(data.left_out(log_y()).x, 0, "X counts only on a log X");
    assert_eq!(data.left_out(Shown::default()), LeftOut::default());
}

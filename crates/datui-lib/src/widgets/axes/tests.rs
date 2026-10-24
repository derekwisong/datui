use super::*;
use crate::chart::chart_data::{XAxisTemporalKind, x_axis_label_at};
use ratatui::widgets::{Dataset, GraphType};

/// Days since the epoch of 2020-01-01 and 2024-12-31.
const FIVE_YEARS: [f64; 2] = [18262.0, 20088.0];

/// Date labels for fixed ticks on an axis spanning `bounds`.
fn dates(bounds: (f64, f64)) -> TickLabel<'static> {
    let numbers = AxisFormat::new(&[], &AxisNumbers::default());
    Box::new(move |v, level| x_axis_label_at(v, XAxisTemporalKind::Date, bounds, level, &numbers))
}

/// A plot starting four cells in, past the y labels and the axis, one dot a cell.
fn track(width: u16) -> Track {
    Track {
        start: 4,
        cells: width - 4,
        sub: 1,
    }
}

fn labels_on(axis: &AxisSpec<'_>, width: u16) -> Vec<String> {
    fit_x_labels(axis, (0, width), track(width))
        .labels
        .into_iter()
        .map(|(_, l)| l)
        .collect()
}

/// Fixed ticks drop the middle first, then shorten; the ends stay while anything
/// fits.
#[test]
fn fixed_labels_drop_the_middle_then_shorten() {
    let [lo, hi] = FIVE_YEARS;
    let axis = AxisSpec::ends_and_middle(FIVE_YEARS, dates((lo, hi)), "");
    assert_eq!(
        labels_on(&axis, 40),
        ["2020-01-01", "2022-07-02", "2024-12-31"]
    );
    assert_eq!(labels_on(&axis, 24), ["2020-01-01", "2024-12-31"]);
    assert_eq!(labels_on(&axis, 17), ["2020-01", "2024-12"]);
    assert_eq!(labels_on(&axis, 16), ["2020", "2024"]);
    assert!(labels_on(&axis, 10).is_empty());
}

/// Placed labels stand apart, never leave the row, and keep the ends longest.
#[test]
fn labels_stand_apart_on_the_row() {
    let names = |i: f64, level| (level == 0).then(|| format!("column_{i}"));
    let axis = AxisSpec::fixed(
        [-0.5, 6.5],
        (0..7).map(f64::from).collect(),
        Box::new(names),
        "",
    );
    for width in 10..120 {
        let placed = fit_x_labels(&axis, (0, width), track(width)).labels;
        for pair in placed.windows(2) {
            let (x, label) = &pair[0];
            assert!(
                x + label.width() as u16 + LABEL_GAP <= pair[1].0,
                "{width}: {placed:?}"
            );
        }
        if let Some((x, label)) = placed.last() {
            assert!(x + label.width() as u16 <= width, "{width}: {placed:?}");
            assert_eq!(label, "column_6", "the last end stays: {placed:?}");
            assert_eq!(placed[0].1, "column_0", "the first end stays: {placed:?}");
        }
    }
}

/// The step behind a set of labels, when they are evenly stepped numbers.
fn step_of(labels: &[String]) -> f64 {
    let values: Vec<f64> = labels.iter().map(|l| l.parse().unwrap()).collect();
    let step = values[1] - values[0];
    for w in values.windows(2) {
        assert!((w[1] - w[0] - step).abs() < 1e-9, "uneven: {labels:?}");
    }
    step
}

/// 1, 2, 2.5 or 5 times a power of ten, and every value a multiple of it.
fn assert_nice(labels: &[String]) {
    let step = step_of(labels);
    let mantissa = step / 10f64.powf(step.log10().floor());
    assert!(
        [1.0, 2.0, 2.5, 5.0]
            .iter()
            .any(|m| (mantissa - m).abs() < 1e-9),
        "step {step}: {labels:?}"
    );
    for l in labels {
        let k = l.parse::<f64>().unwrap() / step;
        assert!((k - k.round()).abs() < 1e-9, "{l} off the step {step}");
    }
}

/// The x axis carries about one label per 15 columns, every one a nice value:
/// three or so at 60 columns, near twenty at 300.
#[test]
fn x_tick_density_follows_the_width() {
    let axis = AxisSpec::numbers([3.0, 997.0], &AxisNumbers::default(), "");
    for (width, least, most) in [(40, 2, 4), (60, 3, 5), (120, 6, 10), (300, 15, 22)] {
        let labels = labels_on(&axis, width);
        assert!(
            (least..=most).contains(&labels.len()),
            "{width} columns: {labels:?}"
        );
        assert_nice(&labels);
    }
    // Minor ticks between the labeled ones, where there is room.
    let wide = fit_x_labels(&axis, (0, 400), track(400));
    assert!(wide.minors.len() >= wide.majors.len(), "{wide:?}");
    let narrow = fit_x_labels(&axis, (0, 40), track(40));
    assert!(narrow.minors.iter().all(|m| !narrow.majors.contains(m)));
}

/// A y axis widens to the nice values either side of its data and labels about
/// one row in four.
#[test]
fn y_tick_density_follows_the_height() {
    let axis = AxisSpec::y_numbers([3.0, 997.0], &AxisNumbers::default(), "");
    for rows in [12u16, 20, 40, 76] {
        let track = Track {
            start: 0,
            cells: rows,
            sub: 4,
        };
        let placed = fit_y_labels(&axis, track, 20);
        let labels: Vec<String> = placed
            .labels
            .iter()
            .map(|(_, l)| l.trim().to_string())
            .collect();
        let per_label = f64::from(rows) / labels.len() as f64;
        assert!((2.2..=7.0).contains(&per_label), "{rows} rows: {labels:?}");
        assert_nice(&labels);
        // Bottom up, the ends of the widened axis on the last and first rows.
        assert_eq!(placed.labels[0], (rows - 1, "0".to_string()), "{placed:?}");
        let (top, label) = placed.labels.last().unwrap();
        assert_eq!(*top, 0, "{rows} rows: {placed:?}");
        assert_eq!(placed.bounds[0], 0.0);
        assert!(placed.bounds[1] >= 997.0);
        assert_eq!(&placed.bounds[1].to_string(), label);
    }
}

/// Whole numbers tick whole, however much room there is.
#[test]
fn whole_numbers_tick_whole() {
    let whole = AxisNumbers {
        whole: true,
        ..AxisNumbers::default()
    };
    let axis = AxisSpec::numbers([0.0, 7.0], &whole, "");
    assert_eq!(
        labels_on(&axis, 200),
        ["0", "1", "2", "3", "4", "5", "6", "7"]
    );
    // Narrow: more labels when they fit, not the two of the nearest step.
    assert_eq!(labels_on(&axis, 30), ["0", "2", "4", "6"]);
    assert_eq!(labels_on(&axis, 20), ["0", "5"]);
    let axis = AxisSpec::y_numbers([3.0, 10.0], &whole, "");
    let track = Track {
        start: 0,
        cells: 9,
        sub: 1,
    };
    let placed = fit_y_labels(&axis, track, 10);
    assert_eq!(
        placed.labels,
        [
            (8, "0".to_string()),
            (4, "5".to_string()),
            (0, "10".to_string())
        ]
    );
}

/// A date axis ticks on calendar boundaries and labels the unit that turns: the
/// years on a narrow plot, half years between them on a wide one; the dates at
/// the ends when nothing else fits.
#[test]
fn date_ticks_fall_on_the_calendar() {
    let axis = AxisSpec::calendar(
        FIVE_YEARS,
        XAxisTemporalKind::Date,
        &AxisNumbers::default(),
        "",
    );
    assert_eq!(
        labels_on(&axis, 80),
        ["2020", "2021", "2022", "2023", "2024"]
    );
    let wide = labels_on(&axis, 300);
    assert_eq!(
        wide[..4],
        ["2020", "Apr", "Jul", "Oct"],
        "quarters, the year at its turn: {wide:?}"
    );
    assert!(wide.contains(&"2024".to_string()), "{wide:?}");
    let narrow = labels_on(&axis, 16);
    assert_eq!(narrow, ["2020", "2024"], "{narrow:?}");

    // Inside a month, days; the first tick names the month and year.
    let march = [19783.0, 19813.0]; // 2024-03-01 to 2024-03-31
    let axis = AxisSpec::calendar(march, XAxisTemporalKind::Date, &AxisNumbers::default(), "");
    let labels = labels_on(&axis, 80);
    assert_eq!(labels[0], "Mar 1 2024", "{labels:?}");
    assert!(
        labels[1..].iter().all(|l| l.parse::<u32>().is_ok()),
        "{labels:?}"
    );

    // A day of microsecond timestamps ticks on the hours.
    let day = [1_709_251_200e6, 1_709_337_600e6]; // 2024-03-01T00:00 to 03-02T00:00
    let axis = AxisSpec::calendar(
        day,
        XAxisTemporalKind::DatetimeUs,
        &AxisNumbers::default(),
        "",
    );
    let labels = labels_on(&axis, 120);
    assert_eq!(labels[0], "Mar 1 2024", "{labels:?}");
    assert!(labels.contains(&"12:00".to_string()), "{labels:?}");
    assert_eq!(labels.last().unwrap(), "Mar 2", "{labels:?}");
}

/// A short form that says the same at both ends is no label: numbers that round
/// alike show none.
#[test]
fn short_forms_tell_ticks_apart() {
    let axis = AxisSpec::numbers([1000.1, 1000.3], &AxisNumbers::default(), "");
    assert!(labels_on(&axis, 12).is_empty());
    assert_eq!(labels_on(&axis, 40), ["1000.1", "1000.2", "1000.3"]);
}

fn axes<'a>(grid: bool) -> PlotAxes<'a> {
    PlotAxes {
        grid: grid.then(Style::default),
        ..PlotAxes::new(
            AxisSpec::numbers([0.0, 10.0], &AxisNumbers::default(), "x title"),
            AxisSpec::y_numbers([0.0, 1000.0], &AxisNumbers::default(), "y title"),
            Style::default(),
            Marker::Braille,
        )
    }
}

fn render_with(axes: &PlotAxes<'_>, area: Rect, g: &Glyphs) -> (Buffer, PlotFrame) {
    let points = [(0.0, 0.0), (10.0, 1000.0)];
    let chart = Chart::new(vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .data(&points),
    ]);
    let mut buf = Buffer::empty(area);
    let frame = axes.render(chart, area, &mut buf, g);
    (buf, frame)
}

fn render(area: Rect, g: &Glyphs) -> (Buffer, PlotFrame) {
    render_with(&axes(false), area, g)
}

/// The frame datui computes is where ratatui draws: the axis corner sits just
/// left of the plot and just under it.
#[test]
fn the_frame_matches_ratatuis_layout() {
    let g = crate::glyphs::unicode();
    for (w, h) in [(9, 6), (20, 5), (20, 7), (30, 12), (60, 20), (120, 40)] {
        let (buf, frame) = render(Rect::new(0, 0, w, h), g);
        let corner = (frame.graph.left() - 1, frame.graph.bottom());
        assert_eq!(buf[corner].symbol(), "└", "{w}x{h}: {:?}", frame.graph);
    }
}

/// The titles take rows of their own while the plot keeps three rows, the x
/// title the first to go.
#[test]
fn titles_give_up_their_rows_to_the_plot() {
    let g = crate::glyphs::unicode();
    let (_, frame) = render(Rect::new(0, 0, 30, 7), g);
    assert!(frame.y_title.is_some() && frame.x_title.is_some());
    assert_eq!(frame.graph.height, 3);
    let (_, frame) = render(Rect::new(0, 0, 30, 6), g);
    assert!(frame.y_title.is_some() && frame.x_title.is_none());
    let (_, frame) = render(Rect::new(0, 0, 30, 5), g);
    assert!(frame.y_title.is_none() && frame.x_title.is_none());
}

/// Each tick carries a mark on its axis line, and its label sits beside it: the
/// y label on the tick's row, the x label centered under its column.
#[test]
fn labels_sit_on_their_tick_marks() {
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        let (buf, frame) = render(Rect::new(0, 0, 60, 20), g);
        let axis_x = frame.graph.left() - 1;
        for (row, label) in &frame.y.labels {
            assert_eq!(buf[(axis_x, *row)].symbol(), g.plot.tick_y, "{label}");
            let text: String = (frame.chart.left()..axis_x)
                .map(|x| buf[(x, *row)].symbol())
                .collect();
            assert_eq!(text.trim(), label.trim());
        }
        let row = frame.graph.bottom();
        let ticks: Vec<u16> = (frame.graph.left()..frame.graph.right())
            .filter(|&x| buf[(x, row)].symbol() == g.plot.tick_x)
            .collect();
        assert!(ticks.len() >= 3, "{ticks:?}");
    }
}

/// The grid draws at the major ticks when on and not at all when off, in either
/// glyph set, and never takes a cell the series drew in.
#[test]
fn the_grid_toggles_and_never_hides_a_series() {
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        let area = Rect::new(0, 0, 60, 20);
        let (off, frame) = render_with(&axes(false), area, g);
        let (on, _) = render_with(&axes(true), area, g);
        let grid = |buf: &Buffer| {
            let all = crate::tests::buffer_lines(buf).concat();
            all.matches(g.plot.grid_across).count() + all.matches(g.plot.grid_down).count()
        };
        assert_eq!(grid(&off), 0, "{:#?}", crate::tests::buffer_lines(&off));
        assert!(grid(&on) > 50, "{:#?}", crate::tests::buffer_lines(&on));
        let graph = frame.graph;
        for y in graph.top()..graph.bottom() {
            for x in graph.left()..graph.right() {
                let mark = off[(x, y)].symbol();
                if !matches!(mark, " " | "\u{2800}") {
                    assert_eq!(on[(x, y)].symbol(), mark, "({x}, {y})");
                }
            }
        }
        // Down at each labeled x tick but the one against the y axis; across at
        // each y tick but the one on the x axis.
        for (row, _) in &frame.y.labels {
            let across = (graph.left()..graph.right())
                .filter(|&x| on[(x, *row)].symbol() == g.plot.grid_across)
                .count();
            if row + 1 < graph.bottom() {
                assert!(across > graph.width as usize / 2, "row {row}");
            } else {
                assert_eq!(across, 0, "the x axis row");
            }
        }
    }
}

fn two_names() -> Legend {
    Legend {
        entries: vec![
            ("first".to_string(), Style::default()),
            ("second".to_string(), Style::default()),
        ],
    }
}

/// The legend has no frame: a swatch and a name per series on the plot's
/// background, cleared of the marks under it. A line rising to the right leaves
/// the top left.
#[test]
fn the_legend_takes_the_emptiest_corner_without_a_frame() {
    let g = crate::glyphs::unicode();
    let mut axes = axes(false);
    axes.legend = Some(two_names());
    let rising: Vec<(f64, f64)> = (0..=100)
        .map(|i| (f64::from(i) / 10.0, f64::from(i) * 10.0))
        .collect();
    let chart = Chart::new(vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .data(&rising),
    ]);
    let area = Rect::new(0, 0, 60, 24);
    let mut buf = Buffer::empty(area);
    let frame = axes.render(chart, area, &mut buf, g);
    let (x, y) = (frame.graph.left(), frame.graph.top());
    let row = |y: u16| -> String {
        (x..x + 10)
            .map(|x| buf[(x, y)].symbol())
            .collect::<String>()
    };
    assert_eq!(
        row(y),
        format!(" {} first  ", g.bar_eighths[7]),
        "{:#?}",
        crate::tests::buffer_lines(&buf)
    );
    assert_eq!(row(y + 1), format!(" {} second ", g.bar_eighths[7]));
    let all = crate::tests::buffer_lines(&buf).join("\n");
    for frame_mark in ["┌", "┐", "┘"] {
        assert!(!all.contains(frame_mark), "{all}");
    }
}

/// Points in every corner: the legend goes where they are fewest, counted by the
/// dots they set, not by the cells they touch.
#[test]
fn the_legend_avoids_a_dense_corner() {
    let g = crate::glyphs::unicode();
    let mut axes = axes(false);
    axes.legend = Some(two_names());
    // A dense cloud over the plot but the bottom left, which has a few points:
    // every place the legend could go touches some.
    let mut points = Vec::new();
    for i in 0..=100 {
        for j in 0..=100 {
            let (x, y) = (f64::from(i) / 10.0, f64::from(j) * 10.0);
            if x > 3.0 || y > 300.0 {
                points.push((x, y));
            }
        }
    }
    for i in 0..10 {
        points.push((f64::from(i) * 0.3, f64::from(i) * 30.0));
    }
    let chart = Chart::new(vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Scatter)
            .data(&points),
    ]);
    let area = Rect::new(0, 0, 60, 24);
    let mut buf = Buffer::empty(area);
    let frame = axes.render(chart, area, &mut buf, g);
    let swatch = (frame.graph.left() + 1, frame.graph.bottom() - 2);
    assert_eq!(
        buf[swatch].symbol(),
        g.bar_eighths[7],
        "{:#?}",
        crate::tests::buffer_lines(&buf)
    );
}

/// Legend names of several Color columns differ at their ends: a wide plot draws
/// them whole, past the small plot's cap, and a narrow one cuts them from the
/// middle, so they still read apart.
#[test]
fn long_legend_names_grow_with_the_plot_and_keep_their_ends() {
    let g = crate::glyphs::unicode();
    let names = [
        "FlightSimulator2024 · 2026-10-18T09:00:00",
        "FlightSimulator2024 · 2026-10-18T21:30:00",
        "FlightSimulator2024 · 2026-10-19T08:15:00",
    ];
    let drawn = |width: u16| -> Vec<String> {
        let mut axes = axes(false);
        axes.legend = Some(Legend {
            entries: names
                .iter()
                .map(|n| (n.to_string(), Style::default()))
                .collect(),
        });
        let chart = Chart::new(vec![Dataset::default().data(&[])]);
        let area = Rect::new(0, 0, width, 24);
        let mut buf = Buffer::empty(area);
        axes.render(chart, area, &mut buf, g);
        let lines = crate::tests::buffer_lines(&buf);
        lines
            .iter()
            .filter_map(|line| {
                let at = line.find(g.bar_eighths[7])?;
                Some(line[at + g.bar_eighths[7].len()..].trim().to_string())
            })
            .collect()
    };
    assert_eq!(drawn(200), names, "whole on a wide plot");
    let narrow = drawn(60);
    assert_eq!(narrow.len(), 3, "{narrow:?}");
    for (cut, name) in narrow.iter().zip(names) {
        assert!(cut.width() < name.width(), "{cut}");
        assert!(cut.contains(g.ellipsis), "{cut}");
        assert!(name.ends_with(&cut[cut.rfind(g.ellipsis).unwrap() + g.ellipsis.len()..]));
    }
    let mut distinct = narrow.clone();
    distinct.dedup();
    assert_eq!(distinct.len(), 3, "{narrow:?}");
}

/// Names are cut at the end while that keeps them apart, and from the middle only
/// when it would not, in the glyph set given.
#[test]
fn names_are_cut_where_they_stay_apart() {
    let g = crate::glyphs::ascii();
    assert_eq!(
        fit_names(&["Cyberpunk 2077", "Doom Eternal"], 8, g),
        ["Cyber...", "Doom ..."]
    );
    assert_eq!(
        fit_names(&["game · 2026-10-18", "game · 2026-10-19"], 12, g),
        ["game ...0-18", "game ...0-19"]
    );
    // Names alike to begin with are left as an end cut would leave them.
    assert_eq!(fit_names(&["same", "same"], 3, g), ["...", "..."]);
}

/// On a narrow plot of 0 to 7 the x row reads `0 2 4 6`: more labels when they
/// fit, rather than the two of the step nearest the spacing.
#[test]
fn a_narrow_axis_takes_more_labels_when_they_fit() {
    let g = crate::glyphs::unicode();
    let whole = AxisNumbers {
        whole: true,
        ..AxisNumbers::default()
    };
    let axes = PlotAxes::new(
        AxisSpec::numbers([0.0, 7.0], &whole, ""),
        AxisSpec::y_numbers([0.0, 1000.0], &AxisNumbers::default(), ""),
        Style::default(),
        Marker::Braille,
    );
    let (buf, frame) = render_with(&axes, Rect::new(0, 0, 34, 12), g);
    let row = frame.labels.expect("a label row").y;
    let labels: Vec<String> = crate::tests::buffer_lines(&buf)[row as usize]
        .split_whitespace()
        .map(str::to_string)
        .collect();
    assert_eq!(
        labels,
        ["0", "2", "4", "6"],
        "{:#?}",
        crate::tests::buffer_lines(&buf)
    );
}

/// A title longer than its row is cut with the set's ellipsis.
#[test]
fn a_long_title_is_cut_to_its_row() {
    assert_eq!(cut("a long title", 6, crate::glyphs::unicode()), "a lon…");
    assert_eq!(cut("a long title", 6, crate::glyphs::ascii()), "a l...");
    assert_eq!(cut("short", 6, crate::glyphs::ascii()), "short");
}

/// A label of a log axis as a number: `2,000`, `5k`, `1e-9`.
fn log_label(label: &str) -> f64 {
    let plain = label.replace(',', "");
    let (digits, unit) = match plain.chars().last() {
        Some('k') => (&plain[..plain.len() - 1], 1e3),
        Some('M') => (&plain[..plain.len() - 1], 1e6),
        _ => (&plain[..], 1.0),
    };
    digits.parse::<f64>().unwrap() * unit
}

/// 1, 2 or 5 times a power of ten.
fn is_one_two_five(v: f64) -> bool {
    let mantissa = v / 10f64.powf((v.log10() + 1e-9).floor());
    [1.0, 2.0, 5.0].iter().any(|m| (mantissa - m).abs() < 1e-6)
}

/// A log X across several decades ticks at powers of ten, with 2 and 5 between
/// when the width has room for them, every tick inside the data, and labels about
/// fifteen columns apart that never touch.
#[test]
fn a_log_x_ticks_decades_and_one_two_five() {
    let (low, high) = (3.0_f64, 70_000.0_f64);
    let axis = AxisSpec::log([low.log10(), high.log10()], &AxisNumbers::default(), "");
    for width in [30u16, 60, 120, 240] {
        let fit = fit_x_labels(&axis, (0, width), track(width));
        let values: Vec<f64> = fit.labels.iter().map(|(_, l)| log_label(l)).collect();
        assert!(values.len() >= 2, "{width}: {:?}", fit.labels);
        assert!(values.len() as f64 <= f64::from(width) / 6.0, "{width}");
        for v in &values {
            assert!(is_one_two_five(*v), "{width}: {v} in {:?}", fit.labels);
            assert!((low..=high).contains(v), "{width}: {v} outside");
        }
        for pair in fit.labels.windows(2) {
            let (x, label) = &pair[0];
            assert!(x + label.width() as u16 + LABEL_GAP <= pair[1].0, "{width}");
        }
    }
    // Wide: 2 and 5 between the powers. Narrow: the powers alone.
    let wide: Vec<f64> = labels_on(&axis, 240).iter().map(|l| log_label(l)).collect();
    assert!(wide.contains(&20.0) && wide.contains(&50.0), "{wide:?}");
    let narrow: Vec<f64> = labels_on(&axis, 60).iter().map(|l| log_label(l)).collect();
    assert!(
        narrow
            .iter()
            .all(|v| (v.log10() - v.log10().round()).abs() < 1e-9),
        "{narrow:?}"
    );
    // Equal ratios sit equal distances apart.
    let fit = fit_x_labels(&axis, (0, 60), track(60));
    let at: Vec<u16> = fit.majors.clone();
    let gaps: Vec<i32> = at
        .windows(2)
        .map(|w| i32::from(w[1]) - i32::from(w[0]))
        .collect();
    assert!(gaps.iter().all(|g| (g - gaps[0]).abs() <= 1), "{at:?}");
}

/// Within a decade the log axis takes round steps, as a number axis does; far
/// under one it writes powers as `1e-9`.
#[test]
fn a_log_x_within_a_decade_and_far_under_one() {
    let axis = AxisSpec::log(
        [150f64.log10(), 900f64.log10()],
        &AxisNumbers::default(),
        "",
    );
    let labels = labels_on(&axis, 80);
    assert!(labels.len() >= 3, "{labels:?}");
    let values: Vec<f64> = labels.iter().map(|l| log_label(l)).collect();
    assert!(
        values.iter().all(|v| (150.0..=900.0).contains(v)),
        "{labels:?}"
    );
    assert!(values.iter().all(|v| v % 50.0 == 0.0), "{labels:?}");

    let tiny = AxisSpec::log([-9.0, -3.0], &AxisNumbers::default(), "");
    let labels = labels_on(&tiny, 120);
    assert!(labels.contains(&"1e-9".to_string()), "{labels:?}");
    assert!(labels.iter().all(|l| l.contains('e')), "{labels:?}");
}

/// A log X writes each label with the digits it needs; past four powers of ten,
/// or a label of more than six cells, every label in a short form, 1 as `1`.
#[test]
fn log_x_labels_take_the_digits_they_need() {
    let labels = |lo: f64, hi: f64, width: u16| {
        let axis = AxisSpec::log([lo.log10(), hi.log10()], &AxisNumbers::default(), "");
        labels_on(&axis, width).join(" ")
    };
    for (lo, hi, width, want) in [
        (1e-9, 1e12, 60, "1e-5 1 1e5 1e10"),
        (1e-9, 1e12, 120, "1e-9 1e-6 1e-3 1 1e3 1e6 1e9 1e12"),
        (1e-3, 1e3, 60, "0.01 1 100"),
        (1e-3, 1e3, 80, "1e-3 1e-2 1e-1 1 1e1 1e2 1e3"),
        (0.01, 100.0, 80, "0.01 0.1 1 10 100"),
        (1.0, 1e9, 80, "1 100 10k 1M 100M"),
        (1.0, 1e9, 120, "1 10 100 1k 10k 100k 1M 10M 100M 1G"),
        // Within a power of ten: round steps, one precision as on any number axis.
        (150.0, 900.0, 80, "250 500 750"),
        (150.0, 900.0, 120, "200 400 600 800"),
        (0.5, 2.0, 80, "0.5 1.0 1.5 2.0"),
        // A single X.
        (100.0, 100.0, 80, "100"),
    ] {
        assert_eq!(labels(lo, hi, width), want, "{lo}..{hi} at {width}");
    }
}

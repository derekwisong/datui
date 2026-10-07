use super::*;
use crate::numfmt::NumberFormat;

fn settings(preset: &str, enabled: bool) -> NumberFormatSettings {
    NumberFormatSettings {
        format: NumberFormat::preset(preset).unwrap(),
        enabled,
        exclude: Vec::new(),
        align_numeric_right: true,
    }
}

fn analysis(mean: f64, std_dev: f64, sorted: Vec<f64>) -> DistributionAnalysis {
    use crate::analysis::statistics::{
        DistributionCharacteristics, OutlierAnalysis, PercentileBreakdown,
    };
    DistributionAnalysis {
        column_name: "close".into(),
        distribution_type: DistributionType::Normal,
        confidence: 0.0,
        characteristics: DistributionCharacteristics {
            shapiro_wilk_stat: None,
            shapiro_wilk_pvalue: None,
            skewness: 0.0,
            kurtosis: 3.0,
            mean,
            median: mean,
            std_dev,
            coefficient_of_variation: std_dev / mean,
        },
        outliers: OutlierAnalysis {
            total_count: 0,
            percentage: 0.0,
            iqr_count: 0,
            zscore_count: 0,
        },
        percentiles: PercentileBreakdown {
            p25: 0.0,
            p50: 0.0,
            p75: 0.0,
            p99: 0.0,
        },
        sorted_sample_values: sorted,
        fits: Vec::new(),
        qq: Vec::new(),
        histogram: Default::default(),
    }
}

/// The histogram's bars sit on the axis they are drawn against: the first bin's
/// bar in the first plot column, and nothing past the chart's right edge. They were
/// shifted right by a bar and a half, onto the next bin and off the end.
/// Busy at the low end, so the first bin has a bar, with a Normal fit.
fn skewed_normal_fit() -> DistributionAnalysis {
    let mut values: Vec<f64> = (0..400).map(|i| 23.0 + (i % 20) as f64).collect();
    values.extend((0..100).map(|i| 23.0 + 3.18 * i as f64));
    values.sort_by(f64::total_cmp);
    let mut dist = analysis(100.0, 80.0, values);
    dist.fits = vec![(
        DistributionType::Normal,
        FitOutcome::Tested(FitTest {
            fitted: crate::analysis::distribution_fit::Fitted::Normal {
                mean: 100.0,
                sd: 80.0,
            },
            p_value: 0.005,
            beyond: 0,
            replicates: 199,
            tested_on: 500,
            aic: 0.0,
        }),
    )];
    dist
}

/// One of the Distribution detail's plots, in a 60x20 area of an 80x20 buffer.
fn render_distribution_plot(
    dist: &DistributionAnalysis,
    g: &crate::glyphs::Glyphs,
    render: fn(DistributionPlotConfig, &mut Buffer),
) -> Buffer {
    render_distribution_plot_in(dist, g, render, 60)
}

fn render_distribution_plot_in(
    dist: &DistributionAnalysis,
    g: &crate::glyphs::Glyphs,
    render: fn(DistributionPlotConfig, &mut Buffer),
    width: u16,
) -> Buffer {
    let numbers = NumberFormatSettings::default();
    render_distribution_plot_as(dist, g, render, width, &numbers)
}

/// The plot with its numbers in `numbers`.
fn render_distribution_plot_as(
    dist: &DistributionAnalysis,
    g: &crate::glyphs::Glyphs,
    render: fn(DistributionPlotConfig, &mut Buffer),
    width: u16,
    numbers: &NumberFormatSettings,
) -> Buffer {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let mut buf = Buffer::empty(Rect::new(0, 0, 80, 20));
    let values = AxisNumbers::measure(numbers, &dist.column_name);
    let counts = AxisNumbers::count(numbers);
    render(
        DistributionPlotConfig {
            dist,
            dist_type: DistributionType::Normal,
            area: Rect::new(0, 0, width, 20),
            shared_y_axis_label_width: 5,
            theme: &theme,
            unified_x_range: Some((23.0, 341.1)),
            histogram_scale: HistogramScale::Linear,
            glyphs: g,
            values: &values,
            counts: &counts,
        },
        &mut buf,
    );
    buf
}

/// The histogram's counts group as the table groups numbers: a bin of thousands
/// reads `9,600`, not `9600`.
#[test]
fn distribution_counts_follow_the_table_number_format() {
    let mut dist = skewed_normal_fit();
    let values = dist.sorted_sample_values.clone();
    dist.sorted_sample_values = values
        .iter()
        .cycle()
        .take(values.len() * 24)
        .copied()
        .collect();
    dist.sorted_sample_values.sort_by(f64::total_cmp);
    let labels = |numbers: &NumberFormatSettings| -> Vec<String> {
        let g = crate::glyphs::unicode();
        let buf = render_distribution_plot_as(&dist, g, render_distribution_histogram, 60, numbers);
        (0..20)
            .filter_map(|y| {
                let row: String = (0..60).map(|x| buf[(x, y)].symbol()).collect();
                let label = row.split_once(['│', '┤'])?.0.trim().to_string();
                (!label.is_empty()).then_some(label)
            })
            .collect()
    };
    let grouped = labels(&settings("thousands", true));
    assert_eq!(grouped.len(), 3, "{grouped:?}");
    assert!(
        grouped[0].contains(',') && grouped[0].len() > 4,
        "{grouped:?}"
    );
    let plain = labels(&settings("thousands", false));
    assert_eq!(plain[0], grouped[0].replace(',', ""), "{plain:?}");
}

/// A histogram is counted and fitted once for a layout: frames that change
/// nothing reuse it, and a new width builds it again.
#[test]
fn a_histogram_is_built_once_per_layout() {
    use crate::analysis::statistics::HISTOGRAMS_BUILT;
    let dist = skewed_normal_fit();
    let g = crate::glyphs::unicode();
    let built = || HISTOGRAMS_BUILT.with(std::cell::Cell::get);
    let before = built();
    for _ in 0..3 {
        render_distribution_plot(&dist, g, render_distribution_histogram);
    }
    assert_eq!(built() - before, 1);
    render_distribution_plot_in(&dist, g, render_distribution_histogram, 70);
    assert_eq!(built() - before, 2);
}

#[test]
fn histogram_bars_stay_on_their_axis() {
    let dist = skewed_normal_fit();
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        let buf = render_distribution_plot(&dist, g, render_distribution_histogram);
        let full = g.plot.column_eighths[7];
        let is_bar = |x: u16| (0..20).any(|y| buf[(x, y)].symbol() == full);
        // Labels, a space, the axis line: the plot's first column.
        assert!(is_bar(5 + 2), "the first bin starts at the axis");
        assert!(
            (60..80).all(|x| !is_bar(x)),
            "nothing is drawn past the chart"
        );
        // The fit's curve is drawn, and goes behind the bars rather than through
        // them: below the top of a bar, every cell is solid.
        let curve = |x: u16, y: u16| PlotMarks::is_mark(g.plot.line, buf[(x, y)].symbol());
        assert!(
            (0..80).any(|x| (0..20).any(|y| curve(x, y) && buf[(x, y)].symbol() != "\u{2800}")),
            "the curve is drawn"
        );
        for x in 0..80 {
            if let Some(top) = (0..20).find(|y| buf[(x, *y)].symbol() == full) {
                assert!(
                    (top..20).all(|y| !curve(x, y)),
                    "a notch in the bar at column {x}"
                );
            }
        }
    }
}

/// The bars span the plot, first column to last, each bin's bar over the columns
/// its values map to: bars of one width stopped up to a bar short of the right
/// end, every bar left of the values the labels give.
#[test]
fn histogram_bars_span_the_plot() {
    let g = crate::glyphs::unicode();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let numbers = NumberFormatSettings::default();
    // Spread evenly over the axis, linear or logarithmic, so every bin has a bar.
    let linear: Vec<f64> = (0..500).map(|i| 23.0 + 318.1 * i as f64 / 499.0).collect();
    let log: Vec<f64> = (0..500)
        .map(|i| 10f64.powf(4.0 * i as f64 / 499.0))
        .collect();
    for (scale, values, range) in [
        (HistogramScale::Linear, linear, (23.0, 341.1)),
        (HistogramScale::Log, log, (1.0, 10_000.0)),
    ] {
        let dist = analysis(100.0, 80.0, values);
        for width in [60u16, 80, 120] {
            // Wider than the plot, to catch a bar drawn past it.
            let mut buf = Buffer::empty(Rect::new(0, 0, width + 10, 20));
            render_distribution_histogram(
                DistributionPlotConfig {
                    dist: &dist,
                    dist_type: DistributionType::Normal,
                    area: Rect::new(0, 0, width, 20),
                    shared_y_axis_label_width: 5,
                    theme: &theme,
                    unified_x_range: Some(range),
                    histogram_scale: scale,
                    glyphs: g,
                    values: &AxisNumbers::measure(&numbers, &dist.column_name),
                    counts: &AxisNumbers::count(&numbers),
                },
                &mut buf,
            );
            let text = crate::tests::buffer_text(&buf);
            let what = format!("{scale:?} at {width}:\n{text}");
            let axis_row = (0..20)
                .rfind(|y| (0..width).any(|x| buf[(x, *y)].symbol() == g.plot.axis.bottom_left))
                .expect(&what);
            let corner = (0..width)
                .find(|x| buf[(*x, axis_row)].symbol() == g.plot.axis.bottom_left)
                .unwrap();
            let (left, right) = (corner + 1, width - 1);
            assert!(
                [g.plot.axis.horizontal, g.plot.tick_x].contains(&buf[(right, axis_row)].symbol()),
                "{what}"
            );
            let is_bar = |x: u16| {
                (0..axis_row).any(|y| g.plot.column_eighths.contains(&buf[(x, y)].symbol()))
            };
            let bars: Vec<u16> = (0..width + 10).filter(|x| is_bar(*x)).collect();
            assert_eq!(
                bars.first(),
                Some(&left),
                "the first bar starts the plot\n{what}"
            );
            assert_eq!(
                bars.last(),
                Some(&right),
                "the last bar ends the plot\n{what}"
            );
            // Each bar and the gap after it, or the last bar alone, take an even share
            // of the plot.
            let mut starts = vec![left];
            starts.extend(bars.windows(2).filter(|w| w[1] > w[0] + 1).map(|w| w[1]));
            let shares: Vec<u16> = starts
                .windows(2)
                .map(|w| w[1] - w[0])
                .chain([right + 1 - starts[starts.len() - 1]])
                .collect();
            let (least, most) = (shares.iter().min().unwrap(), shares.iter().max().unwrap());
            assert!(most - least <= 1, "{shares:?}\n{what}");
        }
    }
}

/// On Log the bins are equal in log space and the labels sit where their values
/// do: over 1 to 10,000 the middle of the axis is 100, not the linear 5,000.
#[test]
fn log_histogram_labels_sit_at_their_values() {
    let values: Vec<f64> = (0..400)
        .map(|i| 10f64.powf(4.0 * i as f64 / 399.0))
        .collect();
    let dist = analysis(1_000.0, 2_000.0, values);
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let numbers = NumberFormatSettings::default();
    let g = crate::glyphs::unicode();
    let mut buf = Buffer::empty(Rect::new(0, 0, 80, 20));
    render_distribution_histogram(
        DistributionPlotConfig {
            dist: &dist,
            dist_type: DistributionType::Normal,
            area: Rect::new(0, 0, 80, 20),
            shared_y_axis_label_width: 5,
            theme: &theme,
            unified_x_range: Some((1.0, 10_000.0)),
            histogram_scale: HistogramScale::Log,
            glyphs: g,
            values: &AxisNumbers::measure(&numbers, &dist.column_name),
            counts: &AxisNumbers::count(&numbers),
        },
        &mut buf,
    );
    let text = crate::tests::buffer_text(&buf);
    let rows: Vec<&str> = text.lines().collect();
    let axis = rows
        .iter()
        .rposition(|r| r.contains(g.plot.axis.bottom_left))
        .expect(&text);
    let row = rows[axis + 1];
    let labels: Vec<(usize, f64)> = row
        .split_whitespace()
        .map(|l| {
            let at = row.find(l).unwrap() + l.len() / 2;
            (at, l.replace(',', "").parse::<f64>().expect(&text))
        })
        .collect();
    assert_eq!(labels.len(), 3, "{text}");
    let [(left, first), (at, middle), (right, last)] = labels[..] else {
        unreachable!()
    };
    assert_eq!((first, middle, last), (1.0, 100.0, 10_000.0), "{text}");
    assert!(
        at.abs_diff((left + right) / 2) <= 1,
        "the middle label is at the axis's middle:\n{text}"
    );
}

/// Under the ASCII set, both of the Distribution detail's plots draw ASCII only:
/// bars, the fit's curve, the Q-Q points and every axis line.
#[test]
fn distribution_plots_are_ascii_under_the_ascii_set() {
    let g = crate::glyphs::ascii();
    let mut dist = skewed_normal_fit();
    let qq: Vec<f64> = (0..dist.sorted_sample_values.len())
        .map(|i| 23.0 + 318.0 * i as f64 / 499.0)
        .collect();
    dist.qq = vec![(DistributionType::Normal, qq)];
    for (name, render) in [
        (
            "histogram",
            render_distribution_histogram as fn(DistributionPlotConfig, &mut Buffer),
        ),
        ("Q-Q plot", render_qq_plot),
    ] {
        let text = crate::tests::buffer_text(&render_distribution_plot(&dist, g, render));
        assert!(text.is_ascii(), "{name}:\n{text}");
        assert!(
            text.contains('|') && text.contains("+-"),
            "{name} axes:\n{text}"
        );
    }
    let qq = crate::tests::buffer_text(&render_distribution_plot(&dist, g, render_qq_plot));
    assert!(qq.contains('*'), "the Q-Q points:\n{qq}");
}

/// The Distribution plots follow the rule every chart does: x labels a space
/// apart with both ends kept, and axis titles on rows that hold nothing else.
#[test]
fn distribution_axes_follow_the_chart_rule() {
    let mut dist = skewed_normal_fit();
    let qq: Vec<f64> = (0..dist.sorted_sample_values.len())
        .map(|i| 23.0 + 318.0 * i as f64 / 499.0)
        .collect();
    dist.qq = vec![(DistributionType::Normal, qq)];
    let is_number = |t: &str| t.trim_end_matches(['k', 'M']).parse::<f64>().is_ok();
    for width in [40, 60, 80] {
        for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
            for (name, render, y_title, x_title) in [
                (
                    "histogram",
                    render_distribution_histogram as fn(DistributionPlotConfig, &mut Buffer),
                    "Counts",
                    None,
                ),
                (
                    "Q-Q plot",
                    render_qq_plot,
                    "Data Values",
                    Some("Theoretical Values"),
                ),
            ] {
                let text = crate::tests::buffer_text(&render_distribution_plot_in(
                    &dist, g, render, width,
                ));
                let rows: Vec<&str> = text.lines().collect();
                let what = format!("{name} at {width}:\n{text}");
                let axis = rows
                    .iter()
                    .rposition(|r| r.contains(g.plot.axis.bottom_left))
                    .expect(&what);
                let labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
                assert!(labels.len() >= 2, "both ends: {what}");
                assert!(labels.iter().all(|l| is_number(l)), "apart: {what}");
                // The plot's title, then the y axis's on a row of its own.
                assert_eq!(rows[1].trim(), y_title, "{what}");
                if let Some(x_title) = x_title {
                    assert_eq!(rows[axis + 2].trim(), x_title, "{what}");
                }
            }
        }
    }
}

#[test]
fn counts_follow_the_data_table_grouping_setting() {
    // A user who turned grouping on should see it wherever they read
    // numbers, not just in the main table.
    assert_eq!(
        format_count(3_088_269, &settings("thousands", true)),
        "3,088,269"
    );
    assert_eq!(
        format_count(3_088_269, &settings("european", true)),
        "3.088.269"
    );
}

#[test]
fn counts_are_raw_when_formatting_is_off() {
    assert_eq!(
        format_count(3_088_269, &settings("thousands", false)),
        "3088269"
    );
    assert_eq!(format_count(0, &settings("thousands", false)), "0");
}

#[test]
fn counts_group_uniformly_with_no_magnitude_threshold() {
    assert_eq!(format_count(42, &settings("thousands", true)), "42");
    assert_eq!(format_count(1000, &settings("thousands", true)), "1,000");
    assert_eq!(format_count(10_000, &settings("thousands", true)), "10,000");
}

fn correlation_matrix(r: f64, pairs: usize) -> crate::analysis::statistics::CorrelationMatrix {
    crate::analysis::statistics::CorrelationMatrix {
        columns: vec!["price".to_string(), "volume".to_string()],
        correlations: vec![vec![1.0, r], vec![r, 1.0]],
        p_values: Some(vec![vec![0.0, 0.004], vec![0.004, 0.0]]),
        sample_sizes: vec![vec![0, pairs], vec![pairs, 0]],
        rank_correlations: Some(vec![vec![1.0, 0.5], vec![0.5, 1.0]]),
        rank_p_values: Some(vec![vec![0.0, 0.03], vec![0.03, 0.0]]),
    }
}

/// The furthest scroll is the first that shows the last statistic: one short
/// of it leaves the last out, and nothing scrolls past it.
#[test]
fn the_statistics_scroll_stops_where_the_last_comes_into_view() {
    let widths = [5, 5, 6, 3, 10, 6, 6, 6, 6];
    for available in [12u16, 20, 30, 45, 80] {
        let mut columns = ColumnScroll {
            offset: usize::MAX,
            max: 0,
        };
        let (start, end) = stat_window(&widths, available, 2, &mut columns);
        assert_eq!(start, columns.max, "clamped to the furthest start");
        assert_eq!(end, widths.len(), "the last is in view at {available}");
        if columns.max > 0 {
            columns.offset = columns.max - 1;
            let (_, end) = stat_window(&widths, available, 2, &mut columns);
            assert!(end < widths.len(), "one short leaves it out at {available}");
        }
    }
    let mut columns = ColumnScroll::default();
    assert_eq!(stat_window(&widths, 200, 2, &mut columns), (0, 9));
    assert_eq!(columns.max, 0, "everything fits, so nothing scrolls");
}

/// The family list keeps its cursor in view and, while families are out of
/// view below, a row to count them: the cursor never takes that row.
#[test]
fn the_family_list_counts_what_is_below_the_cursor() {
    assert_eq!(list_window(3, 5, 8), (0, 5), "everything fits");
    for selected in 0..14 {
        let (offset, shown) = list_window(selected, 14, 12);
        assert!(
            (offset..offset + shown).contains(&selected),
            "{selected} is drawn"
        );
        let below = 14 - offset - shown;
        if below > 0 {
            assert_eq!(shown, 11, "a row is left to count {below} at {selected}");
        } else {
            assert_eq!(shown, 12);
        }
    }
    assert_eq!(list_window(11, 14, 12), (1, 11), "not the last row");
    assert_eq!(list_window(13, 14, 12), (2, 12), "the end needs no count");
    assert_eq!(list_window(4, 14, 1), (4, 1), "one row is the cursor's");
}

/// The matrix scrolls to the selected cell however it got there, and counts
/// the columns it cannot show.
#[test]
fn the_correlation_matrix_keeps_the_selected_column_in_view() {
    let names: Vec<String> = (0..6).map(|i| format!("col_{i}")).collect();
    let n = names.len();
    let matrix = crate::analysis::statistics::CorrelationMatrix {
        columns: names,
        correlations: vec![vec![0.5; n]; n],
        p_values: None,
        sample_sizes: vec![vec![10; n]; n],
        rank_correlations: Some(vec![vec![0.5; n]; n]),
        rank_p_values: None,
    };
    let results = AnalysisResults {
        column_statistics: vec![],
        total_rows: 10,
        sample_size: None,
        per_value: None,
        correlation_matrix: Some(matrix),
        distribution_analyses: vec![],
    };
    let theme = Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 60, 10);
    let mut columns = ColumnScroll::default();
    let mut state = TableState::default();
    let mut header = |selected: (usize, usize), columns: &mut ColumnScroll| {
        let mut buf = Buffer::empty(area);
        state.select(Some(selected.0));
        render_correlation_matrix(
            results.correlation_matrix.as_ref().map(|matrix| Shown {
                matrix,
                method: CorrelationMethod::Pearson,
            }),
            &mut state,
            MatrixCursor {
                cell: Some(selected),
                focused: true,
            },
            columns,
            area,
            &mut buf,
            &theme,
        );
        crate::tests::buffer_text(&buf)
            .lines()
            .next()
            .unwrap()
            .to_string()
    };
    let first = header((0, 0), &mut columns);
    assert!(
        first.contains("col_0") && !first.contains("col_5"),
        "{first:?}"
    );
    assert!(
        first.contains('+'),
        "the hidden columns are counted: {first:?}"
    );
    let last = header((0, 5), &mut columns);
    assert!(
        last.contains("col_5"),
        "the selected column is drawn: {last:?}"
    );
    let back = header((0, 0), &mut columns);
    assert!(
        back.contains("col_0"),
        "and so is the first again: {back:?}"
    );
}

/// One accent on screen: the pane without focus keeps its selection marked by
/// a dimmed rail, never the accent, whichever side has the focus.
#[test]
fn the_unfocused_selection_is_dimmed() {
    let theme = Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let accent = theme.accent();
    let dimmed = theme.dimmed();
    let rail = crate::glyphs::get().rail;
    let area = Rect::new(0, 0, 30, 8);
    let sidebar = |focus: AnalysisFocus| {
        let mut buf = Buffer::empty(area);
        let mut state = TableState::default();
        state.select(Some(2));
        render_sidebar(
            area,
            &mut buf,
            &mut state,
            Some(AnalysisTool::Describe),
            focus,
            &theme,
        );
        buf
    };
    // The pane has the focus: the tool on screen keeps a dimmed rail.
    let buf = sidebar(AnalysisFocus::Main);
    let describe = (0..area.height)
        .find(|&y| row_text(&buf, y).contains("Describe"))
        .unwrap();
    let at = |buf: &Buffer, y: u16| {
        (0..area.width)
            .find(|&x| buf[(x, y)].symbol() == rail)
            .map(|x| buf[(x, y)].fg)
    };
    assert_eq!(at(&buf, describe), Some(dimmed));
    for y in 1..area.height - 1 {
        for x in 1..area.width - 1 {
            assert_ne!(
                buf[(x, y)].fg,
                accent,
                "no accent in the list at ({x}, {y})"
            );
        }
    }
    // The list has the focus: the cursor's rail is the accent; the tool on
    // screen is only bold.
    let buf = sidebar(AnalysisFocus::Sidebar);
    let cursor = (0..area.height)
        .find(|&y| row_text(&buf, y).contains("Correlation"))
        .unwrap();
    assert_eq!(at(&buf, cursor), Some(accent));
    assert_eq!(at(&buf, describe), None);
}

fn row_text(buf: &Buffer, y: u16) -> String {
    (0..buf.area.width).map(|x| buf[(x, y)].symbol()).collect()
}

#[test]
fn describe_shows_a_datetime_range_and_leaves_std_blank() {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let results = crate::analysis::statistics::compute_describe_single_aggregation(
        &crate::analysis::statistics::describe_tests::temporal_frame(),
        &crate::analysis::statistics::describe_tests::temporal_frame()
            .schema()
            .clone(),
        6,
        None,
        false,
    )
    .unwrap();
    let area = Rect::new(0, 0, 220, 6);
    let mut buf = Buffer::empty(area);
    StatisticsTable {
        results: &results,
        focused: false,
        theme: &theme,
        table_cell_padding: 1,
        number_format: &settings("thousands", false),
    }
    .render(
        area,
        &mut buf,
        &mut TableState::default(),
        &mut crate::analysis::analysis_modal::ColumnScroll::default(),
    );
    let text = crate::tests::buffer_text(&buf);
    let mut lines = text.lines();
    let header = lines.next().unwrap();
    let pickup = lines
        .find(|l| l.trim_start().starts_with("pickup"))
        .unwrap_or_else(|| panic!("{text}"));
    // Each value sits under its own header.
    for (stat, value) in [
        ("Mean", "2024-12-31 22:47:55"),
        ("Std", "- "),
        ("Min", "2024-12-31 20:47:55"),
        ("25%", "2024-12-31 21:47:55"),
        ("50%", "2024-12-31 22:47:55"),
        ("75%", "2024-12-31 23:47:55"),
        ("Max", "2025-01-01 00:47:55"),
    ] {
        let x = header.find(stat).unwrap();
        assert!(pickup[x..].starts_with(value), "{stat}:\n{text}");
    }
}

#[test]
fn correlation_detail_shows_the_pair_facts_the_matrix_holds() {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 60, 8);
    let mut buf = Buffer::empty(area);
    render_correlation_pair_summary(
        Shown {
            matrix: &correlation_matrix(0.874, 42),
            method: CorrelationMethod::Pearson,
        },
        (0, 1),
        50,
        area,
        &mut buf,
        &theme,
        &settings("thousands", false),
    );
    let text = crate::tests::buffer_text(&buf);
    assert!(text.contains("Pearson r: 0.8740"), "{text}");
    assert!(text.contains("strong positive"), "{text}");
    let r_squared = crate::glyphs::get().r_squared;
    assert!(text.contains(&format!("{r_squared}: 0.7639")), "{text}");
    assert!(text.contains("P-value: 0.004"), "{text}");
    assert!(text.contains("Pairs used: 42 of 50 rows"), "{text}");
}

#[test]
fn correlation_detail_says_when_too_few_pairs_overlap() {
    // A pair with fewer than 3 overlapping values holds NaN in the matrix.
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 70, 8);
    let mut buf = Buffer::empty(area);
    render_correlation_pair_summary(
        Shown {
            matrix: &correlation_matrix(f64::NAN, 2),
            method: CorrelationMethod::Pearson,
        },
        (0, 1),
        50,
        area,
        &mut buf,
        &theme,
        &settings("thousands", false),
    );
    let text = crate::tests::buffer_text(&buf);
    assert!(text.contains("Fewer than 3 overlapping pairs"), "{text}");
    assert!(!text.contains("Pearson r:"), "{text}");
}

/// A matrix too large to rank says so under Spearman, and still shows Pearson.
#[test]
fn a_matrix_without_ranks_says_why_under_spearman() {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let mut matrix = correlation_matrix(0.874, 42);
    matrix.rank_correlations = None;
    matrix.rank_p_values = None;
    let area = Rect::new(0, 0, 90, 8);
    let mut buf = Buffer::empty(area);
    render_correlation_pair_summary(
        Shown {
            matrix: &matrix,
            method: CorrelationMethod::Spearman,
        },
        (0, 1),
        50,
        area,
        &mut buf,
        &theme,
        &settings("thousands", false),
    );
    assert!(crate::tests::buffer_text(&buf).contains(SPEARMAN_TOO_MANY));
    let mut buf = Buffer::empty(area);
    render_correlation_pair_summary(
        Shown {
            matrix: &matrix,
            method: CorrelationMethod::Pearson,
        },
        (0, 1),
        50,
        area,
        &mut buf,
        &theme,
        &settings("thousands", false),
    );
    assert!(crate::tests::buffer_text(&buf).contains("Pearson r: 0.8740"));
}

#[test]
fn correlation_detail_shows_the_chosen_method() {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 60, 8);
    let mut buf = Buffer::empty(area);
    render_correlation_pair_summary(
        Shown {
            matrix: &correlation_matrix(0.874, 42),
            method: CorrelationMethod::Spearman,
        },
        (0, 1),
        50,
        area,
        &mut buf,
        &theme,
        &settings("thousands", false),
    );
    let text = crate::tests::buffer_text(&buf);
    let rho = crate::glyphs::get().rho;
    assert!(text.contains(&format!("Spearman {rho}: 0.5000")), "{text}");
    assert!(text.contains("P-value: 0.03"), "{text}");
}

/// Rounding never shows a perfect relation that is not one.
#[test]
fn a_coefficient_rounds_to_one_only_when_it_is_one() {
    assert_eq!(format_coefficient(0.9996, 3), "0.999");
    assert_eq!(format_coefficient(-0.9996, 3), "-0.999");
    assert_eq!(format_coefficient(0.99996, 4), "0.9999");
    assert_eq!(format_coefficient(0.9994, 3), "0.999");
    assert_eq!(format_coefficient(1.0, 3), "1.000");
    assert_eq!(format_coefficient(-1.0, 4), "-1.0000");
    assert_eq!(format_coefficient(0.12345, 3), "0.123");
    assert_eq!(format_coefficient(-0.5, 3), "-0.500");
}

#[test]
fn correlation_words_match_the_color_boundaries() {
    assert_eq!(describe_correlation(0.01), "none");
    assert_eq!(describe_correlation(0.2), "weak positive");
    assert_eq!(describe_correlation(-0.5), "moderate negative");
    assert_eq!(describe_correlation(0.9), "strong positive");
    assert_eq!(describe_correlation(-0.9), "strong negative");
}

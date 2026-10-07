use super::*;
use crate::analysis_modal::ExpectedForm;
use crate::data_quality::ExpectedWindows;
use crate::data_quality::fixtures::measure;
use polars::prelude::{DataType, IntoLazy, LazyFrame, col, df};
use ratatui::style::Color;
use std::sync::Arc;

/// Weekday rows over eight weeks, forty a day, the second week missing and a
/// third of the amounts null.
fn frame() -> LazyFrame {
    let days = (0..56)
        .filter(|day| day % 7 < 5 && !(7..14).contains(day))
        .collect::<Vec<i32>>();
    let day = days
        .iter()
        .flat_map(|day| std::iter::repeat_n(19_723 + day, 40))
        .collect::<Vec<_>>();
    let rows = day.len();
    df!(
        "day" => day,
        "amount" => (0..rows).map(|row| (row % 3 != 0).then_some(row as f64)).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("day").cast(DataType::Date))
}

struct Screen {
    state: DataTableState,
    plan: DataQualityPlan,
    results: DataQualityResults,
    metric: QualityMetric,
    theme: Theme,
    ctx: RenderContext,
}

impl Screen {
    /// A daily study of a 30-row sample, weekdays expected through March 3.
    fn sampled() -> Self {
        let lf = frame();
        let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf.clone(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let plan = DataQualityPlan {
            dataset_rows: 30,
            sample_seed: 415,
            grain: QualityGrain::TimeWindows {
                column: "day".to_string(),
                every: "1d".to_string(),
            },
            expected: Some(ExpectedWindows {
                weekdays: true,
                from: Some("2024-01-01".to_string()),
                before: Some("2024-03-04".to_string()),
            }),
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, Some(1_400), &plan);
        assert!(!results.unsampled_segments.is_empty());
        Self {
            state,
            plan,
            results,
            metric: QualityMetric::NullRate,
            theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
            ctx: RenderContext::for_test(),
        }
    }

    fn draw_with(
        &self,
        theme: &Theme,
        page: QualityPage,
        line: usize,
        selected: usize,
        form: Option<&ExpectedForm>,
        (width, height): (u16, u16),
    ) -> Buffer {
        let config = DataQualityWidgetConfig {
            checks_expanded: false,
            state: &self.state,
            plan: &self.plan,
            measured: &self.plan,
            results: Some(&self.results),
            from_cache: false,
            metric: self.metric,
            column_index: 0,
            segment_index: 0,
            interval_index: 0,
            trend_line: line,
            expected_form: form,
            segments_by_change: false,
            page,
            setup: SetupView::default(),
            plan_field: SetupRow::Expected.index(),
            show_access: false,
            observation_detail: false,
            focus: AnalysisFocus::Main,
            theme,
            ctx: &self.ctx,
            findings: &FindingsView::default(),
            rows_kept: false,
            evidence_read: None,
            intent_form: None,
            export_form: None,
        };
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        let mut table = TableState::default();
        table.select(Some(selected));
        render(
            config,
            &mut table,
            &mut TableState::default(),
            &mut DetailScroll::default(),
            area,
            &mut buf,
        );
        buf
    }

    fn draw(&self, page: QualityPage, line: usize, selected: usize, size: (u16, u16)) -> String {
        text(&self.draw_with(&self.theme, page, line, selected, None, size))
    }

    /// The Trends line of the amount column.
    fn amount(&self) -> usize {
        trend_view(&self.results, self.metric, 1)
            .lines
            .iter()
            .position(|line| line.names == ["amount"])
            .unwrap()
    }
}

fn text(buf: &Buffer) -> String {
    let area = buf.area;
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every character outside ASCII is a glyph slot, which `LANG=C` swaps for its
/// ASCII twin.
fn assert_glyph_slots(text: &str) {
    let g = glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.middot,
        g.ellipsis,
        g.selector,
        g.unsampled,
        g.pointer,
        g.updown,
    ]
    .concat()
        + &g.mini_bars.concat();
    for c in text.chars().filter(|c| !c.is_ascii()) {
        assert!(
            slots.contains(c) || "╭╮╰╯│─".contains(c),
            "{c:?} is not a glyph slot:\n{text}"
        );
    }
}

/// A bar the sample drew nothing from has a mark of its own in both glyph sets:
/// not a bar level, and not the blank of a measure with nothing to apply to. So
/// neither a C locale nor a 16-color terminal loses it.
#[test]
fn a_missed_bar_has_its_own_mark_in_both_glyph_sets() {
    let screen = Screen::sampled();
    let view = trend_view(&screen.results, QualityMetric::NullRate, 100);
    let missed = view
        .bars
        .iter()
        .position(|bar| bar.evaluated == 0)
        .expect("a day with no sampled row");
    let amount = &view.lines[screen.amount()];
    for g in [glyphs::unicode(), glyphs::ascii()] {
        let mark = bar_mark(amount, &view.bars[missed], missed, g);
        assert_eq!(mark, g.unsampled);
        assert!(!g.mini_bars.contains(&mark) && mark != " ", "{mark:?}");
        assert_ne!(g.pointer, " ");
        assert_eq!(glyphs::display_width(g.unsampled), 1);
        assert_eq!(glyphs::display_width(g.pointer), 1);
    }
    // The exact count still has every day's rows.
    let rows = &view.lines[0];
    assert_eq!(rows.names, ["rows"]);
    assert_eq!(rows.bars[missed], Some(40.0));
}

/// Trends says how much of the scope the sample reached, offers a coarser window,
/// and sums up the expected windows, at 80x24 and 60x20.
#[test]
fn trends_say_their_coverage_and_gaps_at_80x24_and_60x20() {
    let screen = Screen::sampled();
    for size in [(80, 24), (60, 20)] {
        let text = screen.draw(QualityPage::Trends, 0, 0, size);
        assert!(text.contains("not sampled"), "{size:?}:\n{text}");
        assert!(text.contains("stages weekly"), "{size:?}:\n{text}");
        assert!(text.contains("Expected weekdays"), "{size:?}:\n{text}");
        assert!(text.contains("sampled rows"), "{size:?}:\n{text}");
        assert!(text.contains("amount"), "{size:?}:\n{text}");
        assert_glyph_slots(&text);
    }
}

/// A bar's detail holds every fact at 80x24 and 60x20, the pointer under the bar
/// selected, and walks to the last bar without running past it.
#[test]
fn a_trend_bar_states_its_facts_at_80x24_and_60x20() {
    let screen = Screen::sampled();
    let amount = screen.amount();
    for size in [(80, 24), (60, 20)] {
        let first = screen.draw(QualityPage::TrendDetail, amount, 0, size);
        for label in ["Span", "Segments", "Rows", "Null rate", "95% interval"] {
            assert!(first.contains(label), "{label} at {size:?}:\n{first}");
        }
        assert!(first.contains("bar 1 of"), "{first}");
        assert!(!first.contains("Previous bar"), "nothing before the first");
        let second = screen.draw(QualityPage::TrendDetail, amount, 1, size);
        assert!(second.contains("Previous bar"), "{size:?}:\n{second}");
        // Past the end, the last bar; the pointer sits on the spark's last mark.
        let last = screen.draw(QualityPage::TrendDetail, amount, 10_000, size);
        let lines = last.lines().collect::<Vec<_>>();
        let spark = lines
            .iter()
            .position(|line| line.contains("amount"))
            .unwrap();
        let pointer = lines[spark + 1]
            .find(glyphs::get().pointer)
            .expect("a pointer");
        // Up to the tool list's frame, where there is one.
        let side = format!(" {}", glyphs::get().border.vertical_left);
        let marks =
            lines[spark][..lines[spark].find(&side).unwrap_or(lines[spark].len())].trim_end();
        assert_eq!(
            glyphs::display_width(marks) - 1,
            glyphs::display_width(&lines[spark + 1][..pointer]),
            "{last}"
        );
        assert_glyph_slots(&last);
    }
    // A rows line says rows per segment, not a rate.
    let rows = screen.draw(QualityPage::TrendDetail, 0, 0, (80, 24));
    assert!(rows.contains("Rows per segment"), "{rows}");
    assert!(rows.contains("Per segment"), "{rows}");
    assert!(!rows.contains("95% interval"), "{rows}");
}

/// Nothing on these pages is said by color alone: drawn with every color gone,
/// and with the 16 a basic terminal has, every cell holds the same symbol.
#[test]
fn trends_and_gaps_read_the_same_without_color() {
    let screen = Screen::sampled();
    let mono = Theme {
        colors: screen
            .theme
            .colors
            .keys()
            .map(|key| (key.clone(), Color::Reset))
            .collect(),
    };
    let sixteen = Theme {
        colors: screen
            .theme
            .colors
            .iter()
            .map(|(key, color)| {
                let color = match *color {
                    Color::Rgb(r, g, b) => crate::config::rgb_to_basic_ansi(r, g, b),
                    Color::Indexed(index) => Color::Indexed(index % 16),
                    other => other,
                };
                (key.clone(), color)
            })
            .collect(),
    };
    for (page, line) in [
        (QualityPage::Trends, 0),
        (QualityPage::TrendDetail, screen.amount()),
        (QualityPage::Gaps, 0),
    ] {
        let full = text(&screen.draw_with(&screen.theme, page, line, 1, None, (80, 24)));
        for theme in [&mono, &sixteen] {
            assert_eq!(
                text(&screen.draw_with(theme, page, line, 1, None, (80, 24))),
                full,
                "{page:?}"
            );
        }
    }
}

/// Gaps list each run of windows by why it has no rows: empty by the exact
/// count, not sampled, with the weekends not expected said apart.
#[test]
fn gaps_list_each_kind_at_80x24_and_60x20() {
    let screen = Screen::sampled();
    for size in [(80, 24), (60, 20)] {
        let text = screen.draw(QualityPage::Gaps, 0, 0, size);
        assert!(text.contains("Expected weekdays"), "{size:?}:\n{text}");
        assert!(text.contains("2024-01-08 to 2024-01-12 empty"), "{text}");
        assert!(text.contains("not sampled"), "{text}");
        assert!(text.contains("on weekends, not expected"), "{text}");
        assert!(text.contains("gaps"), "{text}");
        assert_glyph_slots(&text);
    }
    // Wide enough, each missed run says the rows the scope holds there.
    let wide = screen.draw(QualityPage::Gaps, 0, 0, (120, 30));
    assert!(wide.contains("40 rows"), "{wide}");
}

/// Setup's Expected row says what is stated, and its editor lays out at 60x20.
#[test]
fn setup_states_the_expected_windows() {
    let mut screen = Screen::sampled();
    let setup = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
    assert!(
        setup.contains("Expected       weekdays, 2024-01-01 to before 2024-03-04"),
        "{setup}"
    );
    assert!(
        setup.contains("Expected windows: from the segment counts · no read"),
        "{setup}"
    );
    let form = ExpectedForm::new(&screen.plan, &screen.theme);
    let editor = text(&screen.draw_with(
        &screen.theme,
        QualityPage::ExpectedWindows,
        0,
        0,
        Some(&form),
        (60, 20),
    ));
    for text in [
        "Windows",
        "weekdays, Monday to Friday",
        "From",
        "2024-01-01",
        "Before",
    ] {
        assert!(editor.contains(text), "{text}:\n{editor}");
    }
    screen.plan.expected = None;
    let none = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
    assert!(none.contains("none: no window is a gap"), "{none}");
    screen.plan.grain = QualityGrain::Dataset;
    let none = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
    assert!(
        none.contains("Expected       needs a time-window grain"),
        "{none}"
    );
}

/// A distinct share is shown against the bar before but never judged, as
/// Segments never judges one: it falls as a segment grows.
#[test]
fn a_distinct_share_is_not_judged_between_bars() {
    let mut screen = Screen::sampled();
    screen.metric = QualityMetric::DistinctShare;
    let second = screen.draw(QualityPage::TrendDetail, screen.amount(), 1, (100, 30));
    assert!(second.contains("Previous bar"), "{second}");
    assert!(second.contains("+0.0 points: not judged"), "{second}");
    assert!(second.contains("none: a distinct share"), "{second}");
}

/// One day found is no trend, but the week expected around it still has its
/// gaps: Trends sums them up beside the way to a grain.
#[test]
fn one_window_found_still_sums_up_the_expected_ones() {
    let mut screen = Screen::sampled();
    screen.plan = DataQualityPlan {
        compute: crate::data_quality::QualityCompute::Full,
        expected: Some(ExpectedWindows {
            weekdays: false,
            from: Some("2024-01-01".to_string()),
            before: Some("2024-01-08".to_string()),
        }),
        ..screen.plan.clone()
    };
    let one_day = frame().filter(
        col("day")
            .cast(DataType::Int32)
            .eq(polars::prelude::lit(19_724)),
    );
    screen.results = measure(&one_day, None, &screen.plan);
    assert!(!crate::data_quality::shows_trend(
        &screen.plan,
        &screen.results
    ));
    let text = screen.draw(QualityPage::Trends, 0, 0, (80, 24));
    assert!(text.contains("Set Grain"), "{text}");
    assert!(
        text.contains("Expected every day, 7 days: 6 empty"),
        "{text}"
    );
}

/// From past the last window found, Before blank: no window is in range, and
/// Trends says that rather than that every window has rows.
#[test]
fn a_range_with_no_window_says_so() {
    let mut screen = Screen::sampled();
    screen.plan.expected = Some(ExpectedWindows {
        weekdays: false,
        from: Some("2025-01-01".to_string()),
        before: None,
    });
    for page in [QualityPage::Trends, QualityPage::Gaps] {
        let text = screen.draw(page, 0, 0, (80, 24));
        assert!(
            text.contains("Expected every day: no window in range"),
            "{page:?}:\n{text}"
        );
    }
}

use super::*;
use crate::data_quality::fixtures::measure;

/// A value that fits keeps its spaces; one that does not wraps under itself.
#[test]
fn field_values_keep_their_spaces_and_wrap_under_themselves() {
    let row = |value: &str| FieldRow {
        mark: None,
        label: "Range".to_string(),
        value: value.to_string(),
    };
    let text = |lines: Vec<Line<'static>>| {
        lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        text(field_lines(&[row(" South to New  York")], 7, 40, false)),
        ["Range   South to New  York"]
    );
    assert_eq!(
        text(field_lines(&[row("one two three four")], 7, 17, false)),
        ["Range  one two", "       three four"]
    );
}

use polars::prelude::*;

fn fixture() -> LazyFrame {
    df!(
        "id" => &[1i64, 2, 3, 4, 5, 6, 7, 8],
        "region" => &[
            Some("West"), Some("west"), Some("West"), Some("West "),
            Some("East"), None, Some("North"), Some("West"),
        ],
        "amount" => &[1.5f64, 2.0, 3.25, 4.0, 5.0, 6.0, 7.0, 8.0],
    )
    .unwrap()
    .lazy()
}

struct Screen {
    state: DataTableState,
    plan: DataQualityPlan,
    results: DataQualityResults,
    findings: FindingsView,
    theme: Theme,
    ctx: RenderContext,
}

impl Screen {
    fn new() -> Self {
        let lf = fixture();
        let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf.clone(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = measure(&lf, Some(8), &plan);
        Self {
            state,
            plan,
            results,
            findings: FindingsView::default(),
            theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
            ctx: RenderContext::for_test(),
        }
    }

    fn config(&self, page: QualityPage) -> DataQualityWidgetConfig<'_> {
        DataQualityWidgetConfig {
            checks_expanded: false,
            state: &self.state,
            plan: &self.plan,
            measured: &self.plan,
            results: Some(&self.results),
            from_cache: false,
            metric: QualityMetric::NullRate,
            segment_index: 0,
            interval_index: 0,
            trend_line: 0,
            expected_form: None,
            segments_by_change: false,
            page,
            setup: SetupView::default(),
            plan_field: 0,
            show_access: false,
            observation_detail: false,
            findings: &self.findings,
            rows_kept: false,
            evidence_read: None,
            focus: AnalysisFocus::Main,
            theme: &self.theme,
            ctx: &self.ctx,
            intent_form: None,
            export_form: None,
        }
    }

    fn draw(
        &self,
        config: DataQualityWidgetConfig<'_>,
        selected: usize,
        width: u16,
        height: u16,
    ) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        let mut table = TableState::default();
        table.select(Some(selected));
        let mut sidebar = TableState::default();
        sidebar.select(Some(3));
        render(
            config,
            &mut table,
            &mut sidebar,
            &mut DetailScroll::default(),
            area,
            &mut buf,
        );
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }
}

/// The column Detail page: the column on a rule, its findings, then its
/// measurements as label and value rows whose values start in one column.
/// No frame, no Rust debug quotes, nothing the header already says.
#[test]
fn column_detail_is_aligned_rows_without_a_box() {
    let screen = Screen::new();
    let region = screen
        .results
        .columns
        .iter()
        .position(|column| column.name == "region")
        .unwrap();
    // Narrow enough that the tool list gives way; the page is the whole width.
    let rows = screen.draw(screen.config(QualityPage::Detail), region, 70, 24);
    let text = rows.join("\n");
    let g = glyphs::get();
    let b = g.border;
    for corner in [
        b.top_left,
        b.top_right,
        b.bottom_left,
        b.bottom_right,
        b.vertical_left,
    ] {
        assert!(!text.contains(corner), "no box on the page:\n{text}");
    }
    let rule = format!("region {}", g.rule_h);
    assert!(
        rows.iter().any(|row| row.trim_start().starts_with(&rule)),
        "the column is named on a rule:\n{text}"
    );
    assert!(
        text.contains("Mixed spellings"),
        "the column's findings are on the page:\n{text}"
    );
    let findings_end = rows
        .iter()
        .position(|row| row.contains("Mixed spellings"))
        .unwrap();
    let type_row = rows.iter().position(|row| row.contains("Type")).unwrap();
    assert!(findings_end < type_row, "findings first:\n{text}");

    // Every measurement's value starts where the first one's does.
    let value_column = |label: &str| {
        let row = rows
            .iter()
            .find(|row| row.trim_start().starts_with(label))
            .unwrap_or_else(|| panic!("{label} row:\n{text}"));
        let after = row.find(label).unwrap() + label.len();
        after + row[after..].len() - row[after..].trim_start().len()
    };
    let column = value_column("Type");
    for label in ["Missing", "Distinct", "Range", "Most common", "Spellings"] {
        assert_eq!(value_column(label), column, "{label} aligned:\n{text}");
    }

    let spellings = rows
        .iter()
        .position(|row| row.contains("Spellings"))
        .unwrap();
    let measurements = rows[type_row..spellings].join("\n");
    assert!(
        !measurements.contains('"'),
        "values as the table shows them, not debug quoted:\n{text}"
    );
    assert!(measurements.contains("West, 3 rows"), "{text}");
    // Spellings differ by what the table cannot show; quoted, they read apart.
    let spellings = rows[spellings..].join("\n");
    for spelling in ["\"West\" (3)", "\"West \" (1)", "\"west\" (1)"] {
        assert!(spellings.contains(spelling), "{spelling}:\n{text}");
    }
    assert!(
        !text.contains("Evaluated") && !text.contains("sampled"),
        "the header says what was evaluated:\n{text}"
    );
}

/// The report and its checks are built once for a run's results, not once per
/// frame: the overview, a finding's detail and the columns draw from the same one.
#[test]
fn the_report_is_built_once_per_results() {
    use crate::quality_report::REPORTS_BUILT;
    let screen = Screen::new();
    let before = REPORTS_BUILT.with(std::cell::Cell::get);
    for _ in 0..3 {
        screen.draw(screen.config(QualityPage::Overview), 0, 80, 24);
        screen.draw(screen.config(QualityPage::Columns), 0, 80, 24);
        screen.draw(screen.config(QualityPage::Detail), 0, 80, 24);
    }
    assert_eq!(REPORTS_BUILT.with(std::cell::Cell::get) - before, 1);
    // A copy is built again: it may have been changed.
    let copy = screen.results.clone();
    copy.report();
    assert_eq!(REPORTS_BUILT.with(std::cell::Cell::get) - before, 2);
}

/// Section titles are words on a rule, never SCREAMING.
#[test]
fn no_page_has_an_uppercase_title() {
    let screen = Screen::new();
    for page in [
        QualityPage::Setup,
        QualityPage::Overview,
        QualityPage::Columns,
        QualityPage::Segments,
        QualityPage::Trends,
        QualityPage::Detail,
    ] {
        for (width, height) in [(120, 32), (80, 24), (60, 20)] {
            let rows = screen.draw(screen.config(page), 1, width, height);
            for row in &rows {
                let shouting = row.split(|c: char| !c.is_alphabetic()).find(|word| {
                    word.chars().count() >= 4 && word.chars().all(|c| c.is_uppercase())
                });
                assert!(
                    shouting.is_none(),
                    "{page:?} at {width}x{height} shouts {shouting:?}: {row:?}"
                );
            }
        }
    }
}

/// Each dialog is one Surface: one frame, its title on it, nothing boxed
/// inside, at the smallest size datui supports.
#[test]
fn dialogs_are_one_surface() {
    let screen = Screen::new();
    for title in ["Access Plan", "Mixed spellings", "Analysis Tools"] {
        for (width, height) in [(80, 24), (60, 20)] {
            let mut config = screen.config(QualityPage::Setup);
            match title {
                "Access Plan" => config.show_access = true,
                // The first finding on the Overview.
                "Mixed spellings" => {
                    config.page = QualityPage::Overview;
                    config.observation_detail = true;
                }
                _ => config.focus = AnalysisFocus::Sidebar,
            }
            let rows = screen.draw(config, 0, width, height);
            let text = rows.join("\n");
            let corners = glyphs::frame_corners(&rows).len();
            // At 80 columns the tool list keeps its own frame beside the page.
            let expected = if width >= 76 && title != "Analysis Tools" {
                2
            } else {
                1
            };
            assert_eq!(corners, expected, "{title} at {width}x{height}:\n{text}");
            let border = glyphs::get().border;
            let titled = format!("{}{title}", border.top_left);
            let frame = rows
                .iter()
                .find(|row| row.contains(&titled))
                .unwrap_or_else(|| panic!("{title} on its frame:\n{text}"));
            let after = &frame[frame.find(&titled).unwrap() + titled.len()..];
            assert!(after.contains(border.top_right), "{title}:\n{text}");
        }
    }
}

/// Coverage sits under the verdict whether the report found problems or none, at
/// the baseline size and the smallest, with the findings still on screen below
/// it and every character outside ASCII a glyph slot.
#[test]
fn coverage_accompanies_every_verdict_at_80x24_and_60x20() {
    let screen = Screen::new();
    let g = glyphs::get();
    let slots = [
        g.rail, g.rule_h, g.middot, g.ellipsis, g.warning, g.check, g.dash,
    ]
    .concat();
    // The same rows sampled, and found clean: a report with nothing to fix.
    let mut clean = screen.results.clone();
    clean.observations.clear();
    clean.precision = QualityPrecision::Sampled;
    clean.total_rows = Some(80);
    clean.reads = Some(crate::data_quality::ObservedReads {
        reads: 1,
        counted: 1,
        rows: 80,
        copy: None,
    });
    for (results, verdict) in [(&screen.results, "problem"), (&clean, "No problems found")] {
        for (width, height) in [(80, 24), (60, 20)] {
            let mut config = screen.config(QualityPage::Overview);
            config.results = Some(results);
            let rows = screen.draw(config, 0, width, height);
            let text = rows.join("\n");
            let at = rows
                .iter()
                .position(|row| row.contains(verdict))
                .unwrap_or_else(|| panic!("verdict at {width}x{height}:\n{text}"));
            assert!(
                rows[at + 1].trim_start().starts_with("Checks"),
                "coverage under the verdict at {width}x{height}:\n{text}"
            );
            assert!(
                text.contains("Problems") || text.contains("Clean"),
                "findings still on screen at {width}x{height}:\n{text}"
            );
            if results.precision == QualityPrecision::Exact {
                assert!(
                    rows[at + 1].contains("exact") && text.contains("all 8 read, exact"),
                    "{text}"
                );
            } else {
                // Clean, and not everything could be looked at: it says so.
                assert!(rows[at + 1].contains("1 unavailable"), "{text}");
                assert!(text.contains("8 of 80 sampled (10.0%)"), "{text}");
                assert!(
                    text.contains("Nearly unique: needs every row checked"),
                    "{text}"
                );
            }
            for c in text.chars().filter(|c| !c.is_ascii()) {
                assert!(
                    slots.contains(c) || "╭╮╰╯│─".contains(c),
                    "{c:?} is not a glyph slot at {width}x{height}:\n{text}"
                );
            }
        }
    }
}

/// A finding says where its rows come from before Enter: the rows the run kept,
/// a sample no longer kept, or a full scan that kept none. A staged read lists
/// what it reads, at the baseline size and the smallest, in glyph slots.
#[test]
fn evidence_says_where_its_rows_come_from() {
    let screen = Screen::new();
    let mut sampled = screen.results.clone();
    sampled.precision = QualityPrecision::Sampled;
    let report = sampled.report();
    let position = FindingsView::default()
        .shown(report)
        .iter()
        .position(|index| {
            report.findings[*index].kind == Some(crate::data_quality::ObservationKind::Nulls)
        })
        .unwrap();
    let detail = |results: &DataQualityResults, kept: bool, width, height| {
        let mut config = screen.config(QualityPage::Overview);
        config.results = Some(results);
        config.observation_detail = true;
        config.rows_kept = kept;
        screen.draw(config, position, width, height).join("\n")
    };
    assert!(detail(&sampled, true, 80, 24).contains("Enter: the 1 sampled row"));
    assert!(detail(&sampled, false, 80, 24).contains("sample released, asks first"));
    assert!(detail(&screen.results, false, 80, 24).contains("full scan keeps none, asks first"));
    // A sample that held every row is exact, but it was no full scan.
    let whole_sample = DataQualityPlan {
        compute: QualityCompute::Sample,
        ..screen.plan.clone()
    };
    let mut config = screen.config(QualityPage::Overview);
    config.measured = &whole_sample;
    config.observation_detail = true;
    let text = screen.draw(config, position, 80, 24).join("\n");
    assert!(text.contains("rows released, asks first"), "{text}");

    let read = EvidenceRead {
        rows: EvidenceRows::Matching(polars::prelude::lit(true)),
        label: String::new(),
        sample: None,
        scope: QualityScope::CurrentView,
        summary: vec![
            ("Rows", "Missing values · region".to_string()),
            ("Why", "a full scan keeps no rows".to_string()),
            (
                "Reads",
                "current view, as far as the table scrolls".to_string(),
            ),
            ("Shows", "1 row".to_string()),
            ("Source", "local, read only".to_string()),
        ],
    };
    let g = glyphs::get();
    let slots = [g.rail, g.rule_h, g.middot, g.ellipsis, g.warning, g.check].concat();
    for (width, height) in [(80, 24), (60, 20)] {
        let mut config = screen.config(QualityPage::Overview);
        config.observation_detail = true;
        config.evidence_read = Some(&read);
        let text = screen.draw(config, position, width, height).join("\n");
        for label in ["Read Rows", "Why", "Reads", "Shows", "read only"] {
            assert!(text.contains(label), "{label} at {width}x{height}:\n{text}");
        }
        for c in text.chars().filter(|c| !c.is_ascii()) {
            assert!(
                slots.contains(c) || "╭╮╰╯│─·".contains(c),
                "{c:?} at {width}x{height}:\n{text}"
            );
        }
    }
}

/// A line with room for one fact keeps it, cut short, beside the count of the
/// rest, rather than saying only how much it left out.
#[test]
fn a_lone_fact_is_cut_rather_than_dropped() {
    let facts = [
        "Nearly unique: needs every row checked".to_string(),
        "key repeats among 10,000 sampled rows only".to_string(),
    ];
    let lines = pack_facts(&facts, 37, 1);
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with("Nearly unique"), "{lines:?}");
    assert!(lines[0].ends_with("+1 more"), "{lines:?}");
    assert!(glyphs::display_width(&lines[0]) <= 37, "{lines:?}");
}

/// Facts wrap between facts, never inside one, and what does not fit is counted.
#[test]
fn coverage_facts_wrap_whole_and_count_the_rest() {
    let facts = ["one fact", "another fact", "a third fact", "a fourth"]
        .map(String::from)
        .to_vec();
    let dot = glyphs::get().middot;
    assert_eq!(
        pack_facts(&facts, 24, 2),
        [
            format!("one fact {dot} another fact"),
            format!("a third fact {dot} a fourth")
        ]
    );
    assert_eq!(
        pack_facts(&facts, 24, 1),
        [format!("one fact {dot} +3 more")]
    );
}

/// Setup at the baseline size and the smallest: every row in its section, the
/// focused one on screen with the rail whichever it is, and every character
/// outside ASCII a glyph slot, which has an ASCII twin under `LANG=C`.
#[test]
fn setup_fits_80x24_and_60x20_in_glyph_slots() {
    let screen = Screen::new();
    let g = glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.rule_h_focused,
        g.middot,
        g.ellipsis,
        g.selector,
    ]
    .concat();
    for (width, height) in [(80, 24), (60, 20)] {
        for row in SetupRow::ALL {
            let mut config = screen.config(QualityPage::Setup);
            config.plan_field = row.index();
            let rows = screen.draw(config, 0, width, height);
            let text = rows.join("\n");
            assert!(
                rows.iter()
                    .any(|line| line.contains(&format!("{}{}", g.rail, row.label()))),
                "{row:?} focused at {width}x{height}:\n{text}"
            );
            for other in SetupRow::ALL {
                assert!(
                    text.contains(other.label()),
                    "{other:?} on screen at {width}x{height}:\n{text}"
                );
            }
            for section in ["Rows & sample", "Columns", "Study", "Read"] {
                assert!(
                    text.contains(section),
                    "{section} at {width}x{height}:\n{text}"
                );
            }
            for c in text.chars().filter(|c| !c.is_ascii()) {
                assert!(
                    slots.contains(c) || "╭╮╰╯│─".contains(c),
                    "{c:?} is not a glyph slot at {width}x{height}:\n{text}"
                );
            }
        }
    }
}

/// Setup says what a run will read before it runs, and says it honestly: a full
/// run counts its passes rather than promising one read, and Setup's own line
/// says why Enter waits.
#[test]
fn setup_states_the_read_and_why_run_waits() {
    let screen = Screen::new();
    let mut config = screen.config(QualityPage::Setup);
    config.show_access = true;
    let text = screen.draw(config, 0, 100, 30).join("\n");
    assert!(!text.contains("at most one read"), "{text}");
    assert!(text.contains("one per check"), "{text}");

    let config = screen.config(QualityPage::Setup);
    let text = screen.draw(config, 0, 100, 30).join("\n");
    assert!(text.contains("passes over the scope"), "{text}");

    let mut config = screen.config(QualityPage::Setup);
    config.setup.cancelling = Some(Cancelling {
        since: std::time::Instant::now(),
        read_runs_out: true,
    });
    config.setup.note = Some("Run waits: the cancelled run is still stopping");
    let rows = screen.draw(config, 0, 100, 30);
    let text = rows.join("\n");
    assert!(
        rows[0].contains("Cancelling: source read finishing"),
        "the header keeps the state: {text}"
    );
    assert!(
        text.contains("Run waits: source read finishing"),
        "Setup says why Enter did not run: {text}"
    );

    // A run that should have stopped at its next batch and has not says so,
    // without claiming a read it cannot stop.
    let mut config = screen.config(QualityPage::Setup);
    config.setup.cancelling = Some(Cancelling {
        since: std::time::Instant::now(),
        read_runs_out: false,
    });
    let rows = screen.draw(config, 0, 100, 30);
    assert!(
        rows[0].contains("Cancelling: run stopping"),
        "{}",
        rows.join("\n")
    );
    assert!(!rows.join("\n").contains("source read finishing"));
}

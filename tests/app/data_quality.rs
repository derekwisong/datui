//! Data Quality: setup, scope, runs, reports and findings.

use super::*;

#[test]
fn test_data_quality_plan_runs_in_background_and_opens_overview() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::QualityPage;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // Choosing Data Quality opens its Setup with the cursor in it; Enter runs the
    // default plan and leads with the result.
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    assert!(app.analysis_modal.quality.results.is_some());

    // A changed plan runs again from Setup, one e away, and e brings the cursor with
    // it, so the Enter that runs needs no Tab first.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    app.analysis_modal.quality.plan.sample_seed = 7_119;
    let next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    app.event(next.unwrap());
    drain_events(&mut app, &rx);

    assert!(app.analysis_modal.quality.results.is_some());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    assert!(!app.is_busy());

    app.analysis_modal
        .quality
        .results
        .as_mut()
        .unwrap()
        .observations
        .push(datui::analysis::data_quality::QualityObservation {
            kind: datui::analysis::data_quality::ObservationKind::Nulls,
            column: "example".to_string(),
            affected_rows: 1,
            evaluated_rows: 10,
            fact: "1 null row".to_string(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
            full_scale: None,
        });
    app.analysis_modal.quality.table_state.select(Some(0));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.quality.observation_detail);
    let area = Rect::new(0, 0, 80, 24);
    let mut detail_buffer = Buffer::empty(area);
    app.render(area, &mut detail_buffer);
    assert!(
        detail_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("Check: clustered"),
        "the finding says what to check, not only its formula"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.quality.observation_detail);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);

    for area in [
        Rect::new(0, 0, 120, 32),
        Rect::new(0, 0, 80, 24),
        Rect::new(0, 0, 50, 18),
    ] {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(
            screen.contains("Data Quality"),
            "quality breadcrumb should survive a {width}x{height} layout",
            width = area.width,
            height = area.height
        );
    }

    for page in [
        QualityPage::Columns,
        QualityPage::Segments,
        QualityPage::Trends,
        QualityPage::Intervals,
    ] {
        app.analysis_modal.set_quality_page(page);
        for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 50, 18)] {
            let mut buffer = Buffer::empty(area);
            app.render(area, &mut buffer);
            let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
            assert!(screen.contains("Data Quality"));
            match page {
                QualityPage::Columns => assert!(screen.contains("Findings")),
                // One segment is no comparison: the page says what makes one.
                QualityPage::Segments => assert!(screen.contains("one segment")),
                QualityPage::Trends => assert!(screen.contains("Across segments")),
                QualityPage::Intervals => assert!(screen.contains("Time between dates")),
                _ => {}
            }
        }
    }

    // The largest measured move between segments is on the screen, not only in the
    // profile: #196 asks for it and nothing read it before.
    // Result pages read the plan the result was measured with.
    let measured = app.analysis_modal.quality.last_plan.as_mut().unwrap();
    measured.grain = datui::analysis::data_quality::QualityGrain::RowChunks(5);
    measured.comparison = datui::analysis::data_quality::QualityComparison::Previous;
    app.analysis_modal.set_quality_page(QualityPage::Segments);
    let wide = Rect::new(0, 0, 160, 40);
    let mut buffer = Buffer::empty(wide);
    app.render(wide, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("Largest change"),
        "Segments should name the column and measurement that moved"
    );
    // Enter shows a segment's every column and measure; Esc goes back to it.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::SegmentDetail);
    let mut buffer = Buffer::empty(wide);
    app.render(wide, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("current view"),
        "the drill-in names its segment"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Segments);

    // Every remaining page and popup must say its own piece at each width, so a
    // clipped label or a screen that renders nothing at all fails here.
    for (page, expected) in [
        (QualityPage::Setup, "Time roles"),
        (QualityPage::TimeRoles, "Date, time and text columns"),
        (QualityPage::Detail, "Missing"),
    ] {
        app.analysis_modal.set_quality_page(page);
        for area in [
            Rect::new(0, 0, 120, 32),
            Rect::new(0, 0, 80, 24),
            Rect::new(0, 0, 50, 18),
        ] {
            let mut buffer = Buffer::empty(area);
            app.render(area, &mut buffer);
            let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
            assert!(
                screen.contains(expected),
                "{expected:?} should survive a {}x{} layout",
                area.width,
                area.height
            );
        }
    }

    // Once a run exists the header says what was measured on one line, as every
    // tool's does, and the sidebar is the tool list every tool has.
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    let header = &screen[..area.width as usize];
    assert!(
        header.starts_with("Data Quality") && header.contains(" rows"),
        "the header says what was measured: {header:?}"
    );
    assert!(
        !screen.contains("Result"),
        "no second verdict in the sidebar"
    );
    assert!(screen.contains("clean"));

    app.analysis_modal.set_quality_page(QualityPage::Setup);
    for (popup, expected) in [
        ("access", "Estimate basis"),
        // The extra reads a full scan makes for the values a type conflict hides are
        // promised before anything runs, like every other read on this page.
        ("access", "Conflict values"),
    ] {
        app.analysis_modal.quality.show_access = popup == "access";
        let area = Rect::new(0, 0, 120, 32);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(screen.contains(expected), "{popup} popup should not clip");
    }
    app.analysis_modal.quality.show_access = false;

    app.analysis_modal.set_quality_page(QualityPage::Segments);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('b'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.quality.plan.comparison,
        datui::analysis::data_quality::QualityComparison::Baseline
    );
    assert!(app.analysis_modal.quality.plan.baseline_segment.is_some());
    assert!(!app.is_busy());
    // Column and measure are the Trends chart's; Segments shows every column.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('4'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    // The measure the Trends table draws; every column is on it at once.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('m'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.quality.metric,
        datui::analysis::data_quality::QualityMetric::EmptyRate
    );

    // Enter on a highlighted column must open that column, not the first one.
    app.analysis_modal.set_quality_page(QualityPage::Columns);
    app.analysis_modal.quality.table_state.select(Some(3));
    let fourth = app.analysis_modal.quality.results.as_ref().unwrap().columns[3]
        .name
        .clone();
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Detail);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains(&fourth),
        "Detail should open the highlighted column {fourth}"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Columns);
    assert_eq!(
        app.analysis_modal.quality.table_state.selected(),
        Some(3),
        "returning from Detail should land back on the same column"
    );

    app.close_overlay();
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.quality.from_cache);
    assert_eq!(app.analysis_modal.quality.plan.sample_seed, 7_119);
    assert!(app.analysis_modal.quality.results.is_some());
    assert!(!app.is_busy());
    // Selecting the tool no longer moves focus; cross into the result as the
    // user would, with Tab.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // A drift observation's detail is the files themselves: which ones, how many rows
    // each cost the column, the type each holds, and the values the conflict hid.
    {
        let results = app.analysis_modal.quality.results.as_mut().unwrap();
        // Changed in place, the report is built again.
        results.edit(|results| {
            results.observations = vec![datui::analysis::data_quality::QualityObservation {
                kind: datui::analysis::data_quality::ObservationKind::TypeConflict,
                column: "fee".to_string(),
                affected_rows: 2,
                evaluated_rows: 7,
                fact: "1 of 3 files holds a type the scan cannot read".to_string(),
                normalized_category: None,
                files: vec![datui::analysis::data_quality::QualityFileEvidence {
                    number: 2,
                    name: "b.parquet".to_string(),
                    rows: 2,
                    stored_type: Some("str".to_string()),
                    examples: vec!["sixty".to_string()],
                }],
                time_format: None,
                full_scale: None,
            }];
        });
        app.analysis_modal.set_quality_page(QualityPage::Overview);
        app.analysis_modal.quality.table_state.select(Some(0));
        app.analysis_modal.quality.observation_detail = true;
        let area = Rect::new(0, 0, 120, 32);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        for expected in [
            "#2 b.parquet",
            "as str",
            "sixty",
            "rows of the loaded source",
        ] {
            assert!(
                screen.contains(expected),
                "the conflict detail should show {expected:?}"
            );
        }
        app.analysis_modal.quality.observation_detail = false;
    }

    let column = app
        .data_table_state
        .as_ref()
        .unwrap()
        .schema()
        .iter_names()
        .next()
        .unwrap()
        .to_string();
    let original_view = app.data_table_state.as_ref().unwrap().len_generation();
    let results = app.analysis_modal.quality.results.as_mut().unwrap();
    results.edit(|results| {
        results.precision = datui::analysis::data_quality::QualityPrecision::Exact;
        results.observations = vec![datui::analysis::data_quality::QualityObservation {
            kind: datui::analysis::data_quality::ObservationKind::Nulls,
            column,
            affected_rows: 0,
            evaluated_rows: results.evaluated_rows,
            fact: "matching rows".to_string(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
            full_scale: None,
        }];
    });
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.quality.observation_detail);
    // The run's rows are kept: they are cut in memory, off the UI thread.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    drain_events(&mut app, &rx);
    assert_ne!(app.overlay, Overlay::Analysis);
    let area = Rect::new(0, 0, 80, 24);
    let mut evidence_buffer = Buffer::empty(area);
    app.render(area, &mut evidence_buffer);
    assert!(
        evidence_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("Back")
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    assert_ne!(app.overlay, Overlay::Analysis);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::Analysis);
    assert!(app.analysis_modal.quality.observation_detail);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().len_generation(),
        original_view
    );

    app.close_overlay();
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.reverse());
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.quality.from_cache);
    assert!(app.analysis_modal.quality.results.is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
}

/// The Data Quality scope input is a text field: `?` must type into the scope
/// command instead of opening help. Ctrl-C quits from it, as from anywhere (#649).
#[test]
fn test_data_quality_scope_input_owns_question_mark() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::QualityPage;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );

    // e to Setup, Space on the Sample row opens the Sample form, whose first row
    // is the scope typed as text.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char(' '),
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.sample_form.is_some());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    // Rows from is a choice; a row range brings rows that are typed into.
    assert!(!app.text_field_focused());
    for code in [KeyCode::Right, KeyCode::Down] {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::analysis::sample_modal::SampleField::RangeFrom
    );
    assert!(app.text_field_focused());

    // Ctrl-C quits from a text row too (#649).
    let quit = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        matches!(quit, Some(AppEvent::Exit)),
        "Ctrl-C in a sample text row quits"
    );

    let scope = |app: &App| {
        app.analysis_modal
            .sample_form
            .as_ref()
            .unwrap()
            .range_from
            .value()
            .to_string()
    };
    // The prefilled value is selected, so what is typed replaces it.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        scope(&app),
        "?",
        "? in a sample text row must type, not open help"
    );
}

/// On a local file, choosing Data Quality opens its Setup, and Enter there runs the
/// default plan and leads with the result; Setup stays one e away. Tab crosses
/// sidebar and result here exactly as in the other tools.
#[test]
fn data_quality_on_a_local_file_leads_with_the_result() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::QualityPage;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_local_lead.csv");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(
            next,
            Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
        ),
        "a local default plan runs without ceremony"
    );
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);

    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert!(app.analysis_modal.quality.results.is_some());
    assert_eq!(
        app.analysis_modal.quality.page,
        QualityPage::Overview,
        "the result leads; the plan stays an Esc away"
    );
    // The run started from the form, so the cursor is on the result it made.
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    for expected in [AnalysisFocus::Sidebar, AnalysisFocus::Main] {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));
        assert_eq!(app.analysis_modal.focus, expected, "Tab crosses both ways");
    }

    // e from the result opens Setup.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);

    // The footer is the one hint surface: the widget draws no key rows
    // or prose of its own.
    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        !screen.contains("The plan is inert"),
        "the dimmed prose line is gone"
    );
    assert!(
        screen.contains("Run") && screen.contains("Space"),
        "the global bar names Setup's keys"
    );
}

/// The overview is a report: a verdict first, problems ranked above notes, columns
/// that go missing together said once, the clean columns named, and a grouped
/// finding still opens exactly its rows.
#[test]
fn data_quality_reads_as_a_report() {
    use datui::analysis::data_quality::QualityPage;

    let dir = common::fixture_dir();
    let path = dir.join("dq_report.parquet");
    let gap = |row: i64| row % 50 == 7;
    let mut df = df!(
        "id" => (0..200i64).collect::<Vec<_>>(),
        "open" => (0..200i64).map(|row| (!gap(row)).then_some(row as f64)).collect::<Vec<_>>(),
        "close" => (0..200i64).map(|row| (!gap(row)).then_some(row as f64 + 0.5)).collect::<Vec<_>>(),
        "region" => (0..200i64).map(|row| ["West", "west ", "East"][row as usize % 3]).collect::<Vec<_>>(),
        "market" => vec!["US"; 200],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // Before the first run the pane is Setup: every setting, and what a run reads,
    // before anything is read.
    let first = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(first);
    app.render(first, &mut buffer);
    let screen = common::buffer_text(&buffer);
    for section in [
        "Rows & sample",
        "Columns",
        "Study",
        "Read",
        "Time roles",
        "Grain",
    ] {
        assert!(
            screen.contains(section),
            "Setup shows {section:?}:\n{screen}"
        );
    }
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    // The run shows the progress every tool shows: the phase, the clock and what
    // it reads, in place of the view. No plan page, no popup over it.
    let mut buffer = Buffer::empty(first);
    app.render(first, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("Preparing the plan") && screen.contains("random rows"),
        "the shared progress view"
    );
    for gone in ["PROFILE PLAN", "Running", "Planned"] {
        assert!(!screen.contains(gone), "{gone:?} shows during a run");
    }
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    // The run started from the form, so the cursor is already on the report.
    assert_eq!(
        app.analysis_modal.focus,
        datui::analysis::analysis_modal::AnalysisFocus::Main
    );

    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    for expected in [
        "1 problem  2 notes  1 of 5 columns clean",
        "Data Quality",
        "all 200 rows",
        "Overview",
        "Columns",
        "Segments",
        "Trends",
        "Mixed spellings",
        "Missing together",
        "4 rows (2.0%)",
        "Single value",
        "No findings",
    ] {
        assert!(
            screen.contains(expected),
            "overview should show {expected:?}"
        );
    }
    let problem = screen.find("Mixed spellings").unwrap();
    assert!(
        problem < screen.find("Missing together").unwrap(),
        "problems rank above notes"
    );
    assert!(
        !screen.contains("Measured fact"),
        "no raw observation table on the overview"
    );
    for gone in ["Result", "checked", "seed"] {
        assert!(!screen.contains(gone), "{gone:?} repeats the header");
    }

    // The arrows walk the tabs, and every page's footer has one shape: the way out,
    // the page's own action, then Setup.
    let bar = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let bar_row = (area.height as usize - 1) * area.width as usize;
        buffer.content()[bar_row..]
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    };
    let shared = "e Setup";
    for (page, own) in [
        (QualityPage::Overview, "Enter Details"),
        (QualityPage::Columns, "Enter Inspect"),
        (QualityPage::Segments, "Enter Set grain"),
        (QualityPage::Trends, "Enter Set grain"),
    ] {
        if page != QualityPage::Overview {
            app.event(AppEvent::Key(KeyEvent::new(
                KeyCode::Right,
                KeyModifiers::NONE,
            )));
        }
        assert_eq!(app.analysis_modal.quality.page, page);
        assert!(
            bar(&mut app).contains(&format!("{own}  {shared}  Esc Tools")),
            "{page:?}: {:?}",
            bar(&mut app)
        );
    }
    // An empty Trends page names the setting that fills it, and Enter opens it.
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(screen.contains("Set grain") && !screen.contains("Metric"));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    // Straight to the Grain choices, in Setup, in the words the header uses.
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis::analysis_modal::SetupRow::Grain
    );
    assert!(app.analysis_modal.quality.picker.is_some());
    let bar_now = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let bar_row = (area.height as usize - 1) * area.width as usize;
        buffer.content()[bar_row..]
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    };
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(screen.contains("whole dataset") && screen.contains("in chunks of"));
    assert!(bar_now(&mut app).contains("Choose"));
    // Choosing (Space, as in every picker) stages the edit; the report keeps the plan
    // it was measured with, and nothing runs.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char(' '),
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.quality.picker.is_none());
    assert_ne!(
        app.analysis_modal.quality.plan.grain,
        datui::analysis::data_quality::QualityGrain::Dataset
    );
    assert!(app.analysis_modal.quality_plan_pending());
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    let bar = bar_now(&mut app);
    assert!(bar.contains("Discard") && bar.contains("Run"), "{bar}");
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(common::buffer_text(&buffer).contains("Enter runs, Esc discards"));
    // Esc discards the staged edit and goes back to the report.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.quality_plan_pending());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    // Each row names what Space does with it.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(bar_now(&mut app).contains("Space Sample"));
    for _ in 0..2 {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis::analysis_modal::SetupRow::TimeRoles
    );
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("needs an interval to measure"),
        "no threshold without an interval:\n{screen}"
    );
    // Text columns can take a role, read through a format.
    assert!(bar_now(&mut app).contains("Time roles"));
    // Enter runs from any row; the plan is the one measured, so the report opens.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('1'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    assert!(app.analysis_modal.quality.results.is_some());
    // With the tool list focused the bar names its keys, not the page's.
    let tab = || AppEvent::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    app.event(tab());
    let bar = bar_now(&mut app);
    assert!(
        bar.contains("Enter Open") && !bar.contains("Details"),
        "{bar}"
    );
    app.event(tab());
    assert!(bar_now(&mut app).contains("Details"));

    // The clean entry lists what was checked: the most important few, then all.
    for code in [KeyCode::End, KeyCode::Enter] {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }
    assert!(app.analysis_modal.quality.observation_detail);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("Checks"),
        "the clean entry names its checks"
    );
    assert!(screen.contains("Duplicate rows"));
    assert!(screen.contains("4 more checks"));
    assert!(
        screen.contains("All checks"),
        "the bar says Enter shows the rest"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        app.analysis_modal.quality.observation_detail,
        "Enter on the clean entry expands, it does not close"
    );
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(screen.contains("Nearly unique") && screen.contains("Fewer checks"));
    for code in [KeyCode::Esc, KeyCode::Home] {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }

    // The grouped finding opens every row it counts, and only those.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.quality.observation_detail);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("null in both columns")
            && screen.contains("No row misses one without the other"),
        "the detail says the columns go missing together"
    );
    assert!(
        !screen.contains("Why it matters"),
        "facts and advice as a list, not a lecture"
    );
    assert!(
        screen.contains("Show rows"),
        "the bar names what Enter does"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    drain_events(&mut app, &rx);
    assert_ne!(app.overlay, Overlay::Analysis);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 4);
}

/// A finding with more evidence than the screen holds scrolls inside its popup,
/// counts what is below, and lists the values with the most rows first.
#[test]
fn a_long_finding_scrolls() {
    use datui::analysis::data_quality::QualityPage;

    let dir = common::fixture_dir();
    let path = dir.join("dq_long_finding.parquet");
    // Sixty names, each also written in capitals; name 0 has the most rows.
    let names = (0..3_000usize)
        .map(|row| {
            let name = format!("Company {:02}", row % 60);
            if row % 120 < 60 {
                name
            } else {
                name.to_uppercase()
            }
        })
        .chain(std::iter::repeat_n("Company 00".to_string(), 500))
        .collect::<Vec<_>>();
    let rows = names.len() as i64;
    let mut df = df!("id" => (0..rows).collect::<Vec<_>>(), "name" => names).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    let press = |app: &mut App, code| {
        let mut next = app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
        while let Some(ev) = next {
            next = app.event(ev);
        }
    };
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    app.analysis_modal.quality.table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.quality.observation_detail);

    let area = Rect::new(0, 0, 100, 20);
    let render = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let screen = render(&mut app);
    assert!(screen.contains("Mixed spellings"));
    assert!(
        screen.contains("\"Company 00\" (525)  \"COMPANY 00\" (25)"),
        "the value with the most rows leads, its commonest spelling first"
    );
    assert!(screen.contains(" more "), "the frame counts what is below");
    assert!(screen.contains("Scroll"), "the bar says the popup scrolls");
    press(&mut app, KeyCode::End);
    let screen = render(&mut app);
    assert!(!screen.contains(" more "), "nothing left below at the end");
    assert!(screen.contains("Show rows"), "the Enter line is reachable");
    press(&mut app, KeyCode::Home);
    assert_eq!(app.analysis_modal.quality.detail_scroll.offset, 0);
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.quality.observation_detail);
}

/// A finding measured on a sample opens the sample's matching rows, drawn again
/// from its seed: the count the popup promises is the count in the table.
#[test]
fn a_sampled_finding_opens_its_sampled_rows() {
    use datui::analysis::data_quality::{QualityPage, QualityPrecision};

    let dir = common::fixture_dir();
    let path = dir.join("dq_sampled_evidence.parquet");
    let mut df = df!(
        "id" => (0..5_000i64).collect::<Vec<_>>(),
        "v" => (0..5_000i64).map(|row| (row % 10 != 3).then_some(row)).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    // A table sorted on screen: the sample is drawn from the unsorted rows, as
    // every tool draws it, so the sort changes nothing about which rows it holds.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(vec!["id".to_string()], false);
    pump_until_idle(&mut app, &rx, &tx);
    app.analysis_modal.sample.rows = 1_000;

    let enter = || AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(enter());
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let report = datui::analysis::quality_report::build_report(results);
    let index = report
        .findings
        .iter()
        .position(|finding| finding.title == "Missing values")
        .expect("v is missing in a tenth of the rows");
    let expected = report.findings[index].affected_rows;
    assert!(expected > 0 && expected < 1_000);

    app.analysis_modal.quality.table_state.select(Some(index));
    app.event(enter());
    assert!(app.analysis_modal.quality.observation_detail);
    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains(&format!("Enter: the {expected} sampled rows")),
        "the popup says which rows open"
    );
    assert!(screen.contains("Show rows"));
    assert!(!screen.contains("full profile"));

    let mut next = app.event(enter());
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::Analysis);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows(),
        expected,
        "exactly the rows the finding counted"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.overlay,
        Overlay::Analysis,
        "Esc goes back to the report"
    );
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 5_000);
}

/// The findings list narrows by column and by type and orders by rows or rate from
/// the report on screen: nothing is read and nothing is measured again. Duplicate
/// rows and text that does not parse open exactly the rows the run counted, from
/// the rows it kept, even once the file is gone; a finding with no rows says why.
#[test]
fn findings_narrow_order_and_open_kept_evidence_without_a_read() {
    use datui::analysis::data_quality::{QualityPage, QualityPrecision};
    use datui::analysis::quality_report::FindingOrder;

    let name = "dq_findings_kept.parquet";
    let (mut app, rx, tx, path) = open_findings_fixture(name);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A sample smaller than the table, so what opens is the sample's.
    app.analysis_modal.quality.plan.dataset_rows = 800;
    app.analysis_modal.quality.plan.sample_seed = 11;
    let next = press(&mut app, KeyCode::Enter);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let results = app.analysis_modal.quality.results.clone().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let report = datui::analysis::quality_report::build_report(&results);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    // Narrow to a column, then a type; order by rows, then by rate.
    assert!(press(&mut app, KeyCode::Char('c')).is_none());
    assert!(app.analysis_modal.quality.picker.is_some());
    type_text(&mut app, "code");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    let view = app.analysis_modal.quality.findings.clone();
    assert_eq!(view.column.as_deref(), Some("code"));
    let listed = app.analysis_modal.quality_row_count();
    assert!(listed >= 1 && listed < report.findings.len());
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        assert!(
            screen.contains(&format!("{listed} of ")) && screen.contains("column code"),
            "the list says it is narrowed at {width}x{height}:\n{screen}"
        );
        assert!(screen.contains("Numbers as text"), "{screen}");
        assert!(!screen.contains("Missing values"), "{screen}");
        assert_glyph_slots(&screen);
    }
    assert!(
        render(&mut app, 120, 30).contains("All findings"),
        "Esc says what it does"
    );
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.quality.findings.narrowed());
    assert_eq!(
        app.overlay,
        Overlay::Analysis,
        "Esc showed every finding; it did not leave"
    );
    press(&mut app, KeyCode::Char('t'));
    type_text(&mut app, "missing");
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.quality.findings.check,
        Some("Missing values")
    );
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.quality.findings.order,
        FindingOrder::Rows
    );
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.quality.findings.order,
        FindingOrder::Rate
    );
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        let middot = datui::glyphs::get().middot;
        assert!(
            screen.contains(&format!("Missing values {middot} by rate")),
            "{screen}"
        );
        assert_glyph_slots(&screen);
    }
    // The grouped finding: each column's count, and the rows with any of them
    // bounded, not summed.
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.quality.observation_detail);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Null rate in 2 columns"), "{screen}");
    assert!(screen.contains("Rows with any of them"), "{screen}");
    assert!(screen.contains("not counted"), "{screen}");
    assert_glyph_slots(&screen);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.quality.findings.order,
        FindingOrder::Ranked
    );

    // None of it read or measured anything.
    assert_nothing_started(&mut app, &rx);
    let after = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(after.observations.len(), results.observations.len());
    assert_eq!(
        datui::analysis::quality_report::verdict(&datui::analysis::quality_report::build_report(
            after
        )),
        datui::analysis::quality_report::verdict(&report)
    );

    // From here on only the kept rows can answer.
    std::fs::remove_file(&path).unwrap();
    let open = |app: &mut App, check: &'static str| {
        app.analysis_modal.quality.findings.check = Some(check);
        app.analysis_modal.quality.table_state.select(Some(0));
        press(app, KeyCode::Enter);
        assert!(app.analysis_modal.quality.observation_detail);
    };
    let identity = results.identity.clone().unwrap();
    assert!(identity.rows_involved > 0, "the sample holds copies");
    open(&mut app, "Duplicate rows");
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Most copied"), "{screen}");
    assert!(
        screen.contains(&format!(
            "{} sampled rows that have a copy",
            identity.rows_involved
        )),
        "{screen}"
    );
    assert!(screen.contains("Show rows"), "{screen}");
    assert_glyph_slots(&screen);
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::Analysis);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().num_rows(),
        identity.rows_involved
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Analysis);
    assert!(
        app.analysis_modal.quality.observation_detail,
        "Esc from the rows is the finding again"
    );
    press(&mut app, KeyCode::Esc);

    // The code column's values that do not parse, and nothing else.
    let (_, finding) = {
        app.analysis_modal.quality.findings.check = Some("Numbers as text");
        app.analysis_modal.quality.findings.column = Some("code".to_string());
        app.analysis_modal.quality.table_state.select(Some(0));
        app.analysis_modal.selected_finding().unwrap()
    };
    let failures = finding.failures(&results).unwrap();
    assert!(failures > 0);
    press(&mut app, KeyCode::Enter);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("do not parse, such as \"n/a\""), "{screen}");
    assert!(
        screen.contains(&format!("{failures} sampled rows that do not parse")),
        "{screen}"
    );
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), failures);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);

    // Codes that all parse: no rows to show, and the finding says why.
    app.analysis_modal.quality.findings.column = Some("zip".to_string());
    app.analysis_modal.quality.table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("every value parses"), "{screen}");
    assert!(
        screen.contains("Close") && !screen.contains("Show rows"),
        "{screen}"
    );
    press(&mut app, KeyCode::Enter);
    assert!(!app.analysis_modal.quality.observation_detail);
    assert!(app.overlay == Overlay::Analysis && !app.is_busy());
}

/// A full scan keeps no rows, so a finding's rows are a read of their own: Enter
/// shows what it would read, Esc reads nothing, and only Enter on that reads.
#[test]
fn full_scan_evidence_is_read_only_on_confirm() {
    use datui::analysis::data_quality::QualityPrecision;

    let (mut app, rx, tx, _path) = open_findings_fixture("dq_findings_full.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.analysis_modal.quality.plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
    app.analysis_modal.quality.plan.compute = datui::analysis::data_quality::QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.asks_full_scan());
    // The one confirmation, with what the scan reads and writes and its own keys.
    assert!(app.confirmation_modal.active);
    assert_eq!(app.confirmation_modal.yes_label, "Run");
    assert!(
        app.confirmation_modal
            .message
            .contains("Source writes: none"),
        "{}",
        app.confirmation_modal.message
    );
    let asked = {
        let area = Rect::new(0, 0, 80, 24);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    assert!(asked.contains("Run a full scan?"), "{asked}");
    assert!(
        asked.contains("Confirm") && asked.contains("Cancel"),
        "its own footer: {asked}"
    );
    // Esc declines: nothing runs, and Enter on Setup asks again.
    assert!(press(&mut app, KeyCode::Esc).is_none());
    assert!(!app.confirmation_modal.active);
    assert!(!app.confirmation_modal.asks_full_scan());
    assert_nothing_started(&mut app, &rx);
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.active);
    let next = press(&mut app, KeyCode::Enter);
    assert!(!app.confirmation_modal.active);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    let results = app.analysis_modal.quality.results.clone().unwrap();
    assert_eq!(results.precision, QualityPrecision::Exact);
    let identity = results.identity.clone().unwrap();
    assert_eq!(
        identity.rows_involved, 900,
        "three copies of three hundred rows"
    );
    assert!(
        identity.examples.is_empty(),
        "a full scan keeps no examples"
    );

    app.analysis_modal.quality.findings.check = Some("Duplicate rows");
    app.analysis_modal.quality.table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let screen = render(&mut app, 80, 24);
    assert!(
        screen.contains("full scan keeps none, asks first"),
        "{screen}"
    );
    assert!(screen.contains("Read rows"), "{screen}");

    // Enter stages the read and shows it; nothing reads yet.
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.quality.evidence_read.is_some());
    assert_nothing_started(&mut app, &rx);
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        for label in [
            "Read Rows",
            "Why",
            "Reads",
            "Shows",
            "900 rows",
            "read only",
        ] {
            assert!(
                screen.contains(label),
                "{label} at {width}x{height}: {screen}"
            );
        }
        assert!(
            screen.contains("Enter") && screen.contains("Cancel"),
            "{screen}"
        );
        assert_glyph_slots(&screen);
    }
    // Esc reads nothing and goes back to the finding.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.quality.evidence_read.is_none());
    assert!(app.analysis_modal.quality.observation_detail);
    assert_nothing_started(&mut app, &rx);

    // Enter, and Enter again: the read, and exactly the rows the check counted.
    press(&mut app, KeyCode::Enter);
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::Analysis);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 900);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Analysis);
}

/// A plan that needs a run is a form the user answers with Enter, which only the
/// main pane hears. The cursor must move into it when the ceremony opens: left on
/// the sidebar, ↑↓ went on moving the tool selector and Enter only reselected the
/// tool, and there was no visible way to run the plan at all.
#[test]
fn the_data_quality_ceremony_takes_the_cursor_with_it() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::QualityPage;

    let (mut app, _rx, _tx) = open_query_filter_fixture("dq_ceremony_focus.csv");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    // A full-scan plan requires confirmation, so running the tool opens the
    // ceremony instead of reading at once.
    app.analysis_modal.sample.method = datui::analysis::sampling::SampleMethod::EveryRow;
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        next.is_none(),
        "a confirming plan must not run on selection"
    );
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Main,
        "the ceremony owns the keys"
    );

    // The run asked for confirmation, and Enter confirms — no Tab required.
    assert!(app.confirmation_modal.asks_full_scan());
    let next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
}

/// e opens the plan editor, and the editor owns the keyboard: ↑↓ change the field,
/// never the sidebar's tool selector. Tab is swallowed while the editor is open,
/// so unless the cursor moves in with e, no key could ever reach a field.
#[test]
fn e_moves_the_cursor_into_the_plan_editor() {
    use datui::analysis::analysis_modal::AnalysisFocus;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_editor_focus.csv");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.quality.results.is_some());
    // From the tool list: the case where e has to bring the cursor along.
    app.analysis_modal.focus = AnalysisFocus::Sidebar;

    let tool_row = app.analysis_modal.sidebar_state.selected();
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.quality.page,
        datui::analysis::data_quality::QualityPage::Setup
    );
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Main,
        "e moves the cursor onto the plan"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.quality.plan_field, 1,
        "the arrow changes the field"
    );
    assert_eq!(
        app.analysis_modal.sidebar_state.selected(),
        tool_row,
        "the tool selector never moves"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Up,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.quality.plan_field, 0);
}

/// r works from the sidebar too, on a sampled report: it runs at once, with a new
/// seed. A plan that reads every row has no sample to draw again, so r there
/// raises no confirmation over the report and changes nothing.
#[test]
fn r_from_the_sidebar_runs_a_sampled_report_again() {
    use datui::analysis::analysis_modal::AnalysisFocus;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_run_key_focus.csv");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    app.analysis_modal.focus = AnalysisFocus::Sidebar;

    let full = {
        let mut plan = app.analysis_modal.quality.plan.clone();
        plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
        plan.compute = datui::analysis::data_quality::QualityCompute::Full;
        plan
    };
    let sampled = std::mem::replace(&mut app.analysis_modal.quality.plan, full.clone());
    assert!(press(&mut app, KeyCode::Char('r')).is_none());
    assert!(!app.confirmation_modal.asks_full_scan());
    assert_eq!(app.analysis_modal.quality.plan, full);

    app.analysis_modal.quality.plan = sampled;
    let seed = app.analysis_modal.quality.plan.sample_seed;
    let next = press(&mut app, KeyCode::Char('r'));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    assert_ne!(app.analysis_modal.quality.plan.sample_seed, seed);
}

/// Nothing Setup does reads: not choosing Data Quality after another tool
/// sampled, not the Sample form's Enter, not a row's choice. Esc puts back all of
/// it, the shared sample included; declining a full read leaves the sample and
/// the report as they were; one Run is one read; and the same setup again is the
/// report already here.
#[test]
fn data_quality_reads_nothing_until_setup_runs() {
    use datui::analysis::analysis_modal::SetupRow;
    use datui::analysis::data_quality::{QualityGrain, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_reads_nothing.parquet");
    press(&mut app, KeyCode::Char('a'));
    // Describe first: its sample is the one every tool reads from now on.
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert!(app.analysis_modal.describe_results.is_some());
    let shared = app.analysis_modal.sample.clone();

    // Choosing Data Quality opens Setup on that sample, and reads nothing.
    app.analysis_modal.focus = datui::analysis::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.quality.plan.sample(), shared);
    assert!(app.analysis_modal.quality.results.is_none());

    // The Sample form's Enter applies to Setup and returns to it.
    press(&mut app, KeyCode::Char('s'));
    assert!(app.analysis_modal.sample_form.is_some());
    let form = app.analysis_modal.sample_form.as_mut().unwrap();
    form.seed.set_value("99");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.sample_form.is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.quality.plan.sample_seed, 99);
    assert_eq!(
        app.analysis_modal.sample, shared,
        "the shared sample waits for Run"
    );
    // A row's choice, in place and from its list, reads nothing either.
    app.analysis_modal.quality.plan_field = SetupRow::Compare.index();
    assert!(press(&mut app, KeyCode::Right).is_none());
    app.analysis_modal.quality.plan_field = SetupRow::Grain.index();
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    type_text(&mut app, "chunks of 100,");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(
        app.analysis_modal.quality.plan.grain,
        QualityGrain::RowChunks(100_000)
    );
    assert!(press(&mut app, KeyCode::Char('p')).is_none());
    press(&mut app, KeyCode::Esc);
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert!(app.analysis_modal.describe_results.is_some());

    // Esc discards every staged edit, the sample's included.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.plan.sample(), shared);
    assert_eq!(app.analysis_modal.quality.plan.grain, QualityGrain::Dataset);
    assert_eq!(
        app.analysis_modal.focus,
        datui::analysis::analysis_modal::AnalysisFocus::Sidebar,
        "with no report yet, the cursor goes back to the tools"
    );

    // One Run: one read, one report.
    press(&mut app, KeyCode::Tab);
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    let (finished, stages) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1, "one Run dispatches once");
    assert!(stages > 0, "the run names its stages");
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let report = app.analysis_modal.quality.results.clone().unwrap();

    // Declining a full read leaves the sample and the report as they were, and
    // the draft staged.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
    app.analysis_modal.quality.plan.compute = datui::analysis::data_quality::QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.asks_full_scan());
    assert!(press(&mut app, KeyCode::Esc).is_none());
    assert!(!app.confirmation_modal.asks_full_scan());
    assert_eq!(app.analysis_modal.sample, shared);
    assert_eq!(
        app.analysis_modal
            .quality
            .results
            .as_ref()
            .unwrap()
            .evaluated_rows,
        report.evaluated_rows
    );
    assert!(
        app.analysis_modal.setup_edited(),
        "the draft is still staged"
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);

    // The setup the report was measured with is the report: no read.
    press(&mut app, KeyCode::Char('e'));
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(common::buffer_text(&buffer).contains("Report on screen: this setup"));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);

    // Reopened, the unchanged report comes back from the session cache. Esc
    // steps back to the tools, then closes.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Analysis);
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::Analysis);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(app.analysis_modal.quality.from_cache);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
}

/// Each edit reads only what it must, counted by the stages of each run that read
/// the source: #415's "What edits should cost", on a CSV the sampler streams. The
/// first daily run counts its days in the pass that samples. Roles, a coarser window
/// the days nest in, and row chunks read nothing. A partition is counted once. A new
/// seed, size or scope is a new sample, and an earlier seed's rows are still here.
/// Setup says each of these before Run.
#[test]
fn data_quality_edits_read_only_what_they_must() {
    use datui::analysis::data_quality::{
        QualityGrain, QualityScope, QualityStage, TemporalRole, TemporalRoleAssignment,
    };

    let dir = common::fixture_dir();
    let path = dir.join("dq_reuse_reads.csv");
    // Every 97 minutes from 2024-01-01, across weeks and months.
    let minute = 60_000_000i64;
    let start = 1_704_067_200_000_000i64;
    let micros = |offset: i64| {
        (0..3_000i64)
            .map(|row| start + row * 97 * minute + offset)
            .collect::<Vec<_>>()
    };
    let datetime = DataType::Datetime(TimeUnit::Microseconds, None);
    let mut df = df!(
        "id" => (0..3_000i64).collect::<Vec<_>>(),
        "at" => micros(0),
        "sent" => micros(40_000_000),
        "region" => (0..3_000).map(|row| ["North", "South", "East"][row % 3]).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_columns([col("at").cast(datetime.clone()), col("sent").cast(datetime)])
    .collect()
    .unwrap();
    CsvWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let setup = |app: &mut App| {
        let area = Rect::new(0, 0, 120, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let window = |every: &str| QualityGrain::TimeWindows {
        column: "at".into(),
        every: every.into(),
    };
    let seed = app.analysis_modal.quality.plan.sample_seed;
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.dataset_rows = 300;
        plan.grain = window("1d");
    }
    assert!(setup(&mut app).contains("counted by at in that pass"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::ReadingSample],
        "one pass samples and counts the days"
    );

    // Edits that change only the report.
    let edit =
        |app: &mut App, change: &dyn Fn(&mut datui::analysis::data_quality::DataQualityPlan)| {
            press(app, KeyCode::Char('e'));
            change(&mut app.analysis_modal.quality.plan);
        };
    edit(&mut app, &|plan| {
        plan.temporal_roles = vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "at".into(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "sent".into(),
                timezone: None,
            },
        ]
    });
    let text = setup(&mut app);
    assert!(text.contains("Rows: from an earlier run"), "{text}");
    assert!(text.contains("Segment totals: from an earlier count"));
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "a role edit");
    assert!(
        !app.analysis_modal
            .quality
            .results
            .as_ref()
            .unwrap()
            .temporal
            .is_empty()
    );
    edit(&mut app, &|plan| plan.grain = window("1w"));
    assert!(setup(&mut app).contains("summed from earlier daily counts"));
    assert!(
        run_quality_reads(&mut app, &rx).is_empty(),
        "weeks from days"
    );
    edit(&mut app, &|plan| plan.grain = QualityGrain::RowChunks(500));
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "row chunks");
    edit(&mut app, &|plan| {
        plan.grain = QualityGrain::Partition("region".into())
    });
    assert!(setup(&mut app).contains("+1 count of region"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::CountingSegments],
        "a partition is counted once"
    );

    // Other rows: a new sample, counted in its pass.
    for change in [
        &(|plan: &mut datui::analysis::data_quality::DataQualityPlan| plan.sample_seed = 7)
            as &dyn Fn(&mut datui::analysis::data_quality::DataQualityPlan),
        &|plan| plan.dataset_rows = 400,
        &|plan| plan.scope = QualityScope::FirstRows(2_000),
    ] {
        edit(&mut app, change);
        let text = setup(&mut app);
        assert!(
            text.contains("1 streaming pass over every eligible row"),
            "{text}"
        );
        assert!(text.contains("counted by region in that pass"), "{text}");
        assert_eq!(
            run_quality_reads(&mut app, &rx),
            [QualityStage::ReadingSample]
        );
    }

    // The first sample's rows are still held, with their counts.
    edit(&mut app, &|plan| {
        plan.scope = QualityScope::CurrentView;
        plan.dataset_rows = 300;
        plan.sample_seed = seed;
        plan.grain = window("1mo");
    });
    let text = setup(&mut app);
    assert!(text.contains("Rows: from an earlier run"), "{text}");
    assert!(text.contains("summed from earlier daily counts"), "{text}");
    assert!(
        run_quality_reads(&mut app, &rx).is_empty(),
        "an earlier seed"
    );
}

/// On one Parquet file the sampler reads seeded runs, which see too few rows to
/// count segments, so the run counts them in a pass of its own, and Setup says so
/// before Run: a sort leaves the unsorted scan the sample reads, and the whole
/// source is read as loaded whatever the view shows. A filtered view streams, and
/// counts in that pass.
#[test]
fn data_quality_setup_names_every_count_pass_on_one_parquet_file() {
    use datui::analysis::data_quality::{QualityGrain, QualityScope, QualityStage};
    use datui::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

    let dir = common::fixture_dir();
    let path = dir.join("dq_reuse_blocks.parquet");
    let minute = 60_000_000i64;
    let start = 1_704_067_200_000_000i64;
    let mut df = df!(
        "id" => (0..20_000i64).collect::<Vec<_>>(),
        "at" => (0..20_000i64).map(|row| start + row * 97 * minute).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("at").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
    .collect()
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let setup = |app: &mut App| {
        let area = Rect::new(0, 0, 120, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };
    let sorted = || AppEvent::Applied(datui::Applied::Sort(vec!["id".into()], vec![true]));
    let filtered = || {
        AppEvent::Applied(datui::Applied::Filter(vec![FilterStatement {
            columns: Vec::new(),
            column: "id".into(),
            operator: FilterOperator::Gt,
            value: "10".into(),
            logical_op: LogicalOperator::And,
        }]))
    };
    for (name, view, scope, blocks) in [
        ("as loaded", None, QualityScope::CurrentView, true),
        ("sorted", Some(sorted()), QualityScope::CurrentView, true),
        ("sorted", Some(sorted()), QualityScope::WholeSource, true),
        (
            "filtered",
            Some(filtered()),
            QualityScope::WholeSource,
            true,
        ),
        (
            "filtered",
            Some(filtered()),
            QualityScope::CurrentView,
            false,
        ),
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        if let Some(view) = view {
            let mut next = app.event(view);
            while let Some(event) = next {
                next = app.event(event);
            }
            pump_until_idle(&mut app, &rx, &tx);
        }
        press(&mut app, KeyCode::Char('a'));
        app.analysis_modal.sidebar_state.select(Some(3));
        show_sample_form(&mut app);
        {
            let plan = &mut app.analysis_modal.quality.plan;
            plan.dataset_rows = 300;
            plan.scope = scope.clone();
            plan.grain = QualityGrain::TimeWindows {
                column: "at".into(),
                every: "1d".into(),
            };
        }
        let text = setup(&mut app);
        let reads = run_quality_reads(&mut app, &rx);
        if blocks {
            assert!(
                text.contains("Seeded runs of the file") && text.contains("+1 count of at"),
                "{name} {scope:?}: Setup said\n{text}"
            );
            assert_eq!(
                reads,
                [QualityStage::ReadingSample, QualityStage::CountingSegments],
                "{name} {scope:?}"
            );
        } else {
            assert!(
                text.contains("1 streaming pass") && text.contains("counted by at in that pass"),
                "{name} {scope:?}: Setup said\n{text}"
            );
            assert_eq!(reads, [QualityStage::ReadingSample], "{name} {scope:?}");
        }
    }
}

/// Every report carries its coverage under the verdict: a sampled run says which
/// checks ran on the sample, which it could not answer and why, and the rows behind
/// it; a full run says every row was read and how many rows its passes traversed.
#[test]
fn data_quality_coverage_sits_under_every_verdict() {
    use datui::analysis::data_quality::{QualityCompute, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_coverage.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    let draw = |app: &mut App| {
        let area = Rect::new(0, 0, 80, 24);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };

    app.analysis_modal.quality.plan.dataset_rows = 100;
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let text = draw(&mut app);
    assert!(text.contains("Checks  "), "{text}");
    assert!(text.contains("unavailable"), "{text}");
    assert!(text.contains("100 of 600 sampled (16.7%)"), "{text}");
    assert!(
        text.contains("100 traversed"),
        "seeded runs of one Parquet file read only the rows they keep: {text}"
    );
    assert!(
        text.contains("Nearly unique: needs every row checked"),
        "{text}"
    );

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
    app.analysis_modal.quality.plan.compute = QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.asks_full_scan());
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    let text = draw(&mut app);
    assert!(text.contains("all 600 read, exact"), "{text}");
    assert!(!text.contains("unavailable"), "{text}");
    let reads = app
        .analysis_modal
        .quality
        .results
        .as_ref()
        .and_then(|results| results.reads)
        .unwrap();
    assert!(
        reads.counted > 0 && reads.rows >= 600,
        "every watched pass counted: {reads:?}"
    );
}

/// However Setup is reached, Esc discards what was staged in it: after Esc handed
/// the cursor to the tools and Tab brought it back, and after a report tab key
/// pressed in Setup, which does not leave it. A draft never rides out of Setup
/// into `r`.
#[test]
fn data_quality_setup_edits_never_outlive_esc() {
    use datui::analysis::analysis_modal::{AnalysisFocus, SetupRow};
    use datui::analysis::data_quality::{QualityComparison, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_esc_discards.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);

    // No report yet: Esc goes to the tools, Tab comes back, and an edit there is
    // still a staged one.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    app.analysis_modal.quality.plan_field = SetupRow::Compare.index();
    press(&mut app, KeyCode::Right);
    assert_ne!(
        app.analysis_modal.quality.plan.comparison,
        QualityComparison::None
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.quality.plan.comparison,
        QualityComparison::None,
        "Esc discards an edit made after Tab"
    );

    // A report, then a staged edit and a report tab's key: Setup stays, and so does
    // the draft, until Esc takes it away.
    press(&mut app, KeyCode::Tab);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let seed = app.analysis_modal.quality.plan.sample_seed;
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan_field = SetupRow::Compare.index();
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Char('2'));
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    assert_eq!(
        app.analysis_modal.quality.plan.comparison,
        QualityComparison::None
    );

    // r on the report runs the plan the report was measured with, a new seed aside.
    let next = press(&mut app, KeyCode::Char('r'));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    drain_quality(&mut app, &rx, next);
    let plan = &app.analysis_modal.quality.plan;
    assert_ne!(plan.sample_seed, seed);
    assert_eq!(plan.comparison, QualityComparison::None);

    // A report of every row has no sample to draw again: r does nothing there.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
    app.analysis_modal.quality.plan.compute = datui::analysis::data_quality::QualityCompute::Full;
    press(&mut app, KeyCode::Enter);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
    let full = app.analysis_modal.quality.plan.clone();
    assert!(press(&mut app, KeyCode::Char('r')).is_none());
    assert!(!app.confirmation_modal.asks_full_scan());
    assert_eq!(app.analysis_modal.quality.plan, full);
}

/// A full scan asks before it reads. Expected windows are checked against the
/// report on screen, which reads nothing, so Run does not ask.
#[test]
fn expected_windows_on_a_full_scan_run_without_asking() {
    use datui::analysis::data_quality::{
        ExpectedWindows, QualityCompute, QualityGrain, QualityPage,
    };

    let (mut app, rx, _tx) = open_weekday_feed("dq_expected_full.csv");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
        plan.grain = QualityGrain::TimeWindows {
            column: "day".into(),
            every: "1d".into(),
        };
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.asks_full_scan(), "a full scan asks");
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.expected = Some(ExpectedWindows {
        weekdays: true,
        ..ExpectedWindows::default()
    });
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(
        !app.confirmation_modal.asks_full_scan(),
        "nothing to confirm"
    );
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Trends);
    assert!(
        datui::analysis::quality_trends::expected_gaps(
            app.analysis_modal.quality_result_plan(),
            app.analysis_modal.quality.results.as_ref().unwrap(),
        )
        .is_some()
    );
}

/// A rerun that fails leaves the last report on screen, labeled with the setup it
/// was measured with, and says why it failed; the rows the first run kept still
/// serve the setup that made them, with no read.
#[test]
fn a_failed_rerun_keeps_the_last_report() {
    let (mut app, rx, _tx, path) = open_quality_fixture("dq_rerun_fails.parquet", 2_000, 10);
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.dataset_rows = 500;
        plan.sample_seed = 3;
    }
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::analysis::data_quality::QualityStage::ReadingSample]
    );
    let report = format!("{:?}", app.analysis_modal.quality.results);
    let measured = app.analysis_modal.quality.last_plan.clone().unwrap();

    // The source goes away, and a new seed needs it.
    std::fs::remove_file(&path).unwrap();
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.sample_seed = 4;
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 0, "the rerun failed");
    assert!(app.modal_showing(), "and says why");
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(
        format!("{:?}", app.analysis_modal.quality.results),
        report,
        "the last report stays"
    );
    assert_eq!(app.analysis_modal.quality_result_plan(), &measured);
    // The error is dismissed onto Setup, where the run started; Esc there is the
    // report, under the setup it was measured with.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.quality.page.is_setup());
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.quality.page.is_setup());
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let text = common::buffer_text(&buffer);
    assert!(text.contains("sample of 500"), "{text}");

    // The setup the report was measured with is still served from its rows.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.sample_seed = 3;
    app.analysis_modal.quality.plan.grain =
        datui::analysis::data_quality::QualityGrain::RowChunks(100_000);
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "no read");
    assert!(!app.modal_showing());
}

/// `d` in Setup releases the rows runs kept: Read names them before, the report on
/// screen stays, and an edit that would have reused them reads its sample again,
/// as Read says before Run.
#[test]
fn kept_rows_are_released_from_setup() {
    let (mut app, rx, _tx, _path) = open_quality_fixture("dq_release_rows.parquet", 2_000, 10);
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.dataset_rows = 500;
        plan.sample_seed = 3;
    }
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::analysis::data_quality::QualityStage::ReadingSample]
    );
    let screen = |app: &mut App| {
        let area = Rect::new(0, 0, 160, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        common::buffer_text(&buffer)
    };

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.quality.plan.grain =
        datui::analysis::data_quality::QualityGrain::RowChunks(100);
    let text = screen(&mut app);
    assert!(text.contains("500 rows kept"), "{text}");
    assert!(text.contains("Rows: from an earlier run"), "{text}");

    assert!(press(&mut app, KeyCode::Char('d')).is_none());
    assert!(!app.is_busy(), "releasing reads nothing");
    let flash = app.flash_message().unwrap_or_default().to_string();
    assert!(
        flash.starts_with("Released 500 kept rows") && flash.ends_with("the next run reads again"),
        "{flash}"
    );
    assert!(
        app.analysis_modal.quality.results.is_some(),
        "the report stays"
    );
    assert!(app.analysis_modal.quality.page.is_setup(), "and Setup");
    let text = screen(&mut app);
    assert!(!text.contains("rows kept"), "{text}");
    assert!(!text.contains("Release Rows"), "{text}");
    assert!(text.contains("Released since last read"), "{text}");
    press(&mut app, KeyCode::Char('d'));
    assert_eq!(app.flash_message(), Some("Nothing kept to release"));

    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::analysis::data_quality::QualityStage::ReadingSample],
        "the sample is read again"
    );
    press(&mut app, KeyCode::Char('e'));
    assert!(screen(&mut app).contains("500 rows kept"), "and kept again");
}

/// Conflict evidence costs a read only where one was promised. Footers name the
/// file that stores a column in another type, and the file missing a column, on
/// every run; the values the conflict hides are read only by a full scan, which
/// says so in its stages, and are shown beside their file.
#[test]
fn conflict_evidence_is_read_only_by_a_full_scan() {
    use datui::analysis::data_quality::{ObservationKind, QualityCompute, QualityStage};
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "day=1",
        df!("id" => &[1i64, 2], "n" => &[10i64, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "day=2",
        df!("id" => &[3i64, 4], "n" => &["sixty", "seventy"], "extra" => &["x", "y"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "day=3",
        df!("id" => &[5i64, 6], "n" => &[50i64, 60], "extra" => &[Some("z"), None]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    let finding = |app: &App, kind: ObservationKind| {
        app.analysis_modal
            .quality
            .results
            .as_ref()
            .unwrap()
            .observations
            .iter()
            .find(|observation| observation.kind == kind)
            .cloned()
            .unwrap_or_else(|| panic!("a {kind:?} finding"))
    };

    let reads = run_quality_reads(&mut app, &rx);
    assert!(
        !reads.contains(&QualityStage::ReadingConflicts),
        "{reads:?}"
    );
    let conflict = finding(&app, ObservationKind::TypeConflict);
    assert_eq!(conflict.column, "n");
    assert_eq!(conflict.files.len(), 1);
    assert_eq!(conflict.files[0].number, 2);
    assert!(
        conflict.files[0].examples.is_empty(),
        "not read on a sample"
    );
    let absent = finding(&app, ObservationKind::Absent);
    assert_eq!(absent.column, "extra");
    assert_eq!(absent.files[0].number, 1);

    press(&mut app, KeyCode::Char('e'));
    {
        let plan = &mut app.analysis_modal.quality.plan;
        plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
    }
    assert!(
        press(&mut app, KeyCode::Enter).is_none(),
        "a full scan asks"
    );
    let first = press(&mut app, KeyCode::Enter);
    let mut reads = Vec::new();
    let mut next = first;
    loop {
        match next.take() {
            Some(event) => {
                if let AppEvent::JobProgress {
                    ticket,
                    progress: datui::Progress::QualityPhase(phase),
                } = &event
                    && app.job_is_current(*ticket)
                    && phase.reads_source
                {
                    reads.push(phase.stage);
                }
                next = app.event(event);
            }
            None => match next_event(&app, &rx) {
                Some(event) => next = Some(event),
                None => break,
            },
        }
    }
    assert!(reads.contains(&QualityStage::ReadingConflicts), "{reads:?}");
    let conflict = finding(&app, ObservationKind::TypeConflict);
    assert_eq!(conflict.files[0].examples, ["sixty", "seventy"]);
}

/// An empty scope is never a clean report. Rows chosen past the table's end are
/// an error naming them, a second Run included, and no report. A dataset with no
/// rows at all, sampled or scanned, reports that nothing about its values is known:
/// no column called clean, no check passed.
#[test]
fn an_empty_scope_is_never_a_clean_report() {
    let (mut app, rx, _tx, _path) = open_quality_fixture("dq_empty_scope.csv", 1_000, 0);
    app.analysis_modal.quality.plan.scope = datui::analysis::data_quality::QualityScope::ViewRows {
        start: 5_000,
        end: 6_000,
    };
    for _ in 0..2 {
        let first = press(&mut app, KeyCode::Enter);
        let (finished, _) = drain_quality(&mut app, &rx, first);
        assert_eq!(finished, 0);
        assert!(app.modal_showing());
        let area = Rect::new(0, 0, 100, 30);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let text = common::buffer_text(&buffer);
        assert!(
            text.contains("No rows match view rows 5,000-6,000"),
            "{text}"
        );
        assert!(app.analysis_modal.quality.results.is_none());
        press(&mut app, KeyCode::Esc);
    }

    common::ensure_sample_data();
    for full in [false, true] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        let path = PathBuf::from("tests/sample-data/empty.parquet");
        pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        open_quality_setup(&mut app);
        let plan = &mut app.analysis_modal.quality.plan;
        // A declared key and a required column are no more checked than the rest.
        plan.intent = datui::analysis::quality_intent::DeclaredIntent {
            key: vec!["id".into()],
            columns: vec![datui::analysis::quality_intent::ColumnIntent {
                column: "name".into(),
                required: true,
                ..Default::default()
            }],
        };
        if full {
            plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
            plan.compute = datui::analysis::data_quality::QualityCompute::Full;
        }
        let mut first = press(&mut app, KeyCode::Enter);
        if app.confirmation_modal.asks_full_scan() {
            first = press(&mut app, KeyCode::Enter);
        }
        let (finished, _) = drain_quality(&mut app, &rx, first);
        assert_eq!(finished, 1);
        let results = app.analysis_modal.quality.results.clone().unwrap();
        assert_eq!(results.evaluated_rows, 0);
        let area = Rect::new(0, 0, 100, 30);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let text = common::buffer_text(&buffer);
        assert!(!text.contains("No problems found"), "full {full}: {text}");
        assert!(!text.contains("clean"), "full {full}: {text}");
        assert!(text.contains("No rows to check"), "full {full}: {text}");
        // Columns marks none of them clean either.
        app.analysis_modal
            .show_quality_tab(datui::analysis::data_quality::QualityPage::Columns);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let lines = common::buffer_lines(&buffer);
        let check = datui::glyphs::get().check;
        for column in ["id", "name", "value", "date"] {
            let line = lines
                .iter()
                .find(|line| line.split_whitespace().any(|word| word == column))
                .unwrap_or_else(|| panic!("{column} on Columns: {lines:#?}"));
            let before = &line[..line.find(&format!(" {column} ")).unwrap()];
            assert!(!before.contains(check), "full {full}: {line}");
        }
        let report = datui::analysis::quality_report::build_report(&results);
        for check in datui::analysis::quality_report::checks(&results, &report) {
            use datui::analysis::quality_report::Outcome;
            assert!(
                !matches!(check.outcome, Outcome::Passed | Outcome::Found { .. }),
                "{} ran on no rows",
                check.name
            );
        }
    }
}

#[test]
fn test_data_quality_scope_editor_runs_selected_view_rows() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::{QualityPage, QualityScope};

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );
    let key =
        |app: &mut App, code| app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    key(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A run of the default plan settles before the scope edit.
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    // s opens the Sample form over Setup; its scope row takes the rows.
    key(&mut app, KeyCode::Char('s'));
    assert!(app.analysis_modal.sample_form.is_some());
    for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 60, 20)] {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(
            screen.contains("Rows from:")
                && screen.contains("All rows")
                && screen.contains("Random"),
            "the form's values name themselves"
        );
    }
    let set_scope = |app: &mut App, text: &str| {
        let (from, to) = text.trim_start_matches("rows ").split_once("..").unwrap();
        let form = app.analysis_modal.sample_form.as_mut().unwrap();
        form.kind = datui::analysis::sample_modal::RowsKind::Range;
        form.range_from.set_value(from);
        form.range_to.set_value(to);
    };
    set_scope(&mut app, "rows 0..3");
    key(&mut app, KeyCode::Enter);
    let form = app.analysis_modal.sample_form.as_ref().unwrap();
    assert!(form.error.is_some(), "a bad scope stays in the form");
    set_scope(&mut app, "rows 2..3");
    // Enter applies the sample to Setup, and Setup's Enter runs it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.sample_form.is_none());
    assert!(next.is_none(), "applying the form reads nothing");
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.quality.plan.scope,
        QualityScope::ViewRows { start: 2, end: 3 }
    );
    let next = key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.sample.scope,
        QualityScope::ViewRows { start: 2, end: 3 }
    );
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    app.event(next.unwrap());
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal
            .quality
            .results
            .as_ref()
            .unwrap()
            .total_rows,
        Some(2)
    );
    // An edit waits for Enter; Esc puts back the plan the result was measured with.
    key(&mut app, KeyCode::Char('e'));
    for _ in 0..5 {
        key(&mut app, KeyCode::Down);
    }
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis::analysis_modal::SetupRow::Grain
    );
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Enter);
    assert_ne!(
        app.analysis_modal.quality.plan.grain,
        datui::analysis::data_quality::QualityGrain::Dataset
    );
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Setup);
    assert!(app.analysis_modal.quality_plan_pending());
    key(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.quality.plan.grain,
        datui::analysis::data_quality::QualityGrain::Dataset
    );
    assert!(app.analysis_modal.quality.results.is_some());
    key(&mut app, KeyCode::Char('s'));
    set_scope(&mut app, "rows 1..1");
    assert!(key(&mut app, KeyCode::Enter).is_none());
    // The last report stays until a run replaces it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.quality.results.is_some());
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    app.event(next.unwrap());
    drain_events(&mut app, &rx);

    // The earlier sample again is the session cache's, not another read.
    key(&mut app, KeyCode::Char('s'));
    set_scope(&mut app, "rows 2..3");
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(app.analysis_modal.quality.from_cache);
    assert_eq!(app.analysis_modal.quality.page, QualityPage::Overview);
}

#[test]
fn test_data_quality_source_file_scope_uses_loaded_file_order() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::analysis::data_quality::QualityScope;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "region=one", df!("id" => &[1i32, 2]).unwrap());
    write_parquet(dir.path(), "region=two", df!("id" => &[3i32, 4]).unwrap());
    let (mut app, rx, _) = open_local_dataset_with_channel(dir.path());
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A run of the default plan settles first.
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    // e leaves the result for Setup; the scoped run starts there.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.quality.plan.scope = QualityScope::SourceFiles(vec![2]);
    let next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    app.event(next.unwrap());
    drain_events(&mut app, &rx);
    let results = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(results.total_rows, Some(2));
    assert_eq!(results.evaluated_rows, 2);
    let id = results
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(id.min.as_deref(), Some("3"));
    assert_eq!(id.max.as_deref(), Some("4"));

    // The run reads the plan it is given.
    let plan = &mut app.analysis_modal.quality.plan;
    plan.scope = QualityScope::WholeSource;
    plan.dataset_rows = 1;
    app.event(AppEvent::AnalysisCompute(AnalysisTool::DataQuality));
    drain_events(&mut app, &rx);
    let sampled = app.analysis_modal.quality.results.as_ref().unwrap();
    // The sampler counts the whole scope as it spreads the sample over it.
    assert_eq!(sampled.total_rows, Some(4));
    assert_eq!(sampled.evaluated_rows, 1);
    assert_eq!(
        sampled.precision,
        datui::analysis::data_quality::QualityPrecision::Sampled
    );

    let plan = &mut app.analysis_modal.quality.plan;
    plan.method = datui::analysis::sampling::SampleMethod::EveryRow;
    plan.compute = datui::analysis::data_quality::QualityCompute::Full;
    app.event(AppEvent::AnalysisCompute(AnalysisTool::DataQuality));
    drain_events(&mut app, &rx);
    let full = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(full.total_rows, Some(4));
    assert_eq!(full.evaluated_rows, 4);

    // By file, the segments are the shared sample's rows split by the file each
    // came from; a file's size is its footer's, not a guess from the sample.
    let plan = &mut app.analysis_modal.quality.plan;
    plan.method = datui::analysis::sampling::SampleMethod::Spread;
    plan.compute = datui::analysis::data_quality::QualityCompute::Sample;
    plan.dataset_rows = 3;
    plan.grain = datui::analysis::data_quality::QualityGrain::File;
    app.event(AppEvent::AnalysisCompute(AnalysisTool::DataQuality));
    drain_events(&mut app, &rx);
    let by_file = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(by_file.total_rows, Some(4));
    assert_eq!(by_file.evaluated_rows, 3);
    assert_eq!(by_file.segments.len(), 2);
    assert!(
        by_file
            .segments
            .iter()
            .all(|segment| segment.total_rows == Some(2))
    );
    assert_eq!(
        by_file
            .segments
            .iter()
            .map(|segment| segment.evaluated_rows)
            .sum::<usize>(),
        3
    );
}

/// An opened hive directory types its partition columns the way a full scan does.
#[test]
fn test_hive_partition_types_match_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    for sub in [
        "region=eu/year=2020/day=2020-01-01",
        "region=us/year=2021/day=2021-06-30",
    ] {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let mut df = df!("v" => [1i64, 2]).unwrap();
        ParquetWriter::new(File::create(d.join("data.parquet")).unwrap())
            .finish(&mut df)
            .unwrap();
    }

    let app = open_local_dataset(dir.path());
    let mut lf = app.data_table_state.as_ref().unwrap().lf_clone();
    let fast = lf.collect_schema().unwrap();
    let parts = ["region", "year", "day"];
    for name in parts {
        assert!(fast.contains(name), "{name}");
    }
    let mut full = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(dir.path()).unwrap(),
        ScanArgsParquet {
            hive_options: polars::io::HiveOptions::new_enabled(),
            ..Default::default()
        },
    )
    .unwrap();
    let full = full.collect_schema().unwrap();
    for name in parts {
        assert_eq!(fast.get(name), full.get(name), "{name}");
    }
    assert_eq!(fast.get("year"), Some(&DataType::Int64));

    let df = lf.filter(col("year").gt(lit(2020))).collect().unwrap();
    assert_eq!(df.height(), 2);
}

/// The copy dialog keeps one size whatever its scope offers: stepping the scope
/// moves no edge of the frame.
#[test]
fn test_copy_dialog_keeps_its_size_across_scopes() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("copy_fixed_height.csv");
    press(&mut app, KeyCode::Char('y'));
    assert_eq!(app.overlay, Overlay::Copy);
    let mut frames = std::collections::HashSet::new();
    for scope in datui::app::modals::copy_modal::CopyScope::ALL {
        copy_scope(&mut app, scope);
        let rows = rows_at(&mut app, 80, 24);
        frames.insert(common::frame_bottoms(&rows));
    }
    assert_eq!(frames.len(), 1, "one frame for every scope: {frames:?}");
}

/// The copy dialog end to end: `y` opens it, the scopes copy what they say
/// through whatever destination the app holds, choices are sticky, and a
/// null copies as empty, never as the UI's glyph.
#[test]
fn test_copy_dialog_sends_each_scope_to_the_destination() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy_test.csv");
    std::fs::write(&path, "city,pop\nOslo,700000\nParis,2100000\nQuito,\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    // A frame must have been drawn for the view scope to know its rows.
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);

    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));

    let key =
        |app: &mut App, code| app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));

    // Row scope is the default, header off: the current row as bare TSV.
    key(&mut app, KeyCode::Char('y'));
    assert_eq!(app.overlay, Overlay::Copy);
    key(&mut app, KeyCode::Enter);
    assert!(app.at_table());
    assert_eq!(copies.lock().unwrap()[0].text, "Oslo\t700000");

    // The view scope: header on by default, every buffered screen row, and
    // the HTML flavor beside the TSV.
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Char(' ')); // the next scope: Row -> View
    assert_eq!(
        app.copy_modal.scope,
        datui::app::modals::copy_modal::CopyScope::View
    );
    key(&mut app, KeyCode::Enter); // copy
    {
        let copies = copies.lock().unwrap();
        assert_eq!(
            copies[1].text,
            "city\tpop\nOslo\t700000\nParis\t2100000\nQuito\t"
        );
        let html = copies[1].html.as_deref().expect("tsv carries html");
        assert!(html.contains("<th>city</th>"), "{html}");
    }

    // The cell scope, two steps back; its column Picker narrows by typing: 'p'
    // leaves only pop.
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Left);
    key(&mut app, KeyCode::Left);
    assert_eq!(
        app.copy_modal.scope,
        datui::app::modals::copy_modal::CopyScope::Cell
    );
    key(&mut app, KeyCode::Tab); // Scope -> Column
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Char('p'));
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter); // copy
    assert_eq!(copies.lock().unwrap()[2].text, "700000");

    // The scope is sticky; the column is the column cursor's, which `l` moves to
    // pop, and a null cell copies as empty.
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Enter);
    assert_eq!(copies.lock().unwrap()[3].text, "Quito");
    key(&mut app, KeyCode::Char('l'));
    key(&mut app, KeyCode::Char('y'));
    assert_eq!(app.copy_modal.column.as_deref(), Some("pop"));
    key(&mut app, KeyCode::Enter);
    assert_eq!(copies.lock().unwrap()[4].text, "");

    // The table scope collects off-thread, then lands on the same destination
    // with the header the scope defaults to.
    key(&mut app, KeyCode::Char('y'));
    copy_scope(&mut app, datui::app::modals::copy_modal::CopyScope::Table);
    let mut next = key(&mut app, KeyCode::Enter);
    while let Some(ev) = next {
        next = app.event(ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    {
        let copies = copies.lock().unwrap();
        assert_eq!(copies.len(), 6, "the background copy landed");
        assert_eq!(
            copies[5].text,
            "city\tpop\nOslo\t700000\nParis\t2100000\nQuito\t"
        );
    }

    // The completion is a flash on the footer, not a modal.
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(screen.contains("Copied 3 rows as TSV"), "no flash drawn");
}

//! Analysis: describe, distribution, correlation and value counts, cancelling and samples.

use super::*;

/// Esc cancels an analysis while it runs, for every tool: it acts at once rather than
/// queueing behind the run, the bar says so first, and the answer that arrives later
/// is dropped rather than installed.
#[test]
fn test_esc_cancels_a_distribution_analysis_in_flight() {
    use datui::analysis::analysis_modal::AnalysisTool;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(key(KeyCode::Char('a')));
    app.analysis_modal.sidebar_state.select(Some(1));
    show_sample_form(&mut app);
    let next = app.event(key(KeyCode::Enter));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(
            AnalysisTool::DistributionAnalysis
        ))
    ));
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DistributionAnalysis)
    );
    // The run starts on a worker.
    app.event(next.unwrap());
    assert!(app.analysis_modal.computing.is_some());

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(screen.contains("Cancel"), "{screen:?}");
    assert!(!screen.contains("0 / 1"), "no gauge that cannot move");

    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.hard_escape_while_busy(&esc), "Esc jumps the queue");
    app.event(AppEvent::Key(esc));
    assert!(app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.selected_tool, None);
    assert_eq!(
        app.overlay,
        Overlay::Analysis,
        "still on the analysis screen"
    );
    assert_eq!(app.flash_message(), Some("Analysis cancelled"));

    // The worker finishes anyway; what it sends is stale.
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.selected_tool, None);
}

/// The Sample form's size is typed, in shorthand, and applied on Enter; a size it
/// cannot read keeps the form open with why.
#[test]
fn the_sample_size_is_typed_in_shorthand() {
    use datui::analysis::sample_modal::SampleField;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sizes.csv");
    let mut csv = "x\n".to_string();
    for x in 1..=20i64 {
        csv.push_str(&format!("{x}\n"));
    }
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    app.event(key(KeyCode::Char('a')));
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let form = app.analysis_modal.sample_form.as_mut().unwrap();
    assert!(datui::app::form::Form::focus(form, SampleField::Size));
    for c in "zz".chars() {
        app.event(key(KeyCode::Char(c)));
    }
    app.event(key(KeyCode::Enter));
    let form = app.analysis_modal.sample_form.as_ref().expect("stays open");
    assert!(form.error.as_deref().unwrap_or("").contains("50k"));
    for _ in 0..2 {
        app.event(key(KeyCode::Backspace));
    }
    for c in "5k".chars() {
        app.event(key(KeyCode::Char(c)));
    }
    app.event(key(KeyCode::Enter));
    assert!(app.analysis_modal.sample_form.is_none());
    assert_eq!(app.analysis_modal.sample.rows, 5_000);
}

/// `m` on the correlation matrix switches between Pearson and Spearman, named in the
/// title, with nothing read again: y = x³ is a perfect rank relation but not a line.
#[test]
fn m_switches_the_correlation_matrix_between_pearson_and_spearman() {
    use datui::analysis::analysis_modal::AnalysisTool;
    use datui::analysis::statistics::CorrelationMethod;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cubes.csv");
    let mut csv = "x,y\n".to_string();
    for x in 1..=20i64 {
        csv.push_str(&format!("{x},{}\n", x * x * x));
    }
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    app.event(key(KeyCode::Char('a')));
    app.analysis_modal.sidebar_state.select(Some(2));
    show_sample_form(&mut app);
    let next = app.event(key(KeyCode::Enter));
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::CorrelationMatrix))
    ));
    app.event(next.unwrap());
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::CorrelationMatrix)
    );

    let screen = rows_at(&mut app, 120, 24).join("\n");
    assert!(screen.contains("Correlation Matrix"), "{screen}");
    assert!(screen.contains("Pearson r"), "{screen}");
    assert_eq!(screen.matches("1.000").count(), 2, "the diagonal: {screen}");
    assert!(screen.contains("0.9"), "three places: {screen}");

    assert!(
        app.event(key(KeyCode::Char('m'))).is_none(),
        "nothing to read"
    );
    assert_eq!(
        app.analysis_modal.correlation_method,
        CorrelationMethod::Spearman
    );
    let rho = datui::glyphs::get().rho;
    let screen = rows_at(&mut app, 120, 24).join("\n");
    assert!(screen.contains(&format!("Spearman {rho}")), "{screen}");
    assert_eq!(screen.matches("1.000").count(), 4, "{screen}");

    app.event(key(KeyCode::Char('m')));
    assert_eq!(
        app.analysis_modal.correlation_method,
        CorrelationMethod::Pearson
    );
}

/// Enter on a tool always takes the cursor into its pane, whether it shows the
/// Sample form, starts a run or shows a result; Tab and Shift+Tab cross between
/// the pane and the tools, Esc steps back one level, and the results outlive a
/// close on the same view.
#[test]
fn enter_on_a_tool_enters_its_pane_and_esc_steps_back() {
    use datui::analysis::analysis_modal::{AnalysisFocus, AnalysisTool};

    let (mut app, rx, _tx) = open_query_filter_fixture("analysis_focus.csv");
    let press = |app: &mut App, code: KeyCode| {
        let mut next = app.event(key(code));
        while let Some(ev) = next {
            next = app.event(ev);
        }
    };

    press(&mut app, KeyCode::Char('a'));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);

    // Enter on Describe shows its Sample form in the pane, and the cursor goes
    // into it: the form is what the pane is for until the first run.
    show_sample_form(&mut app);
    assert!(app.analysis_modal.sample_form.is_some());
    assert!(app.analysis_modal.describe_results.is_none());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::analysis::sample_modal::SampleField::Rows,
        "the cursor lands on the first setting"
    );
    // Esc hands the cursor back to the list and leaves the form waiting; Enter
    // there runs it as it stands, and the cursor goes with it into the pane.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
    assert!(app.analysis_modal.sample_form.is_some());
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.describe_results.is_some());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // Tab and Shift+Tab both cross to the other pane.
    for code in [KeyCode::Tab, KeyCode::BackTab] {
        press(&mut app, code);
        assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar, "{code:?}");
        press(&mut app, code);
        assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main, "{code:?}");
    }

    // A tool picked once the sample has run starts at once, the cursor in its
    // pane; a tool with a result shows it, the cursor in its pane too.
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DistributionAnalysis)
    );
    assert!(app.analysis_modal.distribution_results.is_some());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Sidebar,
        "Esc: the tools"
    );
    assert_eq!(app.overlay, Overlay::Analysis, "Esc steps back one level");
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::Describe)
    );
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // Esc from the tools closes; the results come back with the tool on screen.
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::Analysis);
    press(&mut app, KeyCode::Char('a'));
    assert_eq!(app.overlay, Overlay::Analysis);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::Describe)
    );
    assert!(app.analysis_modal.describe_results.is_some());
    assert!(app.analysis_modal.distribution_results.is_some());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
    assert_eq!(app.analysis_modal.sidebar_state.selected(), Some(0));

    // Another view, other rows: the results are of the old one, and go.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('r'));
    drain_events(&mut app, &rx);
    press(&mut app, KeyCode::Char('a'));
    assert_eq!(app.analysis_modal.selected_tool, None);
    assert!(app.analysis_modal.describe_results.is_none());
    assert!(app.analysis_modal.distribution_results.is_none());
}

/// The correlation matrix opens on the first pair rather than a column against
/// itself, and Home and End go to the first and last pairs of its own size.
#[test]
fn the_correlation_matrix_starts_on_a_pair() {
    let (mut app, rx, _tx) = open_query_filter_fixture("analysis_pairs.csv");
    let press = |app: &mut App, code: KeyCode| {
        let mut next = app.event(key(code));
        while let Some(ev) = next {
            next = app.event(ev);
        }
    };
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(2));
    show_sample_form(&mut app);
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.correlation_size(), 2, "a and c");
    assert_eq!(app.analysis_modal.selected_correlation, Some((0, 1)));
    press(&mut app, KeyCode::End);
    assert_eq!(app.analysis_modal.selected_correlation, Some((1, 0)));
    press(&mut app, KeyCode::Home);
    assert_eq!(app.analysis_modal.selected_correlation, Some((0, 1)));
    let screen = rows_at(&mut app, 120, 24).join("\n");
    assert!(screen.contains("Enter Detail"), "{screen}");
}

/// One sample for every tool: chosen once in Describe, it is the rows Data Quality
/// reads too, and the header says which rows those are. Esc in the form discards.
#[test]
fn one_sample_serves_every_analysis_tool() {
    use datui::analysis::analysis_modal::AnalysisTool;
    use datui::analysis::data_quality::QualityScope;

    let dir = common::fixture_dir();
    let path = dir.join("shared_sample.parquet");
    let sizes = [("a", 900usize), ("b", 90), ("c", 10)];
    let part: Vec<&str> = sizes
        .iter()
        .flat_map(|(name, size)| std::iter::repeat_n(*name, *size))
        .collect();
    let mut df = df!(
        "part" => &part,
        "value" => (0..part.len() as i64).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let key =
        |app: &mut App, code| app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    let run = |app: &mut App, next: Option<AppEvent>| {
        let mut next = next;
        while let Some(ev) = next {
            next = app.event(ev);
        }
        drain_events(app, &rx);
    };

    key(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let next = key(&mut app, KeyCode::Enter);
    run(&mut app, next);

    // The bar names the key at the baseline width, or the sample is a feature
    // nobody finds: in the result, after the way out and Tab.
    let narrow = Rect::new(0, 0, 80, 24);
    let mut buffer = Buffer::empty(narrow);
    app.render(narrow, &mut buffer);
    let bar: String = (0..narrow.width)
        .map(|x| buffer[(x, narrow.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Sample"),
        "s Sample on the bar at 80 columns: {bar}"
    );

    // Esc discards the form's edits.
    key(&mut app, KeyCode::Char('s'));
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Right);
    key(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.sample_form.is_none());
    assert_eq!(
        app.analysis_modal.sample.method,
        datui::analysis::sampling::SampleMethod::Spread
    );

    key(&mut app, KeyCode::Char('s'));
    app.analysis_modal
        .sample_form
        .as_mut()
        .unwrap()
        .set_scope(&QualityScope::parse_command("partition part=b,c").unwrap());
    let next = key(&mut app, KeyCode::Enter);
    run(&mut app, next);
    let describe = app.analysis_modal.describe_results.as_ref().unwrap();
    assert_eq!(describe.total_rows, 100, "only partitions b and c");
    let area = Rect::new(0, 0, 100, 24);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("all 100 rows") && screen.contains("part=b,c"),
        "the header names the rows read"
    );

    // v shows the sample itself in the table viewer; Esc brings back the table
    // and the tool as they were.
    let next = key(&mut app, KeyCode::Char('v'));
    run(&mut app, next);
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(
        app.overlay,
        Overlay::Analysis,
        "the sample replaces the tool"
    );
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 100);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(
        screen.contains("Sample") && screen.contains("Esc") && screen.contains("Back"),
        "the view says what it is and the way out"
    );
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Analysis);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1_000);
    assert!(app.analysis_modal.describe_results.is_some());

    // Data Quality starts from the same rows without being told again, but opens
    // its Setup rather than reading: only its Run reads.
    app.analysis_modal.focus = datui::analysis::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    let next = key(&mut app, KeyCode::Enter);
    assert!(
        app.analysis_modal.sample_form.is_none(),
        "the sample already chosen is not asked for again"
    );
    assert!(next.is_none(), "choosing Data Quality reads nothing");
    assert!(!app.is_busy());
    assert_eq!(
        app.analysis_modal.quality.page,
        datui::analysis::data_quality::QualityPage::Setup
    );
    let next = key(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    run(&mut app, next);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(
        app.analysis_modal.quality.plan.scope,
        QualityScope::SourcePartition {
            column: "part".to_string(),
            value: "b,c".to_string()
        }
    );
    let quality = app.analysis_modal.quality.results.as_ref().unwrap();
    assert_eq!(quality.total_rows, Some(100));
    let mut buffer = Buffer::empty(narrow);
    app.render(narrow, &mut buffer);
    let bar: String = (0..narrow.width)
        .map(|x| buffer[(x, narrow.height - 1)].symbol().to_string())
        .collect();
    // At 80 columns the report's bar keeps Setup, whose first row is the sample.
    assert!(
        bar.contains("e Setup"),
        "e Setup on the Data Quality footer at 80 columns: {bar}"
    );
    // The seed is typed: arriving on it selects it, so one key makes it that key.
    key(&mut app, KeyCode::Char('s'));
    for _ in 0..8 {
        if app.analysis_modal.sample_form.as_ref().unwrap().field
            == datui::analysis::sample_modal::SampleField::Seed
        {
            break;
        }
        key(&mut app, KeyCode::Down);
    }
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::analysis::sample_modal::SampleField::Seed
    );
    key(&mut app, KeyCode::Char('7'));
    // The form's Enter applies to Setup's draft; the sample every tool reads
    // changes only when Run commits it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(next.is_none(), "applying the sample reads nothing");
    assert_eq!(app.analysis_modal.quality.plan.sample_seed, 7);
    assert_ne!(app.analysis_modal.sample.seed, 7);
    let next = key(&mut app, KeyCode::Enter);
    assert!(matches!(
        next,
        Some(AppEvent::AnalysisCompute(AnalysisTool::DataQuality))
    ));
    assert_eq!(app.analysis_modal.sample.seed, 7);
    run(&mut app, next);

    // The tool list is the same beside every tool: the active one carries the
    // accent, not a dot only Data Quality drew.
    let screen = common::buffer_text(&buffer);
    assert!(!screen.contains(&format!("{} Data Quality", datui::glyphs::get().middot)));
}

/// Analysis counts the rows itself, which is the same `len()` over a union of scans
/// that crashed the table. Its panic is worse: it happens inside `spawn_blocking`,
/// where tokio swallows it, so the panel never finishes and the app wedges on
/// "Running analysis…" with the panic text over the raw-mode screen.
#[test]
fn test_analysing_a_union_of_scans_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "n" => &["a", "b"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "n" => &[10i64, 20]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let every_row = datui::analysis::sampling::Sample {
        method: datui::analysis::sampling::SampleMethod::EveryRow,
        ..Default::default()
    };
    let results = datui::analysis::statistics::compute_statistics_for_sample(
        &state.lf_clone(),
        &every_row,
        None,
        datui::analysis::statistics::ComputeOptions {
            polars_streaming: true,
            ..Default::default()
        },
    )
    .expect("analysis runs over a many-file scan");
    assert_eq!(results.total_rows, 4, "and counts every row");
}

/// A key typed while an analysis runs is held, and Esc cancelling the run drops it: an
/// impatient second Enter replayed after the cancel started the run again behind it.
#[test]
fn test_keys_held_during_an_analysis_do_not_outlive_its_cancel() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    let mut pump = EventPump::new(app, tx, rx);
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    for _ in ticks() {
        if !pump.app.is_busy() && pump.app.data_table_state.is_some() {
            break;
        }
        pump.wait_and_drain(std::time::Duration::from_millis(100))
            .unwrap();
    }

    let press = |pump: &mut EventPump, code| {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    };
    press(&mut pump, KeyCode::Char('a'));
    pump.app.analysis_modal.sidebar_state.select(Some(1));
    // The first Enter shows the tool's Sample form; the second runs it.
    press(&mut pump, KeyCode::Enter);
    press(&mut pump, KeyCode::Enter);
    pump.drain().unwrap();
    assert!(
        pump.app.analysis_modal.computing.is_some(),
        "the run started"
    );
    press(&mut pump, KeyCode::Enter);
    assert_eq!(pump.held_keys().count(), 1, "typed while busy: held");

    press(&mut pump, KeyCode::Esc);
    while pump.replay_one().unwrap() {}
    assert_eq!(pump.held_keys().count(), 0);
    assert!(
        pump.app.analysis_modal.computing.is_none(),
        "the held Enter did not start it again"
    );
    assert_eq!(pump.app.analysis_modal.selected_tool, None);
}

/// A view saved while drilled into a group describes the grouped view, which is what
/// it will reproduce: the getters return the grouped view's filters and sort, while the
/// view getters describe the frame on screen.
#[test]
fn test_view_getters_describe_the_grouped_view_while_drilled() {
    use datui::app::modals::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_view_getters.csv");

    app.event(AppEvent::Applied(datui::Applied::QQuery(
        "select by c".to_string(),
    )));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Applied(datui::Applied::Filter(vec![
        filter_stmt("c", FilterOperator::Gt, "0"),
    ])));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Applied(datui::Applied::Sort(
        vec!["c".to_string()],
        vec![true],
    )));
    pump_until_idle(&mut app, &rx, &tx);

    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(0).unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(state.get_filters().len(), 1);
    assert_eq!(state.get_sort_columns(), ["c".to_string()]);
    assert!(!state.get_sort_ascending());
    assert!(state.view_filters().is_empty());
    assert!(state.view_sort_columns().is_empty());

    state.drill_up().unwrap();
    assert_eq!(state.get_filters().len(), 1);
    assert_eq!(state.view_filters().len(), 1);
    assert_eq!(state.get_sort_columns(), ["c".to_string()]);
}

/// Describe scrolls its statistics as far as the last one and no further. → past
/// the end does nothing, so the first ← always moves back, however many times →
/// was pressed. The bound is what the table drew, not a count kept beside it.
#[test]
fn test_describe_scrolls_to_its_last_statistic_and_back_in_one_press() {
    use datui::analysis::analysis_modal::AnalysisFocus;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );
    app.event(key(KeyCode::Char('a')));
    show_sample_form(&mut app);
    let mut next = app.event(key(KeyCode::Enter));
    while let Some(ev) = next {
        next = app.event(ev);
    }
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.describe_results.is_some());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // 80 columns fit a handful of the nine statistics beside the tool list.
    let area = Rect::new(0, 0, 80, 24);
    let header = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.width)
            .map(|x| buf[(x, 1)].symbol().to_string())
            .collect::<String>()
    };
    let first = header(&mut app);
    assert!(first.contains("Count"), "{first:?}");
    assert!(!first.contains("Max"), "not everything fits: {first:?}");
    // `+N`. In ASCII the tool list's frame corners are `+` too, so there the count
    // is told from them by its digit; in Unicode any `+` is one.
    let counted = |row: &str| {
        if datui::glyphs::active_is_unicode() {
            return row.contains('+');
        }
        row.as_bytes()
            .windows(2)
            .any(|w| w[0] == b'+' && w[1].is_ascii_digit())
    };
    assert!(
        counted(&first),
        "the hidden statistics are counted: {first:?}"
    );
    let max = app.analysis_modal.describe_columns.max;
    assert!(max > 0);

    for _ in 0..20 {
        app.event(key(KeyCode::Right));
        header(&mut app);
    }
    assert_eq!(app.analysis_modal.describe_columns.offset, max);
    let end = header(&mut app);
    assert!(
        end.contains("Max"),
        "the last statistic is reached: {end:?}"
    );
    assert!(!counted(&end), "and nothing is counted past it: {end:?}");

    app.event(key(KeyCode::Left));
    let back = header(&mut app);
    assert_eq!(app.analysis_modal.describe_columns.offset, max - 1);
    assert_ne!(back, end, "one press back moves the table");
    assert!(
        back.contains("+1"),
        "the last statistic is out of view: {back:?}"
    );
}

/// F counts the column cursor's column: by count, nulls on their own line, the summary
/// over them; ← → step columns, s sorts by value, Esc goes back.
/// `F` on a number opens its histogram, binned from the counts; `c` turns to the
/// listing and back, and the bar names the other view.
#[test]
fn test_value_counts_histogram_toggle() {
    let (mut app, rx, tx) = open_csv_with(
        "value_counts_histogram.csv",
        COUNTS_CSV,
        OpenOptions::default(),
    );
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert!(
        !app.value_counts.shows_histogram(),
        "text has only the listing"
    );
    counts_key(&mut app, &rx, &tx, KeyCode::Right);
    assert_eq!(app.value_counts.column(), Some("amount"));
    assert!(app.value_counts.shows_histogram());
    let full = datui::glyphs::get().bar_eighths[7];
    let screen = counts_screen(&mut app, 80, 24);
    assert!(
        screen.contains("Count") && screen.contains(full),
        "{screen}"
    );
    assert!(!screen.contains("Cum %"), "{screen}");
    assert!(screen.contains("c Counts"), "{screen}");
    press_and_send(&mut app, &tx, KeyCode::Char('c'));
    let screen = counts_screen(&mut app, 80, 24);
    assert!(screen.contains("Cum %"), "{screen}");
    assert!(screen.contains("c Histogram"), "{screen}");
    press_and_send(&mut app, &tx, KeyCode::Char('c'));
    assert!(app.value_counts.shows_histogram());
}

#[test]
fn test_value_counts_count_the_column_and_step_columns() {
    let (mut app, rx, tx) =
        open_csv_with("value_counts_keys.csv", COUNTS_CSV, OpenOptions::default());
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(app.overlay, Overlay::ValueCounts);
    assert_eq!(app.value_counts.column(), Some("pay"));
    let line = |s: &str, n| (s.to_string(), n);
    assert_eq!(
        counted_lines(&app),
        [
            line("card", 5),
            line("cash", 2),
            line("null", 2),
            line("check", 1)
        ]
    );
    let counts = app.value_counts.current().unwrap();
    assert!(!counts.is_sample());
    assert_eq!(
        (
            counts.summary.rows,
            counts.summary.distinct,
            counts.summary.nulls
        ),
        (10, 3, 2)
    );
    let screen = counts_screen(&mut app, 80, 24);
    assert!(screen.contains("Value Counts"), "{screen}");
    assert!(screen.contains("all 10 rows"), "{screen}");
    assert!(screen.contains("Distinct 3"), "{screen}");
    assert!(
        screen.contains("Enter Rows") && screen.contains("Esc Back"),
        "the bar names the keys: {screen}"
    );

    counts_key(&mut app, &rx, &tx, KeyCode::Right);
    assert_eq!(app.value_counts.column(), Some("amount"));
    // A number opens as its histogram; `c` turns to the listing.
    assert!(app.value_counts.shows_histogram());
    counts_key(&mut app, &rx, &tx, KeyCode::Char('c'));
    assert!(!app.value_counts.shows_histogram());
    let summary = &app.value_counts.current().unwrap().summary;
    assert_eq!(
        summary.sum,
        Some(datui::analysis::value_counts::Number::Int(60))
    );
    assert_eq!(summary.mean, Some(6.0));
    assert_eq!(summary.min, Some(AnyValue::Int64(1)));
    assert_eq!(summary.max, Some(AnyValue::Int64(10)));
    assert_eq!(counted_lines(&app)[0], line("10", 4));
    let screen = counts_screen(&mut app, 80, 24);
    assert!(
        screen.contains("Sum 60") && screen.contains("Mean 6"),
        "{screen}"
    );

    counts_key(&mut app, &rx, &tx, KeyCode::Char('s'));
    let by_value: Vec<String> = counted_lines(&app).into_iter().map(|(v, _)| v).collect();
    assert_eq!(by_value, ["1", "2", "3", "5", "7", "10"]);

    // Back to a column already counted reads nothing.
    press_and_send(&mut app, &tx, KeyCode::Left);
    assert!(app.value_counts.computing.is_none());
    assert_eq!(app.value_counts.column(), Some("pay"));

    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());
}

/// Enter drills into the rows holding the value, null included, the way a `by`
/// group does; Esc comes back to the counts, and Esc again to the table.
#[test]
fn test_value_counts_enter_drills_and_esc_comes_back() {
    let (mut app, rx, tx) =
        open_csv_with("value_counts_drill.csv", COUNTS_CSV, OpenOptions::default());
    let area = Rect::new(0, 0, 100, 30);
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert!(app.at_table());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key(),
        Some((&["pay".to_string()][..], &["cash".to_string()][..]))
    );
    assert_eq!(on_screen(&app, "id"), ["2", "6"]);

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.overlay, Overlay::ValueCounts, "back to the counts");
    assert!(
        app.value_counts.computing.is_none(),
        "nothing counted again"
    );
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());

    // The nulls' line, ranked by its rows after cash's as many.
    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "id"), ["4", "7"]);
    assert_eq!(on_screen(&app, "pay"), ["null", "null"]);

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "id").len(), 10);
}

/// A tab in the value drilled into is marked in the breadcrumb, not printed raw.
#[test]
fn test_value_counts_drill_breadcrumb_marks_a_tab() {
    let (mut app, rx, tx) = open_csv_with(
        "value_counts_tab.csv",
        "k,n\n\"a\tb\",1\n\"a\tb\",2\nc,3\n",
        OpenOptions::default(),
    );
    let area = Rect::new(0, 0, 80, 12);
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(counted_lines(&app)[0], ("a\tb".to_string(), 2));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "n"), ["1", "2"]);
    let screen = counts_screen(&mut app, 80, 12);
    let crumb = screen.lines().next().unwrap();
    let g = datui::glyphs::get();
    let marked = datui::exact::cell_preview("a\tb", g);
    assert!(crumb.contains(&format!("k={marked}")), "{crumb:?}");
}

/// The counts are of the view: a query's rows, not the file's.
#[test]
fn test_value_counts_count_the_queried_view() {
    let (mut app, rx, tx) = open_query_filter_fixture("value_counts_query.csv");
    app.event(AppEvent::Applied(datui::Applied::QQuery(
        "select where a < 10".to_string(),
    )));
    pump_until_idle(&mut app, &rx, &tx);
    press_and_send(&mut app, &tx, KeyCode::Right);
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(app.value_counts.column(), Some("c"));
    let line = |s: &str, n| (s.to_string(), n);
    assert_eq!(
        counted_lines(&app),
        [line("0", 4), line("1", 3), line("2", 3)]
    );
}

/// Past the top values the rest are one `other` line; Enter there drills into
/// nothing.
#[test]
fn test_value_counts_top_values_then_other() {
    let top = datui::analysis::value_counts::TOP_N;
    let mut csv = String::from("id\n");
    for i in 0..top + 5 {
        csv.push_str(&format!("{i}\n"));
    }
    csv.push_str("0\n0\n1\n");
    let (mut app, rx, tx) = open_csv_with("value_counts_other.csv", &csv, OpenOptions::default());
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    counts_key(&mut app, &rx, &tx, KeyCode::Char('c'));
    let lines = counted_lines(&app);
    assert_eq!(lines.len(), top + 1);
    assert_eq!(lines[0], ("0".to_string(), 3));
    assert_eq!(lines[top], ("other 5".to_string(), 5));
    assert_eq!(
        app.value_counts.current().unwrap().summary.distinct,
        top + 5
    );
    press_and_send(&mut app, &tx, KeyCode::End);
    let screen = counts_screen(&mut app, 80, 24);
    assert!(screen.contains("other (5 values)"), "{screen}");
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(app.overlay, Overlay::ValueCounts);
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// A Parquet file over 100 samples' worth of rows is counted from a sample first,
/// and the header says so; `a` counts every row, and the header says that.
#[test]
fn test_value_counts_sample_first_then_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("counts.parquet");
    let mut df = df!("k" => (0..30_000i64).map(|i| i % 4).collect::<Vec<_>>()).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .with_row_group_size(Some(1_000))
        .finish(&mut df)
        .unwrap();
    let mut config = datui::AppConfig::default();
    config.analysis.sample_rows = 200;
    let (tx, rx) = mpsc::channel();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    let counts = app.value_counts.current().unwrap();
    assert_eq!(counts.sampled_of, Some(30_000));
    assert_eq!(counts.summary.rows, 200);
    let screen = counts_screen(&mut app, 80, 24);
    assert!(screen.contains("sample of 200 of 30,000 rows"), "{screen}");
    assert!(screen.contains("a All rows"), "{screen}");

    counts_key(&mut app, &rx, &tx, KeyCode::Char('a'));
    let counts = app.value_counts.current().unwrap();
    assert!(!counts.is_sample());
    assert_eq!(counts.summary.rows, 30_000);
    let screen = counts_screen(&mut app, 80, 24);
    assert!(screen.contains("all 30,000 rows"), "{screen}");
    assert!(!screen.contains("All rows"), "{screen}");
}

/// `y` copies every value with its count and percentages; `e` exports them.
#[test]
fn test_value_counts_copy_and_export() {
    let (mut app, rx, tx) =
        open_csv_with("value_counts_copy.csv", COUNTS_CSV, OpenOptions::default());
    let copies: Copies = Default::default();
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    pump_until_idle(&mut app, &rx, &tx);
    let text = copies.lock().unwrap().last().cloned().unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "pay\tcount\tpercent\tcumulative_percent");
    assert!(lines[1].starts_with("card\t5\t50"), "{text}");
    assert_eq!(lines.len(), 5, "every value and the nulls: {text}");

    let out = tempfile::tempdir().unwrap();
    let csv = out.path().join("counts.csv");
    press_and_send(&mut app, &tx, KeyCode::Char('e'));
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    let screen = counts_screen(&mut app, 80, 24);
    assert!(
        screen.contains("Value Counts"),
        "the counts stay behind the dialog"
    );
    app.export_modal
        .path_input
        .set_value(csv.display().to_string());
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.overlay, Overlay::ValueCounts);
    let written = std::fs::read_to_string(&csv).unwrap();
    assert!(
        written.starts_with("pay,count,percent,cumulative_percent\ncard,5,"),
        "{written}"
    );
}

/// `F` counts the column cursor's column: `g` and `h` move the cursor, the header
/// carries its style, and stepping columns in Value Counts takes the cursor along.
#[test]
fn test_value_counts_count_the_column_cursors_column() {
    let (mut app, rx, tx) = open_csv_with(
        "value_counts_current.csv",
        COUNTS_CSV,
        OpenOptions::default(),
    );
    let area = Rect::new(0, 0, 80, 24);
    painted(&mut app, &rx, &tx, area);
    let current = |app: &App| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .current_column()
            .map(str::to_string)
    };
    assert_eq!(current(&app).as_deref(), Some("pay"), "the first column");
    press_and_send(&mut app, &tx, KeyCode::Char('g'));
    for c in "id".chars() {
        press_and_send(&mut app, &tx, KeyCode::Char(c));
    }
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(current(&app).as_deref(), Some("id"), "g moves the cursor");
    // The header says which: the cursor's style is on its name.
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert_eq!(header_in_cursor_style(&buffer, area.width), "id");
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(app.value_counts.column(), Some("id"));
    press_and_send(&mut app, &tx, KeyCode::Esc);

    press_and_send(&mut app, &tx, KeyCode::Char('h'));
    painted(&mut app, &rx, &tx, area);
    assert_eq!(current(&app).as_deref(), Some("amount"), "h moves it back");
    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(app.value_counts.column(), Some("amount"));
    // Stepping columns in Value Counts moves the cursor with it.
    counts_key(&mut app, &rx, &tx, KeyCode::Left);
    assert_eq!(app.value_counts.column(), Some("pay"));
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert_eq!(current(&app).as_deref(), Some("pay"));
}

//! The view's sample (`S`): drawn into memory as the table shows it, the step under
//! the query, read by Analysis, charts and export, and checked against memory.

use super::*;
use datui::sampling::SampleMethod;
use std::sync::Arc;

/// A Parquet file of `rows` rows: `id` in order, `group` one of four, `value`.
fn parquet(name: &str, rows: i64) -> PathBuf {
    let path = common::fixture_dir().join(name);
    let mut df = df!(
        "id" => (0..rows).collect::<Vec<_>>(),
        "group" => (0..rows).map(|i| ["a", "b", "c", "d"][(i % 4) as usize]).collect::<Vec<_>>(),
        "value" => (0..rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    path
}

fn open(path: PathBuf) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

fn key(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// `S`, a size typed and a seed, then Enter: the draw starts.
fn draw(app: &mut App, size: &str) {
    key(app, KeyCode::Char('S'));
    assert_eq!(app.input_mode, InputMode::Sample);
    let form = app.sample_form.as_mut().expect("the Sample form");
    form.size.set_value(size);
    form.seed.set_value("7");
    key(app, KeyCode::Enter);
}

/// The footer's line: the screen's last.
fn footer(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    (0..area.width)
        .map(|x| buffer[(x, area.height - 1)].symbol().to_string())
        .collect()
}

fn sampled_rows(app: &App) -> usize {
    app.data_table_state
        .as_ref()
        .and_then(|state| state.sampled())
        .map(|sampled| sampled.rows())
        .unwrap_or(0)
}

/// `S` opens the form over the table; Enter draws, the rows arrive while keys that
/// need every row wait and moving acts at once; the footer names the sample in
/// pipeline order once it is drawn.
#[test]
fn s_draws_a_sample_the_table_shows_as_it_arrives() {
    let (mut app, rx, tx) = open(parquet("table_sample_live.parquet", 10_000));
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    key(&mut app, KeyCode::Char('S'));
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(screen.contains("Sample size:"), "{screen}");
    assert!(screen.contains("Enter Draw"), "{screen}");
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Normal);

    draw(&mut app, "500");
    assert!(app.sample_drawing());
    // A sort needs every row: it waits. Moving reads the rows on hand: it acts.
    let sort = AppEvent::Key(KeyEvent::new(KeyCode::Char(']'), KeyModifiers::NONE));
    assert!(app.handle(&sort).is_err(), "a sort waits for the sample");
    let down = AppEvent::Key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
    assert!(app.handle(&down).is_ok(), "moving acts while it is drawn");

    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.sample_drawing());
    assert_eq!(sampled_rows(&app), 500);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 500);
    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "in source order");
    let line = footer(&mut app);
    assert!(line.contains("sample 500 of 10k"), "{line}");
}

/// The query runs over the sample: source, sample, query. Taking the sample away
/// puts the query back over the source.
#[test]
fn a_query_runs_over_the_sample_and_clearing_it_keeps_the_query() {
    let (mut app, rx, tx) = open(parquet("table_sample_query.parquet", 10_000));
    draw(&mut app, "400");
    pump_until_idle(&mut app, &rx, &tx);
    run_query(&mut app, &rx, &tx, "select where group = \"a\"");
    let state = app.data_table_state.as_ref().unwrap();
    let kept = state.lf().clone().collect().unwrap().height();
    assert!(kept > 0 && kept < 400, "{kept} of the sample's 400");
    assert!(state.sampled().is_some(), "the query keeps the sample");
    let line = footer(&mut app);
    let sample = line.find("sample 400").expect(&line);
    let query = line.rfind("query").expect(&line);
    assert!(sample < query, "pipeline order: {line}");

    // Another sample keeps the query laid on it.
    draw(&mut app, "300");
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.sampled().map(|s| s.rows()), Some(300));
    assert_eq!(state.get_active_query(), "select where group = \"a\"");

    // No sample: the form takes it away.
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_mut().unwrap();
    while form.draft.method != SampleMethod::EveryRow {
        form.field = datui::sample_modal::SampleField::Method;
        form.adjust(true);
    }
    key(&mut app, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.sampled().is_none());
    assert_eq!(state.get_active_query(), "select where group = \"a\"");
    assert_eq!(state.lf().clone().collect().unwrap().height(), 2_500);
}

/// Analysis and the chart read the view's sample, the same rows: Describe counts
/// the sample's rows, the chart has no Rows of its own and names the sample and
/// its seed.
#[test]
fn analysis_and_charts_read_the_views_sample() {
    use datui::analysis_modal::AnalysisTool;
    let (mut app, rx, tx) = open(parquet("table_sample_tools.parquet", 10_000));
    draw(&mut app, "300");
    pump_until_idle(&mut app, &rx, &tx);

    key(&mut app, KeyCode::Char('a'));
    assert_eq!(
        app.analysis_modal.sample.method,
        SampleMethod::EveryRow,
        "every tool reads the view's sample whole"
    );
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let next = key(&mut app, KeyCode::Enter);
    let mut next = next;
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::Describe)
    );
    let results = app
        .analysis_modal
        .describe_results
        .as_ref()
        .expect("Describe ran");
    assert_eq!(results.total_rows, 300);
    assert_eq!(results.sample_size, None, "the sample, read whole");
    key(&mut app, KeyCode::Esc);
    key(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.active);

    key(&mut app, KeyCode::Char('c'));
    assert_eq!(app.input_mode, InputMode::Chart);
    assert!(app.chart_modal.view_sampled);
    assert_eq!(app.chart_modal.row_limit, None);
    assert!(
        !app.chart_modal
            .row_order()
            .contains(&datui::chart_modal::ChartFocus::LimitRows),
        "no Rows row while the view has a sample"
    );
    pump_until(&mut app, &rx, &tx, App::chart_data_ready);
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = common::buffer_text(&buffer);
    assert!(screen.contains("sample 300 of 10k"), "{screen}");
    assert!(screen.contains("seed 7"), "{screen}");
}

/// Export writes the sample: the rows of the view, through the export dialog.
#[test]
fn export_writes_the_sample() {
    let (mut app, rx, tx) = open(parquet("table_sample_export.parquet", 10_000));
    draw(&mut app, "250");
    pump_until_idle(&mut app, &rx, &tx);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.csv");
    export_as(
        &mut app,
        &rx,
        &tx,
        &path,
        datui::export_modal::ExportFormat::Csv,
        false,
    );
    let written = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path))
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(written.height(), 250);
}

/// A sample estimated past the memory available warns on the form, naming the
/// setting and its -c form; Enter again draws it anyway.
#[test]
fn a_sample_past_the_memory_available_warns_and_enter_again_draws() {
    let (mut app, rx, tx) = open(parquet("table_sample_memory.parquet", 10_000));
    app.set_memory_probe(Arc::new(|| Some(1_000)));
    draw(&mut app, "5000");
    assert_eq!(app.input_mode, InputMode::Sample, "the form stays");
    let warning = app
        .sample_form
        .as_ref()
        .and_then(|form| form.error.clone())
        .expect("a warning");
    assert!(warning.contains("available now"), "{warning}");
    assert!(
        warning.contains("-c analysis.sample_memory_limit="),
        "{warning}"
    );
    assert!(!app.sample_drawing());
    let line = footer(&mut app);
    assert!(line.contains("Draw anyway"), "{line}");

    key(&mut app, KeyCode::Enter);
    assert!(app.sample_drawing(), "Enter again draws anyway");
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(sampled_rows(&app), 5_000);
}

/// Reset takes the sample away with everything else.
#[test]
fn reset_takes_the_sample_away() {
    let (mut app, rx, tx) = open(parquet("table_sample_reset.parquet", 2_000));
    draw(&mut app, "100");
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(sampled_rows(&app), 100);
    if let Some(reset) = key(&mut app, KeyCode::Char('R')) {
        app.event(&reset);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.sampled().is_none());
    assert_eq!(state.num_rows(), 2_000);
}

/// An exported chart carries its recipe, a view's JSON, when the dialog says
/// Include: the source, the query, the sample with its seed, and the chart. With
/// Omit the file carries no datui metadata at all.
#[test]
fn an_exported_chart_carries_its_recipe_unless_omitted() {
    use datui::chart_export::{ChartExportFormat, recipe_in};
    let path = parquet("table_sample_recipe.parquet", 10_000);
    let (mut app, rx, tx) = open(path.clone());
    draw(&mut app, "300");
    pump_until_idle(&mut app, &rx, &tx);
    run_query(&mut app, &rx, &tx, "select where group = \"b\"");
    key(&mut app, KeyCode::Char('c'));
    pump_until(&mut app, &rx, &tx, App::chart_data_ready);
    assert!(
        app.chart_export_modal.recipe,
        "chart.export_recipe starts the row at Include"
    );

    let dir = tempfile::tempdir().unwrap();
    for format in [ChartExportFormat::Png, ChartExportFormat::Svg] {
        let file = dir.path().join(format!("with.{}", format.extension()));
        let mut request = chart_export_request(&file, format);
        request.recipe = true;
        run_to_idle(&mut app, &rx, &tx, AppEvent::ChartExport(request));
        let recipe = recipe_in(&std::fs::read(&file).unwrap()).expect("a recipe");
        let json: serde_json::Value = serde_json::from_str(&recipe).unwrap();
        assert!(json["datui"].is_string(), "{json}");
        assert_eq!(
            json["source"].as_str(),
            Some(path.to_str().unwrap()),
            "{json}"
        );
        for left_out in ["id", "name", "created", "match_criteria"] {
            assert!(json.get(left_out).is_none(), "{left_out}: {json}");
        }
        let settings = &json["settings"];
        assert_eq!(settings["query"], "select where group = \"b\"", "{json}");
        assert_eq!(settings["sample"]["seed"], 7, "{json}");
        assert_eq!(settings["sample"]["rows"], 300, "{json}");
        assert!(settings["chart"]["mark"].is_string(), "{json}");
        // It reads back as a view.
        serde_json::from_str::<datui::view::SavedView>(&recipe).expect("a view");

        let bare = dir.path().join(format!("without.{}", format.extension()));
        run_to_idle(
            &mut app,
            &rx,
            &tx,
            AppEvent::ChartExport(chart_export_request(&bare, format)),
        );
        let bytes = std::fs::read(&bare).unwrap();
        assert_eq!(recipe_in(&bytes), None);
        assert!(
            !bytes.windows(5).any(|w| w.eq_ignore_ascii_case(b"datui")),
            "{format:?}: no datui metadata with Omit"
        );
    }
}

/// A random sample of a stream is drawn the same way again: the same seed keeps the
/// same rows whether the count had come in for the first draw or not.
#[test]
fn the_same_seed_draws_the_same_rows_before_and_after_the_count() {
    use datui::filter_modal::FilterOperator;
    use datui::table_sample::DrawPath;
    let (mut app, rx, tx) = open(parquet("table_sample_path.parquet", 20_000));
    // A filter streams the rows, and its count is not in yet when the sample is
    // drawn: a reservoir.
    let mut next = app.event(&AppEvent::Filter(vec![filter_stmt(
        "id",
        FilterOperator::Gt,
        "-1",
    )]));
    // Its page read, and no frame painted: the count waits for one.
    while next.is_some() || app.is_busy() {
        let event = match next.take() {
            Some(event) => event,
            None => rx
                .recv_timeout(common::HANG_GUARD)
                .expect("the page is read"),
        };
        next = app.event(&event);
    }
    assert!(!app.data_table_state.as_ref().unwrap().is_num_rows_valid());
    draw(&mut app, "500");
    pump_until_idle(&mut app, &rx, &tx);
    let sampled = app.data_table_state.as_ref().unwrap().sampled().unwrap();
    assert_eq!(sampled.path(), Some(DrawPath::Reservoir));
    let first = ids(&app);
    assert_eq!(first.len(), 500);

    // Back to the filtered view, whose count comes in now; drawn again, the same.
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_mut().unwrap();
    while form.draft.method != SampleMethod::EveryRow {
        form.field = datui::sample_modal::SampleField::Method;
        form.adjust(true);
    }
    key(&mut app, KeyCode::Enter);
    // The count is the footer's own worker, which `is_busy` does not cover.
    pump_until(&mut app, &rx, &tx, |app| {
        !app.is_busy() && app.data_table_state.as_ref().unwrap().is_num_rows_valid()
    });
    draw(&mut app, "500");
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(ids(&app), first);
}

fn ids(app: &App) -> Vec<i64> {
    app.data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect()
}

/// A pivot is never left off a sample: drawing one under a pivot, or taking a
/// sample away from under one, is refused with the way out.
#[test]
fn a_pivot_is_refused_never_dropped() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let pivot = || {
        AppEvent::Pivot(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "group".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::First,
        })
    };
    let (mut app, rx, tx) = open(parquet("table_sample_pivot.parquet", 400));
    draw(&mut app, "100");
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&pivot());
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .last_pivot_spec()
            .is_some()
    );

    // Taking the sample away would leave the pivot off the source.
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_mut().unwrap();
    while form.draft.method != SampleMethod::EveryRow {
        form.field = datui::sample_modal::SampleField::Method;
        form.adjust(true);
    }
    key(&mut app, KeyCode::Enter);
    let refused = app.error_message().expect("refused").to_string();
    assert!(refused.contains("pivot"), "{refused}");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.sampled().is_some() && state.last_pivot_spec().is_some());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    // Drawn again from the source, under the pivot.
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_mut().unwrap();
    form.draft.method = SampleMethod::Spread;
    form.kind = datui::sample_modal::RowsKind::Source;
    form.size.set_value("50");
    key(&mut app, KeyCode::Enter);
    assert!(!app.sample_drawing(), "nothing drawn under the pivot");
    let refused = app.error_message().expect("refused").to_string();
    assert!(refused.contains("pivot"), "{refused}");
}

/// A redraw that fails before a row comes leaves the sample it would have replaced,
/// with the steps laid on it.
#[test]
fn a_redraw_that_fails_keeps_the_sample_it_would_replace() {
    let (mut app, rx, tx) = open(parquet("table_sample_redraw.parquet", 5_000));
    draw(&mut app, "300");
    pump_until_idle(&mut app, &rx, &tx);
    run_query(&mut app, &rx, &tx, "select where group = \"c\"");
    let before = ids(&app);
    // A time range of a column that holds no times cannot be read.
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_mut().unwrap();
    form.kind = datui::sample_modal::RowsKind::Time;
    form.context.time_columns = vec!["id".to_string()];
    form.time_column = 0;
    form.time_from.set_value("2024-01-01");
    form.time_before.set_value("2024-02-01");
    key(&mut app, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_some(), "the draw failed");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.sampled().map(|s| s.rows()), Some(300));
    assert_eq!(state.get_active_query(), "select where group = \"c\"");
    assert_eq!(ids(&app), before);
}

/// The estimate is of what the sample reads: the source's every column for a source
/// scope, the view's for the view.
#[test]
fn the_estimate_is_of_the_columns_drawn() {
    let (mut app, rx, tx) = open(parquet("table_sample_estimate.parquet", 1_000));
    run_query(&mut app, &rx, &tx, "select id");
    key(&mut app, KeyCode::Char('S'));
    let form = app.sample_form.as_ref().unwrap();
    let (view, source) = (
        form.bytes_per_row.unwrap(),
        form.source_bytes_per_row.unwrap(),
    );
    assert!(view < source, "{view} of one column, {source} of three");
}

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

mod common;

/// Drains all pending events from the channel and processes them (for async operations).
fn drain_events(app: &mut App, rx: &std::sync::mpsc::Receiver<AppEvent>) {
    while let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(5000)) {
        if let Some(next) = app.event(&ev) {
            if let Some(next2) = app.event(&next) {
                app.event(&next2);
            }
        }
    }
}

/// Pumps the load event chain until complete, including background task results from the channel.
fn pump_open_until_loaded(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        if let Some(ev) = next.take() {
            if matches!(ev, AppEvent::Crash(_)) {
                app.event(&ev);
                return;
            }
            next = app.event(&ev);
        } else {
            // No chained event; check the channel for background task results.
            match rx.recv_timeout(std::time::Duration::from_millis(5000)) {
                Ok(ev) => {
                    next = Some(ev);
                }
                Err(_) => return, // Timeout or disconnected: loading complete or stuck.
            }
        }
    }
}

#[test]
fn test_app_creation() {
    let (tx, _) = mpsc::channel();
    let app = App::new(tx, common::test_runtime());
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn test_full_workflow() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    // 1. Create test CSV file inline
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("large_test.csv");

    let mut df = df!(
        "a" => (0..100).collect::<Vec<i32>>(),
        "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
        "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
        "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    // 2. Open the file (pump full load chain)
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.clone()],
        OpenOptions::default(),
    );

    assert!(app.data_table_state.is_some());
    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.num_rows, 100);

    // 2. Filter the data (s = Sort & Filter, switch to Filter tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event));
    assert!(app.sort_filter_modal.active);

    app.sort_filter_modal.switch_tab(); // Filter tab
    app.sort_filter_modal.filter.available_columns =
        app.data_table_state.as_ref().unwrap().headers();
    app.sort_filter_modal.filter.new_column_idx = 2;
    app.sort_filter_modal.filter.new_operator_idx = 0;
    app.sort_filter_modal.filter.new_value = "1".to_string();
    app.sort_filter_modal.filter.add_statement();
    app.sort_filter_modal.focus = datui::sort_filter_modal::SortFilterFocus::Apply;

    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    drain_events(&mut app, &rx);
    assert!(!app.sort_filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.lf.clone().collect().unwrap().shape().0, 33);

    // 3. Sort the data (s = Sort & Filter, Sort tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event));
    assert!(app.sort_filter_modal.active);

    app.sort_filter_modal.sort.columns = app
        .data_table_state
        .as_ref()
        .unwrap()
        .headers()
        .iter()
        .enumerate()
        .map(|(i, h)| datui::sort_modal::SortColumn {
            name: h.clone(),
            sort_order: None,
            display_order: i,
            is_locked: false,
            is_to_be_locked: false,
            is_visible: true,
        })
        .collect();
    app.sort_filter_modal.sort.table_state.select(Some(0));
    app.sort_filter_modal.sort.toggle_selection();
    app.sort_filter_modal.sort.ascending = false;
    app.sort_filter_modal.focus = datui::sort_filter_modal::SortFilterFocus::Apply;

    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    drain_events(&mut app, &rx);
    assert!(!app.sort_filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    let df = datatable.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

#[test]
fn test_chart_open_and_esc_back() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("chart_integration_test.csv");

    let mut df = df!(
        "x" => (0..10).collect::<Vec<i32>>(),
        "y" => (0..10).map(|i| i * 2).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.clone()],
        OpenOptions::default(),
    );
    assert!(app.data_table_state.is_some());
    assert_eq!(app.input_mode, InputMode::Normal);

    let key_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_c));
    assert_eq!(app.input_mode, InputMode::Chart);
    assert!(app.chart_modal.active);

    let key_esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_esc));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.chart_modal.active);
}

#[test]
fn test_chart_q_does_not_exit() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("chart_q_test.csv");

    let mut df = df!("a" => &[1_i32], "b" => &[2_i32]).unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);

    // q does nothing in chart view (no exit)
    let key_q = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    let out = app.event(&AppEvent::Key(key_q));
    assert!(out.is_none());
    assert_eq!(app.input_mode, InputMode::Chart);
}

/// Opens a small x/y dataset in the chart view. Nothing is selected yet.
fn open_chart_view(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join(name);
    let mut df = df!(
        "x" => (0..5).collect::<Vec<i32>>(),
        "y" => (0..5).map(|i| i * 3).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);
    (app, rx, tx)
}

/// Feed background results back until the chart for the current selection is prepared.
fn pump_until_chart_ready(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) {
    for _ in 0..500 {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if app.chart_data_ready() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("chart data was not prepared within 5 seconds");
}

/// Chart data is prepared off the render path: selecting columns starts a background
/// computation (with the throbber up), render draws nothing until it lands, and then
/// draws the prepared series. Nothing here collects on the calling thread.
#[test]
fn test_chart_data_is_prepared_in_the_background() {
    let (mut app, rx, tx) = open_chart_view("chart_render_cache_test.csv");

    // Nothing selected: render draws the empty view and asks for nothing.
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    assert!(!app.chart_preparing());

    // Select x and y, then let any event go through so the selection is noticed.
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(
        app.chart_preparing(),
        "a selection starts a background prepare"
    );
    assert!(!app.chart_data_ready());
    assert!(
        !app.is_busy(),
        "chart preparation must not lock the keyboard"
    );

    // Render while it computes must not block or panic; it just has no data yet.
    Widget::render(&mut app, area, &mut buf);

    pump_until_chart_ready(&mut app, &rx, &tx);
    assert!(!app.chart_preparing());
    Widget::render(&mut app, area, &mut buf);

    // Closing the chart drops the cache and any late result.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.chart_preparing());
}

/// Holding a key through the options must not fan out into a collect per step: one
/// preparation runs at a time, and when it lands the newest selection is the one prepared.
#[test]
fn test_chart_prepares_one_selection_at_a_time() {
    use datui::chart_modal::ChartKind;
    let (mut app, rx, tx) = open_chart_view("chart_one_at_a_time_test.csv");
    app.chart_modal.chart_kind = ChartKind::Histogram;
    app.chart_modal.hist_column = Some("x".to_string());
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Five more distinct requests while the first is still out.
    for _ in 0..5 {
        app.chart_modal.hist_bins += 1;
        app.event(&AppEvent::Resize(80, 24));
    }

    let mut results = 0;
    for _ in 0..500 {
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, AppEvent::BackgroundChartReady { .. }) {
                results += 1;
            }
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if app.chart_data_ready() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(app.chart_data_ready());
    assert_eq!(
        results, 2,
        "the first request, then the newest; the four in between were never spawned"
    );
}

/// An export parked while a *different*, failing selection is in flight is not failed
/// with that selection's error: it waits for the current selection's data and completes.
#[test]
fn test_chart_export_waits_for_the_current_selection_not_a_failed_one() {
    use datui::chart_export::ChartExportFormat;
    let (mut app, rx, tx) = open_chart_view("chart_export_after_failure_test.csv");
    // x against x cannot be charted (duplicate column) and takes a moment to fail.
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["x".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Move on to a valid selection while that one is still out, and ask for an export.
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.eps");
    let next = app
        .event(&AppEvent::ChartExport(
            path.clone(),
            ChartExportFormat::Eps,
            String::new(),
            400,
            300,
        ))
        .expect("ChartExport defers to DoChartExport");
    app.event(&next);

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        path.exists(),
        "the export completed from the valid selection"
    );
    assert!(
        !app.chart_export_modal.active,
        "no error reopened the modal"
    );
    assert!(app.chart_data_ready());
}

/// A chart export uses the prepared data and writes the file off-thread; if the data is
/// not ready yet the export waits for it rather than collecting on the UI thread.
#[test]
fn test_chart_export_waits_for_prepared_data_and_writes_in_background() {
    use datui::chart_export::ChartExportFormat;
    let (mut app, rx, tx) = open_chart_view("chart_export_bg_test.csv");
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.eps");
    // Asked for while the data is still being prepared.
    let next = app
        .event(&AppEvent::ChartExport(
            path.clone(),
            ChartExportFormat::Eps,
            String::new(),
            400,
            300,
        ))
        .expect("ChartExport defers to DoChartExport");
    app.event(&next);
    assert!(
        app.is_busy(),
        "an export owns the busy state until it finishes"
    );

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        path.exists(),
        "the export was written once its data arrived"
    );
    assert!(
        !app.chart_export_modal.active,
        "the export modal closes on success"
    );
}

/// Wait for the outcome of a background scan.
///
/// Scanning runs off the event thread so a slow one cannot freeze the interface, so
/// its result arrives over the channel rather than as a return value.
fn await_scan_outcome(rx: &mpsc::Receiver<AppEvent>) -> AppEvent {
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .expect("background scan should report an outcome")
}

#[test]
fn test_open_s3_url_returns_crash_or_loads() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("s3://my-bucket/path/to/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));

    // The scan is spawned, so this returns nothing; the outcome comes over the channel.
    assert!(
        app.event(&ev).is_none(),
        "scan should be spawned, not run inline"
    );

    match await_scan_outcome(&rx) {
        AppEvent::BackgroundError { message, .. } => {
            assert!(message.contains("S3"), "error should mention S3: {message}");
        }
        AppEvent::BackgroundLazyFrameReady { .. } => {
            // With cloud feature and valid credentials/bucket, the scan can succeed.
        }
        _ => panic!("expected a scan outcome for an S3 URL"),
    }
}

#[test]
fn test_open_http_url_attempts_load_or_returns_friendly_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("https://example.com/data.csv");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let mut next = app.event(&ev);
    while let Some(ref e) = next {
        if matches!(
            e,
            AppEvent::Crash(_) | AppEvent::DoLoadSchema(..) | AppEvent::DoLoadSchemaBlocking(..)
        ) {
            break;
        }
        next = app.event(e);
    }
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => {
            assert!(
                m.contains("HTTP")
                    || m.contains("HTTPS")
                    || m.contains("download")
                    || m.contains("Failed")
                    || m.contains("failed")
                    || m.contains("not yet supported"),
                "error should mention HTTP/download/failure: {}",
                m
            );
        }
        Some(AppEvent::DoLoadSchema(..)) | Some(AppEvent::DoLoadSchemaBlocking(..)) => {
            // With http feature: download can succeed; schema load is the next phase.
        }
        None => {
            // DoLoadScanPaths shows download confirmation modal and returns None; app is waiting for user.
        }
        _ => panic!("expected Crash, DoLoadSchema, or None (confirmation) when opening HTTP URL"),
    }
}

#[test]
fn test_multiple_remote_paths_returns_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let paths = vec![
        PathBuf::from("s3://bucket/a.parquet"),
        PathBuf::from("s3://bucket/b.parquet"),
    ];
    let next = app.event(&AppEvent::Open(paths, OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one S3") || m.contains("one at a time"),
            "error should mention single S3 path: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple S3 URLs"),
    }
    let (tx2, _) = mpsc::channel();
    let mut app = App::new(tx2, common::test_runtime());
    let paths = vec![
        PathBuf::from("https://example.com/a.csv"),
        PathBuf::from("https://example.com/b.csv"),
    ];
    let next = app.event(&AppEvent::Open(paths, OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one") && (m.contains("HTTP") || m.contains("URL")),
            "error should mention single URL: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple HTTP URLs"),
    }
}

#[test]
fn test_open_gs_url_returns_friendly_error_or_attempts_load() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("gs://my-bucket/path/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));

    assert!(
        app.event(&ev).is_none(),
        "scan should be spawned, not run inline"
    );

    match await_scan_outcome(&rx) {
        AppEvent::BackgroundError { message, .. } => {
            assert!(
                message.contains("GCS")
                    || message.contains("gs://")
                    || message.contains("not enabled"),
                "error should mention GCS or gs:// or not enabled: {message}"
            );
        }
        AppEvent::BackgroundLazyFrameReady { .. } => {}
        _ => panic!("expected a scan outcome for a gs:// URL"),
    }
}

#[test]
fn test_csv_null_values_global() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("null_values_test.csv");
    std::fs::write(&csv_path, "x,y\n1,NA\n2,3\n4,N/A\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["NA".to_string(), "N/A".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let y = df.column("y").unwrap();
    assert_eq!(
        y.null_count(),
        2,
        "NA and N/A should be parsed as null in column y"
    );
}

#[test]
fn test_csv_null_values_per_column() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("null_values_per_col_test.csv");
    std::fs::write(&csv_path, "a,b\nx,1\nempty,2\nz,3\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["a=empty".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let a = df.column("a").unwrap();
    let b = df.column("b").unwrap();
    assert_eq!(a.null_count(), 1, "only 'empty' in column a should be null");
    assert_eq!(b.null_count(), 0, "column b has no per-column null spec");
}

/// Simulate the real main-loop startup: process events from the channel, render
/// the widget (which sets visible_rows and needs_recollect), then check the flag
/// and spawn another async collect.  Repeat many times to shake out races between
/// the initial tiny-buffer collect (visible_rows=0) and the corrected one.
#[test]
fn test_startup_buffer_race_does_not_lose_rows() {
    common::ensure_sample_data();
    let csv_path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let terminal_area = Rect::new(0, 0, 120, 50); // 50 rows → ~48 visible

    for iteration in 0..50 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());

        // Simulate what run() does: send Open, mark busy.
        tx.send(AppEvent::Open(
            vec![csv_path.clone()],
            OpenOptions::default(),
        ))
        .unwrap();

        // Process events like the main loop: drain channel, render, check needs_recollect.
        let mut completed = false;
        for _tick in 0..200 {
            // Drain all pending events.
            loop {
                match rx.try_recv() {
                    Ok(AppEvent::Crash(msg)) => panic!("iteration {iteration}: Crash: {msg}"),
                    Ok(event) => {
                        if let Some(next) = app.event(&event) {
                            tx.send(next).unwrap();
                        }
                    }
                    Err(_) => break,
                }
            }

            // Render into a buffer (this sets visible_rows and may set needs_recollect).
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);

            // After render, check needs_recollect — same as the real main loop.
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }

            // Check if we have data and are no longer busy.
            if app.data_table_state.is_some() && !app.is_busy() {
                completed = true;
                break;
            }

            // Brief sleep to let background tasks run (simulates poll timeout).
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert!(
            completed,
            "iteration {iteration}: timed out waiting for data to load"
        );

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.visible_rows > 0,
            "iteration {iteration}: visible_rows should be set by render"
        );

        // The buffer must cover at least the visible window.
        let buffered_rows = state.buffered_end().saturating_sub(state.buffered_start());
        assert!(
            buffered_rows >= state.visible_rows || buffered_rows >= state.num_rows,
            "iteration {iteration}: buffer too small: {buffered_rows} buffered but \
             {visible} visible, {total} total rows",
            visible = state.visible_rows,
            total = state.num_rows,
        );

        // display_slice_df must be Some (not None = no data to show).
        assert!(
            state.display_slice_df().is_some(),
            "iteration {iteration}: display_slice_df is None — buffer not sliced into display"
        );
    }
}

/// A `Background*Ready` event whose `generation` no longer matches the App's
/// `task_generation` must NOT mutate visible state. This guards every non-collect
/// `Background*` handler against the same stale-generation race that the collect
/// path got in commit e4d65e3.
#[test]
fn test_stale_background_events_are_ignored() {
    use datui::statistics::AnalysisResults;

    common::ensure_sample_data();
    let csv_path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // After load, generation is non-zero. Anything tagged generation=0 is stale.
    let stale_gen: u64 = 0;
    assert!(
        app.task_generation() > stale_gen,
        "expected generation to advance past 0 after load"
    );

    // Build dummy results we can identify in modal slots.
    let dummy = AnalysisResults {
        column_statistics: vec![],
        total_rows: 999_999,
        sample_size: None,
        sample_seed: 0,
        correlation_matrix: None,
        distribution_analyses: vec![],
    };

    // Each variant: send with stale generation, assert nothing landed in the modal.
    app.analysis_modal.describe_results = None;
    app.event(&AppEvent::BackgroundDescribeReady {
        generation: stale_gen,
        results: dummy.clone(),
    });
    assert!(
        app.analysis_modal.describe_results.is_none(),
        "stale BackgroundDescribeReady should not write describe_results"
    );

    app.analysis_modal.distribution_results = None;
    app.event(&AppEvent::BackgroundDistributionReady {
        generation: stale_gen,
        results: dummy.clone(),
    });
    assert!(
        app.analysis_modal.distribution_results.is_none(),
        "stale BackgroundDistributionReady should not write distribution_results"
    );

    app.analysis_modal.correlation_results = None;
    app.event(&AppEvent::BackgroundCorrelationReady {
        generation: stale_gen,
        results: dummy,
    });
    assert!(
        app.analysis_modal.correlation_results.is_none(),
        "stale BackgroundCorrelationReady should not write correlation_results"
    );
}

/// Regression for commit 7b7bfe8: holding PageDown at the end of the data once
/// pushed `start_row` past `num_rows`, leaving the app `busy` because every spawn
/// no-op'd (buffer already valid after clamp) but the handler used to gate on
/// `needs && spawn`. Now `slide_table` clamps forward scroll, and the App
/// `handle_scroll` clears `busy` whether or not the spawn actually ran.
#[test]
fn test_scroll_past_end_does_not_hang_busy() {
    // Inline 200-row CSV so the test stays cheap and self-contained.
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("scroll_past_end_test.csv");
    let mut df = polars::df!(
        "id" => (0..200i64).collect::<Vec<_>>(),
        "value" => (0..200i64).map(|i| i * 10).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render once so visible_rows is set for real, then settle the post-render bounce.
    let settle = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>| {
        for _ in 0..200 {
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }
            while let Ok(ev) = rx.try_recv() {
                if let Some(next) = app.event(&ev) {
                    let _ = tx.send(next);
                }
            }
            if !app.is_busy() && !needs {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("app did not settle within 2 seconds");
    };
    settle(&mut app, &rx, &tx);

    let total = app.data_table_state.as_ref().unwrap().num_rows;
    assert!(total > 0, "test data should have rows");

    // Jump to end via End key, then hammer PageDown a bunch — same sequence that
    // used to wedge the app. Each PageDown sets `busy=true` in the key handler;
    // DoScrollDown must clear it once the spawn no-ops past the bottom.
    if let Some(next) = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::End,
        KeyModifiers::NONE,
    ))) {
        let _ = tx.send(next);
    }
    settle(&mut app, &rx, &tx);

    for i in 0..15 {
        if let Some(next) = app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::PageDown,
            KeyModifiers::NONE,
        ))) {
            let _ = tx.send(next);
        }
        settle(&mut app, &rx, &tx);
        assert!(
            !app.is_busy(),
            "iteration {i}: PageDown past end must not leave busy stuck"
        );
    }
}

/// After a transform that invalidates `num_rows`, `spawn_async_collect` should
/// dispatch a background `len()` first (no UI thread blocking) and then chain
/// into the actual buffer collect. This test verifies the two-phase load
/// completes and yields a valid buffer.
#[test]
fn test_async_collect_handles_invalidated_num_rows() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("invalidated_num_rows_test.csv");
    let mut df = polars::df!(
        "id" => (0..500i64).collect::<Vec<_>>(),
        "value" => (0..500i64).map(|i| i * 2).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render so visible_rows is set; settle the bounce.
    let mut buf = Buffer::empty(terminal_area);
    app.render(terminal_area, &mut buf);
    drain_events(&mut app, &rx);

    // Apply a filter via the public event. This invalidates num_rows.
    let filter = FilterStatement {
        column: "value".to_string(),
        operator: FilterOperator::Lt,
        value: "200".to_string(),
        logical_op: LogicalOperator::And,
    };
    app.event(&AppEvent::Filter(vec![filter]));

    // Drain BackgroundLenReady then BackgroundCollectReady.
    for _ in 0..200 {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(
        !app.is_busy(),
        "filter + async len + collect should complete"
    );
    let state = app.data_table_state.as_ref().unwrap();
    // value < 200 → ids 0..100 → 100 rows
    assert_eq!(state.num_rows, 100, "filtered row count should be 100");
    assert!(
        state.display_slice_df().is_some(),
        "display buffer should be populated after async len + collect"
    );
}

/// Opening a Parquet hive directory should paint the first buffer and resolve the exact
/// total row count via the footer-sum path (Fix 1 + Fix 2), without a full data scan.
#[test]
fn test_hive_dir_loads_and_counts_via_footers() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |sub: &str, n: i64| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
        let f = File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut df).unwrap();
    };
    // Hive layout across two partition keys; 30 + 12 + 8 = 50 rows total.
    mk("form_type=a/year=2020", 30);
    mk("form_type=a/year=2021", 12);
    mk("form_type=b/year=2020", 8);

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);

    // Drive render -> buffer collect -> background count to completion.
    let mut counted = false;
    for _ in 0..200 {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        counted = app
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid())
            == Some(50);
        if !app.is_busy() && !needs && counted {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert!(counted, "exact total should resolve to the footer sum (50)");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows, 50, "hive dir total should equal footer sum");
    assert!(
        state.display_slice_df().is_some(),
        "first buffer should be populated"
    );
}

// ---------------------------------------------------------------------------
// Abandoning an in-flight load (Ctrl+O to the home screen)
// ---------------------------------------------------------------------------

/// Drains like the real main loop does: a handler that returns a follow-up event
/// queues it and *ends the pass*, so one frame is drawn between chain steps.
///
/// `pump_open_until_loaded` above chases the chain without breaking, which cannot
/// reproduce a keypress landing between two steps — exactly the window abandonment
/// has to survive. Returns the number of chain steps taken this pass.
fn drain_like_main_loop(
    app: &mut App,
    tx: &mpsc::Sender<AppEvent>,
    rx: &mpsc::Receiver<AppEvent>,
) -> usize {
    let mut steps = 0;
    loop {
        match rx.try_recv() {
            Ok(AppEvent::Crash(msg)) => panic!("Crash during load: {msg}"),
            Ok(event) => {
                if let Some(next) = app.event(&event) {
                    tx.send(next).unwrap();
                    steps += 1;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    steps
}

fn ctrl_o() -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
}

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// Every glyph in the buffer, in row order — enough to ask whether some text is on
/// screen, which is all these tests need.
fn rendered_text(buf: &Buffer) -> String {
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// The same, without the control bar on the last row. The bar reports a load on its
/// own; assertions about what the *view* shows have to exclude it.
fn main_area_text(buf: &Buffer, area: Rect) -> String {
    let cells = (area.width as usize) * (area.height as usize - 1);
    buf.content()
        .iter()
        .take(cells)
        .map(|cell| cell.symbol())
        .collect()
}

/// Ctrl+O at any point during a load must abandon it: whatever is on screen when the
/// user goes home is what is still there afterwards. The load runs to completion in
/// the background and its results are dropped.
///
/// Parameterised over how many chain steps have run, because the pipeline has many
/// interstitial frames and each one is a place the user can press the key.
#[test]
fn test_abandoned_load_never_installs_itself_afterwards() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let area = Rect::new(0, 0, 120, 50);

    for abandon_after in 0..8 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        tx.send(AppEvent::Open(vec![path.clone()], OpenOptions::default()))
            .unwrap();

        let mut steps = 0usize;
        let mut abandoned_at: Option<(Option<PathBuf>, bool)> = None;
        let mut ticks_since_abandon = 0usize;

        for _tick in 0..200 {
            steps += drain_like_main_loop(&mut app, &tx, &rx);

            // Abandon once the chain has taken `abandon_after` steps, or as soon as it
            // has finished if it was shorter than that — going home after a completed
            // load must be just as inert.
            let chain_done = !app.is_busy() && app.data_table_state.is_some();
            if abandoned_at.is_none() && (steps >= abandon_after || chain_done) {
                app.event(&ctrl_o());
                abandoned_at = Some((
                    app.open_path().map(Path::to_path_buf),
                    app.data_table_state.is_some(),
                ));
            }

            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);

            // Long enough for the abandoned scan, schema and count to finish and be
            // dropped; running the full 200 ticks on a million rows is pure wall clock.
            if abandoned_at.is_some() {
                ticks_since_abandon += 1;
                if ticks_since_abandon >= 40 {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let (path_at_abandon, had_state) = abandoned_at.expect("never reached the abandon point");

        assert_eq!(
            app.input_mode,
            InputMode::Home,
            "abandon_after {abandon_after}: should still be at home"
        );
        assert!(
            !app.is_busy(),
            "abandon_after {abandon_after}: abandoning a load must clear busy"
        );
        assert_eq!(
            app.open_path().map(Path::to_path_buf),
            path_at_abandon,
            "abandon_after {abandon_after}: the abandoned load swapped its dataset in afterwards"
        );
        assert_eq!(
            app.data_table_state.is_some(),
            had_state,
            "abandon_after {abandon_after}: data_table_state changed after abandonment"
        );
    }
}

/// The regression this whole change exists for: abandon a slow load, open something
/// else, and the abandoned load must not overwrite the dataset you actually asked
/// for — including its row count, which is counted by a separate background task.
#[test]
fn test_abandoned_load_does_not_corrupt_the_next_open() {
    common::ensure_sample_data();
    let big = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let small = PathBuf::from("tests/sample-data/sales.parquet");
    let area = Rect::new(0, 0, 120, 50);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![big], OpenOptions::default()))
        .unwrap();

    // Let the big load get underway, then leave.
    for _tick in 0..3 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
    }
    app.event(&ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);

    tx.send(AppEvent::Open(vec![small.clone()], OpenOptions::default()))
        .unwrap();

    for _tick in 0..200 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert_eq!(
        app.open_path(),
        Some(small.as_path()),
        "the dataset opened after abandoning should be the one on screen"
    );
    let state = app
        .data_table_state
        .as_ref()
        .expect("second open should have installed a dataset");
    assert_eq!(
        state.num_rows_if_valid(),
        Some(5000),
        "row count belongs to the dataset that is open, not the abandoned one"
    );
}

/// Abandoning drops the incoming dataset, not the one already on screen. Esc from
/// home has to put the user back where they were.
#[test]
fn test_escape_from_home_returns_to_the_dataset_that_was_open() {
    common::ensure_sample_data();
    let open_first = PathBuf::from("tests/sample-data/people.parquet");
    let abandoned = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(
        vec![open_first.clone()],
        OpenOptions::default(),
    ))
    .unwrap();

    // Render as we go: `visible_rows` is set by the render, and without it there is
    // no display slice to assert on later.
    for _tick in 0..200 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(app.open_path(), Some(open_first.as_path()));
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "first dataset should be displayable before we abandon anything"
    );

    // Start a second load and leave before it can install.
    tx.send(AppEvent::Open(vec![abandoned], OpenOptions::default()))
        .unwrap();
    drain_like_main_loop(&mut app, &tx, &rx);
    app.event(&ctrl_o());

    for _tick in 0..100 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert_eq!(
        app.input_mode,
        InputMode::Normal,
        "Esc from home should return to the open dataset"
    );
    assert_eq!(
        app.open_path(),
        Some(open_first.as_path()),
        "the abandoned load must not have replaced what was open"
    );
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "the dataset we returned to should still have its buffer"
    );
}

/// Going home clears the *load's* busy state, and leaves `task_generation` alone —
/// that counter also gates analysis and export results, which keep running.
#[test]
fn test_entering_home_clears_load_state_but_not_task_generation() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    drain_like_main_loop(&mut app, &tx, &rx);
    assert!(app.is_busy(), "a load in flight should be busy");

    let generation_before = app.task_generation();
    app.enter_home();

    assert_eq!(app.input_mode, InputMode::Home);
    assert!(
        !app.is_busy(),
        "abandoning should clear the load's busy flag"
    );
    assert!(
        app.should_drain_keys(),
        "keys typed at the frozen screen were meant for the load"
    );
    assert_eq!(
        app.task_generation(),
        generation_before,
        "going home must not cancel an in-flight export or analysis"
    );
}

/// Opening a remote URL raises a "Continue with download?" confirmation. Declining it
/// used to quit datui, and Ctrl+O was swallowed while it was up, which made a remote
/// open the one thing in the app you could not back out of.
#[cfg(feature = "http")]
fn app_awaiting_download_confirmation() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    // Refused immediately, so the size probe does not sit on its timeout.
    let url = PathBuf::from("http://127.0.0.1:1/data.csv");
    let mut next = app.event(&AppEvent::Open(vec![url], OpenOptions::default()));
    while let Some(ev) = next {
        if matches!(ev, AppEvent::Crash(_)) {
            break;
        }
        next = app.event(&ev);
    }
    // The size probe runs on a background thread now, so the modal arrives by event
    // rather than before the open call returns.
    //
    // The budget is deliberately far longer than the probe should ever need. The
    // first HTTP agent built in a process loads the platform certificate store,
    // which is slow on a cold Windows runner, and a second was not enough: both
    // tests using this helper failed there on the v0.3.2 release commit, the first
    // time Windows had run them. What is being asserted is that datui asks before
    // downloading, not that it asks within any particular time.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !app.awaiting_download_confirmation() && std::time::Instant::now() < deadline {
        while let Ok(ev) = rx.try_recv() {
            if let Some(follow_up) = app.event(&ev) {
                app.event(&follow_up);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    (app, rx)
}

#[cfg(feature = "http")]
#[test]
fn test_declining_a_download_goes_home_instead_of_quitting() {
    let (mut app, _rx) = app_awaiting_download_confirmation();
    assert!(
        app.awaiting_download_confirmation(),
        "opening a remote URL should ask before downloading"
    );

    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "declining a download must not quit datui"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "declining a download should leave the user at home"
    );
    assert!(!app.awaiting_download_confirmation());
}

#[cfg(feature = "http")]
#[test]
fn test_ctrl_o_escapes_the_download_confirmation() {
    let (mut app, _rx) = app_awaiting_download_confirmation();
    assert!(app.awaiting_download_confirmation());

    app.event(&ctrl_o());

    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "Ctrl+O should work while the download confirmation is up"
    );
    assert!(
        !app.awaiting_download_confirmation(),
        "leaving should clear the pending download, not leave it armed"
    );
}

/// Every `DataTableState` must get its own `len_generation`. They used to all start at
/// zero, so an exact row count still running for the dataset you just closed matched
/// the one you just opened and set its row count to the wrong number.
#[test]
fn test_len_generations_are_unique_across_datasets() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );
    let first = app
        .data_table_state
        .as_ref()
        .expect("first dataset should load")
        .len_generation();

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/sales.parquet")],
        OpenOptions::default(),
    );
    let second = app
        .data_table_state
        .as_ref()
        .expect("second dataset should load")
        .len_generation();

    assert_ne!(
        first, second,
        "two datasets must not share a row-count generation"
    );
}

/// Modals render over the home screen, but home used to consume every key, so one
/// raised while the user was at home could not be dismissed: Esc went to home_escape,
/// which at the time quit when nothing was loaded. The only way past an error was to
/// leave datui.
#[test]
fn test_error_modal_over_home_is_dismissable() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    app.event(&AppEvent::BackgroundError {
        generation: app.task_generation(),
        message: "could not read the file".to_string(),
    });

    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc should dismiss the modal, not quit out from under it"
    );

    // With the modal gone, Esc is home's again. An empty home has nowhere left to
    // back out to, so it does nothing; Ctrl+C is what quits.
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc at the top of the home screen must not quit"
    );
    assert_eq!(app.input_mode, InputMode::Home);
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        matches!(out, Some(AppEvent::Exit)),
        "Ctrl+C quits from the home screen"
    );
}

/// Opening a second dataset from the home screen must not show the first one's rows
/// while the second is still loading. Between the keypress and the new dataset being
/// installed, the old table was still on screen — a page of one file's data under the
/// filename of another, for as long as the load took.
#[test]
fn test_opening_from_home_does_not_show_the_previous_dataset() {
    common::ensure_sample_data();
    let first = PathBuf::from("tests/sample-data/people.parquet");
    let second = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![first.clone()], OpenOptions::default()))
        .unwrap();

    for _tick in 0..200 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    assert!(
        rendered_text(&buf).contains("first_name"),
        "the first dataset should be on screen before we go home"
    );

    // Home, then open the second dataset the way a user does: through the path
    // prompt, so the real key path runs rather than a synthesised event.
    app.event(&ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);
    app.event(&key(KeyCode::Char('~')));
    for c in second.to_str().unwrap().chars() {
        app.event(&key(KeyCode::Char(c)));
    }
    if let Some(next) = app.event(&key(KeyCode::Enter)) {
        tx.send(next).unwrap();
    }
    assert_eq!(
        app.input_mode,
        InputMode::Normal,
        "opening from home should leave the home screen"
    );

    // Every frame from here until the second dataset is installed.
    let mut frames = 0usize;
    for _tick in 0..200 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        frames += 1;
        let text = main_area_text(&buf, area);
        assert!(
            !text.contains("first_name") && !text.contains("job_title"),
            "frame {frames} showed the previous dataset while the next one was loading"
        );
        assert!(
            text.contains("large_dataset.parquet") || text.contains("dist_normal"),
            "frame {frames} named neither the dataset being loaded nor the one that arrived"
        );
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.open_path() == Some(second.as_path()) && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(
        app.open_path(),
        Some(second.as_path()),
        "the second dataset should have loaded"
    );
    assert!(
        frames > 1,
        "the load finished in one frame; nothing was tested"
    );
}

/// The one-file schema types partition columns the way a full scan does.
#[test]
fn test_hive_partition_types_match_full_scan() {
    use datui::widgets::datatable::DataTableState;

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

    let (fast, parts) = DataTableState::schema_from_one_hive_parquet(dir.path()).unwrap();
    assert_eq!(parts, ["region", "year", "day"]);
    let mut full = LazyFrame::scan_parquet(
        PlPath::Local(Arc::from(dir.path())),
        ScanArgsParquet {
            hive_options: polars::io::HiveOptions::new_enabled(),
            ..Default::default()
        },
    )
    .unwrap();
    let full = full.collect_schema().unwrap();
    for name in parts {
        assert_eq!(fast.get(&name), full.get(&name), "{name}");
    }
    assert_eq!(fast.get("year"), Some(&DataType::Int64));

    let lf = DataTableState::scan_parquet_hive_with_schema(dir.path(), fast).unwrap();
    let df = lf.filter(col("year").gt(lit(2020))).collect().unwrap();
    assert_eq!(df.height(), 2);
}

/// Esc at a bucket's top goes back to the home listing, not to a directory named `gs:`.
#[test]
fn test_escape_from_a_bucket_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(PathBuf::from("gs://bucket"));

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A remote location shows that it is being listed, not "No datasets here.", until its
/// listing arrives.
#[test]
fn test_remote_listing_shows_progress_until_it_arrives() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let dir = PathBuf::from("gs://bucket/demo");
    app.home.browsing = Some(dir.clone());

    let area = Rect::new(0, 0, 100, 20);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let waiting = screen(&mut app);
    assert!(waiting.contains("Listing gs://bucket/demo"), "{waiting}");
    assert!(!waiting.contains("No datasets here."), "{waiting}");

    app.home.probe_ready(dir, Vec::new());
    let done = screen(&mut app);
    assert!(!done.contains("Listing gs://bucket/demo"), "{done}");
    assert_eq!(app.home.waiting_since, None);
}

/// Esc retraces a browse back to the listing and stops there, however deep the user
/// went — it does not climb above the directory the browse began at.
#[test]
fn test_escape_stops_at_where_browsing_began() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    let deeper = start.join("b");
    std::fs::create_dir_all(&deeper).unwrap();
    let key = |app: &mut App, code| {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };

    app.home.path_input_active = true;
    app.home.path_input = start.display().to_string();
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));

    // As if Enter had descended into `b`.
    app.home.browsing = Some(deeper);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// Backspace still climbs above where a browse began, and Esc from there goes back to
/// the listing rather than on up the tree.
#[test]
fn test_escape_after_backspace_above_the_start_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    std::fs::create_dir_all(&start).unwrap();
    app.home.browsing = Some(start.clone());
    app.home.browse_start = Some(start);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Backspace,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing.as_deref(), Some(tmp.path()));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
}

/// Feed background results back into the app until it is no longer busy.
fn pump_until_idle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    for _ in 0..500 {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if !app.is_busy() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("app did not settle within 5 seconds");
}

/// A 100-row table: `a` 0..100, `c` = a % 3, `name` "alpha_N" for even and "beta_N" for odd `a`.
fn open_query_filter_fixture(
    name: &str,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join(name);
    let mut df = df!(
        "a" => (0..100i64).collect::<Vec<_>>(),
        "c" => (0..100i64).map(|i| i % 3).collect::<Vec<_>>(),
        "name" => (0..100i64)
            .map(|i| if i % 2 == 0 { format!("alpha_{i}") } else { format!("beta_{i}") })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows, 100);
    (app, rx, tx)
}

fn current_rows(app: &App) -> usize {
    let state = app.data_table_state.as_ref().unwrap();
    state.lf.clone().collect().unwrap().height()
}

fn filter_stmt(
    column: &str,
    operator: datui::filter_modal::FilterOperator,
    value: &str,
) -> datui::filter_modal::FilterStatement {
    datui::filter_modal::FilterStatement {
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op: datui::filter_modal::LogicalOperator::And,
    }
}

/// A sidebar filter applies on top of the active DSL query rather than replacing it, and
/// clearing the filters returns to the query result. Reset still clears everything.
#[test]
fn test_sidebar_filter_applies_on_top_of_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("query_then_filter.csv");

    app.event(&AppEvent::Search("select where a < 50".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "1",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..50 with a % 3 == 1: 1, 4, ..., 49
    assert_eq!(
        current_rows(&app),
        17,
        "filter must apply to the query result"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where a < 50");
    assert_eq!(state.get_filters().len(), 1);

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        current_rows(&app),
        50,
        "clearing filters returns to the query result"
    );

    app.event(&AppEvent::Reset);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
    assert!(app
        .data_table_state
        .as_ref()
        .unwrap()
        .get_active_query()
        .is_empty());
}

/// Same for a fuzzy search: sort and filter stack on it, and clearing them keeps it.
#[test]
fn test_sidebar_filter_and_sort_keep_fuzzy_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("fuzzy_then_filter.csv");

    app.event(&AppEvent::FuzzySearch("alpha".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "a",
        FilterOperator::Lt,
        "20",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Sort(vec!["a".to_string()], false));
    pump_until_idle(&mut app, &rx, &tx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(18));

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_fuzzy_query(),
        "alpha"
    );
}

/// And for SQL.
#[test]
fn test_sidebar_filter_keeps_sql_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("sql_then_filter.csv");

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..30 with a % 3 == 0: 0, 3, ..., 27
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// SQL runs against the data as loaded, like the DSL and fuzzy queries: a sidebar filter
/// that was active when the SQL ran is not baked into its result, so clearing the
/// filters afterwards shows the SQL result over the whole table.
#[test]
fn test_sql_runs_against_the_loaded_data_not_the_filtered_view() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("filter_then_sql.csv");

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30, "the SQL replaces the filter");
    assert!(app
        .data_table_state
        .as_ref()
        .unwrap()
        .get_filters()
        .is_empty());

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// Opens an inline CSV with the given options and settles the load.
fn open_csv_with(
    name: &str,
    contents: &str,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join(name);
    std::fs::write(&csv_path, contents).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], options);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());
    (app, rx, tx)
}

/// Footer rows dropped with `skip_tail_rows` stay dropped after a sidebar sort: the
/// load-time trimming is part of the pipeline's root, not just of the first view.
#[test]
fn test_skip_tail_rows_survives_a_sidebar_sort() {
    let mut csv = String::from("a,b\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},{}\n", i * 2));
    }
    // Two summary rows at the end, the kind `skip_tail_rows` exists for.
    csv.push_str("9999,-1\n9998,-2\n");
    let options = OpenOptions {
        skip_tail_rows: Some(2),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("skip_tail_then_sort.csv", &csv, options);
    assert_eq!(current_rows(&app), 100);

    app.event(&AppEvent::Sort(vec!["b".to_string()], false));
    pump_until_idle(&mut app, &rx, &tx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 100, "the footer rows must not come back");
    assert_eq!(
        df.column("b").unwrap().get(0).unwrap(),
        AnyValue::Int64(198)
    );
}

/// Numbers parsed out of padded strings with `parse_strings` are still numbers when a
/// sidebar filter compares them.
#[test]
fn test_parse_strings_survives_a_sidebar_filter() {
    use datui::filter_modal::FilterOperator;
    use datui::ParseStringsTarget;
    let mut csv = String::from("id,amount\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},\" {} \"\n", i * 3));
    }
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("parse_strings_then_filter.csv", &csv, options);
    {
        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.schema.get("amount").unwrap().is_integer(),
            "parse_strings should have made amount numeric"
        );
    }

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "amount",
        FilterOperator::Gt,
        "150",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    // amount = 3 * id > 150 for id 51..100
    assert_eq!(current_rows(&app), 49);
}

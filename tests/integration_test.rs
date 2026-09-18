use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::event_pump::EventPump;
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
        if let Some(next) = app.event(&ev)
            && let Some(next2) = app.event(&next)
        {
            app.event(&next2);
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
        match next.take() {
            Some(ev) => {
                if matches!(ev, AppEvent::Crash(_)) {
                    app.event(&ev);
                    return;
                }
                next = app.event(&ev);
            }
            _ => {
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
            if matches!(ev, AppEvent::BackgroundChartReady) {
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
            // "Could not read from S3" without credentials; with them, the store's own
            // error naming the s3:// URL.
            assert!(
                message.to_lowercase().contains("s3"),
                "error should mention S3: {message}"
            );
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

/// Open a local folder of Parquet files and return the loaded app, or `None` if the
/// open never finished.
fn open_local_dataset(dir: &std::path::Path) -> App {
    open_local_dataset_with_channel(dir).0
}

/// The same, keeping the app's own event channel so a test can drive background work.
fn open_local_dataset_with_channel(
    dir: &std::path::Path,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.to_path_buf()], opts);
    (app, rx, tx)
}

/// Write one Parquet file at `sub/data.parquet` under `dir`.
fn write_parquet(dir: &std::path::Path, sub: &str, mut df: polars::prelude::DataFrame) {
    let d = dir.join(sub);
    std::fs::create_dir_all(&d).unwrap();
    let f = File::create(d.join("data.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut df).unwrap();
}

/// Render a loaded app until its buffer stops growing, and return what the table area
/// shows. Drains the app's events each pass: a buffer fill lands as one, so without it
/// the screen is whatever the first synchronous collect managed.
fn painted(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    area: Rect,
) -> String {
    let mut buf = Buffer::empty(area);
    for _ in 0..60 {
        app.render(area, &mut buf);
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if !needs && !app.is_busy() {
            break;
        }
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    app.render(area, &mut buf);
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// Three kinds of empty cell that used to look identical: a null the data holds, a
/// column the file was written without, and a column the file stores as text while the
/// dataset reads it as a number.
#[test]
fn test_absent_null_and_conflicting_cells_differ_on_screen() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // File one has `note` and a real null in it; it has no `extra` at all.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>], "n" => &[10i64]).unwrap(),
    );
    // File two has every column, and stores `n` as text, which the dataset reads as
    // the integer that most of its rows are.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"], "extra" => &["x"], "n" => &["oops"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "note" => &["yo"], "extra" => &["y"], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    let text = painted(&mut app, &rx, &tx, area);

    assert!(
        text.contains(g.absent),
        "a file written without `extra` should show the absent glyph {:?}, got:\n{}",
        g.absent,
        text
    );
    assert!(
        text.contains(g.null),
        "the real null in `note` should still show the null glyph {:?}",
        g.null
    );
    assert!(
        text.contains(g.conflict),
        "the file storing `n` as text should show the conflict glyph {:?}",
        g.conflict
    );
    assert!(
        text.contains(&format!("extra{}", g.drift_mark)),
        "and `extra` is marked in the header as not being in every file"
    );
    assert!(
        !text.contains(&format!("id{}", g.drift_mark)),
        "while `id`, which every file has, is not"
    );
}

/// Sorting reorders rows across files, so a row's position no longer says which file
/// it came from. The scan's drift column rides along with the row, so the distinction
/// survives.
#[test]
fn test_absent_cells_still_read_as_absent_after_a_sort() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "note" => &[None::<&str>, None]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "note" => &["hi", "yo"], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent before the sort"
    );

    // Descending by id interleaves the two files: 4, 3, 2, 1.
    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], false);
    state.collect();
    assert!(state.error.is_none(), "the sort itself must succeed");

    let text = painted(&mut app, &rx, &tx, area);
    assert!(
        text.contains(g.absent),
        "the rows from the file without `extra` are still absent, not null"
    );
    assert!(text.contains(g.null), "and the real nulls are still nulls");
}

/// The very first frame must mark the absent cells too.
///
/// Every other test here paints through `painted`, which renders up to sixty times, so
/// a mark that only arrives on the second frame looks identical to one that was always
/// there. In the app there is no second frame until something happens: datui draws the
/// dataset and waits. So this renders exactly once, into a fresh buffer, and reads the
/// glyph off it.
#[test]
fn test_the_first_frame_of_a_drifting_dataset_marks_its_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let area = Rect::new(0, 0, 80, 12);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .flat_map(|y| {
            (0..area.width)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();

    assert!(
        screen.contains(g.absent),
        "the row from the file without `extra` is absent, not null, on the first \
         frame as much as the second:\n{screen}"
    );
}

/// Pins the row arithmetic that everything else rests on.
///
/// The scan numbers each run's rows from where that run's first file begins in the
/// dataset. Every other fixture here has files of one or two rows, which makes a run's
/// starting row and its file's *index* the same number — so using one for the other
/// would go unnoticed. These files hold 3, 5 and 2 rows, and the third conflicts, which
/// splits the scan after row 8.
#[test]
fn test_each_row_takes_its_glyph_from_the_file_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    // No `n` at all: its cells are absent.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    // `n` as an integer, and the most rows, so the dataset reads it as one.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text: it cannot be read from here, so its cells conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let dataset = state.dataset_schema().expect("read from the footers");
    let (first, middle, last) = (
        dataset.file_group[0],
        dataset.file_group[1],
        dataset.file_group[2],
    );
    assert_eq!(middle, 0, "the middle file is missing nothing");
    assert_ne!(first, middle, "the first file has no `n`");
    assert_ne!(last, middle, "the last file holds `n` as text");

    let groups = state.display_drift(area.height as usize);
    assert_eq!(
        groups,
        vec![
            first, first, first, middle, middle, middle, middle, middle, last, last
        ],
        "three rows from the first file, five from the second, two from the third"
    );

    // Scrolled, and to a row inside the middle file rather than onto a boundary: the
    // window starts where the view does, so the groups have to shift with it.
    let state = app.data_table_state.as_mut().unwrap();
    state.start_row = 4;
    state.collect();
    assert_eq!(
        state.display_drift(6),
        vec![middle, middle, middle, middle, last, last],
        "from row 4: four more rows of the second file, then the third"
    );
}

/// A conflicting column has no value to order by, so a sort on it leaves those rows
/// out rather than gathering them at one end as though they belonged there — and says
/// how many went.
#[test]
fn test_a_sort_leaves_out_the_rows_its_column_is_not_read_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text here, so it is not read from this file: two rows of conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error.is_none(), "the sort itself must succeed");

    let ids: Vec<i64> = state
        .lf
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids.len(),
        8,
        "the two rows from the file that stores `n` as text are gone: {ids:?}"
    );
    assert!(
        !ids.contains(&8) && !ids.contains(&9),
        "and it is those two, not two others: {ids:?}"
    );
    assert!(
        ids.contains(&0) && ids.contains(&1) && ids.contains(&2),
        "the file with no `n` at all keeps its rows: its cells are absent, not a \
         value in another type: {ids:?}"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 1 file, so the 2 rows there are left out of the sort"
    );
    assert_eq!(left_out.scope, "in all 3 footers");
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back for a note the user has not been offered"
    );
}

/// The filter half: the other two wordings the note has, and the row a filter's own
/// terms matched but its file cannot stand behind.
///
/// A sidebar filter of `id = 3 or n = 0` matches the row whose `id` is 3 — but that
/// row's file stores `n` as text, so its `n` was never read and the view cannot
/// answer either half of the question. It goes, and the note says why.
#[test]
fn test_a_filter_leaves_out_the_rows_its_column_is_not_read_from() {
    use datui::filter_modal::{FilterOperator, LogicalOperator};

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let mut or_id_3 = filter_stmt("id", FilterOperator::Eq, "3");
    or_id_3.logical_op = LogicalOperator::Or;
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Eq, "0"), or_id_3]);
    assert!(state.error.is_none(), "the filter itself must succeed");

    let ids: Vec<i64> = state
        .lf
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids,
        vec![0],
        "id 3 matched a term of its own, but its file's `n` was never read"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 1 file, so the 2 rows there are left out of the filter"
    );

    // Sorting by the same column too: one note, naming both.
    state.sort(vec!["n".to_string()], true);
    let notes = state.notes();
    let both: Vec<&str> = notes
        .iter()
        .filter(|note| note.summary.contains("is not read from"))
        .map(|note| note.summary.as_str())
        .collect();
    assert_eq!(
        both,
        ["n is not read from 1 file, so the 2 rows there are left out of the filter and sort"],
        "one note for the column, not one for each of the two things naming it"
    );
}

/// The offer in the Notes tab, taken: the values a type conflict hid appear on screen.
#[test]
fn test_the_notes_tab_offers_to_read_a_conflicting_column_as_text() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // `n` as text here, so it is not read from this file at all.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.conflict),
        "the row whose file stores `n` as text is a conflict to begin with"
    );
    assert!(
        !before.contains("sixty"),
        "and its value cannot be seen: {before}"
    );

    // Open the Info panel and walk to the Notes tab.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // Walked by key rather than by setting the tab, so the keys the user presses are
    // the ones under test. Tab first: the panel opens on the body, where the arrows
    // move the schema table rather than the tab bar. The conflict note's own words say
    // when we have arrived.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }
    assert!(
        panel.contains("and not read there"),
        "the Notes tab, showing the conflict note: {panel}"
    );

    // Walk to the note that carries the offer, and take it.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("the conflict note offers to read the column as text");
    for _ in 0..at {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);
    assert!(
        panel.contains("Enter  read n as text"),
        "the panel says the offer is there: {panel}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let after = painted(&mut app, &rx, &tx, area);

    assert!(
        after.contains("sixty"),
        "the value the conflict hid is on screen: {after}"
    );
    assert!(
        after.contains("10") && after.contains("20"),
        "and so are the ones that were always readable: {after}"
    );
    assert!(
        !after.contains(g.conflict),
        "nothing conflicts any more: {after}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.read_as_text(),
        [polars::prelude::PlSmallStr::from("n")],
        "and the state says which column it is reading that way"
    );
    assert!(
        !state.notes().iter().any(|note| note.read_as_text.is_some()),
        "the offer is gone, having been taken: {:#?}",
        state.notes()
    );
}

/// The offer is not made where datui could not honour it.
///
/// Reading a column as text needs to know where each file's rows begin, and datui does
/// not for a dataset whose footers could not all be read — the same datasets that
/// cannot draw the marks. The note is still worth saying; the offer on it is not.
#[test]
fn test_no_offer_to_read_as_text_where_the_files_were_not_all_counted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );
    // A third file datui cannot read the footer of.
    let broken = dir.path().join("date=2024-01-03");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("data.parquet"), b"not a parquet file at all").unwrap();

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|note| note.summary.contains("not read there")),
        "the conflict is still worth saying: {notes:#?}"
    );
    assert!(
        notes.iter().all(|note| note.read_as_text.is_none()),
        "but datui cannot act on it, so it does not offer to: {notes:#?}"
    );
}

/// The offer and the count of notes out of view share the last row without landing on
/// top of each other.
///
/// Both are drawn into the panel's bottom row. A `Paragraph` leaves the cells its text
/// does not reach alone, so two of them in one place is not a layout that loses — it is
/// one string written over another.
#[test]
fn test_the_offer_and_the_hidden_count_do_not_overwrite_each_other() {
    let dir = tempfile::tempdir().unwrap();
    // Several drifting columns, so there are more notes than a short panel can show.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[0i64, 1],
            "measurement_value" => &[10i64, 20],
            "b" => &[1i64, 2],
            "c" => &[1i64, 2],
            "d" => &[1i64, 2],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64],
            "measurement_value" => &["sixty"],
            "b" => &["x"],
            "c" => &["x"],
            "d" => &["x"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // Narrow, and short enough that the notes do not all fit: both halves of the last
    // row have something to say, and not enough room to say it in.
    let area = Rect::new(0, 0, 44, 12);
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }

    // Walk to a note carrying the offer, so the panel has both things to say.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("a conflict note offers to read its column as text");
    for _ in 0..at {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);

    let count = (1..9)
        .flat_map(|n| [format!("{n} below"), format!("{n} above")])
        .find(|text| panel.contains(text.as_str()))
        .unwrap_or_else(|| panic!("the panel is short enough to be hiding notes: {panel}"));
    assert!(
        panel.contains("Enter  read"),
        "the offer shares the row with the count: {panel}"
    );
    // The blank column between them is the whole of it. Drawn into the same rect, the
    // count lands on the offer's last characters and there is no gap — the offer's
    // text runs straight into "2 below" with no way to tell where one ends.
    assert!(
        panel.contains(&format!(" {count}")),
        "the two must not run together where they meet: {panel}"
    );
}

/// A filter on a column read as text compares text, and the panel says so.
///
/// `n > 5` was written for a number. Read as text it keeps `"sixty"` and drops `"10"`,
/// which is a different question with the same words — so the note that arrives in
/// place of the conflict note is the one thing standing between the user and a view
/// they would read wrongly.
#[test]
fn test_reading_a_filtered_column_as_text_says_the_comparison_changed() {
    use datui::filter_modal::FilterOperator;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[1i64, 10, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Gt, "5")]);
    assert_eq!(current_rows(&app), 2, "10 and 20 are greater than 5");

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes.iter().any(
            |note| note.summary == "n is read as text, so a filter or sort on it compares text"
        ),
        "the filter means something else now, and the panel says so: {notes:#?}"
    );
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back, since the user has not been told yet"
    );
}

/// Reading a column as text does not undo the widening, so the note about it stays.
///
/// One file wrote `n` as an integer and another as a float, which widen together — so
/// the column is read as a float and the integer file's `7` shows as `7.0`, text read
/// or not. Only the types that *conflict* are read at their own type. The note that
/// explains the `7.0` is the widening note, and an earlier version of this deleted it.
#[test]
fn test_reading_as_text_keeps_the_note_about_a_widened_type() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[7i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64, 2], "n" => &[1.5f64, 2.5]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    let widening = "n is stored as more than one type";
    assert!(
        state
            .notes()
            .iter()
            .any(|n| n.summary.starts_with(widening)),
        "the integer and the float widened together to begin with"
    );
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let text: Vec<String> = state
        .lf
        .clone()
        .collect()
        .unwrap()
        .column("n")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|value| value.unwrap_or("null").to_string())
        .collect();
    assert_eq!(
        text,
        ["7.0", "1.5", "2.5", "sixty"],
        "the file that wrote 7 still reads 7.0: widening is not what the text read undoes"
    );
    let notes = state.notes();
    assert!(
        notes.iter().any(|n| n.summary.starts_with(widening)),
        "so the note explaining that 7.0 has to stay: {notes:#?}"
    );
}

/// A partition written for a day nothing happened holds no rows, and datui says so.
///
/// Worth saying because the dataset then has fewer days of data than it has folders,
/// and the file is invisible in every other way: it adds no rows, changes no schema,
/// and moves nothing on screen.
#[test]
fn test_a_partition_that_holds_no_rows_is_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // A day nothing happened: the folder is there, the file is there, the rows are not.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => Vec::<i64>::new(), "n" => Vec::<i64>::new()).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[2i64], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(current_rows(&app), 3, "the empty day adds nothing");
    let notes = state.notes();
    let empty = notes
        .iter()
        .find(|note| note.summary.contains("no rows"))
        .unwrap_or_else(|| panic!("nothing said about the empty day: {notes:#?}"));
    assert_eq!(empty.summary, "1 file holds no rows");
    assert_eq!(empty.scope, "in all 3 footers");
}

/// The control: every file holding rows says nothing.
#[test]
fn test_a_dataset_whose_files_all_hold_rows_says_nothing_about_empty_ones() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[10i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64], "n" => &[20i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.notes().iter().any(|n| n.summary.contains("no rows")),
        "{:#?}",
        state.notes()
    );
}

/// A pipeline that renamed its partition key partway through.
///
/// The note says the shape and claims nothing about what it costs. What it costs varies:
/// this folder does not open at all, because the scan reads its partition columns off
/// one branch of the tree and the files under the other key fail it — but which branch
/// wins is whatever the filesystem hands back first, so this asserts that *something*
/// went wrong rather than which key won. An earlier version asserted the key, passed
/// here and failed on CI.
#[test]
fn test_a_folder_whose_partition_key_changed_says_the_folders_differ() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    // The day the pipeline changed.
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        frame.contains("Schema field not found"),
        "a renamed key stops this folder opening, whichever key the scan took — the \
         note exists to explain a screen like this one:\n{frame}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("partition by the same keys"))
        .unwrap_or_else(|| panic!("nothing said about the changed key: {notes:#?}"));
    assert_eq!(
        layout.summary,
        "the folders do not all partition by the same keys: 3 files by date, 1 file by dt"
    );
    assert_eq!(layout.scope, "in the names of 4 files");
}

/// The very same disagreement, and this one opens.
///
/// What decides it is not which branch the scan reads by — it is which file name sorts
/// first. The paths are handed to Polars sorted and it takes the hive schema from the
/// first of them, so a `data.parquet` at the root (which sorts above both `date=` and
/// `dt=`) means no file's key is ever checked and the column comes back null. Name it
/// `loose.parquet` and the same folder will not open at all.
///
/// A byte sort of path strings, so this holds on any filesystem — and it is why the
/// note says the shape and not the cost: it cannot see a filename's spelling.
#[test]
fn test_folders_that_differ_may_still_open_and_the_note_claims_only_the_shape() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );
    // At the root, and named so that it sorts before both partition folders.
    write_parquet(
        dir.path(),
        "",
        df!("id" => &[9i64], "n" => &[9i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        !frame.contains("Error"),
        "the same disagreement as the test above, and this one opens: {frame}"
    );
    assert_eq!(current_rows(&app), 5, "every file is read");

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("partition by the same keys"))
        .unwrap_or_else(|| panic!("the folders still differ: {notes:#?}"));
    assert_eq!(
        layout.summary,
        "the folders do not all partition by the same keys: 3 files by date, 1 file by dt",
        "said of a dataset that opened, which is why it says nothing about cost"
    );
    assert_eq!(
        layout.scope, "in the names of 5 files",
        "the file at the root is one of the names read, though it is no layout"
    );
}

/// The control: a folder partitioned the one way opens, and says nothing about keys.
#[test]
fn test_a_folder_partitioned_the_one_way_says_nothing_about_its_keys() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(!frame.contains("Error"), "it opens: {frame}");

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("partition by the same keys")),
        "{:#?}",
        state.notes()
    );
}

/// The counter the loading screen reads is the one the real open writes to.
///
/// Every other test here drives the counter from one side: the render tests set it by
/// hand, the pass test calls the pass directly with a counter of its own. Neither says
/// the two are connected — with only those, pointing the open at the non-reporting
/// pass leaves the feature completely dead in the running app and the suite green.
#[test]
fn test_opening_a_folder_reports_its_footers_to_the_app() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    assert_eq!(
        app.footer_progress.last_pass().begun,
        1,
        "the open ran its footer pass against the app's own counter"
    );
    assert_eq!(
        app.footer_progress.last_pass().read,
        3,
        "and counted each of the three footers off it"
    );
    assert_eq!(
        app.footer_progress.reading(),
        None,
        "with nothing left on screen once they landed"
    );
}

/// A second open starts its own count rather than inheriting the first one's.
///
/// Abandoning a load cancels nothing — the footers keep being read — so a counter
/// shared across loads reports the abandoned folder's progress under the next file's
/// name, which is what a user opening a small CSV after a large folder would see.
#[test]
fn test_each_open_counts_its_own_footers() {
    let first = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(first.path(), day, df!("id" => &[0i64]).unwrap());
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-01-01",
        df!("id" => &[0i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let counter_of_the_first = app.footer_progress.clone();
    assert_eq!(counter_of_the_first.last_pass().read, 3);

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![second.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    assert!(
        !Arc::ptr_eq(&counter_of_the_first, &app.footer_progress),
        "the second open has a counter of its own"
    );
    assert_eq!(
        app.footer_progress.last_pass().read,
        1,
        "counting its one footer, not the three before it"
    );

    // And the behaviour that matters, not just the mechanism: the first open's pass
    // goes on running after it is abandoned, so if the second open shared its counter
    // the first's progress would paint onto the second's screen. Driven here, since
    // a real abandoned pass finishes too fast to catch.
    counter_of_the_first.begin(6541);
    for _ in 0..4102 {
        counter_of_the_first.advance();
    }
    let frame = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    assert!(
        !frame.contains("6,541"),
        "the abandoned folder's count does not appear under the file that replaced \
         it:\n{frame}"
    );
}

/// The accent is about the note being *new*: a sort that has something to say brings
/// it back after the panel has already been opened once.
#[test]
fn test_a_sort_that_leaves_rows_out_offers_its_note_afresh() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        !state.notes_unseen(),
        "the dataset's own notes have been offered"
    );

    state.sort(vec!["n".to_string()], true);
    assert!(
        state.notes_unseen(),
        "the note about the rows the sort left out has not been"
    );

    state.mark_notes_seen();
    state.sort(vec!["n".to_string()], false);
    assert!(
        !state.notes_unseen(),
        "and sorting the same column the other way says nothing new, so the accent \
         stays away"
    );
}

/// A conflicting file that is not the last one, several of them, and two stretches
/// that do not touch.
///
/// The last file is where a run's end and the end of the dataset are the same number,
/// so a dataset whose only conflict is there cannot tell a right implementation from
/// one that drops everything from the first conflict onwards.
#[test]
fn test_the_rows_left_out_are_the_conflicting_files_own_wherever_they_sit() {
    let dir = tempfile::tempdir().unwrap();
    // Read as an integer: six of the ten rows hold it that way.
    let int = |ids: &[i64], ns: &[i64]| df!("id" => ids, "n" => ns).unwrap();
    let text = |ids: &[i64], ns: &[&str]| df!("id" => ids, "n" => ns).unwrap();
    write_parquet(dir.path(), "date=2024-01-01", int(&[0, 1], &[0, 1]));
    write_parquet(dir.path(), "date=2024-01-02", text(&[2, 3], &["a", "b"]));
    write_parquet(dir.path(), "date=2024-01-03", text(&[4], &["c"]));
    write_parquet(dir.path(), "date=2024-01-04", int(&[5, 6], &[5, 6]));
    write_parquet(dir.path(), "date=2024-01-05", text(&[7], &["d"]));
    write_parquet(dir.path(), "date=2024-01-06", int(&[8, 9], &[8, 9]));

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error.is_none(), "the sort itself must succeed");

    let mut ids: Vec<i64> = state
        .lf
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![0, 1, 5, 6, 8, 9],
        "the two stretches that store `n` as text go, and nothing after them does"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 3 files, so the 4 rows there are left out of the sort"
    );
}

/// Clearing the sort brings the rows back and takes the note with it, and a sort on a
/// column the files agree on never took any rows to begin with.
#[test]
fn test_only_the_conflicting_column_costs_rows_and_only_while_it_is_sorted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], true);
    assert_eq!(
        current_rows(&app),
        5,
        "`id` is the same type everywhere, so a sort on it leaves nothing out"
    );
    let state = app.data_table_state.as_mut().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("is not read from")),
        "and says nothing about rows going"
    );

    state.sort(vec!["n".to_string()], true);
    assert_eq!(current_rows(&app), 3, "sorting by `n` leaves the two out");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(Vec::new(), true);
    assert_eq!(
        current_rows(&app),
        5,
        "and clearing the sort brings them back"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("is not read from")),
        "with nothing left saying they went: {:#?}",
        state.notes()
    );
}

/// The control for the test above: a folder whose files agree shows neither glyph, so
/// the assertions there are about the data and not about some other part of the screen.
#[test]
fn test_a_uniform_dataset_shows_no_absent_or_conflicting_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let text = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 20));
    assert!(text.contains(g.null), "the real null still shows");
    assert!(!text.contains(g.absent), "nothing is absent here");
    assert!(!text.contains(g.conflict), "nothing conflicts here");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.drifts(), "and the scan stamped no drift column");
}

/// The drift column is the state's own bookkeeping. It must not reach the schema, the
/// column order, or an export.
#[test]
fn test_the_hidden_drift_column_is_never_part_of_the_data() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.drifts(), "this dataset does drift");

    let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "extra"],
        "no hidden column in the schema"
    );
    assert!(
        !state.get_column_order().iter().any(|c| c.starts_with("__")),
        "nor in the column order"
    );

    // What actually reaches a file is what matters, so drive the real export rather
    // than the accessor the export is supposed to use.
    let out = dir.path().join("out.csv");
    let header = export_csv_header(&mut app, &rx, &tx, &out);
    assert_eq!(header, "date,id,extra", "nor in what an export writes");
}

/// Run a CSV export through the app's own two-phase export events and return the
/// header line of the file it wrote.
fn export_csv_header(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
) -> String {
    export_csv(app, rx, tx, path, false)
        .lines()
        .next()
        .expect("with a header")
        .to_string()
}

/// Run a CSV export through the app's own two-phase export events and return the whole
/// file. `source_file` asks it to name the file each row came from.
fn export_csv(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
    source_file: bool,
) -> String {
    let options = datui::ExportOptions {
        source_file,
        csv_delimiter: b',',
        csv_include_header: true,
        csv_compression: None,
        json_compression: None,
        ndjson_compression: None,
        parquet_compression: None,
    };
    let start = AppEvent::DoExportCollect(
        path.to_path_buf(),
        datui::export_modal::ExportFormat::Csv,
        options,
    );
    if let Some(next) = app.event(&start) {
        let _ = tx.send(next);
    }
    for _ in 0..200 {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if path.exists() && !app.is_busy() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    std::fs::read_to_string(path).expect("the export wrote a file")
}

/// A query builds its own rows, and its schema becomes the column order — so a query
/// root that still carried the hidden drift column turned it into one of the data's,
/// visible in the table and the sidebar.
#[test]
fn test_a_query_never_turns_the_drift_column_into_a_real_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );
    let expected = ["date", "id", "extra"];

    for (what, run) in [
        ("a fuzzy search", 0),
        ("a DSL query", 1),
        ("a SQL query", 2),
        ("a reset", 3),
    ] {
        let mut app = open_local_dataset(dir.path());
        let state = app.data_table_state.as_mut().unwrap();
        match run {
            0 => state.fuzzy_search("x".to_string()),
            1 => state.query("select where id > 0".to_string()),
            2 => state.sql_query("select * from df".to_string()),
            _ => state.reset(),
        }
        assert!(state.error.is_none(), "{what}: {:?}", state.error);
        state.collect();
        assert!(state.error.is_none(), "{what} collect: {:?}", state.error);

        let order: Vec<&str> = state
            .get_column_order()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(order, expected, "column order after {what}");
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, expected, "schema after {what}");
    }
}

/// A reset returns to the data as opened, so the cells that stand for a file the
/// column was never in read as absent again.
#[test]
fn test_a_reset_brings_back_the_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent at open"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    state.collect();
    assert!(state.error.is_none(), "the query: {:?}", state.error);
    assert!(
        !state.drifts(),
        "a query's rows stand for no file, so nulls are plain nulls"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.reset();
    state.collect();
    assert!(state.error.is_none(), "the reset: {:?}", state.error);
    assert!(state.drifts(), "and the reset puts the files back");
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "so the absent cells read as absent again"
    );
}

/// Counting a many-file scan's rows must not kill the app.
///
/// A dataset whose files disagree on a column's type is read as a union of scans, one
/// per run of files. `len()` is `UInt32`, and summing it across a union widens to
/// `UInt128`, which the streaming engine panics on rather than erroring — and datui
/// runs streaming by default. Anything that invalidates the row count (a filter, a
/// query, a reset) took the whole process down with it.
#[test]
fn test_counting_a_union_of_scans_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    // `n` is text in one file and a number in the other, so the two are scanned apart.
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

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    state.fuzzy_search("a".to_string());
    assert!(state.error.is_none(), "fuzzy search: {:?}", state.error);
    state.collect();
    assert!(
        state.error.is_none(),
        "collect after the search: {:?}",
        state.error
    );
}

/// Analysis counts the rows itself, which is the same `len()` over a union of scans
/// that crashed the table. Its panic is worse: it happens inside `spawn_blocking`,
/// where tokio swallows it, so the panel never finishes and the app wedges on
/// "Computing statistics…" with the panic text over the raw-mode screen.
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
    let results = datui::statistics::compute_statistics_with_options(
        &state.lf_clone(),
        None,
        0,
        datui::statistics::ComputeOptions {
            polars_streaming: true,
            ..Default::default()
        },
    )
    .expect("analysis runs over a many-file scan");
    assert_eq!(results.total_rows, 4, "and counts every row");
}

/// Opening a folder whose files disagree leaves something to say, and the Info key
/// carries a quiet accent until the panel has been opened.
#[test]
fn test_a_drifting_dataset_has_notes_and_offers_them_once() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert_eq!(notes.len(), 1, "one column is not in every file");
    assert_eq!(
        notes[0].summary,
        "extra is in 1 of 2 files; absent from the rest, not null"
    );
    assert_eq!(
        notes[0].scope, "in all 2 footers",
        "and says what it is based on"
    );
    assert!(state.notes_unseen(), "not offered yet");

    // Pressing i opens the panel, which is the offer being taken up.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.notes_unseen(), "the accent has done its job");
}

/// A query builds its own rows, so notes about the files behind the dataset no longer
/// describe what is on screen. They come back on a reset.
#[test]
fn test_a_query_puts_the_notes_away_and_a_reset_brings_them_back() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(state.notes().len(), 1, "the dataset has something to say");

    state.sql_query("select id from df".to_string());
    state.collect();
    assert!(state.error.is_none(), "the query: {:?}", state.error);
    assert!(
        state.notes().is_empty(),
        "a note about `extra` would describe a column the frame no longer has"
    );

    state.reset();
    state.collect();
    assert!(state.error.is_none(), "the reset: {:?}", state.error);
    assert_eq!(state.notes().len(), 1, "and the reset brings them back");
}

/// More notes than the panel is tall must not be dropped on the floor: the panel says
/// how many are out of view, and the cursor reaches them.
#[test]
fn test_notes_past_the_fold_are_counted_and_reachable() {
    let dir = tempfile::tempdir().unwrap();
    // Six columns, each arriving one day later, is six notes.
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"], "c" => &["x"],
            "d" => &["x"], "e" => &["x"], "f" => &["x"])
        .unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    assert_eq!(app.data_table_state.as_ref().unwrap().notes().len(), 6);

    // Open the panel, move focus to the tab bar, and walk to the Notes tab. The
    // dataset is partitioned, so Notes is the fourth.
    for key in [
        KeyCode::Char('i'),
        KeyCode::Tab,
        KeyCode::Right,
        KeyCode::Right,
        KeyCode::Right,
    ] {
        app.event(&AppEvent::Key(KeyEvent::new(key, KeyModifiers::NONE)));
    }
    // A short panel cannot show six notes at two lines each plus a gap.
    let area = Rect::new(0, 0, 100, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();

    assert!(
        screen.contains("3 below"),
        "three of the six notes fit whole, so three are out of view, got:\n{screen}"
    );
    assert!(
        screen.contains("a is in 1 of 2 files"),
        "the first note is shown"
    );
    assert!(
        screen.contains("in all 2 footers"),
        "and the scope of the note the cursor is on, got:\n{screen}"
    );

    // The cursor reaches the last note, which scrolls it into view.
    for _ in 0..6 {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('j'),
            KeyModifiers::NONE,
        )));
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("f is in 1 of 2 files"),
        "the last note is reachable, got:\n{screen}"
    );
    assert!(
        screen.contains("above"),
        "and the panel says what scrolled off the top, got:\n{screen}"
    );

    // Too short to hold a note is not the same as having none to hold: the panel
    // says which it is, and never claims there is nothing to say.
    for height in 4u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
        let drew_a_note = screen.contains("is in 1 of");
        let said_no_room = screen.contains("no room");
        assert!(
            drew_a_note ^ said_no_room,
            "a {height}-row panel draws a note or says it has no room for one, \
             exactly one of the two: {screen:?}"
        );
    }

    // A summary without its scope line under it is the misreading the scope line
    // exists to prevent, so no height may produce one.
    for height in 4u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            if !row.contains("is in 1 of 2 files") {
                continue;
            }
            // The line a note rests on follows its summary, and always before the
            // next note begins.
            let found = rows[i + 1..]
                .iter()
                .take_while(|later| !later.contains("is in 1 of 2 files"))
                .any(|later| later.contains("in all 2 footers"));
            assert!(
                found,
                "at height {height}, a note is drawn with no basis under it:\n{}",
                row.trim_end()
            );
        }
    }

    // A note needs its summary and its basis, so one row cannot hold one. Say that
    // rather than draw half a note.
    let area = Rect::new(0, 0, 100, 4);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("6 notes; no room for this one"),
        "too short for a whole note says so, got:\n{screen}"
    );
}

/// A panel exactly as tall as one note draws it, rather than reporting no room.
#[test]
fn test_a_note_that_fills_the_panel_is_drawn_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    for key in [
        KeyCode::Char('i'),
        KeyCode::Tab,
        KeyCode::Right,
        KeyCode::Right,
        KeyCode::Right,
    ] {
        app.event(&AppEvent::Key(KeyEvent::new(key, KeyModifiers::NONE)));
    }
    // Walk every height that can hold at least one note and its basis line.
    for height in 5u16..12 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
        assert!(
            screen.contains("is in 1 of 2 files"),
            "a {height}-row panel has room for a note, so it draws one: {screen:?}"
        );
        assert!(
            !screen.contains("no room"),
            "and does not claim otherwise: {screen:?}"
        );
    }
}

/// A folder whose files agree has nothing to say, and nothing to show for it.
#[test]
fn test_a_uniform_dataset_has_no_notes() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(dir.path(), "date=2024-01-02", df!("id" => &[2i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.notes().is_empty());
    assert!(!state.notes_unseen(), "so no accent either");
}

/// Asking an export to name each row's file keeps the absent-versus-null distinction
/// once the data has left datui: `extra` is empty in both rows, but only one of them
/// came from a file that had the column.
#[test]
fn test_an_export_can_name_the_file_each_row_came_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &[None::<&str>]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .can_name_source_files(),
        "the files disagree, so there is something to name"
    );

    let out = dir.path().join("named.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "date,id,extra,source_file");
    assert!(
        lines[1].ends_with("date=2024-01-01/data.parquet"),
        "the first row came from the file without `extra`: {}",
        lines[1]
    );
    assert!(
        lines[2].ends_with("date=2024-01-02/data.parquet"),
        "and the second from the one that has it, holding a real null: {}",
        lines[2]
    );

    // Off by default, and then the hidden index must not leak in its place.
    let plain = dir.path().join("plain.csv");
    let csv = export_csv(&mut app, &rx, &tx, &plain, false);
    assert_eq!(csv.lines().next().unwrap(), "date,id,extra");
}

/// A dataset may already have a column called `source_file` — a folder of per-file
/// extracts is exactly this feature's audience — and adding one by that name would
/// replace it, silently, in the file the user takes away.
#[test]
fn test_naming_source_files_never_overwrites_a_column_of_that_name() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "source_file" => &["mine-A"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "source_file" => &["mine-B"], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let out = dir.path().join("collide.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(
        lines[0], "date,id,source_file,extra,source_file_1",
        "the dataset keeps its own column and datui's goes on the end under another name"
    );
    assert!(
        lines[1].contains("mine-A"),
        "the dataset's own values survive: {}",
        lines[1]
    );
    assert!(lines[2].contains("mine-B"), "both of them: {}", lines[2]);
}

/// Asking for source files on a frame that no longer has them must not leak datui's
/// bookkeeping instead.
///
/// This exercises the path where the option is on but the dataset cannot honour it, so
/// the export never collects the index at all. The other path — collected with the
/// index, then unable to name it — is guarded by `drop_row_index`, which is unit
/// tested; it needs the dataset to change between the collect being spawned and its
/// result arriving, which keys held while busy make unreachable today.
#[test]
fn test_asking_to_name_files_on_a_query_result_leaks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // A query replaces the frame, so the rows no longer stand for rows of a file and
    // naming them is refused — but the export still runs.
    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    state.collect();
    assert!(!state.can_name_source_files(), "nothing to name any more");

    let out = dir.path().join("refused.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let header = csv.lines().next().unwrap();
    assert!(
        !header.contains("__datui_row"),
        "datui's own bookkeeping must not reach the file: {header}"
    );
}

/// The Options panel must read as a panel at every format: no empty box, and the
/// source-file checkbox under the format's own options rather than adrift at the foot.
#[test]
fn test_the_export_options_panel_reads_as_one_for_every_format() {
    use datui::export_modal::ExportFormat;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    // The modal opens with focus on the path, where Down does not change the format.
    for key in [KeyCode::Char('e'), KeyCode::BackTab] {
        app.event(&AppEvent::Key(KeyEvent::new(key, KeyModifiers::NONE)));
    }
    assert!(app.export_modal.offer_source_file, "the files disagree");
    assert_eq!(
        app.export_modal.focus,
        datui::export_modal::ExportFocus::FormatSelector,
        "so the walk below really does change format"
    );

    let area = Rect::new(0, 0, 120, 30);
    let mut seen = Vec::new();
    let mut wrong = Vec::new();
    for _ in 0..ExportFormat::ALL.len() {
        let format = app.export_modal.selected_format;
        seen.push(format);
        // The last row each format draws of its own. The checkbox goes directly under
        // it, so asking for this row by name pins the row count in
        // `render_format_options`: count too low and the format's last row is
        // truncated away, too high and a blank row opens up. Either way this row is
        // no longer the one above the checkbox.
        let last_of_its_own = match format {
            // Both end on the second compression row.
            ExportFormat::Csv | ExportFormat::Json | ExportFormat::Ndjson => "XZ",
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {
                "No options specific to"
            }
        };

        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        match rows.iter().position(|r| r.contains("Source file:")) {
            None => wrong.push(format!("{format:?}: no Source file row at all")),
            Some(checkbox) if !rows[checkbox - 1].contains(last_of_its_own) => wrong.push(format!(
                "{format:?}: the row above the checkbox should be the one holding \
                 {last_of_its_own:?}, and is {:?}",
                rows[checkbox - 1].trim_end()
            )),
            Some(_) => {}
        }
        // Move to the next format.
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    // Collected rather than asserted in the loop: the formats fail in families, and
    // one report naming every bad format beats six runs that each name the first.
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(
        seen,
        ExportFormat::ALL.to_vec(),
        "the walk must visit every format once, in order"
    );
}

/// A column only a middle file has used to vanish: the schema was one file's, and that
/// file did not have it.
#[test]
fn test_a_column_only_one_local_file_has_is_shown() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-03-01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-02",
        df!("id" => &[3i64], "oops" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-03",
        df!("id" => &[4i64, 5]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "oops"],
        "the partition key, then every column any file has"
    );
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.origin.to_string(), "all 3 footers");
    let drifting: Vec<String> = dataset.drifting().map(|c| c.name.to_string()).collect();
    assert_eq!(drifting, ["oops"]);
}

/// Files written with different integer widths used to fail the strict local scan.
#[test]
fn test_local_files_of_different_integer_widths_open_as_the_wider_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("n" => &[1i32, 2]).unwrap(),
    );
    write_parquet(dir.path(), "date=2024-01-02", df!("n" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.schema.get("n"),
        Some(&polars::prelude::DataType::Int64)
    );
}

/// One unreadable file must not stop the rest of the folder from opening.
#[test]
fn test_one_unreadable_local_file_does_not_stop_the_open() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    let bad = dir.path().join("date=2024-01-02");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("data.parquet"), b"not a parquet file").unwrap();
    write_parquet(dir.path(), "date=2024-01-03", df!("id" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["date", "id"]);
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
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
/// that counter also gates analysis and export results, which keep running. The keys
/// typed at the frozen screen were meant for the load and go with it.
#[test]
fn test_entering_home_clears_load_state_but_not_task_generation() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    let mut pump = EventPump::new(app, tx, rx);
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    pump.drain().unwrap();
    assert!(pump.app.is_busy(), "a load in flight should be busy");
    for code in [KeyCode::Char('j'), KeyCode::Enter] {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    }
    assert_eq!(
        pump.held_keys().count(),
        2,
        "keys typed at a load are held, not dropped"
    );

    let generation_before = pump.app.task_generation();
    pump.terminal_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
        .unwrap();

    assert_eq!(pump.app.input_mode, InputMode::Home);
    assert!(
        !pump.app.is_busy(),
        "abandoning should clear the load's busy flag"
    );
    assert_eq!(
        pump.held_keys().count(),
        0,
        "keys typed at the frozen screen were meant for the load"
    );
    assert_eq!(
        pump.app.task_generation(),
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
        PlRefPath::try_from_path(dir.path()).unwrap(),
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
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_query()
            .is_empty()
    );
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
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_filters()
            .is_empty()
    );

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
    use datui::ParseStringsTarget;
    use datui::filter_modal::FilterOperator;
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

/// SQL after a pivot sees the pivoted columns: the reshape is the root the query runs
/// against, so `SELECT` of a pivoted column works and the reshape stays in the view.
#[test]
fn test_sql_after_pivot_sees_the_pivoted_columns() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_then_sql.csv", &csv, OpenOptions::default());

    app.event(&AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 10);
    assert!(app.data_table_state.as_ref().unwrap().schema.contains("k1"));

    app.event(&AppEvent::SqlSearch(
        "SELECT id, k2 FROM df WHERE k1 > 4".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 5, "ids 5..9");
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names, vec!["id", "k2"]);
}

/// While drilled into a group, a sidebar filter or sort applies within the group and
/// leaves the drill-down in place; drilling back up restores the grouped view.
#[test]
fn test_sidebar_filter_and_sort_stay_inside_a_drill_down() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_down_filter.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 3, "one row per group");

    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_eq!(current_rows(&app), 34, "c == 0: 0, 3, ..., 99");

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "a",
        FilterOperator::Lt,
        "30",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    assert!(
        state.is_drilled_down(),
        "the filter must not undo the drill-down"
    );
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Sort(vec!["a".to_string()], false));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(27));

    app.data_table_state.as_mut().unwrap().drill_up().unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert!(
        state.get_filters().is_empty(),
        "the group's filter stays with the group"
    );
    assert_eq!(current_rows(&app), 3);
}

/// A fuzzy search after a DSL query that renamed columns works on the data as loaded
/// and installs that schema, so a sidebar sort afterwards finds its columns.
#[test]
fn test_fuzzy_after_an_aliasing_query_then_sort_has_no_error() {
    let (mut app, rx, tx) = open_query_filter_fixture("alias_then_fuzzy.csv");

    app.event(&AppEvent::Search("select a, label: name".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema
            .iter_names()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        vec!["a", "label"]
    );

    app.event(&AppEvent::FuzzySearch("alpha".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    assert_eq!(current_rows(&app), 50);
    assert!(state.schema.contains("name") && state.schema.contains("c"));
    assert_eq!(state.headers(), vec!["a", "c", "name"]);

    app.event(&AppEvent::Sort(vec!["a".to_string()], false));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 50);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(98));
}

/// A DSL query after a pivot shows the loaded columns again, so SQL afterwards must run
/// against the loaded data, not against a pivot the user no longer sees.
#[test]
fn test_query_after_pivot_drops_the_reshape_for_sql() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_query_sql.csv", &csv, OpenOptions::default());

    app.event(&AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().schema.contains("k1"));

    app.event(&AppEvent::Search("select id, key".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 20);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .last_pivot_spec()
            .is_none()
    );

    app.event(&AppEvent::SqlSearch("SELECT * FROM df".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    let names: Vec<String> = state.schema.iter_names().map(|s| s.to_string()).collect();
    assert_eq!(names, vec!["id", "key", "val"], "the unpivoted columns");
    assert_eq!(current_rows(&app), 20);
}

/// Drilling into a group swaps the applied filters and sort for the group's; the Sort &
/// Filter sidebar must follow, or Apply would re-send the grouped view's filter against
/// a List column. Drilling back up brings the grouped view's settings back.
#[test]
fn test_drill_down_resyncs_the_sort_filter_sidebar() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sidebar.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let statement = filter_stmt("c", FilterOperator::Gt, "0");
    app.event(&AppEvent::Filter(vec![statement.clone()]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Sort(vec!["c".to_string()], false));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 2, "groups c = 1 and c = 2");
    // What Apply would have left in the sidebar.
    app.sort_filter_modal.filter.statements = vec![statement];
    app.sort_filter_modal.sort.columns = vec![datui::sort_modal::SortColumn {
        name: "c".to_string(),
        sort_order: Some(0),
        display_order: 0,
        is_locked: false,
        is_to_be_locked: false,
        is_visible: true,
    }];

    // Enter on the highlighted group row drills in.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert!(
        app.sort_filter_modal.filter.statements.is_empty(),
        "no filter applies inside the group yet"
    );
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.sort_order.is_none())
    );
    assert_eq!(
        app.sort_filter_modal.filter.available_columns,
        app.data_table_state.as_ref().unwrap().headers()
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    let statements = &app.sort_filter_modal.filter.statements;
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].column, "c");
    assert_eq!(statements[0].value, "0");
    let sorted: Vec<&str> = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .filter(|c| c.sort_order.is_some())
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(sorted, vec!["c"]);
    assert!(!app.sort_filter_modal.sort.ascending);
}

/// A template saved while drilled into a group describes the grouped view, which is what
/// it will reproduce: the getters return the grouped view's filters and sort, while the
/// view getters describe the frame on screen.
#[test]
fn test_template_getters_describe_the_grouped_view_while_drilled() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_template_getters.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Gt,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Sort(vec!["c".to_string()], false));
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

/// SQL inside a drill-down runs on the group, like the sidebar does, not on the whole
/// loaded table.
#[test]
fn test_sql_inside_a_drill_down_stays_in_the_group() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sql.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error.is_none(), "{:?}", state.error);
    assert_eq!(
        current_rows(&app),
        10,
        "a in 0, 3, ..., 27: within the group"
    );
}

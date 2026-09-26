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

/// As `pump_open_until_loaded`, but hands back the message a failed open ended with.
///
/// A load that fails reports it as `BackgroundError` — the reading happens off the
/// event thread — and only the paths that never get that far crash outright.
fn pump_open_until_error(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) -> Option<String> {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        match next.take() {
            Some(AppEvent::Crash(message)) => return Some(message),
            Some(AppEvent::BackgroundError { message, .. }) => return Some(message),
            Some(ev) => next = app.event(&ev),
            None => match rx.recv_timeout(std::time::Duration::from_millis(5000)) {
                Ok(ev) => next = Some(ev),
                Err(_) => return None,
            },
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
    use datui::data_quality::{DataQualityResults, QualityPrecision};
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

    app.analysis_modal.data_quality_results = None;
    app.event(&AppEvent::BackgroundDataQualityReady {
        generation: stale_gen,
        results: DataQualityResults {
            total_rows: Some(999_999),
            evaluated_rows: 1,
            precision: QualityPrecision::Sampled,
            sample_seed: 1,
            columns: vec![],
            observations: vec![],
            segments: vec![],
            temporal: vec![],
            identity: None,
            category_variants: vec![],
        },
    });
    assert!(
        app.analysis_modal.data_quality_results.is_none(),
        "stale BackgroundDataQualityReady should not write data-quality results"
    );
}

#[test]
fn test_data_quality_plan_runs_in_background_and_opens_overview() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityPage;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Plan);
    assert!(app.analysis_modal.data_quality_results.is_none());
    app.analysis_modal.data_quality_plan.sample_seed = 7_119;

    app.analysis_modal.focus = AnalysisFocus::Main;
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);

    assert!(app.analysis_modal.data_quality_results.is_some());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    assert!(!app.is_busy());

    app.analysis_modal
        .data_quality_results
        .as_mut()
        .unwrap()
        .observations
        .push(datui::data_quality::QualityObservation {
            kind: datui::data_quality::ObservationKind::Nulls,
            column: "example".to_string(),
            affected_rows: 1,
            evaluated_rows: 10,
            fact: "1 null row".to_string(),
            normalized_category: None,
            files: Vec::new(),
        });
    app.analysis_modal.data_quality_table_state.select(Some(0));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_observation_detail);
    let area = Rect::new(0, 0, 80, 24);
    let mut detail_buffer = Buffer::empty(area);
    app.render(area, &mut detail_buffer);
    assert!(
        detail_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("OBSERVATION")
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.data_quality_observation_detail);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);

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
    ] {
        app.analysis_modal.set_quality_page(page);
        for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 50, 18)] {
            let mut buffer = Buffer::empty(area);
            app.render(area, &mut buffer);
            let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
            assert!(screen.contains("Data Quality"));
            if page != QualityPage::Trends {
                assert!(screen.contains("Null"));
            }
        }
    }

    // The largest measured move between segments is on the screen, not only in the
    // profile: #196 asks for it and nothing read it before.
    app.analysis_modal.set_quality_page(QualityPage::Segments);
    let wide = Rect::new(0, 0, 160, 40);
    let mut buffer = Buffer::empty(wide);
    app.render(wide, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("Largest change"),
        "Segments should name the column and measurement that moved"
    );

    // Every remaining page and popup must say its own piece at each width, so a
    // clipped label or a screen that renders nothing at all fails here.
    for (page, expected) in [
        (QualityPage::Plan, "Latency threshold"),
        (QualityPage::Scope, "ELIGIBLE ROWS"),
        (QualityPage::TimeRoles, "Semantic role"),
        (QualityPage::Detail, "Provenance:"),
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

    // Once a run exists the sidebar reports what it measured, not a second copy
    // of the planned access already on the plan strip.
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(screen.contains("MEASURED"), "sidebar should report the run");
    assert!(screen.contains("eligible"));

    app.analysis_modal.set_quality_page(QualityPage::Plan);
    for (popup, expected) in [
        ("access", "Estimate basis"),
        // The extra reads a full scan makes for the values a type conflict hides are
        // promised before anything runs, like every other read on this page.
        ("access", "Conflict values"),
        ("confirm", "remote writes are 0 B."),
    ] {
        app.analysis_modal.data_quality_show_access = popup == "access";
        app.analysis_modal.data_quality_confirm_run = popup == "confirm";
        let area = Rect::new(0, 0, 120, 32);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(screen.contains(expected), "{popup} popup should not clip");
    }
    app.analysis_modal.data_quality_show_access = false;
    app.analysis_modal.data_quality_confirm_run = false;

    app.analysis_modal.set_quality_page(QualityPage::Segments);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('b'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_plan.comparison,
        datui::data_quality::QualityComparison::Baseline
    );
    assert!(
        app.analysis_modal
            .data_quality_plan
            .baseline_segment
            .is_some()
    );
    assert!(!app.is_busy());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('m'),
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char(']'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_metric,
        datui::data_quality::QualityMetric::EmptyRate
    );
    assert_eq!(app.analysis_modal.data_quality_column_index, 1);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('4'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    assert_eq!(app.analysis_modal.data_quality_column_index, 1);

    // Enter on a highlighted column must open that column, not the first one.
    app.analysis_modal.set_quality_page(QualityPage::Columns);
    app.analysis_modal.data_quality_table_state.select(Some(3));
    let fourth = app
        .analysis_modal
        .data_quality_results
        .as_ref()
        .unwrap()
        .columns[3]
        .name
        .clone();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Detail);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains(&fourth),
        "Detail should open the highlighted column {fourth}"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Columns);
    assert_eq!(
        app.analysis_modal.data_quality_table_state.selected(),
        Some(3),
        "returning from Detail should land back on the same column"
    );

    app.analysis_modal.close();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(app.analysis_modal.data_quality_plan.sample_seed, 7_119);
    assert!(app.analysis_modal.data_quality_results.is_some());
    assert!(!app.is_busy());

    // A drift observation's detail is the files themselves: which ones, how many rows
    // each cost the column, the type each holds, and the values the conflict hid.
    {
        let results = app.analysis_modal.data_quality_results.as_mut().unwrap();
        results.observations = vec![datui::data_quality::QualityObservation {
            kind: datui::data_quality::ObservationKind::TypeConflict,
            column: "fee".to_string(),
            affected_rows: 2,
            evaluated_rows: 7,
            fact: "1 of 3 files holds a type the scan cannot read".to_string(),
            normalized_category: None,
            files: vec![datui::data_quality::QualityFileEvidence {
                number: 2,
                name: "b.parquet".to_string(),
                rows: 2,
                stored_type: Some("str".to_string()),
                examples: vec!["sixty".to_string()],
            }],
        }];
        app.analysis_modal.set_quality_page(QualityPage::Overview);
        app.analysis_modal.data_quality_table_state.select(Some(0));
        app.analysis_modal.data_quality_observation_detail = true;
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
        app.analysis_modal.data_quality_observation_detail = false;
    }

    let column = app
        .data_table_state
        .as_ref()
        .unwrap()
        .schema
        .iter_names()
        .next()
        .unwrap()
        .to_string();
    let original_view = app.data_table_state.as_ref().unwrap().len_generation();
    let results = app.analysis_modal.data_quality_results.as_mut().unwrap();
    results.precision = datui::data_quality::QualityPrecision::Exact;
    results.observations = vec![datui::data_quality::QualityObservation {
        kind: datui::data_quality::ObservationKind::Nulls,
        column,
        affected_rows: 0,
        evaluated_rows: results.evaluated_rows,
        fact: "matching rows".to_string(),
        normalized_category: None,
        files: Vec::new(),
    }];
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_observation_detail);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.active);
    let area = Rect::new(0, 0, 80, 24);
    let mut evidence_buffer = Buffer::empty(area);
    app.render(area, &mut evidence_buffer);
    assert!(
        evidence_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("Esc back to result")
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.active);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.active);
    assert!(app.analysis_modal.data_quality_observation_detail);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().len_generation(),
        original_view
    );

    app.analysis_modal.close();
    let state = app.data_table_state.as_mut().unwrap();
    state.defer_collect = true;
    state.reverse();
    state.defer_collect = false;
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.data_quality_from_cache);
    assert!(app.analysis_modal.data_quality_results.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Plan);
}

#[test]
fn test_data_quality_scope_editor_runs_selected_view_rows() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::{QualityPage, QualityScope};

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
        |app: &mut App, code| app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    key(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    key(&mut app, KeyCode::Char('e'));
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Scope);
    for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 50, 18)] {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(screen.contains("ELIGIBLE ROWS"));
    }
    app.analysis_modal
        .data_quality_scope_input
        .set_value("rows 0..3");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Scope);
    assert!(app.analysis_modal.data_quality_scope_error.is_some());
    app.analysis_modal
        .data_quality_scope_input
        .set_value("rows 2..3");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Plan);
    assert_eq!(
        app.analysis_modal.data_quality_plan.scope,
        QualityScope::ViewRows { start: 2, end: 3 }
    );
    assert!(!app.analysis_modal.data_quality_editing);
    let next = key(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal
            .data_quality_results
            .as_ref()
            .unwrap()
            .total_rows,
        Some(2)
    );
    key(&mut app, KeyCode::Char('e'));
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Right);
    assert_ne!(
        app.analysis_modal.data_quality_plan.grain,
        datui::data_quality::QualityGrain::Dataset
    );
    key(&mut app, KeyCode::Char('1'));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Plan);
    key(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.data_quality_plan.grain,
        datui::data_quality::QualityGrain::Dataset
    );
    assert!(app.analysis_modal.data_quality_results.is_some());
    key(&mut app, KeyCode::Char('e'));
    key(&mut app, KeyCode::Enter);
    app.analysis_modal
        .data_quality_scope_input
        .set_value("rows 1..1");
    key(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.data_quality_results.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Plan);

    key(&mut app, KeyCode::Char('e'));
    key(&mut app, KeyCode::Enter);
    app.analysis_modal
        .data_quality_scope_input
        .set_value("rows 2..3");
    key(&mut app, KeyCode::Enter);
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
}

#[test]
fn test_data_quality_source_file_scope_uses_loaded_file_order() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityScope;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "region=one", df!("id" => &[1i32, 2]).unwrap());
    write_parquet(dir.path(), "region=two", df!("id" => &[3i32, 4]).unwrap());
    let (mut app, rx, _) = open_local_dataset_with_channel(dir.path());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    app.analysis_modal.data_quality_plan.scope = QualityScope::SourceFiles(vec![2]);
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.total_rows, Some(2));
    assert_eq!(results.evaluated_rows, 2);
    let id = results
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(id.min.as_deref(), Some("3"));
    assert_eq!(id.max.as_deref(), Some("4"));

    app.analysis_modal.data_quality_plan.scope = QualityScope::WholeSource;
    app.analysis_modal.data_quality_plan.sample_rows = 1;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let sampled = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(sampled.total_rows, None);
    assert_eq!(sampled.evaluated_rows, 1);
    assert_eq!(
        sampled.precision,
        datui::data_quality::QualityPrecision::Sampled
    );

    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Full;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let full = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(full.total_rows, Some(4));
    assert_eq!(full.evaluated_rows, 4);

    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Sample;
    app.analysis_modal.data_quality_plan.grain = datui::data_quality::QualityGrain::File;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let by_file = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(by_file.total_rows, Some(4));
    assert_eq!(by_file.evaluated_rows, 2);
    assert_eq!(by_file.segments.len(), 2);
    assert!(
        by_file
            .segments
            .iter()
            .all(|segment| segment.total_rows == Some(2) && segment.evaluated_rows == 1)
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

/// The accent reaches the bar from the dataset, and the config can turn it off.
///
/// `controls.rs` proves the accent is only a colour on the Info chip, but it is handed
/// a flag by hand; the app-side tests read `notes_unseen()`, an accessor. Nothing
/// joined the two, so an accent that never reached the bar — or one that ignored the
/// config — passed both.
#[test]
fn test_the_notes_accent_reaches_the_control_bar_and_the_config_can_stop_it() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let area = Rect::new(0, 0, 120, 24);
    let bar_of = |app: &mut App| -> (Vec<ratatui::style::Color>, String) {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let row = area.height - 1;
        (
            (0..area.width).map(|x| buf[(x, row)].fg).collect(),
            (0..area.width)
                .map(|x| buf[(x, row)].symbol().to_string())
                .collect(),
        )
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        app.data_table_state.as_ref().unwrap().notes_unseen(),
        "the folders disagree, so there is a note and it has not been read"
    );
    let (accented, accented_text) = bar_of(&mut app);

    // Opening the panel clears it, and the bar goes back to its ordinary colours.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let (plain, plain_text) = bar_of(&mut app);
    let accented_cells: Vec<usize> = (0..plain.len())
        .filter(|x| accented[*x] != plain[*x])
        .collect();
    assert!(
        !accented_cells.is_empty(),
        "the accent was on the bar, and reading the notes took it off"
    );
    // And it is the Info key that carries it, not the row count changing width or a
    // status message appearing — either of which would also colour some cells.
    let accented_word: String = accented_cells
        .iter()
        .map(|x| accented_text.chars().nth(*x).unwrap_or(' '))
        .collect();
    assert!(
        accented_word.contains("Info"),
        "the cells that changed spell the Info key: {accented_word:?}"
    );
    assert_eq!(
        accented_text, plain_text,
        "and the accent is only a colour — the bar says the same thing either way"
    );

    // And a user who does not want it never sees it, however many notes there are.
    let mut config = datui::config::AppConfig::default();
    config.display.notes_accent = false;
    let (tx2, rx2) = mpsc::channel();
    let mut off = App::new_with_config(
        tx2.clone(),
        common::test_runtime(),
        datui::config::Theme::from_config(&datui::config::AppConfig::default().theme)
            .expect("the default theme"),
        config,
    );
    pump_open_until_loaded(
        &mut off,
        &rx2,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut off, &rx2, &tx2, area);
    assert!(
        off.data_table_state.as_ref().unwrap().notes_unseen(),
        "there is still a note to accent"
    );
    let (unaccented, unaccented_text) = bar_of(&mut off);
    assert_eq!(
        unaccented_text, plain_text,
        "the same bar, so the colours below are comparable"
    );
    let still_accented: Vec<usize> = accented_cells
        .iter()
        .copied()
        .filter(|x| unaccented[*x] != plain[*x])
        .collect();
    assert!(
        still_accented.is_empty(),
        "and the cells that carry the accent are the ordinary colour at {still_accented:?}"
    );
}

/// All three kinds of empty still read right after a filter.
///
/// The sort case above is the other half of the same criterion. A filter is the one
/// that rebuilds the frame rather than reordering it, and it is the one no test looked
/// at on screen: the row → file mapping has to survive a predicate, not just a reorder.
/// The conflicting column is here rather than in the sort fixture because a filter that
/// does not name it must keep its rows — leaving them out is for a filter that does.
#[test]
fn test_absent_null_and_conflicting_cells_still_differ_after_a_filter() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // Three rows against two, so the majority type for `price` is the number and the
    // text file's rows are the ones left unread.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[1i64, 4, 5],
            "note" => &[None::<&str>, None, None],
            "price" => &[10i64, 40, 50],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64, 3],
            "note" => &["hi", "yo"],
            "extra" => &["x", "y"],
            "price" => &["cheap", "dear"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 20);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.absent) && before.contains(g.null) && before.contains(g.conflict),
        "all three before the filter: {before}"
    );

    // On `id`, which both files hold as the same type — so nothing is left out for
    // being unreadable, and rows from both files survive.
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt(
        "id",
        datui::filter_modal::FilterOperator::GtEq,
        "2",
    )]);
    assert!(state.error.is_none(), "the filter itself must succeed");
    // And a filter actually happened: without this the test passes when the predicate
    // is dropped on the floor, because an unfiltered screen carries all three glyphs
    // too. The criterion is about surviving a predicate, so there has to be one.
    assert_eq!(current_rows(&app), 4, "1 is gone; 2, 3, 4 and 5 are left");

    let after = painted(&mut app, &rx, &tx, area);
    assert!(
        after.contains(g.absent),
        "the rows of the file without `extra` still say absent: {after}"
    );
    assert!(
        after.contains(g.null),
        "and the real nulls are still nulls: {after}"
    );
    assert!(
        after.contains(g.conflict),
        "and the file that holds `price` as text still says so: {after}"
    );
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

/// A column the feed started sending is named by where it starts, not only by a count.
///
/// "in 2 of 3 files" says a column is unusual; "none before date=2024-01-02" says when
/// it began, which for a field added to a feed is the whole question. Through a real
/// folder rather than a hand-built footer list, because the partition it names comes
/// from the file's own path.
#[test]
fn test_a_column_that_starts_partway_through_says_where_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "fee" => &[10i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "fee" => &[30i64]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about_fee = notes
        .iter()
        .find(|note| note.summary.starts_with("fee is in"))
        .unwrap_or_else(|| panic!("no note about `fee`: {notes:#?}"));
    assert_eq!(
        about_fee.summary,
        "fee is in 2 of 3 files, none before date=2024-01-02; absent from the rest, \
         not null"
    );
}

/// And a column that belongs to one partition is named by that partition.
#[test]
fn test_a_column_only_one_partition_has_says_which() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-03-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-03-02",
        df!("id" => &[2i64], "oops" => &["x"]).unwrap(),
    );
    write_parquet(dir.path(), "date=2024-03-03", df!("id" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about_oops = notes
        .iter()
        .find(|note| note.summary.starts_with("oops is in"))
        .unwrap_or_else(|| panic!("no note about `oops`: {notes:#?}"));
    assert_eq!(
        about_oops.summary,
        "oops is in 1 of 3 files, only date=2024-03-02; absent from the rest, \
         not null"
    );
}

/// Files in the folder that are not Parquet are counted, so a silent drop is not one.
///
/// A `.csv` sitting in a folder of Parquet is a file somebody thought was in the table.
/// datui reads none of it and, until now, said nothing at all about it — which is the
/// shape of problem this whole issue is about.
#[test]
fn test_files_that_are_not_parquet_are_counted_rather_than_dropped_in_silence() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(
        dir.path().join("date=2024-01-01/extra.csv"),
        "id
1
",
    )
    .unwrap();
    std::fs::write(dir.path().join("notes.txt"), "read me").unwrap();
    // A writer's bookkeeping, which is not a file anyone meant as data.
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let skipped = notes
        .iter()
        .find(|note| note.summary.contains("not Parquet"))
        .unwrap_or_else(|| panic!("no note about the files that were not read: {notes:#?}"));
    assert_eq!(
        skipped.summary,
        "in the folder, 2 files are not Parquet, 1 file a writer left behind"
    );
    assert_eq!(skipped.scope, "in this folder's listing");
}

/// And a folder holding only what a writer leaves behind says nothing.
///
/// `_SUCCESS` beside the data is a job reporting that it finished. A note about it on
/// every folder any job ever wrote would put an accent on the Info key for the most
/// ordinary thing a folder can contain.
#[test]
fn test_a_writers_own_bookkeeping_is_not_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();
    std::fs::write(dir.path().join("date=2024-01-01/.data.parquet.crc"), "").unwrap();
    // A table format's own log. Everything in here belongs to the writer, whatever it
    // is called — a folder of three hundred commits is six hundred files, and counting
    // them as somebody's mistake would put an accent on the Info key for the most
    // ordinary thing a folder of Parquet can be.
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    for commit in 0..5 {
        std::fs::write(
            dir.path().join(format!("_delta_log/{commit:020}.json")),
            "{}",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join(format!("_delta_log/.{commit:020}.json.crc")),
            "",
        )
        .unwrap();
    }

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("not Parquet")),
        "nothing to say: {:#?}",
        state.notes()
    );
}

/// A table format's own Parquet is not the table's rows.
///
/// Delta writes its checkpoints as Parquet inside `_delta_log/`, with the table's own
/// columns among its own. Read as data they are extra rows in a table that does not
/// have them and columns nobody asked for — a wrong row count, silently. This is about
/// what the table *contains*, not about what a note says.
#[test]
fn test_a_table_formats_own_parquet_is_not_part_of_the_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    // A checkpoint, which is Parquet and is not the table.
    write_parquet(
        dir.path(),
        "_delta_log",
        df!("id" => &[99i64], "txn" => &["commit"]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id"],
        "the checkpoint's own columns are not the table's"
    );
    let ids: Vec<i64> = state
        .lf
        .clone()
        .collect()
        .expect("the table reads")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, [1], "and its rows are not the table's rows");
}

/// An object with nothing in it, whose name said it was data, is a write that stopped.
///
/// The note leads with it because it is the one skip that is a fault rather than a
/// tidy-up — and because nothing else can see it: a file that holds no bytes has no
/// footer to fail to read.
#[test]
fn test_a_write_that_stopped_is_said_to_have_stopped() {
    use datui::schema_union::SkippedFiles;
    let note = datui::notes::from_dataset(
        &datui::schema_union::union_file_schemas(
            &[],
            datui::schema_union::SchemaOrigin::AllFooters(0),
        )
        .with_skipped(SkippedFiles {
            bookkeeping: 2,
            not_parquet: 1,
            empty: 1,
        }),
    );
    let said: Vec<&str> = note.iter().map(|n| n.summary.as_str()).collect();
    assert_eq!(
        said,
        [
            "in the folder, 1 file is empty and was not read, 1 file is not Parquet, \
          2 files a writer left behind"
        ],
        "the stopped write first, then the mistake, then the tidy-up"
    );
}

/// The same rule on disk as in a bucket: a folder with no data in it is nobody's table.
///
/// The cloud half of this has a test; the local half had none, and a mutant that made
/// the rule never fire survived the whole suite. Iceberg keeps its log in a plain
/// `metadata/` — no underscore, no dot — so the name convention alone reads a table's
/// own files as somebody's mistakes.
#[test]
fn test_a_local_folder_with_no_data_in_it_is_nobodys_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "data/date=1", df!("id" => &[1i64]).unwrap());
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    for name in ["v1.metadata.json", "v2.metadata.json", "snap-123.avro"] {
        std::fs::write(dir.path().join("metadata").join(name), "{}").unwrap();
    }
    // And a day that landed as CSV, which is part of the dataset and is a mistake.
    std::fs::create_dir_all(dir.path().join("data/date=2")).unwrap();
    std::fs::write(dir.path().join("data/date=2/part-0.csv"), "id\n2\n").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about = notes
        .iter()
        .find(|note| note.summary.starts_with("in the folder"))
        .unwrap_or_else(|| panic!("no note about what was not read: {notes:#?}"));
    assert_eq!(
        about.summary, "in the folder, 1 file is not Parquet, 3 files a writer left behind",
        "the csv in the partition that has no data of its own, and Iceberg's three"
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

    // The pointer comparison is the one that bites, and it is first because of that.
    // `begin` stores zero, so a second pass resets a shared counter as surely as a
    // fresh one and the count below reads 1 either way — it says what the counter
    // should hold, and the line under it is what makes holding it mean anything.
    assert!(
        !Arc::ptr_eq(&counter_of_the_first, &app.footer_progress),
        "the second open has a counter of its own"
    );
    assert_eq!(
        app.footer_progress.last_pass().read,
        1,
        "counting its one footer, not the three before it"
    );

    // And the behaviour, not just the mechanism: the first open's pass goes on running
    // after it is abandoned, so a shared counter would paint its progress onto the
    // screen of the file that replaced it. Driven by hand, since a real abandoned pass
    // finishes too fast to catch — and rendered while the app is still *loading*,
    // because an idle app never consults the counter at all and the assertion would
    // hold however broken the wiring was.
    counter_of_the_first.begin(6541);
    for _ in 0..4102 {
        counter_of_the_first.advance();
    }
    app.set_loading_phase("Caching schema", 40);
    let mut buf = ratatui::buffer::Buffer::empty(Rect::new(0, 0, 100, 24));
    app.render(Rect::new(0, 0, 100, 24), &mut buf);
    let frame: String = (0..24)
        .flat_map(|y| {
            (0..100)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();
    assert!(
        frame.contains("Caching schema"),
        "the app is on its loading screen, where the counter is read:\n{frame}"
    );
    assert!(
        !frame.contains("6,541"),
        "and the abandoned folder's count does not appear under the file that \
         replaced it:\n{frame}"
    );
}

/// The control bar shows the same count the loading body does.
///
/// Both derive it from `App::loading_phase`, and the point of that is that one wait
/// cannot be described two ways. The truncation test in `controls.rs` builds the bar
/// with a hand-written string, so it says the bar cuts a long message properly and
/// nothing about whether the bar is ever given the count at all: deleting the line
/// that hands it over leaves the body counting and the bar still saying "Caching
/// schema", with the suite green.
#[test]
fn test_the_control_bar_counts_the_footers_the_loading_screen_does() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Caching schema", 40);
    app.footer_progress.begin(6541);
    for _ in 0..1203 {
        app.footer_progress.advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect()
        })
        .collect();

    let body = rows.iter().find(|r| r.contains("Reading footers"));
    assert!(body.is_some(), "the body counts them:\n{}", rows.join("\n"));
    let bar = rows.last().expect("a control bar");
    assert!(
        bar.contains("Reading footers: 1,203 of 6,541"),
        "and so does the bar, rather than the phase the body has stopped showing: \
         {bar:?}"
    );
    // Not beside the per-phase constant, which is 40 here and would read as this
    // count's progress. 1,203 of 6,541 is 18%.
    assert!(
        !bar.contains('%'),
        "a real fraction is not to be shown beside a made-up percentage: {bar:?}"
    );
}

/// The bar says the footers are still arriving, after the dataset is on screen.
///
/// A cloud prefix of many files opens from two of them and reads the rest behind the
/// data. Nothing is blocked and nothing is wrong, so it is said in the control bar
/// rather than on a loading screen — but it is said, because otherwise columns appear
/// minutes later with no explanation.
#[test]
fn test_the_bar_says_the_footers_are_still_arriving_while_the_data_is_up() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    // Said only for a dataset that is itself waiting. The counter is shared with every
    // open, and one abandoned half way through goes on counting: without the dataset's
    // own say-so this bar would count a folder the user walked away from.
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .set_footers_pending(std::sync::Arc::new(|_| None));
    app.footer_progress.begin(6541);
    for _ in 0..1203 {
        app.footer_progress.advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Reading footers: 1,203 of 6,541"),
        "the bar says what is still arriving: {bar:?}"
    );
    // And it prints the count, because this dataset has one: every file of it was read
    // at the open. A spinner here would be spinning over a number in hand. What is not
    // shown is a count that has not been taken — see
    // `a_count_that_has_arrived_is_not_held_back_with_the_columns`, where the dataset
    // says a count is still coming exactly while it has none.
    assert!(
        bar.contains("Rows: 3"),
        "the count it does have is shown: {bar:?}"
    );

    // And says nothing for a dataset that is not the one waiting: the folder this user
    // gave up on goes on reading its footers, and this is not it.
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .give_up_on_pending_footers();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let other: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !other.contains("Reading footers"),
        "a count belonging to a folder the user left is not this dataset's: {other:?}"
    );
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .set_footers_pending(std::sync::Arc::new(|_| None));

    // And stops saying it the moment they have.
    app.footer_progress.done();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Reading footers"),
        "and nothing once they are all in: {bar:?}"
    );
}

/// One frame, one number — while the pass is still running.
///
/// The body and the bar are painted a millisecond apart, with the threads reading the
/// footers moving the counter in between. Reading it once each let them print
/// different numbers for the same wait: measured at four thousand frames out of four
/// thousand. It also let the bar decide there was no count running just after the body
/// had shown one, putting the phase's flat percentage back beside it.
#[test]
fn test_one_frame_says_one_number_while_the_footers_are_still_arriving() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Caching schema", 40);
    app.footer_progress.begin(200_000);

    // A reader, going as fast as the real ones do between two paints.
    let counter = app.footer_progress.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = stop.clone();
    let reading = std::thread::spawn(move || {
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            counter.advance();
        }
    });

    let area = Rect::new(0, 0, 100, 24);
    // The count itself, not the line: the body is centred and the bar carries the rest
    // of the status beside it, so the two lines differ everywhere except here.
    let number = |row: &str| -> Option<String> {
        row.split("Reading footers: ").nth(1).map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .collect()
        })
    };
    for frame in 0..200 {
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect();
        let body = rows
            .iter()
            .find(|r| r.contains("Reading footers"))
            .and_then(|r| number(r));
        let bar = rows.last().and_then(|r| number(r));
        assert_eq!(
            body,
            bar,
            "frame {frame} said two things about one wait:\n{}",
            rows.join("\n")
        );
        assert!(
            body.is_some(),
            "frame {frame} stopped counting mid-pass:\n{}",
            rows.join("\n")
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reading.join().unwrap();
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
        "extra is in 1 of 2 files, only date=2024-01-02; absent from the rest, \
         not null"
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

    // Derived rather than counted out here: how many notes fit depends on how long
    // they are, and a note says more now than it used to. What has to hold is that the
    // ones that do not fit are counted rather than dropped.
    let shown = ["a", "b", "c", "d", "e", "f"]
        .iter()
        .filter(|name| screen.contains(&format!("{name} is in 1 of 2 files")))
        .count();
    assert!(
        (2..6).contains(&shown),
        "some notes fit and some do not, which is what this is about — and a panel \
         this tall holds at least two, or the panel has got much greedier than the \
         note got longer: {shown} of 6"
    );
    assert!(
        screen.contains(&format!("{} below", 6 - shown)),
        "and the ones out of view are counted, got:\n{screen}"
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
    // The shortest panel that draws a note, found rather than written down: how tall
    // that is depends on how long a note is, and a note says more than it used to.
    let mut drawn_at = |height: u16| -> String {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };
    let shortest = (4u16..14)
        .find(|height| drawn_at(*height).contains("is in 1 of 2 files"))
        .expect("some panel in this range draws a note");
    assert!(
        shortest <= 7,
        "a note fits in a short panel; {shortest} rows to draw one means the panel has \
         got greedier, and the loop below would pass on one height and prove nothing"
    );

    // From there up, every height draws one. The bug this guards is a panel that has
    // the room and refuses anyway, which showed as a gap in the middle of this range.
    for height in shortest..14 {
        let screen = drawn_at(height);
        assert!(
            screen.contains("is in 1 of 2 files"),
            "a {height}-row panel has room for a note — {shortest} rows is enough — so \
             it draws one: {screen:?}"
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
    // Both write null — an absent cell and a real null are the same thing to a CSV, and
    // the source file is what tells them apart once the data has left. Asserting the
    // whole line rather than its end, because `extra` being empty is the half of this
    // the doc comment claims and nothing checked.
    assert!(
        lines[1].starts_with("2024-01-01,1,,"),
        "the absent cell is written as null: {}",
        lines[1]
    );
    assert!(
        lines[2].starts_with("2024-01-02,2,,"),
        "and so is the real one: {}",
        lines[2]
    );

    // Off by default, and then the hidden index must not leak in its place.
    let plain = dir.path().join("plain.csv");
    let csv = export_csv(&mut app, &rx, &tx, &plain, false);
    let plain_lines: Vec<&str> = csv.lines().collect();
    assert_eq!(plain_lines[0], "date,id,extra");
    assert_eq!(
        &plain_lines[1..],
        ["2024-01-01,1,", "2024-01-02,2,"],
        "and without it both are still null, and nothing else has appeared"
    );
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

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let screen = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["date", "id"]);
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
    // The dataset opening is the claim, so the rows are what has to be there. A schema
    // computed over a file that would not parse says nothing about whether the other
    // two can be read through it.
    assert!(
        !screen.contains("Error"),
        "and on screen, without an error: {screen}"
    );
    // The ids, not the partition names: `date=2024-01-01` and `date=2024-01-03` put a
    // `1` and a `3` on screen whatever the rows say, so checking for those characters
    // is a check on the folder's own names.
    let state = app.data_table_state.as_ref().unwrap();
    let ids = state
        .lf
        .clone()
        .collect()
        .expect("the two readable files are readable")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<i64>>();
    assert_eq!(ids, [1, 3], "the two readable files' rows are there");
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

/// Browsing a directory of more than 64 folders: nothing is looked into while the
/// listing is built, so no row is labelled from where it sits, and the rows the frame
/// draws are looked into after it — on a worker, a screenful at a time.
///
/// The bug this covers is #270: the first 64 folders of a 6,241-partition share read
/// `multi`, and every identical one after them read `dir`.
#[test]
fn test_a_big_listing_is_labelled_from_the_viewport_not_from_directory_order() {
    let tmp = tempfile::TempDir::new().unwrap();
    for i in 0..200 {
        let partition = tmp.path().join(format!("d{i:03}")).join("year=2024");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("part.parquet"), b"").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();

    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);

    // The listing lands first, and the frame that draws it says of every row only
    // that nothing has looked into it.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        !app.home.visible().is_empty()
    });
    let unlooked_at = text_of(&buf);
    assert!(
        !unlooked_at.contains(" dir"),
        "no row should be called a plain directory before anything looked into one:\n\
         {unlooked_at}"
    );
    let unlooked_at_row = format!("d000/ {}", datui::glyphs::get().ellipsis);
    assert!(
        unlooked_at.contains(&unlooked_at_row),
        "an unlooked-at row should read `{unlooked_at_row}`:\n{unlooked_at}"
    );

    // Then the rows that frame drew are looked into, and say what they are. Asked of
    // the rows on screen rather than the selected one: the cursor starts on the row that
    // opens the whole folder, which is not one of the two hundred being looked into.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        app.home.visible().iter().any(|row| match row {
            datui::home::Row::Entry { entry, .. } => entry.kind == datui::discover::EntryKind::Hive,
            _ => false,
        })
    });
    let looked_at = text_of(&buf);
    assert!(
        looked_at.contains("hive"),
        "the rows on screen should have been looked into:\n{looked_at}"
    );

    // And the bottom of the listing still has not been, which is the point: the
    // budget follows the viewport rather than directory order.
    let kinds: Vec<datui::discover::EntryKind> = app
        .home
        .visible()
        .iter()
        .filter_map(|row| match row {
            datui::home::Row::Entry { entry, .. } => Some(entry.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds.last(),
        Some(&datui::discover::EntryKind::Unknown),
        "the bottom of a two-hundred-row listing is nobody's viewport"
    );
}

/// Draw, ask for what the frame needs, take one answer, draw again — the shape of
/// `run()` around `terminal.draw`, so a state the real loop passes through for one
/// frame can be caught here too.
fn pump_home(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    area: Rect,
    buf: &mut Buffer,
    done: impl Fn(&App) -> bool,
) {
    for _ in 0..200 {
        buf.reset();
        Widget::render(&mut *app, area, buf);
        app.request_what_the_frame_needs();
        if done(app) {
            return;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(500))
            && let Some(next) = app.event(&ev)
        {
            app.event(&next);
        }
    }
    panic!("the home screen never settled");
}

/// Everything a buffer has drawn, as lines.
fn text_of(buf: &Buffer) -> String {
    (0..buf.area.height)
        .map(|y| {
            (0..buf.area.width)
                .filter_map(|x| buf.cell((x, y)).map(|c| c.symbol().to_string()))
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
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

/// Phase 2 promised that aggregations show nulls: a column some files were written
/// without must read as null everywhere it is absent, not vanish from the aggregate and
/// not stop it. Describe is the aggregation a user reaches for first, so this asserts on
/// the panel it paints — the `Nulls` figure beside `extra` — rather than on the results
/// struct behind it. `extra` is in two of the three files, so it counts two values and
/// one null, and that one null is an absence: no file wrote a null into `extra`.
#[test]
fn test_an_aggregation_counts_an_absent_column_as_null() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "extra" => &["y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `a` opens the analysis modal, Enter runs the tool the sidebar starts on, Describe.
    if let Some(next) = app.event(&key(KeyCode::Char('a'))) {
        let _ = tx.send(next);
    }
    if let Some(next) = app.event(&key(KeyCode::Enter)) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect();

    // That the Describe table is the thing on screen, before reading figures off it.
    // Without this the fallback is the data table, whose header also begins with a
    // column name, and the failure would be about the wrong screen.
    assert!(
        rows.iter()
            .any(|line| line.contains("Count") && line.contains("Nulls")),
        "Describe should be on screen with its Count and Nulls columns; got:\n{}",
        rows.join("\n")
    );
    let extra = rows
        .iter()
        .find(|line| line.trim_start().starts_with("extra"))
        .unwrap_or_else(|| {
            panic!(
                "describe should list `extra`, the column two of the three files have; got:\n{}",
                rows.join("\n")
            )
        });
    // `skip(1)` steps over the column name, which this fixture keeps to a single token
    // on purpose: a name with a space in it would put its second half where Count is.
    // The row also runs into the sidebar at the right, which is harmless while only the
    // first two figures are read.
    let figures: Vec<&str> = extra.split_whitespace().skip(1).collect();
    assert_eq!(
        figures.first().copied(),
        Some("2"),
        "two files wrote `extra`, so it counts two values; row was {extra:?}"
    );
    assert_eq!(
        figures.get(1).copied(),
        Some("1"),
        "the third file was written without `extra`, and that absence counts as a null \
         in the aggregate; row was {extra:?}"
    );
}

/// Opening a folder measures what it cost, and the Info panel says so.
///
/// The end of the wiring rather than any one link in it: the open records into the
/// meter the app holds, the panel is handed that same meter, and the Resources tab
/// prints it. Each of those is guarded on its own elsewhere; none of those guards would
/// notice the panel being handed a meter nobody wrote to, which is the failure a user
/// would actually see — a Measurements section that says a dataset of three files was
/// found in no time at all.
///
/// Only the counts are asserted. The times are real elapsed times, so the only claim
/// that holds on every machine is that they were taken.
#[test]
fn test_opening_a_folder_measures_it_and_the_info_panel_says_so() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `i` opens the panel on the Schema tab with the focus in its body; Tab moves the
    // focus to the tab bar, and one step right from Schema is Resources.
    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);

    assert!(
        text.contains("Measurements"),
        "the Resources tab says what the open cost; got:\n{text}"
    );
    assert!(
        text.contains("Listing:") && text.contains("Footers:"),
        "naming the two stretches it timed; got:\n{text}"
    );
    assert!(
        text.contains("3 files"),
        "the listing found three files; got:\n{text}"
    );
    // Six, not three. The open reads a footer from each file, and then the count pass
    // re-walks the folder and reads every footer again to settle the row count — which
    // happens on an ordinary three-file open, not only in some corner. Both are footer
    // reads and both cost what they cost, so the row says six.
    assert!(
        text.contains("6 footers read"),
        "and the footer row counts the open's pass and the count's, which is six reads \
         over three files; got:\n{text}"
    );
    assert!(
        !text.contains("requests"),
        "a local folder is read, not requested, so no request count is claimed; got:\n{text}"
    );
}

/// A route that measures and then gives up leaves nothing on the dataset another route
/// built.
///
/// The routes are tried cheapest first, and the early ones measure before they discover
/// they cannot finish. A folder whose only Parquet sits under a `_delta_log` is the case
/// that reaches the screen: the hive route walks it, counts the checkpoint as the
/// writer's own bookkeeping, records a listing of no files and a footer pass over none,
/// and then gives up — while the full scan's glob does match the checkpoint and opens
/// it. Sharing one meter across the attempts paints `0 files` on a dataset showing rows.
#[test]
fn test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let mut frame = df!("id" => &[1i64, 2, 3]).unwrap();
    let f = File::create(log.join("00000.checkpoint.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut frame).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app
        .data_table_state
        .as_ref()
        .expect("the full scan opened the checkpoint");
    let listing = state.measurements().listing();
    assert!(
        listing.is_none_or(|c| c.files != Some(0)),
        "the hive route walked, found nothing it would read, and gave up — its empty \
         listing must not end up on the dataset that did open; got {listing:?}"
    );

    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        !text.contains("0 files"),
        "and the Resources tab shows no listing of no files; got:\n{text}"
    );
}

/// A dataset Polars opened shows no measurements, even though its rows are counted
/// afterwards.
///
/// `--single-spine-schema false` turns off the footer pass and hands the folder
/// straight to Polars, so there is nothing for datui to report. But the row count is
/// still taken from the footers afterwards, against the same dataset — and that pass
/// writing into the meter would raise a section out of nothing, headed by what opening
/// the dataset cost and containing only work done after it was already on screen.
#[test]
fn test_a_dataset_polars_opened_reports_nothing_about_opening_it() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            single_spine_schema: false,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().expect("the folder opened");
    assert_eq!(
        state.measurements().footers(),
        None,
        "the count pass ran, and belongs to no open this meter measured"
    );
    assert_eq!(
        state.measurements().total(),
        None,
        "and with neither stretch there is nothing to total"
    );

    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        !text.contains("Listing:") && !text.contains("Footers:") && !text.contains("Total:"),
        "so the Resources tab says nothing about what the open cost; got:\n{text}"
    );
    // The page it is showing is datui's own work on any route, and is reported.
    assert!(
        text.contains("Last page:"),
        "while what the page on screen cost is measured whichever route opened the \
         dataset; got:\n{text}"
    );
}

/// A second open does not inherit the first's figures, and a *failed* second open does
/// not give its figures to the dataset it left on screen.
///
/// The meter belongs to the dataset, not to the app, which is what makes the second
/// half true: a load that never reaches the screen never has its meter installed, so
/// the dataset still up keeps its own. An app-held meter gets this wrong in a way that
/// is hard to see — the panel keeps its heading and its shape, and only the numbers
/// underneath belong to something else.
#[test]
fn test_a_second_open_measures_itself_and_not_the_dataset_before_it() {
    let first = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            first.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-02-01",
        df!("id" => &[9i64]).unwrap(),
    );
    // Seven files that are named like Parquet and are not, so the open fails after its
    // listing and its footer pass have both been measured.
    let broken = tempfile::tempdir().unwrap();
    let broken_part = broken.path().join("date=2024-03-01");
    std::fs::create_dir_all(&broken_part).unwrap();
    for i in 0..7 {
        std::fs::write(broken_part.join(format!("f{i}.parquet")), b"not parquet").unwrap();
    }

    let hive = || OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let files_of = |app: &App| {
        app.data_table_state
            .as_ref()
            .and_then(|s| s.measurements().listing())
            .and_then(|c| c.files)
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);
    let meter_of_the_first = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    assert_eq!(
        files_of(&app),
        Some(3),
        "the first open measured its own three files"
    );

    // A second open that succeeds replaces both the dataset and its figures.
    pump_open_until_loaded(&mut app, &rx, vec![second.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        !std::sync::Arc::ptr_eq(
            &meter_of_the_first,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the second dataset was installed with a meter of its own"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "and reports the one file it found, not the four both folders hold between them"
    );

    // A third open that fails leaves the second dataset up — and leaves its figures
    // alone. The failed load measured seven files; none of them may appear here.
    let meter_of_the_second = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    pump_open_until_loaded(&mut app, &rx, vec![broken.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        std::sync::Arc::ptr_eq(
            &meter_of_the_second,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the dataset on screen is still the second one, with the meter it was \
         installed with"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "so the panel still says one file — not the seven the load that failed walked"
    );
}

/// `→` goes inside a local folder that opens as one dataset, as it has always done in a
/// bucket.
///
/// The gate was `is_object_store_url`, so on a local hive tree or a local folder of part
/// files there was no way in at all: Enter opened the whole thing, `←`/`→` folded the
/// section, and the files inside were unreachable from the home screen. That is the
/// escape hatch for a folder classified wrongly, and locally there was none.
#[test]
fn test_right_goes_inside_a_local_multi_file_folder() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let folder = tmp.path().join("sales");
    std::fs::create_dir_all(&folder).unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(folder.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the folder is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::MultiFile),
        "a folder of part files is offered as one dataset"
    );

    // The bar says the door is there, since nothing else on screen does.
    // Wide on purpose: the bar is cut from the right, and this assertion is about
    // what the bar says, not about where the fitting loop stops.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the key: {bar:?}");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(folder.as_path()),
        "→ browsed into the folder rather than folding the section"
    );
}

/// Recents grouped by place: two recents in one directory, one in another, so the
/// home screen shows two place rows. Returns the app with the cursor on the first
/// place row, and the three recents.
///
/// `seed_store` records them in the recents store too, for a test that reads it back.
/// Tests share one isolated store, and every open writes to it, so a test that reads
/// it asks whether its own paths are there rather than what else is.
fn app_with_recents_in_two_places(
    tmp: &tempfile::TempDir,
    seed_store: bool,
) -> (App, Vec<PathBuf>) {
    common::isolate_cache();
    let here = tmp.path().join("here");
    let there = tmp.path().join("there");
    std::fs::create_dir_all(&here).unwrap();
    std::fs::create_dir_all(&there).unwrap();
    let recents = vec![
        here.join("a.parquet"),
        here.join("b.parquet"),
        there.join("c.parquet"),
    ];
    for path in &recents {
        std::fs::write(path, b"x").unwrap();
    }
    // Recorded the way an open records them, so what the test forgets is what the
    // store holds. Oldest first: `push_recent` puts each at the front.
    if seed_store {
        let cache = datui::CacheManager::new("datui").expect("cache");
        for path in recents.iter().rev() {
            cache.push_recent(path);
        }
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("the directory two recents live in is a place row");
    app.home.selected = row;
    (app, recents)
}

/// `Enter` on a place row browses the place: the way back to a directory found by
/// hand, now that a recent's directory is no longer a section of its own.
#[test]
fn test_enter_on_a_place_row_browses_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents) = app_with_recents_in_two_places(&tmp, false);
    let here = recents[0].parent().unwrap().to_path_buf();

    // The bar says → goes inside, the same as on any folder.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the door: {bar:?}");

    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
    assert_eq!(
        app.home.browse_start.as_deref(),
        Some(here.as_path()),
        "Esc comes back from here to the listing"
    );

    // → is the other door to the same place.
    app.event(&key(KeyCode::Esc));
    assert_eq!(app.home.browsing, None);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("back at the listing");
    app.home.selected = row;
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
}

/// `Delete` on a place row forgets every recent under it and nothing else, after
/// asking. What is checked is the store, which is what the next launch reads.
#[test]
fn test_delete_on_a_place_row_forgets_exactly_its_recents_after_confirming() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents) = app_with_recents_in_two_places(&tmp, true);
    let cache = datui::CacheManager::new("datui").expect("cache");
    let holds =
        |cache: &datui::CacheManager, path: &Path| cache.load_recents().iter().any(|p| p == path);
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(&key(KeyCode::Delete));
    let area = Rect::new(0, 0, 120, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(
        screen.contains("Forget 2 recently opened datasets under"),
        "asked first, and told how many: {screen:?}"
    );
    assert!(
        recents.iter().all(|p| holds(&cache, p)),
        "nothing is forgotten until the question is answered"
    );

    // Declined: the store is untouched, and a later confirmation is not armed.
    app.event(&key(KeyCode::Esc));
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(&key(KeyCode::Delete));
    app.event(&key(KeyCode::Enter));
    assert!(
        !holds(&cache, &recents[0]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        !holds(&cache, &recents[1]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        holds(&cache, &recents[2]),
        "the other place's recent is left alone: {:?}",
        cache.load_recents()
    );
}

/// `Enter` and `→` on the place of a recent opened over HTTP say why they do nothing,
/// rather than listing a URL and reporting it unreachable.
#[test]
fn test_the_place_of_an_http_recent_says_it_cannot_be_browsed() {
    common::isolate_cache();
    let url = PathBuf::from("https://example.com/data/y.csv");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], std::slice::from_ref(&url));
    let place = PathBuf::from("https://example.com/data");
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == place))
        .expect("the URL's prefix is its place");
    app.home.selected = row;

    // The bar does not offer the door.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(!bar.contains("Inside"), "{bar:?}");

    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing, None);
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("HTTP")),
        "{:?}",
        app.home.status
    );
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing, None);
}

/// The bar's count is of what is listed, which the header and the `more` row agree on.
#[test]
fn test_the_bar_counts_datasets_past_the_cap() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    for section in 1..app.home.sections.len() {
        app.home.set_collapsed(section, true);
    }
    let area = Rect::new(0, 0, 200, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("12 datasets"), "{bar:?}");
}

/// The rendered list, one string per screen row, without the control bar.
fn list_rows(buf: &Buffer, area: Rect) -> Vec<String> {
    (0..area.height - 1)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

/// With thirty list rows or more, a blank line precedes every section header but the
/// first; below that, none. The spacers are counted against the cap and the scroll,
/// so the selected row is always on screen.
#[test]
fn test_a_tall_list_spaces_its_sections_and_a_short_one_does_not() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let configured = tmp.path().join("configured");
    std::fs::write(
        {
            std::fs::create_dir_all(&configured).unwrap();
            configured.join("c.parquet")
        },
        b"x",
    )
    .unwrap();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home
        .rebuild(std::slice::from_ref(&configured), &recents);

    let is_header = |line: &str| {
        line.contains("RECENT") || line.contains("current directory") || line.contains("configured")
    };
    // Tall: the wordmark and prompt take the top rows; the list below has room.
    let area = Rect::new(0, 0, 100, 50);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            rows[at - 1].trim().is_empty(),
            "a blank line precedes the header at {at}: {:?}",
            rows[at - 1]
        );
    }
    assert!(
        !rows[headers[0] - 1].trim().is_empty()
            || headers[0] == 0
            || rows[headers[0] - 1].contains("filter"),
        "no blank line before the first header"
    );
    let spacers = headers.len() - 1;
    let list_height = app.home.view_height + spacers;
    assert!(list_height >= 30, "{list_height}");
    assert_eq!(
        app.home.view_height,
        list_height - spacers,
        "the spacers come off the height the cap is a share of"
    );

    // The last row on screen, selected: drawn, spacers and all.
    let last = app.home.visible().len() - 1;
    app.home.selected = last;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    let name = match app.home.selected_row() {
        Some(datui::home::Row::Entry { entry, .. }) => entry.name.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        screen.contains(&name),
        "the selected row is on screen: {screen:?}"
    );

    // Short: dense.
    let area = Rect::new(0, 0, 100, 24);
    app.home.selected = 0;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            !rows[at - 1].trim().is_empty(),
            "no blank line before the header at {at} on a short screen"
        );
    }
}

/// The `… N more` row stands for the places the cap hides. `Enter` on it shows them
/// all, and nothing is opened.
#[test]
fn test_enter_on_the_more_row_expands_recent() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    // A short screen, so the cap bites: one place, then the more row.
    let area = Rect::new(0, 0, 120, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::More { .. }))
        .expect("the more row is listed");
    app.home.selected = row;
    app.event(&key(KeyCode::Enter));

    assert_eq!(app.input_mode, InputMode::Home, "nothing was opened");
    assert_eq!(app.home.browsing, None);
    let places = app
        .home
        .visible()
        .iter()
        .filter(|r| matches!(r, datui::home::Row::Place { .. }))
        .count();
    assert_eq!(places, 12, "every place is shown");
    assert!(
        !app.home
            .visible()
            .iter()
            .any(|r| matches!(r, datui::home::Row::More { .. }))
    );
}

/// The hint, and the descent, are only offered on a row that is a dataset folder.
/// `→` elsewhere goes on expanding the section, which on a visible row is already
/// expanded and so does nothing.
#[test]
fn test_right_does_not_browse_from_an_ordinary_row() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;

    // Wide on purpose: the bar is cut from the right, and this assertion is about
    // what the bar says, not about where the fitting loop stops.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "a file is not a folder to go inside: {bar:?}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(tmp.path()),
        "→ on a file does not browse anywhere"
    );
}

/// A Delta table's root is not a folder of Parquet files, and the home screen says so.
///
/// Its data files agree on a schema, so the one-table rule called it `multi` and Enter
/// read every file under it as one table — tombstoned rows back, every rewritten
/// version together, compaction counted twice. Nothing warned.
#[test]
fn test_a_delta_table_is_labelled_and_not_opened_as_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta),
        "the log says what this is"
    );

    let area = Rect::new(0, 0, 200, 24);
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
    let listing = screen(&mut app);
    assert!(
        listing.contains("delta"),
        "the row says what it is: {listing}"
    );
    assert!(
        !listing.contains("multi"),
        "and does not offer it as a folder of files: {listing}"
    );

    // Enter goes inside rather than reading every file under it as one table.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "Enter went inside the table"
    );
    assert!(app.data_table_state.is_none(), "and opened nothing");
    let status = app.home.status.clone().unwrap_or_default();
    assert!(
        status.contains("Delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A lake table typed at `~` is not opened as one table either.
///
/// `home_open_selected` learned to go inside one; the path input had no check at all, so
/// `~` and the table's path loaded every Parquet under the root as one table — the whole
/// of the silent wrong answer, reached one keystroke differently.
#[test]
fn test_a_lake_table_typed_as_a_path_is_gone_inside_not_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.path_input_active = true;
    app.home.path_input = table.to_string_lossy().into_owned();

    let follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    assert!(follow.is_none(), "nothing was opened");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the path went inside the table"
    );
    let status = app.home.status.clone().unwrap_or_default();
    assert!(
        status.contains("Delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A directory of lake tables is not an empty home screen.
///
/// The guidance block is appended under the rows rather than shown instead of them, so a
/// warehouse of fifty `delta` rows printed "No datasets here." underneath them.
#[test]
fn test_a_folder_of_lake_tables_does_not_say_there_is_nothing_here() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for name in ["orders", "customers"] {
        let table = tmp.path().join(name);
        std::fs::create_dir_all(table.join("_delta_log")).unwrap();
        std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
        std::fs::write(table.join("part-0.parquet"), b"x").unwrap();
        std::fs::write(table.join("part-1.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(screen.contains("orders"), "the tables are listed: {screen}");
    assert!(
        !screen.contains("No datasets here."),
        "and the screen does not say there is nothing here: {screen}"
    );
}

/// `→` goes inside a lake table, as it does a hive or multi folder.
///
/// A cloud Delta root used to be labelled `multi`, where `→` descended; recognizing it
/// made `→` fold the section instead. Enter goes inside either way, so nothing was
/// unreachable, but the key that means "look inside this folder" stopped meaning it on
/// the one row where looking inside is all datui can do.
#[test]
fn test_right_goes_inside_a_lake_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta)
    );

    // Wide on purpose: this is about what the bar says, not where it is cut.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Inside"),
        "the key is offered here too: {bar:?}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "→ went inside the table rather than folding the section"
    );
}

/// A sampled column count shows as a floor on the row and in the details pane.
///
/// The `Entry` flag had a test; what reaches the user had none — reverting either render
/// site left the suite green.
#[test]
fn test_a_sampled_column_count_is_marked_on_screen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let folder = tmp.path().join("events");
    std::fs::create_dir_all(&folder).unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the folder is listed");
    app.home.selected = row;

    // As a folder past the footer budget comes back from measurement.
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "events") {
            entry.kind = datui::discover::EntryKind::MultiFile;
            entry.rows = None;
            entry.cols = Some(39);
            entry.cols_sampled = true;
            entry.columns = vec!["id".to_string()];
        }
    }

    let area = Rect::new(0, 0, 160, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        screen.contains("39+"),
        "the row and the pane say the count is a floor: {screen}"
    );
    // Both render sites, named separately: a `contains` over the whole screen would be
    // satisfied by either one alone.
    let times = datui::glyphs::get().times;
    assert!(
        screen.contains(&format!("? {times} 39+")),
        "the row's shape says the width is a floor: {screen}"
    );
    assert!(
        screen.contains("columns   39+"),
        "and so does the details pane: {screen}"
    );
}

/// An unexamined directory is classified before Enter opens it.
///
/// A cached kind this build will not take leaves the row `Unknown`, whose `is_dataset()`
/// is true — so Enter fell through to opening the path as one dataset. For a lake root
/// that is the whole of #237, restored from a cache written by an older datui.
#[test]
fn test_an_unexamined_lake_root_is_classified_before_it_is_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;

    // As a row restored from a cache this build will not take its kind from.
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "orders") {
            entry.kind = datui::discover::EntryKind::Unknown;
        }
    }
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
        "the row the cursor is on is the unexamined one"
    );

    let follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    assert!(follow.is_none(), "nothing was opened as one table");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "it was looked at first, found to be a Delta root, and gone inside"
    );
}

/// `→` into a lake table says the same thing `Enter` does.
///
/// The control bar advertises `→` on that row, and `home_browse_into` clears the status
/// line — so the door the bar points at was the one that arrived inside with no
/// explanation.
#[test]
fn test_right_into_a_lake_table_says_why() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(app.home.browsing.as_deref(), Some(table.as_path()));
    let status = app.home.status.clone().unwrap_or_default();
    assert!(
        status.contains("Delta") && status.contains("not read"),
        "→ says why it is showing files rather than a table: {status:?}"
    );
}

/// A Delta root on a mount that may not answer is looked at on a worker, and recognized.
///
/// `EntryKind::Unknown` — the only thing a remote row that has never been probed can be —
/// is offered as openable, so Enter read the whole root as one table. Classifying it
/// where the keys are read is the other half of the trap: `exists`, `is_dir` and a
/// `read_dir` on a hard-mounted share that has gone away is an uninterruptible freeze,
/// with Ctrl+C on the same thread.
#[test]
fn test_an_unexamined_remote_lake_root_is_classified_off_the_event_thread() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    // A share, as the mount table would have it — and the table in Recent, which is how
    // a row on one comes to be listed without anything having looked at it.
    app.home.network_check = |_| true;
    app.home.rebuild(&[], std::slice::from_ref(&table));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
        "nothing has looked at it, which is the whole point"
    );

    // The key itself decides nothing: it asks.
    let asked = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(asked, Some(AppEvent::ClassifyThenOpen { .. })),
        "Enter handed the look to a worker rather than doing it here"
    );
    let mut follow = asked;
    while let Some(event) = follow {
        follow = app.event(&event);
    }
    assert!(app.is_busy(), "and says so while the worker is out");

    // The worker's answer comes back on the channel.
    let mut opened = false;
    for _ in 0..50 {
        let Ok(event) = rx.recv_timeout(std::time::Duration::from_secs(10)) else {
            break;
        };
        if matches!(event, AppEvent::Open(..)) {
            opened = true;
        }
        let mut follow = app.event(&event);
        while let Some(next) = follow {
            if matches!(next, AppEvent::Open(..)) {
                opened = true;
            }
            follow = app.event(&next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert!(!opened, "it was never opened as one table");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the worker found a Delta root, and Enter went inside it"
    );
    let status = app.home.status.clone().unwrap_or_default();
    assert!(
        status.contains("Delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A background probe answering does not cancel the open the user asked for.
///
/// The first gate was `home_generation`, which means "the listing was rebuilt" and not
/// "the user navigated": a probe of some other root answering bumps it. On a home screen
/// with network roots — the only kind where this path runs at all — that made Enter do
/// nothing, at random, with no message.
#[test]
fn test_a_probe_answering_does_not_cancel_an_open_in_flight() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.network_check = |_| true;
    app.home.rebuild(&[], std::slice::from_ref(&table));
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;

    let mut follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(event) = follow {
        follow = app.event(&event);
    }

    // A listing the user did not ask for lands while the look is out. Through the event,
    // because it is the handler that refreshes the home screen — which is what the first
    // gate mistook for the user having navigated.
    app.event(&AppEvent::HomeProbeReady {
        root: PathBuf::from("/mnt/somewhere-else"),
        rows: Some(Vec::new()),
    });

    for _ in 0..50 {
        let Ok(event) = rx.recv_timeout(std::time::Duration::from_secs(10)) else {
            break;
        };
        let mut follow = app.event(&event);
        while let Some(next) = follow {
            follow = app.event(&next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the answer was still the one the user was waiting for"
    );
}

/// Opening a hive directory from the home screen still reads it as one dataset.
///
/// `home_open_path` used to work that out with a `stat`, which on a share that has gone
/// away is the freeze this whole path exists to avoid. It is told now, from the kind the
/// caller already has — so the thing to pin is that the answer did not change.
#[test]
fn test_a_hive_directory_from_home_still_opens_as_one_dataset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let hive = tmp.path().join("sales");
    for part in ["year=2024", "year=2025"] {
        std::fs::create_dir_all(hive.join(part)).unwrap();
        std::fs::write(hive.join(part).join("part-0.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the folder is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Hive)
    );

    let opened = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    match opened {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![hive.clone()]);
            assert!(options.hive, "read as one partitioned dataset");
        }
        _ => panic!("Enter on a hive folder opens it"),
    }

    // And a single file is not. (The open above left the home screen.)
    app.enter_home();
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;
    match app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(_, options)) => assert!(!options.hive, "a file is not a hive tree"),
        _ => panic!("Enter on a file opens it"),
    }

    // And the case that proves the answer is told rather than stat'ed: a row whose kind
    // says hive but whose path no longer answers, which is how a dropped mount presents
    // itself. `is_dir()` is false there, so a stat would call it a single file.
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let gone = PathBuf::from("/mnt/gone/sales");
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "sales") {
            entry.path = gone.clone();
        }
    }
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == gone))
        .expect("the row is listed");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Hive),
        "the row still says hive"
    );
    match app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![gone]);
            assert!(
                options.hive,
                "told from the kind, not worked out with a stat that cannot reach it"
            );
        }
        other => panic!("Enter opens it: {}", other.is_some()),
    }
}

/// A folder datui offers as one dataset is read as whatever is actually in it.
///
/// A folder the home screen labels `multi` is opened with `hive: true`, and every such
/// folder used to go straight to the Parquet scanner however it was filled. A folder
/// of CSVs therefore failed the way a folder of `.json.gz` did: Parquet seeks to the
/// last four bytes looking for `PAR1`, finds something else, and says the file must
/// end with it — a complaint about files that were never the problem.
#[test]
fn test_a_folder_opened_as_one_dataset_is_read_as_what_it_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("part-0.csv"), "a,b\n1,x\n2,y\n").unwrap();
    std::fs::write(tmp.path().join("part-1.csv"), "a,b\n3,z\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("a folder of CSVs should open as one table of CSVs");
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(
        df.height(),
        3,
        "both files' rows, concatenated: {:?}",
        df.get_column_names()
    );
}

/// A folder holding two different formats is not one table, and the complaint names
/// the folder rather than Parquet's magic number.
///
/// Asserting on the message, not merely on the failure: opening this folder failed
/// before the fix too — with "must end with PAR1", about files nobody asked to be
/// Parquet. A test that only checked that nothing loaded would pass either way and be
/// about nothing.
#[test]
fn test_a_folder_of_two_formats_is_read_as_the_one_it_mostly_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("a.csv"), "a\n1\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), "a\n2\n").unwrap();
    std::fs::write(tmp.path().join("c.json"), "[{\"a\":1}]").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let table = app
        .data_table_state
        .as_ref()
        .expect("two CSVs and a stray JSON is a folder of CSVs");
    assert_eq!(table.headers(), vec!["a"], "read as CSV, not as JSON");
    assert_eq!(
        table.num_rows_if_valid(),
        Some(2),
        "both CSVs, and not the JSON beside them"
    );
}

/// A folder of CSVs with some unrelated folder beside them is still a folder of CSVs.
///
/// The other edge of the same rule: what sends a folder to the Parquet hive scan is a
/// `key=value` partition under it, not merely having a subdirectory. Treating any
/// subfolder as "the data is deeper" handed an ordinary folder of CSVs back to the
/// scan that cannot read them.
#[test]
fn test_a_folder_of_csvs_beside_an_unrelated_folder_still_reads_as_csvs() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("part-0.csv"), "id\n1\n2\n").unwrap();
    std::fs::write(dir.path().join("part-1.csv"), "id\n3\n").unwrap();
    std::fs::create_dir_all(dir.path().join("archive")).unwrap();

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a folder of CSVs should open as one table of CSVs");
    assert_eq!(state.lf.clone().collect().unwrap().height(), 3);
}

/// A hive dataset of something other than Parquet says which files it holds.
///
/// Hive partitioning is a Parquet-only capability in the reader datui uses, so a tree
/// of `date=…/part.json` cannot be read as one table here. It used to reach the
/// Parquet scan anyway and fail with "the file must end with PAR1" — a complaint
/// about files nobody asked to be Parquet, naming neither the folder nor the format.
#[test]
fn test_a_hive_of_json_names_the_files_rather_than_parquets_magic_number() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["2009-03-07", "2009-03-08"] {
        let part = dir.path().join(format!("date={day}"));
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("part.json"), "[{\"id\":1}]").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let complaint = pump_open_until_error(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    )
    .expect("a hive of JSON cannot be read as one table");

    assert!(
        complaint.contains(".json") && complaint.contains("Parquet"),
        "it should say what the files are and why that is a problem: {complaint}"
    );
    assert!(
        !complaint.contains("PAR1"),
        "nothing here was ever Parquet: {complaint}"
    );
}

/// And a hive of Parquet still opens, however deeply it is partitioned.
///
/// The check above follows the partitions down to see what they hold; it must not
/// change what happens to the datasets that were always fine.
#[test]
fn test_a_nested_hive_of_parquet_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "year=2024/month=01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "year=2024/month=02",
        df!("id" => &[3i64]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a nested hive of Parquet is a dataset");
    assert_eq!(state.lf.clone().collect().unwrap().height(), 3);
}

/// Two doors, on a folder datui does not recognize as anything.
///
/// A folder holding a CSV and a JSON is `mixed`: no label datui has says it is one
/// table, and before this the row could only be folded — `→` did nothing and `Enter`
/// tried to open it as a dataset and said it could not. A folder whose storage
/// convention datui does not know is exactly the folder a user most needs to get into,
/// so both doors are open on it now: `→` steps inside, and the first row in there reads
/// the whole of it.
#[test]
fn test_both_doors_are_open_on_a_folder_datui_cannot_name() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let folder = tmp.path().join("exports");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(folder.join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "exports"))
        .expect("the folder is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.label().to_string()),
        Some("mixed".to_string()),
        "nothing datui knows calls this a dataset"
    );

    // The bar says the door is there, on a row no label offers as a dataset.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the key: {bar:?}");

    // One key in.
    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(folder.as_path()),
        "→ went inside a folder datui has no name for"
    );

    // And the first row in there is the other door. (The app rebuilds the listing on
    // the event this returns; here the same call does it on the spot.)
    app.home.rebuild(&[], &[]);
    let names: Vec<String> = app
        .home
        .visible()
        .iter()
        .filter_map(|r| match r {
            datui::home::Row::Entry { entry, .. } | datui::home::Row::Door { entry, .. } => {
                Some(entry.name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        names.first().map(String::as_str),
        Some("exports (all files)"),
        "got {names:?}"
    );
}

/// The `(all files)` row opens the folder it names, whatever the folder is labelled.
///
/// The label describes; this row is the promise that the description cannot lock you
/// out. A `mixed` folder is the case: `open_what_it_is` reads the label back, and a
/// `Directory` sent through it goes *inside* — which, on a row that is already inside,
/// is nowhere.
#[test]
fn test_enter_on_the_whole_folder_row_opens_rather_than_descending() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("every folder carries the row");
    app.home.selected = row;

    let was = app.home.browsing.clone();

    // → does nothing here. This row is inside the folder it opens, so going inside is
    // nowhere: it would re-enter the listing already on screen and lose the cursor and
    // the filter on the way. ← / → fold, the way they do on a file row. Checked before
    // Enter, because Enter puts the app in its loading view and the home bar is gone.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "the bar offers a door to nowhere: {bar:?}"
    );
    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing, was,
        "→ on the row that opens this folder must not re-enter it"
    );
    assert_eq!(
        app.home.selected, row,
        "and must not move the cursor off it"
    );

    // Enter opens it.
    let next = app.event(&key(KeyCode::Enter));
    assert!(
        matches!(next, Some(AppEvent::Open(..))),
        "Enter should open the folder rather than move the cursor"
    );
    assert_eq!(
        app.home.browsing, was,
        "and did not step into the folder it is already in"
    );
}

/// The door into a lake table reads its files, and says they are not the table.
///
/// A lake table is not a folder of Parquet files however much it looks like one:
/// reading one as a union counts tombstoned rows, every rewritten version and both
/// sides of a compaction. Refusing it, though, left a folder the user could see and
/// could not read at all — and this row is the promise that no label locks you out.
/// So the read is labelled instead of refused: a note in the panel, a chip beside the
/// row count, and the row one level up still goes inside and says datui does not read
/// the table itself yet. All three, because each on its own is missable.
#[test]
fn test_the_door_into_a_lake_table_says_its_files_are_not_the_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let events = tmp.path().join("events");
    std::fs::create_dir_all(events.join("_delta_log")).unwrap();
    std::fs::write(events.join("_delta_log").join("00000000.json"), b"{}").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(events.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(events.clone());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the folder carries the row");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta),
        "the listing under it is a Delta table"
    );

    // It opens, and the open carries what it is.
    let options = match app.event(&key(KeyCode::Enter)) {
        Some(AppEvent::Open(_, options)) => options,
        _ => panic!("the door opens the folder it names, whatever the label says"),
    };
    assert_eq!(
        options.read_as_plain_files_of,
        Some("Delta"),
        "and the open says these are a Delta table's files, not the table"
    );

    // The note and the chip, from that one field. Both, because the note is a tab away
    // and the chip is in the corner: each on its own is missable.
    let notes =
        datui::notes::from_the_open(&[], options.read_as_plain_files_of, Default::default());
    assert_eq!(notes.len(), 1, "one note, about the read");
    assert!(
        notes[0].summary.contains("Delta") && notes[0].summary.contains("deleted rows"),
        "it names the format and what the count includes: {:?}",
        notes[0].summary
    );

    // And the row one level up still goes inside rather than reading it.
    let (tx, _rx) = mpsc::channel();
    let mut up = App::new(tx, common::test_runtime());
    up.enter_home();
    up.home.browsing = Some(tmp.path().to_path_buf());
    up.home.rebuild(&[], &[]);
    let row = up
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the folder is listed");
    up.home.selected = row;
    assert!(up.event(&key(KeyCode::Enter)).is_none());
    assert_eq!(up.home.browsing.as_deref(), Some(events.as_path()));
    let said = up.home.status.clone().unwrap_or_default();
    assert!(
        said.contains("Delta") && said.contains("files under it"),
        "the row above is where datui says it does not read the table: {said:?}"
    );
}

/// `hive: true` is what puts the open on the local folder route at all: without it a
/// directory is `Unsupported file type`, and the whole of `folder_format`'s dispatch is
/// behind it. The door row builds its own open, so nothing else pins the flag.
#[test]
fn test_the_door_opens_a_folder_by_the_folder_route() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("a.csv"), b"x,y\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), b"x,y\n3,4\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the folder carries the row");
    app.home.selected = row;

    match app.event(&key(KeyCode::Enter)) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![tmp.path().to_path_buf()]);
            assert!(
                options.hive,
                "without this the open is `Unsupported file type`"
            );
        }
        _ => panic!("Enter on the door should open the folder"),
    }
}

/// → goes inside a row nothing has looked into yet.
///
/// On a share that is most rows: `entry_for_path` calls a remote path with no data
/// extension `Unknown`, and a listing looks into nothing. Excluding `Unknown` from the
/// door would put the folders that cost most to reach back behind a classification —
/// the label deciding access again, one indirection along. A remote file with an odd
/// extension is browsed into and shows an empty listing, which `Esc` backs out of.
#[test]
fn test_right_goes_inside_a_row_nothing_has_looked_into() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let folder = tmp.path().join("archive");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "archive"))
        .expect("the folder is listed");
    app.home.selected = row;
    // Deliberately not classified: this is what a listing hands over before anything
    // has looked into it, and what every row on a share looks like.
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
    );

    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(folder.as_path()),
        "→ went inside without needing to know what it is first"
    );
}

/// Each route's folders are classified in that route's vocabulary.
///
/// The door used to ask the cloud classifier about a local folder, which got two
/// answers wrong in opposite directions. `scan_dir` drops dotted names, so `.hoodie`
/// never reached it and a local Hudi table came back `MultiFile` — the door then read
/// its tombstones, two keystrokes after the row above said datui does not read Hudi
/// tables yet. And the cloud Iceberg rule is the looser of the two on purpose, names
/// only, so a plain folder holding `data/` beside `metadata/` was refused as a lake
/// table it is not: the second door closing on a false verdict, which is the whole
/// thing phase 3 exists to stop.
#[test]
fn test_the_door_reads_a_local_folder_with_the_local_rules() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // A Hudi table: the marker is a dotted name.
    let trips = tmp.path().join("trips");
    std::fs::create_dir_all(trips.join(".hoodie")).unwrap();
    std::fs::write(trips.join(".hoodie").join("hoodie.properties"), b"x").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(trips.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    // And a plain folder that merely looks like an Iceberg table by its names.
    let project = tmp.path().join("project");
    std::fs::create_dir_all(project.join("data")).unwrap();
    std::fs::create_dir_all(project.join("metadata")).unwrap();
    std::fs::write(project.join("data").join("a.parquet"), b"x").unwrap();
    std::fs::write(project.join("metadata").join("notes.md"), b"x").unwrap();

    let door_kind = |dir: &std::path::Path| {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(dir.to_path_buf());
        app.home.rebuild(&[], &[]);
        app.home
            .visible()
            .iter()
            .find_map(|r| match r {
                datui::home::Row::Door { entry, .. } => Some(entry.kind),
                _ => None,
            })
            .expect("the folder carries the row")
    };

    assert_eq!(
        door_kind(&trips),
        datui::discover::EntryKind::Hudi,
        "a dotted marker is not in the listing, so only the local rule can see it"
    );
    assert_eq!(
        door_kind(&project),
        datui::discover::EntryKind::Directory,
        "two folder names are not an Iceberg table: the local rule wants a \
         .metadata.json in one of them"
    );
}

/// A prefix in an object store is read with the reader its own listing calls for.
///
/// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of CSV
/// answered "Could not read from S3. Check credentials and URL" — a false statement
/// about a login that is fine. The listing has already counted what is there and it is
/// on screen, so picking the reader from it costs no request. What is left refused is
/// a prefix holding nothing datui has a multi-file reader for, and that refusal names
/// what is there rather than blaming the connection.
///
/// A fresh app per shape, because opening sets `busy` and the next key would be read
/// against a screen that is no longer the home screen.
#[cfg(feature = "cloud")]
#[test]
fn test_the_cloud_door_reads_a_prefix_with_the_reader_its_listing_calls_for() {
    use datui::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    // Press Enter on the door of a prefix holding these names, and say what happened.
    // A name with a dot in it stands for an object, the rest for sub-prefixes; a name
    // with an `=` in it is a partition, the way a listing hands one over.
    fn door(prefix: &str, names: &[&str]) -> (Option<OpenOptions>, String) {
        let place = PathBuf::from(prefix);
        let rows: Vec<Entry> = names
            .iter()
            .map(|name| {
                let mut entry = Entry::directory(&place.join(name));
                entry.name = (*name).to_string();
                if name.contains('.') {
                    entry.kind = EntryKind::File;
                    entry.size = Some(1_000);
                }
                entry
            })
            .collect();

        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.network_check = |_| true;
        app.home.probe_ready(place.clone(), rows);
        app.home.browsing = Some(place);
        app.home.rebuild(&[], &[]);
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Door { .. }))
            .expect("the prefix carries the row");
        app.home.selected = row;
        let event = app.event(&key(KeyCode::Enter));
        let options = match event {
            Some(AppEvent::Open(_, options)) => Some(options),
            _ => None,
        };
        (options, app.home.status.clone().unwrap_or_default())
    }

    // Data files, none of them Parquet: read as what they are.
    let (options, said) = door("s3://bucket/exports", &["a.csv", "b.csv", "c.csv"]);
    let options = options.expect("a prefix of CSV is a prefix datui can read");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert!(
        !said.to_lowercase().contains("credential"),
        "and nothing blames a login that is fine: {said:?}"
    );

    // Two formats, neither Parquet: the commonest is the reader, and the rest is said.
    // `label()` would call this `mixed`, a word rather than a count.
    let (options, _) = door("s3://bucket/pair", &["a.csv", "a2.csv", "b.json"]);
    let options = options.expect("a prefix of mostly CSV reads as CSV");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert_eq!(
        options.left_out,
        vec![(datui::FileFormat::Json, 1)],
        "and the dataset can say what it passed over"
    );

    // Nothing datui has a reader for. `holds.formats` is empty here, so a test written
    // over the formats alone let it through and the scan came back blaming the login.
    let (options, said) = door("s3://bucket/docs", &["README.md", "notes.txt"]);
    assert!(options.is_none());
    assert!(said.contains("nothing datui can read"), "{said:?}");
    assert!(!said.to_lowercase().contains("credential"), "{said:?}");

    // Parquet opens by its own route, which is the only one with hive partitioning
    // behind it and the one every cloud dataset took before any of this. The listing
    // already calls this prefix a dataset, so no reader is named and the scan makes the
    // Parquet call it always made. Without this case, a change that named a reader for
    // everything would pass every other assertion here.
    let (options, _) = door("s3://bucket/parts", &["part-0.parquet", "part-1.parquet"]);
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a prefix of Parquet is what a cloud folder reads as"
    );

    // No data files at all: tried, because the files below may be Parquet and nothing
    // here has looked.
    let (options, _) = door("s3://bucket/warehouse", &["by_year", "by_station"]);
    assert!(
        options.is_some(),
        "nothing counted directly inside is not a reason to refuse"
    );

    // Including with unreadable files beside the sub-prefixes: a README at the top says
    // nothing about what is under `by_year/`.
    let (options, _) = door("s3://bucket/warehouse2", &["README.md", "by_year"]);
    assert!(
        options.is_some(),
        "a sub-prefix may hold Parquet, and nothing here has looked"
    );

    // And a hive root with one stray data file beside its partitions. `formats` holds
    // only the stray, so picking the reader from it would read the whole root as CSV —
    // a prefix the listing already calls a dataset keeps the route its label named.
    let (options, said) = door(
        "s3://bucket/events",
        &["date=2024-01-01", "date=2024-01-02", "manifest.csv"],
    );
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a hive root is read through its partitions, not as the stray beside them: {said:?}"
    );
}

/// The `N datasets` caption counts what is listed, not the way out of the folder.
///
/// The door's kind is the folder's, so it counts as a dataset — and it is the same
/// dataset as the folder it opens, counted a second time, in the figure whose own
/// comment says counting a place-to-look makes it a lie.
#[test]
fn test_the_caption_does_not_count_the_door_as_a_dataset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for name in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        let file = std::fs::File::create(parts.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(parts);
    app.home.rebuild(&[], &[]);
    app.home.classify_now(16);

    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("3 datasets"),
        "three files, and the door is not a fourth: {bar:?}"
    );
}

/// #275's done-when for this phase: from a folder the rule turns away, one table is
/// still reachable in two keystrokes.
///
/// A folder whose files each bring a column the other lacks is a place to look inside
/// rather than a dataset — that is `is_nested`, and it is stricter than the containment
/// threshold it replaced. What makes a strict rule affordable is the other door: `→`
/// steps in, and the first row in there opens the union anyway.
#[test]
fn test_a_folder_the_nesting_rule_turns_away_is_still_two_keys_from_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sales = tmp.path().join("sales");
    std::fs::create_dir_all(&sales).unwrap();
    for (name, last) in [("old.parquet", "amount"), ("new.parquet", "amt")] {
        let mut frame = polars::prelude::DataFrame::new(
            1,
            vec![
                polars::prelude::Column::new("id".into(), &[1i32]),
                polars::prelude::Column::new(last.into(), &[2i32]),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(sales.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the folder is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    app.home.measure_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Directory),
        "neither file's columns are in the other's"
    );

    // One key in.
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(sales.as_path()));
    app.home.rebuild(&[], &[]);

    // The second key opens the union.
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the folder carries the row");
    app.home.selected = row;
    assert!(
        matches!(app.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens what the rule declined to open in one key"
    );
}

/// `datui <dir>` does what `Enter` on that folder's row does.
///
/// A directory used to be `Unsupported file type` unless `--hive` was passed, while
/// pyarrow, Polars, pandas and Spark all open one. Naming a folder is the request to
/// read it, so the command line answers the same as the other two doors onto a path.
#[test]
fn test_the_command_line_reads_a_folder_the_way_enter_does() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };

    // An app and the channel its background work answers on. The look at a folder is
    // an event now, not a call, so the test drives the same chain `run()` does.
    let app = || {
        let (tx, rx) = mpsc::channel();
        (App::new(tx, common::test_runtime()), rx)
    };
    let named_with =
        |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path, options: OpenOptions| {
            let mut next =
                app.open_the_path_named_on_the_command_line(vec![dir.to_path_buf()], options);
            // Follow the chain to whatever it settles on: the look goes to a worker and
            // answers here, and what it answers with is the decision. Settling on the
            // home screen is an outcome, not a timeout — a helper that could only tell
            // the two apart by waiting would make every such case cost the wait, and
            // would quietly pass a chain that had stopped.
            loop {
                if app.home.browsing.is_some() {
                    return None;
                }
                match next.take() {
                    Some(AppEvent::Open(paths, options)) => {
                        return Some(AppEvent::Open(paths, options));
                    }
                    Some(ev) => next = app.event(&ev),
                    None => match rx.recv_timeout(std::time::Duration::from_millis(4000)) {
                        Ok(ev) => next = Some(ev),
                        Err(_) => panic!("the chain stopped without settling on anything"),
                    },
                }
            }
        };
    // The options the binary really passes, not `OpenOptions::default()`. They differ
    // in exactly the fields a guard over "did the user set anything" reads — the CLI
    // fills in `infer_schema_length` and `parse_strings` on every run with no flags —
    // so a test built on the defaults cannot see a guard that fires on every open.
    let as_the_binary_does = |dir: &Path| {
        use clap::Parser;
        let args =
            datui_cli::Args::try_parse_from(["datui", dir.to_str().unwrap()]).expect("parses");
        OpenOptions::from_args_and_config(&args, &datui::config::AppConfig::default())
    };
    let named = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path| {
        let options = as_the_binary_does(dir);
        named_with(app, rx, dir, options)
    };

    // One table across several files: read as one, on the folder route.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let (mut a, rx_a) = app();
    match named(&mut a, &rx_a, &one) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![one.clone()]);
            assert!(options.hive, "the folder route is what reads a directory");
        }
        _ => panic!("a folder of one table opens as one table"),
    }

    // Separate tables: a place to look inside, browsed into rather than refused. The
    // `(all files)` row in there is the keystroke that unions them anyway.
    let several = tmp.path().join("several");
    parquet(&several, "by_block.parquet", table(&["block", "fee"]));
    parquet(
        &several,
        "daily.parquet",
        table(&["day", "price", "volume"]),
    );
    let (mut b, rx_b) = app();
    assert!(
        named(&mut b, &rx_b, &several).is_none(),
        "a folder of separate tables is somewhere to look, not a refusal"
    );
    assert_eq!(b.home.browsing.as_deref(), Some(several.as_path()));
    assert_eq!(b.input_mode, InputMode::Home);

    // A hive root reads as one table too, and still by the folder route.
    let hive = tmp.path().join("hive");
    parquet(&hive.join("day=1"), "part.parquet", table(&["id"]));
    parquet(&hive.join("day=2"), "part.parquet", table(&["id"]));
    let (mut c, rx_c) = app();
    assert!(
        matches!(named(&mut c, &rx_c, &hive), Some(AppEvent::Open(_, o)) if o.hive),
        "a hive root is read through its partitions"
    );

    // A lake root is not a folder of Parquet files, however much it looks like one.
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(
        delta.join("_delta_log").join("00000000000000000000.json"),
        "{}",
    )
    .unwrap();
    parquet(&delta, "part-00000.parquet", table(&["id"]));
    let (mut d, rx_d) = app();
    assert!(
        named(&mut d, &rx_d, &delta).is_none(),
        "it is not read as Parquet"
    );
    assert_eq!(d.home.browsing.as_deref(), Some(delta.as_path()));
    assert!(
        d.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("Delta")),
        "and it says why: {:?}",
        d.home.status
    );

    // And the read really happens: the whole chain, look and all, is the table.
    let (mut loaded, rx) = app();
    let event = named(&mut loaded, &rx, &one).expect("a folder of one table opens");
    let AppEvent::Open(paths, options) = event else {
        panic!("the rule opens it")
    };
    pump_open_until_loaded(&mut loaded, &rx, paths, options);
    assert_eq!(
        loaded.data_table_state.as_ref().map(|s| s.num_rows),
        Some(2),
        "one row from each file, read as one table"
    );

    // A file is untouched, and not even looked at: it goes straight to the open.
    let (mut e, _rx_e) = app();
    assert!(matches!(
        e.open_the_path_named_on_the_command_line(
            vec![one.join("a.parquet")],
            OpenOptions::default()
        ),
        Some(AppEvent::Open(..))
    ));
    // `--hive` is an answer already given, so it is not second-guessed either.
    let (mut f, _rx_f) = app();
    let forced = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    assert!(
        matches!(
            f.open_the_path_named_on_the_command_line(vec![several.clone()], forced),
            Some(AppEvent::Open(..))
        ),
        "--hive still means read this as one, whatever the folder looks like"
    );

    // And so is `--no-header`. The rule takes each file's first row of data for its
    // column names, finds them all different and calls the folder separate tables — so
    // without this it sends the user to the home screen, for a folder the flag reads
    // perfectly as one table.
    let headless = tmp.path().join("headless");
    std::fs::create_dir_all(&headless).unwrap();
    std::fs::write(headless.join("a.csv"), "alice,30\nbob,25\n").unwrap();
    std::fs::write(headless.join("b.csv"), "carol,41\n").unwrap();
    let (mut g, rx_g) = app();
    let told = OpenOptions {
        has_header: Some(false),
        ..OpenOptions::default()
    };
    assert!(
        named_with(&mut g, &rx_g, &headless, told).is_some(),
        "the user has said how to read these; datui does not judge them by another reading"
    );
}

/// A folder of several formats is read as the commonest, and says what it left out.
///
/// Refusing the whole of a thousand CSVs over one stray JSON was datui deciding that a
/// folder it could read was not worth reading. It reads it now — and a read that
/// silently drops a file is the other half of the same mistake, so the dataset says
/// which formats were passed over and how many of each.
#[test]
fn test_a_mixed_folder_reads_as_the_commonest_format_and_says_what_it_left_out() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path();
    for name in ["a.csv", "b.csv", "c.csv"] {
        std::fs::write(dir.join(name), b"x,y\n1,2\n").unwrap();
    }
    std::fs::write(dir.join("notes.json"), b"{\"x\": 1}").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("the folder opens rather than being refused over the stray");
    assert_eq!(state.num_rows, 3, "one row from each CSV");

    let notes = state.notes();
    let said = notes
        .iter()
        .find(|n| n.summary.contains("more than one format"))
        .unwrap_or_else(|| panic!("the read says what it left out, got {notes:?}"));
    assert!(
        said.summary.contains("1 json"),
        "by format and count: {:?}",
        said.summary
    );
    assert!(
        state.has_notes(),
        "and the Info key offers it, which is the only way anyone finds out"
    );
}

/// A remote row datui has no reader for is named a file, not left Unknown.
///
/// `entry_for_path` had a name and nothing else to go on, and left anything whose
/// extension it did not recognize as `Unknown` — which → enters. So a Recent of
/// `s3://bucket/data.dat` took → into an empty prefix listing with no explanation and
/// Esc as the only way out (#283). Excluding `Unknown` from what → enters was the other
/// way to fix it, and it is the label deciding access one indirection along: before
/// anything has looked into it, every row on a share is `Unknown`, including every
/// folder that costs most to reach. So the row is named instead.
#[test]
fn test_a_remote_name_datui_cannot_read_is_still_a_file_not_a_prefix() {
    let kind = |url: &str| {
        let mut home = datui::home::HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        home.rebuild(&[], &[PathBuf::from(url)]);
        home.sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .find(|r| r.path == Path::new(url))
            .map(|r| r.kind)
            .unwrap_or_else(|| panic!("{url} is listed under RECENT"))
    };

    // An extension datui reads: a file, as it always was.
    assert_eq!(
        kind("s3://bucket/data.psv"),
        datui::discover::EntryKind::File
    );
    // One it does not: still a file. It is certainly not a prefix.
    assert_eq!(
        kind("s3://bucket/data.dat"),
        datui::discover::EntryKind::File,
        "→ must not offer to go inside it"
    );
    // No extension: genuinely ambiguous — it may be a prefix, or a part file written
    // without one — so it stays Unknown and → goes in, which is the trade #279 made.
    assert_eq!(
        kind("s3://bucket/exports"),
        datui::discover::EntryKind::Unknown
    );
    // A trailing slash is a prefix whatever the name has in it.
    assert_eq!(
        kind("s3://bucket/2024.01.15/"),
        datui::discover::EntryKind::Unknown,
        "a dotted prefix is not a file"
    );
}

/// A folder of files written without extensions opens as one table.
///
/// Spark and GBIF both write part files with no extension. `occurrence.parquet/000001`
/// is read by its folder's name; the same files under a folder named anything else were
/// not data at all as far as datui was concerned — nothing listed them and nothing
/// opened them. The bytes say what the names do not, and they are asked once, of the
/// folder somebody is opening, never of a folder somebody is looking at.
#[test]
fn test_a_folder_of_files_written_without_extensions_still_opens() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = DataFrame::new(1, vec![Column::new("id".into(), &[1i32])]).unwrap();
    for name in ["000000", "000001"] {
        ParquetWriter::new(File::create(parts.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    // The names settle nothing, so the folder is a place to look inside.
    assert_eq!(
        datui::discover::classify_directory(&parts),
        datui::discover::EntryKind::Directory,
        "no name in there says data"
    );

    // And the read finds them anyway.
    match datui::discover::folder_format(&parts) {
        datui::discover::FolderFormat::One(format, files) => {
            assert_eq!(format, datui::FileFormat::Parquet);
            assert_eq!(files.len(), 2, "both of them");
        }
        other => panic!("the bytes say Parquet, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![parts.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|s| s.num_rows),
        Some(2),
        "one row from each part"
    );

    // And it is reachable from the home screen: the folder is a place to look inside,
    // and the door inside it reads the whole of what it holds. Two keys, which is the
    // rule for every folder the nesting test turns away — not a dead end, which is what
    // a folder nothing listed and nothing opened was.
    let (tx, _rx) = mpsc::channel();
    let mut home = App::new(tx, common::test_runtime());
    home.enter_home();
    home.home.browsing = Some(parts.clone());
    home.home.rebuild(&[], &[]);
    let door = home
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the folder carries the door");
    home.home.selected = door;
    assert!(
        matches!(home.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens it"
    );

    // A folder whose names do say something is not opened file by file to find out.
    // `LICENSE` beside the Parquet is not sniffed, and not read.
    let named = tmp.path().join("named");
    std::fs::create_dir_all(&named).unwrap();
    ParquetWriter::new(File::create(named.join("a.parquet")).unwrap())
        .finish(&mut frame)
        .unwrap();
    std::fs::write(named.join("LICENSE"), b"MIT").unwrap();
    match datui::discover::folder_format(&named) {
        datui::discover::FolderFormat::One(datui::FileFormat::Parquet, files) => {
            assert_eq!(files.len(), 1, "the LICENSE is not one of them");
        }
        other => panic!("the names settled it, got {other:?}"),
    }
}

/// The nesting rule reaches the formats that have no footer.
///
/// A folder of forty unrelated CSVs was labelled `40 csv`, `Enter` promised one table
/// because nothing had looked, and the read then refused it — the permissive rule with
/// the strict reader, which is the pairing #275 exists to stop. The names at the front
/// of a CSV are the same evidence `is_nested` takes from a Parquet footer, so the same
/// rule now answers for both.
#[test]
fn test_a_folder_of_csv_is_judged_by_its_headers_like_one_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let folder = |name: &str, files: &[&str]| {
        let dir = tmp.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, body) in files.iter().enumerate() {
            std::fs::write(dir.join(format!("part-{i}.csv")), body).unwrap();
        }
        dir
    };
    let looked_at = |dir: &Path| {
        let mut entry = datui::discover::Entry::directory(dir);
        entry.kind = datui::discover::EntryKind::Unknown;
        datui::home::look_into(&entry)
    };

    // Files that agree: one table, as before.
    let same = folder("same", &["a,b\n1,2\n", "a,b\n3,4\n", "a,b\n5,6\n"]);
    assert_eq!(
        looked_at(&same).kind,
        datui::discover::EntryKind::MultiFile,
        "identical headers are one table"
    );

    // A column added along the way: still one table. This is the shape the rule is for,
    // and the one a stricter test would refuse.
    let drift = folder(
        "drift",
        &["id,ts\n1,5\n", "id,ts\n2,6\n", "id,ts,region\n3,7,eu\n"],
    );
    assert_eq!(
        looked_at(&drift).kind,
        datui::discover::EntryKind::MultiFile,
        "a column added later is schema drift, not separate tables"
    );

    // Separate tables: somewhere to look inside, not one table.
    let apart = folder("apart", &["a,b\n1,2\n", "x,y,z\n3,4,5\n", "q\n9\n"]);
    let judged = looked_at(&apart);
    assert_eq!(
        judged.kind,
        datui::discover::EntryKind::Directory,
        "files that each bring something the others lack are not one table"
    );
    assert_eq!(
        judged.rows, None,
        "and no row count, which would be a sum of unrelated things"
    );
    assert!(
        judged.columns.iter().any(|c| c == "q"),
        "but the union of columns, so a column search still finds the folder: {:?}",
        judged.columns
    );

    // A headerless file gives its first row of *data* as the names, because datui
    // reads a CSV as having a header. datui cannot read such a folder as one table at
    // all without `--no-header`: every file would contribute its own first row as
    // column names and the union would be a wide sheet of nulls. So it is not offered
    // as one — and the data values are not offered as column names either.
    let headless = folder("headless", &["1,2\n3,4\n", "5,6\n", "7,8\n"]);
    let judged = looked_at(&headless);
    assert_eq!(
        judged.kind,
        datui::discover::EntryKind::Directory,
        "a folder datui can only read as nulls is not one table"
    );
    assert!(
        judged.columns.is_empty(),
        "and data values are not offered as column names: {:?}",
        judged.columns
    );
    // Opened anyway, through the door, it says what it is seeing and what to do.
    let said = datui::notes::from_the_open(
        &[],
        None,
        datui::schema_union::Disagreement {
            headerless: true,
            ..Default::default()
        },
    );
    assert!(
        said.iter()
            .any(|n| n.summary.contains("no header row") && n.summary.contains("--no-header")),
        "got {said:?}"
    );

    // Compression does not hide the header: it is still at the front of the file.
    let zipped = tmp.path().join("zipped");
    std::fs::create_dir_all(&zipped).unwrap();
    for (name, body) in [("a.csv.gz", "a,b\n1,2\n"), ("b.csv.gz", "x,y,z\n3,4,5\n")] {
        let file = File::create(zipped.join(name)).unwrap();
        let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        std::io::Write::write_all(&mut gz, body.as_bytes()).unwrap();
        gz.finish().unwrap();
    }
    assert_eq!(
        looked_at(&zipped).kind,
        datui::discover::EntryKind::Directory,
        "a gzipped CSV is judged by its header like any other"
    );

    // Two files are the fewest that can disagree; one decides nothing.
    let alone = folder("alone", &["a,b\n1,2\n"]);
    assert_eq!(
        looked_at(&alone).kind,
        datui::discover::EntryKind::Directory,
        "a folder of one data file was never a multi-file dataset"
    );
}

/// The control bar says what Enter will really do, on a row of every shape.
///
/// `WhatEnter` is a prediction the renderer reads and `home_open_selected` is the thing
/// that decides, so the two can drift. This is what stops them: one row of each shape,
/// Enter pressed on it, and the prediction checked against what actually happened.
#[test]
fn test_the_bar_says_what_enter_will_really_do() {
    let tmp = tempfile::TempDir::new().unwrap();
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };

    // One table across two files; separate tables; a lake root; and a plain file.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let apart = tmp.path().join("apart");
    parquet(&apart, "by_block.parquet", table(&["block", "fee"]));
    parquet(&apart, "daily.parquet", table(&["day", "price"]));
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(delta.join("_delta_log").join("0.json"), "{}").unwrap();
    parquet(&delta, "part-0.parquet", table(&["id"]));
    parquet(tmp.path(), "loose.parquet", table(&["id"]));

    // Each row, classified the way the background pass would, then Enter pressed on it.
    for (name, expected) in [
        ("one", datui::WhatEnter::OpensFolder),
        ("apart", datui::WhatEnter::GoesInside),
        ("delta", datui::WhatEnter::GoesInside),
        ("loose.parquet", datui::WhatEnter::OpensFile),
    ] {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(tmp.path().to_path_buf());
        app.home.rebuild(&[], &[]);
        app.home.measure_now(16);
        app.home.classify_now(16);
        app.home.rebuild(&[], &[]);

        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;

        let predicted = app.what_enter_does();
        assert_eq!(predicted, expected, "prediction for {name}");

        let was = app.home.browsing.clone();
        let opened = matches!(app.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..)));
        let went_inside = app.home.browsing != was;
        match expected {
            datui::WhatEnter::OpensFolder | datui::WhatEnter::OpensFile => assert!(
                opened && !went_inside,
                "{name}: the bar promised an open and Enter did {opened}/{went_inside}"
            ),
            datui::WhatEnter::GoesInside => assert!(
                went_inside && !opened,
                "{name}: the bar promised to go inside and Enter did {opened}/{went_inside}"
            ),
            _ => unreachable!("no other shape is asserted here"),
        }
    }

    // The shapes that are not entries at all. Each does something different and each
    // said "Open" before, which is the wrong first impression on three more rows.
    let (tx, _rx) = mpsc::channel();
    let mut other = App::new(tx, common::test_runtime());
    other.enter_home();
    other.home.browsing = Some(tmp.path().to_path_buf());
    other.home.rebuild(&[], &[]);
    let at = |app: &mut App, want: fn(&datui::home::Row) -> bool| {
        app.home.visible().iter().position(want)
    };
    if let Some(i) = at(&mut other, |r| matches!(r, datui::home::Row::Header { .. })) {
        other.home.selected = i;
        assert_eq!(
            other.what_enter_does(),
            datui::WhatEnter::FoldsSection,
            "Enter folds a section header; it does not open anything"
        );
    }

    // And the door, which reads whatever it is standing in.
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(apart.clone());
    app.home.rebuild(&[], &[]);
    let door = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the folder carries the door");
    app.home.selected = door;
    assert_eq!(app.what_enter_does(), datui::WhatEnter::OpensFolder);
    assert!(matches!(
        app.event(&key(KeyCode::Enter)),
        Some(AppEvent::Open(..))
    ));
}

/// A place row under `RECENT` gets the verb its key actually has.
///
/// Enter browses into the place, which is what → does on it too. The bar read
/// `Enter Open … → Inside`: the wrong verb, plus the two-chips-for-one-outcome the
/// labelling exists to remove. Neither the key-pumping test nor the bar's own tests
/// covered a place row, because both were written over entries.
#[test]
fn test_a_place_row_says_inside_and_says_it_once() {
    let tmp = tempfile::TempDir::new().unwrap();
    let held = tmp.path().join("exports");
    std::fs::create_dir_all(&held).unwrap();
    let file = held.join("sales.csv");
    std::fs::write(&file, "a,b\n1,2\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], std::slice::from_ref(&file));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { .. }))
        .expect("a recent under a place row");
    app.home.selected = row;

    assert_eq!(
        app.what_enter_does(),
        datui::WhatEnter::GoesInside,
        "Enter browses the place, which is what → does"
    );

    // And Enter really does browse, so the label is not a guess.
    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(held.as_path()));
}

/// The pane does not point at a door that will not be there.
///
/// `whole_folder_row` gives no `(all files)` row to a folder with nothing in it, nor
/// to any folder while a filter is typed — and the pane said "the first row in there
/// reads the whole folder as one table" for every plain directory regardless.
#[test]
fn test_the_pane_only_promises_a_door_that_exists() {
    let tmp = tempfile::TempDir::new().unwrap();
    let empty = tmp.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let full = tmp.path().join("full");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("a.csv"), "a,b\n1,2\n").unwrap();
    std::fs::write(full.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let pane = |app: &mut App, name: &str| {
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;
        let area = ratatui::layout::Rect::new(0, 0, 120, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
        // The pane wraps and pads, so a sentence spans rows with a border and a run of
        // spaces in the middle. Flattened to single spaces so the text can be looked
        // for as it reads.
        let raw = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join(" ");
        raw.replace('│', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    app.home.measure_now(16);
    app.home.classify_now(16);
    app.home.rebuild(&[], &[]);

    let shown = pane(&mut app, "full");
    assert!(
        shown.contains("first row in there"),
        "a folder with something in it has the door to point at: {shown}"
    );
    assert!(
        !pane(&mut app, "empty").contains("first row in there"),
        "an empty folder has none, so nothing points at one"
    );
}

/// A read that widened a column's type says so, and one with no rule behind it does
/// not widen at all.
///
/// `to_supertypes` was added for exactly this case — one `N/A` makes `amount` a String
/// in one file and an Int64 in the next — and the note was written from the column
/// *names*, which agree. So the folder opened with `amount` silently text for every
/// row, where before it had failed loudly.
#[test]
fn test_widening_a_column_is_never_silent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let drifted = tmp.path().join("drifted");
    std::fs::create_dir_all(&drifted).unwrap();
    std::fs::write(drifted.join("a.csv"), "id,amount\n1,10\n").unwrap();
    std::fs::write(drifted.join("b.csv"), "id,amount\n2,N/A\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![drifted.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let state = app.data_table_state.as_ref().expect("it opens");
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|n| n.summary.contains("more than one type")),
        "the widening is reported: {notes:?}"
    );
    assert!(
        !notes.iter().any(|n| n.summary.contains("same columns")),
        "and not as a disagreement about columns, which these files do not have: {notes:?}"
    );

    // The names agreeing is what made this invisible, so assert they do.
    assert_eq!(state.headers(), vec!["id", "amount"]);
}

/// Naming a folder on the command line draws a frame before it reads anything.
///
/// Looking at a folder reads footers, or the front of a spread of its files, and for a
/// folder of large Parquet that is seconds — 4.6 of them on a real one, and seventeen
/// on one with a deep subtree. It used to happen in `run()` before the first
/// `terminal.draw`, so the whole of it was a blank terminal: no name, no spinner, and
/// no key that worked. The look is an event now, carried out on a worker after the
/// first frame.
#[test]
fn test_looking_at_a_folder_happens_after_the_first_frame() {
    let tmp = tempfile::TempDir::new().unwrap();
    let folder = tmp.path().join("data");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(folder.join("a.csv"), "a,b\n1,2\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let event = app
        .open_the_path_named_on_the_command_line(vec![folder.clone()], OpenOptions::default())
        .expect("a folder is something to act on");

    // The call hands back work to do rather than having done it. Nothing has been
    // decided yet: no home screen, no load.
    match &event {
        AppEvent::LookThenOpenFolder(dir, _) => assert_eq!(dir, &folder),
        _ => panic!("the folder is looked at on a worker, not on the way to the first frame"),
    }
    assert!(
        app.home.browsing.is_none() && app.data_table_state.is_none(),
        "and the look has not run yet, so nothing has been opened or browsed into"
    );

    // A file is not looked at at all — there is nothing to find out — so it keeps
    // going straight to the open and pays for no frame.
    let (tx, _rx) = mpsc::channel();
    let mut on_a_file = App::new(tx, common::test_runtime());
    assert!(matches!(
        on_a_file.open_the_path_named_on_the_command_line(
            vec![folder.join("a.csv")],
            OpenOptions::default()
        ),
        Some(AppEvent::Open(..))
    ));
}

/// Going home while a folder is being looked at is not undone when the look lands.
///
/// The look can take seconds, and Ctrl+O works throughout — which is the point of
/// moving it off the startup thread. So the user can be somewhere else by the time it
/// answers, and the answer must not take them back.
#[test]
fn test_a_look_that_lands_after_the_user_left_is_dropped() {
    let tmp = tempfile::TempDir::new().unwrap();
    let folder = tmp.path().join("one");
    std::fs::create_dir_all(&folder).unwrap();
    for name in ["a.csv", "b.csv"] {
        std::fs::write(folder.join(name), "id,ts\n1,2\n").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let event = app
        .open_the_path_named_on_the_command_line(vec![folder.clone()], OpenOptions::default())
        .expect("a folder is looked at");
    app.event(&event);

    // Ctrl+O while the look is out: the user is at the home screen now.
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    // The look lands. It must find nothing waiting for it.
    let mut landed = None;
    while let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(4000)) {
        if matches!(ev, AppEvent::FolderLookedAt { .. }) {
            landed = app.event(&ev);
            break;
        }
    }
    assert!(
        landed.is_none(),
        "the answer to a question the user walked away from does not open anything"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "and does not take them off the screen they chose"
    );
    assert!(
        app.home.browsing.is_none(),
        "nor browse them into the folder they left: {:?}",
        app.home.browsing
    );
}

/// A setting that agrees with what the rule assumed is not a reason to stop asking it.
///
/// `has_header` and the skips reach `OpenOptions` from the config file as well as the
/// command line, so a guard over "did anyone set this" is true on every run for anyone
/// with `has_header = true` in `~/.config/datui/config.toml` — and every folder they
/// name is then forced down the one-table route, `datui .` included. The question is
/// whether the header is somewhere other than where the rule looked.
#[test]
fn test_a_setting_that_agrees_with_the_rule_changes_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    std::fs::write(apart.join("a.csv"), "id,ts\n1,2\n").unwrap();
    std::fs::write(apart.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let settle = |options: OpenOptions| {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = app.open_the_path_named_on_the_command_line(vec![apart.clone()], options);
        loop {
            if app.home.browsing.is_some() {
                return None;
            }
            match next.take() {
                Some(AppEvent::Open(paths, options)) => return Some((paths, options)),
                Some(ev) => next = app.event(&ev),
                None => match rx.recv_timeout(std::time::Duration::from_millis(4000)) {
                    Ok(ev) => next = Some(ev),
                    Err(_) => panic!("the chain stopped without settling"),
                },
            }
        }
    };

    // A header where one is expected, and a skip of nothing: the same thing the rule
    // assumed, so the folder of separate tables is still somewhere to look inside.
    for agrees in [
        OpenOptions {
            has_header: Some(true),
            ..OpenOptions::default()
        },
        OpenOptions {
            skip_rows: Some(0),
            skip_lines: Some(0),
            ..OpenOptions::default()
        },
    ] {
        assert!(
            settle(agrees).is_none(),
            "a setting the rule already assumed does not force the one-table route"
        );
    }

    // Moving the header does change it — not by overriding the rule, but because the
    // rule now reads the files the way the open will: headerless, every file's columns
    // are `column_1..N` and the narrower nests inside the wider.
    assert!(
        settle(OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        })
        .is_some(),
        "read as headerless, these files are one table"
    );
}

/// A CSV setting does not decide a folder of Parquet.
///
/// `has_header` and the skips are reader settings for delimited text. Nothing about
/// them can change how a Parquet file is read, so a folder of Parquet must reach the
/// same verdict whatever they say — which it does because the rule reads Parquet
/// through its footers and the settings never touch that path.
#[test]
fn test_a_csv_setting_does_not_decide_a_folder_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    for (name, cols) in [
        ("by_block.parquet", vec!["block", "fee"]),
        ("daily.parquet", vec!["day", "price", "volume"]),
    ] {
        let mut frame = DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap();
        ParquetWriter::new(File::create(apart.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    for options in [
        OpenOptions::default(),
        OpenOptions {
            has_header: Some(false),
            skip_rows: Some(3),
            ..OpenOptions::default()
        },
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = app.open_the_path_named_on_the_command_line(vec![apart.clone()], options);
        loop {
            if app.home.browsing.is_some() {
                break;
            }
            match next.take() {
                Some(AppEvent::Open(..)) => {
                    panic!("a CSV setting cannot make two Parquet tables into one")
                }
                Some(ev) => next = app.event(&ev),
                None => match rx.recv_timeout(std::time::Duration::from_millis(4000)) {
                    Ok(ev) => next = Some(ev),
                    Err(_) => panic!("the chain stopped without settling"),
                },
            }
        }
        assert_eq!(app.home.browsing.as_deref(), Some(apart.as_path()));
    }
}

/// A folder with no data files in it is never forced down the one-table route.
///
/// `EntryKind::Directory` covers a folder of separate tables *and* one with nothing
/// readable in it. Opening the second as one table reaches the Parquet hive scan on a
/// tree that has no Parquet, so `datui ~/src --no-header` ended in an error modal
/// rather than the home screen it used to give.
#[test]
fn test_a_folder_with_no_data_is_not_forced_open() {
    let tmp = tempfile::TempDir::new().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::write(src.join("main.rs"), "fn main() {}\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut next = app.open_the_path_named_on_the_command_line(
        vec![src.clone()],
        OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        },
    );
    loop {
        if app.home.browsing.is_some() {
            break;
        }
        match next.take() {
            Some(AppEvent::Open(..)) => {
                panic!("a folder with nothing readable in it has no table to open")
            }
            Some(ev) => next = app.event(&ev),
            None => match rx.recv_timeout(std::time::Duration::from_millis(4000)) {
                Ok(ev) => next = Some(ev),
                Err(_) => panic!("the chain stopped without settling"),
            },
        }
    }
    assert_eq!(app.home.browsing.as_deref(), Some(src.as_path()));
}

/// Options as the binary builds them from these arguments and this config text.
fn options_as_the_binary_does(argv: &[&str], config: &str) -> OpenOptions {
    use clap::Parser;
    let args = datui_cli::Args::try_parse_from(argv).expect("parses");
    let config: datui::config::AppConfig = toml::from_str(config).expect("config parses");
    OpenOptions::from_args_and_config(&args, &config)
}

fn open_and_collect(paths: Vec<PathBuf>, options: OpenOptions) -> (App, DataFrame) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, paths, options);
    let df = app
        .data_table_state
        .as_ref()
        .expect("the file opened")
        .lf
        .clone()
        .collect()
        .unwrap();
    (app, df)
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

/// `--delimiter` is what the file is read with (#290), on every CSV route: one file,
/// several, and a compressed one read both lazily and in memory.
#[test]
fn test_delimiter_flag_splits_the_columns() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let body = "id|name|city\n1|ann|oslo\n2|bob|rome\n";
    let one = tmp.path().join("one.csv");
    let two = tmp.path().join("two.csv");
    std::fs::write(&one, body).unwrap();
    std::fs::write(&two, body).unwrap();
    let gz = tmp.path().join("zipped.csv.gz");
    let mut enc =
        flate2::write::GzEncoder::new(File::create(&gz).unwrap(), flate2::Compression::default());
    std::io::Write::write_all(&mut enc, body.as_bytes()).unwrap();
    enc.finish().unwrap();

    let (_, df) = open_and_collect(
        vec![one.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["id|name|city"], "without the flag: one column");

    let with_flag = |extra: &[&str]| {
        let mut argv = vec!["datui", "x", "--delimiter", "124"];
        argv.extend_from_slice(extra);
        options_as_the_binary_does(&argv, "")
    };
    for (what, paths, opts) in [
        ("one file", vec![one.clone()], with_flag(&[])),
        ("two files", vec![one.clone(), two.clone()], with_flag(&[])),
        ("gzip, lazily", vec![gz.clone()], with_flag(&[])),
        (
            "gzip, in memory",
            vec![gz.clone()],
            with_flag(&["--decompress-in-memory", "true"]),
        ),
    ] {
        let rows = 2 * paths.len();
        let (_, df) = open_and_collect(paths, opts);
        assert_eq!(names(&df), ["id", "name", "city"], "{what}");
        assert_eq!(df.height(), rows, "{what}");
    }
}

/// The flag outranks the separator a `.tsv` implies, and export offers it. Without the
/// flag export offers a comma, not the tab: a `.tsv` exports as CSV, to a `.csv` by
/// default, and a tab there reopens as one column.
#[test]
fn test_delimiter_flag_overrides_the_format_and_reaches_export() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let tsv = tmp.path().join("data.tsv");
    std::fs::write(&tsv, "a;b\tc\n1;2\t3\n").unwrap();

    let export_default = |app: &mut App| {
        app.event(&key(KeyCode::Char('e')));
        app.export_modal.csv_delimiter_input.value().to_string()
    };

    let (mut app, df) = open_and_collect(
        vec![tsv.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["a;b", "c"]);
    assert_eq!(export_default(&mut app), ",");

    let (mut app, df) = open_and_collect(
        vec![tsv],
        options_as_the_binary_does(&["datui", "x", "--delimiter", "59"], ""),
    );
    assert_eq!(names(&df), ["a", "b\tc"]);
    assert_eq!(export_default(&mut app), ";");
}

/// A file's layout in `[file_loading]` no longer reaches the open (#289). It used to
/// apply to every file, and `skip_rows = 2` turned this one's third row into its header.
#[test]
fn test_layout_keys_in_config_do_not_reach_the_open() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let csv = tmp.path().join("plain.csv");
    std::fs::write(&csv, "id,name\n1,ann\n2,bob\n3,cid\n").unwrap();
    let config = "[file_loading]\n\
                  delimiter = 59\n\
                  has_header = false\n\
                  skip_lines = 1\n\
                  skip_rows = 2\n\
                  skip_tail_rows = 1\n";

    let opts = options_as_the_binary_does(&["datui", "x"], config);
    assert_eq!(opts.delimiter, None);
    assert_eq!(opts.has_header, None);
    assert_eq!(opts.skip_lines, None);
    assert_eq!(opts.skip_rows, None);
    assert_eq!(opts.skip_tail_rows, None);

    let (_, df) = open_and_collect(vec![csv], opts);
    assert_eq!(names(&df), ["id", "name"]);
    assert_eq!(df.height(), 3);
}

/// TSV and PSV are read by the CSV reader, so the CSV options mean the same for them:
/// trimmed names, null values, the tail skip. They used to honor only the header and
/// the leading skips.
#[test]
fn test_tsv_and_psv_take_every_csv_option() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for (name, sep) in [("data.tsv", "\t"), ("data.psv", "|")] {
        let path = tmp.path().join(name);
        let body = format!("id{sep} name\n1{sep}NA\n2{sep}bob\n3{sep}FOOTER\n");
        std::fs::write(&path, body).unwrap();
        let opts = options_as_the_binary_does(
            &["datui", "x", "--null-value", "NA", "--skip-tail-rows", "1"],
            "",
        );
        let (_, df) = open_and_collect(vec![path], opts);
        assert_eq!(names(&df), ["id", "name"], "{name}");
        assert_eq!(df.height(), 2, "{name}");
        assert_eq!(df.column("name").unwrap().null_count(), 1, "{name}");
    }
}

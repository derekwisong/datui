use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::fs::File;
use std::path::PathBuf;
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

/// Renders the app in chart view to exercise the chart cache path (no x/y selected, then with x+y).
/// Ensures the chart render path does not panic and cache logic works.
#[test]
fn test_chart_view_render_with_cache() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("chart_render_cache_test.csv");

    let mut df = df!(
        "x" => (0..5).collect::<Vec<i32>>(),
        "y" => (0..5).map(|i| i * 3).collect::<Vec<i32>>()
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

    // Open chart view (no x/y selected yet)
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);

    // Render in chart view with no series (exercises cache path; xy_series and x_bounds stay None)
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);

    // Select x and y via modal state so chart data is computed and cached
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];

    // Render again: should use or populate chart cache (XY series)
    Widget::render(&mut app, area, &mut buf);

    // Close chart (clears cache)
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn test_open_s3_url_returns_crash_or_loads() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("s3://my-bucket/path/to/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => {
            assert!(m.contains("S3"), "error should mention S3: {}", m);
        }
        Some(AppEvent::DoLoadSchema(..)) => {
            // With cloud feature and valid credentials/bucket, load can succeed.
        }
        _ => panic!("expected Crash or DoLoadSchema when opening S3 URL"),
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
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("gs://my-bucket/path/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => {
            assert!(
                m.contains("GCS") || m.contains("gs://") || m.contains("not enabled"),
                "error should mention GCS or gs:// or not enabled: {}",
                m
            );
        }
        Some(AppEvent::DoLoadSchema(..)) => {}
        _ => panic!("expected Crash or DoLoadSchema when opening gs:// URL"),
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

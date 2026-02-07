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

/// Pumps the load event chain (Open -> DoLoadScanPaths -> DoLoadSchema -> DoLoadSchemaBlocking -> DoLoadBuffer -> Collect) until no event is returned.
fn pump_open_until_loaded(app: &mut App, paths: Vec<PathBuf>, options: OpenOptions) {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
        if matches!(ev, AppEvent::Crash(_)) {
            return;
        }
    }
}

#[test]
fn test_app_creation() {
    let (tx, _) = mpsc::channel();
    let app = App::new(tx);
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn test_full_workflow() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);

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
    pump_open_until_loaded(&mut app, vec![csv_path.clone()], OpenOptions::default());

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
    assert!(!app.sort_filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    let df = datatable.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

#[test]
fn test_chart_open_and_esc_back() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);

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

    pump_open_until_loaded(&mut app, vec![csv_path.clone()], OpenOptions::default());
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
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);

    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("chart_q_test.csv");

    let mut df = df!("a" => &[1_i32], "b" => &[2_i32]).unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    pump_open_until_loaded(&mut app, vec![csv_path], OpenOptions::default());
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
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);

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

    pump_open_until_loaded(&mut app, vec![csv_path.clone()], OpenOptions::default());
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
    let mut app = App::new(tx);
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
    let mut app = App::new(tx);
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
    let mut app = App::new(tx);
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
    let mut app = App::new(tx2);
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
    let mut app = App::new(tx);
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

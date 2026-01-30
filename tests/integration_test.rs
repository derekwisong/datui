use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

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

    // 2. Open the file
    let event = AppEvent::Open(vec![csv_path.clone()], OpenOptions::default());
    if let Some(next_event) = app.event(&event) {
        if let Some(collect_event) = app.event(&next_event) {
            app.event(&collect_event);
        }
    }

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

    let event = AppEvent::Open(vec![csv_path.clone()], OpenOptions::default());
    if let Some(next_event) = app.event(&event) {
        if let Some(collect_event) = app.event(&next_event) {
            app.event(&collect_event);
        }
    }
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

    let event = AppEvent::Open(vec![csv_path], OpenOptions::default());
    if let Some(next_event) = app.event(&event) {
        if let Some(collect_event) = app.event(&next_event) {
            app.event(&collect_event);
        }
    }
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

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
    let event = AppEvent::Open(csv_path.clone(), OpenOptions::default());
    if let Some(next_event) = app.event(&event) {
        if let Some(collect_event) = app.event(&next_event) {
            app.event(&collect_event);
        }
    }

    assert!(app.data_table_state.is_some());
    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.num_rows, 100);

    // 2. Filter the data
    let key_event = KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event)); // open filter modal
    assert!(app.filter_modal.active);

    app.filter_modal.available_columns = app.data_table_state.as_ref().unwrap().headers();
    app.filter_modal.new_column_idx = 2; // column "c"
    app.filter_modal.new_operator_idx = 0; // operator "="
    app.filter_modal.new_value = "1".to_string();
    app.filter_modal.add_statement();

    // Press enter on "Confirm" button
    app.filter_modal.focus = datui::filter_modal::FilterFocus::Confirm;
    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    assert!(!app.filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.lf.clone().collect().unwrap().shape().0, 33);

    // 3. Sort the data
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event)); // open sort modal
    assert!(app.sort_modal.active);

    app.sort_modal.columns = app
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
    app.sort_modal.table_state.select(Some(0)); // column "a"
    app.sort_modal.toggle_selection();
    app.sort_modal.ascending = false;

    app.sort_modal.focus = datui::sort_modal::SortFocus::Apply;
    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    assert!(!app.sort_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    let df = datatable.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

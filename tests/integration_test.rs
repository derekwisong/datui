use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
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

    // 1. Open a file
    let path = common::create_large_test_csv();
    let event = AppEvent::Open(path.to_path_buf(), OpenOptions::default());
    if let Some(next_event) = app.event(&event) {
        app.event(&next_event);
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

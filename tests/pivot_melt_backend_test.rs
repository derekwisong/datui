//! Phase 3: Backend and infrastructure tests for Pivot/Melt.
//! No modal UI; uses AppEvent::Pivot / AppEvent::Melt with hardcoded specs.
//! Phase 6: UI-level tests (open modal, Apply, Esc cancel).

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use datui::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotMeltFocus, PivotSpec};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::AnyValue;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

fn ensure_sample_data() {
    common::ensure_sample_data();
}

fn load_file(app: &mut App, path: PathBuf) {
    let event = AppEvent::Open(path, OpenOptions::default());
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
}

#[test]
fn test_pivot_via_events() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, path);

    assert!(app.data_table_state.is_some());
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: false,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
    assert!(df.height() > 0);
}

#[test]
fn test_pivot_long_string_via_events() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/pivot_long_string.parquet");
    load_file(&mut app, path);

    assert!(app.data_table_state.is_some());
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: false,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"X"));
    assert!(names.contains(&"Y"));
    assert!(names.contains(&"Z"));
}

#[test]
fn test_melt_via_events() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/melt_wide.parquet");
    load_file(&mut app, path);

    assert!(app.data_table_state.is_some());
    let cols = app.data_table_state.as_ref().unwrap().schema.iter_names();
    let all: Vec<String> = cols.map(|s| s.to_string()).collect();
    let index = vec!["id".to_string(), "date".to_string()];
    let value_columns: Vec<String> = all
        .iter()
        .filter(|c| *c != "id" && *c != "date")
        .cloned()
        .collect();
    let spec = MeltSpec {
        index,
        value_columns,
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    let event = AppEvent::Melt(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"variable"));
    assert!(names.contains(&"value"));
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(df.height() > 0);
}

#[test]
fn test_melt_wide_many_via_events() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/melt_wide_many.parquet");
    load_file(&mut app, path);

    assert!(app.data_table_state.is_some());
    let cols = app.data_table_state.as_ref().unwrap().schema.iter_names();
    let all: Vec<String> = cols.map(|s| s.to_string()).collect();
    let index = vec!["id".to_string(), "date".to_string()];
    let value_columns: Vec<String> = all
        .iter()
        .filter(|c| *c != "id" && *c != "date")
        .cloned()
        .collect();
    let spec = MeltSpec {
        index,
        value_columns,
        variable_name: "var".to_string(),
        value_name: "val".to_string(),
    };
    let event = AppEvent::Melt(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert!(df.column("var").is_ok());
    assert!(df.column("val").is_ok());
    assert!(df.height() > 0);
}

#[test]
fn test_pivot_on_current_view_after_filter() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, path);

    let raw_count = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap()
        .height();

    let statements = vec![FilterStatement {
        column: "id".to_string(),
        operator: FilterOperator::Eq,
        value: "5".to_string(),
        logical_op: LogicalOperator::And,
    }];
    let _ = app.event(&AppEvent::Filter(statements));

    let filtered_count = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap()
        .height();
    assert!(
        filtered_count < raw_count,
        "filter should reduce rows: raw={}, filtered={}",
        raw_count,
        filtered_count
    );

    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: false,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert!(
        df.height() <= filtered_count,
        "pivoted rows should be <= filtered count (current-view invariant)"
    );
    let id_col = df.column("id").unwrap();
    for i in 0..df.height() {
        let v = id_col.get(i).unwrap();
        match v {
            AnyValue::Int32(n) => {
                assert_eq!(n, 5, "all rows must have id=5 (pivot on filtered view)")
            }
            AnyValue::Int64(n) => {
                assert_eq!(n, 5, "all rows must have id=5 (pivot on filtered view)")
            }
            _ => panic!("id column should be int"),
        }
    }
}

fn send_key(app: &mut App, code: KeyCode) {
    let ev = AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }
}

#[test]
fn test_esc_cancels_pivot_melt_without_change() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, path);

    let rows_before = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap()
        .height();

    send_key(&mut app, KeyCode::Char('p'));
    assert_eq!(app.input_mode, InputMode::PivotMelt);
    assert!(app.pivot_melt_modal.active);

    send_key(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.pivot_melt_modal.active);

    let rows_after = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf
        .clone()
        .collect()
        .unwrap()
        .height();
    assert_eq!(rows_before, rows_after, "Esc must not change table");
}

#[test]
fn test_pivot_via_modal_apply() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, path);

    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    app.pivot_melt_modal.index_columns = vec!["id".to_string(), "date".to_string()];
    app.pivot_melt_modal.pivot_column = Some("key".to_string());
    app.pivot_melt_modal.value_column = Some("value".to_string());
    app.pivot_melt_modal.aggregation_idx = 0;
    app.pivot_melt_modal.focus = PivotMeltFocus::Apply;

    let ev = AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }

    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
}

#[test]
fn test_melt_via_modal_apply() {
    ensure_sample_data();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx);
    let path = PathBuf::from("tests/sample-data/melt_wide.parquet");
    load_file(&mut app, path);

    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    app.pivot_melt_modal.switch_tab();
    app.pivot_melt_modal.melt_index_columns = vec!["id".to_string(), "date".to_string()];
    app.pivot_melt_modal.melt_value_strategy =
        datui::pivot_melt_modal::MeltValueStrategy::AllExceptIndex;
    app.pivot_melt_modal.melt_variable_name = "variable".to_string();
    app.pivot_melt_modal.melt_value_name = "value".to_string();
    app.pivot_melt_modal.focus = PivotMeltFocus::Apply;

    let ev = AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }

    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"variable"));
    assert!(names.contains(&"value"));
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(df.height() > 0);
}

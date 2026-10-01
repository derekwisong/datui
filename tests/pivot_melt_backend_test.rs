//! Phase 3: Backend and infrastructure tests for Pivot and Melt.
//! No modal UI; uses AppEvent::Pivot / AppEvent::Melt with hardcoded specs.
//! Phase 6: UI-level tests (open modal, Apply, Esc cancel).

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use datui::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotMeltFocus, PivotSpec};
use datui::template::MatchCriteria;
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::AnyValue;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

use common::{drain_events, pump_open_until_loaded};

fn ensure_sample_data() {
    common::ensure_sample_data();
}

fn load_file_with(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    path: PathBuf,
    opts: OpenOptions,
) {
    pump_open_until_loaded(app, rx, vec![path], opts);
}

fn load_file(app: &mut App, rx: &std::sync::mpsc::Receiver<AppEvent>, path: PathBuf) {
    load_file_with(app, rx, path, OpenOptions::default());
}

/// TUI-like collect sequence before pivot: DoLoadBuffer → Collect → set visible_rows → collect.
fn simulate_initial_tui_collects(app: &mut App, terminal_height: usize) {
    let state = match app.data_table_state.as_mut() {
        Some(s) => s,
        None => return,
    };
    state.collect();
    state.collect();
    state.visible_rows = terminal_height;
    state.collect();
}

const SIMULATE_TUI_INITIAL_COLLECTS: bool = true;

/// Polars 0.52's eager pivot panicked when the index column was Date (from_physical UInt32);
/// the lazy pivot in 0.55 does not, and `workaround_pivot_date_index` is now a no-op.

#[test]
fn test_pivot_via_events() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    assert!(app.data_table_state.is_some());

    if SIMULATE_TUI_INITIAL_COLLECTS {
        simulate_initial_tui_collects(&mut app, 40);
    } else if let Some(state) = app.data_table_state.as_mut() {
        state.visible_rows = 40;
        state.collect();
    }

    let spec = PivotSpec {
        index: vec!["date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names, vec!["date", "A", "B", "C"]);
    assert_eq!(df.height(), 31);
}

/// Same render path as UI: visible slice (display_slice_df) then cell formatting (get/str_value).
/// With workaround off, pivot can panic inside Polars (restore_logical_type); test still exercises the path.
#[test]
fn test_pivot_date_index_render_simulation() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file_with(
        &mut app,
        &rx,
        path,
        OpenOptions::default().with_workaround_pivot_date_index(false),
    );
    let state = app.data_table_state.as_mut().unwrap();
    state.visible_rows = 40;
    state.collect();
    let spec = PivotSpec {
        index: vec!["date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let mut next = app.event(&AppEvent::Pivot(spec));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    // Drain async collect events from background buffer load.
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    let sliced_df = state
        .display_slice_df()
        .expect("buffer populated after Collect");
    assert_eq!(sliced_df.get_column_names(), vec!["date", "A", "B", "C"]);
    assert_eq!(sliced_df.height(), 31);
    let (height, cols) = sliced_df.shape();
    for col_index in 0..cols {
        let col_data = &sliced_df[col_index];
        for row_index in 0..height {
            let value = col_data.get(row_index).unwrap();
            let _ = if matches!(value, AnyValue::Null) {
                String::new()
            } else {
                value.str_value().to_string()
            };
        }
    }
}

#[test]
fn test_pivot_long_string_via_events() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long_string.parquet");
    load_file(&mut app, &rx, path);

    assert!(app.data_table_state.is_some());
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"X"));
    assert!(names.contains(&"Y"));
    assert!(names.contains(&"Z"));
}

#[test]
fn test_melt_via_events() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/melt_wide.parquet");
    load_file(&mut app, &rx, path);

    assert!(app.data_table_state.is_some());
    let cols = app.data_table_state.as_ref().unwrap().schema().iter_names();
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
    let df = state.lf().clone().collect().unwrap();
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
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/melt_wide_many.parquet");
    load_file(&mut app, &rx, path);

    assert!(app.data_table_state.is_some());
    let cols = app.data_table_state.as_ref().unwrap().schema().iter_names();
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
    let df = state.lf().clone().collect().unwrap();
    assert!(df.column("var").is_ok());
    assert!(df.column("val").is_ok());
    assert!(df.height() > 0);
}

#[test]
fn test_pivot_on_current_view_after_filter() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    let raw_count = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
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
        .lf()
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
        sort_columns: None,
    };
    let event = AppEvent::Pivot(spec);
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);

    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
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
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    let rows_before = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
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
        .lf()
        .clone()
        .collect()
        .unwrap()
        .height();
    assert_eq!(rows_before, rows_after, "Esc must not change table");
}

#[test]
fn test_pivot_via_modal_apply() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    app.pivot_melt_modal.index_columns = vec!["id".to_string(), "date".to_string()];
    app.pivot_melt_modal.pivot_column = Some("key".to_string());
    app.pivot_melt_modal.value_column = Some("value".to_string());
    app.pivot_melt_modal.aggregation = PivotAggregation::Last;

    // Enter applies from anywhere in the form.
    let ev = AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }
    drain_events(&mut app, &rx);

    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
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
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/melt_wide.parquet");
    load_file(&mut app, &rx, path);

    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    app.pivot_melt_modal.switch_tab();
    app.pivot_melt_modal.melt_index_columns = vec!["id".to_string(), "date".to_string()];
    app.pivot_melt_modal.melt_value_strategy =
        datui::pivot_melt_modal::MeltValueStrategy::AllExceptIndex;
    app.pivot_melt_modal
        .melt_variable_input
        .set_value("variable");
    app.pivot_melt_modal.melt_value_input.set_value("value");

    // Enter applies from anywhere in the form.
    let ev = AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }

    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"variable"));
    assert!(names.contains(&"value"));
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(df.height() > 0);
}

/// The whole pivot driven by keys alone: Tab to a row, Space or typing opens
/// its Picker, Enter chooses, Enter applies from anywhere.
#[test]
fn test_pivot_via_keys_only() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    // Index: toggle id and date in the row's Picker.
    send_key(&mut app, KeyCode::Tab);
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::PivotIndex);
    send_key(&mut app, KeyCode::Char(' ')); // opens the picker
    assert!(app.pivot_melt_modal.picker.is_some());
    send_key(&mut app, KeyCode::Char(' ')); // toggles id
    send_key(&mut app, KeyCode::Down);
    send_key(&mut app, KeyCode::Char(' ')); // toggles date
    send_key(&mut app, KeyCode::Enter); // done
    assert!(app.pivot_melt_modal.picker.is_none());
    assert_eq!(app.pivot_melt_modal.index_columns, ["id", "date"]);

    // Columns: typing opens the Picker already narrowed; Enter chooses.
    send_key(&mut app, KeyCode::Tab);
    send_key(&mut app, KeyCode::Char('k'));
    send_key(&mut app, KeyCode::Enter);
    assert_eq!(app.pivot_melt_modal.pivot_column, Some("key".to_string()));

    // Values.
    send_key(&mut app, KeyCode::Tab);
    send_key(&mut app, KeyCode::Char('v'));
    send_key(&mut app, KeyCode::Char('a'));
    send_key(&mut app, KeyCode::Enter);
    assert_eq!(app.pivot_melt_modal.value_column, Some("value".to_string()));

    // Apply from the row the cursor is on.
    let ev = AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let mut next = app.event(&ev);
    while let Some(n) = next.take() {
        next = app.event(&n);
    }
    drain_events(&mut app, &rx);

    assert!(!app.pivot_melt_modal.active);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"A") && names.contains(&"B") && names.contains(&"C"));
}

/// Esc backs out one layer at a time: the Picker first, then the modal.
#[test]
fn test_esc_closes_the_picker_before_the_modal() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    send_key(&mut app, KeyCode::Char('p'));
    send_key(&mut app, KeyCode::Tab);
    send_key(&mut app, KeyCode::Char(' '));
    assert!(app.pivot_melt_modal.picker.is_some());

    send_key(&mut app, KeyCode::Esc);
    assert!(app.pivot_melt_modal.picker.is_none());
    assert!(app.pivot_melt_modal.active, "the modal outlives its picker");
    assert_eq!(app.input_mode, InputMode::PivotMelt);

    send_key(&mut app, KeyCode::Esc);
    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
}

/// Space on a pick-one row's open Picker chooses the highlighted item, like
/// Enter; only a several-choice row toggles. It must never type into the
/// narrow filter, where a space matches nothing and blanks the list.
#[test]
fn test_space_chooses_in_a_pick_one_picker() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path);

    send_key(&mut app, KeyCode::Char('p'));
    send_key(&mut app, KeyCode::Tab); // Index
    send_key(&mut app, KeyCode::Tab); // Columns, a pick-one row
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::PivotColumn);
    send_key(&mut app, KeyCode::Char(' ')); // opens the picker
    assert!(app.pivot_melt_modal.picker.is_some());
    send_key(&mut app, KeyCode::Down);
    send_key(&mut app, KeyCode::Char(' ')); // chooses, like Enter
    assert!(app.pivot_melt_modal.picker.is_none());
    assert_eq!(app.pivot_melt_modal.pivot_column, Some("date".to_string()));
}

/// Save a template after pivot, reload file, apply via T, and verify pivoted result.
#[test]
fn test_template_save_and_apply_pivot() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/pivot_long.parquet");
    load_file(&mut app, &rx, path.clone());

    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let mut next = app.event(&AppEvent::Pivot(spec));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);

    let match_criteria = MatchCriteria {
        exact_path: Some(path.clone()),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
    };
    let template = app
        .create_template_from_current_state(
            "pivot_melt_test_pivot_template".to_string(),
            None,
            match_criteria,
        )
        .expect("create template");

    load_file(&mut app, &rx, path.clone());
    let raw_names: Vec<String> = app
        .data_table_state
        .as_ref()
        .unwrap()
        .schema()
        .iter_names()
        .map(|s| s.to_string())
        .collect();
    assert!(
        raw_names.contains(&"key".to_string()),
        "raw data must have key column before apply"
    );

    send_key(&mut app, KeyCode::Char('V'));
    // The pivot is read in the background.
    assert!(app.is_busy());
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
    assert!(df.height() > 0);

    assert!(template.settings.pivot.is_some());
}

/// The pivot reads its data off the UI thread: the event returns at once with the app
/// busy and the table as it was, and the result is installed when the read lands.
#[test]
fn test_pivot_reads_in_the_background() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    load_file(
        &mut app,
        &rx,
        PathBuf::from("tests/sample-data/pivot_long.parquet"),
    );
    drain_events(&mut app, &rx);
    send_key(&mut app, KeyCode::Char('p'));
    assert!(app.pivot_melt_modal.active);

    let spec = PivotSpec {
        index: vec!["date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let next = app.event(&AppEvent::Pivot(spec));
    assert!(next.is_none(), "nothing more runs on this thread");
    assert!(app.is_busy(), "the control bar shows the pivot running");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_none());
    assert!(
        state.schema().contains("key"),
        "the table is as it was until the read lands"
    );
    assert!(
        app.pivot_melt_modal.active,
        "the modal waits for the result"
    );

    drain_events(&mut app, &rx);
    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_some());
    let df = state.lf().clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names, vec!["date", "A", "B", "C"]);
    assert_eq!(df.height(), 31);
}

/// A pivot that lands after something newer replaced the view is dropped.
#[test]
fn test_a_stale_pivot_result_is_dropped() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    load_file(
        &mut app,
        &rx,
        PathBuf::from("tests/sample-data/pivot_long.parquet"),
    );
    drain_events(&mut app, &rx);

    let spec = PivotSpec {
        index: vec!["date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    let pivoted = polars::prelude::df!("date" => ["2024-01-01"], "A" => [1.0]).unwrap();
    app.event(&AppEvent::PivotReady {
        generation: app.task_generation().wrapping_sub(1),
        spec,
        pivoted,
    });
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_none());
    assert!(state.schema().contains("key"));
}

/// Esc while the pivot is read stops waiting for it, at once rather than behind the
/// keys held meanwhile: the form stays open with its spec, the table as it was, and the
/// answer that lands later is dropped. A second Esc closes the form.
#[test]
fn test_esc_stops_a_pivot_being_read() {
    ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    load_file(
        &mut app,
        &rx,
        PathBuf::from("tests/sample-data/pivot_long.parquet"),
    );
    drain_events(&mut app, &rx);
    send_key(&mut app, KeyCode::Char('p'));

    let spec = PivotSpec {
        index: vec!["date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    app.event(&AppEvent::Pivot(spec));
    assert!(app.is_busy());
    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    assert!(
        app.hard_escape_while_busy(&esc),
        "not held behind the pivot"
    );
    send_key(&mut app, KeyCode::Esc);
    assert!(!app.is_busy());
    assert!(
        app.pivot_melt_modal.active,
        "the spec is still there to change"
    );
    assert_eq!(app.input_mode, InputMode::PivotMelt);

    // The worker finishes regardless; what it sends is stale.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    while app.background_work_in_flight() {
        assert!(
            std::time::Instant::now() < deadline,
            "the worker never ended"
        );
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            app.event(&ev);
        }
    }
    while let Ok(ev) = rx.try_recv() {
        app.event(&ev);
    }
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_none());
    assert!(state.schema().contains("key"));
    assert!(app.pivot_melt_modal.active);

    send_key(&mut app, KeyCode::Esc);
    assert!(!app.pivot_melt_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
}

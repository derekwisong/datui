use crate::*;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::prelude::IntoLazy;
use std::sync::mpsc;

fn key(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Enter on an incomplete pivot form re-accents the gap line instead of
/// raising a modal; the next key dims it again.
#[test]
fn an_incomplete_pivot_apply_stays_inline() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.pivot_melt_modal.active = true;
    app.input_mode = InputMode::PivotMelt;
    key(&mut app, KeyCode::Enter);
    assert!(!app.error_modal.active, "validation is not a failure");
    assert!(app.pivot_melt_modal.attention, "the gap line is lit");
    key(&mut app, KeyCode::Tab);
    assert!(!app.pivot_melt_modal.attention, "an edit dims it again");
}

/// `s` in the views list with an untouched table refuses on the list's own
/// status line; the next key clears it.
#[test]
fn the_views_save_refusal_stays_on_the_surface() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::df!("a" => [1i64, 2]).unwrap();
    app.data_table_state =
        Some(crate::table::DataTableState::new(df.lazy(), None, None, None, None, true).unwrap());
    app.view_modal.active = true;
    key(&mut app, KeyCode::Char('s'));
    assert!(!app.error_modal.active, "a refusal is not a failure");
    assert!(
        app.view_modal
            .status
            .as_deref()
            .unwrap_or("")
            .starts_with("Nothing to save"),
        "the refusal is on the list's status line"
    );
    key(&mut app, KeyCode::Down);
    assert!(app.view_modal.status.is_none(), "the next key clears it");
}

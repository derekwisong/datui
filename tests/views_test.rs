//! The Views surface, end to end: save form, list annotations, apply, delete.
//!
//! These live in their own binary because saving a view writes into the
//! process's config dir, and a saved view's suggested path pattern matches
//! every fixture in `tests/sample-data` — inside `integration_test.rs` it
//! would race the tests that assert "no view matches".

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::template::MatchReason;
use datui::widgets::template_modal::{FormFocus, TemplateModalMode};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use std::fs::File;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

fn press(app: &mut App, code: KeyCode) {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

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
                next = app.event(&ev);
            }
            _ => match rx.recv_timeout(std::time::Duration::from_millis(5000)) {
                Ok(ev) => next = Some(ev),
                Err(_) => return,
            },
        }
    }
}

/// One sequential walk through the surface, so the views it saves never race
/// another test's list.
#[test]
fn the_views_surface_saves_applies_and_deletes() {
    let test_data_dir = PathBuf::from("tests/sample-data");
    std::fs::create_dir_all(&test_data_dir).unwrap();
    let csv_path = test_data_dir.join("views_surface.csv");
    let mut df = df!(
        "vs_id" => (0..50i64).collect::<Vec<_>>(),
        "vs_group" => (0..50i64).map(|i| i % 3).collect::<Vec<_>>(),
    )
    .unwrap();
    CsvWriter::new(&mut File::create(&csv_path).unwrap())
        .finish(&mut df)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    // Open by the canonical path, so the saved exact-path criterion matches
    // the open file and the annotation reads "same file".
    let csv_path = csv_path.canonicalize().unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    assert!(app.data_table_state.is_some());

    // v opens the list.
    press(&mut app, KeyCode::Char('v'));
    assert!(app.template_modal.active);
    assert_eq!(app.template_modal.mode, TemplateModalMode::List);
    assert!(app.template_modal.rows.is_empty());

    // An untouched table has nothing to save: s refuses instead of opening
    // the form and minting a view that carries nothing. The refusal is said
    // on the list's own status line, not in a modal.
    press(&mut app, KeyCode::Char('s'));
    assert!(!app.modal_showing(), "a refusal is not a modal");
    assert!(
        app.template_modal
            .status
            .as_deref()
            .unwrap_or("")
            .starts_with("Nothing to save"),
        "the refusal is on the list's status line"
    );
    assert_eq!(app.template_modal.mode, TemplateModalMode::List);
    assert!(app.template_modal.active, "the list survives the refusal");

    // Give the state something to carry.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["vs_id".to_string()], vec![true]);

    // The form opens name-first, prefilled from the state, criteria folded.
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.template_modal.mode, TemplateModalMode::Create);
    assert_eq!(app.template_modal.form_focus, FormFocus::Name);
    assert_eq!(
        app.template_modal.name_input.value(),
        "views_surface",
        "the suggested name is the file stem, not template0001"
    );
    assert!(
        app.template_modal.schema_match_enabled,
        "schema match starts on"
    );
    assert!(
        !app.template_modal.matching_expanded,
        "the criteria start folded away"
    );

    // Esc discards: back to the list with nothing saved.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.template_modal.mode, TemplateModalMode::List);
    assert!(app.template_modal.rows.is_empty(), "Esc saved nothing");

    // Enter saves from the name row; no Save button to Tab onto. The new
    // view lands in the list saying why it matches.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.template_modal.mode, TemplateModalMode::List);
    assert_eq!(app.template_modal.rows.len(), 1);
    let row = &app.template_modal.rows[0];
    assert_eq!(row.template.name, "views_surface");
    assert_eq!(
        row.reason,
        Some(MatchReason::SameFile),
        "the row says why it matches"
    );

    // The next save suggests a numbered name, so Enter twice never dies on
    // "name already exists".
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.template_modal.name_input.value(), "views_surface 2");
    press(&mut app, KeyCode::Esc);

    // Editing a view that is not applied keeps what it carries: the table's
    // sort has moved on, and save must not overwrite the view's with it.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["vs_group".to_string()], vec![false]);
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.template_modal.mode, TemplateModalMode::Edit);
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.template_modal.mode, TemplateModalMode::List);
    assert_eq!(
        app.template_modal.rows[0].template.settings.sort_columns,
        vec!["vs_id".to_string()],
        "editing an unapplied view leaves its settings alone"
    );

    // Enter applies the selected view and closes the list.
    press(&mut app, KeyCode::Enter);
    assert!(!app.template_modal.active, "apply closes the list");

    // Applied, the view follows the table: adjust the sort and re-save
    // through edit, and the view carries the new state.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["vs_group".to_string()], vec![false]);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('e'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.template_modal.rows[0].template.settings.sort_columns,
        vec!["vs_group".to_string()],
        "editing the applied view updates its settings from the table"
    );
    press(&mut app, KeyCode::Esc);
    assert!(!app.template_modal.active);
    press(&mut app, KeyCode::Char('v'));

    // Esc cancels the delete confirmation and only it; Enter confirms.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('d'));
    assert!(app.template_modal.delete_confirm);
    press(&mut app, KeyCode::Esc);
    assert!(!app.template_modal.delete_confirm);
    assert_eq!(app.template_modal.rows.len(), 1, "cancel deletes nothing");
    assert!(
        app.template_modal.active,
        "Esc closed only the confirmation"
    );

    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Enter);
    assert!(
        app.template_modal.rows.is_empty(),
        "Enter confirms the delete"
    );
    press(&mut app, KeyCode::Esc);
    assert!(!app.template_modal.active);

    // Ctrl+J saves from the description, the same as Ctrl+Enter: some
    // terminals send one as the other, so it must never delete the line being
    // typed. The footer names it.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.template_modal.form_focus, FormFocus::Description);
    for c in "first".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    press(&mut app, KeyCode::Enter);
    for c in "second".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect())
        .collect();
    assert!(
        rows.iter().any(|r| r.contains("^J") && r.contains("Save")),
        "the footer names the chord that saves from the description"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('j'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.template_modal.mode, TemplateModalMode::List, "saved");
    assert_eq!(app.template_modal.rows.len(), 1);
    assert_eq!(
        app.template_modal.rows[0].template.description.as_deref(),
        Some("first\nsecond"),
        "the description is saved whole"
    );
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Enter);
    assert!(app.template_modal.rows.is_empty());
    press(&mut app, KeyCode::Esc);
    assert!(!app.template_modal.active);

    // A view's schema criterion belongs to the view: editing it while a
    // different table is open must not swap in that table's columns.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.template_modal.rows.len(), 1);
    assert_eq!(
        app.template_modal.rows[0]
            .template
            .match_criteria
            .schema_columns
            .as_deref(),
        Some(&["vs_id".to_string(), "vs_group".to_string()][..]),
        "the saved view carries this table's columns"
    );
    press(&mut app, KeyCode::Esc);

    let other_path = test_data_dir.join("views_surface_other.csv");
    let mut other = df!("other_col" => (0..5i64).collect::<Vec<_>>()).unwrap();
    CsvWriter::new(&mut File::create(&other_path).unwrap())
        .finish(&mut other)
        .unwrap();
    let other_path = other_path.canonicalize().unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![other_path], OpenOptions::default());
    press(&mut app, KeyCode::Char('v'));
    assert_eq!(app.template_modal.rows.len(), 1);
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.template_modal.mode, TemplateModalMode::Edit);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.template_modal.rows[0]
            .template
            .match_criteria
            .schema_columns
            .as_deref(),
        Some(&["vs_id".to_string(), "vs_group".to_string()][..]),
        "editing from another table keeps the view's stored schema"
    );

    // Leave no view behind: the suggested path pattern matches every fixture.
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Enter);
    assert!(app.template_modal.rows.is_empty());
    press(&mut app, KeyCode::Esc);
}

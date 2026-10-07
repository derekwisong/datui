//! The Views surface, end to end: save form, list annotations, apply, delete.
//!
//! Kept out of `integration_test` because saving a view writes into the
//! process's config dir, which its tests that assert "no view matches" read.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::view::MatchReason;
use datui::widgets::view_modal::{FormFocus, ViewModalMode};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use std::fs::File;
use std::sync::mpsc;

use crate::common;

use crate::common::{drain_events, pump_open_until_loaded};

fn press(app: &mut App, code: KeyCode) {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

/// One sequential walk through the surface, so the views it saves never race
/// another test's list.
#[test]
fn the_views_surface_saves_applies_and_deletes() {
    let test_data_dir = common::fixture_dir();
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
    assert_eq!(app.overlay, datui::Overlay::View);
    assert_eq!(app.view_modal.mode, ViewModalMode::List);
    assert!(app.view_modal.rows.is_empty());

    // An untouched table has nothing to save: s refuses instead of opening
    // the form and minting a view that carries nothing. The refusal is said
    // on the list's own status line, not in a modal.
    press(&mut app, KeyCode::Char('s'));
    assert!(!app.modal_showing(), "a refusal is not a modal");
    assert!(
        app.view_modal
            .status
            .as_deref()
            .unwrap_or("")
            .starts_with("Nothing to save"),
        "the refusal is on the list's status line"
    );
    assert_eq!(app.view_modal.mode, ViewModalMode::List);
    assert_eq!(
        app.overlay,
        datui::Overlay::View,
        "the list survives the refusal"
    );

    // Give the state something to carry.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["vs_id".to_string()], vec![true]);

    // The form opens name-first, prefilled from the state, criteria folded.
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.view_modal.mode, ViewModalMode::Create);
    assert_eq!(app.view_modal.form_focus, FormFocus::Name);
    assert_eq!(
        app.view_modal.name_input.value(),
        "views_surface",
        "the suggested name is the file stem, not a numbered default"
    );
    assert!(
        app.view_modal.schema_match_enabled,
        "schema match starts on"
    );
    assert!(
        !app.view_modal.matching_expanded,
        "the criteria start folded away"
    );

    // Esc discards: back to the list with nothing saved.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.view_modal.mode, ViewModalMode::List);
    assert!(app.view_modal.rows.is_empty(), "Esc saved nothing");

    // Enter saves from the name row; no Save button to Tab onto. The new
    // view lands in the list saying why it matches.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.view_modal.mode, ViewModalMode::List);
    assert_eq!(app.view_modal.rows.len(), 1);
    let row = &app.view_modal.rows[0];
    assert_eq!(row.view.name, "views_surface");
    assert_eq!(
        row.reason,
        Some(MatchReason::SameFile),
        "the row says why it matches"
    );

    // The next save suggests a numbered name, so Enter twice never dies on
    // "name already exists".
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.view_modal.name_input.value(), "views_surface 2");
    press(&mut app, KeyCode::Esc);

    // Editing a view that is not applied keeps what it carries: the table's
    // sort has moved on, and save must not overwrite the view's with it.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["vs_group".to_string()], vec![false]);
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.view_modal.mode, ViewModalMode::Edit);
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.view_modal.mode, ViewModalMode::List);
    assert_eq!(
        app.view_modal.rows[0].view.settings.sort_columns,
        vec!["vs_id".to_string()],
        "editing an unapplied view leaves its settings alone"
    );

    // Enter applies the selected view and closes the list; its rows are read in
    // the background.
    press(&mut app, KeyCode::Enter);
    assert_ne!(app.overlay, datui::Overlay::View, "apply closes the list");
    drain_events(&mut app, &rx);

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
        app.view_modal.rows[0].view.settings.sort_columns,
        vec!["vs_group".to_string()],
        "editing the applied view updates its settings from the table"
    );
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, datui::Overlay::View);
    press(&mut app, KeyCode::Char('v'));

    // Delete asks with the one confirmation, on No: Enter there and Esc each
    // decline and close only it; Delete, then Enter, deletes.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('d'));
    assert!(app.confirmation_modal.active);
    assert!(!app.confirmation_modal.focus_yes, "a delete starts on No");
    assert_eq!(app.confirmation_modal.yes_label, "Delete");
    press(&mut app, KeyCode::Esc);
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.view_modal.rows.len(), 1, "cancel deletes nothing");
    assert_eq!(
        app.overlay,
        datui::Overlay::View,
        "Esc closed only the confirmation"
    );
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Enter);
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.view_modal.rows.len(), 1, "Enter on No deletes nothing");

    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Enter);
    assert!(app.view_modal.rows.is_empty(), "Enter on Delete deletes");
    assert_eq!(app.overlay, datui::Overlay::View, "the list stays open");
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, datui::Overlay::View);

    // Ctrl+J saves from the description, the same as Ctrl+Enter: some
    // terminals send one as the other, so it must never delete the line being
    // typed. The footer names it.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.view_modal.form_focus, FormFocus::Description);
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
    let rows: Vec<String> = common::buffer_lines(&buf);
    assert!(
        rows.iter().any(|r| r.contains("^J") && r.contains("Save")),
        "the footer names the chord that saves from the description"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('j'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.view_modal.mode, ViewModalMode::List, "saved");
    assert_eq!(app.view_modal.rows.len(), 1);
    assert_eq!(
        app.view_modal.rows[0].view.description.as_deref(),
        Some("first\nsecond"),
        "the description is saved whole"
    );
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Left); // from No to Delete
    press(&mut app, KeyCode::Enter);
    assert!(app.view_modal.rows.is_empty());
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, datui::Overlay::View);

    // A view's schema criterion belongs to the view: editing it while a
    // different table is open must not swap in that table's columns.
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.view_modal.rows.len(), 1);
    assert_eq!(
        app.view_modal.rows[0]
            .view
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
    assert_eq!(app.view_modal.rows.len(), 1);
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.view_modal.mode, ViewModalMode::Edit);
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.view_modal.rows[0]
            .view
            .match_criteria
            .schema_columns
            .as_deref(),
        Some(&["vs_id".to_string(), "vs_group".to_string()][..]),
        "editing from another table keeps the view's stored schema"
    );

    // Leave no view behind.
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Left); // from No to Delete
    press(&mut app, KeyCode::Enter);
    assert!(app.view_modal.rows.is_empty());
    press(&mut app, KeyCode::Esc);

    // A view saved on a logger export read with its dialect carries the names as
    // shown, joined from the header lines and trimmed, so it matches the next export
    // however that one pads them.
    let dialect = OpenOptions {
        comment_char: Some("#".into()),
        header_rows: vec![3, 2],
        skip_initial_space: true,
        parse_strings: Some(datui::ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let log = |name: &str, header: &str, rows: &[&str]| {
        let path = test_data_dir.join(name);
        let mut text = format!("#device_info\n#yyyy-mm-dd, degrees\n{header}\n");
        for row in rows {
            text.push_str(row);
            text.push('\n');
        }
        std::fs::write(&path, text).unwrap();
        path.canonicalize().unwrap()
    };
    let first = log(
        "views_log_a.csv",
        "  Lcl Date,     Latitude",
        &["2024-03-01,    40.100000", "2024-03-02,    40.300000"],
    );
    let second = log(
        "views_log_b.csv",
        "Lcl Date ,Latitude",
        &["2024-04-01, 41.5", "2024-04-02, 41.9", "2024-04-03, 41.7"],
    );
    pump_open_until_loaded(&mut app, &rx, vec![first], dialect.clone());
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["Latitude degrees".to_string()], vec![true]);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.view_modal.rows[0]
            .view
            .match_criteria
            .schema_columns
            .as_deref(),
        Some(
            &[
                "Lcl Date yyyy-mm-dd".to_string(),
                "Latitude degrees".to_string()
            ][..]
        ),
    );
    press(&mut app, KeyCode::Esc);

    pump_open_until_loaded(&mut app, &rx, vec![second], dialect);
    press(&mut app, KeyCode::Char('v'));
    assert_eq!(app.view_modal.rows.len(), 1);
    assert_eq!(
        app.view_modal.rows[0].reason,
        Some(MatchReason::SameColumns)
    );
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    let latitudes: Vec<Option<f64>> = df
        .column("Latitude degrees")
        .unwrap()
        .f64()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(latitudes, [Some(41.9), Some(41.7), Some(41.5)], "applied");

    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('d'));
    press(&mut app, KeyCode::Left); // from No to Delete
    press(&mut app, KeyCode::Enter);
    assert!(app.view_modal.rows.is_empty());
    press(&mut app, KeyCode::Esc);
}

/// A view keeps its sample's settings, never its rows: applied again, it draws the
/// same rows from the seed, and lays its query over them.
#[test]
fn a_view_draws_its_sample_again_from_the_seed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sampled_view.parquet");
    let mut df = df!(
        "sv_id" => (0..20_000i64).collect::<Vec<_>>(),
        "sv_group" => (0..20_000i64).map(|i| i % 5).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let path = path.canonicalize().unwrap();
    let views = datui::view::ViewManager::new(&datui::config::ConfigManager::with_dir(
        dir.path().join("config"),
    ))
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let config = datui::AppConfig::default();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_views(
        tx.clone(),
        common::test_runtime(),
        theme,
        config,
        views.into(),
    );
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    drain_events(&mut app, &rx);

    press(&mut app, KeyCode::Char('S'));
    let form = app.sample.form.as_mut().unwrap();
    form.size.set_value("700");
    form.seed.set_value("31");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    app.event(AppEvent::QQuery("select where sv_group = 2".to_string()));
    drain_events(&mut app, &rx);
    let ids = |app: &App| -> Vec<i64> {
        let state = app.data_table_state.as_ref().unwrap();
        state
            .lf()
            .clone()
            .collect()
            .unwrap()
            .column("sv_id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect()
    };
    let drawn = ids(&app);
    assert!(!drawn.is_empty() && drawn.len() < 700, "{}", drawn.len());

    // A chart of the view, with an option set: the view keeps it.
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.overlay, datui::Overlay::Chart);
    app.chart.modal.hist_bins = 17;
    press(&mut app, KeyCode::Esc);
    drain_events(&mut app, &rx);

    let criteria = datui::view::MatchCriteria {
        exact_path: Some(path.clone()),
        relative_path: None,
        path_pattern: None,
        filename_pattern: None,
        schema_columns: None,
        schema_types: None,
        table: None,
    };
    let saved = app
        .create_view_from_current_state("sampled".to_string(), None, criteria)
        .unwrap();
    let sample = saved.settings.sample.as_ref().expect("the sample is saved");
    assert_eq!((sample.rows, sample.seed), (700, 31));
    assert_eq!(
        saved.settings.query.as_deref(),
        Some("select where sv_group = 2")
    );
    let chart = saved.settings.chart.as_ref().expect("the chart is saved");
    assert_eq!(chart.histogram_bins, 17);

    // Back to the table as opened, then the view again.
    if let Some(reset) = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('R'),
        KeyModifiers::NONE,
    ))) {
        app.event(reset);
    }
    drain_events(&mut app, &rx);
    assert!(app.data_table_state.as_ref().unwrap().sampled().is_none());
    app.chart.modal.hist_bins = 40;
    press(&mut app, KeyCode::Char('V'));
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.sampled().is_some(), "the view draws its sample");
    assert_eq!(state.get_active_query(), "select where sv_group = 2");
    assert_eq!(ids(&app), drawn, "the same rows, from the seed");

    // The table, with the chart one key away: `c` draws the view's chart.
    assert_eq!(app.input_mode, datui::InputMode::Normal);
    assert!(app.chart.modal.restored);
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.overlay, datui::Overlay::Chart);
    assert_eq!(app.chart.modal.spec, chart.spec);
    assert_eq!(app.chart.modal.hist_bins, 17);
}

/// A sample drawn from the view's rows keeps the whole view it was drawn through: a
/// row range of a sorted view is the first rows in that order, and applied again the
/// view sorts the source the same way before it draws.
#[test]
fn a_view_keeps_the_sorted_view_its_row_range_was_drawn_through() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ranged_view.parquet");
    let mut df = df!("rv_id" => (0..5_000i64).collect::<Vec<_>>()).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let path = path.canonicalize().unwrap();
    let views = datui::view::ViewManager::new(&datui::config::ConfigManager::with_dir(
        dir.path().join("config"),
    ))
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let config = datui::AppConfig::default();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_views(tx, common::test_runtime(), theme, config, views.into());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    drain_events(&mut app, &rx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["rv_id".to_string()], vec![true]);
    drain_events(&mut app, &rx);

    press(&mut app, KeyCode::Char('S'));
    let form = app.sample.form.as_mut().unwrap();
    form.kind = datui::sample_modal::RowsKind::Range;
    form.range_from.set_value("1");
    form.range_to.set_value("200");
    form.draft.method = datui::sampling::SampleMethod::FirstRows;
    form.size.set_value("200");
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    let ids = |app: &App| -> Vec<i64> {
        let df = app
            .data_table_state
            .as_ref()
            .unwrap()
            .lf()
            .clone()
            .collect()
            .unwrap();
        df.column("rv_id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect()
    };
    let drawn = ids(&app);
    assert_eq!(drawn.first(), Some(&4_999), "the sorted view's first rows");
    assert_eq!(drawn.len(), 200);

    let criteria = datui::view::MatchCriteria {
        exact_path: Some(path.clone()),
        ..Default::default()
    };
    let saved = app
        .create_view_from_current_state("ranged".to_string(), None, criteria)
        .unwrap();
    let through = saved.settings.sample.as_ref().unwrap().through.as_ref();
    assert_eq!(
        through.map(|t| t.sort_columns.clone()),
        Some(vec!["rv_id".to_string()]),
        "the sort it was drawn through is kept"
    );
    if let Some(reset) = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('R'),
        KeyModifiers::NONE,
    ))) {
        app.event(reset);
    }
    drain_events(&mut app, &rx);
    press(&mut app, KeyCode::Char('V'));
    drain_events(&mut app, &rx);
    assert_eq!(
        ids(&app),
        drawn,
        "the same rows, drawn through the same sort"
    );
}

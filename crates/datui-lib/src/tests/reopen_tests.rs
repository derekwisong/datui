//! A read that finds a file gone since the open offers to reopen the dataset, and the
//! reopen puts back where it was: the query, filters, sort, the saved view applied
//! and the drill-down, over what the files hold now.

use super::chart_prepare_tests::{open, pump};
use crate::app::jobs::{Job, Outcome};
use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

const GONE: &str = "A file was removed or replaced after the dataset was opened: \
                    part-0.parquet. Reopen the dataset to read the current files.";

/// `id,key,val` with `keys` cycled over `rows` ids.
fn write_csv(path: &Path, rows: i64, keys: &[&str]) {
    let mut body = String::from("id,key,val\n");
    for id in 0..rows {
        let key = keys[id as usize % keys.len()];
        body.push_str(&format!("{id},{key},{}\n", id * 10));
    }
    std::fs::write(path, body).unwrap();
}

fn opened_app(
    rows: i64,
    keys: &[&str],
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    tempfile::TempDir,
    PathBuf,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    write_csv(&path, rows, keys);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
    app.views.manager = crate::view::ViewManager::new(&config).unwrap().into();
    open(&mut app, &rx, &tx, path.clone());
    (app, rx, tx, dir, path)
}

/// Handle `event` and what it leads to, until the app is idle.
fn settle(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    event: AppEvent,
) {
    if let Some(next) = app.event(event) {
        tx.send(next).unwrap();
    }
    pump(app, rx, tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });
}

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// A read of the dataset fails, as a worker that hit a file gone since the open says.
fn read_fails(app: &mut App, message: &str) {
    let started = app.job_for_tests(Job::SampleRows, Some("Reading"));
    let ticket = started.ticket();
    started.end(Outcome::Failed {
        message: message.to_string(),
        panicked: false,
    });
    app.event(AppEvent::JobEnded(ticket));
}

fn ids(app: &App) -> Vec<i64> {
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    df.column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect()
}

fn not_k2() -> FilterStatement {
    FilterStatement {
        columns: Vec::new(),
        column: "key".to_string(),
        operator: FilterOperator::NotEq,
        value: "k2".to_string(),
        logical_op: LogicalOperator::And,
    }
}

#[test]
fn a_gone_file_offers_a_reopen_that_keeps_the_query_filters_sort_and_view() {
    let (mut app, rx, tx, _dir, path) = opened_app(6, &["k0", "k1", "k2"]);
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::QQuery("select where id > 0".to_string())),
    );
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::Filter(vec![not_k2()])),
    );
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::Sort(vec!["val".to_string()], vec![true])),
    );
    assert_eq!(ids(&app), [4, 3, 1]);
    // A saved view of this place, applied: it stays applied, with no use recorded.
    let view = app
        .create_view_from_current_state("place".to_string(), None, Default::default())
        .unwrap();
    app.views.active_id = Some(view.id.clone());

    read_fails(&mut app, GONE);
    assert!(app.confirmation_modal.active, "the reopen is offered");
    assert_eq!(app.confirmation_modal.message, GONE);
    assert_eq!(
        (
            app.confirmation_modal.yes_label,
            app.confirmation_modal.no_label
        ),
        ("Reopen", "Close")
    );
    assert_eq!(app.error_message(), None);

    // The files changed: the reopen reads what they hold now.
    write_csv(&path, 9, &["k0", "k1", "k2"]);
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.error_message(), None);
    assert_eq!(ids(&app), [7, 6, 4, 3, 1]);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where id > 0");
    assert_eq!(state.get_filters().len(), 1);
    assert_eq!(state.get_sort_columns(), ["val"]);
    assert_eq!(app.views.active_id.as_deref(), Some(view.id.as_str()));
    let stored = app.views.manager.get_view_by_id(&view.id).unwrap();
    assert_eq!(stored.usage_count, 0, "a reopen is not a use");
}

#[test]
fn a_reopen_drills_into_the_same_group_again() {
    let (mut app, rx, tx, _dir, path) = opened_app(6, &["k0", "k1", "k2"]);
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::QQuery("select n: count id by key".to_string())),
    );
    app.data_table_state
        .as_mut()
        .unwrap()
        .table_state
        .select(Some(1));
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    let key_of = |app: &App| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .drilled_group_key()
            .map(|(columns, values)| (columns.to_vec(), values.to_vec()))
    };
    let drilled = key_of(&app).expect("drilled into a group");
    let group = drilled.1[0].clone();
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::Sort(vec!["id".to_string()], vec![true])),
    );
    let before = ids(&app);
    assert_eq!(before.len(), 2);

    read_fails(&mut app, GONE);
    assert!(app.confirmation_modal.active);
    // The group has more rows now, and moved down the grouped view.
    write_csv(&path, 12, &["k9", "k0", "k1", "k2"]);
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert_eq!(app.error_message(), None);
    assert_eq!(key_of(&app), Some(drilled.clone()), "the same group");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.view_sort_columns(),
        ["id"],
        "sorted inside it as before"
    );
    let after = ids(&app);
    assert_eq!(after.len(), 3, "its rows as the files hold them now");
    assert!(after.is_sorted_by(|a, b| a >= b), "{after:?}");

    // Gone from the files, the group is not drilled into: the grouped view stays.
    read_fails(&mut app, GONE);
    write_csv(&path, 6, &["k9"]);
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert!(state.is_grouped());
    assert_eq!(
        app.flash_message(),
        Some(format!("No rows with key = {group} in the current files").as_str())
    );
}

#[test]
fn close_leaves_the_dataset_as_it_was() {
    let (mut app, rx, tx, _dir, _path) = opened_app(6, &["k0", "k1"]);
    settle(
        &mut app,
        &rx,
        &tx,
        AppEvent::Applied(Applied::QQuery("select where id > 2".to_string())),
    );
    read_fails(&mut app, GONE);
    app.event(key(KeyCode::Right));
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert!(!app.confirmation_modal.active);
    assert!(!app.modal_showing());
    assert!(app.nothing_loading(), "nothing reopens");
    assert_eq!(ids(&app), [3, 4, 5]);
}

/// A dataset with nothing to reopen from (a frame handed over, standard input) says
/// why as any failed read does.
#[test]
fn with_nothing_to_reopen_the_error_is_shown() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::prelude::df!("id" => [1i64, 2]).unwrap();
    let options = OpenOptions::default();
    let state =
        crate::table::DataTableState::from_lazyframe(polars::prelude::IntoLazy::lazy(df), &options)
            .unwrap();
    app.install_for_tests(state, None, &options, None);
    assert!(app.source.opened.is_none());
    read_fails(&mut app, GONE);
    assert!(!app.confirmation_modal.active);
    assert_eq!(app.error_message(), Some(GONE));
}

/// A value drilled into is taken again in its column's new type; one that is not a
/// value of it is said to be gone, and the view stays as it was, readable.
#[test]
fn a_value_drill_follows_its_columns_type_or_is_said_gone() {
    use polars::prelude::AnyValue;
    let (mut app, rx, tx, _dir, path) = opened_app(6, &["k0", "k1", "k2"]);
    let state = app.data_table_state.as_mut().unwrap();
    state
        .deferred(|s| s.drill_into_value("val", AnyValue::Int64(30)))
        .unwrap();
    app.spawn_async_collect(App::LOADING_BUFFER);
    pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert_eq!(ids(&app), [3]);

    // `val` is text now: 30 is cast to "30", and the drill holds.
    read_fails(&mut app, GONE);
    std::fs::write(&path, "id,key,val\n3,k0,30\n4,k1,x\n7,k1,30\n").unwrap();
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert_eq!(app.error_message(), None);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_eq!(ids(&app), [3, 7]);

    // A key drilled into as text, where the column is now numbers: gone.
    let (mut app, rx, tx, _dir, path) = opened_app(6, &["k0", "k1", "k2"]);
    let state = app.data_table_state.as_mut().unwrap();
    state
        .deferred(|s| s.drill_into_value("key", AnyValue::StringOwned("k1".into())))
        .unwrap();
    app.spawn_async_collect(App::LOADING_BUFFER);
    pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert_eq!(ids(&app), [1, 4]);
    read_fails(&mut app, GONE);
    std::fs::write(&path, "id,key,val\n0,1,0\n1,2,10\n").unwrap();
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert_eq!(app.error_message(), None);
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_eq!(
        app.flash_message(),
        Some("No rows with key = k1 in the current files")
    );
    assert_eq!(ids(&app), [0, 1], "the view reads");
}

/// What the question was asked over read the dataset that is replaced: it closes.
#[test]
fn reopen_closes_what_the_question_was_asked_over() {
    let (mut app, rx, tx, _dir, _path) = opened_app(6, &["k0", "k1"]);
    app.open_overlay(Overlay::PivotMelt);
    read_fails(&mut app, GONE);
    settle(&mut app, &rx, &tx, key(KeyCode::Enter));
    assert_eq!(app.overlay, Overlay::None);
    assert_eq!(app.error_message(), None);
    assert_eq!(ids(&app), [0, 1, 2, 3, 4, 5]);
}

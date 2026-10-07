use super::chart_prepare_tests::open;
use crate::*;
use polars::datatypes::AnyValue;
use std::sync::mpsc;

/// An app with `long.csv` open, `id,key,val` over five ids, and views of its own.
fn long_csv_app() -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    tempfile::TempDir,
) {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("long.csv");
    let mut body = String::from("id,key,val\n");
    for id in 0..5 {
        body.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    std::fs::write(&path, body).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
    app.views.manager = ViewManager::new(&config).unwrap().into();
    open(&mut app, &rx, &tx, path);
    (app, rx, tx, dir)
}

/// A view of `long.csv` that pivots `key` into columns.
fn pivot_view(app: &mut App, name: &str) -> SavedView {
    let mut view = app
        .create_view_from_current_state(
            name.to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.pivot = Some(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: pivot_melt_modal::PivotAggregation::First,
    });
    view.settings.column_order.clear();
    view
}

fn columns(app: &App) -> Vec<String> {
    let state = app.data_table_state.as_ref().unwrap();
    state.schema().iter_names().map(|s| s.to_string()).collect()
}

/// The bottom line of a rendered App.
fn footer_text(app: &mut App) -> String {
    use ratatui::widgets::Widget;
    let area = ratatui::layout::Rect::new(0, 0, 120, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect()
}

/// Open `long.csv` with `view` applied on open and handle events until the
/// open is done; `intercept` may take an event instead. Returns how many times the
/// dataset's own rows were asked for.
fn open_with_view(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    dir: &tempfile::TempDir,
    view: &SavedView,
    mut intercept: impl FnMut(&mut App, &AppEvent) -> bool,
) -> usize {
    app.views.manager.update_view(view).unwrap();
    app.source.startup_view = Some(view.name.clone());
    let path = dir.path().join("long.csv");
    let mut next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let asked = app.counting.first_rows_asked;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        while let Some(event) = next.take() {
            if !intercept(app, &event) {
                next = app.event(&event);
            }
        }
        if app.data_table_state.is_some() && !app.is_busy() && !app.awaiting_dataset() {
            return app.counting.first_rows_asked - asked;
        }
        assert!(std::time::Instant::now() < deadline, "the open never ended");
        next = rx.recv_timeout(std::time::Duration::from_millis(50)).ok();
    }
}

/// The saved views are read on a worker while the app starts. A `--view` open
/// that gets to its schema first waits for them rather than showing the rows
/// without the view: the app is built, draws and takes keys with the read still
/// out, and the view the user asked for is the one installed.
#[test]
fn a_startup_view_waits_for_views_still_being_read() {
    let (mut first, _rx, _tx, dir) = long_csv_app();
    let view = pivot_view(&mut first, "pivot");
    let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
    ViewManager::new(&config)
        .unwrap()
        .update_view(&view)
        .unwrap();

    let (views_tx, views_rx) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.views.manager = Views::waiting_on(views_rx);
    app.source.startup_view = Some(view.name.clone());
    app.set_loading_phase("Scanning input", 10);
    app.busy = true;
    // The app draws and handles a key with the views still out.
    footer_text(&mut app);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert!(!app.views.manager.is_read(), "nothing has needed them yet");

    // The views land only after the open has started.
    let sender = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        views_tx.send(ViewManager::new(&config).unwrap()).unwrap();
    });
    let mut next = app.event(&AppEvent::Open(
        vec![dir.path().join("long.csv")],
        OpenOptions::default(),
    ));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        while let Some(event) = next.take() {
            next = app.event(&event);
        }
        if app.data_table_state.is_some() && !app.is_busy() && !app.awaiting_dataset() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the open never ended");
        next = rx.recv_timeout(std::time::Duration::from_millis(50)).ok();
    }
    sender.join().unwrap();
    assert_eq!(app.views.active_id.as_deref(), Some(view.id.as_str()));
    assert!(
        columns(&app).iter().any(|c| c == "k1"),
        "the rows shown are the view's: {:?}",
        columns(&app)
    );
}

/// Applying a view plans its steps and returns: the pivot is read by a worker,
/// with the table as it was and busy meanwhile, and installed when it is in.
#[test]
fn a_view_returns_before_its_pivot_is_read_and_installs_when_it_is() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let view = pivot_view(&mut app, "pivot");

    assert!(app.apply_view(&view).is_ok());
    assert!(app.is_busy(), "the pivot is read in the background");
    assert!(app.view_applying());
    assert_eq!(columns(&app), ["id", "key", "val"], "nothing changed yet");
    assert!(app.views.active_id.is_none());

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert!(!app.error_modal.active, "{}", app.error_modal.message);
    assert_eq!(columns(&app), ["id", "k1", "k2"]);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_some());
    assert_eq!(state.display_df().map(|df| df.height()), Some(5));
    assert_eq!(app.views.active_id.as_deref(), Some(view.id.as_str()));
}

/// Applying a view counts the use on the stored view: a rename made by another
/// instance since this one read the views is kept, and a view another instance
/// deleted is not written back.
#[test]
fn applying_a_view_keeps_another_instances_edit_and_delete() {
    let (mut app, rx, tx, dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "sorted");
    view.settings.pivot = None;
    view.settings.sort_columns = vec!["val".to_string()];
    let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
    let mut other = ViewManager::new(&config).unwrap();
    let mut renamed = other.get_view_by_id(&view.id).cloned().unwrap();
    renamed.name = "renamed elsewhere".to_string();
    other.update_view(&renamed).unwrap();

    assert!(app.apply_view(&view).is_ok());
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    let stored = ViewManager::new(&config).unwrap();
    let stored_view = stored.get_view_by_id(&view.id).unwrap();
    assert_eq!(stored_view.name, "renamed elsewhere");
    assert_eq!(stored_view.usage_count, 1);

    other.delete_view(&view.id).unwrap();
    assert!(app.apply_view(&view).is_ok());
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert!(!app.error_modal.active, "{}", app.error_modal.message);
    let stored = ViewManager::new(&config).unwrap();
    assert!(stored.all_views().is_empty(), "the deleted view came back");
    assert!(app.views.manager.get_view_by_id(&view.id).is_none());
}

/// Without a pivot every step plans at once, and the first rows are read in the
/// background.
#[test]
fn a_view_returns_before_its_rows_are_read() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "sorted");
    view.settings.pivot = None;
    view.settings.sort_columns = vec!["val".to_string()];
    view.settings.sort_descending = vec![true];

    assert!(app.apply_view(&view).is_ok());
    assert!(app.is_busy(), "the rows are read in the background");
    assert!(app.view_applying());
    assert!(
        !app.data_table_state.as_ref().unwrap().is_num_rows_valid(),
        "not even counted"
    );

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    let state = app.data_table_state.as_ref().unwrap();
    let first = state.display_df().unwrap().column("val").unwrap().get(0);
    assert_eq!(first.unwrap(), AnyValue::Int64(40));
    assert_eq!(app.views.active_id.as_deref(), Some(view.id.as_str()));
}

/// A view that pivots and then fails must roll the pivot back too: otherwise the
/// view shows the original columns while SQL still runs against the pivot.
#[test]
fn a_failed_view_rolls_back_the_reshape() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "pivot then break");
    // Applied after the pivot, and referring to a column that does not exist.
    view.settings.column_order = vec!["no_such_column".to_string()];

    let shown = app.data_table_state.as_ref().unwrap().display_df().cloned();
    assert!(app.apply_view(&view).is_ok(), "the pivot plans");
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert!(app.error_modal.active, "the step after it fails");

    let state = app.data_table_state.as_ref().unwrap();
    let root: Vec<String> = state
        .query_root()
        .collect_schema()
        .unwrap()
        .iter_names()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        root,
        vec!["id", "key", "val"],
        "SQL root is the loaded data again"
    );
    assert!(state.last_pivot_spec().is_none());
    assert!(state.reshaped_lf_clone().is_none());
    assert_eq!(state.display_df(), shown.as_ref(), "with its rows");
    assert!(app.views.active_id.is_none());
}

/// A pivot that fails on the data, in the worker, leaves the table as it was.
#[cfg(feature = "sql")]
#[test]
fn a_view_whose_pivot_fails_on_the_data_changes_nothing() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "cast then pivot");
    view.settings.reshape_source = Some(pivot_melt_modal::ReshapeSource {
        sql_query: Some("SELECT id, key, CAST(key AS INT) AS val FROM df".to_string()),
        ..Default::default()
    });

    let shown = app.data_table_state.as_ref().unwrap().display_df().cloned();
    assert!(app.apply_view(&view).is_ok(), "it plans");
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());

    assert!(app.error_modal.active, "the failure is said");
    assert_eq!(columns(&app), ["id", "key", "val"]);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.get_active_sql_query().is_empty());
    assert!(state.last_pivot_spec().is_none());
    assert_eq!(state.display_df(), shown.as_ref(), "with its rows");
    assert!(app.views.active_id.is_none());
}

/// A view saved with SQL, applied in a build without `sql`: it fails and says the
/// build is why, and the table keeps what it showed.
#[cfg(not(feature = "sql"))]
#[test]
fn a_sql_view_without_the_sql_feature_says_why() {
    let (mut app, _rx, _tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "sql view");
    view.settings.pivot = None;
    view.settings.sql_query = Some("SELECT id FROM df".to_string());
    let error = match app.apply_view(&view) {
        Err(error) => error.to_string(),
        Ok(()) => panic!("a SQL view applied without SQL"),
    };
    assert!(
        error.contains("SQL is not supported in this build"),
        "{error}"
    );
    assert_eq!(columns(&app), ["id", "key", "val"]);
    assert!(app.views.active_id.is_none());
}

/// While a view's pivot or rows are read, the bar says Esc stops it.
#[test]
fn the_bar_offers_esc_while_a_view_applies() {
    for pivot in [true, false] {
        let (mut app, rx, tx, _dir) = long_csv_app();
        let mut view = pivot_view(&mut app, "view");
        if !pivot {
            view.settings.pivot = None;
            view.settings.column_order = vec!["id".to_string(), "val".to_string()];
        }
        let bar = footer_text(&mut app);
        assert!(!bar.contains("Stop"), "nothing to stop yet: {bar}");
        assert!(app.apply_view(&view).is_ok());
        let bar = footer_text(&mut app);
        assert!(
            bar.contains("Applying view") && bar.contains("Esc Stop"),
            "pivot {pivot}: {bar}"
        );
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    }
}

/// The Pivot & Melt form's pivot says the same while it is read.
#[test]
fn the_bar_offers_esc_while_a_pivot_is_computed() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('p'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::PivotMelt);
    app.event(&AppEvent::Pivot(pivot_melt_modal::PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: pivot_melt_modal::PivotAggregation::First,
    }));
    let bar = footer_text(&mut app);
    assert!(
        bar.contains("Computing pivot") && bar.contains("Esc Stop"),
        "{bar}"
    );
    assert!(!bar.contains("Help"), "? is held at the form: {bar}");
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
}

/// Esc while a view's pivot is read acts at once, even with keys held, and keeps
/// the table; the pivot's answer is dropped when it lands.
#[test]
fn esc_cancels_a_view_being_pivoted() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let view = pivot_view(&mut app, "pivot");
    assert!(app.apply_view(&view).is_ok());

    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.hard_escape_while_busy(&esc), "it jumps the queue");
    app.event(&AppEvent::Key(esc));
    assert!(!app.is_busy());
    assert!(!app.view_applying());
    assert_eq!(app.flash_message(), Some("View cancelled"));

    // The worker still answers; its answer is stale.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let event = rx.recv_timeout(std::time::Duration::from_secs(1));
        if let Ok(event) = event {
            let pivot = matches!(event, AppEvent::JobEnded(t) if t.kind() == JobKind::ViewPivot);
            if let Some(next) = app.event(&event) {
                let _ = tx.send(next);
            }
            if pivot {
                break;
            }
        }
        assert!(std::time::Instant::now() < deadline, "the pivot never came");
    }
    assert_eq!(columns(&app), ["id", "key", "val"]);
    assert!(app.views.active_id.is_none());
    assert!(!app.is_busy());
}

/// Esc while a view's first rows are read puts the view before it back.
#[test]
fn esc_cancels_a_view_being_read() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "narrow");
    view.settings.pivot = None;
    view.settings.column_order = vec!["id".to_string(), "val".to_string()];
    assert!(app.apply_view(&view).is_ok());
    assert!(app.view_applying());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_column_order(), ["id", "key", "val"]);
    assert_eq!(state.display_df().map(|df| df.width()), Some(3));
    assert!(app.views.active_id.is_none());
    assert!(!app.error_modal.active);
}

/// A pivot answering for a generation since passed is dropped, and the view it
/// belonged to is left waiting on its own.
#[test]
fn a_stale_view_pivot_is_dropped() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let view = pivot_view(&mut app, "pivot");
    // A view's pivot on a generation since passed.
    let passed = app.job_for_tests(Job::ViewPivot(Box::new((view.clone(), None))), None);
    app.jobs.advance();
    assert!(app.apply_view(&view).is_ok());

    let stale = polars::prelude::df!("id" => [1i64], "zz" => [2i64]).unwrap();
    let ticket = passed.ticket();
    passed.end(Outcome::answered(Answer::ViewPivoted(stale)));
    app.event(&AppEvent::JobEnded(ticket));
    assert_eq!(
        columns(&app),
        ["id", "key", "val"],
        "the stale one is dropped"
    );
    assert!(app.view_applying(), "the view still waits on its own");

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert_eq!(columns(&app), ["id", "k1", "k2"]);
}

/// A view applied on open reads the first rows itself: the dataset's own are never
/// asked for, and the view's pivot is what the table shows.
#[test]
fn a_view_applied_on_open_reads_the_rows_once() {
    let (mut app, rx, tx, dir) = long_csv_app();
    let view = pivot_view(&mut app, "on open");
    let loads = open_with_view(&mut app, &rx, &dir, &view, |_, _| false);
    drop(tx);
    assert_eq!(loads, 0, "the view reads the first rows");
    assert!(!app.error_modal.active, "{}", app.error_modal.message);
    assert_eq!(columns(&app), ["id", "k1", "k2"]);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.display_df().map(|df| df.height()), Some(5));
    assert!(app.nothing_loading());
}

/// A view's pivot whose worker dies on open is not applied, and the dataset's own
/// rows are read instead.
#[test]
fn a_view_whose_pivot_worker_dies_on_open_reads_the_dataset() {
    let (mut app, rx, tx, dir) = long_csv_app();
    let view = pivot_view(&mut app, "dies");
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, Job::ViewPivot(_)));
    open_with_view(&mut app, &rx, &dir, &view, |_, _| false);
    drop(tx);
    assert!(app.error_modal.active);
    assert!(
        app.error_modal.message.contains("worker died"),
        "{}",
        app.error_modal.message
    );
    assert!(!app.view_applying());
    assert_eq!(columns(&app), ["id", "key", "val"]);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_pivot_spec().is_none());
    assert!(state.display_df().is_some_and(|df| df.height() > 0));
    assert!(app.views.active_id.is_none());
    assert!(app.nothing_loading());
}

/// A view whose rows' worker dies puts the view before it back.
#[test]
fn a_view_whose_rows_worker_dies_rolls_back() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let mut view = pivot_view(&mut app, "narrow");
    view.settings.pivot = None;
    view.settings.column_order = vec!["id".to_string(), "val".to_string()];
    let shown = app.data_table_state.as_ref().unwrap().display_df().cloned();
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, Job::Rows(_)));
    assert!(app.apply_view(&view).is_ok());
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
    assert!(app.error_modal.active);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_column_order(), ["id", "key", "val"]);
    assert_eq!(state.display_df(), shown.as_ref());
    assert!(app.views.active_id.is_none());
}

/// `long.csv` sorted on `val` descending and filtered to `val > 0`, with the rows
/// that view shows.
fn sorted_and_filtered(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) -> Option<DataFrame> {
    use crate::filter_modal::{FilterOperator, LogicalOperator};
    app.event(&AppEvent::Sort(vec!["val".to_string()], vec![true]));
    super::chart_prepare_tests::pump(app, rx, tx, |a| !crate::tests::work_pending(a));
    app.event(&AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "val".to_string(),
        operator: FilterOperator::Gt,
        value: "0".to_string(),
        logical_op: LogicalOperator::And,
    }]));
    super::chart_prepare_tests::pump(app, rx, tx, |a| !crate::tests::work_pending(a));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 8);
    state.display_df().cloned()
}

/// Handle events until the app is idle. The worker reading the first rows, which
/// `worker_dies` was set to kill, panics.
fn pump_with_dying_rows(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    assert!(
        app.jobs.worker_dies.is_some(),
        "set before the rows are asked for"
    );
    super::chart_prepare_tests::pump(app, rx, tx, |a| !a.is_busy());
}

/// The view before the query is back: its rows, count, sort and filter.
fn assert_rolled_back(app: &App, shown: Option<&DataFrame>) {
    assert!(app.prompt.query_running.is_none());
    assert!(!app.is_busy());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.get_active_query().is_empty());
    assert!(state.get_active_sql_query().is_empty());
    assert_eq!(columns(app), ["id", "key", "val"]);
    assert_eq!(state.display_df(), shown);
    assert!(state.is_num_rows_valid());
    assert_eq!(state.num_rows(), 8);
    assert_eq!(state.get_sort_columns(), ["val"]);
    assert_eq!(state.get_sort_descending(), [true]);
    assert_eq!(state.get_filters().len(), 1);
}

/// #432: a query run from the prompt whose rows' worker dies is not applied, and
/// the reason is under the query, as when the rows fail.
#[cfg(feature = "sql")]
#[test]
fn a_prompt_query_whose_rows_worker_dies_rolls_back() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let shown = sorted_and_filtered(&mut app, &rx, &tx);
    let press = |app: &mut App, code: KeyCode| {
        let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
        while let Some(event) = next.take() {
            next = app.event(&event);
        }
    };
    press(&mut app, KeyCode::Char(':'));
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    app.prompt
        .sql_input
        .set_value("SELECT id FROM df WHERE val > 5");
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, Job::Rows(_)));
    press(&mut app, KeyCode::Enter);
    assert!(app.prompt.query_running.is_some(), "the query planned");
    pump_with_dying_rows(&mut app, &rx, &tx);

    assert_rolled_back(&app, shown.as_ref());
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    assert!(
        app.query_prompt_error()
            .is_some_and(|error| error.contains("worker died")),
        "{:?}",
        app.query_prompt_error()
    );
    assert!(!app.error_modal.active, "no modal over the prompt");
    assert!(app.nothing_loading());
}

/// #432: a query sent without the prompt whose rows' worker dies is not applied,
/// and a dialog says why.
#[test]
fn a_query_whose_rows_worker_dies_rolls_back() {
    let (mut app, rx, tx, _dir) = long_csv_app();
    let shown = sorted_and_filtered(&mut app, &rx, &tx);
    app.jobs.worker_dies = crate::tests::worker_dies_once(|job| matches!(job, Job::Rows(_)));
    app.event(&AppEvent::QQuery("select id where val > 5".to_string()));
    assert!(app.prompt.query_running.is_some(), "the query planned");
    pump_with_dying_rows(&mut app, &rx, &tx);

    assert_rolled_back(&app, shown.as_ref());
    assert!(app.error_modal.active);
    assert!(
        app.error_modal.message.contains("worker died"),
        "{}",
        app.error_modal.message
    );
}

/// Rolling a failed view back restores the frame, and the frame's rows still
/// stand for rows of a file — so what the state believes about them has to be
/// rolled back with it, or the cells go back to reading as plain nulls.
#[test]
fn a_failed_view_rolls_back_what_the_rows_knew() {
    use polars::prelude::{ParquetWriter, df};
    let dir = tempfile::tempdir().unwrap();
    let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    };
    write("date=2024-01-01", df!("id" => &[1i64, 4]).unwrap());
    write(
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    app.input_mode = InputMode::Normal;
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });
    assert!(
        app.data_table_state.as_ref().unwrap().drifts(),
        "the directory drifts to begin with"
    );

    let mut view = app
        .create_view_from_current_state(
            "query then break".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.sql_query = Some("select * from df".to_string());
    // Applied after the query, and referring to a column that does not exist.
    view.settings.column_order = vec!["no_such_column".to_string()];

    assert!(app.apply_view(&view).is_err());

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.drifts(),
        "the rollback puts back what the restored frame carries"
    );
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "extra"],
        "and no hidden column with it"
    );
}

/// The rollback puts back what the dataset said, not what the view was saying.
///
/// A note about rows a sort is leaving out belongs to the sort. Snapshotting it
/// with the dataset's own notes and handing it back on rollback made it permanent
/// — it outlived the sort that earned it, and sorting again added a second copy —
/// because the field it is handed back into is the one only a reset clears.
#[test]
fn a_failed_view_does_not_make_the_views_note_permanent() {
    use polars::prelude::{ParquetWriter, df};
    let dir = tempfile::tempdir().unwrap();
    let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    };
    write(
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write(
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    app.input_mode = InputMode::Normal;
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });

    let left_out = |app: &App| -> usize {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .filter(|note| note.summary.contains("left out of the"))
            .count()
    };

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(vec!["n".to_string()], true);
    assert_eq!(left_out(&app), 1, "the sort has something to say");

    let mut view = app
        .create_view_from_current_state(
            "query then break".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.sql_query = Some("select * from df".to_string());
    view.settings.column_order = vec!["no_such_column".to_string()];
    assert!(app.apply_view(&view).is_err());

    // The sort is back, so the rows it leaves out are back out — and the note has
    // to be back with them. A frame three rows short of the dataset with nothing
    // on screen saying why is the same fault as a note that outlives its sort,
    // seen from the other side.
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.view_sort_columns(),
        ["n"],
        "the rollback puts the sort back"
    );
    assert_eq!(
        state.lf().clone().collect().unwrap().height(),
        3,
        "and the frame still leaves the two rows out"
    );
    assert_eq!(left_out(&app), 1, "so the note is still there to say so");

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(Vec::new(), true);
    assert_eq!(
        left_out(&app),
        0,
        "and clearing the sort takes it away, rollback or no rollback"
    );

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(vec!["n".to_string()], true);
    assert_eq!(left_out(&app), 1, "sorting again says it once, not twice");
}

/// A native List column does not make a table grouped: with no grouping query
/// behind it, a drill is a no-op that leaves the frame and its note alone. Queries
/// drop the drift column, so a grouped view never carries one of these notes.
#[test]
fn a_native_list_column_does_not_drill_and_keeps_the_views_note() {
    use polars::prelude::{IntoLazy, ParquetWriter, df};
    let dir = tempfile::tempdir().unwrap();
    let write = |sub: &str, frame: polars::prelude::DataFrame| {
        // A native List column, written to the file.
        let mut frame = frame
            .lazy()
            .group_by([col("id"), col("n")])
            .agg([col("v")])
            .sort(["id"], Default::default())
            .collect()
            .unwrap();
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    };
    write(
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2], "v" => &[10i64, 11, 12]).unwrap(),
    );
    // `n` as text here, so it is not read from this file.
    write(
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"], "v" => &[13i64, 14]).unwrap(),
    );

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    app.input_mode = InputMode::Normal;
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });

    let state = app.data_table_state.as_mut().unwrap();
    assert!(
        !state.is_grouped(),
        "a native List column, with no group-by"
    );
    assert!(!state.can_drill_down());
    assert!(state.drifts(), "and the files disagree on `n`");

    let left_out = |s: &crate::table::DataTableState| {
        s.notes()
            .iter()
            .filter(|note| note.summary.contains("left out of the"))
            .count()
    };

    state.sort(vec!["n".to_string()], true);
    assert_eq!(
        state.lf().clone().collect().unwrap().height(),
        3,
        "the sort leaves the two rows of the text file out"
    );
    assert_eq!(left_out(state), 1, "and says so");

    state.table_state.select(Some(0));
    state.drill_down_into_group(0).unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
    assert_eq!(left_out(state), 1, "the note stays: {:#?}", state.notes());
}

/// A rollback that stops half way leaves a state that is neither the view's nor
/// the user's, and the note then describes the half that lost.
///
/// The user has no sort at all; the view brings one, on a column the files
/// disagree on, and then fails on a column order that does not fit. Every step of
/// the rollback used to be guarded on the one before, and the first of them
/// collected against the view's column order and errored — so the view's
/// sort stayed in the sidebar, the notes were built from it, and the row counter
/// reported a frame three rows shorter than the one on screen.
#[test]
fn a_rollback_that_fails_early_still_puts_all_of_the_view_back() {
    use polars::prelude::{ParquetWriter, df};
    let dir = tempfile::tempdir().unwrap();
    let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let f = std::fs::File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
    };
    write(
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write(
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    app.input_mode = InputMode::Normal;
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
        a.data_table_state.is_some() && !a.is_busy()
    });
    app.data_table_state.as_mut().unwrap().mark_notes_seen();
    let order_before = app
        .data_table_state
        .as_ref()
        .unwrap()
        .get_column_order()
        .to_vec();

    let mut view = app
        .create_view_from_current_state(
            "sort then break".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    // A sort the user never asked for, and a column order that cannot be applied.
    view.settings.sort_columns = vec!["n".to_string()];
    view.settings.column_order = vec!["no_such_column".to_string()];
    assert!(app.apply_view(&view).is_err());

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.view_sort_columns().is_empty(),
        "the view's sort does not survive its own failure"
    );
    assert_eq!(
        state.get_column_order(),
        order_before,
        "nor does the column order it failed on"
    );
    assert_eq!(
        state.lf().clone().collect().unwrap().height(),
        5,
        "the user's frame is whole"
    );
    assert_eq!(
        state
            .notes()
            .iter()
            .filter(|note| note.summary.contains("left out of the"))
            .count(),
        0,
        "so nothing says rows went: {:#?}",
        state.notes()
    );
    assert!(
        !state.notes_unseen(),
        "and a rollback is not news, so the accent stays where the user left it"
    );
    assert!(state.error().is_none(), "with no error left over");
}

/// A view whose SQL drops a column that the same view's sort names. The
/// sorted frame cannot be built at all, so the row count errors — and reporting
/// that as zero rows used to blank the table and return before `load_buffer`, the
/// only other place a failure is recorded. `apply_view` decides whether to roll
/// back by looking for an error, found none, and returned `Ok`: the user was left
/// with a blank table wearing the view's sort, told nothing.
#[test]
fn a_view_whose_sort_names_a_column_its_query_removed_fails_loudly() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("three.csv");
    std::fs::write(&path, "id,keep,dropped\n0,a,7\n1,b,8\n2,c,9\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);

    let mut view = app
        .create_view_from_current_state(
            "sort what the query dropped".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.sql_query = Some("select id, keep from df".to_string());
    // Applied after the query, and naming the column the query just dropped.
    view.settings.sort_columns = vec!["dropped".to_string()];

    assert!(
        app.apply_view(&view).is_err(),
        "the view fails, rather than quietly leaving a blank table"
    );

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.view_sort_columns().is_empty(),
        "the sort it failed on does not survive"
    );
    assert!(
        state.get_active_sql_query().is_empty(),
        "nor does the query that dropped the column"
    );
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["id", "keep", "dropped"], "the user's frame is back");
    assert_eq!(
        state.lf().clone().collect().unwrap().height(),
        3,
        "with its rows, rather than the blank table the failure used to leave"
    );
    assert!(state.error().is_none(), "and the rollback clears the error");
}

#[cfg(feature = "sql")]
fn words_csv(dir: &tempfile::TempDir) -> PathBuf {
    let path = dir.path().join("words.csv");
    let mut csv = String::from("id,name\n");
    for i in 0..40 {
        csv.push_str(&format!("{i},word_{i}\n"));
    }
    std::fs::write(&path, csv).unwrap();
    path
}

/// #400 through a view: a view whose SQL plans but fails on the data is not
/// left installed. The error is a dialog, over the view as it was.
#[cfg(feature = "sql")]
#[test]
fn a_view_whose_query_fails_on_the_data_is_not_applied() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = words_csv(&dir);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);

    let mut view = app
        .create_view_from_current_state(
            "cast the words".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.sql_query = Some("SELECT CAST(name AS INT) AS n FROM df".to_string());
    view.settings.column_order.clear();
    let applied = app.apply_view(&view);
    assert!(applied.is_ok(), "it plans: {applied:?}");
    // As the event loop does: the frame drawn asks for its rows.
    let area = ratatui::layout::Rect::new(0, 0, 80, 24);
    app.render(area, &mut ratatui::buffer::Buffer::empty(area));
    let state = app.data_table_state.as_mut().unwrap();
    assert!(std::mem::take(&mut state.needs_recollect));
    app.spawn_async_collect(App::LOADING_BUFFER);
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());

    assert!(app.error_modal.active, "the failure is said");
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["id", "name"],
        "the frame is the one before the view"
    );
    assert!(state.get_active_sql_query().is_empty());
    assert!(state.is_num_rows_valid());
    assert_eq!(state.num_rows(), 40);
    assert_ne!(
        app.views.active_id.as_deref(),
        Some(view.id.as_str()),
        "the view that failed is not marked applied"
    );
}

/// The count of the view a query replaced can land while the query runs. If the
/// query then fails, the view comes back with that count rather than with a
/// marker saying it is still being counted, which nothing would ever clear.
#[cfg(feature = "sql")]
#[test]
fn a_count_that_lands_while_a_query_runs_comes_back_with_the_view() {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = words_csv(&dir);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);

    // As if the view's count were still running when the query was sent.
    let state = app.data_table_state.as_mut().unwrap();
    state.invalidate_num_rows();
    let counting = state.len_generation();
    app.counting.len_count_inflight = Some(counting);

    app.event(&AppEvent::SqlQuery(
        "SELECT CAST(name AS INT) AS n FROM df".to_string(),
    ));
    assert!(app.prompt.query_running.is_some());
    app.event(&AppEvent::BackgroundLenReady {
        len_generation: counting,
        num_rows: 40,
        file_row_groups: None,
    });
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());

    assert!(app.prompt.query_running.is_none());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.len_generation(), counting, "the view is back");
    assert!(state.is_num_rows_valid(), "with its count");
    assert_eq!(state.num_rows(), 40);
    assert_ne!(
        app.counting.len_count_inflight,
        Some(counting),
        "not left counting"
    );
}

/// A view whose SQL groups another way and then fails leaves the grouped view it
/// rolled back to drilling by that view's own keys, not the failed view's.
#[cfg(feature = "sql")]
#[test]
fn a_failed_view_rolls_back_what_a_grouped_row_drills_into() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("depts.csv");
    std::fs::write(&path, "dept,salary\neng,1\nops,2\neng,3\nops,4\neng,5\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, path);
    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("SELECT dept, COUNT(*) AS n FROM df GROUP BY dept".to_string());
    assert!(state.error().is_none(), "{:?}", state.error());

    let mut view = app
        .create_view_from_current_state(
            "group another way then break".to_string(),
            None,
            view::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    view.settings.sql_query =
        Some("SELECT salary > 2 AS dept, COUNT(*) AS n FROM df GROUP BY 1".to_string());
    view.settings.column_order = vec!["no_such_column".to_string()];
    assert!(app.apply_view(&view).is_err());

    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(0).unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["eng".to_string()])
    );
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
}

/// A view of `long.csv` that does nothing yet.
fn blank_view(app: &mut App, name: &str) -> SavedView {
    let mut view = pivot_view(app, name);
    view.settings = view::ViewSettings {
        chart: None,
        sample: None,
        query: None,
        sql_query: None,
        fuzzy_query: None,
        filters: Vec::new(),
        sort_columns: Vec::new(),
        sort_descending: Vec::new(),
        sort_ascending: true,
        column_order: Vec::new(),
        locked_columns_count: 0,
        pivot: None,
        melt: None,
        reshape_source: None,
        columns: Vec::new(),
    };
    view
}

/// #458: a view that fails after any one of its steps — while it is planned, once
/// its pivot is read, or when its rows are — puts back all of the view it was
/// applied over: the rows each stage of the pipeline reads, the schema, query,
/// filters, sort, layout, reshape, notes and selection, and the count and buffer,
/// which stay valid for it, so nothing is read again.
#[test]
fn a_view_failing_after_any_step_puts_the_view_back() {
    use crate::filter_modal::{FilterOperator, LogicalOperator};
    let filter = |column: &str| FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator: FilterOperator::Gt,
        value: "0".to_string(),
        logical_op: LogicalOperator::And,
    };
    let melt = |value: &str| MeltSpec {
        index: vec!["id".to_string()],
        value_columns: vec![value.to_string()],
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    let pivot = || PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: pivot_melt_modal::PivotAggregation::First,
    };
    enum Fails {
        /// While planning: `apply_view` says so and nothing is read.
        Planning,
        /// In the background, once the pivot is in or the rows are read.
        Reading,
        /// Every step plans; the worker reading the pivot or the rows dies.
        WorkerDies,
    }
    type Steps = Box<dyn Fn(&mut view::ViewSettings)>;
    let cases: Vec<(&str, Fails, Steps)> = vec![
        (
            "the query",
            Fails::Planning,
            Box::new(|s| s.query = Some("select nope".to_string())),
        ),
        (
            "a filter after the query",
            Fails::Planning,
            Box::new(move |s| {
                s.query = Some("select id, val".to_string());
                s.filters = vec![filter("key")];
            }),
        ),
        (
            "a sort after the filter",
            Fails::Planning,
            Box::new(move |s| {
                s.query = Some("select id, val".to_string());
                s.filters = vec![filter("val")];
                s.sort_columns = vec!["key".to_string()];
            }),
        ),
        (
            "the query a melt runs over",
            Fails::Planning,
            Box::new(move |s| {
                s.melt = Some(melt("val"));
                s.reshape_source = Some(pivot_melt_modal::ReshapeSource {
                    query: Some("select nope".to_string()),
                    ..Default::default()
                });
            }),
        ),
        (
            "the melt",
            Fails::Planning,
            Box::new(move |s| s.melt = Some(melt("nope"))),
        ),
        (
            "a filter after the melt",
            Fails::Planning,
            Box::new(move |s| {
                s.melt = Some(melt("val"));
                s.filters = vec![filter("key")];
            }),
        ),
        (
            "the layout after the sort",
            Fails::Planning,
            Box::new(|s| {
                s.sort_columns = vec!["val".to_string()];
                s.column_order = vec!["no_such_column".to_string()];
            }),
        ),
        (
            "a sort after the pivot is read",
            Fails::Reading,
            Box::new(move |s| {
                s.pivot = Some(pivot());
                s.sort_columns = vec!["val".to_string()];
            }),
        ),
        (
            "reading the pivot",
            Fails::WorkerDies,
            Box::new(move |s| s.pivot = Some(pivot())),
        ),
        (
            "reading the rows after a query, filter and sort",
            Fails::WorkerDies,
            Box::new(move |s| {
                s.query = Some("select id, val where val > 5".to_string());
                s.filters = vec![filter("id")];
                s.sort_columns = vec!["id".to_string()];
            }),
        ),
    ];
    for (step, fails, steps) in cases {
        let (mut app, rx, tx, _dir) = long_csv_app();
        // The view it is applied over: a query, a sort and a filter, a selection.
        app.event(&AppEvent::QQuery(
            "select id, key, val where val >= 0".to_string(),
        ));
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
        sorted_and_filtered(&mut app, &rx, &tx);
        let state = app.data_table_state.as_mut().unwrap();
        state.table_state.select(Some(2));
        let before = state.snapshot();
        assert!(before.has_rows(), "{step}: the view's rows are on hand");

        let mut view = blank_view(&mut app, step);
        steps(&mut view.settings);
        if matches!(fails, Fails::WorkerDies) {
            // The view's first read, its pivot or its rows, panics.
            app.jobs.worker_dies = crate::tests::worker_dies_once(|job| {
                matches!(job, Job::ViewPivot(_) | Job::Rows(_))
            });
        }
        let applied = app.apply_view(&view);
        match fails {
            Fails::Planning => assert!(applied.is_err(), "{step}: fails as it plans"),
            Fails::Reading => {
                assert!(applied.is_ok(), "{step}: plans: {applied:?}");
                super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
                assert!(app.error_modal.active, "{step}: the failure is said");
            }
            Fails::WorkerDies => {
                assert!(applied.is_ok(), "{step}: plans: {applied:?}");
                super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !a.is_busy());
                assert!(
                    app.error_modal.message.contains("worker died"),
                    "{step}: the failure is said: {}",
                    app.error_modal.message
                );
            }
        }

        assert!(app.prompt.query_running.is_none(), "{step}");
        assert!(!app.view_applying(), "{step}");
        assert!(!app.is_busy(), "{step}: nothing is left to read");
        assert!(app.views.active_id.is_none(), "{step}: not marked applied");
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.snapshot(), before, "{step}: the view is put back");
    }
}

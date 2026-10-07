use crate::*;
use polars::datatypes::AnyValue;
use std::sync::mpsc;

fn app() -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    tempfile::TempDir,
) {
    crate::tests::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    let mut body = String::from("a,b\n");
    for a in 0..30 {
        body.push_str(&format!("{a},{}\n", a * 2));
    }
    std::fs::write(&path, body).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    super::chart_prepare_tests::open(&mut app, &rx, &tx, path);
    (app, rx, tx, dir)
}

#[test]
fn r_reverses_in_the_background() {
    let (mut app, rx, tx, _dir) = app();
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('r'),
        KeyModifiers::NONE,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.snapshot().has_rows(),
        "nothing was read on this thread"
    );
    assert!(app.is_busy(), "the rows are being read");

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
    let state = app.data_table_state.as_ref().unwrap();
    let shown = state.display_df().unwrap().column("a").unwrap().get(0);
    assert_eq!(shown.unwrap(), AnyValue::Int64(29));
}

#[test]
fn a_column_order_is_read_in_the_background() {
    let (mut app, rx, tx, _dir) = app();
    app.event(AppEvent::Applied(crate::Applied::ColumnOrder(
        vec!["b".to_string(), "a".to_string()],
        1,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.snapshot().has_rows(),
        "nothing was read on this thread"
    );
    assert!(app.is_busy(), "the rows are being read");

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_column_order(), ["b", "a"]);
    assert_eq!(state.locked_columns_count(), 1);
    assert!(state.snapshot().has_rows());
}

/// A read still out was planned with the columns as they were. A new order does
/// not wait on it, or a column it brings back never arrives.
#[test]
fn a_column_order_does_not_wait_on_a_read_of_other_columns() {
    let (mut app, rx, tx, _dir) = app();
    app.event(AppEvent::Applied(crate::Applied::ColumnOrder(
        vec!["a".to_string()],
        0,
    )));
    app.event(AppEvent::Applied(crate::Applied::ColumnOrder(
        vec!["b".to_string(), "a".to_string()],
        0,
    )));
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
    let state = app.data_table_state.as_ref().unwrap();
    let shown: Vec<String> = state
        .display_df()
        .expect("the rows are read")
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    assert_eq!(shown, ["b", "a"]);
}

/// One Apply in the sort and filter sidebar reads the page once, however much it
/// changed: a filter and a sort together are one view, not two reads.
#[test]
fn one_sidebar_apply_reads_one_page() {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let (mut app, rx, tx, _dir) = app();
    app.sync_sort_filter_modal();
    let before = app.home_app.reads.pages;
    app.sort_filter_modal.filter.statements = vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "3".to_string(),
        logical_op: LogicalOperator::And,
    }];
    let b = app
        .sort_filter_modal
        .sort
        .columns
        .iter_mut()
        .find(|c| c.name == "b")
        .unwrap();
    b.sort_order = Some(1);
    b.sort_descending = true;
    if let Some(next) = app.apply_sort_filter() {
        let _ = tx.send(next);
    }
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
    assert_eq!(
        app.home_app.reads.pages - before,
        1,
        "one read for one apply"
    );
    let state = app.data_table_state.as_ref().unwrap();
    let shown = state.display_df().unwrap().column("a").unwrap().get(0);
    assert_eq!(shown.unwrap(), AnyValue::Int64(29), "filtered and sorted");
}

/// A sidebar apply says what it does: sorting, filtering, or only reading the
/// page again for a change of columns.
#[test]
fn a_sidebar_apply_says_what_it_does() {
    use crate::app::modals::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let (mut app, rx, tx, _dir) = app();
    let filter = FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "3".to_string(),
        logical_op: LogicalOperator::And,
    };
    let order = vec!["b".to_string(), "a".to_string()];
    for (filters, sort, says) in [
        (vec![], vec![], App::LOADING_BUFFER),
        (vec![filter.clone()], vec![], "Filtering..."),
        (vec![filter], vec!["b".to_string()], "Sorting..."),
    ] {
        let descending = vec![false; sort.len()];
        app.event(AppEvent::Applied(crate::Applied::ApplyView(
            order.clone(),
            0,
            filters,
            sort,
            descending,
        )));
        assert_eq!(app.status_message.as_deref(), Some(says));
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !crate::tests::work_pending(a));
    }
}

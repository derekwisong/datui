use crate::*;
use polars::prelude::IntoLazy;

fn app_with(columns: &[&str]) -> App {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::prelude::DataFrame::new(
        2,
        columns
            .iter()
            .map(|name| polars::prelude::Column::new((*name).into(), [1i64, 2]))
            .collect(),
    )
    .unwrap();
    app.data_table_state =
        Some(DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap());
    app
}

fn names(order: &[&str]) -> Vec<String> {
    order.iter().map(|s| s.to_string()).collect()
}

/// The sidebar's last applied order places a hidden column only while the table
/// still shows that order; once something else (a view) set the order, the
/// schema does.
#[test]
fn a_stale_applied_order_does_not_place_hidden_columns() {
    let mut app = app_with(&["a", "b", "c", "d"]);
    app.sort_filter_modal.sort.applied_order = names(&["c", "b", "a", "d"]);
    let state = app.data_table_state.as_mut().unwrap();

    state.set_column_order(names(&["c", "a", "d"]));
    app.sync_sort_filter_modal();
    assert_eq!(
        app.sort_filter_modal.sort.get_full_column_order(),
        names(&["c", "b", "a", "d"])
    );

    // A view put a first: the applied order no longer describes the table.
    let state = app.data_table_state.as_mut().unwrap();
    state.set_column_order(names(&["a", "c", "d"]));
    app.sync_sort_filter_modal();
    assert_eq!(
        app.sort_filter_modal.sort.get_full_column_order(),
        names(&["a", "b", "c", "d"]),
        "b follows a, as in the schema"
    );
}

/// A hidden column that ended the frozen span is frozen again on reopen; a
/// table whose lock changed since freezes what it says.
#[test]
fn a_hidden_column_at_the_end_of_the_lock_stays_locked() {
    let mut app = app_with(&["a", "b", "c"]);
    app.sort_filter_modal.sort.applied_order = names(&["a", "b", "c"]);
    app.sort_filter_modal.sort.applied_locked = 2;
    let state = app.data_table_state.as_mut().unwrap();
    state.set_column_order(names(&["a", "c"]));
    state.set_locked_columns(1);
    app.sync_sort_filter_modal();
    let locked = |app: &App| {
        let mut cols: Vec<_> = app.sort_filter_modal.sort.columns.iter().collect();
        cols.sort_by_key(|c| c.display_order);
        cols.iter().map(|c| c.is_locked).collect::<Vec<_>>()
    };
    assert_eq!(locked(&app), [true, true, false]);

    app.data_table_state.as_mut().unwrap().set_locked_columns(2);
    app.sync_sort_filter_modal();
    assert_eq!(locked(&app), [true, true, true]);
}

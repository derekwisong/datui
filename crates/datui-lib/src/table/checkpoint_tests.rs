use super::*;
use crate::filter_modal::{FilterOperator, LogicalOperator};

fn state() -> DataTableState {
    let lf = df!(
        "id" => (0..20i64).collect::<Vec<_>>(),
        "key" => (0..20).map(|i| if i % 2 == 0 { "a" } else { "b" }).collect::<Vec<_>>(),
        "val" => (0..20i64).map(|i| i * 10).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::from_lazyframe(lf, &crate::OpenOptions::default()).unwrap();
    state.visible_rows = 4;
    state.collect();
    state
}

fn filter(column: &str, op: FilterOperator, value: &str) -> FilterStatement {
    FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator: op,
        value: value.to_string(),
        logical_op: LogicalOperator::And,
    }
}

/// A view with something at every stage: a query, a filter, a sort, a layout, a
/// selection off the top, and its rows and count read.
fn busy_view() -> DataTableState {
    let mut state = state();
    state.query("select id, val, key where val >= 20".to_string());
    state.filter(vec![filter("val", FilterOperator::Lt, "170")]);
    state.sort_by(vec!["val".to_string()], vec![true]);
    state.set_column_order(vec!["val".to_string(), "id".to_string(), "key".to_string()]);
    state.set_locked_columns(1);
    state.scroll_to(3);
    state.collect();
    state.table_state.select(Some(2));
    assert!(state.error().is_none(), "{:?}", state.error());
    assert!(state.is_num_rows_valid());
    assert!(state.display_df().is_some());
    state
}

/// The same view over a melt, so a reshape is what a failure must leave in place.
fn melted_view() -> DataTableState {
    let mut state = state();
    state
        .melt(&MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["val".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        })
        .unwrap();
    state.filter(vec![filter("id", FilterOperator::Gt, "3")]);
    state.collect();
    state.table_state.select(Some(1));
    assert!(state.error().is_none(), "{:?}", state.error());
    state
}

/// Fails as a view's step does: by the error the step leaves, or at the plan.
fn planned(state: &mut DataTableState) -> std::result::Result<(), String> {
    if let Some(e) = state.error() {
        return Err(e.to_string());
    }
    state.check_plan().map_err(|e| e.to_string())
}

/// Each step a view replays, ending in one that fails: every one puts back the
/// whole view, and none of them reads a row on the way.
#[test]
fn a_transition_failing_after_each_step_puts_the_view_back() {
    type Steps = fn(&mut DataTableState) -> std::result::Result<(), String>;
    let cases: [(&str, Steps); 8] = [
        ("the query", |s| {
            s.query("select nope".to_string());
            planned(s)
        }),
        ("a filter after the query", |s| {
            s.query("select id, val".to_string());
            s.filter(vec![filter("key", FilterOperator::Eq, "a")]);
            planned(s)
        }),
        ("a sort after the filter", |s| {
            s.query("select id, val".to_string());
            s.filter(vec![filter("val", FilterOperator::Gt, "0")]);
            s.sort_by(vec!["key".to_string()], vec![false]);
            planned(s)
        }),
        ("a melt after the query", |s| {
            s.query("select id, val".to_string());
            s.melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["nope".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .map_err(|e| e.to_string())?;
            planned(s)
        }),
        ("a filter after the melt", |s| {
            s.melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["val".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .map_err(|e| e.to_string())?;
            s.filter(vec![filter("val", FilterOperator::Gt, "0")]);
            planned(s)
        }),
        ("a sort after the melt", |s| {
            s.melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["val".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .map_err(|e| e.to_string())?;
            s.sort_by(vec!["key".to_string()], vec![false]);
            planned(s)
        }),
        ("the layout after the sort", |s| {
            s.query("select id, val".to_string());
            s.sort_by(vec!["val".to_string()], vec![false]);
            s.set_column_order(vec!["key".to_string()]);
            planned(s)
        }),
        ("a reset, then a query", |s| {
            s.reset();
            s.query("select id where nope > 1".to_string());
            planned(s)
        }),
    ];
    for prior in [busy_view as fn() -> DataTableState, melted_view] {
        for (step, steps) in cases {
            let mut state = prior();
            let before = state.snapshot();
            let failed = state.try_transition(steps);
            assert!(failed.is_err(), "{step}: the steps fail");
            assert_eq!(state.snapshot(), before, "{step}: the view is put back");
            assert!(
                state.prepare_async_collect(None).is_none(),
                "{step}: the rows on hand serve it, so nothing is read again"
            );
        }
    }
}

/// Steps that all plan read nothing; when the new view's rows then fail, the
/// checkpoint they returned puts back the view before them, rows and all.
#[test]
fn a_planned_view_whose_rows_fail_rolls_back_without_reading() {
    for prior in [busy_view as fn() -> DataTableState, melted_view] {
        let mut state = prior();
        let before = state.snapshot();
        let ((), saved) = state
            .try_transition(|s| {
                s.query("select id, val where id > 4".to_string());
                s.filter(vec![filter("val", FilterOperator::Lt, "150")]);
                s.sort_by(vec!["id".to_string()], vec![false]);
                planned(s)
            })
            .unwrap();
        assert!(!state.is_num_rows_valid(), "the view's count is not read");
        assert!(
            state.prepare_async_collect(None).is_some(),
            "nor its rows: both are left to the background read"
        );

        state.roll_back(saved);
        assert_eq!(state.snapshot(), before);
    }
}

/// The view before a query had no count yet; the count lands while the query's
/// rows are read and they fail. The count comes back with the view, as its own.
#[test]
fn a_count_that_lands_meanwhile_comes_back_with_its_view() {
    let mut state = busy_view();
    state.invalidate_num_rows();
    let counting = state.len_generation();
    let ((), mut saved) = state
        .try_transition(|s| {
            s.query("select id".to_string());
            planned(s)
        })
        .unwrap();

    assert!(
        !state.count_landed(counting, 7, None),
        "not a count of the frame on screen"
    );
    assert!(
        !saved.count_landed(state.len_generation(), 99, None),
        "nor is the query's count the view's"
    );
    assert!(saved.count_landed(counting, 7, None));

    state.roll_back(saved);
    assert_eq!(state.len_generation(), counting);
    assert_eq!(state.num_rows_if_valid(), Some(7));
}

/// A checkpoint over data that has since been replaced does not put its frames
/// over the new data; the view returns to the data as loaded instead, with no
/// rows on hand, for the next background read.
#[test]
fn a_checkpoint_over_replaced_data_returns_to_the_data() {
    let mut state = busy_view();
    let saved = state.rollback_point();
    let wider = df!(
        "id" => &[1i64, 2],
        "key" => &["a", "b"],
        "val" => &[10i64, 20],
        "more" => &[true, false],
    )
    .unwrap()
    .lazy();
    let schema = wider.clone().collect_schema().unwrap();
    state.replace_root(wider.clone(), schema.clone());

    state.roll_back(saved);
    assert_eq!(state.schema(), &schema);
    assert!(state.get_active_query().is_empty());
    assert!(state.get_filters().is_empty());
    assert!(state.get_sort_columns().is_empty());
    assert_eq!(
        state.lf().clone().collect().unwrap(),
        wider.collect().unwrap()
    );
    assert!(
        state.prepare_async_collect(None).is_some(),
        "its rows are read in the background"
    );
}

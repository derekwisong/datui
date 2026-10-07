//! Saved views: V, the list, applying one.

use super::*;

/// SQL runs against the data as loaded, like the DSL and fuzzy queries: a sidebar filter
/// that was active when the SQL ran is not baked into its result, so clearing the
/// filters afterwards shows the SQL result over the whole table.
#[cfg(feature = "sql")]
#[test]
fn test_sql_runs_against_the_loaded_data_not_the_filtered_view() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("filter_then_sql.csv");

    app.event(AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(AppEvent::SqlQuery(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30, "the SQL replaces the filter");
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_filters()
            .is_empty()
    );

    app.event(AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// A List column loaded from a file is data, not a group: Enter inspects the row and
/// leaves the view as it was. A `by` query that holds its groups as lists still drills.
#[test]
fn test_enter_inspects_a_loaded_list_column_and_drills_a_by_view() {
    let dir = common::fixture_dir().join("enter_list_column");
    // `t` holds lists as data; `x` is a plain column a `by` query can group on.
    let df = df!("k" => &[1i64, 1, 2, 3], "t" => &["a", "b", "c", "d"])
        .unwrap()
        .lazy()
        .group_by([col("k")])
        .agg([col("t"), (col("k").first() % lit(2i64)).alias("x")])
        .sort(["k"], Default::default())
        .collect()
        .unwrap();
    write_parquet(&dir, "", df);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.join("data.parquet")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 220, 30);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        !before.contains("Enter Drill"),
        "nothing to drill: {before}"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_grouped());
    let headers = state.headers();
    let schema = state.lf().clone().collect_schema().unwrap();

    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.overlay, Overlay::Inspect);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(state.headers(), headers);
    assert_eq!(state.lf().clone().collect_schema().unwrap(), schema);
    assert_eq!(current_rows(&app), 3);

    app.event(AppEvent::QQuery("select k by x".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert!(app.data_table_state.as_ref().unwrap().is_grouped());
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.at_table(), "Enter drilled");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert!(state.lf().clone().collect().unwrap().height() > 0);
}

/// A GROUP BY that fails once it runs is not applied, and the view left in place drills
/// as before: not at all over the rows as loaded, and by its own keys, not the failed
/// statement's, over a grouped view.
#[cfg(feature = "sql")]
#[test]
fn test_a_failed_group_by_leaves_the_grouped_view_drilling_by_its_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_rollback");
    let failing = "SELECT CAST(dept AS INT) AS dept, COUNT(*) AS n FROM df GROUP BY 1";
    // Over the rows as loaded, the failed statement leaves nothing to drill into.
    app.event(AppEvent::SqlQuery(failing.to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.modal_showing(), "the failure is said");
    assert!(!app.data_table_state.as_ref().unwrap().can_drill_down());
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.modal_showing());

    let grouped = "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept";
    run_sql(&mut app, &rx, &tx, grouped);
    app.event(AppEvent::SqlQuery(failing.to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.modal_showing(), "the failure is said");
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(state.get_active_sql_query(), grouped);
    state.drill_down_into_group(0).unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["eng".to_string()])
    );
    assert_eq!(current_rows(&app), 10);
}

/// With no view whose criteria match the open dataset, V opens the views
/// list instead of silently applying the best-scored stranger (scores carry
/// usage and recency, so some view always scores highest) or doing nothing.
#[test]
fn v_with_no_matching_view_opens_the_list() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("t_fallback.csv");
    assert_ne!(app.overlay, Overlay::View);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('V'),
        KeyModifiers::SHIFT,
    )));
    assert_eq!(
        app.overlay,
        Overlay::View,
        "V without a match shows what exists rather than staying silent"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_ne!(app.overlay, Overlay::View);
}

/// The view modal keys and renders off its own `active`, not the input mode,
/// so Ctrl+O must take it down: left up, it came back over the next dataset as a
/// zombie that swallowed keys.
#[test]
fn view_modal_does_not_survive_going_home() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("t_zombie.csv");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('v'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::View);

    app.enter_home();
    assert_ne!(
        app.overlay,
        Overlay::View,
        "going home closes the views list"
    );
}

/// Learned widths and a change of view (#491): a query that keeps a column's name
/// and type but not its values learns the column's width again from its first
/// page, not from the old rows still drawn while it reads; a width set by hand
/// stays; paging and a sort sent again unchanged learn nothing; a new sort and a
/// new filter learn again from the first rows they show.
#[test]
fn a_change_of_view_relearns_widths() {
    use datui::filter_modal::FilterOperator;
    use datui::widgets::column_widths::WidthChoice;
    let csv_path = common::fixture_dir().join("relearn_widths.csv");
    let n = 80usize;
    let long = |i: usize| format!("a much longer description {i}");
    let mut df = df!(
        "id" => (0..n as i64).collect::<Vec<_>>(),
        "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
        "description" => (0..n)
            .map(|i| if i < 40 { format!("short note {i}") } else { long(i) })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    CsvWriter::new(&mut File::create(&csv_path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    let area = Rect::new(0, 0, 100, 24);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let state = app.data_table_state.as_mut().unwrap();
        if std::mem::take(&mut state.needs_recollect) {
            app.spawn_async_collect("Loading buffer...");
            pump_until_idle(app, &rx, &tx);
            app.render(area, &mut buf);
        }
        common::buffer_text(&buf)
    };
    let shown = |app: &App, name: &str| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .shown_width(name)
            .unwrap()
    };
    let short_width = u16::try_from("short note 39".len()).unwrap();
    let long_width = u16::try_from(long(79).len()).unwrap();

    app.event(AppEvent::Resize(area.width, area.height));
    draw(&mut app);
    assert_eq!(shown(&app, "status"), 6);
    app.data_table_state
        .as_mut()
        .unwrap()
        .set_width_choices([("id".to_string(), WidthChoice::Manual(10))]);

    // Same name, same type, other values. The frame drawn while the query reads
    // still holds the old values; they teach the new view nothing.
    app.event(AppEvent::QQuery(
        "select id, status: description".to_string(),
    ));
    draw(&mut app);
    pump_until_idle(&mut app, &rx, &tx);
    let queried = draw(&mut app);
    assert_eq!(shown(&app, "status"), short_width, "{queried}");
    assert!(queried.contains("short note 12"), "{queried}");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.width_choice("id"), WidthChoice::Manual(10));
    assert_eq!(shown(&app, "id"), 10);

    // Paging to the long values keeps the width: they are clipped.
    for _ in 0..10 {
        if app.data_table_state.as_ref().unwrap().start_row() >= 40 {
            break;
        }
        press_and_send(&mut app, &tx, KeyCode::PageDown);
        pump_until_idle(&mut app, &rx, &tx);
        draw(&mut app);
    }
    let start = app.data_table_state.as_ref().unwrap().start_row();
    assert!(start >= 40);
    let paged = draw(&mut app);
    assert_eq!(shown(&app, "status"), short_width, "{paged}");

    // The sidebar sends the sort again on every apply; unchanged, it is not a
    // change of view.
    app.event(AppEvent::Sort(Vec::new(), Vec::new()));
    pump_until_idle(&mut app, &rx, &tx);
    let resent = draw(&mut app);
    assert_eq!(app.data_table_state.as_ref().unwrap().start_row(), start);
    assert_eq!(shown(&app, "status"), short_width, "{resent}");

    // A new sort keeps the row number; descending, the long values are there now,
    // and the width is learned from them.
    app.event(AppEvent::Sort(vec!["status".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let sorted = draw(&mut app);
    assert_eq!(shown(&app, "status"), long_width, "{sorted}");

    // A new filter, viewed from the top, holds only the short ones.
    app.event(AppEvent::Filter(vec![filter_stmt(
        "status",
        FilterOperator::Contains,
        "short",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let filtered = draw(&mut app);
    assert_eq!(shown(&app, "status"), short_width, "{filtered}");
    assert_eq!(shown(&app, "id"), 10);
}

/// The same holds for a query sent without the prompt, as a view applies one:
/// the failure is a dialog, over the view as it was.
#[cfg(feature = "sql")]
#[test]
fn a_query_sent_without_the_prompt_that_fails_leaves_the_view() {
    let (mut app, rx, tx) = open_query_filter_fixture("query_fails_no_prompt.csv");
    run_and_settle(
        &mut app,
        AppEvent::SqlQuery("SELECT CAST(name AS INT) AS n FROM df".to_string()),
        &rx,
        &tx,
    );
    assert!(app.modal_showing(), "no prompt to put it in: a dialog");
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<String> = state.schema().iter_names().map(|n| n.to_string()).collect();
    assert_eq!(names, ["a", "c", "name"]);
    assert!(state.get_active_sql_query().is_empty());
    assert_eq!(current_rows(&app), 100);
}

/// A view saved after a query, a filter and then a pivot replays all three: the pivot
/// clears the query bar, but the view keeps what the pivot ran over and runs it first.
#[cfg(feature = "sql")]
#[test]
fn test_a_view_replays_the_query_before_the_pivot() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let steps = || {
        vec![
            AppEvent::SqlQuery("SELECT id, key, val FROM df WHERE id >= 4".to_string()),
            AppEvent::Filter(vec![filter_stmt(
                "id",
                datui::filter_modal::FilterOperator::Lt,
                "8",
            )]),
            AppEvent::Pivot(PivotSpec {
                index: vec!["id".to_string()],
                pivot_column: "key".to_string(),
                value_column: "val".to_string(),
                aggregation: PivotAggregation::First,
            }),
        ]
    };
    let (view, applied, expected) = view_and_steps_on_the_next_file("view_query_pivot", &steps);

    let source = view.settings.reshape_source.as_ref().expect("the source");
    assert!(source.sql_query.is_some());
    assert_eq!(source.filters.len(), 1);
    assert_eq!(view.settings.sql_query, None);
    assert_eq!(expected.height(), 4, "ids 4..7");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// SQL after a pivot runs on the pivot's result, so the view replays it after the
/// pivot.
#[cfg(feature = "sql")]
#[test]
fn test_a_view_replays_sql_on_the_pivot_after_it() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let steps = || {
        vec![
            AppEvent::Pivot(PivotSpec {
                index: vec!["id".to_string()],
                pivot_column: "key".to_string(),
                value_column: "val".to_string(),
                aggregation: PivotAggregation::First,
            }),
            AppEvent::SqlQuery("SELECT id, k2 FROM df WHERE k1 > 12".to_string()),
        ]
    };
    let (view, applied, expected) = view_and_steps_on_the_next_file("view_pivot_sql", &steps);

    assert!(view.settings.reshape_source.is_none());
    assert_eq!(expected.height(), 5, "ids 5..9");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// The same for a melt: the query it ran over comes first.
#[cfg(feature = "sql")]
#[test]
fn test_a_view_replays_the_query_before_the_melt() {
    use datui::pivot_melt_modal::MeltSpec;
    let steps = || {
        vec![
            AppEvent::SqlQuery(
                "SELECT id, val, val * 2 AS doubled FROM df WHERE id < 3".to_string(),
            ),
            AppEvent::Melt(MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["val".to_string(), "doubled".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            }),
        ]
    };
    let (view, applied, expected) = view_and_steps_on_the_next_file("view_query_melt", &steps);

    assert!(view.settings.reshape_source.is_some());
    assert_eq!(expected.height(), 12, "six rows, two columns each");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// A reshape over another keeps no source: a view saved after a pivot then a melt holds
/// only the melt. Applying it fails on the next file, whose columns the melt never saw,
/// and leaves the table as it was rather than showing a melt of the wrong data.
#[cfg(feature = "sql")]
#[test]
fn test_a_view_of_a_melted_pivot_fails_to_apply_and_changes_nothing() {
    use datui::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};
    let steps = [
        AppEvent::SqlQuery("SELECT * FROM df WHERE id >= 4".to_string()),
        AppEvent::Pivot(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "val".to_string(),
            aggregation: PivotAggregation::First,
        }),
        AppEvent::Melt(MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["k1".to_string(), "k2".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        }),
    ];
    let next_path = common::fixture_dir().join("view_pivot_melt_next.csv");
    std::fs::write(&next_path, long_csv(3)).unwrap();
    let (mut app, rx, tx) = open_csv_with(
        "view_pivot_melt_first.csv",
        &long_csv(1),
        OpenOptions::default(),
    );
    for step in steps {
        app.event(step);
        pump_until_idle(&mut app, &rx, &tx);
        let state = app.data_table_state.as_ref().unwrap();
        assert!(state.error().is_none(), "{:?}", state.error());
    }
    let view = app
        .create_view_from_current_state(
            "view_pivot_melt".to_string(),
            None,
            datui::view::MatchCriteria {
                exact_path: Some(next_path.clone()),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    assert!(view.settings.melt.is_some());
    assert!(view.settings.reshape_source.is_none());

    pump_open_until_loaded(&mut app, &rx, vec![next_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let before = app
        .data_table_state
        .as_ref()
        .unwrap()
        .visible_lf()
        .collect()
        .unwrap();
    app.event(key(KeyCode::Char('V')));
    pump_until_idle(&mut app, &rx, &tx);

    assert!(app.modal_showing(), "the view says it could not apply");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_melt_spec().is_none());
    let after = state.visible_lf().collect().unwrap();
    assert!(after.equals_missing(&before), "{after:?}\n{before:?}");
}

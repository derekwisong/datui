//! The view built by sort, filter, query, SQL, reshapes, drills and column changes.

use super::*;

/// Every way of building on the scan holds the arriving columns off, not just one.
///
/// Each of these replaces the frame's root with a result of its own, whose columns
/// are not the dataset's. The doc on `scan_is_the_root` says so of all of them; the
/// query is the one the app-level test exercises, so this is the rest.
#[test]
fn anything_built_on_the_scan_holds_the_arriving_columns_off() {
    // A string column because a fuzzy search needs one to search.
    let narrow = || {
        df!("id" => &[1i64, 2], "v" => &[10i64, 20], "name" => &["one", "two"])
            .unwrap()
            .lazy()
    };
    let wider = || {
        df!(
            "id" => &[1i64, 2],
            "v" => &[10i64, 20],
            "name" => &["one", "two"],
            "oops" => &["a", "b"],
        )
        .unwrap()
        .lazy()
    };
    let found = || {
        let mut lf = wider();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        FootersFound {
            estimate: None,
            dataset: crate::schema_union::union_sampled(1, &[0], &[Some(footer)]),
            lf: wider(),
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        }
    };
    let fresh = || {
        let mut lf = narrow();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        DataTableState::from_schema_and_lazyframe(
            schema,
            narrow(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
    };

    // A filter and a sort are not built on the scan, they are the scan with
    // something done to it, and `apply_transformations` puts them back over
    // whatever the root becomes. Those the columns may join under.
    let mut sorted = fresh();
    sorted.sort(vec!["id".to_string()], true);
    assert!(
        sorted.join_dataset_schema(found()).is_ok(),
        "a sort is rebuilt over the wider scan, so the columns go in under it"
    );

    let mut queried = fresh();
    queried.query("select doubled: v * 2".to_string());
    assert!(
        queried.join_dataset_schema(found()).is_err(),
        "a query's columns are its own"
    );

    #[cfg(feature = "sql")]
    {
        let mut sql = fresh();
        sql.sql_query("SELECT id FROM df".to_string());
        assert!(sql.error.is_none(), "the statement runs: {:?}", sql.error);
        assert!(
            sql.join_dataset_schema(found()).is_err(),
            "and a SQL statement's are too"
        );
    }

    let mut fuzzy = fresh();
    fuzzy.fuzzy_search("10".to_string());
    assert!(
        fuzzy.join_dataset_schema(found()).is_err(),
        "and what a fuzzy search matched is a result, not the dataset"
    );

    let mut melted = fresh();
    melted
        .melt(&MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["v".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        })
        .expect("the melt runs");
    assert!(
        melted.join_dataset_schema(found()).is_err(),
        "a melt's rows are not the dataset's rows"
    );

    let mut pivoted = fresh();
    pivoted
        .pivot(&PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "name".to_string(),
            value_column: "v".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
        })
        .expect("the pivot runs");
    assert!(
        pivoted.join_dataset_schema(found()).is_err(),
        "and a pivot's columns are made from the data, not read from it"
    );

    let mut drilled = fresh();
    // The field rather than the drill itself, which needs a grouped frame to drill
    // into: what is being asked here is whether the clause is consulted.
    drilled.drilled_down_group_index = Some(0);
    assert!(
        drilled.join_dataset_schema(found()).is_err(),
        "and a drill-down is showing one group of it, not it"
    );
}

#[test]
fn test_filter() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "2".to_string(),
        logical_op: LogicalOperator::And,
    }];
    state.filter(filters);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape().0, 1);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
}

#[test]
fn test_query() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select b where a = 2".to_string());
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape(), (1, 1));
    assert_eq!(
        df.column("b").unwrap().get(0).unwrap(),
        AnyValue::String("y")
    );
}

#[test]
fn test_query_date_accessors() {
    use chrono::NaiveDate;
    let df = df!(
        "event_date" => [
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 6, 20).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
        ],
        "name" => &["a", "b", "c"],
    )
    .unwrap();
    let lf = df.lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

    // Select with date accessors
    state.query("select name, year: event_date.year, month: event_date.month".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape(), (3, 3));
    assert_eq!(
        df.column("year").unwrap().get(0).unwrap(),
        AnyValue::Int32(2024)
    );
    assert_eq!(
        df.column("month").unwrap().get(0).unwrap(),
        AnyValue::Int8(1)
    );
    assert_eq!(
        df.column("month").unwrap().get(1).unwrap(),
        AnyValue::Int8(6)
    );

    // Filter with date accessor
    state.query("select name, event_date where event_date.month = 12".to_string());
    assert!(
        state.error.is_none(),
        "filter should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(
        df.column("name").unwrap().get(0).unwrap(),
        AnyValue::String("c")
    );

    // Filter with YYYY.MM.DD date literal
    state.query("select name, event_date where event_date.date > 2024.06.15".to_string());
    assert!(
        state.error.is_none(),
        "date literal filter should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(
        df.height(),
        2,
        "2024-06-20 and 2024-12-31 are after 2024-06-15"
    );

    // String accessors: upper, lower, len, ends_with
    state.query(
        "select name, upper_name: name.upper, name_len: name.len where name.ends_with[\"c\"]"
            .to_string(),
    );
    assert!(
        state.error.is_none(),
        "string accessors should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1, "only 'c' ends with 'c'");
    assert_eq!(
        df.column("upper_name").unwrap().get(0).unwrap(),
        AnyValue::String("C")
    );

    // Query that returns 0 rows: df and locked_df must be cleared for correct empty-table render
    state.query("select where event_date.date = 2020.01.01".to_string());
    assert!(state.error.is_none());
    assert_eq!(state.num_rows, 0);
    state.visible_rows = 10;
    state.collect();
    assert!(state.df.is_none(), "df must be cleared when num_rows is 0");
    assert!(
        state.locked_df.is_none(),
        "locked_df must be cleared when num_rows is 0"
    );
}

/// Ties keep their order, so the page read at the top (a top-k to Polars) and
/// the page read below it agree on the rows they share.
#[test]
fn a_sort_with_ties_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..3000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    let sorted = df
        .lazy()
        .sort_by_exprs([col("k")], sort_options(vec![false]));
    let top = sorted.clone().slice(0, 200).collect().unwrap();
    let below = sorted.clone().slice(100, 100).collect().unwrap();
    assert!(top.slice(100, 100).equals(&below));
    let one = sorted.slice(5, 1).collect().unwrap();
    assert!(top.slice(5, 1).equals(&one));
}

/// `IN (SELECT …)` reads the subquery's values once rather than once per row,
/// which on the streaming engine ran out of memory for a page of 100k rows
/// (#509), and still returns what polars-sql's own plan does, NULLs included.
/// The data is small, so that without the fix the test fails on the plan rather
/// than on memory. A user's own list question of the same shape is left alone.
#[cfg(feature = "sql")]
#[test]
fn a_sql_in_subquery_reads_its_values_once() {
    let mut df = df!(
        "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..3000i64).map(|i| (i % 7 != 0).then_some(i)).collect::<Vec<_>>(),
    )
    .unwrap();
    // A first value that is a NULL list: its length is NULL, not 1 as exploded.
    let lists: ListChunked = (0..3000i64)
        .map(|i| (i != 0).then(|| Series::new(PlSmallStr::EMPTY, [i])))
        .collect();
    df.with_column(lists.into_column().with_name("l".into()))
        .unwrap();
    let n = df.height();
    for (sql, rows) in [
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1)",
            None,
        ),
        // The values hold a NULL, so no row is NOT IN them.
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1)",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1 AND v IS NOT NULL)",
            None,
        ),
        // No values: every row, a NULL `v` too.
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 5)",
            Some(n),
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 5)",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE k = 2 OR v IN (SELECT v FROM df WHERE k = 1 LIMIT 100)",
            None,
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT MIN(v) FROM df GROUP BY v % 100)",
            None,
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 0 AND v IS NOT NULL))",
            None,
        ),
        // Unknown, not false, for a value outside a set holding a NULL, and for a
        // NULL value.
        (
            "SELECT * FROM df WHERE (v NOT IN (SELECT v FROM df WHERE k = 1)) IS NULL",
            None,
        ),
        // Only NULLs: every row unknown.
        (
            "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE v IS NULL)) IS NULL",
            Some(n),
        ),
        // No values: false, a NULL `v` too.
        (
            "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE k = 5)) IS NULL",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE ARRAY_LENGTH(FIRST(l)) IS NULL AND v NOT IN (SELECT v FROM df WHERE k = 5)",
            Some(n),
        ),
    ] {
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap();
        // Else the check on the view's plan below would pass for nothing.
        assert!(
            (&raw.logical_plan).into_iter().any(asks_per_row),
            "{sql}: polars-sql's plan"
        );
        let expected = raw.collect().unwrap();
        if let Some(rows) = rows {
            assert_eq!(expected.height(), rows, "{sql}");
        }
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        assert!(
            !(&state.lf.logical_plan).into_iter().any(asks_per_row),
            "{sql}"
        );
        for streaming in [false, cfg!(feature = "streaming")] {
            let got = collect_lazy(state.lf.clone(), streaming).unwrap();
            assert!(
                got.equals_missing(&expected),
                "{sql}, streaming {streaming}"
            );
        }
    }
}

/// An `IN (SELECT …)` subquery returns exactly the statement's columns, after a
/// QUALIFY as after a WHERE: polars-sql leaves the column holding the values in a
/// QUALIFY's result (#519). The rows are polars-sql's own, and a user's column
/// named like polars-sql's is kept.
#[cfg(feature = "sql")]
#[test]
fn a_sql_in_subquery_returns_only_the_statements_columns() {
    const LOOKALIKE: &str = "_POLARS_TMP_999999999";
    let df = df!(
        "k" => (0..300i64).map(|i| i % 3).collect::<Vec<_>>(),
        "i" => (0..300i64).collect::<Vec<_>>(),
        "w" => (0..300i64).map(|i| (i % 7 != 0).then_some(i % 11)).collect::<Vec<_>>(),
        LOOKALIKE => (0..300i64).collect::<Vec<_>>(),
    )
    .unwrap();
    let all = ["k", "i", "w", LOOKALIKE];
    for (sql, columns) in [
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r NOT IN (SELECT w FROM df WHERE w > 3 AND w IS NOT NULL) \
             ORDER BY i DESC LIMIT 50",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT * FROM df \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) IN (SELECT w FROM df)",
            &all[..],
        ),
        (
            "SELECT DISTINCT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r, _POLARS_TMP_999999999 FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
            &["k", "r", LOOKALIKE][..],
        ),
        (
            "SELECT * FROM (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) WHERE i > 1",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3) AND i IN (SELECT w FROM df)",
            &["k", "i", "r"][..],
        ),
        (
            "WITH q AS (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
             SELECT * FROM q UNION ALL SELECT * FROM q",
            &["k", "i", "r"][..],
        ),
        // A join suffixes the right side's copy of both polars-sql's column and
        // the user's.
        (
            "WITH q AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
             SELECT * FROM q a JOIN q b ON a.i = b.i JOIN q c ON a.i = c.i ORDER BY a.i",
            &[
                "k",
                "i",
                "w",
                LOOKALIKE,
                "r",
                "k:b",
                "i:b",
                "w:b",
                "_POLARS_TMP_999999999:b",
                "r:b",
                "k:c",
                "i:c",
                "w:c",
                "_POLARS_TMP_999999999:c",
                "r:c",
            ][..],
        ),
        ("SELECT * FROM df WHERE i IN (SELECT w FROM df)", &all[..]),
        (
            "SELECT k, i FROM df WHERE i NOT IN (SELECT w FROM df WHERE w IS NOT NULL)",
            &["k", "i"][..],
        ),
    ] {
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap().collect().unwrap();
        assert!(raw.height() > 0, "{sql}");
        let expected = raw.select(columns.iter().copied()).unwrap();
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, columns, "{sql}");
        for streaming in [false, cfg!(feature = "streaming")] {
            let got = collect_lazy(state.lf.clone(), streaming).unwrap();
            assert!(
                got.equals_missing(&expected),
                "{sql}, streaming {streaming}: {got:?}"
            );
        }
    }
}

/// A SQL ORDER BY keeps tied rows in order, as the sidebar's sort does: the page
/// read at the top (a top-k to Polars) and the next page agree on the rows they
/// share, through a LIMIT and a subquery, and over a grouping, a join, a union or
/// a DISTINCT, whose rows would otherwise come in any order (#495). With either
/// engine.
#[cfg(feature = "sql")]
#[test]
fn a_sql_order_by_with_ties_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    for sql in [
        "SELECT * FROM df ORDER BY k",
        "SELECT v, k FROM df ORDER BY k DESC",
        "SELECT * FROM df ORDER BY k LIMIT 4000",
        "SELECT * FROM (SELECT * FROM df ORDER BY k) WHERE v >= 0",
        "SELECT v % 1000 AS g, COUNT(*) AS n, MIN(v) AS v FROM df GROUP BY g ORDER BY n",
        "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v ORDER BY a.k",
        "SELECT a.k, a.v FROM df a LEFT JOIN df b ON a.v = b.v + 1 ORDER BY a.k",
        "SELECT k, v FROM df UNION SELECT k, v FROM df ORDER BY k",
        "SELECT DISTINCT v % 1000 AS g, v % 1000 AS v FROM df ORDER BY g % 3",
    ] {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let unstable = (&state.lf.logical_plan).into_iter().any(|node| {
            matches!(
                node,
                polars::lazy::dsl::DslPlan::Sort { sort_options, .. }
                    if !sort_options.maintain_order
            )
        });
        assert!(!unstable, "{sql}");
        // The app pages with the streaming engine by default, which runs the top
        // page's top-k its own way.
        for streaming in [false, cfg!(feature = "streaming")] {
            let page =
                |offset, len| collect_lazy(state.lf.clone().slice(offset, len), streaming).unwrap();
            let top = page(0, 200);
            let next = page(100, 200);
            assert!(top.slice(100, 100).equals(&next.slice(0, 100)), "{sql}");
            let one = page(1500, 1);
            let around = page(1400, 200);
            assert!(around.slice(100, 1).equals(&one), "{sql}");
            // Ties keep the order they were read in.
            let v = top.column("v").unwrap().i64().unwrap();
            assert!(
                v.into_no_null_iter().is_sorted(),
                "{sql}, streaming {streaming}: {:?}",
                v.head(Some(10))
            );
        }
    }
}

/// A SQL result with no ORDER BY reads the same rows page by page, and a Sort &
/// Filter sort over it keeps its ties in that order: groupings, distincts, unions
/// and joins give their rows in one order, so every page agrees with a read of
/// the whole result, and a LIMIT keeps the same rows (#508). Both engines give the
/// same order. A grouping sorted by its keys leaves its groups' order to the sort.
#[cfg(feature = "sql")]
#[test]
fn a_sql_result_without_order_by_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    for sql in [
        "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v",
        "SELECT a.k, a.v, b.v AS w FROM df a LEFT JOIN df b ON a.v = b.v + 1",
        "SELECT k, v FROM df UNION ALL SELECT k, v FROM df",
        "SELECT k, v FROM df UNION SELECT k, v FROM df",
        "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300",
        "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g",
        "SELECT g, COUNT(*) AS n FROM (SELECT v % 1000 AS g FROM df) GROUP BY g",
        "SELECT DISTINCT v % 1000 AS g FROM df",
        "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1) LIMIT 1000",
    ] {
        for sort in [false, true] {
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            if sort {
                // By the first column, which most of these results repeat.
                let first = state.schema.get_at_index(0).unwrap().0.to_string();
                state.sort(vec![first], true);
                assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            }
            let mut fulls = Vec::new();
            for streaming in [false, cfg!(feature = "streaming")] {
                let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
                let full = read(state.lf.clone());
                let middle = full.height() as i64 / 2;
                for offset in [0, 100, middle] {
                    let page = read(state.lf.clone().slice(offset, 200));
                    assert!(
                        page.equals_missing(&full.slice(offset, 200)),
                        "{sql}, sort {sort}, streaming {streaming}, offset {offset}"
                    );
                }
                let one = read(state.lf.clone().slice(middle + 7, 1));
                assert!(
                    one.equals_missing(&full.slice(middle + 7, 1)),
                    "{sql}, sort {sort}, streaming {streaming}"
                );
                fulls.push(full);
            }
            assert!(
                fulls[0].equals_missing(&fulls[1]),
                "{sql}, sort {sort}: the engines disagree"
            );
        }
    }
    // Keeping the groups' order as well would double a large grouping's time.
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.sql_query("SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g".to_string());
    assert!((&state.lf.logical_plan).into_iter().any(|node| matches!(
        node,
        polars::lazy::dsl::DslPlan::GroupBy {
            maintain_order: false,
            ..
        }
    )));
}

/// A SQL grouping whose ORDER BY covers every key, by alias, ordinal or name,
/// leaves its groups' order to the sort (#523), and still reads the same rows page
/// by page, in the order polars-sql's own plan gives, with either engine, NULL and
/// NaN keys too. A sort that leaves any key out, sorts by an expression of one, or
/// reads the groups through a LIMIT, a filter or a computed column keeps the
/// groups' order.
#[cfg(feature = "sql")]
#[test]
fn a_sql_grouping_sorted_by_its_keys_leaves_the_order_to_the_sort() {
    use polars::lazy::dsl::DslPlan;
    // NaNs group as one and sort as one, as do 0.0 and -0.0.
    let floats = [0.0, -0.0, f64::NAN, -f64::NAN, 1.0, f64::INFINITY, -1.5];
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
        "w" => (0..5000i64).map(|i| (i % 11 != 0).then_some(i % 700)).collect::<Vec<_>>(),
        "f" => (0..5000usize)
            .map(|i| (i % 9 != 0).then_some(floats[i % floats.len()]))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let groups_ordered = |plan: &DslPlan| -> Vec<bool> {
        plan.into_iter()
            .filter_map(|node| match node {
                DslPlan::GroupBy { maintain_order, .. } => Some(*maintain_order),
                _ => None,
            })
            .collect()
    };
    for (sql, ordered) in [
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY g",
            false,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY 1 DESC LIMIT 300",
            false,
        ),
        (
            "SELECT k AS kk, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY n, g, kk",
            false,
        ),
        (
            "SELECT k AS d, COUNT(*) AS n FROM df GROUP BY k ORDER BY k DESC",
            false,
        ),
        (
            "SELECT w, MIN(v) AS v FROM df GROUP BY w HAVING COUNT(*) > 1 ORDER BY ALL",
            false,
        ),
        (
            "SELECT w, COUNT(*) AS n FROM df GROUP BY w ORDER BY w DESC NULLS FIRST",
            false,
        ),
        (
            "SELECT k, f, COUNT(*) AS n FROM df GROUP BY k, f ORDER BY f DESC NULLS FIRST, k",
            false,
        ),
        (
            "SELECT w % 7 AS w, COUNT(*) AS n FROM df GROUP BY w % 7 ORDER BY w",
            false,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY n",
            true,
        ),
        (
            "SELECT k, w, COUNT(*) AS n FROM df GROUP BY k, w ORDER BY k, w + 0",
            true,
        ),
        (
            "SELECT * FROM (SELECT w, COUNT(*) AS n FROM df GROUP BY w) WHERE n > 7 ORDER BY w",
            true,
        ),
        (
            "SELECT k, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY g",
            true,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY v % 1000",
            true,
        ),
        (
            "SELECT * FROM (SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300) ORDER BY g",
            true,
        ),
        (
            "SELECT g, ROW_NUMBER() OVER () AS r FROM (SELECT v % 1000 AS g FROM df GROUP BY g) ORDER BY g",
            true,
        ),
    ] {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        assert_eq!(groups_ordered(&state.lf.logical_plan), [ordered], "{sql}");
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap();
        for streaming in [false, cfg!(feature = "streaming")] {
            let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
            let full = read(state.lf.clone());
            if !ordered {
                // No ties, so polars-sql's unstable sort gives the one order too.
                assert!(
                    full.equals_missing(&read(raw.clone())),
                    "{sql}, streaming {streaming}"
                );
            }
            let middle = full.height() as i64 / 2;
            for offset in [0, 100, middle] {
                let page = read(state.lf.clone().slice(offset, 200));
                assert!(
                    page.equals_missing(&full.slice(offset, 200)),
                    "{sql}, streaming {streaming}, offset {offset}"
                );
            }
        }
    }
}

#[test]
fn test_by_query_puts_null_group_last() {
    let lf = df!(
        "g" => &[Some(2i64), None, Some(1), Some(2)],
        "v" => &[1i64, 2, 3, 4],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select sum v by g".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert_eq!(column_values(&state, "g"), [Some(1), Some(2), None]);
}

#[test]
fn test_filter_multiple() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![
        FilterStatement {
            columns: Vec::new(),
            column: "c".to_string(),
            operator: FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: LogicalOperator::And,
        },
        FilterStatement {
            columns: Vec::new(),
            column: "d".to_string(),
            operator: FilterOperator::Eq,
            value: "2".to_string(),
            logical_op: LogicalOperator::And,
        },
    ];
    state.filter(filters);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape().0, 7);
}

#[test]
fn test_filter_and_sort() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![FilterStatement {
        columns: Vec::new(),
        column: "c".to_string(),
        operator: FilterOperator::Eq,
        value: "1".to_string(),
        logical_op: LogicalOperator::And,
    }];
    state.filter(filters);
    state.sort(vec!["a".to_string()], false);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

#[test]
fn test_pivot_basic() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
    assert_eq!(df.height(), 2);
}

#[test]
fn test_pivot_aggregation_last() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let a_col = df.column("A").unwrap();
    let row0 = a_col.get(0).unwrap();
    let row1 = a_col.get(1).unwrap();
    assert_eq!(row0, AnyValue::Float64(11.0));
    assert_eq!(row1, AnyValue::Float64(40.0));
}

#[test]
fn test_pivot_aggregation_first() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let a_col = df.column("A").unwrap();
    assert_eq!(a_col.get(0).unwrap(), AnyValue::Float64(10.0));
    assert_eq!(a_col.get(1).unwrap(), AnyValue::Float64(40.0));
}

#[test]
fn test_pivot_aggregation_min_max() {
    let lf = create_pivot_long_lf();
    let mut state_min = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
    state_min
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Min,
            sort_columns: None,
        })
        .unwrap();
    let df_min = state_min.lf.clone().collect().unwrap();
    assert_eq!(
        df_min.column("A").unwrap().get(0).unwrap(),
        AnyValue::Float64(10.0)
    );

    let mut state_max = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state_max
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Max,
            sort_columns: None,
        })
        .unwrap();
    let df_max = state_max.lf.clone().collect().unwrap();
    assert_eq!(
        df_max.column("A").unwrap().get(0).unwrap(),
        AnyValue::Float64(11.0)
    );
}

#[test]
fn test_pivot_aggregation_avg_count() {
    let lf = create_pivot_long_lf();
    let mut state_avg = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
    state_avg
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Avg,
            sort_columns: None,
        })
        .unwrap();
    let df_avg = state_avg.lf.clone().collect().unwrap();
    let a = df_avg.column("A").unwrap().get(0).unwrap();
    if let AnyValue::Float64(x) = a {
        assert!((x - 10.5).abs() < 1e-6);
    } else {
        panic!("expected float");
    }

    let mut state_count = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state_count
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Count,
            sort_columns: None,
        })
        .unwrap();
    let df_count = state_count.lf.clone().collect().unwrap();
    let a = df_count.column("A").unwrap().get(0).unwrap();
    assert_eq!(a, AnyValue::UInt32(2));
}

#[test]
fn test_pivot_string_first_last() {
    let df = df!(
        "id" => &[1_i32, 1, 2, 2],
        "key" => &["X", "Y", "X", "Y"],
        "value" => &["low", "mid", "high", "mid"],
    )
    .unwrap();
    let lf = df.lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let out = state.lf.clone().collect().unwrap();
    assert_eq!(
        out.column("X").unwrap().get(0).unwrap(),
        AnyValue::String("low")
    );
    assert_eq!(
        out.column("Y").unwrap().get(0).unwrap(),
        AnyValue::String("mid")
    );
}

#[test]
fn test_melt_basic() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    state.melt(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 9);
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"variable"));
    assert!(names.contains(&"value"));
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
}

/// Pivoted on a date past the calendar, its new column is named by the stored
/// number, the columns still in date order; Polars' own naming panicked (#506).
/// Without one, the columns are named as they always were.
#[test]
fn a_pivot_on_a_date_past_the_calendar_names_it_by_its_stored_number() {
    for on in PAST_CALENDAR {
        let [first, past] = past_calendar_text(on);
        for with_past in [true, false] {
            let lf = past_calendar_lf()
                .filter(col("id").eq(lit(1)).or(lit(with_past)))
                .select([col("id"), col(on), col("s")]);
            let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
            state
                .pivot(&PivotSpec {
                    index: vec!["id".to_string()],
                    pivot_column: on.to_string(),
                    value_column: "s".to_string(),
                    aggregation: PivotAggregation::First,
                    sort_columns: None,
                })
                .unwrap();
            let df = state.lf.clone().collect().unwrap();
            let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
            let dates = match (with_past, on) {
                (false, _) => vec![first.as_str()],
                // i32::MAX days is after 1970, i64::MIN + 1 before it.
                (true, "d") => vec![first.as_str(), past.as_str()],
                (true, _) => vec![past.as_str(), first.as_str()],
            };
            assert_eq!(names[1..], dates, "{on}");
            assert_eq!(
                df.column(&first).unwrap().str().unwrap().get(0),
                Some("a"),
                "{on}"
            );
            if with_past {
                assert_eq!(
                    df.column(&past).unwrap().str().unwrap().get(1),
                    Some("b"),
                    "{on}"
                );
            }
        }
    }
}

/// Melted with text, a date past the calendar is its stored number, where the
/// cast to text panicked (#506), and one in range Polars' own text. Melted with
/// dates only, the values stay dates.
#[test]
fn a_melt_of_dates_with_text_writes_a_date_past_the_calendar_as_its_number() {
    let melt = |columns: [&str; 2]| {
        let mut state =
            DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
        state
            .melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: columns.map(String::from).to_vec(),
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .unwrap();
        state.lf.clone().collect().unwrap()
    };
    for column in PAST_CALENDAR {
        let [first, past] = past_calendar_text(column);
        let df = melt([column, "s"]);
        let values: Vec<Option<&str>> = df.column("value").unwrap().str().unwrap().iter().collect();
        assert_eq!(
            values,
            [
                Some(first.as_str()),
                Some(past.as_str()),
                Some("a"),
                Some("b")
            ],
            "{column}"
        );
    }
    let df = melt(["t_ms", "t_us"]);
    assert!(matches!(
        df.column("value").unwrap().dtype(),
        DataType::Datetime(..)
    ));
}

/// SQL's three plan rewrites compose: in a join filtered by an `IN` subquery, a
/// date past the calendar met with text is its stored number (#506), the join
/// keeps one row order (#508), and the subquery's values are read once (#509).
#[cfg(feature = "sql")]
#[test]
fn a_sql_join_with_an_in_subquery_and_a_date_past_the_calendar() {
    use polars::lazy::dsl::DslPlan;
    for c in PAST_CALENDAR {
        let [first, past] = past_calendar_text(c);
        let sql = format!(
            "SELECT a.id, COALESCE(a.{c}, b.s) AS x FROM df a JOIN df b ON a.id = b.id \
             WHERE a.id IN (SELECT t.id FROM df t WHERE t.s <> 'z')"
        );
        let mut state =
            DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
        state.sql_query(sql.clone());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let plan = &state.lf.logical_plan;
        assert!(!plan.into_iter().any(asks_per_row), "{sql}");
        let joins: Vec<_> = plan
            .into_iter()
            .filter_map(|node| match node {
                DslPlan::Join { options, .. } => Some(options.args.maintain_order),
                _ => None,
            })
            .collect();
        assert!(!joins.is_empty(), "{sql}");
        assert!(
            joins.iter().all(|order| *order != MaintainOrderJoin::None),
            "{sql}"
        );
        for streaming in [false, cfg!(feature = "streaming")] {
            let df = collect_lazy(state.lf.clone(), streaming).unwrap();
            let ids: Vec<Option<i32>> = df.column("id").unwrap().i32().unwrap().iter().collect();
            assert_eq!(ids, [Some(1), Some(2)], "{sql}, streaming {streaming}");
            let x: Vec<Option<&str>> = df.column("x").unwrap().str().unwrap().iter().collect();
            assert_eq!(
                x,
                [Some(first.as_str()), Some(past.as_str())],
                "{sql}, streaming {streaming}"
            );
            let page = collect_lazy(state.lf.clone().slice(1, 1), streaming).unwrap();
            assert!(page.equals_missing(&df.slice(1, 1)), "{sql}");
        }
    }
}

/// A q query forgets the melt it replaces; rolled back, the melt is
/// what SQL runs against again, not only what the table shows.
#[test]
fn a_rollback_brings_back_the_melt_a_query_forgot() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    state.melt(&spec).unwrap();
    let saved = state.rollback_point();
    state.query("select id".to_string());
    assert!(state.last_melt_spec().is_none());
    state.roll_back(saved);
    assert!(state.last_melt_spec().is_some());
    assert_eq!(state.query_root().collect().unwrap().height(), 9);
}

#[test]
fn test_melt_all_except_index() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "var".to_string(),
        value_name: "val".to_string(),
    };
    state.melt(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert!(df.column("var").is_ok());
    assert!(df.column("val").is_ok());
}

#[test]
fn test_pivot_on_current_view_after_filter() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "id".to_string(),
        operator: FilterOperator::Eq,
        value: "1".to_string(),
        logical_op: LogicalOperator::And,
    }]);
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1);
    let id_col = df.column("id").unwrap();
    assert_eq!(id_col.get(0).unwrap(), AnyValue::Int32(1));
}

/// The one-pass pivot gives what the lazy pivot over the whole view gave, for every
/// aggregation, with nulls in the index, the pivot column and the values, pairs with
/// no rows, and more rows than one streaming morsel, so `first` and `last` are
/// checked for order across batches.
#[test]
fn a_pivot_in_one_pass_matches_the_lazy_pivot() {
    let n = 250_000usize;
    let view = df!(
        "g" => (0..n)
            .map(|i| (i % 13 != 0).then_some((i % 97) as i64))
            .collect::<Vec<_>>(),
        "key" => (0..n)
            .map(|i| (i % 17 != 0).then(|| format!("k{}", (i * 7) % 11)))
            .collect::<Vec<_>>(),
        "v" => (0..n)
            .map(|i| (i % 7 != 0).then_some((i % 1_000) as f64))
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    // Pairs with no rows at all: `k3` never meets a `g` divisible by five.
    .filter(
        (col("g") % lit(5i64))
            .neq(lit(0i64))
            .or(col("key").neq(lit("k3")))
            .fill_null(lit(true)),
    );

    // The pivot as it ran before: the new columns read first, then the lazy pivot.
    let lazy_pivot = |spec: &PivotSpec| {
        let on = spec.pivot_column.as_str();
        let value = spec.value_column.as_str();
        let on_columns = view
            .clone()
            .select([col(on)])
            .unique(None, UniqueKeepStrategy::Any)
            .sort([on], SortMultipleOptions::default().with_nulls_last(true))
            .collect()
            .unwrap();
        let index = if spec.index.is_empty() {
            all() - by_name([on, value], true, false)
        } else {
            by_name(spec.index.iter().map(String::as_str), true, false)
        };
        view.clone()
            .pivot(
                by_name([on], true, false),
                Arc::new(on_columns),
                index,
                by_name([value], true, false),
                pivot_agg_expr(spec.aggregation, element()),
                true,
                PlSmallStr::from_static("_"),
                PivotColumnNaming::Auto,
            )
            .collect()
            .unwrap()
    };
    let close = |a: &DataFrame, b: &DataFrame| {
        a.get_column_names() == b.get_column_names()
            && a.height() == b.height()
            && a.columns().iter().zip(b.columns()).all(|(x, y)| {
                if x.dtype().is_float() {
                    let (x, y) = (x.f64().unwrap(), y.f64().unwrap());
                    x.iter().zip(y.iter()).all(|pair| match pair {
                        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(1.0),
                        (x, y) => x.is_none() && y.is_none(),
                    })
                } else {
                    x.as_materialized_series()
                        .equals_missing(y.as_materialized_series())
                }
            })
    };

    for index in [vec!["g".to_string()], Vec::new()] {
        for aggregation in PivotAggregation::ALL {
            let spec = PivotSpec {
                index: index.clone(),
                pivot_column: "key".to_string(),
                value_column: "v".to_string(),
                aggregation,
                sort_columns: None,
            };
            let expected = lazy_pivot(&spec);
            for streaming in [false, true] {
                let pivoted = PivotJob {
                    view: view.clone(),
                    spec: spec.clone(),
                    streaming,
                }
                .run()
                .unwrap();
                assert!(
                    close(&pivoted, &expected),
                    "{aggregation:?}, index {index:?}, streaming {streaming}:\n\
                     {pivoted:?}\n{expected:?}"
                );
            }
        }
    }
}

#[test]
fn test_fuzzy_token_regex() {
    assert_eq!(fuzzy_token_regex("foo"), "(?i).*f.*o.*o.*");
    assert_eq!(fuzzy_token_regex("a"), "(?i).*a.*");
    // Regex-special characters are escaped
    let pat = fuzzy_token_regex("[");
    assert!(pat.contains("\\["));
}

#[test]
fn test_fuzzy_search() {
    // Filter logic is covered by test_fuzzy_search_regex_direct. This test runs the full
    // path through DataTableState; it requires sample data (CSV with string column).
    crate::tests::ensure_sample_data();
    let path = crate::tests::sample_data_dir().join("3-sfd-header.csv");
    let mut state = DataTableState::from_csv(&path, &Default::default()).unwrap();
    state.visible_rows = 10;
    state.collect();
    let before = state.num_rows;
    state.fuzzy_search("string".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert!(state.num_rows <= before, "fuzzy search should filter rows");
    state.fuzzy_search("".to_string());
    state.collect();
    assert_eq!(state.num_rows, before, "empty fuzzy search should reset");
    assert!(state.get_active_fuzzy_query().is_empty());
}

#[test]
fn test_fuzzy_search_regex_direct() {
    // Sanity check: Polars str().contains with our regex matches "alice" for pattern ".*a.*l.*i.*"
    let lf = df!("name" => &["alice", "bob", "carol"]).unwrap().lazy();
    let pattern = fuzzy_token_regex("alice");
    let out = lf
        .filter(col("name").str().contains(lit(pattern.clone()), false))
        .collect()
        .unwrap();
    assert_eq!(out.height(), 1, "regex {:?} should match alice", pattern);

    // Two columns OR (as in fuzzy_search)
    let lf2 = df!(
        "id" => &[1i32, 2, 3],
        "name" => &["alice", "bob", "carol"],
        "city" => &["NYC", "LA", "Boston"]
    )
    .unwrap()
    .lazy();
    let pat = fuzzy_token_regex("alice");
    let expr = col("name")
        .str()
        .contains(lit(pat.clone()), false)
        .or(col("city").str().contains(lit(pat), false));
    let out2 = lf2.clone().filter(expr).collect().unwrap();
    assert_eq!(out2.height(), 1);

    // Replicate exact fuzzy_search logic: schema from original_lf, string_cols, then filter
    let schema = lf2.clone().collect_schema().unwrap();
    let string_cols: Vec<String> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_string())
        .map(|(name, _)| name.to_string())
        .collect();
    assert!(
        !string_cols.is_empty(),
        "df! string cols should be detected"
    );
    let pattern = fuzzy_token_regex("alice");
    let token_expr = string_cols
        .iter()
        .map(|c| col(c.as_str()).str().contains(lit(pattern.clone()), false))
        .reduce(|a, b| a.or(b))
        .unwrap();
    let out3 = lf2.filter(token_expr).collect().unwrap();
    assert_eq!(
        out3.height(),
        1,
        "fuzzy_search-style filter should match 1 row"
    );
}

/// A fuzzy search replaces the sort along with the rest of the pipeline: a descending
/// sort before it must not leave the result reversed with no sort column to show it.
#[test]
fn fuzzy_search_after_a_descending_sort_is_not_reversed() {
    let lf = df!(
        "id" => &[1i32, 2, 3],
        "name" => &["alice", "bob", "carol"]
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["id".to_string()], false);
    state.fuzzy_search("a".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert!(state.view_sort_columns().is_empty());
    assert!(state.view_sort_ascending());

    // The sidebar re-applies the (empty) filters and sort over the result.
    state.filter(Vec::new());
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("id").unwrap().get(0).unwrap(), AnyValue::Int32(1));
}

/// A reshape likewise replaces the sort. It runs over the sorted view, so its rows
/// come out descending, and a sidebar action afterwards must leave them that way
/// rather than reverse a frame that shows no sort column.
#[test]
fn melt_after_a_descending_sort_is_not_reversed() {
    let lf = df!("id" => &[1i32, 2, 3], "c1" => &[10i32, 20, 30])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["id".to_string()], false);
    state
        .melt(&MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["c1".to_string()],
            variable_name: "var".to_string(),
            value_name: "val".to_string(),
        })
        .unwrap();
    assert!(state.view_sort_columns().is_empty());
    assert!(state.view_sort_ascending());
    let melted = state.lf.clone().collect().unwrap();
    assert_eq!(
        melted.column("id").unwrap().get(0).unwrap(),
        AnyValue::Int32(3)
    );

    state.filter(Vec::new());
    assert!(state.lf.clone().collect().unwrap().equals(&melted));
}

#[test]
fn test_fuzzy_search_no_string_columns() {
    let lf = df!("a" => &[1i32, 2, 3], "b" => &[10i64, 20, 30])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.fuzzy_search("x".to_string());
    assert!(state.error.is_some());
}

/// What the Search hint promises: every word's letters in order, in any text
/// column. Each word may match a different column; letters out of order do not.
#[test]
fn search_matches_every_words_letters_in_order_in_any_text_column() {
    let rows = |query: &str| {
        let lf = df!(
            "name" => &["Smith", "Marion", "Smith"],
            "city" => &["London", "London", "Paris"]
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.fuzzy_search(query.to_string());
        assert!(state.error.is_none(), "{:?}", state.error);
        state.lf.clone().collect().unwrap().height()
    };
    assert_eq!(rows("smth"), 2, "letters in order, not adjacent, any case");
    assert_eq!(rows("smth ldn"), 1, "every word, each in its own column");
    assert_eq!(rows("htims"), 0, "letters out of order");
}

/// By-queries must produce results sorted by the group columns (age_group, then team)
/// so that output order is deterministic and practical. Raw data is deliberately out of order.
#[test]
fn test_by_query_result_sorted_by_group_columns() {
    // Build a small table: age_group (1-5, out of order), team (Red/Blue/Green), score (0-100)
    let df = df!(
        "age_group" => &[3i64, 1, 5, 2, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        "team" => &[
            "Red", "Blue", "Green", "Red", "Blue", "Green", "Green", "Red", "Blue",
            "Green", "Red", "Blue", "Red", "Blue", "Green",
        ],
        "score" => &[50.0f64, 10.0, 90.0, 20.0, 30.0, 40.0, 60.0, 70.0, 80.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0],
    )
    .unwrap();
    let lf = df.lazy();
    let options = crate::OpenOptions::default();
    let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
    state.query("select avg score by age_group, team".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let result = state.lf.collect().unwrap();
    // Result must be sorted by group columns (age_group, then team)
    let sorted = result
        .sort(
            ["age_group", "team"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .unwrap();
    assert_eq!(
        result, sorted,
        "by-query result must be sorted by (age_group, team)"
    );
}

/// Computed group keys (e.g. Fare: 1+floor Fare % 25) must be sorted by their result column
/// values, not by re-evaluating the expression on the result.
#[test]
fn test_by_query_computed_group_key_sorted_by_result_column() {
    let df = df!(
        "x" => &[7.0f64, 12.0, 3.0, 22.0, 17.0, 8.0],
        "v" => &[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0],
    )
    .unwrap();
    let lf = df.lazy();
    let options = crate::OpenOptions::default();
    let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
    // bucket: 1+floor(x)%3 -> values 1,2,3; raw x order 7,12,3,22,17,8 -> buckets 2,2,1,2,2,2
    state.query("select sum v by bucket: 1+floor x % 3".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let result = state.lf.collect().unwrap();
    let bucket = result.column("bucket").unwrap();
    // Must be sorted by bucket (1, 2, 3)
    for i in 1..result.height() {
        let prev: i64 = bucket.get(i - 1).unwrap().try_extract().unwrap_or(0);
        let curr: i64 = bucket.get(i).unwrap().try_extract().unwrap_or(0);
        assert!(
            curr >= prev,
            "bucket column must be sorted: {} then {}",
            prev,
            curr
        );
    }
}

/// The two checks that read footers rather than values: a column a file never had,
/// and a column a file holds in a type the scan cannot read.
///
/// Both are invisible to every measurement over values — an absent cell arrives as
/// a null and a conflicting one is not read at all — so the only way to test them
/// is through a dataset whose files genuinely disagree.
#[test]
fn absent_columns_and_type_conflicts_are_measured_from_the_footers() {
    use crate::data_quality::{DataQualityPlan, ObservationKind, QualityCompute, QualityScope};
    use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
    use polars::prelude::{DataType, IntoLazy, df};

    let urls: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    // `a` agrees with the schema; `b` holds `n` as text, which the scan cannot read
    // as the Int64 the majority wrote; `c` has no `fee` column at all.
    let scan: FileScan = Arc::new(move |urls: &[String], as_text: &[PlSmallStr]| {
        let reading_text = as_text.contains(&PlSmallStr::from("n"));
        let frames: Vec<LazyFrame> = urls
            .iter()
            .map(|url| match url.as_str() {
                "a" => df!(
                    "id" => &[0i64, 1, 2],
                    "n" => &[10i64, 20, 30],
                    "fee" => &[1.5f64, 2.5, 3.5],
                    crate::schema_union::DRIFT_COLUMN => &[0u32, 1, 2],
                )
                .unwrap()
                .lazy()
                .with_column(col("n").cast(if reading_text {
                    DataType::String
                } else {
                    DataType::Int64
                })),
                "b" => {
                    let frame = df!(
                        "id" => &[3i64, 4],
                        "n" => &["sixty", "seventy"],
                        "fee" => &[4.5f64, 5.5],
                        crate::schema_union::DRIFT_COLUMN => &[3u32, 4],
                    )
                    .unwrap()
                    .lazy();
                    if reading_text {
                        frame
                    } else {
                        // Not read from this file at all, as the real scan leaves it.
                        frame.with_column(lit(NULL).cast(DataType::Int64).alias("n"))
                    }
                }
                _ => df!(
                    "id" => &[5i64, 6],
                    "n" => &[50i64, 60],
                    crate::schema_union::DRIFT_COLUMN => &[5u32, 6],
                )
                .unwrap()
                .lazy()
                // A column the file never had reads as null, which is exactly why
                // no measurement over values can tell it from one.
                .with_column(lit(NULL).cast(DataType::Float64).alias("fee"))
                .select([
                    col("id"),
                    col("n").cast(if reading_text {
                        DataType::String
                    } else {
                        DataType::Int64
                    }),
                    col("fee"),
                    col(crate::schema_union::DRIFT_COLUMN),
                ]),
            })
            .collect();
        polars::prelude::concat(frames, Default::default())
    });

    let dataset: DatasetSchema = union_file_schemas(
        &[
            file_schema(
                &[
                    ("id", DataType::Int64),
                    ("n", DataType::Int64),
                    ("fee", DataType::Float64),
                ],
                3,
            ),
            file_schema(
                &[
                    ("id", DataType::Int64),
                    ("n", DataType::String),
                    ("fee", DataType::Float64),
                ],
                2,
            ),
            file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 2),
        ],
        SchemaOrigin::AllFooters(3),
    );
    let state = DataTableState::from_schema_and_lazyframe(
        dataset.schema.clone(),
        scan(&urls, &[]).unwrap(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(urls.clone()),
            scan,
            count: Arc::new(|_| Ok(vec![vec![3], vec![2], vec![2]])),
            offsets: None,
        }),
        dataset: Some(DatasetAtOpen {
            schema: dataset,
            file_rows: vec![3, 2, 2],
            files: urls.clone(),
        }),
        ..Default::default()
    });
    assert!(state.drifts(), "the three files do not agree");

    let (lf, source) = state.data_quality_source_scan();
    let mut source = source.expect("every file is counted, so rows map to files");
    source.conflict_scan = state.quality_conflict_scan();
    let lf = crate::data_quality::prepare_source_quality_scan(lf, Some(&source)).unwrap();
    let plan = DataQualityPlan {
        scope: QualityScope::WholeSource,
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results =
        crate::data_quality::compute_data_quality(&lf, Some(7), &plan, Some(&source), false)
            .unwrap();

    let absent = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::Absent)
        .expect("`fee` is absent from the third file");
    assert_eq!(absent.column, "fee");
    assert_eq!(
        (absent.affected_rows, absent.evaluated_rows),
        (2, 7),
        "the third file's two rows, out of the source's seven"
    );
    assert_eq!(
        absent
            .files
            .iter()
            .map(|file| file.number)
            .collect::<Vec<_>>(),
        vec![3],
        "named by the number the Scope page gives it"
    );
    assert_eq!(
        absent.fact, "1 of 3 files has no such column",
        "every footer was read, so the count is a total rather than a floor"
    );

    let conflict = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::TypeConflict)
        .expect("`n` is text in the second file");
    assert_eq!(conflict.column, "n");
    assert_eq!((conflict.affected_rows, conflict.evaluated_rows), (2, 7));
    let file = conflict.files.first().expect("the file that disagrees");
    assert_eq!(file.number, 2);
    assert_eq!(file.stored_type.as_deref(), Some("str"));
    assert_eq!(
        file.examples,
        vec!["sixty".to_string(), "seventy".to_string()],
        "the values the conflict hides, read at the type that file wrote"
    );

    // The same two checks at the budget that reads no values at all: the footers
    // were read when the dataset opened, so there is nothing left to pay for.
    let metadata = crate::data_quality::compute_data_quality(
        &lf,
        Some(7),
        &DataQualityPlan {
            scope: QualityScope::WholeSource,
            compute: QualityCompute::Metadata,
            ..DataQualityPlan::default()
        },
        Some(&source),
        false,
    )
    .unwrap();
    assert_eq!(metadata.evaluated_rows, 0, "no value was read");
    assert_eq!(
        metadata
            .observations
            .iter()
            .map(|observation| (observation.kind, observation.affected_rows))
            .collect::<Vec<_>>(),
        vec![
            (ObservationKind::Absent, 2),
            (ObservationKind::TypeConflict, 2),
        ],
        "both are reported without reading a value"
    );

    // The drill-in is the files themselves: an absent cell has no value to filter.
    let scope = absent.evidence_scope().expect("a scope, not a predicate");
    assert_eq!(scope, QualityScope::SourceFiles(vec![3]));
    assert!(absent.evidence_predicate(&results).is_none());
    let evidence = state
        .quality_evidence_view(&scope, lit(true))
        .expect("the rows the third file contributed");
    let rows = collect_lazy(evidence.lf.clone(), false).unwrap();
    assert_eq!(
        rows.column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        vec![5, 6],
        "the file that has no `fee`, and only that file"
    );
    assert!(
        rows.column(crate::schema_union::DRIFT_COLUMN).is_err(),
        "the hidden scan index is never handed back as user data"
    );
}

#[test]
fn a_filtered_remote_scan_falls_back_to_the_page_window() {
    // `filter(..).slice(0, N)` stops at the first N matches, so a window of a few
    // pages stops at the first row group with any; the 100k window read forty.
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let lf = df!("a" => (0..1_000i32).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![500, 500]],
            parquet_count_dir: Some(PathBuf::from("/hive")),
            ..Default::default()
        });
    state.visible_rows = 40;
    state.defer_collect = true;

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (0, 1_000),
        "both groups fit the remote window"
    );

    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "990".to_string(),
        logical_op: LogicalOperator::And,
    }]);
    let request = state.prepare_async_collect(None).expect("filtered fill");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (0, 7 * 40),
        "a page plus three either side, not the remote window"
    );

    state.filter(Vec::new());
    assert!(
        state.remote_window(),
        "with the filters cleared the frame is the scan as loaded again"
    );
    assert_eq!(
        state.num_rows_if_valid(),
        Some(1_000),
        "and its footer answers the count"
    );

    // The local hive footer count is gated the same way.
    assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
    state.sort(vec!["a".to_string()], true);
    assert!(state.parquet_count_dir().is_none());
    state.sort(Vec::new(), true);
    assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
    state.reverse();
    assert!(!state.remote_window(), "reversed is not as loaded");
}

#[test]
fn a_new_base_is_measured_afresh() {
    // The width measured on a buffer of the old frame does not plan the new one.
    let big: Vec<String> = (0..100).map(|_| "z".repeat(2_000)).collect();
    let lf = df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 10;
    state.defer_collect = true;
    state.land(Fill {
        df: df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>()).unwrap(),
        buffer_start: 0,
        buffer_end: 100,
        num_rows: 100,
        count_known: true,
    });
    assert!(state.observed_bytes_per_row.is_some());
    state.query("select b".to_string());
    assert!(state.observed_bytes_per_row.is_none());
    assert_eq!(
        state.bytes_per_row(),
        4,
        "the narrow frame, from its schema"
    );
}

#[test]
fn a_reset_remote_scan_is_pristine_again() {
    // A query makes the frame a predicate over the object, so the row-group window
    // and footer count stand down; clearing it brings both back without a len().
    let lf = df!("a" => (0..100).collect::<Vec<i32>>()).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![60, 40]],
            ..Default::default()
        });
    assert!(state.remote_window());
    state.query("select a where a > 50".to_string());
    assert!(!state.remote_window());
    state.query(String::new());
    assert!(state.remote_window());
    assert_eq!(state.num_rows_if_valid(), Some(100));
}

#[test]
fn quality_source_scope_ignores_current_query_and_evidence_matches_scope() {
    let lf = df!("a" => &[1i32, 2, 3, 4]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.drift_files = vec!["first.parquet".into(), "second.parquet".into()];
    state.drift_file_starts = vec![0, 2];
    state.query("select a where a > 2".to_string());
    let (current, _) = state.data_quality_scan(false);
    let (source, context) = state.data_quality_source_scan();
    let source =
        crate::data_quality::prepare_source_quality_scan(source, context.as_ref()).unwrap();
    assert_eq!(current.collect().unwrap().height(), 2);
    assert_eq!(source.collect().unwrap().height(), 4);
    assert_eq!(state.quality_source_file_count(), 2);
    let (raw, mapping) = state.data_quality_source_scan();
    let indexed = crate::data_quality::prepare_source_quality_scan(raw, mapping.as_ref()).unwrap();
    let first_file = crate::data_quality::apply_quality_scope(
        indexed,
        &crate::data_quality::QualityScope::SourceFiles(vec![1]),
        mapping.as_ref(),
    )
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(first_file.height(), 2);
    assert_eq!(
        first_file.column("a").unwrap().i32().unwrap().get(0),
        Some(1)
    );

    let evidence = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::WholeSource,
            col("a").eq(lit(1)),
        )
        .unwrap();
    assert_eq!(evidence.visible_lf().collect().unwrap().height(), 1);
    let bounded = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::FirstRows(1),
            col("a").eq(lit(4)),
        )
        .unwrap();
    assert_eq!(bounded.visible_lf().collect().unwrap().height(), 0);
    let file_evidence = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::SourceFiles(vec![1]),
            col("a").eq(lit(1)),
        )
        .unwrap();
    assert_eq!(file_evidence.visible_lf().collect().unwrap().height(), 1);
}

#[test]
fn source_time_roles_can_use_columns_hidden_by_current_query() {
    let lf = df!("a" => &[1i32, 2], "event" => &[20_000i32, 20_001])
        .unwrap()
        .lazy()
        .with_columns([col("event").cast(DataType::Date)]);
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select a".to_string());
    assert!(
        state
            .quality_temporal_columns(&crate::data_quality::QualityScope::CurrentView)
            .is_empty()
    );
    assert_eq!(
        state.quality_temporal_columns(&crate::data_quality::QualityScope::WholeSource),
        vec!["event"]
    );
}

/// The same name with another type, as a query can make it, is another column:
/// it starts from an automatic width, and the first gets its own back.
#[test]
fn a_column_whose_type_changes_starts_afresh() {
    let df = df!("a" => &["x", "y"], "n" => &[1i64, 2]).unwrap();
    let mut state = state_of(&df, 2);
    state.set_width_choices([("a".to_string(), WidthChoice::Manual(9))]);
    state.query("select a: n".to_string());
    assert_eq!(state.width_choice("a"), WidthChoice::Auto);
    state.query("select a, n".to_string());
    assert_eq!(state.width_choice("a"), WidthChoice::Manual(9));
}

/// A followed file's page near its end, and a filtered view's count after rows
/// arrive, are read from the marks the watcher made, not from the file's start.
#[test]
fn a_followed_view_reads_and_counts_from_its_marks() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grow.csv");
    let mut text = String::from("t,n\n");
    for i in 0..20_000 {
        text.push_str(&format!("{i},{}\n", i % 7));
    }
    std::fs::write(&path, &text).unwrap();
    let scan = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .with_ignore_errors(true)
        .finish()
        .unwrap();
    let options = crate::OpenOptions::default();
    let (lf, tail) =
        crate::follow::bound_to_complete(scan, &path, crate::FileFormat::Csv, &options).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut state = DataTableState::new(lf, None, None, None, None, false).unwrap();
    let rows = tail.rows();
    state.start_following(crate::follow::Follow::start(
        tail,
        std::time::Duration::from_secs(3_600),
        tx,
        None,
    ));
    state.follow_to(rows, false);
    let from_marks = |lf: &LazyFrame| format!("{:?}", lf.logical_plan).contains("FOLLOWED");
    let page = state.buffer_lf(19_990, 10).unwrap();
    assert!(from_marks(&page));
    let t = |df: DataFrame| df.column("t").unwrap().i64().unwrap().to_vec();
    assert_eq!(t(page.collect().unwrap()).first(), Some(&Some(19_990)));

    state.defer_collect = true;
    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "n".to_string(),
        operator: crate::filter_modal::FilterOperator::Eq,
        value: "3".to_string(),
        logical_op: crate::filter_modal::LogicalOperator::And,
    }]);
    let matches = |n: usize| (0..n).filter(|i| i % 7 == 3).count();
    // The first count of the filter reads the whole file.
    assert!(state.source_counter().is_none());
    state.set_num_rows(matches(20_000));

    let mut out = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    let more: String = (20_000..20_050)
        .map(|i| format!("{i},{}\n", i % 7))
        .collect();
    out.write_all(more.as_bytes()).unwrap();
    let follow = state.follow_mut().unwrap();
    follow.check_now();
    // A watcher that hears changes may report before the check it was asked for.
    let (rows, restarted) = loop {
        let crate::AppEvent::Followed(news) =
            rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap()
        else {
            panic!("the watcher said something else");
        };
        follow.take(&news.change);
        if follow.waiting() == 50 {
            break follow.catch_up();
        }
    };
    assert_eq!(rows, 20_050);
    state.follow_to(rows, restarted);
    assert!(!state.is_num_rows_valid());
    let counter = state.source_counter().expect("counts the new rows alone");
    assert_eq!(counter().unwrap(), matches(20_050));
    state.set_num_rows(matches(20_050));
    let last = state.buffer_lf(matches(20_050) - 3, 3).unwrap();
    assert!(from_marks(&last));
    let expected: Vec<_> = (0..20_050i64).filter(|i| i % 7 == 3).map(Some).collect();
    assert_eq!(t(last.collect().unwrap()), expected[expected.len() - 3..]);
}

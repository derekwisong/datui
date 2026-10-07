use super::*;

fn spec(pattern: &str, regex: bool) -> FindSpec {
    FindSpec {
        pattern: pattern.to_string(),
        regex,
        fuzzy: false,
        column: None,
    }
}

fn frame() -> DataFrame {
    df!(
        "name" => ["Alice", "bob", "Carol", "dave", "alice"],
        "city" => ["Oslo", "Lima", "oslo", "Rome", "Lima"],
        "n" => [1i64, 22, 3, 42, 5],
    )
    .unwrap()
}

/// Every match of `spec` in `df` reading forward from `start`, `steps` times.
fn walk(
    df: &DataFrame,
    spec: &FindSpec,
    buffer: Option<(DataFrame, usize)>,
    start: Start,
    direction: Direction,
    steps: usize,
) -> Vec<(usize, String, bool)> {
    let order: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect();
    let names: Vec<String> = searched_columns(&order, df.schema(), spec)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    let mut at = start;
    let mut out = Vec::new();
    for _ in 0..steps {
        let columns = searched_columns(&order, df.schema(), spec);
        let rows = ViewRows::of(df.clone().lazy(), buffer.clone());
        let search = Search::new(rows, columns, Arc::default(), |_| {});
        let Some(found) = search.run(at, direction).unwrap() else {
            break;
        };
        let column = names.iter().position(|n| *n == found.column).map(At::On);
        at = Start {
            row: found.row,
            column,
        };
        out.push((found.row, found.column, found.wrapped));
    }
    out
}

fn cells(found: &[(usize, String, bool)]) -> Vec<(usize, &str)> {
    found.iter().map(|(r, c, _)| (*r, c.as_str())).collect()
}

#[test]
fn plain_text_ignores_case_until_a_capital_is_typed() {
    let df = frame();
    let start = Start {
        row: 0,
        column: None,
    };
    let lower = walk(&df, &spec("oslo", false), None, start, Direction::Next, 2);
    assert_eq!(cells(&lower), [(0, "city"), (2, "city")]);
    let upper = walk(&df, &spec("Oslo", false), None, start, Direction::Next, 2);
    assert_eq!(cells(&upper), [(0, "city"), (0, "city")], "{upper:?}");
    assert!(upper[1].2, "the one match wraps to itself");
}

#[test]
fn plain_text_is_literal() {
    let df = df!("a" => ["a.c", "abc"]).unwrap();
    let start = Start {
        row: 0,
        column: None,
    };
    let found = walk(&df, &spec("a.c", false), None, start, Direction::Next, 2);
    assert_eq!(cells(&found), [(0, "a"), (0, "a")]);
}

#[test]
fn a_regex_matches_and_keeps_smart_case() {
    let df = frame();
    let start = Start {
        row: 0,
        column: None,
    };
    let digits = walk(
        &df,
        &spec(r"^\d{2}$", true),
        None,
        start,
        Direction::Next,
        3,
    );
    assert_eq!(cells(&digits), [(1, "n"), (3, "n"), (1, "n")]);
    // `\S` is a class, not a capital: still case-blind.
    assert!(spec(r"^\Sl", true).ignores_case());
    let a = walk(&df, &spec(r"^a\S", true), None, start, Direction::Next, 2);
    assert_eq!(cells(&a), [(0, "name"), (4, "name")]);
    let capital = walk(&df, &spec("^A", true), None, start, Direction::Next, 2);
    assert_eq!(cells(&capital), [(0, "name"), (0, "name")]);
}

#[test]
fn a_bad_regex_says_why() {
    let err = spec("(ab", true).check().unwrap_err();
    assert!(err.starts_with("Not a regex"), "{err}");
    assert!(!err.contains('\n'), "{err}");
    assert!(spec("(ab", false).check().is_ok(), "plain text is literal");
}

#[test]
fn next_moves_cell_by_cell_and_wraps_to_the_top() {
    let df = frame();
    let start = Start {
        row: 0,
        column: None,
    };
    // Both "Lima" rows and "alice": every cell holding "li", row by row and left
    // to right within a row.
    let found = walk(&df, &spec("li", false), None, start, Direction::Next, 5);
    assert_eq!(
        cells(&found),
        [
            (0, "name"),
            (1, "city"),
            (4, "name"),
            (4, "city"),
            (0, "name")
        ]
    );
    assert!(!found[3].2);
    assert!(found[4].2, "past the last match comes round to the first");
}

#[test]
fn previous_walks_back_and_wraps_to_the_bottom() {
    let df = frame();
    let start = Start {
        row: 1,
        column: Some(At::On(1)),
    };
    let found = walk(&df, &spec("li", false), None, start, Direction::Previous, 3);
    assert_eq!(cells(&found), [(0, "name"), (4, "city"), (4, "name")]);
    assert!(found[1].2, "before the first match comes round to the last");
}

/// On a column not searched, the cursor sits between the searched ones: the
/// cells to its right are ahead of it, those to its left behind.
#[test]
fn from_a_column_not_searched_the_cells_either_side_split() {
    let df = frame();
    let only_city = FindSpec {
        column: Some("city".to_string()),
        ..spec("li", false)
    };
    let order: Vec<String> = ["name", "city", "n"].map(String::from).to_vec();
    let one = |at: At, direction: Direction| {
        let columns = searched_columns(&order, df.schema(), &only_city);
        let rows = ViewRows::of(df.clone().lazy(), None);
        let search = Search::new(rows, columns, Arc::default(), |_| {});
        let found = search
            .run(
                Start {
                    row: 1,
                    column: Some(at),
                },
                direction,
            )
            .unwrap()
            .unwrap();
        (found.row, found.wrapped)
    };
    // On `name`, left of `city`: row 1's Lima is ahead, and behind is round.
    assert_eq!(one(At::Before(0), Direction::Next), (1, false));
    assert_eq!(one(At::Before(0), Direction::Previous), (4, true));
    // On `n`, right of it: the other way about.
    assert_eq!(one(At::Before(1), Direction::Next), (4, false));
    assert_eq!(one(At::Before(1), Direction::Previous), (1, false));
}

#[test]
fn previous_wraps_with_the_row_count_known_too() {
    let df = frame();
    let columns = searched_columns(
        &["name".to_string(), "city".to_string()],
        df.schema(),
        &spec("li", false),
    );
    let mut rows = ViewRows::of(df.clone().lazy(), None);
    rows.num_rows = Some(df.height());
    let search = Search::new(rows, columns, Arc::default(), |_| {});
    let found = search
        .run(
            Start {
                row: 0,
                column: Some(At::On(0)),
            },
            Direction::Previous,
        )
        .unwrap()
        .unwrap();
    assert_eq!((found.row, found.column.as_str()), (4, "city"));
    assert!(found.wrapped);
}

#[test]
fn f_finds_a_match_on_the_cursor_row_itself() {
    let df = frame();
    let start = Start {
        row: 2,
        column: None,
    };
    let found = walk(&df, &spec("oslo", false), None, start, Direction::Next, 1);
    assert_eq!(cells(&found), [(2, "city")]);
}

#[test]
fn a_column_limit_skips_the_others() {
    let df = frame();
    let mut only = spec("li", false);
    only.column = Some("city".to_string());
    let start = Start {
        row: 0,
        column: None,
    };
    let found = walk(&df, &only, None, start, Direction::Next, 3);
    assert_eq!(cells(&found), [(1, "city"), (4, "city"), (1, "city")]);
}

#[test]
fn nothing_found_is_none() {
    let df = frame();
    let start = Start {
        row: 3,
        column: None,
    };
    assert!(walk(&df, &spec("zzz", false), None, start, Direction::Next, 1).is_empty());
    assert!(
        walk(
            &df,
            &spec("zzz", false),
            None,
            start,
            Direction::Previous,
            1
        )
        .is_empty()
    );
}

/// A long view: the buffer holds rows 100..200, the match is far below it, read
/// in growing windows from the view, and the rows read are reported.
#[test]
fn a_match_beyond_the_buffer_is_read_from_the_view() {
    let n = 300_000usize;
    let values: Vec<String> = (0..n)
        .map(|i| {
            if i == 250_123 {
                "needle".to_string()
            } else {
                format!("hay{i}")
            }
        })
        .collect();
    let df = df!("v" => values).unwrap();
    let buffer = df.slice(100, 100);
    let columns = searched_columns(&["v".to_string()], df.schema(), &spec("needle", false));
    let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = reads.clone();
    let rows = ViewRows::of(df.clone().lazy(), Some((buffer, 100)));
    let search = Search::new(rows, columns, Arc::default(), move |read| {
        seen.lock().unwrap().push(read)
    });
    let found = search
        .run(
            Start {
                row: 150,
                column: None,
            },
            Direction::Next,
        )
        .unwrap()
        .unwrap();
    assert_eq!((found.row, found.wrapped), (250_123, false));
    let reads = reads.lock().unwrap();
    // The buffer's rest first, then windows of 65,536, 131,072 and the rest of the
    // view, read from row 200 on: none of it twice.
    assert_eq!(
        reads.as_slice(),
        [50, 50 + 65_536, 50 + 65_536 + 131_072, n - 150]
    );
}

#[test]
fn a_stopped_search_ends_before_reading() {
    let df = frame();
    let columns = searched_columns(&["name".to_string()], df.schema(), &spec("a", false));
    let stop = Arc::new(AtomicBool::new(true));
    let search = Search::new(ViewRows::of(df.lazy(), None), columns, stop, |_| {
        panic!("nothing is read once stopped")
    });
    let ended = search.run(
        Start {
            row: 0,
            column: None,
        },
        Direction::Next,
    );
    assert_eq!(ended.unwrap_err(), CANCELLED);
}

#[test]
fn numbers_dates_and_categories_are_searched_as_text() {
    let df = df!(
        "d" => [chrono::NaiveDate::from_ymd_opt(2024, 5, 17).unwrap()],
        "x" => [12.5f64],
        "b" => [true],
    )
    .unwrap()
    .lazy()
    .with_column(
        lit("north")
            .cast(DataType::from_categories(Categories::global()))
            .alias("c"),
    )
    .collect()
    .unwrap();
    let start = Start {
        row: 0,
        column: None,
    };
    for (pattern, column) in [("05-17", "d"), ("2.5", "x"), ("true", "b"), ("nor", "c")] {
        let found = walk(&df, &spec(pattern, false), None, start, Direction::Next, 1);
        assert_eq!(cells(&found), [(0, column)], "{pattern}");
    }
}

/// A date past the calendar, on which a plain cast panics, is its stored number;
/// durations and times are text too, so no type fails the whole find.
#[test]
fn a_date_past_the_calendar_and_durations_are_text() {
    let d = Series::new("d".into(), [19_860i32, i32::MAX])
        .cast(&DataType::Date)
        .unwrap();
    let t = Series::new("t".into(), [3_600_000_000_000i64, 0])
        .cast(&DataType::Time)
        .unwrap();
    let dur = Series::new("dur".into(), [86_400_000i64, 1])
        .cast(&DataType::Duration(TimeUnit::Milliseconds))
        .unwrap();
    let df = DataFrame::new_infer_height(vec![d.into(), t.into(), dur.into()]).unwrap();
    let start = Start {
        row: 0,
        column: None,
    };
    let past = walk(&df, &spec("since", false), None, start, Direction::Next, 1);
    assert_eq!(cells(&past), [(1, "d")]);
    let time = walk(&df, &spec("01:00", false), None, start, Direction::Next, 1);
    assert_eq!(cells(&time), [(0, "t")]);
    let dur = walk(&df, &spec("1d", false), None, start, Direction::Next, 1);
    assert_eq!(cells(&dur), [(0, "dur")]);
}

/// A sorted view costs a whole pass for any window, so it is read in one: the
/// match far down is found with one read past the buffer, not a window per
/// doubling.
#[test]
fn a_sorted_view_is_read_in_one_window() {
    let n = 300_000usize;
    let values: Vec<String> = (0..n)
        .map(|i| {
            if i == 7 {
                "needle".to_string()
            } else {
                format!("hay{i}")
            }
        })
        .collect();
    let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
    // Descending: the needle (k = 7) is view row n - 8.
    let lf = df.lazy().sort(
        ["k"],
        SortMultipleOptions::default().with_order_descending(true),
    );
    let buffer = lf.clone().slice(0, 100).collect().unwrap();
    let columns = searched_columns(
        &["k".to_string(), "v".to_string()],
        &lf.clone().collect_schema().unwrap(),
        &spec("needle", false),
    );
    let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = reads.clone();
    let rows = ViewRows::of(lf, Some((buffer, 0)));
    assert!(rows.whole);
    let search = Search::new(rows, columns, Arc::default(), move |read| {
        seen.lock().unwrap().push(read)
    });
    let found = search
        .run(
            Start {
                row: 0,
                column: None,
            },
            Direction::Next,
        )
        .unwrap()
        .unwrap();
    assert_eq!((found.row, found.column.as_str()), (n - 8, "v"));
    assert_eq!(reads.lock().unwrap().as_slice(), [100, n]);
}

/// `N` on a view that cannot skip to a window reads the range behind the cursor
/// in one pass, and so does its way round from the bottom.
#[test]
fn previous_on_a_filtered_view_reads_the_range_once() {
    let n = 300_000usize;
    let values: Vec<String> = (0..n)
        .map(|i| {
            if i == 4 {
                "needle".to_string()
            } else {
                format!("hay{i}")
            }
        })
        .collect();
    let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
    // Even k only: the needle (k = 4) is view row 2, of 150,000.
    let lf = df.lazy().filter((col("k") % lit(2)).eq(lit(0)));
    let columns = || {
        searched_columns(
            &["k".to_string(), "v".to_string()],
            &lf.clone().collect_schema().unwrap(),
            &spec("needle", false),
        )
    };
    let previous = |row: usize, buffer: Option<(DataFrame, usize)>| {
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = reads.clone();
        let rows = ViewRows::of(lf.clone(), buffer);
        assert!(rows.reads_up_to && !rows.whole);
        let search = Search::new(rows, columns(), Arc::default(), move |read| {
            seen.lock().unwrap().push(read)
        });
        let found = search
            .run(Start { row, column: None }, Direction::Previous)
            .unwrap()
            .unwrap();
        let reads = reads.lock().unwrap().clone();
        (found.row, found.wrapped, reads)
    };
    let buffer = lf.clone().slice(140_000, 100).collect().unwrap();
    assert_eq!(
        previous(140_050, Some((buffer, 140_000))),
        (2, false, vec![51, 140_051]),
        "the buffer, then every row before it at once"
    );
    assert_eq!(
        previous(1, None),
        (2, true, vec![2, 150_000]),
        "nothing behind; round from the bottom in one read too"
    );
}

/// `n` from deep in a view that cannot skip reads a first window as large as the
/// rows above it, rather than doubling up from a small one and reading those
/// rows again for each window.
#[test]
fn next_from_deep_in_a_filtered_view_reads_the_rows_above_once() {
    let n = 600_000usize;
    let values: Vec<String> = (0..n)
        .map(|i| {
            if i == 598_000 {
                "needle".to_string()
            } else {
                format!("hay{i}")
            }
        })
        .collect();
    let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
    // Even k only: the needle (k = 598,000) is view row 299,000, of 300,000.
    let lf = df.lazy().filter((col("k") % lit(2)).eq(lit(0)));
    let columns = searched_columns(
        &["k".to_string(), "v".to_string()],
        &lf.clone().collect_schema().unwrap(),
        &spec("needle", false),
    );
    let buffer = lf.clone().slice(200_000, 100).collect().unwrap();
    let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = reads.clone();
    let rows = ViewRows::of(lf, Some((buffer, 200_000)));
    assert!(rows.reads_up_to && !rows.whole);
    let search = Search::new(rows, columns, Arc::default(), move |read| {
        seen.lock().unwrap().push(read)
    });
    let found = search
        .run(
            Start {
                row: 200_050,
                column: None,
            },
            Direction::Next,
        )
        .unwrap()
        .unwrap();
    assert_eq!((found.row, found.wrapped), (299_000, false));
    // The buffer's rest, then the rest of the view in one window: not 65,536
    // rows and then 131,072, each reading the 200,000 above it again.
    assert_eq!(reads.lock().unwrap().as_slice(), [50, 99_950]);
}

#[test]
fn a_view_skips_to_a_window_only_unfiltered_over_parquet_or_ipc() {
    use crate::table::reads_up_to_a_window;
    let dir = tempfile::tempdir().unwrap();
    let mut df = df!("k" => [1i64, 2, 3]).unwrap();
    let csv = dir.path().join("t.csv");
    CsvWriter::new(std::fs::File::create(&csv).unwrap())
        .finish(&mut df)
        .unwrap();
    let parquet = dir.path().join("t.parquet");
    ParquetWriter::new(std::fs::File::create(&parquet).unwrap())
        .finish(&mut df)
        .unwrap();
    let csv = LazyCsvReader::new(PlRefPath::try_from_path(&csv).unwrap())
        .finish()
        .unwrap();
    let parquet = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&parquet).unwrap(),
        Default::default(),
    )
    .unwrap();
    assert!(reads_up_to_a_window(&csv));
    assert!(!reads_up_to_a_window(&parquet));
    assert!(reads_up_to_a_window(&parquet.filter(col("k").gt(lit(1)))));
}

#[test]
fn bytes_and_nested_columns_are_skipped() {
    let schema = Schema::from_iter([
        Field::new("b".into(), DataType::Binary),
        Field::new("l".into(), DataType::List(Box::new(DataType::String))),
        Field::new("s".into(), DataType::String),
    ]);
    let order = ["b".to_string(), "l".to_string(), "s".to_string()];
    let searched: Vec<String> = searched_columns(&order, &schema, &spec("x", false))
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(searched, ["s"]);
}

#[test]
fn the_label_quotes_text_and_slashes_a_regex() {
    assert_eq!(spec("abc", false).label(), "\"abc\"");
    assert_eq!(spec("a+", true).label(), "/a+/");
    let long = spec(&"x".repeat(40), false).label();
    assert!(long.chars().count() < 25, "{long}");
}

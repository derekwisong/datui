use super::*;

fn count(df: DataFrame, column: &str) -> ValueCounts {
    let plan = Plan {
        lf: df.lazy(),
        column: column.to_string(),
        read: Read::Exact,
        known_total: None,
        streaming: false,
    };
    plan.run(&ReadWatch::default()).unwrap()
}

fn values(counts: &ValueCounts, order: Order) -> Vec<(String, u64)> {
    counts
        .lines(order)
        .iter()
        .map(|line| {
            let label = match line.kind {
                LineKind::Value(at) => {
                    crate::exact::str_value(&counts.value(at).unwrap()).into_owned()
                }
                LineKind::Null => "null".to_string(),
                LineKind::Other(n) => format!("other {n}"),
            };
            (label, line.rows)
        })
        .collect()
}

#[test]
fn counts_list_by_rows_then_by_value_with_nulls_on_their_own_line() {
    let df =
        df!("k" => [Some("b"), Some("a"), None, Some("b"), Some("c"), Some("a"), Some("b"), None])
            .unwrap();
    let counts = count(df, "k");
    let pair = |s: &str, n| (s.to_string(), n);
    // By count the nulls rank by their rows, after values with as many.
    assert_eq!(
        values(&counts, Order::Count),
        [pair("b", 3), pair("a", 2), pair("null", 2), pair("c", 1)]
    );
    assert_eq!(
        values(&counts, Order::Value),
        [pair("a", 2), pair("b", 3), pair("c", 1), pair("null", 2)]
    );
    let lines = counts.lines(Order::Count);
    assert_eq!(
        lines.iter().map(|l| l.cumulative).collect::<Vec<_>>(),
        [3, 5, 7, 8]
    );
    assert_eq!(counts.summary.rows, 8);
    assert_eq!(counts.summary.distinct, 3);
    assert_eq!(counts.summary.nulls, 2);
    // Text has no sum, mean or range.
    assert_eq!(counts.summary.sum, None);
    assert_eq!(counts.summary.min, None);
    assert!(!counts.is_sample());
}

#[test]
fn past_the_top_values_the_rest_are_one_line() {
    let ids: Vec<i64> = (0..TOP_N as i64 + 5).chain([0, 0, 1]).collect();
    let counts = count(df!("id" => ids).unwrap(), "id");
    let lines = counts.lines(Order::Count);
    assert_eq!(lines.len(), TOP_N + 1);
    assert_eq!(lines[0].rows, 3, "0 is the most common");
    assert_eq!(lines[1].rows, 2);
    let other = lines.last().unwrap();
    assert_eq!(other.kind, LineKind::Other(5));
    assert_eq!(other.rows, 5);
    assert_eq!(other.cumulative, TOP_N as u64 + 8);
    assert_eq!(counts.summary.distinct, TOP_N + 5);
    // The table holds every value, nothing summed.
    let table = counts.table(Order::Count).unwrap();
    assert_eq!(table.height(), TOP_N + 5);
    assert_eq!(
        table.get_column_names(),
        ["id", "count", "percent", "cumulative_percent"]
    );
    let cumulative = table.column("cumulative_percent").unwrap().f64().unwrap();
    assert!((cumulative.get(TOP_N + 4).unwrap() - 100.0).abs() < 1e-9);
}

#[test]
fn integer_summary_is_exact_and_whole() {
    let df = df!("n" => [Some(3i64), Some(1), None, Some(3), Some(i64::MAX), Some(-2)]).unwrap();
    let summary = count(df, "n").summary;
    assert_eq!(summary.rows, 6);
    assert_eq!(summary.distinct, 4);
    assert_eq!(summary.nulls, 1);
    let sum = 3 + 1 + 3 + i64::MAX as i128 - 2;
    assert_eq!(summary.sum, Some(Number::Int(sum)));
    assert_eq!(summary.mean, Some(sum as f64 / 5.0));
    assert_eq!(summary.min, Some(AnyValue::Int64(-2)));
    assert_eq!(summary.max, Some(AnyValue::Int64(i64::MAX)));
}

#[test]
fn summary_math_over_counts() {
    // 2.5 twice, 1.0 once, 4.0 three times: the counts weight the sum.
    let values = Series::new("x".into(), [2.5f64, 1.0, 4.0]);
    let summary = Summary::of(&values, &[2, 1, 3]).unwrap();
    assert_eq!(summary.rows, 6);
    assert_eq!(summary.distinct, 3);
    assert_eq!(summary.nulls, 0);
    assert_eq!(summary.sum, Some(Number::Float(18.0)));
    assert_eq!(summary.mean, Some(3.0));
    assert_eq!(summary.min, Some(AnyValue::Float64(1.0)));
    assert_eq!(summary.max, Some(AnyValue::Float64(4.0)));

    // Unsigned values past i64 add up whole.
    let values = Series::new("u".into(), [u64::MAX, 1]);
    let summary = Summary::of(&values, &[2, 1]).unwrap();
    assert_eq!(summary.sum, Some(Number::Int(u64::MAX as i128 * 2 + 1)));

    // Only nulls: nothing to add, no mean, no range.
    let values = Series::new_null("z".into(), 1)
        .cast(&DataType::Int32)
        .unwrap();
    let summary = Summary::of(&values, &[4]).unwrap();
    assert_eq!((summary.rows, summary.distinct, summary.nulls), (4, 0, 4));
    assert_eq!(summary.sum, Some(Number::Int(0)));
    assert_eq!(summary.mean, None);
    assert_eq!(summary.min, None);
}

#[test]
fn dates_have_a_range_and_no_sum() {
    let dates = Series::new("d".into(), [19000i32, 19005, 18999])
        .cast(&DataType::Date)
        .unwrap();
    let summary = Summary::of(&dates, &[1, 1, 1]).unwrap();
    assert_eq!(summary.sum, None);
    assert_eq!(summary.min, Some(AnyValue::Date(18999)));
    assert_eq!(summary.max, Some(AnyValue::Date(19005)));
}

#[test]
fn an_empty_view_counts_nothing() {
    let df = df!("k" => Vec::<i32>::new()).unwrap();
    let counts = count(df, "k");
    assert!(counts.lines(Order::Count).is_empty());
    assert_eq!(counts.summary.rows, 0);
    assert_eq!(counts.summary.mean, None);
}

#[test]
fn a_stopped_count_is_no_count() {
    let watch = ReadWatch::default();
    watch.stop();
    let plan = Plan {
        lf: df!("k" => [1, 2, 3]).unwrap().lazy(),
        column: "k".to_string(),
        read: Read::Exact,
        known_total: None,
        streaming: false,
    };
    assert!(plan.run(&watch).is_err());
}

#[test]
fn a_small_or_in_memory_view_is_counted_exactly() {
    // In memory, a sample would read every row: the count is exact.
    let plan = Plan {
        lf: df!("k" => (0..50i32).collect::<Vec<_>>()).unwrap().lazy(),
        column: "k".to_string(),
        read: Read::Quick {
            sample_rows: 10,
            seed: 1,
            remote: true,
        },
        known_total: Some(50),
        streaming: false,
    };
    let counts = plan.run(&ReadWatch::default()).unwrap();
    assert!(!counts.is_sample());
    assert_eq!(counts.summary.rows, 50);
}

#[test]
fn a_large_parquet_file_is_sampled_first_and_says_of_how_many() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("big.parquet");
    let mut df = df!("k" => (0..20_000i64).map(|i| i % 7).collect::<Vec<_>>()).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    ParquetWriter::new(file)
        .with_row_group_size(Some(1_000))
        .finish(&mut df)
        .unwrap();
    let lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default())
        .unwrap();
    let plan = |read| Plan {
        lf: lf.clone(),
        column: "k".to_string(),
        read,
        known_total: Some(20_000),
        streaming: false,
    };
    let quick = Read::Quick {
        sample_rows: 2_000,
        seed: 7,
        remote: true,
    };
    let sampled = plan(quick).run(&ReadWatch::default()).unwrap();
    assert_eq!(sampled.sampled_of, Some(20_000));
    assert_eq!(sampled.summary.rows, 2_000);
    let exact = plan(Read::Exact).run(&ReadWatch::default()).unwrap();
    assert!(!exact.is_sample());
    assert_eq!(exact.summary.rows, 20_000);
    assert_eq!(exact.summary.distinct, 7);
}

/// Every line's rows against a group-by of the same frame, and the rows a
/// drill into each line's value finds, for the types that group oddly: NaN and
/// -0.0, empty and escaped text, categoricals, dates, lists, structs, decimals.
#[test]
fn counts_and_drills_agree_with_a_group_by_for_every_kind_of_value() {
    let mixed = df!(
            "f" => [Some(1.5f64), Some(f64::NAN), None, Some(f64::NAN), Some(-0.0), Some(0.1 + 0.2)],
            "s" => [Some(""), Some("a\tb"), Some("x\ny"), None, Some(""), Some("'\"\\")],
            "b" => [Some(true), Some(false), None, Some(true), Some(true), None],
            "l" => [Some(Series::new("".into(), [1i32, 2])), None, Some(Series::new("".into(), [1i32, 2])), Some(Series::new("".into(), Vec::<i32>::new())), None, None],
            "n" => [1.25f64, 1.25, 3.5, 3.5, 3.5, 0.0],
            "d" => [Some(19000i32), None, Some(19000), Some(1), None, Some(1)],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("s")
                .cast(DataType::from_categories(Categories::global()))
                .alias("c"),
            col("n").cast(DataType::Decimal(10, 2)).alias("x"),
            col("d").cast(DataType::Date),
            as_struct(vec![col("b"), col("d")]).alias("st"),
        ])
        .collect()
        .unwrap();
    for column in ["f", "s", "b", "l", "c", "x", "d", "st"] {
        let counts = count(mixed.clone(), column);
        let groups = mixed
            .clone()
            .lazy()
            .group_by([col(column)])
            .agg([len()])
            .collect()
            .unwrap()
            .height();
        let summary = &counts.summary;
        assert_eq!(
            summary.distinct + usize::from(summary.nulls > 0),
            groups,
            "{column}"
        );
        let lines = counts.lines(Order::Count);
        assert_eq!(lines.last().unwrap().cumulative, 6, "{column}");
        let dtype = mixed.schema().get(column).unwrap().clone();
        for line in lines {
            let value = match line.kind {
                LineKind::Value(at) => counts.value(at).unwrap(),
                LineKind::Null => AnyValue::Null,
                LineKind::Other(_) => unreachable!(),
            };
            let found = mixed
                .clone()
                .lazy()
                .filter(col(column).eq_missing(lit(Scalar::new(dtype.clone(), value))))
                .collect()
                .unwrap()
                .height();
            assert_eq!(found as u64, line.rows, "{column} {:?}", line.kind);
        }
    }
}

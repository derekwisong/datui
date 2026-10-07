use super::*;

/// The raw values behind [`durations`], one row each.
const DURATION_VALUES: [Option<i64>; 8] = [
    Some(3_723_004),
    None,
    Some(-1_500),
    Some(0),
    Some(1),
    Some(-1),
    Some(i64::MAX),
    Some(-i64::MAX),
];

/// A duration column per unit over the same raw values, with a null, zero,
/// negatives and the ends of the range.
pub(crate) fn durations() -> DataFrame {
    let values = Series::new("".into(), DURATION_VALUES);
    let columns = [
        ("ms", TimeUnit::Milliseconds),
        ("us", TimeUnit::Microseconds),
        ("ns", TimeUnit::Nanoseconds),
    ]
    .map(|(name, unit)| {
        values
            .cast(&DataType::Duration(unit))
            .unwrap()
            .with_name(name.into())
            .into_column()
    });
    DataFrame::new_infer_height(columns.to_vec()).unwrap()
}

/// [`durations`] as text, by column; a null is None.
pub(crate) fn duration_text() -> [(&'static str, [Option<&'static str>; 8]); 3] {
    [
        (
            "ms",
            [
                Some("PT3723.004S"),
                None,
                Some("-PT1.5S"),
                Some("P0D"),
                Some("PT0.001S"),
                Some("-PT0.001S"),
                Some("PT9223372036854775.807S"),
                Some("-PT9223372036854775.807S"),
            ],
        ),
        (
            "us",
            [
                Some("PT3.723004S"),
                None,
                Some("-PT0.0015S"),
                Some("P0D"),
                Some("PT0.000001S"),
                Some("-PT0.000001S"),
                Some("PT9223372036854.775807S"),
                Some("-PT9223372036854.775807S"),
            ],
        ),
        (
            "ns",
            [
                Some("PT0.003723004S"),
                None,
                Some("-PT0.0000015S"),
                Some("P0D"),
                Some("PT0.000000001S"),
                Some("-PT0.000000001S"),
                Some("PT9223372036.854775807S"),
                Some("-PT9223372036.854775807S"),
            ],
        ),
    ]
}

/// A duration is ISO 8601 text in every unit, with its nulls, the same
/// text in memory and planned, and the same text an NDJSON export writes.
#[test]
fn durations_are_iso_8601_as_the_json_writer_spells_them() {
    let df = durations();
    let cells = frame_as_json(&df).unwrap();
    for (name, expected) in duration_text() {
        let text = cells.column(name).unwrap().str().unwrap();
        assert_eq!(text.iter().collect::<Vec<_>>(), expected, "{name}");
    }
    let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
    assert!(cells.equals_missing(&lazy), "{cells}\n{lazy}");

    let mut ndjson = Vec::new();
    JsonWriter::new(&mut ndjson)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df.clone())
        .unwrap();
    let json = |name: &str, i: usize| {
        cells
            .column(name)
            .unwrap()
            .str()
            .unwrap()
            .get(i)
            .map_or("null".to_string(), |s| format!("\"{s}\""))
    };
    let rebuilt: String = (0..cells.height())
        .map(|i| {
            format!(
                "{{\"ms\":{},\"us\":{},\"ns\":{}}}\n",
                json("ms", i),
                json("us", i),
                json("ns", i)
            )
        })
        .collect();
    assert_eq!(rebuilt, String::from_utf8(ndjson).unwrap());
}

/// Past the end of chrono's range, where the JSON writer gives up, the
/// text is still exact.
#[test]
fn the_longest_negative_duration_is_exact() {
    let mut text = String::new();
    duration_iso(i64::MIN, TimeUnit::Milliseconds, &mut text);
    assert_eq!(text, "-PT9223372036854775.808S");
    text.clear();
    duration_iso(i64::MIN, TimeUnit::Nanoseconds, &mut text);
    assert_eq!(text, "-PT9223372036.854775808S");
}

/// Dates and datetimes in every unit, with and without a zone, and their
/// nulls, years before 0 and past 9999 among them; `past` adds the ends of
/// the stored range, which no writer takes.
pub(crate) fn calendar(past: bool) -> DataFrame {
    let mut stamps = vec![
        Some(0i64),
        None,
        Some(-1),
        Some(1_700_000_000_123),
        Some(-62_000_000_000_000),
        Some(-100_000_000_000_000),
        Some(300_000_000_000_000),
    ];
    let mut days = vec![
        Some(0i32),
        None,
        Some(-1),
        Some(19_724),
        Some(-800_000),
        Some(-1_000_000),
        Some(3_000_000),
    ];
    if past {
        stamps.extend([Some(i64::MIN + 1), Some(i64::MAX)]);
        days.extend([Some(i32::MIN), Some(i32::MAX)]);
    }
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let mut columns = vec![
        Series::new("d".into(), days)
            .cast(&DataType::Date)
            .unwrap()
            .into_column(),
    ];
    for (unit, name) in [
        (TimeUnit::Milliseconds, "ms"),
        (TimeUnit::Microseconds, "us"),
        (TimeUnit::Nanoseconds, "ns"),
    ] {
        for (zone, suffix) in [(None, ""), (paris.clone(), "_tz")] {
            columns.push(
                Series::new(format!("{name}{suffix}").into(), &stamps)
                    .cast(&DataType::Datetime(unit, zone))
                    .unwrap()
                    .into_column(),
            );
        }
    }
    DataFrame::new_infer_height(columns).unwrap()
}

/// Given as text, dates and datetimes read exactly as the CSV and JSON
/// writers write them.
#[test]
fn dates_as_text_are_what_the_writers_write() {
    let df = calendar(false);
    let mut csv = Vec::new();
    CsvWriter::new(&mut csv).finish(&mut df.clone()).unwrap();
    let mut as_text = Vec::new();
    CsvWriter::new(&mut as_text)
        .finish(&mut frame_as_json(&df).unwrap())
        .unwrap();
    assert_eq!(
        String::from_utf8(as_text).unwrap(),
        String::from_utf8(csv).unwrap()
    );
    let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
    assert!(frame_as_json(&df).unwrap().equals_missing(&lazy));

    let mut json = Vec::new();
    JsonWriter::new(&mut json)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df.clone())
        .unwrap();
    let mut prepared = lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap();
    // Nanosecond datetimes go to the writer as they are.
    assert!(
        prepared
            .columns()
            .iter()
            .all(|c| { (c.dtype() == &DataType::String) != c.name().starts_with("ns") })
    );
    let mut as_text = Vec::new();
    JsonWriter::new(&mut as_text)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut prepared)
        .unwrap();
    assert_eq!(
        String::from_utf8(as_text).unwrap(),
        String::from_utf8(json).unwrap()
    );
}

/// The writers panic on a date past the calendar; as text it is its stored
/// number, alone or in a list, and the rest of its column is unchanged.
#[test]
fn a_date_past_the_calendar_is_written_as_its_stored_number() {
    let df = calendar(true);
    let fine = frame_as_json(&calendar(false)).unwrap();
    let cells = frame_as_json(&df).unwrap();
    assert!(cells.slice(0, fine.height()).equals_missing(&fine));
    let text = |name: &str, row: usize| {
        cells
            .column(name)
            .unwrap()
            .str()
            .unwrap()
            .get(row)
            .map(str::to_string)
    };
    let last = df.height() - 1;
    assert_eq!(
        text("d", last - 1).as_deref(),
        Some("-2147483648 days since 1970-01-01")
    );
    assert_eq!(
        text("ms_tz", last).as_deref(),
        Some("9223372036854775807 ms since 1970-01-01 UTC")
    );
    assert_eq!(
        text("us", last - 1).as_deref(),
        Some("-9223372036854775807 us since 1970-01-01 UTC")
    );
    // Every nanosecond count is a date: the writer takes the column as it is.
    assert_eq!(
        cells.column("ns_tz").unwrap().dtype(),
        df.column("ns_tz").unwrap().dtype()
    );
    let mut csv = Vec::new();
    CsvWriter::new(&mut csv).finish(&mut cells.clone()).unwrap();
    assert!(
        String::from_utf8(csv)
            .unwrap()
            .contains(",2262-04-11T23:47:16.854775807,")
    );
    let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
    assert!(cells.equals_missing(&lazy));

    let mut json = Vec::new();
    JsonWriter::new(&mut json)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap())
        .unwrap();
    let json = String::from_utf8(json).unwrap();
    assert!(
        json.lines()
            .last()
            .unwrap()
            .contains(r#""us_tz":"9223372036854775807 us since 1970-01-01 UTC""#),
        "{json}"
    );

    let listed = df
        .clone()
        .lazy()
        .select([col("us").implode(true), col("d").implode(true)])
        .collect()
        .unwrap();
    let cells = frame_as_json(&listed).unwrap();
    let us = cells
        .column("us")
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap()
        .to_string();
    assert!(
            us.starts_with(r#"["1970-01-01 00:00:00",null,"#)
                && us.ends_with(r#""-9223372036854775807 us since 1970-01-01 UTC","9223372036854775807 us since 1970-01-01 UTC"]"#),
            "{us}"
        );
}

fn nested() -> DataFrame {
    let ids = Series::new("ids".into(), [1i64, 2, 3]);
    let lists = Series::new(
        "xs".into(),
        [
            Some(Series::new("".into(), ["a", "b"])),
            None,
            Some(Series::new("".into(), ["say \"hi\", then go"])),
        ],
    );
    let point = StructChunked::from_series(
        "point".into(),
        3,
        [
            Series::new("x".into(), [1i64, 2, 3]),
            Series::new("y".into(), [Some("a"), None, Some("c")]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let pairs = Series::new(
        "pair".into(),
        [
            Some(Series::new("".into(), [1.5f64, f64::NAN])),
            Some(Series::new("".into(), [0.0f64, 2.0])),
            None,
        ],
    )
    .cast(&DataType::Array(Box::new(DataType::Float64), 2))
    .unwrap();
    DataFrame::new_infer_height(vec![ids.into(), lists.into(), point.into(), pairs.into()]).unwrap()
}

#[test]
fn nested_columns_become_json_and_the_rest_stay() {
    let df = frame_as_json(&nested()).unwrap();
    assert_eq!(df.column("ids").unwrap().dtype(), &DataType::Int64);
    let xs = df.column("xs").unwrap().str().unwrap().clone();
    assert_eq!(xs.get(0), Some(r#"["a","b"]"#));
    assert_eq!(xs.get(1), None, "a null list stays null");
    assert_eq!(xs.get(2), Some(r#"["say \"hi\", then go"]"#));
    let point = df.column("point").unwrap().str().unwrap().clone();
    assert_eq!(point.get(0), Some(r#"{"x":1,"y":"a"}"#));
    assert_eq!(point.get(1), Some(r#"{"x":2,"y":null}"#));
    let pair = df.column("pair").unwrap().str().unwrap().clone();
    assert_eq!(pair.get(0), Some("[1.5,null]"), "NaN has no JSON spelling");
    assert_eq!(pair.get(1), Some("[0.0,2.0]"));
    assert_eq!(pair.get(2), None);
}

#[test]
fn lazy_and_in_memory_agree() {
    let eager = frame_as_json(&nested()).unwrap();
    let lazy = lazy_as_json(nested().lazy()).unwrap().collect().unwrap();
    assert!(eager.equals_missing(&lazy), "{eager}\n{lazy}");
}

/// Dates, datetimes and categoricals inside a struct or list read the same
/// in a CSV cell as in an NDJSON export of the same frame.
#[test]
fn cells_match_an_ndjson_export() {
    let df = df!(
        "d" => [Some("2024-01-02"), None],
        "c" => [Some("a"), None],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("d").str().to_date(StrptimeOptions::default()),
        col("c").cast(DataType::from_categories(Categories::global())),
    ])
    .with_columns([col("d")
        .cast(DataType::Datetime(
            TimeUnit::Microseconds,
            Some(TimeZone::UTC),
        ))
        .alias("dt")])
    .select([
        as_struct(vec![col("d"), col("dt"), col("c")]).alias("s"),
        col("c").implode(true).alias("lc"),
    ])
    .collect()
    .unwrap();
    let mut ndjson = Vec::new();
    JsonWriter::new(&mut ndjson)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df.clone())
        .unwrap();
    let cells = frame_as_json(&df).unwrap();
    let (s, lc) = (
        cells.column("s").unwrap().str().unwrap(),
        cells.column("lc").unwrap().str().unwrap(),
    );
    let rebuilt: String = (0..cells.height())
        .map(|i| {
            format!(
                "{{\"s\":{},\"lc\":{}}}\n",
                s.get(i).unwrap(),
                lc.get(i).unwrap()
            )
        })
        .collect();
    assert_eq!(rebuilt, String::from_utf8(ndjson).unwrap());
    assert!(rebuilt.contains(r#""dt":"2024-01-02T00:00:00+00:00","c":"a""#));
}

/// Binary is base64 at any depth, with its nulls, and the same in a copy,
/// a CSV cell and an NDJSON export.
#[test]
fn binary_is_base64_everywhere() {
    let blob = Series::new("blob".into(), [Some(b"hi\xff".as_slice()), None]);
    let blobs = Series::new(
        "blobs".into(),
        [Some(Series::new("".into(), [b"x".as_slice()])), None],
    );
    let pair = Series::new(
        "pair".into(),
        [
            Some(Series::new("".into(), [b"a".as_slice(), b"b".as_slice()])),
            None,
        ],
    )
    .cast(&DataType::Array(Box::new(DataType::Binary), 2))
    .unwrap();
    let meta = StructChunked::from_series(
        "meta".into(),
        2,
        [Series::new(
            "raw".into(),
            [b"ab".as_slice(), b"".as_slice()],
        )]
        .iter(),
    )
    .unwrap()
    .with_outer_validity(Some([true, false].into_iter().collect()))
    .into_series();
    let df = DataFrame::new_infer_height(vec![blob.into(), blobs.into(), pair.into(), meta.into()])
        .unwrap();

    for column in df.columns() {
        let text = leaves_as_json_text(column.as_materialized_series()).unwrap();
        assert_eq!(text.dtype(), &json_text_dtype(column.dtype()));
        assert_eq!(text.null_count(), 1, "{text}");
    }

    let cells = frame_as_json(&df).unwrap();
    let cell = |name: &str| cells.column(name).unwrap().str().unwrap().get(0);
    assert_eq!(cell("blob"), Some("aGn/"));
    assert_eq!(cell("blobs"), Some(r#"["eA=="]"#));
    assert_eq!(cell("pair"), Some(r#"["YQ==","Yg=="]"#));
    assert_eq!(cell("meta"), Some(r#"{"raw":"YWI="}"#));
    let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
    assert!(cells.equals_missing(&lazy), "{cells}\n{lazy}");

    let mut ndjson = Vec::new();
    JsonWriter::new(&mut ndjson)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut lazy_for_json(df.clone().lazy()).unwrap().collect().unwrap())
        .unwrap();
    assert_eq!(
        String::from_utf8(ndjson).unwrap().lines().next(),
        Some(r#"{"blob":"aGn/","blobs":["eA=="],"pair":["YQ==","Yg=="],"meta":{"raw":"YWI="}}"#)
    );

    let copy =
        crate::clipboard::tabular_payload(&df, crate::clipboard::CopyFormat::Tsv, true, true)
            .unwrap();
    let row: Vec<&str> = copy.text.lines().nth(1).unwrap().split('\t').collect();
    assert_eq!(
        row,
        [
            "aGn/",
            r#""[""eA==""]""#,
            r#""[""YQ=="",""Yg==""]""#,
            r#""{""raw"":""YWI=""}""#
        ]
    );
}

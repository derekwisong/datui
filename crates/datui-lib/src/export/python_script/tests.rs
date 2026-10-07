use super::*;

fn statement(column: &str, operator: FilterOperator, value: &str) -> FilterStatement {
    FilterStatement {
        columns: Vec::new(),
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op: LogicalOperator::And,
    }
}

fn script(steps: Vec<Step>) -> String {
    Script {
        source: Source::Read {
            call: "pl.scan_parquet(\"sales.parquet\")".to_string(),
            after: Vec::new(),
            notes: Vec::new(),
            imports: Vec::new(),
        },
        steps,
    }
    .render()
}

/// Six dates, times, durations, decimals and floats, a null in each.
fn typed_frame() -> DataFrame {
    let tz = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let us = |h: i64| 1_704_067_200_000_000 + h * 3_600_000_000;
    df!(
            "d" => &[Some(19723i32), Some(19724), Some(19725), None],
            "t" => &[Some(us(0)), Some(us(5)), Some(us(24)), None],
            "c" => &[Some(5 * 3_600_000_000_000i64), Some(6 * 3_600_000_000_000 + 500_000_000), Some(7 * 3_600_000_000_000), None],
            "du" => &[Some(1_000i64), Some(90_000), Some(3_600_000), None],
            "m" => &[Some("1.50"), Some("2.00"), Some("3.25"), None],
            "f" => &[Some(0.1f32), Some(0.2), Some(0.1), None],
            "x" => &[Some(0.1 + 0.2), Some(0.3), Some(1.0), None],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("d").cast(DataType::Date),
            col("t").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
            col("t")
                .cast(DataType::Datetime(TimeUnit::Microseconds, tz))
                .alias("z"),
            col("c").cast(DataType::Time),
            col("du").cast(DataType::Duration(TimeUnit::Milliseconds)),
            col("m").cast(DataType::Decimal(10, 2)),
        ])
        .collect()
        .unwrap()
}

#[test]
fn every_operator_compares_in_the_columns_own_type() {
    use FilterOperator::*;
    let frame = typed_frame();
    let schema = frame.schema().clone();
    let rows = |column: &str, operator, value: &str| {
        let statement = statement(column, operator, value);
        assert_eq!(SidebarFilter::problem(&statement, schema.get(column)), None);
        let typed = SidebarFilter::typed(&statement, schema.get(column));
        frame
            .clone()
            .lazy()
            .filter(filters_expr(&[typed]).unwrap())
            .collect()
            .unwrap_or_else(|e| panic!("{column} {operator:?} {value}: {e}"))
            .height()
    };
    for (column, value, counts) in [
        ("d", "2024-01-02", [1, 2, 1, 1, 2, 2]),
        // A date alone is its midnight.
        ("t", "2024-01-01", [1, 2, 2, 0, 3, 1]),
        ("t", "2024-01-01 05:00", [1, 2, 1, 1, 2, 2]),
        ("t", "2024-01-01T05:00:00.000", [1, 2, 1, 1, 2, 2]),
        // A clock in the column's zone: 06:00 in Paris is 05:00 UTC.
        ("z", "2024-01-01 06:00", [1, 2, 1, 1, 2, 2]),
        ("z", "2024-01-01 05:00+00:00", [1, 2, 1, 1, 2, 2]),
        ("c", "06:00:00.5", [1, 2, 1, 1, 2, 2]),
        ("du", "1m 30s", [1, 2, 1, 1, 2, 2]),
        ("m", "2", [1, 2, 1, 1, 2, 2]),
        ("m", "1.5", [1, 2, 2, 0, 3, 1]),
        // Exact: 0.1 + 0.2 is not 0.3.
        ("x", "0.3", [1, 2, 2, 0, 3, 1]),
    ] {
        let got = [Eq, NotEq, Gt, Lt, GtEq, LtEq].map(|op| rows(column, op, value));
        assert_eq!(got, counts, "{column} {value}");
    }
    // A float32 column reads the value as a float32.
    assert_eq!(rows("f", Eq, "0.1"), 2);
    assert_eq!(rows("x", IsNull, ""), 1);
    assert_eq!(rows("x", IsNotNull, ""), 3);
}

#[test]
fn a_value_that_does_not_read_as_the_column_says_so() {
    let frame = typed_frame();
    let schema = frame.schema();
    let problem = |column: &str, operator, value: &str| {
        SidebarFilter::problem(&statement(column, operator, value), schema.get(column))
    };
    assert_eq!(
        problem("d", FilterOperator::Eq, "2024-13-01").as_deref(),
        Some("d: \"2024-13-01\" is not a date written YYYY-MM-DD")
    );
    assert!(problem("t", FilterOperator::Gt, "soon").is_some());
    assert!(problem("x", FilterOperator::Lt, "abc").is_some());
    // Text matching and null tests read no value of the column's type.
    assert_eq!(problem("d", FilterOperator::Contains, "2024"), None);
    assert_eq!(problem("d", FilterOperator::IsNull, ""), None);
}

#[test]
fn typed_filters_read_back_in_python() {
    let frame = typed_frame();
    let schema = frame.schema();
    let python = |column: &str, operator, value: &str| {
        SidebarFilter::typed(&statement(column, operator, value), schema.get(column)).python()
    };
    assert_eq!(
        python("d", FilterOperator::Eq, "2024-01-02"),
        "pl.col(\"d\") == pl.date(2024, 1, 2)"
    );
    assert_eq!(
        python("t", FilterOperator::Gt, "2024-01-01 05:00"),
        "pl.col(\"t\") > pl.datetime(2024, 1, 1, 5, 0, 0, 0, time_unit=\"us\")"
    );
    assert_eq!(
        python("z", FilterOperator::LtEq, "2024-01-01 06:00"),
        "pl.col(\"z\") <= pl.datetime(2024, 1, 1, 5, 0, 0, 0, time_unit=\"us\", \
             time_zone=\"UTC\").dt.convert_time_zone(\"Europe/Paris\")"
    );
    assert_eq!(
        python("c", FilterOperator::Eq, "06:00:00.5"),
        "pl.col(\"c\") == pl.time(6, 0, 0, 500000)"
    );
    assert_eq!(
        python("du", FilterOperator::Lt, "1h"),
        "pl.col(\"du\") < pl.duration(milliseconds=3600000, time_unit=\"ms\")"
    );
    assert_eq!(
        python("m", FilterOperator::NotEq, "1.5"),
        "pl.col(\"m\") != pl.lit(\"1.50\").cast(pl.Decimal(10, 2))"
    );
    assert_eq!(
        python("x", FilterOperator::Eq, "0.3"),
        "pl.col(\"x\") == 0.3"
    );
    assert_eq!(
        python("x", FilterOperator::IsNull, ""),
        "pl.col(\"x\").is_null()"
    );
}

#[test]
fn strings_and_floats_read_back_in_python() {
    assert_eq!(py_str("a\"b\\c\nd"), "\"a\\\"b\\\\c\\nd\"");
    assert_eq!(py_str("\u{1}"), "\"\\u0001\"");
    assert_eq!(py_float(5.0), "5.0");
    assert_eq!(py_float(0.1), "0.1");
    assert_eq!(py_float(1e20), "1e20");
    assert_eq!(py_float(f64::NAN), "float(\"nan\")");
}

#[test]
fn text_in_a_comment_cannot_end_it() {
    assert_eq!(
        py_comment("a\nimport os\r\u{2028}x"),
        "# a\\nimport os\\r\\u2028x"
    );
    let text = script(vec![Step::Unreproducible("where k is \"\nboom()".into())]);
    assert!(text.contains("    # where k is \"\\nboom()\n"), "{text}");
}

#[test]
fn sql_is_triple_quoted_only_where_nothing_in_it_ends_the_string() {
    let sql = |sql: &str| {
        Step::Sql {
            sql: sql.into(),
            ordered_by: Vec::new(),
        }
        .python()
    };
    assert_eq!(
        sql("SELECT *\nFROM df"),
        vec![
            ".sql(",
            "    \"\"\"SELECT *\nFROM df\"\"\",",
            "    table_name=\"df\",",
            ")"
        ]
    );
    assert_eq!(
        sql("SELECT *\nFROM df ORDER BY \"a\""),
        vec![".sql(\"SELECT *\\nFROM df ORDER BY \\\"a\\\"\", table_name=\"df\")"]
    );
    // Commented out, every line of it is a comment.
    let text = script(vec![
        Step::Unreproducible("drilled into a group held as lists".into()),
        Step::Sql {
            sql: "SELECT *\nFROM df".into(),
            ordered_by: Vec::new(),
        },
    ]);
    assert!(text.contains("    # FROM df\"\"\",\n"), "{text}");
}

#[test]
fn a_view_with_nothing_applied_is_the_reader() {
    assert_eq!(
        script(Vec::new()),
        "import polars as pl\n\ndf = pl.scan_parquet(\"sales.parquet\")\n"
    );
}

#[test]
fn filters_typed_by_column_then_a_multi_column_sort_then_a_projection() {
    let schema = Schema::from_iter([
        Field::new("region".into(), DataType::String),
        Field::new("amount".into(), DataType::Float64),
        Field::new("qty".into(), DataType::Int64),
    ]);
    let mut or = statement("qty", FilterOperator::GtEq, "3");
    or.logical_op = LogicalOperator::Or;
    let filters: Vec<SidebarFilter> = [
        statement("region", FilterOperator::Eq, "north"),
        statement("amount", FilterOperator::Gt, "10"),
        or,
    ]
    .iter()
    .map(|s| SidebarFilter::typed(s, schema.get(&s.column)))
    .collect();
    let text = script(vec![
        Step::Filter(filters),
        Step::Sort {
            columns: vec!["amount".into(), "region".into()],
            descending: vec![true, false],
        },
        Step::Select(vec!["order_id".into(), "customer".into(), "amount".into()]),
    ]);
    assert_eq!(
        text,
        "import polars as pl\n\n\
             df = (\n    \
             pl.scan_parquet(\"sales.parquet\")\n    \
             .filter(((pl.col(\"region\") == \"north\") & (pl.col(\"amount\") > 10.0)) | (pl.col(\"qty\") >= 3))\n    \
             .sort([\"amount\", \"region\"], descending=[True, False], nulls_last=True, maintain_order=True)\n    \
             .select([\"order_id\", \"customer\", \"amount\"])\n\
             )\n"
    );
}

#[test]
fn contains_filters_are_literal_and_a_number_that_does_not_parse_stays_text() {
    let s = SidebarFilter::typed(
        &statement("name", FilterOperator::NotContains, "a.b"),
        Some(&DataType::String),
    );
    assert_eq!(
        s.python(),
        "~pl.col(\"name\").str.contains(\"a.b\", literal=True)"
    );
    let s = SidebarFilter::typed(
        &statement("n", FilterOperator::Eq, "n/a"),
        Some(&DataType::Int64),
    );
    assert_eq!(s.value, FilterValue::Str("n/a".into()));
}

#[test]
fn one_sort_column_reads_plainly() {
    assert_eq!(
        sort_call(&["amount".into()], &[true]),
        ".sort(\"amount\", descending=True, nulls_last=True, maintain_order=True)"
    );
}

#[test]
fn steps_after_one_python_cannot_repeat_are_commented_out() {
    let text = script(vec![
        Step::Unreproducible("drilled into a group held as lists".into()),
        Step::Reverse,
    ]);
    assert!(
        text.contains("    # drilled into a group held as lists\n    # .reverse()\n"),
        "{text}"
    );
}

#[test]
fn a_placeholder_source_leaves_df_to_the_user() {
    let text = Script {
        source: Source::Placeholder {
            what: "The data datui read from standard input: load it here.".into(),
        },
        steps: vec![Step::Reverse],
    }
    .render();
    assert_eq!(
        text,
        "import polars as pl\n\n\
             # The data datui read from standard input: load it here.\n\
             df = ...\n\n\
             df = (\n    df.lazy()\n    .reverse()\n)\n"
    );
}

#[test]
fn a_grouped_query_groups_then_orders_by_its_keys() {
    let input = Schema::from_iter([
        Field::new("dept".into(), DataType::String),
        Field::new("salary".into(), DataType::Float64),
        Field::new("id".into(), DataType::Int64),
        Field::new("age".into(), DataType::Int64),
    ]);
    let text = script(vec![Step::Query {
        query: "select avg salary, n: count id by dept where age > 30".into(),
        input: Arc::new(input),
        keys: vec!["dept".into()],
    }]);
    assert!(
            text.contains(
                "    .filter(pl.col(\"age\") > 30.0)\n    \
                 .group_by(\"dept\")\n    \
                 .agg(pl.col(\"salary\").mean().alias(\"avg_salary\"), pl.col(\"id\").count().alias(\"n\"))\n    \
                 .sort(\"dept\", nulls_last=True, maintain_order=True)\n"
            ),
            "{text}"
        );
}

#[test]
fn csv_options_become_reader_arguments() {
    let mut options = OpenOptions::new();
    options.delimiter = Some(b';');
    options.has_header = Some(false);
    options.skip_rows = Some(2);
    options.null_values = Some(vec!["NA".into()]);
    options.skip_tail_rows = Some(1);
    let paths = vec![PathBuf::from("data/x.csv")];
    let schema = Schema::default();
    let record = OpenRecord {
        paths: Some(&paths),
        options: &options,
        schema: &schema,
        remote_objects: Vec::new(),
        s3_endpoint: None,
        s3_region: None,
        unsigned: false,
        format: None,
        read_mode: None,
        read_as_text: Vec::new(),
        spec: None,
    };
    let Source::Read { call, after, .. } = source(&record) else {
        panic!("a CSV has a reader");
    };
    assert_eq!(
        call,
        "pl.scan_csv(\"data/x.csv\", separator=\";\", has_header=False, skip_rows=2, \
             try_parse_dates=True, null_values=\"NA\")"
    );
    assert_eq!(
        after,
        vec![".filter(pl.int_range(pl.len()) < pl.len() - 1)"]
    );
}

/// Reads datui does its own way are not written as a Polars reader that would
/// give other rows: a format spec, a GPS log, and the CSV dialect flags.
#[test]
fn reads_python_cannot_repeat_leave_a_placeholder() {
    let schema = Schema::default();
    let placeholder = |paths: &[PathBuf], options: &OpenOptions, spec: Option<&str>| {
        let record = OpenRecord {
            paths: Some(paths),
            options,
            schema: &schema,
            remote_objects: Vec::new(),
            s3_endpoint: None,
            s3_region: None,
            unsigned: false,
            format: None,
            read_mode: None,
            read_as_text: Vec::new(),
            spec: spec.map(str::to_string),
        };
        match source(&record) {
            Source::Placeholder { what } => what,
            Source::Read { call, .. } => panic!("a reader was written: {call}"),
        }
    };
    let plain = OpenOptions::new();
    let what = placeholder(&[PathBuf::from("a.l2")], &plain, Some("acme.l2feed"));
    assert!(what.contains("acme.l2feed"), "{what}");
    let mut named = OpenOptions::new();
    named.spec_name = Some("acme.l2feed".into());
    placeholder(&[PathBuf::from("a.bin")], &named, None);
    placeholder(&[PathBuf::from("track.gpx")], &plain, None);
    placeholder(&[PathBuf::from("drive.nmea")], &plain, None);
    let csv = [PathBuf::from("log.csv")];
    let mut comment = OpenOptions::new();
    comment.comment_char = Some("######".into());
    assert!(placeholder(&csv, &comment, None).contains("--comment"));
    let mut rows = OpenOptions::new();
    rows.header_rows = vec![3, 2];
    assert!(placeholder(&csv, &rows, None).contains("--header-rows"));
    let mut space = OpenOptions::new();
    space.skip_initial_space = true;
    assert!(placeholder(&csv, &space, None).contains("--skip-initial-space"));
}

fn record_for<'a>(
    paths: &'a [PathBuf],
    options: &'a OpenOptions,
    schema: &'a Schema,
) -> OpenRecord<'a> {
    OpenRecord {
        paths: Some(paths),
        options,
        format: None,
        read_mode: None,
        schema,
        remote_objects: Vec::new(),
        s3_endpoint: None,
        s3_region: None,
        unsigned: false,
        read_as_text: Vec::new(),
        spec: None,
    }
}

fn call_of(source: Source) -> (String, Vec<String>) {
    match source {
        Source::Read { call, notes, .. } => (call, notes),
        Source::Placeholder { what } => panic!("a placeholder: {what}"),
    }
}

/// Every reader of an object store gets its settings: S3's endpoint and region
/// for NDJSON as for Parquet, an Azure account, and no signature for a public
/// place. A reader that reads a file whole says it reads no store.
#[test]
fn every_store_reader_gets_its_storage_options() {
    let schema = Schema::default();
    let options = OpenOptions::new();
    let s3 = [PathBuf::from("s3://b/logs/a.jsonl")];
    let mut record = record_for(&s3, &options, &schema);
    record.s3_endpoint = Some("http://localhost:9000".into());
    record.s3_region = Some("us-east-1".into());
    assert_eq!(
        call_of(source(&record)).0,
        "pl.scan_ndjson(\"s3://b/logs/a.jsonl\", storage_options={\"aws_endpoint_url\": \
             \"http://localhost:9000\", \"aws_region\": \"us-east-1\"})"
    );
    let gcs = [PathBuf::from("gs://public/x.parquet")];
    let mut record = record_for(&gcs, &options, &schema);
    record.unsigned = true;
    assert_eq!(
        call_of(source(&record)).0,
        "pl.scan_parquet(\"gs://public/x.parquet\", storage_options={\"skip_signature\": \"true\"})"
    );
    let azure = [PathBuf::from(
        "abfss://data@acct.dfs.core.windows.net/t/x.csv",
    )];
    let (call, notes) = call_of(source(&record_for(&azure, &options, &schema)));
    assert!(
        call.starts_with("pl.scan_csv(\"abfss://data@acct.dfs.core.windows.net/t/x.csv\", ")
            && call.contains("storage_options={\"account_name\": \"acct\"}"),
        "{call}"
    );
    assert!(
        notes.is_empty(),
        "the container is no credential: {notes:?}"
    );
    let json = [PathBuf::from("s3://b/x.json")];
    let (call, notes) = call_of(source(&record_for(&json, &options, &schema)));
    assert_eq!(call, "pl.read_json(\"s3://b/x.json\").lazy()");
    assert!(notes[0].contains("reads no object store"), "{notes:?}");
}

/// An `s3://<id>@bucket` URL is read as the plain URL, with no word of a
/// credential left out: the ID is datui's name for the source.
#[test]
fn a_source_id_is_no_credential() {
    assert_eq!(
        without_secrets("s3://minio@bucket/x.parquet"),
        ("s3://bucket/x.parquet".to_string(), false)
    );
    assert_eq!(
        without_secrets("abfss://c@a.dfs.core.windows.net/x"),
        ("abfss://c@a.dfs.core.windows.net/x".to_string(), false)
    );
}

/// Standard input recorded with `--tee` is read again from the file.
#[test]
fn a_teed_pipe_reads_its_file() {
    let schema = Schema::default();
    let mut options = OpenOptions::new();
    options.tee = Some(PathBuf::from("rec.csv"));
    let stdin = [PathBuf::from("-")];
    let mut record = record_for(&stdin, &options, &schema);
    record.format = Some(FileFormat::Csv);
    assert_eq!(
        call_of(source(&record)).0,
        "pl.scan_csv(\"rec.csv\", try_parse_dates=True)"
    );
}

/// A table datui read lazily that the script reads whole says so, as the Info
/// panel's `Read:` line does; one datui read in memory too says nothing more.
#[test]
fn a_whole_read_of_a_lazy_table_says_so() {
    let schema = Schema::default();
    let mut options = OpenOptions::new();
    options.table = Some("orders".into());
    let db = [PathBuf::from("shop.db")];
    let mut record = record_for(&db, &options, &schema);
    record.format = Some(FileFormat::Sqlite);
    record.read_mode = Some(crate::ReadMode::Lazy);
    let (call, notes) = call_of(source(&record));
    assert!(call.starts_with("pl.read_database("), "{call}");
    assert!(
        notes.contains(
            &"Read: lazy scan in datui; pl.read_database reads the file whole into memory."
                .to_string()
        ),
        "{notes:?}"
    );
    let json = [PathBuf::from("a.json")];
    let mut record = record_for(&json, &options, &schema);
    record.read_mode = Some(crate::ReadMode::InMemory);
    assert!(call_of(source(&record)).1.is_empty());
}

/// `--comment` is Polars' `comment_prefix`.
#[test]
fn a_comment_character_is_the_comment_prefix() {
    let schema = Schema::default();
    let mut options = OpenOptions::new();
    options.comment_char = Some("#".into());
    let csv = [PathBuf::from("log.csv")];
    assert_eq!(
        call_of(source(&record_for(&csv, &options, &schema))).0,
        "pl.scan_csv(\"log.csv\", comment_prefix=\"#\", try_parse_dates=True)"
    );
}

/// A file known by its bytes, read as a format Polars has no reader for: the
/// placeholder names the format and the table on screen.
#[test]
fn a_placeholder_names_the_format_read_and_the_table() {
    let schema = Schema::default();
    let paths = vec![PathBuf::from("flight.bin")];
    let mut options = OpenOptions::new();
    options.table = Some("GPS".into());
    let record = OpenRecord {
        paths: Some(&paths),
        options: &options,
        format: Some(FileFormat::Dataflash),
        read_mode: None,
        schema: &schema,
        remote_objects: Vec::new(),
        s3_endpoint: None,
        s3_region: None,
        unsigned: false,
        read_as_text: Vec::new(),
        spec: None,
    };
    let Source::Placeholder { what } = source(&record) else {
        panic!("Polars reads no DataFlash");
    };
    assert_eq!(
        what,
        "flight.bin --table GPS: Polars has no reader for DataFlash files; load it here."
    );
}

#[test]
fn credentials_in_a_url_stay_out_of_the_script() {
    assert_eq!(
        without_secrets("https://u:p@host.example/d/x.parquet?X-Amz-Signature=abc#f"),
        ("https://host.example/d/x.parquet".to_string(), true)
    );
    assert_eq!(
        without_secrets("s3://bucket/data-?.parquet"),
        ("s3://bucket/data-?.parquet".to_string(), false)
    );
    let options = OpenOptions::new();
    let schema = Schema::default();
    let paths = vec![PathBuf::from(
        "https://user:secret@host.example/d/x.csv?token=s3cr3t",
    )];
    let record = OpenRecord {
        paths: Some(&paths),
        options: &options,
        schema: &schema,
        remote_objects: Vec::new(),
        s3_endpoint: Some("http://key:secret@localhost:9000".into()),
        s3_region: None,
        unsigned: false,
        format: None,
        read_mode: None,
        read_as_text: Vec::new(),
        spec: None,
    };
    let text = Script {
        source: source(&record),
        steps: Vec::new(),
    }
    .render();
    assert!(
        !text.contains("secret") && !text.contains("s3cr3t"),
        "{text}"
    );
    assert!(text.contains("\"https://host.example/d/x.csv\""), "{text}");
    assert!(text.contains("# datui left a user"), "{text}");
}

#[test]
fn stdin_and_bucket_prefixes() {
    let options = OpenOptions::new();
    let schema = Schema::default();
    let stdin = vec![PathBuf::from("-")];
    let record = |paths: &'static [PathBuf]| OpenRecord {
        paths: Some(paths),
        options: &options,
        schema: &schema,
        remote_objects: vec!["s3://b/p/year=2024/a.parquet".into()],
        s3_endpoint: Some("http://localhost:9000".into()),
        s3_region: None,
        unsigned: false,
        format: None,
        read_mode: None,
        read_as_text: Vec::new(),
        spec: None,
    };
    let stdin: &'static [PathBuf] = Box::leak(stdin.into_boxed_slice());
    assert!(matches!(source(&record(stdin)), Source::Placeholder { .. }));
    let prefix: &'static [PathBuf] = Box::leak(vec![PathBuf::from("s3://b/p/")].into_boxed_slice());
    let Source::Read { call, .. } = source(&record(prefix)) else {
        panic!("a Parquet prefix has a reader");
    };
    assert_eq!(
        call,
        "pl.scan_parquet(\"s3://b/p/**/*.parquet\", hive_partitioning=True, \
             storage_options={\"aws_endpoint_url\": \"http://localhost:9000\"})"
    );
}

/// A duration is matched as the table writes it, which the script cannot: left out
/// of the script's filter and named; a regex that does not compile is refused.
#[test]
fn a_kept_find_names_what_the_script_cannot_match() {
    let schema = Schema::from_iter([
        Field::new("name".into(), DataType::String),
        Field::new("took".into(), DataType::Duration(TimeUnit::Milliseconds)),
    ]);
    let statement = FilterStatement {
        columns: vec!["name".into(), "took".into()],
        column: crate::app::modals::filter_modal::ANY_COLUMN.to_string(),
        operator: FilterOperator::Has,
        value: "1d".to_string(),
        logical_op: LogicalOperator::And,
    };
    let filter = SidebarFilter::typed_in(&statement, &schema, &[]);
    assert_eq!(filter.unscriptable_columns(), ["took"]);
    assert!(!filter.python().contains("took"), "{}", filter.python());

    let bad = FilterStatement {
        columns: Vec::new(),
        column: "name".into(),
        operator: FilterOperator::HasRegex,
        value: "(".into(),
        logical_op: LogicalOperator::And,
    };
    let why = SidebarFilter::problem(&bad, Some(&DataType::String)).expect("refused");
    assert!(why.starts_with("Not a regex"), "{why}");
}

/// A find kept over every column searches the shown columns that hold text,
/// in the table and in the script alike: never a list, never a hidden column.
#[test]
fn a_kept_find_searches_the_shown_text_columns() {
    let schema = Schema::from_iter([
        Field::new("name".into(), DataType::String),
        Field::new("tags".into(), DataType::List(Box::new(DataType::String))),
        Field::new("note".into(), DataType::String),
        Field::new("n".into(), DataType::Int64),
    ]);
    let statement = FilterStatement {
        columns: Vec::new(),
        column: crate::app::modals::filter_modal::ANY_COLUMN.to_string(),
        operator: FilterOperator::Has,
        value: "al".to_string(),
        logical_op: LogicalOperator::And,
    };
    let shown = ["name", "tags", "n"].map(String::from);
    let filter = SidebarFilter::typed_in(&statement, &schema, &shown);
    let names: Vec<&str> = filter.searched.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["name", "n"]);
    let script = filter.python();
    assert!(
        !script.contains("tags") && !script.contains("note"),
        "{script}"
    );
    assert!(script.contains("pl.any_horizontal"), "{script}");
}

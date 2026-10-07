use super::*;

/// The sample reads with the separator the open reads with: the format's own, or
/// `--delimiter` over it. A sample that split on `,` regardless would see one
/// column in every TSV and PSV and in any CSV the flag was needed for.
/// A sampled file whose name holds `[` is that file, not the pattern it spells:
/// `d[1].jsonl` would read `d1.jsonl` (#625).
#[test]
fn the_sample_reads_a_file_named_like_a_glob() {
    let dir = tempfile::tempdir().unwrap();
    let names = |name: &str, format| {
        column_schema_of(&dir.path().join(name), format, &ReadAs::default())
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>()
    };
    std::fs::write(dir.path().join("d[1].jsonl"), "{\"own\": 1}\n").unwrap();
    std::fs::write(dir.path().join("d1.jsonl"), "{\"other\": 1}\n").unwrap();
    std::fs::write(dir.path().join("d[1].csv"), "own\n1\n").unwrap();
    std::fs::write(dir.path().join("d1.csv"), "other\n1\n").unwrap();
    assert_eq!(names("d[1].jsonl", crate::FileFormat::Jsonl), ["own"]);
    assert_eq!(names("d[1].csv", crate::FileFormat::Csv), ["own"]);
}

#[test]
fn the_sample_splits_on_the_separator_the_open_uses() {
    let dir = tempfile::tempdir().unwrap();
    let names = |name: &str, body: &str, format, delimiter| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        let as_read = ReadAs {
            delimiter,
            ..ReadAs::default()
        };
        column_schema_of(&path, format, &as_read)
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>()
    };
    use crate::FileFormat::{Csv, Psv, Tsv};
    assert_eq!(names("a.tsv", "a\tb\n1\t2\n", Tsv, None), ["a", "b"]);
    assert_eq!(names("a.psv", "a|b\n1|2\n", Psv, None), ["a", "b"]);
    assert_eq!(names("a.csv", "a|b\n1|2\n", Csv, None), ["a|b"]);
    assert_eq!(names("b.csv", "a|b\n1|2\n", Csv, Some(b'|')), ["a", "b"]);
}

fn file(columns: &[(&str, DataType)], rows: usize) -> Option<FileFooter> {
    let mut schema = Schema::with_capacity(columns.len());
    for (name, dtype) in columns {
        schema.with_column((*name).into(), dtype.clone());
    }
    Some(FileFooter {
        schema: Arc::new(schema),
        row_group_rows: vec![rows],
        file_bytes: 0,
        row_group_bytes: Vec::new(),
        column_bytes: Vec::new(),
    })
}

/// Widths are averaged over every row read: a file without a column counts its
/// rows at nothing, and an unreadable one is left out.
#[test]
fn column_widths_average_over_the_rows_read() {
    let with = |rows, bytes: &[(&str, usize)]| {
        let mut footer = file(&[], rows)?;
        footer.column_bytes = bytes.iter().map(|(n, b)| (n.to_string(), *b)).collect();
        Some(footer)
    };
    let footers = [
        with(3, &[("blob", 3_000), ("id", 24)]),
        None,
        with(1, &[("id", 8)]),
    ];
    assert_eq!(
        column_bytes_per_row(&footers),
        [("blob".to_string(), 750), ("id".to_string(), 8)]
    );
    assert!(column_bytes_per_row(&[with(0, &[("id", 0)])]).is_empty());
}

/// Column sets for a directory, one slice per file.
fn cols(files: &[&[&str]]) -> Vec<Vec<String>> {
    files
        .iter()
        .map(|f| f.iter().map(|c| (*c).to_string()).collect())
        .collect()
}

/// Column names of a dataset that grew from `from` to `to` columns, the older
/// files first.
fn grew(files: usize, from: usize, to: usize) -> Vec<Vec<String>> {
    let names = |n: usize| (0..n).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let mut out: Vec<Vec<String>> = (0..files - 1).map(|_| names(from)).collect();
    out.push(names(to));
    out
}

/// The shapes a directory of one table takes. Scores are what the statistic gives
/// today; the assertion is only that each is read as one table.
#[test]
fn one_table_whatever_its_files_did_over_time() {
    for (what, files) in [
        (
            "identical part files",
            cols(&[&["a", "b", "c"], &["a", "b", "c"], &["a", "b", "c"]]),
        ),
        (
            "a column only one file has",
            cols(&[&["id"], &["id", "oops"], &["id"]]),
        ),
        (
            "a column that starts",
            cols(&[&["id", "ts"], &["id", "ts"], &["id", "ts", "fee"]]),
        ),
        (
            "a column that stops",
            cols(&[&["id", "ts", "fee"], &["id", "ts"], &["id", "ts"]]),
        ),
        (
            "a file truncated to one column",
            cols(&[&["a", "b", "c", "d"], &["a"], &["a", "b", "c", "d"]]),
        ),
        ("five columns grown to fifty", grew(10, 5, 50)),
        ("one file", cols(&[&["a", "b"]])),
        ("no files", Vec::new()),
    ] {
        assert!(is_nested(&files), "{what} should read as one table");
    }
}

/// Directories that a union would read correctly, and that this rule turns away
/// anyway.
///
/// Two files that each bring a column the other lacks are not a dataset that grew:
/// nothing datui can see separates a rename from two tables that happen to share most
/// of their columns. The scorer that came before this took them as one table, and
/// took a directory of six unrelated tables as one table too, because no statistic
/// over column overlap can tell the two apart.
///
/// Turning them away is cheap by design. The row goes inside instead of opening,
/// and the first row in there opens the union anyway — one keystroke, not a wall.
#[test]
fn a_column_each_way_is_not_nesting_and_costs_a_keystroke() {
    for (what, files) in [
        (
            "one column each way",
            cols(&[&["a", "b", "c", "d", "e"], &["a", "b", "c", "d", "f"]]),
        ),
        (
            "a column renamed",
            cols(&[&["id", "ts", "amount"], &["id", "ts", "amt"]]),
        ),
    ] {
        assert!(
            !is_nested(&files),
            "{what} brings a column the widest file cannot account for"
        );
    }
}

/// Directories that hold separate tables, including ones that share columns.
#[test]
fn separate_tables_are_not_one_table() {
    for (what, files) in [
        ("two tables", cols(&[&["a", "b", "c"], &["x", "y", "z"]])),
        (
            "tables sharing a key",
            cols(&[&["id", "a", "b"], &["id", "x", "y"], &["id", "p", "q"]]),
        ),
        (
            // A season of Formula 1 as six tables in one directory, the columns read
            // from the footers of gs://pitscope-prod-data/jolpica/1950/. Every one
            // carries `season`, and two of them most of a race's identity, so this
            // is the shape a rule that only asks whether columns are shared calls
            // one table.
            "a season of six tables",
            cols(&[
                &[
                    "season",
                    "circuit_id",
                    "url",
                    "circuit_name",
                    "lat",
                    "lng",
                    "locality",
                    "country",
                ],
                &["season", "constructor_id", "url", "name", "nationality"],
                &[
                    "season",
                    "round",
                    "driver_id",
                    "position",
                    "points",
                    "wins",
                    "constructor_id",
                ],
                &[
                    "season",
                    "driver_id",
                    "permanent_number",
                    "code",
                    "url",
                    "given_name",
                    "family_name",
                    "date_of_birth",
                    "nationality",
                ],
                &[
                    "season",
                    "round",
                    "race_name",
                    "circuit_id",
                    "race_date",
                    "driver_id",
                    "constructor_id",
                    "number",
                    "grid",
                    "position",
                    "position_text",
                    "points",
                    "laps",
                    "status",
                    "time_millis",
                    "time_text",
                    "fastest_lap_rank",
                    "fastest_lap_number",
                    "fastest_lap_time",
                    "fastest_lap_avg_speed",
                ],
                &[
                    "season",
                    "round",
                    "race_name",
                    "circuit_id",
                    "circuit_name",
                    "locality",
                    "country",
                    "lat",
                    "lng",
                    "date",
                    "time",
                    "qualifying_date",
                    "qualifying_time",
                    "sprint_date",
                    "sprint_time",
                    "sprint_shootout_date",
                    "sprint_shootout_time",
                    "url",
                ],
            ]),
        ),
    ] {
        assert!(!is_nested(&files), "{what} should be separate tables");
    }
}

/// Growth is the shape this rule is built around, and the one the scorer before it
/// could not hold on to: a dataset grown from five columns to fifty, with one
/// dropped along the way, scored 0.196 — below the 0.200 of six unrelated tables
/// sharing a key. Asked as nesting, the same two directories are not close.
#[test]
fn growth_nests_where_unrelated_tables_do_not() {
    assert!(is_nested(&grew(10, 5, 50)));
    let unrelated = cols(&[&["id", "a", "b"], &["id", "x", "y"], &["id", "p", "q"]]);
    assert!(!is_nested(&unrelated));
}

/// Two tables joined on a key sat exactly on the old threshold, which is what made
/// it a threshold rather than a gap. Neither file's columns are in the other's.
#[test]
fn two_tables_sharing_a_key_do_not_nest() {
    let files = cols(&[&["id", "name"], &["id", "customer_id", "amount"]]);
    assert!(!is_nested(&files));
}

/// The leaves a footer names are an encoding choice; the columns a reader sees are
/// not. A list written by parquet-mr and by Arrow must compare as the same column.
#[test]
fn a_nested_column_is_one_column_however_it_was_written() {
    let old_writer = vec![
        "id".to_string(),
        "tags.array".to_string(),
        "refs.array".to_string(),
    ];
    let new_writer = vec![
        "id".to_string(),
        "tags.list.element".to_string(),
        "refs.list.element".to_string(),
    ];
    let files = vec![
        top_level_columns(&old_writer),
        top_level_columns(&new_writer),
    ];
    assert_eq!(files[0], vec!["id", "tags", "refs"]);
    assert!(is_nested(&files), "the same three columns, written twice");
    // Without the flattening each brings two leaves the other lacks, and the
    // directory is demoted.
    assert!(!is_nested(&[old_writer, new_writer]));
}

/// A file with no columns cannot disagree: it brings nothing the widest file
/// cannot account for, which is the whole question.
#[test]
fn an_empty_file_does_not_decide_the_directory() {
    assert!(is_nested(&cols(&[&["a", "b"], &[], &["a", "b"]])));
    assert!(is_nested(&cols(&[&[], &[]])), "nothing to disagree about");
}

fn union(files: &[Option<FileFooter>]) -> DatasetSchema {
    union_file_schemas(files, SchemaOrigin::AllFooters(files.len()))
}

/// The names alone of the columns not read from file `index`.
fn omitted_names(union: &DatasetSchema, index: usize) -> Vec<String> {
    union.omitted[index]
        .iter()
        .map(|(name, _)| name.to_string())
        .collect()
}

fn names(schema: &Schema) -> Vec<String> {
    schema.iter_names().map(|n| n.to_string()).collect()
}

/// Reading a conflicting column as text shows the values the conflict was hiding.
///
/// Three files, all disagreeing on `n`: an integer, text, and a float. Whichever
/// type wins, the other two files' values are unreachable — the column is not read
/// from them at all, and their cells are `≠`. Read as text, every value is there,
/// in dataset order, spelled the way its own file stored it.
#[test]
fn a_conflicting_column_read_as_text_shows_every_file_s_values() {
    use polars::prelude::{ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
        let path = dir.path().join(name);
        let file = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
        paths.push(path.to_string_lossy().to_string());
    };
    // The integer file has the most rows, so `n` is read as an integer.
    write(
        "a.parquet",
        df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
    );
    write("b.parquet", df!("id" => &[3i64], "n" => &["x"]).unwrap());
    // Boolean, not a float: a float would widen with the integer rather than
    // conflict with it, and then there would be only one conflict to show.
    write("c.parquet", df!("id" => &[4i64], "n" => &[true]).unwrap());

    let footers: Vec<Option<FileFooter>> = vec![
        file(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
        file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
        file(&[("id", DataType::Int64), ("n", DataType::Boolean)], 1),
    ];
    let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(3));
    assert_eq!(
        dataset.schema.get("n"),
        Some(&DataType::Int64),
        "the integer file has the most rows"
    );
    let drift = ScanDrift::new(&paths, &dataset, &[3, 1, 1]).expect("the files disagree");

    // As the dataset opens: the other two files' values are not read at all.
    let plain = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &[])
        .unwrap()
        .collect()
        .unwrap();
    let n = plain.column("n").unwrap();
    assert_eq!(
        (0..n.len())
            .map(|i| n.get(i).unwrap().to_string())
            .collect::<Vec<_>>(),
        ["10", "20", "30", "null", "null"],
        "the text and boolean files hold a value, and it is not one this column \
             can carry"
    );

    // Read as text: every file's value, spelled as that file stored it.
    let as_text = [PlSmallStr::from("n")];
    let text = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(
        text.column("n").unwrap().dtype(),
        &DataType::String,
        "the column is text now"
    );
    let n = text.column("n").unwrap().str().unwrap();
    assert_eq!(
        n.iter().collect::<Vec<_>>(),
        [Some("10"), Some("20"), Some("30"), Some("x"), Some("true")],
        "and holds what each file wrote, spelled as that file's own type prints"
    );
    let ids = text.column("id").unwrap().i64().unwrap();
    assert_eq!(
        ids.into_no_null_iter().collect::<Vec<_>>(),
        [0, 1, 2, 3, 4],
        "in dataset order, with the rows still lined up against their ids"
    );
    assert_eq!(
        text.column(DRIFT_COLUMN)
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        [0, 1, 2, 3, 4],
        "and each row still knows its place in the dataset"
    );
}

/// Read as text, a date past the calendar is its stored number, as the table
/// shows it, where Polars' cast panicked and failed the whole read (#506).
#[test]
fn a_date_past_the_calendar_read_as_text_is_its_stored_number() {
    use polars::prelude::{NamedFrom, ParquetWriter, Series, TimeZone};

    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut write = |name: &str, n: Series| {
        let path = dir.path().join(name);
        let file = std::fs::File::create(&path).unwrap();
        let mut frame = polars::prelude::DataFrame::new_infer_height(vec![n.into()]).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
        paths.push(path.to_string_lossy().to_string());
    };
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let stamps = |dtype: DataType| {
        Series::new("n".into(), [0, i64::MIN + 1])
            .cast(&dtype)
            .unwrap()
    };
    let types = [
        DataType::Date,
        DataType::Datetime(TimeUnit::Milliseconds, None),
        DataType::Datetime(TimeUnit::Microseconds, paris),
    ];
    // The text file has the most rows, so `n` is text.
    write("a.parquet", Series::new("n".into(), ["x", "y", "z"]));
    write(
        "b.parquet",
        Series::new("n".into(), [0, i32::MAX])
            .cast(&types[0])
            .unwrap(),
    );
    write("c.parquet", stamps(types[1].clone()));
    write("d.parquet", stamps(types[2].clone()));

    let footers: Vec<Option<FileFooter>> = [DataType::String]
        .into_iter()
        .chain(types)
        .enumerate()
        .map(|(i, dtype)| file(&[("n", dtype)], if i == 0 { 3 } else { 2 }))
        .collect();
    let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(4));
    assert_eq!(dataset.schema.get("n"), Some(&DataType::String));
    let drift = ScanDrift::new(&paths, &dataset, &[3, 2, 2, 2]).expect("the files disagree");
    let as_text = [PlSmallStr::from("n")];
    let text = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(
        text.column("n")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("x"),
            Some("y"),
            Some("z"),
            Some("1970-01-01"),
            Some("2147483647 days since 1970-01-01"),
            Some("1970-01-01 00:00:00.000"),
            Some("-9223372036854775807 ms since 1970-01-01 UTC"),
            Some("1970-01-01 01:00:00.000000+01:00"),
            Some("-9223372036854775807 us since 1970-01-01 UTC"),
        ]
    );
}

/// A column a file simply does not have stays null when the column is read as text,
/// rather than becoming the word "null" or borrowing a neighbour's value.
#[test]
fn reading_as_text_leaves_a_file_without_the_column_alone() {
    use polars::prelude::{ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
        let path = dir.path().join(name);
        let f = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
        paths.push(path.to_string_lossy().to_string());
    };
    write(
        "a.parquet",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // No `n` at all.
    write("b.parquet", df!("id" => &[2i64]).unwrap());
    write("c.parquet", df!("id" => &[3i64], "n" => &["x"]).unwrap());

    let footers: Vec<Option<FileFooter>> = vec![
        file(&[("id", DataType::Int64), ("n", DataType::Int64)], 2),
        file(&[("id", DataType::Int64)], 1),
        file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
    ];
    let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(3));
    let drift = ScanDrift::new(&paths, &dataset, &[2, 1, 1]).expect("the files disagree");
    let as_text = [PlSmallStr::from("n")];
    let text = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(
        text.column("n")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("10"), Some("20"), None, Some("x")],
        "the file with no `n` has none to show"
    );
}

/// The types the predicate offers are exactly the types Polars will cast.
///
/// Asked of Polars rather than remembered: the list of what casts to a string is
/// Polars' to change, and a predicate that drifts from it either hides a column
/// that would have read fine or offers one whose cast fails the whole scan. Each
/// case carries a real value, because an all-null column casts from anything.
#[test]
fn types_the_cast_agrees_with_are_exactly_the_ones_offered() {
    use polars::prelude::*;

    let mk = |dtype: DataType| -> Column {
        Series::new("x".into(), [1i64, 2])
            .cast(&dtype)
            .unwrap_or_else(|e| panic!("cannot build a {dtype:?} column: {e}"))
            .into()
    };
    let mut cases: Vec<(DataType, Column)> = vec![
        DataType::Int64,
        DataType::Float64,
        DataType::Boolean,
        DataType::Date,
        DataType::Time,
        DataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Duration(TimeUnit::Milliseconds),
        DataType::Decimal(10, 2),
        DataType::List(Box::new(DataType::Int64)),
    ]
    .into_iter()
    .map(|dtype| (dtype.clone(), mk(dtype)))
    .collect();
    cases.push((DataType::String, Series::new("x".into(), ["a", "b"]).into()));
    // Bytes that are not text, which is most of why a column is binary.
    cases.push((
        DataType::Binary,
        Series::new("x".into(), [&[0xffu8, 0xfe][..], &[0x41][..]]).into(),
    ));
    let plain =
        StructChunked::from_series("x".into(), 2, [Series::new("a".into(), [1i64, 2])].iter())
            .unwrap()
            .into_series();
    cases.push((plain.dtype().clone(), plain.into()));
    // A struct prints its fields itself rather than casting them, so it manages
    // inner types that a column of that type could not.
    let inners: [Series; 3] = [
        Series::new("a".into(), [1i64, 2])
            .cast(&DataType::Duration(TimeUnit::Milliseconds))
            .unwrap(),
        Series::new("a".into(), [1i64, 2])
            .cast(&DataType::List(Box::new(DataType::Int64)))
            .unwrap(),
        // The same bytes the bare binary case is refused for.
        Series::new("a".into(), [&[0xffu8, 0xfe][..], &[0x41][..]]),
    ];
    for inner in inners {
        let nested = StructChunked::from_series("x".into(), 2, [inner].iter())
            .unwrap()
            .into_series();
        cases.push((nested.dtype().clone(), nested.into()));
    }

    for (dtype, column) in cases {
        let cast_works = DataFrame::new(2, vec![column])
            .unwrap()
            .lazy()
            .select([col("x").cast(DataType::String)])
            .collect()
            .is_ok();
        assert_eq!(
            can_read_as_text(&dtype),
            cast_works,
            "{dtype:?}: the predicate and the cast must agree"
        );
    }
}

/// Asking for a column the cast would refuse leaves it as it was, rather than
/// failing the read of every file including the ones that agreed.
#[test]
fn a_column_the_cast_refuses_is_read_as_it_was() {
    use polars::prelude::{ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
        let path = dir.path().join(name);
        let f = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
        paths.push(path.to_string_lossy().to_string());
    };
    // Bytes that are not text in one file, text in the other.
    write(
        "a.parquet",
        df!("id" => &[0i64, 1], "n" => &[&[0xffu8, 0xfe][..], &[0x41][..]]).unwrap(),
    );
    write("b.parquet", df!("id" => &[2i64], "n" => &["x"]).unwrap());

    let footers: Vec<Option<FileFooter>> = vec![
        file(&[("id", DataType::Int64), ("n", DataType::Binary)], 2),
        file(&[("id", DataType::Int64), ("n", DataType::String)], 1),
    ];
    let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(2));
    let drifting = dataset
        .columns
        .iter()
        .find(|column| column.name == "n")
        .unwrap();
    assert!(
        !drifting.can_read_as_text(),
        "so the Notes tab never offers it"
    );

    let drift = ScanDrift::new(&paths, &dataset, &[2, 1]).expect("the files disagree");
    let as_text = [PlSmallStr::from("n")];
    let frame = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
        .unwrap()
        .collect()
        .expect("the read still succeeds, which is the point");
    assert_eq!(
        frame
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        [0, 1, 2],
        "every file is still read, the agreeing one included"
    );
    assert_ne!(
        frame.column("n").unwrap().dtype(),
        &DataType::String,
        "and the column is as it was, not half-cast"
    );
}

/// The type that rules a column out can be one only a *conflicting* file holds.
///
/// The column here is read as an integer, which casts to text perfectly well. It is
/// the one file storing it as a list that makes the offer impossible — and that
/// file's cast is the one that would fail, taking the read of the other three with
/// it. So the answer has to come from every type any file holds, not from the type
/// the column is read as.
#[test]
fn a_type_only_one_file_holds_can_rule_the_column_out() {
    use polars::prelude::{IntoLazy, ParquetWriter, col, df};

    let dir = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut write = |name: &str, mut frame: polars::prelude::DataFrame| {
        let path = dir.path().join(name);
        let f = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(f).finish(&mut frame).unwrap();
        paths.push(path.to_string_lossy().to_string());
    };
    write(
        "a.parquet",
        df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
    );
    // `n` as a list here: grouped so the column really is List(Int64) on disk.
    write(
        "b.parquet",
        df!("id" => &[3i64], "n" => &[9i64])
            .unwrap()
            .lazy()
            .group_by([col("id")])
            .agg([col("n")])
            .collect()
            .unwrap(),
    );

    let footers: Vec<Option<FileFooter>> = vec![
        file(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
        file(
            &[
                ("id", DataType::Int64),
                ("n", DataType::List(Box::new(DataType::Int64))),
            ],
            1,
        ),
    ];
    let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(2));
    assert_eq!(
        dataset.schema.get("n"),
        Some(&DataType::Int64),
        "read as the integer the three rows have"
    );
    let drifting = dataset
        .columns
        .iter()
        .find(|column| column.name == "n")
        .unwrap();
    assert!(
        can_read_as_text(&drifting.dtype),
        "an integer column casts to text on its own account"
    );
    assert!(
        !drifting.can_read_as_text(),
        "but one file holds a list, and that file's cast is the one that fails"
    );

    let drift = ScanDrift::new(&paths, &dataset, &[3, 1]).expect("the files disagree");
    let as_text = [PlSmallStr::from("n")];
    let frame = lenient_scan(&paths, dataset.schema.clone(), None, Some(&drift), &as_text)
        .unwrap()
        .collect()
        .expect("asking anyway must not cost the read");
    assert_eq!(
        frame
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        [0, 1, 2, 3],
        "every file is read, the three that agreed included"
    );
    assert_eq!(
        frame.column("n").unwrap().dtype(),
        &DataType::Int64,
        "and the column is as it was"
    );
}

/// `text_schema` spells the named columns as text and moves nothing.
#[test]
fn text_schema_respells_without_reordering() {
    let mut schema = Schema::with_capacity(3);
    schema.with_column("a".into(), DataType::Int64);
    schema.with_column("n".into(), DataType::Int64);
    schema.with_column("z".into(), DataType::Float64);
    let schema = Arc::new(schema);

    let text = text_schema(&schema, &[PlSmallStr::from("n")]);
    assert_eq!(
        names(&text),
        ["a", "n", "z"],
        "a column read differently does not move"
    );
    assert_eq!(text.get("n"), Some(&DataType::String));
    assert_eq!(
        text.get("a"),
        Some(&DataType::Int64),
        "nor do its neighbours change"
    );
    assert_eq!(text.get("z"), Some(&DataType::Float64));

    assert!(
        Arc::ptr_eq(&schema, &text_schema(&schema, &[])),
        "asking for nothing is the schema itself"
    );
    assert_eq!(
        names(&text_schema(&schema, &[PlSmallStr::from("ghost")])),
        ["a", "n", "z"],
        "a name the schema does not have adds nothing"
    );
}

/// A sampled dataset counts against the footers it read, not against every file.
///
/// `union_sampled` spreads the per-file findings back across the whole list, and it
/// is tempting to spread the totals with them. It must not: datui opened a few
/// thousand footers out of a few hundred thousand files, and every count it states
/// — the denominator the notes divide by, how many files hold no rows — is a count
/// of what it opened. A total over the full list would be a claim about files it
/// never looked at.
#[test]
fn a_sampled_dataset_counts_what_it_read_and_not_what_it_did_not() {
    let footers = vec![
        file(&[("id", DataType::Int64)], 0),
        file(&[("id", DataType::Int64), ("x", DataType::String)], 5),
        None,
    ];
    // Three footers read, spread across five hundred files.
    let read = [0usize, 250, 499];
    let union = union_sampled(500, &read, &footers);

    assert_eq!(
        union.files, 3,
        "the population is the footers read, not the files there are"
    );
    assert_eq!(union.empty_files, 1, "one of the three held nothing");
    assert_eq!(
        union.origin,
        SchemaOrigin::FooterSample {
            read: 3,
            total: 500
        }
    );
    assert_eq!(
        union.unreadable,
        [499],
        "and the footer that would not parse is named by its place among the files"
    );
    // The per-file findings, though, are spread to the full length: the scan
    // indexes them by file, and it reads all five hundred.
    assert_eq!(union.file_group.len(), 500);
    assert_eq!(union.omitted.len(), 500);
}

/// The row-group note fires on the middle size, and only past the threshold.
///
/// Sizes rather than schemas, so it does not go through `Shape`: what decides this
/// note is a list of numbers, and the interesting cases are all about which number
/// the middle is.
#[test]
fn row_groups_are_noted_by_their_middle_size_and_only_when_it_is_large() {
    const MIB: usize = 1024 * 1024;
    let note = |groups: &[&[usize]]| -> Option<String> {
        let files: Vec<Option<FileFooter>> = groups
            .iter()
            .map(|sizes| {
                Some(FileFooter {
                    schema: Arc::new(Schema::with_capacity(0)),
                    row_group_rows: vec![1],
                    file_bytes: 0,
                    row_group_bytes: sizes.to_vec(),
                    column_bytes: Vec::new(),
                })
            })
            .collect();
        let dataset = union_file_schemas(&files, SchemaOrigin::AllFooters(files.len()));
        crate::notes::from_dataset(&dataset)
            .into_iter()
            .find(|note| note.summary.starts_with("median row group"))
            .map(|note| note.summary)
    };

    assert_eq!(note(&[&[MIB], &[2 * MIB]]), None, "ordinary row groups");
    assert_eq!(
        note(&[&[64 * MIB]]),
        None,
        "the threshold itself is not past it"
    );
    assert_eq!(
        note(&[&[65 * MIB]]).as_deref(),
        Some("median row group 65.0 MiB, each read whole"),
    );
    assert_eq!(
        note(&[&[MIB, MIB, 4096 * MIB]]),
        None,
        "one huge row group among small ones does not describe the dataset"
    );
    assert_eq!(
        note(&[&[100 * MIB, 100 * MIB], &[MIB]]).as_deref(),
        Some("median row group 100.0 MiB, each read whole"),
        "the middle of every row group of every file, not the middle of the files"
    );
    assert_eq!(
        note(&[&[MIB], &[100 * MIB, 100 * MIB]]).as_deref(),
        Some("median row group 100.0 MiB, each read whole"),
        "including when the large ones are not in the first file"
    );
    // Row groups arrive in file order, which is no order at all by size: a middle
    // partition rewritten by another job puts a big one between two small ones.
    assert_eq!(
        note(&[&[100 * MIB], &[MIB], &[100 * MIB]]).as_deref(),
        Some("median row group 100.0 MiB, each read whole"),
        "and when they arrive out of order"
    );
    assert_eq!(
        note(&[&[MIB], &[100 * MIB], &[MIB]]),
        None,
        "which cuts both ways: one big group between two small ones is not the middle"
    );
    assert_eq!(note(&[&[]]), None, "a file with no row groups says nothing");
    // An even count takes the lower of the middle two, which is the reading that
    // errs towards saying nothing.
    assert_eq!(
        note(&[&[64 * MIB, 65 * MIB]]),
        None,
        "two row groups either side of the line: the lower one decides"
    );
    assert_eq!(
        note(&[&[65 * MIB, 66 * MIB]]).as_deref(),
        Some("median row group 65.0 MiB, each read whole"),
        "and when it decides the other way it is still the lower one"
    );
}

/// The sizes come off a real Parquet footer, and they are the compressed ones.
///
/// Compressed, because that is what crosses the wire; the other number the footer
/// offers is the size once decoded. Telling them apart takes data that does not
/// compress to nothing: twenty thousand distinct strings compress to about 30 KiB
/// from about 4 MiB decoded, where a column of one repeated integer goes the other
/// way — the dictionary makes the decoded figure the *smaller* of the two, and a
/// test built on that pins nothing.
#[test]
fn a_real_footer_reports_the_compressed_size_of_each_row_group() {
    use polars::prelude::{ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<String> = (0..20_000)
        .map(|i| format!("{i:0>6}{}", "abcdefghij".repeat(19)))
        .collect();
    let mut frame = df!("s" => rows).unwrap();
    let file = std::fs::File::create(dir.path().join("wide.parquet")).unwrap();
    ParquetWriter::new(file)
        .with_row_group_size(Some(20_000))
        .finish(&mut frame)
        .unwrap();

    let footer = crate::dataset_files::local_footer(&dir.path().join("wide.parquet"))
        .expect("the footer reads");
    assert_eq!(footer.rows(), 20_000);
    assert_eq!(footer.row_group_bytes.len(), 1, "one row group");

    // The file's own size comes from the same read, and is the size on disk: the
    // compressed row group plus the footer and header around it, so larger than
    // the group and far smaller than the decoded data.
    let on_disk = std::fs::metadata(dir.path().join("wide.parquet"))
        .unwrap()
        .len();
    assert_eq!(
        footer.file_bytes as u64, on_disk,
        "the file's size, as the filesystem reports it"
    );

    let size = footer.row_group_bytes[0];
    assert!(size > 0, "a size is reported");
    assert!(
        size < 1_000_000,
        "and it is the compressed size: 20,000 distinct strings of 200 characters \
             are about 4 MiB decoded and a small fraction of that on disk, so {size} \
             bytes is the decoded figure"
    );
}

/// Many small files is two conditions, and both have to hold.
#[test]
fn many_files_are_noted_only_when_they_are_also_small() {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;
    // `sizes` is the shape of the footers read, repeated to fill `read` of them:
    // the note says how many were read, so the fixture has to have that many.
    let note = |files: usize, read: usize, sizes: &[usize]| -> Option<String> {
        let footers: Vec<Option<FileFooter>> = sizes
            .iter()
            .cycle()
            .take(if sizes.is_empty() { 0 } else { read })
            .map(|bytes| {
                Some(FileFooter {
                    schema: Arc::new(Schema::with_capacity(0)),
                    row_group_rows: vec![1],
                    file_bytes: *bytes,
                    row_group_bytes: Vec::new(),
                    column_bytes: Vec::new(),
                })
            })
            .collect();
        let origin = if read == files {
            SchemaOrigin::AllFooters(files)
        } else {
            SchemaOrigin::FooterSample { read, total: files }
        };
        crate::notes::from_dataset(&union_file_schemas(&footers, origin))
            .into_iter()
            .find(|note| note.summary.contains("files, median"))
            .map(|note| note.summary)
    };

    assert_eq!(
        note(10_000, 10_000, &[40 * KIB]),
        None,
        "a year of hourly partitions, and more, is an ordinary shape"
    );
    assert_eq!(
        note(10_001, 10_001, &[40 * KIB]).as_deref(),
        Some("10,001 files, median 40.0 KiB; every footer read before any row"),
        "one more is not"
    );
    assert_eq!(
        note(50_000, 50_000, &[MIB]),
        None,
        "a megabyte is not small by this measure"
    );
    assert!(
        note(50_000, 50_000, &[MIB - 1]).is_some(),
        "a byte under it is"
    );
    assert_eq!(
        note(50_000, 50_000, &[40 * KIB, 40 * KIB, 900 * MIB]).as_deref(),
        Some("50,000 files, median 40.0 KiB; every footer read before any row"),
        "a large minority does not move the middle"
    );
    // Sampled: the count is every file the listing found, the middle size is over
    // the footers datui opened, and the sentence names both rather than leaving
    // the middle to read as a fact about all of them.
    assert_eq!(
        note(500_000, 2, &[40 * KIB, 40 * KIB]).as_deref(),
        Some("500,000 files, median 40.0 KiB; 2 footers read before any row"),
        "the count is the listing's; the footers read are their own number"
    );
    assert_eq!(note(50_000, 0, &[]), None, "no footer read, nothing to say");

    // The scope line under a sampled dataset says what was looked at, which is what
    // stops the middle size reading as a fact about half a million files.
    let sampled = union_file_schemas(
        &[
            Some(FileFooter {
                schema: Arc::new(Schema::with_capacity(0)),
                row_group_rows: vec![1],
                file_bytes: 40 * KIB,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            }),
            Some(FileFooter {
                schema: Arc::new(Schema::with_capacity(0)),
                row_group_rows: vec![1],
                file_bytes: 40 * KIB,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            }),
        ],
        SchemaOrigin::FooterSample {
            read: 2,
            total: 500_000,
        },
    );
    let sampled_note = crate::notes::from_dataset(&sampled)
        .into_iter()
        .find(|note| note.summary.contains("files, median"))
        .expect("the note is made");
    assert_eq!(sampled_note.scope, "in 2 of 500,000 footers (sample)");
    assert_eq!(
        note(50_000, 2, &[0, 0]),
        None,
        "and a size of nothing means the size is not known, not that it is small"
    );
}

/// The keys a path partitions by: a set, sorted, with the file's own name never
/// among them.
///
/// A set because hive columns are matched by name — `y=1/m=1` and `m=2/y=2`
/// partition by the same two things, and a dataset that mixes the two orders reads
/// perfectly well. Sorted so the two spell alike, and deduplicated so a tree that
/// repeats a key is one thing rather than two.
#[test]
fn partition_keys_are_the_key_equals_segments_above_the_file() {
    let keys = |path: &str| partition_keys_of(path);
    assert_eq!(keys("data/date=2024-01-01/a.parquet"), ["date"]);
    assert_eq!(keys("data/y=2024/m=05/a.parquet"), ["m", "y"]);
    assert_eq!(
        keys("data/m=05/y=2024/a.parquet"),
        keys("data/y=2024/m=05/a.parquet"),
        "the same two partitions, written in two orders"
    );
    assert_eq!(
        keys("data/x=1/x=2/a.parquet"),
        ["x"],
        "and a key repeated down the tree is one key"
    );
    assert_eq!(keys("data/a.parquet"), Vec::<String>::new());
    assert_eq!(
        keys("data/x=1/2024=05.parquet"),
        ["x"],
        "the file's own name is not a partition, whatever it looks like"
    );
    assert_eq!(
        keys("data/=2024/a.parquet"),
        Vec::<String>::new(),
        "nor is a segment with nothing before the equals"
    );
    // A backslash separates on Windows and is an ordinary character in a Linux
    // file name. Splitting on it everywhere would break a legitimate name and
    // invent a layout difference out of one directory, which would fire this note
    // on a dataset whose directories agree perfectly.
    #[cfg(windows)]
    assert_eq!(
        keys(r"data\date=2024-01-01\a.parquet"),
        ["date"],
        "a path written the other way round is the same path"
    );
    #[cfg(not(windows))]
    assert_eq!(
        keys(r"data/we\ird=1/f.parquet"),
        ["we\\ird"],
        "a backslash here is part of the name, not a separator"
    );
    #[cfg(not(windows))]
    assert_eq!(
        keys(r"data/x=1\y=2/f.parquet"),
        ["x"],
        "so one directory is one partition, however it is spelled"
    );
}

/// A dataset whose directories do not all partition by the same keys.
///
/// The note says the shape and claims nothing about what it costs: a rename that
/// stops the dataset opening, and one stray unpartitioned file that turns hive
/// reading off and leaves the same directories readable, look identical from here.
#[test]
fn directories_that_partition_differently_are_counted_each_way() {
    let note = |root: &str, paths: &[&str]| -> Option<crate::notes::Note> {
        let footers = vec![
            Some(FileFooter {
                schema: Arc::new(Schema::with_capacity(0)),
                row_group_rows: vec![1],
                file_bytes: 1,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            });
            paths.len()
        ];
        let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(paths.len()))
            .with_partition_layouts(root, &owned);
        crate::notes::from_dataset(&dataset)
            .into_iter()
            .find(|note| note.summary.contains("mixed partition keys"))
    };

    assert_eq!(
        note("d", &["d/date=1/a.parquet", "d/date=2/b.parquet"]),
        None,
        "directories that agree have nothing to say"
    );
    assert_eq!(
        note("d", &["d/a.parquet", "d/b.parquet"]),
        None,
        "nor has a dataset with no partitions at all"
    );
    assert_eq!(
        note("d", &["d/y=1/m=1/a.parquet", "d/m=2/y=2/b.parquet"]),
        None,
        "nor two orders of the same two keys: hive matches columns by name, so \
             that dataset reads perfectly well and has nothing in dispute"
    );
    assert_eq!(
        note(
            "d/run=7",
            &["d/run=7/loose.parquet", "d/run=7/date=1/a.parquet"]
        ),
        None,
        "a key=value directory above the dataset as it was opened is not one of the \
             things its directories disagree about — and these two files are where that \
             matters, since counting `run` would make the one without a key of its \
             own a second layout"
    );
    assert_eq!(
        note(
            "s3://b//data/",
            &["s3://b/data/date=1/a.parquet", "s3://b/data/dt=2/b.parquet"]
        ),
        None,
        "and a path the root is not a prefix of — a typed URL with a doubled \
             slash rebuilds without it — is one this cannot place, so it is left out \
             rather than read from the top"
    );

    let renamed = note(
        "d",
        &[
            "d/date=1/a.parquet",
            "d/date=2/b.parquet",
            "d/date=3/c.parquet",
            "d/dt=4/e.parquet",
        ],
    )
    .expect("the directories disagree");
    assert_eq!(
        renamed.summary,
        "mixed partition keys: 3 files by date, \
             1 file by dt"
    );
    assert_eq!(
        renamed.scope, "in the names of 4 files",
        "read off every name, not off the footers datui opened"
    );

    // A file with no partition at all has no keys to disagree about, so it is no
    // layout — but it is still a name that was read, and the scope counts it.
    let loose = note(
        "d",
        &[
            "d/y=1/m=1/a.parquet",
            "d/y=1/m=2/b.parquet",
            "d/date=3/c.parquet",
            "d/loose.parquet",
        ],
    )
    .expect("the directories disagree");
    assert_eq!(
        loose.summary,
        "mixed partition keys: 2 files by m/y, \
             1 file by date"
    );
    assert_eq!(
        loose.scope, "in the names of 4 files",
        "the unpartitioned file is one of the names read"
    );
}

/// The commonest layout is named first, and past two the rest are counted.
#[test]
fn the_layouts_a_note_names_are_the_commonest_of_them() {
    let layouts = |paths: &[&str]| -> Vec<(Vec<String>, usize)> {
        let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
        union_file_schemas(&[], SchemaOrigin::AllFooters(0))
            .with_partition_layouts("d", &owned)
            .partition_layouts
    };
    let note = |paths: &[&str]| -> String {
        let footers = vec![
            Some(FileFooter {
                schema: Arc::new(Schema::with_capacity(0)),
                row_group_rows: vec![1],
                file_bytes: 1,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            });
            paths.len()
        ];
        let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
        let dataset = union_file_schemas(&footers, SchemaOrigin::AllFooters(paths.len()))
            .with_partition_layouts("d", &owned);
        crate::notes::from_dataset(&dataset)
            .into_iter()
            .find(|note| note.summary.contains("mixed partition keys"))
            .expect("the directories disagree")
            .summary
    };

    // Asserted on the layouts themselves, not on the note: a HashMap hands them
    // back in no order at all, so a note that happened to read correctly would
    // leave the ordering untested nine runs in ten.
    assert_eq!(
        layouts(&[
            "d/zzz=1/b.parquet",
            "d/aaa=1/a.parquet",
            "d/zzz=2/c.parquet",
            "d/zzz=3/e.parquet",
        ]),
        vec![(vec!["zzz".to_string()], 3), (vec!["aaa".to_string()], 1)],
        "commonest first, though the rare one sorts first and arrived first"
    );
    assert_eq!(
        layouts(&[
            "d/zz=1/a.parquet",
            "d/aa=1/b.parquet",
            "d/mm=1/c.parquet",
            "d/qq=1/e.parquet"
        ]),
        vec![
            (vec!["aa".to_string()], 1),
            (vec!["mm".to_string()], 1),
            (vec!["qq".to_string()], 1),
            (vec!["zz".to_string()], 1)
        ],
        "and equally common ones by their keys, so the same dataset reads the \
             same way every time it is opened"
    );

    assert_eq!(
        note(&[
            "d/aa=1/a.parquet",
            "d/bb=1/b.parquet",
            "d/cc=1/c.parquet",
            "d/dd=1/e.parquet",
        ]),
        "mixed partition keys: 1 file by aa, \
             1 file by bb, 2 files by 2 other ways"
    );
    assert_eq!(
        note(&["d/aa=1/a.parquet", "d/bb=1/b.parquet", "d/cc=1/c.parquet"]),
        "mixed partition keys: 1 file by aa, \
             1 file by bb, 1 file by 1 other way",
        "and one of them is one way, not one ways"
    );

    // Past the layouts worth remembering, the tail is still counted in full: a
    // note that says "and 62 other ways" of a hundred would not add up against
    // its own scope line.
    let many: Vec<String> = (0..100)
        .map(|i| format!("d/k{i:0>3}=1/f.parquet"))
        .collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert_eq!(
        note(&many),
        "mixed partition keys: 1 file by k000, \
             1 file by k001, 98 files by 98 other ways"
    );
    let owned: Vec<String> = many.iter().map(|p| p.to_string()).collect();
    let dataset =
        union_file_schemas(&[], SchemaOrigin::AllFooters(0)).with_partition_layouts("d", &owned);
    assert!(
        dataset.partition_layouts.len() <= 64,
        "and it is not holding a hundred of them to say so: {}",
        dataset.partition_layouts.len()
    );
}

/// The counter says nothing until a pass begins, and nothing again once it ends.
///
/// Nothing-when-done is the half that matters: a count left on screen after the
/// footers have landed is a wait the user is not actually having.
#[test]
fn the_footer_count_speaks_only_while_a_pass_is_running() {
    let progress = FooterProgress::default();
    assert_eq!(progress.reading(), None, "nothing has begun");

    progress.begin(3);
    assert_eq!(progress.reading(), Some((0, 3)), "none read yet");
    progress.advance();
    progress.advance();
    assert_eq!(progress.reading(), Some((2, 3)));

    progress.done();
    assert_eq!(progress.reading(), None, "and nothing once it has landed");

    // A second pass starts from nothing rather than from the first one's count.
    progress.begin(2);
    assert_eq!(progress.reading(), Some((0, 2)));
}

/// More advances than footers cannot make the count overtake the total.
///
/// No caller can reach it today: a pass is begun before its readers are spawned
/// and is over before the next one begins, and every open takes a counter of its
/// own. The clamp is for the wiring that comes after this one — "reading 4 of 3
/// footers" is the sort of nonsense that makes a user distrust the rest of the
/// screen. It does mean a future miswiring shows as a count stopped at N of N
/// rather than as an obvious absurdity, which is the price of not showing the
/// absurdity.
#[test]
fn the_footer_count_never_passes_its_total() {
    let progress = FooterProgress::default();
    progress.begin(2);
    for _ in 0..5 {
        progress.advance();
    }
    assert_eq!(progress.reading(), Some((2, 2)));
}

/// A pass that panics still says it has finished.
///
/// The counter outlives the pass — the render holds it — so a pass that stopped
/// without saying so would leave a count on screen for as long as anyone looked,
/// which is the one state this feature exists to prevent.
#[test]
fn a_pass_that_panics_still_says_it_has_finished() {
    let progress = FooterProgress::default();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let pass = progress.pass(3);
        pass.advance();
        panic!("a footer reader gave up");
    }));
    assert!(caught.is_err(), "the panic happened");
    assert_eq!(
        progress.reading(),
        None,
        "and the count went with it rather than sitting there"
    );
    assert_eq!(
        progress.last_pass().read,
        1,
        "with what it managed still readable"
    );
}

/// Files merely missing a column must not split the scan.
///
/// Splitting is only needed to leave a column out of a file that holds it in
/// another type. When a column is simply absent the read is already lenient, so a
/// dataset whose files alternate between having it and not — the worst case for
/// run-splitting — must still be one scan.
#[test]
fn absent_columns_alone_never_split_the_scan() {
    let files = 64;
    let per_file: Vec<Option<FileFooter>> = (0..files)
        .map(|i| {
            let mut s = Schema::with_capacity(2);
            s.with_column("id".into(), DataType::Int64);
            if i % 2 == 1 {
                s.with_column("extra".into(), DataType::String);
            }
            Some(FileFooter {
                schema: Arc::new(s),
                row_group_rows: vec![1],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            })
        })
        .collect();
    let paths: Vec<String> = (0..files).map(|i| format!("part-{i:05}.parquet")).collect();
    let read: Vec<usize> = (0..files).collect();
    let dataset = union_sampled(files, &read, &per_file);
    let rows = vec![1usize; files];
    let drift = ScanDrift::new(&paths, &dataset, &rows).expect("this dataset drifts");
    assert!(dataset.drifts());
    assert_eq!(
        runs_of(&paths, &drift),
        1,
        "absent columns need no split, however they alternate"
    );

    // A type conflict does need one, and only around the files that have it.
    let mut with_conflict = per_file.clone();
    let mut odd = Schema::with_capacity(2);
    odd.with_column("id".into(), DataType::String);
    with_conflict[7] = Some(FileFooter {
        schema: Arc::new(odd),
        row_group_rows: vec![1],
        file_bytes: 0,
        row_group_bytes: Vec::new(),
        column_bytes: Vec::new(),
    });
    let dataset = union_sampled(files, &read, &with_conflict);
    let drift = ScanDrift::new(&paths, &dataset, &rows).unwrap();
    assert_eq!(runs_of(&paths, &drift), 3, "before it, it, and after it");
}

/// How many separate scans `lenient_scan` would build for these paths.
fn runs_of(paths: &[String], drift: &ScanDrift) -> usize {
    let mut runs = 1;
    for pair in paths.windows(2) {
        if drift.unread(&pair[0]) != drift.unread(&pair[1]) {
            runs += 1;
        }
    }
    runs
}

/// Partition values compared the way a reader compares them.
#[test]
fn a_reader_puts_part_2_before_part_10() {
    use std::cmp::Ordering;
    let cmp = |a: &str, b: &str| natural_cmp(a, b);
    assert_eq!(
        cmp("part=2", "part=10"),
        Ordering::Less,
        "which bytes do not"
    );
    assert_eq!(cmp("part=10", "part=2"), Ordering::Greater);
    assert_eq!(cmp("date=2024-01-02", "date=2024-01-03"), Ordering::Less);
    assert_eq!(cmp("date=2024-01-02", "date=2024-01-02"), Ordering::Equal);
    assert_eq!(
        cmp("m=03", "m=3"),
        Ordering::Equal,
        "the same number written two ways is neither before nor after itself — a \
             dataset that spells one month both ways is past helping, and this at \
             least does not invent an order for it"
    );
    assert_eq!(cmp("a=1/b=2", "a=1/b=10"), Ordering::Less);
    assert_eq!(
        cmp("x=a", "x=b"),
        Ordering::Less,
        "and letters are still letters"
    );
}

/// A partition path holds another when the second is inside it.
#[test]
fn a_partition_holds_the_ones_below_it() {
    assert!(partition_holds("y=2024", "y=2024/m=03"));
    assert!(partition_holds("y=2024", "y=2024"));
    assert!(!partition_holds("y=2024", "y=2025"));
    assert!(
        !partition_holds("y=202", "y=2024"),
        "a prefix of the spelling is not a directory above it"
    );
    assert!(!partition_holds("y=2024/m=03", "y=2024"));
}

/// The two things the doc promises about what counts as a partition.
///
/// Reachable only through `with_partition_layouts` otherwise, where every fixture
/// path is `root/key=value/file.parquet` — which exercises neither: no file name
/// there holds an `=`, and no segment lacks a key. Both guards could be deleted
/// with the whole suite green.
#[test]
fn a_file_name_is_not_a_partition_and_neither_is_a_bare_segment() {
    assert_eq!(partition_values_of("/x=1/f.parquet"), ["x=1"]);
    assert_eq!(
        partition_values_of("/x=1/2024=05.parquet"),
        ["x=1"],
        "the file's own name is never a partition, whatever it is called"
    );
    assert_eq!(
        partition_values_of("/raw/x=1/f.parquet"),
        ["x=1"],
        "and a segment with no key before the `=` is not one either"
    );
    assert_eq!(
        partition_values_of("/=1/f.parquet"),
        Vec::<String>::new(),
        "an empty key is no key"
    );
    assert_eq!(
        partition_values_of("/y=2024/m=03/f.parquet"),
        ["y=2024", "m=03"],
        "in the order written, because a partition is a place"
    );
    assert_eq!(
        partition_values_of("/date=2024=05/f.parquet"),
        ["date=2024=05"],
        "and a value may hold an `=` of its own"
    );
}

#[test]
fn a_column_only_a_middle_file_has_is_kept() {
    let files = [
        file(&[("id", DataType::Int64)], 10),
        file(&[("id", DataType::Int64), ("oops", DataType::String)], 10),
        file(&[("id", DataType::Int64)], 10),
    ];
    let union = union(&files);
    assert_eq!(names(&union.schema), ["id", "oops"]);
    let oops = union.columns.iter().find(|c| c.name == "oops").unwrap();
    assert_eq!(oops.present_in, 1);
}

#[test]
fn the_newest_files_order_leads_and_older_columns_follow() {
    let files = [
        file(&[("a", DataType::Int64), ("gone", DataType::Int64)], 1),
        file(&[("b", DataType::Int64), ("a", DataType::Int64)], 1),
    ];
    assert_eq!(names(&union(&files).schema), ["b", "a", "gone"]);
}

#[test]
fn integer_widths_widen_losslessly() {
    let files = [
        file(&[("n", DataType::Int32)], 100),
        file(&[("n", DataType::Int64)], 1),
    ];
    let union = union(&files);
    assert_eq!(union.schema.get("n"), Some(&DataType::Int64));
    assert!(union.columns[0].widened);
    assert_eq!(union.columns[0].conflicting_files, 0);
    assert!(union.omitted.iter().all(|o| o.is_empty()));
}

#[test]
fn an_integer_and_a_float_meet_at_float64() {
    let files = [
        file(&[("n", DataType::Int32)], 1),
        file(&[("n", DataType::Float32)], 1),
    ];
    assert_eq!(union(&files).schema.get("n"), Some(&DataType::Float64));
}

#[test]
fn datetime_units_widen_to_the_finer_one() {
    let ms = DataType::Datetime(TimeUnit::Milliseconds, None);
    let ns = DataType::Datetime(TimeUnit::Nanoseconds, None);
    let files = [file(&[("t", ms)], 1), file(&[("t", ns.clone())], 1)];
    assert_eq!(union(&files).schema.get("t"), Some(&ns));
}

#[test]
fn a_struct_has_every_field_either_file_has() {
    let old = DataType::Struct(vec![Field::new("a".into(), DataType::Int32)]);
    let new = DataType::Struct(vec![
        Field::new("a".into(), DataType::Int64),
        Field::new("b".into(), DataType::String),
    ]);
    let files = [file(&[("s", old)], 1), file(&[("s", new.clone())], 1)];
    assert_eq!(union(&files).schema.get("s"), Some(&new));
}

#[test]
fn a_type_conflict_goes_to_the_majority_of_rows() {
    let files = [
        file(&[("price", DataType::String)], 10),
        file(&[("price", DataType::Int64)], 90),
    ];
    let union = union(&files);
    assert_eq!(union.schema.get("price"), Some(&DataType::Int64));
    assert_eq!(union.columns[0].conflicting_files, 1);
    assert_eq!(union.columns[0].conflicting_types, [DataType::String]);
    assert_eq!(omitted_names(&union, 0), ["price"]);
    assert!(union.omitted[1].is_empty());
}

#[test]
fn the_majority_can_be_the_text_files() {
    let files = [
        file(&[("price", DataType::String)], 90),
        file(&[("price", DataType::Int64)], 10),
    ];
    let union = union(&files);
    assert_eq!(union.schema.get("price"), Some(&DataType::String));
    assert_eq!(omitted_names(&union, 1), ["price"]);
}

#[test]
fn a_type_that_covers_more_files_wins_over_one_that_covers_none_extra() {
    // Float64 is in no file, but reads both numeric ones; the text file loses.
    let files = [
        file(&[("n", DataType::Int32)], 30),
        file(&[("n", DataType::Float32)], 30),
        file(&[("n", DataType::String)], 50),
    ];
    let union = union(&files);
    assert_eq!(union.schema.get("n"), Some(&DataType::Float64));
    assert_eq!(omitted_names(&union, 2), ["n"]);
}

#[test]
fn names_differing_only_by_case_stay_two_columns() {
    let files = [file(
        &[("Price", DataType::Int64), ("price", DataType::Int64)],
        1,
    )];
    assert_eq!(names(&union(&files).schema), ["Price", "price"]);
}

#[test]
fn an_unreadable_footer_is_recorded_and_left_out() {
    let files = [
        file(&[("id", DataType::Int64)], 1),
        None,
        file(&[("id", DataType::Int64), ("late", DataType::Int64)], 1),
    ];
    let union = union(&files);
    assert_eq!(union.unreadable, [1]);
    assert_eq!(names(&union.schema), ["id", "late"]);
    assert!(union.omitted[1].is_empty());
}

#[test]
fn a_column_of_nulls_takes_the_other_files_type() {
    let files = [
        file(&[("x", DataType::Null)], 1),
        file(&[("x", DataType::Int64)], 1),
    ];
    let union = union(&files);
    assert_eq!(union.schema.get("x"), Some(&DataType::Int64));
    assert_eq!(union.columns[0].conflicting_files, 0);
}

#[test]
fn unsigned_and_signed_meet_in_a_wider_signed_type() {
    assert_eq!(
        widen(&DataType::UInt32, &DataType::Int32),
        Some(DataType::Int64)
    );
    assert_eq!(widen(&DataType::UInt64, &DataType::Int64), None);
}

#[test]
fn origins_read_as_sentences() {
    assert_eq!(
        SchemaOrigin::AllFooters(6541).to_string(),
        "all 6,541 footers"
    );
    assert_eq!(
        SchemaOrigin::FooterSample {
            read: 5000,
            total: 200_000
        }
        .to_string(),
        "5,000 of 200,000 footers (sample)"
    );
}

#[test]
fn a_sample_spans_the_files_and_keeps_the_first_and_newest() {
    assert_eq!(footers_to_read(3), [0, 1, 2]);
    assert_eq!(footers_to_read(MAX_FOOTER_READS).len(), MAX_FOOTER_READS);
    let sample = footers_to_read(MAX_FOOTER_READS * 10);
    assert_eq!(sample.len(), MAX_FOOTER_READS);
    assert_eq!(sample.first(), Some(&0));
    assert_eq!(sample.last(), Some(&(MAX_FOOTER_READS * 10 - 1)));
    assert!(sample.windows(2).all(|w| w[0] < w[1]), "ascending");
}

/// The scan's cast policy has no way to read either of these into the other, so
/// they must stay conflicts and be omitted rather than widened into a type the
/// read would then fail on.
#[test]
fn types_the_scan_cannot_cast_are_not_widened() {
    let ms = DataType::Duration(TimeUnit::Milliseconds);
    let us = DataType::Duration(TimeUnit::Microseconds);
    assert_eq!(widen(&ms, &us), None);
    assert_eq!(widen(&DataType::Binary, &DataType::String), None);
    assert_eq!(widen(&DataType::Date, &ms), None);
}

#[test]
fn drifting_counts_against_the_files_read_not_the_busiest_column() {
    // No column is in both files; both are drift.
    let files = [
        file(&[("a", DataType::Int64)], 1),
        file(&[("b", DataType::Int64)], 1),
    ];
    let union = union(&files);
    let drifting: Vec<_> = union.drifting().map(|c| c.name.to_string()).collect();
    assert_eq!(drifting, ["b", "a"]);
}

#[test]
fn drifting_names_only_the_columns_worth_a_note() {
    let files = [
        file(&[("id", DataType::Int64)], 1),
        file(&[("id", DataType::Int64), ("oops", DataType::String)], 1),
    ];
    let union = union(&files);
    let drifting: Vec<_> = union.drifting().map(|c| c.name.to_string()).collect();
    assert_eq!(drifting, ["oops"]);
}

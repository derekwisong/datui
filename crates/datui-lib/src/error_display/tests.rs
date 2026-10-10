use super::*;

/// A sentence starts with a capital, unless it starts with a key or a name the
/// user wrote, which is kept as written.
#[test]
fn keys_keep_their_case() {
    for (what, said) in [
        ("not a WAV file", "Not a WAV file."),
        ("could not read it: gone", "Could not read it: gone."),
        ("type: expected u1", "type: expected u1."),
        (
            "tags.nine: expected a tag number",
            "tags.nine: expected a tag number.",
        ),
        (
            "header_rows: missing `name`",
            "header_rows: missing `name`.",
        ),
        (
            "\"day\" is made from \"date\"",
            "\"day\" is made from \"date\".",
        ),
        ("u9 is not a type", "u9 is not a type."),
        ("`colour` is not a key", "`colour` is not a key."),
    ] {
        assert_eq!(sentence(what), said, "{what}");
    }
    assert_eq!(
        located_message(
            Some(Path::new("d.toml")),
            Some((3, 1)),
            "tags.x: expected a tag number"
        ),
        "\"d.toml\":3:1: tags.x: expected a tag number."
    );
}

/// A reader's error names its file in quotes, once, in sentence case, ended.
#[test]
fn a_file_error_names_the_file_once() {
    let path = Path::new("/d/a.wav");
    for what in [
        "not a WAV file",
        "Not a WAV file.",
        "/d/a.wav: not a WAV file",
        "\"/d/a.wav\": Not a WAV file.",
    ] {
        assert_eq!(file_message(path, what), "\"/d/a.wav\": Not a WAV file.");
    }
    assert_eq!(
        file_message(path, "bad\nTry: --format csv"),
        "\"/d/a.wav\": Bad.\nTry: --format csv"
    );
    // Named once however often it passes through, and its cause is still found.
    let err = in_file(path, color_eyre::eyre::eyre!("too short"));
    let err = in_file(Path::new("/d"), err);
    assert_eq!(err.to_string(), "\"/d/a.wav\": Too short.");
    assert_eq!(
        user_message_from_report(&err, Some(Path::new("/elsewhere"))),
        "\"/d/a.wav\": Too short."
    );
    let missing = in_file(path, io::Error::new(io::ErrorKind::NotFound, "gone").into());
    assert!(matches!(
        error_for_python(&missing),
        (ErrorKindForPython::FileNotFound, ref m) if m.starts_with("\"/d/a.wav\": ")
    ));
    assert_eq!(
        user_message_from_report(&color_eyre::eyre::eyre!("bad header"), Some(path)),
        "\"/d/a.wav\": Bad header."
    );
}

/// A reader's Rust names are said in English.
#[test]
fn rust_names_are_said_plainly() {
    let said = |m: &str| rust_names_said_plainly(m);
    assert!(
        said("out-of-spec: InvalidFooter")
            .unwrap()
            .contains("(invalid footer)")
    );
    assert!(said("OutOfSpec").unwrap().contains("--format"));
    assert_eq!(
        said("InvalidUtf8 at character 0").unwrap(),
        "The file is not UTF-8 text at character 0."
    );
    assert_eq!(said("out-of-spec: the footer is short"), None);
    assert_eq!(said("bad header"), None);
}

/// An expansion that found nothing says so instead of printing Polars' input.
#[test]
fn a_pattern_that_matched_nothing_says_so() {
    let csv = "failed to retrieve file schemas (csv): expanded paths were empty \
                   (path expansion input: 'paths: [PlRefPath { inner: \"/d/x?.csv\" }]', \
                   glob: true).";
    let err = PolarsError::ComputeError(csv.into());
    assert_eq!(
        user_message_from_polars(&err),
        "No files match this pattern."
    );
    let dir = "failed to retrieve first file schema (parquet): expanded paths were \
                   empty (path expansion input: 'paths: [PlRefPath { inner: \"/d/empty\" }]', \
                   glob: true). Hint: passing a schema can allow this scan to succeed.";
    let err = PolarsError::ComputeError(dir.into());
    assert_eq!(user_message_from_polars(&err), "No files found there.");
}

/// A temp copy is named by what the user opened, wherever the message says it,
/// and a message that does not mention it is left alone.
#[test]
fn a_temp_copy_is_named_by_its_source() {
    let file = Path::new("/home/u/tmp/.tmp9tY5X2.parquet");
    let url = Path::new("http://host/broken.parquet");
    let said = named_by_source(
        "\"/home/u/tmp/.tmp9tY5X2.parquet\": Bad.\nIt stopped at /home/u/tmp/.tmp9tY5X2.parquet.",
        file,
        url,
    );
    assert_eq!(
        said,
        "\"http://host/broken.parquet\": Bad.\nIt stopped at http://host/broken.parquet."
    );
    assert_eq!(named_by_source("no path here", file, url), "no path here");
    assert_eq!(named_by_source("x", Path::new(""), url), "x");
    assert_eq!(
        named_by_source("'/home/u/tmp/.tmp9tY5X2.parquet' (os error 2)", file, url),
        "'http://host/broken.parquet' (os error 2)"
    );
}

/// A path that only shares the temp file's as its start or its end is another
/// file, and is left alone; so is the same name under another directory.
#[test]
fn a_similar_path_is_not_renamed() {
    let copy = Path::new("/home/u/tmp/.tmpAb12Cd");
    let gz = Path::new("/data/rows.csv.gz");
    for other in [
        "/home/u/tmp/.tmpAb12Cd.csv",
        "/home/u/tmp/.tmpAb12Cd2",
        "/home/u/tmp/.tmpAb12Cd_old",
        "/home/u/tmp/.tmpAb12Cd/part-0.csv",
        "/mnt/home/u/tmp/.tmpAb12Cd",
        "x/home/u/tmp/.tmpAb12Cd",
    ] {
        let message = format!("\"{other}\": Bad.");
        assert_eq!(named_by_source(&message, copy, gz), message, "{other}");
    }
    assert_eq!(
        named_by_source(
            "/home/u/tmp/.tmpAb12Cd.csv is not /home/u/tmp/.tmpAb12Cd.",
            copy,
            gz
        ),
        "/home/u/tmp/.tmpAb12Cd.csv is not /data/rows.csv.gz.",
    );
}

/// The census directory in `cloud-samples-data`: two headerless CSVs and one with a
/// header, so the names Polars compares are a first row's values.
#[test]
fn files_whose_columns_differ_are_said_as_files() {
    let said = user_message_from_polars(&PolarsError::ComputeError(
        "schema names differ: got 39, expected 25".into(),
    ));
    assert!(said.contains("cannot be read as one table"), "{said}");
    assert!(
        said.contains("a column named \"39\" where another has \"25\""),
        "{said}"
    );
    assert!(said.contains("--no-header"), "{said}");
    assert!(!said.contains("schema"), "no Polars words left: {said}");

    let said = user_message_from_polars(&PolarsError::ComputeError("schema lengths differ".into()));
    assert!(said.contains("different numbers of columns"), "{said}");

    let other =
        user_message_from_polars(&PolarsError::ComputeError("something else entirely".into()));
    assert!(!other.contains("one table"), "{other}");
}

/// A real message datui produced, with Polars' plan on the end of it.
///
/// Captured from a directory of three CSVs that share no columns, before the reader
/// learned to union them. The plan is four lines of internals around one fact worth
/// keeping — the file it stopped at.
#[test]
fn a_polars_query_plan_is_not_shown_to_the_user() {
    let raw = "Operation not allowed: 'union'/'concat' inputs should all have the \
                   same schema,got\nSchema { fields: {\"a\": Int64, \"b\": Int64} } and \
                   \nSchema { fields: {\"q\": Int64} }\n\nResolved plan until failure:\n\n\
                   \t---> FAILED HERE RESOLVING THIS_NODE <---\nCsv SCAN \
                   [/data/mixed/two.csv]\nPROJECT */3 COLUMNS\nESTIMATED ROWS: 2";

    let said = union_schema_message(raw);
    assert!(
        !said.contains("FAILED HERE") && !said.contains("PROJECT"),
        "the plan is gone: {said:?}"
    );
    assert!(
        !said.contains("Schema {"),
        "and so are two schemas printed in full: {said:?}"
    );
    assert!(
        said.contains("/data/mixed/two.csv"),
        "but the file it stopped at is kept: {said:?}"
    );

    // And the general case, for every other error Polars hangs a plan on.
    let other = "Column not found: region\n\nResolved plan until failure:\n\n\
                     \t---> FAILED HERE RESOLVING THIS_NODE <---\nParquet SCAN \
                     [/data/events/part-7.parquet]\nPROJECT 3/9 COLUMNS";
    let tidied = without_the_query_plan(other);
    assert_eq!(
        tidied, "Column not found: region\nIt stopped at /data/events/part-7.parquet.",
        "got {tidied:?}"
    );

    // A message with no plan on it is untouched.
    assert_eq!(
        without_the_query_plan("Column not found: region"),
        "Column not found: region"
    );

    // More than one scan in the plan: the one under the marker, not the first.
    let two_scans = "Column not found: region\n\nResolved plan until failure:\n\n                         Parquet SCAN [/data/a.parquet]\nUNION\n                         \t---> FAILED HERE RESOLVING THIS_NODE <---\n                         Csv SCAN [/data/b.csv]";
    assert!(
        without_the_query_plan(two_scans).ends_with("It stopped at /data/b.csv."),
        "got {:?}",
        without_the_query_plan(two_scans)
    );

    // `unable to vstack` is not a directory problem: the row buffer and the
    // data-quality pass both stitch frames of one open file with it.
    assert!(
        !is_union_schema_error("unable to vstack, column names don't match: \"a\" and \"b\""),
        "a single-file vstack must keep its own message"
    );
}

#[test]
fn test_user_message_from_io_not_found() {
    let err = io::Error::new(io::ErrorKind::NotFound, "No such file");
    let msg = user_message_from_io(&err, None);
    assert!(
        msg.contains("not found"),
        "expected 'not found', got: {}",
        msg
    );
}

/// A file a spreadsheet app holds is named, with what to do, whichever way the
/// sharing violation arrives: from datui's own open, or rewrapped by Polars with
/// the path in its text.
#[test]
fn a_file_another_program_holds_says_so() {
    let path = Path::new(r"C:\data\book.xlsx");
    let held = format!(
        "\"{}\": The file is open in another program that does not allow reading it; \
             close it there and reopen.",
        path.display()
    );
    let raw = || io::Error::from_raw_os_error(32);
    let rewrapped = || {
        io::Error::other(format!(
            "The process cannot access the file because it is being used by another \
                 process. (os error 32): {}",
            path.display()
        ))
    };
    for report in [
        color_eyre::eyre::Report::new(raw()),
        color_eyre::eyre::Report::new(PolarsError::from(rewrapped())),
        color_eyre::eyre::Report::new(PolarsError::from(raw()).context("scan".into())),
        color_eyre::eyre::eyre!("{}", PolarsError::from(rewrapped())),
    ] {
        assert_eq!(
            report_message(true, &report, Some(path)),
            held,
            "{report:?}"
        );
        // Off Windows the codes mean something else. (On Windows the io message
        // inside says so whatever `report_message` is told.)
        if !cfg!(windows) {
            let elsewhere = report_message(false, &report, Some(path));
            assert!(!elsewhere.contains("another program"), "{elsewhere}");
        }
    }
    assert!(held_on(true, &raw()));
    assert!(held_on(true, &io::Error::from_raw_os_error(33)));
    assert!(!held_on(true, &io::Error::from_raw_os_error(5)));
    // EPIPE off Windows.
    assert!(!held_on(false, &raw()));
}

#[test]
fn test_user_message_from_io_permission_denied() {
    let err = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
    let msg = user_message_from_io(&err, None);
    assert!(
        msg.to_lowercase().contains("permission"),
        "expected 'permission', got: {}",
        msg
    );
}

#[test]
fn test_user_message_from_polars_column_not_found() {
    use polars::prelude::PolarsError;
    let err = PolarsError::ColumnNotFound("foo".into());
    let msg = user_message_from_polars(&err);
    assert!(msg.contains("foo"), "expected 'foo', got: {}", msg);
    assert!(
        msg.contains("Column not found"),
        "expected column not found, got: {}",
        msg
    );
}

#[test]
fn test_user_message_from_polars_duplicate() {
    use polars::prelude::PolarsError;
    let err = PolarsError::Duplicate("bar".into());
    let msg = user_message_from_polars(&err);
    assert!(
        msg.contains("Duplicate"),
        "expected 'Duplicate', got: {}",
        msg
    );
    assert!(msg.contains("alias"), "expected alias hint, got: {}", msg);
}

#[test]
fn test_simplify_compute_message_alias_hint() {
    let raw = "projections contained duplicate: 'x'. Try renaming with .alias(\"name\")";
    let msg = simplify_compute_message(raw);
    assert!(
        !msg.contains(".alias("),
        "should strip .alias( hint: {}",
        msg
    );
    assert!(
        msg.contains("Use aliases"),
        "expected alias suggestion: {}",
        msg
    );
}

#[test]
fn test_simplify_compute_message_csv_parse_error() {
    let raw = "could not parse `N/A` as dtype `i64` at column 'column' (column number 1)\n\n\
            The current offset in the file is 292 bytes.\n\n\
            You might want to try: ...\n\
            Original error: ```invalid primitive value found during CSV parsing```";
    let msg = simplify_compute_message(raw);
    assert!(
        msg.contains("CSV parse error"),
        "expected short CSV message: {}",
        msg
    );
    assert!(
        msg.contains("column \"column\""),
        "expected offending column in message: {}",
        msg
    );
    assert!(msg.contains("--infer-rows"), "expected CLI hint: {}", msg);
    assert!(msg.contains("--null"), "expected null-value hint: {}", msg);
    assert!(
        !msg.contains("Original error"),
        "should not regurgitate Polars: {}",
        msg
    );
}

/// A SQL statement's failure as the run reports it.
#[cfg(feature = "sql")]
fn sql_failure(sql: &str, df: polars::prelude::DataFrame) -> PolarsError {
    use polars::prelude::IntoLazy;
    let mut ctx = polars_sql::SQLContext::new();
    ctx.register("df", df.lazy());
    ctx.execute(sql)
        .expect("plans")
        .collect()
        .expect_err("fails at run time")
}

/// The Premier League date column: a few postponed matches carry a marker.
#[cfg(feature = "sql")]
fn matches() -> polars::prelude::DataFrame {
    let dates: Vec<String> = (0..380)
        .map(|i| match i % 30 {
            0 => "Tue Jan 12 2021(P)".to_string(),
            10 => "Sat Feb 20 2021(P)".to_string(),
            _ => "Sun Sep 13 2020".to_string(),
        })
        .collect();
    let scores: Vec<String> = (0..380)
        .map(|i| if i % 50 == 0 { "n/a" } else { "3" }.to_string())
        .collect();
    polars::prelude::df!("Date" => dates, "Team 1" => scores).unwrap()
}

#[cfg(feature = "sql")]
#[test]
fn a_date_that_does_not_parse_is_said_in_sql_terms() {
    let err = sql_failure(
        "SELECT STRPTIME(Date, '%a %b %d %Y') AS d FROM df",
        matches(),
    );
    let failure = conversion_failure(&err).expect("a conversion");
    assert_eq!(failure.column, "Date");
    assert!(failure.parsing);
    assert_eq!(failure.failed, 26);
    let msg = sql_error_message(&err, Some(380));
    assert!(
        msg.starts_with(
            "Date: 26 of 380 values do not match the format, such as \"Tue Jan 12 2021(P)\""
        ),
        "{msg}"
    );
    assert!(msg.contains("SUBSTR(Date, 1, n)"), "{msg}");
    assert!(
        !msg.contains("strict=False") && !msg.contains("str.strptime"),
        "{msg}"
    );
    // Read in batches, the count is a floor.
    let msg = sql_error_message(&err, Some(1000));
    assert!(
        msg.starts_with("At least 26 values in Date do not match"),
        "{msg}"
    );
}

#[cfg(feature = "sql")]
#[test]
fn a_cast_that_fails_names_the_column_and_suggests_try_cast() {
    let err = sql_failure(
        "SELECT CAST(\"Team 1\" AS INT) + 1 AS goals FROM df ORDER BY goals",
        matches(),
    );
    let msg = sql_error_message(&err, Some(380));
    assert!(
        msg.starts_with("\"Team 1\": 8 of 380 values are not whole numbers, such as \"n/a\"."),
        "{msg}"
    );
    assert!(msg.contains("TRY_CAST(\"Team 1\" AS INT)"), "{msg}");
}

/// Without the whole table in the batch, one failure is a floor too.
#[test]
fn a_count_short_of_the_table_is_a_lower_bound() {
    let failure = ConversionFailure {
        column: "FT".to_string(),
        to: "i32".to_string(),
        failed: 1,
        checked: 1,
        examples: vec!["0–3".to_string()],
        parsing: false,
    };
    let lead = |rows| {
        failure
            .sql_message(rows)
            .lines()
            .next()
            .unwrap()
            .to_string()
    };
    assert_eq!(
        lead(None),
        "At least 1 value in FT is not a whole number, such as \"0–3\"."
    );
    assert_eq!(
        lead(Some(1)),
        "FT: 1 of 1 values are not whole numbers, such as \"0–3\"."
    );
}

#[test]
fn anything_else_is_said_as_polars_says_it() {
    let err = PolarsError::InvalidOperation("something else".into());
    assert_eq!(conversion_failure(&err), None);
    assert_eq!(
        sql_error_message(&err, None),
        user_message_from_polars(&err)
    );
}

/// A file the open listed and the store dropped since says so, named under the
/// dataset, without the request's URL, timing and status.
#[cfg(feature = "cloud")]
#[test]
fn a_file_gone_since_the_open_says_reopen() {
    let key = "parquet/by_year/YEAR=2024/ELEMENT=TMAX/0499_0.snappy.parquet";
    let err = PolarsError::from(polars::io::cloud::PolarsObjectStoreError {
        base_url: "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/".into(),
        source: object_store::Error::NotFound {
            path: key.to_string(),
            source: "Server returned non-2xx status code: 404 Not Found".into(),
        },
    });
    let message = user_message_from_polars(&err);
    assert_eq!(
        message,
        "A file was removed or replaced after the dataset was opened: \
         s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/0499_0.snappy.parquet. \
         Reopen the dataset to read the current files."
    );
    // Polars caches a store per bucket, built by whichever path came first: the file is
    // named the same whatever that path was, even the file itself.
    for base_url in [
        "s3://noaa-ghcn-pds/",
        "s3://noaa-ghcn-pds/other/prefix/",
        &format!("s3://noaa-ghcn-pds/{key}"),
    ] {
        let err = PolarsError::from(polars::io::cloud::PolarsObjectStoreError {
            base_url: base_url.into(),
            source: object_store::Error::NotFound {
                path: key.to_string(),
                source: "404".into(),
            },
        });
        assert_eq!(user_message_from_polars(&err), message, "{base_url}");
    }
    // The app offers the reopen from this, however the message was framed.
    assert!(says_gone_since_opened(&format!(
        "Error applying view: {message}"
    )));
    assert!(!says_gone_since_opened("No object there. Check the URL."));
}

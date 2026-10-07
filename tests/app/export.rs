//! Export and copy: files, the clipboard, and copy as Python.

use super::*;

/// Declining an overwrite returns to the filled export form: the typed path
/// survives, the prompt starts on No, and the existing file is untouched.
#[test]
fn declining_an_overwrite_keeps_the_export_form() {
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("already.csv");
    std::fs::write(&target, "old contents").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );

    let key =
        |app: &mut App, code| app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));

    key(&mut app, KeyCode::Char('e'));
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    // The form suggests a name; Backspace takes it away.
    assert_eq!(app.export_modal.path_input.value(), "people-export.parquet");
    key(&mut app, KeyCode::Backspace);
    assert_eq!(app.export_modal.path_input.value(), "");

    // Enter on the empty form says why inline instead of doing nothing,
    // and typing is the correction that clears it.
    key(&mut app, KeyCode::Enter);
    assert!(
        matches!(app.overlay, Overlay::Export { .. }),
        "an empty path raises no modal"
    );
    assert_eq!(
        app.export_modal.path_error.as_deref(),
        Some("Enter a file path.")
    );
    key(&mut app, KeyCode::Char('x'));
    assert_eq!(app.export_modal.path_error, None);
    key(&mut app, KeyCode::Backspace);

    let typed = target.display().to_string();
    app.export_modal.path_input.set_value(&typed);
    key(&mut app, KeyCode::Enter);

    assert!(app.confirmation_modal.active, "an existing file asks first");
    assert!(
        !app.confirmation_modal.focus_yes,
        "a destructive confirmation starts on No"
    );
    assert_eq!(app.confirmation_modal.yes_label, "Overwrite");

    // A reflexive second Enter declines, and the form comes back as typed.
    key(&mut app, KeyCode::Enter);
    assert!(!app.confirmation_modal.active);
    assert!(
        matches!(app.overlay, Overlay::Export { .. }),
        "No returns to the form"
    );
    assert_eq!(app.export_modal.path_input.value(), typed);
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "old contents",
        "declining wrote nothing"
    );

    // Esc from the confirmation does the same.
    key(&mut app, KeyCode::Enter);
    assert!(app.confirmation_modal.active);
    key(&mut app, KeyCode::Esc);
    assert!(
        matches!(app.overlay, Overlay::Export { .. }),
        "Esc returns to the form"
    );
    assert_eq!(app.export_modal.path_input.value(), typed);

    // Esc from the form itself discards it.
    key(&mut app, KeyCode::Esc);
    assert!(!matches!(app.overlay, Overlay::Export { .. }));
    assert!(app.at_table());
}

/// Overwrite agreed to in the dialog: the file is replaced whole, keeps its
/// permission bits, and success is said once it has landed.
#[test]
fn test_an_export_replaces_a_file_after_the_overwrite_is_agreed() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_overwrite_ok.csv");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");
    std::fs::write(&target, "old contents").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
    }

    press(&mut app, KeyCode::Char('e'));
    app.export_modal.selected_format = datui::export_modal::ExportFormat::Csv;
    app.export_modal
        .path_input
        .set_value(target.display().to_string());
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.active, "an existing file asks first");
    press(&mut app, KeyCode::Left);
    let export = press(&mut app, KeyCode::Enter).expect("Overwrite starts the export");
    run_to_idle(&mut app, &rx, &tx, export);

    assert_eq!(app.error_message(), None);
    assert!(
        app.flash_message()
            .is_some_and(|m| m.starts_with("Exported to ")),
        "success is said once the file is in place"
    );
    let written = std::fs::read_to_string(&target).unwrap();
    assert!(written.starts_with("a,c,name\n"), "{written}");
    assert_eq!(written.lines().count(), 101);
    assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o640, "the replaced file's mode carries over");
    }
}

/// Nothing was there when Enter was pressed, so nothing was agreed to be
/// replaced: a file that appears before the export lands is left alone, and
/// the app says so instead of saying it exported.
#[test]
fn test_a_file_that_appears_during_an_export_is_left_alone() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_appears.csv");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");

    press(&mut app, KeyCode::Char('e'));
    app.export_modal
        .path_input
        .set_value(target.display().to_string());
    let export = press(&mut app, KeyCode::Enter).expect("no file there: the export starts");
    assert!(matches!(
        &export,
        AppEvent::Export(request) if request.overwrite == datui::output_file::Overwrite::Forbid
    ));
    std::fs::write(&target, "theirs").unwrap();
    run_to_idle(&mut app, &rx, &tx, export);

    assert!(
        app.export_modal
            .path_error
            .as_deref()
            .is_some_and(|m| m.contains("appeared")),
        "the failure reaches the form: {:?}",
        app.export_modal.path_error
    );
    assert!(
        !app.flash_message()
            .is_some_and(|m| m.starts_with("Exported to ")),
        "no success for an export that did not land"
    );
    assert!(!app.is_busy());
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "theirs");
    assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
}

/// A write that fails brings the dialog back as it was: the typed path, the
/// format and its options, with the reason on the dialog's status line instead
/// of a modal over an empty form.
#[test]
fn test_a_failed_export_reopens_the_form_as_it_was() {
    use datui::export_modal::ExportFormat;
    let (mut app, rx, tx) = open_query_filter_fixture("export_reopens.csv");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("missing").join("out.csv");

    press(&mut app, KeyCode::Char('e'));
    app.export_modal
        .path_input
        .set_value(target.display().to_string());
    app.export_modal.sync_format_to_path();
    app.export_modal.csv_include_header = false;
    let export = press(&mut app, KeyCode::Enter).expect("the export starts");
    assert!(
        !matches!(app.overlay, Overlay::Export { .. }),
        "out of the way while it writes"
    );
    run_to_idle(&mut app, &rx, &tx, export);

    assert_eq!(app.error_message(), None, "no modal");
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    assert!(matches!(app.overlay, datui::Overlay::Export { .. }));
    assert_eq!(
        app.export_modal.path_input.value(),
        target.display().to_string()
    );
    assert_eq!(app.export_modal.selected_format, ExportFormat::Csv);
    assert!(!app.export_modal.csv_include_header);
    assert!(
        app.export_modal
            .path_error
            .as_deref()
            .is_some_and(|m| m.starts_with("Cannot write")),
        "{:?}",
        app.export_modal.path_error
    );
    // The fix is typed where the reason is read, and Enter writes.
    std::fs::create_dir(dir.path().join("missing")).unwrap();
    press(&mut app, KeyCode::Char('x'));
    press(&mut app, KeyCode::Backspace);
    assert_eq!(
        app.export_modal.path_error, None,
        "typing clears the reason"
    );
    let export = press(&mut app, KeyCode::Enter).expect("the export starts again");
    run_to_idle(&mut app, &rx, &tx, export);
    assert!(target.exists());
    assert!(!matches!(app.overlay, Overlay::Export { .. }));
    assert!(
        app.flash_message()
            .is_some_and(|m| m.starts_with("Exported to "))
    );
}

/// A view whose rows fail part way through, over an agreed overwrite, by both
/// routes: streamed to an uncompressed CSV, collected for a gzipped one. The error
/// reaches the app, and the old file's bytes, its mode, and nothing else are left.
#[test]
fn test_a_failed_export_keeps_the_old_file() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_fails.csv");
    // Past the first of the streaming engine's 100,000-row batches, so the
    // streamed route has written rows before the failure.
    let rows = 300_000i64;
    let failing = df!("id" => (0..rows).collect::<Vec<_>>())
        .unwrap()
        .lazy()
        .with_column(col("id").map(
            move |c| {
                if c.i64()?.max().is_some_and(|id| id >= rows - 1) {
                    polars_bail!(ComputeError: "injected failure at the last row");
                }
                Ok(c)
            },
            |_, field| Ok(field.clone()),
        ));
    let state = datui::table::DataTableState::new(failing, None, None, None, None, true).unwrap();
    app.data_table_state = Some(state);
    let dir = tempfile::tempdir().unwrap();

    for (name, compression) in [
        ("out.csv", None),
        ("out.csv.gz", Some(datui::CompressionFormat::Gzip)),
    ] {
        let target = dir.path().join(name);
        std::fs::write(&target, "old contents").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o604)).unwrap();
        }
        let mut request = csv_request(&target, datui::output_file::Overwrite::Replace);
        request.options.csv_compression = compression;
        run_to_idle(&mut app, &rx, &tx, AppEvent::Export(request));

        assert!(
            app.export_modal
                .path_error
                .as_deref()
                .is_some_and(|m| m.contains("injected")),
            "{name}: the failure reaches the form: {:?}",
            app.export_modal.path_error
        );
        assert!(!app.is_busy());
        assert!(
            !app.flash_message()
                .is_some_and(|m| m.starts_with("Exported to "))
        );
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "old contents");
        assert!(leftovers(dir.path(), &["out.csv", "out.csv.gz"]).is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o604);
        }
        press(&mut app, KeyCode::Esc);
    }
}

/// With the streaming engine off, a CSV or Parquet export is collected and still
/// writes what the streamed one does.
#[test]
fn test_an_export_without_the_streaming_engine_writes_the_same_file() {
    use datui::export_modal::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let mut written = Vec::new();
    for streaming in [true, false] {
        let mut config = datui::AppConfig::default();
        config.performance.streaming = streaming;
        let (mut app, rx, tx) =
            open_query_filter_fixture_with(&format!("export_engine_{streaming}.csv"), config);
        app.data_table_state
            .as_mut()
            .unwrap()
            .query("select a, name where c = 1".to_string());
        pump_until_idle(&mut app, &rx, &tx);
        let csv = dir.path().join(format!("{streaming}.csv"));
        export_as(&mut app, &rx, &tx, &csv, ExportFormat::Csv, false);
        let parquet = dir.path().join(format!("{streaming}.parquet"));
        export_as(&mut app, &rx, &tx, &parquet, ExportFormat::Parquet, false);
        let back = ParquetReader::new(File::open(&parquet).unwrap())
            .finish()
            .unwrap();
        written.push((std::fs::read_to_string(&csv).unwrap(), back));
    }
    let (streamed, collected) = (&written[0], &written[1]);
    assert_eq!(streamed.0, collected.0);
    assert!(
        streamed.0.starts_with("a,name\n1,beta_1\n4,alpha_4\n"),
        "{}",
        streamed.0
    );
    assert_eq!(streamed.0.lines().count(), 34);
    assert!(streamed.1.equals_missing(&collected.1));
}

/// A q `by` result holds list columns, which CSV cannot: they are written
/// as JSON text, while Parquet keeps them as lists.
#[test]
fn test_csv_export_writes_a_by_result_lists_as_json() {
    use datui::export_modal::ExportFormat;
    let (mut app, rx, tx) = open_query_filter_fixture("export_nested_by.csv");
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select a, name by c where a < 6".to_string());
    press(&mut app, KeyCode::Char('e'));
    assert!(
        app.export_modal.nested_columns,
        "the dialog knows to say how lists are written"
    );
    let area = Rect::new(0, 0, 80, 24);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(
        common::buffer_text(&buffer).contains("Lists and structs written as JSON"),
        "{}",
        common::buffer_text(&buffer)
    );
    press(&mut app, KeyCode::Esc);
    let dir = tempfile::tempdir().unwrap();

    let out = dir.path().join("by.csv");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Csv, false);
    let back = read_csv_as_text(&out, "c");
    assert_eq!(
        text_column(&back, "a"),
        ["[0,3]", "[1,4]", "[2,5]"].map(|s| Some(s.to_string()))
    );
    assert_eq!(
        text_column(&back, "name"),
        [
            r#"["alpha_0","beta_3"]"#,
            r#"["beta_1","alpha_4"]"#,
            r#"["alpha_2","beta_5"]"#,
        ]
        .map(|s| Some(s.to_string()))
    );

    let out = dir.path().join("by.parquet");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Parquet, false);
    let back = ParquetReader::new(File::open(&out).unwrap())
        .finish()
        .unwrap();
    assert_eq!(
        back.column("a").unwrap().dtype(),
        &DataType::List(Box::new(DataType::Int64)),
        "Parquet keeps the real type"
    );
}

/// SQL's ARRAY_AGG and a struct column both reach a CSV as
/// JSON; a null list stays an empty field.
#[cfg(feature = "sql")]
#[test]
fn test_csv_export_writes_sql_arrays_and_structs_as_json() {
    let dir = tempfile::tempdir().unwrap();
    let point = StructChunked::from_series(
        "point".into(),
        3,
        [
            Series::new("x".into(), [1i64, 2, 3]),
            Series::new("label".into(), [Some("a"), None, Some("c,d")]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = df!(
        "g" => ["p", "q", "p"],
        "v" => [Some(1i64), None, Some(3)],
    )
    .unwrap()
    .hstack(&[point.into()])
    .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));
    app.data_table_state.as_mut().unwrap().sql_query(
        "select g, array_agg(v) as vs, first(point) as point from df group by g".to_string(),
    );

    let out = dir.path().join("sql.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, false);
    let back = read_csv_as_text(&out, "g");
    assert_eq!(
        text_column(&back, "vs"),
        [Some("[1,3]".to_string()), Some("[null]".to_string())],
        "{csv}"
    );
    assert_eq!(
        text_column(&back, "point"),
        [
            Some(r#"{"x":1,"label":"a"}"#.to_string()),
            Some(r#"{"x":2,"label":null}"#.to_string()),
        ],
        "{csv}"
    );
}

/// JSON has no binary type: JSON and NDJSON write binary as base64 text, alone
/// and inside a list or struct, and CSV spells it the same way.
#[test]
fn test_json_export_writes_binary_as_base64() {
    use datui::export_modal::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let blob = Series::new("blob".into(), [Some(b"hi\xff".as_slice()), None]);
    let blobs = Series::new(
        "blobs".into(),
        [Some(Series::new("".into(), [b"x".as_slice()])), None],
    );
    let meta = StructChunked::from_series(
        "meta".into(),
        2,
        [
            Series::new("raw".into(), [b"ab".as_slice(), b"".as_slice()]),
            Series::new("n".into(), [1i64, 2]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = df!("id" => [1i64, 2])
        .unwrap()
        .hstack(&[blob.into(), blobs.into(), meta.into()])
        .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));

    for (file, format, json) in [
        ("out.json", ExportFormat::Json, JsonFormat::Json),
        ("out.jsonl", ExportFormat::Ndjson, JsonFormat::JsonLines),
    ] {
        let out = dir.path().join(file);
        export_as(&mut app, &rx, &tx, &out, format, false);
        let back = JsonReader::new(File::open(&out).unwrap())
            .with_json_format(json)
            .finish()
            .unwrap_or_else(|e| panic!("{file}: {e}"));
        let blob = back.column("blob").unwrap().str().unwrap().clone();
        assert_eq!((blob.get(0), blob.get(1)), (Some("aGn/"), None), "{file}");
        let blobs = back.column("blobs").unwrap().list().unwrap().clone();
        let first = blobs.get_as_series(0).unwrap();
        assert_eq!(first.str().unwrap().get(0), Some("eA=="), "{file}");
        assert!(blobs.get_as_series(1).is_none(), "{file}: a null list");
        let raw = back
            .column("meta")
            .unwrap()
            .struct_()
            .unwrap()
            .field_by_name("raw")
            .unwrap();
        assert_eq!(
            (raw.str().unwrap().get(0), raw.str().unwrap().get(1)),
            (Some("YWI="), Some("")),
            "{file}"
        );
    }

    let out = dir.path().join("out.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, false);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(
        lines,
        [
            "id,blob,blobs,meta",
            r#"1,aGn/,"[""eA==""]","{""raw"":""YWI="",""n"":1}""#,
            r#"2,,,"{""raw"":"""",""n"":2}""#,
        ]
    );
}

/// A duration view exports to CSV by either engine and copies in every scope,
/// as ISO 8601 throughout; CSV cannot write the type itself (#482).
#[test]
fn test_durations_export_and_copy_as_iso_8601() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let values = Series::new("".into(), [Some(3_723_004i64), None, Some(-1_500)]);
    let mut df = df!("id" => [1i64, 2, 3]).unwrap();
    for (name, unit) in [
        ("ms", TimeUnit::Milliseconds),
        ("us", TimeUnit::Microseconds),
        ("ns", TimeUnit::Nanoseconds),
    ] {
        df.with_column(
            values
                .cast(&DataType::Duration(unit))
                .unwrap()
                .with_name(name.into())
                .into_column(),
        )
        .unwrap();
    }
    let path = dir.path().join("durations.parquet");
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let rows = [
        "id,ms,us,ns",
        "1,PT3723.004S,PT3.723004S,PT0.003723004S",
        "2,,,",
        "3,-PT1.5S,-PT0.0015S,-PT0.0000015S",
    ];

    let open = |streaming: bool| {
        let mut config = datui::AppConfig::default();
        config.performance.streaming = streaming;
        let (tx, rx) = mpsc::channel();
        let theme = datui::Theme::from_config(&config.theme).unwrap();
        let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        (app, rx, tx)
    };
    for streaming in [true, false] {
        let (mut app, rx, tx) = open(streaming);
        let out = dir.path().join(format!("streaming-{streaming}.csv"));
        let csv = export_csv(&mut app, &rx, &tx, &out, false);
        assert_eq!(
            csv.lines().collect::<Vec<_>>(),
            rows,
            "streaming={streaming}"
        );
    }

    let (mut app, rx, tx) = open(true);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));
    let tsv: Vec<String> = rows.iter().map(|r| r.replace(',', "\t")).collect();
    let last = |copies: &Arc<Mutex<Vec<Payload>>>| copies.lock().unwrap().last().unwrap().clone();

    // Row, the default scope, header off.
    press(&mut app, KeyCode::Char('y'));
    press(&mut app, KeyCode::Enter);
    assert_eq!(last(&copies).text, tsv[1]);

    // View, with its header and the HTML flavor.
    press(&mut app, KeyCode::Char('y'));
    copy_scope(&mut app, datui::copy_modal::CopyScope::View);
    press(&mut app, KeyCode::Enter);
    let view = last(&copies);
    assert_eq!(view.text, tsv.join("\n"));
    let html = view.html.expect("tsv carries html");
    assert!(html.contains("<td>-PT0.0015S</td>"), "{html}");

    // Table, collected off-thread.
    press(&mut app, KeyCode::Char('y'));
    copy_scope(&mut app, datui::copy_modal::CopyScope::Table);
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(ev) = next {
        next = app.event(ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(last(&copies).text, tsv.join("\n"));

    // One cell: `ns` of the first row.
    press(&mut app, KeyCode::Char('y'));
    copy_scope(&mut app, datui::copy_modal::CopyScope::Cell);
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char('n'));
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Enter);
    assert_eq!(last(&copies).text, "PT0.003723004S");
}

/// Avro has no fixed-size array or categorical type: the export writes them as
/// a list and as strings, inside a list too, and a view read from two files
/// (two chunks) still makes one readable file.
#[test]
fn test_avro_export_writes_arrays_and_categoricals() {
    use datui::export_modal::ExportFormat;
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    for (file, offset) in [("a.parquet", 0i64), ("b.parquet", 2)] {
        let mut df = df!(
            "id" => [offset, offset + 1],
            "tag" => ["x", "y"],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("tag").cast(DataType::from_categories(Categories::global())),
            concat_list([col("id"), col("id")])
                .unwrap()
                .cast(DataType::Array(Box::new(DataType::Int64), 2))
                .alias("pair"),
        ])
        .with_columns([concat_list([col("tag")]).unwrap().alias("tags")])
        .collect()
        .unwrap();
        ParquetWriter::new(File::create(src.join(file)).unwrap())
            .finish(&mut df)
            .unwrap();
    }
    let (mut app, rx, tx) = open_local_dataset_with_channel(&src);
    let schema = app.data_table_state.as_ref().unwrap().schema().clone();
    assert!(
        matches!(schema.get("pair"), Some(DataType::Array(..)))
            && matches!(schema.get("tag"), Some(DataType::Categorical(..))),
        "the view has the types Avro lacks: {schema:?}"
    );

    let out = dir.path().join("out.avro");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Avro, false);
    let back = AvroReader::new(File::open(&out).unwrap())
        .finish()
        .unwrap()
        .sort(["id"], Default::default())
        .unwrap();
    assert_eq!(back.height(), 4);
    assert_eq!(
        back.column("pair").unwrap().dtype(),
        &DataType::List(Box::new(DataType::Int64))
    );
    assert_eq!(back.column("tag").unwrap().dtype(), &DataType::String);
    assert_eq!(
        back.column("tags").unwrap().dtype(),
        &DataType::List(Box::new(DataType::String))
    );
    let tag = back.column("tag").unwrap().str().unwrap().clone();
    assert_eq!(
        (0..4).map(|i| tag.get(i)).collect::<Vec<_>>(),
        [Some("x"), Some("y"), Some("x"), Some("y")]
    );
}

/// Avro names are `[A-Za-z_][A-Za-z0-9_]*`: the export renames columns and
/// struct fields to fit, the record gets a name, and the values stay.
#[test]
fn test_avro_export_writes_valid_names() {
    use datui::export_modal::ExportFormat;
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    let df = df!(
        "my col" => [1i64, 2],
        "2024" => ["x", "y"],
        "a-b" => [1.5f64, 2.5],
        "a_b" => [true, false],
    )
    .unwrap()
    .lazy()
    .with_columns([
        as_struct(vec![col("my col").alias("x y"), col("2024").alias("x-y")]).alias("délai"),
    ])
    .collect()
    .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));
    press(&mut app, KeyCode::Char('e'));
    assert!(app.export_modal.avro_renames, "the dialog says so");
    press(&mut app, KeyCode::Esc);

    let out = dir.path().join("out.avro");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Avro, false);
    let schema = avro_schema(&out);
    let mut names = Vec::new();
    avro_schema_names(&schema, &mut names);
    assert!(names.contains(&"Row".to_string()), "{names:?}");
    for name in &names {
        let mut chars = name.chars();
        assert!(
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{name:?} in {names:?}"
        );
    }
    let docs: Vec<(&str, Option<&str>)> = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["name"].as_str().unwrap(), f["doc"].as_str()))
        .collect();
    assert_eq!(
        docs,
        [
            ("my_col", Some("my col")),
            ("_2024", Some("2024")),
            ("a_b_2", Some("a-b")),
            ("a_b", None),
            ("d_lai", Some("délai")),
        ],
        "the original names stay in the file"
    );

    let back = AvroReader::new(File::open(&out).unwrap()).finish().unwrap();
    let columns: Vec<&str> = back.get_column_names().iter().map(|n| n.as_str()).collect();
    assert_eq!(columns, ["my_col", "_2024", "a_b_2", "a_b", "d_lai"]);
    assert_eq!(
        back.column("_2024").unwrap().str().unwrap().get(1),
        Some("y")
    );
    assert_eq!(
        back.column("a_b_2").unwrap().f64().unwrap().get(0),
        Some(1.5)
    );
    assert_eq!(
        back.column("a_b").unwrap().bool().unwrap().get(0),
        Some(true)
    );
    let point = back.column("d_lai").unwrap().struct_().unwrap().clone();
    assert_eq!(
        point.field_by_name("x_y").unwrap().i64().unwrap().get(1),
        Some(2)
    );
    assert_eq!(
        point.field_by_name("x_y_2").unwrap().str().unwrap().get(0),
        Some("x")
    );
}

/// Every copy format takes list cells as JSON, the same text a CSV export
/// writes, instead of failing on them.
#[test]
fn test_copy_writes_list_cells_as_json() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let (mut app, rx, tx) = open_query_filter_fixture("copy_nested_by.csv");
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select name by c where a < 2".to_string());
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    pump_until_idle(&mut app, &rx, &tx);
    app.render(area, &mut buffer);
    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));

    // The table scope, collected off-thread, as TSV with its header.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    copy_scope(&mut app, datui::copy_modal::CopyScope::Table);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let text = copies.lock().unwrap().last().expect("a copy").text.clone();
    let mut lines: Vec<&str> = text.lines().collect();
    lines[1..].sort();
    assert_eq!(
        lines,
        [
            "c\tname",
            "0\t\"[\"\"alpha_0\"\"]\"",
            "1\t\"[\"\"beta_1\"\"]\""
        ]
    );

    // The same scope as Markdown.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    copy_format(&mut app, datui::clipboard::CopyFormat::Markdown);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let text = copies.lock().unwrap().last().expect("a copy").text.clone();
    assert!(text.contains(r#"["alpha_0"]"#), "{text}");
}

/// Asking an export to name each row's file keeps the absent-versus-null distinction
/// once the data has left datui: `extra` is empty in both rows, but only one of them
/// came from a file that had the column.
#[test]
fn test_an_export_can_name_the_file_each_row_came_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &[None::<&str>]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .can_name_source_files(),
        "the files disagree, so there is something to name"
    );

    let out = dir.path().join("named.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "date,id,extra,source_file");
    assert!(
        lines[1].ends_with(&native("date=2024-01-01/data.parquet")),
        "the first row came from the file without `extra`: {}",
        lines[1]
    );
    assert!(
        lines[2].ends_with(&native("date=2024-01-02/data.parquet")),
        "and the second from the one that has it, holding a real null: {}",
        lines[2]
    );
    // Both write null — an absent cell and a real null are the same thing to a CSV, and
    // the source file is what tells them apart once the data has left. Asserting the
    // whole line rather than its end, because `extra` being empty is the half of this
    // the doc comment claims and nothing checked.
    assert!(
        lines[1].starts_with("2024-01-01,1,,"),
        "the absent cell is written as null: {}",
        lines[1]
    );
    assert!(
        lines[2].starts_with("2024-01-02,2,,"),
        "and so is the real one: {}",
        lines[2]
    );

    // Streamed to Parquet, the names are the same.
    let parquet = dir.path().join("named.parquet");
    export_as(
        &mut app,
        &rx,
        &tx,
        &parquet,
        datui::export_modal::ExportFormat::Parquet,
        true,
    );
    let back = ParquetReader::new(File::open(&parquet).unwrap())
        .finish()
        .unwrap();
    assert_eq!(
        back.get_column_names(),
        ["date", "id", "extra", "source_file"]
    );
    let files = back.column("source_file").unwrap().str().unwrap().clone();
    assert!(
        files
            .get(0)
            .is_some_and(|f| f.ends_with(&native("date=2024-01-01/data.parquet")))
            && files
                .get(1)
                .is_some_and(|f| f.ends_with(&native("date=2024-01-02/data.parquet"))),
        "{files:?}"
    );

    // Off by default, and then the hidden index must not leak in its place.
    let plain = dir.path().join("plain.csv");
    let csv = export_csv(&mut app, &rx, &tx, &plain, false);
    let plain_lines: Vec<&str> = csv.lines().collect();
    assert_eq!(plain_lines[0], "date,id,extra");
    assert_eq!(
        &plain_lines[1..],
        ["2024-01-01,1,", "2024-01-02,2,"],
        "and without it both are still null, and nothing else has appeared"
    );
}

/// Avro widens every `u32`; the files are named from the hidden row index before
/// that, through the collected route.
#[test]
fn test_an_avro_export_can_name_the_file_each_row_came_from() {
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &[None::<&str>]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());

    let out = dir.path().join("named.avro");
    export_as(
        &mut app,
        &rx,
        &tx,
        &out,
        datui::export_modal::ExportFormat::Avro,
        true,
    );
    let back = AvroReader::new(File::open(&out).unwrap()).finish().unwrap();
    let files = back.column("source_file").unwrap().str().unwrap().clone();
    assert!(
        files
            .get(0)
            .unwrap()
            .ends_with(&native("date=2024-01-01/data.parquet"))
            && files
                .get(1)
                .unwrap()
                .ends_with(&native("date=2024-01-02/data.parquet")),
        "{files:?}"
    );
}

/// The Options panel must read as a panel at every format: no empty box, and the
/// source-file checkbox under the format's own options rather than adrift at the foot.
#[test]
fn test_the_export_options_panel_reads_as_one_for_every_format() {
    use datui::export_modal::ExportFormat;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    // The modal opens with focus on the path, where → does not change the format.
    for key in [KeyCode::Char('e'), KeyCode::BackTab] {
        app.event(AppEvent::Key(KeyEvent::new(key, KeyModifiers::NONE)));
    }
    assert!(app.export_modal.offer_source_file, "the files disagree");
    assert_eq!(
        app.export_modal.focus,
        datui::export_modal::ExportFocus::FormatSelector,
        "so the walk below really does change format"
    );

    let area = Rect::new(0, 0, 120, 30);
    let mut seen = Vec::new();
    let mut wrong = Vec::new();
    for _ in 0..ExportFormat::ALL.len() {
        let format = app.export_modal.selected_format;
        seen.push(format);
        // The last row each format draws of its own. The checkbox goes directly under
        // it, so asking for this row by name pins the form's row list: a row missing
        // and the format's last option is gone, a row extra and a gap opens up.
        // Either way this row is no longer the one above the checkbox.
        let last_of_its_own = match format {
            // These end on their compression row.
            ExportFormat::Csv
            | ExportFormat::Tsv
            | ExportFormat::Psv
            | ExportFormat::Json
            | ExportFormat::Ndjson => "Compression:",
            // No options of their own, so the checkbox sits right under the path.
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => "Path:",
        };

        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = common::buffer_lines(&buf);
        match rows.iter().position(|r| r.contains("Source file:")) {
            None => wrong.push(format!("{format:?}: no Source file row at all")),
            Some(checkbox) if !rows[checkbox - 1].contains(last_of_its_own) => wrong.push(format!(
                "{format:?}: the row above the checkbox should be the one holding \
                 {last_of_its_own:?}, and is {:?}",
                rows[checkbox - 1].trim_end()
            )),
            Some(_) => {}
        }
        // Step to the next format.
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
    }
    // Collected rather than asserted in the loop: the formats fail in families, and
    // one report naming every bad format beats six runs that each name the first.
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(
        seen,
        ExportFormat::ALL.to_vec(),
        "the walk must visit every format once, in order"
    );
}

/// The flag outranks the separator a `.tsv` implies, and export offers it. Without the
/// flag export offers a comma, not the tab: a `.tsv` exports as CSV, to a `.csv` by
/// default, and a tab there reopens as one column.
#[test]
fn test_delimiter_flag_overrides_the_format_and_reaches_export() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let tsv = tmp.path().join("data.tsv");
    std::fs::write(&tsv, "a;b\tc\n1;2\t3\n").unwrap();

    let export_default = |app: &mut App| {
        app.event(key(KeyCode::Char('e')));
        app.export_modal.csv_delimiter_input.value().to_string()
    };

    let (mut app, df) = open_and_collect(
        vec![tsv.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["a;b", "c"]);
    assert_eq!(export_default(&mut app), ",");

    let (mut app, df) = open_and_collect(
        vec![tsv],
        options_as_the_binary_does(&["datui", "x", "--delimiter", ";"], ""),
    );
    assert_eq!(names(&df), ["a", "b\tc"]);
    assert_eq!(export_default(&mut app), ";");
}

/// Quitting mid-decompression removes the partial copy before the session ends, even
/// while the worker is stuck in a read and cannot see the stop: the source is a pipe
/// that sends part of the file and then nothing (#510).
#[cfg(unix)]
#[test]
fn quitting_mid_decompression_removes_the_partial_copy() {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::time::{Duration, Instant};
    common::isolate_cache();
    let source = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let pipe = source.path().join("rows.csv.gz");
    let name = std::ffi::CString::new(pipe.as_os_str().as_bytes()).unwrap();
    // SAFETY: a valid, NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let mut sent = b"id,name\n".to_vec();
    for i in 0..10_000 {
        writeln!(sent, "{i},row").unwrap();
    }
    let whole = sent.len() as u64;
    let feeder = {
        let pipe = pipe.clone();
        std::thread::spawn(move || {
            let sink = std::fs::OpenOptions::new().write(true).open(&pipe)?;
            let mut gz = GzEncoder::new(sink, Compression::fast());
            gz.write_all(&sent)?;
            // A sync flush: what is written so far can be decompressed. The encoder
            // is kept, so the rest never comes until the test lets it go.
            gz.flush()?;
            std::io::Result::Ok(gz)
        })
    };
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let copy = || {
        std::fs::read_dir(scratch.path())
            .unwrap()
            // The file's size, not the directory entry's: on Windows that changes only
            // when the writer closes the file.
            .map(|f| std::fs::metadata(f.unwrap().path()).map_or(0, |m| m.len()))
            .collect::<Vec<_>>()
    };
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut next = app.event(AppEvent::Open(vec![pipe], options));
    while let Some(event) = next {
        next = app.event(event);
    }
    // The copy has stopped growing: the worker waits in a read for the rest. The
    // decoder keeps the last bytes until more input comes, so nothing but time says
    // the copy is done: it is sampled until it holds still, most of it there.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = Vec::new();
    loop {
        std::thread::sleep(Duration::from_millis(50));
        let now = copy();
        if now == seen && now.first().is_some_and(|len| *len > whole / 2) {
            break;
        }
        assert!(Instant::now() < deadline, "decompressed {now:?} of {whole}");
        seen = now;
    }

    let sweep = app.exit_sweep();
    drop(app);
    drop(sweep);
    assert!(copy().is_empty(), "left behind: {:?}", copy());
    // The rest of the file, and its end: the stopped worker gives up.
    drop(feeder.join().expect("the feeder"));
}

/// A CSV over HTTP is read again from the copy already downloaded, not fetched again.
#[cfg(feature = "http")]
#[test]
fn h_rereads_a_download_from_the_copy_on_hand() {
    use std::sync::atomic::Ordering;
    common::isolate_cache();
    let (url, fetched) = serve_over_http("adult.csv", b"39,77516\n50,83311\n38,215646\n".to_vec());

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["39", "77516"]);
    let downloads = fetched.load(Ordering::SeqCst);
    assert!(downloads >= 1, "it was downloaded");

    header_from_schema_tab(&mut app, &rx);
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    assert_eq!(
        fetched.load(Ordering::SeqCst),
        downloads,
        "read again from the copy on hand"
    );
}

/// Typing a path whose extension names a format moves the format radio with it, so
/// the export never writes Parquet bytes into a file named `out.csv`.
#[test]
fn test_export_format_follows_typed_extension() {
    use datui::export_modal::ExportFormat;
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_ext_follows.csv");

    press(&mut app, KeyCode::Char('e'));
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    app.export_modal.selected_format = ExportFormat::Parquet;

    let out = common::fixture_dir().join("export_ext_follows_out.csv");
    for ch in out.to_str().unwrap().chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    assert_eq!(
        app.export_modal.selected_format,
        ExportFormat::Csv,
        "typing a .csv path switches the format radio"
    );

    // And the export the Enter key builds carries that format.
    match press(&mut app, KeyCode::Enter) {
        Some(AppEvent::Export(datui::ExportRequest { path, format, .. })) => {
            assert_eq!(format, ExportFormat::Csv);
            assert_eq!(path, out);
        }
        _ => panic!("Enter from the path input did not build an export"),
    }
}

/// An extension that names no format leaves an explicit choice alone.
#[test]
fn test_export_unknown_extension_keeps_the_chosen_format() {
    use datui::export_modal::ExportFormat;
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_ext_unknown.csv");

    press(&mut app, KeyCode::Char('e'));
    app.export_modal.selected_format = ExportFormat::Parquet;
    for ch in "out.dat".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    assert_eq!(app.export_modal.selected_format, ExportFormat::Parquet);
}

/// Enter applies from anywhere in the export form — there is no button to
/// walk to — and Space still toggles the checkbox under the cursor.
#[test]
fn test_export_enter_applies_from_any_row() {
    use datui::export_modal::{ExportFocus, ExportFormat};
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_enter_anywhere.csv");

    press(&mut app, KeyCode::Char('e'));
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    let out = common::fixture_dir().join("export_enter_anywhere_out.csv");
    for ch in out.to_str().unwrap().chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    // Walk to the Header checkbox: Path → Delimiter → Header.
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.export_modal.focus, ExportFocus::CsvIncludeHeader);
    press(&mut app, KeyCode::Char(' '));
    assert!(!app.export_modal.csv_include_header, "Space toggles");

    match press(&mut app, KeyCode::Enter) {
        Some(AppEvent::Export(datui::ExportRequest {
            path,
            format,
            options,
            ..
        })) => {
            assert_eq!(format, ExportFormat::Csv);
            assert_eq!(path, out);
            assert!(
                !options.csv_include_header,
                "the toggled state reached the export"
            );
        }
        _ => panic!("Enter on a checkbox builds the export"),
    }
    assert!(
        !matches!(app.overlay, Overlay::Export { .. }),
        "and the dialog is gone"
    );
}

/// Export's Format is one row of its values: ← / → step along it and focus stays on
/// it, ↓ goes to the next field the format shows, the fields follow the format, and
/// a typed path's format extension follows too, where it names one.
#[test]
fn export_format_steps_along_its_row() {
    use datui::export_modal::{ExportFocus, ExportFormat};
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_format_row.csv");
    let format_row = |app: &mut App| {
        let area = Rect::new(0, 0, 100, 24);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = common::buffer_lines(&buf);
        let at = rows
            .iter()
            .position(|r| r.contains("Format:"))
            .expect("a Format row");
        (at, rows)
    };

    press(&mut app, KeyCode::Char('e'));
    for ch in "flights.csv".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Up);
    assert_eq!(app.export_modal.focus, ExportFocus::FormatSelector);
    let (at, rows) = format_row(&mut app);
    for format in ExportFormat::ALL {
        assert!(
            rows[at].contains(format.as_str()),
            "{format:?}: {}",
            rows[at]
        );
    }

    press(&mut app, KeyCode::Right);
    assert_eq!(app.export_modal.selected_format, ExportFormat::Tsv);
    assert_eq!(
        app.export_modal.focus,
        ExportFocus::FormatSelector,
        "→ stays"
    );
    assert_eq!(app.export_modal.path_input.value(), "flights.tsv");
    let (tsv_at, rows) = format_row(&mut app);
    assert_eq!(tsv_at, at, "the Format row holds its place");
    assert!(
        !rows.iter().any(|r| r.contains("Delimiter:")),
        "TSV says its own"
    );

    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.export_modal.selected_format, ExportFormat::Parquet);
    assert_eq!(app.export_modal.path_input.value(), "flights.parquet");
    let (parquet_at, rows) = format_row(&mut app);
    assert_eq!(parquet_at, at);
    for label in ["Header:", "Compression:"] {
        assert!(!rows.iter().any(|r| r.contains(label)), "Parquet: {label}");
    }
    // ↓ is the path, and the next ↓ wraps past the fields Parquet does not take.
    press(&mut app, KeyCode::Down);
    assert_eq!(app.export_modal.focus, ExportFocus::PathInput);
    press(&mut app, KeyCode::Down);
    assert_eq!(app.export_modal.focus, ExportFocus::FormatSelector);
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Left);
    assert_eq!(app.export_modal.selected_format, ExportFormat::Tsv);
    press(&mut app, KeyCode::Up);
    assert_eq!(
        app.export_modal.focus,
        ExportFocus::Compression,
        "↑ wraps to the last field TSV shows"
    );

    // An extension of the user's own stays as typed.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('e'));
    for ch in "flights.dat".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.export_modal.path_input.value(), "flights.dat");
}

/// A text field's history is Ctrl+P / Ctrl+N; ↑ and ↓ move between fields.
#[test]
fn ctrl_p_recalls_the_last_export_path_and_up_moves_on() {
    use datui::export_modal::ExportFocus;
    let (mut app, _rx, _tx) = open_query_filter_fixture("forms_export_history.csv");
    let out = common::fixture_dir().join("forms_export_history_out.csv");
    let _ = std::fs::remove_file(&out);

    press(&mut app, KeyCode::Char('e'));
    for ch in out.to_str().unwrap().chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    assert!(matches!(
        press(&mut app, KeyCode::Enter),
        Some(AppEvent::Export(_))
    ));

    press(&mut app, KeyCode::Char('e'));
    let suggested = app.export_modal.path_input.value().to_string();
    assert_eq!(suggested, "forms_export_history-export.csv");
    press(&mut app, KeyCode::Up);
    assert_eq!(
        app.export_modal.focus,
        ExportFocus::FormatSelector,
        "↑ leaves the field"
    );
    assert_eq!(
        app.export_modal.path_input.value(),
        suggested,
        "and recalls nothing"
    );
    press(&mut app, KeyCode::Down);
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('p'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(
        app.export_modal.path_input.value(),
        out.to_str().unwrap(),
        "Ctrl+P recalls the path exported to"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('n'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.export_modal.path_input.value(), suggested);
}

/// The export dialog is one Surface: one border, no bordered buttons, the
/// actions as chips in the footer, and no radio glyphs anywhere.
#[test]
fn test_export_modal_is_one_surface() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_one_surface.csv");
    press(&mut app, KeyCode::Char('e'));

    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = common::buffer_lines(&buf);

    assert!(
        rows.iter().any(|r| r.contains("Export Data")),
        "the dialog is up"
    );
    // One frame on screen means one border on the surface and none inside it.
    let frames = common::frame_bottoms(&rows);
    assert_eq!(frames.len(), 1, "exactly one border: {rows:#?}");
    let bottom = frames[0];
    for (key, label) in [("Enter", "Export"), ("Esc", "Cancel")] {
        assert!(
            rows[bottom - 1].contains(key) && rows[bottom - 1].contains(label),
            "{key} {label} is a chip on the footer row: {:?}",
            rows[bottom - 1]
        );
    }
    // The hand-rolled radio list is gone; the picker marks the selection with
    // the rail instead.
    let radios = [
        datui::glyphs::unicode().radio_on,
        datui::glyphs::unicode().radio_off,
        datui::glyphs::ascii().radio_on,
        datui::glyphs::ascii().radio_off,
    ];
    for radio in radios {
        assert!(
            rows.iter().all(|r| !r.contains(radio)),
            "a radio glyph survived: {radio:?}"
        );
    }
}

/// A destination with a cap, as the terminal path has, is sent text alone: no HTML
/// is built for it. A table copy over the cap is refused with the cap's message
/// and leaves the last copy in place; under it, the whole table arrives.
#[test]
fn test_a_capped_destination_gets_text_within_its_cap() {
    use datui::clipboard::{Accepts, Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capped(Arc<Mutex<Vec<Payload>>>, usize);
    impl Destination for Capped {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "terminal"
        }
        fn accepts(&self) -> Accepts {
            Accepts {
                html: false,
                base64_limit: Some(self.1),
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capped.csv");
    let mut csv = String::from("id,name\n");
    for i in 0..5_000 {
        csv.push_str(&format!("{i},name {i}\n"));
    }
    std::fs::write(&path, &csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    let area = Rect::new(0, 0, 120, 32);
    app.render(area, &mut Buffer::empty(area));
    pump_until_idle(&mut app, &rx, &tx);

    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capped(copies.clone(), 4 * 1024)));

    // The view scope: TSV with no HTML beside it.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    copy_scope(&mut app, datui::copy_modal::CopyScope::View);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    {
        let copies = copies.lock().unwrap();
        assert!(copies[0].text.starts_with("id\tname\n0\tname 0"));
        assert!(copies[0].html.is_none(), "no HTML for a capped destination");
    }

    // The whole table is about 60 KB, over a 4 KB cap: refused, nothing sent.
    let copy_table = |app: &mut App| {
        press_key(app, KeyCode::Char('y'), KeyModifiers::NONE);
        copy_scope(app, datui::copy_modal::CopyScope::Table);
        press_key(app, KeyCode::Enter, KeyModifiers::NONE);
        pump_until_idle(app, &rx, &tx);
    };
    copy_table(&mut app);
    let message = app.error_message().expect("refused out loud").to_string();
    assert!(
        message.contains("over 4 KB of base64") && message.contains("osc52_limit"),
        "{message}"
    );
    assert_eq!(copies.lock().unwrap().len(), 1, "the last copy stays");
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    // Under a 1 MB cap the whole table goes, as text alone.
    app.set_clipboard_destination(Box::new(Capped(copies.clone(), 1024 * 1024)));
    copy_table(&mut app);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let copies = copies.lock().unwrap();
    assert_eq!(copies.len(), 2);
    assert_eq!(copies[1].text, csv.trim_end().replace(',', "\t"));
    assert!(copies[1].html.is_none());
}

/// Copies and CSV and JSON exports write a date past the calendar as the table
/// shows it, where Polars' writers panicked; the rest as they always did.
#[test]
fn out_of_range_dates_copy_and_export_as_their_stored_number() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_out_of_range_dates(&dir);
    draw_wide(&mut app, "table");
    let copies: Copies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));
    press_through(&mut app, KeyCode::Char('y'));
    press_through(&mut app, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        copies.lock().unwrap().last().unwrap(),
        "1\t-2147483648 days since 1970-01-01\t-9223372036854775807 ms since 1970-01-01 UTC\t\
         -9223372036854775807 us since 1970-01-01 UTC\t1677-09-21T00:12:43.145224193\t\
         -9223372036854775807 us since 1970-01-01 UTC\t-PT9223372036854.775807S"
    );

    let out = dir.join("oor.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, false);
    assert_eq!(
        csv.lines().collect::<Vec<_>>(),
        [
            "id,d,t_ms,t_us,t_ns,t_tz,dur",
            "1,-2147483648 days since 1970-01-01,-9223372036854775807 ms since 1970-01-01 UTC,\
             -9223372036854775807 us since 1970-01-01 UTC,1677-09-21T00:12:43.145224193,\
             -9223372036854775807 us since 1970-01-01 UTC,-PT9223372036854.775807S",
            "2,1970-01-01,1970-01-01T00:00:00.000,1970-01-01T00:00:00.000000,\
             1970-01-01T00:00:00.000000000,1970-01-01T01:00:00.000000+0100,P0D",
            "3,2147483647 days since 1970-01-01,9223372036854775807 ms since 1970-01-01 UTC,\
             9223372036854775807 us since 1970-01-01 UTC,2262-04-11T23:47:16.854775807,\
             9223372036854775807 us since 1970-01-01 UTC,PT9223372036854.775807S",
        ]
    );

    let out = dir.join("oor.ndjson");
    export_as(
        &mut app,
        &rx,
        &tx,
        &out,
        datui::export_modal::ExportFormat::Ndjson,
        false,
    );
    assert_eq!(app.error_message(), None);
    let ndjson = std::fs::read_to_string(&out).unwrap();
    let lines: Vec<&str> = ndjson.lines().collect();
    assert_eq!(
        lines[1],
        r#"{"id":2,"d":"1970-01-01","t_ms":"1970-01-01 00:00:00","t_us":"1970-01-01 00:00:00","t_ns":"1970-01-01 00:00:00","t_tz":"1970-01-01T01:00:00+01:00","dur":"P0D"}"#
    );
    assert!(
        lines[2].contains(r#""t_tz":"9223372036854775807 us since 1970-01-01 UTC""#),
        "{ndjson}"
    );

    // A `by` query's lists, as JSON in a CSV cell.
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select t_us by id".to_string());
    pump_until_idle(&mut app, &rx, &tx);
    draw_wide(&mut app, "by");
    let csv = export_csv(&mut app, &rx, &tx, &dir.join("by.csv"), false);
    assert!(
        csv.contains(r#"1,"[""-9223372036854775807 us since 1970-01-01 UTC""]""#),
        "{csv}"
    );
}

/// `y`, the Python scope, Enter: the dialog copies the view's pipeline as a
/// script, filters, a sort over two columns and the columns shown included.
#[test]
fn test_copy_as_python_writes_the_view_as_a_script() {
    use datui::clipboard::{Destination, Payload};
    use datui::filter_modal::{FilterOperator, LogicalOperator};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let (mut app, rx, tx, dir) = open_python_fixture();
    {
        let state = app.data_table_state.as_mut().unwrap();
        state.filter(vec![
            python_filter("region", FilterOperator::Eq, "north", LogicalOperator::And),
            python_filter("qty", FilterOperator::Gt, "1", LogicalOperator::And),
        ]);
        state.sort_by(
            vec!["amount".to_string(), "order_id".to_string()],
            vec![true, false],
        );
        state.set_column_order(vec![
            "order_id".to_string(),
            "customer".to_string(),
            "amount".to_string(),
        ]);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);

    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));
    let key =
        |app: &mut App, code| app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    key(&mut app, KeyCode::Char('y'));
    copy_scope(&mut app, datui::copy_modal::CopyScope::Python);
    assert_eq!(app.copy_modal.row_order().len(), 1, "no format or header");
    key(&mut app, KeyCode::Enter);
    assert!(app.at_table());

    let path = dir.path().join("sales.csv");
    let expected = format!(
        "import polars as pl\n\
         \n\
         df = (\n    \
         pl.scan_csv({:?}, try_parse_dates=True)\n    \
         .filter((pl.col(\"region\") == \"north\") & (pl.col(\"qty\") > 1))\n    \
         .sort([\"amount\", \"order_id\"], descending=[True, False], nulls_last=True, maintain_order=True)\n    \
         .select([\"order_id\", \"customer\", \"amount\"])\n\
         )\n",
        path.display().to_string()
    );
    assert_eq!(copies.lock().unwrap()[0].text, expected);

    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("Copied the view as Python"),
        "no flash drawn"
    );
}

/// Every kind of step the script writes, run in Python: the rows are the ones
/// datui shows. Skipped where the project's virtualenv is missing.
#[test]
fn test_copy_as_python_scripts_compute_the_rows_datui_shows() {
    use datui::filter_modal::{FilterOperator, LogicalOperator};
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};

    type Build = Box<dyn Fn(&mut datui::table::DataTableState)>;
    let views: Vec<(&str, Build)> = vec![
        (
            "sidebar filters, an OR, a sort and the columns shown",
            Box::new(|s| {
                s.filter(vec![
                    python_filter("region", FilterOperator::Eq, "north", LogicalOperator::And),
                    python_filter("amount", FilterOperator::GtEq, "40", LogicalOperator::Or),
                    python_filter(
                        "customer",
                        FilterOperator::NotContains,
                        "d",
                        LogicalOperator::And,
                    ),
                ]);
                s.sort_by(vec!["qty".into(), "amount".into()], vec![false, true]);
                s.set_column_order(vec!["amount".into(), "order_id".into(), "qty".into()]);
            }),
        ),
        ("the natural order reversed", Box::new(|s| s.reverse())),
        (
            "a grouped query",
            Box::new(|s| {
                s.query(
                    "select total: sum amount, n: count qty, avg amount by region where qty > 1"
                        .into(),
                )
            }),
        ),
        (
            "a query of expressions and accessors",
            Box::new(|s| {
                s.query(
                    "select up: customer.upper, m: day.month, a: amount.round[1], \
                     b: 5 xbar order_id, w: qty mod 3, c: amount ^ 0 \
                     where customer like \"*d*\" | region in [\"east\", \"west\"], day >= 2024.02.01"
                        .into(),
                )
            }),
        ),
        (
            "a weighted average by a computed key, distinct",
            Box::new(|s| s.query("select distinct qty wavg amount by r: region.upper".into())),
        ),
        #[cfg(feature = "sql")]
        (
            "SQL grouped without an order",
            Box::new(|s| {
                s.sql_query(
                    "SELECT region, AVG(amount) AS avg_amount, COUNT(*) AS n FROM df GROUP BY region"
                        .into(),
                )
            }),
        ),
        (
            "a search, then a sort",
            Box::new(|s| {
                s.fuzzy_search("ad".into());
                s.sort_by(vec!["order_id".into()], vec![true]);
            }),
        ),
        (
            "a pivot of a filtered view, then a filter on the pivot",
            Box::new(|s| {
                s.filter(vec![python_filter(
                    "qty",
                    FilterOperator::Lt,
                    "5",
                    LogicalOperator::And,
                )]);
                s.pivot(&PivotSpec {
                    index: vec!["region".into()],
                    pivot_column: "customer".into(),
                    value_column: "amount".into(),
                    aggregation: PivotAggregation::Avg,
                })
                .unwrap();
                s.sort_by(vec!["region".into()], vec![true]);
            }),
        ),
        (
            "a count pivot, every other column the index",
            Box::new(|s| {
                s.set_column_order(vec!["region".into(), "qty".into(), "customer".into()]);
                s.query("select region, qty, customer".into());
                s.pivot(&PivotSpec {
                    index: Vec::new(),
                    pivot_column: "customer".into(),
                    value_column: "qty".into(),
                    aggregation: PivotAggregation::Count,
                })
                .unwrap();
            }),
        ),
        #[cfg(feature = "sql")]
        (
            "a melt, then SQL over it",
            Box::new(|s| {
                s.melt(&datui::pivot_melt_modal::MeltSpec {
                    index: vec!["order_id".into()],
                    value_columns: vec!["amount".into(), "qty".into()],
                    variable_name: "measure".into(),
                    value_name: "value".into(),
                })
                .unwrap();
                s.sql_query("SELECT * FROM df WHERE value > 3 ORDER BY order_id, measure".into());
            }),
        ),
        (
            "a drill into one value",
            Box::new(|s| {
                s.sort_by(vec!["amount".into()], vec![false]);
                s.drill_into_value("region", AnyValue::StringOwned("south".into()))
                    .unwrap();
            }),
        ),
        (
            "a drill into a group of a grouped query",
            Box::new(|s| {
                s.query("select total: sum amount by region, qty where qty > 2".into());
                s.drill_down_into_group(1).unwrap();
                s.sort_by(vec!["order_id".into()], vec![true]);
            }),
        ),
        #[cfg(feature = "sql")]
        (
            "a drill into a group of a SQL grouping",
            Box::new(|s| {
                s.sql_query("SELECT customer, SUM(qty) AS q FROM df GROUP BY customer".into());
                s.drill_down_into_group(2).unwrap();
            }),
        ),
    ];
    for (what, build) in views {
        let (mut app, rx, tx, _dir) = open_python_fixture();
        build(app.data_table_state.as_mut().unwrap());
        pump_until_idle(&mut app, &rx, &tx);
        assert!(
            app.data_table_state.as_ref().unwrap().error().is_none(),
            "{what}: {:?}",
            app.data_table_state.as_ref().unwrap().error()
        );
        let Some((rows, script)) = run_python_script(&app) else {
            eprintln!("skipped: no .venv to run the scripts with");
            return;
        };
        assert!(
            !script.contains("# "),
            "{what}: a step was not written:\n{script}"
        );
        assert_eq!(rows, view_csv(&app), "{what}:\n{script}");
    }
}

/// A CSV datui reads with `--infer-types` and stray spaces in its header: the
/// script trims the names and types the text columns as datui did.
#[test]
fn test_copy_as_python_reads_a_csv_as_datui_does() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("typed.csv");
    std::fs::write(
        &path,
        " id ,when,amount,code,at,stamp,note\n\
         1,03/15/2024, 12 ,007,10:30,2024-03-15 10:30:00, a \n\
         2,04/01/2024,3.5,010,11:45,2024-04-01 11:45:00,b\n\
         3,,  ,,,,\n\
         4,12/31/2023,-2,100,08:00,2023-12-31 08:00:00,  c\n",
    )
    .unwrap();
    for parse_strings in [true, false] {
        let options = OpenOptions {
            parse_strings: parse_strings.then_some(datui::ParseStringsTarget::All),
            ..OpenOptions::default()
        };
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], options);
        pump_until_idle(&mut app, &rx, &tx);
        let Some((rows, script)) = run_python_script(&app) else {
            eprintln!("skipped: no .venv to run the scripts with");
            return;
        };
        assert!(script.contains(".rename({\" id \": \"id\"})"), "{script}");
        assert_eq!(script.contains(".with_columns("), parse_strings, "{script}");
        assert_eq!(
            rows,
            view_csv(&app),
            "parse_strings={parse_strings}:\n{script}"
        );
    }
}

/// A logger's CSV with comment lines before its header and among its rows, read
/// with `--comment`: the script reads it with `comment_prefix` and computes
/// datui's rows.
#[test]
fn test_copy_as_python_skips_comment_lines_as_datui_does() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("logger.csv");
    std::fs::write(
        &path,
        "# logger 7\n# firmware 2.1\nt,v\n1,10\n# gap\n2,20\n3,30\n",
    )
    .unwrap();
    let options = OpenOptions {
        comment_char: Some("#".to_string()),
        ..OpenOptions::default()
    };
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    pump_until_idle(&mut app, &rx, &tx);
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    assert!(script.contains("comment_prefix=\"#\""), "{script}");
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// NDJSON with dates held as text: the script types them as datui did.
#[test]
fn test_copy_as_python_types_json_dates_as_datui_does() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(
        &path,
        "{\"id\": 1, \"day\": \"2024-03-15\", \"at\": \"2024-03-15T10:30:00\", \"what\": \"a\"}\n\
         {\"id\": 2, \"day\": \"2024-04-01\", \"at\": \"2024-04-01T11:45:00\", \"what\": \"b\"}\n",
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select id, day, at where day > 2024.03.20".into());
    pump_until_idle(&mut app, &rx, &tx);
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    assert!(script.contains("pl.scan_ndjson("), "{script}");
    assert!(script.contains(".str.to_date("), "{script}");
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// An Arrow IPC stream has no footer to scan: the script reads it whole.
#[test]
fn test_copy_as_python_reads_an_arrow_stream() {
    let python = Path::new(".venv/bin/python");
    if !python.exists() {
        eprintln!("skipped: no .venv to write the stream with");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data-00000-of-00001.arrow");
    let written = std::process::Command::new(python)
        .arg("-c")
        .arg(format!(
            "import polars as pl\n\
             pl.DataFrame({{'k': ['a', 'b', 'a'], 'v': [1, 2, 3]}}).write_ipc_stream({:?})",
            path.display().to_string()
        ))
        .status()
        .unwrap();
    assert!(written.success());
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select total: sum v by k".into());
    pump_until_idle(&mut app, &rx, &tx);
    let (rows, script) = run_python_script(&app).unwrap();
    assert!(script.contains("pl.read_ipc_stream("), "{script}");
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// A Hugging Face cache's script reads the split on screen, not every file; so does
/// a saved DatasetDict's.
#[test]
fn test_copy_as_python_reads_one_hugging_face_split() {
    common::ensure_sample_data();
    for (dir, read, not) in [
        ("hf_cache", "people-test.arrow", "people-train"),
        ("hf_dict", "test/data-00000", "train/"),
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        let options = OpenOptions {
            table: Some("test".to_string()),
            ..OpenOptions::default()
        };
        pump_open_until_loaded(
            &mut app,
            &rx,
            vec![PathBuf::from(format!("tests/sample-data/{dir}"))],
            options,
        );
        pump_until_idle(&mut app, &rx, &tx);
        let Some((rows, script)) = run_python_script(&app) else {
            eprintln!("skipped: no .venv to run the scripts with");
            return;
        };
        // The path as written in Python, Windows' `\\` read as `/`.
        let paths = script.replace("\\\\", "/");
        assert!(paths.contains(read), "{script}");
        assert!(!paths.contains(not), "{script}");
        assert_eq!(rows, view_csv(&app), "{script}");
    }
}

/// IPC files and streams together: the script reads each as datui did, scanning the
/// file and reading the stream whole, and stacks them as datui does.
#[test]
fn test_copy_as_python_reads_streams_beside_ipc_files() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/arrow_mixed")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    // Each file as the listing joined it, with the platform's separator.
    let file = |name: &str| {
        let path = PathBuf::from("tests/sample-data/arrow_mixed").join(name);
        format!("{:?}", path.display().to_string())
    };
    assert!(
        script.contains(&format!("pl.scan_ipc({})", file("a.arrow"))),
        "{script}"
    );
    assert!(
        script.contains(&format!("pl.read_ipc_stream({})", file("b.arrow"))),
        "{script}"
    );
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// A file known by its bytes rather than its name, an extensionless Parquet file:
/// Copy as Python reads it as Parquet and the export defaults to Parquet, from the
/// format the open read rather than the name.
#[test]
fn test_a_sniffed_file_copies_and_exports_as_the_format_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob");
    let mut df = df!("k" => ["a", "b", "a"], "v" => [1i64, 2, 3]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.read_as(), Some(datui::FileFormat::Parquet));
    let script = app.python_script(state);
    assert!(
        script.contains(&format!(
            "pl.scan_parquet({:?})",
            path.display().to_string()
        )),
        "{script}"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(matches!(app.overlay, Overlay::Export { .. }));
    assert_eq!(
        app.export_modal.selected_format,
        datui::export_modal::ExportFormat::Parquet
    );
    if let Some((rows, script)) = run_python_script(&app) {
        assert_eq!(rows, view_csv(&app), "{script}");
    }
}

/// The query language's `/` and `%` floor-divide two whole numbers, as Polars' `/`
/// on two expressions does: the script writes `//` there and `/` where a float
/// takes part, so its rows are datui's, negatives and a zero divisor included.
#[test]
fn test_copy_as_python_divides_integers_as_datui_does() {
    let python = Path::new(".venv/bin/python");
    if !python.exists() {
        eprintln!("skipped: no .venv to write the file with");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ints.parquet");
    let written = std::process::Command::new(python)
        .arg("-c")
        .arg(format!(
            "import polars as pl\n\
             pl.DataFrame({{\n\
             'k': ['x', 'y', 'x', 'y', 'x', 'y', 'x'],\n\
             'a': pl.Series([7, -7, 7, -7, 5, None, 0], dtype=pl.Int64),\n\
             'b': pl.Series([2, 2, -2, -2, 0, 3, 4], dtype=pl.Int32),\n\
             'f': [2.0, 2.0, -2.0, -2.0, 0.5, 3.0, 4.0],\n\
             }}).write_parquet({:?})",
            path.display().to_string()
        ))
        .status()
        .unwrap();
    assert!(written.success());
    for query in [
        "select k, q: a / b, r: a % b, m: a mod b, n: -a / b, t: a / f, h: a / 2",
        "select k, a, b where (a / b) < 0",
        "select q: sum a / sum b, t: sum a / sum f by k",
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        app.data_table_state
            .as_mut()
            .unwrap()
            .query(query.to_string());
        pump_until_idle(&mut app, &rx, &tx);
        assert!(
            app.data_table_state.as_ref().unwrap().error().is_none(),
            "{query}: {:?}",
            app.data_table_state.as_ref().unwrap().error()
        );
        let (rows, script) = run_python_script(&app).unwrap();
        assert!(
            script.contains(" // "),
            "{query}: no floor division\n{script}"
        );
        assert_eq!(rows, view_csv(&app), "{query}:\n{script}");
    }
}

/// Names and values with quotes, backslashes, line breaks, triple quotes and
/// non-ASCII text: the script is valid Python that computes datui's rows, and a
/// name that reads as code in a comment stays in the comment.
#[test]
fn test_copy_as_python_escapes_names_and_values() {
    use datui::filter_modal::{FilterOperator, LogicalOperator};
    let python = Path::new(".venv/bin/python");
    if !python.exists() {
        eprintln!("skipped: no .venv to write the file with");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("odd names.parquet");
    let written = std::process::Command::new(python)
        .arg("-c")
        .arg(
            r#"import sys, datetime, polars as pl
pl.DataFrame({
    'na"me': ['O\'Brien "x"', 'C:\\dir\\', 'line1\nline2', '"""', '日本'],
    'pa\\th': [1, 2, 3, 4, 5],
    'multi\nline': ['a', 'b', 'a', 'b', 'a'],
    'when\nraise SystemExit(3)': [datetime.datetime(2024, 1, d) for d in range(1, 6)],
}).write_parquet(sys.argv[1])"#,
        )
        .arg(&path)
        .status()
        .unwrap();
    assert!(written.success());
    type Build = Box<dyn Fn(&mut datui::table::DataTableState)>;
    let views: Vec<(&str, Build)> = vec![
        (
            "filters, a sort and the columns shown",
            Box::new(|s| {
                s.filter(vec![python_filter(
                    "na\"me",
                    FilterOperator::NotContains,
                    "\\",
                    LogicalOperator::And,
                )]);
                s.sort_by(vec!["pa\\th".into()], vec![true]);
                s.set_column_order(vec!["multi\nline".into(), "na\"me".into()]);
            }),
        ),
        (
            "a drill into a value with a line break and quotes",
            Box::new(|s| {
                s.drill_into_value("na\"me", AnyValue::StringOwned("line1\nline2".into()))
                    .unwrap();
            }),
        ),
        #[cfg(feature = "sql")]
        (
            "SQL over lines, ending in a quoted name",
            Box::new(|s| {
                s.sql_query(
                    "SELECT \"na\"\"me\", \"multi\nline\"\nFROM df\nORDER BY \"na\"\"me\"".into(),
                );
            }),
        ),
    ];
    for (what, build) in views {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        build(app.data_table_state.as_mut().unwrap());
        pump_until_idle(&mut app, &rx, &tx);
        assert!(
            app.data_table_state.as_ref().unwrap().error().is_none(),
            "{what}: {:?}",
            app.data_table_state.as_ref().unwrap().error()
        );
        let (rows, script) = run_python_script(&app).unwrap();
        assert!(
            !script.contains("# "),
            "{what}: a step was not written:\n{script}"
        );
        assert_eq!(rows, view_csv(&app), "{what}:\n{script}");
    }

    // A drill into a timestamp is a comment, and the column's name in it is text.
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let first = 1_704_067_200_000_000; // 2024-01-01 in microseconds
    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_into_value(
            "when\nraise SystemExit(3)",
            AnyValue::Datetime(first, TimeUnit::Microseconds, None),
        )
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    let (_, script) = run_python_script(&app).unwrap();
    assert!(
        script.lines().all(|l| !l.trim_start().starts_with("raise")),
        "{script}"
    );
    assert!(script.contains("when\\nraise SystemExit(3)"), "{script}");
}

/// Copy as Python reads a file named like a glob as that file too: the script's
/// scan matches it alone, in each format the script scans (#625).
#[test]
fn copy_as_python_reads_a_file_named_like_a_glob_as_itself() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for ext in ["csv", "parquet", "arrow", "jsonl"] {
        let literal = tmp.path().join(format!("d[1].{ext}"));
        write_marker(&literal, "literal");
        write_marker(&tmp.path().join(format!("d1.{ext}")), "sibling");
        let (app, _) = open_and_collect(vec![literal], OpenOptions::default());
        let Some((rows, script)) = run_python_script(&app) else {
            return;
        };
        assert_eq!(rows, "v\nliteral\n", "{ext}:\n{script}");
        // Read with `glob=False` where the scan has the flag; NDJSON's has not, so
        // its name is escaped instead.
        let (plain, escaped) = (format!("d[1].{ext}\""), format!("d[[]1[]].{ext}\""));
        if ext == "jsonl" {
            assert!(script.contains(&escaped), "{ext}:\n{script}");
            assert!(!script.contains("glob=False"), "{ext}:\n{script}");
        } else {
            assert!(script.contains(&plain), "{ext}:\n{script}");
            assert!(script.contains(", glob=False"), "{ext}:\n{script}");
        }
    }
}

/// A directory named like a glob is read through a pattern over its files, with its
/// own name escaped: the script reads it, not the directory its name matches (#632).
#[test]
fn copy_as_python_reads_a_directory_named_like_a_glob() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let literal = tmp.path().join("d[1]");
    let sibling = tmp.path().join("d1");
    std::fs::create_dir(&literal).unwrap();
    std::fs::create_dir(&sibling).unwrap();
    write_marker(&literal.join("a.csv"), "literal");
    write_marker(&sibling.join("a.csv"), "sibling");
    let (app, df) = open_and_collect(vec![literal], OpenOptions::default());
    assert_eq!(marker_values(&df), ["literal"]);
    let Some((rows, script)) = run_python_script(&app) else {
        return;
    };
    assert_eq!(rows, "v\nliteral\n", "{script}");
    assert!(script.contains("d[[]1[]]/*.csv\""), "{script}");
    assert!(!script.contains("glob=False"), "{script}");
}

/// A saved view keeps each retype as the inline table a spec's `[columns]` entry is,
/// and puts it back on the next file; one whose column is gone is left out with a
/// note. An export writes the type.
#[test]
fn a_retype_is_saved_in_a_view_and_exported() {
    let name = "view_retype";
    let next = common::fixture_dir().join(format!("{name}_next.csv"));
    std::fs::write(&next, "id,code,gone\n4,40,c\n5,50,d\n").unwrap();
    let (mut app, rx, tx) = open_csv_with(
        &format!("{name}_first.csv"),
        "id,code,gone\n1,10,a\n2,20,b\n",
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    let ty = |name: &str, format: Option<&str>| {
        datui::column_types::ColumnType::named(name, format.map(String::from)).unwrap()
    };
    {
        let state = app.data_table_state.as_mut().unwrap();
        state.set_column_type("code", Some(ty("u16", None)));
        state.set_column_type("gone", Some(ty("str", None)));
    }
    pump_until_idle(&mut app, &rx, &tx);

    let parquet = common::fixture_dir().join(format!("{name}.parquet"));
    export_as(
        &mut app,
        &rx,
        &tx,
        &parquet,
        datui::export_modal::ExportFormat::Parquet,
        false,
    );
    let written = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(&parquet).unwrap(),
        Default::default(),
    )
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(written.column("code").unwrap().dtype(), &DataType::UInt16);

    let view = app
        .create_view_from_current_state(
            name.to_string(),
            None,
            datui::view::MatchCriteria {
                exact_path: Some(next.clone()),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
                table: None,
            },
        )
        .unwrap();
    let json = serde_json::to_string(&view.settings.columns).unwrap();
    assert_eq!(
        json,
        r#"[{"name":"code","type":"u16"},{"name":"gone","type":"str"}]"#
    );
    assert_eq!(
        view.settings.columns[0].to_toml(),
        r#"code = { type = "u16" }"#
    );

    pump_open_until_loaded(&mut app, &rx, vec![next], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    app.event(key(KeyCode::Char('V')));
    pump_until_idle(&mut app, &rx, &tx);
    let df = view_frame(&app);
    assert_eq!(df.column("code").unwrap().dtype(), &DataType::UInt16);
    assert_eq!(df.column("gone").unwrap().dtype(), &DataType::String);

    // A step on a column this data does not have is left out, with a note.
    let third = common::fixture_dir().join(format!("{name}_third.csv"));
    std::fs::write(&third, "id,code\n6,60\n").unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![third], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_mut().unwrap();
    let dropped = state.set_column_changes(&view.settings.columns);
    assert_eq!(dropped, ["gone"]);
    assert_eq!(state.retyped_columns(), ["code"]);
    let notes = summaries(&app);
    assert!(
        notes.contains(&"view steps left out, no such column: gone".to_string()),
        "{notes:?}"
    );
}

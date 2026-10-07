use super::*;

#[test]
fn a_metadata_line_parses_into_pairs_and_a_title() {
    let m = parse_metadata(r#"device_info, log_version="1.03", model="X, Y", serial=123,"#);
    assert_eq!(m.title.as_deref(), Some("device_info"));
    assert_eq!(
        m.pairs,
        [
            ("log_version".to_string(), "1.03".to_string()),
            ("model".to_string(), "X, Y".to_string()),
            ("serial".to_string(), "123".to_string()),
        ]
    );
}

#[test]
fn a_line_that_does_not_parse_is_kept_raw() {
    for line in [
        "just some words",
        "a=1, stray",
        r#"a="open"#,
        "=1",
        "title only",
    ] {
        let m = parse_metadata(line);
        assert!(m.pairs.is_empty(), "{line}: {m:?}");
        assert_eq!(m.raw, line.trim());
    }
}

fn spec() -> Delimited {
    Delimited {
        comment_char: Some("#".into()),
        header_rows: Some(HeaderRows {
            name: vec![3],
            unit: Some(2),
        }),
        metadata_line: Some(1),
        ..Default::default()
    }
}

#[test]
fn units_follow_the_shown_names_and_the_metadata_line_is_read() {
    let text =
        "#device_info, version=\"2\"\n#yyyy-mm-dd, hh:mm, volts,\n  Date, Time,  Volts, Volts,\n";
    let HeadFacts { units, metadata } = spec().facts(text.as_bytes(), b',', " ").unwrap();
    assert_eq!(
        units,
        [
            ("Date".to_string(), "yyyy-mm-dd".to_string()),
            ("Time".to_string(), "hh:mm".to_string()),
            ("Volts".to_string(), "volts".to_string()),
        ]
    );
    let metadata = metadata.unwrap();
    assert_eq!(metadata.title.as_deref(), Some("device_info"));
    assert_eq!(metadata.pairs, [("version".to_string(), "2".to_string())]);
}

#[test]
fn a_derived_column_that_takes_a_name_does_not_take_its_unit() {
    let text = "#m=1\nyyyy-mm-dd, hh:mm\nDate, Time\n";
    let spec = Delimited {
        columns: vec![Derived {
            name: "Time".into(),
            from: vec!["Date".into(), "Time".into()],
            kind: DerivedKind::Datetime,
            format: None,
        }],
        ..spec()
    };
    let facts = spec.facts(text.as_bytes(), b',', " ").unwrap();
    assert_eq!(
        facts.units,
        [("Date".to_string(), "yyyy-mm-dd".to_string())]
    );
}

#[test]
fn check_names_the_file_and_why_it_could_not_be_read() {
    let dir = tempfile::tempdir().unwrap();
    let short = dir.path().join("short.csv");
    std::fs::write(&short, "a,b\n1,2\n").unwrap();
    let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = 3";
    let spec = Arc::new(Spec::parse(text, None).unwrap());
    for (file, said) in [
        (
            short,
            "short.csv\": Header line 3 is past the end of the file.",
        ),
        (
            dir.path().join("none.csv"),
            "none.csv\": File or directory not found.",
        ),
    ] {
        let e = check(&spec, Some(&file), 5, &crate::OpenOptions::default()).unwrap_err();
        assert!(e.contains(said), "{e}");
    }
}

/// A flag typed on the command line keeps its value; the rest come from the spec
/// (#651).
#[test]
fn apply_leaves_typed_flags() {
    let mut options = crate::OpenOptions {
        delimiter: Some(b','),
        comment_char: Some("%".into()),
        typed_dialect: crate::TypedDialect {
            delimiter: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let spec = Delimited {
        delimiter: Some(b';'),
        ..spec()
    };
    spec.apply(&mut options);
    assert_eq!(options.delimiter, Some(b','), "typed");
    assert_eq!(options.comment_char.as_deref(), Some("#"), "not typed");
}

#[test]
fn apply_sets_the_dialect_and_skips_every_header_line() {
    let mut options = crate::OpenOptions::default();
    let spec = Delimited {
        header_rows: Some(HeaderRows {
            name: vec![2],
            unit: Some(3),
        }),
        null_values: vec!["NA".into()],
        ..spec()
    };
    spec.apply(&mut options);
    assert_eq!(options.header_rows, [2]);
    assert_eq!(options.skip_lines, Some(3));
    assert_eq!(options.comment_char.as_deref(), Some("#"));
    assert_eq!(options.null_values, Some(vec!["NA".to_string()]));
    assert_eq!(options.format, Some(crate::FileFormat::Csv));
}

#[test]
fn a_datetime_from_date_time_and_offset_is_utc() {
    let df = df!(
        "d" => [Some("2024-03-05"), None, Some("2024-03-05")],
        "t" => [Some("14:03:22"), None, Some("23:30:00")],
        "o" => [Some("-05:00"), None, Some("+0530")],
        "v" => [1, 2, 3],
    )
    .unwrap();
    let spec = Delimited {
        columns: vec![Derived {
            name: "time".into(),
            from: vec!["d".into(), "t".into(), "o".into()],
            kind: DerivedKind::Datetime,
            format: None,
        }],
        ..Default::default()
    };
    let out = spec.derive(df.lazy()).unwrap().collect().unwrap();
    assert_eq!(
        out.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        ["time", "d", "t", "o", "v"],
        "before its first source"
    );
    let time = out.column("time").unwrap();
    assert_eq!(
        time.dtype(),
        &DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
    );
    let shown: Vec<String> = (0..3).map(|i| time.get(i).unwrap().to_string()).collect();
    assert_eq!(shown[0], "2024-03-05 19:03:22 UTC");
    assert_eq!(shown[1], "null");
    assert_eq!(shown[2], "2024-03-05 18:00:00 UTC");
}

/// Every way a delimited spec or the file it reads is refused names the file, in
/// the one shape; a spec's problem with its line and column.
#[test]
fn errors_name_the_file() {
    use crate::formats::readers::bad_input::assert_shape;
    let dir = tempfile::tempdir().unwrap();
    let spec_path = dir.path().join("log.toml");
    let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = \"three\"\n";
    let e = Spec::parse(text, Some(&spec_path)).unwrap_err().to_string();
    eprintln!("{e}");
    assert_shape(&e, &spec_path);
    assert!(e.contains(":3:"), "{e}");

    let data = dir.path().join("a.csv");
    std::fs::write(&data, "a\n1\n").unwrap();
    let spec = Delimited {
        columns: vec![Derived {
            name: "day".into(),
            from: vec!["date".into()],
            kind: DerivedKind::Date,
            format: None,
        }],
        ..Default::default()
    };
    let lf = LazyCsvReader::new(PlRefPath::new(data.to_string_lossy().as_ref()))
        .finish()
        .unwrap();
    let Err(e) = spec.derive(lf) else {
        panic!("a missing column is an error");
    };
    let e = crate::error_display::user_message_from_report(&e.into(), Some(&data));
    eprintln!("{e}");
    assert_shape(&e, &data);
    assert!(e.contains("\"date\""), "{e}");

    let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = 3";
    let spec = Arc::new(Spec::parse(text, None).unwrap());
    let read = DelimitedRead::chosen(spec, Chosen::SpecFile, Vec::new());
    for file in [data.clone(), dir.path().join("none.csv")] {
        let e = read_facts(
            &read,
            std::slice::from_ref(&file),
            &crate::OpenOptions::default(),
        )
        .unwrap_err();
        let e = crate::error_display::user_message_from_report(&e, Some(&file));
        eprintln!("{e}");
        assert_shape(&e, &file);
    }
}

#[test]
fn a_missing_source_column_is_named() {
    let df = df!("a" => [1]).unwrap();
    let spec = Delimited {
        columns: vec![Derived {
            name: "day".into(),
            from: vec!["date".into()],
            kind: DerivedKind::Date,
            format: None,
        }],
        ..Default::default()
    };
    let Err(err) = spec.derive(df.lazy()) else {
        panic!("a missing column is an error");
    };
    let err = err.to_string();
    assert!(
        err.contains("\"day\" is made from \"date\", which the file has no column of"),
        "{err}"
    );
}

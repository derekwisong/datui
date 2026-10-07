use crate::*;
use polars::io::cloud::CloudOptions;

/// The details pane says why a bucket directory's `(all files)` row reads nothing
/// by the rule Enter applies, so it is never shown for one Enter would read.
#[test]
fn the_door_says_why_only_when_enter_would_read_nothing() {
    let door = |formats: &[(&str, usize)], not_read| {
        let mut entry = discover::Entry::directory(Path::new("s3://b/dir/"));
        entry.opens_whole_directory = true;
        entry.holds = discover::Holds {
            formats: formats.iter().map(|(f, n)| (f.to_string(), *n)).collect(),
            not_read,
            ..Default::default()
        };
        entry
    };
    let why = App::why_a_door_reads_nothing(&door(&[("tsv", 2)], 0));
    assert!(why.is_some_and(|why| why.contains("2 tsv")));
    assert!(App::why_a_door_reads_nothing(&door(&[], 3)).is_some());
    assert_eq!(App::why_a_door_reads_nothing(&door(&[("csv", 3)], 0)), None);
    assert_eq!(
        App::why_a_door_reads_nothing(&door(&[("parquet", 3)], 0)),
        None
    );
    let mut hive = door(&[], 3);
    hive.kind = discover::EntryKind::Hive;
    assert_eq!(App::why_a_door_reads_nothing(&hive), None);
}

/// A CSV prefix in a bucket is read with the flags the user gave, not Polars'
/// defaults. Driven through a local glob, which is the same reader with the object
/// store swapped for the filesystem.
#[test]
fn a_csv_prefix_is_read_with_the_users_flags() {
    let dir = tempfile::tempdir().unwrap();
    let preamble = "exported by x\nid;name\n1;NA\n2;bob\n3;FOOTER\n";
    std::fs::write(dir.path().join("a.csv"), preamble).unwrap();
    let glob = format!("{}/*.csv", dir.path().display());
    let options = OpenOptions {
        delimiter: Some(b';'),
        skip_lines: Some(1),
        skip_tail_rows: Some(1),
        null_values: Some(vec!["NA".into()]),
        ..OpenOptions::default()
    };
    let df = App::scan_cloud_prefix(
        &glob,
        CloudOptions::default(),
        FileFormat::Csv,
        true,
        &options,
    )
    .expect("a CSV reader")
    .unwrap()
    .collect()
    .unwrap();
    let names: Vec<_> = df
        .get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(names, ["id", "name"]);
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("name").unwrap().null_count(), 1);

    // Global and per-column null values together read the columns from the
    // prefix itself.
    let options = OpenOptions {
        null_values: Some(vec!["NA".into(), "name=bob".into()]),
        ..options
    };
    let df = App::scan_cloud_prefix(
        &glob,
        CloudOptions::default(),
        FileFormat::Csv,
        true,
        &options,
    )
    .expect("a CSV reader")
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(df.column("name").unwrap().null_count(), 1);
    assert_eq!(df.column("id").unwrap().null_count(), 0);
}

/// A CSV prefix keeps timestamps as text rather than risk a read that fails on
/// them: Polars' date inference takes `2024-01-01 10:00:00 UTC`, as BigQuery
/// exports it, for a datetime and then cannot parse it.
#[test]
fn a_csv_prefix_keeps_timestamps_as_text() {
    let dir = tempfile::tempdir().unwrap();
    let csv = "z,bq\n2013-01-01T10:00:00Z,2024-01-01 10:00:00 UTC\n\
               2013-01-01T11:00:00.5Z,2024-01-02 11:30:15 UTC\n";
    std::fs::write(dir.path().join("a.csv"), csv).unwrap();
    let glob = format!("{}/*.csv", dir.path().display());
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let df = App::scan_cloud_prefix(
        &glob,
        CloudOptions::default(),
        FileFormat::Csv,
        true,
        &options,
    )
    .expect("a CSV reader")
    .unwrap()
    .collect()
    .expect("the read succeeds");
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("z").unwrap().dtype(), &DataType::String);
    assert_eq!(df.column("bq").unwrap().dtype(), &DataType::String);
}

/// With one source, `local`, whose endpoint refuses every connection: a browse
/// lists where it lands, and nothing here leaves the machine.
fn new_app() -> App {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut config = AppConfig::default();
    config.cloud.connections = vec![crate::config::CloudConnectionConfig {
        name: "local".to_string(),
        kind: Some("s3".to_string()),
        endpoint_url: Some("http://127.0.0.1:9".to_string()),
        ..Default::default()
    }];
    App::new_with_config(
        tx,
        crate::tests::test_runtime(),
        Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    )
}

/// A cloud directory on the command line is listed before it is opened; a file, a
/// glob or `--format` is opened as named.
#[test]
fn a_cloud_directory_named_on_the_command_line_is_looked_at_first() {
    let named = |path: &str, options: OpenOptions| {
        App::route_named_paths(vec![PathBuf::from(path)], options)
    };
    for directory in ["s3://local@b/census/data/", "s3://local@b/census"] {
        assert!(matches!(
            named(directory, OpenOptions::default()),
            AppEvent::LookThenOpenDirectory(..)
        ));
    }
    let csv = OpenOptions {
        format: Some(FileFormat::Csv),
        ..OpenOptions::default()
    };
    for (path, options) in [
        ("s3://local@b/census/data/test.csv", OpenOptions::default()),
        ("s3://local@b/census/**/*.csv", OpenOptions::default()),
        ("s3://local@b/census/data/", csv),
    ] {
        assert!(matches!(named(path, options), AppEvent::Open(..)), "{path}");
    }
}

/// What the listing found decides it, as it does for the `(all files)` row.
#[test]
fn a_cloud_directory_opens_as_its_listing_says() {
    use discover::{EntryKind, Holds};
    let dir = PathBuf::from("s3://local@b/census/data");
    let holding = |formats: &[(&str, usize)], directories| Holds {
        formats: formats.iter().map(|(f, n)| (f.to_string(), *n)).collect(),
        directories,
        ..Holds::default()
    };

    // CSV files: read as CSV, as a prefix.
    let mut app = new_app();
    let csv = holding(&[("csv", 3)], 0);
    let Some(AppEvent::Open(paths, options)) = app.open_the_directory_looked_at(
        dir.clone(),
        EntryKind::Directory,
        Some(&csv),
        OpenOptions::default(),
    ) else {
        panic!("a directory of CSV opens");
    };
    assert_eq!(paths, [PathBuf::from("s3://local@b/census/data/")]);
    assert_eq!(options.format, Some(FileFormat::Csv));

    // Only directories: browsed.
    let mut app = new_app();
    let subdirectories = holding(&[], 1);
    assert!(
        app.open_the_directory_looked_at(
            dir.clone(),
            EntryKind::Directory,
            Some(&subdirectories),
            OpenOptions::default(),
        )
        .is_none()
    );
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.browsing.as_deref(), Some(dir.as_path()));

    // A listing that was refused: opened as named, so the error is the store's.
    let mut app = new_app();
    assert!(matches!(
        app.open_the_directory_looked_at(
            dir.clone(),
            EntryKind::Unknown,
            None,
            OpenOptions::default(),
        ),
        Some(AppEvent::Open(paths, _)) if paths == [dir.clone()]
    ));
}

/// A folder marker listed beside the files is not read as one of them. Polars
/// refused the whole prefix over it: "different file extensions".
#[test]
fn a_folder_marker_in_a_csv_prefix_is_not_read() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), "id\n1\n").unwrap();
    std::fs::write(dir.path().join("b.csv"), "id\n2\n").unwrap();
    std::fs::write(dir.path().join("data"), "placeholder").unwrap();
    let prefix = format!("{}/", dir.path().display());
    let df = App::scan_cloud_prefix(
        &prefix,
        CloudOptions::default(),
        FileFormat::Csv,
        true,
        &OpenOptions::default(),
    )
    .expect("a CSV reader")
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(df.height(), 2);
}

/// `az://container/path` names no account: the open takes it from the one Azure
/// connection in the config and goes on as `abfss://`, which opens like any Azure
/// object. With no account anywhere it is refused, saying how to name one.
#[test]
fn an_az_url_opens_as_abfss_or_says_which_account_is_missing() {
    let mut app = new_app();
    app.app_config
        .cloud
        .connections
        .push(crate::config::CloudConnectionConfig {
            name: "lake".to_string(),
            kind: Some("azure".to_string()),
            account: Some("lake001".to_string()),
            ..Default::default()
        });
    let open = |app: &mut App, url: &str| {
        app.event(&AppEvent::Open(
            vec![PathBuf::from(url)],
            OpenOptions::default(),
        ))
    };
    match open(&mut app, "az://raw/2024/day.parquet") {
        Some(AppEvent::Open(paths, _)) => assert_eq!(
            paths,
            [PathBuf::from(
                "abfss://raw@lake001.dfs.core.windows.net/2024/day.parquet"
            )]
        ),
        _ => panic!("expanded to abfss://"),
    }
    assert!(matches!(
        source::input_source(Path::new(
            "abfss://raw@lake001.dfs.core.windows.net/2024/day.parquet"
        )),
        source::InputSource::Azure(_)
    ));

    let accountless = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_err()
        && std::env::var("AZURE_STORAGE_CONNECTION_STRING").is_err();
    if accountless {
        let mut app = new_app();
        match open(&mut app, "az://raw/day.parquet") {
            Some(AppEvent::Crash(message)) => assert!(
                message.contains("abfss://raw@<account>.dfs.core.windows.net/day.parquet"),
                "{message}"
            ),
            _ => panic!("refused"),
        }
    }
}

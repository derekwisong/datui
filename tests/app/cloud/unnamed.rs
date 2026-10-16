//! An object whose name says no format opens as the same file on disk would: its
//! bytes are sniffed, Parquet is read in place, and anything else is downloaded and
//! told apart there (delimited text, lines, or hex).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::config::AppConfig;
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;

use crate::common::next_event;
use crate::fake_s3::FakeS3;

/// What an open showed.
#[derive(Debug, PartialEq)]
enum Shown {
    Table {
        columns: Vec<String>,
        rows: Option<usize>,
    },
    Hex {
        len: u64,
        name: String,
    },
}

/// Open `path`, saying Yes to a download if asked, and say what it showed and whether
/// it asked.
fn open(path: &str, config: AppConfig, dir: &std::path::Path) -> (Shown, bool) {
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel::<AppEvent>();
    let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
    let options = OpenOptions {
        temp_dir: Some(dir.to_path_buf()),
        ..OpenOptions::default()
    };
    let mut asked = false;
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(path)], options));
    loop {
        while let Some(event) = next.take() {
            next = app.event(event);
        }
        if app.awaiting_open_confirmation() {
            asked = true;
            next = Some(AppEvent::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )));
            continue;
        }
        match next_event(&mut app, &rx) {
            Some(event) => next = Some(event),
            None => break,
        }
    }
    assert_eq!(app.error_message(), None, "{path} opens");
    let shown = match app.hex_view() {
        Some(view) => Shown::Hex {
            len: view.len(),
            name: view.name(),
        },
        None => {
            let state = app.data_table_state.as_ref().expect("a dataset");
            Shown::Table {
                columns: state
                    .schema()
                    .iter_names()
                    .filter(|name| !name.starts_with("__datui"))
                    .map(|name| name.to_string())
                    .collect(),
                rows: state.num_rows_if_valid(),
            }
        }
    };
    (shown, asked)
}

/// `bytes` as a file named `file` on disk and as the key `u/7d6d/file` in a bucket.
fn both(bucket: &str, bytes: &[u8]) -> (Shown, (Shown, bool)) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("file");
    std::fs::write(&path, bytes).unwrap();
    let (local, _) = open(&path.to_string_lossy(), AppConfig::default(), dir.path());

    let s3 = FakeS3::serve(
        bucket,
        BTreeMap::from([("u/7d6d/file".to_string(), bytes.to_vec())]),
    );
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let downloads = tempfile::tempdir().unwrap();
    let cloud = open(
        &format!("s3://{bucket}/u/7d6d/file"),
        config,
        downloads.path(),
    );
    (local, cloud)
}

#[test]
fn an_unnamed_object_opens_as_the_same_file_on_disk() {
    let mut df = df!("id" => [1i64, 2, 3], "name" => ["a", "b", "c"]).unwrap();
    let mut parquet = Vec::new();
    ParquetWriter::new(&mut parquet).finish(&mut df).unwrap();
    let binary: Vec<u8> = (0..4096u32).map(|i| (i * 7 % 256) as u8).collect();
    let cases: [(&str, &[u8], bool); 4] = [
        ("unnamed-csv", b"id,name\n1,a\n2,b\n", true),
        ("unnamed-lines", b"started\nstopped\n", true),
        ("unnamed-bytes", &binary, true),
        ("unnamed-parquet", &parquet, false),
    ];
    for (bucket, bytes, downloaded) in cases {
        let (local, (cloud, asked)) = both(bucket, bytes);
        assert_eq!(local, cloud, "{bucket}: the object opens as the file does");
        assert_eq!(
            asked, downloaded,
            "{bucket}: downloaded only when not Parquet"
        );
    }
}

/// A prefix named without its slash is no object: it is still read in place.
#[test]
fn a_prefix_named_without_its_slash_is_still_read_in_place() {
    let mut df = df!("id" => [1i64, 2, 3]).unwrap();
    let mut parquet = Vec::new();
    ParquetWriter::new(&mut parquet).finish(&mut df).unwrap();
    let s3 = FakeS3::serve(
        "unnamed-prefix",
        BTreeMap::from([("data/part-0.parquet".to_string(), parquet)]),
    );
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let downloads = tempfile::tempdir().unwrap();
    let (shown, asked) = open("s3://unnamed-prefix/data", config, downloads.path());
    assert_eq!(
        shown,
        Shown::Table {
            columns: vec!["id".to_string()],
            rows: Some(3),
        }
    );
    assert!(!asked, "nothing downloaded");
}

//! One dataset, opened from a local directory and from a bucket, comes out the same:
//! the schema, the partitions, the count, the notes, what the footer pass read and
//! what a reopen reads (#713). Both routes are the one open over `DatasetFiles`; this
//! is what holds them to it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc;

use datui::config::AppConfig;
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;

use crate::common::pump_open_until_loaded;
use crate::fake_s3::FakeS3;

/// What one open of a dataset showed, and what opening it again cost.
#[derive(Debug, PartialEq)]
struct Outcome {
    columns: Vec<(String, DataType)>,
    partitions: Vec<String>,
    rows: Option<usize>,
    notes: Vec<String>,
    /// Footers read by the open, the pass behind it and the count, in all.
    footers_read: Option<usize>,
    reopen_rows: Option<usize>,
    /// Footers a second open read: none, since the first kept the shape.
    reopen_footers_read: Option<usize>,
    /// The second open had every footer, and nothing was left to read behind it.
    reopen_whole: bool,
}

fn parquet(mut df: DataFrame) -> Vec<u8> {
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
    bytes
}

/// Open `path` in a fresh app and say what it showed.
fn open(path: &str, config: &AppConfig) -> (Option<usize>, Outcome) {
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel::<AppEvent>();
    let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config.clone());
    let options = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from(path)], options);
    assert_eq!(app.error_message(), None, "{path} opens");
    let state = app.data_table_state.as_ref().expect("a dataset");
    let meter = state.measurements();
    let outcome = Outcome {
        columns: state
            .schema()
            .iter()
            .filter(|(name, _)| !name.starts_with("__datui"))
            .map(|(name, dtype)| (name.to_string(), dtype.clone()))
            .collect(),
        partitions: state.partition_columns().unwrap_or_default().to_vec(),
        rows: state.num_rows_if_valid(),
        notes: state.notes().into_iter().map(|n| n.summary).collect(),
        footers_read: meter.footers().and_then(|c| c.files),
        reopen_rows: None,
        reopen_footers_read: None,
        reopen_whole: false,
    };
    (state.footers_pending().is_none().then_some(1), outcome)
}

/// Open the dataset twice from where it is, and fold the second into the first.
fn open_twice(path: &str, config: &AppConfig) -> Outcome {
    let (_, mut first) = open(path, config);
    let (whole, again) = open(path, config);
    first.reopen_rows = again.rows;
    first.reopen_footers_read = again.footers_read;
    first.reopen_whole = whole.is_some();
    first
}

/// The same objects as files under a temporary directory and as keys in `bucket`, each
/// opened twice. The bucket is named for the test: what an open keeps is filed by URL.
fn both(bucket: &str, objects: &[(String, Vec<u8>)]) -> (Outcome, Outcome) {
    let dir = tempfile::tempdir().unwrap();
    for (key, bytes) in objects {
        let path = dir.path().join("data").join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
    }
    let local = open_twice(
        &dir.path().join("data").to_string_lossy(),
        &AppConfig::default(),
    );

    let s3 = FakeS3::serve(
        bucket,
        objects
            .iter()
            .map(|(key, bytes)| (format!("data/{key}"), bytes.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..AppConfig::default()
    };
    let cloud = open_twice(&format!("s3://{bucket}/data/"), &config);
    (local, cloud)
}

#[track_caller]
fn same(bucket: &str, objects: &[(String, Vec<u8>)]) -> Outcome {
    let (mut local, cloud) = both(bucket, objects);
    // The one difference: within a wave a directory's footers cost one round of reads,
    // as a stat of every file would, so it is not stat'ed. A reopen reads them again,
    // and an empty file is found by trying its footer, where a bucket's listing gives
    // its size. What the bucket read is then the test's to say.
    let empty = objects
        .iter()
        .filter(|(key, bytes)| bytes.is_empty() && key.ends_with(".parquet"))
        .count();
    if local
        .footers_read
        .is_some_and(|n| n <= datui::formats::schema_union::FOOTERS_AT_ONCE)
    {
        assert_eq!(
            local.reopen_footers_read, local.footers_read,
            "a small directory is read again"
        );
        assert_eq!(
            local.footers_read,
            cloud.footers_read.map(|n| n + empty),
            "and tries the footer of each empty file once"
        );
        local.reopen_footers_read = cloud.reopen_footers_read;
        local.footers_read = cloud.footers_read;
    }
    assert_eq!(local, cloud, "a directory and a bucket open the same");
    local
}

fn rows(day: usize, n: i64) -> DataFrame {
    df!(
        "id" => (0..n).collect::<Vec<i64>>(),
        "amount" => (0..n).map(|i| i as f64 * day as f64).collect::<Vec<f64>>(),
    )
    .unwrap()
}

/// Within one wave: every footer is read by the open, and the newest file's column
/// leads. What the listing passed over is noted the same from both.
#[test]
fn a_small_partitioned_dataset_opens_the_same_from_a_disk_or_a_bucket() {
    let mut objects: Vec<(String, Vec<u8>)> = (1..=3)
        .map(|day| {
            (
                format!("date=2024-01-0{day}/part-0.parquet"),
                parquet(rows(day, 4)),
            )
        })
        .collect();
    let mut newest = rows(4, 2);
    newest
        .with_column(Column::new("note".into(), ["a", "b"]))
        .unwrap();
    objects.push(("date=2024-01-04/part-0.parquet".into(), parquet(newest)));
    objects.push(("_SUCCESS".into(), Vec::new()));
    objects.push(("date=2024-01-02/stray.csv".into(), b"a,b\n1,2\n".to_vec()));

    let outcome = same("small", &objects);
    assert_eq!(outcome.rows, Some(14));
    assert_eq!(outcome.partitions, ["date"]);
    assert_eq!(
        outcome
            .columns
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>(),
        ["date", "id", "amount", "note"]
    );
    assert_eq!(outcome.footers_read, Some(4), "each footer once");
    assert_eq!(
        outcome.reopen_footers_read, None,
        "a small prefix is not read again"
    );
    assert!(outcome.reopen_whole);
    assert!(
        outcome.notes.iter().any(|n| n.contains("not Parquet")),
        "{:?}",
        outcome.notes
    );
}

/// Past one wave: the two ends open the dataset and the rest are read behind it,
/// each footer once; a column only a middle file has joins; a reopen reads none.
#[test]
fn a_dataset_past_one_wave_opens_the_same_from_a_disk_or_a_bucket() {
    let files = 80;
    let objects: Vec<(String, Vec<u8>)> = (0..files)
        .map(|i| {
            let mut frame = rows(i, 3);
            if i == files / 2 {
                frame
                    .with_column(Column::new("oops".into(), [true, false, true]))
                    .unwrap();
            }
            (
                format!("region={}/part-{i:03}.parquet", i % 4),
                parquet(frame),
            )
        })
        .collect();

    // Past a wave the directory is stat'ed, which finds the empty file the listing does.
    let mut objects = objects;
    objects.push(("region=1/part-empty.parquet".into(), Vec::new()));
    let outcome = same("wave", &objects);
    assert!(
        outcome.notes.iter().any(|n| n.contains("1 empty file")),
        "{:?}",
        outcome.notes
    );
    assert_eq!(outcome.rows, Some(files * 3));
    assert_eq!(outcome.partitions, ["region"]);
    assert!(outcome.columns.iter().any(|(n, _)| n == "oops"), "joined");
    assert_eq!(
        outcome.footers_read,
        Some(files),
        "the ends by the open and the rest behind it, none twice"
    );
    assert_eq!(outcome.reopen_rows, Some(files * 3));
    assert_eq!(outcome.reopen_footers_read, None);
    assert!(outcome.reopen_whole);
}

/// A file that will not read, and one with nothing in it, are left out the same way
/// and noted the same way, and a dataset with one is not remembered as though whole.
#[test]
fn a_dataset_with_a_broken_file_opens_the_same_from_a_disk_or_a_bucket() {
    let mut objects: Vec<(String, Vec<u8>)> = (0..3)
        .map(|i| (format!("part-{i}.parquet"), parquet(rows(i, 5))))
        .collect();
    objects.push(("part-9.parquet".into(), b"not parquet at all".to_vec()));
    // A write that stopped: a data file's name over nothing.
    objects.push(("part-8.parquet".into(), Vec::new()));

    let outcome = same("broken", &objects);
    assert_eq!(outcome.rows, Some(15), "the readable files' rows");
    assert_eq!(outcome.partitions, Vec::<String>::new());
    assert_eq!(
        outcome.reopen_footers_read,
        Some(4),
        "not remembered: a footer that would not read may read next time"
    );
    assert!(
        outcome
            .notes
            .iter()
            .any(|n| n.contains("1 file unreadable")),
        "{:?}",
        outcome.notes
    );
    assert!(
        outcome.notes.iter().any(|n| n.contains("1 empty file")),
        "{:?}",
        outcome.notes
    );
}

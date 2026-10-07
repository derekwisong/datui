use super::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as OsPath};
use polars::prelude::*;
use std::sync::Arc;

/// One row of Parquet with the given columns.
fn parquet(columns: &[&str]) -> Vec<u8> {
    let mut frame = DataFrame::new(
        1,
        columns
            .iter()
            .map(|c| Column::new((*c).into(), &[1i32]))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes).finish(&mut frame).unwrap();
    bytes
}

/// Put `files` in a store and ask what the directory is.
fn kind_of(files: &[(&str, &[&str])]) -> Option<crate::discover::EntryKind> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    rt.block_on(async {
        let mut objects = Vec::new();
        for (key, columns) in files {
            let bytes = parquet(columns);
            objects.push((format!("data/{key}"), bytes.len() as u64));
            store
                .put(
                    &OsPath::from(format!("data/{key}")),
                    PutPayload::from(bytes),
                )
                .await
                .unwrap();
        }
        kind_from_footers(&store, &objects).await
    })
}

/// The shape that prompted this, as it sits in a bucket: one file per table.
#[test]
fn separate_tables_in_a_bucket_are_a_directory() {
    let kind = kind_of(&[
        ("circuits.parquet", &["circuit_id", "lat", "lng"]),
        ("drivers.parquet", &["driver_id", "code", "nationality"]),
        ("laps.parquet", &["lap", "position", "time_millis"]),
    ]);
    assert_eq!(kind, Some(crate::discover::EntryKind::Directory));
}

#[test]
fn parts_of_one_table_stay_one_dataset() {
    let kind = kind_of(&[
        ("part-00000.parquet", &["id", "ts", "amount"]),
        ("part-00001.parquet", &["id", "ts", "amount"]),
        ("part-00002.parquet", &["id", "ts", "amount"]),
    ]);
    assert_eq!(kind, Some(crate::discover::EntryKind::MultiFile));
}

/// A dataset that gained columns over the years is still one dataset, and the
/// files read are its ends, which is where the difference is.
#[test]
fn a_dataset_that_gained_columns_stays_one_dataset() {
    let kind = kind_of(&[
        ("date=2009-01-03.parquet", &["id", "ts"]),
        ("date=2015-06-01.parquet", &["id", "ts", "fee"]),
        (
            "date=2025-06-01.parquet",
            &["id", "ts", "fee", "witness", "address"],
        ),
    ]);
    assert_eq!(kind, Some(crate::discover::EntryKind::MultiFile));
}

/// Nothing readable means nothing decided, and the listing's answer stands.
#[test]
fn a_directory_that_cannot_be_read_is_left_as_it_was() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let objects = vec![
        ("data/a.parquet".to_string(), 10),
        ("data/b.parquet".to_string(), 10),
    ];
    assert_eq!(rt.block_on(kind_from_footers(&store, &objects)), None);
}

/// One file cannot disagree with anything.
#[test]
fn a_single_file_decides_nothing() {
    assert_eq!(kind_of(&[("only.parquet", &["a", "b"])]), None);
}

/// The ends of a listing are where a table-per-file directory differs; its head can
/// be three files of the same table by alphabetical accident.
#[test]
fn the_files_read_span_the_listing() {
    use crate::discover::spread;
    assert_eq!(spread(1), vec![0]);
    assert_eq!(spread(2), vec![0, 1]);
    assert_eq!(spread(3), vec![0, 1, 2]);
    assert_eq!(spread(15), vec![0, 7, 14]);
}

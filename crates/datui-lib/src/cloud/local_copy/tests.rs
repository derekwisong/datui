use super::*;
use polars::prelude::*;

/// Two Parquet files under `dir`, rows 0..10 and 10..20, in a `part=` directory
/// each so the copy has to keep hive paths working.
fn files(dir: &Path) -> Vec<RemoteObject> {
    (0..2)
        .map(|part| {
            let mut df =
                df!("id" => ((part * 10)..(part * 10 + 10)).collect::<Vec<i64>>()).unwrap();
            let path = dir.join(format!("part={part}")).join("data.parquet");
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            ParquetWriter::new(std::fs::File::create(&path).unwrap())
                .finish(&mut df)
                .unwrap();
            let size = std::fs::metadata(&path).unwrap().len();
            RemoteObject {
                url: format!("s3://lake/events/part={part}/data.parquet"),
                size,
                etag: None,
            }
        })
        .collect()
}

/// The URL's bytes from the "bucket" under `source`.
fn bytes_of(source: &Path, url: &str) -> Vec<u8> {
    let key = url.trim_start_matches("s3://lake/events/");
    std::fs::read(source.join(key)).unwrap()
}

fn remote_scan(urls: &[String]) -> LazyFrame {
    let sources = ScanSources::Paths(urls.iter().map(PlRefPath::new).collect());
    let args = polars::lazy::dsl::UnifiedScanArgs {
        hive_options: polars::io::HiveOptions::new_enabled(),
        ..Default::default()
    };
    DslBuilder::scan_parquet(sources, Default::default(), args)
        .unwrap()
        .build()
        .into()
}

/// A fetched copy serves a remote plan, filters and aggregates included, with the
/// same rows and the hive column; no remote path is left in the plan.
#[test]
fn a_copy_reads_as_the_remote_scan_would() {
    let source = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = files(source.path());
    let copy = LocalCopy::fetch(
        root.path(),
        &objects,
        &ReadWatch::default(),
        |object, write| {
            for chunk in bytes_of(source.path(), &object.url).chunks(7) {
                write(chunk)?;
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(copy.objects(), 2);
    assert_eq!(
        copy.bytes(),
        objects.iter().map(|object| object.size).sum::<u64>()
    );

    let urls = objects
        .iter()
        .map(|object| object.url.clone())
        .collect::<Vec<_>>();
    let remote = remote_scan(&urls)
        .filter(col("id").gt(lit(3)))
        .group_by([col("part")])
        .agg([len().alias("rows")])
        .sort(["part"], Default::default());
    // A frame asked for its schema holds its plan as IR, wrapping the plan it
    // came from.
    let remote: LazyFrame = DslPlan::IR {
        dsl: Arc::new(remote.logical_plan),
        version: 0,
        node: None,
        opt_flags: None,
    }
    .into();
    let local = copy.redirect(&remote).expect("every object is in the copy");
    assert!(scan_paths(&local).iter().all(|path| !is_remote(path)));
    let df = local.collect().unwrap();
    assert_eq!(df.column("rows").unwrap().u32().unwrap().get(0), Some(6));
    assert_eq!(df.column("rows").unwrap().u32().unwrap().get(1), Some(10));
    assert_eq!(df.height(), 2, "the partition column survives the copy");
}

#[test]
fn a_plan_reading_an_object_not_copied_is_left_alone() {
    let source = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = files(source.path());
    let copy = LocalCopy::fetch(
        root.path(),
        &objects[..1],
        &ReadWatch::default(),
        |object, write| write(&bytes_of(source.path(), &object.url)),
    )
    .unwrap();
    let urls = objects
        .iter()
        .map(|object| object.url.clone())
        .collect::<Vec<_>>();
    assert!(copy.redirect(&remote_scan(&urls)).is_none());
}

/// A cancel between chunks and a failed object both leave nothing behind.
#[test]
fn a_stopped_or_failed_fetch_leaves_no_files() {
    let source = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = files(source.path());
    let entries = || std::fs::read_dir(root.path()).unwrap().count();

    let stop = ReadWatch::default();
    let stopped = LocalCopy::fetch(root.path(), &objects, &stop, |object, write| {
        let bytes = bytes_of(source.path(), &object.url);
        write(&bytes[..10])?;
        stop.stop();
        write(&bytes[10..])
    });
    assert!(stopped.is_err());
    assert_eq!(entries(), 0, "the partial copy is gone");

    let failed = LocalCopy::fetch(
        root.path(),
        &objects,
        &ReadWatch::default(),
        |object, write| {
            if object.url.contains("part=1") {
                return Err(eyre!("404"));
            }
            write(&bytes_of(source.path(), &object.url))
        },
    );
    assert!(failed.is_err());
    assert_eq!(entries(), 0, "the first object went with it");

    let short = LocalCopy::fetch(
        root.path(),
        &objects,
        &ReadWatch::default(),
        |object, write| write(&bytes_of(source.path(), &object.url)[1..]),
    );
    assert!(short.unwrap_err().to_string().contains("changed"));
    assert_eq!(entries(), 0);
}

/// A finished copy stays while held and goes when dropped; a sweep removes a
/// copy no one holds and keeps one that is held.
#[test]
fn a_copy_lives_while_held_and_a_sweep_clears_orphans() {
    let source = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let objects = files(source.path());
    let fetch = || {
        LocalCopy::fetch(
            root.path(),
            &objects,
            &ReadWatch::default(),
            |object, write| write(&bytes_of(source.path(), &object.url)),
        )
        .unwrap()
    };
    let held = fetch();
    let dir = held.dir().to_path_buf();
    // Copies left by sessions that died: their locks went with the processes. One
    // is as new as a live session's copy before it takes its lock.
    let orphan = |name: &str, age: u64| {
        let dir = root.path().join(name);
        std::fs::create_dir_all(dir.join("lake")).unwrap();
        let lock = std::fs::File::create(dir.join(HELD)).unwrap();
        let then = std::time::SystemTime::now() - std::time::Duration::from_secs(age);
        lock.set_modified(then).unwrap();
        dir
    };
    let old = orphan("copy-old", 2 * UNHELD_GRACE.as_secs());
    let new = orphan("copy-new", 0);
    sweep(root.path());
    assert!(dir.exists(), "a held copy stays");
    assert!(!old.exists(), "an orphan goes");
    assert!(new.exists(), "one too new to judge stays");
    drop(held);
    assert!(!dir.exists(), "dropped, the copy is removed");
}

/// Two keys that land on one file, as on a disk that ignores case, fail the
/// fetch rather than one overwriting the other.
#[test]
fn two_objects_never_share_a_file() {
    let source = tempfile::tempdir().unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut objects = files(source.path());
    objects[1].url = objects[0].url.replace("s3://lake/", "s3://lake//");
    let error = LocalCopy::fetch(
        root.path(),
        &objects,
        &ReadWatch::default(),
        // Asked only for the first: the second has nowhere to go.
        |object, write| write(&bytes_of(source.path(), &object.url)),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("quality_local_copy = 0"),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn urls_map_to_their_bucket_and_key() {
    assert_eq!(
        relative_path("s3://lake/events/region=North/part-0.parquet"),
        PathBuf::from("lake/events/region=North/part-0.parquet")
    );
    assert_eq!(
        relative_path("gs://b/../x.parquet"),
        PathBuf::from("b/_/x.parquet")
    );
    assert_eq!(
        relative_path("s3:///../../etc/passwd"),
        PathBuf::from("_/_/etc/passwd")
    );
    assert_eq!(safe_component("at=12:00", false), "at=12:00");
    assert_eq!(safe_component("at=12:00", true), "at=12%3A00");
    assert_eq!(safe_component(r"a\b", true), "a%5Cb");
}

#[test]
fn etags_compare_unquoted() {
    assert!(same_etag("\"abc\"", "abc"));
    assert!(same_etag("W/\"abc\"", "\"abc\""));
    assert!(!same_etag("\"abc\"", "\"abd\""));
}

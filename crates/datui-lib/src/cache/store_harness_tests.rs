use super::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What a kind's checks store: two values of one encoded size, and one larger than
/// any budget a check sets.
trait Sample: Kind + Send + Sync + 'static {
    fn sample(variant: u8) -> Self::Value;
    fn big() -> Self::Value;
}

impl Sample for Shapes {
    fn sample(variant: u8) -> DatasetShape {
        DatasetShape {
            fingerprint: "fp".into(),
            files: vec![CachedFooter {
                schema: Some(0),
                row_group_rows: vec![usize::from(variant)],
                row_group_bytes: vec![100],
                column_bytes: vec![8],
            }],
            schemas: vec![vec![("id".into(), polars::prelude::DataType::Int64)]],
            taken_at: 1,
        }
    }
    fn big() -> DatasetShape {
        let mut shape = Self::sample(1);
        shape.files = vec![shape.files[0].clone(); 2_000];
        shape
    }
}

impl Sample for Facts {
    fn sample(variant: u8) -> DatasetFacts {
        DatasetFacts {
            mtime: 1,
            size: 2,
            rows: Some(usize::from(variant)),
            cols: Some(1),
            columns: vec!["a".into()],
            ..Default::default()
        }
    }
    fn big() -> DatasetFacts {
        DatasetFacts {
            columns: (0..2_000).map(|i| format!("column_{i}")).collect(),
            ..Self::sample(1)
        }
    }
}

impl Sample for CloudListings {
    fn sample(variant: u8) -> CloudListing {
        CloudListing {
            fingerprint: "fp".into(),
            buckets: vec![format!("bucket-{variant}")],
            listed_at: 1,
        }
    }
    fn big() -> CloudListing {
        CloudListing {
            buckets: (0..2_000).map(|i| format!("bucket-{i}")).collect(),
            ..Self::sample(1)
        }
    }
}

fn encoded<K: Kind>(value: &K::Value) -> Vec<u8> {
    K::encode(value).unwrap()
}

fn same<K: Kind>(a: Option<K::Value>, b: &K::Value) -> bool {
    a.is_some_and(|a| encoded::<K>(&a) == encoded::<K>(b))
}

fn store<K: Kind>() -> (Store<K>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (
        Store::new(&CacheManager::with_dir(dir.path().to_path_buf())),
        dir,
    )
}

fn age(file: &Path, secs: u64) {
    fs::OpenOptions::new()
        .write(true)
        .open(file)
        .unwrap()
        .set_modified(UNIX_EPOCH + Duration::from_secs(secs))
        .unwrap();
}

fn modified(file: &Path) -> SystemTime {
    fs::metadata(file).unwrap().modified().unwrap()
}

fn round_trip<K: Sample>() {
    let (store, _dir) = store::<K>();
    let value = K::sample(1);
    store.put("s3://b/a/", "fp", &value);
    assert!(same::<K>(store.get("s3://b/a/", "fp"), &value), "a hit");
    assert!(
        store.get("s3://b/a/", "moved").is_none(),
        "another fingerprint"
    );
    assert!(store.get("s3://b/z/", "fp").is_none(), "another key");
    let scanned = store.scan();
    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].0, "s3://b/a/");
    // A key whose hash collides costs only its own entry: the frame holds the key.
    let other = K::sample(2);
    fs::copy(store.file("s3://b/a/"), store.file("s3://b/z/")).unwrap();
    assert!(
        store.get("s3://b/z/", "fp").is_none(),
        "a collision is a miss"
    );
    store.put("s3://b/a/", "fp", &other);
    assert!(same::<K>(store.get("s3://b/a/", "fp"), &other), "replaced");
}

fn damage_is_a_miss<K: Sample>() {
    let (store, _dir) = store::<K>();
    store.put("k", "fp", &K::sample(1));
    let file = store.file("k");
    let good = fs::read(&file).unwrap();
    for cut in 0..good.len() {
        fs::write(&file, &good[..cut]).unwrap();
        assert!(store.get("k", "fp").is_none(), "cut at {cut}");
    }
    for at in 0..good.len() {
        for bit in 0..8 {
            let mut bad = good.clone();
            bad[at] ^= 1 << bit;
            fs::write(&file, &bad).unwrap();
            assert!(store.get("k", "fp").is_none(), "bit {bit} of byte {at}");
        }
    }
    assert!(store.scan().is_empty(), "nor does a scan see it");
    fs::write(&file, &good).unwrap();
    assert!(
        store.get("k", "fp").is_some(),
        "and the good bytes still read"
    );
}

fn evicts_by_bytes_in_lru_order<K: Sample>() {
    let (store, _dir) = store::<K>();
    for (i, key) in ["k/a", "k/b", "k/c"].into_iter().enumerate() {
        store.put(key, "fp", &K::sample(1));
        age(&store.file(key), 1_000 + i as u64);
    }
    let one = fs::metadata(store.file("k/a")).unwrap().len();
    // `a` is the oldest, but using it makes `b` the one used longest ago.
    assert!(store.get("k/a", "fp").is_some());

    let store = store.with_budget(3 * one);
    store.put("k/d", "fp", &K::sample(1));
    assert_eq!(store.len(), 3, "kept to its budget in bytes");
    assert!(store.get("k/b", "fp").is_none(), "b went");
    for kept in ["k/a", "k/c", "k/d"] {
        assert!(store.get(kept, "fp").is_some(), "{kept} stayed");
    }

    // One entry larger than the whole budget is still kept: it is the one in use.
    store.put("k/huge", "fp", &K::big());
    assert_eq!(store.len(), 1, "everything else made room");
    assert!(store.get("k/huge", "fp").is_some());
}

fn a_hit_only_touches<K: Sample>() {
    let (store, _dir) = store::<K>();
    store.put("k/a", "fp", &K::sample(1));
    store.put("k/b", "fp", &K::sample(1));
    let (a, b) = (store.file("k/a"), store.file("k/b"));
    age(&a, 1_000);
    age(&b, 1_000);
    let before = fs::read(&a).unwrap();

    assert!(store.get("k/a", "fp").is_some());
    assert!(
        modified(&a) > UNIX_EPOCH + Duration::from_secs(1_000),
        "dated now"
    );
    assert_eq!(fs::read(&a).unwrap(), before, "and not rewritten");
    assert_eq!(
        modified(&b),
        UNIX_EPOCH + Duration::from_secs(1_000),
        "b untouched"
    );

    age(&a, 1_000);
    assert!(store.get("k/a", "moved").is_none());
    assert_eq!(
        modified(&a),
        UNIX_EPOCH + Duration::from_secs(1_000),
        "a miss is not use"
    );
    assert_eq!(store.scan().len(), 2);
    assert_eq!(
        modified(&a),
        UNIX_EPOCH + Duration::from_secs(1_000),
        "nor is a scan"
    );

    // Storing what is already there dates it, and writes nothing.
    let inode = fs::metadata(&a).unwrap();
    store.put("k/a", "fp", &K::sample(1));
    assert!(modified(&a) > UNIX_EPOCH + Duration::from_secs(1_000));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(fs::metadata(&a).unwrap().ino(), inode.ino(), "not replaced");
    }
    let _ = inode;
}

fn sweeps_stale_temp_files<K: Sample>() {
    let (store, _dir) = store::<K>();
    store.put("k/a", "fp", &K::sample(1));
    let stale = store.dir().join("x.shape.1.0.tmp");
    let fresh = store.dir().join("x.shape.1.1.tmp");
    fs::write(&stale, b"half").unwrap();
    fs::write(&fresh, b"half").unwrap();
    age(&stale, 1_000);
    store.put("k/b", "fp", &K::sample(1));
    assert!(!stale.exists(), "a dead writer's temp file goes");
    assert!(fresh.exists(), "a live writer's stays");
}

fn clear_all_removes_it<K: Sample>() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CacheManager::with_dir(dir.path().to_path_buf());
    let store = Store::<K>::new(&cache);
    store.put("k", "fp", &K::sample(1));
    assert!(store.get("k", "fp").is_some());
    cache.clear_all().unwrap();
    assert!(store.get("k", "fp").is_none());
    assert!(!store.dir().exists(), "the kind's directory is gone");
    assert!(
        cache.cache_file(&format!("{}.lock", K::DIR)).exists(),
        "its lock, which another instance may hold, is not"
    );
}

fn two_writers_race<K: Sample>() {
    let (store, _dir) = store::<K>();
    let store = std::sync::Arc::new(store);
    let writers: Vec<_> = (1..=2u8)
        .map(|variant| {
            let store = store.clone();
            std::thread::spawn(move || {
                for i in 0..25 {
                    store.put("shared", "fp", &K::sample(variant));
                    store.put(&format!("own/{variant}/{i}"), "fp", &K::sample(variant));
                    assert!(store.get("shared", "fp").is_some(), "never torn");
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    let shared = encoded::<K>(&store.get("shared", "fp").unwrap());
    assert!(
        shared == encoded::<K>(&K::sample(1)) || shared == encoded::<K>(&K::sample(2)),
        "one writer's value, whole"
    );
    assert_eq!(store.len(), 51, "every entry landed");
    let temps = fs::read_dir(store.dir())
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
        .count();
    assert_eq!(temps, 0, "and no temp file is left");
}

macro_rules! suite {
    ($name:ident, $kind:ty) => {
        mod $name {
            use super::*;
            #[test]
            fn round_trip() {
                super::round_trip::<$kind>();
            }
            #[test]
            fn damage_is_a_miss() {
                super::damage_is_a_miss::<$kind>();
            }
            #[test]
            fn evicts_by_bytes_in_lru_order() {
                super::evicts_by_bytes_in_lru_order::<$kind>();
            }
            #[test]
            fn a_hit_only_touches() {
                super::a_hit_only_touches::<$kind>();
            }
            #[test]
            fn sweeps_stale_temp_files() {
                super::sweeps_stale_temp_files::<$kind>();
            }
            #[test]
            fn clear_all_removes_it() {
                super::clear_all_removes_it::<$kind>();
            }
            #[test]
            fn two_writers_race() {
                super::two_writers_race::<$kind>();
            }
        }
    };
}

suite!(shapes, Shapes);
suite!(facts, Facts);
suite!(cloud_listings, CloudListings);

/// The files earlier builds wrote are removed, not read.
#[test]
fn the_old_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let cache = CacheManager::with_dir(dir.path().to_path_buf());
    let old = [
        "datasets.json",
        "dataset_shapes.json",
        "cloud_sources.json",
        "visits.json",
    ];
    for name in old {
        fs::write(dir.path().join(name), b"{}").unwrap();
    }
    fs::create_dir(dir.path().join("dataset_shapes")).unwrap();
    fs::write(dir.path().join("dataset_shapes/0.shape"), b"x").unwrap();
    cache.record_dataset_facts(&[(PathBuf::from("/d"), Facts::sample(1))]);
    for name in old {
        assert!(!dir.path().join(name).exists(), "{name}");
    }
    assert!(!dir.path().join("dataset_shapes").exists());
}

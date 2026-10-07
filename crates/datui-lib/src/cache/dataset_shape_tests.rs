use super::*;

fn shape(fingerprint: &str, taken_at: u64) -> DatasetShape {
    DatasetShape {
        fingerprint: fingerprint.to_string(),
        files: vec![
            CachedFooter {
                schema: Some(0),
                row_group_rows: vec![10],
                row_group_bytes: vec![1_000],
                column_bytes: Vec::new(),
            },
            CachedFooter {
                schema: Some(0),
                row_group_rows: vec![20],
                row_group_bytes: vec![2_000],
                column_bytes: Vec::new(),
            },
        ],
        schemas: vec![vec![("id".into(), polars::prelude::DataType::Int64)]],
        taken_at,
    }
}

/// A payload that frames fine but does not hold together is refused, never a
/// panic: a schema index past the table, or totals that overflow.
#[test]
fn a_shape_that_does_not_hold_together_is_refused() {
    let good = shape("fp", 1);
    assert_eq!(
        decode_shape(&encode_shape(&good).unwrap()),
        Some(good.clone())
    );
    let mut wild = good.clone();
    wild.files[0].schema = Some(5);
    assert!(decode_shape(&encode_shape(&wild).unwrap()).is_none());
    let mut huge = good.clone();
    huge.files[0].row_group_rows = vec![usize::MAX, 1];
    assert!(decode_shape(&encode_shape(&huge).unwrap()).is_none());
    let bytes = encode_shape(&good).unwrap();
    for cut in 0..bytes.len() {
        assert!(decode_shape(&bytes[..cut]).is_none(), "cut {cut}");
    }
}

/// A dataset that has not changed is remembered; one that has is not.
///
/// The whole cache turns on this: the listing is cheap and happens anyway, and what
/// it fingerprints decides whether thousands of footer reads can be skipped. An
/// entry returned for a dataset that has moved on would seat the wrong row counts
/// under the right name, which is worse than reading the footers again.
#[test]
fn a_shape_comes_back_only_for_the_dataset_it_was_taken_from() {
    let dir = tempfile::tempdir().unwrap();
    let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
    cache.save_dataset_shape("s3://b/events/", shape("2-30-abc", 100));

    assert_eq!(
        cache.dataset_shape("s3://b/events/", "2-30-abc"),
        Some(shape("2-30-abc", 100)),
        "the same dataset, unchanged"
    );
    assert_eq!(
        cache.dataset_shape("s3://b/events/", "3-40-def"),
        None,
        "a file added, removed or rewritten since"
    );
    assert_eq!(
        cache.dataset_shape("s3://b/other/", "2-30-abc"),
        None,
        "and a different dataset that happens to weigh the same"
    );
}

/// The fingerprint sees everything a listing can see, and nothing it cannot.
#[test]
fn the_fingerprint_moves_when_the_files_do() {
    let base = DatasetShape::fingerprint_of([
        ("a.parquet", 100, 7, Some("e1")),
        ("b.parquet", 200, 8, Some("e2")),
    ]);
    assert_eq!(
        base,
        DatasetShape::fingerprint_of([
            ("a.parquet", 100, 7, Some("e1")),
            ("b.parquet", 200, 8, Some("e2")),
        ]),
        "the same listing twice is the same fingerprint"
    );
    for (changed, why) in [
        (vec![("a.parquet", 100, 7, Some("e1"))], "a file removed"),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, Some("e2")),
                ("c.parquet", 50, 9, Some("e3")),
            ],
            "a file added",
        ),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 201, 8, Some("e2")),
            ],
            "a file resized",
        ),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 9, Some("e2")),
            ],
            "a file rewritten, which the stamp catches",
        ),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, Some("e9")),
            ],
            "rewritten within the same second at the same length: only the tag sees it",
        ),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("b.parquet", 200, 8, None),
            ],
            "a tag gone",
        ),
        (
            vec![
                ("a.parquet", 100, 7, Some("e1")),
                ("renamed.parquet", 200, 8, Some("e2")),
            ],
            "a file renamed, which reorders the positional join the cache is",
        ),
    ] {
        assert_ne!(base, DatasetShape::fingerprint_of(changed), "{why}");
    }
}

/// The same in every build: a file named or fingerprinted by a hash that moved with
/// the Rust release would be orphaned by the next upgrade.
#[test]
fn the_stable_hash_is_pinned() {
    assert_eq!(stable_hash(b"123456789"), 0x995d_c9bb_df19_39fa);
    assert_eq!(
        DatasetShape::fingerprint_of([("a.parquet", 100, 7, Some("e1"))]),
        "1-100-7bb7c5da2f222965"
    );
}

/// Every field a reopen uses comes back as it was stored, an unreadable footer and
/// a footer's column widths among them.
#[test]
fn a_shape_comes_back_as_it_was_stored() {
    let dir = tempfile::tempdir().unwrap();
    let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
    let mut stored = shape("f", 7);
    stored.files.push(CachedFooter {
        schema: None,
        row_group_rows: Vec::new(),
        row_group_bytes: Vec::new(),
        column_bytes: Vec::new(),
    });
    stored.files.push(CachedFooter {
        schema: Some(1),
        row_group_rows: vec![0, 300, u32::MAX as usize + 5],
        row_group_bytes: vec![1, 2, 3],
        column_bytes: vec![128, 1 << 40],
    });
    stored.schemas.push(vec![
        ("id".into(), polars::prelude::DataType::Int64),
        (
            "tags".into(),
            polars::prelude::DataType::List(Box::new(polars::prelude::DataType::String)),
        ),
    ]);
    cache.save_dataset_shape("s3://b/events/", stored.clone());
    assert_eq!(cache.dataset_shape("s3://b/events/", "f"), Some(stored));
}

/// What a dataset the size of NOAA's by_station costs to keep and find again.
/// Run by hand: `scripts/dev/test.sh unit shape_cache_timings -- --ignored --nocapture`.
#[test]
#[ignore = "a timing, not a check"]
fn shape_cache_timings() {
    use polars::prelude::DataType;
    const FILES: usize = 842_000;
    let dir = tempfile::tempdir().unwrap();
    let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
    let big = DatasetShape {
        fingerprint: "big".into(),
        files: (0..FILES)
            .map(|i| CachedFooter {
                schema: Some(0),
                row_group_rows: vec![1_000 + (i * 7919) % 90_000],
                row_group_bytes: vec![10_000 + (i * 104_729) % 900_000],
                column_bytes: Vec::new(),
            })
            .collect(),
        schemas: vec![vec![
            ("ID".into(), DataType::String),
            ("DATE".into(), DataType::String),
            ("ELEMENT".into(), DataType::String),
            ("DATA_VALUE".into(), DataType::Int32),
            ("M_FLAG".into(), DataType::String),
            ("Q_FLAG".into(), DataType::String),
            ("S_FLAG".into(), DataType::String),
            ("OBS_TIME".into(), DataType::String),
        ]],
        taken_at: 1,
    };
    let time = |what: &str, work: &mut dyn FnMut()| {
        let began = std::time::Instant::now();
        work();
        eprintln!("{what}: {:.1?}", began.elapsed());
    };
    time("store by_station", &mut || {
        cache.save_dataset_shape("s3://noaa/by_station/", big.clone())
    });
    time("store a small one beside it", &mut || {
        cache.save_dataset_shape("s3://b/small/", shape("small", 2))
    });
    time("lookup + touch by_station", &mut || {
        assert!(
            cache
                .dataset_shape("s3://noaa/by_station/", "big")
                .is_some()
        )
    });
    time("lookup by_station, fingerprint moved", &mut || {
        assert!(
            cache
                .dataset_shape("s3://noaa/by_station/", "moved")
                .is_none()
        )
    });
    time("lookup + touch the small one", &mut || {
        assert!(cache.dataset_shape("s3://b/small/", "small").is_some())
    });
    let on_disk = Store::<Shapes>::new(&cache)
        .dir()
        .read_dir()
        .unwrap()
        .flatten()
        .map(|e| e.metadata().unwrap().len())
        .sum::<u64>();
    eprintln!("on disk: {:.1} MB", on_disk as f64 / 1e6);
}

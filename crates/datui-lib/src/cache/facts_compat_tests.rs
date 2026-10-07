use super::DatasetFacts;
use crate::home::discover::EntryKind;

/// A kind this build does not recognize costs its own row, not the whole index.
///
/// The dataset index is one JSON map read with `unwrap_or_default`, so a value that
/// fails to parse discards every fact datui had learned about every dataset — not
/// the one row it could not read. `EntryKind` gains variants as datui learns to
/// recognize more (a Delta root, an Iceberg root), so an older build reading a newer
/// cache is an ordinary event rather than a corruption.
#[test]
fn an_unknown_kind_costs_only_its_own_row() {
    let json = r#"{"mtime":1,"size":2,"rows":3,"cols":4,"columns":[],"kind":"quicksand"}"#;
    let facts: DatasetFacts = serde_json::from_str(json).expect("the record still parses");
    assert_eq!(
        facts.kind,
        Some(EntryKind::Unknown),
        "a kind from the future reads as unexamined"
    );
    assert_eq!(facts.rows, Some(3), "and the measurements survive with it");
    assert_eq!(facts.classified_by, 0, "recorded before that existed");

    // And the map around it survives too, which is the point.
    let index = r#"{"/a":{"mtime":1,"size":2,"rows":3,"cols":4,"columns":[],"kind":"quicksand"},
                        "/b":{"mtime":1,"size":2,"rows":9,"cols":1,"columns":[],"kind":"hive"}}"#;
    let map: std::collections::HashMap<std::path::PathBuf, DatasetFacts> =
        serde_json::from_str(index).expect("the index still parses");
    assert_eq!(map.len(), 2, "both rows, not none of them");
}

/// `datui cache clear` clears every cache kind and every list, and nothing else:
/// not a lock another instance may hold, not the log, not a foreign file.
#[test]
fn clear_all_removes_the_caches_and_lists_only() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let cache = super::CacheManager::with_dir(dir.path().to_path_buf());
    for name in [
        "query_history.txt",
        "fuzzy_history.txt",
        "recents_history.txt",
        "recents_history.txt.12.0.tmp",
        "cloud_hidden_history.txt",
    ] {
        std::fs::write(dir.path().join(name), b"x").expect("write");
    }
    cache.record_dataset_facts(&[("/d".into(), DatasetFacts::default())]);
    let kept = [
        "facts.lock",
        "recents_history.lock",
        "datui.log",
        "keep.parquet",
        "subdir",
    ];
    for name in kept {
        if name == "subdir" {
            std::fs::create_dir(dir.path().join(name)).expect("mkdir");
        } else {
            std::fs::write(dir.path().join(name), b"x").expect("write");
        }
    }

    cache.clear_all().expect("clear");

    let mut left: Vec<String> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    let mut kept = kept.map(String::from).to_vec();
    kept.sort();
    assert_eq!(left, kept);
}

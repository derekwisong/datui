use super::*;

#[test]
fn a_name_says_its_split_with_or_without_a_shard() {
    assert_eq!(split_of("imdb-train.arrow"), Some("train"));
    assert_eq!(split_of("imdb-test-00001-of-00004.arrow"), Some("test"));
    assert_eq!(split_of("squad_v2-validation.arrow"), Some("validation"));
    assert_eq!(split_of("wiki-40b-train_sft.arrow"), Some("train_sft"));
    assert_eq!(split_of("data-00000-of-00003.arrow"), None, "save_to_disk");
    assert_eq!(split_of("people.arrow"), None);
    assert_eq!(split_of("-train.arrow"), None);
}

fn picked(names: &[&str], table: Option<&str>) -> (Vec<String>, Splits) {
    let (files, splits) = choose(names, table).unwrap();
    (
        files.iter().map(|&i| names[i].to_string()).collect(),
        splits,
    )
}

#[test]
fn train_opens_and_the_other_splits_are_named() {
    let names = [
        "p-test.arrow",
        "p-train-00000-of-00002.arrow",
        "cache-0f3c.arrow",
        "p-train-00001-of-00002.arrow",
        "p-validation.arrow",
    ];
    let (files, splits) = picked(&names, None);
    assert_eq!(
        files,
        [
            "p-train-00000-of-00002.arrow",
            "p-train-00001-of-00002.arrow"
        ]
    );
    assert_eq!(
        splits,
        Splits {
            split: Some("train".into()),
            others: vec!["validation".into(), "test".into()],
            caches: 1,
        }
    );
    let (files, splits) = picked(&names, Some("validation"));
    assert_eq!(files, ["p-validation.arrow"]);
    assert_eq!(splits.others, ["train", "test"]);
    let error = choose(&names, Some("dev")).unwrap_err();
    assert!(error.contains("train, validation, test"), "{error}");
}

#[test]
fn without_train_the_first_split_opens() {
    let (files, splits) = picked(&["x-zeta.arrow", "x-alpha.arrow"], None);
    assert_eq!(files, ["x-alpha.arrow"]);
    assert_eq!(splits.others, ["zeta"]);
}

/// `train`, `validation` and `test` come first, in that order, whatever the names
/// sort as; the rest follow as listed.
#[test]
fn the_splits_datasets_names_come_first() {
    let names = [
        "p-extra.arrow",
        "p-test.arrow",
        "p-validation.arrow",
        "p-a_more.arrow",
    ];
    let (files, splits) = picked(&names, None);
    assert_eq!(files, ["p-validation.arrow"]);
    assert_eq!(splits.others, ["test", "a_more", "extra"]);
    let splits = pick(&["zz", "test", "aa", "train"], None).unwrap();
    assert_eq!(splits.split.as_deref(), Some("train"));
    assert_eq!(splits.others, ["test", "zz", "aa"]);
}

/// A DatasetDict names its splits in `dataset_dict.json`; a name that is not one
/// directory's is not taken.
#[test]
fn a_dataset_dict_names_its_split_directories() {
    assert_eq!(
        dict_splits(r#"{"splits": ["train", "test"]}"#),
        Some(vec!["train".to_string(), "test".to_string()])
    );
    for bad in [
        r#"{"splits": []}"#,
        r#"{"splits": ["../x"]}"#,
        r#"{"splits": [".."]}"#,
        r#"{"splits": ["a/b"]}"#,
        r#"{"splits": [1]}"#,
        r#"{"other": ["train"]}"#,
        "not json",
    ] {
        assert_eq!(dict_splits(bad), None, "{bad}");
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(DATASET_DICT),
        r#"{"splits": ["train", "test"]}"#,
    )
    .unwrap();
    std::fs::create_dir(dir.path().join("train")).unwrap();
    assert_eq!(dataset_dict(dir.path()), None, "test/ is missing");
    std::fs::create_dir(dir.path().join("test")).unwrap();
    assert_eq!(
        dataset_dict(dir.path()),
        Some(vec!["train".to_string(), "test".to_string()])
    );
}

#[test]
fn shards_that_name_no_split_are_one_table() {
    let names = [
        "data-00000-of-00002.arrow",
        "data-00001-of-00002.arrow",
        "cache-1.arrow",
    ];
    let (files, splits) = picked(&names, None);
    assert_eq!(files.len(), 2);
    assert_eq!((splits.split, splits.caches), (None, 1));
    assert!(choose(&names, Some("train")).is_err());
    assert!(choose(&["cache-1.arrow"], None).is_err());
}

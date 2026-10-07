//! The splits of a Hugging Face `datasets` directory.
//!
//! A cache directory holds a file per split, `name-train.arrow`, or a split's shards,
//! `name-train-00000-of-00003.arrow`, beside `dataset_info.json`. A `save_to_disk`
//! DatasetDict holds a subdirectory per split, named in its `dataset_dict.json`. The
//! splits are separate tables: an open reads one, `train` unless `--table` names
//! another, and lists the rest. `map()` writes its results beside a cache's splits as
//! `cache-*.arrow`, with columns of their own, so those are left out and counted.

use std::path::Path;

/// The splits `datasets` names for itself, in the order they are offered: before any
/// other, which follow as listed (a DatasetDict) or by name (a cache).
const FIRST: [&str; 3] = ["train", "validation", "test"];

/// What an open of a cache directory chose, and what it left out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Splits {
    /// The split on screen; `None` where the file names name no splits, as in a
    /// `save_to_disk` directory of `data-*` shards.
    pub split: Option<String>,
    /// The other splits, as `--table` names them.
    pub others: Vec<String>,
    /// The `cache-*.arrow` files `map()` wrote, which were not read.
    pub caches: usize,
}

/// Whether `name` is a file `map()` wrote rather than a split's.
pub fn is_cache(name: &str) -> bool {
    name.starts_with("cache-")
}

/// The split a cache file's name says it holds: `train` for `people-train.arrow` and
/// `people-train-00001-of-00002.arrow`. Split names are word characters and dots.
pub fn split_of(name: &str) -> Option<&str> {
    let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
    let stem = without_shard(stem);
    let (builder, split) = stem.rsplit_once('-')?;
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.';
    (!builder.is_empty() && !split.is_empty() && split.chars().all(word)).then_some(split)
}

/// `stem` without its `-00001-of-00002` shard suffix.
fn without_shard(stem: &str) -> &str {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    stem.rsplit_once("-of-")
        .filter(|(_, total)| digits(total))
        .and_then(|(head, _)| head.rsplit_once('-'))
        .filter(|(_, index)| digits(index))
        .map_or(stem, |(head, _)| head)
}

/// Which of `names`, a cache directory's Arrow files, an open reads: the files of
/// the split [`pick`] chooses, the others by name after the three `datasets` names
/// itself. Indices into `names`, in its order.
///
/// Where some name says no split the files are one table, as `save_to_disk` writes
/// them, and all but `map()`'s are read.
pub fn choose(names: &[&str], table: Option<&str>) -> Result<(Vec<usize>, Splits), String> {
    let data: Vec<usize> = (0..names.len()).filter(|&i| !is_cache(names[i])).collect();
    let caches = names.len() - data.len();
    if data.is_empty() {
        return Err(format!(
            "this directory holds only the cache files map() writes ({caches} cache-*.arrow), and no split"
        ));
    }
    let by_split: Option<Vec<(usize, &str)>> = data
        .iter()
        .map(|&i| split_of(names[i]).map(|split| (i, split)))
        .collect();
    let Some(by_split) = by_split else {
        if let Some(table) = table {
            return Err(format!(
                "--table {table}: this directory's files name no splits to pick from"
            ));
        }
        let splits = Splits {
            caches,
            ..Splits::default()
        };
        return Ok((data, splits));
    };
    let mut splits: Vec<&str> = by_split.iter().map(|(_, split)| *split).collect();
    splits.sort_unstable();
    splits.dedup();
    let mut picked = pick(&splits, table)?;
    let files = by_split
        .iter()
        .filter(|(_, split)| Some(*split) == picked.split.as_deref())
        .map(|(i, _)| *i)
        .collect();
    picked.caches = caches;
    Ok((files, picked))
}

/// The files `datasets` writes beside a cache's Arrow files, which mark the directory.
const CACHE_MARKERS: [&str; 2] = ["dataset_info.json", "state.json"];

/// Whether `dir` is a `datasets` cache directory: its metadata beside Arrow files.
pub fn is_cache_dir(dir: &Path) -> bool {
    CACHE_MARKERS.iter().any(|name| dir.join(name).is_file())
}

/// The splits a cache directory's Arrow files name, in the order an open offers them,
/// as the home screen lists them inside it. Empty for any other directory, and for one
/// whose files name no splits. Its listing is all that is read.
pub fn cache_splits(dir: &Path) -> Vec<String> {
    if !is_cache_dir(dir) {
        return Vec::new();
    }
    let Ok(read) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let names: Vec<String> = read
        .flatten()
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.to_ascii_lowercase().ends_with(".arrow") && !is_cache(n))
        .collect();
    let mut splits: Vec<&str> = Vec::new();
    for name in &names {
        match split_of(name) {
            Some(split) if !splits.contains(&split) => splits.push(split),
            Some(_) => {}
            // A file that names no split makes the directory one table.
            None => return Vec::new(),
        }
    }
    splits.sort();
    pick(&splits, None).map_or_else(
        |_| Vec::new(),
        |picked| picked.split.into_iter().chain(picked.others).collect(),
    )
}

/// The cache directory and the split a path inside one names (`cache/test`), as the
/// home screen lists it and recents record it. `None` for a path that is there.
pub fn split_place(path: &Path) -> Option<(std::path::PathBuf, String)> {
    if path.exists() {
        return None;
    }
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty())?;
    let name = path.file_name()?.to_str()?;
    cache_splits(dir)
        .into_iter()
        .find(|split| split == name)
        .map(|split| (dir.to_path_buf(), split))
}

/// The split of `listed` an open reads, `table` if given, else the first in offered
/// order, and the others in that order: `train`, `validation` and `test`, then the
/// rest as listed.
pub fn pick(listed: &[&str], table: Option<&str>) -> Result<Splits, String> {
    let mut offered: Vec<&str> = listed.to_vec();
    offered.sort_by_key(|split| {
        FIRST
            .iter()
            .position(|first| first == split)
            .unwrap_or(FIRST.len())
    });
    let chosen = match table {
        Some(table) => *offered.iter().find(|s| **s == table).ok_or_else(|| {
            format!(
                "No split named {table}; this directory holds {}",
                offered.join(", ")
            )
        })?,
        None => *offered
            .first()
            .ok_or_else(|| "this directory names no splits".to_string())?,
    };
    Ok(Splits {
        split: Some(chosen.to_string()),
        others: offered
            .iter()
            .filter(|s| **s != chosen)
            .map(|s| s.to_string())
            .collect(),
        caches: 0,
    })
}

/// The splits a `save_to_disk` DatasetDict directory names in its
/// `dataset_dict.json`, in its order, each a subdirectory of `dir`. `None` for any
/// other directory.
pub fn dataset_dict(dir: &Path) -> Option<Vec<String>> {
    let text = std::fs::read_to_string(dir.join(DATASET_DICT)).ok()?;
    let splits = dict_splits(&text)?;
    splits
        .iter()
        .all(|split| dir.join(split).is_dir())
        .then_some(splits)
}

/// The file that marks a DatasetDict directory.
pub const DATASET_DICT: &str = "dataset_dict.json";

/// The split names of a `dataset_dict.json`: `{"splits": ["train", "test"]}`. Each is
/// a directory's name, so one that could name another place is refused.
pub fn dict_splits(text: &str) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let splits: Vec<String> = value
        .get("splits")?
        .as_array()?
        .iter()
        .map(|split| split.as_str().map(str::to_string))
        .collect::<Option<_>>()?;
    let plain = |split: &String| {
        !split.is_empty()
            && split
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            && split != "."
            && split != ".."
    };
    (!splits.is_empty() && splits.iter().all(plain)).then_some(splits)
}

#[cfg(test)]
mod tests {
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
}

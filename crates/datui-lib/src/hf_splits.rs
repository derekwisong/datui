//! The splits of a Hugging Face `datasets` cache directory.
//!
//! The cache holds a file per split, `name-train.arrow`, or a split's shards,
//! `name-train-00000-of-00003.arrow`, beside `dataset_info.json`. The splits are
//! separate tables: an open reads one, `train` unless `--table` names another, and
//! lists the rest. `map()` writes its results beside them as `cache-*.arrow`, with
//! columns of their own, so those are left out and counted.

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
/// `table` if given, else of `train`, else of the first split by name. Indices into
/// `names`, in its order.
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
    let chosen = match table {
        Some(table) => *splits.iter().find(|s| **s == table).ok_or_else(|| {
            format!(
                "No split named {table}; this directory holds {}",
                splits.join(", ")
            )
        })?,
        None => splits.iter().find(|s| **s == "train").unwrap_or(&splits[0]),
    };
    let files = by_split
        .iter()
        .filter(|(_, split)| *split == chosen)
        .map(|(i, _)| *i)
        .collect();
    let splits = Splits {
        split: Some(chosen.to_string()),
        others: splits
            .iter()
            .filter(|s| **s != chosen)
            .map(|s| s.to_string())
            .collect(),
        caches,
    };
    Ok((files, splits))
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
                others: vec!["test".into(), "validation".into()],
                caches: 1,
            }
        );
        let (files, splits) = picked(&names, Some("validation"));
        assert_eq!(files, ["p-validation.arrow"]);
        assert_eq!(splits.others, ["test", "train"]);
        let error = choose(&names, Some("dev")).unwrap_err();
        assert!(error.contains("test, train, validation"), "{error}");
    }

    #[test]
    fn without_train_the_first_split_opens() {
        let (files, splits) = picked(&["x-zeta.arrow", "x-alpha.arrow"], None);
        assert_eq!(files, ["x-alpha.arrow"]);
        assert_eq!(splits.others, ["zeta"]);
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

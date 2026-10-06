//! The local files a glob names, for a read that goes file by file rather than handing
//! Polars the pattern.

use std::path::{Component, Path, PathBuf};

/// How deep a `**` walks. Past this the files belong to something else, and a link
/// loop ends.
const MAX_DEPTH: usize = 64;

/// The files `pattern` matches, sorted by path. `*` and `?` stay within one part of
/// the path; `**` crosses parts. Empty when nothing matches or the pattern does not
/// parse.
pub fn expand(pattern: &Path) -> Vec<PathBuf> {
    let Ok(matcher) = globset::GlobBuilder::new(&pattern.to_string_lossy())
        .literal_separator(true)
        .build()
        .map(|g| g.compile_matcher())
    else {
        return Vec::new();
    };
    // The walk starts at the parts before the first with a glob character in it.
    let mut base = PathBuf::new();
    let mut rest = 0usize;
    let mut deep = false;
    let mut in_glob = false;
    for part in pattern.components() {
        let text = part.as_os_str().to_string_lossy();
        if !in_glob && !crate::source::has_glob_chars(Path::new(text.as_ref())) {
            base.push(part);
            continue;
        }
        in_glob = true;
        if let Component::Normal(_) = part {
            rest += 1;
            deep |= text == "**";
        }
    }
    if base.as_os_str().is_empty() {
        base = PathBuf::from(".");
    }
    let depth = if deep { MAX_DEPTH } else { rest };
    let mut found = Vec::new();
    walk(&base, depth, &matcher, &mut found);
    found.sort();
    found
}

fn walk(dir: &Path, depth: usize, matcher: &globset::GlobMatcher, found: &mut Vec<PathBuf>) {
    if depth == 0 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            walk(&path, depth - 1, matcher, found);
        } else if matcher.is_match(&path) {
            found.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_glob_names_its_files_in_order() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["log_2.csv", "log_1.csv", "other.csv", "sub/log_3.csv"] {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "a\n").unwrap();
        }
        let names = |pattern: &str| -> Vec<String> {
            expand(&dir.path().join(pattern))
                .iter()
                .map(|p| {
                    p.strip_prefix(dir.path())
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/")
                })
                .collect()
        };
        assert_eq!(names("log_*.csv"), ["log_1.csv", "log_2.csv"]);
        assert_eq!(
            names("**/log_*.csv"),
            ["log_1.csv", "log_2.csv", "sub/log_3.csv"]
        );
        assert!(names("none_*.csv").is_empty());
    }
}

//! The queries the docs show run (#683): every `q` block parses, and every `sql` or
//! `q` block that names a dataset (`sql,dataset=penguins`) runs on it, through the
//! app as a query typed at `/` would, and returns the rows it says (`rows=N`).
//!
//! The datasets are listed in `scripts/docs/doc_datasets.toml`. One with a `path`
//! ships with the docs and runs here; one with a `url` is public data, which the
//! nightly job downloads (`doc_examples.py --fetch-datasets DIR`) and runs with
//! `DATUI_DOC_DATA=DIR` and `--ignored`.

use std::path::{Path, PathBuf};
use std::sync::mpsc;

use super::chart_prepare_tests::{open, pump};
use crate::{App, AppEvent};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A fenced block of a docs page: its place, language, attributes and text.
struct Block {
    place: String,
    lang: String,
    attrs: Vec<String>,
    body: String,
}

impl Block {
    fn value(&self, key: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find_map(|a| a.strip_prefix(key)?.strip_prefix('='))
    }

    fn has(&self, attr: &str) -> bool {
        self.attrs.iter().any(|a| a == attr)
    }

    /// The queries it holds: a `q` block holds one a line, a `sql` block one.
    fn queries(&self) -> Vec<String> {
        if self.lang == "q" {
            self.body
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect()
        } else {
            vec![self.body.trim().to_string()]
        }
    }
}

fn markdown_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "md") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&root().join("docs"), &mut out);
    for name in ["README.md", "python/README.md"] {
        out.push(root().join(name));
    }
    out
}

/// Every `q` and `sql` block users read.
fn query_blocks() -> Vec<Block> {
    let mut out = Vec::new();
    for path in markdown_files() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let name = path
            .strip_prefix(root())
            .unwrap_or(&path)
            .display()
            .to_string();
        let mut lines = text.lines().enumerate();
        while let Some((n, line)) = lines.next() {
            let Some(info) = line.trim_start().strip_prefix("```") else {
                continue;
            };
            let mut parts = info.split(',').map(str::trim);
            let lang = parts.next().unwrap_or_default().to_string();
            let attrs: Vec<String> = parts.map(str::to_string).collect();
            let mut body = String::new();
            for (_, line) in lines.by_ref() {
                if line.trim_start().starts_with("```") {
                    break;
                }
                body.push_str(line);
                body.push('\n');
            }
            if lang == "q" || lang == "sql" {
                out.push(Block {
                    place: format!("{name}:{}", n + 1),
                    lang,
                    attrs,
                    body,
                });
            }
        }
    }
    out
}

/// Every runnable `q` block parses, line by line, so a renamed function or keyword
/// fails here rather than in a reader's prompt.
#[test]
fn every_q_block_in_the_docs_parses() {
    let mut checked = 0;
    let mut failures = Vec::new();
    for block in query_blocks()
        .iter()
        .filter(|b| b.lang == "q" && !b.has("template"))
    {
        for query in block.queries() {
            checked += 1;
            if let Err(e) = crate::query::parse_query(&query) {
                failures.push(format!("{}: {query}: {e}", block.place));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert!(checked > 0, "the docs show q queries");
}

/// Where a dataset is: a path that ships with the docs, or the downloaded copy of a
/// public one in `data`.
fn dataset(name: &str, data: Option<&Path>) -> Option<PathBuf> {
    let list = std::fs::read_to_string(root().join("scripts/docs/doc_datasets.toml"))
        .expect("doc_datasets.toml");
    let list: toml::Table = list.parse().expect("doc_datasets.toml is TOML");
    let entry = list
        .get(name)
        .and_then(toml::Value::as_table)
        .unwrap_or_else(|| panic!("dataset {name} is not in doc_datasets.toml"));
    if let Some(path) = entry.get("path").and_then(toml::Value::as_str) {
        return Some(root().join(path));
    }
    let url = entry.get("url").and_then(toml::Value::as_str)?;
    let ext = Path::new(url).extension()?.to_str()?;
    Some(data?.join(format!("{name}.{ext}")))
}

/// Run the blocks whose dataset `pick` accepts; the failures.
fn run_blocks(local: bool, data: Option<&Path>) -> (usize, Vec<String>) {
    let mut ran = 0;
    let mut failures = Vec::new();
    for block in query_blocks().iter().filter(|b| !b.has("template")) {
        let Some(name) = block.value("dataset") else {
            continue;
        };
        let is_local = !block.has("network");
        if is_local != local {
            continue;
        }
        let Some(path) = dataset(name, data) else {
            continue;
        };
        for query in block.queries() {
            ran += 1;
            if let Err(e) = run_query(&path, &block.lang, &query, block.value("rows")) {
                failures.push(format!("{}: {e}", block.place));
            }
        }
    }
    (ran, failures)
}

/// Open `path`, run `query` as `/` would, and check it succeeds with `rows` rows.
fn run_query(path: &Path, lang: &str, query: &str, rows: Option<&str>) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), super::test_runtime());
    open(&mut app, &rx, &tx, path.to_path_buf());
    if let Some(e) = app.error_message() {
        return Err(format!("{} did not open: {e}", path.display()));
    }
    let event = match lang {
        "sql" => AppEvent::SqlQuery(query.to_string()),
        _ => AppEvent::QQuery(query.to_string()),
    };
    if let Some(next) = app.event(&event) {
        let _ = tx.send(next);
    }
    pump(&mut app, &rx, &tx, |a| !super::work_pending(a));
    if let Some(e) = app
        .query_prompt_error()
        .or(app.error_message().map(str::to_string))
    {
        return Err(format!("{query}: {e}"));
    }
    if let Some(want) = rows {
        let got = app
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid());
        if got.map(|n| n.to_string()).as_deref() != Some(want) {
            return Err(format!("{query}: {got:?} rows, the docs say {want}"));
        }
    }
    Ok(())
}

/// The queries on data that ships with the docs run, on every pull request.
#[test]
fn query_blocks_on_local_data_run() {
    let (_, failures) = run_blocks(true, None);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The queries on public data run: the nightly job downloads it and sets
/// DATUI_DOC_DATA.
#[test]
#[ignore = "needs the public datasets: DATUI_DOC_DATA=DIR from doc_examples.py --fetch-datasets"]
fn query_blocks_on_public_data_run() {
    let data = std::env::var_os("DATUI_DOC_DATA").map(PathBuf::from);
    let data = data.expect("DATUI_DOC_DATA names the downloaded datasets");
    let (ran, failures) = run_blocks(false, Some(&data));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    assert!(ran > 0, "the docs run queries on public data");
}

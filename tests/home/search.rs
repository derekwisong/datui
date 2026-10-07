//! Recursive search below the working directory.

use datui::config::SearchConfig;
use datui::discover::Entry;
use datui::search;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn touch(path: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, b"").unwrap();
}

/// Collect every result of a walk, ignoring the streaming.
fn walk_all(root: &Path, config: &SearchConfig) -> (Vec<Entry>, search::Outcome) {
    let mut all = Vec::new();
    let outcome = search::walk(root, config, |batch, _| {
        all.extend(batch);
        true
    });
    (all, outcome)
}

fn names(entries: &[Entry]) -> Vec<String> {
    let mut n: Vec<String> = entries.iter().map(|e| e.name.clone()).collect();
    n.sort();
    n
}

#[test]
fn test_finds_data_nested_below_the_starting_directory() {
    // The whole point: the file is three directories down and you never opened it.
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("a/b/c/quarterly_returns.parquet"));
    touch(&tmp.path().join("top.csv"));

    let (found, outcome) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(
        names(&found),
        vec!["a/b/c/quarterly_returns.parquet", "top.csv"]
    );
    assert!(outcome.complete(), "nothing here should hit a limit");
}

#[test]
fn test_results_are_named_by_their_path_below_the_root() {
    // Three files called sales.parquet are indistinguishable by name alone; the
    // relative path is the only thing that says which one you want.
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("europe/sales.parquet"));
    touch(&tmp.path().join("americas/sales.parquet"));

    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(
        names(&found),
        vec!["americas/sales.parquet", "europe/sales.parquet"]
    );
}

#[test]
fn test_gitignored_data_is_found_because_that_is_why_it_is_gitignored() {
    // The case that decided the design. People gitignore data directories precisely
    // because the data is too big to commit -- which is the same reason they want to
    // open it in datui. Honouring .gitignore hides exactly the wrong files.
    let tmp = TempDir::new().unwrap();
    fs::write(tmp.path().join(".gitignore"), "data/\n*.parquet\n").unwrap();
    touch(&tmp.path().join("data/prices.parquet"));

    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(names(&found), vec!["data/prices.parquet"]);
}

#[test]
fn test_gitignore_can_be_turned_back_on() {
    let tmp = TempDir::new().unwrap();
    fs::write(tmp.path().join(".gitignore"), "data/\n").unwrap();
    touch(&tmp.path().join("data/prices.parquet"));
    touch(&tmp.path().join("kept.parquet"));

    let config = SearchConfig {
        follow_gitignore: true,
        ..Default::default()
    };
    let (found, _) = walk_all(tmp.path(), &config);
    assert_eq!(names(&found), vec!["kept.parquet"]);
}

#[test]
fn test_dependency_directories_are_skipped() {
    // node_modules and site-packages are full of .json, which datui can open. Without
    // the skip list every package manifest on the machine is a search result.
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("node_modules/left-pad/package.json"));
    touch(
        &tmp.path()
            .join("lib/python3.12/site-packages/pandas/fixture.csv"),
    );
    touch(&tmp.path().join("target/debug/build/thing.json"));
    touch(&tmp.path().join("real_data.parquet"));

    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(names(&found), vec!["real_data.parquet"]);
}

#[test]
fn test_hidden_directories_are_skipped() {
    // .git and .venv are the expensive ones, and this matches what the plain
    // directory listing already does.
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join(".git/objects/pack/thing.txt"));
    touch(
        &tmp.path()
            .join(".venv/lib/python3.12/site-packages/x/data.csv"),
    );
    touch(&tmp.path().join("visible.csv"));

    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(names(&found), vec!["visible.csv"]);
}

#[test]
fn test_skip_extra_adds_without_restating_the_defaults() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("node_modules/x/package.json"));
    touch(&tmp.path().join("scratch/notes.csv"));
    touch(&tmp.path().join("keep.csv"));

    let config = SearchConfig {
        skip_extra: vec!["scratch".into()],
        ..Default::default()
    };
    let (found, _) = walk_all(tmp.path(), &config);
    assert_eq!(
        names(&found),
        vec!["keep.csv"],
        "the default skips must still apply alongside the extra one"
    );
}

#[test]
fn test_skip_replaces_the_defaults_wholesale() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("node_modules/x/package.json"));

    let config = SearchConfig {
        skip: vec!["something_else".into()],
        ..Default::default()
    };
    let (found, _) = walk_all(tmp.path(), &config);
    assert_eq!(
        names(&found),
        vec!["node_modules/x/package.json"],
        "an explicit skip list means what it says"
    );
}

#[test]
fn test_the_result_limit_is_reported_not_silently_applied() {
    // A search that quietly returned less than the truth is worse than no search:
    // "not found here" is something people act on.
    let tmp = TempDir::new().unwrap();
    for i in 0..50 {
        touch(&tmp.path().join(format!("f{i:03}.parquet")));
    }

    let mut found = Vec::new();
    let outcome = search::walk_up_to(tmp.path(), &SearchConfig::default(), 10, |batch, _| {
        found.extend(batch);
        true
    });
    assert_eq!(found.len(), 10);
    assert!(outcome.hit_result_limit);
    assert!(!outcome.complete());
    assert_eq!(outcome.note(), Some("partial · too many files"));
}

#[test]
fn test_depth_is_bounded_and_says_so() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("a/b/c/d/e/f/g/h/deep.parquet"));
    touch(&tmp.path().join("shallow.parquet"));

    let config = SearchConfig {
        max_depth: 3,
        ..Default::default()
    };
    let (found, outcome) = walk_all(tmp.path(), &config);
    assert_eq!(names(&found), vec!["shallow.parquet"]);
    assert!(
        outcome.hit_depth_limit,
        "stopping short must be visible, not silent"
    );
}

#[test]
fn test_a_disabled_search_does_no_work_at_all() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("data.parquet"));

    let config = SearchConfig {
        enabled: false,
        ..Default::default()
    };
    let (found, outcome) = walk_all(tmp.path(), &config);
    assert!(found.is_empty());
    assert_eq!(outcome.scanned, 0, "disabled means not one stat");
}

#[test]
fn test_results_stream_rather_than_arriving_all_at_once() {
    // A cold tree must fill the screen while it is still working.
    let tmp = TempDir::new().unwrap();
    for i in 0..500 {
        touch(&tmp.path().join(format!("d{i:03}/f.parquet")));
    }

    let mut batches = 0usize;
    let mut total = 0usize;
    search::walk(tmp.path(), &SearchConfig::default(), |batch, _| {
        if !batch.is_empty() {
            batches += 1;
            total += batch.len();
        }
        true
    });
    assert_eq!(total, 500);
    assert!(batches >= 1, "results must be handed back at least once");
}

#[test]
fn test_a_walk_stops_when_the_caller_stops_wanting_it() {
    // Abandonment is how every other background task here ends. A walk that keeps
    // reading the disk for an answer nobody is waiting for is wasted I/O.
    let tmp = TempDir::new().unwrap();
    for i in 0..5_000 {
        touch(&tmp.path().join(format!("d{i:04}/f.parquet")));
    }

    let mut seen = 0usize;
    search::walk(tmp.path(), &SearchConfig::default(), |batch, _| {
        seen += batch.len();
        false // stop after the first batch
    });
    assert!(
        seen < 5_000,
        "returning false should abandon the walk, not merely be ignored"
    );
}

#[test]
fn test_non_data_files_are_never_offered() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("README.md"));
    touch(&tmp.path().join("main.rs"));
    touch(&tmp.path().join("real.parquet"));

    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(names(&found), vec!["real.parquet"]);
}

#[test]
fn test_the_extension_list_can_be_narrowed() {
    // .json and .txt are formats datui opens and also the noisiest things in a source
    // tree; narrowing is the escape hatch.
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("config.json"));
    touch(&tmp.path().join("notes.txt"));
    touch(&tmp.path().join("prices.parquet"));

    let config = SearchConfig {
        extensions: vec!["parquet".into()],
        ..Default::default()
    };
    let (found, _) = walk_all(tmp.path(), &config);
    assert_eq!(names(&found), vec!["prices.parquet"]);
}

#[test]
fn test_a_network_directory_is_never_walked_recursively() {
    // A remote root is listed one directory at a time by a probe, precisely so a
    // share that stops answering cannot take the interface with it. Recursing into
    // one would undo that.
    fn always_network(_: &Path) -> bool {
        true
    }
    let tmp = TempDir::new().unwrap();
    let root = search::search_root(Some(&tmp.path().to_path_buf()), always_network);
    assert!(
        root.is_none(),
        "a network path must not become a search root"
    );

    fn never_network(_: &Path) -> bool {
        false
    }
    let root = search::search_root(Some(&tmp.path().to_path_buf()), never_network);
    assert_eq!(root.as_deref(), Some(tmp.path()));
}

/// Files `name` by `label` below `root`, without touching the disk: the scorer and the
/// listing read names, not files.
fn found(root: &Path, labels: &[String]) -> Vec<Entry> {
    labels
        .iter()
        .map(|label| Entry::for_test(&root.join(label), label))
        .collect()
}

fn many_and_a_needle(root: &Path, many: usize) -> Vec<Entry> {
    let mut labels: Vec<String> = (0..many)
        .map(|i| format!("d{:02}/f{i:05}.csv", i % 40))
        .collect();
    labels.push("deep/a/b/c/d/e/needle_metrics.parquet".to_string());
    found(root, &labels)
}

/// The listing cap counts matches, not files walked: past it, a match is still found
/// (#547 D1), and the heading says how many matched in all.
#[test]
fn test_the_cap_counts_matches_not_files() {
    let root = Path::new("/data");
    let mut home = datui::home::HomeState {
        filter: "needle".into(),
        search_limit: 10,
        ..Default::default()
    };
    home.search.root = Some(root.to_path_buf());
    home.search.set_results(many_and_a_needle(root, 500));
    home.search_finished(root, 600, None);
    let found_rows = |home: &datui::home::HomeState| {
        home.sections
            .iter()
            .find(|s| s.title == datui::home::HomeState::SEARCH_SECTION)
            .map(|s| s.rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>())
            .unwrap_or_default()
    };
    assert_eq!(
        found_rows(&home),
        vec!["deep/a/b/c/d/e/needle_metrics.parquet"]
    );

    home.filter = "f00".into();
    home.sync_search_section();
    assert_eq!(found_rows(&home).len(), 10, "ten listed");
    let section = home
        .sections
        .iter()
        .find(|s| s.title == datui::home::HomeState::SEARCH_SECTION)
        .unwrap();
    let subtitle = section.subtitle.clone().unwrap_or_default();
    assert!(subtitle.contains("10 of 500 matches"), "{subtitle:?}");
}

/// A longer query narrows the last one's matches and agrees with scoring from scratch;
/// files found since are looked at too.
#[test]
fn test_a_growing_query_narrows_the_last_matches() {
    let root = Path::new("/data");
    let files = many_and_a_needle(root, 300);
    let (first, later) = files.split_at(200);
    let index: Vec<std::sync::Arc<[Entry]>> = vec![first.to_vec().into(), later.to_vec().into()];
    let early: Vec<std::sync::Arc<[Entry]>> = vec![first.to_vec().into()];

    let ne = search::score(&early, "f0", None, 5);
    assert!(ne.narrows_to("f00"));
    assert!(!ne.narrows_to("x"));
    let narrowed = search::score(&index, "f00", Some(&ne), 5);
    let whole = search::score(&index, "f00", None, 5);
    assert_eq!(narrowed.ids, whole.ids);
    assert_eq!(narrowed.top.len(), 5);
    let names = |m: &search::Matches| m.top.iter().map(|e| e.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(&narrowed), names(&whole));
    assert_eq!(narrowed.upto, 301);
}

/// Matches carried forward over a walk's later batches agree with scoring them all.
#[test]
fn test_matches_carry_forward_over_new_files() {
    let root = Path::new("/data");
    let files = many_and_a_needle(root, 300);
    let mut carried = search::score(&[files[..120].to_vec().into()], "f01", None, 7);
    assert!(carried.extend(&files[120..], 120, 7) || !carried.top.is_empty());
    let whole = search::score(&[files.clone().into()], "f01", None, 7);
    assert_eq!(carried.ids, whole.ids);
    let names = |m: &search::Matches| m.top.iter().map(|e| e.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(&carried), names(&whole));
    assert_eq!(carried.scores, whole.scores);
}

/// A walk that stopped short and matched nothing says so, rather than leaving the
/// screen to say `No match.` (#547 D1).
#[test]
fn test_an_empty_partial_search_says_so() {
    let root = Path::new("/data");
    let mut home = datui::home::HomeState {
        filter: "needle".into(),
        ..Default::default()
    };
    home.search.root = Some(root.to_path_buf());
    home.search.running = true;
    home.search_batch(root, found(root, &["a.csv".to_string()]), 10);
    home.search_finished(root, 20_000, Some("partial · out of time".into()));
    let section = home
        .sections
        .iter()
        .find(|s| s.title == datui::home::HomeState::SEARCH_SECTION)
        .expect("the heading stays to say why it is empty");
    assert!(section.rows.is_empty());
    let subtitle = section.subtitle.clone().unwrap_or_default();
    assert!(subtitle.contains("no match in 1 file "), "{subtitle:?}");
    assert!(subtitle.contains("partial · out of time"), "{subtitle:?}");
    assert!(
        home.visible()
            .iter()
            .any(|r| matches!(r, datui::home::Row::Header { .. })),
        "drawn, not dropped as an empty section"
    );

    // A complete walk with no match needs no heading: `No match.` is the whole truth.
    home.search.limited = None;
    home.sync_search_section();
    assert!(
        !home
            .sections
            .iter()
            .any(|s| s.title == datui::home::HomeState::SEARCH_SECTION)
    );
}

/// A found file says what it is stored on, as a listed one does, rather than drawing
/// the unknown place's glyph on local disk (#547 D10).
#[test]
fn test_found_files_say_where_they_live() {
    let tmp = TempDir::new().unwrap();
    touch(&tmp.path().join("a/b/deep.parquet"));
    let (found, _) = walk_all(tmp.path(), &SearchConfig::default());
    assert_eq!(found.len(), 1);
    assert!(found[0].cost.source.is_some(), "{:?}", found[0].cost);
}

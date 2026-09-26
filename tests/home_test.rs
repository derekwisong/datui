//! Home screen: dataset discovery, roots, and filtering.

use datui::discover::{self, EntryKind};
use datui::home::{HomeState, RootOrigin, Row, fuzzy_score};
use std::fs;
use tempfile::TempDir;

mod common;

/// Names of the dataset rows on screen, ignoring section headers.
///
/// The door counts: it is a row on screen, drawn like any other. What it is not is one
/// of the section's `rows` — see [`door_of`].
fn visible_names(home: &HomeState) -> Vec<String> {
    home.visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } | Row::Door { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect()
}

/// The row that opens the directory being browsed as one table.
///
/// A section's own field rather than one of its rows, because its path *is* the
/// directory's and `PathBuf` hashes a trailing slash away: as a row it was the same key
/// as the directory's row one level up in every path-keyed map. See `Section::door`.
fn door_of(home: &HomeState) -> Option<&discover::Entry> {
    home.sections.iter().find_map(|s| s.door.as_ref())
}

fn touch(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("mkdir");
    }
    fs::write(&path, b"x").expect("write");
    path
}

// ---------------------------------------------------------------------------
// Recognising data
// ---------------------------------------------------------------------------

#[test]
fn test_recognises_data_extensions() {
    for name in [
        "a.parquet",
        "a.csv",
        "a.CSV",
        "a.tsv",
        "a.json",
        "a.ipc",
        "a.orc",
        "a.xlsx",
    ] {
        assert!(
            discover::is_data_file(std::path::Path::new(name)),
            "{name} should be data"
        );
    }
    for name in ["readme.md", "script.py", "noext", "a.tar", ".hidden"] {
        assert!(
            !discover::is_data_file(std::path::Path::new(name)),
            "{name} should not be data"
        );
    }
}

#[test]
fn test_recognises_compressed_data() {
    // `sales.csv.gz` is still a CSV; the compression suffix must not hide it.
    assert!(discover::is_data_file(std::path::Path::new("sales.csv.gz")));
    assert!(discover::is_data_file(std::path::Path::new(
        "sales.json.zst"
    )));
    assert!(!discover::is_data_file(std::path::Path::new(
        "backup.tar.gz"
    )));
}

// ---------------------------------------------------------------------------
// Classifying directories — what makes this a dataset browser, not a file browser
// ---------------------------------------------------------------------------

#[test]
fn test_hive_directory_is_one_dataset() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales/year=2024/part-0.parquet");
    touch(tmp.path(), "sales/year=2025/part-0.parquet");

    assert_eq!(
        discover::classify_directory(&tmp.path().join("sales")),
        EntryKind::Hive
    );

    // And it appears as a single row, not a tree to walk. The listing does not look
    // into it — no listing looks into anything — so the row says only that nothing
    // has, until something does.
    let entries = discover::scan_dir(tmp.path());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "sales");
    assert_eq!(entries[0].kind, EntryKind::Unknown);
    assert_eq!(datui::home::look_into(&entries[0]).kind, EntryKind::Hive);
}

#[test]
fn test_homogeneous_directory_is_a_multi_file_dataset() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "exports/jan.parquet");
    touch(tmp.path(), "exports/feb.parquet");

    assert_eq!(
        discover::classify_directory(&tmp.path().join("exports")),
        EntryKind::MultiFile
    );
}

#[test]
fn test_mixed_extensions_are_not_a_dataset() {
    // A directory holding a CSV and a spreadsheet is a directory, not a table.
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "stuff/a.csv");
    touch(tmp.path(), "stuff/b.parquet");

    assert_eq!(
        discover::classify_directory(&tmp.path().join("stuff")),
        EntryKind::Directory
    );
}

/// Compression is not a format: `.csv.gz` and `.json.gz` are two kinds of file.
///
/// `Path::extension` answers `gz` for both, so comparing extensions made every
/// compressed directory look homogeneous whatever was in it — and a directory of two
/// formats was then offered as one table.
#[test]
fn test_compressed_files_are_compared_by_what_they_hold() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "mixed/a.csv.gz");
    touch(tmp.path(), "mixed/b.json.gz");

    assert_eq!(
        discover::classify_directory(&tmp.path().join("mixed")),
        EntryKind::Directory,
        "two formats under one compression suffix are still two formats"
    );

    // And the other half of the same rule: agreeing under compression still agrees.
    touch(tmp.path(), "same/a.csv.gz");
    touch(tmp.path(), "same/b.csv.gz");
    assert_eq!(
        discover::classify_directory(&tmp.path().join("same")),
        EntryKind::MultiFile
    );
}

#[test]
fn test_directory_of_mostly_other_files_is_not_a_dataset() {
    // Two stray CSVs in a source tree must not turn the source tree into a dataset —
    // that would hide the directory behind a table that cannot be opened.
    let tmp = TempDir::new().unwrap();
    let project = tmp.path().join("project");
    for name in ["a.rs", "b.rs", "c.rs", "d.rs", "e.rs", "f.rs"] {
        touch(&project, name);
    }
    touch(&project, "one.csv");
    touch(&project, "two.csv");

    assert_eq!(discover::classify_directory(&project), EntryKind::Directory);
}

#[test]
fn test_single_data_file_directory_is_navigable() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "solo/only.parquet");
    assert_eq!(
        discover::classify_directory(&tmp.path().join("solo")),
        EntryKind::Directory
    );
}

#[test]
fn test_scan_skips_dotfiles_and_non_data() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "good.parquet");
    touch(tmp.path(), ".hidden.parquet");
    touch(tmp.path(), "notes.md");

    let names: Vec<String> = discover::scan_dir(tmp.path())
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["good.parquet"]);
}

#[test]
fn test_scan_of_unreadable_directory_is_empty_not_fatal() {
    // An unmounted NAS must degrade to an empty listing, never a panic or a hang.
    let entries = discover::scan_dir(std::path::Path::new("/definitely/not/here"));
    assert!(entries.is_empty());
}

#[test]
fn test_datasets_sort_before_directories() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "zzz.parquet");
    touch(tmp.path(), "aaa_dir/readme.md");

    let entries = discover::scan_dir(tmp.path());
    assert_eq!(entries[0].kind, EntryKind::File);
    assert_eq!(entries[0].name, "zzz.parquet");
}

// ---------------------------------------------------------------------------
// Roots — how datui learns where the data is
// ---------------------------------------------------------------------------

#[test]
fn test_a_recent_dataset_puts_its_directory_under_recent_not_beside_it() {
    // The whole point: code lives in cwd, data lives on a mount. Opening something
    // there once must be enough for datui to know about the place. It used to become
    // a section of its own, titled by path and drawn exactly like a configured
    // directory; now it is a place row under RECENT, and Enter on it browses there.
    let tmp = TempDir::new().unwrap();
    let mount = tmp.path().join("mnt/data");
    let dataset = touch(&mount, "sales.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&dataset));

    let mount_title = datui::home::display_path(&mount);
    assert!(
        !home.sections.iter().any(|s| s.title == mount_title),
        "a path in a header is a real root; a recent's directory is not one"
    );
    let rows = home.visible();
    let place = rows
        .iter()
        .position(|r| matches!(r, Row::Place { path, .. } if *path == mount))
        .expect("the directory holding a recent dataset is a place row");
    assert!(
        matches!(
            rows.get(place + 1),
            Some(Row::Entry { entry, nested: true, .. }) if entry.path == dataset
        ),
        "the dataset is drawn under its place: {rows:?}"
    );
}

#[test]
fn test_configured_directories_become_roots() {
    let tmp = TempDir::new().unwrap();
    let configured = tmp.path().join("datasets");
    fs::create_dir_all(&configured).unwrap();

    // The working directory is always root 0, so look the configured one up by path.
    let roots = HomeState::roots(std::slice::from_ref(&configured), &[]);
    let root = roots
        .iter()
        .find(|r| r.path == configured)
        .expect("a configured directory should become a root");
    assert_eq!(root.origin, RootOrigin::Configured);
}

#[test]
fn test_unavailable_root_is_reported_not_hidden() {
    // "The mount is down" is information; silently dropping the row is not.
    let missing = std::path::PathBuf::from("/definitely/not/here");
    let roots = HomeState::roots(std::slice::from_ref(&missing), &[]);
    let root = roots.iter().find(|r| r.path == missing).expect("kept");
    assert!(!root.available);
}

#[test]
fn test_roots_are_deduplicated() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    touch(&dir, "a.parquet");

    let roots = HomeState::roots(&[dir.clone(), dir.clone()], std::slice::from_ref(&dir));
    let hits = roots.iter().filter(|r| r.path == dir).count();
    assert_eq!(hits, 1, "a directory named twice should appear once");
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

#[test]
fn test_fuzzy_matches_subsequences() {
    assert!(fuzzy_score("sal", "sales.parquet").is_some());
    assert!(fuzzy_score("slsprq", "sales.parquet").is_some());
    assert!(fuzzy_score("SAL", "sales.parquet").is_some());
    assert!(fuzzy_score("", "anything").is_some());
    assert!(fuzzy_score("zzz", "sales.parquet").is_none());
}

#[test]
fn test_fuzzy_prefers_tighter_matches() {
    let tight = fuzzy_score("sale", "sales.parquet").unwrap();
    let loose = fuzzy_score("sale", "s_a_l_zzzzzzzz_e.parquet").unwrap();
    assert!(
        tight > loose,
        "tight {tight} should beat loose {loose}; higher is better, as in fzf"
    );
}

#[test]
fn test_filter_narrows_the_listing() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales.parquet");
    touch(tmp.path(), "customers.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    // The two files, and the row that opens the directory holding them.
    assert_eq!(visible_names(&home).len(), 3);

    // The filter narrows every row alike, the second door included.
    home.filter = "sal".to_string();
    assert_eq!(visible_names(&home), vec!["sales.parquet"]);

    home.filter = "zzz".to_string();
    assert!(visible_names(&home).is_empty());
}

#[test]
fn test_selection_wraps_and_stays_in_range() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "a.parquet");
    touch(tmp.path(), "b.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    // The list is [header, the row that opens the whole directory, a, b]; the cursor
    // starts on the first dataset, and moving walks headers too, since reaching one is
    // how a section gets expanded.
    let total = home.visible().len();
    assert_eq!(total, 4);
    let start = home.selected;
    assert!(home.selected_entry().is_some(), "should start on a dataset");

    for _ in 0..total {
        home.move_selection(1);
    }
    assert_eq!(home.selected, start, "a full cycle returns to the start");

    home.move_selection(-1);
    assert!(
        home.selected < total,
        "selection stays in range going backwards"
    );
}

#[test]
fn test_selection_clamps_when_filter_shrinks_the_list() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "aaa.parquet");
    touch(tmp.path(), "bbb.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.selected = home.visible().len() - 1;

    home.filter = "aaa".to_string();
    home.clamp_selection();
    assert!(
        home.selected < home.visible().len(),
        "selection must stay inside the shrunken list"
    );
    home.select_first_entry();
    assert_eq!(
        home.selected_entry().map(|e| e.name),
        Some("aaa.parquet".to_string())
    );
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

#[test]
fn test_compact_formatting() {
    assert_eq!(discover::format_rows(950), "950");
    assert_eq!(discover::format_rows(89_000), "89k");
    assert_eq!(discover::format_rows(2_400_000), "2.4M");
    assert_eq!(discover::format_size(500), "500 B");
    assert_eq!(discover::format_size(1536), "1.5 KB");
}

// ---------------------------------------------------------------------------
// Glyph fallback — datui has to be readable over SSH on a plain server
// ---------------------------------------------------------------------------

#[test]
fn test_ascii_and_unicode_sets_cover_the_same_symbols() {
    let u = datui::glyphs::unicode();
    let a = datui::glyphs::ascii();
    for (name, uv, av) in [
        ("selector", u.selector, a.selector),
        ("cursor", u.cursor, a.cursor),
        ("prompt", u.prompt, a.prompt),
        ("rule", u.rule, a.rule),
        ("ellipsis", u.ellipsis, a.ellipsis),
        ("times", u.times, a.times),
        ("updown", u.updown, a.updown),
    ] {
        assert!(!uv.is_empty(), "{name} unicode must not be empty");
        assert!(!av.is_empty(), "{name} ascii must not be empty");
    }
}

#[test]
fn test_ascii_set_is_actually_ascii() {
    // The point of the fallback is that nothing in it can render as a replacement
    // box, so every byte must be plain ASCII.
    let a = datui::glyphs::ascii();
    for (name, value) in [
        ("selector", a.selector),
        ("selector_blank", a.selector_blank),
        ("cursor", a.cursor),
        ("prompt", a.prompt),
        ("rule", a.rule),
        ("ellipsis", a.ellipsis),
        ("times", a.times),
        ("updown", a.updown),
    ] {
        assert!(
            value.is_ascii(),
            "{name} must be ASCII in the fallback set, got {value:?}"
        );
    }
}

#[test]
fn test_selector_and_blank_are_the_same_width() {
    // Rows must not shift horizontally as the selection moves.
    for set in [datui::glyphs::unicode(), datui::glyphs::ascii()] {
        assert_eq!(
            set.selector.chars().count(),
            set.selector_blank.chars().count(),
            "selector and its blank must align"
        );
    }
}

#[test]
fn test_no_nerd_font_glyphs_in_either_set() {
    // Nerd Font icons live in the Private Use Area. datui's own UI must not use them:
    // they render as tofu anywhere the patched font is not installed.
    for set in [datui::glyphs::unicode(), datui::glyphs::ascii()] {
        for value in [
            set.selector,
            set.cursor,
            set.prompt,
            set.rule,
            set.ellipsis,
            set.times,
            set.updown,
        ] {
            for ch in value.chars() {
                let c = ch as u32;
                let private_use = (0xE000..=0xF8FF).contains(&c)
                    || (0xF0000..=0xFFFFD).contains(&c)
                    || (0x100000..=0x10FFFD).contains(&c);
                assert!(!private_use, "{value:?} contains a Private Use glyph");
            }
        }
    }
}

#[test]
fn test_filtered_results_stay_grouped_by_where_they_came_from() {
    // A dataset can be both recent and present in a listed directory. Grouped, the
    // headers explain that; filtered, the headers are gone and the repeat just looks
    // like a bug.
    let tmp = TempDir::new().unwrap();
    let dataset = touch(tmp.path(), "sales.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[tmp.path().to_path_buf()], std::slice::from_ref(&dataset));
    home.filter = "sales".to_string();

    // Grouped results keep provenance, so the same dataset can legitimately appear
    // under RECENT and again under the directory it lives in — each under a heading
    // that says which. What must not happen is a repeat inside one section.
    for section in 0..home.sections.len() {
        let in_section: Vec<_> = home
            .visible()
            .iter()
            .filter_map(|r| match r {
                Row::Entry {
                    entry, section: s, ..
                } if *s == section => Some(entry.path.clone()),
                _ => None,
            })
            .collect();
        let mut deduped = in_section.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            in_section.len(),
            deduped.len(),
            "a dataset should appear once within a section"
        );
    }
}

// ---------------------------------------------------------------------------
// Desktop recents — roots, never rows
// ---------------------------------------------------------------------------

#[test]
fn test_desktop_recents_yield_directories_not_files() {
    // The whole design of this feature: the desktop's recently-used list routinely
    // holds things nobody wants on a screen they are sharing. Offering the directory
    // as somewhere to look is useful; listing the file is not datui's business.
    let tmp = TempDir::new().unwrap();
    let dataset = touch(tmp.path(), "quarterly.csv");

    let xbel = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <xbel version="1.0">
          <bookmark href="file://{}" added="2026-01-01T00:00:00Z"/>
        </xbel>"#,
        dataset.display()
    );

    let dirs = datui::home::dirs_from_xbel(&xbel);
    assert_eq!(dirs, vec![tmp.path().to_path_buf()]);
    assert!(
        !dirs.iter().any(|d| d.ends_with("quarterly.csv")),
        "a file must never come back from this"
    );
}

#[test]
fn test_desktop_recents_ignore_non_data_and_missing_files() {
    let tmp = TempDir::new().unwrap();
    let data = touch(tmp.path(), "real.parquet");
    let doc = touch(tmp.path(), "notes.odt");

    let xbel = format!(
        r#"<xbel>
          <bookmark href="file://{}"/>
          <bookmark href="file://{}"/>
          <bookmark href="file:///nowhere/at/all/ghost.csv"/>
        </xbel>"#,
        data.display(),
        doc.display()
    );

    // The data file's directory is offered once; a document datui cannot open and a
    // path that no longer exists contribute nothing.
    assert_eq!(
        datui::home::dirs_from_xbel(&xbel),
        vec![tmp.path().to_path_buf()]
    );
}

#[test]
fn test_desktop_recents_decode_percent_escapes() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("my data");
    let dataset = touch(&dir, "a.csv");
    let encoded = dataset.display().to_string().replace(' ', "%20");

    let xbel = format!(r#"<xbel><bookmark href="file://{encoded}"/></xbel>"#);
    assert_eq!(datui::home::dirs_from_xbel(&xbel), vec![dir]);
}

#[test]
fn test_desktop_roots_rank_below_everything_else() {
    // They are the weakest signal: useful only before datui has recents of its own.
    let tmp = TempDir::new().unwrap();
    let configured = tmp.path().join("configured");
    let downloads = tmp.path().join("downloads");
    fs::create_dir_all(&configured).unwrap();
    fs::create_dir_all(&downloads).unwrap();

    let roots = HomeState::roots(
        std::slice::from_ref(&configured),
        std::slice::from_ref(&downloads),
    );
    let origins: Vec<RootOrigin> = roots.iter().map(|r| r.origin).collect();
    let desktop_at = origins
        .iter()
        .position(|o| *o == RootOrigin::Desktop)
        .expect("desktop root present");
    assert_eq!(
        desktop_at,
        origins.len() - 1,
        "desktop-derived roots must come last"
    );
}

#[test]
fn test_desktop_recents_can_be_turned_off() {
    use datui::config::DataConfig;
    let default = DataConfig::default();
    assert!(default.use_desktop_recents, "on by default");

    let off: DataConfig = toml::from_str("use_desktop_recents = false").expect("parses");
    assert!(!off.use_desktop_recents);
}

#[test]
fn test_desktop_places_are_listed_but_never_expanded() {
    // The guarantee this feature rests on: a directory the desktop mentioned appears
    // as somewhere to step into, and nothing inside it is listed until you ask. The
    // desktop's recently-used list routinely holds files nobody wants on a shared
    // screen — a bank export, a vault dump — and they are all valid data files.
    let tmp = TempDir::new().unwrap();
    let downloads = tmp.path().join("downloads");
    touch(&downloads, "vault_export.csv");

    let mut home = HomeState::default();
    home.rebuild_with(&[], &[], std::slice::from_ref(&downloads));
    // Elsewhere starts folded; open it so its rows are on screen.
    let elsewhere = home
        .sections
        .iter()
        .position(|s| s.title == "Elsewhere")
        .expect("an Elsewhere section");
    home.set_collapsed(elsewhere, false);

    let names = visible_names(&home);
    assert!(
        !names.iter().any(|n| n.contains("vault_export")),
        "a file inside a desktop-derived place must not be listed: {names:?}"
    );
    assert!(
        home.visible().iter().any(|r| matches!(
            r,
            Row::Entry { entry, .. } if entry.path == downloads && entry.kind == EntryKind::Directory
        )),
        "the place itself should be offered as a directory: {names:?}"
    );
}

#[test]
fn test_desktop_place_contents_appear_only_after_descending() {
    let tmp = TempDir::new().unwrap();
    let downloads = tmp.path().join("downloads");
    touch(&downloads, "vault_export.csv");

    let mut home = HomeState {
        browsing: Some(downloads.clone()),
        ..Default::default()
    };
    home.rebuild_with(&[], &[], std::slice::from_ref(&downloads));

    assert!(
        visible_names(&home).contains(&"vault_export.csv".to_string()),
        "descending is the explicit ask, and then contents show normally"
    );
}

#[test]
fn test_desktop_place_already_covered_is_not_repeated() {
    // If the place is already a configured root it is expanded there; it must not
    // also show up as an unexpanded "elsewhere" row.
    let tmp = TempDir::new().unwrap();
    let shared = tmp.path().join("data");
    touch(&shared, "a.parquet");

    let mut home = HomeState::default();
    home.rebuild_with(
        std::slice::from_ref(&shared),
        &[],
        std::slice::from_ref(&shared),
    );

    let places = home
        .sections
        .iter()
        .filter(|s| s.title == "Elsewhere")
        .count();
    assert_eq!(
        places, 0,
        "a configured root should not repeat as elsewhere"
    );
}

// ---------------------------------------------------------------------------
// Abandoning a load
// ---------------------------------------------------------------------------

/// A stale background result must be dropped rather than applied.
///
/// This is what makes leaving a slow load safe: the work keeps running (Polars has
/// no cancellation), so the only thing standing between an abandoned load and a
/// clobbered screen is the generation check.
#[test]
fn test_stale_background_scan_is_discarded() {
    use datui::{App, AppEvent, OpenOptions};
    use std::sync::mpsc;

    let (tx, _rx) = mpsc::channel::<AppEvent>();
    let theme = datui::config::Theme::from_config(&Default::default()).expect("theme");
    let mut app = App::new_with_theme(tx, common::test_runtime(), theme);

    let current = app.task_generation();

    // A result from a generation the app has moved past produces no follow-up work.
    let stale = AppEvent::BackgroundLazyFrameReady {
        generation: current.wrapping_sub(1),
        path: Some(std::path::PathBuf::from("whatever.parquet")),
        options: OpenOptions::default(),
    };
    assert!(
        app.event(&stale).is_none(),
        "a superseded scan must not continue the load pipeline"
    );
}

// ---------------------------------------------------------------------------
// Collapsing sections
// ---------------------------------------------------------------------------

fn home_with_two_sections() -> (TempDir, HomeState) {
    let tmp = TempDir::new().unwrap();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    touch(&a, "one.parquet");
    touch(&a, "two.parquet");
    touch(&b, "three.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[a, b], &[]);
    (tmp, home)
}

#[test]
fn test_collapsing_hides_a_sections_rows_but_keeps_its_header() {
    let (_tmp, mut home) = home_with_two_sections();
    let rows_before = visible_names(&home).len();
    let headers = |h: &HomeState| {
        h.visible()
            .iter()
            .filter(|r| matches!(r, Row::Header { .. }))
            .count()
    };
    let headers_before = headers(&home);
    assert!(rows_before >= 3);

    home.toggle_collapsed(0);

    assert_eq!(
        headers(&home),
        headers_before,
        "a collapsed section keeps its header"
    );
    assert!(
        visible_names(&home).len() < rows_before,
        "collapsing should hide rows"
    );
}

#[test]
fn test_collapsed_header_reports_what_it_is_hiding() {
    // A collapsed section with no count looks like an empty one.
    let (_tmp, mut home) = home_with_two_sections();
    home.set_collapsed(0, true);

    let header = home
        .visible()
        .into_iter()
        .find(|r| matches!(r, Row::Header { section: 0, .. }))
        .expect("header present");
    match header {
        Row::Header {
            matches, collapsed, ..
        } => {
            assert!(collapsed);
            assert!(
                matches > 0,
                "a collapsed header should still count its rows"
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_collapse_state_survives_a_rebuild() {
    // Rebuilding renumbers sections, so the state is keyed by title rather than index.
    let (_tmp, mut home) = home_with_two_sections();
    let title = home.sections[0].title.clone();
    home.set_collapsed(0, true);

    home.rebuild(&[], &[]);
    let idx = home.sections.iter().position(|s| s.title == title);
    if let Some(idx) = idx {
        assert!(home.is_collapsed(idx), "collapse should survive a rebuild");
    }
}

#[test]
fn test_expanding_restores_the_rows() {
    let (_tmp, mut home) = home_with_two_sections();
    let before = visible_names(&home);

    home.toggle_collapsed(0);
    home.toggle_collapsed(0);

    assert_eq!(visible_names(&home), before);
}

#[test]
fn test_selection_starts_on_a_dataset_not_a_header() {
    let (_tmp, home) = home_with_two_sections();
    assert!(
        home.selected_entry().is_some(),
        "the preview pane needs something to show without a keypress"
    );
    assert!(!home.selection_is_header());
}

// ---------------------------------------------------------------------------
// Measuring rows is lazy
//
// Enriching during rebuild meant every dataset under every root paid for a footer
// walk before the first frame. On a directory holding a dozen large datasets that
// is hundreds of file reads, and the home screen does not appear for tens of
// seconds. These pin the shape of the fix.
// ---------------------------------------------------------------------------

#[test]
fn test_rebuild_does_not_measure_anything() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "a.parquet");
    touch(tmp.path(), "b.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    assert!(
        home.enriched.is_empty(),
        "rebuild must not read any footers; that is what stalls the first paint"
    );
    for row in home.visible() {
        if let Row::Entry { entry, .. } = row {
            assert!(entry.rows.is_none() && entry.cols.is_none());
        }
    }
}

#[test]
fn test_enrichment_is_capped_per_pass_and_reports_more_work() {
    let tmp = TempDir::new().unwrap();
    for i in 0..6 {
        touch(tmp.path(), &format!("f{i}.parquet"));
    }

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let more = home.measure_now(2);
    // The six files. The row that opens the directory holding them is not measured: its
    // path is the directory's, so a measurement of it lands in the slot the directory's
    // own row uses one level up.
    assert!(more, "with 6 rows and a budget of 2, work must remain");
    assert_eq!(home.enriched.len(), 2, "a pass spends only its budget");

    home.measure_now(2);
    assert_eq!(
        home.enriched.len(),
        4,
        "the next pass continues where it left off"
    );

    let more = home.measure_now(10);
    assert_eq!(home.enriched.len(), 6);
    assert!(!more, "nothing left to measure");
}

#[test]
fn test_enrichment_only_touches_rows_that_are_on_screen() {
    let tmp = TempDir::new().unwrap();
    for i in 0..20 {
        touch(tmp.path(), &format!("f{i:02}.parquet"));
    }

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    // A short window measures a short list, however many datasets exist.
    home.measure_now(3);
    assert!(
        home.enriched.len() <= 3,
        "measured {} rows for a 3-row window",
        home.enriched.len()
    );
}

#[test]
fn test_collapsed_sections_are_not_measured() {
    // Folding a section should make it cheaper, not just shorter.
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    for i in 0..4 {
        touch(&dir, &format!("f{i}.parquet"));
    }

    let mut home = HomeState::default();
    home.rebuild(std::slice::from_ref(&dir), &[]);
    let section = home
        .sections
        .iter()
        .position(|s| s.rows.iter().any(|r| r.name.starts_with("f0")))
        .expect("section present");
    home.set_collapsed(section, true);

    home.measure_now(100);
    let measured_in_section = home.enriched.keys().filter(|p| p.starts_with(&dir)).count();
    assert_eq!(
        measured_in_section, 0,
        "a folded section's rows are not drawn, so they must not be read"
    );
}

#[test]
fn test_network_detection_reads_the_mount_table() {
    use datui::home::is_network_path;

    // A local path must not be flagged. Anything unusual about the mount table is
    // treated as "not network", so this is a hint and never a gate.
    let tmp = TempDir::new().unwrap();
    assert!(!is_network_path(tmp.path()));
    assert!(!is_network_path(std::path::Path::new(
        "/definitely/not/mounted"
    )));
}

#[test]
fn test_network_detection_prefers_the_deepest_and_last_mount() {
    use datui::home::network_fs_for_test;

    // Two entries can share a mount point: an NFS share automounted at a path is
    // listed after the autofs entry covering the same path, and it is the NFS entry
    // that describes what a read will actually do. Keeping the first match reports
    // the automount and misses the network entirely.
    let shadowed = "\
25 1 0:22 / / rw - btrfs /dev/mapper/root rw
30 25 0:44 / /mnt/nas/data rw - autofs systemd-1 rw
81 30 0:57 / /mnt/nas/data rw - nfs4 nas:/volume1/data rw
";
    assert!(network_fs_for_test(
        shadowed,
        std::path::Path::new("/mnt/nas/data/sets/returns")
    ));

    // The parent of a network mount is whatever the parent actually is.
    assert!(!network_fs_for_test(
        shadowed,
        std::path::Path::new("/mnt/nas")
    ));

    // A local mount nested under a network one wins, being the closer answer.
    let nested = "\
25 1 0:22 / / rw - nfs4 server:/export rw
30 25 0:44 / /scratch rw - ext4 /dev/sdb1 rw
";
    assert!(!network_fs_for_test(
        nested,
        std::path::Path::new("/scratch/work")
    ));
    assert!(network_fs_for_test(
        nested,
        std::path::Path::new("/elsewhere")
    ));
}

#[test]
fn test_a_vanished_recent_leaves_nothing_behind() {
    // The directory of a recent used to be promoted to a root before the recent
    // itself was checked, so a dataset deleted with its directory left a section
    // titled by a path that no longer existed. A recent that is gone is gone.
    let tmp = TempDir::new().unwrap();
    let gone = tmp.path().join("mount/data");
    let dataset = touch(&gone, "sales.parquet");
    fs::remove_dir_all(tmp.path().join("mount")).unwrap();

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&dataset));

    assert!(
        !home.sections.iter().any(|s| s.unavailable),
        "a vanished recent must not be reported as an unavailable root"
    );
    assert!(
        !home
            .visible()
            .iter()
            .any(|r| matches!(r, Row::Place { path, .. } if *path == gone)),
        "nor as a place with nothing under it"
    );
}

#[test]
fn test_a_directory_that_is_only_a_recents_parent_is_not_a_section() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("somewhere");
    let dataset = touch(&dir, "opened_once.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&dataset));

    let title = datui::home::display_path(&dir);
    assert!(
        !home.sections.iter().any(|s| s.title == title),
        "only the current directory and configured directories are titled by a path"
    );
}

// ---------------------------------------------------------------------------
// Network paths are never touched on the interface thread
//
// An unreachable NFS share does not fail — it blocks. On a `soft` mount that is
// seconds per call; on a `hard` mount, which is the default, it is indefinite and
// uninterruptible, so datui cannot even be killed. Classifying a path as remote
// reads only /proc/self/mountinfo, so the listing can be built without touching
// the remote at all, and the actual reading happens on a thread that is allowed
// to block forever.
// ---------------------------------------------------------------------------

/// Treat everything under a marker directory as if it were a network mount.
fn pretend_remote(path: &std::path::Path) -> bool {
    path.components()
        .any(|c| c.as_os_str() == std::ffi::OsStr::new("PRETEND_REMOTE"))
}

#[test]
fn test_a_remote_root_is_listed_without_being_read() {
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");
    touch(&remote, "should_not_be_listed.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.rebuild(std::slice::from_ref(&remote), &[]);

    // The root appears...
    let section = home
        .sections
        .iter()
        .find(|s| s.title.contains("PRETEND_REMOTE"))
        .expect("a remote root should still be offered");
    assert!(
        section
            .subtitle
            .as_deref()
            .unwrap_or("")
            .contains("network"),
        "it should be marked as network: {:?}",
        section.subtitle
    );

    // ...but its contents were not read, even though they exist on disk.
    assert!(
        !visible_names(&home)
            .iter()
            .any(|n| n.contains("should_not_be_listed")),
        "listing a remote root inline is what freezes datui on a dead network"
    );
    assert_eq!(
        home.pending_probes(),
        vec![remote],
        "it should be queued for an off-thread probe instead"
    );
}

#[test]
fn test_a_share_named_by_its_filesystem_is_still_probed_and_shown() {
    // A root on an NFS mount is subtitled "nfs4 · recent", not "network · recent".
    // Probing used to key off that word, so such a root was never listed, and with
    // no rows its section was hidden.
    let root = std::path::PathBuf::from("/mnt/share/sets");
    let mut home = HomeState::default();
    home.apply_listing(datui::home::Listing {
        sections: vec![datui::home::Section {
            door: None,
            title: "/mnt/share/sets".into(),
            subtitle: Some("nfs4 · recent".into()),
            origin: None,
            rows: Vec::new(),
            unavailable: false,
            unavailable_note: None,
            folded_by_default: true,
            remote_root: Some(root.clone()),
            waiting: true,
            grouped_by_place: false,
            place_labels: Default::default(),
        }],
    });

    assert_eq!(home.pending_probes(), vec![root.clone()]);
    assert!(
        home.visible()
            .iter()
            .any(|r| matches!(r, Row::Header { .. })),
        "a section still being listed should be on screen, not hidden as empty"
    );

    home.probe_ready(root, Vec::new());
    assert!(home.pending_probes().is_empty());
}

#[test]
fn test_a_probe_result_fills_the_remote_root_in() {
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");
    touch(&remote, "sales.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.rebuild(std::slice::from_ref(&remote), &[]);

    // Whatever the probe thread found is what gets shown.
    let rows = discover::scan_dir(&remote);
    home.probe_ready(remote.clone(), rows);
    home.rebuild(std::slice::from_ref(&remote), &[]);

    assert!(
        visible_names(&home).iter().any(|n| n == "sales.parquet"),
        "a completed probe should populate the section"
    );
    assert!(
        home.pending_probes().is_empty(),
        "an answered root should not be probed again"
    );
}

#[test]
fn test_a_root_that_never_answers_is_marked_unreachable() {
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.rebuild(std::slice::from_ref(&remote), &[]);
    home.probe_failed(remote.clone());
    home.rebuild(std::slice::from_ref(&remote), &[]);

    let section = home
        .sections
        .iter()
        .find(|s| s.title.contains("PRETEND_REMOTE"))
        .expect("still listed");
    assert!(section.unavailable, "a share that did not answer says so");
    assert!(
        home.pending_probes().is_empty(),
        "a written-off root must not be retried; the thread is unreclaimable"
    );
}

#[test]
fn test_remote_rows_are_left_to_their_root_probe() {
    // Remote rows are measured by the probe that lists their root, which is already
    // reading that filesystem. Measuring them again here would put a second thread on
    // a share that may never answer, and a thread wedged on a `hard` mount is never
    // reclaimed.
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");
    touch(&remote, "a.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        browsing: Some(remote.clone()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.measure_now(100);

    assert!(
        home.enriched.is_empty(),
        "a remote row should not be queued for the measurement pass"
    );
}

#[test]
fn test_a_remote_recent_is_shown_without_stat() {
    // `exists()` and `metadata()` both stat, so a remote recent is taken on trust.
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data/sales.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.rebuild(&[], std::slice::from_ref(&remote));

    assert!(
        visible_names(&home).iter().any(|n| n == "sales.parquet"),
        "a remote recent should be listed even though it was never stat'ed"
    );
}

// ---------------------------------------------------------------------------
// Object-store and HTTP URLs
// ---------------------------------------------------------------------------

#[test]
fn test_urls_are_treated_as_remote() {
    use datui::home::is_remote_path;
    for url in [
        "s3://bucket/warehouse/events",
        "s3a://bucket/x.parquet",
        "gs://bucket/data",
        "gcs://bucket/data",
        "https://example.com/data.csv",
        "http://example.com/data.csv",
    ] {
        assert!(
            is_remote_path(std::path::Path::new(url)),
            "{url} should never be touched on the interface thread"
        );
    }
    assert!(!is_remote_path(std::path::Path::new("/tmp/local.parquet")));
}

#[test]
fn test_a_recent_url_is_listed_without_being_reached_for() {
    // `s3://bucket/warehouse/events/year=2024` is the path most worth remembering
    // and the least practical to retype, so it belongs in recents — but resolving it
    // means a network call, which the interface thread must never make.
    let url = std::path::PathBuf::from("s3://bucket/warehouse/events.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&url));

    let names = visible_names(&home);
    assert!(
        names.iter().any(|n| n == "events.parquet"),
        "a recent URL should be listed: {names:?}"
    );
}

#[test]
fn test_a_url_is_classified_by_name_not_by_stat() {
    let mut home = HomeState::default();
    let file = std::path::PathBuf::from("s3://bucket/data/sales.parquet");
    let prefix = std::path::PathBuf::from("s3://bucket/data/warehouse");
    home.rebuild(&[], &[file, prefix]);

    let kinds: Vec<_> = home
        .visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some((entry.name.clone(), entry.kind)),
            _ => None,
        })
        .collect();

    assert!(
        kinds.contains(&("sales.parquet".to_string(), EntryKind::File)),
        "an extension marks a dataset: {kinds:?}"
    );
    assert!(
        kinds.contains(&("warehouse".to_string(), EntryKind::Unknown)),
        "a prefix without one stays unclassified rather than being called a plain \
         directory, which would contradict how it reads once probed: {kinds:?}"
    );
}

#[test]
fn test_a_recent_adopts_the_classification_its_root_probe_found() {
    // The reported bug: a hive directory opened from the command line showed as
    // `hive` under its own root but `dir` under Recent, because the Recent row was
    // guessed from the name to avoid reading a remote path. Once the root's probe
    // has landed, that answer is authoritative and both rows must agree.
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("PRETEND_REMOTE/quant");
    let dataset = root.join("factors");
    touch(&dataset, "year=2024/part-0.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.rebuild(std::slice::from_ref(&root), std::slice::from_ref(&dataset));

    // Before the probe: unlabelled rather than wrong.
    let kind_of = |h: &HomeState| {
        h.visible().iter().find_map(|r| match r {
            Row::Entry { entry, .. } if entry.name == "factors" => Some(entry.kind),
            _ => None,
        })
    };
    assert_eq!(kind_of(&home), Some(EntryKind::Unknown));

    // After it, and after something looks into the rows it returned: whatever that
    // found. A probe lists a remote directory; it does not read every subdirectory in it.
    home.probe_ready(root.clone(), discover::scan_dir(&root));
    home.rebuild(std::slice::from_ref(&root), std::slice::from_ref(&dataset));
    home.classify_now(10);

    let kinds: Vec<_> = home
        .visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } if entry.name == "factors" => Some(entry.kind),
            _ => None,
        })
        .collect();
    assert!(
        kinds.iter().all(|k| *k == EntryKind::Hive),
        "every row for the same dataset should agree: {kinds:?}"
    );
}

// ---------------------------------------------------------------------------
// Hazards that are not the network
//
// Guarding by category does not work, because the list of ways a filesystem call
// can block is open-ended: a FIFO, a device node, a socket, a FUSE mount nobody
// classified, a disk that has stopped answering. What follows pins the two
// defences — refuse to read anything that is not a regular file, and never read
// anything at all on the thread that draws.
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[test]
fn test_a_fifo_named_like_a_dataset_is_not_offered() {
    // Opening a FIFO blocks until a writer appears — for a named pipe nobody is
    // writing to, that is forever. A directory listing reports it as `x.parquet`
    // like anything else, so it has to be rejected on kind, before any open.
    use std::os::unix::fs::FileTypeExt;

    let tmp = TempDir::new().unwrap();
    let fifo = tmp.path().join("blocker.parquet");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo");
    assert!(status.success());
    assert!(fs::metadata(&fifo).unwrap().file_type().is_fifo());
    touch(tmp.path(), "real.parquet");

    let names = discover::scan_dir(tmp.path())
        .into_iter()
        .map(|e| e.name)
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec!["real.parquet"],
        "a pipe must never be offered as a dataset: {names:?}"
    );
}

#[cfg(unix)]
#[test]
fn test_measuring_a_directory_holding_a_fifo_completes() {
    // The regression: this hung forever, locally, with no network involved.
    let tmp = TempDir::new().unwrap();
    let fifo = tmp.path().join("blocker.parquet");
    std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.measure_now(100); // completes, rather than blocking on the pipe
}

#[test]
fn test_a_symlink_cycle_does_not_run_away() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    fs::create_dir_all(&dir).unwrap();
    touch(&dir, "a.parquet");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&dir, dir.join("self")).unwrap();

    let mut home = HomeState {
        browsing: Some(dir.clone()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.measure_now(100);
    // Reaching here at all is the assertion: depth and breadth caps hold.
    assert!(!home.visible().is_empty());
}

#[test]
fn test_listing_can_be_built_away_from_the_state_it_updates() {
    // The listing is produced by a free function taking a request, so it can run on
    // a worker. If this ever needs `&HomeState`, the interface thread is doing the
    // reading again.
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales.parquet");

    let request = ListingRequest {
        config_dirs: vec![tmp.path().to_path_buf()],
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: Default::default(),
    };

    // Built on another thread entirely, then handed over.
    let listing = std::thread::spawn(move || build_listing(&request))
        .join()
        .expect("listing thread");

    let mut home = HomeState::default();
    home.apply_listing(listing);
    assert!(visible_names(&home).iter().any(|n| n == "sales.parquet"));
}

#[test]
fn test_applying_a_listing_keeps_the_cursor_where_it_was() {
    // Background results arrive continuously; landing back at the top each time
    // would make the screen unusable.
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "a.parquet");
    touch(tmp.path(), "b.parquet");
    touch(tmp.path(), "c.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.move_selection(1);
    home.move_selection(1);
    let held = home.selected_entry().map(|e| e.path);
    assert!(held.is_some());

    home.rebuild(&[], &[]); // as if a worker delivered a fresh listing
    assert_eq!(
        home.selected_entry().map(|e| e.path),
        held,
        "a refresh should not move the cursor"
    );
}

// ---------------------------------------------------------------------------
// Searching by column
//
// "Which of these has a customer_id?" is the question a data person actually has,
// and the answer is already in the Parquet footer datui read to get the row count.
// ---------------------------------------------------------------------------

fn entry_with_columns(name: &str, columns: &[&str]) -> datui::discover::Entry {
    datui::discover::Entry {
        path: std::path::PathBuf::from(name),
        kind: EntryKind::File,
        name: name.to_string(),
        size: None,
        modified: None,
        rows: None,
        cols: Some(columns.len()),
        cols_sampled: false,
        columns: columns.iter().map(|c| c.to_string()).collect(),
        cost: Default::default(),
        holds: Default::default(),
        opens_whole_directory: false,
    }
}

#[test]
fn test_a_filter_matches_column_names() {
    use datui::home::{match_score, matching_column};

    let sales = entry_with_columns("sales.parquet", &["order_id", "customer_id", "amount"]);
    let weather = entry_with_columns("weather.parquet", &["station", "temp_c"]);

    assert!(match_score("customer_id", &sales).is_some());
    assert!(match_score("customer_id", &weather).is_none());
    assert_eq!(matching_column("customer_id", &sales), Some("customer_id"));
}

#[test]
fn test_column_matches_rank_below_name_matches() {
    // Typing a dataset's name must still find the dataset first; columns are an
    // addition to the filter, not a dilution of it.
    use datui::home::match_score;

    let by_name = entry_with_columns("customer_id.parquet", &["unrelated"]);
    let by_column = entry_with_columns("sales.parquet", &["customer_id"]);

    let name_score = match_score("customer_id", &by_name).expect("name match");
    let column_score = match_score("customer_id", &by_column).expect("column match");
    assert!(
        name_score > column_score,
        "name {name_score} should outrank column {column_score}"
    );
}

#[test]
fn test_column_search_is_case_insensitive_and_partial() {
    use datui::home::matching_column;
    let e = entry_with_columns("t.parquet", &["CustomerID", "ordered_at"]);
    assert_eq!(matching_column("customerid", &e), Some("CustomerID"));
    assert_eq!(matching_column("ORDERED", &e), Some("ordered_at"));
    assert_eq!(matching_column("nope", &e), None);
}

#[test]
fn test_an_empty_filter_matches_without_claiming_a_column() {
    use datui::home::{match_score, matching_column};
    let e = entry_with_columns("t.parquet", &["a"]);
    assert!(match_score("", &e).is_some());
    assert_eq!(
        matching_column("", &e),
        None,
        "an empty filter should not annotate every row with a column"
    );
}

#[test]
fn test_remembered_facts_are_used_only_for_the_same_bytes() {
    // The index is a cache: each entry carries the size and modification time it was
    // taken from, so a dataset that has changed invalidates itself.
    use datui::cache::{CacheManager, DatasetFacts};

    let tmp = TempDir::new().unwrap();
    let cache = CacheManager::with_dir(tmp.path().join("cache"));
    let dataset = touch(tmp.path(), "sales.parquet");
    let meta = fs::metadata(&dataset).unwrap();
    let mtime = meta
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    cache.record_dataset_facts(&[(
        dataset.clone(),
        DatasetFacts {
            mtime,
            size: meta.len(),
            rows: Some(42),
            cols: Some(3),
            cols_sampled: false,
            columns: vec!["customer_id".into()],
            kind: Some(EntryKind::File),
            classified_by: datui::discover::CLASSIFIER_VERSION,
            holds: Default::default(),
            cost: Default::default(),
        },
    )]);

    let known = cache.load_dataset_facts();
    assert_eq!(known.len(), 1);
    assert_eq!(known[&dataset].rows, Some(42));

    // A different fingerprint must not be trusted.
    let stale = DatasetFacts {
        size: meta.len() + 1,
        ..known[&dataset].clone()
    };
    assert_ne!(stale.size, meta.len(), "size is half the fingerprint");
}

#[test]
fn test_the_dataset_index_is_disposable() {
    // Deleting it must cost speed and nothing else.
    use datui::cache::CacheManager;

    let tmp = TempDir::new().unwrap();
    let cache = CacheManager::with_dir(tmp.path().to_path_buf());
    assert!(
        cache.load_dataset_facts().is_empty(),
        "a missing index reads as no knowledge, not an error"
    );

    fs::create_dir_all(tmp.path()).unwrap();
    fs::write(tmp.path().join("datasets.json"), b"{ not json").unwrap();
    assert!(
        cache.load_dataset_facts().is_empty(),
        "a corrupt index reads as no knowledge, not a crash"
    );
}

#[test]
fn test_a_recent_opened_from_a_bucket_shows_what_the_open_learned() {
    // The record an open writes is keyed by the URL as opened, which is what the
    // recents store holds, so the recent row and its place carry rows, columns, the
    // kind and the label with no request made.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let cache = CacheManager::with_dir(tmp.path().join("cache"));
    let dataset = std::path::PathBuf::from("s3://bucket/sales/");
    let place = std::path::PathBuf::from("s3://bucket");
    let facts = |kind, holds, rows| DatasetFacts {
        mtime: 20,
        size: 3000,
        rows,
        cols: Some(3),
        cols_sampled: false,
        columns: vec!["id".into(), "amount".into(), "year".into()],
        kind: Some(kind),
        classified_by: datui::discover::CLASSIFIER_VERSION,
        holds,
        cost: Default::default(),
    };
    let two_parquet = datui::discover::Holds {
        formats: vec![("parquet".to_string(), 2)],
        ..Default::default()
    };
    cache.record_dataset_facts(&[
        (
            dataset.clone(),
            facts(EntryKind::Hive, two_parquet.clone(), Some(20)),
        ),
        // The bucket itself was listed on some earlier run and is in the index too.
        (
            place.clone(),
            facts(EntryKind::MultiFile, two_parquet, None),
        ),
    ]);

    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: vec![dataset.clone()],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| true,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
    });
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    home.apply_listing(listing);

    let rows = home.visible();
    let row = rows
        .iter()
        .find_map(|r| match r {
            Row::Entry { entry, .. } if entry.path == dataset => Some((*entry).clone()),
            _ => None,
        })
        .expect("the recent is listed: {rows:?}");
    assert_eq!(row.rows, Some(20));
    assert_eq!(row.cols, Some(3));
    assert_eq!(row.kind, EntryKind::Hive);
    assert_eq!(row.label(), "hive");
    assert_eq!(row.columns.len(), 3);
    assert!(
        matches!(
            rows.iter().find(|r| matches!(r, Row::Place { .. })),
            Some(Row::Place { path, label: Some(label), .. })
                if *path == place && label == "2 parquet"
        ),
        "the place carries what the index remembers it to be: {rows:?}"
    );
}

#[test]
fn test_a_recent_typed_through_a_named_source_finds_the_record_its_open_wrote() {
    // An open records what it learned under the URL it resolved to, without the
    // source id and in the one Azure spelling; the recent is stored as typed. They
    // have to meet, or a dataset opened through a named source is blank under RECENT
    // however often it is opened.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing, index_key};
    use std::path::{Path, PathBuf};

    assert_eq!(
        index_key(Path::new("s3://lab@bucket/sales/")),
        PathBuf::from("s3://bucket/sales/")
    );
    assert_eq!(
        index_key(Path::new("https://acct.blob.core.windows.net/c/p/")),
        PathBuf::from("abfss://c@acct.dfs.core.windows.net/p/")
    );
    assert_eq!(
        index_key(Path::new("gs://bucket/x.parquet")),
        PathBuf::from("gs://bucket/x.parquet")
    );

    let tmp = TempDir::new().unwrap();
    let cache = CacheManager::with_dir(tmp.path().join("cache"));
    let facts = DatasetFacts {
        mtime: 20,
        size: 3000,
        rows: Some(20),
        cols: Some(3),
        cols_sampled: false,
        columns: vec!["id".into(), "amount".into(), "year".into()],
        kind: Some(EntryKind::Hive),
        classified_by: datui::discover::CLASSIFIER_VERSION,
        holds: Default::default(),
        cost: Default::default(),
    };
    cache.record_dataset_facts(&[
        (PathBuf::from("s3://bucket/sales/"), facts.clone()),
        (
            PathBuf::from("abfss://c@acct.dfs.core.windows.net/p/"),
            facts,
        ),
    ]);
    let typed = vec![
        PathBuf::from("s3://lab@bucket/sales/"),
        PathBuf::from("https://acct.blob.core.windows.net/c/p/"),
    ];
    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: typed.clone(),
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| true,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
    });
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    home.apply_listing(listing);
    for path in &typed {
        let row = home
            .visible()
            .iter()
            .find_map(|r| match r {
                Row::Entry { entry, .. } if entry.path == *path => Some((*entry).clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{path:?} is listed"));
        assert_eq!(row.rows, Some(20), "{path:?}");
        assert_eq!(row.kind, EntryKind::Hive, "{path:?}");
    }
}

#[test]
fn test_a_place_label_is_held_to_the_directories_mtime() {
    // A record of `12 parquet` for a directory that has since lost ten files would sit
    // two rows above the live listing calling it `2 parquet`. The label is shown only
    // while the directory's mtime is the one the record was taken at.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    let dataset = touch(&dir, "a.parquet");
    let mtime = fs::metadata(&dir)
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cache = CacheManager::with_dir(tmp.path().join("cache"));
    let record = |mtime| DatasetFacts {
        mtime,
        size: 0,
        rows: None,
        cols: None,
        cols_sampled: false,
        columns: Vec::new(),
        kind: Some(EntryKind::MultiFile),
        classified_by: datui::discover::CLASSIFIER_VERSION,
        holds: datui::discover::Holds {
            formats: vec![("parquet".to_string(), 12)],
            ..Default::default()
        },
        cost: Default::default(),
    };
    let label_for = |cache: &CacheManager| -> Option<String> {
        let listing = build_listing(&ListingRequest {
            config_dirs: Vec::new(),
            recents: vec![dataset.clone()],
            desktop_dirs: Vec::new(),
            browsing: None,
            probed: Default::default(),
            unreachable: Default::default(),
            probe_errors: Default::default(),
            network_check: |_| false,
            cloud: Vec::new(),
            known: cache.load_dataset_facts(),
        });
        let mut home = HomeState::default();
        home.apply_listing(listing);
        home.visible().iter().find_map(|r| match r {
            Row::Place { path, label, .. } if *path == dir => Some(label.clone()),
            _ => None,
        })?
    };

    cache.record_dataset_facts(&[(dir.clone(), record(mtime))]);
    assert_eq!(label_for(&cache).as_deref(), Some("12 parquet"));

    // The same record with another mtime: the directory has changed since, no label.
    cache.record_dataset_facts(&[(dir.clone(), record(mtime.wrapping_sub(100)))]);
    assert_eq!(label_for(&cache), None);
}

#[test]
fn test_a_place_row_says_nothing_it_does_not_know() {
    // No record, or a record from another classifier, or one that would only say
    // `dir`: no label. A place row never causes a read to find one.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    let dataset = touch(&dir, "a.parquet");
    let cache = CacheManager::with_dir(tmp.path().join("cache"));
    cache.record_dataset_facts(&[(
        dir.clone(),
        DatasetFacts {
            mtime: 0,
            size: 0,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            kind: Some(EntryKind::Directory),
            classified_by: datui::discover::CLASSIFIER_VERSION,
            holds: Default::default(),
            cost: Default::default(),
        },
    )]);
    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: vec![dataset],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
    });
    let mut home = HomeState::default();
    home.apply_listing(listing);
    assert!(
        home.visible()
            .iter()
            .any(|r| matches!(r, Row::Place { path, label: None, .. } if *path == dir)),
        "{:?}",
        home.visible()
    );
}

#[test]
fn test_origin_is_a_chip_and_the_note_is_state_only() {
    // Why a section exists sits by the title; how it is doing sits by the rule. A
    // configured directory with nothing in it stays listed because of the former.
    let tmp = TempDir::new().unwrap();
    let configured = tmp.path().join("configured");
    fs::create_dir_all(&configured).unwrap();
    let elsewhere = tmp.path().join("downloads");
    touch(&elsewhere, "x.parquet");

    let mut home = HomeState::default();
    home.rebuild_with(
        std::slice::from_ref(&configured),
        &[],
        std::slice::from_ref(&elsewhere),
    );
    let section = home
        .sections
        .iter()
        .find(|s| s.title == datui::home::display_path(&configured))
        .expect("configured");
    assert_eq!(section.origin, Some("configured"));
    assert_eq!(section.subtitle, None, "nothing to say about a local disk");
    assert!(
        home.visible().iter().any(|r| matches!(
            r,
            Row::Header { section, .. } if home.sections[*section].origin == Some("configured")
        )),
        "an empty configured directory is still on screen"
    );
    let elsewhere = home
        .sections
        .iter()
        .find(|s| s.title == "Elsewhere")
        .expect("elsewhere");
    assert_eq!(elsewhere.origin, None);
    assert_eq!(
        elsewhere.subtitle, None,
        "the title already says what these are"
    );
}

#[test]
fn test_a_remote_row_uses_remembered_facts_without_a_stat() {
    // Verifying a fingerprint means stat'ing the path, which is the call that blocks
    // on a share that has gone away. A remote row therefore trusts what was recorded
    // from a real read — a stale row count beats an empty one, and these are the
    // datasets that are hardest to reach and most worth remembering.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let remote_root = tmp.path().join("PRETEND_REMOTE/quant");
    let dataset = remote_root.join("prices");
    let cache_dir = tmp.path().join("cache");
    let cache = CacheManager::with_dir(cache_dir);

    cache.record_dataset_facts(&[(
        dataset.clone(),
        DatasetFacts {
            mtime: 0,
            size: 1_600_000_000,
            rows: Some(17_399_008),
            cols: Some(39),
            cols_sampled: false,
            columns: vec!["vwap".into(), "ticker".into()],
            kind: Some(EntryKind::Hive),
            classified_by: datui::discover::CLASSIFIER_VERSION,
            holds: Default::default(),
            cost: Default::default(),
        },
    )]);

    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: vec![dataset.clone()],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: pretend_remote,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
    });

    let mut home = HomeState {
        network_check: pretend_remote,
        cloud: Vec::new(),
        ..Default::default()
    };
    home.apply_listing(listing);

    let row = home
        .visible()
        .into_iter()
        .find_map(|r| match r {
            Row::Entry { entry, .. } if entry.name == "prices" => Some(entry.clone()),
            _ => None,
        })
        .expect("the remote dataset should be listed");

    assert_eq!(
        row.rows,
        Some(17_399_008),
        "counts come from what was recorded"
    );
    assert_eq!(
        row.kind,
        EntryKind::Hive,
        "and so does the kind, rather than a guess"
    );
    assert!(
        row.columns.iter().any(|c| c == "vwap"),
        "so a column search works before anything is read"
    );
}

#[test]
fn test_a_changed_local_dataset_ignores_its_remembered_facts() {
    // The local half of the same rule: a fingerprint that no longer matches is not
    // trusted, so a dataset that has been rewritten is measured again.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let dataset = touch(tmp.path(), "sales.parquet");
    let cache = CacheManager::with_dir(tmp.path().join("cache"));

    cache.record_dataset_facts(&[(
        dataset.clone(),
        DatasetFacts {
            mtime: 1, // deliberately not the file's real mtime
            size: 999_999,
            rows: Some(1_000_000),
            cols: Some(9),
            cols_sampled: false,
            columns: vec!["stale".into()],
            kind: Some(EntryKind::File),
            classified_by: datui::discover::CLASSIFIER_VERSION,
            holds: Default::default(),
            cost: Default::default(),
        },
    )]);

    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: Some(tmp.path().to_path_buf()),
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
    });

    let mut home = HomeState::default();
    home.apply_listing(listing);

    let row = home
        .visible()
        .into_iter()
        .find_map(|r| match r {
            Row::Entry { entry, .. } if entry.name == "sales.parquet" => Some(entry.clone()),
            _ => None,
        })
        .expect("listed");
    assert_eq!(
        row.rows, None,
        "a mismatched fingerprint must not be trusted"
    );
    assert!(row.columns.is_empty());
}

// ---------------------------------------------------------------------------
// Sorting
// ---------------------------------------------------------------------------

fn sized(name: &str, size: u64, rows: usize) -> datui::discover::Entry {
    datui::discover::Entry {
        path: std::path::PathBuf::from(name),
        kind: EntryKind::File,
        name: name.to_string(),
        size: Some(size),
        modified: None,
        rows: Some(rows),
        cols: Some(1),
        cols_sampled: false,
        columns: Vec::new(),
        cost: Default::default(),
        holds: Default::default(),
        opens_whole_directory: false,
    }
}

fn home_with_rows(rows: Vec<datui::discover::Entry>) -> HomeState {
    let mut home = HomeState::default();
    home.apply_listing(datui::home::Listing {
        sections: vec![datui::home::Section {
            door: None,
            title: "TEST".into(),
            subtitle: None,
            origin: None,
            rows,
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            place_labels: Default::default(),
        }],
    });
    home
}

#[test]
fn test_sorting_by_size_and_rows() {
    use datui::home::SortMode;

    let mut home = home_with_rows(vec![
        sized("small.parquet", 100, 9_000),
        sized("huge.parquet", 9_000_000, 10),
        sized("middling.parquet", 5_000, 500),
    ]);

    home.sort = SortMode::Size;
    assert_eq!(
        visible_names(&home),
        vec!["huge.parquet", "middling.parquet", "small.parquet"]
    );

    home.sort = SortMode::Rows;
    assert_eq!(
        visible_names(&home),
        vec!["small.parquet", "middling.parquet", "huge.parquet"]
    );
}

#[test]
fn test_rows_with_nothing_to_sort_by_go_last() {
    // Treating unknown as zero would make "biggest first" open with a page of
    // datasets whose size simply has not been read yet.
    use datui::home::SortMode;

    let mut unknown = sized("unmeasured.parquet", 0, 0);
    unknown.size = None;
    unknown.rows = None;

    let mut home = home_with_rows(vec![unknown, sized("known.parquet", 10, 10)]);
    home.sort = SortMode::Size;
    assert_eq!(
        visible_names(&home),
        vec!["known.parquet", "unmeasured.parquet"]
    );
}

#[test]
fn test_sort_cycles_through_every_mode_and_returns() {
    use datui::home::SortMode;
    let mut mode = SortMode::default();
    let mut seen = vec![mode];
    for _ in 0..3 {
        mode = mode.next();
        seen.push(mode);
    }
    assert_eq!(mode.next(), SortMode::default(), "the cycle should close");
    assert_eq!(
        seen.len(),
        seen.iter().collect::<std::collections::HashSet<_>>().len(),
        "every step should be a different mode: {seen:?}"
    );
}

#[test]
fn test_the_filter_still_wins_over_the_sort() {
    // Sorting reorders what matched; it must not resurrect what did not.
    use datui::home::SortMode;
    let mut home = home_with_rows(vec![
        sized("alpha.parquet", 9_000_000, 1),
        sized("beta.parquet", 1, 1),
    ]);
    home.sort = SortMode::Size;
    home.filter = "beta".into();
    assert_eq!(visible_names(&home), vec!["beta.parquet"]);
}

// ---------------------------------------------------------------------------
// Path completion
//
// The path input is the way to reach somewhere datui has never seen. Typing a full
// path unaided is the kind of friction that stops people using an escape hatch at
// all.
// ---------------------------------------------------------------------------

#[test]
fn test_completion_extends_to_an_unambiguous_prefix() {
    use datui::home::complete_path;

    let tmp = TempDir::new().unwrap();
    fs::create_dir_all(tmp.path().join("warehouse_eu")).unwrap();
    fs::create_dir_all(tmp.path().join("warehouse_us")).unwrap();
    fs::create_dir_all(tmp.path().join("other")).unwrap();

    // Both candidates agree as far as "warehouse_", so completion goes that far and
    // stops: choosing between them would be guessing.
    let typed = format!("{}/war", tmp.path().display());
    let (completed, candidates) = complete_path(&typed);
    assert_eq!(candidates, 2);
    assert!(
        completed.ends_with("/warehouse_"),
        "should extend to where the candidates diverge, got {completed}"
    );

    // And no further: a second press adds nothing rather than picking one.
    let (again, _) = complete_path(&completed);
    assert_eq!(again, completed, "an ambiguous completion is idempotent");
}

#[test]
fn test_a_single_directory_completes_with_its_separator() {
    // So a second Tab descends rather than needing a slash typed by hand.
    use datui::home::complete_path;

    let tmp = TempDir::new().unwrap();
    fs::create_dir_all(tmp.path().join("datasets")).unwrap();

    let typed = format!("{}/data", tmp.path().display());
    let (completed, candidates) = complete_path(&typed);
    assert_eq!(candidates, 1);
    assert!(
        completed.ends_with("datasets/"),
        "a lone directory should gain its separator, got {completed}"
    );
}

#[test]
fn test_completion_leaves_an_unmatched_path_alone() {
    use datui::home::complete_path;

    let tmp = TempDir::new().unwrap();
    let typed = format!("{}/nothing_like_this", tmp.path().display());
    let (completed, candidates) = complete_path(&typed);
    assert_eq!(candidates, 0);
    assert_eq!(
        completed, typed,
        "nothing to complete means nothing changes"
    );
}

#[test]
fn test_completion_hides_dotfiles_unless_asked_for() {
    // Otherwise every completion in a home directory is dotfiles.
    use datui::home::complete_path;

    let tmp = TempDir::new().unwrap();
    fs::create_dir_all(tmp.path().join(".hidden")).unwrap();
    fs::create_dir_all(tmp.path().join("visible")).unwrap();

    let (_, all) = complete_path(&format!("{}/", tmp.path().display()));
    assert_eq!(
        all, 1,
        "a bare directory should offer only the visible entry"
    );

    let (completed, dotted) = complete_path(&format!("{}/.h", tmp.path().display()));
    assert_eq!(dotted, 1, "asking for a dot should find it");
    assert!(completed.ends_with(".hidden/"));
}

#[test]
fn test_completion_of_an_unreadable_directory_is_harmless() {
    use datui::home::complete_path;
    let (completed, candidates) = complete_path("/definitely/not/here/x");
    assert_eq!(candidates, 0);
    assert_eq!(completed, "/definitely/not/here/x");
}

#[test]
fn test_every_surviving_recent_is_listed() {
    // The store bounds how many are kept; the screen must not quietly bound it
    // again. A recent that is held but never shown is worse than one not held.
    let tmp = TempDir::new().unwrap();
    let recents: Vec<std::path::PathBuf> = (0..30)
        .map(|i| touch(tmp.path(), &format!("d{i:02}.parquet")))
        .collect();

    let mut home = HomeState::default();
    home.rebuild(&[], &recents);

    let listed = home
        .visible()
        .iter()
        .filter(|r| matches!(r, Row::Entry { section: 0, .. }))
        .count();
    assert_eq!(
        listed,
        recents.len(),
        "all {} recents should be listed, saw {listed}",
        recents.len()
    );
}

#[test]
fn test_a_recent_that_no_longer_exists_is_dropped() {
    let tmp = TempDir::new().unwrap();
    let gone = tmp.path().join("deleted.parquet");
    let kept = touch(tmp.path(), "kept.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], &[gone, kept]);

    let names = visible_names(&home);
    assert!(names.iter().any(|n| n == "kept.parquet"));
    assert!(
        !names.iter().any(|n| n == "deleted.parquet"),
        "a path that is gone should not be offered: {names:?}"
    );
}

#[test]
fn test_cwd_datasets_are_listed_without_ever_having_been_opened() {
    // Being in the directory is enough; nothing has to be in recents first.
    let tmp = TempDir::new().unwrap();
    let here = touch(tmp.path(), "never_opened.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    assert!(
        visible_names(&home)
            .iter()
            .any(|n| n == "never_opened.parquet"),
        "a dataset in the current directory should be listed on its own"
    );
    assert!(here.exists());
}

#[test]
fn test_scattered_recents_cost_no_directory_listings() {
    // Thirty recents in thirty directories used to be eight directory listings on
    // every rebuild, and eight sections titled by path. Now they are thirty rows
    // under thirty place rows, and nothing beyond the recents themselves is read.
    let tmp = TempDir::new().unwrap();
    let recents: Vec<std::path::PathBuf> = (0..30)
        .map(|i| touch(&tmp.path().join(format!("place{i}")), "data.parquet"))
        .collect();

    let mut home = HomeState::default();
    home.rebuild(&[], &recents);

    let titles: Vec<&str> = home.sections.iter().map(|s| s.title.as_str()).collect();
    assert!(
        !titles.iter().any(|t| t.contains("place")),
        "no recent's directory is a section: {titles:?}"
    );
    let places = home
        .visible()
        .iter()
        .filter(|r| matches!(r, Row::Place { .. }))
        .count();
    assert_eq!(places, 30, "one place row per directory");
}

#[test]
fn test_recents_in_one_directory_share_one_place_row() {
    let tmp = TempDir::new().unwrap();
    let one = tmp.path().join("shared");
    let newer = touch(&one, "part1.parquet");
    let older = touch(&one, "part0.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], &[newer.clone(), older.clone()]);

    let rows = home.visible();
    let places: Vec<&Row> = rows
        .iter()
        .filter(|r| matches!(r, Row::Place { .. }))
        .collect();
    assert_eq!(places.len(), 1, "one place for one directory: {rows:?}");
    assert!(
        matches!(places[0], Row::Place { path, held: 2, .. } if *path == one),
        "the place counts what it holds: {:?}",
        places[0]
    );
    let under: Vec<&std::path::Path> = rows
        .iter()
        .filter_map(|r| match r {
            Row::Entry {
                entry,
                nested: true,
                ..
            } => Some(entry.path.as_path()),
            _ => None,
        })
        .collect();
    assert_eq!(
        under,
        vec![newer.as_path(), older.as_path()],
        "both rows are under it, newest first"
    );
}

#[test]
fn test_places_are_ordered_by_their_newest_recent() {
    // Recents are newest first. A place opened once long ago but again just now is
    // the first place, however many older opens sit under the second.
    let tmp = TempDir::new().unwrap();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    let a_new = touch(&a, "new.parquet");
    let b_1 = touch(&b, "one.parquet");
    let b_2 = touch(&b, "two.parquet");
    let a_old = touch(&a, "old.parquet");

    let mut home = HomeState::default();
    home.rebuild(
        &[],
        &[a_new.clone(), b_1.clone(), b_2.clone(), a_old.clone()],
    );

    let shape: Vec<String> = home
        .visible()
        .iter()
        .filter(|r| r.section() == 0)
        .filter_map(|r| match r {
            Row::Place { path, .. } => Some(format!("{}/", path.display())),
            Row::Entry { entry, .. } => Some(entry.path.display().to_string()),
            _ => None,
        })
        .collect();
    let want: Vec<String> = [
        format!("{}/", a.display()),
        a_new.display().to_string(),
        a_old.display().to_string(),
        format!("{}/", b.display()),
        b_1.display().to_string(),
        b_2.display().to_string(),
    ]
    .to_vec();
    assert_eq!(shape, want);
}

#[test]
fn test_a_sort_orders_within_each_place_and_never_flattens_recent() {
    use datui::home::SortMode;

    let tmp = TempDir::new().unwrap();
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    let a_small = a.join("small.parquet");
    let a_big = a.join("big.parquet");
    let b_huge = b.join("huge.parquet");
    fs::create_dir_all(&a).unwrap();
    fs::create_dir_all(&b).unwrap();
    fs::write(&a_small, vec![0u8; 10]).unwrap();
    fs::write(&a_big, vec![0u8; 1000]).unwrap();
    fs::write(&b_huge, vec![0u8; 100_000]).unwrap();

    let mut home = HomeState {
        sort: SortMode::Size,
        ..Default::default()
    };
    // `a` is the newer place though `b` holds the biggest file.
    home.rebuild(&[], &[a_small.clone(), b_huge.clone(), a_big.clone()]);

    let shape: Vec<std::path::PathBuf> = home
        .visible()
        .iter()
        .filter(|r| r.section() == 0)
        .filter_map(|r| match r {
            Row::Place { path, .. } => Some(path.clone()),
            Row::Entry { entry, .. } => Some(entry.path.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        shape,
        vec![a.clone(), a_big, a_small, b.clone(), b_huge],
        "places keep their order; the rows inside each are by size"
    );
}

/// Thirty recents in ten places, three to a place, so a place row and its rows cost
/// four lines each.
fn ten_places_of_three(tmp: &TempDir) -> Vec<std::path::PathBuf> {
    (0..30)
        .map(|i| {
            touch(
                &tmp.path().join(format!("place{:02}", i / 3)),
                &format!("part{i}.parquet"),
            )
        })
        .collect()
}

#[test]
fn test_recent_shows_whole_places_up_to_a_third_of_the_screen() {
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let mut home = HomeState::default();
    home.rebuild(&[], &recents);

    // Thirty lines of list: ten for RECENT, which is two whole places (eight lines)
    // and not a third one (twelve).
    home.view_height = 30;
    fn recent(home: &HomeState) -> Vec<Row<'_>> {
        home.visible()
            .into_iter()
            .filter(|r| r.section() == 0)
            .collect()
    }
    let rows = recent(&home);
    let places = rows
        .iter()
        .filter(|r| matches!(r, Row::Place { .. }))
        .count();
    assert_eq!(places, 2, "{rows:?}");
    let entries = rows
        .iter()
        .filter(|r| matches!(r, Row::Entry { section: 0, .. }))
        .count();
    assert_eq!(entries, 6, "a place is shown whole or not at all");
    assert!(
        matches!(
            rows.last(),
            Some(Row::More {
                hidden: 24,
                places: 8,
                ..
            })
        ),
        "what is hidden is counted, rows and places both: {:?}",
        rows.last()
    );
    assert!(
        matches!(rows.first(), Some(Row::Header { matches: 30, .. })),
        "the header's count is the true count, not the shown one: {:?}",
        rows.first()
    );

    // Too short for even one place: one is shown anyway. RECENT with nothing in it
    // would be the section saying the opposite of what it holds.
    home.view_height = 6;
    let rows = recent(&home);
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, Row::Place { .. }))
            .count(),
        1,
        "{rows:?}"
    );
    assert!(matches!(
        rows.last(),
        Some(Row::More {
            hidden: 27,
            places: 9,
            ..
        })
    ));
}

#[test]
fn test_expanding_recent_shows_every_place_for_the_session() {
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let mut home = HomeState::default();
    home.rebuild(&[], &recents);
    home.view_height = 30;
    assert!(home.visible().iter().any(|r| matches!(r, Row::More { .. })));

    home.recent_expanded = true;
    let rows = home.visible();
    assert!(!rows.iter().any(|r| matches!(r, Row::More { .. })));
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, Row::Entry { section: 0, .. }))
            .count(),
        30
    );

    // A rebuild — a probe answering, a measurement landing — does not fold it back.
    home.rebuild(&[], &recents);
    assert!(
        !home.visible().iter().any(|r| matches!(r, Row::More { .. })),
        "expanded is for the session, not for one listing"
    );
}

#[test]
fn test_a_filter_reaches_past_the_cap() {
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let mut home = HomeState::default();
    home.rebuild(&[], &recents);
    home.view_height = 30;

    // The last recent is in the last place, well past what the cap shows.
    home.filter = "part29".to_string();
    let rows = home.visible();
    assert!(
        rows.iter()
            .any(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "part29.parquet")),
        "a match anywhere in RECENT is shown: {rows:?}"
    );
    assert!(
        !rows.iter().any(|r| matches!(r, Row::More { .. })),
        "nothing is hidden under a filter, so there is no more row"
    );
    assert!(
        matches!(
            rows.iter().find(|r| matches!(r, Row::Place { .. })),
            Some(Row::Place { path, .. }) if *path == tmp.path().join("place09")
        ),
        "the match is shown under its place: {rows:?}"
    );
}

#[test]
fn test_a_shorter_terminal_keeps_the_cursor_on_its_row_or_in_range() {
    // The cap is a share of the list's height, so a resize takes rows out from under
    // the cursor. An index kept across that pointed past the end — nothing lit, Enter
    // dead — or at whatever row slid into its place.
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let configured = tmp.path().join("configured");
    touch(&configured, "c.parquet");
    let mut home = HomeState::default();
    home.rebuild(std::slice::from_ref(&configured), &recents);
    home.set_view_height(60);
    let shown_places = |home: &HomeState| {
        home.visible()
            .iter()
            .filter(|r| matches!(r, Row::Place { .. }))
            .count()
    };
    assert_eq!(
        shown_places(&home),
        5,
        "twenty of sixty rows: five places of four"
    );

    // On the configured section's header, below RECENT. Shrinking the terminal takes
    // places out of RECENT and the header moves up; the cursor must move with it.
    let title = datui::home::display_path(&configured);
    let header = home
        .visible()
        .iter()
        .position(
            |r| matches!(r, Row::Header { section, .. } if home.sections[*section].title == title),
        )
        .unwrap();
    home.selected = header;
    let key = home.selected_key();
    home.set_view_height(30);
    assert_eq!(shown_places(&home), 2);
    assert_eq!(home.selected_key(), key, "the cursor followed its row up");
    assert!(home.selected < header);
    home.set_view_height(60);
    assert_eq!(home.selected_key(), key, "and back down");

    // On the last row of RECENT, with RECENT the only open section. Shrinking takes
    // that row away, and the cursor lands on a row that exists rather than past the end.
    for section in 1..home.sections.len() {
        home.set_collapsed(section, true);
    }
    let last_recent = home
        .visible()
        .iter()
        .rposition(|r| matches!(r, Row::Entry { section: 0, .. }))
        .unwrap();
    home.selected = last_recent;
    home.set_view_height(8);
    assert_eq!(shown_places(&home), 1);
    assert!(
        matches!(home.selected_row(), Some(Row::More { .. })),
        "the row is behind the cap now, and the more row stands for it: {:?}",
        home.selected_row()
    );
}

#[test]
fn test_a_rebuild_keeps_the_cursor_on_a_place_row() {
    // A probe answering or a bucket listing landing rebuilds the listing. The cursor
    // used to be put back only on an entry, so on a place row it bounced to the first
    // entry, and the Enter meant for the place opened the dataset under it.
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let mut home = HomeState::default();
    home.rebuild(&[], &recents);
    let place = tmp.path().join("place01");
    let at = home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Place { path, .. } if *path == place))
        .unwrap();
    home.selected = at;

    home.rebuild(&[], &recents);
    assert!(
        matches!(home.selected_row(), Some(Row::Place { path, .. }) if path == place),
        "{:?}",
        home.selected_row()
    );

    // A row behind the cap is put back on the more row that stands for it, and a
    // rebuild leaves it there rather than at the first entry.
    home.set_view_height(30);
    let behind = recents.last().unwrap().clone();
    assert!(home.reselect(Some(datui::home::RowKey::Entry(behind))));
    assert!(matches!(home.selected_row(), Some(Row::More { .. })));
    home.rebuild(&[], &recents);
    assert!(
        matches!(home.selected_row(), Some(Row::More { .. })),
        "{:?}",
        home.selected_row()
    );

    // The same for the more row.
    let more = home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::More { .. }))
        .unwrap();
    home.selected = more;
    home.rebuild(&[], &recents);
    assert!(matches!(home.selected_row(), Some(Row::More { .. })));
}

#[test]
fn test_folding_while_browsing_remembers_nothing() {
    // The listing browsed into never draws folded, so a fold written for it would be
    // invisible here and land on the root listing's section of the same path later.
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    touch(&dir, "a.parquet");
    let mut home = HomeState {
        browsing: Some(dir),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    home.set_collapsed(0, true);
    home.toggle_collapsed(0);
    assert!(home.folds.is_empty(), "{:?}", home.folds);
    assert!(!home.is_collapsed(0));
}

#[test]
fn test_the_place_of_an_http_recent_is_not_a_door() {
    use datui::home::place_is_browsable;
    use std::path::Path;
    assert!(!place_is_browsable(Path::new("https://example.com/data")));
    assert!(!place_is_browsable(Path::new("http://example.com")));
    assert!(place_is_browsable(Path::new("s3://bucket/prefix")));
    assert!(place_is_browsable(Path::new("gs://bucket")));
    assert!(place_is_browsable(Path::new("cloud://s3-default")));
    assert!(place_is_browsable(Path::new("/mnt/data")));
}

#[test]
fn test_a_remembered_fold_does_not_fold_the_listing_browsed_into() {
    // Folds are remembered by title, and a directory's title is its path. The
    // directories recents used to be promoted to were sections by that path, folded
    // by default and remembered when toggled — so a user who folded one would now
    // browse into it from its place row and see the heading and nothing else.
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    touch(&dir, "a.parquet");
    let title = datui::home::display_path(&dir);

    let mut home = HomeState::default();
    home.folds.insert(title, true);
    home.browsing = Some(dir);
    home.rebuild(&[], &[]);

    assert!(
        visible_names(&home).iter().any(|n| n == "a.parquet"),
        "the place browsed into is the whole screen and is never folded: {:?}",
        home.visible()
    );
}

#[test]
fn test_a_place_row_is_never_looked_into_or_measured() {
    // A place is a row of the view. Nothing that reads a directory, opens a file or
    // writes the cache is ever handed one, which is what keeps it free to draw.
    let tmp = TempDir::new().unwrap();
    let recents = ten_places_of_three(&tmp);
    let mut home = HomeState::default();
    home.rebuild(&[], &recents);
    home.view_height = 30;

    let places: Vec<std::path::PathBuf> = home
        .visible()
        .iter()
        .filter_map(|r| match r {
            Row::Place { path, .. } => Some(path.clone()),
            _ => None,
        })
        .collect();
    assert!(!places.is_empty());
    for wanted in [home.unclassified_visible(100), home.unmeasured_visible(100)] {
        assert!(
            wanted.iter().all(|e| !places.contains(&e.path)),
            "a place was queued for a read: {wanted:?}"
        );
    }
    assert!(
        home.selected_entry()
            .is_some_and(|e| !places.contains(&e.path))
    );
    home.selected = 1; // the first place row, under the header
    assert!(matches!(home.selected_row(), Some(Row::Place { .. })));
    assert!(
        home.selected_entry().is_none(),
        "a place is not an entry, so nothing opens or caches it as one"
    );
}

#[test]
fn test_a_huge_directory_is_listed_as_a_bounded_prefix() {
    // Nothing here opens a file; the cost being bounded is the entry count itself.
    let tmp = TempDir::new().unwrap();
    for i in 0..5_010 {
        std::fs::write(tmp.path().join(format!("f{i:05}.parquet")), b"").unwrap();
    }

    let scan = discover::scan_dir_bounded(tmp.path());
    assert!(scan.truncated, "a directory past the cap should say so");
    assert!(
        scan.entries.len() <= 5_000,
        "listed {} entries; the cap is 5000",
        scan.entries.len()
    );
}

/// A directory of `n` identically shaped hive partitions.
fn hive_partitions(dir: &std::path::Path, n: usize) {
    for i in 0..n {
        let hive = dir.join(format!("d{i:03}"));
        fs::create_dir_all(hive.join("year=2024")).unwrap();
        fs::write(hive.join("year=2024/part.parquet"), b"").unwrap();
    }
}

#[test]
fn test_a_label_does_not_depend_on_where_the_row_sits() {
    // Classifying the first sixty-four subdirectories and calling every identical one
    // after them `dir` made a row's label a fact about its position in the listing,
    // not about what was in it (#270). Either all of them are looked into or none is.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);

    let entries = discover::scan_dir(tmp.path());
    assert_eq!(entries.len(), 200, "every subdirectory is still listed");

    let first = entries[0].kind;
    if let Some(odd) = entries.iter().find(|e| e.kind != first) {
        panic!(
            "identical directories must carry identical labels; {} reads as {:?} \
             where {} reads as {first:?}",
            odd.name, odd.kind, entries[0].name,
        );
    }
    assert_eq!(
        first,
        EntryKind::Unknown,
        "a listing too big to look into says so, rather than calling every row a \
         plain directory"
    );
}

#[test]
fn test_a_small_listing_is_no_more_looked_into_than_a_large_one() {
    // Classifying only the listings small enough to afford it would move the
    // arbitrariness rather than remove it: two directories holding the same
    // subdirectories would still disagree about what to call them, decided by how many
    // neighbours each directory happened to have. No listing looks into anything.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 4);
    let small = discover::scan_dir(tmp.path());

    let big_dir = TempDir::new().unwrap();
    hive_partitions(big_dir.path(), 200);
    let big = discover::scan_dir(big_dir.path());

    assert_eq!(small.len(), 4);
    assert_eq!(big.len(), 200);
    let kinds = |entries: &[datui::discover::Entry]| {
        let mut kinds: Vec<EntryKind> = entries.iter().map(|e| e.kind).collect();
        kinds.dedup();
        kinds
    };
    assert_eq!(
        kinds(&small),
        vec![EntryKind::Unknown],
        "a four-directory listing looks into nothing"
    );
    assert_eq!(
        kinds(&big),
        vec![EntryKind::Unknown],
        "and neither does a two-hundred-directory one"
    );
}

/// Put the viewport where a frame of `height` rows would put it to show row
/// `selected`, exactly as `render_list` does. The two move together — a test that
/// sets one and not the other describes a screen that cannot exist.
fn looking_at(home: &mut HomeState, selected: usize, height: usize) {
    home.selected = selected;
    home.view_height = height;
    home.scroll = selected.saturating_sub(height.saturating_sub(3).max(1));
}

/// The entry rows on screen, in order, and what each is called.
fn visible_kinds(home: &HomeState) -> Vec<(String, EntryKind)> {
    home.visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some((entry.name.clone(), entry.kind)),
            _ => None,
        })
        .collect()
}

#[test]
fn test_scrolling_classifies_the_rows_that_are_there() {
    // Row four thousand of a listing nobody could afford to classify up front is
    // still a row somebody is reading. What the viewport shows is what gets looked
    // into — not the head of the list, which is where a budget spent in directory
    // order always went.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));

    looking_at(&mut home, 167, 20);
    assert_eq!(home.scroll, 150, "the frame starts here");
    home.classify_now(20);

    let kinds = visible_kinds(&home);
    let on_screen = &kinds[149..169]; // One header row sits above the entries.
    assert!(
        on_screen.iter().all(|(_, k)| *k == EntryKind::Hive),
        "the rows on screen should have been looked into: {on_screen:?}"
    );
    assert_eq!(
        kinds[0].1,
        EntryKind::Unknown,
        "and the top of the list, which nobody is looking at, should not have been"
    );
}

#[test]
fn test_a_kind_that_arrives_late_does_not_move_the_row() {
    // `sort_entries` puts datasets before directories, so a row found to be a hive
    // dataset would jump groups if the listing re-sorted itself as kinds landed —
    // under the cursor, while the user was scrolling. It must not.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    for name in ["a.parquet", "z.parquet"] {
        touch(tmp.path(), name);
    }
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));

    let before = visible_kinds(&home);
    looking_at(&mut home, 117, 20);
    home.classify_now(20);
    let after = visible_kinds(&home);

    let moved: Vec<&String> = before
        .iter()
        .zip(&after)
        .filter(|((was, _), (now, _))| was != now)
        .map(|((was, _), _)| was)
        .collect();
    assert!(
        moved.is_empty(),
        "nothing re-sorts when a kind lands; these rows moved: {moved:?}"
    );
    let landed = before
        .iter()
        .zip(&after)
        .filter(|((_, was), (_, now))| *was == EntryKind::Unknown && *now == EntryKind::Hive)
        .count();
    assert!(
        landed > 0,
        "and a kind did arrive after the rows were drawn, or this proves nothing"
    );
}

#[test]
fn test_what_is_looked_into_is_the_viewport_and_a_screen_either_side() {
    // A buffer above and below, so arrowing off the edge of the screen does not wait
    // for a round trip. Nothing beyond it: a listing of six thousand rows would
    // otherwise spend ten seconds on rows nobody asked about.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));

    looking_at(&mut home, 107, 10);
    let wanted: Vec<String> = home
        .unclassified_visible(200)
        .into_iter()
        .map(|e| e.name)
        .collect();

    assert!(
        wanted.len() < 200,
        "the whole listing was asked about, not the part on screen"
    );
    let all = visible_names(&home);
    for name in &wanted {
        let at = all.iter().position(|n| n == name).unwrap() + 1; // The header row.
        assert!(
            at.abs_diff(home.scroll) <= 2 * home.view_height,
            "{name} sits at row {at}, which is nowhere near the viewport at {}",
            home.scroll
        );
    }
    // The highlighted row comes first, so a batch smaller than the window spends
    // itself on the row about to be acted on rather than on the buffer around it.
    assert_eq!(
        home.unclassified_visible(1)[0].name,
        all[home.selected - 1],
        "the highlighted row is the first one asked about"
    );
}

#[test]
fn test_directories_nobody_has_looked_into_are_not_counted_as_datasets() {
    // The control bar's figure is "how many datasets are listed". A fresh listing has
    // looked into nothing, so every directory in it is `Unknown` — and `is_dataset` says
    // yes to those, because they are offered as openable and looked into first. That
    // is the right answer to "may this be opened" and the wrong one to count.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));

    let counted = |h: &HomeState| {
        h.visible()
            .iter()
            .filter(|r| matches!(r, Row::Entry { entry, .. } if entry.kind.is_known_dataset()))
            .count()
    };
    assert_eq!(
        counted(&home),
        0,
        "two hundred directories nobody has looked into are not two hundred datasets"
    );
    // But there is plainly somewhere to go, so the "nothing here" guidance stays away.
    assert!(
        home.has_any_dataset(),
        "an unlooked-at directory is still somewhere to go"
    );

    looking_at(&mut home, 17, 10);
    home.classify_now(4);
    assert_eq!(
        counted(&home),
        4,
        "and the figure counts them as they are looked into"
    );
}

#[test]
fn test_a_new_listing_moves_the_viewport_with_the_cursor() {
    // The renderer settles `scroll` from `selected` every frame, but the pass that
    // looks into rows is asked for when a listing lands — before that frame. Browsing
    // into a directory selects the first row while `scroll` still points four hundred
    // rows into the listing that was just replaced, and the whole first batch goes to
    // rows nobody is looking at.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));
    looking_at(&mut home, 187, 10);
    assert_eq!(home.scroll, 180);

    // A different directory, as browsing into one produces.
    let next = TempDir::new().unwrap();
    hive_partitions(next.path(), 200);
    home.apply_listing(datui::home::Listing {
        sections: vec![datui::home::Section {
            door: None,
            title: "NEXT".into(),
            subtitle: None,
            origin: None,
            rows: discover::scan_dir(next.path()),
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
            grouped_by_place: false,
            place_labels: Default::default(),
        }],
    });

    assert_eq!(home.selected, 1, "the cursor lands on the first row");
    assert!(
        home.scroll <= home.selected,
        "and the viewport is where that row is, not where the last listing left it: \
         scroll {} for row {}",
        home.scroll,
        home.selected
    );
    let asked: Vec<String> = home
        .unclassified_visible(4)
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(
        asked.iter().all(|n| n < &"d020".to_string()),
        "so the first pass goes to the rows on screen: {asked:?}"
    );
}

#[test]
fn test_paging_past_rows_does_not_leave_them_queued() {
    // Each pass is chosen from the viewport as it is when the last one landed, so a
    // page that scrolled past four hundred rows asks about the ones it stopped on
    // rather than about every row it went by.
    let tmp = TempDir::new().unwrap();
    hive_partitions(tmp.path(), 200);
    let mut home = home_with_rows(discover::scan_dir(tmp.path()));
    looking_at(&mut home, 17, 10);
    let first: Vec<String> = home
        .unclassified_visible(4)
        .into_iter()
        .map(|e| e.name)
        .collect();
    looking_at(&mut home, 187, 10);
    let after_paging: Vec<String> = home
        .unclassified_visible(4)
        .into_iter()
        .map(|e| e.name)
        .collect();

    assert!(
        !first.is_empty() && !after_paging.is_empty(),
        "both passes should have found something to look into"
    );
    assert!(
        after_paging.iter().all(|name| !first.contains(name)),
        "the rows passed over are not still queued: {after_paging:?}"
    );
}

// --- recursive search below the working directory -------------------------------

#[test]
fn test_search_results_only_appear_once_there_is_a_filter() {
    // With no filter every row matches, and twenty thousand matches is not a home
    // screen. The section exists to answer a question, so it waits for one.
    let tmp = TempDir::new().unwrap();
    let deep = touch(&tmp.path().join("a/b"), "buried.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[], &[]);
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.results = vec![datui::discover::Entry::for_test(
        &deep,
        "a/b/buried.parquet",
    )];
    home.search.done = true;

    home.sync_search_section();
    assert!(
        !home
            .sections
            .iter()
            .any(|s| s.title == HomeState::SEARCH_SECTION),
        "no filter, no search section"
    );

    home.filter = "buried".into();
    home.sync_search_section();
    let section = home
        .sections
        .iter()
        .find(|s| s.title == HomeState::SEARCH_SECTION)
        .expect("typing should surface the search section");
    assert_eq!(section.rows.len(), 1);
    assert!(
        visible_names(&home)
            .iter()
            .any(|n| n == "a/b/buried.parquet")
    );
}

#[test]
fn test_search_results_survive_a_rebuild() {
    // A listing is rebuilt whenever a probe answers or a measurement lands. Walking
    // the tree again each time is exactly the per-keystroke cost this avoids.
    let tmp = TempDir::new().unwrap();
    let deep = touch(&tmp.path().join("a"), "found.parquet");

    let mut home = HomeState {
        filter: "found".into(),
        ..Default::default()
    };
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.results = vec![datui::discover::Entry::for_test(&deep, "a/found.parquet")];
    home.search.done = true;
    home.sync_search_section();

    home.rebuild(&[], &[]);

    assert!(
        home.sections
            .iter()
            .any(|s| s.title == HomeState::SEARCH_SECTION),
        "a rebuild must not discard results that came from a walk"
    );
}

#[test]
fn test_a_dataset_already_on_screen_is_not_listed_twice() {
    // The search is for what you could not otherwise see.
    let tmp = TempDir::new().unwrap();
    let here = touch(tmp.path(), "visible.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        filter: "visible".into(),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.results = vec![datui::discover::Entry::for_test(&here, "visible.parquet")];
    home.search.done = true;
    home.sync_search_section();

    let hits = visible_names(&home)
        .iter()
        .filter(|n| n.ends_with("visible.parquet"))
        .count();
    assert_eq!(hits, 1, "the same file must not appear under two headings");
}

#[test]
fn test_a_late_batch_from_an_abandoned_walk_is_dropped() {
    // Walks are abandoned rather than cancelled, so results for a place the user has
    // already left are normal and must not be shown as if they were here.
    let tmp = TempDir::new().unwrap();
    let stale = touch(&tmp.path().join("old"), "stale.parquet");

    let mut home = HomeState {
        filter: "stale".into(),
        ..Default::default()
    };
    home.search.root = Some(tmp.path().join("somewhere_else"));

    home.search_batch(
        &tmp.path().join("old"),
        vec![datui::discover::Entry::for_test(&stale, "stale.parquet")],
        1,
    );
    assert!(
        home.search.results.is_empty(),
        "a batch from a different root describes a place the user has left"
    );
}

#[test]
fn test_a_partial_search_says_so_rather_than_looking_finished() {
    // "Not found here" is something people act on, so a truncated search must never
    // look like a complete one.
    let tmp = TempDir::new().unwrap();
    let found = touch(tmp.path(), "one.parquet");

    let mut home = HomeState {
        filter: "one".into(),
        ..Default::default()
    };
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.results = vec![datui::discover::Entry::for_test(&found, "one.parquet")];
    home.search_finished(tmp.path(), 4321, Some("partial · out of time".into()));

    let section = home
        .sections
        .iter()
        .find(|s| s.title == HomeState::SEARCH_SECTION)
        .expect("section");
    let subtitle = section.subtitle.clone().unwrap_or_default();
    assert!(
        subtitle.contains("out of time"),
        "the reason it stopped must be on screen, got {subtitle:?}"
    );
    assert!(
        subtitle.contains("4321"),
        "how much was searched is the other half of the answer, got {subtitle:?}"
    );
}

#[test]
fn test_clearing_the_filter_takes_the_search_section_away() {
    let tmp = TempDir::new().unwrap();
    let deep = touch(&tmp.path().join("a"), "x.parquet");

    let mut home = HomeState {
        filter: "x".into(),
        ..Default::default()
    };
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.results = vec![datui::discover::Entry::for_test(&deep, "a/x.parquet")];
    home.search.done = true;
    home.sync_search_section();
    assert!(
        home.sections
            .iter()
            .any(|s| s.title == HomeState::SEARCH_SECTION)
    );

    home.filter.clear();
    home.sync_search_section();
    assert!(
        !home
            .sections
            .iter()
            .any(|s| s.title == HomeState::SEARCH_SECTION),
        "with nothing typed there is no question to answer"
    );
}

// --- what the filter matched, for highlighting ----------------------------------

#[test]
fn test_matched_positions_are_the_ones_the_score_walked() {
    // Highlighting has to agree with matching, or the marks land on letters that had
    // nothing to do with why the row is on screen.
    use datui::home::fuzzy_positions;

    // "sal" against "sales.parquet": the first s, a, l.
    assert_eq!(fuzzy_positions("sal", "sales.parquet"), vec![0, 1, 2]);

    // Greedy and left-to-right: each needle character takes the earliest match after
    // the one before it.
    assert_eq!(fuzzy_positions("sp", "sales.parquet"), vec![0, 6]);
    // q(0) u a r(3) t(4) -- each character takes the earliest slot still available.
    assert_eq!(fuzzy_positions("qrt", "quarterly.csv"), vec![0, 3, 4]);
}

#[test]
fn test_matching_ignores_case_but_reports_real_positions() {
    use datui::home::fuzzy_positions;
    assert_eq!(fuzzy_positions("SAL", "sales.parquet"), vec![0, 1, 2]);
    assert_eq!(fuzzy_positions("sal", "SALES.PARQUET"), vec![0, 1, 2]);
}

#[test]
fn test_an_empty_filter_highlights_nothing() {
    use datui::home::fuzzy_positions;
    assert!(fuzzy_positions("", "sales.parquet").is_empty());
}

#[test]
fn test_column_matches_highlight_a_substring_not_a_subsequence() {
    // A column name is short and specific; a fuzzy match over it would mark most of
    // its letters and mean nothing.
    use datui::home::substring_positions;

    assert_eq!(substring_positions("cust", "customer_id"), vec![0, 1, 2, 3]);
    assert_eq!(substring_positions("id", "customer_id"), vec![9, 10]);
    assert_eq!(
        substring_positions("cid", "customer_id"),
        Vec::<usize>::new(),
        "a subsequence that is not a substring must not highlight"
    );
}

#[test]
fn test_substring_matching_is_case_insensitive() {
    use datui::home::substring_positions;
    assert_eq!(substring_positions("ID", "customer_id"), vec![9, 10]);
    assert_eq!(substring_positions("cust", "CUSTOMER_ID"), vec![0, 1, 2, 3]);
}

#[test]
fn test_a_needle_longer_than_the_column_matches_nothing() {
    use datui::home::substring_positions;
    assert!(substring_positions("customer_identifier", "customer_id").is_empty());
}

#[test]
fn test_a_name_that_does_not_match_highlights_nothing() {
    // Ranking and highlighting come from one function now, so a name that does not
    // match has no positions rather than a partial set of them.
    use datui::home::fuzzy_positions;
    assert!(fuzzy_positions("saz", "sales.parquet").is_empty());
    assert!(fuzzy_positions("zzz", "sales.parquet").is_empty());
}

#[test]
fn test_the_best_alignment_wins_not_the_first_one() {
    // The property people arrive with. Greedily, "re" lands inside "warehouse";
    // every mainstream finder puts it on "revenue", because it tries every start.
    use datui::home::fuzzy_positions;
    let name = "warehouse/2024/q3/revenue_detail.parquet";
    let positions = fuzzy_positions("revdetail", name);
    let first = positions[0];
    assert!(
        name[..first].ends_with('/'),
        "the match should start at a path boundary, not mid-word; started at {first}"
    );
    let marked: String = name
        .chars()
        .enumerate()
        .filter(|(i, _)| positions.contains(i))
        .map(|(_, c)| c)
        .collect();
    assert_eq!(marked, "revdetail");
}

#[test]
fn test_a_match_at_a_word_boundary_outranks_one_inside_a_word() {
    use datui::home::fuzzy_score;
    let boundary = fuzzy_score("sales", "my_sales_report.csv").unwrap();
    let inside = fuzzy_score("sales", "zzsalesz.csv").unwrap();
    assert!(boundary > inside, "{boundary} should beat {inside}");
}

#[test]
fn test_consecutive_characters_outrank_scattered_ones() {
    use datui::home::fuzzy_score;
    let solid = fuzzy_score("abc", "abc.csv").unwrap();
    let spaced = fuzzy_score("abc", "a_b_c.csv").unwrap();
    let scattered = fuzzy_score("abc", "axbxc.csv").unwrap();
    assert!(solid > spaced, "{solid} should beat {spaced}");
    assert!(spaced > scattered, "{spaced} should beat {scattered}");
}

#[test]
fn test_a_match_in_the_file_name_outranks_one_in_a_directory() {
    // Search results are named by their path below the search root, so this is the
    // difference between finding the dataset and finding the directory it is under.
    use datui::home::fuzzy_score;
    let in_name = fuzzy_score("report", "archive/old/report.csv").unwrap();
    let in_dir = fuzzy_score("report", "report/2024/summary.csv").unwrap();
    assert!(
        in_name > in_dir,
        "a basename match ({in_name}) should beat a directory match ({in_dir})"
    );
}

// --- what opening a dataset will cost -------------------------------------------

#[test]
fn test_a_parquet_footer_yields_what_the_file_will_weigh_open() {
    // The single most useful number the footer carries, and the one nothing else on
    // screen implies: compressed bytes on disk say nothing about bytes in memory.
    let path = std::path::Path::new("tests/sample-data/charting_demo.parquet");
    if !path.exists() {
        return; // sample data is generated; skip rather than fail a fresh checkout
    }
    let mut entry = datui::discover::Entry::for_test(path, "charting_demo.parquet");
    entry.size = std::fs::metadata(path).ok().map(|m| m.len());
    datui::discover::enrich(&mut entry);

    let uncompressed = entry.cost.uncompressed.expect("uncompressed size");
    let on_disk = entry.size.expect("size on disk");
    assert!(
        uncompressed > on_disk,
        "a compressed file weighs more open ({uncompressed}) than closed ({on_disk})"
    );
    assert!(entry.cost.codec.is_some(), "the codec is in the footer");
    assert!(entry.cost.row_groups.unwrap_or(0) >= 1);
}

#[test]
fn test_a_hive_layout_is_read_from_directory_names_alone() {
    // No file is opened. That is what makes this knowable for a dataset far too
    // large to count -- which is exactly the dataset whose shape you want described.
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("events");
    for year in ["2023", "2024", "2025"] {
        for region in ["emea", "amer"] {
            let dir = root
                .join(format!("year={year}"))
                .join(format!("region={region}"));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("part-0.parquet"), b"").unwrap();
        }
    }

    let layout = discover::partition_layout(&root).expect("a hive layout");
    assert_eq!(layout.keys, vec!["year", "region"], "keys, outermost first");
    assert_eq!(layout.count, 3);
    assert_eq!(layout.first_key_values, vec!["2023", "2024", "2025"]);
    assert!(!layout.more);
}

#[test]
fn test_a_directory_that_is_not_partitioned_reports_no_layout() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "a.parquet");
    touch(tmp.path(), "b.parquet");
    assert!(discover::partition_layout(tmp.path()).is_none());
}

#[test]
fn test_a_dataset_directory_does_not_report_its_inode_as_its_size() {
    // A stat of a dataset directory returns a couple of hundred bytes that have
    // nothing to do with the terabyte inside it. Showing that reads as an answer.
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("big");
    let dir = root.join("year=2024");
    fs::create_dir_all(&dir).unwrap();
    // Not valid Parquet, so enrichment cannot total them and takes the early path.
    fs::write(dir.join("part-0.parquet"), b"not parquet").unwrap();

    let mut entry = datui::discover::Entry::for_test(&root, "big");
    entry.kind = EntryKind::Hive;
    entry.size = Some(198); // what stat'ing the directory would have given
    datui::discover::enrich(&mut entry);

    assert_eq!(
        entry.size, None,
        "an unmeasurable dataset should say nothing rather than say 198 bytes"
    );
    assert!(
        entry.cost.partitions.is_some(),
        "the layout is still knowable when the size is not"
    );
}

#[test]
fn test_the_partition_scan_is_bounded() {
    // A dataset partitioned by day over a decade has thousands of directories, and
    // counting all of them to print an exact number is not worth a second on a share.
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("daily");
    for i in 0..600 {
        fs::create_dir_all(root.join(format!("day={i:04}"))).unwrap();
    }
    let layout = discover::partition_layout(&root).expect("layout");
    assert!(layout.count <= 512, "counted {}", layout.count);
    assert!(layout.more, "stopping short must be visible, not silent");
}

#[test]
fn test_every_row_is_told_which_filesystem_it_is_on() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "local.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let rows: Vec<_> = home.sections.iter().flat_map(|s| s.rows.iter()).collect();
    assert!(!rows.is_empty(), "the directory should have been listed");

    // Asked on every platform: the field is always populated, so a missing answer is
    // a bug rather than a silent None.
    assert!(
        rows.iter().all(|r| r.cost.source.is_some()),
        "every row should have been asked what it is on"
    );

    // Naming the filesystem needs a mount table, and /proc/self/mountinfo is Linux's.
    // Elsewhere the honest answer is "unknown" -- which is what the code reports and
    // what the details pane then shows -- so assert a real answer only where one
    // can exist.
    #[cfg(target_os = "linux")]
    assert!(
        rows.iter()
            .any(|r| r.cost.source.as_deref() != Some("unknown")),
        "on Linux a listed row should know which filesystem it is on"
    );
}

#[test]
fn test_measuring_a_row_keeps_what_the_footer_said_beyond_the_row_count() {
    // Everything a Parquet footer gives up beyond rows and columns -- codec,
    // uncompressed size, row groups, partition layout -- used to be read, cached, and
    // then dropped on the way to the screen, because the measurement record did not
    // carry it. A hive dataset measured the ordinary way showed no partitions.
    use datui::discover::{Cost, Partitions};
    use datui::home::measured_from;

    let mut probe = datui::discover::Entry::for_test(std::path::Path::new("/tmp/events"), "events");
    probe.rows = Some(1_000);
    probe.cols = Some(4);
    probe.cost = Cost {
        source: Some("nfs4".into()),
        uncompressed: Some(2_000_000),
        codec: Some("zstd".into()),
        row_groups: Some(8),
        partitions: Some(Partitions {
            keys: vec!["year".into()],
            first_key_values: vec!["2024".into()],
            count: 1,
            more: false,
        }),
    };
    let original = datui::discover::Entry::for_test(std::path::Path::new("/tmp/events"), "events");

    let measured = measured_from(&probe, &original);
    assert_eq!(measured.cost.codec.as_deref(), Some("zstd"));
    assert_eq!(measured.cost.row_groups, Some(8));
    assert_eq!(measured.cost.uncompressed, Some(2_000_000));
    assert!(measured.cost.partitions.is_some());
    assert_eq!(
        measured.cost.source, None,
        "the source is resolved from the live mount table, not carried from a probe"
    );
}

/// The whole way to the screen: a listing makes an `Unknown` row, the classify pass
/// looks into it, and the label the row carries is what that pass counted. Every other
/// test for the label calls the counting function itself, which is the path no user
/// takes — the counts reached the cache and stopped there.
#[test]
fn test_a_directory_row_is_labelled_by_what_the_pass_counted() {
    let tmp = TempDir::new().unwrap();
    let directory = tmp.path().join("exports");
    std::fs::create_dir_all(&directory).unwrap();
    for name in ["a.csv", "b.csv", "c.csv"] {
        touch(&directory, name);
    }
    std::fs::write(directory.join("_SUCCESS"), b"").unwrap();

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let unlooked = home
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .find(|r| r.path == directory)
        .expect("the directory is listed")
        .clone();
    assert_eq!(unlooked.kind, datui::discover::EntryKind::Unknown);

    // What the background pass does with it, and what it hands back.
    let probe = datui::home::look_into(&unlooked);
    home.enriched.insert(
        directory.clone(),
        datui::home::measured_from(&probe, &unlooked),
    );
    home.apply_measurements();

    let row = home
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .find(|r| r.path == directory)
        .expect("the row is still listed");
    assert_eq!(row.label(), "3 csv", "the label is what the pass counted");
    assert_eq!(
        row.holds.line(true).as_deref(),
        Some("3 csv · 1 skipped (_SUCCESS)"),
        "and the pane has the whole tally"
    );
}

#[test]
fn test_applying_a_measurement_puts_the_layout_on_the_row() {
    use datui::discover::Cost;

    let tmp = TempDir::new().unwrap();
    let path = touch(tmp.path(), "events.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.enriched.insert(
        path.clone(),
        datui::home::Measured {
            rows: Some(10),
            cols: Some(2),
            cols_sampled: false,
            size: Some(100),
            columns: vec!["a".into()],
            kind: None,
            holds: Default::default(),
            cost: Cost {
                codec: Some("snappy".into()),
                row_groups: Some(3),
                ..Default::default()
            },
        },
    );
    home.apply_measurements();

    let row = home
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .find(|r| r.path == path)
        .expect("the row should still be listed");
    assert_eq!(row.cost.codec.as_deref(), Some("snappy"));
    assert_eq!(row.cost.row_groups, Some(3));
    assert!(
        row.cost.source.is_some(),
        "the live source must survive the measurement being folded in"
    );
}

#[test]
fn test_sections_are_ordered_by_intent_and_elsewhere_starts_folded() {
    // Recent, where you are, the cloud, what you configured, and the desktop's
    // places last and folded, since they are places rather than datasets. A recent's
    // own directory is not a section at all: it is a place row under Recent.
    use datui::home::{CloudSource, ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let configured = tmp.path().join("configured");
    touch(&configured, "a.parquet");
    let elsewhere_dir = tmp.path().join("elsewhere");
    touch(&elsewhere_dir, "b.parquet");
    let recent_dir = tmp.path().join("recent_dir");
    let recent = touch(&recent_dir, "c.parquet");

    let listing = build_listing(&ListingRequest {
        config_dirs: vec![configured.clone()],
        recents: vec![recent],
        desktop_dirs: vec![elsewhere_dir],
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: vec![CloudSource {
            id: "s3-default".to_string(),
            label: "Amazon S3".to_string(),
            api: "s3".to_string(),
            buckets: vec![std::path::PathBuf::from("s3://bucket")],
            ..Default::default()
        }],
        known: Default::default(),
    });

    let titles: Vec<&str> = listing.sections.iter().map(|s| s.title.as_str()).collect();
    let pos = |t: &str| {
        titles
            .iter()
            .position(|x| *x == t)
            .unwrap_or_else(|| panic!("{t} in {titles:?}"))
    };
    let derived = datui::home::display_path(&recent_dir);
    let conf = datui::home::display_path(&configured);
    assert_eq!(titles[0], "Recent");
    assert!(
        pos("Cloud") < pos(&conf),
        "cloud before configured: {titles:?}"
    );
    assert!(
        pos(&conf) < pos("Elsewhere"),
        "configured before Elsewhere: {titles:?}"
    );
    assert!(
        !titles.contains(&derived.as_str()),
        "a recent's directory is not a section: {titles:?}"
    );
    assert!(
        listing.sections.len() <= 5,
        "five sections at most: {titles:?}"
    );

    let by_title = |t: &str| &listing.sections[pos(t)];
    assert!(!by_title("Recent").folded_by_default);
    assert!(by_title("Recent").grouped_by_place);
    assert!(!by_title("Cloud").folded_by_default);
    assert!(!by_title(&conf).folded_by_default);
    assert!(by_title("Elsewhere").folded_by_default);

    // The default is a default: opening one is remembered over it, and the listing
    // still knows it holds datasets when every section is folded.
    let elsewhere_idx = pos("Elsewhere");
    drop(titles);
    let mut home = HomeState::default();
    home.apply_listing(listing);
    assert!(home.is_collapsed(elsewhere_idx));
    home.set_collapsed(elsewhere_idx, false);
    assert!(!home.is_collapsed(elsewhere_idx));
    for i in 0..home.sections.len() {
        home.set_collapsed(i, true);
    }
    assert!(
        home.visible()
            .iter()
            .all(|r| matches!(r, Row::Header { .. }))
    );
    assert!(home.has_any_dataset(), "folded is not empty");
}

#[test]
fn test_parent_location_stops_at_a_bucket() {
    use datui::home::parent_location;
    use std::path::{Path, PathBuf};

    assert_eq!(parent_location(Path::new("gs://bucket")), None);
    assert_eq!(parent_location(Path::new("gs://bucket/")), None);
    assert_eq!(parent_location(Path::new("s3://bucket")), None);
    assert_eq!(
        parent_location(Path::new("gs://bucket/demo/")),
        Some(PathBuf::from("gs://bucket"))
    );
    assert_eq!(
        parent_location(Path::new("s3://bucket/a/b/")),
        Some(PathBuf::from("s3://bucket/a"))
    );
    assert_eq!(parent_location(Path::new("https://example.com")), None);
    assert_eq!(
        parent_location(Path::new("/data/sets")),
        Some(PathBuf::from("/data"))
    );
    assert_eq!(parent_location(Path::new("/")), None);
}

/// Two S3-compatible sources and the default one, as the home screen holds them.
fn cloud_home() -> HomeState {
    use datui::home::{CloudSource, CloudStatus};
    use std::path::PathBuf;
    let mut home = HomeState {
        network_check: |_| false,
        ..Default::default()
    };
    home.cloud = vec![
        CloudSource {
            id: "lab".to_string(),
            label: "Lab MinIO".to_string(),
            api: "s3".to_string(),
            note: "127.0.0.1:9000 · datui config".to_string(),
            buckets: vec![
                PathBuf::from("s3://lab@data"),
                PathBuf::from("s3://lab@sales-archive"),
            ],
            status: CloudStatus::Listed,
            ..Default::default()
        },
        CloudSource {
            id: "onprem".to_string(),
            label: "onprem".to_string(),
            api: "s3".to_string(),
            buckets: vec![PathBuf::from("s3://onprem@data")],
            status: CloudStatus::Failed {
                short: "403".to_string(),
                detail: "Access Denied".to_string(),
            },
            ..Default::default()
        },
        CloudSource {
            id: "s3-default".to_string(),
            label: "Amazon S3".to_string(),
            api: "s3".to_string(),
            status: CloudStatus::Listing,
            ..Default::default()
        },
    ];
    home
}

#[test]
fn test_cloud_sources_are_one_section_of_rows() {
    let mut home = cloud_home();
    home.rebuild(&[], &[]);
    let cloud: Vec<&datui::home::Section> = home
        .sections
        .iter()
        .filter(|s| s.title == HomeState::CLOUD_SECTION)
        .collect();
    assert_eq!(cloud.len(), 1, "one section, however many sources");
    let names: Vec<&str> = cloud[0].rows.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, ["Lab MinIO", "onprem", "Amazon S3"]);
    assert_eq!(
        cloud[0].rows[0].path,
        std::path::PathBuf::from("cloud://lab")
    );

    assert_eq!(home.cloud[0].count_text(), "2 buckets");
    // Buckets from before stay counted when a refresh fails.
    assert_eq!(home.cloud[1].count_text(), "1 bucket");
    assert!(home.cloud[1].failed());
    // Nothing to count yet, and a spinner rather than a zero.
    assert_eq!(home.cloud[2].count_text(), "");
    assert!(home.cloud[2].busy());
}

#[test]
fn test_entering_a_source_lists_its_buckets_and_backspace_returns() {
    use std::path::{Path, PathBuf};
    let mut home = cloud_home();
    home.browsing = Some(PathBuf::from("cloud://lab"));
    home.browse_start = home.browsing.clone();
    home.rebuild(&[], &[]);

    assert_eq!(home.sections.len(), 1);
    assert_eq!(home.sections[0].title, "Lab MinIO");
    let names: Vec<&str> = home.sections[0]
        .rows
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["data", "sales-archive"],
        "bucket names, not source IDs"
    );
    assert!(home.pending_probes().is_empty(), "a source is not probed");
    assert!(home.awaiting_listing().is_none());

    // A bucket's parent is its source, and a prefix's parent is its bucket.
    assert_eq!(
        home.parent_of(Path::new("s3://lab@data")),
        Some(PathBuf::from("cloud://lab"))
    );
    assert_eq!(
        home.parent_of(Path::new("s3://lab@data/2024/")),
        Some(PathBuf::from("s3://lab@data"))
    );
    assert_eq!(home.parent_of(Path::new("cloud://lab")), None);

    // Esc from inside a bucket climbs to the source the browse started from.
    home.browsing = Some(PathBuf::from("s3://lab@data/2024/"));
    assert!(home.below_browse_start());
    home.browsing = Some(PathBuf::from("s3://onprem@data"));
    assert!(
        !home.below_browse_start(),
        "another source is not below this one"
    );

    let sep = datui::glyphs::get().trail;
    assert_eq!(
        home.location_label(Path::new("s3://lab@data/2024/")),
        format!("cloud {sep} Lab MinIO {sep} data {sep} 2024")
    );
    assert_eq!(
        home.location_label(Path::new("cloud://lab")),
        format!("cloud {sep} Lab MinIO")
    );
}

#[test]
fn test_a_source_still_listing_waits_and_an_unknown_one_says_so() {
    use std::path::PathBuf;
    let mut home = cloud_home();
    home.browsing = Some(PathBuf::from("cloud://s3-default"));
    home.rebuild(&[], &[]);
    assert!(home.sections[0].waiting);
    assert!(home.awaiting_listing().is_some());

    home.browsing = Some(PathBuf::from("cloud://gone"));
    home.rebuild(&[], &[]);
    assert!(home.sections[0].unavailable);
    assert_eq!(
        home.sections[0].unavailable_note.as_deref(),
        Some("source not found")
    );
}

#[test]
fn test_typing_finds_bucket_names_from_every_source() {
    let mut home = cloud_home();
    home.rebuild(&[], &[]);
    home.filter = "data".to_string();
    home.sync_search_section();
    let found = home
        .sections
        .iter()
        .find(|s| s.title == HomeState::SEARCH_SECTION)
        .expect("a Found section");
    let sep = datui::glyphs::get().trail;
    let names: Vec<&str> = found.rows.iter().map(|r| r.name.as_str()).collect();
    assert!(
        names.contains(&format!("Lab MinIO {sep} data").as_str())
            && names.contains(&format!("onprem {sep} data").as_str()),
        "the same bucket name from two sources stays two rows: {names:?}"
    );
    assert_eq!(found.subtitle.as_deref(), Some("cloud · 2 names"));
}

#[test]
fn test_partitioned_cloud_directories_are_labelled_and_open_whole() {
    use datui::discover::{Entry, EntryKind};
    use std::path::{Path, PathBuf};
    let btc = PathBuf::from("s3://aws-public-blockchain/v1.0/btc");
    let blocks = PathBuf::from("s3://aws-public-blockchain/v1.0/btc/blocks");
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    let directory = |path: &Path, name: &str| {
        let mut entry = Entry::directory(path);
        entry.name = name.to_string();
        entry
    };
    home.probe_ready(
        btc.clone(),
        vec![
            directory(&blocks, "blocks"),
            directory(
                Path::new("s3://aws-public-blockchain/v1.0/btc/transactions"),
                "transactions",
            ),
        ],
    );
    // Every directory on screen is to be peeked at, once. The picker reads the listing,
    // so the rows have to be on it.
    home.browsing = Some(btc.clone());
    home.rebuild(&[], &[]);
    assert_eq!(home.cloud_directories_to_peek(48).len(), 2);
    home.cloud_kinds
        .insert(blocks.clone(), (EntryKind::Hive, Default::default()));
    home.apply_cloud_kinds(&btc);
    assert_eq!(home.probed[&btc][0].kind, EntryKind::Hive);
    assert_eq!(home.probed[&btc][1].kind, EntryKind::Directory);
    home.rebuild(&[], &[]);
    assert_eq!(home.cloud_directories_to_peek(48).len(), 1);
    // A later listing of the same place keeps what was found.
    home.probe_ready(btc.clone(), vec![directory(&blocks, "blocks")]);
    assert_eq!(home.probed[&btc][0].kind, EntryKind::Hive);
    // Nothing local is ever queued for a peek: `read_dir` on an `s3://` path is a
    // different question from a listing request, and a local directory is the other pass.
    let mut local = HomeState {
        browsing: Some(Path::new("/local/dir").to_path_buf()),
        ..Default::default()
    };
    local.rebuild(&[], &[]);
    assert!(local.cloud_directories_to_peek(48).is_empty());

    // Inside it, one row stands for every partition.
    home.probe_ready(
        blocks.clone(),
        vec![
            directory(
                Path::new("s3://aws-public-blockchain/v1.0/btc/blocks/date=2009-01-03"),
                "date=2009-01-03",
            ),
            directory(
                Path::new("s3://aws-public-blockchain/v1.0/btc/blocks/date=2009-01-09"),
                "date=2009-01-09",
            ),
        ],
    );
    home.browsing = Some(blocks.clone());
    home.rebuild(&[], &[]);
    let first = door_of(&home).expect("the directory carries the door");
    assert_eq!(first.name, "blocks (all partitions)");
    assert_eq!(first.kind, EntryKind::Hive);
    assert_eq!(
        first.path,
        PathBuf::from("s3://aws-public-blockchain/v1.0/btc/blocks/")
    );
    assert_eq!(home.sections[0].rows.len(), 2, "the door is not among them");

    // A directory of plain subdirectories gets one too. Its files are a level down, which
    // is what `Enter` on the row reads — and which directory holds them is the question
    // the row exists so you do not have to answer first.
    let parquet = PathBuf::from("s3://noaa-ghcn-pds/parquet");
    home.probe_ready(
        parquet.clone(),
        vec![
            directory(Path::new("s3://noaa-ghcn-pds/parquet/by_year"), "by_year"),
            directory(
                Path::new("s3://noaa-ghcn-pds/parquet/by_station"),
                "by_station",
            ),
        ],
    );
    home.browsing = Some(parquet);
    home.rebuild(&[], &[]);
    assert_eq!(home.sections[0].rows.len(), 2, "the door is not among them");
    assert_eq!(
        door_of(&home).map(|d| d.name.as_str()),
        Some("parquet (all files)")
    );
    assert_eq!(
        datui::home::directory_dataset_url(Path::new("gs://b/x")),
        PathBuf::from("gs://b/x/")
    );
}

#[test]
fn test_google_steps_through_project_bucket_and_prefix() {
    use datui::home::{CloudSource, CloudStatus};
    use std::path::{Path, PathBuf};
    let project = PathBuf::from("cloud://gcs-default/analytics");
    let mut home = HomeState {
        cloud: vec![CloudSource {
            id: "gcs-default".to_string(),
            label: "Google Cloud".to_string(),
            api: "gcs".to_string(),
            buckets: vec![
                project.clone(),
                PathBuf::from("cloud://gcs-default/billing"),
            ],
            status: CloudStatus::Listed,
            ..Default::default()
        }],
        network_check: |_| false,
        ..Default::default()
    };
    assert_eq!(home.cloud[0].count_text(), "2 projects");
    assert_eq!(home.place_kind(&project), Some("project"));

    // The project's listing is what ties a bucket to it.
    home.probe_ready(
        project.clone(),
        vec![datui::discover::Entry::directory(Path::new("gs://events"))],
    );
    let prefix = Path::new("gs://events/2024/");
    assert_eq!(
        home.parent_of(Path::new("gs://events")),
        Some(project.clone())
    );
    assert_eq!(
        home.parent_of(&project),
        Some(PathBuf::from("cloud://gcs-default"))
    );
    assert_eq!(
        home.cloud_source_of(prefix).map(|s| s.id.as_str()),
        Some("gcs-default")
    );
    let sep = datui::glyphs::get().trail;
    assert_eq!(
        home.location_label(prefix),
        format!("cloud {sep} Google Cloud {sep} analytics {sep} events {sep} 2024")
    );
    home.browsing = Some(prefix.to_path_buf());
    home.browse_start = Some(PathBuf::from("cloud://gcs-default"));
    assert!(home.below_browse_start());
}

#[test]
fn test_public_datasets_are_named_rows_and_backspace_returns_to_them() {
    use datui::home::{CloudSource, CloudStatus};
    use std::path::{Path, PathBuf};
    let noaa = PathBuf::from("s3://noaa-ghcn-pds/parquet/");
    let overture = PathBuf::from("abfss://release@overturemapswestus2.dfs.core.windows.net/");
    let mut home = HomeState {
        cloud: vec![CloudSource {
            id: "public".to_string(),
            label: "Public datasets".to_string(),
            api: "public".to_string(),
            buckets: vec![noaa.clone(), overture.clone()],
            names: [
                (noaa.clone(), "NOAA daily weather".to_string()),
                (overture.clone(), "Overture Maps".to_string()),
            ]
            .into_iter()
            .collect(),
            status: CloudStatus::Listed,
            ..Default::default()
        }],
        network_check: |_| false,
        ..Default::default()
    };
    assert_eq!(home.cloud[0].count_text(), "2 datasets");

    home.browsing = Some(PathBuf::from("cloud://public"));
    home.rebuild(&[], &[]);
    let names: Vec<&str> = home.sections[0]
        .rows
        .iter()
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(names, ["NOAA daily weather", "Overture Maps"]);

    let year = Path::new("s3://noaa-ghcn-pds/parquet/by_year");
    assert_eq!(
        home.parent_of(year),
        Some(noaa.clone()),
        "the dataset as listed"
    );
    assert_eq!(
        home.parent_of(Path::new("s3://noaa-ghcn-pds/parquet")),
        Some(PathBuf::from("cloud://public")),
        "a dataset's root goes back to the datasets, not up the bucket"
    );
    assert_eq!(
        home.parent_of(&overture),
        Some(PathBuf::from("cloud://public"))
    );
    assert_eq!(
        home.cloud_source_of(Path::new(
            "abfss://release@overturemapswestus2.dfs.core.windows.net/2026-08-19.0/"
        ))
        .map(|s| s.id.as_str()),
        Some("public")
    );

    let sep = datui::glyphs::get().trail;
    assert_eq!(
        home.location_label(Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2020")),
        format!(
            "cloud {sep} Public datasets {sep} NOAA daily weather {sep} by_year {sep} YEAR=2020"
        )
    );

    home.browsing = Some(year.to_path_buf());
    home.browse_start = Some(PathBuf::from("cloud://public"));
    assert!(home.below_browse_start());
}

#[test]
fn test_azure_steps_through_account_container_and_directory() {
    use datui::home::{CloudSource, CloudStatus, object_place_label};
    use std::path::{Path, PathBuf};
    // The real test for "remote", which is what decides that an account is probed.
    let mut home = HomeState {
        cloud: vec![CloudSource {
            id: "az".to_string(),
            label: "Azure".to_string(),
            api: "azure".to_string(),
            buckets: vec![
                PathBuf::from("cloud://az/datalake001"),
                PathBuf::from("cloud://az/archive002"),
            ],
            status: CloudStatus::Listed,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(home.cloud[0].count_text(), "2 accounts");

    let account = Path::new("cloud://az/datalake001");
    let container = Path::new("abfss://datui-test@datalake001.dfs.core.windows.net/");
    let directory = Path::new("abfss://datui-test@datalake001.dfs.core.windows.net/demo/fred/");

    assert_eq!(object_place_label(account), Some("account"));
    assert_eq!(object_place_label(container), Some("container"));
    // A directory is labelled by what it holds, like a local one.
    assert_eq!(object_place_label(directory), None);

    assert_eq!(
        home.parent_of(directory),
        Some(PathBuf::from(
            "abfss://datui-test@datalake001.dfs.core.windows.net/demo/"
        ))
    );
    assert_eq!(
        home.parent_of(Path::new(
            "abfss://datui-test@datalake001.dfs.core.windows.net/demo/"
        )),
        Some(container.to_path_buf())
    );
    assert_eq!(home.parent_of(container), Some(account.to_path_buf()));
    assert_eq!(home.parent_of(account), Some(PathBuf::from("cloud://az")));

    home.browsing = Some(directory.to_path_buf());
    home.browse_start = Some(PathBuf::from("cloud://az"));
    assert!(home.below_browse_start(), "a directory is below its source");

    let sep = datui::glyphs::get().trail;
    assert_eq!(
        home.location_label(directory),
        format!("cloud {sep} Azure {sep} datalake001 {sep} datui-test {sep} demo {sep} fred")
    );
    assert_eq!(
        home.location_label(account),
        format!("cloud {sep} Azure {sep} datalake001")
    );

    // An account is a place to step into, listed by a probe like a remote directory.
    home.browsing = Some(account.to_path_buf());
    home.rebuild(&[], &[]);
    assert_eq!(home.pending_probes(), vec![account.to_path_buf()]);
    assert_eq!(home.sections[0].title, "datalake001");
}

/// What a previous run found a directory to be is what the next run's listing goes on,
/// since a listing looks into nothing itself. A directory whose footers said its files
/// are separate tables must not be offered as one dataset again until those footers
/// have been read a second time.
///
/// The kind is the only thing carried over here: a directory has no size for the
/// fingerprint that guards the rest, so this is checked against its modification time
/// instead.
#[test]
fn test_a_directory_found_to_be_separate_tables_stays_a_plain_directory() {
    use datui::cache::DatasetFacts;
    use datui::home::{ListingRequest, build_listing};

    let tmp = TempDir::new().unwrap();
    let directory = tmp.path().join("exports");
    fs::create_dir(&directory).unwrap();
    // Two names that share an extension and nothing else, so the listing says `multi`.
    touch(&directory, "circuits.parquet");
    touch(&directory, "drivers.parquet");
    let mtime = fs::metadata(&directory)
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let listed = |known: Vec<(std::path::PathBuf, DatasetFacts)>| {
        let request = ListingRequest {
            config_dirs: Vec::new(),
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: Some(tmp.path().to_path_buf()),
            probed: Default::default(),
            unreachable: Default::default(),
            probe_errors: Default::default(),
            network_check: |_| false,
            cloud: Vec::new(),
            known: known.into_iter().collect(),
        };
        build_listing(&request)
            .sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .find(|entry| entry.path == directory)
            .expect("the directory is listed")
            .kind
    };

    assert_eq!(
        listed(Vec::new()),
        EntryKind::Unknown,
        "with nothing remembered, the listing says only that nothing has looked"
    );

    let facts = |mtime| DatasetFacts {
        mtime,
        size: 0,
        rows: None,
        cols: None,
        cols_sampled: false,
        columns: vec!["circuit_id".into(), "driver_id".into()],
        kind: Some(EntryKind::Directory),
        classified_by: datui::discover::CLASSIFIER_VERSION,
        holds: Default::default(),
        cost: Default::default(),
    };
    assert_eq!(
        listed(vec![(directory.clone(), facts(mtime))]),
        EntryKind::Directory,
        "what the footers said survives the next listing"
    );
    assert_eq!(
        listed(vec![(directory.clone(), facts(mtime - 1))]),
        EntryKind::Unknown,
        "and a directory whose contents changed is looked into again rather than recalled"
    );
}

/// A directory whose footers said its files are separate tables still offers the row that
/// reads them together.
///
/// This used to be the opposite. The peek's answer gated the row, so a directory datui
/// judged not-one-table could not be read as one at all — the judgement made twice,
/// once in the label and once in the door. Reading unrelated Parquet files together is
/// a thing a user may want and every other tool allows; datui's opinion of it belongs
/// in the label, not in what is reachable.
#[cfg(feature = "cloud")]
#[test]
fn test_a_directory_of_separate_tables_still_offers_to_read_them_together() {
    use datui::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    let exports = PathBuf::from("gs://bucket/exports");
    let object = |name: &str| {
        let mut entry = Entry::directory(&exports.join(name));
        entry.name = name.to_string();
        entry.kind = EntryKind::File;
        entry.size = Some(1_000);
        entry
    };
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    home.probe_ready(
        exports.clone(),
        vec![
            object("circuits.parquet"),
            object("drivers.parquet"),
            object("laps.parquet"),
        ],
    );
    home.browsing = Some(exports.clone());

    // With nothing known about the directory, the names alone still offer the union.
    home.rebuild(&[], &[]);
    assert_eq!(
        door_of(&home).map(|d| d.name.as_str()),
        Some("exports (all files)"),
        "unpeeked, the listing offers it"
    );

    // And once the peek has read footers and found separate tables, it still does: the
    // peek decides what the directory is called, not what can be opened. The peek no
    // longer reaches the listing at all — that plumbing went with the gate — so this
    // half stands against the gate being put back where it was, not against the peek.
    home.cloud_kinds
        .insert(exports.clone(), (EntryKind::Directory, Default::default()));
    home.rebuild(&[], &[]);
    assert_eq!(
        door_of(&home).map(|d| d.name.as_str()),
        Some("exports (all files)"),
        "the second door does not close on a verdict"
    );
    assert_eq!(
        home.sections[0].rows.len(),
        3,
        "the three objects; the row that opens them together is the section's door"
    );
}

/// The `(all files)` row is labelled by what the listing under it holds, like any other
/// directory row. It is built rather than listed, so it is the one row whose tally
/// nothing upstream fills in.
#[cfg(feature = "cloud")]
#[test]
fn test_the_whole_directory_row_says_what_the_listing_holds() {
    use datui::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    let exports = PathBuf::from("gs://bucket/exports");
    let object = |name: &str| {
        let mut entry = Entry::directory(&exports.join(name));
        entry.name = name.to_string();
        entry.kind = EntryKind::File;
        entry.size = Some(1_000);
        entry
    };
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    home.probe_ready(
        exports.clone(),
        (0..12)
            .map(|i| object(&format!("part-{i:05}.parquet")))
            .collect(),
    );
    home.browsing = Some(exports.clone());
    home.rebuild(&[], &[]);

    let row = door_of(&home).expect("the directory carries the door");
    assert_eq!(row.name, "exports (all files)");
    assert_eq!(row.holds.data_files(), 12, "the tally the pane reports");
    // And no label. Every other label counts what is directly inside a directory; this
    // row reads the whole of it, so a count beside it would be about a different set of
    // files than the row is.
    assert_eq!(row.label(), "");
    assert!(row.opens_whole_directory);
}

/// Rows that will never be measured are not asked where they live.
///
/// `unmeasured_visible` runs once per row on every frame that draws the home screen,
/// and locating a row on the mount table is the expensive half of each pass. A
/// directory of six thousand date partitions is six thousand rows that are all
/// directories — none of them measurable — so asking the expensive question about
/// every one of them, every frame, was the whole of why browsing one crawled.
#[test]
fn test_rows_that_cannot_be_measured_are_not_located_on_the_mount_table() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ASKED: AtomicUsize = AtomicUsize::new(0);

    fn counting(_: &std::path::Path) -> bool {
        ASKED.fetch_add(1, Ordering::Relaxed);
        false
    }

    let tmp = TempDir::new().unwrap();
    // Partitions, the shape the home screen browses into: each is a directory, so
    // each is something to step into rather than something with a row count.
    for day in 1..=40 {
        fs::create_dir_all(tmp.path().join(format!("2009-01-{day:02}"))).unwrap();
    }

    let mut home = HomeState::default();
    home.rebuild(&[tmp.path().to_path_buf()], &[]);

    // Only the frame's own question is counted, not the rebuild's.
    home.network_check = counting;
    ASKED.store(0, Ordering::Relaxed);

    let wanted = home.unmeasured_visible(1);

    assert!(
        wanted.is_empty(),
        "a directory of partitions has nothing to measure"
    );
    assert_eq!(
        ASKED.load(Ordering::Relaxed),
        0,
        "a row that is a directory is settled by its kind, before where it lives"
    );
}

/// The files a directory reads as are all of them, not a listing's worth.
///
/// Every other listing in `discover` stops at `MAX_ENTRIES_PER_DIR`, because a listing
/// is a menu and five thousand rows is more than anyone reads. These files are not a
/// menu — they are the table — so a cap here would open a directory of six thousand CSVs
/// with a row count, a schema union and every aggregate computed over an arbitrary
/// five thousand of them, and say nothing about it.
#[test]
fn test_a_directory_is_read_as_every_file_in_it() {
    let tmp = TempDir::new().unwrap();
    let directory = tmp.path().join("exports");
    fs::create_dir(&directory).unwrap();
    // One more than the listing cap, so a prefix and the whole thing differ.
    let want = datui::discover::MAX_ENTRIES_PER_DIR + 1;
    for i in 0..want {
        fs::write(directory.join(format!("part-{i:05}.csv")), b"a\n1\n").unwrap();
    }

    match datui::discover::directory_format(&directory) {
        datui::discover::DirectoryFormat::One(format, files) => {
            assert_eq!(format, datui::FileFormat::Csv);
            assert_eq!(
                files.len(),
                want,
                "the directory holds {want} files and every one of them is the table"
            );
        }
        other => panic!("a directory of CSVs reads as CSVs, not {other:?}"),
    }
}

/// What a directory holds is what picks the reader for it.
///
/// The judgement the open path makes before choosing between the Parquet hive scan
/// and reading the files as themselves. Asserted here rather than only through an
/// open, because an open that guesses Parquet and is overruled a step later by the
/// hive schema pass looks, from the outside, exactly like one that guessed right.
#[test]
fn test_a_directory_is_read_as_whatever_is_actually_in_it() {
    use datui::discover::{DirectoryFormat, directory_format};

    let tmp = TempDir::new().unwrap();

    // A hive root: the data is a level down, so the directory settles nothing itself —
    // whatever strays are lying at the top of it.
    touch(tmp.path(), "hive/date=2024-01-01/data.parquet");
    touch(tmp.path(), "hive/stray.csv");
    touch(tmp.path(), "hive/notes.txt");
    assert_eq!(
        directory_format(&tmp.path().join("hive")),
        DirectoryFormat::Deeper
    );

    // A flat directory of compressed JSON: JSON, not Parquet. This is the directory that
    // failed with "file must end with PAR1".
    touch(tmp.path(), "days/by_block.json.gz");
    touch(tmp.path(), "days/daily.json.gz");
    assert!(
        matches!(
            directory_format(&tmp.path().join("days")),
            DirectoryFormat::One(datui::FileFormat::Json, files) if files.len() == 2
        ),
        "a directory of .json.gz is JSON"
    );

    // An unrelated subdirectory is not a partition, so it does not hand the directory
    // back to the scan that walks trees.
    touch(tmp.path(), "exports/a.csv");
    touch(tmp.path(), "exports/b.csv");
    std::fs::create_dir_all(tmp.path().join("exports/archive")).unwrap();
    assert!(
        matches!(
            directory_format(&tmp.path().join("exports")),
            DirectoryFormat::One(datui::FileFormat::Csv, files) if files.len() == 2
        ),
        "a directory of CSVs beside some other directory is still a directory of CSVs"
    );

    // A README is not a candidate; it does not make the directory unreadable.
    touch(tmp.path(), "documented/a.parquet");
    touch(tmp.path(), "documented/b.parquet");
    touch(tmp.path(), "documented/README.txt");
    assert!(matches!(
        directory_format(&tmp.path().join("documented")),
        DirectoryFormat::One(datui::FileFormat::Parquet, _)
    ));

    // Two formats: the commonest is the table, and the rest are counted so the read can
    // say what it passed over rather than refusing the directory over a stray.
    touch(tmp.path(), "both/a.csv");
    touch(tmp.path(), "both/b.csv");
    touch(tmp.path(), "both/c.json");
    match directory_format(&tmp.path().join("both")) {
        DirectoryFormat::Mixed {
            format,
            files,
            passed_over,
        } => {
            assert_eq!(format, datui::FileFormat::Csv);
            assert_eq!(files.len(), 2);
            assert_eq!(passed_over, vec![(datui::FileFormat::Json, 1)]);
        }
        other => panic!("got {other:?}"),
    }

    // Parquet wins a tie, because it is the format a directory of data files is most
    // likely to be about and the one every other route reads in place.
    touch(tmp.path(), "tied/a.csv");
    touch(tmp.path(), "tied/b.parquet");
    match directory_format(&tmp.path().join("tied")) {
        DirectoryFormat::Mixed { format, .. } => assert_eq!(format, datui::FileFormat::Parquet),
        other => panic!("got {other:?}"),
    }
}

/// The row that opens the directory being browsed is a door, not a search result.
///
/// Its name carries the words `all files`, which a fuzzy filter matches for most of the
/// alphabet: `sal` found it beside `sales.parquet`. It steps out of the way while a
/// filter is on and comes back when the filter is cleared, and it stays first whatever
/// the sort, because being the first row inside a directory is the whole of what it is.
#[test]
fn test_the_whole_directory_row_is_a_door_not_a_search_result() {
    use datui::home::SortMode;
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales.parquet");
    touch(tmp.path(), "customers.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let first = |home: &HomeState| visible_names(home).first().cloned();
    assert!(
        first(&home).is_some_and(|n| n.ends_with("(all files)")),
        "got {:?}",
        visible_names(&home)
    );

    home.filter = "sal".to_string();
    assert_eq!(
        visible_names(&home),
        vec!["sales.parquet"],
        "a filter that happens to fuzzy-match `all files` must not surface the door"
    );

    home.filter.clear();
    assert!(first(&home).is_some_and(|n| n.ends_with("(all files)")));

    // And it is not one of the things being ordered.
    for sort in [SortMode::Size, SortMode::Rows, SortMode::Modified] {
        home.sort = sort;
        assert!(
            first(&home).is_some_and(|n| n.ends_with("(all files)")),
            "under {sort:?} the first row was {:?}",
            visible_names(&home)
        );
    }
}

/// The door is not a row of the directory, so no path-keyed map can reach it.
///
/// Its path *is* the directory's — `PathBuf::from("/a/b/")` compares and hashes equal to
/// `PathBuf::from("/a/b")` — so as an `Entry` among the section's rows it was the same
/// key as the directory's own row one level up in every map keyed by path. That cost the
/// directory upstairs its label once already; the guard that fixed it had to be written
/// again by every walker added after it. So the collision is gone instead: the door is
/// `Section::door` and `Row::Door`, and the pattern that reaches rows does not match it.
#[test]
fn test_nothing_that_walks_the_rows_can_reach_the_door() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "a.parquet");
    touch(tmp.path(), "b.parquet");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let door = door_of(&home).expect("the directory carries the door");
    // The collision itself, still there and still the reason for all of this.
    assert_eq!(
        door.path,
        tmp.path().to_path_buf(),
        "the trailing slash is not a different key"
    );

    assert!(
        !home
            .sections
            .iter()
            .any(|s| s.rows.iter().any(|r| r.path == door.path)),
        "a walk of `rows` must not find it"
    );
    assert!(
        !home
            .visible()
            .iter()
            .any(|r| matches!(r, Row::Entry { entry, .. } if entry.path == door.path)),
        "and neither must a walk of `Row::Entry`"
    );
    // It is on screen all the same, and first.
    assert!(matches!(home.visible().first(), Some(Row::Header { .. })));
    assert!(matches!(home.visible().get(1), Some(Row::Door { .. })));
}

/// The two directories that get no door, and the reason each is not one.
///
/// Both are `None` returns in `whole_directory_row` that its doc comment argues for and
/// nothing tested. An empty directory is the one place a second door leads nowhere, and a
/// `cloud://<id>/<account>` place stands for an Azure storage account — its children
/// are containers, and it has no URL to open.
#[test]
fn test_a_directory_with_nothing_in_it_gets_no_door() {
    let tmp = TempDir::new().unwrap();
    let empty = tmp.path().join("empty");
    fs::create_dir_all(&empty).unwrap();

    let mut home = HomeState {
        browsing: Some(empty),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    assert!(
        door_of(&home).is_none(),
        "a row promising to read nothing is worse than no row"
    );

    // And one with something in it does get one, so the guard above is the reason.
    touch(tmp.path(), "a.parquet");
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    assert!(door_of(&home).is_some());
}

#[cfg(feature = "cloud")]
#[test]
fn test_an_azure_account_place_gets_no_door() {
    use std::path::PathBuf;
    let account = PathBuf::from("cloud://az-default/storageaccount");
    let mut home = HomeState {
        network_check: |_| true,
        browsing: Some(account.clone()),
        ..Default::default()
    };
    home.probe_ready(
        account,
        vec![datui::discover::Entry::directory(std::path::Path::new(
            "abfss://raw@storageaccount.dfs.core.windows.net/",
        ))],
    );
    home.rebuild(&[], &[]);
    assert!(
        door_of(&home).is_none(),
        "an account is not a directory: its children are containers and it has no URL"
    );
}

/// The door is named after the directory, at every path a directory can have.
///
/// Built by splitting the path on `/`, the filesystem root — which has no last
/// component — produced a row called `" (all files)"`, and on Windows, where the
/// separator is not the one a split looks for, a local directory would have been named
/// with the whole of its path.
#[test]
fn test_the_door_is_named_after_the_directory_even_at_the_root() {
    let mut home = HomeState {
        browsing: Some(std::path::PathBuf::from("/")),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    let door = door_of(&home).expect("the root is a directory like any other");
    assert_eq!(door.name, "/ (all files)", "got {:?}", door.name);
}

/// Stepping into a directory and back out leaves its label alone.
///
/// The door's path *is* the directory's — `PathBuf` compares and hashes a trailing slash
/// away — so measuring it wrote into the slot the directory's own row uses one level up.
/// That write carries no kind, because the door's kind did not change, and
/// `directories_to_look_into` reads the slot being occupied as the row having been looked
/// into. So the directory upstairs kept `Unknown`: `multi  2 parquet  2 × 1` became
/// `multi/  …`, the pane lost its `holds` line, the caption dropped to `0 datasets`,
/// and nothing cleared `enriched` for the rest of the session — not even Ctrl+R.
#[test]
fn test_stepping_into_a_directory_and_back_does_not_erase_its_label() {
    let tmp = TempDir::new().unwrap();
    let multi = tmp.path().join("multi");
    fs::create_dir_all(&multi).unwrap();
    for name in ["a.parquet", "b.parquet"] {
        let mut frame = polars::prelude::DataFrame::new(
            1,
            vec![
                polars::prelude::Column::new("id".into(), &[1i32]),
                polars::prelude::Column::new("ts".into(), &[2i32]),
            ],
        )
        .unwrap();
        let file = fs::File::create(multi.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let mut home = HomeState {
        browsing: Some(multi.clone()),
        ..Default::default()
    };
    // Inside the directory: the door is on screen and every pass runs over it.
    home.rebuild(&[], &[]);
    assert!(door_of(&home).is_some());
    for _ in 0..4 {
        home.measure_now(16);
        home.classify_now(16);
    }
    assert!(
        !home.enriched.contains_key(&multi),
        "the door must not take the directory's measurement slot"
    );

    // Back out. The directory's own row is classified and labelled as it would have been.
    home.browsing = Some(tmp.path().to_path_buf());
    home.rebuild(&[], &[]);
    for _ in 0..4 {
        home.classify_now(16);
        home.measure_now(16);
    }
    let row = home
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .find(|r| r.name == "multi")
        .expect("the directory is listed");
    assert_eq!(row.kind, EntryKind::MultiFile, "got {:?}", row.kind);
    assert_eq!(row.label(), "2 parquet");
}

/// Nothing remote is read to build the door.
///
/// The rule this whole branch is built on: listing a share that has stopped answering
/// is the call that freezes the interface, so the rows come from whatever the
/// background probe returned. Asking `look_at_directory` for the door's kind put a
/// `read_dir` and a `metadata` per entry back on the share, twelve lines below the
/// comment saying it never does.
#[cfg(feature = "cloud")]
#[test]
fn test_the_door_on_a_share_is_built_from_the_probe_not_the_disk() {
    let tmp = TempDir::new().unwrap();
    let share = tmp.path().join("share");
    fs::create_dir_all(&share).unwrap();
    // On disk: three Parquet files. Through the probe: one CSV. A door built from disk
    // says `3 parquet`; one built from the listing says what the listing said.
    for name in ["a.parquet", "b.parquet", "c.parquet"] {
        touch(&share, name);
    }

    let mut home = HomeState {
        network_check: |_| true,
        browsing: Some(share.clone()),
        ..Default::default()
    };
    let mut stale = datui::discover::Entry::directory(&share.join("stale.csv"));
    stale.name = "stale.csv".to_string();
    stale.kind = EntryKind::File;
    stale.size = Some(10);
    home.probe_ready(share, vec![stale]);
    home.rebuild(&[], &[]);

    let door = door_of(&home).expect("the directory carries the row");
    assert_eq!(
        door.holds.label(),
        "1 csv",
        "built from the probe's rows, not from a read of the share"
    );
}

/// The door is named the way the section title above it names the same place.
///
/// A source id is not part of a name — `s3://lab@bucket` is titled `bucket` — and an
/// Azure container is named by container rather than by the long URL its last component
/// happens to be. Both were reachable: a source with an id lists its buckets as
/// `s3://<id>@bucket`, and an Azure account lists its containers as
/// `abfss://<container>@<account>.dfs.core.windows.net/`.
#[cfg(feature = "cloud")]
#[test]
fn test_the_door_is_named_the_way_the_title_is() {
    use std::path::PathBuf;
    let named = |url: &str| -> String {
        let place = PathBuf::from(url);
        let mut home = HomeState {
            network_check: |_| true,
            browsing: Some(place.clone()),
            ..Default::default()
        };
        let mut object = datui::discover::Entry::directory(&place.join("one.parquet"));
        object.name = "one.parquet".to_string();
        object.kind = EntryKind::File;
        object.size = Some(10);
        home.probe_ready(place, vec![object]);
        home.rebuild(&[], &[]);
        door_of(&home)
            .map(|r| r.name.clone())
            .expect("the place carries the row")
    };

    assert_eq!(named("s3://bucket"), "bucket (all files)");
    assert_eq!(named("s3://lab@bucket"), "bucket (all files)");
    assert_eq!(named("s3://lab@bucket/exports"), "exports (all files)");
    assert_eq!(named("gs://bucket/exports/"), "exports (all files)");
    assert_eq!(
        named("abfss://raw@acct.dfs.core.windows.net"),
        "raw (all files)"
    );
    assert_eq!(
        named("abfss://raw@acct.dfs.core.windows.net/tbl"),
        "tbl (all files)"
    );
}

/// The door is not one of the things the section is counting.
///
/// It is a way to open the directory those rows are *in*, so counting it made a directory
/// of three files say four — in the header chip and in the `N datasets` caption, whose
/// own comment says counting a place-to-look makes the figure a lie. It was inconsistent
/// with itself too: under a filter the door steps out of the way, so the same count meant
/// one thing with a filter typed and another without.
#[test]
fn test_the_door_is_not_counted_among_what_a_directory_holds() {
    let tmp = TempDir::new().unwrap();
    for name in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        touch(tmp.path(), name);
    }

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let header_count = |home: &HomeState| {
        home.visible()
            .iter()
            .find_map(|r| match r {
                Row::Header { matches, .. } => Some(*matches),
                _ => None,
            })
            .expect("a section header")
    };
    assert!(door_of(&home).is_some(), "the door is on screen");
    assert_eq!(header_count(&home), 3, "three files");

    // And the same number once a filter removes the door, which is what made the
    // inconsistency visible.
    home.filter = "part".to_string();
    assert_eq!(header_count(&home), 3);
}

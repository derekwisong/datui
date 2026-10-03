//! Home screen: dataset discovery, roots, and filtering.

use datui::discover::{self, EntryKind};
use datui::home::{HomeState, RootOrigin, Row, fuzzy_score};
use std::fs;
use tempfile::TempDir;

mod common;
#[cfg(feature = "cloud")]
#[path = "common/fake_s3.rs"]
mod fake_s3;

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
        "waves.vcd",
        "compounds.sdf",
        "compounds.sd",
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
    assert!(discover::is_data_file(std::path::Path::new(
        "compounds.sdf.gz"
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
fn test_scan_skips_dotfiles_and_lists_non_data_last() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "good.parquet");
    touch(tmp.path(), ".hidden.parquet");
    touch(tmp.path(), "notes.md");
    touch(tmp.path(), "sub/a.parquet");

    let listed: Vec<(String, EntryKind)> = discover::scan_dir(tmp.path())
        .into_iter()
        .map(|e| (e.name, e.kind))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("good.parquet".to_string(), EntryKind::File),
            ("sub".to_string(), EntryKind::Unknown),
            ("notes.md".to_string(), EntryKind::Other),
        ]
    );
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
fn test_files_datui_cannot_read_are_hidden_until_shown() {
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales.parquet");
    touch(tmp.path(), "README.md");

    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    assert!(!visible_names(&home).contains(&"README.md".to_string()));
    assert!(visible_names(&home).contains(&"sales.parquet".to_string()));

    home.hide_unreadable = false;
    assert!(visible_names(&home).contains(&"README.md".to_string()));
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

    // A page stops at the ends instead of going round.
    home.page_selection(10);
    assert_eq!(home.selected, total - 1, "PgDn stops at the last row");
    home.page_selection(10);
    assert_eq!(home.selected, total - 1, "and stays there");
    home.page_selection(-10);
    assert_eq!(home.selected, 0, "PgUp stops at the first");

    // Home and End are a page as far as it goes, and do not overflow getting there.
    home.page_selection(isize::MAX);
    assert_eq!(home.selected, total - 1);
    home.page_selection(isize::MIN);
    assert_eq!(home.selected, 0);
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
    use datui::config::HomeConfig;
    let default = HomeConfig::default();
    assert!(default.desktop_recents, "on by default");

    let off: HomeConfig = toml::from_str("desktop_recents = false").expect("parses");
    assert!(!off.desktop_recents);
}

/// Files datui cannot read start hidden unless the config shows them, and a later
/// layer that leaves the key out does not turn it back off.
#[test]
fn test_unreadable_files_can_be_shown_from_the_start() {
    use datui::config::HomeConfig;
    assert!(!HomeConfig::default().show_unreadable, "hidden by default");

    let config = common::layered_config(&[
        "[home]\nshow_unreadable = true\n",
        "[home]\ndesktop_recents = true\n",
    ]);
    assert!(config.home.show_unreadable);
    assert!(
        !common::layered_config(&[
            "[home]\nshow_unreadable = true\n",
            "[home]\nshow_unreadable = false\n",
        ])
        .home
        .show_unreadable,
        "a later layer that says false hides them again"
    );

    let (tx, _rx) = std::sync::mpsc::channel();
    let app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    assert!(
        !app.home.hide_unreadable,
        "the home screen starts showing them"
    );
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
        missing: Default::default(),
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
            root: None,
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
        collections: Vec::new(),
        config_dirs: vec![tmp.path().to_path_buf()],
        remembered_dirs: Vec::new(),
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: Default::default(),
        formats: Default::default(),
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
        format_spec: None,
        table: None,
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
        collections: Vec::new(),
        config_dirs: Vec::new(),
        remembered_dirs: Vec::new(),
        recents: vec![dataset.clone()],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| true,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
        formats: Default::default(),
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
        collections: Vec::new(),
        config_dirs: Vec::new(),
        remembered_dirs: Vec::new(),
        recents: typed.clone(),
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| true,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
        formats: Default::default(),
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
            collections: Vec::new(),
            config_dirs: Vec::new(),
            remembered_dirs: Vec::new(),
            recents: vec![dataset.clone()],
            desktop_dirs: Vec::new(),
            browsing: None,
            probed: Default::default(),
            unreachable: Default::default(),
            listing_so_far: Default::default(),
            cut_short: Default::default(),
            probe_errors: Default::default(),
            network_check: |_| false,
            cloud: Vec::new(),
            known: cache.load_dataset_facts(),
            formats: Default::default(),
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
        collections: Vec::new(),
        config_dirs: Vec::new(),
        remembered_dirs: Vec::new(),
        recents: vec![dataset],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
        formats: Default::default(),
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
        collections: Vec::new(),
        config_dirs: Vec::new(),
        remembered_dirs: Vec::new(),
        recents: vec![dataset.clone()],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: pretend_remote,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
        formats: Default::default(),
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
        collections: Vec::new(),
        config_dirs: Vec::new(),
        remembered_dirs: Vec::new(),
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: Some(tmp.path().to_path_buf()),
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
        probe_errors: Default::default(),
        network_check: |_| false,
        cloud: Vec::new(),
        known: cache.load_dataset_facts(),
        formats: Default::default(),
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
        format_spec: None,
        table: None,
    }
}

fn home_with_rows(rows: Vec<datui::discover::Entry>) -> HomeState {
    let mut home = HomeState::default();
    home.apply_listing(datui::home::Listing {
        missing: Default::default(),
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
            root: None,
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

/// Completion takes `\` as a separator on Windows and keeps writing it.
#[cfg(windows)]
#[test]
fn test_completion_follows_windows_separators() {
    use datui::home::complete_path;
    let tmp = TempDir::new().unwrap();
    fs::create_dir_all(tmp.path().join("data").join("sales")).unwrap();

    let (completed, candidates) = complete_path(&format!(r"{}\da", tmp.path().display()));
    assert_eq!(candidates, 1);
    assert!(completed.ends_with(r"\data\"), "{completed}");

    // A trailing `\` lists inside, rather than completing `data` again.
    let (inside, candidates) = complete_path(&completed);
    assert_eq!(candidates, 1);
    assert!(inside.ends_with(r"\data\sales\"), "{inside}");
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

/// Serializes the tests that change the process working directory, which is process
/// state: two of them interleaving would each build the other's listing.
static CWD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Puts the working directory back when the test ends, panicking or not.
struct CwdGuard(std::path::PathBuf);
impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

fn in_cwd(dir: &std::path::Path) -> (std::sync::MutexGuard<'static, ()>, CwdGuard) {
    let lock = CWD.lock().unwrap_or_else(|e| e.into_inner());
    let restore = CwdGuard(std::env::current_dir().unwrap());
    std::env::set_current_dir(dir).unwrap();
    (lock, restore)
}

#[test]
fn test_a_recent_in_the_current_directory_is_not_listed_twice() {
    // Opening a few files from where datui was started used to make the first
    // screen say everything twice: a RECENT place for the cwd, then the
    // current-directory section with the same rows.
    let tmp = TempDir::new().unwrap();
    let here = touch(tmp.path(), "opened_from_cwd.parquet");
    let away_dir = TempDir::new().unwrap();
    let away = touch(away_dir.path(), "opened_from_away.parquet");
    let _cwd = in_cwd(tmp.path());

    let mut home = HomeState::default();
    home.rebuild(&[], &[here, away]);

    let names = visible_names(&home);
    assert_eq!(
        names
            .iter()
            .filter(|n| *n == "opened_from_cwd.parquet")
            .count(),
        1,
        "the current-directory section already lists it: {names:?}"
    );
    // A recent in any other directory keeps its place row and its row.
    assert!(names.iter().any(|n| n == "opened_from_away.parquet"));
    let places: Vec<std::path::PathBuf> = home
        .visible()
        .into_iter()
        .filter_map(|r| match r {
            Row::Place { path, .. } => Some(path),
            _ => None,
        })
        .collect();
    assert_eq!(
        places,
        vec![away_dir.path().to_path_buf()],
        "no place row repeats the current directory"
    );
}

#[test]
fn test_a_cwd_recent_the_directory_listing_does_not_show_stays_under_recent() {
    // The dedupe goes by what the sections actually contain, not by the path
    // alone: a dotfile is skipped by the directory scan, so dropping its recent
    // for living in the cwd would make it vanish from both sections.
    let tmp = TempDir::new().unwrap();
    let hidden = touch(tmp.path(), ".seen_once.parquet");
    touch(tmp.path(), "listed.parquet");
    let _cwd = in_cwd(tmp.path());

    let mut home = HomeState::default();
    home.rebuild(&[], &[hidden]);

    let names = visible_names(&home);
    assert!(
        names.iter().any(|n| n == ".seen_once.parquet"),
        "nothing else on screen shows it: {names:?}"
    );
    assert!(
        home.visible()
            .iter()
            .any(|r| matches!(r, Row::Place { held: 1, .. })),
        "and the place row survives with it"
    );
}

#[test]
fn test_the_recent_cap_counts_only_the_places_it_shows() {
    // Three recents in the cwd are suppressed as duplicates; they must not use up
    // the cap's budget or be counted by the `… N more in M places` row.
    let tmp = TempDir::new().unwrap();
    let mut recents: Vec<std::path::PathBuf> = (0..3)
        .map(|i| touch(tmp.path(), &format!("dup{i}.parquet")))
        .collect();
    let elsewhere = TempDir::new().unwrap();
    recents.extend(ten_places_of_three(&elsewhere));
    let _cwd = in_cwd(tmp.path());

    let mut home = HomeState::default();
    home.rebuild(&[], &recents);
    home.view_height = 30;

    let rows: Vec<Row<'_>> = home
        .visible()
        .into_iter()
        .filter(|r| r.section() == 0)
        .collect();
    // The same arithmetic as a listing with no cwd recents at all: two whole
    // places shown, eight hidden, and a header that counts thirty, not
    // thirty-three.
    assert!(
        matches!(rows.first(), Some(Row::Header { matches: 30, .. })),
        "{:?}",
        rows.first()
    );
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, Row::Place { .. }))
            .count(),
        2,
        "{rows:?}"
    );
    assert!(
        matches!(
            rows.last(),
            Some(Row::More {
                hidden: 24,
                places: 8,
                ..
            })
        ),
        "the suppressed place is not among the hidden: {:?}",
        rows.last()
    );
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
        missing: Default::default(),
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
            root: None,
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
    home.search
        .set_results(vec![datui::discover::Entry::for_test(
            &deep,
            "a/b/buried.parquet",
        )]);
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
    home.search
        .set_results(vec![datui::discover::Entry::for_test(
            &deep,
            "a/found.parquet",
        )]);
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
    home.search
        .set_results(vec![datui::discover::Entry::for_test(
            &here,
            "visible.parquet",
        )]);
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
        home.search.indexed == 0,
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
    home.search
        .set_results(vec![datui::discover::Entry::for_test(
            &found,
            "one.parquet",
        )]);
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
        subtitle.contains("4,321"),
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
    home.search
        .set_results(vec![datui::discover::Entry::for_test(&deep, "a/x.parquet")]);
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
        tables: None,
        opens_one: false,
        ipc_stream: false,
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
    assert_eq!(row.holds.skipped, 1, "the marker is counted");
    assert_eq!(
        row.holds.line(true).as_deref(),
        Some("3 csv"),
        "and the pane names what there is to open"
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
        collections: Vec::new(),
        config_dirs: vec![configured.clone()],
        remembered_dirs: Vec::new(),
        recents: vec![recent],
        desktop_dirs: vec![elsewhere_dir],
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        listing_so_far: Default::default(),
        cut_short: Default::default(),
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
        formats: Default::default(),
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

// The hive label comes from the cloud classifier.
#[cfg(feature = "cloud")]
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
    assert_eq!(first.name, "blocks (hive table: date)");
    assert_eq!(first.kind, EntryKind::Hive);
    assert_eq!(
        first.path,
        PathBuf::from("s3://aws-public-blockchain/v1.0/btc/blocks/")
    );
    assert_eq!(home.sections[0].rows.len(), 2, "the door is not among them");
    assert!(
        home.selection_is_the_door(),
        "a hive table is one dataset, so the cursor lands on the row that opens it"
    );

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
        Some("parquet (all files, mixed)")
    );
    // Not a dataset: the cursor lands on the first thing in it, so the first Enter never
    // reads `by_year` and `by_station` together.
    assert!(!home.selection_is_the_door());
    assert_eq!(
        home.selected_entry().map(|e| e.path),
        Some(PathBuf::from("s3://noaa-ghcn-pds/parquet/by_year"))
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
fn test_collections_are_sections_of_named_datasets() {
    use datui::home::{Collection, CollectionDataset};
    use std::path::{Path, PathBuf};
    let dir = TempDir::new().unwrap();
    let sales = dir.path().join("sales.csv");
    fs::write(&sales, "a,b\n1,2\n").unwrap();
    let archive = dir.path().join("archive");
    fs::create_dir(&archive).unwrap();
    fs::write(archive.join("2024.csv"), "a,b\n1,2\n").unwrap();
    let gone = dir.path().join("gone.parquet");
    let noaa = PathBuf::from("s3://noaa-ghcn-pds/parquet/");
    let penguins = PathBuf::from("https://example.com/penguins.csv");
    let overture = PathBuf::from("abfss://release@overturemapswestus2.dfs.core.windows.net/");
    let dataset = |name: &str, location: &Path| CollectionDataset {
        name: name.to_string(),
        location: location.to_path_buf(),
        details: vec![("about".to_string(), format!("{name} data"))],
        size: None,
    };
    let mut home = HomeState {
        collections: vec![
            Collection {
                name: "mine".to_string(),
                label: "My datasets".to_string(),
                builtin: false,
                datasets: vec![
                    dataset("Sales", &sales),
                    dataset("Archive", &archive),
                    dataset("Gone", &gone),
                    dataset("Weather", &noaa),
                    dataset("Penguins", &penguins),
                ],
            },
            Collection {
                name: "public".to_string(),
                label: "Public datasets".to_string(),
                builtin: true,
                // The same place as `Weather`: the collection listed first names it.
                datasets: vec![dataset("Overture Maps", &overture), dataset("NOAA", &noaa)],
            },
        ],
        network_check: |_| false,
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    let section = |title: &str| {
        home.sections
            .iter()
            .find(|s| s.title == title)
            .unwrap_or_else(|| panic!("no {title} section"))
    };
    let mine = section("My datasets");
    assert_eq!(mine.origin, Some("configured"));
    assert_eq!(section("Public datasets").origin, Some("built in"));
    let rows: Vec<(&str, EntryKind)> = mine
        .rows
        .iter()
        .map(|r| (r.name.as_str(), r.kind))
        .collect();
    assert_eq!(
        rows,
        [
            ("Sales", EntryKind::File),
            ("Archive", EntryKind::Directory),
            ("Gone", EntryKind::Unknown),
            ("Weather", EntryKind::Directory),
            ("Penguins", EntryKind::File),
        ],
        "named by the config, each kind found without reading anything remote"
    );
    let titles: Vec<&str> = home.sections.iter().map(|s| s.title.as_str()).collect();
    let (mine_at, public_at) = (
        titles.iter().position(|t| *t == "My datasets").unwrap(),
        titles.iter().position(|t| *t == "Public datasets").unwrap(),
    );
    assert!(
        mine_at < public_at,
        "the built-in catalog comes last: {titles:?}"
    );

    // A missing local dataset stays, and says so.
    assert!(home.missing.contains(&gone));
    assert_eq!(home.place_kind(&gone), Some("missing"));
    assert_eq!(home.place_kind(&noaa), Some("dataset"));
    assert_eq!(home.place_kind(&sales), None);
    assert_eq!(
        home.place_details(&noaa).unwrap(),
        [("about".to_string(), "Weather data".to_string())]
    );
    // Nothing is asked of a remote dataset's store until it is entered.
    assert!(home.cloud_directories_to_peek(10).is_empty());

    let year = Path::new("s3://noaa-ghcn-pds/parquet/by_year");
    assert_eq!(
        home.parent_of(year),
        Some(noaa.clone()),
        "the dataset as listed"
    );
    assert_eq!(
        home.parent_of(Path::new("s3://noaa-ghcn-pds/parquet")),
        None,
        "a dataset's root goes back to the listing, not up the bucket"
    );
    assert_eq!(home.parent_of(&overture), None);
    let sep = datui::glyphs::get().trail;
    assert_eq!(
        home.location_label(Path::new("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2020")),
        format!("My datasets {sep} Weather {sep} by_year {sep} YEAR=2020")
    );
    home.browsing = Some(year.to_path_buf());
    home.browse_start = Some(noaa.clone());
    assert!(home.below_browse_start());

    // Inside, the dataset is titled by its name, not its URL.
    home.browsing = Some(noaa.clone());
    home.browse_start = Some(noaa.clone());
    home.rebuild(&[], &[]);
    assert_eq!(home.sections[0].title, "Weather");
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
            collections: Vec::new(),
            config_dirs: Vec::new(),
            remembered_dirs: Vec::new(),
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: Some(tmp.path().to_path_buf()),
            probed: Default::default(),
            unreachable: Default::default(),
            listing_so_far: Default::default(),
            cut_short: Default::default(),
            probe_errors: Default::default(),
            network_check: |_| false,
            cloud: Vec::new(),
            known: known.into_iter().collect(),
            formats: Default::default(),
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
        Some("exports (3 Parquet files, one schema)"),
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
        Some("exports (3 Parquet files, one schema)"),
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
    assert_eq!(row.name, "exports (12 Parquet files, one schema)");
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
        first(&home).is_some_and(|n| n.ends_with("(2 Parquet files, one schema)")),
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
    assert!(first(&home).is_some_and(|n| n.ends_with("(2 Parquet files, one schema)")));

    // And it is not one of the things being ordered.
    for sort in [SortMode::Size, SortMode::Rows, SortMode::Modified] {
        home.sort = sort;
        assert!(
            first(&home).is_some_and(|n| n.ends_with("(2 Parquet files, one schema)")),
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
    assert_eq!(door.name, "/ (all files, mixed)", "got {:?}", door.name);
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

    assert_eq!(named("s3://bucket"), "bucket (1 Parquet file)");
    assert_eq!(named("s3://lab@bucket"), "bucket (1 Parquet file)");
    assert_eq!(named("s3://lab@bucket/exports"), "exports (1 Parquet file)");
    assert_eq!(named("gs://bucket/exports/"), "exports (1 Parquet file)");
    assert_eq!(
        named("abfss://raw@acct.dfs.core.windows.net"),
        "raw (1 Parquet file)"
    );
    assert_eq!(
        named("abfss://raw@acct.dfs.core.windows.net/tbl"),
        "tbl (1 Parquet file)"
    );
}

/// The door is not one of the things the section is counting.
///
/// It is a way to open the directory those rows are *in*, so counting it made a directory
/// of three files say four — in the header chip and in the `N datasets` caption, whose
/// own comment says counting a place-to-look makes the figure a lie. It was inconsistent
/// with itself too: under a filter the door steps out of the way, so the same count meant
/// one thing with a filter typed and another without.
/// The door says what its directory is stored on, as the rows beside it do (#547 D10).
#[test]
fn test_the_door_says_where_it_reads() {
    let tmp = TempDir::new().unwrap();
    for name in ["part-0.parquet", "part-1.parquet"] {
        touch(tmp.path(), name);
    }
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    let door = door_of(&home).expect("a door");
    let row = home.sections[0].rows.first().expect("a row");
    assert!(door.cost.source.is_some());
    assert_eq!(door.cost.source, row.cost.source);
}

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

// ---------------------------------------------------------------------------
// Column search reaches the recursive walk
// ---------------------------------------------------------------------------

/// A dataset found by the walk carries what earlier runs measured, so filtering
/// by a column name surfaces it — the promise "customer_id finds every dataset
/// with that column" used to stop at the rows already listed, because search
/// results never consulted the facts index.
#[test]
fn a_found_dataset_matches_by_its_remembered_columns() {
    use datui::cache::DatasetFacts;
    use datui::discover;

    let tmp = TempDir::new().unwrap();
    let buried = touch(&tmp.path().join("deep"), "sales.parquet");
    let meta = fs::metadata(&buried).unwrap();
    let mtime = meta
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut home = HomeState {
        filter: "revenue".into(),
        ..Default::default()
    };
    home.known.insert(
        buried.clone(),
        DatasetFacts {
            mtime,
            size: meta.len(),
            rows: Some(9),
            cols: Some(2),
            cols_sampled: false,
            columns: vec!["revenue".into(), "region".into()],
            kind: Some(EntryKind::File),
            classified_by: discover::CLASSIFIER_VERSION,
            holds: Default::default(),
            cost: Default::default(),
        },
    );
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.running = true;

    let mut walked = Vec::new();
    datui::search::walk(
        tmp.path(),
        &datui::config::SearchConfig::default(),
        |batch, _| {
            walked.extend(batch);
            true
        },
    );
    assert_eq!(walked.len(), 1, "the walk found the buried file");
    home.search_batch(tmp.path(), walked, 1);

    assert_eq!(
        home.search.files().next().unwrap().columns,
        vec!["revenue".to_string(), "region".to_string()],
        "the walk's entry took the remembered columns"
    );
    let section = home
        .sections
        .iter()
        .find(|s| s.title == HomeState::SEARCH_SECTION)
        .expect("the found section is on screen");
    assert_eq!(section.rows.len(), 1, "and the row matched by column name");
}

/// Inside a directory of notes: no `(all files)` row, since there is nothing for it to
/// read, and one row saying how many files are hidden, so the directory does not look
/// empty or broken. Ctrl+A shows them and the row goes.
#[test]
fn test_inside_a_directory_of_notes_a_row_says_what_is_hidden() {
    let tmp = TempDir::new().unwrap();
    for i in 0..10 {
        touch(tmp.path(), &format!("note{i}.md"));
    }
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let rows = home.visible();
    assert!(
        !rows.iter().any(|r| matches!(r, Row::Door { .. })),
        "nothing for the door to read"
    );
    assert!(
        rows.iter()
            .any(|r| matches!(r, Row::Hidden { count: 10, .. })),
        "{rows:?}"
    );
    home.select_first_entry();
    assert!(
        matches!(home.selected_row(), Some(Row::Hidden { .. })),
        "the cursor lands on the only row there is"
    );

    home.hide_unreadable = false;
    let rows = home.visible();
    assert!(!rows.iter().any(|r| matches!(r, Row::Hidden { .. })));
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, Row::Entry { .. }))
            .count(),
        10
    );
}

/// A part file with no extension is listed when its bytes say it is data, and the
/// door stays; a `LICENSE` beside it is hidden like any file datui cannot open.
#[test]
fn test_an_extensionless_part_file_is_listed_by_its_bytes() {
    let tmp = TempDir::new().unwrap();
    fs::write(tmp.path().join("part-00000"), b"PAR1 footer here PAR1").unwrap();
    fs::write(tmp.path().join("LICENSE"), b"MIT").unwrap();
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);

    let rows = home.visible();
    let listed: Vec<&str> = rows
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some(entry.name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(listed, vec!["part-00000"], "{rows:?}");
    assert!(rows.iter().any(|r| matches!(r, Row::Door { .. })));
    assert!(
        rows.iter()
            .any(|r| matches!(r, Row::Hidden { count: 1, .. }))
    );
}

/// Part files a signature cannot identify — Spark's text output — still earn the door:
/// the open reads a directory of them by their bytes.
#[test]
fn test_unidentified_extensionless_files_keep_the_door() {
    let tmp = TempDir::new().unwrap();
    fs::write(tmp.path().join("part-00000"), b"id,name\n1,a\n").unwrap();
    let (_, holds) = discover::look_at_directory(tmp.path());
    assert_eq!(holds.unnamed, 1);
    assert_eq!(holds.not_read, 0);
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    assert!(home.visible().iter().any(|r| matches!(r, Row::Door { .. })));
}

/// Coming back is by row, not index, and waits for the row: a walk still running when
/// the user went inside is started again, and the cursor goes to the row it left when
/// the walk finds it — unless the user has moved the cursor since.
#[test]
fn test_coming_back_waits_for_a_row_still_to_arrive() {
    let tmp = TempDir::new().unwrap();
    for name in ["a.csv", "b.csv", "deep_x.csv"] {
        touch(tmp.path(), name);
    }
    let sub = tmp.path().join("sub");
    let deep = touch(&sub, "deep/deep.parquet");
    let found = || vec![datui::discover::Entry::for_test(&deep, "deep.parquet")];
    let mut home = HomeState {
        browsing: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.filter = "deep".into();
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.running = true;
    home.search_batch(tmp.path(), found(), 3);
    let at = home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Entry { entry, .. } if entry.path == deep))
        .expect("the walk's row");
    home.move_selection(at as isize - home.selected as isize);

    // Inside, and back: the walk was still out, so it is not kept.
    home.leave_mark();
    home.browsing = Some(deep.parent().unwrap().to_path_buf());
    home.filter.clear();
    home.search.reset();
    home.rebuild(&[], &[]);
    home.browsing = Some(tmp.path().to_path_buf());
    home.come_back(Some(sub.clone()));
    assert_eq!(home.filter, "deep");
    assert_eq!(home.search.indexed, 0);
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.running = true;
    home.rebuild(&[], &[]);
    assert_ne!(
        home.selected_entry().map(|e| e.path),
        Some(deep.clone()),
        "not there yet"
    );
    assert!(home.returning.is_some(), "still waiting for it");
    home.search_batch(tmp.path(), found(), 3);
    assert_eq!(home.selected_entry().map(|e| e.path), Some(deep.clone()));
    assert!(home.returning.is_none());

    // The same again, but the user moves first: the cursor stays where they put it.
    home.search.running = true;
    home.leave_mark();
    home.come_back(None);
    home.search.root = Some(tmp.path().to_path_buf());
    home.search.running = true;
    home.rebuild(&[], &[]);
    assert!(home.returning.is_some());
    home.move_selection(1);
    let moved = home.selected;
    home.search_batch(tmp.path(), found(), 3);
    assert_eq!(home.selected, moved);
    assert!(home.returning.is_none());
}

// ---------------------------------------------------------------------------
// Coming back: leaving a place puts the cursor on the row it was entered from
// ---------------------------------------------------------------------------

mod coming_back {
    use super::touch;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use datui::home::Row;
    use datui::{App, AppEvent};
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::Receiver;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    thread_local! {
        /// Each test's private cache, removed when its thread ends.
        static CACHES: std::cell::RefCell<Vec<TempDir>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    /// The home screen over `config`, its first listing landed. Its recents are its
    /// own: other tests in this binary open datasets, and a recent landing in the
    /// shared cache mid-test moved the rows a test was coming back to (#658).
    pub(super) fn home_app(mut config: datui::config::AppConfig) -> (App, Receiver<AppEvent>) {
        config.home.desktop_recents = false;
        config.home.hide = vec!["public".to_string()];
        // Whatever this machine is logged in to is not part of the test.
        config.cloud.discover = Some(datui::config::CloudDiscover::None);
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new_with_config(
            tx,
            crate::common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        );
        let cache = TempDir::new().unwrap();
        app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
        CACHES.with(|caches| caches.borrow_mut().push(cache));
        app.enter_home();
        settle(&mut app, &rx, |_| true);
        (app, rx)
    }

    fn handle(app: &mut App, event: AppEvent) {
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
    }

    /// Handle events until no listing is out and `done` holds, then draw a frame, which
    /// is what settles the scroll.
    pub(super) fn settle(app: &mut App, rx: &Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            while let Ok(event) = rx.try_recv() {
                handle(app, event);
            }
            if !app.home.listing_in_flight && !app.home.search.running && done(app) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the home screen never settled: browsing {:?}, listing {}, rows {:?}",
                app.home.browsing,
                app.home.listing_in_flight,
                entries(app)
            );
            if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
                handle(app, event);
            }
        }
        draw(app);
    }

    pub(super) fn draw(app: &mut App) {
        let area = Rect::new(0, 0, 100, 20);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
    }

    pub(super) fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
    }

    pub(super) fn entries(app: &App) -> Vec<PathBuf> {
        app.home
            .visible()
            .iter()
            .filter_map(|row| match row {
                Row::Entry { entry, .. } => Some(entry.path.clone()),
                _ => None,
            })
            .collect()
    }

    /// Put the cursor on the row for `path`, the way arrowing to it would.
    pub(super) fn select(app: &mut App, path: &Path) {
        let index = app
            .home
            .visible()
            .iter()
            .position(|row| matches!(row, Row::Entry { entry, .. } if entry.path == path))
            .unwrap_or_else(|| panic!("a row for {path:?} in {:?}", entries(app)));
        let delta = index as isize - app.home.selected as isize;
        app.home.move_selection(delta);
        draw(app);
    }

    pub(super) fn on(app: &App) -> Option<PathBuf> {
        app.home.selected_entry().map(|entry| entry.path)
    }

    /// Where the cursor is in the viewport. Other tests in this binary open datasets,
    /// and the recents they leave can add rows above it in the shared cache, so this,
    /// not the index, is what coming back has to keep.
    pub(super) fn on_screen(app: &App) -> usize {
        app.home.selected - app.home.scroll
    }

    /// Enter or →, then wait for the place it went into to list. Its rows, not the
    /// in-flight flag alone: a superseded listing clears that flag as it is dropped.
    pub(super) fn go_into(app: &mut App, rx: &Receiver<AppEvent>, code: KeyCode, place: &Path) {
        let before = entries(app);
        assert!(press(app, code).is_none(), "went inside, opened nothing");
        assert_eq!(app.home.browsing.as_deref(), Some(place));
        settle(app, rx, |app| {
            let now = entries(app);
            !now.is_empty() && now != before
        });
    }

    /// Esc, then wait for where it went back to.
    pub(super) fn go_back(app: &mut App, rx: &Receiver<AppEvent>, to: Option<&Path>) {
        press(app, KeyCode::Esc);
        assert_eq!(app.home.browsing.as_deref(), to);
        settle(app, rx, |app| app.home.returning.is_none());
    }

    /// Forty directories of mixed files, so each is a place to go inside rather than a
    /// table, and one near the end is well off the first screen.
    fn many_directories(tmp: &Path) {
        for i in 0..40 {
            let dir = tmp.join(format!("d{i:02}"));
            touch(&dir, "x.csv");
            touch(&dir, "y.parquet");
        }
    }

    fn local_config(dir: &Path) -> datui::config::AppConfig {
        let mut config = datui::config::AppConfig::default();
        config.home.directories = vec![dir.to_string_lossy().into_owned()];
        config
    }

    #[test]
    fn esc_from_a_directory_puts_the_cursor_back_on_it_level_by_level() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d30 = tmp.path().join("d30");
        let inner = d30.join("inner");
        let deeper = inner.join("deeper");
        touch(&inner, "a.csv");
        touch(&deeper, "x.csv");
        touch(&deeper, "y.parquet");
        // After `inner`, so the row entered is not the only one.
        touch(&d30, "zz/x.csv");
        touch(&d30, "zz/y.parquet");
        let (mut app, rx) = home_app(local_config(tmp.path()));

        select(&mut app, &d30);
        let (offset, scroll) = (on_screen(&app), app.home.scroll);
        assert!(scroll > 0, "the row is below the first screen");
        go_into(&mut app, &rx, KeyCode::Enter, &d30);
        select(&mut app, &inner);
        let inner_at = app.home.selected;
        go_into(&mut app, &rx, KeyCode::Enter, &inner);
        select(&mut app, &deeper);
        go_into(&mut app, &rx, KeyCode::Right, &deeper);

        go_back(&mut app, &rx, Some(&inner));
        assert_eq!(on(&app), Some(deeper.clone()));
        go_back(&mut app, &rx, Some(&d30));
        assert_eq!(on(&app), Some(inner.clone()));
        assert_eq!(app.home.selected, inner_at);
        go_back(&mut app, &rx, None);
        assert_eq!(on(&app), Some(d30.clone()));
        assert_eq!(on_screen(&app), offset);
        assert!(app.home.trail.is_empty(), "{:?}", app.home.trail);
    }

    /// The row comes back on the line it was left on, not wherever keeping it in view
    /// would put it: here mid-screen, after the list scrolled down past it (#551).
    #[test]
    fn esc_puts_the_row_back_on_the_line_it_was_left_on() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d30 = tmp.path().join("d30");
        let (mut app, rx) = home_app(local_config(tmp.path()));

        select(&mut app, &tmp.path().join("d39"));
        select(&mut app, &d30);
        let (offset, scroll) = (on_screen(&app), app.home.scroll);
        assert!(scroll > 0, "the list scrolled");
        assert!(offset > 2 && offset < 12, "mid-screen: {offset}");
        go_into(&mut app, &rx, KeyCode::Enter, &d30);
        go_back(&mut app, &rx, None);
        assert_eq!(on(&app), Some(d30));
        assert_eq!(on_screen(&app), offset);
    }

    /// Backspace goes up whether or not the user came that way; where they did not,
    /// the cursor lands on the directory just left.
    #[test]
    fn backspace_above_where_the_browse_began_lands_on_the_directory_left() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d12 = tmp.path().join("d12");
        let (mut app, rx) = home_app(datui::config::AppConfig::default());
        // Typed at `~`: the browse starts in `d12`, its parent never listed.
        press(&mut app, KeyCode::Char('~'));
        for c in d12.to_string_lossy().chars() {
            press(&mut app, KeyCode::Char(c));
        }
        go_into(&mut app, &rx, KeyCode::Enter, &d12);

        press(&mut app, KeyCode::Backspace);
        assert_eq!(app.home.browsing.as_deref(), Some(tmp.path()));
        settle(&mut app, &rx, |app| app.home.returning.is_none());
        assert_eq!(on(&app), Some(d12));
        // And Esc still goes back to the listing the path was typed at.
        go_back(&mut app, &rx, None);
    }

    /// The pane's facts share one key column, the place's own details too, and a long
    /// value wraps under itself rather than back to the pane's edge (#547 D6).
    #[test]
    fn the_details_pane_lines_its_facts_up_and_hangs_long_values() {
        let tmp = TempDir::new().unwrap();
        let file = touch(tmp.path(), "penguins.csv");
        std::fs::write(&file, "species,mass\nAdelie,3750\n").unwrap();
        let mut config = datui::config::AppConfig::default();
        config.sources = vec![datui::config::SourceConfig {
            name: "lab".to_string(),
            label: Some("Lab".to_string()),
            datasets: vec![datui::config::DatasetConfig {
                name: "Palmer penguins".to_string(),
                path: Some(file.to_string_lossy().into_owned()),
                description: "Size measurements for three penguin species observed on \
                              three islands in the Palmer Archipelago, Antarctica"
                    .to_string(),
                publisher: "Palmer Station LTER".to_string(),
                homepage: "https://allisonhorst.github.io/palmerpenguins/articles/intro.html"
                    .to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }];
        let (mut app, _rx) = home_app(config);
        select(&mut app, &file);
        let area = Rect::new(0, 0, 120, 40);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<Vec<String>> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect();
        let text = |row: &[String]| row.concat();
        // The column a fact's value starts at, from its key.
        let value_at = |key: &str| {
            rows.iter()
                .find_map(|row| {
                    let line = text(row);
                    let at = line.find(&format!("│ {key} "))?;
                    let start = line[..at].chars().count() + 2 + key.chars().count();
                    (start..row.len()).find(|&x| row[x] != " ")
                })
                .unwrap_or_else(|| {
                    panic!(
                        "no {key} line: {:#?}",
                        rows.iter().map(|r| text(r)).collect::<Vec<_>>()
                    )
                })
        };
        let column = value_at("kind");
        for key in ["storage", "about", "publisher", "homepage"] {
            assert_eq!(value_at(key), column, "{key} lines up with kind");
        }
        // The line after `about` carries the rest of it, under the value.
        let about = rows
            .iter()
            .position(|row| text(row).contains("│ about "))
            .unwrap();
        let next = &rows[about + 1];
        let first = (0..next.len())
            .skip_while(|&x| next[x] != "│")
            .skip(1)
            .find(|&x| next[x] != " ")
            .expect("a continued value");
        assert_eq!(first, column, "{:?}", text(next));
    }

    /// A Parquet file whose footer cannot be read says so before Enter, and the open
    /// that fails is reported once, in the dialog, not again on the prompt (#547 D8).
    #[test]
    fn a_broken_parquet_file_says_so_and_its_failure_is_said_once() {
        let tmp = TempDir::new().unwrap();
        let broken = touch(tmp.path(), "broken.parquet");
        std::fs::write(&broken, vec![7u8; 4000]).unwrap();
        touch(tmp.path(), "fine.csv");
        let (mut app, rx) = home_app(local_config(tmp.path()));
        select(&mut app, &broken);
        settle(&mut app, &rx, |app| {
            app.home.enriched.contains_key(&broken) && !app.home.measure_in_flight
        });
        let area = Rect::new(0, 0, 120, 30);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let screen: String = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(screen.contains("Its footer could not be read"), "{screen}");
        assert!(!screen.contains("Columns are read when opened"), "{screen}");

        let Some(AppEvent::Open(paths, options)) = press(&mut app, KeyCode::Enter) else {
            panic!("Enter on a file opens it");
        };
        crate::common::pump_open_until_loaded(&mut app, &rx, paths, options);
        assert!(app.error_message().is_some(), "the dialog says it failed");
        assert_eq!(
            app.home.status, None,
            "and the prompt does not say it again"
        );
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.error_message(), None);
        assert_eq!(app.home.status, None, "nor after the dialog is dismissed");
    }

    /// The whole screen at 80×24 and 200×50: the bar teaches typing, `~` and `?` at
    /// both, the order shows only where every key fits, and the count is on the rule
    /// (#547 M2, D11).
    #[test]
    fn the_home_screen_at_80_by_24_and_200_by_50() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let (mut app, _rx) = home_app(local_config(tmp.path()));
        for (w, h) in [(80u16, 24u16), (200, 50)] {
            let area = Rect::new(0, 0, w, h);
            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);
            let row =
                |y: u16| -> String { (0..w).map(|x| buf[(x, y)].symbol().to_string()).collect() };
            let bar = row(h - 1);
            for chip in ["type  Filter", "~  Path", "?  Help", "^C  Quit"] {
                assert!(bar.contains(chip), "{w}x{h}: {chip} in {bar:?}");
            }
            assert!(!bar.contains("datasets"), "{w}x{h}: {bar:?}");
            assert_eq!(bar.contains("by name"), w == 200, "{w}x{h}: {bar:?}");
            let screen: Vec<String> = (0..h).map(row).collect();
            assert!(
                screen.iter().any(|r| r.contains("  40   configured")),
                "{w}x{h}: the rule counts the rows: {screen:#?}"
            );
        }
    }

    #[test]
    fn esc_from_a_collection_dataset_puts_the_cursor_back_on_it() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        // A directory section long enough to scroll, in place of the working
        // directory. Other tests in this binary open files meanwhile, and a recent
        // they leave lands above the collection; a list that fits the screen cannot
        // scroll to keep the cursor's line, so the line moved and this failed.
        let listed = TempDir::new().unwrap();
        many_directories(listed.path());
        let mut config = local_config(listed.path());
        config.sources = vec![datui::config::SourceConfig {
            name: "lab".to_string(),
            label: Some("Lab".to_string()),
            datasets: ["d01", "d02", "d03"]
                .iter()
                .map(|name| datui::config::DatasetConfig {
                    name: name.to_string(),
                    path: Some(tmp.path().join(name).to_string_lossy().into_owned()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }];
        let (mut app, rx) = home_app(config);
        let d03 = tmp.path().join("d03");

        select(&mut app, &d03);
        let offset = on_screen(&app);
        go_into(&mut app, &rx, KeyCode::Enter, &d03);
        go_back(&mut app, &rx, None);
        assert_eq!(on(&app), Some(d03));
        assert_eq!(on_screen(&app), offset);
    }

    /// A search result opened and closed: home comes back to the results, filter and
    /// row, and Esc then backs out the filter and the directory as usual.
    #[test]
    fn a_search_result_opened_and_closed_comes_back_to_the_results() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d07 = tmp.path().join("d07");
        let found = touch(&d07, "zz/deep.csv");
        touch(&d07, "aa/deep_first.csv");
        std::fs::write(&found, "id,name\n1,ada\n").unwrap();
        let (mut app, rx) = home_app(local_config(tmp.path()));

        select(&mut app, &d07);
        go_into(&mut app, &rx, KeyCode::Enter, &d07);
        for c in "deep".chars() {
            press(&mut app, KeyCode::Char(c));
        }
        settle(&mut app, &rx, |app| app.home.search.done);
        select(&mut app, &found);
        let offset = on_screen(&app);
        let Some(AppEvent::Open(paths, options)) = press(&mut app, KeyCode::Enter) else {
            panic!("Enter on a file opens it");
        };
        crate::common::pump_open_until_loaded(&mut app, &rx, paths, options);
        assert!(app.data_table_state.is_some(), "the table loaded");

        app.enter_home();
        settle(&mut app, &rx, |app| app.home.selected_entry().is_some());
        assert_eq!(app.home.filter, "deep");
        assert_eq!(on(&app), Some(found));
        assert_eq!(on_screen(&app), offset);
        press(&mut app, KeyCode::Esc);
        assert!(app.home.filter.is_empty());
        go_back(&mut app, &rx, None);
        assert_eq!(on(&app), Some(d07));
    }

    /// Past the files the screen scores as it types, the scoring runs on a worker and
    /// still finds the one match among thousands (#547 D1, D2).
    #[test]
    fn typing_in_a_large_tree_finds_the_one_match() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d07 = tmp.path().join("d07");
        for i in 0..2_500 {
            touch(
                &d07.join(format!("bulk{:02}", i % 25)),
                &format!("f{i:04}.csv"),
            );
        }
        let needle = touch(&d07, "deep/a/b/c/needle_metrics.parquet");
        let (mut app, rx) = home_app(local_config(tmp.path()));

        select(&mut app, &d07);
        go_into(&mut app, &rx, KeyCode::Enter, &d07);
        for c in "needle".chars() {
            press(&mut app, KeyCode::Char(c));
        }
        settle(&mut app, &rx, |app| {
            app.home.search.done && !app.home.search.scoring && entries(app).contains(&needle)
        });
        assert!(app.home.search.indexed > 2_500, "every file was kept");
        assert_eq!(app.home.filter, "needle");

        // A new query over every file kept is scored on a worker, and lists the cap.
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Char('f'));
        assert!(
            app.home.search.scoring,
            "too many files to score as the key lands"
        );
        settle(&mut app, &rx, |app| !app.home.search.scoring);
        let found = app
            .home
            .sections
            .iter()
            .find(|s| s.title == datui::home::HomeState::SEARCH_SECTION)
            .expect("found");
        assert_eq!(found.rows.len(), 1_000);
        let subtitle = found.subtitle.clone().unwrap_or_default();
        assert!(subtitle.contains("1,000 of 2,500 matches"), "{subtitle:?}");
    }

    /// A table opened from inside a directory, and back: home is where it was left,
    /// and Esc from there still comes back to the row the directory was entered from.
    #[test]
    fn a_dataset_opened_inside_a_directory_leaves_the_way_back() {
        let tmp = TempDir::new().unwrap();
        many_directories(tmp.path());
        let d25 = tmp.path().join("d25");
        let people = d25.join("people.csv");
        std::fs::write(&people, "id,name\n1,ada\n2,grace\n").unwrap();
        let (mut app, rx) = home_app(local_config(tmp.path()));

        select(&mut app, &d25);
        let offset = on_screen(&app);
        go_into(&mut app, &rx, KeyCode::Enter, &d25);
        select(&mut app, &people);
        let Some(AppEvent::Open(paths, options)) = press(&mut app, KeyCode::Enter) else {
            panic!("Enter on a file opens it");
        };
        crate::common::pump_open_until_loaded(&mut app, &rx, paths, options);
        assert!(app.data_table_state.is_some(), "the table loaded");

        app.enter_home();
        settle(&mut app, &rx, |app| app.home.selected_entry().is_some());
        assert_eq!(app.home.browsing.as_deref(), Some(d25.as_path()));
        assert_eq!(on(&app), Some(people));
        go_back(&mut app, &rx, None);
        assert_eq!(on(&app), Some(d25));
        assert_eq!(on_screen(&app), offset);
    }
}

/// A cloud source, its bucket, and prefixes three deep, against a local stand-in for
/// S3, then back out one level at a time.
#[cfg(feature = "cloud")]
#[test]
fn test_esc_back_through_a_cloud_source_puts_the_cursor_on_each_row_entered() {
    use coming_back::{entries, go_back, go_into, home_app, on, on_screen, press, select, settle};
    use crossterm::event::KeyCode;
    use std::path::PathBuf;

    let objects = [
        "a/x.csv",
        "b/x.csv",
        "m/1/x.csv",
        "m/q/k/x.csv",
        "m/q/z/x.csv",
        "m/q/z/y.csv",
    ]
    .iter()
    .map(|key| (key.to_string(), b"a\n1\n".to_vec()))
    .collect();
    let s3 = fake_s3::FakeS3::serve("lake", objects);
    // Signatures are not checked, so any variable cargo sets will do for the keys;
    // setting one here would race the other tests in this binary.
    let connection = |name: &str, key: &str| datui::config::CloudConnectionConfig {
        name: name.to_string(),
        kind: Some("s3".to_string()),
        endpoint_url: Some(s3.endpoint.clone()),
        region: Some("us-east-1".to_string()),
        addressing: Some("path".to_string()),
        access_key_id_env: Some(key.to_string()),
        secret_access_key_env: Some("CARGO_PKG_NAME".to_string()),
        ..Default::default()
    };
    let mut config = datui::config::AppConfig::default();
    // Bucket names are what `Found` is about here, not the working directory.
    config.home.search.enabled = false;
    // Two, so the one entered is not the first row. Keys of their own: the same server
    // with the same key would be one source.
    config.cloud.connections = vec![
        connection("aa", "CARGO_PKG_NAME"),
        connection("lab", "CARGO_PKG_VERSION"),
    ];
    let (mut app, rx) = home_app(config);
    let source = datui::home::cloud_place("lab");
    settle(&mut app, &rx, |app| entries(app).contains(&source));

    let bucket = PathBuf::from("s3://lab@lake");
    let m = PathBuf::from("s3://lab@lake/m/");
    let q = PathBuf::from("s3://lab@lake/m/q/");
    let z = PathBuf::from("s3://lab@lake/m/q/z/");

    select(&mut app, &source);
    let at_source = on_screen(&app);
    go_into(&mut app, &rx, KeyCode::Enter, &source);
    select(&mut app, &bucket);
    go_into(&mut app, &rx, KeyCode::Enter, &bucket);
    select(&mut app, &m);
    let at_m = on_screen(&app);
    go_into(&mut app, &rx, KeyCode::Right, &m);
    select(&mut app, &q);
    go_into(&mut app, &rx, KeyCode::Right, &q);
    select(&mut app, &z);
    go_into(&mut app, &rx, KeyCode::Right, &z);

    go_back(&mut app, &rx, Some(&q));
    assert_eq!(on(&app), Some(z));
    go_back(&mut app, &rx, Some(&m));
    assert_eq!(on(&app), Some(q));
    go_back(&mut app, &rx, Some(&bucket));
    assert_eq!(on(&app), Some(m));
    assert_eq!(on_screen(&app), at_m);
    go_back(&mut app, &rx, Some(&source));
    assert_eq!(on(&app), Some(bucket.clone()));
    go_back(&mut app, &rx, None);
    assert_eq!(on(&app), Some(source));
    assert_eq!(on_screen(&app), at_source);

    // A bucket under `Found` is entered from the results, so Esc goes back to them,
    // filter and row, rather than to the bucket's source.
    for c in "lake".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    let found = |app: &datui::App| {
        app.home.visible().iter().position(|row| {
            matches!(row, datui::home::Row::Entry { entry, section, .. }
                if entry.path == bucket
                    && app.home.sections[*section].title == datui::home::HomeState::SEARCH_SECTION)
        })
    };
    let at_found = found(&app).expect("the bucket under Found");
    app.home
        .move_selection(at_found as isize - app.home.selected as isize);
    coming_back::draw(&mut app);
    let offset = on_screen(&app);
    go_into(&mut app, &rx, KeyCode::Enter, &bucket);
    go_back(&mut app, &rx, None);
    assert_eq!(app.home.filter, "lake");
    assert_eq!(Some(app.home.selected), found(&app));
    assert_eq!(on_screen(&app), offset);
}

// ---------------------------------------------------------------------------
// Viewport
// ---------------------------------------------------------------------------

/// The home screen at 80×24 over one directory of `files` CSVs, its listing landed.
///
/// The directory's name is longer than its heading has room for, so the heading is
/// cut on every platform rather than only where the temp path is long (#575).
fn home_at_80x24(
    files: usize,
) -> (
    TempDir,
    datui::App,
    std::sync::mpsc::Receiver<datui::AppEvent>,
) {
    common::isolate_cache();
    let tmp = TempDir::with_prefix("a-directory-name-longer-than-its-heading-has-room-for-at-80-")
        .unwrap();
    for i in 0..files {
        touch(tmp.path(), &format!("f{i:02}.csv"));
    }
    let mut config = datui::config::AppConfig::default();
    config.home.directories = vec![tmp.path().to_string_lossy().into_owned()];
    config.home.desktop_recents = false;
    config.cloud.hide = ["s3-default", "gcs-default", "az", "azure-env"]
        .map(String::from)
        .to_vec();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    // Inside the directory, so the list is its files alone. The top level lists the
    // working directory and the recents above it, which other tests in this binary
    // change while they run: one recent of theirs put f05 off an 80x24 screen, and a
    // few put it on f57's line at 200x50, so that one click there was a double click.
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();
    listed(&mut app, &rx, |app| {
        visible_names(&app.home).contains(&"f00.csv".to_string())
    });
    (tmp, app, rx)
}

/// Handle events until no listing is out and `done` holds, then draw.
fn listed(
    app: &mut datui::App,
    rx: &std::sync::mpsc::Receiver<datui::AppEvent>,
    done: impl Fn(&datui::App) -> bool,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while app.home.listing_in_flight || !done(app) {
        // The listing's answer comes on the channel: wait for it, not for time.
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        let event = rx.recv_timeout(left).expect("the listing never landed");
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
    }
    cursor_line(app);
}

/// Draw a frame at 80×24 and say which screen line the cursor is on.
fn cursor_line(app: &mut datui::App) -> u16 {
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut *app, area, &mut buf);
    // A row carries the rail; a section header's rule turns heavy instead.
    let g = datui::glyphs::get();
    let lines: Vec<u16> = (0..area.height)
        .filter(|&y| {
            (0..3).any(|x| buf[(x, y)].symbol() == g.rail)
                || (0..area.width).any(|x| buf[(x, y)].symbol() == g.rule_h_focused)
        })
        .collect();
    let screen: Vec<String> = (0..area.height)
        .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect())
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "one row carries the rail: {lines:?}\n{}",
        screen.join("\n")
    );
    lines[0]
}

fn press_and_draw(app: &mut datui::App, code: crossterm::event::KeyCode) -> u16 {
    let _ = app.event(&datui::AppEvent::Key(crossterm::event::KeyEvent::new(
        code,
        crossterm::event::KeyModifiers::NONE,
    )));
    cursor_line(app)
}

/// The mouse on the home list, at 80×24 and on a wide screen: a click puts the
/// cursor on the row under it, the wheel moves it three rows and stops at the ends
/// rather than going round, and a double click opens the row, as Enter does.
#[test]
fn test_a_click_selects_a_home_row_and_the_wheel_stops_at_the_ends() {
    use crossterm::event::{KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    for (width, height) in [(80, 24), (200, 50)] {
        let (_tmp, app, rx) = home_at_80x24(60);
        let (tx, _) = std::sync::mpsc::channel();
        let mut pump = datui::event_pump::EventPump::new(app, tx, rx);
        let area = Rect::new(0, 0, width, height);
        let draw = |app: &mut datui::App| {
            let mut buf = Buffer::empty(area);
            Widget::render(&mut *app, area, &mut buf);
            buf
        };
        let at = |buf: &Buffer, text: &str| {
            (0..area.height)
                .find_map(|y| {
                    let line: String = (0..area.width).map(|x| buf[(x, y)].symbol()).collect();
                    line.find(text)
                        .map(|i| (line[..i].chars().count() as u16, y))
                })
                .unwrap_or_else(|| panic!("{text} on screen"))
        };
        let mouse = |kind, (column, row): (u16, u16)| MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        };
        let click = |at| mouse(MouseEventKind::Down(MouseButton::Left), at);
        let on = |pump: &datui::event_pump::EventPump| {
            pump.app
                .home
                .selected_entry()
                .and_then(|e| e.path.file_name().map(|n| n.to_string_lossy().into_owned()))
        };

        let buf = draw(&mut pump.app);
        let f05 = at(&buf, "f05.csv");
        assert!(pump.terminal_mouse(click(f05)).unwrap());
        assert_eq!(on(&pump).as_deref(), Some("f05.csv"), "{width}x{height}");

        draw(&mut pump.app);
        let wheel = |kind| mouse(kind, (2, 2));
        pump.terminal_mouse(wheel(MouseEventKind::ScrollDown))
            .unwrap();
        assert_eq!(on(&pump).as_deref(), Some("f08.csv"));
        let last = pump.app.home.visible().len() - 1;
        for _ in 0..last {
            pump.terminal_mouse(wheel(MouseEventKind::ScrollDown))
                .unwrap();
        }
        assert_eq!(pump.app.home.selected, last, "stops at the last");
        // The table's sideways wheel means nothing here.
        pump.terminal_mouse(wheel(MouseEventKind::ScrollRight))
            .unwrap();
        assert_eq!(pump.app.home.selected, last);
        pump.terminal_mouse(wheel(MouseEventKind::ScrollUp))
            .unwrap();
        assert_eq!(pump.app.home.selected, last - 3);

        let buf = draw(&mut pump.app);
        let f57 = at(&buf, "f57.csv");
        // On f05's cell, this click would finish a double click.
        assert_ne!(f57, f05, "{width}x{height}");
        pump.terminal_mouse(click(f57)).unwrap();
        assert_eq!(
            pump.app.input_mode,
            datui::InputMode::Home,
            "one click selects"
        );
        pump.terminal_mouse(click(f57)).unwrap();
        pump.drain().unwrap();
        assert_ne!(
            pump.app.input_mode,
            datui::InputMode::Home,
            "a double click opens f57.csv"
        );
    }
}

/// Up from the bottom moves the cursor up the screen; the list scrolls only once the
/// cursor is two lines from the top (#551).
#[test]
fn test_up_from_the_bottom_moves_the_cursor_not_the_list() {
    use crossterm::event::KeyCode;
    let (_tmp, mut app, _rx) = home_at_80x24(60);
    let first = press_and_draw(&mut app, KeyCode::Home);
    let top = first - app.home.selected as u16;
    let bottom = press_and_draw(&mut app, KeyCode::End);
    assert!(app.home.scroll > 0, "sixty files are more than a screen");
    let height = bottom - top + 1;
    assert!(height > 10, "list is {height} lines");

    let scroll = app.home.scroll;
    for step in 1..=(height - 3) {
        let line = press_and_draw(&mut app, KeyCode::Up);
        assert_eq!(line, bottom - step, "after {step} up");
        assert_eq!(app.home.scroll, scroll, "the list holds still");
    }
    assert_eq!(bottom - (height - 3), top + 2, "two lines of margin");
    // From here the list scrolls under a cursor that stays two lines down.
    for step in 1..=5 {
        let line = press_and_draw(&mut app, KeyCode::Up);
        assert_eq!(line, top + 2, "after {step} more up");
        assert_eq!(app.home.scroll, scroll - step as usize);
    }
    // Down again moves the cursor, not the list.
    let line = press_and_draw(&mut app, KeyCode::Down);
    assert_eq!(line, top + 3);
    assert_eq!(app.home.scroll, scroll - 5);
}

/// Down from the top moves the cursor down the screen until it is two lines from the
/// bottom, then the list scrolls; Up then moves the cursor back up the screen.
#[test]
fn test_down_from_the_top_moves_the_cursor_until_the_margin() {
    use crossterm::event::KeyCode;
    let (_tmp, mut app, _rx) = home_at_80x24(60);
    let first = press_and_draw(&mut app, KeyCode::Home);
    let top = first - app.home.selected as u16;
    let bottom = press_and_draw(&mut app, KeyCode::End);
    let height = bottom - top + 1;
    let first = press_and_draw(&mut app, KeyCode::Home);
    assert_eq!(app.home.scroll, 0);

    let mut line = first;
    while line < bottom - 2 {
        let next = press_and_draw(&mut app, KeyCode::Down);
        assert_eq!(next, line + 1);
        assert_eq!(app.home.scroll, 0, "the list holds still");
        line = next;
    }
    for step in 1..=5 {
        assert_eq!(press_and_draw(&mut app, KeyCode::Down), bottom - 2);
        assert_eq!(app.home.scroll, step);
    }
    let scroll = app.home.scroll;
    for step in 1..=(height - 5) {
        assert_eq!(press_and_draw(&mut app, KeyCode::Up), bottom - 2 - step);
        assert_eq!(app.home.scroll, scroll, "the list holds still");
    }
}

/// A page moves the cursor a screenful; the list scrolls only as far as keeps it in
/// view, and paging back does the same from the other edge.
#[test]
fn test_paging_scrolls_only_as_far_as_the_cursor_needs() {
    use crossterm::event::KeyCode;
    let (_tmp, mut app, _rx) = home_at_80x24(60);
    let first = press_and_draw(&mut app, KeyCode::Home);
    let top = first - app.home.selected as u16;
    let bottom = press_and_draw(&mut app, KeyCode::End);
    press_and_draw(&mut app, KeyCode::Home);

    assert_eq!(press_and_draw(&mut app, KeyCode::PageDown), bottom - 2);
    let scroll = app.home.scroll;
    assert!(scroll > 0);
    // Back up the screen without scrolling, then a page up scrolls to keep the margin.
    assert_eq!(press_and_draw(&mut app, KeyCode::Up), bottom - 3);
    assert_eq!(app.home.scroll, scroll);
    let line = press_and_draw(&mut app, KeyCode::PageUp);
    assert_eq!(app.home.scroll, 0);
    assert_eq!(line, top + app.home.selected as u16);
}

/// Rows arriving above the cursor push the list down, not the cursor: it stays on
/// its row and on its line.
#[test]
fn test_rows_arriving_above_the_cursor_leave_it_on_its_line() {
    use crossterm::event::KeyCode;
    let (tmp, mut app, rx) = home_at_80x24(60);
    // Up from the end, so the cursor is mid-screen rather than held at a margin.
    press_and_draw(&mut app, KeyCode::End);
    let f50 = tmp.path().join("f50.csv");
    let mut line = 0;
    while app.home.selected_entry().map(|e| e.path) != Some(f50.clone()) {
        line = press_and_draw(&mut app, KeyCode::Up);
    }
    for i in 0..5 {
        touch(tmp.path(), &format!("e{i}.csv"));
    }
    let _ = app.event(&datui::AppEvent::Key(crossterm::event::KeyEvent::new(
        KeyCode::Char('r'),
        crossterm::event::KeyModifiers::CONTROL,
    )));
    listed(&mut app, &rx, |app| {
        visible_names(&app.home).contains(&"e4.csv".to_string())
    });
    assert_eq!(app.home.selected_entry().map(|e| e.path), Some(f50));
    assert_eq!(cursor_line(&mut app), line);
}

/// The view does not open past the end: rows going away below the cursor bring the
/// view up rather than leaving blank lines under the last row.
#[test]
fn test_the_view_never_leaves_blank_lines_below_the_last_row() {
    use crossterm::event::KeyCode;
    let (tmp, mut app, rx) = home_at_80x24(60);
    let bottom = press_and_draw(&mut app, KeyCode::End);
    let f44 = tmp.path().join("f44.csv");
    let mut before = bottom;
    while app.home.selected_entry().map(|e| e.path) != Some(f44.clone()) {
        before = press_and_draw(&mut app, KeyCode::Up);
    }
    for i in 45..60 {
        fs::remove_file(tmp.path().join(format!("f{i:02}.csv"))).unwrap();
    }
    let _ = app.event(&datui::AppEvent::Key(crossterm::event::KeyEvent::new(
        KeyCode::Char('r'),
        crossterm::event::KeyModifiers::CONTROL,
    )));
    // The reload lists in the background; wait for the rows to go.
    listed(&mut app, &rx, |app| {
        !visible_names(&app.home).contains(&"f59.csv".to_string())
    });
    let line = cursor_line(&mut app);
    assert_eq!(app.home.selected_entry().map(|e| e.path), Some(f44));
    assert!(line > before, "the view came up: line {line}, was {before}");
    let rows = app.home.visible().len();
    assert_eq!(
        line as usize + (rows - 1 - app.home.selected),
        bottom as usize,
        "the last row is on the bottom line"
    );
}

// ---------------------------------------------------------------------------
// Landing: the open-all row says what it opens, and the cursor lands on it only when
// that is one dataset
// ---------------------------------------------------------------------------

mod landing {
    use super::coming_back::{draw, go_into, home_app, press, select, settle};
    use super::touch;
    use crossterm::event::KeyCode;
    use datui::discover::EntryKind;
    use datui::{App, AppEvent};
    use polars::prelude::*;
    use std::path::Path;
    use std::sync::mpsc::Receiver;
    use tempfile::TempDir;

    fn parquet(path: &Path, columns: &[(&str, &[i64])]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let height = columns[0].1.len();
        let columns: Vec<Column> = columns
            .iter()
            .map(|(name, values)| Column::new((*name).into(), *values))
            .collect();
        let mut frame = DataFrame::new(height, columns).unwrap();
        let file = std::fs::File::create(path).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
    }

    fn csv(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    /// One directory of each kind the issue names (#547).
    fn fixtures(root: &Path) {
        for year in [2023, 2024] {
            for month in [1, 2] {
                parquet(
                    &root.join(format!("sales_hive/year={year}/month={month}/part.parquet")),
                    &[("id", &[1, 2, 3]), ("amount", &[10, 20, 30])],
                );
            }
        }
        for name in ["a", "b", "c"] {
            parquet(
                &root.join(format!("same_schema/{name}.parquet")),
                &[("id", &[1, 2]), ("v", &[3, 4])],
            );
        }
        parquet(&root.join("diff_schema/a.parquet"), &[("id", &[1])]);
        parquet(
            &root.join("diff_schema/b.parquet"),
            &[("name", &[1]), ("z", &[2])],
        );
        parquet(&root.join("mixed/a.parquet"), &[("id", &[1, 2])]);
        csv(&root.join("mixed/b.csv"), "id\n3\n");
        parquet(&root.join("mixed/sub/c.parquet"), &[("id", &[5])]);
        touch(
            &root.join("delta_tbl/_delta_log"),
            "00000000000000000000.json",
        );
        parquet(&root.join("delta_tbl/part-0.parquet"), &[("id", &[1, 2])]);
        // A directory of datasets: a hive table, two Parquet files, a directory of CSV.
        parquet(
            &root.join("data/events/day=1/part.parquet"),
            &[("id", &[1])],
        );
        parquet(&root.join("data/processed/a.parquet"), &[("id", &[1])]);
        parquet(&root.join("data/processed/b.parquet"), &[("id", &[2])]);
        csv(&root.join("data/raw/a.csv"), "id\n1\n");
        csv(&root.join("data/raw/b.csv"), "id\n2\n");
    }

    /// The home screen over `root`, with every row on it measured, as it is by the time
    /// anyone has read the listing.
    fn home_over(root: &Path) -> (App, Receiver<AppEvent>) {
        let mut config = datui::config::AppConfig::default();
        config.home.directories = vec![root.to_string_lossy().into_owned()];
        config.home.search.enabled = false;
        let (mut app, rx) = home_app(config);
        let measured = ["sales_hive", "same_schema", "diff_schema"].map(|d| root.join(d));
        // Not just listed in `enriched`: a look sends a row's kind before its footers,
        // and a door named from that record alone lists only the keys on screen. Both
        // passes done, every record is the measured one.
        settle(&mut app, &rx, |app| {
            !app.home.measure_in_flight
                && !app.home.classify_in_flight
                && measured.iter().all(|d| app.home.enriched.contains_key(d))
        });
        (app, rx)
    }

    fn door(app: &App) -> datui::discover::Entry {
        app.home
            .sections
            .iter()
            .find_map(|s| s.door.clone())
            .expect("the directory carries the open-all row")
    }

    fn step_in(app: &mut App, rx: &Receiver<AppEvent>, dir: &Path) {
        select(app, dir);
        go_into(app, rx, KeyCode::Right, dir);
    }

    fn row_kind(app: &App, path: &Path) -> EntryKind {
        app.home
            .sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .find(|r| r.path == path)
            .map(|r| r.kind)
            .expect("the row is listed")
    }

    /// Open what Enter on the highlighted row opens, and wait for it.
    fn enter_and_load(app: &mut App, rx: &Receiver<AppEvent>) {
        match press(app, KeyCode::Enter) {
            Some(AppEvent::Open(paths, options)) => {
                crate::common::pump_open_until_loaded(app, rx, paths, options)
            }
            _ => panic!("Enter should open the directory"),
        }
        assert!(app.data_table_state.is_some(), "the directory opened");
    }

    #[test]
    fn a_hive_table_lands_on_its_row_and_opens_with_its_partition_columns() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let hive = tmp.path().join("sales_hive");
        let (mut app, rx) = home_over(tmp.path());
        step_in(&mut app, &rx, &hive);

        let row = door(&app);
        assert_eq!(row.name, "sales_hive (hive table: year, month)");
        assert!(
            app.home.selection_is_the_door(),
            "one dataset: the cursor is on it"
        );
        // The width the open will have, partition columns counted.
        assert_eq!((row.rows, row.cols), (Some(12), Some(4)));

        enter_and_load(&mut app, &rx);
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.headers()[..2], ["year", "month"]);
        // Partitions are pruned: with the 2024 files no longer Parquet, a filter on 2023
        // still reads, because it never opens them.
        for month in [1, 2] {
            std::fs::write(
                hive.join(format!("year=2024/month={month}/part.parquet")),
                b"x",
            )
            .unwrap();
        }
        let pruned = state
            .lf()
            .clone()
            .filter(col("year").eq(lit(2023)))
            .collect()
            .expect("the 2024 partition is never read");
        assert_eq!(pruned.height(), 6);
    }

    #[test]
    fn files_with_one_schema_land_on_their_row_and_open_as_one_table() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let same = tmp.path().join("same_schema");
        let (mut app, rx) = home_over(tmp.path());
        assert_eq!(row_kind(&app, &same), EntryKind::MultiFile);
        step_in(&mut app, &rx, &same);

        assert_eq!(door(&app).name, "same_schema (3 Parquet files, one schema)");
        assert!(app.home.selection_is_the_door());
        enter_and_load(&mut app, &rx);
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!((state.num_rows(), state.headers().len()), (6, 2));
    }

    /// The root row and the open-all row inside agree: the footers that made
    /// `diff_schema/` a place to look into upstairs make its row inside a union.
    #[test]
    fn files_whose_schemas_differ_land_on_the_first_file() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let diff = tmp.path().join("diff_schema");
        let (mut app, rx) = home_over(tmp.path());
        assert_eq!(row_kind(&app, &diff), EntryKind::Directory);
        step_in(&mut app, &rx, &diff);

        assert_eq!(
            door(&app).name,
            "diff_schema (2 Parquet files, schemas differ)"
        );
        assert!(!app.home.selection_is_the_door());
        assert_eq!(
            app.home.selected_entry().map(|e| e.path),
            Some(diff.join("a.parquet"))
        );
    }

    #[test]
    fn a_mixed_directory_lands_on_its_first_child_and_says_what_is_read() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let mixed = tmp.path().join("mixed");
        let (mut app, rx) = home_over(tmp.path());
        step_in(&mut app, &rx, &mixed);

        let row = door(&app);
        assert_eq!(row.name, "mixed (all files, mixed)");
        assert_eq!(
            datui::home::door_reads(&row),
            Some((
                "1 parquet".to_string(),
                Some("1 csv, 1 directory".to_string())
            ))
        );
        assert!(!app.home.selection_is_the_door());
        assert!(app.home.selected_entry().is_some(), "on a row inside");
    }

    #[test]
    fn a_directory_of_datasets_lands_on_its_first_child() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let data = tmp.path().join("data");
        let (mut app, rx) = home_over(tmp.path());
        step_in(&mut app, &rx, &data);

        let row = door(&app);
        assert_eq!(row.name, "data (all files, mixed)");
        assert_eq!(
            datui::home::door_reads(&row),
            Some(("every Parquet file below".to_string(), None))
        );
        assert!(!app.home.selection_is_the_door());
        let on = app.home.selected_entry().expect("a row inside").path;
        assert!(on.starts_with(&data) && on != data, "got {on:?}");
    }

    /// A lake table's files are not its rows, so its open-all row says so and the cursor
    /// starts on the first thing in it.
    #[test]
    fn a_lake_table_lands_on_its_first_child() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let delta = tmp.path().join("delta_tbl");
        let (mut app, rx) = home_over(tmp.path());
        select(&mut app, &delta);
        go_into(&mut app, &rx, KeyCode::Enter, &delta);

        assert_eq!(door(&app).name, "delta_tbl (Delta files, not the table)");
        assert!(!app.home.selection_is_the_door());
        assert_eq!(
            app.home.selected_entry().map(|e| e.path),
            Some(delta.join("part-0.parquet"))
        );
    }

    /// `~` and Enter on a directory goes inside it, as → does, rather than reading all of
    /// it as one table.
    #[test]
    fn a_directory_typed_at_the_path_prompt_is_browsed() {
        let tmp = TempDir::new().unwrap();
        fixtures(tmp.path());
        let (mut app, rx) = home_over(tmp.path());
        for (dir, lands) in [("same_schema", true), ("data", false)] {
            let dir = tmp.path().join(dir);
            press(&mut app, KeyCode::Char('~'));
            for c in dir.to_string_lossy().chars() {
                press(&mut app, KeyCode::Char(c));
            }
            go_into(&mut app, &rx, KeyCode::Enter, &dir);
            assert!(app.data_table_state.is_none(), "nothing was opened");
            assert_eq!(app.home.selection_is_the_door(), lands, "{dir:?}");
            press(&mut app, KeyCode::Esc);
            settle(&mut app, &rx, |app| app.home.browsing.is_none());
        }
        draw(&mut app);
    }
}

/// Footers read after the cursor landed on a door turn it down as one table: the cursor
/// goes to the first file, as it would have had they been read first. Once the user has
/// moved, the cursor stays where they put it.
#[cfg(feature = "cloud")]
#[test]
fn test_late_footers_move_a_landed_cursor_and_only_a_landed_one() {
    use datui::discover::{Entry, EntryKind};
    use datui::home::Measured;
    use std::path::PathBuf;

    let exports = PathBuf::from("gs://bucket/exports");
    let object = |name: &str| {
        let mut entry = Entry::directory(&exports.join(name));
        entry.name = name.to_string();
        entry.kind = EntryKind::File;
        entry.size = Some(1_000);
        entry
    };
    let fresh = || {
        let mut home = HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        home.probe_ready(
            exports.clone(),
            vec![
                object("a.parquet"),
                object("b.parquet"),
                object("c.parquet"),
            ],
        );
        home.browsing = Some(exports.clone());
        home.rebuild(&[], &[]);
        assert!(
            home.selection_is_the_door(),
            "the names alone say one table"
        );
        home
    };
    let turned_down = |home: &mut HomeState| {
        let door = door_of(home).unwrap().path.clone();
        home.enriched.insert(
            door,
            Measured {
                kind: Some(EntryKind::Directory),
                ..Default::default()
            },
        );
        home.apply_measurements();
        assert_eq!(
            door_of(home).map(|d| d.name.as_str()),
            Some("exports (3 Parquet files, schemas differ)")
        );
    };

    let mut home = fresh();
    turned_down(&mut home);
    assert!(!home.selection_is_the_door());
    assert_eq!(
        home.selected_entry().map(|e| e.path),
        Some(exports.join("a.parquet"))
    );

    // Down and back up: the user chose the door, and a late answer leaves it there.
    let mut home = fresh();
    home.move_selection(1);
    home.move_selection(-1);
    assert!(home.selection_is_the_door());
    turned_down(&mut home);
    assert!(home.selection_is_the_door(), "the user's own choice stays");
}

/// A prefix in an object store is scanned whole, so the pane does not claim its
/// subdirectories are skipped.
#[cfg(feature = "cloud")]
#[test]
fn test_a_mixed_prefix_says_it_reads_below() {
    use datui::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    let place = PathBuf::from("gs://bucket/mix");
    let row = |name: &str, kind: EntryKind| {
        let mut entry = Entry::directory(&place.join(name));
        entry.name = name.to_string();
        entry.kind = kind;
        entry.size = Some(1_000);
        entry
    };
    let mut home = HomeState {
        network_check: |_| true,
        ..Default::default()
    };
    home.probe_ready(
        place.clone(),
        vec![
            row("a.csv", EntryKind::File),
            row("b.csv", EntryKind::File),
            row("c.json", EntryKind::File),
            row("sub", EntryKind::Directory),
        ],
    );
    home.browsing = Some(place);
    home.rebuild(&[], &[]);
    let door = door_of(&home).expect("the prefix carries the door").clone();
    assert_eq!(door.name, "mix (all files, mixed)");
    assert_eq!(
        datui::home::door_reads(&door),
        Some((
            "every csv file below".to_string(),
            Some("1 json".to_string())
        ))
    );
    assert!(!home.selection_is_the_door());
}

/// A file a format spec's glob names is listed as data under the spec's name, from its
/// name alone; the other files stay as they were.
#[test]
fn a_file_a_spec_names_is_listed_under_the_spec() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("day.l2"), [0u8, 1, 2]).unwrap();
    std::fs::write(tmp.path().join("notes.xyz"), "text").unwrap();
    let spec = datui::formats::Spec::parse(
        "name = \"acme.l2feed\"\nmatch = { glob = \"*.l2\" }\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]",
        None,
    )
    .unwrap();
    let registry = datui::formats::Registry::of(vec![spec]);
    let mut rows = discover::scan_dir(tmp.path());
    datui::home::name_by_spec(&registry, &mut rows);
    let day = rows.iter().find(|r| r.name == "day.l2").unwrap();
    assert_eq!(day.kind, EntryKind::File);
    assert_eq!(day.label(), "acme.l2feed");
    let notes = rows.iter().find(|r| r.name == "notes.xyz").unwrap();
    assert_eq!(notes.kind, EntryKind::Other);
    assert_eq!(rows[0].name, "day.l2", "data sorts first");
}

/// A CSV a delimited spec's glob names keeps its place as data and gains the spec's
/// name; a binary spec's glob does not take a CSV.
#[test]
fn a_csv_a_delimited_spec_names_is_listed_under_the_spec() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("log_001.csv"), "a\n1\n").unwrap();
    std::fs::write(tmp.path().join("other.csv"), "a\n1\n").unwrap();
    let delimited = datui::formats::Spec::parse(
        "name = \"acme.instrument-log\"\nkind = \"delimited\"\nmatch = { glob = \"log_*.csv\" }",
        None,
    )
    .unwrap();
    let binary = datui::formats::Spec::parse(
        "name = \"acme.raw\"\nmatch = { glob = \"*.csv\" }\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]",
        None,
    )
    .unwrap();
    let registry = datui::formats::Registry::of(vec![binary, delimited]);
    let mut rows = discover::scan_dir(tmp.path());
    datui::home::name_by_spec(&registry, &mut rows);
    let log = rows.iter().find(|r| r.name == "log_001.csv").unwrap();
    assert_eq!(log.kind, EntryKind::File);
    assert_eq!(log.label(), "acme.instrument-log");
    let other = rows.iter().find(|r| r.name == "other.csv").unwrap();
    assert_eq!(other.format_spec, None);
}

/// A file row that is not read lazily where it is says how it is read, dim, beside
/// its name: `in memory`, `converts`. Lazy rows say nothing. At 80 columns the word
/// gives way before a long name is cut, and the details pane at 200 says it in full.
#[test]
fn test_a_file_row_says_how_it_will_be_read() {
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    common::isolate_cache();
    let tmp = TempDir::new().unwrap();
    for name in [
        "events.json",
        "log.csv.gz",
        "ride.gpx",
        "notes.csv",
        "sales.parquet",
        "stream.arrow",
        "a_json_export_with_a_long_descriptive_name.json",
    ] {
        touch(tmp.path(), name);
    }
    fs::write(tmp.path().join("file.arrow"), b"ARROW1\0\0").unwrap();
    let mut config = datui::config::AppConfig::default();
    config.home.directories = vec![tmp.path().to_string_lossy().into_owned()];
    config.home.desktop_recents = false;
    config.cloud.hide = ["s3-default", "gcs-default", "az", "azure-env"]
        .map(String::from)
        .to_vec();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = datui::App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    app.enter_home();
    // The stream is told from the file by its first bytes, which measuring reads.
    listed(&mut app, &rx, |app| {
        app.home.visible().iter().any(|r| match r {
            Row::Entry { entry, .. } => entry.name == "stream.arrow" && entry.cost.ipc_stream,
            _ => false,
        })
    });

    // Tall enough for the checkout's own directory, listed first as the current one.
    for (width, height) in [(80u16, 60u16), (200, 60)] {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        Widget::render(&mut app, area, &mut buf);
        let screen: Vec<String> = (0..height)
            .map(|y| (0..width).map(|x| buf[(x, y)].symbol()).collect())
            .collect();
        let row = |name: &str| {
            screen
                .iter()
                .find(|l| l.contains(name))
                .unwrap_or_else(|| panic!("{name} at {width}x{height}:\n{}", screen.join("\n")))
                .clone()
        };
        for (name, marker) in [
            ("events.json", Some("in memory")),
            ("log.csv.gz", Some("converts")),
            ("ride.gpx", Some("converts")),
            ("stream.arrow", Some("converts")),
            ("notes.csv", None),
            ("sales.parquet", None),
            ("file.arrow", None),
        ] {
            let line = row(name);
            // The list's half of the line: the pane beside it at 200 is not the row.
            let list: String = line.chars().take(width as usize * 5 / 8).collect();
            for word in ["in memory", "converts", "downloads"] {
                assert_eq!(
                    list.contains(word),
                    marker == Some(word),
                    "{name} at {width}x{height}: {line}"
                );
            }
        }
        let long = "a_json_export_with_a_long_descriptive_name";
        // The list's half again at 200: the pane beside it names the row under the cursor.
        let list_width = if width == 80 {
            80
        } else {
            width as usize * 5 / 8
        };
        let line: String = screen
            .iter()
            .map(|l| l.chars().take(list_width).collect::<String>())
            .find(|l| l.contains("a_json_export"))
            .expect("the long row");
        if width == 80 {
            assert!(
                !line.contains("in memory"),
                "the word gives way first at 80: {line}"
            );
        } else {
            assert!(
                line.contains(&format!("{long}.json")) && line.contains("in memory"),
                "{line}"
            );
        }
    }

    // The pane, which draws at 200, says it in words for the row under the cursor.
    for c in "events".chars() {
        let _ = app.event(&datui::AppEvent::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char(c),
            crossterm::event::KeyModifiers::NONE,
        )));
    }
    let area = Rect::new(0, 0, 200, 60);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    let screen: Vec<String> = (0..60)
        .map(|y| (0..200).map(|x| buf[(x, y)].symbol()).collect())
        .collect();
    assert!(
        screen.iter().any(|l| l
            .split_whitespace()
            .collect::<Vec<_>>()
            .ends_with(&["read", "in", "memory"])),
        "the pane's read line:\n{}",
        screen.join("\n")
    );
}

// ---------------------------------------------------------------------------
// The ROWS preview: the first rows of the selected file, read once (#547 M4, M8)
// ---------------------------------------------------------------------------

mod first_rows {
    use super::coming_back::{draw, go_into, home_app, press, select, settle};
    use crossterm::event::KeyCode;
    use datui::AppEvent;
    use datui::home_preview::Stamp;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::Receiver;
    use tempfile::TempDir;

    /// `people.csv` of `rows` rows, in a directory of its own under `dir`.
    fn people(dir: &Path, rows: usize) -> PathBuf {
        let mut text = String::from("id,name,score\n");
        for i in 0..rows {
            text.push_str(&format!("{i},person_{i:03},{}.5\n", i * 3));
        }
        let path = dir.join("small").join("people.csv");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
        path
    }

    fn config(dir: &Path) -> datui::config::AppConfig {
        let mut config = datui::config::AppConfig::default();
        config.home.directories = vec![dir.to_string_lossy().into_owned()];
        config
    }

    fn render(app: &mut datui::App, w: u16, h: u16) -> Vec<String> {
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        Widget::render(&mut *app, area, &mut buf);
        (0..h)
            .map(|y| (0..w).map(|x| buf[(x, y)].symbol()).collect())
            .collect()
    }

    /// Draw at `w`×`h` until the selected file's preview has landed.
    fn wait_for_rows(app: &mut datui::App, rx: &Receiver<AppEvent>, w: u16, h: u16) {
        let entry = app.home.selected_entry().expect("a row selected");
        let stamp = Stamp::of_entry(&entry);
        render(app, w, h);
        settle(app, rx, |app| {
            app.home_previews
                .rows(&entry.path, stamp)
                .is_some_and(|rows| rows.is_some())
        });
    }

    /// Into `small/`, where the file is the only row and the list leaves rows free.
    fn into_small(tmp: &Path) -> (datui::App, Receiver<AppEvent>, PathBuf) {
        let file = people(tmp, 40);
        let (mut app, rx) = home_app(config(tmp));
        select(&mut app, &tmp.join("small"));
        go_into(&mut app, &rx, KeyCode::Right, &tmp.join("small"));
        select(&mut app, &file);
        (app, rx, file)
    }

    fn open(app: &mut datui::App, rx: &Receiver<AppEvent>) -> bool {
        let Some(AppEvent::Open(paths, options)) = press(app, KeyCode::Enter) else {
            panic!("Enter on a file opens it");
        };
        let prepared = options.prepared.is_some();
        crate::common::pump_open_until_loaded(app, rx, paths, options);
        prepared
    }

    fn first_cell(app: &datui::App, column: &str) -> String {
        let df = app
            .data_table_state
            .as_ref()
            .and_then(|state| state.display_df())
            .expect("rows on screen");
        df.column(column).unwrap().get(0).unwrap().to_string()
    }

    /// The pane shows the file's first rows, and Enter opens the dataset that read
    /// built: no scan and no page of its own. Read once, shown twice.
    #[test]
    fn a_previewed_file_opens_on_the_page_its_preview_read() {
        let tmp = TempDir::new().unwrap();
        let (mut app, rx, file) = into_small(tmp.path());
        wait_for_rows(&mut app, &rx, 200, 50);
        let screen = render(&mut app, 200, 50).join("\n");
        assert!(screen.contains("ROWS"), "{screen}");
        assert!(screen.contains("person_000"), "real values: {screen}");
        assert_eq!(app.reads.previews, 1);
        // Drawn again, at another size too, it is not read again.
        render(&mut app, 80, 24);
        render(&mut app, 200, 50);
        assert_eq!(app.reads.previews, 1);

        let read = app.reads;
        assert!(open(&mut app, &rx), "the open takes what the preview built");
        assert_eq!(app.reads, read, "the open read nothing of {file:?} again");
        assert_eq!(first_cell(&app, "name"), "\"person_000\"");
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(
            state.num_rows_if_valid(),
            Some(40),
            "the page held them all"
        );
    }

    /// Without a preview, the same open scans and reads its page: the counter above
    /// is one that moves.
    #[test]
    fn a_file_with_no_preview_is_read_by_its_open() {
        let tmp = TempDir::new().unwrap();
        let file = people(tmp.path(), 40);
        let mut config = config(tmp.path());
        config.home.preview_max = datui::config::ByteSize(0);
        let (mut app, rx) = home_app(config);
        select(&mut app, &tmp.path().join("small"));
        go_into(&mut app, &rx, KeyCode::Right, &tmp.path().join("small"));
        select(&mut app, &file);
        render(&mut app, 200, 50);
        let before = app.reads;
        assert!(!open(&mut app, &rx));
        assert_eq!(app.reads.previews, before.previews, "nothing previewed");
        assert_eq!(app.reads.scans, before.scans + 1);
        assert!(app.reads.pages > before.pages);
        assert_eq!(first_cell(&app, "name"), "\"person_000\"");
    }

    /// A file changed since its preview is opened as it is now, not as it was.
    #[test]
    fn a_file_changed_since_its_preview_is_read_again() {
        let tmp = TempDir::new().unwrap();
        let (mut app, rx, file) = into_small(tmp.path());
        wait_for_rows(&mut app, &rx, 200, 50);
        std::fs::write(&file, "id,name,score\n7,changed,1.0\n").unwrap();
        let before = app.reads;
        assert!(!open(&mut app, &rx), "the old page is not installed");
        assert_eq!(app.reads.scans, before.scans + 1);
        assert_eq!(first_cell(&app, "name"), "\"changed\"");
    }

    /// Parquet's first page too, and its columns in the pane from the same read.
    #[test]
    fn a_parquet_file_previews_its_first_page() {
        let tmp = TempDir::new().unwrap();
        crate::common::ensure_sample_data();
        let dir = tmp.path().join("small");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("people.parquet");
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sample-data/people.parquet"),
            &file,
        )
        .unwrap();
        let (mut app, rx) = home_app(config(tmp.path()));
        select(&mut app, &dir);
        go_into(&mut app, &rx, KeyCode::Right, &dir);
        select(&mut app, &file);
        wait_for_rows(&mut app, &rx, 200, 50);
        let screen = render(&mut app, 200, 50).join("\n");
        assert!(screen.contains("ROWS"), "{screen}");
        let read = app.reads;
        assert!(open(&mut app, &rx));
        assert_eq!(app.reads, read, "nothing read again");
    }

    /// Below the pane's width the rows take the lines the list leaves free, at the
    /// bottom of the screen; a list with none free gets no strip, and nothing is read
    /// for one.
    #[test]
    fn the_rows_strip_at_80_by_24() {
        let tmp = TempDir::new().unwrap();
        let (mut app, rx, _file) = into_small(tmp.path());
        wait_for_rows(&mut app, &rx, 80, 24);
        let screen = render(&mut app, 80, 24);
        let heading = screen
            .iter()
            .position(|line| line.trim_start().starts_with("ROWS"))
            .unwrap_or_else(|| panic!("a strip: {screen:#?}"));
        assert!(screen[heading + 1].contains("id") && screen[heading + 1].contains("name"));
        assert!(screen[heading + 2].contains("person_000"), "{screen:#?}");
        // Its last row sits on the line above the control bar.
        assert!(screen[22].contains("person_"), "{screen:#?}");
        // The list is above it, whole.
        assert!(screen[..heading].iter().any(|l| l.contains("people.csv")));

        // A directory that fills the screen leaves no room: no strip, no read.
        let full = TempDir::new().unwrap();
        for i in 0..40 {
            super::touch(full.path(), &format!("f{i:02}.csv"));
        }
        let (mut app, rx) = home_app(config(full.path()));
        settle(&mut app, &rx, |_| true);
        select(&mut app, &full.path().join("f00.csv"));
        let before = app.reads;
        let screen = render(&mut app, 80, 24);
        assert!(!screen.iter().any(|l| l.contains("ROWS")), "{screen:#?}");
        assert_eq!(app.reads.previews, before.previews);
        draw(&mut app);
    }

    /// At 200×50 the list keeps a reading measure: a row's size sits near its name,
    /// and the pane takes the rest of the width (#547 M8, D13).
    #[test]
    fn the_list_keeps_its_measure_at_200_by_50() {
        let tmp = TempDir::new().unwrap();
        let (mut app, rx, _file) = into_small(tmp.path());
        wait_for_rows(&mut app, &rx, 200, 50);
        let screen = render(&mut app, 200, 50);
        let row = screen
            .iter()
            .find(|line| line.contains("people.csv") && line.contains(" now"))
            .unwrap_or_else(|| panic!("the file's row: {screen:#?}"));
        let chars: Vec<char> = row.chars().collect();
        let name_end = row
            .find("people.csv")
            .map(|i| row[..i].chars().count() + 10)
            .unwrap();
        let size_at = row
            .find(" B ")
            .or_else(|| row.find(" KB "))
            .map(|i| row[..i].chars().count())
            .unwrap_or_else(|| panic!("the size on the row: {row:?}"));
        assert!(
            size_at - name_end <= 60,
            "size {size_at} is {} columns from the name: {row:?}",
            size_at - name_end
        );
        assert!(chars.len() == 200);
        // The pane starts where the list ends and shows the rows.
        let rows_at = screen
            .iter()
            .find_map(|line| line.find("ROWS").map(|i| line[..i].chars().count()))
            .expect("ROWS in the pane");
        assert!(rows_at < 100, "the pane takes the width: ROWS at {rows_at}");
    }
}

// ---------------------------------------------------------------------------
// The public catalog: rows that say what they are, one key away (#547 M5, D12)
// ---------------------------------------------------------------------------

mod catalog {
    use super::coming_back::{press, settle};
    use crossterm::event::KeyCode;
    use datui::home::Row;
    use datui::{App, AppEvent, UnaskedDownload};
    use std::sync::mpsc::Receiver;
    use tempfile::TempDir;

    fn app_with_catalog(config: datui::config::AppConfig) -> (App, Receiver<AppEvent>, TempDir) {
        let mut config = config;
        config.home.desktop_recents = false;
        config.cloud.discover = Some(datui::config::CloudDiscover::None);
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new_with_config(
            tx,
            crate::common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        );
        let cache = TempDir::new().unwrap();
        app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
        app.enter_home();
        settle(&mut app, &rx, |_| true);
        (app, rx, cache)
    }

    fn select_named(app: &mut App, name: &str) {
        let index = app
            .home
            .visible()
            .iter()
            .position(|row| matches!(row, Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("a row named {name}"));
        app.home.selected = index;
    }

    fn screen(app: &mut App, w: u16, h: u16) -> Vec<String> {
        use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        Widget::render(&mut *app, area, &mut buf);
        (0..h)
            .map(|y| (0..w).map(|x| buf[(x, y)].symbol()).collect())
            .collect()
    }

    /// A built-in web file's row says its format and what it weighs before anything is
    /// fetched, at 80 and at 200 columns.
    #[test]
    fn catalog_rows_say_their_format_and_size() {
        let (mut app, _rx, _cache) = app_with_catalog(datui::config::AppConfig::default());
        for (w, h) in [(80, 24), (200, 50)] {
            // Folded sections ahead of it would push it off a short screen.
            select_named(&mut app, "Palmer penguins");
            let lines = screen(&mut app, w, h);
            let row = lines
                .iter()
                .find(|line| line.find("Palmer penguins").is_some_and(|at| at < 12))
                .unwrap_or_else(|| panic!("{w}x{h}: {lines:#?}"));
            assert!(row.contains("Palmer penguins  csv"), "{w}x{h}: {row:?}");
            assert!(row.contains("16.1 KB"), "{w}x{h}: {row:?}");
        }
    }

    /// Enter on a small built-in web file asks for its download without a question;
    /// the same URL typed at `~` keeps the question.
    #[test]
    fn a_small_builtin_file_opens_without_a_question_and_a_typed_url_asks() {
        let (mut app, _rx, _cache) = app_with_catalog(datui::config::AppConfig::default());
        select_named(&mut app, "Palmer penguins");
        let Some(AppEvent::Open(paths, options)) = press(&mut app, KeyCode::Enter) else {
            panic!("Enter opens it");
        };
        let unasked = options.download_unasked.expect("downloaded unasked");
        assert_eq!(unasked.limit, UnaskedDownload::LIMIT);
        assert!(unasked.covers(None), "its listed size is under the limit");
        let url = paths[0].to_string_lossy().into_owned();

        let (mut app, _rx, _cache) = app_with_catalog(datui::config::AppConfig::default());
        press(&mut app, KeyCode::Char('~'));
        for c in url.chars() {
            press(&mut app, KeyCode::Char(c));
        }
        let Some(AppEvent::Open(_, options)) = press(&mut app, KeyCode::Enter) else {
            panic!("Enter at ~ opens the URL");
        };
        assert_eq!(options.download_unasked, None, "a typed URL is asked about");
    }

    /// On screen, a local directory of directories, a hive table and a public dataset
    /// directory read in one grammar: `name/  label` (#547 M7).
    #[test]
    fn rows_read_name_slash_two_spaces_label_on_screen() {
        let tmp = TempDir::new().unwrap();
        for sub in ["a", "b", "c"] {
            super::touch(&tmp.path().join("data").join(sub), "x.csv");
        }
        super::touch(tmp.path(), "events/year=2024/part-0.parquet");
        super::touch(tmp.path(), "events/year=2025/part-0.parquet");
        let mut config = datui::config::AppConfig::default();
        config.home.directories = vec![tmp.path().to_string_lossy().into_owned()];
        let (mut app, rx, _cache) = app_with_catalog(config);
        let data = tmp.path().join("data");
        let events = tmp.path().join("events");
        // Both labels arrive from their own measures; wait for each.
        let labelled = |app: &App| {
            let rows = app.home.visible();
            rows.iter().any(|row| {
                matches!(row, Row::Entry { entry, .. }
                    if entry.path == data && entry.holds.directories == 3)
            }) && rows.iter().any(|row| {
                matches!(row, Row::Entry { entry, .. }
                    if entry.path == events && entry.kind == datui::discover::EntryKind::Hive)
            })
        };
        settle(&mut app, &rx, labelled);
        // Each row selected first, so it is on screen whatever the current directory
        // lists above it.
        for (w, h) in [(80, 24), (200, 50)] {
            for (name, expect) in [
                ("data", "data/  3 dirs"),
                ("events", "events/  hive"),
                (
                    "NOAA daily weather (GHCN-D)",
                    "NOAA daily weather (GHCN-D)/  dataset",
                ),
            ] {
                select_named(&mut app, name);
                let lines = screen(&mut app, w, h).join("\n");
                assert!(lines.contains(expect), "{w}x{h}: {expect:?} in\n{lines}");
            }
        }
    }

    /// A recent opened from a collection is named as the collection names it, with the
    /// format its name no longer says (#547 D12).
    #[test]
    fn a_recent_from_a_collection_keeps_its_name() {
        use datui::home::{Collection, CollectionDataset, ListingRequest, build_listing};
        let url = std::path::PathBuf::from("https://example.com/data/penguins.csv");
        let listing = build_listing(&ListingRequest {
            recents: vec![url.clone()],
            collections: vec![Collection {
                name: "public".to_string(),
                label: "Public datasets".to_string(),
                builtin: true,
                datasets: vec![CollectionDataset {
                    name: "Palmer penguins".to_string(),
                    location: url.clone(),
                    details: Vec::new(),
                    size: Some(16_480),
                }],
            }],
            config_dirs: Vec::new(),
            remembered_dirs: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: None,
            probed: Default::default(),
            unreachable: Default::default(),
            listing_so_far: Default::default(),
            cut_short: Default::default(),
            probe_errors: Default::default(),
            network_check: |_| true,
            cloud: Vec::new(),
            known: Default::default(),
            formats: Default::default(),
        });
        let recent = listing
            .sections
            .iter()
            .find(|s| s.title == datui::home::HomeState::RECENT_SECTION)
            .expect("a Recent section");
        let row = recent.rows.iter().find(|r| r.path == url).unwrap();
        assert_eq!(row.name, "Palmer penguins");
        assert_eq!(row.label(), "csv");
        assert_eq!(row.size, Some(16_480));
    }
}

// ---------------------------------------------------------------------------
// Frecency: what is opened often comes first (#547 M9)
// ---------------------------------------------------------------------------

mod frecency {
    use super::coming_back::settle;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use datui::home::Row;
    use datui::{App, AppEvent, CacheManager};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Two sales files in a configured directory, `often` opened three times and
    /// `last` once since, and the home screen over them with that history.
    fn opened(
        tmp: &TempDir,
        often: &str,
        last: &str,
    ) -> (App, std::sync::mpsc::Receiver<AppEvent>, PathBuf, PathBuf) {
        let dir = tmp.path().join("data");
        let often = super::touch(&dir, often);
        let last = super::touch(&dir, last);
        let cache = CacheManager::with_dir(tmp.path().join("cache"));
        for path in [&often, &often, &often, &last] {
            assert_eq!(
                cache.push_recent(path),
                datui::cache::HistoryUpdate::Written
            );
        }
        let mut config = datui::config::AppConfig::default();
        config.home.directories = vec![dir.to_string_lossy().into_owned()];
        config.home.desktop_recents = false;
        config.home.hide = vec!["public".to_string()];
        config.cloud.discover = Some(datui::config::CloudDiscover::None);
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new_with_config(
            tx,
            crate::common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        );
        app.use_cache(cache);
        app.enter_home();
        settle(&mut app, &rx, |app| !app.home.newest_recent.is_none());
        let canonical = |p: &PathBuf| std::fs::canonicalize(p).unwrap();
        (app, rx, canonical(&often), canonical(&last))
    }

    fn recent_order(app: &App) -> Vec<PathBuf> {
        app.home
            .visible()
            .iter()
            .filter_map(|row| match row {
                Row::Entry { section, entry, .. }
                    if app.home.sections[*section].title
                        == datui::home::HomeState::RECENT_SECTION =>
                {
                    Some(entry.path.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Recent is ranked by frecency, and the cursor still lands on the file opened
    /// last, so it is one Enter away.
    #[test]
    fn recent_is_ranked_by_frecency_and_lands_on_the_newest() {
        let tmp = TempDir::new().unwrap();
        let (mut app, _rx, often, last) = opened(&tmp, "sales_q1.csv", "sales_q2.csv");
        assert_eq!(recent_order(&app), [often, last.clone()]);
        app.home.select_first_entry();
        assert_eq!(app.home.selected_entry().map(|e| e.path), Some(last));
    }

    /// Of two files `sales` matches equally, the one opened most is first, in a
    /// directory's section too, where the name would otherwise put the other first.
    #[test]
    fn a_match_opened_most_comes_first() {
        let tmp = TempDir::new().unwrap();
        let (mut app, _rx, often, last) = opened(&tmp, "sales_q2.csv", "sales_q1.csv");
        for c in "sales".chars() {
            app.event(&AppEvent::Key(KeyEvent::new(
                KeyCode::Char(c),
                KeyModifiers::NONE,
            )));
        }
        let dir = often.parent().unwrap().to_path_buf();
        let in_dir: Vec<PathBuf> = app
            .home
            .visible()
            .iter()
            .filter_map(|row| match row {
                Row::Entry { section, entry, .. }
                    if app.home.sections[*section].root.as_deref() == Some(dir.as_path()) =>
                {
                    Some(entry.path.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(in_dir, [often, last]);
    }

    /// Visits are counted per open and kept only for what is still recent.
    #[test]
    fn visits_are_counted_and_follow_the_recents() {
        let tmp = TempDir::new().unwrap();
        let cache = CacheManager::with_dir(tmp.path().join("cache"));
        let a = super::touch(tmp.path(), "a.csv");
        let b = super::touch(tmp.path(), "b.csv");
        for path in [&a, &b, &a] {
            cache.push_recent(path);
        }
        let visits = cache.load_visits();
        let a = std::fs::canonicalize(&a).unwrap();
        let b = std::fs::canonicalize(&b).unwrap();
        assert_eq!(visits[&a].count, 2);
        assert_eq!(visits[&b].count, 1);
        let ranked = datui::cache::by_frecency(cache.load_recents(), &visits);
        assert_eq!(ranked, [a.clone(), b.clone()]);
        cache.forget_recent(&b);
        cache.push_recent(&a);
        assert!(
            !cache.load_visits().contains_key(&b),
            "forgotten, then pruned"
        );
    }
}

// ---------------------------------------------------------------------------
// The `~` prompt drives the list (#547 M6)
// ---------------------------------------------------------------------------

mod path_prompt {
    use super::coming_back::{home_app, press};
    use crossterm::event::KeyCode;
    use datui::{App, AppEvent};
    use std::sync::mpsc::Receiver;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn type_text(app: &mut App, text: &str) {
        for c in text.chars() {
            press(app, KeyCode::Char(c));
        }
    }

    /// Handle events until the typed directory is listed under the prompt.
    fn listed(app: &mut App, rx: &Receiver<AppEvent>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let dir = datui::home::typed_dir(&app.home.path_input).to_string();
            if app.home.path_listing.as_ref().is_some_and(|l| l.dir == dir) {
                return;
            }
            assert!(Instant::now() < deadline, "{dir} was never listed");
            if let Ok(event) = rx.recv_timeout(Duration::from_millis(20)) {
                let mut next = Some(event);
                while let Some(event) = next {
                    next = app.event(&event);
                }
            }
        }
    }

    fn screen(app: &mut App, w: u16, h: u16) -> Vec<String> {
        use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        Widget::render(&mut *app, area, &mut buf);
        (0..h)
            .map(|y| (0..w).map(|x| buf[(x, y)].symbol()).collect())
            .collect()
    }

    fn project() -> TempDir {
        let tmp = TempDir::new().unwrap();
        super::touch(tmp.path(), "summary.csv");
        super::touch(tmp.path(), "src/main.py");
        super::touch(tmp.path(), "data/a.csv");
        tmp
    }

    /// While `~` is typed the list is the directory being typed, filtered by the last
    /// segment; an ambiguous Tab completes nothing and leaves the candidates showing,
    /// ↑↓ picks one, and Enter takes it.
    #[test]
    fn the_list_is_the_typed_directory_and_arrows_pick() {
        let tmp = project();
        let (mut app, rx) = home_app(datui::config::AppConfig::default());
        press(&mut app, KeyCode::Char('~'));
        let typed = format!("{}/s", tmp.path().display());
        type_text(&mut app, &typed);
        listed(&mut app, &rx);
        let names: Vec<String> = app
            .home
            .path_candidates()
            .iter()
            .map(|n| n.name.clone())
            .collect();
        assert_eq!(names, ["src", "summary.csv"]);

        press(&mut app, KeyCode::Tab);
        assert_eq!(app.home.path_input, typed, "ambiguous: nothing added");
        for (w, h) in [(80, 24), (200, 50)] {
            let lines = screen(&mut app, w, h);
            assert!(lines.iter().any(|l| l.contains("src/")), "{lines:#?}");
            assert!(
                lines.iter().any(|l| l.contains("summary.csv")),
                "{lines:#?}"
            );
            let bar = &lines[h as usize - 1];
            assert!(
                bar.contains("Tab  Complete") && bar.contains("Pick"),
                "{bar}"
            );
        }

        press(&mut app, KeyCode::Down);
        press(&mut app, KeyCode::Down);
        assert_eq!(app.home.path_pick, Some(1));
        press(&mut app, KeyCode::Up);
        assert_eq!(
            app.home.picked_path(),
            Some(format!("{}/src/", tmp.path().display()))
        );
        // Enter goes into the picked directory, as it does for one typed.
        assert!(press(&mut app, KeyCode::Enter).is_none());
        assert!(!app.home.path_input_active);
        assert_eq!(
            app.home.browsing.as_deref(),
            Some(tmp.path().join("src").as_path())
        );
    }

    /// One candidate left: Tab completes it whole, a directory with its separator, so
    /// the next Tab is inside it.
    #[test]
    fn tab_completes_the_one_candidate() {
        let tmp = project();
        let (mut app, rx) = home_app(datui::config::AppConfig::default());
        press(&mut app, KeyCode::Char('~'));
        type_text(&mut app, &format!("{}/su", tmp.path().display()));
        listed(&mut app, &rx);
        press(&mut app, KeyCode::Tab);
        assert_eq!(
            app.home.path_input,
            format!("{}/summary.csv", tmp.path().display())
        );
        press(&mut app, KeyCode::Char('x'));
        for _ in 0.."summary.csvx".len() {
            press(&mut app, KeyCode::Backspace);
        }
        type_text(&mut app, "da");
        listed(&mut app, &rx);
        press(&mut app, KeyCode::Tab);
        assert_eq!(
            app.home.path_input,
            format!("{}/data/", tmp.path().display())
        );
        listed(&mut app, &rx);
        assert_eq!(
            app.home
                .path_candidates()
                .iter()
                .map(|n| n.name.as_str())
                .collect::<Vec<_>>(),
            ["a.csv"]
        );
    }

    /// A bucket completes from what datui already knows of it, the public catalog
    /// included, with nothing asked of the store: `s3://noaa` + Tab is the bucket.
    #[test]
    fn a_bucket_completes_from_what_is_known() {
        let mut config = datui::config::AppConfig::default();
        config.home.hide = Vec::new();
        let (tx, _rx) = std::sync::mpsc::channel();
        config.home.desktop_recents = false;
        config.cloud.discover = Some(datui::config::CloudDiscover::None);
        let mut app = App::new_with_config(
            tx,
            crate::common::test_runtime(),
            datui::Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        );
        let cache = TempDir::new().unwrap();
        app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
        app.enter_home();
        press(&mut app, KeyCode::Char('~'));
        type_text(&mut app, "s3://noaa");
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.home.path_input, "s3://noaa-ghcn-pds/");
        let names: Vec<String> = app
            .home
            .path_candidates()
            .iter()
            .map(|n| n.name.clone())
            .collect();
        assert_eq!(names, ["parquet"]);
    }
}

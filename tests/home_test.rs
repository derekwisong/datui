//! Home screen: dataset discovery, roots, and filtering.

use datui::discover::{self, EntryKind};
use datui::home::{fuzzy_score, HomeState, RootOrigin};
use std::fs;
use tempfile::TempDir;

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

    // And it appears as a single row, not a tree to walk.
    let entries = discover::scan_dir(tmp.path());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "sales");
    assert_eq!(entries[0].kind, EntryKind::Hive);
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
    // A folder holding a CSV and a spreadsheet is a folder, not a table.
    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "stuff/a.csv");
    touch(tmp.path(), "stuff/b.parquet");

    assert_eq!(
        discover::classify_directory(&tmp.path().join("stuff")),
        EntryKind::Directory
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
fn test_recent_dataset_implies_its_directory_is_a_root() {
    // The whole point: code lives in cwd, data lives on a mount. Opening something
    // there once must be enough for datui to know about the place.
    let tmp = TempDir::new().unwrap();
    let mount = tmp.path().join("mnt/data");
    let dataset = touch(&mount, "sales.parquet");

    let roots = HomeState::roots(&[], &[dataset], &[]);
    let derived = roots
        .iter()
        .find(|r| r.path == mount)
        .expect("the directory holding a recent dataset should become a root");
    assert_eq!(derived.origin, RootOrigin::Recent);
    assert!(derived.available);
}

#[test]
fn test_configured_directories_become_roots() {
    let tmp = TempDir::new().unwrap();
    let configured = tmp.path().join("datasets");
    fs::create_dir_all(&configured).unwrap();

    let roots = HomeState::roots(std::slice::from_ref(&configured), &[], &[]);
    assert_eq!(roots[0].path, configured);
    assert_eq!(roots[0].origin, RootOrigin::Configured);
}

#[test]
fn test_unavailable_root_is_reported_not_hidden() {
    // "The mount is down" is information; silently dropping the row is not.
    let missing = std::path::PathBuf::from("/definitely/not/here");
    let roots = HomeState::roots(std::slice::from_ref(&missing), &[], &[]);
    let root = roots.iter().find(|r| r.path == missing).expect("kept");
    assert!(!root.available);
}

#[test]
fn test_roots_are_deduplicated() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("data");
    let dataset = touch(&dir, "a.parquet");

    let roots = HomeState::roots(std::slice::from_ref(&dir), &[dataset], &[]);
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
    assert!(tight < loose, "tight {tight} should beat loose {loose}");
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
    assert_eq!(home.visible().len(), 2);

    home.filter = "sal".to_string();
    let visible = home.visible();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].1.name, "sales.parquet");

    home.filter = "zzz".to_string();
    assert!(home.visible().is_empty());
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

    assert_eq!(home.selected, 0);
    home.move_selection(1);
    assert_eq!(home.selected, 1);
    home.move_selection(1);
    assert_eq!(home.selected, 0, "should wrap");
    home.move_selection(-1);
    assert_eq!(home.selected, 1, "should wrap backwards");
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
    home.selected = 1;

    home.filter = "aaa".to_string();
    home.clamp_selection();
    assert_eq!(home.selected, 0);
    assert!(home.selected_entry().is_some());
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
        ("enter", u.enter, a.enter),
        ("backspace", u.backspace, a.backspace),
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
        ("enter", a.enter),
        ("backspace", a.backspace),
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
            set.enter,
            set.backspace,
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
fn test_filtering_does_not_show_the_same_dataset_twice() {
    // A dataset can be both recent and present in a listed directory. Grouped, the
    // headers explain that; filtered, the headers are gone and the repeat just looks
    // like a bug.
    let tmp = TempDir::new().unwrap();
    let dataset = touch(tmp.path(), "sales.parquet");

    let mut home = HomeState::default();
    home.rebuild(&[tmp.path().to_path_buf()], std::slice::from_ref(&dataset));
    home.filter = "sales".to_string();

    let hits = home
        .visible()
        .iter()
        .filter(|(_, e)| e.path.ends_with("sales.parquet"))
        .count();
    assert_eq!(hits, 1, "a dataset should appear once in filtered results");
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
    let recent = touch(&tmp.path().join("mount"), "sales.parquet");
    fs::create_dir_all(&downloads).unwrap();

    let roots = HomeState::roots(
        std::slice::from_ref(&configured),
        std::slice::from_ref(&recent),
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

    let names: Vec<&str> = home
        .visible()
        .iter()
        .map(|(_, e)| e.name.as_str())
        .collect();
    assert!(
        !names.iter().any(|n| n.contains("vault_export")),
        "a file inside a desktop-derived place must not be listed: {names:?}"
    );
    assert!(
        home.visible()
            .iter()
            .any(|(_, e)| e.path == downloads && e.kind == EntryKind::Directory),
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
        home.visible()
            .iter()
            .any(|(_, e)| e.name == "vault_export.csv"),
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

//! Home screen: dataset discovery, roots, and filtering.

use datui::discover::{self, EntryKind};
use datui::home::{fuzzy_score, HomeState, RootOrigin, Row};
use std::fs;
use tempfile::TempDir;

mod common;

/// Names of the dataset rows on screen, ignoring section headers.
fn visible_names(home: &HomeState) -> Vec<String> {
    home.visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some(entry.name.clone()),
            Row::Header { .. } => None,
        })
        .collect()
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

    // The working directory is always root 0, so look the configured one up by path.
    let roots = HomeState::roots(std::slice::from_ref(&configured), &[], &[]);
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
    assert_eq!(visible_names(&home).len(), 2);

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

    // The list is [header, a, b]; the cursor starts on the first dataset, and moving
    // walks headers too, since reaching one is how a section gets expanded.
    let total = home.visible().len();
    assert_eq!(total, 3);
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
                Row::Entry { entry, section: s } if *s == section => Some(entry.path.clone()),
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
30 25 0:44 / /mnt/gilead/data rw - autofs systemd-1 rw
81 30 0:57 / /mnt/gilead/data rw - nfs4 192.168.2.68:/volume1/data rw
";
    assert!(network_fs_for_test(
        shadowed,
        std::path::Path::new("/mnt/gilead/data/sets/prices")
    ));

    // The parent of a network mount is whatever the parent actually is.
    assert!(!network_fs_for_test(
        shadowed,
        std::path::Path::new("/mnt/gilead")
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
fn test_an_unreadable_derived_root_is_shown_not_dropped() {
    // A network share that has stopped answering is exactly what the section heading
    // exists to report. Dropping it leaves the user wondering where their data went.
    let tmp = TempDir::new().unwrap();
    let gone = tmp.path().join("mount/data");
    let dataset = touch(&gone, "sales.parquet");
    fs::remove_dir_all(tmp.path().join("mount")).unwrap();

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&dataset));

    assert!(
        home.sections.iter().any(|s| s.unavailable),
        "an unreadable root should be listed as unavailable"
    );
}

#[test]
fn test_an_empty_but_readable_derived_root_is_dropped() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().join("empty");
    let dataset = touch(&dir, "gone.parquet");
    fs::remove_file(&dataset).unwrap();

    let mut home = HomeState::default();
    home.rebuild(&[], std::slice::from_ref(&dataset));

    assert!(
        !home.sections.iter().any(|s| s.title.contains("empty")),
        "a readable root with nothing in it is noise"
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
fn test_a_probe_result_fills_the_remote_root_in() {
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");
    touch(&remote, "sales.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
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
            Row::Header { .. } => None,
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

    // After it: whatever the probe actually determined.
    home.probe_ready(root.clone(), discover::scan_dir(&root));
    home.rebuild(std::slice::from_ref(&root), std::slice::from_ref(&dataset));

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
    use datui::home::{build_listing, ListingRequest};

    let tmp = TempDir::new().unwrap();
    touch(tmp.path(), "sales.parquet");

    let request = ListingRequest {
        config_dirs: vec![tmp.path().to_path_buf()],
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        network_check: |_| false,
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
        columns: columns.iter().map(|c| c.to_string()).collect(),
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
            columns: vec!["customer_id".into()],
            kind: Some(EntryKind::File),
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
fn test_a_remote_row_uses_remembered_facts_without_a_stat() {
    // Verifying a fingerprint means stat'ing the path, which is the call that blocks
    // on a share that has gone away. A remote row therefore trusts what was recorded
    // from a real read — a stale row count beats an empty one, and these are the
    // datasets that are hardest to reach and most worth remembering.
    use datui::cache::{CacheManager, DatasetFacts};
    use datui::home::{build_listing, ListingRequest};

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
            columns: vec!["vwap".into(), "ticker".into()],
            kind: Some(EntryKind::Hive),
        },
    )]);

    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: vec![dataset.clone()],
        desktop_dirs: Vec::new(),
        browsing: None,
        probed: Default::default(),
        unreachable: Default::default(),
        network_check: pretend_remote,
        known: cache.load_dataset_facts(),
    });

    let mut home = HomeState {
        network_check: pretend_remote,
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
    use datui::home::{build_listing, ListingRequest};

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
            columns: vec!["stale".into()],
            kind: Some(EntryKind::File),
        },
    )]);

    let listing = build_listing(&ListingRequest {
        config_dirs: Vec::new(),
        recents: Vec::new(),
        desktop_dirs: Vec::new(),
        browsing: Some(tmp.path().to_path_buf()),
        probed: Default::default(),
        unreachable: Default::default(),
        network_check: |_| false,
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
        columns: Vec::new(),
    }
}

fn home_with_rows(rows: Vec<datui::discover::Entry>) -> HomeState {
    let mut home = HomeState::default();
    home.apply_listing(datui::home::Listing {
        sections: vec![datui::home::Section {
            title: "TEST".into(),
            subtitle: None,
            rows,
            unavailable: false,
        }],
        root_paths: Vec::new(),
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
fn test_the_working_directory_outranks_incidental_roots() {
    // Standing in a directory is the strongest statement of what you are working on.
    // Ordered after recent-derived roots, a single recent root holding sixty files
    // buried the very place the user had just cd'd into — the data was found, and
    // unreachable.
    let tmp = TempDir::new().unwrap();
    let elsewhere = tmp.path().join("elsewhere");
    let recent = touch(&elsewhere, "opened_once.parquet");

    let roots = HomeState::roots(&[], std::slice::from_ref(&recent), &[]);
    let cwd_at = roots.iter().position(|r| r.origin == RootOrigin::Cwd);
    let derived_at = roots.iter().position(|r| r.origin == RootOrigin::Recent);

    if let (Some(cwd_at), Some(derived_at)) = (cwd_at, derived_at) {
        assert!(
            cwd_at < derived_at,
            "the working directory should come before a root that exists only \
             because something in it was opened once"
        );
    }
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
fn test_recent_directories_promoted_to_roots_are_capped() {
    // Fifty recents scattered across fifty directories would be fifty directory
    // listings on every rebuild. The newest few are where the work is.
    let tmp = TempDir::new().unwrap();
    let recents: Vec<std::path::PathBuf> = (0..30)
        .map(|i| touch(&tmp.path().join(format!("place{i}")), "data.parquet"))
        .collect();

    let roots = HomeState::roots(&[], &recents, &[]);
    let derived = roots
        .iter()
        .filter(|r| r.origin == RootOrigin::Recent)
        .count();
    assert!(
        derived <= 8,
        "thirty scattered recents produced {derived} roots; the cap is 8"
    );
    assert_eq!(derived, 8, "the budget should be spent, not left unused");

    // Newest first: place0 is the most recent, so it must be one of the kept roots.
    assert!(
        roots.iter().any(|r| r.path == tmp.path().join("place0")),
        "the most recently used directory must survive the cap"
    );
    assert!(
        !roots.iter().any(|r| r.path == tmp.path().join("place29")),
        "the oldest directory should fall off the end"
    );
}

#[test]
fn test_many_recents_in_one_directory_cost_one_root() {
    // The cap counts directories, not recents. Opening thirty files from the same
    // place must not exhaust a budget meant for thirty different places.
    let tmp = TempDir::new().unwrap();
    let one = tmp.path().join("shared");
    let recents: Vec<std::path::PathBuf> = (0..30)
        .map(|i| touch(&one, &format!("part{i}.parquet")))
        .collect();
    let elsewhere = touch(&tmp.path().join("other"), "data.parquet");

    let mut all = recents;
    all.push(elsewhere.clone());

    let roots = HomeState::roots(&[], &all, &[]);
    assert!(
        roots.iter().any(|r| r.path == tmp.path().join("other")),
        "a directory listed last must still become a root when the ones before it \
         all resolved to the same place"
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

#[test]
fn test_subdirectories_past_the_budget_are_listed_without_being_opened() {
    // Classifying a subdirectory costs a read_dir and several stats. A directory of
    // thousands of them must not turn one listing into thousands of round trips —
    // the ones past the budget are still listed, just as places to step into.
    let tmp = TempDir::new().unwrap();
    for i in 0..200 {
        let hive = tmp.path().join(format!("d{i:03}"));
        std::fs::create_dir_all(hive.join("year=2024")).unwrap();
        std::fs::write(hive.join("year=2024/part.parquet"), b"").unwrap();
    }

    let entries = discover::scan_dir(tmp.path());
    assert_eq!(entries.len(), 200, "every subdirectory is still listed");

    let hives = entries.iter().filter(|e| e.kind == EntryKind::Hive).count();
    assert!(
        hives <= 64,
        "{hives} directories were opened to classify them; the budget is 64"
    );
    assert!(
        hives > 0,
        "the ones inside the budget should still be classified"
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
    assert!(visible_names(&home)
        .iter()
        .any(|n| n == "a/b/buried.parquet"));
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
    assert!(home
        .sections
        .iter()
        .any(|s| s.title == HomeState::SEARCH_SECTION));

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
    // difference between finding the dataset and finding the folder it is under.
    use datui::home::fuzzy_score;
    let in_name = fuzzy_score("report", "archive/old/report.csv").unwrap();
    let in_dir = fuzzy_score("report", "report/2024/summary.csv").unwrap();
    assert!(
        in_name > in_dir,
        "a basename match ({in_name}) should beat a directory match ({in_dir})"
    );
}

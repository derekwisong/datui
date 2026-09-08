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

    let more = home.enrich_visible(100, 2);
    assert!(more, "with 6 rows and a budget of 2, work must remain");
    assert_eq!(home.enriched.len(), 2, "a pass spends only its budget");

    home.enrich_visible(100, 2);
    assert_eq!(
        home.enriched.len(),
        4,
        "the next pass continues where it left off"
    );

    let more = home.enrich_visible(100, 10);
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
    home.enrich_visible(3, 100);
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

    home.enrich_visible(100, 100);
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
fn test_remote_rows_are_never_measured_on_this_thread() {
    // Reading a Parquet footer opens the file, which is the call that hangs.
    let tmp = TempDir::new().unwrap();
    let remote = tmp.path().join("PRETEND_REMOTE/data");
    touch(&remote, "a.parquet");

    let mut home = HomeState {
        network_check: pretend_remote,
        browsing: Some(remote.clone()),
        ..Default::default()
    };
    home.rebuild(&[], &[]);
    home.enrich_visible(100, 100);

    assert!(
        home.enriched.is_empty(),
        "measuring a remote row would open a remote file"
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

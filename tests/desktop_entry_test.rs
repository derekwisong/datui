//! The shipped freedesktop entry.
//!
//! This file is how datui becomes visible to desktop launchers — including
//! Omarchy's menu, which lists applications from the freedesktop database rather
//! than from anything Omarchy-specific. A typo here fails silently: the entry is
//! simply ignored and datui vanishes from every launcher, with nothing to notice.

use std::collections::HashMap;

const DESKTOP_FILE: &str = "scripts/packaging/datui.desktop";

fn entry() -> HashMap<String, String> {
    let text = std::fs::read_to_string(DESKTOP_FILE)
        .unwrap_or_else(|e| panic!("{DESKTOP_FILE} should exist: {e}"));

    let mut in_group = false;
    let mut map = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            in_group = line == "[Desktop Entry]";
            continue;
        }
        if in_group {
            if let Some((key, value)) = line.split_once('=') {
                map.insert(key.trim().to_string(), value.trim().to_string());
            }
        }
    }
    map
}

#[test]
fn test_has_the_keys_the_spec_requires() {
    let e = entry();
    assert_eq!(e.get("Type").map(String::as_str), Some("Application"));
    assert_eq!(e.get("Name").map(String::as_str), Some("datui"));
    assert!(e.contains_key("Exec"), "an Application entry needs Exec");
}

#[test]
fn test_runs_in_a_terminal() {
    // datui is a TUI and needs a real TTY. Without this the launcher starts it
    // detached, it finds no terminal, and it exits immediately.
    let e = entry();
    assert_eq!(e.get("Terminal").map(String::as_str), Some("true"));
}

#[test]
fn test_exec_accepts_files_and_launches_bare() {
    // `%F` lets a file manager pass a selection through ("Open with datui"), and
    // bare `datui` opens the home screen — which is what a launcher click should do.
    let e = entry();
    let exec = e.get("Exec").expect("Exec");
    assert!(
        exec.starts_with("datui"),
        "Exec should invoke datui: {exec}"
    );
    assert!(
        exec.contains("%F") || exec.contains("%U"),
        "Exec should accept file arguments: {exec}"
    );
}

#[test]
fn test_declares_console_only() {
    // ConsoleOnly tells launchers this is a terminal application; some menus use it
    // to decide placement.
    let e = entry();
    let categories = e.get("Categories").expect("Categories");
    assert!(
        categories.contains("ConsoleOnly"),
        "a TUI should declare ConsoleOnly: {categories}"
    );
    assert!(
        categories.ends_with(';'),
        "Categories is a semicolon-terminated list: {categories}"
    );
}

#[test]
fn test_mime_types_are_semicolon_terminated_and_plausible() {
    let e = entry();
    let mime = e.get("MimeType").expect("MimeType");
    assert!(
        mime.ends_with(';'),
        "MimeType is a semicolon-terminated list: {mime}"
    );
    for required in ["text/csv", "application/vnd.apache.parquet"] {
        assert!(
            mime.contains(required),
            "datui's two headline formats should be listed; missing {required}"
        );
    }
    for entry in mime.split(';').filter(|s| !s.is_empty()) {
        assert!(
            entry.contains('/') && !entry.contains(' '),
            "not a media type: {entry:?}"
        );
    }
}

#[test]
fn test_packaging_ships_it_everywhere() {
    // Three packagers, three asset lists. Adding the file without wiring it into all
    // of them means it ships on some distributions and not others.
    let cargo_toml = std::fs::read_to_string("Cargo.toml").expect("Cargo.toml");
    let mentions = cargo_toml.matches("datui.desktop").count();
    assert!(
        mentions >= 4,
        "expected the desktop file in the deb, rpm and aur asset lists \
         (source + dest for rpm), found {mentions} mentions"
    );
    assert!(
        cargo_toml.contains("usr/share/applications/datui.desktop"),
        "should install to the freedesktop applications directory"
    );
}

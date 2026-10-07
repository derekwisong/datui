use super::*;

fn manager_with(text: Option<&str>) -> (tempfile::TempDir, ConfigManager) {
    let dir = tempfile::tempdir().unwrap();
    if let Some(text) = text {
        std::fs::write(dir.path().join("config.toml"), text).unwrap();
    }
    let manager = ConfigManager::with_dir(dir.path().to_path_buf());
    (dir, manager)
}

#[test]
fn init_writes_once_and_force_replaces() {
    let (dir, manager) = manager_with(None);
    let (text, code) = command(&manager, &ConfigAction::Init { force: false }, &[]);
    assert_eq!(code, 0, "{text}");
    assert!(dir.path().join("config.toml").exists());
    let (text, code) = command(&manager, &ConfigAction::Init { force: false }, &[]);
    assert_eq!(code, 1);
    assert!(text.contains("--force"), "{text}");
    assert_eq!(
        command(&manager, &ConfigAction::Init { force: true }, &[]).1,
        0
    );
}

#[test]
fn path_lists_imports_before_the_file() {
    let (dir, manager) = manager_with(Some("import = [\"theme.toml\"]\n"));
    std::fs::write(dir.path().join("theme.toml"), "[theme]\nmode = \"light\"\n").unwrap();
    let (text, code) = command(&manager, &ConfigAction::Path, &[]);
    assert_eq!(code, 0);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert!(
        lines[0].ends_with("theme.toml") && lines[1].ends_with("config.toml"),
        "{text}"
    );

    let (_dir, manager) = manager_with(None);
    let (text, _) = command(&manager, &ConfigAction::Path, &[]);
    assert!(text.contains("not there"), "{text}");
}

#[test]
fn keys_say_what_set_each_value() {
    let (_dir, manager) =
        manager_with(Some("[display]\nrow_numbers = true\nrow_start_index = 0\n"));
    let overrides = vec!["display.row_numbers_start=7".parse().unwrap()];
    let (text, code) = command(&manager, &ConfigAction::Keys, &overrides);
    assert_eq!(code, 0, "{text}");
    let row = |key: &str| {
        text.lines()
            .find(|l| l.starts_with(&format!("{key} ")))
            .unwrap_or_else(|| panic!("{key} listed"))
            .split_whitespace()
            .collect::<Vec<_>>()
    };
    let numbers = row("display.row_numbers");
    // From the end: the type, `"auto" | bool`, is several words.
    let [.., value, source] = numbers.as_slice() else {
        panic!("{numbers:?}");
    };
    assert_eq!(*value, "true");
    assert!(source.ends_with("config.toml"), "{numbers:?}");
    assert_eq!(row("display.row_numbers_start")[3..], ["7", "-c"]);
    assert_eq!(row("display.mouse")[3..], ["true", "default"]);
    for setting in SETTINGS.iter().filter(|s| !s.key.ends_with(".*")) {
        assert!(text.contains(setting.key), "{} listed", setting.key);
    }
}

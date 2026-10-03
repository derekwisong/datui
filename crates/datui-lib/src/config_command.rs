//! `datui config`: write the default file, list the files read, list every key.

use std::path::Path;

use datui_cli::ConfigAction;
use datui_cli::settings::{DefaultValue, Kind, Override, SETTINGS, Setting};

use crate::config::{AppConfig, ConfigLayer, ConfigManager, LayerSource};

/// What `datui config ACTION` prints, and its exit code. `overrides` are the `-c`
/// settings, which `keys` counts as a layer.
pub fn command(
    manager: &ConfigManager,
    action: &ConfigAction,
    overrides: &[Override],
) -> (String, i32) {
    let file = manager.config_path("config.toml");
    match action {
        ConfigAction::Init { force } => match manager.write_default_config(*force) {
            Ok(path) => (format!("Wrote {}\n", path.display()), 0),
            Err(e) => (format!("{e}\n"), 1),
        },
        ConfigAction::Path => match AppConfig::read_layers(&file, &[]) {
            Ok(layers) => (paths(&file, &layers), 0),
            Err(e) => (format!("{e}\n"), 1),
        },
        ConfigAction::Keys => {
            let read = AppConfig::read_layers(&file, overrides).and_then(|layers| {
                AppConfig::from_read_layers(&file, overrides, &layers).map(|c| (layers, c))
            });
            match read {
                Ok((layers, config)) => (keys(&config, &layers), 0),
                Err(e) => (format!("{e}\n"), 1),
            }
        }
    }
}

/// The files read, lowest precedence first; the user's own file last, said to be
/// missing when it is.
fn paths(file: &Path, layers: &[(LayerSource, ConfigLayer)]) -> String {
    let mut out = String::new();
    for (source, _) in layers {
        out.push_str(&format!("{source}\n"));
    }
    if layers.is_empty() {
        out.push_str(&format!("{} (not there: defaults apply)\n", file.display()));
    }
    out
}

/// Every key: kind, default, the value in effect and the layer that set it.
fn keys(config: &AppConfig, layers: &[(LayerSource, ConfigLayer)]) -> String {
    let toml::Value::Table(effective) =
        toml::Value::try_from(config).unwrap_or(toml::Value::Table(Default::default()))
    else {
        unreachable!("a struct serializes to a table")
    };
    let mut rows: Vec<[String; 5]> = vec![[
        "KEY".into(),
        "TYPE".into(),
        "DEFAULT".into(),
        "VALUE".into(),
        "SET BY".into(),
    ]];
    for setting in SETTINGS {
        for key in expand(setting, layers) {
            let value = value_at(&effective, &key).map_or_else(|| "unset".to_string(), shown);
            let set_by = layers
                .iter()
                .rev()
                .find(|(_, layer)| layer.get(&key).is_some())
                .map_or_else(|| "default".to_string(), |(source, _)| source.to_string());
            rows.push([
                key,
                shorten(setting.kind.describe().replace("\\|", "|"), 24),
                shorten(default_text(setting), 32),
                value,
                set_by,
            ]);
        }
    }
    let widths: Vec<usize> = (0..4)
        .map(|i| rows.iter().map(|r| r[i].chars().count()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < 4 {
                out.push_str(&format!("{cell:<width$}  ", width = widths[i]));
            } else {
                out.push_str(cell);
            }
        }
        out.push('\n');
    }
    out
}

/// The keys a setting stands for: itself, or for `glyphs.*` each name the layers set.
fn expand(setting: &Setting, layers: &[(LayerSource, ConfigLayer)]) -> Vec<String> {
    let Some(prefix) = setting.key.strip_suffix(".*") else {
        return vec![setting.key.to_string()];
    };
    let mut names: Vec<String> = layers
        .iter()
        .filter_map(|(_, layer)| layer.get(prefix).and_then(toml::Value::as_table))
        .flat_map(|table| table.keys().map(|name| format!("{prefix}.{name}")))
        .collect();
    names.sort();
    names.dedup();
    names
}

fn default_text(setting: &Setting) -> String {
    match setting.default {
        DefaultValue::Value(v) => v.to_string(),
        DefaultValue::Unset(_) if setting.kind == Kind::Tables => "none".into(),
        DefaultValue::Unset(_) => "unset".into(),
        DefaultValue::Color { dark, light } => format!("{dark} / {light}"),
    }
}

/// A value on one line, long lists shortened.
fn shown(value: &toml::Value) -> String {
    let text = match value {
        toml::Value::Array(items) if items.iter().any(toml::Value::is_table) => {
            format!("{} entries", items.len())
        }
        toml::Value::Table(_) => "table".to_string(),
        other => other.to_string(),
    };
    shorten(text, 40)
}

/// `text` cut to `most` characters, the cut marked.
fn shorten(text: String, most: usize) -> String {
    if text.chars().count() > most {
        let cut: String = text.chars().take(most - 3).collect();
        format!("{cut}...")
    } else {
        text
    }
}

fn value_at<'a>(table: &'a toml::Table, key: &str) -> Option<&'a toml::Value> {
    let mut parts = key.split('.');
    let mut value = table.get(parts.next()?)?;
    for part in parts {
        value = value.as_table()?.get(part)?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
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
        assert_eq!(numbers[3], "true");
        assert!(numbers[4].ends_with("config.toml"), "{numbers:?}");
        assert_eq!(row("display.row_numbers_start")[3..], ["7", "-c"]);
        assert_eq!(row("display.mouse")[3..], ["true", "default"]);
        for setting in SETTINGS.iter().filter(|s| !s.key.ends_with(".*")) {
            assert!(text.contains(setting.key), "{} listed", setting.key);
        }
    }
}

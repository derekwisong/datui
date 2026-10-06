//! Named themes: the two built in, and every `*.toml` in the config directory's
//! `themes/`, each a set of `theme.colors` slots named by its file. `theme.dark` and
//! `theme.light` pick one per mode; `theme.colors` lies over whichever is in use.

use std::path::{Path, PathBuf};

use crate::config::{ColorConfig, ColorParser, ThemeMode};

/// The config directory's folder of theme files.
pub const FOLDER: &str = "themes";
/// The built-in dark theme: Tokyo Night with one cyan accent.
pub const NIGHT_MARKET: &str = "night-market";
/// The built-in light theme: Tokyo Night's day variant.
pub const DAY_MARKET: &str = "day-market";

/// The keys a theme file holds besides its slots.
const EXTENDS: &str = "extends";
const DESCRIPTION: &str = "description";
/// How long an `extends` chain may be: deeper is surely a mistake.
const MAX_DEPTH: usize = 16;

/// The built-in theme for a mode, by name.
pub fn built_in_name(mode: ThemeMode) -> &'static str {
    match mode {
        ThemeMode::Light => DAY_MARKET,
        _ => NIGHT_MARKET,
    }
}

/// A theme file that parsed.
#[derive(Debug, Clone, PartialEq)]
pub struct ThemeFile {
    /// Its file stem.
    pub name: String,
    pub path: PathBuf,
    pub extends: Option<String>,
    pub description: Option<String>,
    /// The slots it sets, each checked to be a color.
    pub colors: toml::Table,
}

/// A theme file left out for a mistake.
#[derive(Debug, Clone, PartialEq)]
pub struct Broken {
    pub name: String,
    pub path: PathBuf,
    pub message: String,
}

impl Broken {
    /// `path: message`, for a warning.
    pub fn full(&self) -> String {
        format!("{}: {}", self.path.display(), self.message)
    }
}

/// Every theme there is: the built-ins, and what `themes/` holds.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Library {
    /// Sorted by name.
    pub files: Vec<ThemeFile>,
    pub broken: Vec<Broken>,
}

/// Where a theme comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum Source<'a> {
    BuiltIn(ThemeMode),
    File(&'a ThemeFile),
}

impl Library {
    /// The themes in `config_dir`'s `themes/`; only the built-ins without one. A file
    /// with a mistake is left out and kept in `broken`, so one bad file never stops
    /// datui from starting.
    pub fn read(config_dir: Option<&Path>) -> Self {
        let mut library = Self::default();
        let Some(dir) = config_dir else {
            return library;
        };
        let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir.join(FOLDER)) {
            Ok(entries) => entries
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.extension().is_some_and(|x| x == "toml")
                        && std::fs::metadata(p).is_ok_and(|m| m.is_file())
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        paths.sort();
        for path in paths {
            let name = path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let read = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| parse(&name, &path, &text));
            match read {
                Ok(file) => library.files.push(file),
                Err(message) => library.broken.push(Broken {
                    name,
                    path,
                    message,
                }),
            }
        }
        library
    }

    /// The theme named `name`, if there is one.
    pub fn find(&self, name: &str) -> Option<Source<'_>> {
        match name {
            NIGHT_MARKET => Some(Source::BuiltIn(ThemeMode::Dark)),
            DAY_MARKET => Some(Source::BuiltIn(ThemeMode::Light)),
            _ => self.files.iter().find(|f| f.name == name).map(Source::File),
        }
    }

    /// Every theme's name, the built-ins first.
    pub fn names(&self) -> Vec<&str> {
        let mut names = vec![NIGHT_MARKET, DAY_MARKET];
        names.extend(self.files.iter().map(|f| f.name.as_str()));
        names
    }

    /// The colors of theme `name` when it is used in `mode`. Its unset slots come from
    /// the theme it `extends`, or, when it extends none, from the built-in for `mode`.
    pub fn resolve(&self, name: &str, mode: ThemeMode) -> Result<ColorConfig, String> {
        let mut chain: Vec<&ThemeFile> = Vec::new();
        let mut at = name;
        let base = loop {
            match self.find(at) {
                Some(Source::BuiltIn(built)) => break ColorConfig::for_mode(built),
                Some(Source::File(file)) => {
                    if let Some(seen) = chain.iter().position(|f| f.name == file.name) {
                        let names: Vec<&str> = chain[seen..]
                            .iter()
                            .map(|f| f.name.as_str())
                            .chain([file.name.as_str()])
                            .collect();
                        return Err(format!("themes extend in a circle: {}", names.join(" > ")));
                    }
                    if chain.len() >= MAX_DEPTH {
                        return Err(format!(
                            "theme {name} extends more than {MAX_DEPTH} themes deep"
                        ));
                    }
                    chain.push(file);
                    match &file.extends {
                        Some(next) => at = next,
                        None => break ColorConfig::for_mode(mode),
                    }
                }
                None => return Err(self.missing(at, chain.last().copied())),
            }
        };
        let mut palette = slots(&base);
        for file in chain.iter().rev() {
            palette.extend(file.colors.clone());
        }
        toml::Value::Table(palette)
            .try_into()
            .map_err(|e: toml::de::Error| e.message().to_string())
    }

    /// Why `name`, asked for by `by` (or by the config), is not a theme.
    fn missing(&self, name: &str, by: Option<&ThemeFile>) -> String {
        let from = by.map_or_else(String::new, |f| {
            format!(" ({} extends it)", f.path.display())
        });
        if let Some(broken) = self.broken.iter().find(|b| b.name == name) {
            return format!(
                "theme {name} was left out for a mistake{from}: {}",
                broken.full()
            );
        }
        format!(
            "no theme is named {name}{from}. Themes: {}",
            self.names().join(", ")
        )
    }
}

/// A slot table for `colors`.
pub fn slots(colors: &ColorConfig) -> toml::Table {
    match toml::Value::try_from(colors) {
        Ok(toml::Value::Table(table)) => table,
        _ => unreachable!("a struct serializes to a table"),
    }
}

/// A theme file's text, checked: its keys are slots, `extends` or `description`,
/// and every slot is a color `theme.colors` would take.
pub fn parse(name: &str, path: &Path, text: &str) -> Result<ThemeFile, String> {
    if name == NIGHT_MARKET || name == DAY_MARKET {
        return Err(format!(
            "{name} is a built-in theme's name; rename the file and set extends = \"{name}\""
        ));
    }
    let table: toml::Table = text.parse().map_err(|e: toml::de::Error| match e.span() {
        Some(span) => format!(
            "line {}: {}",
            text[..span.start].matches('\n').count() + 1,
            e.message()
        ),
        None => e.message().to_string(),
    })?;
    let known = slots(&ColorConfig::dark());
    let parser = ColorParser::new();
    let mut file = ThemeFile {
        name: name.to_string(),
        path: path.to_path_buf(),
        extends: None,
        description: None,
        colors: toml::Table::new(),
    };
    for (key, value) in table {
        let text = |value: toml::Value| match value {
            toml::Value::String(s) => Ok(s),
            other => Err(format!("{key} must be a string, got {other}")),
        };
        match key.as_str() {
            EXTENDS => file.extends = Some(text(value)?),
            DESCRIPTION => file.description = Some(text(value)?),
            _ if known.contains_key(&key) => {
                let color = text(value)?;
                // "default" is no stripe, not a color.
                if !(key == "table_alternate_row" && color == "default") {
                    parser.parse(&color).map_err(|e| format!("{key}: {e}"))?;
                }
                file.colors.insert(key, toml::Value::String(color));
            }
            "theme" | "colors" => {
                return Err(format!(
                    "a theme file holds the slots at its top level; drop the [{key}...] header"
                ));
            }
            _ => {
                let near: Vec<&str> =
                    datui_cli::settings::suggestions(&format!("theme.colors.{key}"))
                        .into_iter()
                        .filter_map(|k| k.strip_prefix("theme.colors."))
                        .collect();
                let mut message = format!("{key} is not a color slot");
                if !near.is_empty() {
                    message.push_str(&format!("; did you mean {}?", near.join(" or ")));
                }
                return Err(message);
            }
        }
    }
    if file.extends.as_deref() == Some(name) {
        return Err(format!("{name} extends itself"));
    }
    Ok(file)
}

/// The built-in themes' descriptions.
fn built_in_description(mode: ThemeMode) -> &'static str {
    match mode {
        ThemeMode::Light => "Tokyo Night's day variant, for a light terminal",
        _ => "Tokyo Night with one cyan accent, for a dark terminal",
    }
}

/// What `datui theme ACTION` prints, and its exit code. `config` is the
/// configuration in effect, or why it could not be read.
pub fn command(
    action: &datui_cli::ThemeAction,
    config: color_eyre::Result<crate::config::AppConfig>,
) -> (String, i32) {
    use datui_cli::ThemeAction;
    use datui_cli::exit::{FAILURE, SUCCESS};
    let config = match config {
        Ok(config) => config,
        Err(e) => return (format!("{e}\n"), FAILURE),
    };
    let theme = &config.theme;
    let library = &theme.library;
    match action {
        ThemeAction::List => {
            let set_for = |name: &str| {
                let modes: Vec<&str> = [
                    (theme.dark_theme.as_str(), "dark"),
                    (theme.light_theme.as_str(), "light"),
                ]
                .into_iter()
                .filter(|(used, _)| *used == name)
                .map(|(_, mode)| mode)
                .collect();
                if modes.is_empty() {
                    "-".to_string()
                } else {
                    modes.join(", ")
                }
            };
            let mut rows = vec![vec![
                "NAME".to_string(),
                "SET FOR".to_string(),
                "FROM".to_string(),
                "DESCRIPTION".to_string(),
            ]];
            for mode in [ThemeMode::Dark, ThemeMode::Light] {
                let name = built_in_name(mode);
                rows.push(vec![
                    name.to_string(),
                    set_for(name),
                    "built in".to_string(),
                    built_in_description(mode).to_string(),
                ]);
            }
            for file in &library.files {
                rows.push(vec![
                    file.name.clone(),
                    set_for(&file.name),
                    file.path.display().to_string(),
                    file.description.clone().unwrap_or_default(),
                ]);
            }
            for broken in &library.broken {
                rows.push(vec![
                    broken.name.clone(),
                    "-".to_string(),
                    broken.path.display().to_string(),
                    format!(
                        "{} not read: {}",
                        crate::glyphs::get().warning,
                        broken.message
                    ),
                ]);
            }
            (crate::catalog::table(&rows), SUCCESS)
        }
        ThemeAction::Show { name } => {
            let (mode, description) = match library.find(name) {
                // A copy is a new theme: the built-in's description is not its own.
                Some(Source::BuiltIn(mode)) => (mode, None),
                // A theme that extends none fills its unset slots from the built-in for
                // the mode it is used in: show it as the light one only where it is that.
                Some(Source::File(file)) => (
                    if theme.light_theme == *name && theme.dark_theme != *name {
                        ThemeMode::Light
                    } else {
                        ThemeMode::Dark
                    },
                    file.description.clone(),
                ),
                None => return (format!("{}\n", library.missing(name, None)), FAILURE),
            };
            match library.resolve(name, mode) {
                Ok(colors) => (show(name, description.as_deref(), &colors), SUCCESS),
                Err(e) => (format!("{e}\n"), FAILURE),
            }
        }
    }
}

/// A theme file with every slot, each under what it colors: a starting point to save
/// into `themes/` and edit.
pub fn show(name: &str, description: Option<&str>, colors: &ColorConfig) -> String {
    let palette = slots(colors);
    let mut out = format!(
        "# Every slot of the {name} theme. Saved as themes/NAME.toml in the config\n\
         # directory, it is a theme named NAME to edit. A file with extends = \"{name}\"\n\
         # and only the slots it changes works too.\n"
    );
    if let Some(description) = description {
        out.push_str(&format!(
            "description = {}\n",
            toml::Value::String(description.to_string())
        ));
    }
    for setting in datui_cli::settings::in_section("theme.colors") {
        let slot = setting.name();
        let Some(value) = palette.get(slot) else {
            continue;
        };
        out.push_str(&format!("\n# {}\n{slot} = {value}\n", setting.doc));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn library(files: &[(&str, &str)]) -> Library {
        let mut library = Library::default();
        for (name, text) in files {
            let path = PathBuf::from(format!("{name}.toml"));
            match parse(name, &path, text) {
                Ok(file) => library.files.push(file),
                Err(message) => library.broken.push(Broken {
                    name: name.to_string(),
                    path,
                    message,
                }),
            }
        }
        library
    }

    #[test]
    fn built_ins_are_today_s_palettes() {
        let none = Library::default();
        for mode in [ThemeMode::Dark, ThemeMode::Light] {
            assert_eq!(none.resolve(NIGHT_MARKET, mode), Ok(ColorConfig::dark()));
            assert_eq!(none.resolve(DAY_MARKET, mode), Ok(ColorConfig::light()));
        }
    }

    /// The ten series slots are in every built-in, `theme show`, `config init`, and a
    /// theme that extends one inherits them.
    #[test]
    fn ten_series_slots_everywhere() {
        let lib = library(&[("dusk", "extends = \"day-market\"\nchart_9 = \"red\"\n")]);
        let dusk = lib.resolve("dusk", ThemeMode::Dark).unwrap();
        assert_eq!(dusk.chart_8, ColorConfig::light().chart_8);
        assert_eq!(dusk.chart_9, "red");
        for (name, colors) in [
            (NIGHT_MARKET, ColorConfig::dark()),
            (DAY_MARKET, ColorConfig::light()),
        ] {
            let shown = show(name, None, &colors);
            for slot in ["chart_8", "chart_9", "chart_10"] {
                assert!(shown.contains(&format!("\n{slot} = \"#")), "{shown}");
            }
        }
        let init = crate::config::ConfigManager::with_dir(PathBuf::from("unused"))
            .generate_default_config();
        assert!(init.contains("chart_10 = \"#f4ef8a\""), "{init}");
    }

    #[test]
    fn a_theme_fills_unset_slots_from_extends_or_the_mode() {
        let lib = library(&[
            ("dusk", "extends = \"day-market\"\naccent = \"#e0af68\"\n"),
            ("bare", "accent = \"#e0af68\"\n"),
            ("deeper", "extends = \"dusk\"\nchip_key = \"red\"\n"),
        ]);
        let dusk = lib.resolve("dusk", ThemeMode::Dark).unwrap();
        assert_eq!(dusk.accent, "#e0af68");
        assert_eq!(dusk.controls_bg, ColorConfig::light().controls_bg);

        for (mode, stock) in [
            (ThemeMode::Dark, ColorConfig::dark()),
            (ThemeMode::Light, ColorConfig::light()),
        ] {
            let bare = lib.resolve("bare", mode).unwrap();
            assert_eq!(bare.accent, "#e0af68");
            assert_eq!(bare.controls_bg, stock.controls_bg, "{mode:?}");
        }

        let deeper = lib.resolve("deeper", ThemeMode::Dark).unwrap();
        assert_eq!(deeper.chip_key, "red");
        assert_eq!(deeper.accent, "#e0af68");
        assert_eq!(deeper.controls_bg, ColorConfig::light().controls_bg);
    }

    #[test]
    fn a_circle_or_an_unknown_name_is_an_error_naming_it() {
        let lib = library(&[
            ("a", "extends = \"b\"\n"),
            ("b", "extends = \"a\"\n"),
            ("c", "extends = \"nope\"\n"),
        ]);
        let e = lib.resolve("a", ThemeMode::Dark).unwrap_err();
        assert!(e.contains("a > b > a"), "{e}");
        let e = lib.resolve("c", ThemeMode::Dark).unwrap_err();
        assert!(e.contains("nope") && e.contains("c.toml extends it"), "{e}");
        let e = lib.resolve("zzz", ThemeMode::Dark).unwrap_err();
        assert!(e.contains("no theme is named zzz"), "{e}");
    }

    #[test]
    fn a_file_with_a_mistake_is_left_out_with_why() {
        let lib = library(&[
            ("bad-color", "accent = \"#zz\"\n"),
            ("bad-key", "acent = \"red\"\n"),
            ("bad-toml", "accent = \n"),
            ("self", "extends = \"self\"\n"),
            ("night-market", "accent = \"red\"\n"),
            ("good", "extends = \"bad-color\"\n"),
            ("header", "[theme.colors]\naccent = \"red\"\n"),
        ]);
        let why = |name: &str| {
            lib.broken
                .iter()
                .find(|b| b.name == name)
                .map(|b| b.message.clone())
                .unwrap_or_else(|| panic!("{name} not broken"))
        };
        assert!(why("bad-color").starts_with("accent:"));
        assert!(
            why("bad-key").contains("did you mean accent"),
            "{}",
            why("bad-key")
        );
        assert!(why("bad-toml").starts_with("line 1"), "{}", why("bad-toml"));
        assert!(why("self").contains("extends itself"));
        assert!(why("header").contains("top level"), "{}", why("header"));
        assert!(why("night-market").contains("built-in"));
        let e = lib.resolve("good", ThemeMode::Dark).unwrap_err();
        assert!(e.contains("left out for a mistake"), "{e}");
    }

    #[test]
    fn show_parses_back_to_the_same_theme() {
        let lib = library(&[(
            "dusk",
            "extends = \"night-market\"\ndescription = \"Dusk\"\naccent = \"#e0af68\"\n",
        )]);
        let colors = lib.resolve("dusk", ThemeMode::Dark).unwrap();
        let text = show("dusk", Some("Dusk"), &colors);
        let back = parse("copy", Path::new("copy.toml"), &text).unwrap();
        assert_eq!(back.extends, None);
        assert_eq!(back.description.as_deref(), Some("Dusk"));
        assert_eq!(back.colors, slots(&colors), "every slot");
        let again = library(&[("copy", &text)]);
        assert_eq!(again.resolve("copy", ThemeMode::Light), Ok(colors));
    }
}

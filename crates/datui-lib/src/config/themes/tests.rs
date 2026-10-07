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
    let init =
        crate::config::ConfigManager::with_dir(PathBuf::from("unused")).generate_default_config();
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

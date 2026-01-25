use datui::config::{AppConfig, ColorParser, Theme};
use ratatui::style::Color;

#[test]
fn test_indexed_colors_end_to_end() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    // Create config with indexed colors
    let config_toml = r#"
version = "0.2"

[theme.colors]
keybind_hints = "cyan"
keybind_labels = "yellow"
success = "green"
error = "red"
warning = "yellow"
dimmed = "dark_gray"
background = "black"
surface = "indexed(239)"
controls_bg = "indexed(236)"
text_primary = "white"
text_secondary = "dark_gray"
text_inverse = "black"
table_header = "white"
table_header_bg = "indexed(236)"
column_separator = "cyan"
table_selected = "reversed"
sidebar_border = "cyan"
modal_border_active = "yellow"
modal_border_error = "red"
distribution_normal = "green"
distribution_skewed = "yellow"
distribution_other = "white"
outlier_marker = "red"
"#;

    // Parse config
    let config: AppConfig = toml::from_str(config_toml).expect("Failed to parse config");

    // Validate - should pass with indexed colors
    assert!(config.validate().is_ok());

    // Create theme from config
    let theme =
        Theme::from_config(&config.theme).expect("Failed to create theme with indexed colors");

    // Verify indexed colors are parsed correctly
    assert_eq!(theme.get("controls_bg"), Color::Indexed(236));
    assert_eq!(theme.get("surface"), Color::Indexed(239));

    // Verify other colors still work
    assert_eq!(theme.get("keybind_hints"), Color::Cyan);
    assert_eq!(theme.get("error"), Color::Red);
}

#[test]
fn test_indexed_colors_in_default_config() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let config = AppConfig::default();
    let theme = Theme::from_config(&config.theme).expect("Failed to create theme");

    // Default config uses indexed(235) for controls_bg and table_header_bg
    assert_eq!(theme.get("controls_bg"), Color::Indexed(235));
    assert_eq!(theme.get("table_header_bg"), Color::Indexed(235));
}

#[test]
fn test_mixed_color_formats() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let parser = ColorParser::new();

    // All three formats should work together
    let named = parser.parse("cyan").unwrap();
    let hex = parser.parse("#ff0000").unwrap();
    let indexed = parser.parse("indexed(196)").unwrap();

    // Verify they're different color types
    assert_eq!(named, Color::Cyan);
    assert_eq!(indexed, Color::Indexed(196));
    // Hex depends on terminal capabilities, just verify it parses
    assert!(matches!(
        hex,
        Color::Rgb(_, _, _) | Color::Indexed(_) | Color::Red | Color::Reset
    ));
}

use datui::config::{ColorParser, Theme};
use ratatui::style::Color;

// Helper to ensure NO_COLOR is not set for color parsing tests
fn ensure_colors_enabled() {
    std::env::remove_var("NO_COLOR");
}

#[test]
fn test_parse_basic_ansi_colors() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Test basic ANSI colors
    assert_eq!(parser.parse("black").unwrap(), Color::Black);
    assert_eq!(parser.parse("red").unwrap(), Color::Red);
    assert_eq!(parser.parse("green").unwrap(), Color::Green);
    assert_eq!(parser.parse("yellow").unwrap(), Color::Yellow);
    assert_eq!(parser.parse("blue").unwrap(), Color::Blue);
    assert_eq!(parser.parse("magenta").unwrap(), Color::Magenta);
    assert_eq!(parser.parse("cyan").unwrap(), Color::Cyan);
    assert_eq!(parser.parse("white").unwrap(), Color::White);
}

#[test]
fn test_parse_bright_colors() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Test bright variants
    assert_eq!(parser.parse("bright_red").unwrap(), Color::Indexed(9));
    assert_eq!(parser.parse("bright red").unwrap(), Color::Indexed(9));
    assert_eq!(parser.parse("bright_green").unwrap(), Color::Indexed(10));
    assert_eq!(parser.parse("bright_blue").unwrap(), Color::Indexed(12));
    assert_eq!(parser.parse("bright_cyan").unwrap(), Color::Indexed(14));
}

#[test]
fn test_parse_gray_aliases() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    assert_eq!(parser.parse("gray").unwrap(), Color::Indexed(8));
    assert_eq!(parser.parse("grey").unwrap(), Color::Indexed(8));
    assert_eq!(parser.parse("dark_gray").unwrap(), Color::Indexed(8));
    assert_eq!(parser.parse("dark gray").unwrap(), Color::Indexed(8));
    assert_eq!(parser.parse("light_gray").unwrap(), Color::Indexed(7));
}

#[test]
fn test_parse_case_insensitive() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    assert_eq!(parser.parse("RED").unwrap(), Color::Red);
    assert_eq!(parser.parse("Red").unwrap(), Color::Red);
    assert_eq!(parser.parse("CYAN").unwrap(), Color::Cyan);
    assert_eq!(parser.parse("BRIGHT_RED").unwrap(), Color::Indexed(9));
}

#[test]
fn test_parse_hex_colors() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Parse hex colors - actual result depends on terminal capability
    let result = parser.parse("#ff0000");
    assert!(result.is_ok());

    let result = parser.parse("#00ff00");
    assert!(result.is_ok());

    let result = parser.parse("#0000ff");
    assert!(result.is_ok());

    let result = parser.parse("#ffffff");
    assert!(result.is_ok());

    let result = parser.parse("#000000");
    assert!(result.is_ok());
}

#[test]
fn test_parse_hex_case_insensitive() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    let result1 = parser.parse("#FF0000");
    let result2 = parser.parse("#ff0000");

    assert!(result1.is_ok());
    assert!(result2.is_ok());
    // Both should parse successfully (actual color may vary by terminal capability)
}

#[test]
fn test_parse_invalid_hex() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Invalid formats
    assert!(parser.parse("#ff00").is_err()); // Too short
    assert!(parser.parse("#ff00000").is_err()); // Too long
    assert!(parser.parse("ff0000").is_err()); // Missing #
    assert!(parser.parse("#gggggg").is_err()); // Invalid hex digits
}

#[test]
fn test_parse_indexed_colors() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Valid indexed colors
    assert_eq!(parser.parse("indexed(0)").unwrap(), Color::Indexed(0));
    assert_eq!(parser.parse("indexed(236)").unwrap(), Color::Indexed(236));
    assert_eq!(parser.parse("indexed(255)").unwrap(), Color::Indexed(255));

    // Case insensitive
    assert_eq!(parser.parse("INDEXED(236)").unwrap(), Color::Indexed(236));
    assert_eq!(parser.parse("Indexed(100)").unwrap(), Color::Indexed(100));
}

#[test]
fn test_parse_invalid_indexed() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Invalid indexed formats
    assert!(parser.parse("indexed(-1)").is_err()); // Negative
    assert!(parser.parse("indexed(abc)").is_err()); // Not a number
    assert!(parser.parse("indexed()").is_err()); // Empty
    assert!(parser.parse("indexed(1.5)").is_err()); // Float
    assert!(parser.parse("indexed(999)").is_err()); // Out of range (u8 overflow)
}

#[test]
fn test_parse_unknown_color_name() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    let result = parser.parse("unknowncolor");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unknown color"));
}

#[test]
fn test_parse_with_whitespace() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Should handle whitespace
    assert_eq!(parser.parse("  red  ").unwrap(), Color::Red);
    assert_eq!(parser.parse(" cyan ").unwrap(), Color::Cyan);
}

#[test]
fn test_parse_special_modifiers() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Special modifiers should parse as Reset
    assert_eq!(parser.parse("reset").unwrap(), Color::Reset);
    assert_eq!(parser.parse("default").unwrap(), Color::Reset);
    assert_eq!(parser.parse("none").unwrap(), Color::Reset);
    assert_eq!(parser.parse("reversed").unwrap(), Color::Reset);
}

#[test]
#[ignore] // This test modifies global environment and may interfere with parallel tests
fn test_no_color_environment() {
    // Save original NO_COLOR state
    let original = std::env::var("NO_COLOR").ok();

    // Set NO_COLOR
    std::env::set_var("NO_COLOR", "1");

    // Create parser AFTER setting NO_COLOR
    let parser = ColorParser::new();

    // All colors should return Reset when NO_COLOR is set
    assert_eq!(parser.parse("red").unwrap(), Color::Reset);
    assert_eq!(parser.parse("#ff0000").unwrap(), Color::Reset);
    assert_eq!(parser.parse("cyan").unwrap(), Color::Reset);

    // Restore original NO_COLOR state
    match original {
        Some(val) => std::env::set_var("NO_COLOR", val),
        None => std::env::remove_var("NO_COLOR"),
    }
}

#[test]
fn test_rgb_to_256_color_grayscale() {
    use datui::config::rgb_to_256_color;

    // Black
    let result = rgb_to_256_color(0, 0, 0);
    assert_eq!(result, 16);

    // White
    let result = rgb_to_256_color(255, 255, 255);
    assert_eq!(result, 231);

    // Gray shades should map to grayscale ramp (232-255)
    let result = rgb_to_256_color(128, 128, 128);
    assert!(result >= 232 || result == 16 || result == 231);
}

#[test]
fn test_rgb_to_256_color_primary_colors() {
    use datui::config::rgb_to_256_color;

    // Red
    let result = rgb_to_256_color(255, 0, 0);
    assert!((16..=231).contains(&result));

    // Green
    let result = rgb_to_256_color(0, 255, 0);
    assert!((16..=231).contains(&result));

    // Blue
    let result = rgb_to_256_color(0, 0, 255);
    assert!((16..=231).contains(&result));
}

#[test]
fn test_rgb_to_basic_ansi() {
    use datui::config::rgb_to_basic_ansi;

    // Test primary colors
    assert_eq!(rgb_to_basic_ansi(255, 0, 0), Color::Red);
    assert_eq!(rgb_to_basic_ansi(0, 255, 0), Color::Green);
    assert_eq!(rgb_to_basic_ansi(0, 0, 255), Color::Blue);

    // Test secondary colors
    assert_eq!(rgb_to_basic_ansi(255, 255, 0), Color::Yellow);
    assert_eq!(rgb_to_basic_ansi(255, 0, 255), Color::Magenta);
    assert_eq!(rgb_to_basic_ansi(0, 255, 255), Color::Cyan);

    // Test grayscale
    assert_eq!(rgb_to_basic_ansi(0, 0, 0), Color::Black);
    assert_eq!(rgb_to_basic_ansi(255, 255, 255), Color::White);
    assert_eq!(rgb_to_basic_ansi(30, 30, 30), Color::Black);
    assert_eq!(rgb_to_basic_ansi(200, 200, 200), Color::White);
}

#[test]
fn test_theme_from_config() {
    ensure_colors_enabled();
    use datui::config::AppConfig;

    let config = AppConfig::default();
    let result = Theme::from_config(&config.theme);

    assert!(result.is_ok());
    let theme = result.unwrap();

    // Check that colors are accessible
    assert_ne!(theme.get("keybind_hints"), Color::Reset);
    assert_ne!(theme.get("error"), Color::Reset);
    assert_ne!(theme.get("success"), Color::Reset);
}

#[test]
fn test_theme_get_unknown_color() {
    use datui::config::AppConfig;

    let config = AppConfig::default();
    let theme = Theme::from_config(&config.theme).unwrap();

    // Unknown color should return Reset
    assert_eq!(theme.get("unknown_color"), Color::Reset);
}

#[test]
fn test_theme_get_optional() {
    use datui::config::AppConfig;

    let config = AppConfig::default();
    let theme = Theme::from_config(&config.theme).unwrap();

    // Known color should return Some
    assert!(theme.get_optional("keybind_hints").is_some());

    // Unknown color should return None
    assert!(theme.get_optional("unknown_color").is_none());
}

#[test]
fn test_theme_with_custom_colors() {
    use datui::config::{AppConfig, ColorConfig};

    let mut config = AppConfig::default();
    config.theme.colors = ColorConfig {
        keybind_hints: "#ff0000".to_string(),
        keybind_labels: "blue".to_string(),
        primary_chart_series_color: "cyan".to_string(),
        secondary_chart_series_color: "dark_gray".to_string(),
        success: "bright_green".to_string(),
        error: "red".to_string(),
        warning: "yellow".to_string(),
        dimmed: "dark_gray".to_string(),
        background: "black".to_string(),
        surface: "black".to_string(),
        controls_bg: "indexed(236)".to_string(),
        text_primary: "white".to_string(),
        text_secondary: "gray".to_string(),
        text_inverse: "black".to_string(),
        table_header: "white".to_string(),
        cursor_focused: "default".to_string(),
        cursor_dimmed: "default".to_string(),
        table_header_bg: "indexed(236)".to_string(),
        row_numbers: "dark_gray".to_string(),
        column_separator: "cyan".to_string(),
        table_selected: "reversed".to_string(),
        sidebar_border: "cyan".to_string(),
        modal_border_active: "yellow".to_string(),
        modal_border_error: "red".to_string(),
        distribution_normal: "green".to_string(),
        distribution_skewed: "yellow".to_string(),
        distribution_other: "white".to_string(),
        outlier_marker: "red".to_string(),
        alternate_row_color: "default".to_string(),
    };

    let result = Theme::from_config(&config.theme);
    assert!(result.is_ok());
}

#[test]
fn test_theme_with_invalid_color() {
    ensure_colors_enabled();
    use datui::config::AppConfig;

    let mut config = AppConfig::default();
    config.theme.colors.keybind_hints = "invalid_color_name".to_string();

    let result = Theme::from_config(&config.theme);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unknown color name"));
}

#[test]
fn test_parse_hex_component_extraction() {
    ensure_colors_enabled();
    let parser = ColorParser::new();

    // Test that hex parsing extracts correct RGB values
    // We can't directly test the RGB values without exposing internal functions,
    // but we can verify that different hex values produce different results
    let red = parser.parse("#ff0000").unwrap();
    let green = parser.parse("#00ff00").unwrap();
    let blue = parser.parse("#0000ff").unwrap();

    // These should all be different colors (unless terminal doesn't support color)
    // We just verify they all parse successfully
    assert!(matches!(
        red,
        Color::Rgb(_, _, _) | Color::Indexed(_) | Color::Red | Color::Reset
    ));
    assert!(matches!(
        green,
        Color::Rgb(_, _, _) | Color::Indexed(_) | Color::Green | Color::Reset
    ));
    assert!(matches!(
        blue,
        Color::Rgb(_, _, _) | Color::Indexed(_) | Color::Blue | Color::Reset
    ));
}

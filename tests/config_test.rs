use datui::config::{
    AppConfig, ConfigManager, NumberFormatConfig, DEFAULT_CHART_ROW_LIMIT, MAX_CHART_ROW_LIMIT,
};
use datui::numfmt::{Grouping, NumberFormatSettings};
use polars::prelude::DataType;
use std::fs;
use tempfile::TempDir;

// Helper to create a temporary config directory for testing
fn setup_test_config_dir() -> (TempDir, ConfigManager) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config_manager = ConfigManager::with_dir(temp_dir.path().to_path_buf());
    (temp_dir, config_manager)
}

#[test]
fn test_default_config() {
    let config = AppConfig::default();

    // Check version
    assert_eq!(config.version, "0.2");

    // Check display defaults
    assert_eq!(config.display.pages_lookahead, 3);
    assert_eq!(config.display.pages_lookback, 3);
    assert!(!config.display.row_numbers);
    assert_eq!(config.display.row_start_index, 1);
    assert_eq!(config.display.table_cell_padding, 2);

    // Check performance defaults
    assert_eq!(config.performance.sampling_threshold, None);
    assert_eq!(config.performance.event_poll_interval_ms, 25);

    // Check theme defaults
    assert_eq!(config.theme.colors.keybind_hints, "cyan");
    assert_eq!(config.theme.colors.row_numbers, "dark_gray");
    assert_eq!(config.theme.colors.alternate_row_color, "indexed(235)");

    // Check query defaults
    assert_eq!(config.query.history_limit, 1000);
    assert!(config.query.enable_history);

    // Check template defaults
    assert!(!config.templates.auto_apply);

    // Check debug defaults
    assert!(!config.debug.enabled);
    assert!(config.debug.show_performance);
}

#[test]
fn test_generate_default_config() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    let template = config_manager.generate_default_config();

    // Check that template contains expected sections
    assert!(template.contains("[file_loading]"));
    assert!(template.contains("[display]"));
    assert!(template.contains("[performance]"));
    assert!(template.contains("[theme.colors]"));
    assert!(template.contains("[query]"));
    assert!(template.contains("[templates]"));
    assert!(template.contains("[debug]"));

    // Check that it contains version
    assert!(template.contains("version = \"0.2\""));
}

#[test]
fn test_write_default_config() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    let config_path = config_manager
        .write_default_config(false)
        .expect("Failed to write config");

    assert!(config_path.exists());

    // Read and verify content
    let content = fs::read_to_string(&config_path).expect("Failed to read config");
    assert!(content.contains("[display]"));
    assert!(content.contains("version = \"0.2\""));
}

#[test]
fn test_write_config_without_force_fails_if_exists() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    // Write once - should succeed
    config_manager
        .write_default_config(false)
        .expect("First write should succeed");

    // Write again without force - should fail
    let result = config_manager.write_default_config(false);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[test]
fn test_write_config_with_force_overwrites() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    // Write once
    let first_path = config_manager
        .write_default_config(false)
        .expect("First write should succeed");

    // Write again with force - should succeed
    let second_path = config_manager
        .write_default_config(true)
        .expect("Second write with force should succeed");

    assert_eq!(first_path, second_path);
    assert!(first_path.exists());
}

#[test]
fn test_load_config_with_no_file() {
    let _temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Create a temporary app name for this test
    let test_app_name = format!("datui_test_{}", std::process::id());

    // Override config dir temporarily by using a custom load function
    // Since AppConfig::load uses the app_name, we need to ensure no config file exists
    let config = AppConfig::load(&test_app_name).expect("Should load default config");

    // Should return default config
    assert_eq!(config.version, "0.2");
    assert_eq!(config.display.pages_lookahead, 3);
}

#[test]
fn test_load_and_parse_minimal_config() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    // Write a minimal config
    let config_path = config_manager.config_path("config.toml");
    config_manager
        .ensure_config_dir()
        .expect("Failed to create config dir");

    let minimal_config = r#"
version = "0.2"

[display]
row_numbers = true
row_start_index = 0
"#;

    fs::write(&config_path, minimal_config).expect("Failed to write minimal config");

    // Load config by reading directly (simulate AppConfig::load_user_config)
    let content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: AppConfig = toml::from_str(&content).expect("Failed to parse config");

    // Check that custom values are loaded
    assert_eq!(config.version, "0.2");
    assert!(config.display.row_numbers);
    assert_eq!(config.display.row_start_index, 0);

    // Check that defaults are still present for unspecified values
    assert_eq!(config.display.pages_lookahead, 3); // Default
    assert_eq!(config.performance.sampling_threshold, None); // Default: no sampling
}

#[test]
fn test_merge_configs() {
    let mut base = AppConfig::default();
    let mut override_config = AppConfig::default();

    // Modify override config
    override_config.display.row_numbers = true;
    override_config.display.pages_lookahead = 5;
    override_config.performance.sampling_threshold = Some(50000);
    override_config.theme.colors.keybind_hints = "blue".to_string();

    // Merge
    base.merge(override_config);

    // Check that values were merged
    assert!(base.display.row_numbers);
    assert_eq!(base.display.pages_lookahead, 5);
    assert_eq!(base.performance.sampling_threshold, Some(50000));
    assert_eq!(base.theme.colors.keybind_hints, "blue");

    // Check that unmodified values remain default
    assert_eq!(base.display.pages_lookback, 3); // Still default
    assert_eq!(base.query.history_limit, 1000); // Still default
}

#[test]
fn test_validate_config_valid() {
    let config = AppConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_validate_config_invalid_version() {
    let config = AppConfig {
        version: "1.0".to_string(),
        ..Default::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unsupported config version"));
}

#[test]
fn test_validate_config_zero_sampling_threshold() {
    let mut config = AppConfig::default();
    config.performance.sampling_threshold = Some(0);

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("sampling_threshold must be greater than 0 when set"));
}

#[test]
fn test_chart_config_default_and_validation() {
    let config = AppConfig::default();
    assert_eq!(config.chart.row_limit, Some(DEFAULT_CHART_ROW_LIMIT));

    let mut invalid = AppConfig::default();
    invalid.chart.row_limit = Some(0);
    assert!(invalid.validate().is_err());
    invalid.chart.row_limit = Some(MAX_CHART_ROW_LIMIT + 1);
    assert!(invalid.validate().is_err());

    let mut unlimited = AppConfig::default();
    unlimited.chart.row_limit = None;
    assert!(unlimited.validate().is_ok());
}

#[test]
fn test_validate_config_zero_event_poll_interval() {
    let mut config = AppConfig::default();
    config.performance.event_poll_interval_ms = 0;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("event_poll_interval_ms must be greater than 0"));
}

#[test]
fn test_parse_full_config() {
    // Clear NO_COLOR for color validation
    std::env::remove_var("NO_COLOR");

    let full_config = r##"
version = "0.2"

[file_loading]
delimiter = 44
has_header = true
skip_lines = 1
skip_rows = 0

[display]
pages_lookahead = 5
pages_lookback = 5
row_numbers = true
row_start_index = 0

[performance]
sampling_threshold = 50000
event_poll_interval_ms = 50

[theme.colors]
keybind_hints = "blue"
keybind_labels = "magenta"
success = "bright_green"
error = "bright_red"
warning = "yellow"
dimmed = "gray"
background = "#1e1e1e"
surface = "#2d2d2d"
controls_bg = "#3a3a3a"
text_primary = "white"
text_secondary = "gray"
text_inverse = "black"
table_header = "white"
table_header_bg = "dark_gray"
column_separator = "blue"
table_selected = "reversed"
sidebar_border = "blue"
modal_border_active = "yellow"
modal_border_error = "red"
distribution_normal = "green"
distribution_skewed = "yellow"
distribution_other = "white"
outlier_marker = "red"

[ui.controls]
row_count_width = 25

[query]
history_limit = 500
enable_history = true

[templates]
auto_apply = true

[debug]
enabled = false
show_performance = true
show_query = true
show_transformations = true
"##;

    let config: AppConfig = toml::from_str(full_config).expect("Failed to parse full config");

    // Verify all sections
    assert_eq!(config.version, "0.2");
    assert_eq!(config.file_loading.delimiter, Some(44));
    assert_eq!(config.file_loading.has_header, Some(true));
    assert_eq!(config.display.pages_lookahead, 5);
    assert!(config.display.row_numbers);
    assert_eq!(config.performance.sampling_threshold, Some(50000));
    assert_eq!(config.theme.colors.keybind_hints, "blue");
    assert_eq!(config.ui.controls.row_count_width, 25);
    assert_eq!(config.query.history_limit, 500);
    assert!(config.templates.auto_apply);

    // Validate
    assert!(config.validate().is_ok());
}

#[test]
fn test_merge_option_fields() {
    use datui::config::FileLoadingConfig;

    let mut base = FileLoadingConfig::default();
    assert_eq!(base.delimiter, None);
    assert_eq!(base.has_header, None);

    let override_config = FileLoadingConfig {
        delimiter: Some(44),
        has_header: Some(true),
        ..Default::default()
    };

    base.merge(override_config);

    assert_eq!(base.delimiter, Some(44));
    assert_eq!(base.has_header, Some(true));
}

#[test]
fn test_merge_does_not_override_with_defaults() {
    use datui::config::DisplayConfig;

    let mut base = DisplayConfig {
        unicode: Default::default(),
        pages_lookahead: 5,
        pages_lookback: 5,
        max_buffered_rows: 100_000,
        max_buffered_mb: 512,
        row_numbers: true,
        row_start_index: 0,
        table_cell_padding: 1,
        column_colors: true,
        sidebar_width: None,
        align_numeric_right: false,
        number_format: NumberFormatConfig::Preset("thousands".to_string()),
    };

    let override_config = DisplayConfig::default();

    base.merge(override_config);

    // Base values should remain unchanged because override had defaults
    assert_eq!(base.pages_lookahead, 5);
    assert_eq!(base.pages_lookback, 5);
    assert!(base.row_numbers);
    assert_eq!(base.row_start_index, 0);
    // Non-default number formatting must survive a merge of defaults too.
    assert!(!base.align_numeric_right);
    assert_eq!(
        base.number_format,
        NumberFormatConfig::Preset("thousands".to_string())
    );
}

#[test]
fn test_color_config_merge() {
    use datui::config::ColorConfig;

    let mut base = ColorConfig::default();
    let override_config = ColorConfig {
        keybind_hints: "blue".to_string(),
        error: "bright_red".to_string(),
        ..Default::default()
    };

    base.merge(override_config);

    assert_eq!(base.keybind_hints, "blue");
    assert_eq!(base.error, "bright_red");
    // Other colors should remain default
    assert_eq!(base.keybind_labels, "indexed(252)");
    assert_eq!(base.success, "green");
}

#[test]
fn test_new_color_fields() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    use datui::config::{AppConfig, Theme};
    use ratatui::style::Color;

    // Test that new color fields have correct defaults
    let config = AppConfig::default();
    assert_eq!(config.theme.colors.primary_chart_series_color, "cyan");
    assert_eq!(
        config.theme.colors.secondary_chart_series_color,
        "indexed(245)"
    );
    // Chart view series colors
    assert_eq!(config.theme.colors.chart_series_color_1, "cyan");
    assert_eq!(config.theme.colors.chart_series_color_2, "magenta");
    assert_eq!(config.theme.colors.chart_series_color_3, "green");
    assert_eq!(config.theme.colors.chart_series_color_4, "yellow");
    assert_eq!(config.theme.colors.chart_series_color_5, "blue");
    assert_eq!(config.theme.colors.chart_series_color_6, "red");
    assert_eq!(config.theme.colors.chart_series_color_7, "bright_cyan");
    assert_eq!(config.theme.colors.controls_bg, "indexed(235)");
    assert_eq!(config.theme.colors.table_header_bg, "indexed(235)");
    assert_eq!(config.theme.colors.column_separator, "cyan");
    assert_eq!(config.theme.colors.sidebar_border, "indexed(235)");

    // Test that new colors can be parsed and retrieved from theme
    let theme = Theme::from_config(&config.theme).unwrap();
    if std::env::var("NO_COLOR").is_err() {
        assert_ne!(theme.get("primary_chart_series_color"), Color::Reset);
        assert_ne!(theme.get("secondary_chart_series_color"), Color::Reset);
        assert_ne!(theme.get("chart_series_color_1"), Color::Reset);
        assert_ne!(theme.get("chart_series_color_7"), Color::Reset);
        // controls_bg and table_header_bg default to indexed(235)
        assert_eq!(theme.get("controls_bg"), Color::Indexed(235));
        assert_eq!(theme.get("table_header_bg"), Color::Indexed(235));
        assert_ne!(theme.get("column_separator"), Color::Reset);
        assert_ne!(theme.get("sidebar_border"), Color::Reset);
    }
}

#[test]
fn test_new_color_fields_custom_values() {
    use datui::config::AppConfig;

    let mut config = AppConfig::default();
    config.theme.colors.primary_chart_series_color = "#00ff00".to_string();
    config.theme.colors.secondary_chart_series_color = "#ff00ff".to_string();
    config.theme.colors.table_header_bg = "indexed(240)".to_string();
    config.theme.colors.column_separator = "bright_blue".to_string();
    config.theme.colors.sidebar_border = "bright_red".to_string();

    // Should validate successfully
    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_config_with_invalid_chart_series_color() {
    std::env::remove_var("NO_COLOR");
    let mut config = AppConfig::default();
    config.theme.colors.chart_series_color_1 = "invalid_color_name".to_string();
    let result = config.validate();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("theme.colors.chart_series_color_1"), "{}", err);
}

#[test]
fn test_validate_config_with_invalid_color() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.keybind_hints = "not_a_valid_color".to_string();

    let result = config.validate();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("theme.colors.keybind_hints"), "{}", err);
}

#[test]
fn test_validate_config_with_valid_hex_color() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.keybind_hints = "#ff0000".to_string();
    config.theme.colors.keybind_labels = "#00ff00".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_config_with_mixed_colors() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.keybind_hints = "cyan".to_string();
    config.theme.colors.error = "#ff0000".to_string();
    config.theme.colors.success = "bright_green".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_template_sampling_threshold_default_none() {
    // Default is None (no sampling); template and Rust default must match
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template_str = config_manager.generate_default_config();

    let template_config: AppConfig =
        toml::from_str(&template_str).expect("Template should be valid TOML");

    assert_eq!(
        template_config.performance.sampling_threshold, None,
        "Template sampling_threshold should default to None (no sampling)"
    );

    let rust_default = AppConfig::default();
    assert_eq!(
        rust_default.performance.sampling_threshold, None,
        "Rust default sampling_threshold should be None"
    );
}

// ---------------------------------------------------------------------------
// Number formatting (display.number_format / display.align_numeric_right)
// ---------------------------------------------------------------------------

#[test]
fn test_number_format_defaults_are_inert() {
    let config = AppConfig::default();

    // Default must render exactly as before, so upgrading changes nothing.
    assert_eq!(
        config.display.number_format,
        NumberFormatConfig::Preset("none".to_string())
    );
    let settings = config
        .display
        .number_format
        .resolve(config.display.align_numeric_right)
        .expect("default config must resolve");
    // Inert because formatting starts off -- not because there is nothing to
    // apply. Every column resolves to Passthrough while disabled.
    assert!(!settings.enabled);
    assert!(settings
        .formatter_for("pos", &DataType::Int64)
        .is_passthrough());

    // F still needs something to turn on, so the toggle target is Thousands.
    let toggled = NumberFormatSettings {
        enabled: true,
        ..settings.clone()
    };
    assert_eq!(toggled.format.grouping, Grouping::Thousands);
    assert!(!toggled
        .formatter_for("pos", &DataType::Int64)
        .is_passthrough());

    // Alignment, unlike grouping, is on by default: it changes neither the
    // characters of a value nor a column's width.
    assert!(config.display.align_numeric_right);
    assert!(settings.align_numeric_right);
}

#[test]
fn test_configured_format_starts_enabled() {
    // A user who configured a format wants to see it without pressing F.
    let settings = NumberFormatConfig::Preset("thousands".to_string())
        .resolve(true)
        .unwrap();
    assert!(settings.enabled);
    assert_eq!(settings.format.grouping, Grouping::Thousands);
}

#[test]
fn test_toggle_target_keeps_user_settings_when_grouping_is_none() {
    // Grouping off but min_digits and excludes configured: pressing F must
    // honour those, not reset to a bare preset.
    let toml_str = r#"
version = "0.2"

[display.number_format]
grouping = "none"
group_separator = "_"
exclude_columns = ["*_id"]
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let settings = config.display.number_format.resolve(true).unwrap();

    assert!(!settings.enabled, "grouping = none should start off");
    assert_eq!(settings.format.grouping, Grouping::Thousands);
    assert_eq!(settings.format.group_sep, '_');
    assert_eq!(settings.exclude.len(), 1);
}

#[test]
fn test_non_grouping_format_still_starts_enabled() {
    // Decimal separator alone is a real change, so it applies immediately
    // rather than being treated as "nothing configured".
    let toml_str = r#"
version = "0.2"

[display.number_format]
grouping = "none"
decimal_separator = ","
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let settings = config.display.number_format.resolve(true).unwrap();

    assert!(settings.enabled);
    // Grouping stays off: the user asked only for a decimal separator.
    assert_eq!(settings.format.grouping, Grouping::None);
    assert_eq!(settings.format.decimal_sep, ',');
}

#[test]
fn test_number_format_preset_shorthand() {
    let toml_str = r#"
version = "0.2"

[display]
number_format = "thousands"
"#;
    let config: AppConfig = toml::from_str(toml_str).expect("shorthand form should parse");
    assert_eq!(
        config.display.number_format,
        NumberFormatConfig::Preset("thousands".to_string())
    );

    let settings = config.display.number_format.resolve(true).unwrap();
    assert_eq!(settings.format.grouping, Grouping::Thousands);
    assert_eq!(settings.format.group_sep, ',');
}

#[test]
fn test_number_format_table_form() {
    let toml_str = r#"
version = "0.2"

[display.number_format]
grouping = "thousands"
group_separator = " "
decimal_separator = ","
floats = false
float_precision = 2
exclude_columns = ["*_id", "year"]
"#;
    let config: AppConfig = toml::from_str(toml_str).expect("table form should parse");
    let settings = config.display.number_format.resolve(false).unwrap();

    assert_eq!(settings.format.grouping, Grouping::Thousands);
    assert_eq!(settings.format.group_sep, ' ');
    assert_eq!(settings.format.decimal_sep, ',');
    assert!(!settings.format.floats);
    assert_eq!(settings.format.float_precision, Some(2));
    assert_eq!(settings.exclude.len(), 2);
    assert!(!settings.align_numeric_right);
}

#[test]
fn test_number_format_all_presets_resolve() {
    for name in [
        "none",
        "thousands",
        "european",
        "si",
        "swiss",
        "indian",
        "underscore",
    ] {
        let cfg = NumberFormatConfig::Preset(name.to_string());
        assert!(cfg.resolve(true).is_ok(), "preset {name} should resolve");
    }
}

#[test]
fn test_number_format_unknown_preset_is_rejected() {
    let cfg = NumberFormatConfig::Preset("klingon".to_string());
    let err = cfg.resolve(true).unwrap_err().to_string();
    assert!(err.contains("unknown value 'klingon'"), "got: {err}");
    // The message must list what IS valid.
    assert!(err.contains("thousands"), "got: {err}");
    assert!(err.contains("system"), "got: {err}");
}

#[test]
fn test_number_format_separator_conflict_is_rejected() {
    let toml_str = r#"
version = "0.2"

[display.number_format]
grouping = "thousands"
group_separator = "."
decimal_separator = "."
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("must differ"), "got: {err}");
}

#[test]
fn test_number_format_multichar_separator_is_rejected() {
    let toml_str = r#"
version = "0.2"

[display.number_format]
group_separator = ", "
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("single character"), "got: {err}");
}

#[test]
fn test_number_format_bad_value_fails_validation_not_parsing() {
    // A bad preset name is a valid TOML string, so it must be caught by
    // validate() (which reports the config file path) rather than silently
    // falling back at render time.
    let toml_str = r#"
version = "0.2"

[display]
number_format = "nonsense"
"#;
    let config: AppConfig = toml::from_str(toml_str).expect("should parse as a string");
    assert!(config.validate().is_err());
}

#[test]
fn test_number_format_merge_overrides_default() {
    let mut base = AppConfig::default();
    let toml_str = r#"
version = "0.2"

[display]
number_format = "indian"
align_numeric_right = false
"#;
    let user: AppConfig = toml::from_str(toml_str).unwrap();
    base.merge(user);

    assert_eq!(
        base.display.number_format,
        NumberFormatConfig::Preset("indian".to_string())
    );
    assert!(!base.display.align_numeric_right);
}

#[test]
fn test_number_format_merge_keeps_existing_when_user_omits() {
    let mut base = AppConfig::default();
    base.display.number_format = NumberFormatConfig::Preset("european".to_string());

    // A user config that says nothing about number_format must not reset it.
    let user: AppConfig = toml::from_str("version = \"0.2\"\n").unwrap();
    base.merge(user);

    assert_eq!(
        base.display.number_format,
        NumberFormatConfig::Preset("european".to_string())
    );
}

#[test]
fn test_generated_config_documents_number_format() {
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();

    // The generated config is the main discovery surface: it must show the
    // shorthand, the long form, and the runtime toggle.
    assert!(template.contains("number_format = \"none\""));
    assert!(template.contains("[display.number_format]"));
    assert!(template.contains("align_numeric_right = true"));
    assert!(template.contains("Press F"));
    assert!(template.contains("exclude_columns"));
    // No magnitude threshold: the comment must point at exclude_columns as the
    // way to leave identifier columns alone.
    assert!(!template.contains("min_digits"));
    assert!(!template.contains("include_columns"));
    assert!(template.contains("identifiers rather than quantities"));

    // Generated configs must not carry trailing whitespace.
    for (i, line) in template.lines().enumerate() {
        assert_eq!(
            line,
            line.trim_end(),
            "trailing whitespace on line {}",
            i + 1
        );
    }

    // And the whole thing must still round-trip as valid TOML.
    let parsed: AppConfig = toml::from_str(&template).expect("generated config must parse");
    parsed.validate().expect("generated config must validate");
}

#[test]
fn test_number_format_unknown_key_is_reported() {
    // A misspelled key must not be silently ignored. Every field has a default,
    // so an ignored typo would resolve to "no formatting" -- identical to the
    // default config, leaving the user no way to tell the difference.
    let toml_str = r#"
version = "0.2"

[display.number_format]
groupng = "thousands"
"#;
    let config: AppConfig = toml::from_str(toml_str).expect("should still parse");
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("unknown key 'groupng'"), "got: {err}");
    // The message must say what IS accepted.
    assert!(err.contains("grouping"), "got: {err}");
    assert!(err.contains("exclude_columns"), "got: {err}");
}

#[test]
fn test_number_format_reports_every_unknown_key() {
    let toml_str = r#"
version = "0.2"

[display.number_format]
groupng = "thousands"
floatz = true
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("unknown keys"), "should pluralise: {err}");
    assert!(err.contains("'groupng'"), "got: {err}");
    assert!(err.contains("'floatz'"), "got: {err}");
}

#[test]
fn test_number_format_known_keys_are_not_flagged_as_unknown() {
    // Guard against the unknown-key capture swallowing real fields.
    let toml_str = r#"
version = "0.2"

[display.number_format]
grouping = "thousands"
group_separator = "_"
decimal_separator = "."
floats = false
float_precision = 3
exclude_columns = ["year"]
"#;
    let config: AppConfig = toml::from_str(toml_str).unwrap();
    config.validate().expect("all known keys must validate");
    let settings = config.display.number_format.resolve(true).unwrap();
    assert_eq!(settings.format.group_sep, '_');
    assert_eq!(settings.format.float_precision, Some(3));
    assert!(!settings.format.floats);
    assert_eq!(settings.exclude.len(), 1);
}

#[test]
fn test_number_format_wrong_types_still_fail_at_parse_time() {
    // Capturing unknown keys must not turn type errors into generic ones.
    for bad in [
        "[display.number_format]\nfloat_precision = \"two\"\n",
        "[display.number_format]\nfloats = \"yes\"\n",
        "[display]\nnumber_format = 7\n",
    ] {
        let src = format!("version = \"0.2\"\n{bad}");
        let parsed = toml::from_str::<AppConfig>(&src);
        assert!(parsed.is_err(), "should fail to parse: {bad}");
    }
}

// ============================================================================
// Config `import` layering
//
// `import` is what lets an external theme system (Omarchy, chezmoi, a dotfiles
// repo) drop a generated file into place and have datui pick it up, without
// datui knowing anything about that system.
// ============================================================================

/// Write `contents` to `dir/name` and return the path.
fn write_config(dir: &TempDir, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, contents).expect("Failed to write config");
    path
}

#[test]
fn test_import_applies_theme_colors() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "theme.toml",
        "[theme.colors]\nsuccess = \"#00ff00\"\nerror = \"#ff0000\"\n",
    );
    let root = write_config(&temp_dir, "config.toml", "import = [\"theme.toml\"]\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.colors.success, "#00ff00");
    assert_eq!(config.theme.colors.error, "#ff0000");
}

#[test]
fn test_import_is_overridden_by_importing_file() {
    // The whole point of the precedence order: a user keeps their own tweaks
    // even while following a generated theme.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "theme.toml",
        "[theme.colors]\nsuccess = \"#00ff00\"\nerror = \"#ff0000\"\n",
    );
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"theme.toml\"]\n\n[theme.colors]\nerror = \"#123456\"\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.colors.error, "#123456", "user value must win");
    assert_eq!(
        config.theme.colors.success, "#00ff00",
        "untouched imported value must survive"
    );
}

#[test]
fn test_imports_apply_in_declaration_order() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "first.toml",
        "[theme.colors]\nsuccess = \"#111111\"\nerror = \"#aaaaaa\"\n",
    );
    write_config(
        &temp_dir,
        "second.toml",
        "[theme.colors]\nsuccess = \"#222222\"\n",
    );
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"first.toml\", \"second.toml\"]\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.colors.success, "#222222", "later import wins");
    assert_eq!(config.theme.colors.error, "#aaaaaa");
}

#[test]
fn test_nested_import_is_merged_before_its_importer() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "base.toml",
        "[theme.colors]\nsuccess = \"#111111\"\nerror = \"#aaaaaa\"\n",
    );
    write_config(
        &temp_dir,
        "mid.toml",
        "import = [\"base.toml\"]\n\n[theme.colors]\nsuccess = \"#222222\"\n",
    );
    let root = write_config(&temp_dir, "config.toml", "import = [\"mid.toml\"]\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(
        config.theme.colors.success, "#222222",
        "importer beats importee"
    );
    assert_eq!(
        config.theme.colors.error, "#aaaaaa",
        "nested value reaches the top"
    );
}

#[test]
fn test_missing_import_is_skipped_not_fatal() {
    // The Omarchy state directory does not exist on a machine that has never
    // set a theme. datui must still start.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"nope.toml\"]\n\n[display]\nrow_numbers = true\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Missing import must not be fatal");

    assert!(
        config.display.row_numbers,
        "rest of the config still applies"
    );
    assert_eq!(
        config.theme.colors.success,
        AppConfig::default().theme.colors.success
    );
}

#[test]
fn test_import_relative_path_resolves_against_importing_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let nested = temp_dir.path().join("themes");
    fs::create_dir(&nested).expect("Failed to create dir");
    fs::write(
        nested.join("dark.toml"),
        "[theme.colors]\nsuccess = \"#00ff00\"\n",
    )
    .expect("Failed to write theme");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"themes/dark.toml\"]\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.colors.success, "#00ff00");
}

#[test]
fn test_import_expands_env_var() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let theme = write_config(
        &temp_dir,
        "theme.toml",
        "[theme.colors]\nsuccess = \"#00ff00\"\n",
    );
    // Unique name: tests in a binary share one process environment.
    std::env::set_var("DATUI_TEST_IMPORT_DIR", temp_dir.path());
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"$DATUI_TEST_IMPORT_DIR/theme.toml\"]\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");
    std::env::remove_var("DATUI_TEST_IMPORT_DIR");

    assert!(theme.exists());
    assert_eq!(config.theme.colors.success, "#00ff00");
}

#[test]
fn test_circular_import_is_an_error() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(&temp_dir, "a.toml", "import = [\"b.toml\"]\n");
    write_config(&temp_dir, "b.toml", "import = [\"a.toml\"]\n");
    let root = write_config(&temp_dir, "config.toml", "import = [\"a.toml\"]\n");

    let err = AppConfig::load_from_file(&root).expect_err("cycle must be reported");

    assert!(
        err.to_string().contains("circular config import"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_self_import_is_an_error() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(&temp_dir, "config.toml", "import = [\"config.toml\"]\n");

    let err = AppConfig::load_from_file(&root).expect_err("self-import must be reported");

    assert!(
        err.to_string().contains("circular config import"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_deep_import_chain_is_capped() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    // 20 files, each importing the next: past any legitimate use.
    for i in 0..20 {
        write_config(
            &temp_dir,
            &format!("l{i}.toml"),
            &format!("import = [\"l{}.toml\"]\n", i + 1),
        );
    }
    write_config(
        &temp_dir,
        "l20.toml",
        "[theme.colors]\nsuccess = \"#00ff00\"\n",
    );
    let root = write_config(&temp_dir, "config.toml", "import = [\"l0.toml\"]\n");

    let err = AppConfig::load_from_file(&root).expect_err("depth cap must trigger");

    assert!(err.to_string().contains("deep"), "unexpected error: {err}");
}

#[test]
fn test_unparseable_import_is_an_error() {
    // Unlike a missing file, a file the user named that is actually broken must
    // be loud — otherwise it just looks like the theme silently not applying.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(&temp_dir, "theme.toml", "this is not toml =\n");
    let root = write_config(&temp_dir, "config.toml", "import = [\"theme.toml\"]\n");

    let err = AppConfig::load_from_file(&root).expect_err("broken import must be reported");
    let msg = err.to_string();

    assert!(msg.contains("Failed to parse"), "unexpected error: {msg}");
    assert!(
        msg.contains("theme.toml"),
        "error must name the file: {msg}"
    );
}

#[test]
fn test_imported_invalid_color_is_reported() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "theme.toml",
        "[theme.colors]\nsuccess = \"#not-a-color\"\n",
    );
    let root = write_config(&temp_dir, "config.toml", "import = [\"theme.toml\"]\n");

    let err = AppConfig::load_from_file(&root).expect_err("invalid color must be reported");

    assert!(
        err.to_string().contains("theme.colors.success"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_config_without_import_is_unchanged() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(&temp_dir, "config.toml", "[display]\nrow_numbers = true\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert!(config.display.row_numbers);
    assert!(config.import.is_empty());
}

#[test]
fn test_load_from_missing_config_file_yields_defaults() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = temp_dir.path().join("config.toml");

    let config = AppConfig::load_from_file(&root).expect("Missing config is not an error");

    assert_eq!(config.version, AppConfig::default().version);
    assert!(config.import.is_empty());
}

#[test]
fn test_generated_config_documents_import() {
    // The generated config is the discovery surface for this feature.
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();

    assert!(template.contains("import"));
    assert!(template.contains("omarchy/current/theme/datui.toml"));

    // Still valid TOML with the new key present.
    let parsed: AppConfig = toml::from_str(&template).expect("Template should be valid TOML");
    assert!(parsed.import.is_empty());
}

// ============================================================================
// Theme mode (light / dark chrome defaults)
//
// datui's chrome slots (header fills, row striping, borders, dim text) resolve
// to fixed shades because no ANSI colour means "slightly off from the
// background". A set tuned for a dark terminal is unreadable on a light one.
// ============================================================================

use datui::config::{ColorConfig, ThemeMode};

#[test]
fn test_default_mode_is_dark_chrome() {
    // Existing configs must not change appearance: the stock palette stays dark.
    let config = AppConfig::default();
    assert_eq!(config.theme.colors, ColorConfig::dark());
    assert_eq!(config.theme.colors.table_header_bg, "indexed(235)");
}

#[test]
fn test_light_mode_selects_light_chrome() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(&temp_dir, "config.toml", "[theme]\nmode = \"light\"\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.mode, Some(ThemeMode::Light));
    assert_eq!(config.theme.colors, ColorConfig::light());
}

#[test]
fn test_light_chrome_inverts_rather_than_lightens() {
    // The bug this fixes: fixed dark shades on a light terminal. The light set's
    // fills must be near-white, not near-black.
    let light = ColorConfig::light();
    for (name, value) in [
        ("table_header_bg", &light.table_header_bg),
        ("alternate_row_color", &light.alternate_row_color),
        ("controls_bg", &light.controls_bg),
    ] {
        let n: u8 = value
            .trim_start_matches("indexed(")
            .trim_end_matches(')')
            .parse()
            .unwrap_or_else(|_| panic!("{name} should be an indexed colour, got {value}"));
        assert!(
            n >= 250,
            "{name} must be a near-white fill on a light terminal, got indexed({n})"
        );
    }
    assert_eq!(ColorConfig::dark().table_header_bg, "indexed(235)");
}

#[test]
fn test_explicit_colors_override_light_mode() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "[theme]\nmode = \"light\"\n\n[theme.colors]\ntable_header_bg = \"#123456\"\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.colors.table_header_bg, "#123456");
    // Untouched slots still come from the light set.
    assert_eq!(
        config.theme.colors.alternate_row_color,
        ColorConfig::light().alternate_row_color
    );
}

#[test]
fn test_mode_can_come_from_an_import() {
    // A theme file can declare the polarity it was built for.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(&temp_dir, "theme.toml", "[theme]\nmode = \"light\"\n");
    let root = write_config(&temp_dir, "config.toml", "import = [\"theme.toml\"]\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.mode, Some(ThemeMode::Light));
    assert_eq!(config.theme.colors, ColorConfig::light());
}

#[test]
fn test_own_mode_beats_imported_mode() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(&temp_dir, "theme.toml", "[theme]\nmode = \"light\"\n");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"theme.toml\"]\n\n[theme]\nmode = \"dark\"\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.mode, Some(ThemeMode::Dark));
    assert_eq!(config.theme.colors, ColorConfig::dark());
}

#[test]
fn test_light_palette_is_valid_and_complete() {
    // Every light value must parse, and none may be left at its dark counterpart
    // by accident where the two sets are meant to differ.
    let mut config = AppConfig::default();
    config.theme.colors = ColorConfig::light();
    config.validate().expect("light palette must validate");

    let dark = ColorConfig::dark();
    let light = ColorConfig::light();
    assert_ne!(light.table_header_bg, dark.table_header_bg);
    assert_ne!(light.controls_bg, dark.controls_bg);
    assert_ne!(light.alternate_row_color, dark.alternate_row_color);
    assert_ne!(light.keybind_labels, dark.keybind_labels);
}

// ============================================================================
// Shipped Omarchy template
// ============================================================================

#[test]
fn test_omarchy_template_covers_every_color_slot() {
    // The template maps datui's colour slots onto an Omarchy palette. If a slot is
    // added to ColorConfig and not to the template, the generated theme silently
    // leaves that slot at datui's default — which is exactly the kind of drift a
    // human reviewer will not catch. Fail here instead.
    let template = fs::read_to_string("contrib/omarchy/datui.toml.tpl")
        .expect("contrib/omarchy/datui.toml.tpl should exist");

    let serialized = toml::to_string(&ColorConfig::default()).expect("serialize");
    let expected: std::collections::BTreeSet<String> = serialized
        .lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, _)| k.trim().to_string())
        .collect();

    let found: std::collections::BTreeSet<String> = template
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(|l| l.split_once('='))
        .map(|(k, _)| k.trim().to_string())
        .collect();

    let missing: Vec<_> = expected.difference(&found).collect();
    assert!(
        missing.is_empty(),
        "template is missing colour slots: {missing:?}"
    );

    let unknown: Vec<_> = found
        .difference(&expected)
        .filter(|k| *k != "mode")
        .collect();
    assert!(
        unknown.is_empty(),
        "template sets slots that do not exist in ColorConfig: {unknown:?}"
    );
}

// ============================================================================
// History files (query history, recents)
// ============================================================================

#[test]
fn test_history_write_is_atomic_and_exact() {
    // A truncate-then-write leaves the file readable half-finished, and two datui
    // instances writing at once interleave into one corrupt file — entries torn
    // mid-path, or two paths concatenated onto a single line. Both were observed.
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());

    let entries: Vec<String> = (0..200)
        .map(|i| format!("/some/quite/long/path/number-{i:04}/dataset.parquet"))
        .collect();
    cache
        .save_history_file("things", &entries)
        .expect("save history");

    let read_back = cache.load_history_file("things").expect("load history");
    assert_eq!(read_back, entries);

    // No stray temp files left behind.
    let leftovers: Vec<_> = fs::read_dir(temp_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp files left behind: {leftovers:?}"
    );
}

#[test]
fn test_recents_deduplicate_and_cap() {
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());

    let a = temp_dir.path().join("a.parquet");
    let b = temp_dir.path().join("b.parquet");
    fs::write(&a, b"x").unwrap();
    fs::write(&b, b"x").unwrap();

    cache.push_recent(&a);
    cache.push_recent(&b);
    cache.push_recent(&a);

    let recents = cache.load_recents();
    assert_eq!(recents.len(), 2, "reopening moves rather than duplicates");
    assert!(recents[0].ends_with("a.parquet"), "most recent leads");
}

#[test]
fn test_concurrent_recents_do_not_lose_entries() {
    // Opening two datasets at once is ordinary — a launcher, a file manager, two
    // terminals. Writing atomically stops the file becoming corrupt, but not one
    // instance's entry being overwritten by another's; the read and the write have
    // to be a single locked operation.
    use datui::CacheManager;
    use std::sync::Arc;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = Arc::new(CacheManager::with_dir(temp_dir.path().to_path_buf()));

    let paths: Vec<std::path::PathBuf> = (0..16)
        .map(|i| {
            let p = temp_dir.path().join(format!("dataset-{i:02}.parquet"));
            fs::write(&p, b"x").unwrap();
            p
        })
        .collect();

    let handles: Vec<_> = paths
        .iter()
        .cloned()
        .map(|path| {
            let cache = Arc::clone(&cache);
            std::thread::spawn(move || cache.push_recent(&path))
        })
        .collect();
    for h in handles {
        h.join().expect("writer thread");
    }

    let recents = cache.load_recents();
    assert_eq!(
        recents.len(),
        paths.len(),
        "every concurrent open should survive; got {recents:#?}"
    );

    // And every line is a whole, valid path — never two concatenated or one torn.
    for entry in &recents {
        assert!(
            entry.exists(),
            "history holds a path that is not a real file: {entry:?}"
        );
    }
}

#[test]
fn test_history_update_is_dropped_rather_than_blocking() {
    // A stuck peer must never wedge a writer forever. It used to also have to never
    // delay an *open*, which is why the deadline was a quarter of a second -- but
    // recording a recent now happens off the opening path, so nothing is waiting on
    // this and the deadline is free to clear real contention by a wide margin.
    // Sixteen writers on a Windows runner did not clear 250ms, and giving up means
    // silently dropping somebody's entry.
    use datui::CacheManager;
    use fs2::FileExt;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());
    cache.ensure_cache_dir().unwrap();

    let lock_path = temp_dir.path().join("held_history.lock");
    let holder = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    holder.lock_exclusive().unwrap();

    let started = std::time::Instant::now();
    let result = cache.update_history_file("held", |entries| entries.push("nope".into()));
    let elapsed = started.elapsed();

    FileExt::unlock(&holder).unwrap();

    assert!(
        result.is_ok(),
        "a contended update is skipped, not an error"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(6),
        "gave up after {elapsed:?}; a held lock must be abandoned, never waited on \
         forever"
    );
    assert!(
        elapsed >= std::time::Duration::from_secs(1),
        "gave up after only {elapsed:?}; too eager a deadline silently drops entries \
         that a handful of simultaneous opens would have written fine"
    );
    assert!(
        cache
            .load_history_file("held")
            .unwrap_or_default()
            .is_empty(),
        "the update should have been dropped"
    );
}

#[test]
fn test_recents_store_urls_verbatim() {
    // Canonicalising a URL is meaningless, and it would stat a path that does not
    // exist locally.
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());

    let url = std::path::PathBuf::from("s3://bucket/warehouse/events/year=2024");
    cache.push_recent(&url);

    let recents = cache.load_recents();
    assert_eq!(recents, vec![url], "a URL should round-trip unchanged");
}

#[test]
fn test_a_single_recent_can_be_forgotten() {
    // A recents list you cannot edit is one people stop trusting: an experiment, a
    // file that would not open, something private — all land there, and clearing the
    // whole cache to remove one is too blunt.
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());

    let keep = temp_dir.path().join("keep.parquet");
    let drop = temp_dir.path().join("private.csv");
    fs::write(&keep, b"x").unwrap();
    fs::write(&drop, b"x").unwrap();
    cache.push_recent(&keep);
    cache.push_recent(&drop);

    cache.forget_recent(&drop.canonicalize().unwrap());

    let recents = cache.load_recents();
    assert!(recents.iter().any(|p| p.ends_with("keep.parquet")));
    assert!(
        !recents.iter().any(|p| p.ends_with("private.csv")),
        "the forgotten entry should be gone: {recents:?}"
    );
}

#[test]
fn test_clearing_recents_leaves_other_caches_alone() {
    // `--clear-cache` is too blunt for "forget where I have been": it would also
    // discard query history and every measurement, costing speed for no reason.
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());

    let dataset = temp_dir.path().join("a.parquet");
    fs::write(&dataset, b"x").unwrap();
    cache.push_recent(&dataset);
    cache
        .save_history_file("query", &["select 1".to_string()])
        .unwrap();

    cache.clear_recents();

    assert!(cache.load_recents().is_empty(), "recents should be gone");
    assert_eq!(
        cache.load_history_file("query").unwrap(),
        vec!["select 1".to_string()],
        "query history should survive"
    );
}

#[test]
fn test_the_generated_default_config_is_valid_toml() {
    // It ships as the file people edit. A default config that does not parse is the
    // worst possible first impression, and the only thing standing between the two is
    // that every rendered line gets commented -- including the ones a multi-line array
    // spills onto.
    let manager = ConfigManager::new("datui").unwrap();
    let generated = manager.generate_default_config();

    toml::from_str::<toml::Value>(&generated)
        .expect("the generated default config must parse as TOML");

    // Uncommented content would be a bug; every line is either blank or a comment.
    for (n, line) in generated.lines().enumerate() {
        assert!(
            line.trim().is_empty() || line.trim_start().starts_with('#'),
            "line {} of the generated config is live rather than commented: {line:?}",
            n + 1
        );
    }
}

#[test]
fn test_a_multi_line_array_default_is_fully_commented() {
    // `skip` renders across ten lines. Commenting only the first left the elements
    // behind as bare text, which does not parse.
    let manager = ConfigManager::new("datui").unwrap();
    let generated = manager.generate_default_config();

    assert!(
        generated.contains("# skip = ["),
        "the skip list should appear in the generated config"
    );
    assert!(
        generated.contains("#     \"node_modules\","),
        "array elements must be commented too"
    );
}

#[test]
fn test_search_settings_round_trip_through_toml() {
    let toml = r#"
[data.search]
enabled = false
max_depth = 3
skip_extra = ["archive"]
extensions = ["parquet"]
"#;
    let config: AppConfig = toml::from_str(toml).unwrap();
    assert!(!config.data.search.enabled);
    assert_eq!(config.data.search.max_depth, 3);
    assert_eq!(config.data.search.skip_extra, vec!["archive".to_string()]);
    assert_eq!(config.data.search.extensions, vec!["parquet".to_string()]);
    // Untouched fields keep their defaults, and skip_extra adds to skip rather than
    // replacing it.
    assert!(!config.data.search.cross_filesystems);
    assert!(config
        .data
        .search
        .skipped_dirs()
        .contains(&"node_modules".to_string()));
    assert!(config
        .data
        .search
        .skipped_dirs()
        .contains(&"archive".to_string()));
}

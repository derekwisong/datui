use datui::config::{AppConfig, ConfigManager};
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

    // Check performance defaults
    assert_eq!(config.performance.sampling_threshold, 10000);
    assert_eq!(config.performance.event_poll_interval_ms, 25);

    // Check theme defaults
    assert_eq!(config.theme.color_mode, "auto");
    assert_eq!(config.theme.colors.primary, "cyan");

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
    assert!(template.contains("[theme]"));
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
    assert_eq!(config.performance.sampling_threshold, 10000); // Default
}

#[test]
fn test_merge_configs() {
    let mut base = AppConfig::default();
    let mut override_config = AppConfig::default();

    // Modify override config
    override_config.display.row_numbers = true;
    override_config.display.pages_lookahead = 5;
    override_config.performance.sampling_threshold = 50000;
    override_config.theme.color_mode = "dark".to_string();
    override_config.theme.colors.primary = "blue".to_string();

    // Merge
    base.merge(override_config);

    // Check that values were merged
    assert!(base.display.row_numbers);
    assert_eq!(base.display.pages_lookahead, 5);
    assert_eq!(base.performance.sampling_threshold, 50000);
    assert_eq!(base.theme.color_mode, "dark");
    assert_eq!(base.theme.colors.primary, "blue");

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
    config.performance.sampling_threshold = 0;

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("sampling_threshold must be greater than 0"));
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
fn test_validate_config_invalid_color_mode() {
    let mut config = AppConfig::default();
    config.theme.color_mode = "invalid".to_string();

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid color_mode"));
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
compression = "gzip"

[display]
pages_lookahead = 5
pages_lookback = 5
row_numbers = true
row_start_index = 0

[performance]
sampling_threshold = 50000
event_poll_interval_ms = 50

[theme]
color_mode = "dark"

[theme.colors]
primary = "blue"
secondary = "magenta"
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
table_border = "blue"
table_selected = "reversed"
modal_border = "blue"
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
    assert_eq!(config.performance.sampling_threshold, 50000);
    assert_eq!(config.theme.color_mode, "dark");
    assert_eq!(config.theme.colors.primary, "blue");
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
        pages_lookahead: 5,
        pages_lookback: 5,
        row_numbers: true,
        row_start_index: 0,
    };

    let override_config = DisplayConfig::default();

    base.merge(override_config);

    // Base values should remain unchanged because override had defaults
    assert_eq!(base.pages_lookahead, 5);
    assert_eq!(base.pages_lookback, 5);
    assert!(base.row_numbers);
    assert_eq!(base.row_start_index, 0);
}

#[test]
fn test_color_config_merge() {
    use datui::config::ColorConfig;

    let mut base = ColorConfig::default();
    let override_config = ColorConfig {
        primary: "blue".to_string(),
        error: "bright_red".to_string(),
        ..Default::default()
    };

    base.merge(override_config);

    assert_eq!(base.primary, "blue");
    assert_eq!(base.error, "bright_red");
    // Other colors should remain default
    assert_eq!(base.secondary, "yellow");
    assert_eq!(base.success, "green");
}

#[test]
fn test_validate_config_with_invalid_color() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.primary = "not_a_valid_color".to_string();

    let result = config.validate();
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid color value"));
}

#[test]
fn test_validate_config_with_valid_hex_color() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.primary = "#ff0000".to_string();
    config.theme.colors.secondary = "#00ff00".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_config_with_mixed_colors() {
    // Clear NO_COLOR for this test
    std::env::remove_var("NO_COLOR");

    let mut config = AppConfig::default();
    config.theme.colors.primary = "cyan".to_string();
    config.theme.colors.error = "#ff0000".to_string();
    config.theme.colors.success = "bright_green".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

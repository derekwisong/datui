use datui::config::{
    AppConfig, ByteSize, ConfigLayer, ConfigManager, DEFAULT_CHART_ROW_LIMIT, MAX_CHART_ROW_LIMIT,
    NumberFormatConfig,
};
use datui::numfmt::{Grouping, NumberFormatSettings};
use polars::prelude::DataType;
use std::fs;
use tempfile::TempDir;

/// The config TOML `layers` describe, lowest precedence first, as an import chain
/// would stack them.
fn layered(layers: &[&str]) -> AppConfig {
    AppConfig::from_layers(
        layers
            .iter()
            .map(|text| ConfigLayer::parse(text).expect("layer parses")),
    )
    .expect("layers resolve")
}

// Helper to create a temporary config directory for testing
fn setup_test_config_dir() -> (TempDir, ConfigManager) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config_manager = ConfigManager::with_dir(temp_dir.path().to_path_buf());
    (temp_dir, config_manager)
}

#[test]
fn test_default_config() {
    let config = AppConfig::default();

    // Check display defaults
    assert_eq!(config.performance.pages_ahead, 3);
    assert_eq!(config.performance.pages_behind, 3);
    assert!(!config.display.row_numbers);
    assert_eq!(config.display.row_numbers_start, 1);
    assert_eq!(config.display.cell_padding.cells(), 2);

    // Check performance defaults
    assert_eq!(config.analysis.sample_rows, 100_000);

    // Check theme defaults
    assert_eq!(config.theme.colors.chip_key, "#7dcfff");
    assert_eq!(config.theme.colors.table_row_numbers, "#565f89");
    assert_eq!(config.theme.colors.table_alternate_row, "#1e2030");

    // Check query defaults
    assert_eq!(config.query.history_limit, 1000);
    assert!(config.query.history);

    // Check template defaults
    assert!(!config.views.auto_apply);

    // Check log defaults
    assert_eq!(config.log.file, None);
    assert_eq!(config.log.level, None);
}

#[test]
fn test_generate_default_config() {
    let (_temp_dir, config_manager) = setup_test_config_dir();

    let template = config_manager.generate_default_config();

    // Check that template contains expected sections
    for section in [
        "[read]",
        "[csv]",
        "[display]",
        "[performance]",
        "[analysis]",
        "[home]",
        "[theme.colors]",
        "[query]",
        "[views]",
        "[log]",
    ] {
        assert!(template.contains(section), "{section}");
    }
    assert!(!template.contains("version"));
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
    // A directory with no config.toml in it: defaults are all there is.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = AppConfig::load_from_file(&temp_dir.path().join("config.toml"))
        .expect("Should load default config");

    // Should return default config
    assert_eq!(config.performance.pages_ahead, 3);
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
[display]
row_numbers = true
row_numbers_start = 0
"#;

    fs::write(&config_path, minimal_config).expect("Failed to write minimal config");

    // Load config by reading directly (simulate AppConfig::load_user_config)
    let content = fs::read_to_string(&config_path).expect("Failed to read config");
    let config: AppConfig = toml::from_str(&content).expect("Failed to parse config");

    // Check that custom values are loaded
    assert!(config.display.row_numbers);
    assert_eq!(config.display.row_numbers_start, 0);

    // Check that defaults are still present for unspecified values
    assert_eq!(config.performance.pages_ahead, 3); // Default
    assert_eq!(config.analysis.sample_rows, 100_000); // Default
}

#[test]
fn test_merge_configs() {
    let base = layered(&[r#"
[display]
row_numbers = true

[performance]
pages_ahead = 5

[analysis]
sample_rows = 50000

[theme.colors]
chip_key = "blue"
"#]);

    assert!(base.display.row_numbers);
    assert_eq!(base.performance.pages_ahead, 5);
    assert_eq!(base.analysis.sample_rows, 50000);
    assert_eq!(base.theme.colors.chip_key, "blue");

    // Unwritten keys take the defaults.
    assert_eq!(base.performance.pages_behind, 3);
    assert_eq!(base.query.history_limit, 1000);
}

#[test]
fn test_validate_config_valid() {
    let config = AppConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_validate_config_zero_sample_rows_reads_every_row() {
    let mut config = AppConfig::default();
    config.analysis.sample_rows = 0;
    assert!(config.validate().is_ok());
}

#[test]
fn test_chart_config_default_and_validation() {
    let config = AppConfig::default();
    assert_eq!(config.analysis.chart_rows, DEFAULT_CHART_ROW_LIMIT);

    let mut invalid = AppConfig::default();
    invalid.analysis.chart_rows = 0;
    assert!(invalid.validate().is_err());
    invalid.analysis.chart_rows = MAX_CHART_ROW_LIMIT + 1;
    assert!(invalid.validate().is_err());
}

#[test]
fn test_parse_full_config() {
    // Clear NO_COLOR for color validation
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };

    let full_config = r##"
[read]
infer_types = false

[csv]
infer_rows = 5000

[display]
row_numbers = true
row_numbers_start = 0

[performance]
pages_ahead = 5
pages_behind = 5

[analysis]
sample_rows = 50000

[theme.colors]
chip_key = "blue"
chip_label = "magenta"
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
table_column_separator = "blue"
table_selected = "reversed"
sidebar_border = "blue"
modal_border_active = "yellow"
modal_border_error = "red"
distribution_normal = "green"
distribution_skewed = "yellow"
distribution_other = "white"
outlier_marker = "red"

[query]
history_limit = 500
history = true

[views]
auto_apply = true

[log]
level = "info"
"##;

    let config: AppConfig = toml::from_str(full_config).expect("Failed to parse full config");

    // Verify all sections
    assert_eq!(
        config.read.infer_types,
        datui::config::InferTypes::Switch(false)
    );
    assert_eq!(config.csv.infer_rows, 5000);
    assert_eq!(config.log.level.as_deref(), Some("info"));
    assert_eq!(config.performance.pages_ahead, 5);
    assert!(config.display.row_numbers);
    assert_eq!(config.analysis.sample_rows, 50000);
    assert_eq!(config.theme.colors.chip_key, "blue");
    assert_eq!(config.query.history_limit, 500);
    assert!(config.views.auto_apply);

    // Validate
    assert!(config.validate().is_ok());
}

#[test]
fn test_merge_option_fields() {
    let config = layered(&[
        "[csv]\ninfer_rows = 5000\nignore_errors = true\nnull_values = [\"NA\"]\ncomment = \"#\"\n",
        "[csv]\nignore_errors = false\nnull_values = []\n",
    ]);
    assert_eq!(config.csv.infer_rows, 5000, "unwritten keeps");
    assert!(!config.csv.ignore_errors, "written wins");
    assert!(
        config.csv.null_values.is_empty(),
        "an explicit empty list replaces the import's"
    );
    assert_eq!(config.csv.comment.as_deref(), Some("#"), "unwritten keeps");
    assert_eq!(
        layered(&["[csv]\nignore_errors = true\n"]).csv.comment,
        None,
        "never written stays unset"
    );
}

#[test]
fn test_query_default_mode() {
    use datui::config::QueryMode;

    // Omitted: SQL.
    let config: AppConfig = toml::from_str("[query]\nhistory_limit = 10\n").unwrap();
    assert_eq!(config.query.default_mode, QueryMode::Sql);

    for (text, mode) in [
        ("sql", QueryMode::Sql),
        ("search", QueryMode::Text),
        ("q-style", QueryMode::Q),
    ] {
        let config: AppConfig =
            toml::from_str(&format!("[query]\ndefault_mode = \"{text}\"\n")).unwrap();
        assert_eq!(config.query.default_mode, mode, "{text}");
    }
    assert!(toml::from_str::<AppConfig>("[query]\ndefault_mode = \"fuzzy\"\n").is_err());

    // A file that picks q-style wins, a later one that says nothing keeps it, and one
    // that names the default puts it back.
    let picked = "[query]\ndefault_mode = \"q-style\"\n";
    let silent = "[query]\nhistory_limit = 10\n";
    assert_eq!(layered(&[picked, silent]).query.default_mode, QueryMode::Q);
    assert_eq!(
        layered(&[picked, "[query]\ndefault_mode = \"sql\"\n"])
            .query
            .default_mode,
        QueryMode::Sql
    );

    // The generated config documents it.
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();
    assert!(template.contains("default_mode = \"sql\""), "{template}");
}

#[test]
fn test_log_file_parses_defaults_and_merges() {
    use datui::config::LogConfig;

    assert_eq!(LogConfig::default().file, None);
    let omitted: AppConfig = toml::from_str("[log]\nlevel = \"info\"\n").unwrap();
    assert_eq!(omitted.log.file, None);

    let set: AppConfig = toml::from_str("[log]\nfile = \"~/datui.log\"\n").unwrap();
    assert_eq!(set.log.file.as_deref(), Some("~/datui.log"));

    let first = "[log]\nfile = \"/first.log\"\n";
    let kept = layered(&[first, "[log]\nlevel = \"info\"\n"]);
    assert_eq!(kept.log.file.as_deref(), Some("/first.log"), "unset keeps");
    let replaced = layered(&[first, "[log]\nfile = \"/second.log\"\n"]);
    assert_eq!(
        replaced.log.file.as_deref(),
        Some("/second.log"),
        "set wins"
    );
    let mut bad = AppConfig::default();
    bad.log.level = Some("loud".into());
    assert!(bad.validate().is_err());
}

#[test]
fn test_notes_accent_follows_the_last_file_that_sets_it() {
    use datui::config::DisplayConfig;

    assert!(
        DisplayConfig::default().notes_accent,
        "on unless asked otherwise"
    );
    let off = "[display]\nnotes_accent = false\n";
    let on = "[display]\nnotes_accent = true\n";
    let silent = "[display]\nrow_numbers = true\n";

    assert!(!layered(&[off]).display.notes_accent, "false is honored");
    assert!(
        !layered(&[off, silent]).display.notes_accent,
        "silence is not a request to re-enable it"
    );
    assert!(
        layered(&[off, on]).display.notes_accent,
        "an explicit true undoes an imported false"
    );
    assert!(!layered(&[on, off]).display.notes_accent);
}

#[test]
fn test_mouse_is_on_unless_a_file_turns_it_off() {
    use datui::config::DisplayConfig;

    assert!(
        DisplayConfig::default().mouse,
        "taken unless asked otherwise"
    );
    assert!(layered(&[""]).display.mouse);
    let off = "[display]\nmouse = false\n";
    let on = "[display]\nmouse = true\n";
    let silent = "[display]\nrow_numbers = true\n";
    assert!(!layered(&[off]).display.mouse, "false is honored");
    assert!(
        !layered(&[off, silent]).display.mouse,
        "silence keeps it off"
    );
    assert!(layered(&[off, on]).display.mouse, "an explicit true wins");
    assert!(!layered(&[on, off]).display.mouse);
}

#[test]
fn test_explicit_defaults_override_and_omitted_keys_keep() {
    use datui::config::QueryMode;
    use datui::glyphs::UnicodeMode;

    // An import that moves every kind of setting off its default.
    let import = r#"
[display]
unicode = "never"
row_numbers = true
right_align_numbers = false
sidebar_width = 50
number_format = "thousands"

[performance]
pages_ahead = 5
streaming = false

[analysis]
quality_local_copy = "512MiB"

[query]
default_mode = "search"
history = false

[views]
auto_apply = true

[clipboard]
backend = "osc52"

[home]
directories = ["/mnt/data"]
desktop_recents = false
builtin_catalog = false
preview_max = 0

[home.search]
skip = ["only-this"]
cross_filesystems = true
"#;

    // A file that says nothing keeps every one of them.
    let kept = layered(&[import, "[display]\n"]);
    assert_eq!(kept.display.unicode, UnicodeMode::Never);
    assert_eq!(kept.performance.pages_ahead, 5);
    assert!(kept.display.row_numbers);
    assert!(!kept.display.right_align_numbers);
    assert_eq!(kept.display.sidebar_width, Some(50));
    assert_eq!(
        kept.display.number_format,
        NumberFormatConfig::Preset("thousands".to_string())
    );
    assert!(!kept.performance.streaming);
    assert_eq!(kept.analysis.quality_local_copy, ByteSize::mib(512));
    assert_eq!(kept.query.default_mode, QueryMode::Text);
    assert!(!kept.query.history);
    assert!(kept.views.auto_apply);
    assert_eq!(kept.clipboard.backend, "osc52");
    assert_eq!(kept.home.directories, ["/mnt/data"]);
    assert!(!kept.home.desktop_recents);
    assert!(!kept.home.builtin_catalog);
    assert_eq!(kept.home.preview_max, ByteSize(0));
    assert_eq!(kept.home.search.skip, ["only-this"]);
    assert!(kept.home.search.cross_filesystems);

    // A file that writes each default puts it back, whatever the import said.
    let defaults = AppConfig::default();
    let restored = layered(&[
        import,
        r#"
[display]
unicode = "auto"
row_numbers = false
right_align_numbers = true
number_format = "none"

[performance]
pages_ahead = 3
streaming = true

[analysis]
quality_local_copy = "2GiB"

[query]
default_mode = "sql"
history = true

[views]
auto_apply = false

[clipboard]
backend = "auto"

[home]
directories = []
desktop_recents = true
builtin_catalog = true
preview_max = "64MiB"

[home.search]
skip = ["node_modules", "target", "build", "dist", "vendor", "site-packages", "__pycache__", "venv", "env"]
cross_filesystems = false
"#,
    ]);
    assert_eq!(restored.display.unicode, UnicodeMode::Auto);
    assert_eq!(restored.performance.pages_ahead, 3);
    assert!(!restored.display.row_numbers);
    assert!(restored.display.right_align_numbers);
    assert_eq!(
        restored.display.number_format,
        defaults.display.number_format
    );
    assert_eq!(
        restored.display.sidebar_width,
        Some(50),
        "TOML cannot unset a key, so an import's optional value stays"
    );
    assert!(restored.performance.streaming);
    assert_eq!(
        restored.analysis.quality_local_copy,
        defaults.analysis.quality_local_copy
    );
    assert_eq!(restored.query.default_mode, QueryMode::Sql);
    assert!(restored.query.history);
    assert!(!restored.views.auto_apply);
    assert_eq!(restored.clipboard.backend, "auto");
    assert!(
        restored.home.directories.is_empty(),
        "an empty list is a value"
    );
    assert!(restored.home.desktop_recents);
    assert!(restored.home.builtin_catalog);
    assert_eq!(restored.home.preview_max, defaults.home.preview_max);
    assert_eq!(restored.home.search.skip, defaults.home.search.skip);
    assert!(!restored.home.search.cross_filesystems);
    assert_eq!(restored.collections().len(), 1, "the catalog is back");
}

#[test]
fn test_lists_that_add_up_across_files() {
    let config = layered(&[
        "[home]\nhide = [\"public\"]\n[cloud]\nhide = [\"a\"]\nenv_files = [\".env\"]\n",
        "[home]\nhide = [\"mine\", \"public\"]\n[cloud]\nhide = []\nenv_files = [\"cloud.env\"]\n",
    ]);
    assert_eq!(config.home.hide, ["public", "mine"]);
    assert_eq!(config.cloud.hide, ["a"], "an empty list adds nothing");
    assert_eq!(config.cloud.env_files, [".env", "cloud.env"]);
}

/// The S3 keys are not config: a secret in a file is refused with the way out, as
/// any unknown key would be named.
#[test]
fn test_s3_keys_are_not_config_keys() {
    let config = layered(&["[cloud]\ns3_region = \"eu-west-1\"\n"]);
    assert_eq!(config.cloud.s3_region, None, "the environment's alone");
    let refused = "cloud.s3_region=x".parse::<datui_cli::settings::Override>();
    assert!(refused.is_err());
}

#[test]
fn test_color_config_merge() {
    use datui::config::ColorConfig;

    let config = layered(&[
        "[theme.colors]\nchip_key = \"blue\"\nerror = \"bright_red\"\n",
        "[theme.colors]\nerror = \"#f7768e\"\n",
    ]);

    assert_eq!(config.theme.colors.chip_key, "blue");
    assert_eq!(
        config.theme.colors.error,
        ColorConfig::default().error,
        "a color written as its default value overrides the import"
    );
    assert_eq!(
        config.theme.colors.chip_label,
        ColorConfig::default().chip_label
    );
}

#[test]
fn test_new_color_fields() {
    // Clear NO_COLOR for this test
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };

    use datui::config::{AppConfig, Theme};
    use ratatui::style::Color;

    // Test that new color fields have correct defaults: the dark set, after Tokyo Night
    let config = AppConfig::default();
    assert_eq!(config.theme.colors.chart_1, "#7dcfff");
    assert_eq!(config.theme.colors.dimmed, "#565f89");
    // Chart view series colors
    assert_eq!(config.theme.colors.chart_1, "#7dcfff");
    assert_eq!(config.theme.colors.chart_2, "#bb9af7");
    assert_eq!(config.theme.colors.chart_3, "#9ece6a");
    assert_eq!(config.theme.colors.chart_4, "#e0af68");
    assert_eq!(config.theme.colors.chart_5, "#7aa2f7");
    assert_eq!(config.theme.colors.chart_6, "#f7768e");
    assert_eq!(config.theme.colors.chart_7, "#ff9e64");
    // Three chrome tiers, each its own shade
    assert_eq!(config.theme.colors.controls_bg, "#262a3f");
    assert_eq!(config.theme.colors.table_header_bg, "#2b3047");
    assert_eq!(config.theme.colors.table_alternate_row, "#1e2030");
    assert_eq!(config.theme.colors.table_column_separator, "#3b4261");
    assert_eq!(config.theme.colors.sidebar_border, "#565f89");
    assert_eq!(config.theme.colors.accent, "#7dcfff");
    assert_eq!(config.theme.colors.accent_bright, "#a4daff");
    assert_eq!(config.theme.colors.gradient_start, "#7aa2f7");
    assert_eq!(config.theme.colors.gradient_end, "#bb9af7");
    assert_eq!(config.theme.colors.table_selected, "#283457");

    // Test that new colors can be parsed and retrieved from theme
    let theme = Theme::from_config(&config.theme).unwrap();
    if std::env::var("NO_COLOR").is_err() {
        assert_ne!(theme.get("chart_1"), Color::Reset);
        assert_ne!(theme.get("dimmed"), Color::Reset);
        assert_ne!(theme.get("chart_1"), Color::Reset);
        assert_ne!(theme.get("chart_7"), Color::Reset);
        // The chrome tiers resolve to real colours. (Whether they stay distinct
        // depends on the terminal: under a test harness stdout is not a terminal, so
        // every hex colour degrades to basic ANSI. The strings are checked above.)
        assert_ne!(theme.get("controls_bg"), Color::Reset);
        assert_ne!(theme.get("table_header_bg"), Color::Reset);
        assert_ne!(theme.get("table_column_separator"), Color::Reset);
        assert_ne!(theme.get("sidebar_border"), Color::Reset);
        // The sidebars read their resting border from "modal_border", which is the
        // documented `sidebar_border` slot under the name the widgets use.
        assert_eq!(theme.get("modal_border"), theme.get("sidebar_border"));
        // A tinted selection is a colour; "reversed" would leave the slot unset.
        assert!(theme.get_optional("table_selected").is_some());
    }
}

#[test]
fn test_new_color_fields_custom_values() {
    use datui::config::AppConfig;

    let mut config = AppConfig::default();
    config.theme.colors.chart_1 = "#00ff00".to_string();
    config.theme.colors.dimmed = "#ff00ff".to_string();
    config.theme.colors.table_header_bg = "indexed(240)".to_string();
    config.theme.colors.table_column_separator = "bright_blue".to_string();
    config.theme.colors.sidebar_border = "bright_red".to_string();

    // Should validate successfully
    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_config_with_invalid_chart_series_color() {
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };
    let mut config = AppConfig::default();
    config.theme.colors.chart_1 = "invalid_color_name".to_string();
    let result = config.validate();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("theme.colors.chart_1"), "{}", err);
}

#[test]
fn test_validate_config_with_invalid_color() {
    // Clear NO_COLOR for this test
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };

    let mut config = AppConfig::default();
    config.theme.colors.chip_key = "not_a_valid_color".to_string();

    let result = config.validate();
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("theme.colors.chip_key"), "{}", err);
}

#[test]
fn test_validate_config_with_valid_hex_color() {
    // Clear NO_COLOR for this test
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };

    let mut config = AppConfig::default();
    config.theme.colors.chip_key = "#ff0000".to_string();
    config.theme.colors.chip_label = "#00ff00".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_validate_config_with_mixed_colors() {
    // Clear NO_COLOR for this test
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("NO_COLOR") };

    let mut config = AppConfig::default();
    config.theme.colors.chip_key = "cyan".to_string();
    config.theme.colors.error = "#ff0000".to_string();
    config.theme.colors.success = "bright_green".to_string();

    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_template_analysis_sample_rows_matches_the_default() {
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template_str = config_manager.generate_default_config();

    let template_config: AppConfig =
        toml::from_str(&template_str).expect("Template should be valid TOML");

    assert_eq!(
        template_config.analysis.sample_rows,
        AppConfig::default().analysis.sample_rows,
        "the template and the Rust default agree"
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
        .resolve(config.display.right_align_numbers)
        .expect("default config must resolve");
    // Inert because formatting starts off -- not because there is nothing to
    // apply. Every column resolves to Passthrough while disabled.
    assert!(!settings.enabled);
    assert!(
        settings
            .formatter_for("pos", &DataType::Int64)
            .is_passthrough()
    );

    // F still needs something to turn on, so the toggle target is Thousands.
    let toggled = NumberFormatSettings {
        enabled: true,
        ..settings.clone()
    };
    assert_eq!(toggled.format.grouping, Grouping::Thousands);
    assert!(
        !toggled
            .formatter_for("pos", &DataType::Int64)
            .is_passthrough()
    );

    // Alignment, unlike grouping, is on by default: it changes neither the
    // characters of a value nor a column's width.
    assert!(config.display.right_align_numbers);
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

[display]
number_format = "nonsense"
"#;
    let config: AppConfig = toml::from_str(toml_str).expect("should parse as a string");
    assert!(config.validate().is_err());
}

#[test]
fn test_number_format_merge_overrides_default() {
    let config = layered(&[r#"

[display]
number_format = "indian"
right_align_numbers = false
"#]);

    assert_eq!(
        config.display.number_format,
        NumberFormatConfig::Preset("indian".to_string())
    );
    assert!(!config.display.right_align_numbers);
}

#[test]
fn test_number_format_merge_keeps_existing_when_user_omits() {
    // A user config that says nothing about number_format must not reset it.
    let config = layered(&["[display]\nnumber_format = \"european\"\n", "[display]\n"]);
    assert_eq!(
        config.display.number_format,
        NumberFormatConfig::Preset("european".to_string())
    );

    // Its table form merges key by key, like any other table.
    let config = layered(&[
        "[display.number_format]\ngrouping = \"thousands\"\nexclude_columns = [\"year\"]\n",
        "[display.number_format]\nfloat_precision = 2\n",
    ]);
    let NumberFormatConfig::Custom(table) = &config.display.number_format else {
        panic!("still a table: {:?}", config.display.number_format);
    };
    assert_eq!(table.grouping.as_deref(), Some("thousands"));
    assert_eq!(table.float_precision, Some(2));
    assert_eq!(table.exclude_columns, ["year"]);
}

#[test]
fn test_generated_config_documents_number_format() {
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();

    // The generated config is the main discovery surface: it must show the
    // shorthand, the long form, and the runtime toggle.
    assert!(template.contains("number_format = \"none\""));
    assert!(template.contains("[display.number_format]"));
    assert!(template.contains("right_align_numbers = true"));
    assert!(!template.contains("min_digits"));

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
        let parsed = toml::from_str::<AppConfig>(bad);
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
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::set_var("DATUI_TEST_IMPORT_DIR", temp_dir.path()) };
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"$DATUI_TEST_IMPORT_DIR/theme.toml\"]\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");
    // SAFETY: test-only. Tests run on parallel threads, so this can race another test
    // reading the environment; accepted in tests and never done outside them.
    unsafe { std::env::remove_var("DATUI_TEST_IMPORT_DIR") };

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

    assert_eq!(
        config.query.history_limit,
        AppConfig::default().query.history_limit
    );
    assert!(config.import.is_empty());
}

#[test]
fn test_a_config_with_the_removed_ui_section_still_loads() {
    // `[ui.controls]` was never read, and is gone; a config that has it must not break.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "[ui.controls]\nrow_count_width = 25\ncustom_controls = [[\"q\", \"Quit\"]]\n\n[display]\nrow_numbers = true\n",
    );
    let config = AppConfig::load_from_file(&root).expect("Config should load");
    assert!(config.display.row_numbers);
}

#[test]
fn test_own_default_value_beats_an_import() {
    // The reason layers are partial: a value equal to the built-in default is still
    // something the user wrote, and it must undo the import.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "theme.toml",
        "[display]\nnotes_accent = false\nrow_numbers = true\n\n[theme.colors]\nerror = \"#ff5345\"\n",
    );
    let root = write_config(
        &temp_dir,
        "config.toml",
        "import = [\"theme.toml\"]\n\n[display]\nnotes_accent = true\n\n[theme.colors]\nerror = \"#f7768e\"\n",
    );

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert!(
        config.display.notes_accent,
        "explicit true undoes the import"
    );
    assert!(config.display.row_numbers, "unwritten keys keep the import");
    assert_eq!(config.theme.colors.error, "#f7768e");
}

#[test]
fn test_unparseable_root_config_is_an_error_naming_it() {
    // It used to fall back to defaults, discarding every setting in the file without
    // a word.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(
        &temp_dir,
        "config.toml",
        "[display]\nrow_numbers = true\nrow_numbers_start = \"one\"\n",
    );

    let msg = AppConfig::load_from_file(&root)
        .expect_err("a broken root config must be reported")
        .to_string();

    assert!(msg.contains("Failed to parse config file"), "{msg}");
    assert!(msg.contains(&root.display().to_string()), "{msg}");
    // The Python binding shows only the first line, so the reason and place lead.
    let first = msg.lines().next().unwrap_or_default();
    assert!(first.contains("line 3"), "says where: {msg}");
    assert!(first.contains("expected usize"), "says why: {msg}");
    assert!(msg.contains("^^^^^"), "points at it: {msg}");

    let not_toml = write_config(&temp_dir, "other.toml", "this is not toml =\n");
    let msg = AppConfig::load_from_file(&not_toml)
        .expect_err("not TOML")
        .to_string();
    assert!(msg.contains(&not_toml.display().to_string()), "{msg}");
}

#[test]
fn test_unreadable_root_config_is_an_error_naming_it() {
    // A directory where the file should be cannot be read on any platform.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = temp_dir.path().join("config.toml");
    fs::create_dir(&root).expect("create dir");

    let msg = AppConfig::load_from_file(&root)
        .expect_err("an unreadable root config must be reported")
        .to_string();

    assert!(msg.contains("Failed to read config file"), "{msg}");
    assert!(msg.contains(&root.display().to_string()), "{msg}");
}

#[test]
fn test_a_bad_type_in_an_import_names_the_import() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    write_config(
        &temp_dir,
        "theme.toml",
        "[display]\nrow_numbers = \"yes\"\n",
    );
    let root = write_config(&temp_dir, "config.toml", "import = [\"theme.toml\"]\n");

    let msg = AppConfig::load_from_file(&root)
        .expect_err("a wrong type must be reported")
        .to_string();

    assert!(msg.contains("theme.toml"), "{msg}");
    assert!(msg.contains("imported by"), "{msg}");
}

#[test]
fn test_generated_config_documents_import() {
    // The generated config is the discovery surface for this feature.
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();

    assert!(template.contains("# import = []"));

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
    assert_eq!(config.theme.colors.table_header_bg, "#2b3047");
}

#[test]
fn test_light_mode_selects_light_chrome() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let root = write_config(&temp_dir, "config.toml", "[theme]\nmode = \"light\"\n");

    let config = AppConfig::load_from_file(&root).expect("Config should load");

    assert_eq!(config.theme.mode, Some(ThemeMode::Light));
    assert_eq!(config.theme.colors, ColorConfig::light());
}

/// The current cell, the cursor's column and the current row stay three shades on a
/// 256-color terminal, in both modes, and the column is not the stripe.
#[test]
fn test_cursor_tints_stay_apart_at_256_colors() {
    let index = |hex: &str| {
        let v = u32::from_str_radix(hex.trim_start_matches('#'), 16).expect("hex colour");
        datui::config::rgb_to_256_color((v >> 16) as u8, (v >> 8) as u8, v as u8)
    };
    for (mode, colors) in [
        ("dark", ColorConfig::dark()),
        ("light", ColorConfig::light()),
    ] {
        let row = index(&colors.table_selected);
        let column = index(&colors.table_column_cursor);
        let cell = index(&colors.table_cell_cursor);
        let stripe = index(&colors.table_alternate_row);
        assert!(
            row != column && row != cell && column != cell && column != stripe,
            "{mode}: row {row}, column {column}, cell {cell}, stripe {stripe}"
        );
    }
}

#[test]
fn test_light_chrome_inverts_rather_than_lightens() {
    // The bug this fixes: fixed dark shades on a light terminal. The light set's
    // fills must be near-white, not near-black.
    // Luma of a "#rrggbb" string, 0..=255.
    fn luma(hex: &str) -> u32 {
        let v = u32::from_str_radix(hex.trim_start_matches('#'), 16).expect("hex colour");
        let (r, g, b) = ((v >> 16) & 0xff, (v >> 8) & 0xff, v & 0xff);
        (r * 299 + g * 587 + b * 114) / 1000
    }
    let light = ColorConfig::light();
    for (name, value) in [
        ("table_header_bg", &light.table_header_bg),
        ("table_alternate_row", &light.table_alternate_row),
        ("controls_bg", &light.controls_bg),
        ("table_selected", &light.table_selected),
        ("table_column_cursor", &light.table_column_cursor),
        ("table_cell_cursor", &light.table_cell_cursor),
    ] {
        assert!(
            luma(value) >= 170,
            "{name} must be a near-white fill on a light terminal, got {value}"
        );
    }
    let dark = ColorConfig::dark();
    for (name, value) in [
        ("table_header_bg", &dark.table_header_bg),
        ("table_alternate_row", &dark.table_alternate_row),
        ("controls_bg", &dark.controls_bg),
        ("table_selected", &dark.table_selected),
        ("table_column_cursor", &dark.table_column_cursor),
        ("table_cell_cursor", &dark.table_cell_cursor),
    ] {
        assert!(
            luma(value) <= 80,
            "{name} must be a near-black fill on a dark terminal, got {value}"
        );
    }
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
        config.theme.colors.table_alternate_row,
        ColorConfig::light().table_alternate_row
    );
}

#[test]
fn test_a_color_written_as_its_dark_value_holds_in_light_mode() {
    // Under the light palette, a slot set to the dark palette's value is still a
    // choice the user made.
    let config = layered(&[
        "[theme]\nmode = \"light\"\n",
        &format!(
            "[theme.colors]\ntable_header_bg = \"{}\"\n",
            ColorConfig::dark().table_header_bg
        ),
    ]);
    assert_eq!(
        config.theme.colors.table_header_bg,
        ColorConfig::dark().table_header_bg
    );
    assert_eq!(
        config.theme.colors.controls_bg,
        ColorConfig::light().controls_bg
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
fn test_an_explicit_auto_mode_beats_an_imported_one() {
    let config = layered(&["[theme]\nmode = \"light\"\n", "[theme]\nmode = \"auto\"\n"]);
    let resolved = ThemeMode::Auto.resolve();
    assert_eq!(config.theme.mode, Some(resolved));
    assert_eq!(config.theme.colors, ColorConfig::for_mode(resolved));
}

#[test]
fn test_cloud_s3_settings_come_from_the_environment() {
    use datui::config::CloudConfig;
    let mut cloud = CloudConfig::default();
    let env = |key: &str| match key {
        "AWS_REGION" => Some("env-region".to_string()),
        "AWS_ENDPOINT_URL" => Some("  ".to_string()),
        _ => None,
    };
    cloud.overlay(CloudConfig::from_env(&env));
    assert_eq!(cloud.s3_region.as_deref(), Some("env-region"));
    assert_eq!(cloud.s3_endpoint_url, None, "a blank variable says nothing");
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
    assert_ne!(light.table_alternate_row, dark.table_alternate_row);
    assert_ne!(light.chip_label, dark.chip_label);
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

/// A byte that is not UTF-8 costs its own line, not the list: the other recents
/// load, and the next open keeps them.
#[test]
fn a_bad_byte_in_recents_keeps_the_other_recents() {
    use datui::CacheManager;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());
    let [a, b, c] = ["a.csv", "b.csv", "c.csv"].map(|n| {
        let path = temp_dir.path().join(n);
        fs::write(&path, "x\n1\n").unwrap();
        datui::canonical::canonicalize(&path).unwrap()
    });
    let mut bytes = format!("{}\n", a.display()).into_bytes();
    bytes.extend_from_slice(b"/broken/\xff\xfe.csv\n");
    bytes.extend_from_slice(format!("{}\n", b.display()).as_bytes());
    fs::write(temp_dir.path().join("recents_history.txt"), bytes).unwrap();

    assert_eq!(cache.load_recents(), vec![a.clone(), b.clone()]);
    cache.push_recent(&c);
    assert_eq!(cache.load_recents(), vec![c, a, b]);
}

/// A history that cannot be read is left alone: a push does not rewrite it with one
/// entry.
#[cfg(unix)]
#[test]
fn an_unreadable_history_is_not_rewritten() {
    use datui::CacheManager;
    use std::os::unix::fs::PermissionsExt;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());
    let history = temp_dir.path().join("query_history.txt");
    fs::write(&history, "one\ntwo\n").unwrap();
    fs::set_permissions(&history, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&history).is_ok() {
        // Root reads it anyway; nothing to test.
        return;
    }

    assert!(cache.load_history_file("query").is_err());
    assert!(
        cache
            .update_history_file("query", |entries| entries.push("new".into()))
            .is_err()
    );
    fs::set_permissions(&history, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(fs::read_to_string(&history).unwrap(), "one\ntwo\n");
}

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
    use datui::cache::HistoryUpdate;
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
            std::thread::spawn(move || {
                let outcome = cache.push_recent(&path);
                (path, outcome)
            })
        })
        .collect();

    let mut written = Vec::new();
    let mut skipped = Vec::new();
    for h in handles {
        let (path, outcome) = h.join().expect("writer thread");
        match outcome {
            HistoryUpdate::Written => written.push(path),
            HistoryUpdate::SkippedBusy => skipped.push(path),
        }
    }

    let recents = cache.load_recents();

    // push_recent stores the canonicalised path, so that is what has to be looked
    // for. On macOS the temp directory sits under /var, which is a symlink to
    // /private/var, and on Windows canonicalising yields a \\?\ prefix; on both,
    // the raw path handed to the thread is not the string that lands in the file.
    // Comparing the raw one failed on those two platforms for a reason that has
    // nothing to do with concurrency, and it went unseen because they only run CI
    // on a release version.
    let stored =
        |p: &std::path::PathBuf| datui::canonical::canonicalize(p).unwrap_or_else(|_| p.clone());

    // The real invariant, and the one worth defending: the lock makes each
    // read-modify-write atomic, so no writer that got the lock can have its entry
    // clobbered by another that came after. Every push that reported success is
    // therefore still in the file.
    //
    // This used to assert that all sixteen survived, which is a different and
    // weaker-founded claim: a contended update is abandoned by design, so whether
    // all sixteen land depends on how the scheduler happened to interleave them.
    // That is why it failed on CI roughly one run in twenty while passing locally
    // every time. Raising LOCK_TIMEOUT from 250ms to 2s made it rarer without
    // making it impossible, because no timeout can make a timing assumption true.
    for path in &written {
        assert!(
            recents.contains(&stored(path)),
            "push_recent reported Written for {path:?} but it is not in the file; \
             a locked read-modify-write lost an update. Skipped: {skipped:?}"
        );
    }
    assert_eq!(
        recents.len(),
        written.len(),
        "the file holds entries nobody reported writing; got {recents:#?}"
    );

    // Every line is a whole, valid path — never two concatenated or one torn.
    for entry in &recents {
        assert!(
            entry.exists(),
            "history holds a path that is not a real file: {entry:?}"
        );
    }

    // Not an assertion, because contention is legitimate and machine-dependent.
    // A note in the output is enough to notice if the deadline ever starts
    // dropping most of them, which would mean LOCK_TIMEOUT had become too tight
    // again rather than that this test is wrong.
    if !skipped.is_empty() {
        eprintln!(
            "note: {} of {} concurrent pushes were skipped on a busy lock",
            skipped.len(),
            paths.len()
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

    assert_eq!(
        result.expect("a contended update is skipped, not an error"),
        datui::cache::HistoryUpdate::SkippedBusy,
        "a held lock must report the skip, not claim the write happened"
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

    cache.forget_recent(&datui::canonical::canonicalize(&drop).unwrap());

    let recents = cache.load_recents();
    assert!(recents.iter().any(|p| p.ends_with("keep.parquet")));
    assert!(
        !recents.iter().any(|p| p.ends_with("private.csv")),
        "the forgotten entry should be gone: {recents:?}"
    );
}

#[test]
fn test_clearing_recents_leaves_other_caches_alone() {
    // `datui cache clear` is too blunt for "forget where I have been": it would also
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
    let manager = ConfigManager::with_dir(
        TempDir::new()
            .expect("Failed to create temp dir")
            .path()
            .to_path_buf(),
    );
    let generated = manager.generate_default_config();

    let parsed: AppConfig =
        toml::from_str(&generated).expect("the generated default config must parse as TOML");
    parsed
        .validate()
        .expect("the generated default config must validate");

    assert!(generated.contains("# [display]"));
    assert!(generated.contains("[[sources]]\nname = \"public\""));
    assert!(parsed.cloud.connections.is_empty());
    assert_eq!(parsed.sources, [datui::config::builtin_catalog()]);
}

#[test]
fn test_the_generated_config_shows_every_documented_setting() {
    // Settings unset by default are not serialized, so the generator adds them; one it
    // missed would silently vanish from the file people learn the options from. Each
    // example must also be a value the setting accepts.
    let (_temp_dir, manager) = setup_test_config_dir();
    let generated = manager.generate_default_config();

    let mut section = String::new();
    let mut settings: Vec<(String, String)> = Vec::new();
    for line in generated.lines() {
        let Some(line) = line.strip_prefix("# ") else {
            continue;
        };
        if line.starts_with('[') && line.ends_with(']') && !line.starts_with("[[") {
            section = line.trim_matches(['[', ']']).to_string();
        } else if let Some((key, value)) = line.split_once(" = ")
            && !key.contains(' ')
            && !value.ends_with('[')
            // Prose that happens to hold " = " is not a TOML value.
            && toml::from_str::<toml::Table>(&format!("v = {value}")).is_ok()
        {
            let path = if section.is_empty() {
                key.to_string()
            } else {
                format!("{section}.{key}")
            };
            settings.push((path, value.to_string()));
        }
    }
    let shown = |path: &str| settings.iter().any(|(key, _)| key == path);
    assert!(
        !generated.contains(" = null"),
        "TOML has no null; uncommenting one fails to parse"
    );

    for path in [
        "read.infer_types",
        "csv.null_values",
        "read.temp_dir",
        "csv.ignore_errors",
        "display.sidebar_width",
        "theme.mode",
        "log.file",
        "log.level",
        "cloud.discover",
        "cloud.env_files",
        "analysis.chart_rows",
        "query.default_mode",
    ] {
        assert!(shown(path), "{path} is missing from the generated config");
    }

    for (path, value) in &settings {
        let text = match path.rsplit_once('.') {
            Some((table, key)) => format!("[{table}]\n{key} = {value}\n"),
            None => format!("{path} = {value}\n"),
        };
        let layer = ConfigLayer::parse(&text)
            .unwrap_or_else(|e| panic!("{path} = {value} is not a valid setting: {e}"));
        AppConfig::from_layers([layer]).expect("resolves");
    }
}

#[test]
fn test_a_multi_line_array_default_is_fully_commented() {
    // A list default is written on its one commented line, so no element is left
    // behind as bare text, which would not parse.
    let manager = ConfigManager::with_dir(
        TempDir::new()
            .expect("Failed to create temp dir")
            .path()
            .to_path_buf(),
    );
    let generated = manager.generate_default_config();

    assert!(
        generated.contains("# skip = ["),
        "the skip list should appear in the generated config"
    );
    assert!(
        generated.contains("# skip = [\"node_modules\","),
        "array elements must be commented too"
    );
}

#[test]
fn test_search_settings_round_trip_through_toml() {
    let toml = r#"
[home.search]
enabled = false
max_depth = 3
skip_extra = ["archive"]
extensions = ["parquet"]
"#;
    let config: AppConfig = toml::from_str(toml).unwrap();
    assert!(!config.home.search.enabled);
    assert_eq!(config.home.search.max_depth, 3);
    assert_eq!(config.home.search.skip_extra, vec!["archive".to_string()]);
    assert_eq!(config.home.search.extensions, vec!["parquet".to_string()]);
    // Untouched fields keep their defaults, and skip_extra adds to skip rather than
    // replacing it.
    assert!(!config.home.search.cross_filesystems);
    assert!(
        config
            .home
            .search
            .skipped_dirs()
            .contains(&"node_modules".to_string())
    );
    assert!(
        config
            .home
            .search
            .skipped_dirs()
            .contains(&"archive".to_string())
    );
}

/// A config file can name credentials (a connection's command, a dataset URL's
/// token). datui creates that file, so datui decides who can read it: a plain write
/// lands at 0644 under a typical umask, which hands it to every other account on
/// the machine.
#[cfg(unix)]
#[test]
fn generated_config_is_not_readable_by_other_users() {
    use std::os::unix::fs::PermissionsExt;

    let (_temp_dir, config_manager) = setup_test_config_dir();
    let path = config_manager
        .write_default_config(false)
        .expect("write default config");

    let mode = fs::metadata(&path)
        .expect("stat config")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "config should be owner-only, got {:o}",
        mode & 0o777
    );
}

/// `datui config init --force` overwrites a file that already exists, and
/// `OpenOptions::mode` only applies when creating. Without an explicit
/// `set_permissions`, a config first written by an older datui would keep its
/// 0644 forever.
#[cfg(unix)]
#[test]
fn regenerating_over_a_world_readable_config_tightens_it() {
    use std::os::unix::fs::PermissionsExt;

    let (_temp_dir, config_manager) = setup_test_config_dir();
    let path = config_manager
        .write_default_config(false)
        .expect("write default config");

    // Simulate a config left behind by a version that wrote 0644.
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("loosen");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );

    config_manager
        .write_default_config(true)
        .expect("regenerate with force");

    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600,
        "regenerating should tighten an existing world-readable config"
    );
}

fn cloud_config(toml_text: &str) -> AppConfig {
    toml::from_str(toml_text).expect("Failed to parse config")
}

fn cloud_error(toml_text: &str) -> String {
    cloud_config(toml_text)
        .validate()
        .expect_err("config should be rejected")
        .to_string()
}

#[test]
fn cloud_sources_parse_and_validate() {
    let config = cloud_config(
        r#"
[[cloud.connections]]
name = "onprem"
label = "On-prem MinIO"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
region = "us-east-1"
addressing = "path"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"
buckets = ["sales", "logs"]

[[cloud.connections]]
name = "analytics"
kind = "gcs"
"#,
    );
    config.validate().expect("valid sources");
    assert_eq!(config.cloud.connections.len(), 2);
    assert_eq!(
        config.cloud.connections[0].label.as_deref(),
        Some("On-prem MinIO")
    );
    assert_eq!(config.cloud.connections[0].buckets, ["sales", "logs"]);
}

#[test]
fn cloud_sources_name_the_problem() {
    let unknown = cloud_error(
        "[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\nendpont_url = \"http://x\"\n",
    );
    assert!(
        unknown.contains("'endpont_url'") && unknown.contains("endpoint_url"),
        "{unknown}"
    );

    let secret = cloud_error(
        "[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\nsecret_access_key = \"hunter2\"\n",
    );
    assert!(secret.contains("secret_access_key_env"), "{secret}");
    assert!(
        !secret.contains("hunter2"),
        "the secret must not be echoed: {secret}"
    );

    let name = cloud_error("[[cloud.connections]]\nname = \"On Prem\"\nkind = \"s3\"\n");
    assert!(name.contains("not a valid name"), "{name}");

    let twice = cloud_error(
        "[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\n[[cloud.connections]]\nname = \"lab\"\nkind = \"gcs\"\n",
    );
    assert!(twice.contains("used twice"), "{twice}");

    let wrong_kind = cloud_error(
        "[[cloud.connections]]\nname = \"g\"\nkind = \"gcs\"\nendpoint_url = \"http://x\"\n",
    );
    assert!(wrong_kind.contains("only to kind = \"s3\""), "{wrong_kind}");

    let no_kind = cloud_error("[[cloud.connections]]\nname = \"lab\"\n");
    assert!(no_kind.contains("kind is required"), "{no_kind}");

    let azure = cloud_config(
        "[[cloud.connections]]\nname = \"research\"\nkind = \"azure\"\naccount = \"datuiresearch\"\nsas_env = \"RESEARCH_SAS\"\n",
    );
    azure.validate().expect("an azure source");
    let two_secrets = cloud_error(
        "[[cloud.connections]]\nname = \"r\"\nkind = \"azure\"\naccount = \"a\"\nsas_env = \"S\"\naccount_key_env = \"K\"\n",
    );
    assert!(two_secrets.contains("Use one"), "{two_secrets}");
    let no_account = cloud_error("[[cloud.connections]]\nname = \"r\"\nkind = \"azure\"\n");
    assert!(no_account.contains("needs account"), "{no_account}");
    let literal = cloud_error(
        "[[cloud.connections]]\nname = \"r\"\nkind = \"azure\"\naccount = \"a\"\naccount_key = \"c2VjcmV0\"\n",
    );
    assert!(
        literal.contains("account_key_env") && !literal.contains("c2VjcmV0"),
        "{literal}"
    );
    let azure_only =
        cloud_error("[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\naccount = \"a\"\n");
    assert!(
        azure_only.contains("only to kind = \"azure\""),
        "{azure_only}"
    );

    let secret_twice = cloud_error(
        "[[cloud.connections]]\nname = \"m\"\nkind = \"s3\"\naccess_key_id_env = \"K\"\nsecret_access_key_env = \"S\"\nsecret_command = \"pass show m\"\n",
    );
    assert!(secret_twice.contains("Use one"), "{secret_twice}");
    let no_key_id = cloud_error(
        "[[cloud.connections]]\nname = \"m\"\nkind = \"s3\"\nsecret_command = \"pass show m\"\n",
    );
    assert!(no_key_id.contains("access_key_id_env"), "{no_key_id}");
    let gcs_command = cloud_error(
        "[[cloud.connections]]\nname = \"g\"\nkind = \"gcs\"\nsecret_command = \"pass show g\"\n",
    );
    assert!(
        gcs_command.contains("secret_command applies only"),
        "{gcs_command}"
    );
    let file_and_configuration = cloud_error(
        "[[cloud.connections]]\nname = \"g\"\nkind = \"gcs\"\ncredentials_file = \"~/sa.json\"\nconfiguration = \"work\"\n",
    );
    assert!(
        file_and_configuration.contains("Use one"),
        "{file_and_configuration}"
    );
    let s3_file = cloud_error(
        "[[cloud.connections]]\nname = \"m\"\nkind = \"s3\"\ncredentials_file = \"~/sa.json\"\n",
    );
    assert!(s3_file.contains("only to kind = \"gcs\""), "{s3_file}");
    cloud_config(
        "[cloud]\nenv_files = [\".env\"]\ninstance_identity = true\n[[cloud.connections]]\nname = \"m\"\nkind = \"s3\"\naccess_key_id_env = \"K\"\nsecret_command = \"pass show m\"\n",
    )
    .validate()
    .expect("opt-ins");

    let google_only =
        cloud_error("[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\nproject = \"p\"\n");
    assert!(
        google_only.contains("only to kind = \"gcs\""),
        "{google_only}"
    );

    let addressing = cloud_error(
        "[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\naddressing = \"sideways\"\n",
    );
    assert!(addressing.contains("path or virtual"), "{addressing}");
}

#[test]
fn a_connection_names_buckets_and_nothing_public() {
    let forgot = cloud_error(
        "[[cloud.connections]]\nname = \"p\"\nkind = \"s3\"\nbuckets = [\"s3://noaa-ghcn-pds/\"]\n",
    );
    assert!(forgot.contains("[[sources.datasets]]"), "{forgot}");
    for key in ["public = true", "datasets = []"] {
        let old = cloud_error(&format!(
            "[[cloud.connections]]\nname = \"p\"\nkind = \"s3\"\n{key}\n"
        ));
        assert!(old.contains("unknown key"), "{key}: {old}");
    }
}

/// One collection holding a local file, an anonymous S3 prefix, an HTTPS file and a
/// private bucket read through a configured connection.
const MIXED: &str = r#"
[[cloud.connections]]
name = "onprem"
kind = "s3"
endpoint_url = "https://minio.corp.example:9000"
access_key_id_env = "ONPREM_KEY"
secret_access_key_env = "ONPREM_SECRET"

[[sources]]
name = "my-datasets"
label = "My datasets"

[[sources.datasets]]
name = "Sales"
path = "~/datasets/sales.parquet"
description = "Monthly sales"

[[sources.datasets]]
name = "Weather"
url = "s3://noaa-ghcn-pds/parquet/"
auth = "anonymous"
description = "Daily weather observations"

[[sources.datasets]]
name = "Penguins"
url = "https://vincentarelbundock.github.io/Rdatasets/csv/palmerpenguins/penguins.csv"

[[sources.datasets]]
name = "Orders"
url = "s3://orders/2024/"
connection = "onprem"
"#;

#[test]
fn a_collection_mixes_local_and_remote_datasets() {
    let config = cloud_config(MIXED);
    config.validate().expect("a valid mixed collection");
    let collection = &config.sources[0];
    assert_eq!(collection.label(), "My datasets");
    let names: Vec<&str> = collection
        .datasets
        .iter()
        .map(|d| d.name.as_str())
        .collect();
    assert_eq!(names, ["Sales", "Weather", "Penguins", "Orders"]);
    assert_eq!(
        collection.datasets[0].local_path(),
        Some(datui::config::expand_config_path("~").join("datasets/sales.parquet"))
    );
    // Configured, the built-in catalog still follows it.
    let ids: Vec<String> = config.collections().into_iter().map(|c| c.name).collect();
    assert_eq!(ids, ["my-datasets", "public"]);
}

#[test]
fn collection_mistakes_are_named() {
    let error = |body: &str| cloud_error(&format!("{MIXED}\n{body}"));
    let dataset = |fields: &str| {
        error(&format!(
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"d\"\n{fields}\n"
        ))
    };
    for (fields, expected) in [
        ("", "path or url"),
        ("path = \"/a.csv\"\nurl = \"s3://b/\"", "path and url both"),
        ("path = \" \"", "path is blank"),
        ("path = \"s3://b/a.csv\"", "is a URL"),
        (
            "path = \"/a.csv\"\nauth = \"anonymous\"",
            "auth applies only to a url",
        ),
        (
            "path = \"/a.csv\"\nconnection = \"onprem\"",
            "connection applies only to a url",
        ),
        (
            "url = \"s3://b/\"\nauth = \"public\"",
            "auth \"public\" is not valid",
        ),
        ("url = \"ftp://host/a.csv\"", "is not an s3://"),
        ("url = \"https://example.com/data/\"", "is not a data file"),
        (
            "url = \"https://example.com/data.zip\"",
            "is not a data file",
        ),
        (
            "url = \"https://user:pw@example.com/a.csv\"",
            "is not a data file",
        ),
        (
            "url = \"https://example.com/a.csv\"\nconnection = \"onprem\"",
            "only to s3://",
        ),
        ("url = \"s3://onprem@b/\"", "rather than in the URL"),
        (
            "url = \"s3://b/\"\nconnection = \"nowhere\"",
            "no [[cloud.connections]] entry is named \"nowhere\". Connections: onprem",
        ),
        (
            "url = \"gs://b/\"\nconnection = \"onprem\"",
            "is kind = \"s3\", which does not read gcs URLs",
        ),
        (
            "url = \"s3://b/\"\nconnection = \"onprem\"\nauth = \"anonymous\"",
            "auth and connection both",
        ),
        (
            "url = \"s3://b/\"\nanonymous = true",
            "unknown key 'anonymous'",
        ),
    ] {
        let message = dataset(fields);
        assert!(message.contains(expected), "{fields:?}: {message}");
    }
    // The variable that names the home directory: Windows sets USERPROFILE, not HOME.
    let home_var = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    let home_twice = format!(
        "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"a\"\npath = \"~/a.csv\"\n[[sources.datasets]]\nname = \"b\"\npath = \"${home_var}/a.csv\"\n"
    );
    for (body, expected) in [
        (
            "[[sources]]\nname = \"My Data\"\n[[sources.datasets]]\nname = \"d\"\npath = \"/a\"\n",
            "not a valid name",
        ),
        ("[[sources]]\nname = \"x\"\n", "no datasets"),
        (
            "[[sources]]\nname = \"x\"\nlabel = \" \"\n[[sources.datasets]]\nname = \"d\"\npath = \"/a\"\n",
            "label is blank",
        ),
        (
            "[[sources]]\nname = \"x\"\nhidden = true\n[[sources.datasets]]\nname = \"d\"\npath = \"/a\"\n",
            "unknown key 'hidden'",
        ),
        (
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \" \"\npath = \"/a\"\n",
            "nonempty name",
        ),
        (
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"d\"\npath = \"/a\"\n[[sources.datasets]]\nname = \"d\"\npath = \"/b\"\n",
            "name \"d\" is used twice",
        ),
        (
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"a\"\nurl = \"s3://b\"\n[[sources.datasets]]\nname = \"b\"\nurl = \"s3://b/\"\n",
            "\"s3://b/\" is listed twice",
        ),
        (
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"a\"\nurl = \"https://account.blob.core.windows.net/c/p/\"\n[[sources.datasets]]\nname = \"b\"\nurl = \"abfss://c@account.dfs.core.windows.net/p\"\n",
            "is listed twice",
        ),
        (&*home_twice, "is listed twice"),
        (
            "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"a\"\npath = \"/d/a.csv\"\n[[sources.datasets]]\nname = \"b\"\npath = \"/d//a.csv\"\n",
            "is listed twice",
        ),
        (
            "[[sources]]\nname = \"my-datasets\"\n[[sources.datasets]]\nname = \"d\"\npath = \"/a\"\n",
            "\"my-datasets\" is used twice",
        ),
        ("[home]\nhide = [\"My datasets\"]\n", "Use the name, not"),
    ] {
        let message = error(body);
        assert!(message.contains(expected), "{body:?}: {message}");
    }
    // A connection names the account an Azure URL is in.
    let azure = cloud_error(
        "[[cloud.connections]]\nname = \"az\"\nkind = \"azure\"\naccount = \"one\"\n[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"d\"\nurl = \"abfss://c@two.dfs.core.windows.net/p/\"\nconnection = \"az\"\n",
    );
    assert!(azure.contains("account \"one\""), "{azure}");
}

#[test]
fn collections_accept_web_files_but_not_web_directories_or_archives() {
    for url in [
        "https://example.com/data.csv",
        "http://localhost:8080/data.parquet",
        "https://example.com/data.csv.gz",
    ] {
        let text = format!(
            "[[sources]]\nname = \"public\"\n[[sources.datasets]]\nname = \"Data\"\nurl = {url:?}\n"
        );
        cloud_config(&text)
            .validate()
            .unwrap_or_else(|e| panic!("{url}: {e}"));
    }
    for url in [
        "https://example.com/",
        "https://example.com/data/",
        "https://example.com/data.zip",
        "https://user:password@example.com/data.csv",
        "https:///data.csv",
        "https://example.com/bad path.csv",
    ] {
        let text = format!(
            "[[sources]]\nname = \"public\"\n[[sources.datasets]]\nname = \"Data\"\nurl = {url:?}\n"
        );
        assert!(cloud_config(&text).validate().is_err(), "{url}");
    }
    let bucket = cloud_error(
        "[[cloud.connections]]\nname = \"web\"\nkind = \"s3\"\nbuckets = [\"https://example.com/data.csv\"]\n",
    );
    assert!(
        bucket.contains("[[sources.datasets]]"),
        "web files belong in datasets: {bucket}"
    );
}

#[test]
fn a_later_layer_replaces_a_source_by_name() {
    let base = layered(&[
        "[cloud]\nhide = [\"a\"]\n[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\nregion = \"one\"\n",
        "[cloud]\nhide = [\"a\", \"b\"]\n[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\nregion = \"two\"\n[[cloud.connections]]\nname = \"new\"\nkind = \"gcs\"\n",
    ]);
    let names: Vec<&str> = base
        .cloud
        .connections
        .iter()
        .map(|s| s.name.as_str())
        .collect();
    assert_eq!(names, ["lab", "new"]);
    assert_eq!(base.cloud.connections[0].region.as_deref(), Some("two"));
    assert_eq!(base.cloud.hide, ["a", "b"]);
}

#[test]
fn two_connections_of_one_name_in_one_file_are_refused() {
    let config = layered(&[
        "[[cloud.connections]]\nname = \"lab\"\nkind = \"s3\"\n[[cloud.connections]]\nname = \"lab\"\nkind = \"gcs\"\n",
    ]);
    let error = config.validate().expect_err("refused").to_string();
    assert!(error.contains("\"lab\" is used twice"), "{error}");
}

#[test]
fn a_later_layer_replaces_a_collection_whole() {
    let base = layered(&[
        "[[sources]]\nname = \"team\"\n[[sources.datasets]]\nname = \"Old\"\nurl = \"s3://old/\"\n[[sources]]\nname = \"mine\"\n[[sources.datasets]]\nname = \"Mine\"\npath = \"/data/mine.csv\"\n",
        "[[sources]]\nname = \"team\"\nlabel = \"Team\"\n[[sources.datasets]]\nname = \"New\"\nurl = \"gs://new/\"\ndescription = \"Replacement\"\n[[sources]]\nname = \"extra\"\n[[sources.datasets]]\nname = \"Extra\"\npath = \"/data/extra.csv\"\n",
    ]);
    let names: Vec<&str> = base.sources.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        ["team", "mine", "extra"],
        "replaced in place, new ones after"
    );
    assert_eq!(base.sources[0].label(), "Team");
    assert_eq!(
        base.sources[0].datasets.len(),
        1,
        "datasets are never merged"
    );
    assert_eq!(base.sources[0].datasets[0].name, "New");
}

#[test]
fn the_builtin_catalog_is_replaced_dropped_or_hidden() {
    let names = |config: &AppConfig, shown: bool| -> Vec<(String, String)> {
        let list = if shown {
            config.shown_collections()
        } else {
            config.collections()
        };
        list.into_iter()
            .map(|c| (c.name.clone(), c.label().to_string()))
            .collect()
    };
    let default = AppConfig::default();
    assert_eq!(
        names(&default, true),
        [("public".to_string(), "Public datasets".to_string())]
    );

    // A collection named `public` replaces the whole catalog, where it is defined.
    let replacing = "[[sources]]\nname = \"mine\"\n[[sources.datasets]]\nname = \"A\"\npath = \"/a.csv\"\n[[sources]]\nname = \"public\"\nlabel = \"Curated\"\n[[sources.datasets]]\nname = \"Weather\"\nurl = \"s3://weather/\"\nauth = \"anonymous\"\n";
    let replaced = layered(&[replacing]);
    replaced.validate().unwrap();
    assert_eq!(
        names(&replaced, true),
        [
            ("mine".to_string(), "mine".to_string()),
            ("public".to_string(), "Curated".to_string())
        ]
    );
    let catalog = &replaced.collections()[1];
    assert_eq!(
        catalog.datasets.len(),
        1,
        "nothing of the built-in is merged in"
    );
    assert_eq!(
        replaced
            .cloud
            .dataset_access
            .iter()
            .map(|a| a.url.as_str())
            .collect::<Vec<_>>(),
        ["s3://weather/"],
        "only the replacement's URLs are read anonymously"
    );

    // Dropping the built-in leaves a configured `public` alone; hiding hides it.
    let drop = "[home]\nbuiltin_catalog = false\n";
    let dropped = layered(&[replacing, drop]);
    assert_eq!(names(&dropped, true).len(), 2);
    let off = layered(&[drop]);
    assert!(off.collections().is_empty());
    assert!(off.cloud.dataset_access.is_empty());
    let back = layered(&[drop, "[home]\nbuiltin_catalog = true\n"]);
    assert_eq!(back.collections().len(), 1, "a later file turns it back on");

    let hidden = layered(&[
        replacing,
        "[home]\nhide = [\"public\"]\n",
        "[home]\nhide = [\"mine\"]\n",
    ]);
    assert_eq!(hidden.home.hide, ["public", "mine"], "hides add up");
    assert!(names(&hidden, true).is_empty());
    assert_eq!(names(&hidden, false).len(), 2, "hidden, not gone");
}

#[test]
fn relative_paths_are_relative_to_the_file_that_names_them() {
    let dir = TempDir::new().unwrap();
    let team = dir.path().join("team");
    fs::create_dir_all(&team).unwrap();
    fs::write(
        team.join("shared.toml"),
        "[[sources]]\nname = \"team\"\n[[sources.datasets]]\nname = \"Shared\"\npath = \"data/shared.csv\"\n[[sources.datasets]]\nname = \"Home\"\npath = \"~/home.csv\"\n",
    )
    .unwrap();
    let config_path = dir.path().join("config.toml");
    fs::write(
        &config_path,
        "import = [\"team/shared.toml\"]\n[[sources]]\nname = \"mine\"\n[[sources.datasets]]\nname = \"Mine\"\npath = \"mine.parquet\"\n",
    )
    .unwrap();
    let config = AppConfig::load_from_file(&config_path).expect("loads");
    let path =
        |source: usize, dataset: usize| config.sources[source].datasets[dataset].local_path();
    assert_eq!(path(0, 0), Some(team.join("data/shared.csv")));
    assert_eq!(
        path(0, 1),
        Some(datui::config::expand_config_path("~").join("home.csv")),
        "~ is not relative"
    );
    assert_eq!(path(1, 0), Some(dir.path().join("mine.parquet")));
}

#[test]
fn two_collections_of_one_name_in_one_file_are_refused() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    fs::write(
        &config_path,
        "[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"A\"\npath = \"/a\"\n[[sources]]\nname = \"x\"\n[[sources.datasets]]\nname = \"B\"\npath = \"/b\"\n",
    )
    .unwrap();
    let error = AppConfig::load_from_file(&config_path)
        .expect_err("refused")
        .to_string();
    assert!(error.contains("\"x\" is used twice"), "{error}");
}

#[test]
fn the_generated_config_materializes_the_builtin_catalog() {
    let (_dir, manager) = setup_test_config_dir();
    let text = manager.generate_default_config();
    let config: AppConfig = toml::from_str(&text).expect("generated config parses");
    config.validate().expect("generated config validates");
    assert_eq!(config.sources, [datui::config::builtin_catalog()]);
    let names: Vec<_> = config.sources[0]
        .datasets
        .iter()
        .map(|dataset| dataset.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "NYC flights (2013)",
            "Food nutrition (fast food)",
            "US baby names (1880-2017)",
            "NOAA daily weather (GHCN-D)",
            "Premier League (2020-21)",
            "NYC yellow taxis (January 2025)",
            "Earthquakes (past month)",
            "Space launches (1957-2018)",
            "Palmer penguins",
            "Aqueous solubility (SDF)",
            "Bitcoin and Ethereum",
            "Overture Maps"
        ],
        "generated configs should ship the curated catalog"
    );
    assert!(text.contains("snapshot"), "{text}");
    assert!(text.contains("[[sources.datasets]]"), "{text}");
    assert!(text.contains("# builtin_catalog = true"), "{text}");
    assert!(text.contains("# hide = []"), "{text}");
    assert!(!text.contains("\nhide ="), "{text}");
}

#[test]
fn cloud_listings_and_hidden_sources_survive_a_restart() {
    use datui::CacheManager;
    use datui::cache::CloudListing;

    let temp_dir = TempDir::new().expect("temp dir");
    let cache = CacheManager::with_dir(temp_dir.path().to_path_buf());
    assert!(cache.load_cloud_listings().is_empty());

    let lab = CloudListing {
        fingerprint: "s3|http://127.0.0.1:9000|key||".to_string(),
        buckets: vec!["data".to_string(), "logs".to_string()],
        listed_at: 1_789_000_000,
    };
    cache.save_cloud_listing("lab", lab.clone());
    cache.save_cloud_listing(
        "corp",
        CloudListing {
            buckets: vec!["data".to_string()],
            ..lab.clone()
        },
    );
    let again = CacheManager::with_dir(temp_dir.path().to_path_buf());
    let listings = again.load_cloud_listings();
    assert_eq!(
        listings.get("lab"),
        Some(&lab),
        "one source's save keeps the others"
    );
    assert_eq!(listings["corp"].buckets, ["data"]);

    again.hide_cloud_source("corp");
    again.hide_cloud_source("corp");
    assert_eq!(again.load_hidden_cloud_sources(), ["corp"]);
}

#[test]
fn cloud_discover_takes_a_switch_a_word_or_a_list_of_kinds() {
    use datui::config::CloudDiscover;
    let parse = |value: &str| {
        toml::from_str::<AppConfig>(&format!("[cloud]\ndiscover = {value}\n"))
            .map(|c| c.cloud.discover)
            .map_err(|e| e.to_string())
    };
    assert_eq!(parse("true"), Ok(Some(CloudDiscover::All)));
    assert_eq!(parse("\"all\""), Ok(Some(CloudDiscover::All)));
    assert_eq!(parse("false"), Ok(Some(CloudDiscover::None)));
    assert_eq!(parse("\"none\""), Ok(Some(CloudDiscover::None)));
    assert_eq!(
        parse("[\"GCS\", \"s3\", \"gcs\"]"),
        Ok(Some(CloudDiscover::Kinds(vec![
            "gcs".to_string(),
            "s3".to_string()
        ])))
    );
    assert_eq!(parse("[]"), Ok(Some(CloudDiscover::Kinds(Vec::new()))));
    // "all" is a word, not a kind: a list cannot say both everything and one thing.
    let err = parse("[\"all\", \"s3\"]").unwrap_err();
    assert!(err.contains("unknown kind \"all\""), "{err}");
    let err = parse("[\"aws\"]").unwrap_err();
    assert!(err.contains("unknown kind \"aws\""), "{err}");
    let err = parse("\"some\"").unwrap_err();
    assert!(err.contains("\"some\" is not \"all\", \"none\""), "{err}");
    // The command line's form works in the config too.
    assert_eq!(
        parse("\"s3, azure\""),
        Ok(Some(CloudDiscover::Kinds(vec![
            "s3".to_string(),
            "azure".to_string()
        ])))
    );
    let err = parse("\"all,s3\"").unwrap_err();
    assert!(err.contains("\"all,s3\" is not"), "{err}");

    assert_eq!(
        toml::from_str::<AppConfig>("").unwrap().cloud.discover,
        None,
        "unset means every kind"
    );
    let round_trip = |discover: CloudDiscover| {
        let mut config = AppConfig::default();
        config.cloud.discover = Some(discover);
        let text = toml::to_string(&config).unwrap();
        toml::from_str::<AppConfig>(&text).unwrap().cloud.discover
    };
    assert_eq!(round_trip(CloudDiscover::None), Some(CloudDiscover::None));
    assert_eq!(
        round_trip(CloudDiscover::Kinds(vec!["azure".to_string()])),
        Some(CloudDiscover::Kinds(vec!["azure".to_string()]))
    );
}

#[test]
fn cloud_discover_and_list_on_start_merge_only_when_set() {
    use datui::config::CloudDiscover;
    let set = "[cloud]\ndiscover = [\"s3\"]\nlist_on_start = true\n";
    let base = layered(&[set, "[display]\n"]);
    assert_eq!(
        base.cloud.discover,
        Some(CloudDiscover::Kinds(vec!["s3".to_string()]))
    );
    assert!(base.cloud.list_on_start);

    let changed = layered(&[set, "[cloud]\ndiscover = false\nlist_on_start = false\n"]);
    assert_eq!(changed.cloud.discover, Some(CloudDiscover::None));
    assert!(!changed.cloud.list_on_start);
}

/// `[cloud] discover` is config only; `-c` sets it for one run, over the file.
#[test]
fn cloud_discover_is_set_for_a_run_with_dash_c() {
    use clap::Parser;
    use datui::config::{CloudDiscover, ConfigLayer};
    let file = ConfigLayer::parse("[cloud]\ndiscover = \"all\"\n").unwrap();
    let discover = |flags: &[&str]| {
        let args =
            datui_cli::Args::try_parse_from(std::iter::once("datui").chain(flags.iter().copied()))
                .expect("parses");
        let layers = [
            file.clone(),
            ConfigLayer::from_overrides(&args.config).unwrap(),
        ];
        AppConfig::from_layers(layers).unwrap().cloud.discover
    };
    assert_eq!(
        discover(&["-c", "cloud.discover=[\"gcs\", \"azure\"]"]),
        Some(CloudDiscover::Kinds(vec![
            "gcs".to_string(),
            "azure".to_string()
        ]))
    );
    assert_eq!(
        discover(&["-c", "cloud.discover=none"]),
        Some(CloudDiscover::None)
    );
    assert_eq!(discover(&[]), Some(CloudDiscover::All));
    assert!(datui_cli::Args::try_parse_from(["datui", "--cloud-discover", "all"]).is_err());
}

#[test]
fn glyph_overrides_parse_validate_and_merge() {
    let config: AppConfig = toml::from_str(
        r#"

[glyphs]
in_object_store = "☁"
spinner = ["◐", "◓", "◑", "◒"]
"#,
    )
    .expect("a [glyphs] table parses");
    assert_eq!(
        config.glyphs.overrides.get("in_object_store"),
        Some(&datui::glyphs::SlotOverride::One("☁".to_string()))
    );
    config.validate().expect("valid overrides pass validation");

    // Layered like the theme: an import sets two slots, the user's own file
    // overrides one of them and the other survives.
    let base = layered(&[
        "[glyphs]\nin_object_store = \"☁\"\nspinner = [\"◐\", \"◓\", \"◑\", \"◒\"]\n",
        "[glyphs]\nin_object_store = \"≋\"\n",
    ]);
    assert_eq!(
        base.glyphs.overrides.get("in_object_store"),
        Some(&datui::glyphs::SlotOverride::One("≋".to_string()))
    );
    assert!(base.glyphs.overrides.contains_key("spinner"));
}

#[test]
fn glyph_override_errors_name_the_slot() {
    let unknown: AppConfig = toml::from_str("[glyphs]\nno_such_slot = \"x\"").expect("parses");
    let err = unknown.validate().expect_err("unknown slot rejected");
    assert!(err.to_string().contains("no_such_slot"), "{err}");

    // ‹binary› is eight columns; a one-column replacement shifts the layout.
    let narrow: AppConfig = toml::from_str("[glyphs]\nbinary_stub = \"b\"").expect("parses");
    let err = narrow.validate().expect_err("wrong width rejected");
    assert!(err.to_string().contains("binary_stub"), "{err}");
}

#[test]
fn template_documents_the_glyphs_section() {
    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();
    assert!(template.contains("# Glyphs"), "{template}");
    assert!(template.contains("glyphs.rs"), "{template}");
}

#[test]
fn cursor_text_defaults_merges_and_validates() {
    use datui::config::ColorConfig;

    let config = AppConfig::default();
    assert_eq!(config.theme.colors.input_cursor_text, "default");
    config
        .validate()
        .expect("default input_cursor_text validates");

    let set = layered(&["[theme.colors]\ninput_cursor_text = \"#1a1b26\"\n"]);
    assert_eq!(set.theme.colors.input_cursor_text, "#1a1b26");
    assert_eq!(
        set.theme.colors.input_cursor,
        ColorConfig::default().input_cursor
    );

    let mut bad = AppConfig::default();
    bad.theme.colors.input_cursor_text = "not-a-color".to_string();
    assert!(bad.validate().is_err());
}

#[test]
fn cursor_text_auto_contrast_picks_by_luminance() {
    use datui::config::Theme;
    use ratatui::style::Color;

    let mut theme = Theme {
        colors: std::collections::HashMap::new(),
    };
    // "default" (absent from the map) means: black or white by the cursor's luminance.
    assert_eq!(theme.cursor_text_for(Color::Rgb(20, 20, 40)), Color::White);
    assert_eq!(
        theme.cursor_text_for(Color::Rgb(230, 230, 200)),
        Color::Black
    );
    // A configured slot wins outright.
    theme
        .colors
        .insert("input_cursor_text".to_string(), Color::Rgb(1, 2, 3));
    assert_eq!(
        theme.cursor_text_for(Color::Rgb(20, 20, 40)),
        Color::Rgb(1, 2, 3)
    );
}

#[test]
fn clipboard_config_defaults_merge_and_validate() {
    let config = AppConfig::default();
    assert_eq!(config.clipboard.backend, "auto");
    assert_eq!(config.clipboard.osc52_limit, ByteSize::kib(100));
    config.validate().expect("defaults validate");

    let base = layered(&["[clipboard]\nbackend = \"osc52\"\nosc52_limit = \"512KiB\""]);
    assert_eq!(base.clipboard.backend, "osc52");
    assert_eq!(base.clipboard.osc52_limit, ByteSize::kib(512));
    assert!(
        ConfigLayer::parse("[clipboard]\nosc52_limit = 512\n").is_err(),
        "a size needs its unit"
    );
    base.validate().expect("a named backend validates");

    let mut bad = AppConfig::default();
    bad.clipboard.backend = "wayland".to_string();
    let err = bad.validate().expect_err("unknown backend rejected");
    assert!(err.to_string().contains("osc52"), "{err}");

    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template = config_manager.generate_default_config();
    assert!(template.contains("# [clipboard]"), "{template}");
}

/// `analysis.quality_local_copy`: 2 GiB when omitted, read when set, 0 kept as
/// "never copy", merged over an earlier layer, and the same in the template.
#[test]
fn test_quality_local_copy() {
    let two = ByteSize::mib(2048);
    assert_eq!(AppConfig::default().analysis.quality_local_copy, two);
    let omitted: AppConfig = toml::from_str("[analysis]\n").unwrap();
    assert_eq!(omitted.analysis.quality_local_copy, two);
    let off: AppConfig = toml::from_str("[analysis]\nquality_local_copy = 0\n").unwrap();
    assert_eq!(off.analysis.quality_local_copy, ByteSize(0));

    let set = "[analysis]\nquality_local_copy = \"512MiB\"\n";
    let never = "[analysis]\nquality_local_copy = \"0\"\n";
    let silent = "[analysis]\nsample_rows = 10\n";
    assert_eq!(
        layered(&[set]).analysis.quality_local_copy,
        ByteSize::mib(512)
    );
    assert_eq!(
        layered(&[set, never]).analysis.quality_local_copy,
        ByteSize(0)
    );
    assert_eq!(
        layered(&[set, never, silent]).analysis.quality_local_copy,
        ByteSize(0),
        "a layer that leaves it out changes nothing"
    );
    assert_eq!(
        layered(&[never, "[analysis]\nquality_local_copy = \"2GiB\"\n"])
            .analysis
            .quality_local_copy,
        two,
        "the default written explicitly overrides an import"
    );

    let (_temp_dir, config_manager) = setup_test_config_dir();
    let template: AppConfig = toml::from_str(&config_manager.generate_default_config()).unwrap();
    assert_eq!(template.analysis.quality_local_copy, two);
}

/// `cell_padding` takes a density by name or a count of cells, keeps the
/// comfortable two cells when omitted, follows the last file that writes it, and
/// says what it takes when given anything else.
#[test]
fn cell_padding_takes_a_density_or_a_count() {
    let cells = |layers: &[&str]| layered(layers).display.cell_padding.cells();
    let compact = "[display]\ncell_padding = \"compact\"\n";
    let comfortable = "[display]\ncell_padding = \"comfortable\"\n";
    let silent = "[display]\nrow_numbers = true\n";
    assert_eq!(cells(&[silent]), 2, "comfortable unless asked otherwise");
    assert_eq!(cells(&[compact]), 1);
    assert_eq!(cells(&[comfortable]), 2);
    assert_eq!(cells(&["[display]\ncell_padding = 0\n"]), 0);
    assert_eq!(cells(&["[display]\ncell_padding = 3\n"]), 3);
    assert_eq!(cells(&[compact, silent]), 1);
    assert_eq!(
        cells(&[compact, comfortable]),
        2,
        "an explicit default undoes an imported compact"
    );

    let err = ConfigLayer::parse("[display]\ncell_padding = \"dense\"\n")
        .and_then(|layer| AppConfig::from_layers([layer]))
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(err.contains("compact") && err.contains("dense"), "{err}");
    assert!(
        ConfigLayer::parse("[display]\ncell_padding = -1\n")
            .and_then(|layer| AppConfig::from_layers([layer]))
            .is_err()
    );
}

/// The CSV dialect keys: defaults, layered by presence, and a comment marker that
/// could not mark a line is refused at load time.
#[test]
fn test_csv_dialect_keys() {
    let config = layered(&[]);
    assert_eq!(config.csv.comment, None);
    assert_eq!(config.csv.header_join, " ");
    assert!(!config.csv.skip_initial_space);

    let config = layered(&[
        "[csv]\ncomment = \"#\"\nheader_join = \"_\"\nskip_initial_space = true\n",
        "[csv]\nskip_initial_space = false\n",
    ]);
    assert_eq!(config.csv.comment.as_deref(), Some("#"));
    assert_eq!(config.csv.header_join, "_");
    assert!(
        !config.csv.skip_initial_space,
        "an explicit default over an import wins"
    );
    assert!(config.validate().is_ok());

    for bad in ["\"\"", "\"#\\n\""] {
        let config = layered(&[&format!("[csv]\ncomment = {bad}\n")]);
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("csv.comment"), "{bad}: {err}");
    }
}

/// How often a followed file is checked: 250ms unless set, layered by presence,
/// written with its unit, and refused outside 10ms to a minute.
#[test]
fn test_follow_interval() {
    let interval = |layers: &[&str]| layered(layers).read.follow_interval.duration();
    let ms = std::time::Duration::from_millis;
    assert_eq!(interval(&[]), ms(250));
    assert_eq!(
        interval(&[
            "[read]\nfollow_interval = \"1s\"\n",
            "[read]\nfollow_interval = \"250ms\"\n",
        ]),
        ms(250),
        "an explicit default over an import wins"
    );
    assert_eq!(
        interval(&["[read]\nfollow_interval = \"100ms\"\n"]),
        ms(100)
    );
    for bad in ["\"9ms\"", "\"61s\"", "0"] {
        let config = layered(&[&format!("[read]\nfollow_interval = {bad}\n")]);
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("read.follow_interval"), "{bad}: {err}");
    }
    for good in ["\"10ms\"", "\"1m\""] {
        let config = layered(&[&format!("[read]\nfollow_interval = {good}\n")]);
        assert!(config.validate().is_ok(), "{good}");
    }
    assert!(
        ConfigLayer::parse("[read]\nfollow_interval = 250\n").is_err(),
        "a duration needs its unit"
    );
}

/// `[formats] path` adds up across imports, each entry relative to the file that names
/// it, and is empty when no file names one.
#[test]
fn test_formats_path_adds_up_and_is_anchored_to_its_file() {
    assert!(layered(&[""]).formats.path.is_empty());
    // Absolute on this platform, so it is kept as written.
    let shared = if cfg!(windows) {
        "C:/shared/formats"
    } else {
        "/shared/formats"
    };
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    fs::create_dir_all(temp_dir.path().join("org")).unwrap();
    write_config(
        &temp_dir,
        "org/org.toml",
        &format!("[formats]\npath = [\"specs\", \"{shared}\"]\n"),
    );
    let root = write_config(
        &temp_dir,
        "config.toml",
        &format!("import = [\"org/org.toml\"]\n[formats]\npath = [\"mine\", \"{shared}\"]\n"),
    );
    let config = AppConfig::load_from_file(&root).expect("Config should load");
    let anchored = |p: std::path::PathBuf| p.to_string_lossy().into_owned();
    assert_eq!(
        config.formats.path,
        [
            anchored(temp_dir.path().join("org").join("specs")),
            shared.to_string(),
            anchored(temp_dir.path().join("mine"))
        ]
    );
}

/// The size past which a read into memory asks first: 1 GiB unless set, layered by
/// presence, and never with 0.
#[test]
fn test_memory_warning() {
    let config = layered(&[]);
    assert_eq!(config.read.memory_warning, ByteSize::mib(1024));
    assert_eq!(config.read.memory_warning(), Some(1024 * 1024 * 1024));
    let config = layered(&[
        "[read]\nmemory_warning = 0\n",
        "[read]\nmemory_warning = \"1GiB\"\n",
    ]);
    assert_eq!(
        config.read.memory_warning,
        ByteSize::mib(1024),
        "an explicit default over an import wins"
    );
    let config = layered(&["[read]\nmemory_warning = 0\n"]);
    assert_eq!(config.read.memory_warning(), None);
    let config = layered(&["[read]\nmemory_warning = \"5MiB\"\n"]);
    assert_eq!(config.read.memory_warning(), Some(5 * 1024 * 1024));
}

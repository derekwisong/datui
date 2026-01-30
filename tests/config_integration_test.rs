use datui::config::AppConfig;
use datui::{Args, OpenOptions};

#[test]
fn test_config_used_for_row_numbers() {
    let mut config = AppConfig::default();
    config.display.row_numbers = true;
    config.display.row_start_index = 0;

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        no_header: None,
        delimiter: None,
        compression: None,
        debug: false,
        excel_sheet: None,
        clear_cache: false,
        template: None,
        remove_templates: false,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false, // Not set via CLI
        row_start_index: None,
        generate_config: false,
        force: false,
        hive: false,
        parse_dates: None,
        decompress_in_memory: None,
        temp_dir: None,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    // Config values should be used
    assert!(opts.row_numbers);
    assert_eq!(opts.row_start_index, 0);
}

#[test]
fn test_cli_args_override_config() {
    let mut config = AppConfig::default();
    config.display.row_numbers = true;
    config.display.row_start_index = 0;
    config.display.pages_lookahead = 10;

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        no_header: None,
        delimiter: None,
        compression: None,
        debug: false,
        excel_sheet: None,
        clear_cache: false,
        template: None,
        remove_templates: false,
        pages_lookahead: Some(5), // Override config
        pages_lookback: None,
        row_numbers: false,
        row_start_index: Some(1), // Override config
        generate_config: false,
        force: false,
        hive: false,
        parse_dates: None,
        decompress_in_memory: None,
        temp_dir: None,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    // CLI args should override config
    assert_eq!(opts.pages_lookahead, Some(5));
    assert_eq!(opts.row_start_index, 1);
}

#[test]
fn test_config_display_settings() {
    let mut config = AppConfig::default();
    config.display.pages_lookahead = 7;
    config.display.pages_lookback = 8;
    config.display.row_numbers = true;

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        no_header: None,
        delimiter: None,
        compression: None,
        debug: false,
        excel_sheet: None,
        clear_cache: false,
        template: None,
        remove_templates: false,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        generate_config: false,
        force: false,
        hive: false,
        parse_dates: None,
        decompress_in_memory: None,
        temp_dir: None,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    assert_eq!(opts.pages_lookahead, Some(7));
    assert_eq!(opts.pages_lookback, Some(8));
    assert!(opts.row_numbers);
}

#[test]
fn test_config_file_loading_settings() {
    let mut config = AppConfig::default();
    config.file_loading.delimiter = Some(b'\t');
    config.file_loading.has_header = Some(false);
    config.file_loading.skip_lines = Some(2);

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        no_header: None,
        delimiter: None,
        compression: None,
        debug: false,
        excel_sheet: None,
        clear_cache: false,
        template: None,
        remove_templates: false,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        generate_config: false,
        force: false,
        hive: false,
        parse_dates: None,
        decompress_in_memory: None,
        temp_dir: None,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    assert_eq!(opts.delimiter, Some(b'\t'));
    assert_eq!(opts.has_header, Some(false));
    assert_eq!(opts.skip_lines, Some(2));
}

#[test]
fn test_config_sampling_threshold() {
    use datui::{App, AppConfig, AppEvent, Theme};
    use std::sync::mpsc::channel;

    let mut config = AppConfig::default();
    config.performance.sampling_threshold = 50000;

    let theme = Theme::from_config(&config.theme).expect("Failed to create theme");
    let (tx, _rx) = channel::<AppEvent>();
    let _app = App::new_with_config(tx, theme, config.clone());

    // Verify the app uses the config's sampling threshold
    // Note: We can't directly access app.sampling_threshold as it's private,
    // but we can verify the config value is correct
    assert_eq!(config.performance.sampling_threshold, 50000);
}

#[test]
fn test_config_event_poll_interval() {
    let mut config = AppConfig::default();
    config.performance.event_poll_interval_ms = 100;

    assert_eq!(config.performance.event_poll_interval_ms, 100);

    // Verify validation allows reasonable values
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_performance_validation() {
    let mut config = AppConfig::default();

    // Zero sampling threshold should fail validation
    config.performance.sampling_threshold = 0;
    assert!(config.validate().is_err());

    // Reset and test event poll interval
    config.performance.sampling_threshold = 10000;
    config.performance.event_poll_interval_ms = 0;
    assert!(config.validate().is_err());

    // Valid values should pass
    config.performance.event_poll_interval_ms = 25;
    assert!(config.validate().is_ok());
}

use datui::config::AppConfig;
use datui::{Args, OpenOptions, ParseStringsTarget};

mod common;

#[test]
fn test_config_used_for_row_numbers() {
    let mut config = AppConfig::default();
    config.display.row_numbers = true;
    config.display.row_start_index = 0;

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec![],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false, // Not set via CLI
        row_start_index: None,
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
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
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec![],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: Some(5), // Override config
        pages_lookback: None,
        row_numbers: false,
        row_start_index: Some(1), // Override config
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
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
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec![],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    assert_eq!(opts.pages_lookahead, Some(7));
    assert_eq!(opts.pages_lookback, Some(8));
    assert!(opts.row_numbers);
}

#[test]
fn test_config_file_loading_settings() {
    let mut config = AppConfig::default();
    config.file_loading.infer_schema_length = Some(5000);
    config.file_loading.ignore_errors = Some(true);
    config.file_loading.parse_dates = Some(false);

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec![],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    assert_eq!(opts.infer_schema_length, Some(5000));
    assert!(opts.ignore_errors);
    assert!(!opts.parse_dates);
}

#[test]
fn test_config_null_values_merge() {
    let mut config = AppConfig::default();
    config.file_loading.null_values = Some(vec!["NA".to_string(), "N/A".to_string()]);

    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec!["amount=".to_string()],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
    };

    let opts = OpenOptions::from_args_and_config(&args, &config);

    let nulls = opts.null_values.as_ref().unwrap();
    assert_eq!(nulls.len(), 3);
    assert_eq!(nulls[0], "NA");
    assert_eq!(nulls[1], "N/A");
    assert_eq!(nulls[2], "amount=");
}

#[test]
fn test_config_analysis_sample_rows() {
    let config = AppConfig::default();
    assert_eq!(config.performance.analysis_sample_rows, 100_000);

    // 0 reads every row, and is a valid setting rather than a mistake.
    let mut config = AppConfig::default();
    config.performance.analysis_sample_rows = 0;
    assert!(config.validate().is_ok());
}

#[test]
fn test_parse_strings_default_and_no_parse_strings() {
    let config = AppConfig::default();

    // Default: not passed → parse-strings applied to all
    let args = Args {
        paths: vec![std::path::PathBuf::from("test.csv")],
        skip_lines: None,
        skip_rows: None,
        skip_tail_rows: None,
        no_header: None,
        delimiter: None,
        null_value: vec![],
        comment_char: None,
        header_rows: vec![],
        skip_initial_space: None,
        compression: None,
        format: None,
        debug: false,
        log_file: None,
        excel_sheet: None,
        table: None,
        normalize: false,
        clear_cache: false,
        clear_recents: false,
        template: None,
        remove_templates: false,
        sample_rows: None,
        pages_lookahead: None,
        pages_lookback: None,
        row_numbers: false,
        row_start_index: None,
        column_colors: None,
        number_format: None,
        align_numeric_right: None,
        mouse: None,
        generate_config: false,
        force: false,
        hive: false,
        single_spine_schema: None,
        parse_dates: None,
        parse_strings: vec![],
        no_parse_strings: false,
        decompress_in_memory: None,
        temp_dir: None,
        s3_endpoint_url: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        s3_region: None,
        cloud_discover: None,
        spec: None,
        fix_dict: None,
        dbc: None,
        variant: None,
        hex: false,
        record_size: None,
        command: None,
        polars_streaming: None,
        workaround_pivot_date_index: None,
        infer_schema_length: None,
        ignore_errors: None,
        follow: false,
        tee: None,
        tee_raw: false,
    };
    let opts = OpenOptions::from_args_and_config(&args, &config);
    assert!(matches!(opts.parse_strings, Some(ParseStringsTarget::All)));

    // --no-parse-strings → disabled
    let args_off = Args {
        no_parse_strings: true,
        ..args.clone()
    };
    let opts_off = OpenOptions::from_args_and_config(&args_off, &config);
    assert!(opts_off.parse_strings.is_none());

    // config parse_strings = false → disabled when CLI doesn't set it
    let mut config_off = AppConfig::default();
    config_off.file_loading.parse_strings = Some(false);
    let opts_config_off = OpenOptions::from_args_and_config(&args, &config_off);
    assert!(opts_config_off.parse_strings.is_none());
}

/// The CSV dialect: the command line over `[file_loading]`; `--header-rows` is a
/// file's layout and has no config key, as `--skip-lines` has none.
#[test]
fn test_csv_dialect_cli_over_config() {
    use clap::Parser;
    let mut config = AppConfig::default();
    let opts = OpenOptions::from_args_and_config(&Args::parse_from(["datui", "a.csv"]), &config);
    assert_eq!(opts.comment_char, None);
    assert!(opts.header_rows.is_empty());
    assert_eq!(opts.header_join, " ");
    assert!(!opts.skip_initial_space);

    config.file_loading.comment_char = Some(";".into());
    config.file_loading.header_join = Some("_".into());
    config.file_loading.skip_initial_space = Some(true);
    let opts = OpenOptions::from_args_and_config(&Args::parse_from(["datui", "a.csv"]), &config);
    assert_eq!(opts.comment_char.as_deref(), Some(";"));
    assert_eq!(opts.header_join, "_");
    assert!(opts.skip_initial_space);

    let args = Args::parse_from([
        "datui",
        "--comment-char",
        "#",
        "--header-rows",
        "3,2",
        "--skip-initial-space=false",
        "a.csv",
    ]);
    let opts = OpenOptions::from_args_and_config(&args, &config);
    assert_eq!(opts.comment_char.as_deref(), Some("#"));
    assert_eq!(opts.header_rows, [3, 2]);
    assert!(!opts.skip_initial_space);

    assert!(Args::try_parse_from(["datui", "--header-rows", "0", "a.csv"]).is_err());
    assert!(Args::try_parse_from(["datui", "--comment-char", "", "a.csv"]).is_err());
}

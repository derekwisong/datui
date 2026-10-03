use clap::Parser;
use datui::config::AppConfig;
use datui::{Args, OpenOptions, ParseStringsTarget};

mod common;

fn args(flags: &[&str]) -> Args {
    Args::try_parse_from(std::iter::once("datui").chain(flags.iter().copied())).expect("parses")
}

#[test]
fn test_config_used_for_row_numbers() {
    let mut config = AppConfig::default();
    config.display.row_numbers = true;
    config.display.row_numbers_start = 0;

    let opts = OpenOptions::from_args_and_config(&args(&["a.csv"]), &config);

    assert!(opts.row_numbers);
    assert_eq!(opts.row_start_index, 0);
}

/// `--row-numbers=false` turns off what the config turns on, and the bare flag on.
#[test]
fn test_row_numbers_flag_over_config() {
    let mut config = AppConfig::default();
    config.display.row_numbers = true;
    let opts = OpenOptions::from_args_and_config(&args(&["--row-numbers=false"]), &config);
    assert!(!opts.row_numbers);
    config.display.row_numbers = false;
    let opts = OpenOptions::from_args_and_config(&args(&["--row-numbers"]), &config);
    assert!(opts.row_numbers);
}

#[test]
fn test_config_display_settings() {
    let mut config = AppConfig::default();
    config.performance.pages_ahead = 7;
    config.performance.pages_behind = 8;
    config.display.row_numbers = true;

    let opts = OpenOptions::from_args_and_config(&args(&[]), &config);

    assert_eq!(opts.pages_lookahead, Some(7));
    assert_eq!(opts.pages_lookback, Some(8));
    assert!(opts.row_numbers);
}

#[test]
fn test_config_read_and_csv_settings() {
    let mut config = AppConfig::default();
    config.csv.infer_rows = 5000;
    config.csv.ignore_errors = true;
    config.read.infer_types = datui::config::InferTypes::Switch(false);

    let opts = OpenOptions::from_args_and_config(&args(&[]), &config);

    assert_eq!(opts.infer_schema_length, Some(5000));
    assert_eq!(
        opts.parse_strings_sample_rows, 5000,
        "one row count for one guess"
    );
    assert!(opts.ignore_errors);
    assert!(!opts.parse_dates);

    let opts = OpenOptions::from_args_and_config(&args(&["--infer-rows", "20"]), &config);
    assert_eq!(opts.infer_schema_length, Some(20));
    assert_eq!(opts.parse_strings_sample_rows, 20);
}

/// `--null` replaces the config's list, as every flag replaces its key.
#[test]
fn test_null_flag_replaces_the_config_list() {
    let mut config = AppConfig::default();
    config.csv.null_values = vec!["NA".to_string(), "N/A".to_string()];

    let opts = OpenOptions::from_args_and_config(&args(&[]), &config);
    assert_eq!(opts.null_values.as_deref().unwrap(), ["NA", "N/A"]);

    let opts = OpenOptions::from_args_and_config(&args(&["--null", "amount="]), &config);
    assert_eq!(opts.null_values.as_deref().unwrap(), ["amount="]);
}

#[test]
fn test_config_analysis_sample_rows() {
    let config = AppConfig::default();
    assert_eq!(config.analysis.sample_rows, 100_000);

    // 0 reads every row, and is a valid setting rather than a mistake.
    let mut config = AppConfig::default();
    config.analysis.sample_rows = 0;
    assert!(config.validate().is_ok());
}

#[test]
fn test_infer_types_over_config() {
    let config = AppConfig::default();

    let opts = OpenOptions::from_args_and_config(&args(&[]), &config);
    assert!(matches!(opts.parse_strings, Some(ParseStringsTarget::All)));
    assert!(opts.parse_dates);

    let opts = OpenOptions::from_args_and_config(&args(&["--infer-types=off"]), &config);
    assert!(opts.parse_strings.is_none());
    assert!(!opts.parse_dates);

    let opts = OpenOptions::from_args_and_config(&args(&["--infer-types=a,b"]), &config);
    assert!(
        matches!(&opts.parse_strings, Some(ParseStringsTarget::Columns(c)) if c == &["a", "b"])
    );

    // The config turns it off when no flag says otherwise; the flag turns it back on.
    let mut config_off = AppConfig::default();
    config_off.read.infer_types = datui::config::InferTypes::Switch(false);
    let opts = OpenOptions::from_args_and_config(&args(&[]), &config_off);
    assert!(opts.parse_strings.is_none());
    let opts = OpenOptions::from_args_and_config(&args(&["--infer-types"]), &config_off);
    assert!(matches!(opts.parse_strings, Some(ParseStringsTarget::All)));
}

/// The CSV dialect: the command line over `[csv]`; `--header-rows` is a
/// file's layout and has no config key, as `--skip-lines` has none.
#[test]
fn test_csv_dialect_cli_over_config() {
    let mut config = AppConfig::default();
    let opts = OpenOptions::from_args_and_config(&Args::parse_from(["datui", "a.csv"]), &config);
    assert_eq!(opts.comment_char, None);
    assert!(opts.header_rows.is_empty());
    assert_eq!(opts.header_join, " ");
    assert!(!opts.skip_initial_space);

    config.csv.comment = Some(";".into());
    config.csv.header_join = "_".into();
    config.csv.skip_initial_space = true;
    let opts = OpenOptions::from_args_and_config(&Args::parse_from(["datui", "a.csv"]), &config);
    assert_eq!(opts.comment_char.as_deref(), Some(";"));
    assert_eq!(opts.header_join, "_");
    assert!(opts.skip_initial_space);

    let args = Args::parse_from([
        "datui",
        "--comment",
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
    assert!(Args::try_parse_from(["datui", "--comment", "", "a.csv"]).is_err());
}

/// The open's own flags: the table, the spec file, the dictionaries, the view.
#[test]
fn test_open_flags_reach_the_options() {
    let config = AppConfig::default();
    let opts = OpenOptions::from_args_and_config(
        &args(&[
            "x.bin",
            "-t",
            "Sales",
            "--format",
            "./acme.toml",
            "--dict",
            "a.xml",
            "--dict",
            "b.dbc",
            "--view",
            "daily",
            "--hex-width",
            "32",
            "--footer-rows",
            "2",
        ]),
        &config,
    );
    assert_eq!(opts.table.as_deref(), Some("Sales"));
    assert_eq!(opts.spec_file, Some("./acme.toml".into()));
    assert_eq!(opts.spec_name, None);
    assert_eq!(
        opts.dicts,
        [std::path::PathBuf::from("a.xml"), "b.dbc".into()]
    );
    assert_eq!(opts.view.as_deref(), Some("daily"));
    assert_eq!(opts.record_size, Some(32));
    assert_eq!(opts.skip_tail_rows, Some(2));
}

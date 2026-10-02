use clap::Parser;
use color_eyre::Result;
use datui::{APP_NAME, Args, RunInput, error_display};
use datui::{ConfigManager, TemplateManager};

fn handle_early_exit_flags(args: &Args) -> Result<Option<()>> {
    if let Some(datui::cli::Command::Formats { action }) = &args.command {
        let config = match datui::AppConfig::load(APP_NAME) {
            Ok(config) => config,
            Err(e) => {
                eprintln!(
                    "Error: {}",
                    error_display::user_message_from_report(&e, None)
                );
                std::process::exit(1);
            }
        };
        let (text, code) = datui::formats::command(action.as_ref(), &config);
        if code == 0 {
            print!("{text}");
        } else {
            eprint!("{text}");
        }
        std::process::exit(code);
    }

    if args.generate_config {
        match ConfigManager::new(APP_NAME) {
            Ok(config_manager) => match config_manager.write_default_config(args.force) {
                Ok(path) => {
                    println!("Configuration file written to: {}", path.display());
                    return Ok(Some(()));
                }
                Err(e) => {
                    eprintln!(
                        "Error: {}",
                        error_display::user_message_from_report(&e, None)
                    );
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!(
                    "Error: {}",
                    error_display::user_message_from_report(&e, None)
                );
                std::process::exit(1);
            }
        }
    }

    if args.clear_recents {
        match datui::CacheManager::new(APP_NAME) {
            Ok(cache) => {
                cache.clear_recents();
                println!("Recently opened datasets forgotten");
                return Ok(Some(()));
            }
            Err(_e) => {
                println!("No recents to clear");
                return Ok(Some(()));
            }
        }
    }

    if args.clear_cache {
        match datui::CacheManager::new(APP_NAME) {
            Ok(cache) => {
                if let Err(e) = cache.clear_all() {
                    eprintln!(
                        "Error: {}",
                        error_display::user_message_from_report(&e, None)
                    );
                    std::process::exit(1);
                }
                println!("Cache cleared successfully");
                return Ok(Some(()));
            }
            Err(_e) => {
                println!("No cache to clear");
                return Ok(Some(()));
            }
        }
    }

    if args.remove_templates {
        match ConfigManager::new(APP_NAME) {
            Ok(config) => match TemplateManager::new(&config) {
                Ok(mut template_manager) => {
                    if let Err(e) = template_manager.remove_all_templates() {
                        eprintln!(
                            "Error: {}",
                            error_display::user_message_from_report(&e, None)
                        );
                        std::process::exit(1);
                    }
                    println!("All templates removed successfully");
                    return Ok(Some(()));
                }
                Err(e) => {
                    eprintln!(
                        "Error: {}",
                        error_display::user_message_from_report(&e, None)
                    );
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!(
                    "Error: {}",
                    error_display::user_message_from_report(&e, None)
                );
                std::process::exit(1);
            }
        }
    }

    Ok(None)
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(()) = handle_early_exit_flags(&args)? {
        return Ok(());
    }

    // The configuration is read, and these flags applied over it, behind the first
    // frame: a config on a slow mount must not hold up the screen.
    let ran = datui::run(RunInput::Cli(Box::new(args)), None);
    // Cleaned up after, but ended by the signal as far as the caller can tell. A
    // terminal that hung up may have failed the session on its way out; that is the
    // signal's doing, not an error to print to it.
    if let Some(signal) = datui::ended_by_signal() {
        std::process::exit(128 + signal);
    }
    if let Err(e) = ran {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datui::{Args, OpenOptions};
    use std::path::PathBuf;

    #[test]
    fn test_args_to_open_options() {
        let args = Args {
            paths: vec![PathBuf::from("test.csv")],
            skip_lines: Some(1),
            skip_rows: Some(2),
            skip_tail_rows: None,
            no_header: Some(true),
            delimiter: Some(b','),
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
            generate_config: false,
            force: false,
            hive: false,
            single_spine_schema: None,
            column_colors: None,
            number_format: None,
            align_numeric_right: None,
            mouse: None,
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
            command: None,
            polars_streaming: None,
            workaround_pivot_date_index: None,
            infer_schema_length: None,
            ignore_errors: None,
        };
        let opts: OpenOptions = (&args).into();
        assert_eq!(opts.skip_lines, Some(1));
        assert_eq!(opts.skip_rows, Some(2));
        assert_eq!(opts.has_header, Some(false));
        assert_eq!(opts.delimiter, Some(b','));
    }

    #[test]
    fn test_no_path_opens_home_screen() {
        use clap::Parser;

        // No PATH is no longer an error: datui starts at its home screen so you can
        // pick a dataset without having to name one on the command line first.
        let args = Args::try_parse_from(vec!["datui"]).expect("no-path invocation is valid");
        assert!(args.paths.is_empty());
    }

    #[test]
    fn test_path_not_required_with_generate_config() {
        use clap::Parser;

        let result = Args::try_parse_from(vec!["datui", "--generate-config"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.paths.is_empty());
        assert!(args.generate_config);
    }

    #[test]
    fn test_log_file_flag() {
        use clap::Parser;

        let args = Args::try_parse_from(vec!["datui", "--log-file", "/tmp/x.log", "a.csv"])
            .expect("--log-file takes a path");
        assert_eq!(args.log_file, Some(PathBuf::from("/tmp/x.log")));
        assert_eq!(Args::try_parse_from(vec!["datui"]).unwrap().log_file, None);
    }

    #[test]
    fn test_path_not_required_with_clear_cache() {
        use clap::Parser;

        let result = Args::try_parse_from(vec!["datui", "--clear-cache"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.paths.is_empty());
        assert!(args.clear_cache);
    }

    #[test]
    fn test_path_not_required_with_remove_templates() {
        use clap::Parser;

        let result = Args::try_parse_from(vec!["datui", "--remove-templates"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.paths.is_empty());
        assert!(args.remove_templates);
    }

    #[test]
    fn test_path_accepted_with_generate_config() {
        use clap::Parser;

        let result = Args::try_parse_from(vec!["datui", "--generate-config", "test.csv"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert_eq!(args.paths, vec![PathBuf::from("test.csv")]);
        assert!(args.generate_config);
    }
}

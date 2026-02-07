use clap::Parser;
use color_eyre::Result;
use datui::{error_display, Args, OpenOptions, RunInput, APP_NAME};
use datui::{AppConfig, ConfigManager, TemplateManager};

fn handle_early_exit_flags(args: &Args) -> Result<Option<()>> {
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

    let mut config = AppConfig::load(APP_NAME).unwrap_or_else(|e| {
        eprintln!("Warning: Failed to load config: {}. Using defaults.", e);
        AppConfig::default()
    });

    if let Some(cc) = args.column_colors {
        config.display.column_colors = cc;
    }

    if let Some(st) = args.sampling_threshold {
        config.performance.sampling_threshold = if st == 0 { None } else { Some(st) };
    }

    if let Some(ps) = args.polars_streaming {
        config.performance.polars_streaming = ps;
    }

    let opts = OpenOptions::from_args_and_config(&args, &config);
    let input = RunInput::Paths(args.paths.clone(), opts);

    if let Err(e) = datui::run(input, Some(config), args.debug) {
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
            no_header: Some(true),
            delimiter: Some(b','),
            compression: None,
            debug: false,
            excel_sheet: None,
            clear_cache: false,
            template: None,
            remove_templates: false,
            sampling_threshold: None,
            pages_lookahead: None,
            pages_lookback: None,
            row_numbers: false,
            row_start_index: None,
            generate_config: false,
            force: false,
            hive: false,
            column_colors: None,
            parse_dates: None,
            decompress_in_memory: None,
            temp_dir: None,
            s3_endpoint_url: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            polars_streaming: None,
        };
        let opts: OpenOptions = (&args).into();
        assert_eq!(opts.skip_lines, Some(1));
        assert_eq!(opts.skip_rows, Some(2));
        assert_eq!(opts.has_header, Some(false));
        assert_eq!(opts.delimiter, Some(b','));
    }

    #[test]
    fn test_path_required_for_normal_operation() {
        use clap::Parser;

        let result = Args::try_parse_from(vec!["datui"]);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        assert!(err.to_string().contains("PATH"));
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

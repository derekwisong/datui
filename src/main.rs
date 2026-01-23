use clap::Parser;
use color_eyre::Result;
use datui::{App, AppConfig, AppEvent, Args, OpenOptions, Theme, APP_NAME};
use ratatui::DefaultTerminal;
use std::sync::mpsc::channel;

fn render(terminal: &mut DefaultTerminal, app: &mut App) -> Result<()> {
    terminal.draw(|frame| frame.render_widget(app, frame.area()))?;
    Ok(())
}

fn run(mut terminal: DefaultTerminal, args: &Args, config: &AppConfig, theme: Theme) -> Result<()> {
    let (tx, rx) = channel::<AppEvent>();
    let mut app = App::new_with_config(tx.clone(), theme, config.clone());
    if args.debug {
        app.enable_debug();
    }
    // Create OpenOptions with config defaults, CLI args override
    let opts = OpenOptions::from_args_and_config(args, config);
    render(&mut terminal, &mut app)?;

    // Path should be present if we got here (required_unless_present ensures this)
    let path = args.path.as_ref().expect("Path should be present");
    tx.send(AppEvent::Open(path.clone(), opts))?;

    loop {
        if crossterm::event::poll(std::time::Duration::from_millis(
            config.performance.event_poll_interval_ms,
        ))? {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key) => tx.send(AppEvent::Key(key))?,
                crossterm::event::Event::Resize(cols, rows) => {
                    tx.send(AppEvent::Resize(cols, rows))?
                }
                _ => {}
            }
        }

        let updated = match rx.recv_timeout(std::time::Duration::from_millis(0)) {
            Ok(event) => {
                match event {
                    AppEvent::Exit => break,
                    AppEvent::Crash(msg) => {
                        return Err(color_eyre::eyre::eyre!(msg));
                    }
                    event => {
                        if let Some(event) = app.event(&event) {
                            tx.send(event)?;
                        }
                    }
                }
                true
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => false,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if updated {
            render(&mut terminal, &mut app)?;
        }
    }
    Ok(())
}

fn handle_early_exit_flags(args: &Args) -> Result<Option<()>> {
    if args.generate_config {
        match datui::ConfigManager::new(datui::APP_NAME) {
            Ok(config_manager) => match config_manager.write_default_config(args.force) {
                Ok(path) => {
                    println!("Configuration file written to: {}", path.display());
                    return Ok(Some(()));
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("Error initializing config manager: {}", e);
                std::process::exit(1);
            }
        }
    }

    if args.clear_cache {
        match datui::CacheManager::new(datui::APP_NAME) {
            Ok(cache) => {
                if let Err(e) = cache.clear_all() {
                    eprintln!("Error clearing cache: {}", e);
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
        match datui::ConfigManager::new(datui::APP_NAME) {
            Ok(config) => {
                use datui::TemplateManager;
                match TemplateManager::new(&config) {
                    Ok(mut template_manager) => {
                        if let Err(e) = template_manager.remove_all_templates() {
                            eprintln!("Error removing templates: {}", e);
                            std::process::exit(1);
                        }
                        println!("All templates removed successfully");
                        return Ok(Some(()));
                    }
                    Err(e) => {
                        eprintln!("Error initializing template manager: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error initializing config manager: {}", e);
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

    // Load configuration (default + user config)
    let config = AppConfig::load(APP_NAME).unwrap_or_else(|e| {
        eprintln!("Warning: Failed to load config: {}. Using defaults.", e);
        AppConfig::default()
    });

    // Create theme from config
    let theme = Theme::from_config(&config.theme).unwrap_or_else(|e| {
        eprintln!(
            "Warning: Failed to create theme from config: {}. Using default theme.",
            e
        );
        Theme::from_config(&AppConfig::default().theme)
            .expect("Default theme should always be valid")
    });

    color_eyre::install()?;
    let terminal = ratatui::init();
    let result = run(terminal, &args, &config, theme);
    ratatui::restore();
    if let Err(e) = result {
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
            path: Some(PathBuf::from("test.csv")),
            skip_lines: Some(1),
            skip_rows: Some(2),
            no_header: Some(true),
            delimiter: Some(b','),
            compression: None,
            debug: false,
            clear_cache: false,
            template: None,
            remove_templates: false,
            pages_lookahead: None,
            pages_lookback: None,
            row_numbers: false,
            row_start_index: None,
            generate_config: false,
            force: false,
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

        // Test that path is required when no special flags are present
        let result = Args::try_parse_from(vec!["datui"]);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        assert!(err.to_string().contains("PATH"));
    }

    #[test]
    fn test_path_not_required_with_generate_config() {
        use clap::Parser;

        // Path should not be required with --generate-config
        let result = Args::try_parse_from(vec!["datui", "--generate-config"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.path.is_none());
        assert!(args.generate_config);
    }

    #[test]
    fn test_path_not_required_with_clear_cache() {
        use clap::Parser;

        // Path should not be required with --clear-cache
        let result = Args::try_parse_from(vec!["datui", "--clear-cache"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.path.is_none());
        assert!(args.clear_cache);
    }

    #[test]
    fn test_path_not_required_with_remove_templates() {
        use clap::Parser;

        // Path should not be required with --remove-templates
        let result = Args::try_parse_from(vec!["datui", "--remove-templates"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert!(args.path.is_none());
        assert!(args.remove_templates);
    }

    #[test]
    fn test_path_accepted_with_generate_config() {
        use clap::Parser;

        // Path should still be accepted even with --generate-config
        let result = Args::try_parse_from(vec!["datui", "--generate-config", "test.csv"]);
        assert!(result.is_ok());

        let args = result.unwrap();
        assert_eq!(args.path, Some(PathBuf::from("test.csv")));
        assert!(args.generate_config);
    }
}

use clap::Parser;
use color_eyre::Result;
use datui::cli::Command;
use datui::{APP_NAME, Args, ConfigManager, RunInput, error_display};
use datui_cli::exit;
use std::io::IsTerminal;

/// Run a command that opens no data, and exit with its code.
fn run_command(command: &Command, args: &Args) -> ! {
    let fail = |e: color_eyre::Report| {
        (
            format!("{}\n", error_display::user_message_from_report(&e, None)),
            exit::FAILURE,
        )
    };
    let (text, code) = match command {
        Command::Formats { action } => match datui::AppConfig::load_with(APP_NAME, &args.config) {
            Ok(config) => datui::formats::command(action.as_ref(), args, &config),
            Err(e) => fail(e),
        },
        Command::Config { action } => match ConfigManager::new(APP_NAME) {
            Ok(manager) => datui::config_command::command(&manager, action, &args.config),
            Err(e) => fail(e),
        },
        Command::Catalog { action } => {
            datui::catalog::command(action, datui::AppConfig::load_with(APP_NAME, &args.config))
        }
        Command::Cache { action } => {
            let cache = datui::CacheManager::new(APP_NAME).ok();
            datui::commands::cache(cache.as_ref(), action)
        }
        Command::Completions { shell } => (datui::cli::completions(*shell), exit::SUCCESS),
        Command::Man { page, list, dir } => datui::commands::man(
            page.as_deref(),
            *list,
            dir.as_deref(),
            std::io::stdout().is_terminal(),
        ),
        Command::Views { action } => match ConfigManager::new(APP_NAME) {
            Ok(manager) => datui::commands::views(&manager, action),
            Err(e) => fail(e),
        },
    };
    if code == exit::SUCCESS {
        print!("{text}");
    } else if matches!(command, Command::Formats { .. } | Command::Catalog { .. }) {
        // A failed check is a report of its own.
        eprint!("{text}");
    } else {
        eprint!("Error: {text}");
    }
    std::process::exit(code);
}

fn main() -> Result<()> {
    // A usage error ends with clap's status, exit::USAGE.
    let args = Args::parse();

    if let Some(command) = &args.command {
        run_command(command, &args);
    }

    // The configuration is read, and these flags applied over it, behind the first
    // frame: a config on a slow mount must not hold up the screen.
    let ran = datui::run(RunInput::Cli(Box::new(args)), None);
    // Cleaned up after, but ended by the signal as far as the caller can tell. A
    // terminal that hung up may have failed the session on its way out; that is the
    // signal's doing, not an error to print to it.
    if let Some(status) = datui::ended_by_signal() {
        std::process::exit(status);
    }
    if let Err(e) = ran {
        eprintln!("Error: {}", e);
        std::process::exit(exit::FAILURE);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use datui::{Args, OpenOptions};
    use std::path::PathBuf;

    #[test]
    fn test_args_to_open_options() {
        let args = Args::try_parse_from([
            "datui",
            "test.csv",
            "--skip-lines",
            "1",
            "--skip-rows",
            "2",
            "--no-header",
            "--delimiter",
            ",",
        ])
        .unwrap();
        let opts: OpenOptions = (&args).into();
        assert_eq!(opts.skip_lines, Some(1));
        assert_eq!(opts.skip_rows, Some(2));
        assert_eq!(opts.has_header, Some(false));
        assert_eq!(opts.delimiter, Some(b','));
    }

    #[test]
    fn test_no_path_opens_home_screen() {
        // No PATH is no longer an error: datui starts at its home screen so you can
        // pick a dataset without having to name one on the command line first.
        let args = Args::try_parse_from(vec!["datui"]).expect("no-path invocation is valid");
        assert!(args.paths.is_empty());
    }

    #[test]
    fn test_log_flags() {
        let args = Args::try_parse_from(vec![
            "datui",
            "--log-file",
            "/tmp/x.log",
            "--log-level",
            "debug",
            "a.csv",
        ])
        .expect("--log-file takes a path");
        assert_eq!(args.log_file, Some(PathBuf::from("/tmp/x.log")));
        assert_eq!(args.log_level.as_deref(), Some("debug"));
        assert_eq!(Args::try_parse_from(vec!["datui"]).unwrap().log_file, None);
        assert!(Args::try_parse_from(vec!["datui", "--log-level", "loud"]).is_err());
    }
}

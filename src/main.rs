use clap::Parser;
use color_eyre::Result;
use datui::{App, AppEvent, OpenOptions};
use ratatui::DefaultTerminal;
use std::path::PathBuf;
use std::sync::mpsc::channel;

#[derive(Parser, Debug)]
#[command(version, about = "datui")]
struct Args {
    path: PathBuf,

    /// Skip this many lines when reading a file
    #[arg(long = "skip-lines")]
    skip_lines: Option<usize>,

    /// Skip this many rows when reading a file
    #[arg(long = "skip-rows")]
    skip_rows: Option<usize>,

    /// Specify that the file has no header
    #[arg(long = "no-header")]
    no_header: Option<bool>,

    /// Specify the delimiter to use when reading a file
    #[arg(long = "delimiter")]
    delimiter: Option<u8>,

    /// Enable debug mode to show operational information
    #[arg(long = "debug", action)]
    debug: bool,

    /// Clear all cache data and exit
    #[arg(long = "clear-cache", action)]
    clear_cache: bool,

    /// Apply a template by name when starting the application
    #[arg(long = "template")]
    template: Option<String>,

    /// Remove all templates and exit
    #[arg(long = "remove-templates", action)]
    remove_templates: bool,
}

impl From<&Args> for OpenOptions {
    fn from(args: &Args) -> Self {
        let mut opts = OpenOptions::new();
        if let Some(skip_lines) = args.skip_lines {
            opts = opts.with_skip_lines(skip_lines);
        }
        if let Some(skip_rows) = args.skip_rows {
            opts = opts.with_skip_rows(skip_rows);
        }
        if let Some(no_header) = args.no_header {
            opts = opts.with_has_header(!no_header);
        }
        if let Some(delimiter) = args.delimiter {
            opts = opts.with_delimiter(delimiter);
        }

        opts
    }
}

fn render(terminal: &mut DefaultTerminal, app: &mut App) -> Result<()> {
    terminal.draw(|frame| frame.render_widget(app, frame.area()))?;
    Ok(())
}

fn run(mut terminal: DefaultTerminal, args: &Args) -> Result<()> {
    let (tx, rx) = channel::<AppEvent>();
    let mut app = App::new(tx.clone());
    if args.debug {
        app.enable_debug();
    }
    let opts: OpenOptions = args.into();
    render(&mut terminal, &mut app)?;
    tx.send(AppEvent::Open(args.path.clone(), opts))?;

    loop {
        if crossterm::event::poll(std::time::Duration::from_millis(25))? {
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

    color_eyre::install()?;
    let terminal = ratatui::init();
    let result = run(terminal, &args);
    ratatui::restore();
    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_args_to_open_options() {
        let args = Args {
            path: PathBuf::new(),
            skip_lines: Some(1),
            skip_rows: Some(2),
            no_header: Some(true),
            delimiter: Some(b','),
            debug: false,
            clear_cache: false,
            template: None,
            remove_templates: false,
        };
        let opts: OpenOptions = (&args).into();
        assert_eq!(opts.skip_lines, Some(1));
        assert_eq!(opts.skip_rows, Some(2));
        assert_eq!(opts.has_header, Some(false));
        assert_eq!(opts.delimiter, Some(b','));
    }
}

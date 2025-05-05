use clap::Parser;
use color_eyre::Result;
use crossterm::{self, event::KeyCode};
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
                crossterm::event::Event::Resize(cols, rows) => tx.send(AppEvent::Resize(cols, rows))?,
                _ => {}
            }
        }

        let updated = match rx.recv_timeout(std::time::Duration::from_millis(0)) {
            Ok(event) => {
                match event {
                    AppEvent::Key(event) if event.code == KeyCode::Esc => break,
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

fn main() -> Result<()> {
    let args = Args::parse();
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

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
    #[arg(long="skip-lines")]
    skip_lines: Option<usize>,

    /// Skip this many rows when reading a file
    #[arg(long="skip-rows")]
    skip_rows: Option<usize>,

    /// Specify that the file has no header
    #[arg(long="no-header")]
    no_header: Option<bool>,

    /// Specify the delimiter to use when reading a file
    #[arg(long="delimiter")]
    delimiter: Option<u8>,
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
    let opts: OpenOptions = args.into();
    render(&mut terminal, &mut app)?;
    tx.send(AppEvent::Open(args.path.clone(), opts))?;

    loop {
        if crossterm::event::poll(std::time::Duration::from_millis(0))? {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key) => tx.send(AppEvent::Key(key))?,
                crossterm::event::Event::Resize(_, _) => tx.send(AppEvent::Collect)?,
                _ => {}
            }
        }

        match rx.recv_timeout(std::time::Duration::from_millis(0)) {
            Ok(event) => match event {
                AppEvent::Key(event) if event.code == KeyCode::Esc => break,
                AppEvent::Exit => break,
                event => {
                    if let Some(event) = app.event(&event) {
                        tx.send(event)?;
                    }
                }
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }

        render(&mut terminal, &mut app)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    color_eyre::install()?;
    let terminal = ratatui::init();
    let result = run(terminal, &args);
    ratatui::restore();
    result
}

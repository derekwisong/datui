use color_eyre::Result;
use crossterm;
use ratatui::DefaultTerminal;
use std::path::PathBuf;

use datui::{App, AppEvent};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about = "datui")]
struct Args {
  path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    color_eyre::install()?;
    let terminal = ratatui::init();

    let mut app = App::default();
    app.event(&AppEvent::Open(args.path))?;

    let result = run(terminal, app);
    ratatui::restore();
    result
}

fn run(mut terminal: DefaultTerminal, mut app: App) -> Result<()> {
    while app.is_running() {
        terminal.draw(|frame| frame.render_widget(&app, frame.area()))?;
        match crossterm::event::read()? {
            crossterm::event::Event::Key(key) => app.event(&AppEvent::Key(key)),
            _ => Ok(())
        }?;
    }
    Ok(())
}

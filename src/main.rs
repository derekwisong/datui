use color_eyre::Result;
use crossterm::{self, event::KeyCode};
use ratatui::DefaultTerminal;
use std::path::PathBuf;

use std::sync::mpsc::{channel, Sender};

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
    let result = run(terminal, &args);
    ratatui::restore();
    result
}

fn input_loop(tx_input: Sender<AppEvent>) -> Result<()> {
    loop {
        match crossterm::event::read()? {
            crossterm::event::Event::Key(key) => {
                let key_event = AppEvent::Key(key);
                tx_input.send(key_event)?;
            }
            crossterm::event::Event::Resize(_, _) => {
                let resize_event = AppEvent::Updated;
                tx_input.send(resize_event)?;
            }
            _ => {}
        }
    }
}

fn run(mut terminal: DefaultTerminal, args: &Args) -> Result<()> {
    let (tx, rx) = channel::<AppEvent>();

    let tx_input = tx.clone();
    std::thread::spawn(move || input_loop(tx_input));

    let mut app = App::new(tx.clone());
    tx.send(AppEvent::Open(args.path.clone()))?;

    while let Ok(event) = rx.recv() {
        match event {
            AppEvent::Key(event) if event.code == KeyCode::Esc => break,
            AppEvent::Exit => break,
            AppEvent::Updated => {},
            _ => app.event(&event)?,
        }
        terminal.draw(|frame| frame.render_widget(&mut app, frame.area()))?;
    }

    Ok(())
}

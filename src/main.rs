use color_eyre::Result;
use crossterm::{self, event::KeyCode};
use ratatui::DefaultTerminal;
use std::path::PathBuf;

use std::sync::mpsc::channel;

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

fn run(mut terminal: DefaultTerminal, args: &Args) -> Result<()> {
    let (tx, rx) = channel::<AppEvent>();

    // input thread
    {
        let tx_input = tx.clone();
        std::thread::spawn(move || {
            loop {
                if let Ok(event) = crossterm::event::read() {
                    if let crossterm::event::Event::Key(key) = event {
                        let key_event = AppEvent::Key(key);
                        tx_input.send(key_event).unwrap();
                    }
                }
            }
        });
    }


    let mut app = App::new(tx.clone());
    tx.send(AppEvent::Open(args.path.clone()))?;

    // dispatch event and draw the app
    while let Ok(event) = rx.recv() {
        match event {
            AppEvent::Key(event) if event.code == KeyCode::Esc => break,
            AppEvent::Exit => break,
            AppEvent::Updated => {},
            // all other events to go the app.event
            _ => app.event(&event)?,
        }
        terminal.draw(|frame| frame.render_widget(&app, frame.area()))?;
    }

    Ok(())
}

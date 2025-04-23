use clap::Parser;
use color_eyre::Result;
use crossterm::{self, event::KeyCode};
use datui::{App, AppEvent};
use ratatui::DefaultTerminal;
use std::path::PathBuf;
use std::sync::mpsc::channel;

#[derive(Parser, Debug)]
#[command(version, about = "datui")]
struct Args {
    path: PathBuf,
}

fn render(terminal: &mut DefaultTerminal, app: &mut App) -> Result<()> {
    terminal.draw(|frame| frame.render_widget(app, frame.area()))?;
    Ok(())
}

fn run(mut terminal: DefaultTerminal, args: &Args) -> Result<()> {
    let (tx, rx) = channel::<AppEvent>();
    let mut app = App::new(tx.clone());
    render(&mut terminal, &mut app)?;
    tx.send(AppEvent::Open(args.path.clone()))?;

    loop {
        if crossterm::event::poll(std::time::Duration::from_millis(0))? {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key) => tx.send(AppEvent::Key(key))?,
                crossterm::event::Event::Resize(_, _) => tx.send(AppEvent::Updated)?,
                _ => {}
            }
        }

        match rx.recv_timeout(std::time::Duration::from_millis(0)) {
            Ok(event) => match event {
                AppEvent::Key(event) if event.code == KeyCode::Esc => break,
                AppEvent::Exit => break,
                AppEvent::Updated => {}
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

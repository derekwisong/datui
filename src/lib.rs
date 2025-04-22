use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::style::Style;
use ratatui::widgets::{Block, Padding, Paragraph, StatefulWidget};
use widgets::datatable::{DataTable, DataTableState};

mod widgets;
pub enum AppEvent {
    Key(KeyEvent),
    Open(PathBuf),
    Updated,
    Exit,
}

pub struct App {
    data_table_state: Option<DataTableState>,
    path: Option<PathBuf>,
    events: Sender<AppEvent>,
    focus: u32,
    num_events: u64,
}

impl App {
    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    pub fn new(events: Sender<AppEvent>) -> App {
        App {
            path: None,
            data_table_state: None,
            events,
            focus: 0,
            num_events: 0,
        }
    }

    fn load(&mut self, path: &Path) -> Result<()> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        self.data_table_state = Some(DataTableState::new(lf));
        self.path = Some(path.to_path_buf());
        Ok(())
    }

    fn key(&mut self, event: &KeyEvent) -> Result<()> {
        match event.code {
            KeyCode::Char('q') => {
                self.send_event(AppEvent::Exit)?;
            }
            KeyCode::Down => {
                if let Some(ref mut state) = self.data_table_state {
                    state.table_state.select_next();
                }
            }
            KeyCode::Up => {
                if let Some(ref mut state) = self.data_table_state {
                    state.table_state.select_previous();
                }
            }
            KeyCode::Tab => {
                self.focus = (self.focus + 1) % 2;
            }
            _ => {}
        };
        Ok(())
    }

    pub fn event(&mut self, event: &AppEvent) -> Result<()> {
        match event {
            AppEvent::Key(key) => self.key(key)?,
            AppEvent::Open(path) => self.load(path)?,
            _ => {}
        }
        self.send_event(AppEvent::Updated)?;
        self.num_events += 1;
        Ok(())
    }
}

struct Controls {
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Paragraph::new("Scroll up and down with arrow keys. Press 'q' or <Esc> to quit")
            .wrap(ratatui::widgets::Wrap { trim: true })
            .centered()
            .render(area, buf);
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Fill(1), Constraint::Max(5)])
            .split(area);

        match &mut self.data_table_state {
            Some(state) => DataTable::new().render(layout[0], buf, state),
            None => Paragraph::new("No data loaded").render(layout[0], buf),
        }

        let mut block = Block::default()
            .title(format!("Number of events: {}", self.num_events))
            .padding(Padding::top(1))
            .borders(ratatui::widgets::Borders::ALL);

        if self.focus == 0 {
            block = block.style(Style::default().fg(ratatui::style::Color::Yellow));
        }

        Controls {}.render(block.inner(layout[1]), buf);
        block.render(layout[1], buf);

        //let controls = Controls {};
        //controls.render(layout[1], buf);
    }
}

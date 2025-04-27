use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{Paragraph, StatefulWidget};

mod widgets;

use widgets::debug::DebugState;
use widgets::controls::Controls;
use widgets::datatable::{DataTable, DataTableState};

pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_lines: None,
            skip_rows: None,
        }
    }

    pub fn with_skip_lines(mut self, skip_lines: usize) -> Self {
        self.skip_lines = Some(skip_lines);
        self
    }

    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.skip_rows = Some(skip_rows);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }
}

pub enum AppEvent {
    Key(KeyEvent),
    Open(PathBuf, OpenOptions),
    Exit,
    Crash(String),
    Collect,
}


pub struct App {
    data_table_state: Option<DataTableState>,
    path: Option<PathBuf>,
    events: Sender<AppEvent>,
    focus: u32,
    debug: DebugState,
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
            debug: DebugState::default(),
        }
    }

    fn load(&mut self, path: &Path, options: &OpenOptions) -> Result<()> {
        let lf = match path.extension() {
            Some(ext) if ext.eq_ignore_ascii_case("parquet") => DataTableState::from_parquet(path)?,
            Some(ext) if ext.eq_ignore_ascii_case("csv") => {
                DataTableState::from_csv(path, options)?
            }
            Some(ext) if ext.eq_ignore_ascii_case("tsv") => {
                DataTableState::from_delimited(path, b'\t')?
            }
            Some(ext) if ext.eq_ignore_ascii_case("psv") => {
                DataTableState::from_delimited(path, b'|')?
            }
            Some(ext) if ext.eq_ignore_ascii_case("json") => DataTableState::from_json(path)?,
            Some(ext) if ext.eq_ignore_ascii_case("jsonl") => {
                DataTableState::from_json_lines(path)?
            }
            Some(ext) if ext.eq_ignore_ascii_case("ndjson") => DataTableState::from_ndjson(path)?,
            _ => return Err(color_eyre::eyre::eyre!("Unsupported file type")),
        };
        self.data_table_state = Some(lf);
        self.path = Some(path.to_path_buf());
        Ok(())
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        match event.code {
            KeyCode::Char('q') => Some(AppEvent::Exit),
            KeyCode::Right if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_right();
                }
                None
            }
            KeyCode::Left if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_left();
                }
                None
            }
            KeyCode::Down if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.select_next();
                }
                None
            }
            KeyCode::PageDown if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.page_down();
                }
                None
            }
            KeyCode::Up if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.select_previous();
                }
                None
            }
            KeyCode::PageUp if event.is_press() => {
                if let Some(ref mut state) = self.data_table_state {
                    state.page_up();
                }
                None
            }
            KeyCode::Tab if event.is_press() => {
                self.focus = (self.focus + 1) % 2;
                None
            }
            _ => None,
        }
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;
        match event {
            AppEvent::Key(key) => self.key(key),
            AppEvent::Open(path, options) => match self.load(path, options) {
                Ok(_) => Some(AppEvent::Collect),
                Err(e) => Some(AppEvent::Crash(e.to_string())),
            },
            AppEvent::Collect => {
                if let Some(ref mut state) = self.data_table_state {
                    state.collect();
                }
                None
            }
            _ => None,
        }
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.debug.num_frames += 1;
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![
                Constraint::Fill(1),
                Constraint::Length(1),
                Constraint::Length(1)
            ])
            .split(area);

        match &mut self.data_table_state {
            Some(state) => DataTable::new().render(layout[0], buf, state),
            None => Paragraph::new("No data loaded").render(layout[0], buf),
        }

        Controls::new().render(layout[1], buf);
        self.debug.render(layout[2], buf);
    }
}

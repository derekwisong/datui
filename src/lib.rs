use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use color_eyre::Result;
use polars::prelude::*;
use crossterm::event::{KeyCode, KeyEvent};

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
};

use ratatui::widgets::{Cell, Padding, Paragraph, Row, Table as RatatuiTable, Wrap};
use ratatui::style::{Style, Modifier};
use ratatui::text::Span;

pub enum AppEvent {
    Key(KeyEvent),
    Open(PathBuf),
    Updated,
    Exit,
}

pub struct Table {
    data: LazyFrame,
    title: String,
    index: usize,
}

struct SchemaView<'a> {
    lf: &'a LazyFrame,
}

impl<'a> Widget for &'a SchemaView<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = ratatui::widgets::Block::default()
            .title("Schema")
            .borders(ratatui::widgets::Borders::ALL);

        let rows: Vec<Row> = if let Ok(schema) = self.lf.clone().collect_schema() {
            schema
                .iter()
                .map(|(name, dtype)| {
                    let cells: Vec<Cell> = vec![
                        Cell::from(Span::raw(name.to_string())),
                        Cell::from(Span::raw(dtype.to_string())),
                    ];
                    Row::new(cells)
                })
                .collect()
        }
        else {
            vec![]
        };
        // iterate the schema and create two colum table,
        // one column for the name and one for the type

        // calculate widths as fractions of total width, equal per column
        let widths = vec![area.width / 2; 2];
        // Create and render the table
        let table = RatatuiTable::new(rows, widths)
            .header(Row::new(vec![
                Cell::from(Span::styled("Column", Style::default().add_modifier(Modifier::BOLD))),
                Cell::from(Span::styled("Type", Style::default().add_modifier(Modifier::BOLD))),
            ]))
            .block(block);

        table.render(area, buf);

    }
}

impl Table {
    pub fn from_parquet(path: &Path) -> Result<Table> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        let title = path.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let tbl = Table { 
            data: lf, 
            title: title,
            index: 0
        };
        Ok(tbl)
    }
}

pub struct App {
    data: Option<Table>,
    path: Option<PathBuf>,
    events: Sender<AppEvent>
}

impl App {
    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    pub fn new(events: Sender<AppEvent>) -> App {
        App {
            data: None,
            path: None,
            events
        }
    }

    fn load(&mut self, path: &Path) -> Result<()> {
        self.data = Some(Table::from_parquet(path)?);
        self.path = Some(path.to_path_buf());
        Ok(())
    }

    fn key(&mut self, event: &KeyEvent) -> Result<()> {
        match event.code {
            KeyCode::Char('q') => { 
                self.send_event(AppEvent::Exit)?;
            },
            KeyCode::Down => {
                if let Some(ref mut data) = self.data {
                    data.index += 1;
                }
            },
            KeyCode::Up => {
                if let Some(ref mut data) = self.data {
                    if data.index > 0 {
                        data.index -= 1;
                    }
                }
            },
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
        self.events.send(AppEvent::Updated)?;
        Ok(())
    }
}

struct Controls {

}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = ratatui::widgets::Block::default()
            .title("Controls")
            .padding(Padding::top(1))
            .borders(ratatui::widgets::Borders::ALL);

        Paragraph::new("Scroll up and down with arrow keys. Press 'q' or <Esc> to quit")
            .block(block)
            .wrap(ratatui::widgets::Wrap { trim: true })
            .centered()
            .render(area, buf);
    }
}

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![
                Constraint::Fill(1),
                Constraint::Max(5),
            ])
            .split(area);
        

        match &self.data {
            Some(t) => t.render(layout[0], buf),
            None => Paragraph::new("No data loaded")
                .centered()
                .block(
                    ratatui::widgets::Block::default()
                        .padding(Padding::top(area.height / 2))
                        .borders(ratatui::widgets::Borders::ALL)
                )
                .render(layout[0], buf),
        };

        let controls = Controls {};
        controls.render(layout[1], buf);
    }
}

impl Widget for &Table {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![
                Constraint::Fill(1),
                Constraint::Max(25),
            ])
            .split(area);


        let lf = self.data.clone();
        // filter the lf to start at row number self.index
        let lf = lf.slice(self.index as i64, self.index as u32 + area.height as u32 - 2);

        match lf.collect() {
            Ok(df) => {
                let (height, cols) = df.shape();

                // Extract column headers
                let headers: Vec<Span> = df.get_column_names()
                    .iter()
                    .map(|name| Span::styled(name.to_string(), Style::default().add_modifier(Modifier::BOLD)))
                    .collect();

                // Iterate each column of the DataFrame and build a vector of rows
                // for ratatui.  each ratatui row is a vector of cells.
                // to do the iteration you have to iterate the columns in colum orer,
                // but create the rows in row order.
                let rows: Vec<Row> = (0..height)
                    .map(|row_index| {
                            let row = df.get(row_index).unwrap();
                            let cells: Vec<Cell> = (0..cols)
                                .map(|col_index| {
                                    let value = row.get(col_index).unwrap();
                                    Cell::from(Span::raw(value.to_string()))
                                })
                                .collect();
                            Row::new(cells)
                        })
                        .collect();
                // calculate widths as fractions of total width, equal per column
                let widths = vec![area.width / headers.len() as u16; headers.len()];

                // Create and render the table
                let table = RatatuiTable::new(rows, widths)
                    .header(Row::new(headers))
                    .block(
                        ratatui::widgets::Block::default()
                            .title(self.title.as_str())
                            .borders(ratatui::widgets::Borders::ALL));

                table.render(layout[0], buf);

                SchemaView { lf: &self.data }.render(layout[1], buf);
            },
            Err(e) => {
                Paragraph::new(e.to_string())
                .block(
                    ratatui::widgets::Block::default()
                        .padding(Padding::top(area.height / 2))
                        .borders(ratatui::widgets::Borders::ALL)
                        .title("Error")
                )
                .centered()
                .wrap(Wrap { trim: false })
                .render(area, buf);
            }
        }
    }
}


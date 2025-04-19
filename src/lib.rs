use std::path::{Path, PathBuf};
use color_eyre::Result;
use polars::prelude::*;
use crossterm::event::{KeyCode, KeyEvent};

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
    text::Line,
};

use ratatui::widgets::{Table as RatatuiTable, Row, Cell};
use ratatui::style::{Style, Modifier};
use ratatui::text::Span;

pub enum AppEvent {
    Key(KeyEvent),
    Open(PathBuf)
}

pub struct Table {
    data: LazyFrame,
    title: String
}

impl Table {
    pub fn from_parquet(path: &Path) -> Result<Table> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        let title = path.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let tbl = Table { data: lf, title: title };
        Ok(tbl)
    }
}

pub struct App {
    running: bool,
    data: Option<Table>,
    path: Option<PathBuf>
}

impl App {
    fn load(&mut self, path: &Path) -> Result<()> {
        self.data = Some(Table::from_parquet(path)?);
        self.path = Some(path.to_path_buf());
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    fn key(&mut self, event: &KeyEvent) -> Result<()> {
        match event.code {
            KeyCode::Char('q') => { 
                self.running = false;
            },
            _ => {}
        };
        Ok(())
    }

    pub fn event(&mut self, event: &AppEvent) -> Result<()> {
        match event {
            AppEvent::Key(key) => self.key(key)?,
            AppEvent::Open(path) => self.load(path)?
        }
        Ok(())
    }

}

impl Default for App {
    fn default() -> Self {
        App {
            running: true,
            data: None,
            path: None
        }
    }
}

impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match &self.data {
            Some(t) => t.render(area, buf),
            None => Line::raw("No data").render(area, buf)
        }
    }
}

impl Widget for &Table {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let lf = self.data.clone().limit(area.height as u32 - 2);

        if let Ok(df) = lf.collect() {
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

            table.render(area, buf);
        } else {
            Line::raw("Failed to load data").render(area, buf);
        }
    }
}


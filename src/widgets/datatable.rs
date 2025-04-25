use color_eyre::Result;
use std::{fs::File, path::Path};

use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style, Stylize},
    text::Span,
    widgets::{
        Block, Borders, Cell, Padding, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::OpenOptions;

pub struct DataTableState {
    lf: LazyFrame,
    df: Option<DataFrame>,
    pub table_state: TableState,
    pub start_row: usize,
    pub visible_rows: usize,
    error: Option<PolarsError>,
}

impl DataTableState {
    pub fn new(lf: LazyFrame) -> Self {
        Self {
            lf,
            df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
            error: None,
        }
    }
    pub fn from_parquet(path: &Path) -> Result<Self> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        Ok(Self::new(lf))
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_csv_customize(path, |mut reader| {
            if let Some(skip_lines) = options.skip_lines {
                reader = reader.with_skip_lines(skip_lines);
            }
            if let Some(skip_rows) = options.skip_rows {
                reader = reader.with_skip_rows(skip_rows);
            }
            if let Some(has_header) = options.has_header {
                reader = reader.with_has_header(has_header);
            }
            reader
        })
    }

    // takes a function that consumes a LazyCsvReader and returns a LazyCsvReader
    // this allows for customization of the csv reader
    pub fn from_csv_customize<F>(path: &Path, func: F) -> Result<Self>
    where
        F: FnOnce(LazyCsvReader) -> LazyCsvReader,
    {
        let reader = LazyCsvReader::new(path);
        let lf = func(reader).finish()?;
        Ok(Self::new(lf))
    }

    pub fn from_ndjson(path: &Path) -> Result<Self> {
        let lf = LazyJsonLineReader::new(path).finish()?;
        Ok(Self::new(lf))
    }

    pub fn from_json(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::Json)
    }

    pub fn from_json_lines(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::JsonLines)
    }

    fn from_json_with_format(path: &Path, format: JsonFormat) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        Ok(Self::new(lf))
    }

    pub fn from_delimited(path: &Path, delimiter: u8) -> Result<Self> {
        let reader = LazyCsvReader::new(path).with_separator(delimiter);
        let lf = reader.finish()?;
        Ok(Self::new(lf))
    }

    fn slide_table(&mut self, rows: i64) {
        self.start_row = if self.start_row as i64 + rows < 0 {
            0
        } else {
            (self.start_row as i64 + rows) as usize
        };

        self.collect();
    }

    pub fn collect(&mut self) {
        match self
            .lf
            .clone()
            .slice(self.start_row as i64, self.visible_rows as u32)
            .collect()
        {
            Ok(df) => {
                self.df = Some(df);
                self.error = None;
            }
            Err(e) => self.error = Some(e),
        }
    }

    pub fn select_next(&mut self) {
        self.table_state.select_next();
        if let Some(selected) = self.table_state.selected() {
            if selected >= self.visible_rows && self.visible_rows > 0 {
                self.slide_table(1);
            }
        }
    }

    pub fn page_down(&mut self) {
        self.slide_table(self.visible_rows as i64);
    }

    pub fn select_previous(&mut self) {
        if let Some(selected) = self.table_state.selected() {
            self.table_state.select_previous();
            if selected == 0 && self.start_row > 0 {
                self.slide_table(-1);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn page_up(&mut self) {
        self.slide_table(-(self.visible_rows as i64));
    }
}
pub struct DataTable {}

impl DataTable {
    pub fn new() -> Self {
        Self {}
    }

    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
    ) {
        let headers: Vec<Span> = df
            .get_column_names()
            .iter()
            .map(|name| {
                Span::styled(
                    name.to_string(),
                    Style::default().add_modifier(Modifier::BOLD),
                )
            })
            .collect();

        let (height, cols) = df.shape();
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

        let widths = vec![area.width / headers.len() as u16; headers.len()];

        StatefulWidget::render(
            Table::new(rows, widths)
                .header(Row::new(headers))
                .row_highlight_style(Style::new().reversed()),
            area,
            buf,
            state,
        );
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.visible_rows = if area.height > 0 {
            (area.height - 1) as usize
        } else {
            0
        };

        if let Some(selected) = state.table_state.selected() {
            if selected >= state.visible_rows as usize {
                state
                    .table_state
                    .select(Some(state.visible_rows as usize - 1))
            }
        }

        if let Some(df) = state.df.as_ref() {
            self.render_dataframe(df, area, buf, &mut state.table_state);
        } else if let Some(error) = state.error.as_ref() {
            Paragraph::new(format!("Error: {}", error))
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::NONE)
                        .padding(Padding::top(area.height / 2)),
                )
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(area, buf);
        } else {
            Paragraph::new("No data").render(area, buf);
        }
    }
}

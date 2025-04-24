use color_eyre::Result;
use std::path::Path;

use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style, Stylize},
    text::Span,
    widgets::{Cell, Paragraph, Row, StatefulWidget, Table, TableState, Widget},
};

pub struct DataTableState {
    lf: LazyFrame,
    df: Option<DataFrame>,
    pub table_state: TableState,
    pub start_row: usize,
    pub visible_rows: usize,
}

impl DataTableState {
    pub fn from_parquet(path: &Path) -> Result<Self> {
        let lf = LazyFrame::scan_parquet(path, Default::default())?;
        Ok(Self {
            lf,
            df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
        })
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
            }
            Err(_) => {
                self.df = None;
            }
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
        // determine the viewport of the table. when the selected row is
        // scrolled out of view, the table will scroll to keep it in view
        // this is done by scrolling down one row.
        state.visible_rows = area.height as usize - 1; // minus header
                                                       // selected is relative to the viewport, not the start row which is
                                                       // the first row in the underlying data table
        if let Some(selected) = state.table_state.selected() {
            // if the the selected row is out of view, set it to the last row
            if selected >= state.visible_rows as usize {
                state
                    .table_state
                    .select(Some(state.visible_rows as usize - 1))
            }
        }

        if let Some(df) = state.df.as_ref() {
            self.render_dataframe(df, area, buf, &mut state.table_state);
        } else {
            Paragraph::new("No data").render(area, buf);
        }
    }
}

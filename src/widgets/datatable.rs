use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style, Stylize},
    text::Span,
    widgets::{Cell, Row, StatefulWidget, Table, TableState},
};

pub struct DataTableState {
    lf: LazyFrame,
    pub table_state: TableState,
}

impl DataTableState {
    pub fn new(lf: LazyFrame) -> Self {
        Self {
            lf,
            table_state: TableState::default()
        }
    }
}
pub struct DataTable {}

impl DataTable {
    pub fn new() -> Self {
        Self {}
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let df = state.lf.clone().slice(0, area.height as u32).collect().unwrap();
        let (height, cols) = df.shape();

        // Extract column headers
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
        StatefulWidget::render(
            Table::new(rows, widths)
                .header(Row::new(headers))
                .row_highlight_style(Style::new().reversed()),
            area,
            buf,
            &mut state.table_state,
        )
    }
}

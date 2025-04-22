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
    pub table_state: TableState,
}

impl DataTableState {
    pub fn new(lf: LazyFrame) -> Self {
        Self {
            lf,
            table_state: TableState::default(),
        }
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
        let lf = state.lf.clone().slice(0, area.height as u32);
        match lf.collect() {
            Ok(df) => self.render_dataframe(&df, area, buf, &mut state.table_state),
            Err(e) => Paragraph::new(e.to_string()).render(area, buf),
        }
    }
}

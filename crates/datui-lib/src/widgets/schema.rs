use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Modifier, Style},
    text::Span,
    widgets::{Block, BorderType, Borders, Cell, Row, Table, Widget},
};

struct SchemaView<'a> {
    lf: &'a LazyFrame,
}

impl<'a> Widget for &'a SchemaView<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .title("Schema")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded);

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
        } else {
            vec![]
        };
        // iterate the schema and create two colum table,
        // one column for the name and one for the type

        // calculate widths as fractions of total width, equal per column
        let widths = vec![area.width / 2; 2];
        // Create and render the table
        let table = Table::new(rows, widths)
            .header(Row::new(vec![
                Cell::from(Span::styled(
                    "Column",
                    Style::default().add_modifier(Modifier::BOLD),
                )),
                Cell::from(Span::styled(
                    "Type",
                    Style::default().add_modifier(Modifier::BOLD),
                )),
            ]))
            .block(block);

        Widget::render(table, area, buf);
    }
}

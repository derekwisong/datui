use ratatui::layout::Constraint;
use ratatui::widgets::{Block, Borders, Padding, Row, Table, Widget};
use ratatui::{buffer::Buffer, layout::Rect};

use super::datatable::DataTableState;

/// A widget that displays information about the currently loaded data frame

pub(crate) struct DataTableInfo<'a> {
    state: &'a DataTableState,
}

impl<'a> DataTableInfo<'a> {
    pub fn new(state: &'a DataTableState) -> Self {
        Self { state }
    }
}

impl<'a> Widget for &'a DataTableInfo<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        // create a table of the schema name -> type
        let schema_table = Table::new(
            self.state
                .schema
                .iter()
                .map(|(name, dtype)| Row::new(vec![name.to_string(), dtype.to_string()])),
            [Constraint::Percentage(50), Constraint::Percentage(50)],
        )
        .header(Row::new(vec!["Column", "Type"]))
        .block(Block::default().title("Schema").borders(Borders::ALL).padding(Padding::new(1, 1, 1, 1)));
        schema_table.render(area, buf);
    }
}

use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, LineGauge, Padding, Paragraph, Row, Table, Widget};
use ratatui::{buffer::Buffer, layout::Rect};
// stylize
use ratatui::style::{palette::tailwind, Color, Style, Stylize};

use super::datatable::DataTableState;

/// A widget that displays information about the currently loaded data frame
pub(crate) struct DataTableInfo<'a> {
    state: &'a DataTableState,
    border_color: Color,
}

impl<'a> DataTableInfo<'a> {
    pub fn new(state: &'a DataTableState) -> Self {
        Self {
            state,
            border_color: Color::White, // Default
        }
    }

    pub fn with_border_color(mut self, color: Color) -> Self {
        self.border_color = color;
        self
    }
}

impl<'a> Widget for &'a DataTableInfo<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let block = Block::default()
            .title(Line::from("Info").bold())
            .title_alignment(Alignment::Center)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(self.border_color))
            .padding(Padding::new(1, 1, 0, 0));

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Fill(1),
            ])
            .split(block.inner(area));

        // create a table of the schema name -> type
        let schema_table = Table::new(
            self.state
                .schema
                .iter()
                .map(|(name, dtype)| Row::new(vec![name.to_string(), dtype.to_string()])),
            [Constraint::Percentage(50), Constraint::Percentage(50)],
        )
        .header(Row::new(vec!["Column", "Type"]).bold())
        .block(
            Block::default()
                .title(Line::from("Schema").underlined())
                .padding(Padding::new(1, 1, 1, 1)),
        );

        Paragraph::new(format!(
            "Rows x Columns: {} x {}",
            self.state.num_rows,
            self.state.schema.len()
        ))
        .render(layout[0], buf);

        let selected: Option<usize> = self
            .state
            .table_state
            .selected()
            .map(|s| s + self.state.start_row);

        if let Some(selected) = selected {
            LineGauge::default()
                .filled_style(Style::from(tailwind::SKY.c500))
                .label(format!("Selected: {}", selected))
                .ratio((selected + 1) as f64 / self.state.num_rows as f64)
                .render(layout[1], buf);
        } else {
            Paragraph::new("No row selected").render(layout[1], buf);
        }
        schema_table.render(layout[layout.len() - 1], buf);
        block.render(area, buf);
    }
}

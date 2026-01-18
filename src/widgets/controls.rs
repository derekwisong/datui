use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Paragraph, Widget},
};

#[derive(Default)]
pub struct Controls {
    pub row_count: Option<usize>,
    pub dimmed: bool,
    pub query_active: bool,
}

impl Controls {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_row_count(row_count: usize) -> Self {
        Self {
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
        }
    }

    pub fn with_dimmed(mut self, dimmed: bool) -> Self {
        self.dimmed = dimmed;
        self
    }

    pub fn with_query_active(mut self, query_active: bool) -> Self {
        self.query_active = query_active;
        self
    }
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        const CONTROLS: [(&str, &str); 8] = [
            ("/", "Query"),
            ("f", "Filter"),
            ("s", "Sort"),
            ("a", "Analysis"),
            ("r", "Reverse"),
            ("R", "Reset"),
            ("i", "Info"),
            ("q", "Quit"),
        ];

        let mut constraints = CONTROLS.iter().fold(vec![], |mut acc, (key, action)| {
            acc.push(Constraint::Length(key.chars().count() as u16 + 2));
            acc.push(Constraint::Length(action.chars().count() as u16 + 1));
            acc
        });

        // Add space for row count if available
        if self.row_count.is_some() {
            constraints.push(Constraint::Length(15)); // Space for "Rows: 12345"
        }
        constraints.push(Constraint::Fill(1)); // Fill the remaining space

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);
        let color = Color::DarkGray;

        // Use dimmed style if dimmed is true
        let base_style = if self.dimmed {
            Style::default().fg(Color::DarkGray)
        } else {
            Style::default()
        };

        // iterate over the controls and render them
        for (i, (key, action)) in CONTROLS.iter().enumerate() {
            let j = i * 2;
            Paragraph::new(*key)
                .style(base_style.bold())
                .centered()
                .render(layout[j], buf);
            // Make "Query" label cyan when query is active
            let action_style = if *action == "Query" && self.query_active {
                base_style.bg(color).fg(Color::Cyan)
            } else {
                base_style.bg(color)
            };
            Paragraph::new(*action)
                .style(action_style)
                .render(layout[j + 1], buf);
        }

        // Render row count if available
        let mut fill_start_idx = CONTROLS.len() * 2;
        if let Some(count) = self.row_count {
            let row_count_text = format!("Rows: {}", count);
            Paragraph::new(row_count_text)
                .style(base_style.bg(color).fg(if self.dimmed {
                    Color::DarkGray
                } else {
                    Color::White
                }))
                .right_aligned()
                .render(layout[fill_start_idx], buf);
            fill_start_idx += 1;
        }

        Paragraph::new("")
            .style(base_style.bg(color))
            .render(layout[fill_start_idx], buf);
    }
}

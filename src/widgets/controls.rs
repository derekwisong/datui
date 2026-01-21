use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Paragraph, Widget},
};

#[derive(Default)]
pub struct Controls {
    pub row_count: Option<usize>,
    pub dimmed: bool,
    pub query_active: bool,
    pub custom_controls: Option<Vec<(&'static str, &'static str)>>,
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
            custom_controls: None,
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

    pub fn with_custom_controls(mut self, controls: Vec<(&'static str, &'static str)>) -> Self {
        self.custom_controls = Some(controls);
        self
    }
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        const DEFAULT_CONTROLS: [(&str, &str); 8] = [
            ("/", "Query"),
            ("f", "Filter"),
            ("s", "Sort"),
            ("a", "Analysis"),
            ("r", "Reverse"),
            ("R", "Reset"),
            ("i", "Info"),
            ("q", "Quit"),
        ];

        let controls: Vec<(&str, &str)> = if let Some(ref custom) = self.custom_controls {
            custom.to_vec()
        } else {
            DEFAULT_CONTROLS.to_vec()
        };

        let mut constraints = controls.iter().fold(vec![], |mut acc, (key, action)| {
            acc.push(Constraint::Length(key.chars().count() as u16 + 2));
            acc.push(Constraint::Length(action.chars().count() as u16 + 1));
            acc
        });

        constraints.push(Constraint::Fill(1));
        if self.row_count.is_some() {
            constraints.push(Constraint::Length(20));
        }

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);

        let base_style = Style::default().bg(Color::Indexed(236)).fg(Color::White);
        for (i, (key, action)) in controls.iter().enumerate() {
            let j = i * 2;
            Paragraph::new(*key)
                .style(base_style.add_modifier(Modifier::BOLD))
                .centered()
                .render(layout[j], buf);
            Paragraph::new(*action)
                .style(base_style)
                .render(layout[j + 1], buf);
        }

        let fill_idx = controls.len() * 2;
        if let Some(count) = self.row_count {
            let row_count_text = format!("Rows: {}", format_number_with_commas(count));
            Paragraph::new(row_count_text)
                .style(base_style)
                .right_aligned()
                .render(layout[fill_idx + 1], buf);
        }

        Paragraph::new("")
            .style(base_style)
            .render(layout[fill_idx], buf);
    }
}

fn format_number_with_commas(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().rev().collect();

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(*ch);
    }

    result.chars().rev().collect()
}

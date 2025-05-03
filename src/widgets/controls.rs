use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    widgets::{Paragraph, Widget},
};

pub struct Controls {}

impl Controls {
    pub fn new() -> Self {
        Self {}
    }
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        const CONTROLS: [(&str, &str); 5] = [
            ("Arrows", "Scroll"), 
            ("PgUp/PgDown", "Jump"),
            ("Home", "Top"),
            ("i", "Info"),
            ("q", "Quit")
        ];

        let mut constraints = CONTROLS.iter().fold(vec![], |mut acc, (key, action)| {
            acc.push(Constraint::Length(key.chars().count() as u16 + 2));
            acc.push(Constraint::Length(action.chars().count() as u16 + 1));
            acc
        });
        constraints.push(Constraint::Fill(1)); // Fill the remaining space

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);
        let color = Color::DarkGray;

        // iterate over the controls and render them
        for (i, (key, action)) in CONTROLS.iter().enumerate() {
            let j = i * 2;
            Paragraph::new(*key)
                .style(Style::default().bold())
                .centered()
                .render(layout[j], buf);
            Paragraph::new(*action)
                .style(Style::default().bg(color))
                .render(layout[j + 1], buf);
        }

        Paragraph::new("")
            .style(Style::default().bg(color))
            .render(layout[layout.len() - 1], buf);
    }
}

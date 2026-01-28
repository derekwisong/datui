use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    widgets::{Block, Paragraph, Widget},
};

pub struct Controls {
    pub row_count: Option<usize>,
    pub dimmed: bool,
    pub query_active: bool,
    pub custom_controls: Option<Vec<(&'static str, &'static str)>>,
    pub bg_color: Color,
    pub key_color: Color,   // Color for keybind hints (keys in toolbar)
    pub label_color: Color, // Color for action labels
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            row_count: None,
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color: Color::Indexed(236), // Default for backward compatibility
            key_color: Color::Cyan,        // Keys in cyan
            label_color: Color::White,     // Labels in white
        }
    }
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
            bg_color: Color::Indexed(236), // Default
            key_color: Color::Cyan,        // Keys in cyan
            label_color: Color::White,     // Labels in white
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

    pub fn with_colors(mut self, bg_color: Color, key_color: Color, label_color: Color) -> Self {
        self.bg_color = bg_color;
        self.key_color = key_color;
        self.label_color = label_color;
        self
    }

    pub fn with_row_count_and_colors(
        row_count: usize,
        bg_color: Color,
        key_color: Color,
        label_color: Color,
    ) -> Self {
        Self {
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color,
            key_color,
            label_color,
        }
    }
}

impl Widget for &Controls {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let no_bg = self.bg_color == Color::Reset;
        if !no_bg {
            Block::default()
                .style(Style::default().bg(self.bg_color))
                .render(area, buf);
        }

        const DEFAULT_CONTROLS: [(&str, &str); 9] = [
            ("/", "Query"),
            ("i", "Info"),
            ("a", "Analysis"),
            ("s", "Sort & Filter"),
            ("p", "Pivot & Melt"),
            ("e", "Export"),
            ("r", "Reverse"),
            ("R", "Reset"),
            ("q", "Quit"),
        ];

        let controls: Vec<(&str, &str)> = if let Some(ref custom) = self.custom_controls {
            custom.to_vec()
        } else {
            DEFAULT_CONTROLS.to_vec()
        };

        // Width of one key-label pair (fixed; pairs are never shrunk).
        // Key: key.len() + 1 (one trailing space). Label: action.len() + 1 (one trailing = gap before next key).
        let pair_width = |(key, action): &(&str, &str)| -> u16 {
            (key.chars().count() as u16 + 1) + (action.chars().count() as u16 + 1)
        };

        // Reserve space for fill and row count; only show pairs that fit.
        let right_reserved = if self.row_count.is_some() { 21 } else { 1 };
        let mut available = area.width.saturating_sub(right_reserved);

        let mut n_show = 0;
        for pair in controls.iter() {
            let need = pair_width(pair);
            if available >= need {
                available -= need;
                n_show += 1;
            } else {
                break;
            }
        }

        // Key: +1 trailing (avoids cut-off when terminals render bold/colored wider). Left-aligned so no leading gap.
        // Label: +1 trailing (single space between label and next key).
        let mut constraints: Vec<Constraint> = controls
            .iter()
            .take(n_show)
            .flat_map(|(key, action)| {
                [
                    Constraint::Length(key.chars().count() as u16 + 1),
                    Constraint::Length(action.chars().count() as u16 + 1),
                ]
            })
            .collect();

        constraints.push(Constraint::Fill(1));
        if self.row_count.is_some() {
            constraints.push(Constraint::Length(20));
        }

        let layout = Layout::new(Direction::Horizontal, constraints).split(area);

        let (key_style, label_style, fill_style) = if no_bg {
            (
                Style::default().fg(self.key_color),
                Style::default().fg(self.label_color),
                Style::default(),
            )
        } else {
            let base = Style::default().bg(self.bg_color);
            (base.fg(self.key_color), base.fg(self.label_color), base)
        };

        for (i, (key, action)) in controls.iter().take(n_show).enumerate() {
            let j = i * 2;
            Paragraph::new(*key).style(key_style).render(layout[j], buf);
            Paragraph::new(*action)
                .style(label_style)
                .render(layout[j + 1], buf);
        }

        let fill_idx = n_show * 2;
        if let Some(count) = self.row_count {
            let row_count_text = format!("Rows: {}", format_number_with_commas(count));
            Paragraph::new(row_count_text)
                .style(label_style)
                .right_aligned()
                .render(layout[fill_idx + 1], buf);
        }

        Paragraph::new("")
            .style(fill_style)
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

use crate::render::context::RenderContext;
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
    pub throbber_color: Color,
    pub use_unicode_throbber: bool, // When true, use 8-dot braille spinner (4 rows tall); else |/-\
    pub busy: bool,                 // When true, show throbber at far right
    pub throbber_frame: u8,         // Spinner frame (0..3 or 0..7 for unicode)
    pub status_message: Option<String>, // When Some, replaces keybindings with spinner + message
    pub row_count_pending: bool, // When true, the exact count is still being determined: show a spinner in place of the (provisional, possibly inaccurate) number
    pub row_count_unknown: bool, // When true, the count could not be determined: show "?" instead of a misleading provisional number (takes effect only when not pending)
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
            throbber_color: Color::Cyan,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            row_count_pending: false,
            row_count_unknown: false,
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
            throbber_color: Color::Cyan,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            row_count_pending: false,
            row_count_unknown: false,
        }
    }

    pub fn with_busy(mut self, busy: bool, throbber_frame: u8) -> Self {
        self.busy = busy;
        self.throbber_frame = throbber_frame;
        self
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

    pub fn with_colors(
        mut self,
        bg_color: Color,
        key_color: Color,
        label_color: Color,
        throbber_color: Color,
    ) -> Self {
        self.bg_color = bg_color;
        self.key_color = key_color;
        self.label_color = label_color;
        self.throbber_color = throbber_color;
        self
    }

    pub fn with_unicode_throbber(mut self, use_unicode: bool) -> Self {
        self.use_unicode_throbber = use_unicode;
        self
    }

    pub fn with_status_message(mut self, message: Option<String>) -> Self {
        self.status_message = message;
        self
    }

    pub fn with_row_count_pending(mut self, pending: bool) -> Self {
        self.row_count_pending = pending;
        self
    }

    pub fn with_row_count_unknown(mut self, unknown: bool) -> Self {
        self.row_count_unknown = unknown;
        self
    }

    /// Create Controls from RenderContext (Phase 2+).
    /// This is the preferred way to create Controls with proper theming.
    pub fn from_context(row_count: usize, ctx: &RenderContext) -> Self {
        Self {
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color: ctx.controls_bg,
            key_color: ctx.keybind_hints,
            label_color: ctx.keybind_labels,
            throbber_color: ctx.throbber,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            row_count_pending: false,
            row_count_unknown: false,
        }
    }

    pub fn with_row_count_and_colors(
        row_count: usize,
        bg_color: Color,
        key_color: Color,
        label_color: Color,
        throbber_color: Color,
    ) -> Self {
        Self {
            row_count: Some(row_count),
            dimmed: false,
            query_active: false,
            custom_controls: None,
            bg_color,
            key_color,
            label_color,
            throbber_color,
            use_unicode_throbber: false,
            busy: false,
            throbber_frame: 0,
            status_message: None,
            row_count_pending: false,
            row_count_unknown: false,
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

        // Throbber character for status mode (reused below).
        const THROBBER_ASCII: [char; 4] = ['|', '/', '-', '\\'];
        const THROBBER_BRAILLE_EIGHT: [char; 8] = ['⣷', '⣯', '⣟', '⡿', '⢿', '⣻', '⣽', '⣾'];

        let throbber_ch = || -> String {
            if self.busy {
                if self.use_unicode_throbber {
                    THROBBER_BRAILLE_EIGHT[self.throbber_frame as usize % 8].to_string()
                } else {
                    THROBBER_ASCII[self.throbber_frame as usize % 4].to_string()
                }
            } else {
                " ".to_string()
            }
        };

        // Spinner frame independent of `busy` — used for the row-count placeholder while the
        // exact total is still being determined (that background count does not set `busy`).
        let spinner_ch = || -> char {
            if self.use_unicode_throbber {
                THROBBER_BRAILLE_EIGHT[self.throbber_frame as usize % 8]
            } else {
                THROBBER_ASCII[self.throbber_frame as usize % 4]
            }
        };

        // Row-count text: while the count is pending a spinner stands in for the number, and if
        // the count couldn't be determined a "?" is shown — so the user never mistakes an
        // incomplete partial total for the final figure.
        let row_count_text = |count: usize| -> String {
            if self.row_count_pending {
                format!("Rows: {}", spinner_ch())
            } else if self.row_count_unknown {
                "Rows: ?".to_string()
            } else {
                format!("Rows: {}", format_number_with_commas(count))
            }
        };

        let throbber_style = if no_bg {
            Style::default().fg(self.throbber_color)
        } else {
            Style::default().bg(self.bg_color).fg(self.throbber_color)
        };

        let (label_style, fill_style) = if no_bg {
            (Style::default().fg(self.label_color), Style::default())
        } else {
            let base = Style::default().bg(self.bg_color);
            (base.fg(self.label_color), base)
        };

        // Status message mode: [spinner 2ch] [message Fill] [row_count 21ch]
        if let Some(ref msg) = self.status_message {
            let mut constraints = vec![
                Constraint::Length(2), // spinner
                Constraint::Fill(1),   // status message
            ];
            if self.row_count.is_some() {
                constraints.push(Constraint::Length(21));
            }

            let layout = Layout::new(Direction::Horizontal, constraints).split(area);

            // Spinner on the left
            Paragraph::new(format!("{} ", throbber_ch()))
                .style(throbber_style)
                .render(layout[0], buf);

            // Status message
            Paragraph::new(msg.as_str())
                .style(label_style)
                .render(layout[1], buf);

            // Row count (right-aligned, if available)
            if let Some(count) = self.row_count {
                Paragraph::new(row_count_text(count))
                    .style(label_style)
                    .right_aligned()
                    .render(layout[2], buf);
            }

            return;
        }

        // Normal keybinding mode (unchanged from original)
        const DEFAULT_CONTROLS: [(&str, &str); 9] = [
            ("/", "Query"),
            ("i", "Info"),
            ("a", "Analysis"),
            ("c", "Chart"),
            ("s", "Sort & Filter"),
            ("p", "Pivot & Melt"),
            ("e", "Export"),
            ("?", "Help"),
            ("q", "Quit"),
        ];

        let controls: Vec<(&str, &str)> = if let Some(ref custom) = self.custom_controls {
            custom.to_vec()
        } else {
            DEFAULT_CONTROLS.to_vec()
        };

        let pair_width = |(key, action): &(&str, &str)| -> u16 {
            (key.chars().count() as u16 + 1) + (action.chars().count() as u16 + 1)
        };

        // Reserve space for fill and row count (no right-side throbber in normal mode).
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

        let key_style = if no_bg {
            Style::default().fg(self.key_color)
        } else {
            Style::default().bg(self.bg_color).fg(self.key_color)
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
            Paragraph::new(row_count_text(count))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn render_to_string(controls: &Controls, width: u16) -> String {
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        controls.render(area, &mut buf);
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect::<String>()
    }

    #[test]
    fn shows_number_when_count_known() {
        let controls = Controls::with_row_count(1_234_567);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: 1,234,567"), "got: {out:?}");
    }

    #[test]
    fn shows_spinner_not_number_when_count_pending() {
        // ASCII throbber, frame 1 -> '/'
        let controls = Controls::with_row_count(42)
            .with_row_count_pending(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: /"), "expected spinner, got: {out:?}");
        // The provisional number must not leak into the display.
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn shows_question_mark_when_count_failed() {
        let controls = Controls::with_row_count(42).with_row_count_unknown(true);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: ?"), "expected '?', got: {out:?}");
        assert!(!out.contains("42"), "provisional count leaked: {out:?}");
    }

    #[test]
    fn pending_takes_precedence_over_unknown() {
        let controls = Controls::with_row_count(42)
            .with_row_count_pending(true)
            .with_row_count_unknown(true)
            .with_busy(false, 1);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Rows: /"), "expected spinner, got: {out:?}");
        assert!(
            !out.contains('?'),
            "should not show '?' while pending: {out:?}"
        );
    }

    #[test]
    fn pending_spinner_shown_in_status_message_mode() {
        let controls = Controls::with_row_count(99)
            .with_row_count_pending(true)
            .with_status_message(Some("Loading buffer...".to_string()))
            .with_busy(false, 0);
        let out = render_to_string(&controls, 80);
        assert!(out.contains("Loading buffer..."), "got: {out:?}");
        // Frame 0 ASCII -> '|'
        assert!(out.contains("Rows: |"), "expected spinner, got: {out:?}");
        assert!(!out.contains("99"), "provisional count leaked: {out:?}");
    }
}

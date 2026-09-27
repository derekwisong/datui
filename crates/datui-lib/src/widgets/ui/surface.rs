//! The one border. A modal, sidebar or overlay gets one rounded frame and one
//! title; structure inside comes from alignment, section rules and the accent.

use super::HintBar;
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Widget};

/// The frame every surface draws: rounded border, unpadded Title Case title,
/// optionally a one-line chip footer on the last inner row.
pub struct Surface<'a> {
    title: &'a str,
    footer: Option<&'a HintBar<'a>>,
}

impl<'a> Surface<'a> {
    pub fn new(title: &'a str) -> Self {
        Self {
            title,
            footer: None,
        }
    }

    pub fn footer(mut self, footer: &'a HintBar<'a>) -> Self {
        self.footer = Some(footer);
        self
    }

    /// Clear the area, draw the frame and footer, and return the content area:
    /// the inside minus a one-column gutter each side and the footer row.
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) -> Rect {
        Clear.render(area, buf);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(ctx.modal_border))
            .title(self.title)
            .title_style(Style::reset());
        let inner = block.inner(area);
        block.render(area, buf);

        let padded = Rect {
            x: inner.x + 1,
            y: inner.y,
            width: inner.width.saturating_sub(2),
            height: inner.height,
        };
        if let Some(footer) = self.footer {
            if padded.height > 0 {
                let footer_area = Rect {
                    y: padded.y + padded.height - 1,
                    height: 1,
                    ..padded
                };
                footer.render(footer_area, buf);
            }
            return Rect {
                height: padded.height.saturating_sub(1),
                ..padded
            };
        }
        padded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(width: u16, height: u16, with_footer: bool) -> (Vec<String>, Rect) {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        let footer = HintBar::from_ctx(&ctx)
            .hint("Enter", "Apply")
            .hint("Esc", "Cancel");
        let surface = if with_footer {
            Surface::new("Export Data").footer(&footer)
        } else {
            Surface::new("Export Data")
        };
        let content = surface.render(area, &mut buf, &ctx);
        let rows = (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        (rows, content)
    }

    #[test]
    fn one_border_one_title_and_the_footer_inside_the_frame() {
        let (rows, content) = render(40, 8, true);
        assert!(rows[0].contains("Export Data"), "title on the frame");
        // One border: the frame's corners and nothing box-drawn inside.
        for row in &rows[1..7] {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a second border inside the surface: {row:?}"
            );
        }
        assert!(
            rows[6].contains("Enter") && rows[6].contains("Apply"),
            "the footer is the last inner row: {:?}",
            rows[6]
        );
        // Content sits above the footer, inside a one-column gutter.
        assert_eq!(content, Rect::new(2, 1, 36, 5));
    }

    #[test]
    fn without_a_footer_the_content_runs_to_the_bottom() {
        let (_, content) = render(40, 8, false);
        assert_eq!(content, Rect::new(2, 1, 36, 6));
    }

    /// A degenerate area must not underflow or draw outside itself.
    #[test]
    fn a_tiny_area_stays_in_bounds() {
        for (w, h) in [(0, 0), (1, 1), (2, 2), (3, 1)] {
            let (_, content) = render(w, h, true);
            assert!(content.width <= w && content.height <= h);
        }
    }
}

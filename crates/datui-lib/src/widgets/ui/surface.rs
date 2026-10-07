//! The one border. A modal, sidebar or overlay gets one rounded frame and one
//! title; structure inside comes from alignment, section rules and the accent.

use super::HintBar;
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Block, Borders, Clear, Widget};

/// The frame every surface draws: rounded border, unpadded Title Case title,
/// optionally a one-line chip footer on the last inner row.
pub struct Surface<'a> {
    title: &'a str,
    footer: Option<&'a HintBar>,
    /// The frame's own style when the default border slot is wrong for it:
    /// an error surface carries the error border, a confirmation the active one.
    border: Option<Style>,
}

impl<'a> Surface<'a> {
    pub fn new(title: &'a str) -> Self {
        Self {
            title,
            footer: None,
            border: None,
        }
    }

    pub fn footer(mut self, footer: &'a HintBar) -> Self {
        self.footer = Some(footer);
        self
    }

    pub fn border_style(mut self, style: Style) -> Self {
        self.border = Some(style);
        self
    }

    /// The content area [`Self::render`] returns for `area` with a footer, known
    /// before drawing: a layout that decides the footer needs it first.
    pub fn content_area(area: Rect) -> Rect {
        // The frame's two rows, then the footer and the gap above it, as `render`.
        let inside = area.height.saturating_sub(2);
        let gap = u16::from(inside > 2);
        Rect {
            x: area.x + 2,
            y: area.y + 1,
            width: area.width.saturating_sub(4),
            height: inside.saturating_sub(1 + gap),
        }
    }

    /// Clear the area, draw the frame and footer, and return the content area:
    /// the inside minus a one-column gutter each side, the footer row, and the blank
    /// row that keeps the content's last line off the chips (#650).
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) -> Rect {
        Clear.render(area, buf);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_set(crate::glyphs::get().border)
            .border_style(
                self.border
                    .unwrap_or_else(|| Style::default().fg(ctx.modal_border)),
            )
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
                footer.render_flush(footer_area, buf);
            }
            // The gap only where there is content left to keep apart.
            let gap = u16::from(padded.height > 2);
            return Rect {
                height: padded.height.saturating_sub(1 + gap),
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
            .screen(datui_cli::keys::Context::Export)
            .key("Enter")
            .key("Esc");
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
            rows[6].contains("Enter") && rows[6].contains("Export"),
            "the footer is the last inner row: {:?}",
            rows[6]
        );
        // Content sits above the footer, a blank row between, inside a one-column
        // gutter.
        assert_eq!(content, Rect::new(2, 1, 36, 4));
        assert!(rows[5].trim_matches(['│', ' ']).is_empty(), "{:?}", rows[5]);
        // Known before drawing, for a layout that sizes the footer from it.
        assert_eq!(Surface::content_area(Rect::new(0, 0, 40, 8)), content);
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

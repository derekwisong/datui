//! Work in progress said where its result will appear: the spinner and one plain
//! status, styled as the loading screen's phase line.

use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

#[derive(Clone, Copy)]
pub struct Working<'a> {
    pub text: &'a str,
    /// The throbber frame, so this spinner turns with the control bar's.
    pub frame: usize,
}

impl Working<'_> {
    pub fn line(&self, ctx: &RenderContext) -> Line<'static> {
        let g = crate::glyphs::get();
        Line::from(vec![
            Span::styled(
                format!("{}  ", g.spinner[self.frame % g.spinner.len()]),
                Style::default().fg(ctx.throbber),
            ),
            Span::styled(
                self.text.to_string(),
                Style::default()
                    .fg(ctx.text_primary)
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    }

    /// In place of what `area` held, centered a little above the middle, where the
    /// loading screen puts its phase.
    pub fn render_centered(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        Clear.render(area, buf);
        if area.height == 0 {
            return;
        }
        let y = area.y + (area.height / 2).saturating_sub(1);
        Paragraph::new(self.line(ctx)).centered().render(
            Rect {
                y,
                height: 1,
                ..area
            },
            buf,
        );
    }

    /// Over the top right of what `area` already shows, which stays on screen until
    /// the work is done.
    pub fn render_corner(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        let line = self.line(ctx);
        // A cell of air either side, so the label never touches a mark.
        let width = (line.width() as u16 + 2).min(area.width);
        if area.height == 0 || width == 0 {
            return;
        }
        let spot = Rect {
            x: area.right() - width,
            y: area.y,
            width,
            height: 1,
        };
        Clear.render(spot, buf);
        Paragraph::new(line).centered().render(spot, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(buf: &Buffer) -> Vec<String> {
        let w = buf.area.width as usize;
        buf.content()
            .chunks(w)
            .map(|row| row.iter().map(|c| c.symbol()).collect())
            .collect()
    }

    #[test]
    fn centered_replaces_what_was_there() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 30, 5);
        let mut buf = Buffer::empty(area);
        Paragraph::new(vec![Line::from("old"); 5]).render(area, &mut buf);
        Working {
            text: "Running query...",
            frame: 0,
        }
        .render_centered(area, &mut buf, &ctx);
        let rows = text(&buf);
        assert!(rows[1].contains("Running query..."), "{rows:#?}");
        assert!(rows.iter().all(|row| !row.contains("old")), "{rows:#?}");
    }

    #[test]
    fn corner_keeps_what_is_under_it() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 40, 3);
        let mut buf = Buffer::empty(area);
        Paragraph::new(vec![Line::from("chart"); 3]).render(area, &mut buf);
        Working {
            text: "Computing...",
            frame: 0,
        }
        .render_corner(area, &mut buf, &ctx);
        let rows = text(&buf);
        assert!(rows[0].trim_end().ends_with("Computing..."), "{rows:#?}");
        assert!(rows.iter().all(|row| row.starts_with("chart")), "{rows:#?}");
    }
}

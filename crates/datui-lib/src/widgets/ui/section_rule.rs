//! A title on a rule: the way to divide space inside a Surface without a
//! second border.

use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// Never a focus signal: the section's focused row carries the rail and the
/// accent, and the rule reads the same whether focus is in its section or not.
pub struct SectionRule<'a> {
    pub title: &'a str,
    /// Optional flat chip after the title, usually a count.
    pub chip: Option<&'a str>,
}

impl SectionRule<'_> {
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let g = crate::glyphs::get();
        let title_style = Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD);
        let rule_glyph = g.rule_h;
        let rule_style = Style::default().fg(ctx.column_separator);

        let mut spans = vec![Span::styled(self.title, title_style), Span::raw(" ")];
        let mut used = crate::glyphs::display_width(self.title) + 1;
        if let Some(chip) = self.chip {
            let chip = format!(" {chip} ");
            used += crate::glyphs::display_width(&chip) + 1;
            spans.push(Span::styled(
                chip,
                Style::default().bg(ctx.controls_bg).fg(ctx.text_primary),
            ));
            spans.push(Span::raw(" "));
        }
        let rule_w = (area.width as usize).saturating_sub(used);
        spans.push(Span::styled(rule_glyph.repeat(rule_w), rule_style));
        Paragraph::new(Line::from(spans)).render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(rule: &SectionRule, width: u16) -> String {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        rule.render(area, &mut buf, &ctx);
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect()
    }

    #[test]
    fn the_rule_runs_from_the_title_to_the_edge() {
        let g = crate::glyphs::get();
        let out = render(
            &SectionRule {
                title: "Format",
                chip: None,
            },
            20,
        );
        assert_eq!(out, format!("Format {}", g.rule_h.repeat(13)));
    }

    #[test]
    fn a_chip_sits_between_title_and_rule() {
        let out = render(
            &SectionRule {
                title: "Format",
                chip: Some("6"),
            },
            20,
        );
        assert!(out.starts_with("Format  6 "), "got {out:?}");
    }

    /// The rule is plain: no heavy glyph and no bright accent, which once
    /// marked the focused section beside the rail.
    #[test]
    fn the_rule_never_signals_focus() {
        let g = crate::glyphs::get();
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 20, 1);
        let mut buf = Buffer::empty(area);
        SectionRule {
            title: "Sort",
            chip: None,
        }
        .render(area, &mut buf, &ctx);
        assert_eq!(buf[(0, 0)].fg, ctx.accent);
        assert_eq!(buf[(19, 0)].symbol(), g.rule_h);
        assert_eq!(buf[(19, 0)].fg, ctx.column_separator);
    }
}

//! A title on a rule: the way to divide space inside a Surface without a
//! second border.

use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

pub struct SectionRule<'a> {
    pub title: &'a str,
    /// Optional flat chip after the title, usually a count.
    pub chip: Option<&'a str>,
    pub focused: bool,
}

impl SectionRule<'_> {
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let g = crate::glyphs::get();
        let title_style = if self.focused {
            Style::default()
                .fg(ctx.accent_bright)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        };
        let rule_glyph = if self.focused {
            g.rule_h_focused
        } else {
            g.rule_h
        };
        let rule_style = Style::default().fg(if self.focused {
            ctx.accent
        } else {
            ctx.column_separator
        });

        let mut spans = vec![Span::styled(self.title, title_style), Span::raw(" ")];
        let mut used = self.title.chars().count() + 1;
        if let Some(chip) = self.chip {
            let chip = format!(" {chip} ");
            used += chip.chars().count() + 1;
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
                focused: false,
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
                focused: false,
            },
            20,
        );
        assert!(out.starts_with("Format  6 "), "got {out:?}");
    }

    /// Focus changes the rule's weight, never the width.
    #[test]
    fn focus_never_moves_the_text() {
        let make = |focused| SectionRule {
            title: "Format",
            chip: Some("6"),
            focused,
        };
        let plain = render(&make(false), 20);
        let focused = render(&make(true), 20);
        assert_eq!(plain.find("6"), focused.find("6"));
        assert_eq!(plain.chars().count(), focused.chars().count());
    }
}

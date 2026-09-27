//! `label  value` on one line inside a Surface. The focused row shows its
//! label in the accent; the chosen value is always echoed, so nothing is
//! ambiguous when focus is elsewhere.

use crate::render::context::RenderContext;
use crate::widgets::text_input::TextInput;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Paragraph, Widget};

/// What sits after the label.
pub enum FormValue<'a> {
    /// A text field. The caller sets the input's focus before rendering; the
    /// input draws its own cursor.
    Input(&'a TextInput),
    /// A checkbox.
    Toggle(bool),
    /// A pick-one value, cycled or chosen through a Picker; the row echoes the
    /// current choice.
    Choice(&'a str),
}

pub struct FormRow<'a> {
    pub label: &'a str,
    pub value: FormValue<'a>,
    pub focused: bool,
    /// Where the value column starts, shared by every row so values align.
    pub label_width: u16,
}

impl FormRow<'_> {
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let label_style = if self.focused {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.label)
        };
        let label_w = self.label_width.min(area.width);
        Paragraph::new(self.label).style(label_style).render(
            Rect {
                width: label_w,
                ..area
            },
            buf,
        );

        let value_area = Rect {
            x: area.x + label_w,
            width: area.width.saturating_sub(label_w),
            ..area
        };
        if value_area.width == 0 {
            return;
        }
        match &self.value {
            FormValue::Input(input) => (*input).render(value_area, buf),
            FormValue::Toggle(on) => {
                let g = crate::glyphs::get();
                let marker = if *on { g.checkbox_on } else { g.checkbox_off };
                Paragraph::new(marker)
                    .style(Style::default().fg(ctx.text_primary))
                    .render(value_area, buf);
            }
            FormValue::Choice(value) => {
                Paragraph::new(*value)
                    .style(Style::default().fg(ctx.text_primary))
                    .render(value_area, buf);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render_row(row: &FormRow, width: u16) -> (String, Buffer) {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        row.render(area, &mut buf, &ctx);
        let text = (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect();
        (text, buf)
    }

    #[test]
    fn the_value_is_echoed_at_the_shared_column() {
        let row = FormRow {
            label: "Compression:",
            value: FormValue::Choice("Gzip"),
            focused: false,
            label_width: 17,
        };
        let (text, _) = render_row(&row, 40);
        assert_eq!(text.find("Gzip"), Some(17), "got {text:?}");
    }

    #[test]
    fn a_toggle_draws_the_checkbox_glyph() {
        let g = crate::glyphs::get();
        for (on, marker) in [(true, g.checkbox_on), (false, g.checkbox_off)] {
            let row = FormRow {
                label: "Include header:",
                value: FormValue::Toggle(on),
                focused: false,
                label_width: 17,
            };
            let (text, _) = render_row(&row, 40);
            assert!(text.contains(marker), "expected {marker:?} in {text:?}");
        }
    }

    /// Focus is a color, never a layout change.
    #[test]
    fn focus_accents_the_label_and_moves_nothing() {
        let make = |focused| FormRow {
            label: "Path:",
            value: FormValue::Choice("out.csv"),
            focused,
            label_width: 17,
        };
        let (plain_text, plain) = render_row(&make(false), 40);
        let (focused_text, focused) = render_row(&make(true), 40);
        assert_eq!(plain_text, focused_text);
        let changed: Vec<u16> = (0..40)
            .filter(|&x| plain[(x, 0)].fg != focused[(x, 0)].fg)
            .collect();
        assert!(!changed.is_empty(), "focus is invisible");
        assert!(
            changed.iter().all(|&x| x < 17),
            "focus colored the value, not just the label column: {changed:?}"
        );
    }

    #[test]
    fn a_narrow_row_never_panics() {
        for width in 0..20 {
            let row = FormRow {
                label: "Include header:",
                value: FormValue::Toggle(true),
                focused: true,
                label_width: 17,
            };
            let _ = render_row(&row, width);
        }
    }
}

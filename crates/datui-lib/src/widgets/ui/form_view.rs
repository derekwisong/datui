//! A dialog of form rows: one Surface, a FormRow per field, the open Picker below the
//! rows, and a status line above the footer saying what Enter will do.

use crate::app::form::Form;
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, PickerState, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Paragraph, Widget};

/// What a form dialog draws. `footer` is the form's own keys; while a picker is open
/// its keys stand in, from the `Picker` group of `screen`'s registry entries.
pub struct FormView<'a, F> {
    pub title: &'a str,
    pub screen: datui_cli::keys::Context,
    pub footer: HintBar,
    /// Where the values start, past the longest label.
    pub label_width: u16,
    /// Each field shown, its label and its value.
    pub rows: Vec<(F, &'a str, FormValue<'a>)>,
    pub focused: F,
    pub picker: Option<&'a PickerState>,
    /// The last line: what Enter will do, or what stops it.
    pub status: (String, Style),
}

impl<'a, F: Copy + PartialEq> FormView<'a, F> {
    /// Draw it in `area`, recording each row's place for clicks.
    pub fn render<T: Form<Field = F>>(self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        let footer = match self.picker {
            Some(_) => HintBar::from_ctx(ctx)
                .screen(self.screen)
                .group("Picker")
                .key("Enter")
                .weight(3)
                .key("(type)")
                .weight(1)
                .key("Esc")
                .weight(4),
            None => self.footer,
        };
        crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);
        let content = Surface::new(self.title)
            .footer(&footer)
            .render(area, buf, ctx);
        if content.height < 3 || content.width < 10 {
            return;
        }
        // The status line sits on the last content row, directly above the footer.
        let status_y = content.y + content.height - 1;
        let mut y = content.y;
        for (field, label, value) in self.rows {
            if y >= status_y {
                break;
            }
            let row = Rect {
                y,
                height: 1,
                ..content
            };
            FormRow {
                label,
                value,
                focused: field == self.focused,
                label_width: self.label_width,
            }
            .render_picking(row, buf, ctx, self.picker.is_some());
            crate::app::pointer::record_field::<T>(row, field);
            y += 1;
        }
        // The open Picker drops in below the rows and reaches down to the status line.
        if let Some(state) = self.picker {
            // It owns the keys even with no room to draw: the rows take no clicks.
            crate::app::pointer::record(content, crate::app::pointer::Hit::Picker);
            let picker_y = y + 1;
            if picker_y < status_y {
                let picker_area = Rect {
                    x: content.x + 2,
                    y: picker_y,
                    width: content.width.saturating_sub(2),
                    height: status_y - picker_y,
                };
                Picker::from_state(state, true).render(picker_area, buf, ctx);
            }
        }
        let (text, style) = self.status;
        Paragraph::new(text).style(style).render(
            Rect {
                y: status_y,
                height: 1,
                ..content
            },
            buf,
        );
    }
}

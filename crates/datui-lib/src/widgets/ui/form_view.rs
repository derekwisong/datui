//! A dialog of form rows: one Surface, a FormRow per field (with the lines a field
//! brings under it), the open Picker below the rows, and a status line above the
//! footer saying what Enter will do or why it did not.

use crate::app::form::Form;
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, PickerState, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};

/// One line of a form dialog.
pub enum FormLine<'a, F> {
    /// A field: its label and value.
    Field(F, &'a str, FormValue<'a>),
    /// A line under the field above it, in the value column: what it holds, or the
    /// forms its value takes.
    Note(Line<'a>),
}

/// What a form dialog draws. `footer` is the form's own keys; while a picker is open
/// its keys stand in, from the `Picker` group of `screen`'s registry entries.
pub struct FormView<'a, F> {
    pub title: &'a str,
    pub screen: datui_cli::keys::Context,
    pub footer: Option<HintBar>,
    /// Where the values start, past the longest label.
    pub label_width: u16,
    /// The fields shown, in order, with the lines they bring.
    pub rows: Vec<FormLine<'a, F>>,
    /// The field with the rail; `None` while something beside the form has the focus.
    pub focused: Option<F>,
    pub picker: Option<&'a PickerState>,
    /// The last line: what Enter will do, or what stops it. While it is `Some`, a
    /// blank and the line are kept under the rows, empty or not, so nothing moves
    /// when a reason comes. A long one wraps upward into rows the fields leave free.
    pub status: Option<(String, Style)>,
    /// Whether its whole area takes no clicks but its own, as a dialog over the screen.
    /// `false` only where the caller decides what is around it: the Sample form inline
    /// beside the analysis tools, or over a pane the caller shields.
    pub shields: bool,
}

impl<'a, F: Copy + PartialEq> FormView<'a, F> {
    /// Draw it in `area`, recording each field's row for clicks.
    pub fn render<T: Form<Field = F>>(self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        self.draw(area, buf, ctx, Some(crate::app::pointer::record_field::<T>));
    }

    /// Draw it in `area` for a form whose fields take no clicks.
    pub fn render_unrecorded(self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        self.draw(area, buf, ctx, None);
    }

    fn draw(self, area: Rect, buf: &mut Buffer, ctx: &RenderContext, record: Option<fn(Rect, F)>) {
        let footer = match (self.picker, self.footer) {
            (Some(_), _) => Some(
                HintBar::from_ctx(ctx)
                    .screen(self.screen)
                    .group("Picker")
                    .key("Enter")
                    .weight(3)
                    .key("(type)")
                    .weight(1)
                    .key("Esc")
                    .weight(4),
            ),
            (None, footer) => footer,
        };
        if self.shields {
            crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);
        }
        let mut surface = Surface::new(self.title);
        if let Some(footer) = &footer {
            surface = surface.footer(footer);
        }
        let content = surface.render(area, buf, ctx);
        if content.height == 0 || content.width < 4 {
            return;
        }
        let bottom = content.bottom();
        // The status line and the blank above it, while there is a row left for a field.
        let reserved = match self.status {
            Some(_) if content.height > 2 => 2,
            _ => 0,
        };
        let room = (content.height - reserved) as usize;
        // Overlays scroll inside a capped frame: the focused field stays on screen.
        let focused = self.focused;
        let at = self
            .rows
            .iter()
            .position(|line| matches!(line, FormLine::Field(f, ..) if Some(*f) == focused))
            .unwrap_or(0);
        // With the notes under it, as many as fit beside it.
        let mut end = at;
        while end + 1 - at < room && matches!(self.rows.get(end + 1), Some(FormLine::Note(_))) {
            end += 1;
        }
        let first = end.saturating_sub(room.saturating_sub(1));
        let picking = self.picker.is_some();
        let mut y = content.y;
        for line in self.rows.into_iter().skip(first).take(room) {
            let row = Rect {
                y,
                height: 1,
                ..content
            };
            match line {
                FormLine::Field(field, label, value) => {
                    // The row first: values that record their own clicks lie on top.
                    if let Some(record) = record {
                        record(row, field);
                    }
                    FormRow {
                        label,
                        value,
                        focused: Some(field) == focused,
                        label_width: self.label_width,
                    }
                    .render_picking(row, buf, ctx, picking);
                }
                FormLine::Note(text) => {
                    let indent = self.label_width + 1;
                    Paragraph::new(text).render(
                        Rect {
                            x: row.x + indent,
                            width: row.width.saturating_sub(indent),
                            ..row
                        },
                        buf,
                    );
                }
            }
            y += 1;
        }
        let status_y = bottom - 1;
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
        let Some((text, style)) = self.status.filter(|(text, _)| !text.is_empty()) else {
            return;
        };
        if reserved == 0 {
            return;
        }
        // At the rail's column, as the spec line under a form reads, keeping a blank
        // under the last row (one line under an open picker); cut with an ellipsis
        // past the room there is.
        let width = content.width as usize;
        let free = if picking {
            1
        } else {
            bottom.saturating_sub(y + 1).max(1) as usize
        };
        let mut lines = crate::widgets::info::wrap_to(&text, width);
        if lines.len() > free {
            lines.truncate(free);
            if let Some(last) = lines.last_mut() {
                let cut = format!("{last} {}", crate::glyphs::get().ellipsis);
                *last = crate::glyphs::fit(&cut, width);
            }
        }
        let top = bottom - lines.len() as u16;
        for (i, line) in lines.into_iter().enumerate() {
            Paragraph::new(line).style(style).render(
                Rect {
                    y: top + i as u16,
                    height: 1,
                    ..content
                },
                buf,
            );
        }
    }
}

#[cfg(test)]
mod tests;

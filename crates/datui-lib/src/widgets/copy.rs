//! Copy dialog rendering: one Surface, a FormRow per axis, the focused row's
//! Picker below the rows, and the spec line saying what Enter will do.

use crate::copy_modal::{CopyFocus, CopyModal};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Paragraph, Widget};

/// Past the longest label, "Format:", plus air.
const LABEL_WIDTH: u16 = 9;

fn row_label(focus: CopyFocus) -> &'static str {
    match focus {
        CopyFocus::Scope => "Scope:",
        CopyFocus::Column => "Column:",
        CopyFocus::Format => "Format:",
        CopyFocus::Header => "Header:",
    }
}

pub fn render_copy_modal(area: Rect, buf: &mut Buffer, modal: &mut CopyModal, ctx: &RenderContext) {
    let footer = match &modal.picker {
        Some(_) => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Choose", 3)
            .hint_weighted("type", "Narrow", 1)
            .hint_weighted("Esc", "Back", 4),
        None if modal.focus == CopyFocus::Header => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Copy", 3)
            .hint_weighted("Space", "Toggle", 2)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4),
        None => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Copy", 3)
            .hint_weighted("Space", "Edit", 2)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4),
    };
    let content = Surface::new("Copy").footer(&footer).render(area, buf, ctx);
    if content.height < 3 || content.width < 10 {
        return;
    }

    // The spec line sits on the last content row, directly above the footer.
    let spec_y = content.y + content.height - 1;

    let rows = modal.row_order();
    let mut y = content.y;
    for &row in &rows {
        if y >= spec_y {
            break;
        }
        let value = match row {
            CopyFocus::Scope => FormValue::Choice(modal.scope.as_str()),
            CopyFocus::Format => FormValue::Choice(modal.format.as_str()),
            CopyFocus::Header => FormValue::Toggle(modal.header()),
            CopyFocus::Column => match modal.column.as_deref() {
                Some(column) => FormValue::Choice(column),
                None => FormValue::Placeholder("none"),
            },
        };
        FormRow {
            label: row_label(row),
            value,
            focused: modal.focus == row,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                y,
                height: 1,
                ..content
            },
            buf,
            ctx,
        );
        y += 1;
    }

    // The focused row's Picker drops in below the rows and reaches down to
    // the spec line.
    if let Some(state) = &modal.picker {
        let picker_y = y + 1;
        if picker_y < spec_y {
            let picker_area = Rect {
                x: content.x + 2,
                y: picker_y,
                width: content.width.saturating_sub(2),
                height: spec_y - picker_y,
            };
            Picker::from_state(state, true).render(picker_area, buf, ctx);
        }
    }

    // What Enter will do, echoed live; the gap re-accents when Enter hit it.
    let (text, style) = match modal.spec_line() {
        Ok(line) => (line, Style::default().fg(ctx.text_primary)),
        Err(gap) if modal.attention => (gap, Style::default().fg(ctx.warning)),
        Err(gap) => (gap, Style::default().fg(ctx.dimmed)),
    };
    Paragraph::new(text).style(style).render(
        Rect {
            y: spec_y,
            height: 1,
            ..content
        },
        buf,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::copy_modal::{CopyContext, CopyScope};

    fn render_to_text(modal: &mut CopyModal) -> String {
        let area = Rect::new(0, 0, 46, 14);
        let mut buf = Buffer::empty(area);
        let ctx = RenderContext::for_test();
        render_copy_modal(area, &mut buf, modal, &ctx);
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_dialog_echoes_every_choice_and_the_spec() {
        let mut modal = CopyModal::new();
        modal.open(
            vec!["city".into(), "pop".into()],
            None,
            CopyContext {
                row_number: 1235,
                view_rows: 42,
                view_cols: 8,
                total_rows: Some(1_000_000),
            },
        );
        let text = render_to_text(&mut modal);
        assert!(text.contains("Scope:"), "{text}");
        assert!(text.contains("Row"), "{text}");
        assert!(text.contains("Format:"), "{text}");
        assert!(text.contains("Copy row 1,235 as TSV"), "{text}");
    }

    #[test]
    fn the_python_scope_shows_only_the_scope_row() {
        let mut modal = CopyModal::new();
        modal.open(vec!["city".into()], None, CopyContext::default());
        modal.scope = CopyScope::Python;
        let text = render_to_text(&mut modal);
        assert!(text.contains("Python (Polars)"), "{text}");
        assert!(!text.contains("Format:"), "{text}");
        assert!(!text.contains("Header:"), "{text}");
        assert!(
            text.contains("Copy the view as a Python (Polars) script"),
            "{text}"
        );
    }

    #[test]
    fn the_open_picker_lists_and_narrows() {
        let mut modal = CopyModal::new();
        modal.open(
            vec!["city".into(), "population".into()],
            None,
            CopyContext::default(),
        );
        modal.scope = CopyScope::Cell;
        modal.focus = CopyFocus::Column;
        modal.open_picker();
        let text = render_to_text(&mut modal);
        assert!(text.contains("city"), "{text}");
        assert!(text.contains("population"), "{text}");
        modal
            .picker
            .as_mut()
            .unwrap()
            .filter_key('p', crossterm::event::KeyModifiers::NONE);
        let text = render_to_text(&mut modal);
        assert!(text.contains("population"), "{text}");
        assert!(!text.contains("\n city"), "{text}");
    }
}

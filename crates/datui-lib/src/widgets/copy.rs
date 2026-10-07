//! The copy dialog: a FormView of its axes, the spec line saying what Enter will do.

use crate::app::modals::copy_modal::{CopyFocus, CopyModal};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormLine, FormValue, FormView, HintBar};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;

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
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Copy)
        .group("Form")
        .key("Enter")
        .weight(3);
    let footer = match modal.focus {
        CopyFocus::Header => footer.key_as("Space", "Toggle"),
        CopyFocus::Column => footer.key_as("Space", "Pick"),
        CopyFocus::Scope | CopyFocus::Format => footer.key("← / →"),
    };
    let footer = footer.weight(2).key("Tab").weight(1).key("Esc").weight(4);
    let rows = modal
        .row_order()
        .into_iter()
        .map(|row| {
            let value = match row {
                CopyFocus::Scope => FormValue::Choice(modal.scope.as_str()),
                CopyFocus::Format => FormValue::Choice(modal.format.as_str()),
                CopyFocus::Header => FormValue::Toggle(modal.header()),
                CopyFocus::Column => match modal.column.as_deref() {
                    Some(column) => FormValue::Choice(column),
                    None => FormValue::Placeholder("none"),
                },
            };
            FormLine::Field(row, row_label(row), value)
        })
        .collect();
    // What Enter will do, echoed live; the gap re-accents when Enter hit it.
    let status = match modal.spec_line() {
        Ok(line) => (line, Style::default().fg(ctx.text_primary)),
        Err(gap) if modal.attention => (gap, Style::default().fg(ctx.warning)),
        Err(gap) => (gap, Style::default().fg(ctx.dimmed)),
    };
    crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);
    FormView {
        title: "Copy",
        screen: Context::Copy,
        footer: Some(footer),
        label_width: LABEL_WIDTH,
        rows,
        focused: Some(modal.focus),
        picker: modal.picker.as_ref(),
        status: Some(status),
    }
    .render::<CopyModal>(area, buf, ctx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::modals::copy_modal::{CopyContext, CopyScope};

    fn render_to_text(modal: &mut CopyModal) -> String {
        let area = Rect::new(0, 0, 46, 14);
        let mut buf = Buffer::empty(area);
        let ctx = RenderContext::for_test();
        render_copy_modal(area, &mut buf, modal, &ctx);
        crate::tests::buffer_text(&buf)
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

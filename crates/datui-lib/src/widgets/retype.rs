//! The type picker and the combine form: one Surface each over the table.

use crate::app::modals::retype_modal::{CombineField, CombineModal, RetypeModal, Stage};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormLine, FormValue, FormView, HintBar, Picker, Surface};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// The type picker over `area`: the types, or a type's formats with what each makes
/// of the column's first value, and a format typed with what it makes of it.
pub fn render_retype(area: Rect, buf: &mut Buffer, modal: &RetypeModal, ctx: &RenderContext) {
    let picker = &modal.picker;
    let widest = picker
        .items()
        .iter()
        .map(|item| crate::glyphs::display_width(item))
        .max()
        .unwrap_or(0) as u16;
    let width = (widest + 6).clamp(40, 60).min(area.width);
    // Frame, filter line, the list, the blank above the footer and the footer.
    let height = (picker.items().len() as u16 + 5).min(23).min(area.height);
    if width < 10 || height < 6 {
        return;
    }
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 3,
        width,
        height,
    };
    crate::app::pointer::record(popup, crate::app::pointer::Hit::Modal);
    let back = match modal.stage {
        Stage::Type => "Cancel",
        Stage::Format { .. } => "Back",
    };
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Retype)
        .key("Enter")
        .key_as("Esc", back);
    let title = modal.title();
    let inner = Surface::new(&title).footer(&footer).render(popup, buf, ctx);
    if inner.height < 3 {
        return;
    }
    let hint = match modal.stage {
        Stage::Type => "type to narrow",
        Stage::Format { .. } => "type to narrow, or a format such as %d.%m.%Y",
    };
    let filter = if picker.filter.is_empty() {
        Line::from(Span::styled(hint, Style::default().fg(ctx.dimmed)))
    } else {
        Line::from(Span::raw(picker.filter.clone()))
    };
    Paragraph::new(filter).render(Rect { height: 1, ..inner }, buf);
    let list = Rect {
        y: inner.y + 1,
        height: inner.height.saturating_sub(1),
        ..inner
    };
    if picker.filtered().is_empty() {
        let said = match modal.typed_format() {
            Some((format, Some(read))) => {
                let arrow = crate::glyphs::get().arrow_right;
                let first = modal.examples.first().cloned().unwrap_or_default();
                format!("{format}  {first} {arrow} {read}")
            }
            Some((format, None)) => format!("{format}: does not read the first value"),
            None => "No type matches".to_string(),
        };
        Paragraph::new(Span::styled(said, Style::default().fg(ctx.text_secondary)))
            .render(Rect { height: 1, ..list }, buf);
    } else {
        Picker::from_state(picker, true).render(list, buf, ctx);
    }
}

fn label(field: CombineField) -> &'static str {
    match field {
        CombineField::Date => "Date:",
        CombineField::Time => "Time:",
        CombineField::Offset => "UTC offset:",
        CombineField::Kind => "As:",
        CombineField::Name => "Name:",
    }
}

/// Past the longest label, "UTC offset:", plus air.
const LABEL_WIDTH: u16 = 13;

/// The combine form over `area`.
pub fn render_combine(area: Rect, buf: &mut Buffer, modal: &CombineModal, ctx: &RenderContext) {
    let fields: Vec<CombineField> = crate::app::form::Form::fields(modal)
        .into_iter()
        .map(|(f, _)| f)
        .collect();
    let width = (area.width * 3 / 4).clamp(30, 56).min(area.width);
    let wanted = if modal.picker.is_some() {
        16
    } else {
        fields.len() as u16 + 6
    };
    let height = wanted.min(area.height);
    if height < 6 {
        return;
    }
    let popup = crate::render::layout::centered_rect(area, width, height);
    let footer = HintBar::from_ctx(ctx)
        .screen(Context::Combine)
        .group("Fields")
        .key("Enter")
        .weight(3)
        .key("Space")
        .weight(2)
        .key("Tab")
        .weight(1)
        .key("Esc")
        .weight(4);
    let kinds: Vec<&str> = crate::formats::column_types::DerivedKind::ALL
        .iter()
        .map(|k| k.name())
        .collect();
    let kind_at = crate::formats::column_types::DerivedKind::ALL
        .iter()
        .position(|k| *k == modal.kind)
        .unwrap_or(0);
    let none = || FormValue::Placeholder(crate::app::modals::retype_modal::NONE);
    let rows = fields
        .into_iter()
        .map(|field| {
            let value = match field {
                CombineField::Date => FormValue::Choice(&modal.date),
                CombineField::Time => modal.time.as_deref().map_or_else(none, FormValue::Choice),
                CombineField::Offset => {
                    modal.offset.as_deref().map_or_else(none, FormValue::Choice)
                }
                CombineField::Kind => FormValue::Options {
                    items: &kinds,
                    selected: kind_at,
                    clicks: None,
                },
                CombineField::Name => FormValue::Input(&modal.name),
            };
            FormLine::Field(field, label(field), value)
        })
        .collect();
    let status = match &modal.problem {
        Some(problem) => (problem.clone(), Style::default().fg(ctx.warning)),
        None => (modal.spec_line(), Style::default().fg(ctx.text_primary)),
    };
    crate::app::pointer::record(popup, crate::app::pointer::Hit::Modal);
    FormView {
        title: "Combine into Datetime",
        screen: Context::Combine,
        footer: Some(footer),
        label_width: LABEL_WIDTH,
        rows,
        focused: Some(modal.focus),
        picker: modal.picker.as_ref().map(|(_, state)| state),
        status: Some(status),
    }
    .render::<CombineModal>(popup, buf, ctx);
}

#[cfg(test)]
mod tests;

//! The type picker and the combine form: one Surface each over the table.

use crate::render::context::RenderContext;
use crate::retype_modal::{CombineField, CombineModal, RetypeModal, Stage};
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, Surface};
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
    crate::pointer::record(popup, crate::pointer::Hit::Modal);
    let back = match modal.stage {
        Stage::Type => "Cancel",
        Stage::Format { .. } => "Back",
    };
    let footer = HintBar::from_ctx(ctx).hints(&[("Enter", "Choose"), ("Esc", back)]);
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
    let fields: Vec<CombineField> = crate::form::Form::fields(modal)
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
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    crate::pointer::record(popup, crate::pointer::Hit::Modal);
    let footer = match &modal.picker {
        Some(_) => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Choose", 3)
            .hint_weighted("type", "Narrow", 1)
            .hint_weighted("Esc", "Back", 4),
        None => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Make", 3)
            .hint_weighted("Space", "Pick", 2)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4),
    };
    let content = Surface::new("Combine into Datetime")
        .footer(&footer)
        .render(popup, buf, ctx);
    if content.height < 3 {
        return;
    }
    let spec_y = content.y + content.height - 1;
    let mut y = content.y;
    let kinds: Vec<&str> = crate::column_types::DerivedKind::ALL
        .iter()
        .map(|k| k.name())
        .collect();
    let kind_at = crate::column_types::DerivedKind::ALL
        .iter()
        .position(|k| *k == modal.kind)
        .unwrap_or(0);
    for &field in &fields {
        if y >= spec_y {
            break;
        }
        let row = Rect {
            y,
            height: 1,
            ..content
        };
        crate::pointer::record_field::<CombineModal>(row, field);
        let value = match field {
            CombineField::Date => FormValue::Choice(&modal.date),
            CombineField::Time => match &modal.time {
                Some(c) => FormValue::Choice(c),
                None => FormValue::Placeholder(crate::retype_modal::NONE),
            },
            CombineField::Offset => match &modal.offset {
                Some(c) => FormValue::Choice(c),
                None => FormValue::Placeholder(crate::retype_modal::NONE),
            },
            CombineField::Kind => FormValue::Options {
                items: &kinds,
                selected: kind_at,
                clicks: None,
            },
            CombineField::Name => FormValue::Choice(&modal.name),
        };
        FormRow {
            label: label(field),
            value,
            focused: modal.focus == field,
            label_width: LABEL_WIDTH,
        }
        .render(row, buf, ctx);
        y += 1;
    }
    if let Some((_, state)) = &modal.picker {
        crate::pointer::record(content, crate::pointer::Hit::Picker);
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
    let (text, style) = match &modal.problem {
        Some(problem) => (problem.clone(), Style::default().fg(ctx.warning)),
        None => (modal.spec_line(), Style::default().fg(ctx.text_primary)),
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
    use polars::prelude::DataType;

    fn text(buf: &Buffer) -> String {
        let area = buf.area;
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
    fn the_picker_lists_the_types_then_the_formats_with_a_preview() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        let mut modal = RetypeModal::new(
            "when".into(),
            DataType::String,
            None,
            vec!["03/04/2024".into()],
        );
        let mut buf = Buffer::empty(area);
        render_retype(area, &mut buf, &modal, &ctx);
        let shown = text(&buf);
        assert!(shown.contains("Type of when, read as str"), "{shown}");
        assert!(shown.contains("as read"), "{shown}");
        assert!(shown.contains("datetime"), "{shown}");
        let date = crate::column_types::TYPE_NAMES
            .iter()
            .position(|n| *n == "date")
            .unwrap();
        modal.picker.select_original(date + 1);
        modal.choose();
        let mut buf = Buffer::empty(area);
        render_retype(area, &mut buf, &modal, &ctx);
        let shown = text(&buf);
        assert!(shown.contains("Date Format"), "{shown}");
        assert!(shown.contains("%d/%m/%Y  03/04/2024"), "{shown}");
        assert!(shown.contains("2024-04-03"), "{shown}");
        for c in "%d %m".chars() {
            modal.picker.type_char(c);
        }
        let mut buf = Buffer::empty(area);
        render_retype(area, &mut buf, &modal, &ctx);
        let shown = text(&buf);
        assert!(
            shown.contains("%d %m: does not read the first value"),
            "{shown}"
        );
    }

    #[test]
    fn the_combine_form_echoes_what_it_makes() {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 80, 24);
        let columns = vec!["Lcl Date".to_string(), "Lcl Time".to_string()];
        let modal = CombineModal::new("Lcl Date".into(), columns.clone(), &columns);
        let mut buf = Buffer::empty(area);
        render_combine(area, &mut buf, &modal, &ctx);
        let shown = text(&buf);
        assert!(shown.contains("Combine into Datetime"), "{shown}");
        assert!(shown.contains("UTC offset:"), "{shown}");
        assert!(
            shown.contains("datetime = datetime from Lcl Date, Lcl Time"),
            "{shown}"
        );
    }
}

//! Chart export dialog rendering: one Surface, a FormRow per field, the actions in
//! the footer.

use crate::chart_export_modal::{ChartExportFocus, ChartExportModal, FIELDS};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Surface};
use ratatui::layout::Rect;

/// The value column's offset: past the longest label, "Description:", plus air.
const LABEL_WIDTH: u16 = 14;

/// Rows the dialog wants: the fields, the blank row and the footer, the border.
pub const HEIGHT: u16 = FIELDS.len() as u16 + 4;

pub fn render_chart_export_modal(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartExportModal,
    ctx: &RenderContext,
) {
    let footer = HintBar::from_ctx(ctx)
        .hint_weighted("Enter", "Export", 3)
        .hint_weighted("Tab", "Next", 1)
        .hint_weighted("Esc", "Cancel", 4);
    let content = Surface::new("Export Chart")
        .footer(&footer)
        .render(area, buf, ctx);
    if content.height < 1 || content.width < 4 {
        return;
    }
    let focus = modal.focus;
    for field in FIELDS {
        let focused = field == focus;
        match field {
            ChartExportFocus::PathInput => modal.path_input.set_focused(focused),
            ChartExportFocus::WidthInput => modal.width_input.set_focused(focused),
            ChartExportFocus::HeightInput => modal.height_input.set_focused(focused),
            ChartExportFocus::TitleInput => modal.title_input.set_focused(focused),
            ChartExportFocus::DescriptionInput => modal.description_input.set_focused(focused),
            ChartExportFocus::NotesInput => modal.notes_input.set_focused(focused),
            ChartExportFocus::SourceInput => modal.source_input.set_focused(focused),
            ChartExportFocus::BylineInput => modal.byline_input.set_focused(focused),
            _ => {}
        }
    }
    // Overlays scroll inside a capped frame: keep the focused field on screen.
    let rows = content.height as usize;
    let at = FIELDS.iter().position(|f| *f == focus).unwrap_or(0);
    let first = at.saturating_sub(rows.saturating_sub(1));
    for (i, field) in FIELDS.iter().enumerate().skip(first).take(rows) {
        let value = match field {
            ChartExportFocus::Format => FormValue::Choice(modal.format.as_str()),
            ChartExportFocus::Style => FormValue::Choice(modal.style.label()),
            ChartExportFocus::Size => FormValue::Choice(modal.size.label()),
            ChartExportFocus::Legend => FormValue::Choice(modal.legend.label()),
            field => match modal.input(*field) {
                Some(input) => FormValue::Input(input),
                None => continue,
            },
        };
        FormRow {
            label: field.label(),
            value,
            focused: *field == focus,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                y: content.y + (i - first) as u16,
                height: 1,
                ..content
            },
            buf,
            ctx,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart_export_modal::ExportDefaults;
    use ratatui::buffer::Buffer;

    fn render_rows(width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        let mut modal = ChartExportModal::new();
        modal.open(&theme, 1000, ExportDefaults::default());
        modal.path_input.set_value("out.png");
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_chart_export_modal(area, &mut buf, &mut modal, &ctx);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    /// One border, a FormRow per field, the actions as footer chips: no bordered
    /// fields, no buttons.
    #[test]
    fn one_surface_with_a_row_per_field() {
        let rows = render_rows(64, HEIGHT);
        assert!(rows[0].contains("Export Chart"), "title: {:?}", rows[0]);
        for row in &rows[1..rows.len() - 1] {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a second border inside the surface: {row:?}"
            );
        }
        let text = rows.join("\n");
        for label in [
            "Path:",
            "Format:",
            "PNG",
            "Style:",
            "Light",
            "Size:",
            "Document",
            "Width:",
            "1600",
            "Height:",
            "1000",
            "Legend:",
            "Title:",
            "Description:",
            "Notes:",
            "Source:",
            "Byline:",
        ] {
            assert!(text.contains(label), "{label}:\n{text}");
        }
        let footer = &rows[rows.len() - 2];
        assert!(
            footer.contains("Enter") && footer.contains("Export") && footer.contains("Esc"),
            "footer chips: {footer:?}"
        );
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (3, 2), (10, 4), (30, 6), (64, 10)] {
            let _ = render_rows(w, h);
        }
    }
}

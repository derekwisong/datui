//! Chart export dialog rendering: a FormView, a row per field, the actions in the
//! footer.

use crate::chart::chart_export_modal::{ChartExportModal, FIELDS};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormLine, FormValue, FormView, HintBar};
use ratatui::layout::Rect;

/// The value column's offset: past the longest label, "Description:", plus air.
const LABEL_WIDTH: u16 = 14;

/// Rows the dialog wants: the fields the chart's type shows, a blank and the
/// status line, the blank row and the footer, the border.
pub fn height(modal: &ChartExportModal) -> u16 {
    modal.shown().len() as u16 + STATUS_ROWS + 4
}

/// The blank under the fields and the status line, which says why Enter did not
/// write. Kept whether or not there is a reason, so nothing moves when one comes.
const STATUS_ROWS: u16 = 2;

pub fn render_chart_export_modal(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartExportModal,
    ctx: &RenderContext,
) {
    let footer = HintBar::from_ctx(ctx)
        .screen(datui_cli::keys::Context::Chart)
        .group("Export dialog")
        .key("Enter")
        .weight(3)
        .key("Tab")
        .weight(1)
        .key("Esc")
        .weight(4);
    let focus = modal.focus;
    for (field, _) in FIELDS {
        if let Some(input) = modal.input_mut(field) {
            input.set_focused(field == focus);
        }
    }
    let rows = modal
        .shown()
        .into_iter()
        .filter_map(|field| {
            let value = match modal.choice(field) {
                Some(choice) => FormValue::Choice(choice),
                None => FormValue::Input(modal.input(field)?),
            };
            Some(FormLine::Field(field, field.label(), value))
        })
        .collect();
    let error = modal.error.clone().unwrap_or_default();
    FormView {
        title: "Export Chart",
        screen: datui_cli::keys::Context::Chart,
        footer: Some(footer),
        label_width: LABEL_WIDTH,
        rows,
        focused: Some(focus),
        picker: None,
        status: Some((error, ratatui::style::Style::default().fg(ctx.warning))),
        shields: true,
    }
    .render::<ChartExportModal>(area, buf, ctx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart::chart_export_modal::ExportDefaults;
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
        let rows = render_rows(64, height(&ChartExportModal::new()));
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

    /// Why Enter did not write sits on the last line above the footer, under a
    /// blank, with every field still drawn above it.
    #[test]
    fn the_reason_sits_on_the_status_line() {
        let ctx = RenderContext::for_test();
        let mut modal = ChartExportModal::new();
        modal.error = Some("Enter a file path.".to_string());
        let area = Rect::new(0, 0, 64, height(&ChartExportModal::new()));
        let mut buf = Buffer::empty(area);
        render_chart_export_modal(area, &mut buf, &mut modal, &ctx);
        let rows: Vec<String> = (0..height(&ChartExportModal::new()))
            .map(|y| (0..64).map(|x| buf[(x, y)].symbol().to_string()).collect())
            .collect();
        let at = rows
            .iter()
            .position(|row| row.contains("Enter a file path."));
        assert_eq!(
            at,
            Some(usize::from(height(&ChartExportModal::new())) - 4),
            "{rows:#?}"
        );
        assert!(
            rows[usize::from(height(&ChartExportModal::new())) - 5]
                .trim_matches(['│', ' '])
                .is_empty()
        );
        assert!(rows.iter().any(|row| row.contains("Byline:")));
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (3, 2), (10, 4), (30, 6), (64, 10)] {
            let _ = render_rows(w, h);
        }
    }
}

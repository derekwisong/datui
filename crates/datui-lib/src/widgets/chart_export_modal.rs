//! Chart export modal rendering: one Surface, a Picker for the format,
//! FormRows for path, title and size, actions in the footer.

use crate::chart_export::ChartExportFormat;
use crate::chart_export_modal::{ChartExportFocus, ChartExportModal};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, SectionRule, Surface};
use ratatui::layout::Rect;

/// The value column's offset inside the options half: past the longest label,
/// "Height:", plus two cells of air.
const LABEL_WIDTH: u16 = 9;

/// Columns the format list needs: rail plus the longest name plus air.
const FORMAT_WIDTH: u16 = 8;

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
    if content.height < 2 || content.width < 4 {
        return;
    }

    // Left: the format picker under its section rule. Right: one FormRow per
    // option, values on one column.
    let format_focused = modal.focus == ChartExportFocus::FormatSelector;
    SectionRule {
        title: "Format",
        chip: None,
        focused: format_focused,
    }
    .render(
        Rect {
            width: FORMAT_WIDTH.min(content.width),
            height: 1,
            ..content
        },
        buf,
        ctx,
    );
    let list_area = Rect {
        y: content.y + 1,
        width: FORMAT_WIDTH.min(content.width),
        height: content.height - 1,
        ..content
    };
    let names: Vec<&str> = ChartExportFormat::ALL.iter().map(|f| f.as_str()).collect();
    let selected = ChartExportFormat::ALL
        .iter()
        .position(|f| *f == modal.selected_format);
    Picker::new(names, selected, format_focused).render(list_area, buf, ctx);

    modal
        .path_input
        .set_focused(modal.focus == ChartExportFocus::PathInput);
    modal
        .title_input
        .set_focused(modal.focus == ChartExportFocus::TitleInput);
    modal
        .width_input
        .set_focused(modal.focus == ChartExportFocus::WidthInput);
    modal
        .height_input
        .set_focused(modal.focus == ChartExportFocus::HeightInput);

    let rows: [(&str, FormValue, ChartExportFocus); 4] = [
        (
            "Path:",
            FormValue::Input(&modal.path_input),
            ChartExportFocus::PathInput,
        ),
        (
            "Title:",
            FormValue::Input(&modal.title_input),
            ChartExportFocus::TitleInput,
        ),
        (
            "Width:",
            FormValue::Input(&modal.width_input),
            ChartExportFocus::WidthInput,
        ),
        (
            "Height:",
            FormValue::Input(&modal.height_input),
            ChartExportFocus::HeightInput,
        ),
    ];

    let options_x = content.x + FORMAT_WIDTH + 2;
    let options_width = (content.x + content.width).saturating_sub(options_x);
    if options_width == 0 {
        return;
    }
    for (i, (label, value, focus)) in rows.into_iter().enumerate() {
        // Aligned with the format items, one row below the section rule.
        let y = content.y + 1 + i as u16;
        if y >= content.y + content.height {
            break;
        }
        FormRow {
            label,
            value,
            focused: modal.focus == focus,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                x: options_x,
                y,
                width: options_width,
                height: 1,
            },
            buf,
            ctx,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn render_rows(width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        let mut modal = ChartExportModal::new();
        modal.open(&theme, 1000);
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

    /// One border, the format list beside one FormRow per option, and the
    /// actions as footer chips — no bordered fields, no buttons.
    #[test]
    fn one_surface_with_the_format_list_and_the_rows() {
        let rows = render_rows(64, 10);
        assert!(rows[0].contains("Export Chart"), "title: {:?}", rows[0]);
        for row in &rows[1..9] {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a second border inside the surface: {row:?}"
            );
        }
        assert!(rows[1].contains("Format"));
        assert!(rows[2].contains("PNG") && rows[2].contains("Path:"));
        assert!(rows[3].contains("EPS") && rows[3].contains("Title:"));
        assert!(rows[4].contains("Width:") && rows[4].contains("1024"));
        assert!(rows[5].contains("Height:") && rows[5].contains("768"));
        assert!(
            rows[8].contains("Enter") && rows[8].contains("Export") && rows[8].contains("Esc"),
            "footer chips: {:?}",
            rows[8]
        );
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (3, 2), (10, 4), (30, 6), (64, 10)] {
            let _ = render_rows(w, h);
        }
    }
}

//! The dialog that writes the Data Quality report on screen to a file: a path and
//! a form, and a line saying what the form holds or why Enter did not write.

use crate::analysis::quality_export::ExportForm;
use crate::render::layout::dialog_in;
use crate::widgets::data_quality::DataQualityWidgetConfig;
use crate::widgets::ui::{FormLine, FormValue, FormView};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;

const LABEL_WIDTH: u16 = 9;

pub fn render(
    form: &ExportForm,
    config: &DataQualityWidgetConfig<'_>,
    area: Rect,
    buf: &mut Buffer,
) {
    let ctx = config.ctx;
    let width = 64.min(area.width.saturating_sub(2));
    // Path, format, a blank and the status line, inside the frame.
    let popup = dialog_in(area, width, 6);
    // Not a `Form`: its keys are Data Quality's, and its rows take no clicks.
    FormView {
        title: "Export Report",
        screen: datui_cli::keys::Context::DataQuality,
        footer: None,
        label_width: LABEL_WIDTH,
        rows: vec![
            FormLine::Field(false, "Path:", FormValue::Input(&form.path)),
            FormLine::Field(true, "Format:", FormValue::Choice(form.format.label())),
        ],
        focused: Some(form.on_format),
        picker: None,
        status: Some(match &form.error {
            Some(error) => (error.clone(), Style::default().fg(ctx.warning)),
            None => (
                crate::glyphs::dotted(&format!("{} · no read", form.format.holds())),
                Style::default().fg(ctx.dimmed),
            ),
        }),
        // The report under it is shielded by Data Quality's drawing.
        shields: false,
    }
    .render_unrecorded(popup, buf, ctx);
}

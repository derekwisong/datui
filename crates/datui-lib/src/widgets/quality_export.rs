//! The dialog that writes the Data Quality report on screen to a file: a path and
//! a form, and a line saying what the form holds or why Enter did not write.

use crate::quality_export::ExportForm;
use crate::render::layout::centered_rect;
use crate::widgets::data_quality::{DataQualityWidgetConfig, fit};
use crate::widgets::ui::{FormRow, FormValue, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};

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
    let popup = centered_rect(area.inner(ratatui::layout::Margin::new(1, 1)), width, 6);
    let content = Surface::new("Export Report").render(popup, buf, ctx);
    if content.height < 2 || content.width < 8 {
        return;
    }
    let line = |index: u16| Rect {
        y: content.y + index,
        height: 1,
        ..content
    };
    FormRow {
        label: "Path:",
        value: FormValue::Input(&form.path),
        focused: !form.on_format,
        label_width: LABEL_WIDTH,
    }
    .render(line(0), buf, ctx);
    FormRow {
        label: "Format:",
        value: FormValue::Choice(form.format.label()),
        focused: form.on_format,
        label_width: LABEL_WIDTH,
    }
    .render(line(1), buf, ctx);
    let (status, warn) = match &form.error {
        Some(error) => (error.clone(), true),
        None => (
            crate::glyphs::dotted(&format!("{} · no read", form.format.holds())),
            false,
        ),
    };
    Paragraph::new(Line::styled(
        fit(&crate::glyphs::dotted(&status), content.width as usize),
        Style::default().fg(if warn { ctx.warning } else { ctx.dimmed }),
    ))
    .render(line(content.height - 1), buf);
}

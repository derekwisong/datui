//! Chart view rendering (chart widget and chart export modal).
//!
//! Draws only what `App::chart_cache` already holds. The data is prepared off the UI
//! thread by `App::ensure_chart_data`; while the current selection's data is still on
//! its way the chart area says so over the chart it replaces, or the empty axes, or in
//! place of a plot when there is neither. A selection that failed to prepare shows why.

use crate::ChartRequest;
use crate::chart_plot::PlotContext;
use crate::render::context::RenderContext;
use crate::widgets::{self, chart::ChartView, ui::Working};
use ratatui::layout::Rect;
use ratatui::widgets::{Clear, Widget};

pub fn render(
    chart_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    Clear.render(chart_area, buf);
    app.chart.modal.units = app
        .data_table_state
        .as_ref()
        .map(|state| state.units())
        .unwrap_or_default();

    let request = ChartRequest::from_modal(&app.chart.modal);
    let outcome = request
        .as_ref()
        .and_then(|request| app.chart.cache.get(request));
    let error = outcome.and_then(|o| o.as_ref().err()).map(String::as_str);
    // Chosen but not here yet: it is being prepared (`App::ensure_chart_data` runs
    // after every event). The chart of the same columns before an option changed
    // stays up meanwhile.
    let computing = request.is_some() && outcome.is_none();
    let prepared = match &request {
        Some(request) if computing => app.chart.cache.standing_in(request),
        _ => outcome.and_then(|o| o.as_ref().ok()),
    };
    let notes = prepared
        .map(|p| app.chart_notes_of(p, crate::glyphs::get().middot))
        .unwrap_or_default();
    let aggregating = request.as_ref().is_some_and(ChartRequest::aggregates);

    let schema = app
        .data_table_state
        .as_ref()
        .map(|state| state.schema().as_ref());
    // The spec charted, the picker's choice previewed; the panel's own until it
    // asks for a chart.
    let unrequested;
    let spec = match &request {
        Some(request) => &request.spec,
        None => {
            unrequested = app.chart.modal.effective_spec();
            &unrequested
        }
    };
    let plot = crate::chart_plot::plot(
        prepared,
        &PlotContext {
            modal: &app.chart.modal,
            spec,
            numbers: &ctx.number_format,
            schema,
        },
    );

    // An aggregate reads every row: say how many, where the table knows.
    let grouping;
    let working_text = if aggregating {
        grouping = app.chart_status();
        grouping.as_str()
    } else {
        "Computing chart..."
    };
    widgets::chart::render_chart_view(
        chart_area,
        buf,
        &mut app.chart.modal,
        &app.theme,
        ctx,
        ChartView {
            plot,
            notes,
            error,
            working: computing.then_some(Working {
                text: working_text,
                frame: app.throbber_frame as usize,
            }),
            schema,
        },
    );

    if app.chart.export_modal.active {
        // A commitment, so a compact centered dialog, never scaling with the
        // terminal; it scrolls inside its frame on a short one.
        let modal_width = (chart_area.width * 3 / 4).min(66);
        let modal_height =
            widgets::chart_export_modal::height(&app.chart.export_modal).min(chart_area.height);
        let modal_area =
            crate::render::layout::centered_rect(chart_area, modal_width, modal_height);
        widgets::chart_export_modal::render_chart_export_modal(
            modal_area,
            buf,
            &mut app.chart.export_modal,
            ctx,
        );
    }
}

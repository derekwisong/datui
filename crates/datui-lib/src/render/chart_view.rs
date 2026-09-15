//! Chart view rendering (chart widget and chart export modal).
//!
//! Draws only what `App::chart_cache` already holds. The data is prepared off the UI
//! thread by `App::ensure_chart_data`; while the current selection's data is still on
//! its way the chart area shows the empty axes and the control bar spins.

use crate::chart_data;
use crate::chart_modal::ChartKind;
use crate::render::context::RenderContext;
use crate::widgets::{self, chart::ChartRenderData};
use crate::ChartRequest;
use ratatui::layout::Rect;
use ratatui::widgets::{Clear, Widget};

pub fn render(
    chart_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    _ctx: &RenderContext,
) {
    Clear.render(chart_area, buf);

    let request = ChartRequest::from_modal(&app.chart_modal);
    let ready = request
        .as_ref()
        .is_some_and(|r| app.chart_cache.satisfies(r));
    let cache = &app.chart_cache;

    let render_data = match app.chart_modal.chart_kind {
        ChartKind::XY => {
            let mut series = None;
            let mut x_bounds = None;
            let mut x_axis_kind = match (
                app.chart_modal.effective_x_column(),
                app.data_table_state.as_ref(),
            ) {
                (Some(x), Some(state)) => {
                    chart_data::x_axis_temporal_kind_for_column(&state.schema, x)
                }
                _ => chart_data::XAxisTemporalKind::Numeric,
            };
            if ready {
                match request {
                    Some(ChartRequest::XY { .. }) => {
                        if let Some(c) = cache.xy.as_ref() {
                            x_axis_kind = c.x_axis_kind;
                            series = if app.chart_modal.log_scale {
                                c.series_log.as_ref().or(Some(&c.series))
                            } else {
                                Some(&c.series)
                            };
                        }
                    }
                    Some(ChartRequest::XRange { .. }) => {
                        if let Some(c) = cache.x_range.as_ref() {
                            x_axis_kind = c.x_axis_kind;
                            x_bounds = Some((c.x_min, c.x_max));
                        }
                    }
                    _ => {}
                }
            }
            ChartRenderData::XY {
                series,
                x_axis_kind,
                x_bounds,
            }
        }
        ChartKind::Histogram => ChartRenderData::Histogram {
            data: cache.histogram.as_ref().filter(|_| ready).map(|c| &c.data),
        },
        ChartKind::BoxPlot => ChartRenderData::BoxPlot {
            data: cache.box_plot.as_ref().filter(|_| ready).map(|c| &c.data),
        },
        ChartKind::Kde => ChartRenderData::Kde {
            data: cache.kde.as_ref().filter(|_| ready).map(|c| &c.data),
        },
        ChartKind::Heatmap => ChartRenderData::Heatmap {
            data: cache.heatmap.as_ref().filter(|_| ready).map(|c| &c.data),
        },
    };

    widgets::chart::render_chart_view(
        chart_area,
        buf,
        &mut app.chart_modal,
        &app.theme,
        render_data,
    );

    if app.chart_export_modal.active {
        const CHART_EXPORT_MODAL_HEIGHT: u16 = 20;
        let modal_width = (chart_area.width * 3 / 4).clamp(80, 108);
        let modal_height = CHART_EXPORT_MODAL_HEIGHT
            .min(chart_area.height)
            .max(CHART_EXPORT_MODAL_HEIGHT);
        let modal_x = chart_area.x + chart_area.width.saturating_sub(modal_width) / 2;
        let modal_y = chart_area.y + chart_area.height.saturating_sub(modal_height) / 2;
        let modal_area = Rect {
            x: modal_x,
            y: modal_y,
            width: modal_width,
            height: modal_height,
        };
        widgets::chart_export_modal::render_chart_export_modal(
            modal_area,
            buf,
            &mut app.chart_export_modal,
            _ctx.modal_border,
            _ctx.modal_border_active,
        );
    }
}

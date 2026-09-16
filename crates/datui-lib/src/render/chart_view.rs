//! Chart view rendering (chart widget and chart export modal).
//!
//! Draws only what `App::chart_cache` already holds. The data is prepared off the UI
//! thread by `App::ensure_chart_data`; while the current selection's data is still on
//! its way the chart area shows the empty axes and the control bar spins.

use crate::chart_data;
use crate::chart_modal::ChartKind;
use crate::render::context::RenderContext;
use crate::widgets::{self, chart::ChartRenderData};
use crate::{ChartPrepared, ChartRequest};
use ratatui::layout::Rect;
use ratatui::widgets::{Clear, Widget};

pub fn render(
    chart_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    _ctx: &RenderContext,
) {
    Clear.render(chart_area, buf);

    let prepared = ChartRequest::from_modal(&app.chart_modal)
        .and_then(|request| app.chart_cache.prepared(&request));

    let render_data = match (app.chart_modal.chart_kind, prepared) {
        (ChartKind::XY, Some(ChartPrepared::XY(c))) => ChartRenderData::XY {
            series: if app.chart_modal.log_scale {
                c.series_log.as_ref().or(Some(&c.series))
            } else {
                Some(&c.series)
            },
            x_axis_kind: c.x_axis_kind,
            x_bounds: None,
        },
        (ChartKind::XY, Some(ChartPrepared::XRange(c))) => ChartRenderData::XY {
            series: None,
            x_axis_kind: c.x_axis_kind,
            x_bounds: Some((c.x_min, c.x_max)),
        },
        // Still on its way: empty axes, typed from the schema so the labels are right.
        (ChartKind::XY, _) => ChartRenderData::XY {
            series: None,
            x_axis_kind: match (
                app.chart_modal.effective_x_column(),
                app.data_table_state.as_ref(),
            ) {
                (Some(x), Some(state)) => {
                    chart_data::x_axis_temporal_kind_for_column(&state.schema, x)
                }
                _ => chart_data::XAxisTemporalKind::Numeric,
            },
            x_bounds: None,
        },
        (ChartKind::Histogram, prepared) => ChartRenderData::Histogram {
            data: match prepared {
                Some(ChartPrepared::Histogram(d)) => Some(d),
                _ => None,
            },
        },
        (ChartKind::BoxPlot, prepared) => ChartRenderData::BoxPlot {
            data: match prepared {
                Some(ChartPrepared::BoxPlot(d)) => Some(d),
                _ => None,
            },
        },
        (ChartKind::Kde, prepared) => ChartRenderData::Kde {
            data: match prepared {
                Some(ChartPrepared::Kde(d)) => Some(d),
                _ => None,
            },
        },
        (ChartKind::Heatmap, prepared) => ChartRenderData::Heatmap {
            data: match prepared {
                Some(ChartPrepared::Heatmap(d)) => Some(d),
                _ => None,
            },
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

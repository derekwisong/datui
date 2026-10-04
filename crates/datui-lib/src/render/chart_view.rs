//! Chart view rendering (chart widget and chart export modal).
//!
//! Draws only what `App::chart_cache` already holds. The data is prepared off the UI
//! thread by `App::ensure_chart_data`; while the current selection's data is still on
//! its way the chart area says so over the chart it replaces, or the empty axes, or in
//! place of a plot when there is neither. A selection that failed to prepare shows why.

use crate::chart_data;
use crate::chart_modal::ChartKind;
use crate::render::context::RenderContext;
use crate::widgets::{
    self,
    chart::{ChartRenderData, ChartView, PlotNumbers},
    ui::Working,
};
use crate::{ChartPrepared, ChartRequest};
use ratatui::layout::Rect;
use ratatui::widgets::{Clear, Widget};

pub fn render(
    chart_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    Clear.render(chart_area, buf);
    app.chart_modal.units = app
        .data_table_state
        .as_ref()
        .map(|state| state.units())
        .unwrap_or_default();

    let request = ChartRequest::from_modal(&app.chart_modal);
    let outcome = request
        .as_ref()
        .and_then(|request| app.chart_cache.get(request));
    let error = outcome.and_then(|o| o.as_ref().err()).map(String::as_str);
    // Chosen but not here yet: it is being prepared (`App::ensure_chart_data` runs
    // after every event). The chart of the same columns before an option changed
    // stays up meanwhile.
    let computing = request.is_some() && outcome.is_none();
    let prepared = match &request {
        Some(request) if computing => app.chart_cache.standing_in(request),
        _ => outcome.and_then(|o| o.as_ref().ok()),
    };
    let notes = prepared.map(ChartPrepared::notes).unwrap_or_default();

    // Axis numbers print as the table prints their columns; whole ones tick whole.
    let schema = app
        .data_table_state
        .as_ref()
        .map(|state| state.schema().as_ref());
    let column = |name: &str| chart_data::AxisNumbers::column(&ctx.number_format, schema, name);
    let columns =
        |names: &[String]| chart_data::AxisNumbers::columns(&ctx.number_format, schema, names);
    let modal = &app.chart_modal;
    let xy_numbers = || PlotNumbers {
        x: modal
            .effective_x_column()
            .map(|x| column(x))
            .unwrap_or_default(),
        y: columns(&modal.effective_y_columns()),
    };

    let render_data = match (app.chart_modal.chart_kind, prepared) {
        (ChartKind::XY, Some(ChartPrepared::XY(c))) => ChartRenderData::XY {
            series: if app.chart_modal.log_scale {
                c.series_log.as_ref().or(Some(&c.series))
            } else {
                Some(&c.series)
            },
            breaks: Some(&c.breaks),
            values: Some(&c.series),
            x_axis_kind: c.x_axis_kind,
            x_bounds: None,
            numbers: xy_numbers(),
        },
        (ChartKind::XY, Some(ChartPrepared::XRange(c))) => ChartRenderData::XY {
            series: None,
            breaks: None,
            values: None,
            x_axis_kind: c.x_axis_kind,
            x_bounds: Some((c.x_min, c.x_max)),
            numbers: xy_numbers(),
        },
        // Still on its way: empty axes, typed from the schema so the labels are right.
        (ChartKind::XY, _) => {
            let x_axis_kind = match (modal.effective_x_column(), schema) {
                (Some(x), Some(schema)) => chart_data::x_axis_temporal_kind_for_column(schema, x),
                _ => chart_data::XAxisTemporalKind::Numeric,
            };
            ChartRenderData::XY {
                series: None,
                breaks: None,
                values: None,
                x_axis_kind,
                x_bounds: None,
                numbers: xy_numbers(),
            }
        }
        (ChartKind::Histogram, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::Histogram(d)) => Some(d),
                _ => None,
            };
            ChartRenderData::Histogram {
                data,
                x: data.map(|d| column(&d.column)).unwrap_or_default(),
            }
        }
        (ChartKind::BoxPlot, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::BoxPlot(d)) => Some(d),
                _ => None,
            };
            let names: Vec<String> = data
                .map(|d| d.stats.iter().map(|s| s.name.clone()).collect())
                .unwrap_or_default();
            ChartRenderData::BoxPlot {
                data,
                y: columns(&names),
            }
        }
        (ChartKind::Kde, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::Kde(d)) => Some(d),
                _ => None,
            };
            let names: Vec<String> = data
                .map(|d| d.series.iter().map(|s| s.name.clone()).collect())
                .unwrap_or_default();
            ChartRenderData::Kde {
                data,
                x: columns(&names),
            }
        }
        (ChartKind::Heatmap, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::Heatmap(d)) => Some(d),
                _ => None,
            };
            ChartRenderData::Heatmap {
                data,
                numbers: PlotNumbers {
                    x: data.map(|d| column(&d.x_column)).unwrap_or_default(),
                    y: data.map(|d| column(&d.y_column)).unwrap_or_default(),
                },
            }
        }
        (ChartKind::Bar, prepared) => ChartRenderData::Bar {
            data: match prepared {
                Some(ChartPrepared::Bar(d)) => Some(d),
                _ => None,
            },
        },
    };

    widgets::chart::render_chart_view(
        chart_area,
        buf,
        &mut app.chart_modal,
        &app.theme,
        ctx,
        ChartView {
            data: render_data,
            notes,
            error,
            working: computing.then_some(Working {
                text: "Computing chart...",
                frame: app.throbber_frame as usize,
            }),
        },
    );

    if app.chart_export_modal.active {
        // A commitment, so a compact centered dialog: the format list plus a
        // row per option, never scaling with the terminal.
        let modal_width = (chart_area.width * 3 / 4).min(66);
        let modal_height = 9.min(chart_area.height);
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
            ctx,
        );
    }
}

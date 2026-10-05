//! Chart view rendering (chart widget and chart export modal).
//!
//! Draws only what `App::chart_cache` already holds. The data is prepared off the UI
//! thread by `App::ensure_chart_data`; while the current selection's data is still on
//! its way the chart area says so over the chart it replaces, or the empty axes, or in
//! place of a plot when there is neither. A selection that failed to prepare shows why.

use crate::chart_data;
use crate::chart_modal::Mark;
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
    let aggregating = request.as_ref().is_some_and(ChartRequest::aggregates);

    // Axis numbers print as the table prints their columns; whole ones tick whole.
    let schema = app
        .data_table_state
        .as_ref()
        .map(|state| state.schema().as_ref());
    let column = |name: &str| chart_data::AxisNumbers::column(&ctx.number_format, schema, name);
    let columns =
        |names: &[String]| chart_data::AxisNumbers::columns(&ctx.number_format, schema, names);
    let modal = &app.chart_modal;
    let spec = modal.effective_spec();
    let x_name = spec.encoding.x.field.clone();
    let y_numbers = || {
        use crate::chart_modal::Aggregate;
        match spec.encoding.y.aggregate {
            Aggregate::Count | Aggregate::Distinct => {
                chart_data::AxisNumbers::count(&ctx.number_format)
            }
            // A mean or median of whole numbers is not whole.
            a if a.is_fractional() => columns(&spec.encoding.y.field).fractional(),
            _ => columns(&spec.encoding.y.field),
        }
    };
    let xy_numbers = || PlotNumbers {
        x: x_name.as_deref().map(column).unwrap_or_default(),
        y: y_numbers(),
    };
    const NO_NAMES: &[String] = &[];

    let render_data = match (modal.mark(), prepared) {
        (Mark::Line | Mark::Scatter, Some(ChartPrepared::XY(c))) => ChartRenderData::XY {
            series: if modal.log_scale {
                c.series_log.as_ref().or(Some(&c.series))
            } else {
                Some(&c.series)
            },
            breaks: Some(&c.breaks),
            values: Some(&c.series),
            names: &c.names,
            x_axis_kind: c.x_axis_kind,
            x_bounds: None,
            other: c.other,
            numbers: xy_numbers(),
        },
        (Mark::Line | Mark::Scatter, Some(ChartPrepared::XRange(c))) => ChartRenderData::XY {
            series: None,
            breaks: None,
            values: None,
            names: NO_NAMES,
            x_axis_kind: c.x_axis_kind,
            x_bounds: Some((c.x_min, c.x_max)),
            other: false,
            numbers: xy_numbers(),
        },
        // Still on its way: empty axes, typed from the schema so the labels are right.
        (Mark::Line | Mark::Scatter, _) => {
            let x_axis_kind = match (x_name.as_deref(), schema) {
                (Some(x), Some(schema)) => chart_data::x_axis_temporal_kind_for_column(schema, x),
                _ => chart_data::XAxisTemporalKind::Numeric,
            };
            ChartRenderData::XY {
                series: None,
                breaks: None,
                values: None,
                names: NO_NAMES,
                x_axis_kind,
                x_bounds: None,
                other: false,
                numbers: xy_numbers(),
            }
        }
        (Mark::Histogram, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::Histogram(d)) => Some(d),
                _ => None,
            };
            ChartRenderData::Histogram {
                data,
                x: data.map(|d| column(&d.column)).unwrap_or_default(),
            }
        }
        (Mark::Box, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::BoxPlot(d)) => Some(d),
                _ => None,
            };
            ChartRenderData::BoxPlot {
                data,
                y: columns(&spec.encoding.y.field),
            }
        }
        (Mark::Kde, prepared) => {
            let data = match prepared {
                Some(ChartPrepared::Kde(d)) => Some(d),
                _ => None,
            };
            ChartRenderData::Kde {
                data,
                x: x_name.as_deref().map(column).unwrap_or_default(),
            }
        }
        (Mark::Heatmap, prepared) => {
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
        (Mark::Bar, prepared) => ChartRenderData::Bar {
            data: match prepared {
                Some(ChartPrepared::Bar(d)) => Some(d),
                _ => None,
            },
        },
    };

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
        &mut app.chart_modal,
        &app.theme,
        ctx,
        ChartView {
            data: render_data,
            notes,
            error,
            working: computing.then_some(Working {
                text: working_text,
                frame: app.throbber_frame as usize,
            }),
            schema,
        },
    );

    if app.chart_export_modal.active {
        // A commitment, so a compact centered dialog, never scaling with the
        // terminal; it scrolls inside its frame on a short one.
        let modal_width = (chart_area.width * 3 / 4).min(66);
        let modal_height =
            widgets::chart_export_modal::height(&app.chart_export_modal).min(chart_area.height);
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

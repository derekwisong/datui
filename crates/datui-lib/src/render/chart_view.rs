//! Chart view rendering (cache prep, chart widget, chart export modal).

use crate::chart_data;
use crate::chart_modal::ChartKind;
use crate::render::context::RenderContext;
use crate::widgets;
use ratatui::layout::Rect;
use ratatui::widgets::{Clear, Widget};

/// Renders the chart view when input_mode is Chart: fills cache, draws chart, then export modal if active.
pub fn render(
    chart_area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    _ctx: &RenderContext,
) {
    Clear.render(chart_area, buf);
    let mut xy_series: Option<&Vec<Vec<(f64, f64)>>> = None;
    let mut x_axis_kind = chart_data::XAxisTemporalKind::Numeric;
    let mut x_bounds: Option<(f64, f64)> = None;
    let mut hist_data: Option<&chart_data::HistogramData> = None;
    let mut box_data: Option<&chart_data::BoxPlotData> = None;
    let mut kde_data: Option<&chart_data::KdeData> = None;
    let mut heatmap_data: Option<&chart_data::HeatmapData> = None;

    let row_limit_opt = app.chart_modal.row_limit;
    let row_limit = app.chart_modal.effective_row_limit();
    match app.chart_modal.chart_kind {
        ChartKind::XY => {
            if let Some(x_column) = app.chart_modal.effective_x_column() {
                let x_key = x_column.to_string();
                let y_columns = app.chart_modal.effective_y_columns();
                if !y_columns.is_empty() {
                    let use_cache = app.chart_cache.xy.as_ref().filter(|c| {
                        c.x_column == x_key
                            && c.y_columns == y_columns
                            && c.row_limit == row_limit_opt
                    });
                    if use_cache.is_none() {
                        if let Some(state) = app.data_table_state.as_ref() {
                            if let Ok(result) = chart_data::prepare_chart_data(
                                &state.lf,
                                &state.schema,
                                x_column,
                                &y_columns,
                                row_limit,
                            ) {
                                app.chart_cache.xy = Some(crate::ChartCacheXY {
                                    x_column: x_key.clone(),
                                    y_columns: y_columns.clone(),
                                    row_limit: row_limit_opt,
                                    series: result.series,
                                    series_log: None,
                                    x_axis_kind: result.x_axis_kind,
                                });
                            }
                        }
                    }
                    if app.chart_modal.log_scale {
                        if let Some(cache) = app.chart_cache.xy.as_mut() {
                            if cache.x_column == x_key
                                && cache.y_columns == y_columns
                                && cache.row_limit == row_limit_opt
                                && cache.series_log.is_none()
                                && cache.series.iter().any(|s| !s.is_empty())
                            {
                                cache.series_log = Some(
                                    cache
                                        .series
                                        .iter()
                                        .map(|pts| {
                                            pts.iter()
                                                .map(|&(x, y)| (x, y.max(0.0).ln_1p()))
                                                .collect()
                                        })
                                        .collect(),
                                );
                            }
                        }
                    }
                    if let Some(cache) = app.chart_cache.xy.as_ref() {
                        if cache.x_column == x_key
                            && cache.y_columns == y_columns
                            && cache.row_limit == row_limit_opt
                        {
                            x_axis_kind = cache.x_axis_kind;
                            if app.chart_modal.log_scale {
                                if let Some(ref log) = cache.series_log {
                                    if log.iter().any(|v| !v.is_empty()) {
                                        xy_series = Some(log);
                                    }
                                }
                            } else if cache.series.iter().any(|s| !s.is_empty()) {
                                xy_series = Some(&cache.series);
                            }
                        }
                    }
                } else {
                    let use_cache = app
                        .chart_cache
                        .x_range
                        .as_ref()
                        .filter(|c| c.x_column == x_key && c.row_limit == row_limit_opt);
                    if use_cache.is_none() {
                        if let Some(state) = app.data_table_state.as_ref() {
                            if let Ok(result) = chart_data::prepare_chart_x_range(
                                &state.lf,
                                &state.schema,
                                x_column,
                                row_limit,
                            ) {
                                app.chart_cache.x_range = Some(crate::ChartCacheXRange {
                                    x_column: x_key.clone(),
                                    row_limit: row_limit_opt,
                                    x_min: result.x_min,
                                    x_max: result.x_max,
                                    x_axis_kind: result.x_axis_kind,
                                });
                            }
                        }
                    }
                    if let Some(cache) = app.chart_cache.x_range.as_ref() {
                        if cache.x_column == x_key && cache.row_limit == row_limit_opt {
                            x_axis_kind = cache.x_axis_kind;
                            x_bounds = Some((cache.x_min, cache.x_max));
                        }
                    } else if let Some(state) = app.data_table_state.as_ref() {
                        x_axis_kind =
                            chart_data::x_axis_temporal_kind_for_column(&state.schema, x_column);
                    }
                }
            }
        }
        ChartKind::Histogram => {
            if let (Some(state), Some(column)) = (
                app.data_table_state.as_ref(),
                app.chart_modal.effective_hist_column(),
            ) {
                let bins = app.chart_modal.hist_bins;
                let use_cache = app.chart_cache.histogram.as_ref().filter(|c| {
                    c.column == column && c.bins == bins && c.row_limit == row_limit_opt
                });
                if use_cache.is_none() {
                    if let Ok(data) =
                        chart_data::prepare_histogram_data(&state.lf, &column, bins, row_limit)
                    {
                        app.chart_cache.histogram = Some(crate::ChartCacheHistogram {
                            column: column.clone(),
                            bins,
                            row_limit: row_limit_opt,
                            data,
                        });
                    }
                }
                hist_data = app
                    .chart_cache
                    .histogram
                    .as_ref()
                    .filter(|c| {
                        c.column == column && c.bins == bins && c.row_limit == row_limit_opt
                    })
                    .map(|c| &c.data);
            }
        }
        ChartKind::BoxPlot => {
            if let (Some(state), Some(column)) = (
                app.data_table_state.as_ref(),
                app.chart_modal.effective_box_column(),
            ) {
                let use_cache = app
                    .chart_cache
                    .box_plot
                    .as_ref()
                    .filter(|c| c.column == column && c.row_limit == row_limit_opt);
                if use_cache.is_none() {
                    if let Ok(data) = chart_data::prepare_box_plot_data(
                        &state.lf,
                        std::slice::from_ref(&column),
                        row_limit,
                    ) {
                        app.chart_cache.box_plot = Some(crate::ChartCacheBoxPlot {
                            column: column.clone(),
                            row_limit: row_limit_opt,
                            data,
                        });
                    }
                }
                box_data = app
                    .chart_cache
                    .box_plot
                    .as_ref()
                    .filter(|c| c.column == column && c.row_limit == row_limit_opt)
                    .map(|c| &c.data);
            }
        }
        ChartKind::Kde => {
            if let (Some(state), Some(column)) = (
                app.data_table_state.as_ref(),
                app.chart_modal.effective_kde_column(),
            ) {
                let bandwidth = app.chart_modal.kde_bandwidth_factor;
                let use_cache = app.chart_cache.kde.as_ref().filter(|c| {
                    c.column == column
                        && c.bandwidth_factor == bandwidth
                        && c.row_limit == row_limit_opt
                });
                if use_cache.is_none() {
                    if let Ok(data) = chart_data::prepare_kde_data(
                        &state.lf,
                        std::slice::from_ref(&column),
                        bandwidth,
                        row_limit,
                    ) {
                        app.chart_cache.kde = Some(crate::ChartCacheKde {
                            column: column.clone(),
                            bandwidth_factor: bandwidth,
                            row_limit: row_limit_opt,
                            data,
                        });
                    }
                }
                kde_data = app
                    .chart_cache
                    .kde
                    .as_ref()
                    .filter(|c| {
                        c.column == column
                            && c.bandwidth_factor == bandwidth
                            && c.row_limit == row_limit_opt
                    })
                    .map(|c| &c.data);
            }
        }
        ChartKind::Heatmap => {
            if let (Some(state), Some(x_column), Some(y_column)) = (
                app.data_table_state.as_ref(),
                app.chart_modal.effective_heatmap_x_column(),
                app.chart_modal.effective_heatmap_y_column(),
            ) {
                let bins = app.chart_modal.heatmap_bins;
                let use_cache = app.chart_cache.heatmap.as_ref().filter(|c| {
                    c.x_column == x_column
                        && c.y_column == y_column
                        && c.bins == bins
                        && c.row_limit == row_limit_opt
                });
                if use_cache.is_none() {
                    if let Ok(data) = chart_data::prepare_heatmap_data(
                        &state.lf, &x_column, &y_column, bins, row_limit,
                    ) {
                        app.chart_cache.heatmap = Some(crate::ChartCacheHeatmap {
                            x_column: x_column.clone(),
                            y_column: y_column.clone(),
                            bins,
                            row_limit: row_limit_opt,
                            data,
                        });
                    }
                }
                heatmap_data = app
                    .chart_cache
                    .heatmap
                    .as_ref()
                    .filter(|c| {
                        c.x_column == x_column
                            && c.y_column == y_column
                            && c.bins == bins
                            && c.row_limit == row_limit_opt
                    })
                    .map(|c| &c.data);
            }
        }
    }

    let render_data = match app.chart_modal.chart_kind {
        ChartKind::XY => widgets::chart::ChartRenderData::XY {
            series: xy_series,
            x_axis_kind,
            x_bounds,
        },
        ChartKind::Histogram => widgets::chart::ChartRenderData::Histogram { data: hist_data },
        ChartKind::BoxPlot => widgets::chart::ChartRenderData::BoxPlot { data: box_data },
        ChartKind::Kde => widgets::chart::ChartRenderData::Kde { data: kde_data },
        ChartKind::Heatmap => widgets::chart::ChartRenderData::Heatmap { data: heatmap_data },
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

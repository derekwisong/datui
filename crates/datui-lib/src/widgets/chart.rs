//! Chart view widget: sidebar (type, x/y columns, options) and chart area.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Chart, Dataset, GraphType, List, ListItem, Paragraph, StatefulWidget,
        Tabs, Widget,
    },
};

use crate::chart_data::{
    format_axis_label, format_x_axis_label, BoxPlotData, HeatmapData, HistogramData, KdeData,
    XAxisTemporalKind,
};
use crate::chart_modal::{ChartFocus, ChartKind, ChartModal, ChartType};
use crate::config::Theme;
use std::collections::HashSet;

const SIDEBAR_WIDTH: u16 = 42;
const LABEL_WIDTH: u16 = 20;
const TAB_HEIGHT: u16 = 3;
const HEATMAP_TITLE_HEIGHT: u16 = 1;
const HEATMAP_X_LABEL_HEIGHT: u16 = 2;

pub enum ChartRenderData<'a> {
    XY {
        series: Option<&'a Vec<Vec<(f64, f64)>>>,
        x_axis_kind: XAxisTemporalKind,
        x_bounds: Option<(f64, f64)>,
    },
    Histogram {
        data: Option<&'a HistogramData>,
    },
    BoxPlot {
        data: Option<&'a BoxPlotData>,
    },
    Kde {
        data: Option<&'a KdeData>,
    },
    Heatmap {
        data: Option<&'a HeatmapData>,
    },
}

/// Renders a single axis column list (shared by X and Y). Display order: selected (remembered) items first.
/// Remembered items use modal_border_active; others use text_primary. Selected row uses REVERSED (like main datatable).
fn render_axis_list(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    list_state: &mut ratatui::widgets::ListState,
    display_items: &[String],
    selected_set: &HashSet<String>,
    is_focused: bool,
    theme: &Theme,
) {
    let active_color = theme.get("modal_border_active");
    let text_primary = theme.get("text_primary");

    let list_items: Vec<ListItem> = display_items
        .iter()
        .map(|name| {
            let style = if selected_set.contains(name) {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(text_primary)
            };
            ListItem::new(Line::from(Span::styled(name.as_str(), style)))
        })
        .collect();

    let list = List::new(list_items).highlight_style(if is_focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    });
    StatefulWidget::render(list, area, buf, list_state);
}

#[allow(clippy::too_many_arguments)]
fn render_filter_group(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    input: &mut crate::widgets::text_input::TextInput,
    list_state: &mut ratatui::widgets::ListState,
    display_items: &[String],
    selected_set: &HashSet<String>,
    is_input_focused: bool,
    is_list_focused: bool,
    theme: &Theme,
    title: &str,
) {
    let border_color = theme.get("modal_border");
    let active_color = theme.get("modal_border_active");
    let group_border = if is_input_focused || is_list_focused {
        active_color
    } else {
        border_color
    };
    let group_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(group_border))
        .title(title);
    let group_inner = group_block.inner(area);
    group_block.render(area, buf);

    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Input row
            Constraint::Length(1), // Divider row
            Constraint::Min(3),    // List
        ])
        .split(group_inner);

    input.set_focused(is_input_focused);
    input.render(inner[0], buf);

    let divider = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(group_border));
    divider.render(inner[1], buf);

    render_axis_list(
        inner[2],
        buf,
        list_state,
        display_items,
        selected_set,
        is_list_focused,
        theme,
    );
}

fn render_number_option(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    label: &str,
    value: &str,
    is_focused: bool,
    theme: &Theme,
) {
    let border_color = theme.get("modal_border");
    let active_color = theme.get("modal_border_active");
    let style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(LABEL_WIDTH), Constraint::Min(1)])
        .split(area);
    Paragraph::new(label).style(style).render(row[0], buf);
    Paragraph::new(value).style(style).render(row[1], buf);
}

/// Renders the chart view: title, left sidebar (chart type, x/y inputs+lists, checkboxes), and chart area (no border).
/// When only x is selected (no chart data), `x_bounds` may be `Some((min, max))` from the x column so the x axis shows the proper range.
pub fn render_chart_view(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartModal,
    theme: &Theme,
    render_data: ChartRenderData<'_>,
) {
    modal.clamp_list_selections_to_filtered();

    let border_color = theme.get("modal_border");
    let active_color = theme.get("modal_border_active");
    let text_primary = theme.get("text_primary");
    let text_secondary = theme.get("text_secondary");

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(TAB_HEIGHT), Constraint::Fill(1)])
        .split(area);

    let tab_titles: Vec<Line> = ChartKind::ALL
        .iter()
        .map(|k| Line::from(Span::raw(k.as_str())))
        .collect();
    let selected_tab = ChartKind::ALL
        .iter()
        .position(|&k| k == modal.chart_kind)
        .unwrap_or(0);
    let tab_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(" Chart ");
    let tab_highlight = if modal.focus == ChartFocus::TabBar {
        Style::default()
            .fg(active_color)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(active_color)
    };
    let tabs = Tabs::new(tab_titles)
        .block(tab_block)
        .select(selected_tab)
        .style(Style::default().fg(border_color))
        .highlight_style(tab_highlight);
    tabs.render(layout[0], buf);

    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(SIDEBAR_WIDTH), Constraint::Fill(1)])
        .split(layout[1]);

    // Sidebar (border, title "Options")
    let sidebar_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(" Options ");
    let sidebar_inner = sidebar_block.inner(main_layout[0]);
    sidebar_block.render(main_layout[0], buf);

    let focus = modal.focus;

    match modal.chart_kind {
        ChartKind::XY => {
            let x_display = modal.x_display_list();
            let y_display = modal.y_display_list();
            let x_selected_set: HashSet<String> = modal.x_column.iter().cloned().collect();
            let y_selected_set: HashSet<String> = modal.y_columns.iter().cloned().collect();

            let sidebar_content = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Plot style label
                    Constraint::Length(1), // Plot style radio grid
                    Constraint::Length(1), // Padding between style and X axis
                    Constraint::Length(1), // X axis label
                    Constraint::Min(4),    // X axis box (input + list)
                    Constraint::Length(1), // Space between X and Y groups
                    Constraint::Length(1), // Y axis label
                    Constraint::Min(4),    // Y axis box (input + list)
                    Constraint::Length(1), // Start y axis at 0
                    Constraint::Length(1), // Log Scale
                    Constraint::Length(1), // Legend
                ])
                .split(sidebar_inner);

            let is_type_focused = focus == ChartFocus::ChartType;
            let type_label_style = if is_type_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new("Plot style:")
                .style(type_label_style)
                .render(sidebar_content[0], buf);

            let type_options = [
                (ChartType::Line, "Line"),
                (ChartType::Scatter, "Scatter"),
                (ChartType::Bar, "Bar"),
            ];
            let type_grid = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(6),
                ])
                .split(sidebar_content[1]);
            for (idx, (t, label)) in type_options.iter().enumerate() {
                let marker = if modal.chart_type == *t { "●" } else { "○" };
                let style = if modal.chart_type == *t || is_type_focused {
                    Style::default().fg(active_color)
                } else {
                    Style::default().fg(border_color)
                };
                let cell = format!("{} {}", marker, label);
                Paragraph::new(Line::from(Span::styled(cell, style))).render(type_grid[idx], buf);
            }

            Paragraph::new("X axis:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[3], buf);

            render_filter_group(
                sidebar_content[4],
                buf,
                &mut modal.x_input,
                &mut modal.x_list_state,
                &x_display,
                &x_selected_set,
                focus == ChartFocus::XInput,
                focus == ChartFocus::XList,
                theme,
                " Filter Columns ",
            );

            Paragraph::new("Y axis:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[6], buf);

            render_filter_group(
                sidebar_content[7],
                buf,
                &mut modal.y_input,
                &mut modal.y_list_state,
                &y_display,
                &y_selected_set,
                focus == ChartFocus::YInput,
                focus == ChartFocus::YList,
                theme,
                " Filter Columns ",
            );

            let y0_row = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(LABEL_WIDTH),
                    Constraint::Length(2),
                    Constraint::Min(1),
                ])
                .split(sidebar_content[8]);
            let is_y0_focused = focus == ChartFocus::YStartsAtZero;
            let y0_label_style = if is_y0_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new("Start y axis at 0:")
                .style(y0_label_style)
                .render(y0_row[0], buf);
            let y0_marker = if modal.y_starts_at_zero { "☑" } else { "☐" };
            let y0_check_style = if is_y0_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new(Line::from(Span::styled(y0_marker, y0_check_style)))
                .render(y0_row[1], buf);

            let log_row = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(LABEL_WIDTH),
                    Constraint::Length(2),
                    Constraint::Min(1),
                ])
                .split(sidebar_content[9]);
            let is_log_focused = focus == ChartFocus::LogScale;
            let log_label_style = if is_log_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new("Log Scale:")
                .style(log_label_style)
                .render(log_row[0], buf);
            let log_marker = if modal.log_scale { "☑" } else { "☐" };
            let log_check_style = if is_log_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new(Line::from(Span::styled(log_marker, log_check_style)))
                .render(log_row[1], buf);

            let legend_row = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Length(LABEL_WIDTH),
                    Constraint::Length(2),
                    Constraint::Min(1),
                ])
                .split(sidebar_content[10]);
            let is_legend_focused = focus == ChartFocus::ShowLegend;
            let legend_label_style = if is_legend_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new("Legend:")
                .style(legend_label_style)
                .render(legend_row[0], buf);
            let legend_marker = if modal.show_legend { "☑" } else { "☐" };
            let legend_check_style = if is_legend_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            Paragraph::new(Line::from(Span::styled(legend_marker, legend_check_style)))
                .render(legend_row[1], buf);
        }
        ChartKind::Histogram => {
            let hist_display = modal.hist_display_list();
            let hist_selected_set: HashSet<String> = modal.hist_column.iter().cloned().collect();
            let sidebar_content = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Column label
                    Constraint::Min(4),    // Column selector
                    Constraint::Length(1), // Bins
                ])
                .split(sidebar_inner);
            Paragraph::new("Value column:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[0], buf);
            render_filter_group(
                sidebar_content[1],
                buf,
                &mut modal.hist_input,
                &mut modal.hist_list_state,
                &hist_display,
                &hist_selected_set,
                focus == ChartFocus::HistInput,
                focus == ChartFocus::HistList,
                theme,
                " Filter Columns ",
            );
            render_number_option(
                sidebar_content[2],
                buf,
                "Bins:",
                &format!("{}", modal.hist_bins),
                focus == ChartFocus::HistBins,
                theme,
            );
        }
        ChartKind::BoxPlot => {
            let box_display = modal.box_display_list();
            let box_selected_set: HashSet<String> = modal.box_column.iter().cloned().collect();
            let sidebar_content = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Min(4)])
                .split(sidebar_inner);
            Paragraph::new("Value column:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[0], buf);
            render_filter_group(
                sidebar_content[1],
                buf,
                &mut modal.box_input,
                &mut modal.box_list_state,
                &box_display,
                &box_selected_set,
                focus == ChartFocus::BoxInput,
                focus == ChartFocus::BoxList,
                theme,
                " Filter Columns ",
            );
        }
        ChartKind::Kde => {
            let kde_display = modal.kde_display_list();
            let kde_selected_set: HashSet<String> = modal.kde_column.iter().cloned().collect();
            let sidebar_content = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Column label
                    Constraint::Min(4),    // Column selector
                    Constraint::Length(1), // Bandwidth
                ])
                .split(sidebar_inner);
            Paragraph::new("Value column:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[0], buf);
            render_filter_group(
                sidebar_content[1],
                buf,
                &mut modal.kde_input,
                &mut modal.kde_list_state,
                &kde_display,
                &kde_selected_set,
                focus == ChartFocus::KdeInput,
                focus == ChartFocus::KdeList,
                theme,
                " Filter Columns ",
            );
            render_number_option(
                sidebar_content[2],
                buf,
                "Bandwidth:",
                &format!("x{:.1}", modal.kde_bandwidth_factor),
                focus == ChartFocus::KdeBandwidth,
                theme,
            );
        }
        ChartKind::Heatmap => {
            let x_display = modal.heatmap_x_display_list();
            let y_display = modal.heatmap_y_display_list();
            let x_selected_set: HashSet<String> = modal.heatmap_x_column.iter().cloned().collect();
            let y_selected_set: HashSet<String> = modal.heatmap_y_column.iter().cloned().collect();
            let sidebar_content = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // X label
                    Constraint::Min(4),    // X selector
                    Constraint::Length(1), // Spacer
                    Constraint::Length(1), // Y label
                    Constraint::Min(4),    // Y selector
                    Constraint::Length(1), // Bins
                ])
                .split(sidebar_inner);
            Paragraph::new("X axis:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[0], buf);
            render_filter_group(
                sidebar_content[1],
                buf,
                &mut modal.heatmap_x_input,
                &mut modal.heatmap_x_list_state,
                &x_display,
                &x_selected_set,
                focus == ChartFocus::HeatmapXInput,
                focus == ChartFocus::HeatmapXList,
                theme,
                " Filter Columns ",
            );
            Paragraph::new("Y axis:")
                .style(Style::default().fg(text_primary))
                .render(sidebar_content[3], buf);
            render_filter_group(
                sidebar_content[4],
                buf,
                &mut modal.heatmap_y_input,
                &mut modal.heatmap_y_list_state,
                &y_display,
                &y_selected_set,
                focus == ChartFocus::HeatmapYInput,
                focus == ChartFocus::HeatmapYList,
                theme,
                " Filter Columns ",
            );
            render_number_option(
                sidebar_content[5],
                buf,
                "Bins:",
                &format!("{}", modal.heatmap_bins),
                focus == ChartFocus::HeatmapBins,
                theme,
            );
        }
    }

    let chart_inner = main_layout[1];
    match render_data {
        ChartRenderData::XY {
            series,
            x_axis_kind,
            x_bounds,
        } => render_xy_chart(
            chart_inner,
            buf,
            modal,
            theme,
            series,
            x_axis_kind,
            x_bounds,
            text_secondary,
        ),
        ChartRenderData::Histogram { data } => {
            render_histogram_chart(chart_inner, buf, theme, data, text_secondary)
        }
        ChartRenderData::BoxPlot { data } => {
            render_box_plot_chart(chart_inner, buf, theme, data, text_secondary)
        }
        ChartRenderData::Kde { data } => {
            render_kde_chart(chart_inner, buf, modal, theme, data, text_secondary)
        }
        ChartRenderData::Heatmap { data } => {
            render_heatmap_chart(chart_inner, buf, theme, data, text_secondary)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_xy_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    chart_data: Option<&Vec<Vec<(f64, f64)>>>,
    x_axis_kind: XAxisTemporalKind,
    x_bounds: Option<(f64, f64)>,
    text_secondary: ratatui::style::Color,
) {
    let chart_type = modal.chart_type;
    let y_starts_at_zero = modal.y_starts_at_zero;
    let log_scale = modal.log_scale;
    let show_legend = modal.show_legend;

    let has_x_selected = modal.effective_x_column().is_some();
    let has_data = chart_data
        .map(|d| d.iter().any(|s| !s.is_empty()))
        .unwrap_or(false);

    if has_x_selected && !has_data {
        let x_name = modal
            .effective_x_column()
            .map(|s| s.as_str())
            .unwrap_or("X");
        let y_names: String = modal.effective_y_columns().join(", ");
        let axis_label_style = Style::default().fg(theme.get("text_primary"));
        const PLACEHOLDER_MIN: f64 = 0.0;
        const PLACEHOLDER_MAX: f64 = 1.0;
        let (x_min, x_max) = x_bounds.unwrap_or((PLACEHOLDER_MIN, PLACEHOLDER_MAX));
        let format_x = |v: f64| format_x_axis_label(v, x_axis_kind);
        let x_labels = vec![
            Span::styled(format_x(x_min), axis_label_style),
            Span::styled(format_x((x_min + x_max) / 2.0), axis_label_style),
            Span::styled(format_x(x_max), axis_label_style),
        ];
        let y_labels = vec![
            Span::styled(format_axis_label(PLACEHOLDER_MIN), axis_label_style),
            Span::styled(
                format_axis_label((PLACEHOLDER_MIN + PLACEHOLDER_MAX) / 2.0),
                axis_label_style,
            ),
            Span::styled(format_axis_label(PLACEHOLDER_MAX), axis_label_style),
        ];
        let x_axis = Axis::default()
            .title(x_name)
            .bounds([x_min, x_max])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(x_labels);
        let y_axis = Axis::default()
            .title(y_names)
            .bounds([PLACEHOLDER_MIN, PLACEHOLDER_MAX])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(y_labels);
        let empty_dataset = Dataset::default()
            .name("")
            .data(&[])
            .graph_type(match chart_type {
                ChartType::Line => GraphType::Line,
                ChartType::Scatter => GraphType::Scatter,
                ChartType::Bar => GraphType::Bar,
            });
        let mut chart = Chart::new(vec![empty_dataset])
            .x_axis(x_axis)
            .y_axis(y_axis);
        if show_legend {
            chart = chart.legend_position(Some(ratatui::widgets::LegendPosition::TopRight));
        } else {
            chart = chart.legend_position(None);
        }
        chart.render(area, buf);
        return;
    }

    if has_data {
        let data = chart_data.unwrap();
        let y_columns = modal.effective_y_columns();
        let graph_type = match chart_type {
            ChartType::Line => GraphType::Line,
            ChartType::Scatter => GraphType::Scatter,
            ChartType::Bar => GraphType::Bar,
        };
        let marker = match chart_type {
            ChartType::Line => symbols::Marker::Braille,
            ChartType::Scatter => symbols::Marker::Dot,
            ChartType::Bar => symbols::Marker::HalfBlock,
        };

        let series_colors = [
            "chart_series_color_1",
            "chart_series_color_2",
            "chart_series_color_3",
            "chart_series_color_4",
            "chart_series_color_5",
            "chart_series_color_6",
            "chart_series_color_7",
        ];

        let mut all_x_min = f64::INFINITY;
        let mut all_x_max = f64::NEG_INFINITY;
        let mut all_y_min = f64::INFINITY;
        let mut all_y_max = f64::NEG_INFINITY;

        let plot_points_with_names: Vec<(String, Vec<(f64, f64)>)> = data
            .iter()
            .zip(y_columns.iter())
            .filter_map(|(points, name)| {
                if points.is_empty() {
                    return None;
                }
                let pts = if log_scale {
                    points
                        .iter()
                        .map(|&(x, y)| (x, y.max(0.0).ln_1p()))
                        .collect()
                } else {
                    points.clone()
                };
                Some((name.clone(), pts))
            })
            .collect();

        let names_and_points: Vec<(&str, &[(f64, f64)])> = plot_points_with_names
            .iter()
            .map(|(name, pts)| (name.as_str(), pts.as_slice()))
            .collect();

        for (_, points) in &names_and_points {
            let (x_min, x_max) = points
                .iter()
                .map(|&(x, _)| x)
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), x| {
                    (a.min(x), b.max(x))
                });
            let (y_min, y_max) = points
                .iter()
                .map(|&(_, y)| y)
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), y| {
                    (a.min(y), b.max(y))
                });
            all_x_min = all_x_min.min(x_min);
            all_x_max = all_x_max.max(x_max);
            all_y_min = all_y_min.min(y_min);
            all_y_max = all_y_max.max(y_max);
        }

        let datasets: Vec<Dataset> = names_and_points
            .iter()
            .enumerate()
            .map(|(i, (name, points))| {
                let color_key = series_colors
                    .get(i)
                    .copied()
                    .unwrap_or("primary_chart_series_color");
                let style = Style::default().fg(theme.get(color_key));
                Dataset::default()
                    .name(*name)
                    .marker(marker)
                    .graph_type(graph_type)
                    .style(style)
                    .data(points)
            })
            .collect();

        if datasets.is_empty() {
            Paragraph::new("No valid data points")
                .style(Style::default().fg(text_secondary))
                .centered()
                .render(area, buf);
            return;
        }

        let y_min_bounds = if chart_type == ChartType::Bar {
            0.0_f64.min(all_y_min)
        } else if y_starts_at_zero {
            0.0
        } else {
            all_y_min
        };
        let y_max_bounds = if all_y_max > y_min_bounds {
            all_y_max
        } else {
            y_min_bounds + 1.0
        };
        let x_min_bounds = if all_x_max > all_x_min {
            all_x_min
        } else {
            all_x_min - 0.5
        };
        let x_max_bounds = if all_x_max > all_x_min {
            all_x_max
        } else {
            all_x_min + 0.5
        };

        let axis_label_style = Style::default().fg(theme.get("text_primary"));
        let format_x = |v: f64| format_x_axis_label(v, x_axis_kind);
        let x_labels = vec![
            Span::styled(format_x(x_min_bounds), axis_label_style),
            Span::styled(
                format_x((x_min_bounds + x_max_bounds) / 2.0),
                axis_label_style,
            ),
            Span::styled(format_x(x_max_bounds), axis_label_style),
        ];
        let format_y_label = |log_v: f64| {
            let v = if log_scale { log_v.exp_m1() } else { log_v };
            format_axis_label(v)
        };
        let y_labels = vec![
            Span::styled(format_y_label(y_min_bounds), axis_label_style),
            Span::styled(
                format_y_label((y_min_bounds + y_max_bounds) / 2.0),
                axis_label_style,
            ),
            Span::styled(format_y_label(y_max_bounds), axis_label_style),
        ];

        let x_axis_title = modal.effective_x_column().map(|s| s.as_str()).unwrap_or("");
        let y_axis_title = y_columns.join(", ");
        let x_axis = Axis::default()
            .title(x_axis_title)
            .bounds([x_min_bounds, x_max_bounds])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(x_labels);
        let y_axis = Axis::default()
            .title(y_axis_title)
            .bounds([y_min_bounds, y_max_bounds])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(y_labels);

        let mut chart = Chart::new(datasets).x_axis(x_axis).y_axis(y_axis);
        if show_legend {
            chart = chart.legend_position(Some(ratatui::widgets::LegendPosition::TopRight));
        } else {
            chart = chart.legend_position(None);
        }
        chart.render(area, buf);
    } else {
        Paragraph::new("Select X and Y columns in sidebar — Tab to change focus")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
    }
}

fn render_histogram_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: Option<&HistogramData>,
    text_secondary: ratatui::style::Color,
) {
    let Some(data) = data else {
        Paragraph::new("Select a column for histogram")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    };
    if data.bins.is_empty() {
        Paragraph::new("No data for histogram")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let points: Vec<(f64, f64)> = data.bins.iter().map(|b| (b.center, b.count)).collect();
    let series = [points];

    let x_min_bounds = data.x_min;
    let x_max_bounds = if data.x_max > data.x_min {
        data.x_max
    } else {
        data.x_min + 1.0
    };
    let y_min_bounds = 0.0;
    let y_max_bounds = if data.max_count > 0.0 {
        data.max_count
    } else {
        1.0
    };

    let axis_label_style = Style::default().fg(theme.get("text_primary"));
    let x_labels = vec![
        Span::styled(format_axis_label(x_min_bounds), axis_label_style),
        Span::styled(
            format_axis_label((x_min_bounds + x_max_bounds) / 2.0),
            axis_label_style,
        ),
        Span::styled(format_axis_label(x_max_bounds), axis_label_style),
    ];
    let y_labels = vec![
        Span::styled(format_axis_label(y_min_bounds), axis_label_style),
        Span::styled(
            format_axis_label((y_min_bounds + y_max_bounds) / 2.0),
            axis_label_style,
        ),
        Span::styled(format_axis_label(y_max_bounds), axis_label_style),
    ];

    let x_axis = Axis::default()
        .title(data.column.as_str())
        .bounds([x_min_bounds, x_max_bounds])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(x_labels);
    let y_axis = Axis::default()
        .title("Count")
        .bounds([y_min_bounds, y_max_bounds])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(y_labels);

    let style = Style::default().fg(theme.get("primary_chart_series_color"));
    let dataset = Dataset::default()
        .name("")
        .marker(symbols::Marker::HalfBlock)
        .graph_type(GraphType::Bar)
        .style(style)
        .data(&series[0]);

    Chart::new(vec![dataset])
        .x_axis(x_axis)
        .y_axis(y_axis)
        .render(area, buf);
}

fn render_kde_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    data: Option<&KdeData>,
    text_secondary: ratatui::style::Color,
) {
    let Some(data) = data else {
        Paragraph::new("Select a column for KDE")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    };
    if data.series.is_empty() {
        Paragraph::new("No data for KDE")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let series_colors = [
        "chart_series_color_1",
        "chart_series_color_2",
        "chart_series_color_3",
        "chart_series_color_4",
        "chart_series_color_5",
        "chart_series_color_6",
        "chart_series_color_7",
    ];

    let datasets: Vec<Dataset> = data
        .series
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let color_key = series_colors
                .get(i)
                .copied()
                .unwrap_or("primary_chart_series_color");
            let style = Style::default().fg(theme.get(color_key));
            Dataset::default()
                .name(s.name.as_str())
                .graph_type(GraphType::Line)
                .marker(symbols::Marker::Braille)
                .style(style)
                .data(&s.points)
        })
        .collect();

    let x_axis = Axis::default()
        .title("Value")
        .bounds([data.x_min, data.x_max])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(vec![
            Span::styled(
                format_axis_label(data.x_min),
                Style::default().fg(theme.get("text_primary")),
            ),
            Span::styled(
                format_axis_label((data.x_min + data.x_max) / 2.0),
                Style::default().fg(theme.get("text_primary")),
            ),
            Span::styled(
                format_axis_label(data.x_max),
                Style::default().fg(theme.get("text_primary")),
            ),
        ]);
    let y_axis = Axis::default()
        .title("Density")
        .bounds([0.0, data.y_max])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(vec![
            Span::styled(
                format_axis_label(0.0),
                Style::default().fg(theme.get("text_primary")),
            ),
            Span::styled(
                format_axis_label(data.y_max / 2.0),
                Style::default().fg(theme.get("text_primary")),
            ),
            Span::styled(
                format_axis_label(data.y_max),
                Style::default().fg(theme.get("text_primary")),
            ),
        ]);

    let mut chart = Chart::new(datasets).x_axis(x_axis).y_axis(y_axis);
    if modal.show_legend {
        chart = chart.legend_position(Some(ratatui::widgets::LegendPosition::TopRight));
    } else {
        chart = chart.legend_position(None);
    }
    chart.render(area, buf);
}

fn render_box_plot_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: Option<&BoxPlotData>,
    text_secondary: ratatui::style::Color,
) {
    let Some(data) = data else {
        Paragraph::new("Select a column for box plot")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    };
    if data.stats.is_empty() {
        Paragraph::new("No data for box plot")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let series_colors = [
        "chart_series_color_1",
        "chart_series_color_2",
        "chart_series_color_3",
        "chart_series_color_4",
        "chart_series_color_5",
        "chart_series_color_6",
        "chart_series_color_7",
    ];
    let mut segments: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut segment_styles: Vec<Style> = Vec::new();
    let box_half = 0.3;
    let cap_half = 0.2;
    for (i, stat) in data.stats.iter().enumerate() {
        let x = i as f64;
        let color_key = series_colors
            .get(i)
            .copied()
            .unwrap_or("primary_chart_series_color");
        let style = Style::default().fg(theme.get(color_key));
        segments.push(vec![
            (x - box_half, stat.q1),
            (x + box_half, stat.q1),
            (x + box_half, stat.q3),
            (x - box_half, stat.q3),
            (x - box_half, stat.q1),
        ]);
        segment_styles.push(style);
        segments.push(vec![
            (x - box_half, stat.median),
            (x + box_half, stat.median),
        ]);
        segment_styles.push(style);
        segments.push(vec![(x, stat.min), (x, stat.q1)]);
        segment_styles.push(style);
        segments.push(vec![(x, stat.q3), (x, stat.max)]);
        segment_styles.push(style);
        segments.push(vec![(x - cap_half, stat.min), (x + cap_half, stat.min)]);
        segment_styles.push(style);
        segments.push(vec![(x - cap_half, stat.max), (x + cap_half, stat.max)]);
        segment_styles.push(style);
    }

    let datasets: Vec<Dataset> = segments
        .iter()
        .zip(segment_styles.iter())
        .map(|(points, style)| {
            Dataset::default()
                .name("")
                .graph_type(GraphType::Line)
                .style(*style)
                .data(points)
        })
        .collect();

    let x_min_bounds = -0.5;
    let x_max_bounds = (data.stats.len() as f64 - 1.0).max(0.0) + 0.5;
    let axis_label_style = Style::default().fg(theme.get("text_primary"));
    let x_labels: Vec<Span> = data
        .stats
        .iter()
        .map(|s| Span::styled(s.name.as_str(), axis_label_style))
        .collect();
    let y_labels = vec![
        Span::styled(format_axis_label(data.y_min), axis_label_style),
        Span::styled(
            format_axis_label((data.y_min + data.y_max) / 2.0),
            axis_label_style,
        ),
        Span::styled(format_axis_label(data.y_max), axis_label_style),
    ];

    let x_axis = Axis::default()
        .title("Columns")
        .bounds([x_min_bounds, x_max_bounds])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(x_labels);
    let y_axis = Axis::default()
        .title("Value")
        .bounds([data.y_min, data.y_max])
        .style(Style::default().fg(theme.get("text_primary")))
        .labels(y_labels);

    Chart::new(datasets)
        .x_axis(x_axis)
        .y_axis(y_axis)
        .render(area, buf);
}

fn render_heatmap_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: Option<&HeatmapData>,
    text_secondary: ratatui::style::Color,
) {
    let Some(data) = data else {
        Paragraph::new("Select X and Y columns for heatmap")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    };
    if data.counts.is_empty() || data.max_count <= 0.0 {
        Paragraph::new("No data for heatmap")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(HEATMAP_TITLE_HEIGHT),
            Constraint::Min(1),
            Constraint::Length(HEATMAP_X_LABEL_HEIGHT),
        ])
        .split(area);
    let title = format!("{} vs {}", data.x_column, data.y_column);
    Paragraph::new(title)
        .style(Style::default().fg(theme.get("text_primary")))
        .render(layout[0], buf);

    let y_labels = [
        format_axis_label(data.y_max),
        format_axis_label((data.y_min + data.y_max) / 2.0),
        format_axis_label(data.y_min),
    ];
    let y_label_width = y_labels.iter().map(|s| s.len()).max().unwrap_or(1) as u16;
    let y_label_width = y_label_width.clamp(4, 12);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(y_label_width + 1), Constraint::Min(1)])
        .split(layout[1]);
    let label_area = body[0];
    let plot_area = body[1];
    if plot_area.width == 0 || plot_area.height == 0 {
        return;
    }

    let label_style = Style::default().fg(theme.get("text_primary"));
    if label_area.height >= 3 {
        buf.set_string(label_area.x, label_area.y, &y_labels[0], label_style);
        let mid_y = label_area.y + label_area.height / 2;
        buf.set_string(label_area.x, mid_y, &y_labels[1], label_style);
        let bottom_y = label_area.y + label_area.height.saturating_sub(1);
        buf.set_string(label_area.x, bottom_y, &y_labels[2], label_style);
    }

    let intensity_chars: Vec<char> = " .:-=+*#%@".chars().collect();
    for row in 0..plot_area.height {
        for col in 0..plot_area.width {
            let max_x_bin = data.x_bins.saturating_sub(1) as f64;
            let max_y_bin = data.y_bins.saturating_sub(1) as f64;
            let x_bin = ((col as f64 / plot_area.width as f64) * data.x_bins as f64)
                .floor()
                .clamp(0.0, max_x_bin) as usize;
            let y_bin_raw = ((row as f64 / plot_area.height as f64) * data.y_bins as f64).floor();
            let y_bin = data
                .y_bins
                .saturating_sub(1)
                .saturating_sub(y_bin_raw.clamp(0.0, max_y_bin) as usize);
            let count = data.counts[y_bin][x_bin];
            let level = ((count / data.max_count) * (intensity_chars.len() as f64 - 1.0))
                .round()
                .clamp(0.0, intensity_chars.len() as f64 - 1.0) as usize;
            let ch = intensity_chars[level];
            let cell = &mut buf[(plot_area.x + col, plot_area.y + row)];
            let symbol = ch.to_string();
            cell.set_symbol(&symbol);
            cell.set_style(Style::default().fg(theme.get("primary_chart_series_color")));
        }
    }

    let x_labels = [
        format_axis_label(data.x_min),
        format_axis_label((data.x_min + data.x_max) / 2.0),
        format_axis_label(data.x_max),
    ];
    let x_label_area = layout[2];
    let mid_x = x_label_area.x + x_label_area.width / 2;
    let right_x = x_label_area.x + x_label_area.width.saturating_sub(1);
    buf.set_string(x_label_area.x, x_label_area.y, &x_labels[0], label_style);
    buf.set_string(
        mid_x.saturating_sub((x_labels[1].len() / 2) as u16),
        x_label_area.y,
        &x_labels[1],
        label_style,
    );
    buf.set_string(
        right_x.saturating_sub(x_labels[2].len() as u16),
        x_label_area.y,
        &x_labels[2],
        label_style,
    );
    let x_title = format!("X: {}", data.x_column);
    let y_title = format!("Y: {}", data.y_column);
    if x_label_area.height > 1 {
        buf.set_string(x_label_area.x, x_label_area.y + 1, &x_title, label_style);
        buf.set_string(
            x_label_area.x + x_label_area.width.saturating_sub(y_title.len() as u16),
            x_label_area.y + 1,
            &y_title,
            label_style,
        );
    }
}

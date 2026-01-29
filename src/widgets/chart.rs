//! Chart view widget: sidebar (type, x/y columns, options) and chart area.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, Borders, Chart, Dataset, GraphType, List, ListItem, Paragraph, StatefulWidget,
        Widget,
    },
};

use crate::chart_data::XAxisTemporalKind;
use crate::chart_modal::{ChartFocus, ChartModal, ChartType};
use crate::config::Theme;

const SIDEBAR_WIDTH: u16 = 42;
const LABEL_WIDTH: u16 = 20;

/// Renders the chart view: title, left sidebar (chart type, x/y inputs+lists, checkboxes), and chart area (no border).
pub fn render_chart_view(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartModal,
    theme: &Theme,
    chart_data: Option<&Vec<Vec<(f64, f64)>>>,
    x_axis_kind: XAxisTemporalKind,
) {
    modal.clamp_list_selections_to_filtered();

    let border_color = theme.get("modal_border");
    let active_color = theme.get("modal_border_active");
    let text_primary = theme.get("text_primary");
    let text_secondary = theme.get("text_secondary");

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Fill(1)])
        .split(area);

    // Title row
    Paragraph::new("Chart")
        .style(
            Style::default()
                .fg(theme.get("table_header"))
                .bg(theme.get("controls_bg")),
        )
        .render(layout[0], buf);

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

    let chart_type = modal.chart_type;
    let y_starts_at_zero = modal.y_starts_at_zero;
    let log_scale = modal.log_scale;
    let show_legend = modal.show_legend;
    let focus = modal.focus;
    let x_filtered = modal.x_filtered();
    let y_filtered = modal.y_filtered();

    // Sidebar content: one row after another, with padding between chart type and X, and between X and Y groups
    let sidebar_content = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Chart type label
            Constraint::Length(1), // Chart type radio grid
            Constraint::Length(1), // Padding between chart type and X axis
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

    // Chart type: label on first row
    let is_type_focused = focus == ChartFocus::ChartType;
    let type_label_style = if is_type_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    Paragraph::new("Chart type:")
        .style(type_label_style)
        .render(sidebar_content[0], buf);

    // Chart type: radio buttons in a grid on the line below (3 items in a row)
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

    // X axis label (normal text color)
    Paragraph::new("X axis:")
        .style(Style::default().fg(text_primary))
        .render(sidebar_content[3], buf);

    // X axis: one box "Filter Columns" (input + divider + list); border lights when input or list focused
    let x_box_area = sidebar_content[4];
    let x_group_border = match focus {
        ChartFocus::XInput | ChartFocus::XList => active_color,
        _ => border_color,
    };
    let x_group_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(x_group_border))
        .title(" Filter Columns ");
    let x_group_inner = x_group_block.inner(x_box_area);
    x_group_block.render(x_box_area, buf);

    let x_inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Input row
            Constraint::Length(1), // Divider row (border between input and list)
            Constraint::Min(3),    // List
        ])
        .split(x_group_inner);

    modal.x_input.set_focused(focus == ChartFocus::XInput);
    modal.x_input.render(x_inner[0], buf);

    // Border between input and list
    let x_divider = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(x_group_border));
    x_divider.render(x_inner[1], buf);

    let x_list_items: Vec<ListItem> = x_filtered
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let selected = modal.x_list_state.selected() == Some(i);
            let style = if selected && focus == ChartFocus::XList {
                Style::default().fg(active_color)
            } else if selected {
                Style::default().fg(border_color)
            } else {
                Style::default().fg(text_secondary)
            };
            ListItem::new(Line::from(Span::styled(name.as_str(), style)))
        })
        .collect();
    let x_list = List::new(x_list_items);
    let mut x_list_state = std::mem::take(&mut modal.x_list_state);
    StatefulWidget::render(x_list, x_inner[2], buf, &mut x_list_state);
    modal.x_list_state = x_list_state;

    // Y axis label (normal text color)
    Paragraph::new("Y axis:")
        .style(Style::default().fg(text_primary))
        .render(sidebar_content[6], buf);

    // Y axis: one box "Filter Columns" (input + divider + list); border lights when input or list focused
    let y_box_area = sidebar_content[7];
    let y_group_border = match focus {
        ChartFocus::YInput | ChartFocus::YList => active_color,
        _ => border_color,
    };
    let y_group_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(y_group_border))
        .title(" Filter Columns ");
    let y_group_inner = y_group_block.inner(y_box_area);
    y_group_block.render(y_box_area, buf);

    let y_inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Input row
            Constraint::Length(1), // Divider row (border between input and list)
            Constraint::Min(3),    // List
        ])
        .split(y_group_inner);

    modal.y_input.set_focused(focus == ChartFocus::YInput);
    modal.y_input.render(y_inner[0], buf);

    // Border between input and list
    let y_divider = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(y_group_border));
    y_divider.render(y_inner[1], buf);

    let y_list_items: Vec<ListItem> = y_filtered
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let selected = modal.y_list_state.selected() == Some(i);
            let style = if selected && focus == ChartFocus::YList {
                Style::default().fg(active_color)
            } else if selected {
                Style::default().fg(border_color)
            } else {
                Style::default().fg(text_secondary)
            };
            ListItem::new(Line::from(Span::styled(name.as_str(), style)))
        })
        .collect();
    let y_list = List::new(y_list_items);
    let mut y_list_state = std::mem::take(&mut modal.y_list_state);
    StatefulWidget::render(y_list, y_inner[2], buf, &mut y_list_state);
    modal.y_list_state = y_list_state;

    // Start y axis at 0: label + checkbox
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
    let y0_marker = if y_starts_at_zero { "☑" } else { "☐" };
    let y0_check_style = if is_y0_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    Paragraph::new(Line::from(Span::styled(y0_marker, y0_check_style))).render(y0_row[1], buf);

    // Log Scale: label + checkbox
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
    let log_marker = if log_scale { "☑" } else { "☐" };
    let log_check_style = if is_log_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    Paragraph::new(Line::from(Span::styled(log_marker, log_check_style))).render(log_row[1], buf);

    // Legend: label + checkbox
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
    let legend_marker = if show_legend { "☑" } else { "☐" };
    let legend_check_style = if is_legend_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    Paragraph::new(Line::from(Span::styled(legend_marker, legend_check_style)))
        .render(legend_row[1], buf);

    // Chart area: no border, no title
    let chart_inner = main_layout[1];

    let has_data = chart_data
        .map(|d| d.iter().any(|s| !s.is_empty()))
        .unwrap_or(false);

    if has_data {
        let data = chart_data.unwrap();
        let y_columns = &modal.y_columns;
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
                .render(chart_inner, buf);
            return;
        }

        // Bar chart draws from (x, 0); ensure 0 is in y bounds
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

        let x_axis = Axis::default()
            .bounds([x_min_bounds, x_max_bounds])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(x_labels);
        let y_axis = Axis::default()
            .bounds([y_min_bounds, y_max_bounds])
            .style(Style::default().fg(theme.get("text_primary")))
            .labels(y_labels);

        let mut chart = Chart::new(datasets).x_axis(x_axis).y_axis(y_axis);
        if show_legend {
            chart = chart.legend_position(Some(ratatui::widgets::LegendPosition::TopRight));
        } else {
            chart = chart.legend_position(None);
        }
        chart.render(chart_inner, buf);
    } else {
        Paragraph::new("Select X and Y columns in sidebar — Tab to change focus")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(chart_inner, buf);
    }
}

fn format_axis_label(v: f64) -> String {
    if v.abs() >= 1e6 || (v.abs() < 1e-2 && v != 0.0) {
        format!("{:.2e}", v)
    } else {
        format!("{:.2}", v)
    }
}

/// Format x-axis label: dates/datetimes/times when kind is temporal, else numeric.
fn format_x_axis_label(v: f64, kind: XAxisTemporalKind) -> String {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};

    match kind {
        XAxisTemporalKind::Numeric => format_axis_label(v),
        XAxisTemporalKind::Date => {
            const UNIX_EPOCH_CE_DAYS: i32 = 719_163;
            let days = v.trunc() as i32;
            match NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_CE_DAYS.saturating_add(days)) {
                Some(d) => d.format("%Y-%m-%d").to_string(),
                None => format_axis_label(v),
            }
        }
        XAxisTemporalKind::DatetimeUs => DateTime::from_timestamp_micros(v.trunc() as i64)
            .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| format_axis_label(v)),
        XAxisTemporalKind::DatetimeMs => DateTime::from_timestamp_millis(v.trunc() as i64)
            .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| format_axis_label(v)),
        XAxisTemporalKind::DatetimeNs => {
            let millis = (v.trunc() as i64) / 1_000_000;
            DateTime::from_timestamp_millis(millis)
                .map(|dt: DateTime<Utc>| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| format_axis_label(v))
        }
        XAxisTemporalKind::Time => {
            let nsecs = v.trunc() as u64;
            let secs = (nsecs / 1_000_000_000) as u32;
            let subsec = (nsecs % 1_000_000_000) as u32;
            match NaiveTime::from_num_seconds_from_midnight_opt(secs, subsec) {
                Some(t) => t.format("%H:%M:%S").to_string(),
                None => format_axis_label(v),
            }
        }
    }
}

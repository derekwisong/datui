//! Chart view widget: the tab line, the Options sidebar (one Surface of
//! FormRows, column rows edited through the shared Picker), and the chart
//! canvas.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Chart, Dataset, GraphType, Paragraph, Widget, Wrap},
};

use crate::chart_data::{
    BarData, BoxPlotData, HeatmapData, HistogramData, KdeData, XAxisTemporalKind,
    format_axis_label, format_bar_value, format_x_axis_label, segments,
};
use crate::chart_modal::{ChartFocus, ChartKind, ChartModal, ChartType};
use crate::config::Theme;
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, Picker, Surface};

const SIDEBAR_WIDTH: u16 = 42;
/// Where the value column starts, past the rail gutter: the longest label,
/// "Y from zero:", plus two cells of air.
const LABEL_WIDTH: u16 = 14;
const HEATMAP_TITLE_HEIGHT: u16 = 1;
const HEATMAP_X_LABEL_HEIGHT: u16 = 2;

/// What the chart area shows: the plot, the notes under it about its input, or the
/// reason it could not be prepared.
pub struct ChartView<'a> {
    pub data: ChartRenderData<'a>,
    /// One line each, dimmed under the plot: a sample, values a range left out.
    pub notes: Vec<String>,
    /// Preparing the selection failed; shown in place of an empty plot.
    pub error: Option<&'a str>,
}

pub enum ChartRenderData<'a> {
    XY {
        series: Option<&'a Vec<Vec<(f64, f64)>>>,
        /// Per series, where its line starts again after a gap.
        breaks: Option<&'a Vec<Vec<usize>>>,
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
    Bar {
        data: Option<&'a BarData>,
    },
}

fn row_label(focus: ChartFocus) -> &'static str {
    match focus {
        ChartFocus::Style => "Style:",
        ChartFocus::XColumn | ChartFocus::HeatmapX => "X axis:",
        ChartFocus::YColumns => "Y series:",
        ChartFocus::HeatmapY => "Y axis:",
        ChartFocus::YStartsAtZero => "Y from zero:",
        ChartFocus::LogScale => "Log scale:",
        ChartFocus::ShowLegend => "Legend:",
        ChartFocus::Column => "Column:",
        ChartFocus::Bins => "Bins:",
        ChartFocus::Bandwidth => "Bandwidth:",
        ChartFocus::Range => "Range:",
        ChartFocus::Category => "Category:",
        ChartFocus::Value => "Value:",
        ChartFocus::Order => "Order:",
        ChartFocus::LimitRows => "Sample size:",
    }
}

fn echo_or_placeholder<'a>(value: &'a str, placeholder: &'a str) -> FormValue<'a> {
    if value.is_empty() {
        FormValue::Placeholder(placeholder)
    } else {
        FormValue::Choice(value)
    }
}

/// The tab line: every chart kind, the active one on the accent. It is state,
/// not a focus stop — 1-6 and [ ] switch from anywhere.
fn render_tab_line(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let mut spans = vec![Span::raw(" ")];
    for (i, kind) in ChartKind::ALL.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled(
                format!(" {} ", g.rule),
                Style::default().fg(ctx.dimmed),
            ));
        }
        let style = if *kind == modal.chart_kind {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_secondary)
        };
        spans.push(Span::styled(kind.as_str(), style));
    }
    Paragraph::new(Line::from(spans)).render(area, buf);
}

/// The Options sidebar: one Surface, a FormRow per option of the active chart
/// kind, and the shared Picker below the rows while a column row is edited.
fn render_sidebar(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartModal,
    ctx: &RenderContext,
) {
    let content = Surface::new("Options").render(area, buf, ctx);
    if content.height < 2 || content.width < 4 {
        return;
    }

    let rows = modal.row_order();
    let mut y = content.y;
    let bottom = content.y + content.height;
    for &row in rows {
        if y >= bottom {
            break;
        }
        let joined;
        let number;
        let value = match row {
            ChartFocus::Style => FormValue::Choice(modal.chart_type.as_str()),
            ChartFocus::XColumn => {
                echo_or_placeholder(modal.x_column.as_deref().unwrap_or(""), "none")
            }
            ChartFocus::YColumns => {
                joined = modal.y_columns.join(", ");
                echo_or_placeholder(&joined, "none")
            }
            ChartFocus::YStartsAtZero => FormValue::Toggle(modal.y_starts_at_zero),
            ChartFocus::LogScale => FormValue::Toggle(modal.log_scale),
            ChartFocus::ShowLegend => FormValue::Toggle(modal.show_legend),
            ChartFocus::Column => {
                let column = match modal.chart_kind {
                    ChartKind::Histogram => modal.hist_column.as_deref(),
                    ChartKind::BoxPlot => modal.box_column.as_deref(),
                    ChartKind::Kde => modal.kde_column.as_deref(),
                    _ => None,
                };
                echo_or_placeholder(column.unwrap_or(""), "none")
            }
            ChartFocus::HeatmapX => {
                echo_or_placeholder(modal.heatmap_x_column.as_deref().unwrap_or(""), "none")
            }
            ChartFocus::HeatmapY => {
                echo_or_placeholder(modal.heatmap_y_column.as_deref().unwrap_or(""), "none")
            }
            ChartFocus::Bins => {
                number = match modal.chart_kind {
                    ChartKind::Heatmap => modal.heatmap_bins.to_string(),
                    _ => modal.hist_bins.to_string(),
                };
                FormValue::Choice(&number)
            }
            ChartFocus::Bandwidth => {
                number = format!("x{:.1}", modal.kde_bandwidth_factor);
                FormValue::Choice(&number)
            }
            ChartFocus::Range => FormValue::Choice(modal.value_range.label()),
            ChartFocus::Category => {
                echo_or_placeholder(modal.bar_category.as_deref().unwrap_or(""), "none")
            }
            ChartFocus::Value => {
                echo_or_placeholder(modal.bar_value.as_deref().unwrap_or(""), "none")
            }
            ChartFocus::Order => FormValue::Choice(modal.bar_order.label()),
            ChartFocus::LimitRows => {
                number = modal.row_limit_display();
                FormValue::Choice(&number)
            }
        };
        FormRow {
            label: row_label(row),
            value,
            focused: modal.focus == row,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                y,
                height: 1,
                ..content
            },
            buf,
            ctx,
        );
        y += 1;
    }

    // The focused row's Picker drops in below the rows; the selection carries
    // the rail while the list is up, and its cursor previews on the canvas.
    if let Some(state) = &modal.picker {
        let picker_y = y + 1;
        if picker_y < bottom {
            let picker_area = Rect {
                x: content.x + 2,
                y: picker_y,
                width: content.width.saturating_sub(2),
                height: bottom - picker_y,
            };
            let mut picker = Picker::from_state(state, true);
            if modal.is_multi_row(modal.focus) {
                let marks = state
                    .filtered()
                    .into_iter()
                    .map(|(_, item)| modal.is_marked(item))
                    .collect();
                picker = picker.marks(marks);
            }
            picker.render(picker_area, buf, ctx);
        }
    }
}

/// Renders the chart view: the tab line, the Options sidebar, and the chart
/// area (no border). When only x is selected (no chart data), `x_bounds` may
/// be `Some((min, max))` from the x column so the x axis shows the proper range.
pub fn render_chart_view(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartModal,
    theme: &Theme,
    ctx: &RenderContext,
    view: ChartView<'_>,
) {
    let text_secondary = theme.get("text_secondary");

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Fill(1)])
        .split(area);
    render_tab_line(layout[0], buf, modal, ctx);

    // The sidebar caps its share of the width, so a narrow terminal still
    // keeps a canvas.
    let sidebar_width = SIDEBAR_WIDTH.min(layout[1].width / 2);
    let main_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(sidebar_width), Constraint::Fill(1)])
        .split(layout[1]);
    render_sidebar(main_layout[0], buf, modal, ctx);

    let mut chart_inner = main_layout[1];
    if let Some(message) = view.error {
        Paragraph::new(message)
            .style(Style::default().fg(ctx.error))
            .wrap(Wrap { trim: true })
            .centered()
            .render(chart_inner, buf);
        return;
    }
    // The notes sit under the plot, where the axis ends, wrapped rather than cut on a
    // narrow canvas: the plot gives up the rows, never the notes, so the chart cannot
    // look whole when it is not.
    let lines: Vec<Line> = view
        .notes
        .iter()
        .map(|note| Line::styled(note.as_str(), Style::default().fg(ctx.dimmed)))
        .collect();
    let wrapped: usize = lines
        .iter()
        .map(|line| crate::render::home_view::wrapped_rows(line, chart_inner.width as usize))
        .sum();
    let note_rows = (wrapped as u16).min(chart_inner.height / 2);
    if note_rows > 0 {
        let [plot, notes] = Layout::vertical([Constraint::Fill(1), Constraint::Length(note_rows)])
            .areas(chart_inner);
        Paragraph::new(lines)
            .right_aligned()
            .wrap(Wrap { trim: true })
            .render(notes, buf);
        chart_inner = plot;
    }
    match view.data {
        ChartRenderData::XY {
            series,
            breaks,
            x_axis_kind,
            x_bounds,
        } => render_xy_chart(
            chart_inner,
            buf,
            modal,
            theme,
            XYData {
                series,
                breaks,
                x_axis_kind,
                x_bounds,
            },
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
        ChartRenderData::Bar { data } => {
            let picked =
                modal.effective_bar_category().is_some() && modal.effective_bar_value().is_some();
            render_bar_chart(chart_inner, buf, ctx, data, picked)
        }
    }
}

/// One horizontal bar per category: the label, the value, then the bar, from a zero
/// line that sits at the left edge unless some value is negative. The bars that fit
/// are drawn and the rest are counted on a `+ 212 more` chip, never squeezed in.
fn render_bar_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
    data: Option<&BarData>,
    picked: bool,
) {
    let hint = |text: &str, buf: &mut ratatui::buffer::Buffer| {
        Paragraph::new(text.to_string())
            .style(Style::default().fg(ctx.text_secondary))
            .centered()
            .render(area, buf);
    };
    // Picked but not here yet: the control bar spins; the canvas waits blank.
    let Some(data) = data else {
        if !picked {
            hint("Select a category and a value", buf);
        }
        return;
    };
    if data.bars.is_empty() {
        hint("No data for bar chart", buf);
        return;
    }
    if area.height < 2 || area.width < 5 {
        return;
    }
    // A cell of air between the sidebar and the labels.
    let area = Rect {
        x: area.x + 1,
        width: area.width - 1,
        ..area
    };
    let g = crate::glyphs::get();
    let width = area.width as usize;

    // A header row, then a row per bar; when they do not all fit, the last row is
    // the count of the rest.
    let rows = area.height as usize - 1;
    let total = data.bars.len() + data.more;
    let shown = if total <= rows {
        data.bars.len()
    } else {
        rows.saturating_sub(1).min(data.bars.len())
    };
    let bars = &data.bars[..shown];
    let hidden = total - shown;

    let values: Vec<String> = bars.iter().map(|b| format_bar_value(b.value)).collect();
    let value_w = values
        .iter()
        .map(|v| crate::glyphs::display_width(v))
        .max()
        .unwrap_or(0);
    let label_cap = (width * 2 / 5).max(4);
    // Wide enough for the column's name above the labels, too.
    let label_w = bars
        .iter()
        .map(|b| crate::glyphs::display_width(b.label.as_deref().unwrap_or(g.null)))
        .chain([crate::glyphs::display_width(&data.category)])
        .max()
        .unwrap_or(0)
        .clamp(1, label_cap);
    let value_x = label_w + 1;
    let bar_x = value_x + value_w + 1;
    let bar_w = width.saturating_sub(bar_x);

    let header = Style::default().fg(ctx.text_secondary);
    let text = Style::default().fg(ctx.text_primary);
    let bar_style = Style::default().fg(ctx.primary_chart_series_color);
    let put = |buf: &mut ratatui::buffer::Buffer, x: usize, y: u16, s: &str, style: Style| {
        if x < width {
            buf.set_stringn(area.x + x as u16, y, s, width - x, style);
        }
    };
    let fit = |s: &str, w: usize| -> String {
        if crate::glyphs::display_width(s) <= w {
            return s.to_string();
        }
        let room = w.saturating_sub(crate::glyphs::display_width(g.ellipsis));
        format!("{}{}", crate::glyphs::take_columns(s, room), g.ellipsis)
    };

    // The value column's name ends where the values end, or starts where they start
    // when it is the wider of the two.
    put(buf, 0, area.y, &fit(&data.category, label_w), header);
    let name_w = crate::glyphs::display_width(&data.value_column);
    let name_x = if name_w <= value_w {
        value_x + value_w - name_w
    } else {
        value_x
    };
    put(buf, name_x, area.y, &data.value_column, header);

    let lo = bars.iter().map(|b| b.value).fold(0.0_f64, f64::min);
    let hi = bars.iter().map(|b| b.value).fold(0.0_f64, f64::max);
    let span = if hi > lo { hi - lo } else { 1.0 };
    // A negative value keeps a cell left of zero, however small it is beside the rest.
    let zero = (((-lo / span) * bar_w as f64).round() as usize)
        .max(usize::from(lo < 0.0))
        .min(bar_w);
    for (i, (bar, value)) in bars.iter().zip(&values).enumerate() {
        let y = area.y + 1 + i as u16;
        match &bar.label {
            Some(label) => put(buf, 0, y, &fit(label, label_w), text),
            None => put(buf, 0, y, g.null, Style::default().fg(ctx.dimmed)),
        }
        let pad = value_w - crate::glyphs::display_width(value);
        put(buf, value_x + pad, y, value, text);
        if bar_w == 0 {
            continue;
        }
        // A value that is not zero always draws: the thinnest mark, at least.
        let nonzero = usize::from(bar.value != 0.0);
        if bar.value >= 0.0 {
            // Eighths of a cell, so short bars still differ.
            let eighths = ((bar.value / span) * bar_w as f64 * 8.0).round() as usize;
            let eighths = eighths.max(nonzero).min((bar_w - zero.min(bar_w)) * 8);
            let mut body = g.bar_eighths[7].repeat(eighths / 8);
            if let Some(part) = (eighths % 8).checked_sub(1) {
                body.push_str(g.bar_eighths[part]);
            }
            put(buf, bar_x + zero, y, &body, bar_style);
        } else {
            // Leftward from zero in whole cells: the eighths fill from the left.
            let cells = ((-bar.value / span) * bar_w as f64).round() as usize;
            let cells = cells.max(nonzero).min(zero);
            put(
                buf,
                bar_x + zero - cells,
                y,
                &g.bar_eighths[7].repeat(cells),
                bar_style,
            );
        }
    }
    if hidden > 0 {
        let y = area.y + 1 + shown as u16;
        let chip = format!(" + {} more ", crate::numfmt::group_chrome(hidden));
        put(
            buf,
            0,
            y,
            &chip,
            Style::default().bg(ctx.controls_bg).fg(ctx.text_primary),
        );
    }
}

/// The XY chart's prepared series, as `ChartRenderData::XY` carries them.
struct XYData<'a> {
    series: Option<&'a Vec<Vec<(f64, f64)>>>,
    breaks: Option<&'a Vec<Vec<usize>>>,
    x_axis_kind: XAxisTemporalKind,
    x_bounds: Option<(f64, f64)>,
}

/// One XY series with where its line breaks.
struct SeriesRuns<'a> {
    name: &'a str,
    points: &'a [(f64, f64)],
    breaks: &'a [usize],
}

fn render_xy_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    xy: XYData<'_>,
    text_secondary: ratatui::style::Color,
) {
    let XYData {
        series: chart_data,
        breaks,
        x_axis_kind,
        x_bounds,
    } = xy;
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
        if let Some(data) = chart_data {
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

            // Data is already in display form (log-scaled when log_scale) from cache; use as-is.
            let no_breaks = Vec::new();
            let names_and_points: Vec<SeriesRuns> = data
                .iter()
                .zip(y_columns.iter())
                .enumerate()
                .filter_map(|(i, (points, name))| {
                    if points.is_empty() {
                        return None;
                    }
                    let series_breaks = breaks.and_then(|b| b.get(i)).unwrap_or(&no_breaks);
                    Some(SeriesRuns {
                        name: name.as_str(),
                        points: points.as_slice(),
                        breaks: series_breaks.as_slice(),
                    })
                })
                .collect();

            for SeriesRuns { points, .. } in &names_and_points {
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

            // A series is drawn as its runs between gaps, so a line never bridges a
            // missing value; only the first run is named, which keeps one legend entry.
            let datasets: Vec<Dataset> = names_and_points
                .iter()
                .enumerate()
                .flat_map(|(i, series)| {
                    let color_key = series_colors
                        .get(i)
                        .copied()
                        .unwrap_or("primary_chart_series_color");
                    let style = Style::default().fg(theme.get(color_key));
                    segments(series.points, series.breaks)
                        .into_iter()
                        .enumerate()
                        .map(move |(j, run)| {
                            let dataset = Dataset::default()
                                .marker(marker)
                                .graph_type(graph_type)
                                .style(style)
                                .data(run);
                            if j == 0 {
                                dataset.name(series.name)
                            } else {
                                dataset
                            }
                        })
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
        }
    } else {
        Paragraph::new("Select X and Y columns in the sidebar.")
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

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn open_modal() -> ChartModal {
        let mut modal = ChartModal::new();
        modal.open(
            crate::chart_modal::ChartColumns {
                numeric: &["price".to_string(), "volume".to_string()],
                datetime: &["date".to_string()],
                category: &["carrier".to_string()],
            },
            Some(10_000),
            1,
        );
        modal
    }

    fn render_rows(modal: &mut ChartModal, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("default theme colors must resolve");
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_chart_view(
            area,
            &mut buf,
            modal,
            &theme,
            &ctx,
            ChartView {
                data: ChartRenderData::XY {
                    series: None,
                    breaks: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                },
                notes: Vec::new(),
                error: None,
            },
        );
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    /// The tab line names every chart kind, the sidebar is one Surface of
    /// FormRows with every choice echoed, and nothing inside grows a border.
    #[test]
    fn the_xy_form_echoes_every_option_inside_one_surface() {
        let mut modal = open_modal();
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["price".to_string()];
        let rows = render_rows(&mut modal, 100, 24);

        for kind in ChartKind::ALL {
            assert!(rows[0].contains(kind.as_str()), "tab line: {:?}", rows[0]);
        }
        assert!(
            rows[1].contains("Options"),
            "the surface title: {:?}",
            rows[1]
        );
        for row in &rows[2..23] {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a second border inside the surface: {row:?}"
            );
        }
        assert!(rows[2].contains("Style:") && rows[2].contains("Line"));
        assert!(rows[3].contains("X axis:") && rows[3].contains("date"));
        assert!(rows[4].contains("Y series:") && rows[4].contains("price"));
        assert!(rows[5].contains("Y from zero:"));
        assert!(rows[6].contains("Log scale:"));
        assert!(rows[7].contains("Legend:"));
        assert!(rows[8].contains("Sample size:") && rows[8].contains("10,000"));
    }

    /// Only the active chart kind's options render.
    #[test]
    fn each_kind_shows_only_its_own_options() {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Kde);
        let rows = render_rows(&mut modal, 100, 24);
        let body = rows.join("\n");
        assert!(body.contains("Column:") && body.contains("Bandwidth:"));
        assert!(!body.contains("Bins:") && !body.contains("Log scale:"));

        modal.set_chart_kind(ChartKind::Heatmap);
        let rows = render_rows(&mut modal, 100, 24);
        let body = rows.join("\n");
        assert!(body.contains("X axis:") && body.contains("Y axis:") && body.contains("Bins:"));
        assert!(!body.contains("Bandwidth:"));
    }

    /// The open Picker drops in below the rows, with a checkbox per item on
    /// the Y series row.
    #[test]
    fn the_y_picker_shows_toggles_below_the_rows() {
        let g = crate::glyphs::get();
        let mut modal = open_modal();
        modal.focus = ChartFocus::YColumns;
        modal.open_picker();
        modal.picker_toggle(); // price in
        let rows = render_rows(&mut modal, 100, 24);
        let body = rows.join("\n");
        assert!(
            body.contains(&format!("{} price", g.checkbox_on)),
            "chosen series checked: {body}"
        );
        assert!(
            body.contains(&format!("{} volume", g.checkbox_off)),
            "other items unchecked: {body}"
        );
    }

    fn render_view(modal: &mut ChartModal, view: ChartView<'_>, w: u16, h: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("default theme colors must resolve");
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render_chart_view(area, &mut buf, modal, &theme, &ctx, view);
        (0..h)
            .map(|y| (0..w).map(|x| buf[(x, y)].symbol().to_string()).collect())
            .collect()
    }

    /// A sampled or clipped chart says so under the plot, even at 80x24.
    #[test]
    fn notes_sit_under_the_plot() {
        let mut modal = open_modal();
        modal.x_column = Some("price".to_string());
        modal.y_columns = vec!["volume".to_string()];
        let series = vec![vec![(0.0, 1.0), (1.0, 2.0)]];
        let rows = render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::XY {
                    series: Some(&series),
                    breaks: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                },
                notes: vec!["sample of 10,000 of 3.5M rows".to_string()],
                error: None,
            },
            80,
            24,
        );
        assert!(
            rows[23].contains("sample of 10,000 of 3.5M rows"),
            "{:?}",
            rows[23]
        );
    }

    /// On a canvas narrower than a note, the note wraps; nothing of it is cut.
    #[test]
    fn notes_wrap_on_a_narrow_canvas() {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Histogram);
        let notes = [
            "sample of 1,000,000 of 36.8M rows",
            "1,207 values outside p1-p99",
        ];
        let rows = render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::Histogram { data: None },
                notes: notes.iter().map(|n| n.to_string()).collect(),
                error: None,
            },
            60,
            20,
        );
        // The canvas is the right half; read its last rows as one line of words.
        let text = rows[14..]
            .iter()
            .map(|r| r.chars().skip(30).collect::<String>().trim().to_string())
            .collect::<Vec<_>>()
            .join(" ");
        for note in notes {
            assert!(text.contains(note), "{note:?} whole in {text:?}");
        }
    }

    /// A failed preparation shows its message where the plot would be.
    #[test]
    fn an_error_replaces_the_plot() {
        let mut modal = open_modal();
        let rows = render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::XY {
                    series: None,
                    breaks: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                },
                notes: Vec::new(),
                error: Some("column not found: gone"),
            },
            100,
            24,
        );
        assert!(rows.join("\n").contains("column not found: gone"));
    }

    /// A line breaks at a gap: nothing is drawn between the runs on either side.
    #[test]
    fn a_line_does_not_bridge_a_gap() {
        let mut modal = open_modal();
        modal.x_column = Some("price".to_string());
        modal.y_columns = vec!["volume".to_string()];
        modal.show_legend = false;
        let series = vec![vec![(0.0, 0.0), (1.0, 0.0), (9.0, 0.0), (10.0, 0.0)]];
        let draw = |modal: &mut ChartModal, breaks: &Vec<Vec<usize>>| {
            render_view(
                modal,
                ChartView {
                    data: ChartRenderData::XY {
                        series: Some(&series),
                        breaks: Some(breaks),
                        x_axis_kind: XAxisTemporalKind::Numeric,
                        x_bounds: None,
                    },
                    notes: Vec::new(),
                    error: None,
                },
                100,
                24,
            )
        };
        let blank = |c: char| c == ' ' || c == '\u{2800}';
        let marks = |rows: &[String]| -> usize {
            rows.iter()
                .map(|r| r.chars().skip(44).filter(|c| !blank(*c)).count())
                .sum()
        };
        let joined = draw(&mut modal, &vec![Vec::new()]);
        let broken = draw(&mut modal, &vec![vec![2]]);
        assert!(
            marks(&broken) < marks(&joined),
            "the run from 1 to 9 is not drawn"
        );
    }

    fn bar_data(n: usize) -> BarData {
        use crate::chart_data::Bar;
        BarData {
            category: "carrier".to_string(),
            value_column: "delay".to_string(),
            bars: (0..n)
                .map(|i| Bar {
                    label: Some(format!("C{i}")),
                    value: (n - i) as f64,
                })
                .collect(),
            more: 0,
            no_value: 0,
            rows: Default::default(),
        }
    }

    fn render_bars(data: &BarData, w: u16, h: u16) -> Vec<String> {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Bar);
        render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::Bar { data: Some(data) },
                notes: Vec::new(),
                error: None,
            },
            w,
            h,
        )
    }

    /// A bar per category: its label, its value beside it, and a bar as long as the
    /// value, the largest filling the width.
    #[test]
    fn bars_draw_label_value_and_length() {
        let g = crate::glyphs::get();
        let full = g.bar_eighths[7];
        let data = bar_data(3);
        let rows = render_bars(&data, 100, 24);
        // The canvas starts past the 42-column sidebar.
        let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
        assert!(canvas[1].trim_start().starts_with("carrier") && canvas[1].contains("delay"));
        let bar_len = |row: &str| row.matches(full).count();
        let words = |row: &str| row.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
        assert_eq!(words(&canvas[2]), "C0 3", "{:?}", canvas[2]);
        assert_eq!(words(&canvas[4]), "C2 1", "{:?}", canvas[4]);
        let (a, c) = (bar_len(&canvas[2]), bar_len(&canvas[4]));
        assert!(
            a > 40 && (a as f64 / c as f64 - 3.0).abs() < 0.2,
            "{a} vs {c}"
        );
    }

    /// More bars than rows: the ones that fit, then a chip counting the rest, which
    /// includes those the preparation already left out.
    #[test]
    fn bars_past_the_height_are_counted_on_a_chip() {
        let mut data = bar_data(50);
        data.more = 200;
        let rows = render_bars(&data, 80, 24);
        let body = rows.join("\n");
        // 23 rows under the tab line: a header, 21 bars, the chip.
        assert!(body.contains("C20 "), "{body}");
        assert!(!body.contains("C21 "), "{body}");
        assert!(rows[23].contains(" + 229 more "), "{:?}", rows[23]);
    }

    /// A negative value draws left of the zero line; a null category reads as null.
    #[test]
    fn negative_bars_grow_left_and_null_categories_show() {
        use crate::chart_data::Bar;
        let g = crate::glyphs::get();
        let full = g.bar_eighths[7];
        let mut data = bar_data(0);
        data.bars = vec![
            Bar {
                label: Some("UA".to_string()),
                value: 10.0,
            },
            Bar {
                label: None,
                value: -10.0,
            },
        ];
        let rows = render_bars(&data, 100, 24);
        let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
        let first = |row: &str| row.find(full).map(|i| row[..i].chars().count());
        let pos = first(&canvas[2]).unwrap();
        let neg = first(&canvas[3]).unwrap();
        assert!(neg < pos, "the negative bar starts left of zero");
        assert!(
            canvas[3].trim_start().starts_with(g.null),
            "{:?}",
            canvas[3]
        );
    }

    /// A value far smaller than the rest still draws a mark, either side of zero, so
    /// no bar that is not zero reads as zero.
    #[test]
    fn a_tiny_value_still_draws_a_mark() {
        use crate::chart_data::Bar;
        let g = crate::glyphs::get();
        let mut data = bar_data(0);
        data.bars = [
            ("big", 10_000.0),
            ("tiny", 0.1),
            ("zero", 0.0),
            ("neg", -0.1),
        ]
        .into_iter()
        .map(|(label, value)| Bar {
            label: Some(label.to_string()),
            value,
        })
        .collect();
        let rows = render_bars(&data, 100, 24);
        let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
        // The bar zone: the row with its label and value taken out.
        let marks = |row: &str| {
            let mut words = row.split_whitespace();
            let (label, value) = (words.next().unwrap(), words.next().unwrap());
            let zone = row.replacen(label, "", 1).replacen(value, "", 1);
            g.bar_eighths
                .iter()
                .filter(|m| !m.trim().is_empty() && zone.contains(**m))
                .count()
        };
        assert!(marks(&canvas[3]) > 0, "tiny: {:?}", canvas[3]);
        assert_eq!(marks(&canvas[4]), 0, "zero: {:?}", canvas[4]);
        assert!(marks(&canvas[5]) > 0, "neg: {:?}", canvas[5]);
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (3, 2), (10, 4), (20, 6), (60, 20), (80, 24)] {
            let mut modal = open_modal();
            modal.focus = ChartFocus::XColumn;
            modal.open_picker();
            let _ = render_rows(&mut modal, w, h);
            let mut data = bar_data(30);
            data.bars[3].value = -4.0;
            data.bars[4].label = Some("a label longer than the whole canvas is".to_string());
            let _ = render_bars(&data, w, h);
        }
    }
}

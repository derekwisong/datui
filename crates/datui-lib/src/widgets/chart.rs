//! Chart view widget: the tab line, the Options sidebar (one Surface of
//! FormRows, column rows edited through the shared Picker), and the chart
//! canvas.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Chart, Dataset, GraphType, Paragraph, Widget, Wrap},
};

use crate::chart_data::{
    AxisNumbers, BarData, BoxPlotData, HeatmapData, HistogramData, KdeData, XAxisTemporalKind,
    segments,
};
use crate::chart_modal::{ChartFocus, ChartKind, ChartModal, ChartType};
use crate::config::Theme;
use crate::glyphs::Glyphs;
use crate::render::context::RenderContext;
use crate::widgets::axes::{
    AxisSpec, Legend, PlotAxes, Track, cut, fit_x_labels, fit_y_labels, resolution,
};
use crate::widgets::crosshair::{self, PlotPlace};
use crate::widgets::ui::{FormRow, FormValue, Picker, Surface};
use unicode_width::UnicodeWidthStr;

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
        /// The series before any log, for the crosshair's readout; `None` reads
        /// `series`.
        values: Option<&'a Vec<Vec<(f64, f64)>>>,
        x_axis_kind: XAxisTemporalKind,
        x_bounds: Option<(f64, f64)>,
        numbers: PlotNumbers,
    },
    Histogram {
        data: Option<&'a HistogramData>,
        /// The column's numbers; the counts are the table's.
        x: AxisNumbers,
    },
    BoxPlot {
        data: Option<&'a BoxPlotData>,
        y: AxisNumbers,
    },
    Kde {
        data: Option<&'a KdeData>,
        x: AxisNumbers,
    },
    Heatmap {
        data: Option<&'a HeatmapData>,
        numbers: PlotNumbers,
    },
    Bar {
        data: Option<&'a BarData>,
    },
}

/// What each axis holds, so its ticks print as the table prints its columns: whole
/// for an integer column, grouped and separated in the table's number format.
#[derive(Default)]
pub struct PlotNumbers {
    pub x: AxisNumbers,
    pub y: AxisNumbers,
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
        ChartFocus::Grid => "Grid:",
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
            ChartFocus::Grid => FormValue::Toggle(modal.grid),
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
            ChartFocus::Value => echo_or_placeholder(
                modal
                    .bar_value
                    .as_ref()
                    .map(crate::chart_data::BarValue::label)
                    .unwrap_or(""),
                "none",
            ),
            ChartFocus::Order => FormValue::Choice(modal.bar_order.label()),
            ChartFocus::LimitRows => {
                number = modal.row_limit_display();
                FormValue::Choice(&number)
            }
        };
        FormRow {
            label: row_label(row),
            value,
            // While the plot has the keys its crosshair carries the focus.
            focused: modal.focus == row && !modal.plot_focus,
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
    modal.plot = None;
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
    modal.plot = render_plot(
        chart_inner,
        buf,
        modal,
        theme,
        ctx,
        view.data,
        crate::glyphs::get(),
    );
}

/// The plot itself, drawn with the marks of the glyph set `g`; where an XY plot with
/// points was drawn.
fn render_plot(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    ctx: &RenderContext,
    data: ChartRenderData<'_>,
    g: &Glyphs,
) -> Option<PlotPlace> {
    let text_secondary = theme.get("text_secondary");
    match data {
        ChartRenderData::XY {
            series,
            breaks,
            values,
            x_axis_kind,
            x_bounds,
            numbers,
        } => {
            let xy = XYData {
                series,
                breaks,
                values,
                x_axis_kind,
                x_bounds,
                numbers,
            };
            return render_xy_chart(area, buf, modal, theme, xy, text_secondary, g);
        }
        ChartRenderData::Histogram { data, x } => {
            let numbers = PlotNumbers {
                x,
                y: AxisNumbers::count(&ctx.number_format),
            };
            render_histogram_chart(area, buf, modal, theme, data, numbers, g)
        }
        ChartRenderData::BoxPlot { data, y } => {
            render_box_plot_chart(area, buf, modal, theme, data, y, g)
        }
        ChartRenderData::Kde { data, x } => {
            let numbers = PlotNumbers {
                x: x.fractional(),
                y: AxisNumbers::measure(&ctx.number_format, "Density"),
            };
            render_kde_chart(area, buf, modal, theme, data, numbers, g)
        }
        ChartRenderData::Heatmap { data, numbers } => {
            render_heatmap_chart(area, buf, theme, data, numbers, text_secondary, g)
        }
        ChartRenderData::Bar { data } => {
            let picked =
                modal.effective_bar_category().is_some() && modal.effective_bar_value().is_some();
            render_bar_chart(area, buf, ctx, data, picked, g)
        }
    }
    None
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
    g: &Glyphs,
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

    let mut values = data.value_labels(&ctx.number_format);
    values.truncate(shown);
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

/// The XY series' colors, in order.
const SERIES_COLORS: [&str; 7] = [
    "chart_series_color_1",
    "chart_series_color_2",
    "chart_series_color_3",
    "chart_series_color_4",
    "chart_series_color_5",
    "chart_series_color_6",
    "chart_series_color_7",
];

/// The XY chart's prepared series, as `ChartRenderData::XY` carries them.
struct XYData<'a> {
    series: Option<&'a Vec<Vec<(f64, f64)>>>,
    breaks: Option<&'a Vec<Vec<usize>>>,
    values: Option<&'a Vec<Vec<(f64, f64)>>>,
    x_axis_kind: XAxisTemporalKind,
    x_bounds: Option<(f64, f64)>,
    numbers: PlotNumbers,
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
    g: &Glyphs,
) -> Option<PlotPlace> {
    let XYData {
        series: chart_data,
        breaks,
        values,
        x_axis_kind,
        x_bounds,
        numbers,
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
        const PLACEHOLDER_MIN: f64 = 0.0;
        const PLACEHOLDER_MAX: f64 = 1.0;
        let (x_min, x_max) = x_bounds.unwrap_or((PLACEHOLDER_MIN, PLACEHOLDER_MAX));
        let axes = plot_axes(
            theme,
            x_axis([x_min, x_max], x_axis_kind, &numbers.x, x_name),
            AxisSpec::y_numbers([PLACEHOLDER_MIN, PLACEHOLDER_MAX], &numbers.y, &y_names),
            g.plot.line,
            modal.grid,
        );
        let empty_dataset = Dataset::default()
            .name("")
            .data(&[])
            .graph_type(match chart_type {
                ChartType::Line => GraphType::Line,
                ChartType::Scatter => GraphType::Scatter,
                ChartType::Bar => GraphType::Bar,
            });
        axes.render(Chart::new(vec![empty_dataset]), area, buf, g);
        return None;
    }

    if has_data {
        if let Some(data) = chart_data {
            let y_columns = modal.effective_y_columns();
            let graph_type = match chart_type {
                ChartType::Line => GraphType::Line,
                ChartType::Scatter => GraphType::Scatter,
                ChartType::Bar => GraphType::Bar,
            };

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

            // A scatter of few points marks each with a dot a cell wide; past one
            // point per four cells, the line's finer marks keep them apart.
            let points: usize = names_and_points.iter().map(|s| s.points.len()).sum();
            let cells = usize::from(area.width) * usize::from(area.height);
            let finer = resolution(g.plot.line) > resolution(g.plot.point);
            let marker = match chart_type {
                ChartType::Line => g.plot.line,
                ChartType::Scatter if finer && points * 4 > cells => g.plot.line,
                ChartType::Scatter => g.plot.point,
                ChartType::Bar => g.plot.bar,
            };

            // A series is drawn as its runs between gaps, so a line never bridges a
            // missing value; only the first run is named, which keeps one legend entry.
            let name_width = legend_width(names_and_points.iter().map(|s| s.name));
            let datasets: Vec<Dataset> = names_and_points
                .iter()
                .enumerate()
                .flat_map(|(i, series)| {
                    let color_key = SERIES_COLORS
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
                                dataset.name(legend_name(series.name, name_width))
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
                return None;
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

            let x_axis_title = modal
                .effective_x_column()
                .map(|s| modal.axis_title(s))
                .unwrap_or_default();
            let y_axis_title = y_columns
                .iter()
                .map(|c| modal.axis_title(c))
                .collect::<Vec<_>>()
                .join(", ");
            let y_bounds = [y_min_bounds, y_max_bounds];
            // On a log scale a tick stands for its value before the log.
            let y = if log_scale {
                AxisSpec::y_log(y_bounds, &numbers.y.clone().fractional(), &y_axis_title)
            } else {
                AxisSpec::y_numbers(y_bounds, &numbers.y, &y_axis_title)
            };
            let mut axes = plot_axes(
                theme,
                x_axis(
                    [x_min_bounds, x_max_bounds],
                    x_axis_kind,
                    &numbers.x,
                    &x_axis_title,
                ),
                y,
                marker,
                modal.grid,
            );
            axes.legend = legend(show_legend, names_and_points.len(), name_width);
            let x_bounds = [x_min_bounds, x_max_bounds];
            let sub = resolution(marker).0;
            // The crosshair's readout takes the rows under the plot while the plot
            // has the keys.
            let values = values.unwrap_or(data);
            let cursor = modal
                .cursor_x
                .filter(|_| modal.plot_focus)
                .and_then(|x| crosshair::nearest(&crosshair::xs(values), x));
            let readout = cursor
                .map(|x| {
                    let written = crosshair::format_x(x, x_axis_kind, &numbers.x);
                    let entries = readout_entries(
                        theme,
                        g,
                        (x, &x_axis_title, written),
                        &numbers.y,
                        values,
                        &y_columns,
                    );
                    crosshair::readout_lines(&entries, area.width as usize, g)
                })
                .unwrap_or_default();
            let rows = (readout.len() as u16).min(area.height / 3);
            let [plot_area, readout_area] =
                Layout::vertical([Constraint::Fill(1), Constraint::Length(rows)]).areas(area);
            let frame = axes.render(Chart::new(datasets), plot_area, buf, g);
            let place = PlotPlace {
                graph: frame.graph,
                x_bounds,
                sub,
            };
            if let Some(x) = cursor {
                let style = Style::default().fg(theme.get("accent"));
                crosshair::draw(buf, &place, x, style, g);
                Paragraph::new(readout).render(readout_area, buf);
            }
            return Some(place);
        }
    } else {
        Paragraph::new("Select X and Y columns in the sidebar.")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
    }
    None
}

/// The readout at the crosshair's `x`: x under its title as `written`, then each
/// series' value there under its name in its color, `∅` where it has a gap.
fn readout_entries(
    theme: &Theme,
    g: &Glyphs,
    (x, x_title, written): (f64, &str, String),
    y: &AxisNumbers,
    series: &[Vec<(f64, f64)>],
    names: &[String],
) -> Vec<crosshair::Entry> {
    let value_style = Style::default().fg(theme.get("text_primary"));
    let x_title = if x_title.is_empty() { "x" } else { x_title };
    let mut entries = vec![crosshair::Entry {
        name: x_title.to_string(),
        name_style: Style::default().fg(theme.get("text_secondary")),
        value: written,
        value_style,
    }];
    for (i, (value, name)) in crosshair::values_at(series, x)
        .into_iter()
        .zip(names)
        .enumerate()
    {
        let color = SERIES_COLORS
            .get(i)
            .copied()
            .unwrap_or("primary_chart_series_color");
        let (value, value_style) = match value {
            Some(v) => (crosshair::format_number(v, y), value_style),
            None => (g.null.to_string(), Style::default().fg(theme.get("dimmed"))),
        };
        entries.push(crosshair::Entry {
            name: name.clone(),
            name_style: Style::default().fg(theme.get(color)),
            value,
            value_style,
        });
    }
    entries
}

/// Axes drawn in the theme's primary text color, ticked for series drawn with
/// `marker`, with the grid in `chart_grid` when `grid` is on.
fn plot_axes<'a>(
    theme: &Theme,
    x: AxisSpec<'a>,
    y: AxisSpec<'a>,
    marker: ratatui::symbols::Marker,
    grid: bool,
) -> PlotAxes<'a> {
    let style = Style::default().fg(theme.get("text_primary"));
    PlotAxes {
        grid: grid.then(|| Style::default().fg(theme.get("chart_grid"))),
        ..PlotAxes::new(x, y, style, marker)
    }
}

/// An XY chart's x axis: numbers on nice steps, or dates and times on calendar
/// boundaries.
fn x_axis<'a>(
    bounds: [f64; 2],
    kind: XAxisTemporalKind,
    numbers: &AxisNumbers,
    title: &'a str,
) -> AxisSpec<'a> {
    AxisSpec::calendar(bounds, kind, numbers, title)
}

/// The legend, when it is on and there is more than one series to tell apart: the
/// y title names a lone one.
fn legend(show: bool, series: usize, name_width: usize) -> Option<Legend> {
    (show && series > 1).then_some(Legend {
        width: name_width as u16,
        rows: series as u16,
    })
}

/// The widest of the legend's names, in cells.
fn legend_width<'a>(names: impl Iterator<Item = &'a str>) -> usize {
    names.map(UnicodeWidthStr::width).max().unwrap_or(0)
}

/// A legend name padded to the legend's width. ratatui writes each name over the
/// plot without clearing the rest of its row, so marks showed through beside a
/// short name.
fn legend_name(name: &str, width: usize) -> String {
    let pad = width.saturating_sub(UnicodeWidthStr::width(name));
    format!("{name}{:pad$}", "")
}

fn render_histogram_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    data: Option<&HistogramData>,
    numbers: PlotNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
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

    let axes = plot_axes(
        theme,
        AxisSpec::numbers(
            [x_min_bounds, x_max_bounds],
            &numbers.x,
            data.column.as_str(),
        ),
        AxisSpec::y_numbers([y_min_bounds, y_max_bounds], &numbers.y, "Count"),
        g.plot.bar,
        modal.grid,
    );

    let columns = axes.frame(area).graph.width;
    let points = bin_columns(data, [x_min_bounds, x_max_bounds], columns);
    let style = Style::default().fg(theme.get("chart_1"));
    let dataset = Dataset::default()
        .name("")
        .marker(g.plot.bar)
        .graph_type(GraphType::Bar)
        .style(style)
        .data(&points);

    axes.render(Chart::new(vec![dataset]), area, buf, g);
}

/// A histogram's bars as a column of the plot each, `columns` wide over `bounds`: a
/// bin fills the columns its values land on, less one between it and the next when
/// it is three or more wide, so the bars read as bins and not as needles.
fn bin_columns(data: &HistogramData, [lo, hi]: [f64; 2], columns: u16) -> Vec<(f64, f64)> {
    let n = data.bins.len();
    if n == 0 || columns < 2 || hi <= lo {
        return data.bins.iter().map(|b| (b.center, b.count)).collect();
    }
    let last = f64::from(columns - 1);
    // The value at a column's center, as the canvas maps values to columns.
    let at = |c: u16| lo + f64::from(c) / last * (hi - lo);
    let bin_of = |v: f64| (((v - lo) / (hi - lo) * n as f64).floor() as usize).min(n - 1);
    let bin_cols: Vec<u16> = (0..columns).map(|c| bin_of(at(c)) as u16).collect();
    (0..columns)
        .filter_map(|c| {
            let bin = bin_cols[c as usize];
            let wide = bin_cols.iter().filter(|b| **b == bin).count();
            let last_of_bin = bin_cols.get(c as usize + 1).is_some_and(|b| *b != bin);
            (!(last_of_bin && wide >= 3)).then(|| (at(c), data.bins[bin as usize].count))
        })
        .collect()
}

fn render_kde_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    data: Option<&KdeData>,
    numbers: PlotNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
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
        "chart_1", "chart_2", "chart_3", "chart_4", "chart_5", "chart_6", "chart_7",
    ];

    let name_width = legend_width(data.series.iter().map(|s| s.name.as_str()));
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
                .name(legend_name(&s.name, name_width))
                .graph_type(GraphType::Line)
                .marker(g.plot.line)
                .style(style)
                .data(&s.points)
        })
        .collect();

    let mut axes = plot_axes(
        theme,
        AxisSpec::numbers([data.x_min, data.x_max], &numbers.x, "Value"),
        AxisSpec::y_numbers([0.0, data.y_max], &numbers.y, "Density"),
        g.plot.line,
        modal.grid,
    );
    axes.legend = legend(modal.show_legend, data.series.len(), name_width);
    axes.render(Chart::new(datasets), area, buf, g);
}

fn render_box_plot_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    data: Option<&BoxPlotData>,
    y_numbers: AxisNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
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
        "chart_1", "chart_2", "chart_3", "chart_4", "chart_5", "chart_6", "chart_7",
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
                .marker(g.plot.point)
                .style(*style)
                .data(points)
        })
        .collect();

    let x_min_bounds = -0.5;
    let x_max_bounds = (data.stats.len() as f64 - 1.0).max(0.0) + 0.5;
    // A box's name sits under the box, at its index.
    let name = |i: f64, level| {
        let stat = data.stats.get(i as usize)?;
        (level == 0).then(|| stat.name.clone())
    };
    let x = AxisSpec::fixed(
        [x_min_bounds, x_max_bounds],
        (0..data.stats.len()).map(|i| i as f64).collect(),
        Box::new(name),
        "Columns",
    );
    let y = AxisSpec::y_numbers([data.y_min, data.y_max], &y_numbers, "Value");
    plot_axes(theme, x, y, g.plot.point, modal.grid).render(Chart::new(datasets), area, buf, g);
}

fn render_heatmap_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: Option<&HeatmapData>,
    numbers: PlotNumbers,
    text_secondary: ratatui::style::Color,
    g: &Glyphs,
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

    const Y_LABEL_MAX: u16 = 12;
    // Nice values up the side, one per few rows, each on the row its value falls in.
    let y_axis = AxisSpec::numbers([data.y_min, data.y_max], &numbers.y, "");
    let rows = layout[1];
    let y_track = Track {
        start: rows.y,
        cells: rows.height,
        sub: 1,
    };
    let y_labels = fit_y_labels(&y_axis, y_track, Y_LABEL_MAX).labels;
    let y_label_width = y_labels.iter().map(|(_, l)| l.width()).max().unwrap_or(1);
    let y_label_width = (y_label_width as u16).clamp(4, Y_LABEL_MAX);
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
    for (row, label) in &y_labels {
        let pad = usize::from(y_label_width).saturating_sub(label.width()) as u16;
        buf.set_stringn(
            label_area.x + pad,
            *row,
            label,
            usize::from(y_label_width),
            label_style,
        );
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
            cell.set_style(Style::default().fg(theme.get("chart_1")));
        }
    }

    let x_label_area = layout[2];
    let x_axis = AxisSpec::numbers([data.x_min, data.x_max], &numbers.x, "");
    let span = (x_label_area.left(), x_label_area.right());
    let x_track = Track {
        start: plot_area.x,
        cells: plot_area.width,
        sub: 1,
    };
    for (x, label) in fit_x_labels(&x_axis, span, x_track).labels {
        buf.set_string(x, x_label_area.y, label, label_style);
    }
    if x_label_area.height > 1 {
        // Each title keeps half the row when both do not fit, a space between them.
        let x_title = format!("X: {}", data.x_column);
        let y_title = format!("Y: {}", data.y_column);
        let width = x_label_area.width as usize;
        let (x_title, y_title) = if x_title.width() + y_title.width() < width {
            (x_title, y_title)
        } else {
            let half = width.saturating_sub(1) / 2;
            (cut(&x_title, half, g), cut(&y_title, half, g))
        };
        let row = x_label_area.y + 1;
        buf.set_string(x_label_area.x, row, &x_title, label_style);
        let y_x = x_label_area.right() - y_title.width() as u16;
        buf.set_string(y_x, row, &y_title, label_style);
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
            false,
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
                    values: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    numbers: PlotNumbers::default(),
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
        assert!(rows[8].contains("Grid:"));
        assert!(rows[9].contains("Sample size:") && rows[9].contains("10,000"));
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
                    values: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    numbers: PlotNumbers::default(),
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
                data: ChartRenderData::Histogram {
                    data: None,
                    x: AxisNumbers::default(),
                },
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
                    values: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    numbers: PlotNumbers::default(),
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
                        values: None,
                        x_axis_kind: XAxisTemporalKind::Numeric,
                        x_bounds: None,
                        numbers: PlotNumbers::default(),
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
            value_dtype: polars::prelude::DataType::Float64,
            counted: None,
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
        assert_eq!(words(&canvas[2]), "C0 3.00", "{:?}", canvas[2]);
        assert_eq!(words(&canvas[4]), "C2 1.00", "{:?}", canvas[4]);
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

    fn plot_text(modal: &ChartModal, data: ChartRenderData<'_>, g: &Glyphs) -> String {
        plot_text_in(modal, data, g, Rect::new(0, 0, 60, 20))
    }

    fn plot_text_in(
        modal: &ChartModal,
        data: ChartRenderData<'_>,
        g: &Glyphs,
        area: Rect,
    ) -> String {
        plot_text_with(&RenderContext::for_test(), modal, data, g, area)
    }

    fn plot_text_with(
        ctx: &RenderContext,
        modal: &ChartModal,
        data: ChartRenderData<'_>,
        g: &Glyphs,
        area: Rect,
    ) -> String {
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("default theme colors must resolve");
        let mut buf = Buffer::empty(area);
        render_plot(area, &mut buf, modal, &theme, ctx, data, g);
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// At 40, 60 and 80 columns, in both glyph sets, every plot's x labels stand a
    /// space apart with both ends kept, and its axis titles sit on rows of their own
    /// that hold nothing of the plot.
    #[test]
    fn axis_labels_stand_apart_and_titles_keep_their_rows() {
        /// A plot, its x and y titles, and what its x labels look like.
        type Case<'a> = (
            &'a str,
            ChartRenderData<'a>,
            &'a str,
            &'a str,
            &'a dyn Fn(&str) -> bool,
        );
        use crate::chart_data::{HistogramBin, KdeSeries};
        // 2020-01-01 to 2024-12-31, in days since the epoch.
        let dates: Vec<Vec<(f64, f64)>> = vec![
            (0..=100)
                .map(|i| (18262.0 + f64::from(i) * 18.26, f64::from(i % 7) * 1000.0))
                .collect(),
        ];
        let numbers: Vec<Vec<(f64, f64)>> = vec![
            (0..20)
                .map(|i| (i as f64 * 1234.5, (i * i) as f64))
                .collect(),
        ];
        let histogram = HistogramData {
            column: "price".to_string(),
            bins: (0..10)
                .map(|i| HistogramBin {
                    center: i as f64 * 250.0 + 125.0,
                    count: (1 + i % 4) as f64 * 1000.0,
                })
                .collect(),
            x_min: 0.0,
            x_max: 2500.0,
            max_count: 4000.0,
            rows: Default::default(),
            clipped: None,
        };
        let kde = KdeData {
            series: vec![KdeSeries {
                name: "price".to_string(),
                points: (0..=100)
                    .map(|i| {
                        let x = i as f64 * 100.0 - 5000.0;
                        (x, (-x * x / 2e6).exp())
                    })
                    .collect(),
            }],
            x_min: -5000.0,
            x_max: 5000.0,
            y_max: 1.0,
            rows: Default::default(),
            clipped: None,
        };
        let is_date = |t: &str| {
            [4, 5, 7, 10].contains(&t.len()) && t.chars().all(|c| c.is_ascii_digit() || c == '-')
        };
        let is_number = |t: &str| {
            t.trim_end_matches(['k', 'M', 'G', 'T'])
                .parse::<f64>()
                .is_ok()
        };

        let mut modal = open_modal();
        modal.y_columns = vec!["price".to_string()];
        modal.show_legend = false;
        for columns in [40u16, 60, 80] {
            let area = Rect::new(0, 0, columns - SIDEBAR_WIDTH.min(columns / 2), 18);
            for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
                let cases: [Case; 4] = [
                    (
                        "date",
                        ChartRenderData::XY {
                            series: Some(&dates),
                            breaks: None,
                            values: None,
                            x_axis_kind: XAxisTemporalKind::Date,
                            x_bounds: None,
                            numbers: PlotNumbers::default(),
                        },
                        "date",
                        "price",
                        &is_date,
                    ),
                    (
                        "number",
                        ChartRenderData::XY {
                            series: Some(&numbers),
                            breaks: None,
                            values: None,
                            x_axis_kind: XAxisTemporalKind::Numeric,
                            x_bounds: None,
                            numbers: PlotNumbers::default(),
                        },
                        "volume",
                        "price",
                        &is_number,
                    ),
                    (
                        "histogram",
                        ChartRenderData::Histogram {
                            data: Some(&histogram),
                            x: AxisNumbers::default(),
                        },
                        "price",
                        "Count",
                        &is_number,
                    ),
                    (
                        "KDE",
                        ChartRenderData::Kde {
                            data: Some(&kde),
                            x: AxisNumbers::default(),
                        },
                        "Value",
                        "Density",
                        &is_number,
                    ),
                ];
                for (what, data, x_title, y_title, is_label) in cases {
                    modal.x_column = Some(x_title.to_string());
                    let text = plot_text_in(&modal, data, g, area);
                    let rows: Vec<&str> = text.lines().collect();
                    let what = format!("{what} at {columns} columns:\n{text}");
                    let axis = rows
                        .iter()
                        .rposition(|r| r.contains(g.plot.axis.bottom_left))
                        .expect(&what);
                    let labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
                    assert!(labels.len() >= 2, "both ends: {what}");
                    assert!(labels.iter().all(|l| is_label(l)), "apart: {what}");
                    assert_eq!(rows[0].trim_end(), y_title, "y title row: {what}");
                    assert_eq!(rows[axis + 2].trim_start(), x_title, "x title row: {what}");
                }
            }
        }
    }

    /// Every label on an axis in one format, in the table's number style: KDE
    /// densities around 0.01 keep one precision all the way up, and under the
    /// european format a narrow axis's short form reads `12,5k`.
    #[test]
    fn an_axis_writes_every_label_in_one_format() {
        use crate::chart_data::KdeSeries;
        use crate::numfmt::NumberFormat;
        let g = crate::glyphs::unicode();
        let y_labels = |text: &str| -> Vec<String> {
            text.lines()
                .filter_map(|r| r.split_once(['│', '┤'])?.0.split_whitespace().next())
                .map(String::from)
                .collect()
        };
        let mut modal = open_modal();
        modal.show_legend = false;

        let kde = KdeData {
            series: vec![KdeSeries {
                name: "price".to_string(),
                points: (0..=100)
                    .map(|i| {
                        let x = f64::from(i);
                        (x, 0.0126 * (-(x - 50.0).powi(2) / 400.0).exp())
                    })
                    .collect(),
            }],
            x_min: 0.0,
            x_max: 100.0,
            y_max: 0.0126,
            rows: Default::default(),
            clipped: None,
        };
        let data = ChartRenderData::Kde {
            data: Some(&kde),
            x: AxisNumbers::default(),
        };
        let text = plot_text_in(&modal, data, g, Rect::new(0, 0, 40, 12));
        assert_eq!(y_labels(&text), ["0.02", "0.01", "0.00"], "{text}");

        let european = NumberFormat::preset("european").unwrap();
        let mut ctx = RenderContext::for_test();
        ctx.number_format.format = european.clone();
        modal.x_column = Some("volume".to_string());
        modal.y_columns = vec!["price".to_string()];
        modal.y_starts_at_zero = false;
        let series = vec![vec![(0.0, 12_000.0), (5.0, 12_600.0)]];
        let xy = || ChartRenderData::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            numbers: PlotNumbers {
                x: AxisNumbers::default(),
                y: AxisNumbers {
                    format: european.clone(),
                    whole: false,
                },
            },
        };
        let text = plot_text_with(&ctx, &modal, xy(), g, Rect::new(0, 0, 40, 12));
        assert_eq!(y_labels(&text), ["13.000", "12.500", "12.000"], "{text}");
        // Too narrow for those: the short form, each in the same unit and places.
        let text = plot_text_with(&ctx, &modal, xy(), g, Rect::new(0, 0, 16, 12));
        assert_eq!(y_labels(&text), ["13,0k", "12,5k", "12,0k"], "{text}");
    }

    /// A heatmap's y labels in one format; an integer column's middle label is left
    /// out when it falls between two whole numbers, where it read `0` twice.
    #[test]
    fn heatmap_y_labels_are_whole_where_the_column_is() {
        let heatmap = |(y_min, y_max)| HeatmapData {
            x_column: "price".to_string(),
            y_column: "flag".to_string(),
            x_min: 0.0,
            x_max: 10.0,
            y_min,
            y_max,
            x_bins: 4,
            y_bins: 4,
            counts: vec![vec![1.0; 4]; 4],
            max_count: 1.0,
            rows: Default::default(),
        };
        let y_labels = |text: &str| -> Vec<String> {
            // The plot's rows, each starting with its label if it has one.
            text.lines()
                .filter(|r| r.contains('@'))
                .filter_map(|r| r.split_whitespace().next())
                .filter(|l| !l.starts_with('@'))
                .map(String::from)
                .collect()
        };
        let whole = PlotNumbers {
            x: AxisNumbers::default(),
            y: AxisNumbers {
                whole: true,
                ..AxisNumbers::default()
            },
        };
        let g = crate::glyphs::unicode();
        let modal = open_modal();
        let area = Rect::new(0, 0, 40, 12);
        let data = heatmap((0.0, 1.0));
        let text = plot_text_in(
            &modal,
            ChartRenderData::Heatmap {
                data: Some(&data),
                numbers: whole,
            },
            g,
            area,
        );
        assert_eq!(y_labels(&text), ["1", "0"], "{text}");
        let data = heatmap((0.0, 0.0126));
        let text = plot_text_in(
            &modal,
            ChartRenderData::Heatmap {
                data: Some(&data),
                numbers: PlotNumbers::default(),
            },
            g,
            area,
        );
        assert_eq!(y_labels(&text), ["0.010", "0.005", "0.000"], "{text}");
    }

    /// A count, or an integer column, ticks in whole numbers as the table prints
    /// them: never `4321.00`, and never a tick between two whole numbers.
    #[test]
    fn whole_number_axes_tick_whole_in_the_table_format() {
        use crate::chart_data::HistogramBin;
        use crate::numfmt::NumberFormat;
        let thousands = NumberFormat::preset("thousands").unwrap();
        let whole = AxisNumbers {
            format: thousands.clone(),
            whole: true,
        };
        let mut ctx = RenderContext::for_test();
        ctx.number_format.format = thousands.clone();
        let g = crate::glyphs::unicode();
        let area = Rect::new(0, 0, 40, 12);
        // What sits left of the y axis.
        let y_labels = |text: &str| -> Vec<String> {
            text.lines()
                .filter_map(|r| r.split_once(['│', '┤'])?.0.split_whitespace().next())
                .map(String::from)
                .collect()
        };
        let axis_row = |text: &str| {
            let rows: Vec<&str> = text.lines().collect();
            let axis = rows.iter().rposition(|r| r.contains('└')).expect(text);
            rows[axis + 1]
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
        };

        // Counts up the side; an integer column's values, 0 to 7, along the bottom.
        let histogram = HistogramData {
            column: "passengers".to_string(),
            bins: (0..7)
                .map(|i| HistogramBin {
                    center: f64::from(i) + 0.5,
                    count: [4321.0, 12.0, 900.0][i as usize % 3],
                })
                .collect(),
            x_min: 0.0,
            x_max: 7.0,
            max_count: 4321.0,
            rows: Default::default(),
            clipped: None,
        };
        let data = ChartRenderData::Histogram {
            data: Some(&histogram),
            x: whole.clone(),
        };
        let text = plot_text_with(&ctx, &open_modal(), data, g, area);
        // Up to the nice value past the tallest bar, in thousands-grouped whole steps.
        assert_eq!(y_labels(&text), ["5,000", "2,500", "0"], "{text}");
        // Four labels fit, so not the two of the step nearest the spacing.
        assert_eq!(axis_row(&text), "0 2 4 6", "{text}");

        // The same for an XY chart of integer columns.
        let series = vec![vec![(0.0, 3.0), (5.0, 10.0)]];
        let mut modal = open_modal();
        modal.x_column = Some("volume".to_string());
        modal.y_columns = vec!["price".to_string()];
        modal.show_legend = false;
        let xy = |numbers| ChartRenderData::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            numbers,
        };
        let numbers = PlotNumbers {
            x: whole.clone(),
            y: whole,
        };
        let text = plot_text_with(&ctx, &modal, xy(numbers), g, area);
        // 3 to 10 widens out to whole steps either side.
        assert_eq!(y_labels(&text), ["10", "5", "0"], "{text}");
        assert_eq!(axis_row(&text), "0 2 4", "{text}");
        // A float column ticks at the same nice values, written as exactly as they are.
        let text = plot_text_with(&ctx, &modal, xy(PlotNumbers::default()), g, area);
        assert_eq!(axis_row(&text), "0 2 4", "{text}");
    }

    /// Under the ASCII set every plot draws ASCII only, and still draws: its marks,
    /// its bars, its axes and its legend frame. The Unicode set keeps its own.
    #[test]
    fn every_plot_is_ascii_under_the_ascii_set() {
        use crate::chart_data::{BoxPlotStats, HistogramBin, KdeSeries};
        let (ascii, unicode) = (crate::glyphs::ascii(), crate::glyphs::unicode());
        let check = |what: &str, text: &str, marks: &[char]| {
            assert!(text.is_ascii(), "{what}:\n{text}");
            assert!(text.contains("+-"), "{what} has its axis corner:\n{text}");
            for mark in marks {
                assert!(text.contains(*mark), "{what} draws {mark:?}:\n{text}");
            }
        };

        let mut modal = open_modal();
        modal.x_column = Some("price".to_string());
        modal.y_columns = vec!["price".to_string(), "volume".to_string()];
        modal.show_legend = true;
        let series = vec![
            (0..20).map(|i| (i as f64, (i * i) as f64)).collect(),
            (0..20)
                .map(|i| (i as f64, 400.0 - (i * i) as f64))
                .collect(),
        ];
        let xy = |series| ChartRenderData::XY {
            series,
            breaks: None,
            values: None,
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            numbers: PlotNumbers::default(),
        };
        for (chart_type, mark) in [
            (ChartType::Line, '*'),
            (ChartType::Scatter, 'o'),
            (ChartType::Bar, '#'),
        ] {
            modal.chart_type = chart_type;
            let text = plot_text(&modal, xy(Some(&series)), ascii);
            check(chart_type.as_str(), &text, &[mark]);
            // The legend's frame, top right under the y title's row.
            assert!(text.lines().nth(1).unwrap().ends_with('+'), "{text}");
            let text = plot_text(&modal, xy(Some(&series)), unicode);
            assert!(text.contains('└') && text.contains('┐'), "{text}");
        }
        // Axes before the data is in.
        check("placeholder", &plot_text(&modal, xy(None), ascii), &[]);

        let histogram = HistogramData {
            column: "price".to_string(),
            bins: (0..10)
                .map(|i| HistogramBin {
                    center: i as f64 + 0.5,
                    count: (1 + i % 4) as f64,
                })
                .collect(),
            x_min: 0.0,
            x_max: 10.0,
            max_count: 4.0,
            rows: Default::default(),
            clipped: None,
        };
        let data = ChartRenderData::Histogram {
            data: Some(&histogram),
            x: AxisNumbers::default(),
        };
        check("histogram", &plot_text(&modal, data, ascii), &['#']);

        let box_plot = BoxPlotData {
            stats: ["price", "volume"]
                .iter()
                .map(|name| BoxPlotStats {
                    name: name.to_string(),
                    min: 0.0,
                    q1: 2.0,
                    median: 5.0,
                    q3: 7.0,
                    max: 10.0,
                })
                .collect(),
            y_min: 0.0,
            y_max: 10.0,
            rows: Default::default(),
            clipped: None,
        };
        let data = ChartRenderData::BoxPlot {
            data: Some(&box_plot),
            y: AxisNumbers::default(),
        };
        check("box plot", &plot_text(&modal, data, ascii), &['o']);

        let kde = KdeData {
            series: vec![KdeSeries {
                name: "price".to_string(),
                points: (0..=100)
                    .map(|i| {
                        let x = i as f64 / 10.0 - 5.0;
                        (x, (-x * x / 2.0).exp())
                    })
                    .collect(),
            }],
            x_min: -5.0,
            x_max: 5.0,
            y_max: 1.0,
            rows: Default::default(),
            clipped: None,
        };
        let data = ChartRenderData::Kde {
            data: Some(&kde),
            x: AxisNumbers::default(),
        };
        check("KDE", &plot_text(&modal, data, ascii), &['*']);

        // Bars draw no axes of their own.
        let bars = bar_data(5);
        let text = plot_text(&modal, ChartRenderData::Bar { data: Some(&bars) }, ascii);
        assert!(text.is_ascii() && text.contains('#'), "bars:\n{text}");
    }

    /// The legend reads clean over a full plot: a short name's row is blank past
    /// the name, not the marks behind it.
    #[test]
    fn the_legend_hides_the_plot_behind_it() {
        let mut modal = open_modal();
        modal.x_column = Some("price".to_string());
        modal.y_columns = vec!["price".to_string(), "volume".to_string()];
        modal.show_legend = true;
        modal.chart_type = ChartType::Bar;
        // Bars in every column fill the plot, legend corner included.
        let series: Vec<Vec<(f64, f64)>> = vec![(0..120).map(|i| (i as f64, 100.0)).collect(); 2];
        for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
            let text = plot_text(
                &modal,
                ChartRenderData::XY {
                    series: Some(&series),
                    breaks: None,
                    values: None,
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    numbers: PlotNumbers::default(),
                },
                g,
            );
            let rows: Vec<Vec<char>> = text.lines().map(|l| l.chars().collect()).collect();
            let corner = g.plot.axis.top_left.chars().next().unwrap();
            // The frame's first row is under the y title's: its corner is the one a
            // rule runs right from, not the axis's tick mark.
            let rule = g.plot.axis.horizontal.chars().next().unwrap();
            let left = rows[1]
                .windows(2)
                .position(|w| w[0] == corner && w[1] == rule)
                .expect(&text);
            let interior = |y: usize| {
                rows[y][left + 1..rows[y].len() - 1]
                    .iter()
                    .collect::<String>()
            };
            assert_eq!(interior(2), "price ", "{text}");
            assert_eq!(interior(3), "volume", "{text}");
        }
    }

    /// Ten years of daily dates against a value, as the XY view gets them.
    fn decade() -> Vec<Vec<(f64, f64)>> {
        // 2015-01-01 on, in days since the epoch.
        vec![
            (0..3653)
                .map(|i| (16436.0 + f64::from(i), 1000.0 + f64::from(i % 400) * 6.0))
                .collect(),
        ]
    }

    fn xy_dates(series: &Vec<Vec<(f64, f64)>>) -> ChartRenderData<'_> {
        ChartRenderData::XY {
            series: Some(series),
            breaks: None,
            values: None,
            x_axis_kind: XAxisTemporalKind::Date,
            x_bounds: None,
            numbers: PlotNumbers::default(),
        }
    }

    /// On a 300-column terminal a line chart labels its x axis 15 to 20 times, on
    /// calendar boundaries, and its y axis about once per four rows, at round values.
    #[test]
    fn a_wide_chart_carries_ticks_scaled_to_the_space() {
        let mut modal = open_modal();
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["price".to_string()];
        let series = decade();
        let rows = render_view(
            &mut modal,
            ChartView {
                data: xy_dates(&series),
                notes: Vec::new(),
                error: None,
            },
            300,
            60,
        );
        // The canvas, right of the sidebar.
        let rows: Vec<String> = rows
            .iter()
            .map(|r| r.chars().skip(SIDEBAR_WIDTH as usize).collect())
            .collect();
        let axis = rows.iter().rposition(|r| r.contains('└')).unwrap();
        let x_labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
        assert!(
            (15..=20).contains(&x_labels.len()),
            "{} x labels: {x_labels:?}",
            x_labels.len()
        );
        assert!(
            x_labels
                .iter()
                .all(|l| l.len() == 4 || ["Apr", "Jul", "Oct"].contains(l)),
            "{x_labels:?}"
        );
        let y_labels: Vec<f64> = rows[..axis]
            .iter()
            .filter_map(|r| r.split_once('┤')?.0.split_whitespace().last()?.parse().ok())
            .collect();
        let plot_rows = axis - 2; // the tab line and the y title
        let per_label = plot_rows as f64 / y_labels.len() as f64;
        assert!(
            (3.0..=5.0).contains(&per_label),
            "{per_label}: {y_labels:?}"
        );
        let step = y_labels[0] - y_labels[1];
        let mantissa = step / 10f64.powf(step.log10().floor());
        assert!([1.0, 2.0, 2.5, 5.0].contains(&mantissa), "{y_labels:?}");
        assert!(
            y_labels.windows(2).all(|w| w[0] - w[1] == step)
                && y_labels.iter().all(|v| v % step == 0.0),
            "{y_labels:?}"
        );
    }

    /// The grid shows in the view only while it is on, in the `chart_grid` color.
    #[test]
    fn the_grid_follows_the_toggle() {
        let g = crate::glyphs::unicode();
        let mut modal = open_modal();
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["price".to_string()];
        let series = decade();
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("default theme colors must resolve");
        let draw = |modal: &ChartModal| {
            let area = Rect::new(0, 0, 100, 30);
            let mut buf = Buffer::empty(area);
            let ctx = RenderContext::for_test();
            render_plot(area, &mut buf, modal, &theme, &ctx, xy_dates(&series), g);
            buf
        };
        let grid_cells = |buf: &Buffer| {
            buf.content()
                .iter()
                .filter(|c| c.symbol() == g.plot.grid_down || c.symbol() == g.plot.grid_across)
                .collect::<Vec<_>>()
                .len()
        };
        assert_eq!(grid_cells(&draw(&modal)), 0);
        modal.toggle_grid();
        let on = draw(&modal);
        assert!(grid_cells(&on) > 100);
        let grid = theme.get("chart_grid");
        assert!(
            on.content()
                .iter()
                .filter(|c| c.symbol() == g.plot.grid_across)
                .all(|c| c.fg == grid)
        );
    }

    /// The light theme's grid stays a color under 16 colors, not the white of the
    /// background, and the grid draws in it.
    #[test]
    fn the_light_grid_survives_sixteen_colors() {
        use ratatui::style::Color;
        let g = crate::glyphs::unicode();
        let sixteen = |hex: &str| {
            let channel = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).unwrap();
            crate::config::rgb_to_basic_ansi(channel(1), channel(3), channel(5))
        };
        let light = crate::config::ColorConfig::light();
        let grid = sixteen(&light.chart_grid);
        assert!(
            !matches!(grid, Color::White | Color::Black | Color::Reset),
            "{grid:?}"
        );
        let dark = sixteen(&crate::config::ColorConfig::default().chart_grid);
        assert!(!matches!(dark, Color::White | Color::Black), "{dark:?}");

        let mut theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        theme.colors.insert("chart_grid".to_string(), grid);
        let mut modal = open_modal();
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["price".to_string()];
        modal.toggle_grid();
        let series = decade();
        let area = Rect::new(0, 0, 80, 24);
        let mut buf = Buffer::empty(area);
        let ctx = RenderContext::for_test();
        render_plot(area, &mut buf, &modal, &theme, &ctx, xy_dates(&series), g);
        let cells: Vec<_> = buf
            .content()
            .iter()
            .filter(|c| c.symbol() == g.plot.grid_across || c.symbol() == g.plot.grid_down)
            .collect();
        assert!(cells.len() > 50);
        assert!(cells.iter().all(|c| c.fg == grid));
    }

    fn rows_of(buf: &Buffer) -> Vec<String> {
        let area = buf.area;
        (0..area.height)
            .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect())
            .collect()
    }

    /// A log scale ticks at the powers of ten, and 2 and 5 between when there is
    /// room, every label in one format; the short form names each tick's own k or M.
    #[test]
    fn log_scale_ticks_fall_on_the_decades() {
        let g = crate::glyphs::unicode();
        let mut modal = open_modal();
        modal.x_column = Some("x".to_string());
        modal.y_columns = vec!["count".to_string()];
        modal.log_scale = true;
        // 0 to 300,000, as the view has it: ln(1 + y).
        let linear: Vec<Vec<(f64, f64)>> = vec![
            (0..=300)
                .map(|i| (f64::from(i), f64::from(i).powi(2) * 3.333))
                .collect(),
        ];
        let logged = crate::chart_jobs::log_series(&linear);
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let ctx = RenderContext::for_test();
        let y_labels = |height: u16| {
            let area = Rect::new(0, 0, 60, height);
            let mut buf = Buffer::empty(area);
            let data = ChartRenderData::XY {
                series: Some(&logged),
                breaks: None,
                values: Some(&linear),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                numbers: PlotNumbers::default(),
            };
            render_plot(area, &mut buf, &modal, &theme, &ctx, data, g);
            let rows = rows_of(&buf);
            let labels: Vec<String> = rows
                .iter()
                .filter_map(|row| {
                    let (label, _) = row.split_once(g.plot.tick_y)?;
                    let label = label.trim();
                    (!label.is_empty()).then(|| label.to_string())
                })
                .collect();
            (labels, rows)
        };
        // Top down: falling, each a 1, 2 or 5 a power of ten up, or zero.
        let nice = |labels: &[String]| {
            let values: Vec<f64> = labels.iter().map(|l| l.parse().unwrap()).collect();
            values.windows(2).all(|w| w[0] > w[1])
                && values.iter().all(|&v| {
                    let decade = 10f64.powf(v.log10().floor());
                    v == 0.0 || [1.0, 2.0, 5.0].contains(&(v / decade))
                })
        };
        let (tall, rows) = y_labels(40);
        assert!(nice(&tall), "{rows:#?}");
        // Every power of ten up to the top, which the axis widens to.
        for decade in ["0", "10", "100", "1000", "10000", "100000", "1000000"] {
            assert!(tall.contains(&decade.to_string()), "{decade}: {rows:#?}");
        }
        assert_eq!(tall[0], "1000000", "{rows:#?}");
        assert_eq!(tall.last().unwrap(), "0", "{rows:#?}");
        let (short, rows) = y_labels(12);
        assert!(nice(&short), "{rows:#?}");
        assert!(short.len() >= 3 && short.len() < tall.len(), "{rows:#?}");
    }

    /// With the plot focused the crosshair stands on a point: a line down its column
    /// in the accent, and under the plot each series' value there.
    #[test]
    fn the_crosshair_reads_out_every_series() {
        let mut modal = open_modal();
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["price".to_string(), "volume".to_string()];
        let series: Vec<Vec<(f64, f64)>> = vec![
            (0..10)
                .map(|i| (19_783.0 + f64::from(i), 1.5 * f64::from(i)))
                .collect(),
            (0..10)
                .filter(|i| *i != 4)
                .map(|i| (19_783.0 + f64::from(i), f64::from(i * 100)))
                .collect(),
        ];
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let ctx = RenderContext::for_test();
        let draw = |modal: &ChartModal, g: &Glyphs| {
            let area = Rect::new(0, 0, 60, 20);
            let mut buf = Buffer::empty(area);
            let data = ChartRenderData::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                x_axis_kind: XAxisTemporalKind::Date,
                x_bounds: None,
                numbers: PlotNumbers::default(),
            };
            let place = render_plot(area, &mut buf, modal, &theme, &ctx, data, g);
            (buf, place)
        };
        for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
            // The options have the keys: no crosshair, no readout.
            modal.plot_focus = false;
            modal.cursor_x = Some(19_786.0);
            let (off, place) = draw(&modal, g);
            let place = place.expect("an XY plot with points says where it is");
            assert!(!rows_of(&off).concat().contains("price:"));

            modal.plot_focus = true;
            let (on, _) = draw(&modal, g);
            let rows = rows_of(&on);
            let last = rows.last().unwrap().trim_end();
            assert_eq!(
                last, "date: 2024-03-04   price: 4.5   volume: 300",
                "{rows:#?}"
            );
            let column = place.column(19_786.0);
            let accent = theme.get("accent");
            let marks = (place.graph.top()..place.graph.bottom())
                .filter(|&y| on[(column, y)].symbol() == g.plot.axis.vertical)
                .inspect(|&y| assert_eq!(on[(column, y)].fg, accent))
                .count();
            assert!(marks > 5, "{rows:#?}");

            // A gap in a series reads as one.
            modal.cursor_x = Some(19_787.0);
            let (gap, _) = draw(&modal, g);
            let rows = rows_of(&gap);
            assert!(
                rows.last()
                    .unwrap()
                    .contains(&format!("volume: {}", g.null)),
                "{rows:#?}"
            );
        }
    }

    /// A scatter of a few points marks each with a whole-cell dot; a dense one
    /// switches to braille, which keeps neighbors apart.
    #[test]
    fn a_scatter_picks_its_marker_by_density() {
        let g = crate::glyphs::unicode();
        let mut modal = open_modal();
        modal.x_column = Some("price".to_string());
        modal.y_columns = vec!["volume".to_string()];
        modal.chart_type = ChartType::Scatter;
        let draw = |n: usize| {
            let series = vec![
                (0..n)
                    .map(|i| (i as f64, ((i * 7919) % 1000) as f64))
                    .collect::<Vec<_>>(),
            ];
            let data = ChartRenderData::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                numbers: PlotNumbers::default(),
            };
            plot_text_in(&modal, data, g, Rect::new(0, 0, 60, 20))
        };
        let braille = |text: &str| text.chars().any(|c| ('\u{2801}'..='\u{28ff}').contains(&c));
        let sparse = draw(20);
        assert!(
            sparse.contains(ratatui::symbols::DOT) && !braille(&sparse),
            "{sparse}"
        );
        let dense = draw(2000);
        assert!(
            braille(&dense) && !dense.contains(ratatui::symbols::DOT),
            "{dense}"
        );
    }

    /// A histogram's bins fill their columns, with a column of air between bins wide
    /// enough to spare one.
    #[test]
    fn histogram_bins_fill_their_columns() {
        use crate::chart_data::HistogramBin;
        let histogram = HistogramData {
            column: "price".to_string(),
            bins: (0..4)
                .map(|i| HistogramBin {
                    center: f64::from(i) * 25.0 + 12.5,
                    count: 10.0,
                })
                .collect(),
            x_min: 0.0,
            x_max: 100.0,
            max_count: 10.0,
            rows: Default::default(),
            clipped: None,
        };
        let points = bin_columns(&histogram, [0.0, 100.0], 41);
        // Four bins of about ten columns, less a gap after each but the last.
        assert_eq!(points.len(), 41 - 3, "{points:?}");
        let text = plot_text_in(
            &open_modal(),
            ChartRenderData::Histogram {
                data: Some(&histogram),
                x: AxisNumbers::default(),
            },
            crate::glyphs::unicode(),
            Rect::new(0, 0, 60, 20),
        );
        let row = text.lines().find(|r| r.contains('█')).expect(&text);
        let bars: Vec<&str> = row.split(' ').filter(|r| r.contains('█')).collect();
        assert_eq!(bars.len(), 4, "{text}");
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

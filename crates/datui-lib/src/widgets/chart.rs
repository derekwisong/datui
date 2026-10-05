//! Chart view widget: the panel of shelves on the left (Type, X, Y, Color, then the
//! options), the plot on the right under its title, and the shared Picker over
//! them while a shelf is edited.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Chart, Clear, Dataset, GraphType, Paragraph, Widget, Wrap},
};

use crate::chart_data::{
    AxisNumbers, BarData, BoxPlotData, HeatmapData, HistogramData, KdeData, XAxisTemporalKind,
    segments,
};
use crate::chart_modal::{
    Aggregate, ChartFocus, ChartModal, Cumulative, Mark, PickerFor, ShelfUse, TimeUnit,
};
use crate::config::Theme;
use crate::glyphs::Glyphs;
use crate::pointer::Hit;
use crate::render::context::RenderContext;
use crate::widgets::axes::{
    AxisSpec, Legend, PlotAxes, Track, cut, fit_x_labels, fit_y_labels, resolution,
};
use crate::widgets::crosshair::{self, PlotPlace};
use crate::widgets::ui::{Picker, SectionRule, Surface, Working};
use polars::prelude::Schema;
use unicode_width::UnicodeWidthStr;

/// The panel's width, at most: the label column, the value column, and air.
const SIDEBAR_WIDTH: u16 = 40;
/// Where the value column starts, past the rail gutter: the longest label,
/// "Y from zero", plus air.
const LABEL_WIDTH: u16 = 13;
const HEATMAP_TITLE_HEIGHT: u16 = 1;
const HEATMAP_X_LABEL_HEIGHT: u16 = 2;

/// What the chart area shows: the plot, the notes over it about its input, or the
/// reason it could not be prepared.
pub struct ChartView<'a> {
    pub data: ChartRenderData<'a>,
    /// Dimmed at the right of the title row: a sample, values a range left out.
    pub notes: Vec<String>,
    /// Preparing the selection failed; shown in place of an empty plot.
    pub error: Option<&'a str>,
    /// The selection is being prepared: said over the chart standing in for it, or in
    /// place of a plot when there is none.
    pub working: Option<Working<'a>>,
    /// The view's schema, so column names take their type's color.
    pub schema: Option<&'a Schema>,
}

pub enum ChartRenderData<'a> {
    XY {
        series: Option<&'a Vec<Vec<(f64, f64)>>>,
        /// Per series, where its line starts again after a gap.
        breaks: Option<&'a Vec<Vec<usize>>>,
        /// The series before any log, for the crosshair's readout; `None` reads
        /// `series`.
        values: Option<&'a Vec<Vec<(f64, f64)>>>,
        /// Each series' name: its Y column or its color group.
        names: &'a [String],
        x_axis_kind: XAxisTemporalKind,
        x_bounds: Option<(f64, f64)>,
        numbers: PlotNumbers,
        /// The last series is Other, drawn in `dimmed` under the rest.
        other: bool,
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

impl ChartRenderData<'_> {
    /// Whether there is a plot to draw: data, or a line chart's axes, which stand
    /// empty until their series arrive.
    fn draws_plot(&self) -> bool {
        match self {
            Self::XY { .. } => true,
            Self::Histogram { data, .. } => data.is_some(),
            Self::BoxPlot { data, .. } => data.is_some(),
            Self::Kde { data, .. } => data.is_some(),
            Self::Heatmap { data, .. } => data.is_some(),
            Self::Bar { data } => data.is_some(),
        }
    }
}

/// What each axis holds, so its ticks print as the table prints its columns: whole
/// for an integer column, grouped and separated in the table's number format.
#[derive(Default)]
pub struct PlotNumbers {
    pub x: AxisNumbers,
    pub y: AxisNumbers,
}

/// One line of the panel.
enum PanelLine {
    Blank,
    Rule(&'static str),
    /// A shelf or an option: its label, its value, and the field it edits.
    Row {
        label: &'static str,
        value: Vec<Span<'static>>,
        field: Option<ChartFocus>,
        dimmed: bool,
    },
}

/// The style a column name takes: its type's color.
fn column_span(name: &str, schema: Option<&Schema>, ctx: &RenderContext) -> Span<'static> {
    let color = schema
        .and_then(|s| s.get(name))
        .map(|dtype| ctx.type_color(dtype))
        .unwrap_or(ctx.text_primary);
    Span::styled(name.to_string(), Style::default().fg(color))
}

fn plain(text: impl Into<String>, ctx: &RenderContext) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(ctx.text_primary))
}

fn quiet(text: impl Into<String>, ctx: &RenderContext) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(ctx.dimmed))
}

/// The columns of a shelf, joined, each in its type's color.
fn columns_spans(
    names: &[String],
    schema: Option<&Schema>,
    ctx: &RenderContext,
) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            spans.push(quiet(", ", ctx));
        }
        spans.push(column_span(name, schema, ctx));
    }
    spans
}

/// The panel's lines for the chart on screen: every shelf, dimmed where the type
/// does not use it, each with the line under it, then the options.
fn panel_lines(modal: &ChartModal, schema: Option<&Schema>, ctx: &RenderContext) -> Vec<PanelLine> {
    let g = crate::glyphs::get();
    let spec = &modal.spec;
    let mark = spec.mark;
    let encoding = &spec.encoding;
    let row = |label, value, field: Option<ChartFocus>, dimmed| PanelLine::Row {
        label,
        value,
        field,
        dimmed,
    };
    let sub = |value, field: Option<ChartFocus>, dimmed| PanelLine::Row {
        label: "",
        value,
        field,
        dimmed,
    };
    let pick = |what: &str| vec![quiet(format!("pick {what}"), ctx)];
    let mut lines = vec![PanelLine::Rule("Chart"), PanelLine::Blank];

    // Type.
    lines.push(row(
        "Type",
        vec![plain(mark.label(), ctx)],
        Some(ChartFocus::Type),
        false,
    ));
    if let Some(dtype) = &modal.suggested {
        lines.push(sub(
            vec![quiet(format!("suggested for {dtype}"), ctx)],
            None,
            false,
        ));
    }
    lines.push(PanelLine::Blank);

    // X.
    let x_value = match encoding.x.field.as_deref() {
        Some(x) => vec![column_span(x, schema, ctx)],
        None if mark == Mark::Box => vec![quiet(crate::chart_modal::NONE_ITEM, ctx)],
        None if mark == Mark::Bar => pick("a category"),
        None => pick("a column"),
    };
    lines.push(row("X", x_value, Some(ChartFocus::X), false));
    let rows = modal.row_order();
    if rows.contains(&ChartFocus::TimeUnit) {
        let unit = encoding.x.time_unit;
        let value = if unit == TimeUnit::None {
            vec![quiet("by row", ctx)]
        } else {
            vec![quiet("by ", ctx), plain(unit.label(), ctx)]
        };
        lines.push(sub(value, Some(ChartFocus::TimeUnit), false));
    }
    if mark == Mark::Bar {
        let order = match modal.bar_order {
            crate::chart_data::BarOrder::Value => format!("by value {}", g.sort_desc),
            crate::chart_data::BarOrder::Label => format!("by label {}", g.sort_asc),
        };
        lines.push(sub(vec![plain(order, ctx)], Some(ChartFocus::Order), false));
    }
    if mark == Mark::Histogram {
        let mut value = vec![plain(format!("{} bins", modal.hist_bins), ctx)];
        if modal.value_range != crate::chart_data::ValueRange::All {
            value.push(quiet(
                format!(" {} {}", g.middot, modal.value_range.label()),
                ctx,
            ));
        }
        lines.push(sub(value, Some(ChartFocus::Bins), false));
    }
    lines.push(PanelLine::Blank);

    // Y.
    match (mark, modal.y_use()) {
        (_, ShelfUse::Dimmed(why)) => lines.push(row("Y", vec![quiet(why, ctx)], None, true)),
        (Mark::Histogram, _) => {
            let value = if modal.share {
                if modal.colored() {
                    "share of group"
                } else {
                    "share"
                }
            } else {
                "count"
            };
            lines.push(row(
                "Y",
                vec![plain(value, ctx)],
                Some(ChartFocus::Y),
                false,
            ));
        }
        _ => {
            let value = if encoding.y.aggregate == Aggregate::Count {
                vec![plain("rows", ctx)]
            } else if encoding.y.field.is_empty() {
                pick("a column")
            } else {
                columns_spans(&encoding.y.field, schema, ctx)
            };
            lines.push(row("Y", value, Some(ChartFocus::Y), false));
        }
    }
    if modal.takes_aggregate() {
        // With cumulative on the rows run as a total and the aggregate waits.
        let value = match encoding.y.cumulative {
            // First and last say which order the rows are read in.
            Cumulative::Off if encoding.y.aggregate.follows_row_order() => {
                let order = modal.row_order.as_deref().unwrap_or("row order");
                vec![
                    plain(encoding.y.aggregate.label(), ctx),
                    quiet(format!(" {} by {order}", g.middot), ctx),
                ]
            }
            Cumulative::Off => vec![plain(encoding.y.aggregate.label(), ctx)],
            _ if encoding.y.aggregate == Aggregate::Count => vec![plain("running count", ctx)],
            how => vec![
                quiet(encoding.y.aggregate.label(), ctx),
                quiet(format!(" {} ", g.middot), ctx),
                plain(how.label(), ctx),
                quiet(" of rows", ctx),
            ],
        };
        // A row of its own: unlabeled under Y, `none` read as a second Y column.
        lines.push(row("Aggregate", value, Some(ChartFocus::Aggregate), false));
        if rows.contains(&ChartFocus::Quantile) {
            let p = format!("p{}", encoding.y.quantile());
            lines.push(sub(vec![plain(p, ctx)], Some(ChartFocus::Quantile), false));
        }
    }
    lines.push(PanelLine::Blank);

    // Color.
    match modal.color_use() {
        ShelfUse::Dimmed(why) => {
            let value = match encoding.color.field.as_deref() {
                Some(c) => c.to_string(),
                None => crate::chart_modal::NONE_ITEM.to_string(),
            };
            lines.push(row("Color", vec![quiet(value, ctx)], None, true));
            lines.push(sub(vec![quiet(why, ctx)], None, true));
        }
        ShelfUse::Used => {
            let value = match encoding.color.field.as_deref() {
                Some(c) => vec![column_span(c, schema, ctx)],
                None => vec![quiet(crate::chart_modal::NONE_ITEM, ctx)],
            };
            lines.push(row("Color", value, Some(ChartFocus::Color), false));
            if encoding.color.field.is_some() {
                lines.push(sub(
                    color_values_line(modal, ctx),
                    Some(ChartFocus::ColorValues),
                    false,
                ));
            }
        }
    }
    lines.push(PanelLine::Blank);

    // Options.
    lines.push(PanelLine::Rule("Options"));
    lines.push(PanelLine::Blank);
    let on_off = |on: bool| plain(if on { "on" } else { "off" }, ctx);
    for field in rows.iter().copied() {
        let (label, value) = match field {
            ChartFocus::Cumulative => ("Cumulative", plain(encoding.y.cumulative.label(), ctx)),
            ChartFocus::Bins if mark == Mark::Heatmap => {
                ("Bins", plain(modal.heatmap_bins.to_string(), ctx))
            }
            ChartFocus::Bandwidth => (
                "Bandwidth",
                plain(format!("{:.1}x", modal.kde_bandwidth_factor), ctx),
            ),
            ChartFocus::Range => ("Range", plain(modal.value_range.label(), ctx)),
            ChartFocus::YStartsAtZero => ("Y from zero", on_off(modal.y_starts_at_zero)),
            ChartFocus::LogScale => ("Log scale", on_off(modal.log_scale)),
            ChartFocus::ShowLegend => (
                "Legend",
                plain(if modal.show_legend { "auto" } else { "off" }, ctx),
            ),
            ChartFocus::Grid => ("Grid", on_off(modal.grid)),
            ChartFocus::LimitRows => ("Rows", plain(rows_value(modal), ctx)),
            _ => continue,
        };
        lines.push(row(label, vec![value], Some(field), false));
    }
    if modal.aggregates() {
        lines.push(row("Rows", vec![quiet("all, exact", ctx)], None, false));
    }
    lines
}

/// The Rows option: how many rows a chart that samples reads.
fn rows_value(modal: &ChartModal) -> String {
    match modal.row_limit {
        None => "every row".to_string(),
        Some(_) => format!("sample {}", modal.row_limit_display()),
    }
}

/// The line under Color: which of the column's values have a series, and with
/// Other on, how many values it gathers: `top 10 + 6 other`.
fn color_values_line(modal: &ChartModal, ctx: &RenderContext) -> Vec<Span<'static>> {
    let picked = &modal.spec.encoding.color.values;
    let Some(counts) = modal
        .color_counts
        .as_ref()
        .filter(|_| modal.has_color_counts())
    else {
        return vec![quiet("counting values", ctx)];
    };
    let total = counts.values.len();
    let of = crate::numfmt::group_chrome(total);
    let (drawn, rest) = if picked.is_empty() {
        let drawn = total.min(modal.series_max());
        (format!("top {drawn}"), total - drawn)
    } else {
        let rest = counts
            .values
            .iter()
            .filter(|(v, _)| !picked.contains(v))
            .count();
        (format!("{} picked", picked.len()), rest)
    };
    if picked.is_empty() && rest == 0 {
        return vec![plain(format!("all {total}"), ctx)];
    }
    // With Other every value is drawn: the rest is counted, not the whole.
    if modal.shows_other() && rest > 0 {
        let rest = crate::numfmt::group_chrome(rest);
        return vec![plain(drawn, ctx), plain(format!(" + {rest} other"), ctx)];
    }
    let by_rows = if picked.is_empty() { " by rows" } else { "" };
    vec![plain(drawn, ctx), quiet(format!(" of {of}{by_rows}"), ctx)]
}

/// The panel: its lines, with the blank ones given up first when the height runs
/// out, and scrolled to keep the focused row on screen. Returns where each field
/// was drawn, for the Picker to drop from.
fn render_sidebar(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    schema: Option<&Schema>,
    ctx: &RenderContext,
) -> Option<Rect> {
    if area.height == 0 || area.width < 4 {
        return None;
    }
    let mut lines = panel_lines(modal, schema, ctx);
    if lines.len() > area.height as usize {
        lines.retain(|l| !matches!(l, PanelLine::Blank));
    }
    let focus = (!modal.plot_focus).then_some(modal.focus);
    let at = lines
        .iter()
        .position(
            |l| matches!(l, PanelLine::Row { field, .. } if *field == focus && focus.is_some()),
        )
        .unwrap_or(0);
    let height = area.height as usize;
    let first = at.saturating_sub(height.saturating_sub(1));
    let g = crate::glyphs::get();
    let mut focused_at = None;
    for (i, line) in lines.iter().enumerate().skip(first).take(height) {
        let y = area.y + (i - first) as u16;
        let row = Rect {
            y,
            height: 1,
            ..area
        };
        match line {
            PanelLine::Blank => {}
            PanelLine::Rule(title) => SectionRule {
                title,
                chip: None,
                focused: false,
            }
            .render(
                Rect {
                    x: area.x + 1,
                    width: area.width - 1,
                    ..row
                },
                buf,
                ctx,
            ),
            PanelLine::Row {
                label,
                value,
                field,
                dimmed,
            } => {
                let focused = field.is_some() && *field == focus;
                if let Some(field) = field {
                    crate::pointer::record_field::<ChartModal>(row, *field);
                }
                if focused {
                    focused_at = Some(row);
                    buf.set_string(area.x, y, g.rail, Style::default().fg(ctx.accent));
                }
                let label_style = if *dimmed {
                    Style::default().fg(ctx.dimmed)
                } else if focused {
                    Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(ctx.label)
                };
                let label_area = Rect {
                    x: area.x + 2,
                    width: LABEL_WIDTH.min(area.width.saturating_sub(2)),
                    ..row
                };
                Paragraph::new(*label)
                    .style(label_style)
                    .render(label_area, buf);
                let value_x = area.x + 2 + LABEL_WIDTH;
                if value_x < area.right() {
                    let spans: Vec<Span> = if *dimmed {
                        value
                            .iter()
                            .map(|s| {
                                Span::styled(s.content.clone(), Style::default().fg(ctx.dimmed))
                            })
                            .collect()
                    } else {
                        value.clone()
                    };
                    Paragraph::new(Line::from(spans)).render(
                        Rect {
                            x: value_x,
                            width: area.right() - value_x,
                            ..row
                        },
                        buf,
                    );
                }
            }
        }
    }
    focused_at
}

/// The open Picker, over the panel and the plot under the row it edits: one
/// Surface, the narrowing filter on its first line, then the list.
fn render_picker(
    area: Rect,
    anchor: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    ctx: &RenderContext,
) {
    let Some(state) = &modal.picker else {
        return;
    };
    // It owns the keys, drawn or not: the panel's rows take no clicks.
    crate::pointer::record(area, Hit::Picker);
    let title = match modal.picker_for {
        Some(PickerFor::X) => "X",
        Some(PickerFor::Y) => "Y",
        Some(PickerFor::Color) => "Color",
        Some(PickerFor::ColorValues) => modal.color().map(String::as_str).unwrap_or("Values"),
        None => "",
    };
    let filtered = state.filtered();
    let widest = filtered
        .iter()
        .map(|(_, item)| UnicodeWidthStr::width(*item))
        .max()
        .unwrap_or(0)
        .max(14) as u16;
    let detail_w = modal
        .picker_details
        .iter()
        .map(|d| d.len())
        .max()
        .unwrap_or(0) as u16;
    let width = (widest + detail_w + 9).clamp(28, 44).min(area.width);
    let x = (anchor.x + 2 + LABEL_WIDTH.saturating_sub(4)).min(area.right().saturating_sub(width));
    let y = anchor.y + 1;
    let height = u16::try_from(filtered.len())
        .unwrap_or(u16::MAX)
        .saturating_add(5)
        .min(area.bottom().saturating_sub(y))
        .max(5);
    let y = y.min(area.bottom().saturating_sub(height));
    let frame = Rect {
        x,
        y,
        width,
        height: height.min(area.height),
    };
    Clear.render(frame, buf);
    let content = Surface::new(title).render(frame, buf, ctx);
    if content.height == 0 {
        return;
    }
    let filter = if state.filter.is_empty() {
        quiet("type to narrow", ctx)
    } else {
        plain(state.filter.clone(), ctx)
    };
    let g = crate::glyphs::get();
    Paragraph::new(Line::from(vec![
        quiet(format!("{} ", g.prompt), ctx),
        filter,
    ]))
    .render(
        Rect {
            height: 1,
            ..content
        },
        buf,
    );
    let mut list = Rect {
        y: content.y + 1,
        height: content.height.saturating_sub(1),
        ..content
    };
    if modal.picker_for == Some(PickerFor::ColorValues)
        && modal.spec.encoding.color.values.is_empty()
        && list.height > 1
    {
        Paragraph::new(quiet(
            format!("default: top {} by rows", modal.series_max()),
            ctx,
        ))
        .render(Rect { height: 1, ..list }, buf);
        list.y += 1;
        list.height -= 1;
    }
    let mut picker = Picker::from_state(state, true);
    if modal.picker_multi() {
        let marks = filtered.iter().map(|(i, _)| modal.is_marked(*i)).collect();
        picker = picker.marks(marks);
    }
    if !modal.picker_details.is_empty() {
        let details = filtered
            .iter()
            .map(|(i, _)| modal.picker_details.get(*i).cloned().unwrap_or_default())
            .collect();
        picker = picker.details(details);
    }
    picker.render(list, buf, ctx);
}

/// The fewest cells the notes are cut to; with less room they are left out.
const NOTE_MIN: usize = 8;

/// The plot's title row: how the chart was made of the rows at the left, what it
/// says of the rows it read (a sample, values left out) at the right. The columns
/// are named at their axes. The how keeps its room; the notes are cut to what is
/// left, or left out when hardly any is. A row with nothing to say stays blank, so
/// the plot does not move when it gets something.
fn render_title(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    notes: &[String],
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let width = area.width as usize;
    let how = cut(&modal.how(), width, g);
    buf.set_string(
        area.x,
        area.y,
        &how,
        Style::default().fg(ctx.text_secondary),
    );
    let used = how.width();
    // Two cells of air after the how.
    let room = width.saturating_sub(if used > 0 { used + 2 } else { 0 });
    let notes = notes.join(&format!(" {} ", g.middot));
    if notes.is_empty() || room < NOTE_MIN.min(notes.width()) {
        return;
    }
    let notes = cut(&notes, room, g);
    let x = area.right() - notes.width() as u16;
    buf.set_string(x, area.y, &notes, Style::default().fg(ctx.dimmed));
}

/// Renders the chart view: the panel, a rule, and the plot under its title.
pub fn render_chart_view(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ChartModal,
    theme: &Theme,
    ctx: &RenderContext,
    view: ChartView<'_>,
) {
    // The panel caps its share of the width, so a narrow terminal still keeps a
    // plot.
    let sidebar_width = SIDEBAR_WIDTH.min(area.width / 2);
    let [sidebar, rule, plot_area] = Layout::horizontal([
        Constraint::Length(sidebar_width),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .areas(area);
    let anchor = render_sidebar(sidebar, buf, modal, view.schema, ctx);
    let g = crate::glyphs::get();
    for y in rule.top()..rule.bottom() {
        buf.set_string(rule.x, y, g.rule, Style::default().fg(ctx.column_separator));
    }
    // A cell of air beside the rule.
    let plot_area = Rect {
        x: plot_area.x + 1,
        width: plot_area.width.saturating_sub(1),
        ..plot_area
    };
    let [title, chart_inner] =
        Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(plot_area);
    let notes: &[String] = if view.error.is_some() {
        &[]
    } else {
        &view.notes
    };
    render_title(title, buf, modal, notes, ctx);

    modal.plot = None;
    if let Some(message) = view.error {
        Paragraph::new(message)
            .style(Style::default().fg(ctx.error))
            .wrap(Wrap { trim: true })
            .centered()
            .render(chart_inner, buf);
    } else {
        match view.working {
            Some(working) if !view.data.draws_plot() => {
                working.render_centered(chart_inner, buf, ctx)
            }
            working => {
                modal.plot = render_plot(
                    chart_inner,
                    buf,
                    modal,
                    theme,
                    ctx,
                    view.data,
                    crate::glyphs::get(),
                );
                if let Some(working) = working {
                    working.render_corner(chart_inner, buf, ctx);
                }
            }
        }
    }
    if let Some(anchor) = anchor
        && modal.picker.is_some()
    {
        render_picker(area, anchor, buf, modal, ctx);
    }
}

/// What the plot asks for while a shelf it needs is empty.
fn missing(modal: &ChartModal) -> &'static str {
    let encoding = &modal.spec.encoding;
    match modal.mark() {
        Mark::Line | Mark::Scatter if encoding.x.field.is_none() => "Pick X and Y in the panel",
        Mark::Bar if encoding.x.field.is_none() => "Pick a category for X",
        Mark::Histogram | Mark::Kde => "Pick a column for X",
        Mark::Heatmap => "Pick X and Y in the panel",
        _ => "Pick a column for Y",
    }
}

/// The plot itself, drawn with the marks of the glyph set `g`; where a line or
/// scatter plot with points was drawn.
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
    let hint = |buf: &mut ratatui::buffer::Buffer| {
        Paragraph::new(missing(modal))
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
    };
    match data {
        ChartRenderData::XY {
            series,
            breaks,
            values,
            names,
            x_axis_kind,
            x_bounds,
            numbers,
            other,
        } => {
            let xy = XYData {
                series,
                breaks,
                values,
                names,
                x_axis_kind,
                x_bounds,
                numbers,
                other,
            };
            return render_xy_chart(area, buf, modal, theme, xy, text_secondary, g);
        }
        ChartRenderData::Histogram { data: None, .. }
        | ChartRenderData::BoxPlot { data: None, .. }
        | ChartRenderData::Kde { data: None, .. }
        | ChartRenderData::Heatmap { data: None, .. } => hint(buf),
        ChartRenderData::Histogram {
            data: Some(data),
            x,
        } => {
            let numbers = PlotNumbers {
                x,
                y: if data.share {
                    AxisNumbers::measure(&ctx.number_format, "Share")
                } else {
                    AxisNumbers::count(&ctx.number_format)
                },
            };
            let x_title = modal.axis_title(&data.column);
            let look = HistogramLook {
                grid: modal.grid,
                legend: modal.show_legend,
                x_title: &x_title,
            };
            render_histogram_chart(area, buf, &look, theme, data, numbers, g)
        }
        ChartRenderData::BoxPlot {
            data: Some(data),
            y,
        } => render_box_plot_chart(area, buf, modal, theme, data, y, g),
        ChartRenderData::Kde {
            data: Some(data),
            x,
        } => {
            let numbers = PlotNumbers {
                x: x.fractional(),
                y: AxisNumbers::measure(&ctx.number_format, "Density"),
            };
            render_kde_chart(area, buf, modal, theme, data, numbers, g)
        }
        ChartRenderData::Heatmap {
            data: Some(data),
            numbers,
        } => render_heatmap_chart(area, buf, theme, data, numbers, text_secondary, g),
        ChartRenderData::Bar { data } => {
            let picked = ChartModal::is_complete(&modal.effective_spec());
            render_bar_chart(
                area,
                buf,
                (ctx, theme),
                data,
                (picked, modal.show_legend),
                g,
            )
        }
    }
    None
}

/// One horizontal bar per category: the label, the value, then the bar, from a zero
/// line that sits at the left edge unless some value is negative. The bars that fit
/// are drawn and the rest are counted on a `+ 212 more` chip, never squeezed in.
/// Split by a color, each category is a row per group, in the group's color, under
/// a legend of the groups.
fn render_bar_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    (ctx, theme): (&RenderContext, &Theme),
    data: Option<&BarData>,
    (picked, legend): (bool, bool),
    g: &Glyphs,
) {
    let hint = |text: &str, buf: &mut ratatui::buffer::Buffer| {
        Paragraph::new(text.to_string())
            .style(Style::default().fg(ctx.text_secondary))
            .centered()
            .render(area, buf);
    };
    // Picked but not here yet: the chart view says it is being computed.
    let Some(data) = data else {
        if !picked {
            hint("Pick a category for X", buf);
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

    if !data.groups.is_empty() {
        render_grouped_bars(area, buf, (ctx, theme), data, legend, g);
        return;
    }
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

/// Bars split by a color: a legend line of the groups, then per category its label
/// and a bar per group, each its group's color, scaled together. The categories
/// that fit are drawn; the rest are counted.
fn render_grouped_bars(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    (ctx, theme): (&RenderContext, &Theme),
    data: &BarData,
    legend: bool,
    g: &Glyphs,
) {
    let width = area.width as usize;
    let groups = data.groups.len();
    let other = other_at(data.other, groups);
    let color = |i: usize| series_style(theme, i, other);
    // The legend: a swatch and a name per group.
    let mut names = vec![Span::styled(
        format!("{}  ", data.value_column),
        Style::default().fg(ctx.text_secondary),
    )];
    for (i, name) in data.groups.iter().enumerate().filter(|_| legend) {
        names.push(Span::styled(g.bar_eighths[7].repeat(2), color(i)));
        names.push(Span::styled(
            format!(" {name}  "),
            Style::default().fg(ctx.text_primary),
        ));
    }
    Paragraph::new(Line::from(names)).render(Rect { height: 1, ..area }, buf);
    let rows = (area.height as usize).saturating_sub(1);
    let total = data.bars.len() + data.more;
    let per = groups.max(1);
    let fit = rows / per;
    let shown = if total * per <= rows {
        data.bars.len()
    } else {
        fit.saturating_sub(1).min(data.bars.len())
    };
    let label_w = data.bars[..shown]
        .iter()
        .map(|b| crate::glyphs::display_width(b.label.as_deref().unwrap_or(g.null)))
        .max()
        .unwrap_or(0)
        .clamp(1, (width * 2 / 5).max(4));
    let bar_x = label_w + 1;
    let bar_w = width.saturating_sub(bar_x);
    let values = || {
        data.bars[..shown]
            .iter()
            .flat_map(|b| b.by_group.iter().flatten().copied())
    };
    let lo = values().fold(0.0_f64, f64::min);
    let hi = values().fold(0.0_f64, f64::max);
    let span = if hi > lo { hi - lo } else { 1.0 };
    let zero = (((-lo / span) * bar_w as f64).round() as usize)
        .max(usize::from(lo < 0.0))
        .min(bar_w);
    let put = |buf: &mut ratatui::buffer::Buffer, x: usize, y: u16, s: &str, style: Style| {
        if x < width {
            buf.set_stringn(area.x + x as u16, y, s, width - x, style);
        }
    };
    for (i, bar) in data.bars[..shown].iter().enumerate() {
        let top = area.y + 1 + (i * per) as u16;
        let label = bar.label.as_deref().unwrap_or(g.null);
        let label = if crate::glyphs::display_width(label) > label_w {
            let room = label_w.saturating_sub(crate::glyphs::display_width(g.ellipsis));
            format!("{}{}", crate::glyphs::take_columns(label, room), g.ellipsis)
        } else {
            label.to_string()
        };
        put(buf, 0, top, &label, Style::default().fg(ctx.text_primary));
        if bar_w == 0 {
            continue;
        }
        for (k, value) in bar.by_group.iter().enumerate() {
            let Some(v) = value else { continue };
            let y = top + k as u16;
            let cells = ((v.abs() / span) * bar_w as f64).round() as usize;
            let cells = cells.max(usize::from(*v != 0.0));
            let (x, cells) = if *v >= 0.0 {
                (bar_x + zero, cells.min(bar_w - zero.min(bar_w)))
            } else {
                let cells = cells.min(zero);
                (bar_x + zero - cells, cells)
            };
            put(buf, x, y, &g.bar_eighths[7].repeat(cells), color(k));
        }
    }
    let hidden = total - shown;
    if hidden > 0 {
        let y = area.y + 1 + (shown * per) as u16;
        if y < area.bottom() {
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
}

/// The line or scatter chart's prepared series, as `ChartRenderData::XY` carries
/// them.
struct XYData<'a> {
    series: Option<&'a Vec<Vec<(f64, f64)>>>,
    breaks: Option<&'a Vec<Vec<usize>>>,
    values: Option<&'a Vec<Vec<(f64, f64)>>>,
    names: &'a [String],
    x_axis_kind: XAxisTemporalKind,
    x_bounds: Option<(f64, f64)>,
    numbers: PlotNumbers,
    other: bool,
}

/// One XY series with where its line breaks.
struct SeriesRuns<'a> {
    /// Its place among every series, which picks its color: an empty series
    /// before it keeps its color too.
    index: usize,
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
        names,
        x_axis_kind,
        x_bounds,
        numbers,
        other,
    } = xy;
    let other_at = other.then(|| names.len().saturating_sub(1));
    let scatter = modal.mark() == Mark::Scatter;
    let graph_type = if scatter {
        GraphType::Scatter
    } else {
        GraphType::Line
    };
    let y_starts_at_zero = modal.y_starts_at_zero;
    let log_scale = modal.log_scale;
    let show_legend = modal.show_legend;
    let spec = modal.effective_spec();
    let y_columns: Vec<String> = if spec.encoding.y.aggregate == Aggregate::Count {
        vec!["count".to_string()]
    } else {
        spec.encoding.y.field.clone()
    };

    let has_x_selected = spec.encoding.x.field.is_some();
    let has_data = chart_data
        .map(|d| d.iter().any(|s| !s.is_empty()))
        .unwrap_or(false);

    if has_x_selected && !has_data {
        let x_name = spec.encoding.x.field.as_deref().unwrap_or("X");
        let y_names: String = y_columns.join(", ");
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
        let empty_dataset = Dataset::default().name("").data(&[]).graph_type(graph_type);
        axes.render(Chart::new(vec![empty_dataset]), area, buf, g);
        return None;
    }

    if has_data {
        if let Some(data) = chart_data {
            let mut all_x_min = f64::INFINITY;
            let mut all_x_max = f64::NEG_INFINITY;
            let mut all_y_min = f64::INFINITY;
            let mut all_y_max = f64::NEG_INFINITY;

            // Data is already in display form (log-scaled when log_scale) from cache; use as-is.
            let no_breaks = Vec::new();
            let names_and_points: Vec<SeriesRuns> = data
                .iter()
                .zip(names.iter())
                .enumerate()
                .filter_map(|(i, (points, name))| {
                    if points.is_empty() {
                        return None;
                    }
                    let series_breaks = breaks.and_then(|b| b.get(i)).unwrap_or(&no_breaks);
                    Some(SeriesRuns {
                        index: i,
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
            let marker = match scatter {
                false => g.plot.line,
                true if finer && points * 4 > cells => g.plot.line,
                true => g.plot.point,
            };

            // A series is drawn as its runs between gaps, so a line never bridges a
            // missing value.
            // Other first, so the series drawn over it keep their colors.
            let datasets: Vec<Dataset> = names_and_points
                .iter()
                .filter(|s| Some(s.index) == other_at)
                .chain(
                    names_and_points
                        .iter()
                        .filter(|s| Some(s.index) != other_at),
                )
                .flat_map(|series| {
                    let style = series_style(theme, series.index, other_at);
                    segments(series.points, series.breaks)
                        .into_iter()
                        .map(move |run| {
                            Dataset::default()
                                .marker(marker)
                                .graph_type(graph_type)
                                .style(style)
                                .data(run)
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

            let y_min_bounds = if y_starts_at_zero {
                0.0_f64.min(all_y_min)
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

            let x_axis_title = spec
                .encoding
                .x
                .field
                .as_deref()
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
            axes.legend = legend(
                show_legend,
                names_and_points
                    .iter()
                    .map(|s| (s.name, series_style(theme, s.index, other_at))),
            );
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
                        names,
                        other_at,
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
        Paragraph::new(missing(modal))
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
    other_at: Option<usize>,
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
        let (value, value_style) = match value {
            Some(v) => (crosshair::format_number(v, y), value_style),
            None => (g.null.to_string(), Style::default().fg(theme.get("dimmed"))),
        };
        entries.push(crosshair::Entry {
            name: name.clone(),
            name_style: series_style(theme, i, other_at),
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
fn legend<'a>(show: bool, entries: impl Iterator<Item = (&'a str, Style)>) -> Option<Legend> {
    let entries: Vec<(String, Style)> = entries.map(|(n, s)| (n.to_string(), s)).collect();
    (show && entries.len() > 1).then_some(Legend { entries })
}

/// The style series `i` draws in: its palette color, or `dimmed` for Other, the
/// rows of every value without a series of its own.
fn series_style(theme: &Theme, i: usize, other_at: Option<usize>) -> Style {
    if other_at == Some(i) {
        return Style::default().fg(theme.get("dimmed"));
    }
    let colors = theme.series_colors();
    Style::default().fg(colors[i % colors.len()])
}

/// Where Other is among `n` series, when the last one is.
fn other_at(other: bool, n: usize) -> Option<usize> {
    (other && n > 0).then(|| n - 1)
}

/// A histogram: filled bars, or split by a color, each group's bins as a step
/// outline over the others, since filled bars would hide one another.
/// How a histogram is drawn: its grid, its legend, and the X axis's title.
pub struct HistogramLook<'a> {
    pub grid: bool,
    pub legend: bool,
    pub x_title: &'a str,
}

/// A histogram of `data` in `area`, in the chart view's marks: for the Value Counts
/// screen's histogram view.
pub fn render_histogram(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    ctx: &RenderContext,
    data: &HistogramData,
    x: AxisNumbers,
) {
    let numbers = PlotNumbers {
        x,
        y: AxisNumbers::count(&ctx.number_format),
    };
    let look = HistogramLook {
        grid: false,
        legend: false,
        x_title: &data.column,
    };
    render_histogram_chart(area, buf, &look, theme, data, numbers, crate::glyphs::get());
}

fn render_histogram_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    look: &HistogramLook<'_>,
    theme: &Theme,
    data: &HistogramData,
    numbers: PlotNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
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

    let y_title = if data.share { "Share" } else { "Count" };
    let x_title = look.x_title;
    let marker = if data.groups.is_empty() {
        g.plot.bar
    } else {
        g.plot.line
    };
    let mut axes = plot_axes(
        theme,
        AxisSpec::numbers([x_min_bounds, x_max_bounds], &numbers.x, x_title),
        AxisSpec::y_numbers([y_min_bounds, y_max_bounds], &numbers.y, y_title),
        marker,
        look.grid,
    );
    if !data.groups.is_empty() {
        let steps = step_outlines(data);
        let other = other_at(data.other, steps.len());
        // Other first, under the rest.
        let mut datasets: Vec<(usize, Dataset)> = steps
            .iter()
            .enumerate()
            .map(|(i, points)| {
                let dataset = Dataset::default()
                    .graph_type(GraphType::Line)
                    .marker(marker)
                    .style(series_style(theme, i, other))
                    .data(points);
                (i, dataset)
            })
            .collect();
        datasets.sort_by_key(|(i, _)| Some(*i) != other);
        let datasets: Vec<Dataset> = datasets.into_iter().map(|(_, d)| d).collect();
        axes.legend = legend(
            look.legend,
            data.groups
                .iter()
                .enumerate()
                .map(|(i, group)| (group.name.as_str(), series_style(theme, i, other))),
        );
        axes.render(Chart::new(datasets), area, buf, g);
        return;
    }

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

/// Each group's bins as the outline of its bars: up the left edge of each bin,
/// across its top, and down at the end.
fn step_outlines(data: &HistogramData) -> Vec<Vec<(f64, f64)>> {
    let n = data.bins.len().max(1);
    let width = (data.x_max - data.x_min) / n as f64;
    data.groups
        .iter()
        .map(|group| {
            let mut points = vec![(data.x_min, 0.0)];
            for (i, &count) in group.counts.iter().enumerate() {
                let x0 = data.x_min + i as f64 * width;
                points.push((x0, count));
                points.push((x0 + width, count));
            }
            points.push((data.x_max, 0.0));
            points
        })
        .collect()
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
    data: &KdeData,
    numbers: PlotNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
    if data.series.is_empty() {
        Paragraph::new("No data for KDE")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let other = other_at(data.other, data.series.len());
    let mut datasets: Vec<(usize, Dataset)> = data
        .series
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let dataset = Dataset::default()
                .graph_type(GraphType::Line)
                .marker(g.plot.line)
                .style(series_style(theme, i, other))
                .data(&s.points);
            (i, dataset)
        })
        .collect();
    // Other first, under the rest.
    datasets.sort_by_key(|(i, _)| Some(*i) != other);
    let datasets: Vec<Dataset> = datasets.into_iter().map(|(_, d)| d).collect();

    let x_title = modal.x().map(|x| modal.axis_title(x)).unwrap_or_default();
    let mut axes = plot_axes(
        theme,
        AxisSpec::numbers([data.x_min, data.x_max], &numbers.x, &x_title),
        AxisSpec::y_numbers([0.0, data.y_max], &numbers.y, "Density"),
        g.plot.line,
        modal.grid,
    );
    axes.legend = legend(
        modal.show_legend,
        data.series
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.as_str(), series_style(theme, i, other))),
    );
    axes.render(Chart::new(datasets), area, buf, g);
}

fn render_box_plot_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    data: &BoxPlotData,
    y_numbers: AxisNumbers,
    g: &Glyphs,
) {
    let text_secondary = theme.get("text_secondary");
    if data.stats.is_empty() {
        Paragraph::new("No data for box plot")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let mut segments: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut segment_styles: Vec<Style> = Vec::new();
    let box_half = 0.3;
    let cap_half = 0.2;
    for (i, stat) in data.stats.iter().enumerate() {
        let x = i as f64;
        let style = series_style(theme, i, None);
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
    let spec = modal.effective_spec();
    let x_title = spec.encoding.x.field.as_deref().unwrap_or("");
    let y_title = spec
        .encoding
        .y
        .field
        .first()
        .map(|y| modal.axis_title(y))
        .unwrap_or_default();
    let x = AxisSpec::fixed(
        [x_min_bounds, x_max_bounds],
        (0..data.stats.len()).map(|i| i as f64).collect(),
        Box::new(name),
        x_title,
    );
    let y = AxisSpec::y_numbers([data.y_min, data.y_max], &y_numbers, &y_title);
    plot_axes(theme, x, y, g.plot.point, modal.grid).render(Chart::new(datasets), area, buf, g);
}

fn render_heatmap_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: &HeatmapData,
    numbers: PlotNumbers,
    text_secondary: ratatui::style::Color,
    g: &Glyphs,
) {
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
    // The axes' titles sit as every plot's do: Y over its labels, X at the right
    // under its own.
    let title_style = Style::default().fg(theme.get("text_primary"));
    let y_title = cut(&data.y_column, layout[0].width as usize, g);
    buf.set_string(layout[0].x, layout[0].y, &y_title, title_style);

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
        let x_title = cut(&data.x_column, x_label_area.width as usize, g);
        let x = x_label_area.right() - x_title.width() as u16;
        buf.set_string(x, x_label_area.y + 1, &x_title, title_style);
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
                bucketable: &["date".to_string()],
                category: &["carrier".to_string()],
            },
            None,
            Some(10_000),
            false,
            1,
        );
        modal
    }

    /// Series names for test plots: as many as any test draws.
    fn names() -> &'static [String] {
        static NAMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
        NAMES.get_or_init(|| {
            ["price", "volume", "c", "d", "e", "f", "g"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        })
    }

    fn render_rows(modal: &mut ChartModal, width: u16, height: u16) -> Vec<String> {
        render_view(
            modal,
            ChartView {
                data: ChartRenderData::XY {
                    series: None,
                    breaks: None,
                    values: None,
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
            },
            width,
            height,
        )
    }

    /// The panel: every shelf in the same place for every type, then the options;
    /// no border inside it.
    #[test]
    fn the_panel_shows_every_shelf_and_echoes_every_option() {
        let mut modal = open_modal();
        modal.spec.encoding.x.field = Some("date".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        let rows = render_rows(&mut modal, 100, 30);
        let text = rows.join("\n");
        for row in &rows {
            assert!(
                !row.contains('╭') && !row.contains('╰'),
                "a border inside the panel: {row:?}"
            );
        }
        let line = |label: &str| {
            rows.iter()
                .find(|r| {
                    let head: String = r.chars().take(15).collect();
                    head.trim_start().trim_start_matches('▎').trim() == label
                })
                .cloned()
                .unwrap_or_else(|| panic!("{label} in {text}"))
        };
        assert!(rows[0].contains("Chart"));
        assert!(line("Type").contains("Line"));
        assert!(line("X").contains("date"));
        assert!(line("Y").contains("price"));
        assert!(line("Color").contains("none"));
        assert!(text.contains("by row"), "the bucket under a date X: {text}");
        assert!(text.contains("Options"));
        assert!(line("Y from zero").contains("off"));
        assert!(line("Rows").contains("sample 10,000"));
        assert!(line("Aggregate").contains("none"));
    }

    /// The aggregate is a row of its own under Y, labeled, so `none` does not read
    /// as a second Y column; focused, it carries the rail and the accent like any
    /// row.
    #[test]
    fn the_aggregate_row_is_labeled() {
        let ctx = RenderContext::for_test();
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let mut modal = open_modal();
        modal.set_mark(Mark::Scatter);
        modal.spec.encoding.x.field = Some("volume".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        modal.focus = ChartFocus::Aggregate;
        let area = Rect::new(0, 0, 100, 30);
        let mut buf = Buffer::empty(area);
        render_chart_view(
            area,
            &mut buf,
            &mut modal,
            &theme,
            &ctx,
            ChartView {
                data: ChartRenderData::XY {
                    series: None,
                    breaks: None,
                    values: None,
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
            },
        );
        // Each panel row past the rail gutter: its label column and its value.
        let cells = |y: u16, xs: std::ops::Range<u16>| -> String {
            xs.map(|x| buf[(x, y)].symbol()).collect::<String>()
        };
        let label_end = 2 + LABEL_WIDTH;
        let at = |label: &str| {
            (0..area.height)
                .find(|&y| cells(y, 2..label_end).trim_end() == label)
                .unwrap_or_else(|| panic!("{label} in the panel"))
        };
        let (y, row) = (at("Y"), at("Aggregate"));
        assert_eq!(row, y + 1, "right under Y");
        assert_eq!(cells(row, label_end..SIDEBAR_WIDTH).trim_end(), "none");
        assert!(cells(y, label_end..SIDEBAR_WIDTH).contains("price"));
        assert_eq!(buf[(0, row)].symbol(), crate::glyphs::get().rail);
        assert_eq!(buf[(2, row)].fg, ctx.accent, "the focused label");
    }

    /// The title row says only how the rows were made; the Y column is named once,
    /// at its axis. With nothing to say the row is blank.
    #[test]
    fn the_title_says_how_and_y_is_named_at_its_axis() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Scatter);
        modal.spec.encoding.x.field = Some("volume".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        let plot = |r: &String| -> String { r.chars().skip(SIDEBAR_WIDTH as usize + 1).collect() };
        let check = |modal: &mut ChartModal, how: &str| {
            let rows = render_rows(modal, 100, 30);
            assert_eq!(plot(&rows[0]).trim(), how, "{:?}", rows[0]);
            let named: Vec<String> = rows
                .iter()
                .map(plot)
                .filter(|r| r.contains("price"))
                .collect();
            assert_eq!(named.len(), 1, "price once, at its axis: {named:#?}");
            assert_eq!(named[0].trim(), "price", "{named:#?}");
        };
        check(&mut modal, "");
        modal.spec.encoding.color.field = Some("carrier".to_string());
        check(&mut modal, "colored by carrier");
        modal.spec.encoding.color.field = None;
        modal.spec.encoding.y.aggregate = Aggregate::Mean;
        check(&mut modal, "mean by volume");
        modal.set_mark(Mark::Line);
        modal.spec.encoding.y.cumulative = Cumulative::Sum;
        check(&mut modal, "by volume, running sum");
    }

    /// A shelf the type does not use stays, dimmed, with why.
    #[test]
    fn a_shelf_that_does_not_apply_is_dimmed_not_hidden() {
        let ctx = RenderContext::for_test();
        let mut modal = open_modal();
        modal.set_mark(Mark::Kde);
        modal.spec.encoding.x.field = Some("price".to_string());
        let rows = render_rows(&mut modal, 100, 30);
        let text = rows.join("\n");
        assert!(text.contains("density"), "{text}");
        assert!(text.contains("Bandwidth") && !text.contains("Log scale"));

        modal.set_mark(Mark::Box);
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 100, 30);
        let mut buf = Buffer::empty(area);
        render_chart_view(
            area,
            &mut buf,
            &mut modal,
            &theme,
            &ctx,
            ChartView {
                data: ChartRenderData::BoxPlot {
                    data: None,
                    y: AxisNumbers::default(),
                },
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
            },
        );
        let row = (0..area.height)
            .find(|&y| {
                (0..14)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
                    .trim()
                    == "Color"
            })
            .expect("the Color shelf stays");
        assert_eq!(buf[(2, row)].fg, ctx.dimmed, "dimmed");
        let text: String = (0..area.height)
            .map(|y| (0..40).map(|x| buf[(x, y)].symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("same as X"), "{text}");
    }

    /// The line under Color says how many values Other gathers while it is on: on by
    /// default for a scatter, off for a line; ←/→ on the line turn it over.
    #[test]
    fn the_values_line_counts_other() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Scatter);
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        modal.spec.encoding.color.field = Some("carrier".to_string());
        modal.color_counts = Some(crate::chart_modal::ColorCounts {
            column: "carrier".to_string(),
            values: (0..16)
                .map(|i| (Some(format!("C{i}")), 100 - i as u64))
                .collect(),
        });
        let line = |modal: &mut ChartModal| -> String {
            let rows = render_rows(modal, 100, 30);
            let at = rows.iter().position(|r| r.contains("Color")).unwrap();
            rows[at + 1]
                .chars()
                .take(40)
                .collect::<String>()
                .trim()
                .to_string()
        };
        assert_eq!(line(&mut modal), "top 10 + 6 other");
        modal.step(ChartFocus::ColorValues, 1);
        assert_eq!(line(&mut modal), "top 10 of 16 by rows");
        modal.spec.encoding.color.other = None;
        modal.set_mark(Mark::Line);
        assert_eq!(line(&mut modal), "top 10 of 16 by rows");
        modal.step(ChartFocus::ColorValues, -1);
        assert_eq!(line(&mut modal), "top 10 + 6 other");
        modal.spec.encoding.color.values = vec![Some("C3".to_string()), Some("C9".to_string())];
        assert_eq!(line(&mut modal), "2 picked + 14 other");
    }

    /// A quantile's percentile sits on the line under Aggregate; first and last say
    /// the order they read the rows in, the sort's own words when there is one, and
    /// only then read the view sorted.
    #[test]
    fn the_aggregate_row_says_percentile_and_order() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Line);
        modal.spec.encoding.x.field = Some("volume".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        let panel = |modal: &mut ChartModal| -> Vec<String> {
            render_rows(modal, 100, 30)
                .iter()
                .map(|r| r.chars().take(40).collect::<String>().trim().to_string())
                .collect()
        };
        let g = crate::glyphs::get();
        modal.spec.encoding.y.aggregate = Aggregate::Quantile;
        let rows = panel(&mut modal);
        let at = rows
            .iter()
            .position(|r| r.starts_with("Aggregate"))
            .unwrap();
        assert_eq!(rows[at], "Aggregate    quantile");
        assert_eq!(rows[at + 1], "p90");
        modal.step(ChartFocus::Quantile, -1);
        assert_eq!(panel(&mut modal)[at + 1], "p75");

        modal.spec.encoding.y.aggregate = Aggregate::Last;
        let rows = panel(&mut modal);
        assert_eq!(
            rows[at],
            format!("Aggregate    last {} by row order", g.middot)
        );
        let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
        assert!(!request.sorted);
        modal.row_order = Some(format!("time {}", g.sort_asc));
        let rows = panel(&mut modal);
        assert_eq!(
            rows[at],
            format!("Aggregate    last {} by time {}", g.middot, g.sort_asc)
        );
        let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
        assert!(request.sorted, "first and last read the view sorted");
        modal.spec.encoding.y.aggregate = Aggregate::Mean;
        let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
        assert!(!request.sorted, "a mean needs no order");
    }

    /// Ten series take ten colors, each its own, and the legend names all ten.
    #[test]
    fn ten_series_take_ten_colors() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Line);
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        modal.show_legend = true;
        let series: Vec<Vec<(f64, f64)>> = (0..10)
            .map(|s| (0..5).map(|i| (i as f64, (i * 10 + s) as f64)).collect())
            .collect();
        let names: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
        let ctx = RenderContext::for_test();
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        // Hex all the way, as a true-color terminal shows them.
        let mut theme = theme;
        let config = crate::config::ColorConfig::default();
        let hex = [
            &config.chart_1,
            &config.chart_2,
            &config.chart_3,
            &config.chart_4,
            &config.chart_5,
            &config.chart_6,
            &config.chart_7,
            &config.chart_8,
            &config.chart_9,
            &config.chart_10,
        ];
        for (i, value) in hex.iter().enumerate() {
            let channel = |at: usize| u8::from_str_radix(&value[at..at + 2], 16).unwrap();
            let color = ratatui::style::Color::Rgb(channel(1), channel(3), channel(5));
            theme.colors.insert(format!("chart_{}", i + 1), color);
        }
        assert_eq!(theme.series_colors().len(), 10);
        let area = Rect::new(0, 0, 80, 30);
        let mut buf = Buffer::empty(area);
        render_plot(
            area,
            &mut buf,
            &modal,
            &theme,
            &ctx,
            ChartRenderData::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                names: &names,
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                numbers: PlotNumbers::default(),
                other: false,
            },
            crate::glyphs::unicode(),
        );
        let mut swatches = Vec::new();
        for y in 0..area.height {
            let row: String = (0..area.width).map(|x| buf[(x, y)].symbol()).collect();
            if let Some(name) = names.iter().find(|n| row.contains(&format!("█ {n} "))) {
                let x = (0..area.width)
                    .find(|&x| buf[(x, y)].symbol() == "█")
                    .unwrap();
                swatches.push((name.clone(), buf[(x, y)].fg));
            }
        }
        assert_eq!(swatches.len(), 10, "{swatches:?}");
        let mut colors: Vec<_> = swatches.iter().map(|(_, c)| format!("{c:?}")).collect();
        colors.dedup();
        assert_eq!(colors.len(), 10, "{swatches:?}");
    }

    /// A 16-color terminal shows the ten slots as fewer colors: the chart draws one
    /// series per color, and the line under Color says how many.
    #[test]
    fn sixteen_colors_cap_the_series() {
        let sixteen = |hex: &str| {
            let channel = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).unwrap();
            crate::config::rgb_to_basic_ansi(channel(1), channel(3), channel(5))
        };
        let config = crate::config::ColorConfig::default();
        let mut theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        for (i, value) in [
            &config.chart_1,
            &config.chart_2,
            &config.chart_3,
            &config.chart_4,
            &config.chart_5,
            &config.chart_6,
            &config.chart_7,
            &config.chart_8,
            &config.chart_9,
            &config.chart_10,
        ]
        .iter()
        .enumerate()
        {
            theme
                .colors
                .insert(format!("chart_{}", i + 1), sixteen(value));
        }
        let colors = theme.series_colors();
        assert!((2..10).contains(&colors.len()), "{colors:?}");
        let mut modal = open_modal();
        modal.series_cap = Some(colors.len());
        modal.set_mark(Mark::Line);
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        modal.spec.encoding.color.field = Some("carrier".to_string());
        modal.color_counts = Some(crate::chart_modal::ColorCounts {
            column: "carrier".to_string(),
            values: (0..16)
                .map(|i| (Some(format!("C{i}")), 100 - i as u64))
                .collect(),
        });
        let rows = render_rows(&mut modal, 100, 30);
        let at = rows.iter().position(|r| r.contains("Color")).unwrap();
        assert!(
            rows[at + 1].contains(&format!("top {} of 16 by rows", colors.len())),
            "{}",
            rows[at + 1]
        );
        let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
        assert_eq!(request.series_cap, colors.len());
    }

    /// Other is the legend's last entry and is drawn in `dimmed`, under the series.
    #[test]
    fn other_is_the_legends_last_entry() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Scatter);
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        modal.show_legend = true;
        let series: Vec<Vec<(f64, f64)>> = (0..3)
            .map(|s| (0..5).map(|i| (i as f64, (i * 10 + s) as f64)).collect())
            .collect();
        let names: Vec<String> = ["UA", "B6", crate::chart_data::OTHER]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let ctx = RenderContext::for_test();
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 60, 24);
        let mut buf = Buffer::empty(area);
        render_plot(
            area,
            &mut buf,
            &modal,
            &theme,
            &ctx,
            ChartRenderData::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                names: &names,
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                numbers: PlotNumbers::default(),
                other: true,
            },
            crate::glyphs::unicode(),
        );
        let rows: Vec<String> = (0..area.height)
            .map(|y| (0..area.width).map(|x| buf[(x, y)].symbol()).collect())
            .collect();
        let at = |name: &str| {
            rows.iter()
                .position(|r| r.contains(&format!("█ {name}")))
                .unwrap_or_else(|| panic!("{name}: {rows:#?}"))
        };
        assert_eq!(at("UA") + 1, at("B6"));
        assert_eq!(at("B6") + 1, at("Other"));
        let row = at("Other") as u16;
        let swatch = (0..area.width)
            .find(|&x| buf[(x, row)].symbol() == "█")
            .unwrap();
        assert_eq!(buf[(swatch, row)].fg, theme.get("dimmed"));
    }

    /// The open Picker drops over the panel under its row, with a checkbox per item
    /// where it takes several, and each value's rows beside it.
    #[test]
    fn the_value_picker_lists_values_by_rows() {
        let g = crate::glyphs::get();
        let mut modal = open_modal();
        modal.spec.encoding.color.field = Some("carrier".to_string());
        modal.color_counts = Some(crate::chart_modal::ColorCounts {
            column: "carrier".to_string(),
            values: vec![(Some("UA".to_string()), 2514), (Some("B6".to_string()), 12)],
        });
        modal.focus = ChartFocus::ColorValues;
        modal.open_picker();
        modal.picker_toggle(); // UA in
        let rows = render_rows(&mut modal, 100, 30);
        let body = rows.join("\n");
        assert!(
            body.contains(&format!("{} UA", g.checkbox_on)) && body.contains("2,514"),
            "{body}"
        );
        assert!(body.contains(&format!("{} B6", g.checkbox_off)), "{body}");
        assert!(body.contains("1 picked"), "{body}");
    }

    /// An open Picker owns the clicks, drawn or not: the panel's rows take none.
    #[test]
    fn an_open_picker_with_no_room_still_takes_the_clicks() {
        let mut modal = open_modal();
        modal.focus = ChartFocus::Y;
        modal.open_picker();
        let hits = crate::pointer::recording(|| {
            render_rows(&mut modal, 100, 7);
        });
        assert!(hits.iter().any(|(_, h)| *h == Hit::Picker), "{hits:?}");
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

    /// A sampled or clipped chart says so at the right of the title row, at 80x24
    /// too; the bottom right holds only the X axis's title.
    #[test]
    fn notes_sit_at_the_title_rows_right() {
        let mut modal = open_modal();
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        let series = vec![vec![(0.0, 1.0), (1.0, 2.0)]];
        let note = "sample of 10,000 of 3.5M rows";
        let rows = render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::XY {
                    series: Some(&series),
                    breaks: None,
                    values: None,
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                notes: vec![note.to_string()],
                error: None,
                working: None,
                schema: None,
            },
            120,
            24,
        );
        assert!(rows[0].trim_end().ends_with(note), "{:?}", rows[0]);
        assert!(rows[23].trim_end().ends_with("price"), "{:?}", rows[23]);
        let canvas = |r: &String| r.chars().skip(42).collect::<String>();
        assert!(
            rows[1..].iter().all(|r| !canvas(r).contains("sample")),
            "{rows:#?}"
        );
    }

    /// On a narrow canvas the how keeps its room and the notes are cut to the rest,
    /// with the ellipsis.
    #[test]
    fn notes_give_way_to_the_how() {
        let mut modal = open_modal();
        modal.set_mark(Mark::Histogram);
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
                working: None,
                schema: None,
            },
            100,
            20,
        );
        let g = crate::glyphs::get();
        let title = rows[0].chars().skip(42).collect::<String>();
        assert!(title.starts_with("count per bin  "), "{title:?}");
        assert!(title.contains("sample of 1,000,000"), "{title:?}");
        assert!(title.trim_end().ends_with(g.ellipsis), "{title:?}");
        let canvas = |r: &String| r.chars().skip(42).collect::<String>();
        assert!(
            rows[1..].iter().all(|r| !canvas(r).contains("sample")),
            "{rows:#?}"
        );
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
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                notes: Vec::new(),
                error: Some("column not found: gone"),
                working: None,
                schema: None,
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
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
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
                        names: names(),
                        x_axis_kind: XAxisTemporalKind::Numeric,
                        x_bounds: None,
                        other: false,
                        numbers: PlotNumbers::default(),
                    },
                    notes: Vec::new(),
                    error: None,
                    working: None,
                    schema: None,
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
                    by_group: Vec::new(),
                })
                .collect(),
            more: 0,
            no_value: 0,
            rows: Default::default(),
            value_dtype: polars::prelude::DataType::Float64,
            counted: None,
            groups: Vec::new(),
            other: false,
            rows_note: None,
        }
    }

    fn render_bars(data: &BarData, w: u16, h: u16) -> Vec<String> {
        let mut modal = open_modal();
        modal.set_mark(Mark::Bar);
        render_view(
            &mut modal,
            ChartView {
                data: ChartRenderData::Bar { data: Some(data) },
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
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
                by_group: Vec::new(),
            },
            Bar {
                label: None,
                value: -10.0,
                by_group: Vec::new(),
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
            by_group: Vec::new(),
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
            groups: Vec::new(),
            other: false,
            share: false,
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
            other: false,
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
        modal.spec.encoding.y.field = vec!["price".to_string()];
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
                            names: names(),
                            x_axis_kind: XAxisTemporalKind::Date,
                            x_bounds: None,
                            other: false,
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
                            names: names(),
                            x_axis_kind: XAxisTemporalKind::Numeric,
                            x_bounds: None,
                            other: false,
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
                    modal.spec.encoding.x.field = Some(x_title.to_string());
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
            other: false,
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
        modal.spec.encoding.x.field = Some("volume".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        modal.y_starts_at_zero = false;
        let series = vec![vec![(0.0, 12_000.0), (5.0, 12_600.0)]];
        let xy = || ChartRenderData::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: names(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            other: false,
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
            groups: Vec::new(),
            other: false,
            share: false,
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
        modal.spec.encoding.x.field = Some("volume".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        modal.show_legend = false;
        let xy = |numbers| ChartRenderData::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: names(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            other: false,
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
    /// its bars, its axes and its legend. The Unicode set keeps its own.
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
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
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
            names: names(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            other: false,
            numbers: PlotNumbers::default(),
        };
        for (chart_type, mark) in [(Mark::Line, '*'), (Mark::Scatter, 'o')] {
            modal.spec.mark = chart_type;
            let text = plot_text(&modal, xy(Some(&series)), ascii);
            check(chart_type.label(), &text, &[mark]);
            // The legend's swatches, ASCII too.
            assert!(text.contains("# price"), "{text}");
            let text = plot_text(&modal, xy(Some(&series)), unicode);
            assert!(text.contains("█ price"), "{text}");
        }
        // Axes before the data is in.
        check("placeholder", &plot_text(&modal, xy(None), ascii), &[]);

        let histogram = HistogramData {
            column: "price".to_string(),
            groups: Vec::new(),
            other: false,
            share: false,
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
            of: 0,
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
            other: false,
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

    /// The legend reads clean over a full plot, with no frame: a short name's row is
    /// blank past the name, not the marks behind it.
    #[test]
    fn the_legend_hides_the_plot_behind_it() {
        let mut modal = open_modal();
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
        modal.show_legend = true;
        // A line up and down in every column fills the plot, legend corner included.
        let series: Vec<Vec<(f64, f64)>> = vec![
            (0..240)
                .map(|i| (i as f64 / 2.0, if i % 2 == 0 { 0.0 } else { 100.0 }))
                .collect();
            2
        ];
        for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
            let text = plot_text(
                &modal,
                ChartRenderData::XY {
                    series: Some(&series),
                    breaks: None,
                    values: None,
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                g,
            );
            let swatch = g.bar_eighths[7];
            let price = format!(" {swatch} price  ");
            let volume = format!(" {swatch} volume ");
            let lines: Vec<&str> = text.lines().collect();
            let at = lines
                .iter()
                .position(|l| l.contains(&price))
                .unwrap_or_else(|| panic!("{text}"));
            assert!(lines[at + 1].contains(&volume), "{text}");
            let corner = g.plot.axis.top_left;
            assert!(!lines[at - 1].contains(corner), "no frame: {text}");
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
            names: names(),
            x_axis_kind: XAxisTemporalKind::Date,
            x_bounds: None,
            other: false,
            numbers: PlotNumbers::default(),
        }
    }

    /// On a 300-column terminal a line chart labels its x axis 15 to 20 times, on
    /// calendar boundaries, and its y axis about once per four rows, at round values.
    #[test]
    fn a_wide_chart_carries_ticks_scaled_to_the_space() {
        let mut modal = open_modal();
        modal.spec.encoding.x.field = Some("date".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
        let series = decade();
        let rows = render_view(
            &mut modal,
            ChartView {
                data: xy_dates(&series),
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
            },
            300,
            60,
        );
        // The canvas, right of the sidebar.
        let rows: Vec<String> = rows
            .iter()
            .map(|r| r.chars().skip(SIDEBAR_WIDTH as usize + 2).collect())
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
        modal.spec.encoding.x.field = Some("date".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
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
        modal.spec.encoding.x.field = Some("date".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string()];
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
        modal.spec.encoding.x.field = Some("x".to_string());
        modal.spec.encoding.y.field = vec!["count".to_string()];
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
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
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
        modal.spec.encoding.x.field = Some("date".to_string());
        modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
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
                names: names(),
                x_axis_kind: XAxisTemporalKind::Date,
                x_bounds: None,
                other: false,
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
        modal.spec.encoding.x.field = Some("price".to_string());
        modal.spec.encoding.y.field = vec!["volume".to_string()];
        modal.spec.mark = Mark::Scatter;
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
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
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
            groups: Vec::new(),
            other: false,
            share: false,
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
            modal.focus = ChartFocus::X;
            modal.open_picker();
            let _ = render_rows(&mut modal, w, h);
            let mut data = bar_data(30);
            data.bars[3].value = -4.0;
            data.bars[4].label = Some("a label longer than the whole canvas is".to_string());
            let _ = render_bars(&data, w, h);
        }
    }
}

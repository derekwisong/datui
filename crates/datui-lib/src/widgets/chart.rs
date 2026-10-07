//! Chart view widget: the panel of shelves on the left (Type, X, Y, Color, then the
//! options), the plot on the right under its title, and the shared Picker over
//! them while a shelf is edited.

use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Chart, Clear, Dataset, GraphType, Paragraph, Widget, Wrap},
};

use crate::chart_data::{
    BarData, BoxPlotData, HeatmapData, HistogramData, KdeData, XAxisTemporalKind, other_at,
    segments,
};
use crate::chart_modal::{
    Aggregate, ChartFocus, ChartModal, Cumulative, Mark, PickerFor, ShelfUse, TimeUnit,
};
use crate::chart_plot::{Axis, Curve, LinesData, Plot, PlotData};
use crate::config::Theme;
use crate::glyphs::Glyphs;
use crate::pointer::Hit;
use crate::render::context::RenderContext;
use crate::widgets::axes::{
    AxisSpec, Legend, PlotAxes, Track, cut, fit_x_labels, fit_y_labels, resolution,
};
use crate::widgets::axis_numbers::AxisNumbers;
use crate::widgets::crosshair::{self, PlotPlace};
use crate::widgets::ui::{FormRow, FormValue, Picker, SectionRule, Surface, Working};
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
    /// What to plot: `None` while a kind other than a line or scatter waits for its
    /// data, or a shelf it needs is empty.
    pub plot: Option<Plot<'a>>,
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
            ChartFocus::LimitRows => {
                lines.push(row("Rows", rows_value(modal, ctx), Some(field), false));
                // The row's keys, or what is waiting on them, while it has focus.
                if modal.focus == field && !modal.plot_focus {
                    lines.push(sub(rows_hint(modal, ctx), None, false));
                }
                continue;
            }
            _ => continue,
        };
        lines.push(row(label, vec![value], Some(field), false));
    }
    if modal.aggregates() {
        lines.push(row("Rows", vec![quiet("all, exact", ctx)], None, false));
    }
    lines
}

/// The Rows option: how many rows a chart that samples reads, `Sample 10,000` or
/// `Every row (36.8M)`; a size being typed shows as typed, with the cursor.
fn rows_value(modal: &ChartModal, ctx: &RenderContext) -> Vec<Span<'static>> {
    let shown = modal.rows_shown();
    if let Some(typed) = shown.typed {
        return vec![
            plain(format!("Sample {typed}"), ctx),
            Span::styled(crate::glyphs::get().cursor, Style::default().fg(ctx.accent)),
        ];
    }
    if shown.every {
        let mut spans = vec![plain("Every row", ctx)];
        if let Some(rows) = modal.view_rows {
            spans.push(quiet(
                format!(" ({})", crate::home::discover::format_rows(rows)),
                ctx,
            ));
        }
        return spans;
    }
    vec![plain(
        format!("Sample {}", crate::numfmt::group_chrome(modal.sample_rows)),
        ctx,
    )]
}

/// Under the focused Rows row: why a typed size cannot be read, that a change
/// waits for Enter, or the row's keys.
fn rows_hint(modal: &ChartModal, ctx: &RenderContext) -> Vec<Span<'static>> {
    let g = crate::glyphs::get();
    if let Some(error) = modal.rows_shown().error {
        return vec![Span::styled(error, Style::default().fg(ctx.warning))];
    }
    if modal.rows_pending() {
        return vec![quiet("Enter to read", ctx)];
    }
    vec![quiet(
        format!(
            "{}/{} switch {} type a size",
            g.arrow_left, g.arrow_right, g.middot
        ),
        ctx,
    )]
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
            PanelLine::Rule(title) => SectionRule { title, chip: None }.render(
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
                    // An open picker's line has the one rail; the row keeps its
                    // accent label.
                    if modal.picker.is_none() {
                        buf.set_string(area.x, y, g.rail, Style::default().fg(ctx.accent));
                    }
                }
                // Past the rail and a cell of air.
                FormRow {
                    label,
                    value: FormValue::Spans {
                        spans: value.clone(),
                        dimmed: *dimmed,
                    },
                    focused,
                    label_width: LABEL_WIDTH,
                }
                .render_body(
                    Rect {
                        x: area.x + 2,
                        width: area.width.saturating_sub(2),
                        ..row
                    },
                    buf,
                    ctx,
                );
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
            Some(working) if view.plot.is_none() => working.render_centered(chart_inner, buf, ctx),
            working => {
                modal.plot = render_plot(
                    chart_inner,
                    buf,
                    modal,
                    theme,
                    ctx,
                    view.plot,
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
    plot: Option<Plot<'_>>,
    g: &Glyphs,
) -> Option<PlotPlace> {
    let text_secondary = theme.text_secondary();
    let hint = |buf: &mut ratatui::buffer::Buffer| {
        Paragraph::new(missing(modal))
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
    };
    let picked = || ChartModal::is_complete(&modal.effective_spec());
    let Some(plot) = plot else {
        if modal.mark() == Mark::Bar {
            render_bar_chart(
                area,
                buf,
                (ctx, theme),
                None,
                (picked(), modal.show_legend),
                g,
            );
        } else {
            hint(buf);
        }
        return None;
    };
    let (x, y) = (&plot.x, &plot.y);
    match &*plot.data {
        PlotData::Lines(_) | PlotData::XRange(_) => {
            return render_xy_chart(area, buf, modal, theme, &plot, text_secondary, g);
        }
        PlotData::Histogram(data) => {
            let look = HistogramLook {
                grid: modal.grid,
                legend: modal.show_legend,
            };
            render_histogram_chart(area, buf, &look, theme, (data, &plot.curves()), (x, y), g)
        }
        PlotData::Box(data) => {
            render_box_plot_chart(area, buf, modal, theme, data, (&x.title, y), g)
        }
        PlotData::Kde(data) => {
            render_kde_chart(area, buf, modal, theme, (data, &plot.curves()), (x, y), g)
        }
        PlotData::Heatmap(data) => {
            render_heatmap_chart(area, buf, theme, data, (x, y), text_secondary, g)
        }
        PlotData::Bars(data) => render_bar_chart(
            area,
            buf,
            (ctx, theme),
            Some(data),
            (picked(), modal.show_legend),
            g,
        ),
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

fn render_xy_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &ChartModal,
    theme: &Theme,
    plot: &Plot<'_>,
    text_secondary: ratatui::style::Color,
    g: &Glyphs,
) -> Option<PlotPlace> {
    let empty = LinesData::default();
    let (lines, x_bounds) = match &*plot.data {
        PlotData::Lines(lines) => (lines, None),
        PlotData::XRange(range) => (&empty, Some((range.x_min, range.x_max))),
        _ => return None,
    };
    let other_at = lines.other.then(|| lines.names.len().saturating_sub(1));
    let graph_type = if plot.scatter {
        GraphType::Scatter
    } else {
        GraphType::Line
    };
    let (x, y) = (&plot.x, &plot.y);
    let drawn: Vec<_> = plot.drawn().collect();

    if drawn.is_empty() {
        if modal.x().is_none() {
            Paragraph::new(missing(modal))
                .style(Style::default().fg(text_secondary))
                .centered()
                .render(area, buf);
            return None;
        }
        // The axes stand empty until the series arrive.
        const PLACEHOLDER_MIN: f64 = 0.0;
        const PLACEHOLDER_MAX: f64 = 1.0;
        let (x_min, x_max) = x_bounds.unwrap_or((PLACEHOLDER_MIN, PLACEHOLDER_MAX));
        let axes = plot_axes(
            theme,
            x_axis([x_min, x_max], x.kind, &x.numbers, &x.title),
            AxisSpec::y_numbers([PLACEHOLDER_MIN, PLACEHOLDER_MAX], &y.numbers, &y.title),
            g.plot.line,
            modal.grid,
        );
        let empty_dataset = Dataset::default().name("").data(&[]).graph_type(graph_type);
        axes.render(Chart::new(vec![empty_dataset]), area, buf, g);
        return None;
    }

    // Kept with the prepared series; worked out here only for series made here.
    let [all_x_min, all_x_max, all_y_min, all_y_max] =
        lines.shown_bounds(y.log).unwrap_or([0.0; 4]);

    // A scatter of few points marks each with a dot a cell wide; past one point per
    // four cells, the line's finer marks keep them apart.
    let points: usize = drawn.iter().map(|s| s.points.len()).sum();
    let cells = usize::from(area.width) * usize::from(area.height);
    let finer = resolution(g.plot.line) > resolution(g.plot.point);
    let marker = match plot.scatter {
        false => g.plot.line,
        true if finer && points * 4 > cells => g.plot.line,
        true => g.plot.point,
    };

    // A series is drawn as its runs between gaps, so a line never bridges a missing
    // value. Other first, so the series drawn over it keep their colors.
    let curves = plot.curves();
    let datasets = curve_datasets(&curves, theme, marker, graph_type);

    let y_min_bounds = if plot.y_from_zero {
        0.0_f64.min(all_y_min)
    } else {
        all_y_min
    };
    let y_max_bounds = if all_y_max > y_min_bounds {
        all_y_max
    } else {
        y_min_bounds + 1.0
    };
    let (x_min_bounds, x_max_bounds) = if all_x_max > all_x_min {
        (all_x_min, all_x_max)
    } else {
        (all_x_min - 0.5, all_x_min + 0.5)
    };

    let y_bounds = [y_min_bounds, y_max_bounds];
    // On a log scale a tick stands for its value before the log.
    let y_numbers = if y.log {
        y.numbers.clone().fractional()
    } else {
        y.numbers.clone()
    };
    let y_spec = if y.log {
        AxisSpec::y_log(y_bounds, &y_numbers, &y.title)
    } else {
        AxisSpec::y_numbers(y_bounds, &y_numbers, &y.title)
    };
    let mut axes = plot_axes(
        theme,
        x_axis([x_min_bounds, x_max_bounds], x.kind, &x.numbers, &x.title),
        y_spec,
        marker,
        modal.grid,
    );
    axes.legend = legend(
        modal.show_legend,
        drawn
            .iter()
            .map(|s| (s.name, series_style(theme, s.index, other_at))),
    );
    let x_bounds = [x_min_bounds, x_max_bounds];
    let sub = resolution(marker).0;
    // The crosshair's readout takes the rows under the plot while the plot has the keys;
    // it reads values before any log scaling.
    let values = &lines.series[..];
    let cursor = modal
        .cursor_x
        .filter(|_| modal.plot_focus)
        .and_then(|cursor| {
            if lines.xs.is_empty() {
                crosshair::nearest(&crosshair::xs(values), cursor)
            } else {
                crosshair::nearest(&lines.xs, cursor)
            }
        });
    let readout = cursor
        .map(|at| {
            let written = crosshair::format_x(at, x.kind, &x.numbers);
            let entries = readout_entries(
                theme,
                g,
                (at, &x.title, written),
                &y.numbers,
                values,
                &lines.names,
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
    if let Some(at) = cursor {
        let style = Style::default().fg(theme.accent());
        crosshair::draw(buf, &place, at, style, g);
        Paragraph::new(readout).render(readout_area, buf);
    }
    Some(place)
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
    let value_style = Style::default().fg(theme.text_primary());
    let x_title = if x_title.is_empty() { "x" } else { x_title };
    let mut entries = vec![crosshair::Entry {
        name: x_title.to_string(),
        name_style: Style::default().fg(theme.text_secondary()),
        value: written,
        value_style,
    }];
    for (i, (value, name)) in crosshair::values_at(series, x)
        .into_iter()
        .zip(names)
        .enumerate()
    {
        let (value, value_style) = match value {
            Some(v) => (y.write(v), value_style),
            None => (g.null.to_string(), Style::default().fg(theme.dimmed())),
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
    let style = Style::default().fg(theme.text_primary());
    PlotAxes {
        grid: grid.then(|| Style::default().fg(theme.chart_grid())),
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

/// A dataset per run of each curve, in the order they come: Other first, so the
/// series drawn over it keep their colors.
fn curve_datasets<'a>(
    curves: &'a [Curve<'_>],
    theme: &Theme,
    marker: ratatui::symbols::Marker,
    graph_type: GraphType,
) -> Vec<Dataset<'a>> {
    curves
        .iter()
        .flat_map(|curve| {
            let style = series_style(theme, curve.index, curve.other.then_some(curve.index));
            segments(&curve.points, curve.breaks)
                .into_iter()
                .map(move |run| {
                    Dataset::default()
                        .marker(marker)
                        .graph_type(graph_type)
                        .style(style)
                        .data(run)
                })
        })
        .collect()
}

/// The style series `i` draws in: its palette color, or `dimmed` for Other, the
/// rows of every value without a series of its own.
fn series_style(theme: &Theme, i: usize, other_at: Option<usize>) -> Style {
    if other_at == Some(i) {
        return Style::default().fg(theme.dimmed());
    }
    let colors = theme.series_colors();
    Style::default().fg(colors[i % colors.len()])
}

/// How a histogram is drawn: its grid and its legend.
pub struct HistogramLook {
    pub grid: bool,
    pub legend: bool,
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
    let x = Axis {
        title: data.column.clone(),
        numbers: x,
        ..Axis::default()
    };
    let y = Axis {
        title: "Count".to_string(),
        numbers: AxisNumbers::count(&ctx.number_format),
        ..Axis::default()
    };
    let look = HistogramLook {
        grid: false,
        legend: false,
    };
    render_histogram_chart(
        area,
        buf,
        &look,
        theme,
        (data, &[]),
        (&x, &y),
        crate::glyphs::get(),
    );
}

/// A histogram: filled bars, or, split by a color, each group's bins as a step
/// outline over the others (filled bars would hide one another).
fn render_histogram_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    look: &HistogramLook,
    theme: &Theme,
    (data, curves): (&HistogramData, &[Curve<'_>]),
    (x, y): (&Axis, &Axis),
    g: &Glyphs,
) {
    let text_secondary = theme.text_secondary();
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

    let marker = if data.groups.is_empty() {
        g.plot.bar
    } else {
        g.plot.line
    };
    let mut axes = plot_axes(
        theme,
        AxisSpec::numbers([x_min_bounds, x_max_bounds], &x.numbers, &x.title),
        AxisSpec::y_numbers([y_min_bounds, y_max_bounds], &y.numbers, &y.title),
        marker,
        look.grid,
    );
    if !data.groups.is_empty() {
        let other = other_at(data.other, data.groups.len());
        let datasets = curve_datasets(curves, theme, marker, GraphType::Line);
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

    let frame = axes.frame(area);
    let points = bin_columns(data, [x_min_bounds, x_max_bounds], frame.graph.width);
    let style = Style::default().fg(theme.chart_1());
    let dataset = Dataset::default()
        .name("")
        .marker(g.plot.bar)
        .graph_type(GraphType::Bar)
        .style(style)
        .data(&points);

    axes.render_in(frame, Chart::new(vec![dataset]), buf, g);
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
    (data, curves): (&KdeData, &[Curve<'_>]),
    (x, y): (&Axis, &Axis),
    g: &Glyphs,
) {
    let text_secondary = theme.text_secondary();
    if data.series.is_empty() {
        Paragraph::new("No data for KDE")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let other = other_at(data.other, data.series.len());
    let datasets = curve_datasets(curves, theme, g.plot.line, GraphType::Line);

    let mut axes = plot_axes(
        theme,
        AxisSpec::numbers([data.x_min, data.x_max], &x.numbers, &x.title),
        AxisSpec::y_numbers([0.0, data.y_max], &y.numbers, &y.title),
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
    (x_title, y): (&str, &Axis),
    g: &Glyphs,
) {
    let text_secondary = theme.text_secondary();
    if data.stats.is_empty() {
        Paragraph::new("No data for box plot")
            .style(Style::default().fg(text_secondary))
            .centered()
            .render(area, buf);
        return;
    }

    let mut segments: Vec<Vec<(f64, f64)>> = Vec::new();
    let mut segment_styles: Vec<Style> = Vec::new();
    for (i, stat) in data.stats.iter().enumerate() {
        let marks = stat.marks(i as f64, 0.3, 0.2);
        for segment in [
            &marks.outline[..],
            &marks.median,
            &marks.low,
            &marks.high,
            &marks.low_cap,
            &marks.high_cap,
        ] {
            segments.push(segment.to_vec());
            segment_styles.push(series_style(theme, i, None));
        }
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
        x_title,
    );
    let y = AxisSpec::y_numbers([data.y_min, data.y_max], &y.numbers, &y.title);
    plot_axes(theme, x, y, g.plot.point, modal.grid).render(Chart::new(datasets), area, buf, g);
}

fn render_heatmap_chart(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    theme: &Theme,
    data: &HeatmapData,
    (x, y): (&Axis, &Axis),
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
    let title_style = Style::default().fg(theme.text_primary());
    let y_title = cut(&y.title, layout[0].width as usize, g);
    buf.set_string(layout[0].x, layout[0].y, &y_title, title_style);

    const Y_LABEL_MAX: u16 = 12;
    // Nice values up the side, one per few rows, each on the row its value falls in.
    let y_axis = AxisSpec::numbers([data.y_min, data.y_max], &y.numbers, "");
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

    let label_style = Style::default().fg(theme.text_primary());
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

    // Ten steps of density, the same in either glyph set.
    const RAMP: [&str; 10] = [" ", ".", ":", "-", "=", "+", "*", "#", "%", "@"];
    let style = Style::default().fg(theme.chart_1());
    let max_x_bin = data.x_bins.saturating_sub(1) as f64;
    let max_y_bin = data.y_bins.saturating_sub(1) as f64;
    for row in 0..plot_area.height {
        let y_bin_raw = ((row as f64 / plot_area.height as f64) * data.y_bins as f64).floor();
        let y_bin = data
            .y_bins
            .saturating_sub(1)
            .saturating_sub(y_bin_raw.clamp(0.0, max_y_bin) as usize);
        for col in 0..plot_area.width {
            let x_bin = ((col as f64 / plot_area.width as f64) * data.x_bins as f64)
                .floor()
                .clamp(0.0, max_x_bin) as usize;
            let count = data.counts[y_bin][x_bin];
            let level = ((count / data.max_count) * (RAMP.len() as f64 - 1.0))
                .round()
                .clamp(0.0, RAMP.len() as f64 - 1.0) as usize;
            buf[(plot_area.x + col, plot_area.y + row)]
                .set_symbol(RAMP[level])
                .set_style(style);
        }
    }

    let x_label_area = layout[2];
    let x_axis = AxisSpec::numbers([data.x_min, data.x_max], &x.numbers, "");
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
        let x_title = cut(&x.title, x_label_area.width as usize, g);
        let x = x_label_area.right() - x_title.width() as u16;
        buf.set_string(x, x_label_area.y + 1, &x_title, title_style);
    }
}

#[cfg(test)]
mod tests;

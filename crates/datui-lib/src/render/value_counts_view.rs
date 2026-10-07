//! The Value Counts screen: a takeover over the table. One header line says which
//! column and what was read; the summary strip under it; then a line per value with
//! its rows, percent, cumulative percent and a bar. The keys are on the footer.

use crate::analysis::value_counts::{LineKind, Number, Order, Summary, ValueCounts};
use crate::analysis::value_counts_modal::ValueCountsModal;
use crate::numfmt::{self, CellFormatter};
use crate::render::context::RenderContext;
use polars::prelude::{AnyValue, DataType};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Columns between the summary's items, and between the listing's columns.
const GAP: usize = 3;

pub fn render(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    let g = crate::glyphs::get();
    let spinner = g.spinner[app.throbber_frame as usize % g.spinner.len()];
    draw(area, buf, &mut app.value_counts, ctx, spinner);
    if app.value_counts.shows_histogram() {
        draw_histogram(area, buf, app, ctx);
    }
    if matches!(app.overlay, crate::Overlay::Export { .. }) {
        // The same compact dialog the table's export opens.
        let dialog = crate::widgets::export::dialog_area(area);
        crate::widgets::export::render_export_modal(dialog, buf, &mut app.export_modal, ctx);
    }
}

/// Draw the screen for `modal` into `area`.
pub fn draw(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut ValueCountsModal,
    ctx: &RenderContext,
    spinner: &str,
) {
    Clear.render(area, buf);
    if area.height == 0 || area.width < 4 {
        return;
    }
    let g = crate::glyphs::get();
    let column = modal.column().unwrap_or_default().to_string();
    let counts = modal.current().cloned();
    let count_fmt = ctx.number_format.formatter_for("", &DataType::UInt64);

    // What the numbers are of: the column, and all of it or a sample. Chrome, so
    // grouped whatever the table's setting, as the analysis header is.
    let chrome = numfmt::group_chrome;
    let read = match &counts {
        Some(c) => match c.sampled_of {
            Some(of) => format!(
                "sample of {} of {} rows",
                chrome(c.summary.rows),
                chrome(of)
            ),
            None => format!("all {} rows", chrome(c.summary.rows)),
        },
        // A count that failed says why below; the header does not claim one runs.
        None if modal.failed.as_ref().is_some_and(|(c, _)| *c == column) => {
            "not counted".to_string()
        }
        None => "counting".to_string(),
    };
    let mut header = format!("Value Counts {} {column} {} {read}", g.middot, g.middot);
    if counts.is_some() && modal.counting() {
        header.push_str(&format!(" {} {spinner} counting every row", g.middot));
    }
    let header_style = Style::default().bg(ctx.controls_bg).fg(ctx.table_header);
    buf.set_style(Rect { height: 1, ..area }, header_style);
    Paragraph::new(crate::glyphs::fit_cells(&header, area.width as usize, g.ellipsis).into_owned())
        .style(header_style.add_modifier(Modifier::BOLD))
        .render(Rect { height: 1, ..area }, buf);

    // A gutter for the rail, as every list has.
    let body = Rect {
        x: area.x + 1,
        y: area.y + 1,
        width: area.width - 1,
        height: area.height - 1,
    };
    let Some(counts) = counts else {
        let line = match &modal.failed {
            Some((failed, why)) if *failed == column => Line::from(Span::styled(
                format!("Could not count {column}: {why}"),
                Style::default().fg(ctx.warning),
            )),
            _ => {
                let seen = modal
                    .computing
                    .as_ref()
                    .and_then(|c| c.watch.rows_seen())
                    .map(|rows| format!(" {} {} rows read", g.middot, chrome(rows)))
                    .unwrap_or_default();
                Line::from(Span::styled(
                    format!("{spinner} Counting {column}...{seen}"),
                    Style::default().fg(ctx.text_secondary),
                ))
            }
        };
        if body.height > 1 {
            Paragraph::new(line).render(
                Rect {
                    y: body.y + 1,
                    height: 1,
                    ..body
                },
                buf,
            );
        }
        return;
    };

    let width = body.width as usize;
    let strip = summary_items(
        &counts.summary,
        &column,
        &counts.dtype,
        counts.is_sample(),
        ctx,
    );
    let strip_lines = pack(&strip, width);
    let mut y = body.y;
    let bottom = body.y + body.height;
    for items in &strip_lines {
        if y >= bottom {
            return;
        }
        let mut spans = Vec::new();
        for (i, (label, value)) in items.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" ".repeat(GAP)));
            }
            spans.push(Span::styled(
                format!("{label} "),
                Style::default().fg(ctx.text_secondary),
            ));
            spans.push(Span::styled(
                value.clone(),
                Style::default().fg(ctx.text_primary),
            ));
        }
        Paragraph::new(Line::from(spans)).render(Rect::new(body.x, y, body.width, 1), buf);
        y += 1;
    }
    // A blank line between the strip and the listing, when there is room for both.
    if body.height as usize > strip_lines.len() + 3 {
        y += 1;
    }
    modal.body_top = y;
    if y >= bottom || modal.shows_histogram() {
        return;
    }

    let lines = counts.lines(modal.order);
    let value_fmt = ctx.number_format.formatter_for(&column, &counts.dtype);
    let labels: Vec<(String, Style)> = lines
        .iter()
        .map(|line| label(&counts, line.kind, &value_fmt, ctx))
        .collect();
    let numbers: Vec<String> = lines
        .iter()
        .map(|l| count_text(l.rows, &count_fmt))
        .collect();
    let total = counts.summary.rows.max(1) as f64;

    let order_mark = |order: Order| {
        if modal.order == order {
            if order == Order::Count {
                g.sort_desc
            } else {
                g.sort_asc
            }
        } else {
            ""
        }
    };
    let value_head = format!("{column}{}", order_mark(Order::Value));
    let count_head = format!("Count{}", order_mark(Order::Count));
    let count_w = numbers
        .iter()
        .map(|n| crate::glyphs::display_width(n))
        .chain([crate::glyphs::display_width(&count_head)])
        .max()
        .unwrap_or(5);
    const PCT_W: usize = 6;
    // The value column takes what it needs, up to two fifths of the screen.
    let value_cap = (width * 2 / 5).max(8);
    let value_w = labels
        .iter()
        .map(|(l, _)| crate::glyphs::display_width(l))
        .chain([crate::glyphs::display_width(&value_head)])
        .max()
        .unwrap_or(1)
        .clamp(1, value_cap);
    let count_x = value_w + GAP;
    let pct_x = count_x + count_w + GAP;
    let cum_x = pct_x + PCT_W + 1;
    let bar_x = cum_x + PCT_W + GAP;
    // A bar is read against the others, not the screen's edge: past this it only
    // puts distance between them and the numbers.
    const BAR_MAX: usize = 60;
    let bar_w = width.saturating_sub(bar_x).min(BAR_MAX);

    let put = |buf: &mut Buffer, x: usize, y: u16, s: &str, style: Style| {
        if x < width {
            buf.set_stringn(body.x + x as u16, y, s, width - x, style);
        }
    };
    let right = |buf: &mut Buffer, x: usize, w: usize, y: u16, s: &str, style: Style| {
        let pad = w.saturating_sub(crate::glyphs::display_width(s));
        put(buf, x + pad, y, s, style);
    };

    // Numbers line up on their last digit, as in the table, under their name.
    let numbers_right = ctx.number_format.align_numeric_right
        && (counts.dtype.is_primitive_numeric() || matches!(counts.dtype, DataType::Decimal(..)));
    // The column names, in the header tier.
    let head_style = Style::default()
        .fg(ctx.table_header)
        .add_modifier(Modifier::BOLD);
    buf.set_style(
        Rect::new(body.x, y, body.width, 1),
        Style::default().bg(ctx.table_header_bg),
    );
    let fit = |s: &str, w: usize| crate::glyphs::fit_cells(s, w, g.ellipsis).into_owned();
    let value_head = fit(&value_head, value_w);
    let value_head_style = head_style.fg(ctx.type_color(&counts.dtype));
    if numbers_right {
        right(buf, 0, value_w, y, &value_head, value_head_style);
    } else {
        put(buf, 0, y, &value_head, value_head_style);
    }
    right(buf, count_x, count_w, y, &count_head, head_style);
    right(buf, pct_x, PCT_W, y, "%", head_style);
    right(buf, cum_x, PCT_W, y, "Cum %", head_style);
    y += 1;
    if y >= bottom {
        return;
    }

    let height = (bottom - y) as usize;
    modal.scroll_into_view(height);
    let peak = lines
        .iter()
        .filter(|l| !matches!(l.kind, LineKind::Other(_)))
        .map(|l| l.rows)
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let bar_style = Style::default().fg(ctx.primary_chart_series_color);
    let text = Style::default().fg(ctx.text_primary);
    for (i, line) in lines.iter().enumerate().skip(modal.offset).take(height) {
        let selected = i == modal.selected;
        if selected {
            buf.set_style(Rect::new(area.x, y, area.width, 1), ctx.highlight_style());
            buf.set_string(area.x, y, g.rail, Style::default().fg(ctx.accent));
        }
        let (label, style) = &labels[i];
        if numbers_right && matches!(line.kind, LineKind::Value(_)) {
            right(buf, 0, value_w, y, &fit(label, value_w), *style);
        } else {
            put(buf, 0, y, &fit(label, value_w), *style);
        }
        right(buf, count_x, count_w, y, &numbers[i], text);
        right(
            buf,
            pct_x,
            PCT_W,
            y,
            &crate::numfmt::percent(line.rows as f64 / total),
            text,
        );
        right(
            buf,
            cum_x,
            PCT_W,
            y,
            &crate::numfmt::percent(line.cumulative as f64 / total),
            Style::default().fg(ctx.text_secondary),
        );
        // The other line sums many values: a bar beside one value's would say
        // nothing true about it.
        if bar_w > 0 && !matches!(line.kind, LineKind::Other(_)) {
            let eighths = ((line.rows as f64 / peak) * (bar_w * 8) as f64).round() as usize;
            let eighths = eighths.max(usize::from(line.rows > 0)).min(bar_w * 8);
            let mut bar = g.bar_eighths[7].repeat(eighths / 8);
            if let Some(part) = (eighths % 8).checked_sub(1) {
                bar.push_str(g.bar_eighths[part]);
            }
            put(buf, bar_x, y, &bar, bar_style);
        }
        y += 1;
    }
}

/// The histogram view: the counts in bins under the summary, where the listing
/// would be, and what the bins left out under it.
fn draw_histogram(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    let modal = &app.value_counts;
    let Some(counts) = modal.current() else {
        return;
    };
    let Some(histogram) = &counts.histogram else {
        return;
    };
    let top = modal.body_top.max(area.y + 1);
    if top >= area.bottom() {
        return;
    }
    let mut plot = Rect {
        x: area.x + 1,
        y: top,
        width: area.width.saturating_sub(2),
        height: area.bottom() - top,
    };
    let notes = crate::chart_data::chart_notes(
        &Default::default(),
        histogram.clipped.as_ref(),
        crate::glyphs::get().middot,
    );
    if !notes.is_empty() && plot.height > 4 {
        plot.height -= 1;
        Paragraph::new(notes.join("  "))
            .style(Style::default().fg(ctx.dimmed))
            .right_aligned()
            .render(
                Rect {
                    y: plot.bottom(),
                    height: 1,
                    ..plot
                },
                buf,
            );
    }
    let schema = app.data_table_state.as_ref().map(|s| s.schema().as_ref());
    let x = crate::widgets::axis_numbers::AxisNumbers::column(
        &ctx.number_format,
        schema,
        &counts.column,
    );
    crate::widgets::chart::render_histogram(plot, buf, &app.theme, ctx, histogram, x);
}

/// A value's line label and its style: the value as the table writes it, the null
/// mark, or the count of values summed into `other`.
fn label(
    counts: &ValueCounts,
    kind: LineKind,
    fmt: &CellFormatter,
    ctx: &RenderContext,
) -> (String, Style) {
    let g = crate::glyphs::get();
    match kind {
        LineKind::Value(at) => {
            let text = match counts.value(at) {
                Ok(value) => {
                    let mut scratch = String::new();
                    let shown = numfmt::format_any_value(fmt, &value, &mut scratch).into_owned();
                    crate::exact::cell_preview(&shown, g)
                }
                Err(_) => String::new(),
            };
            (text, Style::default().fg(ctx.text_primary))
        }
        LineKind::Null => (g.null.to_string(), Style::default().fg(ctx.dimmed)),
        LineKind::Other(values) => (
            format!(
                "other ({} values)",
                count_text(
                    values as u64,
                    &ctx.number_format.formatter_for("", &DataType::UInt64)
                )
            ),
            Style::default().fg(ctx.text_secondary),
        ),
    }
}

/// The summary strip's items, label and value: count, distinct and nulls always;
/// sum, mean, min and max for numbers; min and max for dates and times. A sample's
/// sum is not the column's, and is left out.
pub fn summary_items(
    summary: &Summary,
    column: &str,
    dtype: &DataType,
    sample: bool,
    ctx: &RenderContext,
) -> Vec<(&'static str, String)> {
    // How many rows, values and nulls are datui's counts, not the column's data:
    // grouped as every count on screen is, whatever the table's number format.
    let mut items = vec![
        ("Rows", crate::numfmt::group_chrome(summary.rows)),
        ("Distinct", crate::numfmt::group_chrome(summary.distinct)),
        ("Nulls", crate::numfmt::group_chrome(summary.nulls)),
    ];
    let floats = ctx.number_format.formatter_for(column, &DataType::Float64);
    if let Some(sum) = summary.sum.filter(|_| !sample) {
        let text = match sum {
            Number::Int(n) => match i64::try_from(n) {
                Ok(n) => {
                    let ints = ctx.number_format.formatter_for(column, &DataType::Int64);
                    let mut scratch = String::new();
                    numfmt::format_any_value(&ints, &AnyValue::Int64(n), &mut scratch).into_owned()
                }
                Err(_) => n.to_string(),
            },
            Number::Float(f) => float_text(f, &floats),
        };
        items.push(("Sum", text));
    }
    if let Some(mean) = summary.mean {
        items.push(("Mean", float_text(mean, &floats)));
    }
    let values = ctx.number_format.formatter_for(column, dtype);
    for (name, value) in [("Min", &summary.min), ("Max", &summary.max)] {
        if let Some(value) = value {
            let mut scratch = String::new();
            items.push((
                name,
                numfmt::format_any_value(&values, value, &mut scratch).into_owned(),
            ));
        }
    }
    items
}

/// Items to lines no wider than `width`, in order, at least one per line.
fn pack(items: &[(&'static str, String)], width: usize) -> Vec<Vec<(&'static str, String)>> {
    let mut lines: Vec<Vec<(&'static str, String)>> = Vec::new();
    let mut used = 0;
    for item in items {
        let w = crate::glyphs::display_width(item.0) + 1 + crate::glyphs::display_width(&item.1);
        match lines.last_mut() {
            Some(line) if used + GAP + w <= width => {
                line.push(item.clone());
                used += GAP + w;
            }
            _ => {
                lines.push(vec![item.clone()]);
                used = w;
            }
        }
    }
    lines
}

/// A count as the table writes its integers: grouped while digit grouping is on.
fn count_text(n: u64, fmt: &CellFormatter) -> String {
    let mut scratch = String::new();
    numfmt::format_any_value(fmt, &AnyValue::UInt64(n), &mut scratch).into_owned()
}

/// A sum or mean: four decimals at most, trailing zeros dropped, scientific past
/// where that reads; grouped as the column's floats are.
pub fn float_text(v: f64, fmt: &CellFormatter) -> String {
    if !v.is_finite() {
        return v.to_string();
    }
    if v != 0.0 && (v.abs() >= 1e15 || v.abs() < 1e-4) {
        return format!("{v:.4e}");
    }
    let mut text = format!("{v:.4}");
    if text.contains('.') {
        let trimmed = text.trim_end_matches('0').trim_end_matches('.').len();
        text.truncate(trimmed);
    }
    if text == "-0" {
        text = "0".to_string();
    }
    match fmt {
        CellFormatter::Number(nf) => {
            let mut out = String::new();
            nf.regroup_decimal(&text, &mut out);
            out
        }
        CellFormatter::Passthrough => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::sampling::ReadWatch;
    use polars::prelude::*;

    fn screen(modal: &mut ValueCountsModal, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        draw(area, &mut buf, modal, &ctx, "*");
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    fn counted(df: DataFrame, column: &str) -> ValueCountsModal {
        let counts = crate::analysis::value_counts::Plan {
            lf: df.lazy(),
            column: column.to_string(),
            read: crate::analysis::value_counts::Read::Exact,
            known_total: None,
            streaming: false,
        }
        .run(&ReadWatch::default())
        .unwrap();
        let mut modal = ValueCountsModal::default();
        modal.open(vec![column.to_string()], 0, 1);
        modal.hold(counts);
        modal
    }

    #[test]
    fn the_screen_lists_values_with_the_summary_above() {
        let mut modal = counted(
            df!("pay" => [Some(1i64), Some(1), Some(2), None, Some(1)]).unwrap(),
            "pay",
        );
        modal.view = Some(crate::analysis::value_counts_modal::CountsView::Listing);
        let rows = screen(&mut modal, 80, 12);
        let g = crate::glyphs::get();
        assert!(rows[0].starts_with("Value Counts"), "{rows:#?}");
        assert!(rows[0].contains("all 5 rows"), "{rows:#?}");
        assert!(rows[1].contains("Rows 5") && rows[1].contains("Distinct 2"));
        assert!(rows[1].contains("Nulls 1") && rows[1].contains("Sum 5"));
        assert!(rows[1].contains("Mean 1.25") && rows[1].contains("Max 2"));
        let head = rows.iter().position(|r| r.contains("Cum %")).unwrap();
        assert!(rows[head].contains(&format!("Count{}", g.sort_desc)));
        let first = &rows[head + 1];
        assert!(first.starts_with(g.rail), "the cursor's rail: {first:?}");
        assert!(first.contains("60.0%"), "{first:?}");
        assert!(rows[head + 2].contains("20.0%") && rows[head + 2].contains("80.0%"));
        assert!(rows[head + 3].contains(g.null), "nulls on their own line");
        assert!(rows[head + 3].contains("100.0%"));
    }

    /// The strip's counts are datui's own, grouped as every count on screen is,
    /// whatever the table's number format says about the column's values.
    #[test]
    fn the_strip_groups_its_counts() {
        let mut modal = counted(df!("n" => (0..1_500i64).collect::<Vec<_>>()).unwrap(), "n");
        modal.view = Some(crate::analysis::value_counts_modal::CountsView::Listing);
        let rows = screen(&mut modal, 100, 12);
        assert!(rows[1].contains("Rows 1,500"), "{rows:#?}");
        assert!(rows[1].contains("Distinct 1,500"), "{rows:#?}");
    }

    #[test]
    fn a_narrow_screen_wraps_the_strip_and_keeps_the_listing() {
        let mut modal = counted(
            df!("amount" => [1.5f64, 2.25, 1.5, 1000.0]).unwrap(),
            "amount",
        );
        modal.view = Some(crate::analysis::value_counts_modal::CountsView::Listing);
        let rows = screen(&mut modal, 40, 12);
        assert!(rows[1].contains("Rows 4"));
        assert!(rows.iter().any(|r| r.contains("Sum 1005.25")), "{rows:#?}");
        assert!(rows.iter().any(|r| r.contains("Cum %")));
        assert!(rows.iter().all(|r| crate::glyphs::display_width(r) <= 40));
    }

    #[test]
    fn a_sample_says_so_in_the_header() {
        let mut modal = counted(df!("k" => ["a", "b"]).unwrap(), "k");
        let mut counts = (**modal.current().unwrap()).clone();
        counts.sampled_of = Some(1_000_000);
        modal.hold(counts);
        let rows = screen(&mut modal, 80, 8);
        assert!(
            rows[0].contains("sample of 2 of 1,000,000 rows"),
            "{rows:#?}"
        );

        // A sample's sum is not the column's.
        let mut modal = counted(df!("n" => [1i64, 2]).unwrap(), "n");
        let mut counts = (**modal.current().unwrap()).clone();
        counts.sampled_of = Some(10);
        modal.hold(counts);
        let rows = screen(&mut modal, 80, 8);
        assert!(
            rows[1].contains("Mean 1.5") && !rows[1].contains("Sum"),
            "{rows:#?}"
        );
    }

    #[test]
    fn before_the_counts_the_screen_says_it_is_counting() {
        let mut modal = ValueCountsModal::default();
        modal.open(vec!["k".to_string()], 0, 1);
        modal.computing = Some(crate::analysis::value_counts_modal::Computing {
            column: "k".to_string(),
            exact: false,
            watch: ReadWatch::default(),
            file_starts: None,
        });
        let rows = screen(&mut modal, 60, 6);
        assert!(rows[2].contains("Counting k..."), "{rows:#?}");
        modal.computing = None;
        modal.failed = Some(("k".to_string(), "boom".to_string()));
        let rows = screen(&mut modal, 60, 6);
        assert!(rows[2].contains("Could not count k: boom"), "{rows:#?}");
        assert!(!rows[0].contains("counting"), "nothing runs: {rows:#?}");
    }

    #[test]
    fn the_other_line_has_no_bar() {
        let ids: Vec<i64> = (0..crate::analysis::value_counts::TOP_N as i64 + 50).collect();
        let mut modal = counted(df!("id" => ids).unwrap(), "id");
        modal.view = Some(crate::analysis::value_counts_modal::CountsView::Listing);
        modal.move_to_end();
        let rows = screen(&mut modal, 80, 8);
        let g = crate::glyphs::get();
        let at = rows
            .iter()
            .position(|r| r.contains("other (50 values)"))
            .unwrap();
        assert!(!rows[at].contains(g.bar_eighths[7]), "{rows:#?}");
        assert!(
            rows[at - 1].contains(g.bar_eighths[7]),
            "a value's line has one: {rows:#?}"
        );
    }

    #[test]
    fn shares_and_means_read_plainly() {
        let plain = CellFormatter::Passthrough;
        assert_eq!(float_text(1.25, &plain), "1.25");
        assert_eq!(float_text(3.0, &plain), "3");
        assert_eq!(float_text(2.0 / 3.0, &plain), "0.6667");
        assert_eq!(float_text(1e20, &plain), "1.0000e20");
    }
}

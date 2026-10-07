//! The XY chart's crosshair: a cursor that stands on a point's x, steps to the next
//! point a column or more away, and a readout under the plot of every series' value
//! there. Pure: the chart view draws it, the chart keys and a click move it.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::{Line, Span},
};
use unicode_width::UnicodeWidthStr;

use crate::chart::chart_data::{XAxisTemporalKind, x_datetime, x_time};
use crate::glyphs::Glyphs;
use crate::widgets::axes::{Track, cut};
use crate::widgets::axis_numbers::AxisNumbers;

/// Cells between two entries of the readout.
const READOUT_GAP: usize = 3;

/// Where an XY plot was drawn: its cells and the x range across them, as the series'
/// marker divides each cell.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlotPlace {
    pub graph: Rect,
    pub x_bounds: [f64; 2],
    /// Dots across a cell.
    pub sub: u16,
}

impl PlotPlace {
    /// The column `x` is drawn in, as the series' points are.
    pub fn column(&self, x: f64) -> u16 {
        let [lo, hi] = self.x_bounds;
        let f = if hi > lo { (x - lo) / (hi - lo) } else { 0.0 };
        Track {
            start: self.graph.left(),
            cells: self.graph.width,
            sub: self.sub,
        }
        .cell(f)
    }
}

/// A move of the cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Move {
    Left,
    Right,
    First,
    Last,
}

/// The x of every point of every series, in order, each once.
pub fn xs(series: &[Vec<(f64, f64)>]) -> Vec<f64> {
    let mut xs: Vec<f64> = series
        .iter()
        .flatten()
        .map(|&(x, _)| x)
        .filter(|x| x.is_finite())
        .collect();
    xs.sort_by(f64::total_cmp);
    xs.dedup();
    xs
}

/// The x among `xs` nearest `x`.
pub fn nearest(xs: &[f64], x: f64) -> Option<f64> {
    let i = xs.partition_point(|&v| v < x);
    [i.checked_sub(1), Some(i)]
        .into_iter()
        .flatten()
        .filter_map(|i| xs.get(i).copied())
        .min_by(|a, b| (a - x).abs().total_cmp(&(b - x).abs()))
}

/// Where the cursor at `from` goes: to the first point of the next column that has
/// one, so sparse points are stepped one by one and dense ones a column at a time;
/// or to the first or last point.
pub fn step(xs: &[f64], place: &PlotPlace, from: f64, to: Move) -> Option<f64> {
    let here = place.column(from);
    match to {
        Move::First => xs.first().copied(),
        Move::Last => xs.last().copied(),
        Move::Right => {
            let i = xs.partition_point(|&v| place.column(v) <= here);
            xs.get(i).copied()
        }
        Move::Left => {
            let i = xs.partition_point(|&v| place.column(v) < here);
            // The first point of that column, so stepping back and forth lands alike.
            i.checked_sub(1).and_then(|prev| {
                let column = place.column(xs[prev]);
                let first = xs.partition_point(|&v| place.column(v) < column);
                xs.get(first).copied()
            })
        }
    }
    .or_else(|| nearest(xs, from))
}

/// The point drawn nearest `column`: a click lands on what it was aimed at.
pub fn at_column(xs: &[f64], place: &PlotPlace, column: u16) -> Option<f64> {
    let i = xs.partition_point(|&v| place.column(v) < column);
    [i.checked_sub(1), Some(i)]
        .into_iter()
        .flatten()
        .filter_map(|i| xs.get(i).copied())
        .min_by_key(|&v| place.column(v).abs_diff(column))
}

/// `x` as the readout writes it: in full, a date or time to the second, a number as
/// the table writes its column.
pub fn format_x(x: f64, kind: XAxisTemporalKind, numbers: &AxisNumbers) -> String {
    let when = match kind {
        XAxisTemporalKind::Numeric => None,
        XAxisTemporalKind::Date => x_datetime(x, kind).map(|d| d.format("%Y-%m-%d").to_string()),
        XAxisTemporalKind::Time => x_time(x).map(|t| t.format("%H:%M:%S%.f").to_string()),
        _ => x_datetime(x, kind).map(|d| d.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
    };
    when.unwrap_or_else(|| numbers.write(x))
}

/// Each series' value at `x`, by its points before any log: `None` where it has no
/// point there, a gap in its line.
pub fn values_at(series: &[Vec<(f64, f64)>], x: f64) -> Vec<Option<f64>> {
    series
        .iter()
        .map(|points| points.iter().find(|&&(px, _)| px == x).map(|&(_, y)| y))
        .collect()
}

/// One entry of the readout: a name in its color and the value beside it.
pub struct Entry {
    pub name: String,
    pub name_style: Style,
    pub value: String,
    pub value_style: Style,
}

impl Entry {
    fn width(&self) -> usize {
        self.name.width() + 2 + self.value.width()
    }
}

/// The readout's lines in `width` cells: entries flow left to right, three cells
/// apart, onto as many lines as they take; an entry wider than a line is cut.
pub fn readout_lines(entries: &[Entry], width: usize, g: &Glyphs) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut used = 0;
    for entry in entries {
        let w = entry.width();
        if lines.is_empty() || used + READOUT_GAP + w > width {
            lines.push(Line::default());
            used = 0;
        } else {
            if let Some(line) = lines.last_mut() {
                line.push_span(Span::raw(" ".repeat(READOUT_GAP)));
            }
            used += READOUT_GAP;
        }
        let line = lines.last_mut().expect("a line was pushed");
        let name = cut(&format!("{}:", entry.name), width, g);
        let room = width.saturating_sub(name.width() + 1);
        used += name.width();
        line.push_span(Span::styled(name, entry.name_style));
        if room > 0 {
            let value = cut(&entry.value, room, g);
            used += 1 + value.width();
            line.push_span(Span::raw(" "));
            line.push_span(Span::styled(value, entry.value_style));
        }
    }
    lines
}

/// The cursor: a line down the plot at `x`'s column, under the series, whose marks
/// stay where they fall; and its tick on the x axis.
pub fn draw(buf: &mut Buffer, place: &PlotPlace, x: f64, style: Style, g: &Glyphs) {
    let graph = place.graph;
    if graph.is_empty() {
        return;
    }
    let column = place.column(x);
    for y in graph.top()..graph.bottom() {
        let cell = &mut buf[(column, y)];
        let symbol = cell.symbol();
        if matches!(symbol, " " | "\u{2800}")
            || symbol == g.plot.grid_across
            || symbol == g.plot.grid_down
        {
            cell.set_symbol(g.plot.axis.vertical).set_style(style);
        }
    }
    let axis = graph.bottom();
    if axis < buf.area.bottom() {
        let cell = &mut buf[(column, axis)];
        if cell.symbol() == g.plot.axis.horizontal || cell.symbol() == g.plot.tick_x {
            cell.set_symbol(g.plot.tick_x).set_style(style);
        }
    }
}

#[cfg(test)]
mod tests;

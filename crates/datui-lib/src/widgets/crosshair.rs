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

use crate::chart_data::{AxisNumbers, XAxisTemporalKind, x_datetime, x_time};
use crate::glyphs::Glyphs;
use crate::widgets::axes::{Track, cut};

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
    when.unwrap_or_else(|| format_number(x, numbers))
}

/// `v` as the table writes its column.
pub fn format_number(v: f64, numbers: &AxisNumbers) -> String {
    let mut out = String::new();
    numbers.format.write_f64(v, &mut String::new(), &mut out);
    out
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
mod tests {
    use super::*;

    fn place(width: u16) -> PlotPlace {
        PlotPlace {
            graph: Rect::new(10, 0, width, 10),
            x_bounds: [0.0, 100.0],
            sub: 1,
        }
    }

    /// Sparse points step one by one; dense ones a column at a time, never past a
    /// column that has a point.
    #[test]
    fn the_cursor_steps_point_to_point_and_column_to_column() {
        let sparse = xs(&[vec![(0.0, 1.0), (50.0, 2.0), (100.0, 3.0)]]);
        let p = place(101);
        assert_eq!(step(&sparse, &p, 0.0, Move::Right), Some(50.0));
        assert_eq!(step(&sparse, &p, 50.0, Move::Right), Some(100.0));
        assert_eq!(step(&sparse, &p, 100.0, Move::Right), Some(100.0), "stays");
        assert_eq!(step(&sparse, &p, 50.0, Move::Left), Some(0.0));
        assert_eq!(step(&sparse, &p, 0.0, Move::Left), Some(0.0), "stays");

        // A thousand points across eleven columns: ten-odd a column.
        let dense: Vec<f64> = (0..1000).map(|i| f64::from(i) / 10.0).collect();
        let p = place(11);
        let mut at = 0.0;
        let mut columns = vec![p.column(at)];
        while let Some(next) = step(&dense, &p, at, Move::Right).filter(|&n| n != at) {
            at = next;
            columns.push(p.column(at));
        }
        assert_eq!(columns, (10..21).collect::<Vec<u16>>());
        // And back the same way, on the first point of each column.
        let back = step(&dense, &p, at, Move::Left).unwrap();
        assert_eq!(p.column(back), 19);
        assert_eq!(step(&dense, &p, back, Move::Right), Some(at));
        assert_eq!(step(&dense, &p, 42.0, Move::First), Some(0.0));
        assert_eq!(step(&dense, &p, 42.0, Move::Last), Some(99.9));
    }

    /// A click lands on the point drawn nearest it.
    #[test]
    fn a_click_lands_on_the_nearest_point() {
        let sparse = xs(&[vec![(0.0, 1.0), (50.0, 2.0)], vec![(100.0, 3.0)]]);
        let p = place(101);
        assert_eq!(at_column(&sparse, &p, 10), Some(0.0));
        assert_eq!(at_column(&sparse, &p, 40), Some(50.0));
        assert_eq!(at_column(&sparse, &p, 300), Some(100.0));
        assert_eq!(nearest(&sparse, 80.0), Some(100.0));
        assert_eq!(nearest(&[], 80.0), None);
    }

    /// Each series' value at the cursor, none where it has a gap there.
    #[test]
    fn values_at_the_cursor() {
        let series = vec![vec![(1.0, 10.0), (2.0, 20.0)], vec![(1.0, -1.0)]];
        assert_eq!(values_at(&series, 2.0), [Some(20.0), None]);
        assert_eq!(values_at(&series, 1.0), [Some(10.0), Some(-1.0)]);
    }

    /// Dates read in full, numbers as the table writes them.
    #[test]
    fn the_readout_writes_x_in_full() {
        let plain = AxisNumbers::default();
        assert_eq!(
            format_x(19_783.0, XAxisTemporalKind::Date, &plain),
            "2024-03-01"
        );
        assert_eq!(
            format_x(1_709_294_400_000.0, XAxisTemporalKind::DatetimeMs, &plain),
            "2024-03-01 12:00:00"
        );
        assert_eq!(format_x(2.5, XAxisTemporalKind::Numeric, &plain), "2.5");
        assert_eq!(format_x(3.0, XAxisTemporalKind::Numeric, &plain), "3");
    }

    fn entry(name: &str, value: &str) -> Entry {
        Entry {
            name: name.to_string(),
            name_style: Style::default(),
            value: value.to_string(),
            value_style: Style::default(),
        }
    }

    fn text(lines: &[Line<'_>]) -> Vec<String> {
        lines.iter().map(|l| l.to_string()).collect()
    }

    /// Entries flow onto as many lines as they take, never cut while a line holds one.
    #[test]
    fn the_readout_flows_onto_lines() {
        let g = crate::glyphs::unicode();
        let entries = [
            entry("date", "2024-03-01"),
            entry("temperature", "21.5"),
            entry("humidity", "44"),
        ];
        assert_eq!(
            text(&readout_lines(&entries, 80, g)),
            ["date: 2024-03-01   temperature: 21.5   humidity: 44"]
        );
        assert_eq!(
            text(&readout_lines(&entries, 40, g)),
            ["date: 2024-03-01   temperature: 21.5", "humidity: 44"]
        );
        assert_eq!(
            text(&readout_lines(&entries, 12, g)),
            ["date: 2024-…", "temperature:", "humidity: 44"]
        );
    }

    /// The cursor's line runs down the plot under the series, and its tick sits on
    /// the axis.
    #[test]
    fn the_cursor_line_keeps_the_series_marks() {
        for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
            let area = Rect::new(0, 0, 20, 6);
            let mut buf = Buffer::empty(area);
            let p = PlotPlace {
                graph: Rect::new(2, 0, 18, 5),
                x_bounds: [0.0, 17.0],
                sub: 1,
            };
            for x in 0..20 {
                buf[(x, 5)].set_symbol(g.plot.axis.horizontal);
            }
            buf[(7, 2)].set_symbol("x");
            draw(&mut buf, &p, 5.0, Style::default(), g);
            let column: String = (0..6).map(|y| buf[(7, y)].symbol().to_string()).collect();
            let v = g.plot.axis.vertical;
            assert_eq!(column, format!("{v}{v}x{v}{v}{}", g.plot.tick_x));
        }
    }
}

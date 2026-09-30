//! Axis labels and titles for every chart. ratatui places whatever x labels it is
//! given, cutting them or running them together on a narrow plot, and draws the axis
//! titles over the plot's corners. So datui chooses which labels fit and draws them,
//! and gives each title a row of its own or none.
//!
//! The rule, for every chart: labels never touch, two cells between them.
//! Middle labels go first; when even the ends do not fit, the labels step down to a
//! shorter form (dates to year-month or year) and the middle may come back; the ends
//! stay while anything fits. A title never covers the plot: it is cut to its row, and
//! dropped when the plot has no rows to spare.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    text::Span,
    widgets::{Axis, Chart, Widget},
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::glyphs::Glyphs;

/// Rows the plot keeps before an axis title gives up its row.
const MIN_PLOT_ROWS: u16 = 3;
/// Forms a label steps through at most, so a label that never runs out stops.
const MAX_LEVELS: usize = 8;
/// Cells between two x labels: one would do, but two dates a space apart read as a
/// range.
const LABEL_GAP: u16 = 2;

/// A tick's label at a level of detail, 0 the fullest; `None` past the shortest.
pub type TickLabel<'a> = &'a dyn Fn(f64, usize) -> Option<String>;

/// One axis: its range, where its ticks sit, how to write them, and its title.
pub struct AxisSpec<'a> {
    pub bounds: [f64; 2],
    /// Tick values in order along the axis; the ends are kept longest.
    pub ticks: Vec<f64>,
    pub label: TickLabel<'a>,
    pub title: &'a str,
}

impl<'a> AxisSpec<'a> {
    /// Ticks at the ends of `bounds` and halfway between.
    pub fn ends_and_middle(bounds: [f64; 2], label: TickLabel<'a>, title: &'a str) -> Self {
        let [lo, hi] = bounds;
        Self {
            bounds,
            ticks: vec![lo, (lo + hi) / 2.0, hi],
            label,
            title,
        }
    }
}

/// A chart's two axes and how they are drawn.
pub struct PlotAxes<'a> {
    pub x: AxisSpec<'a>,
    pub y: AxisSpec<'a>,
    /// The axis lines.
    pub line: Style,
    pub labels: Style,
    pub titles: Style,
}

/// Where a chart's parts sit in its area.
pub struct PlotFrame {
    pub y_title: Option<Rect>,
    /// What ratatui's `Chart` is drawn in: the plot, its axes and y labels.
    pub chart: Rect,
    /// The plot inside `chart`, right of the y axis and above the x axis.
    pub graph: Rect,
    /// The x labels' row, under the x axis.
    pub labels: Option<Rect>,
    pub x_title: Option<Rect>,
    y_labels: Vec<String>,
}

impl PlotAxes<'_> {
    /// The frame for `area`: title rows while the plot keeps its rows, then the
    /// plot as ratatui lays it out beside the y labels that fit.
    pub fn frame(&self, area: Rect) -> PlotFrame {
        // The x axis line and the label row under it.
        let base = 2 + MIN_PLOT_ROWS;
        let y_title = !self.y.title.is_empty() && area.height > base;
        let x_title = !self.x.title.is_empty() && area.height > base + u16::from(y_title);
        let mut chart = area;
        let y_title = y_title.then(|| {
            chart.y += 1;
            chart.height -= 1;
            Rect { height: 1, ..area }
        });
        let x_title = x_title.then(|| {
            chart.height -= 1;
            Rect {
                y: chart.bottom(),
                height: 1,
                ..area
            }
        });
        let rows = graph_area(chart, 0).0.height;
        let y_labels = fit_y_labels(&self.y, rows, chart.width / 3);
        let width = y_labels.iter().map(|l| l.width()).max().unwrap_or(0) as u16;
        let (graph, labels) = graph_area(chart, width);
        PlotFrame {
            y_title,
            chart,
            graph,
            labels,
            x_title,
            y_labels,
        }
    }

    /// Draw `chart` (its datasets and legend) with these axes in `area`, then its
    /// x labels and titles.
    pub fn render(&self, chart: Chart<'_>, area: Rect, buf: &mut Buffer, g: &Glyphs) -> PlotFrame {
        let frame = self.frame(area);
        // Two empty labels have ratatui keep the label row and draw the x axis line
        // without widening the space left of the y axis.
        let chart = chart
            .x_axis(
                Axis::default()
                    .bounds(self.x.bounds)
                    .style(self.line)
                    .labels(["", ""]),
            )
            .y_axis(
                Axis::default()
                    .bounds(self.y.bounds)
                    .style(self.line)
                    .labels(
                        frame
                            .y_labels
                            .iter()
                            .map(|l| Span::styled(l.as_str(), self.labels)),
                    ),
            );
        chart.render(frame.chart, buf);
        g.plot.redraw_axes(frame.chart, buf);
        if let Some(row) = frame.labels {
            let plot = (frame.graph.left(), frame.graph.width);
            for (x, label) in fit_x_labels(&self.x, (row.left(), row.right()), plot) {
                buf.set_string(x, row.y, label, self.labels);
            }
        }
        if let Some(row) = frame.y_title {
            let title = cut(self.y.title, row.width as usize, g);
            buf.set_string(row.x, row.y, title, self.titles);
        }
        if let Some(row) = frame.x_title {
            let title = cut(self.x.title, row.width as usize, g);
            let x = row.right() - title.width() as u16;
            buf.set_string(x, row.y, title, self.titles);
        }
        frame
    }
}

/// The plot and the x label row ratatui's `Chart` lays out in `chart` beside y
/// labels `y_label_width` wide, when its x axis has labels. Kept in step with
/// `Chart::layout`; `the_frame_matches_ratatuis_layout` checks it.
fn graph_area(chart: Rect, y_label_width: u16) -> (Rect, Option<Rect>) {
    if chart.is_empty() {
        return (Rect::default(), None);
    }
    let mut x = chart.left();
    let mut y = chart.bottom() - 1;
    let mut labels = None;
    if y > chart.top() {
        labels = Some(Rect {
            y,
            height: 1,
            ..chart
        });
        y -= 1;
    }
    x += y_label_width.min(chart.width / 3);
    if y > chart.top() {
        y -= 1;
    }
    if x + 1 < chart.right() {
        x += 1;
    }
    let graph = Rect::new(
        x,
        chart.top(),
        chart.right().saturating_sub(x),
        y - chart.top() + 1,
    );
    (graph, labels)
}

/// Every subset of `n` evenly spaced ticks that keeps them evenly spaced and keeps
/// both ends, most ticks first.
fn strides(n: usize) -> impl Iterator<Item = Vec<usize>> {
    let last = n.saturating_sub(1);
    (1..=last.max(1))
        .filter(move |s| last.is_multiple_of(*s))
        .map(move |s| (0..n).step_by(s).collect())
}

/// The y labels for a plot `rows` high with at most `width` cells left of its axis:
/// as many ticks as have a row each, in the fullest form that fits the width and
/// tells them apart.
fn fit_y_labels(axis: &AxisSpec<'_>, rows: u16, width: u16) -> Vec<String> {
    let Some(ticks) = strides(axis.ticks.len()).find(|t| t.len() <= rows.max(2) as usize) else {
        return Vec::new();
    };
    let at = |level| -> Option<Vec<String>> {
        ticks
            .iter()
            .map(|&i| (axis.label)(axis.ticks[i], level))
            .collect()
    };
    let fits = |labels: &Vec<String>| {
        labels.iter().all(|l| l.width() <= width as usize) && distinct(labels.iter())
    };
    (0..MAX_LEVELS)
        .map_while(at)
        .find(fits)
        .or_else(|| at(0))
        .unwrap_or_default()
}

/// Neighbors differ; two alike say nothing about the space between them.
fn distinct<'a>(labels: impl Iterator<Item = &'a String>) -> bool {
    let labels: Vec<_> = labels.collect();
    labels.windows(2).all(|w| w[0] != w[1])
}

/// The x labels that fit on a row spanning columns `span` (start, end) under a plot
/// at `plot` (left, width), each with its column: centered under its tick, kept on
/// the row, `LABEL_GAP` cells from the next. Middle labels are dropped first, then the
/// labels step down to shorter forms; none at all when even the ends do not fit.
pub fn fit_x_labels(axis: &AxisSpec<'_>, span: (u16, u16), plot: (u16, u16)) -> Vec<(u16, String)> {
    let (start, end) = span;
    let (left, width) = plot;
    let [lo, hi] = axis.bounds;
    let column = |v: f64| {
        let f = if hi > lo {
            ((v - lo) / (hi - lo)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        left + (f * f64::from(width.saturating_sub(1))).round() as u16
    };
    for level in 0..MAX_LEVELS {
        let Some(labels) = axis
            .ticks
            .iter()
            .map(|&v| (axis.label)(v, level))
            .collect::<Option<Vec<String>>>()
        else {
            break;
        };
        for subset in strides(labels.len()) {
            let mut placed: Vec<(u16, String)> = Vec::with_capacity(subset.len());
            let mut next_free = start;
            let fits = subset.iter().all(|&i| {
                let w = labels[i].width() as u16;
                if w > end.saturating_sub(start) {
                    return false;
                }
                let x = column(axis.ticks[i])
                    .saturating_sub(w / 2)
                    .clamp(start, end - w);
                let clear =
                    x >= next_free && placed.last().is_none_or(|(_, prev)| *prev != labels[i]);
                next_free = x + w + LABEL_GAP;
                placed.push((x, labels[i].clone()));
                clear
            });
            if fits {
                return placed;
            }
        }
    }
    Vec::new()
}

/// `text` in at most `width` cells, cut with the ellipsis when longer.
pub fn cut(text: &str, width: usize, g: &Glyphs) -> String {
    if text.width() <= width {
        return text.to_string();
    }
    let keep = width.saturating_sub(g.ellipsis.width());
    let mut used = 0;
    let mut out: String = text
        .chars()
        .take_while(|c| {
            used += c.width().unwrap_or(0);
            used <= keep
        })
        .collect();
    if keep + g.ellipsis.width() <= width {
        out.push_str(g.ellipsis);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart_data::{XAxisTemporalKind, axis_label_at, x_axis_label_at};
    use ratatui::widgets::Dataset;

    /// Days since the epoch of 2020-01-01 and 2024-12-31.
    const FIVE_YEARS: [f64; 2] = [18262.0, 20088.0];

    fn labels_on(axis: &AxisSpec<'_>, width: u16) -> Vec<String> {
        // The plot starts four cells in, past the y labels and the axis.
        fit_x_labels(axis, (0, width), (4, width - 4))
            .into_iter()
            .map(|(_, l)| l)
            .collect()
    }

    /// The middle goes first, then the form shortens; the ends stay while anything
    /// fits.
    #[test]
    fn labels_drop_the_middle_then_shorten() {
        let [lo, hi] = FIVE_YEARS;
        let label = |v, level| x_axis_label_at(v, XAxisTemporalKind::Date, (lo, hi), level);
        let axis = AxisSpec::ends_and_middle(FIVE_YEARS, &label, "");
        assert_eq!(
            labels_on(&axis, 40),
            ["2020-01-01", "2022-07-02", "2024-12-31"]
        );
        assert_eq!(labels_on(&axis, 24), ["2020-01-01", "2024-12-31"]);
        assert_eq!(labels_on(&axis, 17), ["2020-01", "2024-12"]);
        assert_eq!(labels_on(&axis, 16), ["2020", "2024"]);
        assert!(labels_on(&axis, 10).is_empty());
    }

    /// Placed labels stand apart, never leave the row, and keep the ends longest.
    #[test]
    fn labels_stand_apart_on_the_row() {
        let names = |i: f64, level| (level == 0).then(|| format!("column_{i}"));
        let axis = AxisSpec {
            bounds: [-0.5, 6.5],
            ticks: (0..7).map(f64::from).collect(),
            label: &names,
            title: "",
        };
        for width in 10..120 {
            let placed = fit_x_labels(&axis, (0, width), (4, width - 4));
            for pair in placed.windows(2) {
                let (x, label) = &pair[0];
                assert!(
                    x + label.width() as u16 + LABEL_GAP <= pair[1].0,
                    "{width}: {placed:?}"
                );
            }
            if let Some((x, label)) = placed.last() {
                assert!(x + label.width() as u16 <= width, "{width}: {placed:?}");
                assert_eq!(label, "column_6", "the last end stays: {placed:?}");
                assert_eq!(placed[0].1, "column_0", "the first end stays: {placed:?}");
            }
        }
    }

    /// A short form that says the same at both ends is no label: a range inside one
    /// year steps down to month and day, and numbers that round alike show none.
    #[test]
    fn short_forms_tell_the_ends_apart() {
        // 2024-03-01 to 2024-03-08.
        let march = (19783.0, 19790.0);
        let label = |v, level| x_axis_label_at(v, XAxisTemporalKind::Date, march, level);
        let axis = AxisSpec::ends_and_middle([march.0, march.1], &label, "");
        assert_eq!(labels_on(&axis, 16), ["03-01", "03-08"]);

        let axis = AxisSpec::ends_and_middle([1000.1, 1000.3], &axis_label_at, "");
        assert!(labels_on(&axis, 12).is_empty());
    }

    fn render(area: Rect, g: &Glyphs) -> (Buffer, PlotFrame) {
        let axes = PlotAxes {
            x: AxisSpec::ends_and_middle([0.0, 10.0], &axis_label_at, "x title"),
            y: AxisSpec::ends_and_middle([0.0, 1000.0], &axis_label_at, "y title"),
            line: Style::default(),
            labels: Style::default(),
            titles: Style::default(),
        };
        let points = [(0.0, 0.0), (10.0, 1000.0)];
        let chart = Chart::new(vec![Dataset::default().data(&points)]);
        let mut buf = Buffer::empty(area);
        let frame = axes.render(chart, area, &mut buf, g);
        (buf, frame)
    }

    /// The frame datui computes is where ratatui draws: the axis corner sits just
    /// left of the plot and just under it.
    #[test]
    fn the_frame_matches_ratatuis_layout() {
        let g = crate::glyphs::unicode();
        for (w, h) in [(9, 6), (20, 5), (20, 7), (30, 12), (60, 20), (120, 40)] {
            let (buf, frame) = render(Rect::new(0, 0, w, h), g);
            let corner = (frame.graph.left() - 1, frame.graph.bottom());
            assert_eq!(buf[corner].symbol(), "└", "{w}x{h}: {:?}", frame.graph);
        }
    }

    /// The titles take rows of their own while the plot keeps three rows, the x
    /// title the first to go.
    #[test]
    fn titles_give_up_their_rows_to_the_plot() {
        let g = crate::glyphs::unicode();
        let (_, frame) = render(Rect::new(0, 0, 30, 7), g);
        assert!(frame.y_title.is_some() && frame.x_title.is_some());
        assert_eq!(frame.graph.height, 3);
        let (_, frame) = render(Rect::new(0, 0, 30, 6), g);
        assert!(frame.y_title.is_some() && frame.x_title.is_none());
        let (_, frame) = render(Rect::new(0, 0, 30, 5), g);
        assert!(frame.y_title.is_none() && frame.x_title.is_none());
    }

    /// A title longer than its row is cut with the set's ellipsis.
    #[test]
    fn a_long_title_is_cut_to_its_row() {
        assert_eq!(cut("a long title", 6, crate::glyphs::unicode()), "a lon…");
        assert_eq!(cut("a long title", 6, crate::glyphs::ascii()), "a l...");
        assert_eq!(cut("short", 6, crate::glyphs::ascii()), "short");
    }
}

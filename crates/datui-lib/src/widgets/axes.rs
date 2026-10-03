//! Axis ticks, labels, titles and the grid for every chart. ratatui places whatever
//! labels it is given, cutting them or running them together on a narrow plot, and
//! draws the axis titles over the plot's corners. So datui chooses the ticks, draws
//! their labels and marks, and gives each title a row of its own or none.
//!
//! Ticks fall on nice values: 1, 2 or 5 times a power of ten, or calendar boundaries
//! on a time axis, as many as the space holds (about one label per 15 columns, one
//! per 4 rows). The rule for labels, on every chart: they never touch, two cells
//! between them. A crowded axis first takes a coarser step, then a shorter form of
//! its labels (`12.3k`, or a date without its year); the ends of a fixed axis stay
//! while anything fits. A title never covers the plot: it is cut to its row, and
//! dropped when the plot has no rows to spare.

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Rect},
    style::Style,
    symbols::Marker,
    text::Span,
    widgets::{Axis, Chart, LegendPosition, Widget},
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::chart_data::{AxisFormat, AxisNumbers, XAxisTemporalKind, x_axis_label_at};
use crate::glyphs::Glyphs;
use crate::widgets::ticks;

/// Rows the plot keeps before an axis title gives up its row.
const MIN_PLOT_ROWS: u16 = 3;
/// Forms a label steps through at most, so a label that never runs out stops.
const MAX_LEVELS: usize = 8;
/// Cells between two x labels: one would do, but two dates a space apart read as a
/// range.
const LABEL_GAP: u16 = 2;
/// Columns between x labels the axis aims for, and the fewest it takes.
const X_SPACING: f64 = 15.0;
const X_LEAST: f64 = 6.0;
/// Rows between y labels the axis aims for, and the fewest it takes.
const Y_SPACING: f64 = 4.0;
const Y_LEAST: f64 = 2.0;
/// The fewest cells between minor ticks, across and down.
const X_MINOR_GAP: f64 = 3.0;
const Y_MINOR_GAP: f64 = 3.0;

/// A tick's label at a level of detail, 0 the fullest; `None` past the shortest.
pub type TickLabel<'a> = Box<dyn Fn(f64, usize) -> Option<String> + 'a>;

/// How an axis chooses its ticks.
enum Scale<'a> {
    /// Nice numbers, written in one format; `widen` stretches the axis out to the
    /// ticks either side of its ends, so a y axis starts and ends on a label.
    Numbers { numbers: AxisNumbers, widen: bool },
    /// Calendar boundaries on a time axis.
    Calendar {
        kind: XAxisTemporalKind,
        numbers: AxisNumbers,
    },
    /// Ticks given in advance: evenly spaced ones thin out keeping both ends.
    Fixed {
        ticks: Vec<f64>,
        label: TickLabel<'a>,
    },
}

/// One axis: its range, how its ticks are chosen and written, and its title.
pub struct AxisSpec<'a> {
    pub bounds: [f64; 2],
    scale: Scale<'a>,
    pub title: &'a str,
    /// Labels right-aligned in at least this many cells.
    pad: usize,
}

/// A way to tick an axis: where, the minor ticks between, and the labels at each
/// level of detail.
#[derive(Clone, Debug, Default)]
pub struct TickSet {
    pub ticks: Vec<f64>,
    pub minor: Vec<f64>,
    pub levels: Vec<Vec<String>>,
    /// The axis's range under this set: widened to the outer ticks, or as given.
    pub bounds: [f64; 2],
}

impl<'a> AxisSpec<'a> {
    /// An axis ticked at `ticks`, written by `label`. Evenly spaced ticks thin out
    /// keeping both ends.
    pub fn fixed(bounds: [f64; 2], ticks: Vec<f64>, label: TickLabel<'a>, title: &'a str) -> Self {
        Self {
            bounds,
            scale: Scale::Fixed { ticks, label },
            title,
            pad: 0,
        }
    }

    /// Ticks at the ends of `bounds` and halfway between.
    pub fn ends_and_middle(bounds: [f64; 2], label: TickLabel<'a>, title: &'a str) -> Self {
        let [lo, hi] = bounds;
        Self::fixed(bounds, vec![lo, (lo + hi) / 2.0, hi], label, title)
    }

    /// A numeric axis over `bounds` holding `numbers`, ticked at nice values (whole
    /// ones when the numbers are whole), every tick in one format.
    pub fn numbers(bounds: [f64; 2], numbers: &AxisNumbers, title: &'a str) -> Self {
        Self {
            bounds,
            scale: Scale::Numbers {
                numbers: numbers.clone(),
                widen: false,
            },
            title,
            pad: 0,
        }
    }

    /// A numeric y axis: widened out to the nice ticks either side of its range, so
    /// it starts and ends on a label.
    pub fn y_numbers(bounds: [f64; 2], numbers: &AxisNumbers, title: &'a str) -> Self {
        Self {
            scale: Scale::Numbers {
                numbers: numbers.clone(),
                widen: true,
            },
            ..Self::numbers(bounds, numbers, title)
        }
    }

    /// A time axis of `kind` over `bounds`, ticked on calendar boundaries; `numbers`
    /// writes a value past what a date can stand for.
    pub fn calendar(
        bounds: [f64; 2],
        kind: XAxisTemporalKind,
        numbers: &AxisNumbers,
        title: &'a str,
    ) -> Self {
        if kind == XAxisTemporalKind::Numeric {
            return Self::numbers(bounds, numbers, title);
        }
        Self {
            bounds,
            scale: Scale::Calendar {
                kind,
                numbers: numbers.clone(),
            },
            title,
            pad: 0,
        }
    }

    /// A numeric axis whose position `v` stands for the number `shown(v)`, as on a log
    /// scale: ticked at its ends and middle, its format chosen from the numbers they
    /// stand for.
    pub fn numbers_as(
        bounds: [f64; 2],
        numbers: &AxisNumbers,
        title: &'a str,
        shown: impl Fn(f64) -> f64 + 'a,
    ) -> Self {
        let [lo, hi] = bounds;
        let ticks = vec![lo, (lo + hi) / 2.0, hi];
        let shown_ticks: Vec<f64> = ticks.iter().map(|&v| shown(v)).collect();
        let format = AxisFormat::new(&shown_ticks, numbers);
        let label = Box::new(move |v, level| format.label(shown(v), level));
        Self::fixed(bounds, ticks, label, title)
    }

    /// The same axis, its labels right-aligned in at least `width` cells.
    pub fn padded(self, width: usize) -> Self {
        Self { pad: width, ..self }
    }

    /// The ways to tick this axis along `track`, the preferred first: about one label
    /// per `spacing` cells, then coarser. A second group follows when the first may
    /// come up empty: a time axis's dates at its ends and middle.
    fn tick_sets(
        &self,
        track: Track,
        spacing: f64,
        least: f64,
        minor_gap: f64,
    ) -> Vec<Vec<TickSet>> {
        let [lo, hi] = self.bounds;
        let length = track.length();
        match &self.scale {
            Scale::Fixed { ticks, label } => vec![
                strides(ticks.len())
                    .map(|subset| {
                        let ticks: Vec<f64> = subset.iter().map(|&i| ticks[i]).collect();
                        let levels = (0..MAX_LEVELS)
                            .map_while(|level| ticks.iter().map(|&v| label(v, level)).collect())
                            .collect();
                        TickSet {
                            ticks,
                            minor: Vec::new(),
                            levels,
                            bounds: self.bounds,
                        }
                    })
                    .collect(),
            ],
            Scale::Numbers { numbers, widen } => {
                vec![number_sets(
                    self.bounds,
                    numbers,
                    *widen,
                    length,
                    spacing,
                    least,
                    minor_gap,
                )]
            }
            Scale::Calendar { kind, numbers } => {
                let primary = calendar_sets(self.bounds, *kind, length, spacing, least, minor_gap);
                let format = AxisFormat::ends_and_middle(self.bounds, numbers);
                let kind = *kind;
                let ends = AxisSpec::ends_and_middle(
                    self.bounds,
                    Box::new(move |v, level| x_axis_label_at(v, kind, (lo, hi), level, &format)),
                    "",
                );
                let fallback = ends.tick_sets(track, spacing, least, minor_gap).remove(0);
                vec![primary, fallback]
            }
        }
    }
}

/// Whether `sets` hold more than one tick each where it counts.
fn with_ticks(sets: Vec<TickSet>) -> Vec<TickSet> {
    sets.into_iter().filter(|s| s.ticks.len() >= 2).collect()
}

/// Of the options, each a set and the fewest cells between its ticks, finest first:
/// the one nearest `spacing` apart and every coarser one, those at least `least`
/// apart.
fn preferred<T>(options: Vec<(T, f64)>, spacing: f64, least: f64) -> Vec<T> {
    let closeness = |gap: f64| (gap / spacing).ln().abs();
    let best = options
        .iter()
        .enumerate()
        .filter(|(_, (_, gap))| *gap >= least)
        .min_by(|a, b| closeness(a.1.1).total_cmp(&closeness(b.1.1)))
        .map(|(i, _)| i);
    match best {
        Some(best) => options.into_iter().skip(best).map(|(set, _)| set).collect(),
        // Nothing is far enough apart: the coarsest, which may still have room.
        None => options
            .into_iter()
            .last()
            .map(|(set, _)| set)
            .into_iter()
            .collect(),
    }
}

/// Nice-number tick sets over `bounds` for an axis `length` cells long.
fn number_sets(
    bounds: [f64; 2],
    numbers: &AxisNumbers,
    widen: bool,
    length: f64,
    spacing: f64,
    least: f64,
    minor_gap: f64,
) -> Vec<TickSet> {
    let [lo, hi] = bounds;
    if hi.partial_cmp(&lo) != Some(std::cmp::Ordering::Greater) || length <= 0.0 {
        // A point or nothing: the one value there is.
        let format = AxisFormat::new(&[lo], numbers);
        let levels = (0..2)
            .map_while(|level| format.label(lo, level).map(|l| vec![l]))
            .collect();
        return vec![TickSet {
            ticks: vec![lo],
            minor: Vec::new(),
            levels,
            bounds,
        }];
    }
    let finest = (hi - lo) * least.min(minor_gap) / length;
    let options: Vec<((f64, [f64; 2]), f64)> = ticks::nice_steps(lo, hi, finest, numbers.whole)
        .into_iter()
        .map(|step| {
            let range = if widen {
                ticks::widen(lo, hi, step)
            } else {
                bounds
            };
            let gap = step / (range[1] - range[0]) * length;
            ((step, range), gap)
        })
        .filter(|((step, range), _)| ticks::multiples(range[0], range[1], *step).len() >= 2)
        .collect();
    let sets = preferred(options, spacing, least)
        .into_iter()
        .map(|(step, range)| {
            let majors = ticks::multiples(range[0], range[1], step);
            let format = AxisFormat::new(&majors, numbers);
            let levels = (0..2)
                .map_while(|level| majors.iter().map(|&v| format.label(v, level)).collect())
                .collect();
            let per_cell = length / (range[1] - range[0]);
            let minor = ticks::minor_steps(step, numbers.whole)
                .into_iter()
                .find(|m| m * per_cell >= minor_gap)
                .map(|m| {
                    ticks::multiples(range[0], range[1], m)
                        .into_iter()
                        .filter(|v| !majors.iter().any(|t| (t - v).abs() < m * 1e-6))
                        .collect()
                })
                .unwrap_or_default();
            TickSet {
                ticks: majors,
                minor,
                levels,
                bounds: range,
            }
        })
        .collect();
    with_ticks(sets)
}

/// Calendar tick sets over `bounds` for a `kind` axis `length` cells long.
fn calendar_sets(
    bounds: [f64; 2],
    kind: XAxisTemporalKind,
    length: f64,
    spacing: f64,
    least: f64,
    minor_gap: f64,
) -> Vec<TickSet> {
    let [lo, hi] = bounds;
    let (Some(start), Some(end)) = (ticks::to_datetime(lo, kind), ticks::to_datetime(hi, kind))
    else {
        return Vec::new();
    };
    if hi.partial_cmp(&lo) != Some(std::cmp::Ordering::Greater) || length <= 0.0 {
        return Vec::new();
    }
    let per_cell = length / (hi - lo);
    // Every step with two ticks or more, finest first, each with the fewest cells
    // between two of its ticks.
    let all: Vec<(
        ticks::CalendarStep,
        Vec<chrono::NaiveDateTime>,
        Vec<f64>,
        f64,
    )> = ticks::calendar_steps(kind)
        .filter_map(|step| {
            let at = ticks::calendar_ticks(start, end, step);
            let values: Vec<f64> = at
                .iter()
                .map(|t| ticks::from_datetime(*t, kind))
                .collect::<Option<_>>()?;
            let gap = values
                .windows(2)
                .map(|w| (w[1] - w[0]) * per_cell)
                .fold(f64::INFINITY, f64::min);
            (values.len() >= 2).then_some((step, at, values, gap))
        })
        .collect();
    let options = all
        .iter()
        .enumerate()
        .map(|(i, (.., gap))| (i, *gap))
        .collect();
    preferred(options, spacing, least)
        .into_iter()
        .map(|i| {
            let (step, at, values, _) = &all[i];
            // Minor ticks: the finest step that ticks at every one of these, and
            // between, with room.
            let minor = all[..i]
                .iter()
                .filter(|(.., gap)| *gap >= minor_gap)
                .find(|(_, _, finer, _)| values.iter().all(|v| finer.contains(v)))
                .map(|(_, _, finer, _)| {
                    finer
                        .iter()
                        .copied()
                        .filter(|v| !values.contains(v))
                        .collect()
                })
                .unwrap_or_default();
            TickSet {
                ticks: values.clone(),
                minor,
                levels: vec![
                    ticks::calendar_labels(at, step.unit, kind, true),
                    ticks::calendar_labels(at, step.unit, kind, false),
                ],
                bounds,
            }
        })
        .collect()
}

/// Where an axis's values land on screen: `cells` cells from `start`, each split in
/// `sub` dots by the marker the plot draws with.
#[derive(Clone, Copy, Debug)]
pub struct Track {
    pub start: u16,
    pub cells: u16,
    pub sub: u16,
}

impl Track {
    /// The cell `f` of the way along, as ratatui's canvas rounds a point to its dot.
    pub fn cell(&self, f: f64) -> u16 {
        let dots = u32::from(self.cells) * u32::from(self.sub.max(1));
        let dot = (f.clamp(0.0, 1.0) * f64::from(dots.saturating_sub(1))).round() as u32;
        self.start + (dot / u32::from(self.sub.max(1))) as u16
    }

    /// Cells from the first value's to the last's.
    fn length(&self) -> f64 {
        let sub = f64::from(self.sub.max(1));
        (f64::from(self.cells) * sub - 1.0).max(0.0) / sub
    }
}

/// The dots per cell, across and down, of a canvas drawn with `marker`.
pub fn resolution(marker: Marker) -> (u16, u16) {
    match marker {
        Marker::Braille | Marker::Octant => (2, 4),
        Marker::Sextant => (2, 3),
        Marker::Quadrant => (2, 2),
        Marker::HalfBlock => (1, 2),
        _ => (1, 1),
    }
}

/// An axis's ticks as placed: each labeled tick's cell and label, and the cells of
/// every major and minor tick.
#[derive(Clone, Debug, Default)]
pub struct Placed {
    pub labels: Vec<(u16, String)>,
    pub majors: Vec<u16>,
    pub minors: Vec<u16>,
    pub bounds: [f64; 2],
}

/// A chart's legend: how many series it names and the widest name.
#[derive(Clone, Copy, Debug)]
pub struct Legend {
    pub width: u16,
    pub rows: u16,
}

/// A chart's two axes and how they are drawn.
pub struct PlotAxes<'a> {
    pub x: AxisSpec<'a>,
    pub y: AxisSpec<'a>,
    /// The axis lines and their tick marks.
    pub line: Style,
    pub labels: Style,
    pub titles: Style,
    /// The grid at the major ticks, in this style; `None` draws none.
    pub grid: Option<Style>,
    /// The marker the series draw with: ticks sit on the cells their values land on.
    pub marker: Marker,
    /// The legend, placed in the corner the series leave emptiest; `None` draws none.
    pub legend: Option<Legend>,
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
    /// The y axis's ticks, labels and range, rows counted on screen.
    pub y: Placed,
    y_width: u16,
}

impl<'a> PlotAxes<'a> {
    /// Plain axes in `style`, as the chart view draws them: no grid, no legend, ticks
    /// placed for `marker`.
    pub fn new(x: AxisSpec<'a>, y: AxisSpec<'a>, style: Style, marker: Marker) -> Self {
        Self {
            x,
            y,
            line: style,
            labels: style,
            titles: style,
            grid: None,
            marker,
            legend: None,
        }
    }

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
        // The plot's rows do not depend on the labels' width; its columns do.
        let rows = graph_area(chart, 0).0;
        let track = Track {
            start: rows.top(),
            cells: rows.height,
            sub: resolution(self.marker).1,
        };
        let y = fit_y_labels(&self.y, track, chart.width / 3);
        let width = y.labels.iter().map(|(_, l)| l.width()).max().unwrap_or(0) as u16;
        let (graph, labels) = graph_area(chart, width);
        PlotFrame {
            y_title,
            chart,
            graph,
            labels,
            x_title,
            y,
            y_width: width,
        }
    }

    /// Draw `chart`'s datasets with these axes in `area`: the grid under them, the
    /// legend over them, then the tick marks, labels and titles.
    pub fn render(&self, chart: Chart<'_>, area: Rect, buf: &mut Buffer, g: &Glyphs) -> PlotFrame {
        let frame = self.frame(area);
        let x_track = Track {
            start: frame.graph.left(),
            cells: frame.graph.width,
            sub: resolution(self.marker).0,
        };
        let x = frame
            .labels
            .map(|row| fit_x_labels(&self.x, (row.left(), row.right()), x_track))
            .unwrap_or_default();
        // Two empty x labels have ratatui keep the label row and draw the x axis line;
        // blank y labels as wide as datui's keep the space left of the y axis, where
        // datui writes them on the rows of their ticks.
        let blank = " ".repeat(frame.y_width as usize);
        let y_labels = if frame.y_width > 0 {
            vec![Span::raw(blank.as_str()), Span::raw(blank.as_str())]
        } else {
            Vec::new()
        };
        let chart = chart
            .x_axis(
                Axis::default()
                    .bounds(self.x.bounds)
                    .style(self.line)
                    .labels(["", ""]),
            )
            .y_axis(
                Axis::default()
                    .bounds(frame.y.bounds)
                    .style(self.line)
                    .labels(y_labels),
            )
            .hidden_legend_constraints((Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)));
        let legend = self
            .legend
            .and_then(|legend| legend_corner(&chart, frame.chart, frame.graph, legend));
        if let Some(style) = self.grid {
            draw_grid(buf, frame.graph, &x.majors, &frame.y.majors, style, g);
        }
        chart.legend_position(legend).render(frame.chart, buf);
        g.plot.redraw_axes(frame.chart, buf);
        draw_tick_marks(buf, &frame, &x, self.line, g);
        let label_x = frame.chart.left();
        for (row, label) in &frame.y.labels {
            let pad = (frame.y_width as usize).saturating_sub(label.width());
            buf.set_string(label_x + pad as u16, *row, label, self.labels);
        }
        if let Some(row) = frame.labels {
            for (x, label) in &x.labels {
                buf.set_string(*x, row.y, label, self.labels);
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

/// The corner of `graph` whose legend-sized patch holds the fewest marks of
/// `chart`'s series; the top right on a tie.
fn legend_corner(
    chart: &Chart<'_>,
    area: Rect,
    graph: Rect,
    legend: Legend,
) -> Option<LegendPosition> {
    let (w, h) = (legend.width + 2, legend.rows + 2);
    if legend.rows == 0 || w > graph.width || h > graph.height {
        return None;
    }
    let mut probe = Buffer::empty(area);
    chart.clone().legend_position(None).render(area, &mut probe);
    let (left, right) = (graph.left(), graph.right() - w);
    let (top, bottom) = (graph.top(), graph.bottom() - h);
    let marks = |x0: u16, y0: u16| {
        (y0..y0 + h)
            .flat_map(|y| (x0..x0 + w).map(move |x| (x, y)))
            .filter(|&(x, y)| !matches!(probe[(x, y)].symbol(), " " | "\u{2800}"))
            .count()
    };
    [
        (LegendPosition::TopRight, right, top),
        (LegendPosition::TopLeft, left, top),
        (LegendPosition::BottomRight, right, bottom),
        (LegendPosition::BottomLeft, left, bottom),
    ]
    .into_iter()
    .min_by_key(|&(_, x, y)| marks(x, y))
    .map(|(corner, ..)| corner)
}

/// The grid: a dotted line across at each y tick and down at each x tick, but not
/// beside the axes, where it would double them. Drawn before the series, whose marks
/// replace it where they fall.
fn draw_grid(
    buf: &mut Buffer,
    graph: Rect,
    columns: &[u16],
    rows: &[u16],
    style: Style,
    g: &Glyphs,
) {
    let rows: Vec<u16> = rows
        .iter()
        .copied()
        .filter(|&y| y + 1 < graph.bottom())
        .collect();
    let columns: Vec<u16> = columns
        .iter()
        .copied()
        .filter(|&x| x > graph.left())
        .collect();
    for &y in &rows {
        for x in graph.left()..graph.right() {
            buf[(x, y)].set_symbol(g.plot.grid_across).set_style(style);
        }
    }
    for &x in &columns {
        for y in graph.top()..graph.bottom() {
            buf[(x, y)].set_symbol(g.plot.grid_down).set_style(style);
        }
    }
}

/// A mark on the axis line at every tick, major or minor.
fn draw_tick_marks(buf: &mut Buffer, frame: &PlotFrame, x: &Placed, style: Style, g: &Glyphs) {
    let graph = frame.graph;
    if graph.left() > frame.chart.left() {
        let column = graph.left() - 1;
        for &row in frame.y.majors.iter().chain(&frame.y.minors) {
            let cell = &mut buf[(column, row)];
            if cell.symbol() == g.plot.axis.vertical {
                cell.set_symbol(g.plot.tick_y).set_style(style);
            }
        }
    }
    let row = graph.bottom();
    if row < frame.chart.bottom() {
        for &column in x.majors.iter().chain(&x.minors) {
            let cell = &mut buf[(column, row)];
            if cell.symbol() == g.plot.axis.horizontal {
                cell.set_symbol(g.plot.tick_x).set_style(style);
            }
        }
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

/// Where `v` falls along `bounds`, from 0 to 1.
fn fraction(v: f64, [lo, hi]: [f64; 2]) -> f64 {
    if hi > lo { (v - lo) / (hi - lo) } else { 0.0 }
}

/// The y labels for a plot whose rows are `track`, with at most `width` cells left of
/// its axis: about one per four rows, each on its tick's row, in the fullest form
/// that fits the width and tells them apart.
pub fn fit_y_labels(axis: &AxisSpec<'_>, track: Track, width: u16) -> Placed {
    let groups = axis.tick_sets(track, Y_SPACING, Y_LEAST, Y_MINOR_GAP);
    let place = |set: &TickSet, labels: Option<&Vec<String>>| {
        let row = |v: f64| track.cell(1.0 - fraction(v, set.bounds));
        Placed {
            labels: labels
                .map(|labels| {
                    set.ticks
                        .iter()
                        .zip(labels)
                        .map(|(&v, l)| (row(v), format!("{l:>w$}", w = axis.pad)))
                        .collect()
                })
                .unwrap_or_default(),
            majors: set.ticks.iter().map(|&v| row(v)).collect(),
            minors: set.minor.iter().map(|&v| row(v)).collect(),
            bounds: set.bounds,
        }
    };
    let fits = |set: &TickSet, labels: &Vec<String>| {
        let rows: Vec<u16> = set
            .ticks
            .iter()
            .map(|&v| track.cell(1.0 - fraction(v, set.bounds)))
            .collect();
        set.ticks.len() <= usize::from(track.cells.max(2))
            && rows.windows(2).all(|w| w[0] != w[1])
            && labels
                .iter()
                .all(|l| l.width().max(axis.pad) <= width as usize)
            && distinct(labels.iter())
    };
    for set in groups.iter().flatten() {
        if let Some(labels) = set.levels.iter().find(|labels| fits(set, labels)) {
            return place(set, Some(labels));
        }
    }
    // Nothing fits: the first set in its fullest form, cut short by the plot.
    match groups.iter().flatten().next() {
        Some(set) => place(set, set.levels.first()),
        None => Placed {
            bounds: axis.bounds,
            ..Placed::default()
        },
    }
}

/// Neighbors differ; two alike say nothing about the space between them.
fn distinct<'a>(labels: impl Iterator<Item = &'a String>) -> bool {
    let labels: Vec<_> = labels.collect();
    labels.windows(2).all(|w| w[0] != w[1])
}

/// The x labels that fit on a row spanning columns `span` (start, end) under a plot
/// whose columns are `track`, each with its column: centered under its tick, kept on
/// the row, `LABEL_GAP` cells from the next. About one label per 15 columns; a
/// coarser step when they crowd, then shorter forms; none at all when even two do
/// not fit.
pub fn fit_x_labels(axis: &AxisSpec<'_>, span: (u16, u16), track: Track) -> Placed {
    let (start, end) = span;
    let column = |v: f64| track.cell(fraction(v, axis.bounds));
    let groups = axis.tick_sets(track, X_SPACING, X_LEAST, X_MINOR_GAP);
    for sets in &groups {
        for level in 0..MAX_LEVELS {
            for set in sets {
                let Some(labels) = set.levels.get(level) else {
                    continue;
                };
                let mut placed: Vec<(u16, String)> = Vec::with_capacity(labels.len());
                let mut next_free = start;
                let fits = set.ticks.iter().zip(labels).all(|(&v, label)| {
                    let w = label.width() as u16;
                    if w > end.saturating_sub(start) {
                        return false;
                    }
                    let x = column(v).saturating_sub(w / 2).clamp(start, end - w);
                    let clear =
                        x >= next_free && placed.last().is_none_or(|(_, prev)| prev != label);
                    next_free = x + w + LABEL_GAP;
                    placed.push((x, label.clone()));
                    clear
                });
                if fits {
                    return Placed {
                        labels: placed,
                        majors: set.ticks.iter().map(|&v| column(v)).collect(),
                        minors: set.minor.iter().map(|&v| column(v)).collect(),
                        bounds: axis.bounds,
                    };
                }
            }
        }
    }
    Placed {
        bounds: axis.bounds,
        ..Placed::default()
    }
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
    use crate::chart_data::{XAxisTemporalKind, x_axis_label_at};
    use ratatui::widgets::{Dataset, GraphType};

    /// Days since the epoch of 2020-01-01 and 2024-12-31.
    const FIVE_YEARS: [f64; 2] = [18262.0, 20088.0];

    /// Date labels for fixed ticks on an axis spanning `bounds`.
    fn dates(bounds: (f64, f64)) -> TickLabel<'static> {
        let numbers = AxisFormat::new(&[], &AxisNumbers::default());
        Box::new(move |v, level| {
            x_axis_label_at(v, XAxisTemporalKind::Date, bounds, level, &numbers)
        })
    }

    /// A plot starting four cells in, past the y labels and the axis, one dot a cell.
    fn track(width: u16) -> Track {
        Track {
            start: 4,
            cells: width - 4,
            sub: 1,
        }
    }

    fn labels_on(axis: &AxisSpec<'_>, width: u16) -> Vec<String> {
        fit_x_labels(axis, (0, width), track(width))
            .labels
            .into_iter()
            .map(|(_, l)| l)
            .collect()
    }

    /// Fixed ticks drop the middle first, then shorten; the ends stay while anything
    /// fits.
    #[test]
    fn fixed_labels_drop_the_middle_then_shorten() {
        let [lo, hi] = FIVE_YEARS;
        let axis = AxisSpec::ends_and_middle(FIVE_YEARS, dates((lo, hi)), "");
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
        let axis = AxisSpec::fixed(
            [-0.5, 6.5],
            (0..7).map(f64::from).collect(),
            Box::new(names),
            "",
        );
        for width in 10..120 {
            let placed = fit_x_labels(&axis, (0, width), track(width)).labels;
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

    /// The step behind a set of labels, when they are evenly stepped numbers.
    fn step_of(labels: &[String]) -> f64 {
        let values: Vec<f64> = labels.iter().map(|l| l.parse().unwrap()).collect();
        let step = values[1] - values[0];
        for w in values.windows(2) {
            assert!((w[1] - w[0] - step).abs() < 1e-9, "uneven: {labels:?}");
        }
        step
    }

    /// 1, 2, 2.5 or 5 times a power of ten, and every value a multiple of it.
    fn assert_nice(labels: &[String]) {
        let step = step_of(labels);
        let mantissa = step / 10f64.powf(step.log10().floor());
        assert!(
            [1.0, 2.0, 2.5, 5.0]
                .iter()
                .any(|m| (mantissa - m).abs() < 1e-9),
            "step {step}: {labels:?}"
        );
        for l in labels {
            let k = l.parse::<f64>().unwrap() / step;
            assert!((k - k.round()).abs() < 1e-9, "{l} off the step {step}");
        }
    }

    /// The x axis carries about one label per 15 columns, every one a nice value:
    /// three or so at 60 columns, near twenty at 300.
    #[test]
    fn x_tick_density_follows_the_width() {
        let axis = AxisSpec::numbers([3.0, 997.0], &AxisNumbers::default(), "");
        for (width, least, most) in [(40, 2, 4), (60, 3, 5), (120, 6, 10), (300, 15, 22)] {
            let labels = labels_on(&axis, width);
            assert!(
                (least..=most).contains(&labels.len()),
                "{width} columns: {labels:?}"
            );
            assert_nice(&labels);
        }
        // Minor ticks between the labeled ones, where there is room.
        let wide = fit_x_labels(&axis, (0, 400), track(400));
        assert!(wide.minors.len() >= wide.majors.len(), "{wide:?}");
        let narrow = fit_x_labels(&axis, (0, 40), track(40));
        assert!(narrow.minors.iter().all(|m| !narrow.majors.contains(m)));
    }

    /// A y axis widens to the nice values either side of its data and labels about
    /// one row in four.
    #[test]
    fn y_tick_density_follows_the_height() {
        let axis = AxisSpec::y_numbers([3.0, 997.0], &AxisNumbers::default(), "");
        for rows in [12u16, 20, 40, 76] {
            let track = Track {
                start: 0,
                cells: rows,
                sub: 4,
            };
            let placed = fit_y_labels(&axis, track, 20);
            let labels: Vec<String> = placed
                .labels
                .iter()
                .map(|(_, l)| l.trim().to_string())
                .collect();
            let per_label = f64::from(rows) / labels.len() as f64;
            assert!((2.2..=7.0).contains(&per_label), "{rows} rows: {labels:?}");
            assert_nice(&labels);
            // Bottom up, the ends of the widened axis on the last and first rows.
            assert_eq!(placed.labels[0], (rows - 1, "0".to_string()), "{placed:?}");
            let (top, label) = placed.labels.last().unwrap();
            assert_eq!(*top, 0, "{rows} rows: {placed:?}");
            assert_eq!(placed.bounds[0], 0.0);
            assert!(placed.bounds[1] >= 997.0);
            assert_eq!(&placed.bounds[1].to_string(), label);
        }
    }

    /// Whole numbers tick whole, however much room there is.
    #[test]
    fn whole_numbers_tick_whole() {
        let whole = AxisNumbers {
            whole: true,
            ..AxisNumbers::default()
        };
        let axis = AxisSpec::numbers([0.0, 7.0], &whole, "");
        assert_eq!(
            labels_on(&axis, 200),
            ["0", "1", "2", "3", "4", "5", "6", "7"]
        );
        assert_eq!(labels_on(&axis, 30), ["0", "5"]);
        let axis = AxisSpec::y_numbers([3.0, 10.0], &whole, "");
        let track = Track {
            start: 0,
            cells: 9,
            sub: 1,
        };
        let placed = fit_y_labels(&axis, track, 10);
        assert_eq!(
            placed.labels,
            [
                (8, "0".to_string()),
                (4, "5".to_string()),
                (0, "10".to_string())
            ]
        );
    }

    /// A date axis ticks on calendar boundaries and labels the unit that turns: the
    /// years on a narrow plot, half years between them on a wide one; the dates at
    /// the ends when nothing else fits.
    #[test]
    fn date_ticks_fall_on_the_calendar() {
        let axis = AxisSpec::calendar(
            FIVE_YEARS,
            XAxisTemporalKind::Date,
            &AxisNumbers::default(),
            "",
        );
        assert_eq!(
            labels_on(&axis, 80),
            ["2020", "2021", "2022", "2023", "2024"]
        );
        let wide = labels_on(&axis, 300);
        assert_eq!(
            wide[..4],
            ["2020", "Apr", "Jul", "Oct"],
            "quarters, the year at its turn: {wide:?}"
        );
        assert!(wide.contains(&"2024".to_string()), "{wide:?}");
        let narrow = labels_on(&axis, 16);
        assert_eq!(narrow, ["2020", "2024"], "{narrow:?}");

        // Inside a month, days; the first tick names the month and year.
        let march = [19783.0, 19813.0]; // 2024-03-01 to 2024-03-31
        let axis = AxisSpec::calendar(march, XAxisTemporalKind::Date, &AxisNumbers::default(), "");
        let labels = labels_on(&axis, 80);
        assert_eq!(labels[0], "Mar 1 2024", "{labels:?}");
        assert!(
            labels[1..].iter().all(|l| l.parse::<u32>().is_ok()),
            "{labels:?}"
        );

        // A day of microsecond timestamps ticks on the hours.
        let day = [1_709_251_200e6, 1_709_337_600e6]; // 2024-03-01T00:00 to 03-02T00:00
        let axis = AxisSpec::calendar(
            day,
            XAxisTemporalKind::DatetimeUs,
            &AxisNumbers::default(),
            "",
        );
        let labels = labels_on(&axis, 120);
        assert_eq!(labels[0], "Mar 1 2024", "{labels:?}");
        assert!(labels.contains(&"12:00".to_string()), "{labels:?}");
        assert_eq!(labels.last().unwrap(), "Mar 2", "{labels:?}");
    }

    /// A short form that says the same at both ends is no label: numbers that round
    /// alike show none.
    #[test]
    fn short_forms_tell_ticks_apart() {
        let axis = AxisSpec::numbers([1000.1, 1000.3], &AxisNumbers::default(), "");
        assert!(labels_on(&axis, 12).is_empty());
        assert_eq!(labels_on(&axis, 40), ["1000.1", "1000.2", "1000.3"]);
    }

    fn axes<'a>(grid: bool) -> PlotAxes<'a> {
        PlotAxes {
            grid: grid.then(Style::default),
            ..PlotAxes::new(
                AxisSpec::numbers([0.0, 10.0], &AxisNumbers::default(), "x title"),
                AxisSpec::y_numbers([0.0, 1000.0], &AxisNumbers::default(), "y title"),
                Style::default(),
                Marker::Braille,
            )
        }
    }

    fn render_with(axes: &PlotAxes<'_>, area: Rect, g: &Glyphs) -> (Buffer, PlotFrame) {
        let points = [(0.0, 0.0), (10.0, 1000.0)];
        let chart = Chart::new(vec![
            Dataset::default()
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .data(&points),
        ]);
        let mut buf = Buffer::empty(area);
        let frame = axes.render(chart, area, &mut buf, g);
        (buf, frame)
    }

    fn render(area: Rect, g: &Glyphs) -> (Buffer, PlotFrame) {
        render_with(&axes(false), area, g)
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

    /// Each tick carries a mark on its axis line, and its label sits beside it: the
    /// y label on the tick's row, the x label centered under its column.
    #[test]
    fn labels_sit_on_their_tick_marks() {
        for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
            let (buf, frame) = render(Rect::new(0, 0, 60, 20), g);
            let axis_x = frame.graph.left() - 1;
            for (row, label) in &frame.y.labels {
                assert_eq!(buf[(axis_x, *row)].symbol(), g.plot.tick_y, "{label}");
                let text: String = (frame.chart.left()..axis_x)
                    .map(|x| buf[(x, *row)].symbol())
                    .collect();
                assert_eq!(text.trim(), label.trim());
            }
            let row = frame.graph.bottom();
            let ticks: Vec<u16> = (frame.graph.left()..frame.graph.right())
                .filter(|&x| buf[(x, row)].symbol() == g.plot.tick_x)
                .collect();
            assert!(ticks.len() >= 3, "{ticks:?}");
        }
    }

    fn text(buf: &Buffer) -> Vec<String> {
        let area = buf.area;
        (area.top()..area.bottom())
            .map(|y| {
                (area.left()..area.right())
                    .map(|x| buf[(x, y)].symbol())
                    .collect()
            })
            .collect()
    }

    /// The grid draws at the major ticks when on and not at all when off, in either
    /// glyph set, and never takes a cell the series drew in.
    #[test]
    fn the_grid_toggles_and_never_hides_a_series() {
        for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
            let area = Rect::new(0, 0, 60, 20);
            let (off, frame) = render_with(&axes(false), area, g);
            let (on, _) = render_with(&axes(true), area, g);
            let grid = |buf: &Buffer| {
                let all = text(buf).concat();
                all.matches(g.plot.grid_across).count() + all.matches(g.plot.grid_down).count()
            };
            assert_eq!(grid(&off), 0, "{:#?}", text(&off));
            assert!(grid(&on) > 50, "{:#?}", text(&on));
            let graph = frame.graph;
            for y in graph.top()..graph.bottom() {
                for x in graph.left()..graph.right() {
                    let mark = off[(x, y)].symbol();
                    if !matches!(mark, " " | "\u{2800}") {
                        assert_eq!(on[(x, y)].symbol(), mark, "({x}, {y})");
                    }
                }
            }
            // Down at each labeled x tick but the one against the y axis; across at
            // each y tick but the one on the x axis.
            for (row, _) in &frame.y.labels {
                let across = (graph.left()..graph.right())
                    .filter(|&x| on[(x, *row)].symbol() == g.plot.grid_across)
                    .count();
                if row + 1 < graph.bottom() {
                    assert!(across > graph.width as usize / 2, "row {row}");
                } else {
                    assert_eq!(across, 0, "the x axis row");
                }
            }
        }
    }

    /// The legend takes the corner the series leave emptiest: a line rising to the
    /// right leaves the top left.
    #[test]
    fn the_legend_takes_the_emptiest_corner() {
        let g = crate::glyphs::unicode();
        let mut axes = axes(false);
        axes.legend = Some(Legend { width: 6, rows: 2 });
        let rising: Vec<(f64, f64)> = (0..=100)
            .map(|i| (f64::from(i) / 10.0, f64::from(i) * 10.0))
            .collect();
        let chart = Chart::new(vec![
            Dataset::default()
                .name("first ")
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .data(&rising),
            Dataset::default()
                .name("second")
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .data(&rising),
        ]);
        let area = Rect::new(0, 0, 60, 24);
        let mut buf = Buffer::empty(area);
        let frame = axes.render(chart, area, &mut buf, g);
        let corner = (frame.graph.left(), frame.graph.top());
        assert_eq!(buf[corner].symbol(), "┌", "{:#?}", text(&buf));
    }

    /// A title longer than its row is cut with the set's ellipsis.
    #[test]
    fn a_long_title_is_cut_to_its_row() {
        assert_eq!(cut("a long title", 6, crate::glyphs::unicode()), "a lon…");
        assert_eq!(cut("a long title", 6, crate::glyphs::ascii()), "a l...");
        assert_eq!(cut("short", 6, crate::glyphs::ascii()), "short");
    }
}

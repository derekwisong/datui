//! What a chart plots. A worker prepares the data ([`PlotData`]), which the chart
//! cache keeps; [`plot`] gives it its axes for the view, titled and numbered as the
//! table prints the columns, and the screen and every export draw that [`Plot`].

use std::borrow::Cow;

use polars::prelude::{DataType, Schema};

use crate::chart::chart_data::{
    self, BarData, BoxPlotData, ChartXRangeResult, HeatmapData, HistogramData, KdeData, RowsRead,
    XAxisTemporalKind,
};
use crate::chart::chart_modal::{Aggregate, ChartModal, ChartSpec, Mark};
use crate::numfmt;
use crate::widgets::axis_numbers::AxisNumbers;

/// A chart's data, as prepared off the UI thread. Each payload names the columns it
/// was drawn from, since the axes are titled from it.
#[derive(Debug, Clone)]
pub enum PlotData {
    Lines(LinesData),
    /// A line or scatter chart's X alone: its range, for the axes to stand over.
    XRange(ChartXRangeResult),
    Histogram(HistogramData),
    Box(BoxPlotData),
    Kde(KdeData),
    Heatmap(HeatmapData),
    Bars(BarData),
}

/// Each series' points.
pub type Points = Vec<Vec<(f64, f64)>>;

/// A line or scatter chart's series.
#[derive(Debug, Clone, Default)]
pub struct LinesData {
    /// One per series: its Y column, or its color group.
    pub names: Vec<String>,
    pub series: Points,
    /// The series as a log scale or a clipped range draws them, and which. Only the
    /// chart on screen keeps one (`ChartCache::touch`): it is as large as the series.
    pub series_shown: Option<(Shown, Points)>,
    /// The range of the Ys that are not outliers, when some are.
    pub y_clip: Option<YClip>,
    /// Per series, where its line starts again after a gap (see `chart_data::segments`).
    pub breaks: Vec<Vec<usize>>,
    /// Every X of every series, in order, each once: where the crosshair stops.
    pub xs: Vec<f64>,
    /// The least and greatest X and Y over every point, before any log.
    pub bounds: Option<[f64; 4]>,
    pub x_axis_kind: XAxisTemporalKind,
    pub rows: RowsRead,
    /// What an aggregate over every row read, said in the title row.
    pub rows_note: Option<String>,
    /// The last series is Other: every value of a color without a series of its own.
    pub other: bool,
}

/// How a line or scatter chart draws its Y values: on a log scale, and clipped to
/// [`LinesData::y_clip`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Shown {
    pub log: bool,
    pub clip: bool,
}

/// The Y range of the points that are not outliers, and how many are: a clipped
/// chart draws those at its edge.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct YClip {
    pub lo: f64,
    pub hi: f64,
    pub outside: usize,
}

impl YClip {
    /// The range of `series`'s Y values that are not outliers, as Distribution counts
    /// them; `None` when none is.
    fn of(series: &[Vec<(f64, f64)>]) -> Option<Self> {
        let ys: Vec<f64> = series
            .iter()
            .flatten()
            .map(|&(_, y)| y)
            .filter(|y| y.is_finite())
            .collect();
        let (low, high) = crate::analysis::statistics::outlier_bounds(&ys)?;
        let inside = |y: &&f64| (low..=high).contains(*y);
        let lo = ys
            .iter()
            .filter(inside)
            .copied()
            .fold(f64::INFINITY, f64::min);
        let hi = ys
            .iter()
            .filter(inside)
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let outside = ys.iter().filter(|y| !inside(y)).count();
        (outside > 0 && hi > lo).then_some(Self { lo, hi, outside })
    }
}

/// One series of [`LinesData`] that has points.
#[derive(Debug, Clone, Copy)]
pub struct Drawn<'a> {
    /// Its place among every series, which picks its color on screen: an empty
    /// series before it keeps its color too.
    pub index: usize,
    pub name: &'a str,
    pub points: &'a [(f64, f64)],
    pub breaks: &'a [usize],
    pub other: bool,
}

impl LinesData {
    /// The series a worker grouped, with what is drawn from them worked out now:
    /// their X values in order and their bounds.
    pub fn new(grouped: chart_data::GroupedSeries, rows_note: Option<String>) -> Self {
        Self {
            series_shown: None,
            y_clip: YClip::of(&grouped.series),
            xs: crate::widgets::crosshair::xs(&grouped.series),
            bounds: extent(&grouped.series),
            names: grouped.names,
            series: grouped.series,
            breaks: grouped.breaks,
            x_axis_kind: grouped.x_axis_kind,
            rows: grouped.rows,
            rows_note,
            other: grouped.other,
        }
    }

    /// What `shown` draws of these series: no clip when no point lies past the
    /// percentiles.
    fn wanted(&self, shown: Shown) -> Shown {
        Shown {
            clip: shown.clip && self.y_clip.is_some(),
            ..shown
        }
    }

    /// What the series are drawn as now: as `shown` asks once a copy of them is kept,
    /// and as they are before that.
    fn drawn_as(&self, shown: Shown) -> Shown {
        let wanted = self.wanted(shown);
        match &self.series_shown {
            Some((kept, _)) if *kept == wanted => wanted,
            _ => Shown::default(),
        }
    }

    /// Keep a copy of the series as `shown` draws them when that differs from the
    /// series, and none otherwise.
    pub fn keep_shown(&mut self, shown: Shown) {
        let wanted = self.wanted(shown);
        if wanted == Shown::default() {
            self.series_shown = None;
        } else if self.drawn_as(shown) != wanted {
            let clip = self.y_clip.filter(|_| wanted.clip);
            let series = self
                .series
                .iter()
                .map(|points| {
                    points
                        .iter()
                        .map(|&(x, y)| (x, shown_y(y, wanted.log, clip)))
                        .collect()
                })
                .collect();
            self.series_shown = Some((wanted, series));
        }
    }

    /// The series as drawn: logged on a log scale, clipped to the percentiles.
    pub fn shown(&self, shown: Shown) -> &[Vec<(f64, f64)>] {
        match &self.series_shown {
            Some((kept, series)) if *kept == self.wanted(shown) => series,
            _ => &self.series,
        }
    }

    /// The least and greatest X and Y of the points as drawn. The log and the clip
    /// are monotone: the bounds of the points drawn are the bounds drawn.
    pub fn shown_bounds(&self, shown: Shown) -> Option<[f64; 4]> {
        let drawn = self.drawn_as(shown);
        let clip = self.y_clip.filter(|_| drawn.clip);
        match self.bounds {
            Some([x0, x1, y0, y1]) => Some([
                x0,
                x1,
                shown_y(y0, drawn.log, clip),
                shown_y(y1, drawn.log, clip),
            ]),
            None => extent(self.shown(shown)),
        }
    }

    /// The series with points, as drawn.
    pub fn drawn(&self, shown: Shown) -> impl Iterator<Item = Drawn<'_>> {
        let last = self.names.len().saturating_sub(1);
        self.shown(shown)
            .iter()
            .zip(self.names.iter())
            .enumerate()
            .filter(|(_, (points, _))| !points.is_empty())
            .map(move |(index, (points, name))| Drawn {
                index,
                name,
                points,
                breaks: self.breaks.get(index).map_or(&[][..], Vec::as_slice),
                other: self.other && index == last,
            })
    }
}

/// The least and greatest X and Y over every point; `None` with no points.
pub fn extent(series: &[Vec<(f64, f64)>]) -> Option<[f64; 4]> {
    let mut points = series.iter().flatten().peekable();
    points.peek()?;
    Some(points.fold(
        [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ],
        |[x0, x1, y0, y1], &(x, y)| [x0.min(x), x1.max(x), y0.min(y), y1.max(y)],
    ))
}

/// A Y as the log scale draws it: `ln(1 + y)`, negatives at zero.
fn log_y(y: f64) -> f64 {
    y.max(0.0).ln_1p()
}

/// A Y as drawn: at the edge of `clip` past it, then on the log scale.
fn shown_y(y: f64, log: bool, clip: Option<YClip>) -> f64 {
    let y = clip.map_or(y, |clip| y.clamp(clip.lo, clip.hi));
    if log { log_y(y) } else { y }
}

impl PlotData {
    /// Nothing to draw: no points, bins, boxes, cells or bars.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Lines(lines) => lines.drawn(Shown::default()).next().is_none(),
            Self::XRange(_) => true,
            Self::Bars(data) => data.bars.is_empty(),
            Self::Histogram(data) => data.bins.is_empty(),
            Self::Kde(data) => data.series.is_empty(),
            Self::Box(data) => data.stats.is_empty(),
            Self::Heatmap(data) => data.counts.is_empty() || data.max_count <= 0.0,
        }
    }

    /// What the chart says under the plot about the rows and values it drew;
    /// `middot` joins a sample's seed on.
    pub fn notes(&self, middot: &str) -> Vec<String> {
        let rows_of = |rows: usize| crate::home::discover::format_rows(rows);
        match self {
            Self::Bars(d) => {
                let mut notes = chart_data::chart_notes(&d.rows, None, middot);
                if let Some(note) = &d.rows_note {
                    notes.push(note.clone());
                } else if let Some(rows) = d.counted {
                    notes.push(format!("counts of {} rows", rows_of(rows)));
                } else if d.rows.sample_size.is_none() && !d.value_column.is_empty() {
                    notes.push(format!(
                        "all {} rows",
                        numfmt::group_chrome(d.rows.total_rows)
                    ));
                }
                if d.no_value > 0 {
                    let noun = if d.no_value == 1 {
                        "category"
                    } else {
                        "categories"
                    };
                    notes.push(format!(
                        "{} {noun} without a value",
                        numfmt::group_chrome(d.no_value)
                    ));
                }
                notes
            }
            Self::Lines(c) if c.rows_note.is_some() => c.rows_note.iter().cloned().collect(),
            Self::Lines(c) => chart_data::chart_notes(&c.rows, None, middot),
            Self::XRange(c) => chart_data::chart_notes(&c.rows, None, middot),
            Self::Histogram(d) => chart_data::chart_notes(&d.rows, d.clipped.as_ref(), middot),
            Self::Box(d) => {
                let mut notes = chart_data::chart_notes(&d.rows, d.clipped.as_ref(), middot);
                if d.of > 0 {
                    notes.push(format!(
                        "the {} largest of {} categories",
                        d.stats.len(),
                        numfmt::group_chrome(d.of)
                    ));
                }
                notes
            }
            Self::Kde(d) => chart_data::chart_notes(&d.rows, d.clipped.as_ref(), middot),
            Self::Heatmap(d) => chart_data::chart_notes(&d.rows, None, middot),
        }
    }
}

/// One axis: its title, what its numbers are, and whether they are dates.
#[derive(Debug, Clone, Default)]
pub struct Axis {
    pub title: String,
    pub numbers: AxisNumbers,
    pub kind: XAxisTemporalKind,
    /// Values are `ln(1 + y)`; ticks name `y`.
    pub log: bool,
    /// Outliers are drawn at the edge.
    pub clip: bool,
}

/// A chart as the screen and every export draw it: its data, borrowed from the chart
/// cache on screen and owned by an export, which outlives the frame, and its axes.
/// A bar chart's value axis is its X.
#[derive(Debug, Clone)]
pub struct Plot<'a> {
    pub data: Cow<'a, PlotData>,
    pub x: Axis,
    pub y: Axis,
    pub scatter: bool,
    pub y_from_zero: bool,
}

impl Plot<'_> {
    /// The plot with its data its own.
    pub fn into_owned(self) -> Plot<'static> {
        Plot {
            data: Cow::Owned(self.data.into_owned()),
            ..self
        }
    }

    /// How the Y axis draws a line or scatter chart's values.
    pub fn shown(&self) -> Shown {
        Shown {
            log: self.y.log,
            clip: self.y.clip,
        }
    }

    /// A line or scatter chart's series, as the Y axis draws them.
    pub fn drawn(&self) -> impl Iterator<Item = Drawn<'_>> {
        let lines = match &*self.data {
            PlotData::Lines(lines) => Some(lines),
            _ => None,
        };
        let shown = self.shown();
        lines.into_iter().flat_map(move |lines| lines.drawn(shown))
    }
    /// The plot's curves, Other first so the rest are drawn over it: a line or
    /// scatter chart's series with points, the density curves, or a histogram's
    /// groups as the outlines of their bars.
    pub fn curves(&self) -> Vec<Curve<'_>> {
        let curves: Vec<Curve<'_>> = match &*self.data {
            PlotData::Lines(_) => self
                .drawn()
                .enumerate()
                .map(|(slot, s)| Curve {
                    index: s.index,
                    slot,
                    points: Cow::Borrowed(s.points),
                    breaks: s.breaks,
                    other: s.other,
                })
                .collect(),
            PlotData::Kde(data) => data
                .series
                .iter()
                .enumerate()
                .map(|(i, s)| Curve {
                    index: i,
                    slot: i,
                    points: Cow::Borrowed(&s.points),
                    breaks: &[],
                    other: data.other && i + 1 == data.series.len(),
                })
                .collect(),
            PlotData::Histogram(data) if !data.groups.is_empty() => data
                .step_outlines()
                .into_iter()
                .enumerate()
                .map(|(i, points)| Curve {
                    index: i,
                    slot: i,
                    points: Cow::Owned(points),
                    breaks: &[],
                    other: data.other && i + 1 == data.groups.len(),
                })
                .collect(),
            _ => Vec::new(),
        };
        let other = curves.iter().position(|c| c.other);
        let mut curves: Vec<Option<Curve<'_>>> = curves.into_iter().map(Some).collect();
        chart_data::drawing_order(curves.len(), other)
            .filter_map(|i| curves[i].take())
            .collect()
    }

    /// The names a legend lists, in order: a series, a color group or a curve each.
    pub fn legend(&self) -> Vec<&str> {
        match &*self.data {
            PlotData::Lines(_) => self.drawn().map(|s| s.name).collect(),
            PlotData::Bars(data) => data.groups.iter().map(String::as_str).collect(),
            PlotData::Histogram(data) => data.groups.iter().map(|g| g.name.as_str()).collect(),
            PlotData::Kde(data) => data.series.iter().map(|s| s.name.as_str()).collect(),
            PlotData::Box(_) | PlotData::Heatmap(_) | PlotData::XRange(_) => Vec::new(),
        }
    }

    /// Where Other is in the legend: last, when it has one.
    pub fn other_at(&self) -> Option<usize> {
        match &*self.data {
            PlotData::Lines(_) => self.drawn().position(|s| s.other),
            PlotData::Bars(data) => chart_data::other_at(data.other, data.groups.len()),
            _ => self.curves().iter().find(|c| c.other).map(|c| c.slot),
        }
    }
}

/// A line a plot draws through its data, as every device draws it.
#[derive(Debug, Clone)]
pub struct Curve<'a> {
    /// Its place among every series, which colors it on screen: an empty series
    /// before it keeps its color too.
    pub index: usize,
    /// Its place in the legend, which names and colors it in a file.
    pub slot: usize,
    pub points: Cow<'a, [(f64, f64)]>,
    /// Where it starts again after a gap.
    pub breaks: &'a [usize],
    /// Other: every value of a color without a series of its own, drawn under the rest.
    pub other: bool,
}

/// What a plot is drawn from beside its data: the panel's options, the spec, and
/// how the view's columns print.
pub struct PlotContext<'a> {
    pub modal: &'a ChartModal,
    /// The spec drawn: on screen, with the picker's choice previewed.
    pub spec: &'a ChartSpec,
    pub numbers: &'a numfmt::NumberFormatSettings,
    pub schema: Option<&'a Schema>,
}

/// The plot of `data` for the spec: a line or scatter chart's axes always, standing
/// empty until its series arrive; any other kind once its data has.
pub fn plot<'a>(data: Option<&'a PlotData>, context: &PlotContext<'_>) -> Option<Plot<'a>> {
    let PlotContext {
        modal,
        spec,
        numbers,
        schema,
    } = *context;
    let column = |name: &str| AxisNumbers::column(numbers, schema, name);
    let title = |name: &str| modal.axis_title(name);
    let axis = |title: String, numbers: AxisNumbers| Axis {
        title,
        numbers,
        ..Default::default()
    };
    let encoding = &spec.encoding;
    let x_name = encoding.x.field.as_deref();
    let ys = &encoding.y.field;
    let (x, y) = match (spec.mark, data) {
        (Mark::Line | Mark::Scatter, data) => return Some(lines(data, context)),
        (Mark::Histogram, Some(PlotData::Histogram(data))) => (
            axis(title(&data.column), column(&data.column)),
            if data.share {
                axis("Share".to_string(), AxisNumbers::measure(numbers, "Share"))
            } else {
                axis("Count".to_string(), AxisNumbers::count(numbers))
            },
        ),
        (Mark::Kde, Some(PlotData::Kde(_))) => (
            axis(
                x_name.map(title).unwrap_or_default(),
                x_name.map(column).unwrap_or_default().fractional(),
            ),
            axis(
                "Density".to_string(),
                AxisNumbers::measure(numbers, "Density"),
            ),
        ),
        (Mark::Box, Some(PlotData::Box(_))) => (
            axis(
                x_name.unwrap_or_default().to_string(),
                AxisNumbers::default(),
            ),
            axis(
                ys.first().map(|y| title(y)).unwrap_or_default(),
                AxisNumbers::columns(numbers, schema, ys),
            ),
        ),
        (Mark::Heatmap, Some(PlotData::Heatmap(data))) => (
            axis(title(&data.x_column), column(&data.x_column)),
            axis(title(&data.y_column), column(&data.y_column)),
        ),
        (Mark::Bar, Some(PlotData::Bars(data))) => (
            axis(
                data.value_column.clone(),
                AxisNumbers {
                    format: data.value_format(numbers),
                    whole: data.value_dtype.is_integer(),
                },
            ),
            Axis::default(),
        ),
        _ => return None,
    };
    Some(Plot {
        data: Cow::Borrowed(data?),
        x,
        y,
        scatter: false,
        y_from_zero: false,
    })
}

/// A line or scatter chart's series and axes; the axes alone, over the X column's
/// range or typed from the schema, while the series are on their way.
fn lines<'a>(data: Option<&'a PlotData>, context: &PlotContext<'_>) -> Plot<'a> {
    let PlotContext {
        modal,
        spec,
        numbers,
        schema,
    } = *context;
    let encoding = &spec.encoding;
    let ys = &encoding.y.field;
    let aggregate = encoding.y.aggregate;
    let y_numbers = match aggregate {
        Aggregate::Count | Aggregate::Distinct => AxisNumbers::count(numbers),
        // A mean or median of whole numbers is not whole.
        a if a.is_fractional() => AxisNumbers::columns(numbers, schema, ys).fractional(),
        _ => AxisNumbers::columns(numbers, schema, ys),
    };
    let y_title = if aggregate == Aggregate::Count {
        "count".to_string()
    } else {
        ys.iter()
            .map(|y| modal.axis_title(y))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Durations stay durations through any aggregate but a count.
    let durations = !ys.is_empty()
        && ys.iter().all(|y| {
            schema
                .and_then(|s| s.get(y))
                .is_some_and(|d| matches!(d, DataType::Duration(_)))
        });
    let y_kind = if durations && !aggregate.takes_any_y() && aggregate != Aggregate::Count {
        XAxisTemporalKind::Duration
    } else {
        XAxisTemporalKind::Numeric
    };
    let x_name = encoding.x.field.as_deref();
    let kind = match data {
        Some(PlotData::Lines(lines)) => lines.x_axis_kind,
        Some(PlotData::XRange(range)) => range.x_axis_kind,
        // Still on its way: typed from the schema so the labels are right.
        _ => match (x_name, schema) {
            (Some(x), Some(schema)) => chart_data::x_axis_temporal_kind_for_column(schema, x),
            _ => XAxisTemporalKind::Numeric,
        },
    };
    let shown = modal.shown();
    let data = match data {
        // A chart standing in while another is prepared has no copy as drawn: made
        // here, for as long as it stands in.
        Some(PlotData::Lines(lines)) if lines.drawn_as(shown) != lines.wanted(shown) => {
            let mut lines = lines.clone();
            lines.keep_shown(shown);
            Cow::Owned(PlotData::Lines(lines))
        }
        Some(data @ (PlotData::Lines(_) | PlotData::XRange(_))) => Cow::Borrowed(data),
        _ => Cow::Owned(PlotData::Lines(LinesData::default())),
    };
    Plot {
        data,
        x: Axis {
            title: x_name.map(|x| modal.axis_title(x)).unwrap_or_default(),
            numbers: x_name
                .map(|x| AxisNumbers::column(numbers, schema, x))
                .unwrap_or_default(),
            kind,
            ..Default::default()
        },
        y: Axis {
            title: y_title,
            numbers: y_numbers,
            kind: y_kind,
            log: shown.log,
            clip: shown.clip,
        },
        scatter: spec.mark == Mark::Scatter,
        y_from_zero: modal.y_starts_at_zero,
    }
}

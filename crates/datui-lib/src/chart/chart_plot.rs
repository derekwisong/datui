//! What a chart plots. A worker prepares the data ([`PlotData`]), which the chart
//! cache keeps; [`plot`] gives it its axes for the view, titled and numbered as the
//! table prints the columns, and the screen and every export draw that [`Plot`].

use std::borrow::Cow;

use polars::prelude::{DataType, Schema};

use crate::chart::chart_data::{
    self, BarData, BoxPlotData, ChartXRangeResult, HeatmapData, HistogramData, KdeData, RowsRead,
    XAxisTemporalKind,
};
use crate::chart::chart_modal::{
    Aggregate, ChartModal, ChartSpec, Mark, RawLine, SMOOTH_POINTS, SMOOTH_SECONDS, Smooth,
};
use crate::chart::chart_trend::{self, Trend, TrendFits, TrendKey, TrendOver};
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

/// Each series' points, borrowed.
pub type PointsRef<'a> = &'a [Vec<(f64, f64)>];

/// A line or scatter chart's series.
#[derive(Debug, Clone, Default)]
pub struct LinesData {
    /// One per series: its Y column, or its color group.
    pub names: Vec<String>,
    /// Shared, so a trend's job reads them where they are.
    pub series: std::sync::Arc<Points>,
    /// The series as smoothing, a clipped range or a log scale draws them. Only the
    /// chart on screen keeps one (`ChartCache::touch`): it is as large as the series.
    /// Boxed: most charts on hand are not the one on screen.
    pub series_shown: Option<Box<ShownSeries>>,
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
    /// Per series and point, the sum and count of the rows it stands for, when a
    /// point is not one row (`ChartDataResult::weights`): what smoothing averages.
    pub weights: Option<Vec<Vec<(f64, f64)>>>,
    /// Rows an envelope stepped on a log X left out (X of 0 or less), which its points
    /// no longer hold.
    pub left_out_rows: usize,
    /// Values of 0 or less an envelope read, over every Y column, among the rows it
    /// placed: what a log Y leaves out of it.
    pub y_left_out_rows: usize,
    /// An envelope's steps as a log Y draws them, with where each series' line breaks:
    /// each step's least value above 0 and its greatest, a step with none left out.
    /// `None` when no value is 0 or less, or for points that are not an envelope's.
    pub log_y_steps: Option<Box<chart_data::LogYSteps>>,
    /// The smoothing windows these series offer (see [`smooth_ladder`]); empty when
    /// none does anything.
    pub ladder: Vec<Smooth>,
    /// `ladder` is empty because every window is within one envelope step's rows.
    pub steps_too_big: bool,
    /// Per series, the rows read without an X or a Y.
    pub nulls: Vec<usize>,
    /// The trend fitted through the series by a job, for the key it names.
    pub trend_fits: Option<Box<TrendFits>>,
}

/// How a line or scatter chart draws its values: smoothed, clipped to the range
/// of the values that are not outliers, on a log scale, in that order; with the raw
/// line behind a smoothed one, and a scatter's trend over its points. On a log
/// scale the points at 0 or less on it are left out once smoothed, before the clip.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Shown {
    /// Y as `log10(y)`.
    pub log: bool,
    /// X as `log10(x)`.
    pub log_x: bool,
    pub clip: bool,
    pub smooth: Smooth,
    pub raw: RawLine,
    pub trend: Trend,
    pub over: TrendOver,
}

impl Shown {
    /// The fits a trend draws, when one is on.
    pub fn trend_key(&self) -> Option<TrendKey> {
        (!self.trend.is_off()).then_some(TrendKey {
            trend: self.trend,
            over: self.over,
        })
    }
}

/// The series drawn as they are.
const PLAIN: Shown = Shown {
    log: false,
    log_x: false,
    clip: false,
    smooth: Smooth::Off,
    raw: RawLine::Off,
    trend: Trend::Off,
    over: TrendOver::Color,
};

/// The series as a [`Shown`] draws them, kept with the chart on screen. A copy is
/// made only where it differs: Every row can be millions of points.
#[derive(Debug, Clone)]
pub struct ShownSeries {
    /// What they are drawn as, settled ([`LinesData::wanted`]).
    pub shown: Shown,
    /// The smoothed values, before any clip or log: what the crosshair reads.
    pub smoothed: Option<Points>,
    /// The line as drawn, when a clip or the log changes it; otherwise it is drawn
    /// as `smoothed`, or as the series.
    pub series: Option<Points>,
    /// The raw line behind a smoothed one, when a clip or the log changes it;
    /// otherwise it is drawn as the series.
    pub raw: Option<Points>,
    /// Where the drawn values were clipped, and how many were.
    pub clip: Option<YClip>,
    /// The least and greatest X and Y drawn, the raw line's included.
    pub bounds: Option<[f64; 4]>,
    /// Per series, where its line starts again, when a log scale left points out.
    pub breaks: Option<Vec<Vec<usize>>>,
    /// The same for the raw line behind a smoothed one: a log Y leaves out other
    /// points of it.
    pub raw_breaks: Option<Vec<Vec<usize>>>,
    /// The points of the series a log scale left out.
    pub left_out: LeftOut,
    /// The points of the smoothed line a log scale left out, when it is smoothed.
    pub smoothed_left_out: Option<LeftOut>,
    /// Where the crosshair stands when a log scale left points out: the Xs of the
    /// points drawn, in order, each once.
    pub stops: Option<Vec<f64>>,
    /// The trend's lines as drawn, cut at the points' Y range.
    pub trend: Vec<TrendCurve>,
    /// Whether the trend's fits were in hand when this was made: a job's land later.
    pub fitted: bool,
}

/// One trend line as drawn: of a series (its index) or of every point, in the units
/// drawn, starting again where it left the Y range.
#[derive(Debug, Clone, PartialEq)]
pub struct TrendCurve {
    pub series: Option<usize>,
    pub points: Vec<(f64, f64)>,
    pub breaks: Vec<usize>,
}

/// Points a formula's trend line is drawn through, across its X range.
const TREND_STEPS: usize = 160;

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
        Self::on(series, false)
    }

    /// As [`Self::of`], with the outliers found on the scale the Y axis draws: on a
    /// log scale among the `log10` of the values above 0, so a decade is a decade.
    fn on(series: &[Vec<(f64, f64)>], log: bool) -> Option<Self> {
        let ys: Vec<f64> = series
            .iter()
            .flatten()
            .map(|&(_, y)| y)
            .filter(|&y| y.is_finite() && (!log || y > 0.0))
            .collect();
        let scaled = |y: f64| if log { y.log10() } else { y };
        let keys: Vec<f64> = ys.iter().map(|&y| scaled(y)).collect();
        let (low, high) = crate::analysis::statistics::outlier_bounds(&keys)?;
        let inside = |y: &&f64| (low..=high).contains(&scaled(**y));
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
    /// their X values in order, their bounds and the smoothing windows they offer.
    pub fn new(grouped: chart_data::GroupedSeries, rows_note: Option<String>) -> Self {
        let xs = crate::widgets::crosshair::xs(&grouped.series);
        let longest = grouped.series.iter().map(Vec::len).max().unwrap_or(0);
        Self {
            series_shown: None,
            trend_fits: None,
            nulls: grouped.nulls,
            y_clip: YClip::of(&grouped.series),
            ladder: smooth_ladder(&xs, longest, 1.0, grouped.x_axis_kind),
            steps_too_big: false,
            weights: None,
            left_out_rows: 0,
            y_left_out_rows: 0,
            log_y_steps: None,
            xs,
            bounds: extent(&grouped.series),
            names: grouped.names,
            series: std::sync::Arc::new(grouped.series),
            breaks: grouped.breaks,
            x_axis_kind: grouped.x_axis_kind,
            rows: grouped.rows,
            rows_note,
            other: grouped.other,
        }
    }

    /// With the rows each point stands for, when a point is not one row: a window
    /// of points is then that many rows.
    pub fn weighed(mut self, weights: Option<Vec<Vec<(f64, f64)>>>) -> Self {
        if let Some(weights) = &weights {
            let rows = weights
                .iter()
                .map(|w| w.iter().map(|&(_, n)| n).sum::<f64>() as usize)
                .max()
                .unwrap_or(0);
            let step = weights
                .iter()
                .flatten()
                .map(|&(_, n)| n)
                .fold(1.0, f64::max);
            self.ladder = smooth_ladder(&self.xs, rows, step, self.x_axis_kind);
            self.steps_too_big = self.ladder.is_empty()
                && !smooth_ladder(&self.xs, rows, 1.0, self.x_axis_kind).is_empty();
        }
        self.weights = weights;
        self
    }

    /// With what an envelope leaves out on a log scale: rows at X of 0 or less, and
    /// values at Y of 0 or less.
    pub fn leaving_out(mut self, x_rows: usize, y_values: usize) -> Self {
        self.left_out_rows = x_rows;
        self.y_left_out_rows = y_values;
        self
    }

    /// With an envelope's steps as a log Y draws them (see [`Self::log_y_steps`]).
    pub fn with_log_y_steps(mut self, steps: Option<chart_data::LogYSteps>) -> Self {
        self.log_y_steps = steps.map(Box::new);
        self
    }

    /// The trailing window `smooth` is over these series' X: none when the X does
    /// not take it (a length of time over a number, a count over a time).
    fn window(&self, smooth: Smooth) -> Option<Window> {
        let time = per_second(self.x_axis_kind);
        match (smooth, time) {
            (Smooth::Seconds(s), Some(per)) => Some(Window::Length(f64::from(s) * per)),
            (Smooth::Points(n), None) => Some(Window::Points(n as usize)),
            _ => None,
        }
    }

    /// What `shown` draws of these series: smoothing the X takes, a clip when some
    /// value is an outlier, and the raw line behind a smoothed one, by itself (auto)
    /// only when there is one series.
    pub fn wanted(&self, shown: Shown) -> Shown {
        let smooth = match self.window(shown.smooth) {
            Some(_) => shown.smooth,
            None => Smooth::Off,
        };
        let raw = match (smooth, shown.raw) {
            (Smooth::Off, _) => RawLine::Off,
            (_, RawLine::Auto) if self.series.iter().filter(|s| !s.is_empty()).count() == 1 => {
                RawLine::On
            }
            (_, RawLine::Auto) => RawLine::Off,
            (_, raw) => raw,
        };
        let log_x = shown.log_x && self.x_axis_kind == XAxisTemporalKind::Numeric;
        // Power and y = x read X as a number.
        let time = self.x_axis_kind != XAxisTemporalKind::Numeric;
        let trend = if time && !shown.trend.takes_time() {
            Trend::Off
        } else {
            shown.trend
        };
        Shown {
            log: shown.log,
            log_x,
            // The outliers of a smoothed line are found once it is smoothed, and on a
            // log scale among the points drawn.
            clip: shown.clip && (!smooth.is_off() || log_x || shown.log || self.y_clip.is_some()),
            smooth,
            raw,
            trend,
            over: if trend.takes_over() {
                shown.over
            } else {
                TrendOver::Color
            },
        }
    }

    /// The copy kept as `shown` draws the series, when one is: made with the fits in
    /// hand now.
    fn kept(&self, shown: Shown) -> Option<&ShownSeries> {
        let wanted = self.wanted(shown);
        let fitted = self.fits_for(wanted).is_some();
        self.series_shown
            .as_deref()
            .filter(|kept| kept.shown == wanted && kept.fitted == fitted)
    }

    /// The fits `wanted` (settled) draws, once fitted.
    fn fits_for(&self, wanted: Shown) -> Option<&TrendFits> {
        let key = wanted.trend_key()?;
        self.trend_fits.as_deref().filter(|fits| fits.key == key)
    }

    /// The trend `shown` draws, once fitted.
    pub fn fits(&self, shown: Shown) -> Option<&TrendFits> {
        self.fits_for(self.wanted(shown))
    }

    /// The fits `shown` asks for that a job has still to make.
    pub fn trend_wanted(&self, shown: Shown) -> Option<TrendKey> {
        let wanted = self.wanted(shown);
        let key = wanted.trend_key()?;
        (self.fits_for(wanted).is_none()).then_some(key)
    }

    /// Take fits a job made; the copy as drawn is made again with them.
    pub fn take_fits(&mut self, fits: TrendFits) {
        self.trend_fits = Some(Box::new(fits));
        self.series_shown = None;
    }

    /// The rows without an X or a Y in the series `fits` were fitted on.
    pub fn nulls_of(&self, fits: &TrendFits) -> usize {
        if fits.key.over == TrendOver::Overall || fits.key.trend == Trend::Identity {
            return self.nulls.iter().sum();
        }
        fits.fits
            .iter()
            .filter_map(|f| f.series)
            .filter_map(|s| self.nulls.get(s))
            .sum()
    }

    /// The trend lines as drawn.
    pub fn trend_curves(&self, shown: Shown) -> &[TrendCurve] {
        self.kept(shown).map_or(&[], |kept| kept.trend.as_slice())
    }

    /// What the series are drawn as now: as `shown` asks once a copy of them is kept,
    /// and as they are before that.
    fn drawn_as(&self, shown: Shown) -> Shown {
        self.kept(shown).map_or(PLAIN, |kept| kept.shown)
    }

    /// Keep a copy of the series as `shown` draws them when that differs from the
    /// series, and none otherwise. A trend is fitted by a job ([`Self::trend_wanted`]).
    pub fn keep_shown(&mut self, shown: Shown) {
        let wanted = self.wanted(shown);
        if wanted == PLAIN {
            self.series_shown = None;
        } else if self.drawn_as(shown) != wanted {
            self.series_shown = Some(Box::new(self.made(wanted)));
        }
    }

    /// The series as `wanted` draws them: smoothed, then on a log scale without the
    /// points at 0 or less on it, then clipped (see [`clip_of`]), then logged.
    /// A smoothing window reads every point, the ones a log scale leaves out included;
    /// a smoothed line keeps its own points above 0 on a log Y, the raw line its own.
    fn made(&self, wanted: Shown) -> ShownSeries {
        let smoothed: Option<Points> = self.window(wanted.smooth).map(|window| {
            self.series
                .iter()
                .enumerate()
                .map(|(i, points)| {
                    let weights = self.weights.as_ref().and_then(|w| w.get(i));
                    trailing_mean(points, weights.map(Vec::as_slice), window)
                })
                .collect()
        });
        let raw_drawn = wanted.raw == RawLine::On;
        let logged = wanted.log_x || wanted.log;
        // An envelope's steps on a log Y stand on their least value above 0.
        let (base, base_breaks): (&Points, &[Vec<usize>]) = match &self.log_y_steps {
            Some(steps) if wanted.log => (&steps.0, &steps.1),
            _ => (&self.series, &self.breaks),
        };
        let drawable = |series: &Points, breaks: &[Vec<usize>]| {
            Drawable::of(series, breaks, wanted.log_x, wanted.log)
        };
        // On a log scale, the points drawn: the series' and the smoothed line's.
        let kept = logged.then(|| {
            let positive = drawable(base, base_breaks);
            let series = positive.of_series(base);
            let smoothed = smoothed.as_ref().map(|s| {
                let positive = drawable(s, &self.breaks);
                (positive.of_series(s), positive)
            });
            (YClip::on(&series, wanted.log), series, positive, smoothed)
        });
        let (series_clip, series, smoothed_drawn) = match &kept {
            Some((clip, series, _, smoothed)) => (*clip, series, smoothed.as_ref().map(|(s, _)| s)),
            None => (self.y_clip, &*self.series, smoothed.as_ref()),
        };
        let clip = if wanted.clip {
            clip_of(smoothed_drawn, series, series_clip, raw_drawn, wanted.log)
        } else {
            None
        };
        let changed = logged || clip.is_some();
        let shown_x = |x: f64| if wanted.log_x { x.log10() } else { x };
        let draw = |series: &Points| -> Option<Points> {
            changed.then(|| {
                series
                    .iter()
                    .map(|points| {
                        points
                            .iter()
                            .map(|&(x, y)| (shown_x(x), shown_y(y, wanted.log, clip)))
                            .collect()
                    })
                    .collect()
            })
        };
        let line = smoothed_drawn.unwrap_or(series);
        let drawn_line = draw(line);
        let raw = if raw_drawn { draw(series) } else { None };
        let drawn = extent(drawn_line.as_ref().unwrap_or(line));
        let behind = raw_drawn.then(|| extent(raw.as_ref().unwrap_or(series)));
        let bounds = match (drawn, behind.flatten()) {
            (Some([x0, x1, y0, y1]), Some([a0, a1, b0, b1])) => {
                Some([x0.min(a0), x1.max(a1), y0.min(b0), y1.max(b1)])
            }
            (drawn, raw) => drawn.or(raw),
        };
        // The crosshair stands on what is drawn: the line, and the raw line behind it.
        let stops = kept.as_ref().map(|_| {
            let mut xs = crate::widgets::crosshair::xs(line);
            if raw_drawn {
                xs.extend(crate::widgets::crosshair::xs(series));
                xs.sort_by(f64::total_cmp);
                xs.dedup();
            }
            xs
        });
        let (left_out, smoothed_left_out, breaks, raw_breaks) = match kept {
            Some((_, _, positive, Some((_, smoothed)))) => (
                positive.left_out,
                Some(LeftOut {
                    counted: Counted::Smoothed,
                    ..smoothed.left_out
                }),
                Some(smoothed.breaks),
                Some(positive.breaks),
            ),
            Some((_, _, positive, None)) => (positive.left_out, None, Some(positive.breaks), None),
            None => (LeftOut::default(), None, None, None),
        };
        let fits = self.fits_for(wanted);
        let trend = match (fits, bounds) {
            (Some(fits), Some([x0, _, y0, y1])) => fits
                .fits
                .iter()
                .map(|fit| {
                    // On a log X, from the least X drawn: the line has no value at 0.
                    let from = wanted.log_x.then(|| 10f64.powf(x0));
                    let line: Vec<(f64, f64)> = trend_line(fit, from)
                        .into_iter()
                        .map(|(x, y)| (shown_x(x), shown_y(y, wanted.log, None)))
                        .collect();
                    let (points, breaks) = cut_to(&line, y0, y1);
                    TrendCurve {
                        series: fit.series,
                        points,
                        breaks,
                    }
                })
                .collect(),
            _ => Vec::new(),
        };
        ShownSeries {
            shown: wanted,
            smoothed,
            series: drawn_line,
            raw,
            clip,
            bounds,
            left_out,
            smoothed_left_out,
            breaks,
            raw_breaks,
            stops,
            trend,
            fitted: fits.is_some(),
        }
    }

    /// The series as drawn: smoothed, clipped to the range, logged on a log scale.
    pub fn shown(&self, shown: Shown) -> &[Vec<(f64, f64)>] {
        match self.kept(shown) {
            Some(kept) => kept
                .series
                .as_ref()
                .or(kept.smoothed.as_ref())
                .unwrap_or(&self.series),
            None => &self.series,
        }
    }

    /// The values the crosshair reads: the smoothed ones of a smoothed line (with
    /// the raw ones beside them), the series' own otherwise; never clipped or logged.
    pub fn values(&self, shown: Shown) -> (PointsRef<'_>, Option<PointsRef<'_>>) {
        match self.kept(shown).and_then(|kept| kept.smoothed.as_ref()) {
            Some(smoothed) => (smoothed, Some(&self.series)),
            None => (&self.series, None),
        }
    }

    /// The outliers drawn at the edge, when the range clips some.
    pub fn clip_drawn(&self, shown: Shown) -> Option<YClip> {
        let wanted = self.wanted(shown);
        if !wanted.clip {
            return None;
        }
        match self.kept(shown) {
            Some(kept) => kept.clip,
            None if wanted.smooth.is_off() && !wanted.log_x && !wanted.log => self.y_clip,
            None => None,
        }
    }

    /// The least and greatest X and Y of the points as drawn.
    pub fn shown_bounds(&self, shown: Shown) -> Option<[f64; 4]> {
        match self.kept(shown) {
            Some(kept) => kept.bounds,
            None => self.bounds.or_else(|| extent(&self.series)),
        }
    }

    /// The series with points, as drawn.
    pub fn drawn(&self, shown: Shown) -> impl Iterator<Item = Drawn<'_>> {
        self.drawn_of(self.shown(shown), self.breaks_drawn(shown))
    }

    /// Per series, where its line starts again as drawn: fewer points on a log scale.
    fn breaks_drawn(&self, shown: Shown) -> &[Vec<usize>] {
        self.kept(shown)
            .and_then(|kept| kept.breaks.as_deref())
            .unwrap_or(&self.breaks)
    }

    /// What a log scale leaves out of each line drawn: the series' points (or an
    /// envelope's rows) at 0 or less on it, or a smoothed line's own, or both when
    /// the raw line is drawn behind the smoothed one.
    pub fn left_out(&self, shown: Shown) -> Vec<LeftOut> {
        let wanted = self.wanted(shown);
        if !wanted.log_x && !wanted.log {
            return Vec::new();
        }
        let kept = self.kept(shown);
        let data = if self.weights.is_some() {
            // An envelope's points stand for rows: it counted them as it read.
            LeftOut {
                x: if wanted.log_x { self.left_out_rows } else { 0 },
                y: if wanted.log { self.y_left_out_rows } else { 0 },
                counted: Counted::Rows {
                    columns: self.series.len(),
                },
            }
        } else {
            kept.map_or_else(LeftOut::default, |kept| kept.left_out)
        };
        match kept.and_then(|kept| kept.smoothed_left_out) {
            Some(smoothed) if wanted.raw == RawLine::On => vec![data, smoothed],
            Some(smoothed) => vec![smoothed],
            None => vec![data],
        }
    }

    /// The series a line is drawn for, the smoothed line or the raw one behind it, by
    /// their place among every series: what a legend names.
    pub fn named(&self, shown: Shown) -> Vec<Drawn<'_>> {
        let mut named: Vec<Drawn<'_>> = self.drawn(shown).collect();
        for raw in self.raw_drawn(shown) {
            if !named.iter().any(|d| d.index == raw.index) {
                named.push(raw);
            }
        }
        named.sort_by_key(|d| d.index);
        named
    }

    /// The series with points that a log scale left none of.
    pub fn emptied(&self, shown: Shown) -> Vec<&str> {
        let wanted = self.wanted(shown);
        if !wanted.log_x && !wanted.log {
            return Vec::new();
        }
        let drawn: Vec<usize> = self.named(shown).iter().map(|s| s.index).collect();
        self.series
            .iter()
            .zip(&self.names)
            .enumerate()
            .filter(|(i, (points, _))| !points.is_empty() && !drawn.contains(i))
            .map(|(_, (_, name))| name.as_str())
            .collect()
    }

    /// The Xs the crosshair stands on: on a log scale, those of the points drawn.
    pub fn stops(&self, shown: Shown) -> &[f64] {
        if let Some(stops) = self.kept(shown).and_then(|kept| kept.stops.as_deref()) {
            return stops;
        }
        if self.wanted(shown).log_x {
            &self.xs[self.xs.partition_point(|&x| x <= 0.0)..]
        } else {
            &self.xs
        }
    }

    /// The raw lines drawn behind smoothed ones, when they are.
    pub fn raw_drawn(&self, shown: Shown) -> impl Iterator<Item = Drawn<'_>> {
        let raw: &[Vec<(f64, f64)>] = match self.kept(shown) {
            Some(kept) if kept.shown.raw == RawLine::On => {
                kept.raw.as_ref().unwrap_or(&self.series)
            }
            _ => &[],
        };
        let breaks = self
            .kept(shown)
            .and_then(|kept| kept.raw_breaks.as_deref())
            .unwrap_or(&self.breaks);
        self.drawn_of(raw, breaks)
    }

    fn drawn_of<'a>(
        &'a self,
        series: &'a [Vec<(f64, f64)>],
        breaks: &'a [Vec<usize>],
    ) -> impl Iterator<Item = Drawn<'a>> {
        let last = self.names.len().saturating_sub(1);
        series
            .iter()
            .zip(self.names.iter())
            .enumerate()
            .filter(|(_, (points, _))| !points.is_empty())
            .map(move |(index, (points, name))| Drawn {
                index,
                name,
                points,
                breaks: breaks.get(index).map_or(&[][..], Vec::as_slice),
                other: self.other && index == last,
            })
    }
}

/// A fit's line in data units across its X range (from `from` when it is later):
/// LOWESS's own points, or a formula's at even steps, in log X on a log X.
fn trend_line(fit: &chart_trend::Fit, from: Option<f64>) -> Vec<(f64, f64)> {
    let [lo, hi] = fit.x_range;
    let lo = from.map_or(lo, |from| lo.max(from));
    if let chart_trend::Model::Curve(points) = &fit.model {
        return points
            .iter()
            .copied()
            .filter(|&(x, _)| x >= lo && x <= hi)
            .collect();
    }
    if hi < lo {
        return Vec::new();
    }
    let log = from.is_some() && lo > 0.0;
    let (a, b) = if log {
        (lo.log10(), hi.log10())
    } else {
        (lo, hi)
    };
    (0..=TREND_STEPS)
        .map(|i| {
            let t = a + (b - a) * i as f64 / TREND_STEPS as f64;
            let x = if log { 10f64.powf(t) } else { t };
            (x, fit.model.at(x))
        })
        .collect()
}

/// `line` within Y of `lo..=hi`: cut where it leaves the range, at the edge, and
/// started again where it comes back, so a curve never draws past the points'
/// range or squeezes them. Returns the points and where each run after the first
/// starts.
fn cut_to(line: &[(f64, f64)], lo: f64, hi: f64) -> (Points1, Vec<usize>) {
    let mut points = Vec::with_capacity(line.len());
    let mut breaks = Vec::new();
    let inside = |y: f64| (lo..=hi).contains(&y);
    let edge = |y: f64| if y < lo { lo } else { hi };
    let cross = |(x0, y0): (f64, f64), (x1, y1): (f64, f64), at: f64| {
        (x0 + (x1 - x0) * (at - y0) / (y1 - y0), at)
    };
    // Whether the last point pushed continues to the next.
    let mut open = false;
    let start = |points: &mut Points1, breaks: &mut Vec<usize>, p: (f64, f64)| {
        if !points.is_empty() {
            breaks.push(points.len());
        }
        points.push(p);
    };
    let mut prev: Option<(f64, f64)> = None;
    for &(x, y) in line {
        if !y.is_finite() || !x.is_finite() {
            prev = None;
            open = false;
            continue;
        }
        let here = (x, y);
        match prev {
            _ if inside(y) && open => points.push(here),
            Some(p) if inside(y) => {
                start(&mut points, &mut breaks, cross(p, here, edge(p.1)));
                points.push(here);
                open = true;
            }
            None if inside(y) => {
                start(&mut points, &mut breaks, here);
                open = true;
            }
            Some(p) if open => {
                points.push(cross(p, here, edge(y)));
                open = false;
            }
            // From below the range to above it, or back: through it.
            Some(p) if (p.1 < lo && y > hi) || (p.1 > hi && y < lo) => {
                start(&mut points, &mut breaks, cross(p, here, edge(p.1)));
                points.push(cross(p, here, edge(y)));
            }
            _ => {}
        }
        prev = Some(here);
    }
    (points, breaks)
}

/// One series' points.
type Points1 = Vec<(f64, f64)>;

/// Where the range clips: `series`' outliers (`series_clip`); a smoothed line's own
/// outliers; with the raw line behind it, the span of both lines without their
/// outliers, each line's outliers at its edge, every point there counted.
fn clip_of(
    smoothed: Option<&Points>,
    series: &Points,
    series_clip: Option<YClip>,
    raw_drawn: bool,
    log: bool,
) -> Option<YClip> {
    let Some(smoothed) = smoothed else {
        return series_clip;
    };
    if !raw_drawn {
        return YClip::on(smoothed, log);
    }
    let range = |clip: Option<YClip>, series: &Points| {
        clip.map(|c| (c.lo, c.hi))
            .or_else(|| extent(series).map(|[_, _, lo, hi]| (lo, hi)))
    };
    let (Some((a, b)), Some((c, d))) = (
        range(YClip::on(smoothed, log), smoothed),
        range(series_clip, series),
    ) else {
        return None;
    };
    let (lo, hi) = (a.min(c), b.max(d));
    let outside = [smoothed, series]
        .into_iter()
        .flatten()
        .flatten()
        .filter(|&&(_, y)| y.is_finite() && !(lo..=hi).contains(&y))
        .count();
    (outside > 0 && hi > lo).then_some(YClip { lo, hi, outside })
}

/// What a log scale left out of a line: the points at 0 or less on a log axis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeftOut {
    /// At X of 0 or less, on a log X.
    pub x: usize,
    /// At Y of 0 or less, on a log Y, among those a log X kept.
    pub y: usize,
    pub counted: Counted,
}

/// What a [`LeftOut`] counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Counted {
    /// The series' points.
    #[default]
    Points,
    /// A smoothed line's points: means, not values.
    Smoothed,
    /// An envelope's rows: of X, rows; of Y, values over `columns` Y columns.
    Rows { columns: usize },
}

impl LeftOut {
    /// What the title row says, `at_most` the glyph set's `≤`: `3 points with X ≤ 0
    /// left out`; one line for both axes when they count alike, `5 points with X or
    /// Y ≤ 0 left out`.
    pub fn notes(&self, at_most: &str) -> Vec<String> {
        let (x_noun, y_noun) = match self.counted {
            Counted::Points => ("point", "point"),
            Counted::Smoothed => ("smoothed point", "smoothed point"),
            Counted::Rows { columns } if columns > 1 => ("row", "value"),
            Counted::Rows { .. } => ("row", "row"),
        };
        let note = |n: usize, noun: &str, axis: &str| {
            let s = if n == 1 { "" } else { "s" };
            format!(
                "{} {noun}{s} with {axis} {at_most} 0 left out",
                crate::numfmt::group_chrome(n)
            )
        };
        match (self.x, self.y) {
            (0, 0) => Vec::new(),
            (x, 0) => vec![note(x, x_noun, "X")],
            (0, y) => vec![note(y, y_noun, "Y")],
            (x, y) if x_noun == y_noun => vec![note(x + y, x_noun, "X or Y")],
            (x, y) => vec![note(x, x_noun, "X"), note(y, y_noun, "Y")],
        }
    }
}

/// Which points a log scale draws: per series, those above zero on each log axis,
/// and where its line starts again without the rest. A run left out breaks the line,
/// as a gap does.
#[derive(Debug, Clone, PartialEq)]
pub struct Drawable {
    /// Per series, the indexes of the points kept.
    pub kept: Vec<Vec<usize>>,
    /// Per series, where its line starts again, as indexes into the points kept.
    pub breaks: Vec<Vec<usize>>,
    /// Points left out over every series: by X first, then by Y.
    pub left_out: LeftOut,
}

impl Drawable {
    pub fn of(series: &[Vec<(f64, f64)>], breaks: &[Vec<usize>], log_x: bool, log_y: bool) -> Self {
        let mut out = Self {
            kept: Vec::with_capacity(series.len()),
            breaks: Vec::with_capacity(series.len()),
            left_out: LeftOut::default(),
        };
        let positive = |v: f64| v > 0.0 && v.is_finite();
        for (s, points) in series.iter().enumerate() {
            let starts = breaks.get(s).map_or(&[][..], Vec::as_slice);
            let (mut kept, mut again) = (Vec::new(), Vec::new());
            // A gap, or a point left out, since the last point kept.
            let mut gap = false;
            for (i, &(x, y)) in points.iter().enumerate() {
                gap |= starts.binary_search(&i).is_ok();
                if log_x && !positive(x) {
                    out.left_out.x += 1;
                    gap = true;
                } else if log_y && !positive(y) {
                    // An empty window's mean is no point, not one left out.
                    if !y.is_nan() {
                        out.left_out.y += 1;
                    }
                    gap = true;
                } else {
                    if gap && !kept.is_empty() {
                        again.push(kept.len());
                    }
                    gap = false;
                    kept.push(i);
                }
            }
            out.kept.push(kept);
            out.breaks.push(again);
        }
        out
    }

    /// `series` (the series these were found in, or values at the same points) with
    /// only the points kept.
    fn of_series(&self, series: &[Vec<(f64, f64)>]) -> Points {
        series
            .iter()
            .zip(&self.kept)
            .map(|(points, kept)| kept.iter().map(|&i| points[i]).collect())
            .collect()
    }
}

/// A trailing window: a length of X, or a number of points.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Window {
    Length(f64),
    Points(usize),
}

/// X units per second on a time axis (see [`XAxisTemporalKind`]); `None` for a
/// number.
pub(crate) fn per_second(kind: XAxisTemporalKind) -> Option<f64> {
    match kind {
        XAxisTemporalKind::Numeric => None,
        XAxisTemporalKind::Date => Some(1.0 / 86_400.0),
        XAxisTemporalKind::DatetimeMs => Some(1e3),
        XAxisTemporalKind::DatetimeUs => Some(1e6),
        XAxisTemporalKind::DatetimeNs | XAxisTemporalKind::Time => Some(1e9),
        XAxisTemporalKind::Duration => Some(1.0),
    }
}

/// A sum that keeps what rounding drops (Neumaier), so a window that took in and let
/// go of one huge value leaves no error in the means after it.
#[derive(Debug, Default, Clone, Copy)]
struct Sum {
    sum: f64,
    lost: f64,
}

impl Sum {
    fn add(&mut self, v: f64) {
        let t = self.sum + v;
        self.lost += if self.sum.abs() >= v.abs() {
            (self.sum - t) + v
        } else {
            (v - t) + self.sum
        };
        self.sum = t;
    }

    fn get(self) -> f64 {
        self.sum + self.lost
    }
}

/// Each point with its Y the mean of the finite Ys in its trailing window, nothing
/// after it: those whose X is in `(x - length, x]`, or the last `n` points. At the
/// start the window holds what there is; with no finite Y in it the point is NaN.
///
/// With `weights`, a point stands for rows, by their sum and count: the mean is of
/// the rows, and `n` points are `n` rows, the oldest step's rows taken in part at
/// its mean. Points out of X order are smoothed in X order and come back where
/// they were.
pub fn trailing_mean(
    points: &[(f64, f64)],
    weights: Option<&[(f64, f64)]>,
    window: Window,
) -> Vec<(f64, f64)> {
    let weights = weights.filter(|w| w.len() == points.len());
    if !points.is_sorted_by(|a, b| a.0 <= b.0) {
        let mut order: Vec<usize> = (0..points.len()).collect();
        order.sort_by(|&a, &b| points[a].0.total_cmp(&points[b].0));
        let sorted: Vec<(f64, f64)> = order.iter().map(|&i| points[i]).collect();
        let sorted_weights: Option<Vec<(f64, f64)>> =
            weights.map(|w| order.iter().map(|&i| w[i]).collect());
        let mut out = vec![(0.0, 0.0); points.len()];
        for (&i, point) in
            order
                .iter()
                .zip(trailing_mean(&sorted, sorted_weights.as_deref(), window))
        {
            out[i] = point;
        }
        return out;
    }
    // Each point's rows: their sum, how many have a value, and how many it counts
    // as in a window of points.
    let unit = |i: usize| -> (f64, f64, f64) {
        match weights {
            Some(w) => (w[i].0, w[i].1, w[i].1),
            None if points[i].1.is_finite() => (points[i].1, 1.0, 1.0),
            None => (0.0, 0.0, 1.0),
        }
    };
    let n = points.len();
    let mut out = Vec::with_capacity(n);
    // The window is points[lo..hi]: its rows' sum, count and size.
    let (mut lo, mut hi) = (0, 0);
    let mut held = Held::default();
    for (i, &(x, _)) in points.iter().enumerate() {
        let mut part = None;
        match window {
            // Every point at this X is in, as none of them is after it.
            Window::Length(length) => {
                while hi < n && points[hi].0 <= x {
                    held.take(unit(hi), 1.0);
                    hi += 1;
                }
                while lo < hi && points[lo].0 <= x - length {
                    held.take(unit(lo), -1.0);
                    lo += 1;
                }
            }
            Window::Points(want) => {
                let want = want.max(1) as f64;
                while hi <= i {
                    held.take(unit(hi), 1.0);
                    hi += 1;
                }
                while lo < i && held.size - unit(lo).2 >= want {
                    held.take(unit(lo), -1.0);
                    lo += 1;
                }
                // The oldest step's rows past the window, left out at its mean.
                let (s, c, z) = unit(lo);
                if held.size > want && z > 0.0 {
                    let out_of = (held.size - want) / z;
                    part = Some((s * out_of, c * out_of));
                }
            }
        }
        // An empty window starts its sum again, so no rounding carries on.
        if held.count <= 0.0 {
            held.sum = Sum::default();
            held.count = 0.0;
        }
        let (s, c) = match part {
            Some((ps, pc)) => (held.sum.get() - ps, held.count - pc),
            None => (held.sum.get(), held.count),
        };
        out.push((x, if c > 0.0 { s / c } else { f64::NAN }));
    }
    out
}

/// A window's rows: their sum, how many have a value, and how many points they
/// count as.
#[derive(Debug, Default)]
struct Held {
    sum: Sum,
    count: f64,
    size: f64,
}

impl Held {
    /// Take a point's `(sum, count, size)` in (`sign` 1) or out (-1).
    fn take(&mut self, (s, c, z): (f64, f64, f64), sign: f64) {
        self.sum.add(sign * s);
        self.count += sign * c;
        self.size += sign * z;
    }
}

/// The windows smoothing offers over these Xs (in order, each once, of a series
/// at most `longest` points or rows long): over time, the lengths wider than the
/// median gap between Xs and at most half their span; over numbers, the counts of
/// more than one and at most half the longest series. When fewer than three are,
/// the next ones up are offered as long as they are wider than the gap and no
/// wider than the span (the longest series); none when nothing is. A count is
/// also more than `step`, the most rows one point stands for, since a window
/// within one point draws only that point's mean.
pub fn smooth_ladder(
    xs: &[f64],
    longest: usize,
    step: f64,
    kind: XAxisTemporalKind,
) -> Vec<Smooth> {
    let Some(per) = per_second(kind) else {
        let longest = longest as f64;
        let steps = ladder(&SMOOTH_POINTS, step.max(1.0), longest / 2.0, longest);
        return steps.into_iter().map(Smooth::Points).collect();
    };
    let mut gaps: Vec<f64> = xs.windows(2).map(|w| (w[1] - w[0]) / per).collect();
    let gap = if gaps.is_empty() {
        0.0
    } else {
        let mid = gaps.len() / 2;
        *gaps.select_nth_unstable_by(mid, f64::total_cmp).1
    };
    let span = match (xs.first(), xs.last()) {
        (Some(first), Some(last)) => (last - first) / per,
        _ => 0.0,
    };
    ladder(&SMOOTH_SECONDS, gap, span / 2.0, span)
        .into_iter()
        .map(Smooth::Seconds)
        .collect()
}

/// The steps of `all` above `above` and at most `upto`; when fewer than three are,
/// up to three from the first above `above`, none past `most`.
fn ladder(all: &[u32], above: f64, upto: f64, most: f64) -> Vec<u32> {
    const AT_LEAST: usize = 3;
    let over = |w: &u32| f64::from(*w) > above;
    let kept: Vec<u32> = all
        .iter()
        .copied()
        .filter(|w| over(w) && f64::from(*w) <= upto)
        .collect();
    if kept.len() >= AT_LEAST {
        return kept;
    }
    all.iter()
        .copied()
        .filter(over)
        .take(AT_LEAST)
        .filter(|&w| f64::from(w) <= most)
        .collect()
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

/// A Y as drawn: at the edge of `clip` past it, then on the log scale as `log10(y)`
/// (only points above 0 reach it there).
fn shown_y(y: f64, log: bool, clip: Option<YClip>) -> f64 {
    let y = clip.map_or(y, |clip| y.clamp(clip.lo, clip.hi));
    if log { y.log10() } else { y }
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
    /// A log scale: values are drawn as their `log10`; ticks name the value.
    pub log: bool,
    /// Outliers are drawn at the edge.
    pub clip: bool,
    /// Values are a trailing mean, with the raw line behind as this says.
    pub smooth: Smooth,
    pub raw: RawLine,
    /// A scatter's trend over the points, per series or overall.
    pub trend: Trend,
    pub over: TrendOver,
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
            log_x: self.x.log,
            clip: self.y.clip,
            smooth: self.y.smooth,
            raw: self.y.raw,
            trend: self.y.trend,
            over: self.y.over,
        }
    }

    /// A scatter's trend lines, drawn after the points; none otherwise.
    pub fn trend_curves(&self) -> Vec<(Option<usize>, Curve<'_>)> {
        let PlotData::Lines(lines) = &*self.data else {
            return Vec::new();
        };
        let slots: Vec<usize> = self.drawn().map(|d| d.index).collect();
        lines
            .trend_curves(self.shown())
            .iter()
            .map(|t| {
                let index = t.series.unwrap_or(0);
                // In a file a series is colored by its place in the legend.
                let slot = slots.iter().position(|&i| i == index).unwrap_or(0);
                let curve = Curve {
                    index,
                    slot,
                    points: Cow::Borrowed(&t.points),
                    breaks: &t.breaks,
                    other: false,
                };
                (t.series, curve)
            })
            .collect()
    }

    /// What a trend's reading says of this plot's axes: X's unit or name, Y's unit,
    /// and what a point is under an aggregate.
    pub fn trend_words(&self, modal: &ChartModal) -> chart_trend::Words<'_> {
        let unit = |column: &str| {
            modal
                .units
                .iter()
                .find(|(name, _)| name == column)
                .map(|(_, unit)| unit.clone())
        };
        let x = modal.x().cloned().unwrap_or_default();
        let aggregate = modal.aggregate();
        let y_unit = match modal.y() {
            [y] if !aggregate.is_count() => unit(y),
            _ => None,
        };
        let y_name = match modal.y() {
            [y] => y.clone(),
            _ => "y".to_string(),
        };
        chart_trend::Words {
            x: &self.x,
            y: &self.y,
            x_unit: unit(&x),
            x_name: x,
            y_name,
            y_unit,
            points: chart_trend::points_of(aggregate),
        }
    }

    /// The trend's plain reading, and the mark when Pearson and Spearman disagree.
    pub fn trend_reading(
        &self,
        modal: &ChartModal,
        g: &crate::glyphs::Glyphs,
    ) -> Option<(Vec<String>, Option<String>)> {
        let fits = self.trend_fits()?;
        let parts = chart_trend::reading(fits, &self.trend_words(modal), g)?;
        let names = match &*self.data {
            PlotData::Lines(lines) => lines.names.as_slice(),
            _ => &[],
        };
        Some((parts, chart_trend::rank_mark(fits, names, g)))
    }

    /// The trend fitted for the plot, once fitted.
    pub fn trend_fits(&self) -> Option<&TrendFits> {
        match &*self.data {
            PlotData::Lines(lines) => lines.fits(self.shown()),
            _ => None,
        }
    }

    /// A smoothed line chart's raw lines, drawn first so the smoothed lines are
    /// drawn over them; none otherwise.
    pub fn raw_curves(&self) -> Vec<Curve<'_>> {
        let PlotData::Lines(lines) = &*self.data else {
            return Vec::new();
        };
        let slot = self.slots();
        lines
            .raw_drawn(self.shown())
            .map(|s| Curve {
                index: s.index,
                slot: slot(s.index),
                points: Cow::Borrowed(s.points),
                breaks: s.breaks,
                other: s.other,
            })
            .collect()
    }

    /// A line or scatter chart's series a line is drawn for, smoothed or raw, in
    /// legend order (see [`LinesData::named`]).
    pub fn named(&self) -> Vec<Drawn<'_>> {
        match &*self.data {
            PlotData::Lines(lines) => lines.named(self.shown()),
            _ => Vec::new(),
        }
    }

    /// A series' place in the legend, from its place among every series.
    fn slots(&self) -> impl Fn(usize) -> usize + '_ {
        let named: Vec<usize> = self.named().iter().map(|d| d.index).collect();
        move |index| named.iter().position(|&i| i == index).unwrap_or(0)
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
        let slot = self.slots();
        let curves: Vec<Curve<'_>> = match &*self.data {
            PlotData::Lines(_) => self
                .drawn()
                .map(|s| Curve {
                    index: s.index,
                    slot: slot(s.index),
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
            PlotData::Lines(_) => self.named().iter().map(|s| s.name).collect(),
            PlotData::Bars(data) => data.groups.iter().map(String::as_str).collect(),
            PlotData::Histogram(data) => data.groups.iter().map(|g| g.name.as_str()).collect(),
            PlotData::Kde(data) => data.series.iter().map(|s| s.name.as_str()).collect(),
            PlotData::Box(_) | PlotData::Heatmap(_) | PlotData::XRange(_) => Vec::new(),
        }
    }

    /// Where Other is in the legend: last, when it has one.
    pub fn other_at(&self) -> Option<usize> {
        match &*self.data {
            PlotData::Lines(_) => self.named().iter().position(|s| s.other),
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
            log: shown.log_x,
            ..Default::default()
        },
        y: Axis {
            title: y_title,
            numbers: y_numbers,
            kind: y_kind,
            log: shown.log,
            clip: shown.clip,
            smooth: shown.smooth,
            raw: shown.raw,
            trend: shown.trend,
            over: shown.over,
        },
        scatter: spec.mark == Mark::Scatter,
        y_from_zero: modal.y_from_zero_shown(),
    }
}

#[cfg(test)]
mod tests;

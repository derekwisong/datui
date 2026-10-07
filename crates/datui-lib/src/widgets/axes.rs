//! Axis ticks, labels, titles and grid for every chart. ratatui would cut or run labels
//! together and draw titles over the plot, so datui picks ticks and draws labels and
//! marks itself. Ticks fall on nice values (1, 2 or 5 times a power of ten, or calendar
//! boundaries), about one label per 15 columns or 4 rows, never closer than two cells: a
//! crowded axis takes a coarser step, then shorter labels (`12.3k`, yearless dates),
//! keeping a fixed axis's ends while anything fits. A title gets its own row, cut to
//! fit, or is dropped.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    symbols::Marker,
    text::Span,
    widgets::{Axis, Chart, Widget},
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::chart::chart_data::{XAxisTemporalKind, x_axis_label_at};
use crate::glyphs::Glyphs;
use crate::widgets::axis_numbers::{AxisFormat, AxisNumbers};
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
    /// A log scale, where position `v` stands for the value `exp_m1(v)`, from zero
    /// up: ticks at each power of ten, with 2 and 5 between when there is room.
    Log { numbers: AxisNumbers },
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
    /// it starts and ends on a label, short of a tick that would leave most of a
    /// step empty ([`ticks::widen_snug`]).
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

    /// A log-scale y axis (position `v` is `exp_m1(v)`): ticks at nice values (1, 10, 100,
    /// with 2 and 5 between when there is room), widened to the ticks around its range, one
    /// format throughout.
    pub fn y_log(bounds: [f64; 2], numbers: &AxisNumbers, title: &'a str) -> Self {
        Self {
            bounds,
            scale: Scale::Log {
                numbers: numbers.clone(),
            },
            title,
            pad: 0,
        }
    }

    /// The same axis, its labels right-aligned in at least `width` cells.
    pub fn padded(self, width: usize) -> Self {
        Self { pad: width, ..self }
    }

    /// The ways to tick this axis of `length`, preferred first: about one label per
    /// `spacing`, then coarser, none closer than `least`, minor ticks at least `minor_gap`
    /// apart (cells on screen, points in a file). A second group follows when the first may
    /// be empty: a time axis's ends and middle.
    pub fn tick_sets(
        &self,
        length: f64,
        spacing: f64,
        least: f64,
        minor_gap: f64,
    ) -> Vec<Vec<TickSet>> {
        let [lo, hi] = self.bounds;
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
            Scale::Log { numbers } => vec![log_sets(
                self.bounds,
                numbers,
                length,
                spacing,
                least,
                minor_gap,
            )],
            Scale::Calendar { kind, numbers } => {
                let primary = calendar_sets(self.bounds, *kind, length, spacing, least, minor_gap);
                let format = AxisFormat::ends_and_middle(self.bounds, numbers);
                let kind = *kind;
                let ends = AxisSpec::ends_and_middle(
                    self.bounds,
                    Box::new(move |v, level| x_axis_label_at(v, kind, (lo, hi), level, &format)),
                    "",
                );
                let fallback = ends.tick_sets(length, spacing, least, minor_gap).remove(0);
                vec![primary, fallback]
            }
        }
    }
}

/// Whether `sets` hold more than one tick each where it counts.
fn with_ticks(sets: Vec<TickSet>) -> Vec<TickSet> {
    sets.into_iter().filter(|s| s.ticks.len() >= 2).collect()
}

/// One way to tick an axis, as [`preferred`] weighs it: the fewest cells between two
/// of its ticks, and how many ticks it has.
struct Candidate<T> {
    set: T,
    gap: f64,
    ticks: usize,
}

/// Of the options (finest first), the one nearest `spacing` and every coarser one at
/// least `least` apart. When the nearest has only two ticks and a finer one fits, the
/// finer comes first (`0 2 4 6`, not `0 5`), the two-tick set as fallback.
fn preferred<T>(options: Vec<Candidate<T>>, spacing: f64, least: f64) -> Vec<T> {
    let closeness = |gap: f64| (gap / spacing).ln().abs();
    let best = options
        .iter()
        .enumerate()
        .filter(|(_, o)| o.gap >= least)
        .min_by(|a, b| closeness(a.1.gap).total_cmp(&closeness(b.1.gap)))
        .map(|(i, _)| i);
    let best = best.map(|best| {
        if options[best].ticks > 2 {
            return best;
        }
        (0..best)
            .rev()
            .find(|&i| options[i].gap >= least)
            .unwrap_or(best)
    });
    match best {
        Some(best) => options.into_iter().skip(best).map(|o| o.set).collect(),
        // Nothing is far enough apart: the coarsest, which may still have room.
        None => options
            .into_iter()
            .last()
            .map(|o| o.set)
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
    let options: Vec<Candidate<(f64, [f64; 2])>> = ticks::nice_steps(lo, hi, finest, numbers.whole)
        .into_iter()
        .map(|step| {
            let range = if widen {
                ticks::widen_snug(lo, hi, step, numbers.whole)
            } else {
                bounds
            };
            Candidate {
                set: (step, range),
                gap: step / (range[1] - range[0]) * length,
                ticks: ticks::multiples(range[0], range[1], step).len(),
            }
        })
        .filter(|o| o.ticks >= 2)
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

/// Log-scale tick sets over `bounds` (positions are `exp_m1`) for an axis of `length`
/// cells: from one up, powers of ten (with 2 and 5 between, or every second or third
/// power), 0 below where the axis starts there; under one, plain nice steps. Each set
/// widens the axis to its ticks around the data.
fn log_sets(
    bounds: [f64; 2],
    numbers: &AxisNumbers,
    length: f64,
    spacing: f64,
    least: f64,
    minor_gap: f64,
) -> Vec<TickSet> {
    let [lo, hi] = bounds;
    let (low, high) = (lo.exp_m1().max(0.0), hi.exp_m1());
    if hi.partial_cmp(&lo) != Some(std::cmp::Ordering::Greater) || length <= 0.0 || high <= 0.0 {
        let format = AxisFormat::log(&[low], numbers);
        return vec![TickSet {
            ticks: vec![lo],
            minor: Vec::new(),
            levels: vec![vec![format.label(low, 0).unwrap_or_default()]],
            bounds,
        }];
    }
    // Each option: its values (before the log) and the minor ones between.
    let options: Vec<(Vec<f64>, Vec<f64>)> = if high <= 1.0 {
        // Under one a log scale is close to linear, never more than twice as steep.
        let finest = (high - low) * least.min(minor_gap) / length / 2.0;
        ticks::nice_steps(low, high, finest, false)
            .into_iter()
            .map(|step| {
                let [a, b] = ticks::widen(low, high, step);
                (ticks::multiples(a, b, step), Vec::new())
            })
            .collect()
    } else {
        let decade = |v: f64| (v.log10() + 1e-9).floor() as i32;
        // Every `every` powers of ten from the one at or under the data's least (one
        // at the least, or zero) to the one at or over its most.
        let values = |mantissas: &[f64], every: i32| {
            let first = if low >= 1.0 { decade(low) } else { 0 };
            let first = first - first.rem_euclid(every);
            let mut values: Vec<f64> = if low < 1.0 { vec![0.0] } else { Vec::new() };
            let mut k = first;
            'up: loop {
                for &m in mantissas {
                    // Parsed, so 2e21 is the double nearest it and not 2 times one.
                    let v: f64 = format!("{m}e{k}").parse().unwrap_or(f64::INFINITY);
                    values.push(v);
                    if v >= high * (1.0 - 1e-9) {
                        break 'up;
                    }
                }
                k += every;
                if k > 308 {
                    break;
                }
            }
            // A zero under them stands in for the one, which sits too close to it
            // past a step of a whole power.
            if every > 1 || mantissas.len() == 1 {
                values.retain(|&v| v != 1.0 || low >= 1.0);
            }
            // From the tick at or under the data's least.
            let start = values
                .iter()
                .rposition(|&v| v <= low * (1.0 + 1e-9))
                .unwrap_or(0);
            values.split_off(start)
        };
        let fine = values(&[1.0, 2.0, 5.0], 1);
        let mut options = vec![(fine.clone(), Vec::new())];
        for every in [1, 2, 3, 5, 10, 20, 50, 100] {
            let majors = values(&[1.0], every);
            let minor = if every == 1 {
                fine.iter()
                    .copied()
                    .filter(|v| !majors.contains(v))
                    .collect()
            } else {
                Vec::new()
            };
            options.push((majors, minor));
        }
        options
    };
    let candidates: Vec<Candidate<(Vec<f64>, Vec<f64>)>> = options
        .into_iter()
        .filter(|(values, _)| values.len() >= 2)
        .map(|(values, minor)| {
            let at: Vec<f64> = values.iter().map(|v| v.ln_1p()).collect();
            let per_cell = length / (at[at.len() - 1] - at[0]);
            let gap = at
                .windows(2)
                .map(|w| (w[1] - w[0]) * per_cell)
                .fold(f64::INFINITY, f64::min);
            Candidate {
                ticks: values.len(),
                set: (values, minor),
                gap,
            }
        })
        .collect();
    preferred(candidates, spacing, least)
        .into_iter()
        .map(|(values, minor)| {
            let format = AxisFormat::log(&values, numbers);
            let levels = (0..2)
                .map_while(|level| values.iter().map(|&v| format.label(v, level)).collect())
                .collect();
            let at: Vec<f64> = values.iter().map(|v| v.ln_1p()).collect();
            let bounds = [at[0], at[at.len() - 1]];
            let per_cell = length / (bounds[1] - bounds[0]);
            // Minor ticks inside the axis, and only where every one keeps its
            // distance from the next.
            let minor: Vec<f64> = minor
                .iter()
                .filter(|v| **v > values[0] && **v < values[values.len() - 1])
                .map(|v| v.ln_1p())
                .collect();
            let mut all: Vec<f64> = minor.iter().chain(&at).copied().collect();
            all.sort_by(f64::total_cmp);
            let roomy = all
                .windows(2)
                .all(|w| (w[1] - w[0]) * per_cell >= minor_gap);
            let minor = if roomy { minor } else { Vec::new() };
            TickSet {
                ticks: at,
                minor,
                levels,
                bounds,
            }
        })
        .collect()
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
        .map(|(i, (_, _, values, gap))| Candidate {
            set: i,
            gap: *gap,
            ticks: values.len(),
        })
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

/// A chart's legend: a name per series, each in the style its series draws in.
#[derive(Clone, Debug, Default)]
pub struct Legend {
    pub entries: Vec<(String, Style)>,
}

/// The widest a legend name is drawn before it is cut.
const LEGEND_NAME_MAX: usize = 24;

impl Legend {
    /// The cells it covers for names `name_width` wide: a cell of air each side, the
    /// swatch and a space, then the name; a row per series.
    fn size(&self, name_width: usize) -> (u16, u16) {
        (name_width as u16 + 4, self.entries.len() as u16)
    }
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
    /// The legend, placed where it covers the fewest marks; `None` draws none.
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
        self.render_in(self.frame(area), chart, buf, g)
    }

    /// [`Self::render`] in a frame already worked out by [`Self::frame`].
    pub fn render_in(
        &self,
        frame: PlotFrame,
        chart: Chart<'_>,
        buf: &mut Buffer,
        g: &Glyphs,
    ) -> PlotFrame {
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
            .legend_position(None);
        // The marks drawn once, on their own: the legend is placed by them, and they
        // go over the grid, which their blank cells leave alone.
        let mut marks = Buffer::empty(frame.chart);
        chart.render(frame.chart, &mut marks);
        let legend = self
            .legend
            .as_ref()
            .and_then(|legend| place_legend(&marks, &frame, legend, g));
        if let Some(style) = self.grid {
            draw_grid(buf, frame.graph, &x.majors, &frame.y.majors, style, g);
        }
        let blank = ratatui::buffer::Cell::default();
        for (i, cell) in marks.content().iter().enumerate() {
            if *cell != blank {
                let (x, y) = marks.pos_of(i);
                buf[(x, y)] = cell.clone();
            }
        }
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
        if let (Some(legend), Some(place)) = (&self.legend, legend) {
            draw_legend(buf, legend, place, self.labels, g);
        }
        frame
    }
}

/// Where a legend goes: its area, and how wide its names are drawn.
#[derive(Clone, Copy, Debug)]
struct LegendPlace {
    area: Rect,
    name_width: usize,
}

/// Where in `frame`'s plot the legend covers the fewest marks of `probe` (braille cells
/// by dots): a corner or edge middle, corners first and top right first on ties. `None`
/// when the plot cannot spare a quarter.
fn place_legend(
    probe: &Buffer,
    frame: &PlotFrame,
    legend: &Legend,
    g: &Glyphs,
) -> Option<LegendPlace> {
    let graph = frame.graph;
    let widest = legend
        .entries
        .iter()
        .map(|(name, _)| name.width())
        .max()
        .unwrap_or(0);
    let name_width = widest
        .min(LEGEND_NAME_MAX)
        .min((graph.width / 2).saturating_sub(4) as usize);
    let (w, h) = legend.size(name_width);
    if legend.entries.is_empty()
        || name_width == 0
        || name_width < widest.min(4)
        || w > graph.width / 2
        || h > graph.height / 2
    {
        return None;
    }
    let weight = |symbol: &str| -> usize {
        let mut chars = symbol.chars();
        match (chars.next(), chars.next()) {
            (None | Some(' '), _) => 0,
            (Some(c), None) if ('\u{2800}'..='\u{28ff}').contains(&c) => {
                (c as u32 - 0x2800).count_ones() as usize
            }
            _ => 1,
        }
    };
    // The axes ratatui drew are not marks.
    let axis = [g.plot.axis.vertical, g.plot.axis.horizontal];
    let marks = |x0: u16, y0: u16| -> usize {
        (y0..y0 + h)
            .flat_map(|y| (x0..x0 + w).map(move |x| (x, y)))
            .map(|(x, y)| probe[(x, y)].symbol())
            .filter(|symbol| !axis.contains(symbol))
            .map(weight)
            .sum()
    };
    let (left, right) = (graph.left(), graph.right() - w);
    let (top, bottom) = (graph.top(), graph.bottom() - h);
    let (center, middle) = (left + (right - left) / 2, top + (bottom - top) / 2);
    [
        (right, top),
        (left, top),
        (right, bottom),
        (left, bottom),
        (center, top),
        (center, bottom),
        (left, middle),
        (right, middle),
    ]
    .into_iter()
    .min_by_key(|&(x, y)| marks(x, y))
    .map(|(x, y)| LegendPlace {
        area: Rect::new(x, y, w, h),
        name_width,
    })
}

/// The legend in `place`: on the plot's background, cleared of the marks under it,
/// a swatch in each series' color and its name beside it. No frame: the cleared
/// patch sets it off.
fn draw_legend(buf: &mut Buffer, legend: &Legend, place: LegendPlace, text: Style, g: &Glyphs) {
    let area = place.area;
    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            buf[(x, y)].reset();
        }
    }
    for ((name, style), y) in legend.entries.iter().zip(area.top()..) {
        buf.set_string(area.x + 1, y, g.bar_eighths[7], *style);
        let name = cut(name, place.name_width, g);
        buf.set_string(area.x + 3, y, name, text);
    }
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
    let groups = axis.tick_sets(track.length(), Y_SPACING, Y_LEAST, Y_MINOR_GAP);
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

/// The x labels fitting a row over columns `span` under a plot of `track`, each centered
/// under its tick, kept on the row, `LABEL_GAP` cells apart: about one per 15 columns,
/// coarser then shorter when crowded, none when even two do not fit.
pub fn fit_x_labels(axis: &AxisSpec<'_>, span: (u16, u16), track: Track) -> Placed {
    let (start, end) = span;
    let column = |v: f64| track.cell(fraction(v, axis.bounds));
    let groups = axis.tick_sets(track.length(), X_SPACING, X_LEAST, X_MINOR_GAP);
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
mod tests;

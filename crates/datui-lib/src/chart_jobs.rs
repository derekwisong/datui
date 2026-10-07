//! Chart preparation off the UI thread: what a spec asks for, what the worker
//! prepared, the cache of both, and chart exports written from it.

use std::path::Path;
use std::sync::{Arc, Mutex};

use color_eyre::Result;
use polars::prelude::{LazyFrame, Schema};

use crate::chart_data::{self, ColorSplit, ValueRange};
use crate::chart_modal::{Aggregate, ChartModal, ChartSpec, ColorCounts, Mark};
use crate::jobs::{Answer, Job};
use crate::output_file::Overwrite;
use crate::{
    App, AppEvent, ExportProgress, InputMode, chart_export, logging, numfmt, output_file, sampling,
};
use chart_export::{ChartExportFormat, ChartExportRequest, ExportOptions, Figure};

/// Outcomes of chart preparation keyed by the request that produced them, least
/// recently used first. A failure is remembered too, so a selection that cannot be
/// charted is not retried after every event; the chart shows its message.
/// Bounded so that toggling between a few selections does not collect again, without
/// holding every series ever prepared: line and scatter series are the only payload
/// that grows with the row limit, so few of those are kept and only the current one
/// has its log copy.
#[derive(Default)]
pub(crate) struct ChartCache {
    pub(crate) entries: Vec<(ChartRequest, Result<ChartPrepared, String>)>,
    /// The rows read for these entries and which view (`len_generation`) they are
    /// from, so another option re-draws from them rather than reading again.
    held: chart_data::HeldRows,
    held_view: Option<u64>,
    /// The Color columns' values as counted for these entries, so going back to a
    /// color whose chart is cached brings its value picker back too.
    colors: Vec<ColorCounts>,
}

impl ChartCache {
    pub(crate) const CAPACITY: usize = 8;
    pub(crate) const XY_CAPACITY: usize = 2;

    /// Forget every entry and the rows read. A worker still reading keeps the handle it
    /// was given and fills that one, never the next view's.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.held = chart_data::HeldRows::default();
        self.held_view = None;
        self.colors.clear();
    }

    /// Keep a Color column's values, the last count of it.
    pub(crate) fn hold_colors(&mut self, colors: ColorCounts) {
        self.colors.retain(|c| c.column != colors.column);
        self.colors.push(colors);
    }

    /// The values counted for Color column `column`.
    pub(crate) fn colors(&self, column: &str) -> Option<&ColorCounts> {
        self.colors.iter().find(|c| c.column == column)
    }

    /// The rows held for `view`, or a fresh holder when they belong to another.
    pub(crate) fn held_rows(&mut self, view: Option<u64>) -> chart_data::HeldRows {
        if self.held_view != view {
            self.held = chart_data::HeldRows::default();
            self.held_view = view;
        }
        self.held.clone()
    }

    pub(crate) fn get(&self, request: &ChartRequest) -> Option<&Result<ChartPrepared, String>> {
        self.entries
            .iter()
            .find(|(r, _)| r == request)
            .map(|(_, outcome)| outcome)
    }

    /// The prepared data for `request`, if it has been prepared.
    pub(crate) fn prepared(&self, request: &ChartRequest) -> Option<&ChartPrepared> {
        self.get(request).and_then(|outcome| outcome.as_ref().ok())
    }

    /// What stands in for `request` while it is prepared: the newest chart prepared of
    /// the same columns, drawn under another option (sample size, bins, range).
    pub(crate) fn standing_in(&self, request: &ChartRequest) -> Option<&ChartPrepared> {
        self.entries
            .iter()
            .rev()
            .filter(|(r, _)| r.same_columns(request))
            .find_map(|(_, outcome)| outcome.as_ref().ok())
    }

    /// Whether `request` has been prepared.
    pub(crate) fn satisfies(&self, request: &ChartRequest) -> bool {
        self.prepared(request).is_some()
    }

    pub(crate) fn insert(&mut self, request: ChartRequest, outcome: Result<ChartPrepared, String>) {
        self.entries.retain(|(r, _)| *r != request);
        self.entries.push((request, outcome));
        Self::evict(&mut self.entries, Self::XY_CAPACITY, |outcome| {
            matches!(outcome, Ok(ChartPrepared::XY(_)))
        });
        Self::evict(&mut self.entries, Self::CAPACITY, |_| true);
    }

    /// Drop the least recently used of the entries `counts` selects until at most `cap`
    /// remain.
    fn evict(
        entries: &mut Vec<(ChartRequest, Result<ChartPrepared, String>)>,
        cap: usize,
        counts: impl Fn(&Result<ChartPrepared, String>) -> bool,
    ) {
        let mut over = entries
            .iter()
            .filter(|(_, o)| counts(o))
            .count()
            .saturating_sub(cap);
        entries.retain(|(_, o)| {
            if over > 0 && counts(o) {
                over -= 1;
                false
            } else {
                true
            }
        });
    }

    /// Note that `request` is the selection on screen: its entry moves to the back,
    /// where eviction reaches it last, and it alone keeps a log-scale copy of its
    /// series, built here when wanted. A pure in-memory map, cheap enough for the event
    /// thread; it never happens in render.
    pub(crate) fn touch(&mut self, request: &ChartRequest, log_scale: bool) {
        let Some(i) = self.entries.iter().position(|(r, _)| r == request) else {
            return;
        };
        let current = self.entries.remove(i);
        self.entries.push(current);
        let Some(((_, current), others)) = self.entries.split_last_mut() else {
            return;
        };
        for (_, outcome) in others {
            if let Ok(ChartPrepared::XY(xy)) = outcome {
                xy.series_log = None;
            }
        }
        if let Ok(ChartPrepared::XY(xy)) = current
            && log_scale
            && xy.series_log.is_none()
        {
            xy.series_log = Some(log_series(&xy.series));
        }
    }
}

/// The least and greatest X and Y over every point; `None` with no points.
pub(crate) fn extent(series: &[Vec<(f64, f64)>]) -> Option<[f64; 4]> {
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

pub(crate) fn log_series(series: &[Vec<(f64, f64)>]) -> Vec<Vec<(f64, f64)>> {
    series
        .iter()
        .map(|pts| pts.iter().map(|&(x, y)| (x, log_y(y))).collect())
        .collect()
}

/// What the chart view needs prepared for the panel's spec. Compared with the cache
/// and with the computation in flight, so each spec is prepared once, off the UI
/// thread, and a result for a spec the user has since moved past is stale.
///
/// Only what the chart type reads takes part: another bin count on a line chart is
/// the same request.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChartRequest {
    pub(crate) spec: ChartSpec,
    pub(crate) bins: usize,
    pub(crate) bandwidth: f64,
    pub(crate) range: ValueRange,
    pub(crate) order: chart_data::BarOrder,
    pub(crate) share: bool,
    /// Rows a chart that samples reads; `None` for one that aggregates every row.
    pub(crate) row_limit: Option<usize>,
    /// A line is drawn from each step's lowest and highest value rather than a
    /// sample: see [`chart_data::prepare_chart_data`].
    pub(crate) envelope: bool,
    /// Only an X is picked: its range gives the empty axes their bounds.
    pub(crate) x_only: bool,
    /// Most series drawn: one per distinct series color on this terminal.
    pub(crate) series_cap: usize,
    /// First or last over a sorted view: the rows are read in its order.
    pub(crate) sorted: bool,
}

impl ChartRequest {
    /// Whether preparing `other` reads what this needs: another order of the same
    /// bars is the same pass.
    pub(crate) fn reads_as(&self, other: &Self) -> bool {
        Self {
            order: other.order,
            ..self.clone()
        } == *other
    }

    /// Whether this is `last` with another aggregate: a step through the aggregates.
    pub(crate) fn steps_aggregate_from(&self, last: &Self) -> bool {
        let mut spec = self.spec.clone();
        if spec.encoding.y.aggregate == last.spec.encoding.y.aggregate {
            return false;
        }
        spec.encoding.y.aggregate = last.spec.encoding.y.aggregate;
        spec == last.spec
    }

    /// Whether `other` is the same chart of the same columns, whatever its options.
    pub(crate) fn same_columns(&self, other: &Self) -> bool {
        let (a, b) = (&self.spec, &other.spec);
        // The bucket, the aggregate and cumulative are what the numbers are: a chart
        // under another of them would show old numbers under new labels.
        a.mark == b.mark
            && a.encoding.x == b.encoding.x
            && a.encoding.y == b.encoding.y
            && a.encoding.color.field == b.encoding.color.field
            && self.x_only == other.x_only
    }

    /// The request for what the panel shows, or `None` while a shelf the chart needs
    /// is empty.
    pub(crate) fn from_modal(modal: &ChartModal) -> Option<Self> {
        let mut spec = modal.effective_spec();
        let mark = spec.mark;
        let x_only = mark.is_xy()
            && spec.encoding.x.field.is_some()
            && spec.encoding.y.field.is_empty()
            && spec.encoding.y.aggregate != Aggregate::Count;
        if !x_only && !ChartModal::is_complete(&spec) {
            return None;
        }
        // Leave out what this chart does not read, so a change to it asks for nothing.
        // Whether color splits the chart is read from the spec charted, a Y the
        // picker previews included.
        // No more series than there are colors to tell them apart.
        let series_cap = modal.series_max();
        spec.encoding.y.field.truncate(series_cap);
        spec.encoding.color.values.truncate(series_cap);
        let colored = ChartModal::colored_in(&spec);
        if colored {
            spec.encoding.color.other = Some(ChartModal::shows_other_in(&spec));
        } else {
            spec.encoding.color = Default::default();
        }
        if x_only {
            spec.encoding.y = Default::default();
            spec.encoding.color = Default::default();
            spec.encoding.x.time_unit = Default::default();
        }
        let aggregates = modal.aggregates() && !x_only;
        let bins = match mark {
            Mark::Histogram => modal.hist_bins,
            Mark::Heatmap => modal.heatmap_bins,
            _ => 0,
        };
        Some(Self {
            bins,
            bandwidth: if mark == Mark::Kde {
                modal.kde_bandwidth_factor
            } else {
                0.0
            },
            range: if matches!(mark, Mark::Histogram | Mark::Kde | Mark::Box) {
                modal.value_range
            } else {
                ValueRange::All
            },
            order: if mark == Mark::Bar {
                modal.bar_order
            } else {
                Default::default()
            },
            share: mark == Mark::Histogram && modal.share,
            row_limit: if aggregates { None } else { modal.row_limit },
            envelope: mark == Mark::Line && !aggregates && !colored,
            x_only,
            sorted: aggregates
                && spec.encoding.y.aggregate.follows_row_order()
                && modal.row_order.is_some(),
            spec,
            series_cap,
        })
    }

    /// Whether this request groups every row of the view rather than sampling.
    pub(crate) fn aggregates(&self) -> bool {
        matches!(self.spec.mark, Mark::Line | Mark::Scatter | Mark::Bar)
            && self.spec.encoding.y.aggregate != Aggregate::None
            && !self.x_only
    }

    /// The Polars work. Runs on a worker thread. With a color, the color column's
    /// values are counted first (and handed back for the value picker), and the
    /// groups are the ones picked or the largest.
    pub(crate) fn prepare(
        &self,
        lf: &LazyFrame,
        schema: &Schema,
        sampling: &chart_data::ChartSampling,
    ) -> Result<(ChartPrepared, Option<ColorCounts>)> {
        let encoding = &self.spec.encoding;
        let counts = encoding
            .color
            .field
            .as_deref()
            .map(|c| chart_data::value_rows(lf, c, sampling).map(|rows| (c, rows)))
            .transpose()?;
        let groups = counts.as_ref().map(|(_, rows)| {
            chart_data::color_groups(rows, &encoding.color.values, self.series_cap)
        });
        // Some value without a series of its own: Other takes its rows, or they are
        // left out and the note says the rows are the groups'.
        let left_out = counts
            .as_ref()
            .zip(groups.as_ref())
            .is_some_and(|((_, rows), groups)| {
                rows.values.iter().any(|(v, _)| !groups.contains(v))
            });
        let other = encoding.color.other == Some(true) && left_out;
        let split = counts
            .as_ref()
            .zip(groups.as_ref())
            .map(|((column, _), groups)| ColorSplit {
                column,
                groups,
                other,
            });
        let picker = counts.as_ref().map(|(column, rows)| ColorCounts {
            column: column.to_string(),
            values: rows.values.clone(),
        });
        let x = encoding.x.field.as_deref().unwrap_or_default();
        let first_y = encoding.y.field.first().map(String::as_str);
        let prepared = match self.spec.mark {
            Mark::Line | Mark::Scatter if self.x_only => {
                ChartPrepared::XRange(chart_data::prepare_chart_x_range(lf, schema, x, sampling)?)
            }
            Mark::Line | Mark::Scatter => {
                let aggregate = encoding.y.aggregate;
                let grouped = if aggregate != Aggregate::None {
                    chart_data::prepare_aggregate_xy(
                        lf,
                        schema,
                        &chart_data::AggregateSpec {
                            x,
                            time_unit: encoding.x.time_unit,
                            ys: &encoding.y.field,
                            aggregate,
                            quantile: encoding.y.quantile(),
                            cumulative: encoding.y.cumulative,
                            color: split,
                        },
                        sampling,
                    )?
                } else if let (Some(split), Some(y)) = (split, first_y) {
                    chart_data::prepare_xy_by(lf, schema, x, y, split, sampling)?
                } else {
                    let r = chart_data::prepare_chart_data(
                        lf,
                        schema,
                        x,
                        &encoding.y.field,
                        sampling,
                        self.envelope,
                    )?;
                    chart_data::GroupedSeries {
                        names: encoding.y.field.clone(),
                        series: r.series,
                        breaks: r.breaks,
                        x_axis_kind: r.x_axis_kind,
                        rows: r.rows,
                        other: false,
                    }
                };
                ChartPrepared::XY(ChartCacheXY {
                    other: grouped.other,
                    names: grouped.names,
                    xs: crate::widgets::crosshair::xs(&grouped.series),
                    bounds: extent(&grouped.series),
                    breaks: grouped.breaks,
                    series: grouped.series,
                    series_log: None,
                    x_axis_kind: grouped.x_axis_kind,
                    rows: grouped.rows,
                    rows_note: (aggregate != Aggregate::None).then(|| {
                        rows_note(
                            grouped.rows.total_rows,
                            sampling.known_total == Some(grouped.rows.total_rows),
                            left_out && !other,
                        )
                    }),
                })
            }
            Mark::Bar if encoding.y.aggregate != Aggregate::None => {
                let mut data = chart_data::prepare_bar_aggregate(
                    lf,
                    schema,
                    &chart_data::BarAggregate {
                        category: x,
                        value: first_y,
                        aggregate: encoding.y.aggregate,
                        quantile: encoding.y.quantile(),
                        color: split,
                        order: self.order,
                        cap: chart_data::BAR_CAP,
                    },
                    sampling,
                )?;
                // Every category is a bar, a null one too: uncolored, it is every row.
                data.rows_note = Some(rows_note(data.rows.total_rows, true, left_out && !other));
                ChartPrepared::Bar(data)
            }
            Mark::Bar => ChartPrepared::Bar(chart_data::prepare_bar_data(
                lf,
                x,
                first_y.unwrap_or_default(),
                self.order,
                chart_data::BAR_CAP,
                sampling,
            )?),
            Mark::Histogram => ChartPrepared::Histogram(chart_data::prepare_histogram_by(
                lf, x, self.bins, self.range, self.share, split, sampling,
            )?),
            Mark::Kde => ChartPrepared::Kde(match split {
                Some(split) => {
                    chart_data::prepare_kde_by(lf, x, self.bandwidth, self.range, split, sampling)?
                }
                None => chart_data::prepare_kde_data(lf, x, self.bandwidth, self.range, sampling)?,
            }),
            Mark::Box => {
                let y = first_y.unwrap_or_default();
                ChartPrepared::BoxPlot(match encoding.x.field.as_deref() {
                    // One box per category: the largest by rows.
                    Some(by) => {
                        let rows = chart_data::value_rows(lf, by, sampling)?;
                        let groups = chart_data::color_groups(&rows, &[], self.series_cap);
                        let mut data = chart_data::prepare_box_by(
                            lf,
                            y,
                            ColorSplit {
                                column: by,
                                groups: &groups,
                                other: false,
                            },
                            self.range,
                            sampling,
                        )?;
                        // Counted only when some category was left without a box.
                        if rows.values.len() > groups.len() {
                            data.of = rows.values.len();
                        }
                        data
                    }
                    None => chart_data::prepare_box_plot_data(lf, y, self.range, sampling)?,
                })
            }
            Mark::Heatmap => ChartPrepared::Heatmap(chart_data::prepare_heatmap_data(
                lf,
                x,
                first_y.unwrap_or_default(),
                self.bins,
                sampling,
            )?),
        };
        Ok((prepared, picker))
    }
}

/// The outcome handed from the chart worker to `BackgroundChartReady`, with the
/// Color column's values when it counted them.
pub(crate) type ChartResultSlot =
    Arc<Mutex<Option<Result<(ChartPrepared, Option<ColorCounts>), String>>>>;

/// The chart preparation currently running. There is at most one: a burst of selection
/// changes must not fan out into a full collect per column, so the next request waits
/// for this one to land and then the newest selection is the one prepared. Being the
/// only one is also what ties a `BackgroundChartReady` to it, so no generation is
/// needed to match them up.
pub(crate) struct ChartInflight {
    /// `len_generation` of the dataset the request was spawned against, so a result
    /// cannot be installed for a different dataset that happens to share column names.
    pub(crate) dataset: Option<u64>,
    pub(crate) request: ChartRequest,
    /// Set when the view or dataset it was spawned for has gone. The worker cannot be
    /// cancelled, so the record stays until its result lands and is discarded; the next
    /// request waits for it, which is what keeps the number of collects at one.
    pub(crate) stale: bool,
    /// Set when the selection moves past the request or its view goes. A streamed
    /// count or group-by stops at its next batch; a sampled read is bounded and runs
    /// to the end.
    pub(crate) cancel: Arc<std::sync::atomic::AtomicBool>,
}

/// A prepared chart, ready to go into the cache. Each payload names the columns it
/// was drawn from, since render and export label the axes from it.
pub(crate) enum ChartPrepared {
    XY(ChartCacheXY),
    XRange(chart_data::ChartXRangeResult),
    Histogram(chart_data::HistogramData),
    BoxPlot(chart_data::BoxPlotData),
    Kde(chart_data::KdeData),
    Heatmap(chart_data::HeatmapData),
    Bar(chart_data::BarData),
}

impl ChartPrepared {
    /// What the chart says under the plot about the rows and values it drew;
    /// `middot` joins a sample's seed on.
    pub(crate) fn notes(&self, middot: &str) -> Vec<String> {
        let rows_of = |rows: usize| crate::discover::format_rows(rows);
        match self {
            Self::Bar(d) => {
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
            Self::XY(c) if c.rows_note.is_some() => c.rows_note.iter().cloned().collect(),
            Self::XY(c) => chart_data::chart_notes(&c.rows, None, middot),
            Self::XRange(c) => chart_data::chart_notes(&c.rows, None, middot),
            Self::Histogram(d) => chart_data::chart_notes(&d.rows, d.clipped.as_ref(), middot),
            Self::BoxPlot(d) => {
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

/// A chart export with its figure built from the cache; `write` is the slow part and
/// runs off the UI thread.
pub(crate) struct ChartExportJob {
    pub(crate) figure: Figure,
    pub(crate) options: ExportOptions,
}

impl ChartExportJob {
    /// Draw the chart into a temporary file and commit it over `path`.
    pub(crate) fn write(
        &self,
        path: &Path,
        format: ChartExportFormat,
        overwrite: Overwrite,
    ) -> Result<()> {
        let bytes = chart_export::render(&self.figure, &self.options, format)?;
        let out = output_file::OutputFile::create(path, overwrite)?;
        std::fs::write(out.path(), bytes)?;
        out.commit()?;
        Ok(())
    }
}

pub(crate) struct ChartCacheXY {
    /// One per series: its Y column, or its color group.
    pub(crate) names: Vec<String>,
    pub(crate) series: Vec<Vec<(f64, f64)>>,
    /// Where each series' line starts again after a null (see `chart_data::segments`).
    pub(crate) breaks: Vec<Vec<usize>>,
    pub(crate) series_log: Option<Vec<Vec<(f64, f64)>>>,
    /// Every X of every series, in order, each once: where the crosshair stops.
    pub(crate) xs: Vec<f64>,
    /// The least and greatest X and Y over every point, before any log.
    pub(crate) bounds: Option<[f64; 4]>,
    pub(crate) x_axis_kind: chart_data::XAxisTemporalKind,
    pub(crate) rows: chart_data::RowsRead,
    /// What an aggregate over every row read, said in the title row.
    pub(crate) rows_note: Option<String>,
    /// The last series is Other.
    pub(crate) other: bool,
}

/// What an aggregate read, under the plot: every row of the view (`all 336,776
/// rows`) when it counted them all, the rows of the groups a color drew, or the rows
/// with an X.
fn rows_note(counted: usize, whole: bool, grouped: bool) -> String {
    let n = numfmt::group_chrome(counted);
    if grouped {
        format!("{n} rows in the groups shown")
    } else if whole {
        format!("all {n} rows")
    } else {
        format!("{n} rows")
    }
}

impl App {
    /// True while chart data for the current view is being prepared off-thread — either
    /// its worker is running, or it is waiting its turn behind an orphaned worker that
    /// cannot be cancelled (see `ChartInflight::stale`). Either way the user is waiting
    /// on a computation and the throbber should say so.
    pub fn chart_preparing(&self) -> bool {
        match self.chart_inflight.as_ref() {
            Some(inflight) if !inflight.stale => true,
            Some(_) => self.chart_request_pending(),
            None => self.chart_settling(),
        }
    }

    /// Whether the selection on screen is a step through the aggregates still waiting
    /// for the next step.
    fn chart_settling(&self) -> bool {
        self.chart_asked
            .as_ref()
            .and_then(|(_, until)| *until)
            .is_some_and(|until| std::time::Instant::now() < until)
    }

    /// Whether the chart view wants data it does not have and cannot be told it will
    /// never get.
    pub(crate) fn chart_request_pending(&self) -> bool {
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return false;
        }
        ChartRequest::from_modal(&self.chart_modal)
            .is_some_and(|request| self.chart_cache.get(&request).is_none())
    }

    /// Forget everything chart-related that belongs to the view or dataset on its way
    /// out: the cache, the handed-over slot, an export parked on data that is now never
    /// coming, and an export write still running (its file may still appear, but its
    /// result is ignored and `busy` is released). The preparation in flight is marked
    /// stale rather than forgotten: it cannot be cancelled, so it is waited for and its
    /// result discarded on arrival. Called when the chart view closes and whenever the
    /// dataset changes or is left for the home screen.
    pub(crate) fn reset_chart_state(&mut self) {
        self.chart_cache.clear();
        self.chart_asked = None;
        if let Some(inflight) = self.chart_inflight.as_mut() {
            inflight.stale = true;
            inflight
                .cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        // A failed export reopens its modal; it must not follow the user to the next
        // dataset.
        self.chart_export_modal.close();
        *self
            .pending_chart_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        let writing = self
            .jobs
            .supersede(|job| matches!(job, Job::ChartExport { .. }));
        let waiting = self.chart_export_waiting.take().is_some();
        if writing || waiting {
            self.export_progress = None;
            self.status_message = None;
            self.busy = false;
        }
    }

    /// What the chart being prepared is doing: an aggregate groups every row of the
    /// view, and says how many where the table knows.
    pub(crate) fn chart_status(&self) -> String {
        let aggregating = self
            .chart_inflight
            .as_ref()
            .filter(|i| !i.stale)
            .map(|i| i.request.aggregates())
            .or_else(|| ChartRequest::from_modal(&self.chart_modal).map(|r| r.aggregates()))
            .unwrap_or(false);
        if !aggregating {
            return "Preparing chart...".to_string();
        }
        match self
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid())
        {
            Some(rows) => format!("Grouping {} rows...", crate::discover::format_rows(rows)),
            None => "Grouping every row...".to_string(),
        }
    }

    /// The series of the line or scatter chart on screen, by name, once prepared:
    /// its Y columns or its color groups.
    pub fn chart_names(&self) -> Option<Vec<String>> {
        let request = ChartRequest::from_modal(&self.chart_modal)?;
        match self.chart_cache.prepared(&request)? {
            ChartPrepared::XY(xy) => Some(xy.names.clone()),
            _ => None,
        }
    }

    /// True when the chart cache holds the data for the modal's current selection.
    pub fn chart_data_ready(&self) -> bool {
        ChartRequest::from_modal(&self.chart_modal).is_some_and(|r| self.chart_cache.satisfies(&r))
    }

    /// Start preparing the chart the modal currently asks for, unless the cache already
    /// has it, it is known to fail, or another preparation is still running (the newest
    /// selection is picked up when that one lands). Runs after every event, so a change
    /// of column or option is noticed as soon as it is made and render only ever draws.
    pub(crate) fn ensure_chart_data(&mut self) {
        const CHART_AGGREGATE_SETTLE: std::time::Duration = std::time::Duration::from_millis(150);
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return;
        }
        // What Every row costs, as the table counted it.
        self.chart_modal.view_rows = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.num_rows_if_valid());
        let request = ChartRequest::from_modal(&self.chart_modal);
        if let Some(inflight) = self.chart_inflight.as_ref()
            && !request
                .as_ref()
                .is_some_and(|r| r.reads_as(&inflight.request))
        {
            // A count streaming a large view for a selection the cursor has moved
            // past would hold up the next chart for as long as it reads.
            inflight
                .cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let Some(request) = request else {
            return;
        };
        // Stepping none, count, distinct, sum, mean grouped every row at each step, and
        // drew each: a step waits a moment for the next, and only where it stops is
        // prepared. A Wake when the wait ends prepares it.
        let settle = match self.chart_asked.take() {
            Some((asked, until)) if asked == request => until,
            Some((asked, _)) if request.steps_aggregate_from(&asked) => {
                let tx = self.events.clone();
                std::thread::spawn(move || {
                    std::thread::sleep(CHART_AGGREGATE_SETTLE);
                    let _ = tx.send(AppEvent::Wake);
                });
                Some(std::time::Instant::now() + CHART_AGGREGATE_SETTLE)
            }
            _ => None,
        };
        self.chart_asked = Some((request.clone(), settle));
        if self.chart_cache.get(&request).is_some() {
            self.chart_cache.touch(&request, self.chart_modal.log_scale);
            // A cached chart's colors were counted with it.
            if let Some(colors) = request
                .spec
                .encoding
                .color
                .field
                .as_deref()
                .and_then(|c| self.chart_cache.colors(c))
            {
                self.chart_modal.color_counts = Some(colors.clone());
            }
            return;
        }
        if self.chart_inflight.is_some() || self.chart_settling() {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // Unsorted: the rows a chart draws do not depend on the table's order, a line
        // is drawn in X order anyway, and a sort would make a sampled read read it all.
        // First and last are the order's: they read the view as sorted.
        let lf = if request.sorted {
            state.lf().clone()
        } else {
            state.analysis_lf()
        };
        let schema = state.schema().clone();
        let dataset = Some(state.len_generation());
        let sampling = chart_data::ChartSampling {
            // None for an aggregate: it reads every row.
            limit: request.row_limit,
            known_total: state.num_rows_if_valid(),
            seed: self.analysis_modal.sample.seed,
            streaming: self.app_config.performance.streaming,
            full_passes: !state.is_remote_source(),
            held: self.chart_cache.held_rows(dataset),
            cancel: Arc::default(),
        };
        self.chart_inflight = Some(ChartInflight {
            dataset,
            request: request.clone(),
            stale: false,
            cancel: Arc::clone(&sampling.cancel),
        });
        let slot = self.pending_chart_result.clone();
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            // A panic in the preparation must still report back: without the event the
            // in-flight record would stand for the rest of the session and every later
            // selection would be refused.
            let result = logging::catch_panic(|| request.prepare(&lf, &schema, &sampling))
                .unwrap_or_else(|_| Err(color_eyre::eyre::eyre!("Chart preparation panicked")))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None));
            *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            let _ = tx.send(AppEvent::BackgroundChartReady);
        });
    }

    /// The chart view's events: an export asked for, and a preparation landing.
    pub(crate) fn chart_event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        match event {
            AppEvent::ChartExport(request) => {
                self.busy = true;
                self.export_progress = Some(ExportProgress::new(&request.path, "Exporting chart"));
                Some(AppEvent::DoChartExport(request.clone()))
            }
            AppEvent::DoChartExport(request) => {
                // `ChartExport` arms `busy` and defers here so the phase can be drawn
                // first. A Ctrl-O in that window has already left the chart view, and
                // there is nothing to export any more: release the app rather than park
                // an export that no view would ever prepare.
                if self.input_mode != InputMode::Chart || !self.chart_modal.active {
                    self.export_progress = None;
                    self.status_message = None;
                    self.busy = false;
                    return None;
                }
                self.start_chart_export(request.clone());
                None
            }
            AppEvent::BackgroundChartReady => {
                // The result belongs to the one preparation in flight. It is installed
                // only while that record is current (a reset marks it stale when its
                // view or dataset goes) and only into the dataset it was computed from.
                // Taking the record is what lets the next request start; the slot is
                // emptied either way so a discarded series is not kept around.
                let inflight = self.chart_inflight.take()?;
                let outcome = self
                    .pending_chart_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| Err("Chart preparation produced no result".to_string()));
                if inflight.stale {
                    return None;
                }
                // A count stopped part way is no answer, and must not be remembered as
                // a failure; the selection is prepared again when it comes back.
                if outcome.is_err() && inflight.cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    return None;
                }
                let dataset = self.data_table_state.as_ref().map(|s| s.len_generation());
                if dataset != inflight.dataset {
                    return None;
                }
                let outcome = outcome.map(|(prepared, colors)| {
                    if let Some(colors) = colors {
                        self.chart_cache.hold_colors(colors.clone());
                        self.chart_modal.color_counts = Some(colors);
                    }
                    prepared
                });
                self.chart_cache.insert(inflight.request, outcome);
                // An export parked on chart data resumes against the *current*
                // selection, whatever just landed: it is written if that selection is
                // now prepared, fails with the reason if that is the one that failed,
                // and otherwise waits for the next result (which `ensure_chart_data`
                // starts once this handler returns).
                if let Some(request) = self.chart_export_waiting.take() {
                    self.start_chart_export(request);
                }
                None
            }
            _ => unreachable!("not an event for chart_event"),
        }
    }

    /// The figure to export from the prepared chart for the current spec. `Ok(None)`
    /// means that chart is still being prepared and the caller should wait for it.
    pub(crate) fn build_chart_figure(&self) -> Result<Option<Figure>> {
        let Some(state) = self.data_table_state.as_ref() else {
            return Err(color_eyre::eyre::eyre!("No data loaded"));
        };
        let modal = &self.chart_modal;
        let Some(request) = ChartRequest::from_modal(modal).filter(|r| !r.x_only) else {
            return Err(color_eyre::eyre::eyre!(
                "Pick the columns the chart needs first"
            ));
        };
        let prepared = match self.chart_cache.get(&request) {
            Some(Ok(prepared)) => prepared,
            // A selection known not to chart is never retried, so waiting for its data
            // would wait forever: fail the export now with the reason.
            Some(Err(message)) => return Err(color_eyre::eyre::eyre!("{}", message)),
            None => return Ok(None),
        };
        let context = PlotContext {
            modal,
            spec: &request.spec,
            numbers: &self.number_format,
            schema: Some(state.schema().as_ref()),
        };
        // A single X column has nothing to export.
        let plot = match prepared {
            ChartPrepared::XRange(_) => None,
            prepared => plot(Some(prepared), &context).filter(|plot| !plot.is_empty()),
        }
        .ok_or_else(|| color_eyre::eyre::eyre!("No valid data points to export"))?;
        let mut plot = plot.into_owned();
        // The screen's title row says how the rows were made; a file has none, so
        // its Y axis names the aggregate.
        let y = &request.spec.encoding.y;
        if let chart_export::Plot::Lines(lines) = &mut plot
            && !matches!(y.aggregate, Aggregate::None | Aggregate::Count)
        {
            lines.y.title = format!("{} {}", y.aggregate_name(), lines.y.title);
        }
        Ok(Some(Figure {
            plot,
            // The file always has the middle dot; the terminal may be ASCII.
            chart_notes: self.chart_notes_of(prepared, "·"),
            grid: modal.grid,
        }))
    }

    /// Write the chart from the prepared data off-thread, or park the export until that
    /// data is ready. `busy` was set by `ChartExport` and stays set until the export ends.
    fn start_chart_export(&mut self, mut request: ChartExportRequest) {
        // How the chart was made, from the view and chart as they are now; none
        // when the dialog says Omit.
        request.options.recipe = if request.recipe {
            self.chart_recipe()
        } else {
            None
        };
        match self.build_chart_figure() {
            Ok(Some(figure)) => {
                self.chart_export_waiting = None;
                let write = Job::ChartExport {
                    path: request.path.clone(),
                    format: request.format,
                };
                self.spawn_job(write, Some("Exporting chart..."), move |_| {
                    let ChartExportRequest {
                        path,
                        format,
                        options,
                        overwrite,
                        ..
                    } = request;
                    ChartExportJob { figure, options }
                        .write(&path, format, overwrite)
                        .map_err(|e| Self::format_export_error(&e))?;
                    Ok(Answer::ChartExported)
                });
            }
            // Still being prepared; `BackgroundChartReady` comes back here.
            Ok(None) => self.chart_export_waiting = Some(request),
            Err(e) => {
                let message = Self::format_export_error(&e);
                self.finish_chart_export(&request.path, request.format, Err(message));
            }
        }
    }

    pub(crate) fn finish_chart_export(
        &mut self,
        path: &Path,
        format: ChartExportFormat,
        result: Result<(), String>,
    ) {
        self.chart_export_waiting = None;
        self.export_progress = None;
        self.status_message = None;
        self.busy = false;
        match result {
            Ok(()) => {
                self.flash_path("Chart exported to ", path);
                self.chart_export_modal.close();
            }
            // The form comes back as it was, the reason on its status line.
            Err(message) => {
                self.chart_export_modal.reopen_with_path(path, format);
                self.chart_export_modal.error = Some(message);
            }
        }
    }

    /// What a chart says under its plot about the rows it drew. A view with a sample
    /// is charted from all of it: the note says which sample, with its seed, so the
    /// chart can be drawn again.
    pub(crate) fn chart_notes_of(
        &self,
        prepared: &crate::chart_jobs::ChartPrepared,
        middot: &str,
    ) -> Vec<String> {
        let mut notes = prepared.notes(middot);
        let sampled = self.data_table_state.as_ref().and_then(|s| s.sampled());
        if let Some(sampled) = sampled {
            let mut note = sampled.label();
            if matches!(
                sampled.sample().method,
                sampling::SampleMethod::Spread | sampling::SampleMethod::PerPartition { .. }
            ) {
                note.push_str(&format!(" {middot} seed {}", sampled.sample().seed));
            }
            notes.insert(0, note);
        }
        notes
    }
}

/// What a plot is drawn from beside its data: the panel's options, the spec, and
/// how the view's columns print.
pub(crate) struct PlotContext<'a> {
    pub(crate) modal: &'a ChartModal,
    /// The spec drawn: on screen, with the picker's choice previewed.
    pub(crate) spec: &'a crate::chart_modal::ChartSpec,
    pub(crate) numbers: &'a crate::numfmt::NumberFormatSettings,
    pub(crate) schema: Option<&'a Schema>,
}

/// The plot of `prepared` for the spec, as the screen and every export draw it: a
/// line or scatter chart's axes always, standing empty until its series arrive;
/// any other kind once its data has.
pub(crate) fn plot<'a>(
    prepared: Option<&'a ChartPrepared>,
    context: &PlotContext<'_>,
) -> Option<chart_export::Plot<'a>> {
    use chart_data::AxisNumbers;
    use chart_export::{Axis, Plot};
    use std::borrow::Cow;
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
    Some(match (spec.mark, prepared) {
        (Mark::Line | Mark::Scatter, prepared) => Plot::Lines(lines(prepared, context)),
        (Mark::Histogram, Some(ChartPrepared::Histogram(data))) => Plot::Histogram {
            x: axis(title(&data.column), column(&data.column)),
            y: if data.share {
                axis("Share".to_string(), AxisNumbers::measure(numbers, "Share"))
            } else {
                axis("Count".to_string(), AxisNumbers::count(numbers))
            },
            data: Cow::Borrowed(data),
        },
        (Mark::Kde, Some(ChartPrepared::Kde(data))) => Plot::Kde {
            x: axis(
                x_name.map(title).unwrap_or_default(),
                x_name.map(column).unwrap_or_default().fractional(),
            ),
            y: axis(
                "Density".to_string(),
                AxisNumbers::measure(numbers, "Density"),
            ),
            data: Cow::Borrowed(data),
        },
        (Mark::Box, Some(ChartPrepared::BoxPlot(data))) => Plot::Box {
            x_title: x_name.unwrap_or_default().to_string(),
            y: axis(
                ys.first().map(|y| title(y)).unwrap_or_default(),
                AxisNumbers::columns(numbers, schema, ys),
            ),
            data: Cow::Borrowed(data),
        },
        (Mark::Heatmap, Some(ChartPrepared::Heatmap(data))) => Plot::Heatmap {
            x: axis(title(&data.x_column), column(&data.x_column)),
            y: axis(title(&data.y_column), column(&data.y_column)),
            data: Cow::Borrowed(data),
        },
        (Mark::Bar, Some(ChartPrepared::Bar(data))) => Plot::Bars {
            value: axis(
                data.value_column.clone(),
                AxisNumbers {
                    format: data.value_format(numbers),
                    whole: data.value_dtype.is_integer(),
                },
            ),
            data: Cow::Borrowed(data),
        },
        _ => return None,
    })
}

/// A line or scatter chart's series and axes; the axes alone, over the X column's
/// range or typed from the schema, while the series are on their way.
fn lines<'a>(
    prepared: Option<&'a ChartPrepared>,
    context: &PlotContext<'_>,
) -> chart_export::Lines<'a> {
    use chart_data::AxisNumbers;
    use chart_export::Axis;
    use std::borrow::Cow;
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
    let names = if aggregate == Aggregate::Count {
        "count".to_string()
    } else {
        ys.iter()
            .map(|y| modal.axis_title(y))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let y = Axis {
        title: names,
        numbers: y_numbers,
        log: modal.log_scale,
        ..Default::default()
    };
    let x_name = encoding.x.field.as_deref();
    let x = |kind| Axis {
        title: x_name.map(|x| modal.axis_title(x)).unwrap_or_default(),
        numbers: x_name
            .map(|x| AxisNumbers::column(numbers, schema, x))
            .unwrap_or_default(),
        kind,
        log: false,
    };
    let empty = |kind, x_bounds| chart_export::Lines {
        series: Cow::Borrowed(&[][..]),
        values: Cow::Borrowed(&[][..]),
        breaks: Cow::Borrowed(&[][..]),
        names: Cow::Borrowed(&[][..]),
        xs: Cow::Borrowed(&[][..]),
        bounds: None,
        other: false,
        scatter: spec.mark == Mark::Scatter,
        x_bounds,
        x: x(kind),
        y: y.clone(),
        y_from_zero: modal.y_starts_at_zero,
    };
    match prepared {
        Some(ChartPrepared::XY(cache)) => chart_export::Lines {
            series: match (modal.log_scale, &cache.series_log) {
                (false, _) => Cow::Borrowed(&cache.series[..]),
                (true, Some(logged)) => Cow::Borrowed(&logged[..]),
                (true, None) => Cow::Owned(log_series(&cache.series)),
            },
            values: Cow::Borrowed(&cache.series[..]),
            breaks: Cow::Borrowed(&cache.breaks[..]),
            names: Cow::Borrowed(&cache.names[..]),
            xs: Cow::Borrowed(&cache.xs[..]),
            // The log is monotone: the bounds of the logged points are the logged
            // bounds.
            bounds: cache.bounds.map(|[x0, x1, y0, y1]| match modal.log_scale {
                true => [x0, x1, log_y(y0), log_y(y1)],
                false => [x0, x1, y0, y1],
            }),
            other: cache.other,
            x_bounds: None,
            x: x(cache.x_axis_kind),
            ..empty(cache.x_axis_kind, None)
        },
        Some(ChartPrepared::XRange(range)) => {
            empty(range.x_axis_kind, Some((range.x_min, range.x_max)))
        }
        // Still on its way: typed from the schema so the labels are right.
        _ => {
            let kind = match (x_name, schema) {
                (Some(x), Some(schema)) => chart_data::x_axis_temporal_kind_for_column(schema, x),
                _ => chart_data::XAxisTemporalKind::Numeric,
            };
            empty(kind, None)
        }
    }
}

//! Chart preparation off the UI thread: what a spec asks for, what the worker
//! prepared, the cache of both, and chart exports written from it.

use std::path::Path;
use std::sync::Arc;

use color_eyre::Result;
use polars::prelude::{LazyFrame, Schema};

use crate::app::jobs::{Answer, ChartPrep, Job};
use crate::chart::chart_data::{self, ColorSplit, ValueRange};
use crate::chart::chart_modal::{Aggregate, ChartModal, ChartSpec, ColorCounts, Mark};
use crate::chart::chart_plot::{LinesData, PlotContext, PlotData, plot};
use crate::export::output_file::Overwrite;
use crate::{
    App, AppEvent, Overlay, analysis::sampling, chart::chart_export, export::output_file, numfmt,
};
use chart_export::{ChartExportFormat, ChartExportRequest, ExportOptions, Figure};

/// The chart view, its export form, and the preparations it keeps or waits on.
#[derive(Default)]
pub struct Charts {
    pub modal: ChartModal,
    pub export_modal: crate::chart::chart_export_modal::ChartExportModal,
    pub(crate) cache: ChartCache,
    /// The selection the chart last asked for, and, when it stepped the aggregate of
    /// the one before, until when it waits for the next step before it is prepared.
    pub(crate) asked: Option<(ChartRequest, Option<std::time::Instant>)>,
    /// A chart export that asked for data still being prepared. The preparation's end
    /// picks it up; `busy` stays set until then.
    pub(crate) export_waiting: Option<ChartExportRequest>,
}

/// Chart preparation outcomes by request, least recently used first. Failures are kept
/// so they are not retried every event. Bounded so toggling a few selections does not
/// recollect; line and scatter series (which grow with the row limit) are kept few, and
/// only the current one has its log copy.
#[derive(Default)]
pub(crate) struct ChartCache {
    pub(crate) entries: Vec<(ChartRequest, Result<PlotData, String>)>,
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

    pub(crate) fn get(&self, request: &ChartRequest) -> Option<&Result<PlotData, String>> {
        self.entries
            .iter()
            .find(|(r, _)| r == request)
            .map(|(_, outcome)| outcome)
    }

    /// The prepared data for `request`, if it has been prepared.
    pub(crate) fn prepared(&self, request: &ChartRequest) -> Option<&PlotData> {
        self.get(request).and_then(|outcome| outcome.as_ref().ok())
    }

    /// What stands in for `request` while it is prepared: the newest chart prepared of
    /// the same columns, drawn under another option (sample size, bins, range).
    pub(crate) fn standing_in(&self, request: &ChartRequest) -> Option<&PlotData> {
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

    pub(crate) fn insert(&mut self, request: ChartRequest, outcome: Result<PlotData, String>) {
        self.entries.retain(|(r, _)| *r != request);
        self.entries.push((request, outcome));
        Self::evict(&mut self.entries, Self::XY_CAPACITY, |outcome| {
            matches!(outcome, Ok(PlotData::Lines(_)))
        });
        Self::evict(&mut self.entries, Self::CAPACITY, |_| true);
    }

    /// Drop the least recently used of the entries `counts` selects until at most `cap`
    /// remain.
    fn evict(
        entries: &mut Vec<(ChartRequest, Result<PlotData, String>)>,
        cap: usize,
        counts: impl Fn(&Result<PlotData, String>) -> bool,
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

    /// Mark `request` as the selection on screen: last to be evicted, and the only one with
    /// a log copy (made here if `log_scale`). An in-memory map over held points, fine on
    /// the event thread, never in render.
    pub(crate) fn touch(&mut self, request: &ChartRequest, log_scale: bool) {
        let Some(i) = self.entries.iter().position(|(r, _)| r == request) else {
            return;
        };
        let current = self.entries.remove(i);
        self.entries.push(current);
        let last = self.entries.len() - 1;
        for (i, (_, outcome)) in self.entries.iter_mut().enumerate() {
            if let Ok(PlotData::Lines(lines)) = outcome {
                lines.keep_log(i == last && log_scale);
            }
        }
    }
}

/// What the chart view needs prepared for the panel's spec, compared with the cache and
/// the job in flight so each spec is prepared once off the UI thread. Only what the
/// chart type reads takes part (bin count does not change a line chart's request).
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
        // Leave out what this chart does not read; color splitting is read from the charted
        // spec, previewed Y included. No more series than colors to tell them apart.
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
    ) -> Result<(PlotData, Option<ColorCounts>)> {
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
                PlotData::XRange(chart_data::prepare_chart_x_range(lf, schema, x, sampling)?)
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
                let rows = grouped.rows.total_rows;
                PlotData::Lines(LinesData::new(
                    grouped,
                    (aggregate != Aggregate::None).then(|| {
                        rows_note(rows, sampling.known_total == Some(rows), left_out && !other)
                    }),
                ))
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
                PlotData::Bars(data)
            }
            Mark::Bar => PlotData::Bars(chart_data::prepare_bar_data(
                lf,
                x,
                first_y.unwrap_or_default(),
                self.order,
                chart_data::BAR_CAP,
                sampling,
            )?),
            Mark::Histogram => PlotData::Histogram(chart_data::prepare_histogram_by(
                lf, x, self.bins, self.range, self.share, split, sampling,
            )?),
            Mark::Kde => PlotData::Kde(match split {
                Some(split) => {
                    chart_data::prepare_kde_by(lf, x, self.bandwidth, self.range, split, sampling)?
                }
                None => chart_data::prepare_kde_data(lf, x, self.bandwidth, self.range, sampling)?,
            }),
            Mark::Box => {
                let y = first_y.unwrap_or_default();
                PlotData::Box(match encoding.x.field.as_deref() {
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
            Mark::Heatmap => PlotData::Heatmap(chart_data::prepare_heatmap_data(
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
    /// Whether chart data for the current view is being prepared: running, or waiting
    /// behind a superseded preparation still reading (unstoppable, see `ChartPrep`).
    pub fn chart_preparing(&self) -> bool {
        if self.chart_prep().is_some() {
            true
        } else if self.jobs.running(is_chart_prep) {
            self.chart_request_pending()
        } else {
            self.chart_settling()
        }
    }

    /// The chart preparation running whose answer is still wanted.
    fn chart_prep(&self) -> Option<&ChartPrep> {
        match self.jobs.current(is_chart_prep)? {
            (_, Job::ChartPrepare(prep)) => Some(prep),
            _ => None,
        }
    }

    /// Whether the selection on screen is a step through the aggregates still waiting
    /// for the next step.
    fn chart_settling(&self) -> bool {
        self.chart
            .asked
            .as_ref()
            .and_then(|(_, until)| *until)
            .is_some_and(|until| std::time::Instant::now() < until)
    }

    /// Whether the chart view wants data it does not have and cannot be told it will
    /// never get.
    pub(crate) fn chart_request_pending(&self) -> bool {
        if !self.overlay.shows(&Overlay::Chart) {
            return false;
        }
        ChartRequest::from_modal(&self.chart.modal)
            .is_some_and(|request| self.chart.cache.get(&request).is_none())
    }

    /// Forget chart state belonging to the outgoing view or dataset: the cache, a parked
    /// export, and a running export write (its result ignored, `busy` released). A running
    /// preparation is superseded and its answer dropped. Called when the chart view closes,
    /// the dataset changes, or home is entered.
    pub(crate) fn reset_chart_state(&mut self) {
        self.chart.cache.clear();
        self.chart.asked = None;
        if let Some(prep) = self.chart_prep() {
            prep.cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.jobs.supersede(is_chart_prep);
        // A failed export reopens its modal; it must not follow the user to the next
        // dataset.
        if self.overlay == Overlay::ChartExport {
            self.step_back();
        }
        self.chart.export_modal.close();
        let writing = self
            .jobs
            .supersede(|job| matches!(job, Job::ChartExport { .. }));
        let waiting = self.chart.export_waiting.take().is_some();
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
            .chart_prep()
            .map(|prep| prep.request.aggregates())
            .or_else(|| ChartRequest::from_modal(&self.chart.modal).map(|r| r.aggregates()))
            .unwrap_or(false);
        if !aggregating {
            return "Preparing chart...".to_string();
        }
        match self
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid())
        {
            Some(rows) => format!(
                "Grouping {} rows...",
                crate::home::discover::format_rows(rows)
            ),
            None => "Grouping every row...".to_string(),
        }
    }

    /// The series of the line or scatter chart on screen, by name, once prepared:
    /// its Y columns or its color groups.
    pub fn chart_names(&self) -> Option<Vec<String>> {
        let request = ChartRequest::from_modal(&self.chart.modal)?;
        match self.chart.cache.prepared(&request)? {
            PlotData::Lines(xy) => Some(xy.names.clone()),
            _ => None,
        }
    }

    /// True when the chart cache holds the data for the modal's current selection.
    pub fn chart_data_ready(&self) -> bool {
        ChartRequest::from_modal(&self.chart.modal).is_some_and(|r| self.chart.cache.satisfies(&r))
    }

    /// Start preparing the modal's current chart unless cached, known to fail, or another
    /// preparation is running (the newest selection follows when it lands). Runs after every
    /// event, so render only draws.
    pub(crate) fn ensure_chart_data(&mut self) {
        const CHART_AGGREGATE_SETTLE: std::time::Duration = std::time::Duration::from_millis(150);
        if !self.overlay.shows(&Overlay::Chart) {
            return;
        }
        // What Every row costs, as the table counted it.
        self.chart.modal.view_rows = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.num_rows_if_valid());
        let request = ChartRequest::from_modal(&self.chart.modal);
        if let Some(prep) = self.chart_prep()
            && !request.as_ref().is_some_and(|r| r.reads_as(&prep.request))
        {
            // A count streaming a large view for a selection the cursor has moved
            // past would hold up the next chart for as long as it reads.
            prep.cancel
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let Some(request) = request else {
            return;
        };
        // Stepping none, count, distinct, sum, mean grouped every row at each step, and
        // drew each: a step waits a moment for the next, and only where it stops is
        // prepared. A Wake when the wait ends prepares it.
        let settle = match self.chart.asked.take() {
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
        self.chart.asked = Some((request.clone(), settle));
        if self.chart.cache.get(&request).is_some() {
            self.chart.cache.touch(&request, self.chart.modal.log_scale);
            // A cached chart's colors were counted with it.
            if let Some(colors) = request
                .spec
                .encoding
                .color
                .field
                .as_deref()
                .and_then(|c| self.chart.cache.colors(c))
            {
                self.chart.modal.color_counts = Some(colors.clone());
            }
            return;
        }
        if self.jobs.running(is_chart_prep) || self.chart_settling() {
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
            held: self.chart.cache.held_rows(dataset),
            cancel: Arc::default(),
        };
        let prep = ChartPrep {
            dataset,
            request: request.clone(),
            cancel: Arc::clone(&sampling.cancel),
        };
        self.spawn_job(Job::ChartPrepare(Box::new(prep)), None, move |_| {
            request
                .prepare(&lf, &schema, &sampling)
                .map(|prepared| Answer::ChartPrepared(Box::new(prepared)))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None))
        });
    }

    /// A chart preparation ended with `outcome`. It is installed only while the job is
    /// `current` (a reset supersedes it when its view or dataset goes) and only into
    /// the dataset it was read from. Its ending is what lets the next start.
    pub(crate) fn chart_prepared(
        &mut self,
        prep: ChartPrep,
        current: bool,
        outcome: Result<(PlotData, Option<ColorCounts>), String>,
    ) {
        if !current {
            return;
        }
        // A count stopped part way is no answer, and must not be remembered as a
        // failure; the selection is prepared again when it comes back.
        if outcome.is_err() && prep.cancel.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let dataset = self.data_table_state.as_ref().map(|s| s.len_generation());
        if dataset != prep.dataset {
            return;
        }
        let outcome = outcome.map(|(prepared, colors)| {
            if let Some(colors) = colors {
                self.chart.cache.hold_colors(colors.clone());
                self.chart.modal.color_counts = Some(colors);
            }
            prepared
        });
        self.chart.cache.insert(prep.request, outcome);
        // A parked export resumes against the current selection: written if prepared, failed
        // if that one failed, else waiting for the next result.
        if let Some(request) = self.chart.export_waiting.take() {
            self.start_chart_export(request);
        }
    }

    /// The figure to export from the prepared chart for the current spec. `Ok(None)`
    /// means that chart is still being prepared and the caller should wait for it.
    pub(crate) fn build_chart_figure(&self) -> Result<Option<Figure>> {
        let Some(state) = self.data_table_state.as_ref() else {
            return Err(color_eyre::eyre::eyre!("No data loaded"));
        };
        let modal = &self.chart.modal;
        let Some(request) = ChartRequest::from_modal(modal).filter(|r| !r.x_only) else {
            return Err(color_eyre::eyre::eyre!(
                "Pick the columns the chart needs first"
            ));
        };
        let prepared = match self.chart.cache.get(&request) {
            Some(Ok(prepared)) => prepared,
            // A selection known not to chart is never retried, so waiting for its data
            // would wait forever: fail the export now with the reason.
            Some(Err(message)) => return Err(color_eyre::eyre::eyre!("{}", message)),
            None => return Ok(None),
        };
        let context = PlotContext {
            modal,
            spec: &request.spec,
            numbers: &self.display.number_format,
            schema: Some(state.schema().as_ref()),
        };
        // A single X column has nothing to export.
        let plot = plot(Some(prepared), &context)
            .filter(|plot| !plot.data.is_empty())
            .ok_or_else(|| color_eyre::eyre::eyre!("No valid data points to export"))?;
        let mut plot = plot.into_owned();
        // The screen's title row says how the rows were made; a file has none, so
        // its Y axis names the aggregate.
        let y = &request.spec.encoding.y;
        if matches!(*plot.data, PlotData::Lines(_))
            && !matches!(y.aggregate, Aggregate::None | Aggregate::Count)
        {
            plot.y.title = format!("{} {}", y.aggregate_name(), plot.y.title);
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
    pub(crate) fn start_chart_export(&mut self, mut request: ChartExportRequest) {
        // How the chart was made, from the view and chart as they are now; none
        // when the dialog says Omit.
        request.options.recipe = if request.recipe {
            self.chart_recipe()
        } else {
            None
        };
        match self.build_chart_figure() {
            Ok(Some(figure)) => {
                self.chart.export_waiting = None;
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
            // Still being prepared; its job's end comes back here.
            Ok(None) => self.chart.export_waiting = Some(request),
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
        self.chart.export_waiting = None;
        self.export_progress = None;
        self.status_message = None;
        self.busy = false;
        match result {
            Ok(()) => {
                self.flash_path("Chart exported to ", path);
                self.chart.export_modal.close();
            }
            // The form comes back as it was, the reason on its status line.
            Err(message) => {
                self.chart.export_modal.reopen_with_path(path, format);
                self.chart.export_modal.error = Some(message);
                if self.overlay == Overlay::Chart {
                    self.open_overlay(Overlay::ChartExport);
                }
            }
        }
    }

    /// What a chart says under its plot about the rows it drew. A view with a sample
    /// is charted from all of it: the note says which sample, with its seed, so the
    /// chart can be drawn again.
    pub(crate) fn chart_notes_of(
        &self,
        prepared: &crate::chart::chart_jobs::PlotData,
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

/// A chart preparation's job.
fn is_chart_prep(job: &Job) -> bool {
    matches!(job, Job::ChartPrepare(_))
}

//! Chart preparation off the UI thread: what a spec asks for, what the worker
//! prepared, the cache of both, and chart exports written from it.

use std::path::Path;
use std::sync::{Arc, Mutex};

use color_eyre::Result;
use polars::prelude::{LazyFrame, Schema};

use crate::chart_data::{self, ColorSplit, ValueRange};
use crate::chart_export::{ChartExportFormat, ExportOptions, Figure};
use crate::chart_modal::{Aggregate, ChartModal, ChartSpec, ColorCounts, Mark};
use crate::output_file::Overwrite;
use crate::{numfmt, output_file};

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

pub(crate) fn log_series(series: &[Vec<(f64, f64)>]) -> Vec<Vec<(f64, f64)>> {
    series
        .iter()
        .map(|pts| pts.iter().map(|&(x, y)| (x, y.max(0.0).ln_1p())).collect())
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
                    x_column: x.to_string(),
                    names: grouped.names,
                    series: grouped.series,
                    breaks: grouped.breaks,
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
                None => {
                    chart_data::prepare_kde_data(lf, &[x], self.bandwidth, self.range, sampling)?
                }
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
                    None => chart_data::prepare_box_plot_data(lf, &[y], self.range, sampling)?,
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
    /// What the chart says under the plot about the rows and values it drew.
    pub(crate) fn notes(&self) -> Vec<String> {
        let rows_of = |rows: usize| crate::discover::format_rows(rows);
        match self {
            Self::Bar(d) => {
                let mut notes = chart_data::chart_notes(&d.rows, None);
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
            Self::XY(c) => chart_data::chart_notes(&c.rows, None),
            Self::XRange(c) => chart_data::chart_notes(&c.rows, None),
            Self::Histogram(d) => chart_data::chart_notes(&d.rows, d.clipped.as_ref()),
            Self::BoxPlot(d) => {
                let mut notes = chart_data::chart_notes(&d.rows, d.clipped.as_ref());
                if d.of > 0 {
                    notes.push(format!(
                        "the {} largest of {} categories",
                        d.stats.len(),
                        numfmt::group_chrome(d.of)
                    ));
                }
                notes
            }
            Self::Kde(d) => chart_data::chart_notes(&d.rows, d.clipped.as_ref()),
            Self::Heatmap(d) => chart_data::chart_notes(&d.rows, None),
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
        let bytes = crate::chart_export::render(&self.figure, &self.options, format)?;
        let out = output_file::OutputFile::create(path, overwrite)?;
        std::fs::write(out.path(), bytes)?;
        out.commit()?;
        Ok(())
    }
}

pub(crate) struct ChartCacheXY {
    pub(crate) x_column: String,
    /// One per series: its Y column, or its color group.
    pub(crate) names: Vec<String>,
    pub(crate) series: Vec<Vec<(f64, f64)>>,
    /// Where each series' line starts again after a null (see `chart_data::segments`).
    pub(crate) breaks: Vec<Vec<usize>>,
    pub(crate) series_log: Option<Vec<Vec<(f64, f64)>>>,
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

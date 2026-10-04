//! Chart preparation off the UI thread: what a selection asks for, what the worker
//! prepared, the cache of both, and chart exports written from it.

use std::path::Path;
use std::sync::{Arc, Mutex};

use color_eyre::Result;
use polars::prelude::{LazyFrame, Schema};

use crate::chart_export::{
    BoxPlotExportBounds, ChartExportBounds, ChartExportFormat, ChartExportSeries, write_bar_eps,
    write_bar_png, write_box_plot_eps, write_box_plot_png, write_chart_eps, write_chart_png,
    write_heatmap_eps, write_heatmap_png,
};
use crate::chart_modal::{ChartKind, ChartModal, ChartType};
use crate::output_file::Overwrite;
use crate::{chart_data, numfmt, output_file};

/// Outcomes of chart preparation keyed by the request that produced them, least
/// recently used first. A failure is remembered too, so a selection that cannot be
/// charted is not retried after every event; the chart shows its message.
/// Bounded so that toggling between a few selections does not collect again, without
/// holding every series ever prepared: XY series are the only payload that grows with
/// the row limit, so few of those are kept and only the current one has its log copy.
#[derive(Default)]
pub(crate) struct ChartCache {
    pub(crate) entries: Vec<(ChartRequest, Result<ChartPrepared, String>)>,
    /// The rows read for these entries and which view (`len_generation`) they are
    /// from, so another option re-draws from them rather than reading again.
    held: chart_data::HeldRows,
    held_view: Option<u64>,
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
    /// where eviction reaches it last, and it alone keeps a log-scale copy of its XY
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

/// What the chart view needs prepared for the modal's current selection. Compared with
/// the cache and with the computation in flight, so each selection is prepared once, off
/// the UI thread, and a result for a selection the user has since moved past is stale.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ChartRequest {
    XY {
        x_column: String,
        y_columns: Vec<String>,
        row_limit: Option<usize>,
        /// Draw each step's lowest and highest value: see
        /// [`chart_data::prepare_chart_data`].
        envelope: bool,
    },
    /// Only an x column is selected: its range gives the placeholder axis its bounds.
    XRange {
        x_column: String,
        row_limit: Option<usize>,
    },
    Histogram {
        column: String,
        bins: usize,
        range: chart_data::ValueRange,
        row_limit: Option<usize>,
    },
    BoxPlot {
        column: String,
        range: chart_data::ValueRange,
        row_limit: Option<usize>,
    },
    Kde {
        column: String,
        bandwidth_factor: f64,
        range: chart_data::ValueRange,
        row_limit: Option<usize>,
    },
    Heatmap {
        x_column: String,
        y_column: String,
        bins: usize,
        row_limit: Option<usize>,
    },
    Bar {
        category: String,
        value: chart_data::BarValue,
        order: chart_data::BarOrder,
        row_limit: Option<usize>,
    },
}

impl ChartRequest {
    /// Whether preparing `other` reads what this needs. A count is of the whole view,
    /// so another order or sample size of the same category is the same pass.
    pub(crate) fn reads_as(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Bar {
                    category: a,
                    value: chart_data::BarValue::Count,
                    ..
                },
                Self::Bar {
                    category: b,
                    value: chart_data::BarValue::Count,
                    ..
                },
            ) => a == b,
            _ => self == other,
        }
    }

    /// Whether `other` is the same kind of chart of the same columns, whatever its
    /// options.
    pub(crate) fn same_columns(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::XY {
                    x_column: a,
                    y_columns: ay,
                    ..
                },
                Self::XY {
                    x_column: b,
                    y_columns: by,
                    ..
                },
            ) => a == b && ay == by,
            (Self::XRange { x_column: a, .. }, Self::XRange { x_column: b, .. })
            | (Self::Histogram { column: a, .. }, Self::Histogram { column: b, .. })
            | (Self::BoxPlot { column: a, .. }, Self::BoxPlot { column: b, .. })
            | (Self::Kde { column: a, .. }, Self::Kde { column: b, .. }) => a == b,
            (
                Self::Heatmap {
                    x_column: a,
                    y_column: ay,
                    ..
                },
                Self::Heatmap {
                    x_column: b,
                    y_column: by,
                    ..
                },
            ) => a == b && ay == by,
            (
                Self::Bar {
                    category: a,
                    value: av,
                    ..
                },
                Self::Bar {
                    category: b,
                    value: bv,
                    ..
                },
            ) => a == b && av == bv,
            _ => false,
        }
    }

    pub(crate) fn from_modal(modal: &ChartModal) -> Option<Self> {
        let row_limit = modal.row_limit;
        let range = modal.value_range;
        match modal.chart_kind {
            ChartKind::XY => {
                let x_column = modal.effective_x_column()?.clone();
                let y_columns = modal.effective_y_columns();
                Some(if y_columns.is_empty() {
                    Self::XRange {
                        x_column,
                        row_limit,
                    }
                } else {
                    Self::XY {
                        x_column,
                        y_columns,
                        row_limit,
                        // A line is drawn from each step's lowest and highest value
                        // rather than a sample; scatter and bar keep the sample.
                        envelope: modal.chart_type == ChartType::Line,
                    }
                })
            }
            ChartKind::Histogram => Some(Self::Histogram {
                column: modal.effective_hist_column()?,
                bins: modal.hist_bins,
                range,
                row_limit,
            }),
            ChartKind::BoxPlot => Some(Self::BoxPlot {
                column: modal.effective_box_column()?,
                range,
                row_limit,
            }),
            ChartKind::Kde => Some(Self::Kde {
                column: modal.effective_kde_column()?,
                bandwidth_factor: modal.kde_bandwidth_factor,
                range,
                row_limit,
            }),
            ChartKind::Heatmap => Some(Self::Heatmap {
                x_column: modal.effective_heatmap_x_column()?,
                y_column: modal.effective_heatmap_y_column()?,
                bins: modal.heatmap_bins,
                row_limit,
            }),
            ChartKind::Bar => Some(Self::Bar {
                category: modal.effective_bar_category()?,
                value: modal.effective_bar_value()?,
                order: modal.bar_order,
                row_limit,
            }),
        }
    }

    /// The Polars work. Runs on a worker thread.
    pub(crate) fn prepare(
        &self,
        lf: &LazyFrame,
        schema: &Schema,
        sampling: &chart_data::ChartSampling,
    ) -> Result<ChartPrepared> {
        Ok(match self {
            Self::XY {
                x_column,
                y_columns,
                envelope,
                ..
            } => {
                let r = chart_data::prepare_chart_data(
                    lf, schema, x_column, y_columns, sampling, *envelope,
                )?;
                ChartPrepared::XY(ChartCacheXY {
                    x_column: x_column.clone(),
                    y_columns: y_columns.clone(),
                    series: r.series,
                    breaks: r.breaks,
                    series_log: None,
                    x_axis_kind: r.x_axis_kind,
                    rows: r.rows,
                })
            }
            Self::XRange { x_column, .. } => ChartPrepared::XRange(
                chart_data::prepare_chart_x_range(lf, schema, x_column, sampling)?,
            ),
            Self::Histogram {
                column,
                bins,
                range,
                ..
            } => ChartPrepared::Histogram(chart_data::prepare_histogram_data(
                lf, column, *bins, *range, sampling,
            )?),
            Self::BoxPlot { column, range, .. } => {
                ChartPrepared::BoxPlot(chart_data::prepare_box_plot_data(
                    lf,
                    std::slice::from_ref(column),
                    *range,
                    sampling,
                )?)
            }
            Self::Kde {
                column,
                bandwidth_factor,
                range,
                ..
            } => ChartPrepared::Kde(chart_data::prepare_kde_data(
                lf,
                std::slice::from_ref(column),
                *bandwidth_factor,
                *range,
                sampling,
            )?),
            Self::Heatmap {
                x_column,
                y_column,
                bins,
                ..
            } => ChartPrepared::Heatmap(chart_data::prepare_heatmap_data(
                lf, x_column, y_column, *bins, sampling,
            )?),
            Self::Bar {
                category,
                value,
                order,
                ..
            } => ChartPrepared::Bar(match value {
                chart_data::BarValue::Count => chart_data::prepare_bar_counts(
                    lf,
                    category,
                    *order,
                    chart_data::BAR_CAP,
                    sampling,
                )?,
                chart_data::BarValue::Column(value) => chart_data::prepare_bar_data(
                    lf,
                    category,
                    value,
                    *order,
                    chart_data::BAR_CAP,
                    sampling,
                )?,
            }),
        })
    }
}

/// The outcome handed from the chart worker to `BackgroundChartReady`.
pub(crate) type ChartResultSlot = Arc<Mutex<Option<Result<ChartPrepared, String>>>>;

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
    /// Set when the selection moves past the request or its view goes. A streamed count
    /// stops at its next batch; every other read is bounded and runs to the end.
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
        if let Self::Bar(d) = self {
            let mut notes = chart_data::chart_notes(&d.rows, None);
            if let Some(rows) = d.counted {
                notes.push(format!(
                    "counts of {} rows",
                    crate::discover::format_rows(rows)
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
            return notes;
        }
        let (rows, clipped) = match self {
            Self::XY(c) => (&c.rows, None),
            Self::XRange(c) => (&c.rows, None),
            Self::Histogram(d) => (&d.rows, d.clipped.as_ref()),
            Self::BoxPlot(d) => (&d.rows, d.clipped.as_ref()),
            Self::Kde(d) => (&d.rows, d.clipped.as_ref()),
            Self::Heatmap(d) => (&d.rows, None),
            Self::Bar(d) => (&d.rows, None),
        };
        chart_data::chart_notes(rows, clipped)
    }
}

/// A chart export with its data taken from the cache; `write` is the slow part and runs
/// off the UI thread.
pub(crate) enum ChartExportJob {
    Series {
        series: Vec<ChartExportSeries>,
        chart_type: ChartType,
        bounds: ChartExportBounds,
    },
    BoxPlot {
        data: chart_data::BoxPlotData,
        bounds: BoxPlotExportBounds,
    },
    Heatmap {
        data: chart_data::HeatmapData,
        bounds: ChartExportBounds,
    },
    Bar {
        data: chart_data::BarData,
        /// The value column's format in the table, for the values and the axis.
        format: crate::numfmt::NumberFormat,
        title: Option<String>,
        notes: Vec<String>,
    },
}

impl ChartExportJob {
    /// Draw the chart into a temporary file and commit it over `path`.
    pub(crate) fn write(
        &self,
        path: &Path,
        format: ChartExportFormat,
        size: (u32, u32),
        overwrite: Overwrite,
    ) -> Result<()> {
        let out = output_file::OutputFile::create(path, overwrite)?;
        self.draw(out.path(), format, size)?;
        out.commit()?;
        Ok(())
    }

    /// The writers open the path themselves: plotters picks PNG from its extension.
    fn draw(&self, path: &Path, format: ChartExportFormat, size: (u32, u32)) -> Result<()> {
        match (self, format) {
            (
                Self::Series {
                    series,
                    chart_type,
                    bounds,
                },
                ChartExportFormat::Png,
            ) => write_chart_png(path, series, *chart_type, bounds, size),
            (
                Self::Series {
                    series,
                    chart_type,
                    bounds,
                },
                ChartExportFormat::Eps,
            ) => write_chart_eps(path, series, *chart_type, bounds),
            (Self::BoxPlot { data, bounds }, ChartExportFormat::Png) => {
                write_box_plot_png(path, data, bounds, size)
            }
            (Self::BoxPlot { data, bounds }, ChartExportFormat::Eps) => {
                write_box_plot_eps(path, data, bounds)
            }
            (Self::Heatmap { data, bounds }, ChartExportFormat::Png) => {
                write_heatmap_png(path, data, bounds, size)
            }
            (Self::Heatmap { data, bounds }, ChartExportFormat::Eps) => {
                write_heatmap_eps(path, data, bounds)
            }
            (
                Self::Bar {
                    data,
                    format,
                    title,
                    notes,
                },
                ChartExportFormat::Png,
            ) => write_bar_png(path, data, format, title.as_deref(), notes, size),
            (
                Self::Bar {
                    data,
                    format,
                    title,
                    notes,
                },
                ChartExportFormat::Eps,
            ) => write_bar_eps(path, data, format, title.as_deref(), notes),
        }
    }
}

pub(crate) struct ChartCacheXY {
    /// Axis labels; one series per y column.
    pub(crate) x_column: String,
    pub(crate) y_columns: Vec<String>,
    pub(crate) series: Vec<Vec<(f64, f64)>>,
    /// Where each series' line starts again after a null (see `chart_data::segments`).
    pub(crate) breaks: Vec<Vec<usize>>,
    pub(crate) series_log: Option<Vec<Vec<(f64, f64)>>>,
    pub(crate) x_axis_kind: chart_data::XAxisTemporalKind,
    pub(crate) rows: chart_data::RowsRead,
}

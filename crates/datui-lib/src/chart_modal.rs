//! Chart view state: the chart's spec and the panel that edits it.
//!
//! The spec is a set of shelves, named as Vega-Lite names them so a later version
//! can save it: `mark` (the Type shelf), `encoding.x.field` with its `timeUnit`,
//! `encoding.y.field` with its `aggregate`, and `encoding.color.field`. Every chart
//! type shows the same shelves; a shelf a type does not use is dimmed, never
//! hidden (`ChartModal::shelf`). Options that are not part of what is charted
//! (bins, ranges, the grid, the sample size) sit beside the spec.
//!
//! The panel is one form of the shared focus model (`crate::form`); column and
//! value rows are edited through the one shared Picker.

use crate::chart_data::{BarOrder, ValueRange};
use crate::widgets::ui::PickerState;
use polars::prelude::DataType;
use serde::{Deserialize, Serialize};

/// The Type shelf: what marks the chart draws.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mark {
    #[default]
    Line,
    Scatter,
    Bar,
    Histogram,
    Box,
    Kde,
    Heatmap,
}

impl Mark {
    pub const ALL: [Self; 7] = [
        Self::Line,
        Self::Scatter,
        Self::Bar,
        Self::Histogram,
        Self::Box,
        Self::Kde,
        Self::Heatmap,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Line => "Line",
            Self::Scatter => "Scatter",
            Self::Bar => "Bar",
            Self::Histogram => "Histogram",
            Self::Box => "Box",
            Self::Kde => "KDE",
            Self::Heatmap => "Heatmap",
        }
    }

    /// The Vega-Lite mark that draws it.
    pub fn vega_lite(self) -> &'static str {
        match self {
            Self::Line | Self::Kde => "line",
            Self::Scatter => "point",
            Self::Bar | Self::Histogram => "bar",
            Self::Box => "boxplot",
            Self::Heatmap => "rect",
        }
    }

    /// Line and scatter: X against one or more Y columns.
    pub fn is_xy(self) -> bool {
        matches!(self, Self::Line | Self::Scatter)
    }
}

/// How a temporal X is bucketed before Y is aggregated.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    #[default]
    None,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl TimeUnit {
    pub const ALL: [Self; 6] = [
        Self::None,
        Self::Day,
        Self::Week,
        Self::Month,
        Self::Quarter,
        Self::Year,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    /// The Polars duration a date is truncated to.
    pub fn every(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Day => Some("1d"),
            Self::Week => Some("1w"),
            Self::Month => Some("1mo"),
            Self::Quarter => Some("1q"),
            Self::Year => Some("1y"),
        }
    }

    /// The Vega-Lite `timeUnit`.
    pub fn vega_lite(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Day => Some("yearmonthdate"),
            Self::Week => Some("yearweek"),
            Self::Month => Some("yearmonth"),
            Self::Quarter => Some("yearquarter"),
            Self::Year => Some("year"),
        }
    }
}

/// What a Y value is, per X (and color): the column as it is, or an aggregate of
/// the rows that share it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Aggregate {
    #[default]
    None,
    Count,
    /// The distinct values of Y: `nunique` of the query language, nulls left out.
    Distinct,
    Sum,
    Mean,
    Median,
    /// The sample standard deviation (one degree of freedom); a group of one row
    /// has none.
    Stdev,
    /// A percentile of Y, linearly interpolated: [`YEncoding::quantile`].
    Quantile,
    Min,
    Max,
    /// The first and last Y of each group in the view's row order: its sort, or the
    /// order the rows were read in.
    First,
    Last,
}

impl Aggregate {
    pub const ALL: [Self; 12] = [
        Self::None,
        Self::Count,
        Self::Distinct,
        Self::Sum,
        Self::Mean,
        Self::Median,
        Self::Stdev,
        Self::Quantile,
        Self::Min,
        Self::Max,
        Self::First,
        Self::Last,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Count => "count",
            Self::Distinct => "distinct",
            Self::Sum => "sum",
            Self::Mean => "mean",
            Self::Median => "median",
            Self::Stdev => "stdev",
            Self::Quantile => "quantile",
            Self::Min => "min",
            Self::Max => "max",
            Self::First => "first",
            Self::Last => "last",
        }
    }

    /// How it reads in a title or a column name: a quantile as its percentile,
    /// `p90`.
    pub fn named(self, quantile: u8) -> String {
        match self {
            Self::Quantile => format!("p{quantile}"),
            other => other.label().to_string(),
        }
    }

    /// The Vega-Lite aggregate. A quantile is one only at the quartiles and the
    /// median; first and last have none, and are left out.
    pub fn vega_lite(self, quantile: u8) -> Option<&'static str> {
        match self {
            Self::None | Self::First | Self::Last => None,
            Self::Quantile => match quantile {
                25 => Some("q1"),
                50 => Some("median"),
                75 => Some("q3"),
                _ => None,
            },
            other => Some(other.label()),
        }
    }

    /// Whether it reads the rows in the view's order.
    pub fn follows_row_order(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }

    /// Whether every value it makes may have a fraction, whatever Y is.
    pub fn is_fractional(self) -> bool {
        matches!(
            self,
            Self::Mean | Self::Median | Self::Stdev | Self::Quantile
        )
    }

    /// Whether Y may be any column, not only a number: a count of its distinct
    /// values is a number whatever they are.
    pub fn takes_any_y(self) -> bool {
        self == Self::Distinct
    }

    /// Whether cumulative can run with it: the rows run as a total, which for a
    /// count, sum, mean, median, min or max is what was asked. A running sum of
    /// distinct counts is not the distinct count so far, nor one of deviations,
    /// percentiles or first and last values anything: those take none.
    pub fn runs_cumulative(self) -> bool {
        matches!(
            self,
            Self::Count | Self::Sum | Self::Mean | Self::Median | Self::Min | Self::Max
        )
    }

    /// Whether every value it makes is a whole number, whatever Y is.
    pub fn is_count(self) -> bool {
        matches!(self, Self::Count | Self::Distinct)
    }
}

/// A running total of the aggregated Y, along X.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cumulative {
    #[default]
    Off,
    /// The values added up.
    Sum,
    /// Returns compounded: each value is a rate, and the line is what 1 grew to,
    /// less 1 (`(1 + a)(1 + b)... - 1`).
    Compound,
}

impl Cumulative {
    pub const ALL: [Self; 3] = [Self::Off, Self::Sum, Self::Compound];

    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Sum => "running sum",
            Self::Compound => "compound",
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XEncoding {
    pub field: Option<String>,
    pub time_unit: TimeUnit,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct YEncoding {
    /// One column, or several on a line or scatter chart (one series each).
    pub field: Vec<String>,
    pub aggregate: Aggregate,
    pub cumulative: Cumulative,
    /// The percentile a quantile takes; unset, [`QUANTILE_DEFAULT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentile: Option<u8>,
}

/// The percentiles the quantile steps through.
pub const QUANTILES: [u8; 8] = [1, 5, 10, 25, 75, 90, 95, 99];

/// The percentile a quantile starts at.
pub const QUANTILE_DEFAULT: u8 = 90;

impl YEncoding {
    /// The percentile a quantile takes.
    pub fn quantile(&self) -> u8 {
        self.percentile.unwrap_or(QUANTILE_DEFAULT)
    }

    /// The aggregate as a title or a column name says it: `mean`, `p90`.
    pub fn aggregate_name(&self) -> String {
        self.aggregate.named(self.quantile())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColorEncoding {
    pub field: Option<String>,
    /// The values given a series each, in color order. Empty: the largest
    /// [`COLOR_MAX`] by rows. `None` is the rows with no value.
    pub values: Vec<Option<String>>,
    /// Whether every other value's rows make one more series, Other. Unset: on for a
    /// scatter, off otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other: Option<bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Encoding {
    pub x: XEncoding,
    pub y: YEncoding,
    pub color: ColorEncoding,
}

/// What is charted.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChartSpec {
    pub mark: Mark,
    pub encoding: Encoding,
}

impl ChartSpec {
    /// The spec as a Vega-Lite fragment: `mark` and the three encodings.
    pub fn to_vega_lite(&self) -> serde_json::Value {
        let mut x = serde_json::Map::new();
        if let Some(field) = &self.encoding.x.field {
            x.insert("field".into(), field.clone().into());
        }
        if let Some(unit) = self.encoding.x.time_unit.vega_lite() {
            x.insert("timeUnit".into(), unit.into());
        }
        let mut y = serde_json::Map::new();
        if let Some(field) = self.encoding.y.field.first() {
            y.insert("field".into(), field.clone().into());
        }
        if let Some(aggregate) = self
            .encoding
            .y
            .aggregate
            .vega_lite(self.encoding.y.quantile())
        {
            y.insert("aggregate".into(), aggregate.into());
        }
        let mut encoding = serde_json::Map::new();
        encoding.insert("x".into(), x.into());
        encoding.insert("y".into(), y.into());
        if let Some(field) = &self.encoding.color.field {
            encoding.insert("color".into(), serde_json::json!({ "field": field }));
        }
        serde_json::json!({ "mark": self.mark.vega_lite(), "encoding": encoding })
    }
}

/// Series a color splits a chart into, at most: one per palette color
/// (`chart_1` to `chart_10`). A terminal of fewer colors draws fewer:
/// [`ChartModal::series_max`].
pub const COLOR_MAX: usize = 10;

/// Most Y columns a line or scatter chart draws at once.
pub const Y_SERIES_MAX: usize = COLOR_MAX;

/// Default histogram bin count.
pub const HISTOGRAM_DEFAULT_BINS: usize = 40;
pub const HISTOGRAM_MIN_BINS: usize = 5;
pub const HISTOGRAM_MAX_BINS: usize = 100;

/// Default heatmap bin count (applies to both axes).
pub const HEATMAP_DEFAULT_BINS: usize = 20;
pub const HEATMAP_MIN_BINS: usize = 5;
pub const HEATMAP_MAX_BINS: usize = 60;

/// KDE bandwidth multiplier bounds and step.
pub const KDE_BANDWIDTH_MIN: f64 = 0.2;
pub const KDE_BANDWIDTH_MAX: f64 = 5.0;
pub const KDE_BANDWIDTH_STEP: f64 = 0.1;

/// The largest sample size (Polars slice takes u32).
pub const CHART_ROW_LIMIT_MAX: usize = u32::MAX as usize;

/// A change to the Rows row not yet read: ←/→ or a typed size waits for Enter, or
/// for focus to leave the row, so the chart reads once rather than per key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowsDraft {
    /// Every row rather than a sample.
    pub every: bool,
    /// The sample size being typed, as typed (`50k`).
    pub typed: Option<String>,
    /// Why the typed size cannot be read, until it is edited.
    pub error: Option<&'static str>,
}

/// One row of the panel: a shelf, the line under it, or an option.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartFocus {
    /// The Type shelf: ←/→ step the chart type.
    #[default]
    Type,
    /// The X shelf's column.
    X,
    /// Under X: the time bucket of a temporal X.
    TimeUnit,
    /// Under X on a bar chart: bars by value or by label.
    Order,
    /// Under X on a histogram; an option on a heatmap.
    Bins,
    /// The Y shelf's column (or columns); on a histogram, count or share.
    Y,
    /// Under Y: the aggregate.
    Aggregate,
    /// Under the aggregate, a quantile's percentile.
    Quantile,
    /// The Color shelf's column.
    Color,
    /// Under Color: which values get a series.
    ColorValues,
    /// Options.
    Cumulative,
    Bandwidth,
    Range,
    YStartsAtZero,
    LogScale,
    ShowLegend,
    Grid,
    /// Sample size, for charts that sample.
    LimitRows,
}

/// Whether a shelf takes part in the chart on screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShelfUse {
    /// Edited and charted.
    Used,
    /// Shown dimmed with why: `density`, `same as X`.
    Dimmed(&'static str),
}

/// The view's columns a chart can take, by role.
#[derive(Clone, Copy, Default)]
pub struct ChartColumns<'a> {
    pub numeric: &'a [String],
    pub datetime: &'a [String],
    /// Dates and datetimes, which a time bucket truncates (not times of day).
    pub bucketable: &'a [String],
    /// Categories: text, categorical, boolean and integer columns
    /// (see `chart_data::is_category_dtype`).
    pub category: &'a [String],
}

/// The values of the Color column, most rows first, as the chart's last count
/// found them: what the value picker lists.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColorCounts {
    pub column: String,
    pub values: Vec<(Option<String>, u64)>,
}

/// What the open Picker edits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerFor {
    X,
    Y,
    Color,
    ColorValues,
}

/// The item a Picker offers for "nothing": no Color, no category on a box plot.
pub const NONE_ITEM: &str = "none";

/// Chart view state: the spec, the options, and the panel's focus.
#[derive(Default)]
pub struct ChartModal {
    pub spec: ChartSpec,
    /// The distinct colors the series slots come out as on this terminal
    /// (`Theme::series_colors`); `None` before the app says, read as [`COLOR_MAX`].
    pub series_cap: Option<usize>,
    /// The view's sort as the footer writes it (`time ▲`), which first and last
    /// read the rows in; `None` for the order they were read in.
    pub row_order: Option<String>,
    /// The cursor column's type when `c` chose the chart (`f64`), shown under Type
    /// until the type is changed.
    pub suggested: Option<String>,
    pub y_starts_at_zero: bool,
    pub log_scale: bool,
    pub show_legend: bool,
    /// The grid at the major ticks, on the kinds with axes (`g`).
    pub grid: bool,
    /// Histogram: each bin's share of its group's rows rather than its count.
    pub share: bool,
    pub hist_bins: usize,
    pub heatmap_bins: usize,
    pub kde_bandwidth_factor: f64,
    /// Histogram, Box and KDE: which values are drawn.
    pub value_range: ValueRange,
    pub bar_order: BarOrder,
    /// Rows a chart that samples reads: up to this many, spread across the table.
    /// None = every row. What the chart reads; the Rows row edits `rows_draft`.
    pub row_limit: Option<usize>,
    /// The sample size Sample returns to, kept while Every row is chosen.
    pub sample_rows: usize,
    /// The Rows row's pending change, if any.
    pub rows_draft: Option<RowsDraft>,
    /// The view's row count when the table knows it, for Every row's cost.
    pub view_rows: Option<usize>,
    /// The view is a sample, held in memory: the chart reads all of it, and has no
    /// Rows row of its own.
    pub view_sampled: bool,
    /// A saved view put its chart here: `c` brings it back, whichever column the
    /// cursor is on.
    pub restored: bool,
    pub focus: ChartFocus,
    /// The one Picker, open for the focused row; None while the form has the keys.
    pub picker: Option<PickerState>,
    pub picker_for: Option<PickerFor>,
    /// Per item of the value picker: its rows, shown beside it.
    pub picker_details: Vec<String>,
    pub numeric_candidates: Vec<String>,
    pub temporal_candidates: Vec<String>,
    /// Dates and datetimes: the X columns a time bucket truncates.
    pub bucketable_candidates: Vec<String>,
    pub category_candidates: Vec<String>,
    /// The Color column's values by rows, once a chart counted them.
    pub color_counts: Option<ColorCounts>,
    /// The dataset the choices were made on (`App::dataset_generation`), and the
    /// column the table's cursor was on: `c` again from the same column on the
    /// same dataset brings the chart back as it was left.
    pub dataset: Option<u64>,
    pub opened_on: Option<String>,
    /// Each column's unit, from a delimited spec's unit row: the axis titles name
    /// them. Set as the chart is drawn.
    pub units: Vec<(String, String)>,
    /// The plot has the keys rather than the panel (`x`): ←→ move the crosshair.
    pub plot_focus: bool,
    /// The x of the point the crosshair stands on, kept while the panel has the
    /// keys so it comes back where it was.
    pub cursor_x: Option<f64>,
    /// Where the line or scatter plot was last drawn, for the crosshair and a
    /// click; `None` when it has no points on screen.
    pub plot: Option<crate::widgets::crosshair::PlotPlace>,
}

impl ChartModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Most series a chart draws: one per distinct series color, up to [`COLOR_MAX`].
    /// A 16-color terminal draws fewer rather than two in one color.
    pub fn series_max(&self) -> usize {
        self.series_cap.unwrap_or(COLOR_MAX).clamp(1, COLOR_MAX)
    }

    /// An axis title for `column`: its name, and its unit when it has one.
    pub fn axis_title(&self, column: &str) -> String {
        match self.units.iter().find(|(name, _)| name == column) {
            Some((_, unit)) => format!("{column} ({unit})"),
            None => column.to_string(),
        }
    }

    pub fn mark(&self) -> Mark {
        self.spec.mark
    }

    pub fn x(&self) -> Option<&String> {
        self.spec.encoding.x.field.as_ref()
    }

    pub fn y(&self) -> &[String] {
        &self.spec.encoding.y.field
    }

    pub fn aggregate(&self) -> Aggregate {
        self.spec.encoding.y.aggregate
    }

    pub fn color(&self) -> Option<&String> {
        self.spec.encoding.color.field.as_ref()
    }

    /// Open the chart view. From the column it was left on, on the same dataset,
    /// the chart comes back as it was, less any column the view no longer has.
    /// Otherwise the chart is chosen from the cursor column's type (`cursor`): a
    /// histogram of a number, a bar of a category's counts, a line over a date.
    pub fn open(
        &mut self,
        columns: ChartColumns<'_>,
        cursor: Option<(&str, &DataType)>,
        default_row_limit: Option<usize>,
        grid: bool,
        dataset: u64,
    ) {
        self.close_picker();
        self.temporal_candidates = columns.datetime.to_vec();
        self.bucketable_candidates = columns.bucketable.to_vec();
        self.numeric_candidates = columns.numeric.to_vec();
        self.category_candidates = columns.category.to_vec();
        self.plot_focus = false;
        self.rows_draft = None;
        let opened_on = cursor.map(|(name, _)| name.to_string());
        let restored = std::mem::take(&mut self.restored);
        if self.dataset == Some(dataset) && (self.opened_on == opened_on || restored) {
            self.keep_existing_choices();
            self.settle();
            self.focus = ChartFocus::Type;
            return;
        }
        self.dataset = Some(dataset);
        self.opened_on = opened_on;
        self.cursor_x = None;
        self.color_counts = None;
        self.spec = ChartSpec::default();
        self.suggested = None;
        self.y_starts_at_zero = false;
        self.log_scale = false;
        self.show_legend = true;
        self.grid = grid;
        self.share = false;
        self.value_range = ValueRange::All;
        self.row_limit = default_row_limit.and_then(|n| {
            if n == 0 {
                None
            } else {
                Some(n.clamp(1, CHART_ROW_LIMIT_MAX))
            }
        });
        self.sample_rows = self
            .row_limit
            .unwrap_or(crate::config::DEFAULT_CHART_ROW_LIMIT);
        self.hist_bins = HISTOGRAM_DEFAULT_BINS;
        self.kde_bandwidth_factor = 1.0;
        self.heatmap_bins = HEATMAP_DEFAULT_BINS;
        self.bar_order = BarOrder::Value;
        if let Some((name, dtype)) = cursor {
            self.suggest(name, dtype);
        }
        self.focus = ChartFocus::Type;
    }

    /// Put a saved view's chart in place for `dataset`: the next `c` draws it.
    pub fn restore(&mut self, saved: &crate::view::SavedChart, dataset: u64) {
        self.spec = saved.spec.clone();
        self.hist_bins = saved.histogram_bins.max(1);
        self.heatmap_bins = saved.heatmap_bins.max(1);
        self.kde_bandwidth_factor = saved.bandwidth;
        self.value_range = saved.range;
        self.bar_order = saved.bar_order;
        self.share = saved.share;
        self.y_starts_at_zero = saved.y_starts_at_zero;
        self.log_scale = saved.log_scale;
        self.show_legend = saved.legend;
        self.grid = saved.grid;
        self.row_limit = saved.rows;
        if let Some(rows) = saved.rows {
            self.sample_rows = rows;
        }
        self.suggested = None;
        self.color_counts = None;
        self.cursor_x = None;
        self.dataset = Some(dataset);
        self.restored = true;
    }

    /// The chart as a view keeps it, with `seed`, the one its own sample is drawn
    /// with, and `export`, how it was last exported.
    pub fn saved(
        &self,
        seed: u64,
        export: Option<crate::view::SavedChartExport>,
    ) -> crate::view::SavedChart {
        crate::view::SavedChart {
            spec: self.spec.clone(),
            histogram_bins: self.hist_bins,
            heatmap_bins: self.heatmap_bins,
            bandwidth: self.kde_bandwidth_factor,
            range: self.value_range,
            bar_order: self.bar_order,
            share: self.share,
            y_starts_at_zero: self.y_starts_at_zero,
            log_scale: self.log_scale,
            legend: self.show_legend,
            grid: self.grid,
            rows: if self.view_sampled {
                None
            } else {
                self.row_limit
            },
            seed: (!self.view_sampled && self.row_limit.is_some()).then_some(seed),
            export,
        }
    }

    /// Show Me: the chart a column's type suggests. A number: its histogram. A
    /// category: a bar of its counts. A date or time: a line over it, of the
    /// first numeric column, with Y left to pick when there is none.
    fn suggest(&mut self, name: &str, dtype: &DataType) {
        let name = name.to_string();
        let encoding = &mut self.spec.encoding;
        if self.temporal_candidates.contains(&name) {
            self.spec.mark = Mark::Line;
            encoding.x.field = Some(name);
            if let Some(y) = self.numeric_candidates.first() {
                encoding.y.field = vec![y.clone()];
            }
        } else if dtype.is_float() || (dtype.is_numeric() && !dtype.is_integer()) {
            self.spec.mark = Mark::Histogram;
            encoding.x.field = Some(name);
        } else if self.category_candidates.contains(&name) && !dtype.is_integer() {
            self.spec.mark = Mark::Bar;
            encoding.x.field = Some(name);
            encoding.y.aggregate = Aggregate::Count;
        } else if self.numeric_candidates.contains(&name) {
            // An integer: a measure more often than a code.
            self.spec.mark = Mark::Histogram;
            encoding.x.field = Some(name);
        } else {
            return;
        }
        self.suggested = Some(crate::column_types::dtype_label(dtype));
    }

    /// Close the chart view. The choices stay for the next open on this dataset.
    pub fn close(&mut self) {
        self.close_picker();
        self.plot_focus = false;
        self.rows_draft = None;
    }

    pub fn close_picker(&mut self) {
        self.picker = None;
        self.picker_for = None;
        self.picker_details.clear();
    }

    /// Whether the crosshair can take the keys: a line or scatter plot with points
    /// on screen.
    pub fn has_crosshair(&self) -> bool {
        self.spec.mark.is_xy() && self.plot.is_some()
    }

    /// Drop the choices whose columns the view no longer offers.
    fn keep_existing_choices(&mut self) {
        let mut all: Vec<&String> = self.numeric_candidates.iter().collect();
        all.extend(self.temporal_candidates.iter());
        all.extend(self.category_candidates.iter());
        let has = |c: &String| all.contains(&c);
        let encoding = &mut self.spec.encoding;
        if encoding.x.field.as_ref().is_some_and(|c| !has(c)) {
            encoding.x.field = None;
        }
        encoding.y.field.retain(|c| has(c));
        if encoding.color.field.as_ref().is_some_and(|c| !has(c)) {
            encoding.color.field = None;
            encoding.color.values.clear();
        }
    }

    // ----- What applies -----

    fn is_temporal(&self, column: &str) -> bool {
        self.temporal_candidates.iter().any(|c| c == column)
    }

    /// Whether X is a date or a time, which a line can bucket.
    pub fn x_is_temporal(&self) -> bool {
        self.x().is_some_and(|x| self.is_temporal(x))
    }

    /// Whether X is a date or a datetime, which a time bucket truncates; a time of
    /// day is not.
    pub fn x_is_bucketable(&self) -> bool {
        self.x()
            .is_some_and(|x| self.bucketable_candidates.iter().any(|c| c == x))
    }

    /// The columns a shelf takes on the chart on screen.
    fn pool(&self, shelf: PickerFor) -> Vec<String> {
        let mark = self.spec.mark;
        let x = self.x();
        match shelf {
            PickerFor::X => match mark {
                Mark::Line | Mark::Scatter => {
                    let mut out = self.temporal_candidates.clone();
                    for c in &self.numeric_candidates {
                        if !out.contains(c) {
                            out.push(c.clone());
                        }
                    }
                    out
                }
                Mark::Bar | Mark::Box => self.category_candidates.clone(),
                Mark::Histogram | Mark::Kde | Mark::Heatmap => self.numeric_candidates.clone(),
            },
            // A distinct count takes any column; the rest a number.
            PickerFor::Y
                if self.spec.encoding.y.aggregate.takes_any_y() && self.takes_aggregate() =>
            {
                let mut all = self.numeric_candidates.clone();
                for c in self
                    .temporal_candidates
                    .iter()
                    .chain(&self.category_candidates)
                {
                    if !all.contains(c) {
                        all.push(c.clone());
                    }
                }
                all.retain(|c| Some(c) != x);
                all
            }
            // The X column against itself is only a diagonal.
            PickerFor::Y => self
                .numeric_candidates
                .iter()
                .filter(|c| Some(*c) != x || mark == Mark::Box)
                .cloned()
                .collect(),
            PickerFor::Color => self
                .category_candidates
                .iter()
                .filter(|c| Some(*c) != x)
                .cloned()
                .collect(),
            PickerFor::ColorValues => Vec::new(),
        }
    }

    /// Whether the Y shelf takes several columns: a line or scatter chart draws one
    /// series each.
    pub fn y_is_multi(&self) -> bool {
        self.spec.mark.is_xy()
    }

    /// How the Y shelf is used on the chart on screen.
    pub fn y_use(&self) -> ShelfUse {
        match self.spec.mark {
            Mark::Kde => ShelfUse::Dimmed("density"),
            _ => ShelfUse::Used,
        }
    }

    /// How the Color shelf is used on the chart on screen.
    pub fn color_use(&self) -> ShelfUse {
        Self::color_use_in(&self.spec)
    }

    /// How `spec` uses its Color shelf.
    pub fn color_use_in(spec: &ChartSpec) -> ShelfUse {
        let y = &spec.encoding.y;
        match spec.mark {
            Mark::Box => ShelfUse::Dimmed("same as X"),
            Mark::Heatmap => ShelfUse::Dimmed("density"),
            Mark::Line | Mark::Scatter if y.field.len() > 1 => ShelfUse::Dimmed("one per Y column"),
            Mark::Bar if y.aggregate == Aggregate::None => ShelfUse::Dimmed("needs an aggregate"),
            _ => ShelfUse::Used,
        }
    }

    /// Whether `spec`'s color splits its chart into series.
    pub fn colored_in(spec: &ChartSpec) -> bool {
        spec.encoding.color.field.is_some() && Self::color_use_in(spec) == ShelfUse::Used
    }

    /// Whether the chart takes an aggregate: a line, a scatter, a bar.
    pub fn takes_aggregate(&self) -> bool {
        matches!(self.spec.mark, Mark::Line | Mark::Scatter | Mark::Bar)
    }

    /// Whether the chart reads every row (an aggregate over the whole view) rather
    /// than a sample.
    pub fn aggregates(&self) -> bool {
        self.takes_aggregate() && self.aggregate() != Aggregate::None
    }

    /// Whether the color splits the chart into series.
    pub fn colored(&self) -> bool {
        Self::colored_in(&self.spec)
    }

    /// Whether a colored `spec` draws Other: as set, or on for a scatter, whose
    /// cloud keeps its shape with every point drawn.
    pub fn shows_other_in(spec: &ChartSpec) -> bool {
        Self::colored_in(spec)
            && spec
                .encoding
                .color
                .other
                .unwrap_or(spec.mark == Mark::Scatter)
    }

    pub fn shows_other(&self) -> bool {
        Self::shows_other_in(&self.spec)
    }

    /// The panel's rows for the chart on screen, in Tab order. A dimmed shelf is
    /// left out: focus passes over it.
    pub fn row_order(&self) -> Vec<ChartFocus> {
        use ChartFocus::*;
        let mark = self.spec.mark;
        let mut rows = vec![Type, X];
        match mark {
            Mark::Line | Mark::Scatter if self.x_is_bucketable() => rows.push(TimeUnit),
            Mark::Bar => rows.push(Order),
            Mark::Histogram => rows.push(Bins),
            _ => {}
        }
        if self.y_use() == ShelfUse::Used {
            rows.push(Y);
        }
        if self.takes_aggregate() {
            rows.push(Aggregate);
            if self.aggregate() == self::Aggregate::Quantile {
                rows.push(Quantile);
            }
        }
        if self.color_use() == ShelfUse::Used {
            rows.push(Color);
            if self.color().is_some() {
                rows.push(ColorValues);
            }
        }
        // Options.
        match mark {
            Mark::Line | Mark::Scatter => {
                if self.aggregates() && self.aggregate().runs_cumulative() {
                    rows.push(Cumulative);
                }
                rows.extend([YStartsAtZero, LogScale, ShowLegend, Grid]);
            }
            Mark::Bar => rows.push(ShowLegend),
            Mark::Histogram => rows.extend([Range, ShowLegend, Grid]),
            Mark::Kde => rows.extend([Bandwidth, Range, ShowLegend, Grid]),
            Mark::Box => rows.extend([Range, Grid]),
            Mark::Heatmap => rows.push(Bins),
        }
        if !self.aggregates() && !self.view_sampled {
            rows.push(LimitRows);
        }
        rows
    }

    /// Whether the chart on screen has axes to draw a grid on: the heatmap's cells
    /// and the bar chart's rows have none.
    pub fn has_grid(&self) -> bool {
        self.row_order().contains(&ChartFocus::Grid)
    }

    // ----- Type -----

    /// Switch the chart type. The shelves carry over where the new type takes
    /// what they hold; a column it cannot take moves to where it can, or goes.
    pub fn set_mark(&mut self, mark: Mark) {
        if mark == self.spec.mark {
            return;
        }
        let old = self.spec.mark;
        self.spec.mark = mark;
        self.suggested = None;
        self.close_picker();
        if !mark.is_xy() {
            self.plot_focus = false;
        }
        let encoding = &mut self.spec.encoding;
        // A number charted against something becomes the value of a one-column
        // chart, and the other way round.
        let numeric = |c: &String| self.numeric_candidates.contains(c);
        let category = |c: &String| self.category_candidates.contains(c);
        match mark {
            Mark::Histogram | Mark::Kde => {
                if !encoding.x.field.as_ref().is_some_and(numeric) {
                    encoding.x.field = encoding.y.field.first().cloned();
                }
            }
            Mark::Box => {
                if encoding.y.field.is_empty()
                    && let Some(x) = encoding.x.field.clone().filter(numeric)
                {
                    encoding.y.field = vec![x];
                    encoding.x.field = None;
                }
                if !encoding.x.field.as_ref().is_some_and(category) {
                    encoding.x.field = None;
                }
            }
            Mark::Bar => {
                if !encoding.x.field.as_ref().is_some_and(category) {
                    encoding.x.field = encoding.color.field.take();
                    encoding.color.values.clear();
                }
                if encoding.y.field.is_empty() && encoding.y.aggregate == Aggregate::None {
                    encoding.y.aggregate = Aggregate::Count;
                }
            }
            Mark::Heatmap => {
                if !encoding.x.field.as_ref().is_some_and(numeric) {
                    encoding.x.field = None;
                }
            }
            Mark::Line | Mark::Scatter => {
                if matches!(old, Mark::Histogram | Mark::Kde | Mark::Box)
                    && encoding.y.field.is_empty()
                    && let Some(x) = encoding.x.field.take()
                {
                    encoding.y.field = vec![x];
                }
            }
        }
        self.settle();
        if !self.row_order().contains(&self.focus) {
            self.focus = ChartFocus::Type;
        }
    }

    /// Keep the spec within what the chart on screen takes.
    fn settle(&mut self) {
        let x_pool = self.pool(PickerFor::X);
        let y_pool = self.pool(PickerFor::Y);
        let color_pool = self.pool(PickerFor::Color);
        let mark = self.spec.mark;
        let x_temporal = self.x_is_bucketable();
        let encoding = &mut self.spec.encoding;
        if encoding
            .x
            .field
            .as_ref()
            .is_some_and(|x| !x_pool.contains(x))
        {
            encoding.x.field = None;
        }
        encoding.y.field.retain(|y| y_pool.contains(y));
        if !mark.is_xy() {
            encoding.y.field.truncate(1);
        }
        // A bucket holds many rows: without an aggregate there is nothing to bucket.
        if !x_temporal || !mark.is_xy() || encoding.y.aggregate == Aggregate::None {
            encoding.x.time_unit = TimeUnit::None;
        }
        if encoding
            .color
            .field
            .as_ref()
            .is_some_and(|c| !color_pool.contains(c))
        {
            encoding.color.field = None;
            encoding.color.values.clear();
        }
        if !matches!(mark, Mark::Line | Mark::Scatter | Mark::Bar) {
            encoding.y.aggregate = Aggregate::None;
        }
        if !encoding.y.aggregate.runs_cumulative() || !mark.is_xy() {
            encoding.y.cumulative = Cumulative::Off;
        }
    }

    pub fn step_mark(&mut self, delta: i8) {
        let mark = crate::form::step_value(&Mark::ALL, self.spec.mark, delta);
        self.set_mark(mark);
    }

    // ----- Picker -----

    /// Which picker the focused row opens, if it opens one.
    pub fn picker_for(&self, focus: ChartFocus) -> Option<PickerFor> {
        match focus {
            ChartFocus::X => Some(PickerFor::X),
            ChartFocus::Y if self.spec.mark != Mark::Histogram => Some(PickerFor::Y),
            ChartFocus::Color => Some(PickerFor::Color),
            ChartFocus::ColorValues => Some(PickerFor::ColorValues),
            _ => None,
        }
    }

    /// Whether the focused row's picker toggles several items.
    pub fn picker_is_multi(&self, which: PickerFor) -> bool {
        match which {
            PickerFor::Y => self.y_is_multi(),
            PickerFor::ColorValues => true,
            PickerFor::X | PickerFor::Color => false,
        }
    }

    /// Whether a shelf's picker offers "none" first.
    fn offers_none(&self, which: PickerFor) -> bool {
        which == PickerFor::Color || (which == PickerFor::X && self.spec.mark == Mark::Box)
    }

    /// What a picker offers, and what goes beside each item.
    fn picker_items(&self, which: PickerFor) -> (Vec<String>, Vec<String>) {
        if which == PickerFor::ColorValues {
            let null = crate::glyphs::get().null;
            let Some(counts) = &self.color_counts else {
                return (Vec::new(), Vec::new());
            };
            return counts
                .values
                .iter()
                .map(|(value, rows)| {
                    (
                        value.clone().unwrap_or_else(|| null.to_string()),
                        crate::numfmt::group_chrome(*rows as usize),
                    )
                })
                .unzip();
        }
        let mut items = Vec::new();
        if self.offers_none(which) {
            items.push(NONE_ITEM.to_string());
        }
        items.extend(self.pool(which));
        (items, Vec::new())
    }

    /// The value at `i` of the value picker.
    fn color_value_at(&self, i: usize) -> Option<Option<String>> {
        self.color_counts
            .as_ref()?
            .values
            .get(i)
            .map(|(value, _)| value.clone())
    }

    /// Where the row's current choice sits among `items`.
    fn current_index(&self, which: PickerFor, items: &[String]) -> Option<usize> {
        let encoding = &self.spec.encoding;
        let current = match which {
            PickerFor::X => encoding.x.field.as_deref(),
            PickerFor::Y => encoding.y.field.first().map(String::as_str),
            PickerFor::Color => encoding.color.field.as_deref(),
            PickerFor::ColorValues => return Some(0),
        };
        match current {
            Some(current) => items.iter().position(|i| i == current),
            None if self.offers_none(which) => Some(0),
            None => None,
        }
    }

    /// Open the Picker for the focused row, cursor on the current choice. The value
    /// picker opens only once the chart has counted the values.
    pub fn open_picker(&mut self) {
        let Some(which) = self.picker_for(self.focus) else {
            return;
        };
        if which == PickerFor::ColorValues && !self.has_color_counts() {
            return;
        }
        let (items, details) = self.picker_items(which);
        let mut state = PickerState::new(items.clone());
        if let Some(i) = self.current_index(which, &items) {
            state.select_original(i);
        }
        self.picker = Some(state);
        self.picker_for = Some(which);
        self.picker_details = details;
    }

    /// Whether the value picker has values to list: the chart has counted the
    /// Color column on screen.
    pub fn has_color_counts(&self) -> bool {
        self.color_counts
            .as_ref()
            .is_some_and(|c| Some(&c.column) == self.color())
    }

    /// ←/→ on a pick-one column row: the next or previous column, chosen at once.
    pub fn step_picker_row(&mut self, delta: i8) {
        let Some(which) = self.picker_for(self.focus) else {
            return;
        };
        if self.picker_is_multi(which) {
            return;
        }
        let (items, _) = self.picker_items(which);
        if items.is_empty() {
            return;
        }
        let next = match self.current_index(which, &items) {
            Some(at) => crate::form::step_index(at, items.len(), delta),
            None if delta < 0 => items.len() - 1,
            None => 0,
        };
        self.choose(which, &items[next], next);
    }

    /// The original index under the open Picker's cursor.
    fn picker_cursor(&self) -> Option<usize> {
        self.picker.as_ref()?.selected_original()
    }

    /// Enter in the Picker: a pick-one row takes the cursor's item; a row of
    /// several keeps its toggles, or adopts the cursor's item when none are
    /// toggled, so Enter on a fresh list still charts something. The Picker closes.
    pub fn picker_choose(&mut self) {
        let (Some(which), Some(i)) = (self.picker_for, self.picker_cursor()) else {
            self.close_picker();
            return;
        };
        let item = self.picker.as_ref().and_then(|p| p.items().get(i).cloned());
        self.close_picker();
        let Some(item) = item else {
            return;
        };
        match which {
            PickerFor::Y if self.y_is_multi() => {
                if self.spec.encoding.y.field.is_empty() {
                    self.spec.encoding.y.field.push(item);
                }
            }
            PickerFor::ColorValues => {
                if self.spec.encoding.color.values.is_empty()
                    && let Some(value) = self.color_value_at(i)
                {
                    self.spec.encoding.color.values.push(value);
                }
            }
            _ => self.choose(which, &item, i),
        }
        self.settle();
    }

    /// Take `item` (at `i` in the row's items) as the row's one choice.
    fn choose(&mut self, which: PickerFor, item: &str, i: usize) {
        let none = self.offers_none(which) && i == 0;
        let encoding = &mut self.spec.encoding;
        match which {
            PickerFor::X => {
                encoding.x.field = (!none).then(|| item.to_string());
                // A series cannot be the X axis too, nor a color.
                encoding.y.field.retain(|y| y != item);
                if encoding.color.field.as_deref() == Some(item) {
                    encoding.color.field = None;
                    encoding.color.values.clear();
                }
                encoding.x.time_unit = TimeUnit::None;
            }
            PickerFor::Y => encoding.y.field = vec![item.to_string()],
            PickerFor::Color => {
                let field = (!none).then(|| item.to_string());
                if field != encoding.color.field {
                    encoding.color.values.clear();
                }
                encoding.color.field = field;
            }
            PickerFor::ColorValues => {}
        }
        self.settle();
        if !self.row_order().contains(&self.focus) {
            self.focus = ChartFocus::Type;
        }
    }

    /// Space in a picker of several: flip the cursor's item in or out, up to the
    /// palette's colors.
    pub fn picker_toggle(&mut self) {
        let (Some(which), Some(i)) = (self.picker_for, self.picker_cursor()) else {
            return;
        };
        match which {
            PickerFor::Y if self.y_is_multi() => {
                let Some(item) = self.picker.as_ref().and_then(|p| p.items().get(i).cloned())
                else {
                    return;
                };
                let most = Y_SERIES_MAX.min(self.series_max());
                let field = &mut self.spec.encoding.y.field;
                if let Some(pos) = field.iter().position(|c| *c == item) {
                    field.remove(pos);
                } else if field.len() < most {
                    field.push(item);
                }
            }
            PickerFor::ColorValues => {
                let Some(value) = self.color_value_at(i) else {
                    return;
                };
                let most = self.series_max();
                let values = &mut self.spec.encoding.color.values;
                if let Some(pos) = values.iter().position(|v| *v == value) {
                    values.remove(pos);
                } else if values.len() < most {
                    values.push(value);
                }
            }
            _ => {}
        }
    }

    /// Whether the item at `i` of the open picker is toggled on.
    pub fn is_marked(&self, i: usize) -> bool {
        match self.picker_for {
            Some(PickerFor::Y) => self
                .picker
                .as_ref()
                .and_then(|p| p.items().get(i))
                .is_some_and(|item| self.y().contains(item)),
            Some(PickerFor::ColorValues) => self
                .color_value_at(i)
                .is_some_and(|v| self.spec.encoding.color.values.contains(&v)),
            _ => false,
        }
    }

    /// Whether the open picker toggles several items.
    pub fn picker_multi(&self) -> bool {
        self.picker_for.is_some_and(|w| self.picker_is_multi(w))
    }

    // ----- What is charted right now -----

    /// The spec to chart: the one chosen, with an open Picker's cursor previewed on
    /// the Y shelf (and on X, except for a line or scatter, whose X re-reads
    /// everything).
    pub fn effective_spec(&self) -> ChartSpec {
        let mut spec = self.spec.clone();
        let (Some(which), Some(i)) = (self.picker_for, self.picker_cursor()) else {
            return spec;
        };
        let Some(item) = self.picker.as_ref().and_then(|p| p.items().get(i).cloned()) else {
            return spec;
        };
        match which {
            PickerFor::Y if self.y_is_multi() => {
                if !spec.encoding.y.field.contains(&item) {
                    spec.encoding.y.field.push(item);
                }
            }
            PickerFor::Y => spec.encoding.y.field = vec![item],
            PickerFor::X if !spec.mark.is_xy() => {
                spec.encoding.x.field = (!(self.offers_none(which) && i == 0)).then_some(item);
            }
            _ => {}
        }
        spec
    }

    // ----- Stepping -----

    /// ←/→ (and Space, on a choice) on the focused row.
    pub fn step(&mut self, focus: ChartFocus, delta: i8) {
        match focus {
            ChartFocus::Type => self.step_mark(delta),
            ChartFocus::TimeUnit => {
                let encoding = &mut self.spec.encoding;
                encoding.x.time_unit =
                    crate::form::step_value(&TimeUnit::ALL, encoding.x.time_unit, delta);
                // A bucket holds many rows, so it needs something to make of them.
                if encoding.x.time_unit != TimeUnit::None && encoding.y.aggregate == Aggregate::None
                {
                    encoding.y.aggregate = Aggregate::Mean;
                }
            }
            ChartFocus::Aggregate => {
                let was = self.aggregate();
                let bucketable = self.x_is_bucketable() && self.spec.mark.is_xy();
                let encoding = &mut self.spec.encoding;
                encoding.y.aggregate =
                    crate::form::step_value(&Aggregate::ALL, encoding.y.aggregate, delta);
                // A date X with a group per raw value is close to a group per row:
                // an aggregate starts by the day.
                if was == Aggregate::None
                    && bucketable
                    && encoding.y.aggregate != Aggregate::None
                    && encoding.x.time_unit == TimeUnit::None
                {
                    encoding.x.time_unit = TimeUnit::Day;
                }
                self.settle();
            }
            ChartFocus::Cumulative => {
                let y = &mut self.spec.encoding.y;
                y.cumulative = crate::form::step_value(&Cumulative::ALL, y.cumulative, delta);
            }
            ChartFocus::Quantile => {
                let y = &mut self.spec.encoding.y;
                y.percentile = Some(crate::form::step_value(&QUANTILES, y.quantile(), delta));
            }
            ChartFocus::Y if self.spec.mark == Mark::Histogram => self.share = !self.share,
            ChartFocus::Order => {
                self.bar_order = crate::form::step_value(&BarOrder::ALL, self.bar_order, delta);
            }
            ChartFocus::Range => {
                self.value_range =
                    crate::form::step_value(&ValueRange::ALL, self.value_range, delta);
            }
            ChartFocus::Bins => self.adjust_bins(delta.into()),
            ChartFocus::Bandwidth => {
                self.adjust_kde_bandwidth_factor(f64::from(delta) * KDE_BANDWIDTH_STEP)
            }
            ChartFocus::LimitRows => self.toggle_rows(),
            ChartFocus::YStartsAtZero => self.y_starts_at_zero = !self.y_starts_at_zero,
            ChartFocus::LogScale => self.log_scale = !self.log_scale,
            ChartFocus::ShowLegend => self.show_legend = !self.show_legend,
            ChartFocus::Grid => self.grid = !self.grid,
            // The values line: Space picks them, ←/→ turn Other on or off.
            ChartFocus::ColorValues => {
                self.spec.encoding.color.other = Some(!self.shows_other());
            }
            focus => {
                if self.picker_for(focus).is_some() {
                    self.step_picker_row(delta);
                }
            }
        }
        if !self.row_order().contains(&self.focus) {
            self.focus = ChartFocus::Type;
            self.leave_rows();
        }
    }

    pub fn is_toggle_row(&self, focus: ChartFocus) -> bool {
        matches!(
            focus,
            ChartFocus::YStartsAtZero
                | ChartFocus::LogScale
                | ChartFocus::ShowLegend
                | ChartFocus::Grid
        )
    }

    pub fn toggle_grid(&mut self) {
        self.grid = !self.grid;
    }

    fn adjust_bins(&mut self, delta: i32) {
        if self.spec.mark == Mark::Heatmap {
            self.heatmap_bins = (self.heatmap_bins as i32 + delta)
                .clamp(HEATMAP_MIN_BINS as i32, HEATMAP_MAX_BINS as i32)
                as usize;
        } else {
            self.hist_bins = (self.hist_bins as i32 + delta)
                .clamp(HISTOGRAM_MIN_BINS as i32, HISTOGRAM_MAX_BINS as i32)
                as usize;
        }
    }

    pub fn adjust_kde_bandwidth_factor(&mut self, delta: f64) {
        let next = (self.kde_bandwidth_factor + delta).clamp(KDE_BANDWIDTH_MIN, KDE_BANDWIDTH_MAX);
        self.kde_bandwidth_factor = (next * 10.0).round() / 10.0;
    }

    /// `+`/`-`: step the focused number row.
    pub fn adjust_number_row(&mut self, delta: i32) {
        match self.focus {
            ChartFocus::Bins => self.adjust_bins(delta),
            ChartFocus::Bandwidth => {
                self.adjust_kde_bandwidth_factor(delta as f64 * KDE_BANDWIDTH_STEP)
            }
            _ => {}
        }
    }

    // ----- The Rows row -----

    /// What the Rows row shows: the pending change, or what the chart reads.
    pub fn rows_shown(&self) -> RowsDraft {
        self.rows_draft.clone().unwrap_or(RowsDraft {
            every: self.row_limit.is_none(),
            ..RowsDraft::default()
        })
    }

    /// Whether the Rows row holds a change the chart has not read.
    pub fn rows_pending(&self) -> bool {
        self.rows_draft
            .as_ref()
            .is_some_and(|draft| draft.typed.is_some() || draft.every != self.row_limit.is_none())
    }

    /// ←/→ or Space on Rows: Sample or Every row, pending until Enter. A size being
    /// typed is taken first; one that cannot be read stays to be fixed.
    pub fn toggle_rows(&mut self) {
        if !self.take_typed_size() {
            return;
        }
        let mut draft = self.rows_shown();
        draft.every = !draft.every;
        self.rows_draft = Some(draft);
    }

    /// A key typed on Rows: part of a sample size (`50k`). Typing chooses Sample.
    pub fn type_rows(&mut self, c: char) {
        let mut draft = self.rows_shown();
        draft.every = false;
        draft.error = None;
        draft.typed.get_or_insert_with(String::new).push(c);
        self.rows_draft = Some(draft);
    }

    /// Whether a size is being typed on Rows.
    pub fn typing_rows(&self) -> bool {
        self.rows_draft.as_ref().is_some_and(|d| d.typed.is_some())
    }

    /// Backspace on Rows: one character of the typed size off.
    pub fn backspace_rows(&mut self) {
        if let Some(draft) = self.rows_draft.as_mut()
            && let Some(typed) = draft.typed.as_mut()
        {
            typed.pop();
            draft.error = None;
            if typed.is_empty() {
                draft.typed = None;
            }
        }
    }

    /// Esc on Rows with a change pending: put back what the chart reads.
    pub fn discard_rows(&mut self) {
        self.rows_draft = None;
    }

    /// Resolve a typed size into the draft's sample size. False, with the reason on
    /// the row, when it cannot be read.
    fn take_typed_size(&mut self) -> bool {
        let Some(draft) = self.rows_draft.as_mut() else {
            return true;
        };
        let Some(typed) = draft.typed.take() else {
            return true;
        };
        match crate::sampling::parse_size(&typed) {
            Ok(rows) => {
                let rows = rows.min(CHART_ROW_LIMIT_MAX);
                // A sample of at least every row is every row.
                if self.view_rows.is_some_and(|total| rows >= total) {
                    draft.every = true;
                } else {
                    draft.every = false;
                    self.sample_rows = rows;
                }
                true
            }
            Err(e) => {
                draft.error = Some(e.short());
                draft.typed = Some(typed);
                false
            }
        }
    }

    /// Enter on Rows: read what it says. False when a typed size cannot be read,
    /// which stays on the row with why.
    pub fn commit_rows(&mut self) -> bool {
        if !self.take_typed_size() {
            return false;
        }
        if let Some(draft) = self.rows_draft.take() {
            self.row_limit = (!draft.every).then_some(self.sample_rows);
        }
        true
    }

    /// Focus left the Rows row: what it says is read, and a size that cannot be
    /// is dropped.
    pub fn leave_rows(&mut self) {
        if !self.commit_rows() {
            self.rows_draft = None;
        }
    }

    /// Whether the spec names everything its chart needs.
    pub fn is_complete(spec: &ChartSpec) -> bool {
        let encoding = &spec.encoding;
        let x = encoding.x.field.is_some();
        let y = !encoding.y.field.is_empty();
        match spec.mark {
            Mark::Line | Mark::Scatter => x && (y || encoding.y.aggregate == Aggregate::Count),
            Mark::Bar => x && (y || encoding.y.aggregate == Aggregate::Count),
            Mark::Histogram | Mark::Kde => x,
            Mark::Box => y,
            Mark::Heatmap => x && y,
        }
    }

    pub fn can_export(&self) -> bool {
        Self::is_complete(&self.effective_spec())
    }

    /// How the chart was made of the rows, as a phrase that stands alone: `mean by
    /// month, running sum, colored by carrier`; empty when there is nothing to say.
    /// The columns charted are named at their axes, not here.
    pub fn how(&self) -> String {
        let spec = self.effective_spec();
        let encoding = &spec.encoding;
        let x = encoding.x.field.clone().unwrap_or_default();
        let mut parts: Vec<String> = Vec::new();
        match spec.mark {
            Mark::Histogram => parts.push(if self.share {
                "share per bin".to_string()
            } else {
                "count per bin".to_string()
            }),
            // The y axis already says density.
            Mark::Kde => {}
            Mark::Box if !x.is_empty() => parts.push(format!("one box per {x}")),
            Mark::Box => {}
            Mark::Heatmap => parts.push("rows per cell".to_string()),
            Mark::Line | Mark::Scatter | Mark::Bar => {
                let aggregate = encoding.y.aggregate;
                let mut how = String::new();
                // Cumulative runs over the rows, not the aggregate.
                if aggregate != Aggregate::None && encoding.y.cumulative == Cumulative::Off {
                    how.push_str(&encoding.y.aggregate_name());
                    how.push(' ');
                }
                let unit = encoding.x.time_unit;
                if unit != TimeUnit::None {
                    how.push_str(&format!("by {}", unit.label()));
                } else if aggregate != Aggregate::None {
                    how.push_str(&format!("by {x}"));
                }
                if !how.trim().is_empty() {
                    parts.push(how.trim().to_string());
                }
                match encoding.y.cumulative {
                    Cumulative::Off => {}
                    Cumulative::Sum => parts.push("running sum".to_string()),
                    Cumulative::Compound => parts.push("compounded".to_string()),
                }
            }
        }
        if self.colored() {
            parts.push(format!("colored by {}", self.color().unwrap()));
        }
        parts.join(", ")
    }
}

impl crate::form::Form for ChartModal {
    type Field = ChartFocus;

    fn shown_picker(&mut self) -> Option<(&mut crate::widgets::ui::PickerState, bool)> {
        let multi = self.picker_multi();
        self.picker.as_mut().map(|p| (p, multi))
    }

    fn dismiss_picker(&mut self) {
        ChartModal::close_picker(self);
    }

    fn pick(&mut self, toggle: bool) {
        if toggle {
            self.picker_toggle();
        } else {
            self.picker_choose();
        }
    }

    fn fields(&self) -> Vec<(ChartFocus, crate::form::FieldKind)> {
        use crate::form::FieldKind;
        self.row_order()
            .into_iter()
            .map(|row| {
                let kind = match self.picker_for(row) {
                    Some(which) => FieldKind::Picker {
                        multi: self.picker_is_multi(which),
                    },
                    None if self.is_toggle_row(row) => FieldKind::Checkbox,
                    // Type, the buckets, the aggregates and the numbers step.
                    None => FieldKind::Choice,
                };
                (row, kind)
            })
            .collect()
    }

    fn focused(&self) -> ChartFocus {
        self.focus
    }

    fn set_focused(&mut self, field: ChartFocus) {
        if field != ChartFocus::LimitRows {
            self.leave_rows();
        }
        self.focus = field;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::Form;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    struct Cols {
        numeric: Vec<String>,
        datetime: Vec<String>,
        category: Vec<String>,
    }

    fn cols() -> Cols {
        Cols {
            numeric: s(&["delay", "distance", "year"]),
            datetime: s(&["date"]),
            category: s(&["carrier", "origin", "year"]),
        }
    }

    fn open_on(cursor: Option<(&str, &DataType)>) -> ChartModal {
        let c = cols();
        let mut modal = ChartModal::new();
        modal.open(
            ChartColumns {
                numeric: &c.numeric,
                datetime: &c.datetime,
                bucketable: &c.datetime,
                category: &c.category,
            },
            cursor,
            Some(10_000),
            false,
            1,
        );
        modal
    }

    #[test]
    fn an_axis_title_names_the_unit() {
        let mut modal = ChartModal::default();
        assert_eq!(modal.axis_title("cht1"), "cht1");
        modal.units = vec![("cht1".to_string(), "deg F".to_string())];
        assert_eq!(modal.axis_title("cht1"), "cht1 (deg F)");
    }

    /// `c` picks the chart from the cursor column's type and says so under Type.
    #[test]
    fn quick_chart_follows_the_cursor_column_type() {
        let modal = open_on(Some(("delay", &DataType::Float64)));
        assert_eq!(modal.mark(), Mark::Histogram);
        assert_eq!(modal.x().map(String::as_str), Some("delay"));
        assert_eq!(modal.suggested.as_deref(), Some("f64"));

        let modal = open_on(Some(("carrier", &DataType::String)));
        assert_eq!(modal.mark(), Mark::Bar);
        assert_eq!(modal.aggregate(), Aggregate::Count);
        assert!(ChartModal::is_complete(&modal.spec), "counts need no Y");

        let modal = open_on(Some(("date", &DataType::Date)));
        assert_eq!(modal.mark(), Mark::Line);
        assert_eq!(modal.x().map(String::as_str), Some("date"));
        assert_eq!(modal.y(), ["delay"], "the first numeric column");

        let modal = open_on(Some(("year", &DataType::Int64)));
        assert_eq!(modal.mark(), Mark::Histogram, "an integer is a measure");

        let modal = open_on(None);
        assert_eq!(modal.mark(), Mark::Line);
        assert!(modal.x().is_none() && modal.suggested.is_none());
    }

    #[test]
    fn changing_the_type_clears_the_suggestion() {
        let mut modal = open_on(Some(("delay", &DataType::Float64)));
        modal.step(ChartFocus::Type, 1);
        assert_eq!(modal.mark(), Mark::Box);
        assert!(modal.suggested.is_none());
        assert_eq!(modal.y(), ["delay"], "the histogram's value is the box's");
        assert!(modal.x().is_none(), "a box's X is a category or none");
    }

    /// Every type shows the same shelves; one it does not use is dimmed and
    /// focus passes over it.
    #[test]
    fn shelves_dim_by_type() {
        let mut modal = open_on(None);
        let rows = |m: &ChartModal| m.row_order();
        assert!(rows(&modal).contains(&ChartFocus::Color));
        modal.set_mark(Mark::Kde);
        assert_eq!(modal.y_use(), ShelfUse::Dimmed("density"));
        assert!(!rows(&modal).contains(&ChartFocus::Y));
        modal.set_mark(Mark::Box);
        assert_eq!(modal.color_use(), ShelfUse::Dimmed("same as X"));
        assert!(!rows(&modal).contains(&ChartFocus::Color));
        modal.set_mark(Mark::Heatmap);
        assert_eq!(modal.color_use(), ShelfUse::Dimmed("density"));
        modal.set_mark(Mark::Line);
        modal.spec.encoding.y.field = s(&["delay", "distance"]);
        assert_eq!(modal.color_use(), ShelfUse::Dimmed("one per Y column"));
        modal.set_mark(Mark::Bar);
        modal.spec.encoding.y.aggregate = Aggregate::None;
        assert_eq!(modal.color_use(), ShelfUse::Dimmed("needs an aggregate"));
    }

    /// The panel's rows follow the type: a bucket under a date X, the order under
    /// a bar's X, the bins under a histogram's; the sample size only where the
    /// chart samples.
    #[test]
    fn rows_follow_the_type() {
        use ChartFocus::*;
        let mut modal = open_on(Some(("date", &DataType::Date)));
        assert_eq!(
            modal.fields().iter().map(|(f, _)| *f).collect::<Vec<_>>(),
            [
                Type,
                X,
                TimeUnit,
                Y,
                Aggregate,
                Color,
                YStartsAtZero,
                LogScale,
                ShowLegend,
                Grid,
                LimitRows
            ]
        );
        modal.step(TimeUnit, 1);
        assert_eq!(modal.spec.encoding.x.time_unit, super::TimeUnit::Day);
        assert_eq!(
            modal.aggregate(),
            super::Aggregate::Mean,
            "a bucket needs one"
        );
        let rows = modal.row_order();
        assert!(rows.contains(&Cumulative) && !rows.contains(&LimitRows));

        modal.set_mark(Mark::Bar);
        assert_eq!(modal.spec.encoding.x.time_unit, super::TimeUnit::None);
        assert!(modal.row_order().contains(&Order));
        modal.set_mark(Mark::Histogram);
        assert!(modal.row_order().contains(&Bins));
    }

    /// Step the aggregate row by one until it reads `to`.
    fn step_to(modal: &mut ChartModal, to: Aggregate, delta: i8) {
        for _ in 0..Aggregate::ALL.len() {
            if modal.aggregate() == to {
                return;
            }
            modal.step(ChartFocus::Aggregate, delta);
        }
        assert_eq!(modal.aggregate(), to);
    }

    #[test]
    fn the_aggregate_steps_through_every_one() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        let labels: Vec<&str> = (0..Aggregate::ALL.len())
            .map(|_| {
                modal.step(ChartFocus::Aggregate, 1);
                modal.aggregate().label()
            })
            .collect();
        assert_eq!(
            labels,
            [
                "count", "distinct", "sum", "mean", "median", "stdev", "quantile", "min", "max",
                "first", "last", "none"
            ]
        );
        modal.step(ChartFocus::Aggregate, -1);
        assert_eq!(modal.aggregate(), Aggregate::Last);
    }

    #[test]
    fn cumulative_goes_with_the_aggregate() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.spec.encoding.y.aggregate = Aggregate::Sum;
        modal.step(ChartFocus::Cumulative, 1);
        assert_eq!(modal.spec.encoding.y.cumulative, Cumulative::Sum);
        modal.step(ChartFocus::Cumulative, 1);
        assert_eq!(modal.spec.encoding.y.cumulative, Cumulative::Compound);
        step_to(&mut modal, Aggregate::None, -1);
        assert_eq!(modal.spec.encoding.y.cumulative, Cumulative::Off);
    }

    #[test]
    fn color_picks_a_category_then_values_by_count() {
        let mut modal = open_on(Some(("delay", &DataType::Float64)));
        modal.focus = ChartFocus::Color;
        modal.open_picker();
        let items = modal.picker.as_ref().unwrap().items().to_vec();
        assert_eq!(items, ["none", "carrier", "origin", "year"]);
        modal.picker.as_mut().unwrap().select_original(1);
        modal.picker_choose();
        assert_eq!(modal.color().map(String::as_str), Some("carrier"));
        assert!(modal.row_order().contains(&ChartFocus::ColorValues));

        // Nothing to pick until a chart has counted the values.
        modal.focus = ChartFocus::ColorValues;
        modal.open_picker();
        assert!(modal.picker.is_none());
        modal.color_counts = Some(ColorCounts {
            column: "carrier".to_string(),
            values: (0..COLOR_MAX + 2)
                .map(|i| (Some(format!("C{i}")), 100 - i as u64))
                .chain([(None, 1)])
                .collect(),
        });
        modal.open_picker();
        assert_eq!(modal.picker_details[0], "100");
        for _ in 0..COLOR_MAX + 2 {
            modal.picker_toggle();
            modal.picker.as_mut().unwrap().move_down();
        }
        assert_eq!(modal.spec.encoding.color.values.len(), COLOR_MAX, "capped");
        assert!(modal.is_marked(0));
        modal.picker.as_mut().unwrap().select_original(0);
        modal.picker_toggle();
        assert!(!modal.is_marked(0));
        modal.picker_choose();
        assert_eq!(modal.spec.encoding.color.values.len(), COLOR_MAX - 1);

        // Another column starts from the top values again.
        modal.focus = ChartFocus::Color;
        modal.step(ChartFocus::Color, 1);
        assert_eq!(modal.color().map(String::as_str), Some("origin"));
        assert!(modal.spec.encoding.color.values.is_empty());
        modal.step(ChartFocus::Color, -1);
        modal.step(ChartFocus::Color, -1);
        assert!(modal.color().is_none(), "none is the first choice");
    }

    #[test]
    fn the_spec_reads_as_vega_lite() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.step(ChartFocus::TimeUnit, 3);
        modal.spec.encoding.color.field = Some("carrier".to_string());
        assert_eq!(
            modal.spec.to_vega_lite(),
            serde_json::json!({
                "mark": "line",
                "encoding": {
                    "x": {"field": "date", "timeUnit": "yearmonth"},
                    "y": {"field": "delay", "aggregate": "mean"},
                    "color": {"field": "carrier"},
                }
            })
        );
        let saved = serde_json::to_value(&modal.spec).unwrap();
        assert_eq!(saved["encoding"]["x"]["timeUnit"], "month");
        assert_eq!(saved["encoding"]["y"]["aggregate"], "mean");
    }

    #[test]
    fn the_title_says_how_the_rows_were_made() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.step(ChartFocus::TimeUnit, 3);
        modal.spec.encoding.y.cumulative = Cumulative::Sum;
        modal.spec.encoding.color.field = Some("carrier".to_string());
        assert_eq!(modal.how(), "by month, running sum, colored by carrier");
    }

    /// Distinct takes any Y, strings too, and reads `distinct by month`; leaving it
    /// for an aggregate of numbers lets a string Y go, as a sum never had one. It
    /// takes no cumulative.
    #[test]
    fn distinct_takes_any_y_and_no_cumulative() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        step_to(&mut modal, Aggregate::Distinct, 1);
        modal.focus = ChartFocus::Y;
        modal.open_picker();
        let items = modal.picker.as_ref().unwrap().items().to_vec();
        assert!(items.contains(&"carrier".to_string()), "{items:?}");
        modal.close_picker();
        modal.spec.encoding.y.field = vec!["carrier".to_string()];
        modal.step(ChartFocus::TimeUnit, 2);
        assert_eq!(modal.how(), "distinct by month");
        assert!(!modal.row_order().contains(&ChartFocus::Cumulative));
        modal.spec.encoding.y.cumulative = Cumulative::Sum;
        modal.step(ChartFocus::Aggregate, 0);
        assert_eq!(modal.spec.encoding.y.cumulative, Cumulative::Off);
        // Over to sum: a string is no number to add.
        modal.step(ChartFocus::Aggregate, 1);
        assert_eq!(modal.aggregate(), Aggregate::Sum);
        assert!(modal.spec.encoding.y.field.is_empty());
        assert_eq!(Aggregate::Distinct.vega_lite(90), Some("distinct"));
    }

    /// A quantile's percentile is the line under Aggregate: ←/→ step it, the title
    /// says `p95 by month`, and Vega-Lite names the quartiles. Stdev, quantile,
    /// first and last take no cumulative.
    #[test]
    fn a_quantile_steps_its_percentile() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        step_to(&mut modal, Aggregate::Quantile, 1);
        modal.step(ChartFocus::TimeUnit, 2);
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::Month);
        let rows = modal.row_order();
        let at = rows
            .iter()
            .position(|r| *r == ChartFocus::Aggregate)
            .unwrap();
        assert_eq!(rows[at + 1], ChartFocus::Quantile);
        assert_eq!(modal.how(), "p90 by month");
        modal.step(ChartFocus::Quantile, 1);
        assert_eq!(modal.spec.encoding.y.quantile(), 95);
        assert_eq!(modal.how(), "p95 by month");
        modal.step(ChartFocus::Quantile, 1);
        modal.step(ChartFocus::Quantile, 1);
        assert_eq!(modal.spec.encoding.y.quantile(), 1, "wraps");
        assert_eq!(Aggregate::Quantile.vega_lite(25), Some("q1"));
        assert_eq!(Aggregate::Quantile.vega_lite(75), Some("q3"));
        assert_eq!(Aggregate::Quantile.vega_lite(90), None);
        assert_eq!(Aggregate::Last.vega_lite(90), None);
        for aggregate in [
            Aggregate::Stdev,
            Aggregate::Quantile,
            Aggregate::First,
            Aggregate::Last,
        ] {
            modal.spec.encoding.y.aggregate = aggregate;
            assert!(!modal.row_order().contains(&ChartFocus::Cumulative));
        }
        modal.spec.encoding.y.aggregate = Aggregate::Last;
        assert!(!modal.row_order().contains(&ChartFocus::Quantile));
        assert_eq!(modal.how(), "last by month");
    }

    /// Each type's phrase reads alone and never names the Y column, which its axis
    /// names.
    #[test]
    fn the_how_names_no_y_column() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.step(ChartFocus::TimeUnit, 3);
        assert_eq!(modal.how(), "mean by month");
        modal.set_mark(Mark::Scatter);
        modal.spec.encoding.x.time_unit = TimeUnit::None;
        modal.spec.encoding.y.aggregate = Aggregate::None;
        assert_eq!(modal.how(), "");
        modal.spec.encoding.color.field = Some("carrier".to_string());
        assert_eq!(modal.how(), "colored by carrier");
        modal.spec.encoding.color.field = None;
        modal.spec.encoding.y.aggregate = Aggregate::Mean;
        assert_eq!(modal.how(), "mean by date");
        modal.set_mark(Mark::Histogram);
        assert_eq!(modal.how(), "count per bin");
        modal.set_mark(Mark::Kde);
        assert_eq!(modal.how(), "");
        modal.set_mark(Mark::Heatmap);
        assert_eq!(modal.how(), "rows per cell");
        for mark in Mark::ALL {
            modal.set_mark(mark);
            assert!(!modal.how().contains("delay"), "{mark:?}: {}", modal.how());
        }
    }

    /// Reopening from the same column on the same dataset keeps the chart; another
    /// column suggests again.
    #[test]
    fn reopening_from_the_same_column_keeps_the_chart() {
        let mut modal = open_on(Some(("delay", &DataType::Float64)));
        modal.set_mark(Mark::Kde);
        modal.toggle_grid();
        modal.close();
        let c = cols();
        let columns = ChartColumns {
            numeric: &c.numeric,
            datetime: &c.datetime,
            bucketable: &c.datetime,
            category: &c.category,
        };
        modal.open(columns, Some(("delay", &DataType::Float64)), None, false, 1);
        assert_eq!(modal.mark(), Mark::Kde);
        assert!(modal.grid);
        modal.open(
            columns,
            Some(("carrier", &DataType::String)),
            None,
            false,
            1,
        );
        assert_eq!(modal.mark(), Mark::Bar);
        assert!(!modal.grid);
    }

    #[test]
    fn the_y_picker_leaves_out_x_and_caps_its_series() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.spec.encoding.x.field = Some("delay".to_string());
        modal.spec.encoding.y.field.clear();
        modal.focus = ChartFocus::Y;
        modal.open_picker();
        assert_eq!(modal.picker.as_ref().unwrap().items(), ["distance", "year"]);
        assert!(modal.picker_multi());
        modal.picker_choose();
        assert_eq!(
            modal.y(),
            ["distance"],
            "Enter on a fresh list takes the cursor"
        );
    }

    #[test]
    fn number_rows_route_by_the_type() {
        let mut modal = open_on(Some(("delay", &DataType::Float64)));
        modal.focus = ChartFocus::Bins;
        modal.adjust_number_row(1);
        assert_eq!(modal.hist_bins, HISTOGRAM_DEFAULT_BINS + 1);
        modal.set_mark(Mark::Heatmap);
        modal.focus = ChartFocus::Bins;
        modal.adjust_number_row(-1);
        assert_eq!(modal.heatmap_bins, HEATMAP_DEFAULT_BINS - 1);
    }

    /// Rows: ←/→ switch between a sample and every row, a typed size edits the
    /// sample, and nothing reaches what the chart reads until Enter or focus leaves.
    #[test]
    fn rows_change_is_read_on_enter() {
        let mut modal = open_on(Some(("delay", &DataType::Float64)));
        modal.view_rows = Some(36_800_000);
        modal.focus = ChartFocus::LimitRows;
        assert_eq!(modal.row_limit, Some(10_000));
        modal.step(ChartFocus::LimitRows, 1);
        assert!(modal.rows_shown().every && modal.rows_pending());
        assert_eq!(modal.row_limit, Some(10_000), "pending until Enter");
        modal.step(ChartFocus::LimitRows, -1);
        assert!(!modal.rows_pending(), "back where it was");
        modal.step(ChartFocus::LimitRows, 1);
        assert!(modal.commit_rows());
        assert_eq!(modal.row_limit, None);

        // Typing a size chooses Sample; Backspace edits; Esc puts it back.
        for c in "250kx".chars() {
            modal.type_rows(c);
        }
        modal.backspace_rows();
        assert_eq!(modal.rows_shown().typed.as_deref(), Some("250k"));
        assert!(!modal.rows_shown().every);
        modal.discard_rows();
        assert_eq!(modal.row_limit, None);
        assert!(modal.rows_shown().every);

        for c in "250k".chars() {
            modal.type_rows(c);
        }
        assert!(modal.commit_rows());
        assert_eq!(modal.row_limit, Some(250_000));
        assert_eq!(modal.rows_draft, None);

        // A size it cannot read stays on the row with why, and changes nothing.
        modal.type_rows('0');
        assert!(!modal.commit_rows());
        assert!(modal.rows_shown().error.is_some());
        assert_eq!(modal.row_limit, Some(250_000));
        modal.backspace_rows();
        modal.type_rows('2');
        modal.type_rows('m');
        // Leaving the row reads it.
        crate::form::Form::focus(&mut modal, ChartFocus::Type);
        assert_eq!(modal.row_limit, Some(2_000_000));

        // At least every row is Every row; Sample remembers its size.
        modal.focus = ChartFocus::LimitRows;
        for c in "40m".chars() {
            modal.type_rows(c);
        }
        assert!(modal.commit_rows());
        assert_eq!(modal.row_limit, None);
        modal.step(ChartFocus::LimitRows, 1);
        assert!(modal.commit_rows());
        assert_eq!(modal.row_limit, Some(2_000_000));
    }

    /// A bucket goes with its aggregate: none takes the bucket away. A time of day
    /// has no bucket to pick.
    #[test]
    fn a_bucket_needs_an_aggregate_and_a_date() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.step(ChartFocus::TimeUnit, 3);
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::Month);
        modal.spec.encoding.y.aggregate = Aggregate::Sum;
        step_to(&mut modal, Aggregate::None, -1);
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::None);

        let numeric = s(&["delay"]);
        let datetime = s(&["date", "clock"]);
        let mut modal = ChartModal::new();
        modal.open(
            ChartColumns {
                numeric: &numeric,
                datetime: &datetime,
                bucketable: &datetime[..1],
                category: &[],
            },
            Some(("clock", &DataType::Time)),
            None,
            false,
            1,
        );
        assert_eq!(modal.mark(), Mark::Line);
        assert!(!modal.row_order().contains(&ChartFocus::TimeUnit));
    }

    /// An aggregate over a date X starts by the day: a group per raw value would be
    /// close to a group per row.
    #[test]
    fn an_aggregate_on_a_date_starts_by_the_day() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::None);
        modal.step(ChartFocus::Aggregate, 1);
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::Day);
        step_to(&mut modal, Aggregate::Mean, 1);
        assert_eq!(modal.spec.encoding.x.time_unit, TimeUnit::Day);
        modal.step(ChartFocus::TimeUnit, -1);
        assert_eq!(
            modal.spec.encoding.x.time_unit,
            TimeUnit::None,
            "still the user's call"
        );
    }

    /// An integer histogram turned into a box: its column is the box's value, and
    /// not also its category.
    #[test]
    fn a_box_from_an_integer_histogram_has_no_category() {
        let mut modal = open_on(Some(("year", &DataType::Int64)));
        modal.set_mark(Mark::Box);
        assert_eq!(modal.y(), ["year"]);
        assert!(modal.x().is_none());
    }

    /// Whether color splits a chart reads the spec charted: a second Y previewed in
    /// the picker dims the color.
    #[test]
    fn color_use_reads_the_spec_charted() {
        let mut modal = open_on(Some(("date", &DataType::Date)));
        modal.spec.encoding.color.field = Some("carrier".to_string());
        assert!(modal.colored());
        let mut spec = modal.spec.clone();
        spec.encoding.y.field.push("distance".to_string());
        assert!(!ChartModal::colored_in(&spec));
    }
}

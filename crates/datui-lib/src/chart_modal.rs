//! Chart view state: chart type, axis columns, and options.
//!
//! The options form is a flat list of rows per chart kind; column rows are
//! edited through the one shared Picker, so the state here is the choices
//! themselves plus which row holds focus.

use crate::chart_data::{BarOrder, BarValue, ValueRange};
use crate::widgets::ui::PickerState;

/// Chart kind: full chart category shown as tabs, switched with 1-6 or [ ].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartKind {
    #[default]
    XY,
    Histogram,
    BoxPlot,
    Kde,
    Heatmap,
    /// One horizontal bar per category.
    Bar,
}

impl ChartKind {
    pub const ALL: [Self; 6] = [
        Self::XY,
        Self::Histogram,
        Self::BoxPlot,
        Self::Kde,
        Self::Heatmap,
        Self::Bar,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::XY => "XY",
            Self::Histogram => "Histogram",
            Self::BoxPlot => "Box Plot",
            Self::Kde => "KDE",
            Self::Heatmap => "Heatmap",
            Self::Bar => "Bar",
        }
    }
}

/// XY chart type: Line, Scatter, or Bar (maps to ratatui GraphType).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartType {
    #[default]
    Line,
    Scatter,
    Bar,
}

impl ChartType {
    pub const ALL: [Self; 3] = [Self::Line, Self::Scatter, Self::Bar];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Line => "Line",
            Self::Scatter => "Scatter",
            Self::Bar => "Bar",
        }
    }
}

/// Focus: one row of the active chart kind's options form. The tab bar is not
/// focusable — the chart kind switches from anywhere with 1-6 and [ ].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartFocus {
    /// XY plot style: Line / Scatter / Bar, cycled in place.
    #[default]
    Style,
    /// XY x-axis column (single pick).
    XColumn,
    /// XY y-axis series (toggle up to `Y_SERIES_MAX`).
    YColumns,
    YStartsAtZero,
    LogScale,
    ShowLegend,
    /// The grid at the major ticks: XY, Histogram, Box Plot and KDE.
    Grid,
    /// The value column of the Histogram, Box Plot, or KDE on screen.
    Column,
    /// Heatmap axis columns (single pick each).
    HeatmapX,
    HeatmapY,
    /// Bin count of the Histogram or Heatmap on screen.
    Bins,
    /// KDE bandwidth multiplier.
    Bandwidth,
    /// Which values the Histogram, Box Plot, or KDE draws: all, or a percentile range.
    Range,
    /// Bar chart: the category column, one bar per value (single pick).
    Category,
    /// Bar chart: what sets each bar's length, the rows per category or a numeric
    /// column (single pick).
    Value,
    /// Bar chart: bars by value or by label.
    Order,
    /// Sample size shared by every chart kind; the last row of each form.
    LimitRows,
}

/// Maximum number of y-axis series that can be selected (remembered).
pub const Y_SERIES_MAX: usize = 7;

/// Default histogram bin count.
pub const HISTOGRAM_DEFAULT_BINS: usize = 20;
pub const HISTOGRAM_MIN_BINS: usize = 5;
pub const HISTOGRAM_MAX_BINS: usize = 80;

/// Default heatmap bin count (applies to both axes).
pub const HEATMAP_DEFAULT_BINS: usize = 20;
pub const HEATMAP_MIN_BINS: usize = 5;
pub const HEATMAP_MAX_BINS: usize = 60;

/// KDE bandwidth multiplier bounds and step.
pub const KDE_BANDWIDTH_MIN: f64 = 0.2;
pub const KDE_BANDWIDTH_MAX: f64 = 5.0;
pub const KDE_BANDWIDTH_STEP: f64 = 0.1;

/// Chart sample size bounds (the Sample size row). User can go down to 0; 0 becomes every row (None).
pub const CHART_ROW_LIMIT_MIN: usize = 0;
/// Maximum applicable limit (Polars slice takes u32).
pub const CHART_ROW_LIMIT_MAX: usize = u32::MAX as usize;
/// PgUp/PgDown step for Sample size.
pub const CHART_ROW_LIMIT_PAGE_STEP: usize = 100_000;
/// Default numeric limit when switching from every row with + or PgUp.
pub const DEFAULT_CHART_ROW_LIMIT: usize = 10_000;
/// Below this limit, +/- step is CHART_ROW_LIMIT_STEP_SMALL; at or above, CHART_ROW_LIMIT_STEP_LARGE.
pub const CHART_ROW_LIMIT_STEP_THRESHOLD: usize = 20_000;
pub const CHART_ROW_LIMIT_STEP_SMALL: i32 = 1_000;
pub const CHART_ROW_LIMIT_STEP_LARGE: i32 = 5_000;

fn format_usize_with_commas(n: usize) -> String {
    let s = n.to_string();
    let len = s.len();
    if len <= 3 {
        return s;
    }
    let first_len = len % 3;
    let first_len = if first_len == 0 { 3 } else { first_len };
    let mut out = s[..first_len].to_string();
    for i in (first_len..len).step_by(3) {
        out.push(',');
        out.push_str(&s[i..i + 3]);
    }
    out
}

/// The view's columns a chart can take, by role.
#[derive(Clone, Copy, Default)]
pub struct ChartColumns<'a> {
    pub numeric: &'a [String],
    pub datetime: &'a [String],
    /// Bar chart categories (see `chart_data::is_category_dtype`).
    pub category: &'a [String],
}

/// Chart view state: chart kind, axes/columns, and options.
#[derive(Default)]
pub struct ChartModal {
    pub active: bool,
    pub chart_kind: ChartKind,
    pub chart_type: ChartType,
    /// Remembered x-axis column (single).
    pub x_column: Option<String>,
    /// Remembered y-axis column names (order = series order; max Y_SERIES_MAX).
    pub y_columns: Vec<String>,
    pub y_starts_at_zero: bool,
    pub log_scale: bool,
    pub show_legend: bool,
    /// The grid at the major ticks, on the kinds with axes (`g`).
    pub grid: bool,
    pub focus: ChartFocus,
    /// The one Picker, open for the focused column row; None while the form
    /// has the keys.
    pub picker: Option<PickerState>,
    /// X-axis candidates: datetime first, then numeric.
    pub x_candidates: Vec<String>,
    /// Numeric columns: the pool for every other column row.
    pub numeric_candidates: Vec<String>,
    /// Bar chart categories: text, categorical, boolean and integer columns.
    pub category_candidates: Vec<String>,
    /// Histogram: remembered column (single selection).
    pub hist_column: Option<String>,
    pub hist_bins: usize,
    /// Box plot: remembered column (single selection).
    pub box_column: Option<String>,
    /// KDE: remembered column (single selection).
    pub kde_column: Option<String>,
    pub kde_bandwidth_factor: f64,
    /// Heatmap: remembered x/y columns (single selection each).
    pub heatmap_x_column: Option<String>,
    pub heatmap_y_column: Option<String>,
    pub heatmap_bins: usize,
    /// Histogram, Box Plot and KDE: which values are drawn.
    pub value_range: ValueRange,
    /// Bar chart: remembered category and value columns, and the bar order.
    pub bar_category: Option<String>,
    pub bar_value: Option<BarValue>,
    pub bar_order: BarOrder,
    /// Rows a chart reads: up to this many, sampled across the table. None = every row.
    pub row_limit: Option<usize>,
    /// The dataset the choices were made on (`App::dataset_generation`). Reopening the
    /// chart on the same dataset — after a sort or a filter — keeps them.
    pub dataset: Option<u64>,
    /// Each column's unit, from a delimited spec's unit row: the axis titles name
    /// them. Set as the chart is drawn.
    pub units: Vec<(String, String)>,
}

impl ChartModal {
    /// An axis title for `column`: its name, and its unit when it has one.
    pub fn axis_title(&self, column: &str) -> String {
        match self.units.iter().find(|(name, _)| name == column) {
            Some((_, unit)) => format!("{column} ({unit})"),
            None => column.to_string(),
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Open the chart view. On a dataset seen before, the chart comes back as it was
    /// left, less any column the view no longer has; otherwise it starts with no
    /// columns picked. `default_row_limit` is the initial Sample size (e.g. from
    /// config); None = every row. `grid` is whether a new chart starts with its grid.
    pub fn open(
        &mut self,
        columns: ChartColumns<'_>,
        default_row_limit: Option<usize>,
        grid: bool,
        dataset: u64,
    ) {
        let ChartColumns {
            numeric: numeric_columns,
            datetime: datetime_columns,
            category: category_columns,
        } = columns;
        self.active = true;
        self.picker = None;
        // x_candidates: datetime first, then numeric (for list order).
        self.x_candidates = datetime_columns.to_vec();
        for c in numeric_columns {
            if !self.x_candidates.contains(c) {
                self.x_candidates.push(c.clone());
            }
        }
        self.numeric_candidates = numeric_columns.to_vec();
        self.category_candidates = category_columns.to_vec();
        if self.dataset == Some(dataset) {
            self.keep_existing_choices();
            self.focus = self.row_order()[0];
            return;
        }
        self.dataset = Some(dataset);
        self.chart_kind = ChartKind::XY;
        self.chart_type = ChartType::Line;
        self.y_starts_at_zero = false;
        self.log_scale = false;
        self.show_legend = true;
        self.grid = grid;
        self.value_range = ValueRange::All;
        self.row_limit = default_row_limit.and_then(|n| {
            if n == 0 {
                None
            } else {
                Some(n.clamp(1, CHART_ROW_LIMIT_MAX))
            }
        });
        self.x_column = None;
        self.y_columns.clear();
        self.hist_column = None;
        self.hist_bins = HISTOGRAM_DEFAULT_BINS;
        self.box_column = None;
        self.kde_column = None;
        self.kde_bandwidth_factor = 1.0;
        self.heatmap_x_column = None;
        self.heatmap_y_column = None;
        self.heatmap_bins = HEATMAP_DEFAULT_BINS;
        self.bar_category = None;
        self.bar_value = None;
        self.bar_order = BarOrder::Value;
        self.focus = self.row_order()[0];
    }

    /// Close the chart view. The choices stay for the next open on this dataset.
    pub fn close(&mut self) {
        self.active = false;
        self.picker = None;
    }

    /// Drop the choices whose columns the view no longer offers, and a Y series that
    /// is the X column.
    fn keep_existing_choices(&mut self) {
        let keep = |choice: &mut Option<String>, pool: &[String]| {
            if choice.as_ref().is_some_and(|c| !pool.contains(c)) {
                *choice = None;
            }
        };
        keep(&mut self.x_column, &self.x_candidates);
        keep(&mut self.hist_column, &self.numeric_candidates);
        keep(&mut self.box_column, &self.numeric_candidates);
        keep(&mut self.kde_column, &self.numeric_candidates);
        keep(&mut self.heatmap_x_column, &self.numeric_candidates);
        keep(&mut self.heatmap_y_column, &self.numeric_candidates);
        keep(&mut self.bar_category, &self.category_candidates);
        if let Some(BarValue::Column(c)) = &self.bar_value
            && (!self.numeric_candidates.contains(c) || Some(c) == self.bar_category.as_ref())
        {
            self.bar_value = None;
        }
        let (numeric, x) = (&self.numeric_candidates, &self.x_column);
        self.y_columns
            .retain(|c| numeric.contains(c) && Some(c) != x.as_ref());
    }

    // ----- Focus -----

    /// The active chart kind's rows, in Tab order.
    pub fn row_order(&self) -> &'static [ChartFocus] {
        use ChartFocus::*;
        match self.chart_kind {
            ChartKind::XY => &[
                Style,
                XColumn,
                YColumns,
                YStartsAtZero,
                LogScale,
                ShowLegend,
                Grid,
                LimitRows,
            ],
            ChartKind::Histogram => &[Column, Bins, Range, Grid, LimitRows],
            ChartKind::BoxPlot => &[Column, Range, Grid, LimitRows],
            ChartKind::Kde => &[Column, Bandwidth, Range, Grid, LimitRows],
            ChartKind::Heatmap => &[HeatmapX, HeatmapY, Bins, LimitRows],
            ChartKind::Bar => &[Category, Value, Order, LimitRows],
        }
    }

    pub fn next_focus(&mut self) {
        let order = self.row_order();
        self.focus = match order.iter().position(|&f| f == self.focus) {
            Some(pos) => order[(pos + 1) % order.len()],
            None => order[0],
        };
    }

    pub fn prev_focus(&mut self) {
        let order = self.row_order();
        self.focus = match order.iter().position(|&f| f == self.focus) {
            Some(pos) if pos > 0 => order[pos - 1],
            Some(_) => order[order.len() - 1],
            None => order[0],
        };
    }

    /// Switch the chart kind directly (the 1-6 keys). Focus lands on the new
    /// form's first row; a Picker open for the old form dies with it.
    pub fn set_chart_kind(&mut self, kind: ChartKind) {
        self.chart_kind = kind;
        self.picker = None;
        self.focus = self.row_order()[0];
    }

    pub fn next_chart_kind(&mut self) {
        let idx = ChartKind::ALL
            .iter()
            .position(|&k| k == self.chart_kind)
            .unwrap_or(0);
        self.set_chart_kind(ChartKind::ALL[(idx + 1) % ChartKind::ALL.len()]);
    }

    pub fn prev_chart_kind(&mut self) {
        let idx = ChartKind::ALL
            .iter()
            .position(|&k| k == self.chart_kind)
            .unwrap_or(0);
        let prev = if idx == 0 {
            ChartKind::ALL.len() - 1
        } else {
            idx - 1
        };
        self.set_chart_kind(ChartKind::ALL[prev]);
    }

    // ----- Rows -----

    /// Rows edited through the Picker.
    pub fn is_picker_row(&self, focus: ChartFocus) -> bool {
        matches!(
            focus,
            ChartFocus::XColumn
                | ChartFocus::YColumns
                | ChartFocus::Column
                | ChartFocus::HeatmapX
                | ChartFocus::HeatmapY
                | ChartFocus::Category
                | ChartFocus::Value
        )
    }

    /// Rows where the Picker toggles several choices rather than picking one.
    pub fn is_multi_row(&self, focus: ChartFocus) -> bool {
        focus == ChartFocus::YColumns
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

    /// Whether the chart on screen has axes to draw a grid on: the heatmap's cells
    /// and the bar chart's rows have none.
    pub fn has_grid(&self) -> bool {
        self.row_order().contains(&ChartFocus::Grid)
    }

    pub fn is_number_row(&self, focus: ChartFocus) -> bool {
        matches!(
            focus,
            ChartFocus::Bins | ChartFocus::Bandwidth | ChartFocus::LimitRows
        )
    }

    // ----- Picker -----

    /// What the focused row's Picker offers. The Y series leave out the X column:
    /// charted against itself it is only a diagonal. The bar value offers Count first,
    /// then the numeric columns less the category, for the same reason.
    pub fn picker_items(&self) -> Vec<String> {
        match self.focus {
            ChartFocus::XColumn => self.x_candidates.clone(),
            ChartFocus::YColumns => self
                .numeric_candidates
                .iter()
                .filter(|c| Some(*c) != self.x_column.as_ref())
                .cloned()
                .collect(),
            ChartFocus::Column | ChartFocus::HeatmapX | ChartFocus::HeatmapY => {
                self.numeric_candidates.clone()
            }
            ChartFocus::Category => self.category_candidates.clone(),
            ChartFocus::Value => std::iter::once(BarValue::Count.label().to_string())
                .chain(
                    self.numeric_candidates
                        .iter()
                        .filter(|c| Some(*c) != self.bar_category.as_ref())
                        .cloned(),
                )
                .collect(),
            _ => Vec::new(),
        }
    }

    /// The current single choice of the focused row, for opening the Picker on it.
    fn focused_row_choice(&self) -> Option<&str> {
        match self.focus {
            ChartFocus::XColumn => self.x_column.as_deref(),
            ChartFocus::YColumns => self.y_columns.first().map(|s| s.as_str()),
            ChartFocus::Column => match self.chart_kind {
                ChartKind::Histogram => self.hist_column.as_deref(),
                ChartKind::BoxPlot => self.box_column.as_deref(),
                ChartKind::Kde => self.kde_column.as_deref(),
                _ => None,
            },
            ChartFocus::HeatmapX => self.heatmap_x_column.as_deref(),
            ChartFocus::HeatmapY => self.heatmap_y_column.as_deref(),
            ChartFocus::Category => self.bar_category.as_deref(),
            _ => None,
        }
    }

    /// Where the focused row's current choice sits in `items`, the Picker's list.
    fn focused_row_index(&self, items: &[String]) -> Option<usize> {
        if self.focus == ChartFocus::Value {
            // By place, not name: Count is first, whatever the columns are called.
            return match self.bar_value.as_ref()? {
                BarValue::Count => Some(0),
                BarValue::Column(c) => items.iter().skip(1).position(|i| i == c).map(|i| i + 1),
            };
        }
        let current = self.focused_row_choice()?;
        items.iter().position(|item| item == current)
    }

    /// Open the Picker for the focused row, cursor on the current choice.
    pub fn open_picker(&mut self) {
        if !self.is_picker_row(self.focus) {
            return;
        }
        let items = self.picker_items();
        let mut state = PickerState::new(items.clone());
        if let Some(i) = self.focused_row_index(&items) {
            state.select_original(i);
        }
        self.picker = Some(state);
    }

    /// The item under the open Picker's cursor.
    fn picker_cursor_item(&self) -> Option<String> {
        let i = self.picker.as_ref()?.selected_original()?;
        self.picker_items().get(i).cloned()
    }

    /// The bar value under the open Value Picker's cursor: Count is its first item.
    fn picker_cursor_bar_value(&self) -> Option<BarValue> {
        if self.focus != ChartFocus::Value {
            return None;
        }
        let i = self.picker.as_ref()?.selected_original()?;
        if i == 0 {
            return Some(BarValue::Count);
        }
        self.picker_items().get(i).cloned().map(BarValue::Column)
    }

    /// Enter in the Picker: a pick-one row takes the cursor's item; the Y row
    /// keeps its toggled choices, or adopts the cursor's item when none are
    /// toggled, so Enter on a fresh list still charts something. Either way
    /// the Picker closes.
    pub fn picker_choose(&mut self) {
        let bar_value = self.picker_cursor_bar_value();
        let Some(item) = self.picker_cursor_item() else {
            self.picker = None;
            return;
        };
        self.picker = None;
        match self.focus {
            ChartFocus::XColumn => {
                // A series cannot be the X axis too.
                self.y_columns.retain(|c| *c != item);
                self.x_column = Some(item);
            }
            ChartFocus::YColumns => {
                if self.y_columns.is_empty() {
                    self.y_columns.push(item);
                }
            }
            ChartFocus::Column => match self.chart_kind {
                ChartKind::Histogram => self.hist_column = Some(item),
                ChartKind::BoxPlot => self.box_column = Some(item),
                ChartKind::Kde => self.kde_column = Some(item),
                _ => {}
            },
            ChartFocus::HeatmapX => self.heatmap_x_column = Some(item),
            ChartFocus::HeatmapY => self.heatmap_y_column = Some(item),
            ChartFocus::Category => {
                // The value cannot be the category too.
                if matches!(&self.bar_value, Some(BarValue::Column(c)) if *c == item) {
                    self.bar_value = None;
                }
                self.bar_category = Some(item);
            }
            ChartFocus::Value => self.bar_value = bar_value,
            _ => {}
        }
    }

    /// Space in the Y row's Picker: flip the cursor's series in or out, up to
    /// `Y_SERIES_MAX`.
    pub fn picker_toggle(&mut self) {
        if !self.is_multi_row(self.focus) {
            return;
        }
        let Some(item) = self.picker_cursor_item() else {
            return;
        };
        if let Some(pos) = self.y_columns.iter().position(|c| *c == item) {
            self.y_columns.remove(pos);
        } else if self.y_columns.len() < Y_SERIES_MAX {
            self.y_columns.push(item);
        }
    }

    /// Whether an item in the Y row's Picker is currently a series.
    pub fn is_marked(&self, item: &str) -> bool {
        self.focus == ChartFocus::YColumns && self.y_columns.iter().any(|c| c == item)
    }

    // ----- Effective selections (what gets charted right now) -----

    /// Effective x column for chart/export: the remembered x (no preview on scroll).
    pub fn effective_x_column(&self) -> Option<&String> {
        self.x_column.as_ref()
    }

    /// Effective y columns: the toggled series, plus the Y Picker cursor's
    /// item as a preview while that Picker is open.
    pub fn effective_y_columns(&self) -> Vec<String> {
        let mut out = self.y_columns.clone();
        if self.focus == ChartFocus::YColumns
            && let Some(item) = self.picker_cursor_item()
            && !out.contains(&item)
        {
            out.push(item);
        }
        out
    }

    /// The value column a single-column kind would chart right now: the open
    /// Picker's cursor previews; otherwise the remembered choice.
    fn effective_single(&self, kind: ChartKind, remembered: &Option<String>) -> Option<String> {
        if self.chart_kind == kind
            && self.focus == ChartFocus::Column
            && let Some(item) = self.picker_cursor_item()
        {
            return Some(item);
        }
        remembered.clone()
    }

    pub fn effective_hist_column(&self) -> Option<String> {
        self.effective_single(ChartKind::Histogram, &self.hist_column)
    }

    pub fn effective_box_column(&self) -> Option<String> {
        self.effective_single(ChartKind::BoxPlot, &self.box_column)
    }

    pub fn effective_kde_column(&self) -> Option<String> {
        self.effective_single(ChartKind::Kde, &self.kde_column)
    }

    pub fn effective_heatmap_x_column(&self) -> Option<String> {
        if self.focus == ChartFocus::HeatmapX
            && let Some(item) = self.picker_cursor_item()
        {
            return Some(item);
        }
        self.heatmap_x_column.clone()
    }

    pub fn effective_heatmap_y_column(&self) -> Option<String> {
        if self.focus == ChartFocus::HeatmapY
            && let Some(item) = self.picker_cursor_item()
        {
            return Some(item);
        }
        self.heatmap_y_column.clone()
    }

    /// The bar chart's category: the open Picker's cursor previews.
    pub fn effective_bar_category(&self) -> Option<String> {
        if self.focus == ChartFocus::Category
            && let Some(item) = self.picker_cursor_item()
        {
            return Some(item);
        }
        self.bar_category.clone()
    }

    /// The bar chart's value: the open Picker's cursor previews, and a value column
    /// that is the category is none.
    pub fn effective_bar_value(&self) -> Option<BarValue> {
        if let Some(value) = self.picker_cursor_bar_value() {
            return Some(value);
        }
        let category = self.effective_bar_category();
        self.bar_value.clone().filter(|v| match v {
            BarValue::Count => true,
            BarValue::Column(c) => Some(c) != category.as_ref(),
        })
    }

    // ----- Toggles and numbers -----

    pub fn toggle_y_starts_at_zero(&mut self) {
        self.y_starts_at_zero = !self.y_starts_at_zero;
    }

    pub fn toggle_log_scale(&mut self) {
        self.log_scale = !self.log_scale;
    }

    pub fn toggle_show_legend(&mut self) {
        self.show_legend = !self.show_legend;
    }

    pub fn toggle_grid(&mut self) {
        self.grid = !self.grid;
    }

    /// Cycle chart type: Line -> Scatter -> Bar -> Line.
    pub fn next_chart_type(&mut self) {
        self.chart_type = match self.chart_type {
            ChartType::Line => ChartType::Scatter,
            ChartType::Scatter => ChartType::Bar,
            ChartType::Bar => ChartType::Line,
        };
    }

    pub fn prev_chart_type(&mut self) {
        self.chart_type = match self.chart_type {
            ChartType::Line => ChartType::Bar,
            ChartType::Scatter => ChartType::Line,
            ChartType::Bar => ChartType::Scatter,
        };
    }

    /// Step the bar order (the Order row) forward or back.
    pub fn cycle_bar_order(&mut self, delta: i32) {
        let all = BarOrder::ALL;
        let i = all.iter().position(|&o| o == self.bar_order).unwrap_or(0) as i32;
        let n = all.len() as i32;
        self.bar_order = all[(i + delta).rem_euclid(n) as usize];
    }

    /// Step the value range (the Range row) forward or back.
    pub fn cycle_value_range(&mut self, delta: i32) {
        let all = ValueRange::ALL;
        let i = all.iter().position(|&r| r == self.value_range).unwrap_or(0) as i32;
        let n = all.len() as i32;
        self.value_range = all[(i + delta).rem_euclid(n) as usize];
    }

    /// Display string for Sample size: "Every row" or number with commas.
    pub fn row_limit_display(&self) -> String {
        match self.row_limit {
            None => "Every row".to_string(),
            Some(n) => format_usize_with_commas(n),
        }
    }

    pub fn adjust_hist_bins(&mut self, delta: i32) {
        let next = (self.hist_bins as i32 + delta)
            .clamp(HISTOGRAM_MIN_BINS as i32, HISTOGRAM_MAX_BINS as i32);
        self.hist_bins = next as usize;
    }

    pub fn adjust_heatmap_bins(&mut self, delta: i32) {
        let next = (self.heatmap_bins as i32 + delta)
            .clamp(HEATMAP_MIN_BINS as i32, HEATMAP_MAX_BINS as i32);
        self.heatmap_bins = next as usize;
    }

    pub fn adjust_kde_bandwidth_factor(&mut self, delta: f64) {
        let next = (self.kde_bandwidth_factor + delta).clamp(KDE_BANDWIDTH_MIN, KDE_BANDWIDTH_MAX);
        self.kde_bandwidth_factor = (next * 10.0).round() / 10.0;
    }

    /// Adjust the focused number row: bins, bandwidth, or the row limit,
    /// whichever the active form shows.
    pub fn adjust_number_row(&mut self, delta: i32) {
        match self.focus {
            ChartFocus::Bins if self.chart_kind == ChartKind::Histogram => {
                self.adjust_hist_bins(delta)
            }
            ChartFocus::Bins if self.chart_kind == ChartKind::Heatmap => {
                self.adjust_heatmap_bins(delta)
            }
            ChartFocus::Bandwidth => {
                self.adjust_kde_bandwidth_factor(delta as f64 * KDE_BANDWIDTH_STEP)
            }
            ChartFocus::Range => self.cycle_value_range(delta),
            ChartFocus::Order => self.cycle_bar_order(delta),
            ChartFocus::LimitRows => self.adjust_row_limit(delta),
            _ => {}
        }
    }

    /// Adjust row limit by delta (+/-). Step size depends on current value. None = unlimited.
    pub fn adjust_row_limit(&mut self, delta: i32) {
        let current = match self.row_limit {
            None if delta > 0 => {
                self.row_limit = Some(DEFAULT_CHART_ROW_LIMIT);
                return;
            }
            None => return,
            Some(n) => n,
        };
        let step = if current < CHART_ROW_LIMIT_STEP_THRESHOLD {
            CHART_ROW_LIMIT_STEP_SMALL as usize
        } else {
            CHART_ROW_LIMIT_STEP_LARGE as usize
        };
        let next = match delta.cmp(&0) {
            std::cmp::Ordering::Greater => current.saturating_add(step).min(CHART_ROW_LIMIT_MAX),
            std::cmp::Ordering::Less => current.saturating_sub(step),
            std::cmp::Ordering::Equal => current,
        };
        self.row_limit = if next == 0 { None } else { Some(next) };
    }

    /// Adjust row limit by 100,000 (PgUp / PgDown). None = unlimited.
    pub fn adjust_row_limit_page(&mut self, delta: i32) {
        let current = match self.row_limit {
            None if delta > 0 => {
                self.row_limit = Some(DEFAULT_CHART_ROW_LIMIT);
                return;
            }
            None => return,
            Some(n) => n,
        };
        let step = CHART_ROW_LIMIT_PAGE_STEP;
        let next = match delta.cmp(&0) {
            std::cmp::Ordering::Greater => current.saturating_add(step).min(CHART_ROW_LIMIT_MAX),
            std::cmp::Ordering::Less => current.saturating_sub(step),
            std::cmp::Ordering::Equal => current,
        };
        self.row_limit = if next == 0 { None } else { Some(next) };
    }

    pub fn can_export(&self) -> bool {
        match self.chart_kind {
            ChartKind::XY => {
                self.effective_x_column().is_some() && !self.effective_y_columns().is_empty()
            }
            ChartKind::Histogram => self.effective_hist_column().is_some(),
            ChartKind::BoxPlot => self.effective_box_column().is_some(),
            ChartKind::Kde => self.effective_kde_column().is_some(),
            ChartKind::Heatmap => {
                self.effective_heatmap_x_column().is_some()
                    && self.effective_heatmap_y_column().is_some()
            }
            ChartKind::Bar => {
                self.effective_bar_category().is_some() && self.effective_bar_value().is_some()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    /// An axis is titled with its column's unit, when a delimited spec read one.
    #[test]
    fn an_axis_title_names_the_unit() {
        let mut modal = ChartModal::default();
        assert_eq!(modal.axis_title("cht1"), "cht1");
        modal.units = vec![("cht1".to_string(), "deg F".to_string())];
        assert_eq!(modal.axis_title("cht1"), "cht1 (deg F)");
        assert_eq!(modal.axis_title("volts"), "volts");
    }

    use super::{ChartColumns, ChartFocus, ChartKind, ChartModal, ChartType, Y_SERIES_MAX};
    use crate::chart_data::{BarOrder, BarValue};

    fn columns<'a>(numeric: &'a [String], datetime: &'a [String]) -> ChartColumns<'a> {
        ChartColumns {
            numeric,
            datetime,
            category: &[],
        }
    }

    fn open_modal() -> ChartModal {
        let numeric = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let datetime = vec!["date".to_string()];
        let mut modal = ChartModal::new();
        modal.open(columns(&numeric, &datetime), Some(10_000), false, 1);
        modal
    }

    #[test]
    fn open_no_default_columns() {
        let modal = open_modal();
        assert!(modal.active);
        assert_eq!(modal.chart_kind, ChartKind::XY);
        assert_eq!(modal.chart_type, ChartType::Line);
        assert!(modal.x_column.is_none());
        assert!(modal.y_columns.is_empty());
        assert!(!modal.y_starts_at_zero);
        assert!(!modal.log_scale);
        assert!(modal.show_legend);
        assert!(modal.picker.is_none());
        assert_eq!(modal.focus, ChartFocus::Style);
        assert_eq!(modal.row_limit, Some(10_000));
        assert_eq!(modal.x_candidates, ["date", "a", "b", "c"]);
        assert_eq!(modal.numeric_candidates, ["a", "b", "c"]);
    }

    #[test]
    fn toggles_persist() {
        let mut modal = open_modal();
        assert!(!modal.y_starts_at_zero);
        modal.toggle_y_starts_at_zero();
        assert!(modal.y_starts_at_zero);
        modal.toggle_log_scale();
        assert!(modal.log_scale);
        modal.toggle_show_legend();
        assert!(!modal.show_legend);
    }

    #[test]
    fn tab_walks_the_xy_rows_and_wraps() {
        let mut modal = open_modal();
        let walked: Vec<ChartFocus> = (0..8)
            .map(|_| {
                modal.next_focus();
                modal.focus
            })
            .collect();
        assert_eq!(
            walked,
            vec![
                ChartFocus::XColumn,
                ChartFocus::YColumns,
                ChartFocus::YStartsAtZero,
                ChartFocus::LogScale,
                ChartFocus::ShowLegend,
                ChartFocus::Grid,
                ChartFocus::LimitRows,
                ChartFocus::Style,
            ]
        );
        modal.prev_focus();
        assert_eq!(modal.focus, ChartFocus::LimitRows);
    }

    /// Switching the chart kind lands focus on the new form's first row and
    /// closes any Picker — the row it was scoped to is gone.
    #[test]
    fn switching_kind_resets_focus_and_closes_the_picker() {
        let mut modal = open_modal();
        modal.focus = ChartFocus::XColumn;
        modal.open_picker();
        assert!(modal.picker.is_some());
        modal.set_chart_kind(ChartKind::Kde);
        assert!(modal.picker.is_none());
        assert_eq!(modal.focus, ChartFocus::Column);
        modal.next_chart_kind();
        assert_eq!(modal.chart_kind, ChartKind::Heatmap);
        assert_eq!(modal.focus, ChartFocus::HeatmapX);
        modal.next_chart_kind();
        assert_eq!(modal.chart_kind, ChartKind::Bar);
        assert_eq!(modal.focus, ChartFocus::Category);
        modal.next_chart_kind();
        assert_eq!(modal.chart_kind, ChartKind::XY, "wraps");
        modal.prev_chart_kind();
        modal.prev_chart_kind();
        modal.prev_chart_kind();
        assert_eq!(modal.chart_kind, ChartKind::Kde);
    }

    /// Each kind keeps its own column choice while the tabs change.
    #[test]
    fn choices_survive_kind_switches() {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Histogram);
        modal.open_picker();
        modal.picker_choose(); // "a", the cursor's initial item
        assert_eq!(modal.hist_column.as_deref(), Some("a"));
        modal.set_chart_kind(ChartKind::Kde);
        assert!(modal.kde_column.is_none());
        modal.set_chart_kind(ChartKind::Histogram);
        assert_eq!(modal.hist_column.as_deref(), Some("a"));
    }

    #[test]
    fn the_picker_opens_on_the_current_choice() {
        let mut modal = open_modal();
        modal.x_column = Some("b".to_string());
        modal.focus = ChartFocus::XColumn;
        modal.open_picker();
        // x candidates are [date, a, b, c]; the cursor sits on the choice.
        assert_eq!(modal.picker.as_ref().unwrap().selected_original(), Some(2));
    }

    #[test]
    fn y_picker_toggles_and_caps_at_the_series_max() {
        let cols: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        let mut modal = ChartModal::new();
        modal.open(columns(&cols, &[]), Some(10_000), false, 1);
        modal.focus = ChartFocus::YColumns;
        modal.open_picker();
        for _ in 0..=Y_SERIES_MAX {
            modal.picker_toggle();
            if let Some(p) = modal.picker.as_mut() {
                p.move_down();
            }
        }
        assert_eq!(modal.y_columns.len(), Y_SERIES_MAX, "capped");
        // Toggling a chosen series off works.
        modal.picker.as_mut().unwrap().select_original(0);
        modal.picker_toggle();
        assert_eq!(modal.y_columns.len(), Y_SERIES_MAX - 1);
    }

    /// Enter on a fresh Y Picker adopts the cursor's item, so the first
    /// series never has to be toggled explicitly.
    #[test]
    fn choosing_on_an_empty_y_row_takes_the_cursor_item() {
        let mut modal = open_modal();
        modal.focus = ChartFocus::YColumns;
        modal.open_picker();
        modal.picker_choose();
        assert_eq!(modal.y_columns, ["a"]);
        assert!(modal.picker.is_none());
        // With series already chosen, Enter just closes.
        modal.open_picker();
        modal.picker.as_mut().unwrap().move_down();
        modal.picker_choose();
        assert_eq!(modal.y_columns, ["a"]);
    }

    /// The open Picker's cursor previews: the chart is prepared for what the
    /// cursor is on, before anything is committed.
    #[test]
    fn the_picker_cursor_previews_the_selection() {
        let mut modal = open_modal();
        modal.focus = ChartFocus::YColumns;
        modal.open_picker();
        assert_eq!(modal.effective_y_columns(), ["a"]);
        modal.picker.as_mut().unwrap().move_down();
        assert_eq!(modal.effective_y_columns(), ["b"]);
        modal.picker = None;
        assert!(modal.effective_y_columns().is_empty(), "no preview closed");

        modal.set_chart_kind(ChartKind::Histogram);
        assert_eq!(modal.effective_hist_column(), None);
        modal.open_picker();
        assert_eq!(modal.effective_hist_column().as_deref(), Some("a"));
        // The x column never previews: only the remembered choice charts.
        modal.set_chart_kind(ChartKind::XY);
        modal.focus = ChartFocus::XColumn;
        modal.open_picker();
        assert_eq!(modal.effective_x_column(), None);
    }

    /// The X column is not offered as a Y series, and picking a series as X takes
    /// it out of the series.
    #[test]
    fn the_x_column_is_not_a_y_choice() {
        let mut modal = open_modal();
        modal.x_column = Some("a".to_string());
        modal.focus = ChartFocus::YColumns;
        assert_eq!(modal.picker_items(), ["b", "c"]);

        modal.y_columns = vec!["b".to_string(), "c".to_string()];
        modal.focus = ChartFocus::XColumn;
        modal.open_picker();
        modal.picker.as_mut().unwrap().select_original(2); // b
        modal.picker_choose();
        assert_eq!(modal.x_column.as_deref(), Some("b"));
        assert_eq!(modal.y_columns, ["c"]);
    }

    /// Reopening on the same dataset keeps the chart as it was left, less columns
    /// the view no longer has; another dataset starts clean.
    #[test]
    fn reopening_keeps_choices_while_their_columns_exist() {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Histogram);
        modal.x_column = Some("date".to_string());
        modal.y_columns = vec!["a".to_string(), "b".to_string()];
        modal.hist_column = Some("c".to_string());
        modal.value_range = crate::chart_data::ValueRange::Percentile1To99;
        modal.toggle_grid();
        modal.close();

        let numeric = vec!["a".to_string(), "c".to_string()];
        let datetime = vec!["date".to_string()];
        modal.open(columns(&numeric, &datetime), Some(10_000), false, 1);
        assert_eq!(modal.chart_kind, ChartKind::Histogram);
        assert_eq!(modal.x_column.as_deref(), Some("date"));
        assert_eq!(modal.y_columns, ["a"], "b is gone from the view");
        assert_eq!(modal.hist_column.as_deref(), Some("c"));
        assert_eq!(
            modal.value_range,
            crate::chart_data::ValueRange::Percentile1To99
        );
        assert!(modal.grid, "the grid stays as it was left");
        modal.close();

        modal.open(columns(&numeric, &datetime), Some(10_000), false, 2);
        assert!(!modal.grid, "a new dataset starts from the config");
        modal.open(columns(&numeric, &datetime), Some(10_000), true, 3);
        assert!(modal.grid);
        assert_eq!(modal.chart_kind, ChartKind::XY);
        assert!(modal.x_column.is_none() && modal.y_columns.is_empty());
        assert!(modal.hist_column.is_none());
    }

    #[test]
    fn the_range_row_cycles() {
        use crate::chart_data::ValueRange;
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::BoxPlot);
        assert_eq!(
            modal.row_order(),
            [
                ChartFocus::Column,
                ChartFocus::Range,
                ChartFocus::Grid,
                ChartFocus::LimitRows
            ]
        );
        modal.focus = ChartFocus::Range;
        modal.adjust_number_row(1);
        assert_eq!(modal.value_range, ValueRange::Percentile1To99);
        modal.adjust_number_row(1);
        assert_eq!(modal.value_range, ValueRange::All);
        modal.adjust_number_row(-1);
        assert_eq!(modal.value_range, ValueRange::Percentile1To99);
    }

    #[test]
    fn adjust_number_row_routes_by_the_visible_form() {
        let mut modal = open_modal();
        modal.set_chart_kind(ChartKind::Histogram);
        modal.focus = ChartFocus::Bins;
        modal.adjust_number_row(1);
        assert_eq!(modal.hist_bins, super::HISTOGRAM_DEFAULT_BINS + 1);
        assert_eq!(modal.heatmap_bins, super::HEATMAP_DEFAULT_BINS);
        modal.set_chart_kind(ChartKind::Heatmap);
        modal.focus = ChartFocus::Bins;
        modal.adjust_number_row(-1);
        assert_eq!(modal.heatmap_bins, super::HEATMAP_DEFAULT_BINS - 1);
        modal.set_chart_kind(ChartKind::Kde);
        modal.focus = ChartFocus::Bandwidth;
        modal.adjust_number_row(1);
        assert_eq!(modal.kde_bandwidth_factor, 1.1);
        modal.focus = ChartFocus::LimitRows;
        modal.adjust_number_row(-1);
        assert_eq!(modal.row_limit, Some(9_000));
    }

    /// The bar form: a category from the category pool, a value from the numeric pool
    /// less the category, and the order cycling by value or label.
    #[test]
    fn the_bar_form_picks_a_category_and_a_value() {
        let numeric = vec!["year".to_string(), "delay".to_string()];
        let category = vec!["carrier".to_string(), "year".to_string()];
        let mut modal = ChartModal::new();
        modal.open(
            ChartColumns {
                numeric: &numeric,
                datetime: &[],
                category: &category,
            },
            Some(10_000),
            false,
            1,
        );
        modal.set_chart_kind(ChartKind::Bar);
        assert_eq!(
            modal.row_order(),
            [
                ChartFocus::Category,
                ChartFocus::Value,
                ChartFocus::Order,
                ChartFocus::LimitRows
            ]
        );
        assert!(!modal.can_export());
        assert_eq!(modal.picker_items(), ["carrier", "year"]);
        modal.open_picker();
        assert_eq!(modal.effective_bar_category().as_deref(), Some("carrier"));
        modal.picker.as_mut().unwrap().select_original(1);
        modal.picker_choose();
        assert_eq!(modal.bar_category.as_deref(), Some("year"));

        modal.next_focus();
        assert_eq!(
            modal.picker_items(),
            ["Count", "delay"],
            "Count first; the category is not a value"
        );
        modal.open_picker();
        assert_eq!(modal.effective_bar_value(), Some(BarValue::Count));
        modal.picker_choose();
        assert_eq!(modal.bar_value, Some(BarValue::Count));
        assert!(modal.can_export());
        modal.open_picker();
        modal.picker.as_mut().unwrap().select_original(1);
        modal.picker_choose();
        assert_eq!(modal.bar_value, Some(BarValue::Column("delay".to_string())));
        modal.open_picker();
        assert_eq!(
            modal.picker.as_ref().unwrap().selected_original(),
            Some(1),
            "the Picker opens on the value chosen"
        );
        modal.picker = None;
        assert!(modal.can_export());

        modal.next_focus();
        assert_eq!(modal.bar_order, BarOrder::Value);
        modal.adjust_number_row(1);
        assert_eq!(modal.bar_order, BarOrder::Label);
        modal.adjust_number_row(1);
        assert_eq!(modal.bar_order, BarOrder::Value);
    }

    /// Picking the value column as the category takes it out of the value.
    #[test]
    fn a_value_picked_as_the_category_is_cleared() {
        let numeric = vec!["year".to_string(), "delay".to_string()];
        let category = vec!["year".to_string()];
        let mut modal = ChartModal::new();
        modal.open(
            ChartColumns {
                numeric: &numeric,
                datetime: &[],
                category: &category,
            },
            None,
            false,
            1,
        );
        modal.set_chart_kind(ChartKind::Bar);
        modal.bar_value = Some(BarValue::Column("year".to_string()));
        modal.open_picker();
        modal.picker_choose();
        assert_eq!(modal.bar_category.as_deref(), Some("year"));
        assert!(modal.bar_value.is_none());

        // Count counts any category.
        modal.bar_value = Some(BarValue::Count);
        modal.open_picker();
        modal.picker_choose();
        assert_eq!(modal.bar_value, Some(BarValue::Count));
    }
}

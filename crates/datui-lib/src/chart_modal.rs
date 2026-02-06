//! Chart view state: chart type, axis columns, and options.

use ratatui::widgets::ListState;

use crate::widgets::text_input::TextInput;

/// Chart kind: full chart category shown as tabs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartKind {
    #[default]
    XY,
    Histogram,
    BoxPlot,
    Kde,
    Heatmap,
}

impl ChartKind {
    pub const ALL: [Self; 5] = [
        Self::XY,
        Self::Histogram,
        Self::BoxPlot,
        Self::Kde,
        Self::Heatmap,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::XY => "XY",
            Self::Histogram => "Histogram",
            Self::BoxPlot => "Box Plot",
            Self::Kde => "KDE",
            Self::Heatmap => "Heatmap",
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

/// Focus area in the chart sidebar.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartFocus {
    #[default]
    TabBar,
    ChartType,
    XInput,
    XList,
    YInput,
    YList,
    YStartsAtZero,
    LogScale,
    ShowLegend,
    HistInput,
    HistList,
    HistBins,
    BoxInput,
    BoxList,
    KdeInput,
    KdeList,
    KdeBandwidth,
    HeatmapXInput,
    HeatmapXList,
    HeatmapYInput,
    HeatmapYList,
    HeatmapBins,
    /// Limit Rows (shared across all chart types; at bottom of options).
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

/// Chart row limit bounds (for Limit Rows option). User can go down to 0; 0 becomes unlimited (None).
pub const CHART_ROW_LIMIT_MIN: usize = 0;
/// Maximum applicable limit (Polars slice takes u32).
pub const CHART_ROW_LIMIT_MAX: usize = u32::MAX as usize;
/// PgUp/PgDown step for Limit Rows.
pub const CHART_ROW_LIMIT_PAGE_STEP: usize = 100_000;
/// Default numeric limit when switching from Unlimited with + or PgUp.
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

/// Chart modal state: chart kind, axes/columns, and options.
#[derive(Default)]
pub struct ChartModal {
    pub active: bool,
    pub chart_kind: ChartKind,
    pub chart_type: ChartType,
    /// Remembered x-axis column (single; set with spacebar).
    pub x_column: Option<String>,
    /// Remembered y-axis column names (order = series order; set with spacebar, max Y_SERIES_MAX).
    pub y_columns: Vec<String>,
    pub y_starts_at_zero: bool,
    pub log_scale: bool,
    pub show_legend: bool,
    pub focus: ChartFocus,
    /// Text input for x-axis column search.
    pub x_input: TextInput,
    /// Text input for y-axis column search.
    pub y_input: TextInput,
    /// List state for x-axis list (index into x_display_list).
    pub x_list_state: ListState,
    /// List state for y-axis list (index into y_display_list).
    pub y_list_state: ListState,
    /// Available columns for x-axis (datetime + numeric, order preserved for list).
    pub x_candidates: Vec<String>,
    /// Available numeric columns for y-axis.
    pub y_candidates: Vec<String>,
    /// Histogram: remembered column (single selection).
    pub hist_column: Option<String>,
    pub hist_bins: usize,
    pub hist_input: TextInput,
    pub hist_list_state: ListState,
    pub hist_candidates: Vec<String>,
    /// Box plot: remembered column (single selection).
    pub box_column: Option<String>,
    pub box_input: TextInput,
    pub box_list_state: ListState,
    pub box_candidates: Vec<String>,
    /// KDE: remembered column (single selection).
    pub kde_column: Option<String>,
    pub kde_bandwidth_factor: f64,
    pub kde_input: TextInput,
    pub kde_list_state: ListState,
    pub kde_candidates: Vec<String>,
    /// Heatmap: remembered x/y columns (single selection each).
    pub heatmap_x_column: Option<String>,
    pub heatmap_y_column: Option<String>,
    pub heatmap_bins: usize,
    pub heatmap_x_input: TextInput,
    pub heatmap_y_input: TextInput,
    pub heatmap_x_list_state: ListState,
    pub heatmap_y_list_state: ListState,
    pub heatmap_x_candidates: Vec<String>,
    pub heatmap_y_candidates: Vec<String>,
    /// Maximum rows for chart data. None = unlimited (display "Unlimited"); Some(n) = cap at n.
    pub row_limit: Option<usize>,
}

impl ChartModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open the chart modal. No default x or y columns; user selects with spacebar.
    /// `default_row_limit` is the initial value for Limit Rows (e.g. from config); None = unlimited.
    pub fn open(
        &mut self,
        numeric_columns: &[String],
        datetime_columns: &[String],
        default_row_limit: Option<usize>,
    ) {
        self.active = true;
        self.chart_kind = ChartKind::XY;
        self.chart_type = ChartType::Line;
        self.y_starts_at_zero = false;
        self.log_scale = false;
        self.show_legend = true;
        self.focus = ChartFocus::TabBar;
        self.row_limit = default_row_limit.and_then(|n| {
            if n == 0 {
                None
            } else {
                Some(n.clamp(1, CHART_ROW_LIMIT_MAX))
            }
        });

        // x_candidates: datetime first, then numeric (for list order).
        self.x_candidates = datetime_columns.to_vec();
        for c in numeric_columns {
            if !self.x_candidates.contains(c) {
                self.x_candidates.push(c.clone());
            }
        }
        self.y_candidates = numeric_columns.to_vec();
        self.hist_candidates = numeric_columns.to_vec();
        self.box_candidates = numeric_columns.to_vec();
        self.kde_candidates = numeric_columns.to_vec();
        self.heatmap_x_candidates = numeric_columns.to_vec();
        self.heatmap_y_candidates = numeric_columns.to_vec();

        // No default x or y; user selects with spacebar.
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

        self.x_input.set_value(String::new());
        self.y_input.set_value(String::new());
        self.hist_input.set_value(String::new());
        self.box_input.set_value(String::new());
        self.kde_input.set_value(String::new());
        self.heatmap_x_input.set_value(String::new());
        self.heatmap_y_input.set_value(String::new());

        let x_display = self.x_display_list();
        let y_display = self.y_display_list();
        self.x_list_state
            .select(if x_display.is_empty() { None } else { Some(0) });
        self.y_list_state
            .select(if y_display.is_empty() { None } else { Some(0) });
        let hist_display = self.hist_display_list();
        let box_display = self.box_display_list();
        let kde_display = self.kde_display_list();
        let heatmap_x_display = self.heatmap_x_display_list();
        let heatmap_y_display = self.heatmap_y_display_list();
        self.hist_list_state.select(if hist_display.is_empty() {
            None
        } else {
            Some(0)
        });
        self.box_list_state.select(if box_display.is_empty() {
            None
        } else {
            Some(0)
        });
        self.kde_list_state.select(if kde_display.is_empty() {
            None
        } else {
            Some(0)
        });
        self.heatmap_x_list_state
            .select(if heatmap_x_display.is_empty() {
                None
            } else {
                Some(0)
            });
        self.heatmap_y_list_state
            .select(if heatmap_y_display.is_empty() {
                None
            } else {
                Some(0)
            });
    }

    /// X-axis candidates filtered by current x search string (case-insensitive substring).
    pub fn x_filtered(&self) -> Vec<String> {
        let q = self.x_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.x_candidates.clone();
        }
        self.x_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    /// Y-axis candidates filtered by current y search string (case-insensitive substring).
    pub fn y_filtered(&self) -> Vec<String> {
        let q = self.y_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.y_candidates.clone();
        }
        self.y_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    /// X display list: remembered x first (if in filtered), then rest of filtered. Used for list rendering and index.
    pub fn x_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.x_filtered(), &self.x_column)
    }

    /// Y display list: remembered y columns first (in order, that are in filtered), then rest of filtered.
    pub fn y_display_list(&self) -> Vec<String> {
        let filtered = self.y_filtered();
        let mut out: Vec<String> = self
            .y_columns
            .iter()
            .filter(|c| filtered.contains(c))
            .cloned()
            .collect();
        for c in &filtered {
            if !out.contains(c) {
                out.push(c.clone());
            }
        }
        out
    }

    fn display_list_with_selected(filtered: Vec<String>, selected: &Option<String>) -> Vec<String> {
        if let Some(ref selected) = selected {
            if let Some(pos) = filtered.iter().position(|c| c == selected) {
                let mut out = vec![filtered[pos].clone()];
                for (i, c) in filtered.iter().enumerate() {
                    if i != pos {
                        out.push(c.clone());
                    }
                }
                return out;
            }
        }
        filtered
    }

    pub fn hist_filtered(&self) -> Vec<String> {
        let q = self.hist_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.hist_candidates.clone();
        }
        self.hist_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    pub fn hist_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.hist_filtered(), &self.hist_column)
    }

    pub fn box_filtered(&self) -> Vec<String> {
        let q = self.box_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.box_candidates.clone();
        }
        self.box_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    pub fn box_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.box_filtered(), &self.box_column)
    }

    pub fn kde_filtered(&self) -> Vec<String> {
        let q = self.kde_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.kde_candidates.clone();
        }
        self.kde_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    pub fn kde_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.kde_filtered(), &self.kde_column)
    }

    pub fn heatmap_x_filtered(&self) -> Vec<String> {
        let q = self.heatmap_x_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.heatmap_x_candidates.clone();
        }
        self.heatmap_x_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    pub fn heatmap_y_filtered(&self) -> Vec<String> {
        let q = self.heatmap_y_input.value().trim().to_lowercase();
        if q.is_empty() {
            return self.heatmap_y_candidates.clone();
        }
        self.heatmap_y_candidates
            .iter()
            .filter(|c| c.to_lowercase().contains(&q))
            .cloned()
            .collect()
    }

    pub fn heatmap_x_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.heatmap_x_filtered(), &self.heatmap_x_column)
    }

    pub fn heatmap_y_display_list(&self) -> Vec<String> {
        Self::display_list_with_selected(self.heatmap_y_filtered(), &self.heatmap_y_column)
    }

    /// Effective x column for chart/export: the remembered x (no preview on scroll).
    pub fn effective_x_column(&self) -> Option<&String> {
        self.x_column.as_ref()
    }

    /// Effective y columns for chart/export: when Y list focused, remembered + highlighted (if not already remembered); else just remembered.
    pub fn effective_y_columns(&self) -> Vec<String> {
        let mut out = self.y_columns.clone();
        if self.focus == ChartFocus::YList {
            let display = self.y_display_list();
            if let Some(i) = self.y_list_state.selected() {
                if i < display.len() {
                    let name = &display[i];
                    if !out.contains(name) {
                        out.push(name.clone());
                    }
                }
            }
        }
        out
    }

    pub fn effective_hist_column(&self) -> Option<String> {
        if self.focus == ChartFocus::HistList {
            let display = self.hist_display_list();
            if let Some(i) = self.hist_list_state.selected() {
                if i < display.len() {
                    return Some(display[i].clone());
                }
            }
        }
        self.hist_column.clone()
    }

    pub fn effective_box_column(&self) -> Option<String> {
        if self.focus == ChartFocus::BoxList {
            let display = self.box_display_list();
            if let Some(i) = self.box_list_state.selected() {
                if i < display.len() {
                    return Some(display[i].clone());
                }
            }
        }
        self.box_column.clone()
    }

    pub fn effective_kde_column(&self) -> Option<String> {
        if self.focus == ChartFocus::KdeList {
            let display = self.kde_display_list();
            if let Some(i) = self.kde_list_state.selected() {
                if i < display.len() {
                    return Some(display[i].clone());
                }
            }
        }
        self.kde_column.clone()
    }

    pub fn effective_heatmap_x_column(&self) -> Option<String> {
        if self.focus == ChartFocus::HeatmapXList {
            let display = self.heatmap_x_display_list();
            if let Some(i) = self.heatmap_x_list_state.selected() {
                if i < display.len() {
                    return Some(display[i].clone());
                }
            }
        }
        self.heatmap_x_column.clone()
    }

    pub fn effective_heatmap_y_column(&self) -> Option<String> {
        if self.focus == ChartFocus::HeatmapYList {
            let display = self.heatmap_y_display_list();
            if let Some(i) = self.heatmap_y_list_state.selected() {
                if i < display.len() {
                    return Some(display[i].clone());
                }
            }
        }
        self.heatmap_y_column.clone()
    }

    /// Called when Y list loses focus: if no series remembered and we had a highlighted row, remember it.
    pub fn y_list_blur(&mut self) {
        if !self.y_columns.is_empty() {
            return;
        }
        let display = self.y_display_list();
        if let Some(i) = self.y_list_state.selected() {
            if i < display.len() {
                self.y_columns.push(display[i].clone());
            }
        }
    }

    /// Clamp x/y list selection to display list length (e.g. after search filter changes).
    pub fn clamp_list_selections_to_filtered(&mut self) {
        let x_display = self.x_display_list();
        let y_display = self.y_display_list();
        let hist_display = self.hist_display_list();
        let box_display = self.box_display_list();
        let kde_display = self.kde_display_list();
        let heatmap_x_display = self.heatmap_x_display_list();
        let heatmap_y_display = self.heatmap_y_display_list();
        if let Some(s) = self.x_list_state.selected() {
            if s >= x_display.len() {
                self.x_list_state.select(if x_display.is_empty() {
                    None
                } else {
                    Some(x_display.len().saturating_sub(1))
                });
            }
        }
        if let Some(s) = self.y_list_state.selected() {
            if s >= y_display.len() {
                self.y_list_state.select(if y_display.is_empty() {
                    None
                } else {
                    Some(y_display.len().saturating_sub(1))
                });
            }
        }
        if let Some(s) = self.hist_list_state.selected() {
            if s >= hist_display.len() {
                self.hist_list_state.select(if hist_display.is_empty() {
                    None
                } else {
                    Some(hist_display.len().saturating_sub(1))
                });
            }
        }
        if let Some(s) = self.box_list_state.selected() {
            if s >= box_display.len() {
                self.box_list_state.select(if box_display.is_empty() {
                    None
                } else {
                    Some(box_display.len().saturating_sub(1))
                });
            }
        }
        if let Some(s) = self.kde_list_state.selected() {
            if s >= kde_display.len() {
                self.kde_list_state.select(if kde_display.is_empty() {
                    None
                } else {
                    Some(kde_display.len().saturating_sub(1))
                });
            }
        }
        if let Some(s) = self.heatmap_x_list_state.selected() {
            if s >= heatmap_x_display.len() {
                self.heatmap_x_list_state
                    .select(if heatmap_x_display.is_empty() {
                        None
                    } else {
                        Some(heatmap_x_display.len().saturating_sub(1))
                    });
            }
        }
        if let Some(s) = self.heatmap_y_list_state.selected() {
            if s >= heatmap_y_display.len() {
                self.heatmap_y_list_state
                    .select(if heatmap_y_display.is_empty() {
                        None
                    } else {
                        Some(heatmap_y_display.len().saturating_sub(1))
                    });
            }
        }
    }

    pub fn close(&mut self) {
        self.active = false;
        self.chart_kind = ChartKind::XY;
        self.x_column = None;
        self.y_columns.clear();
        self.x_candidates.clear();
        self.y_candidates.clear();
        self.hist_column = None;
        self.box_column = None;
        self.kde_column = None;
        self.heatmap_x_column = None;
        self.heatmap_y_column = None;
        self.hist_candidates.clear();
        self.box_candidates.clear();
        self.kde_candidates.clear();
        self.heatmap_x_candidates.clear();
        self.heatmap_y_candidates.clear();
        self.focus = ChartFocus::TabBar;
    }

    /// Move focus to next/previous in sidebar. When leaving Y list, apply blur (remember highlight if only one).
    pub fn next_focus(&mut self) {
        let prev = self.focus;
        if prev == ChartFocus::YList {
            self.y_list_blur();
        }
        let order = self.focus_order();
        if let Some(pos) = order.iter().position(|f| *f == prev) {
            self.focus = order[(pos + 1) % order.len()];
        } else {
            self.focus = order[0];
        }
    }

    pub fn prev_focus(&mut self) {
        let prev = self.focus;
        if prev == ChartFocus::YList {
            self.y_list_blur();
        }
        let order = self.focus_order();
        if let Some(pos) = order.iter().position(|f| *f == prev) {
            let next = if pos == 0 { order.len() - 1 } else { pos - 1 };
            self.focus = order[next];
        } else {
            self.focus = order[0];
        }
    }

    /// Toggle Y starts at 0 (when focus is YStartsAtZero).
    pub fn toggle_y_starts_at_zero(&mut self) {
        self.y_starts_at_zero = !self.y_starts_at_zero;
    }

    /// Toggle log scale (when focus is LogScale).
    pub fn toggle_log_scale(&mut self) {
        self.log_scale = !self.log_scale;
    }

    /// Toggle show legend (when focus is ShowLegend).
    pub fn toggle_show_legend(&mut self) {
        self.show_legend = !self.show_legend;
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

    pub fn next_chart_kind(&mut self) {
        let idx = ChartKind::ALL
            .iter()
            .position(|&k| k == self.chart_kind)
            .unwrap_or(0);
        self.chart_kind = ChartKind::ALL[(idx + 1) % ChartKind::ALL.len()];
        self.focus = ChartFocus::TabBar;
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
        self.chart_kind = ChartKind::ALL[prev];
        self.focus = ChartFocus::TabBar;
    }

    /// Effective row limit to pass to prepare_* (unlimited = CHART_ROW_LIMIT_MAX).
    pub fn effective_row_limit(&self) -> usize {
        self.row_limit.unwrap_or(CHART_ROW_LIMIT_MAX)
    }

    /// Display string for Limit Rows: "Unlimited" or number with commas.
    pub fn row_limit_display(&self) -> String {
        match self.row_limit {
            None => "Unlimited".to_string(),
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
            std::cmp::Ordering::Less => current.saturating_sub(step).max(CHART_ROW_LIMIT_MIN),
            std::cmp::Ordering::Equal => current,
        };
        self.row_limit = if next == 0 { None } else { Some(next) };
    }

    /// Adjust row limit by 10,000 (PgUp / PgDown). None = unlimited.
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
            std::cmp::Ordering::Less => current.saturating_sub(step).max(CHART_ROW_LIMIT_MIN),
            std::cmp::Ordering::Equal => current,
        };
        self.row_limit = if next == 0 { None } else { Some(next) };
    }

    /// Move x-axis list highlight down (does not change remembered x; use spacebar to remember).
    pub fn x_list_down(&mut self) {
        let display = self.x_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .x_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.x_list_state.select(Some(i));
    }

    /// Move x-axis list highlight up.
    pub fn x_list_up(&mut self) {
        let display = self.x_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self.x_list_state.selected().unwrap_or(0).saturating_sub(1);
        self.x_list_state.select(Some(i));
    }

    /// Toggle x selection with spacebar: set remembered x to the highlighted row (single selection).
    pub fn x_list_toggle(&mut self) {
        let display = self.x_display_list();
        if let Some(i) = self.x_list_state.selected() {
            if i < display.len() {
                self.x_column = Some(display[i].clone());
            }
        }
    }

    /// Move y-axis list highlight down (does not change remembered y; use spacebar to toggle).
    pub fn y_list_down(&mut self) {
        let display = self.y_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .y_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.y_list_state.select(Some(i));
    }

    /// Move y-axis list highlight up.
    pub fn y_list_up(&mut self) {
        let display = self.y_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self.y_list_state.selected().unwrap_or(0).saturating_sub(1);
        self.y_list_state.select(Some(i));
    }

    /// Toggle y selection with spacebar: add highlighted to remembered (up to Y_SERIES_MAX) or remove if already remembered.
    pub fn y_list_toggle(&mut self) {
        let display = self.y_display_list();
        let Some(i) = self.y_list_state.selected() else {
            return;
        };
        if i >= display.len() {
            return;
        }
        let name = display[i].clone();
        if let Some(pos) = self.y_columns.iter().position(|c| c == &name) {
            self.y_columns.remove(pos);
        } else if self.y_columns.len() < Y_SERIES_MAX {
            self.y_columns.push(name);
        }
    }

    pub fn hist_list_down(&mut self) {
        let display = self.hist_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .hist_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.hist_list_state.select(Some(i));
    }

    pub fn hist_list_up(&mut self) {
        let display = self.hist_display_list();
        if display.is_empty() {
            return;
        }
        let i = self
            .hist_list_state
            .selected()
            .unwrap_or(0)
            .saturating_sub(1);
        self.hist_list_state.select(Some(i));
    }

    pub fn hist_list_toggle(&mut self) {
        let display = self.hist_display_list();
        if let Some(i) = self.hist_list_state.selected() {
            if i < display.len() {
                self.hist_column = Some(display[i].clone());
            }
        }
    }

    pub fn box_list_down(&mut self) {
        let display = self.box_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .box_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.box_list_state.select(Some(i));
    }

    pub fn box_list_up(&mut self) {
        let display = self.box_display_list();
        if display.is_empty() {
            return;
        }
        let i = self
            .box_list_state
            .selected()
            .unwrap_or(0)
            .saturating_sub(1);
        self.box_list_state.select(Some(i));
    }

    pub fn box_list_toggle(&mut self) {
        let display = self.box_display_list();
        if let Some(i) = self.box_list_state.selected() {
            if i < display.len() {
                self.box_column = Some(display[i].clone());
            }
        }
    }

    pub fn kde_list_down(&mut self) {
        let display = self.kde_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .kde_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.kde_list_state.select(Some(i));
    }

    pub fn kde_list_up(&mut self) {
        let display = self.kde_display_list();
        if display.is_empty() {
            return;
        }
        let i = self
            .kde_list_state
            .selected()
            .unwrap_or(0)
            .saturating_sub(1);
        self.kde_list_state.select(Some(i));
    }

    pub fn kde_list_toggle(&mut self) {
        let display = self.kde_display_list();
        if let Some(i) = self.kde_list_state.selected() {
            if i < display.len() {
                self.kde_column = Some(display[i].clone());
            }
        }
    }

    pub fn heatmap_x_list_down(&mut self) {
        let display = self.heatmap_x_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .heatmap_x_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.heatmap_x_list_state.select(Some(i));
    }

    pub fn heatmap_x_list_up(&mut self) {
        let display = self.heatmap_x_display_list();
        if display.is_empty() {
            return;
        }
        let i = self
            .heatmap_x_list_state
            .selected()
            .unwrap_or(0)
            .saturating_sub(1);
        self.heatmap_x_list_state.select(Some(i));
    }

    pub fn heatmap_x_list_toggle(&mut self) {
        let display = self.heatmap_x_display_list();
        if let Some(i) = self.heatmap_x_list_state.selected() {
            if i < display.len() {
                self.heatmap_x_column = Some(display[i].clone());
            }
        }
    }

    pub fn heatmap_y_list_down(&mut self) {
        let display = self.heatmap_y_display_list();
        let len = display.len();
        if len == 0 {
            return;
        }
        let i = self
            .heatmap_y_list_state
            .selected()
            .unwrap_or(0)
            .saturating_add(1)
            .min(len.saturating_sub(1));
        self.heatmap_y_list_state.select(Some(i));
    }

    pub fn heatmap_y_list_up(&mut self) {
        let display = self.heatmap_y_display_list();
        if display.is_empty() {
            return;
        }
        let i = self
            .heatmap_y_list_state
            .selected()
            .unwrap_or(0)
            .saturating_sub(1);
        self.heatmap_y_list_state.select(Some(i));
    }

    pub fn heatmap_y_list_toggle(&mut self) {
        let display = self.heatmap_y_display_list();
        if let Some(i) = self.heatmap_y_list_state.selected() {
            if i < display.len() {
                self.heatmap_y_column = Some(display[i].clone());
            }
        }
    }

    pub fn is_text_input_focused(&self) -> bool {
        matches!(
            self.focus,
            ChartFocus::XInput
                | ChartFocus::YInput
                | ChartFocus::HistInput
                | ChartFocus::BoxInput
                | ChartFocus::KdeInput
                | ChartFocus::HeatmapXInput
                | ChartFocus::HeatmapYInput
        )
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
        }
    }

    fn focus_order(&self) -> &'static [ChartFocus] {
        match self.chart_kind {
            ChartKind::XY => &[
                ChartFocus::TabBar,
                ChartFocus::ChartType,
                ChartFocus::XInput,
                ChartFocus::XList,
                ChartFocus::YInput,
                ChartFocus::YList,
                ChartFocus::YStartsAtZero,
                ChartFocus::LogScale,
                ChartFocus::ShowLegend,
                ChartFocus::LimitRows,
            ],
            ChartKind::Histogram => &[
                ChartFocus::TabBar,
                ChartFocus::HistInput,
                ChartFocus::HistList,
                ChartFocus::HistBins,
                ChartFocus::LimitRows,
            ],
            ChartKind::BoxPlot => &[
                ChartFocus::TabBar,
                ChartFocus::BoxInput,
                ChartFocus::BoxList,
                ChartFocus::LimitRows,
            ],
            ChartKind::Kde => &[
                ChartFocus::TabBar,
                ChartFocus::KdeInput,
                ChartFocus::KdeList,
                ChartFocus::KdeBandwidth,
                ChartFocus::LimitRows,
            ],
            ChartKind::Heatmap => &[
                ChartFocus::TabBar,
                ChartFocus::HeatmapXInput,
                ChartFocus::HeatmapXList,
                ChartFocus::HeatmapYInput,
                ChartFocus::HeatmapYList,
                ChartFocus::HeatmapBins,
                ChartFocus::LimitRows,
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChartFocus, ChartKind, ChartModal, ChartType, Y_SERIES_MAX};

    #[test]
    fn open_no_default_columns() {
        let numeric = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let datetime = vec!["date".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &datetime, Some(10_000));
        assert!(modal.active);
        assert_eq!(modal.chart_kind, ChartKind::XY);
        assert_eq!(modal.chart_type, ChartType::Line);
        assert!(modal.x_column.is_none());
        assert!(modal.y_columns.is_empty());
        assert!(!modal.y_starts_at_zero);
        assert!(!modal.log_scale);
        assert!(modal.show_legend);
        assert_eq!(modal.focus, ChartFocus::TabBar);
        assert_eq!(modal.row_limit, Some(10_000));
    }

    #[test]
    fn open_numeric_only_no_defaults() {
        let numeric = vec!["x".to_string(), "y".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &[], Some(10_000));
        assert!(modal.x_column.is_none());
        assert!(modal.y_columns.is_empty());
    }

    #[test]
    fn toggles_persist() {
        let mut modal = ChartModal::new();
        modal.open(&["a".into(), "b".into()], &[], Some(10_000));
        assert!(!modal.y_starts_at_zero);
        modal.toggle_y_starts_at_zero();
        assert!(modal.y_starts_at_zero);
        modal.toggle_log_scale();
        assert!(modal.log_scale);
        modal.toggle_show_legend();
        assert!(!modal.show_legend);
    }

    #[test]
    fn x_display_list_puts_remembered_first() {
        let mut modal = ChartModal::new();
        modal.open(&["a".into(), "b".into(), "c".into()], &[], Some(10_000));
        assert_eq!(modal.x_display_list(), vec!["a", "b", "c"]);
        modal.x_column = Some("c".to_string());
        assert_eq!(modal.x_display_list(), vec!["c", "a", "b"]);
    }

    #[test]
    fn y_list_toggle_add_remove() {
        let mut modal = ChartModal::new();
        modal.open(&["a".into(), "b".into(), "c".into()], &[], Some(10_000));
        modal.y_list_state.select(Some(0)); // highlight "a"
        modal.y_list_toggle();
        assert_eq!(modal.y_columns, vec!["a"]);
        modal.y_list_toggle(); // toggle "a" off
        assert!(modal.y_columns.is_empty());
        modal.y_list_toggle(); // toggle "a" on again
        assert_eq!(modal.y_columns, vec!["a"]);
        modal.y_list_state.select(Some(1));
        modal.y_list_toggle();
        assert_eq!(modal.y_columns.len(), 2);
    }

    #[test]
    fn y_series_max_cap() {
        let mut modal = ChartModal::new();
        let cols: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        modal.open(&cols, &[], Some(10_000));
        for i in 0..Y_SERIES_MAX {
            modal.y_list_state.select(Some(i));
            modal.y_list_toggle();
        }
        assert_eq!(modal.y_columns.len(), Y_SERIES_MAX);
        modal.y_list_state.select(Some(Y_SERIES_MAX));
        modal.y_list_toggle(); // should not add
        assert_eq!(modal.y_columns.len(), Y_SERIES_MAX);
    }
}

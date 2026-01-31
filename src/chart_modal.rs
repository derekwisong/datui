//! Chart view state: chart type, axis columns, and options.

use ratatui::widgets::ListState;

use crate::widgets::text_input::TextInput;

/// Chart type: Line, Scatter, or Bar (maps to ratatui GraphType).
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
    ChartType,
    XInput,
    XList,
    YInput,
    YList,
    YStartsAtZero,
    LogScale,
    ShowLegend,
}

/// Maximum number of y-axis series that can be selected (remembered).
pub const Y_SERIES_MAX: usize = 7;

/// Chart modal state: type, x/y column selection, and options.
#[derive(Default)]
pub struct ChartModal {
    pub active: bool,
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
}

impl ChartModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open the chart modal. No default x or y columns; user selects with spacebar.
    pub fn open(&mut self, numeric_columns: &[String], datetime_columns: &[String]) {
        self.active = true;
        self.chart_type = ChartType::Line;
        self.y_starts_at_zero = false;
        self.log_scale = false;
        self.show_legend = true;
        self.focus = ChartFocus::ChartType;

        // x_candidates: datetime first, then numeric (for list order).
        self.x_candidates = datetime_columns.to_vec();
        for c in numeric_columns {
            if !self.x_candidates.contains(c) {
                self.x_candidates.push(c.clone());
            }
        }
        self.y_candidates = numeric_columns.to_vec();

        // No default x or y; user selects with spacebar.
        self.x_column = None;
        self.y_columns.clear();

        self.x_input.set_value(String::new());
        self.y_input.set_value(String::new());

        let x_display = self.x_display_list();
        let y_display = self.y_display_list();
        self.x_list_state
            .select(if x_display.is_empty() { None } else { Some(0) });
        self.y_list_state
            .select(if y_display.is_empty() { None } else { Some(0) });
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
        let filtered = self.x_filtered();
        if let Some(ref x) = self.x_column {
            if let Some(pos) = filtered.iter().position(|c| c == x) {
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
    }

    pub fn close(&mut self) {
        self.active = false;
        self.x_column = None;
        self.y_columns.clear();
        self.x_candidates.clear();
        self.y_candidates.clear();
        self.focus = ChartFocus::ChartType;
    }

    /// Move focus to next/previous in sidebar. When leaving Y list, apply blur (remember highlight if only one).
    pub fn next_focus(&mut self) {
        if self.focus == ChartFocus::YList {
            self.y_list_blur();
        }
        self.focus = match self.focus {
            ChartFocus::ChartType => ChartFocus::XInput,
            ChartFocus::XInput => ChartFocus::XList,
            ChartFocus::XList => ChartFocus::YInput,
            ChartFocus::YInput => ChartFocus::YList,
            ChartFocus::YList => ChartFocus::YStartsAtZero,
            ChartFocus::YStartsAtZero => ChartFocus::LogScale,
            ChartFocus::LogScale => ChartFocus::ShowLegend,
            ChartFocus::ShowLegend => ChartFocus::ChartType,
        };
    }

    pub fn prev_focus(&mut self) {
        if self.focus == ChartFocus::YList {
            self.y_list_blur();
        }
        self.focus = match self.focus {
            ChartFocus::ChartType => ChartFocus::ShowLegend,
            ChartFocus::XInput => ChartFocus::ChartType,
            ChartFocus::XList => ChartFocus::XInput,
            ChartFocus::YInput => ChartFocus::XList,
            ChartFocus::YList => ChartFocus::YInput,
            ChartFocus::YStartsAtZero => ChartFocus::YList,
            ChartFocus::LogScale => ChartFocus::YStartsAtZero,
            ChartFocus::ShowLegend => ChartFocus::LogScale,
        };
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
}

#[cfg(test)]
mod tests {
    use super::{ChartFocus, ChartModal, ChartType, Y_SERIES_MAX};

    #[test]
    fn open_no_default_columns() {
        let numeric = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let datetime = vec!["date".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &datetime);
        assert!(modal.active);
        assert_eq!(modal.chart_type, ChartType::Line);
        assert!(modal.x_column.is_none());
        assert!(modal.y_columns.is_empty());
        assert!(!modal.y_starts_at_zero);
        assert!(!modal.log_scale);
        assert!(modal.show_legend);
        assert_eq!(modal.focus, ChartFocus::ChartType);
    }

    #[test]
    fn open_numeric_only_no_defaults() {
        let numeric = vec!["x".to_string(), "y".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &[]);
        assert!(modal.x_column.is_none());
        assert!(modal.y_columns.is_empty());
    }

    #[test]
    fn toggles_persist() {
        let mut modal = ChartModal::new();
        modal.open(&["a".into(), "b".into()], &[]);
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
        modal.open(&["a".into(), "b".into(), "c".into()], &[]);
        assert_eq!(modal.x_display_list(), vec!["a", "b", "c"]);
        modal.x_column = Some("c".to_string());
        assert_eq!(modal.x_display_list(), vec!["c", "a", "b"]);
    }

    #[test]
    fn y_list_toggle_add_remove() {
        let mut modal = ChartModal::new();
        modal.open(&["a".into(), "b".into(), "c".into()], &[]);
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
        modal.open(&cols, &[]);
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

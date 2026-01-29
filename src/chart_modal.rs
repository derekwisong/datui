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

/// Chart modal state: type, x/y column selection, and options.
#[derive(Default)]
pub struct ChartModal {
    pub active: bool,
    pub chart_type: ChartType,
    /// Selected x-axis column name (from schema).
    pub x_column: Option<String>,
    /// Selected y-axis column names (order = series order).
    pub y_columns: Vec<String>,
    pub y_starts_at_zero: bool,
    pub log_scale: bool,
    pub show_legend: bool,
    pub focus: ChartFocus,
    /// Text input for x-axis column search.
    pub x_input: TextInput,
    /// Text input for y-axis column search.
    pub y_input: TextInput,
    /// List state for x-axis column list (index into filtered list).
    pub x_list_state: ListState,
    /// List state for y-axis column list (index into filtered list).
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

    /// Open the chart modal and set defaults from schema (column names for x/y).
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

        // Default x: first datetime column, else first numeric.
        self.x_column = datetime_columns
            .first()
            .cloned()
            .or_else(|| numeric_columns.first().cloned());

        // Default y: first numeric column (that isn't x), or just first numeric.
        self.y_columns = numeric_columns
            .iter()
            .filter(|c| Some((*c).clone()) != self.x_column)
            .take(1)
            .cloned()
            .collect();
        if self.y_columns.is_empty() {
            self.y_columns = numeric_columns.first().cloned().into_iter().collect();
        }

        // Do not auto-fill filter inputs; leave them empty for the user to type
        self.x_input.set_value(String::new());
        self.y_input.set_value(String::new());

        let x_filtered = self.x_filtered();
        let y_filtered = self.y_filtered();
        let x_idx = self
            .x_column
            .as_ref()
            .and_then(|x| x_filtered.iter().position(|c| c == x))
            .unwrap_or(0);
        let y_idx = self
            .y_columns
            .first()
            .and_then(|y| y_filtered.iter().position(|c| c == y))
            .unwrap_or(0);
        self.x_list_state
            .select(Some(x_idx.min(x_filtered.len().saturating_sub(1))));
        self.y_list_state
            .select(Some(y_idx.min(y_filtered.len().saturating_sub(1))));
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

    /// Clamp x/y list selection to filtered list length (e.g. after search filter changes).
    pub fn clamp_list_selections_to_filtered(&mut self) {
        let xf = self.x_filtered();
        let yf = self.y_filtered();
        if let Some(s) = self.x_list_state.selected() {
            if s >= xf.len() {
                self.x_list_state.select(if xf.is_empty() {
                    None
                } else {
                    Some(xf.len() - 1)
                });
            }
        }
        if let Some(s) = self.y_list_state.selected() {
            if s >= yf.len() {
                self.y_list_state.select(if yf.is_empty() {
                    None
                } else {
                    Some(yf.len() - 1)
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

    /// Move focus to next/previous in sidebar.
    pub fn next_focus(&mut self) {
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

    /// Move x-axis list selection down; updates x_column from filtered list.
    pub fn x_list_down(&mut self) {
        let filtered = self.x_filtered();
        let len = filtered.len();
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
        self.x_column = Some(filtered[i].clone());
    }

    /// Move x-axis list selection up; updates x_column from filtered list.
    pub fn x_list_up(&mut self) {
        let filtered = self.x_filtered();
        let len = filtered.len();
        if len == 0 {
            return;
        }
        let i = self.x_list_state.selected().unwrap_or(0).saturating_sub(1);
        self.x_list_state.select(Some(i));
        self.x_column = Some(filtered[i].clone());
    }

    /// Confirm x selection from list (Enter); updates x_column only; filter input is unchanged.
    pub fn x_list_select(&mut self) {
        let filtered = self.x_filtered();
        if let Some(i) = self.x_list_state.selected() {
            if i < filtered.len() {
                self.x_column = Some(filtered[i].clone());
            }
        }
    }

    /// Move y-axis list selection down; updates y_columns from filtered list.
    pub fn y_list_down(&mut self) {
        let filtered = self.y_filtered();
        let len = filtered.len();
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
        self.y_columns = vec![filtered[i].clone()];
    }

    /// Move y-axis list selection up; updates y_columns from filtered list.
    pub fn y_list_up(&mut self) {
        let filtered = self.y_filtered();
        let len = filtered.len();
        if len == 0 {
            return;
        }
        let i = self.y_list_state.selected().unwrap_or(0).saturating_sub(1);
        self.y_list_state.select(Some(i));
        self.y_columns = vec![filtered[i].clone()];
    }

    /// Confirm y selection from list (Enter); updates y_columns only; filter input is unchanged.
    pub fn y_list_select(&mut self) {
        let filtered = self.y_filtered();
        if let Some(i) = self.y_list_state.selected() {
            if i < filtered.len() {
                self.y_columns = vec![filtered[i].clone()];
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChartFocus, ChartModal, ChartType};

    #[test]
    fn open_sets_sensible_defaults() {
        let numeric = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let datetime = vec!["date".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &datetime);
        assert!(modal.active);
        assert_eq!(modal.chart_type, ChartType::Line);
        assert_eq!(modal.x_column.as_deref(), Some("date"));
        assert_eq!(modal.y_columns, vec!["a"]);
        assert!(!modal.y_starts_at_zero);
        assert!(!modal.log_scale);
        assert!(modal.show_legend);
        assert_eq!(modal.focus, ChartFocus::ChartType);
    }

    #[test]
    fn open_numeric_only_defaults() {
        let numeric = vec!["x".to_string(), "y".to_string()];
        let mut modal = ChartModal::new();
        modal.open(&numeric, &[]);
        assert_eq!(modal.x_column.as_deref(), Some("x"));
        assert_eq!(modal.y_columns, vec!["y"]);
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
}

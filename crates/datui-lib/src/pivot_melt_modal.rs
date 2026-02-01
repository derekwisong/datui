//! Pivot / Melt modal state and focus.
//!
//! Phase 4: Pivot tab UI. Phase 5: Melt tab UI.

use crate::widgets::text_input::TextInput;
use polars::datatypes::DataType;
use ratatui::widgets::TableState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PivotMeltTab {
    #[default]
    Pivot,
    Melt,
}

/// Focus: tab bar, tab-specific body controls, footer buttons.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PivotMeltFocus {
    #[default]
    TabBar,
    // Pivot tab
    PivotFilter,
    PivotIndexList,
    PivotPivotCol,
    PivotValueCol,
    PivotAggregation,
    PivotSortToggle,
    // Melt tab
    MeltFilter,
    MeltIndexList,
    MeltStrategy,
    MeltPattern,
    MeltType,
    MeltExplicitList,
    MeltVarName,
    MeltValName,
    // Footer
    Apply,
    Cancel,
    Clear,
}

/// Melt value-column strategy.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MeltValueStrategy {
    #[default]
    AllExceptIndex,
    ByPattern,
    ByType,
    ExplicitList,
}

impl MeltValueStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllExceptIndex => "All except index",
            Self::ByPattern => "By pattern",
            Self::ByType => "By type",
            Self::ExplicitList => "Explicit list",
        }
    }
}

/// Type filter for Melt "by type".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MeltTypeFilter {
    #[default]
    Numeric,
    String,
    Datetime,
    Boolean,
}

impl MeltTypeFilter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Numeric => "Numeric",
            Self::String => "String",
            Self::Datetime => "Datetime",
            Self::Boolean => "Boolean",
        }
    }
}

/// Aggregation for pivot value column.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PivotAggregation {
    #[default]
    Last,
    First,
    Min,
    Max,
    Avg,
    Med,
    Std,
    Count,
}

impl PivotAggregation {
    pub const ALL: [Self; 8] = [
        Self::Last,
        Self::First,
        Self::Min,
        Self::Max,
        Self::Avg,
        Self::Med,
        Self::Std,
        Self::Count,
    ];

    pub const STRING_ONLY: [Self; 2] = [Self::First, Self::Last];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Last => "last",
            Self::First => "first",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Med => "med",
            Self::Std => "std",
            Self::Count => "count",
        }
    }
}

/// Spec for pivot operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PivotSpec {
    pub index: Vec<String>,
    pub pivot_column: String,
    pub value_column: String,
    pub aggregation: PivotAggregation,
    pub sort_columns: bool,
}

/// Spec for melt operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeltSpec {
    pub index: Vec<String>,
    pub value_columns: Vec<String>,
    pub variable_name: String,
    pub value_name: String,
}

pub struct PivotMeltModal {
    pub active: bool,
    pub active_tab: PivotMeltTab,
    pub focus: PivotMeltFocus,

    /// Column names from current schema. Set when opening modal.
    pub available_columns: Vec<String>,
    /// Column name -> DataType. Set when opening.
    pub column_dtypes: HashMap<String, DataType>,

    // Pivot form
    pub pivot_filter_input: TextInput,
    pub pivot_index_table: TableState,
    pub index_columns: Vec<String>,
    pub pivot_column: Option<String>,
    pub pivot_pool_idx: usize,
    pub pivot_pool_table: TableState,
    pub value_column: Option<String>,
    pub value_pool_idx: usize,
    pub value_pool_table: TableState,
    pub aggregation_idx: usize,
    pub sort_new_columns: bool,

    // Melt form
    pub melt_filter_input: TextInput,
    pub melt_index_table: TableState,
    pub melt_index_columns: Vec<String>,
    pub melt_value_strategy: MeltValueStrategy,
    pub melt_pattern: String,
    pub melt_pattern_cursor: usize,
    pub melt_type_filter: MeltTypeFilter,
    pub melt_explicit_list: Vec<String>,
    pub melt_explicit_table: TableState,
    pub melt_variable_name: String,
    pub melt_variable_cursor: usize,
    pub melt_value_name: String,
    pub melt_value_cursor: usize,
}

impl Default for PivotMeltModal {
    fn default() -> Self {
        Self {
            active: false,
            active_tab: PivotMeltTab::default(),
            focus: PivotMeltFocus::default(),
            available_columns: Vec::new(),
            column_dtypes: HashMap::new(),
            pivot_filter_input: TextInput::new(),
            pivot_index_table: TableState::default(),
            index_columns: Vec::new(),
            pivot_column: None,
            pivot_pool_idx: 0,
            pivot_pool_table: TableState::default(),
            value_column: None,
            value_pool_idx: 0,
            value_pool_table: TableState::default(),
            aggregation_idx: 0,
            sort_new_columns: false,
            melt_filter_input: TextInput::new(),
            melt_index_table: TableState::default(),
            melt_index_columns: Vec::new(),
            melt_value_strategy: MeltValueStrategy::default(),
            melt_pattern: String::new(),
            melt_pattern_cursor: 0,
            melt_type_filter: MeltTypeFilter::default(),
            melt_explicit_list: Vec::new(),
            melt_explicit_table: TableState::default(),
            melt_variable_name: "variable".to_string(),
            melt_variable_cursor: 0,
            melt_value_name: "value".to_string(),
            melt_value_cursor: 0,
        }
    }
}

impl PivotMeltModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self, history_limit: usize, theme: &crate::config::Theme) {
        self.active = true;
        self.active_tab = PivotMeltTab::Pivot;
        self.focus = PivotMeltFocus::TabBar;
        self.pivot_filter_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.melt_filter_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.reset_form();
    }

    pub fn close(&mut self) {
        self.active = false;
    }

    pub fn reset_form(&mut self) {
        self.pivot_filter_input.clear();
        self.pivot_index_table
            .select(if self.available_columns.is_empty() {
                None
            } else {
                Some(0)
            });
        self.index_columns.clear();
        self.pivot_column = None;
        self.pivot_pool_idx = 0;
        self.value_column = None;
        self.value_pool_idx = 0;
        let pool = self.pivot_pool();
        if !pool.is_empty() {
            self.pivot_column = pool.first().cloned();
            self.pivot_pool_table.select(Some(0));
        } else {
            self.pivot_pool_table.select(None);
        }
        let vpool = self.pivot_value_pool();
        if !vpool.is_empty() {
            self.value_column = vpool.first().cloned();
            self.value_pool_idx = 0;
            self.value_pool_table.select(Some(0));
        } else {
            self.value_pool_table.select(None);
        }
        self.aggregation_idx = 0;
        self.sort_new_columns = false;
        self.melt_filter_input.clear();
        self.melt_index_table
            .select(if self.available_columns.is_empty() {
                None
            } else {
                Some(0)
            });
        self.melt_index_columns.clear();
        self.melt_value_strategy = MeltValueStrategy::default();
        self.melt_pattern.clear();
        self.melt_pattern_cursor = 0;
        self.melt_type_filter = MeltTypeFilter::default();
        self.melt_explicit_list.clear();
        self.melt_explicit_table.select(None);
        self.melt_variable_name = "variable".to_string();
        self.melt_variable_cursor = 0;
        self.melt_value_name = "value".to_string();
        self.melt_value_cursor = 0;
        self.focus = PivotMeltFocus::TabBar;
    }

    fn pivot_focus_order() -> &'static [PivotMeltFocus] {
        &[
            PivotMeltFocus::PivotFilter,
            PivotMeltFocus::PivotIndexList,
            PivotMeltFocus::PivotPivotCol,
            PivotMeltFocus::PivotValueCol,
            PivotMeltFocus::PivotAggregation,
            PivotMeltFocus::PivotSortToggle,
            PivotMeltFocus::Apply,
            PivotMeltFocus::Cancel,
            PivotMeltFocus::Clear,
        ]
    }

    fn melt_focus_order() -> &'static [PivotMeltFocus] {
        &[
            PivotMeltFocus::MeltFilter,
            PivotMeltFocus::MeltIndexList,
            PivotMeltFocus::MeltStrategy,
            PivotMeltFocus::MeltPattern,
            PivotMeltFocus::MeltType,
            PivotMeltFocus::MeltExplicitList,
            PivotMeltFocus::MeltVarName,
            PivotMeltFocus::MeltValName,
            PivotMeltFocus::Apply,
            PivotMeltFocus::Cancel,
            PivotMeltFocus::Clear,
        ]
    }

    pub fn next_focus(&mut self) {
        match self.focus {
            PivotMeltFocus::TabBar => {
                self.focus = match self.active_tab {
                    PivotMeltTab::Pivot => PivotMeltFocus::PivotFilter,
                    PivotMeltTab::Melt => PivotMeltFocus::MeltFilter,
                };
            }
            f => {
                let order = match self.active_tab {
                    PivotMeltTab::Pivot => Self::pivot_focus_order(),
                    PivotMeltTab::Melt => Self::melt_focus_order(),
                };
                if let Some(pos) = order.iter().position(|&x| x == f) {
                    if pos + 1 < order.len() {
                        self.focus = order[pos + 1];
                    } else {
                        self.focus = PivotMeltFocus::TabBar;
                    }
                } else {
                    self.focus = PivotMeltFocus::TabBar;
                }
            }
        }
    }

    pub fn prev_focus(&mut self) {
        match self.focus {
            PivotMeltFocus::TabBar => {
                let order = match self.active_tab {
                    PivotMeltTab::Pivot => Self::pivot_focus_order(),
                    PivotMeltTab::Melt => Self::melt_focus_order(),
                };
                self.focus = order[order.len() - 1];
            }
            f => {
                let order = match self.active_tab {
                    PivotMeltTab::Pivot => Self::pivot_focus_order(),
                    PivotMeltTab::Melt => Self::melt_focus_order(),
                };
                if let Some(pos) = order.iter().position(|&x| x == f) {
                    if pos > 0 {
                        self.focus = order[pos - 1];
                    } else {
                        self.focus = PivotMeltFocus::TabBar;
                    }
                } else {
                    self.focus = PivotMeltFocus::TabBar;
                }
            }
        }
    }

    pub fn switch_tab(&mut self) {
        self.active_tab = match self.active_tab {
            PivotMeltTab::Pivot => PivotMeltTab::Melt,
            PivotMeltTab::Melt => PivotMeltTab::Pivot,
        };
        self.focus = PivotMeltFocus::TabBar;
    }

    // ----- Pivot helpers -----

    pub fn pivot_filtered_columns(&self) -> Vec<String> {
        let filter_lower = self.pivot_filter_input.value.to_lowercase();
        self.available_columns
            .iter()
            .filter(|c| c.to_lowercase().contains(&filter_lower))
            .cloned()
            .collect()
    }

    pub fn pivot_pool(&self) -> Vec<String> {
        let idx_set: std::collections::HashSet<_> = self.index_columns.iter().collect();
        self.pivot_filtered_columns()
            .into_iter()
            .filter(|c| !idx_set.contains(c))
            .collect()
    }

    pub fn pivot_value_pool(&self) -> Vec<String> {
        let idx_set: std::collections::HashSet<_> = self.index_columns.iter().collect();
        let pivot = self.pivot_column.as_deref();
        self.pivot_filtered_columns()
            .into_iter()
            .filter(|c| !idx_set.contains(c) && pivot != Some(c.as_str()))
            .collect()
    }

    pub fn pivot_aggregation_options(&self) -> Vec<PivotAggregation> {
        let value_col = match &self.value_column {
            Some(s) => s,
            None => return PivotAggregation::ALL.to_vec(),
        };
        let dtype = self.column_dtypes.get(value_col);
        let is_string = dtype.is_some_and(|d| matches!(d, DataType::String));
        if is_string {
            PivotAggregation::STRING_ONLY.to_vec()
        } else {
            PivotAggregation::ALL.to_vec()
        }
    }

    pub fn pivot_aggregation(&self) -> PivotAggregation {
        let opts = self.pivot_aggregation_options();
        if opts.is_empty() {
            return PivotAggregation::Last;
        }
        let i = self.aggregation_idx.min(opts.len().saturating_sub(1));
        opts[i]
    }

    pub fn pivot_validation_error(&self) -> Option<String> {
        if self.index_columns.is_empty() {
            return Some("Select at least one index column.".to_string());
        }
        let pivot = match &self.pivot_column {
            Some(s) => s,
            None => return Some("Select a pivot column.".to_string()),
        };
        if self.index_columns.contains(pivot) {
            return Some("Pivot column must not be in index.".to_string());
        }
        let value = match &self.value_column {
            Some(s) => s,
            None => return Some("Select a value column.".to_string()),
        };
        if self.index_columns.contains(value) || pivot == value {
            return Some("Value column must not be in index or equal to pivot.".to_string());
        }
        let pool = self.pivot_value_pool();
        if !pool.contains(value) {
            return Some("Value column not in available columns.".to_string());
        }
        None
    }

    pub fn build_pivot_spec(&self) -> Option<PivotSpec> {
        if self.pivot_validation_error().is_some() {
            return None;
        }
        let pivot = self.pivot_column.clone()?;
        let value = self.value_column.clone()?;
        Some(PivotSpec {
            index: self.index_columns.clone(),
            pivot_column: pivot,
            value_column: value,
            aggregation: self.pivot_aggregation(),
            sort_columns: self.sort_new_columns,
        })
    }

    pub fn pivot_toggle_index_at_selection(&mut self) {
        let filtered = self.pivot_filtered_columns();
        let i = match self.pivot_index_table.selected() {
            Some(i) if i < filtered.len() => i,
            _ => return,
        };
        let col = filtered[i].clone();
        if let Some(pos) = self.index_columns.iter().position(|c| c == &col) {
            self.index_columns.remove(pos);
        } else {
            self.index_columns.push(col);
        }
        self.pivot_fix_pivot_and_value_after_index_change();
    }

    fn pivot_fix_pivot_and_value_after_index_change(&mut self) {
        let pool = self.pivot_pool();
        let in_index = |s: &str| self.index_columns.iter().any(|c| c.as_str() == s);
        let pivot_valid = self
            .pivot_column
            .as_deref()
            .map(|p| !in_index(p) && pool.iter().any(|c| c.as_str() == p))
            .unwrap_or(false);
        if !pivot_valid {
            if pool.is_empty() {
                self.pivot_column = None;
                self.pivot_pool_idx = 0;
                self.pivot_pool_table.select(None);
            } else {
                self.pivot_column = pool.first().cloned();
                self.pivot_pool_idx = 0;
                self.pivot_pool_table.select(Some(0));
            }
        }
        self.pivot_fix_value_after_pivot_change();
    }

    pub fn pivot_move_index_selection(&mut self, down: bool) {
        let filtered = self.pivot_filtered_columns();
        let n = filtered.len();
        if n == 0 {
            return;
        }
        let i = self.pivot_index_table.selected().unwrap_or(0);
        let next = if down {
            (i + 1).min(n.saturating_sub(1))
        } else {
            i.saturating_sub(1)
        };
        self.pivot_index_table.select(Some(next));
    }

    pub fn pivot_move_pivot_selection(&mut self, down: bool) {
        let pool = self.pivot_pool();
        let n = pool.len();
        if n == 0 {
            return;
        }
        let i = self.pivot_pool_idx;
        self.pivot_pool_idx = if down {
            (i + 1).min(n - 1)
        } else {
            i.saturating_sub(1)
        };
        self.pivot_column = pool.get(self.pivot_pool_idx).cloned();
        self.pivot_pool_table.select(Some(self.pivot_pool_idx));
        self.pivot_fix_value_after_pivot_change();
    }

    fn pivot_fix_value_after_pivot_change(&mut self) {
        let vpool = self.pivot_value_pool();
        if vpool.is_empty() {
            self.value_column = None;
            self.value_pool_idx = 0;
            self.value_pool_table.select(None);
            return;
        }
        let pivot = self.pivot_column.as_deref();
        let valid = self
            .value_column
            .as_deref()
            .map(|v| pivot != Some(v) && vpool.iter().any(|c| c.as_str() == v))
            .unwrap_or(false);
        if !valid {
            self.value_column = vpool.first().cloned();
            self.value_pool_idx = 0;
            self.value_pool_table.select(Some(0));
            if self.value_column.is_some() {
                let opts = self.pivot_aggregation_options();
                if !opts.is_empty() && self.aggregation_idx >= opts.len() {
                    self.aggregation_idx = opts.len() - 1;
                }
            }
        }
    }

    pub fn pivot_move_value_selection(&mut self, down: bool) {
        let pool = self.pivot_value_pool();
        let n = pool.len();
        if n == 0 {
            return;
        }
        let i = self.value_pool_idx;
        self.value_pool_idx = if down {
            (i + 1).min(n - 1)
        } else {
            i.saturating_sub(1)
        };
        self.value_column = pool.get(self.value_pool_idx).cloned();
        self.value_pool_table.select(Some(self.value_pool_idx));
        if self.value_column.is_some() {
            let opts = self.pivot_aggregation_options();
            if !opts.is_empty() && self.aggregation_idx >= opts.len() {
                self.aggregation_idx = opts.len() - 1;
            }
        }
    }

    pub fn pivot_move_aggregation(&mut self, down: bool) {
        let opts = self.pivot_aggregation_options();
        let n = opts.len();
        if n == 0 {
            return;
        }
        let i = self.aggregation_idx;
        self.aggregation_idx = if down {
            (i + 1) % n
        } else if i == 0 {
            n - 1
        } else {
            i - 1
        };
    }

    // ----- Melt helpers -----

    pub fn melt_filtered_columns(&self) -> Vec<String> {
        let filter_lower = self.melt_filter_input.value.to_lowercase();
        self.available_columns
            .iter()
            .filter(|c| c.to_lowercase().contains(&filter_lower))
            .cloned()
            .collect()
    }

    pub fn melt_index_pool(&self) -> Vec<String> {
        self.melt_filtered_columns()
    }

    pub fn melt_value_pool(&self) -> Vec<String> {
        let idx_set: std::collections::HashSet<_> = self.melt_index_columns.iter().collect();
        self.available_columns
            .iter()
            .filter(|c| !idx_set.contains(*c))
            .cloned()
            .collect()
    }

    fn dtype_matches(&self, col: &str) -> bool {
        let dtype = match self.column_dtypes.get(col) {
            Some(d) => d,
            None => return false,
        };
        match self.melt_type_filter {
            MeltTypeFilter::Numeric => matches!(
                dtype,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
            ),
            MeltTypeFilter::String => matches!(dtype, DataType::String),
            MeltTypeFilter::Datetime => matches!(
                dtype,
                DataType::Datetime(_, _) | DataType::Date | DataType::Time
            ),
            MeltTypeFilter::Boolean => matches!(dtype, DataType::Boolean),
        }
    }

    pub fn melt_resolve_value_columns(&self) -> Result<Vec<String>, String> {
        let pool = self.melt_value_pool();
        match self.melt_value_strategy {
            MeltValueStrategy::AllExceptIndex => {
                if pool.is_empty() {
                    return Err("No columns to melt (all columns are index).".to_string());
                }
                Ok(pool)
            }
            MeltValueStrategy::ByPattern => {
                let re = regex::Regex::new(&self.melt_pattern)
                    .map_err(|e| format!("Invalid pattern: {}", e))?;
                let matched: Vec<String> = pool.into_iter().filter(|c| re.is_match(c)).collect();
                if matched.is_empty() {
                    return Err("Pattern matches no columns.".to_string());
                }
                Ok(matched)
            }
            MeltValueStrategy::ByType => {
                let matched: Vec<String> = self
                    .melt_value_pool()
                    .into_iter()
                    .filter(|c| self.dtype_matches(c))
                    .collect();
                if matched.is_empty() {
                    return Err("No columns of selected type.".to_string());
                }
                Ok(matched)
            }
            MeltValueStrategy::ExplicitList => {
                if self.melt_explicit_list.is_empty() {
                    return Err("Select at least one value column.".to_string());
                }
                Ok(self.melt_explicit_list.clone())
            }
        }
    }

    pub fn melt_validation_error(&self) -> Option<String> {
        if self.melt_index_columns.is_empty() {
            return Some("Select at least one index column.".to_string());
        }
        let v = self.melt_variable_name.trim();
        if v.is_empty() {
            return Some("Variable name cannot be empty.".to_string());
        }
        if self.melt_index_columns.contains(&v.to_string()) {
            return Some("Variable name must not equal an index column.".to_string());
        }
        let w = self.melt_value_name.trim();
        if w.is_empty() {
            return Some("Value name cannot be empty.".to_string());
        }
        if self.melt_index_columns.contains(&w.to_string()) {
            return Some("Value name must not equal an index column.".to_string());
        }
        if v == w {
            return Some("Variable and value names must differ.".to_string());
        }
        match self.melt_resolve_value_columns() {
            Ok(cols) if cols.is_empty() => Some("No value columns selected.".to_string()),
            Err(e) => Some(e),
            Ok(_) => None,
        }
    }

    pub fn build_melt_spec(&self) -> Option<MeltSpec> {
        if self.melt_validation_error().is_some() {
            return None;
        }
        let value_columns = self.melt_resolve_value_columns().ok()?;
        Some(MeltSpec {
            index: self.melt_index_columns.clone(),
            value_columns,
            variable_name: self.melt_variable_name.trim().to_string(),
            value_name: self.melt_value_name.trim().to_string(),
        })
    }

    pub fn melt_toggle_index_at_selection(&mut self) {
        let filtered = self.melt_filtered_columns();
        let i = match self.melt_index_table.selected() {
            Some(i) if i < filtered.len() => i,
            _ => return,
        };
        let col = filtered[i].clone();
        if let Some(pos) = self.melt_index_columns.iter().position(|c| c == &col) {
            self.melt_index_columns.remove(pos);
        } else {
            self.melt_index_columns.push(col);
        }
        self.melt_fix_explicit_after_index_change();
    }

    fn melt_fix_explicit_after_index_change(&mut self) {
        let idx_set: std::collections::HashSet<_> =
            self.melt_index_columns.iter().map(|s| s.as_str()).collect();
        self.melt_explicit_list
            .retain(|c| !idx_set.contains(c.as_str()));
        if !self.melt_explicit_pool().is_empty() && self.melt_explicit_table.selected().is_none() {
            self.melt_explicit_table.select(Some(0));
        }
    }

    pub fn melt_move_index_selection(&mut self, down: bool) {
        let filtered = self.melt_filtered_columns();
        let n = filtered.len();
        if n == 0 {
            return;
        }
        let i = self.melt_index_table.selected().unwrap_or(0);
        let next = if down {
            (i + 1).min(n.saturating_sub(1))
        } else {
            i.saturating_sub(1)
        };
        self.melt_index_table.select(Some(next));
    }

    pub fn melt_move_strategy(&mut self, down: bool) {
        use MeltValueStrategy::{AllExceptIndex, ByPattern, ByType, ExplicitList};
        let strategies = [AllExceptIndex, ByPattern, ByType, ExplicitList];
        let n = strategies.len();
        let i = strategies
            .iter()
            .position(|s| *s == self.melt_value_strategy)
            .unwrap_or(0);
        let next = if down {
            (i + 1) % n
        } else if i == 0 {
            n - 1
        } else {
            i - 1
        };
        self.melt_value_strategy = strategies[next];
    }

    pub fn melt_move_type_filter(&mut self, down: bool) {
        use MeltTypeFilter::{Boolean, Datetime, Numeric, String as Str};
        let types = [Numeric, Str, Datetime, Boolean];
        let n = types.len();
        let i = types
            .iter()
            .position(|t| *t == self.melt_type_filter)
            .unwrap_or(0);
        let next = if down {
            (i + 1) % n
        } else if i == 0 {
            n - 1
        } else {
            i - 1
        };
        self.melt_type_filter = types[next];
    }

    pub fn melt_explicit_pool(&self) -> Vec<String> {
        self.melt_value_pool()
    }

    pub fn melt_toggle_explicit_at_selection(&mut self) {
        let pool = self.melt_explicit_pool();
        let i = match self.melt_explicit_table.selected() {
            Some(i) if i < pool.len() => i,
            _ => return,
        };
        let col = pool[i].clone();
        if let Some(pos) = self.melt_explicit_list.iter().position(|c| c == &col) {
            self.melt_explicit_list.remove(pos);
        } else {
            self.melt_explicit_list.push(col);
        }
    }

    pub fn melt_move_explicit_selection(&mut self, down: bool) {
        let pool = self.melt_explicit_pool();
        let n = pool.len();
        if n == 0 {
            return;
        }
        let i = self.melt_explicit_table.selected().unwrap_or(0);
        let next = if down {
            (i + 1).min(n.saturating_sub(1))
        } else {
            i.saturating_sub(1)
        };
        self.melt_explicit_table.select(Some(next));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pivot_melt_modal_new() {
        let m = PivotMeltModal::new();
        assert!(!m.active);
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
    }

    #[test]
    fn test_open_close() {
        let mut m = PivotMeltModal::new();
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        m.open(1000, &theme);
        assert!(m.active);
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        m.close();
        assert!(!m.active);
    }

    #[test]
    fn test_switch_tab() {
        let mut m = PivotMeltModal::new();
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        m.open(1000, &theme);
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
        m.switch_tab();
        assert!(matches!(m.active_tab, PivotMeltTab::Melt));
        m.switch_tab();
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
    }

    #[test]
    fn test_next_focus() {
        let mut m = PivotMeltModal::new();
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotFilter));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotIndexList));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotPivotCol));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotValueCol));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotAggregation));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::PivotSortToggle));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Apply));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Cancel));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Clear));
        m.next_focus();
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
    }

    #[test]
    fn test_prev_focus() {
        let mut m = PivotMeltModal::new();
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        m.prev_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Clear));
        m.prev_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Cancel));
        m.prev_focus();
        assert!(matches!(m.focus, PivotMeltFocus::Apply));
    }
}

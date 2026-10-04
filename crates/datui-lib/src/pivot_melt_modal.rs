//! Pivot & Melt builder state: a form of rows, each edited through one Picker
//! scoped to that row alone, and the live preview of the spec it stages. The spec
//! is echoed live; Enter applies it.

use crate::filter_modal::FilterStatement;
use crate::widgets::text_input::TextInput;
use crate::widgets::ui::PickerState;
use polars::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PivotMeltTab {
    #[default]
    Pivot,
    Melt,
}

/// Focus: the tab bar, or one row of the active tab's form.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PivotMeltFocus {
    #[default]
    TabBar,
    // Pivot tab
    PivotIndex,
    PivotColumn,
    PivotValue,
    PivotAggregation,
    // Melt tab
    MeltIndex,
    MeltStrategy,
    MeltPattern,
    MeltType,
    MeltColumns,
    MeltVariable,
    MeltValue,
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
    pub const ALL: [Self; 4] = [
        Self::AllExceptIndex,
        Self::ByPattern,
        Self::ByType,
        Self::ExplicitList,
    ];

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
    pub const ALL: [Self; 4] = [Self::Numeric, Self::String, Self::Datetime, Self::Boolean];

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PivotSpec {
    pub index: Vec<String>,
    pub pivot_column: String,
    pub value_column: String,
    pub aggregation: PivotAggregation,
    /// Deprecated: new columns are always sorted alphabetically. Kept for view deserialization.
    #[serde(default)]
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub sort_columns: Option<bool>,
}

/// Spec for melt operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeltSpec {
    pub index: Vec<String>,
    pub value_columns: Vec<String>,
    pub variable_name: String,
    pub value_name: String,
}

/// What a pivot or melt ran over: the query, filters and sort in effect when it was
/// applied. A view replays these before the reshape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReshapeSource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fuzzy_query: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filters: Vec<FilterStatement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_columns: Vec<String>,
    /// Per entry of `sort_columns`, whether it runs descending.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_descending: Vec<bool>,
}

impl ReshapeSource {
    /// Nothing to replay: the reshape ran over the data as loaded.
    pub fn is_empty(&self) -> bool {
        self.query.is_none()
            && self.sql_query.is_none()
            && self.fuzzy_query.is_none()
            && self.filters.is_empty()
            && self.sort_columns.is_empty()
    }

    /// The per-column directions its sort runs, ascending where none was recorded.
    pub fn sort_directions(&self) -> Vec<bool> {
        if self.sort_descending.len() == self.sort_columns.len() {
            self.sort_descending.clone()
        } else {
            vec![false; self.sort_columns.len()]
        }
    }
}

/// Rows of the view the preview reshapes. Small enough that every edit reruns it at
/// once; the head is read once per opening of the builder.
pub const PREVIEW_INPUT_ROWS: usize = 1_000;

/// Rows of the result the preview keeps to draw.
pub const PREVIEW_ROWS: usize = 50;

/// New columns past which the preview warns that a pivot is wide.
pub const PREVIEW_WIDE_PIVOT: usize = 100;

/// A staged reshape the preview runs: what Enter would apply.
#[derive(Debug, Clone, PartialEq)]
pub enum PreviewSpec {
    Pivot(PivotSpec),
    Melt(MeltSpec),
}

/// The head of the view the preview runs over.
#[derive(Debug, Clone)]
pub struct PreviewInput {
    pub rows: std::sync::Arc<polars::prelude::DataFrame>,
    /// The head is all of the view: the preview's shape is the result's.
    pub whole: bool,
}

/// What a preview found: the first rows of the result, and its shape.
#[derive(Debug, Clone)]
pub struct PreviewFrame {
    pub head: polars::prelude::DataFrame,
    pub rows: usize,
    pub columns: usize,
    /// A pivot's new columns, past its index.
    pub new_columns: Option<usize>,
    /// A melt's value columns: each input row becomes this many.
    pub melted_columns: Option<usize>,
}

/// The builder's live preview: the head it runs over, the request in flight, and the
/// last answer. Requests are numbered; one runs at a time, and an edit made while it
/// runs is previewed when it ends, so typing never piles up workers.
#[derive(Debug, Default)]
pub struct ReshapePreview {
    /// Which opening of the builder this is: an answer for an earlier one is dropped.
    pub epoch: u64,
    /// The newest request; an answer for an older one is not shown.
    pub token: u64,
    /// What the newest request is for; `None` while the spec is incomplete.
    pub wanted: Option<PreviewSpec>,
    /// The request whose worker is running.
    pub running: Option<u64>,
    pub input: Option<PreviewInput>,
    /// The view is sorted: the head is read without its order.
    pub sorted: bool,
    /// The view's row count when it was known at opening.
    pub view_rows: Option<usize>,
    /// The answer for `shown.0`: the result, or why it failed.
    pub shown: Option<(PreviewSpec, Result<PreviewFrame, String>)>,
}

impl ReshapePreview {
    /// Whether the preview on screen is not for the spec staged now.
    pub fn stale(&self) -> bool {
        self.wanted.is_some() && self.shown.as_ref().map(|(spec, _)| spec) != self.wanted.as_ref()
    }
}

/// A fresh epoch for each opening of the builder.
fn next_epoch() -> u64 {
    static EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// `spec` run over `input`, in memory: the preview's worker.
pub fn run_preview(
    input: &polars::prelude::DataFrame,
    spec: &PreviewSpec,
) -> Result<PreviewFrame, String> {
    use polars::prelude::IntoLazy;
    let message = |e: color_eyre::Report| crate::error_display::user_message_from_report(&e, None);
    let (result, new_columns, melted_columns) = match spec {
        PreviewSpec::Pivot(spec) => {
            let job =
                crate::widgets::datatable::PivotJob::new(input.clone().lazy(), spec.clone(), false);
            let df = job.run().map_err(message)?;
            let new = df.width().saturating_sub(spec.index.len());
            (df, Some(new), None)
        }
        PreviewSpec::Melt(spec) => {
            let lf = crate::widgets::datatable::DataTableState::melt_lf(input.clone().lazy(), spec)
                .map_err(message)?;
            let df = lf
                .collect()
                .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
            (df, None, Some(spec.value_columns.len()))
        }
    };
    Ok(PreviewFrame {
        head: result.head(Some(PREVIEW_ROWS)),
        rows: result.height(),
        columns: result.width(),
        new_columns,
        melted_columns,
    })
}

pub struct PivotMeltModal {
    pub active: bool,
    /// The live preview of the staged spec.
    pub preview: ReshapePreview,
    pub active_tab: PivotMeltTab,
    pub focus: PivotMeltFocus,

    /// Column names from current schema. Set when opening modal.
    pub available_columns: Vec<String>,
    /// Column name -> DataType. Set when opening.
    pub column_dtypes: HashMap<String, DataType>,

    /// The one Picker, open for the focused row; None while the form has the keys.
    pub picker: Option<PickerState>,

    /// Set when Enter was pressed on an incomplete form: the spec line that
    /// names the gap re-accents instead of a modal repeating it. Any other key
    /// clears it.
    pub attention: bool,

    // Pivot form
    pub index_columns: Vec<String>,
    pub pivot_column: Option<String>,
    pub value_column: Option<String>,
    pub aggregation: PivotAggregation,

    // Melt form
    pub melt_index_columns: Vec<String>,
    pub melt_value_strategy: MeltValueStrategy,
    pub melt_pattern_input: TextInput,
    pub melt_type_filter: MeltTypeFilter,
    pub melt_explicit_list: Vec<String>,
    pub melt_variable_input: TextInput,
    pub melt_value_input: TextInput,
}

impl Default for PivotMeltModal {
    fn default() -> Self {
        let mut modal = Self {
            active: false,
            preview: ReshapePreview::default(),
            active_tab: PivotMeltTab::default(),
            focus: PivotMeltFocus::default(),
            available_columns: Vec::new(),
            column_dtypes: HashMap::new(),
            picker: None,
            attention: false,
            index_columns: Vec::new(),
            pivot_column: None,
            value_column: None,
            aggregation: PivotAggregation::default(),
            melt_index_columns: Vec::new(),
            melt_value_strategy: MeltValueStrategy::default(),
            melt_pattern_input: TextInput::new(),
            melt_type_filter: MeltTypeFilter::default(),
            melt_explicit_list: Vec::new(),
            melt_variable_input: TextInput::new(),
            melt_value_input: TextInput::new(),
        };
        modal.melt_variable_input.suggest("variable");
        modal.melt_value_input.suggest("value");
        modal
    }
}

impl PivotMeltModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self, history_limit: usize, theme: &crate::config::Theme) {
        self.active = true;
        self.preview = ReshapePreview {
            epoch: next_epoch(),
            ..ReshapePreview::default()
        };
        self.active_tab = PivotMeltTab::Pivot;
        self.melt_pattern_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.melt_variable_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.melt_value_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.reset_form();
    }

    pub fn close(&mut self) {
        self.active = false;
        self.picker = None;
        self.preview = ReshapePreview::default();
    }

    pub fn reset_form(&mut self) {
        self.focus = PivotMeltFocus::TabBar;
        self.picker = None;
        self.index_columns.clear();
        self.pivot_column = None;
        self.value_column = None;
        self.aggregation = PivotAggregation::default();
        self.melt_index_columns.clear();
        self.melt_value_strategy = MeltValueStrategy::default();
        self.melt_pattern_input.clear();
        self.melt_type_filter = MeltTypeFilter::default();
        self.melt_explicit_list.clear();
        self.melt_variable_input.suggest("variable");
        self.melt_value_input.suggest("value");
    }

    // ----- Focus -----

    /// The active tab's rows, in Tab order. Melt's value rows follow its strategy.
    pub fn row_order(&self) -> &'static [PivotMeltFocus] {
        use PivotMeltFocus::*;
        match self.active_tab {
            PivotMeltTab::Pivot => &[PivotIndex, PivotColumn, PivotValue, PivotAggregation],
            PivotMeltTab::Melt => match self.melt_value_strategy {
                MeltValueStrategy::AllExceptIndex => {
                    &[MeltIndex, MeltStrategy, MeltVariable, MeltValue]
                }
                MeltValueStrategy::ByPattern => &[
                    MeltIndex,
                    MeltStrategy,
                    MeltPattern,
                    MeltVariable,
                    MeltValue,
                ],
                MeltValueStrategy::ByType => {
                    &[MeltIndex, MeltStrategy, MeltType, MeltVariable, MeltValue]
                }
                MeltValueStrategy::ExplicitList => &[
                    MeltIndex,
                    MeltStrategy,
                    MeltColumns,
                    MeltVariable,
                    MeltValue,
                ],
            },
        }
    }

    pub fn switch_tab(&mut self) {
        self.active_tab = match self.active_tab {
            PivotMeltTab::Pivot => PivotMeltTab::Melt,
            PivotMeltTab::Melt => PivotMeltTab::Pivot,
        };
        self.focus = PivotMeltFocus::TabBar;
        self.picker = None;
    }

    // ----- Rows -----

    /// Rows whose value is a text field rather than a picked choice.
    pub fn is_text_row(&self, focus: PivotMeltFocus) -> bool {
        matches!(
            focus,
            PivotMeltFocus::MeltPattern | PivotMeltFocus::MeltVariable | PivotMeltFocus::MeltValue
        )
    }

    /// Rows whose value steps through a short list: the aggregation, the melt
    /// strategy and its type.
    pub fn is_choice_row(&self, focus: PivotMeltFocus) -> bool {
        matches!(
            focus,
            PivotMeltFocus::PivotAggregation
                | PivotMeltFocus::MeltStrategy
                | PivotMeltFocus::MeltType
        )
    }

    /// Rows edited through the Picker: the column rows.
    pub fn is_picker_row(&self, focus: PivotMeltFocus) -> bool {
        !self.is_text_row(focus) && !self.is_choice_row(focus) && focus != PivotMeltFocus::TabBar
    }

    /// ←/→ (and Space) on a choice row: the next or previous value, wrapping. A
    /// new strategy changes which melt rows follow it.
    pub fn step_choice(&mut self, delta: i8) {
        use crate::form::step_value;
        match self.focus {
            PivotMeltFocus::PivotAggregation => {
                self.aggregation = step_value(&PivotAggregation::ALL, self.aggregation, delta);
            }
            PivotMeltFocus::MeltStrategy => {
                self.melt_value_strategy =
                    step_value(&MeltValueStrategy::ALL, self.melt_value_strategy, delta);
            }
            PivotMeltFocus::MeltType => {
                self.melt_type_filter =
                    step_value(&MeltTypeFilter::ALL, self.melt_type_filter, delta);
            }
            _ => {}
        }
    }

    /// ←/→ on a pick-one column row: the next or previous column, chosen at once.
    /// A row with nothing chosen yet starts at the first (→) or the last (←).
    pub fn step_picker_row(&mut self, delta: i8) {
        if !self.is_picker_row(self.focus) || self.is_multi_row(self.focus) {
            return;
        }
        let chosen = match self.focus {
            PivotMeltFocus::PivotColumn => self.pivot_column.is_some(),
            PivotMeltFocus::PivotValue => self.value_column.is_some(),
            _ => false,
        };
        self.open_picker();
        if let Some(picker) = self.picker.as_mut() {
            if delta < 0 {
                picker.move_up();
            } else if chosen {
                picker.move_down();
            }
        }
        self.picker_choose();
    }

    /// Rows where the Picker toggles several choices rather than picking one.
    pub fn is_multi_row(&self, focus: PivotMeltFocus) -> bool {
        matches!(
            focus,
            PivotMeltFocus::PivotIndex | PivotMeltFocus::MeltIndex | PivotMeltFocus::MeltColumns
        )
    }

    /// The focused row's text field, when it has one.
    pub fn focused_text_input_mut(&mut self) -> Option<&mut TextInput> {
        match self.focus {
            PivotMeltFocus::MeltPattern => Some(&mut self.melt_pattern_input),
            PivotMeltFocus::MeltVariable => Some(&mut self.melt_variable_input),
            PivotMeltFocus::MeltValue => Some(&mut self.melt_value_input),
            _ => None,
        }
    }

    // ----- Picker -----

    /// What the focused row's Picker offers: each row sees only its own pool,
    /// so narrowing one list can never silently empty another.
    pub fn picker_items(&self) -> Vec<String> {
        let minus = |exclude: &[&str]| -> Vec<String> {
            self.available_columns
                .iter()
                .filter(|c| !exclude.contains(&c.as_str()))
                .cloned()
                .collect()
        };
        match self.focus {
            PivotMeltFocus::PivotIndex | PivotMeltFocus::MeltIndex => {
                self.available_columns.clone()
            }
            PivotMeltFocus::PivotColumn => {
                let index: Vec<&str> = self.index_columns.iter().map(|s| s.as_str()).collect();
                minus(&index)
            }
            PivotMeltFocus::PivotValue => {
                let mut exclude: Vec<&str> =
                    self.index_columns.iter().map(|s| s.as_str()).collect();
                if let Some(pivot) = self.pivot_column.as_deref() {
                    exclude.push(pivot);
                }
                minus(&exclude)
            }
            PivotMeltFocus::PivotAggregation => PivotAggregation::ALL
                .iter()
                .map(|a| a.as_str().to_string())
                .collect(),
            PivotMeltFocus::MeltStrategy => MeltValueStrategy::ALL
                .iter()
                .map(|s| s.as_str().to_string())
                .collect(),
            PivotMeltFocus::MeltType => MeltTypeFilter::ALL
                .iter()
                .map(|t| t.as_str().to_string())
                .collect(),
            PivotMeltFocus::MeltColumns => {
                let index: Vec<&str> = self.melt_index_columns.iter().map(|s| s.as_str()).collect();
                minus(&index)
            }
            _ => Vec::new(),
        }
    }

    /// Open the Picker for the focused row, cursor on the current choice.
    pub fn open_picker(&mut self) {
        if !self.is_picker_row(self.focus) {
            return;
        }
        let items = self.picker_items();
        let current = match self.focus {
            PivotMeltFocus::PivotColumn => self.pivot_column.as_deref(),
            PivotMeltFocus::PivotValue => self.value_column.as_deref(),
            PivotMeltFocus::PivotAggregation => Some(self.aggregation.as_str()),
            PivotMeltFocus::MeltStrategy => Some(self.melt_value_strategy.as_str()),
            PivotMeltFocus::MeltType => Some(self.melt_type_filter.as_str()),
            _ => None,
        };
        let mut state = PickerState::new(items.clone());
        if let Some(current) = current
            && let Some(i) = items.iter().position(|item| item == current)
        {
            state.select_original(i);
        }
        self.picker = Some(state);
    }

    /// Enter in the Picker: a pick-one row takes the cursor's item; a toggle
    /// row's choices are already staged. Either way the Picker closes.
    pub fn picker_choose(&mut self) {
        let Some(state) = self.picker.take() else {
            return;
        };
        if self.is_multi_row(self.focus) {
            return;
        }
        let Some(i) = state.selected_original() else {
            return;
        };
        let items = self.picker_items();
        let Some(item) = items.get(i) else {
            return;
        };
        match self.focus {
            PivotMeltFocus::PivotColumn => {
                if self.value_column.as_deref() == Some(item.as_str()) {
                    self.value_column = None;
                }
                self.pivot_column = Some(item.clone());
            }
            PivotMeltFocus::PivotValue => self.value_column = Some(item.clone()),
            PivotMeltFocus::PivotAggregation => self.aggregation = PivotAggregation::ALL[i],
            PivotMeltFocus::MeltStrategy => self.melt_value_strategy = MeltValueStrategy::ALL[i],
            PivotMeltFocus::MeltType => self.melt_type_filter = MeltTypeFilter::ALL[i],
            _ => {}
        }
    }

    /// Space in a toggle row's Picker: flip the cursor's column in or out.
    pub fn picker_toggle(&mut self) {
        let Some(i) = self.picker.as_ref().and_then(|s| s.selected_original()) else {
            return;
        };
        let items = self.picker_items();
        let Some(col) = items.get(i).cloned() else {
            return;
        };
        let toggle = |list: &mut Vec<String>| {
            if let Some(pos) = list.iter().position(|c| *c == col) {
                list.remove(pos);
            } else {
                list.push(col.clone());
            }
        };
        match self.focus {
            PivotMeltFocus::PivotIndex => {
                toggle(&mut self.index_columns);
                // A column moved into the index can no longer pivot or fill values.
                if let Some(pivot) = self.pivot_column.as_deref()
                    && self.index_columns.iter().any(|c| c == pivot)
                {
                    self.pivot_column = None;
                }
                if let Some(value) = self.value_column.as_deref()
                    && self.index_columns.iter().any(|c| c == value)
                {
                    self.value_column = None;
                }
            }
            PivotMeltFocus::MeltIndex => {
                toggle(&mut self.melt_index_columns);
                let index = &self.melt_index_columns;
                self.melt_explicit_list.retain(|c| !index.contains(c));
            }
            PivotMeltFocus::MeltColumns => toggle(&mut self.melt_explicit_list),
            _ => {}
        }
    }

    /// Whether an item in the focused row's Picker is currently chosen.
    pub fn is_marked(&self, item: &str) -> bool {
        match self.focus {
            PivotMeltFocus::PivotIndex => self.index_columns.iter().any(|c| c == item),
            PivotMeltFocus::MeltIndex => self.melt_index_columns.iter().any(|c| c == item),
            PivotMeltFocus::MeltColumns => self.melt_explicit_list.iter().any(|c| c == item),
            _ => false,
        }
    }

    // ----- Spec echo -----

    /// The staged pivot as one line — `index × columns → agg(values)` — or
    /// what is still missing from it.
    pub fn pivot_spec_line(&self, g: &crate::glyphs::Glyphs) -> Result<String, String> {
        if let Some(err) = self.pivot_validation_error() {
            return Err(err);
        }
        let pivot = self.pivot_column.as_deref().unwrap_or_default();
        let value = self.value_column.as_deref().unwrap_or_default();
        Ok(format!(
            "{} {} {} {} {}({})",
            self.index_columns.join(", "),
            g.times,
            pivot,
            g.arrow_right,
            self.aggregation.as_str(),
            value
        ))
    }

    /// The staged melt as one line, or what is still missing from it.
    pub fn melt_spec_line(&self) -> Result<String, String> {
        if let Some(err) = self.melt_validation_error() {
            return Err(err);
        }
        let n = self.melt_resolve_value_columns()?.len();
        let columns = if n == 1 { "column" } else { "columns" };
        Ok(match self.melt_value_strategy {
            MeltValueStrategy::AllExceptIndex => {
                format!("melt {n} value {columns} (all except index)")
            }
            MeltValueStrategy::ByPattern => format!(
                "melt {n} value {columns} by pattern \"{}\"",
                self.melt_pattern_input.value()
            ),
            MeltValueStrategy::ByType => format!(
                "melt {n} {} {columns}",
                self.melt_type_filter.as_str().to_lowercase()
            ),
            MeltValueStrategy::ExplicitList => format!("melt {n} chosen {columns}"),
        })
    }

    // ----- Pivot backend -----

    pub fn pivot_validation_error(&self) -> Option<String> {
        if self.index_columns.is_empty() {
            return Some("Select at least one index column.".to_string());
        }
        let pivot = match &self.pivot_column {
            Some(s) => s,
            None => return Some("Select the column whose values become columns.".to_string()),
        };
        if self.index_columns.contains(pivot) {
            return Some("Pivot column must not be in index.".to_string());
        }
        let value = match &self.value_column {
            Some(s) => s,
            None => return Some("Select the column that fills the cells.".to_string()),
        };
        if self.index_columns.contains(value) || pivot == value {
            return Some("Value column must not be in index or equal to pivot.".to_string());
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
            aggregation: self.aggregation,
            sort_columns: None,
        })
    }

    // ----- Melt backend -----

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
                let re = regex::Regex::new(self.melt_pattern_input.value())
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
        let v = self.melt_variable_input.value().trim().to_string();
        if v.is_empty() {
            return Some("Variable name cannot be empty.".to_string());
        }
        if self.melt_index_columns.contains(&v) {
            return Some("Variable name must not equal an index column.".to_string());
        }
        let w = self.melt_value_input.value().trim().to_string();
        if w.is_empty() {
            return Some("Value name cannot be empty.".to_string());
        }
        if self.melt_index_columns.contains(&w) {
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
            variable_name: self.melt_variable_input.value().trim().to_string(),
            value_name: self.melt_value_input.value().trim().to_string(),
        })
    }

    /// The spec Enter would apply on the active tab, if it is complete.
    pub fn staged_spec(&self) -> Option<PreviewSpec> {
        match self.active_tab {
            PivotMeltTab::Pivot => self.build_pivot_spec().map(PreviewSpec::Pivot),
            PivotMeltTab::Melt => self.build_melt_spec().map(PreviewSpec::Melt),
        }
    }
}

impl crate::form::Form for PivotMeltModal {
    type Field = PivotMeltFocus;

    /// The tab bar, then the active tab's rows. Melt's value rows follow its strategy.
    fn fields(&self) -> Vec<(PivotMeltFocus, crate::form::FieldKind)> {
        use crate::form::FieldKind;
        std::iter::once(PivotMeltFocus::TabBar)
            .chain(self.row_order().iter().copied())
            .map(|row| {
                let kind = if row == PivotMeltFocus::TabBar || self.is_choice_row(row) {
                    FieldKind::Choice
                } else if self.is_text_row(row) {
                    FieldKind::Text
                } else {
                    FieldKind::Picker {
                        multi: self.is_multi_row(row),
                    }
                };
                (row, kind)
            })
            .collect()
    }

    fn focused(&self) -> PivotMeltFocus {
        self.focus
    }

    fn set_focused(&mut self, field: PivotMeltFocus) {
        self.focus = field;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::Form;

    fn modal_with_columns(columns: &[&str]) -> PivotMeltModal {
        let mut m = PivotMeltModal::new();
        m.available_columns = columns.iter().map(|s| s.to_string()).collect();
        m.column_dtypes = columns
            .iter()
            .map(|s| (s.to_string(), DataType::Int64))
            .collect();
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        m.open(1000, &theme);
        m
    }

    #[test]
    fn test_pivot_melt_modal_new() {
        let m = PivotMeltModal::new();
        assert!(!m.active);
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        assert!(m.picker.is_none());
    }

    #[test]
    fn test_open_close() {
        let mut m = modal_with_columns(&["a", "b"]);
        assert!(m.active);
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        m.close();
        assert!(!m.active);
    }

    #[test]
    fn test_switch_tab_returns_to_the_tab_bar() {
        let mut m = modal_with_columns(&["a", "b"]);
        m.focus_next();
        m.switch_tab();
        assert!(matches!(m.active_tab, PivotMeltTab::Melt));
        assert!(matches!(m.focus, PivotMeltFocus::TabBar));
        m.switch_tab();
        assert!(matches!(m.active_tab, PivotMeltTab::Pivot));
    }

    #[test]
    fn tab_walks_the_pivot_rows_and_wraps() {
        let mut m = modal_with_columns(&["a", "b"]);
        let walked: Vec<PivotMeltFocus> = (0..5)
            .map(|_| {
                m.focus_next();
                m.focus
            })
            .collect();
        assert_eq!(
            walked,
            vec![
                PivotMeltFocus::PivotIndex,
                PivotMeltFocus::PivotColumn,
                PivotMeltFocus::PivotValue,
                PivotMeltFocus::PivotAggregation,
                PivotMeltFocus::TabBar,
            ]
        );
        m.focus_prev();
        assert_eq!(m.focus, PivotMeltFocus::PivotAggregation);
    }

    /// The melt rows follow the strategy: only the active strategy's own row
    /// is walkable, so Tab never lands on a control that does nothing.
    #[test]
    fn the_melt_rows_follow_the_strategy() {
        let mut m = modal_with_columns(&["a", "b"]);
        m.switch_tab();
        assert!(!m.row_order().contains(&PivotMeltFocus::MeltPattern));
        m.melt_value_strategy = MeltValueStrategy::ByPattern;
        assert!(m.row_order().contains(&PivotMeltFocus::MeltPattern));
        assert!(!m.row_order().contains(&PivotMeltFocus::MeltType));
        m.melt_value_strategy = MeltValueStrategy::ExplicitList;
        assert!(m.row_order().contains(&PivotMeltFocus::MeltColumns));
    }

    /// Each row's Picker sees only its own pool: the index never offers what
    /// cannot be an index, and the value list excludes the index and the
    /// pivot column — the cross-list filter trap is structurally gone.
    #[test]
    fn each_picker_is_scoped_to_its_row() {
        let mut m = modal_with_columns(&["a", "b", "c", "d"]);
        m.index_columns = vec!["a".to_string()];
        m.pivot_column = Some("b".to_string());

        m.focus = PivotMeltFocus::PivotIndex;
        assert_eq!(m.picker_items(), ["a", "b", "c", "d"]);
        m.focus = PivotMeltFocus::PivotColumn;
        assert_eq!(m.picker_items(), ["b", "c", "d"]);
        m.focus = PivotMeltFocus::PivotValue;
        assert_eq!(m.picker_items(), ["c", "d"]);
    }

    #[test]
    fn toggling_a_column_into_the_index_clears_a_now_invalid_choice() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.pivot_column = Some("a".to_string());
        m.value_column = Some("b".to_string());
        m.focus = PivotMeltFocus::PivotIndex;
        m.open_picker();
        m.picker_toggle(); // toggles "a", the cursor's initial item
        assert_eq!(m.index_columns, ["a"]);
        assert_eq!(m.pivot_column, None, "a is index now, not a pivot column");
        assert_eq!(m.value_column, Some("b".to_string()), "b is untouched");
    }

    #[test]
    fn choosing_the_value_column_as_pivot_clears_the_value() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.value_column = Some("b".to_string());
        m.focus = PivotMeltFocus::PivotColumn;
        m.open_picker();
        m.picker.as_mut().unwrap().select_original(1); // "b"
        m.picker_choose();
        assert_eq!(m.pivot_column, Some("b".to_string()));
        assert_eq!(m.value_column, None);
        assert!(m.picker.is_none(), "choosing closes the picker");
    }

    #[test]
    fn the_picker_opens_on_the_current_choice() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.pivot_column = Some("c".to_string());
        m.focus = PivotMeltFocus::PivotColumn;
        m.open_picker();
        let state = m.picker.as_ref().unwrap();
        assert_eq!(state.selected_original(), Some(2), "c is item 2");
    }

    #[test]
    fn choice_rows_step_and_wrap() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.focus = PivotMeltFocus::PivotAggregation;
        m.step_choice(-1);
        assert_eq!(m.aggregation, PivotAggregation::Count, "last wraps back");
        m.step_choice(1);
        assert_eq!(m.aggregation, PivotAggregation::Last);
        m.switch_tab();
        m.focus = PivotMeltFocus::MeltStrategy;
        m.step_choice(1);
        assert_eq!(m.melt_value_strategy, MeltValueStrategy::ByPattern);
        assert!(m.row_order().contains(&PivotMeltFocus::MeltPattern));
    }

    #[test]
    fn a_column_row_steps_through_its_own_pool() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.index_columns = vec!["a".to_string()];
        m.focus = PivotMeltFocus::PivotColumn;
        m.step_picker_row(1);
        assert_eq!(
            m.pivot_column.as_deref(),
            Some("b"),
            "the index is not offered"
        );
        m.step_picker_row(1);
        assert_eq!(m.pivot_column.as_deref(), Some("c"));
        assert!(m.picker.is_none(), "stepping never leaves the picker open");
    }

    #[test]
    fn a_melt_index_toggle_drops_the_column_from_the_explicit_list() {
        let mut m = modal_with_columns(&["a", "b", "c"]);
        m.switch_tab();
        m.melt_explicit_list = vec!["a".to_string(), "b".to_string()];
        m.focus = PivotMeltFocus::MeltIndex;
        m.open_picker();
        m.picker_toggle(); // "a" into the index
        assert_eq!(m.melt_index_columns, ["a"]);
        assert_eq!(m.melt_explicit_list, ["b"]);
    }

    #[test]
    fn the_pivot_spec_line_echoes_the_full_spec_or_names_the_gap() {
        let g = crate::glyphs::unicode();
        let mut m = modal_with_columns(&["dept", "job", "salary"]);
        assert!(m.pivot_spec_line(g).is_err(), "nothing chosen yet");
        m.index_columns = vec!["dept".to_string()];
        m.pivot_column = Some("job".to_string());
        m.value_column = Some("salary".to_string());
        m.aggregation = PivotAggregation::Avg;
        assert_eq!(
            m.pivot_spec_line(g).unwrap(),
            "dept × job → avg(salary)".to_string()
        );
    }

    #[test]
    fn the_melt_spec_line_counts_what_the_strategy_resolves() {
        let mut m = modal_with_columns(&["id", "q1", "q2", "q3"]);
        m.switch_tab();
        m.melt_index_columns = vec!["id".to_string()];
        assert_eq!(
            m.melt_spec_line().unwrap(),
            "melt 3 value columns (all except index)"
        );
        m.melt_value_strategy = MeltValueStrategy::ByPattern;
        m.melt_pattern_input.set_value("q[12]");
        assert_eq!(
            m.melt_spec_line().unwrap(),
            "melt 2 value columns by pattern \"q[12]\""
        );
    }

    #[test]
    fn esc_worthy_state_dies_with_reset() {
        let mut m = modal_with_columns(&["a", "b"]);
        m.index_columns = vec!["a".to_string()];
        m.melt_pattern_input.set_value("x");
        m.reset_form();
        assert!(m.index_columns.is_empty());
        assert_eq!(m.melt_pattern_input.value(), "");
        assert_eq!(m.melt_variable_input.value(), "variable");
        assert_eq!(m.melt_value_input.value(), "value");
    }
}

use crate::data_quality::{
    DataQualityPlan, DataQualityResults, QualityComparison, QualityCompute, QualityGrain,
    QualityMetric, QualityPage, QualityScope, TemporalRole, TemporalRoleAssignment,
};
use crate::statistics::{AnalysisResults, DistributionType};
use crate::widgets::text_input::TextInput;
use ratatui::widgets::TableState;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisView {
    #[default]
    Main, // Main tool view
    DistributionDetail, // Full-screen distribution detail view
    CorrelationDetail,  // Full-screen correlation pair detail view
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisTool {
    #[default]
    Describe, // Column describe table
    DistributionAnalysis, // Distribution analysis table
    CorrelationMatrix,    // Correlation matrix
    DataQuality,          // Multi-scale quality profile
}

/// Progress state for the analysis progress overlay (display only).
#[derive(Debug, Clone)]
pub struct AnalysisProgress {
    pub phase: String,
    pub current: usize,
    pub total: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisFocus {
    #[default]
    Main, // Focus on main area (tool view)
    Sidebar,              // Focus on sidebar (tool list)
    DistributionSelector, // Focus on distribution selector in detail view
}

#[derive(Default)]
pub struct AnalysisModal {
    pub active: bool,
    pub scroll_position: usize,
    pub selected_column: Option<usize>,
    pub describe_column_offset: usize, // For horizontal scrolling in describe table
    pub distribution_column_offset: usize, // For horizontal scrolling in distribution table
    pub correlation_column_offset: usize, // For horizontal scrolling in correlation matrix
    pub random_seed: u64,
    pub table_state: TableState,              // For describe table
    pub distribution_table_state: TableState, // For distribution table
    pub correlation_table_state: TableState,  // For correlation matrix
    pub sidebar_state: TableState,            // For sidebar tool list
    /// Cached results per tool; each tool computes and stores its own state independently.
    pub describe_results: Option<AnalysisResults>,
    pub distribution_results: Option<AnalysisResults>,
    pub correlation_results: Option<AnalysisResults>,
    pub data_quality_results: Option<DataQualityResults>,
    /// When Some, show progress overlay (phase, current/total); in-progress data lives in App.
    pub computing: Option<AnalysisProgress>,
    pub show_help: bool,
    pub view: AnalysisView,
    pub focus: AnalysisFocus,
    /// None = no tool selected yet (show instructions); Some(tool) = user chose a tool (may be computing or showing results).
    pub selected_tool: Option<AnalysisTool>,
    pub selected_distribution: Option<usize>, // Selected row in distribution table
    pub selected_correlation: Option<(usize, usize)>, // Selected cell in correlation matrix (row, col)
    pub detail_section: usize, // Current section in detail view (0=Characteristics, 1=Outliers, 2=Percentiles)
    pub selected_theoretical_distribution: DistributionType, // Selected theoretical distribution for Q-Q plot
    pub distribution_selector_state: TableState,             // For distribution selector list
    pub histogram_scale: HistogramScale,                     // Scale for histogram (linear or log)
    pub data_quality_page: QualityPage,
    pub data_quality_plan: DataQualityPlan,
    pub data_quality_table_state: TableState,
    pub data_quality_editing: bool,
    pub data_quality_plan_field: usize,
    pub data_quality_scope_input: TextInput,
    pub data_quality_scope_error: Option<String>,
    pub data_quality_scope_file_offset: usize,
    pub data_quality_show_access: bool,
    pub data_quality_observation_detail: bool,
    pub data_quality_confirm_run: bool,
    pub data_quality_plan_before_edit: Option<DataQualityPlan>,
    pub data_quality_last_plan: Option<DataQualityPlan>,
    pub data_quality_from_cache: bool,
    pub data_quality_metric: QualityMetric,
    pub data_quality_column_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistogramScale {
    #[default]
    Linear,
    Log,
}

impl AnalysisModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self) {
        self.active = true;
        self.scroll_position = 0;
        self.selected_column = None;
        self.describe_column_offset = 0;
        self.distribution_column_offset = 0;
        self.correlation_column_offset = 0;
        self.table_state.select(Some(0));
        self.distribution_table_state.select(Some(0));
        self.correlation_table_state.select(Some(0));
        self.sidebar_state.select(Some(0)); // Highlight first tool; user must press Enter to select
        self.view = AnalysisView::Main;
        self.focus = AnalysisFocus::Sidebar; // Sidebar focused by default when no tool selected
        self.selected_tool = None; // No tool until user selects from sidebar
        self.selected_distribution = Some(0);
        self.selected_correlation = Some((0, 0));
        self.detail_section = 0;
        self.computing = None;
        self.describe_results = None;
        self.distribution_results = None;
        self.correlation_results = None;
        self.data_quality_results = None;
        self.data_quality_page = QualityPage::Plan;
        self.data_quality_plan = DataQualityPlan::default();
        self.data_quality_table_state.select(Some(0));
        self.data_quality_editing = false;
        self.data_quality_plan_field = 0;
        self.data_quality_scope_input = TextInput::new();
        self.data_quality_scope_error = None;
        self.data_quality_scope_file_offset = 0;
        self.data_quality_show_access = false;
        self.data_quality_observation_detail = false;
        self.data_quality_confirm_run = false;
        self.data_quality_plan_before_edit = None;
        self.data_quality_last_plan = None;
        self.data_quality_from_cache = false;
        self.data_quality_metric = QualityMetric::NullRate;
        self.data_quality_column_index = 0;
        // Generate initial random seed (use 0 if system time is before UNIX_EPOCH)
        self.random_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.scroll_position = 0;
        self.selected_column = None;
        self.describe_column_offset = 0;
        self.distribution_column_offset = 0;
        self.correlation_column_offset = 0;
        self.view = AnalysisView::Main;
        self.focus = AnalysisFocus::Main;
        self.selected_tool = None;
        self.selected_distribution = None;
        self.selected_correlation = None;
        self.detail_section = 0;
        self.computing = None;
        self.describe_results = None;
        self.distribution_results = None;
        self.correlation_results = None;
        self.data_quality_results = None;
        self.data_quality_page = QualityPage::Plan;
        self.data_quality_editing = false;
        self.data_quality_scope_input = TextInput::new();
        self.data_quality_scope_error = None;
        self.data_quality_scope_file_offset = 0;
        self.data_quality_show_access = false;
        self.data_quality_observation_detail = false;
        self.data_quality_confirm_run = false;
        self.data_quality_plan_before_edit = None;
        self.data_quality_last_plan = None;
        self.data_quality_from_cache = false;
        self.data_quality_metric = QualityMetric::NullRate;
        self.data_quality_column_index = 0;
    }

    /// Returns the cached results for the currently selected tool, if any.
    pub fn current_results(&self) -> Option<&AnalysisResults> {
        match self.selected_tool {
            Some(AnalysisTool::Describe) => self.describe_results.as_ref(),
            Some(AnalysisTool::DistributionAnalysis) => self.distribution_results.as_ref(),
            Some(AnalysisTool::CorrelationMatrix) => self.correlation_results.as_ref(),
            Some(AnalysisTool::DataQuality) => None,
            None => None,
        }
    }

    pub fn switch_focus(&mut self) {
        if self.view == AnalysisView::DistributionDetail {
            self.focus = match self.focus {
                AnalysisFocus::Main => AnalysisFocus::DistributionSelector,
                AnalysisFocus::DistributionSelector => AnalysisFocus::Main,
                _ => AnalysisFocus::DistributionSelector,
            };
        } else {
            self.focus = match self.focus {
                AnalysisFocus::Main => AnalysisFocus::Sidebar,
                AnalysisFocus::Sidebar => AnalysisFocus::Main,
                _ => AnalysisFocus::Main,
            };
        }
    }

    pub fn select_tool(&mut self) {
        if let Some(idx) = self.sidebar_state.selected() {
            self.selected_tool = Some(match idx {
                0 => AnalysisTool::Describe,
                1 => AnalysisTool::DistributionAnalysis,
                2 => AnalysisTool::CorrelationMatrix,
                3 => AnalysisTool::DataQuality,
                _ => AnalysisTool::Describe,
            });
            self.focus = AnalysisFocus::Main;
        }
    }

    pub fn next_tool(&mut self) {
        if let Some(current) = self.sidebar_state.selected() {
            let next = (current + 1).min(3);
            self.sidebar_state.select(Some(next));
        }
    }

    pub fn previous_tool(&mut self) {
        if let Some(current) = self.sidebar_state.selected()
            && current > 0
        {
            self.sidebar_state.select(Some(current - 1));
        }
    }

    pub fn open_distribution_detail(&mut self) {
        if self.focus == AnalysisFocus::Main
            && self.selected_tool == Some(AnalysisTool::DistributionAnalysis)
            && let Some(idx) = self.distribution_table_state.selected()
        {
            if let Some(results) = &self.distribution_results
                && let Some(dist_analysis) = results.distribution_analyses.get(idx)
            {
                self.selected_theoretical_distribution = dist_analysis.distribution_type;
            }
            self.view = AnalysisView::DistributionDetail;
            self.detail_section = 0;
            self.focus = AnalysisFocus::DistributionSelector;
            if self.selected_theoretical_distribution == DistributionType::Unknown {
                self.selected_theoretical_distribution = DistributionType::Normal;
            }
            self.distribution_selector_state.select(None);
        }
    }

    pub fn open_correlation_detail(&mut self) {
        if self.focus == AnalysisFocus::Main
            && self.selected_tool == Some(AnalysisTool::CorrelationMatrix)
            && let Some((row, col)) = self.selected_correlation
            && row != col
        {
            self.view = AnalysisView::CorrelationDetail;
        }
    }

    pub fn close_detail(&mut self) {
        self.view = AnalysisView::Main;
        self.detail_section = 0;
        self.focus = AnalysisFocus::Main;
    }

    pub fn next_detail_section(&mut self) {
        self.detail_section = (self.detail_section + 1) % 3;
    }

    pub fn previous_detail_section(&mut self) {
        self.detail_section = if self.detail_section == 0 {
            2
        } else {
            self.detail_section - 1
        };
    }

    pub fn scroll_left(&mut self) {
        match self.selected_tool {
            Some(AnalysisTool::Describe) if self.describe_column_offset > 0 => {
                self.describe_column_offset -= 1;
            }
            Some(AnalysisTool::DistributionAnalysis) if self.distribution_column_offset > 0 => {
                self.distribution_column_offset -= 1;
            }
            _ => {}
        }
    }

    pub fn scroll_right(&mut self, max_columns: usize, visible_columns: usize) {
        match self.selected_tool {
            Some(AnalysisTool::Describe) => {
                let offset = &mut self.describe_column_offset;
                if *offset + visible_columns < max_columns
                    && *offset < max_columns.saturating_sub(1)
                {
                    *offset += 1;
                }
            }
            Some(AnalysisTool::DistributionAnalysis) => {
                let offset = &mut self.distribution_column_offset;
                if *offset + visible_columns < max_columns
                    && *offset < max_columns.saturating_sub(1)
                {
                    *offset += 1;
                }
            }
            _ => {}
        }
    }

    pub fn recalculate(&mut self) {
        self.random_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
    }

    pub fn quality_row_count(&self) -> usize {
        let Some(results) = self.data_quality_results.as_ref() else {
            return 0;
        };
        match self.data_quality_page {
            QualityPage::Plan => 6,
            QualityPage::Scope => 0,
            QualityPage::TimeRoles => TemporalRole::ALL.len(),
            QualityPage::Overview => results.observations.len(),
            QualityPage::Columns | QualityPage::Detail => results.columns.len(),
            QualityPage::Segments => results.segments.len(),
            QualityPage::Trends => results.temporal.len(),
        }
    }

    pub fn set_quality_page(&mut self, page: QualityPage) {
        self.data_quality_page = page;
        self.data_quality_observation_detail = false;
        self.data_quality_table_state.select(Some(0));
    }

    pub fn cycle_quality_metric(&mut self) {
        let current = QualityMetric::ALL
            .iter()
            .position(|metric| *metric == self.data_quality_metric)
            .unwrap_or(0);
        self.data_quality_metric = QualityMetric::ALL[(current + 1) % QualityMetric::ALL.len()];
    }

    pub fn cycle_quality_column(&mut self, count: usize, forward: bool) {
        if count == 0 {
            self.data_quality_column_index = 0;
        } else if forward {
            self.data_quality_column_index = (self.data_quality_column_index + 1) % count;
        } else {
            self.data_quality_column_index = (self.data_quality_column_index + count - 1) % count;
        }
    }

    pub fn adjust_quality_plan(&mut self, forward: bool, partition_columns: &[String]) {
        match self.data_quality_plan_field {
            0 => {
                let choices = [
                    QualityScope::CurrentView,
                    QualityScope::WholeSource,
                    QualityScope::FirstRows(10_000),
                    QualityScope::FirstRows(1_000_000),
                ];
                let current = choices
                    .iter()
                    .position(|choice| choice == &self.data_quality_plan.scope)
                    .unwrap_or(0);
                let next = if forward {
                    (current + 1) % choices.len()
                } else {
                    (current + choices.len() - 1) % choices.len()
                };
                self.data_quality_plan.scope = choices[next].clone();
                self.data_quality_plan.baseline_segment = None;
            }
            1 => {
                let mut choices = vec![QualityGrain::Dataset, QualityGrain::File];
                choices.extend(
                    partition_columns
                        .iter()
                        .cloned()
                        .map(QualityGrain::Partition),
                );
                choices.push(QualityGrain::RowChunks(1_000_000));
                choices.extend(
                    self.data_quality_plan
                        .temporal_roles
                        .iter()
                        .map(|assignment| QualityGrain::TimeWindows {
                            column: assignment.column.clone(),
                            every: "1w".to_string(),
                        }),
                );
                let current = choices
                    .iter()
                    .position(|choice| choice == &self.data_quality_plan.grain)
                    .unwrap_or(0);
                let next = if forward {
                    (current + 1) % choices.len()
                } else if current == 0 {
                    choices.len() - 1
                } else {
                    current - 1
                };
                self.data_quality_plan.grain = choices[next].clone();
                self.data_quality_plan.baseline_segment = None;
            }
            2 => {
                self.data_quality_plan.compute = match (self.data_quality_plan.compute, forward) {
                    (QualityCompute::Metadata, true) | (QualityCompute::Sample, false) => {
                        QualityCompute::Sample
                    }
                    (QualityCompute::Sample, true) | (QualityCompute::Full, false) => {
                        QualityCompute::Full
                    }
                    (QualityCompute::Full, true) => QualityCompute::Metadata,
                    (QualityCompute::Metadata, false) => QualityCompute::Full,
                };
            }
            3 => {
                self.data_quality_plan.comparison =
                    match (self.data_quality_plan.comparison, forward) {
                        (QualityComparison::None, true) | (QualityComparison::Previous, false) => {
                            QualityComparison::Previous
                        }
                        (QualityComparison::Previous, true)
                        | (QualityComparison::Baseline, false) => QualityComparison::Baseline,
                        (QualityComparison::Baseline, true) => QualityComparison::None,
                        (QualityComparison::None, false) => QualityComparison::Baseline,
                    };
                if self.data_quality_plan.comparison != QualityComparison::Baseline {
                    self.data_quality_plan.baseline_segment = None;
                }
            }
            5 => {
                let choices = [None, Some(3_600), Some(86_400), Some(604_800)];
                let current = choices
                    .iter()
                    .position(|choice| *choice == self.data_quality_plan.latency_threshold_seconds)
                    .unwrap_or(0);
                let next = if forward {
                    (current + 1) % choices.len()
                } else if current == 0 {
                    choices.len() - 1
                } else {
                    current - 1
                };
                self.data_quality_plan.latency_threshold_seconds = choices[next];
            }
            _ => {}
        }
    }

    pub fn cycle_quality_time_role(
        &mut self,
        role_index: usize,
        columns: &[String],
        forward: bool,
    ) {
        let Some(role) = TemporalRole::ALL.get(role_index).copied() else {
            return;
        };
        let current = self
            .data_quality_plan
            .temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .and_then(|assignment| columns.iter().position(|name| name == &assignment.column))
            .map(|index| index + 1)
            .unwrap_or(0);
        let choices = columns.len() + 1;
        let next = if forward {
            (current + 1) % choices
        } else if current == 0 {
            choices - 1
        } else {
            current - 1
        };
        self.data_quality_plan
            .temporal_roles
            .retain(|assignment| assignment.role != role);
        if next > 0 {
            self.data_quality_plan
                .temporal_roles
                .push(TemporalRoleAssignment {
                    role,
                    column: columns[next - 1].clone(),
                    timezone: None,
                });
        }
    }

    pub fn next_row(&mut self, max_rows: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            self.next_tool();
            return;
        }
        match self.selected_tool {
            Some(AnalysisTool::Describe) => {
                if let Some(current) = self.table_state.selected() {
                    let next = (current + 1).min(max_rows.saturating_sub(1));
                    self.table_state.select(Some(next));
                } else {
                    self.table_state.select(Some(0));
                }
            }
            Some(AnalysisTool::DistributionAnalysis) => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = (current + 1).min(max_rows.saturating_sub(1));
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                } else {
                    self.distribution_table_state.select(Some(0));
                    self.selected_distribution = Some(0);
                }
            }
            Some(AnalysisTool::CorrelationMatrix) => {
                if let Some((row, col)) = self.selected_correlation {
                    let next_row = (row + 1).min(max_rows.saturating_sub(1));
                    self.selected_correlation = Some((next_row, col));
                    self.correlation_table_state.select(Some(next_row));
                }
            }
            Some(AnalysisTool::DataQuality) => {
                let current = self.data_quality_table_state.selected().unwrap_or(0);
                self.data_quality_table_state
                    .select(Some((current + 1).min(max_rows.saturating_sub(1))));
            }
            None => {}
        }
    }

    pub fn previous_row(&mut self) {
        if self.focus == AnalysisFocus::Sidebar {
            self.previous_tool();
            return;
        }
        match self.selected_tool {
            Some(AnalysisTool::Describe) => {
                if let Some(current) = self.table_state.selected()
                    && current > 0
                {
                    self.table_state.select(Some(current - 1));
                }
            }
            Some(AnalysisTool::DistributionAnalysis) => {
                if let Some(current) = self.distribution_table_state.selected()
                    && current > 0
                {
                    let prev = current - 1;
                    self.distribution_table_state.select(Some(prev));
                    self.selected_distribution = Some(prev);
                }
            }
            Some(AnalysisTool::CorrelationMatrix) => {
                if let Some((row, col)) = self.selected_correlation
                    && row > 0
                {
                    let prev_row = row - 1;
                    self.selected_correlation = Some((prev_row, col));
                    self.correlation_table_state.select(Some(prev_row));
                }
            }
            Some(AnalysisTool::DataQuality) => {
                let current = self.data_quality_table_state.selected().unwrap_or(0);
                self.data_quality_table_state
                    .select(Some(current.saturating_sub(1)));
            }
            None => {}
        }
    }

    pub fn page_down(&mut self, max_rows: usize, page_size: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            return;
        }

        match self.selected_tool {
            Some(AnalysisTool::Describe) => {
                if let Some(current) = self.table_state.selected() {
                    let next = (current + page_size).min(max_rows.saturating_sub(1));
                    self.table_state.select(Some(next));
                }
            }
            Some(AnalysisTool::DistributionAnalysis) => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = (current + page_size).min(max_rows.saturating_sub(1));
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                }
            }
            Some(AnalysisTool::CorrelationMatrix) => {
                if let Some((row, col)) = self.selected_correlation {
                    let next_row = (row + page_size).min(max_rows.saturating_sub(1));
                    self.selected_correlation = Some((next_row, col));
                    self.correlation_table_state.select(Some(next_row));
                }
            }
            Some(AnalysisTool::DataQuality) => {
                let current = self.data_quality_table_state.selected().unwrap_or(0);
                self.data_quality_table_state
                    .select(Some((current + page_size).min(max_rows.saturating_sub(1))));
            }
            None => {}
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            return;
        }

        match self.selected_tool {
            Some(AnalysisTool::Describe) => {
                if let Some(current) = self.table_state.selected() {
                    let next = current.saturating_sub(page_size);
                    self.table_state.select(Some(next));
                }
            }
            Some(AnalysisTool::DistributionAnalysis) => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = current.saturating_sub(page_size);
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                }
            }
            Some(AnalysisTool::CorrelationMatrix) => {
                if let Some((row, col)) = self.selected_correlation {
                    let prev_row = row.saturating_sub(page_size);
                    self.selected_correlation = Some((prev_row, col));
                    self.correlation_table_state.select(Some(prev_row));
                }
            }
            Some(AnalysisTool::DataQuality) => {
                let current = self.data_quality_table_state.selected().unwrap_or(0);
                self.data_quality_table_state
                    .select(Some(current.saturating_sub(page_size)));
            }
            None => {}
        }
    }

    pub fn move_correlation_cell(
        &mut self,
        direction: (i32, i32),
        max_rows: usize,
        max_cols: usize,
        visible_cols: usize,
    ) {
        if let Some((row, col)) = self.selected_correlation {
            let new_row = ((row as i32) + direction.0)
                .max(0)
                .min((max_rows - 1) as i32) as usize;
            let new_col = ((col as i32) + direction.1)
                .max(0)
                .min((max_cols - 1) as i32) as usize;
            self.selected_correlation = Some((new_row, new_col));
            self.correlation_table_state.select(Some(new_row));

            if new_col < self.correlation_column_offset {
                self.correlation_column_offset = new_col;
            } else if new_col >= self.correlation_column_offset + visible_cols.saturating_sub(1) {
                if new_col >= visible_cols {
                    self.correlation_column_offset =
                        new_col.saturating_sub(visible_cols.saturating_sub(1));
                } else {
                    self.correlation_column_offset = 0;
                }
            }
        }
    }

    pub fn next_distribution(&mut self) {
        let max_idx = 13;

        if let Some(current) = self.distribution_selector_state.selected() {
            let next = (current + 1).min(max_idx);
            self.distribution_selector_state.select(Some(next));
            self.select_distribution();
        } else {
            self.distribution_selector_state.select(Some(0));
            self.select_distribution();
        }
    }

    pub fn previous_distribution(&mut self) {
        if let Some(current) = self.distribution_selector_state.selected() {
            if current > 0 {
                self.distribution_selector_state.select(Some(current - 1));
                self.select_distribution();
            }
        } else {
            self.distribution_selector_state.select(Some(0));
            self.select_distribution();
        }
    }

    pub fn select_distribution(&mut self) {
        if let Some(idx) = self.distribution_selector_state.selected()
            && let Some(results) = &self.distribution_results
        {
            let dist_analysis_idx = self.distribution_table_state.selected().unwrap_or(0);
            if let Some(dist_analysis) = results.distribution_analyses.get(dist_analysis_idx) {
                // Use the same distribution list and p-value lookup as the widget
                let distributions = [
                    ("Normal", DistributionType::Normal),
                    ("LogNormal", DistributionType::LogNormal),
                    ("Uniform", DistributionType::Uniform),
                    ("PowerLaw", DistributionType::PowerLaw),
                    ("Exponential", DistributionType::Exponential),
                    ("Beta", DistributionType::Beta),
                    ("Gamma", DistributionType::Gamma),
                    ("Chi-Squared", DistributionType::ChiSquared),
                    ("Student's t", DistributionType::StudentsT),
                    ("Poisson", DistributionType::Poisson),
                    ("Bernoulli", DistributionType::Bernoulli),
                    ("Binomial", DistributionType::Binomial),
                    ("Geometric", DistributionType::Geometric),
                    ("Weibull", DistributionType::Weibull),
                ];

                let mut distribution_scores: Vec<(DistributionType, f64)> = distributions
                    .iter()
                    .map(|(_, dist_type)| {
                        let p_value = dist_analysis
                            .all_distribution_pvalues
                            .get(dist_type)
                            .copied()
                            .unwrap_or_else(|| {
                                if *dist_type == DistributionType::Geometric {
                                    0.01
                                } else {
                                    0.0
                                }
                            });
                        (*dist_type, p_value)
                    })
                    .collect();

                distribution_scores
                    .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

                let valid_idx = idx.min(distribution_scores.len().saturating_sub(1));
                if let Some((dist_type, _)) = distribution_scores.get(valid_idx) {
                    self.selected_theoretical_distribution = *dist_type;
                    if idx != valid_idx {
                        self.distribution_selector_state.select(Some(valid_idx));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod quality_scope_tests {
    use super::*;

    #[test]
    fn scope_choices_wrap_without_changing_grain() {
        let mut modal = AnalysisModal::new();
        modal.data_quality_plan_field = 0;
        modal.adjust_quality_plan(true, &[]);
        assert_eq!(modal.data_quality_plan.scope, QualityScope::WholeSource);
        modal.adjust_quality_plan(false, &[]);
        assert_eq!(modal.data_quality_plan.scope, QualityScope::CurrentView);
        modal.adjust_quality_plan(false, &[]);
        assert_eq!(
            modal.data_quality_plan.scope,
            QualityScope::FirstRows(1_000_000)
        );
        assert_eq!(modal.data_quality_plan.grain, QualityGrain::Dataset);
    }
}

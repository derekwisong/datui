use crate::data_quality::{
    DataQualityPlan, DataQualityResults, QUALITY_WINDOW_WIDTHS, QualityComparison, QualityCompute,
    QualityGrain, QualityMetric, QualityPage, TemporalRole, TemporalRoleAssignment,
};
use crate::statistics::{AnalysisResults, DistributionType};
use ratatui::widgets::TableState;

/// What a plan field's picker sets.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanChoice {
    Grain(QualityGrain),
    Values(QualityCompute),
    Compare(QualityComparison),
    Latency(Option<i64>),
}

impl PlanChoice {
    fn is_current(&self, plan: &DataQualityPlan) -> bool {
        match self {
            Self::Grain(grain) => &plan.grain == grain,
            Self::Values(QualityCompute::Metadata) => plan.compute == QualityCompute::Metadata,
            Self::Values(_) => plan.compute != QualityCompute::Metadata,
            Self::Compare(comparison) => &plan.comparison == comparison,
            Self::Latency(seconds) => &plan.latency_threshold_seconds == seconds,
        }
    }
}

/// A plan field's choices, open as a list.
#[derive(Debug, Clone)]
pub struct PlanPicker {
    pub field: usize,
    pub choices: Vec<PlanChoice>,
    pub state: crate::widgets::ui::PickerState,
}

/// What the data offers the plan's choices: partition columns, date columns
/// (and whether they hold times of day), and whether there are files to split by.
#[derive(Debug, Clone, Default)]
pub struct PlanContext {
    pub partitions: Vec<String>,
    pub time_columns: Vec<(String, bool)>,
    pub files: bool,
}

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
    /// When the run began, for the elapsed time on screen. A run is one Polars query
    /// with no steps to count, so time is the only progress there is to show.
    pub started: std::time::Instant,
}

impl AnalysisProgress {
    pub fn new(phase: &str) -> Self {
        Self {
            phase: phase.to_string(),
            started: std::time::Instant::now(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisFocus {
    #[default]
    Main, // Focus on main area (tool view)
    Sidebar,              // Focus on sidebar (tool list)
    DistributionSelector, // Focus on distribution selector in detail view
}

/// A popup taller than the screen scrolls: the offset, and the most it can be.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DetailScroll {
    pub offset: u16,
    pub max: u16,
}

#[derive(Default)]
pub struct AnalysisModal {
    pub active: bool,
    pub scroll_position: usize,
    pub selected_column: Option<usize>,
    pub describe_column_offset: usize, // For horizontal scrolling in describe table
    pub distribution_column_offset: usize, // For horizontal scrolling in distribution table
    pub correlation_column_offset: usize, // For horizontal scrolling in correlation matrix
    /// The rows every tool reads: one scope, method, size and seed for all of them,
    /// so switching tools compares like with like. Kept across opens; `s` edits it.
    pub sample: crate::sampling::Sample,
    /// The dataset the sample's scope was chosen for. A scope naming a partition or a
    /// file of one dataset means nothing on the next.
    pub sample_dataset: Option<u64>,
    /// The dataset a sample was last run on. Once one has been, a tool picked with
    /// no result runs on it at once; before, the Sample form asks first.
    pub sample_run_for: Option<u64>,
    /// The Sample form, while it is open.
    pub sample_form: Option<crate::sample_modal::SampleForm>,
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
    pub selected_theoretical_distribution: DistributionType, // Selected theoretical distribution for Q-Q plot
    pub distribution_selector_state: TableState,             // For distribution selector list
    pub histogram_scale: HistogramScale,                     // Scale for histogram (linear or log)
    pub data_quality_page: QualityPage,
    pub data_quality_plan: DataQualityPlan,
    pub data_quality_table_state: TableState,
    /// The list a plan field's choices open in, while it is open.
    pub data_quality_picker: Option<PlanPicker>,
    pub data_quality_plan_field: usize,
    pub data_quality_show_access: bool,
    pub data_quality_observation_detail: bool,
    /// Where the finding popup is scrolled to, and how far it can go (set as it draws).
    pub data_quality_detail_scroll: DetailScroll,
    /// The segment a drill-in shows, and the one Segments selects on the way back.
    pub data_quality_segment_index: usize,
    /// Segments listed clearest change first rather than in their own order.
    pub data_quality_segments_by_change: bool,
    /// The clean entry's popup lists every check rather than the most important.
    pub data_quality_checks_expanded: bool,
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

    /// A modal whose shared sample starts at the configured size: `[performance]
    /// analysis_sample_rows`, where 0 means every row.
    pub fn with_sample_rows(rows: usize) -> Self {
        let mut modal = Self::default();
        modal.sample.seed = crate::sample_modal::new_seed();
        if rows == 0 {
            modal.sample.method = crate::sampling::SampleMethod::EveryRow;
        } else {
            modal.sample.rows = rows;
        }
        modal
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
        self.computing = None;
        self.describe_results = None;
        self.distribution_results = None;
        self.correlation_results = None;
        self.data_quality_results = None;
        self.data_quality_page = QualityPage::Plan;
        self.data_quality_plan = DataQualityPlan::default();
        self.data_quality_table_state.select(Some(0));
        self.data_quality_picker = None;
        self.data_quality_plan_field = 0;
        self.data_quality_show_access = false;
        self.data_quality_observation_detail = false;
        self.data_quality_confirm_run = false;
        self.data_quality_plan_before_edit = None;
        self.data_quality_last_plan = None;
        self.data_quality_from_cache = false;
        self.data_quality_metric = QualityMetric::NullRate;
        self.data_quality_column_index = 0;
        self.sample_form = None;
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
        self.computing = None;
        self.describe_results = None;
        self.distribution_results = None;
        self.correlation_results = None;
        self.data_quality_results = None;
        self.data_quality_page = QualityPage::Plan;
        self.data_quality_picker = None;
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

    /// The tool under the sidebar cursor.
    pub fn highlighted_tool(&self) -> Option<AnalysisTool> {
        Some(match self.sidebar_state.selected()? {
            0 => AnalysisTool::Describe,
            1 => AnalysisTool::DistributionAnalysis,
            2 => AnalysisTool::CorrelationMatrix,
            3 => AnalysisTool::DataQuality,
            _ => return None,
        })
    }

    /// Select the tool under the sidebar cursor. Focus stays on the sidebar:
    /// it moves only when the user presses Tab, never as a side effect.
    pub fn select_tool(&mut self) {
        if let Some(idx) = self.sidebar_state.selected() {
            self.selected_tool = Some(match idx {
                0 => AnalysisTool::Describe,
                1 => AnalysisTool::DistributionAnalysis,
                2 => AnalysisTool::CorrelationMatrix,
                3 => AnalysisTool::DataQuality,
                _ => AnalysisTool::Describe,
            });
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
        self.focus = AnalysisFocus::Main;
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

    /// Another sample: a new seed for the shared sample.
    pub fn recalculate(&mut self) {
        self.sample.seed = crate::sample_modal::new_seed();
    }

    pub fn quality_row_count(&self) -> usize {
        let Some(results) = self.data_quality_results.as_ref() else {
            return 0;
        };
        match self.data_quality_page {
            QualityPage::Plan => 6,
            QualityPage::TimeRoles => TemporalRole::ALL.len(),
            QualityPage::Overview => crate::quality_report::build_report(results).findings.len(),
            QualityPage::Columns | QualityPage::Detail => results.columns.len(),
            QualityPage::Segments => results.segments.len(),
            QualityPage::SegmentDetail => {
                crate::data_quality::segment_changes(results, self.data_quality_segment_index).len()
            }
            // The trend table's lines; the width only changes how many bars.
            QualityPage::Trends => {
                crate::data_quality::trend_rows(results, self.data_quality_metric, 1)
                    .0
                    .len()
            }
        }
    }

    /// Whether the highlighted Overview entry is the clean-columns entry, which opens
    /// no rows and instead lists the checks.
    pub fn quality_selected_is_clean(&self) -> bool {
        self.data_quality_page == QualityPage::Overview
            && self.data_quality_results.as_ref().is_some_and(|results| {
                crate::quality_report::build_report(results)
                    .findings
                    .get(self.data_quality_table_state.selected().unwrap_or(0))
                    .is_some_and(|finding| finding.kind.is_none())
            })
    }

    /// Whether `s` opens the Sample form here: on a tool's main view, with nothing
    /// else holding the keys — no run, no popup, no editor, no text field.
    pub fn sample_key_opens_form(&self) -> bool {
        self.active
            && self.view == AnalysisView::Main
            && self.selected_tool.is_some()
            && self.computing.is_none()
            && !self.show_help
            && self.data_quality_picker.is_none()
            && self.data_quality_page != QualityPage::TimeRoles
            && !self.data_quality_confirm_run
            && !self.data_quality_show_access
            && !self.data_quality_observation_detail
            && self.sample_form.is_none()
    }

    /// Whether the Sample form's scope field owns typed characters, so Ctrl-C and `?`
    /// are text there as in any field.
    pub fn sample_scope_typing(&self) -> bool {
        self.active
            && self.sample_form.as_ref().is_some_and(|form| {
                form.field.is_text() && (!form.inline || self.focus == AnalysisFocus::Main)
            })
    }

    pub fn set_quality_page(&mut self, page: QualityPage) {
        self.data_quality_page = page;
        self.data_quality_observation_detail = false;
        self.data_quality_table_state.select(Some(0));
    }

    /// Move between the column lens and its detail without losing which column
    /// the user was looking at.
    pub fn set_quality_column_page(&mut self, page: QualityPage) {
        // Detail moves the same selection with Up/Down, so capture it from there
        // too or navigating inside Detail is lost on the way back.
        if matches!(
            self.data_quality_page,
            QualityPage::Columns | QualityPage::Detail
        ) {
            self.data_quality_column_index = self.data_quality_table_state.selected().unwrap_or(0);
        }
        self.set_quality_page(page);
        self.data_quality_table_state
            .select(Some(self.data_quality_column_index));
    }

    /// Open the highlighted segment's columns, or go back to the list with the
    /// segment still selected.
    pub fn open_segment_detail(&mut self) {
        if let Some(segment) = self.selected_segment() {
            self.data_quality_segment_index = segment;
            self.set_quality_page(QualityPage::SegmentDetail);
        }
    }

    pub fn close_segment_detail(&mut self) {
        self.set_quality_page(QualityPage::Segments);
        let position = self
            .segment_order()
            .iter()
            .position(|segment| *segment == self.data_quality_segment_index);
        self.data_quality_table_state
            .select(Some(position.unwrap_or(0)));
    }

    /// The order Segments lists its rows in.
    pub fn segment_order(&self) -> Vec<usize> {
        self.data_quality_results
            .as_ref()
            .map(|results| {
                crate::data_quality::segment_order(results, self.data_quality_segments_by_change)
            })
            .unwrap_or_default()
    }

    /// The segment under the cursor on Segments, whichever order it is listed in.
    pub fn selected_segment(&self) -> Option<usize> {
        let position = self.data_quality_table_state.selected()?;
        self.segment_order().get(position).copied()
    }

    /// List segments in their own order or clearest change first, keeping the one
    /// under the cursor under it.
    pub fn toggle_segment_order(&mut self) {
        let segment = self.selected_segment();
        self.data_quality_segments_by_change = !self.data_quality_segments_by_change;
        let position = segment
            .and_then(|segment| self.segment_order().iter().position(|s| *s == segment))
            .unwrap_or(0);
        self.data_quality_table_state.select(Some(position));
    }

    /// Move the finding popup by `rows`, within what it last drew.
    pub fn scroll_quality_detail(&mut self, rows: i32) {
        let scroll = &mut self.data_quality_detail_scroll;
        scroll.offset = (scroll.offset as i32 + rows).clamp(0, scroll.max as i32) as u16;
    }

    /// Show a tab, keeping the column in view across Columns, Segments and Trends.
    /// The plan acts on Enter, which only the main pane hears, so it brings the
    /// cursor along.
    pub fn show_quality_tab(&mut self, page: QualityPage) {
        if matches!(
            self.data_quality_page,
            QualityPage::Columns | QualityPage::Detail
        ) {
            self.data_quality_column_index = self.data_quality_table_state.selected().unwrap_or(0);
        }
        self.set_quality_page(page);
        match page {
            QualityPage::Columns => self
                .data_quality_table_state
                .select(Some(self.data_quality_column_index)),
            QualityPage::Plan => self.focus = AnalysisFocus::Main,
            _ => {}
        }
    }

    /// The next or previous tab, stopping at either end.
    pub fn step_quality_tab(&mut self, forward: bool) {
        let tabs = QualityPage::TABS;
        let Some(at) = tabs
            .iter()
            .position(|page| *page == self.data_quality_page.tab())
        else {
            return;
        };
        let next = if forward {
            (at + 1).min(tabs.len() - 1)
        } else {
            at.saturating_sub(1)
        };
        if next != at {
            self.show_quality_tab(tabs[next]);
        }
    }

    pub fn cycle_quality_metric(&mut self) {
        let current = QualityMetric::ALL
            .iter()
            .position(|metric| *metric == self.data_quality_metric)
            .unwrap_or(0);
        self.data_quality_metric = QualityMetric::ALL[(current + 1) % QualityMetric::ALL.len()];
    }

    /// The plan the result on screen was measured with; the working plan until a
    /// run exists. Result pages read this one, so an edit not yet run never
    /// relabels what was measured.
    pub fn quality_result_plan(&self) -> &DataQualityPlan {
        self.data_quality_last_plan
            .as_ref()
            .unwrap_or(&self.data_quality_plan)
    }

    /// The plan has been edited since the result on screen was measured.
    pub fn quality_plan_pending(&self) -> bool {
        self.data_quality_results.is_some()
            && self.data_quality_last_plan.as_ref() != Some(&self.data_quality_plan)
    }

    /// Rows on the Plan page: the latency threshold only once two time roles give
    /// it an interval to measure.
    pub fn quality_plan_rows(&self) -> usize {
        if self.data_quality_plan.temporal_roles.len() >= 2 {
            6
        } else {
            5
        }
    }

    /// The choices a plan field offers, in the words the header uses.
    pub fn plan_choices(&self, field: usize, context: &PlanContext) -> Vec<(String, PlanChoice)> {
        match field {
            1 => {
                let mut grains = vec![QualityGrain::Dataset];
                if context.files {
                    grains.push(QualityGrain::File);
                }
                grains.extend(
                    context
                        .partitions
                        .iter()
                        .cloned()
                        .map(QualityGrain::Partition),
                );
                // Any date column can split the rows by day, week or month; no role
                // has to be invented for it first. Hours only where there are times.
                for (column, has_time) in &context.time_columns {
                    for every in QUALITY_WINDOW_WIDTHS {
                        if every == "1h" && !has_time {
                            continue;
                        }
                        grains.push(QualityGrain::TimeWindows {
                            column: column.clone(),
                            every: every.to_string(),
                        });
                    }
                }
                grains.extend([100_000, 1_000_000].map(QualityGrain::RowChunks));
                if !grains.contains(&self.data_quality_plan.grain) {
                    grains.insert(0, self.data_quality_plan.grain.clone());
                }
                grains
                    .into_iter()
                    .map(|grain| (grain.label(), PlanChoice::Grain(grain)))
                    .collect()
            }
            2 => {
                let read = if self.sample.method == crate::sampling::SampleMethod::EveryRow {
                    QualityCompute::Full
                } else {
                    QualityCompute::Sample
                };
                vec![
                    ("read".to_string(), PlanChoice::Values(read)),
                    (
                        "file metadata only".to_string(),
                        PlanChoice::Values(QualityCompute::Metadata),
                    ),
                ]
            }
            3 => [
                QualityComparison::None,
                QualityComparison::Previous,
                QualityComparison::Baseline,
            ]
            .into_iter()
            .map(|comparison| {
                (
                    comparison.choice_label().to_string(),
                    PlanChoice::Compare(comparison),
                )
            })
            .collect(),
            5 => [None, Some(3_600), Some(86_400), Some(604_800)]
                .into_iter()
                .map(|seconds| {
                    (
                        match seconds {
                            None => "none",
                            Some(3_600) => "1 hour",
                            Some(86_400) => "1 day",
                            _ => "1 week",
                        }
                        .to_string(),
                        PlanChoice::Latency(seconds),
                    )
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Open the focused plan field's choices, the current one selected.
    pub fn open_plan_picker(&mut self, field: usize, context: &PlanContext) {
        let choices = self.plan_choices(field, context);
        if choices.is_empty() {
            return;
        }
        let current = choices
            .iter()
            .position(|(_, choice)| choice.is_current(&self.data_quality_plan))
            .unwrap_or(0);
        let (labels, choices): (Vec<_>, Vec<_>) = choices.into_iter().unzip();
        let mut state = crate::widgets::ui::PickerState::new(labels);
        state.select_original(current);
        self.data_quality_plan_field = field;
        self.data_quality_picker = Some(PlanPicker {
            field,
            choices,
            state,
        });
    }

    /// Take the picker's selection into the plan and close it.
    pub fn choose_plan_picker(&mut self) {
        let Some(picker) = self.data_quality_picker.take() else {
            return;
        };
        let Some(choice) = picker
            .state
            .selected_original()
            .and_then(|index| picker.choices.get(index))
        else {
            return;
        };
        let plan = &mut self.data_quality_plan;
        match choice.clone() {
            PlanChoice::Grain(grain) => {
                if plan.grain != grain {
                    plan.baseline_segment = None;
                }
                plan.grain = grain;
            }
            PlanChoice::Values(compute) => plan.compute = compute,
            PlanChoice::Compare(comparison) => {
                plan.comparison = comparison;
                if comparison != QualityComparison::Baseline {
                    plan.baseline_segment = None;
                }
            }
            PlanChoice::Latency(seconds) => plan.latency_threshold_seconds = seconds,
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
                // The order the selector lists them in.
                let distribution_scores =
                    crate::distribution_fit::listing_order(&dist_analysis.fits);
                let valid_idx = idx.min(distribution_scores.len().saturating_sub(1));
                if let Some(dist_type) = distribution_scores.get(valid_idx) {
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

    /// Grain choices come from the data: files, partition columns, and a day, week
    /// or month of any date column (hours only where there are times), then chunks.
    #[test]
    fn grain_choices_come_from_the_data() {
        let mut modal = AnalysisModal::new();
        let context = PlanContext {
            partitions: vec!["year".to_string()],
            time_columns: vec![("date".to_string(), false), ("stamp".to_string(), true)],
            files: true,
        };
        let labels = modal
            .plan_choices(1, &context)
            .into_iter()
            .map(|(label, _)| label)
            .collect::<Vec<_>>();
        assert_eq!(
            labels,
            [
                "whole dataset",
                "by file",
                "by year",
                "by day of date",
                "by week of date",
                "by month of date",
                "by hour of stamp",
                "by day of stamp",
                "by week of stamp",
                "by month of stamp",
                "in chunks of 100,000 rows",
                "in chunks of 1,000,000 rows",
            ]
        );
        // Choosing opens the list on the current value and sets the one chosen.
        modal.open_plan_picker(1, &context);
        let picker = modal.data_quality_picker.as_mut().unwrap();
        assert_eq!(picker.state.selected_original(), Some(0));
        picker.state.move_down();
        picker.state.move_down();
        picker.state.move_down();
        modal.choose_plan_picker();
        assert!(modal.data_quality_picker.is_none());
        assert_eq!(
            modal.data_quality_plan.grain,
            QualityGrain::TimeWindows {
                column: "date".to_string(),
                every: "1d".to_string()
            }
        );
    }

    /// Values: read or metadata only; which of sample and full scan a read is, the
    /// shared sample says.
    #[test]
    fn a_read_is_the_samples_kind() {
        let mut modal = AnalysisModal::new();
        let context = PlanContext::default();
        modal.data_quality_plan.compute = QualityCompute::Metadata;
        modal.open_plan_picker(2, &context);
        modal
            .data_quality_picker
            .as_mut()
            .unwrap()
            .state
            .select_original(0);
        modal.choose_plan_picker();
        assert_eq!(modal.data_quality_plan.compute, QualityCompute::Sample);
        modal.sample.method = crate::sampling::SampleMethod::EveryRow;
        modal.open_plan_picker(2, &context);
        modal.choose_plan_picker();
        assert_eq!(modal.data_quality_plan.compute, QualityCompute::Full);
        assert_eq!(modal.quality_plan_rows(), 5, "no latency row without roles");
    }
}

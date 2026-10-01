use crate::data_quality::{
    DataQualityPlan, DataQualityResults, IntervalClock, IntervalFact, QUALITY_WINDOW_WIDTHS,
    QualityComparison, QualityCompute, QualityGrain, QualityMetric, QualityPage, TIME_FORMATS,
    TemporalRole, TemporalRoleAssignment, TimeInterpretation, TimeKind,
};
use crate::quality_report::{EvidenceRows, Finding, FindingsView, QualityReport};
use crate::statistics::{AnalysisResults, DistributionType};
use ratatui::widgets::TableState;

/// The rows of Data Quality Setup, top to bottom: the rows read, what the columns
/// mean, and how the study splits and compares them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupRow {
    Sample,
    TextAsTime,
    TimeRoles,
    Intervals,
    Grain,
    Compare,
    Values,
    Latency,
    WindowBy,
}

impl SetupRow {
    pub const ALL: [Self; 9] = [
        Self::Sample,
        Self::TextAsTime,
        Self::TimeRoles,
        Self::Intervals,
        Self::Grain,
        Self::Compare,
        Self::Values,
        Self::Latency,
        Self::WindowBy,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Sample => "Sample",
            Self::TextAsTime => "Text as time",
            Self::TimeRoles => "Time roles",
            Self::Intervals => "Intervals",
            Self::Grain => "Grain",
            Self::Compare => "Compare",
            Self::Values => "Values",
            Self::Latency => "Latency over",
            Self::WindowBy => "Window by",
        }
    }

    /// The row at `index`, the last one past the end.
    pub fn at(index: usize) -> Self {
        Self::ALL[index.min(Self::ALL.len() - 1)]
    }

    pub fn index(self) -> usize {
        Self::ALL.iter().position(|row| *row == self).unwrap_or(0)
    }
}

/// What a Setup row's picker sets.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanChoice {
    Grain(QualityGrain),
    Values(QualityCompute),
    Compare(QualityComparison),
    Latency(Option<i64>),
    /// Which time puts an interval in a window.
    Clock(IntervalClock),
    /// A text column to read as time; choosing it asks for the format next.
    TextColumn(String),
    /// How a text column is read as time, or `None` to read it as text again.
    Format(String, Option<(TimeKind, &'static str)>),
    /// Only the findings that name this column, or all of them.
    FindingColumn(Option<String>),
    /// Only the findings this check made, or all of them.
    FindingCheck(Option<&'static str>),
}

impl PlanChoice {
    fn is_current(&self, plan: &DataQualityPlan) -> bool {
        match self {
            Self::Grain(grain) => &plan.grain == grain,
            Self::Values(QualityCompute::Metadata) => plan.compute == QualityCompute::Metadata,
            Self::Values(_) => plan.compute != QualityCompute::Metadata,
            Self::Compare(comparison) => &plan.comparison == comparison,
            Self::Latency(seconds) => &plan.latency_threshold_seconds == seconds,
            Self::Clock(clock) => plan.interval_clock == *clock,
            Self::TextColumn(column) => plan.time_format(column).is_some(),
            Self::Format(column, format) => {
                plan.time_format(column)
                    .map(|current| (current.kind, current.format.as_str()))
                    == format.map(|(kind, format)| (kind, format))
            }
            // The findings list is not the plan: its picker selects its own current.
            Self::FindingColumn(_) | Self::FindingCheck(_) => false,
        }
    }
}

/// A Setup row's choices, or the findings list's narrowing, open as a list.
#[derive(Debug, Clone)]
pub struct PlanPicker {
    /// The Setup row the choices set; `None` for the findings list's.
    pub row: Option<SetupRow>,
    pub title: String,
    pub choices: Vec<PlanChoice>,
    pub state: crate::widgets::ui::PickerState,
}

/// What the data offers Setup's choices: partition columns, the columns a time
/// window can split by (and whether they hold times of day), whether there are
/// files to split by, and the text columns that could be read as time, each with
/// a few of its values from the rows on screen.
#[derive(Debug, Clone, Default)]
pub struct PlanContext {
    pub partitions: Vec<String>,
    pub time_columns: Vec<(String, bool)>,
    pub files: bool,
    pub text_columns: Vec<(String, Vec<String>)>,
}

/// Rows a finding names that the run did not keep, waiting for Enter to read them.
/// Nothing reads until then, and Esc drops it.
#[derive(Debug, Clone)]
pub struct EvidenceRead {
    pub rows: EvidenceRows,
    /// The table's label once the rows are shown.
    pub label: String,
    /// Draw this sample again from its seed, rather than read the scope.
    pub sample: Option<crate::sampling::Sample>,
    /// The scope the rows are read from.
    pub scope: crate::data_quality::QualityScope,
    /// What the read is, as the dialog says it: label, value.
    pub summary: Vec<(&'static str, String)>,
}

/// A latency threshold as Setup offers it.
pub fn threshold_label(seconds: Option<i64>) -> &'static str {
    match seconds {
        None => "none",
        Some(3_600) => "1 hour",
        Some(86_400) => "1 day",
        Some(604_800) => "1 week",
        Some(_) => "custom",
    }
}

/// Read `column` as time through `format`, or as text again with `None`. A time
/// window on a column that is text again has no clock, so the grain goes back to
/// the whole dataset rather than failing the run.
pub fn set_time_format(plan: &mut DataQualityPlan, column: &str, format: Option<(TimeKind, &str)>) {
    plan.time_formats
        .retain(|interpretation| interpretation.column != column);
    match format {
        Some((kind, format)) => plan.time_formats.push(TimeInterpretation {
            column: column.to_string(),
            kind,
            format: format.to_string(),
        }),
        None => {
            if matches!(&plan.grain, QualityGrain::TimeWindows { column: on, .. } if on == column) {
                plan.grain = QualityGrain::Dataset;
                plan.baseline_segment = None;
            }
        }
    }
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
    /// When the run began, for the elapsed time on screen. A run has no total to
    /// count against, so time is the progress there is to show.
    pub started: std::time::Instant,
    /// Whether the stage now running reads the source or works on rows already
    /// read. `None` until a Data Quality run names its first stage.
    pub reads_source: Option<bool>,
    /// Whether a cancel stops the stage now running partway. `None` until a Data
    /// Quality run names its first stage.
    pub interruptible: Option<bool>,
    /// The sampler's count of rows seen, where the read can count them.
    pub read: Option<crate::sampling::ReadWatch>,
    /// What the run starts from, when it reuses something: said before it starts.
    pub reuse: Option<String>,
}

impl AnalysisProgress {
    pub fn new(phase: &str) -> Self {
        Self {
            phase: phase.to_string(),
            started: std::time::Instant::now(),
            reads_source: None,
            interruptible: None,
            read: None,
            reuse: None,
        }
    }

    /// The stage now running reads the source in one collect nothing can stop: a
    /// cancel waits for its end.
    pub fn read_runs_out(&self) -> bool {
        self.reads_source == Some(true) && self.interruptible == Some(false)
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

/// A result table wider than its pane scrolls by statistic: the first one shown,
/// and the furthest that first can go, which is where the last statistic comes
/// into view. The table sets `max` as it draws, so the keys stop where it does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColumnScroll {
    pub offset: usize,
    pub max: usize,
}

impl ColumnScroll {
    pub fn left(&mut self) {
        self.offset = self.offset.min(self.max).saturating_sub(1);
    }

    pub fn right(&mut self) {
        if self.offset < self.max {
            self.offset += 1;
        }
    }
}

#[derive(Default)]
pub struct AnalysisModal {
    pub active: bool,
    pub scroll_position: usize,
    pub selected_column: Option<usize>,
    pub describe_columns: ColumnScroll,
    pub distribution_columns: ColumnScroll,
    /// Kept on the selected cell by the matrix as it draws.
    pub correlation_columns: ColumnScroll,
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
    /// The plan: the one the last Run committed, and while Setup is open, the draft
    /// being staged for the next.
    pub data_quality_plan: DataQualityPlan,
    /// The plan as it stood when Setup opened: Esc puts it back. `None` while Setup
    /// is closed.
    pub data_quality_setup_before: Option<DataQualityPlan>,
    /// The report page Setup was opened from, and goes back to.
    pub data_quality_setup_return: QualityPage,
    /// Why Enter did not run, said on Setup's own line until the next edit.
    pub data_quality_setup_note: Option<String>,
    pub data_quality_table_state: TableState,
    /// The list a Setup row's choices open in, while it is open.
    pub data_quality_picker: Option<PlanPicker>,
    /// The Setup row under the cursor, or the role in the time roles editor.
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
    /// The interval a detail shows, and the one Intervals selects on the way back.
    pub data_quality_interval_index: usize,
    /// How Overview narrows and orders its findings; the report is not measured again.
    pub data_quality_findings: FindingsView,
    /// A read for a finding's rows, shown with what it reads until Enter or Esc.
    pub data_quality_evidence_read: Option<EvidenceRead>,
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
        self.describe_columns = ColumnScroll::default();
        self.distribution_columns = ColumnScroll::default();
        self.correlation_columns = ColumnScroll::default();
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
        self.data_quality_page = QualityPage::Setup;
        self.data_quality_plan = DataQualityPlan::default();
        self.data_quality_setup_before = None;
        self.data_quality_setup_return = QualityPage::Overview;
        self.data_quality_setup_note = None;
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
        self.data_quality_interval_index = 0;
        self.sample_form = None;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.scroll_position = 0;
        self.selected_column = None;
        self.describe_columns = ColumnScroll::default();
        self.distribution_columns = ColumnScroll::default();
        self.correlation_columns = ColumnScroll::default();
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
        self.data_quality_page = QualityPage::Setup;
        self.data_quality_setup_before = None;
        self.data_quality_setup_note = None;
        self.data_quality_picker = None;
        self.data_quality_show_access = false;
        self.data_quality_observation_detail = false;
        self.data_quality_confirm_run = false;
        self.data_quality_plan_before_edit = None;
        self.data_quality_last_plan = None;
        self.data_quality_from_cache = false;
        self.data_quality_metric = QualityMetric::NullRate;
        self.data_quality_column_index = 0;
        self.data_quality_interval_index = 0;
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

    /// Tab on the main view: the tool list and the result trade focus. The detail
    /// views have one focusable thing, so Tab is not offered there; nor is it
    /// before a tool is chosen, when the pane beside the list is empty.
    pub fn switch_focus(&mut self) {
        self.focus = match self.focus {
            AnalysisFocus::Sidebar if self.selected_tool.is_some() => AnalysisFocus::Main,
            _ => AnalysisFocus::Sidebar,
        };
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
        if let Some(columns) = self.column_scroll_mut() {
            columns.left();
        }
    }

    pub fn scroll_right(&mut self) {
        if let Some(columns) = self.column_scroll_mut() {
            columns.right();
        }
    }

    /// The selected tool's statistic scroll, for the tools that scroll by statistic.
    pub fn column_scroll(&self) -> Option<&ColumnScroll> {
        match self.selected_tool {
            Some(AnalysisTool::Describe) => Some(&self.describe_columns),
            Some(AnalysisTool::DistributionAnalysis) => Some(&self.distribution_columns),
            _ => None,
        }
    }

    fn column_scroll_mut(&mut self) -> Option<&mut ColumnScroll> {
        match self.selected_tool {
            Some(AnalysisTool::Describe) => Some(&mut self.describe_columns),
            Some(AnalysisTool::DistributionAnalysis) => Some(&mut self.distribution_columns),
            _ => None,
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
            QualityPage::Setup => SetupRow::ALL.len(),
            QualityPage::TimeRoles => TemporalRole::ALL.len(),
            QualityPage::IntervalPairs => self.data_quality_plan.candidate_pairs().len(),
            QualityPage::Intervals => results.temporal.len(),
            QualityPage::IntervalDetail => self.interval_facts().len(),
            QualityPage::Overview => self
                .data_quality_findings
                .shown(&crate::quality_report::build_report(results))
                .len(),
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
            && self
                .selected_finding()
                .is_some_and(|(_, finding)| finding.kind.is_none())
    }

    /// The report on screen and the finding under the cursor, as Overview lists
    /// them: narrowed and ordered.
    pub fn selected_finding(&self) -> Option<(QualityReport, Finding)> {
        let results = self.data_quality_results.as_ref()?;
        let report = crate::quality_report::build_report(results);
        let finding = self
            .data_quality_findings
            .selected(&report, self.data_quality_table_state.selected()?)?
            .clone();
        Some((report, finding))
    }

    /// Narrow the findings to a column (`by_column`) or a check, from a list of
    /// those the report has, the current one selected.
    pub fn open_findings_picker(&mut self, by_column: bool) {
        let Some(results) = self.data_quality_results.as_ref() else {
            return;
        };
        let report = crate::quality_report::build_report(results);
        let view = &self.data_quality_findings;
        let findings = |count: usize| match count {
            0 => "none".to_string(),
            1 => "1 finding".to_string(),
            count => format!("{} findings", crate::numfmt::group_chrome(count)),
        };
        let (title, choices, current) = if by_column {
            let columns = crate::quality_report::column_choices(&report, results);
            let width = columns
                .iter()
                .map(|(name, _)| crate::glyphs::display_width(name))
                .max()
                .unwrap_or(0);
            let current = view
                .column
                .as_ref()
                .and_then(|column| columns.iter().position(|(name, _)| name == column))
                .map_or(0, |position| position + 1);
            let mut choices = vec![("All columns".to_string(), PlanChoice::FindingColumn(None))];
            choices.extend(columns.into_iter().map(|(name, count)| {
                let pad = width.saturating_sub(crate::glyphs::display_width(&name));
                (
                    format!("{name}{}  {}", " ".repeat(pad), findings(count)),
                    PlanChoice::FindingColumn(Some(name)),
                )
            }));
            ("Findings by Column", choices, current)
        } else {
            let checks = crate::quality_report::check_choices(&report);
            let width = checks
                .iter()
                .map(|(name, _)| crate::glyphs::display_width(name))
                .max()
                .unwrap_or(0);
            let current = view
                .check
                .and_then(|check| checks.iter().position(|(name, _)| *name == check))
                .map_or(0, |position| position + 1);
            let mut choices = vec![("All types".to_string(), PlanChoice::FindingCheck(None))];
            choices.extend(checks.into_iter().map(|(name, count)| {
                let pad = width.saturating_sub(crate::glyphs::display_width(name));
                (
                    format!("{name}{}  {}", " ".repeat(pad), findings(count)),
                    PlanChoice::FindingCheck(Some(name)),
                )
            }));
            ("Findings by Type", choices, current)
        };
        let (labels, choices): (Vec<_>, Vec<_>) = choices.into_iter().unzip();
        let mut state = crate::widgets::ui::PickerState::new(labels);
        state.select_original(current);
        self.data_quality_picker = Some(PlanPicker {
            row: None,
            title: title.to_string(),
            choices,
            state,
        });
    }

    /// The next order for the findings, keeping the finding under the cursor under it.
    pub fn cycle_findings_order(&mut self) {
        let selected = self.selected_finding();
        self.data_quality_findings.order = self.data_quality_findings.order.next();
        self.reselect_finding(selected.map(|(_, finding)| finding));
    }

    /// Show every finding again, in the order chosen.
    pub fn clear_findings_narrowing(&mut self) {
        let selected = self.selected_finding();
        self.data_quality_findings.column = None;
        self.data_quality_findings.check = None;
        self.reselect_finding(selected.map(|(_, finding)| finding));
    }

    /// Put the cursor on `finding` where the list now shows it, or on the first.
    fn reselect_finding(&mut self, finding: Option<Finding>) {
        let position = self.data_quality_results.as_ref().and_then(|results| {
            let report = crate::quality_report::build_report(results);
            let finding = finding?;
            let shown = self.data_quality_findings.shown(&report);
            shown.iter().position(|index| {
                let listed = &report.findings[*index];
                listed.title == finding.title && listed.columns == finding.columns
            })
        });
        self.data_quality_table_state
            .select(Some(position.unwrap_or(0)));
        *self.data_quality_table_state.offset_mut() = 0;
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
            && !matches!(
                self.data_quality_page,
                QualityPage::TimeRoles | QualityPage::IntervalPairs
            )
            && !self.data_quality_confirm_run
            && !self.data_quality_show_access
            && !self.data_quality_observation_detail
            && self.data_quality_evidence_read.is_none()
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
        self.data_quality_evidence_read = None;
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

    /// Open the highlighted interval's detail, its first count under the cursor.
    pub fn open_interval_detail(&mut self) {
        let Some(index) = self.data_quality_table_state.selected() else {
            return;
        };
        if self
            .data_quality_results
            .as_ref()
            .is_some_and(|results| index < results.temporal.len())
        {
            self.data_quality_interval_index = index;
            self.set_quality_page(QualityPage::IntervalDetail);
        }
    }

    /// Back to the list, the interval still selected.
    pub fn close_interval_detail(&mut self) {
        self.set_quality_page(QualityPage::Intervals);
        self.data_quality_table_state
            .select(Some(self.data_quality_interval_index));
    }

    /// The counts an interval's detail lists, each with rows it can open: those
    /// the interval measured, in the order the detail shows them.
    pub fn interval_facts(&self) -> Vec<IntervalFact> {
        let plan = self.quality_result_plan();
        self.data_quality_results
            .as_ref()
            .and_then(|results| results.temporal.get(self.data_quality_interval_index))
            .map(|profile| {
                IntervalFact::ALL
                    .into_iter()
                    .filter(|fact| profile.count(*fact, plan).is_some())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The count under the cursor in an interval's detail.
    pub fn selected_interval_fact(&self) -> Option<IntervalFact> {
        let facts = self.interval_facts();
        facts
            .get(self.data_quality_table_state.selected().unwrap_or(0))
            .copied()
    }

    /// The rows behind the count under the cursor in an interval's detail, when it
    /// has any and they can be told by their values: a predicate over the rows the
    /// run read, with what to call them.
    pub fn interval_evidence(&self) -> Option<(polars::prelude::Expr, String, usize)> {
        let results = self.data_quality_results.as_ref()?;
        if !matches!(
            results.precision,
            crate::data_quality::QualityPrecision::Exact
                | crate::data_quality::QualityPrecision::Sampled
        ) {
            return None;
        }
        let plan = self.quality_result_plan();
        let profile = results.temporal.get(self.data_quality_interval_index)?;
        let fact = self.selected_interval_fact()?;
        let (count, _) = profile.count(fact, plan)?;
        if count == 0 {
            return None;
        }
        let predicate = profile.evidence_predicate(fact, plan)?;
        Some((
            predicate,
            format!(
                "Data Quality / {} / {} / {}",
                profile.label(),
                profile.segment,
                fact.short()
            ),
            count,
        ))
    }

    /// Move the finding popup by `rows`, within what it last drew.
    pub fn scroll_quality_detail(&mut self, rows: i32) {
        let scroll = &mut self.data_quality_detail_scroll;
        scroll.offset = (scroll.offset as i32 + rows).clamp(0, scroll.max as i32) as u16;
    }

    /// Show a tab, keeping the column in view across Columns, Segments and Trends.
    pub fn show_quality_tab(&mut self, page: QualityPage) {
        if matches!(
            self.data_quality_page,
            QualityPage::Columns | QualityPage::Detail
        ) {
            self.data_quality_column_index = self.data_quality_table_state.selected().unwrap_or(0);
        }
        self.set_quality_page(page);
        if page == QualityPage::Columns {
            self.data_quality_table_state
                .select(Some(self.data_quality_column_index));
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

    /// The plan the result on screen was measured with; the plan until a run
    /// exists. Result pages read this one, so a draft never relabels what was
    /// measured.
    pub fn quality_result_plan(&self) -> &DataQualityPlan {
        self.data_quality_last_plan
            .as_ref()
            .unwrap_or(&self.data_quality_plan)
    }

    /// The plan differs from the one the result on screen was measured with.
    pub fn quality_plan_pending(&self) -> bool {
        self.data_quality_results.is_some()
            && self.data_quality_last_plan.as_ref() != Some(&self.data_quality_plan)
    }

    /// The Setup row under the cursor.
    pub fn setup_row(&self) -> SetupRow {
        SetupRow::at(self.data_quality_plan_field)
    }

    /// Whether Setup holds staged changes Esc would discard.
    pub fn setup_edited(&self) -> bool {
        self.data_quality_setup_before
            .as_ref()
            .is_some_and(|before| *before != self.data_quality_plan)
    }

    /// The choices a Setup row offers, in the words the header uses.
    pub fn plan_choices(&self, row: SetupRow, context: &PlanContext) -> Vec<(String, PlanChoice)> {
        match row {
            SetupRow::Grain => {
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
            SetupRow::Values => {
                // The draft's sample says which kind of read it is.
                let read =
                    if self.data_quality_plan.method == crate::sampling::SampleMethod::EveryRow {
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
            SetupRow::Compare => [
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
            SetupRow::WindowBy if !self.data_quality_plan.windows_intervals() => Vec::new(),
            SetupRow::WindowBy => IntervalClock::ALL
                .into_iter()
                .map(|clock| (clock.label().to_string(), PlanChoice::Clock(clock)))
                .collect(),
            SetupRow::Latency if self.data_quality_plan.interval_pairs().is_empty() => Vec::new(),
            SetupRow::Latency => [None, Some(3_600), Some(86_400), Some(604_800)]
                .into_iter()
                .map(|seconds| {
                    (
                        threshold_label(seconds).to_string(),
                        PlanChoice::Latency(seconds),
                    )
                })
                .collect(),
            // Each text column with the first value on screen, so choosing which one
            // holds times is choosing among things seen.
            SetupRow::TextAsTime => context
                .text_columns
                .iter()
                .map(|(column, examples)| {
                    let label = match (self.data_quality_plan.time_format(column), examples.first())
                    {
                        (Some(format), _) => format!("{column}  as {}", format.label()),
                        (None, Some(example)) => format!("{column}  {example}"),
                        (None, None) => column.clone(),
                    };
                    (label, PlanChoice::TextColumn(column.clone()))
                })
                .collect(),
            SetupRow::Sample | SetupRow::TimeRoles | SetupRow::Intervals => Vec::new(),
        }
    }

    /// Open the focused Setup row's choices, the current one selected.
    pub fn open_plan_picker(&mut self, row: SetupRow, context: &PlanContext) {
        let choices = self.plan_choices(row, context);
        self.data_quality_plan_field = row.index();
        self.show_picker(row, row.label().to_string(), choices);
    }

    fn show_picker(&mut self, row: SetupRow, title: String, choices: Vec<(String, PlanChoice)>) {
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
        self.data_quality_picker = Some(PlanPicker {
            row: Some(row),
            title,
            choices,
            state,
        });
    }

    /// How `column` can be read as time: each format with how many of `examples`,
    /// values on screen, it reads, the formats that read the most first; and, when
    /// it has a format, reading it as text again.
    pub fn open_format_picker(&mut self, column: &str, examples: &[String]) {
        let mut formats = TIME_FORMATS
            .iter()
            .enumerate()
            .map(|(order, (kind, format))| {
                let interpretation = TimeInterpretation {
                    column: column.to_string(),
                    kind: *kind,
                    format: format.to_string(),
                };
                let read = examples
                    .iter()
                    .filter(|value| interpretation.reads(value))
                    .count();
                (read, order, *kind, *format)
            })
            .collect::<Vec<_>>();
        // Most read first; the offered order among equals, so the list is stable.
        formats.sort_by_key(|(read, order, _, _)| (std::cmp::Reverse(*read), *order));
        let mut choices = formats
            .into_iter()
            .map(|(read, _, kind, format)| {
                // The count first, so a narrow list clips the format, not the
                // evidence for choosing it.
                let label = if examples.is_empty() {
                    format!("{} {format}", kind.label())
                } else {
                    format!(
                        "reads {read} of {}  {} {format}",
                        examples.len(),
                        kind.label()
                    )
                };
                (
                    label,
                    PlanChoice::Format(column.to_string(), Some((kind, format))),
                )
            })
            .collect::<Vec<_>>();
        if self.data_quality_plan.time_format(column).is_some() {
            choices.push((
                "text, not a time".to_string(),
                PlanChoice::Format(column.to_string(), None),
            ));
        }
        self.show_picker(SetupRow::TextAsTime, format!("Read {column} As"), choices);
    }

    /// Take the picker's selection into the plan and close it. A text column chosen
    /// to read as time comes back: its format is the next choice.
    pub fn choose_plan_picker(&mut self) -> Option<String> {
        let picker = self.data_quality_picker.take()?;
        let choice = picker
            .state
            .selected_original()
            .and_then(|index| picker.choices.get(index))?;
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
            PlanChoice::Clock(clock) => plan.interval_clock = clock,
            PlanChoice::TextColumn(column) => return Some(column),
            PlanChoice::Format(column, format) => set_time_format(plan, &column, format),
            PlanChoice::FindingColumn(column) => {
                let selected = self.selected_finding();
                self.data_quality_findings.column = column;
                self.reselect_finding(selected.map(|(_, finding)| finding));
            }
            PlanChoice::FindingCheck(check) => {
                let selected = self.selected_finding();
                self.data_quality_findings.check = check;
                self.reselect_finding(selected.map(|(_, finding)| finding));
            }
        }
        None
    }

    /// The next or previous choice of a Setup row whose choices are a short list,
    /// in place: ←→ on Grain, Compare, Values, Latency and Window by.
    pub fn cycle_setup_choice(&mut self, row: SetupRow, context: &PlanContext, forward: bool) {
        let choices = self.plan_choices(row, context);
        if choices.is_empty() || matches!(row, SetupRow::TextAsTime) {
            return;
        }
        let current = choices
            .iter()
            .position(|(_, choice)| choice.is_current(&self.data_quality_plan))
            .unwrap_or(0);
        let next = if forward {
            (current + 1).min(choices.len() - 1)
        } else {
            current.saturating_sub(1)
        };
        let (labels, choices): (Vec<_>, Vec<_>) = choices.into_iter().unzip();
        let mut state = crate::widgets::ui::PickerState::new(labels);
        state.select_original(next);
        self.data_quality_picker = Some(PlanPicker {
            row: Some(row),
            title: row.label().to_string(),
            choices,
            state,
        });
        self.choose_plan_picker();
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

    /// Move the correlation cursor by `(rows, columns)`, stopping at the edges. The
    /// matrix scrolls to keep the cell in view as it draws, from the width it has.
    pub fn move_correlation_cell(&mut self, (rows, cols): (isize, isize)) {
        let n = self
            .correlation_results
            .as_ref()
            .and_then(|results| results.correlation_matrix.as_ref())
            .map_or(0, |matrix| matrix.columns.len());
        if n == 0 {
            return;
        }
        if let Some((row, col)) = self.selected_correlation {
            let row = row.saturating_add_signed(rows).min(n - 1);
            let col = col.saturating_add_signed(cols).min(n - 1);
            self.selected_correlation = Some((row, col));
            self.correlation_table_state.select(Some(row));
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
            text_columns: Vec::new(),
        };
        let labels = modal
            .plan_choices(SetupRow::Grain, &context)
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
        modal.open_plan_picker(SetupRow::Grain, &context);
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
        modal.open_plan_picker(SetupRow::Values, &context);
        modal
            .data_quality_picker
            .as_mut()
            .unwrap()
            .state
            .select_original(0);
        modal.choose_plan_picker();
        assert_eq!(modal.data_quality_plan.compute, QualityCompute::Sample);
        modal.data_quality_plan.method = crate::sampling::SampleMethod::EveryRow;
        modal.open_plan_picker(SetupRow::Values, &context);
        modal.choose_plan_picker();
        assert_eq!(modal.data_quality_plan.compute, QualityCompute::Full);
        modal.open_plan_picker(SetupRow::Latency, &context);
        assert!(
            modal.data_quality_picker.is_none(),
            "no threshold without an interval to measure"
        );
    }

    /// A text column is read as time in two choices, the column and then its
    /// format, the formats that read the values on screen first; reading it as text
    /// again takes back a time window that needed it.
    #[test]
    fn text_is_read_as_time_through_a_chosen_format() {
        let mut modal = AnalysisModal::new();
        let context = PlanContext {
            text_columns: vec![(
                "created".to_string(),
                vec!["2024-01-31 08:15:00".to_string()],
            )],
            ..PlanContext::default()
        };
        modal.open_plan_picker(SetupRow::TextAsTime, &context);
        assert_eq!(modal.choose_plan_picker(), Some("created".to_string()));
        modal.open_format_picker("created", &["2024-01-31 08:15:00".to_string()]);
        let picker = modal.data_quality_picker.as_ref().unwrap();
        assert_eq!(picker.title, "Read created As");
        let first = picker.state.filtered()[0].1.to_string();
        assert_eq!(first, "reads 1 of 1  datetime %Y-%m-%d %H:%M:%S");
        assert_eq!(modal.choose_plan_picker(), None);
        let format = modal.data_quality_plan.time_format("created").unwrap();
        assert_eq!(format.kind, TimeKind::Datetime);

        // Now a time window can split by it; as text again, the window goes.
        let windows = PlanContext {
            time_columns: vec![("created".to_string(), true)],
            ..context.clone()
        };
        modal.open_plan_picker(SetupRow::Grain, &windows);
        let picker = modal.data_quality_picker.as_mut().unwrap();
        picker.state.move_down();
        picker.state.move_down();
        modal.choose_plan_picker();
        assert!(matches!(
            modal.data_quality_plan.grain,
            QualityGrain::TimeWindows { .. }
        ));
        modal.open_format_picker("created", &[]);
        let picker = modal.data_quality_picker.as_mut().unwrap();
        let text = picker
            .state
            .filtered()
            .iter()
            .position(|(_, label)| *label == "text, not a time")
            .unwrap();
        for _ in 0..text {
            picker.state.move_down();
        }
        modal.choose_plan_picker();
        assert!(modal.data_quality_plan.time_formats.is_empty());
        assert_eq!(modal.data_quality_plan.grain, QualityGrain::Dataset);
    }
}

use crate::statistics::{AnalysisResults, DistributionType};
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
    pub column_offset: usize, // For horizontal scrolling in describe table
    pub random_seed: u64,
    pub table_state: TableState,              // For describe table
    pub distribution_table_state: TableState, // For distribution table
    pub correlation_table_state: TableState,  // For correlation matrix
    pub sidebar_state: TableState,            // For sidebar tool list
    pub analysis_results: Option<AnalysisResults>,
    pub show_help: bool,
    pub view: AnalysisView,
    pub focus: AnalysisFocus,
    pub selected_tool: AnalysisTool,
    pub selected_distribution: Option<usize>, // Selected row in distribution table
    pub selected_correlation: Option<(usize, usize)>, // Selected cell in correlation matrix (row, col)
    pub detail_section: usize, // Current section in detail view (0=Characteristics, 1=Outliers, 2=Percentiles)
    pub selected_theoretical_distribution: DistributionType, // Selected theoretical distribution for Q-Q plot
    pub distribution_selector_state: TableState,             // For distribution selector list
}

impl AnalysisModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self) {
        self.active = true;
        self.scroll_position = 0;
        self.selected_column = None;
        self.column_offset = 0;
        self.table_state.select(Some(0));
        self.distribution_table_state.select(Some(0));
        self.correlation_table_state.select(Some(0));
        self.sidebar_state.select(Some(0)); // Select Describe tool
        self.view = AnalysisView::Main;
        self.focus = AnalysisFocus::Main;
        self.selected_tool = AnalysisTool::Describe;
        self.selected_distribution = Some(0);
        self.selected_correlation = Some((0, 0));
        self.detail_section = 0;
        // Generate initial random seed
        self.random_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.scroll_position = 0;
        self.selected_column = None;
        self.column_offset = 0;
        self.view = AnalysisView::Main;
        self.focus = AnalysisFocus::Main;
        self.selected_tool = AnalysisTool::Describe;
        self.selected_distribution = None;
        self.selected_correlation = None;
        self.detail_section = 0;
    }

    pub fn switch_focus(&mut self) {
        if self.view == AnalysisView::DistributionDetail {
            // In distribution detail view, toggle between main area and distribution selector
            self.focus = match self.focus {
                AnalysisFocus::Main => AnalysisFocus::DistributionSelector,
                AnalysisFocus::DistributionSelector => AnalysisFocus::Main,
                _ => AnalysisFocus::DistributionSelector, // Default to selector if somehow in wrong state
            };
        } else {
            // In main view, toggle between main area and sidebar
            self.focus = match self.focus {
                AnalysisFocus::Main => AnalysisFocus::Sidebar,
                AnalysisFocus::Sidebar => AnalysisFocus::Main,
                _ => AnalysisFocus::Main, // Default to main if somehow in wrong state
            };
        }
    }

    pub fn select_tool(&mut self) {
        if let Some(idx) = self.sidebar_state.selected() {
            self.selected_tool = match idx {
                0 => AnalysisTool::Describe,
                1 => AnalysisTool::DistributionAnalysis,
                2 => AnalysisTool::CorrelationMatrix,
                _ => AnalysisTool::Describe,
            };
            self.focus = AnalysisFocus::Main; // Switch focus back to main area
        }
    }

    pub fn next_tool(&mut self) {
        if let Some(current) = self.sidebar_state.selected() {
            let next = (current + 1).min(2); // 3 tools: 0, 1, 2
            self.sidebar_state.select(Some(next));
        }
    }

    pub fn previous_tool(&mut self) {
        if let Some(current) = self.sidebar_state.selected() {
            if current > 0 {
                self.sidebar_state.select(Some(current - 1));
            }
        }
    }

    pub fn open_distribution_detail(&mut self) {
        if self.focus == AnalysisFocus::Main
            && self.selected_tool == AnalysisTool::DistributionAnalysis
        {
            if let Some(idx) = self.distribution_table_state.selected() {
                // Initialize selected theoretical distribution to detected type
                if let Some(results) = &self.analysis_results {
                    if let Some(dist_analysis) = results.distribution_analyses.get(idx) {
                        self.selected_theoretical_distribution = dist_analysis.distribution_type;
                    }
                }
                self.view = AnalysisView::DistributionDetail;
                self.detail_section = 0;
                self.focus = AnalysisFocus::DistributionSelector; // Start with distribution selector focused
                                                                  // Set selector state to match selected distribution
                                                                  // If detected type is Unknown, default to Normal (index 0) since Unknown is not in selector
                let dist_idx = match self.selected_theoretical_distribution {
                    DistributionType::Normal => 0,
                    DistributionType::LogNormal => 1,
                    DistributionType::Uniform => 2,
                    DistributionType::PowerLaw => 3,
                    DistributionType::Exponential => 4,
                    DistributionType::Unknown => 0, // Default to Normal if Unknown
                };
                // If it was Unknown, also update selected_theoretical_distribution to Normal
                if self.selected_theoretical_distribution == DistributionType::Unknown {
                    self.selected_theoretical_distribution = DistributionType::Normal;
                }
                self.distribution_selector_state.select(Some(dist_idx));
            }
        }
    }

    pub fn open_correlation_detail(&mut self) {
        if self.focus == AnalysisFocus::Main
            && self.selected_tool == AnalysisTool::CorrelationMatrix
        {
            if let Some((row, col)) = self.selected_correlation {
                if row != col {
                    // Not diagonal
                    self.view = AnalysisView::CorrelationDetail;
                }
            }
        }
    }

    pub fn close_detail(&mut self) {
        self.view = AnalysisView::Main;
        self.detail_section = 0;
        self.focus = AnalysisFocus::Main; // Reset focus when returning to main view
    }

    pub fn next_detail_section(&mut self) {
        self.detail_section = (self.detail_section + 1) % 3; // 0, 1, 2
    }

    pub fn previous_detail_section(&mut self) {
        self.detail_section = if self.detail_section == 0 {
            2
        } else {
            self.detail_section - 1
        };
    }

    pub fn scroll_left(&mut self) {
        if self.column_offset > 0 {
            self.column_offset -= 1;
        }
    }

    pub fn scroll_right(&mut self, max_columns: usize, visible_columns: usize) {
        if self.column_offset + visible_columns < max_columns {
            self.column_offset += 1;
        }
    }

    pub fn recalculate(&mut self) {
        // Generate new random seed for recalculation
        self.random_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
    }

    pub fn next_row(&mut self, max_rows: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            // Navigate sidebar tool list
            self.next_tool();
            return;
        }

        // Navigate within main area based on selected tool
        match self.selected_tool {
            AnalysisTool::Describe => {
                if let Some(current) = self.table_state.selected() {
                    let next = (current + 1).min(max_rows.saturating_sub(1));
                    self.table_state.select(Some(next));
                } else {
                    self.table_state.select(Some(0));
                }
            }
            AnalysisTool::DistributionAnalysis => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = (current + 1).min(max_rows.saturating_sub(1));
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                } else {
                    self.distribution_table_state.select(Some(0));
                    self.selected_distribution = Some(0);
                }
            }
            AnalysisTool::CorrelationMatrix => {
                if let Some((row, col)) = self.selected_correlation {
                    let next_row = (row + 1).min(max_rows.saturating_sub(1));
                    self.selected_correlation = Some((next_row, col));
                    self.correlation_table_state.select(Some(next_row));
                }
            }
        }
    }

    pub fn previous_row(&mut self) {
        if self.focus == AnalysisFocus::Sidebar {
            // Navigate sidebar tool list
            self.previous_tool();
            return;
        }

        // Navigate within main area based on selected tool
        match self.selected_tool {
            AnalysisTool::Describe => {
                if let Some(current) = self.table_state.selected() {
                    if current > 0 {
                        self.table_state.select(Some(current - 1));
                    }
                }
            }
            AnalysisTool::DistributionAnalysis => {
                if let Some(current) = self.distribution_table_state.selected() {
                    if current > 0 {
                        let prev = current - 1;
                        self.distribution_table_state.select(Some(prev));
                        self.selected_distribution = Some(prev);
                    }
                }
            }
            AnalysisTool::CorrelationMatrix => {
                if let Some((row, col)) = self.selected_correlation {
                    if row > 0 {
                        let prev_row = row - 1;
                        self.selected_correlation = Some((prev_row, col));
                        self.correlation_table_state.select(Some(prev_row));
                    }
                }
            }
        }
    }

    pub fn page_down(&mut self, max_rows: usize, page_size: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            return; // Sidebar doesn't use page navigation
        }

        match self.selected_tool {
            AnalysisTool::Describe => {
                if let Some(current) = self.table_state.selected() {
                    let next = (current + page_size).min(max_rows.saturating_sub(1));
                    self.table_state.select(Some(next));
                }
            }
            AnalysisTool::DistributionAnalysis => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = (current + page_size).min(max_rows.saturating_sub(1));
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                }
            }
            AnalysisTool::CorrelationMatrix => {
                if let Some((row, col)) = self.selected_correlation {
                    let next_row = (row + page_size).min(max_rows.saturating_sub(1));
                    self.selected_correlation = Some((next_row, col));
                    self.correlation_table_state.select(Some(next_row));
                }
            }
        }
    }

    pub fn page_up(&mut self, page_size: usize) {
        if self.focus == AnalysisFocus::Sidebar {
            return; // Sidebar doesn't use page navigation
        }

        match self.selected_tool {
            AnalysisTool::Describe => {
                if let Some(current) = self.table_state.selected() {
                    let next = current.saturating_sub(page_size);
                    self.table_state.select(Some(next));
                }
            }
            AnalysisTool::DistributionAnalysis => {
                if let Some(current) = self.distribution_table_state.selected() {
                    let next = current.saturating_sub(page_size);
                    self.distribution_table_state.select(Some(next));
                    self.selected_distribution = Some(next);
                }
            }
            AnalysisTool::CorrelationMatrix => {
                if let Some((row, col)) = self.selected_correlation {
                    let prev_row = row.saturating_sub(page_size);
                    self.selected_correlation = Some((prev_row, col));
                    self.correlation_table_state.select(Some(prev_row));
                }
            }
        }
    }

    pub fn move_correlation_cell(
        &mut self,
        direction: (i32, i32),
        max_rows: usize,
        max_cols: usize,
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
        }
    }

    // Distribution selector navigation
    pub fn next_distribution(&mut self) {
        if let Some(current) = self.distribution_selector_state.selected() {
            let next = (current + 1).min(4); // 5 distributions: Normal, LogNormal, Uniform, PowerLaw, Exponential
            self.distribution_selector_state.select(Some(next));
        } else {
            self.distribution_selector_state.select(Some(0));
        }
    }

    pub fn previous_distribution(&mut self) {
        if let Some(current) = self.distribution_selector_state.selected() {
            if current > 0 {
                self.distribution_selector_state.select(Some(current - 1));
            }
        } else {
            self.distribution_selector_state.select(Some(0));
        }
    }

    pub fn select_distribution(&mut self) {
        if let Some(idx) = self.distribution_selector_state.selected() {
            self.selected_theoretical_distribution = match idx {
                0 => DistributionType::Normal,
                1 => DistributionType::LogNormal,
                2 => DistributionType::Uniform,
                3 => DistributionType::PowerLaw,
                4 => DistributionType::Exponential,
                _ => DistributionType::Normal,
            };
        }
    }
}

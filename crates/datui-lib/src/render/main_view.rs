/// Determines which full-screen content is active in the main view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainViewContent {
    /// Data table with optional sidebar and input strip.
    Datatable,
    /// Full-screen analysis modal.
    Analysis,
    /// Full-screen chart view.
    Chart,
}

impl MainViewContent {
    /// Determine active main-view content from app state.
    pub fn from_app_state(analysis_active: bool, input_mode_chart: bool) -> Self {
        if analysis_active {
            MainViewContent::Analysis
        } else if input_mode_chart {
            MainViewContent::Chart
        } else {
            MainViewContent::Datatable
        }
    }
}

/// Control bar configuration provided by the active main view.
/// The main render loop uses this to build the Controls widget.
#[derive(Debug, Clone)]
pub enum ControlBarSpec {
    /// Default datatable controls (row count, etc.) with optional dimmed and query-active state.
    Datatable { dimmed: bool, query_active: bool },
    /// Custom keybinding list for this view (e.g. analysis or chart).
    Custom(Vec<(&'static str, &'static str)>),
}

/// Returns the control bar keybindings and options for the current main view content.
/// The main render loop calls this and applies the result to the Controls widget.
pub fn control_bar_spec(app: &crate::App, content: MainViewContent) -> ControlBarSpec {
    match content {
        MainViewContent::Datatable => {
            let query_active = app
                .data_table_state
                .as_ref()
                .map(|s| !s.active_query.trim().is_empty())
                .unwrap_or(false);
            let dimmed = app.show_help
                || app.input_mode == crate::InputMode::Editing
                || app.input_mode == crate::InputMode::SortFilter
                || app.input_mode == crate::InputMode::PivotMelt
                || app.input_mode == crate::InputMode::Info
                || app.sort_filter_modal.active;
            ControlBarSpec::Datatable {
                dimmed,
                query_active,
            }
        }
        MainViewContent::Analysis => {
            let mut pairs = vec![
                ("Esc", "Back"),
                ("↑↓", "Navigate"),
                ("←→", "Scroll Columns"),
                ("Tab", "Sidebar"),
                ("Enter", "Select"),
            ];
            if app.sampling_threshold.is_some() {
                if let Some(results) = app.analysis_modal.current_results() {
                    if results.sample_size.is_some() {
                        pairs.push(("r", "Resample"));
                    }
                }
            }
            ControlBarSpec::Custom(pairs)
        }
        MainViewContent::Chart => ControlBarSpec::Custom(vec![("Esc", "Back"), ("e", "Export")]),
    }
}

//! The analysis modal's keys.

use crate::feedback::Confirm;
use crate::form::ListMove;
use crate::{ANALYSIS_READ_WAITS, App, AppEvent, analysis_modal, sample_modal, sampling};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys while the analysis modal is open.
    pub(crate) fn analysis_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        // Esc stops waiting for a run in flight, acting at once (see
        // `hard_escape_while_busy`) rather than queueing behind it.
        if event.code == KeyCode::Esc && self.analysis_modal.computing.is_some() {
            self.cancel_analysis();
            return None;
        }
        // The Sample form owns the keys while it has the cursor: floating over a result,
        // or in an empty pane once Tab moves in.
        if self
            .analysis_modal
            .sample_form
            .as_ref()
            .is_some_and(|form| {
                !form.inline || self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
            })
        {
            return self.sample_form_key(event);
        }
        let quality =
            self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        if event.code == KeyCode::Char('s') && self.analysis_modal.sample_key_opens_form() {
            // Data Quality stages the sample in Setup; every other tool runs on it.
            if quality {
                self.open_quality_sample_form();
            } else {
                self.open_sample_form();
            }
            return None;
        }
        // The sample in use's rows: on a report, not over a draft naming other rows.
        if event.code == KeyCode::Char('v')
            && self.analysis_modal.sample_key_opens_form()
            && !(quality && self.analysis_modal.quality.page.is_setup())
        {
            return self.read_sample_view();
        }
        if quality
            && self.analysis_modal.view == analysis_modal::AnalysisView::Main
            && let std::ops::ControlFlow::Break(answer) = self.quality_key(event)
        {
            return answer;
        }
        if let Some(step) = ListMove::from_key(event) {
            self.analysis_list_move(step);
            return None;
        }
        match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.view != analysis_modal::AnalysisView::Main {
                        self.analysis_modal.close_detail();
                    } else if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                        && self.analysis_modal.selected_tool.is_some()
                    {
                        // One level at a time: the pane hands the cursor back to the tools; Esc there
                        // closes.
                        self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
                        self.sync_sample_form_focus();
                    } else {
                        self.close_overlay();
                    }
                }
                KeyCode::Char('?') => {
                    self.open_help_overlay();
                }
                // Both coefficients come from the one run, so switching reads nothing.
                KeyCode::Char('m')
                    if matches!(
                        self.analysis_modal.selected_tool,
                        Some(analysis_modal::AnalysisTool::CorrelationMatrix)
                    ) =>
                {
                    self.analysis_modal.correlation_method =
                        self.analysis_modal.correlation_method.toggled();
                }
                // Another sample, or every row: only where results are a sample, and only on the
                // main view, not inside a detail.
                KeyCode::Char('r') if self.analysis_results_are_sampled() => {
                    let sample = sampling::Sample {
                        seed: sample_modal::new_seed(),
                        ..self.analysis_modal.sample.clone()
                    };
                    return self.apply_sample(sample);
                }
                // Results keep their rows; `t` reads the new ones with the same sample.
                KeyCode::Char('t')
                    if self.follow_rows_waiting()
                        && self.analysis_modal.current_results().is_some() =>
                {
                    self.take_follow_rows(false);
                    return self.apply_sample(self.analysis_modal.sample.clone());
                }
                // Refused while a cancelled run still reads: Polars cannot stop it, and a second
                // full read beside it can run memory out.
                KeyCode::Char('a')
                    if self.analysis_results_are_sampled() && self.cancelled_work_running() =>
                {
                    self.flash_note(ANALYSIS_READ_WAITS.to_string());
                }
                KeyCode::Char('a') if self.analysis_results_are_sampled() => {
                    let total = self
                        .analysis_modal
                        .current_results()
                        .map(|r| r.total_rows)
                        .unwrap_or_default();
                    self.confirmation_modal.show(
                        format!(
                            "Read all {} rows? It can take much longer than the sample. \
                             Esc stops waiting; the read finishes in the background.",
                            crate::numfmt::group_chrome(total)
                        ),
                        Confirm::ReadAll,
                    );
                    self.confirmation_modal.yes_label = "Read all";
                }
                KeyCode::Tab | KeyCode::BackTab => {
                    // Tab moves sidebar <-> result; detail views have one focusable thing.
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.switch_focus();
                        self.sync_sample_form_focus();
                    }
                }
                KeyCode::Enter
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Sidebar {
                        // Enter again on the tool showing its Sample form runs it as the form stands: two
                        // Enters from the list take the defaults.
                        if self.analysis_modal.sample_form.as_ref().is_some_and(|f| f.inline)
                            && self.analysis_modal.highlighted_tool()
                                == self.analysis_modal.selected_tool
                        {
                            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
                            return self.run_sample_form();
                        }
                        // Enter on a tool enters its pane: its result, its Sample form, or its run.
                        self.analysis_modal.select_tool();
                        self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
                        self.analysis_modal.sample_form = None;
                        // A tool with a result shows it; one without shows the Sample form so the first
                        // run reads the rows asked for.
                        let has_result = match self.analysis_modal.selected_tool {
                            Some(analysis_modal::AnalysisTool::Describe) => {
                                self.analysis_modal.describe_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                self.analysis_modal.distribution_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                self.analysis_modal.correlation_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::DataQuality) => {
                                self.restore_recent_quality_plan();
                                // The plan's rows are the shared sample's; a draft staged in Setup stays.
                                let draft = self.analysis_modal.quality.setup_before.is_some();
                                if !draft {
                                    self.sync_quality_plan();
                                }
                                (!draft && self.restore_cached_quality())
                                    || self.analysis_modal.quality.results.is_some()
                            }
                            None => true,
                        };
                        // Data Quality never runs from the list: Setup is its pane, and only Run reads.
                        if !has_result
                            && self.analysis_modal.selected_tool
                                == Some(analysis_modal::AnalysisTool::DataQuality)
                        {
                            self.open_quality_setup();
                            return None;
                        }
                        // Once a sample has run on this dataset every tool reads it: a tool without a
                        // result runs at once, and s changes the sample for all.
                        let sample_run = self.analysis_modal.sample_run_for
                            == Some(self.dataset_generation);
                        if !has_result && sample_run {
                            return self.start_analysis_run();
                        }
                        // Before the first run the form is the pane: Enter runs, arrows change a setting,
                        // Esc returns to the list.
                        if !has_result {
                            self.open_first_run_form();
                        }
                    } else {
                        // Enter in main area opens detail view if applicable
                        match self.analysis_modal.selected_tool {
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                self.analysis_modal.open_distribution_detail();
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                self.analysis_modal.open_correlation_detail();
                            }
                            _ => {}
                        }
                    }
                }
                KeyCode::Char('s')
                    // Toggle histogram scale (linear/log) in distribution detail view
                    if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail =>
                {
                    self.analysis_modal.histogram_scale = match self.analysis_modal.histogram_scale {
                        analysis_modal::HistogramScale::Linear => analysis_modal::HistogramScale::Log,
                        analysis_modal::HistogramScale::Log => analysis_modal::HistogramScale::Linear,
                    };
                }
                KeyCode::Left | KeyCode::Char('h')
                    if !event.modifiers.contains(KeyModifiers::CONTROL)
                        && self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar
                        | analysis_modal::AnalysisFocus::DistributionSelector => {}
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(
                                    analysis_modal::AnalysisTool::Describe
                                    | analysis_modal::AnalysisTool::DistributionAnalysis,
                                ) => {
                                    self.analysis_modal.scroll_left();
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    // The matrix keeps the cell in view as it draws.
                                    self.analysis_modal.move_correlation_cell((0, -1));
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                None => {}
                            }
                        }
                    }
                }
                KeyCode::Right | KeyCode::Char('l')
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar
                        | analysis_modal::AnalysisFocus::DistributionSelector => {}
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                // The table set how far it scrolls as it drew.
                                Some(
                                    analysis_modal::AnalysisTool::Describe
                                    | analysis_modal::AnalysisTool::DistributionAnalysis,
                                ) => {
                                    self.analysis_modal.scroll_right();
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    // The matrix keeps the cell in view as it draws.
                                    self.analysis_modal.move_correlation_cell((0, 1));
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                None => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        None
    }

    /// ↑↓, PageUp, PageDown, Home and End: the tools in the sidebar, a distribution
    /// in the detail's selector, or the focused tool's rows.
    fn analysis_list_move(&mut self, step: ListMove) {
        use analysis_modal::{AnalysisFocus, AnalysisTool, AnalysisView};
        let main_view = self.analysis_modal.view == AnalysisView::Main;
        match self.analysis_modal.focus {
            AnalysisFocus::Sidebar if main_view => match step {
                ListMove::Up => self.analysis_modal.previous_tool(),
                ListMove::Down => self.analysis_modal.next_tool(),
                ListMove::Home => self.analysis_modal.sidebar_state.select(Some(0)),
                ListMove::End => {
                    let last = AnalysisTool::ALL.len() - 1;
                    self.analysis_modal.sidebar_state.select(Some(last));
                }
                ListMove::PageUp | ListMove::PageDown => {}
            },
            AnalysisFocus::DistributionSelector => {
                let detail = self.analysis_modal.view == AnalysisView::DistributionDetail;
                match step {
                    ListMove::Up if detail => self.analysis_modal.previous_distribution(),
                    ListMove::Down if detail => self.analysis_modal.next_distribution(),
                    ListMove::Home | ListMove::End => {
                        let last = self.analysis_modal.distribution_choices().saturating_sub(1);
                        let at = if step == ListMove::Home { 0 } else { last };
                        self.analysis_modal
                            .distribution_selector_state
                            .select(Some(at));
                        self.analysis_modal.select_distribution();
                    }
                    _ => {}
                }
            }
            AnalysisFocus::Main if main_view => {
                let rows = match self.analysis_modal.selected_tool {
                    Some(AnalysisTool::Describe) => self
                        .data_table_state
                        .as_ref()
                        .map_or(0, |s| s.schema().len()),
                    Some(AnalysisTool::DistributionAnalysis) => self
                        .analysis_modal
                        .current_results()
                        .map_or(0, |r| r.distribution_analyses.len()),
                    Some(AnalysisTool::CorrelationMatrix) => self.analysis_modal.correlation_size(),
                    Some(AnalysisTool::DataQuality) => self.analysis_modal.quality_row_count(),
                    None => 0,
                };
                self.analysis_modal.move_row(step, rows);
            }
            _ => {}
        }
    }
}

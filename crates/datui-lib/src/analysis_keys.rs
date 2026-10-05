//! The analysis modal's keys.

use crate::{
    ANALYSIS_READ_WAITS, App, AppEvent, QUALITY_RUN_WAITS, analysis_modal, data_quality,
    sample_modal, sampling,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys while the analysis modal is open.
    pub(crate) fn analysis_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        // A run in flight, whichever tool: Esc stops waiting for it. It acts at once
        // (see `hard_escape_while_busy`) rather than queueing behind the run it is
        // meant to cancel.
        if event.code == KeyCode::Esc && self.analysis_modal.computing.is_some() {
            self.cancel_analysis();
            return None;
        }
        // The Sample form owns the keys while it has the cursor: always when it
        // floats over a result, and in a tool's empty pane once Tab moves in.
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
        // The rows of the sample in use: on a report, not over a draft that may
        // name other rows.
        if event.code == KeyCode::Char('v')
            && self.analysis_modal.sample_key_opens_form()
            && !(quality && self.analysis_modal.data_quality_page.is_setup())
        {
            return self.read_sample_view();
        }
        if quality && self.analysis_modal.view == analysis_modal::AnalysisView::Main {
            use crate::data_quality::QualityPage;

            // A finding's popup scrolls when it holds more than the screen does.
            if self.analysis_modal.data_quality_observation_detail {
                let rows = match event.code {
                    KeyCode::Down | KeyCode::Char('j') => Some(1),
                    KeyCode::Up | KeyCode::Char('k') => Some(-1),
                    KeyCode::PageDown => Some(10),
                    KeyCode::PageUp => Some(-10),
                    KeyCode::End => Some(i32::from(u16::MAX)),
                    KeyCode::Home => Some(-i32::from(u16::MAX)),
                    _ => None,
                };
                if let Some(rows) = rows {
                    self.analysis_modal.scroll_quality_detail(rows);
                    return None;
                }
            }
            if (self.analysis_modal.data_quality_show_access
                || self.analysis_modal.data_quality_observation_detail
                || self.analysis_modal.data_quality_evidence_read.is_some())
                && !matches!(event.code, KeyCode::Esc | KeyCode::Enter)
            {
                return None;
            }

            // A Setup row's choices own the keys while they are open.
            if self.analysis_modal.data_quality_picker.is_some() {
                match event.code {
                    KeyCode::Esc => self.analysis_modal.data_quality_picker = None,
                    KeyCode::Enter => self.choose_setup_picker(),
                    code => {
                        if let Some(picker) = self.analysis_modal.data_quality_picker.as_mut() {
                            match code {
                                KeyCode::Up => picker.state.move_up(),
                                KeyCode::Down => picker.state.move_down(),
                                KeyCode::Backspace => picker.state.backspace(),
                                KeyCode::Char(c) => picker.state.filter_key(c, event.modifiers),
                                _ => {}
                            }
                        }
                    }
                }
                return None;
            }
            // The export dialog owns every key while it is open, `?` included
            // when the path types.
            if self.analysis_modal.data_quality_export.is_some()
                && (event.code != KeyCode::Char('?') || self.analysis_modal.export_typing())
            {
                return self.quality_export_key(event);
            }
            // The intent form owns every key while it is open, `?` included
            // when it types.
            if self.analysis_modal.data_quality_intent_form.is_some()
                && (event.code != KeyCode::Char('?') || self.analysis_modal.intent_typing())
            {
                self.intent_form_key(event);
                return None;
            }
            // The intent list owns the keys: which column, and its form.
            if self.analysis_modal.data_quality_page == QualityPage::Intent
                && event.code != KeyCode::Char('?')
            {
                let columns = self.quality_intent_columns().len();
                let field = self
                    .analysis_modal
                    .data_quality_plan_field
                    .min(columns.saturating_sub(1));
                match event.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        if event.code == KeyCode::Esc
                            && let Some(plan) =
                                self.analysis_modal.data_quality_plan_before_edit.take()
                        {
                            self.analysis_modal.data_quality_plan = plan;
                        }
                        self.analysis_modal.data_quality_plan_before_edit = None;
                        self.analysis_modal.data_quality_setup_note = None;
                        self.analysis_modal.set_quality_page(QualityPage::Setup);
                        self.analysis_modal.data_quality_plan_field =
                            analysis_modal::SetupRow::Intent.index();
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.analysis_modal.data_quality_plan_field =
                            (field + 1).min(columns.saturating_sub(1));
                    }
                    KeyCode::PageUp => {
                        self.analysis_modal.data_quality_plan_field = field.saturating_sub(10);
                    }
                    KeyCode::PageDown => {
                        self.analysis_modal.data_quality_plan_field =
                            (field + 10).min(columns.saturating_sub(1));
                    }
                    KeyCode::Home => self.analysis_modal.data_quality_plan_field = 0,
                    KeyCode::End => {
                        self.analysis_modal.data_quality_plan_field = columns.saturating_sub(1);
                    }
                    KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                        self.analysis_modal.data_quality_plan_field = field;
                        self.open_intent_form();
                    }
                    _ => {}
                }
                return None;
            }
            // The role editor owns the keys: the role, and its column.
            if self.analysis_modal.data_quality_page == QualityPage::TimeRoles
                && event.code != KeyCode::Char('?')
            {
                let field = self.analysis_modal.data_quality_plan_field;
                match event.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        if event.code == KeyCode::Esc
                            && let Some(plan) =
                                self.analysis_modal.data_quality_plan_before_edit.take()
                        {
                            self.analysis_modal.data_quality_plan = plan;
                        }
                        self.analysis_modal.data_quality_plan_before_edit = None;
                        self.analysis_modal.data_quality_setup_note = None;
                        self.analysis_modal.set_quality_page(QualityPage::Setup);
                        self.analysis_modal.data_quality_plan_field =
                            analysis_modal::SetupRow::TimeRoles.index();
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.analysis_modal.data_quality_plan_field =
                            (field + 1).min(crate::data_quality::TemporalRole::ALL.len() - 1);
                    }
                    KeyCode::Left | KeyCode::Char('h') | KeyCode::Right | KeyCode::Char('l') => {
                        let columns = self.quality_time_candidates();
                        self.analysis_modal.cycle_quality_time_role(
                            field,
                            &columns,
                            matches!(event.code, KeyCode::Right | KeyCode::Char('l')),
                        );
                    }
                    _ => {}
                }
                return None;
            }
            // The Expected editor owns the keys; `?` is help unless it types.
            if self.analysis_modal.data_quality_page == QualityPage::ExpectedWindows
                && (event.code != KeyCode::Char('?')
                    || self.analysis_modal.quality_expected_typing())
            {
                self.expected_form_key(event);
                return None;
            }
            // The pairs editor owns the keys: which pair, and whether it is measured.
            if self.analysis_modal.data_quality_page == QualityPage::IntervalPairs
                && event.code != KeyCode::Char('?')
            {
                let pairs = self.analysis_modal.data_quality_plan.candidate_pairs();
                let field = self
                    .analysis_modal
                    .data_quality_plan_field
                    .min(pairs.len().saturating_sub(1));
                match event.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        if event.code == KeyCode::Esc
                            && let Some(plan) =
                                self.analysis_modal.data_quality_plan_before_edit.take()
                        {
                            self.analysis_modal.data_quality_plan = plan;
                        }
                        self.analysis_modal.data_quality_plan_before_edit = None;
                        self.analysis_modal.data_quality_setup_note = None;
                        self.analysis_modal.set_quality_page(QualityPage::Setup);
                        self.analysis_modal.data_quality_plan_field =
                            analysis_modal::SetupRow::Intervals.index();
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.analysis_modal.data_quality_plan_field =
                            (field + 1).min(pairs.len().saturating_sub(1));
                    }
                    KeyCode::Home => self.analysis_modal.data_quality_plan_field = 0,
                    KeyCode::End => {
                        self.analysis_modal.data_quality_plan_field = pairs.len().saturating_sub(1);
                    }
                    KeyCode::Char(' ')
                    | KeyCode::Left
                    | KeyCode::Char('h')
                    | KeyCode::Right
                    | KeyCode::Char('l') => {
                        if let Some(pair) = pairs.get(field) {
                            self.analysis_modal.data_quality_plan.toggle_interval(*pair);
                        }
                    }
                    _ => {}
                }
                return None;
            }
            // Setup is edited where it stands: ↑↓ or Tab the row, ←→ a short
            // list's choice, Space the row's editor, Enter runs from any row, and
            // Esc discards every staged edit.
            if self.analysis_modal.data_quality_page == QualityPage::Setup
                && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                && !self.analysis_modal.data_quality_show_access
            {
                use analysis_modal::SetupRow;
                // Setup can hold the cursor without having been opened: after Esc
                // handed it to the tools and Tab brought it back, or after a run
                // that failed. Whatever is edited now is still a draft.
                if self.analysis_modal.data_quality_setup_before.is_none() {
                    self.open_quality_setup();
                }
                let rows = SetupRow::ALL.len();
                let field = self.analysis_modal.data_quality_plan_field.min(rows - 1);
                match event.code {
                    KeyCode::Up | KeyCode::Char('k') | KeyCode::BackTab => {
                        self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                        return None;
                    }
                    KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => {
                        self.analysis_modal.data_quality_plan_field = (field + 1).min(rows - 1);
                        return None;
                    }
                    KeyCode::Home => {
                        self.analysis_modal.data_quality_plan_field = 0;
                        return None;
                    }
                    KeyCode::End => {
                        self.analysis_modal.data_quality_plan_field = rows - 1;
                        return None;
                    }
                    KeyCode::Char(' ') => {
                        self.analysis_modal.data_quality_plan_field = field;
                        return self.open_setup_row();
                    }
                    KeyCode::Left | KeyCode::Char('h') | KeyCode::Right | KeyCode::Char('l') => {
                        let forward = matches!(event.code, KeyCode::Right | KeyCode::Char('l'));
                        let row = SetupRow::at(field);
                        match row {
                            SetupRow::Grain
                            | SetupRow::Compare
                            | SetupRow::Values
                            | SetupRow::Latency
                            | SetupRow::WindowBy => {
                                let context = self.quality_plan_context();
                                self.analysis_modal.data_quality_setup_note = None;
                                self.analysis_modal
                                    .cycle_setup_choice(row, &context, forward);
                            }
                            // A row whose value is a form or a list opens it.
                            _ if forward => return self.open_setup_row(),
                            _ => {}
                        }
                        return None;
                    }
                    KeyCode::Enter => return self.run_quality_setup(),
                    KeyCode::Char('d') => {
                        self.release_quality_rows();
                        return None;
                    }
                    KeyCode::Esc => {
                        self.leave_quality_setup();
                        return None;
                    }
                    _ => {}
                }
            }

            match event.code {
                KeyCode::Esc if self.analysis_modal.data_quality_show_access => {
                    self.analysis_modal.data_quality_show_access = false;
                    return None;
                }
                // A staged read of a finding's rows: Enter reads, Esc goes back to
                // the finding having read nothing.
                KeyCode::Esc if self.analysis_modal.data_quality_evidence_read.is_some() => {
                    self.analysis_modal.data_quality_evidence_read = None;
                    return None;
                }
                KeyCode::Enter if self.analysis_modal.data_quality_evidence_read.is_some() => {
                    return self.confirm_evidence_read();
                }
                KeyCode::Esc if self.analysis_modal.data_quality_observation_detail => {
                    self.analysis_modal.data_quality_observation_detail = false;
                    return None;
                }
                KeyCode::Enter if self.analysis_modal.data_quality_show_access => {
                    self.analysis_modal.data_quality_show_access = false;
                    return None;
                }
                KeyCode::Enter if self.analysis_modal.data_quality_observation_detail => {
                    // The clean entry has no rows to open; Enter shows every
                    // check it passed, and again the most important few.
                    if self.analysis_modal.quality_selected_is_clean() {
                        self.analysis_modal.data_quality_checks_expanded =
                            !self.analysis_modal.data_quality_checks_expanded;
                        self.analysis_modal.data_quality_detail_scroll.offset = 0;
                        return None;
                    }
                    let event = self.open_quality_evidence();
                    // Rows on their way, or a read waiting for Enter, keep the
                    // finding open: Esc from the rows comes back to it. Enter on
                    // a finding with no rows closes it.
                    if self.analysis_modal.active
                        && !self.error_modal.active
                        && self.analysis_modal.data_quality_evidence_read.is_none()
                        && self.analysis_modal.computing.is_none()
                    {
                        self.analysis_modal.data_quality_observation_detail = false;
                    }
                    return event;
                }
                // A drill-in backs out to the list it came from.
                KeyCode::Esc
                    if self.analysis_modal.data_quality_page == QualityPage::SegmentDetail =>
                {
                    self.analysis_modal.close_segment_detail();
                    return None;
                }
                KeyCode::Esc
                    if self.analysis_modal.data_quality_page == QualityPage::IntervalDetail =>
                {
                    self.analysis_modal.close_interval_detail();
                    return None;
                }
                KeyCode::Esc
                    if matches!(
                        self.analysis_modal.data_quality_page,
                        QualityPage::TrendDetail | QualityPage::Gaps
                    ) =>
                {
                    self.analysis_modal.close_to_trends();
                    return None;
                }
                KeyCode::Esc if self.analysis_modal.data_quality_page == QualityPage::Detail => {
                    self.analysis_modal
                        .set_quality_column_page(QualityPage::Columns);
                    return None;
                }
                KeyCode::Char('p') => {
                    self.analysis_modal.data_quality_show_access =
                        !self.analysis_modal.data_quality_show_access;
                    return None;
                }
                // Setup, with every setting, from anywhere in the report.
                KeyCode::Char('e') => {
                    self.open_quality_setup();
                    return None;
                }
                // The report's tabs; Setup is left with Enter or Esc, so a draft is
                // never left staged behind a report page.
                KeyCode::Char(digit @ '1'..='5')
                    if !self.analysis_modal.data_quality_page.is_setup() =>
                {
                    let tab = digit as usize - '1' as usize;
                    self.analysis_modal.show_quality_tab(QualityPage::TABS[tab]);
                    return None;
                }
                KeyCode::Char('m')
                    if self.analysis_modal.data_quality_page == QualityPage::Trends =>
                {
                    self.analysis_modal.cycle_quality_metric();
                    return None;
                }
                KeyCode::Char('m')
                    if self.analysis_modal.data_quality_page == QualityPage::TrendDetail =>
                {
                    self.analysis_modal.cycle_trend_detail_metric();
                    return None;
                }
                KeyCode::Char('w')
                    if matches!(
                        self.analysis_modal.data_quality_page,
                        QualityPage::Trends | QualityPage::TrendDetail
                    ) && self.analysis_modal.data_quality_results.is_some() =>
                {
                    self.stage_coarser_grain();
                    return None;
                }
                KeyCode::Char('g')
                    if matches!(
                        self.analysis_modal.data_quality_page,
                        QualityPage::Trends | QualityPage::TrendDetail
                    ) && self
                        .analysis_modal
                        .data_quality_results
                        .as_ref()
                        .is_some_and(|results| {
                            crate::quality_trends::expected_gaps(
                                self.analysis_modal.quality_result_plan(),
                                results,
                            )
                            .is_some()
                        }) =>
                {
                    self.analysis_modal.set_quality_page(QualityPage::Gaps);
                    return None;
                }
                KeyCode::Char('b')
                    if self.analysis_modal.data_quality_page == QualityPage::Segments
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    // The segment under the cursor, looked up while the results
                    // still order the list.
                    let selected = self.analysis_modal.selected_segment();
                    if let Some(mut results) = self.analysis_modal.data_quality_results.take() {
                        if let Some(label) = selected
                            .and_then(|index| results.segments.get(index))
                            .map(|segment| segment.label.clone())
                        {
                            let mut plan = self.analysis_modal.quality_result_plan().clone();
                            plan.comparison = crate::data_quality::QualityComparison::Baseline;
                            plan.baseline_segment = Some(label.clone());
                            results.compare_segments(&plan);
                            self.cache_quality_result(&results, plan.clone());
                            self.analysis_modal.data_quality_last_plan = Some(plan);
                            let working = &mut self.analysis_modal.data_quality_plan;
                            working.comparison = crate::data_quality::QualityComparison::Baseline;
                            working.baseline_segment = Some(label);
                        }
                        self.analysis_modal.data_quality_results = Some(results);
                    }
                    return None;
                }
                KeyCode::Char('o')
                    if self.analysis_modal.data_quality_page == QualityPage::Segments =>
                {
                    self.analysis_modal.toggle_segment_order();
                    return None;
                }
                // Overview's findings, narrowed and ordered from the report on
                // screen: nothing is measured again.
                KeyCode::Char(key @ ('c' | 't' | 'o'))
                    if self.analysis_modal.data_quality_page == QualityPage::Overview
                        && self.analysis_modal.data_quality_results.is_some()
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    if key == 'o' {
                        self.analysis_modal.cycle_findings_order();
                    } else {
                        self.analysis_modal.open_findings_picker(key == 'c');
                    }
                    return None;
                }
                // Esc shows every finding again before it leaves the page.
                KeyCode::Esc
                    if self.analysis_modal.data_quality_page == QualityPage::Overview
                        && self.analysis_modal.data_quality_findings.narrowed()
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    self.analysis_modal.clear_findings_narrowing();
                    return None;
                }
                // Write the report on screen to a file: what was measured, never a
                // draft, and nothing read to do it.
                KeyCode::Char('x')
                    if !self.analysis_modal.data_quality_page.is_setup()
                        && self.analysis_modal.data_quality_results.is_some() =>
                {
                    self.open_quality_export();
                    return None;
                }
                // Another sample, run at once: a new seed for every tool. On a sampled
                // report only, as on every tool, never over a draft, and not beside
                // a cancelled read.
                KeyCode::Char('r')
                    if !self.analysis_modal.data_quality_page.is_setup()
                        && self.analysis_modal.data_quality_results.is_some()
                        && self.analysis_modal.data_quality_plan.compute
                            == data_quality::QualityCompute::Sample =>
                {
                    if self.cancelled_analysis_running().is_some() {
                        self.flash_note(QUALITY_RUN_WAITS.to_string());
                        return None;
                    }
                    let before = self.analysis_modal.data_quality_plan.clone();
                    self.analysis_modal.data_quality_plan.sample_seed = sample_modal::new_seed();
                    let event = self.run_quality_setup();
                    // Refused, with the reason on Setup's line: the plan stays the
                    // one the report was run with, and the reason is said here.
                    if let Some(note) = self.analysis_modal.data_quality_setup_note.take() {
                        self.analysis_modal.data_quality_plan = before;
                        self.flash_note(note);
                    }
                    return event;
                }
                KeyCode::Enter
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    if let Some(setup) = self.quality_page_setup() {
                        // Straight to the setting that fills the page, in Setup.
                        self.open_quality_setup();
                        self.analysis_modal.data_quality_plan_field = match setup {
                            data_quality::QualitySetup::Grain => analysis_modal::SetupRow::Grain,
                            data_quality::QualitySetup::TimeRoles => {
                                analysis_modal::SetupRow::TimeRoles
                            }
                            data_quality::QualitySetup::Intervals => {
                                analysis_modal::SetupRow::Intervals
                            }
                        }
                        .index();
                        return self.open_setup_row();
                    } else if self.analysis_modal.data_quality_page == QualityPage::Overview {
                        let findings = self.analysis_modal.quality_row_count();
                        self.analysis_modal.data_quality_checks_expanded = false;
                        self.analysis_modal.data_quality_detail_scroll =
                            analysis_modal::DetailScroll::default();
                        self.analysis_modal.data_quality_observation_detail = self
                            .analysis_modal
                            .data_quality_table_state
                            .selected()
                            .is_some_and(|index| index < findings);
                    } else if self.analysis_modal.data_quality_page == QualityPage::Columns {
                        self.analysis_modal
                            .set_quality_column_page(QualityPage::Detail);
                    } else if self.analysis_modal.data_quality_page == QualityPage::Detail {
                        self.analysis_modal
                            .set_quality_column_page(QualityPage::Columns);
                    } else if self.analysis_modal.data_quality_page == QualityPage::Segments
                        && self.analysis_modal.data_quality_results.is_some()
                    {
                        self.analysis_modal.open_segment_detail();
                    } else if self.analysis_modal.data_quality_page == QualityPage::SegmentDetail {
                        self.analysis_modal.close_segment_detail();
                    } else if self.analysis_modal.data_quality_page == QualityPage::Trends
                        && self.analysis_modal.data_quality_results.is_some()
                    {
                        self.analysis_modal.open_trend_detail();
                    } else if matches!(
                        self.analysis_modal.data_quality_page,
                        QualityPage::TrendDetail | QualityPage::Gaps
                    ) {
                        self.analysis_modal.close_to_trends();
                    } else if self.analysis_modal.data_quality_page == QualityPage::Intervals {
                        self.analysis_modal.open_interval_detail();
                    } else if self.analysis_modal.data_quality_page == QualityPage::IntervalDetail {
                        return self.open_interval_evidence();
                    }
                    return None;
                }
                KeyCode::Down | KeyCode::Char('j')
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    let rows = self.analysis_modal.quality_row_count();
                    self.analysis_modal.next_row(rows);
                    return None;
                }
                KeyCode::Up | KeyCode::Char('k')
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    self.analysis_modal.previous_row();
                    return None;
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    self.analysis_modal.step_quality_tab(false);
                    return None;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.analysis_modal.step_quality_tab(true);
                    return None;
                }
                KeyCode::PageDown
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    let rows = self.analysis_modal.quality_row_count();
                    self.analysis_modal.page_down(rows, 10);
                    return None;
                }
                KeyCode::PageUp
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    self.analysis_modal.page_up(10);
                    return None;
                }
                KeyCode::Home
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    self.analysis_modal.data_quality_table_state.select(Some(0));
                    return None;
                }
                KeyCode::End
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    let rows = self.analysis_modal.quality_row_count();
                    if rows > 0 {
                        self.analysis_modal
                            .data_quality_table_state
                            .select(Some(rows - 1));
                    }
                    return None;
                }
                _ => {}
            }
        }
        match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.view != analysis_modal::AnalysisView::Main {
                        // Close detail view
                        self.analysis_modal.close_detail();
                    } else {
                        self.analysis_modal.close();
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
                // Another sample, or every row. Both only where the results are a
                // sample, and only on the main view: inside a detail an undocumented
                // `r` cleared the results out from under it.
                KeyCode::Char('r') if self.analysis_results_are_sampled() => {
                    let sample = sampling::Sample {
                        seed: sample_modal::new_seed(),
                        ..self.analysis_modal.sample.clone()
                    };
                    return self.apply_sample(sample);
                }
                // The results keep the rows they were read of; `t` reads the new ones
                // too, with the same sample.
                KeyCode::Char('t')
                    if self.follow_rows_waiting()
                        && self.analysis_modal.current_results().is_some() =>
                {
                    self.take_follow_rows(false);
                    return self.apply_sample(self.analysis_modal.sample.clone());
                }
                // Refused while a cancelled run is still reading: Polars cannot stop it,
                // and a second full read beside it is how memory runs out.
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
                    self.pending_read_all = true;
                    self.confirmation_modal.show(format!(
                        "Read all {} rows? It can take much longer than the sample. \
                         Esc stops waiting; the read finishes in the background.",
                        crate::numfmt::group_chrome(total)
                    ));
                    self.confirmation_modal.yes_label = "Read all";
                }
                KeyCode::Tab => {
                    // One rule for the whole screen: Tab moves sidebar <-> result.
                    // The detail views have a single focusable thing, so it stays.
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.switch_focus();
                        self.sync_sample_form_focus();
                    }
                }
                KeyCode::Enter
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Sidebar {
                        // Enter again on the tool whose Sample form is showing runs it
                        // with the form as it stands: two Enters from the list take the
                        // defaults, and the cursor never leaves it.
                        if self.analysis_modal.sample_form.as_ref().is_some_and(|f| f.inline)
                            && self.analysis_modal.highlighted_tool()
                                == self.analysis_modal.selected_tool
                        {
                            return self.run_sample_form();
                        }
                        // Select tool from sidebar
                        self.analysis_modal.select_tool();
                        self.analysis_modal.sample_form = None;
                        // A tool with a result shows it. One without shows the Sample
                        // form in its pane, so the first run reads the rows asked for;
                        // Enter runs it with the defaults as they stand.
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
                                // The plan's rows are the shared sample's, whatever
                                // the last plan here read.
                                // A draft staged in Setup stays as it is.
                                let draft = self.analysis_modal.data_quality_setup_before.is_some();
                                if !draft {
                                    self.sync_quality_plan();
                                }
                                (!draft && self.restore_cached_quality())
                                    || self.analysis_modal.data_quality_results.is_some()
                            }
                            None => true,
                        };
                        // Data Quality never runs from the list: its Setup is the pane,
                        // and only its Run reads, whatever another tool already sampled.
                        if !has_result
                            && self.analysis_modal.selected_tool
                                == Some(analysis_modal::AnalysisTool::DataQuality)
                        {
                            self.open_quality_setup();
                            return None;
                        }
                        // Once a sample has been run on this dataset, every tool reads
                        // it: a tool with no result runs at once, and s changes the
                        // sample for all of them.
                        let sample_run = self.analysis_modal.sample_run_for
                            == Some(self.dataset_generation);
                        if !has_result && sample_run {
                            return self.start_analysis_run();
                        }
                        // Before the first, the form is what the pane is for, so the
                        // cursor goes with it: Enter runs, the arrows change a setting,
                        // Esc hands the cursor back to the list. A tool with a result
                        // leaves the cursor on the list.
                        if !has_result {
                            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
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
                KeyCode::Down | KeyCode::Char('j') => {
                    match self.analysis_modal.view {
                        analysis_modal::AnalysisView::Main => {
                            match self.analysis_modal.focus {
                                analysis_modal::AnalysisFocus::Sidebar => {
                                    // Navigate sidebar tool list
                                    self.analysis_modal.next_tool();
                                }
                                analysis_modal::AnalysisFocus::Main => {
                                    // Navigate in main area based on selected tool
                                    match self.analysis_modal.selected_tool {
                                        Some(analysis_modal::AnalysisTool::Describe) => {
                                            if let Some(state) = &self.data_table_state {
                                                let max_rows = state.schema().len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(
                                            analysis_modal::AnalysisTool::DistributionAnalysis,
                                        ) => {
                                            if let Some(results) =
                                                self.analysis_modal.current_results()
                                            {
                                                let max_rows = results.distribution_analyses.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                            // The matrix keeps the cell in view as it draws.
                                            self.analysis_modal.move_correlation_cell((1, 0));
                                        }
                                        Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                        None => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                        analysis_modal::AnalysisView::DistributionDetail
                            if self.analysis_modal.focus
                                == analysis_modal::AnalysisFocus::DistributionSelector =>
                        {
                            self.analysis_modal.next_distribution();
                        }
                        _ => {}
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
                KeyCode::Up | KeyCode::Char('k') => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.previous_row();
                    } else if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail
                        && self.analysis_modal.focus
                            == analysis_modal::AnalysisFocus::DistributionSelector
                    {
                        self.analysis_modal.previous_distribution();
                    }
                }
                KeyCode::Left | KeyCode::Char('h')
                    if !event.modifiers.contains(KeyModifiers::CONTROL)
                        && self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            // Sidebar navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            // Distribution selector navigation handled by Up/Down
                        }
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
                        analysis_modal::AnalysisFocus::Sidebar => {
                            // Sidebar navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            // Distribution selector navigation handled by Up/Down
                        }
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
                KeyCode::PageDown
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    match self.analysis_modal.selected_tool {
                        Some(analysis_modal::AnalysisTool::Describe) => {
                            if let Some(state) = &self.data_table_state {
                                let max_rows = state.schema().len();
                                let page_size = 10;
                                self.analysis_modal.page_down(max_rows, page_size);
                            }
                        }
                        Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                            if let Some(results) = self.analysis_modal.current_results() {
                                let max_rows = results.distribution_analyses.len();
                                let page_size = 10;
                                self.analysis_modal.page_down(max_rows, page_size);
                            }
                        }
                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                            if let Some(results) = self.analysis_modal.current_results()
                                && let Some(corr) = &results.correlation_matrix {
                                    let max_rows = corr.columns.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                        }
                        Some(analysis_modal::AnalysisTool::DataQuality) => {}
                        None => {}
                    }
                }
                KeyCode::PageUp
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    let page_size = 10;
                    self.analysis_modal.page_up(page_size);
                }
                KeyCode::Home
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            self.analysis_modal.sidebar_state.select(Some(0));
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            self.analysis_modal
                                .distribution_selector_state
                                .select(Some(0));
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    self.analysis_modal.table_state.select(Some(0));
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    self.analysis_modal
                                        .distribution_table_state
                                        .select(Some(0));
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    self.analysis_modal.correlation_table_state.select(Some(0));
                                    self.analysis_modal.selected_correlation = Some((0, 0));
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {
                                    self.analysis_modal.data_quality_table_state.select(Some(0));
                                }
                                None => {}
                            }
                        }
                    }
                }
                KeyCode::End
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            self.analysis_modal.sidebar_state.select(Some(3));
                            // Last tool
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            self.analysis_modal
                                .distribution_selector_state
                                .select(Some(13)); // Last distribution (Weibull, index 13 of 14 total)
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    if let Some(state) = &self.data_table_state {
                                        let max_rows = state.schema().len();
                                        if max_rows > 0 {
                                            self.analysis_modal
                                                .table_state
                                                .select(Some(max_rows - 1));
                                        }
                                    }
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    if let Some(results) = self.analysis_modal.current_results() {
                                        let max_rows = results.distribution_analyses.len();
                                        if max_rows > 0 {
                                            self.analysis_modal
                                                .distribution_table_state
                                                .select(Some(max_rows - 1));
                                        }
                                    }
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    if let Some(results) = self.analysis_modal.current_results()
                                        && let Some(corr) = &results.correlation_matrix {
                                            let max_rows = corr.columns.len();
                                            if max_rows > 0 {
                                                self.analysis_modal
                                                    .correlation_table_state
                                                    .select(Some(max_rows - 1));
                                                self.analysis_modal.selected_correlation =
                                                    Some((max_rows - 1, max_rows - 1));
                                            }
                                        }
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {
                                    let rows = self.analysis_modal.quality_row_count();
                                    if rows > 0 {
                                        self.analysis_modal
                                            .data_quality_table_state
                                            .select(Some(rows - 1));
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        None
    }
}

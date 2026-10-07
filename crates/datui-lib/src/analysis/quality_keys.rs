//! Data Quality's keys in the analysis modal: Setup and its editors, and the report.

use std::ops::ControlFlow;

use crate::app::form::ListMove;
use crate::{
    App, AppEvent, QUALITY_RUN_WAITS, analysis::analysis_modal, analysis::data_quality,
    analysis::sample_modal,
};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// A key on Data Quality's main view: `Break` with its answer when this took it,
    /// `Continue` for the analysis modal's own keys.
    pub(crate) fn quality_key(&mut self, event: &KeyEvent) -> ControlFlow<Option<AppEvent>> {
        use crate::analysis::data_quality::QualityPage;

        // A finding's popup scrolls when it holds more than the screen does.
        if self.analysis_modal.quality.observation_detail
            && let Some(step) = ListMove::from_key(event)
        {
            let most = 65_535;
            let rows = step.delta(10).clamp(-most, most) as i32;
            self.analysis_modal.scroll_quality_detail(rows);
            return ControlFlow::Break(None);
        }
        if (self.analysis_modal.quality.show_access
            || self.analysis_modal.quality.observation_detail
            || self.analysis_modal.quality.evidence_read.is_some())
            && !matches!(event.code, KeyCode::Esc | KeyCode::Enter)
        {
            return ControlFlow::Break(None);
        }

        // A Setup row's choices own the keys while they are open.
        if self.analysis_modal.quality.picker.is_some() {
            match event.code {
                KeyCode::Esc => self.analysis_modal.quality.picker = None,
                KeyCode::Enter => self.choose_setup_picker(),
                code => {
                    if let Some(picker) = self.analysis_modal.quality.picker.as_mut() {
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
            return ControlFlow::Break(None);
        }
        // The export dialog owns every key while open, `?` too when the path types.
        if self.analysis_modal.quality.export.is_some()
            && (event.code != KeyCode::Char('?') || self.analysis_modal.export_typing())
        {
            return ControlFlow::Break(self.quality_export_key(event));
        }
        // The intent form owns every key while open, `?` too when it types.
        if self.analysis_modal.quality.intent_form.is_some()
            && (event.code != KeyCode::Char('?') || self.analysis_modal.intent_typing())
        {
            self.intent_form_key(event);
            return ControlFlow::Break(None);
        }
        // The intent list owns the keys: which column, and its form.
        if self.analysis_modal.quality.page == QualityPage::Intent
            && event.code != KeyCode::Char('?')
        {
            let columns = self.quality_intent_columns().len();
            let field = self
                .analysis_modal
                .quality
                .plan_field
                .min(columns.saturating_sub(1));
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    let discard = event.code == KeyCode::Esc;
                    self.leave_setup_editor(analysis_modal::SetupRow::Intent, discard);
                }
                KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                    self.analysis_modal.quality.plan_field = field;
                    self.open_intent_form();
                }
                _ => self.move_setup_editor(event, field, columns),
            }
            return ControlFlow::Break(None);
        }
        // The role editor owns the keys: the role, and its column.
        if self.analysis_modal.quality.page == QualityPage::TimeRoles
            && event.code != KeyCode::Char('?')
        {
            let field = self.analysis_modal.quality.plan_field;
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    let discard = event.code == KeyCode::Esc;
                    self.leave_setup_editor(analysis_modal::SetupRow::TimeRoles, discard);
                }
                KeyCode::Left | KeyCode::Char('h') | KeyCode::Right | KeyCode::Char('l') => {
                    let columns = self.quality_time_candidates();
                    self.analysis_modal.cycle_quality_time_role(
                        field,
                        &columns,
                        matches!(event.code, KeyCode::Right | KeyCode::Char('l')),
                    );
                }
                _ => {
                    let roles = crate::analysis::data_quality::TemporalRole::ALL.len();
                    self.move_setup_editor(event, field, roles);
                }
            }
            return ControlFlow::Break(None);
        }
        // The Expected editor owns the keys; `?` is help unless it types.
        if self.analysis_modal.quality.page == QualityPage::ExpectedWindows
            && (event.code != KeyCode::Char('?') || self.analysis_modal.quality_expected_typing())
        {
            self.expected_form_key(event);
            return ControlFlow::Break(None);
        }
        // The pairs editor owns the keys: which pair, and whether it is measured.
        if self.analysis_modal.quality.page == QualityPage::IntervalPairs
            && event.code != KeyCode::Char('?')
        {
            let pairs = self.analysis_modal.quality.plan.candidate_pairs();
            let field = self
                .analysis_modal
                .quality
                .plan_field
                .min(pairs.len().saturating_sub(1));
            match event.code {
                KeyCode::Esc | KeyCode::Enter => {
                    let discard = event.code == KeyCode::Esc;
                    self.leave_setup_editor(analysis_modal::SetupRow::Intervals, discard);
                }
                KeyCode::Char(' ')
                | KeyCode::Left
                | KeyCode::Char('h')
                | KeyCode::Right
                | KeyCode::Char('l') => {
                    if let Some(pair) = pairs.get(field) {
                        self.analysis_modal.quality.plan.toggle_interval(*pair);
                    }
                }
                _ => self.move_setup_editor(event, field, pairs.len()),
            }
            return ControlFlow::Break(None);
        }
        // Setup is edited in place: ↑↓ or Tab the row, ←→ a short list's choice, Space the
        // row's editor, Enter runs from any row, Esc discards staged edits.
        if self.analysis_modal.quality.page == QualityPage::Setup
            && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
            && !self.analysis_modal.quality.show_access
        {
            use analysis_modal::SetupRow;
            // Setup can hold the cursor without being opened (Esc to the tools then Tab back,
            // or after a failed run): edits are still a draft.
            if self.analysis_modal.quality.setup_before.is_none() {
                self.open_quality_setup();
            }
            let rows = SetupRow::ALL.len();
            let field = self.analysis_modal.quality.plan_field.min(rows - 1);
            match event.code {
                KeyCode::BackTab | KeyCode::Tab => {
                    let back = event.code == KeyCode::BackTab;
                    let step = if back { ListMove::Up } else { ListMove::Down };
                    self.analysis_modal.quality.plan_field = step.apply(field, rows, 1);
                    return ControlFlow::Break(None);
                }
                _ if ListMove::from_key(event).is_some() => {
                    self.move_setup_editor(event, field, rows);
                    return ControlFlow::Break(None);
                }
                KeyCode::Char(' ') => {
                    self.analysis_modal.quality.plan_field = field;
                    return ControlFlow::Break(self.open_setup_row());
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
                            self.analysis_modal.quality.setup_note = None;
                            self.analysis_modal
                                .cycle_setup_choice(row, &context, forward);
                        }
                        // A row whose value is a form or a list opens it.
                        _ if forward => return ControlFlow::Break(self.open_setup_row()),
                        _ => {}
                    }
                    return ControlFlow::Break(None);
                }
                KeyCode::Enter => return ControlFlow::Break(self.run_quality_setup(false)),
                KeyCode::Char('d') => {
                    self.release_quality_rows();
                    return ControlFlow::Break(None);
                }
                KeyCode::Esc => {
                    self.leave_quality_setup();
                    return ControlFlow::Break(None);
                }
                _ => {}
            }
        }

        match event.code {
            KeyCode::Esc if self.analysis_modal.quality.show_access => {
                self.analysis_modal.quality.show_access = false;
                return ControlFlow::Break(None);
            }
            // A staged read of a finding's rows: Enter reads, Esc returns having read nothing.
            KeyCode::Esc if self.analysis_modal.quality.evidence_read.is_some() => {
                self.analysis_modal.quality.evidence_read = None;
                return ControlFlow::Break(None);
            }
            KeyCode::Enter if self.analysis_modal.quality.evidence_read.is_some() => {
                return ControlFlow::Break(self.confirm_evidence_read());
            }
            KeyCode::Esc if self.analysis_modal.quality.observation_detail => {
                self.analysis_modal.quality.observation_detail = false;
                return ControlFlow::Break(None);
            }
            KeyCode::Enter if self.analysis_modal.quality.show_access => {
                self.analysis_modal.quality.show_access = false;
                return ControlFlow::Break(None);
            }
            KeyCode::Enter if self.analysis_modal.quality.observation_detail => {
                // The clean entry has no rows: Enter toggles every passed check versus the top few.
                if self.analysis_modal.quality_selected_is_clean() {
                    self.analysis_modal.quality.checks_expanded =
                        !self.analysis_modal.quality.checks_expanded;
                    self.analysis_modal.quality.detail_scroll.offset = 0;
                    return ControlFlow::Break(None);
                }
                let event = self.open_quality_evidence();
                // Rows on their way, or a read awaiting Enter, keep the finding open (Esc from the
                // rows returns to it); Enter on a finding with no rows closes it.
                if self.overlay == crate::Overlay::Analysis
                    && !self.error_modal.active
                    && self.analysis_modal.quality.evidence_read.is_none()
                    && self.analysis_modal.computing.is_none()
                {
                    self.analysis_modal.quality.observation_detail = false;
                }
                return ControlFlow::Break(event);
            }
            // A drill-in backs out to the list it came from.
            KeyCode::Esc if self.analysis_modal.quality.page == QualityPage::SegmentDetail => {
                self.analysis_modal.close_segment_detail();
                return ControlFlow::Break(None);
            }
            KeyCode::Esc if self.analysis_modal.quality.page == QualityPage::IntervalDetail => {
                self.analysis_modal.close_interval_detail();
                return ControlFlow::Break(None);
            }
            KeyCode::Esc
                if matches!(
                    self.analysis_modal.quality.page,
                    QualityPage::TrendDetail | QualityPage::Gaps
                ) =>
            {
                self.analysis_modal.close_to_trends();
                return ControlFlow::Break(None);
            }
            KeyCode::Esc if self.analysis_modal.quality.page == QualityPage::Detail => {
                self.analysis_modal
                    .set_quality_column_page(QualityPage::Columns);
                return ControlFlow::Break(None);
            }
            KeyCode::Char('p') => {
                self.analysis_modal.quality.show_access = !self.analysis_modal.quality.show_access;
                return ControlFlow::Break(None);
            }
            // Setup, with every setting, from anywhere in the report.
            KeyCode::Char('e') => {
                self.open_quality_setup();
                return ControlFlow::Break(None);
            }
            // The report's tabs; Setup is left with Enter or Esc, so no draft hides behind a
            // report page.
            KeyCode::Char(digit @ '1'..='5') if !self.analysis_modal.quality.page.is_setup() => {
                let tab = digit as usize - '1' as usize;
                self.analysis_modal.show_quality_tab(QualityPage::TABS[tab]);
                return ControlFlow::Break(None);
            }
            KeyCode::Char('m') if self.analysis_modal.quality.page == QualityPage::Trends => {
                self.analysis_modal.cycle_quality_metric();
                return ControlFlow::Break(None);
            }
            KeyCode::Char('m') if self.analysis_modal.quality.page == QualityPage::TrendDetail => {
                self.analysis_modal.cycle_trend_detail_metric();
                return ControlFlow::Break(None);
            }
            KeyCode::Char('w')
                if matches!(
                    self.analysis_modal.quality.page,
                    QualityPage::Trends | QualityPage::TrendDetail
                ) && self.analysis_modal.quality.results.is_some() =>
            {
                self.stage_coarser_grain();
                return ControlFlow::Break(None);
            }
            KeyCode::Char('g')
                if matches!(
                    self.analysis_modal.quality.page,
                    QualityPage::Trends | QualityPage::TrendDetail
                ) && self
                    .analysis_modal
                    .quality
                    .results
                    .as_ref()
                    .is_some_and(|results| {
                        crate::analysis::quality_trends::expected_gaps(
                            self.analysis_modal.quality_result_plan(),
                            results,
                        )
                        .is_some()
                    }) =>
            {
                self.analysis_modal.set_quality_page(QualityPage::Gaps);
                return ControlFlow::Break(None);
            }
            KeyCode::Char('b')
                if self.analysis_modal.quality.page == QualityPage::Segments
                    && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
            {
                // The segment under the cursor, looked up while the results still order the list.
                let selected = self.analysis_modal.selected_segment();
                if let Some(mut results) = self.analysis_modal.quality.results.take() {
                    if let Some(label) = selected
                        .and_then(|index| results.segments.get(index))
                        .map(|segment| segment.label.clone())
                    {
                        let mut plan = self.analysis_modal.quality_result_plan().clone();
                        plan.comparison =
                            crate::analysis::data_quality::QualityComparison::Baseline;
                        plan.baseline_segment = Some(label.clone());
                        results.compare_segments(&plan);
                        self.cache_quality_result(&results, plan.clone());
                        self.analysis_modal.quality.last_plan = Some(plan);
                        let working = &mut self.analysis_modal.quality.plan;
                        working.comparison =
                            crate::analysis::data_quality::QualityComparison::Baseline;
                        working.baseline_segment = Some(label);
                    }
                    self.analysis_modal.quality.results = Some(results);
                }
                return ControlFlow::Break(None);
            }
            KeyCode::Char('o') if self.analysis_modal.quality.page == QualityPage::Segments => {
                self.analysis_modal.toggle_segment_order();
                return ControlFlow::Break(None);
            }
            // Overview's findings narrowed and ordered from the report on screen; nothing is
            // remeasured.
            KeyCode::Char(key @ ('c' | 't' | 'o'))
                if self.analysis_modal.quality.page == QualityPage::Overview
                    && self.analysis_modal.quality.results.is_some()
                    && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
            {
                if key == 'o' {
                    self.analysis_modal.cycle_findings_order();
                } else {
                    self.analysis_modal.open_findings_picker(key == 'c');
                }
                return ControlFlow::Break(None);
            }
            // Esc shows every finding again before it leaves the page.
            KeyCode::Esc
                if self.analysis_modal.quality.page == QualityPage::Overview
                    && self.analysis_modal.quality.findings.narrowed()
                    && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
            {
                self.analysis_modal.clear_findings_narrowing();
                return ControlFlow::Break(None);
            }
            // Write the on-screen report: what was measured, never a draft; nothing read.
            KeyCode::Char('x')
                if !self.analysis_modal.quality.page.is_setup()
                    && self.analysis_modal.quality.results.is_some() =>
            {
                self.open_quality_export();
                return ControlFlow::Break(None);
            }
            // Another sample at once, a new seed for every tool: on a sampled report only,
            // never over a draft or beside a cancelled read.
            KeyCode::Char('r')
                if !self.analysis_modal.quality.page.is_setup()
                    && self.analysis_modal.quality.results.is_some()
                    && self.analysis_modal.quality.plan.compute
                        == data_quality::QualityCompute::Sample =>
            {
                if self.cancelled_analysis_running().is_some() {
                    self.flash_note(QUALITY_RUN_WAITS.to_string());
                    return ControlFlow::Break(None);
                }
                let before = self.analysis_modal.quality.plan.clone();
                self.analysis_modal.quality.plan.sample_seed = sample_modal::new_seed();
                let event = self.run_quality_setup(false);
                // Refused, reason on Setup's line: the plan stays the report's.
                if let Some(note) = self.analysis_modal.quality.setup_note.take() {
                    self.analysis_modal.quality.plan = before;
                    self.flash_note(note);
                }
                return ControlFlow::Break(event);
            }
            KeyCode::Enter if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main => {
                if let Some(setup) = self.quality_page_setup() {
                    // Straight to the setting that fills the page, in Setup.
                    self.open_quality_setup();
                    self.analysis_modal.quality.plan_field = match setup {
                        data_quality::QualitySetup::Grain => analysis_modal::SetupRow::Grain,
                        data_quality::QualitySetup::TimeRoles => {
                            analysis_modal::SetupRow::TimeRoles
                        }
                        data_quality::QualitySetup::Intervals => {
                            analysis_modal::SetupRow::Intervals
                        }
                    }
                    .index();
                    return ControlFlow::Break(self.open_setup_row());
                } else if self.analysis_modal.quality.page == QualityPage::Overview {
                    let findings = self.analysis_modal.quality_row_count();
                    self.analysis_modal.quality.checks_expanded = false;
                    self.analysis_modal.quality.detail_scroll =
                        analysis_modal::DetailScroll::default();
                    self.analysis_modal.quality.observation_detail = self
                        .analysis_modal
                        .quality
                        .table_state
                        .selected()
                        .is_some_and(|index| index < findings);
                } else if self.analysis_modal.quality.page == QualityPage::Columns {
                    self.analysis_modal
                        .set_quality_column_page(QualityPage::Detail);
                } else if self.analysis_modal.quality.page == QualityPage::Detail {
                    self.analysis_modal
                        .set_quality_column_page(QualityPage::Columns);
                } else if self.analysis_modal.quality.page == QualityPage::Segments
                    && self.analysis_modal.quality.results.is_some()
                {
                    self.analysis_modal.open_segment_detail();
                } else if self.analysis_modal.quality.page == QualityPage::SegmentDetail {
                    self.analysis_modal.close_segment_detail();
                } else if self.analysis_modal.quality.page == QualityPage::Trends
                    && self.analysis_modal.quality.results.is_some()
                {
                    self.analysis_modal.open_trend_detail();
                } else if matches!(
                    self.analysis_modal.quality.page,
                    QualityPage::TrendDetail | QualityPage::Gaps
                ) {
                    self.analysis_modal.close_to_trends();
                } else if self.analysis_modal.quality.page == QualityPage::Intervals {
                    self.analysis_modal.open_interval_detail();
                } else if self.analysis_modal.quality.page == QualityPage::IntervalDetail {
                    return ControlFlow::Break(self.open_interval_evidence());
                }
                return ControlFlow::Break(None);
            }
            KeyCode::Left | KeyCode::Char('h') => {
                self.analysis_modal.step_quality_tab(false);
                return ControlFlow::Break(None);
            }
            KeyCode::Right | KeyCode::Char('l') => {
                self.analysis_modal.step_quality_tab(true);
                return ControlFlow::Break(None);
            }
            _ => {}
        }
        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
            && let Some(step) = ListMove::from_key(event)
        {
            let rows = self.analysis_modal.quality_row_count();
            self.analysis_modal.move_row(step, rows);
            return ControlFlow::Break(None);
        }
        ControlFlow::Continue(())
    }

    /// A Setup editor's list key: its cursor moves over its `rows`, ten to a page.
    fn move_setup_editor(&mut self, event: &KeyEvent, field: usize, rows: usize) {
        if let Some(step) = ListMove::from_key(event) {
            self.analysis_modal.quality.plan_field = step.apply(field, rows, 10);
        }
    }

    /// Back from a Setup editor to its `row` of Setup; `discard` puts back the plan
    /// the editor opened on.
    fn leave_setup_editor(&mut self, row: analysis_modal::SetupRow, discard: bool) {
        let modal = &mut self.analysis_modal;
        if let Some(plan) = modal.quality.plan_before_edit.take()
            && discard
        {
            modal.quality.plan = plan;
        }
        modal.quality.setup_note = None;
        modal.set_quality_page(crate::analysis::data_quality::QualityPage::Setup);
        modal.quality.plan_field = row.index();
    }
}

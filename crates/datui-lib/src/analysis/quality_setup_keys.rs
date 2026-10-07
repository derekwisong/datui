//! Data Quality's Setup and its editors, the report's export dialog and the intent
//! form: each owns the keys while it is open.

use std::ops::ControlFlow;

use crate::app::form::ListMove;
use crate::{App, AppEvent, analysis::analysis_modal};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// A key for an open Setup row picker, form or editor: `Break` with its answer when
    /// one took it.
    pub(crate) fn quality_setup_key(&mut self, event: &KeyEvent) -> ControlFlow<Option<AppEvent>> {
        use crate::analysis::data_quality::QualityPage;

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
        ControlFlow::Continue(())
    }

    /// A Setup editor's list key: its cursor moves over its `rows`, ten to a page.
    pub(crate) fn move_setup_editor(&mut self, event: &KeyEvent, field: usize, rows: usize) {
        if let Some(step) = ListMove::from_key(event) {
            self.analysis_modal.quality.plan_field = step.apply(field, rows, 10);
        }
    }

    /// Back from a Setup editor to its `row` of Setup; `discard` puts back the plan
    /// the editor opened on.
    pub(crate) fn leave_setup_editor(&mut self, row: analysis_modal::SetupRow, discard: bool) {
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

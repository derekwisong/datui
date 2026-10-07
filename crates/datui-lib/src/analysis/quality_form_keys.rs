//! The Data Quality forms: the intent and expected-values forms, Setup's rows and
//! pickers, and the quality export.

use crate::feedback::Confirm;
use crate::form::FormKey;
use crate::output_file::Overwrite;
use crate::{
    App, AppEvent, analysis::analysis_modal, analysis::data_quality, analysis::intent_modal, form,
};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Open the intent form on the column under the cursor of the Column intent list.
    pub(crate) fn open_intent_form(&mut self) {
        let columns = self.quality_intent_columns();
        let modal = &mut self.analysis_modal;
        let Some((column, dtype)) = columns.get(modal.quality.plan_field) else {
            return;
        };
        let plan = &modal.quality.plan;
        modal.quality.intent_form = Some(intent_modal::IntentForm::new(
            column,
            dtype.clone(),
            plan.time_format(column).cloned(),
            &plan.intent,
            &self.theme,
        ));
    }

    /// Keys in the intent form: Tab and ↑↓ walk rows, Space and ←→ change a choice, text
    /// fields type, Enter stages the declaration in Setup's draft, Esc drops edits.
    /// Reads nothing.
    pub(crate) fn intent_form_key(&mut self, event: &KeyEvent) {
        let modal = &mut self.analysis_modal;
        let Some(form) = modal.quality.intent_form.as_mut() else {
            return;
        };
        match form::key(form, event) {
            FormKey::Cancel => modal.quality.intent_form = None,
            FormKey::Submit => match form.apply(&mut modal.quality.plan.intent) {
                Ok(()) => modal.quality.intent_form = None,
                Err(error) => form.error = Some(error),
            },
            FormKey::Act(_) => form.adjust(true),
            FormKey::Step(_, delta) => form.adjust(delta > 0),
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                }
                form.error = None;
            }
            FormKey::Moved | FormKey::Other => {}
        }
    }

    /// The export dialog, on a name made from the dataset's.
    pub(crate) fn open_quality_export(&mut self) {
        let stem = self
            .path
            .as_ref()
            .and_then(|path| path.file_stem())
            .and_then(|stem| stem.to_str())
            .filter(|stem| !stem.is_empty())
            .unwrap_or("data")
            .to_string();
        self.analysis_modal.quality.export = Some(
            crate::analysis::quality_export::ExportForm::new(&stem, &self.theme),
        );
    }

    /// Keys in the export dialog: Tab between path and form, arrows or Space change the
    /// form, Enter writes (asking over an existing file), Esc closes.
    pub(crate) fn quality_export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.quality.export.as_mut()?;
        match event.code {
            KeyCode::Esc => self.analysis_modal.quality.export = None,
            KeyCode::Tab | KeyCode::BackTab | KeyCode::Up | KeyCode::Down => form.toggle_focus(),
            KeyCode::Left
            | KeyCode::Right
            | KeyCode::Char(' ')
            | KeyCode::Char('h')
            | KeyCode::Char('l')
                if form.on_format =>
            {
                form.cycle_format();
            }
            KeyCode::Enter => match form.target() {
                Err(error) => form.error = Some(error),
                Ok((path, format)) => {
                    if path.exists() {
                        let shown = path.display().to_string();
                        self.confirmation_modal.show_destructive(
                            format!("File already exists:\n{shown}\n\nOverwrite it?"),
                            "Overwrite",
                            Confirm::QualityExport(path, format),
                        );
                    } else {
                        // The dialog stays up while writing: a failure shows on its status line.
                        return Some(AppEvent::QualityReportExport(
                            path,
                            format,
                            Overwrite::Forbid,
                        ));
                    }
                }
            },
            _ if !form.on_format => {
                let _ = form.path.handle_key(event, None);
                form.error = None;
            }
            _ => {}
        }
        None
    }

    /// Space on a Setup row: the Sample form, the role editor, or the row's choices.
    pub(crate) fn open_setup_row(&mut self) -> Option<AppEvent> {
        use analysis_modal::SetupRow;
        self.analysis_modal.quality.setup_note = None;
        match self.analysis_modal.setup_row() {
            SetupRow::Sample => self.open_quality_sample_form(),
            SetupRow::TimeRoles => {
                // With no date, time or text column there is no role to assign.
                if !self.quality_time_candidates().is_empty() {
                    self.analysis_modal.quality.plan_before_edit =
                        Some(self.analysis_modal.quality.plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::TimeRoles);
                    self.analysis_modal.quality.plan_field = 0;
                }
            }
            SetupRow::Intent => {
                if !self.quality_intent_columns().is_empty() {
                    self.analysis_modal.quality.plan_before_edit =
                        Some(self.analysis_modal.quality.plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::Intent);
                    self.analysis_modal.quality.plan_field = 0;
                }
            }
            SetupRow::Intervals => {
                // Two assigned roles make the first pair to choose.
                if !self
                    .analysis_modal
                    .quality
                    .plan
                    .candidate_pairs()
                    .is_empty()
                {
                    self.analysis_modal.quality.plan_before_edit =
                        Some(self.analysis_modal.quality.plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::IntervalPairs);
                    self.analysis_modal.quality.plan_field = 0;
                }
            }
            SetupRow::Expected => {
                // Gaps are counted in windows; without a time-window grain there is nothing to
                // expect.
                if matches!(
                    self.analysis_modal.quality.plan.grain,
                    data_quality::QualityGrain::TimeWindows { .. }
                ) {
                    self.analysis_modal.quality.expected_form =
                        Some(analysis_modal::ExpectedForm::new(
                            &self.analysis_modal.quality.plan,
                            &self.theme,
                        ));
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::ExpectedWindows);
                }
            }
            row => {
                let context = self.quality_plan_context();
                self.analysis_modal.open_plan_picker(row, &context);
            }
        }
        None
    }

    /// Keys in the Expected editor: ↑↓ the row, ←→ the cadence, typing in From and
    /// Before. Enter writes the draft or says why not; Esc keeps it. Either way back to
    /// Setup's Expected row.
    pub(crate) fn expected_form_key(&mut self, event: &KeyEvent) {
        let every = match &self.analysis_modal.quality.plan.grain {
            data_quality::QualityGrain::TimeWindows { every, .. } => every.clone(),
            _ => String::new(),
        };
        let Some(form) = self.analysis_modal.quality.expected_form.as_mut() else {
            return;
        };
        match form::key(form, event) {
            FormKey::Cancel => {}
            FormKey::Submit => match form.expected() {
                Ok(expected) => self.analysis_modal.quality.plan.expected = expected,
                Err(problem) => {
                    form.error = Some(problem);
                    return;
                }
            },
            FormKey::Step(_, delta) => {
                form.cycle(&every, delta > 0);
                return;
            }
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut() {
                    let _ = input.handle_key(event, None);
                    form.error = None;
                }
                return;
            }
            FormKey::Act(_) | FormKey::Moved | FormKey::Other => return,
        }
        self.analysis_modal.quality.expected_form = None;
        self.analysis_modal.quality.setup_note = None;
        self.analysis_modal
            .set_quality_page(data_quality::QualityPage::Setup);
        self.analysis_modal.quality.plan_field = analysis_modal::SetupRow::Expected.index();
    }

    /// `w` on Trends: stage the next coarser grain in Setup, cursor on Grain, for
    /// thinly sampled segments. Nothing runs until Enter; Setup's Read says what it
    /// reads; Esc reverts.
    pub(crate) fn stage_coarser_grain(&mut self) {
        let Some(coarser) = self.analysis_modal.quality_result_plan().coarser_grain() else {
            return;
        };
        self.open_quality_setup();
        let plan = &mut self.analysis_modal.quality.plan;
        plan.grain = coarser;
        plan.baseline_segment = None;
        self.analysis_modal.quality.plan_field = analysis_modal::SetupRow::Grain.index();
    }

    /// Enter in a Setup row's list: take the choice; after a text column, ask for its
    /// format beside sample values.
    pub(crate) fn choose_setup_picker(&mut self) {
        self.analysis_modal.quality.setup_note = None;
        if let Some(column) = self.analysis_modal.choose_plan_picker() {
            let examples = self
                .data_table_state
                .as_ref()
                .map(|state| state.buffered_values(&column, 3))
                .unwrap_or_default();
            self.analysis_modal.open_format_picker(&column, &examples);
        }
    }
}

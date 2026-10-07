//! `S` at the table: the sample form and the keys that act while a sample is drawn.

use crate::analysis::sample_modal::SampleForm;
use crate::analysis::table_sample::{Limit, MemoryCheck};
use crate::app::jobs::Job;
use crate::table::DataTableState;
use crate::{App, AppEvent, InputMode, Overlay, analysis::sampling, app::form};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::sync::Arc;

/// What Enter on a Sample form that edits the view's sample comes to.
pub(crate) enum Submitted {
    /// The form says why it cannot apply, or warns about memory, on its own line.
    Stays,
    /// No sample: take the view's away.
    Clear,
    /// Draw this sample; `anyway` past the memory warning, with no running check.
    Draw {
        sample: sampling::Sample,
        anyway: bool,
    },
}

impl App {
    /// `S` at the table: the Sample form, on the view's sample, or on a new one.
    pub(crate) fn open_table_sample_form(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let sample = match state.sampled() {
            Some(sampled) => sampled.sample().clone(),
            None => sampling::Sample {
                rows: match self.app_config.analysis.sample_rows {
                    0 => sampling::DEFAULT_SAMPLE_ROWS,
                    rows => rows,
                },
                seed: crate::analysis::sample_modal::new_seed(),
                ..sampling::Sample::default()
            },
        };
        let context = self.sample_context(state.unsampled());
        let mut form = SampleForm::new(&sample, context, &self.theme);
        form.view = true;
        form.bytes_per_row = Some(state.unsampled().sample_row_bytes(false));
        form.source_bytes_per_row = Some(state.unsampled().sample_row_bytes(true));
        self.sample.form = Some(form);
        self.open_overlay(Overlay::Sample);
    }

    /// Keys while the table's Sample form is open.
    pub(crate) fn table_sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.sample.form.as_ref()?;
        match self.sample_form_edit(event) {
            form::FormKey::Cancel => {
                self.close_overlay();
            }
            form::FormKey::Submit => match Self::submit_view_sample(
                self.sample.form.as_mut()?,
                MemoryCheck {
                    limit: Limit::of_setting(self.app_config.analysis.sample_memory_limit),
                    probe: Arc::clone(&self.sample.memory_probe),
                },
            ) {
                Submitted::Stays => {}
                Submitted::Clear => {
                    self.close_overlay();
                    self.clear_table_sample();
                }
                Submitted::Draw { sample, anyway } => {
                    self.close_overlay();
                    self.apply_table_sample(sample, None, anyway, false);
                }
            },
            _ => {}
        }
        None
    }

    /// A key on the table's Sample form, as the analysis form takes it.
    fn sample_form_edit(
        &mut self,
        event: &KeyEvent,
    ) -> form::FormKey<crate::analysis::sample_modal::SampleField> {
        let Some(form) = self.sample.form.as_mut() else {
            return form::FormKey::Other;
        };
        let file_count = form.context.files.len();
        let key = form::key(form, event);
        match &key {
            form::FormKey::Step(_, delta) => {
                form.adjust(*delta > 0);
                form.edited();
            }
            form::FormKey::Text(crate::analysis::sample_modal::SampleField::Files)
                if matches!(event.code, KeyCode::PageDown | KeyCode::PageUp) =>
            {
                form.file_offset = if event.code == KeyCode::PageDown {
                    (form.file_offset + crate::widgets::sample_form::FILES_SHOWN)
                        .min(file_count.saturating_sub(1))
                } else {
                    form.file_offset
                        .saturating_sub(crate::widgets::sample_form::FILES_SHOWN)
                };
            }
            form::FormKey::Text(_) => {
                if let Some(input) = form.input_mut(form.field) {
                    let _ = input.handle_key(event, None);
                }
                form.edited();
            }
            _ => {}
        }
        key
    }

    /// Enter on the view's sample form. An estimate past available memory warns on the
    /// form, naming the setting; a second Enter draws it anyway, once.
    pub(crate) fn submit_view_sample(form: &mut SampleForm, memory: MemoryCheck) -> Submitted {
        if form.no_sample() {
            return Submitted::Clear;
        }
        let sample = match form.finish() {
            Ok(sample) => sample,
            Err(error) => {
                form.error = Some(error);
                return Submitted::Stays;
            }
        };
        if form.anyway {
            return Submitted::Draw {
                sample,
                anyway: true,
            };
        }
        if let Some(warning) = form.estimate().and_then(|bytes| memory.refuses(bytes)) {
            form.error = Some(warning);
            form.anyway = true;
            return Submitted::Stays;
        }
        Submitted::Draw {
            sample,
            anyway: false,
        }
    }

    /// The key a draw from `scope` of `source` is remembered by: the scope and the view
    /// steps it reads through.
    pub(crate) fn sample_path_key(
        source: &DataTableState,
        scope: &crate::analysis::data_quality::QualityScope,
    ) -> String {
        let settings = crate::view::ViewSettings {
            sample: None,
            chart: None,
            ..crate::view_settings_of(source)
        };
        let steps = if scope.uses_source() {
            String::new()
        } else {
            serde_json::to_string(&settings).unwrap_or_default()
        };
        format!("{}\n{steps}", scope.command())
    }

    /// While a sample is drawn, keys reading only rows on hand act at once (moving,
    /// finding, inspecting, and every key of the find line, inspector, Info and help);
    /// anything needing every row (sort, query, Analysis, chart, export) waits.
    pub(crate) fn key_acts_while_sampling(&self, key: &KeyEvent) -> bool {
        if self.busy
            || self.loading.waits()
            || !self
                .jobs
                .keys_held_only_by(|job| matches!(job, Job::SampleDraw(_)))
        {
            return false;
        }
        match (&self.overlay, &self.input_mode) {
            (Overlay::Inspect | Overlay::Info, _) => return true,
            (Overlay::None, InputMode::Editing)
                if self.prompt.input_type == Some(crate::InputType::Find) =>
            {
                return true;
            }
            (Overlay::None, InputMode::Normal) if self.help.is_open() => return true,
            (Overlay::None, InputMode::Normal) if self.in_normal_table_view() => {}
            _ => return false,
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Up
            | KeyCode::Down
            | KeyCode::PageUp
            | KeyCode::PageDown
            | KeyCode::Home
            | KeyCode::End
            | KeyCode::Char(' ') => true,
            KeyCode::Char('f' | 'b' | 'd' | 'u') if ctrl => true,
            KeyCode::Char('j' | 'k' | 'G' | '/' | 'f' | 'n' | 'N' | 'g' | 'i') => !ctrl,
            KeyCode::Enter => self.enter_inspects(),
            _ => false,
        }
    }
}

//! `S` at the table: the sample form and the keys that act while a sample is drawn.

use crate::analysis::analysis_modal::AnalysisProgress;
use crate::analysis::sample_modal::SampleForm;
use crate::analysis::table_sample::{Limit, MemoryCheck};
use crate::form::FormKey;
use crate::jobs::Job;
use crate::table::DataTableState;
use crate::{
    App, AppEvent, InputMode, Overlay, analysis::analysis_modal, analysis::data_quality,
    analysis::sample_keys, analysis::sample_modal, analysis::sampling, form,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::DataType;
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

    /// Open the Sample form on a copy of the shared sample. A per-partition split
    /// offers partition columns first, then text, integers and dates; never floats.
    pub(crate) fn open_sample_form(&mut self) {
        self.open_sample_form_as(false);
    }

    /// A tool with nothing to show: the Sample form is its pane. The caller places the
    /// cursor (into the form when picked, back to the list on Esc).
    pub(crate) fn open_first_run_form(&mut self) {
        self.open_sample_form_as(true);
        self.sync_sample_form_focus();
    }

    /// The scope field shows its cursor only while the form has the cursor.
    pub(crate) fn sync_sample_form_focus(&mut self) {
        let focused = self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main;
        if let Some(form) = self.analysis_modal.sample_form.as_mut() {
            let has_cursor = focused || !form.inline;
            form.sync_focus(has_cursor);
        }
    }

    /// Run the tool on screen with the form's sample, or say why its scope does not
    /// parse. In Data Quality the sample is staged in Setup's draft; Run reads.
    pub(crate) fn run_sample_form(&mut self) -> Option<AppEvent> {
        let quality =
            self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        // The view's sample: it is drawn again, and the tool runs on it once it is.
        if self.analysis_modal.sample_form.as_ref()?.view {
            let memory = self.memory_check();
            let form = self.analysis_modal.sample_form.as_mut()?;
            return match Self::submit_view_sample(form, memory) {
                sample_keys::Submitted::Stays => None,
                sample_keys::Submitted::Clear => {
                    self.analysis_modal.sample_form = None;
                    self.clear_table_sample();
                    if quality {
                        None
                    } else {
                        self.start_analysis_run()
                    }
                }
                sample_keys::Submitted::Draw { sample, anyway } => {
                    self.analysis_modal.sample_form = None;
                    self.apply_table_sample(sample, None, anyway, !quality);
                    if !quality {
                        self.analysis_modal.computing =
                            Some(AnalysisProgress::new("Drawing the sample"));
                    }
                    None
                }
            };
        }
        let finished = self.analysis_modal.sample_form.as_mut()?.finish();
        match finished {
            Ok(sample) if quality => {
                self.analysis_modal.sample_form = None;
                self.analysis_modal.quality.plan.adopt_sample(&sample);
                self.analysis_modal.quality.setup_note = None;
                None
            }
            // The form stays open, as filled, while a cancelled run finishes.
            Ok(_) if self.read_waits_for_cancelled() => None,
            Ok(sample) => {
                self.analysis_modal.sample_form = None;
                self.apply_sample(sample)
            }
            Err(error) => {
                if let Some(form) = self.analysis_modal.sample_form.as_mut() {
                    form.error = Some(error);
                }
                None
            }
        }
    }

    fn open_sample_form_as(&mut self, inline: bool) {
        let sample = self.analysis_modal.sample.clone();
        self.open_sample_form_on(&sample, inline);
    }

    /// `s` in Data Quality: the Sample form over Setup on the draft's sample; Enter
    /// stages it and returns to Setup. Only Run reads.
    pub(crate) fn open_quality_sample_form(&mut self) {
        self.open_quality_setup();
        let sample = self.analysis_modal.quality.plan.sample();
        self.open_sample_form_on(&sample, false);
    }

    fn open_sample_form_on(&mut self, sample: &sampling::Sample, inline: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // A view with a sample: the form edits it; rows come from the view it was drawn
        // from.
        let (sample, view) = match state.sampled() {
            Some(sampled) => (sampled.sample().clone(), true),
            None => (sample.clone(), false),
        };
        let context = self.sample_context(state.unsampled());
        let mut form = sample_modal::SampleForm::new(&sample, context, &self.theme);
        form.inline = inline;
        form.view = view;
        form.bytes_per_row = Some(state.unsampled().sample_row_bytes(false));
        form.source_bytes_per_row = Some(state.unsampled().sample_row_bytes(true));
        self.analysis_modal.sample_form = Some(form);
        self.sync_sample_form_focus();
    }

    /// What the Sample form offers for `state`: partitions, files, time columns, and
    /// columns an equal-per-value sample can split by.
    pub(crate) fn sample_context(&self, state: &DataTableState) -> sample_modal::SampleContext {
        let mut partition_columns = state.partition_columns().unwrap_or_default().to_vec();
        let mut partition_values = Vec::new();
        // A directory whose files agree opens as one scan with no partition columns, but
        // its directory names still count: one branch is walked for the columns and one
        // listing read for the first column's values (local and cheap).
        if let Some(dir) = self.path.as_ref().filter(|path| path.is_dir()) {
            if partition_columns.is_empty() {
                partition_columns =
                    crate::formats::readers::hive::discover_hive_partition_columns(dir)
                        .into_iter()
                        .filter(|column| state.schema().get(column).is_some())
                        .collect();
            }
            if let Some(first) = partition_columns.first() {
                let prefix = format!("{first}=");
                let mut values: Vec<String> = std::fs::read_dir(dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        let name = entry.file_name().to_string_lossy().to_string();
                        name.strip_prefix(&prefix).map(str::to_string)
                    })
                    .collect();
                values.sort();
                if !values.is_empty() {
                    partition_values.push((first.clone(), values));
                }
            }
        }
        // An equal-per-value split column: partition columns, then text (ticker, region),
        // then dates and integers. Never floats.
        let mut value_columns = partition_columns.clone();
        for kind in 0..3 {
            for (name, dtype) in state.schema().iter() {
                let rank = match dtype {
                    DataType::String | DataType::Categorical(..) | DataType::Boolean => 0,
                    DataType::Date => 1,
                    dtype if dtype.is_integer() => 2,
                    _ => continue,
                };
                if rank == kind && !value_columns.iter().any(|column| column == name.as_str()) {
                    value_columns.push(name.to_string());
                }
            }
        }
        sample_modal::SampleContext {
            view_rows: state.num_rows_if_valid(),
            filtered: state.changes_rows(),
            files: state.quality_source_file_names().to_vec(),
            partition_columns,
            partition_values,
            time_columns: state.quality_temporal_columns(&data_quality::QualityScope::WholeSource),
            value_columns,
        }
    }

    pub(crate) fn sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.sample_form.as_mut()?;
        let file_count = form.context.files.len();
        let key = form::key(form, event);
        match key {
            // In a tool's empty pane the form stays: Esc discards the edit and returns the
            // cursor to the tool list.
            FormKey::Cancel if form.inline => {
                self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
                self.open_first_run_form();
            }
            FormKey::Cancel => self.analysis_modal.sample_form = None,
            FormKey::Submit => return self.run_sample_form(),
            FormKey::Step(_, delta) => {
                form.adjust(delta > 0);
                form.edited();
            }
            FormKey::Text(sample_modal::SampleField::Files)
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
            FormKey::Text(_) => {
                if let Some(input) = form.input_mut(form.field) {
                    let _ = input.handle_key(event, None);
                }
                form.edited();
            }
            FormKey::Act(_) | FormKey::Moved | FormKey::Other => {}
        }
        None
    }
}

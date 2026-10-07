//! The Analysis modal's Sample form: opening it on a tool's sample, its keys, and
//! running the tool on what it draws.

use crate::analysis::analysis_modal::AnalysisProgress;
use crate::app::form::FormKey;
use crate::table::DataTableState;
use crate::{
    App, AppEvent, analysis::analysis_modal, analysis::data_quality, analysis::sample_keys,
    analysis::sample_modal, analysis::sampling, app::form,
};
use crossterm::event::{KeyCode, KeyEvent};
use polars::datatypes::DataType;

impl App {
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

    pub(crate) fn open_sample_form_as(&mut self, inline: bool) {
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

    pub(crate) fn open_sample_form_on(&mut self, sample: &sampling::Sample, inline: bool) {
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

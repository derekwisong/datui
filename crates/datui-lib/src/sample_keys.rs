//! The view's sample: `S` at the table, the draw that fills it as the table shows
//! it, and taking it away.
//!
//! The sample is a step of the view, between the source and the query
//! ([`crate::widgets::datatable::Sampled`]). Its rows are drawn off the UI thread by a
//! [`Job::SampleDraw`], which keeps them in memory a chunk at a time
//! ([`crate::table_sample`]); each chunk is laid under the view's frames as it lands,
//! as a followed pipe's rows are.

use crate::jobs::{Answer, Job, SampleDraw};
use crate::sample_modal::SampleForm;
use crate::table_sample::{Limit, MemoryCheck, MemoryProbe};
use crate::widgets::datatable::DataTableState;
use crate::{App, AppEvent, InputMode, form, sampling};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::sync::Arc;

/// What the status line says while a sample is drawn.
const DRAWING: &str = "Sampling...";

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
                seed: crate::sample_modal::new_seed(),
                ..sampling::Sample::default()
            },
        };
        let context = self.sample_context(state.unsampled());
        let mut form = SampleForm::new(&sample, context, &self.theme);
        form.view = true;
        form.bytes_per_row = Some(state.unsampled().estimated_row_bytes());
        self.sample_form = Some(form);
        self.input_mode = InputMode::Sample;
    }

    /// Keys while the table's Sample form is open.
    pub(crate) fn table_sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.sample_form.as_ref()?;
        match self.sample_form_edit(event) {
            form::FormKey::Cancel => {
                self.sample_form = None;
                self.input_mode = InputMode::Normal;
            }
            form::FormKey::Submit => match Self::submit_view_sample(
                self.sample_form.as_mut()?,
                MemoryCheck {
                    limit: Limit::of_setting(self.app_config.analysis.sample_memory_limit),
                    probe: Arc::clone(&self.memory_probe),
                },
            ) {
                Submitted::Stays => {}
                Submitted::Clear => {
                    self.sample_form = None;
                    self.input_mode = InputMode::Normal;
                    self.clear_table_sample();
                }
                Submitted::Draw { sample, anyway } => {
                    self.sample_form = None;
                    self.input_mode = InputMode::Normal;
                    self.apply_table_sample(sample, None, anyway, false);
                }
            },
            _ => {}
        }
        None
    }

    /// A key on the table's Sample form, as the analysis form takes it: the field's
    /// own keys, or what the form does next.
    fn sample_form_edit(
        &mut self,
        event: &KeyEvent,
    ) -> form::FormKey<crate::sample_modal::SampleField> {
        let Some(form) = self.sample_form.as_mut() else {
            return form::FormKey::Other;
        };
        let file_count = form.context.files.len();
        let key = form::key(form, event);
        match &key {
            form::FormKey::Step(_, delta) => {
                form.adjust(*delta > 0);
                form.edited();
            }
            form::FormKey::Text(crate::sample_modal::SampleField::Files)
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

    /// Enter on a form that edits the view's sample. A sample whose estimate is more
    /// than the memory there is to hold it warns on the form, naming the setting; a
    /// second Enter draws it anyway, once.
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

    /// The memory check a sample is drawn under: `analysis.sample_memory_limit`
    /// against what is held, or the memory available now.
    pub(crate) fn memory_check(&self) -> MemoryCheck {
        MemoryCheck {
            limit: Limit::of_setting(self.app_config.analysis.sample_memory_limit),
            probe: Arc::clone(&self.memory_probe),
        }
    }

    /// Read the memory available now from `probe` rather than from the system: for a
    /// test that decides how much there is.
    pub fn set_memory_probe(&mut self, probe: MemoryProbe) {
        self.memory_probe = probe;
    }

    /// Draw `sample` as the view's sample, in place of any it has. The table shows
    /// the rows as they land, with `replay` (the query, filters, sort and columns of
    /// a view being applied) laid over them; with none, the view's own steps go back
    /// on, unless the sample was drawn through them. `anyway` draws with no running
    /// memory check. `then_analyze` runs Analysis's tool once it is drawn.
    pub(crate) fn apply_table_sample(
        &mut self,
        sample: sampling::Sample,
        replay: Option<crate::view::ViewSettings>,
        anyway: bool,
        then_analyze: bool,
    ) {
        self.draw_table_sample(sample, None, replay, anyway, then_analyze);
    }

    /// [`Self::apply_table_sample`], drawing row by row from `of` rows when a view
    /// says its sample was drawn that way: the same rows again.
    pub(crate) fn draw_table_sample(
        &mut self,
        sample: sampling::Sample,
        of: Option<usize>,
        replay: Option<crate::view::ViewSettings>,
        anyway: bool,
        then_analyze: bool,
    ) {
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let source = state.unsampled();
        // A view scope reads the view as shown: what it shows other than the source
        // is what the sample stands for, so it is not laid on again.
        let through = !sample.scope.uses_source() && source.changes_rows()
            || !source.column_changes().is_empty() && !sample.scope.uses_source();
        let replay = replay.or_else(|| (!through).then(|| crate::view_settings_of(source)));
        let (cut, known_total) = Self::table_sample_source(source, &sample.scope);
        let known_total = of.or(known_total);
        let bytes_per_row = Some(source.estimated_row_bytes());
        let streaming = self.app_config.performance.streaming;
        let rows = Arc::new(crate::table_sample::SampleRows::default());
        let watch = sampling::ReadWatch::default();
        let memory = if anyway {
            MemoryCheck::off()
        } else {
            self.memory_check()
        };
        let job = Job::SampleDraw(Box::new(SampleDraw {
            sample: sample.clone(),
            rows: Arc::clone(&rows),
            watch: watch.clone(),
            through,
            replay,
            then_analyze,
        }));
        self.spawn_job(job, Some(DRAWING), move |worker| {
            let report = worker.reporter();
            let failed = |error: color_eyre::eyre::Report| {
                crate::error_display::user_message_from_report(&error, None)
            };
            let lf = cut.cut(&sample.scope).map_err(failed)?;
            let schema = lf.clone().collect_schema().map_err(|e| failed(e.into()))?;
            report(crate::Progress::SampleBegun(schema));
            let live = crate::table_sample::Live {
                rows,
                notify: Arc::new(move || report(crate::Progress::SampleGrew)),
                memory,
                watch,
                bytes_per_row,
            };
            let drawn = crate::table_sample::draw(&lf, &sample, known_total, streaming, &live)
                .map_err(failed)?;
            Ok(Answer::SampleDrawn(drawn))
        });
    }

    /// Where a view's sample is drawn from: the loaded source for a source scope,
    /// otherwise the view as shown (in its order for a row range), with every column
    /// the view has, whole: the sample is the view's rows, not an analysis's.
    fn table_sample_source(
        state: &DataTableState,
        scope: &crate::data_quality::QualityScope,
    ) -> (sampling::SampleSource, Option<usize>) {
        use crate::data_quality::QualityScope;
        if scope.uses_source() {
            let (lf, source) = state.data_quality_source_scan();
            return (sampling::SampleSource::loaded(lf, source), None);
        }
        let lf = match scope {
            QualityScope::FirstRows(_) | QualityScope::ViewRows { .. } => state.lf().clone(),
            _ => state.analysis_lf(),
        };
        let columns: Vec<polars::prelude::Expr> = state
            .schema()
            .iter_names()
            .map(|name| polars::prelude::col(name.clone()))
            .collect();
        (
            sampling::SampleSource::view(lf.select(columns)),
            sampling::view_scope_rows(state.num_rows_if_valid(), scope),
        )
    }

    /// The view's sample goes: the view it was drawn from comes back, with the
    /// query, filters and sort laid on the sample laid on it instead, unless the
    /// sample was drawn through the view's own.
    pub(crate) fn clear_table_sample(&mut self) {
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.take() else {
            return;
        };
        let Some(sampled) = state.sampled() else {
            self.data_table_state = Some(state);
            return;
        };
        let through = sampled.through();
        let settings = crate::view_settings_of(&state);
        let mut source = state.into_unsampled();
        if !through {
            source.deferred(|s| {
                s.reset_view_for_replay();
                if let Err(error) = Self::replay_view(s, &settings, None) {
                    log::warn!(target: "datui", "the view's steps did not go back on the source: {error}");
                }
            });
        }
        self.data_table_state = Some(source);
        self.sample_changed();
        self.flash_note("Sample cleared".to_string());
        self.spawn_async_collect(Self::LOADING_BUFFER);
    }

    /// The view's rows changed underneath every result read from them: Analysis's,
    /// the chart's and the rows a page read held.
    pub(crate) fn sample_changed(&mut self) {
        self.forget_the_rows_read();
        self.chart_cache.clear();
        let modal = &mut self.analysis_modal;
        modal.describe_results = None;
        modal.distribution_results = None;
        modal.correlation_results = None;
        modal.data_quality_results = None;
        modal.data_quality_last_plan = None;
        let sampled = self
            .data_table_state
            .as_ref()
            .is_some_and(|state| state.sampled().is_some());
        self.analysis_modal.follow_view_sample(sampled);
        self.sync_quality_plan();
    }

    /// The draw in flight, if one is: its record.
    pub(crate) fn sample_draw(&self) -> Option<&SampleDraw> {
        match self.jobs.current(|job| matches!(job, Job::SampleDraw(_))) {
            Some((_, Job::SampleDraw(draw))) => Some(draw),
            _ => None,
        }
    }

    /// Whether the view's sample is being drawn.
    pub fn sample_drawing(&self) -> bool {
        self.sample_draw().is_some()
    }

    /// Esc while the sample is drawn: it stops, and the rows so far stay.
    pub(crate) fn stop_sample_draw(&mut self) {
        if let Some(draw) = self.sample_draw() {
            draw.watch.stop();
        }
    }

    /// Stop the draw in flight and drop whatever it still sends: another sample
    /// replaces it, or the view it was for goes.
    pub(crate) fn put_down_sample_draw(&mut self) {
        if let Some(draw) = self.sample_draw() {
            draw.watch.stop();
        }
        self.jobs.supersede(|job| matches!(job, Job::SampleDraw(_)));
    }

    /// Whether the view on screen is the one the draw in flight fills.
    fn draw_fills_view(&self, draw: &SampleDraw) -> bool {
        self.data_table_state
            .as_ref()
            .and_then(|state| state.sampled())
            .is_some_and(|sampled| sampled.holds(&draw.rows))
    }

    /// The draw has cut its rows to their scope: the view becomes the sample's,
    /// empty, and its rows arrive into it.
    pub(crate) fn sample_begun(&mut self, schema: &polars::prelude::Schema) {
        let Some(draw) = self.sample_draw().cloned() else {
            return;
        };
        let Some(state) = self.data_table_state.take() else {
            return;
        };
        let source = state.into_unsampled();
        let mut view = match DataTableState::sampled_from(
            source,
            draw.sample.clone(),
            schema,
            Arc::clone(&draw.rows),
            draw.through,
        ) {
            Ok(view) => view,
            Err(error) => {
                // The view it was drawn from stays.
                self.put_down_sample_draw();
                self.error_modal
                    .show(format!("Cannot show the sample: {error}"));
                return;
            }
        };
        if let Some(settings) = &draw.replay
            && let Err(error) = view.deferred(|s| Self::replay_view(s, settings, None))
        {
            self.flash_note(format!(
                "The view's steps did not apply to the sample: {error}"
            ));
        }
        self.data_table_state = Some(view);
        self.sample_changed();
        self.spawn_collect(None);
    }

    /// The draw kept another chunk: the view reads it, staying where it is.
    pub(crate) fn sample_grew(&mut self) {
        let Some(draw) = self.sample_draw().cloned() else {
            return;
        };
        if !self.draw_fills_view(&draw) {
            return;
        }
        let grew = self
            .data_table_state
            .as_mut()
            .is_some_and(DataTableState::sample_grew);
        // The page is read again when nothing else is reading it; the next chunk, or
        // the end, reads it otherwise.
        if grew && self.rows_in_flight().is_none() && self.in_normal_table_view() {
            self.spawn_collect(None);
        }
    }

    /// The draw ended: the rows go into source order, and a run Analysis waits for
    /// starts. A draw memory stopped says why.
    pub(crate) fn sample_drawn(
        &mut self,
        job: Job,
        current: bool,
        drawn: crate::table_sample::Drawn,
    ) -> Option<AppEvent> {
        let Job::SampleDraw(draw) = job else {
            return None;
        };
        if !current || !self.draw_fills_view(&draw) {
            return None;
        }
        if let Some(state) = self.data_table_state.as_mut() {
            state.sample_drawn(drawn);
        }
        self.forget_the_rows_read();
        if let Some(reason) = draw.rows.stopped() {
            self.flash_note(reason);
        }
        self.spawn_collect(None);
        if draw.then_analyze && self.analysis_modal.active {
            self.analysis_modal.computing = None;
            return self.start_analysis_run();
        }
        None
    }

    /// The draw failed, or was stopped before it kept a row: the view it was drawn
    /// from comes back.
    pub(crate) fn sample_draw_failed(&mut self, job: &Job, current: bool, message: &str) {
        let Job::SampleDraw(draw) = job else {
            return;
        };
        if !current {
            return;
        }
        if self.draw_fills_view(draw)
            && let Some(state) = self.data_table_state.take()
        {
            self.data_table_state = Some(state.into_unsampled());
            self.sample_changed();
            self.spawn_async_collect(Self::LOADING_BUFFER);
        }
        if draw.then_analyze {
            self.analysis_modal.computing = None;
        }
        if message == sampling::CANCELLED {
            self.flash_note("Sample stopped".to_string());
        } else {
            self.error_modal.show(message.to_string());
        }
    }

    /// While a sample is drawn, the keys that read only the rows on hand act at
    /// once: moving, finding, inspecting, and every key of the find line, the
    /// inspector, the Info panel and the help. Anything that needs every row
    /// (a sort, a query, Analysis, a chart, an export) waits for the sample.
    pub(crate) fn key_acts_while_sampling(&self, key: &KeyEvent) -> bool {
        if self.busy
            || self.loading.waits()
            || !self
                .jobs
                .keys_held_only_by(|job| matches!(job, Job::SampleDraw(_)))
        {
            return false;
        }
        match self.input_mode {
            InputMode::Inspect | InputMode::Info => return true,
            InputMode::Editing if self.input_type == Some(crate::InputType::Find) => return true,
            InputMode::Normal if self.help.is_open() => return true,
            InputMode::Normal if self.in_normal_table_view() => {}
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

    /// What a chart says under its plot about the rows it drew. A view with a sample
    /// is charted from all of it: the note says which sample, with its seed, so the
    /// chart can be drawn again.
    pub(crate) fn chart_notes_of(
        &self,
        prepared: &crate::chart_jobs::ChartPrepared,
        middot: &str,
    ) -> Vec<String> {
        let mut notes = prepared.notes(middot);
        let sampled = self.data_table_state.as_ref().and_then(|s| s.sampled());
        if let Some(sampled) = sampled {
            let mut note = sampled.label();
            if matches!(
                sampled.sample().method,
                sampling::SampleMethod::Spread | sampling::SampleMethod::PerPartition { .. }
            ) {
                note.push_str(&format!(" {middot} seed {}", sampled.sample().seed));
            }
            notes.insert(0, note);
        }
        notes
    }

    /// Apply `view`, whose rows are a sample: the view goes back to its source, the
    /// query and filters the sample was drawn through go on, and the sample is drawn
    /// again from its seed. The view's own steps go on the sample as it arrives.
    pub(crate) fn apply_sampled_view(
        &mut self,
        view: &crate::view::SavedView,
        saved: &crate::view::SavedSample,
        why: Option<crate::view::MatchReason>,
    ) -> color_eyre::Result<()> {
        let sample = saved.sample()?;
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.take() else {
            return Ok(());
        };
        let mut source = state.into_unsampled();
        let through = saved.through.clone().unwrap_or_default();
        let replayed = source.try_transition(|s| {
            s.reset_view_for_replay();
            Self::replay_query(
                s,
                through.sql_query.as_deref(),
                through.query.as_deref(),
                through.fuzzy_query.as_deref(),
            )?;
            Self::replay_filters_and_sort(
                s,
                &through.filters,
                &through.sort_columns,
                through.sort_directions(),
            )
        });
        self.data_table_state = Some(source);
        replayed?;
        self.sample_changed();
        if let Some(path) = &self.path {
            use crate::logging::LogFailure;
            self.view_manager
                .record_use(&view.id, path)
                .or_log("record a view's use");
        }
        self.active_view_id = Some(view.id.clone());
        let mut settings = view.settings.clone();
        settings.sample = None;
        self.draw_table_sample(sample, saved.of, Some(settings), false, false);
        if let Some(why) = why {
            self.flash_view_applied(&view.name, why);
        }
        self.first_rows_settled();
        Ok(())
    }
}

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
        form.bytes_per_row = Some(state.unsampled().sample_row_bytes(false));
        form.source_bytes_per_row = Some(state.unsampled().sample_row_bytes(true));
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

    /// [`Self::apply_table_sample`], drawing a random sample of a stream the way
    /// `path` says, as a view says it was drawn: the same rows again.
    pub(crate) fn draw_table_sample(
        &mut self,
        sample: sampling::Sample,
        path: Option<crate::table_sample::DrawPath>,
        replay: Option<crate::view::ViewSettings>,
        anyway: bool,
        then_analyze: bool,
    ) {
        use crate::data_quality::QualityScope;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let source = state.unsampled();
        // A view scope reads the view as shown: what it shows other than the source
        // is what the sample stands for, so it is not laid on again. Its order is
        // part of which rows a row range reads.
        let reads_view = !sample.scope.uses_source();
        let ranged = matches!(
            sample.scope,
            QualityScope::FirstRows(_) | QualityScope::ViewRows { .. }
        );
        let sorted = !source.get_sort_columns().is_empty() || !source.get_sort_ascending();
        let through = reads_view
            && (source.changes_rows() || !source.column_changes().is_empty() || ranged && sorted);
        // The steps to lay on the new sample: a view's being applied; those on the
        // sample it replaces; or the view's own, unless the sample stands for them.
        let replay = replay.or_else(|| match state.sampled() {
            Some(_) => Some(crate::view_settings_of(state)),
            None => (!through).then(|| crate::view_settings_of(source)),
        });
        // A pivot is read whole once its rows are all there, which a sample being
        // drawn is not: say so rather than leave it off.
        if replay
            .as_ref()
            .is_some_and(|settings| settings.pivot.is_some())
        {
            self.error_modal.show(PIVOT_OVER_A_SAMPLE.to_string());
            return;
        }
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let source = state.unsampled();
        let (cut, known_total) = Self::table_sample_source(source, &sample.scope);
        // How a random sample of a stream is drawn is decided once for what it is
        // drawn from: the view's word, the way it was drawn here before, or by
        // whether the count is in. The same seed keeps the same rows either way.
        let path_key = Self::sample_path_key(source, &sample.scope);
        let path = (sample.method == sampling::SampleMethod::Spread).then(|| {
            path.or_else(|| {
                self.sample_paths
                    .iter()
                    .find(|(key, _)| *key == path_key)
                    .map(|(_, path)| *path)
            })
            .unwrap_or(match known_total {
                Some(of) => crate::table_sample::DrawPath::Bernoulli { of },
                None => crate::table_sample::DrawPath::Reservoir,
            })
        });
        let bytes_per_row = Some(source.sample_row_bytes(sample.scope.uses_source()));
        let streaming = self.app_config.performance.streaming;
        let rows = Arc::new(crate::table_sample::SampleRows::default());
        let (memory, watch) = if anyway {
            (MemoryCheck::off(), sampling::ReadWatch::default())
        } else {
            let memory = self.memory_check();
            // A sampler that keeps its rows to the end is checked as it holds them.
            let held = memory.clone();
            let watch = sampling::ReadWatch::judging_held(Arc::new(move |bytes, rows| {
                held.holds_too_much(bytes, rows)
            }));
            (memory, watch)
        };
        let job = Job::SampleDraw(Box::new(SampleDraw {
            sample: sample.clone(),
            rows: Arc::clone(&rows),
            watch: watch.clone(),
            through,
            replay,
            then_analyze,
            path,
            path_key,
            schema: None,
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
            let drawn =
                crate::table_sample::draw(&lf, &sample, known_total, path, streaming, &live)
                    .map_err(failed)?;
            Ok(Answer::SampleDrawn(drawn))
        });
    }

    /// What a draw from `scope` of `source` is remembered by: the scope, and the
    /// view's steps it reads through.
    fn sample_path_key(
        source: &DataTableState,
        scope: &crate::data_quality::QualityScope,
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
        let Some(sampled) = self.data_table_state.as_ref().and_then(|s| s.sampled()) else {
            return;
        };
        let through = sampled.through();
        let settings = self.data_table_state.as_ref().map(crate::view_settings_of);
        // A pivot over the sample would have to read the whole source to move onto
        // it: the sample stays, and the way out is said.
        if !through && settings.as_ref().is_some_and(|s| s.pivot.is_some()) {
            self.error_modal.show(PIVOT_OFF_A_SAMPLE.to_string());
            return;
        }
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.take() else {
            return;
        };
        let mut source = state.into_unsampled();
        if !through && let Some(settings) = settings {
            let laid = source.deferred(|s| {
                s.reset_view_for_replay();
                Self::replay_view(s, &settings, None).map(|_| ())
            });
            if let Err(error) = laid {
                self.error_modal.show(format!(
                    "The view's steps did not go back on the source: {error}"
                ));
            }
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

    /// The draw has cut its rows to their scope: what their columns are is kept
    /// for the view that takes its first rows.
    pub(crate) fn sample_begun(&mut self, schema: &polars::prelude::SchemaRef) {
        if let Some(Job::SampleDraw(draw)) = self
            .jobs
            .current_mut(|job| matches!(job, Job::SampleDraw(_)))
        {
            draw.schema = Some(schema.clone());
        }
    }

    /// The view becomes the sample's, with the steps laid on it, in place of the
    /// view it is drawn from or the sample it replaces. Only once rows have come:
    /// a draw that fails or stops first leaves the view as it was. Returns whether
    /// the view is the draw's.
    fn take_on_sample(&mut self, draw: &SampleDraw) -> bool {
        if self.draw_fills_view(draw) {
            return true;
        }
        let Some(schema) = draw.schema.as_ref() else {
            return false;
        };
        let Some(state) = self.data_table_state.take() else {
            return false;
        };
        let source = state.into_unsampled();
        let mut view = match DataTableState::sampled_from(
            source,
            draw.sample.clone(),
            schema,
            Arc::clone(&draw.rows),
            draw.through,
            draw.path,
        ) {
            Ok(view) => view,
            Err(error) => {
                self.put_down_sample_draw();
                self.error_modal
                    .show(format!("Cannot show the sample: {error}"));
                return false;
            }
        };
        if let Some(settings) = &draw.replay {
            let laid = view.deferred(|s| Self::replay_view(s, settings, None));
            match laid {
                Ok(crate::Replayed::Planned) => {}
                // Refused before the draw started; never left off silently.
                Ok(crate::Replayed::Pivot(_)) => {
                    self.error_modal.show(PIVOT_OVER_A_SAMPLE.to_string());
                }
                Err(error) => self.flash_note(format!(
                    "The view's steps did not apply to the sample: {error}"
                )),
            }
        }
        self.data_table_state = Some(view);
        self.sample_changed();
        true
    }

    /// The draw kept another chunk: the view reads it, staying where it is.
    pub(crate) fn sample_grew(&mut self) {
        let Some(draw) = self.sample_draw().cloned() else {
            return;
        };
        if !self.take_on_sample(&draw) {
            return;
        }
        let grew = self
            .data_table_state
            .as_mut()
            .and_then(DataTableState::sample_grew);
        // Rows read through the frame before it grew are of fewer rows, in another
        // order: nothing may land them now.
        if grew == Some(false) {
            self.forget_the_rows_read();
        }
        // The page is read again when nothing else is reading it; the next chunk, or
        // the end, reads it otherwise.
        if grew.is_some() && self.rows_in_flight().is_none() && self.in_normal_table_view() {
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
        if !current || !self.take_on_sample(&draw) {
            return None;
        }
        if let Some(path) = drawn.path {
            self.sample_paths.retain(|(key, _)| *key != draw.path_key);
            self.sample_paths.push((draw.path_key.clone(), path));
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

    /// The draw failed, or was stopped before it kept a row. The view stays as it
    /// was when no row had come; rows that had stay, as a sample cut short.
    pub(crate) fn sample_draw_failed(&mut self, job: &Job, current: bool, message: &str) {
        let Job::SampleDraw(draw) = job else {
            return;
        };
        if !current {
            return;
        }
        if self.draw_fills_view(draw)
            && let Some(state) = self.data_table_state.as_mut()
        {
            state.sample_drawn(crate::table_sample::Drawn {
                cut: true,
                path: draw.path,
                ..Default::default()
            });
            self.forget_the_rows_read();
            self.spawn_collect(None);
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
    /// view the sample was drawn through goes on, and the sample is drawn again from
    /// its seed, the way it was. The view's own steps go on the sample as it arrives.
    pub(crate) fn apply_sampled_view(
        &mut self,
        view: &crate::view::SavedView,
        saved: &crate::view::SavedSample,
        why: Option<crate::view::MatchReason>,
    ) -> color_eyre::Result<()> {
        let sample = saved.sample()?;
        let through = saved.through.as_deref();
        if through.is_some_and(|through| through.pivot.is_some()) || view.settings.pivot.is_some() {
            return Err(color_eyre::eyre::eyre!("{PIVOT_OVER_A_SAMPLE}"));
        }
        self.put_down_sample_draw();
        let Some(state) = self.data_table_state.take() else {
            return Ok(());
        };
        let mut source = state.into_unsampled();
        let replayed = source.try_transition(|s| {
            s.reset_view_for_replay();
            match through {
                Some(through) => Self::replay_view(s, through, None).map(|_| ()),
                None => Ok(()),
            }
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
        self.restore_view_chart(view.settings.chart.as_ref());
        let mut settings = view.settings.clone();
        settings.sample = None;
        self.draw_table_sample(sample, saved.path, Some(settings), false, false);
        if let Some(why) = why {
            self.flash_view_applied(&view.name, why);
        }
        self.first_rows_settled();
        Ok(())
    }
}

/// Why a sample is not drawn under a pivot.
const PIVOT_OVER_A_SAMPLE: &str = "A pivot cannot be laid on a sample as it is drawn: sample \
     the pivoted view (Rows from: All rows), or take the pivot away with R";

/// Why the sample under a pivot stays.
const PIVOT_OFF_A_SAMPLE: &str = "The pivot on the sample cannot move to the source: R takes \
     away both";

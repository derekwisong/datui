//! The view's sample: the draw that fills it as the table shows it, and taking it
//! away. The sample is a view step between source and query
//! ([`crate::table::Sampled`]); a [`Job::SampleDraw`] draws it off the UI thread a
//! chunk at a time ([`crate::analysis::table_sample`]), each chunk laid under the view's
//! frames as it lands, like a followed pipe's rows.

use crate::analysis::table_sample::{Limit, MemoryCheck, MemoryProbe};
use crate::app::jobs::{Answer, Job, SampleDraw};
use crate::table::DataTableState;
use crate::{App, AppEvent, analysis::analysis_modal, analysis::data_quality, analysis::sampling};
use std::sync::Arc;

/// The sample form, and what the draws learned of memory and of the paths they took.
pub struct SampleState {
    /// The Sample form over the table (`S`).
    pub form: Option<crate::analysis::sample_modal::SampleForm>,
    /// Where available memory is read from for the sample's check: the system's,
    /// unless a test sets it.
    pub(crate) memory_probe: crate::analysis::table_sample::MemoryProbe,
    /// How each random sample of a stream was drawn on this dataset, by source: redrawn,
    /// the same seed keeps the same rows whether or not the count has come in.
    pub(crate) paths: Vec<(String, crate::analysis::table_sample::DrawPath)>,
}

impl SampleState {
    /// A new dataset is on screen: the paths draws took were the last one's.
    pub(crate) fn reset_for_dataset(&mut self) {
        self.paths.clear();
    }
}

/// What the status line says while a sample is drawn.
const DRAWING: &str = "Sampling...";

/// Why a sample is not drawn under a pivot.
const PIVOT_OVER_A_SAMPLE: &str = "A pivot cannot be laid on a sample as it is drawn: sample \
     the pivoted view (Rows from: All rows), or take the pivot away with R";

/// Why the sample under a pivot stays.
const PIVOT_OFF_A_SAMPLE: &str = "The pivot on the sample cannot move to the source: R takes \
     away both";

impl App {
    /// The memory check a sample is drawn under: `analysis.sample_memory_limit`
    /// against what is held, or the memory available now.
    pub(crate) fn memory_check(&self) -> MemoryCheck {
        MemoryCheck {
            limit: Limit::of_setting(self.app_config.analysis.sample_memory_limit),
            probe: Arc::clone(&self.sample.memory_probe),
        }
    }

    /// Read available memory from `probe` instead of the system (for tests).
    pub fn set_memory_probe(&mut self, probe: MemoryProbe) {
        self.sample.memory_probe = probe;
    }

    /// Draw `sample` as the view's sample, replacing any. Rows show as they land, with
    /// `replay` (an applied view's query, filters, sort and columns) laid over them;
    /// without it the view's own steps go back on unless the sample was drawn through
    /// them. `anyway` skips the running memory check; `then_analyze` runs Analysis's
    /// tool once drawn.
    pub(crate) fn apply_table_sample(
        &mut self,
        sample: sampling::Sample,
        replay: Option<crate::view::ViewSettings>,
        anyway: bool,
        then_analyze: bool,
    ) {
        self.draw_table_sample(sample, None, replay, anyway, then_analyze);
    }

    /// [`Self::apply_table_sample`], drawing a stream's random sample the way `path`
    /// says (as a saved view recorded), giving the same rows.
    pub(crate) fn draw_table_sample(
        &mut self,
        sample: sampling::Sample,
        path: Option<crate::analysis::table_sample::DrawPath>,
        replay: Option<crate::view::ViewSettings>,
        anyway: bool,
        then_analyze: bool,
    ) {
        use crate::analysis::data_quality::QualityScope;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let source = state.unsampled();
        // A view scope reads the view as shown, so its steps are part of the sample and not
        // laid on again; its order decides which rows a row range reads.
        let reads_view = !sample.scope.uses_source();
        let ranged = matches!(
            sample.scope,
            QualityScope::FirstRows(_) | QualityScope::ViewRows { .. }
        );
        let sorted = !source.get_sort_columns().is_empty() || !source.get_sort_ascending();
        let through = reads_view
            && (source.changes_rows() || !source.column_changes().is_empty() || ranged && sorted);
        // Steps for the new sample: an applied view's, those on the replaced sample, or the
        // view's own unless the sample stands for them.
        let replay = replay.or_else(|| match state.sampled() {
            Some(_) => Some(crate::view_settings_of(state)),
            None => (!through).then(|| crate::view_settings_of(source)),
        });
        // A pivot needs all its rows, which a sample being drawn lacks: say so rather than
        // drop it.
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
        // How a stream's random sample is drawn is decided once per source: the view's
        // word, the earlier draw's, or whether the count is in. The seed keeps the rows.
        let path_key = Self::sample_path_key(source, &sample.scope);
        let path = (sample.method == sampling::SampleMethod::Spread).then(|| {
            path.or_else(|| {
                self.sample
                    .paths
                    .iter()
                    .find(|(key, _)| *key == path_key)
                    .map(|(_, path)| *path)
            })
            .unwrap_or(match known_total {
                Some(of) => crate::analysis::table_sample::DrawPath::Bernoulli { of },
                None => crate::analysis::table_sample::DrawPath::Reservoir,
            })
        });
        let bytes_per_row = Some(source.sample_row_bytes(sample.scope.uses_source()));
        let streaming = self.app_config.performance.streaming;
        let rows = Arc::new(crate::analysis::table_sample::SampleRows::default());
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
            let live = crate::analysis::table_sample::Live {
                rows,
                notify: Arc::new(move || report(crate::Progress::SampleGrew)),
                memory,
                watch,
                bytes_per_row,
            };
            let drawn = crate::analysis::table_sample::draw(
                &lf,
                &sample,
                known_total,
                path,
                streaming,
                &live,
            )
            .map_err(failed)?;
            Ok(Answer::SampleDrawn(drawn))
        });
    }

    /// Where a view's sample is drawn from: the loaded source for a source scope, else
    /// the view as shown (in order, for a row range) with every column whole.
    fn table_sample_source(
        state: &DataTableState,
        scope: &crate::analysis::data_quality::QualityScope,
    ) -> (sampling::SampleSource, Option<usize>) {
        use crate::analysis::data_quality::QualityScope;
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

    /// Remove the view's sample: the source view returns with the sample's query,
    /// filters and sort laid on it, unless the sample was drawn through the view's own.
    pub(crate) fn clear_table_sample(&mut self) {
        let Some(sampled) = self.data_table_state.as_ref().and_then(|s| s.sampled()) else {
            return;
        };
        let through = sampled.through();
        let settings = self.data_table_state.as_ref().map(crate::view_settings_of);
        // Moving a pivot off the sample would read the whole source: the sample stays, and
        // the way out is said.
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

    /// The view's rows changed under every result read from them: Analysis's, the
    /// chart's, and in-flight pages.
    pub(crate) fn sample_changed(&mut self) {
        self.forget_the_rows_read();
        self.chart.cache.clear();
        let modal = &mut self.analysis_modal;
        modal.describe_results = None;
        modal.distribution_results = None;
        modal.correlation_results = None;
        modal.quality.results = None;
        modal.quality.last_plan = None;
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

    /// Stop the draw in flight and drop what it still sends: replaced by another
    /// sample, or its view is gone.
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

    /// The draw cut its rows to scope: keep their columns for the view taking its first
    /// rows.
    pub(crate) fn sample_begun(&mut self, schema: &polars::prelude::SchemaRef) {
        if let Some(Job::SampleDraw(draw)) = self
            .jobs
            .current_mut(|job| matches!(job, Job::SampleDraw(_)))
        {
            draw.schema = Some(schema.clone());
        }
    }

    /// The view becomes the sample's, steps laid on, replacing its source view or the
    /// previous sample. Only once rows have come: a draw failing first leaves the view.
    /// Returns whether the view is the draw's.
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
        // Rows read before it grew are fewer and in another order: none may land now.
        if grew == Some(false) {
            self.forget_the_rows_read();
        }
        // Re-read the page when nothing else is reading it; else the next chunk or the end
        // does.
        if grew.is_some() && self.rows_in_flight().is_none() && self.in_normal_table_view() {
            self.spawn_collect(None);
        }
    }

    /// The draw ended: rows go into source order, and a waiting Analysis run starts. A
    /// draw stopped by memory says why.
    pub(crate) fn sample_drawn(
        &mut self,
        draw: SampleDraw,
        current: bool,
        drawn: crate::analysis::table_sample::Drawn,
    ) -> Option<AppEvent> {
        if !current || !self.take_on_sample(&draw) {
            return None;
        }
        if let Some(path) = drawn.path {
            self.sample.paths.retain(|(key, _)| *key != draw.path_key);
            self.sample.paths.push((draw.path_key.clone(), path));
        }
        if let Some(state) = self.data_table_state.as_mut() {
            state.sample_drawn(drawn);
        }
        self.forget_the_rows_read();
        if let Some(reason) = draw.rows.stopped() {
            self.flash_note(reason);
        }
        self.spawn_collect(None);
        if draw.then_analyze && self.overlay == crate::Overlay::Analysis {
            self.analysis_modal.computing = None;
            return self.start_analysis_run();
        }
        None
    }

    /// The draw failed or stopped before keeping a row: the view stays as it was. Rows
    /// that came stay, as a sample cut short.
    pub(crate) fn sample_draw_failed(&mut self, draw: &SampleDraw, current: bool, message: &str) {
        if !current {
            return;
        }
        if self.draw_fills_view(draw)
            && let Some(state) = self.data_table_state.as_mut()
        {
            state.sample_drawn(crate::analysis::table_sample::Drawn {
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
            self.read_failed(message);
        }
    }

    /// Apply `view`, whose rows are a sample: back to its source, its drawn-through
    /// view goes on, and the sample is redrawn from its seed; the view's own steps go on
    /// the sample as it arrives.
    pub(crate) fn apply_sampled_view(
        &mut self,
        view: &crate::view::SavedView,
        saved: &crate::view::SavedSample,
        applying: &crate::view::view_apply::Applying,
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
        self.mark_view_applied(view, applying);
        let mut settings = view.settings.clone();
        settings.sample = None;
        self.draw_table_sample(sample, saved.path, Some(settings), false, false);
        match applying {
            crate::view::view_apply::Applying::Matched(why) => {
                self.flash_view_applied(&view.name, *why)
            }
            // A sample's rows are drawn afresh: the group drilled into may not be among them.
            crate::view::view_apply::Applying::Restored(Some(_)) => {
                self.flash_note("Reopened as a sample; the drill-down was not kept".to_string())
            }
            _ => {}
        }
        self.first_rows_settled();
        Ok(())
    }

    /// Where the value tools (Describe, Distribution, Correlation) read the shared
    /// sample from, and its known size. Row ranges count in the table's order; other
    /// view scopes skip the sort, which no statistic needs and which would make a
    /// sampled read read everything.
    pub(crate) fn sample_source(
        &self,
        state: &DataTableState,
    ) -> (sampling::SampleSource, Option<usize>) {
        Self::sample_source_for(state, &self.analysis_modal.sample.scope)
    }

    pub(crate) fn sample_source_for(
        state: &DataTableState,
        scope: &data_quality::QualityScope,
    ) -> (sampling::SampleSource, Option<usize>) {
        if scope.uses_source() {
            let (lf, source) = state.data_quality_source_scan();
            return (sampling::SampleSource::loaded(lf, source), None);
        }
        let lf = match scope {
            data_quality::QualityScope::FirstRows(_)
            | data_quality::QualityScope::ViewRows { .. } => state.lf().clone(),
            _ => state.analysis_lf(),
        };
        (
            sampling::SampleSource::view(lf.select(state.binary_stub_exprs())),
            sampling::view_scope_rows(state.num_rows_if_valid(), scope),
        )
    }

    /// Read the shared sample, as the tool on screen does, to show as a table: Data
    /// Quality's kept sample is cut when it matches, otherwise redrawn from its seed.
    pub(crate) fn read_sample_view(&mut self) -> Option<AppEvent> {
        let sample = self.analysis_modal.sample.clone();
        self.read_sample_rows(sample, None)
    }

    /// Show the sample's rows in place of the table, as Data Quality's drill-in does;
    /// Esc brings back the table and Analysis.
    pub(crate) fn show_sample_view(&mut self, df: polars::prelude::DataFrame, label: String) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let view = match state.sample_view(df) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot show the sample: {error}"));
                return;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality.evidence_return = Some(Box::new(original));
            self.quality.evidence_label = Some(label);
            self.step_back();
            self.forget_the_rows_read();
            self.spawn_async_collect("Loading the sample...");
        }
    }

    /// Adopt a new shared sample: every tool's old results go and the tool on screen
    /// reruns. Data Quality only takes it into its plan until Run.
    pub(crate) fn apply_sample(&mut self, sample: sampling::Sample) -> Option<AppEvent> {
        // A run it would start waits for a cancelled one, with every result kept.
        if self.analysis_modal.selected_tool != Some(analysis_modal::AnalysisTool::DataQuality)
            && self.read_waits_for_cancelled()
        {
            return None;
        }
        // A first run on the sample as it stands takes nothing from the other tools.
        if sample != self.analysis_modal.sample {
            self.analysis_modal.describe_results = None;
            self.analysis_modal.distribution_results = None;
            self.analysis_modal.correlation_results = None;
            self.analysis_modal.quality.results = None;
            self.analysis_modal.quality.last_plan = None;
            self.analysis_modal.quality.from_cache = false;
        }
        self.analysis_modal.sample = sample;
        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
        self.analysis_modal.sample_run_for = Some(self.dataset_generation);
        self.sync_quality_plan();
        if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            return None;
        }
        self.start_analysis_run()
    }
}

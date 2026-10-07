//! Data quality runs: the Setup plan, the run itself, the evidence rows it opens,
//! and the samples, copies and cached results it keeps within its memory budget.

use crate::analysis_modal::AnalysisProgress;
use crate::export_modal::ExportFormat;
use crate::feedback::Confirm;
use crate::jobs::{Answer, Job, Progress};
use crate::quality_memory::{
    KeptQualitySample, QUALITY_RELEASED_REMEMBERED, QualityCacheEntry, QualityCopyJob, RetainedCopy,
};
use crate::table::DataTableState;
use crate::{
    App, AppEvent, QUALITY_RUN_WAITS, analysis_modal, data_quality, glyphs, jobs, numfmt,
    quality_report, sampling, widgets,
};
use color_eyre::Result;
use polars::prelude::LazyFrame;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl App {
    /// The finding under the cursor's rows: from the rows the run kept, at once, or
    /// staged as a read that says what it reads and waits for Enter. A finding with
    /// no rows to show opens nothing.
    pub(crate) fn open_quality_evidence(&mut self) -> Option<AppEvent> {
        let (_, finding) = self.analysis_modal.selected_finding()?;
        let results = self.analysis_modal.data_quality_results.as_ref()?;
        let rows = finding.evidence(results).ok()?;
        let sampled = results.precision == data_quality::QualityPrecision::Sampled;
        let count = finding.evidence_count(results);
        let label = format!(
            "Data Quality / {} / {}",
            finding.title,
            quality_report::columns_label(&finding.columns, 40)
        );
        let what = format!(
            "{} {} {}",
            finding.title,
            glyphs::get().middot,
            quality_report::columns_label(&finding.columns, 40)
        );
        self.show_quality_rows(rows, label, sampled, what, count)
    }

    /// The rows an interval's count under the cursor counted: from the rows the run
    /// kept, or staged as a read when it kept none. Nothing opens for a count of none.
    pub(crate) fn open_interval_evidence(&mut self) -> Option<AppEvent> {
        let schema = self.data_table_state.as_ref().map(|state| state.schema());
        let (predicate, label, count) = self
            .analysis_modal
            .interval_evidence(schema.map(|schema| schema.as_ref()))?;
        let sampled = self
            .analysis_modal
            .data_quality_results
            .as_ref()
            .is_some_and(|results| results.precision == data_quality::QualityPrecision::Sampled);
        let what = label
            .trim_start_matches("Data Quality / ")
            .replace(" / ", &format!(" {} ", glyphs::get().middot));
        self.show_quality_rows(
            quality_report::EvidenceRows::Matching(predicate),
            label,
            sampled,
            what,
            Some(count),
        )
    }

    /// The rows the report on screen measured, while they are kept: a sampled run's
    /// rows, same dataset, view and sample. `None` after a full scan, which keeps
    /// none, and once they are released.
    pub(crate) fn quality_rows_kept(&self) -> Option<std::sync::Arc<data_quality::QualitySample>> {
        self.analysis_modal.data_quality_results.as_ref()?;
        let plan = self.analysis_modal.quality_result_plan();
        if plan.compute != data_quality::QualityCompute::Sample {
            return None;
        }
        self.kept_quality_sample(&plan.sample())
    }

    /// Open rows a Data Quality result counted. Kept rows are cut in memory and
    /// shown; anything else would read the source, so it is staged with what it
    /// reads, and only Enter on that reads.
    fn show_quality_rows(
        &mut self,
        rows: quality_report::EvidenceRows,
        label: String,
        sampled: bool,
        what: String,
        count: Option<usize>,
    ) -> Option<AppEvent> {
        let plan = self.analysis_modal.quality_result_plan().clone();
        let by_files = matches!(rows, quality_report::EvidenceRows::Files(_));
        let label = if sampled {
            format!("{label} / sampled")
        } else {
            label
        };
        if !by_files && self.quality_rows_kept().is_some() {
            return self.read_sample_rows(plan.sample(), Some((rows, label)));
        }
        let state = self.data_table_state.as_ref()?;
        let g = glyphs::get();
        let rows_label = |rows: usize| {
            format!(
                "{} {}",
                numfmt::group_chrome(rows),
                if rows == 1 { "row" } else { "rows" }
            )
        };
        let scope = match &rows {
            quality_report::EvidenceRows::Files(files) => files.clone(),
            _ => plan.scope.clone(),
        };
        // A sample as large as the scope reads every row too, but it was not a full
        // scan: its rows were kept, and since released.
        let not_kept = if plan.compute == data_quality::QualityCompute::Full {
            "a full scan keeps no rows"
        } else {
            "the rows read are no longer kept"
        };
        let (why, reads) = match &rows {
            quality_report::EvidenceRows::Files(data_quality::QualityScope::SourceFiles(files)) => {
                (
                    "their rows are in the files, not the report".to_string(),
                    format!(
                        "the {} named {}",
                        files.len(),
                        if files.len() == 1 { "file" } else { "files" }
                    ),
                )
            }
            _ if sampled => (
                "the sampled rows are no longer kept".to_string(),
                format!(
                    "the sample again: {} {} {}",
                    widgets::data_quality::compute_label(&plan),
                    g.middot,
                    widgets::data_quality::planned_read_label(state, &plan)
                ),
            ),
            quality_report::EvidenceRows::Duplicates => (
                not_kept.to_string(),
                format!(
                    "every row of {}, once {} {}",
                    plan.scope.label(),
                    g.middot,
                    widgets::data_quality::scope_read_label(state, &plan)
                ),
            ),
            // The table counts what matches, which reads every row; then it reads the
            // rows it shows.
            _ => (
                not_kept.to_string(),
                format!(
                    "every row of {} to count them, then the rows on screen {} {}",
                    plan.scope.label(),
                    g.middot,
                    widgets::data_quality::scope_read_label(state, &plan)
                ),
            ),
        };
        let shows = match (&rows, count) {
            (quality_report::EvidenceRows::Duplicates, Some(count)) => {
                format!("{}, copies together", rows_label(count))
            }
            (_, Some(count)) => rows_label(count),
            (_, None) => "the rows that match".to_string(),
        };
        let source = if state.is_remote_source() {
            "remote, read only"
        } else {
            "local, read only"
        };
        self.analysis_modal.data_quality_evidence_read = Some(analysis_modal::EvidenceRead {
            summary: vec![
                ("Rows", what),
                ("Why", why),
                ("Reads", reads),
                ("Shows", shows),
                ("Source", source.to_string()),
            ],
            sample: (sampled && !by_files).then(|| plan.sample()),
            scope,
            rows,
            label,
        });
        None
    }

    /// Enter on a staged read: read the rows it named, as it said.
    pub(crate) fn confirm_evidence_read(&mut self) -> Option<AppEvent> {
        // Beside a cancelled run still reading, the read stays staged for later.
        let staged = self.analysis_modal.data_quality_evidence_read.as_ref()?;
        let kept = staged
            .sample
            .as_ref()
            .is_some_and(|sample| self.kept_quality_sample(sample).is_some());
        if !kept && self.read_waits_for_cancelled() {
            return None;
        }
        let read = self.analysis_modal.data_quality_evidence_read.take()?;
        if let Some(sample) = read.sample {
            return self.read_sample_rows(sample, Some((read.rows, read.label)));
        }
        let predicate = match read.rows {
            quality_report::EvidenceRows::Matching(predicate) => predicate,
            // A column its file never had, or holds in a type the scan cannot read,
            // has no value to filter on: its rows are the ones those files hold.
            quality_report::EvidenceRows::Files(_) => polars::prelude::lit(true),
            quality_report::EvidenceRows::Duplicates => {
                return self.read_duplicate_rows(read.scope, read.label);
            }
        };
        self.open_quality_scope_rows(&read.scope, predicate, read.label)
    }

    /// Every row of `scope` that repeats, read in one pass off the UI thread and
    /// shown as a table: what a full scan's duplicate finding opens once asked to.
    fn read_duplicate_rows(
        &mut self,
        scope: data_quality::QualityScope,
        label: String,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let (lf, schema) = match state.quality_scope_frame(&scope) {
            Ok(frame) => frame,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot open matching rows: {error}"));
                return None;
            }
        };
        let keys = schema.iter_names().cloned().collect::<Vec<_>>();
        // Binary values as the run grouped them: one stub for all, so they never
        // split a group the check counted as copies.
        let columns = schema
            .iter()
            .map(|(name, dtype)| {
                if matches!(dtype, polars::prelude::DataType::Binary) {
                    polars::prelude::lit(crate::table::binary_stub()).alias(name.clone())
                } else {
                    polars::prelude::col(name.clone())
                }
            })
            .collect::<Vec<_>>();
        let streaming = self.app_config.performance.streaming;
        self.analysis_modal.computing = Some(AnalysisProgress::new("Reading the rows that repeat"));
        self.spawn_job(
            Job::SampleRows,
            Some("Reading the rows that repeat..."),
            move |_| {
                let df = data_quality::duplicate_rows(lf.select(columns), &keys, streaming)
                    .map_err(|error| format!("{error}"))?;
                Ok(Answer::Sample { df, label })
            },
        );
        None
    }

    /// The rows of `scope` matching `predicate`, in the table viewer in place of the
    /// table; Esc brings the table and the report back.
    fn open_quality_scope_rows(
        &mut self,
        scope: &data_quality::QualityScope,
        predicate: polars::prelude::Expr,
        label: String,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let view = match state.quality_evidence_view(scope, predicate) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot open matching rows: {error}"));
                return None;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality_evidence_return = Some(Box::new(original));
            self.quality_evidence_label = Some(label);
            self.analysis_modal.active = false;
            self.forget_the_rows_read();
            self.spawn_async_collect("Loading matching rows...");
        }
        None
    }

    pub(crate) fn return_from_quality_evidence(&mut self, reopen_analysis: bool) -> bool {
        let Some(original) = self.quality_evidence_return.take() else {
            return false;
        };
        self.jobs.advance();
        self.len_count_inflight = None;
        self.data_table_state = Some(*original);
        self.quality_evidence_label = None;
        self.analysis_modal.active = reopen_analysis;
        self.busy = false;
        self.status_message = None;
        true
    }

    pub(crate) fn restore_recent_quality_plan(&mut self) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        if self.analysis_modal.data_quality_plan != data_quality::DataQualityPlan::default() {
            return;
        }
        if let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
        }) {
            self.analysis_modal.data_quality_plan = cached.plan.clone();
        }
    }

    /// What the data offers Setup's choices. Read from the schema and the rows
    /// already on screen: nothing here reads the source.
    pub(crate) fn quality_plan_context(&self) -> analysis_modal::PlanContext {
        let Some(state) = self.data_table_state.as_ref() else {
            return analysis_modal::PlanContext::default();
        };
        let plan = &self.analysis_modal.data_quality_plan;
        let scope = &plan.scope;
        let schema = state.schema();
        let mut partitions = state.partition_columns().unwrap_or_default().to_vec();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do.
        if partitions.is_empty()
            && let Some(dir) = self.path.as_ref().filter(|path| path.is_dir())
        {
            partitions = crate::readers::hive::discover_hive_partition_columns(dir)
                .into_iter()
                .filter(|column| schema.get(column).is_some())
                .collect();
        }
        // Date and time columns, then text read as time: a window can split by either.
        let mut time_columns: Vec<(String, bool)> = state
            .quality_temporal_columns(scope)
            .into_iter()
            .map(|column| {
                let has_time =
                    !matches!(schema.get(&column), Some(polars::prelude::DataType::Date));
                (column, has_time)
            })
            .collect();
        for format in &plan.time_formats {
            if !time_columns
                .iter()
                .any(|(column, _)| *column == format.column)
            {
                time_columns.push((
                    format.column.clone(),
                    format.kind == data_quality::TimeKind::Datetime,
                ));
            }
        }
        analysis_modal::PlanContext {
            partitions,
            time_columns,
            files: state.quality_source_file_count() > 1,
            text_columns: state
                .quality_text_columns(scope)
                .into_iter()
                .map(|column| {
                    let examples = state.buffered_values(&column, 3);
                    (column, examples)
                })
                .collect(),
        }
    }

    /// The columns a time role can be given: date and time columns, then text,
    /// which a role reads through its Text as time format.
    pub(crate) fn quality_time_candidates(&self) -> Vec<String> {
        let Some(state) = self.data_table_state.as_ref() else {
            return Vec::new();
        };
        let scope = &self.analysis_modal.data_quality_plan.scope;
        let mut columns = state.quality_temporal_columns(scope);
        columns.extend(state.quality_text_columns(scope));
        columns
    }

    /// The columns Column intent lists: the draft's scope's, from the schema.
    pub(crate) fn quality_intent_columns(&self) -> Vec<(String, polars::prelude::DataType)> {
        self.data_table_state
            .as_ref()
            .map(|state| {
                crate::widgets::quality_intent::intent_columns(
                    state.quality_schema(&self.analysis_modal.data_quality_plan.scope),
                )
            })
            .unwrap_or_default()
    }

    /// What a Data Quality run reads from, as far as the app knows without reading:
    /// where it was opened from, its files, and what the view does to the rows when
    /// the scope is the view.
    fn quality_source_identity(
        &self,
        state: &DataTableState,
        scope: &data_quality::QualityScope,
    ) -> crate::quality_export::SourceIdentity {
        let format = self
            .original_file_format
            .map(|format| format.as_str().to_string())
            .or_else(|| {
                self.path
                    .as_ref()
                    .and_then(|path| path.extension())
                    .and_then(|extension| extension.to_str())
                    .map(str::to_string)
            });
        let mut view = Vec::new();
        if !scope.uses_source() {
            if !state.get_active_query().is_empty() {
                view.push(format!("query: {}", state.get_active_query()));
            }
            if !state.get_active_sql_query().is_empty() {
                view.push(format!("SQL: {}", state.get_active_sql_query()));
            }
            if !state.get_active_fuzzy_query().is_empty() {
                view.push(format!("text: {}", state.get_active_fuzzy_query()));
            }
            for (index, filter) in state.view_filters().iter().enumerate() {
                let join = if index == 0 {
                    String::new()
                } else {
                    format!("{} ", filter.logical_op.as_str())
                };
                view.push(format!(
                    "filter: {join}{} {} {}",
                    filter.column,
                    filter.operator.as_str(),
                    filter.value
                ));
            }
            if state.reshape_source().is_some() {
                view.push("reshaped: pivot or melt".to_string());
            }
        }
        let remote = state.is_remote_source();
        // A local path made whole, so the report names the file wherever it is read;
        // no file system access.
        let piped = self.reads_stdin();
        let location = self.path.as_ref().map(|path| {
            match std::path::absolute(path).ok().filter(|_| !remote && !piped) {
                Some(path) => path.display().to_string(),
                None => path.display().to_string(),
            }
        });
        crate::quality_export::SourceIdentity {
            location,
            remote,
            format,
            view,
            ..crate::quality_export::SourceIdentity::default()
        }
        .with_files(state.quality_source_file_names())
    }

    /// The Setup setting the Data Quality page on screen lacks before it can show
    /// anything; Enter opens it, and the control bar says so.
    pub(crate) fn quality_page_setup(&self) -> Option<data_quality::QualitySetup> {
        let modal = &self.analysis_modal;
        data_quality::page_setup(
            modal.data_quality_page,
            modal.quality_result_plan(),
            modal.data_quality_results.as_ref(),
            self.has_quality_time_columns(),
        )
    }

    /// Whether the plan's scope has a column it reads as time: a date or time
    /// column, or text given a format. What an empty Trends page points to.
    pub(crate) fn has_quality_time_columns(&self) -> bool {
        !self
            .analysis_modal
            .data_quality_plan
            .time_formats
            .is_empty()
            || self.data_table_state.as_ref().is_some_and(|state| {
                !state
                    .quality_temporal_columns(&self.analysis_modal.data_quality_plan.scope)
                    .is_empty()
            })
    }

    /// Whether Data Quality's retained rows are the rows `plan` reads, so a run
    /// starts from them rather than from the source. They serve any grain: every
    /// column is kept, and where each row sat.
    pub(crate) fn quality_kept_serves(&self, plan: &data_quality::DataQualityPlan) -> bool {
        plan.compute == data_quality::QualityCompute::Sample
            && self.kept_quality_sample(&plan.sample()).is_some()
    }

    /// Where a run of `plan` gets its exact segment totals: from the retained rows'
    /// counts, from the pass that reads a new sample, or from a read of their own.
    pub(crate) fn quality_segment_count(
        &self,
        plan: &data_quality::DataQualityPlan,
    ) -> data_quality::SegmentCount {
        if plan.compute != data_quality::QualityCompute::Sample {
            return data_quality::SegmentCount::NotNeeded;
        }
        match self.kept_quality_sample(&plan.sample()) {
            Some(kept) => kept.segment_count(plan),
            None => data_quality::fresh_segment_count(plan, self.quality_may_read_blocks(plan)),
        }
    }

    /// Whether the rows `plan` reads were read this session and released to the
    /// memory budget, so a Run reads them again.
    pub(crate) fn quality_released(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        let sample = plan.sample();
        plan.compute == data_quality::QualityCompute::Sample
            && self
                .quality_released
                .iter()
                .any(|(dataset, view, released)| {
                    *dataset == self.dataset_generation
                        && *view == view_generation
                        && *released == sample
                })
    }

    /// Whether the dataset is one Parquet or IPC file, the one kind the sampler may
    /// read seeded runs of.
    fn quality_one_columnar_file(&self) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        let columnar = matches!(
            self.original_file_format,
            Some(ExportFormat::Parquet | ExportFormat::Ipc)
        ) || self.path.as_ref().is_some_and(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| {
                    matches!(
                        extension.to_ascii_lowercase().as_str(),
                        "parquet" | "pq" | "arrow" | "arrows" | "ipc" | "feather"
                    )
                })
        });
        columnar && state.loaded_file_count() == 1
    }

    /// Whether a random sample of `plan` reads seeded runs of one file rather than
    /// stream every row, as Setup's Read says. Told from the path and the view, since
    /// the sampler's own test needs the plan built. Yes only where the scan is read as
    /// loaded: the whole source whatever the view, or a view that picks no rows (a
    /// sort does not count: samples read the view unsorted). Where it is not sure,
    /// Setup names the longer read.
    pub(crate) fn quality_reads_blocks(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        self.quality_one_columnar_file()
            && match plan.scope {
                data_quality::QualityScope::WholeSource => true,
                data_quality::QualityScope::CurrentView => !state.changes_rows(),
                // Read in the order on screen, sort included.
                data_quality::QualityScope::FirstRows(_)
                | data_quality::QualityScope::ViewRows { .. } => {
                    state.source_file_count() == Some(1)
                }
                _ => false,
            }
    }

    /// Whether a random sample of `plan` may read seeded runs: where
    /// [`Self::quality_reads_blocks`] is sure, and wherever the view may still read
    /// the scan as loaded, a query's included.
    ///
    /// Leans to yes: seeded runs see too few rows to count segments, so a yes is what
    /// makes Setup name a count pass, and a run that streams after all counts in its
    /// one pass and reads less than Setup said, never more.
    pub(crate) fn quality_may_read_blocks(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(state) = self.data_table_state.as_ref() else {
            return false;
        };
        self.quality_reads_blocks(plan)
            || (self.quality_one_columnar_file()
                && state.may_keep_scan_rows()
                && matches!(
                    plan.scope,
                    data_quality::QualityScope::CurrentView
                        | data_quality::QualityScope::FirstRows(_)
                        | data_quality::QualityScope::ViewRows { .. }
                ))
    }

    /// Whether the session cache holds a report measuring what `plan` measures on
    /// this view: the windows it expects are checked against the report, not read.
    pub(crate) fn quality_cached(&self, plan: &data_quality::DataQualityPlan) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        self.quality_cache.iter().any(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(plan)
        })
    }

    /// Everything Data Quality's runs kept for reuse on this dataset, as `d` in Setup
    /// would release it: sampled rows in memory and a full scan's local copy on
    /// disk. `None` when there is neither.
    pub(crate) fn quality_kept_rows(&self) -> Option<widgets::data_quality::KeptRows> {
        let kept = self
            .quality_samples
            .iter()
            .filter(|kept| kept.dataset_generation == self.dataset_generation)
            .collect::<Vec<_>>();
        let copy_bytes = self
            .quality_copies
            .iter()
            .filter(|kept| kept.dataset_generation == self.dataset_generation)
            .map(|kept| kept.copy.bytes())
            .sum::<u64>();
        (!kept.is_empty() || copy_bytes > 0).then(|| widgets::data_quality::KeptRows {
            samples: kept.len(),
            rows: kept.iter().map(|kept| kept.rows.df().height()).sum(),
            bytes: kept.iter().map(|kept| kept.rows.estimated_bytes()).sum(),
            copy_bytes,
        })
    }

    /// `d` in Setup: let go of every row runs kept, as the memory budget would, and
    /// the local copy a full scan fetched, whose files go from disk. A run that would
    /// have reused either reads again, and Setup's Read says so before Run. Reports
    /// stay: they are results, and showing one reads nothing.
    pub(crate) fn release_quality_rows(&mut self) {
        let Some(kept) = self.quality_kept_rows() else {
            self.flash_note("Nothing kept to release".to_string());
            return;
        };
        for released in std::mem::take(&mut self.quality_samples) {
            self.quality_released.retain(|(dataset, view, sample)| {
                !(*dataset == released.dataset_generation
                    && *view == released.view_generation
                    && *sample == released.sample)
            });
            self.quality_released.insert(
                0,
                (
                    released.dataset_generation,
                    released.view_generation,
                    released.sample,
                ),
            );
        }
        self.quality_released.truncate(QUALITY_RELEASED_REMEMBERED);
        // A run still reading the copy holds it until it ends; then the files go.
        let generation = self.dataset_generation;
        self.quality_copies
            .retain(|kept| kept.dataset_generation != generation);
        if kept.copy_bytes > 0 {
            self.quality_copy_released = Some(generation);
        }
        let rows = format!(
            "{} kept {} ({})",
            numfmt::group_chrome(kept.rows),
            if kept.rows == 1 { "row" } else { "rows" },
            widgets::info::format_bytes(kept.bytes as u64)
        );
        let copy = format!(
            "the local copy ({})",
            widgets::info::format_bytes(kept.copy_bytes)
        );
        self.flash_note(match (kept.samples > 0, kept.copy_bytes > 0) {
            (true, true) => format!("Released {rows} and {copy}; the next run reads again"),
            (false, true) => format!("Released {copy}; the next full scan fetches again"),
            _ => format!("Released {rows}; the next run reads again"),
        });
    }

    /// Rows a sampled Data Quality run read, when they are the rows `sample` names
    /// now: same dataset, same view, same sample.
    fn kept_quality_sample(
        &self,
        sample: &sampling::Sample,
    ) -> Option<std::sync::Arc<data_quality::QualitySample>> {
        self.kept_quality_entry(sample)
            .map(|kept| kept.rows.clone())
    }

    fn kept_quality_entry(&self, sample: &sampling::Sample) -> Option<&KeptQualitySample> {
        let view_generation = self.data_table_state.as_ref()?.len_generation();
        self.quality_samples.iter().find(|kept| {
            kept.dataset_generation == self.dataset_generation
                && kept.view_generation == view_generation
                && &kept.sample == sample
        })
    }

    /// Keep what a run read, newest first, in place of any earlier copy of the same
    /// rows: a later run returns them with the counts it added.
    pub(crate) fn retain_quality_sample(&mut self, kept: &KeptQualitySample) {
        if kept.dataset_generation != self.dataset_generation {
            return;
        }
        self.quality_samples.retain(|entry| !entry.same_rows(kept));
        self.quality_released.retain(|(dataset, view, sample)| {
            !(*dataset == kept.dataset_generation
                && *view == kept.view_generation
                && *sample == kept.sample)
        });
        self.quality_samples.insert(0, kept.clone());
        self.trim_quality_memory();
    }

    /// Hold Data Quality's reports and retained rows to [`QUALITY_MEMORY_BUDGET`].
    ///
    /// A report whose rows are still retained goes first: remaking it reads nothing.
    /// Then the oldest rows, whose next run reads them again, which Setup says. A
    /// report with no rows behind it, a full scan's, goes last: it is the dearest to
    /// remake. The newest report and the newest rows always stay, whatever their size,
    /// so a finished read is never thrown away to make room for itself.
    fn trim_quality_memory(&mut self) {
        loop {
            let used = self
                .quality_cache
                .iter()
                .map(|entry| entry.bytes)
                .sum::<usize>()
                + self
                    .quality_samples
                    .iter()
                    .map(|kept| kept.rows.estimated_bytes())
                    .sum::<usize>();
            if used <= self.quality_memory_budget {
                return;
            }
            let remakeable = self
                .quality_cache
                .iter()
                .enumerate()
                .skip(1)
                .rev()
                .find(|(_, entry)| {
                    entry.plan.compute == data_quality::QualityCompute::Sample
                        && self.quality_samples.iter().any(|kept| {
                            kept.dataset_generation == entry.dataset_generation
                                && kept.view_generation == entry.view_generation
                                && kept.sample == entry.plan.sample()
                        })
                })
                .map(|(index, _)| index);
            if let Some(index) = remakeable {
                self.quality_cache.remove(index);
            } else if self.quality_samples.len() > 1 {
                if let Some(released) = self.quality_samples.pop() {
                    self.quality_released.insert(
                        0,
                        (
                            released.dataset_generation,
                            released.view_generation,
                            released.sample,
                        ),
                    );
                    self.quality_released.truncate(QUALITY_RELEASED_REMEMBERED);
                }
            } else if self.quality_cache.len() > 1 {
                self.quality_cache.pop();
            } else {
                return;
            }
        }
    }

    pub(crate) fn restore_cached_quality(&mut self) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        let plan = self.analysis_modal.data_quality_plan.clone();
        let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(&plan)
        }) else {
            return false;
        };
        let mut results = cached.results.clone();
        if cached.plan != plan {
            // Compared as the plan compares, and kept under the windows it expects now.
            if cached.plan.compares_differently(&plan) {
                results.compare_segments(&plan);
            }
            self.cache_quality_result(&results, plan.clone());
        }
        self.analysis_modal.data_quality_results = Some(results);
        self.analysis_modal.data_quality_last_plan = Some(plan);
        self.analysis_modal.data_quality_from_cache = true;
        self.analysis_modal
            .set_quality_page(data_quality::QualityPage::Overview);
        true
    }

    pub(crate) fn cache_quality_result(
        &mut self,
        results: &data_quality::DataQualityResults,
        plan: data_quality::DataQualityPlan,
    ) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        // One report per measurement: a plan that only expects other windows
        // replaces it.
        self.quality_cache.retain(|entry| {
            !(entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan.same_measurement(&plan))
        });
        self.quality_cache.insert(
            0,
            QualityCacheEntry {
                dataset_generation: self.dataset_generation,
                view_generation,
                plan,
                bytes: results.estimated_bytes(),
                results: results.clone(),
            },
        );
        self.trim_quality_memory();
    }

    /// The scope a full scan's passes read: `lf` over a local copy in place of its
    /// remote objects, fetched first when `job` says so. `kept` hears whether the
    /// copy stands in for the source: the copy to keep, or `None`, after which the
    /// dataset's full scans read the source. The copy comes back too, for the caller
    /// to hold while the passes read it.
    pub(crate) fn quality_scope_on_copy(
        lf: LazyFrame,
        job: QualityCopyJob,
        watch: &data_quality::QualityWatch,
        fetch: impl FnOnce(
            &[crate::local_copy::RemoteObject],
            &Path,
        ) -> Result<crate::local_copy::LocalCopy>,
        kept: impl FnOnce(Option<Arc<crate::local_copy::LocalCopy>>),
    ) -> Result<(LazyFrame, Option<Arc<crate::local_copy::LocalCopy>>)> {
        let (copy, fetched) = match job {
            QualityCopyJob::Source => return Ok((lf, None)),
            QualityCopyJob::Kept(copy) => (copy, false),
            QualityCopyJob::Fetch { objects, root } => {
                watch.stage(data_quality::QualityStage::CopyingSource, true, true)?;
                let copy = fetch(&objects, &root).map_err(|error| {
                    if watch.cancelled() {
                        color_eyre::eyre::eyre!(crate::sampling::CANCELLED)
                    } else {
                        error
                    }
                })?;
                (Arc::new(copy), true)
            }
        };
        // The copy must read as the source does, or the source is read as before.
        let local = copy.redirect(&lf).filter(|local| {
            let schemas = (local.clone().collect_schema(), lf.clone().collect_schema());
            matches!(schemas, (Ok(local), Ok(source)) if local == source)
        });
        let Some(local) = local else {
            log::warn!(target: "datui", "local copy does not read as the source; reading the source");
            kept(None);
            return Ok((lf, None));
        };
        if fetched {
            kept(Some(copy.clone()));
        }
        watch.use_copy(data_quality::CopyRead {
            bytes: copy.bytes(),
            objects: copy.objects(),
            fetched,
        });
        Ok((local, Some(copy)))
    }

    /// Copy `objects` under `root`, each streamed from its store and written as it
    /// arrives; a cancel stops it at the next chunk and the partial copy is removed.
    #[cfg(feature = "cloud")]
    fn fetch_quality_copy(
        objects: &[crate::local_copy::RemoteObject],
        root: &Path,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        stop: &crate::sampling::ReadWatch,
    ) -> Result<crate::local_copy::LocalCopy> {
        use crate::download::StreamError;
        use object_store::ObjectStoreExt;

        crate::local_copy::LocalCopy::fetch(root, objects, stop, |object, write| {
            let url = object.url.as_str();
            let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;
            let (_, key) = Self::cloud_bucket_and_key(url)?;
            let path = crate::cloud_browse::object_path(&key);
            let listed = object.etag.clone();
            let open = async move {
                let got = store.get(&path).await.map_err(|e| e.to_string())?;
                // Rewritten since it opened, perhaps at the same size: the copy would
                // not be the dataset on screen.
                if let (Some(listed), Some(fetched)) = (&listed, &got.meta.e_tag)
                    && !crate::local_copy::same_etag(listed, fetched)
                {
                    return Err("it changed since it opened. Open the dataset again".to_string());
                }
                Ok((got.into_stream(), None))
            };
            let watch = stop.clone();
            crate::download::stream_into(runtime, open, move || watch.stopped(), write)
                .map(drop)
                .map_err(|error| match error {
                    StreamError::Write(report) => report,
                    StreamError::Open(e) | StreamError::Read(e) => {
                        color_eyre::eyre::eyre!("Could not copy {url}: {e}")
                    }
                    StreamError::Short { expected, got } => color_eyre::eyre::eyre!(
                        "Could not copy {url}: it ended after {got} of {expected} bytes"
                    ),
                    StreamError::Cut => color_eyre::eyre::eyre!(crate::sampling::CANCELLED),
                })
        })
    }

    /// Where Data Quality's local copies are written.
    fn quality_copies_root(&self) -> PathBuf {
        self.cache.cache_dir().join(crate::local_copy::COPIES_DIR)
    }

    /// `analysis.quality_local_copy`, in bytes.
    fn quality_copy_limit(&self) -> u64 {
        self.app_config.analysis.quality_local_copy.bytes()
    }

    /// Bytes on disk in the copies kept.
    pub fn quality_copy_bytes(&self) -> u64 {
        self.quality_copies
            .iter()
            .map(|kept| kept.copy.bytes())
            .sum()
    }

    /// The copy this dataset's objects were fetched into this session, while kept.
    fn quality_copy_kept(&self) -> Option<&Arc<crate::local_copy::LocalCopy>> {
        let state = self.data_table_state.as_ref()?;
        self.quality_copies
            .iter()
            .find(|kept| {
                kept.dataset_generation == self.dataset_generation
                    && state.each_remote_object().is_some_and(|mut objects| {
                        objects.all(|object| {
                            object.is_some_and(|object| kept.copy.covers(&object.url))
                        })
                    })
            })
            .map(|kept| &kept.copy)
    }

    /// Free bytes where copies are written, asked at most every few seconds.
    fn quality_copy_free_space(&self) -> Option<u64> {
        let root = self.quality_copies_root();
        let Ok(mut cached) = self.quality_copy_free.lock() else {
            return crate::local_copy::free_space(&root);
        };
        match *cached {
            Some((asked, free)) if asked.elapsed() < std::time::Duration::from_secs(5) => free,
            _ => {
                let free = crate::local_copy::free_space(&root);
                *cached = Some((std::time::Instant::now(), free));
                free
            }
        }
    }

    /// How a run of `plan` gets its rows from a remote source, from what the open
    /// learned: no read, and no more than a stat of the cache directory.
    pub(crate) fn quality_copy_plan(
        &self,
        plan: &data_quality::DataQualityPlan,
    ) -> data_quality::CopyPlan {
        use data_quality::{CopyPlan, NoCopy};
        let Some(state) = self.data_table_state.as_ref() else {
            return CopyPlan::NotApplicable;
        };
        if plan.compute != data_quality::QualityCompute::Full || !state.is_remote_source() {
            return CopyPlan::NotApplicable;
        }
        if !state.quality_reads_whole_source(&plan.scope) {
            return CopyPlan::Passes(NoCopy::PartOfTheSource);
        }
        if let Some(copy) = self.quality_copy_kept() {
            return CopyPlan::Kept {
                bytes: copy.bytes(),
                objects: copy.objects(),
            };
        }
        let limit = self.quality_copy_limit();
        if limit == 0 {
            return CopyPlan::Passes(NoCopy::Off);
        }
        if self.quality_copy_unusable == Some(self.dataset_generation) {
            return CopyPlan::Passes(NoCopy::Unusable);
        }
        let Some((bytes, objects)) = state.remote_objects_size() else {
            return CopyPlan::Passes(NoCopy::SizeUnknown);
        };
        if bytes > limit {
            return CopyPlan::Passes(NoCopy::TooLarge { bytes, limit });
        }
        let free = self.quality_copy_free_space();
        if free.is_none_or(|free| bytes > free) {
            return CopyPlan::Passes(NoCopy::NoRoom { bytes, free });
        }
        CopyPlan::Fetch { bytes, objects }
    }

    /// Whether this dataset's copy was released this session, so Run fetches again.
    pub(crate) fn quality_copy_released(&self) -> bool {
        self.quality_copy_released == Some(self.dataset_generation)
    }

    /// Keep a copy a run fetched, newest first. Older copies go past the budget;
    /// the newest stays, so a finished fetch is never thrown away for itself. With
    /// none, the dataset's copy did not read as its source: any kept one goes too.
    pub(crate) fn retain_quality_copy(
        &mut self,
        dataset_generation: u64,
        copy: Option<Arc<crate::local_copy::LocalCopy>>,
    ) {
        if dataset_generation != self.dataset_generation {
            return;
        }
        let Some(copy) = copy else {
            self.quality_copies
                .retain(|kept| kept.dataset_generation != dataset_generation);
            self.quality_copy_unusable = Some(dataset_generation);
            return;
        };
        self.quality_copies.insert(
            0,
            RetainedCopy {
                dataset_generation,
                copy,
            },
        );
        self.quality_copy_released = None;
        let limit = self.quality_copy_limit();
        while self.quality_copies.len() > 1 && self.quality_copy_bytes() > limit {
            self.quality_copies.pop();
        }
    }

    /// Read `sample` off the UI thread and show its rows: all of them, or only a
    /// finding's, under the finding's label. The sample is drawn again from its seed,
    /// so these are the rows the tool measured.
    pub(crate) fn read_sample_rows(
        &mut self,
        sample: sampling::Sample,
        evidence: Option<(quality_report::EvidenceRows, String)>,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let (source, known_total) = Self::sample_source_for(state, &sample.scope);
        let streaming = self.app_config.performance.streaming;
        // The rows Data Quality just measured, when they are the rows asked for: cut
        // from memory rather than drawn again from the files.
        let kept = self.kept_quality_sample(&sample).map(|kept| {
            let columns: Vec<_> = state
                .schema()
                .iter_names()
                .filter(|name| kept.df().column(name.as_str()).is_ok())
                .map(|name| polars::prelude::col(name.clone()))
                .collect();
            (kept, columns)
        });
        if kept.is_none() && self.read_waits_for_cancelled() {
            return None;
        }
        self.analysis_modal.computing = Some(AnalysisProgress::new(if evidence.is_some() {
            "Reading the matching sampled rows"
        } else {
            "Reading the sample"
        }));
        self.spawn_job(Job::SampleRows, Some("Reading the sample..."), move |_| {
            // The columns shown are the table's; a finding is cut from every column
            // the run read first, so duplicates are judged as the run judged them.
            let (rows, columns) = match kept {
                Some((kept, columns)) => (Ok(kept.analysis_rows(kept.df().clone())), Some(columns)),
                None => (
                    source
                        .cut(&sample.scope)
                        .and_then(|lf| sampling::read(&lf, &sample, known_total, streaming)),
                    None,
                ),
            };
            let shown = |df: polars::prelude::DataFrame| match &columns {
                Some(columns) => polars::prelude::IntoLazy::lazy(df)
                    .select(columns.clone())
                    .collect()
                    .map_err(color_eyre::eyre::Report::from),
                None => Ok(df),
            };
            let read = rows.and_then(|rows| {
                let label = format!(
                    "Sample {} {}",
                    crate::glyphs::get().middot,
                    sample.outcome(
                        rows.total_rows,
                        rows.sample_size,
                        rows.per_value.as_ref().map(|per_value| per_value.kept),
                    )
                );
                match evidence {
                    Some((quality_report::EvidenceRows::Duplicates, label)) => {
                        // Every column the run grouped by: the scope's own, not the
                        // row numbers kept beside them.
                        let keys = rows
                            .df
                            .get_column_names()
                            .into_iter()
                            .filter(|name| !name.starts_with("__datui"))
                            .cloned()
                            .collect::<Vec<_>>();
                        let df = data_quality::duplicate_rows(
                            polars::prelude::IntoLazy::lazy(rows.df),
                            &keys,
                            streaming,
                        )?;
                        Ok((shown(df)?, label))
                    }
                    Some((quality_report::EvidenceRows::Matching(predicate), label)) => {
                        let df = polars::prelude::IntoLazy::lazy(rows.df)
                            .filter(predicate)
                            .collect()?;
                        Ok((shown(df)?, label))
                    }
                    // Files are read from the scope, never from a sample.
                    Some((quality_report::EvidenceRows::Files(_), label)) => {
                        Ok((shown(rows.df)?, label))
                    }
                    None => Ok((shown(rows.df)?, label)),
                }
            });
            let (df, label) = read.map_err(|error| format!("{error}"))?;
            Ok(Answer::Sample { df, label })
        });
        None
    }

    /// Mirror the shared sample into the Data Quality plan, which carries it into the
    /// engine and into the session cache's key. Metadata-only stays metadata-only.
    pub(crate) fn sync_quality_plan(&mut self) {
        let sample = self.analysis_modal.sample.clone();
        self.analysis_modal.data_quality_plan.adopt_sample(&sample);
    }

    /// Open Data Quality Setup: the plan, staged. Edits wait for Run, and Esc puts
    /// back the plan as it stood here. Opening it again while open changes nothing.
    pub(crate) fn open_quality_setup(&mut self) {
        use data_quality::QualityPage;
        let modal = &mut self.analysis_modal;
        if !modal.data_quality_page.is_setup() {
            modal.data_quality_setup_return = modal.data_quality_page.tab();
        }
        if modal.data_quality_setup_before.is_none() {
            modal.data_quality_setup_before = Some(modal.data_quality_plan.clone());
        }
        if modal.data_quality_page != QualityPage::Setup {
            modal.set_quality_page(QualityPage::Setup);
            modal.data_quality_plan_field = 0;
        }
        modal.focus = analysis_modal::AnalysisFocus::Main;
    }

    /// Esc on Setup: every staged edit goes, and the report it came from comes back.
    /// With no report yet, Setup stays in the pane and the cursor goes to the tools.
    pub(crate) fn leave_quality_setup(&mut self) {
        use data_quality::QualityPage;
        let modal = &mut self.analysis_modal;
        if let Some(before) = modal.data_quality_setup_before.take() {
            modal.data_quality_plan = before;
        }
        modal.data_quality_setup_note = None;
        modal.data_quality_picker = None;
        if modal.data_quality_results.is_some() {
            let back = match modal.data_quality_setup_return {
                page if page.is_setup() => QualityPage::Overview,
                page => page,
            };
            modal.set_quality_page(back);
        } else {
            modal.set_quality_page(QualityPage::Setup);
            modal.focus = analysis_modal::AnalysisFocus::Sidebar;
        }
    }

    /// The confirmation a full scan asks: what it reads, what it fetches from a
    /// remote source, and that it writes nothing there.
    fn quality_full_scan_question(&self, plan: &data_quality::DataQualityPlan) -> String {
        let mut lines = vec![
            "Run a full scan?".to_string(),
            String::new(),
            "Reads: every eligible row, up to the whole source".to_string(),
        ];
        if let data_quality::CopyPlan::Fetch { bytes, .. } = self.quality_copy_plan(plan) {
            lines.push(format!(
                "Fetch: {} once, to a local copy",
                crate::widgets::info::format_bytes(bytes)
            ));
        }
        lines.push("Source writes: none".to_string());
        lines.join("\n")
    }

    /// What stops Setup from running as it stands, said on its own line: a time
    /// window on text that has no format to read it with.
    fn quality_setup_problem(&self) -> Option<String> {
        let plan = &self.analysis_modal.data_quality_plan;
        let schema = self.data_table_state.as_ref()?.quality_schema(&plan.scope);
        match &plan.grain {
            data_quality::QualityGrain::TimeWindows { column, .. }
                if plan.compute != data_quality::QualityCompute::Metadata
                    && !plan.reads_as_time(column, schema) =>
            {
                Some(format!(
                    "{column}: text, no format {} set Text as time",
                    crate::glyphs::get().middot
                ))
            }
            _ => None,
        }
    }

    /// Run, from Setup: the one place a Data Quality run starts. The draft becomes
    /// the plan, and its sample the one every tool reads; then the report for it is
    /// shown if one is already here, and otherwise read, once.
    ///
    /// Waits, with the reason on Setup, while a cancelled run is still stopping: a
    /// second read beside it is how memory runs out. A full scan asks first, and
    /// Esc there leaves the draft staged and the last report as it was.
    /// `confirmed` is the full-scan question's Yes.
    pub(crate) fn run_quality_setup(&mut self, confirmed: bool) -> Option<AppEvent> {
        use data_quality::QualityPage;
        if self.cancelled_analysis_running().is_some() {
            self.analysis_modal.data_quality_setup_note = Some(QUALITY_RUN_WAITS.to_string());
            return None;
        }
        if let Some(problem) = self.quality_setup_problem() {
            self.analysis_modal.data_quality_setup_note = Some(problem);
            return None;
        }
        // A report already here, on screen or cached, reads nothing: nothing to confirm.
        let plan = &self.analysis_modal.data_quality_plan;
        let here = (self.analysis_modal.data_quality_results.is_some()
            && self
                .analysis_modal
                .data_quality_last_plan
                .as_ref()
                .is_some_and(|last| last.same_measurement(plan)))
            || self.quality_cached(plan);
        if plan.requires_confirmation() && !here && !confirmed {
            // Asked with the one confirmation; its Yes comes back here.
            let message = self.quality_full_scan_question(plan);
            self.confirmation_modal
                .show(message, Confirm::QualityFullScan);
            self.confirmation_modal.yes_label = "Run";
            return None;
        }
        self.commit_quality_plan();
        let modal = &mut self.analysis_modal;
        if modal.data_quality_results.is_some()
            && modal.data_quality_last_plan.as_ref() == Some(&modal.data_quality_plan)
        {
            let back = match modal.data_quality_setup_return {
                page if page.is_setup() => QualityPage::Overview,
                page => page,
            };
            modal.set_quality_page(back);
            return None;
        }
        // Only the expected windows or the comparison changed: the report on screen
        // holds every count the windows are checked against and every segment the
        // comparison is worked out from, so it is relabeled, not read again.
        if let (Some(results), Some(last)) = (
            modal.data_quality_results.as_ref(),
            modal.data_quality_last_plan.as_ref(),
        ) && last.same_measurement(&modal.data_quality_plan)
        {
            let mut results = results.clone();
            let plan = modal.data_quality_plan.clone();
            let page = if last.compares_differently(&plan) {
                results.compare_segments(&plan);
                QualityPage::Segments
            } else {
                QualityPage::Trends
            };
            modal.data_quality_results = Some(results.clone());
            modal.data_quality_last_plan = Some(plan.clone());
            modal.set_quality_page(page);
            self.cache_quality_result(&results, plan);
            return None;
        }
        if self.restore_cached_quality() {
            return None;
        }
        self.analysis_modal.data_quality_from_cache = false;
        let mut progress = AnalysisProgress::new("Preparing the plan");
        if self.quality_kept_serves(&self.analysis_modal.data_quality_plan) {
            progress.reuse = Some("Starts from rows a run already read".to_string());
        }
        self.analysis_modal.computing = Some(progress);
        self.busy = true;
        Some(AppEvent::AnalysisCompute(
            analysis_modal::AnalysisTool::DataQuality,
        ))
    }

    /// The draft is the plan now: Setup closes on it, and its sample becomes the
    /// one every tool reads. The other tools' results were of the old sample, so
    /// they go; Data Quality's last report stays, labeled with what it measured,
    /// until the run replaces it.
    fn commit_quality_plan(&mut self) {
        let modal = &mut self.analysis_modal;
        let sample = modal.data_quality_plan.sample();
        if sample != modal.sample {
            modal.describe_results = None;
            modal.distribution_results = None;
            modal.correlation_results = None;
        }
        modal.sample = sample;
        modal.sample_dataset = Some(self.dataset_generation);
        modal.sample_run_for = Some(self.dataset_generation);
        modal.data_quality_setup_before = None;
        modal.data_quality_setup_note = None;
        modal.data_quality_picker = None;
    }

    /// Run the data quality check on the plan Setup committed.
    pub(crate) fn run_quality_compute(&mut self) -> Option<AppEvent> {
        // The plan Run committed; Setup's Run is the only way here.
        if let Some(state) = &self.data_table_state {
            let plan = self.analysis_modal.data_quality_plan.clone();
            let source_scope = plan.scope.uses_source();
            let (lf, source, cached_rows) = if source_scope {
                let (lf, source) = state.data_quality_source_scan();
                (lf, source, None)
            } else {
                let ordered = matches!(
                    plan.scope,
                    data_quality::QualityScope::FirstRows(_)
                        | data_quality::QualityScope::ViewRows { .. }
                );
                let (lf, source) = state.data_quality_scan(ordered);
                let rows = state.num_rows_if_valid().map(|rows| match &plan.scope {
                    data_quality::QualityScope::CurrentView => rows,
                    data_quality::QualityScope::FirstRows(limit) => rows.min(*limit),
                    data_quality::QualityScope::ViewRows { start, end } => {
                        rows.min(*end).saturating_sub(start.saturating_sub(1))
                    }
                    _ => unreachable!(),
                });
                (lf, source, rows)
            };
            let streaming = state.polars_streaming();
            // An audio file's signal checks read its samples whole: a full run's.
            let audio = (plan.compute == data_quality::QualityCompute::Full)
                .then(|| state.window_for_quality(&plan.scope))
                .flatten()
                .and_then(crate::audio::recording);
            let view_generation = state.len_generation();
            let dataset_generation = self.dataset_generation;
            let kept_entry = self.kept_quality_entry(&plan.sample());
            let kept = kept_entry.map(|kept| kept.rows.clone());
            // A sampled run on rows already read is labeled as their read was:
            // the file may have changed since, and these rows did not.
            let kept_source = kept_entry
                .filter(|_| plan.compute == data_quality::QualityCompute::Sample)
                .map(|kept| kept.source.clone());
            let mut identity = self.quality_source_identity(state, &plan.scope);
            let copy_job = match self.quality_copy_plan(&plan) {
                data_quality::CopyPlan::Kept { .. } => self
                    .quality_copy_kept()
                    .cloned()
                    .map_or(QualityCopyJob::Source, QualityCopyJob::Kept),
                data_quality::CopyPlan::Fetch { .. } => match state.remote_objects() {
                    Some(objects) => QualityCopyJob::Fetch {
                        objects,
                        root: self.quality_copies_root(),
                    },
                    None => QualityCopyJob::Source,
                },
                _ => QualityCopyJob::Source,
            };
            #[cfg(feature = "cloud")]
            let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
            // Only a confirmed full scan pays to read the values a type
            // conflict hides, and only its access plan promised the read.
            let mut source = source;
            if plan.compute == data_quality::QualityCompute::Full
                && let Some(source) = source.as_mut()
            {
                source.conflict_scan = state.quality_conflict_scan();
            }
            // Each stage the worker enters comes back as the job's progress, so a
            // cancelled run's stages are dropped. The watch is the job's: Esc
            // stops the run through its record.
            let started = self.start_job(
                Job::Analysis(jobs::AnalysisRun::default()),
                Some("Profiling data quality..."),
            );
            let ticket = started.ticket();
            let phases = self.events.clone();
            let watch = data_quality::QualityWatch::new(move |phase| {
                let _ = phases.send(AppEvent::JobProgress {
                    ticket,
                    progress: Progress::QualityPhase(phase),
                });
            });
            if let Some(progress) = self.analysis_modal.computing.as_mut() {
                progress.read = Some(watch.read().clone());
            }
            if let Some(Job::Analysis(run)) = self.jobs.job_mut(ticket) {
                run.watch = Some(watch.clone());
            }
            started.run(&self.runtime, move |worker| {
                // A stat of a local file as the run begins, not a read.
                match kept_source {
                    Some(source) => identity = source,
                    None => identity.stat(),
                }
                let lf = if source_scope {
                    data_quality::prepare_source_quality_scan(lf, source.as_ref())
                        .map_err(|error| format!("{error}"))?
                } else {
                    lf
                };
                let lf = data_quality::apply_quality_scope(lf, &plan.scope, source.as_ref())
                    .map_err(|error| format!("{error}"))?;
                // Held to the end of the run: the copy stays on disk while its
                // passes read it, released or not.
                let fetch = |objects: &[crate::local_copy::RemoteObject], root: &Path| {
                    #[cfg(feature = "cloud")]
                    {
                        Self::fetch_quality_copy(objects, root, &cloud, &runtime, watch.read())
                    }
                    #[cfg(not(feature = "cloud"))]
                    {
                        let _ = (objects, root);
                        Err(color_eyre::eyre::eyre!("Built without cloud support"))
                    }
                };
                let kept_copy = |copy: Option<Arc<crate::local_copy::LocalCopy>>| {
                    worker.send(AppEvent::BackgroundQualityCopyKept {
                        dataset_generation,
                        copy,
                    });
                };
                let (lf, held) =
                    Self::quality_scope_on_copy(lf, copy_job, &watch, fetch, kept_copy)
                        .map_err(|error| format!("{error}"))?;
                let (results, rows) = crate::data_quality::compute_data_quality_watched(
                    &lf,
                    cached_rows,
                    &plan,
                    source.as_ref(),
                    streaming,
                    kept.as_deref(),
                    &watch,
                );
                let results = match (results, audio) {
                    (Ok(mut results), Some(audio)) => {
                        crate::data_quality::add_signal_observations(&mut results, &audio, &watch)
                            .map(|()| results)
                    }
                    (results, _) => results,
                };
                // Let go before the answer goes out: a `d` handled as soon
                // as it lands must find the app's handle the last one.
                drop(held);
                let kept = rows.map(|rows| KeptQualitySample {
                    dataset_generation,
                    view_generation,
                    sample: plan.sample(),
                    rows: std::sync::Arc::new(rows),
                    source: identity.clone(),
                });
                match results {
                    Ok(mut results) => {
                        results.source = Some(Box::new(identity));
                        Ok(Answer::DataQuality {
                            results: Box::new(results),
                            kept,
                            plan: Box::new(plan),
                        })
                    }
                    Err(error) => {
                        // Stopped after the sample was read: the read is kept.
                        if let Some(kept) = kept {
                            worker.send(AppEvent::BackgroundQualitySampleKept { kept });
                        }
                        Err(format!("{error}"))
                    }
                }
            });
        } else {
            self.analysis_modal.computing = None;
            self.busy = false;
        }
        None
    }
}

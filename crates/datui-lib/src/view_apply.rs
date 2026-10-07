//! Saved views: the list, the save form, matching a view to a dataset, and replaying
//! its steps with a rollback when its rows fail.

use crate::filter_modal::FilterStatement;
use crate::jobs::{Answer, Job};
use crate::table::DataTableState;
use crate::view::SavedView;
use crate::widgets::view_modal::{FormFocus, ViewModalMode, ViewRow};
use crate::{
    App, QueryRun, Replayed, RunOrigin, active_query_settings, view, view_settings_of, widgets,
};
use color_eyre::Result;
use polars::prelude::DataFrame;

/// Saved views, and the one applied to the dataset on screen.
pub struct SavedViews {
    pub(crate) manager: crate::view::Views,
    pub(crate) active_id: Option<String>, // ID of currently applied view
}

impl App {
    /// Open the views list for the dataset on screen, scored against it.
    pub(crate) fn open_view_list(&mut self) {
        if self.view_dataset().is_none() {
            return;
        }
        self.view_modal.table_state.select(Some(0));
        self.refresh_view_list();
        self.view_modal.active = true;
        self.view_modal.mode = ViewModalMode::List;
    }

    /// Rebuild the list's rows from the store, scored and annotated against
    /// the open dataset; the selection stays near where it was.
    pub(crate) fn refresh_view_list(&mut self) {
        let (Some(state), Some(dataset)) = (&self.data_table_state, self.view_dataset()) else {
            return;
        };
        let rows: Vec<ViewRow> = self
            .views
            .manager
            .find_relevant_views(dataset, state.source_schema())
            .into_iter()
            .map(|(view, score)| {
                let reason = view::match_reason(&view, dataset, state.source_schema());
                ViewRow {
                    view,
                    score,
                    reason,
                }
            })
            .collect();
        self.view_modal.broken_views = self.views.manager.broken_views.clone();
        let selected = self.view_modal.table_state.selected().unwrap_or(0);
        self.view_modal.table_state.select(if rows.is_empty() {
            None
        } else {
            Some(selected.min(rows.len() - 1))
        });
        self.view_modal.rows = rows;
    }

    /// Open the save-view form prefilled from the open dataset: a name the
    /// user will recognize, this file's paths and patterns as criteria, and
    /// schema match on — the criterion that carries the view to the next
    /// table shaped like this one.
    pub(crate) fn open_save_view_form(&mut self) {
        self.view_modal
            .enter_create_mode(self.display.history_limit, &self.theme);

        let query = self.data_table_state.as_ref().and_then(|state| {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            sql_query.or(fuzzy_query).or(query)
        });
        self.view_modal.name_input.suggest(
            self.views
                .manager
                .suggest_name(self.path.as_deref(), query.as_deref()),
        );

        // Data piped in has no file to pin; its columns are what match it.
        if let Some(path) = self.path.as_ref().filter(|_| !self.reads_stdin()) {
            // Pin this file: its absolute path or URL, its path relative to the
            // working directory when it is local and under it, and glob suggestions.
            let absolute_path = view::exact_location(path);
            self.view_modal
                .exact_path_input
                .suggest(absolute_path.to_string_lossy());
            if let Some(relative) = view::relative_location(path) {
                self.view_modal.relative_path_input.suggest(relative);
            }

            // Suggest a path pattern from the absolute path: the parent of a
            // bare relative name is "", and ""/*.parquet is a pattern that
            // matches every parquet file anywhere, forever. The separator is the
            // path's own, or a Windows path never fits its pattern.
            if let Some(parent) = absolute_path.parent()
                && let Some(parent_str) = parent.to_str()
                && !parent_str.is_empty()
                && let Some(ext) = absolute_path.extension()
            {
                let separator = if crate::source::is_remote_url(path) {
                    '/'
                } else {
                    std::path::MAIN_SEPARATOR
                };
                self.view_modal.path_pattern_input.suggest(format!(
                    "{}{separator}*.{}",
                    parent_str.trim_end_matches(separator),
                    ext.to_string_lossy()
                ));
            }

            // Suggest a filename pattern with digit runs wildcarded, so
            // sales_2024.csv offers itself to sales_2025.csv.
            if let Some(filename) = path.file_name()
                && let Some(filename_str) = filename.to_str()
            {
                use regex::Regex;
                let pattern = match Regex::new(r"\d+") {
                    Ok(re) => re.replace_all(filename_str, "*").to_string(),
                    Err(_) => filename_str.to_string(),
                };
                self.view_modal.filename_pattern_input.suggest(pattern);
            }
        }

        self.view_modal.table = self.view_table().map(str::to_string);

        // Schema match starts on: "apply this to a similar table" is the
        // reason views exist, and the columns are the only criterion that
        // says similar.
        if let Some(ref state) = self.data_table_state
            && !state.source_schema().is_empty()
        {
            self.view_modal.schema_match_enabled = true;
        }
    }

    /// Validate and persist the form: a new view, or the edited one. The
    /// settings are rebuilt from the table's applied state either way. A
    /// failed save keeps the form open.
    pub(crate) fn save_view_form(&mut self) {
        self.view_modal.name_error = None;
        let name = self.view_modal.name_input.value().trim().to_string();
        if name.is_empty() {
            self.view_modal.name_error = Some("name is required".to_string());
            self.view_modal.form_focus = FormFocus::Name;
            return;
        }
        let renaming_to_taken = match &self.view_modal.editing_view_id {
            None => self.views.manager.view_exists(&name),
            Some(id) => self
                .views
                .manager
                .get_view_by_name(&name)
                .is_some_and(|other| other.id != *id),
        };
        if renaming_to_taken {
            self.view_modal.name_error = Some("name already exists".to_string());
            self.view_modal.form_focus = FormFocus::Name;
            return;
        }

        let non_empty = |input: &widgets::text_input::TextInput| {
            let value = input.value().trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let match_criteria = view::MatchCriteria {
            exact_path: non_empty(&self.view_modal.exact_path_input).map(std::path::PathBuf::from),
            relative_path: non_empty(&self.view_modal.relative_path_input),
            path_pattern: non_empty(&self.view_modal.path_pattern_input),
            filename_pattern: non_empty(&self.view_modal.filename_pattern_input),
            // The columns the view's settings run on, not the query's output: the
            // next file is matched as loaded.
            schema_columns: if self.view_modal.schema_match_enabled {
                self.data_table_state.as_ref().map(|state| {
                    state
                        .source_schema()
                        .iter_names()
                        .map(|s| s.to_string())
                        .collect()
                })
            } else {
                None
            },
            schema_types: None,
            table: self.view_modal.table.clone(),
        };
        let description = {
            let value = self.view_modal.description_input.value();
            (!value.is_empty()).then(|| value.to_string())
        };

        let saved = if let Some(editing_id) = self.view_modal.editing_view_id.clone() {
            let Some(mut view) = self.views.manager.get_view_by_id(&editing_id).cloned() else {
                return;
            };
            view.name = name;
            view.description = description;
            let stored_schema = view.match_criteria.schema_columns.take();
            view.match_criteria = match_criteria;
            let editing_the_active_view =
                self.views.active_id.as_deref() == Some(editing_id.as_str());
            // The same principle as the settings below: editing an unapplied
            // view must not swap the columns it matches on for the columns of
            // whatever table happens to be open. The toggle still works — off
            // drops the criterion — and the active view follows its table.
            if !editing_the_active_view
                && self.view_modal.schema_match_enabled
                && stored_schema.is_some()
            {
                view.match_criteria.schema_columns = stored_schema;
            }
            // The settings follow the table only while this view is the one
            // dressing it. Editing an unapplied view changes its name,
            // description and matching alone — it must not overwrite what
            // the view carries with whatever the table happens to show.
            if editing_the_active_view && let Some(state) = &self.data_table_state {
                view.settings = view_settings_of(state);
                view.settings.chart = self.saved_chart();
            }
            match self.views.manager.update_view(&view) {
                Ok(()) => true,
                Err(e) => {
                    // Deleted elsewhere, it has left the list too; otherwise the form
                    // stays, edits and all, to try again.
                    if self.views.manager.get_view_by_id(&editing_id).is_none() {
                        self.refresh_view_list();
                        self.view_modal.exit_form();
                    }
                    self.error_modal.show(format!("Error saving view: {e}"));
                    return;
                }
            }
        } else {
            self.create_view_from_current_state(name, description, match_criteria)
                .is_ok()
        };
        if saved {
            self.refresh_view_list();
            self.view_modal.exit_form();
        }
    }

    /// The selected view's score breakdown, for the list's `i` popup.
    pub(crate) fn view_score_details(&self) -> Option<(String, String)> {
        let state = self.data_table_state.as_ref()?;
        let path = self.view_dataset()?;
        let idx = self.view_modal.table_state.selected()?;
        let row = self.view_modal.rows.get(idx)?;
        let view = &row.view;

        let exact_path_match = view::exact_path_matches(&view.match_criteria, path);
        let relative_path_match = view::relative_path_matches(&view.match_criteria, path);
        let file_cols: std::collections::HashSet<&str> = state
            .source_schema()
            .iter_names()
            .map(|s| s.as_str())
            .collect();
        let exact_schema_match =
            view.match_criteria
                .schema_columns
                .as_ref()
                .is_some_and(|required| {
                    let required: std::collections::HashSet<&str> =
                        required.iter().map(|s| s.as_str()).collect();
                    required.is_subset(&file_cols) && file_cols.len() == required.len()
                });

        let mut details = format!("Total score: {:.1}\n\n", row.score);
        if exact_path_match && exact_schema_match {
            details.push_str("Exact path + exact schema: 2000.0\n");
        } else if exact_path_match {
            details.push_str("Exact path: 1000.0\n");
        } else if relative_path_match && exact_schema_match {
            details.push_str("Relative path + exact schema: 1950.0\n");
        } else if relative_path_match {
            details.push_str("Relative path: 950.0\n");
        } else if exact_schema_match {
            details.push_str("Exact schema: 900.0\n");
        } else {
            if view::path_pattern_matches(&view.match_criteria, path) {
                details.push_str("Path pattern match: 50.0+\n");
            }
            if view::filename_pattern_matches(&view.match_criteria, path) {
                details.push_str("Filename pattern match: 30.0+\n");
            }
            if let Some(required_cols) = &view.match_criteria.schema_columns {
                let matching_count = required_cols
                    .iter()
                    .filter(|col| file_cols.contains(col.as_str()))
                    .count();
                if matching_count > 0 {
                    details.push_str(&format!(
                        "Partial schema match: {:.1} ({} columns)\n",
                        matching_count as f64 * 2.0,
                        matching_count
                    ));
                }
            }
        }
        if view.usage_count > 0 {
            details.push_str(&format!(
                "Usage count: {:.1}\n",
                (view.usage_count.min(10) as f64) * 1.0
            ));
        }
        if let Some(last_used) = view.last_used
            && let Ok(duration) = std::time::SystemTime::now().duration_since(last_used)
        {
            let days_since = duration.as_secs() / 86400;
            if days_since <= 7 {
                details.push_str("Recent usage: 5.0\n");
            } else if days_since <= 30 {
                details.push_str("Recent usage: 2.0\n");
            }
        }
        Some((format!("Score: {}", view.name), details))
    }

    /// Start applying `view`. Its steps are planned here, which reads nothing; a
    /// step that cannot be planned fails here and changes nothing. The reads — a pivot,
    /// then the view's first rows — run in the background, and the view is installed
    /// when they are in. One that fails there puts the view before it back (#400).
    pub(crate) fn apply_view(&mut self, view: &SavedView) -> Result<()> {
        self.apply_view_with(view, None)
    }

    /// [`Self::apply_view`], for a view applied because its criteria fit as `why`
    /// says: once its rows are in, a flash names it and the reason.
    pub(crate) fn apply_matched_view(
        &mut self,
        view: &SavedView,
        why: view::MatchReason,
    ) -> Result<()> {
        self.apply_view_with(view, Some(why))
    }

    fn apply_view_with(&mut self, view: &SavedView, why: Option<view::MatchReason>) -> Result<()> {
        self.jobs.supersede(|job| matches!(job, Job::ViewPivot(_)));
        if let Some(saved) = &view.settings.sample {
            return self.apply_sampled_view(view, saved, why);
        }
        let Some(state) = self.data_table_state.as_mut() else {
            return Ok(());
        };
        match state.try_transition(|s| Self::replay_view(s, &view.settings, None))? {
            (Replayed::Planned, rollback) => {
                self.view_planned(view, rollback, why);
                Ok(())
            }
            (Replayed::Pivot(job), rollback) => {
                // The table stays as it is while the pivot is read.
                state.roll_back(rollback);
                // Past any load-ahead for the view on screen, whose rows must not land
                // in the one that replaces it.
                self.jobs.try_advance();
                let pivot_view = Job::ViewPivot(Box::new((view.clone(), why)));
                self.spawn_job(pivot_view, Some(Self::APPLYING_VIEW), move |_| {
                    let pivoted = job
                        .run()
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                    Ok(Answer::ViewPivoted(pivoted))
                });
                Ok(())
            }
        }
    }

    /// The view's steps are planned over `rollback`, the view it replaces: mark it
    /// applied and read its first rows. Until they are in, a failure puts `rollback`
    /// back and the view marked applied before it.
    pub(crate) fn view_planned(
        &mut self,
        view: &SavedView,
        rollback: crate::table::ViewRollback,
        why: Option<view::MatchReason>,
    ) {
        if let Some(path) = &self.path {
            use crate::logging::LogFailure;
            self.views
                .manager
                .record_use(&view.id, path)
                .or_log("record a view's use");
        }
        let previous = self.views.active_id.replace(view.id.clone());
        self.restore_view_chart(view.settings.chart.as_ref());
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        self.prompt.query_running = Some(QueryRun {
            origin: RunOrigin::View {
                previous,
                matched: why.map(|why| (view.name.clone(), why)),
            },
            frame: state.len_generation(),
            rollback,
            counts: self.counting.markers(),
            rows: None,
        });
        if !self.spawn_async_collect(Self::APPLYING_VIEW) {
            // Nothing to read: the view has no rows. Applied on open, it was the
            // open's last step.
            if let Some(why) = why {
                self.flash_view_applied(&view.name, why);
            }
            self.prompt.query_running = None;
            self.busy = false;
            self.status_message = None;
            self.first_rows_settled();
        }
    }

    /// Whether a view is being applied at the table: its pivot or its first rows are
    /// being read.
    pub(crate) fn view_applying(&self) -> bool {
        if !self.is_busy() || !self.in_normal_table_view() {
            return false;
        }
        let pivot = self
            .jobs
            .current(|job| matches!(job, Job::ViewPivot(_)))
            .is_some();
        let rows = self.prompt.query_running.as_ref().is_some_and(|run| {
            matches!(run.origin, RunOrigin::View { .. })
                && self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == run.frame)
        });
        pivot || rows
    }

    /// Stop applying a view and keep the one before it. As with a pivot, a worker runs
    /// to the end and the bump drops its answer.
    pub(crate) fn cancel_view(&mut self) {
        self.jobs.advance();
        self.screen_generation = self.screen_generation.wrapping_add(1);
        if let Some(run) = self.take_query_run() {
            self.roll_back_query_run(run);
        }
        // A collect for the view, queued behind a worker, would read it after all.
        self.forget_the_rows_read();
        self.read_after_view_rollback();
        self.flash_note("View cancelled".to_string());
    }

    /// A view's pivot could not be read or planned: the view before it stays.
    pub(crate) fn view_pivot_failed(&mut self, message: &str) {
        self.error_modal
            .show(format!("Error applying view: {message}"));
        self.read_after_view_rollback();
    }

    /// The view before a failed or cancelled one is back: read its rows if it has none
    /// on hand, as when the view was applied on open, else stop being busy.
    pub(crate) fn read_after_view_rollback(&mut self) {
        self.busy = false;
        self.status_message = None;
        if !self.spawn_async_collect(Self::LOADING_BUFFER) {
            self.first_rows_settled();
        }
    }

    /// Run a view's steps on `state` in the order they were built. With a pivot or melt:
    /// the query, filters and sort it ran over, the reshape, then the query, filters and
    /// sort on its result. Without one: the query, filters and sort. Column order last.
    /// Stops at the first step that fails, and at a pivot unless `pivoted` holds it.
    pub(crate) fn replay_view(
        state: &mut DataTableState,
        settings: &view::ViewSettings,
        pivoted: Option<DataFrame>,
    ) -> Result<Replayed> {
        if settings.pivot.is_some() || settings.melt.is_some() {
            if let Some(source) = &settings.reshape_source {
                Self::replay_query(
                    state,
                    source.sql_query.as_deref(),
                    source.query.as_deref(),
                    source.fuzzy_query.as_deref(),
                )?;
                Self::replay_filters_and_sort(
                    state,
                    &source.filters,
                    &source.sort_columns,
                    source.sort_directions(),
                )?;
            }
            let reshaped = match (&settings.pivot, &settings.melt, pivoted) {
                (Some(spec), _, Some(pivoted)) => state.install_pivot(spec, pivoted),
                (Some(spec), _, None) => {
                    Self::check_plan(state)?;
                    return Ok(Replayed::Pivot(Box::new(state.plan_pivot(spec))));
                }
                (None, Some(spec), _) => state.melt(spec),
                (None, None, _) => Ok(()),
            };
            reshaped.map_err(|e| {
                color_eyre::eyre::eyre!(
                    "{}",
                    crate::error_display::user_message_from_report(&e, None)
                )
            })?;
        }
        Self::replay_query(
            state,
            settings.sql_query.as_deref(),
            settings.query.as_deref(),
            settings.fuzzy_query.as_deref(),
        )?;
        // Before the filters, which may compare in the types it gives.
        if !settings.columns.is_empty() {
            state.set_column_changes(&settings.columns);
        }
        Self::replay_filters_and_sort(
            state,
            &settings.filters,
            &settings.sort_columns,
            settings.sort_directions(),
        )?;
        if !settings.column_order.is_empty() {
            state.set_column_order(settings.column_order.clone());
            state.set_locked_columns(settings.locked_columns_count);
        }
        Self::check_plan(state)?;
        Ok(Replayed::Planned)
    }

    /// Whether the frame the steps so far built can be read, by its plan alone.
    fn check_plan(state: &DataTableState) -> Result<()> {
        state.check_plan().map_err(|e| {
            color_eyre::eyre::eyre!("{}", crate::error_display::user_message_from_polars(&e))
        })
    }

    /// A view's query: SQL or q (at most one is stored), then a Text query.
    fn replay_query(
        state: &mut DataTableState,
        sql: Option<&str>,
        dsl: Option<&str>,
        fuzzy: Option<&str>,
    ) -> Result<()> {
        let stated = |q: Option<&str>| q.filter(|q| !q.trim().is_empty()).map(str::to_string);
        if let Some(sql) = stated(sql) {
            state.sql_query(sql);
        } else if let Some(query) = stated(dsl) {
            state.query(query);
        }
        if state.error().is_none()
            && let Some(fuzzy) = stated(fuzzy)
        {
            state.fuzzy_search(fuzzy);
        }
        match state.error().cloned() {
            Some(error) => Err(color_eyre::eyre::eyre!(
                "{}",
                crate::error_display::user_message_from_polars(&error)
            )),
            None => Ok(()),
        }
    }

    /// A view's sidebar filters, then its sort.
    fn replay_filters_and_sort(
        state: &mut DataTableState,
        filters: &[FilterStatement],
        sort_columns: &[String],
        descending: Vec<bool>,
    ) -> Result<()> {
        if !filters.is_empty() {
            state.filter(filters.to_vec());
            if let Some(error) = state.error().cloned() {
                return Err(color_eyre::eyre::eyre!("{}", error));
            }
        }
        if !sort_columns.is_empty() {
            state.sort_by(sort_columns.to_vec(), descending);
            if let Some(error) = state.error().cloned() {
                return Err(color_eyre::eyre::eyre!("{}", error));
            }
        }
        Ok(())
    }

    pub fn create_view_from_current_state(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: view::MatchCriteria,
    ) -> Result<view::SavedView> {
        let settings = match &self.data_table_state {
            Some(state) => view::ViewSettings {
                chart: self.saved_chart(),
                ..view_settings_of(state)
            },
            None => view::ViewSettings {
                chart: None,
                sample: None,
                query: None,
                sql_query: None,
                fuzzy_query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
                reshape_source: None,
                columns: Vec::new(),
            },
        };

        self.views
            .manager
            .create_view(name, description, match_criteria, settings)
    }
}

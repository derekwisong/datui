//! The command line: its modes (rows, SQL, q, text), completion, running a query off
//! the UI thread, and rolling a failed one back.

use crate::config::QueryMode;
use crate::widgets::text_input::TextInput;
use crate::{App, InputMode, InputType, QueryRun, RunOrigin, sql_assist};
use polars::datatypes::DataType;

impl App {
    /// A query or view whose first rows could not be read is not applied: put back
    /// what it replaced and say why where its origin says to.
    pub(crate) fn fail_query_run(
        &mut self,
        run: QueryRun,
        message: &str,
        conversion: Option<&crate::error_display::ConversionFailure>,
    ) {
        let rows = run.rows;
        let origin = self.roll_back_query_run(run);
        self.first_rows_settled();
        self.status_message = None;
        self.busy = false;
        // Run from the prompt, the reason goes under the query, which stays open to
        // be fixed. Sent any other way — a view applied — there is nothing to edit,
        // and the error modal says why.
        let mode = match origin {
            RunOrigin::View { .. } => {
                self.error_modal
                    .show(format!("Error applying view: {message}"));
                self.read_after_view_rollback();
                return;
            }
            RunOrigin::Query(mode) if self.query_prompt_mode() == Some(mode) => mode,
            RunOrigin::Query(_) => {
                self.error_modal.show(message.to_string());
                return;
            }
        };
        let sql = mode == QueryMode::Sql;
        self.query_run_error = Some(match conversion {
            Some(failure) if sql => failure.sql_message(rows),
            _ => message.to_string(),
        });
        self.inline_failures = self.inline_failures.wrapping_add(1);
    }

    /// Put back the view a running query or view replaced, with its row count, and
    /// return where the query came from.
    pub(crate) fn roll_back_query_run(&mut self, run: QueryRun) -> RunOrigin {
        if let Some(state) = self.data_table_state.as_mut() {
            state.roll_back(run.rollback);
        }
        self.len_count_inflight = run.len_count_inflight;
        self.count_after_paint = run.count_after_paint;
        self.len_count_failed = run.len_count_failed;
        if let RunOrigin::View { previous, .. } = &run.origin {
            self.active_view_id = previous.clone();
        }
        run.origin
    }

    /// The command line's language while it is open.
    pub fn query_prompt_mode(&self) -> Option<QueryMode> {
        (self.input_mode == InputMode::Editing && self.input_type == Some(InputType::Query))
            .then_some(self.query_mode)
    }

    /// `:` at the table: the command line, holding the query in effect, selected,
    /// so typing states a new one and the arrows edit it.
    pub(crate) fn open_command_line(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.input_mode = InputMode::Editing;
        self.input_type = Some(InputType::Query);
        self.query_run_error = None;
        self.sql_completion = None;
        self.query_input.set_value(state.get_active_query());
        self.sql_input.set_value(state.get_active_sql_query());
        self.query_input.select_all();
        self.sql_input.select_all();
        state.suppress_error_display = true;
        self.sql_columns = state.sql_table_columns();
        self.query_mode = self.opening_query_mode();
        self.query_text_restored = !self.query_input_shown().is_empty();
        self.sync_query_focus();
    }

    /// The language `:` opens in: the query in effect's own, so editing never
    /// reinterprets it; else the one Ctrl+T last chose; else the configured default.
    fn opening_query_mode(&self) -> QueryMode {
        let active = self.data_table_state.as_ref().and_then(|state| {
            if !state.get_active_sql_query().trim().is_empty() {
                Some(QueryMode::Sql)
            } else if !state.get_active_query().trim().is_empty() {
                Some(QueryMode::Q)
            } else {
                None
            }
        });
        active
            .or(self.query_mode_chosen)
            .unwrap_or(self.app_config.query.default_mode)
            .resolve()
    }

    /// The command line's input for its current language.
    pub(crate) fn query_input_mut(&mut self) -> &mut TextInput {
        match self.query_mode {
            QueryMode::Sql => &mut self.sql_input,
            QueryMode::Q => &mut self.query_input,
        }
    }

    /// The command line's input for its current language.
    pub(crate) fn query_input_shown(&self) -> &TextInput {
        match self.query_mode {
            QueryMode::Sql => &self.sql_input,
            QueryMode::Q => &self.query_input,
        }
    }

    /// Switch the prompt's mode. Each mode keeps its own text; an error from the
    /// last run belongs to the mode that ran it.
    pub(crate) fn set_query_mode(&mut self, mode: QueryMode) {
        self.query_mode = mode.resolve();
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
        }
        self.query_run_error = None;
        self.sync_query_focus();
    }

    /// Tab in the command line: complete the column name (or, in SQL, the table
    /// name) being typed, and on further presses step through the other names that
    /// match.
    pub(crate) fn complete_column_name(&mut self) {
        let sql = self.query_mode == QueryMode::Sql;
        let columns = std::mem::take(&mut self.sql_columns);
        let mut cycle = self.sql_completion.take();
        let input = self.query_input_mut();
        let line = input
            .line_at(input.cursor_line())
            .unwrap_or_default()
            .to_string();
        let value = input.value().to_string();
        let complete = if sql {
            sql_assist::tab
        } else {
            sql_assist::q_tab
        };
        if let Some(step) = complete(
            &columns,
            &line,
            input.cursor_col(),
            &value,
            input.cursor(),
            &mut cycle,
        ) {
            input.replace_before_cursor(step.span, &step.insert);
            sql_assist::landed(&mut cycle, input.value(), input.cursor());
        }
        self.sql_completion = cycle;
        self.sql_columns = columns;
    }

    /// The columns of `df` the word at the command line's cursor could name, for
    /// the list under the input: every column while nothing is being typed.
    pub(crate) fn sql_column_matches(&self) -> Vec<&(String, DataType)> {
        let input = self.query_input_shown();
        let line = input.line_at(input.cursor_line()).unwrap_or_default();
        let word = match self.query_mode {
            QueryMode::Sql => sql_assist::word_before(line, input.cursor_col()),
            QueryMode::Q => sql_assist::q_word_before(line, input.cursor_col()),
        }
        .map(|w| w.text)
        .unwrap_or_default();
        sql_assist::matching(&self.sql_columns, &word)
    }

    /// The command line's text, while it is open.
    pub fn query_prompt_text(&self) -> Option<&str> {
        self.query_prompt_mode()?;
        Some(self.query_input_shown().value())
    }

    /// Why the last run failed, for the line under the input: a statement that
    /// failed while running, else one that could not be planned.
    pub fn query_prompt_error(&self) -> Option<String> {
        if let Some(error) = &self.query_run_error {
            return Some(error.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let error = state.error()?;
        Some(if self.query_mode == QueryMode::Sql {
            crate::error_display::sql_error_message(error, state.sql_table_rows())
        } else {
            crate::error_display::user_message_from_polars(error)
        })
    }

    /// Bumped each time a running statement's failure is put in the prompt.
    pub fn inline_failures(&self) -> u64 {
        self.inline_failures
    }

    /// Plan a query in `mode` and read its first rows in the background. A query that
    /// cannot be planned leaves its error on the state, where the prompt shows it. One
    /// that plans stays pending — the prompt open, when it came from there — until its
    /// rows are in; if they fail, the view it replaced comes back.
    pub(crate) fn run_query(&mut self, mode: QueryMode, text: &str, status: &str) {
        self.query_run_error = None;
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let rollback = state.rollback_point();
        let rows = state.sql_table_rows();
        // A query that cannot be planned changes nothing and leaves its error showing.
        state.deferred(|s| match mode {
            QueryMode::Sql => s.sql_query(text.to_string()),
            QueryMode::Q => s.query(text.to_string()),
        });
        if state.error().is_some() {
            return;
        }
        self.query_running = Some(QueryRun {
            origin: RunOrigin::Query(mode),
            frame: state.len_generation(),
            rollback,
            len_count_inflight: self.len_count_inflight,
            count_after_paint: self.count_after_paint,
            len_count_failed: self.len_count_failed,
            rows,
        });
        if !self.spawn_async_collect(status) {
            // Nothing to read: the rows on hand already show it.
            self.query_running = None;
            if self.query_prompt_mode() == Some(mode) {
                self.leave_query_prompt_after_run();
            }
        }
    }

    /// The query still running over the frame on screen, taken. One whose frame has
    /// since been replaced is dropped: its rollback would undo what replaced it.
    pub(crate) fn take_query_run(&mut self) -> Option<QueryRun> {
        let run = self.query_running.take()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        (run.frame == frame).then_some(run)
    }

    /// A query ran and its rows are in: the prompt closes on them.
    pub(crate) fn leave_query_prompt_after_run(&mut self) {
        self.sql_completion = None;
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        self.sql_input.set_focused(false);
        self.query_input.set_focused(false);
        if let Some(state) = &mut self.data_table_state {
            state.suppress_error_display = false;
        }
    }

    /// Only the current language's input carries the cursor.
    pub(crate) fn sync_query_focus(&mut self) {
        let mode = self.query_mode;
        self.sql_input.set_focused(mode == QueryMode::Sql);
        self.query_input.set_focused(mode == QueryMode::Q);
    }

    /// Esc from anywhere in the prompt: nothing runs and nothing typed survives.
    pub(crate) fn close_query_prompt(&mut self) {
        self.query_run_error = None;
        self.sql_completion = None;
        self.query_input.clear();
        self.sql_input.clear();
        self.query_input.set_focused(false);
        self.sql_input.set_focused(false);
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
            state.suppress_error_display = false;
        }
    }
}

//! The command line: its modes (rows, SQL, q, text), completion, running a query off
//! the UI thread, and rolling a failed one back.

use crate::config::QueryMode;
use crate::widgets::text_input::TextInput;
use crate::{App, InputMode, InputType, QueryRun, RunOrigin, sql_assist};
use polars::datatypes::DataType;

/// The command line: its inputs per mode, completion, and the query it is running.
pub struct QueryPrompt {
    // One input per command line language, each with its own history. The history
    // ids ("query", "sql") name files already on disk; they stay as they are so no
    // history is lost or read as another language's.
    pub(crate) query_input: TextInput, // q, history id "query"
    pub(crate) sql_input: TextInput,   // SQL, history id "sql"
    /// The find prompt (`/`) and the find `n` and `N` repeat; history id "find".
    pub find: crate::find::Find,
    /// The column cursor moved last: the footer offers the column's keys.
    pub(crate) column_hints: bool,
    pub(crate) input_type: Option<InputType>,
    pub(crate) query_mode: QueryMode,
    /// The language Ctrl+T last chose, which the command line opens on until a query
    /// in effect says otherwise.
    pub(crate) query_mode_chosen: Option<QueryMode>,
    /// The command line holds the query in effect, selected and untouched: Ctrl+T
    /// carries it selected, so typing still replaces it.
    pub(crate) query_text_restored: bool,
    /// The columns of `df`, for the command line's list and completion. Taken from
    /// the schema when it opens.
    pub(crate) sql_columns: Vec<(String, DataType)>,
    /// A Tab completion in progress in the command line.
    pub(crate) sql_completion: Option<sql_assist::Cycle>,
    /// A query whose first collect is running, and the view to go back to if it
    /// fails. From the prompt, the prompt stays open until it is done.
    pub(crate) query_running: Option<QueryRun>,
    /// Why the last statement failed once it ran, shown under it in the prompt.
    pub(crate) query_run_error: Option<String>,
    /// Bumped when a running statement's failure lands in the prompt. Keys typed while
    /// it ran were not answers to it; see `EventPump`.
    pub(crate) inline_failures: u64,
}

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
        self.prompt.query_run_error = Some(match conversion {
            Some(failure) if sql => failure.sql_message(rows),
            _ => message.to_string(),
        });
        self.prompt.inline_failures = self.prompt.inline_failures.wrapping_add(1);
    }

    /// Put back the view a running query or view replaced, with its row count, and
    /// return where the query came from.
    pub(crate) fn roll_back_query_run(&mut self, run: QueryRun) -> RunOrigin {
        if let Some(state) = self.data_table_state.as_mut() {
            state.roll_back(run.rollback);
        }
        self.counting.restore(run.counts);
        if let RunOrigin::View { previous, .. } = &run.origin {
            self.views.active_id = previous.clone();
        }
        run.origin
    }

    /// The command line's language while it is open.
    pub fn query_prompt_mode(&self) -> Option<QueryMode> {
        (self.input_mode == InputMode::Editing && self.prompt.input_type == Some(InputType::Query))
            .then_some(self.prompt.query_mode)
    }

    /// `:` at the table: the command line, holding the query in effect, selected,
    /// so typing states a new one and the arrows edit it.
    pub(crate) fn open_command_line(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.input_mode = InputMode::Editing;
        self.prompt.input_type = Some(InputType::Query);
        self.prompt.query_run_error = None;
        self.prompt.sql_completion = None;
        self.prompt.query_input.set_value(state.get_active_query());
        self.prompt
            .sql_input
            .set_value(state.get_active_sql_query());
        self.prompt.query_input.select_all();
        self.prompt.sql_input.select_all();
        state.suppress_error_display = true;
        self.prompt.sql_columns = state.sql_table_columns();
        self.prompt.query_mode = self.opening_query_mode();
        self.prompt.query_text_restored = !self.query_input_shown().is_empty();
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
            .or(self.prompt.query_mode_chosen)
            .unwrap_or(self.app_config.query.default_mode)
            .resolve()
    }

    /// The command line's input for its current language.
    pub(crate) fn query_input_mut(&mut self) -> &mut TextInput {
        match self.prompt.query_mode {
            QueryMode::Sql => &mut self.prompt.sql_input,
            QueryMode::Q => &mut self.prompt.query_input,
        }
    }

    /// The command line's input for its current language.
    pub(crate) fn query_input_shown(&self) -> &TextInput {
        match self.prompt.query_mode {
            QueryMode::Sql => &self.prompt.sql_input,
            QueryMode::Q => &self.prompt.query_input,
        }
    }

    /// Switch the prompt's mode. Each mode keeps its own text; an error from the
    /// last run belongs to the mode that ran it.
    pub(crate) fn set_query_mode(&mut self, mode: QueryMode) {
        self.prompt.query_mode = mode.resolve();
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
        }
        self.prompt.query_run_error = None;
        self.sync_query_focus();
    }

    /// Tab in the command line: complete the column name (or, in SQL, the table
    /// name) being typed, and on further presses step through the other names that
    /// match.
    pub(crate) fn complete_column_name(&mut self) {
        let sql = self.prompt.query_mode == QueryMode::Sql;
        let columns = std::mem::take(&mut self.prompt.sql_columns);
        let mut cycle = self.prompt.sql_completion.take();
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
        self.prompt.sql_completion = cycle;
        self.prompt.sql_columns = columns;
    }

    /// The columns of `df` the word at the command line's cursor could name, for
    /// the list under the input: every column while nothing is being typed.
    pub(crate) fn sql_column_matches(&self) -> Vec<&(String, DataType)> {
        let input = self.query_input_shown();
        let line = input.line_at(input.cursor_line()).unwrap_or_default();
        let word = match self.prompt.query_mode {
            QueryMode::Sql => sql_assist::word_before(line, input.cursor_col()),
            QueryMode::Q => sql_assist::q_word_before(line, input.cursor_col()),
        }
        .map(|w| w.text)
        .unwrap_or_default();
        sql_assist::matching(&self.prompt.sql_columns, &word)
    }

    /// The command line's text, while it is open.
    pub fn query_prompt_text(&self) -> Option<&str> {
        self.query_prompt_mode()?;
        Some(self.query_input_shown().value())
    }

    /// Why the last run failed, for the line under the input: a statement that
    /// failed while running, else one that could not be planned.
    pub fn query_prompt_error(&self) -> Option<String> {
        if let Some(error) = &self.prompt.query_run_error {
            return Some(error.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let error = state.error()?;
        Some(if self.prompt.query_mode == QueryMode::Sql {
            crate::error_display::sql_error_message(error, state.sql_table_rows())
        } else {
            crate::error_display::user_message_from_polars(error)
        })
    }

    /// Bumped each time a running statement's failure is put in the prompt.
    pub fn inline_failures(&self) -> u64 {
        self.prompt.inline_failures
    }

    /// Plan a query in `mode` and read its first rows in the background. A query that
    /// cannot be planned leaves its error on the state, where the prompt shows it. One
    /// that plans stays pending — the prompt open, when it came from there — until its
    /// rows are in; if they fail, the view it replaced comes back.
    pub(crate) fn run_query(&mut self, mode: QueryMode, text: &str, status: &str) {
        self.prompt.query_run_error = None;
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
        self.prompt.query_running = Some(QueryRun {
            origin: RunOrigin::Query(mode),
            frame: state.len_generation(),
            rollback,
            counts: self.counting.markers(),
            rows,
        });
        if !self.spawn_async_collect(status) {
            // Nothing to read: the rows on hand already show it.
            self.prompt.query_running = None;
            if self.query_prompt_mode() == Some(mode) {
                self.leave_query_prompt_after_run();
            }
        }
    }

    /// The query still running over the frame on screen, taken. One whose frame has
    /// since been replaced is dropped: its rollback would undo what replaced it.
    pub(crate) fn take_query_run(&mut self) -> Option<QueryRun> {
        let run = self.prompt.query_running.take()?;
        let frame = self.data_table_state.as_ref()?.len_generation();
        (run.frame == frame).then_some(run)
    }

    /// A query ran and its rows are in: the prompt closes on them.
    pub(crate) fn leave_query_prompt_after_run(&mut self) {
        self.prompt.sql_completion = None;
        self.input_mode = InputMode::Normal;
        self.prompt.input_type = None;
        self.prompt.sql_input.set_focused(false);
        self.prompt.query_input.set_focused(false);
        if let Some(state) = &mut self.data_table_state {
            state.suppress_error_display = false;
        }
    }

    /// Only the current language's input carries the cursor.
    pub(crate) fn sync_query_focus(&mut self) {
        let mode = self.prompt.query_mode;
        self.prompt.sql_input.set_focused(mode == QueryMode::Sql);
        self.prompt.query_input.set_focused(mode == QueryMode::Q);
    }

    /// Esc from anywhere in the prompt: nothing runs and nothing typed survives.
    pub(crate) fn close_query_prompt(&mut self) {
        self.prompt.query_run_error = None;
        self.prompt.sql_completion = None;
        self.prompt.query_input.clear();
        self.prompt.sql_input.clear();
        self.prompt.query_input.set_focused(false);
        self.prompt.sql_input.set_focused(false);
        self.input_mode = InputMode::Normal;
        self.prompt.input_type = None;
        if let Some(state) = &mut self.data_table_state {
            state.dismiss_error();
            state.suppress_error_display = false;
        }
    }
}

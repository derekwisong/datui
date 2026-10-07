//! Keys in the command line (`:`) and the find prompt (`/`).

use crate::config::QueryMode;
use crate::widgets::text_input::TextInputEvent;
use crate::{App, AppEvent, InputType};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// The row the command line's text names, when it is digits alone.
pub(crate) fn row_number(text: &str) -> Option<usize> {
    let text = text.trim();
    // Past the last row is the last row: digits too many to count are still a row.
    (!text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()))
        .then(|| text.parse().unwrap_or(usize::MAX))
}

impl App {
    /// Keys while a prompt (the command line, find) is being edited.
    pub(crate) fn editing_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match self.input_type {
            Some(InputType::Query) => self.command_line_key(event),
            Some(InputType::Find) => self.find_prompt_key(event),
            None => None,
        }
    }

    /// A key in the command line. Ctrl+T switches between SQL and q, keeping the
    /// text; Tab completes a column name; Enter goes to a row when the text is
    /// digits, else runs the query.
    fn command_line_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if event.is_press()
            && event.modifiers == KeyModifiers::CONTROL
            && event.code == KeyCode::Char('t')
        {
            let text = self.query_prompt_text().unwrap_or_default().to_string();
            let next = self.query_mode.next();
            if next != self.query_mode {
                self.query_input_mut().clear();
                self.set_query_mode(next);
                self.query_mode_chosen = Some(next);
                let restored = self.query_text_restored;
                let input = self.query_input_mut();
                input.set_value(text);
                if restored {
                    input.select_all();
                }
            }
            return None;
        }
        if event.is_press() {
            self.query_text_restored = false;
        }
        if event.is_press()
            && event.code == KeyCode::Tab
            && !event.modifiers.contains(KeyModifiers::SHIFT)
        {
            self.complete_column_name();
            return None;
        }
        // Shift+Tab completes nothing, and moves nothing.
        if event.code == KeyCode::BackTab {
            return None;
        }

        self.sync_query_focus();
        let mode = self.query_mode;
        let input = match mode {
            QueryMode::Sql => &mut self.sql_input,
            QueryMode::Q => &mut self.query_input,
        };
        match input.handle_key(event, Some(&self.cache)) {
            TextInputEvent::Submit => {
                let text = input.value().to_string();
                if let Some(row) = row_number(&text) {
                    return self.go_to_row(row);
                }
                let _ = input.save_to_history(&self.cache);
                return Some(match mode {
                    QueryMode::Sql => AppEvent::SqlQuery(text),
                    QueryMode::Q => AppEvent::QQuery(text),
                });
            }
            TextInputEvent::Cancel => self.close_query_prompt(),
            TextInputEvent::HistoryChanged | TextInputEvent::None if event.is_press() => {
                self.query_run_error = None;
            }
            TextInputEvent::HistoryChanged | TextInputEvent::None => {}
        }
        None
    }

    /// Enter on digits: the command line closes and the cursor goes to that row,
    /// counted as the row numbers count.
    fn go_to_row(&mut self, display_line: usize) -> Option<AppEvent> {
        self.close_query_prompt();
        let state = self.data_table_state.as_mut()?;
        let mut row_index = display_line
            .saturating_sub(state.row_start_index())
            .min(i64::MAX as usize / 2);
        if let Some(rows) = state.num_rows_if_valid() {
            row_index = row_index.min(rows.saturating_sub(1));
        }
        let would_collect =
            state.scroll_would_trigger_collect(row_index as i64 - state.start_row() as i64);
        if would_collect {
            self.busy = true;
            return Some(AppEvent::GoToLine(row_index));
        }
        state.scroll_to_row_centered(row_index);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::row_number;

    #[test]
    fn only_digits_name_a_row() {
        assert_eq!(row_number("42"), Some(42));
        assert_eq!(row_number(" 0 "), Some(0));
        assert_eq!(row_number(""), None);
        assert_eq!(row_number("4 2"), None);
        assert_eq!(row_number("-3"), None);
        assert_eq!(row_number("select 1"), None);
    }
}

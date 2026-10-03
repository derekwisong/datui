//! Keys in the query, find and go-to-line prompts.

use crate::config::QueryMode;
use crate::widgets::text_input::TextInputEvent;
use crate::{App, AppEvent, InputMode, InputType, QueryFocus};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys while a prompt (query, find, go to line) is being edited.
    pub(crate) fn editing_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if self.input_type == Some(InputType::Query) {
            const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];
            const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

            // One chord switches the mode from anywhere in the prompt: in
            // the input ←/→ belong to the cursor, so the tab bar alone
            // cost four keys.
            if event.is_press()
                && event.modifiers == KeyModifiers::CONTROL
                && event.code == KeyCode::Char('t')
            {
                self.set_query_mode(self.query_mode.next());
                return None;
            }

            if self.query_focus == QueryFocus::TabBar && event.is_press() {
                // Enter included: it must never dead-end, so from the tab
                // bar it returns to the input, one keystroke from running.
                if event.code == KeyCode::BackTab
                    || event.code == KeyCode::Enter
                    || (event.code == KeyCode::Tab
                        && !event.modifiers.contains(KeyModifiers::SHIFT))
                {
                    self.query_focus = QueryFocus::Input;
                    self.sync_query_focus();
                    return None;
                }
                if RIGHT_KEYS.contains(&event.code) {
                    self.set_query_mode(self.query_mode.next());
                    return None;
                }
                if LEFT_KEYS.contains(&event.code) {
                    self.set_query_mode(self.query_mode.prev());
                    return None;
                }
                if event.code == KeyCode::Esc {
                    self.close_query_prompt();
                }
                return None;
            }

            // Shift+Tab goes up to the tab bar from every mode. Tab completes a
            // name in SQL, and in the other modes, with nothing to complete, it
            // goes to the tab bar too.
            let shift_tab = event.code == KeyCode::BackTab
                || (event.code == KeyCode::Tab && event.modifiers.contains(KeyModifiers::SHIFT));
            if event.is_press() && event.code == KeyCode::Tab && !shift_tab {
                if self.query_mode == QueryMode::Sql {
                    self.complete_sql_name();
                } else {
                    self.query_focus = QueryFocus::TabBar;
                    self.sync_query_focus();
                }
                return None;
            }
            if event.is_press() && shift_tab {
                self.query_focus = QueryFocus::TabBar;
                self.sync_query_focus();
                return None;
            }

            if self.query_focus != QueryFocus::Input {
                return None;
            }

            self.sync_query_focus();
            let mode = self.query_mode;
            let input = match mode {
                QueryMode::Sql => &mut self.sql_input,
                QueryMode::Text => &mut self.fuzzy_input,
                QueryMode::Q => &mut self.query_input,
            };
            match input.handle_key(event, Some(&self.cache)) {
                TextInputEvent::Submit => {
                    let _ = input.save_to_history(&self.cache);
                    let text = input.value().to_string();
                    return Some(match mode {
                        QueryMode::Sql => AppEvent::SqlQuery(text),
                        QueryMode::Text => AppEvent::TextQuery(text),
                        QueryMode::Q => AppEvent::QQuery(text),
                    });
                }
                TextInputEvent::Cancel => self.close_query_prompt(),
                TextInputEvent::HistoryChanged | TextInputEvent::None => {}
            }
            return None;
        }

        if self.input_type == Some(InputType::Find) {
            return self.find_prompt_key(event);
        }

        // Line number input (GoToLine): ":" then type line number, Enter to jump, Esc to cancel
        if self.input_type == Some(InputType::GoToLine) {
            // The prompt borrows `query_input`, whose history is the query
            // history: without this, ↑ filled the line with a past query
            // and Enter on it closed silently.
            let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
            if matches!(event.code, KeyCode::Up | KeyCode::Down)
                || (ctrl && matches!(event.code, KeyCode::Char('p' | 'n')))
            {
                return None;
            }
            self.query_input.set_focused(true);
            let result = self.query_input.handle_key(event, None);
            match result {
                TextInputEvent::Submit => {
                    let value = self.query_input.value().trim().to_string();
                    self.query_input.clear();
                    self.query_input.set_focused(false);
                    self.input_mode = InputMode::Normal;
                    self.input_type = None;
                    if let Some(state) = &mut self.data_table_state
                        && let Ok(display_line) = value.parse::<usize>()
                    {
                        let row_index = display_line.saturating_sub(state.row_start_index());
                        let would_collect = state.scroll_would_trigger_collect(
                            row_index as i64 - state.start_row() as i64,
                        );
                        if would_collect {
                            self.busy = true;
                            return Some(AppEvent::GoToLine(row_index));
                        }
                        state.scroll_to_row_centered(row_index);
                    }
                }
                TextInputEvent::Cancel => {
                    self.query_input.clear();
                    self.query_input.set_focused(false);
                    self.input_mode = InputMode::Normal;
                    self.input_type = None;
                }
                TextInputEvent::HistoryChanged | TextInputEvent::None => {}
            }
            return None;
        }

        None
    }
}

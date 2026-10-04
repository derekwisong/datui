//! The sort and filter modal's keys.

use crate::filter_modal::{FilterEditStep, FilterOperator};
use crate::sort_filter_modal::{SortFilterFocus, SortFilterTab};
use crate::sort_modal::SortFocus;
use crate::widgets::column_widths::WidthChoice;
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys in the sort and filter modal.
    pub(crate) fn sort_filter_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let on_tab_bar = self.sort_filter_modal.focus == SortFilterFocus::TabBar;
        let on_body = self.sort_filter_modal.focus == SortFilterFocus::Body;
        let sort_tab = self.sort_filter_modal.active_tab == SortFilterTab::Sort;
        let filter_tab = self.sort_filter_modal.active_tab == SortFilterTab::Filter;
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let on_find = on_body && sort_tab && self.sort_filter_modal.sort.focus == SortFocus::Filter;
        let on_column_list =
            on_body && sort_tab && self.sort_filter_modal.sort.focus == SortFocus::ColumnList;
        // The status line is about the last key; this one replaces it.
        self.sort_filter_modal.sort.status = None;

        // Ctrl+J is the apply chord beside Ctrl+Enter: it works on every
        // terminal, and some send Ctrl+Enter as Ctrl+J.
        let apply_chord = ctrl && matches!(event.code, KeyCode::Enter | KeyCode::Char('j'));

        // The inline filter editor owns the keys while it is up: a small form
        // within the form. Esc ends the edit and only the edit.
        if filter_tab && self.sort_filter_modal.filter.editor.is_some() {
            if apply_chord {
                return self.apply_sort_filter();
            }
            let m = &mut self.sort_filter_modal.filter;
            let editor = m.editor.as_mut().expect("checked above");
            match event.code {
                KeyCode::Esc => m.cancel_editor(),
                // Enter chooses the step's pick; from the value it commits the row.
                // Space chooses too: a space typed into the narrowing filter
                // matches nothing and blanks the list. (The value field below
                // keeps Space for typing.)
                KeyCode::Enter | KeyCode::Tab | KeyCode::Right | KeyCode::Char(' ')
                    if editor.step != FilterEditStep::Value =>
                {
                    match editor.step {
                        FilterEditStep::Column => {
                            if editor.column.selected_original().is_some() {
                                editor.step = FilterEditStep::Operator;
                            }
                        }
                        // A null test has no value to ask for: choosing it commits.
                        FilterEditStep::Operator
                            if editor
                                .operator
                                .selected_original()
                                .and_then(|i| FilterOperator::iterator().nth(i))
                                .is_some_and(|op| !op.takes_value()) =>
                        {
                            m.commit_editor();
                        }
                        FilterEditStep::Operator => {
                            editor.step = FilterEditStep::Value;
                            // Pre-filled from the statement under edit; typing
                            // replaces it, arrows keep it editable.
                            editor.value.select_all();
                        }
                        FilterEditStep::Value => {}
                    }
                }
                KeyCode::Enter => m.commit_editor(),
                KeyCode::BackTab => {
                    editor.step = match editor.step {
                        FilterEditStep::Column | FilterEditStep::Operator => FilterEditStep::Column,
                        FilterEditStep::Value => FilterEditStep::Operator,
                    };
                }
                KeyCode::Up => match editor.step {
                    FilterEditStep::Column => editor.column.move_up(),
                    FilterEditStep::Operator => editor.operator.move_up(),
                    FilterEditStep::Value => {}
                },
                KeyCode::Down => match editor.step {
                    FilterEditStep::Column => editor.column.move_down(),
                    FilterEditStep::Operator => editor.operator.move_down(),
                    FilterEditStep::Value => {}
                },
                KeyCode::Backspace if editor.step == FilterEditStep::Column => {
                    editor.column.backspace();
                }
                KeyCode::Backspace if editor.step == FilterEditStep::Operator => {
                    editor.operator.backspace();
                }
                KeyCode::Char(c) if editor.step == FilterEditStep::Column => {
                    editor.column.filter_key(c, event.modifiers);
                }
                KeyCode::Char(c) if editor.step == FilterEditStep::Operator => {
                    editor.operator.filter_key(c, event.modifiers);
                }
                // The value is an ordinary text field, readline included.
                _ if editor.step == FilterEditStep::Value => {
                    let _ = editor.value.handle_key(event, None);
                }
                _ => {}
            }
            return None;
        }

        match event.code {
            KeyCode::Esc => {
                for col in &mut self.sort_filter_modal.sort.columns {
                    col.is_to_be_locked = false;
                }
                self.sort_filter_modal.sort.has_unapplied_changes = false;
                self.sort_filter_modal.close();
                self.input_mode = InputMode::Normal;
            }
            _ if apply_chord => return self.apply_sort_filter(),
            KeyCode::Tab => self.sort_filter_modal.next_focus(),
            KeyCode::BackTab => self.sort_filter_modal.prev_focus(),
            // The find field keeps its readline keys; Up/Down and the rest fall
            // through to the arms below.
            _ if on_find
                && !matches!(
                    event.code,
                    KeyCode::Tab
                        | KeyCode::BackTab
                        | KeyCode::Esc
                        | KeyCode::Enter
                        | KeyCode::Up
                        | KeyCode::Down
                ) =>
            {
                let _ = self
                    .sort_filter_modal
                    .sort
                    .filter_input
                    .handle_key(event, Some(&self.cache));
            }
            // Arrows switch tabs from the tab bar and from the lists; only a text
            // field keeps them to itself.
            KeyCode::Left | KeyCode::Right if on_tab_bar || on_body => {
                self.sort_filter_modal.switch_tab();
            }
            KeyCode::Char('h') | KeyCode::Char('l') if on_tab_bar => {
                self.sort_filter_modal.switch_tab();
            }
            // On the Filters list Enter edits the row under the cursor (or starts
            // a new one on the add row); everywhere else Enter applies.
            // On the Filters tab Enter means add/edit wherever focus sits — the
            // sidebar opens on the tab bar, and Enter closing the dialog from
            // there is how a first filter never gets added. The footer says
            // ^J is the apply here.
            KeyCode::Enter if filter_tab => {
                self.sort_filter_modal.focus = SortFilterFocus::Body;
                let history_limit = self.history_limit;
                self.sort_filter_modal
                    .filter
                    .open_editor(&self.theme, history_limit);
            }
            KeyCode::Enter => return self.apply_sort_filter(),
            // Enter means add/edit on this tab, so apply gets a key that needs
            // no modifier: Ctrl+Enter only exists on terminals speaking the
            // kitty protocol.
            KeyCode::Char('a') if filter_tab => return self.apply_sort_filter(),
            // Columns list: every per-column property, one key each.
            KeyCode::Char(' ') if on_column_list => {
                self.sort_filter_modal.sort.cycle_sort();
            }
            KeyCode::Up | KeyCode::Char('k') if on_body && sort_tab => {
                let s = &mut self.sort_filter_modal.sort;
                if s.focus == SortFocus::ColumnList {
                    let i = match s.table_state.selected() {
                        Some(i) => {
                            if i == 0 {
                                s.filtered_columns().len().saturating_sub(1)
                            } else {
                                i - 1
                            }
                        }
                        None => 0,
                    };
                    s.table_state.select(Some(i));
                }
            }
            KeyCode::Down | KeyCode::Char('j') if on_body && sort_tab => {
                let s = &mut self.sort_filter_modal.sort;
                if s.focus == SortFocus::ColumnList {
                    let i = match s.table_state.selected() {
                        Some(i) => {
                            if i >= s.filtered_columns().len().saturating_sub(1) {
                                0
                            } else {
                                i + 1
                            }
                        }
                        None => 0,
                    };
                    s.table_state.select(Some(i));
                } else {
                    s.focus = SortFocus::ColumnList;
                }
            }
            KeyCode::Char(']') if on_column_list => {
                self.sort_filter_modal.sort.move_selection_down();
            }
            KeyCode::Char('[') if on_column_list => {
                self.sort_filter_modal.sort.move_selection_up();
            }
            KeyCode::Char('+') | KeyCode::Char('=') if on_column_list => {
                self.sort_filter_modal.sort.move_column_display_up();
                self.sort_filter_modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('-') | KeyCode::Char('_') if on_column_list => {
                self.sort_filter_modal.sort.move_column_display_down();
                self.sort_filter_modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('L') if on_column_list => {
                self.sort_filter_modal.sort.toggle_lock_at_column();
                self.sort_filter_modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('v') if on_column_list => {
                self.sort_filter_modal.sort.toggle_visibility();
                self.sort_filter_modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('<' | ',') if on_column_list => {
                self.sort_filter_modal
                    .sort
                    .change_width(WidthChoice::narrower);
            }
            KeyCode::Char('>' | '.') if on_column_list => {
                self.sort_filter_modal.sort.change_width(WidthChoice::wider);
            }
            KeyCode::Char('f') if on_column_list => {
                self.sort_filter_modal
                    .sort
                    .change_width(|_, _| WidthChoice::Fit);
            }
            KeyCode::Char('w') if on_column_list => {
                self.sort_filter_modal
                    .sort
                    .change_width(|_, _| WidthChoice::Auto);
            }
            KeyCode::Char('C') if on_body && sort_tab => {
                self.sort_filter_modal.sort.clear_selection();
            }
            KeyCode::Char(c) if on_column_list && c.is_ascii_digit() => {
                if let Some(digit) = c.to_digit(10) {
                    self.sort_filter_modal
                        .sort
                        .jump_selection_to_order(digit as usize);
                }
            }
            // Filters list: the cursor walks the statements plus the add row.
            KeyCode::Up | KeyCode::Char('k') if on_body && filter_tab => {
                self.sort_filter_modal.filter.move_cursor_up();
            }
            KeyCode::Down | KeyCode::Char('j') if on_body && filter_tab => {
                self.sort_filter_modal.filter.move_cursor_down();
            }
            KeyCode::Char('d') | KeyCode::Delete if on_body && filter_tab => {
                self.sort_filter_modal.filter.delete_at_cursor();
            }
            KeyCode::Delete if on_column_list => {
                self.sort_filter_modal.sort.remove_sort();
            }
            KeyCode::Char(' ') if on_body && filter_tab => {
                self.sort_filter_modal.filter.toggle_logical_at_cursor();
            }
            KeyCode::Char('C') if on_body && filter_tab => {
                self.sort_filter_modal.filter.statements.clear();
                self.sort_filter_modal.filter.cursor = 0;
            }
            _ => {}
        }
        None
    }
}

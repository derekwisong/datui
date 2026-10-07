//! The Sort & Filter sidebar's keys: the shared form keys (`crate::form`), then
//! what each entry does with them, then the list keys (`[` `]` move, `d` removes,
//! and the Columns tab's per-column keys).

use crate::filter_modal::FilterEditStep;
use crate::form::{Form, FormKey, PickerKey};
use crate::sort_filter_modal::SortFilterField;
use crate::widgets::column_widths::WidthChoice;
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys in the sort and filter sidebar.
    pub(crate) fn sort_filter_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The status line is about the last key; this one replaces it.
        self.sort_filter_modal.sort.status = None;

        // Ctrl+J is the apply chord beside Ctrl+Enter: it works on every
        // terminal, and some send Ctrl+Enter as Ctrl+J.
        let apply_chord = ctrl && matches!(event.code, KeyCode::Enter | KeyCode::Char('j'));

        if self.sort_filter_modal.filter.editor.is_some() {
            if apply_chord {
                return self.apply_sort_filter();
            }
            self.filter_editor_key(event);
            return None;
        }

        if let Some(picker) = self.sort_filter_modal.sort_picker.as_mut() {
            match crate::form::picker_key(picker, false, event) {
                PickerKey::Close => self.sort_filter_modal.sort_picker = None,
                PickerKey::Choose | PickerKey::Toggle | PickerKey::ChooseAndMove(_) => {
                    self.sort_filter_modal.choose_sort();
                }
                PickerKey::Handled | PickerKey::Other => {}
            }
            return None;
        }

        let modal = &mut self.sort_filter_modal;
        // The Columns list is a list: it pages, has ends, and ↓ stops at its last
        // column rather than wrapping round the form.
        if let SortFilterField::Column(i) = modal.focus {
            let last = modal.sort.filtered_columns().len().saturating_sub(1);
            let page = modal.sort.page_rows.max(1);
            let to = match event.code {
                KeyCode::Down | KeyCode::Char('j') if i >= last => Some(last),
                KeyCode::PageDown => Some((i + page).min(last)),
                KeyCode::PageUp => Some(i.saturating_sub(page)),
                KeyCode::Home => Some(0),
                KeyCode::End => Some(last),
                _ => None,
            };
            if let Some(to) = to {
                modal.focus(SortFilterField::Column(to));
                return None;
            }
        }
        let from = modal.focus;
        let list_cursor = modal.sort.table_state.selected();
        match crate::form::key(modal, event) {
            // Into the list from find, focus lands where the list's cursor is (the
            // table's column cursor on open), not on its first row.
            FormKey::Moved
                if from == SortFilterField::Find
                    && modal.focus == SortFilterField::Column(0)
                    && let Some(i) = list_cursor =>
            {
                modal.focus(SortFilterField::Column(i));
            }
            FormKey::Cancel => {
                for col in &mut modal.sort.columns {
                    col.is_to_be_locked = false;
                }
                modal.sort.has_unapplied_changes = false;
                modal.close();
                self.input_mode = InputMode::Normal;
            }
            FormKey::Submit => return self.apply_sort_filter(),
            FormKey::Step(SortFilterField::TabBar, _) => modal.switch_tab(),
            FormKey::Step(SortFilterField::Sort(i), _) => modal.sort.flip_sort(i),
            FormKey::Step(SortFilterField::Filter(i), _) => {
                modal.filter.cursor = i;
                modal.filter.toggle_logical_at_cursor();
            }
            FormKey::Step(SortFilterField::Column(_), delta) => {
                if delta < 0 {
                    modal.sort.cycle_sort_back();
                } else {
                    modal.sort.cycle_sort();
                }
            }
            FormKey::Act(SortFilterField::AddSort) => modal.open_sort_picker(),
            FormKey::Act(field @ (SortFilterField::Filter(_) | SortFilterField::AddFilter)) => {
                // The editor opens on the cursor's row: the focused one.
                modal.filter.cursor = match field {
                    SortFilterField::Filter(i) => i,
                    _ => modal.filter.statements.len(),
                };
                let history_limit = self.display.history_limit;
                modal.filter.open_editor(&self.theme, history_limit);
            }
            FormKey::Text(SortFilterField::Find) => {
                let before = modal.sort.filter_input.value().to_string();
                let _ = modal.sort.filter_input.handle_key(event, Some(&self.cache));
                // A narrowed list starts at its first match, where ↓ lands.
                if modal.sort.filter_input.value() != before {
                    modal.sort.table_state.select(Some(0));
                }
            }
            FormKey::Other => return self.sort_filter_list_key(event),
            FormKey::Moved | FormKey::Step(..) | FormKey::Act(_) | FormKey::Text(_) => {}
        }
        None
    }

    /// The keys an entry of the list takes beyond the form's: reorder, remove,
    /// clear, and on the Columns tab every per-column property.
    fn sort_filter_list_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = &mut self.sort_filter_modal;
        let focus = modal.focus;
        let on_entry = matches!(focus, SortFilterField::Sort(_) | SortFilterField::Filter(_));
        let on_column = matches!(focus, SortFilterField::Column(_));
        let in_effect = modal.active_tab == crate::sort_filter_modal::SortFilterTab::InEffect;
        match event.code {
            // What is in effect: one key per change.
            KeyCode::Char('[') if on_entry => modal.move_focused(true),
            KeyCode::Char(']') if on_entry => modal.move_focused(false),
            KeyCode::Char('d') | KeyCode::Delete if on_entry => {
                modal.remove_focused();
            }
            // Clearing acts from the rows, never from the tab bar.
            KeyCode::Char('C') if focus == SortFilterField::TabBar => {}
            KeyCode::Char('C') if in_effect => modal.clear_in_effect(),
            // The Columns list: every per-column property, one key each.
            KeyCode::Char(']') if on_column => modal.sort.move_selection_down(),
            KeyCode::Char('[') if on_column => modal.sort.move_selection_up(),
            KeyCode::Char('+') | KeyCode::Char('=') if on_column => {
                modal.sort.move_column_display_up();
                modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('-') | KeyCode::Char('_') if on_column => {
                modal.sort.move_column_display_down();
                modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('L') if on_column => {
                modal.sort.toggle_lock_at_column();
                modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('v') if on_column => {
                modal.sort.toggle_visibility();
                modal.sort.has_unapplied_changes = true;
            }
            KeyCode::Char('<' | ',') if on_column => {
                modal.sort.change_width(WidthChoice::narrower);
            }
            KeyCode::Char('>' | '.') if on_column => modal.sort.change_width(WidthChoice::wider),
            KeyCode::Char('f') if on_column => {
                modal.sort.change_width(|_, _| WidthChoice::Fit);
            }
            KeyCode::Char('w') if on_column => {
                modal.sort.change_width(|_, _| WidthChoice::Auto);
            }
            KeyCode::Char('C') => modal.sort.clear_selection(),
            KeyCode::Delete if on_column => modal.sort.remove_sort(),
            KeyCode::Char(c) if on_column && c.is_ascii_digit() => {
                if let Some(digit) = c.to_digit(10) {
                    modal.sort.jump_selection_to_order(digit as usize);
                }
            }
            _ => {}
        }
        // A key that moved the cursor's column keeps focus on its row.
        if on_column && let Some(i) = modal.sort.table_state.selected() {
            modal.focus(SortFilterField::Column(i));
        }
        None
    }

    /// The inline filter editor owns the keys while it is up: a small form within
    /// the form. Esc ends the edit and only the edit.
    fn filter_editor_key(&mut self, event: &KeyEvent) {
        let m = &mut self.sort_filter_modal.filter;
        let Some(editor) = m.editor.as_mut() else {
            return;
        };
        let mut committed = false;
        match event.code {
            KeyCode::Esc => m.cancel_editor(),
            // Enter chooses the step's pick; from the value it commits the row.
            // Space chooses too: a space typed into the narrowing filter matches
            // nothing and blanks the list. (The value field below keeps Space for
            // typing.)
            KeyCode::Enter | KeyCode::Tab | KeyCode::Right | KeyCode::Char(' ')
                if editor.step != FilterEditStep::Value =>
            {
                match editor.step {
                    FilterEditStep::Column => {
                        if editor.column.selected_original().is_some() {
                            editor.step = FilterEditStep::Operator;
                            // The operators the chosen column's type takes.
                            m.retarget_operators();
                        }
                    }
                    // A null test has no value to ask for: choosing it commits.
                    FilterEditStep::Operator
                        if editor
                            .selected_operator()
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
            KeyCode::Enter => {
                m.commit_editor();
                committed = true;
            }
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
        if committed {
            // Said now, on the row just saved, rather than when applying.
            self.sort_filter_modal.sort.status = self.filter_problem();
        }
        // A new statement lands on the add row's place; focus follows the cursor.
        let m = &self.sort_filter_modal.filter;
        if m.editor.is_none() {
            let field = if m.on_add_row() {
                SortFilterField::AddFilter
            } else {
                SortFilterField::Filter(m.cursor)
            };
            self.sort_filter_modal.focus(field);
        }
    }
}

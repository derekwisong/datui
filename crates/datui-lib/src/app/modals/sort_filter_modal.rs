//! The Sort & Filter sidebar: what is in effect first — the sorts and the filters,
//! each a row to flip, edit, reorder or remove, and a row to add one — and the
//! Columns tab beside it, every per-column property in one list. One form
//! (`crate::app::form`): the tab bar is its first field, and every entry is a field.

use crate::app::form::{FieldKind, Form};
use crate::app::modals::filter_modal::FilterModal;
use crate::app::modals::sort_modal::SortModal;
use crate::widgets::ui::PickerState;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterTab {
    /// The sorts and filters in effect. Shown as "Sort & Filter".
    #[default]
    InEffect,
    /// Every per-column property: sort, order, lock, visibility, width.
    Columns,
}

/// The sidebar's fields. The entries are numbered: a sort by its place in the
/// sort, a filter by its place in the list, a column by its row in the (found)
/// column list.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterField {
    #[default]
    TabBar,
    Sort(usize),
    AddSort,
    Filter(usize),
    AddFilter,
    /// The Columns tab's find field.
    Find,
    Column(usize),
}

#[derive(Default)]
pub struct SortFilterModal {
    pub active_tab: SortFilterTab,
    pub focus: SortFilterField,
    pub sort: SortModal,
    pub filter: FilterModal,
    /// The column Picker of "add sort", while it is open.
    pub sort_picker: Option<PickerState>,
    /// The table's column cursor when the sidebar opened: where a new sort or
    /// filter starts.
    pub current_column: Option<String>,
}

impl SortFilterModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open on what is in effect, focus on its first entry (the add row when
    /// nothing is). The Columns tab's cursor starts on `current`, the table's
    /// column cursor, which a new sort or filter also starts on.
    pub fn open(
        &mut self,
        history_limit: usize,
        theme: &crate::config::Theme,
        current: Option<&str>,
    ) {
        self.active_tab = SortFilterTab::InEffect;
        self.sort.status = None;
        self.sort.history_limit = history_limit;
        self.sort.filter_input = crate::widgets::text_input::TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        let at = current.and_then(|name| {
            self.sort
                .filtered_columns()
                .iter()
                .position(|(_, c)| c.name == name)
        });
        if let Some(at) = at {
            self.sort.table_state.select(Some(at));
        } else if self.sort.table_state.selected().is_none() && !self.sort.columns.is_empty() {
            self.sort.table_state.select(Some(0));
        }
        self.current_column = current.map(str::to_string);
        self.filter.current_column = current.map(str::to_string);
        self.filter.editor = None;
        self.sort_picker = None;
        self.focus = SortFilterField::TabBar;
        self.focus_next();
    }

    pub fn close(&mut self) {
        self.filter.editor = None;
        self.sort_picker = None;
    }

    pub fn switch_tab(&mut self) {
        self.active_tab = match self.active_tab {
            SortFilterTab::InEffect => SortFilterTab::Columns,
            SortFilterTab::Columns => SortFilterTab::InEffect,
        };
        // An edit in flight belongs to the tab it was typed on.
        self.filter.editor = None;
        self.sort_picker = None;
        self.focus = SortFilterField::TabBar;
    }

    /// Whether the focused field, or an open editor or picker, takes typing.
    pub fn typing(&self) -> bool {
        self.filter.editor.is_some()
            || self.sort_picker.is_some()
            || self.focus == SortFilterField::Find
    }

    /// Open the column Picker that adds a sort, on the table's column cursor. The
    /// columns already sorted are not offered.
    pub fn open_sort_picker(&mut self) {
        let sorted = self.sort.get_sorted_columns();
        let items: Vec<String> = self
            .sort
            .get_full_column_order()
            .into_iter()
            .filter(|name| !sorted.contains(name))
            .collect();
        if items.is_empty() {
            return;
        }
        let mut picker = PickerState::new(items.clone());
        if let Some(i) = self
            .current_column
            .as_ref()
            .and_then(|current| items.iter().position(|c| c == current))
        {
            picker.select_original(i);
        }
        self.sort_picker = Some(picker);
    }

    /// Enter in the add-sort Picker: the cursor's column joins the sort, last and
    /// ascending, and focus lands on it.
    pub fn choose_sort(&mut self) {
        let Some(picker) = self.sort_picker.take() else {
            return;
        };
        let Some(name) = picker
            .selected_original()
            .and_then(|i| picker.items().get(i).cloned())
        else {
            return;
        };
        if let Some(entry) = self.sort.add_sort(&name) {
            self.focus = SortFilterField::Sort(entry);
        }
    }

    /// Remove the focused sort or filter; focus stays on the row that takes its
    /// place, or the add row when it was the last.
    pub fn remove_focused(&mut self) {
        match self.focus {
            SortFilterField::Sort(i) => {
                self.sort.remove_sort_entry(i);
                let n = self.sort.sort_entries().len();
                self.focus = if i < n {
                    SortFilterField::Sort(i)
                } else if n > 0 {
                    SortFilterField::Sort(n - 1)
                } else {
                    SortFilterField::AddSort
                };
            }
            SortFilterField::Filter(i) => {
                self.filter.cursor = i;
                self.filter.delete_at_cursor();
                let n = self.filter.statements.len();
                self.focus = if i < n {
                    SortFilterField::Filter(i)
                } else if n > 0 {
                    SortFilterField::Filter(n - 1)
                } else {
                    SortFilterField::AddFilter
                };
                self.sync_filter_cursor();
            }
            _ => {}
        }
    }

    /// Move the focused sort or filter one place earlier or later; focus goes
    /// with it.
    pub fn move_focused(&mut self, earlier: bool) {
        match self.focus {
            SortFilterField::Sort(i) => {
                let to = self.sort.move_sort_entry(i, earlier);
                self.focus = SortFilterField::Sort(to);
            }
            SortFilterField::Filter(i) => {
                let to = self.filter.move_statement(i, earlier);
                self.focus = SortFilterField::Filter(to);
                self.sync_filter_cursor();
            }
            _ => {}
        }
    }

    /// Drop every sort and filter.
    pub fn clear_in_effect(&mut self) {
        for entry in (0..self.sort.sort_entries().len()).rev() {
            self.sort.remove_sort_entry(entry);
        }
        self.filter.statements.clear();
        self.focus = SortFilterField::AddSort;
        self.sync_filter_cursor();
    }

    /// The filter list's cursor follows focus: a statement, or the add row.
    fn sync_filter_cursor(&mut self) {
        self.filter.cursor = match self.focus {
            SortFilterField::Filter(i) => i.min(self.filter.statements.len()),
            _ => self.filter.statements.len(),
        };
    }
}

impl Form for SortFilterModal {
    type Field = SortFilterField;

    fn fields(&self) -> Vec<(SortFilterField, FieldKind)> {
        let mut fields = vec![(SortFilterField::TabBar, FieldKind::Choice)];
        match self.active_tab {
            SortFilterTab::InEffect => {
                // A sort's value is its direction; a filter's, its and/or. Space
                // flips a sort and opens a filter's editor.
                fields.extend(
                    (0..self.sort.sort_entries().len())
                        .map(|i| (SortFilterField::Sort(i), FieldKind::Choice)),
                );
                fields.push((SortFilterField::AddSort, FieldKind::Button));
                fields.extend((0..self.filter.statements.len()).map(|i| {
                    (
                        SortFilterField::Filter(i),
                        FieldKind::Picker { multi: false },
                    )
                }));
                fields.push((SortFilterField::AddFilter, FieldKind::Button));
            }
            SortFilterTab::Columns => {
                fields.push((SortFilterField::Find, FieldKind::Text));
                // A column's value is its sort: none, ascending, descending.
                fields.extend(
                    (0..self.sort.filtered_columns().len())
                        .map(|i| (SortFilterField::Column(i), FieldKind::Choice)),
                );
            }
        }
        fields
    }

    fn focused(&self) -> SortFilterField {
        self.focus
    }

    fn list_row(&self, field: SortFilterField) -> bool {
        matches!(
            field,
            SortFilterField::Sort(_) | SortFilterField::Filter(_) | SortFilterField::Column(_)
        )
    }

    fn set_focused(&mut self, field: SortFilterField) {
        self.focus = field;
        if let SortFilterField::Column(i) = field {
            self.sort.table_state.select(Some(i));
        }
        self.sync_filter_cursor();
    }
}

#[cfg(test)]
mod tests;

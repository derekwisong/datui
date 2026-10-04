//! The Sort & Filter sidebar: what is in effect first — the sorts and the filters,
//! each a row to flip, edit, reorder or remove, and a row to add one — and the
//! Columns tab beside it, every per-column property in one list. One form
//! (`crate::form`): the tab bar is its first field, and every entry is a field.

use crate::filter_modal::FilterModal;
use crate::form::{FieldKind, Form};
use crate::sort_modal::SortModal;
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
    pub active: bool,
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
        self.active = true;
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
        self.active = false;
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

    fn set_focused(&mut self, field: SortFilterField) {
        self.focus = field;
        if let SortFilterField::Column(i) = field {
            self.sort.table_state.select(Some(i));
        }
        self.sync_filter_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use crate::sort_modal::SortColumn;
    use crate::widgets::column_widths::WidthChoice;

    fn column(name: &str, place: usize, sort: Option<(usize, bool)>) -> SortColumn {
        SortColumn {
            name: name.to_string(),
            sort_order: sort.map(|(o, _)| o),
            sort_descending: sort.is_some_and(|(_, d)| d),
            display_order: place,
            is_locked: false,
            is_to_be_locked: false,
            is_visible: true,
            width: WidthChoice::Auto,
            shown_width: None,
        }
    }

    fn statement(column: &str) -> FilterStatement {
        FilterStatement {
            columns: Vec::new(),
            column: column.to_string(),
            operator: FilterOperator::Eq,
            value: "x".to_string(),
            logical_op: LogicalOperator::And,
        }
    }

    fn modal() -> SortFilterModal {
        let mut m = SortFilterModal::new();
        m.sort.columns = vec![
            column("a", 0, None),
            column("b", 1, Some((2, true))),
            column("c", 2, Some((1, false))),
        ];
        m.filter.statements = vec![statement("a"), statement("c")];
        m.filter.available_columns = vec!["a".into(), "b".into(), "c".into()];
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("theme");
        m.open(10, &theme, Some("a"));
        m
    }

    #[test]
    fn it_opens_on_the_first_entry_in_effect() {
        let m = modal();
        assert_eq!(m.active_tab, SortFilterTab::InEffect);
        assert_eq!(m.focus, SortFilterField::Sort(0));
        let mut empty = SortFilterModal::new();
        let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("theme");
        empty.open(10, &theme, None);
        assert_eq!(empty.focus, SortFilterField::AddSort);
    }

    #[test]
    fn the_rows_walk_sorts_then_filters_then_back_to_the_tabs() {
        let mut m = modal();
        let walked: Vec<SortFilterField> = (0..7)
            .map(|_| {
                let at = m.focus;
                m.focus_next();
                at
            })
            .collect();
        assert_eq!(
            walked,
            [
                SortFilterField::Sort(0),
                SortFilterField::Sort(1),
                SortFilterField::AddSort,
                SortFilterField::Filter(0),
                SortFilterField::Filter(1),
                SortFilterField::AddFilter,
                SortFilterField::TabBar,
            ]
        );
    }

    #[test]
    fn a_sort_entry_flips_moves_and_goes() {
        let mut m = modal();
        // Sort(0) is c, ascending.
        m.sort.flip_sort(0);
        assert_eq!(
            m.sort.sorted_columns_and_directions(),
            (vec!["c".to_string(), "b".to_string()], vec![true, true])
        );
        m.move_focused(false);
        assert_eq!(m.focus, SortFilterField::Sort(1), "focus goes with it");
        assert_eq!(m.sort.get_sorted_columns(), ["b", "c"]);
        m.remove_focused();
        assert_eq!(m.sort.get_sorted_columns(), ["b"]);
        assert_eq!(m.focus, SortFilterField::Sort(0));
        m.remove_focused();
        assert_eq!(m.focus, SortFilterField::AddSort);
    }

    #[test]
    fn adding_a_sort_offers_the_unsorted_columns_from_the_cursor() {
        let mut m = modal();
        m.focus = SortFilterField::AddSort;
        m.open_sort_picker();
        let picker = m.sort_picker.as_ref().expect("open");
        assert_eq!(picker.items(), ["a"], "b and c are sorted already");
        m.choose_sort();
        assert_eq!(m.sort.get_sorted_columns(), ["c", "b", "a"]);
        assert_eq!(m.focus, SortFilterField::Sort(2));
    }

    #[test]
    fn a_filter_entry_moves_and_goes() {
        let mut m = modal();
        m.set_focused(SortFilterField::Filter(1));
        assert_eq!(m.filter.cursor, 1);
        m.move_focused(true);
        assert_eq!(m.filter.statements[0].column, "c");
        assert_eq!(m.focus, SortFilterField::Filter(0));
        m.remove_focused();
        assert_eq!(m.filter.statements.len(), 1);
        m.remove_focused();
        assert_eq!(m.focus, SortFilterField::AddFilter);
        assert!(m.filter.on_add_row());
    }

    #[test]
    fn the_columns_tab_walks_find_then_every_column() {
        let mut m = modal();
        m.switch_tab();
        assert_eq!(m.focus, SortFilterField::TabBar);
        m.focus_next();
        assert_eq!(m.focus, SortFilterField::Find);
        m.focus_next();
        m.focus_next();
        assert_eq!(m.focus, SortFilterField::Column(1));
        assert_eq!(
            m.sort.table_state.selected(),
            Some(1),
            "the list cursor follows"
        );
    }

    #[test]
    fn clearing_drops_every_sort_and_filter() {
        let mut m = modal();
        m.clear_in_effect();
        assert!(m.sort.get_sorted_columns().is_empty());
        assert!(m.filter.statements.is_empty());
        assert_eq!(m.focus, SortFilterField::AddSort);
    }
}

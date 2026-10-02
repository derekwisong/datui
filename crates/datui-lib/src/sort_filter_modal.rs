//! Combined Sort & Filter sidebar with a Columns tab and a Filters tab.

use crate::filter_modal::FilterModal;
use crate::sort_modal::{SortFocus, SortModal};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterTab {
    /// Every per-column property: sort, order, lock, visibility. Shown as "Columns".
    #[default]
    Sort,
    Filter,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterFocus {
    #[default]
    TabBar,
    Body,
}

#[derive(Default)]
pub struct SortFilterModal {
    pub active: bool,
    pub active_tab: SortFilterTab,
    pub focus: SortFilterFocus,
    pub sort: SortModal,
    pub filter: FilterModal,
}

impl SortFilterModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open on the Columns tab with its cursor on `current`, the table's column
    /// cursor, which a new filter also starts on.
    pub fn open(
        &mut self,
        history_limit: usize,
        theme: &crate::config::Theme,
        current: Option<&str>,
    ) {
        self.active = true;
        self.active_tab = SortFilterTab::Sort;
        self.focus = SortFilterFocus::TabBar;
        self.sort.focus = SortFocus::ColumnList;
        self.sort.status = None;
        self.sort.history_limit = history_limit;
        self.sort.filter_input = crate::widgets::text_input::TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        // The render draws the cursor rail at `selected().unwrap_or(0)`; select
        // the row for real, or Space/L/v on the first row silently do nothing
        // until an arrow press makes the shown cursor true.
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
        self.filter.current_column = current.map(str::to_string);
        // The cursor is where `sync_sort_filter_modal` put it: on the add row.
        self.filter.editor = None;
    }

    pub fn close(&mut self) {
        self.active = false;
        self.filter.editor = None;
    }

    pub fn switch_tab(&mut self) {
        self.active_tab = match self.active_tab {
            SortFilterTab::Sort => SortFilterTab::Filter,
            SortFilterTab::Filter => SortFilterTab::Sort,
        };
        // An edit in flight belongs to the tab it was typed on.
        self.filter.editor = None;
    }

    pub fn next_focus(&mut self) {
        match self.focus {
            SortFilterFocus::TabBar => {
                self.focus = SortFilterFocus::Body;
                if self.active_tab == SortFilterTab::Sort {
                    self.sort.focus = SortFocus::Filter;
                }
            }
            SortFilterFocus::Body => {
                let at_end = match self.active_tab {
                    SortFilterTab::Sort => self.sort.next_body_focus(),
                    // The Filters tab's body is one list.
                    SortFilterTab::Filter => true,
                };
                if at_end {
                    self.focus = SortFilterFocus::TabBar;
                }
            }
        }
    }

    pub fn prev_focus(&mut self) {
        match self.focus {
            SortFilterFocus::TabBar => {
                self.focus = SortFilterFocus::Body;
                if self.active_tab == SortFilterTab::Sort {
                    self.sort.focus = SortFocus::ColumnList;
                }
            }
            SortFilterFocus::Body => {
                let at_start = match self.active_tab {
                    SortFilterTab::Sort => self.sort.prev_body_focus(),
                    SortFilterTab::Filter => true,
                };
                if at_start {
                    self.focus = SortFilterFocus::TabBar;
                }
            }
        }
    }
}

//! Combined Sort & Filter modal with tabs.

use crate::filter_modal::{FilterFocus, FilterModal};
use crate::sort_modal::{SortFocus, SortModal};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterTab {
    #[default]
    Sort,
    Filter,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SortFilterFocus {
    #[default]
    TabBar,
    Body,
    Apply,
    Cancel,
    Clear,
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

    pub fn open(&mut self) {
        self.active = true;
        self.active_tab = SortFilterTab::Sort;
        self.focus = SortFilterFocus::TabBar;
        self.sort.focus = SortFocus::ColumnList;
        self.filter.focus = FilterFocus::Column;
    }

    pub fn close(&mut self) {
        self.active = false;
    }

    pub fn switch_tab(&mut self) {
        self.active_tab = match self.active_tab {
            SortFilterTab::Sort => SortFilterTab::Filter,
            SortFilterTab::Filter => SortFilterTab::Sort,
        };
    }

    pub fn next_focus(&mut self) {
        match self.focus {
            SortFilterFocus::TabBar => {
                self.focus = SortFilterFocus::Body;
                match self.active_tab {
                    SortFilterTab::Sort => self.sort.focus = SortFocus::Filter,
                    SortFilterTab::Filter => self.filter.focus = FilterFocus::Column,
                }
            }
            SortFilterFocus::Body => {
                let at_end = match self.active_tab {
                    SortFilterTab::Sort => self.sort.next_body_focus(),
                    SortFilterTab::Filter => self.filter.next_body_focus(),
                };
                if at_end {
                    self.focus = SortFilterFocus::Apply;
                }
            }
            SortFilterFocus::Apply => self.focus = SortFilterFocus::Cancel,
            SortFilterFocus::Cancel => self.focus = SortFilterFocus::Clear,
            SortFilterFocus::Clear => {
                self.focus = SortFilterFocus::TabBar;
            }
        }
    }

    pub fn prev_focus(&mut self) {
        match self.focus {
            SortFilterFocus::TabBar => {
                self.focus = SortFilterFocus::Clear;
            }
            SortFilterFocus::Body => {
                let at_start = match self.active_tab {
                    SortFilterTab::Sort => self.sort.prev_body_focus(),
                    SortFilterTab::Filter => self.filter.prev_body_focus(),
                };
                if at_start {
                    self.focus = SortFilterFocus::TabBar;
                }
            }
            SortFilterFocus::Apply => {
                self.focus = SortFilterFocus::Body;
                match self.active_tab {
                    SortFilterTab::Sort => self.sort.focus = SortFocus::Order,
                    SortFilterTab::Filter => self.filter.focus = FilterFocus::Statements,
                }
            }
            SortFilterFocus::Cancel => self.focus = SortFilterFocus::Apply,
            SortFilterFocus::Clear => self.focus = SortFilterFocus::Cancel,
        }
    }
}

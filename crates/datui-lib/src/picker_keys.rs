//! The three pickers: go to a column, read as a format, and switch to another table
//! of the same source.

use crate::open_options::OpenOptions;
use crate::{App, AppEvent, Overlay, table_switch};
use crossterm::event::{KeyCode, KeyEvent};

/// The go-to-column, format and table pickers.
#[derive(Default)]
pub struct Pickers {
    /// The shown columns, narrowed by what is typed, while `g` is choosing one.
    pub go_to_column: crate::widgets::ui::PickerState,
    /// The specs `b` offers for the dataset on screen.
    pub format_picker: crate::widgets::ui::PickerState,
    /// The tables `T` offers: the picker's lines, and what each opens.
    pub table_picker: crate::widgets::ui::PickerState,
    pub table_choices: Option<crate::table_switch::Tables>,
}

impl App {
    /// `g` at the table: pick a shown column by name, bring it on screen and put the
    /// column cursor on it. Starts on the cursor's column, so ↑↓ move from there.
    pub(crate) fn open_go_to_column(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let names = state.get_column_order().to_vec();
        if names.is_empty() {
            return;
        }
        let at = state
            .current_column()
            .and_then(|current| names.iter().position(|n| n == current))
            .unwrap_or(0);
        self.pickers.go_to_column = crate::widgets::ui::PickerState::new(names);
        self.pickers.go_to_column.select_original(at);
        self.open_overlay(Overlay::GoToColumn);
    }

    /// The column picker owns the keys: type to narrow, ↑↓ move, Enter goes, Esc
    /// closes without moving.
    pub(crate) fn go_to_column_key(&mut self, event: &KeyEvent) {
        match event.code {
            KeyCode::Esc => self.close_overlay(),
            KeyCode::Enter => {
                let Some(index) = self.pickers.go_to_column.selected_original() else {
                    // Nothing matches; the picker says so and stays.
                    return;
                };
                // By name: the order may have changed under the picker since it opened.
                let name = self.pickers.go_to_column.items()[index].clone();
                if let Some(state) = self.data_table_state.as_mut() {
                    state.go_to_column(&name);
                }
                self.close_overlay();
            }
            KeyCode::Up => self.pickers.go_to_column.move_up(),
            KeyCode::Down => self.pickers.go_to_column.move_down(),
            KeyCode::Backspace => self.pickers.go_to_column.backspace(),
            KeyCode::Char(c) => self.pickers.go_to_column.filter_key(c, event.modifiers),
            _ => {}
        }
    }

    /// `b` at a table read through a format spec: the specs that could read it, the
    /// ones that matched first, to read it again with another.
    pub(crate) fn open_format_picker(&mut self) {
        let Some(read) = self
            .data_table_state
            .as_ref()
            .and_then(|s| s.format_read())
            .cloned()
        else {
            // Only a file read through a spec has a format to pick.
            return;
        };
        let mut names = vec![read.spec.name.clone()];
        names.extend(read.also.iter().cloned());
        for found in &self.formats.specs {
            if found.spec.layout == read.spec.layout
                && !found.spec.is_delimited()
                && !names.contains(&found.spec.name)
            {
                names.push(found.spec.name.clone());
            }
        }
        self.pickers.format_picker = crate::widgets::ui::PickerState::new(names);
        self.open_overlay(Overlay::PickFormat);
    }

    /// The format picker owns the keys: type to narrow, ↑↓ move, Enter reads the file
    /// again with the spec chosen, Esc closes.
    pub(crate) fn format_picker_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match event.code {
            KeyCode::Esc => self.close_overlay(),
            KeyCode::Enter => {
                let index = self.pickers.format_picker.selected_original()?;
                let name = self.pickers.format_picker.items()[index].clone();
                self.close_overlay();
                let current = self
                    .data_table_state
                    .as_ref()
                    .and_then(|s| s.format_read())
                    .map(|read| read.spec.name.clone());
                if current.as_deref() == Some(name.as_str()) {
                    return None;
                }
                let (paths, options) = self.source.opened.clone()?;
                let options = OpenOptions {
                    spec_name: Some(name),
                    spec_file: None,
                    spec_fetched: None,
                    table: None,
                    format_read: None,
                    sqlite: None,
                    format: None,
                    ..options
                };
                self.set_loading_phase("Scanning input", 10);
                self.name_what_is_loading(paths[0].clone());
                return Some(AppEvent::Open(paths, options));
            }
            KeyCode::Up => self.pickers.format_picker.move_up(),
            KeyCode::Down => self.pickers.format_picker.move_down(),
            KeyCode::Backspace => self.pickers.format_picker.backspace(),
            KeyCode::Char(c) => self.pickers.format_picker.filter_key(c, event.modifiers),
            _ => {}
        }
        None
    }

    /// The tables of the source on screen, from what its open holds; `None` for a
    /// source of one.
    pub fn sibling_tables(&self) -> Option<table_switch::Tables> {
        let state = self.data_table_state.as_ref()?;
        let (paths, options) = self.source.opened.as_ref()?;
        table_switch::of(state, paths, options)
    }

    /// Whether the source on screen has another table for `T` to open.
    pub fn offers_other_tables(&self) -> bool {
        match (self.data_table_state.as_ref(), self.source.opened.as_ref()) {
            (Some(state), Some((paths, options))) => table_switch::several(state, paths, options),
            _ => false,
        }
    }

    /// `T` at the table: the source's tables, the one on screen marked, to open
    /// another. A source of one says so.
    pub(crate) fn open_table_picker(&mut self) {
        let Some(tables) = self.sibling_tables().filter(table_switch::Tables::several) else {
            self.flash_note("Only one table here".to_string());
            return;
        };
        let labels = tables.tables.iter().map(|t| t.label.clone()).collect();
        self.pickers.table_picker = crate::widgets::ui::PickerState::new(labels);
        if let Some(at) = tables.current {
            self.pickers.table_picker.select_original(at);
        }
        self.pickers.table_choices = Some(tables);
        self.open_overlay(Overlay::PickTable);
    }

    /// The table picker owns the keys: type to narrow, ↑↓ move, Enter opens the table
    /// chosen, Esc closes.
    pub(crate) fn table_picker_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match event.code {
            KeyCode::Esc => self.close_overlay(),
            KeyCode::Enter => {
                let index = self.pickers.table_picker.selected_original()?;
                let tables = self.pickers.table_choices.take()?;
                self.close_overlay();
                if tables.current == Some(index) {
                    return None;
                }
                let table = tables.tables.get(index)?.table.clone();
                return self.switch_table(table);
            }
            KeyCode::Up => self.pickers.table_picker.move_up(),
            KeyCode::Down => self.pickers.table_picker.move_down(),
            KeyCode::Backspace => self.pickers.table_picker.backspace(),
            KeyCode::Char(c) => self.pickers.table_picker.filter_key(c, event.modifiers),
            _ => {}
        }
        None
    }

    /// Open `table` of the file on screen in its place (`None`: the whole file), as
    /// `--table` or home's row for it would: the query, filters and sort go with the
    /// table they were on, recents record it, and a view for it applies.
    pub(crate) fn switch_table(&mut self, table: Option<String>) -> Option<AppEvent> {
        let (paths, options) = self.source.opened.clone()?;
        let shown = match &table {
            Some(name) => crate::members::place(&paths[0], name),
            None => paths[0].clone(),
        };
        let options = OpenOptions {
            table,
            // `--view` was for the first open; a view for this table applies as on
            // any open.
            view: None,
            prepared: None,
            ..options
        };
        self.set_loading_phase("Scanning input", 10);
        self.name_what_is_loading(shown);
        Some(AppEvent::Open(paths, options))
    }
}

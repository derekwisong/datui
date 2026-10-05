//! The info panel's keys.

use crate::widgets::info::InfoTab;
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the info panel.
    pub(crate) fn info_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let schema_tab = self.info_modal.active_tab == InfoTab::Schema;
        let notes_tab = self.info_modal.active_tab == InfoTab::Notes;
        let documentation_tab = self.info_modal.active_tab == InfoTab::Documentation
            && self.info_documentation.is_open();
        // The Metadata and the file's own tab scroll their lists the same way.
        let detail_tab = matches!(
            self.info_modal.active_tab,
            InfoTab::Metadata | InfoTab::Format
        );
        let notes = self
            .data_table_state
            .as_ref()
            .map(|s| s.notes().len())
            .unwrap_or(0);
        let total_rows = self
            .data_table_state
            .as_ref()
            .map(|s| s.schema().len())
            .unwrap_or(0);
        let visible = self.info_modal.schema_visible_height;

        match event.code {
            KeyCode::Esc | KeyCode::Char('i') if event.is_press() => {
                self.info_modal.close();
                self.input_mode = InputMode::Normal;
            }
            // The file's bytes, in the hex view; Esc there comes back to the table.
            // The rows counted exactly, where they are an estimate.
            KeyCode::Char('c') if event.is_press() && self.row_estimate().is_some() => {
                self.count_exactly();
            }
            KeyCode::Char('x') if event.is_press() => {
                if let Some(path) = self.hex_target() {
                    self.info_modal.close();
                    self.input_mode = InputMode::Normal;
                    self.open_hex(path, crate::hex_view::Origin::Table, false, None);
                }
            }
            // Delimited text: read the first row as data, or as names again. The read
            // takes the screen, so the panel closes for it.
            KeyCode::Char('H') if event.is_press() && schema_tab => {
                if self.header_toggle_offered() {
                    self.info_modal.close();
                    self.input_mode = InputMode::Normal;
                    return self.toggle_header();
                }
            }
            // The tabs switch from anywhere in the panel, which is a viewer, not a
            // form: its body always has the keys, so Tab and the arrows across have
            // no field to move between and step the tabs instead.
            KeyCode::Left | KeyCode::Char('h') | KeyCode::BackTab if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab_prev(offered);
            }
            KeyCode::Right | KeyCode::Char('l') | KeyCode::Tab if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab(offered);
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() && schema_tab => {
                self.info_modal.schema_table_down(total_rows, visible);
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() && schema_tab => {
                self.info_modal.schema_table_up(total_rows, visible);
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() && documentation_tab => {
                self.info_documentation.move_cursor(1);
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() && documentation_tab => {
                self.info_documentation.move_cursor(-1);
            }
            KeyCode::PageDown if event.is_press() && documentation_tab => {
                let page = self.info_documentation.view_height.max(1) as isize;
                self.info_documentation.move_cursor(page);
            }
            KeyCode::PageUp if event.is_press() && documentation_tab => {
                let page = self.info_documentation.view_height.max(1) as isize;
                self.info_documentation.move_cursor(-page);
            }
            KeyCode::Enter | KeyCode::Char(' ') if event.is_press() && documentation_tab => {
                self.info_documentation.toggle_legend();
            }
            KeyCode::Char('y') if event.is_press() && documentation_tab => {
                match self.info_documentation.copy_text() {
                    Some(text) => self.copy_documentation_text(text),
                    None => self.flash_note("Nothing to copy on this line".to_string()),
                }
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() && notes_tab => {
                self.info_modal.notes_move(1, notes);
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() && notes_tab => {
                self.info_modal.notes_move(-1, notes);
            }
            KeyCode::Enter if event.is_press() && notes_tab => {
                self.read_the_selected_note_s_column_as_text();
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() && detail_tab => {
                self.info_modal.detail_scroll_by(1);
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() && detail_tab => {
                self.info_modal.detail_scroll_by(-1);
            }
            KeyCode::PageDown if event.is_press() && detail_tab => {
                self.info_modal.detail_page(true);
            }
            KeyCode::PageUp if event.is_press() && detail_tab => {
                self.info_modal.detail_page(false);
            }
            KeyCode::Home if event.is_press() && detail_tab => {
                self.info_modal.detail_scroll = 0;
            }
            KeyCode::End if event.is_press() && detail_tab => {
                // The render clamps it to the last page.
                self.info_modal.detail_scroll = usize::MAX;
            }
            _ => {}
        }
        None
    }
}

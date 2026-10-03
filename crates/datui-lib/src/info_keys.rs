//! The info panel's keys.

use crate::widgets::info::{InfoFocus, InfoTab};
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the info panel.
    pub(crate) fn info_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let on_body = self.info_modal.focus == InfoFocus::Body;
        let schema_tab = self.info_modal.active_tab == InfoTab::Schema;
        let notes_tab = self.info_modal.active_tab == InfoTab::Notes;
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
            KeyCode::Char('x') if event.is_press() => {
                if let Some(path) = self.hex_target() {
                    self.info_modal.close();
                    self.input_mode = InputMode::Normal;
                    self.open_hex(path, crate::hex_view::Origin::Table, false, None);
                }
            }
            KeyCode::Tab if event.is_press() && schema_tab => {
                self.info_modal.next_focus();
            }
            KeyCode::BackTab if event.is_press() && schema_tab => {
                self.info_modal.prev_focus();
            }
            // From anywhere in the panel: the arrows have no other job on any tab's
            // body, and the panel opens with the body focused, so gating them on
            // tab-bar focus made a fresh `i` then `→` do nothing.
            KeyCode::Left | KeyCode::Char('h') if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab_prev(offered);
            }
            KeyCode::Right | KeyCode::Char('l') if event.is_press() => {
                let offered = self.info_tabs_on_offer();
                self.info_modal.switch_tab(offered);
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() && on_body && schema_tab => {
                self.info_modal.schema_table_down(total_rows, visible);
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() && on_body && schema_tab => {
                self.info_modal.schema_table_up(total_rows, visible);
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

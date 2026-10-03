//! The copy modal's keys.

use crate::{App, AppEvent, InputMode, copy_modal};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the copy modal.
    pub(crate) fn copy_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let picker_open = self.copy_modal.picker.is_some();
        if event.code == KeyCode::Char('?') && !picker_open {
            self.show_help = true;
            return None;
        }

        // The open Picker owns the keys: type to narrow, ↑↓ move, Enter
        // chooses, and Esc backs out of the Picker and only the Picker.
        if picker_open {
            match event.code {
                KeyCode::Esc => self.copy_modal.picker = None,
                KeyCode::Enter => self.copy_modal.picker_choose(),
                KeyCode::Tab => {
                    self.copy_modal.picker_choose();
                    self.copy_modal.next_focus();
                }
                KeyCode::BackTab => {
                    self.copy_modal.picker_choose();
                    self.copy_modal.prev_focus();
                }
                KeyCode::Up => {
                    if let Some(picker) = self.copy_modal.picker.as_mut() {
                        picker.move_up();
                    }
                }
                KeyCode::Down => {
                    if let Some(picker) = self.copy_modal.picker.as_mut() {
                        picker.move_down();
                    }
                }
                // Every row here picks one, so Space chooses like Enter; a
                // typed space would narrow the list to nothing.
                KeyCode::Char(' ') => self.copy_modal.picker_choose(),
                KeyCode::Backspace => {
                    if let Some(picker) = self.copy_modal.picker.as_mut() {
                        picker.backspace();
                    }
                }
                KeyCode::Char(c) => {
                    if let Some(picker) = self.copy_modal.picker.as_mut() {
                        picker.filter_key(c, event.modifiers);
                    }
                }
                _ => {}
            }
            return None;
        }

        // Whatever this key does, the form is being edited again: the
        // re-accented gap line goes back to plain (Enter below re-arms it).
        self.copy_modal.attention = false;

        match event.code {
            KeyCode::Esc => {
                self.copy_modal.close();
                self.input_mode = InputMode::Normal;
            }
            // Enter copies from anywhere in the form; what it will do has
            // been echoed on the spec line all along.
            KeyCode::Enter => {
                if self.copy_modal.validation_error().is_some() {
                    self.copy_modal.attention = true;
                    return None;
                }
                return self.perform_copy();
            }
            KeyCode::Tab | KeyCode::Down | KeyCode::Char('j') => self.copy_modal.next_focus(),
            KeyCode::BackTab | KeyCode::Up | KeyCode::Char('k') => self.copy_modal.prev_focus(),
            KeyCode::Char(' ') => {
                if self.copy_modal.focus == copy_modal::CopyFocus::Header {
                    self.copy_modal.toggle_header();
                } else {
                    self.copy_modal.open_picker();
                }
            }
            _ => {}
        }
        None
    }
}

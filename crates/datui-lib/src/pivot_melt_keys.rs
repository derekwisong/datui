//! The pivot and melt modal's keys.

use crate::pivot_melt_modal::{PivotMeltFocus, PivotMeltTab};
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys in the pivot and melt modal.
    pub(crate) fn pivot_melt_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        // Acts at once (see `hard_escape_while_busy`), ahead of the keys held
        // behind the pivot; a second Esc closes the form.
        if event.code == KeyCode::Esc && self.pivot_computing() {
            self.cancel_pivot();
            return None;
        }
        let picker_open = self.pivot_melt_modal.picker.is_some();
        let text_focus = !picker_open
            && self
                .pivot_melt_modal
                .is_text_row(self.pivot_melt_modal.focus);
        let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
        if event.code == KeyCode::Char('?') && (ctrl_help || (!text_focus && !picker_open)) {
            self.open_help_overlay();
            return None;
        }

        // The open Picker owns the keys: type to narrow, ↑↓ move, Space
        // toggles on a several-choice row, Enter chooses, and Esc backs
        // out of the Picker and only the Picker.
        if picker_open {
            match event.code {
                KeyCode::Esc => self.pivot_melt_modal.picker = None,
                KeyCode::Enter => self.pivot_melt_modal.picker_choose(),
                KeyCode::Tab => {
                    self.pivot_melt_modal.picker_choose();
                    self.pivot_melt_modal.next_focus();
                }
                KeyCode::BackTab => {
                    self.pivot_melt_modal.picker_choose();
                    self.pivot_melt_modal.prev_focus();
                }
                KeyCode::Up => {
                    if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                        picker.move_up();
                    }
                }
                KeyCode::Down => {
                    if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                        picker.move_down();
                    }
                }
                KeyCode::Char(' ')
                    if self
                        .pivot_melt_modal
                        .is_multi_row(self.pivot_melt_modal.focus) =>
                {
                    self.pivot_melt_modal.picker_toggle();
                }
                // On a pick-one row Space chooses like Enter. It must
                // never reach the narrowing filter: a typed space matches
                // nothing, and the list blanking under the key that just
                // opened it reads as breakage.
                KeyCode::Char(' ') => self.pivot_melt_modal.picker_choose(),
                KeyCode::Backspace => {
                    if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                        picker.backspace();
                    }
                }
                KeyCode::Char(c) => {
                    if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                        picker.filter_key(c, event.modifiers);
                    }
                }
                _ => {}
            }
            return None;
        }

        // Whatever this key does, the form is being edited again: the
        // re-accented gap line goes back to plain (Enter below re-arms it).
        self.pivot_melt_modal.attention = false;

        match event.code {
            KeyCode::Esc => {
                self.pivot_melt_modal.close();
                self.input_mode = InputMode::Normal;
            }
            // Enter applies from anywhere in the form; what it will do has
            // been echoed on the spec line all along.
            KeyCode::Enter => {
                return match self.pivot_melt_modal.active_tab {
                    PivotMeltTab::Pivot => {
                        if self.pivot_melt_modal.pivot_validation_error().is_some() {
                            // The spec line already names the gap; it
                            // re-accents rather than a modal repeating it.
                            self.pivot_melt_modal.attention = true;
                            None
                        } else {
                            self.pivot_melt_modal
                                .build_pivot_spec()
                                .map(AppEvent::Pivot)
                        }
                    }
                    PivotMeltTab::Melt => {
                        if self.pivot_melt_modal.melt_validation_error().is_some() {
                            self.pivot_melt_modal.attention = true;
                            None
                        } else {
                            self.pivot_melt_modal.build_melt_spec().map(AppEvent::Melt)
                        }
                    }
                };
            }
            KeyCode::Tab | KeyCode::Down => self.pivot_melt_modal.next_focus(),
            KeyCode::BackTab | KeyCode::Up => self.pivot_melt_modal.prev_focus(),
            // Arrows switch tabs from the tab bar and the picked rows; a
            // text row keeps them for its cursor.
            KeyCode::Left | KeyCode::Right if !text_focus => {
                self.pivot_melt_modal.switch_tab();
            }
            KeyCode::Char('h') | KeyCode::Char('l')
                if self.pivot_melt_modal.focus == PivotMeltFocus::TabBar =>
            {
                self.pivot_melt_modal.switch_tab();
            }
            // A picked row edits through the Picker scoped to that row
            // alone: Space opens it, typing opens it already narrowed.
            KeyCode::Char(' ')
                if self
                    .pivot_melt_modal
                    .is_picker_row(self.pivot_melt_modal.focus) =>
            {
                self.pivot_melt_modal.open_picker();
            }
            KeyCode::Char(c)
                if self
                    .pivot_melt_modal
                    .is_picker_row(self.pivot_melt_modal.focus) =>
            {
                // Only a plain character opens the picker by typing;
                // a chord is a chord, not the first letter of a search.
                if event
                    .modifiers
                    .intersection(KeyModifiers::CONTROL | KeyModifiers::ALT)
                    .is_empty()
                {
                    self.pivot_melt_modal.open_picker();
                    if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                        picker.type_char(c);
                    }
                }
            }
            // A text row is an ordinary text field, readline included.
            _ if text_focus => {
                if let Some(input) = self.pivot_melt_modal.focused_text_input_mut() {
                    let _ = input.handle_key(event, None);
                }
            }
            _ => {}
        }
        None
    }
}

//! The copy modal's keys: the shared form keys (`crate::form`), then what each row
//! does with them.

use crate::copy_modal::CopyFocus;
use crate::form::{FormKey, PickerKey};
use crate::{App, AppEvent, InputMode};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the copy modal.
    pub(crate) fn copy_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if let Some(picker) = self.copy_modal.picker.as_mut() {
            match crate::form::picker_key(picker, false, event) {
                PickerKey::Close => self.copy_modal.picker = None,
                PickerKey::Choose | PickerKey::Toggle => self.copy_modal.picker_choose(),
                PickerKey::ChooseAndMove(forward) => {
                    self.copy_modal.picker_choose();
                    crate::form::Form::move_focus(
                        &mut self.copy_modal,
                        if forward { 1 } else { -1 },
                    );
                }
                PickerKey::Handled | PickerKey::Other => {}
            }
            return None;
        }

        // Whatever this key does, the form is being edited again: the re-accented
        // gap line goes back to plain (Enter below re-arms it).
        self.copy_modal.attention = false;

        match crate::form::key(&mut self.copy_modal, event) {
            FormKey::Cancel => {
                self.copy_modal.close();
                self.input_mode = InputMode::Normal;
            }
            // Enter copies from anywhere in the form; what it will do has been
            // echoed on the spec line all along.
            FormKey::Submit => {
                if self.copy_modal.validation_error().is_some() {
                    self.copy_modal.attention = true;
                    return None;
                }
                return self.perform_copy();
            }
            FormKey::Step(CopyFocus::Scope, delta) => self.copy_modal.step_scope(delta),
            FormKey::Step(CopyFocus::Format, delta) => self.copy_modal.step_format(delta),
            FormKey::Step(CopyFocus::Column, delta) => self.copy_modal.step_column(delta),
            FormKey::Act(CopyFocus::Header) => self.copy_modal.toggle_header(),
            FormKey::Act(CopyFocus::Column) => self.copy_modal.open_picker(),
            FormKey::Other if event.code == KeyCode::Char('?') => self.open_help_overlay(),
            _ => {}
        }
        None
    }
}

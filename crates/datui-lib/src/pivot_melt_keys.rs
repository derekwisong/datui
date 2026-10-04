//! The pivot and melt modal's keys: the shared form keys (`crate::form`), then what
//! each row does with them.

use crate::form::{FormKey, PickerKey};
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
        if event.code == KeyCode::Char('?') && event.modifiers.contains(KeyModifiers::CONTROL) {
            self.open_help_overlay();
            return None;
        }

        let multi = self
            .pivot_melt_modal
            .is_multi_row(self.pivot_melt_modal.focus);
        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
            match crate::form::picker_key(picker, multi, event) {
                PickerKey::Close => self.pivot_melt_modal.picker = None,
                PickerKey::Choose => self.pivot_melt_modal.picker_choose(),
                PickerKey::Toggle => self.pivot_melt_modal.picker_toggle(),
                PickerKey::ChooseAndMove(forward) => {
                    self.pivot_melt_modal.picker_choose();
                    crate::form::Form::move_focus(
                        &mut self.pivot_melt_modal,
                        if forward { 1 } else { -1 },
                    );
                }
                PickerKey::Handled | PickerKey::Other => {}
            }
            return None;
        }

        // Whatever this key does, the form is being edited again: the
        // re-accented gap line goes back to plain (Enter below re-arms it).
        self.pivot_melt_modal.attention = false;

        match crate::form::key(&mut self.pivot_melt_modal, event) {
            FormKey::Cancel => {
                self.pivot_melt_modal.close();
                self.input_mode = InputMode::Normal;
            }
            // Enter applies from anywhere in the form; what it will do has
            // been echoed on the spec line all along.
            FormKey::Submit => return self.submit_pivot_melt(),
            FormKey::Step(PivotMeltFocus::TabBar, _) => self.pivot_melt_modal.switch_tab(),
            FormKey::Step(row, delta) => {
                if self.pivot_melt_modal.is_choice_row(row) {
                    self.pivot_melt_modal.step_choice(delta);
                } else {
                    self.pivot_melt_modal.step_picker_row(delta);
                }
            }
            // A column row edits through the Picker scoped to that row alone.
            FormKey::Act(_) => self.pivot_melt_modal.open_picker(),
            // A text row is an ordinary text field, readline included.
            FormKey::Text(_) => {
                if let Some(input) = self.pivot_melt_modal.focused_text_input_mut() {
                    let _ = input.handle_key(event, None);
                }
            }
            FormKey::Other if event.code == KeyCode::Char('?') => self.open_help_overlay(),
            FormKey::Moved | FormKey::Other => {}
        }
        None
    }

    /// Enter: run the staged pivot or melt, or re-accent the spec line that names
    /// what is missing rather than a modal repeating it.
    fn submit_pivot_melt(&mut self) -> Option<AppEvent> {
        let modal = &mut self.pivot_melt_modal;
        match modal.active_tab {
            PivotMeltTab::Pivot => {
                if modal.pivot_validation_error().is_some() {
                    modal.attention = true;
                    None
                } else {
                    modal.build_pivot_spec().map(AppEvent::Pivot)
                }
            }
            PivotMeltTab::Melt => {
                if modal.melt_validation_error().is_some() {
                    modal.attention = true;
                    None
                } else {
                    modal.build_melt_spec().map(AppEvent::Melt)
                }
            }
        }
    }
}

//! Keys of the type picker and the combine form, and opening them from the Info
//! panel's Schema tab and the cell menu.

use crate::form::FormKey;
use crate::retype_modal::{Chosen, CombineField, CombineModal, RetypeModal};
use crate::{App, AppEvent, Overlay};
use crossterm::event::{KeyCode, KeyEvent};
use polars::prelude::DataType;

/// The retype and combine forms.
#[derive(Default)]
pub struct ColumnForms {
    /// The type picker, while it is open.
    pub retype: Option<crate::retype_modal::RetypeModal>,
    /// The combine form, while it is open.
    pub combine: Option<crate::retype_modal::CombineModal>,
}

/// How many values on screen the format picker judges and previews formats by.
const EXAMPLES: usize = 20;

impl App {
    /// A line of the cell menu that no key reaches.
    pub(crate) fn menu_action(&mut self, action: crate::context_menu::MenuAction) {
        let Some(column) = self
            .data_table_state
            .as_ref()
            .and_then(|s| s.current_column().map(str::to_string))
        else {
            return;
        };
        match action {
            crate::context_menu::MenuAction::ChangeType => self.open_retype(&column),
            crate::context_menu::MenuAction::CombineDatetime => self.open_combine(&column),
        }
    }

    /// The type picker for `column`, over what is on screen; Esc or Enter come back
    /// to it.
    pub(crate) fn open_retype(&mut self, column: &str) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let Some(as_read) = state.type_as_read(column) else {
            return;
        };
        let current = state.column_type_of(column).cloned();
        let examples = state.values_on_screen(column, EXAMPLES);
        self.column_forms.retype = Some(RetypeModal::new(
            column.to_string(),
            as_read,
            current.as_ref(),
            examples,
        ));
        self.open_over(|returns_to| Overlay::Retype { returns_to });
    }

    /// The combine form, its date the column `date`.
    pub(crate) fn open_combine(&mut self, date: &str) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let schema = state.schema().clone();
        let columns: Vec<String> = schema
            .iter()
            .filter(|(_, dtype)| {
                matches!(dtype, DataType::String | DataType::Date | DataType::Time)
            })
            .map(|(name, _)| name.to_string())
            .collect();
        let taken: Vec<String> = schema.iter_names().map(|n| n.to_string()).collect();
        self.column_forms.combine = Some(CombineModal::new(date.to_string(), columns, &taken));
        self.open_over(|returns_to| Overlay::Combine { returns_to });
    }

    /// The type picker's keys: type to narrow, ↑↓ move, Enter chooses, Esc goes back.
    pub(crate) fn retype_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = self.column_forms.retype.as_mut()?;
        match event.code {
            KeyCode::Esc => {
                if !modal.back() {
                    self.close_overlay();
                }
            }
            KeyCode::Enter => match modal.choose() {
                Chosen::Nothing | Chosen::Format => {}
                Chosen::AsRead => {
                    let column = modal.column.clone();
                    self.close_overlay();
                    self.retype_column(&column, None);
                }
                Chosen::Type(ty) => {
                    let column = modal.column.clone();
                    self.close_overlay();
                    self.retype_column(&column, Some(ty));
                }
            },
            KeyCode::Up => modal.picker.move_up(),
            KeyCode::Down => modal.picker.move_down(),
            KeyCode::Backspace => modal.picker.backspace(),
            KeyCode::Char(c) => modal.picker.filter_key(c, event.modifiers),
            _ => {}
        }
        None
    }

    /// `column` as `ty`, or as read with `None`; the values that did not fit are
    /// counted behind it, for the Notes.
    pub(crate) fn retype_column(
        &mut self,
        column: &str,
        ty: Option<crate::column_types::ColumnType>,
    ) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        state.set_column_type(column, ty);
        self.count_unfit();
    }

    /// The combine form's keys: the shared form keys, then what each field does.
    pub(crate) fn combine_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = self.column_forms.combine.as_mut()?;
        if crate::form::picker_form_key(modal, event) {
            return None;
        }
        modal.problem = None;
        match crate::form::key(modal, event) {
            FormKey::Cancel => self.close_overlay(),
            FormKey::Submit => {
                let derived = match modal.derived() {
                    Ok(derived) => derived,
                    Err(problem) => {
                        modal.problem = Some(problem);
                        return None;
                    }
                };
                let made = self
                    .data_table_state
                    .as_mut()
                    .map(|state| state.add_made_column(derived));
                match made {
                    Some(Err(problem)) => {
                        if let Some(modal) = self.column_forms.combine.as_mut() {
                            modal.problem = Some(problem);
                        }
                    }
                    _ => self.close_overlay(),
                }
            }
            FormKey::Step(CombineField::Kind, delta) => modal.step_kind(delta),
            FormKey::Act(CombineField::Kind) => modal.step_kind(1),
            FormKey::Act(_) | FormKey::Step(_, _) => {
                if modal.focus != CombineField::Name {
                    modal.open_picker();
                }
            }
            FormKey::Text(CombineField::Name) => {
                modal.name.handle_key(event, None);
            }
            _ => {}
        }
        None
    }
}

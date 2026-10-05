//! The view modal's keys.

use crate::form::FormKey;
use crate::widgets::view_modal::{FormFocus, ViewModalMode};
use crate::{App, AppEvent};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys while the view modal is open.
    pub(crate) fn view_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.view_modal.mode != ViewModalMode::List;
        // The list's status line is about the last key; this one replaces it.
        self.view_modal.status = None;
        if form && self.view_modal.score_details.is_none() {
            return self.view_form_key(event);
        }
        match event.code {
            KeyCode::Esc => {
                if self.view_modal.score_details.is_some() {
                    self.view_modal.score_details = None;
                } else if form {
                    // Back to the list; the form's staged edits die with it.
                    self.view_modal.exit_form();
                } else {
                    self.view_modal.close();
                }
            }
            _ if self.view_modal.score_details.is_some() => {}
            // The list.
            KeyCode::Up | KeyCode::Char('k') if !form => self.view_modal.select_prev(),
            KeyCode::Down | KeyCode::Char('j') if !form => self.view_modal.select_next(),
            KeyCode::Enter if !form => {
                if let Some(view) = self.view_modal.selected_view().cloned() {
                    if let Err(e) = self.apply_view(&view) {
                        // The list stays open, so the user sees what failed.
                        self.error_modal.show(format!("Error applying view: {}", e));
                    } else {
                        self.view_modal.active = false;
                    }
                }
            }
            KeyCode::Char('s') if !form => {
                // A view saved from an untouched table would carry
                // nothing, and — matching by schema — it would shadow
                // real views in the V/auto-apply gate as a well-used
                // no-op. Refuse at the door, not after the form.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.is_at_defaults())
                {
                    // A refusal is validation, not a failure: it is said on
                    // the surface's own status line, not in a modal.
                    self.view_modal.status = Some(
                            "Nothing to save yet: set a sample, query, filter, sort, column layout, or pivot/melt first."
                                .to_string(),
                        );
                } else {
                    self.open_save_view_form();
                }
            }
            KeyCode::Char('e') if !form => {
                if let Some(view) = self.view_modal.selected_view().cloned() {
                    self.view_modal
                        .enter_edit_mode(&view, self.history_limit, &self.theme);
                }
            }
            // Asked with the one confirmation, on No: a reflexive second key
            // declines.
            KeyCode::Char('d') if !form => {
                if let Some(view) = self.view_modal.selected_view() {
                    let message = format!("Delete \"{}\"? This cannot be undone.", view.name);
                    self.pending_delete_view = Some(view.id.clone());
                    self.confirmation_modal.show_destructive(message, "Delete");
                }
            }
            KeyCode::Char('i') if !form => {
                self.view_modal.score_details = self.view_score_details();
            }
            _ => {}
        }
        None
    }

    /// Keys in the save/edit form: the shared form keys (`crate::form`), then what
    /// each row does with them.
    fn view_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match crate::form::key(&mut self.view_modal, event) {
            // Back to the list; the form's staged edits die with it.
            FormKey::Cancel => self.view_modal.exit_form(),
            FormKey::Submit => self.save_view_form(),
            FormKey::Act(FormFocus::Matching) => self.view_modal.toggle_matching(),
            FormKey::Act(FormFocus::SchemaMatch) => {
                self.view_modal.schema_match_enabled = !self.view_modal.schema_match_enabled;
            }
            FormKey::Text(FormFocus::Description)
                if matches!(event.code, KeyCode::PageUp | KeyCode::PageDown) =>
            {
                // PageUp/PageDown move through the description five lines at a time.
                const DESCRIPTION_PAGE_LINES: isize = 5;
                let delta = if event.code == KeyCode::PageUp {
                    -DESCRIPTION_PAGE_LINES
                } else {
                    DESCRIPTION_PAGE_LINES
                };
                self.view_modal
                    .description_input
                    .move_cursor_by_lines(delta);
            }
            FormKey::Text(field) => {
                if field == FormFocus::Name {
                    // The error clears as soon as the name changes.
                    self.view_modal.name_error = None;
                }
                // The event goes through whole: text fields keep their readline
                // bindings, so Ctrl+W must arrive as Ctrl+W.
                if let Some(input) = self.view_modal.focused_input_mut() {
                    input.handle_key(event, None);
                }
            }
            _ => {}
        }
        None
    }
}

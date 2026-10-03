//! The template modal's keys.

use crate::widgets::view_modal::{FormFocus, ViewModalMode};
use crate::{App, AppEvent};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys while the template modal is open.
    pub(crate) fn view_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.view_modal.mode != ViewModalMode::List;
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The list's status line is about the last key; this one replaces it.
        self.view_modal.status = None;
        match event.code {
            KeyCode::Esc => {
                if self.view_modal.score_details.is_some() {
                    self.view_modal.score_details = None;
                } else if self.view_modal.delete_confirm {
                    self.view_modal.delete_confirm = false;
                } else if self.view_modal.show_help {
                    self.view_modal.show_help = false;
                } else if form {
                    // Back to the list; the form's staged edits die with it.
                    self.view_modal.exit_form();
                } else {
                    self.view_modal.close();
                }
            }
            // The delete confirmation owns the keys while it is up.
            KeyCode::Enter | KeyCode::Char('d') | KeyCode::Char('D')
                if self.view_modal.delete_confirm =>
            {
                self.view_modal.delete_confirm = false;
                if let Some(view) = self.view_modal.selected_view().cloned()
                    && self.view_manager.delete_view(&view.id).is_ok()
                {
                    self.refresh_view_list();
                }
            }
            _ if self.view_modal.delete_confirm => {}
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
                            "Nothing to save yet: set a query, filter, sort, column layout, or pivot/melt first."
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
            KeyCode::Char('d') if !form => {
                if self.view_modal.selected_view().is_some() {
                    self.view_modal.delete_confirm = true;
                }
            }
            KeyCode::Char('i') if !form => {
                self.view_modal.score_details = self.view_score_details();
            }
            // The form.
            KeyCode::Tab if form => self.view_modal.next_focus(),
            KeyCode::BackTab if form => self.view_modal.prev_focus(),
            // Ctrl+J too: it works on every terminal, and some send
            // Ctrl+Enter as Ctrl+J.
            KeyCode::Enter | KeyCode::Char('j') if form && ctrl => self.save_view_form(),
            KeyCode::Enter if form => {
                // Enter saves from anywhere; inside the multiline
                // description it types, and the footer names Ctrl+J.
                if self.view_modal.form_focus == FormFocus::Description {
                    let event = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
                    self.view_modal.description_input.handle_key(&event, None);
                } else {
                    self.save_view_form();
                }
            }
            KeyCode::Up | KeyCode::Down
                if form && self.view_modal.form_focus == FormFocus::Description =>
            {
                let event = KeyEvent::new(event.code, KeyModifiers::empty());
                self.view_modal.description_input.handle_key(&event, None);
            }
            KeyCode::Up if form => self.view_modal.prev_focus(),
            KeyCode::Down if form => self.view_modal.next_focus(),
            KeyCode::PageUp | KeyCode::PageDown
                if form && self.view_modal.form_focus == FormFocus::Description =>
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
            KeyCode::Char(' ') if form && self.view_modal.form_focus == FormFocus::Matching => {
                self.view_modal.toggle_matching();
            }
            KeyCode::Char(' ') if form && self.view_modal.form_focus == FormFocus::SchemaMatch => {
                self.view_modal.schema_match_enabled = !self.view_modal.schema_match_enabled;
            }
            KeyCode::Char(_) if form => {
                if self.view_modal.form_focus == FormFocus::Name {
                    // The error clears as soon as the name changes.
                    self.view_modal.name_error = None;
                }
                // The event goes through whole: text fields keep their
                // readline bindings, so Ctrl+W must arrive as Ctrl+W.
                if let Some(input) = self.view_modal.focused_input_mut() {
                    input.handle_key(event, None);
                }
            }
            KeyCode::Backspace
            | KeyCode::Delete
            | KeyCode::Left
            | KeyCode::Right
            | KeyCode::Home
            | KeyCode::End
                if form =>
            {
                if self.view_modal.form_focus == FormFocus::Name {
                    self.view_modal.name_error = None;
                }
                if let Some(input) = self.view_modal.focused_input_mut() {
                    input.handle_key(event, None);
                }
            }
            _ => {}
        }
        None
    }
}

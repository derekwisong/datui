//! The row inspector: opening and closing it, its keys, and drilling into nested
//! values.

use crate::app::form::ListMove;
use crate::{
    App, AppEvent, Overlay, app::modals::copy_modal, inspector::inspector_drill,
    inspector::inspector_modal, inspector::inspector_reader,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Whether Enter at the table opens the inspector, as Space does: a table whose rows
    /// do not drill into groups (a `by` view, a SQL GROUP BY), and not inside a
    /// drill-down.
    pub fn enter_inspects(&self) -> bool {
        self.at_table()
            && self
                .data_table_state
                .as_ref()
                .is_some_and(|state| !state.can_drill_down())
    }

    /// Space at the table, and Enter where there is nothing to drill into: the
    /// inspector over the selected row.
    pub(crate) fn open_inspector(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.inspect_row().is_none() {
            self.flash_note("No row to inspect".to_string());
            return;
        }
        self.inspector_modal
            .open(state.inspect_fields(), state.current_column());
        self.open_overlay(Overlay::Inspect);
    }

    pub(super) fn close_inspector(&mut self) {
        self.close_overlay();
    }

    /// Rebuild the inspector's list for the row shown: Filled, Compare and the find
    /// depend on its values.
    pub(crate) fn refresh_inspector_list(&mut self) {
        if let Some(state) = self.data_table_state.as_ref() {
            let row = state.inspect_row();
            crate::widgets::inspector::refresh_list(&mut self.inspector_modal, state, row.as_ref());
        }
    }

    /// The focused value's pane: as last drawn if still focused, else built for the key
    /// (without the table's preview, which only a frame knows).
    pub(super) fn inspector_pane(&self) -> Option<crate::widgets::inspector::Pane> {
        let modal = &self.inspector_modal;
        if modal.drill.is_some() {
            return modal.pane.as_ref().map(|(_, pane)| pane.clone());
        }
        let state = self.data_table_state.as_ref()?;
        let row = state.inspect_row()?;
        let field = modal.focused()?;
        if let Some(pane) = modal.pane_for(row.frame, row.row, &field.name) {
            return Some(pane.clone());
        }
        let shown = crate::widgets::inspector::shown(field, &row, modal.read.as_ref(), state);
        Some(crate::widgets::inspector::pane(
            &field.dtype,
            &shown,
            &crate::widgets::inspector::PaneAsk {
                choice: modal.view,
                width: modal
                    .pane
                    .as_ref()
                    .map_or(80, |(key, _)| key.width as usize),
                table: None,
                indented: crate::widgets::inspector::Indented::None,
                not_json: modal.known_not_json(row.frame, row.row, &field.name),
                unpacked: modal.unpacked(&(row.frame, row.row, field.name.clone())),
                read_key: "Enter",
            },
        ))
    }

    /// The inspector's keys. Row moves move the table's cursor, so the table is where
    /// the inspector left it.
    pub(crate) fn inspector_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        let modal = &mut self.inspector_modal;
        if modal.finding {
            match event.code {
                KeyCode::Esc => modal.clear_find(),
                KeyCode::Enter | KeyCode::Tab | KeyCode::Down => modal.finding = false,
                KeyCode::Up => {
                    modal.finding = false;
                    self.refresh_inspector_list();
                    self.inspector_modal.move_field(ListMove::Up);
                    return None;
                }
                KeyCode::Backspace => modal.find_backspace(),
                KeyCode::Char(c) => modal.find_key(c, event.modifiers),
                _ => {}
            }
            // Refocus now, not at the next frame: a replayed key must act on the field the find
            // left focused.
            self.refresh_inspector_list();
            return None;
        }
        if modal.value_find.as_ref().is_some_and(|f| f.editing) {
            self.value_find_key(event);
            return None;
        }
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return None;
        }
        if modal.focus == inspector_modal::Focus::Value {
            return self.inspector_value_key(event);
        }
        if modal.drill.is_some() {
            return self.drill_key(event);
        }
        self.refresh_inspector_list();
        let modal = &mut self.inspector_modal;
        if let Some(step) = ListMove::from_key(event) {
            modal.move_field(step);
            return None;
        }
        match event.code {
            // Esc backs out one level: the find, then Compare, then the inspector.
            KeyCode::Esc if !modal.filter.is_empty() => modal.clear_find(),
            KeyCode::Esc if modal.compare => {
                modal.compare = false;
                modal.filled_only = false;
            }
            KeyCode::Esc | KeyCode::Char(' ') => self.close_inspector(),
            // Two panes: Tab and Shift+Tab both cross to the value.
            KeyCode::Tab | KeyCode::BackTab => {
                if modal.focused().is_some() {
                    modal.focus = inspector_modal::Focus::Value;
                }
            }
            KeyCode::Char('/') => modal.finding = true,
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('f') => {
                modal.filled_only = !modal.filled_only;
                modal.list_offset = 0;
            }
            KeyCode::Char('s') => {
                modal.order = modal.order.next();
                modal.list_offset = 0;
            }
            KeyCode::Char('c') => {
                modal.compare = !modal.compare;
                if !modal.compare {
                    modal.filled_only = false;
                }
            }
            KeyCode::Char('m') => self.toggle_inspector_pin(),
            KeyCode::Right | KeyCode::Char('l') => return self.scroll_key(crate::Scroll::Next),
            KeyCode::Left | KeyCode::Char('h') => return self.scroll_key(crate::Scroll::Prev),
            KeyCode::Char('y') => self.copy_inspected_field(),
            KeyCode::Char('Y') => self.copy_inspected_row(),
            KeyCode::Char('o') => self.open_inspected_value(),
            KeyCode::Char('r') => self.read_focused_field(),
            KeyCode::Enter => return self.inspector_enter(),
            _ => {}
        }
        None
    }

    /// Keys inside a drilled level: the row's moves, but `→` and Enter open the focused
    /// item and `←` and Esc step up.
    fn drill_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let modal = &mut self.inspector_modal;
        if let Some(step) = ListMove::from_key(event) {
            modal.move_field(step);
            return None;
        }
        match event.code {
            KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                modal.drill_out();
            }
            KeyCode::Char(' ') => self.close_inspector(),
            // A level with nothing in it has no value to cross to.
            KeyCode::Tab | KeyCode::BackTab
                if modal
                    .drill
                    .as_ref()
                    .is_some_and(|drill| drill.level().focused().is_some()) =>
            {
                modal.focus = inspector_modal::Focus::Value;
            }
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => self.copy_drilled_item(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                let drill = modal.drill.as_ref()?;
                let (frame, row) = (drill.frame, drill.row);
                let (label, node) = drill.level().focused()?;
                let path = drill.item_key(&label);
                if node.opens() && !modal.known_not_json(frame, row, &path) {
                    self.inspector_open(frame, row, label, path, node);
                }
            }
            _ => {}
        }
        None
    }

    /// `e` in the inspector: the focused value's next view, if it has several. A number
    /// has one, so a text field's view is unchanged.
    pub(super) fn inspector_view(&mut self) {
        if let Some(view) = self.inspector_pane().and_then(|pane| pane.next_view()) {
            self.inspector_modal.choose_view(view);
        }
    }

    /// `w`: word wrap or hard wrap, for every value until it is pressed again.
    pub(super) fn inspector_wrap(&mut self) {
        let modal = &mut self.inspector_modal;
        modal.wrap = match modal.wrap {
            inspector_reader::Wrap::Word => inspector_reader::Wrap::Hard,
            inspector_reader::Wrap::Hard => inspector_reader::Wrap::Word,
        };
    }

    /// `m`: pin this row for Compare, or let the pin go when it is this row.
    fn toggle_inspector_pin(&mut self) {
        let Some(row) = self.data_table_state.as_ref().and_then(|s| s.inspect_row()) else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let here = modal
            .pinned
            .as_ref()
            .is_some_and(|p| (p.frame, p.row) == (row.frame, row.row));
        if here {
            modal.pinned = None;
            self.flash_note("Unpinned".to_string());
        } else {
            let n = row.display_row;
            modal.pinned = Some(row);
            modal.compare = true;
            self.flash_note(format!(
                "Pinned row {}; Compare shows it",
                copy_modal::thousands(n)
            ));
        }
    }

    /// Enter in the inspector: a group row's rows, as at the table; else open a nested
    /// value, or read fields the buffer lacks.
    fn inspector_enter(&mut self) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        if state.can_drill_down() {
            self.close_inspector();
            self.drill_selected_row();
            return None;
        }
        let row = state.inspect_row()?;
        let field = self.inspector_modal.focused().cloned()?;
        let shown = crate::widgets::inspector::shown(
            &field,
            &row,
            self.inspector_modal.read.as_ref(),
            state,
        );
        use crate::widgets::inspector::Shown;
        match shown {
            // A failed read is asked again: the pane said why, and Enter is the retry.
            Shown::Unread | Shown::Failed(_) => self.read_focused_field(),
            Shown::Value(ref v)
                if crate::widgets::inspector::value_opens(v)
                    && !self
                        .inspector_modal
                        .known_not_json(row.frame, row.row, &field.name) =>
            {
                let column = if field.buffered() {
                    row.values.column(&field.name).ok()
                } else {
                    self.inspector_modal
                        .read_values(row.frame, row.row)
                        .and_then(|values| values.column(&field.name).ok())
                };
                if let Some(column) = column {
                    let node =
                        inspector_drill::Node::Native(column.as_materialized_series().clone());
                    let path = inspector_drill::path_key([field.name.as_str()]);
                    self.inspector_open(row.frame, row.row, field.name.clone(), path, node);
                }
            }
            _ => {}
        }
        None
    }
}

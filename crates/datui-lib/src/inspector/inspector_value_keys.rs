//! The inspector's value view: its keys, and the find over a long value.

use crate::app::form::ListMove;
use crate::{App, AppEvent, inspector::inspector_modal, inspector::inspector_reader};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// The keys with the focus in the value: scroll it, search it, change its view.
    pub(crate) fn inspector_value_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let Some(pane) = self.inspector_pane() else {
            self.inspector_modal.focus = inspector_modal::Focus::List;
            return None;
        };
        let modal = &mut self.inspector_modal;
        let h = modal.page.max(1);
        let content = &pane.content;
        let page = h.saturating_sub(1);
        if let Some(step) = ListMove::from_key(event) {
            match step {
                ListMove::Home => modal.reader.home(),
                ListMove::End => modal.reader.end(content, h),
                _ => modal.reader.scroll(content, h, step.delta(page)),
            }
            return None;
        }
        match event.code {
            KeyCode::Esc
                if modal
                    .value_find
                    .as_ref()
                    .is_some_and(|f| !f.text.is_empty()) =>
            {
                modal.value_find = None;
            }
            KeyCode::Esc | KeyCode::Tab | KeyCode::BackTab => {
                modal.focus = inspector_modal::Focus::List;
            }
            KeyCode::Char(' ') => self.close_inspector(),
            KeyCode::Char('/') => {
                modal.value_find = Some(inspector_modal::ValueFind {
                    editing: true,
                    pane: pane.id,
                    ..Default::default()
                });
            }
            KeyCode::Char('n') => self.next_value_hit(1),
            KeyCode::Char('N') => self.next_value_hit(-1),
            KeyCode::Char('e') => self.inspector_view(),
            KeyCode::Char('w') => self.inspector_wrap(),
            KeyCode::Char('y') => {
                if modal.drill.is_some() {
                    self.copy_drilled_item();
                } else {
                    self.copy_inspected_field();
                }
            }
            KeyCode::Char('o') if modal.drill.is_none() => self.open_inspected_value(),
            KeyCode::Right | KeyCode::Char('l') if modal.drill.is_none() => {
                return self.scroll_key(crate::Scroll::Next);
            }
            KeyCode::Left | KeyCode::Char('h') if modal.drill.is_none() => {
                return self.scroll_key(crate::Scroll::Prev);
            }
            _ => {}
        }
        None
    }

    /// A key in the value's find line. Enter finds every place and goes to the first at
    /// or after the pane's top.
    pub(crate) fn value_find_key(&mut self, event: &KeyEvent) {
        let pane = self.inspector_pane();
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        match event.code {
            KeyCode::Esc => modal.value_find = None,
            KeyCode::Enter => {
                find.editing = false;
                let Some(pane) = pane else {
                    return;
                };
                find.hits = inspector_reader::find_hits(&pane.content, &find.text);
                find.pane = pane.id;
                let h = modal.page.max(1);
                let from = modal.reader.window(&pane.content, h).from;
                let at = find.hits.partition_point(|&p| p < from);
                find.current = (!find.hits.is_empty()).then(|| at % find.hits.len());
                if let Some(at) = find.current {
                    let pos = find.hits[at];
                    modal.reader.jump(&pane.content, h, pos);
                }
            }
            KeyCode::Backspace => {
                find.text.pop();
            }
            KeyCode::Char(c) => inspector_modal::edit_find(&mut find.text, c, event.modifiers),
            _ => {}
        }
    }

    /// `n` and `N` in the value: the next or the last place found, round the ends.
    pub(crate) fn next_value_hit(&mut self, step: isize) {
        let Some(pane) = self.inspector_pane() else {
            return;
        };
        let modal = &mut self.inspector_modal;
        let Some(find) = modal.value_find.as_mut() else {
            return;
        };
        if find.pane != pane.id && !find.text.is_empty() {
            find.hits = inspector_reader::find_hits(&pane.content, &find.text);
            find.pane = pane.id;
            find.current = None;
        }
        let n = find.hits.len();
        if n == 0 {
            return;
        }
        let at = match find.current {
            Some(at) => (at as isize + step).rem_euclid(n as isize) as usize,
            None if step > 0 => 0,
            None => n - 1,
        };
        find.current = Some(at);
        let pos = find.hits[at];
        let h = modal.page.max(1);
        modal.reader.jump(&pane.content, h, pos);
    }
}

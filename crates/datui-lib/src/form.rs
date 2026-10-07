//! One focus convention for every form and dialog.
//!
//! A form is an ordered list of fields, each of a [`FieldKind`]. A modal says which
//! fields it shows right now and which one has focus ([`Form`]); this module moves
//! focus and decides what a key means, so every dialog answers the same keys the
//! same way:
//!
//! | Key | Does |
//! |---|---|
//! | Tab / Shift+Tab, ↓ / ↑ | next / previous field, wrapping |
//! | ← / → | step a choice; move the cursor in a text field |
//! | Space | toggle a checkbox, next value of a choice, open a picker, press a button |
//! | Enter | submit, from any field (a multiline field types it; Ctrl+J submits) |
//! | Esc | cancel the form (an open picker closes first: [`picker_key`]) |
//! | Ctrl+P / Ctrl+N | history in a text field |
//!
//! `h` `j` `k` `l` are the arrows on a field that does not type. What a submit, a
//! step or an action does stays with the modal: [`key`] says which happened, to
//! which field.

use crate::widgets::ui::PickerState;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// How a field takes keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// One line of text. The arrows across and every editing key are its own;
    /// Ctrl+P / Ctrl+N recall its history.
    Text,
    /// Several lines: Enter breaks the line, and ↑ / ↓ move between lines,
    /// leaving the field only from its first or last line ([`Form::text_edge`]).
    MultilineText,
    /// A value from a short list: ← / → step it, Space takes the next, wrapping.
    Choice,
    /// On or off: Space, ← or → flip it.
    Checkbox,
    /// A value from a list too long to step through: Space opens a picker. A
    /// pick-one picker also steps with ← / →; `multi` marks one that toggles
    /// several values, which does not.
    Picker { multi: bool },
    /// An action: Space does it.
    Button,
}

impl FieldKind {
    /// Whether the field takes typed characters, so letters are not keys there.
    pub fn types(self) -> bool {
        matches!(self, Self::Text | Self::MultilineText)
    }
}

/// What a key did to a form, for the modal to carry out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormKey<F> {
    /// Focus moved; nothing else to do.
    Moved,
    /// Enter (or Ctrl+J): apply the form.
    Submit,
    /// Esc: close the form, discarding its edits.
    Cancel,
    /// Step the field's value by `delta` (-1 or 1): ← / → on a choice or a pick-one
    /// picker, Space on a choice.
    Step(F, i8),
    /// Act on the field: flip a checkbox, open a picker, press a button.
    Act(F),
    /// The key belongs to the focused text field: hand it to its input.
    Text(F),
    /// Not a form key here; the modal's own keys (or none) take it.
    Other,
}

/// A form: its fields in order and the one with focus. The modal owns both; the
/// default methods move focus the same way for every form.
pub trait Form {
    /// The modal's name for a field.
    type Field: Copy + Eq + std::fmt::Debug;

    /// The fields on screen, top to bottom, each with its kind. A field the current
    /// state hides or disables is left out, so focus never lands on it.
    fn fields(&self) -> Vec<(Self::Field, FieldKind)>;

    fn focused(&self) -> Self::Field;

    /// Put focus on `field`. Called only with a field of [`Form::fields`]; a modal
    /// that shows a text cursor in the focused field syncs it here.
    fn set_focused(&mut self, field: Self::Field);

    /// For a multiline field: whether its cursor is on the first line, and on the
    /// last, where ↑ and ↓ leave it.
    fn text_edge(&self, _field: Self::Field) -> (bool, bool) {
        (true, true)
    }

    /// Whether `field` is a row of a list (a sort, a filter, a column) rather than a
    /// setting. A click on a list row only focuses it, so it can be picked to move or
    /// remove without changing it; a click on it once focused acts. A setting (a
    /// checkbox, a choice, a button) acts on the first click.
    fn list_row(&self, _field: Self::Field) -> bool {
        false
    }

    /// The focused field's kind; `None` when focus is on a field not shown.
    fn focused_kind(&self) -> Option<FieldKind> {
        let focused = self.focused();
        self.fields()
            .into_iter()
            .find(|(field, _)| *field == focused)
            .map(|(_, kind)| kind)
    }

    /// Focus `field` if the form shows it: a click on a row, or a modal moving
    /// focus itself. Returns whether it did.
    fn focus(&mut self, field: Self::Field) -> bool {
        let shown = self.fields().iter().any(|(f, _)| *f == field);
        if shown {
            self.set_focused(field);
        }
        shown
    }

    /// Move focus `delta` fields along, wrapping. A focus the form no longer shows
    /// counts from the top.
    fn move_focus(&mut self, delta: isize) {
        let fields = self.fields();
        if fields.is_empty() {
            return;
        }
        let focused = self.focused();
        let n = fields.len() as isize;
        let next = match fields.iter().position(|(f, _)| *f == focused) {
            Some(at) => (at as isize + delta).rem_euclid(n),
            None if delta < 0 => n - 1,
            None => 0,
        };
        self.set_focused(fields[next as usize].0);
    }

    fn focus_next(&mut self) {
        self.move_focus(1);
    }

    fn focus_prev(&mut self) {
        self.move_focus(-1);
    }

    /// After a change that hides fields: when the focused one went, focus the first
    /// field instead, so it never points at nothing.
    fn settle_focus(&mut self) {
        if self.focused_kind().is_none()
            && let Some((first, _)) = self.fields().first().copied()
        {
            self.set_focused(first);
        }
    }
}

fn plain(event: &KeyEvent) -> bool {
    event
        .modifiers
        .intersection(KeyModifiers::CONTROL | KeyModifiers::ALT)
        .is_empty()
}

/// What `event` does to `form` while no picker is open. Focus moves happen here;
/// everything else is returned for the modal to do.
pub fn key<T: Form + ?Sized>(form: &mut T, event: &KeyEvent) -> FormKey<T::Field> {
    let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
    let shift = event.modifiers.contains(KeyModifiers::SHIFT);
    let Some(kind) = form.focused_kind() else {
        // Focus on nothing (a form with no fields, or one just emptied): only the
        // ways in and out work.
        form.settle_focus();
        return match event.code {
            KeyCode::Esc => FormKey::Cancel,
            KeyCode::Enter => FormKey::Submit,
            _ => FormKey::Other,
        };
    };
    let field = form.focused();
    // A field that does not type reads hjkl as the arrows.
    let code = match event.code {
        KeyCode::Char('h') if !kind.types() && plain(event) => KeyCode::Left,
        KeyCode::Char('j') if !kind.types() && plain(event) => KeyCode::Down,
        KeyCode::Char('k') if !kind.types() && plain(event) => KeyCode::Up,
        KeyCode::Char('l') if !kind.types() && plain(event) => KeyCode::Right,
        code => code,
    };
    match code {
        KeyCode::Esc => FormKey::Cancel,
        // Ctrl+J beside Ctrl+Enter: some terminals send one as the other.
        KeyCode::Enter | KeyCode::Char('j' | 'J') if ctrl => FormKey::Submit,
        KeyCode::Enter if kind == FieldKind::MultilineText => FormKey::Text(field),
        KeyCode::Enter => FormKey::Submit,
        KeyCode::BackTab => {
            form.focus_prev();
            FormKey::Moved
        }
        KeyCode::Tab if shift => {
            form.focus_prev();
            FormKey::Moved
        }
        KeyCode::Tab => {
            form.focus_next();
            FormKey::Moved
        }
        KeyCode::Up | KeyCode::Down if event.modifiers.is_empty() => {
            let up = code == KeyCode::Up;
            if kind == FieldKind::MultilineText {
                let (first, last) = form.text_edge(field);
                if (up && !first) || (!up && !last) {
                    return FormKey::Text(field);
                }
            }
            form.move_focus(if up { -1 } else { 1 });
            FormKey::Moved
        }
        KeyCode::Left | KeyCode::Right => {
            let delta = if code == KeyCode::Left { -1 } else { 1 };
            match kind {
                FieldKind::Text | FieldKind::MultilineText => FormKey::Text(field),
                FieldKind::Choice | FieldKind::Picker { multi: false } => {
                    FormKey::Step(field, delta)
                }
                FieldKind::Checkbox => FormKey::Act(field),
                FieldKind::Picker { multi: true } | FieldKind::Button => FormKey::Other,
            }
        }
        KeyCode::Char(' ') if plain(event) => match kind {
            FieldKind::Text | FieldKind::MultilineText => FormKey::Text(field),
            FieldKind::Choice => FormKey::Step(field, 1),
            FieldKind::Checkbox | FieldKind::Picker { .. } | FieldKind::Button => {
                FormKey::Act(field)
            }
        },
        _ if kind.types() => FormKey::Text(field),
        _ => FormKey::Other,
    }
}

/// A move of a list's cursor, from the keys every list answers: ↑ / `k`, ↓ / `j`,
/// PageUp, PageDown, Home and End.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListMove {
    Up,
    Down,
    PageUp,
    PageDown,
    Home,
    End,
}

impl ListMove {
    /// The move `event` asks for, if it is a press of a list key.
    pub fn from_key(event: &KeyEvent) -> Option<Self> {
        if !event.is_press() {
            return None;
        }
        Some(match event.code {
            KeyCode::Up | KeyCode::Char('k') => Self::Up,
            KeyCode::Down | KeyCode::Char('j') => Self::Down,
            KeyCode::PageUp => Self::PageUp,
            KeyCode::PageDown => Self::PageDown,
            KeyCode::Home => Self::Home,
            KeyCode::End => Self::End,
            _ => return None,
        })
    }

    /// How far it moves, `page` rows to a page; Home and End as far as a move goes.
    pub fn delta(self, page: usize) -> isize {
        let page = isize::try_from(page.max(1)).unwrap_or(isize::MAX);
        match self {
            Self::Up => -1,
            Self::Down => 1,
            Self::PageUp => -page,
            Self::PageDown => page,
            Self::Home => isize::MIN,
            Self::End => isize::MAX,
        }
    }

    /// `at` moved in a list of `len` rows, `page` rows to a page, kept in the list.
    pub fn apply(self, at: usize, len: usize, page: usize) -> usize {
        let last = len.saturating_sub(1);
        at.saturating_add_signed(self.delta(page)).min(last)
    }
}

/// What a key did in an open picker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerKey {
    /// Moved or narrowed the list; nothing else to do.
    Handled,
    /// Esc: close the picker, choosing nothing.
    Close,
    /// Enter, or Space on a pick-one list: take the cursor's item and close.
    Choose,
    /// Space on a list that takes several: flip the cursor's item, staying open.
    Toggle,
    /// Tab / Shift+Tab: choose, close, and move focus on (`true`) or back.
    ChooseAndMove(bool),
    /// Not a picker key.
    Other,
}

/// What `event` does in an open picker: type to narrow, ↑↓ move, Space chooses
/// (or toggles in a list of several), Enter chooses, Esc closes the picker only.
/// A space never reaches the narrowing filter: it would match nothing and blank
/// the list under the key that opened it.
pub fn picker_key(picker: &mut PickerState, multi: bool, event: &KeyEvent) -> PickerKey {
    let shift = event.modifiers.contains(KeyModifiers::SHIFT);
    match event.code {
        KeyCode::Esc => PickerKey::Close,
        KeyCode::Enter => PickerKey::Choose,
        KeyCode::BackTab => PickerKey::ChooseAndMove(false),
        KeyCode::Tab => PickerKey::ChooseAndMove(!shift),
        KeyCode::Up => {
            picker.move_up();
            PickerKey::Handled
        }
        KeyCode::Down => {
            picker.move_down();
            PickerKey::Handled
        }
        KeyCode::Char(' ') if multi => PickerKey::Toggle,
        KeyCode::Char(' ') => PickerKey::Choose,
        KeyCode::Backspace => {
            picker.backspace();
            PickerKey::Handled
        }
        KeyCode::Char(c) => {
            picker.filter_key(c, event.modifiers);
            PickerKey::Handled
        }
        _ => PickerKey::Other,
    }
}

/// Index `at` stepped by `delta` through `len` values, wrapping.
pub fn step_index(at: usize, len: usize, delta: i8) -> usize {
    if len == 0 {
        return 0;
    }
    (at as isize + delta as isize).rem_euclid(len as isize) as usize
}

/// `current` stepped by `delta` through `all`, wrapping; the first value when
/// `current` is not among them.
pub fn step_value<T: Copy + PartialEq>(all: &[T], current: T, delta: i8) -> T {
    let at = all.iter().position(|v| *v == current);
    match at {
        Some(at) => all[step_index(at, all.len(), delta)],
        None => all[0],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum F {
        Name,
        Notes,
        Format,
        Header,
        Column,
        Tags,
        Go,
    }

    struct T {
        focus: F,
        show_header: bool,
        edge: (bool, bool),
    }

    impl Form for T {
        type Field = F;
        fn fields(&self) -> Vec<(F, FieldKind)> {
            let mut fields = vec![
                (F::Name, FieldKind::Text),
                (F::Notes, FieldKind::MultilineText),
                (F::Format, FieldKind::Choice),
            ];
            if self.show_header {
                fields.push((F::Header, FieldKind::Checkbox));
            }
            fields.extend([
                (F::Column, FieldKind::Picker { multi: false }),
                (F::Tags, FieldKind::Picker { multi: true }),
                (F::Go, FieldKind::Button),
            ]);
            fields
        }
        fn focused(&self) -> F {
            self.focus
        }
        fn set_focused(&mut self, field: F) {
            self.focus = field;
        }
        fn text_edge(&self, _: F) -> (bool, bool) {
            self.edge
        }
    }

    fn form() -> T {
        T {
            focus: F::Name,
            show_header: true,
            edge: (true, true),
        }
    }

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    #[test]
    fn tab_and_the_arrows_walk_every_field_and_wrap() {
        let mut f = form();
        let mut walked = vec![f.focus];
        for _ in 0..7 {
            assert_eq!(key(&mut f, &press(KeyCode::Down)), FormKey::Moved);
            walked.push(f.focus);
        }
        assert_eq!(
            walked,
            [
                F::Name,
                F::Notes,
                F::Format,
                F::Header,
                F::Column,
                F::Tags,
                F::Go,
                F::Name
            ]
        );
        key(&mut f, &press(KeyCode::Up));
        assert_eq!(f.focus, F::Go, "↑ from the first field wraps to the last");
        key(&mut f, &press(KeyCode::Tab));
        assert_eq!(f.focus, F::Name);
        key(&mut f, &press(KeyCode::BackTab));
        assert_eq!(f.focus, F::Go);
        key(&mut f, &KeyEvent::new(KeyCode::Tab, KeyModifiers::SHIFT));
        assert_eq!(
            f.focus,
            F::Tags,
            "Shift+Tab as Tab with Shift goes back too"
        );
    }

    #[test]
    fn hidden_fields_are_skipped_and_a_hidden_focus_settles() {
        let mut f = form();
        f.show_header = false;
        f.focus = F::Format;
        key(&mut f, &press(KeyCode::Down));
        assert_eq!(f.focus, F::Column);
        f.focus = F::Header;
        f.settle_focus();
        assert_eq!(f.focus, F::Name);
        assert!(!f.focus(F::Header), "a hidden field cannot take focus");
        assert!(f.focus(F::Tags));
        assert_eq!(f.focus, F::Tags);
    }

    #[test]
    fn the_arrows_across_step_choices_and_edit_text() {
        let mut f = form();
        assert_eq!(
            key(&mut f, &press(KeyCode::Left)),
            FormKey::Text(F::Name),
            "a text field keeps ← for its cursor"
        );
        f.focus = F::Format;
        assert_eq!(
            key(&mut f, &press(KeyCode::Left)),
            FormKey::Step(F::Format, -1)
        );
        assert_eq!(
            key(&mut f, &press(KeyCode::Right)),
            FormKey::Step(F::Format, 1)
        );
        f.focus = F::Column;
        assert_eq!(
            key(&mut f, &press(KeyCode::Right)),
            FormKey::Step(F::Column, 1)
        );
        f.focus = F::Tags;
        assert_eq!(key(&mut f, &press(KeyCode::Right)), FormKey::Other);
        f.focus = F::Header;
        assert_eq!(key(&mut f, &press(KeyCode::Right)), FormKey::Act(F::Header));
    }

    #[test]
    fn space_acts_on_the_field() {
        let mut f = form();
        let space = press(KeyCode::Char(' '));
        assert_eq!(key(&mut f, &space), FormKey::Text(F::Name), "text types it");
        f.focus = F::Format;
        assert_eq!(key(&mut f, &space), FormKey::Step(F::Format, 1));
        for field in [F::Header, F::Column, F::Tags, F::Go] {
            f.focus = field;
            assert_eq!(key(&mut f, &space), FormKey::Act(field));
        }
    }

    #[test]
    fn enter_submits_from_any_field_but_types_in_a_multiline_one() {
        let mut f = form();
        for field in [F::Name, F::Format, F::Header, F::Column, F::Tags, F::Go] {
            f.focus = field;
            assert_eq!(key(&mut f, &press(KeyCode::Enter)), FormKey::Submit);
        }
        f.focus = F::Notes;
        assert_eq!(key(&mut f, &press(KeyCode::Enter)), FormKey::Text(F::Notes));
        assert_eq!(key(&mut f, &ctrl('j')), FormKey::Submit);
        assert_eq!(
            key(
                &mut f,
                &KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL)
            ),
            FormKey::Submit
        );
    }

    #[test]
    fn esc_cancels_from_any_field() {
        let mut f = form();
        for field in [F::Name, F::Notes, F::Format, F::Go] {
            f.focus = field;
            assert_eq!(key(&mut f, &press(KeyCode::Esc)), FormKey::Cancel);
        }
    }

    #[test]
    fn history_is_ctrl_p_and_n_and_the_arrows_never_recall() {
        let mut f = form();
        assert_eq!(key(&mut f, &ctrl('p')), FormKey::Text(F::Name));
        assert_eq!(key(&mut f, &ctrl('n')), FormKey::Text(F::Name));
        assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Moved);
        assert_eq!(f.focus, F::Go);
    }

    #[test]
    fn a_multiline_field_keeps_the_arrows_until_its_edge() {
        let mut f = form();
        f.focus = F::Notes;
        f.edge = (false, false);
        assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Text(F::Notes));
        assert_eq!(key(&mut f, &press(KeyCode::Down)), FormKey::Text(F::Notes));
        f.edge = (true, false);
        assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Moved);
        assert_eq!(f.focus, F::Name);
    }

    #[test]
    fn vim_keys_move_only_where_nothing_types() {
        let mut f = form();
        assert_eq!(
            key(&mut f, &press(KeyCode::Char('j'))),
            FormKey::Text(F::Name)
        );
        f.focus = F::Format;
        assert_eq!(
            key(&mut f, &press(KeyCode::Char('l'))),
            FormKey::Step(F::Format, 1)
        );
        key(&mut f, &press(KeyCode::Char('j')));
        assert_eq!(f.focus, F::Header);
        key(&mut f, &press(KeyCode::Char('k')));
        assert_eq!(f.focus, F::Format);
        assert_eq!(key(&mut f, &press(KeyCode::Char('x'))), FormKey::Other);
    }

    #[test]
    fn the_picker_narrows_moves_and_never_types_a_space() {
        let mut p = PickerState::new(vec!["alpha".into(), "beta".into(), "gamma".into()]);
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Down)),
            PickerKey::Handled
        );
        assert_eq!(p.selected_original(), Some(1));
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Char('g'))),
            PickerKey::Handled
        );
        assert_eq!(p.filter, "g");
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Char(' '))),
            PickerKey::Choose
        );
        assert_eq!(
            picker_key(&mut p, true, &press(KeyCode::Char(' '))),
            PickerKey::Toggle
        );
        assert_eq!(p.filter, "g", "a space never narrows");
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Enter)),
            PickerKey::Choose
        );
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Esc)),
            PickerKey::Close
        );
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::Tab)),
            PickerKey::ChooseAndMove(true)
        );
        assert_eq!(
            picker_key(&mut p, false, &press(KeyCode::BackTab)),
            PickerKey::ChooseAndMove(false)
        );
    }

    #[test]
    fn steps_wrap_both_ways() {
        assert_eq!(step_index(0, 3, -1), 2);
        assert_eq!(step_index(2, 3, 1), 0);
        assert_eq!(step_value(&['a', 'b', 'c'], 'b', 1), 'c');
        assert_eq!(step_value(&['a', 'b', 'c'], 'z', 1), 'a');
    }
}

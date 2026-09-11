//! Key input handling.
//!
//! [`Input`] is a backend-independent key press. Crossterm events convert into
//! it, and [`TextArea::input`] maps one to an editing action using a default
//! key map modelled on readline, so the emacs-style bindings terminal users
//! expect (`Ctrl-A`, `Ctrl-E`, `Ctrl-K`, `Ctrl-W`, ...) work alongside the
//! arrow keys.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::cursor::CursorMove;
use super::TextArea;

/// A key, independent of any particular terminal backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Key {
    Char(char),
    Backspace,
    Enter,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    PageUp,
    PageDown,
    Tab,
    BackTab,
    Delete,
    Esc,
    /// A key the editor has no use for.
    #[default]
    None,
}

/// A key press together with its modifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Input {
    pub key: Key,
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
}

impl Input {
    pub fn new(key: Key) -> Self {
        Self {
            key,
            ..Self::default()
        }
    }
}

impl From<&KeyEvent> for Input {
    fn from(event: &KeyEvent) -> Self {
        let key = match event.code {
            KeyCode::Char(c) => Key::Char(c),
            KeyCode::Backspace => Key::Backspace,
            KeyCode::Enter => Key::Enter,
            KeyCode::Left => Key::Left,
            KeyCode::Right => Key::Right,
            KeyCode::Up => Key::Up,
            KeyCode::Down => Key::Down,
            KeyCode::Home => Key::Home,
            KeyCode::End => Key::End,
            KeyCode::PageUp => Key::PageUp,
            KeyCode::PageDown => Key::PageDown,
            KeyCode::Tab => Key::Tab,
            KeyCode::BackTab => Key::BackTab,
            KeyCode::Delete => Key::Delete,
            KeyCode::Esc => Key::Esc,
            _ => Key::None,
        };
        Self {
            key,
            ctrl: event.modifiers.contains(KeyModifiers::CONTROL),
            alt: event.modifiers.contains(KeyModifiers::ALT),
            shift: event.modifiers.contains(KeyModifiers::SHIFT),
        }
    }
}

impl From<KeyEvent> for Input {
    fn from(event: KeyEvent) -> Self {
        Self::from(&event)
    }
}

/// What a key press asks the editor to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Action {
    Insert(char),
    InsertNewline,
    InsertTab,
    DeletePrevChar,
    DeleteNextChar,
    DeletePrevWord,
    DeleteNextWord,
    DeleteToLineEnd,
    DeleteToLineStart,
    /// A cursor movement; the flag says whether it extends a selection.
    Move(CursorMove, bool),
    Undo,
    Redo,
    Copy,
    Cut,
    Paste,
    /// Nothing this editor handles.
    Nop,
}

/// The default key map.
///
/// Kept as a free function so it can be unit tested without a buffer.
pub(super) fn action_for(input: Input) -> Action {
    let Input {
        key,
        ctrl,
        alt,
        shift,
    } = input;

    // Plain (or shifted) printable characters insert themselves.
    if let Key::Char(c) = key {
        if !ctrl && !alt {
            return Action::Insert(c);
        }
    }

    match (key, ctrl, alt) {
        (Key::Enter, _, _) | (Key::Char('m'), true, false) => Action::InsertNewline,
        (Key::Tab, false, false) => Action::InsertTab,

        (Key::Backspace, false, false) | (Key::Char('h'), true, false) => Action::DeletePrevChar,
        (Key::Delete, false, false) | (Key::Char('d'), true, false) => Action::DeleteNextChar,
        (Key::Backspace, _, true)
        | (Key::Char('w'), true, false)
        | (Key::Char('h'), false, true) => Action::DeletePrevWord,
        (Key::Delete, _, true) | (Key::Char('d'), false, true) => Action::DeleteNextWord,
        (Key::Char('k'), true, false) => Action::DeleteToLineEnd,
        (Key::Char('j'), true, false) => Action::DeleteToLineStart,

        (Key::Char('u'), true, false) => Action::Undo,
        (Key::Char('r'), true, false) => Action::Redo,
        (Key::Char('c'), true, false) => Action::Copy,
        (Key::Char('x'), true, false) => Action::Cut,
        (Key::Char('y'), true, false) => Action::Paste,

        (Key::Right, true, false) | (Key::Char('f'), false, true) => {
            Action::Move(CursorMove::WordForward, shift)
        }
        (Key::Left, true, false) | (Key::Char('b'), false, true) => {
            Action::Move(CursorMove::WordBack, shift)
        }
        (Key::Right, false, false) | (Key::Char('f'), true, false) => {
            Action::Move(CursorMove::Forward, shift)
        }
        (Key::Left, false, false) | (Key::Char('b'), true, false) => {
            Action::Move(CursorMove::Back, shift)
        }
        (Key::Up, false, false) | (Key::Char('p'), true, false) => {
            Action::Move(CursorMove::Up, shift)
        }
        (Key::Down, false, false) | (Key::Char('n'), true, false) => {
            Action::Move(CursorMove::Down, shift)
        }
        (Key::Home, false, false) | (Key::Char('a'), true, false) => {
            Action::Move(CursorMove::Head, shift)
        }
        (Key::End, false, false) | (Key::Char('e'), true, false) => {
            Action::Move(CursorMove::End, shift)
        }
        (Key::Home, true, _) | (Key::Char('<'), false, true) => {
            Action::Move(CursorMove::Top, shift)
        }
        (Key::End, true, _) | (Key::Char('>'), false, true) => {
            Action::Move(CursorMove::Bottom, shift)
        }
        (Key::PageDown, false, false) | (Key::Char('v'), true, false) => {
            Action::Move(CursorMove::DownBy(0), shift)
        }
        (Key::PageUp, false, false) | (Key::Char('v'), false, true) => {
            Action::Move(CursorMove::UpBy(0), shift)
        }

        _ => Action::Nop,
    }
}

impl TextArea {
    /// Apply one key press, returning whether it changed the buffer or the
    /// cursor.
    pub fn input(&mut self, input: impl Into<Input>) -> bool {
        match action_for(input.into()) {
            Action::Insert(c) => self.insert_char(c),
            Action::InsertNewline => self.insert_newline(),
            Action::InsertTab => self.insert_tab(),
            Action::DeletePrevChar => self.delete_prev_char(),
            Action::DeleteNextChar => self.delete_next_char(),
            Action::DeletePrevWord => self.delete_prev_word(),
            Action::DeleteNextWord => self.delete_next_word(),
            Action::DeleteToLineEnd => self.delete_line_by_end(),
            Action::DeleteToLineStart => self.delete_line_by_head(),
            Action::Move(movement, true) => self.move_cursor_selecting(movement),
            Action::Move(movement, false) => self.move_cursor(movement),
            Action::Undo => self.undo(),
            Action::Redo => self.redo(),
            Action::Copy => self.copy(),
            Action::Cut => self.cut(),
            Action::Paste => self.paste(),
            Action::Nop => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> Input {
        Input::from(KeyEvent::new(code, modifiers))
    }

    #[test]
    fn plain_characters_insert_themselves() {
        assert_eq!(
            action_for(key(KeyCode::Char('a'), KeyModifiers::NONE)),
            Action::Insert('a')
        );
        assert_eq!(
            action_for(key(KeyCode::Char('A'), KeyModifiers::SHIFT)),
            Action::Insert('A')
        );
    }

    #[test]
    fn readline_bindings_match_their_arrow_key_equivalents() {
        let pairs = [
            (
                key(KeyCode::Char('a'), KeyModifiers::CONTROL),
                key(KeyCode::Home, KeyModifiers::NONE),
            ),
            (
                key(KeyCode::Char('e'), KeyModifiers::CONTROL),
                key(KeyCode::End, KeyModifiers::NONE),
            ),
            (
                key(KeyCode::Char('f'), KeyModifiers::CONTROL),
                key(KeyCode::Right, KeyModifiers::NONE),
            ),
            (
                key(KeyCode::Char('b'), KeyModifiers::CONTROL),
                key(KeyCode::Left, KeyModifiers::NONE),
            ),
            (
                key(KeyCode::Char('h'), KeyModifiers::CONTROL),
                key(KeyCode::Backspace, KeyModifiers::NONE),
            ),
            (
                key(KeyCode::Char('d'), KeyModifiers::CONTROL),
                key(KeyCode::Delete, KeyModifiers::NONE),
            ),
        ];
        for (emacs, arrow) in pairs {
            assert_eq!(
                action_for(emacs),
                action_for(arrow),
                "{emacs:?} vs {arrow:?}"
            );
        }
    }

    #[test]
    fn shifted_movement_extends_the_selection() {
        assert_eq!(
            action_for(key(KeyCode::Right, KeyModifiers::SHIFT)),
            Action::Move(CursorMove::Forward, true)
        );
        assert_eq!(
            action_for(key(KeyCode::Right, KeyModifiers::NONE)),
            Action::Move(CursorMove::Forward, false)
        );
    }

    #[test]
    fn ctrl_arrows_move_by_word() {
        assert_eq!(
            action_for(key(KeyCode::Right, KeyModifiers::CONTROL)),
            Action::Move(CursorMove::WordForward, false)
        );
        assert_eq!(
            action_for(key(KeyCode::Left, KeyModifiers::CONTROL)),
            Action::Move(CursorMove::WordBack, false)
        );
    }

    #[test]
    fn unmapped_keys_do_nothing() {
        assert_eq!(
            action_for(key(KeyCode::Esc, KeyModifiers::NONE)),
            Action::Nop
        );
        assert_eq!(
            action_for(key(KeyCode::F(5), KeyModifiers::NONE)),
            Action::Nop
        );
        assert_eq!(
            action_for(key(KeyCode::BackTab, KeyModifiers::SHIFT)),
            Action::Nop
        );
    }

    #[test]
    fn key_events_convert_with_their_modifiers() {
        let input = key(
            KeyCode::Char('k'),
            KeyModifiers::CONTROL | KeyModifiers::ALT,
        );
        assert_eq!(input.key, Key::Char('k'));
        assert!(input.ctrl);
        assert!(input.alt);
        assert!(!input.shift);
    }
}

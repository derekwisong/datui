//! A paste: text the terminal brackets (bracketed paste), taken as one edit by the
//! field that takes typed text, and by nothing else. Unbracketed, a paste arrived as
//! keys: a frame for each, a search started per character at home, and a pasted line
//! break pressed Enter.

use crate::{App, AppEvent, InputMode, Overlay};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// What a paste goes into, where the app stands now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PasteTarget {
    /// The home screen's `~` prompt, or its filter.
    Home,
    /// The focused text field: the command line, find, a form's text field.
    Field,
    /// Nothing takes text here (the table, a dialog's buttons): the paste is dropped,
    /// never read as keys.
    Nowhere,
}

impl App {
    /// Where pasted text goes now.
    pub(crate) fn paste_target(&self) -> PasteTarget {
        // These take keys ahead of any field, and none of them types.
        if self.help.is_open()
            || self.confirmation_modal.active
            || self.error_modal.active
            || self.context_menu.is_some()
        {
            return PasteTarget::Nowhere;
        }
        if self.input_mode == InputMode::Home && self.overlay == Overlay::None {
            if self.info.documentation.is_open() {
                return PasteTarget::Nowhere;
            }
            return PasteTarget::Home;
        }
        if self.text_field_focused() {
            return PasteTarget::Field;
        }
        PasteTarget::Nowhere
    }

    /// Take a paste as one edit. At home it is typed into the `~` prompt or the filter
    /// at once (one search, one listing); in a field it is typed character by
    /// character, so the field reacts as to typing, within the one frame. Line breaks
    /// and tabs are spaces: a paste never applies or moves focus. Returns the follow-up
    /// a character asked for, which ends the paste.
    pub(crate) fn paste(&mut self, text: &str) -> Option<AppEvent> {
        match self.paste_target() {
            PasteTarget::Home => {
                self.paste_at_home(one_line(text).trim());
                None
            }
            PasteTarget::Field => {
                for c in one_line(text).chars() {
                    let key = KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE);
                    if let Some(follow_up) = self.key(&key) {
                        return Some(follow_up);
                    }
                }
                None
            }
            PasteTarget::Nowhere => None,
        }
    }

    /// As the home screen's character keys do, once for the whole text.
    fn paste_at_home(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        self.home.status = None;
        self.home.apply_new_measurements();
        if self.home.path_input_active {
            self.home.path_input.push_str(text);
            self.list_the_typed_directory();
            self.home.pick_first_path();
            return;
        }
        // A kept filter is selected: the paste replaces it, as a typed character does.
        if std::mem::take(&mut self.home.filter_selected) {
            self.home.filter.clear();
        }
        self.home.filter.push_str(text);
        self.spawn_home_search();
        self.home.sync_search_section();
        self.home.select_first_entry();
        #[cfg(feature = "cloud")]
        self.narrow_cloud_listing();
    }
}

/// `text` on one line: a line break (CRLF, LF or CR) or tab is a space, other control
/// characters are left out, and line breaks at either end go.
pub(crate) fn one_line(text: &str) -> String {
    let text = text.trim_matches(['\r', '\n']);
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' => {
                chars.next_if_eq(&'\n');
                out.push(' ');
            }
            '\n' | '\t' => out.push(' '),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::one_line;

    #[test]
    fn a_paste_is_one_line() {
        assert_eq!(one_line("a\r\nb\nc\rd\te"), "a b c d e");
        assert_eq!(one_line("\n/data/x.csv\r\n"), "/data/x.csv");
        assert_eq!(one_line("bell\u{7}less\u{1b}[31m"), "bellless[31m");
        assert_eq!(one_line("  kept  "), "  kept  ");
    }
}

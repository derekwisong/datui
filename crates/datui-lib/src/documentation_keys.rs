//! The documentation viewer: opening a dataset's documentation, its keys, copying
//! from it and following its links.

use crate::feedback::Confirm;
use crate::{App, clipboard, glyphs, link_open, widgets};
use crossterm::event::{KeyCode, KeyEvent};
use std::path::Path;

impl App {
    /// Info's Documentation tab for the open dataset: its catalog entry (or the one it
    /// is inside) and its format spec's docs; closed when neither has any.
    pub(crate) fn open_info_documentation(&mut self) {
        self.info.info_documentation.close();
        let spec = self.data_table_state.as_ref().and_then(|state| {
            state
                .format_read()
                .map(|read| read.spec.clone())
                .or_else(|| state.delimited_read().map(|read| read.spec.clone()))
        });
        let name = self
            .path
            .as_deref()
            .and_then(Path::file_name)
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(doc) = widgets::documentation::Documented::new(
            self.info.catalog_entry.clone(),
            spec.and_then(|s| s.docs()).map(std::sync::Arc::new),
            name,
        ) {
            self.info.info_documentation.open(doc, None);
            self.info.info_documentation.links_open = self.home_app.local_desktop;
        }
    }

    /// A key while the Documentation view is open over home.
    pub(crate) fn documentation_key(&mut self, event: &KeyEvent) {
        let page = self.info.documentation.view_height.max(1) as isize;
        match event.code {
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Left => self.info.documentation.close(),
            KeyCode::Up | KeyCode::Char('k') => self.info.documentation.move_cursor(-1),
            KeyCode::Down | KeyCode::Char('j') => self.info.documentation.move_cursor(1),
            KeyCode::PageUp => self.info.documentation.move_cursor(-page),
            KeyCode::PageDown => self.info.documentation.move_cursor(page),
            KeyCode::Home | KeyCode::Char('g') => {
                self.info.documentation.move_cursor(isize::MIN / 2)
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.info.documentation.move_cursor(isize::MAX / 2)
            }
            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right => {
                self.info.documentation.toggle_legend();
            }
            KeyCode::Char('y') => self.copy_documentation_line(),
            KeyCode::Char('o') => {
                let link = self.info.documentation.link();
                self.ask_to_open_link(link);
            }
            // The view takes no text, so ? is help here, as at the table.
            KeyCode::Char('?') => self.open_help_overlay(),
            _ => {}
        }
    }

    /// `y` in the Documentation view: the line's link or value, whole.
    fn copy_documentation_line(&mut self) {
        match self.info.documentation.copy_text() {
            Some(text) => self.copy_documentation_text(text),
            None => self.flash_note("Nothing to copy on this line".to_string()),
        }
    }

    /// `o` on a Documentation page: ask, showing the whole URL, before opening a
    /// browser. Nothing without a link; a status line where no local browser would
    /// show it or the link is not http(s).
    pub(crate) fn ask_to_open_link(&mut self, link: Option<String>) {
        let Some(link) = link else {
            return;
        };
        if !self.home_app.local_desktop {
            self.flash_note("o opens links on a local desktop; y copies it".to_string());
            return;
        }
        match link_open::checked_url(&link) {
            Ok(url) => {
                self.confirmation_modal.show_choice(
                    format!("Open {url}?"),
                    "Open",
                    "Cancel",
                    Confirm::OpenLink(url),
                );
            }
            Err(why) => self.flash_note(format!("Not opened: {why}; y copies it")),
        }
    }

    /// Put a line of a Documentation page on the clipboard, whole.
    pub(crate) fn copy_documentation_text(&mut self, text: String) {
        let shown: String = text.chars().take(60).collect();
        let said = if shown.len() < text.len() {
            format!("Copied {shown}{}", glyphs::get().ellipsis)
        } else {
            format!("Copied {shown}")
        };
        self.finish_copy(clipboard::Payload::text(text), said);
    }
}

//! A text editing widget built directly on ratatui.
//!
//! [`TextArea`] owns a buffer of lines, a cursor, a selection, an undo history
//! and a yank buffer, and knows how to draw itself into a [`Rect`]. It has no
//! opinion about history of previously submitted values, theming or focus:
//! that belongs to [`crate::widgets::text_input::TextInput`], which wraps this
//! type and is what the rest of datui uses.
//!
//! Coordinates are `(row, column)` pairs where the column is a *character*
//! index, not a terminal column. Character indices are what the editing and
//! cursor logic works in; display width is resolved only while rendering, in
//! [`render`].

mod cursor;
mod edit;
mod history;
mod input;
mod render;

#[cfg(test)]
mod tests;

use std::cell::Cell;

use ratatui::style::{Modifier, Style};

pub use cursor::CursorMove;
pub use input::{Input, Key};

use history::History;

/// How much of the buffer was on screen the last time it was drawn.
///
/// Kept in a [`Cell`] so that rendering, which only has `&self`, can record the
/// scroll position it settled on and the page size that `PageUp`/`PageDown`
/// should use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Viewport {
    /// First visible row.
    pub row: usize,
    /// First visible terminal column.
    pub col: usize,
    pub height: u16,
    pub width: u16,
}

/// A multi-line text editor.
#[derive(Debug, Clone)]
pub struct TextArea {
    lines: Vec<String>,
    cursor: (usize, usize),
    /// Fixed end of the selection, if a selection is active. The moving end is
    /// the cursor.
    selection_anchor: Option<(usize, usize)>,
    /// Text held by the most recent copy, cut or kill, pasted by `paste`.
    yank: String,
    history: History,
    style: Style,
    cursor_style: Style,
    selection_style: Style,
    cursor_visible: bool,
    tab_len: usize,
    viewport: Cell<Viewport>,
}

impl Default for TextArea {
    fn default() -> Self {
        Self::new()
    }
}

impl TextArea {
    /// An empty editor holding a single empty line.
    pub fn new() -> Self {
        Self {
            lines: vec![String::new()],
            cursor: (0, 0),
            selection_anchor: None,
            yank: String::new(),
            history: History::default(),
            style: Style::default(),
            cursor_style: Style::default().add_modifier(Modifier::REVERSED),
            selection_style: Style::default().add_modifier(Modifier::REVERSED),
            cursor_visible: true,
            tab_len: 4,
            viewport: Cell::new(Viewport::default()),
        }
    }

    /// An editor holding `text`, with the cursor at the end of it.
    pub fn from_text(text: &str) -> Self {
        let mut ta = Self::new();
        ta.set_text(text);
        ta
    }

    /// Replace the whole buffer, moving the cursor to the end and discarding
    /// undo history, selection and scroll position.
    pub fn set_text(&mut self, text: &str) {
        self.lines = split_lines(text);
        let row = self.lines.len() - 1;
        let col = char_count(&self.lines[row]);
        self.cursor = (row, col);
        self.selection_anchor = None;
        self.history.clear();
        self.viewport.set(Viewport::default());
    }

    /// Empty the buffer.
    pub fn clear(&mut self) {
        self.set_text("");
    }

    /// The buffer, one entry per line. Always holds at least one line.
    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    /// The buffer as a single newline-joined string.
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    /// Number of lines in the buffer.
    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    /// The line at `row`, if it exists.
    pub fn line(&self, row: usize) -> Option<&str> {
        self.lines.get(row).map(String::as_str)
    }

    /// True when the buffer holds no characters at all.
    pub fn is_empty(&self) -> bool {
        self.lines.len() == 1 && self.lines[0].is_empty()
    }

    /// Cursor position as `(row, character column)`.
    pub fn cursor(&self) -> (usize, usize) {
        self.cursor
    }

    /// Move the cursor to an absolute position, clamped into the buffer. This
    /// cancels any selection.
    pub fn set_cursor(&mut self, row: usize, col: usize) {
        self.selection_anchor = None;
        self.cursor = self.clamp_position((row, col));
    }

    /// Selected range as `(start, end)` with `start <= end`, if a selection is
    /// active and non-empty.
    pub fn selection(&self) -> Option<((usize, usize), (usize, usize))> {
        let anchor = self.selection_anchor?;
        if anchor == self.cursor {
            return None;
        }
        Some(if anchor <= self.cursor {
            (anchor, self.cursor)
        } else {
            (self.cursor, anchor)
        })
    }

    /// Begin a selection anchored at the cursor.
    pub fn start_selection(&mut self) {
        self.selection_anchor = Some(self.cursor);
    }

    /// Drop any selection, leaving the cursor where it is.
    pub fn cancel_selection(&mut self) {
        self.selection_anchor = None;
    }

    /// Select the entire buffer.
    pub fn select_all(&mut self) {
        self.selection_anchor = Some((0, 0));
        let row = self.lines.len() - 1;
        self.cursor = (row, char_count(&self.lines[row]));
    }

    /// Text held by the yank buffer, as left by the last copy, cut or kill.
    pub fn yanked_text(&self) -> &str {
        &self.yank
    }

    /// Overwrite the yank buffer, for example from an external paste.
    pub fn set_yank(&mut self, text: impl Into<String>) {
        self.yank = text.into();
    }

    /// Style applied to the text.
    pub fn style(&self) -> Style {
        self.style
    }

    pub fn set_style(&mut self, style: Style) {
        self.style = style;
    }

    pub fn set_cursor_style(&mut self, style: Style) {
        self.cursor_style = style;
    }

    pub fn set_selection_style(&mut self, style: Style) {
        self.selection_style = style;
    }

    /// Whether the cursor cell is highlighted. Unfocused inputs hide it.
    pub fn set_cursor_visible(&mut self, visible: bool) {
        self.cursor_visible = visible;
    }

    pub fn cursor_visible(&self) -> bool {
        self.cursor_visible
    }

    /// Width of a tab stop, in columns. Tab keys insert this many spaces.
    pub fn set_tab_len(&mut self, len: usize) {
        self.tab_len = len.max(1);
    }

    pub fn tab_len(&self) -> usize {
        self.tab_len
    }

    /// Scroll position as `(first visible row, first visible terminal column)`
    /// from the last render.
    pub fn scroll_offsets(&self) -> (usize, usize) {
        let vp = self.viewport.get();
        (vp.row, vp.col)
    }

    /// Number of characters in the line the cursor is on.
    fn line_len(&self, row: usize) -> usize {
        self.lines.get(row).map(|l| char_count(l)).unwrap_or(0)
    }

    /// Clamp a position onto a real character boundary inside the buffer.
    fn clamp_position(&self, (row, col): (usize, usize)) -> (usize, usize) {
        let row = row.min(self.lines.len() - 1);
        (row, col.min(self.line_len(row)))
    }
}

/// Split `text` into buffer lines, tolerating CRLF and always yielding at least
/// one line.
fn split_lines(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    text.replace("\r\n", "\n")
        .split('\n')
        .map(|s| s.replace('\r', ""))
        .collect()
}

/// Character count of a line.
fn char_count(line: &str) -> usize {
    line.chars().count()
}

/// Byte offset of character `col`, or the line length when `col` is past the end.
fn byte_of_char(line: &str, col: usize) -> usize {
    line.char_indices()
        .nth(col)
        .map(|(i, _)| i)
        .unwrap_or(line.len())
}

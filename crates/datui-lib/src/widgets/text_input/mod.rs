//! The text entry field used everywhere in datui.
//!
//! One widget covers both shapes of input the app needs. A single-line input
//! submits on Enter and recalls previous values with the arrow keys; a
//! multi-line input inserts newlines instead and recalls with `Ctrl-P` /
//! `Ctrl-N`. Everything else, including the editing keys, is shared, so the
//! query bar, the modal filter boxes and the template description all behave
//! the same way.
//!
//! Editing itself belongs to [`crate::widgets::textarea::TextArea`], which this
//! type owns. What is added here is theming, focus, and history of previously
//! submitted values.

pub mod history;

use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::Widget,
};

use crate::cache::CacheManager;
use crate::config::Theme;
use crate::widgets::textarea::{CursorMove, TextArea};

use history::InputHistory;

/// What a key press did to the input, for the caller to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextInputEvent {
    /// The key was handled internally; nothing for the caller to do.
    None,
    /// Enter was pressed on a single-line input.
    Submit,
    /// Esc was pressed.
    Cancel,
    /// The value was replaced by an entry from the history.
    HistoryChanged,
}

/// Whether the field holds one line or many.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TextInputMode {
    /// Enter submits, and the value never contains a newline.
    #[default]
    SingleLine,
    /// Enter inserts a line break.
    MultiLine,
}

/// A themed, optionally history-backed text field.
#[derive(Debug, Clone)]
pub struct TextInput {
    mode: TextInputMode,
    textarea: TextArea,
    /// Mirror of the editor contents, so callers can borrow the value cheaply.
    /// Rewritten by [`TextInput::sync`] after every mutation.
    value: String,
    history: InputHistory,
    text_color: Option<Color>,
    background_color: Option<Color>,
    cursor_color: Option<Color>,
    focused: bool,
}

impl Default for TextInput {
    fn default() -> Self {
        Self::new()
    }
}

impl TextInput {
    /// A single-line field.
    pub fn new() -> Self {
        Self::with_mode(TextInputMode::SingleLine)
    }

    /// A multi-line field.
    pub fn multiline() -> Self {
        Self::with_mode(TextInputMode::MultiLine)
    }

    fn with_mode(mode: TextInputMode) -> Self {
        let mut input = Self {
            mode,
            textarea: TextArea::new(),
            value: String::new(),
            history: InputHistory::new(1000),
            text_color: None,
            background_color: None,
            cursor_color: None,
            focused: false,
        };
        input.apply_styles();
        input
    }

    /// Whether the field holds one line or many.
    pub fn mode(&self) -> TextInputMode {
        self.mode
    }

    fn is_single_line(&self) -> bool {
        self.mode == TextInputMode::SingleLine
    }

    /// Set the text colour.
    pub fn with_text_color(mut self, color: Color) -> Self {
        self.text_color = Some(color);
        self.apply_styles();
        self
    }

    /// Set the background colour of the input area.
    pub fn with_background(mut self, color: Color) -> Self {
        self.background_color = Some(color);
        self.apply_styles();
        self
    }

    /// Take text and cursor colours from the theme.
    pub fn with_theme(mut self, theme: &Theme) -> Self {
        self.text_color = Some(theme.get("text_primary"));
        self.cursor_color = Some(theme.get("cursor_focused"));
        self.apply_styles();
        self
    }

    /// Persist and recall values under `history_id`, which names the cache file.
    pub fn with_history(mut self, history_id: String) -> Self {
        self.history.id = Some(history_id);
        self
    }

    /// Cap how many entries the history keeps.
    pub fn with_history_limit(mut self, limit: usize) -> Self {
        self.history.limit = limit;
        self
    }

    /// Push the current colours into the editor, including the cursor style,
    /// which depends on whether the field has focus.
    fn apply_styles(&mut self) {
        let mut style = Style::default();
        if let Some(color) = self.text_color {
            style = style.fg(color);
        }
        if let Some(color) = self.background_color {
            style = style.bg(color);
        }
        self.textarea.set_style(style);
        self.textarea.set_cursor_style(self.cursor_style());
        self.textarea
            .set_selection_style(Style::default().add_modifier(Modifier::REVERSED));
        self.textarea.set_cursor_visible(self.focused);
    }

    /// The cursor highlight. A theme that leaves the cursor colour unset falls
    /// back to reversing the text, which works on every terminal.
    fn cursor_style(&self) -> Style {
        match self.cursor_color {
            Some(color) if color != Color::Reset => {
                Style::default().bg(color).fg(contrasting_fg(color))
            }
            _ => Style::default().add_modifier(Modifier::REVERSED),
        }
    }

    /// Show or hide the cursor. Only the focused field draws one.
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
        self.textarea.set_cursor_visible(focused);
    }

    pub fn is_focused(&self) -> bool {
        self.focused
    }

    /// The current contents. Multi-line values are newline separated.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Replace the contents, leaving the cursor at the end.
    pub fn set_value(&mut self, value: impl AsRef<str>) {
        let value = value.as_ref();
        if self.is_single_line() {
            self.textarea.set_text(&flatten(value));
        } else {
            self.textarea.set_text(value);
        }
        self.history.reset_position();
        self.sync();
    }

    /// Empty the field and stop any history walk in progress.
    pub fn clear(&mut self) {
        self.textarea.clear();
        self.history.reset_position();
        self.sync();
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Cursor position as a character offset into [`TextInput::value`].
    pub fn cursor(&self) -> usize {
        let (row, col) = self.textarea.cursor();
        self.textarea
            .lines()
            .iter()
            .take(row)
            .map(|line| line.chars().count() + 1)
            .sum::<usize>()
            + col
    }

    /// Move the cursor to a character offset into [`TextInput::value`].
    pub fn set_cursor(&mut self, cursor: usize) {
        let (row, col) = self.line_col_of(cursor);
        self.textarea.set_cursor(row, col);
    }

    /// Line the cursor is on, counting from zero.
    pub fn cursor_line(&self) -> usize {
        self.textarea.cursor().0
    }

    /// Character offset of the cursor within its line.
    pub fn cursor_col(&self) -> usize {
        self.textarea.cursor().1
    }

    /// Move the cursor to a line and column, clamped into the text.
    pub fn set_cursor_line_col(&mut self, line: usize, col: usize) {
        self.textarea.set_cursor(line, col);
    }

    /// Move the cursor up or down by whole lines, clamped at the ends.
    pub fn move_cursor_by_lines(&mut self, delta: isize) {
        let movement = if delta < 0 {
            CursorMove::UpBy(delta.unsigned_abs())
        } else {
            CursorMove::DownBy(delta as usize)
        };
        self.textarea.move_cursor(movement);
    }

    /// Number of lines in the value; always at least one.
    pub fn line_count(&self) -> usize {
        self.textarea.line_count()
    }

    /// The text of one line, if it exists.
    pub fn line_at(&self, line: usize) -> Option<&str> {
        self.textarea.line(line)
    }

    /// Scroll position of the last render, as `(row, column)`.
    pub fn scroll_offsets(&self) -> (usize, usize) {
        self.textarea.scroll_offsets()
    }

    /// The history entries loaded so far. Empty until something loads them.
    pub fn history_entries(&self) -> &[String] {
        self.history.entries()
    }

    /// Load the history from the cache if it has not been loaded yet.
    pub fn load_history(&mut self, cache: &CacheManager) -> Result<()> {
        self.history.ensure_loaded(cache)
    }

    /// Add the current value to the history and persist it.
    pub fn save_to_history(&mut self, cache: &CacheManager) -> Result<()> {
        let value = self.value.clone();
        self.history.remember(&value, cache)
    }

    /// Replace the value with an older history entry.
    pub fn navigate_history_up(&mut self, cache: Option<&CacheManager>) {
        let current = self.value.clone();
        if let Some(entry) = self.history.older(&current, cache) {
            self.textarea.set_text(&entry);
            self.sync();
        }
    }

    /// Replace the value with a newer history entry, or the value that was
    /// being edited before the walk started.
    pub fn navigate_history_down(&mut self) {
        if let Some(entry) = self.history.newer() {
            self.textarea.set_text(&entry);
            self.sync();
        }
    }

    /// Handle one key press.
    ///
    /// `cache` is only needed by inputs that have a history; pass `None` when
    /// there is none or when the caller does not want disk access.
    pub fn handle_key(&mut self, event: &KeyEvent, cache: Option<&CacheManager>) -> TextInputEvent {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let single_line = self.is_single_line();
        let recall = self.history.is_enabled();

        match event.code {
            KeyCode::Esc => TextInputEvent::Cancel,
            KeyCode::Enter if single_line => {
                if let Some(cache) = cache {
                    let _ = self.save_to_history(cache);
                }
                TextInputEvent::Submit
            }
            KeyCode::Char('m' | 'M') if ctrl && single_line => {
                if let Some(cache) = cache {
                    let _ = self.save_to_history(cache);
                }
                TextInputEvent::Submit
            }
            KeyCode::Up if single_line && recall => {
                self.navigate_history_up(cache);
                TextInputEvent::HistoryChanged
            }
            KeyCode::Down if single_line && recall => {
                self.navigate_history_down();
                TextInputEvent::HistoryChanged
            }
            KeyCode::Char('p' | 'P') if ctrl && recall => {
                self.navigate_history_up(cache);
                TextInputEvent::HistoryChanged
            }
            KeyCode::Char('n' | 'N') if ctrl && recall => {
                self.navigate_history_down();
                TextInputEvent::HistoryChanged
            }
            _ => {
                if self.textarea.input(event) {
                    self.history.reset_position();
                }
                self.sync();
                TextInputEvent::None
            }
        }
    }

    /// Refresh the mirrored value, collapsing a single-line field back onto one
    /// line if an edit somehow introduced a break.
    fn sync(&mut self) {
        if self.is_single_line() && self.textarea.line_count() > 1 {
            let flattened = flatten(&self.textarea.text());
            self.textarea.set_text(&flattened);
        }
        self.value = self.textarea.text();
    }

    /// Split a character offset into the value into a line and column.
    fn line_col_of(&self, cursor: usize) -> (usize, usize) {
        let mut remaining = cursor;
        for (row, line) in self.textarea.lines().iter().enumerate() {
            let len = line.chars().count();
            if remaining <= len {
                return (row, remaining);
            }
            remaining -= len + 1;
        }
        let last = self.textarea.line_count() - 1;
        (last, self.textarea.line(last).unwrap_or("").chars().count())
    }
}

impl Widget for &TextInput {
    fn render(self, area: Rect, buf: &mut Buffer) {
        (&self.textarea).render(area, buf);
    }
}

/// Collapse line breaks so a single-line field stays on one line.
fn flatten(value: &str) -> String {
    value.replace(['\n', '\r'], " ")
}

/// A readable foreground for a solid cursor block of the given colour.
fn contrasting_fg(cursor: Color) -> Color {
    match cursor {
        Color::Black | Color::Red | Color::Blue | Color::Magenta | Color::DarkGray => Color::White,
        _ => Color::Black,
    }
}

#[cfg(test)]
mod tests;

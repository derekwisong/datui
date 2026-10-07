//! The text entry field used everywhere. Single-line inputs submit on Enter and recall
//! history with arrows; multi-line ones insert newlines and recall with Ctrl-P/Ctrl-N;
//! statements submit on Enter, break lines on Alt+Enter and wrap. Editing belongs to
//! [`crate::widgets::textarea::TextArea`]; this adds theming, focus and history.

pub mod history;

use crate::logging::LogFailure;
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
    /// Enter submits and Alt+Enter breaks the line: a statement that is run,
    /// long enough to want several lines. Long lines wrap instead of
    /// scrolling, and ↑/↓ move between rows before they recall history.
    Statement,
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
    /// Text under the cursor block; the theme picks it, never this widget.
    cursor_text: Option<Color>,
    /// How selected text is drawn; the theme picks it.
    selection_style: Option<Style>,
    focused: bool,
    /// The value is one the form proposed, untouched since. While it holds,
    /// the whole value is selected whenever the field has focus.
    suggested: bool,
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

    /// A field for a statement: Enter runs it, Alt+Enter breaks the line.
    pub fn statement() -> Self {
        Self::with_mode(TextInputMode::Statement)
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
            cursor_text: None,
            selection_style: None,
            focused: false,
            suggested: false,
        };
        input.textarea.set_wrap(mode == TextInputMode::Statement);
        input.apply_styles();
        input
    }

    /// Whether the field holds one line or many.
    #[cfg(test)]
    pub fn mode(&self) -> TextInputMode {
        self.mode
    }

    fn is_single_line(&self) -> bool {
        self.mode == TextInputMode::SingleLine
    }

    /// Whether Enter hands the value to the caller rather than typing.
    fn submits_on_enter(&self) -> bool {
        self.mode != TextInputMode::MultiLine
    }

    /// Set the text colour.
    #[cfg(test)]
    pub fn with_text_color(mut self, color: Color) -> Self {
        self.text_color = Some(color);
        self.apply_styles();
        self
    }

    /// Set the background colour of the input area.
    #[cfg(test)]
    pub fn with_background(mut self, color: Color) -> Self {
        self.background_color = Some(color);
        self.apply_styles();
        self
    }

    /// Take text and cursor colours from the theme.
    pub fn with_theme(mut self, theme: &Theme) -> Self {
        self.text_color = Some(theme.text_primary());
        let cursor = theme.input_cursor();
        self.cursor_color = Some(cursor);
        self.cursor_text = Some(theme.cursor_text_for(cursor));
        self.selection_style = Some(theme.text_selection_style());
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
        self.textarea.set_selection_style(
            self.selection_style
                .unwrap_or_else(|| Style::default().add_modifier(Modifier::REVERSED)),
        );
        self.textarea.set_cursor_visible(self.focused);
    }

    /// The cursor highlight. A theme that leaves the cursor colour unset falls
    /// back to reversing the text, which works on every terminal.
    fn cursor_style(&self) -> Style {
        match (self.cursor_color, self.cursor_text) {
            (Some(color), Some(text)) if color != Color::Reset => {
                Style::default().bg(color).fg(text)
            }
            _ => Style::default().add_modifier(Modifier::REVERSED),
        }
    }

    /// Show or hide the cursor. Only the focused field draws one.
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
        self.textarea.set_cursor_visible(focused);
        if self.suggested {
            if focused {
                self.textarea.select_all();
            } else {
                self.textarea.cancel_selection();
            }
        }
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
        self.suggested = false;
        let value = value.as_ref();
        if self.is_single_line() {
            self.textarea.set_text(&flatten(value));
        } else {
            self.textarea.set_text(value);
        }
        self.history.reset_position();
        self.sync();
    }

    /// Fill with a proposed default: until the first key it is selected while focused (a
    /// printable replaces it, Backspace or Delete clears it, movement or Enter keeps it);
    /// leaving drops the selection.
    pub fn suggest(&mut self, value: impl AsRef<str>) {
        self.set_value(value);
        self.suggested = !self.value.is_empty();
        if self.suggested && self.focused {
            self.textarea.select_all();
        }
    }

    /// Whether the value is still the untouched default from [`TextInput::suggest`].
    pub fn is_suggested(&self) -> bool {
        self.suggested
    }

    /// Select the whole value, so the next printable replaces it while any
    /// cursor movement drops the selection and edits in place.
    pub fn select_all(&mut self) {
        self.textarea.select_all();
    }

    /// Empty the field and stop any history walk in progress.
    pub fn clear(&mut self) {
        self.suggested = false;
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
    #[cfg(test)]
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
    #[cfg(test)]
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

    /// Rows the value takes when drawn `width` columns wide: its lines, or
    /// for a statement, its lines as wrapped.
    pub fn visual_rows(&self, width: u16) -> usize {
        self.textarea.visual_rows(width)
    }

    /// Replace the `count` characters before the cursor with `text`.
    pub fn replace_before_cursor(&mut self, count: usize, text: &str) {
        self.suggested = false;
        self.textarea.replace_before_cursor(count, text);
        self.history.reset_position();
        self.sync();
    }

    /// Scroll position of the last render, as `(row, column)`.
    #[cfg(test)]
    pub fn scroll_offsets(&self) -> (usize, usize) {
        self.textarea.scroll_offsets()
    }

    /// The history entries loaded so far. Empty until something loads them.
    #[cfg(test)]
    pub fn history_entries(&self) -> &[String] {
        self.history.entries()
    }

    /// Load the history from the cache if it has not been loaded yet.
    #[cfg(test)]
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
        self.suggested = false;
        let current = self.value.clone();
        if let Some(entry) = self.history.older(&current, cache) {
            self.textarea.set_text(&entry);
            self.sync();
        }
    }

    /// Replace the value with a newer history entry, or the value that was
    /// being edited before the walk started.
    pub fn navigate_history_down(&mut self) {
        self.suggested = false;
        if let Some(entry) = self.history.newer() {
            self.textarea.set_text(&entry);
            self.sync();
        }
    }

    /// Handle one key press; `cache` only for inputs with history (`None` skips disk).
    pub fn handle_key(&mut self, event: &KeyEvent, cache: Option<&CacheManager>) -> TextInputEvent {
        if event.code == KeyCode::Esc {
            return TextInputEvent::Cancel;
        }
        if !std::mem::take(&mut self.suggested) {
            return self.apply_key(event, cache);
        }
        // The first key settles a suggestion. It acts on the whole value even when
        // focus never reached the field through `set_focused`.
        self.textarea.select_all();
        let selected = self.textarea.selection();
        let before = self.value.clone();
        let result = self.apply_key(event, cache);
        // A key the editor has no use for changes nothing, so the value is still
        // the form's proposal.
        self.suggested = self.value == before && self.textarea.selection() == selected;
        if self.suggested && !self.focused {
            self.textarea.cancel_selection();
        }
        result
    }

    fn apply_key(&mut self, event: &KeyEvent, cache: Option<&CacheManager>) -> TextInputEvent {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let alt = event.modifiers.contains(KeyModifiers::ALT);
        let single_line = self.is_single_line();
        let statement = self.mode == TextInputMode::Statement;
        let submits = self.submits_on_enter();
        let recall = self.history.is_enabled();

        match event.code {
            // Alt, not Ctrl: legacy terminals send Ctrl+Enter as Ctrl+J, and both
            // submit.
            KeyCode::Enter if statement && alt => {
                self.textarea.insert_newline();
                self.history.reset_position();
                self.sync();
                TextInputEvent::None
            }
            KeyCode::Enter if submits => self.submit(cache),
            KeyCode::Char('m' | 'M') if ctrl && submits => self.submit(cache),
            // The save chord, in every mode. Without the keyboard-enhancement
            // protocol some terminals send Ctrl+Enter as Ctrl+J, so the two must
            // mean the same thing; in a multiline field plain Enter types.
            KeyCode::Enter | KeyCode::Char('j' | 'J') if ctrl => self.submit(cache),
            KeyCode::Up if single_line && recall => {
                self.navigate_history_up(cache);
                TextInputEvent::HistoryChanged
            }
            KeyCode::Down if single_line && recall => {
                self.navigate_history_down();
                TextInputEvent::HistoryChanged
            }
            // A statement's rows come first; past the top or bottom row the
            // arrows walk the history as they do in a one-line field.
            KeyCode::Up | KeyCode::Down if statement && event.modifiers.is_empty() => {
                let up = event.code == KeyCode::Up;
                let moved =
                    self.textarea
                        .move_cursor(if up { CursorMove::Up } else { CursorMove::Down });
                if moved || !recall {
                    self.sync();
                    return TextInputEvent::None;
                }
                if up {
                    self.navigate_history_up(cache);
                } else {
                    self.navigate_history_down();
                }
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

    fn submit(&mut self, cache: Option<&CacheManager>) -> TextInputEvent {
        self.textarea.cancel_selection();
        if let Some(cache) = cache {
            self.save_to_history(cache).or_log("save input history");
        }
        TextInputEvent::Submit
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
    #[cfg(test)]
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

#[cfg(test)]
mod tests;

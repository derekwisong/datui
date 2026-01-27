use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::Widget,
};
use tui_textarea::{Input, Key, TextArea};

use crate::cache::CacheManager;
use crate::config::Theme;

use super::text_input_common::{add_to_history, load_history_impl, save_history_impl};

/// Event emitted by TextInput widget
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextInputEvent {
    None,
    Submit,         // Enter pressed
    Cancel,         // Esc pressed
    HistoryChanged, // History navigation occurred
}

/// Single-line text input widget wrapping tui-textarea with history support
pub struct TextInput {
    textarea: TextArea<'static>,
    // Public fields for backward compatibility (kept in sync with textarea)
    pub value: String,
    pub cursor: usize,
    pub history_id: Option<String>, // None = no history, Some(id) = use history with this ID
    pub history: Vec<String>,       // Loaded history entries (lazy-loaded)
    pub history_index: Option<usize>, // Current position in history (None = editing new value)
    pub history_temp: Option<String>, // Temporary storage when navigating history
    pub history_limit: usize,       // Maximum number of history entries to keep
    pub history_loaded: bool,       // Track if history has been loaded (for lazy loading)
    // Styling (internal, set via builder methods)
    text_color: Option<Color>,
    cursor_bg: Option<Color>,
    cursor_fg: Option<Color>,
    background_color: Option<Color>,
    cursor_focused: Option<Color>, // Cursor color when focused (from theme)
    focused: bool,                 // Whether the widget is currently focused
}

impl TextInput {
    /// Create a new TextInput widget
    pub fn new() -> Self {
        let mut textarea = TextArea::default();
        // Configure for single-line: disable cursor line underline
        // Don't set line_number_style - leaving it unset means no line numbers (default behavior)
        textarea.set_cursor_line_style(Style::default()); // No underline

        let mut widget = Self {
            textarea,
            value: String::new(),
            cursor: 0,
            history_id: None,
            history: Vec::new(),
            history_index: None,
            history_temp: None,
            history_limit: 1000,
            history_loaded: false,
            text_color: None,
            cursor_bg: None,
            cursor_fg: None,
            background_color: None,
            cursor_focused: None,
            focused: false,
        };
        // Apply any colors that were set (none initially, but this ensures consistency)
        widget.apply_colors_to_textarea();
        widget
    }

    /// Sync value and cursor from textarea
    fn sync_from_textarea(&mut self) {
        self.value = self.textarea.lines().first().cloned().unwrap_or_default();
        self.cursor = self.textarea.cursor().1;
    }

    /// Apply text and background colors to the textarea
    fn apply_colors_to_textarea(&mut self) {
        // Build style from text_color and background_color
        // Ensure no modifiers (like underline) are applied
        let mut style = Style::default();
        if let Some(text_color) = self.text_color {
            style = style.fg(text_color);
        }
        if let Some(bg_color) = self.background_color {
            style = style.bg(bg_color);
        }
        // Apply style to textarea - this sets the text style (no underline by default)
        self.textarea.set_style(style);
        // Disable cursor line underline (this is separate from text underline)
        self.textarea.set_cursor_line_style(Style::default());
    }

    /// Sync textarea from value and cursor
    fn sync_to_textarea(&mut self) {
        let single_line = self.value.replace(['\n', '\r'], " ");
        self.textarea = TextArea::new(vec![single_line]);
        // Re-apply colors and cursor line style configuration
        // This is necessary because creating a new TextArea resets the style
        self.apply_colors_to_textarea();
        // Re-apply cursor style based on focus state (since textarea was recreated)
        let was_focused = self.focused;
        self.focused = false; // Temporarily set to false so set_focused will update
        self.set_focused(was_focused);
        use tui_textarea::CursorMove;
        self.textarea.move_cursor(CursorMove::Jump(
            0,
            self.cursor.min(u16::MAX as usize) as u16,
        ));
    }

    /// Set all colors at once
    /// Note: cursor_bg and cursor_fg are deprecated - cursor colors are now automatically reversed
    #[allow(deprecated)]
    pub fn with_style(mut self, text_color: Color, cursor_bg: Color, cursor_fg: Color) -> Self {
        self.text_color = Some(text_color);
        self.cursor_bg = Some(cursor_bg);
        self.cursor_fg = Some(cursor_fg);
        self.apply_colors_to_textarea();
        self
    }

    /// Set text color only
    pub fn with_text_color(mut self, color: Color) -> Self {
        self.text_color = Some(color);
        self.apply_colors_to_textarea();
        self
    }

    /// Set cursor colors only (deprecated: cursor colors are now automatically reversed)
    #[deprecated(note = "Cursor colors are now automatically reversed from text/background colors")]
    pub fn with_cursor_colors(mut self, bg: Color, fg: Color) -> Self {
        self.cursor_bg = Some(bg);
        self.cursor_fg = Some(fg);
        self
    }

    /// Set optional background color for input area
    pub fn with_background(mut self, color: Color) -> Self {
        self.background_color = Some(color);
        self.apply_colors_to_textarea();
        self
    }

    /// Convenience method to set colors from theme
    /// Cursor colors come from theme cursor_focused setting
    /// If cursor_focused is "default" (Color::Reset), uses REVERSED modifier (tui-textarea default)
    pub fn with_theme(mut self, theme: &Theme) -> Self {
        let text_primary = theme.get("text_primary");
        // Set text color from theme
        self.text_color = Some(text_primary);
        // Set cursor color from theme (defaults to Color::Reset which uses REVERSED)
        self.cursor_focused = Some(theme.get("cursor_focused"));
        self.apply_colors_to_textarea();
        self
    }

    /// Enable history with the given ID
    pub fn with_history(mut self, history_id: String) -> Self {
        self.history_id = Some(history_id);
        self
    }

    /// Set history limit
    pub fn with_history_limit(mut self, limit: usize) -> Self {
        self.history_limit = limit;
        self
    }

    /// Set focused state
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
        // Use tui-textarea's set_cursor_style to hide/show cursor
        // Setting the same style as text hides the cursor (per tui-textarea docs)
        if focused {
            // When focused, always use a visible cursor style
            // Default to REVERSED modifier (tui-textarea's default) for maximum compatibility
            let cursor_color = self.cursor_focused.unwrap_or(Color::Reset);
            let cursor_style = if cursor_color == Color::Reset {
                // Use reversed modifier (tui-textarea default behavior)
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                // Use theme-based cursor color with explicit background
                let text_color = match cursor_color {
                    Color::White => Color::Black,
                    Color::Black => Color::White,
                    Color::Red => Color::White,
                    Color::Green => Color::Black,
                    Color::Yellow => Color::Black,
                    Color::Blue => Color::White,
                    Color::Magenta => Color::White,
                    Color::Cyan => Color::Black,
                    Color::Gray => Color::Black,
                    Color::DarkGray => Color::White,
                    Color::LightRed => Color::Black,
                    Color::LightGreen => Color::Black,
                    Color::LightYellow => Color::Black,
                    Color::LightBlue => Color::Black,
                    Color::LightMagenta => Color::Black,
                    Color::LightCyan => Color::Black,
                    _ => Color::Black,
                };
                Style::default().bg(cursor_color).fg(text_color)
            };
            self.textarea.set_cursor_style(cursor_style);
        } else {
            // When unfocused, set cursor style to exactly match textarea's text style (hides cursor)
            // Get the actual textarea style to match it exactly
            let textarea_style = self.textarea.style();
            self.textarea.set_cursor_style(textarea_style);
        }
    }

    /// Get the current value (single line)
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Set the value
    pub fn set_value(&mut self, value: String) {
        self.value = value;
        self.sync_to_textarea();
    }

    /// Get cursor position
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Set cursor position
    pub fn set_cursor(&mut self, cursor: usize) {
        self.cursor = cursor;
        use tui_textarea::CursorMove;
        self.textarea
            .move_cursor(CursorMove::Jump(0, cursor.min(u16::MAX as usize) as u16));
    }

    /// Load history from cache (lazy loading)
    pub fn load_history(&mut self, cache: &CacheManager) -> Result<()> {
        if self.history_loaded {
            return Ok(());
        }
        if let Some(ref history_id) = self.history_id {
            self.history = load_history_impl(cache, history_id)?;
            self.history_loaded = true;
        }
        Ok(())
    }

    /// Save current value to history
    pub fn save_to_history(&mut self, cache: &CacheManager) -> Result<()> {
        if let Some(history_id) = self.history_id.clone() {
            self.sync_from_textarea(); // Ensure value is up to date
            if !self.value.is_empty() {
                // Add to history with deduplication
                add_to_history(&mut self.history, self.value.clone());
                // Save to cache
                save_history_impl(cache, &history_id, &self.history, self.history_limit)?;
            }
        }
        Ok(())
    }

    /// Clear the input
    pub fn clear(&mut self) {
        self.textarea = TextArea::default();
        self.value.clear();
        self.cursor = 0;
        self.history_index = None;
        self.history_temp = None;
    }

    /// Check if input is empty
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Navigate history up (older entries)
    pub fn navigate_history_up(&mut self, cache: Option<&CacheManager>) {
        if self.history_id.is_none() {
            return;
        }

        // Lazy load history if needed
        if !self.history_loaded {
            if let Some(cache) = cache {
                if let Err(e) = self.load_history(cache) {
                    eprintln!("Warning: Could not load history: {}", e);
                    return;
                }
            } else {
                return;
            }
        }

        if self.history.is_empty() {
            return;
        }

        // Save current value to temp if we're starting history navigation
        if self.history_index.is_none() {
            self.sync_from_textarea(); // Ensure value is up to date
            self.history_temp = Some(self.value.clone());
        }

        // Move to previous (older) entry
        let new_index = if let Some(current_idx) = self.history_index {
            if current_idx > 0 {
                current_idx - 1
            } else {
                current_idx // Already at oldest
            }
        } else {
            self.history.len() - 1 // Start from most recent
        };

        self.history_index = Some(new_index);
        if let Some(entry) = self.history.get(new_index) {
            self.value = entry.clone();
            self.cursor = self.value.chars().count();
            self.sync_to_textarea();
        }
    }

    /// Navigate history down (newer entries)
    pub fn navigate_history_down(&mut self) {
        if self.history_id.is_none() || self.history_index.is_none() {
            return;
        }

        let current_idx = self.history_index.unwrap();
        if current_idx >= self.history.len() - 1 {
            // Restore temp value
            if let Some(ref temp) = self.history_temp {
                self.value = temp.clone();
                self.cursor = self.value.chars().count();
                self.sync_to_textarea();
            }
            self.history_index = None;
            self.history_temp = None;
        } else {
            // Move to next (newer) entry
            let new_index = current_idx + 1;
            self.history_index = Some(new_index);
            if let Some(entry) = self.history.get(new_index) {
                self.value = entry.clone();
                self.cursor = self.value.chars().count();
                self.sync_to_textarea();
            }
        }
    }

    /// Handle a key event
    pub fn handle_key(&mut self, event: &KeyEvent, cache: Option<&CacheManager>) -> TextInputEvent {
        // Convert KeyEvent to tui_textarea::Input
        let input = self.key_event_to_input(event);

        match event.code {
            KeyCode::Enter => {
                // For single-line, Enter means submit (don't insert newline)
                // Save to history before submitting
                if let Some(cache) = cache {
                    let _ = self.save_to_history(cache);
                }
                return TextInputEvent::Submit;
            }
            KeyCode::Esc => {
                return TextInputEvent::Cancel;
            }
            KeyCode::Up if self.history_id.is_some() => {
                self.navigate_history_up(cache);
                return TextInputEvent::HistoryChanged;
            }
            KeyCode::Down if self.history_id.is_some() => {
                self.navigate_history_down();
                return TextInputEvent::HistoryChanged;
            }
            _ => {
                // For single-line input, ignore newline insertion
                if matches!(input.key, Key::Char('\n') | Key::Char('\r')) {
                    return TextInputEvent::None;
                }
                // Handle the input
                self.textarea.input(input);
                // Sync value and cursor from textarea
                self.sync_from_textarea();
                // Clear history navigation state when user types
                if self.history_index.is_some() {
                    self.history_index = None;
                    self.history_temp = None;
                }
            }
        }
        TextInputEvent::None
    }

    /// Convert crossterm KeyEvent to tui_textarea::Input
    fn key_event_to_input(&self, event: &KeyEvent) -> Input {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        let alt = event.modifiers.contains(KeyModifiers::ALT);
        let shift = event.modifiers.contains(KeyModifiers::SHIFT);

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
            KeyCode::BackTab => Key::Tab, // BackTab as Tab
            KeyCode::Delete => Key::Delete,
            KeyCode::Insert => Key::Null, // Insert not supported
            KeyCode::F(_) => Key::Null,
            KeyCode::Null => Key::Null,
            KeyCode::Esc => Key::Esc,
            KeyCode::CapsLock
            | KeyCode::ScrollLock
            | KeyCode::NumLock
            | KeyCode::PrintScreen
            | KeyCode::Pause
            | KeyCode::Menu
            | KeyCode::Media(_)
            | KeyCode::Modifier(_)
            | KeyCode::KeypadBegin => Key::Null,
        };

        Input {
            key,
            ctrl,
            alt,
            shift,
        }
    }
}

impl Default for TextInput {
    fn default() -> Self {
        Self::new()
    }
}

impl Widget for &TextInput {
    fn render(self, area: Rect, buf: &mut ratatui::buffer::Buffer) {
        // Render the textarea - it handles all text rendering and styling
        self.textarea.render(area, buf);

        // Remove underline modifier from all cells (tui-textarea handles cursor visibility via set_cursor_style)
        for y in area.y..area.bottom() {
            for x in area.x..area.right() {
                let cell = &mut buf[(x, y)];
                let mut style = cell.style();
                style = style.remove_modifier(Modifier::UNDERLINED);
                cell.set_style(style);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_input_new() {
        let input = TextInput::new();
        assert_eq!(input.value(), "");
        assert_eq!(input.cursor(), 0);
        assert_eq!(input.history_id, None);
        assert_eq!(input.history_limit, 1000);
        assert!(!input.focused);
    }

    #[test]
    fn test_set_value() {
        let mut input = TextInput::new();
        input.set_value("hello".to_string());
        assert_eq!(input.value(), "hello");
    }

    #[test]
    fn test_clear() {
        let mut input = TextInput::new();
        input.set_value("hello".to_string());
        input.clear();
        assert_eq!(input.value(), "");
    }

    #[test]
    fn test_is_empty() {
        let mut input = TextInput::new();
        assert!(input.is_empty());
        input.set_value("hello".to_string());
        assert!(!input.is_empty());
    }
}

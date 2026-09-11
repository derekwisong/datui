//! Editing primitives and cursor movement for [`TextArea`].
//!
//! Every mutation funnels through [`TextArea::replace_range`], which swaps one
//! contiguous range for one string and records the delta on the undo stack.
//! Building each operation out of that single primitive is what keeps undo
//! exact: the inverse of a replacement is another replacement.

use super::cursor::{next_word_start, prev_word_start, CursorMove};
use super::history::Edit;
use super::{byte_of_char, char_count, split_lines, TextArea};

impl TextArea {
    /// Replace `[start, end)` with `text`, recording it for undo.
    ///
    /// Returns whether the buffer changed. The cursor lands at the end of the
    /// inserted text.
    fn replace_range(&mut self, start: (usize, usize), end: (usize, usize), text: &str) -> bool {
        let start = self.clamp_position(start);
        let end = self.clamp_position(end);
        if start == end && text.is_empty() {
            return false;
        }
        // Normalise first so that what is recorded for undo is exactly what
        // lands in the buffer, even if the caller pasted CRLF line endings.
        let inserted = split_lines(text).join("\n");
        let cursor_before = self.cursor;
        let removed = self.remove_raw(start, end);
        let cursor_after = self.insert_raw(start, &inserted);
        self.cursor = cursor_after;
        self.selection_anchor = None;
        self.history.push(Edit {
            start,
            removed,
            inserted,
            cursor_before,
            cursor_after,
        });
        true
    }

    /// Delete `[start, end)` without touching the undo stack, returning the
    /// text that was there.
    fn remove_raw(&mut self, (sr, sc): (usize, usize), (er, ec): (usize, usize)) -> String {
        if (sr, sc) == (er, ec) {
            return String::new();
        }
        let sb = byte_of_char(&self.lines[sr], sc);
        let eb = byte_of_char(&self.lines[er], ec);
        if sr == er {
            let removed = self.lines[sr][sb..eb].to_string();
            self.lines[sr].replace_range(sb..eb, "");
            return removed;
        }
        let mut removed = self.lines[sr][sb..].to_string();
        for line in &self.lines[sr + 1..er] {
            removed.push('\n');
            removed.push_str(line);
        }
        removed.push('\n');
        removed.push_str(&self.lines[er][..eb]);

        let tail = self.lines[er][eb..].to_string();
        self.lines[sr].truncate(sb);
        self.lines[sr].push_str(&tail);
        self.lines.drain(sr + 1..=er);
        removed
    }

    /// Insert `text` at a position without touching the undo stack, returning
    /// the position just past what was inserted.
    fn insert_raw(&mut self, (row, col): (usize, usize), text: &str) -> (usize, usize) {
        if text.is_empty() {
            return (row, col);
        }
        let byte = byte_of_char(&self.lines[row], col);
        let tail = self.lines[row].split_off(byte);
        let mut chunks = split_lines(text);
        let first = chunks.remove(0);
        self.lines[row].push_str(&first);

        if chunks.is_empty() {
            let end = (row, col + char_count(&first));
            self.lines[row].push_str(&tail);
            return end;
        }

        let end_row = row + chunks.len();
        let end_col = char_count(chunks.last().expect("chunks is non-empty"));
        let last = chunks.len() - 1;
        chunks[last].push_str(&tail);
        for (offset, line) in chunks.into_iter().enumerate() {
            self.lines.insert(row + 1 + offset, line);
        }
        (end_row, end_col)
    }

    /// The range an edit should operate on: the selection if there is one,
    /// otherwise the empty range at the cursor.
    fn edit_range(&self) -> ((usize, usize), (usize, usize)) {
        self.selection().unwrap_or((self.cursor, self.cursor))
    }

    /// Insert one character, replacing the selection if there is one.
    pub fn insert_char(&mut self, c: char) -> bool {
        let mut buf = [0u8; 4];
        let (start, end) = self.edit_range();
        self.replace_range(start, end, c.encode_utf8(&mut buf))
    }

    /// Insert a string, replacing the selection if there is one. Newlines in
    /// `text` split lines.
    pub fn insert_str(&mut self, text: &str) -> bool {
        let (start, end) = self.edit_range();
        self.replace_range(start, end, text)
    }

    /// Split the current line at the cursor.
    pub fn insert_newline(&mut self) -> bool {
        let (start, end) = self.edit_range();
        self.replace_range(start, end, "\n")
    }

    /// Insert spaces up to the next tab stop.
    pub fn insert_tab(&mut self) -> bool {
        let width = self.tab_len - (self.cursor.1 % self.tab_len);
        let spaces = " ".repeat(width);
        self.insert_str(&spaces)
    }

    /// Delete the selection, or the character before the cursor.
    pub fn delete_prev_char(&mut self) -> bool {
        if let Some((start, end)) = self.selection() {
            return self.replace_range(start, end, "");
        }
        let Some(prev) = self.position_before(self.cursor) else {
            return false;
        };
        self.replace_range(prev, self.cursor, "")
    }

    /// Delete the selection, or the character after the cursor.
    pub fn delete_next_char(&mut self) -> bool {
        if let Some((start, end)) = self.selection() {
            return self.replace_range(start, end, "");
        }
        let Some(next) = self.position_after(self.cursor) else {
            return false;
        };
        self.replace_range(self.cursor, next, "")
    }

    /// Delete backwards to the start of the previous word, yanking it.
    pub fn delete_prev_word(&mut self) -> bool {
        if let Some((start, end)) = self.selection() {
            return self.yank_and_replace(start, end);
        }
        let (row, col) = self.cursor;
        if col == 0 {
            return self.delete_prev_char();
        }
        let chars: Vec<char> = self.lines[row].chars().collect();
        let target = prev_word_start(&chars, col);
        self.yank_and_replace((row, target), self.cursor)
    }

    /// Delete forwards to the start of the next word, yanking it.
    pub fn delete_next_word(&mut self) -> bool {
        if let Some((start, end)) = self.selection() {
            return self.yank_and_replace(start, end);
        }
        let (row, col) = self.cursor;
        let chars: Vec<char> = self.lines[row].chars().collect();
        if col >= chars.len() {
            return self.delete_next_char();
        }
        let target = next_word_start(&chars, col);
        self.yank_and_replace(self.cursor, (row, target))
    }

    /// Delete from the cursor to the end of the line, yanking it. At the end of
    /// a line this joins the line below, matching readline's `kill-line`.
    pub fn delete_line_by_end(&mut self) -> bool {
        let (row, col) = self.cursor;
        let len = self.line_len(row);
        if col < len {
            return self.yank_and_replace(self.cursor, (row, len));
        }
        if row + 1 < self.lines.len() {
            return self.replace_range(self.cursor, (row + 1, 0), "");
        }
        false
    }

    /// Delete from the start of the line to the cursor, yanking it. At the
    /// start of a line this joins onto the line above.
    pub fn delete_line_by_head(&mut self) -> bool {
        let (row, col) = self.cursor;
        if col > 0 {
            return self.yank_and_replace((row, 0), self.cursor);
        }
        if row > 0 {
            let prev_len = self.line_len(row - 1);
            return self.replace_range((row - 1, prev_len), self.cursor, "");
        }
        false
    }

    /// Delete the active selection, if any.
    pub fn delete_selection(&mut self) -> bool {
        match self.selection() {
            Some((start, end)) => self.replace_range(start, end, ""),
            None => false,
        }
    }

    /// Copy the selection into the yank buffer.
    pub fn copy(&mut self) -> bool {
        let Some((start, end)) = self.selection() else {
            return false;
        };
        self.yank = self.text_in_range(start, end);
        self.selection_anchor = None;
        true
    }

    /// Move the selection into the yank buffer.
    pub fn cut(&mut self) -> bool {
        match self.selection() {
            Some((start, end)) => self.yank_and_replace(start, end),
            None => false,
        }
    }

    /// Insert the yank buffer at the cursor.
    pub fn paste(&mut self) -> bool {
        if self.yank.is_empty() {
            return false;
        }
        let yank = std::mem::take(&mut self.yank);
        let inserted = self.insert_str(&yank);
        self.yank = yank;
        inserted
    }

    /// Undo the most recent edit.
    pub fn undo(&mut self) -> bool {
        let Some(edit) = self.history.pop_undo() else {
            return false;
        };
        let inverse = edit.inverted();
        self.apply_without_recording(&inverse);
        self.history.push_redo(edit);
        true
    }

    /// Redo the most recently undone edit.
    pub fn redo(&mut self) -> bool {
        let Some(edit) = self.history.pop_redo() else {
            return false;
        };
        self.apply_without_recording(&edit);
        self.history.push_undo(edit);
        true
    }

    fn apply_without_recording(&mut self, edit: &Edit) {
        let end = self.position_advanced(edit.start, &edit.removed);
        self.remove_raw(edit.start, end);
        self.insert_raw(edit.start, &edit.inserted);
        self.cursor = self.clamp_position(edit.cursor_after);
        self.selection_anchor = None;
    }

    /// Where `text` ends if it starts at `from`.
    fn position_advanced(&self, (row, col): (usize, usize), text: &str) -> (usize, usize) {
        if text.is_empty() {
            return (row, col);
        }
        let mut lines = text.split('\n');
        let first = lines.next().unwrap_or("");
        let rest: Vec<&str> = lines.collect();
        if rest.is_empty() {
            (row, col + char_count(first))
        } else {
            (
                row + rest.len(),
                char_count(rest.last().expect("rest is non-empty")),
            )
        }
    }

    fn yank_and_replace(&mut self, start: (usize, usize), end: (usize, usize)) -> bool {
        let text = self.text_in_range(start, end);
        if text.is_empty() {
            return false;
        }
        self.yank = text;
        self.replace_range(start, end, "")
    }

    /// The text between two positions.
    fn text_in_range(&self, (sr, sc): (usize, usize), (er, ec): (usize, usize)) -> String {
        if sr == er {
            let line = &self.lines[sr];
            let sb = byte_of_char(line, sc);
            let eb = byte_of_char(line, ec);
            return line[sb..eb].to_string();
        }
        let mut out = self.lines[sr][byte_of_char(&self.lines[sr], sc)..].to_string();
        for line in &self.lines[sr + 1..er] {
            out.push('\n');
            out.push_str(line);
        }
        out.push('\n');
        out.push_str(&self.lines[er][..byte_of_char(&self.lines[er], ec)]);
        out
    }

    /// The position one character before `pos`, or `None` at the start of the
    /// buffer.
    fn position_before(&self, (row, col): (usize, usize)) -> Option<(usize, usize)> {
        if col > 0 {
            Some((row, col - 1))
        } else if row > 0 {
            Some((row - 1, self.line_len(row - 1)))
        } else {
            None
        }
    }

    /// The position one character after `pos`, or `None` at the end of the
    /// buffer.
    fn position_after(&self, (row, col): (usize, usize)) -> Option<(usize, usize)> {
        if col < self.line_len(row) {
            Some((row, col + 1))
        } else if row + 1 < self.lines.len() {
            Some((row + 1, 0))
        } else {
            None
        }
    }

    /// Move the cursor, dropping any selection.
    pub fn move_cursor(&mut self, movement: CursorMove) -> bool {
        self.move_cursor_inner(movement, false)
    }

    /// Move the cursor, extending the selection from where it was.
    pub fn move_cursor_selecting(&mut self, movement: CursorMove) -> bool {
        self.move_cursor_inner(movement, true)
    }

    fn move_cursor_inner(&mut self, movement: CursorMove, selecting: bool) -> bool {
        if selecting {
            if self.selection_anchor.is_none() {
                self.selection_anchor = Some(self.cursor);
            }
        } else {
            self.selection_anchor = None;
        }
        let Some(target) = self.position_for(movement) else {
            return false;
        };
        let changed = target != self.cursor;
        self.cursor = target;
        changed
    }

    /// Resolve a movement to an absolute position, or `None` when it cannot be
    /// made (already at the edge of the buffer).
    fn position_for(&self, movement: CursorMove) -> Option<(usize, usize)> {
        let (row, col) = self.cursor;
        let page = self.viewport.get().height.max(1) as usize;
        Some(match movement {
            CursorMove::Forward => self.position_after((row, col))?,
            CursorMove::Back => self.position_before((row, col))?,
            CursorMove::Up => {
                let target = row.checked_sub(1)?;
                (target, col.min(self.line_len(target)))
            }
            CursorMove::Down => {
                let target = row + 1;
                if target >= self.lines.len() {
                    return None;
                }
                (target, col.min(self.line_len(target)))
            }
            CursorMove::Head => (row, 0),
            CursorMove::End => (row, self.line_len(row)),
            CursorMove::Top => (0, 0),
            CursorMove::Bottom => {
                let last = self.lines.len() - 1;
                (last, self.line_len(last))
            }
            CursorMove::WordForward => {
                let chars: Vec<char> = self.lines[row].chars().collect();
                if col >= chars.len() {
                    self.position_after((row, col))?
                } else {
                    (row, next_word_start(&chars, col))
                }
            }
            CursorMove::WordBack => {
                if col == 0 {
                    self.position_before((row, col))?
                } else {
                    let chars: Vec<char> = self.lines[row].chars().collect();
                    (row, prev_word_start(&chars, col))
                }
            }
            CursorMove::UpBy(n) => {
                let n = if n == 0 { page } else { n };
                let target = row.saturating_sub(n);
                (target, col.min(self.line_len(target)))
            }
            CursorMove::DownBy(n) => {
                let n = if n == 0 { page } else { n };
                let target = (row + n).min(self.lines.len() - 1);
                (target, col.min(self.line_len(target)))
            }
            CursorMove::Jump(r, c) => self.clamp_position((r, c)),
        })
    }
}

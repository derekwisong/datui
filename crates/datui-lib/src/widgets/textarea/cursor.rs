//! Cursor movement over a text buffer.
//!
//! Positions are `(row, col)` pairs where `row` indexes [`super::TextArea::lines`]
//! and `col` is a *character* index inside that line. Display width only matters
//! at render time, so nothing in this module deals in terminal columns.

/// A cursor movement request.
///
/// Passed to [`super::TextArea::move_cursor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorMove {
    /// One character to the right, wrapping to the next line.
    Forward,
    /// One character to the left, wrapping to the previous line.
    Back,
    /// One line up, keeping the column where possible.
    Up,
    /// One line down, keeping the column where possible.
    Down,
    /// Start of the current line.
    Head,
    /// End of the current line.
    End,
    /// Start of the first line.
    Top,
    /// End of the last line.
    Bottom,
    /// Start of the next word.
    WordForward,
    /// Start of the previous word.
    WordBack,
    /// Up by `n` lines, clamped at the first line.
    UpBy(usize),
    /// Down by `n` lines, clamped at the last line.
    DownBy(usize),
    /// An absolute position, clamped into the buffer.
    Jump(usize, usize),
}

/// Character classes used to decide where a word starts and ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CharClass {
    Space,
    Word,
    Punctuation,
}

fn class_of(c: char) -> CharClass {
    if c.is_whitespace() {
        CharClass::Space
    } else if c.is_alphanumeric() || c == '_' {
        CharClass::Word
    } else {
        CharClass::Punctuation
    }
}

/// Index of the start of the word after `col`, or the line length when the rest
/// of the line holds no further word.
pub(super) fn next_word_start(chars: &[char], col: usize) -> usize {
    let len = chars.len();
    let mut i = col.min(len);
    if i == len {
        return len;
    }
    // Step over the run the cursor currently sits in, then over any whitespace.
    let start_class = class_of(chars[i]);
    if start_class != CharClass::Space {
        while i < len && class_of(chars[i]) == start_class {
            i += 1;
        }
    }
    while i < len && class_of(chars[i]) == CharClass::Space {
        i += 1;
    }
    i
}

/// Index of the start of the word before `col`, or 0.
pub(super) fn prev_word_start(chars: &[char], col: usize) -> usize {
    let mut i = col.min(chars.len());
    while i > 0 && class_of(chars[i - 1]) == CharClass::Space {
        i -= 1;
    }
    if i == 0 {
        return 0;
    }
    let class = class_of(chars[i - 1]);
    while i > 0 && class_of(chars[i - 1]) == class {
        i -= 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(s: &str) -> Vec<char> {
        s.chars().collect()
    }

    #[test]
    fn next_word_start_skips_run_then_whitespace() {
        let c = chars("hello world");
        assert_eq!(next_word_start(&c, 0), 6);
        assert_eq!(next_word_start(&c, 3), 6);
        assert_eq!(next_word_start(&c, 6), 11);
        assert_eq!(next_word_start(&c, 11), 11);
    }

    #[test]
    fn next_word_start_treats_punctuation_as_its_own_run() {
        let c = chars("a == b");
        assert_eq!(next_word_start(&c, 0), 2);
        assert_eq!(next_word_start(&c, 2), 5);
    }

    #[test]
    fn prev_word_start_walks_back_over_whitespace() {
        let c = chars("hello world");
        assert_eq!(prev_word_start(&c, 11), 6);
        assert_eq!(prev_word_start(&c, 6), 0);
        assert_eq!(prev_word_start(&c, 0), 0);
        assert_eq!(prev_word_start(&c, 8), 6);
    }

    #[test]
    fn word_moves_handle_empty_lines() {
        let c: Vec<char> = Vec::new();
        assert_eq!(next_word_start(&c, 0), 0);
        assert_eq!(prev_word_start(&c, 0), 0);
    }

    #[test]
    fn word_moves_are_unicode_aware() {
        let c = chars("héllo wörld");
        assert_eq!(next_word_start(&c, 0), 6);
        assert_eq!(prev_word_start(&c, 11), 6);
    }
}

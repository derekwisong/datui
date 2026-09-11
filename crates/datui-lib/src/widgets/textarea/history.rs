//! Undo/redo stack for [`super::TextArea`].
//!
//! Edits are stored as deltas rather than buffer snapshots: every change is a
//! replacement of one contiguous range with one string, so its inverse is the
//! same operation with `removed` and `inserted` swapped.

/// One reversible change to the buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Edit {
    /// Where the replaced range begins, as `(row, char column)`.
    pub start: (usize, usize),
    /// Text that used to occupy the range. Empty for a pure insertion.
    pub removed: String,
    /// Text that replaced it. Empty for a pure deletion.
    pub inserted: String,
    /// Cursor position before the edit was applied.
    pub cursor_before: (usize, usize),
    /// Cursor position after the edit was applied.
    pub cursor_after: (usize, usize),
}

impl Edit {
    /// The same change, run backwards.
    pub fn inverted(&self) -> Edit {
        Edit {
            start: self.start,
            removed: self.inserted.clone(),
            inserted: self.removed.clone(),
            cursor_before: self.cursor_after,
            cursor_after: self.cursor_before,
        }
    }
}

/// Bounded undo and redo stacks.
#[derive(Debug, Clone)]
pub(super) struct History {
    undo: Vec<Edit>,
    redo: Vec<Edit>,
    max_entries: usize,
}

impl History {
    pub fn new(max_entries: usize) -> Self {
        Self {
            undo: Vec::new(),
            redo: Vec::new(),
            max_entries,
        }
    }

    /// Record a newly applied edit, invalidating anything that was redoable.
    pub fn push(&mut self, edit: Edit) {
        self.redo.clear();
        if self.max_entries == 0 {
            return;
        }
        if self.undo.len() == self.max_entries {
            self.undo.remove(0);
        }
        self.undo.push(edit);
    }

    /// Take the most recent edit so it can be undone.
    pub fn pop_undo(&mut self) -> Option<Edit> {
        self.undo.pop()
    }

    /// Take the most recently undone edit so it can be redone.
    pub fn pop_redo(&mut self) -> Option<Edit> {
        self.redo.pop()
    }

    pub fn push_redo(&mut self, edit: Edit) {
        self.redo.push(edit);
    }

    /// Re-record an undone edit without clearing the redo stack.
    pub fn push_undo(&mut self, edit: Edit) {
        self.undo.push(edit);
    }

    pub fn clear(&mut self) {
        self.undo.clear();
        self.redo.clear();
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(inserted: &str) -> Edit {
        Edit {
            start: (0, 0),
            removed: String::new(),
            inserted: inserted.to_string(),
            cursor_before: (0, 0),
            cursor_after: (0, inserted.chars().count()),
        }
    }

    #[test]
    fn inverted_swaps_removed_and_inserted() {
        let e = edit("abc");
        let inv = e.inverted();
        assert_eq!(inv.removed, "abc");
        assert_eq!(inv.inserted, "");
        assert_eq!(inv.cursor_before, (0, 3));
        assert_eq!(inv.cursor_after, (0, 0));
    }

    #[test]
    fn push_clears_redo() {
        let mut h = History::default();
        h.push(edit("a"));
        let undone = h.pop_undo().unwrap();
        h.push_redo(undone);
        h.push(edit("b"));
        assert!(h.pop_redo().is_none());
    }

    #[test]
    fn undo_stack_is_bounded() {
        let mut h = History::new(2);
        h.push(edit("a"));
        h.push(edit("b"));
        h.push(edit("c"));
        assert_eq!(h.pop_undo().unwrap().inserted, "c");
        assert_eq!(h.pop_undo().unwrap().inserted, "b");
        assert!(h.pop_undo().is_none());
    }

    #[test]
    fn zero_capacity_records_nothing() {
        let mut h = History::new(0);
        h.push(edit("a"));
        assert!(h.pop_undo().is_none());
    }
}

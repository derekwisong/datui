//! Row inspector state: which field is focused, the find text, the text mode,
//! how much of a long value is shown, and the fields read for this row that the
//! table's rows do not hold.
//!
//! The values themselves are not kept here. The inspector shows the table's
//! selected row from the buffer the table already holds, every frame, so moving
//! the row is moving the table's cursor and nothing is copied out of the buffer.

use crate::widgets::datatable::InspectField;
use crate::widgets::ui::PickerState;
use polars::prelude::DataFrame;

/// Bytes of a value shown per step: the first screenful of a huge string or
/// list costs this much formatting, and Enter shows as much again.
pub const CHUNK_BYTES: usize = 16 * 1024;

/// The fields of one row that the buffer does not hold, read on request.
#[derive(Debug, Clone)]
pub enum FieldRead {
    /// Asked for; the worker is reading.
    Reading { frame: u64, row: usize },
    /// One row, the fields read.
    Read {
        frame: u64,
        row: usize,
        values: DataFrame,
    },
    /// The read failed, or found a different row than the table shows.
    Failed {
        frame: u64,
        row: usize,
        message: String,
    },
}

impl FieldRead {
    /// The frame and row this read is for.
    pub fn key(&self) -> (u64, usize) {
        match self {
            Self::Reading { frame, row }
            | Self::Read { frame, row, .. }
            | Self::Failed { frame, row, .. } => (*frame, *row),
        }
    }
}

/// The text of the value pane, built for one field at one width and kept until
/// any of that changes, so a long value is not wrapped again every frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyKey {
    pub frame: u64,
    pub row: usize,
    pub field: String,
    pub escaped: bool,
    pub chunks: usize,
    pub width: u16,
    /// What was on hand for the field: a value, a null, or where its read stood.
    pub state: u8,
}

#[derive(Default)]
pub struct InspectorModal {
    pub active: bool,
    /// The fields in order, with the find text over their names.
    pub picker: PickerState,
    pub fields: Vec<InspectField>,
    /// The find field has the keys.
    pub finding: bool,
    /// Text shown as an escaped literal instead of as itself.
    pub escaped: bool,
    /// First line of the value pane on screen.
    pub scroll: usize,
    /// How many [`CHUNK_BYTES`] of the value are shown.
    pub chunks: usize,
    /// Lines the value pane showed last frame: a page for PgUp/PgDn.
    pub page: usize,
    /// The row the pane was last drawn for; a new one starts at its top.
    pub shown_row: Option<(u64, usize)>,
    pub read: Option<FieldRead>,
    pub body: Option<(BodyKey, crate::widgets::inspector::Body)>,
}

impl InspectorModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open over the table's selected row. The field and the text mode carry
    /// over from the last open while the field still exists.
    pub fn open(&mut self, fields: Vec<InspectField>) {
        let keep = self
            .focused()
            .map(|f| f.name.clone())
            .and_then(|name| fields.iter().position(|f| f.name == name));
        self.picker = PickerState::new(fields.iter().map(|f| f.name.clone()).collect());
        if let Some(index) = keep {
            self.picker.select_original(index);
        }
        self.fields = fields;
        self.active = true;
        self.finding = false;
        self.read = None;
        self.body = None;
        self.shown_row = None;
        self.reset_pane();
    }

    pub fn close(&mut self) {
        self.active = false;
        self.finding = false;
        self.read = None;
        self.body = None;
    }

    /// The focused field, or None when the find text admits nothing.
    pub fn focused(&self) -> Option<&InspectField> {
        self.fields.get(self.picker.selected_original()?)
    }

    /// The fields the find text admits, with their indices in `fields`.
    pub fn visible(&self) -> Vec<usize> {
        self.picker.filtered().into_iter().map(|(i, _)| i).collect()
    }

    fn reset_pane(&mut self) {
        self.scroll = 0;
        self.chunks = 1;
    }

    pub fn next_field(&mut self) {
        self.picker.move_down();
        self.reset_pane();
    }

    pub fn prev_field(&mut self) {
        self.picker.move_up();
        self.reset_pane();
    }

    pub fn first_field(&mut self) {
        if let Some(&first) = self.visible().first() {
            self.picker.select_original(first);
        }
        self.reset_pane();
    }

    pub fn last_field(&mut self) {
        if let Some(&last) = self.visible().last() {
            self.picker.select_original(last);
        }
        self.reset_pane();
    }

    /// A typed key while finding: narrows the fields.
    pub fn find_key(&mut self, c: char, mods: crossterm::event::KeyModifiers) {
        self.picker.filter_key(c, mods);
        self.reset_pane();
    }

    pub fn find_backspace(&mut self) {
        self.picker.backspace();
        self.reset_pane();
    }

    pub fn clear_find(&mut self) {
        self.picker.clear_filter();
        self.finding = false;
    }

    pub fn toggle_escaped(&mut self) {
        self.escaped = !self.escaped;
        self.scroll = 0;
    }

    pub fn scroll_by(&mut self, lines: isize) {
        self.scroll = self.scroll.saturating_add_signed(lines);
    }

    /// Show another chunk of a long value.
    pub fn more(&mut self) {
        self.chunks += 1;
    }

    /// The table moved to another row: its pane starts at the top, and what
    /// was read for the last row is let go.
    pub fn row_shown(&mut self, frame: u64, row: usize) {
        if self.shown_row != Some((frame, row)) {
            self.shown_row = Some((frame, row));
            self.reset_pane();
            if self.read.as_ref().is_some_and(|r| r.key() != (frame, row)) {
                self.read = None;
            }
        }
    }

    /// The fields read for `(frame, row)`, if they are on hand.
    pub fn read_values(&self, frame: u64, row: usize) -> Option<&DataFrame> {
        match &self.read {
            Some(FieldRead::Read {
                frame: f,
                row: r,
                values,
            }) if (*f, *r) == (frame, row) => Some(values),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;
    use polars::prelude::DataType;

    fn fields(names: &[&str]) -> Vec<InspectField> {
        names
            .iter()
            .map(|n| InspectField {
                name: n.to_string(),
                dtype: DataType::String,
                hidden: false,
            })
            .collect()
    }

    #[test]
    fn find_narrows_and_keeps_the_focused_field() {
        let mut m = InspectorModal::new();
        m.open(fields(&["id", "description", "amount", "status"]));
        m.next_field();
        assert_eq!(m.focused().unwrap().name, "description");
        m.find_key('a', KeyModifiers::NONE);
        m.find_key('m', KeyModifiers::NONE);
        assert_eq!(m.visible(), vec![2]);
        assert_eq!(m.focused().unwrap().name, "amount");
        m.clear_find();
        assert_eq!(m.visible().len(), 4);
        assert_eq!(m.focused().unwrap().name, "amount");
    }

    #[test]
    fn a_new_row_starts_at_the_top_and_drops_the_last_read() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a"]));
        m.row_shown(1, 5);
        m.scroll = 7;
        m.chunks = 3;
        m.read = Some(FieldRead::Reading { frame: 1, row: 5 });
        m.row_shown(1, 5);
        assert_eq!((m.scroll, m.chunks), (7, 3), "the same row keeps its place");
        m.row_shown(1, 6);
        assert_eq!((m.scroll, m.chunks), (0, 1));
        assert!(m.read.is_none());
    }

    #[test]
    fn reopening_keeps_the_field_while_it_exists() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a", "b", "c"]));
        m.last_field();
        m.close();
        m.open(fields(&["c", "a"]));
        assert_eq!(m.focused().unwrap().name, "c");
        m.close();
        m.open(fields(&["x", "y"]));
        assert_eq!(m.focused().unwrap().name, "x");
    }
}

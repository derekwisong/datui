//! Row inspector state: which field is focused and which are listed, where the
//! focus is (the list or the value), the find text, the value's view and where
//! it is read to, the row compared with, and the fields read for this row that
//! the table's rows do not hold.
//!
//! The values themselves are not kept here. The inspector shows the table's
//! selected row from the buffer the table already holds, every frame, so moving
//! the row is moving the table's cursor and nothing is copied out of the buffer.

use crate::inspector_drill::{Drill, JsonWait, Level, Node};
use crate::inspector_reader::{Reader, Wrap};
use crate::widgets::datatable::{InspectField, InspectRow};
use crate::widgets::inspector::Pane;
use polars::prelude::DataFrame;
use std::sync::Arc;

/// The most bytes of a value one key formats: a nested value or a JSON document
/// laid out in the pane stops here, and the reader wraps no more than this of a
/// long text for a key.
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

/// Where the keys go: the field list, or the focused value's pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Focus {
    #[default]
    List,
    Value,
}

/// The order the fields are listed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    /// The table's column order, hidden columns last.
    #[default]
    Table,
    /// By name.
    Name,
    /// Fields with a value first, then nulls and empties.
    Filled,
}

impl Order {
    pub fn next(self) -> Self {
        match self {
            Order::Table => Order::Name,
            Order::Name => Order::Filled,
            Order::Filled => Order::Table,
        }
    }

    /// As the list's rule says it; nothing for the table's order.
    pub fn label(self) -> Option<&'static str> {
        match self {
            Order::Table => None,
            Order::Name => Some("A-Z"),
            Order::Filled => Some("filled first"),
        }
    }
}

/// A way of showing a value. Only the views that apply to a value are offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Text that parses as JSON, indented.
    Json,
    /// Text as itself.
    Raw,
    /// Text or bytes as an escaped literal.
    Escaped,
    /// Bytes as a hex dump.
    Hex,
    /// Bytes as the text they hold: UTF-8, or decompressed gzip or zstd.
    Text,
}

impl View {
    pub fn label(self) -> &'static str {
        match self {
            View::Json => "JSON",
            View::Raw => "Raw",
            View::Escaped => "Escaped",
            View::Hex => "Hex",
            View::Text => "Text",
        }
    }
}

/// A search inside the focused value.
#[derive(Debug, Clone, Default)]
pub struct ValueFind {
    pub text: String,
    /// The find line has the keys.
    pub editing: bool,
    /// Where the text is, for the pane `pane`: bytes, or rows of a short value.
    pub hits: Vec<usize>,
    pub current: Option<usize>,
    pub pane: u64,
}

/// Long JSON text being indented on a worker, by frame, row and the text's place.
#[derive(Debug, Clone)]
pub enum Pretty {
    Pending {
        token: u64,
        place: (u64, usize, String),
    },
    Ready {
        place: (u64, usize, String),
        text: Arc<str>,
    },
    Failed {
        place: (u64, usize, String),
    },
}

impl Pretty {
    pub fn place(&self) -> &(u64, usize, String) {
        match self {
            Pretty::Pending { place, .. }
            | Pretty::Ready { place, .. }
            | Pretty::Failed { place } => place,
        }
    }
}

/// Bytes decompressed on a worker for their Text view, by frame, row and field.
#[derive(Debug, Clone)]
pub enum Unpack {
    Pending {
        token: u64,
        place: (u64, usize, String),
    },
    Ready {
        place: (u64, usize, String),
        text: Arc<crate::inspector_bytes::Decoded>,
    },
    Failed {
        place: (u64, usize, String),
    },
}

impl Unpack {
    pub fn place(&self) -> &(u64, usize, String) {
        match self {
            Unpack::Pending { place, .. }
            | Unpack::Ready { place, .. }
            | Unpack::Failed { place } => place,
        }
    }
}

/// What the value pane was built from: when any of it changes, the pane is
/// built again, and a long value is not laid out again every frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaneKey {
    pub frame: u64,
    pub row: usize,
    pub field: String,
    pub view: Option<View>,
    pub width: u16,
    /// What was on hand for the field: a value, a null, or where its read stood.
    pub state: u8,
    /// Where an indented copy of long JSON stood: none, asked, ready, failed.
    pub pretty: u8,
    /// Where text decompressed from bytes stood, the same way.
    pub unpacked: u8,
}

impl PaneKey {
    /// The same value in the same view, perhaps at another width: a resize keeps
    /// the pane's place in it.
    pub fn same_value(&self, other: &Self) -> bool {
        *self
            == Self {
                width: self.width,
                ..other.clone()
            }
    }
}

#[derive(Default)]
pub struct InspectorModal {
    pub active: bool,
    pub fields: Vec<InspectField>,
    /// The find text over the fields' names, then their values.
    pub filter: String,
    /// The find line has the keys.
    pub finding: bool,
    /// The focused field, an index into `fields`, while it is listed.
    selected: usize,
    /// The fields listed, in the order listed: the find text, the Filled toggle and
    /// the order applied. Kept by [`Self::set_visible`].
    pub visible: Vec<usize>,
    /// The first field listed when the list scrolls.
    pub list_offset: usize,
    /// Fields the list showed last frame: a page for PgUp/PgDn.
    pub list_page: usize,
    pub order: Order,
    /// Only fields with a value, or with Compare on, only those that differ.
    pub filled_only: bool,
    pub focus: Focus,
    /// The view chosen with `e`, for the field it was chosen on.
    pub view: Option<View>,
    view_field: Option<String>,
    pub wrap: Wrap,
    /// Where the value pane is in its value.
    pub reader: Reader,
    pub pane: Option<(PaneKey, Pane)>,
    pane_id: u64,
    pub value_find: Option<ValueFind>,
    /// Lines the value pane showed last frame: a page for PgUp/PgDn.
    pub page: usize,
    /// The row the pane was last drawn for; a new one starts at its top.
    pub shown_row: Option<(u64, usize)>,
    pub read: Option<FieldRead>,
    /// After Enter read a field, the rows moved to are read too while the focus
    /// stays on that field.
    pub follow: Option<String>,
    /// The list has a column for another row: the pinned one, or the next.
    pub compare: bool,
    /// The row `m` pinned for Compare.
    pub pinned: Option<InspectRow>,
    /// Compare shows the row before as well as the next: the last frame was
    /// wide enough for three.
    pub compare_both: bool,
    /// The levels opened under the focused field, when Enter drilled into it.
    pub drill: Option<Drill>,
    /// Text being parsed as JSON off this thread, to open as a level.
    pub json_wait: Option<JsonWait>,
    /// The last [`JsonWait::token`] handed out.
    pub json_token: u64,
    /// Text that looked like JSON and did not parse, by frame, row and path: Enter
    /// there shows it as text, not a second try that fails the same way.
    pub not_json: Option<(u64, usize, String)>,
    /// Long JSON text indented off this thread for the JSON view.
    pub pretty: Option<Pretty>,
    pub pretty_token: u64,
    /// Compressed bytes decompressed off this thread for the Text view.
    pub unpack: Option<Unpack>,
    pub unpack_token: u64,
}

impl InspectorModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open over the table's selected row, focused on `current`, the table's column
    /// cursor; without one, the last field focused while it still exists.
    pub fn open(&mut self, fields: Vec<InspectField>, current: Option<&str>) {
        let keep = current
            .map(str::to_string)
            .or_else(|| self.focused().map(|f| f.name.clone()))
            .and_then(|name| fields.iter().position(|f| f.name == name));
        self.selected = keep.unwrap_or(0);
        self.visible = (0..fields.len()).collect();
        self.fields = fields;
        self.active = true;
        self.finding = false;
        self.focus = Focus::List;
        self.list_offset = 0;
        self.read = None;
        self.follow = None;
        self.pane = None;
        self.shown_row = None;
        self.drill = None;
        self.json_wait = None;
        self.not_json = None;
        self.pretty = None;
        self.unpack = None;
        self.value_find = None;
        self.reader = Reader::default();
    }

    pub fn close(&mut self) {
        self.active = false;
        self.finding = false;
        self.focus = Focus::List;
        self.read = None;
        self.follow = None;
        self.pane = None;
        self.drill = None;
        self.json_wait = None;
        self.not_json = None;
        self.pretty = None;
        self.unpack = None;
        self.value_find = None;
    }

    /// A new id for a pane just built: the reader starts at its top.
    pub fn next_pane_id(&mut self) -> u64 {
        self.pane_id += 1;
        self.pane_id
    }

    /// Where text decompressed from the bytes at `place` stands.
    pub fn unpacked(&self, place: &(u64, usize, String)) -> crate::widgets::inspector::Unpacked {
        use crate::widgets::inspector::Unpacked;
        match &self.unpack {
            Some(Unpack::Pending { place: p, .. }) if p == place => Unpacked::Pending,
            Some(Unpack::Ready { place: p, text }) if p == place => Unpacked::Ready(text.clone()),
            Some(Unpack::Failed { place: p }) if p == place => Unpacked::Failed,
            _ => Unpacked::None,
        }
    }

    /// Whether the text at `path` of row `row` of frame `frame` was found not to be JSON.
    pub fn known_not_json(&self, frame: u64, row: usize, path: &str) -> bool {
        self.not_json
            .as_ref()
            .is_some_and(|(f, r, p)| (*f, *r) == (frame, row) && p == path)
    }

    /// The focused field: the one selected while it is listed, else the first
    /// listed. None when nothing is listed.
    pub fn focused(&self) -> Option<&InspectField> {
        self.focused_index().and_then(|i| self.fields.get(i))
    }

    fn focused_index(&self) -> Option<usize> {
        if self.visible.contains(&self.selected) {
            Some(self.selected)
        } else {
            self.visible.first().copied()
        }
    }

    /// Where the focused field is among those listed.
    pub fn focused_position(&self) -> usize {
        self.focused_index()
            .and_then(|i| self.visible.iter().position(|&v| v == i))
            .unwrap_or(0)
    }

    /// The fields listed. A focused field no longer listed gives the focus to the
    /// first that is.
    pub fn set_visible(&mut self, visible: Vec<usize>) {
        if !visible.contains(&self.selected)
            && let Some(&first) = visible.first()
        {
            self.selected = first;
        }
        self.visible = visible;
    }

    fn select_position(&mut self, at: usize) {
        if let Some(&i) = self.visible.get(at)
            && i != self.selected
        {
            self.selected = i;
            self.field_changed();
        }
    }

    /// The focus moved to another field: its value shows in its own view, and a
    /// read follows the rows only while the focus stays on its field.
    fn field_changed(&mut self) {
        let name = self.focused().map(|f| f.name.clone());
        if self.view_field != name {
            self.view = None;
        }
        if self.follow.is_some() && self.follow != name {
            self.follow = None;
        }
    }

    /// Move the focus `delta` items in the level drilled into.
    fn step(&mut self, delta: isize) -> bool {
        let Some(drill) = self.drill.as_mut() else {
            return false;
        };
        let level = drill.level_mut();
        let last = level.node.len().saturating_sub(1);
        level.selected = level.selected.saturating_add_signed(delta).min(last);
        true
    }

    pub fn next_field(&mut self) {
        if self.step(1) || self.visible.is_empty() {
            return;
        }
        let n = self.visible.len();
        self.select_position((self.focused_position() + 1) % n);
    }

    pub fn prev_field(&mut self) {
        if self.step(-1) || self.visible.is_empty() {
            return;
        }
        let n = self.visible.len();
        self.select_position((self.focused_position() + n - 1) % n);
    }

    pub fn first_field(&mut self) {
        if !self.step(isize::MIN) {
            self.select_position(0);
        }
    }

    pub fn last_field(&mut self) {
        if !self.step(isize::MAX) {
            self.select_position(self.visible.len().saturating_sub(1));
        }
    }

    /// A page of fields down (`1`) or up (`-1`): the list scrolls a page and the
    /// focus moves as far.
    pub fn page_fields(&mut self, direction: isize) {
        let page = self.list_page.max(1) as isize;
        if self.step(direction * page) {
            return;
        }
        let last = self.visible.len().saturating_sub(1);
        let at = self
            .focused_position()
            .saturating_add_signed(direction * page)
            .min(last);
        self.list_offset = self
            .list_offset
            .saturating_add_signed(direction * page)
            .min(last);
        self.select_position(at);
    }

    /// Choose the view `e` moves to.
    pub fn choose_view(&mut self, view: View) {
        self.view = Some(view);
        self.view_field = self.focused().map(|f| f.name.clone());
    }

    /// Open `node` as a level under the one shown, or under the row's field.
    pub fn drill_in(&mut self, frame: u64, row: usize, label: String, node: Node) {
        let level = Level {
            label,
            node,
            selected: 0,
        };
        match self.drill.as_mut() {
            Some(drill) if (drill.frame, drill.row) == (frame, row) => drill.levels.push(level),
            _ => {
                self.drill = Some(Drill {
                    frame,
                    row,
                    levels: vec![level],
                })
            }
        }
        self.json_wait = None;
        self.focus = Focus::List;
    }

    /// Step up one level; false at the row, where there is no level to leave.
    pub fn drill_out(&mut self) -> bool {
        let Some(drill) = self.drill.as_mut() else {
            return false;
        };
        drill.levels.pop();
        if drill.levels.is_empty() {
            self.drill = None;
        }
        self.json_wait = None;
        self.focus = Focus::List;
        true
    }

    /// A ticket for text about to be parsed as JSON off this thread.
    pub fn wait_for_json(&mut self, frame: u64, row: usize, label: String, path: String) -> u64 {
        self.json_token += 1;
        self.json_wait = Some(JsonWait {
            token: self.json_token,
            frame,
            row,
            label,
            path,
        });
        self.json_token
    }

    /// A typed key while finding: narrows the fields. Ctrl+W drops a word and
    /// Ctrl+U the whole text; any other chord types nothing.
    pub fn find_key(&mut self, c: char, mods: crossterm::event::KeyModifiers) {
        edit_find(&mut self.filter, c, mods);
    }

    pub fn find_backspace(&mut self) {
        self.filter.pop();
    }

    pub fn clear_find(&mut self) {
        self.filter.clear();
        self.finding = false;
    }

    /// The table moved to another row: what was read, opened or indented for the
    /// last row is let go.
    pub fn row_shown(&mut self, frame: u64, row: usize) {
        if self.shown_row != Some((frame, row)) {
            self.shown_row = Some((frame, row));
            // A level opened under another row is not this row's.
            if self
                .drill
                .as_ref()
                .is_some_and(|d| (d.frame, d.row) != (frame, row))
            {
                self.drill = None;
            }
            if self
                .json_wait
                .as_ref()
                .is_some_and(|w| (w.frame, w.row) != (frame, row))
            {
                self.json_wait = None;
            }
            if self.read.as_ref().is_some_and(|r| r.key() != (frame, row)) {
                self.read = None;
            }
            if self
                .pretty
                .as_ref()
                .is_some_and(|p| (p.place().0, p.place().1) != (frame, row))
            {
                self.pretty = None;
            }
            if self
                .unpack
                .as_ref()
                .is_some_and(|u| (u.place().0, u.place().1) != (frame, row))
            {
                self.unpack = None;
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

    /// The pane as last drawn, while it is for `field` of `(frame, row)`.
    pub fn pane_for(&self, frame: u64, row: usize, field: &str) -> Option<&Pane> {
        self.pane
            .as_ref()
            .filter(|(key, _)| (key.frame, key.row) == (frame, row) && key.field == field)
            .map(|(_, pane)| pane)
    }
}

/// A key typed into a find line: a character, Ctrl+W to drop a word, Ctrl+U to
/// clear. Other chords type nothing.
pub fn edit_find(text: &mut String, c: char, mods: crossterm::event::KeyModifiers) {
    use crossterm::event::KeyModifiers;
    let ctrl = mods.contains(KeyModifiers::CONTROL);
    if ctrl && c == 'w' {
        while text.ends_with(' ') {
            text.pop();
        }
        while text.chars().next_back().is_some_and(|c| c != ' ') {
            text.pop();
        }
    } else if ctrl && c == 'u' {
        text.clear();
    } else if !ctrl && !mods.contains(KeyModifiers::ALT) {
        text.push(c);
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
    fn the_focus_moves_among_the_fields_listed() {
        let mut m = InspectorModal::new();
        m.open(fields(&["id", "description", "amount", "status"]), None);
        m.next_field();
        assert_eq!(m.focused().unwrap().name, "description");
        m.set_visible(vec![2, 3]);
        assert_eq!(
            m.focused().unwrap().name,
            "amount",
            "unlisted: the first listed"
        );
        m.next_field();
        assert_eq!(m.focused().unwrap().name, "status");
        m.next_field();
        assert_eq!(m.focused().unwrap().name, "amount", "round to the top");
        m.set_visible(vec![0, 1, 2, 3]);
        assert_eq!(m.focused().unwrap().name, "amount");
        m.find_key('a', KeyModifiers::NONE);
        m.find_key('m', KeyModifiers::CONTROL);
        assert_eq!(m.filter, "a", "a chord types nothing");
        m.find_key('w', KeyModifiers::CONTROL);
        assert!(m.filter.is_empty());
    }

    #[test]
    fn a_page_moves_the_focus_and_the_list_alike() {
        let mut m = InspectorModal::new();
        let names: Vec<String> = (0..50).map(|i| format!("f{i}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        m.open(fields(&names), None);
        m.list_page = 10;
        m.page_fields(1);
        assert_eq!((m.focused_position(), m.list_offset), (10, 10));
        for _ in 0..4 {
            m.page_fields(1);
        }
        assert_eq!(m.focused_position(), 49, "stops at the last");
        m.page_fields(-1);
        assert_eq!(m.focused_position(), 39);
    }

    #[test]
    fn a_new_row_drops_the_last_read() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a"]), None);
        m.row_shown(1, 5);
        m.read = Some(FieldRead::Reading { frame: 1, row: 5 });
        m.row_shown(1, 5);
        assert!(m.read.is_some(), "the same row keeps it");
        m.row_shown(1, 6);
        assert!(m.read.is_none());
    }

    #[test]
    fn reopening_keeps_the_field_while_it_exists() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a", "b", "c"]), None);
        m.last_field();
        m.close();
        m.open(fields(&["c", "a"]), None);
        assert_eq!(m.focused().unwrap().name, "c");
        m.close();
        m.open(fields(&["x", "y"]), None);
        assert_eq!(m.focused().unwrap().name, "x");
    }

    #[test]
    fn opens_on_the_column_cursors_field() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a", "b", "c"]), Some("b"));
        assert_eq!(m.focused().unwrap().name, "b");
        m.last_field();
        m.close();
        // The cursor wins over the field focused last time.
        m.open(fields(&["a", "b", "c"]), Some("a"));
        assert_eq!(m.focused().unwrap().name, "a");
    }

    #[test]
    fn a_view_is_chosen_for_its_field() {
        let mut m = InspectorModal::new();
        m.open(fields(&["a", "b"]), None);
        m.choose_view(View::Escaped);
        assert_eq!(m.view, Some(View::Escaped));
        m.next_field();
        assert_eq!(m.view, None, "another field starts in its own view");
    }
}

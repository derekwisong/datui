//! The row inspector: every field of the table's selected row, and the focused
//! field's whole value. A takeover over the table: one Surface titled with the
//! row. Below 140 columns the fields are listed above the value; wider, the
//! fields sit on the left, in as many columns as fit, and the value on the right
//! at full height. Tab moves the focus between the list and the value, and the
//! rail moves with it.
//!
//! The value pane reads any length: only the rows on screen are wrapped (see
//! [`crate::inspector_reader`]), so the end of a 2 MiB value is a key away.

use crate::copy_modal::thousands;
use crate::exact;
use crate::inspector_bytes::{self, Decoded, Sniffed};
use crate::inspector_drill::{JSON_INLINE_BYTES, Node, Shape, json_text, looks_like_json};
use crate::inspector_modal::{
    CHUNK_BYTES, FieldRead, Focus, InspectorModal, Order, PaneKey, Pretty, View,
};
pub use crate::inspector_reader::Tone;
use crate::inspector_reader::{self as reader, Content, TextForm, Window};
use crate::render::context::RenderContext;
use crate::table::{DataTableState, InspectField, InspectRow, NullKind, dtype_label};
use crate::widgets::ui::{HintBar, SectionRule, Surface};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// The longest line the value pane wraps to: a reading surface keeps its
/// measure on a wide terminal.
const MEASURE: usize = 100;
/// Cells between a field's name, type and preview.
const GAP: usize = 2;
/// The fewest lines the value pane keeps beside the field list.
const VALUE_MIN: usize = 3;
/// The Surface's inner width from which the fields and the value sit side by
/// side: a 140-column terminal.
pub const WIDE: usize = 136;
/// The Surface's inner width from which a row with bytes gives the value pane a
/// 32-byte hex row: a 240-column terminal.
pub const WIDER: usize = 236;
/// Cells between the field list and the value side by side.
const PANE_GAP: usize = 3;
/// The narrowest the value pane gets beside a list that needs the room: still a
/// comfortable measure for prose.
const VALUE_FLOOR: usize = 56;
/// The rail and a hex dump row of 32 bytes: the value pane's width for bytes
/// where the list keeps room beside it.
const HEX_WIDE: usize = 1 + 8 + 1 + 32 * 3 + 2 + 32;
/// The narrowest preview a column of fields keeps.
const PREVIEW_MIN: usize = 14;
/// JSON text up to this long has a JSON view; longer text reads raw.
pub const PRETTY_MAX: usize = 1024 * 1024;
/// Bytes shown escaped: the start of a long binary value.
const ESCAPED_BYTES: usize = 64 * 1024;
/// Bytes of a value matched by the find text.
const MATCH_BYTES: usize = 4096;

/// What the inspector has for one field of the row.
#[derive(Debug, Clone)]
pub enum Shown<'a> {
    Value(AnyValue<'a>),
    Null(NullKind),
    /// Not among the rows read for the table: hidden, or binary.
    Unread,
    Reading,
    Failed(&'a str),
}

impl Shown<'_> {
    /// One number per kind, for the pane's cache key.
    fn kind(&self) -> u8 {
        match self {
            Self::Value(_) => 0,
            Self::Null(_) => 1,
            Self::Unread => 2,
            Self::Reading => 3,
            Self::Failed(_) => 4,
        }
    }
}

/// `field` of `row`: from the buffer, or from what was read for the row.
pub fn shown<'a>(
    field: &InspectField,
    row: &'a InspectRow,
    read: Option<&'a FieldRead>,
    state: &DataTableState,
) -> Shown<'a> {
    let value = if field.buffered() {
        row.values
            .column(&field.name)
            .ok()
            .and_then(|c| c.get(0).ok())
    } else {
        match read {
            Some(r) if r.key() != (row.frame, row.row) => return Shown::Unread,
            Some(FieldRead::Read { values, .. }) => {
                values.column(&field.name).ok().and_then(|c| c.get(0).ok())
            }
            Some(FieldRead::Reading { .. }) => return Shown::Reading,
            Some(FieldRead::Failed { message, .. }) => return Shown::Failed(message),
            None => return Shown::Unread,
        }
    };
    match value {
        None => Shown::Unread,
        Some(AnyValue::Null) => Shown::Null(state.null_kind(&field.name, row.drift_group)),
        Some(v) => Shown::Value(v),
    }
}

/// Whether a field holds something: a value, nothing (null or empty), or not
/// known until it is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    Value,
    Null,
    Empty,
    Unknown,
}

pub fn fill_of(shown: &Shown) -> Fill {
    match shown {
        Shown::Null(_) => Fill::Null,
        Shown::Value(v) if empty_preview(v).is_some() => Fill::Empty,
        Shown::Value(_) => Fill::Value,
        _ => Fill::Unknown,
    }
}

/// Whether two rows' values of a field differ; None when either is not read.
pub fn differs(a: &Shown, b: &Shown) -> Option<bool> {
    match (a, b) {
        (Shown::Value(x), Shown::Value(y)) => {
            if x == y {
                return Some(false);
            }
            // NaN is not equal to itself; their exact texts are.
            let nested = exact::is_nested_value(x) || exact::is_nested_value(y);
            Some(nested || exact::value_text(x) != exact::value_text(y))
        }
        (Shown::Null(_), Shown::Null(_)) => Some(false),
        (Shown::Null(_), Shown::Value(_)) | (Shown::Value(_), Shown::Null(_)) => Some(true),
        _ => None,
    }
}

/// A type as the pane names it, with the unit and zone the short label drops.
/// ASCII only: `us`, not `μs`.
pub fn type_text(dtype: &DataType) -> String {
    let unit = |u: &TimeUnit| match u {
        TimeUnit::Milliseconds => "ms",
        TimeUnit::Microseconds => "us",
        TimeUnit::Nanoseconds => "ns",
    };
    match dtype {
        DataType::Datetime(u, Some(tz)) => format!("datetime[{}, {tz}]", unit(u)),
        DataType::Datetime(u, None) => format!("datetime[{}]", unit(u)),
        DataType::Duration(u) => format!("duration[{}]", unit(u)),
        DataType::Decimal(p, s) => format!("decimal({p},{s})"),
        DataType::List(inner) => format!("list[{}]", type_text(inner)),
        DataType::Array(inner, n) => format!("array[{}; {n}]", type_text(inner)),
        other => dtype_label(other),
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{} {}", thousands(n), if n == 1 { one } else { many })
}

/// A byte count as people read it, and exactly: `1.0 MB (1,048,576 bytes)`.
pub fn size_text(n: usize) -> String {
    if n < 1024 {
        plural(n, "byte", "bytes")
    } else {
        format!(
            "{} ({} bytes)",
            crate::discover::format_size(n as u64),
            thousands(n)
        )
    }
}

/// What `y` copies from the pane.
#[derive(Debug, Clone)]
pub enum CopyAs {
    /// The stored value, exact: text as itself, numbers exact, lists as JSON.
    Stored,
    /// Bytes, as base64.
    Base64,
    /// The text the view shows: indented JSON, or text decoded from bytes.
    Text(Arc<str>),
    /// The escaped literal of the stored text.
    Escaped,
}

/// The value pane for the focused value: what its rule says, what it reads, the
/// views it has and the one shown, and what `y` copies.
#[derive(Debug, Clone)]
pub struct Pane {
    pub id: u64,
    pub facts: String,
    pub content: Content,
    /// The views that apply, the default first. Empty: `e` does nothing.
    pub views: Vec<View>,
    pub view: Option<View>,
    pub copy: CopyAs,
    /// Text longer than is indented on a key, shown raw until a worker indents it.
    pub indent: bool,
    /// Compressed bytes in their Text view, waiting on a worker to decompress them.
    pub unpack: bool,
}

impl Pane {
    fn lines(lines: Vec<(String, Tone)>, facts: Vec<String>) -> Self {
        Self {
            id: 0,
            facts: join_facts(&facts),
            content: Content::Lines(lines),
            views: Vec::new(),
            view: None,
            copy: CopyAs::Stored,
            indent: false,
            unpack: false,
        }
    }

    /// The view `e` moves to: the next of those that apply.
    pub fn next_view(&self) -> Option<View> {
        if self.views.len() < 2 {
            return None;
        }
        let at = self
            .view
            .and_then(|v| self.views.iter().position(|w| *w == v))
            .unwrap_or(0);
        Some(self.views[(at + 1) % self.views.len()])
    }
}

fn join_facts(facts: &[String]) -> String {
    facts.join(&format!(" {} ", crate::glyphs::get().middot))
}

/// Where an indented copy of long JSON text stands.
#[derive(Debug, Clone)]
pub enum Indented {
    /// Not asked for, or not wanted.
    None,
    Pending,
    Ready(Arc<str>),
    Failed,
}

impl Indented {
    fn code(&self) -> u8 {
        match self {
            Indented::None => 0,
            Indented::Pending => 1,
            Indented::Ready(_) => 2,
            Indented::Failed => 3,
        }
    }
}

/// The pane for text: as itself, escaped, or indented when it is JSON.
fn text_pane(
    s: &str,
    kind: String,
    choice: Option<View>,
    indented: &Indented,
    not_json: bool,
) -> Pane {
    let f = exact::text_facts(s);
    let mut facts = vec![kind];
    facts.push(if s.is_empty() {
        "empty".to_string()
    } else {
        plural(f.chars, "char", "chars")
    });
    if f.lines > 1 {
        facts.push(plural(f.lines, "line", "lines"));
    }
    if f.leading_spaces > 0 {
        facts.push(plural(f.leading_spaces, "leading space", "leading spaces"));
    }
    if f.trailing_spaces > 0 {
        facts.push(plural(
            f.trailing_spaces,
            "trailing space",
            "trailing spaces",
        ));
    }
    let json = !not_json && s.len() <= PRETTY_MAX && looks_like_json(s);
    let (json_ok, pretty) = if json && s.len() <= JSON_INLINE_BYTES {
        match serde_json::from_str::<JsonValue>(s) {
            Ok(v) => (
                true,
                Some(Arc::<str>::from(json_text(&v, true, usize::MAX).0)),
            ),
            Err(_) => (false, None),
        }
    } else if json {
        match indented {
            Indented::Ready(text) => (true, Some(text.clone())),
            Indented::Failed => (false, None),
            _ => (true, None),
        }
    } else {
        (false, None)
    };
    let mut views = Vec::new();
    if json_ok {
        views.push(View::Json);
    }
    views.extend([View::Raw, View::Escaped]);
    let view = choice.filter(|v| views.contains(v)).unwrap_or(views[0]);
    let mut pane = Pane {
        id: 0,
        facts: String::new(),
        content: Content::Lines(Vec::new()),
        views,
        view: Some(view),
        copy: CopyAs::Stored,
        indent: false,
        unpack: false,
    };
    match view {
        View::Json => match pretty {
            Some(text) => {
                facts.push("json".to_string());
                pane.content = Content::text(text.clone(), TextForm::Raw);
                pane.copy = CopyAs::Text(text);
            }
            None => {
                facts.push("json, indenting...".to_string());
                pane.content = Content::text(Arc::from(s), TextForm::Raw);
                pane.indent = true;
            }
        },
        View::Escaped => {
            facts.push("escaped".to_string());
            pane.content = Content::text(Arc::from(s), TextForm::Escaped);
            pane.copy = CopyAs::Escaped;
        }
        _ if s.is_empty() => {
            pane.content = Content::Lines(vec![("empty string".to_string(), Tone::Dim)]);
        }
        _ => pane.content = Content::text(Arc::from(s), TextForm::Raw),
    }
    pane.facts = join_facts(&facts);
    pane
}

/// Where text decompressed from gzip or zstd bytes stands, for the pane.
#[derive(Debug, Clone)]
pub enum Unpacked {
    /// No worker answers here (a level drilled into): compressed bytes have no
    /// Text view.
    Unavailable,
    /// Not asked for yet.
    None,
    Pending,
    Ready(Arc<Decoded>),
    Failed,
}

impl Unpacked {
    fn code(&self) -> u8 {
        match self {
            Unpacked::Unavailable => 0,
            Unpacked::None => 1,
            Unpacked::Pending => 2,
            Unpacked::Ready(_) => 3,
            Unpacked::Failed => 4,
        }
    }
}

/// The pane for bytes: a hex dump, the text they hold, or escaped. UTF-8 is its
/// own text; gzip and zstd are decompressed by a worker when their Text view is
/// asked for (`unpack`), never while the pane is built.
fn binary_pane(bytes: &[u8], choice: Option<View>, width: usize, unpacked: &Unpacked) -> Pane {
    let sniffed = inspector_bytes::sniff(bytes);
    let mut facts = vec!["binary".to_string(), size_text(bytes.len())];
    if bytes.is_empty() {
        facts.push("empty".to_string());
    }
    if let Some(kind) = sniffed.filter(|k| *k != Sniffed::Utf8) {
        facts.push(kind.label());
    }
    let compressed = matches!(sniffed, Some(Sniffed::Gzip | Sniffed::Zstd));
    let decoded = match unpacked {
        Unpacked::Ready(d) if compressed => Some(Decoded::clone(d)),
        _ if compressed => None,
        _ => inspector_bytes::decode_text(bytes, sniffed),
    };
    if compressed && matches!(unpacked, Unpacked::Failed) {
        facts.push("not text".to_string());
    }
    let unpacks = compressed && matches!(unpacked, Unpacked::None | Unpacked::Pending);
    let mut views = Vec::new();
    if decoded.is_some() && sniffed == Some(Sniffed::Utf8) {
        views.push(View::Text);
    }
    views.push(View::Hex);
    if (decoded.is_some() || unpacks) && sniffed != Some(Sniffed::Utf8) {
        views.push(View::Text);
    }
    views.push(View::Escaped);
    let view = choice.filter(|v| views.contains(v)).unwrap_or(views[0]);
    let mut pane = Pane {
        id: 0,
        facts: String::new(),
        content: Content::Lines(Vec::new()),
        views,
        view: Some(view),
        copy: CopyAs::Base64,
        indent: false,
        unpack: false,
    };
    match (view, decoded) {
        (View::Text, Some(d)) => {
            facts.push(if d.from == "UTF-8" {
                "UTF-8 text".to_string()
            } else {
                format!("{} text", d.from)
            });
            if d.cut {
                facts.push(format!("first {} KB", inspector_bytes::DECODE_MAX / 1024));
            }
            let text: Arc<str> = Arc::from(d.text);
            pane.content = Content::text(text.clone(), TextForm::Raw);
            pane.copy = CopyAs::Text(text);
        }
        (View::Text, None) => {
            facts.push("decompressing...".to_string());
            pane.content = Content::Lines(vec![("Decompressing...".to_string(), Tone::Dim)]);
            pane.unpack = true;
        }
        (View::Escaped, _) => {
            let head = &bytes[..bytes.len().min(ESCAPED_BYTES)];
            let mut literal = exact::escaped_bytes(head);
            if head.len() < bytes.len() {
                literal.pop();
                facts.push(format!("first {} KB", ESCAPED_BYTES / 1024));
            }
            facts.push("escaped".to_string());
            pane.content = Content::text(Arc::from(literal), TextForm::Raw);
        }
        _ if bytes.is_empty() => {
            pane.content = Content::Lines(vec![("empty binary".to_string(), Tone::Dim)]);
        }
        _ => {
            pane.content = Content::Hex {
                bytes: Arc::from(bytes),
                per_line: reader::hex_per_line(width),
            };
        }
    }
    pane.facts = join_facts(&facts);
    pane
}

/// Everything a pane is built from beside the value.
pub struct PaneAsk<'a> {
    pub choice: Option<View>,
    /// The pane's width: text wraps to the reading measure inside it, and a hex
    /// dump fills it.
    pub width: usize,
    /// The table's preview of a scalar, said beside the exact value when they differ.
    pub table: Option<&'a str>,
    pub indented: Indented,
    /// Text found not to be JSON: no JSON view.
    pub not_json: bool,
    /// Where text decompressed from the bytes stands.
    pub unpacked: Unpacked,
    /// The key that reads a field not read yet.
    pub read_key: &'a str,
}

/// The value pane for `shown`, a value of type `dtype`.
pub fn pane(dtype: &DataType, shown: &Shown, ask: &PaneAsk) -> Pane {
    let g = crate::glyphs::get();
    let width = ask.width.clamp(1, MEASURE);
    let kind = type_text(dtype);
    let mut lines = Vec::new();
    match shown {
        Shown::Unread => {
            reader::wrap_lines(
                &format!(
                    "Not read {} {} reads hidden and binary fields",
                    g.middot, ask.read_key
                ),
                width,
                Tone::Dim,
                &mut lines,
            );
            Pane::lines(lines, vec![kind, "not read".to_string()])
        }
        Shown::Reading => {
            reader::wrap_lines("Reading...", width, Tone::Dim, &mut lines);
            Pane::lines(lines, vec![kind])
        }
        Shown::Failed(message) => {
            reader::wrap_lines(message, width, Tone::Warn, &mut lines);
            Pane::lines(lines, vec![kind])
        }
        Shown::Null(null) => {
            let (glyph, word, why) = match null {
                NullKind::Null => (g.null, "null", None),
                NullKind::Absent => (g.absent, "absent", Some("not in this row's file")),
                NullKind::Conflict => (
                    g.conflict,
                    "conflicting",
                    Some("another type in this row's file, not read"),
                ),
            };
            let text = match why {
                Some(why) => format!("{glyph} {word}: {why}"),
                None => format!("{glyph} {word}"),
            };
            reader::wrap_lines(&text, width, Tone::Dim, &mut lines);
            Pane::lines(lines, vec![kind, word.to_string()])
        }
        Shown::Value(value) => match value {
            AnyValue::String(s) => text_pane(s, kind, ask.choice, &ask.indented, ask.not_json),
            AnyValue::StringOwned(s) => {
                text_pane(s.as_str(), kind, ask.choice, &ask.indented, ask.not_json)
            }
            AnyValue::Categorical(..)
            | AnyValue::CategoricalOwned(..)
            | AnyValue::Enum(..)
            | AnyValue::EnumOwned(..) => {
                let s = exact::value_text(value);
                text_pane(&s, kind, ask.choice, &Indented::None, true)
            }
            AnyValue::Binary(b) => binary_pane(b, ask.choice, ask.width, &ask.unpacked),
            AnyValue::BinaryOwned(b) => binary_pane(b, ask.choice, ask.width, &ask.unpacked),
            v if exact::is_nested_value(v) => {
                let mut facts = vec![kind];
                if let Some(n) = exact::nested_len(v) {
                    facts.push(match v {
                        AnyValue::Struct(..) | AnyValue::StructOwned(_) => {
                            plural(n, "field", "fields")
                        }
                        _ => plural(n, "item", "items"),
                    });
                }
                let pretty = exact::nested_pretty(v, CHUNK_BYTES);
                if pretty.cut {
                    facts.push(format!("first {} KB", CHUNK_BYTES / 1024));
                }
                Pane {
                    id: 0,
                    facts: join_facts(&facts),
                    content: Content::text(Arc::from(pretty.text), TextForm::Raw),
                    views: Vec::new(),
                    view: None,
                    copy: CopyAs::Stored,
                    indent: false,
                    unpack: false,
                }
            }
            v => {
                let text = exact::value_text(v);
                reader::wrap_lines(&text, width, Tone::Plain, &mut lines);
                if let Some(table) = ask.table.filter(|t| *t != text) {
                    lines.push((String::new(), Tone::Plain));
                    reader::wrap_lines(
                        &format!("In the table: {table}"),
                        width,
                        Tone::Dim,
                        &mut lines,
                    );
                }
                Pane::lines(lines, vec![kind])
            }
        },
    }
}

/// A preview for a value the table would show as nothing: an empty string or
/// empty bytes, said so rather than left blank.
fn empty_preview(value: &AnyValue) -> Option<String> {
    let g = crate::glyphs::get();
    match value {
        AnyValue::String("") => Some("\"\"".to_string()),
        AnyValue::StringOwned(s) if s.is_empty() => Some("\"\"".to_string()),
        AnyValue::Binary([]) => Some(format!("0 bytes {} empty", g.middot)),
        AnyValue::BinaryOwned(b) if b.is_empty() => Some(format!("0 bytes {} empty", g.middot)),
        _ => None,
    }
}

/// The table's one-line preview of a value, formatted as the table formats it,
/// and only as much of it as `room` cells can show. A long text or bytes say
/// their size after a cut preview, so a huge value shows before it is focused.
fn preview(field: &InspectField, value: &AnyValue, room: usize, ctx: &RenderContext) -> String {
    let g = crate::glyphs::get();
    let budget = room.saturating_mul(4).max(16);
    let sized = |text: String, len: usize| {
        if len > 1024 && crate::glyphs::cell_width(&text) > room {
            let size = format!(" {} {}", g.middot, crate::discover::format_size(len as u64));
            let keep = room.saturating_sub(crate::glyphs::cell_width(&size));
            let cut = crate::glyphs::fit_cells(&text, keep, g.ellipsis).into_owned();
            format!("{cut}{size}")
        } else {
            text
        }
    };
    match value {
        AnyValue::String(s) => sized(
            exact::preview(exact::prefix(s, budget), g).into_owned(),
            s.len(),
        ),
        AnyValue::StringOwned(s) => sized(
            exact::preview(exact::prefix(s, budget), g).into_owned(),
            s.len(),
        ),
        AnyValue::Binary(b) => binary_preview(b),
        AnyValue::BinaryOwned(b) => binary_preview(b),
        v if exact::is_nested_value(v) => {
            exact::preview(&exact::nested_compact(v, budget).text, g).into_owned()
        }
        v => {
            let formatter = ctx.number_format.formatter_for(&field.name, &field.dtype);
            let mut scratch = String::new();
            let text = crate::numfmt::format_any_value(&formatter, v, &mut scratch);
            exact::preview(&text, g).into_owned()
        }
    }
}

/// Bytes in the list: their size, and what they are when that is known.
fn binary_preview(b: &[u8]) -> String {
    let size = if b.len() < 1024 {
        plural(b.len(), "byte", "bytes")
    } else {
        crate::discover::format_size(b.len() as u64)
    };
    match inspector_bytes::sniff(&b[..b.len().min(4096)]) {
        // A prefix that is UTF-8 says little about the rest.
        Some(Sniffed::Utf8) | None => size,
        Some(kind) => format!("{size} {} {}", crate::glyphs::get().middot, kind.label()),
    }
}

/// The text of a value the find text is matched against: its start, lowercased.
fn match_text(shown: &Shown) -> Option<String> {
    let Shown::Value(v) = shown else {
        return None;
    };
    Some(
        match v {
            AnyValue::String(s) => exact::prefix(s, MATCH_BYTES).to_string(),
            AnyValue::StringOwned(s) => exact::prefix(s, MATCH_BYTES).to_string(),
            AnyValue::Binary(_) | AnyValue::BinaryOwned(_) => return None,
            v if exact::is_nested_value(v) => exact::nested_compact(v, MATCH_BYTES).text,
            v => exact::value_text(v),
        }
        .to_lowercase(),
    )
}

/// A value the pane shows as one exact line: not text, bytes or nested.
fn is_scalar(value: &AnyValue) -> bool {
    !matches!(
        value,
        AnyValue::String(_)
            | AnyValue::StringOwned(_)
            | AnyValue::Binary(_)
            | AnyValue::BinaryOwned(_)
    ) && !exact::is_nested_value(value)
}

fn null_glyph(kind: NullKind) -> &'static str {
    let g = crate::glyphs::get();
    match kind {
        NullKind::Null => g.null,
        NullKind::Absent => g.absent,
        NullKind::Conflict => g.conflict,
    }
}

/// The rows Compare puts beside a row: the pinned one, or the next; and from
/// [`WIDER`], unpinned, the row before it too, the three in row order.
#[derive(Clone)]
pub struct Compared {
    /// The row before, in a three-row compare. None at the first row, whose
    /// column stays, empty, so nothing moves.
    pub before: Option<InspectRow>,
    pub after: Option<InspectRow>,
    /// Three columns: before, this, after.
    pub both: bool,
    pub pinned: bool,
}

impl Compared {
    /// The rows compared with, in row order.
    pub fn rows(&self) -> impl Iterator<Item = &InspectRow> {
        self.before.iter().chain(self.after.iter())
    }
}

/// What Compare puts beside `row`, while it is on.
pub fn compared(
    modal: &InspectorModal,
    state: &DataTableState,
    row: &InspectRow,
) -> Option<Compared> {
    if !modal.compare {
        return None;
    }
    if let Some(pinned) = &modal.pinned
        && (pinned.frame, pinned.row) != (row.frame, row.row)
    {
        return Some(Compared {
            before: None,
            after: Some(pinned.clone()),
            both: false,
            pinned: true,
        });
    }
    let after = state.inspect_row_at(row.row + 1);
    let before = modal
        .compare_both
        .then(|| row.row.checked_sub(1))
        .flatten()
        .and_then(|r| state.inspect_row_at(r));
    (after.is_some() || before.is_some()).then_some(Compared {
        before,
        after,
        both: modal.compare_both,
        pinned: false,
    })
}

/// Whether `field`'s value `this` differs from any compared row's; None when
/// no compared row's value is read.
fn differs_from(
    this: &Shown,
    field: &InspectField,
    other: &Compared,
    state: &DataTableState,
) -> Option<bool> {
    let mut known = None;
    for row in other.rows() {
        match differs(this, &shown(field, row, None, state)) {
            Some(true) => return Some(true),
            Some(false) => known = Some(false),
            None => {}
        }
    }
    known
}

/// The fields listed, in the order listed: the order chosen, then the nulls toggle (or,
/// comparing, only the fields that differ), then the find text — names first,
/// then values.
pub fn visible_fields(modal: &InspectorModal, state: &DataTableState) -> Vec<usize> {
    let fields = &modal.fields;
    let row = state.inspect_row();
    let mut order: Vec<usize> = (0..fields.len()).collect();
    if modal.order == Order::Name {
        order.sort_by_cached_key(|&i| fields[i].name.to_lowercase());
    }
    let Some(row) = row else {
        return order;
    };
    let read = modal.read.as_ref();
    let shown_at = |i: usize| shown(&fields[i], &row, read, state);
    if modal.order == Order::Filled {
        order.sort_by_key(|&i| fill_of(&shown_at(i)) != Fill::Value);
    }
    if modal.filled_only {
        match compared(modal, state, &row) {
            Some(other) => order
                .retain(|&i| differs_from(&shown_at(i), &fields[i], &other, state) == Some(true)),
            None => order.retain(|&i| matches!(fill_of(&shown_at(i)), Fill::Value | Fill::Unknown)),
        }
    }
    if !modal.filter.is_empty() {
        let needle = modal.filter.to_lowercase();
        let (names, rest): (Vec<usize>, Vec<usize>) = order
            .into_iter()
            .partition(|&i| fields[i].name.to_lowercase().contains(&needle));
        let values = rest
            .into_iter()
            .filter(|&i| match_text(&shown_at(i)).is_some_and(|t| t.contains(&needle)));
        order = names.into_iter().chain(values).collect();
    }
    order
}

/// Counts for the list's rule: nulls and empties, and with Compare, how many
/// fields differ.
struct Counts {
    nulls: usize,
    empties: usize,
    differ: Option<usize>,
}

fn counts(
    modal: &InspectorModal,
    state: &DataTableState,
    row: &InspectRow,
    other: Option<&Compared>,
) -> Counts {
    let mut c = Counts {
        nulls: 0,
        empties: 0,
        differ: other.map(|_| 0),
    };
    for field in &modal.fields {
        let this = shown(field, row, modal.read.as_ref(), state);
        match fill_of(&this) {
            Fill::Null => c.nulls += 1,
            Fill::Empty => c.empties += 1,
            _ => {}
        }
        if let (Some(other), Some(n)) = (other, c.differ.as_mut())
            && differs_from(&this, field, other, state) == Some(true)
        {
            *n += 1;
        }
    }
    c
}

/// The inspector's title: the row, of how many, the group it is in inside a
/// drill-down (the breadcrumb the takeover covers), and the row compared with.
fn title(display_row: usize, state: &DataTableState, other: Option<&Compared>) -> String {
    let g = crate::glyphs::get();
    let mut title = format!("Row {}", thousands(display_row));
    if let Some(total) = state.num_rows_if_valid() {
        title.push_str(&format!(" of {}", thousands(total)));
    }
    if state.is_drilled_down()
        && let Some((columns, values)) = state.drilled_group_key()
    {
        let key: Vec<String> = columns
            .iter()
            .zip(values)
            .map(|(c, v)| format!("{c}={v}"))
            .collect();
        if !key.is_empty() {
            title.push_str(&format!(
                " {} {}",
                g.middot,
                key.join(&format!(" {} ", g.middot))
            ));
        }
    }
    if let Some(other) = other {
        let pinned = if other.pinned { "pinned " } else { "" };
        let rows: Vec<String> = other.rows().map(|r| thousands(r.display_row)).collect();
        title.push_str(&format!(
            " {} compare with {pinned}{}",
            g.middot,
            rows.join(" and ")
        ));
    }
    title
}

/// Where the list starts, given `cap` slots for `n` items with the focus on
/// `sel` and the list last starting at `offset`; and whether the first slot
/// counts the items above and the last the items below.
pub fn list_window(n: usize, sel: usize, offset: usize, cap: usize) -> (usize, bool, bool) {
    if n <= cap {
        return (0, false, false);
    }
    if cap < 3 {
        let o = sel.min(n.saturating_sub(cap));
        return (o, false, false);
    }
    let fits = |o: usize| {
        let above = o > 0;
        let room = cap - usize::from(above);
        let below = o + room < n;
        (above, below, room - usize::from(below))
    };
    // No room left empty past the end: the last page is full.
    let mut o = offset.min(n + 1 - cap);
    for _ in 0..4 {
        let (above, below, items) = fits(o);
        if sel < o {
            o = sel;
        } else if sel >= o + items {
            o += sel + 1 - (o + items);
        } else {
            return (o, above, below);
        }
    }
    let (above, below, _) = fits(o);
    (o, above, below)
}

/// What the side-by-side layout sizes the field list from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListShape {
    /// Every field of the row, listed or not: narrowing the list moves nothing.
    pub fields: usize,
    /// A column of fields at its narrowest.
    pub min_col: usize,
    /// One column: Compare puts other rows beside each field.
    pub single: bool,
    /// The row has bytes: the value pane widens for a 32-byte hex row where the
    /// list keeps room beside it.
    pub bytes: bool,
}

/// The value pane's width side by side with the list in `width` cells and
/// `rows` rows. It depends on the terminal and the row's fields, never on the
/// focused one, so nothing moves as the focus does. A list longer than the
/// screen takes the columns it needs, down to [`VALUE_FLOOR`] for the value;
/// otherwise the value has its measure, or from [`WIDER`] a 32-byte hex row
/// for a row with bytes.
fn value_pane_width(width: usize, rows: usize, list: ListShape) -> usize {
    let mut max = (MEASURE + 1).min(width * 45 / 100);
    if list.bytes && !list.single && width >= WIDER {
        max = HEX_WIDE;
    }
    if list.single {
        return max;
    }
    let floor = VALUE_FLOOR.min(max);
    let need = list.fields.div_ceil(rows.max(1)).max(1);
    let fit = ((width.saturating_sub(floor + PANE_GAP) + GAP) / (list.min_col + GAP)).max(1);
    let cols = need.min(fit);
    let list_w = cols * list.min_col + (cols - 1) * GAP;
    width.saturating_sub(list_w + PANE_GAP).clamp(floor, max)
}

/// Where each part of the inspector goes inside the Surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub wide: bool,
    pub list_rule: Rect,
    pub list: Rect,
    /// Columns of fields across the list.
    pub cols: usize,
    pub value_rule: Rect,
    /// The value's rows, with a column for the rail at its left.
    pub value: Rect,
}

/// Lay out `content` for `fields` listed and a value that needs `value_need`
/// rows. Below [`WIDE`] the list sits above the value and takes the rows it
/// needs, leaving the value what its lines need. The split follows what the
/// panes hold, never where the focus is, so Tab moves nothing. From [`WIDE`] the two
/// sit side by side, each at full height, the value as wide as
/// [`value_pane_width`] says, and fields flow into as many columns of
/// `list.min_col` cells as fit.
pub fn layout(content: Rect, fields: usize, value_need: usize, list: ListShape) -> Layout {
    let line = |y: u16, x: u16, width: u16| Rect {
        x,
        y,
        width,
        height: 1,
    };
    let width = content.width as usize;
    let h = content.height as usize;
    if width >= WIDE {
        let rows = h.saturating_sub(1).max(1);
        let value_w = value_pane_width(width, rows, list);
        let list_w = width - value_w - PANE_GAP;
        let mut cols = ((list_w + GAP) / (list.min_col + GAP)).max(1);
        if list.single {
            cols = 1;
        }
        while cols > 1 && (cols - 1) * rows >= fields {
            cols -= 1;
        }
        let value_x = content.x + (list_w + PANE_GAP) as u16;
        return Layout {
            wide: true,
            list_rule: line(content.y, content.x, list_w as u16),
            list: Rect {
                x: content.x,
                y: content.y + 1,
                width: list_w as u16,
                height: rows as u16,
            },
            cols,
            value_rule: line(content.y, value_x, value_w as u16),
            value: Rect {
                x: value_x,
                y: content.y + 1,
                width: value_w as u16,
                height: rows as u16,
            },
        };
    }
    let avail = h.saturating_sub(2);
    let fields = fields.max(1);
    let list_h = if fields + value_need.max(VALUE_MIN) <= avail {
        fields
    } else {
        // A long value takes up to half, and the rest goes to the list, but only
        // the rows its fields fill: the value has the remainder.
        let value_h = value_need.clamp(VALUE_MIN, (avail / 2).max(VALUE_MIN));
        avail.saturating_sub(value_h).clamp(1, fields)
    }
    .min(avail.saturating_sub(1))
    .max(1);
    let value_y = content.y + 1 + list_h as u16;
    Layout {
        wide: false,
        list_rule: line(content.y, content.x, content.width),
        list: Rect {
            x: content.x,
            y: content.y + 1,
            width: content.width,
            height: list_h as u16,
        },
        cols: 1,
        value_rule: line(value_y, content.x, content.width),
        value: Rect {
            x: content.x,
            y: value_y + 1,
            width: content.width,
            height: (content.y + content.height).saturating_sub(value_y + 1),
        },
    }
}

/// The pane's width inside `value`, past the rail column: a hex dump's.
fn pane_width(value: Rect) -> usize {
    (value.width as usize).saturating_sub(1).max(1)
}

/// The pane's wrapping width inside `value`: capped at the reading measure.
fn value_width(value: Rect) -> usize {
    pane_width(value).min(MEASURE)
}

/// Build, or take from the cache, the pane for the focused field of `row`.
fn field_pane(
    modal: &mut InspectorModal,
    state: &DataTableState,
    row: &InspectRow,
    field: &InspectField,
    width: usize,
    ctx: &RenderContext,
) -> Pane {
    let value = shown(field, row, modal.read.as_ref(), state);
    let place = (row.frame, row.row, field.name.clone());
    let indented = match &modal.pretty {
        Some(Pretty::Pending { place: p, .. }) if *p == place => Indented::Pending,
        Some(Pretty::Ready { place: p, text }) if *p == place => Indented::Ready(text.clone()),
        Some(Pretty::Failed { place: p }) if *p == place => Indented::Failed,
        _ => Indented::None,
    };
    let unpacked = modal.unpacked(&place);
    let key = PaneKey {
        frame: row.frame,
        row: row.row,
        field: field.name.clone(),
        view: modal.view,
        width: width as u16,
        state: value.kind(),
        pretty: indented.code(),
        unpacked: unpacked.code(),
    };
    if let Some((cached, pane)) = &modal.pane
        && *cached == key
    {
        return pane.clone();
    }
    // Text, bytes and nested values are shown whole; only a scalar's preview can
    // say something the exact text does not.
    let table = match &value {
        Shown::Value(v) if is_scalar(v) => Some(preview(field, v, 64, ctx)),
        _ => None,
    };
    let read_key = if state.can_drill_down() { "r" } else { "Enter" };
    let mut built = pane(
        &field.dtype,
        &value,
        &PaneAsk {
            choice: modal.view,
            width,
            table: table.as_deref(),
            indented,
            not_json: modal.known_not_json(row.frame, row.row, &field.name),
            unpacked,
            read_key,
        },
    );
    built.id = renewed(modal, &key, &built);
    modal.pane = Some((key, built.clone()));
    built
}

/// The id for a pane just built for `key`: the last one's while it shows the same
/// value at another width, so the reader stays where it was; else a new one,
/// which the reader starts at the top of.
fn renewed(modal: &mut InspectorModal, key: &PaneKey, built: &Pane) -> u64 {
    match &modal.pane {
        Some((cached, old)) if cached.same_value(key) => {
            let id = old.id;
            let old = old.content.clone();
            modal.reader.carry(&old, &built.content);
            id
        }
        _ => modal.next_pane_id(),
    }
}

/// What Enter does on the focused field, for the footer.
fn enter_label(
    modal: &InspectorModal,
    state: &DataTableState,
    row: &InspectRow,
    field: &InspectField,
) -> Option<&'static str> {
    // On a group's row, Enter drills into its rows, as at the table.
    if state.can_drill_down() {
        return Some("Rows");
    }
    match shown(field, row, modal.read.as_ref(), state) {
        Shown::Unread => Some("Read"),
        Shown::Failed(_) => Some("Retry"),
        Shown::Value(v)
            if value_opens(&v) && !modal.known_not_json(row.frame, row.row, &field.name) =>
        {
            Some("Open")
        }
        _ => None,
    }
}

/// Draw the inspector over `area` for the table's selected row.
pub fn render(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut InspectorModal,
    state: &DataTableState,
    codebook: Option<&crate::codebook::Codebook>,
    ctx: &RenderContext,
) {
    let row = state.inspect_row();
    if let Some(row) = &row {
        modal.row_shown(row.frame, row.row);
    }
    if let (Some(row), Some(_)) = (&row, &modal.drill) {
        let title = title(row.display_row, state, None);
        render_drill(area, buf, modal, &title, ctx);
        return;
    }
    let content = Surface::content_area(area);
    modal.compare_both = content.width as usize >= WIDER;
    let visible = visible_fields(modal, state);
    modal.set_visible(visible);
    let other = row.as_ref().and_then(|r| compared(modal, state, r));

    let focused = modal.focused().cloned();

    // The list's columns, measured over every field so moving moves nothing.
    let g = crate::glyphs::get();
    let name_w = modal
        .fields
        .iter()
        .map(|f| {
            crate::glyphs::cell_width(&f.name)
                + if f.hidden {
                    1 + crate::glyphs::cell_width(g.hidden_mark)
                } else {
                    0
                }
        })
        .max()
        .unwrap_or(0)
        .clamp(4, 28);
    let type_w = modal
        .fields
        .iter()
        .map(|f| crate::glyphs::cell_width(&dtype_label(&f.dtype)))
        .max()
        .unwrap_or(0)
        .min(14);
    let shape = ListShape {
        fields: modal.fields.len(),
        min_col: 1 + name_w + GAP + type_w + GAP + PREVIEW_MIN,
        single: other.is_some(),
        bytes: modal
            .fields
            .iter()
            .any(|f| matches!(f.dtype, DataType::Binary | DataType::BinaryOffset)),
    };
    // The value's width does not depend on what it needs; only the stacked
    // layout's heights do.
    let probe = layout(content, modal.visible.len(), VALUE_MIN, shape);
    let width = value_width(probe.value);
    let pane = match (&row, &focused) {
        (Some(row), Some(field)) => Some(field_pane(
            modal,
            state,
            row,
            field,
            pane_width(probe.value),
            ctx,
        )),
        _ => None,
    };
    let pane = pane.unwrap_or_else(|| {
        let message = if row.is_none() {
            "Reading the row..."
        } else {
            "No field matches"
        };
        Pane::lines(vec![(message.to_string(), Tone::Dim)], Vec::new())
    });
    modal.reader.prepare(pane.id, width, modal.wrap);
    // What the codebook says of the field, and of the value under the cursor.
    let note = match (&row, &focused, codebook) {
        (Some(row), Some(field), Some(book)) => book
            .column(&field.name)
            .map(|column| {
                codebook_lines(
                    column,
                    &shown(field, row, modal.read.as_ref(), state),
                    width,
                )
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    let value_need = modal
        .reader
        .rows_needed(&pane.content, content.height as usize);
    let need = value_need + if note.is_empty() { 0 } else { note.len() + 1 };

    let mut lay = layout(content, modal.visible.len(), need, shape);
    // The note follows the value, a blank row between, and never takes more than half
    // the pane: a long value scrolls above it.
    let note_rows = if note.is_empty() || lay.value.height < 3 {
        0
    } else {
        (note.len() + 1).min(lay.value.height as usize / 2) as u16
    };
    lay.value.height -= note_rows;
    let note_area = Rect {
        y: lay.value.y + lay.value.height.min(value_need as u16),
        height: note_rows,
        ..lay.value
    };

    // The footer, from what the layout leaves visible.
    let enter = match (&row, &focused) {
        (Some(row), Some(field)) => enter_label(modal, state, row, field),
        _ => None,
    };
    let unread_on_group = state.can_drill_down()
        && matches!(
            (&row, &focused),
            (Some(row), Some(field))
                if matches!(shown(field, row, modal.read.as_ref(), state), Shown::Unread | Shown::Failed(_))
        );
    let value_rows = lay.value.height as usize;
    let overflows = need > value_rows || modal.reader.window(&pane.content, value_rows).above;
    let footer = footer(
        modal,
        &pane,
        &FooterFacts {
            enter,
            read_key: unread_on_group,
            has_value: row.is_some() && focused.is_some(),
            many_fields: modal.visible.len() > 1,
            list_overflows: modal.visible.len() > lay.list.height as usize * lay.cols,
            value_overflows: overflows,
            comparing: other.is_some(),
        },
        ctx,
    );
    let title = match &row {
        Some(row) => title(row.display_row, state, other.as_ref()),
        None => "Row".to_string(),
    };
    let title = crate::glyphs::fit_cells(&title, area.width.saturating_sub(4) as usize, g.ellipsis);
    let content = Surface::new(&title).footer(&footer).render(area, buf, ctx);
    if content.height < 4 || content.width < 12 {
        return;
    }

    // The list's rule: the find line while finding, else the counts.
    let mut rule_used = None;
    if modal.finding || !modal.filter.is_empty() {
        draw_find_line(
            buf,
            lay.list_rule,
            &modal.filter,
            modal.finding,
            &format!(
                "{} of {}",
                thousands(modal.visible.len()),
                thousands(modal.fields.len())
            ),
            ctx,
        );
    } else {
        let chip = match &row {
            Some(row) => list_chip(modal, state, row, other.as_ref()),
            None => thousands(modal.fields.len()),
        };
        SectionRule {
            title: "Fields",
            chip: Some(&chip),
        }
        .render(lay.list_rule, buf, ctx);
        rule_used = Some("Fields".len() + 1 + crate::glyphs::cell_width(&chip) + 3);
    }

    draw_fields(
        buf,
        &lay,
        modal,
        state,
        ListOf {
            row: row.as_ref(),
            other: other.as_ref(),
            name_w,
            type_w,
            rule_used,
        },
        ctx,
    );

    let name = focused.as_ref().map(|f| f.name.clone()).unwrap_or_default();
    draw_value(buf, &lay, &name, &pane, modal, ctx);
    for (i, (text, tone)) in note
        .iter()
        .take(note_rows.saturating_sub(1) as usize)
        .enumerate()
    {
        let style = match tone {
            Tone::Dim => Style::default().fg(ctx.dimmed),
            _ => Style::default().fg(ctx.text_primary),
        };
        Paragraph::new(text.as_str()).style(style).render(
            Rect {
                x: note_area.x + 1,
                y: note_area.y + 1 + i as u16,
                width: note_area.width.saturating_sub(1),
                height: 1,
            },
            buf,
        );
    }
}

/// The codebook's lines for a field: its meaning and unit, then what the value under
/// the cursor stands for when the codebook lists it.
pub fn codebook_lines(
    column: &crate::codebook::Column,
    shown: &Shown,
    width: usize,
) -> Vec<(String, Tone)> {
    let mut lines = Vec::new();
    let about = column.about();
    if !about.is_empty() {
        reader::wrap_lines(&about, width.max(1), Tone::Dim, &mut lines);
    }
    let value = match shown {
        Shown::Null(_) => Some(None),
        Shown::Value(v) => Some(Some(exact::value_text(v))),
        _ => None,
    };
    if let Some(line) = value.and_then(|v| column.legend_line(v.as_deref())) {
        reader::wrap_lines(&line, width.max(1), Tone::Plain, &mut lines);
    }
    lines
}

/// The chip on the list's rule: how many fields, how many null and empty, how
/// many differ, and the order when it is not the table's.
fn list_chip(
    modal: &InspectorModal,
    state: &DataTableState,
    row: &InspectRow,
    other: Option<&Compared>,
) -> String {
    let c = counts(modal, state, row, other);
    let total = thousands(modal.fields.len());
    let mut parts = vec![if modal.visible.len() < modal.fields.len() {
        format!("{} of {total}", thousands(modal.visible.len()))
    } else {
        total
    }];
    match c.differ {
        Some(n) => parts.push(format!("{} differ", thousands(n))),
        None => {
            if c.nulls > 0 {
                parts.push(format!("{} null", thousands(c.nulls)));
            }
            if c.empties > 0 {
                parts.push(format!("{} empty", thousands(c.empties)));
            }
        }
    }
    if modal.filled_only {
        parts.push(
            if c.differ.is_some() {
                "differ only"
            } else {
                "nulls hidden"
            }
            .to_string(),
        );
    }
    if let Some(order) = modal.order.label() {
        parts.push(order.to_string());
    }
    join_facts(&parts)
}

/// A find line where a rule stands: the text, the cursor while typing, a count.
fn draw_find_line(
    buf: &mut Buffer,
    at: Rect,
    text: &str,
    typing: bool,
    count: &str,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let label_style = if typing {
        Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.dimmed)
    };
    let mut spans = vec![
        Span::styled("Find: ", label_style),
        Span::styled(text.to_string(), Style::default().fg(ctx.text_primary)),
    ];
    if typing {
        spans.push(Span::styled(g.cursor, Style::default().fg(ctx.accent)));
    }
    spans.push(Span::styled(
        format!("   {count}"),
        Style::default().fg(ctx.dimmed),
    ));
    Paragraph::new(Line::from(spans)).render(at, buf);
}

/// What the footer offers, as the frame found it.
struct FooterFacts {
    enter: Option<&'static str>,
    /// On a group's row, a field not read yet: `r` reads it.
    read_key: bool,
    has_value: bool,
    many_fields: bool,
    list_overflows: bool,
    value_overflows: bool,
    comparing: bool,
}

/// The keys that act now, primary first; the weights say which yield first on
/// a narrow footer, the way out last.
fn footer<'a>(
    modal: &InspectorModal,
    pane: &'a Pane,
    f: &FooterFacts,
    ctx: &RenderContext,
) -> HintBar<'a> {
    let g = crate::glyphs::get();
    let mut bar = HintBar::from_ctx(ctx);
    if modal.finding {
        return bar
            .hint_weighted("Enter", "Done", 3)
            .hint_weighted("type", "Find", 1)
            .hint_weighted("Esc", "Clear", 4);
    }
    if modal.value_find.as_ref().is_some_and(|f| f.editing) {
        return bar
            .hint_weighted("Enter", "Find", 3)
            .hint_weighted("type", "Text", 1)
            .hint_weighted("Esc", "Clear", 4);
    }
    let copy = match pane.copy {
        CopyAs::Base64 => "Copy base64",
        CopyAs::Text(_) if pane.view == Some(View::Json) => "Copy JSON",
        CopyAs::Text(_) => "Copy text",
        _ => "Copy",
    };
    let view = pane.next_view().map(View::label);
    if modal.focus == Focus::Value {
        if f.value_overflows {
            bar = bar
                .hint_weighted(g.updown, "Scroll", 8)
                .hint_weighted("Home/End", "Top/End", 6);
        }
        bar = bar.hint_weighted("/", "Find", 7);
        if modal
            .value_find
            .as_ref()
            .is_some_and(|f| !f.hits.is_empty())
        {
            bar = bar.hint_weighted("n/N", "Next", 7);
        }
        if let Some(view) = view {
            bar = bar.hint_weighted("e", view, 5);
        }
        if pane.content.wraps() {
            let wrap = match modal.wrap {
                reader::Wrap::Word => "Hard wrap",
                reader::Wrap::Hard => "Word wrap",
            };
            bar = bar.hint_weighted("w", wrap, 3);
        }
        if f.has_value {
            bar = bar
                .hint_weighted("y", copy, 4)
                .hint_weighted("o", "Open", 2);
        }
        bar = bar.hint_weighted(g.updown_lr, "Row", 1);
        return bar.hint_weighted("Esc", "Fields", 10);
    }
    if let Some(label) = f.enter {
        bar = bar.hint_weighted("Enter", label, 9);
    }
    if f.read_key {
        bar = bar.hint_weighted("r", "Read", 9);
    }
    if f.has_value {
        bar = bar.hint_weighted("Tab", "Value", 8);
        if !matches!(f.enter, Some("Read" | "Retry")) {
            bar = bar.hint_weighted("y", copy, 7);
        }
    }
    // The arrows are the first keys anyone tries: their chip yields before the
    // view key, which nothing else would reveal.
    if f.many_fields {
        bar = bar.hint_weighted(g.updown, "Field", 4);
    }
    bar = bar
        .hint_weighted(g.updown_lr, "Row", 6)
        .hint_weighted("/", "Find", 5);
    if let Some(view) = view {
        bar = bar.hint_weighted("e", view, 5);
    }
    if f.list_overflows {
        bar = bar.hint_weighted("PgUp/PgDn", "Page", 3);
    }
    // The toggle names its state: what `f` hides is the nulls and empties.
    let filled = match (f.comparing, modal.filled_only) {
        (true, _) => "Differ",
        (false, true) => "Nulls: hidden",
        (false, false) => "Nulls: shown",
    };
    bar = bar.hint_weighted("Y", "Copy row", 2);
    // Comparing, Esc is the way out of Compare, and says so.
    if !modal.compare {
        bar = bar.hint_weighted("c", "Compare", 2);
    }
    bar = bar.hint_weighted("f", filled, 1);
    if f.comparing {
        bar = bar.hint_weighted("m", "Pin", 1);
    }
    // Esc backs out a level at a time: the find, Compare, then the inspector.
    let esc = if !modal.filter.is_empty() {
        "Clear"
    } else if modal.compare {
        "No compare"
    } else {
        "Close"
    };
    bar.hint_weighted("Esc", esc, 10)
}

/// The rows the list shows, and its name and type columns' widths.
struct ListOf<'a> {
    row: Option<&'a InspectRow>,
    other: Option<&'a Compared>,
    name_w: usize,
    type_w: usize,
    /// Cells of the list's rule its title and chip take, when it is a rule:
    /// Compare names its rows over the rest.
    rule_used: Option<usize>,
}

/// The fields, in as many columns as the layout has: rail, name, type, the
/// table's preview, and with Compare the other rows' previews in row order, a
/// mark where they differ and the rows named over them on the rule. The first
/// slot counts the fields above, the last those below.
fn draw_fields(
    buf: &mut Buffer,
    lay: &Layout,
    modal: &mut InspectorModal,
    state: &DataTableState,
    of: ListOf,
    ctx: &RenderContext,
) {
    let ListOf {
        row,
        other,
        name_w,
        type_w,
        rule_used,
    } = of;
    let g = crate::glyphs::get();
    let rows = lay.list.height as usize;
    let cols = lay.cols.max(1);
    let cap = rows * cols;
    let n = modal.visible.len();
    let sel = modal.focused_position();
    let (offset, above, below) = list_window(n, sel, modal.list_offset, cap);
    let items = cap - usize::from(above) - usize::from(below);
    modal.list_offset = offset;
    modal.list_page = items.max(1);
    let list_w = lay.list.width as usize;
    let col_w = if cols > 1 {
        (list_w - GAP * (cols - 1)) / cols
    } else {
        list_w
    };
    // Names stay whole while a column has its narrowest preview beside them.
    let beside = 1 + GAP + type_w + GAP + PREVIEW_MIN;
    let name_w = name_w.min((col_w / 3).max(col_w.saturating_sub(beside)).max(4));
    let rest = col_w.saturating_sub(1 + name_w + GAP + type_w + GAP);
    let read = modal.read.as_ref();
    // The rows previewed, in row order, each with what was read for it.
    let cells: Vec<(Option<&InspectRow>, Option<&FieldRead>)> = match other {
        Some(c) if c.both => vec![
            (c.before.as_ref(), None),
            (row, read),
            (c.after.as_ref(), None),
        ],
        Some(c) => vec![(row, read), (c.after.as_ref(), None)],
        None => vec![(row, read)],
    };
    // With Compare, a gap between previews and two cells for the mark.
    let each = match cells.len() {
        1 => rest,
        n => rest.saturating_sub(GAP * (n - 1) + 2) / n,
    };
    let slot_rect = |slot: usize| {
        let col = slot / rows;
        let r = slot % rows;
        Rect {
            x: lay.list.x + (col * (col_w + GAP)) as u16,
            y: lay.list.y + r as u16,
            width: col_w as u16,
            height: 1,
        }
    };
    if n == 0 {
        Paragraph::new("No field matches")
            .style(Style::default().fg(ctx.dimmed))
            .render(slot_rect(0), buf);
        return;
    }
    let dim = Style::default().fg(ctx.dimmed);
    if above {
        Paragraph::new(format!("  {} {} above", g.ellipsis, thousands(offset)))
            .style(dim)
            .render(slot_rect(0), buf);
    }
    if below {
        let left = n - offset - items;
        Paragraph::new(format!("  {} {} more", g.ellipsis, thousands(left)))
            .style(dim)
            .render(slot_rect(cap - 1), buf);
    }
    if let (Some(used), Some(row), Some(_)) = (rule_used, row, other) {
        let first = 1 + name_w + GAP + type_w + GAP;
        let rows: Vec<(usize, usize, bool)> = cells
            .iter()
            .enumerate()
            .filter_map(|(j, (at, _))| {
                at.map(|r| (first + j * (each + GAP), r.display_row, r.row == row.row))
            })
            .collect();
        draw_compare_labels(buf, lay.list_rule, &rows, each, used, ctx);
    }
    let list_focused = modal.focus == Focus::List && !modal.finding;
    let value_cell = |field: &InspectField, at: Option<&InspectRow>, room: usize, own_read| match at
    {
        Some(r) => match shown(field, r, own_read, state) {
            Shown::Value(v) => match empty_preview(&v) {
                Some(empty) => (empty, dim),
                None => (
                    preview(field, &v, room, ctx),
                    Style::default().fg(ctx.text_primary),
                ),
            },
            Shown::Null(kind) => (
                null_glyph(kind).to_string(),
                dim.add_modifier(Modifier::ITALIC),
            ),
            Shown::Unread => ("not read".to_string(), dim),
            Shown::Reading => ("reading...".to_string(), dim),
            Shown::Failed(_) => ("not read".to_string(), Style::default().fg(ctx.warning)),
        },
        None => (String::new(), Style::default()),
    };
    for k in 0..items.min(n - offset) {
        let i = offset + k;
        let index = modal.visible[i];
        let field = &modal.fields[index];
        let at = slot_rect(k + usize::from(above));
        let is_selected = i == sel;
        let marker = match (is_selected, modal.finding, list_focused) {
            (true, false, true) => g.rail,
            (true, true, _) => g.middot,
            _ => " ",
        };
        let mut name = field.name.clone();
        if field.hidden {
            name = format!("{name} {}", g.hidden_mark);
        }
        let name = crate::glyphs::fit_cells(&name, name_w, g.ellipsis);
        let name_pad = name_w.saturating_sub(crate::glyphs::cell_width(&name));
        let label =
            crate::glyphs::fit_cells(&dtype_label(&field.dtype), type_w, g.ellipsis).into_owned();
        let label_pad = type_w.saturating_sub(crate::glyphs::cell_width(&label));
        let name_style = if is_selected {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else if ctx.column_colors {
            Style::default().fg(ctx.type_color(&field.dtype))
        } else {
            Style::default().fg(ctx.text_primary)
        };
        let mut spans = vec![
            Span::styled(marker, Style::default().fg(ctx.accent)),
            Span::styled(name.into_owned(), name_style),
            Span::raw(" ".repeat(name_pad + GAP)),
            Span::styled(label, dim),
            Span::raw(" ".repeat(label_pad + GAP)),
        ];
        for (j, (at_row, own)) in cells.iter().enumerate() {
            let (text, style) = value_cell(field, *at_row, each, *own);
            let text = crate::glyphs::fit_cells(&text, each, g.ellipsis).into_owned();
            let pad = each.saturating_sub(crate::glyphs::cell_width(&text));
            spans.push(Span::styled(text, style));
            if j + 1 < cells.len() {
                spans.push(Span::raw(" ".repeat(pad + GAP)));
            } else if other.is_some() {
                spans.push(Span::raw(" ".repeat(pad + 1)));
            }
        }
        if let (Some(other), Some(this_row)) = (other, row) {
            let differ = differs_from(&shown(field, this_row, read, state), field, other, state);
            if differ == Some(true) {
                spans.push(Span::styled(
                    g.diff_mark,
                    Style::default()
                        .fg(ctx.text_primary)
                        .add_modifier(Modifier::BOLD),
                ));
            }
        }
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected && list_focused {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(at, buf);
    }
}

/// Compare's rows named over their previews on the list's rule, `Row 41`: the
/// row shown in the text color, the others dimmed. `rows` are each preview's
/// cells into the rule and its row's number, and whether it is the row shown.
/// A name is drawn where it fits its column and clears the rule's title and
/// chip, the first `used` cells.
fn draw_compare_labels(
    buf: &mut Buffer,
    rule: Rect,
    rows: &[(usize, usize, bool)],
    each: usize,
    used: usize,
    ctx: &RenderContext,
) {
    for &(x, n, this) in rows {
        let label = format!(" Row {} ", thousands(n));
        // A name starts a cell left of its preview, over the gap, so its words
        // sit over the values.
        if x <= used + 1
            || crate::glyphs::cell_width(&label) > each + 1
            || x + each >= rule.width as usize
        {
            continue;
        }
        let style = if this {
            Style::default().fg(ctx.text_primary)
        } else {
            Style::default().fg(ctx.dimmed)
        };
        Paragraph::new(label.clone()).style(style).render(
            Rect {
                x: rule.x + (x - 1) as u16,
                width: crate::glyphs::cell_width(&label) as u16,
                ..rule
            },
            buf,
        );
    }
}

/// The focused value under its rule: its rows from where the reader is, the
/// rail down its left while it has the focus, and where it is on the rule.
fn draw_value(
    buf: &mut Buffer,
    lay: &Layout,
    name: &str,
    pane: &Pane,
    modal: &mut InspectorModal,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let h = lay.value.height as usize;
    modal.page = h.max(1);
    let focused = modal.focus == Focus::Value;
    // A search's places are for the pane they were found in.
    if let Some(find) = modal.value_find.as_mut()
        && find.pane != pane.id
        && !find.text.is_empty()
    {
        find.hits = reader::find_hits(&pane.content, &find.text);
        find.current = None;
        find.pane = pane.id;
    }
    let win: Window = modal.reader.window(&pane.content, h);
    let rule = lay.value_rule;
    match modal
        .value_find
        .as_ref()
        .filter(|f| f.editing || !f.text.is_empty())
    {
        Some(find) => {
            let count = match (find.hits.len(), find.current) {
                (0, _) if !find.editing => "no match".to_string(),
                (0, _) => String::new(),
                (n, Some(at)) => format!("{} of {}", thousands(at + 1), thousands(n)),
                (n, None) => plural(n, "match", "matches"),
            };
            draw_find_line(buf, rule, &find.text, find.editing, &count, ctx);
        }
        None => {
            let width = rule.width as usize;
            let name = crate::glyphs::fit_cells(name, width / 3, g.ellipsis);
            let name_w = crate::glyphs::cell_width(&name);
            let overflows = win.above || win.below;
            // The position, at its widest plus its padding and a cell of rule after
            // it, comes before the facts: reading a long value, where you are
            // matters more than its size, and the facts must not shift as it changes.
            let reserve = if overflows {
                reader::Reader::position_width(&pane.content) + 3
            } else {
                0
            };
            // The title and its space, the chip's padding and space, a bit of rule.
            let room = width.saturating_sub(name_w + 1 + 3 + 2 + reserve);
            let facts = (!pane.facts.is_empty() && room >= 4)
                .then(|| crate::glyphs::fit_cells(&pane.facts, room, g.ellipsis));
            SectionRule {
                title: &name,
                chip: facts.as_deref(),
            }
            .render(rule, buf, ctx);
            let used = name_w
                + 1
                + facts
                    .as_deref()
                    .map_or(0, |f| crate::glyphs::cell_width(f) + 3);
            if overflows {
                let text = format!(" {} ", modal.reader.position(&pane.content, &win));
                let w = crate::glyphs::cell_width(&text);
                // Over the rule's tail, where the rule has room for it.
                if used + 2 + w < width {
                    Paragraph::new(text).style(dim_or(ctx, focused)).render(
                        Rect {
                            x: rule.x + (width - w - 1) as u16,
                            width: w as u16,
                            ..rule
                        },
                        buf,
                    );
                }
            }
        }
    }
    let needle = modal
        .value_find
        .as_ref()
        .map(|f| f.text.clone())
        .unwrap_or_default();
    let text_w = pane_width(lay.value) as u16;
    for (i, row) in win.rows.iter().enumerate() {
        let y = lay.value.y + i as u16;
        if focused {
            Paragraph::new(g.rail)
                .style(Style::default().fg(ctx.accent))
                .render(
                    Rect {
                        x: lay.value.x,
                        y,
                        width: 1,
                        height: 1,
                    },
                    buf,
                );
        }
        let style = match row.tone {
            Tone::Plain => Style::default().fg(ctx.text_primary),
            Tone::Dim => Style::default().fg(ctx.dimmed),
            Tone::Warn => Style::default().fg(ctx.warning),
        };
        let mut spans = Vec::new();
        let mut at = 0;
        for (a, b) in reader::hits_in_row(&row.text, &needle) {
            spans.push(Span::styled(row.text[at..a].to_string(), style));
            spans.push(Span::styled(row.text[a..b].to_string(), ctx.find_match));
            at = b;
        }
        spans.push(Span::styled(row.text[at..].to_string(), style));
        Paragraph::new(Line::from(spans)).render(
            Rect {
                x: lay.value.x + 1,
                y,
                width: text_w.min(lay.value.width.saturating_sub(1)),
                height: 1,
            },
            buf,
        );
    }
}

fn dim_or(ctx: &RenderContext, focused: bool) -> Style {
    if focused {
        Style::default().fg(ctx.accent)
    } else {
        Style::default().fg(ctx.dimmed)
    }
}

/// Whether Enter opens `value` as a level: a list, array or struct, or text that
/// reads as a JSON object or array.
pub fn value_opens(value: &AnyValue) -> bool {
    match value {
        AnyValue::String(s) => crate::inspector_drill::opens_as_json(s),
        AnyValue::StringOwned(s) => crate::inspector_drill::opens_as_json(s),
        v => exact::is_nested_value(v),
    }
}

/// The value pane for an item of a level drilled into, labelled `label`.
pub fn node_pane(node: &Node, choice: Option<View>, width: usize) -> Pane {
    let ask = PaneAsk {
        choice,
        width,
        table: None,
        indented: Indented::None,
        not_json: false,
        unpacked: Unpacked::Unavailable,
        read_key: "Enter",
    };
    match node {
        Node::Native(series) => {
            let shown = match series.get(0) {
                Ok(AnyValue::Null) | Err(_) => Shown::Null(NullKind::Null),
                Ok(v) => Shown::Value(v),
            };
            pane(series.dtype(), &shown, &ask)
        }
        Node::Json { .. } => json_pane(node.json().unwrap_or(&JsonValue::Null), &ask),
    }
}

/// The value pane for a JSON value: text as text is shown, an object or array
/// indented up to a few chunks.
fn json_pane(value: &JsonValue, ask: &PaneAsk) -> Pane {
    let g = crate::glyphs::get();
    let width = ask.width.clamp(1, MEASURE);
    let scalar = |text: String, kind: &str| {
        let mut lines = Vec::new();
        reader::wrap_lines(&text, width, Tone::Plain, &mut lines);
        Pane::lines(lines, vec![kind.to_string()])
    };
    match value {
        JsonValue::String(s) => pane(&DataType::String, &Shown::Value(AnyValue::String(s)), ask),
        JsonValue::Null => Pane::lines(
            vec![(format!("{} null", g.null), Tone::Dim)],
            vec!["null".to_string()],
        ),
        JsonValue::Bool(b) => scalar(b.to_string(), "bool"),
        JsonValue::Number(n) => scalar(n.to_string(), "number"),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let (kind, count) = match value {
                JsonValue::Object(map) => ("object", plural(map.len(), "key", "keys")),
                JsonValue::Array(items) => ("array", plural(items.len(), "item", "items")),
                _ => unreachable!(),
            };
            let cap = CHUNK_BYTES * 4;
            let (text, cut) = json_text(value, true, cap);
            let mut facts = vec![kind.to_string(), count];
            if cut {
                facts.push(format!("first {} KB", cap / 1024));
            }
            Pane {
                id: 0,
                facts: join_facts(&facts),
                content: Content::text(Arc::from(text), TextForm::Raw),
                views: Vec::new(),
                view: None,
                copy: CopyAs::Stored,
                indent: false,
                unpack: false,
            }
        }
    }
}

/// An item's one-line preview in a level's list, and its style.
fn node_preview(node: &Node, room: usize, ctx: &RenderContext) -> (String, Style) {
    let g = crate::glyphs::get();
    let plain = Style::default().fg(ctx.text_primary);
    let dim = Style::default().fg(ctx.dimmed);
    let null = (g.null.to_string(), dim.add_modifier(Modifier::ITALIC));
    let budget = room.saturating_mul(4).max(16);
    match node {
        Node::Native(series) => match series.get(0) {
            Ok(AnyValue::Null) | Err(_) => null,
            Ok(v) => match empty_preview(&v) {
                Some(empty) => (empty, dim),
                None => {
                    let field = InspectField {
                        name: series.name().to_string(),
                        dtype: series.dtype().clone(),
                        hidden: false,
                    };
                    (preview(&field, &v, room, ctx), plain)
                }
            },
        },
        Node::Json { .. } => match node.json() {
            None | Some(JsonValue::Null) => null,
            Some(JsonValue::String(s)) if s.is_empty() => ("\"\"".to_string(), dim),
            Some(JsonValue::String(s)) => (
                exact::preview(exact::prefix(s, budget), g).into_owned(),
                plain,
            ),
            Some(v) => (
                exact::preview(&json_text(v, false, budget).0, g).into_owned(),
                plain,
            ),
        },
    }
}

/// The title inside a drill: the row's, then each level's step. When it does not
/// fit, the first steps after the row give way to an ellipsis: the row and where
/// the drill is now stay.
fn drill_title(root: &str, labels: &[&str], max: usize) -> String {
    let g = crate::glyphs::get();
    let sep = format!(" {} ", g.trail);
    let steps: Vec<String> = labels
        .iter()
        .map(|l| exact::preview(l, g).into_owned())
        .collect();
    let joined = |skip: usize| {
        let mut parts = vec![root.to_string()];
        if skip > 0 {
            parts.push(g.ellipsis.to_string());
        }
        parts.extend(steps[skip..].iter().cloned());
        parts.join(&sep)
    };
    for skip in 0..steps.len() {
        let title = joined(skip);
        if crate::glyphs::cell_width(&title) <= max {
            return title;
        }
    }
    let last = joined(steps.len().saturating_sub(1));
    crate::glyphs::fit_cells(&last, max, g.ellipsis).into_owned()
}

/// Draw a level of a drill: its items (a table, for a list of structs) above the
/// focused item's value.
fn render_drill(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut InspectorModal,
    root_title: &str,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let Some(drill) = modal.drill.clone() else {
        return;
    };
    let level = drill.level();
    let node = &level.node;
    let shape = node.shape();
    let len = node.len();
    let selected = level.selected.min(len.saturating_sub(1));
    let focused = node.child(selected);
    let content = Surface::content_area(area);
    let columns = node.table_columns();
    let header = usize::from(columns.is_some());
    // Stacked at every width: a table of items wants the width.
    let stacked = Rect {
        width: content.width.min(WIDE as u16 - 1),
        ..content
    };
    let full = pane_width(content);
    let width = full.min(MEASURE);

    let pane = match &focused {
        Some((label, child)) => {
            let key = PaneKey {
                frame: drill.frame,
                row: drill.row,
                field: drill.item_key(label),
                view: modal.view,
                width: full as u16,
                state: 5,
                pretty: 0,
                unpacked: 0,
            };
            match &modal.pane {
                Some((cached, pane)) if *cached == key => pane.clone(),
                _ => {
                    let mut built = node_pane(child, modal.view, full);
                    built.id = renewed(modal, &key, &built);
                    modal.pane = Some((key, built.clone()));
                    built
                }
            }
        }
        None => Pane::lines(
            vec![(
                format!("No {}", shape.items_title().to_lowercase()),
                Tone::Dim,
            )],
            Vec::new(),
        ),
    };
    modal.reader.prepare(pane.id, width, modal.wrap);
    let need = modal
        .reader
        .rows_needed(&pane.content, content.height as usize);
    let mut lay = layout(
        Rect {
            height: content.height.saturating_sub(header as u16),
            ..stacked
        },
        len,
        need,
        ListShape {
            fields: len,
            min_col: 1,
            single: true,
            bytes: false,
        },
    );
    // The table's header takes the row under the rule; the rest move down one.
    lay.list.y += header as u16;
    lay.value_rule.y += header as u16;
    lay.value.y += header as u16;
    lay.value_rule.width = content.width;
    lay.value.width = content.width;
    lay.list.width = content.width;
    lay.list_rule.width = content.width;
    let list_h = lay.list.height as usize;
    let value_rows = lay.value.height as usize;
    let overflows = need > value_rows || modal.reader.window(&pane.content, value_rows).above;
    let opens = focused.as_ref().is_some_and(|(label, child)| {
        child.opens() && !modal.known_not_json(drill.frame, drill.row, &drill.item_key(label))
    });

    let mut bar = HintBar::from_ctx(ctx);
    if modal.focus == Focus::Value {
        bar = footer(
            modal,
            &pane,
            &FooterFacts {
                enter: None,
                read_key: false,
                has_value: focused.is_some(),
                many_fields: len > 1,
                list_overflows: false,
                value_overflows: overflows,
                comparing: false,
            },
            ctx,
        );
    } else {
        if opens {
            bar = bar.hint_weighted("Enter", "Open", 9);
        }
        if focused.is_some() {
            bar = bar
                .hint_weighted("Tab", "Value", 8)
                .hint_weighted("y", "Copy", 7);
        }
        if len > 1 {
            let word = match shape {
                Shape::Struct => "Field",
                Shape::Object => "Key",
                _ => "Item",
            };
            bar = bar.hint_weighted(g.updown, word, 6);
        }
        if let Some(view) = pane.next_view() {
            bar = bar.hint_weighted("e", view.label(), 3);
        }
        if len > list_h {
            bar = bar.hint_weighted("PgUp/PgDn", "Page", 2);
        }
        bar = bar.hint_weighted("Esc", "Back", 10);
    }

    let labels: Vec<&str> = drill.levels.iter().map(|l| l.label.as_str()).collect();
    let title = drill_title(root_title, &labels, area.width.saturating_sub(4) as usize);
    let content = Surface::new(&title).footer(&bar).render(area, buf, ctx);
    if content.height < 4 || content.width < 12 {
        return;
    }
    let count = thousands(len);
    SectionRule {
        title: shape.items_title(),
        chip: Some(&count),
    }
    .render(lay.list_rule, buf, ctx);

    let offset = list_window(len, selected, modal.list_offset, list_h).0;
    modal.list_offset = offset;
    modal.list_page = list_h.saturating_sub(2).max(1);
    let below = len.saturating_sub(offset + list_h);
    let page = node.children(offset, list_h);
    let rows = ListRows {
        y: lay.list.y,
        offset,
        selected,
        below,
        list_h,
        focused: modal.focus == Focus::List,
    };
    match columns {
        Some(columns) => draw_table(buf, content, node, &columns, &page, &rows, ctx),
        None => draw_items(buf, content, node, &page, &rows, ctx),
    }

    let name = focused.map(|(label, _)| label).unwrap_or_default();
    draw_value(buf, &lay, &name, &pane, modal, ctx);
}

/// Where a level's items are drawn, and which of them.
struct ListRows {
    y: u16,
    offset: usize,
    selected: usize,
    /// Items past the last row drawn.
    below: usize,
    list_h: usize,
    focused: bool,
}

impl ListRows {
    /// The `i`th row drawn is the last, with items under it: it counts them instead.
    fn more_at(&self, i: usize) -> bool {
        i + 1 == self.list_h && self.below > 0 && self.offset + i != self.selected
    }

    fn more_line(&self) -> String {
        format!(
            "  {} {} more",
            crate::glyphs::get().ellipsis,
            thousands(self.below + 1)
        )
    }
}

/// The rail and the label at the start of an item's row, and the row's style.
fn item_head(
    label: &str,
    label_w: usize,
    name_style: Style,
    is_selected: bool,
    focused: bool,
    ctx: &RenderContext,
) -> Vec<Span<'static>> {
    let g = crate::glyphs::get();
    let label = exact::preview(label, g);
    let label = crate::glyphs::fit_cells(&label, label_w, g.ellipsis).into_owned();
    let pad = label_w.saturating_sub(crate::glyphs::cell_width(&label));
    let name_style = if is_selected {
        Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
    } else {
        name_style
    };
    vec![
        Span::styled(
            if is_selected && focused { g.rail } else { " " },
            Style::default().fg(ctx.accent),
        ),
        Span::styled(label, name_style),
        Span::raw(" ".repeat(pad + GAP)),
    ]
}

/// A struct's fields, a list's items or an object's keys: name, type, preview.
fn draw_items(
    buf: &mut Buffer,
    content: Rect,
    node: &Node,
    page: &[(String, Node)],
    rows: &ListRows,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let width = content.width as usize;
    // An object's keys are measured only up to its first thousand, so a page past
    // them is measured too: a wider key there would be cut to the same few cells.
    let page_w = page
        .iter()
        .map(|(label, _)| crate::glyphs::cell_width(&exact::preview(label, g)))
        .max()
        .unwrap_or(0);
    let label_w = node
        .label_width()
        .max(page_w)
        .clamp(3, (width / 3).clamp(4, 28));
    // Measured over the level, not the page, so scrolling moves no column.
    let type_w = match node.shape() {
        Shape::Object | Shape::Array => "object".len(),
        Shape::List => node
            .child(0)
            .map_or(0, |(_, c)| crate::glyphs::cell_width(&c.type_label())),
        _ => node
            .children(0, usize::MAX)
            .iter()
            .map(|(_, c)| crate::glyphs::cell_width(&c.type_label()))
            .max()
            .unwrap_or(0),
    }
    .min(14);
    let preview_w = width.saturating_sub(1 + label_w + GAP + type_w + GAP);
    for (i, (label, child)) in page.iter().enumerate() {
        let y = rows.y + i as u16;
        let at = Rect {
            y,
            height: 1,
            ..content
        };
        if rows.more_at(i) {
            Paragraph::new(rows.more_line())
                .style(Style::default().fg(ctx.dimmed))
                .render(at, buf);
            break;
        }
        let is_selected = rows.offset + i == rows.selected;
        let name_style = if ctx.column_colors {
            Style::default().fg(ctx.type_color(&child.color_dtype()))
        } else {
            Style::default().fg(ctx.text_primary)
        };
        let mut spans = item_head(label, label_w, name_style, is_selected, rows.focused, ctx);
        let kind = crate::glyphs::fit_cells(&child.type_label(), type_w, g.ellipsis).into_owned();
        let kind_pad = type_w.saturating_sub(crate::glyphs::cell_width(&kind));
        spans.push(Span::styled(kind, Style::default().fg(ctx.dimmed)));
        spans.push(Span::raw(" ".repeat(kind_pad + GAP)));
        let (text, style) = node_preview(child, preview_w, ctx);
        let text = crate::glyphs::fit_cells(&text, preview_w, g.ellipsis).into_owned();
        spans.push(Span::styled(text, style));
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected && rows.focused {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(at, buf);
    }
}

/// Items measured for a table's column widths: the first ones, so scrolling the
/// table moves no column.
const TABLE_SAMPLE: usize = 32;
/// The widest a table column grows.
const TABLE_CELL_MAX: usize = 24;

/// A list of structs or an array of objects as a table: a header of the fields,
/// then a row per item, as many columns as fit.
fn draw_table(
    buf: &mut Buffer,
    content: Rect,
    node: &Node,
    columns: &[(String, DataType)],
    page: &[(String, Node)],
    rows: &ListRows,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let width = content.width as usize;
    let label_w = node.label_width().max(3);
    let sample = node.children(0, TABLE_SAMPLE);
    let cell_text = |item: &Node, column: &str, room: usize| -> (String, Style) {
        if item.shape() == Shape::Leaf {
            // A null item, or one that is not an object, fills its row's first cell.
            return node_preview(item, room, ctx);
        }
        match item.cell(column) {
            Some(cell) => node_preview(&cell, room, ctx),
            None => (g.absent.to_string(), Style::default().fg(ctx.dimmed)),
        }
    };
    let mut widths = Vec::new();
    let mut used = 1 + label_w + GAP;
    for (column, _) in columns {
        let w = sample
            .iter()
            .map(|(_, item)| crate::glyphs::cell_width(&cell_text(item, column, TABLE_CELL_MAX).0))
            .chain(std::iter::once(crate::glyphs::cell_width(column)))
            .max()
            .unwrap_or(0)
            .clamp(3, TABLE_CELL_MAX);
        if used + w > width {
            break;
        }
        used += w + GAP;
        widths.push(w);
    }
    // At least one column, cut to the room there is.
    if widths.is_empty() {
        widths.push(width.saturating_sub(1 + label_w + GAP).max(1));
    }
    let hidden = columns.len() - widths.len();

    let cell = |text: &str, w: usize| {
        let text = crate::glyphs::fit_cells(text, w, g.ellipsis).into_owned();
        let pad = w.saturating_sub(crate::glyphs::cell_width(&text));
        (text, " ".repeat(pad + GAP))
    };
    let mut head = vec![Span::raw(" ".repeat(1 + label_w + GAP))];
    for ((column, dtype), &w) in columns.iter().zip(&widths) {
        let (text, pad) = cell(&exact::preview(column, g), w);
        // A column's name takes its type's color, as the table's header does.
        let fg = if ctx.column_colors {
            ctx.type_color(dtype)
        } else {
            ctx.dimmed
        };
        head.push(Span::styled(
            text,
            Style::default().fg(fg).add_modifier(Modifier::BOLD),
        ));
        head.push(Span::raw(pad));
    }
    if hidden > 0 {
        head.push(Span::styled(
            format!("+{hidden}"),
            Style::default().fg(ctx.dimmed),
        ));
    }
    Paragraph::new(Line::from(head)).render(
        Rect {
            // The header stands on the row above the items.
            y: rows.y - 1,
            height: 1,
            ..content
        },
        buf,
    );

    for (i, (label, item)) in page.iter().enumerate() {
        let at = Rect {
            y: rows.y + i as u16,
            height: 1,
            ..content
        };
        if rows.more_at(i) {
            Paragraph::new(rows.more_line())
                .style(Style::default().fg(ctx.dimmed))
                .render(at, buf);
            break;
        }
        let is_selected = rows.offset + i == rows.selected;
        let mut spans = item_head(
            label,
            label_w,
            Style::default().fg(ctx.dimmed),
            is_selected,
            rows.focused,
            ctx,
        );
        for (k, ((column, _), &w)) in columns.iter().zip(&widths).enumerate() {
            if k > 0 && item.shape() == Shape::Leaf {
                break;
            }
            let (text, style) = cell_text(item, column, w);
            let (text, pad) = cell(&text, w);
            spans.push(Span::styled(text, style));
            spans.push(Span::raw(pad));
        }
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected && rows.focused {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(at, buf);
    }
}

/// The row as one JSON object, field by field in the table's order: numbers
/// exact, text and dates as strings, lists and structs as JSON, bytes as base64.
/// Fields not read are left out and counted.
pub fn row_json(
    fields: &[InspectField],
    row: &InspectRow,
    read: Option<&FieldRead>,
) -> (String, usize, usize) {
    let mut out = String::from("{");
    let (mut kept, mut unread) = (0usize, 0usize);
    for field in fields {
        let column = if field.buffered() {
            row.values.column(&field.name).ok().cloned()
        } else {
            match read {
                Some(FieldRead::Read { values, .. })
                    if read.is_some_and(|r| r.key() == (row.frame, row.row)) =>
                {
                    values.column(&field.name).ok().cloned()
                }
                _ => None,
            }
        };
        let Some(column) = column else {
            unread += 1;
            continue;
        };
        let Ok(value) = column.get(0) else {
            unread += 1;
            continue;
        };
        if kept > 0 {
            out.push_str(", ");
        }
        out.push_str(&serde_json::to_string(&field.name).unwrap_or_default());
        out.push_str(": ");
        out.push_str(&json_value_text(&column, &value));
        kept += 1;
    }
    out.push('}');
    (out, kept, unread)
}

/// One value as JSON: exact numbers bare, everything else as the exact text in
/// quotes, nested values as the JSON a copy writes.
fn json_value_text(column: &Column, value: &AnyValue) -> String {
    let quoted = |s: &str| serde_json::to_string(s).unwrap_or_default();
    match value {
        AnyValue::Null => "null".to_string(),
        AnyValue::Boolean(b) => b.to_string(),
        AnyValue::Int8(_)
        | AnyValue::Int16(_)
        | AnyValue::Int32(_)
        | AnyValue::Int64(_)
        | AnyValue::Int128(_)
        | AnyValue::UInt8(_)
        | AnyValue::UInt16(_)
        | AnyValue::UInt32(_)
        | AnyValue::UInt64(_) => exact::value_text(value),
        AnyValue::Float32(_) | AnyValue::Float64(_) => {
            let text = exact::value_text(value);
            // NaN and the infinities have no JSON number.
            if text.parse::<f64>().is_ok_and(f64::is_finite) {
                text
            } else {
                quoted(&text)
            }
        }
        v if exact::is_nested_value(v) => {
            exact::copy_text(column).unwrap_or_else(|_| "null".to_string())
        }
        v => quoted(&exact::value_text(v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(width: usize) -> PaneAsk<'static> {
        PaneAsk {
            choice: None,
            width,
            table: None,
            indented: Indented::None,
            not_json: false,
            unpacked: Unpacked::None,
            read_key: "Enter",
        }
    }

    fn lines(p: &Pane) -> Vec<String> {
        let mut r = reader::Reader::default();
        r.prepare(1, 100, reader::Wrap::Word);
        r.window(&p.content, 1000)
            .rows
            .into_iter()
            .map(|s| s.text)
            .collect()
    }

    #[test]
    fn a_float_shows_exact_and_what_the_table_rounds_it_to() {
        let p = pane(
            &DataType::Float64,
            &Shown::Value(AnyValue::Float64(1000000.125)),
            &PaneAsk {
                table: Some("1.0000e6"),
                ..ask(40)
            },
        );
        assert_eq!(lines(&p), ["1000000.125", "", "In the table: 1.0000e6"]);
        assert_eq!(p.facts, "f64");
        let p = pane(
            &DataType::Float64,
            &Shown::Value(AnyValue::Float64(2.5)),
            &PaneAsk {
                table: Some("2.5"),
                ..ask(40)
            },
        );
        assert_eq!(lines(&p), ["2.5"]);
        assert!(p.views.is_empty(), "a number has one view");
    }

    #[test]
    fn text_reads_raw_or_escaped_and_names_its_facts() {
        let value = Shown::Value(AnyValue::String("line1\nline2\ttab\\n"));
        let raw = pane(&DataType::String, &value, &ask(40));
        assert_eq!(lines(&raw), ["line1", "line2   tab\\n"]);
        let m = crate::glyphs::get().middot;
        assert_eq!(raw.facts, format!("str {m} 17 chars {m} 2 lines"));
        assert_eq!(raw.views, [View::Raw, View::Escaped]);
        assert_eq!(raw.next_view(), Some(View::Escaped));
        let esc = pane(
            &DataType::String,
            &value,
            &PaneAsk {
                choice: Some(View::Escaped),
                ..ask(40)
            },
        );
        assert_eq!(lines(&esc), [r#""line1\nline2\ttab\\n""#]);
        assert!(esc.facts.ends_with("escaped"));
        assert_eq!(esc.next_view(), Some(View::Raw));
    }

    #[test]
    fn empty_text_and_edge_spaces_are_named() {
        let empty = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String("")),
            &ask(40),
        );
        assert_eq!(lines(&empty), ["empty string"]);
        let esc = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String("")),
            &PaneAsk {
                choice: Some(View::Escaped),
                ..ask(40)
            },
        );
        assert_eq!(lines(&esc), ["\"\""]);
        let padded = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String("  x ")),
            &ask(40),
        );
        assert!(
            padded.facts.contains("2 leading spaces"),
            "{}",
            padded.facts
        );
        assert!(
            padded.facts.contains("1 trailing space"),
            "{}",
            padded.facts
        );
    }

    #[test]
    fn a_null_is_not_an_empty_string_and_says_which_null() {
        let g = crate::glyphs::get();
        let null = pane(&DataType::String, &Shown::Null(NullKind::Null), &ask(60));
        assert_eq!(lines(&null), [format!("{} null", g.null)]);
        let absent = pane(&DataType::String, &Shown::Null(NullKind::Absent), &ask(80));
        assert!(lines(&absent)[0].starts_with(&format!("{} absent", g.absent)));
    }

    /// Improvement 5: JSON text reads indented, with `json` on the rule; `e`
    /// cycles JSON, raw and escaped, and copy follows the view.
    #[test]
    fn json_text_reads_indented_and_cycles_its_views() {
        let doc = r#"{"order": {"id": 1, "items": [{"sku": "A1"}]}, "flags": ["vip"]}"#;
        let p = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String(doc)),
            &ask(80),
        );
        assert_eq!(p.views, [View::Json, View::Raw, View::Escaped]);
        assert_eq!(p.view, Some(View::Json));
        assert!(p.facts.ends_with("json"), "{}", p.facts);
        assert_eq!(lines(&p)[..3], ["{", "  \"order\": {", "    \"id\": 1,"]);
        assert!(matches!(p.copy, CopyAs::Text(_)));
        assert_eq!(p.next_view(), Some(View::Raw));
        // Text that only looks like JSON has no JSON view.
        let bad = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String("{not json}")),
            &ask(80),
        );
        assert_eq!(bad.views, [View::Raw, View::Escaped]);
        // Long JSON waits on a worker, raw meanwhile.
        let long = format!("[{}0]", "1, ".repeat(30_000));
        let p = pane(
            &DataType::String,
            &Shown::Value(AnyValue::String(&long)),
            &ask(80),
        );
        assert!(p.indent && p.facts.ends_with("indenting..."), "{}", p.facts);
    }

    /// M4: bytes say their size in human units and what they are; UTF-8 bytes
    /// read as text by default, gzip offers its text, and hex is 32 bytes a row
    /// where that fits.
    #[test]
    fn bytes_are_sized_sniffed_and_read_as_text_where_they_are() {
        let m = crate::glyphs::get().middot;
        let utf8 = "Grüße\nsecond line".as_bytes();
        let p = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(utf8)),
            &ask(80),
        );
        assert_eq!(p.views, [View::Text, View::Hex, View::Escaped]);
        assert_eq!(lines(&p), ["Grüße", "second line"]);
        assert!(p.facts.contains("UTF-8 text"), "{}", p.facts);

        let big = vec![0x90u8; 1 << 20];
        let p = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(&big)),
            &ask(140),
        );
        assert_eq!(p.facts, format!("binary {m} 1.0 MB (1,048,576 bytes)"));
        assert_eq!(p.views, [View::Hex, View::Escaped]);
        assert!(matches!(p.content, Content::Hex { per_line: 32, .. }));
        assert!(matches!(p.copy, CopyAs::Base64));

        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut e, b"hello gzip").unwrap();
        let gz = e.finish().unwrap();
        let p = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(&gz)),
            &ask(80),
        );
        assert!(p.facts.ends_with("gzip"), "{}", p.facts);
        assert_eq!(p.views, [View::Hex, View::Text, View::Escaped]);
        // Its Text view asks a worker to decompress it, and shows what it answers;
        // building the pane decompresses nothing.
        let text = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(&gz)),
            &PaneAsk {
                choice: Some(View::Text),
                ..ask(80)
            },
        );
        assert!(text.unpack);
        assert_eq!(lines(&text), ["Decompressing..."]);
        let decoded = inspector_bytes::decode_text(&gz, inspector_bytes::sniff(&gz)).unwrap();
        let text = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(&gz)),
            &PaneAsk {
                choice: Some(View::Text),
                unpacked: Unpacked::Ready(Arc::new(decoded)),
                ..ask(80)
            },
        );
        assert!(!text.unpack);
        assert_eq!(lines(&text), ["hello gzip"]);
        // Bytes that do not decompress to text lose the Text view, and say so.
        let failed = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(&gz)),
            &PaneAsk {
                choice: Some(View::Text),
                unpacked: Unpacked::Failed,
                ..ask(80)
            },
        );
        assert_eq!(failed.views, [View::Hex, View::Escaped]);
        assert_eq!(failed.view, Some(View::Hex));
        assert!(failed.facts.ends_with("not text"), "{}", failed.facts);

        let empty = pane(
            &DataType::Binary,
            &Shown::Value(AnyValue::Binary(b"")),
            &ask(80),
        );
        assert_eq!(lines(&empty), ["empty binary"]);
        assert!(empty.facts.contains("empty"));
    }

    #[test]
    fn empty_values_are_visible_in_the_list() {
        let g = crate::glyphs::get();
        assert_eq!(
            empty_preview(&AnyValue::String("")).as_deref(),
            Some("\"\"")
        );
        assert_eq!(empty_preview(&AnyValue::String(" ")), None);
        assert_eq!(
            empty_preview(&AnyValue::Binary(b"")),
            Some(format!("0 bytes {} empty", g.middot))
        );
        assert_eq!(fill_of(&Shown::Value(AnyValue::String(""))), Fill::Empty);
        assert_eq!(fill_of(&Shown::Null(NullKind::Null)), Fill::Null);
        assert_eq!(fill_of(&Shown::Unread), Fill::Unknown);
    }

    #[test]
    fn values_differ_by_their_exact_text() {
        let v = |x: f64| Shown::Value(AnyValue::Float64(x));
        assert_eq!(differs(&v(1.0), &v(1.0)), Some(false));
        assert_eq!(differs(&v(f64::NAN), &v(f64::NAN)), Some(false));
        assert_eq!(differs(&v(1.0), &v(2.0)), Some(true));
        assert_eq!(differs(&v(1.0), &Shown::Null(NullKind::Null)), Some(true));
        assert_eq!(differs(&v(1.0), &Shown::Unread), None);
    }

    /// The list counts what is above and below, and the focus is always among
    /// the fields shown.
    #[test]
    fn the_list_window_keeps_the_focus_and_marks_both_ends() {
        assert_eq!(list_window(10, 3, 0, 20), (0, false, false));
        // 214 fields in 20 slots: the last slot counts the rest.
        let (o, above, below) = list_window(214, 0, 0, 20);
        assert_eq!((o, above, below), (0, false, true));
        let (o, above, below) = list_window(214, 19, 0, 20);
        assert!(above && below);
        assert!(19 >= o && 19 < o + 18, "{o}");
        let (o, above, below) = list_window(214, 213, 0, 20);
        assert!(above && !below);
        assert_eq!(o, 214 - 19);
        for sel in 0..214 {
            let (o, above, below) = list_window(214, sel, 100, 20);
            let items = 20 - usize::from(above) - usize::from(below);
            assert!(sel >= o && sel < o + items, "{sel}: {o}");
        }
    }

    /// M2: a short row lists whole at 80x24; a long one shares the rows and the
    /// value keeps what its lines need; from 140 columns the panes sit side by
    /// side at full height, and the fields flow into columns that fit.
    #[test]
    fn the_layout_fits_the_row_and_the_terminal() {
        let list = |fields: usize, min_col: usize| ListShape {
            fields,
            min_col,
            single: false,
            bytes: false,
        };
        // 80x24: the Surface's content is 76x20, 18 rows past the two rules.
        let content = Rect::new(2, 1, 76, 20);
        let l = layout(content, 14, 1, list(14, 40));
        assert!(!l.wide);
        assert_eq!(l.list.height, 14, "every field listed");
        assert_eq!(l.value.height, 4);
        let l = layout(content, 214, 1, list(214, 40));
        assert_eq!(
            (l.list.height, l.value.height),
            (15, 3),
            "the value keeps three"
        );
        let l = layout(content, 214, 40, list(214, 40));
        assert_eq!(l.value.height, 9, "a long value takes half");
        // Two fields and a long value: the list takes its two rows, the value the rest.
        let l = layout(content, 2, 1_000, list(2, 40));
        assert_eq!((l.list.height, l.value.height), (2, 16));
        // 200x50: side by side. 214 fields take three columns and the value
        // narrows to its floor; a short row leaves the value its measure.
        let content = Rect::new(2, 1, 196, 46);
        let l = layout(content, 214, 1, list(214, 44));
        assert!(l.wide);
        assert_eq!(l.value.height, 45);
        assert_eq!(l.cols, 3, "{l:?}");
        assert!(l.value.width as usize >= VALUE_FLOOR, "{l:?}");
        assert!(l.cols * l.list.height as usize >= 130);
        let l = layout(content, 14, 1, list(14, 44));
        assert_eq!((l.cols, l.value.width), (1, 88), "{l:?}");
        // Narrowing the list (a find, hidden nulls) moves nothing: the pane is sized
        // from every field.
        let narrowed = layout(content, 3, 1, list(214, 44));
        assert_eq!(narrowed.value, layout(content, 214, 1, list(214, 44)).value);
        // 300x80: every one of 214 fields; a row with bytes gives the value a
        // 32-byte hex row.
        let content = Rect::new(2, 1, 296, 76);
        let l = layout(content, 214, 1, list(214, 45));
        assert!(l.cols * l.list.height as usize >= 214, "{l:?}");
        assert_eq!(l.value.width as usize, MEASURE + 1);
        let bytes = ListShape {
            bytes: true,
            ..list(214, 45)
        };
        let l = layout(content, 214, 1, bytes);
        assert_eq!(l.value.width as usize, HEX_WIDE);
        assert_eq!(reader::hex_per_line(pane_width(l.value)), 32);
        assert!(l.cols * l.list.height as usize >= 214, "{l:?}");
        // Compare keeps the list one column and the value its measure.
        let compare = ListShape {
            single: true,
            ..bytes
        };
        let l = layout(content, 214, 1, compare);
        assert_eq!((l.cols, l.value.width as usize), (1, MEASURE + 1));
        // A short row keeps one column.
        let l = layout(content, 14, 1, list(14, 45));
        assert_eq!(l.cols, 1);
        // 140 columns: side by side.
        assert!(layout(Rect::new(2, 1, 136, 30), 14, 1, list(14, 45)).wide);
        assert!(!layout(Rect::new(2, 1, 135, 30), 14, 1, list(14, 45)).wide);
    }

    /// A long trail keeps the row and where the drill is now; the steps between
    /// give way to an ellipsis.
    #[test]
    fn a_long_trail_drops_its_middle_steps() {
        let g = crate::glyphs::get();
        let t = g.trail;
        let labels = ["customer", "address", "geo", "point"];
        let whole = drill_title("Row 1 of 5", &labels, 200);
        assert_eq!(
            whole,
            format!("Row 1 of 5 {t} customer {t} address {t} geo {t} point")
        );
        let cut = drill_title("Row 1 of 5", &labels, 34);
        assert_eq!(
            cut,
            format!("Row 1 of 5 {t} {} {t} geo {t} point", g.ellipsis)
        );
        let marked = drill_title("Row 1", &["a\nb"], 40);
        assert!(!marked.contains('\n'), "{marked}");
    }

    #[test]
    fn json_values_show_as_themselves() {
        let m = crate::glyphs::get().middot;
        let doc: JsonValue = serde_json::from_str(r#"{"a": [1, 2], "s": "x\ny"}"#).unwrap();
        let b = json_pane(&doc, &ask(40));
        assert_eq!(b.facts, format!("object {m} 2 keys"));
        assert_eq!(lines(&b)[..2], ["{", "  \"a\": ["]);
        let s = json_pane(&doc["s"], &ask(40));
        assert_eq!(lines(&s), ["x", "y"]);
        assert!(s.views.contains(&View::Escaped), "text has an escaped form");
        let n = json_pane(&JsonValue::from(1.5), &ask(40));
        assert_eq!(
            (lines(&n), n.facts.as_str()),
            (vec!["1.5".to_string()], "number")
        );
    }

    #[test]
    fn types_name_their_unit_and_zone_in_ascii() {
        let tz = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
        assert_eq!(
            type_text(&DataType::Datetime(TimeUnit::Microseconds, tz)),
            "datetime[us, Europe/Paris]"
        );
        assert_eq!(type_text(&DataType::Decimal(10, 4)), "decimal(10,4)");
    }

    #[test]
    fn sizes_read_in_human_units_and_exactly() {
        assert_eq!(size_text(73), "73 bytes");
        assert_eq!(size_text(1), "1 byte");
        assert_eq!(size_text(4 << 20), "4.0 MB (4,194,304 bytes)");
    }
}

//! Drilling into a nested value from the row inspector: a struct's fields, a
//! list's items, or the objects and arrays of JSON stored as text.
//!
//! A level holds where it is, never a copy of what is there: a column's value is
//! a one-row slice of the buffer's series, and a JSON value is a path into a
//! document parsed once. Only the items on screen are ever turned into rows, so a
//! list of a million items costs as much to show as a list of ten.

use polars::prelude::*;
use serde_json::Value;
use std::sync::Arc;

/// Text up to this long is parsed as JSON on the key that asks; longer text is
/// parsed off the event thread.
pub const JSON_INLINE_BYTES: usize = 64 * 1024;
/// Text longer than this is not parsed: its tree would take several times its size.
pub const JSON_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Keys whose widths size a column of names: an object's keys past this are fitted
/// to the width found, not measured.
const MEASURED: usize = 1000;

/// One place in a value: what a level of the drill shows, or one of its items.
#[derive(Debug, Clone)]
pub enum Node {
    /// One value of a column, as a one-row slice: drilling copies nothing.
    Native(Series),
    /// A value in a JSON document parsed from text: the steps from its root.
    Json { root: Arc<Value>, path: Vec<Step> },
}

/// One step into a JSON document. An object's step is its key, not its position:
/// serde_json's map has no lookup by position, and walking to the 500,000th key of
/// a large object for every item drawn, on every frame, is what a key avoids.
#[derive(Debug, Clone)]
pub enum Step {
    Key(Arc<str>),
    Index(usize),
}

/// What a node holds, as the drill sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    Struct,
    List,
    Object,
    Array,
    /// A scalar, text, bytes or a null: nothing to drill into.
    Leaf,
}

impl Shape {
    /// What the rule over a level's items calls them.
    pub fn items_title(self) -> &'static str {
        match self {
            Shape::Struct => "Fields",
            Shape::Object => "Keys",
            _ => "Items",
        }
    }
}

impl Node {
    /// The JSON value at this node's path.
    pub fn json(&self) -> Option<&Value> {
        let Node::Json { root, path } = self else {
            return None;
        };
        let mut at: &Value = root;
        for step in path {
            at = match (at, step) {
                (Value::Object(map), Step::Key(key)) => map.get(&**key)?,
                (Value::Array(items), Step::Index(i)) => items.get(*i)?,
                _ => return None,
            };
        }
        Some(at)
    }

    fn is_null(&self) -> bool {
        match self {
            Node::Native(s) => s.null_count() == s.len(),
            Node::Json { .. } => matches!(self.json(), None | Some(Value::Null)),
        }
    }

    pub fn shape(&self) -> Shape {
        if self.is_null() {
            return Shape::Leaf;
        }
        match self {
            Node::Native(s) => match s.dtype() {
                DataType::Struct(_) => Shape::Struct,
                DataType::List(_) | DataType::Array(..) => Shape::List,
                _ => Shape::Leaf,
            },
            Node::Json { .. } => match self.json() {
                Some(Value::Object(_)) => Shape::Object,
                Some(Value::Array(_)) => Shape::Array,
                _ => Shape::Leaf,
            },
        }
    }

    /// A list's items, as one series.
    fn items(&self) -> Option<Series> {
        let Node::Native(s) = self else {
            return None;
        };
        match s.get(0).ok()? {
            AnyValue::List(inner) | AnyValue::Array(inner, _) => Some(inner),
            _ => None,
        }
    }

    /// A struct's fields, each a one-row series named for its field.
    fn fields(&self) -> Option<Vec<Series>> {
        let Node::Native(s) = self else {
            return None;
        };
        Some(s.struct_().ok()?.fields_as_series())
    }

    /// How many fields, items or keys a level of this node lists.
    pub fn len(&self) -> usize {
        match self.shape() {
            Shape::Struct => self.fields().map_or(0, |f| f.len()),
            Shape::List => self.items().map_or(0, |s| s.len()),
            Shape::Object | Shape::Array => match self.json() {
                Some(Value::Object(map)) => map.len(),
                Some(Value::Array(items)) => items.len(),
                _ => 0,
            },
            Shape::Leaf => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn json_child(&self, step: Step) -> Node {
        let Node::Json { root, path } = self else {
            unreachable!("only a JSON node has JSON children")
        };
        let mut path = path.clone();
        path.push(step);
        Node::Json {
            root: Arc::clone(root),
            path,
        }
    }

    /// `count` items from `start`, each with its label: a field's or key's name, or
    /// `[i]`. Only these are made, whatever the length.
    pub fn children(&self, start: usize, count: usize) -> Vec<(String, Node)> {
        match self.shape() {
            Shape::Struct => self
                .fields()
                .unwrap_or_default()
                .into_iter()
                .skip(start)
                .take(count)
                .map(|s| (s.name().to_string(), Node::Native(s)))
                .collect(),
            Shape::List => {
                let Some(items) = self.items() else {
                    return Vec::new();
                };
                let end = items.len().min(start.saturating_add(count));
                (start..end)
                    .map(|i| (format!("[{i}]"), Node::Native(items.slice(i as i64, 1))))
                    .collect()
            }
            Shape::Object => match self.json() {
                Some(Value::Object(map)) => page(map.keys(), start, count)
                    .into_iter()
                    .map(|key| (key.clone(), self.json_child(Step::Key(key.as_str().into()))))
                    .collect(),
                _ => Vec::new(),
            },
            Shape::Array => {
                let end = self.len().min(start.saturating_add(count));
                (start..end)
                    .map(|i| (format!("[{i}]"), self.json_child(Step::Index(i))))
                    .collect()
            }
            Shape::Leaf => Vec::new(),
        }
    }

    pub fn child(&self, i: usize) -> Option<(String, Node)> {
        self.children(i, 1).pop()
    }

    /// The widest label a level of this node lists, without walking a long list:
    /// a list's last index is its widest, and only the first keys are measured.
    pub fn label_width(&self) -> usize {
        match self.shape() {
            Shape::List | Shape::Array => format!("[{}]", self.len().saturating_sub(1)).len(),
            Shape::Struct => self
                .fields()
                .unwrap_or_default()
                .iter()
                .map(|s| crate::glyphs::cell_width(s.name()))
                .max()
                .unwrap_or(0),
            Shape::Object => match self.json() {
                Some(Value::Object(map)) => map
                    .keys()
                    .take(MEASURED)
                    .map(|k| crate::glyphs::cell_width(k))
                    .max()
                    .unwrap_or(0),
                _ => 0,
            },
            Shape::Leaf => 0,
        }
    }

    /// The text of a text value, for `f`; None for any other value.
    pub fn with_text<R>(&self, f: impl FnOnce(&str) -> R) -> Option<R> {
        match self {
            Node::Native(s) => match s.get(0).ok()? {
                AnyValue::String(text) => Some(f(text)),
                AnyValue::StringOwned(text) => Some(f(text.as_str())),
                _ => None,
            },
            Node::Json { .. } => match self.json()? {
                Value::String(text) => Some(f(text)),
                _ => None,
            },
        }
    }

    /// Whether Enter opens this node as a level: a struct, a list, an object or an
    /// array, or text that reads as a JSON object or array.
    pub fn opens(&self) -> bool {
        self.shape() != Shape::Leaf || self.with_text(opens_as_json).unwrap_or(false)
    }

    /// The type as the item list names it.
    pub fn type_label(&self) -> String {
        match self {
            Node::Native(s) => crate::table::dtype_label(s.dtype()),
            Node::Json { .. } => json_kind(self.json().unwrap_or(&Value::Null)).to_string(),
        }
    }

    /// The type the item list colors this node's name by.
    pub fn color_dtype(&self) -> DataType {
        match self {
            Node::Native(s) => s.dtype().clone(),
            Node::Json { .. } => json_dtype(self.json().unwrap_or(&Value::Null)),
        }
    }

    /// The columns a list of structs, or an array of objects, shows its items in,
    /// with the type that colors each name: the struct's fields, or the first
    /// object's keys. None for any other level.
    pub fn table_columns(&self) -> Option<Vec<(String, DataType)>> {
        match self.shape() {
            Shape::List => match self.items()?.dtype() {
                DataType::Struct(fields) if !fields.is_empty() => Some(
                    fields
                        .iter()
                        .map(|f| (f.name().to_string(), f.dtype().clone()))
                        .collect(),
                ),
                _ => None,
            },
            Shape::Array => match self.json()? {
                Value::Array(items) => match items.first()? {
                    Value::Object(first) if !first.is_empty() => Some(
                        first
                            .iter()
                            .map(|(k, v)| (k.clone(), json_dtype(v)))
                            .collect(),
                    ),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    /// One item's value under `column` in a level shown as a table; None when the
    /// item has no such field or key.
    pub fn cell(&self, column: &str) -> Option<Node> {
        match self.shape() {
            // One field made, not all of them for each cell of a row.
            Shape::Struct => match self {
                Node::Native(s) => s
                    .struct_()
                    .ok()?
                    .field_by_name(column)
                    .ok()
                    .map(Node::Native),
                Node::Json { .. } => None,
            },
            Shape::Object => match self.json()? {
                Value::Object(map) if map.contains_key(column) => {
                    Some(self.json_child(Step::Key(column.into())))
                }
                _ => None,
            },
            _ => None,
        }
    }
}

/// Items `start..start + count` of `items`, walked to from the nearer end: an
/// object's keys can only be stepped through, and `End` on a large one is then as
/// quick as `Home`.
fn page<T>(
    items: impl DoubleEndedIterator<Item = T> + ExactSizeIterator,
    start: usize,
    count: usize,
) -> Vec<T> {
    let len = items.len();
    let end = len.min(start.saturating_add(count));
    if start >= end {
        return Vec::new();
    }
    if start <= len - end {
        return items.skip(start).take(end - start).collect();
    }
    let mut back: Vec<T> = items.rev().skip(len - end).take(end - start).collect();
    back.reverse();
    back
}

/// The type a JSON value's name is colored by, as a column of that type is.
fn json_dtype(value: &Value) -> DataType {
    match value {
        Value::String(_) => DataType::String,
        Value::Bool(_) => DataType::Boolean,
        Value::Number(n) if n.is_f64() => DataType::Float64,
        Value::Number(_) => DataType::Int64,
        Value::Array(_) => DataType::List(Box::new(DataType::Null)),
        Value::Object(_) => DataType::Struct(Vec::new()),
        Value::Null => DataType::Null,
    }
}

/// A JSON value's kind, as the item list names it.
pub fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "str",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Whether `text` reads as a JSON object or array: it starts and ends with the
/// brackets of one. Whether it parses is learned only by parsing it.
pub fn looks_like_json(text: &str) -> bool {
    let t = text.trim();
    matches!(
        (t.as_bytes().first(), t.as_bytes().last()),
        (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']'))
    )
}

/// Whether Enter offers to open `text` as JSON: it reads as an object or array and
/// is not over [`JSON_MAX_BYTES`], past which Enter shows more of it instead.
pub fn opens_as_json(text: &str) -> bool {
    text.len() <= JSON_MAX_BYTES && looks_like_json(text)
}

/// Parse `text` as JSON, refusing text over [`JSON_MAX_BYTES`]. serde_json stops at
/// 128 levels of nesting, so a deep document is an error, not a stack overflow.
pub fn parse_json(text: &str) -> Result<Value, String> {
    if text.len() > JSON_MAX_BYTES {
        return Err(format!(
            "{} MiB of text is over the {} MiB that opens as JSON",
            text.len().div_ceil(1024 * 1024),
            JSON_MAX_BYTES / (1024 * 1024)
        ));
    }
    serde_json::from_str(text).map_err(|e| format!("not JSON: {e}"))
}

/// Writes up to `cap` bytes, then fails, so serializing a huge value stops there.
struct Capped {
    out: Vec<u8>,
    cap: usize,
    cut: bool,
}

impl std::io::Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let room = self.cap.saturating_sub(self.out.len());
        if bytes.len() > room {
            self.out.extend_from_slice(&bytes[..room]);
            self.cut = true;
            return Err(std::io::Error::other("cap reached"));
        }
        self.out.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// JSON on one line with a space after each comma and colon, as datui writes a
/// list or struct on one line.
struct Spaced;

impl serde_json::ser::Formatter for Spaced {
    fn begin_array_value<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_key<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    fn begin_object_value<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_all(b": ")
    }
}

/// `value` as JSON text, indented or on one line, stopping at `cap` bytes. True
/// when it was cut there.
pub fn json_text(value: &Value, pretty: bool, cap: usize) -> (String, bool) {
    use serde::Serialize;
    let mut w = Capped {
        out: Vec::new(),
        cap,
        cut: false,
    };
    // An error here is only the cap: serializing a parsed value cannot fail otherwise.
    let _ = if pretty {
        serde_json::to_writer_pretty(&mut w, value)
    } else {
        value.serialize(&mut serde_json::Serializer::with_formatter(&mut w, Spaced))
    };
    let text = match String::from_utf8(w.out) {
        Ok(text) => text,
        // Cut inside a character: keep what is whole.
        Err(e) => {
            let valid = e.utf8_error().valid_up_to();
            let mut bytes = e.into_bytes();
            bytes.truncate(valid);
            String::from_utf8(bytes).unwrap_or_default()
        }
    };
    (text, w.cut)
}

/// A JSON value as copy text: text as itself, a null as empty, anything else as
/// JSON, indented when it is an object or array. None when it is over `cap` bytes.
pub fn json_copy_text(value: &Value, cap: usize) -> Option<String> {
    match value {
        Value::String(s) => (s.len() <= cap).then(|| s.clone()),
        Value::Null => Some(String::new()),
        v => {
            let (text, cut) = json_text(v, true, cap);
            (!cut).then_some(text)
        }
    }
}

/// One level of a drill: the value it lists, and the item focused in it.
#[derive(Debug, Clone)]
pub struct Level {
    /// Its step in the breadcrumb: a field's or key's name, or `[i]`.
    pub label: String,
    pub node: Node,
    pub selected: usize,
}

impl Level {
    /// The focused item, and its label.
    pub fn focused(&self) -> Option<(String, Node)> {
        self.node.child(self.selected)
    }
}

/// The levels opened under one field of one row, outermost first.
#[derive(Debug, Clone)]
pub struct Drill {
    pub frame: u64,
    pub row: usize,
    pub levels: Vec<Level>,
}

impl Drill {
    pub fn level(&self) -> &Level {
        self.levels.last().expect("a drill has a level")
    }

    pub fn level_mut(&mut self) -> &mut Level {
        self.levels.last_mut().expect("a drill has a level")
    }
}

/// Text being parsed as JSON off the event thread, and where its level opens.
#[derive(Debug, Clone)]
pub struct JsonWait {
    pub token: u64,
    pub frame: u64,
    pub row: usize,
    pub label: String,
    /// The text's place, as [`path_key`] names it.
    pub path: String,
}

/// A place in a row, for telling one item from another: the field, then each step
/// opened and the item. Unit separators cannot be typed into a name, so two places
/// never meet.
pub fn path_key<'a>(steps: impl IntoIterator<Item = &'a str>) -> String {
    steps.into_iter().collect::<Vec<_>>().join("\u{1f}")
}

impl Drill {
    /// The place of the item `label` in the level shown.
    pub fn item_key(&self, label: &str) -> String {
        path_key(
            self.levels
                .iter()
                .map(|l| l.label.as_str())
                .chain(std::iter::once(label)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn struct_row() -> Series {
        let df = df!(
            "id" => [7i64],
            "name" => ["ann"],
        )
        .unwrap();
        df.into_struct("customer".into()).into_series()
    }

    #[test]
    fn a_struct_lists_its_fields_as_one_row_slices() {
        let node = Node::Native(struct_row());
        assert_eq!(node.shape(), Shape::Struct);
        assert_eq!(node.len(), 2);
        let kids = node.children(0, 10);
        let labels: Vec<_> = kids.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(labels, ["id", "name"]);
        let (_, name) = &kids[1];
        assert_eq!(name.shape(), Shape::Leaf);
        assert_eq!(name.with_text(str::to_string).as_deref(), Some("ann"));
        assert_eq!(node.cell("id").unwrap().type_label(), "i64");
        assert!(node.cell("nope").is_none());
    }

    /// A list of a million items lists a page of them: only those are made.
    #[test]
    fn a_long_list_makes_only_the_page_asked_for() {
        let inner = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
        let list = Series::new("l".into(), [inner]);
        let node = Node::Native(list);
        assert_eq!(node.shape(), Shape::List);
        assert_eq!(node.len(), 1_000_000);
        let page = node.children(999_998, 20);
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].0, "[999998]");
        assert_eq!(page[1].1.type_label(), "i64");
        assert_eq!(node.label_width(), "[999999]".len());
    }

    #[test]
    fn a_null_struct_or_list_is_a_leaf() {
        let list = Series::new_null("l".into(), 1).cast(&DataType::List(Box::new(DataType::Int64)));
        let node = Node::Native(list.unwrap());
        assert_eq!(node.shape(), Shape::Leaf);
        assert!(!node.opens());
    }

    #[test]
    fn json_objects_keep_their_order_and_arrays_their_items() {
        let root = Arc::new(parse_json(r#"{"z": 1, "a": [true, null, {"k": "v"}]}"#).unwrap());
        let node = Node::Json {
            root,
            path: Vec::new(),
        };
        assert_eq!(node.shape(), Shape::Object);
        let kids = node.children(0, 9);
        assert_eq!(kids[0].0, "z", "the document's order, not sorted");
        assert_eq!(kids[0].1.type_label(), "number");
        let (_, a) = &kids[1];
        assert_eq!(a.shape(), Shape::Array);
        assert_eq!(a.len(), 3);
        let (label, obj) = a.child(2).unwrap();
        assert_eq!(label, "[2]");
        assert_eq!(obj.shape(), Shape::Object);
        assert_eq!(obj.cell("k").unwrap().json(), Some(&Value::from("v")));
        assert_eq!(a.child(1).unwrap().1.shape(), Shape::Leaf);
    }

    /// A step into an object is its key: the items near the end of a large object
    /// are found without walking to them, in the document's order.
    #[test]
    fn a_large_objects_last_keys_resolve_by_key() {
        let n = 200_000;
        let text = format!(
            "{{{}}}",
            (0..n)
                .map(|i| format!("\"k{i}\": {i}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let node = Node::Json {
            root: Arc::new(parse_json(&text).unwrap()),
            path: Vec::new(),
        };
        let tail = node.children(n - 3, 10);
        let labels: Vec<_> = tail.iter().map(|(l, _)| l.as_str()).collect();
        assert_eq!(labels, ["k199997", "k199998", "k199999"]);
        assert_eq!(tail[2].1.json(), Some(&Value::from(n - 1)));
        let Node::Json { path, .. } = &tail[2].1 else {
            panic!("a JSON item");
        };
        assert!(matches!(&path[..], [Step::Key(k)] if &**k == "k199999"));
        let head = node.children(1, 2);
        assert_eq!(head[1].0, "k2");
        assert_eq!(page(0..10, 7, 5), [7, 8, 9]);
        assert_eq!(page(0..10, 2, 3), [2, 3, 4]);
        assert!(page(0..10, 10, 3).is_empty());
    }

    #[test]
    fn text_opens_only_when_it_reads_as_an_object_or_array() {
        assert!(looks_like_json(" {\"a\": 1}\n"));
        assert!(looks_like_json("[1, 2]"));
        assert!(!looks_like_json("{not closed"));
        assert!(!looks_like_json("\"text\""));
        assert!(!looks_like_json("42"));
        let s = Series::new("s".into(), ["[1, 2]"]);
        assert!(Node::Native(s).opens());
        let s = Series::new("s".into(), ["plain"]);
        assert!(!Node::Native(s).opens());
    }

    /// Depth and size are bounded: a deep document is an error, a huge text is
    /// refused before it is parsed.
    #[test]
    fn parsing_is_bounded() {
        let deep = format!("{}{}", "[".repeat(10_000), "]".repeat(10_000));
        assert!(parse_json(&deep).is_err());
        let huge = format!("[{}0]", "0,".repeat(JSON_MAX_BYTES / 2));
        let err = parse_json(&huge).unwrap_err();
        assert!(err.contains("MiB"), "{err}");
        assert!(parse_json("{oops}").unwrap_err().starts_with("not JSON"));
    }

    #[test]
    fn json_text_stops_at_its_cap() {
        let value: Value = serde_json::from_str(&format!("[{}1]", "1,".repeat(10_000))).unwrap();
        let (text, cut) = json_text(&value, true, 100);
        assert!(cut);
        assert_eq!(text.len(), 100);
        let (whole, cut) = json_text(&Value::from("é"), false, 100);
        assert!(!cut);
        assert_eq!(whole, "\"é\"");
        // Cut inside a character: the part that is whole.
        let (text, cut) = json_text(&Value::from("ééé"), false, 4);
        assert!(cut);
        assert_eq!(text, "\"é");
        assert_eq!(json_copy_text(&value, 100), None);
        assert_eq!(json_copy_text(&Value::from("raw"), 100).unwrap(), "raw");
    }

    #[test]
    fn a_list_of_structs_and_an_array_of_objects_are_tables() {
        let items = df!("sku" => ["A1", "B7"], "qty" => [2i64, 1])
            .unwrap()
            .into_struct("".into())
            .into_series();
        let list = Series::new("items".into(), [items]);
        let node = Node::Native(list);
        assert_eq!(
            node.table_columns(),
            Some(vec![
                ("sku".to_string(), DataType::String),
                ("qty".to_string(), DataType::Int64)
            ])
        );
        let (_, first) = node.child(0).unwrap();
        assert_eq!(first.shape(), Shape::Struct);
        assert_eq!(
            first
                .cell("sku")
                .unwrap()
                .with_text(str::to_string)
                .as_deref(),
            Some("A1")
        );

        let root = Arc::new(parse_json(r#"[{"a": 1, "b": 2}, {"b": 3}]"#).unwrap());
        let arr = Node::Json {
            root,
            path: Vec::new(),
        };
        assert_eq!(
            arr.table_columns(),
            Some(vec![
                ("a".to_string(), DataType::Int64),
                ("b".to_string(), DataType::Int64)
            ])
        );
        assert!(arr.child(1).unwrap().1.cell("a").is_none());
    }
}

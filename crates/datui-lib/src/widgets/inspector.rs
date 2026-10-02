//! The row inspector: every field of the table's selected row, and the focused
//! field's whole value. A takeover over the table — one Surface titled with the
//! row, the fields as a list (name, type, the table's preview), and under a
//! section rule the value itself: exact, wrapped, scrolled, never rounded.

use crate::copy_modal::thousands;
use crate::exact;
use crate::inspector_drill::{Node, Shape, json_text, opens_as_json};
use crate::inspector_modal::{BodyKey, CHUNK_BYTES, FieldRead, InspectorModal};
use crate::render::context::RenderContext;
use crate::widgets::datatable::{DataTableState, InspectField, InspectRow, NullKind, dtype_label};
use crate::widgets::ui::{HintBar, SectionRule, Surface};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use serde_json::Value as JsonValue;

/// The longest line the value pane wraps to: a reading surface keeps its
/// measure on a wide terminal.
const MEASURE: usize = 100;
/// Cells between a field's name, type and preview.
const GAP: usize = 2;
/// The fewest lines the value pane keeps when the field list takes the rest.
const VALUE_MIN: usize = 3;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Plain,
    Dim,
    Warn,
}

/// What a cut value holds past the lines formatted so far, so the pane's last
/// line can count the whole value and not only the part formatted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Rest {
    /// The value is formatted whole.
    #[default]
    None,
    /// This many more lines, known without formatting them: a hex dump.
    Lines(usize),
    /// This much more of the value, in its unit: `2,031,616 chars`.
    Units(String),
    /// More, of a length not known without formatting it: a nested value.
    Unknown,
}

/// The value pane: its lines, the facts for its rule, and whether Enter has
/// more to show.
#[derive(Debug, Clone, Default)]
pub struct Body {
    pub lines: Vec<(String, Tone)>,
    pub facts: String,
    pub more: bool,
    /// Past the last line, when the value was cut. The last line then says so.
    pub rest: Rest,
    /// Whether `e` changes what the pane shows: text and bytes only.
    pub escapable: bool,
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

/// Split `line` into rows of at most `width` cells, at grapheme boundaries.
fn wrap_into(line: &str, width: usize, tone: Tone, out: &mut Vec<(String, Tone)>) {
    use ratatui::buffer::CellWidth;
    let width = width.max(1);
    if line.is_empty() {
        out.push((String::new(), tone));
        return;
    }
    if line.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        for chunk in line.as_bytes().chunks(width) {
            out.push((String::from_utf8_lossy(chunk).into_owned(), tone));
        }
        return;
    }
    let span = Span::raw(line);
    let mut row = String::new();
    let mut used = 0usize;
    for g in span.styled_graphemes(Style::default()) {
        let w = usize::from(g.symbol.cell_width());
        if used + w > width && !row.is_empty() {
            out.push((std::mem::take(&mut row), tone));
            used = 0;
        }
        row.push_str(g.symbol);
        used += w;
    }
    if !row.is_empty() {
        out.push((row, tone));
    }
}

/// A sentence of the pane's own, wrapped at spaces; a word wider than the pane
/// is split.
fn wrap_words(text: &str, width: usize, tone: Tone, out: &mut Vec<(String, Tone)>) {
    let mut row = String::new();
    for word in text.split(' ') {
        let sep = usize::from(!row.is_empty());
        if !row.is_empty()
            && crate::glyphs::cell_width(&row) + sep + crate::glyphs::cell_width(word) > width
        {
            wrap_into(&std::mem::take(&mut row), width, tone, out);
        }
        if !row.is_empty() {
            row.push(' ');
        }
        row.push_str(word);
    }
    wrap_into(&row, width, tone, out);
}

/// Text as it reads raw: a line per line break, tabs to the next stop of
/// four, and any other control character or direction control as the control
/// mark, as in a table cell.
fn raw_lines(text: &str, width: usize, out: &mut Vec<(String, Tone)>) {
    let g = crate::glyphs::get();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let mut expanded = String::with_capacity(line.len());
        let mut column = 0usize;
        for c in line.chars() {
            match c {
                '\t' => {
                    let stop = 4 - column % 4;
                    expanded.extend(std::iter::repeat_n(' ', stop));
                    column += stop;
                }
                c if exact::marked(c) => {
                    expanded.push_str(g.control_mark);
                    column += 1;
                }
                c => {
                    expanded.push(c);
                    column += 1;
                }
            }
        }
        wrap_into(&expanded, width, Tone::Plain, out);
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{} {}", thousands(n), if n == 1 { one } else { many })
}

/// `… 1,234 more chars`: what a cut-off pane has past its last line.
fn more_line(n: usize, one: &str, many: &str) -> String {
    let g = crate::glyphs::get();
    format!(
        "{} {} more {}",
        g.ellipsis,
        thousands(n),
        if n == 1 { one } else { many }
    )
}

/// The value pane for `field`. Shows at most `chunks` × [`CHUNK_BYTES`] of a
/// long value, wrapped to `width`; `table` is the table's preview of it, said
/// beside the exact value when the two differ.
pub fn body(
    field: &InspectField,
    shown: &Shown,
    escaped: bool,
    chunks: usize,
    width: usize,
    table: Option<&str>,
) -> Body {
    let g = crate::glyphs::get();
    let budget = CHUNK_BYTES.saturating_mul(chunks.max(1));
    let kind = type_text(&field.dtype);
    let mut lines = Vec::new();
    let mut facts = vec![kind.clone()];
    let mut more = false;
    let mut rest = Rest::None;
    match shown {
        Shown::Unread => {
            facts.push("not read".to_string());
            wrap_words(
                "Not read with the table's rows; Enter reads this row's hidden and binary fields",
                width,
                Tone::Dim,
                &mut lines,
            );
            more = true;
        }
        Shown::Reading => wrap_words("Reading...", width, Tone::Dim, &mut lines),
        Shown::Failed(message) => wrap_words(message, width, Tone::Warn, &mut lines),
        Shown::Null(kind) => {
            let (glyph, word, why) = match kind {
                NullKind::Null => (g.null, "null", None),
                NullKind::Absent => (
                    g.absent,
                    "absent",
                    Some("this row's file has no such column"),
                ),
                NullKind::Conflict => (
                    g.conflict,
                    "conflicting",
                    Some("this row's file holds the column in another type, so it was not read"),
                ),
            };
            facts.push(word.to_string());
            let text = match why {
                Some(why) => format!("{glyph} {word}: {why}"),
                None => format!("{glyph} {word}"),
            };
            wrap_words(&text, width, Tone::Dim, &mut lines);
        }
        Shown::Value(value) => match value {
            AnyValue::String(_)
            | AnyValue::StringOwned(_)
            | AnyValue::Categorical(..)
            | AnyValue::CategoricalOwned(..)
            | AnyValue::Enum(..)
            | AnyValue::EnumOwned(..) => {
                let owned;
                let s: &str = match value {
                    AnyValue::String(s) => s,
                    AnyValue::StringOwned(s) => s.as_str(),
                    v => {
                        owned = exact::value_text(v);
                        &owned
                    }
                };
                let f = exact::text_facts(s);
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
                let shown_text = exact::prefix(s, budget);
                let cut = shown_text.len() < s.len();
                if escaped {
                    let mut literal = exact::escaped(shown_text);
                    if cut {
                        literal.pop();
                    }
                    wrap_into(&literal, width, Tone::Plain, &mut lines);
                } else if s.is_empty() {
                    lines.push(("empty string".to_string(), Tone::Dim));
                } else {
                    raw_lines(shown_text, width, &mut lines);
                }
                if cut {
                    let left = s[shown_text.len()..].chars().count();
                    lines.push((more_line(left, "char", "chars"), Tone::Dim));
                    rest = Rest::Units(plural(left, "char", "chars"));
                    more = true;
                }
            }
            AnyValue::Binary(_) | AnyValue::BinaryOwned(_) => {
                let bytes: &[u8] = match value {
                    AnyValue::Binary(b) => b,
                    AnyValue::BinaryOwned(b) => b,
                    _ => unreachable!(),
                };
                facts.push(plural(bytes.len(), "byte", "bytes"));
                if bytes.is_empty() {
                    facts.push("empty".to_string());
                }
                let n = bytes.len().min(budget / 4);
                if escaped {
                    let mut literal = exact::escaped_bytes(&bytes[..n]);
                    if n < bytes.len() {
                        literal.pop();
                    }
                    wrap_into(&literal, width, Tone::Plain, &mut lines);
                    if n < bytes.len() {
                        rest = Rest::Units(plural(bytes.len() - n, "byte", "bytes"));
                    }
                } else if bytes.is_empty() {
                    lines.push(("empty binary".to_string(), Tone::Dim));
                } else {
                    let per_line = match width {
                        w if w >= 76 => 16,
                        w if w >= 42 => 8,
                        _ => 4,
                    };
                    let dumps = exact::hex_lines(&bytes[..n], per_line);
                    let first = lines.len();
                    let count = dumps.len();
                    for line in dumps {
                        wrap_into(&line, width, Tone::Plain, &mut lines);
                    }
                    // Every dump line is padded to one width, so each wraps to as many
                    // rows, and the lines past the cut are counted, not formatted.
                    let rows_each = (lines.len() - first) / count.max(1);
                    if n < bytes.len() {
                        rest = Rest::Lines((bytes.len() - n).div_ceil(per_line) * rows_each);
                    }
                }
                if n < bytes.len() {
                    lines.push((more_line(bytes.len() - n, "byte", "bytes"), Tone::Dim));
                    more = true;
                }
            }
            v if exact::is_nested_value(v) => {
                if let Some(n) = exact::nested_len(v) {
                    facts.push(match v {
                        AnyValue::Struct(..) | AnyValue::StructOwned(_) => {
                            plural(n, "field", "fields")
                        }
                        _ => plural(n, "item", "items"),
                    });
                }
                let pretty = exact::nested_pretty(v, budget);
                for line in pretty.text.split('\n') {
                    wrap_into(line, width, Tone::Plain, &mut lines);
                }
                if pretty.cut {
                    lines.push((format!("{} more", g.ellipsis), Tone::Dim));
                    rest = Rest::Unknown;
                    more = true;
                }
            }
            v => {
                let text = exact::value_text(v);
                wrap_into(&text, width, Tone::Plain, &mut lines);
                if let Some(table) = table.filter(|t| *t != text) {
                    lines.push((String::new(), Tone::Plain));
                    wrap_into(
                        &format!("In the table: {table}"),
                        width,
                        Tone::Dim,
                        &mut lines,
                    );
                }
            }
        },
    }
    // Only text and bytes have an escaped form to be in.
    let has_text = escapable(shown);
    if escaped && has_text {
        facts.push("escaped".to_string());
    }
    Body {
        lines,
        facts: facts.join(&format!(" {} ", g.middot)),
        more,
        rest,
        escapable: has_text,
    }
}

/// Whether `e` changes how `shown` reads: only text and bytes have an escaped
/// form.
pub fn escapable(shown: &Shown) -> bool {
    matches!(
        shown,
        Shown::Value(
            AnyValue::String(_)
                | AnyValue::StringOwned(_)
                | AnyValue::Categorical(..)
                | AnyValue::CategoricalOwned(..)
                | AnyValue::Enum(..)
                | AnyValue::EnumOwned(..)
                | AnyValue::Binary(_)
                | AnyValue::BinaryOwned(_)
        )
    )
}

/// The pane's last line when `hidden` lines of `body` do not fit: every line
/// left, counted over the whole value and not only the part formatted.
fn overflow_line(body: &Body, hidden: usize) -> String {
    let g = crate::glyphs::get();
    // A cut value's own last line says what was cut; it is not a line of the value.
    let content = match body.rest {
        Rest::None => hidden,
        _ => hidden.saturating_sub(1),
    };
    let cut_line = || {
        body.lines
            .last()
            .map(|(t, _)| t.clone())
            .unwrap_or_default()
    };
    match &body.rest {
        Rest::None => more_line(hidden, "line", "lines"),
        Rest::Lines(n) => more_line(content + n, "line", "lines"),
        _ if content == 0 => cut_line(),
        Rest::Units(units) => format!("{}, then {units}", more_line(content, "line", "lines")),
        Rest::Unknown => format!("{} {}+ more lines", g.ellipsis, thousands(content)),
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

/// Rows of the field list for `fields` visible fields in `avail` rows shared
/// with the value: every field when they all fit beside a short value, else
/// half. Depends on the row's fields, never on the focused one, so moving
/// between fields moves nothing.
pub fn list_rows(fields: usize, avail: usize) -> usize {
    if fields + VALUE_MIN <= avail {
        return fields.max(1);
    }
    fields
        .max(1)
        .min((avail / 2).max(3))
        .min(avail.saturating_sub(2))
        .max(1)
}

/// The table's one-line preview of a value, formatted as the table formats it,
/// and only as much of it as `room` cells can show.
fn preview(field: &InspectField, value: &AnyValue, room: usize, ctx: &RenderContext) -> String {
    let g = crate::glyphs::get();
    let budget = room.saturating_mul(4).max(16);
    match value {
        AnyValue::String(s) => exact::preview(exact::prefix(s, budget), g).into_owned(),
        AnyValue::StringOwned(s) => exact::preview(exact::prefix(s, budget), g).into_owned(),
        AnyValue::Binary(b) => plural(b.len(), "byte", "bytes"),
        AnyValue::BinaryOwned(b) => plural(b.len(), "byte", "bytes"),
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

/// The inspector's title: the row, of how many, and the group it is in inside
/// a drill-down, as the breadcrumb the takeover covers says it.
fn title(display_row: usize, state: &DataTableState) -> String {
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
    title
}

/// Draw the inspector over `area` for the table's selected row.
pub fn render(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut InspectorModal,
    state: &DataTableState,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let row = state.inspect_row();
    if let Some(row) = &row {
        modal.row_shown(row.frame, row.row);
    }
    if let (Some(row), Some(_)) = (&row, &modal.drill) {
        let title = title(row.display_row, state);
        render_drill(area, buf, modal, &title, ctx);
        return;
    }
    let content_w = area.width.saturating_sub(4) as usize;
    let measure = content_w.min(MEASURE);

    // The focused field's pane, from the cache while nothing it shows changed.
    let focused = modal.focused().cloned();
    // What Enter does here, for the footer: read, retry a failed read, or show more.
    let mut enter = None;
    let body = match (&row, &focused) {
        (Some(row), Some(field)) => {
            let value = shown(field, row, modal.read.as_ref(), state);
            enter = match &value {
                Shown::Unread => Some("Read"),
                Shown::Failed(_) => Some("Retry"),
                Shown::Value(v)
                    if value_opens(v) && !modal.known_not_json(row.frame, row.row, &field.name) =>
                {
                    Some("Open")
                }
                _ => None,
            };
            let key = BodyKey {
                frame: row.frame,
                row: row.row,
                field: field.name.clone(),
                escaped: modal.escaped,
                chunks: modal.chunks,
                width: measure as u16,
                state: value.kind(),
            };
            match &modal.body {
                Some((cached, body)) if *cached == key => body.clone(),
                _ => {
                    // Text, bytes and nested values are shown whole below; only a
                    // scalar's preview can say something the exact text does not.
                    let table = match &value {
                        Shown::Value(v) if is_scalar(v) => Some(preview(field, v, 64, ctx)),
                        _ => None,
                    };
                    let built = body(
                        field,
                        &value,
                        modal.escaped,
                        modal.chunks,
                        measure,
                        table.as_deref(),
                    );
                    modal.body = Some((key, built.clone()));
                    built
                }
            }
        }
        (None, _) => Body {
            lines: vec![("Reading the row...".to_string(), Tone::Dim)],
            ..Body::default()
        },
        (Some(_), None) => Body {
            lines: vec![("No field matches".to_string(), Tone::Dim)],
            ..Body::default()
        },
    };

    // The layout, worked out before the footer: the footer offers the scroll
    // keys only when the value overflows its pane.
    let visible = modal.visible();
    // The Surface's content: inside the border, less the footer row.
    let content_h = area.height.saturating_sub(3) as usize;
    let avail = content_h.saturating_sub(2);
    let list_h = list_rows(visible.len(), avail);
    let body_h = content_h.saturating_sub(list_h + 2);
    let overflows = body.lines.len() > body_h;
    let list_overflows = visible.len() > list_h;

    let escape_label = if modal.escaped { "Raw" } else { "Escaped" };
    let footer = if modal.finding {
        HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Done", 3)
            .hint_weighted("type", "Find", 1)
            .hint_weighted("Esc", "Clear", 4)
    } else {
        // Only keys that act here; the weights say which yield first on a narrow
        // footer, the way out last.
        let mut bar = HintBar::from_ctx(ctx);
        if let Some(label) = enter.or(body.more.then_some("More")) {
            bar = bar.hint_weighted("Enter", label, 9);
        }
        // A field not read yet has nothing to copy: `y` only says so.
        if !matches!(enter, Some("Read" | "Retry")) && row.is_some() && focused.is_some() {
            bar = bar.hint_weighted("y", "Copy", 8);
        }
        if visible.len() > 1 {
            bar = bar.hint_weighted(g.updown, "Field", 7);
        }
        bar = bar
            .hint_weighted(g.updown_lr, "Row", 6)
            .hint_weighted("/", "Find", 5);
        if overflows {
            bar = bar.hint_weighted("PgUp/PgDn", "Scroll", 4);
        }
        if body.escapable {
            bar = bar.hint_weighted("e", escape_label, 3);
        }
        if list_overflows {
            bar = bar.hint_weighted("Home/End", "First/Last", 2);
        }
        let esc = if modal.picker.filter.is_empty() {
            "Close"
        } else {
            "Clear"
        };
        bar.hint_weighted("Esc", esc, 10)
    };
    let title = match &row {
        Some(row) => title(row.display_row, state),
        None => "Row".to_string(),
    };
    let title = crate::glyphs::fit_cells(&title, area.width.saturating_sub(4) as usize, g.ellipsis);
    let content = Surface::new(&title).footer(&footer).render(area, buf, ctx);
    if content.height < 4 || content.width < 12 {
        return;
    }

    let line = |y: u16| Rect {
        y,
        height: 1,
        ..content
    };

    // The find line stands where the list's rule stands, so finding moves nothing.
    if modal.finding || !modal.picker.filter.is_empty() {
        let label_style = if modal.finding {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.dimmed)
        };
        let mut spans = vec![
            Span::styled("Find: ", label_style),
            Span::styled(
                modal.picker.filter.clone(),
                Style::default().fg(ctx.text_primary),
            ),
        ];
        if modal.finding {
            spans.push(Span::styled(g.cursor, Style::default().fg(ctx.accent)));
        }
        spans.push(Span::styled(
            format!(
                "   {} of {}",
                thousands(visible.len()),
                thousands(modal.fields.len())
            ),
            Style::default().fg(ctx.dimmed),
        ));
        Paragraph::new(Line::from(spans)).render(line(content.y), buf);
    } else {
        let count = thousands(modal.fields.len());
        SectionRule {
            title: "Fields",
            chip: Some(&count),
            focused: false,
        }
        .render(line(content.y), buf, ctx);
    }

    // The fields: name, type, the table's preview.
    let list_y = content.y + 1;
    let width = content.width as usize;
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
        .clamp(4, (width / 3).clamp(4, 28));
    let type_w = modal
        .fields
        .iter()
        .map(|f| crate::glyphs::cell_width(&dtype_label(&f.dtype)))
        .max()
        .unwrap_or(0)
        .min(14);
    let preview_w = width.saturating_sub(1 + name_w + GAP + type_w + GAP);
    let selected = modal.picker.visible_selection();
    let offset = selected.saturating_sub(list_h.saturating_sub(1));
    let below = visible.len().saturating_sub(offset + list_h);
    if visible.is_empty() {
        Paragraph::new("No field matches")
            .style(Style::default().fg(ctx.dimmed))
            .render(line(list_y), buf);
    }
    for (i, &index) in visible.iter().enumerate().skip(offset).take(list_h) {
        let y = list_y + (i - offset) as u16;
        let is_selected = i == selected;
        if i + 1 == offset + list_h && below > 0 && !is_selected {
            Paragraph::new(format!("  {} {} more", g.ellipsis, below + 1))
                .style(Style::default().fg(ctx.dimmed))
                .render(line(y), buf);
            break;
        }
        let field = &modal.fields[index];
        let marker = match (is_selected, modal.finding) {
            (true, false) => g.rail,
            (true, true) => g.middot,
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
        let (value_text, value_style) = match &row {
            Some(row) => match shown(field, row, modal.read.as_ref(), state) {
                Shown::Value(v) => match empty_preview(&v) {
                    Some(empty) => (empty, Style::default().fg(ctx.dimmed)),
                    None => (
                        preview(field, &v, preview_w, ctx),
                        Style::default().fg(ctx.text_primary),
                    ),
                },
                Shown::Null(kind) => (
                    null_glyph(kind).to_string(),
                    Style::default()
                        .fg(ctx.dimmed)
                        .add_modifier(Modifier::ITALIC),
                ),
                Shown::Unread => ("not read".to_string(), Style::default().fg(ctx.dimmed)),
                Shown::Reading => ("reading...".to_string(), Style::default().fg(ctx.dimmed)),
                Shown::Failed(_) => ("not read".to_string(), Style::default().fg(ctx.warning)),
            },
            None => (String::new(), Style::default()),
        };
        let value_text = crate::glyphs::fit_cells(&value_text, preview_w, g.ellipsis);
        let name_style = if is_selected {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else if ctx.column_colors {
            Style::default().fg(ctx.type_color(&field.dtype))
        } else {
            Style::default().fg(ctx.text_primary)
        };
        let spans = vec![
            Span::styled(marker, Style::default().fg(ctx.accent)),
            Span::styled(name.into_owned(), name_style),
            Span::raw(" ".repeat(name_pad + GAP)),
            Span::styled(label, Style::default().fg(ctx.dimmed)),
            Span::raw(" ".repeat(label_pad + GAP)),
            Span::styled(value_text.into_owned(), value_style),
        ];
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected && !modal.finding {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(line(y), buf);
    }

    // The focused value.
    let rule_y = list_y + list_h as u16;
    let name = focused.as_ref().map(|f| f.name.clone()).unwrap_or_default();
    draw_value(buf, content, rule_y, &name, &body, modal, ctx);
}

/// The focused value under its rule from `rule_y` to the bottom of `content`:
/// its lines from the pane's scroll, and a last line counting what is left.
fn draw_value(
    buf: &mut Buffer,
    content: Rect,
    rule_y: u16,
    name: &str,
    body: &Body,
    modal: &mut InspectorModal,
    ctx: &RenderContext,
) {
    let measure = (content.width as usize).min(MEASURE);
    let g = crate::glyphs::get();
    let line = |y: u16| Rect {
        y,
        height: 1,
        ..content
    };
    let width = content.width as usize;
    let name = crate::glyphs::fit_cells(name, width / 2, g.ellipsis);
    SectionRule {
        title: &name,
        chip: (!body.facts.is_empty()).then_some(body.facts.as_str()),
        focused: false,
    }
    .render(line(rule_y), buf, ctx);

    let body_y = rule_y + 1;
    let body_h = (content.y + content.height).saturating_sub(body_y) as usize;
    modal.page = body_h.max(1);
    let last_start = body.lines.len().saturating_sub(body_h);
    modal.scroll = modal.scroll.min(last_start);
    let below = body.lines.len().saturating_sub(modal.scroll + body_h);
    for (i, (text, tone)) in body
        .lines
        .iter()
        .skip(modal.scroll)
        .take(body_h)
        .enumerate()
    {
        let y = body_y + i as u16;
        if i + 1 == body_h && below > 0 {
            Paragraph::new(overflow_line(body, below + 1))
                .style(Style::default().fg(ctx.dimmed))
                .render(line(y), buf);
            break;
        }
        let style = match tone {
            Tone::Plain => Style::default().fg(ctx.text_primary),
            Tone::Dim => Style::default().fg(ctx.dimmed),
            Tone::Warn => Style::default().fg(ctx.warning),
        };
        Paragraph::new(text.as_str()).style(style).render(
            Rect {
                width: content.width.min(measure as u16),
                ..line(y)
            },
            buf,
        );
    }
}

/// Whether Enter opens `value` as a level: a list, array or struct, or text that
/// reads as a JSON object or array.
pub fn value_opens(value: &AnyValue) -> bool {
    match value {
        AnyValue::String(s) => opens_as_json(s),
        AnyValue::StringOwned(s) => opens_as_json(s),
        v => exact::is_nested_value(v),
    }
}

/// The value pane for an item of a level drilled into, labelled `label`.
pub fn node_body(label: &str, node: &Node, escaped: bool, chunks: usize, width: usize) -> Body {
    match node {
        Node::Native(series) => {
            let field = InspectField {
                name: label.to_string(),
                dtype: series.dtype().clone(),
                hidden: false,
            };
            let shown = match series.get(0) {
                Ok(AnyValue::Null) | Err(_) => Shown::Null(NullKind::Null),
                Ok(v) => Shown::Value(v),
            };
            body(&field, &shown, escaped, chunks, width, None)
        }
        Node::Json { .. } => json_body(
            node.json().unwrap_or(&JsonValue::Null),
            escaped,
            chunks,
            width,
        ),
    }
}

/// The value pane for a JSON value: text as text is shown, an object or array
/// indented up to the pane's budget.
fn json_body(value: &JsonValue, escaped: bool, chunks: usize, width: usize) -> Body {
    let g = crate::glyphs::get();
    let scalar = |text: String, kind: &str| {
        let mut lines = Vec::new();
        wrap_into(&text, width, Tone::Plain, &mut lines);
        Body {
            lines,
            facts: kind.to_string(),
            ..Body::default()
        }
    };
    match value {
        JsonValue::String(s) => {
            let field = InspectField {
                name: String::new(),
                dtype: DataType::String,
                hidden: false,
            };
            body(
                &field,
                &Shown::Value(AnyValue::String(s)),
                escaped,
                chunks,
                width,
                None,
            )
        }
        JsonValue::Null => Body {
            lines: vec![(format!("{} null", g.null), Tone::Dim)],
            facts: "null".to_string(),
            ..Body::default()
        },
        JsonValue::Bool(b) => scalar(b.to_string(), "bool"),
        JsonValue::Number(n) => scalar(n.to_string(), "number"),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            let (kind, count) = match value {
                JsonValue::Object(map) => ("object", plural(map.len(), "key", "keys")),
                JsonValue::Array(items) => ("array", plural(items.len(), "item", "items")),
                _ => unreachable!(),
            };
            let budget = CHUNK_BYTES.saturating_mul(chunks.max(1));
            let (text, cut) = json_text(value, true, budget);
            let mut lines = Vec::new();
            for line in text.split('\n') {
                wrap_into(line, width, Tone::Plain, &mut lines);
            }
            let mut rest = Rest::None;
            if cut {
                lines.push((format!("{} more", g.ellipsis), Tone::Dim));
                rest = Rest::Unknown;
            }
            Body {
                lines,
                facts: format!("{kind} {} {count}", g.middot),
                more: cut,
                rest,
                escapable: false,
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

/// Draw a level of a drill: its items (a table, for a list of structs) and the
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
    let measure = (area.width.saturating_sub(4) as usize).min(MEASURE);

    let body = match &focused {
        Some((label, child)) => {
            let key = BodyKey {
                frame: drill.frame,
                row: drill.row,
                field: drill.item_key(label),
                escaped: modal.escaped,
                chunks: modal.chunks,
                width: measure as u16,
                state: 5,
            };
            match &modal.body {
                Some((cached, body)) if *cached == key => body.clone(),
                _ => {
                    let built = node_body(label, child, modal.escaped, modal.chunks, measure);
                    modal.body = Some((key, built.clone()));
                    built
                }
            }
        }
        None => Body {
            lines: vec![(
                format!("No {}", shape.items_title().to_lowercase()),
                Tone::Dim,
            )],
            ..Body::default()
        },
    };

    let columns = node.table_columns();
    let header = usize::from(columns.is_some());
    let content_h = area.height.saturating_sub(3) as usize;
    let avail = content_h.saturating_sub(2 + header);
    let list_h = list_rows(len, avail);
    let body_h = content_h.saturating_sub(list_h + 2 + header);
    let overflows = body.lines.len() > body_h;
    let list_overflows = len > list_h;
    let opens = focused.as_ref().is_some_and(|(label, child)| {
        child.opens() && !modal.known_not_json(drill.frame, drill.row, &drill.item_key(label))
    });

    let mut bar = HintBar::from_ctx(ctx);
    if opens {
        bar = bar.hint_weighted("Enter", "Open", 9);
    } else if body.more {
        bar = bar.hint_weighted("Enter", "More", 9);
    }
    if focused.is_some() {
        bar = bar.hint_weighted("y", "Copy", 8);
    }
    if len > 1 {
        let word = match shape {
            Shape::Struct => "Field",
            Shape::Object => "Key",
            _ => "Item",
        };
        bar = bar.hint_weighted(g.updown, word, 7);
    }
    if overflows {
        bar = bar.hint_weighted("PgUp/PgDn", "Scroll", 4);
    }
    if body.escapable {
        let label = if modal.escaped { "Raw" } else { "Escaped" };
        bar = bar.hint_weighted("e", label, 3);
    }
    if list_overflows {
        bar = bar.hint_weighted("Home/End", "First/Last", 2);
    }
    let footer = bar.hint_weighted("Esc", "Back", 10);

    let labels: Vec<&str> = drill.levels.iter().map(|l| l.label.as_str()).collect();
    let title = drill_title(root_title, &labels, area.width.saturating_sub(4) as usize);
    let content = Surface::new(&title).footer(&footer).render(area, buf, ctx);
    if content.height < 4 || content.width < 12 {
        return;
    }
    let line = |y: u16| Rect {
        y,
        height: 1,
        ..content
    };
    let count = thousands(len);
    SectionRule {
        title: shape.items_title(),
        chip: Some(&count),
        focused: false,
    }
    .render(line(content.y), buf, ctx);

    let list_y = content.y + 1;
    let offset = selected.saturating_sub(list_h.saturating_sub(1));
    let below = len.saturating_sub(offset + list_h);
    let page = node.children(offset, list_h);
    let rows = ListRows {
        y: list_y + header as u16,
        offset,
        selected,
        below,
        list_h,
    };
    match columns {
        Some(columns) => draw_table(buf, content, node, &columns, &page, &rows, ctx),
        None => draw_items(buf, content, node, &page, &rows, ctx),
    }

    let rule_y = list_y + (header + list_h) as u16;
    let name = focused.map(|(label, _)| label).unwrap_or_default();
    draw_value(buf, content, rule_y, &name, &body, modal, ctx);
}

/// Where a level's items are drawn, and which of them.
struct ListRows {
    y: u16,
    offset: usize,
    selected: usize,
    /// Items past the last row drawn.
    below: usize,
    list_h: usize,
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
            if is_selected { g.rail } else { " " },
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
    let label_w = node.label_width().clamp(3, (width / 3).clamp(4, 28));
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
        let mut spans = item_head(label, label_w, name_style, is_selected, ctx);
        let kind = crate::glyphs::fit_cells(&child.type_label(), type_w, g.ellipsis).into_owned();
        let kind_pad = type_w.saturating_sub(crate::glyphs::cell_width(&kind));
        spans.push(Span::styled(kind, Style::default().fg(ctx.dimmed)));
        spans.push(Span::raw(" ".repeat(kind_pad + GAP)));
        let (text, style) = node_preview(child, preview_w, ctx);
        let text = crate::glyphs::fit_cells(&text, preview_w, g.ellipsis).into_owned();
        spans.push(Span::styled(text, style));
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected {
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
    columns: &[String],
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
    for column in columns {
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
    for (column, &w) in columns.iter().zip(&widths) {
        let (text, pad) = cell(&exact::preview(column, g), w);
        head.push(Span::styled(
            text,
            Style::default().fg(ctx.dimmed).add_modifier(Modifier::BOLD),
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
            ctx,
        );
        for (k, (column, &w)) in columns.iter().zip(&widths).enumerate() {
            if k > 0 && item.shape() == Shape::Leaf {
                break;
            }
            let (text, style) = cell_text(item, column, w);
            let (text, pad) = cell(&text, w);
            spans.push(Span::styled(text, style));
            spans.push(Span::raw(pad));
        }
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_selected {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(at, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(name: &str, dtype: DataType) -> InspectField {
        InspectField {
            name: name.to_string(),
            dtype,
            hidden: false,
        }
    }

    fn texts(body: &Body) -> Vec<&str> {
        body.lines.iter().map(|(t, _)| t.as_str()).collect()
    }

    #[test]
    fn a_float_shows_exact_and_what_the_table_rounds_it_to() {
        let f = field("amount", DataType::Float64);
        let b = body(
            &f,
            &Shown::Value(AnyValue::Float64(1000000.125)),
            false,
            1,
            40,
            Some("1.0000e6"),
        );
        assert_eq!(texts(&b), ["1000000.125", "", "In the table: 1.0000e6"]);
        assert_eq!(b.facts, "f64");
        // When the table shows the value as it is, nothing more is said.
        let b = body(
            &f,
            &Shown::Value(AnyValue::Float64(2.5)),
            false,
            1,
            40,
            Some("2.5"),
        );
        assert_eq!(texts(&b), ["2.5"]);
    }

    #[test]
    fn raw_text_breaks_lines_and_escaped_text_shows_the_escapes() {
        let f = field("note", DataType::String);
        let value = Shown::Value(AnyValue::String("line1\nline2\ttab\\n"));
        let raw = body(&f, &value, false, 1, 40, None);
        assert_eq!(texts(&raw), ["line1", "line2   tab\\n"]);
        let m = crate::glyphs::get().middot;
        assert_eq!(raw.facts, format!("str {m} 17 chars {m} 2 lines"));
        let esc = body(&f, &value, true, 1, 40, None);
        assert_eq!(texts(&esc), [r#""line1\nline2\ttab\\n""#]);
        assert!(esc.facts.ends_with("escaped"));
    }

    #[test]
    fn empty_text_and_edge_spaces_are_named() {
        let f = field("s", DataType::String);
        let empty = body(&f, &Shown::Value(AnyValue::String("")), false, 1, 40, None);
        assert_eq!(texts(&empty), ["empty string"]);
        assert_eq!(empty.lines[0].1, Tone::Dim);
        let esc = body(&f, &Shown::Value(AnyValue::String("")), true, 1, 40, None);
        assert_eq!(texts(&esc), ["\"\""]);
        let padded = body(
            &f,
            &Shown::Value(AnyValue::String("  x ")),
            false,
            1,
            40,
            None,
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
        let f = field("s", DataType::String);
        let null = body(&f, &Shown::Null(NullKind::Null), false, 1, 60, None);
        let g = crate::glyphs::get();
        assert_eq!(texts(&null), [format!("{} null", g.null).as_str()]);
        let absent = body(&f, &Shown::Null(NullKind::Absent), false, 1, 80, None);
        assert!(texts(&absent)[0].starts_with(&format!("{} absent", g.absent)));
        let conflict = body(&f, &Shown::Null(NullKind::Conflict), false, 1, 80, None);
        assert!(texts(&conflict)[0].starts_with(&format!("{} conflicting", g.conflict)));
    }

    #[test]
    fn a_huge_value_shows_a_chunk_and_offers_more() {
        let f = field("blob", DataType::String);
        let big = "x".repeat(CHUNK_BYTES * 3 + 5);
        let value = Shown::Value(AnyValue::String(&big));
        let first = body(&f, &value, false, 1, 100, None);
        assert!(first.more);
        assert_eq!(
            first.lines.len(),
            CHUNK_BYTES / 100 + 2,
            "the chunk, then the count"
        );
        let last = &first.lines.last().unwrap().0;
        assert!(
            last.ends_with(&format!("{} more chars", thousands(CHUNK_BYTES * 2 + 5))),
            "{last}"
        );
        let all = body(&f, &value, false, 4, 100, None);
        assert!(!all.more);
        assert!(all.facts.contains(&thousands(big.len())));
    }

    #[test]
    fn wrapping_keeps_wide_characters_whole() {
        let mut out = Vec::new();
        wrap_into("東京大阪京都", 5, Tone::Plain, &mut out);
        let rows: Vec<&str> = out.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(rows, ["東京", "大阪", "京都"]);
    }

    #[test]
    fn nested_values_expand_and_binary_dumps() {
        let f = field("l", DataType::List(Box::new(DataType::Float64)));
        let list = AnyValue::List(Series::new("".into(), [1.5f64, -0.0]));
        let b = body(&f, &Shown::Value(list), false, 1, 40, None);
        assert_eq!(texts(&b), ["[", "  1.5,", "  -0.0", "]"]);
        assert!(b.facts.contains("2 items"), "{}", b.facts);

        let f = field("b", DataType::Binary);
        let bin = body(
            &f,
            &Shown::Value(AnyValue::Binary(b"AB\x00")),
            false,
            1,
            80,
            None,
        );
        assert_eq!(
            texts(&bin)[0],
            format!("00000000  41 42 00{}  AB.", " ".repeat(39))
        );
        let esc = body(
            &f,
            &Shown::Value(AnyValue::Binary(b"AB\x00")),
            true,
            1,
            80,
            None,
        );
        assert_eq!(texts(&esc), [r#"b"AB\x00""#]);
    }

    #[test]
    fn an_unread_field_says_enter_reads_it() {
        let f = InspectField {
            hidden: true,
            ..field("secret", DataType::String)
        };
        let b = body(&f, &Shown::Unread, false, 1, 100, None);
        assert!(b.more, "Enter has something to do");
        assert!(texts(&b)[0].contains("Enter reads"), "{:?}", texts(&b));
        // The pane's own sentences wrap at spaces, not inside a word.
        let narrow = body(&f, &Shown::Unread, false, 1, 30, None);
        let joined = texts(&narrow).join(" ");
        assert_eq!(
            joined,
            "Not read with the table's rows; Enter reads this row's hidden and binary fields"
        );
        assert!(texts(&narrow).iter().all(|l| l.chars().count() <= 30));
    }

    /// D3: a cut value's last line counts the whole value, not only the chunk
    /// formatted: a hex dump in lines, text in its chars past the lines shown.
    #[test]
    fn the_overflow_line_counts_the_whole_value() {
        let f = field("blob", DataType::Binary);
        let bytes = vec![7u8; 1 << 20];
        let b = body(
            &f,
            &Shown::Value(AnyValue::Binary(&bytes)),
            false,
            1,
            80,
            None,
        );
        // 4 KiB dumped at 16 bytes a line; the rest counted, not formatted.
        assert_eq!(b.lines.len(), 256 + 1);
        assert_eq!(b.rest, Rest::Lines((bytes.len() - 4096) / 16));
        // 20 lines on screen: the 19th onward of 65,536 are left.
        let hidden = b.lines.len() - 19;
        assert_eq!(
            overflow_line(&b, hidden),
            more_line(65_536 - 19, "line", "lines")
        );

        let f = field("text", DataType::String);
        let big = "x".repeat(CHUNK_BYTES * 2);
        let b = body(
            &f,
            &Shown::Value(AnyValue::String(&big)),
            false,
            1,
            100,
            None,
        );
        assert_eq!(b.rest, Rest::Units(plural(CHUNK_BYTES, "char", "chars")));
        let line = overflow_line(&b, 10);
        assert!(
            line.ends_with(&format!(
                "9 more lines, then {} chars",
                thousands(CHUNK_BYTES)
            )),
            "{line}"
        );
        // Only the cut line itself left: it says what was cut.
        assert_eq!(overflow_line(&b, 1), b.lines.last().unwrap().0);

        // A value formatted whole counts its own lines.
        let short = body(
            &f,
            &Shown::Value(AnyValue::String("a\nb\nc")),
            false,
            1,
            40,
            None,
        );
        assert_eq!(short.rest, Rest::None);
        assert_eq!(overflow_line(&short, 2), more_line(2, "line", "lines"));
    }

    /// The hex dump's count is exact at the edges: every size is ceil(len / 16) lines,
    /// formatted or counted, however many chunks were formatted.
    #[test]
    fn hex_line_counts_are_exact_at_the_edges() {
        let f = field("blob", DataType::Binary);
        for len in [
            0usize,
            1,
            15,
            16,
            17,
            4095,
            4096,
            4097,
            1 << 20,
            (1 << 20) + 1,
        ] {
            for chunks in [1, 2] {
                let bytes = vec![1u8; len];
                let b = body(
                    &f,
                    &Shown::Value(AnyValue::Binary(&bytes)),
                    false,
                    chunks,
                    80,
                    None,
                );
                let formatted = match b.rest {
                    // The last line says what was cut; it is not a line of the dump.
                    Rest::Lines(n) => b.lines.len() - 1 + n,
                    Rest::None => b.lines.len(),
                    ref other => panic!("{len}: {other:?}"),
                };
                let want = len.div_ceil(16).max(1);
                assert_eq!(formatted, want, "{len} bytes, {chunks} chunks");
                // Every line of it hidden but the first: the count is all the rest.
                if len > 4096 * chunks {
                    let hidden = b.lines.len() - 1;
                    assert_eq!(
                        overflow_line(&b, hidden),
                        more_line(want - 1, "line", "lines"),
                        "{len}"
                    );
                }
            }
        }
    }

    /// A cut text's count is of chars, not bytes, whatever their width.
    #[test]
    fn a_cut_text_counts_multi_byte_chars() {
        let f = field("text", DataType::String);
        for ch in ["é", "€", "🦀"] {
            let big = ch.repeat(CHUNK_BYTES);
            let b = body(
                &f,
                &Shown::Value(AnyValue::String(&big)),
                false,
                1,
                100,
                None,
            );
            let shown = exact::prefix(&big, CHUNK_BYTES).chars().count();
            assert!(shown > 0 && shown < CHUNK_BYTES, "{ch}");
            assert_eq!(
                b.rest,
                Rest::Units(plural(CHUNK_BYTES - shown, "char", "chars")),
                "{ch}"
            );
            assert!(
                b.facts.contains(&plural(CHUNK_BYTES, "char", "chars")),
                "{}",
                b.facts
            );
        }
    }

    /// D5: empty text and empty bytes are said, in the list and in the pane.
    #[test]
    fn empty_values_are_visible() {
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
        assert_eq!(empty_preview(&AnyValue::Binary(b"a")), None);
        let f = field("b", DataType::Binary);
        let b = body(&f, &Shown::Value(AnyValue::Binary(b"")), false, 1, 80, None);
        assert_eq!(texts(&b), ["empty binary"]);
        assert_eq!(b.lines[0].1, Tone::Dim);
        assert!(b.facts.ends_with("empty"), "{}", b.facts);
    }

    /// D6: `e` acts on text and bytes only.
    #[test]
    fn only_text_and_bytes_are_escapable() {
        let cases = [
            (DataType::String, Shown::Value(AnyValue::String("a")), true),
            (DataType::Binary, Shown::Value(AnyValue::Binary(b"a")), true),
            (DataType::Int64, Shown::Value(AnyValue::Int64(1)), false),
            (DataType::Date, Shown::Value(AnyValue::Date(1)), false),
            (DataType::String, Shown::Null(NullKind::Null), false),
            (DataType::Binary, Shown::Unread, false),
        ];
        for (dtype, shown, escapable) in cases {
            let b = body(&field("f", dtype.clone()), &shown, false, 1, 40, None);
            assert_eq!(b.escapable, escapable, "{dtype:?} {shown:?}");
        }
    }

    /// D11: every field is listed when they all fit beside a short value; a long
    /// list keeps half. The value always keeps its few lines.
    #[test]
    fn the_list_takes_the_rows_the_value_does_not_need() {
        // 80x24: 18 rows shared by the 14-field list and the value.
        assert_eq!(list_rows(14, 18), 14);
        assert_eq!(list_rows(15, 18), 15);
        assert_eq!(list_rows(16, 18), 9);
        assert_eq!(list_rows(214, 18), 9);
        assert_eq!(list_rows(1, 18), 1);
        assert_eq!(list_rows(0, 18), 1);
        // 60x20: 14 rows.
        assert_eq!(list_rows(14, 14), 7);
        assert_eq!(list_rows(11, 14), 11);
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
        // A key with a line break is marked, not broken.
        let marked = drill_title("Row 1", &["a\nb"], 40);
        assert!(!marked.contains('\n'), "{marked}");
    }

    #[test]
    fn json_values_show_as_themselves() {
        let m = crate::glyphs::get().middot;
        let doc: JsonValue = serde_json::from_str(r#"{"a": [1, 2], "s": "x\ny"}"#).unwrap();
        let b = json_body(&doc, false, 1, 40);
        assert_eq!(b.facts, format!("object {m} 2 keys"));
        assert_eq!(texts(&b)[..2], ["{", "  \"a\": ["]);
        let s = json_body(&doc["s"], false, 1, 40);
        assert_eq!(texts(&s), ["x", "y"]);
        assert!(s.escapable, "text has an escaped form");
        let n = json_body(&JsonValue::from(1.5), false, 1, 40);
        assert_eq!((texts(&n), n.facts.as_str()), (vec!["1.5"], "number"));
        // An array too long for the budget is cut and says so.
        let big: JsonValue = serde_json::from_str(&format!("[{}0]", "0,".repeat(20_000))).unwrap();
        let b = json_body(&big, false, 1, 40);
        assert_eq!(b.rest, Rest::Unknown);
        assert!(b.lines.len() < 20_000);
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
}

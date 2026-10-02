//! The row inspector: every field of the table's selected row, and the focused
//! field's whole value. A takeover over the table — one Surface titled with the
//! row, the fields as a list (name, type, the table's preview), and under a
//! section rule the value itself: exact, wrapped, scrolled, never rounded.

use crate::copy_modal::thousands;
use crate::exact;
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
    let content_w = area.width.saturating_sub(4) as usize;
    let measure = content_w.min(MEASURE);

    // The focused field's pane, from the cache while nothing it shows changed.
    let focused = modal.focused().cloned();
    // What Enter does here, for the footer: read, retry a failed read, or show more.
    let mut enter = None;
    let body = match (&row, &focused) {
        (Some(row), Some(field)) => {
            let value = shown(field, row, modal.read.as_ref(), state);
            enter = match value {
                Shown::Unread => Some("Read"),
                Shown::Failed(_) => Some("Retry"),
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
        bar = bar
            .hint_weighted("y", "Copy", 8)
            .hint_weighted(g.updown, "Field", 7)
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
    let name = crate::glyphs::fit_cells(&name, width / 2, g.ellipsis);
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
            Paragraph::new(overflow_line(&body, below + 1))
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

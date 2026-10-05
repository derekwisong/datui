//! The Documentation view: what a catalog says of one dataset, and what the format spec
//! that reads a file says of it, as a page to read.
//!
//! `Ctrl+E` on a home row opens it full screen; the Info panel's Documentation tab
//! draws the same page for the open dataset. A catalog's word comes first: its
//! description, its links, and a column's description, unit and legend each stand over
//! the spec's. Fields are `label  value` lines, each link is a line of its own that is
//! cut with `…` rather than wrapped (`y` copies it whole), and a column's value legend
//! opens under it with `Enter`.

use std::collections::HashSet;
use std::sync::Arc;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};
use unicode_width::UnicodeWidthStr;

use crate::catalog::{ColumnNote, Dataset};
use crate::formats::SpecDocs;
use crate::glyphs;
use crate::render::context::RenderContext;
use crate::widgets::ui::{HintBar, SectionRule, Surface};

/// The widest a line of prose runs on a wide terminal: a reading surface keeps its
/// measure.
const MEASURE: u16 = 110;

/// What a page documents: a catalog's dataset, what the format spec that reads the file
/// says of it, or both.
#[derive(Debug, Clone, Default)]
pub struct Documented {
    /// The dataset, and the label of the catalog that lists it.
    pub catalog: Option<(String, Arc<Dataset>)>,
    /// What the format spec that reads the file says of it.
    pub spec: Option<Arc<SpecDocs>>,
    /// The file's name: the page's title when no catalog names it.
    pub name: String,
}

impl Documented {
    /// The page of a catalog's dataset, of what a spec says, or of both; `None` with
    /// neither.
    pub fn new(
        catalog: Option<(String, Arc<Dataset>)>,
        spec: Option<Arc<SpecDocs>>,
        name: String,
    ) -> Option<Self> {
        (catalog.is_some() || spec.is_some()).then_some(Self {
            catalog,
            spec,
            name,
        })
    }

    /// The page's title: the catalog's name for the dataset, else the file's.
    pub fn title(&self) -> &str {
        match &self.catalog {
            Some((_, entry)) => &entry.name,
            None => &self.name,
        }
    }

    /// Each column's note, in the spec's order, then the columns only the catalog
    /// notes. Where both note a column, the catalog's description, unit and legend each
    /// stand over the spec's when it gives one.
    pub fn columns(&self) -> Vec<(String, ColumnNote)> {
        let catalog: &[(String, ColumnNote)] = self
            .catalog
            .as_ref()
            .map_or(&[], |(_, entry)| &entry.columns);
        let spec: &[(String, ColumnNote)] = self.spec.as_ref().map_or(&[], |s| &s.columns);
        let noted = |name: &str| catalog.iter().find(|(n, _)| n == name).map(|(_, n)| n);
        let mut out: Vec<(String, ColumnNote)> = spec
            .iter()
            .map(|(name, note)| {
                let note = match noted(name) {
                    Some(over) => layered(over, note),
                    None => note.clone(),
                };
                (name.clone(), note)
            })
            .collect();
        out.extend(
            catalog
                .iter()
                .filter(|(name, _)| !spec.iter().any(|(n, _)| n == name))
                .cloned(),
        );
        out
    }
}

/// `over`'s description, unit and legend, each in place of `under`'s when it gives one.
fn layered(over: &ColumnNote, under: &ColumnNote) -> ColumnNote {
    let pick = |a: &String, b: &String| if a.is_empty() { b } else { a }.clone();
    ColumnNote {
        description: pick(&over.description, &under.description),
        unit: pick(&over.unit, &under.unit),
        values: if over.values.is_empty() {
            under.values.clone()
        } else {
            over.values.clone()
        },
        ty: pick(&over.ty, &under.ty),
    }
}

/// A section of named fields of a spec's header or footer, each with its note.
fn field_section(out: &mut Vec<DocLine>, title: &'static str, fields: &[(String, ColumnNote)]) {
    if fields.is_empty() {
        return;
    }
    out.push(DocLine::Blank);
    out.push(DocLine::Section(title, fields.len()));
    for (name, note) in fields {
        out.push(DocLine::Column {
            name: name.clone(),
            about: note.about(),
            values: 0,
        });
    }
}

/// One line of the page, before it is laid out to a width.
#[derive(Debug, Clone, PartialEq)]
pub enum DocLine {
    /// The dataset's description, wrapped.
    About(String),
    /// `label  value`, the value wrapped under itself.
    Field(&'static str, String),
    /// `label  url` on one line, never wrapped: cut with `…` when too wide.
    Link(&'static str, String),
    /// A section title on a rule, with a count.
    Section(&'static str, usize),
    /// A column: its name, what it means, and how many codes its legend has.
    Column {
        name: String,
        about: String,
        values: usize,
    },
    /// One code of an open legend.
    Legend(String, String),
    /// A record type of a format spec: its name, and what picks it, its columns and
    /// what it is.
    RecordType(String, String),
    /// A bookmark: its name and where it goes.
    Bookmark(String, String),
    Blank,
}

impl DocLine {
    /// Whether the cursor stops here.
    fn focusable(&self) -> bool {
        !matches!(self, DocLine::Blank | DocLine::Section(..))
    }

    /// What `y` copies on this line: a link's URL, a bookmark's path, a field's value.
    pub fn copy_text(&self) -> Option<&str> {
        match self {
            DocLine::Link(_, url) => Some(url),
            DocLine::Field(_, value) => Some(value),
            DocLine::Bookmark(_, path) => Some(path),
            DocLine::Legend(code, _) => Some(code),
            DocLine::Column { name, .. } => Some(name),
            DocLine::RecordType(name, _) => Some(name),
            DocLine::About(text) => Some(text),
            DocLine::Section(..) | DocLine::Blank => None,
        }
    }
}

/// The page's lines for `doc`, with the legends of `expanded` columns open. `measured`
/// is the size something has measured, if any.
pub fn lines(doc: &Documented, expanded: &HashSet<String>, measured: Option<u64>) -> Vec<DocLine> {
    let mut out = Vec::new();
    let entry = doc.catalog.as_ref().map(|(_, entry)| entry.as_ref());
    let spec = doc.spec.as_deref();
    // The catalog's word, else the spec's.
    let first_said = |of_entry: Option<&str>, of_spec: Option<&str>| {
        [of_entry, of_spec]
            .into_iter()
            .flatten()
            .find(|text| !text.is_empty())
            .unwrap_or_default()
            .to_string()
    };
    let description = first_said(
        entry.map(|e| e.description.as_str()),
        spec.map(|s| s.description.as_str()),
    );
    if !description.is_empty() {
        out.push(DocLine::About(description));
        out.push(DocLine::Blank);
    }
    if let Some((catalog, entry)) = &doc.catalog {
        catalog_fields(&mut out, entry, catalog, measured);
    }
    if let Some(spec) = spec {
        out.push(DocLine::Field("format spec", spec.spec.clone()));
        if let Some(file) = &spec.file {
            out.push(DocLine::Field("spec file", crate::home::display_path(file)));
        }
    }
    let documentation = first_said(
        entry.map(|e| e.documentation.as_str()),
        spec.map(|s| s.documentation.as_str()),
    );
    let links: Vec<(&'static str, String)> = [
        (
            "homepage",
            entry.map(|e| e.homepage.clone()).unwrap_or_default(),
        ),
        ("documentation", documentation),
    ]
    .into_iter()
    .filter(|(_, url)| !url.is_empty())
    .collect();
    if !links.is_empty() {
        out.push(DocLine::Blank);
        out.push(DocLine::Section("LINKS", links.len()));
        for (label, url) in links {
            out.push(DocLine::Link(label, url));
        }
    }
    if let Some(spec) = spec {
        field_section(&mut out, "HEADER", &spec.header);
    }
    if let Some(spec) = spec.filter(|s| !s.record_types.is_empty()) {
        let middot = glyphs::get().middot;
        out.push(DocLine::Blank);
        out.push(DocLine::Section("RECORD TYPES", spec.record_types.len()));
        for record in &spec.record_types {
            let columns = match record.columns {
                1 => "1 column".to_string(),
                n => format!("{n} columns"),
            };
            let mut about = format!("{} {middot} {columns}", record.picked_by);
            if !record.description.is_empty() {
                about = format!("{about} {middot} {}", record.description);
            }
            out.push(DocLine::RecordType(record.name.clone(), about));
        }
    }
    let columns = doc.columns();
    if !columns.is_empty() {
        out.push(DocLine::Blank);
        out.push(DocLine::Section("COLUMNS", columns.len()));
        for (name, note) in &columns {
            out.push(DocLine::Column {
                name: name.clone(),
                about: note.about(),
                values: note.values.len(),
            });
            if expanded.contains(name) {
                for (code, meaning) in &note.values {
                    let code = if code.is_empty() { "blank" } else { code };
                    out.push(DocLine::Legend(code.to_string(), meaning.clone()));
                }
            }
        }
    }
    if let Some(spec) = spec {
        field_section(&mut out, "FOOTER", &spec.footer);
    }
    if let Some(entry) = entry.filter(|e| !e.bookmarks.is_empty()) {
        out.push(DocLine::Blank);
        out.push(DocLine::Section("BOOKMARKS", entry.bookmarks.len()));
        for (name, path) in &entry.bookmarks {
            out.push(DocLine::Bookmark(name.clone(), path.clone()));
        }
    }
    out
}

/// What the catalog says of where `entry` is and whose it is.
fn catalog_fields(out: &mut Vec<DocLine>, entry: &Dataset, catalog: &str, measured: Option<u64>) {
    out.push(DocLine::Field("catalog", catalog.to_string()));
    for (label, value) in [("publisher", &entry.publisher), ("license", &entry.license)] {
        if !value.is_empty() {
            out.push(DocLine::Field(label, value.clone()));
        }
    }
    out.push(DocLine::Field("format", format_of(entry)));
    match (&entry.path, &entry.url) {
        (Some(_), _) => out.push(DocLine::Field(
            "path",
            crate::home::display_path(&entry.location()),
        )),
        (None, Some(url)) => {
            out.push(DocLine::Link("url", url.clone()));
            out.push(DocLine::Field("login", crate::home::login_of(entry)));
        }
        (None, None) => {}
    }
    match (measured, entry.size) {
        (Some(size), _) => out.push(DocLine::Field("size", crate::discover::format_size(size))),
        (None, Some(hint)) => out.push(DocLine::Field(
            "size",
            format!("~{}", crate::discover::format_size(hint)),
        )),
        (None, None) => {}
    }
}

/// What the dataset is, in a word: its file format, or `directory`.
fn format_of(entry: &Dataset) -> String {
    let location = entry.location();
    let text = location.to_string_lossy();
    if text.ends_with('/') || (entry.path.is_some() && location.is_dir()) {
        return "directory".to_string();
    }
    crate::FileFormat::from_path(&location)
        .map(|f| f.name().to_string())
        .unwrap_or_else(|| {
            if crate::catalog::is_object_store_dataset(&text) {
                "directory".to_string()
            } else {
                "file".to_string()
            }
        })
}

/// The view's state: the page shown, where the cursor is, which legends are open.
#[derive(Debug, Clone, Default)]
pub struct DocState {
    pub doc: Option<Documented>,
    /// What has measured the file, if anything has: shown in place of the hint.
    pub measured: Option<u64>,
    /// Index into [`Self::lines`] of the line the cursor is on.
    pub cursor: usize,
    /// First visual row drawn.
    pub scroll: usize,
    /// Columns whose legends are open.
    pub expanded: HashSet<String>,
    /// Rows the last frame had room for, for paging.
    pub view_height: usize,
    /// Whether `o` opens a link here: set by the app when it opens the page.
    pub links_open: bool,
}

impl DocState {
    /// Show `doc`, from the top, every legend closed.
    pub fn open(&mut self, doc: Documented, measured: Option<u64>) {
        *self = Self {
            doc: Some(doc),
            measured,
            ..Self::default()
        };
        self.cursor = self.first_focusable();
    }

    pub fn close(&mut self) {
        *self = Self::default();
    }

    pub fn is_open(&self) -> bool {
        self.doc.is_some()
    }

    pub fn lines(&self) -> Vec<DocLine> {
        match &self.doc {
            Some(doc) => lines(doc, &self.expanded, self.measured),
            None => Vec::new(),
        }
    }

    fn first_focusable(&self) -> usize {
        self.lines()
            .iter()
            .position(DocLine::focusable)
            .unwrap_or(0)
    }

    /// Move the cursor `delta` focusable lines, stopping at either end.
    pub fn move_cursor(&mut self, delta: isize) {
        let lines = self.lines();
        let focusable: Vec<usize> = (0..lines.len()).filter(|&i| lines[i].focusable()).collect();
        if focusable.is_empty() {
            return;
        }
        let at = focusable
            .iter()
            .position(|&i| i >= self.cursor)
            .unwrap_or(focusable.len() - 1) as isize;
        let to = (at + delta).clamp(0, focusable.len() as isize - 1) as usize;
        self.cursor = focusable[to];
    }

    /// Open or close the legend of the column the cursor is on. False when it is not on
    /// a column with one.
    pub fn toggle_legend(&mut self) -> bool {
        let lines = self.lines();
        // On a code of an open legend, the legend's column.
        let column = lines[..=self.cursor.min(lines.len().saturating_sub(1))]
            .iter()
            .rev()
            .find_map(|line| match line {
                DocLine::Column { name, values, .. } if *values > 0 => Some(name.clone()),
                DocLine::Legend(..) => None,
                _ => Some(String::new()),
            })
            .filter(|name| !name.is_empty());
        let on_legend_or_column = matches!(
            lines.get(self.cursor),
            Some(DocLine::Legend(..)) | Some(DocLine::Column { values: 1.., .. })
        );
        let Some(column) = column.filter(|_| on_legend_or_column) else {
            return false;
        };
        if !self.expanded.remove(&column) {
            self.expanded.insert(column.clone());
        }
        // Back onto the column's own line when its legend closes under the cursor.
        let lines = self.lines();
        if let Some(at) = lines
            .iter()
            .position(|l| matches!(l, DocLine::Column { name, values: 1.., .. } if *name == column))
            && !self.expanded.contains(&column)
        {
            self.cursor = at;
        }
        true
    }

    /// The link on the cursor's line, as the page has it: what `o` offers to open.
    /// Only a Link line's; a value or a bookmark is never opened.
    pub fn link(&self) -> Option<String> {
        match self.lines().get(self.cursor) {
            Some(DocLine::Link(_, url)) => Some(url.clone()),
            _ => None,
        }
    }

    /// Whether the footer offers `o`: on a link, where a browser would show it.
    pub fn offers_open(&self) -> bool {
        self.links_open && self.link().is_some()
    }

    /// What `y` copies at the cursor.
    pub fn copy_text(&self) -> Option<String> {
        self.lines()
            .get(self.cursor)
            .and_then(DocLine::copy_text)
            .map(str::to_string)
    }
}

/// `text` cut to `width` columns with the ellipsis when it does not fit.
fn cut(text: &str, width: usize) -> String {
    if text.width() <= width {
        return text.to_string();
    }
    let ellipsis = glyphs::get().ellipsis;
    let room = width.saturating_sub(ellipsis.width());
    let mut out = String::new();
    for c in text.chars() {
        if out.width() + c.to_string().width() > room {
            break;
        }
        out.push(c);
    }
    out.push_str(ellipsis);
    out
}

/// `text` wrapped at word boundaries to `width` columns.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(8);
    let mut rows = Vec::new();
    let mut row = String::new();
    for word in text.split_whitespace() {
        let candidate = if row.is_empty() {
            word.to_string()
        } else {
            format!("{row} {word}")
        };
        if candidate.width() <= width || row.is_empty() {
            row = candidate;
            while row.width() > width {
                // One word longer than the row: cut it where the row ends.
                let head: String = row.chars().take(width).collect();
                let rest: String = row.chars().skip(width).collect();
                rows.push(head);
                row = rest;
            }
        } else {
            rows.push(std::mem::take(&mut row));
            row = word.to_string();
        }
    }
    if !row.is_empty() || rows.is_empty() {
        rows.push(row);
    }
    rows
}

/// Lay `lines` out at `width`: the visual rows of each, and its first row's index.
fn layout(
    lines: &[DocLine],
    width: usize,
    cursor: usize,
    ctx: &RenderContext,
) -> (Vec<Line<'static>>, Vec<usize>) {
    let g = glyphs::get();
    let key_w = lines
        .iter()
        .filter_map(|l| match l {
            DocLine::Field(label, _) | DocLine::Link(label, _) => Some(label.len()),
            _ => None,
        })
        .max()
        .unwrap_or(8)
        + 2;
    let col_w = lines
        .iter()
        .filter_map(|l| match l {
            DocLine::Column { name, .. }
            | DocLine::Bookmark(name, _)
            | DocLine::RecordType(name, _) => Some(name.width()),
            _ => None,
        })
        .max()
        .unwrap_or(0)
        .min(width / 3)
        + 2;
    let label = Style::default().fg(ctx.label);
    let plain = Style::default().fg(ctx.text_primary);
    let dim = Style::default().fg(ctx.dimmed);
    // One column for the rail, as every list in datui reserves it.
    let inner = width.saturating_sub(1);
    let mut rows: Vec<Line<'static>> = Vec::new();
    let mut starts = Vec::with_capacity(lines.len());
    for (i, line) in lines.iter().enumerate() {
        starts.push(rows.len());
        let focused = i == cursor;
        let rail = if focused { g.rail } else { " " };
        let rail = Span::styled(rail, Style::default().fg(ctx.accent));
        let key_style = if focused {
            Style::default().fg(ctx.accent)
        } else {
            label
        };
        let mut push = |spans: Vec<Span<'static>>, first: bool| {
            let mut all = vec![if first { rail.clone() } else { Span::raw(" ") }];
            all.extend(spans);
            let mut line = Line::from(all);
            if focused {
                line = line.patch_style(ctx.highlight_style());
            }
            rows.push(line);
        };
        match line {
            DocLine::Blank => rows.push(Line::from("")),
            DocLine::Section(..) => rows.push(Line::from("")),
            DocLine::About(text) => {
                for (n, row) in wrap(text, inner).into_iter().enumerate() {
                    push(vec![Span::styled(row, plain)], n == 0);
                }
            }
            DocLine::Field(key, value) => {
                let room = inner.saturating_sub(key_w);
                for (n, row) in wrap(value, room).into_iter().enumerate() {
                    let head = if n == 0 {
                        format!("{key:<key_w$}")
                    } else {
                        " ".repeat(key_w)
                    };
                    push(
                        vec![Span::styled(head, key_style), Span::styled(row, plain)],
                        n == 0,
                    );
                }
            }
            DocLine::Link(key, url) => {
                let room = inner.saturating_sub(key_w);
                push(
                    vec![
                        Span::styled(format!("{key:<key_w$}"), key_style),
                        Span::styled(cut(url, room), plain.add_modifier(Modifier::UNDERLINED)),
                    ],
                    true,
                );
            }
            DocLine::Column {
                name,
                about,
                values,
            } => {
                let marker = if *values == 0 {
                    String::new()
                } else {
                    let open = if i + 1 < lines.len() && matches!(lines[i + 1], DocLine::Legend(..))
                    {
                        g.expanded
                    } else {
                        g.collapsed
                    };
                    format!("  {}{values} values", open)
                };
                let room = inner.saturating_sub(col_w);
                let text = format!("{about}{marker}");
                for (n, row) in wrap(&text, room).into_iter().enumerate() {
                    let head = if n == 0 {
                        format!("{:<col_w$}", cut(name, col_w - 2))
                    } else {
                        " ".repeat(col_w)
                    };
                    push(
                        vec![Span::styled(head, key_style), Span::styled(row, plain)],
                        n == 0,
                    );
                }
            }
            DocLine::RecordType(name, about) => {
                let room = inner.saturating_sub(col_w);
                for (n, row) in wrap(about, room).into_iter().enumerate() {
                    let head = if n == 0 {
                        format!("{:<col_w$}", cut(name, col_w - 2))
                    } else {
                        " ".repeat(col_w)
                    };
                    push(
                        vec![Span::styled(head, key_style), Span::styled(row, plain)],
                        n == 0,
                    );
                }
            }
            DocLine::Legend(code, meaning) => {
                let pad = col_w + 2;
                let code_w = 6usize.max(code.width() + 2);
                let room = inner.saturating_sub(pad + code_w);
                for (n, row) in wrap(meaning, room).into_iter().enumerate() {
                    let head = if n == 0 {
                        format!("{}{code:<code_w$}", " ".repeat(pad))
                    } else {
                        " ".repeat(pad + code_w)
                    };
                    push(
                        vec![Span::styled(head, dim), Span::styled(row, plain)],
                        n == 0,
                    );
                }
            }
            DocLine::Bookmark(name, path) => {
                let room = inner.saturating_sub(col_w);
                push(
                    vec![
                        Span::styled(format!("{:<col_w$}", cut(name, col_w - 2)), key_style),
                        Span::styled(cut(path, room), plain),
                    ],
                    true,
                );
            }
        }
    }
    (rows, starts)
}

/// Draw the page in `area` (no frame): what the Info tab and the full-screen view share.
/// Keeps the cursor's line in view.
pub fn render_page(state: &mut DocState, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    if area.width < 10 || area.height == 0 {
        return;
    }
    let area = Rect {
        width: area.width.min(MEASURE),
        ..area
    };
    let lines = state.lines();
    state.cursor = state.cursor.min(lines.len().saturating_sub(1));
    let (rows, starts) = layout(&lines, area.width as usize, state.cursor, ctx);
    let height = area.height as usize;
    state.view_height = height;
    // The cursor's whole line in view, and the section title above a first line.
    let first = starts.get(state.cursor).copied().unwrap_or(0);
    let last = starts
        .get(state.cursor + 1)
        .copied()
        .unwrap_or(rows.len())
        .saturating_sub(1);
    if first < state.scroll {
        state.scroll = first;
    }
    if last >= state.scroll + height {
        state.scroll = last + 1 - height.min(last + 1);
    }
    if state.scroll > 0 && state.cursor == state.first_focusable() {
        state.scroll = 0;
    }
    let shown: Vec<Line> = rows
        .iter()
        .skip(state.scroll)
        .take(height)
        .cloned()
        .collect();
    Paragraph::new(shown).render(area, buf);
    // Section titles are drawn over their placeholder rows, as rules.
    for (i, line) in lines.iter().enumerate() {
        let DocLine::Section(title, count) = line else {
            continue;
        };
        let Some(row) = starts[i].checked_sub(state.scroll).filter(|r| *r < height) else {
            continue;
        };
        let chip = count.to_string();
        SectionRule {
            title,
            chip: Some(&chip),
            focused: false,
        }
        .render(
            Rect {
                x: area.x + 1,
                y: area.y + row as u16,
                width: area.width.saturating_sub(1),
                height: 1,
            },
            buf,
            ctx,
        );
    }
}

/// The full-screen view: the page in a frame titled with the dataset's name, and the
/// keys that work on it.
pub fn render_view(state: &mut DocState, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    let Some(title) = state.doc.as_ref().map(|d| d.title().to_string()) else {
        return;
    };
    let mut footer = HintBar::from_ctx(ctx);
    let lines = state.lines();
    match lines.get(state.cursor) {
        Some(DocLine::Column { values: 1.., .. }) | Some(DocLine::Legend(..)) => {
            footer = footer.hint("Enter", "Values");
        }
        _ => {}
    }
    if state.offers_open() {
        footer = footer.hint("o", "Open");
    }
    footer = footer.hint("y", "Copy").hint("Esc", "Back");
    let title = format!("Documentation {} {title}", glyphs::get().trail);
    let inner = Surface::new(&title).footer(&footer).render(area, buf, ctx);
    render_page(state, inner, buf, ctx);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noaa() -> Arc<Dataset> {
        Arc::new(
            crate::catalog::bundled()
                .datasets
                .into_iter()
                .find(|d| d.id == "noaa")
                .unwrap(),
        )
    }

    fn page(entry: Arc<Dataset>) -> Documented {
        Documented {
            catalog: Some(("Public datasets".into(), entry)),
            ..Documented::default()
        }
    }

    fn screen(state: &mut DocState, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_view(state, area, &mut buf, &ctx);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn a_link_is_one_line_cut_never_wrapped() {
        let mut state = DocState::default();
        state.open(page(noaa()), None);
        let rows = screen(&mut state, 50, 40);
        let text = rows.join("\n");
        assert!(text.contains("publisher"), "{text}");
        let docs: Vec<&String> = rows
            .iter()
            .filter(|r| r.contains("documentation"))
            .collect();
        assert_eq!(docs.len(), 1, "{text}");
        assert!(docs[0].contains(glyphs::get().ellipsis), "{text}");
        assert!(!text.contains("readme.txt"), "{text}");
        assert!(text.contains("LINKS"), "{text}");
    }

    #[test]
    fn y_copies_the_whole_link_and_enter_opens_a_legend() {
        let mut state = DocState::default();
        state.open(page(noaa()), None);
        while !matches!(
            state.lines()[state.cursor],
            DocLine::Link("documentation", _)
        ) {
            state.move_cursor(1);
        }
        assert_eq!(
            state.copy_text().as_deref(),
            Some("https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt")
        );
        while !matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "ELEMENT")
        {
            state.move_cursor(1);
        }
        assert!(state.toggle_legend());
        let text = screen(&mut state, 100, 60).join("\n");
        assert!(text.contains("PRCP"), "{text}");
        state.move_cursor(3);
        assert!(state.toggle_legend(), "closes from inside the legend");
        assert!(!screen(&mut state, 100, 60).join("\n").contains("PRCP"));
        assert!(
            matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "ELEMENT")
        );
    }

    const ORDERS: &str = r#"
name = "acme.orders"
description = "Order entry capture"
documentation = "https://example.com/orders.pdf"
match = { glob = "*.ord" }

[records]
framing = "length_prefixed"
size = "len"
type = "kind"
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]

[[variants]]
name = "add"
when = "A"
description = "An order added to the book"
fields = [
  { name = "price", type = "u4", scale = 4, description = "Limit price", unit = "USD" },
  { name = "side", type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
]

[[variants]]
name = "exec"
when = ["E", "C"]
fields = [{ name = "shares", type = "u4", description = "Shares executed" }]
"#;

    fn spec_page(text: &str) -> Documented {
        let spec = crate::formats::Spec::parse(text, None).unwrap();
        Documented::new(None, spec.docs().map(Arc::new), "day.ord".into()).unwrap()
    }

    #[test]
    fn a_spec_read_from_a_file_names_its_file() {
        let file = dirs::home_dir().unwrap().join("specs").join("orders.toml");
        let spec = crate::formats::Spec::parse(ORDERS, Some(&file)).unwrap();
        let doc = Documented::new(None, spec.docs().map(Arc::new), "day.ord".into()).unwrap();
        let page = lines(&doc, &HashSet::new(), None);
        let at = |line: &DocLine| page.iter().position(|l| l == line);
        let name = at(&DocLine::Field("format spec", "acme.orders".into())).unwrap();
        let shown = crate::home::display_path(&file);
        assert!(shown.starts_with('~'), "{shown}");
        assert_eq!(at(&DocLine::Field("spec file", shown)), Some(name + 1));
        // Parsed from text, there is no file to name.
        let parsed = lines(&spec_page(ORDERS), &HashSet::new(), None);
        assert!(
            !parsed
                .iter()
                .any(|l| matches!(l, DocLine::Field("spec file", _)))
        );
    }

    #[test]
    fn a_variant_specs_page_lists_record_types_columns_and_legends() {
        let mut state = DocState::default();
        state.open(spec_page(ORDERS), None);
        let m = glyphs::get().middot;
        let lines = state.lines();
        assert_eq!(lines[0], DocLine::About("Order entry capture".into()));
        assert!(lines.contains(&DocLine::Field("format spec", "acme.orders".into())));
        assert!(lines.contains(&DocLine::Link(
            "documentation",
            "https://example.com/orders.pdf".into()
        )));
        assert!(lines.contains(&DocLine::Section("RECORD TYPES", 2)));
        assert!(lines.contains(&DocLine::RecordType(
            "add".into(),
            format!("kind = \"A\" {m} 4 columns {m} An order added to the book")
        )));
        assert!(lines.contains(&DocLine::RecordType(
            "exec".into(),
            format!("kind in (\"E\", \"C\") {m} 3 columns")
        )));
        assert!(lines.contains(&DocLine::Section("COLUMNS", 3)));
        assert!(lines.contains(&DocLine::Column {
            name: "price".into(),
            about: "Limit price (USD)".into(),
            values: 0,
        }));
        // The enum is the column's legend, opened with Enter.
        while !matches!(&state.lines()[state.cursor], DocLine::Column { name, .. } if name == "side")
        {
            state.move_cursor(1);
        }
        assert!(state.toggle_legend());
        assert!(
            state
                .lines()
                .contains(&DocLine::Legend("2".into(), "SELL".into()))
        );
        let text = screen(&mut state, 100, 40).join("\n");
        assert!(text.contains("Documentation"), "{text}");
        assert!(text.contains("day.ord"), "{text}");
        assert!(text.contains("RECORD TYPES"), "{text}");
        assert!(!text.contains("catalog"), "{text}");
    }

    #[test]
    fn a_delimited_specs_page_lists_its_column_notes() {
        let doc = spec_page(
            r#"
name = "acme.log"
kind = "delimited"
description = "Instrument log"

[columns]
temp = { description = "Air temperature", unit = "deg F" }
"#,
        );
        let lines = lines(&doc, &HashSet::new(), None);
        assert!(
            !lines
                .iter()
                .any(|l| matches!(l, DocLine::Section("RECORD TYPES", _)))
        );
        assert!(lines.contains(&DocLine::Column {
            name: "temp".into(),
            about: "Air temperature (deg F)".into(),
            values: 0,
        }));
    }

    /// A declared type stands beside the unit, as the type row pairs them.
    #[test]
    fn a_typed_column_shows_its_type_beside_its_unit() {
        let doc = spec_page(
            r#"
name = "acme.log"
kind = "delimited"

[columns]
Latitude = { type = "f64", unit = "deg", description = "GPS latitude" }
LogIdx = { type = "i64" }
"#,
        );
        let lines = lines(&doc, &HashSet::new(), None);
        assert!(lines.contains(&DocLine::Column {
            name: "Latitude".into(),
            about: "f64 · deg  GPS latitude".into(),
            values: 0,
        }));
        assert!(lines.contains(&DocLine::Column {
            name: "LogIdx".into(),
            about: "i64".into(),
            values: 0,
        }));
    }

    #[test]
    fn a_catalogs_word_stands_over_the_specs() {
        let catalog = crate::catalog::parse(
            r#"
label = "Mine"

[orders]
name = "Orders"
path = "/data/day.ord"
description = "Orders from the lab"
documentation = "https://example.com/lab.txt"
columns.price = { description = "Price the lab quotes" }
columns.venue = { description = "Where it traded" }
"#,
            "mine",
            crate::catalog::Origin::Mine,
            None,
        )
        .unwrap();
        let entry = Arc::new(catalog.datasets.into_iter().next().unwrap());
        let spec = crate::formats::Spec::parse(ORDERS, None).unwrap();
        let doc = Documented::new(
            Some(("Mine".into(), entry)),
            spec.docs().map(Arc::new),
            "day.ord".into(),
        )
        .unwrap();
        assert_eq!(doc.title(), "Orders");
        let lines = lines(&doc, &HashSet::new(), None);
        assert_eq!(lines[0], DocLine::About("Orders from the lab".into()));
        assert!(lines.contains(&DocLine::Field("catalog", "Mine".into())));
        assert!(lines.contains(&DocLine::Field("format spec", "acme.orders".into())));
        let links: Vec<&DocLine> = lines
            .iter()
            .filter(|l| matches!(l, DocLine::Link("documentation", _)))
            .collect();
        assert_eq!(
            links,
            [&DocLine::Link(
                "documentation",
                "https://example.com/lab.txt".into()
            )]
        );
        assert!(lines.contains(&DocLine::Section("RECORD TYPES", 2)));
        let columns: Vec<(String, String)> = lines
            .iter()
            .filter_map(|l| match l {
                DocLine::Column { name, about, .. } => Some((name.clone(), about.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            columns,
            [
                (
                    "price".to_string(),
                    "Price the lab quotes (USD)".to_string()
                ),
                ("side".into(), String::new()),
                ("shares".into(), "Shares executed".into()),
                ("venue".into(), "Where it traded".into()),
            ]
        );
    }

    #[test]
    fn a_catalog_note_keeps_the_specs_legend_and_unit() {
        let over = ColumnNote {
            description: "Side of the book".into(),
            ..ColumnNote::default()
        };
        let under = ColumnNote {
            description: "Side".into(),
            unit: "flag".into(),
            values: vec![("1".into(), "BUY".into())],
            ty: String::new(),
        };
        assert_eq!(
            layered(&over, &under),
            ColumnNote {
                description: "Side of the book".into(),
                ..under.clone()
            }
        );
        let legend = ColumnNote {
            values: vec![("B".into(), "Buy".into())],
            ..ColumnNote::default()
        };
        assert_eq!(layered(&legend, &under).values, legend.values);
        assert_eq!(layered(&legend, &under).description, "Side");
    }

    #[test]
    fn header_and_footer_fields_are_documented_in_their_own_sections() {
        let doc = spec_page(
            r#"
name = "acme.tape"
match = { glob = "*.tape" }

[header]
fields = [
  { name = "magic", type = "str", size = 4 },
  { type = "pad", size = 4 },
  { name = "trade_date", type = "u4", description = "Session date" },
  { name = "tick", type = "u4", unit = "ns" },
]

[records]
fields = [{ name = "px", type = "u4", description = "Price" }]

[footer]
fields = [{ name = "rows", type = "u4", description = "Records written" }]
"#,
        );
        let lines = lines(&doc, &HashSet::new(), None);
        let at = |line: &DocLine| lines.iter().position(|l| l == line).unwrap();
        let column = |name: &str, about: &str| DocLine::Column {
            name: name.into(),
            about: about.into(),
            values: 0,
        };
        let header = at(&DocLine::Section("HEADER", 2));
        assert_eq!(
            lines[header + 1..header + 3],
            [column("trade_date", "Session date"), column("tick", "ns")]
        );
        let columns = at(&DocLine::Section("COLUMNS", 1));
        let footer = at(&DocLine::Section("FOOTER", 1));
        assert!(header < columns && columns < footer);
        assert_eq!(lines[footer + 1], column("rows", "Records written"));
        // Nothing documented in the footer, no FOOTER section.
        let bare = spec_page(
            "name = \"a.b\"\n[header]\nfields = [{ name = \"v\", type = \"u1\", description = \"Version\" }]\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]\n[footer]\nfields = [{ name = \"n\", type = \"u4\" }]\n",
        );
        let lines = super::lines(&bare, &HashSet::new(), None);
        assert!(lines.contains(&DocLine::Section("HEADER", 1)));
        assert!(
            !lines
                .iter()
                .any(|l| matches!(l, DocLine::Section("FOOTER", _)))
        );
    }

    #[test]
    fn the_cursor_stays_in_view_and_bookmarks_are_listed() {
        let mut state = DocState::default();
        state.open(page(noaa()), None);
        state.move_cursor(isize::MAX / 2);
        let text = screen(&mut state, 80, 16).join("\n");
        assert!(text.contains("Central Park, NY"), "{text}");
    }
}

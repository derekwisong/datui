//! The home screen.
//!
//! # Design
//!
//! This is the first screen datui has that is *designed* rather than assembled, so
//! the choices are deliberate and worth stating:
//!
//! - **No boxes.** The default TUI look is borders around everything; it reads as
//!   busy and dated. Structure comes from alignment and negative space instead, with
//!   a single vertical rule where two panes genuinely need separating.
//! - **One accent, spent carefully.** The theme's accent appears in four places: the
//!   wordmark, section headers, the selection marker, and the prompt. Everything else
//!   is primary, secondary or dim text. Restraint is what reads as professional.
//! - **Metadata right-aligns into columns.** Ragged trailing text looks accidental;
//!   a clean column looks designed, and it makes datasets comparable at a glance.
//! - **Typed columns are the signature.** The schema preview colours each type with
//!   the same palette the table uses. No file picker tells you the shape of your data
//!   before you open it — this is the thing worth being known for, and it is why the
//!   preview pane earns its width.
//!
//! Everything is drawn from the active theme, so on Omarchy this inherits the desktop
//! palette and sits in the same visual family as the rest of the system.

use crate::discover::{self, Entry, EntryKind};
use crate::glyphs;
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Width reserved for the right-aligned metadata columns in the list.
const META_WIDTH: u16 = 34;
/// Below this the preview pane crowds the list out; drop it.
const PREVIEW_MIN_WIDTH: u16 = 92;
/// Below this even the metadata columns have to go.
const META_MIN_WIDTH: u16 = 56;

/// Truncate from the left, keeping the tail, to at most `width` display columns.
///
/// Paths are identified by their leaf, so the end is the part worth keeping. The
/// ellipsis is measured rather than assumed to be one column: it is three characters
/// wide when datui has fallen back to ASCII.
fn truncate_start(text: &str, width: usize) -> String {
    let len = text.chars().count();
    if len <= width {
        return text.to_string();
    }
    let ellipsis = glyphs::get().ellipsis;
    let ellipsis_width = ellipsis.chars().count();
    if width <= ellipsis_width {
        return text.chars().skip(len - width).collect();
    }
    let tail: String = text.chars().skip(len - (width - ellipsis_width)).collect();
    format!("{ellipsis}{tail}")
}

/// Colour a Polars type with the palette the table uses, so a column reads the same
/// here as it will once opened.
fn type_color(dtype: &polars::prelude::DataType, ctx: &RenderContext) -> ratatui::style::Color {
    use polars::prelude::DataType as D;
    match dtype {
        D::String | D::Categorical(_, _) | D::Enum(_, _) => ctx.str_col,
        D::Int8 | D::Int16 | D::Int32 | D::Int64 | D::Int128 => ctx.int_col,
        D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => ctx.int_col,
        D::Float32 | D::Float64 | D::Decimal(_, _) => ctx.float_col,
        D::Boolean => ctx.bool_col,
        D::Date | D::Datetime(_, _) | D::Duration(_) | D::Time => ctx.temporal_col,
        D::Binary | D::BinaryOffset => ctx.binary_col,
        _ => ctx.text_secondary,
    }
}

/// The three metadata columns, already padded to fixed widths so they align down the
/// list. Empty strings where a fact is genuinely unknown — a CSV's row count cannot
/// be had without scanning it, and inventing one would be worse than a blank.
fn meta_columns(entry: &Entry) -> String {
    let shape = match (entry.rows, entry.cols) {
        (Some(r), Some(c)) => format!("{} {} {}", discover::format_rows(r), glyphs::get().times, c),
        _ => String::new(),
    };
    let size = entry.size.map(discover::format_size).unwrap_or_default();
    let age = entry.modified.map(discover::format_age).unwrap_or_default();
    format!("{shape:>13}  {size:>9}  {age:>4}")
}

pub fn render(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    Clear.render(area, buf);

    // Generous left margin and a breath at the top: the whole design rests on space.
    let padded = Rect {
        x: area.x.saturating_add(2),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(4),
        height: area.height.saturating_sub(1),
    };
    if padded.width < 20 || padded.height < 6 {
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // wordmark
            Constraint::Length(1), // spacer
            Constraint::Length(1), // prompt
            Constraint::Length(1), // spacer
            Constraint::Fill(1),   // body
        ])
        .split(padded);

    render_wordmark(rows[0], buf, app, ctx);
    render_prompt(rows[2], buf, app, ctx);

    let show_preview = padded.width >= PREVIEW_MIN_WIDTH;
    if show_preview {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),
                Constraint::Length(3), // the rule and its gutters
                Constraint::Length(42),
            ])
            .split(rows[4]);
        render_list(body[0], buf, app, ctx);
        render_rule(body[1], buf, ctx);
        render_preview(body[2], buf, app, ctx);
    } else {
        render_list(rows[4], buf, app, ctx);
    }
}

/// `datui` at rest, with the current location trailing right. Identity without noise.
fn render_wordmark(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    let location = app
        .home
        .browsing
        .as_ref()
        .map(|p| crate::home::display_path(p))
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| crate::home::display_path(&p))
        })
        .unwrap_or_default();

    let left = Span::styled(
        "datui",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    );
    // Keep the tail of a long path; the leaf is what tells you where you are.
    let location = truncate_start(&location, (area.width as usize).saturating_sub(8));
    let pad = (area.width as usize).saturating_sub(5 + location.chars().count());
    Paragraph::new(Line::from(vec![
        left,
        Span::raw(" ".repeat(pad)),
        Span::styled(location, Style::default().fg(ctx.dimmed)),
    ]))
    .render(area, buf);
}

fn render_prompt(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    let home = &app.home;
    let g = glyphs::get();
    let (glyph, value, hint) = if home.path_input_active {
        (
            "~ ",
            home.path_input.as_str(),
            "path to a file or directory",
        )
    } else {
        (g.prompt, home.filter.as_str(), "type to filter")
    };

    let mut spans = vec![Span::styled(
        glyph,
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    )];
    spans.push(Span::styled(value, Style::default().fg(ctx.text_primary)));
    spans.push(Span::styled(
        g.cursor,
        Style::default().fg(ctx.keybind_hints),
    ));
    if value.is_empty() {
        spans.push(Span::styled(
            format!("  {hint}"),
            Style::default().fg(ctx.dimmed),
        ));
    }
    if let Some(status) = &home.status {
        spans.push(Span::styled(
            format!("   {status}"),
            Style::default().fg(ctx.warning),
        ));
    }
    Paragraph::new(Line::from(spans)).render(area, buf);
}

/// A single hairline between list and preview. One rule, not two borders.
fn render_rule(area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    if area.width == 0 {
        return;
    }
    let x = area.x + area.width / 2;
    for y in area.y..area.y.saturating_add(area.height) {
        if let Some(cell) = buf.cell_mut((x, y)) {
            cell.set_symbol(glyphs::get().rule);
            cell.set_fg(ctx.column_separator);
        }
    }
}

fn render_list(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    let visible = app.home.visible();

    if visible.is_empty() {
        let lines = if app.home.filter.is_empty() {
            vec![
                Line::from(Span::styled(
                    "Nothing here yet.",
                    Style::default().fg(ctx.text_secondary),
                )),
                Line::from(""),
                Line::from(Span::styled(
                    "Press ~ to open a path directly — datui remembers where it leads.",
                    Style::default().fg(ctx.dimmed),
                )),
                Line::from(Span::styled(
                    "Or set [data] directories in your config for places you visit often.",
                    Style::default().fg(ctx.dimmed),
                )),
            ]
        } else {
            vec![Line::from(Span::styled(
                "No match.",
                Style::default().fg(ctx.dimmed),
            ))]
        };
        Paragraph::new(lines).render(area, buf);
        return;
    }

    let show_meta = area.width >= META_MIN_WIDTH;
    let name_width = if show_meta {
        area.width.saturating_sub(META_WIDTH) as usize
    } else {
        area.width as usize
    };

    // Headers appear only in the unfiltered listing: once results are ranked across
    // sections, grouping them would be a lie about the order.
    let grouped = app.home.filter.is_empty();
    let mut lines: Vec<(Option<usize>, Line)> = Vec::new();
    let mut last_section: Option<usize> = None;

    for (idx, (si, entry)) in visible.iter().enumerate() {
        if grouped && last_section != Some(*si) {
            let section = &app.home.sections[*si];
            if !lines.is_empty() {
                lines.push((None, Line::from("")));
            }
            lines.push((None, section_header(section, area.width as usize, ctx)));
            last_section = Some(*si);
        }
        lines.push((
            Some(idx),
            entry_line(entry, idx == app.home.selected, name_width, show_meta, ctx),
        ));
    }

    let height = area.height as usize;
    let sel_line = lines
        .iter()
        .position(|(i, _)| *i == Some(app.home.selected))
        .unwrap_or(0);
    // Keep a little context above the selection rather than pinning it to the edge.
    let scroll = sel_line.saturating_sub(height.saturating_sub(3).max(1));

    let body: Vec<Line> = lines.into_iter().skip(scroll).map(|(_, l)| l).collect();
    Paragraph::new(body).render(area, buf);
}

/// Section headers are small caps in the accent, with the provenance trailing right —
/// so the list explains itself without a legend.
fn section_header<'a>(
    section: &'a crate::home::Section,
    width: usize,
    ctx: &RenderContext,
) -> Line<'a> {
    // Labels like "Recent" read as small caps; a filesystem path does not, and
    // shouting a path is both ugly and harder to read.
    let note = if section.unavailable {
        "unavailable".to_string()
    } else {
        section.subtitle.clone().unwrap_or_default()
    };
    let is_path = section.title.starts_with('/') || section.title.starts_with('~');
    let mut title = if is_path {
        section.title.clone()
    } else {
        section.title.to_uppercase()
    };
    // Keep the tail of a long path: the leaf is what identifies it. Budget against
    // the note that shares this line, plus a gap, so the two can never collide — and
    // count the ellipsis itself, which is three characters wide in ASCII mode.
    title = truncate_start(&title, width.saturating_sub(note.chars().count() + 3));
    let pad = width.saturating_sub(title.chars().count() + note.chars().count() + 1);
    Line::from(vec![
        Span::styled(
            title,
            Style::default()
                .fg(ctx.keybind_hints)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" ".repeat(pad)),
        Span::styled(
            note,
            Style::default().fg(if section.unavailable {
                ctx.warning
            } else {
                ctx.dimmed
            }),
        ),
    ])
}

fn entry_line<'a>(
    entry: &'a Entry,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    ctx: &RenderContext,
) -> Line<'a> {
    // The selection marker is the loudest thing on screen, and the only thing that
    // needs to be found instantly.
    let g = glyphs::get();
    let marker = if selected {
        g.selector
    } else {
        g.selector_blank
    };

    let mut name = entry.name.clone();
    if entry.kind == EntryKind::Directory {
        name.push('/');
    }
    let kind = entry.kind.label();
    let kind_cell = if kind.is_empty() {
        String::new()
    } else {
        format!(" {kind}")
    };

    // Truncate the name, never the metadata: the columns must stay aligned.
    let budget = name_width.saturating_sub(2 + kind_cell.chars().count() + 1);
    if name.chars().count() > budget && budget > 1 {
        name = name.chars().take(budget - 1).collect::<String>() + g.ellipsis;
    }
    let pad = name_width.saturating_sub(2 + name.chars().count() + kind_cell.chars().count());

    let name_style = if selected {
        Style::default()
            .fg(ctx.text_primary)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.text_primary)
    };
    let kind_style = match entry.kind {
        EntryKind::Hive => Style::default().fg(ctx.temporal_col),
        EntryKind::MultiFile => Style::default().fg(ctx.float_col),
        _ => Style::default().fg(ctx.dimmed),
    };

    let mut spans = vec![
        Span::styled(
            marker,
            Style::default()
                .fg(ctx.keybind_hints)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(name, name_style),
        Span::styled(kind_cell, kind_style),
    ];
    if show_meta {
        spans.push(Span::raw(" ".repeat(pad)));
        spans.push(Span::styled(
            meta_columns(entry),
            Style::default().fg(if selected {
                ctx.text_secondary
            } else {
                ctx.dimmed
            }),
        ));
    }
    Line::from(spans)
}

fn render_preview(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    let Some(entry) = app.home.selected_entry() else {
        return;
    };

    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            entry.name.clone(),
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            crate::home::display_path(&entry.path),
            Style::default().fg(ctx.dimmed),
        )),
        Line::from(""),
    ];

    match app.home_schema(&entry) {
        Some(schema) if !schema.is_empty() => {
            lines.push(Line::from(Span::styled(
                format!("{} COLUMNS", schema.len()),
                Style::default()
                    .fg(ctx.keybind_hints)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(""));

            let name_w = schema
                .iter()
                .map(|(n, _)| n.chars().count())
                .max()
                .unwrap_or(0)
                .min(22);
            let room = (area.height as usize).saturating_sub(lines.len() + 1);
            for (name, dtype) in schema.iter().take(room) {
                let mut display = name.clone();
                if display.chars().count() > name_w {
                    display = display.chars().take(name_w - 1).collect::<String>()
                        + glyphs::get().ellipsis;
                }
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{display:<name_w$}  "),
                        Style::default().fg(ctx.text_secondary),
                    ),
                    Span::styled(
                        format!("{dtype}"),
                        Style::default().fg(type_color(dtype, ctx)),
                    ),
                ]));
            }
            if schema.len() > room {
                lines.push(Line::from(Span::styled(
                    format!("{} {} more", glyphs::get().ellipsis, schema.len() - room),
                    Style::default().fg(ctx.dimmed),
                )));
            }
        }
        _ => {
            let note: &[&str] = match entry.kind {
                EntryKind::Directory => &["Directory.", "Enter to look inside."],
                _ => &[
                    "Schema needs a scan for this format.",
                    "datui shows it once opened.",
                ],
            };
            for part in note {
                lines.push(Line::from(Span::styled(
                    *part,
                    Style::default().fg(ctx.dimmed),
                )));
            }
        }
    }

    Paragraph::new(lines)
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(area, buf);
}

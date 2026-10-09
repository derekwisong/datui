//! The home screen. Its design rules:
//!
//! - **No boxes.** Structure comes from alignment and space, with one vertical rule
//!   where two panes need separating.
//! - **One accent**, in four places: the wordmark, section headers, the selection
//!   marker and the prompt. Everything else is primary, secondary or dim text.
//! - **Metadata right-aligns into columns**, so datasets compare at a glance.
//! - **Typed columns**: the schema preview colors each type as the table does,
//!   showing a dataset's shape before it is opened.
//!
//! Everything is drawn from the active theme.

use crate::formats::MatchChip;
use crate::glyphs;
use crate::home::discover::{self, Entry, EntryKind};
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Width reserved for the right-aligned metadata columns in the list.
const META_WIDTH: u16 = 34;
/// Width given to the preview pane when there is room for it.
const PREVIEW_WIDTH: u16 = 40;
/// Below this the preview would squeeze the list under [`META_MIN_WIDTH`] and cost
/// every row its size and shape, so the preview yields first.
const PREVIEW_MIN_WIDTH: u16 = META_MIN_WIDTH + PREVIEW_WIDTH + 6;
/// Below this even the metadata columns have to go.
const META_MIN_WIDTH: u16 = 56;

/// The three metadata columns, padded to fixed widths to align down the list. Empty
/// where a fact is unknown (a CSV's row count needs a scan). `unmeasured` shows an
/// ellipsis for a dataset nothing has read yet. `hint` is a catalog's size
/// (`~33.0 MiB`) until measured; `gone` replaces the size for an unavailable web file
/// (`HTTP 404`, `no answer`).
fn meta_columns(entry: &Entry, unmeasured: bool, hint: Option<u64>, gone: Option<&str>) -> String {
    // A dataset too large to count still knows its width: `? x 158`.
    let times = glyphs::get().times;
    // A column count from a spread of a directory is a floor, marked `+`.
    let more = if entry.cols_sampled { "+" } else { "" };
    let shape = match (entry.rows, entry.cols) {
        (Some(r), Some(c)) => format!("{} {times} {c}{more}", discover::format_rows(r)),
        // A directory that is not one table has no row count (a sum over unrelated tables),
        // so its width is named alone: `72 cols`, not `× 72` or a bare `72` that reads as
        // rows.
        (None, Some(c)) if entry.kind == EntryKind::Directory => format!("{c}{more} cols"),
        (None, Some(c)) => format!("? {times} {c}{more}"),
        _ if unmeasured => glyphs::get().ellipsis.to_string(),
        _ => String::new(),
    };
    let size = match (gone, entry.size, hint) {
        (Some(gone), _, _) => gone.to_string(),
        (None, Some(size), _) => crate::numfmt::bytes(size),
        (None, None, Some(hint)) => format!("~{}", crate::numfmt::bytes(hint)),
        (None, None, None) => String::new(),
    };
    let age = entry.modified.map(discover::format_age).unwrap_or_default();
    format!("{shape:>13}  {size:>9}  {age:>4}")
}

pub fn render(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    Clear.render(area, buf);
    // Set again below where this frame has room for the selected file's first rows.
    app.home_app.preview_room = None;

    // One column of breathing room: vertical space is scarce.
    let padded = Rect {
        x: area.x.saturating_add(1),
        y: area.y,
        width: area.width.saturating_sub(2),
        height: area.height,
    };
    if padded.width < 20 || padded.height < 5 {
        return;
    }

    // The wordmark costs two rows and twenty-five columns over the title bar; on a short
    // or narrow terminal, or with `home.wordmark` off, the one-line bar comes back.
    let wordmark = glyphs::get().wordmark.filter(|_| {
        app.app_config.home.wordmark
            && padded.height >= WORDMARK_MIN_HEIGHT
            && padded.width >= WORDMARK_MIN_WIDTH
    });
    let title_h = wordmark.map(|w| w.len() as u16).unwrap_or(1);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(title_h), // title bar or wordmark
            Constraint::Length(1),       // prompt
            Constraint::Fill(1),         // body
        ])
        .split(padded);

    match wordmark {
        Some(lines) => render_wordmark(rows[0], buf, app, ctx, lines),
        None => render_title_bar(rows[0], buf, app, ctx),
    }
    render_prompt(rows[1], buf, app, ctx);

    let show_preview = padded.width >= PREVIEW_MIN_WIDTH;
    if show_preview {
        // The list keeps a reading measure on wide screens, so a row's facts sit near its
        // name; the pane takes the rest.
        let list_w = padded
            .width
            .saturating_sub(3 + PREVIEW_WIDTH)
            .min(LIST_MAX_WIDTH);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(list_w),
                Constraint::Length(3), // the rule and its gutters
                Constraint::Fill(1),
            ])
            .split(rows[2]);
        render_list(body[0], buf, app, ctx);
        render_rule(body[1], buf, ctx);
        render_preview(body[2], buf, app, ctx, area.height);
    } else {
        let used = render_list(rows[2], buf, app, ctx);
        render_rows_strip(rows[2], used, buf, app, ctx, area.height);
    }
}

/// The widest the list gets: past it a row's facts drift from its name.
const LIST_MAX_WIDTH: u16 = 84;

/// The fewest rows the bottom strip is drawn in: heading, column names, two rows.
const STRIP_MIN_HEIGHT: usize = 4;

/// Below the pane's width, the selected file's first rows in rows the list leaves free
/// at the bottom. Never over the list: with no spare rows there is no strip, and
/// nothing is read.
fn render_rows_strip(
    area: Rect,
    used: usize,
    buf: &mut Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
    screen_height: u16,
) {
    let free = (area.height as usize).saturating_sub(used);
    if free < STRIP_MIN_HEIGHT || app.home.path_input_active {
        return;
    }
    let Some(entry) = app.home.selected_entry().cloned() else {
        return;
    };
    app.home_app.preview_room = Some(screen_height);
    let Some(preview) = app.home_preview_rows(&entry) else {
        return;
    };
    // A blank line between the list and the strip when there is one to spare.
    let room = free.saturating_sub(1).max(STRIP_MIN_HEIGHT);
    let lines = rows_block(&preview, area.width as usize, room, ctx);
    let height = lines.len() as u16;
    let strip = Rect {
        x: area.x,
        y: area.y + area.height - height,
        width: area.width,
        height,
    };
    Paragraph::new(lines).render(strip, buf);
}

/// The `ROWS` block: a heading, the leading columns' names in their type colors, and
/// as many first rows and columns as `room` and `width` fit.
fn rows_block(
    preview: &crate::home::home_preview::PreviewRows,
    width: usize,
    room: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    const CELL_W: usize = 20;
    const GAP: usize = 2;
    let g = glyphs::get();
    let null_w = glyphs::display_width(g.null);
    // Each column as wide as its name or its widest cell shown, up to a cap.
    let widths: Vec<usize> = preview
        .columns
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            preview
                .rows
                .iter()
                .map(|row| match &row[i] {
                    Some(text) => glyphs::display_width(text),
                    None => null_w,
                })
                .chain([glyphs::display_width(name)])
                .max()
                .unwrap_or(1)
                .clamp(1, CELL_W)
        })
        .collect();
    let mut shown = 0;
    let mut used = 0;
    for w in &widths {
        let need = if shown == 0 { *w } else { GAP + *w };
        if used + need > width {
            break;
        }
        used += need;
        shown += 1;
    }
    // A first column wider than the space is cut to it rather than left out.
    let widths: Vec<usize> = if shown == 0 {
        vec![width.max(1)]
    } else {
        widths[..shown].to_vec()
    };
    let shown = widths.len().min(preview.columns.len());
    let mut lines = vec![rows_heading(shown, preview.total_columns, width, ctx)];
    let cell = |text: &str, w: usize, right: bool| -> String {
        let text = if glyphs::display_width(text) > w {
            let cut =
                glyphs::take_columns(text, w.saturating_sub(glyphs::display_width(g.ellipsis)));
            format!("{cut}{}", g.ellipsis)
        } else {
            text.to_string()
        };
        let pad = w.saturating_sub(glyphs::display_width(&text));
        if right {
            format!("{}{text}", " ".repeat(pad))
        } else {
            format!("{text}{}", " ".repeat(pad))
        }
    };
    let numeric: Vec<bool> = preview.columns[..shown]
        .iter()
        .map(|(_, dtype)| crate::numfmt::is_numeric_dtype(dtype))
        .collect();
    let mut header = Vec::new();
    for (i, (name, dtype)) in preview.columns[..shown].iter().enumerate() {
        if i > 0 {
            header.push(Span::raw(" ".repeat(GAP)));
        }
        header.push(Span::styled(
            cell(name, widths[i], numeric[i]),
            Style::default().fg(ctx.type_color(dtype)),
        ));
    }
    lines.push(Line::from(header));
    let rows_room = room.saturating_sub(lines.len());
    for row in preview.rows.iter().take(rows_room) {
        let mut spans = Vec::new();
        for (i, value) in row[..shown].iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" ".repeat(GAP)));
            }
            spans.push(match value {
                Some(text) => Span::styled(
                    cell(text, widths[i], numeric[i]),
                    Style::default().fg(ctx.text_primary),
                ),
                None => Span::styled(
                    cell(g.null, widths[i], numeric[i]),
                    Style::default().fg(ctx.dimmed),
                ),
            });
        }
        lines.push(Line::from(spans));
    }
    lines
}

/// `ROWS` on a rule, and how many of the columns the block shows when not all of them.
fn rows_heading(shown: usize, total: usize, width: usize, ctx: &RenderContext) -> Line<'static> {
    let note = (shown < total).then(|| format!("{shown} of {total} columns"));
    pane_heading_counted("ROWS", note.as_deref(), width, ctx)
}

/// Below this many rows the wordmark gives way to the one-line title bar.
const WORDMARK_MIN_HEIGHT: u16 = 28;
/// Below this width the wordmark leaves the path beside it only an ellipsis.
const WORDMARK_MIN_WIDTH: u16 = 52;

/// Interpolate two colors when both are RGB; otherwise (named, indexed, default) the
/// first stop, so the wordmark is the accent.
fn mix(a: Color, b: Color, t: f32) -> Color {
    match (a, b) {
        (Color::Rgb(r1, g1, b1), Color::Rgb(r2, g2, b2)) => {
            let lerp =
                |x: u8, y: u8| -> u8 { (x as f32 + (y as f32 - x as f32) * t).round() as u8 };
            Color::Rgb(lerp(r1, r2), lerp(g1, g2), lerp(b1, b2))
        }
        _ => a,
    }
}

/// The wordmark: three rows of box drawing colored along the theme's gradient (the
/// one place it is used), with the location beside the middle row.
fn render_wordmark(
    area: Rect,
    buf: &mut Buffer,
    app: &crate::App,
    ctx: &RenderContext,
    lines: &[&str],
) {
    let location = app
        .home
        .browsing
        .as_ref()
        .map(|p| app.home.location_label(p))
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| crate::home::display_path(&p))
        })
        .unwrap_or_default();
    let mark_w = lines
        .iter()
        .map(|l| glyphs::display_width(l))
        .max()
        .unwrap_or(0);
    let start = ctx.gradient_start;
    let end = ctx.gradient_end;
    for (row, line) in lines.iter().enumerate() {
        if row as u16 >= area.height {
            break;
        }
        let mut spans: Vec<Span> = Vec::with_capacity(mark_w + 2);
        spans.push(Span::raw(" "));
        for (i, ch) in line.chars().enumerate() {
            let t = if mark_w > 1 {
                i as f32 / (mark_w - 1) as f32
            } else {
                0.0
            };
            spans.push(Span::styled(
                ch.to_string(),
                Style::default()
                    .fg(mix(start, end, t))
                    .add_modifier(Modifier::BOLD),
            ));
        }
        if row == lines.len() / 2 {
            let room = (area.width as usize).saturating_sub(mark_w + 4);
            spans.push(Span::styled(
                format!("   {}", glyphs::fit_start(&location, room)),
                Style::default().fg(ctx.text_secondary),
            ));
        }
        Paragraph::new(Line::from(spans)).render(
            Rect {
                x: area.x,
                y: area.y + row as u16,
                width: area.width,
                height: 1,
            },
            buf,
        );
    }
}

/// A filled bar with the name and location, mirroring the footer so the list sits
/// between two anchors.
fn render_title_bar(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    let location = app
        .home
        .browsing
        .as_ref()
        .map(|p| app.home.location_label(p))
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| crate::home::display_path(&p))
        })
        .unwrap_or_default();

    let bar = Style::default().bg(ctx.controls_bg);
    let left = Span::styled(
        " datui ",
        bar.fg(ctx.keybind_hints).add_modifier(Modifier::BOLD),
    );
    // Keep the tail of a long path; the leaf is what tells you where you are.
    let location = glyphs::fit_start(&location, (area.width as usize).saturating_sub(12));
    let pad = (area.width as usize).saturating_sub(8 + glyphs::display_width(&location));
    Paragraph::new(Line::from(vec![
        left,
        Span::styled(" ".repeat(pad), bar),
        Span::styled(location, bar.fg(ctx.text_secondary)),
        Span::styled(" ", bar),
    ]))
    .render(area, buf);
}

fn render_prompt(area: Rect, buf: &mut Buffer, app: &crate::App, ctx: &RenderContext) {
    let home = &app.home;
    let g = glyphs::get();
    // Placeholders name what the field takes; the filter's says the non-obvious part
    // (typing also searches below here).
    let (glyph, value, hint) = if home.path_input_active {
        (" ~ ", home.path_input.as_str(), "file or directory")
    } else {
        (g.prompt, home.filter.as_str(), "narrow and search")
    };

    let mut spans = vec![Span::styled(
        glyph,
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    )];
    // A filter kept from before is shown selected: typing replaces it.
    let value_style = if !home.path_input_active && home.filter_selected {
        app.theme.text_selection_style()
    } else {
        Style::default().fg(ctx.text_primary)
    };
    spans.push(Span::styled(value, value_style));
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
        // Keep the tail ("\"<long path>\": <reason>"): the reason matters; the row shows the
        // name.
        let used: usize = spans
            .iter()
            .map(|s| glyphs::display_width(&s.content))
            .sum();
        let room = (area.width as usize).saturating_sub(used + 3);
        spans.push(Span::styled(
            format!("   {}", glyphs::fit_start(status, room)),
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

/// Draw the list; returns the lines used, or all of them when a message was drawn.
fn render_list(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) -> usize {
    if app.home.path_input_active {
        return render_path_list(area, buf, app, ctx);
    }
    // This frame's start and row room, settled before borrowing the listing: the scroll
    // and what to look into both derive from it.
    let height = area.height as usize;
    // With room, a blank line before every header but the first (at most four); under
    // thirty lines the list is dense. Spacers come off the height RECENT's cap uses.
    let spaced = height >= SPACED_LIST_HEIGHT;
    let headers = app.home.list_lines(spaced).headers();
    let spacers = if spaced { headers.saturating_sub(1) } else { 0 };
    // Height first: RECENT's cap is a share of it, and the cursor must be back on its
    // row before the scroll settles.
    app.home.set_view_height(height.saturating_sub(spacers));
    // Each row's line, spacers counted, so the scroll keeps the selected row on screen.
    let row_lines = app.home.list_lines(spaced);
    let line_of = |row: usize| (row < row_lines.rows()).then(|| row_lines.line_of(row));
    // The view stays where the last frame left it unless the cursor would leave it.
    let total = row_lines.total();
    let first_line = crate::home::settle_top(
        line_of(app.home.scroll).unwrap_or(total),
        line_of(app.home.selected).unwrap_or(0),
        height,
        total,
    );
    // As a row index, for the look-into passes and the next frame; a view starting on a
    // spacer starts on the header below.
    app.home.scroll = row_lines.first_row_from(first_line);
    let first_line = line_of(app.home.scroll).unwrap_or(first_line);
    // Which row each line of the list shows, for a click; a spacer shows none.
    let lines_drawn = (0..height)
        .map(|dy| row_lines.row_on(first_line + dy))
        .collect();
    app.pointer.home_list_drawn(area, lines_drawn);

    let awaiting = app.home.awaiting_listing().map(|d| d.to_path_buf());
    let since = match awaiting {
        Some(_) => Some(
            *app.home
                .waiting_since
                .get_or_insert_with(std::time::Instant::now),
        ),
        None => {
            app.home.waiting_since = None;
            None
        }
    };
    let listed = row_lines.rows();

    if let (Some(dir), Some(since), true) = (&awaiting, since, listed == 0) {
        let spinner = glyphs::get().spinner;
        let mut spans = vec![
            Span::styled(
                spinner[app.throbber_frame as usize % spinner.len()],
                Style::default().fg(ctx.accent),
            ),
            Span::raw(" "),
            Span::styled(
                format!("Listing {}", crate::home::display_path(dir)),
                Style::default().fg(ctx.text_secondary),
            ),
        ];
        let secs = since.elapsed().as_secs();
        if secs >= 1 {
            spans.push(Span::styled(
                format!("  {secs}s"),
                Style::default().fg(ctx.dimmed),
            ));
        }
        Paragraph::new(Line::from(spans)).render(area, buf);
        return area.height as usize;
    }

    if app.home.listing_in_flight && listed == 0 {
        Paragraph::new(Line::from(Span::styled(
            "Looking...",
            Style::default().fg(ctx.dimmed),
        )))
        .render(area, buf);
        return area.height as usize;
    }

    if listed == 0 && !app.home.filter.is_empty() {
        // Browsing, the empty answer says where it looked, or a miss reads as a bug.
        let text = match &app.home.browsing {
            Some(dir) => format!("No match under {}.", crate::home::display_path(dir)),
            None => "No match.".to_string(),
        };
        Paragraph::new(Line::from(Span::styled(
            text,
            Style::default().fg(ctx.dimmed),
        )))
        .render(area, buf);
        return area.height as usize;
    }

    // Guidance shows whenever nothing is openable, not only for an empty list, so a
    // first run is never a bare listing. Counted over every section, folded or not.
    let has_dataset = app.home.has_any_dataset();
    // Cloud sources are something to browse too, so they count as content.
    let guidance = if has_dataset || !app.home.filter.is_empty() || !app.home.cloud.is_empty() {
        Vec::new()
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "No datasets here.",
                Style::default().fg(ctx.text_secondary),
            )),
            Line::from(""),
            Line::from(vec![
                Span::styled("  ~  ", Style::default().fg(ctx.keybind_hints)),
                Span::styled("type a path", Style::default().fg(ctx.dimmed)),
            ]),
            Line::from(vec![
                Span::styled("     ", Style::default()),
                Span::styled(
                    "or list datasets in catalog.toml",
                    Style::default().fg(ctx.dimmed),
                ),
            ]),
        ]
    };

    let show_meta = area.width >= META_MIN_WIDTH;
    let name_width = if show_meta {
        area.width.saturating_sub(META_WIDTH) as usize
    } else {
        area.width as usize
    };

    let list = ListDraw {
        ctx,
        width: area.width as usize,
        name_width,
        show_meta,
        frame: app.throbber_frame as usize,
    };
    let mut lines: Vec<Line> = Vec::new();
    // Only rows on screen are built and drawn: a search may list thousands.
    let last_line = first_line + height;
    for idx in app.home.scroll..listed {
        let at = row_lines.line_of(idx);
        if at >= last_line {
            break;
        }
        let Some(row) = app.home.row_at(idx) else {
            break;
        };
        let spacer = spaced && idx > 0 && matches!(row, crate::home::Row::Header { .. });
        if spacer && at > first_line {
            lines.push(Line::from(""));
        }
        if at < first_line {
            continue;
        }
        let selected = idx == app.home.selected;
        match &row {
            crate::home::Row::Header {
                section,
                matches,
                collapsed,
            } => {
                lines.push(section_header(
                    &app.home.sections[*section],
                    *matches,
                    *collapsed,
                    selected,
                    &list,
                ));
            }
            crate::home::Row::Entry { entry, hit, .. }
                if crate::home::cloud_source_id(&entry.path).is_some() =>
            {
                let source = app.home.cloud_source_of(&entry.path);
                lines.push(source_line(
                    entry,
                    source,
                    selected,
                    area.width as usize,
                    app.throbber_frame as usize,
                    &hit.positions(&app.home.filter, entry),
                    ctx,
                ));
            }
            crate::home::Row::Entry {
                entry, nested, hit, ..
            } => {
                let marks = hit.positions(&app.home.filter, entry);
                lines.push(entry_line(
                    entry,
                    selected,
                    EntryNotes {
                        // A row matched by a column rather than its name says so.
                        matched_column: hit.column_of(entry),
                        marks: &marks,
                        // Sources a URL can name, from the config (not those listed so far, so hidden or
                        // undiscovered ones are not reported missing). Inside a source the trail names it.
                        known_sources: app
                            .home
                            .browsing
                            .is_none()
                            .then_some(app.app_config.cloud.connections.as_slice()),
                        place_kind: app.home.place_kind(&entry.path),
                        look: app.home.cloud_look(entry),
                        indent: if *nested { NEST_INDENT } else { 0 },
                        size_hint: app.home.size_hint(&entry.path),
                        gone: app.home.web_gone.get(&entry.path).map(|g| g.cell.as_str()),
                    },
                    &list,
                ));
            }
            // Drawn like an entry, but arrives by its own variant: it is not a row of the
            // directory.
            crate::home::Row::Door { entry, .. } => {
                lines.push(entry_line(
                    entry,
                    selected,
                    EntryNotes {
                        place_kind: app.home.place_kind(&entry.path),
                        look: app.home.cloud_look(entry),
                        ..EntryNotes::default()
                    },
                    &list,
                ));
            }
            crate::home::Row::Place {
                path,
                label,
                source,
                ..
            } => {
                lines.push(place_line(
                    path,
                    label.as_deref(),
                    source.as_deref(),
                    selected,
                    name_width,
                    show_meta,
                    ctx,
                ));
            }
            crate::home::Row::More {
                hidden,
                places,
                measuring,
                ..
            } => {
                lines.push(more_line(
                    *hidden, *places, *measuring, selected, name_width, show_meta, ctx,
                ));
            }
            crate::home::Row::Hidden { section, count } => {
                let tables = holds_tables(&app.home.sections[*section]);
                lines.push(hidden_line(
                    *count, tables, selected, name_width, show_meta, ctx,
                ));
            }
            crate::home::Row::Up { .. } => lines.push(note_row(
                glyphs::get().up.to_string(),
                selected,
                name_width,
                show_meta,
                ctx,
            )),
        }
    }

    let mut body: Vec<Line> = lines;
    body.extend(guidance);
    let used = body.len();
    Paragraph::new(body).render(area, buf);
    used
}

/// While `~` is typed, the list is the typed directory: names matching the last
/// segment, best first, with the ↑↓ pick on the rail.
fn render_path_list(
    area: Rect,
    buf: &mut Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) -> usize {
    let g = glyphs::get();
    let height = area.height as usize;
    let width = area.width as usize;
    // No row of the home list is under the pointer meanwhile.
    app.pointer.home_list_drawn(area, vec![None; height]);
    let home = &app.home;
    let dir = crate::home::typed_dir(&home.path_input);
    let segment = &home.path_input[dir.len()..];
    let listing = home.path_listing.as_ref().filter(|l| l.dir == dir);
    let candidates = home.path_candidates();
    let shown_dir = if dir.is_empty() { "./" } else { dir };
    let count = format!("{}", candidates.len());
    let rule_w =
        width.saturating_sub(glyphs::display_width(shown_dir) + glyphs::display_width(&count) + 6);
    let mut lines = vec![Line::from(vec![
        Span::styled(g.expanded, Style::default().fg(ctx.accent)),
        Span::styled(
            glyphs::fit_start(shown_dir, width.saturating_sub(count.len() + 8)),
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!("  {count}  "), Style::default().fg(ctx.dimmed)),
        Span::styled(
            g.rule_h.repeat(rule_w),
            Style::default().fg(ctx.column_separator),
        ),
    ])];
    let note = match listing {
        None => Some(format!("Listing {shown_dir}...")),
        Some(l) if l.failed => Some(format!("Nothing to list at {shown_dir}")),
        Some(_) if candidates.is_empty() && !segment.is_empty() => {
            Some(format!("No name here starts like {segment}"))
        }
        Some(_) if candidates.is_empty() => Some("Nothing here".to_string()),
        Some(_) => None,
    };
    if let Some(note) = note {
        lines.push(Line::from(Span::styled(
            format!("  {note}"),
            Style::default().fg(ctx.dimmed),
        )));
    }
    // The picked row stays on screen: the list scrolls under it.
    let room = height.saturating_sub(lines.len());
    let pick = home.path_pick;
    let first = match pick {
        Some(p) if p >= room.saturating_sub(1) => p + 2 - room.max(1),
        _ => 0,
    };
    let hidden_after = candidates.len().saturating_sub(first + room);
    let take = if hidden_after > 0 {
        room.saturating_sub(1)
    } else {
        room
    };
    for (i, name) in candidates.iter().enumerate().skip(first).take(take) {
        let selected = pick == Some(i);
        let base = if selected {
            ctx.highlight_style()
        } else {
            Style::default()
        };
        let marker = if selected {
            g.selector
        } else {
            g.selector_blank
        };
        let mut text = name.name.clone();
        if name.dir {
            text.push('/');
        }
        let positions = crate::home::fuzzy_positions(segment, &name.name);
        let name_style = if selected {
            base.fg(ctx.text_primary).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        let hit_style = base
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
        let mut spans = vec![Span::styled(
            marker,
            base.fg(ctx.keybind_hints).add_modifier(Modifier::BOLD),
        )];
        spans.extend(highlight_spans(&text, &positions, name_style, hit_style));
        let used = glyphs::display_width(marker) + glyphs::display_width(&text);
        spans.push(Span::styled(" ".repeat(width.saturating_sub(used)), base));
        lines.push(Line::from(spans));
    }
    if hidden_after > 0 {
        lines.push(Line::from(Span::styled(
            format!("  {} {hidden_after} more", g.ellipsis),
            Style::default().fg(ctx.dimmed),
        )));
    }
    let used = lines.len();
    Paragraph::new(lines).render(area, buf);
    used
}

/// How far a row under a place is indented: enough to read as "under".
const NEST_INDENT: usize = 2;

/// The list height from which a blank line precedes each section header but the
/// first.
const SPACED_LIST_HEIGHT: usize = 30;

/// [`meta_columns`]' width: shape, size and age right-aligned in fixed cells. Rows
/// without meta columns are drawn to the same edge.
const META_COLUMNS_WIDTH: usize = 13 + 2 + 9 + 2 + 4;
/// The shape cell and the gap after it, at the head of the meta columns.
const SHAPE_COLUMNS_WIDTH: usize = 13 + 2;

/// Where an entry row of `name_width` ends: marker, name, meta columns when shown,
/// one cell of air. Place and `more` rows share this edge.
fn row_width(name_width: usize, show_meta: bool) -> usize {
    let marker = glyphs::display_width(glyphs::get().selector_blank);
    marker + name_width + if show_meta { META_COLUMNS_WIDTH } else { 0 } + 1
}

/// A recents group's place: its path with a trailing slash and, at the right, its
/// filesystem. Quiet (secondary text, no locality glyph): a heading, not a row to
/// open.
fn place_line(
    path: &std::path::Path,
    label: Option<&str>,
    source: Option<&str>,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    ctx: &RenderContext,
) -> Line<'static> {
    let chrome = RowChrome::new(selected, ctx);
    let width = row_width(name_width, show_meta);
    let source = source.unwrap_or("").to_string();
    // Unknown or local: nothing to say. Only places that could be slow or cost money
    // name their filesystem, as the rows' glyph does.
    let locality = crate::home::locality::Locality::of_fstype(&source);
    let source = match locality {
        crate::home::locality::Locality::Object
        | crate::home::locality::Locality::Network
        | crate::home::locality::Locality::Memory => source,
        _ => String::new(),
    };
    let source_style = chrome.base.fg(locality_color(Some(locality), ctx));
    let mut name = crate::home::display_path(path);
    // A file of tables (a SQLite database, a NumPy archive) is a place, and a file.
    if !name.ends_with('/')
        && !crate::home::discover::data_format(path).is_some_and(crate::FileFormat::holds_tables)
    {
        name.push('/');
    }
    // What the dataset index remembers the place to be (`hive`, `12 parquet`); from the
    // cache only.
    let label = label.map(|l| format!("  {l}")).unwrap_or_default();
    // Marker, name, [label,] space, source, space: padding puts the source on the right
    // edge; the label goes before the name is cut.
    let fixed = glyphs::display_width(chrome.marker) + glyphs::display_width(&source) + 1;
    let room = width.saturating_sub(fixed + 1).max(1);
    let label = if glyphs::display_width(&name) + glyphs::display_width(&label) <= room {
        label
    } else {
        String::new()
    };
    let name = glyphs::fit_start(
        &name,
        room.saturating_sub(glyphs::display_width(&label)).max(1),
    );
    let pad =
        width.saturating_sub(fixed + glyphs::display_width(&name) + glyphs::display_width(&label));
    Line::from(vec![
        chrome.marker(),
        Span::styled(name, chrome.name(ctx.text_secondary)),
        Span::styled(label, chrome.base.fg(ctx.dimmed)),
        chrome.pad(pad),
        Span::styled(source, source_style),
        chrome.pad(1),
    ])
}

/// What a cap hides, as one row: `RECENT`'s `… 13 more in 5 places`, or a
/// directory's `… 4,958 more`, with `· measuring` while a rows sort still measures.
fn more_line(
    hidden: usize,
    places: usize,
    measuring: bool,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    ctx: &RenderContext,
) -> Line<'static> {
    let g = glyphs::get();
    let ellipsis = g.ellipsis;
    let more = crate::numfmt::group_chrome(hidden);
    let text = match places {
        0 if measuring => format!("{ellipsis} {more} more {} measuring", g.middot),
        0 => format!("{ellipsis} {more} more"),
        1 => format!("{ellipsis} {more} more in 1 place"),
        _ => format!("{ellipsis} {more} more in {places} places"),
    };
    note_row(text, selected, name_width, show_meta, ctx)
}

/// The row that stands for files datui cannot open, hidden inside a browsed directory.
fn hidden_line(
    count: usize,
    tables: bool,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    ctx: &RenderContext,
) -> Line<'static> {
    let ellipsis = glyphs::get().ellipsis;
    let text = match (tables, count) {
        (true, 1) => format!("{ellipsis} 1 internal table"),
        (true, _) => format!("{ellipsis} {count} internal tables"),
        (false, 1) => format!("{ellipsis} 1 file with no reader"),
        (false, _) => format!("{ellipsis} {count} files with no reader"),
    };
    note_row(text, selected, name_width, show_meta, ctx)
}

/// Whether a section lists a database's tables rather than files.
fn holds_tables(section: &crate::home::Section) -> bool {
    section.rows.iter().any(|row| row.table.is_some())
}

/// A dimmed row that stands for rows not drawn, to the same edge as the entries.
fn note_row(
    text: String,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    ctx: &RenderContext,
) -> Line<'static> {
    let chrome = RowChrome::new(selected, ctx);
    let width = row_width(name_width, show_meta);
    let pad = width
        .saturating_sub(glyphs::display_width(chrome.marker) + glyphs::display_width(&text) + 1);
    Line::from(vec![
        chrome.marker(),
        Span::styled(text, chrome.base.fg(ctx.dimmed)),
        chrome.pad(pad),
        chrome.pad(1),
    ])
}

/// What every home row is drawn with in one frame.
#[derive(Clone, Copy)]
struct ListDraw<'f> {
    ctx: &'f RenderContext,
    /// The list's whole width; a section header spans it.
    width: usize,
    /// What an entry's name may take beside the meta columns.
    name_width: usize,
    show_meta: bool,
    /// The throbber's frame, for a section still listing.
    frame: usize,
}

/// What one entry's row says beside its name, worked out by the caller.
#[derive(Default)]
struct EntryNotes<'a, 'm> {
    /// The column the filter matched, when the name did not.
    matched_column: Option<&'a str>,
    /// The matched characters: in the name, or in `matched_column` when set.
    marks: &'m [usize],
    /// Sources a URL may name; `None` where the trail already names it.
    known_sources: Option<&'a [crate::config::CloudConnectionConfig]>,
    place_kind: Option<&'static str>,
    /// Where a bucket directory is in being looked into.
    look: Option<crate::home::CloudLook>,
    indent: usize,
    /// What a catalog says the row's file weighs, until it is measured.
    size_hint: Option<u64>,
    /// Why a web file cannot be had, in place of its size.
    gone: Option<&'a str>,
}

/// Section headers carry the fold marker and provenance note, so the list explains
/// itself.
fn section_header<'a>(
    section: &'a crate::home::Section,
    matches: usize,
    collapsed: bool,
    selected: bool,
    list: &ListDraw,
) -> Line<'a> {
    let ListDraw {
        ctx, width, frame, ..
    } = *list;
    let g = glyphs::get();
    let note = if section.unavailable {
        // The reason when there is one (a refused listing says what to fix).
        section
            .unavailable_note
            .clone()
            .unwrap_or_else(|| "unavailable".to_string())
    } else if section.waiting {
        match &section.subtitle {
            Some(subtitle) => format!("listing {} {subtitle}", g.middot),
            None => "listing".to_string(),
        }
    } else if matches == 0
        && section.door.is_none()
        && section.origin == Some(crate::home::RootOrigin::Cwd.note())
    {
        // Launched with nothing to open: say so and how to go elsewhere, so the public rows
        // below do not read as this directory's.
        format!("nothing to open here {} ~ types a path", g.middot)
    } else {
        section.subtitle.clone().unwrap_or_default()
    };
    // The note is trimmed before the title and takes at most half the line (`Found`'s
    // short title leaves its note, the answer, more).
    let note_room = if section.title == crate::home::HomeState::SEARCH_SECTION {
        width.saturating_sub(glyphs::display_width(&section.title) + 16)
    } else {
        width / 2
    };
    // A callout reads from its mark and its file; the end of its message gives way.
    let note = if note.starts_with(g.warning) {
        glyphs::fit_cells(&note, note_room, g.ellipsis).into_owned()
    } else {
        glyphs::fit_start(&note, note_room)
    };
    // Paths and URLs keep their case (`s3://datui-sales` uppercased is a different
    // bucket); only word headings ("RECENT") are uppercased.
    let is_path = section.title.starts_with('/')
        || section.title.starts_with('~')
        || section.title.contains("://")
        // `C:\data` and `\\server\share`.
        || std::path::Path::new(&section.title).is_absolute();
    let mut title = if is_path {
        section.title.clone()
    } else {
        section.title.to_uppercase()
    };
    let marker = if collapsed { g.collapsed } else { g.expanded };
    // The count shows folded or not; until the listing is in, a spinner instead of a
    // guessed count.
    let chip = if section.waiting {
        let spinner = g.spinner;
        format!(" {} ", spinner[frame % spinner.len()])
    } else {
        format!(" {matches} ")
    };
    // Why the section is here (`current directory`, `configured`), as a flat chip after
    // the count.
    let origin = section
        .origin
        .map(|origin| format!(" {origin} "))
        .unwrap_or_default();
    let origin_cells = if origin.is_empty() {
        0
    } else {
        glyphs::display_width(&origin) + 1
    };
    // Marker, title, chip, [origin,] rule, note. The title yields to the note down to
    // three cells, then the note goes; the origin chip goes before the note. One cell of
    // rule always stays: its weight shows the cursor on a heading.
    const MIN_RULE: usize = 1;
    let mut note = note;
    let mut origin = origin;
    let mut origin_cells = origin_cells;
    let mut fixed = glyphs::display_width(marker)
        + 1
        + glyphs::display_width(&chip)
        + 1
        + origin_cells
        + glyphs::display_width(&note)
        + 2
        + MIN_RULE;
    if width.saturating_sub(fixed) < 3 {
        note = String::new();
        fixed = glyphs::display_width(marker)
            + 1
            + glyphs::display_width(&chip)
            + 1
            + origin_cells
            + 2
            + MIN_RULE;
    }
    if width.saturating_sub(fixed) < 3 {
        origin = String::new();
        origin_cells = 0;
        fixed = glyphs::display_width(marker) + 1 + glyphs::display_width(&chip) + 1 + 2 + MIN_RULE;
    }
    title = glyphs::fit_start(&title, width.saturating_sub(fixed));
    let rule_w = width.saturating_sub(fixed + glyphs::display_width(&title)) + MIN_RULE;

    // A title on a rule, not a filled bar: accented title, flat count chip, rule out to
    // the note. The focused section is brighter with a heavier rule.
    let title_style = if selected {
        Style::default()
            .fg(ctx.accent_bright)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
    };
    let chip_style = Style::default().bg(ctx.controls_bg).fg(ctx.text_primary);
    let origin_style = Style::default()
        .bg(ctx.controls_bg)
        .fg(ctx.accent)
        .add_modifier(Modifier::BOLD);
    let rule_glyph = if selected { g.rule_h_focused } else { g.rule_h };
    let rule_style = Style::default().fg(if selected {
        ctx.accent
    } else {
        ctx.column_separator
    });
    let mut spans = vec![
        Span::styled(marker, Style::default().fg(ctx.accent)),
        Span::styled(title, title_style),
        Span::raw(" "),
        Span::styled(chip, chip_style),
        Span::raw(" "),
    ];
    if origin_cells > 0 {
        spans.push(Span::styled(origin, origin_style));
        spans.push(Span::raw(" "));
    }
    spans.extend([
        Span::styled(rule_glyph.repeat(rule_w), rule_style),
        Span::raw(" "),
        Span::styled(
            note,
            Style::default().fg(if section.unavailable {
                ctx.warning
            } else {
                ctx.text_secondary
            }),
        ),
        Span::raw(" "),
    ]);
    Line::from(spans)
}

/// Split `text` into spans, styling the characters at `positions`; consecutive
/// positions merge into one span.
fn highlight_spans(
    text: &str,
    positions: &[usize],
    plain: Style,
    hit: Style,
) -> Vec<Span<'static>> {
    if positions.is_empty() {
        return vec![Span::styled(text.to_string(), plain)];
    }
    let marked: std::collections::HashSet<usize> = positions.iter().copied().collect();
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut run = String::new();
    let mut run_is_hit = false;

    for (i, ch) in text.chars().enumerate() {
        let is_hit = marked.contains(&i);
        if !run.is_empty() && is_hit != run_is_hit {
            spans.push(Span::styled(
                std::mem::take(&mut run),
                if run_is_hit { hit } else { plain },
            ));
        }
        run_is_hit = is_hit;
        run.push(ch);
    }
    if !run.is_empty() {
        spans.push(Span::styled(run, if run_is_hit { hit } else { plain }));
    }
    spans
}

/// A cloud source under `CLOUD`: name, API, bucket count (or why none) and the
/// login's origin. Its own layout: a source has no rows, columns, size or age.
fn source_line<'a>(
    entry: &'a Entry,
    source: Option<&crate::home::CloudSource>,
    selected: bool,
    width: usize,
    frame: usize,
    marks: &[usize],
    ctx: &RenderContext,
) -> Line<'a> {
    let g = glyphs::get();
    let chrome = RowChrome::new(selected, ctx);
    let base = chrome.base;

    let api = source.map(|s| s.api.name()).unwrap_or("");
    let count = match source {
        Some(s) if s.busy() => {
            let spinner = g.spinner[frame % g.spinner.len()];
            match s.count_text() {
                text if text.is_empty() => spinner.to_string(),
                text => format!("{text} {spinner}"),
            }
        }
        Some(s) => s.count_text(),
        None => String::new(),
    };
    let failed = source.is_some_and(|s| s.failed());
    let note = source.map(|s| s.note.clone()).unwrap_or_default();

    // Marker, place, name, api, count, note: the name up to a third of the line, the
    // count a fixed column, the note the rest.
    const API_W: usize = 7;
    const COUNT_W: usize = 14;
    let place = format!("{} ", g.in_object_store);
    let fixed =
        glyphs::display_width(chrome.marker) + glyphs::display_width(&place) + API_W + COUNT_W + 3;
    let name_w = entry
        .name
        .chars()
        .count()
        .min((width / 3).max(12))
        .max(16)
        .min(width.saturating_sub(fixed));
    let mut name = entry.name.clone();
    let mut positions = marks.to_vec();
    if name.chars().count() > name_w && name_w > 1 {
        let kept = name_w - 1;
        name = name.chars().take(kept).collect::<String>() + g.ellipsis;
        positions.retain(|p| *p < kept);
    }
    let name_pad = name_w.saturating_sub(name.chars().count());
    let note_w = width.saturating_sub(fixed + name_w);
    // Cut from the end: the leading account or endpoint tells sources apart.
    let note = glyphs::fit(&note, note_w);

    let mut spans = vec![
        chrome.marker(),
        Span::styled(place, base.fg(ctx.keybind_hints)),
    ];
    spans.extend(highlight_spans(
        &name,
        &positions,
        chrome.name(ctx.text_primary),
        chrome.hit,
    ));
    spans.push(chrome.pad(name_pad + 1));
    spans.push(Span::styled(format!("{api:<API_W$}"), base.fg(ctx.dimmed)));
    let count_style = if failed {
        base.fg(ctx.warning)
    } else {
        base.fg(ctx.text_secondary)
    };
    let count = glyphs::fit_start(&count, COUNT_W - 1);
    spans.push(Span::styled(format!("{count:<COUNT_W$}"), count_style));
    spans.push(Span::styled(" ".to_string(), base));
    let note_pad = note_w.saturating_sub(glyphs::display_width(&note));
    spans.push(Span::styled(note, base.fg(ctx.dimmed)));
    spans.push(Span::styled(" ".repeat(note_pad + 1), base));
    Line::from(spans)
}

/// A home line's rail and selected-row tint, carried to the edge. Tinted, not
/// reversed, so a row's own colors survive; "reversed" in the theme gets the old look.
struct RowChrome {
    selected: bool,
    base: Style,
    marker: &'static str,
    marker_style: Style,
    hit: Style,
}

impl RowChrome {
    fn new(selected: bool, ctx: &RenderContext) -> Self {
        let g = glyphs::get();
        let base = if selected {
            ctx.highlight_style()
        } else {
            Style::default()
        };
        RowChrome {
            selected,
            base,
            // The loudest thing on screen, found instantly.
            marker: if selected {
                g.selector
            } else {
                g.selector_blank
            },
            marker_style: base.fg(ctx.keybind_hints).add_modifier(Modifier::BOLD),
            hit: base
                .fg(ctx.keybind_hints)
                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        }
    }

    fn marker(&self) -> Span<'static> {
        Span::styled(self.marker, self.marker_style)
    }

    /// A name in `fg`, bold on the selected row.
    fn name(&self, fg: Color) -> Style {
        if self.selected {
            self.base.fg(fg).add_modifier(Modifier::BOLD)
        } else {
            self.base.fg(fg)
        }
    }

    fn pad(&self, cells: usize) -> Span<'static> {
        Span::styled(" ".repeat(cells), self.base)
    }
}

/// Where a row's data lives, as the glyph drawn before its name.
fn locality_glyph(locality: Option<crate::home::locality::Locality>) -> &'static str {
    let g = glyphs::get();
    match locality {
        Some(crate::home::locality::Locality::Object) => g.in_object_store,
        Some(crate::home::locality::Locality::Network) => g.over_network,
        Some(crate::home::locality::Locality::Memory) => g.in_memory,
        Some(crate::home::locality::Locality::Local) => g.here,
        Some(crate::home::locality::Locality::Unknown) | None => g.place_unknown,
    }
}

/// Places that can stall or cost money are colored; local disk, most rows, is dimmed
/// to texture.
fn locality_color(locality: Option<crate::home::locality::Locality>, ctx: &RenderContext) -> Color {
    match locality {
        Some(crate::home::locality::Locality::Object) => ctx.keybind_hints,
        Some(crate::home::locality::Locality::Network) => ctx.warning,
        Some(crate::home::locality::Locality::Memory) => ctx.temporal_col,
        _ => ctx.dimmed,
    }
}

fn entry_line<'a>(
    entry: &'a Entry,
    selected: bool,
    notes: EntryNotes<'a, '_>,
    list: &ListDraw,
) -> Line<'a> {
    let ListDraw {
        ctx,
        name_width,
        show_meta,
        frame,
        ..
    } = *list;
    let g = glyphs::get();
    let chrome = RowChrome::new(selected, ctx);
    // A row under a place is indented from the name's side; the meta columns stay put.
    let name_width = name_width.saturating_sub(notes.indent);

    let mut name = entry.name.clone();
    // Nothing is there to go inside of, whatever the path looks like.
    if shows_as_a_place(entry) && notes.place_kind != Some("missing") {
        name.push('/');
    }
    let label = crate::home::describe(
        entry,
        notes.place_kind,
        notes.look,
        frame,
        notes.known_sources,
    );
    // Where the data lives, right before its name: on an ultrawide the pane is too far
    // to tell cloud from local.
    let locality = entry
        .cost
        .source
        .as_deref()
        .map(crate::home::locality::Locality::of_fstype);
    let place_cell = format!("{} ", locality_glyph(locality));
    let place_w = glyphs::display_width(&place_cell);
    let (name_width, meta) = meta_cells(entry, &notes, locality, show_meta, name_width);
    let cell = fit_kind_cell(
        entry,
        &label,
        notes.matched_column,
        &name,
        name_width,
        place_w,
    );

    // Marks only when the name matched; a column match would mark unrelated letters.
    let marks = if notes.matched_column.is_none() {
        notes.marks.to_vec()
    } else {
        Vec::new()
    };
    let budget = name_width.saturating_sub(2 + place_w + glyphs::display_width(&cell.text) + 1);
    let (name, name_positions) = fit_name(name, marks, budget, entry.opens_whole_directory);
    let pad = name_width
        .saturating_sub(2 + glyphs::display_width(&name) + glyphs::display_width(&cell.text));

    // Unreadable files are listed so the directory reads as it is, dimmed.
    let name_fg = if entry.kind == EntryKind::Other {
        ctx.dimmed
    } else {
        ctx.text_primary
    };
    let base = chrome.base;
    let kind_style = if notes.matched_column.is_some() {
        base.fg(ctx.keybind_hints)
    } else if label.missing_source {
        base.fg(ctx.warning)
    } else if matches!(entry.kind, EntryKind::Hive | EntryKind::MultiFile) {
        Style::default()
            .bg(ctx.controls_bg)
            .fg(ctx.accent)
            .add_modifier(Modifier::BOLD)
    } else {
        base.fg(ctx.dimmed)
    };

    let mut spans = vec![
        chrome.marker(),
        chrome.pad(notes.indent),
        Span::styled(place_cell, base.fg(locality_color(locality, ctx))),
    ];
    spans.extend(highlight_spans(
        &name,
        &name_positions,
        chrome.name(name_fg),
        chrome.hit,
    ));
    match (&cell.column, notes.matched_column) {
        // The column note is a substring match, highlighted as one; `kept` counts the note's
        // own characters so the ellipsis gets no mark.
        (Some((shown, kept)), Some(_)) => {
            spans.push(Span::styled(format!(" {}", g.middot), kind_style));
            let mut positions = notes.marks.to_vec();
            positions.retain(|p| p < kept);
            spans.extend(highlight_spans(shown, &positions, kind_style, chrome.hit));
        }
        _ if cell.chip => {
            // One cell of row background, then the chip; stripped, not sliced (an empty cell
            // would panic).
            spans.push(chrome.pad(1));
            let chip = cell.text.strip_prefix(' ').unwrap_or(&cell.text);
            spans.push(Span::styled(chip.to_string(), kind_style));
        }
        _ => spans.push(Span::styled(cell.text, kind_style)),
    }
    match meta {
        Some(meta) => {
            spans.push(chrome.pad(pad));
            spans.push(Span::styled(meta, base.fg(ctx.dimmed)));
        }
        // The meta columns went to the name: pad across them so the bar reaches the edge.
        None if show_meta => spans.push(chrome.pad(pad)),
        None => {}
    }
    spans.push(chrome.pad(1));
    Line::from(spans)
}

/// An entry row's meta columns, and the name's width once a row with nothing to show
/// there gives them up (all, or the shape cells; size and age keep theirs).
fn meta_cells(
    entry: &Entry,
    notes: &EntryNotes,
    locality: Option<crate::home::locality::Locality>,
    show_meta: bool,
    name_width: usize,
) -> (usize, Option<String>) {
    // An unmeasured recent from a store or share (probes run over roots, not recents'
    // places): shows what was remembered, or that nothing is known.
    let unmeasured = notes.indent > 0
        && notes.place_kind.is_none()
        && entry.rows.is_none()
        && entry.cols.is_none()
        && entry.kind != EntryKind::Unknown
        && matches!(
            locality,
            Some(
                crate::home::locality::Locality::Object | crate::home::locality::Locality::Network
            )
        );
    match show_meta.then(|| meta_columns(entry, unmeasured, notes.size_hint, notes.gone)) {
        Some(meta) if meta.trim().is_empty() => (name_width + META_COLUMNS_WIDTH, None),
        Some(meta) if meta.chars().take(SHAPE_COLUMNS_WIDTH).all(|c| c == ' ') => (
            name_width + SHAPE_COLUMNS_WIDTH,
            Some(meta.chars().skip(SHAPE_COLUMNS_WIDTH).collect()),
        ),
        meta => (name_width, meta),
    }
}

/// The cell beside a row's name, as drawn.
struct KindCell {
    text: String,
    /// Hive and multi-file datasets wear their label as a flat chip.
    chip: bool,
    /// The matched column as cut to fit, and how many characters are the column's own.
    column: Option<(String, usize)>,
}

/// The cell beside a row's name: the matched column, else the label, else how Enter
/// reads it; cut or dropped so the name keeps room and the meta columns align.
fn fit_kind_cell(
    entry: &Entry,
    label: &crate::home::RowLabel,
    matched_column: Option<&str>,
    name: &str,
    name_width: usize,
    place_w: usize,
) -> KindCell {
    let g = glyphs::get();
    let kind = label.short.as_str();
    // Never a chip of nothing: the chip is drawn by taking the cell apart again.
    let chip = matched_column.is_none()
        && !kind.is_empty()
        && matches!(entry.kind, EntryKind::Hive | EntryKind::MultiFile);
    // Two cells between a name and what it is, on every row (#547 M7).
    let text = match kind {
        "" => String::new(),
        _ if chip => format!("  {kind} "),
        _ => format!("  {kind}"),
    };
    let ellipsis_len = g.ellipsis.chars().count();
    // A column note (why the row is listed) stays but may be long: its tail goes, and
    // marks in the cut tail go with it.
    let column = matched_column.map(|column| {
        let room = name_width.saturating_sub(2 + place_w + 1 + 2 + 2);
        let whole = column.chars().count();
        match room.checked_sub(ellipsis_len) {
            Some(kept) if kept > 0 && whole > room => (
                column.chars().take(kept).collect::<String>() + g.ellipsis,
                kept,
            ),
            _ => (column.to_string(), whole),
        }
    });
    let text = match &column {
        Some((shown, _)) => format!(" {}{shown}", g.middot),
        None => text,
    };
    let fits =
        |cell: &str| name_width.saturating_sub(2 + place_w + glyphs::display_width(cell) + 1) > 1;
    // On a narrow screen a plain label goes (it describes; the name identifies). A column
    // note, source id, `source not found:` or curated word stays, a source id cut to fit.
    let is_label = matched_column.is_none() && !label.source && !label.curated;
    let (text, chip) = if fits(&text) {
        (text, chip)
    } else if is_label {
        (String::new(), false)
    } else if label.source && matched_column.is_none() {
        let room = name_width.saturating_sub(2 + place_w + 1 + 2);
        (glyphs::fit_middle(&text, room), false)
    } else {
        (text, chip)
    };
    // How Enter reads a file not scanned in place, in an empty label's cell while the
    // whole name still fits (the pane says the same).
    let text = match discover::how_read(entry).and_then(read_marker) {
        Some(marker) if matched_column.is_none() && text.is_empty() => {
            let cell = format!("  {marker}");
            let room = name_width.saturating_sub(2 + place_w + 1);
            if glyphs::display_width(name) + glyphs::display_width(&cell) <= room {
                cell
            } else {
                text
            }
        }
        _ => text,
    };
    KindCell { text, chip, column }
}

/// `name` cut to `budget` (never the metadata), surviving match `positions` shifted.
/// A path keeps its tail, a filename its head; the door's name drops the directory
/// (the section title says it) before what Enter opens.
fn fit_name(
    name: String,
    mut positions: Vec<usize>,
    budget: usize,
    door: bool,
) -> (String, Vec<usize>) {
    let g = glyphs::get();
    let ellipsis_len = g.ellipsis.chars().count();
    let length = name.chars().count();
    if length <= budget {
        return (name, positions);
    }
    let door_cut = name
        .rfind(" (")
        .filter(|_| door)
        .map(|at| name.split_at(at));
    if budget > ellipsis_len
        && let Some((base, what)) = door_cut
    {
        let cut = if budget > what.chars().count() + ellipsis_len {
            let room = budget - what.chars().count() - ellipsis_len;
            let kept: String = base.chars().take(room).collect();
            format!("{kept}{}{what}", g.ellipsis)
        } else {
            // Narrower still: drop the directory; what Enter opens keeps its head.
            let what = what.trim_start();
            if what.chars().count() <= budget {
                what.to_string()
            } else {
                let kept: String = what.chars().take(budget - ellipsis_len).collect();
                format!("{kept}{}", g.ellipsis)
            }
        };
        return (cut, Vec::new());
    }
    if budget <= 1 {
        return (name, positions);
    }
    // A relative path too (a search hit under a directory): its leaf is the file.
    if name.starts_with('/') || name.starts_with('~') || name.trim_end_matches('/').contains('/') {
        let name = glyphs::fit_start(&name, budget);
        // The tail survived: positions shift left by what was dropped, right by the
        // ellipsis.
        let dropped = length + ellipsis_len - name.chars().count();
        positions.retain(|p| *p >= dropped);
        for p in &mut positions {
            *p = *p - dropped + ellipsis_len;
        }
        (name, positions)
    } else {
        let kept = budget - 1;
        positions.retain(|p| *p < kept);
        (
            name.chars().take(kept).collect::<String>() + g.ellipsis,
            positions,
        )
    }
}

/// A file row's word for how opening reads it: `converts`, `in memory`, or
/// `downloads` (copied whole first). `None` when scanned in place, as most are.
fn read_marker(how: discover::HowRead) -> Option<&'static str> {
    if how.download {
        return Some("downloads");
    }
    how.mode.marker()
}

/// The pane's `read` line: [`crate::ReadMode::label`], after the download if any.
fn read_words(how: discover::HowRead) -> String {
    let mode = how.mode.label();
    if how.download {
        format!("download {} {mode}", glyphs::get().arrow_right)
    } else {
        mode.to_string()
    }
}

/// A filled heading in the preview pane, the same grammar as the list's section
/// headers.
fn pane_heading(text: &str, width: usize, ctx: &RenderContext) -> Line<'static> {
    pane_heading_counted(text, None, width, ctx)
}

/// A pane heading with a flat count chip, as list sections carry: `COLUMNS [20]`,
/// `ROWS [5 of 20 columns]`. The title is always a noun.
fn pane_heading_counted(
    text: &str,
    chip: Option<&str>,
    width: usize,
    ctx: &RenderContext,
) -> Line<'static> {
    let g = glyphs::get();
    let label = text.to_string();
    let mut used = glyphs::display_width(&label) + 2;
    let mut spans = vec![
        Span::styled(
            label,
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
    ];
    if let Some(chip) = chip {
        let chip = format!(" {chip} ");
        used += glyphs::display_width(&chip) + 1;
        spans.push(Span::styled(
            chip,
            Style::default().bg(ctx.controls_bg).fg(ctx.text_primary),
        ));
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled(
        g.rule_h.repeat(width.saturating_sub(used)),
        Style::default().fg(ctx.column_separator),
    ));
    Line::from(spans)
}

/// The most column notes the details pane lists a line each.
const CODEBOOK_ROWS: usize = 12;

/// A documented file's column notes in the details pane: wrapped when `room` rows hold
/// them all, else one cut line per column. Ctrl+E shows the whole page.
fn column_notes_block(
    columns: &[(String, crate::home::catalog::ColumnNote)],
    width: usize,
    room: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(""), pane_heading("COLUMNS", width, ctx)];
    let key_w = key_column(columns.iter().map(|(name, _)| name.as_str())).min(22);
    let style = Style::default().fg(ctx.text_secondary);
    // A column with only a legend (a spec's enum) says how long it is, as the page does.
    let about = |note: &crate::home::catalog::ColumnNote| match note.about() {
        about if about.is_empty() && !note.values.is_empty() => {
            format!("{} values", note.values.len())
        }
        about => about,
    };
    let wrapped: Vec<Line<'static>> = columns
        .iter()
        .flat_map(|(name, note)| fact_lines(name, about(note), key_w, width, style, ctx))
        .collect();
    if lines.len() + wrapped.len() <= room {
        lines.extend(wrapped);
        return lines;
    }
    let value_w = width.saturating_sub(key_w + 2);
    // A row kept for the count of the rest.
    let fits = room.saturating_sub(lines.len() + 1).clamp(1, CODEBOOK_ROWS);
    for (name, note) in columns.iter().take(fits) {
        let name = glyphs::fit_cells(name, key_w, glyphs::get().ellipsis);
        lines.push(Line::from(vec![
            Span::styled(format!("{name:<key_w$}  "), Style::default().fg(ctx.dimmed)),
            Span::styled(
                glyphs::fit_cells(&about(note), value_w, glyphs::get().ellipsis).into_owned(),
                style,
            ),
        ]));
    }
    if columns.len() > fits {
        lines.push(Line::from(Span::styled(
            format!("{} {} more", glyphs::get().ellipsis, columns.len() - fits),
            Style::default().fg(ctx.dimmed),
        )));
    }
    lines
}

/// How many rows a line takes once the pane word-wraps it (as `Wrap { trim: false }`:
/// a word that does not fit moves whole). Trailing spaces are counted, so it errs one
/// high: it feeds a budget, where too few leaves a blank row and too many overflows.
pub(crate) fn wrapped_rows(line: &Line<'_>, width: usize) -> usize {
    use unicode_width::UnicodeWidthStr;
    let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
    if width == 0 {
        return 1;
    }
    let mut rows = 1;
    let mut used = 0;
    for word in text.split_inclusive(' ') {
        // Cells, not characters: some scripts' characters are two cells.
        let len = UnicodeWidthStr::width(word);
        if used + len > width && used > 0 {
            rows += 1;
            used = 0;
        }
        used += len;
        // A single word longer than the pane wraps inside itself.
        while used > width {
            rows += 1;
            used -= width;
        }
    }
    rows
}

/// As many of a schema's columns as `room` rows hold, and how many: counted as built,
/// since a long type wraps, and the `… N more` line must stay on screen.
fn schema_lines(
    schema: &[(String, polars::prelude::DataType)],
    name_w: usize,
    width: usize,
    room: usize,
    ctx: &RenderContext,
) -> (usize, Vec<Line<'static>>) {
    let _g = glyphs::get();
    let mut lines = Vec::new();
    let mut used = 0;
    for (name, dtype) in schema {
        let display = glyphs::fit(name, name_w);
        let line = Line::from(vec![
            Span::styled(
                format!("{display:<name_w$}  "),
                Style::default().fg(ctx.text_secondary),
            ),
            Span::styled(
                format!("{dtype}"),
                Style::default().fg(ctx.type_color(dtype)),
            ),
        ]);
        let takes = wrapped_rows(&line, width);
        if used + takes > room {
            break;
        }
        used += takes;
        lines.push(line);
    }
    (lines.len(), lines)
}

/// One `key   value` fact, the value emphasized. A long value wraps under itself, not
/// under the key.
fn fact_lines(
    key: &str,
    value: String,
    key_w: usize,
    width: usize,
    style: Style,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let indent = key_w + 2;
    let room = width.saturating_sub(indent);
    let key_span = Span::styled(format!("{key:<key_w$}  "), Style::default().fg(ctx.dimmed));
    // Too narrow to hang anything under: the pane's own wrap does what it can.
    if room < 12 || glyphs::display_width(&value) <= room {
        return vec![Line::from(vec![key_span, Span::styled(value, style)])];
    }
    let pieces = crate::render::overlays::wrap_help_line(&value, room);
    pieces
        .into_iter()
        .enumerate()
        .map(|(i, piece)| {
            let lead = if i == 0 {
                key_span.clone()
            } else {
                Span::raw(" ".repeat(indent))
            };
            Line::from(vec![lead, Span::styled(piece, style)])
        })
        .collect()
}

/// The key column width for a pane's facts, shared so values line up.
fn key_column<'a>(keys: impl IntoIterator<Item = &'a str>) -> usize {
    keys.into_iter()
        .map(glyphs::display_width)
        .max()
        .unwrap_or(0)
}

/// A compression ratio when worth stating: above about 1.2x, the difference between
/// on-disk and open size.
fn ratio_of(size: Option<u64>, uncompressed: Option<u64>) -> Option<f64> {
    let (on_disk, in_memory) = (size?, uncompressed?);
    if on_disk == 0 {
        return None;
    }
    let ratio = in_memory as f64 / on_disk as f64;
    (ratio >= 1.2).then_some(ratio)
}

/// The name, path and every known fact, its key column at least `key_w` wide so later
/// facts align; returns the key column used. Separate from the pane for testing.
fn preview_head_keyed(
    entry: &Entry,
    place_kind: Option<&'static str>,
    looking: Option<crate::home::CloudLook>,
    spec: Option<&crate::formats::Spec>,
    width: usize,
    key_w: usize,
    ctx: &RenderContext,
) -> (Vec<Line<'static>>, usize) {
    let g = glyphs::get();
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            entry.name.clone(),
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        // One line, tail kept: a wrapped path costs rows to repeat the leaf.
        Line::from(Span::styled(
            glyphs::fit_start(&crate::home::display_path(&entry.path), width),
            Style::default().fg(ctx.dimmed),
        )),
    ];

    // In sidebar order: what it is, where, what is inside, what reading takes, when it
    // changed.
    let mut facts: Vec<(&str, String, Style)> = Vec::new();
    let plain = Style::default().fg(ctx.text_secondary);

    let kind = crate::home::describe(entry, place_kind, looking, 0, None).words;
    if !kind.is_empty() {
        facts.push(("kind", kind, plain));
    }
    // The spec reading it and why: after `kind`, the path cut in the middle and the
    // conditions as chips, never wrapped mid-word.
    let spec_at = facts.len();
    let spec_path = spec
        .and_then(|s| s.path.as_deref())
        .map(crate::home::display_path);
    let chips = spec.map(|s| s.match_chips_for(&entry.path));
    if let Some(how) = discover::how_read(entry) {
        facts.push(("read", read_words(how), plain));
    }
    if let Some(source) = entry
        .cost
        .source
        .as_deref()
        .map(crate::home::locality::Source::from_fstype)
    {
        // The filesystem's own name; color carries the warning.
        let style = match source.locality {
            crate::home::locality::Locality::Network | crate::home::locality::Locality::Object => {
                Style::default().fg(ctx.warning)
            }
            crate::home::locality::Locality::Memory => Style::default().fg(ctx.success),
            _ => plain,
        };
        facts.push(("storage", source.label().to_string(), style));
    }
    // What there is to open inside, without the partition count when the `partitions`
    // fact carries one (they differ in caps). A looked-into directory with nothing to
    // open says so.
    match entry.holds.line(entry.cost.partitions.is_none()) {
        // One part per line under one label, so a count never breaks mid-line.
        Some(line) => {
            for (i, part) in line.split(" · ").enumerate() {
                let key = if i == 0 { "contains" } else { "" };
                facts.push((key, part.to_string(), plain));
            }
        }
        None if entry.kind == EntryKind::Directory
            && place_kind.is_none()
            && looking.is_none()
            && !entry.opens_whole_directory =>
        {
            facts.push(("contains", "no data files".to_string(), plain));
        }
        None => {}
    }
    // A spec's variants are record types, which its own `records` line counts.
    if let Some(n) = entry.cost.tables.filter(|_| entry.format_spec.is_none()) {
        let what = if n == 1 { "table" } else { "tables" };
        facts.push(("contains", format!("{n} {what}"), plain));
    }
    // A door that is not one table reads part of the directory: which part, and what it
    // leaves out.
    if entry.opens_whole_directory
        && let Some((reads, skips)) = crate::home::door_reads(entry)
    {
        facts.push(("reads", reads, plain));
        if let Some(skips) = skips {
            facts.push(("skips", skips, plain));
        }
    }
    if let Some(rows) = entry.rows {
        facts.push(("rows", discover::format_rows(rows), plain));
    }
    if let Some(cols) = entry.cols {
        let more = if entry.cols_sampled { "+" } else { "" };
        facts.push(("columns", format!("{cols}{more}"), plain));
    }
    if let Some(size) = entry.size {
        facts.push(("on disk", crate::numfmt::bytes(size), plain));
    }
    if let Some(uncompressed) = entry.cost.uncompressed {
        // The number nothing else implies: 200 MB of zstd Parquet is gigabytes open.
        facts.push((
            "in memory",
            crate::numfmt::bytes(uncompressed),
            Style::default().fg(ctx.float_col),
        ));
    }
    let ratio = ratio_of(entry.size, entry.cost.uncompressed).map(|r| format!("{r:.1}{}", g.times));
    match (&entry.cost.codec, ratio) {
        (Some(codec), Some(r)) => facts.push(("compression", format!("{codec}, {r}"), plain)),
        (Some(codec), None) => facts.push(("compression", codec.clone(), plain)),
        (None, Some(r)) => facts.push(("compression", r, plain)),
        (None, None) => {}
    }
    if let Some(groups) = entry.cost.row_groups {
        // One huge row group cannot be read in parallel or skipped; thousands of tiny ones
        // cost overhead.
        facts.push(("row groups", groups.to_string(), plain));
    }
    if let Some(parts) = &entry.cost.partitions {
        let count = if parts.more {
            format!("{}+", parts.count)
        } else {
            parts.count.to_string()
        };
        facts.push((
            "partitions",
            format!("{count} by {}", parts.keys.join(", ")),
            Style::default().fg(ctx.temporal_col),
        ));
        if let (Some(first), Some(last)) = (
            parts.first_key_values.first(),
            parts.first_key_values.last(),
        ) {
            let key = parts.keys.first().map(String::as_str).unwrap_or("");
            // "to", not an arrow glyph: reads the same everywhere.
            let range = if first == last {
                first.clone()
            } else {
                format!("{first} to {last}")
            };
            facts.push(("range", format!("{key} {range}"), plain));
        }
    }
    if let Some(modified) = entry.modified {
        // "now" already reads as a time; "now ago" does not.
        let age = discover::format_age(modified);
        if age == "now" {
            facts.push(("modified", age, plain));
        } else if !age.is_empty() {
            facts.push(("modified", format!("{age} ago"), plain));
        }
    }

    let spec_keys = spec_path
        .as_ref()
        .map(|_| "spec")
        .into_iter()
        .chain(chips.as_ref().map(|_| "match"));
    let key_w = key_column(facts.iter().map(|(k, _, _)| *k).chain(spec_keys)).max(key_w);
    if !facts.is_empty() || chips.is_some() {
        lines.push(Line::from(""));
        lines.push(pane_heading("DETAILS", width, ctx));
        let mut spec_lines = Vec::new();
        if let Some(path) = &spec_path {
            let room = width.saturating_sub(key_w + 2);
            spec_lines.extend(fact_lines(
                "spec",
                elide_path(path, room),
                key_w,
                width,
                plain,
                ctx,
            ));
        }
        if let Some(chips) = &chips {
            let filled = chips_filled(ctx);
            spec_lines.extend(chip_fact_lines("match", chips, key_w, width, filled, ctx));
        }
        let mut spec_lines = Some(spec_lines);
        for (i, (key, value, style)) in facts.into_iter().enumerate() {
            if i == spec_at {
                lines.extend(spec_lines.take().unwrap_or_default());
            }
            lines.extend(fact_lines(key, value, key_w, width, style, ctx));
        }
        lines.extend(spec_lines.take().unwrap_or_default());
    }

    (lines, key_w)
}

/// `path` in `width` columns, cut between components so the file name stays
/// (`~/…/formats/demo-mktdata.toml`); the first component stays if room; a name too
/// long alone keeps its end. Platform separators are kept as spelled.
fn elide_path(path: &str, width: usize) -> String {
    if glyphs::display_width(path) <= width {
        return path.to_string();
    }
    let ellipsis = glyphs::get().ellipsis;
    // Each separator, and where the component after it starts.
    let seps: Vec<(char, usize)> = path
        .char_indices()
        .filter(|(_, c)| std::path::is_separator(*c))
        .map(|(i, c)| (c, i + c.len_utf8()))
        .collect();
    let Some(&(first_sep, after_first)) = seps.first() else {
        return glyphs::fit_start(path, width);
    };
    let first = &path[..after_first - first_sep.len_utf8()];
    // The most trailing components that fit after `first/…/`, then after `…/`.
    for with_first in [true, false] {
        for &(sep, start) in &seps[1..] {
            let tail = &path[start..];
            let cut = if with_first {
                format!("{first}{first_sep}{ellipsis}{sep}{tail}")
            } else {
                format!("{ellipsis}{sep}{tail}")
            };
            if glyphs::display_width(&cut) <= width {
                return cut;
            }
        }
    }
    let (_, last) = seps[seps.len() - 1];
    glyphs::fit_start(&path[last..], width)
}

/// Whether chips are drawn on the chrome tier (UTF-8 with a visible header tint);
/// otherwise bracketed, text values quoted.
fn chips_filled(ctx: &RenderContext) -> bool {
    glyphs::get().unicode && crate::config::tint_shows(Some(ctx.table_header_bg)).is_some()
}

/// One spec match condition as spans within `room`: a flat chip (dim name, value in
/// its type's color), or `[name value]` without a tint. The name is cut before the
/// value, keeping at least its first character.
fn chip_spans(
    chip: &MatchChip,
    room: usize,
    filled: bool,
    ctx: &RenderContext,
) -> (Vec<Span<'static>>, usize) {
    use crate::formats::ChipKind;
    let value_color = match chip.kind {
        ChipKind::Int | ChipKind::Hex => ctx.int_col,
        ChipKind::Magic | ChipKind::Text | ChipKind::Glob => ctx.str_col,
    };
    let base = if filled {
        Style::default().bg(ctx.table_header_bg)
    } else {
        Style::default()
    };
    let dim = base.fg(ctx.dimmed);
    let (open, close) = if filled { (" ", " ") } else { ("[", "]") };
    let ellipsis = glyphs::get().ellipsis;
    let value = chip.value_text(!filled);
    let (name_w, value_w) = (
        glyphs::display_width(&chip.name),
        glyphs::display_width(&value),
    );
    let inner = room.saturating_sub(3);
    // The value keeps up to half the chip, the name the rest (at least a letter and the
    // marker).
    let name_floor = name_w.min(1 + glyphs::display_width(ellipsis));
    let name_room = name_w.min(inner.saturating_sub(value_w.min(inner / 2)).max(name_floor));
    let name = glyphs::fit_cells(&chip.name, name_room, ellipsis).into_owned();
    let name_w = glyphs::display_width(&name);
    let value = glyphs::fit_cells(&value, inner.saturating_sub(name_w), ellipsis).into_owned();
    let value_w = glyphs::display_width(&value);
    let width = 3 + name_w + value_w;
    let spans = vec![
        Span::styled(open, dim),
        Span::styled(name, dim),
        Span::styled(" ", base),
        Span::styled(value, base.fg(value_color)),
        Span::styled(close, dim),
    ];
    (spans, width)
}

/// The `match` fact: chips under the value column, wrapping between whole chips; `no
/// match` when the spec has none.
fn chip_fact_lines(
    key: &str,
    chips: &[MatchChip],
    key_w: usize,
    width: usize,
    filled: bool,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    if chips.is_empty() {
        let said = crate::formats::FORMAT_ONLY.to_string();
        let style = Style::default().fg(ctx.text_secondary);
        return fact_lines(key, said, key_w, width, style, ctx);
    }
    let indent = key_w + 2;
    let room = width.saturating_sub(indent).max(1);
    let mut rows: Vec<(Vec<Span<'static>>, usize)> = vec![(Vec::new(), 0)];
    for chip in chips {
        let (spans, w) = chip_spans(chip, room, filled, ctx);
        let row = rows.last_mut().expect("one row at least");
        if row.1 > 0 && row.1 + 1 + w > room {
            rows.push((spans, w));
        } else {
            if row.1 > 0 {
                row.0.push(Span::raw(" "));
                row.1 += 1;
            }
            row.0.extend(spans);
            row.1 += w;
        }
    }
    rows.into_iter()
        .enumerate()
        .map(|(i, (spans, _))| {
            let lead = if i == 0 {
                Span::styled(format!("{key:<key_w$}  "), Style::default().fg(ctx.dimmed))
            } else {
                Span::raw(" ".repeat(indent))
            };
            Line::from(std::iter::once(lead).chain(spans).collect::<Vec<_>>())
        })
        .collect()
}

/// A catalog heading's pane: what the catalog is, where it comes from, how to hide it
/// (Delete hides only the bundled one; any hides by id in `[home] hide`).
fn catalog_details(
    catalog: &crate::home::ShownCatalog,
    width: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let plain = Style::default().fg(ctx.text_secondary);
    let middot = glyphs::get().middot;
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            catalog.label.clone(),
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        pane_heading("DETAILS", width, ctx),
    ];
    let bundled = catalog.origin == crate::home::catalog::Origin::Bundled;
    let for_good = format!("[home] hide = [\"{}\"]", catalog.id);
    let mut facts: Vec<(&str, String)> = vec![("datasets", catalog.datasets.len().to_string())];
    match (&catalog.file, bundled) {
        (_, true) => facts.push((
            "source",
            format!(
                "{} {middot} datui catalog show {}",
                crate::home::BUNDLED_ORIGIN,
                catalog.id
            ),
        )),
        (Some(file), false) => facts.push(("file", crate::home::display_path(file))),
        (None, false) => {}
    }
    if !catalog.description.is_empty() {
        facts.push(("about", catalog.description.clone()));
    }
    if bundled {
        facts.push(("hide", "Del hides it until datui cache clear".to_string()));
        facts.push(("", format!("{for_good} for good")));
    } else {
        facts.push(("hide", for_good));
    }
    let key_w = key_column(facts.iter().map(|(k, _)| *k));
    for (key, value) in facts {
        lines.extend(fact_lines(key, value, key_w, width, plain, ctx));
    }
    lines
}

/// The details pane for a cloud source: what it points at, how it logs in, when its
/// buckets were listed, and a failed listing's whole message.
fn source_details(
    entry: &Entry,
    source: &crate::home::CloudSource,
    width: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let plain = Style::default().fg(ctx.text_secondary);
    let mut lines: Vec<Line> = vec![Line::from(Span::styled(
        entry.name.clone(),
        Style::default()
            .fg(ctx.text_primary)
            .add_modifier(Modifier::BOLD),
    ))];
    let mut facts: Vec<(String, String, Style)> = source
        .details
        .iter()
        .map(|(k, v)| (k.clone(), v.clone(), plain))
        .collect();
    let middot = glyphs::get().middot;
    let listed = match source.listed_at.map(discover::format_age) {
        Some(age) if age == "now" => format!(" {middot} listed now"),
        Some(age) if !age.is_empty() => format!(" {middot} listed {age} ago"),
        _ => String::new(),
    };
    let noun = match source.api {
        crate::cloud::source::ProviderKind::Azure => "accounts",
        _ => "buckets",
    };
    match &source.status {
        crate::home::CloudStatus::Listing if source.buckets.is_empty() => {
            facts.push((noun.to_string(), "listing".to_string(), plain));
        }
        crate::home::CloudStatus::Unlisted if source.buckets.is_empty() => {
            facts.push((noun.to_string(), "not listed".to_string(), plain));
        }
        _ => facts.push((
            noun.to_string(),
            format!("{}{listed}", source.buckets.len()),
            plain,
        )),
    }
    lines.push(Line::from(""));
    lines.push(pane_heading("DETAILS", width, ctx));
    let key_w = key_column(facts.iter().map(|(k, _, _)| k.as_str()));
    for (key, value, style) in facts {
        lines.extend(fact_lines(&key, value, key_w, width, style, ctx));
    }
    if let crate::home::CloudStatus::Failed { short, detail } = &source.status {
        lines.push(Line::from(""));
        lines.push(pane_heading(&short.to_uppercase(), width, ctx));
        lines.push(Line::from(Span::styled(
            detail.clone(),
            Style::default().fg(ctx.warning),
        )));
    }
    lines
}

/// The pane for a row that is not an entry: a place, the hidden files, a catalog.
fn other_details(
    app: &crate::App,
    width: usize,
    height: usize,
    ctx: &RenderContext,
) -> Option<Paragraph<'static>> {
    let wrapped = |lines| Paragraph::new(lines).wrap(ratatui::widgets::Wrap { trim: false });
    match app.home.selected_row()? {
        crate::home::Row::Place {
            path, source, held, ..
        } => Some(wrapped(place_details(
            &path,
            source.as_deref(),
            held,
            width,
            ctx,
        ))),
        crate::home::Row::Hidden { section, count } => {
            let section = &app.home.sections[section];
            let names: Vec<&str> = section
                .rows
                .iter()
                .filter(|row| row.hidden_by_default())
                .map(|row| row.name.as_str())
                .collect();
            let lines = hidden_details(&names, count, holds_tables(section), height, ctx);
            Some(Paragraph::new(lines))
        }
        _ => Some(wrapped(catalog_details(
            app.home.selected_catalog()?,
            width,
            ctx,
        ))),
    }
}

fn render_preview(
    area: Rect,
    buf: &mut Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
    screen_height: u16,
) {
    let width = area.width as usize;
    // The list is the typed directory meanwhile; a row's details would be off screen.
    if app.home.path_input_active {
        return;
    }
    if let Some(pane) = other_details(app, width, area.height as usize, ctx) {
        pane.render(area, buf);
        return;
    }
    let Some(entry) = app.home.selected_entry().cloned() else {
        return;
    };
    if crate::home::cloud_source_id(&entry.path).is_some() {
        if let Some(source) = app.home.cloud_source_of(&entry.path) {
            Paragraph::new(source_details(&entry, source, width, ctx))
                .wrap(ratatui::widgets::Wrap { trim: false })
                .render(area, buf);
        }
        return;
    }
    let g = glyphs::get();
    // The source listing's details for this place (an Azure subscription, a publisher),
    // below the facts in their key column.
    let place_details = app.home.place_details(&entry.path).map(<[_]>::to_vec);
    let key_w = place_details
        .as_ref()
        .map(|details| key_column(details.iter().map(|(k, _)| k.as_str())))
        .unwrap_or(0);
    // The spec that reads a file, which the spec alone describes without reading it.
    let spec = entry
        .format_spec
        .as_deref()
        .filter(|_| entry.kind == EntryKind::File && entry.table.is_none())
        .and_then(|name| app.home.formats.get(name))
        .cloned();
    let (mut lines, key_w) = preview_head_keyed(
        &entry,
        app.home.place_kind(&entry.path),
        app.home.cloud_look(&entry),
        spec.as_deref(),
        width,
        key_w,
        ctx,
    );
    // That a bucket door cannot read the directory whole, before Enter (which says why).
    #[cfg(feature = "cloud")]
    if entry.opens_whole_directory && crate::App::why_a_door_reads_nothing(&entry).is_some() {
        lines.push(Line::from(Span::styled(
            format!("{} prefix opens Parquet only", g.warning),
            Style::default().fg(ctx.warning),
        )));
    }
    if let Some(details) = place_details {
        for (key, value) in details {
            let style = if key == "network" || key == "shared keys" || key == "access" {
                Style::default().fg(ctx.warning)
            } else {
                Style::default().fg(ctx.text_secondary)
            };
            lines.extend(fact_lines(&key, value, key_w, width, style, ctx));
        }
    }
    // Column notes merged as the Documentation page does (catalog over spec). A file
    // inside a catalog dataset gets none: the dataset's columns need not be the file's.
    if let Some((_, mut doc)) = app.home_documented_row().filter(|(p, _)| *p == entry.path) {
        if app.home.catalog_dataset(&entry.path).is_none()
            && app.home.bookmark(&entry.path).is_none()
        {
            doc.catalog = None;
        }
        let columns = doc.columns();
        if !columns.is_empty() {
            let drawn: usize = lines.iter().map(|line| wrapped_rows(line, width)).sum();
            let room = (area.height as usize).saturating_sub(drawn);
            lines.extend(column_notes_block(&columns, width, room, ctx));
        }
    }

    // Rows before the schema (real values say more than types), at most the block's own
    // rows.
    app.home_app.preview_room = Some(screen_height);
    if let Some(preview) = app.home_preview_rows(&entry) {
        let drawn: usize = lines.iter().map(|line| wrapped_rows(line, width)).sum();
        let room = (area.height as usize)
            .saturating_sub(drawn + 1)
            .min(2 + crate::home::home_preview::PREVIEW_ROWS);
        if room >= STRIP_MIN_HEIGHT {
            lines.push(Line::from(""));
            lines.extend(rows_block(&preview, width, room, ctx));
        }
    }

    // ---- Schema ------------------------------------------------------------------
    lines.push(Line::from(""));
    match app.home_schema(&entry) {
        Some(schema) if !schema.is_empty() => {
            lines.push(pane_heading_counted(
                "COLUMNS",
                Some(&crate::numfmt::group_chrome(schema.len())),
                width,
                ctx,
            ));

            let name_w = schema
                .iter()
                .map(|(n, _)| glyphs::display_width(n))
                .max()
                .unwrap_or(0)
                .min(22);
            // Rows the lines will occupy after wrapping, not their count, so the schema tail and
            // `… N more` stay on screen.
            let drawn: usize = lines.iter().map(|line| wrapped_rows(line, width)).sum();
            let room = (area.height as usize).saturating_sub(drawn + 1);
            let (shown, mut schema_lines) = schema_lines(&schema, name_w, width, room, ctx);
            lines.append(&mut schema_lines);
            if schema.len() > shown {
                lines.push(Line::from(Span::styled(
                    format!("{} {} more", g.ellipsis, schema.len() - shown),
                    Style::default().fg(ctx.dimmed),
                )));
            }
        }
        _ => {
            // One fragment or none: the details and the footer already say what this is and what
            // Enter does.
            let variants = spec
                .as_deref()
                .filter(|s| s.lists_variants())
                .map(crate::formats::members::variant_tables);
            let spec_columns = spec
                .as_deref()
                .and_then(crate::formats::Spec::static_columns)
                .filter(|c| !c.is_empty());
            let (notes, warning) = enter_notes(
                &entry,
                &NoteFacts {
                    reading: app.home_schema_pending(&entry.path)
                        || app.home_preview_pending(&entry.path),
                    web_gone: app
                        .home
                        .web_gone
                        .get(&entry.path)
                        .map(|g| g.message.as_str()),
                    measured: app.home.enriched.contains_key(&entry.path),
                    missing: app.home.missing.contains(&entry.path),
                    variants: variants.as_ref().map(Vec::len),
                    spec_columns: spec_columns.as_ref().map(Vec::len),
                },
            );
            if let Some(warning) = warning {
                lines.push(Line::from(Span::styled(
                    format!("{} {warning}", g.warning),
                    Style::default().fg(ctx.warning),
                )));
            }
            let note_w = key_column(notes.iter().map(|(k, _)| *k)).max(key_w);
            let style = Style::default().fg(ctx.text_secondary);
            let said_spec = notes.iter().any(|(_, v)| v.ends_with("(spec)"));
            for (key, value) in notes {
                lines.extend(fact_lines(key, value, note_w, width, style, ctx));
            }
            if said_spec {
                let drawn: usize = lines.iter().map(|line| wrapped_rows(line, width)).sum();
                let room = (area.height as usize).saturating_sub(drawn + 1);
                lines.extend(spec_schema_lines(
                    variants.as_deref(),
                    spec_columns.as_deref(),
                    note_w + 2,
                    width,
                    room,
                    ctx,
                ));
            }
        }
    }

    Paragraph::new(lines)
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(area, buf);
}

/// What the pane says of opening a row whose schema it cannot show.
#[derive(Clone, Copy)]
struct NoteFacts<'a> {
    /// Its schema or rows are being read.
    reading: bool,
    /// Why a web file cannot be had, when a HEAD settled that.
    web_gone: Option<&'a str>,
    /// It has been measured.
    measured: bool,
    /// A catalog's local dataset that is not there.
    missing: bool,
    /// How many record types its spec lists, when the spec lists them.
    variants: Option<usize>,
    /// How many columns its spec names, when it names them.
    spec_columns: Option<usize>,
}

/// The pane's notes on what Enter does with a row whose schema it cannot show, and a
/// warning to draw instead of them.
fn enter_notes(entry: &Entry, facts: &NoteFacts) -> (Vec<(&'static str, String)>, Option<String>) {
    let NoteFacts {
        reading,
        web_gone,
        measured,
        missing,
        variants,
        spec_columns,
    } = *facts;
    let g = glyphs::get();
    // Whether a door will be inside to point at (none for an empty or markers-only
    // directory). The current filter does not matter: stepping in clears it.
    let door_in_there = !crate::home::holds_nothing_to_open(&entry.holds);
    let footer_unreadable = entry.kind == EntryKind::File
        && entry.rows.is_none()
        && discover::is_parquet_path(&entry.path)
        && measured;
    // A file of tables that opens one of them, as its format's descriptor says.
    let opens = discover::data_format(&entry.path)
        .and_then(|f| f.descriptor().tables.as_ref())
        .and_then(|t| t.opens);
    // What → lists: a spec's record types, or a file's tables.
    let inside = if entry.format_spec.is_some() {
        (g.arrow_right, "its record types".to_string())
    } else {
        (g.arrow_right, "its tables".to_string())
    };
    let step_in = |then: &str| format!("step in {} {then}", g.middot);
    let notes: Vec<(&'static str, String)> = match entry.kind {
        // The door says what it does.
        _ if entry.opens_whole_directory => {
            let does = match crate::home::door_kind(entry) {
                crate::home::DoorKind::Lake => DOOR_OF_A_LAKE_TABLE,
                crate::home::DoorKind::Hive => DOOR_OF_A_HIVE_TABLE,
                crate::home::DoorKind::SchemasDiffer => DOOR_OF_FILES_THAT_DIFFER,
                crate::home::DoorKind::Mixed | crate::home::DoorKind::Single => DOOR_OF_A_MIX,
                crate::home::DoorKind::OneSchema | crate::home::DoorKind::Unknown => THE_DOOR,
            };
            vec![("Enter", does.to_string())]
        }
        // A web file a HEAD showed unavailable: why, as a callout.
        _ if web_gone.is_some() => return (Vec::new(), web_gone.map(str::to_string)),
        // Where the other door is, for a directory not read as one table: one row further in.
        EntryKind::Directory if door_in_there => {
            vec![("Enter", step_in(INSIDE_AND_THE_DOOR))]
        }
        EntryKind::Directory => Vec::new(),
        // A catalog's local dataset that is not there: the kind line says so.
        EntryKind::Unknown if missing => Vec::new(),
        EntryKind::Unknown => vec![("schema", "not read".to_string())],
        EntryKind::Other => {
            vec![("format", format!("unknown {} Enter shows hex", g.middot))]
        }
        EntryKind::File if entry.enter_lists_tables() => {
            vec![("Enter", "its tables".to_string())]
        }
        // Spec variants are the spec's to say, whatever count the row carries.
        EntryKind::File if variants.is_some() || entry.cost.tables.is_some_and(|n| n > 1) => {
            let opens = opens.filter(|_| entry.cost.opens_one);
            let mut notes = vec![
                ("Enter", opens.unwrap_or("every record").to_string()),
                inside,
            ];
            if let Some(n) = variants {
                let what = if n == 1 { "type" } else { "types" };
                notes.push(("records", format!("{n} {what} (spec)")));
            }
            notes
        }
        // The log says which files are live, and datui does not read it.
        k if k.is_lake_table() && door_in_there => {
            vec![("Enter", step_in(INSIDE_A_LAKE_TABLE))]
        }
        k if k.is_lake_table() => vec![("Enter", step_in("log not read"))],
        // Measured with an unreadable footer: the open will likely fail too, so say so as a
        // callout before Enter.
        EntryKind::File if footer_unreadable => {
            return (Vec::new(), Some(FOOTER_UNREADABLE.to_string()));
        }
        _ if spec_columns.is_some() => {
            let n = spec_columns.unwrap_or_default();
            let what = if n == 1 { "column" } else { "columns" };
            vec![("schema", format!("{n} {what} (spec)"))]
        }
        _ if reading => vec![("schema", "reading...".to_string())],
        // Only Parquet gives columns unread; others are read on open, nothing to warn of.
        _ => vec![("schema", "on open".to_string())],
    };
    (notes, None)
}

/// What a spec says a file holds, under `records` or `schema`: record types with column
/// counts packed under the value column (`Status 6 · OrderAdd 9 · …`), or each column and
/// type, as `room` rows hold.
fn spec_schema_lines(
    variants: Option<&[crate::formats::members::Table]>,
    columns: Option<&[(String, polars::prelude::DataType)]>,
    indent: usize,
    width: usize,
    room: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    if let Some(variants) = variants {
        return variant_lines(variants, indent, width, room, ctx);
    }
    let Some(columns) = columns else {
        return Vec::new();
    };
    let name_w = columns
        .iter()
        .map(|(n, _)| glyphs::display_width(n))
        .max()
        .unwrap_or(0)
        .min(22);
    let (_, mut lines) = schema_lines(columns, name_w, width, room, ctx);
    if columns.len() > lines.len() {
        if lines.len() == room && !lines.is_empty() {
            lines.pop();
        }
        lines.push(Line::from(Span::styled(
            format!(
                "{} {} more",
                glyphs::get().ellipsis,
                columns.len() - lines.len()
            ),
            Style::default().fg(ctx.dimmed),
        )));
    }
    lines
}

/// A spec's variants as `name count` items, ` · ` between, breaking only between
/// items; `… N more` past `room` rows.
fn variant_lines(
    variants: &[crate::formats::members::Table],
    indent: usize,
    width: usize,
    room: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let g = glyphs::get();
    let avail = width.saturating_sub(indent).max(1);
    let sep = format!(" {} ", g.middot);
    let sep_w = glyphs::display_width(&sep);
    let name_style = Style::default().fg(ctx.text_secondary);
    let count_style = Style::default().fg(ctx.dimmed);
    let mut rows: Vec<(Vec<Span<'static>>, usize, usize)> = Vec::new();
    for variant in variants {
        let count = variant.columns.len().to_string();
        let count_w = count.len() + 1;
        let name = glyphs::fit_cells(&variant.name, avail.saturating_sub(count_w), g.ellipsis)
            .into_owned();
        let w = glyphs::display_width(&name) + count_w;
        let item = [
            Span::styled(name, name_style),
            Span::styled(format!(" {count}"), count_style),
        ];
        match rows.last_mut() {
            Some((spans, used, n)) if *used + sep_w + w <= avail => {
                spans.push(Span::styled(sep.clone(), count_style));
                spans.extend(item);
                *used += sep_w + w;
                *n += 1;
            }
            _ => rows.push((item.to_vec(), w, 1)),
        }
    }
    if room == 0 {
        return Vec::new();
    }
    let mut shown = 0;
    let mut lines: Vec<Line<'static>> = Vec::new();
    let lead = || Span::raw(" ".repeat(indent));
    for (spans, _, n) in rows.iter().take(room) {
        lines.push(Line::from(
            std::iter::once(lead())
                .chain(spans.iter().cloned())
                .collect::<Vec<_>>(),
        ));
        shown += n;
    }
    if shown < variants.len() {
        if lines.len() == room {
            if let Some((_, _, n)) = rows.get(lines.len() - 1) {
                shown -= n;
            }
            lines.pop();
        }
        lines.push(Line::from(vec![
            lead(),
            Span::styled(
                format!("{} {} more", g.ellipsis, variants.len() - shown),
                count_style,
            ),
        ]));
    }
    lines
}

/// The callout on a Parquet file whose footer could not be read, after the warning glyph.
const FOOTER_UNREADABLE: &str = "footer unreadable";

/// What `Enter` does on a directory it steps into rather than opens, after `step in`.
const INSIDE_AND_THE_DOOR: &str = "first row opens all";

/// The same, for a lake table, whose files are not its rows.
const INSIDE_A_LAKE_TABLE: &str = "first row reads files, log ignored";

/// What `Enter` does on the `(all files)` row itself.
const THE_DOOR: &str = "all files as one table";

/// The same, in a hive directory.
const DOOR_OF_A_HIVE_TABLE: &str = "all partitions as one table";

/// The same, in a lake table, whose log decides which files are live.
const DOOR_OF_A_LAKE_TABLE: &str = "all files, log ignored";

/// The same, where the files' columns disagree.
const DOOR_OF_FILES_THAT_DIFFER: &str = "files stacked, columns matched by name";

/// The same, where only part of the directory is read: the `reads` line says which.
const DOOR_OF_A_MIX: &str = "matching files as one table";

/// The pane for the hidden-files row: which files, as many as fit.
fn hidden_details(
    names: &[&str],
    count: usize,
    tables: bool,
    height: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let g = glyphs::get();
    let (title, what) = match (tables, count) {
        (true, 1) => ("Hidden tables", "1 internal SQLite table".to_string()),
        (true, _) => ("Hidden tables", format!("{count} internal SQLite tables")),
        (false, 1) => ("Hidden files", "1 file with no reader".to_string()),
        (false, _) => ("Hidden files", format!("{count} files with no reader")),
    };
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            title.to_string(),
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(what, Style::default().fg(ctx.dimmed))),
        Line::from(""),
    ];
    // The rows left, less one for the `… N more` line when not all of them fit.
    let room = height.saturating_sub(lines.len());
    let shown = if names.len() > room {
        room.saturating_sub(1)
    } else {
        names.len()
    };
    for name in &names[..shown] {
        lines.push(Line::from(Span::styled(
            name.to_string(),
            Style::default().fg(ctx.text_secondary),
        )));
    }
    if shown < names.len() {
        lines.push(Line::from(Span::styled(
            format!("{} {} more", g.ellipsis, names.len() - shown),
            Style::default().fg(ctx.dimmed),
        )));
    }
    lines
}

/// The pane for a place under `RECENT`: what, where, how many recents. Never reads the
/// place.
fn place_details(
    path: &std::path::Path,
    source: Option<&str>,
    held: usize,
    width: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let plain = Style::default().fg(ctx.text_secondary);
    // Its last component, or the whole path at a root or bucket.
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| crate::home::display_path(path));
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            name,
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            glyphs::fit_start(&crate::home::display_path(path), width),
            Style::default().fg(ctx.dimmed),
        )),
    ];
    let mut facts: Vec<(&str, String, Style)> = Vec::new();
    if let Some(source) = source.map(crate::home::locality::Source::from_fstype) {
        let style = match source.locality {
            crate::home::locality::Locality::Network | crate::home::locality::Locality::Object => {
                Style::default().fg(ctx.warning)
            }
            crate::home::locality::Locality::Memory => Style::default().fg(ctx.success),
            _ => plain,
        };
        facts.push(("storage", source.label().to_string(), style));
    }
    facts.push(("opened here", held.to_string(), plain));
    // Said before Enter is pressed rather than after: Enter does nothing here.
    if !crate::home::place_is_browsable(path) {
        facts.push(("listing", "none over HTTP".to_string(), plain));
    }
    let key_w = key_column(facts.iter().map(|(k, _, _)| *k));
    for (key, value, style) in facts {
        lines.extend(fact_lines(key, value, key_w, width, style, ctx));
    }
    lines
}

/// Whether a row's name gets a trailing slash, decided up front so it does not change
/// when the label lands. An unexamined local row is a directory (`project.old`,
/// `v1.2`); a remote row cannot be stat'ed, so a name with an extension is a file and
/// anything else a prefix.
fn shows_as_a_place(entry: &Entry) -> bool {
    // The door is an action: → does nothing on it, so no slash.
    if entry.opens_whole_directory {
        return false;
    }
    // A directory dataset is still a place: `events/  hive`.
    if matches!(
        entry.kind,
        EntryKind::Directory | EntryKind::Hive | EntryKind::MultiFile
    ) || entry.kind.is_lake_table()
    {
        return true;
    }
    if entry.kind != EntryKind::Unknown {
        return false;
    }
    !crate::home::is_remote_path(&entry.path)
        || (!crate::home::discover::is_data_file(&entry.path) && entry.path.extension().is_none())
}

#[cfg(test)]
mod tests;

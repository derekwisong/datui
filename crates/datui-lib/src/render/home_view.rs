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
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Width reserved for the right-aligned metadata columns in the list.
const META_WIDTH: u16 = 34;
/// Width given to the preview pane when there is room for it.
const PREVIEW_WIDTH: u16 = 40;
/// Below this, showing the preview would squeeze the list under [`META_MIN_WIDTH`]
/// and cost every row its size and shape. The scent is what the list is *for*, so the
/// preview yields first.
const PREVIEW_MIN_WIDTH: u16 = META_MIN_WIDTH + PREVIEW_WIDTH + 6;
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
    // A dataset too large to count still knows its width. Showing `? x 158` says more
    // than a blank, and the `?` is an admission rather than a guess.
    let times = glyphs::get().times;
    // A column count read from a spread of a folder rather than all of it is a floor,
    // so it says so, the way the row count already says `?` when it is out of reach.
    let more = if entry.cols_sampled { "+" } else { "" };
    let shape = match (entry.rows, entry.cols) {
        (Some(r), Some(c)) => format!("{} {times} {c}{more}", discover::format_rows(r)),
        (None, Some(c)) => format!("? {times} {c}{more}"),
        _ => String::new(),
    };
    let size = entry.size.map(discover::format_size).unwrap_or_default();
    let age = entry.modified.map(discover::format_age).unwrap_or_default();
    format!("{shape:>13}  {size:>9}  {age:>4}")
}

pub fn render(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    Clear.render(area, buf);

    // One column of breathing room, no more. Vertical space is the scarce thing in a
    // terminal: every blank line here is a dataset the user cannot see.
    let padded = Rect {
        x: area.x.saturating_add(1),
        y: area.y,
        width: area.width.saturating_sub(2),
        height: area.height,
    };
    if padded.width < 20 || padded.height < 5 {
        return;
    }

    // The wordmark costs two rows more than the title bar. On a tall enough terminal
    // that is nothing; on a short one it is two datasets, so the bar comes back.
    let wordmark = glyphs::get()
        .wordmark
        .filter(|_| padded.height >= WORDMARK_MIN_HEIGHT);
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
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),
                Constraint::Length(3), // the rule and its gutters
                Constraint::Length(PREVIEW_WIDTH),
            ])
            .split(rows[2]);
        render_list(body[0], buf, app, ctx);
        render_rule(body[1], buf, ctx);
        render_preview(body[2], buf, app, ctx);
    } else {
        render_list(rows[2], buf, app, ctx);
    }
}

/// Below this many rows the wordmark gives way to the one-line title bar.
const WORDMARK_MIN_HEIGHT: u16 = 28;

/// Interpolate two colours, when both are RGB. Anything else — a named ANSI colour,
/// an indexed one, the terminal default — has no arithmetic, so the first stop is
/// used for every column and the wordmark is simply the accent.
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

/// The wordmark: three rows of box drawing, coloured column by column along the
/// theme's gradient. Once, here, and nowhere else — a gradient on data would be
/// decoration. The location sits beside the middle row where the title bar used to
/// put it.
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
    let mark_w = lines.iter().map(|l| l.chars().count()).max().unwrap_or(0);
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
                format!("   {}", truncate_start(&location, room)),
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

/// A filled bar carrying the name and current location, mirroring the control bar at
/// the foot of the screen so the list sits between two anchors.
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
    let location = truncate_start(&location, (area.width as usize).saturating_sub(12));
    let pad = (area.width as usize).saturating_sub(8 + location.chars().count());
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
    // Placeholders name what the field takes, rather than instructing you to type
    // in the box the cursor is already sitting in. The filter one earns its space by
    // saying the part that is not obvious: typing searches below here as well.
    let (glyph, value, hint) = if home.path_input_active {
        (" ~ ", home.path_input.as_str(), "file or directory")
    } else {
        (g.prompt, home.filter.as_str(), "filter and search")
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
        // Keep the tail: these read "Failed to load <long path>: <reason>", and the
        // reason is the part worth the space. The name is already on the row.
        let used: usize = spans.iter().map(|s| s.content.chars().count()).sum();
        let room = (area.width as usize).saturating_sub(used + 3);
        spans.push(Span::styled(
            format!("   {}", truncate_start(status, room)),
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
    // Where this frame starts and how many rows it has room for. Settled before
    // anything borrows the listing, because both the scroll below and the decision
    // about what is worth looking into are made from it.
    //
    // Keep a little context above the selection rather than pinning it to the edge.
    // One drawn line per row now that the spacer is gone.
    let height = area.height as usize;
    app.home.scroll = app
        .home
        .selected
        .saturating_sub(height.saturating_sub(3).max(1));
    app.home.view_height = height;

    // Nothing is read here. Rows carry whatever a worker has measured so far, and
    // the request for more is made after the frame, not during it.
    app.home.pending_enrich = !app.home.unmeasured_visible(1).is_empty();
    app.home.pending_classify = !app.home.unclassified_visible(1).is_empty();
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
    let visible = app.home.visible();

    if let (Some(dir), Some(since), true) = (&awaiting, since, visible.is_empty()) {
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
        return;
    }

    if app.home.listing_in_flight && visible.is_empty() {
        Paragraph::new(Line::from(Span::styled(
            "Looking…",
            Style::default().fg(ctx.dimmed),
        )))
        .render(area, buf);
        return;
    }

    if visible.is_empty() && !app.home.filter.is_empty() {
        Paragraph::new(Line::from(Span::styled(
            "No match.",
            Style::default().fg(ctx.dimmed),
        )))
        .render(area, buf);
        return;
    }

    // Guidance shows whenever there is nothing openable — not only when the list is
    // literally empty. A first run in a directory holding one folder would otherwise
    // present a bare listing with no hint of what datui is for, which is the worst
    // possible first impression for a screen meant to be the way in.
    //
    // Counted over every section, folded or not: with everything folded the headers
    // are the content, and a hint that says there is nothing here would be wrong.
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
                    "or set [data] directories in your config",
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

    // Sources a URL can name. Taken from the config rather than from the sources
    // listed so far, so a source that is hidden or still being discovered is not
    // reported as missing.
    let known_sources: Vec<String> = app
        .app_config
        .cloud
        .sources
        .iter()
        .map(|s| s.name.clone())
        .collect();
    let mut lines: Vec<Line> = Vec::new();
    for (idx, row) in visible.iter().enumerate() {
        let selected = idx == app.home.selected;
        match row {
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
                    area.width as usize,
                    app.throbber_frame as usize,
                    ctx,
                ));
            }
            crate::home::Row::Entry { entry, .. }
                if crate::home::cloud_source_id(&entry.path).is_some() =>
            {
                let source = app.home.cloud_source_of(&entry.path);
                lines.push(source_line(
                    entry,
                    source,
                    selected,
                    area.width as usize,
                    app.throbber_frame as usize,
                    &app.home.filter,
                    ctx,
                ));
            }
            crate::home::Row::Entry { entry, .. } => {
                // When a row is here because of a column rather than its name, say so:
                // otherwise it reads as the filter having gone wrong.
                let via = if crate::home::fuzzy_score(&app.home.filter, &entry.name).is_some() {
                    None
                } else {
                    crate::home::matching_column(&app.home.filter, entry)
                };
                lines.push(entry_line(
                    entry,
                    selected,
                    name_width,
                    show_meta,
                    via,
                    &app.home.filter,
                    // Inside a source the trail already names it.
                    app.home
                        .browsing
                        .is_none()
                        .then_some(known_sources.as_slice()),
                    app.home.place_kind(&entry.path),
                    ctx,
                ));
            }
        }
    }

    let mut body: Vec<Line> = lines.into_iter().skip(app.home.scroll).collect();
    body.extend(guidance);
    Paragraph::new(body).render(area, buf);
}

/// Section headers carry the collapse marker and the provenance note, so the list
/// explains itself without a legend.
#[allow(clippy::too_many_arguments)]
fn section_header<'a>(
    section: &'a crate::home::Section,
    matches: usize,
    collapsed: bool,
    selected: bool,
    width: usize,
    frame: usize,
    ctx: &RenderContext,
) -> Line<'a> {
    let g = glyphs::get();
    let note = if section.unavailable {
        // The reason when there is one. A refused bucket listing says what to fix; a
        // share that has gone away has nothing to add beyond the word itself.
        section
            .unavailable_note
            .clone()
            .unwrap_or_else(|| "unavailable".to_string())
    } else if section.waiting {
        match &section.subtitle {
            Some(subtitle) => format!("listing · {subtitle}"),
            None => "listing".to_string(),
        }
    } else {
        section.subtitle.clone().unwrap_or_default()
    };
    // The note is trimmed before the title is, and never takes more than half the
    // line. A note is context; the title is what the section *is*, and a search
    // heading carrying a long path would otherwise crowd the title out entirely.
    let note = truncate_start(&note, width / 2);
    // A title that names a place keeps its case; only the word-like headings —
    // "RECENT", "ELSEWHERE" — are shouted. A URL is a place, and uppercasing one turns
    // `s3://datui-sales` into `S3://DATUI-SALES`, which is not the bucket's name and in
    // a case-sensitive store is not even a valid one.
    let is_path = section.title.starts_with('/')
        || section.title.starts_with('~')
        || section.title.contains("://");
    let mut title = if is_path {
        section.title.clone()
    } else {
        section.title.to_uppercase()
    };
    let marker = if collapsed { g.collapsed } else { g.expanded };
    // Shown whether folded or not: how much is in a place is worth knowing before
    // deciding to look in it, and a folded section would otherwise read as empty.
    // Until the listing is in, a count would be a guess, and a zero reads as empty.
    let chip = if section.waiting {
        let spinner = g.spinner;
        format!(" {} ", spinner[frame % spinner.len()])
    } else {
        format!(" {matches} ")
    };
    // marker, title, space, chip, space, rule, space, note, space. The title gives
    // way to the note only down to three cells; below that the note goes instead,
    // since a heading that is all note and no title says nothing.
    let mut note = note;
    let mut fixed =
        marker.chars().count() + 1 + chip.chars().count() + 1 + note.chars().count() + 2;
    if width.saturating_sub(fixed) < 3 {
        note = String::new();
        fixed = marker.chars().count() + 1 + chip.chars().count() + 1 + 2;
    }
    title = truncate_start(&title, width.saturating_sub(fixed));
    let rule_w = width.saturating_sub(fixed + title.chars().count());

    // A title on a rule, not a filled bar: the accent carries the title, the count
    // sits in a flat chip, and the rule runs out to the provenance note. The section
    // the cursor is in is brighter and its rule heavier, which is the whole of the
    // focus language — nothing moves, one thing lights up.
    let title_style = if selected {
        Style::default()
            .fg(ctx.accent_bright)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
    };
    let chip_style = Style::default().bg(ctx.controls_bg).fg(ctx.text_primary);
    let rule_glyph = if selected { g.rule_h_focused } else { g.rule_h };
    let rule_style = Style::default().fg(if selected {
        ctx.accent
    } else {
        ctx.column_separator
    });
    Line::from(vec![
        Span::styled(marker, Style::default().fg(ctx.accent)),
        Span::styled(title, title_style),
        Span::raw(" "),
        Span::styled(chip, chip_style),
        Span::raw(" "),
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
    ])
}

/// Split `text` into spans, styling the characters at `positions` differently.
///
/// Consecutive positions are merged into one span, so a run of matched characters is
/// a single styled stretch rather than a stutter of one-character spans.
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

/// A cloud source under `CLOUD`: its name, the API it speaks, how many buckets it
/// has (or why it has none) and where its login came from.
///
/// A column layout of its own rather than a dataset's. A source has no rows, columns,
/// size or age, and three blank metadata columns beside every source would say so on
/// every line.
fn source_line<'a>(
    entry: &'a Entry,
    source: Option<&crate::home::CloudSource>,
    selected: bool,
    width: usize,
    frame: usize,
    filter: &str,
    ctx: &RenderContext,
) -> Line<'a> {
    let g = glyphs::get();
    let base = match ctx.table_selected {
        Some(bg) if selected => Style::default().bg(bg),
        None if selected => Style::default().add_modifier(Modifier::REVERSED),
        _ => Style::default(),
    };
    let marker = if selected {
        g.selector
    } else {
        g.selector_blank
    };
    let name_style = if selected {
        base.fg(ctx.text_primary).add_modifier(Modifier::BOLD)
    } else {
        base.fg(ctx.text_primary)
    };
    let hit_style = base
        .fg(ctx.keybind_hints)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);

    let api = source.map(|s| s.api.as_str()).unwrap_or("");
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

    // marker, place, name, api, count, note. The name takes what it needs up to a
    // third of the line, the count a fixed column, and the note whatever is left.
    const API_W: usize = 7;
    const COUNT_W: usize = 14;
    let place = format!("{} ", g.in_object_store);
    let fixed = marker.chars().count() + place.chars().count() + API_W + COUNT_W + 3;
    let name_w = entry
        .name
        .chars()
        .count()
        .min((width / 3).max(12))
        .max(16)
        .min(width.saturating_sub(fixed));
    let mut name = entry.name.clone();
    let mut positions = crate::home::fuzzy_positions(filter, &name);
    if name.chars().count() > name_w && name_w > 1 {
        let kept = name_w - 1;
        name = name.chars().take(kept).collect::<String>() + g.ellipsis;
        positions.retain(|p| *p < kept);
    }
    let name_pad = name_w.saturating_sub(name.chars().count());
    let note_w = width.saturating_sub(fixed + name_w);
    // Cut from the end: the account or endpoint leads, and it is the part that tells
    // two sources apart.
    let note = if note.chars().count() > note_w && note_w > 1 {
        note.chars().take(note_w - 1).collect::<String>() + g.ellipsis
    } else {
        note
    };

    let mut spans = vec![
        Span::styled(
            marker,
            base.fg(ctx.keybind_hints).add_modifier(Modifier::BOLD),
        ),
        Span::styled(place, base.fg(ctx.keybind_hints)),
    ];
    spans.extend(highlight_spans(&name, &positions, name_style, hit_style));
    spans.push(Span::styled(" ".repeat(name_pad + 1), base));
    spans.push(Span::styled(format!("{api:<API_W$}"), base.fg(ctx.dimmed)));
    let count_style = if failed {
        base.fg(ctx.warning)
    } else {
        base.fg(ctx.text_secondary)
    };
    let count = truncate_start(&count, COUNT_W - 1);
    spans.push(Span::styled(format!("{count:<COUNT_W$}"), count_style));
    spans.push(Span::styled(" ".to_string(), base));
    let note_pad = note_w.saturating_sub(note.chars().count());
    spans.push(Span::styled(note, base.fg(ctx.dimmed)));
    spans.push(Span::styled(" ".repeat(note_pad + 1), base));
    Line::from(spans)
}

#[allow(clippy::too_many_arguments)]
fn entry_line<'a>(
    entry: &'a Entry,
    selected: bool,
    name_width: usize,
    show_meta: bool,
    matched_column: Option<&'a str>,
    filter: &str,
    known_sources: Option<&[String]>,
    place_kind: Option<&'static str>,
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
    if shows_as_a_place(entry) {
        name.push('/');
    }
    // A column hit takes the place of the kind label: both are a short note about
    // what this row is, and two of them would crowd the name.
    //
    // Inside an object store the service's own word is used, so a bucket reads as a
    // bucket rather than as a directory.
    let path_text = entry.path.to_string_lossy();
    let named_source = crate::source::split_source_id(&path_text).0;
    let described = entry.label();
    let kind = match entry.kind {
        EntryKind::Directory => place_kind
            .or_else(|| crate::home::object_place_label(&entry.path))
            .unwrap_or(described.as_ref()),
        _ => described.as_ref(),
    };
    // Two stores can hold the same bucket and key, so a row from one that is named in
    // its URL says which, where a kind would otherwise go. A recent whose source has
    // since left the config says that instead of failing only when it is opened.
    let missing_source = named_source
        .is_some_and(|id| known_sources.is_some_and(|known| !known.iter().any(|k| k == id)));
    let missing_note = named_source.map(|id| format!("source not found: {id}"));
    let kind: &str = match (named_source, kind, known_sources) {
        (Some(_), "", Some(_)) if missing_source => missing_note.as_deref().unwrap_or(""),
        (Some(id), "", Some(_)) => id,
        _ => kind,
    };
    // Nothing has looked into this row yet, and it has nothing else to say for itself.
    // An ellipsis says so and claims nothing: the word `dir` was a claim, and a blank
    // is what a file's empty label looks like.
    let kind = if kind.is_empty() && entry.kind == EntryKind::Unknown {
        g.ellipsis
    } else {
        kind
    };
    // Hive and multi-file datasets wear a flat chip; the rest stay as a word.
    let kind_is_chip =
        matched_column.is_none() && matches!(entry.kind, EntryKind::Hive | EntryKind::MultiFile);
    let kind_cell = match matched_column {
        Some(column) => format!(" ·{column}"),
        None if kind.is_empty() => String::new(),
        None if kind_is_chip => format!("  {kind} "),
        None => format!(" {kind}"),
    };

    // Where this row's data lives, immediately before its name. The detail pane has
    // carried this for a long time, and on a full-screen ultrawide the pane is far
    // enough from the cursor that a cloud path reads as a local one at a glance.
    let locality = entry
        .cost
        .source
        .as_deref()
        .map(crate::locality::Locality::of_fstype);
    let place = match locality {
        Some(crate::locality::Locality::Object) => g.in_object_store,
        Some(crate::locality::Locality::Network) => g.over_network,
        Some(crate::locality::Locality::Memory) => g.in_memory,
        Some(crate::locality::Locality::Local) => g.here,
        Some(crate::locality::Locality::Unknown) | None => g.place_unknown,
    };
    let place_cell = format!("{place} ");

    // A label counts now — `5000+ parquet` is thirteen characters where `iceberg` was
    // seven — and on a narrow screen it can leave the name nothing to be truncated
    // into, which puts the meta columns out of their alignment and clips them. The name
    // is what identifies a row and the label only describes it, so the label goes; the
    // details pane still has it. The test is the one truncation already makes, so there
    // is no width here to pick.
    //
    // Only a label. A column note says *why this row is in the list*, and the draw site
    // writes it from `matched_column` rather than from this cell, so blanking the cell
    // would hand the name a budget the note then overruns — the misalignment this
    // exists to stop, one column wider. And `source not found:` is a warning the pane
    // does not carry, so dropping it leaves a broken recent looking like a working one.
    let is_label = matched_column.is_none() && !missing_source;
    let (kind_cell, kind_is_chip) = if !is_label
        || name_width.saturating_sub(2 + place_cell.chars().count() + kind_cell.chars().count() + 1)
            > 1
    {
        (kind_cell, kind_is_chip)
    } else {
        (String::new(), false)
    };

    // Positions are taken from the untruncated name, because that is what matched.
    // Truncation then shifts them, and dropping the ones that fall outside is exactly
    // right: a character no longer on screen cannot be highlighted.
    //
    // Only when the name is *why* this row is here — a row matched by one of its
    // columns would otherwise get marks scattered over letters that had nothing to do
    // with it.
    let mut name_positions = if matched_column.is_none() {
        crate::home::fuzzy_positions(filter, &name)
    } else {
        Vec::new()
    };

    // Truncate the name, never the metadata: the columns must stay aligned. A row
    // whose name is a path keeps its tail, since the leaf is what identifies it;
    // an ordinary filename keeps its head, where the distinguishing part usually is.
    let budget =
        name_width.saturating_sub(2 + place_cell.chars().count() + kind_cell.chars().count() + 1);
    if name.chars().count() > budget && budget > 1 {
        let original_len = name.chars().count();
        if name.starts_with('/') || name.starts_with('~') {
            name = truncate_start(&name, budget);
            // The tail survived: shift every position left by what was dropped, and
            // right by the ellipsis now standing in for it.
            let kept = name.chars().count();
            let ellipsis = g.ellipsis.chars().count();
            let dropped = original_len + ellipsis - kept;
            name_positions.retain(|p| *p >= dropped);
            for p in &mut name_positions {
                *p = *p - dropped + ellipsis;
            }
        } else {
            let kept = budget - 1;
            name = name.chars().take(kept).collect::<String>() + g.ellipsis;
            // The head survived, so surviving positions keep their index.
            name_positions.retain(|p| *p < kept);
        }
    }
    let pad = name_width.saturating_sub(2 + name.chars().count() + kind_cell.chars().count());

    // The selected row is tinted across its full width and carries the rail, the way
    // the table marks its current row. Tinted rather than reversed so the colours
    // that say what a row is survive on the row you are looking at. A theme that
    // asks for "reversed" gets the old look.
    let base = match ctx.table_selected {
        Some(bg) if selected => Style::default().bg(bg),
        None if selected => Style::default().add_modifier(Modifier::REVERSED),
        _ => Style::default(),
    };
    let name_style = if selected {
        base.fg(ctx.text_primary).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ctx.text_primary)
    };
    let kind_style = if matched_column.is_some() {
        base.fg(ctx.keybind_hints)
    } else if missing_source {
        base.fg(ctx.warning)
    } else {
        match entry.kind {
            EntryKind::Hive | EntryKind::MultiFile => Style::default()
                .bg(ctx.controls_bg)
                .fg(ctx.accent)
                .add_modifier(Modifier::BOLD),
            _ => base.fg(ctx.dimmed),
        }
    };

    let hit_style = base
        .fg(ctx.keybind_hints)
        .add_modifier(Modifier::BOLD | Modifier::UNDERLINED);

    // Loud enough to find, quiet enough to ignore. Somewhere that can stall or cost
    // money is coloured; a local disk, which is the overwhelming majority of rows, is
    // dimmed so the column reads as texture rather than as a warning repeated on every
    // line.
    let place_style = base.fg(match locality {
        Some(crate::locality::Locality::Object) => ctx.keybind_hints,
        Some(crate::locality::Locality::Network) => ctx.warning,
        Some(crate::locality::Locality::Memory) => ctx.temporal_col,
        _ => ctx.dimmed,
    });

    let mut spans = vec![
        Span::styled(
            marker,
            base.fg(ctx.keybind_hints).add_modifier(Modifier::BOLD),
        ),
        Span::styled(place_cell, place_style),
    ];
    spans.extend(highlight_spans(
        &name,
        &name_positions,
        name_style,
        hit_style,
    ));
    // The column note is a substring match, so its highlight has to be one too.
    match matched_column {
        Some(column) => {
            spans.push(Span::styled(" ·".to_string(), kind_style));
            let positions = crate::home::substring_positions(filter, column);
            spans.extend(highlight_spans(column, &positions, kind_style, hit_style));
        }
        None if kind_is_chip => {
            // One cell of the row's own background, then the chip.
            spans.push(Span::styled(" ".to_string(), base));
            spans.push(Span::styled(kind_cell[1..].to_string(), kind_style));
        }
        None => spans.push(Span::styled(kind_cell.clone(), kind_style)),
    }
    if show_meta {
        spans.push(Span::styled(" ".repeat(pad), base));
        spans.push(Span::styled(meta_columns(entry), base.fg(ctx.dimmed)));
    }
    // Carry the reverse to the edge, so the bar is a bar and not a ragged highlight.
    spans.push(Span::styled(" ", base));
    Line::from(spans)
}

/// A filled heading inside the preview pane.
///
/// The same grammar as the list's section headers — a filled bar, not a box — so the
/// two halves of the screen read as one program.
fn pane_heading(text: &str, width: usize, ctx: &RenderContext) -> Line<'static> {
    let g = glyphs::get();
    let label = text.to_string();
    let rule_w = width.saturating_sub(label.chars().count() + 2);
    Line::from(vec![
        Span::styled(
            label,
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        Span::styled(
            g.rule_h.repeat(rule_w),
            Style::default().fg(ctx.column_separator),
        ),
    ])
}

/// One `key   value` line, with the value carrying the emphasis.
fn fact_line(
    key: &str,
    value: String,
    key_w: usize,
    style: Style,
    ctx: &RenderContext,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<key_w$}  "), Style::default().fg(ctx.dimmed)),
        Span::styled(value, style),
    ])
}

/// A compression ratio, when it is worth stating.
///
/// Below about 1.2x the number is noise; above it, it is the difference between what
/// a file weighs and what it will weigh once open.
fn ratio_of(size: Option<u64>, uncompressed: Option<u64>) -> Option<f64> {
    let (on_disk, in_memory) = (size?, uncompressed?);
    if on_disk == 0 {
        return None;
    }
    let ratio = in_memory as f64 / on_disk as f64;
    (ratio >= 1.2).then_some(ratio)
}

/// The name, the path, and everything known about the dataset.
///
/// Split out from the pane so it can be checked without an application behind it.
fn preview_head(
    entry: &Entry,
    place_kind: Option<&'static str>,
    width: usize,
    ctx: &RenderContext,
) -> Vec<Line<'static>> {
    let g = glyphs::get();
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            entry.name.clone(),
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD),
        )),
        // One line, tail kept: a wrapped path costs three rows to say what the leaf
        // already said.
        Line::from(Span::styled(
            truncate_start(&crate::home::display_path(&entry.path), width),
            Style::default().fg(ctx.dimmed),
        )),
    ];

    // One list. The split into "what it costs" and "what it is" was a distinction
    // the reader has to be told about; these are all just details of the same thing,
    // and a person scanning them does not need them sorted into camps.
    //
    // Ordered by what decides whether to press Enter: where it lives, what it holds,
    // what reading it will take, and when it last changed.
    let mut facts: Vec<(&str, String, Style)> = Vec::new();
    let plain = Style::default().fg(ctx.text_secondary);

    if let Some(source) = entry
        .cost
        .source
        .as_deref()
        .map(crate::locality::Source::from_fstype)
    {
        // The filesystem's own name and nothing else. Colour carries the warning:
        // a sentence explaining that a network is a network is a sentence the
        // reader has to skip on every row they look at.
        let style = match source.locality {
            crate::locality::Locality::Network | crate::locality::Locality::Object => {
                Style::default().fg(ctx.warning)
            }
            crate::locality::Locality::Memory => Style::default().fg(ctx.success),
            _ => plain,
        };
        facts.push(("source", source.label().to_string(), style));
    }
    let described = entry.label();
    let kind = match entry.kind {
        EntryKind::Directory => place_kind
            .or_else(|| crate::home::object_place_label(&entry.path))
            .unwrap_or(described.as_ref()),
        _ => described.as_ref(),
    };
    if !kind.is_empty() {
        facts.push(("kind", kind.to_string(), plain));
    }
    // What one listing of it found, beside what the label boiled that down to.
    if let Some(line) = entry.holds.line() {
        facts.push(("holds", line, plain));
    }
    if let Some(rows) = entry.rows {
        facts.push(("rows", discover::format_rows(rows), plain));
    }
    if let Some(cols) = entry.cols {
        let more = if entry.cols_sampled { "+" } else { "" };
        facts.push(("columns", format!("{cols}{more}"), plain));
    }
    if let Some(size) = entry.size {
        facts.push(("on disk", discover::format_size(size), plain));
    }
    if let Some(uncompressed) = entry.cost.uncompressed {
        // The one number nothing else here implies: 200 MB of zstd Parquet is two
        // gigabytes once it is open.
        let mut text = discover::format_size(uncompressed);
        match (ratio_of(entry.size, Some(uncompressed)), &entry.cost.codec) {
            (Some(r), Some(codec)) => text.push_str(&format!("  {codec} {r:.1}{}", g.times)),
            (None, Some(codec)) => text.push_str(&format!("  {codec}")),
            (Some(r), None) => text.push_str(&format!("  {r:.1}{}", g.times)),
            (None, None) => {}
        }
        facts.push(("in memory", text, Style::default().fg(ctx.float_col)));
    } else if let Some(codec) = &entry.cost.codec {
        facts.push(("codec", codec.clone(), plain));
    }
    if let Some(groups) = entry.cost.row_groups {
        // One enormous row group cannot be read in parallel or skipped through; a
        // thousand tiny ones cost more in overhead than they save.
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
            // Spelled rather than drawn: an arrow glyph here would be the only one
            // on the screen, and "to" reads the same on every terminal.
            let range = if first == last {
                first.clone()
            } else {
                format!("{first} to {last}")
            };
            facts.push(("", format!("{key} {range}"), plain));
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

    if !facts.is_empty() {
        lines.push(Line::from(""));
        lines.push(pane_heading("DETAILS", width, ctx));
        let key_w = facts.iter().map(|(k, _, _)| k.len()).max().unwrap_or(0);
        for (key, value, style) in facts {
            lines.push(fact_line(key, value, key_w, style, ctx));
        }
    }

    lines
}

/// The details pane for a cloud source: what it points at, how it logs in, and when
/// its buckets were listed. When listing failed, the whole message, since the row only
/// had room for a word of it.
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
    let listed = match source.listed_at.map(discover::format_age) {
        Some(age) if age == "now" => " · listed now".to_string(),
        Some(age) if !age.is_empty() => format!(" · listed {age} ago"),
        _ => String::new(),
    };
    let noun = match source.api.as_str() {
        "azure" => "accounts",
        "public" => "datasets",
        _ => "buckets",
    };
    match &source.status {
        crate::home::CloudStatus::Listing if source.buckets.is_empty() => {
            facts.push((noun.to_string(), "listing".to_string(), plain));
        }
        _ => facts.push((
            noun.to_string(),
            format!("{}{listed}", source.buckets.len()),
            plain,
        )),
    }
    lines.push(Line::from(""));
    lines.push(pane_heading("DETAILS", width, ctx));
    let key_w = facts.iter().map(|(k, _, _)| k.len()).max().unwrap_or(0);
    for (key, value, style) in facts {
        lines.push(fact_line(&key, value, key_w, style, ctx));
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

fn render_preview(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    let Some(entry) = app.home.selected_entry() else {
        return;
    };
    let width = area.width as usize;
    if crate::home::cloud_source_id(&entry.path).is_some() {
        if let Some(source) = app.home.cloud_source_of(&entry.path) {
            Paragraph::new(source_details(&entry, source, width, ctx))
                .wrap(ratatui::widgets::Wrap { trim: false })
                .render(area, buf);
        }
        return;
    }
    let g = glyphs::get();
    let mut lines = preview_head(&entry, app.home.place_kind(&entry.path), width, ctx);
    // What the source's listing said about this place: an Azure account's
    // subscription, region and namespace.
    if let Some(details) = app.home.place_details(&entry.path) {
        let key_w = details.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
        for (key, value) in details {
            let style = if key == "network" || key == "shared keys" || key == "access" {
                Style::default().fg(ctx.warning)
            } else {
                Style::default().fg(ctx.text_secondary)
            };
            lines.push(fact_line(key, value.clone(), key_w.max(8), style, ctx));
        }
    }

    // ---- Schema ------------------------------------------------------------------
    lines.push(Line::from(""));
    match app.home_schema(&entry) {
        Some(schema) if !schema.is_empty() => {
            lines.push(pane_heading(
                &format!("{} COLUMNS", schema.len()),
                width,
                ctx,
            ));

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
                    display = display.chars().take(name_w - 1).collect::<String>() + g.ellipsis;
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
                    format!("{} {} more", g.ellipsis, schema.len() - room),
                    Style::default().fg(ctx.dimmed),
                )));
            }
        }
        _ => {
            // One fragment, or none. The details list above already says what this
            // is, and the control bar already says what Enter does; a sentence
            // repeating either is a sentence to read past on every row.
            let reading = app.home_schema_pending(&entry.path);
            let note = match entry.kind {
                // Nothing to add: "kind directory" is directly above.
                EntryKind::Directory => "",
                EntryKind::Unknown => "Not read yet.",
                // The log says which files are live, and datui does not read it.
                k if k.is_lake_table() => "Enter goes inside. The table itself is not read yet.",
                _ if crate::home::is_object_store_url(&entry.path) => "Read when opened.",
                _ if reading => "Reading…",
                _ => "Schema needs a full read.",
            };
            if !note.is_empty() {
                lines.push(Line::from(Span::styled(
                    note,
                    Style::default().fg(ctx.dimmed),
                )));
            }
        }
    }

    Paragraph::new(lines)
        .wrap(ratatui::widgets::Wrap { trim: false })
        .render(area, buf);
}

/// Whether a row's name is drawn with a trailing slash.
///
/// A row nothing has looked into is still a place: a listing datui made itself only ever
/// puts a directory on one, so `project.old` and `v1.2` keep their slash however many
/// dots are in the name. A remote row — a bucket, an HTTP URL, a path on a share — is
/// the one nothing can stat, and there the name is all there is — so a name with an extension is a file even when it is one datui does not
/// read (`export.txt`), and everything else is a prefix.
///
/// Said up front so the name does not change shape a frame later when the label lands;
/// the `…` beside it already carries the part that is not known.
fn shows_as_a_place(entry: &Entry) -> bool {
    if entry.kind == EntryKind::Directory {
        return true;
    }
    if entry.kind != EntryKind::Unknown {
        return false;
    }
    !crate::home::is_remote_path(&entry.path)
        || (!crate::discover::is_data_file(&entry.path) && entry.path.extension().is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::Entry;
    use crate::home::Section;

    fn row(path: &str, kind: crate::discover::EntryKind) -> Entry {
        Entry {
            path: std::path::PathBuf::from(path),
            kind,
            name: path.rsplit('/').next().unwrap_or(path).to_string(),
            size: None,
            modified: None,
            rows: None,
            cols: None,
            cols_sampled: false,
            columns: Vec::new(),
            cost: Default::default(),
            holds: Default::default(),
        }
    }

    /// A label describes and a name identifies, so on a screen too narrow for both the
    /// label goes. Before this a `5000+ parquet` chip left the name nothing to be
    /// truncated into and shoved the size and modified columns out of alignment.
    #[test]
    fn a_label_gives_way_to_the_name_on_a_narrow_screen() {
        let ctx = RenderContext::for_test();
        let mut entry = row("/data/exports", crate::discover::EntryKind::MultiFile);
        // Real metadata, so the meta columns are a string the offset can be found by.
        entry.size = Some(4096);
        entry.rows = Some(12);
        entry.holds = crate::discover::Holds {
            formats: vec![("parquet".to_string(), 5000)],
            truncated: true,
            ..Default::default()
        };
        assert_eq!(entry.label(), "5000+ parquet");

        // Where the meta columns begin: everything drawn before them. It must not
        // depend on how long a row's label is, or the columns stop lining up.
        let offset_of_meta = |line: Line<'_>, entry: &Entry| -> usize {
            let meta = meta_columns(entry);
            let at = line
                .spans
                .iter()
                .position(|s| s.content == meta)
                .expect("the meta columns are drawn");
            line.spans[..at]
                .iter()
                .map(|s| s.content.chars().count())
                .sum()
        };
        let meta_starts_at = |entry: &Entry, width: usize| -> usize {
            let line = entry_line(entry, false, width, true, None, "", None, None, &ctx);
            offset_of_meta(line, entry)
        };

        // The same kind, so only the label's length differs: a `Directory` row carries
        // a trailing slash and would be a character wider for a reason of its own.
        let mut short = row("/data/exports", crate::discover::EntryKind::MultiFile);
        short.size = Some(4096);
        short.rows = Some(12);
        short.holds = crate::discover::Holds {
            formats: vec![("csv".to_string(), 2)],
            ..Default::default()
        };
        assert_eq!(short.label(), "2 csv");

        for width in [22usize, 24, 30, 48, 100] {
            assert_eq!(
                meta_starts_at(&entry, width),
                meta_starts_at(&short, width),
                "the meta columns must start in the same place at {width}"
            );
        }

        // A column note is not a label: it says why the row is in the list, and the
        // draw site writes it from `matched_column` rather than from the cell, so
        // blanking the cell would hand the name a budget the note then overruns.
        let noted = |entry: &Entry, width: usize| -> usize {
            let line = entry_line(
                entry,
                false,
                width,
                true,
                Some("transaction_amount"),
                "",
                None,
                None,
                &ctx,
            );
            offset_of_meta(line, entry)
        };
        for width in [20usize, 24, 30, 48] {
            assert_eq!(
                noted(&entry, width),
                noted(&short, width),
                "a matched row is drawn by its note, whatever its label would say, \
                 at {width}"
            );
        }
    }

    /// A dot in a folder's name does not make it a file. A local row nothing has looked
    /// into came from a listing that saw a directory, so the name is not the evidence.
    #[test]
    fn a_local_folder_keeps_its_slash_however_its_name_is_spelled() {
        for name in ["project.old", "v1.2", "site.com", "datui.git", "plain"] {
            let entry = row(&format!("/home/derek/{name}"), EntryKind::Unknown);
            assert!(
                shows_as_a_place(&entry),
                "{name} is a folder the listing saw"
            );
        }
    }

    /// Remote, the name is all there is. One with an extension is a file even when it
    /// is one datui does not read, and one without is a prefix.
    #[test]
    fn a_remote_row_is_read_as_a_prefix_only_when_its_name_is_not_a_file() {
        // A path on a network mount takes the same branch, but whether one *is* a
        // network mount is a question about this machine's mount table, so the two
        // URL schemes are what a test can say.
        for prefix in ["s3://bucket", "https://host"] {
            for name in ["export.txt", "part-00000.parquet", "notes.md"] {
                assert!(
                    !shows_as_a_place(&row(&format!("{prefix}/{name}"), EntryKind::Unknown)),
                    "{prefix}/{name} is named like a file"
                );
            }
        }
        for prefix in ["s3://bucket", "https://host"] {
            for name in ["exports", "2024", "raw"] {
                assert!(
                    shows_as_a_place(&row(&format!("{prefix}/{name}"), EntryKind::Unknown)),
                    "{prefix}/{name} is a prefix"
                );
            }
        }
    }

    fn header_width(section: &Section, width: usize) -> usize {
        let ctx = RenderContext::for_test();
        let line = section_header(section, 3, false, false, width, 0, &ctx);
        line.spans.iter().map(|s| s.content.chars().count()).sum()
    }

    #[test]
    fn a_long_note_never_pushes_the_header_past_the_screen() {
        // A search heading carries the path it searched, which is easily longer than
        // the terminal. The note is context; the title is what the section is.
        let section = Section {
            title: "Found".to_string(),
            subtitle: Some(
                "/very/deeply/nested/path/that/goes/on/and/on/for/quite/a/while · 99999 searched"
                    .to_string(),
            ),
            rows: Vec::new(),
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
        };

        for width in [20usize, 40, 80, 120] {
            let rendered = header_width(&section, width);
            assert!(
                rendered <= width,
                "a {width}-wide screen produced a {rendered}-character header"
            );
        }
    }

    #[test]
    fn a_recent_from_a_source_names_it_or_says_it_is_gone() {
        let ctx = RenderContext::for_test();
        let path = std::path::Path::new("s3://lab@data/sales.parquet");
        let mut entry = Entry::for_test(path, "sales.parquet");
        entry.kind = EntryKind::Unknown;
        let text = |known: Option<&[String]>| -> String {
            entry_line(&entry, false, 80, false, None, "", known, None, &ctx)
                .spans
                .iter()
                .map(|s| s.content.to_string())
                .collect()
        };
        let lab = ["lab".to_string()];
        assert!(text(Some(&lab)).contains("sales.parquet lab"));
        assert!(text(Some(&[])).contains("source not found: lab"));
        // Inside a source the trail already says which, so nothing is added.
        assert!(!text(None).contains("lab"));
    }

    /// The row's spans, as (text, is_highlighted) pairs.
    fn row_spans(name: &str, filter: &str, column: Option<&str>) -> Vec<(String, bool)> {
        let ctx = RenderContext::for_test();
        let entry = Entry::for_test(std::path::Path::new("/tmp/x"), name);
        let line = entry_line(
            &entry,
            false,
            60,
            false,
            column,
            filter,
            Some(&[]),
            None,
            &ctx,
        );
        line.spans
            .iter()
            .skip(1) // the selection marker
            .map(|s| {
                (
                    s.content.to_string(),
                    s.style.add_modifier.contains(Modifier::UNDERLINED),
                )
            })
            .collect()
    }

    fn highlighted_text(spans: &[(String, bool)]) -> String {
        spans
            .iter()
            .filter(|(_, hit)| *hit)
            .map(|(t, _)| t.as_str())
            .collect()
    }

    fn preview_text(entry: &Entry, width: usize) -> String {
        let ctx = RenderContext::for_test();
        preview_head(entry, None, width, &ctx)
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn costed(name: &str, cost: crate::discover::Cost, size: Option<u64>) -> Entry {
        let mut e = Entry::for_test(std::path::Path::new("/tmp/x"), name);
        e.size = size;
        e.cost = cost;
        e
    }

    #[test]
    fn a_network_source_is_named_rather_than_described() {
        // The filesystem's own name and nothing else. A sentence explaining that a
        // network is a network is a sentence to skip on every row.
        let e = costed(
            "prices.parquet",
            crate::discover::Cost {
                source: Some("nfs4".into()),
                ..Default::default()
            },
            None,
        );
        let text = preview_text(&e, 44);
        assert!(text.contains("nfs4"), "{text}");
    }

    #[test]
    fn local_disk_gets_no_warning() {
        let e = costed(
            "prices.parquet",
            crate::discover::Cost {
                source: Some("ext4".into()),
                ..Default::default()
            },
            None,
        );
        let text = preview_text(&e, 44);
        assert!(text.contains("ext4"), "{text}");
        assert!(
            !text.contains("network"),
            "an ordinary disk should say nothing alarming: {text}"
        );
    }

    #[test]
    fn what_a_file_weighs_open_is_stated_with_its_ratio() {
        // The number nothing else on screen implies.
        let e = costed(
            "prices.parquet",
            crate::discover::Cost {
                source: Some("ext4".into()),
                uncompressed: Some(2_000_000_000),
                codec: Some("zstd".into()),
                row_groups: Some(12),
                ..Default::default()
            },
            Some(200_000_000),
        );
        let text = preview_text(&e, 44);
        assert!(text.contains("in memory"), "{text}");
        assert!(text.contains("zstd"), "{text}");
        assert!(text.contains("10.0"), "the ratio should be stated: {text}");
        assert!(text.contains("row groups"), "{text}");
    }

    #[test]
    fn a_ratio_too_small_to_matter_is_left_out() {
        // Below about 1.2x the number is noise dressed as insight.
        let e = costed(
            "prices.parquet",
            crate::discover::Cost {
                source: Some("ext4".into()),
                uncompressed: Some(1_050_000),
                codec: Some("uncompressed".into()),
                ..Default::default()
            },
            Some(1_000_000),
        );
        let text = preview_text(&e, 44);
        assert!(!text.contains("1.0×") && !text.contains("1.1×"), "{text}");
    }

    #[test]
    fn a_partition_layout_names_its_keys_and_its_range() {
        let e = costed(
            "events",
            crate::discover::Cost {
                source: Some("nfs4".into()),
                partitions: Some(crate::discover::Partitions {
                    keys: vec!["year".into(), "region".into()],
                    first_key_values: vec!["2023".into(), "2024".into(), "2025".into()],
                    count: 3,
                    more: false,
                }),
                ..Default::default()
            },
            None,
        );
        let text = preview_text(&e, 44);
        assert!(text.contains("3 by year, region"), "{text}");
        assert!(text.contains("year 2023 to 2025"), "{text}");
    }

    #[test]
    fn a_bounded_partition_count_says_it_is_a_floor() {
        let e = costed(
            "daily",
            crate::discover::Cost {
                partitions: Some(crate::discover::Partitions {
                    keys: vec!["day".into()],
                    first_key_values: vec!["0001".into()],
                    count: 512,
                    more: true,
                }),
                ..Default::default()
            },
            None,
        );
        assert!(preview_text(&e, 44).contains("512+"));
    }

    #[test]
    fn the_preview_never_draws_past_its_pane() {
        let e = costed(
            "a_dataset_with_a_very_long_name_indeed.parquet",
            crate::discover::Cost {
                source: Some("fuse.sshfs".into()),
                uncompressed: Some(9_000_000_000),
                codec: Some("zstd".into()),
                row_groups: Some(1024),
                partitions: Some(crate::discover::Partitions {
                    keys: vec!["year".into(), "month".into(), "day".into()],
                    first_key_values: vec!["2001".into(), "2025".into()],
                    count: 9999,
                    more: true,
                }),
            },
            Some(400_000_000),
        );
        for width in [24usize, 40, 80] {
            let ctx = RenderContext::for_test();
            for line in preview_head(&e, None, width, &ctx) {
                // The pane wraps rather than clips, so a long value is allowed to run
                // on; what must not happen is a *heading* bar overrunning its width.
                let text: String = l_text(&line);
                if text.trim_start().starts_with("OPENING") {
                    assert!(
                        text.chars().count() <= width,
                        "a {width}-wide pane drew a {}-character heading",
                        text.chars().count()
                    );
                }
            }
        }
    }

    fn l_text(line: &Line<'_>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn the_matched_characters_are_the_highlighted_ones() {
        let spans = row_spans("sales_2024.parquet", "sales", None);
        assert_eq!(highlighted_text(&spans), "sales");
        // Reassembling the spans must give back exactly the name; highlighting is a
        // change of style, never of text. The locality marker sits between the
        // selection marker and the name, so the name is what follows it rather than
        // what the row starts with.
        let text: String = spans.iter().map(|(t, _)| t.as_str()).collect();
        assert!(text.contains("sales_2024.parquet"), "got {text:?}");
    }

    #[test]
    fn a_scattered_match_highlights_each_run_separately() {
        let spans = row_spans("sales_by_region.parquet", "sreg", None);
        assert_eq!(highlighted_text(&spans), "sreg");
        let runs = spans.iter().filter(|(_, hit)| *hit).count();
        assert!(
            runs >= 2,
            "a match spread across the name should be several runs, got {runs}"
        );
    }

    #[test]
    fn consecutive_matches_become_one_span_not_a_stutter() {
        let spans = row_spans("sales.parquet", "sales", None);
        let runs = spans.iter().filter(|(_, hit)| *hit).count();
        assert_eq!(runs, 1, "five adjacent characters are one run, got {runs}");
    }

    #[test]
    fn nothing_is_highlighted_without_a_filter() {
        let spans = row_spans("sales.parquet", "", None);
        assert_eq!(highlighted_text(&spans), "");
    }

    #[test]
    fn a_row_matched_by_a_column_highlights_the_column_not_the_name() {
        // Marks scattered over a name that had nothing to do with the match read as
        // the filter having gone wrong.
        let spans = row_spans("orders.parquet", "cust", Some("customer_id"));
        assert_eq!(highlighted_text(&spans), "cust");
        let text: String = spans.iter().map(|(t, _)| t.as_str()).collect();
        assert!(
            text.contains("·customer_id"),
            "the column note should still read whole, got {text:?}"
        );
    }

    #[test]
    fn the_title_survives_a_note_that_wants_the_whole_line() {
        // Trimming the note first is the point: a header that says only where it
        // looked, and not what it is, has lost the more useful half.
        let section = Section {
            title: "Found".to_string(),
            subtitle: Some("x".repeat(200)),
            rows: Vec::new(),
            unavailable: false,
            unavailable_note: None,
            folded_by_default: false,
            remote_root: None,
            waiting: false,
        };
        let ctx = RenderContext::for_test();
        let line = section_header(&section, 3, false, false, 40, 0, &ctx);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(
            text.contains("FOUND"),
            "the title should still be readable, got {text:?}"
        );
    }
}

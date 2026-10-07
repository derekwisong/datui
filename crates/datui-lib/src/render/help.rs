//! The help overlay: a screen's keys in task groups, in two columns where the
//! terminal is wide enough, sized to its content and scrolled only when the
//! terminal is too small for it.

use crate::glyphs::{asciify_instructions, display_width, take_columns};
use crate::help::{Block, Help, Line};
use crate::render::context::RenderContext;
use crate::widgets::ui::{HintBar, SectionRule, Surface};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;

/// The narrowest a column of keys is laid out at.
const MIN_COLUMN: u16 = 44;
/// Between two columns.
const GAP: u16 = 3;
/// The widest the overlay grows: two columns at a comfortable measure.
const MAX_WIDTH: u16 = 140;
/// One column's widest: a reading surface caps its measure.
const MAX_ONE_COLUMN: u16 = 84;
/// The key column is never wider than this; a longer key wraps its line under it.
const MAX_KEY: usize = 18;

/// A drawn line of a column.
enum Drawn {
    Blank,
    Heading(&'static str),
    /// A key's line, or its continuation: the key text on the first, the index of the
    /// key among those shown.
    Key {
        index: usize,
        key: Option<String>,
        text: String,
        key_width: usize,
        /// Whether Enter presses it here; a key that would type into a text field
        /// is dimmed.
        runs: bool,
    },
    Note(String, bool),
}

/// `text` broken into lines of at most `width` columns, between words where it can.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split(' ') {
        let mut word = word.to_string();
        loop {
            let used = display_width(&line);
            let len = display_width(&word);
            let sep = usize::from(!line.is_empty());
            if used + sep + len <= width {
                if sep == 1 {
                    line.push(' ');
                }
                line.push_str(&word);
                break;
            }
            if !line.is_empty() {
                lines.push(std::mem::take(&mut line));
                continue;
            }
            // A word longer than the line: cut it, a character at least, so a wide
            // character on a one-column line still moves on.
            let mut head = take_columns(&word, width).to_string();
            if head.is_empty() {
                head = word.chars().next().map(String::from).unwrap_or_default();
            }
            word = word[head.len()..].to_string();
            lines.push(head);
            if word.is_empty() {
                break;
            }
        }
    }
    if !line.is_empty() || lines.is_empty() {
        lines.push(line);
    }
    lines
}

/// The lines of `blocks` in a column `width` wide; `first` is the index of the first
/// key among those shown.
fn lay_out(help: &Help, blocks: &[Block], width: usize, first: usize) -> Vec<Drawn> {
    let ascii = |s: &str| asciify_instructions(s).into_owned();
    let key_width = blocks
        .iter()
        .flat_map(|b| &b.lines)
        .filter_map(|l| match l {
            Line::Key(k) => Some(display_width(&ascii(k.keys))),
            Line::Note(..) => None,
        })
        .filter(|&w| w <= MAX_KEY)
        .max()
        .unwrap_or(0);
    let mut out = Vec::new();
    let mut index = first;
    for (n, block) in blocks.iter().enumerate() {
        if n > 0 {
            out.push(Drawn::Blank);
        }
        out.push(Drawn::Heading(block.name));
        for line in &block.lines {
            match line {
                Line::Key(key) => {
                    let keys = ascii(key.keys);
                    let runs = help.runnable(key).is_some();
                    // Past the rail, the key column and two spaces.
                    let room = width.saturating_sub(1 + key_width + 2);
                    let mut text = wrap(&ascii(key.line), room.max(8)).into_iter();
                    if display_width(&keys) > key_width {
                        // A long key takes its own line; its description goes under.
                        out.push(Drawn::Key {
                            index,
                            key: Some(keys),
                            text: String::new(),
                            key_width,
                            runs,
                        });
                    } else {
                        out.push(Drawn::Key {
                            index,
                            key: Some(keys),
                            text: text.next().unwrap_or_default(),
                            key_width,
                            runs,
                        });
                    }
                    for rest in text {
                        out.push(Drawn::Key {
                            index,
                            key: None,
                            text: rest,
                            key_width,
                            runs,
                        });
                    }
                    index += 1;
                }
                Line::Note(example, meaning) => {
                    let example = ascii(example);
                    let meaning = ascii(meaning);
                    let room = width.saturating_sub(1);
                    if display_width(&example) + 2 + display_width(&meaning) <= room {
                        let pad = room - display_width(&example) - display_width(&meaning);
                        out.push(Drawn::Note(
                            format!("{example}{}{meaning}", " ".repeat(pad.clamp(2, 4))),
                            false,
                        ));
                    } else {
                        for part in wrap(&example, room) {
                            out.push(Drawn::Note(part, false));
                        }
                        for part in wrap(&meaning, room.saturating_sub(4)) {
                            out.push(Drawn::Note(format!("    {part}"), true));
                        }
                    }
                }
            }
        }
    }
    out
}

/// The keys in `blocks`.
fn key_count(blocks: &[Block]) -> usize {
    blocks
        .iter()
        .flat_map(|b| &b.lines)
        .filter(|l| matches!(l, Line::Key(_)))
        .count()
}

/// Split `blocks` into columns of `width`: two when `two`, as even as the order
/// allows.
fn columns(help: &Help, blocks: &[Block], width: usize, two: bool) -> Vec<Vec<Drawn>> {
    if !two || blocks.len() < 2 {
        return vec![lay_out(help, blocks, width, 0)];
    }
    let mut best: Option<(usize, Vec<Vec<Drawn>>)> = None;
    for split in 1..blocks.len() {
        let left = lay_out(help, &blocks[..split], width, 0);
        let right = lay_out(help, &blocks[split..], width, key_count(&blocks[..split]));
        let height = left.len().max(right.len());
        if best.as_ref().is_none_or(|(h, _)| height < *h) {
            best = Some((height, vec![left, right]));
        }
    }
    best.map(|(_, c)| c).unwrap_or_default()
}

/// The overlay's frame in `area`: wide enough for two columns where the terminal is,
/// and as tall as its content.
fn frame(area: Rect, two: bool, content_height: u16) -> Rect {
    let width = if two {
        area.width.saturating_sub(4).min(MAX_WIDTH)
    } else {
        area.width.saturating_sub(2).min(MAX_ONE_COLUMN)
    };
    // The frame's borders, the footer and the gap above it.
    let height = (content_height + 4)
        .min(area.height.saturating_sub(2))
        .max(5.min(area.height));
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

/// Draw the help over `area`. The scroll is kept so the selected key is in view and
/// written back to `help`.
pub fn render_help(area: Rect, buf: &mut Buffer, help: &mut Help, ctx: &RenderContext) {
    // Help owns the clicks over the view: only its footer's keys take them.
    crate::pointer::record(area, crate::pointer::Hit::Modal);
    let blocks = help.blocks();
    let shown = key_count(&blocks);
    if shown > 0 {
        help.selected = help.selected.min(shown - 1);
    }
    let two = area.width >= 2 * MIN_COLUMN + GAP + 8;
    let width = if two {
        area.width.saturating_sub(4).min(MAX_WIDTH)
    } else {
        area.width.saturating_sub(2).min(MAX_ONE_COLUMN)
    };
    // Inside the frame: two border columns, a gutter each side, and the scrollbar's.
    let inner_width = width.saturating_sub(5);
    let column_width = if two {
        (inner_width - GAP) / 2
    } else {
        inner_width
    };
    let cols = columns(help, &blocks, column_width as usize, two);
    let filter_line = u16::from(help.filtering || !help.filter.is_empty());
    let body_height = cols.iter().map(Vec::len).max().unwrap_or(0) as u16;
    // The filter line, the body, a blank, and the reference.
    let content = filter_line + body_height.max(1) + 2;
    let popup = frame(area, two, content);

    let footer = HintBar::from_ctx(ctx).screen(Context::Help).key("Enter");
    let footer = if help.filtering {
        footer.key_as("Esc", "Clear")
    } else {
        footer.key("/").key("Esc")
    };
    let title = help.title();
    let inner = Surface::new(&title).footer(&footer).render(popup, buf, ctx);
    if inner.height == 0 || inner.width < 2 {
        return;
    }
    let scrollbar_x = inner.right().saturating_sub(1);
    let inner = Rect {
        width: inner.width - 1,
        ..inner
    };

    let mut y = inner.y;
    if filter_line == 1 {
        let g = crate::glyphs::get();
        buf.set_string(inner.x, y, "/", Style::default().fg(ctx.accent));
        let text = take_columns(&help.filter, inner.width.saturating_sub(3) as usize);
        buf.set_string(inner.x + 2, y, text, Style::default().fg(ctx.text_primary));
        if help.filtering {
            let at = inner.x + 2 + display_width(text) as u16;
            if at < inner.right() {
                buf.set_string(at, y, " ", Style::default().bg(ctx.accent));
            }
        } else if shown == 0 {
            let _ = g;
        }
        y += 1;
    }

    // The reference line is the last, under a blank.
    let reference_y = inner.bottom().saturating_sub(1);
    let body = Rect {
        x: inner.x,
        y,
        width: inner.width,
        height: reference_y.saturating_sub(y).saturating_sub(1),
    };
    let reference = {
        let full = format!(
            "All keys: datui man keys {} {}",
            crate::glyphs::get().middot,
            datui_cli::keys::REFERENCE_URL
        );
        if display_width(&full) <= inner.width as usize {
            full
        } else {
            "All keys: datui man keys".to_string()
        }
    };
    if reference_y > body.y {
        buf.set_string(
            inner.x,
            reference_y,
            take_columns(&reference, inner.width as usize),
            Style::default().fg(ctx.dimmed),
        );
    }

    if shown == 0 && blocks.is_empty() {
        buf.set_string(
            body.x,
            body.y,
            "No key matches",
            Style::default().fg(ctx.dimmed),
        );
        return;
    }

    // Keep the selected key in view.
    let height = body.height as usize;
    let total = body_height as usize;
    let rows_of_selected = cols.iter().find_map(|col| {
        let rows: Vec<usize> = col
            .iter()
            .enumerate()
            .filter(|(_, d)| matches!(d, Drawn::Key { index, .. } if *index == help.selected))
            .map(|(i, _)| i)
            .collect();
        (!rows.is_empty()).then(|| (rows[0], *rows.last().unwrap_or(&rows[0])))
    });
    if let Some((top, bottom)) = rows_of_selected {
        if bottom >= help.scroll + height {
            help.scroll = bottom + 1 - height.max(1);
        }
        if top < help.scroll {
            // The group's heading with its first key.
            help.scroll = top.saturating_sub(1);
        }
    }
    help.scroll = help.scroll.min(total.saturating_sub(height));

    let g = crate::glyphs::get();
    for (c, col) in cols.iter().enumerate() {
        let x = body.x + c as u16 * (column_width + GAP);
        let area = Rect {
            x,
            width: column_width.min(body.right().saturating_sub(x)),
            ..body
        };
        for (row, drawn) in col.iter().skip(help.scroll).take(height).enumerate() {
            let line = Rect {
                y: area.y + row as u16,
                height: 1,
                ..area
            };
            draw(line, buf, drawn, help.selected, g, ctx);
        }
    }

    if total > height && height > 0 {
        let max_scroll = total - height;
        let bar = body.height;
        let thumb = ((height as f64 / total as f64) * bar as f64).max(1.0) as u16;
        let pos = ((help.scroll as f64 / max_scroll as f64) * (bar - thumb.min(bar)) as f64) as u16;
        for y in 0..bar {
            // The thumb and the track in glyphs of their own, so the bar reads in
            // the ASCII set and on a terminal without color.
            let (glyph, color) = if y >= pos && y < pos + thumb {
                (g.scroll_thumb, ctx.text_primary)
            } else {
                (g.scroll_track, ctx.dimmed)
            };
            buf.set_string(scrollbar_x, body.y + y, glyph, Style::default().fg(color));
        }
    }
}

fn draw(
    line: Rect,
    buf: &mut Buffer,
    drawn: &Drawn,
    selected: usize,
    g: &crate::glyphs::Glyphs,
    ctx: &RenderContext,
) {
    let width = line.width as usize;
    match drawn {
        Drawn::Blank => {}
        Drawn::Heading(name) => SectionRule {
            title: name,
            chip: None,
        }
        .render(line, buf, ctx),
        Drawn::Key {
            index,
            key,
            text,
            key_width,
            runs,
        } => {
            let is_selected = *index == selected;
            let base = if is_selected {
                ctx.highlight_style()
            } else {
                Style::default()
            };
            if is_selected {
                buf.set_style(line, base);
                buf.set_string(line.x, line.y, g.rail, base.fg(ctx.accent));
            }
            let mut x = line.x + 1;
            if let Some(key) = key {
                buf.set_string(
                    x,
                    line.y,
                    take_columns(key, width.saturating_sub(1)),
                    base.fg(if *runs { ctx.accent } else { ctx.dimmed }),
                );
            }
            x += (*key_width + 2) as u16;
            if x < line.right() {
                buf.set_string(
                    x,
                    line.y,
                    take_columns(text, (line.right() - x) as usize),
                    base.fg(ctx.text_primary),
                );
            }
        }
        Drawn::Note(text, dim) => {
            let style = Style::default().fg(if *dim { ctx.dimmed } else { ctx.text_primary });
            buf.set_string(
                line.x + 1,
                line.y,
                take_columns(text, width.saturating_sub(1)),
                style,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datui_cli::keys::Context;

    fn screen(help: &mut Help, width: u16, height: u16) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_help(area, &mut buf, help, &RenderContext::for_test());
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    #[test]
    fn wrap_breaks_between_words() {
        assert_eq!(wrap("the quick brown fox", 9), ["the quick", "brown fox"]);
        assert_eq!(wrap("abcdefghij", 4), ["abcd", "efgh", "ij"]);
        assert_eq!(wrap("", 4), [""]);
        // A wide character on a one-column line still moves on.
        assert_eq!(wrap("世界", 1), ["世", "界"]);
    }

    /// On a wide terminal the table's keys sit in two columns and fit without
    /// scrolling, the keys of every screen and the reference among them.
    #[test]
    fn the_table_help_fits_two_columns_at_160_by_50() {
        let mut help = Help::default();
        help.open(Context::Table, false);
        let rows = screen(&mut help, 160, 50);
        let text = rows.join("\n");
        assert!(text.contains("Table Help"), "{text}");
        for want in [
            "Explore",
            "Analyze",
            "Everywhere",
            "Value counts",
            "datui man keys",
        ] {
            assert!(text.contains(want), "{want} missing:\n{text}");
        }
        assert_eq!(help.scroll, 0, "nothing scrolled:\n{text}");
        // Two columns: Explore's heading and another group's share a row.
        assert!(
            rows.iter().any(|r| r.contains("Explore")
                && r.matches('─').count() > 10
                && r.trim_start().len() > 80),
            "{text}"
        );
    }

    /// At 80×24 the help is one column and scrolls to keep the selection in view.
    #[test]
    fn at_80_by_24_the_selection_stays_in_view() {
        let mut help = Help::default();
        help.open(Context::Table, false);
        help.selected = help.shown_keys().len() - 1;
        let rows = screen(&mut help, 80, 24);
        let text = rows.join("\n");
        assert!(help.scroll > 0, "{text}");
        let last = help.shown_keys().last().unwrap().line;
        assert!(text.contains(&last[..20.min(last.len())]), "{text}");
        assert!(rows.iter().all(|r| display_width(r) <= 80));
    }

    /// A help that scrolls draws its bar in two glyphs: the thumb, and the track
    /// above and below it, each with an ASCII twin.
    #[test]
    fn the_scrollbar_track_has_its_own_glyph() {
        let g = crate::glyphs::get();
        let mut help = Help::default();
        help.open(Context::Table, false);
        let rows = screen(&mut help, 80, 24);
        let bar = rows.join("\n");
        assert!(bar.contains(g.scroll_thumb), "{bar:?}");
        assert!(bar.contains(g.scroll_track), "{bar:?}");
        assert_ne!(g.scroll_thumb, g.scroll_track);
    }

    /// The filter shows on its own line, and a filter matching nothing says so.
    #[test]
    fn the_filter_is_shown() {
        let mut help = Help::default();
        help.open(Context::Table, false);
        help.filtering = true;
        help.filter = "zzz".into();
        let text = screen(&mut help, 100, 30).join("\n");
        assert!(text.contains("/ zzz"), "{text}");
        assert!(text.contains("No key matches"), "{text}");
    }

    /// Every screen's help draws at 60×20, the smallest the app supports.
    #[test]
    fn every_screen_draws_small() {
        for s in datui_cli::keys::SCREENS {
            let mut help = Help::default();
            help.open(s.context, false);
            let text = screen(&mut help, 60, 20).join("\n");
            assert!(text.contains("Help"), "{}:\n{text}", s.title);
        }
    }
}

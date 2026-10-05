//! The hex view: a takeover of the main area. A header line names the file, a header
//! row numbers the bytes, then a row of bytes per line (offset, hex, ASCII), with the
//! byte inspector beside them when there is room, and a status line at the foot. The
//! keys are on the control bar.

use crate::hex_view::{ByteClass, Geometry, HexView, PromptKind, class, hex_x, readings};
use crate::render::context::RenderContext;
use crate::widgets::ui::Surface;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

pub fn render(area: Rect, buf: &mut Buffer, app: &mut crate::App, ctx: &RenderContext) {
    let has_specs = app.has_format_specs();
    if let Some(view) = app.hex.as_mut() {
        draw(area, buf, view, ctx, has_specs);
    }
}

/// The color of a byte of `class`.
fn class_color(class: ByteClass, ctx: &RenderContext) -> ratatui::style::Color {
    match class {
        ByteClass::Null => ctx.hex_null,
        ByteClass::Printable => ctx.hex_printable,
        ByteClass::Whitespace => ctx.hex_whitespace,
        ByteClass::Control => ctx.hex_control,
        ByteClass::High => ctx.hex_high,
        ByteClass::Ff => ctx.hex_ff,
    }
}

/// An offset as the offset column shows it.
pub fn offset_text(at: u64, digits: usize, decimal: bool) -> String {
    if decimal {
        format!("{at:>digits$}")
    } else {
        format!("{at:0digits$x}")
    }
}

/// Rows the prompt takes at the foot, frame included.
fn prompt_rows(view: &HexView) -> u16 {
    match view.prompt {
        None => 0,
        Some(PromptKind::Find) => 4,
        Some(_) if view.prompt_error.is_some() => 4,
        Some(_) => 3,
    }
}

/// Draw `view` into `area`.
pub fn draw(
    area: Rect,
    buf: &mut Buffer,
    view: &mut HexView,
    ctx: &RenderContext,
    has_specs: bool,
) {
    Clear.render(area, buf);
    if area.height < 3 || area.width < 8 {
        return;
    }
    let g = crate::glyphs::get();
    let prompt = prompt_rows(view).min(area.height.saturating_sub(3));
    let body_height = area.height - 2 - prompt;
    // Without room beside the bytes, an open inspector takes the rows under them, all
    // but a few: the cursor's row stays on screen above it.
    let below = if !view.panel_fits(area.width) && view.inspector_open && !view.is_empty() {
        let lines = readings(view.slice(view.cursor, crate::hex_view::INSPECTED)).len() as u16 + 2;
        lines.min(body_height.saturating_sub(4))
    } else {
        0
    };
    // Header, byte numbers, status line, the prompt when one is open, and the
    // inspector when it is under the bytes.
    let rows = body_height.saturating_sub(1 + below) as usize;
    let geometry = view.lay_out(area.width, rows);

    header(Rect { height: 1, ..area }, buf, view, &geometry, ctx);
    let body = Rect {
        y: area.y + 1,
        height: body_height,
        ..area
    };
    let bytes_area = if geometry.panel {
        // The panel sits against the bytes, not at the far edge: on a wide screen the
        // readings stay next to the bytes they read.
        let used = crate::hex_view::row_width(geometry.shown, geometry.digits, geometry.ascii) + 2;
        Rect {
            width: (used as u16).min(body.width - crate::hex_view::PANEL_WIDTH - 1),
            ..body
        }
    } else {
        Rect {
            height: body.height - below,
            ..body
        }
    };
    column_numbers(
        Rect {
            height: 1,
            ..bytes_area
        },
        buf,
        view,
        &geometry,
        ctx,
    );
    let rows_area = Rect {
        y: bytes_area.y + 1,
        height: bytes_area.height.saturating_sub(1),
        ..bytes_area
    };
    if view.is_empty() {
        Paragraph::new(Span::styled("Empty file", Style::default().fg(ctx.dimmed))).render(
            Rect {
                height: 1,
                ..rows_area
            },
            buf,
        );
    } else {
        byte_rows(rows_area, buf, view, &geometry, ctx);
    }
    if geometry.panel {
        let x = body.x + bytes_area.width;
        for y in body.y..body.y + body.height {
            buf[(x, y)]
                .set_symbol(g.rule)
                .set_style(Style::default().fg(ctx.column_separator));
        }
        let panel = Rect {
            x: x + 1,
            width: crate::hex_view::PANEL_WIDTH,
            ..body
        };
        inspector(panel, buf, view, ctx);
    } else if below > 0 {
        let under = Rect {
            y: body.y + body.height - below,
            height: below,
            width: body.width.min(crate::hex_view::PANEL_WIDTH + 12),
            ..body
        };
        inspector(under, buf, view, ctx);
    }
    if prompt > 0 {
        let strip = Rect {
            y: area.y + area.height - 1 - prompt,
            height: prompt,
            ..area
        };
        prompt_strip(strip, buf, view, ctx);
    }
    status(
        Rect {
            y: area.y + area.height - 1,
            height: 1,
            ..area
        },
        buf,
        view,
        has_specs,
        ctx,
    );
    if let Some(picker) = &view.picker {
        crate::render::datatable_main::render_picker(
            area,
            buf,
            picker,
            ("Format", "Read", "No spec matches"),
            ctx,
        );
    }
}

fn header(area: Rect, buf: &mut Buffer, view: &HexView, geometry: &Geometry, ctx: &RenderContext) {
    let g = crate::glyphs::get();
    let dot = g.middot;
    let size = crate::numfmt::group_chrome(view.len() as usize);
    let mut text = format!(
        "Hex {dot} {} {dot} {size} {}",
        view.name(),
        if view.len() == 1 { "byte" } else { "bytes" }
    );
    let fixed = if view.record_size.is_some() {
        "fixed"
    } else {
        "auto"
    };
    text.push_str(&format!(" {dot} {} a row ({fixed})", geometry.per_row));
    if geometry.shown < geometry.per_row {
        text.push_str(&format!(
            ", {} to {} shown",
            geometry.first_col,
            geometry.first_col + geometry.shown - 1
        ));
    }
    let style = Style::default().bg(ctx.controls_bg).fg(ctx.table_header);
    buf.set_style(area, style);
    Paragraph::new(crate::glyphs::fit_cells(&text, area.width as usize, g.ellipsis).into_owned())
        .style(style.add_modifier(Modifier::BOLD))
        .render(area, buf);
}

/// The header row: what the offsets count in, and each byte's place in the row.
fn column_numbers(
    area: Rect,
    buf: &mut Buffer,
    view: &HexView,
    geometry: &Geometry,
    ctx: &RenderContext,
) {
    let style = Style::default()
        .bg(ctx.table_header_bg)
        .fg(ctx.table_header);
    buf.set_style(area, style);
    let label = if view.decimal { "offset" } else { "offset h" };
    let label = format!("{label:<width$}", width = geometry.digits);
    buf.set_stringn(area.x, area.y, &label, area.width as usize, style);
    let hex_start = area.x as usize + geometry.digits + 2;
    let col = (view.cursor % geometry.per_row as u64) as usize;
    let base = hex_x(geometry.first_col);
    for i in geometry.first_col..geometry.first_col + geometry.shown {
        let x = hex_start + hex_x(i) - base;
        if x + 2 > (area.x + area.width) as usize {
            break;
        }
        let text = format!("{:02x}", i % 256);
        let cell = if i == col {
            style.fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            style
        };
        buf.set_string(x as u16, area.y, text, cell);
    }
}

fn byte_rows(
    area: Rect,
    buf: &mut Buffer,
    view: &HexView,
    geometry: &Geometry,
    ctx: &RenderContext,
) {
    let g = crate::glyphs::get();
    let per = geometry.per_row as u64;
    let rows = (area.height as usize).min(geometry.rows);
    let lo = view.top;
    let hi = (view.top + per * rows as u64).min(view.len());
    let matches = view.matches_on_screen(lo, hi);
    let in_match = |at: u64| matches.iter().any(|&(s, e)| s <= at && at < e);
    let selection = view.selection();
    let selected = |at: u64| selection.is_some_and(|(s, e)| s <= at && at <= e);
    let cursor_style = ctx.cell_cursor_style();
    let match_style = ctx.find_match;
    let select_style = ctx.highlight_style();
    let hex_start = area.x + geometry.digits as u16 + 2;
    let base = hex_x(geometry.first_col);
    let ascii_start = hex_start as usize + crate::hex_view::hex_width(geometry.shown) + 2;
    let right = (area.x + area.width) as usize;
    let cursor_row = view.cursor / per;
    for r in 0..rows {
        let row_at = view.top + r as u64 * per;
        if row_at >= view.len() {
            break;
        }
        let y = area.y + r as u16;
        let offset_style = if row_at / per == cursor_row {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.row_numbers)
        };
        buf.set_stringn(
            area.x,
            y,
            offset_text(row_at, geometry.digits, view.decimal),
            geometry.digits.min(area.width as usize),
            offset_style,
        );
        for i in geometry.first_col..geometry.first_col + geometry.shown {
            let at = row_at + i as u64;
            if at >= view.len() {
                break;
            }
            let b = view.bytes.as_slice()[at as usize];
            let color = class_color(class(b), ctx);
            let style = if at == view.cursor {
                cursor_style
            } else if in_match(at) {
                match_style
            } else if selected(at) {
                select_style.fg(color)
            } else {
                Style::default().fg(color)
            };
            let x = hex_start as usize + hex_x(i) - base;
            if x + 2 <= right {
                buf.set_string(x as u16, y, format!("{b:02x}"), style);
                // A marked range reads as one run: the gaps inside it are tinted too.
                let next = at + 1;
                if i + 1 < geometry.first_col + geometry.shown
                    && next < view.len()
                    && selected(at)
                    && selected(next)
                {
                    let gap_end = hex_start as usize + hex_x(i + 1) - base;
                    for gx in x + 2..gap_end.min(right) {
                        buf[(gx as u16, y)].set_style(select_style);
                    }
                }
            }
            if geometry.ascii {
                let ax = ascii_start + (i - geometry.first_col);
                if ax < right {
                    let symbol = match b {
                        0x20..=0x7e => (b as char).to_string(),
                        _ => g.hex_dot.to_string(),
                    };
                    buf[(ax as u16, y)].set_symbol(&symbol).set_style(style);
                }
            }
        }
    }
}

/// The readings at the cursor: a title, then a line per reading, little-endian and
/// big-endian side by side.
fn inspector(area: Rect, buf: &mut Buffer, view: &HexView, ctx: &RenderContext) {
    if area.height == 0 || area.width < 10 || view.is_empty() {
        return;
    }
    let g = crate::glyphs::get();
    let title = format!(
        "At {}",
        offset_text(view.cursor, 1, view.decimal).trim_start()
    );
    let title = if view.decimal {
        title
    } else {
        format!("At 0x{:x}", view.cursor)
    };
    crate::widgets::ui::SectionRule {
        title: &title,
        chip: None,
    }
    .render(Rect { height: 1, ..area }, buf, ctx);
    let label_w = 9usize;
    let value_w = (area.width as usize).saturating_sub(label_w + 2) / 2;
    let mut y = area.y + 1;
    let bottom = area.y + area.height;
    if y < bottom {
        let head = format!("{:label_w$} {:value_w$} {}", "", "LE", "BE",);
        buf.set_stringn(
            area.x,
            y,
            head,
            area.width as usize,
            Style::default().fg(ctx.dimmed),
        );
        y += 1;
    }
    let readings = readings(view.slice(view.cursor, crate::hex_view::INSPECTED));
    let fit = |text: &str, w: usize| crate::glyphs::fit_cells(text, w, g.ellipsis).into_owned();
    for reading in &readings {
        if y >= bottom {
            break;
        }
        buf.set_stringn(
            area.x,
            y,
            format!("{:label_w$}", reading.label),
            label_w,
            Style::default().fg(ctx.label),
        );
        let x = area.x + label_w as u16 + 1;
        let full = (area.width as usize).saturating_sub(label_w + 1);
        match &reading.be {
            // Too long for the columns side by side: little-endian on this line, and
            // big-endian under it.
            Some(be)
                if crate::glyphs::display_width(&reading.le) > value_w
                    || crate::glyphs::display_width(be) > value_w =>
            {
                buf.set_string(x, y, fit(&reading.le, full), Style::default());
                if !be.is_empty() && y + 1 < bottom {
                    y += 1;
                    buf.set_string(
                        area.x,
                        y,
                        format!("{:>label_w$}", "BE"),
                        Style::default().fg(ctx.dimmed),
                    );
                    buf.set_string(x, y, fit(be, full), Style::default());
                }
            }
            Some(be) => {
                buf.set_string(x, y, fit(&reading.le, value_w), Style::default());
                buf.set_string(
                    x + value_w as u16 + 1,
                    y,
                    fit(be, value_w),
                    Style::default(),
                );
            }
            None => {
                buf.set_string(x, y, fit(&reading.le, full), Style::default());
            }
        }
        y += 1;
    }
    if let Some((first, last)) = view.selection()
        && y < bottom
    {
        let n = last - first + 1;
        buf.set_stringn(
            area.x,
            y,
            format!(
                "{:label_w$} {} {}",
                "marked",
                crate::numfmt::group_chrome(n as usize),
                if n == 1 { "byte" } else { "bytes" }
            ),
            area.width as usize,
            Style::default().fg(ctx.accent),
        );
    }
}

fn prompt_strip(area: Rect, buf: &mut Buffer, view: &HexView, ctx: &RenderContext) {
    let Some(kind) = view.prompt else {
        return;
    };
    let title = match kind {
        PromptKind::GoTo => "Go to Offset",
        PromptKind::Find => "Find Bytes",
        PromptKind::RecordSize => "Bytes per Row",
    };
    let mut surface = Surface::new(title);
    if view.prompt_error.is_some() {
        surface = surface.border_style(Style::default().fg(ctx.modal_border_error));
    }
    let content = surface.render(area, buf, ctx);
    if content.height == 0 {
        return;
    }
    (&view.input).render(
        Rect {
            height: 1,
            ..content
        },
        buf,
    );
    if content.height < 2 {
        return;
    }
    let second = Rect {
        y: content.y + 1,
        height: 1,
        ..content
    };
    if let Some(error) = &view.prompt_error {
        let g = crate::glyphs::get();
        Paragraph::new(Span::styled(
            crate::glyphs::fit_cells(error, second.width as usize, g.ellipsis).into_owned(),
            Style::default().fg(ctx.error),
        ))
        .render(second, buf);
        return;
    }
    if kind == PromptKind::Find {
        let g = crate::glyphs::get();
        let check = if view.utf16 {
            g.checkbox_on
        } else {
            g.checkbox_off
        };
        let line = Line::from(vec![
            Span::styled(
                format!("{check} UTF-16"),
                Style::default().fg(if view.utf16 {
                    ctx.accent
                } else {
                    ctx.text_secondary
                }),
            ),
            Span::styled(
                "   text, 0x..., or hex pairs; ?? is any byte",
                Style::default().fg(ctx.text_secondary),
            ),
        ]);
        Paragraph::new(line).render(second, buf);
    }
}

fn status(area: Rect, buf: &mut Buffer, view: &HexView, has_specs: bool, ctx: &RenderContext) {
    let g = crate::glyphs::get();
    let dot = g.middot;
    let len = view.len();
    let mut parts = Vec::new();
    if len > 0 {
        let at = if view.decimal {
            format!(
                "{} of {}",
                crate::numfmt::group_chrome(view.cursor as usize),
                crate::numfmt::group_chrome(len as usize)
            )
        } else {
            format!("0x{:x} of 0x{:x}", view.cursor, len)
        };
        parts.push(at);
        let percent = if len <= 1 {
            100.0
        } else {
            view.cursor as f64 * 100.0 / (len - 1) as f64
        };
        parts.push(format!("{percent:.1}%"));
    }
    if let Some((first, last)) = view.selection() {
        let n = last - first + 1;
        parts.push(format!(
            "marked {} {}",
            crate::numfmt::group_chrome(n as usize),
            if n == 1 { "byte" } else { "bytes" }
        ));
    }
    if let Some(found) = &view.found {
        match found.hit {
            Some(_) => parts.push(format!("found {}", found.pattern.label)),
            None => parts.push(format!("no match for {}", found.pattern.label)),
        }
        if let Some(stride) = found.stride {
            parts.push(format!("every {stride} bytes"));
        }
    }
    let mut text = parts.join(&format!(" {dot} "));
    if view.fallback {
        let said = if has_specs {
            "format unknown, B reads it with a spec"
        } else {
            "format unknown"
        };
        if text.is_empty() {
            text = said.to_string();
        } else {
            text = format!("{text} {dot} {said}");
        }
    }
    Paragraph::new(crate::glyphs::fit_cells(&text, area.width as usize, g.ellipsis).into_owned())
        .style(Style::default().fg(ctx.text_secondary))
        .render(area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed_records::Bytes;
    use crate::hex_view::{HexSource, Origin};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn view(bytes: Vec<u8>) -> HexView {
        HexView::new(
            HexSource {
                path: PathBuf::from("sample.bin"),
                bytes: Arc::new(Bytes::Owned(bytes)),
            },
            Origin::Launch,
            false,
            1,
        )
    }

    fn screen(view: &mut HexView, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        draw(area, &mut buf, view, &ctx, false);
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

    fn sample() -> Vec<u8> {
        let mut bytes = b"PAR1 hello, world\n\0\0\x01\x02\xff\x80".to_vec();
        bytes.extend((0..=255u8).cycle().take(4000));
        bytes
    }

    #[test]
    fn sixty_by_twenty_shows_eight_bytes_a_row_and_the_gutter() {
        let mut v = view(sample());
        let s = screen(&mut v, 60, 20);
        assert!(s[0].starts_with("Hex"), "{s:?}");
        assert!(s[0].contains("8 a row"), "{s:?}");
        assert!(s[1].starts_with("offset h"), "{s:?}");
        assert_eq!(
            s[2], "00000000  50 41 52 31  20 68 65 6c  PAR1 hel",
            "{s:?}"
        );
        assert!(
            s[3].starts_with("00000008  6c 6f 2c 20  77 6f 72 6c"),
            "{s:?}"
        );
        assert!(s[19].starts_with("0x0 of 0x"), "{s:?}");
        assert!(
            !s.iter().any(|l| l.contains("LE")),
            "no room for the panel: {s:?}"
        );
    }

    #[test]
    fn eighty_by_twenty_four_shows_sixteen() {
        let mut v = view(sample());
        let s = screen(&mut v, 80, 24);
        let g = crate::glyphs::get();
        assert!(s[0].contains("16 a row"), "{s:?}");
        assert_eq!(
            s[3],
            format!(
                "00000010  64 0a 00 00  01 02 ff 80   00 01 02 03  04 05 06 07  d{}",
                g.hex_dot.repeat(15)
            )
        );
        assert!(s[1].contains("00 01 02 03  04 05 06 07   08"), "{s:?}");
    }

    #[test]
    fn wide_screens_carry_the_inspector_beside_the_bytes() {
        let mut v = view(sample());
        let s = screen(&mut v, 140, 40);
        assert!(s[0].contains("16 a row"), "{s:?}");
        assert!(s[1].contains("At 0x0"), "{s:?}");
        assert!(
            s.iter()
                .any(|l| l.contains("u32") && l.contains("827474256")),
            "{s:?}"
        );
        let s = screen(&mut v, 250, 60);
        assert!(s[0].contains("32 a row"), "{s:?}");
        assert!(
            s.iter()
                .any(|l| l.contains("text") && l.contains("PAR1 hello, world")),
            "{s:?}"
        );
        v.inspector = false;
        let s = screen(&mut v, 320, 60);
        assert!(s[0].contains("64 a row"), "{s:?}");
    }

    #[test]
    fn under_fifty_columns_the_gutter_goes() {
        let mut v = view(sample());
        let s = screen(&mut v, 44, 12);
        assert_eq!(s[2], "00000000  50 41 52 31  20 68 65 6c", "{s:?}");
    }

    #[test]
    fn an_empty_file_and_a_one_byte_file_draw() {
        let mut v = view(Vec::new());
        let s = screen(&mut v, 80, 24);
        assert_eq!(s[2], "Empty file");
        let mut v = view(vec![0x41]);
        let s = screen(&mut v, 80, 24);
        assert!(
            s[2].starts_with("00000000  41") && s[2].ends_with("A"),
            "{s:?}"
        );
        assert!(s[23].contains("100.0%"), "{s:?}");
    }

    #[test]
    fn a_wide_record_shows_the_cursor_s_part_of_it() {
        let mut v = view(sample());
        v.record_size = Some(100);
        v.go(99);
        let s = screen(&mut v, 80, 24);
        assert!(s[0].contains("100 a row (fixed)"), "{s:?}");
        assert!(s[0].contains("to 99 shown"), "{s:?}");
    }

    #[test]
    fn the_prompt_and_its_error_sit_at_the_foot() {
        let mut v = view(sample());
        v.prompt = Some(PromptKind::GoTo);
        v.input.set_value("0xzz");
        v.prompt_error = Some("0xzz is not an offset".to_string());
        let s = screen(&mut v, 80, 24);
        assert!(s.iter().any(|l| l.contains("Go to Offset")), "{s:?}");
        assert!(
            s.iter().any(|l| l.contains("0xzz is not an offset")),
            "{s:?}"
        );
        assert!(s[23].starts_with("0x0 of"), "{s:?}");
    }

    #[test]
    fn a_fallback_says_no_spec_matched_and_names_the_key() {
        let mut v = view(sample());
        v.fallback = true;
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 120, 10);
        let mut buf = Buffer::empty(area);
        draw(area, &mut buf, &mut v, &ctx, true);
        let last: String = (0..120).map(|x| buf[(x, 9)].symbol().to_string()).collect();
        assert!(last.contains("B reads it with a spec"), "{last}");
    }
    /// The screen at each size the issue names, against the text in `snapshots/`,
    /// drawn with the Unicode glyphs; the active set's glyphs stand in for them.
    #[test]
    fn screens_match_their_snapshots() {
        let (u, g) = (crate::glyphs::unicode(), crate::glyphs::get());
        for (width, height, expected) in [
            (60, 20, include_str!("snapshots/hex_view_60x20.txt")),
            (80, 24, include_str!("snapshots/hex_view_80x24.txt")),
            (140, 40, include_str!("snapshots/hex_view_140x40.txt")),
            (250, 60, include_str!("snapshots/hex_view_250x60.txt")),
        ] {
            let mut v = view(sample());
            v.go(5);
            let shown = screen(&mut v, width, height);
            let last = expected.lines().count() - 1;
            // The header and status lines separate with a middot; the gutter's dots
            // are the hex view's own glyph.
            let expected: Vec<String> = expected
                .lines()
                .enumerate()
                .map(|(i, line)| {
                    let dot = if i == 0 || i == last {
                        g.middot
                    } else {
                        g.hex_dot
                    };
                    line.replace(u.hex_dot, dot)
                        .replace(u.rule_h, g.rule_h)
                        .replace(u.rule, g.rule)
                })
                .collect();
            assert_eq!(shown, expected, "{width}x{height}");
        }
    }
}

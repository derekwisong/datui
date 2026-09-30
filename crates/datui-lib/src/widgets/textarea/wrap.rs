//! Soft wrapping: a long line breaks onto the rows below instead of scrolling
//! sideways.
//!
//! Wrapping is a matter of display only. The buffer keeps its lines as typed,
//! and a visual row is a character range of one of them. Rows break after the
//! last space that fits, or mid-word when a word is wider than the field.

use ratatui::{buffer::Buffer, layout::Rect};

use super::{TextArea, Viewport};

/// One visual row: buffer line and the character range `[start, end)` on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VisualRow {
    pub line: usize,
    pub start: usize,
    pub end: usize,
}

impl TextArea {
    /// Break long lines onto the rows below rather than scrolling sideways.
    pub fn set_wrap(&mut self, wrap: bool) {
        self.wrap = wrap;
    }

    pub fn wraps(&self) -> bool {
        self.wrap
    }

    /// Wrapping, and drawn at least once: before the first frame there is no
    /// width to wrap to, and movement goes by buffer line.
    pub(super) fn wraps_now(&self) -> bool {
        self.wrap && self.viewport.get().width > 0
    }

    /// Rows the buffer takes at `width` columns: one per line without
    /// wrapping.
    pub fn visual_rows(&self, width: u16) -> usize {
        if !self.wrap || width == 0 {
            return self.lines.len();
        }
        (0..self.lines.len())
            .map(|line| self.line_rows(line, width as usize).len())
            .sum()
    }

    /// The rows one line wraps into. A line that exactly fills its last row
    /// gets an empty row after it, where the cursor at its end is drawn, so
    /// the height does not change as the cursor moves.
    fn line_rows(&self, line: usize, width: usize) -> Vec<VisualRow> {
        let chars: Vec<char> = self.lines[line].chars().collect();
        let mut rows = Vec::new();
        let mut start = 0;
        let mut used = 0;
        // Just past the last space in the current row: where it breaks.
        let mut after_space = None;
        let mut i = 0;
        while i < chars.len() {
            let w = self.char_width(chars[i], used);
            if used + w > width && i > start {
                let end = after_space.filter(|&b| b > start).unwrap_or(i);
                rows.push(VisualRow { line, start, end });
                start = end;
                used = 0;
                after_space = None;
                i = start;
                continue;
            }
            used += w;
            if chars[i] == ' ' {
                after_space = Some(i + 1);
            }
            i += 1;
        }
        rows.push(VisualRow {
            line,
            start,
            end: chars.len(),
        });
        if used >= width {
            rows.push(VisualRow {
                line,
                start: chars.len(),
                end: chars.len(),
            });
        }
        rows
    }

    /// Every visual row of the buffer, in order.
    pub(super) fn layout(&self, width: usize) -> Vec<VisualRow> {
        (0..self.lines.len())
            .flat_map(|line| self.line_rows(line, width))
            .collect()
    }

    /// The visual row holding `(line, col)`: the last row of that line
    /// starting at or before the column.
    fn row_of(layout: &[VisualRow], (line, col): (usize, usize)) -> usize {
        layout
            .iter()
            .rposition(|r| r.line == line && r.start <= col)
            .unwrap_or(0)
    }

    /// Display width of `[start, col)` on a line.
    fn width_between(&self, line: usize, start: usize, col: usize) -> usize {
        let mut used = 0;
        for c in self.lines[line].chars().skip(start).take(col - start) {
            used += self.char_width(c, used);
        }
        used
    }

    /// The position one visual row up or down from the cursor, keeping its
    /// display column where the row allows. `None` at the first or last row.
    pub(super) fn visual_step(&self, down: bool) -> Option<(usize, usize)> {
        let width = self.viewport.get().width as usize;
        let layout = self.layout(width);
        let here = Self::row_of(&layout, self.cursor);
        let there = if down {
            Some(here + 1).filter(|&r| r < layout.len())?
        } else {
            here.checked_sub(1)?
        };
        let from = layout[here];
        let x = self.width_between(from.line, from.start, self.cursor.1);
        let to = layout[there];
        // The end of a row that continues below is the next row's start, so a
        // cursor landing there would show on the wrong row.
        let continues = layout
            .get(there + 1)
            .is_some_and(|next| next.line == to.line);
        let last = if continues && to.end > to.start {
            to.end - 1
        } else {
            to.end
        };
        let mut col = to.start;
        let mut used = 0;
        for c in self.lines[to.line]
            .chars()
            .skip(to.start)
            .take(last - to.start)
        {
            let w = self.char_width(c, used);
            if used + w > x {
                break;
            }
            used += w;
            col += 1;
        }
        Some((to.line, col))
    }

    /// Draw the buffer wrapped to `area`, scrolled by whole rows to keep the
    /// cursor in view.
    pub(super) fn render_wrapped(&self, area: Rect, buf: &mut Buffer) {
        let width = area.width as usize;
        let layout = self.layout(width);
        let cursor_row = Self::row_of(&layout, self.cursor);
        let height = area.height as usize;
        let mut top = self.viewport.get().row.min(layout.len().saturating_sub(1));
        if cursor_row < top {
            top = cursor_row;
        } else if height > 0 && cursor_row >= top + height {
            top = cursor_row + 1 - height;
        }
        self.viewport.set(Viewport {
            row: top,
            col: 0,
            height: area.height,
            width: area.width,
        });
        buf.set_style(area, self.style);

        for (offset, row) in layout.iter().skip(top).take(height).enumerate() {
            let y = area.y + offset as u16;
            let mut x = area.x;
            let mut used = 0;
            let chars = self.lines[row.line].chars().skip(row.start);
            for (i, c) in chars.take(row.end - row.start).enumerate() {
                let col = row.start + i;
                let w = self.char_width(c, used);
                used += w;
                if w == 0 {
                    continue;
                }
                let style = self.style_at(row.line, col);
                let symbol = if c == '\t' {
                    " ".to_string()
                } else {
                    c.to_string()
                };
                buf[(x, y)].set_symbol(&symbol).set_style(style);
                for extra in 1..w as u16 {
                    if x + extra < area.right() {
                        buf[(x + extra, y)].set_symbol(" ").set_style(style);
                    }
                }
                x = x.saturating_add(w as u16);
                if x >= area.right() {
                    break;
                }
            }
            let line_len = self.lines[row.line].chars().count();
            if row.end == line_len
                && self.cursor == (row.line, line_len)
                && Self::row_of(&layout, self.cursor) == top + offset
                && x < area.right()
            {
                buf[(x, y)]
                    .set_symbol(" ")
                    .set_style(self.style_at(row.line, line_len));
            }
        }
    }
}

//! Drawing a [`TextArea`] into a ratatui buffer.
//!
//! The widget is implemented on `&TextArea` so callers keep ownership of their
//! editor state across frames. Scroll position is the one thing rendering needs
//! to write back, and it lives in a [`std::cell::Cell`] for that reason: the
//! area is only known at draw time, so that is the only place the viewport can
//! be reconciled with the cursor.

use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::Style,
    widgets::{StatefulWidget, Widget},
};
use unicode_width::UnicodeWidthChar;

use super::{TextArea, Viewport};

/// One drawable cell: the symbol, its display width and its style.
struct DisplayCell {
    symbol: String,
    width: usize,
    style: Style,
}

impl TextArea {
    /// Display column of a character position, with tabs expanded.
    pub fn display_col(&self, row: usize, col: usize) -> usize {
        let Some(line) = self.lines.get(row) else {
            return 0;
        };
        let mut width = 0;
        for c in line.chars().take(col) {
            width += self.char_width(c, width);
        }
        width
    }

    /// Display width of one character starting at display column `at`.
    fn char_width(&self, c: char, at: usize) -> usize {
        if c == '\t' {
            self.tab_len - (at % self.tab_len)
        } else {
            UnicodeWidthChar::width(c).unwrap_or(0)
        }
    }

    /// Scroll position that keeps the cursor on screen inside `area`, starting
    /// from wherever the previous frame left it.
    fn viewport_for(&self, area: Rect) -> Viewport {
        let prev = self.viewport.get();
        let (cursor_row, cursor_col) = self.cursor;

        let height = area.height as usize;
        let mut row = prev.row.min(self.lines.len().saturating_sub(1));
        if cursor_row < row {
            row = cursor_row;
        } else if height > 0 && cursor_row >= row + height {
            row = cursor_row + 1 - height;
        }

        let width = area.width as usize;
        let cursor_x = self.display_col(cursor_row, cursor_col);
        let mut col = prev.col;
        if cursor_x < col {
            col = cursor_x;
        } else if width > 0 && cursor_x >= col + width {
            col = cursor_x + 1 - width;
        }

        Viewport {
            row,
            col,
            height: area.height,
            width: area.width,
        }
    }

    /// Style for the character at `(row, col)`, layering selection and cursor
    /// highlights over the base style.
    fn style_at(&self, row: usize, col: usize) -> Style {
        let mut style = self.style;
        if let Some((start, end)) = self.selection() {
            if (row, col) >= start && (row, col) < end {
                style = style.patch(self.selection_style);
            }
        }
        if self.cursor_visible && self.cursor == (row, col) {
            style = style.patch(self.cursor_style);
        }
        style
    }

    /// Expand one line into drawable cells, including the cursor cell when it
    /// sits past the end of the line.
    fn cells_for_line(&self, row: usize) -> Vec<DisplayCell> {
        let line = &self.lines[row];
        let mut cells = Vec::with_capacity(line.len() + 1);
        let mut width = 0;

        for (col, c) in line.chars().enumerate() {
            let style = self.style_at(row, col);
            if c == '\t' {
                let spaces = self.tab_len - (width % self.tab_len);
                for _ in 0..spaces {
                    cells.push(DisplayCell {
                        symbol: " ".to_string(),
                        width: 1,
                        style,
                    });
                }
                width += spaces;
                continue;
            }
            let char_width = UnicodeWidthChar::width(c).unwrap_or(0);
            if char_width == 0 {
                continue;
            }
            cells.push(DisplayCell {
                symbol: c.to_string(),
                width: char_width,
                style,
            });
            width += char_width;
        }

        let line_len = line.chars().count();
        if self.cursor == (row, line_len) {
            cells.push(DisplayCell {
                symbol: " ".to_string(),
                width: 1,
                style: self.style_at(row, line_len),
            });
        }
        cells
    }

    /// Draw one line of cells at `y`, skipping the first `scroll` display
    /// columns.
    fn render_line(cells: &[DisplayCell], area: Rect, y: u16, scroll: usize, buf: &mut Buffer) {
        let mut x = area.x;
        let mut column = 0;
        for cell in cells {
            let end = column + cell.width;
            if end <= scroll {
                column = end;
                continue;
            }
            if x >= area.right() {
                return;
            }
            // A wide character straddling either edge of the viewport is drawn
            // as blanks: half a glyph would corrupt the line.
            let clipped_left = column < scroll;
            let overflows_right = x as usize + cell.width > area.right() as usize;
            if clipped_left || overflows_right {
                let visible = if clipped_left {
                    end - scroll
                } else {
                    cell.width
                };
                for _ in 0..visible {
                    if x >= area.right() {
                        break;
                    }
                    buf[(x, y)].set_symbol(" ").set_style(cell.style);
                    x += 1;
                }
                column = end;
                continue;
            }
            buf[(x, y)].set_symbol(&cell.symbol).set_style(cell.style);
            for offset in 1..cell.width as u16 {
                buf[(x + offset, y)].set_symbol(" ").set_style(cell.style);
            }
            x += cell.width as u16;
            column = end;
        }
    }
}

impl Widget for &TextArea {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let viewport = self.viewport_for(area);
        self.viewport.set(viewport);

        buf.set_style(area, self.style);

        for offset in 0..area.height {
            let row = viewport.row + offset as usize;
            if row >= self.lines.len() {
                break;
            }
            let cells = self.cells_for_line(row);
            TextArea::render_line(&cells, area, area.y + offset, viewport.col, buf);
        }
    }
}

impl StatefulWidget for &TextArea {
    /// Receives the scroll position settled on by this frame, as
    /// `(first visible row, first visible column)`.
    type State = (usize, usize);

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        Widget::render(self, area, buf);
        *state = self.scroll_offsets();
    }
}

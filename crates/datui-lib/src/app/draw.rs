//! Drawing frames, with a scroll moved by the terminal.
//!
//! Ratatui's diff compares each cell with the cell at the same place last frame, so a
//! one-line scroll looks like a whole new page and is sent as one. When a band of
//! full-width lines in the new frame is last frame's band moved up or down, this asks
//! the terminal to move it (a scroll region: `CSI top;bottom r`, `CSI n S` or `T`),
//! moves its own copy of the screen the same way, and diffs against that: only the
//! lines the scroll exposed, and whatever else changed, are sent.
//!
//! [`Drawer`] keeps what the terminal shows (`shown`) in place of Ratatui's previous
//! buffer, which `Terminal` does not let out. Every frame goes through it; a frame
//! that is not such a move is the plain diff against `shown`, which is exactly what
//! `Terminal::draw` would send.

use std::hash::{Hash, Hasher};
use std::io;

use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::Backend;
use ratatui::buffer::{Buffer, Cell};

/// A band of lines `top..bottom` that moved up (`up`) or down by `by` lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Moved {
    pub(crate) top: u16,
    pub(crate) bottom: u16,
    pub(crate) by: u16,
    pub(crate) up: bool,
}

/// What the terminal shows, and the buffer the next frame is taken into.
#[derive(Default)]
pub(crate) struct Drawer {
    shown: Buffer,
    next: Buffer,
    /// One hash per line of `shown`, then of `next`.
    shown_lines: Vec<u64>,
    next_lines: Vec<u64>,
    scroll: bool,
}

impl Drawer {
    pub(crate) fn new(scroll: bool) -> Self {
        Self {
            scroll,
            ..Self::default()
        }
    }

    /// Whether a scroll is moved by the terminal (`display.scroll_region`).
    pub(crate) fn set_scroll(&mut self, scroll: bool) {
        self.scroll = scroll;
    }

    /// Clear the terminal; the next frame is drawn whole.
    pub(crate) fn clear<B: Backend<Error = io::Error>>(
        &mut self,
        terminal: &mut Terminal<B>,
    ) -> io::Result<()> {
        terminal.clear()?;
        self.shown.reset();
        self.shown_lines.clear();
        Ok(())
    }

    /// Render a frame and send what changed, as `Terminal::draw` does with no cursor.
    pub(crate) fn draw<B, F>(&mut self, terminal: &mut Terminal<B>, render: F) -> io::Result<()>
    where
        B: Backend<Error = io::Error> + io::Write,
        F: FnOnce(&mut Frame),
    {
        // A new size clears the screen and resizes Ratatui's buffers.
        terminal.autoresize()?;
        render(&mut terminal.get_frame());
        std::mem::swap(terminal.current_buffer_mut(), &mut self.next);
        let area = self.next.area;
        if self.shown.area != area {
            self.shown = Buffer::empty(area);
            self.shown_lines.clear();
        }
        line_hashes(&self.next, &mut self.next_lines);
        if self.scroll
            && self.shown_lines.len() == self.next_lines.len()
            && let Some(moved) = find_move(&self.shown_lines, &self.next_lines)
        {
            match scroll_terminal(terminal.backend_mut(), moved) {
                Ok(()) => shift(&mut self.shown, moved),
                // A Windows console without ANSI: nothing was written.
                Err(e) if e.kind() == io::ErrorKind::Unsupported => self.scroll = false,
                Err(e) => return Err(e),
            }
        }
        terminal
            .backend_mut()
            .draw(self.shown.diff_iter(&self.next))?;
        terminal.hide_cursor()?;
        Backend::flush(terminal.backend_mut())?;
        std::mem::swap(&mut self.shown, &mut self.next);
        std::mem::swap(&mut self.shown_lines, &mut self.next_lines);
        // The next frame renders into a blank buffer of the current size.
        let blank = terminal.current_buffer_mut();
        blank.resize(area);
        blank.reset();
        Ok(())
    }
}

fn scroll_terminal<B: Backend<Error = io::Error> + io::Write>(
    backend: &mut B,
    moved: Moved,
) -> io::Result<()> {
    // Lines a scroll exposes take the current background; the last frame ended with
    // its attributes reset, and this makes sure. A command, not bytes: a console
    // without ANSI would print them.
    crossterm::queue!(
        backend,
        crossterm::style::SetAttribute(crossterm::style::Attribute::Reset)
    )?;
    let region = moved.top..moved.bottom;
    if moved.up {
        backend.scroll_region_up(region, moved.by)
    } else {
        backend.scroll_region_down(region, moved.by)
    }
}

fn line_hashes(buf: &Buffer, out: &mut Vec<u64>) {
    out.clear();
    let width = usize::from(buf.area.width).max(1);
    out.extend(buf.content.chunks(width).map(|line| {
        line.iter().fold(0u64, |h, cell| {
            let mut packed = Packed(0);
            for &b in cell.symbol().as_bytes() {
                packed.write_u8(b);
            }
            cell.fg.hash(&mut packed);
            cell.bg.hash(&mut packed);
            packed.write_u16(cell.modifier.bits());
            (h.rotate_left(5) ^ packed.0).wrapping_mul(0x517c_c1b7_2722_0a95)
        })
    }));
}

/// What a cell shows, folded into a word by shifts, then mixed into its line's
/// hash with one multiply (the Fx hash): every cell is hashed each frame. A
/// collision, or a field left out, costs only bytes: the diff after a move sends
/// whatever differs.
struct Packed(u64);

impl Hasher for Packed {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_u8(b);
        }
    }

    fn write_u8(&mut self, n: u8) {
        self.0 = self.0.rotate_left(8) ^ u64::from(n);
    }

    fn write_u16(&mut self, n: u16) {
        self.0 = self.0.rotate_left(16) ^ u64::from(n);
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = self.0.rotate_left(8) ^ n;
    }

    fn write_usize(&mut self, n: usize) {
        self.write_u64(n as u64);
    }
}

/// The band whose move saves the most lines: lines in it that changed in place,
/// less the `by` lines the move exposes, which are drawn new. None when no move
/// saves at least two.
pub(crate) fn find_move(shown: &[u64], next: &[u64]) -> Option<Moved> {
    let height = shown.len();
    let mut best: Option<(usize, Moved)> = None;
    for by in 1..height {
        for up in [true, false] {
            // Lines `y` of the new frame equal to line `y + by` (up) or `y - by` shown.
            let same = |y: usize| {
                if up {
                    next[y] == shown[y + by]
                } else {
                    next[y + by] == shown[y]
                }
            };
            let mut y = 0;
            while y < height - by {
                if !same(y) {
                    y += 1;
                    continue;
                }
                let start = y;
                while y < height - by && same(y) {
                    y += 1;
                }
                // The region holds the band and the lines it moves into or out of.
                let (top, bottom) = (start, y + by);
                let changed = (top..bottom).filter(|&l| next[l] != shown[l]).count();
                let saved = changed.saturating_sub(by);
                if saved >= 2 && best.is_none_or(|(s, _)| saved > s) {
                    best = Some((
                        saved,
                        Moved {
                            top: top as u16,
                            bottom: bottom as u16,
                            by: by as u16,
                            up,
                        },
                    ));
                }
            }
        }
    }
    best.map(|(_, moved)| moved)
}

/// Move `buf`'s lines as the terminal moved its own, blanking the exposed ones.
pub(crate) fn shift(buf: &mut Buffer, moved: Moved) {
    let width = usize::from(buf.area.width);
    let lines = &mut buf.content[usize::from(moved.top) * width..usize::from(moved.bottom) * width];
    let exposed = usize::from(moved.by) * width;
    let len = lines.len();
    if moved.up {
        lines.rotate_left(exposed);
        lines[len - exposed..].fill(Cell::EMPTY);
    } else {
        lines.rotate_right(exposed);
        lines[..exposed].fill(Cell::EMPTY);
    }
}

/// The area a frame is drawn in; for tests that build a fixed-size terminal.
#[cfg(test)]
pub(crate) fn fixed(width: u16, height: u16) -> ratatui::TerminalOptions {
    ratatui::TerminalOptions {
        viewport: ratatui::Viewport::Fixed(ratatui::layout::Rect::new(0, 0, width, height)),
    }
}

#[cfg(test)]
mod tests;

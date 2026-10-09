//! Drawing frames, with a scroll moved by the terminal.
//!
//! Ratatui's diff compares each cell with the cell at the same place last frame, so a
//! one-line scroll looks like a whole new page and is sent as one. When a band of
//! full-width lines in the new frame is last frame's band moved up or down, this asks
//! the terminal to move it ([`MoveLines`]: delete or insert lines inside a scroll
//! region), moves its own copy of the screen the same way, and diffs against that:
//! only the lines the move exposed, and whatever else changed, are sent.
//!
//! [`Drawer`] keeps what the terminal shows (`shown`) in place of Ratatui's previous
//! buffer, which `Terminal` does not let out. Every frame goes through it; a frame
//! that is not such a move is the plain diff against `shown`, which is exactly what
//! `Terminal::draw` would send. Every frame is one synchronized update (DEC 2026),
//! which terminals without it ignore, so a move and its diff show at once.

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
    /// One hash per line of `shown`, then of `next`; empty while `scroll` is off.
    shown_lines: Vec<u64>,
    next_lines: Vec<u64>,
    /// Lines changed in place before each line, for [`find_move`].
    changed: Vec<u32>,
    scroll: bool,
    repaint: bool,
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
        self.shown_lines.clear();
    }

    /// Clear the terminal; the next frame is drawn whole.
    pub(crate) fn clear<B: Backend<Error = io::Error>>(
        &mut self,
        terminal: &mut Terminal<B>,
    ) -> io::Result<()> {
        terminal.clear()?;
        self.forget();
        Ok(())
    }

    /// Clear and draw every cell with the next frame, in its update: what the
    /// terminal shows may have drifted from `shown` (another program wrote to it, or
    /// it drew a glyph wider than measured), and a move would carry the drift along.
    pub(crate) fn repaint(&mut self) {
        self.repaint = true;
    }

    fn forget(&mut self) {
        self.shown.reset();
        self.shown_lines.clear();
    }

    /// Render a frame and send what changed, as `Terminal::draw` does with no cursor,
    /// in one synchronized update flushed once. A frame that changes nothing on screen
    /// writes nothing at all: no update brackets, colors or cursor.
    pub(crate) fn draw<B, F>(&mut self, terminal: &mut Terminal<B>, render: F) -> io::Result<()>
    where
        B: Backend<Error = io::Error> + io::Write,
        F: FnOnce(&mut Frame),
    {
        let asked = std::mem::take(&mut self.repaint);
        // A repaint is asked for on a resize, whose clear `autoresize` writes: inside
        // the update, so the cleared screen is never shown.
        if asked {
            crossterm::queue!(
                terminal.backend_mut(),
                crossterm::terminal::BeginSynchronizedUpdate
            )?;
        }
        // A new size clears the screen and resizes Ratatui's buffers.
        terminal.autoresize()?;
        render(&mut terminal.get_frame());
        std::mem::swap(terminal.current_buffer_mut(), &mut self.next);
        let area = self.next.area;
        if self.shown.area != area {
            self.shown = Buffer::empty(area);
            self.shown_lines.clear();
        }
        let mut moved = None;
        if self.scroll {
            line_hashes(&self.next, &mut self.next_lines);
            if !asked
                && self.shown_lines.len() == self.next_lines.len()
                && let Some(band) =
                    find_move(&self.shown_lines, &self.next_lines, &mut self.changed)
                && saves_bytes(&self.shown, &self.next, band)
            {
                moved = Some(band);
            }
        }
        let repaint = asked;
        if repaint {
            self.forget();
        }
        let changed =
            repaint || moved.is_some() || self.shown.diff_iter(&self.next).next().is_some();
        if changed {
            let out = terminal.backend_mut();
            if !asked {
                crossterm::queue!(out, crossterm::terminal::BeginSynchronizedUpdate)?;
            }
            if repaint {
                crossterm::queue!(
                    out,
                    crossterm::terminal::Clear(crossterm::terminal::ClearType::All)
                )?;
            }
            if let Some(band) = moved {
                match crossterm::queue!(out, MoveLines(band)) {
                    Ok(()) => shift(&mut self.shown, band),
                    // A Windows console without ANSI: nothing was written.
                    Err(e) if e.kind() == io::ErrorKind::Unsupported => {
                        self.scroll = false;
                        self.next_lines.clear();
                    }
                    Err(e) => return Err(e),
                }
            }
            out.draw(self.shown.diff_iter(&self.next))?;
            // Queued, not `Terminal::hide_cursor`, which flushes mid-frame.
            crossterm::queue!(
                out,
                crossterm::cursor::Hide,
                crossterm::terminal::EndSynchronizedUpdate
            )?;
            Backend::flush(out)?;
        }
        std::mem::swap(&mut self.shown, &mut self.next);
        std::mem::swap(&mut self.shown_lines, &mut self.next_lines);
        // The next frame renders into a blank buffer of the current size.
        let blank = terminal.current_buffer_mut();
        blank.resize(area);
        blank.reset();
        Ok(())
    }
}

/// Move a band of lines with the terminal's own line editing: set the scroll region
/// to the band, delete (up) or insert (down) `by` lines at its top, and reset the
/// region. Delete and insert line work wherever scroll regions do (the Linux console
/// and Emacs `term` have no `CSI S` / `CSI T`), and the lines they open take the
/// current background, which the reset attributes make the default. Origin mode is
/// turned off first, so the cursor position is the screen's, not the region's.
struct MoveLines(Moved);

impl crossterm::Command for MoveLines {
    fn write_ansi(&self, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        let Moved {
            top,
            bottom,
            by,
            up,
        } = self.0;
        write!(
            f,
            "\x1b[0m\x1b[?6l\x1b[{};{bottom}r\x1b[{};1H\x1b[{by}{}\x1b[r",
            top + 1,
            top + 1,
            if up { 'M' } else { 'L' }
        )
    }

    #[cfg(windows)]
    fn execute_winapi(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "moving lines needs ANSI",
        ))
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
/// saves at least two. `changed` is scratch, kept to save allocating it each frame.
///
/// A move goes at most half the screen, since a farther one leaves fewer lines to
/// keep than it exposes, and at most [`MAX_MOVE`] lines, so the search is linear in
/// the height: with the lines changed in place counted ahead, a 300-line screen is
/// about 40,000 comparisons.
pub(crate) fn find_move(shown: &[u64], next: &[u64], changed: &mut Vec<u32>) -> Option<Moved> {
    let height = shown.len();
    if shown == next {
        return None;
    }
    changed.clear();
    changed.push(0);
    let mut count = 0;
    for (s, n) in shown.iter().zip(next) {
        count += u32::from(s != n);
        changed.push(count);
    }
    let mut best: Option<(u32, Moved)> = None;
    for by in 1..=(height / 2).min(MAX_MOVE) {
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
                let saved = (changed[bottom] - changed[top]).saturating_sub(by as u32);
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

/// The farthest move looked for: a half page on a 128-line screen.
const MAX_MOVE: usize = 64;

/// About the bytes a move's escapes take.
const MOVE_COST: usize = 30;

/// About the bytes the diff spends to start a run of changed cells: a cursor move
/// and, usually, colors.
const RUN_COST: usize = 12;

/// About the bytes the diff sends to turn `from` into `to`: a byte a cell, and
/// [`RUN_COST`] for each run of changed cells.
fn diff_cost<'a>(from: impl Iterator<Item = &'a Cell>, to: &[Cell]) -> usize {
    let (mut cost, mut in_run) = (0, false);
    for (a, b) in from.zip(to) {
        if a == b {
            in_run = false;
        } else {
            cost += if in_run { 1 } else { 1 + RUN_COST };
            in_run = true;
        }
    }
    cost
}

/// Whether `moved` and the diff after it cost fewer bytes than the plain diff, by
/// the estimate of [`diff_cost`]: lines can be equal but for a few cells (a row
/// number), which the plain diff sends cheaper than an exposed line.
pub(crate) fn saves_bytes<'a>(shown: &'a Buffer, next: &'a Buffer, moved: Moved) -> bool {
    let width = usize::from(shown.area.width);
    let line = |buf: &'a Buffer, y: usize| &buf.content[y * width..(y + 1) * width];
    let (top, bottom, by) = (
        usize::from(moved.top),
        usize::from(moved.bottom),
        usize::from(moved.by),
    );
    let plain: usize = (top..bottom)
        .map(|y| diff_cost(line(shown, y).iter(), line(next, y)))
        .sum();
    let after: usize = (top..bottom)
        .map(|y| {
            let from = if moved.up { y + by } else { y.wrapping_sub(by) };
            if (top..bottom).contains(&from) {
                diff_cost(line(shown, from).iter(), line(next, y))
            } else {
                diff_cost(std::iter::repeat_n(&Cell::EMPTY, width), line(next, y))
            }
        })
        .sum();
    after + MOVE_COST < plain
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

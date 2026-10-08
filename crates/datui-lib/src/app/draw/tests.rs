use super::*;
use crate::*;
use polars::prelude::{IntoLazy, df};
use ratatui::backend::CrosstermBackend;

const COLS: u16 = 80;
const LINES: u16 = 24;

/// What was written to the terminal, kept for the test to read.
#[derive(Clone, Default)]
struct Tape(std::rc::Rc<std::cell::RefCell<Vec<u8>>>);

impl std::io::Write for Tape {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Tape {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}

type Out = Terminal<CrosstermBackend<Tape>>;

fn in_memory(width: u16, height: u16) -> (Out, Tape) {
    let tape = Tape::default();
    let backend = CrosstermBackend::new(tape.clone());
    (
        Terminal::with_options(backend, fixed(width, height)).unwrap(),
        tape,
    )
}

/// A terminal that writes into memory, and the screen those bytes make.
struct Emulated {
    terminal: Out,
    tape: Tape,
    drawer: Drawer,
    screen: vt100::Parser,
}

impl Emulated {
    fn new(scroll: bool) -> Self {
        let (terminal, tape) = in_memory(COLS, LINES);
        Self {
            terminal,
            tape,
            drawer: Drawer::new(scroll),
            screen: vt100::Parser::new(LINES, COLS, 0),
        }
    }

    /// Draw `frame`; the bytes it took.
    fn draw(&mut self, frame: &Buffer) -> Vec<u8> {
        self.drawer
            .draw(&mut self.terminal, |f| {
                f.buffer_mut().content.clone_from_slice(&frame.content)
            })
            .unwrap();
        let out = self.tape.take();
        self.screen.process(&out);
        out
    }

    /// Each cell's text and look; a cell never written reads as a space.
    fn lines(&self) -> Vec<String> {
        let screen = self.screen.screen();
        (0..LINES)
            .map(|y| {
                (0..COLS)
                    .map(|x| {
                        let c = screen.cell(y, x).unwrap();
                        let text = if c.has_contents() { c.contents() } else { " " };
                        format!(
                            "{text}{:?}{:?}{}{}{}{}{}|",
                            c.fgcolor(),
                            c.bgcolor(),
                            c.bold() as u8,
                            c.dim() as u8,
                            c.italic() as u8,
                            c.underline() as u8,
                            c.inverse() as u8
                        )
                    })
                    .collect()
            })
            .collect()
    }
}

/// What a fresh terminal shows after drawing `frame` whole.
fn redrawn(frame: &Buffer) -> Emulated {
    let mut fresh = Emulated::new(false);
    fresh.draw(frame);
    fresh
}

/// A scroll region is set, scrolled and reset; only the reset is `CSI r` bare.
fn moved_by_terminal(out: &[u8]) -> bool {
    find(out, b"\x1b[r").is_some()
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

struct Driven {
    app: App,
    rx: std::sync::mpsc::Receiver<AppEvent>,
    tx: std::sync::mpsc::Sender<AppEvent>,
}

fn app(rows: i64) -> Driven {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    let ids: Vec<i64> = (0..rows).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("name-{i}")).collect();
    let amounts: Vec<f64> = ids.iter().map(|i| *i as f64 * 1.5).collect();
    let df = df!("id" => ids, "name" => names, "amount" => amounts).unwrap();
    app.data_table_state =
        Some(DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap());
    let mut driven = Driven { app, rx, tx };
    let first = render(&mut driven);
    assert!(
        crate::tests::buffer_text(&first).contains("name-1"),
        "{}",
        crate::tests::buffer_text(&first)
    );
    driven
}

impl Driven {
    /// Handle what the app sent itself until it waits on nothing.
    fn settle(&mut self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            while let Ok(event) = self.rx.try_recv() {
                self.handle(event);
            }
            self.app.frame_painted();
            if !self.app.is_busy() {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "the app stayed busy");
            if let Ok(event) = self.rx.recv_timeout(std::time::Duration::from_millis(50)) {
                self.handle(event);
            }
        }
    }

    /// As the event pump does: what the app returns comes back through the channel.
    fn handle(&mut self, event: AppEvent) {
        if let Some(next) = self.app.event(event) {
            let _ = self.tx.send(next);
        }
    }
}

/// The frame for the app's state once its rows are in.
fn render(driven: &mut Driven) -> Buffer {
    let area = Rect::new(0, 0, COLS, LINES);
    let mut buf = Buffer::empty(area);
    for _ in 0..3 {
        driven.settle();
        buf = Buffer::empty(area);
        driven.app.render(area, &mut buf);
    }
    buf
}

fn press(driven: &mut Driven, code: KeyCode) {
    driven.handle(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

/// Fast path on and off, driven through the same frames.
struct Pair {
    fast: Emulated,
    plain: Emulated,
    moves: usize,
}

impl Pair {
    fn new() -> Self {
        Self {
            fast: Emulated::new(true),
            plain: Emulated::new(false),
            moves: 0,
        }
    }

    /// Draw `frame` on both; each screen is what a full redraw shows, and a frame the
    /// terminal did not move is sent byte for byte as the plain diff.
    fn draw(&mut self, frame: &Buffer, what: &str) {
        let fast = self.fast.draw(frame);
        let plain = self.plain.draw(frame);
        let whole = redrawn(frame);
        assert_eq!(
            self.fast.lines(),
            whole.lines(),
            "{what}: the screen after the move differs from a redraw:\n{}\n---\n{}",
            self.fast.screen.screen().contents(),
            whole.screen.screen().contents()
        );
        assert_eq!(self.plain.lines(), whole.lines(), "{what}: plain diff");
        if moved_by_terminal(&fast) {
            self.moves += 1;
            assert!(
                fast.len() * 3 < plain.len(),
                "{what}: {} bytes moved, {} plain",
                fast.len(),
                plain.len()
            );
        } else {
            assert_eq!(fast, plain, "{what}: a frame not moved is the plain diff");
        }
    }
}

/// Down past the page's last line and back up past its first: each scroll is moved
/// by the terminal, and the screen, footer and rail included, is the redraw's.
#[test]
fn a_scroll_moved_by_the_terminal_shows_what_a_redraw_shows() {
    let mut app = app(400);
    let mut pair = Pair::new();
    pair.draw(&render(&mut app), "first frame");
    for i in 0..40 {
        press(&mut app, KeyCode::Down);
        pair.draw(&render(&mut app), &format!("down {i}"));
    }
    let after_down = pair.moves;
    assert!(after_down >= 15, "{after_down} of 40 downs moved");
    for i in 0..60 {
        press(&mut app, KeyCode::Up);
        pair.draw(&render(&mut app), &format!("up {i}"));
    }
    assert!(
        pair.moves - after_down >= 15,
        "ups moved {}",
        pair.moves - after_down
    );
    for (i, code) in [KeyCode::PageDown, KeyCode::PageDown, KeyCode::PageUp]
        .into_iter()
        .enumerate()
    {
        press(&mut app, code);
        pair.draw(&render(&mut app), &format!("page {i}"));
    }
}

/// A move that is not a scroll (a column, an overlay) is the plain diff.
#[test]
fn frames_that_are_not_a_scroll_are_the_plain_diff() {
    let mut app = app(400);
    let mut pair = Pair::new();
    pair.draw(&render(&mut app), "first frame");
    for _ in 0..30 {
        press(&mut app, KeyCode::Down);
        pair.draw(&render(&mut app), "down");
    }
    let moves = pair.moves;
    for (what, code) in [
        ("column right", KeyCode::Right),
        ("column left", KeyCode::Left),
        ("help", KeyCode::Char('?')),
        ("help closed", KeyCode::Esc),
    ] {
        press(&mut app, code);
        pair.draw(&render(&mut app), what);
    }
    assert_eq!(pair.moves, moves, "none of those was moved by the terminal");
}

/// Off, nothing is moved by the terminal.
#[test]
fn off_never_moves() {
    let mut app = app(400);
    let mut plain = Emulated::new(false);
    plain.draw(&render(&mut app));
    for _ in 0..40 {
        press(&mut app, KeyCode::Down);
        assert!(!moved_by_terminal(&plain.draw(&render(&mut app))));
    }
}

/// A new size draws the frame whole on the cleared screen.
#[test]
fn a_new_size_is_drawn_whole() {
    let mut drawer = Drawer::new(true);
    let (mut terminal, _) = in_memory(10, 4);
    drawer
        .draw(&mut terminal, |f| {
            f.buffer_mut()
                .set_string(0, 0, "abc", ratatui::style::Style::default())
        })
        .unwrap();
    // As Ratatui's resize does: a blank screen and new buffers.
    let (mut terminal, tape) = in_memory(12, 5);
    drawer
        .draw(&mut terminal, |f| {
            f.buffer_mut()
                .set_string(0, 0, "abc", ratatui::style::Style::default())
        })
        .unwrap();
    let out = tape.take();
    assert!(
        find(&out, b"abc").is_some(),
        "{:?}",
        String::from_utf8_lossy(&out)
    );
}

fn lines_of(texts: &[&str]) -> Buffer {
    let width = 4;
    let mut buf = Buffer::empty(Rect::new(0, 0, width, texts.len() as u16));
    for (y, t) in texts.iter().enumerate() {
        buf.set_string(0, y as u16, t, ratatui::style::Style::default());
    }
    buf
}

fn hashes(buf: &Buffer) -> Vec<u64> {
    let mut out = Vec::new();
    line_hashes(buf, &mut out);
    out
}

/// The band between a fixed header and footer moved up one line.
#[test]
fn a_band_between_header_and_footer_moves() {
    let before = lines_of(&["head", "r1", "r2", "r3", "r4", "r5", "foot"]);
    let after = lines_of(&["head", "r2", "r3", "r4", "r5", "r6", "foot"]);
    let moved = find_move(&hashes(&before), &hashes(&after)).unwrap();
    assert_eq!(
        moved,
        Moved {
            top: 1,
            bottom: 6,
            by: 1,
            up: true
        }
    );
    let mut shifted = before.clone();
    shift(&mut shifted, moved);
    assert_eq!(hashes(&shifted)[1..5], hashes(&after)[1..5]);
    assert_eq!(shifted[(0, 5)], Cell::EMPTY);

    let moved = find_move(&hashes(&after), &hashes(&before)).unwrap();
    assert_eq!(
        (moved.top, moved.bottom, moved.by, moved.up),
        (1, 6, 1, false)
    );
}

/// Nothing moved, or too little to save a line: no move.
#[test]
fn a_frame_that_did_not_move_has_no_move() {
    let a = lines_of(&["a", "b", "c", "d"]);
    assert_eq!(find_move(&hashes(&a), &hashes(&a)), None);
    let blank = lines_of(&["", "", "", ""]);
    assert_eq!(find_move(&hashes(&blank), &hashes(&blank)), None);
    let b = lines_of(&["a", "x", "c", "d"]);
    assert_eq!(find_move(&hashes(&a), &hashes(&b)), None);
}

/// A frame takes no new buffers: the screen, the frame Ratatui renders into, the
/// one taken from it and the line hashes are the same allocations frame after
/// frame, trading places.
#[test]
fn frames_reuse_their_buffers() {
    let mut driven = app(400);
    let mut fast = Emulated::new(true);
    fast.draw(&render(&mut driven));
    for _ in 0..30 {
        press(&mut driven, KeyCode::Down);
        fast.draw(&render(&mut driven));
    }
    let held = |e: &mut Emulated| {
        let d = &e.drawer;
        let mut buffers = [
            d.shown.content.as_ptr(),
            d.next.content.as_ptr(),
            e.terminal.current_buffer_mut().content.as_ptr(),
        ];
        buffers.sort();
        let mut lines = [d.shown_lines.as_ptr(), d.next_lines.as_ptr()];
        lines.sort();
        (buffers, lines)
    };
    let before = held(&mut fast);
    let mut moved = 0;
    for _ in 0..20 {
        press(&mut driven, KeyCode::Down);
        moved += usize::from(moved_by_terminal(&fast.draw(&render(&mut driven))));
        assert_eq!(held(&mut fast), before);
    }
    assert_eq!(moved, 20);
}

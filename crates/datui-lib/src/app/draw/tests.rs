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

/// A backend whose reported size the test sets, so `autoresize` sees a resize.
struct Resizable {
    inner: CrosstermBackend<Tape>,
    size: std::rc::Rc<std::cell::Cell<ratatui::layout::Size>>,
}

impl std::io::Write for Resizable {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.inner)
    }
}

impl Backend for Resizable {
    type Error = std::io::Error;

    fn draw<'a, I>(&mut self, content: I) -> std::io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.inner.draw(content)
    }

    fn hide_cursor(&mut self) -> std::io::Result<()> {
        self.inner.hide_cursor()
    }

    fn show_cursor(&mut self) -> std::io::Result<()> {
        self.inner.show_cursor()
    }

    fn get_cursor_position(&mut self) -> std::io::Result<ratatui::layout::Position> {
        Ok(ratatui::layout::Position::ORIGIN)
    }

    fn set_cursor_position<P: Into<ratatui::layout::Position>>(
        &mut self,
        position: P,
    ) -> std::io::Result<()> {
        self.inner.set_cursor_position(position)
    }

    fn clear(&mut self) -> std::io::Result<()> {
        self.inner.clear()
    }

    fn clear_region(&mut self, clear_type: ClearType) -> std::io::Result<()> {
        self.inner.clear_region(clear_type)
    }

    fn size(&self) -> std::io::Result<ratatui::layout::Size> {
        Ok(self.size.get())
    }

    fn window_size(&mut self) -> std::io::Result<ratatui::backend::WindowSize> {
        Ok(ratatui::backend::WindowSize {
            columns_rows: self.size.get(),
            pixels: ratatui::layout::Size::default(),
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Backend::flush(&mut self.inner)
    }
}

/// A frame of numbered lines, one per screen line, starting at `from`.
fn numbered(width: u16, height: u16, from: usize) -> Buffer {
    let mut buf = Buffer::empty(Rect::new(0, 0, width, height));
    for y in 0..height {
        let text = format!(
            "line {} {}",
            from + usize::from(y),
            "x".repeat(usize::from(y))
        );
        buf.set_string(0, y, text, ratatui::style::Style::default());
    }
    buf
}

/// The screen a fresh terminal shows after drawing `frame` whole.
fn redraw_of(frame: &Buffer) -> Vec<String> {
    let (width, height) = (frame.area.width, frame.area.height);
    let (mut terminal, tape) = in_memory(width, height);
    Drawer::new(false)
        .draw(&mut terminal, |f| {
            f.buffer_mut().content.clone_from_slice(&frame.content)
        })
        .unwrap();
    let mut screen = vt100::Parser::new(height, width, 0);
    screen.process(&tape.take());
    cells_of(&screen)
}

/// Each cell's text and background; a cell never written reads as a space.
fn cells_of(parser: &vt100::Parser) -> Vec<String> {
    let screen = parser.screen();
    let (height, width) = screen.size();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| {
                    let c = screen.cell(y, x).unwrap();
                    let text = if c.has_contents() { c.contents() } else { " " };
                    format!("{text}{:?}{:?}|", c.fgcolor(), c.bgcolor())
                })
                .collect()
        })
        .collect()
}

/// A new size, on the same terminal, draws the frame whole on the cleared screen.
#[test]
fn a_new_size_is_drawn_whole() {
    let tape = Tape::default();
    let size = std::rc::Rc::new(std::cell::Cell::new(ratatui::layout::Size::new(20, 6)));
    let backend = Resizable {
        inner: CrosstermBackend::new(tape.clone()),
        size: size.clone(),
    };
    let mut terminal = Terminal::new(backend).unwrap();
    let mut drawer = Drawer::new(true);
    let mut screen = vt100::Parser::new(6, 20, 0);
    let mut draw =
        |terminal: &mut Terminal<Resizable>, screen: &mut vt100::Parser, frame: &Buffer| {
            drawer
                .draw(terminal, |f| {
                    f.buffer_mut().content.clone_from_slice(&frame.content)
                })
                .unwrap();
            screen.process(&tape.take());
        };
    draw(&mut terminal, &mut screen, &numbered(20, 6, 0));
    draw(&mut terminal, &mut screen, &numbered(20, 6, 1));
    // The window grows a line and narrows; the terminal keeps what it showed.
    size.set(ratatui::layout::Size::new(16, 7));
    screen.screen_mut().set_size(7, 16);
    let frame = numbered(16, 7, 2);
    draw(&mut terminal, &mut screen, &frame);
    assert_eq!(cells_of(&screen), redraw_of(&frame));
}

/// Text the terminal shows that was never drawn (another program wrote it) goes
/// with a repaint, though a move would have carried it along.
#[test]
fn a_repaint_draws_over_what_drifted() {
    let (mut terminal, tape) = in_memory(20, 6);
    let mut drawer = Drawer::new(true);
    let mut screen = vt100::Parser::new(6, 20, 0);
    let mut draw = |drawer: &mut Drawer, screen: &mut vt100::Parser, frame: &Buffer| {
        drawer
            .draw(&mut terminal, |f| {
                f.buffer_mut().content.clone_from_slice(&frame.content)
            })
            .unwrap();
        screen.process(&tape.take());
    };
    draw(&mut drawer, &mut screen, &numbered(20, 6, 0));
    screen.process(b"\x1b[3;15Hstray");
    drawer.repaint();
    let frame = numbered(20, 6, 1);
    draw(&mut drawer, &mut screen, &frame);
    assert_eq!(cells_of(&screen), redraw_of(&frame));
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// What a terminal does with the bytes, for [`random_frames`].
#[derive(Clone, Copy, PartialEq)]
enum Honors {
    /// Every sequence, as vt100 models it.
    Everything,
    /// No `CSI n S` / `CSI n T`, as the Linux console and Emacs `term`.
    NoScrollUpDown,
    /// No insert or delete line: a terminal the move would leave behind.
    NoInsertDelete,
}

/// `out` without the CSI sequences ending in one of `finals`.
fn without(out: &[u8], finals: &[u8]) -> Vec<u8> {
    let mut kept = Vec::with_capacity(out.len());
    let mut i = 0;
    while i < out.len() {
        if out[i..].starts_with(b"\x1b[") {
            let end = i
                + 2
                + out[i + 2..]
                    .iter()
                    .position(|b| b.is_ascii_alphabetic())
                    .unwrap();
            if !(finals.contains(&out[end]) && !out[i + 2..end].contains(&b'?')) {
                kept.extend_from_slice(&out[i..=end]);
            }
            i = end + 1;
        } else {
            kept.push(out[i]);
            i += 1;
        }
    }
    kept
}

/// Frames from a few kinds of line, so repeats, blanks, wide characters and equal
/// lines are common, shifted and edited at random; after every frame the screen is
/// a fresh redraw's. Returns (frames, moves, wrong screens).
fn random_frames(terminal: Honors, seeds: u64) -> (usize, usize, usize) {
    const W: u16 = 24;
    const H: u16 = 10;
    let kinds = [
        "",
        "aaaa",
        "aaaa",
        "bb",
        "日本語x",
        "zzzzzzzzzzzzzzzzzzzzzzzz",
        "a",
        "      x",
    ];
    let backgrounds = [
        ratatui::style::Color::Reset,
        ratatui::style::Color::Blue,
        ratatui::style::Color::Reset,
    ];
    let (mut frames, mut moves, mut wrong) = (0, 0, 0);
    for seed in 1..=seeds {
        let (mut term, tape) = in_memory(W, H);
        let mut drawer = Drawer::new(true);
        let mut screen = vt100::Parser::new(H, W, 0);
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut lines: Vec<u64> = (0..H).map(|_| rng.below(24)).collect();
        for _ in 0..60 {
            let top = rng.below(3) as usize;
            let bottom = usize::from(H) - rng.below(3) as usize;
            let by = 1 + rng.below(3) as usize;
            if bottom > top + by {
                if rng.below(2) == 0 {
                    lines[top..bottom].rotate_left(by);
                } else {
                    lines[top..bottom].rotate_right(by);
                }
            }
            for _ in 0..rng.below(4) {
                lines[rng.below(u64::from(H)) as usize] = rng.below(24);
            }
            let mut frame = Buffer::empty(Rect::new(0, 0, W, H));
            for (y, &k) in lines.iter().enumerate() {
                let style = ratatui::style::Style::default().bg(backgrounds[(k / 8 % 3) as usize]);
                frame.set_style(Rect::new(0, y as u16, W, 1), style);
                frame.set_string(0, y as u16, kinds[(k % 8) as usize], style);
            }
            drawer
                .draw(&mut term, |f| {
                    f.buffer_mut().content.clone_from_slice(&frame.content)
                })
                .unwrap();
            let out = tape.take();
            moves += usize::from(moved_by_terminal(&out));
            screen.process(&match terminal {
                Honors::Everything => out,
                Honors::NoScrollUpDown => without(&out, b"ST"),
                Honors::NoInsertDelete => without(&out, b"LM"),
            });
            frames += 1;
            wrong += usize::from(cells_of(&screen) != redraw_of(&frame));
        }
    }
    (frames, moves, wrong)
}

/// Whatever moves the drawer picks, among duplicate, blank and wide lines, the
/// screen is a redraw's: a move only chooses how the terminal's lines are reused.
#[test]
fn random_frames_show_what_a_redraw_shows() {
    let (frames, moves, wrong) = random_frames(Honors::Everything, 60);
    assert_eq!(wrong, 0, "{wrong} of {frames} screens differ");
    assert!(moves * 4 > frames, "{moves} moves in {frames} frames");
}

/// Moves use only insert and delete line, so a terminal without `CSI S` / `CSI T`
/// (the Linux console) shows the same; one without insert and delete line would
/// not, which shows the test can tell.
#[test]
fn moves_need_only_insert_and_delete_line() {
    let (frames, _, wrong) = random_frames(Honors::NoScrollUpDown, 20);
    assert_eq!(wrong, 0, "{wrong} of {frames} screens differ");
    let (frames, _, wrong) = random_frames(Honors::NoInsertDelete, 20);
    assert!(wrong * 2 > frames, "only {wrong} of {frames} differ");
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
    let moved = find_move(&hashes(&before), &hashes(&after), &mut Vec::new()).unwrap();
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

    let moved = find_move(&hashes(&after), &hashes(&before), &mut Vec::new()).unwrap();
    assert_eq!(
        (moved.top, moved.bottom, moved.by, moved.up),
        (1, 6, 1, false)
    );
}

/// Nothing moved, or too little to save a line: no move.
#[test]
fn a_frame_that_did_not_move_has_no_move() {
    let a = lines_of(&["a", "b", "c", "d"]);
    assert_eq!(find_move(&hashes(&a), &hashes(&a), &mut Vec::new()), None);
    let blank = lines_of(&["", "", "", ""]);
    assert_eq!(
        find_move(&hashes(&blank), &hashes(&blank), &mut Vec::new()),
        None
    );
    let b = lines_of(&["a", "x", "c", "d"]);
    assert_eq!(find_move(&hashes(&a), &hashes(&b), &mut Vec::new()), None);
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

/// A resize, or the terminal back in focus, asks the run loop for a repaint.
#[test]
fn resize_and_focus_ask_for_a_repaint() {
    let mut driven = app(10);
    assert!(!driven.app.take_repaint());
    driven.handle(AppEvent::TerminalFocused);
    assert!(driven.app.take_repaint());
    assert!(!driven.app.take_repaint());
    driven.handle(AppEvent::Resize(COLS, LINES));
    assert!(driven.app.take_repaint());
}

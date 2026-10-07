//! The one reader of the terminal.
//!
//! The run loop waits on a single channel: worker results, continuations and keys all
//! wake it, so nothing waits out a poll interval for news that has already arrived.
//! Terminal events get onto that channel through this thread, which is the only thing
//! that reads them — through Crossterm's own `EventStream`, so Crossterm's parser and
//! its reader lock are never raced by a second reader.
//!
//! The stream is polled by hand on a dedicated thread rather than on the shared Tokio
//! runtime: a runtime busy with blocking work must not delay a key. A parked thread
//! costs nothing while the terminal is quiet, and stopping it is deterministic, which
//! matters to the Python binding: a reader left behind would read the REPL's input
//! after the TUI is gone.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{JoinHandle, Thread};
use std::time::Instant;

use crossterm::event::{Event, EventStream};
use futures_core::Stream;

use crate::AppEvent;
use crate::app::terminal_color::{self, ReplyScanner, Scanned};

/// Wakes the reader thread when Crossterm has an event, or when it is told to stop.
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// The running reader. Stopped (and joined) on drop.
pub struct TerminalInput {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl TerminalInput {
    /// Start forwarding every terminal event to `tx` as [`AppEvent::Terminal`]. A read
    /// error ends the session as [`AppEvent::Crash`], as it did when the loop read the
    /// terminal itself.
    pub fn start(tx: Sender<AppEvent>) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("datui-input".into())
            .spawn(move || read(tx, &stopping))?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }

    /// Stop reading. Returns once the reader has let go of the terminal, so whatever is
    /// typed next goes to the shell (or the Python REPL), not to this session.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

impl Drop for TerminalInput {
    fn drop(&mut self) {
        self.stop();
    }
}

fn read(tx: Sender<AppEvent>, stop: &AtomicBool) {
    let mut stream = EventStream::new();
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    // The terminal's answer about its background arrives as keys; this takes it off.
    let mut replies = ReplyScanner::default();
    let mut scanned = Vec::new();
    let mut held_since: Option<Instant> = None;
    while !stop.load(Ordering::SeqCst) {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(event))) => {
                replies.feed(event, terminal_color::armed(), &mut scanned);
                held_since = replies
                    .holding()
                    .then(|| held_since.unwrap_or_else(Instant::now));
                if pass_on(&tx, &mut scanned).is_err() {
                    break;
                }
            }
            Poll::Ready(Some(Err(e))) => {
                let _ = tx.send(AppEvent::Crash(format!("Cannot read the terminal: {e}")));
                break;
            }
            Poll::Ready(None) => break,
            // A spurious unpark only costs one more poll.
            Poll::Pending => match held_since {
                None => std::thread::park(),
                Some(since) => {
                    let left = terminal_color::HOLD.saturating_sub(since.elapsed());
                    if left.is_zero() {
                        // Not a reply after all: the keys go on as typed.
                        replies.flush(&mut scanned);
                        held_since = None;
                        if pass_on(&tx, &mut scanned).is_err() {
                            break;
                        }
                    } else {
                        std::thread::park_timeout(left);
                    }
                }
            },
        }
    }
    // Dropping the stream wakes Crossterm's own blocked poll and lets its thread end,
    // releasing the terminal.
    drop(stream);
}

/// Hand what the scanner let through to the loop: events through [`forward`], a reply
/// as [`AppEvent::TerminalBackground`].
fn pass_on(tx: &Sender<AppEvent>, scanned: &mut Vec<Scanned>) -> Result<(), ()> {
    for item in scanned.drain(..) {
        match item {
            Scanned::Event(event) => forward(tx, event)?,
            Scanned::Background(mode) => {
                terminal_color::disarm();
                if let Some(mode) = mode {
                    tx.send(AppEvent::TerminalBackground(mode))
                        .map_err(|_| ())?;
                }
            }
        }
    }
    Ok(())
}

/// Hand one event to the loop. Only presses are keys: a terminal speaking the kitty
/// protocol may report releases and repeats too, and the app acts on presses alone.
/// Of the mouse, only what the app acts on ([`crate::app::pointer::wanted`]).
fn forward(tx: &Sender<AppEvent>, event: Event) -> Result<(), ()> {
    let event = match event {
        Event::Key(key) if !key.is_press() => return Ok(()),
        Event::Key(_) | Event::Resize(..) => event,
        Event::Mouse(mouse) if crate::app::pointer::wanted(&mouse) => event,
        // The terminal is back in front: its scheme may have changed meanwhile.
        Event::FocusGained => return tx.send(AppEvent::TerminalFocused).map_err(|_| ()),
        _ => return Ok(()),
    };
    tx.send(AppEvent::Terminal(event)).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{
        KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };
    use std::sync::mpsc;

    /// Key releases and repeats never reach the app; key presses, resizes, a left
    /// press with its drag and release, a right press and the wheel do, in order.
    /// Motion with no button down does not.
    #[test]
    fn only_presses_resizes_the_mouse_buttons_and_the_wheel_are_forwarded() {
        let (tx, rx) = mpsc::channel();
        let press = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        let mut release = press;
        release.kind = KeyEventKind::Release;
        let mut repeat = press;
        repeat.kind = KeyEventKind::Repeat;
        let mouse = |kind| MouseEvent {
            kind,
            column: 3,
            row: 4,
            modifiers: KeyModifiers::NONE,
        };
        let click = mouse(MouseEventKind::Down(MouseButton::Left));
        let wheel = mouse(MouseEventKind::ScrollDown);
        for event in [
            Event::Key(release),
            Event::Key(press),
            Event::Key(repeat),
            Event::FocusLost,
            Event::Mouse(mouse(MouseEventKind::Moved)),
            Event::Mouse(mouse(MouseEventKind::Drag(MouseButton::Left))),
            Event::Mouse(mouse(MouseEventKind::Up(MouseButton::Left))),
            Event::Mouse(mouse(MouseEventKind::Down(MouseButton::Right))),
            Event::Mouse(click),
            Event::Mouse(wheel),
            Event::Resize(80, 24),
        ] {
            forward(&tx, event).unwrap();
        }
        drop(tx);
        let got: Vec<_> = rx
            .iter()
            .map(|e| match e {
                AppEvent::Terminal(event) => event,
                _ => panic!("only terminal events are sent"),
            })
            .collect();
        assert_eq!(
            got,
            vec![
                Event::Key(press),
                Event::Mouse(mouse(MouseEventKind::Drag(MouseButton::Left))),
                Event::Mouse(mouse(MouseEventKind::Up(MouseButton::Left))),
                Event::Mouse(mouse(MouseEventKind::Down(MouseButton::Right))),
                Event::Mouse(click),
                Event::Mouse(wheel),
                Event::Resize(80, 24)
            ]
        );
    }

    /// Focus coming back is news for the palette; focus leaving is not.
    #[test]
    fn focus_gained_is_sent_as_its_own_event() {
        let (tx, rx) = mpsc::channel();
        forward(&tx, Event::FocusLost).unwrap();
        forward(&tx, Event::FocusGained).unwrap();
        drop(tx);
        let got: Vec<_> = rx.iter().collect();
        assert!(matches!(got.as_slice(), [AppEvent::TerminalFocused]));
    }

    /// A reply the scanner took off the stream reaches the loop as the mode it names,
    /// with the keys around it in order; one whose color could not be read is dropped.
    #[test]
    fn a_reply_is_sent_as_the_terminal_background() {
        let (tx, rx) = mpsc::channel();
        let key = |c| Event::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        let mut scanned = vec![
            Scanned::Event(key('j')),
            Scanned::Background(Some(crate::config::ThemeMode::Light)),
            Scanned::Background(None),
            Scanned::Event(key('k')),
        ];
        pass_on(&tx, &mut scanned).unwrap();
        drop(tx);
        let got: Vec<_> = rx.iter().collect();
        assert!(matches!(
            got.as_slice(),
            [
                AppEvent::Terminal(Event::Key(j)),
                AppEvent::TerminalBackground(crate::config::ThemeMode::Light),
                AppEvent::Terminal(Event::Key(k)),
            ] if j.code == KeyCode::Char('j') && k.code == KeyCode::Char('k')
        ));
    }
}

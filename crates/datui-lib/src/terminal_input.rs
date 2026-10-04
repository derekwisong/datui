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

use crossterm::event::{Event, EventStream};
use futures_core::Stream;

use crate::AppEvent;

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
    while !stop.load(Ordering::SeqCst) {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(event))) => {
                if forward(&tx, event).is_err() {
                    break;
                }
            }
            Poll::Ready(Some(Err(e))) => {
                let _ = tx.send(AppEvent::Crash(format!("Cannot read the terminal: {e}")));
                break;
            }
            Poll::Ready(None) => break,
            // A spurious unpark only costs one more poll.
            Poll::Pending => std::thread::park(),
        }
    }
    // Dropping the stream wakes Crossterm's own blocked poll and lets its thread end,
    // releasing the terminal.
    drop(stream);
}

/// Hand one event to the loop. Only presses are keys: a terminal speaking the kitty
/// protocol may report releases and repeats too, and the app acts on presses alone.
/// Of the mouse, only what the app acts on ([`crate::pointer::wanted`]).
fn forward(tx: &Sender<AppEvent>, event: Event) -> Result<(), ()> {
    let event = match event {
        Event::Key(key) if !key.is_press() => return Ok(()),
        Event::Key(_) | Event::Resize(..) => event,
        Event::Mouse(mouse) if crate::pointer::wanted(&mouse) => event,
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
            Event::FocusGained,
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
}

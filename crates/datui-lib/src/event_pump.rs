//! The main loop's event handling, minus the terminal.
//!
//! `run()` reads the terminal and draws frames; everything between, including what
//! happens to a key typed while the app is busy, lives here so it can be driven
//! without a terminal. Keys typed while busy are held, in order, and replayed one per
//! loop iteration once the app is idle, each through the same path a fresh key takes.
//! Any follow-up event a replayed key produces is drained before the next held key is
//! offered, so a queued Enter finishes its search before the key typed after it acts.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::time::Duration;

use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::{App, AppEvent};

/// Keys held while busy. Beyond this the newest is dropped, and the user told: the
/// oldest may be the `/` that puts the rest into the query bar, and without it the
/// tail of a typed query would replay as hotkeys.
pub const MAX_HELD_KEYS: usize = 32;

/// What a pass over the channel found.
#[derive(Debug)]
pub enum Drained {
    /// Keep going; `updated` says whether anything was handled and a frame is due.
    Continue {
        updated: bool,
    },
    Exit,
    Crash(String),
}

/// The screen the held keys were typed at. A change means they were meant for
/// something that is no longer there: the modal that ended the work and has not been
/// seen yet, or the dataset left for the home screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Screen {
    generation: u64,
    modal: bool,
}

/// Owns the app, its channel and the keys held while it was busy.
pub struct EventPump {
    pub app: App,
    tx: Sender<AppEvent>,
    rx: Receiver<AppEvent>,
    held: VecDeque<KeyEvent>,
    held_for: Screen,
}

impl EventPump {
    pub fn new(app: App, tx: Sender<AppEvent>, rx: Receiver<AppEvent>) -> Self {
        let held_for = Self::screen_of(&app);
        Self {
            app,
            tx,
            rx,
            held: VecDeque::new(),
            held_for,
        }
    }

    pub fn send(&self, event: AppEvent) -> Result<()> {
        self.tx.send(event)?;
        Ok(())
    }

    /// The keys waiting for the app to go idle, oldest first.
    pub fn held_keys(&self) -> impl Iterator<Item = &KeyEvent> {
        self.held.iter()
    }

    /// True while held keys are waiting on an idle app: the loop should not wait on
    /// the terminal, since each iteration replays one.
    pub fn replaying(&self) -> bool {
        !self.held.is_empty() && !self.app.is_busy()
    }

    /// A key read from the terminal. Handled now if nothing is ahead of it and the app
    /// is idle, or if it is one of the few that act while busy; otherwise it waits
    /// behind whatever was typed before it. Returns whether the app changed.
    pub fn terminal_key(&mut self, key: KeyEvent) -> Result<bool> {
        self.discard_stale();
        let acts_now = if self.app.is_busy() {
            self.app.key_acts_while_busy(&key)
        } else {
            self.held.is_empty()
        };
        if acts_now {
            self.dispatch(key)?;
            return Ok(true);
        }
        self.hold(key);
        Ok(false)
    }

    /// Replay the oldest held key if the app is idle. Returns whether one was replayed.
    pub fn replay_one(&mut self) -> Result<bool> {
        self.discard_stale();
        if self.app.is_busy() {
            return Ok(false);
        }
        let Some(key) = self.held.pop_front() else {
            return Ok(false);
        };
        if self.held.is_empty() {
            self.app.set_input_dropped(false);
        }
        self.dispatch(key)?;
        Ok(true)
    }

    /// Handle everything waiting on the channel.
    pub fn drain(&mut self) -> Result<Drained> {
        let first = self.rx.try_recv();
        self.drain_from(first)
    }

    /// Wait up to `timeout` for the next event, then handle it and everything behind
    /// it. For drivers without a terminal to wait on: a background result is the only
    /// thing that can end a busy state.
    pub fn wait_and_drain(&mut self, timeout: Duration) -> Result<Drained> {
        let first = self.rx.recv_timeout(timeout).map_err(|e| match e {
            RecvTimeoutError::Timeout => TryRecvError::Empty,
            RecvTimeoutError::Disconnected => TryRecvError::Disconnected,
        });
        self.drain_from(first)
    }

    fn drain_from(&mut self, mut next: Result<AppEvent, TryRecvError>) -> Result<Drained> {
        let mut updated = false;
        loop {
            match next {
                Ok(AppEvent::Exit) => return Ok(Drained::Exit),
                Ok(AppEvent::Crash(msg)) => return Ok(Drained::Crash(msg)),
                Ok(event) => {
                    updated = true;
                    let follow_up = match self.app.handle(&event) {
                        Ok(follow_up) => follow_up,
                        Err(deferred) => {
                            self.hold(deferred);
                            None
                        }
                    };
                    self.discard_stale();
                    if let Some(follow_up) = follow_up {
                        self.tx.send(follow_up)?;
                        // A handler that returns a follow-up event is deferring work so
                        // the UI can show the current phase first — the `Do*` events all
                        // rely on this. Draining the follow-up in the same pass defeats
                        // that: the phase label never renders and the throbber never
                        // moves. Break so a frame is drawn and keys are polled first.
                        // Order is unaffected; the follow-up was appended to the queue.
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(Drained::Exit),
            }
            next = self.rx.try_recv();
        }
        Ok(Drained::Continue { updated })
    }

    /// Offer one key to the app, the way the channel drain does.
    fn dispatch(&mut self, key: KeyEvent) -> Result<()> {
        match self.app.handle(&AppEvent::Key(key)) {
            Ok(Some(follow_up)) => self.tx.send(follow_up)?,
            Ok(None) => {}
            // Only reachable if the app went busy between the check and the call, which
            // nothing on this thread does; the key keeps its place either way.
            Err(deferred) => self.held.push_front(deferred),
        }
        self.discard_stale();
        Ok(())
    }

    /// Hold a key for later. A held navigation key repeats fast and would replay as a
    /// burst, each step chaining another collect, so consecutive repeats become one
    /// press. At the cap the newest key is dropped and the user told; never the oldest,
    /// which may be the `/` the rest were typed into.
    fn hold(&mut self, key: KeyEvent) {
        if self.held.is_empty() {
            self.held_for = Self::screen_of(&self.app);
        }
        if is_navigation(&key) && self.held.back() == Some(&key) {
            return;
        }
        if self.held.len() >= MAX_HELD_KEYS {
            self.app.set_input_dropped(true);
            return;
        }
        self.held.push_back(key);
    }

    /// Drop the held keys if the screen they were typed at has gone: a modal has
    /// appeared that they were not answers to, or the view they were meant for was
    /// abandoned.
    fn discard_stale(&mut self) {
        if !self.held.is_empty() && self.held_for != Self::screen_of(&self.app) {
            self.held.clear();
            self.app.set_input_dropped(false);
        }
    }

    fn screen_of(app: &App) -> Screen {
        Screen {
            generation: app.screen_generation(),
            modal: app.modal_showing(),
        }
    }
}

/// Keys that move the view and are commonly held down.
fn is_navigation(key: &KeyEvent) -> bool {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Up
        | KeyCode::Down
        | KeyCode::PageUp
        | KeyCode::PageDown
        | KeyCode::Home
        | KeyCode::End
        | KeyCode::Char('j')
        | KeyCode::Char('k')
        | KeyCode::Char('G') => true,
        KeyCode::Char('f') | KeyCode::Char('b') | KeyCode::Char('d') | KeyCode::Char('u') => ctrl,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InputMode, LoadingState, OpenOptions};
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    use std::io::Write;
    use std::sync::mpsc;

    fn plain(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn held(pump: &EventPump) -> Vec<KeyCode> {
        pump.held_keys().map(|k| k.code).collect()
    }

    fn pump() -> EventPump {
        let (tx, rx) = mpsc::channel();
        let app = App::new(tx.clone(), crate::tests::test_runtime());
        EventPump::new(app, tx, rx)
    }

    /// Type each character as the terminal would deliver it.
    fn type_keys(pump: &mut EventPump, text: &str) {
        for c in text.chars() {
            pump.terminal_key(plain(KeyCode::Char(c))).unwrap();
        }
    }

    /// Run the loop the way `run()` does, without a terminal, until the app is idle
    /// with nothing held and nothing on the channel, or it exits.
    fn settle(pump: &mut EventPump) -> Drained {
        for _ in 0..10_000 {
            let replayed = pump.replay_one().unwrap();
            let drained = if pump.app.is_busy() && !replayed {
                pump.wait_and_drain(Duration::from_secs(10))
            } else {
                pump.drain()
            }
            .unwrap();
            let updated = match &drained {
                Drained::Continue { updated } => *updated,
                _ => return drained,
            };
            if replayed || updated {
                continue;
            }
            assert!(
                !pump.app.is_busy(),
                "the app stayed busy with no background result"
            );
            if pump.held_keys().next().is_none() {
                return drained;
            }
        }
        panic!("the loop did not settle");
    }

    /// A pump with a three-row CSV loaded, the way `run()` loads one.
    fn loaded_pump() -> (EventPump, tempfile::TempDir) {
        crate::text_input_flows::isolate_cache();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("people.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "name,age\nada,36\ngrace,45\nalan,41").expect("write csv");
        drop(file);

        let mut pump = pump();
        pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
            .unwrap();
        settle(&mut pump);
        assert!(
            pump.app.data_table_state.is_some(),
            "the CSV should have loaded"
        );
        assert_eq!(pump.app.input_mode, InputMode::Normal);
        // A frame sets the visible row count, as it has before any key in `run()`.
        rendered(&mut pump.app);
        (pump, dir)
    }

    fn rendered(app: &mut App) -> String {
        let area = Rect::new(0, 0, 100, 20);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    }

    /// The app hands a key it cannot act on back to the caller rather than dropping
    /// it; `event()`, for callers with nowhere to hold one, drops it as before.
    #[test]
    fn a_key_while_busy_comes_back_deferred() {
        let mut p = pump();
        p.app.busy = true;
        assert!(matches!(
            p.app.handle(&AppEvent::Key(plain(KeyCode::Char('j')))),
            Err(k) if k.code == KeyCode::Char('j')
        ));
        assert!(p
            .app
            .event(&AppEvent::Key(plain(KeyCode::Char('j'))))
            .is_none());
        p.app.busy = false;
        assert!(matches!(
            p.app.handle(&AppEvent::Key(plain(KeyCode::Char('j')))),
            Ok(None)
        ));
    }

    /// Typed at a spinner, `/hello` opens the query bar with "hello" in it once the
    /// work is done. Nothing in it acts early: the `h` and `l` do not scroll a column
    /// and the `q` in `/query` does not quit.
    #[test]
    fn a_query_typed_while_busy_lands_in_the_query_bar() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        type_keys(&mut p, "/hello");
        assert_eq!(
            p.app.input_mode,
            InputMode::Normal,
            "nothing acts while busy"
        );
        assert_eq!(held(&p).len(), 6);
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

        p.app.busy = false;
        assert!(matches!(settle(&mut p), Drained::Continue { .. }));
        assert_eq!(p.app.input_mode, InputMode::Editing);
        assert_eq!(p.app.query_input.value(), "hello");

        p.app.busy = true;
        type_keys(&mut p, "query");
        p.app.busy = false;
        assert!(
            matches!(settle(&mut p), Drained::Continue { .. }),
            "q did not quit"
        );
        assert_eq!(p.app.query_input.value(), "helloquery");
    }

    /// A key that arrives behind the event that ended the busy state is handled after
    /// the keys typed before it, not before them.
    #[test]
    fn a_fresh_key_waits_behind_the_held_ones() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        type_keys(&mut p, "/abc");
        // The busy-clearing event was handled; the next key read is still behind.
        p.app.busy = false;
        type_keys(&mut p, "d");
        assert_eq!(p.app.input_mode, InputMode::Normal, "d waited its turn");

        settle(&mut p);
        assert_eq!(p.app.query_input.value(), "abcd");
    }

    /// A held Enter runs its search before the key typed after it is offered: the G
    /// stays held while the search collects, then lands on the last row of the result.
    #[test]
    fn a_held_enter_finishes_its_search_before_the_next_key() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        type_keys(&mut p, "/select name where age > 40");
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        p.terminal_key(plain(KeyCode::Char('G'))).unwrap();
        p.app.busy = false;

        // Replay up to and including the Enter; the Search it asks for is on the channel.
        while held(&p).len() > 1 {
            assert!(p.replay_one().unwrap());
            if held(&p).len() > 1 {
                assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
            }
        }
        assert_eq!(
            p.app.input_mode,
            InputMode::Editing,
            "the search has not run"
        );
        assert_eq!(held(&p), vec![KeyCode::Char('G')], "G waits for it");

        // The drain runs the Search (and whatever it spawns) with G still held.
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        assert_eq!(p.app.input_mode, InputMode::Normal);
        assert_eq!(
            p.app.data_table_state.as_ref().unwrap().get_active_query(),
            "select name where age > 40"
        );
        assert_eq!(
            held(&p),
            vec![KeyCode::Char('G')],
            "G is offered only after"
        );

        settle(&mut p);
        let state = p.app.data_table_state.as_ref().unwrap();
        assert_eq!(state.get_active_query(), "select name where age > 40");
        assert_eq!(state.num_rows, 2);
        assert_eq!(
            state.table_state.selected(),
            Some(1),
            "G went to the last row of the result"
        );
    }

    /// Past the cap the newest key is dropped and the user told; the oldest, which may
    /// be the `/` the rest were typed into, is never evicted.
    #[test]
    fn the_cap_drops_the_newest_key_and_says_so() {
        let mut p = pump();
        p.app.busy = true;
        p.app.status_message = Some("Loading buffer...".to_string());
        type_keys(&mut p, "/");
        type_keys(&mut p, &"a".repeat(39));
        assert_eq!(held(&p).len(), MAX_HELD_KEYS);
        assert_eq!(held(&p)[0], KeyCode::Char('/'));
        assert!(p.app.input_dropped);
        assert!(rendered(&mut p.app).contains("input dropped while busy"));

        p.app.busy = false;
        settle(&mut p);
        assert!(held(&p).is_empty());
        assert!(
            !p.app.input_dropped,
            "the notice goes with the last held key"
        );
    }

    /// A held navigation key repeats fast; the repeats become one press, so releasing
    /// PageDown after a collect moves one page rather than thirty-two.
    #[test]
    fn a_held_navigation_key_is_one_press() {
        let mut p = pump();
        p.app.busy = true;
        for _ in 0..50 {
            p.terminal_key(plain(KeyCode::PageDown)).unwrap();
        }
        assert_eq!(held(&p), vec![KeyCode::PageDown]);
        for _ in 0..10 {
            p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
        }
        assert_eq!(held(&p), vec![KeyCode::PageDown, KeyCode::Char('j')]);
        // Repeated text is not navigation and is kept.
        type_keys(&mut p, "aa");
        assert_eq!(held(&p).len(), 4);
    }

    /// Work that ends by opening a modal drops the keys typed before it: a held Enter
    /// was not an answer to a message the user has not seen.
    #[test]
    fn keys_held_before_an_error_modal_appears_are_dropped() {
        let mut p = pump();
        p.app.busy = true;
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        p.terminal_key(plain(KeyCode::Esc)).unwrap();
        assert_eq!(held(&p).len(), 2);

        p.send(AppEvent::BackgroundError {
            generation: p.app.task_generation(),
            message: "disk on fire".to_string(),
        })
        .unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        assert!(!p.app.is_busy());
        assert!(held(&p).is_empty());
        settle(&mut p);
        assert!(p.app.error_modal.active, "the error is still on screen");
    }

    /// Going home mid-load drops the keys typed at the load: replayed into the home
    /// screen they could open a dataset nobody asked for.
    #[test]
    fn ctrl_o_during_a_load_drops_the_held_keys() {
        let mut p = pump();
        p.app.busy = true;
        p.app.loading_state = LoadingState::Loading {
            file_path: None,
            file_size: 0,
            current_phase: "Scanning".to_string(),
            progress_percent: 0,
        };
        p.terminal_key(plain(KeyCode::Char('j'))).unwrap();
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        assert_eq!(held(&p).len(), 2);

        p.terminal_key(ctrl('o')).unwrap();
        assert_eq!(p.app.input_mode, InputMode::Home);
        assert!(!p.app.is_busy());
        assert!(held(&p).is_empty());
    }

    /// Ctrl-C and Ctrl-Q quit from any mode while busy, including the chart view,
    /// which has no CONTROL arm of its own.
    #[test]
    fn ctrl_c_quits_from_chart_mode_while_busy() {
        for c in ['c', 'q'] {
            let mut p = pump();
            p.app.input_mode = InputMode::Chart;
            p.app.chart_modal.active = true;
            p.app.busy = true;
            assert!(matches!(
                p.app.handle(&AppEvent::Key(ctrl(c))),
                Ok(Some(AppEvent::Exit))
            ));
            p.terminal_key(ctrl(c)).unwrap();
            assert!(matches!(p.drain().unwrap(), Drained::Exit));
        }
    }

    /// The home screen is never busy on its own account, so it keeps its keys while
    /// an export left running behind it finishes.
    #[test]
    fn home_keys_act_while_background_work_runs() {
        let mut p = pump();
        p.app.enter_home();
        p.app.busy = true;
        p.terminal_key(plain(KeyCode::Char('x'))).unwrap();
        assert!(held(&p).is_empty());
        assert_eq!(p.app.home.filter, "x");
    }
}

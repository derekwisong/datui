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

use crate::{App, AppEvent, GenerationLease};

/// Keys held while busy. Beyond this the newest is dropped, and the user told: the
/// oldest may be the `/` that puts the rest into the query bar, and without it the
/// tail of a typed query would replay as hotkeys.
pub const MAX_HELD_KEYS: usize = 32;

/// What a fresh key from the terminal should do while the app cannot take it directly.
enum Act {
    /// Handle it now.
    Now,
    /// Hold it for replay once the app is idle.
    Hold,
    /// Discard it: a bare Enter/Esc at a busy table confirms nothing.
    Drop,
}

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
    /// Continuations a handler returned, each holding a lease on the generation.
    ///
    /// Ahead of the channel rather than appended to it. A follow-up is the rest of the
    /// event just handled, so it belongs before results that arrived while that handler
    /// ran — and what sits in the channel right behind it is the finished phase's
    /// `BackgroundWorkFinished`, which is exactly what made the generation look free in
    /// the middle of an errand.
    ///
    /// The lease covers the gap the break leaves. A frame is drawn and the terminal is
    /// polled before the continuation runs, so a key can be handled in between, and that
    /// key must not find the generation free either.
    next_up: VecDeque<(AppEvent, GenerationLease)>,
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
            next_up: VecDeque::new(),
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

    /// A key read from the terminal. Handled now if it is a hard escape, or the app is
    /// idle with nothing queued ahead of it, or it is one of the view keys that act at a
    /// busy table; a bare Enter/Esc at a busy table is dropped; otherwise it waits behind
    /// whatever was typed before it. Returns whether the app changed.
    pub fn terminal_key(&mut self, key: KeyEvent) -> Result<bool> {
        self.discard_stale();
        match self.classify(&key) {
            Act::Now => {
                self.dispatch(key)?;
                Ok(true)
            }
            Act::Drop => Ok(false),
            Act::Hold => {
                self.hold(key);
                Ok(false)
            }
        }
    }

    fn classify(&self, key: &KeyEvent) -> Act {
        // Escapes jump ahead of anything queued, busy or idle: Ctrl-Q/Ctrl-C quit,
        // Ctrl-O goes home, a confirmation modal is answered. Checked first so a
        // Ctrl-C typed during replay is not appended behind the held keys, where a
        // modal opening could discard it.
        if self.app.hard_escape_while_busy(key) {
            return Act::Now;
        }
        let queued = !self.held.is_empty();
        // Idle with nothing ahead: ordinary front-of-line handling.
        if !self.app.is_busy() && !queued {
            return Act::Now;
        }
        // Busy, nothing queued yet, at the plain table view: the harmless view keys act
        // (quit, column scroll, help); a bare Enter/Esc confirms nothing and is dropped;
        // everything else is type-ahead and waits. Once anything is queued, or the view
        // is a text field or modal, every key waits to keep the typed order.
        if self.app.is_busy() && !queued && self.app.in_normal_table_view() {
            if self.app.key_acts_while_busy(key) {
                return Act::Now;
            }
            if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                return Act::Drop;
            }
        }
        Act::Hold
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

    /// The next event to handle: a continuation first, then the channel.
    fn take_next(&mut self) -> Result<(AppEvent, Option<GenerationLease>), TryRecvError> {
        match self.next_up.pop_front() {
            Some((event, lease)) => Ok((event, Some(lease))),
            None => self.rx.try_recv().map(|event| (event, None)),
        }
    }

    /// Handle everything waiting on the channel.
    pub fn drain(&mut self) -> Result<Drained> {
        let first = self.take_next();
        self.drain_from(first)
    }

    /// Wait up to `timeout` for the next event, then handle it and everything behind
    /// it. For drivers without a terminal to wait on: a background result is the only
    /// thing that can end a busy state.
    pub fn wait_and_drain(&mut self, timeout: Duration) -> Result<Drained> {
        // A continuation is already here; waiting on the channel would sit on it for the
        // whole timeout while the errand it belongs to is halfway through.
        if !self.next_up.is_empty() {
            let first = self.take_next();
            return self.drain_from(first);
        }
        let first = self
            .rx
            .recv_timeout(timeout)
            .map(|event| (event, None))
            .map_err(|e| match e {
                RecvTimeoutError::Timeout => TryRecvError::Empty,
                RecvTimeoutError::Disconnected => TryRecvError::Disconnected,
            });
        self.drain_from(first)
    }

    fn drain_from(
        &mut self,
        mut next: Result<(AppEvent, Option<GenerationLease>), TryRecvError>,
    ) -> Result<Drained> {
        let mut updated = false;
        loop {
            match next {
                Ok((AppEvent::Exit, _)) => return Ok(Drained::Exit),
                Ok((AppEvent::Crash(msg), _)) => return Ok(Drained::Crash(msg)),
                Ok((event, continuation)) => {
                    updated = true;
                    let follow_up = match self.app.handle(&event) {
                        Ok(follow_up) => follow_up,
                        Err(deferred) => {
                            self.hold(deferred);
                            None
                        }
                    };
                    // After the handler, never before: whatever phase this event started
                    // has taken its own lease by now, so the count does not dip to zero
                    // between the two.
                    drop(continuation);
                    self.discard_stale();
                    if let Some(follow_up) = follow_up {
                        // A handler that returns a follow-up event is deferring work so
                        // the UI can show the current phase first — the `Do*` events all
                        // rely on this. Draining the follow-up in the same pass defeats
                        // that: the phase label never renders and the throbber never
                        // moves. Break so a frame is drawn and keys are polled first.
                        self.queue_continuation(follow_up);
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(Drained::Exit),
            }
            next = self.take_next();
        }
        Ok(Drained::Continue { updated })
    }

    /// Hold a continuation, and the generation, until it is dispatched.
    fn queue_continuation(&mut self, follow_up: AppEvent) {
        let lease = self.app.lease_the_generation();
        self.next_up.push_back((follow_up, lease));
    }

    /// Offer one key to the app, the way the channel drain does, then reconcile the
    /// keys queued behind it with the screen it left.
    fn dispatch(&mut self, key: KeyEvent) -> Result<()> {
        let gen_before = self.app.screen_generation();
        match self.app.handle(&AppEvent::Key(key)) {
            Ok(Some(follow_up)) => self.queue_continuation(follow_up),
            Ok(None) => {}
            // Only reachable if the app went busy between the check and the call, which
            // nothing on this thread does; the key keeps its place either way.
            Err(deferred) => self.held.push_front(deferred),
        }
        if self.app.screen_generation() != gen_before {
            // This key abandoned the view — home, or a declined download. The keys
            // queued behind it were typed for the screen that is now gone.
            self.held.clear();
            self.app.set_input_dropped(false);
        } else {
            // A modal this key opened — an overwrite prompt, an error it surfaced — is
            // one the queued keys are the answer to, unlike a modal that arrives on its
            // own from a background result (handled in `drain_from`). Keep them and
            // re-baseline the stamp so `discard_stale` does not then drop them.
            self.held_for = Self::screen_of(&self.app);
        }
        Ok(())
    }

    /// Hold a key for later. A held navigation key repeats fast and would replay as a
    /// burst, each step chaining another collect, so consecutive repeats become one
    /// press — but only at the plain table view and only while every key already held is
    /// itself a navigation key. Once `/` or any other key is held the run is text, so
    /// nothing after it coalesces and a typed `/bookkeeper` keeps both `k`s. At the cap
    /// the newest key is dropped and the user told; never the oldest, which may be the
    /// `/` the rest were typed into.
    fn hold(&mut self, key: KeyEvent) {
        if self.held.is_empty() {
            self.held_for = Self::screen_of(&self.app);
        }
        if is_navigation(&key)
            && self.held.back() == Some(&key)
            && self.app.in_normal_table_view()
            && self.held.iter().all(is_navigation)
        {
            return;
        }
        if self.held.len() >= MAX_HELD_KEYS {
            self.app.set_input_dropped(true);
            return;
        }
        self.held.push_back(key);
    }

    /// Drop the held keys if the screen they were typed at has gone: the view was
    /// abandoned (a bumped generation), or a modal appeared that they were not answers
    /// to. A modal that a held key opens itself is re-baselined in `dispatch`, so this
    /// only fires for a change the keys did not cause — a background result, above all.
    fn discard_stale(&mut self) {
        if self.held.is_empty() {
            return;
        }
        let now = Self::screen_of(&self.app);
        let abandoned = now.generation != self.held_for.generation;
        let new_modal = now.modal && !self.held_for.modal;
        if abandoned || new_modal {
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

/// Keys that move the view and are commonly held down. Column scroll (Left/Right/h/l)
/// is included so a held one collapses when it cannot act live (behind other keys, or
/// in a modal).
fn is_navigation(key: &KeyEvent) -> bool {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Up
        | KeyCode::Down
        | KeyCode::PageUp
        | KeyCode::PageDown
        | KeyCode::Home
        | KeyCode::End
        | KeyCode::Left
        | KeyCode::Right
        | KeyCode::Char('j')
        | KeyCode::Char('k')
        | KeyCode::Char('h')
        | KeyCode::Char('l')
        | KeyCode::Char('G') => true,
        KeyCode::Char('f') | KeyCode::Char('b') | KeyCode::Char('d') | KeyCode::Char('u') => ctrl,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export_modal::{ExportFocus, ExportFormat};
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

    /// A continuation never goes to the back of the channel.
    ///
    /// The gap #221 was left short by is created by exactly one thing: putting a
    /// handler's follow-up behind whatever arrived while that handler ran — including
    /// the finished phase's `BackgroundWorkFinished`. `queue_continuation` is the only
    /// way a follow-up should travel, and `EventPump::send`, for callers pushing an
    /// event of their own, is the only `tx.send` that belongs in this file.
    #[test]
    fn a_continuation_never_goes_to_the_back_of_the_channel() {
        let source = include_str!("event_pump.rs");
        // Split so this test's own needle is not one of the things it finds.
        let needle = concat!("self.tx", ".send(");
        assert_eq!(
            source.matches(needle).count(),
            1,
            "the one send left should be `EventPump::send`. A follow-up sent to the \
             channel lands behind the lease release of the phase that produced it, and \
             the generation reads free in the middle of an errand — see GenerationLease."
        );
    }

    /// A continuation holds the generation until it has been dispatched.
    ///
    /// This is the gap #221 was left short by. An errand of several phases hands off
    /// through a returned event, and the pump breaks there so a frame can be drawn —
    /// so for one iteration of the loop the phase that finished has dropped its lease
    /// and the phase that follows has not taken one. A collect starting in that window
    /// bumps `task_generation` out from under the errand, and `BackgroundSchemaReady`'s
    /// mismatch branch then returns without resetting anything: the dataset never opens,
    /// silently, for the rest of the session.
    ///
    /// The open is the errand used here because its first handoff is the one that costs
    /// most, and because it needs no worker to reach: `Open` returns `DoLoadScanPaths`
    /// before anything has been spawned at all.
    #[test]
    fn a_continuation_holds_the_generation_until_it_is_dispatched() {
        crate::text_input_flows::isolate_cache();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("people.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "name,age\nada,36").expect("write csv");
        drop(file);

        let mut p = pump();
        assert!(!p.app.work_a_bump_would_strand(), "nothing is running yet");

        p.send(AppEvent::Open(vec![path], OpenOptions::default()))
            .unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

        // `Open` has returned `DoLoadScanPaths` and nothing has been spawned: this is
        // the moment the loop draws a frame and reads the terminal.
        assert!(
            !p.next_up.is_empty(),
            "the continuation is waiting to be dispatched"
        );
        assert!(
            p.app.work_a_bump_would_strand(),
            "and the generation is held while it waits"
        );

        // Dispatching it hands the lease to the phase it starts, rather than dropping
        // one before the other takes it.
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        assert!(
            p.app.work_a_bump_would_strand(),
            "the scan it started is running now, and holds it in turn"
        );

        settle(&mut p);
        assert!(
            p.app.data_table_state.is_some(),
            "and the open finishes, which is the point"
        );
        assert!(
            !p.app.work_a_bump_would_strand(),
            "with the generation free again afterwards"
        );
    }

    /// A key handled in that window does not find the generation free either.
    ///
    /// The pump breaks so a frame can be drawn and the terminal polled, so exactly one
    /// key can be handled between a continuation being queued and being dispatched.
    /// Ctrl-C and the other hard escapes act even while busy, and `App::handle` runs the
    /// deferred errands at its tail whatever the key was.
    #[test]
    fn a_key_in_the_handoff_window_does_not_find_the_generation_free() {
        crate::text_input_flows::isolate_cache();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("people.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "name,age\nada,36").expect("write csv");
        drop(file);

        let mut p = pump();
        p.send(AppEvent::Open(vec![path.clone()], OpenOptions::default()))
            .unwrap();
        settle(&mut p);
        assert!(
            p.app.data_table_state.is_some(),
            "a dataset to owe a collect to"
        );

        // An errand waiting for the generation to come free. Without one the tail of
        // `App::handle` has nothing to run, and the key below would prove nothing.
        p.app.collect_owed = Some((p.app.dataset_generation, "Loading buffer...".to_string()));

        // And the user opens something else, which after one drain is mid-handoff.
        p.send(AppEvent::Open(vec![path], OpenOptions::default()))
            .unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
        assert!(!p.next_up.is_empty(), "mid-handoff");
        let held_at = p.app.task_generation();

        p.terminal_key(plain(KeyCode::Char('?'))).unwrap();

        assert_eq!(
            p.app.task_generation(),
            held_at,
            "the owed collect did not go in on the back of a key handled in the window"
        );
        assert!(
            p.app.collect_owed.is_some(),
            "it is still owed, waiting for the open in front of it"
        );
    }

    /// An errand of several phases never lets go of the generation until it is done.
    ///
    /// The invariant #221 actually wants, asserted at the boundary rather than through a
    /// proxy: every time the pump breaks — which is every time a frame is drawn and a key
    /// could be handled — an errand still in progress is holding a lease. What used to
    /// cover this was a list of three flags in the predicate; what covers it now is the
    /// continuation lease, and that has to be true at each phase change rather than only
    /// at the first.
    ///
    /// The export is the errand with the most of them: collect, then write.
    #[test]
    fn an_export_holds_the_generation_at_every_phase_change() {
        let (mut p, dir) = loaded_pump();
        let out = dir.path().join("out.csv");

        p.send(AppEvent::DoExport(
            out.clone(),
            crate::ExportFormat::Csv,
            crate::ExportOptions {
                csv_delimiter: b',',
                csv_include_header: true,
                source_file: false,
                csv_compression: None,
                json_compression: None,
                ndjson_compression: None,
                parquet_compression: None,
            },
        ))
        .unwrap();

        let mut breaks = 0;
        for _ in 0..10_000 {
            let drained = if p.app.is_busy() {
                p.wait_and_drain(Duration::from_secs(10)).unwrap()
            } else {
                p.drain().unwrap()
            };
            match drained {
                Drained::Continue { updated } => {
                    // Mid-errand, at the moment the loop would draw and poll.
                    if !p.next_up.is_empty() {
                        breaks += 1;
                        assert!(
                            p.app.work_a_bump_would_strand(),
                            "phase change {breaks} left the generation free"
                        );
                    }
                    if !updated && p.next_up.is_empty() && !p.app.is_busy() {
                        break;
                    }
                }
                other => panic!("the export should not end the loop: {other:?}"),
            }
        }

        assert!(
            breaks >= 2,
            "the export handed off at least twice — collect, then write — and each was \
             checked; saw {breaks}"
        );
        assert!(out.exists(), "and the file was written, which is the point");

        // A lease is released through the channel, so the last one can still be in
        // flight when the loop above runs out of work to do — `busy` is cleared by the
        // handler that consumed the result, one event ahead of the release behind it.
        for _ in 0..200 {
            if !p.app.work_a_bump_would_strand() {
                break;
            }
            let _ = p.wait_and_drain(Duration::from_millis(50));
        }
        assert!(
            !p.app.work_a_bump_would_strand(),
            "with the generation free once it is done"
        );
    }

    /// A deferred collect that turns out to have nothing to do still takes the loading
    /// screen down.
    ///
    /// `DoLoadBuffer` clears `loading_state` itself when `spawn_async_collect` finds the
    /// buffer already serves the view. Deferred — and the open's last step is now always
    /// deferred, because the pump holds a lease for the whole of that handler — the
    /// branch that runs instead is the retry's, which knew nothing about the loading
    /// screen. It read "Loading buffer... 70%" with the app idle, for the rest of the
    /// session.
    #[test]
    fn a_deferred_collect_with_nothing_to_do_takes_the_loading_screen_down() {
        let (mut p, _dir) = loaded_pump();
        // The buffer already holds every row, so the collect will find nothing to do.
        p.app.collect_owed = Some((p.app.dataset_generation, "Loading buffer...".to_string()));
        p.app.loading_state = crate::LoadingState::Loading {
            file_path: None,
            file_size: 0,
            current_phase: "Loading buffer".to_string(),
            progress_percent: 70,
        };
        p.app.busy = true;

        p.send(AppEvent::Update).unwrap();
        assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));

        assert!(
            p.app.collect_owed.is_none(),
            "the errand is done either way"
        );
        assert!(
            matches!(p.app.loading_state, crate::LoadingState::Idle),
            "and the loading screen is down rather than stuck at 70%"
        );
        assert!(!p.app.is_busy(), "with the keyboard back");
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
        assert!(
            p.app
                .event(&AppEvent::Key(plain(KeyCode::Char('j'))))
                .is_none()
        );
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

    /// Work that ends by opening a modal from a background result drops the keys typed
    /// before it: a held key was not an answer to a message the user has not seen.
    #[test]
    fn keys_held_before_an_error_modal_appears_are_dropped() {
        let mut p = pump();
        p.app.busy = true;
        // A typed sequence (starts with `/`, so it is held, not dropped).
        type_keys(&mut p, "/x");
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

    // --- Fixes from the high-effort review of #162 --------------------------------

    /// Item 1: Ctrl-C in a focused text field copies (reaches the textarea) rather than
    /// quitting; Ctrl-Q still quits from there.
    #[test]
    fn ctrl_c_in_the_query_bar_does_not_quit() {
        let (mut p, _dir) = loaded_pump();
        p.terminal_key(plain(KeyCode::Char('/'))).unwrap();
        assert_eq!(p.app.input_mode, InputMode::Editing);

        let out = p.app.handle(&AppEvent::Key(ctrl('c')));
        assert!(
            !matches!(out, Ok(Some(AppEvent::Exit))),
            "Ctrl-C in the query bar must not quit"
        );
        assert_eq!(
            p.app.input_mode,
            InputMode::Editing,
            "still in the query bar"
        );
        // Ctrl-Q quits from anywhere, the query bar included.
        assert!(matches!(
            p.app.handle(&AppEvent::Key(ctrl('q'))),
            Ok(Some(AppEvent::Exit))
        ));
    }

    /// Ctrl-C copies in every search box, not only the query bar: the Sort tab's column
    /// search and the chart view's column searches are text fields too. At the plain
    /// table it still quits.
    #[test]
    fn ctrl_c_copies_in_the_sort_and_chart_search_boxes() {
        use crate::chart_modal::ChartFocus;
        use crate::sort_filter_modal::{SortFilterFocus, SortFilterTab};
        use crate::sort_modal::SortFocus;

        let (mut p, _dir) = loaded_pump();
        p.app.input_mode = InputMode::SortFilter;
        p.app.sort_filter_modal.active = true;
        p.app.sort_filter_modal.active_tab = SortFilterTab::Sort;
        p.app.sort_filter_modal.focus = SortFilterFocus::Body;
        p.app.sort_filter_modal.sort.focus = SortFocus::Filter;
        assert!(
            p.app.text_field_focused(),
            "the sort search is a text field"
        );
        assert!(!matches!(
            p.app.handle(&AppEvent::Key(ctrl('c'))),
            Ok(Some(AppEvent::Exit))
        ));

        let (mut p2, _d) = loaded_pump();
        p2.app.input_mode = InputMode::Chart;
        p2.app.chart_modal.active = true;
        for focus in [
            ChartFocus::XInput,
            ChartFocus::YInput,
            ChartFocus::HistInput,
            ChartFocus::BoxInput,
            ChartFocus::KdeInput,
            ChartFocus::HeatmapXInput,
            ChartFocus::HeatmapYInput,
        ] {
            p2.app.chart_modal.focus = focus;
            assert!(
                p2.app.text_field_focused(),
                "{focus:?} is a column search box"
            );
        }
        p2.app.chart_modal.focus = ChartFocus::XInput;
        assert!(!matches!(
            p2.app.handle(&AppEvent::Key(ctrl('c'))),
            Ok(Some(AppEvent::Exit))
        ));
        p2.app.chart_modal.focus = ChartFocus::XList;
        assert!(!p2.app.text_field_focused(), "a list is not a text field");

        let (mut p3, _d3) = loaded_pump();
        assert!(matches!(
            p3.app.handle(&AppEvent::Key(ctrl('c'))),
            Ok(Some(AppEvent::Exit))
        ));
    }

    /// Item 2: a `q` at the startup spinner (busy, plain table view, nothing held) quits
    /// at once; a typed `/query` is still held and typed.
    #[test]
    fn q_at_the_startup_spinner_quits_but_slash_query_types() {
        let mut p = pump();
        p.app.busy = true;
        assert!(p.app.in_normal_table_view());
        p.terminal_key(plain(KeyCode::Char('q'))).unwrap();
        assert!(
            matches!(p.drain().unwrap(), Drained::Exit),
            "q quits at once"
        );

        let mut p2 = pump();
        p2.app.busy = true;
        type_keys(&mut p2, "/query");
        assert_eq!(held(&p2).len(), 6, "the q in /query is held, not a quit");
    }

    /// Item 3: coalescing never eats a doubled letter in typed text, but does collapse a
    /// held navigation key in the plain table view.
    #[test]
    fn doubled_letters_survive_but_navigation_coalesces() {
        let mut p = pump();
        p.app.busy = true;
        type_keys(&mut p, "/bookkeeper");
        let typed: String = held(&p)
            .iter()
            .filter_map(|c| match c {
                KeyCode::Char(ch) => Some(*ch),
                _ => None,
            })
            .collect();
        assert_eq!(typed, "/bookkeeper", "both k's, o's and e's survive");

        let mut p2 = pump();
        p2.app.busy = true;
        type_keys(&mut p2, "jj");
        assert_eq!(
            held(&p2),
            vec![KeyCode::Char('j')],
            "a doubled navigation key collapses"
        );
    }

    /// Item 4: a hard escape typed during the replay window (idle, keys still held) acts
    /// at once rather than queueing behind the held keys; a non-escape waits.
    #[test]
    fn escapes_act_during_the_replay_window() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        type_keys(&mut p, "/foo");
        p.app.busy = false; // replay window: idle with keys still held

        assert!(p.terminal_key(ctrl('o')).unwrap(), "Ctrl-O acts now");
        assert_eq!(p.app.input_mode, InputMode::Home);
        assert!(held(&p).is_empty(), "going home cleared the held keys");

        let (mut p2, _d) = loaded_pump();
        p2.app.busy = true;
        type_keys(&mut p2, "/foo");
        p2.app.busy = false;
        assert!(
            !p2.terminal_key(plain(KeyCode::Char('x'))).unwrap(),
            "a fresh non-escape key waits behind the held ones"
        );
        assert_eq!(held(&p2).last().copied(), Some(KeyCode::Char('x')));
    }

    /// Item 5: a replayed Enter's Search runs before a key typed in the same moment. The
    /// run loop drains the channel after a replay, so the fresh key finds the app busy
    /// and waits; it then acts on the search's result.
    #[test]
    fn a_replayed_search_runs_before_a_fresh_key() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        type_keys(&mut p, "/select name where age > 40");
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        p.app.busy = false;

        // Replay one key, then drain, exactly as the run loop does before polling.
        loop {
            let replayed = p.replay_one().unwrap();
            assert!(matches!(p.drain().unwrap(), Drained::Continue { .. }));
            if p.app.is_busy() {
                // The Search is running: a key typed now must wait for it.
                assert!(
                    !p.terminal_key(plain(KeyCode::Char('G'))).unwrap(),
                    "G waits behind the running search"
                );
                break;
            }
            assert!(replayed, "should still be replaying the query");
        }

        settle(&mut p);
        let state = p.app.data_table_state.as_ref().unwrap();
        assert_eq!(state.get_active_query(), "select name where age > 40");
        assert_eq!(state.num_rows, 2);
        assert_eq!(
            state.table_state.selected(),
            Some(1),
            "G ran after the search, on its result"
        );
    }

    /// Item 6: a bare Enter or Esc at a busy table confirms nothing and is dropped; the
    /// Enter that submits a typed query is held because `/` is queued ahead of it.
    #[test]
    fn a_bare_enter_or_esc_at_a_busy_table_is_dropped() {
        let mut p = pump();
        p.app.busy = true;
        assert!(!p.terminal_key(plain(KeyCode::Enter)).unwrap());
        assert!(!p.terminal_key(plain(KeyCode::Esc)).unwrap());
        assert!(held(&p).is_empty(), "neither is queued");

        let (mut p2, _d) = loaded_pump();
        p2.app.busy = true;
        type_keys(&mut p2, "/x");
        p2.terminal_key(plain(KeyCode::Enter)).unwrap();
        assert_eq!(
            held(&p2).last().copied(),
            Some(KeyCode::Enter),
            "the query's Enter is held"
        );
    }

    /// Item 7: column scroll acts live at a busy table rather than queueing, and a held
    /// column-scroll key coalesces when it cannot act live.
    #[test]
    fn column_scroll_acts_live_and_coalesces() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        let before = p.app.data_table_state.as_ref().unwrap().termcol_index;
        assert!(
            p.terminal_key(plain(KeyCode::Right)).unwrap(),
            "Right scrolls a column live"
        );
        assert!(held(&p).is_empty(), "it did not queue");
        assert_eq!(
            p.app.data_table_state.as_ref().unwrap().termcol_index,
            before + 1
        );

        let (mut p2, _d) = loaded_pump();
        p2.app.busy = true;
        // A non-actor navigation key is held first, so the Rights behind it queue.
        p2.terminal_key(plain(KeyCode::Char('k'))).unwrap();
        for _ in 0..20 {
            p2.terminal_key(plain(KeyCode::Right)).unwrap();
        }
        assert_eq!(
            held(&p2),
            vec![KeyCode::Char('k'), KeyCode::Right],
            "the held Rights collapse to one"
        );
    }

    /// Item 8: F1 and `?` open help during a long load, at once, with nothing held.
    #[test]
    fn help_opens_during_a_load() {
        let (mut p, _dir) = loaded_pump();
        p.app.busy = true;
        assert!(p.terminal_key(plain(KeyCode::F(1))).unwrap());
        assert!(p.app.show_help, "F1 opened help immediately");
        assert!(held(&p).is_empty());

        let (mut p2, _d) = loaded_pump();
        p2.app.busy = true;
        assert!(p2.terminal_key(plain(KeyCode::Char('?'))).unwrap());
        assert!(p2.app.show_help, "? opened help immediately");
        assert!(held(&p2).is_empty());
    }

    /// Item 9: a modal a replayed key opens itself (an overwrite prompt) does not discard
    /// the answer keys queued behind it, so held Enter, Left, Enter completes the export.
    #[test]
    fn a_prompt_a_replayed_key_opens_keeps_its_answer_keys() {
        let (mut p, dir) = loaded_pump();
        let path = dir.path().join("out.csv");
        std::fs::write(&path, "old").expect("seed an existing file");

        // Stage the export modal on an existing path, focused on the path field.
        p.app.export_modal.active = true;
        p.app.export_modal.selected_format = ExportFormat::Csv;
        p.app.export_modal.focus = ExportFocus::PathInput;
        p.app
            .export_modal
            .path_input
            .set_value(path.display().to_string());
        p.app.input_mode = InputMode::Export;

        // Keys typed while busy in Export mode are all held (not a plain table view).
        p.app.busy = true;
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        p.terminal_key(plain(KeyCode::Left)).unwrap();
        p.terminal_key(plain(KeyCode::Enter)).unwrap();
        assert_eq!(held(&p).len(), 3);
        p.app.busy = false;

        // The first replayed Enter opens the overwrite confirmation.
        assert!(p.replay_one().unwrap());
        assert!(
            p.app.confirmation_modal.active,
            "the overwrite prompt is up"
        );
        assert_eq!(
            held(&p),
            vec![KeyCode::Left, KeyCode::Enter],
            "the answer keys were not discarded by the prompt"
        );

        settle(&mut p);
        assert!(
            p.app.success_modal.active,
            "the held Left+Enter answered the prompt and the export ran"
        );
        assert!(
            std::fs::read(&path).unwrap().len() > 3,
            "the file was overwritten with exported data"
        );
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

    /// A pump with `rows` rows loaded, named `r0000` on, so a row on screen can be
    /// told from every other.
    fn numbered_pump(rows: usize) -> (EventPump, tempfile::TempDir) {
        crate::text_input_flows::isolate_cache();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("numbered.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "name").unwrap();
        for i in 0..rows {
            writeln!(file, "r{i:04}").unwrap();
        }
        drop(file);

        let mut pump = pump();
        pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
            .unwrap();
        settle(&mut pump);
        // The frame sizes the table, and the collect it owes runs, as in `run()`.
        rendered(&mut pump.app);
        if let Some(state) = pump.app.data_table_state.as_mut()
            && std::mem::take(&mut state.needs_recollect)
        {
            pump.app.spawn_async_collect(App::LOADING_BUFFER);
        }
        settle(&mut pump);
        rendered(&mut pump.app);
        (pump, dir)
    }

    fn table(pump: &EventPump) -> &crate::widgets::datatable::DataTableState {
        pump.app.data_table_state.as_ref().expect("a dataset")
    }

    /// Page inside the buffer until the view is near enough its end to load ahead.
    fn page_to_the_edge(pump: &mut EventPump) {
        for _ in 0..50 {
            if table(pump).wants_to_load_ahead() {
                return;
            }
            pump.terminal_key(plain(KeyCode::PageDown)).unwrap();
            assert!(
                !pump.app.is_busy(),
                "paging inside the buffer fetches nothing"
            );
            rendered(&mut pump.app);
        }
        panic!("the view never came near the end of the buffer");
    }

    /// Paging towards the end of the buffer grows it before the view gets there, and
    /// nothing waits on that: no `busy`, no message, keys still live.
    #[test]
    fn paging_near_the_end_of_the_buffer_loads_ahead_quietly() {
        let (mut p, _dir) = numbered_pump(2000);
        page_to_the_edge(&mut p);
        let end = table(&p).buffered_end();

        p.app.request_what_the_frame_needs();
        assert!(
            p.app
                .collect_inflight
                .is_some_and(|inflight| !inflight.waited_on),
            "a load-ahead went out"
        );
        assert!(!p.app.is_busy(), "and nothing waits on it");
        assert_eq!(p.app.status_message, None);

        for _ in 0..100 {
            p.wait_and_drain(Duration::from_millis(100)).unwrap();
            if p.app.collect_inflight.is_none() {
                break;
            }
        }
        assert!(
            table(&p).buffered_end() > end,
            "the buffer grew ahead of the view"
        );
        assert!(!p.app.is_busy());

        // Asked once for a position: the next frame does not ask again.
        let generation = p.app.task_generation;
        p.app.request_what_the_frame_needs();
        p.app.request_what_the_frame_needs();
        assert!(p.app.task_generation - generation <= 1);
    }

    /// PageDown held while a load-ahead is out waits on it rather than fetching the same
    /// rows again, and the repeats become one press: one fetch, nothing piled up.
    #[test]
    fn holding_pagedown_during_a_load_ahead_waits_on_it() {
        let (mut p, _dir) = numbered_pump(2000);
        page_to_the_edge(&mut p);
        let state = table(&p);
        let (start, end, page) = (
            state.buffered_start(),
            state.buffered_end(),
            state.visible_rows,
        );
        let dataset = state.len_generation();
        // Out, and bringing the next few pages: no thread, so it stays out.
        let generation = p.app.task_generation;
        p.app.collect_inflight = Some(crate::InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            generation,
            dataset,
            start,
            end: end + 10 * page,
            waited_on: false,
        });

        // As `run()` takes them: a key, then whatever it set going.
        for _ in 0..30 {
            p.terminal_key(plain(KeyCode::PageDown)).unwrap();
            p.drain().unwrap();
        }
        assert_eq!(
            p.app.task_generation, generation,
            "no second fetch went out"
        );
        assert!(
            p.app
                .collect_inflight
                .is_some_and(|inflight| inflight.waited_on),
            "the page that left the buffer waits on the load-ahead"
        );
        assert!(p.app.is_busy());
        assert!(held(&p).len() <= 1, "held repeats coalesce: {:?}", held(&p));
    }

    /// A page whose rows are still coming draws the last page that was whole, not a
    /// page of blanks under the new position.
    #[test]
    fn a_page_still_loading_draws_the_last_whole_one() {
        let (mut p, _dir) = numbered_pump(2000);
        assert!(rendered(&mut p.app).contains("r0000"));
        let state = p.app.data_table_state.as_mut().unwrap();
        assert!(
            state.slide_table(1500),
            "far past the buffer, so a fetch is owed"
        );

        let screen = rendered(&mut p.app);
        assert!(screen.contains("r0000"), "the page before stays up");
    }

    /// A fetch goes unmentioned while it is young, so paging does not blink a sentence
    /// over the key chips; one that takes a while says what it is doing.
    #[test]
    fn the_bar_says_loading_only_once_a_fetch_takes_a_while() {
        let (mut p, _dir) = numbered_pump(200);
        let dataset = table(&p).len_generation();
        p.app.busy = true;
        p.app.status_message = Some(App::LOADING_BUFFER.to_string());
        p.app.collect_inflight = Some(crate::InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            generation: p.app.task_generation,
            dataset,
            start: 0,
            end: 200,
            waited_on: true,
        });
        assert!(!rendered(&mut p.app).contains("Loading buffer"));

        if let Some(inflight) = p.app.collect_inflight.as_mut() {
            inflight.began -= Duration::from_secs(1);
        }
        assert!(rendered(&mut p.app).contains("Loading buffer"));
    }
}

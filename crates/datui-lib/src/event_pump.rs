//! The main loop, minus the terminal.
//!
//! `run()` sets the terminal up and hands [`EventPump::run`] a way to draw a frame;
//! keys reach the pump through the same channel as worker results
//! ([`crate::terminal_input`]), so the loop sleeps until either arrives or a deadline
//! passes ([`Pacer`]). Everything between, including what happens to a key typed
//! while the app is busy, lives here so it can be driven without a terminal. Keys
//! typed while busy are held, in order, and replayed one per
//! loop iteration once the app is idle, each through the same path a fresh key takes.
//! Any follow-up event a replayed key produces is drained before the next held key is
//! offered, so a queued Enter finishes its search before the key typed after it acts.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::time::{Duration, Instant};

use color_eyre::Result;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent};

use crate::jobs::Hold;
use crate::pointer::Pointer;
use crate::{App, AppEvent};

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
    /// Hold this key in its place: what it means where it was typed.
    HoldAs(KeyEvent),
    /// Discard it: a bare Enter/Esc at a busy table confirms nothing.
    Drop,
    /// Handle it now, and drop the `n` and `N` held behind it: Esc stopping a find
    /// stops the finds typed after it too, or each would need an Esc of its own.
    StopFind,
}

/// Something read from the terminal for the app: a key or the mouse. Kept in the
/// order it arrived.
#[derive(Debug, Clone, Copy)]
enum Input {
    Key(KeyEvent),
    Mouse(MouseEvent),
}

/// What a pass over the channel found.
#[derive(Debug)]
pub enum Drained {
    /// Keep going; `updated` says whether anything was handled and a frame is due,
    /// and `progress_only` that everything handled was a report from work still
    /// running ([`AppEvent::is_progress`]), whose frame may wait for the next.
    Continue {
        updated: bool,
        progress_only: bool,
    },
    Exit,
    Crash(String),
    /// A path named at startup is not there.
    NotFound(std::path::PathBuf),
}

/// The screen the held keys were typed at. A change means they were meant for
/// something that is no longer there: the modal that ended the work and has not been
/// seen yet, the dataset left for the home screen, or a statement's failure put
/// under it in the query prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Screen {
    generation: u64,
    modal: bool,
    inline_failures: u64,
}

/// Owns the app, its channel and the keys held while it was busy.
pub struct EventPump {
    pub app: App,
    tx: Sender<AppEvent>,
    rx: Receiver<AppEvent>,
    held: VecDeque<KeyEvent>,
    held_for: Screen,
    /// Continuations a handler returned, each holding the generation.
    ///
    /// Ahead of the channel rather than appended to it. A follow-up is the rest of the
    /// event just handled, so it belongs before results that arrived while that handler
    /// ran.
    ///
    /// The hold covers the gap the break leaves. A frame is drawn before the
    /// continuation runs, and a key replayed then must not find the generation free.
    next_up: VecDeque<(AppEvent, Hold)>,
    /// Events that arrived before there was an app to take them, while `run` read the
    /// settings, then the startup open: handled first, in that order. The keys typed
    /// meanwhile are in [`Self::typed`].
    backlog: VecDeque<AppEvent>,
    /// Keys read from the terminal and not yet offered to the app, oldest first. Each
    /// waits for what has arrived on the channel behind it, as it did when the loop
    /// read the terminal itself: taken in channel order, a held-down `j` put the
    /// load-ahead's answer behind every repeat, scrolled off the buffer and folded the
    /// rest into one press.
    typed: VecDeque<Input>,
    /// Events handled since a key was last offered. Bounded by [`RESULTS_PER_KEY`], so
    /// a worker reporting faster than it is handled cannot starve the keyboard.
    since_key: usize,
    /// How many of the keys at the front of [`Self::typed`] came from the backlog.
    /// Typed before anything on the channel was sent, they wait on none of it: a
    /// Ctrl+O typed while the settings were read lost to the startup open whenever
    /// that open answered before the channel was ever found empty.
    early: usize,
}

/// The most channel events handled while a typed key waits; then the key is offered.
const RESULTS_PER_KEY: usize = 64;

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
            backlog: VecDeque::new(),
            typed: VecDeque::new(),
            since_key: 0,
            early: 0,
        }
    }

    /// Handle `events` before anything on the channel: they arrived first, or are the
    /// startup open those keys were typed at. Keys among them are offered, in order,
    /// after the other events and ahead of the channel.
    pub fn handle_first(&mut self, events: impl IntoIterator<Item = AppEvent>) {
        for event in events {
            match event {
                AppEvent::Terminal(Event::Key(key)) => {
                    self.typed.push_back(Input::Key(key));
                    self.early += 1;
                }
                AppEvent::Terminal(Event::Mouse(mouse)) => {
                    self.typed.push_back(Input::Mouse(mouse));
                    self.early += 1;
                }
                event => self.backlog.push_back(event),
            }
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
    /// busy table; Enter with nothing to drill into waits as Space; a bare Enter/Esc at a
    /// busy table is dropped; otherwise it waits behind whatever was typed before it.
    /// Returns whether the app changed.
    pub fn terminal_key(&mut self, key: KeyEvent) -> Result<bool> {
        self.discard_stale();
        match self.classify(&key) {
            Act::Now => {
                self.dispatch(key)?;
                Ok(true)
            }
            Act::StopFind => {
                self.drop_held_finds();
                self.dispatch(key)?;
                Ok(true)
            }
            Act::Drop => Ok(false),
            Act::Hold => {
                self.hold(key);
                Ok(false)
            }
            Act::HoldAs(meant) => {
                self.hold(meant);
                Ok(false)
            }
        }
    }

    /// A mouse event read from the terminal, which means what it lands on in the last
    /// frame ([`App::pointer`]). It is never held: aimed at what is on screen now, it
    /// would land on something else later. The wheel's arrows and a chip's key act
    /// where a typed key would act at once and are dropped where it would wait, so the
    /// wheel across moves the column cursor at a busy table, as ←→ do, and the wheel
    /// down waits for nothing. A click moves the cursor where ↓ would act at once: at
    /// an idle table with nothing held, and on the home screen, which keeps its keys.
    /// Returns whether the app changed.
    ///
    /// The rest stand for keys and act where those keys would: focusing a form's field
    /// where ↓ would, a dragged width where `>` would, a dropped header where `L`
    /// would, opening the context menu where ↓ would at the table, and a menu line or
    /// a tool pressing its key as typed.
    pub fn terminal_mouse(&mut self, mouse: MouseEvent) -> Result<bool> {
        self.discard_stale();
        let acts = |p: &Self, code: KeyCode| {
            matches!(
                p.classify(&KeyEvent::new(code, KeyModifiers::NONE)),
                Act::Now
            )
        };
        match self.app.pointer(&mouse, std::time::Instant::now()) {
            Pointer::Nothing => Ok(false),
            Pointer::Keys(keys) => self.press_now(keys),
            Pointer::Point(target, then) => {
                if !acts(self, KeyCode::Down) {
                    // Not a click the next one can make a double click of.
                    self.app.forget_click();
                    return Ok(false);
                }
                self.app.point(&target);
                self.press_now(then)?;
                Ok(true)
            }
            Pointer::Form { field, act, keys } => {
                if !acts(self, KeyCode::Down) {
                    return Ok(false);
                }
                let mut then = keys;
                if let Some(id) = field {
                    let Some(clicked) = self.app.focus_field(&id) else {
                        return Ok(false);
                    };
                    if let Some(back) = act.filter(|_| clicked.acts) {
                        then.extend(crate::pointer::act_key(clicked.kind, back));
                    }
                }
                self.press_now(then)?;
                Ok(true)
            }
            Pointer::Tool(tool) => {
                if !acts(self, KeyCode::Enter) {
                    return Ok(false);
                }
                self.app.point_at_tool(tool);
                self.press_now([KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)])?;
                Ok(true)
            }
            Pointer::Resize { column, x } => {
                if !acts(self, KeyCode::Char('>')) {
                    return Ok(false);
                }
                self.app.start_resize(column, x);
                Ok(false)
            }
            Pointer::Width { column, width } => {
                if !self.app.in_normal_table_view() || !acts(self, KeyCode::Char('>')) {
                    return Ok(false);
                }
                self.app.set_dragged_width(column, width);
                Ok(true)
            }
            Pointer::Drop { column, onto } => {
                if !self.app.in_normal_table_view() || !acts(self, KeyCode::Char('L')) {
                    return Ok(false);
                }
                if let Some(event) = self.app.drop_column(&column, &onto) {
                    self.queue_continuation(event);
                }
                Ok(true)
            }
            Pointer::Menu(hit, at) => {
                if !acts(self, KeyCode::Down) {
                    return Ok(false);
                }
                // Only on the cell the cursor landed on: rows that moved since they
                // were drawn are not the ones clicked.
                if self.app.point_for_menu(&hit) {
                    self.app.open_context_menu(at);
                }
                Ok(true)
            }
            Pointer::MenuChoose(i) => {
                if let Some(AppEvent::Press(key)) = self.app.choose_from_menu(i) {
                    self.press_now([key])?;
                }
                Ok(true)
            }
            Pointer::Redraw => Ok(true),
            Pointer::CloseMenu => {
                self.app.close_context_menu();
                Ok(true)
            }
        }
    }

    /// Press `keys` in order while each would act at once; the rest are dropped.
    fn press_now(&mut self, keys: impl IntoIterator<Item = KeyEvent>) -> Result<bool> {
        let mut acted = false;
        for key in keys {
            match self.classify(&key) {
                Act::Now => {}
                Act::StopFind => self.drop_held_finds(),
                _ => break,
            }
            self.dispatch(key)?;
            acted = true;
        }
        Ok(acted)
    }

    fn classify(&self, key: &KeyEvent) -> Act {
        // Escapes jump ahead of anything queued, busy or idle: Ctrl-Q/Ctrl-C quit,
        // Ctrl-O goes home, a confirmation modal is answered. Checked first so a
        // Ctrl-C typed during replay is not appended behind the held keys, where a
        // modal opening could discard it.
        if self.app.hard_escape_while_busy(key) {
            if key.code == KeyCode::Esc && self.app.finding() {
                return Act::StopFind;
            }
            return Act::Now;
        }
        // The open menu reads nothing: its own keys act at once. A line chosen
        // presses its key as typed, and that key waits as typed.
        if self.app.menu_takes(key) {
            return Act::Now;
        }
        let queued = !self.held.is_empty();
        // Idle with nothing ahead: ordinary front-of-line handling.
        if !self.app.is_busy() && !queued {
            return Act::Now;
        }
        // The loading screen has nothing to type ahead into, so nothing is
        // held there: the allowed keys act, everything else is dropped. Held
        // once, a stray key would queue `q` behind it for the whole load.
        if self.app.is_busy() && self.app.awaiting_dataset() {
            if self.app.key_acts_while_busy(key) {
                return Act::Now;
            }
            return Act::Drop;
        }
        // Enter with nothing to drill into is Space, and waits as Space does. Held as
        // Space, so a result that can be drilled by the time it replays is not drilled
        // into. Only while what is held moves the cursor: after a `/` it is text's Enter.
        if self.app.is_busy()
            && key.code == KeyCode::Enter
            && self.app.in_normal_table_view()
            && self.app.enter_inspects()
            && self.held.iter().all(is_navigation)
        {
            return Act::HoldAs(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        }
        // A sample being drawn holds only what needs every row: moving, finding and
        // inspecting the rows on hand act at once, the find line and inspector too.
        if self.app.is_busy() && !queued && self.app.key_acts_while_sampling(key) {
            return Act::Now;
        }
        // Busy, nothing queued yet, at the plain table view: the harmless view keys act
        // (quit, the column cursor, help); a bare Enter that would drill, or Esc, confirms
        // nothing and is dropped; everything else is type-ahead and waits. Once anything
        // is queued, or the view is a text field or modal, every key waits to keep the
        // typed order.
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

    /// The next event to handle: a continuation first, then the backlog, then the
    /// channel. `Empty` once a typed key has waited long enough, or came from the
    /// backlog, so it is offered.
    fn take_next(&mut self) -> Result<(AppEvent, Option<Hold>), TryRecvError> {
        if let Some((event, lease)) = self.next_up.pop_front() {
            return Ok((event, Some(lease)));
        }
        if let Some(event) = self.backlog.pop_front() {
            return Ok((event, None));
        }
        if !self.typed.is_empty() && (self.early > 0 || self.since_key >= RESULTS_PER_KEY) {
            return Err(TryRecvError::Empty);
        }
        self.rx.try_recv().map(|event| (event, None))
    }

    /// Handle everything waiting on the channel.
    pub fn drain(&mut self) -> Result<Drained> {
        let first = self.take_next();
        self.drain_from(first)
    }

    /// Wait up to `timeout` for the next event, then handle it and everything behind
    /// it. The run loop's only wait: keys, worker results and continuations all arrive
    /// here, so whichever comes first ends it. `Duration::MAX` waits as long as it
    /// takes.
    pub fn wait_and_drain(&mut self, timeout: Duration) -> Result<Drained> {
        // A continuation is already here; waiting on the channel would sit on it for the
        // whole timeout while the errand it belongs to is halfway through.
        if !self.next_up.is_empty() || !self.backlog.is_empty() || !self.typed.is_empty() {
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
        mut next: Result<(AppEvent, Option<Hold>), TryRecvError>,
    ) -> Result<Drained> {
        let mut updated = false;
        let mut progress_only = true;
        loop {
            match next {
                Ok((AppEvent::Exit, _)) => return Ok(Drained::Exit),
                Ok((AppEvent::Crash(msg), _)) => return Ok(Drained::Crash(msg)),
                // A path named at startup is not there: the session ends as it always
                // has, with the file named. The look's answer says so only while it is
                // current, so a user who has moved on meanwhile stays.
                Ok((AppEvent::NamedPathMissing(path), _)) => {
                    return Ok(Drained::NotFound(path));
                }
                // Offered once what arrived behind it is handled ([`Self::typed`]).
                // A key the app pressed for the user (Enter on a help line): offered
                // next, as typed, so `classify` holds, converts or drops it as it
                // would the key itself.
                Ok((AppEvent::Press(key), hold)) => {
                    if hold.is_some() {
                        drop(hold);
                        self.app.let_waiting_errands_in();
                    }
                    if self.typed.is_empty() {
                        self.since_key = 0;
                    }
                    self.typed.push_front(Input::Key(key));
                    if self.early > 0 {
                        self.early += 1;
                    }
                }
                Ok((AppEvent::Terminal(Event::Key(key)), _)) => {
                    if self.typed.is_empty() {
                        self.since_key = 0;
                    }
                    self.typed.push_back(Input::Key(key));
                }
                Ok((AppEvent::Terminal(Event::Mouse(mouse)), _)) => {
                    if self.typed.is_empty() {
                        self.since_key = 0;
                    }
                    self.typed.push_back(Input::Mouse(mouse));
                }
                Ok((AppEvent::Terminal(Event::Resize(cols, rows)), _)) => {
                    next = Ok((AppEvent::Resize(cols, rows), None));
                    continue;
                }
                Ok((AppEvent::Terminal(_), _)) => {}
                Ok((event, mut continuation)) => {
                    updated = true;
                    progress_only &= event.is_progress();
                    self.since_key += 1;
                    self.app.pointer.changed();
                    let follow_up = match self.app.handle(&event) {
                        Ok(follow_up) => follow_up,
                        Err(deferred) => {
                            self.hold(deferred);
                            None
                        }
                    };
                    self.discard_stale();
                    if let Some(follow_up) = follow_up {
                        // A handler that returns a follow-up event is deferring work so
                        // the UI can show the current phase first — the `Do*` events all
                        // rely on this. Draining the follow-up in the same pass defeats
                        // that: the phase label never renders and the throbber never
                        // moves. Break so a frame is drawn and keys are polled first.
                        //
                        // Its hold is taken before this event's is let go, so the
                        // generation is never free between the two phases.
                        self.queue_continuation(follow_up);
                        drop(continuation);
                        break;
                    }
                    // After the handler, never before: whatever phase this event started
                    // holds the generation by now. Then what waited on the hold gets its
                    // turn, as it would after any other event.
                    if let Some(hold) = continuation.take() {
                        drop(hold);
                        self.app.let_waiting_errands_in();
                    }
                }
                Err(TryRecvError::Empty) => {
                    // The pointer was aimed at the frame on screen. When something has
                    // been handled since (a resize, a list that arrived, rows read), it
                    // waits for the frame that shows it, which this asks for now.
                    if matches!(self.typed.front(), Some(Input::Mouse(_)))
                        && !self.app.pointer.on_screen()
                    {
                        updated = true;
                        progress_only = false;
                        break;
                    }
                    let Some(mut input) = self.typed.pop_front() else {
                        break;
                    };
                    // A drag reports every cell the pointer crosses; only where it is
                    // now matters, so the moves waiting behind it are one.
                    while let (Input::Mouse(now), Some(Input::Mouse(next))) =
                        (input, self.typed.front())
                        && is_drag(&now)
                        && is_drag(next)
                    {
                        input = Input::Mouse(*next);
                        self.typed.pop_front();
                        self.early = self.early.saturating_sub(1);
                    }
                    self.since_key = 0;
                    self.early = self.early.saturating_sub(1);
                    // One key per frame, as when the loop read the terminal itself: a
                    // key that acted is drawn before the next is offered, and a
                    // continuation it queued gets its frame first.
                    let acted = match input {
                        Input::Key(key) => self.terminal_key(key)?,
                        Input::Mouse(mouse) => self.terminal_mouse(mouse)?,
                    };
                    if acted {
                        updated = true;
                        progress_only = false;
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => return Ok(Drained::Exit),
            }
            next = self.take_next();
        }
        Ok(Drained::Continue {
            updated,
            progress_only: updated && progress_only,
        })
    }

    /// The run loop, given a way to draw a frame: draw the first one, then replay one
    /// held key, handle what has arrived, sleep until something arrives or a deadline
    /// passes, and redraw, until the app exits.
    ///
    /// Keys ([`AppEvent::Terminal`]), worker results and the app's own news all come
    /// through the one channel, so whichever is first ends the sleep; nothing waits out
    /// a poll interval. Whatever was handled is drawn before the loop sleeps, and a
    /// continuation queued by it runs right after that frame.
    pub fn run(&mut self, mut draw: impl FnMut(&mut App) -> Result<()>) -> Result<Ended> {
        let mut pacer = Pacer::default();
        let mut first_rows = crate::first_rows_trace::FirstRowsTrace::from_env();
        draw(&mut self.app)?;
        self.app.frame_painted();
        first_rows.painted(&self.app);
        pacer.drew(Instant::now());
        loop {
            let mut pass = Pass::default();
            // A replayed key may have queued a follow-up (a Search, an Export); it is
            // handled in this drain, before anything typed since can overtake it.
            pass.updated = self.replay_one()?;
            pass.progress_only = !pass.updated;
            if let Some(end) = pass.add(self.drain()?) {
                return Ok(end);
            }
            if !pass.updated {
                let now = Instant::now();
                pacer.spinning(self.app.something_is_spinning(), now);
                let timeout = pacer.timeout(self.app.next_deadline(), now);
                if let Some(end) = pass.add(self.wait_and_drain(timeout)?) {
                    return Ok(end);
                }
            }
            let app = &mut self.app;
            let now = Instant::now();
            let mut redraw = pacer.handled(pass.updated, pass.progress_only, now);
            // The throbber turns while busy, and while a background row count or
            // anything else with a spinner of its own is still out.
            pacer.spinning(app.something_is_spinning(), now);
            if pacer.turn_spinner(now) {
                app.throbber_frame = app.throbber_frame.wrapping_add(1);
                redraw = true;
            }
            redraw |= app.tick_flash();
            redraw |= app.tick_follow_clock();
            redraw |= app.flash_background_panic();
            redraw |= app.flash_polars_warning();

            app.request_what_the_frame_needs();

            if redraw {
                draw(app)?;
                // The rows the frame found it needs are read, and a count waiting for
                // them to be on screen starts.
                app.frame_painted();
                first_rows.painted(app);
                pacer.drew(now);
                // And it marks the rows it drew without knowing them. Asked now, not
                // on the next pass: with nothing else arriving there may not be one.
                app.request_what_the_frame_needs();
            }
        }
    }

    /// Hold a continuation, and the generation, until it is dispatched.
    fn queue_continuation(&mut self, follow_up: AppEvent) {
        let hold = self.app.hold_the_generation();
        self.next_up.push_back((follow_up, hold));
    }

    /// Offer one key to the app, the way the channel drain does, then reconcile the
    /// keys queued behind it with the screen it left.
    fn dispatch(&mut self, key: KeyEvent) -> Result<()> {
        // The key may change the screen: a click waits for the frame that shows it.
        self.app.pointer.changed();
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
    /// nothing after it coalesces and a typed `/bookkeeper` keeps both `k`s. A column
    /// cursor key reads nothing, so each one is kept: `l` `l` `F` counts the column two
    /// along, as it would idle. So is each `n` and `N`: five typed move five matches.
    /// At the cap
    /// the newest key is dropped and the user told; never the oldest, which may be the
    /// `/` the rest were typed into.
    fn hold(&mut self, key: KeyEvent) {
        if self.held.is_empty() {
            self.held_for = Self::screen_of(&self.app);
        }
        if is_navigation(&key)
            && !replays_each_press(&key)
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

    /// Drop the `n` and `N` held at the front, among the keys that move the cursor.
    /// Past any other key they are text, or meant for what that key opens.
    fn drop_held_finds(&mut self) {
        let run = self.held.iter().take_while(|k| is_navigation(k)).count();
        let rest = self.held.split_off(run);
        self.held
            .retain(|k| !matches!(k.code, KeyCode::Char('n' | 'N')));
        self.held.extend(rest);
        if self.held.is_empty() {
            self.app.set_input_dropped(false);
        }
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
        let failed_inline = now.inline_failures != self.held_for.inline_failures;
        if abandoned || new_modal || failed_inline {
            self.held.clear();
            self.app.set_input_dropped(false);
        }
    }

    fn screen_of(app: &App) -> Screen {
        Screen {
            generation: app.screen_generation(),
            modal: app.modal_showing(),
            inline_failures: app.inline_failures(),
        }
    }
}

/// How the run loop ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    Quit,
    Crash(String),
    /// A path named at startup is not there.
    NotFound(std::path::PathBuf),
}

/// What one turn of the run loop handled.
#[derive(Default)]
struct Pass {
    updated: bool,
    progress_only: bool,
}

impl Pass {
    /// Fold one channel drain in; or say how the loop ends.
    fn add(&mut self, drained: Drained) -> Option<Ended> {
        match drained {
            Drained::Continue {
                updated,
                progress_only,
            } => {
                if updated {
                    self.progress_only = progress_only && (self.progress_only || !self.updated);
                    self.updated = true;
                }
                None
            }
            Drained::Exit => Some(Ended::Quit),
            Drained::Crash(msg) => Some(Ended::Crash(msg)),
            Drained::NotFound(path) => Some(Ended::NotFound(path)),
        }
    }
}

/// How often a spinner turns: about 30 frames a second, plenty for a throbber.
pub const SPINNER_FRAME: Duration = Duration::from_millis(33);

/// The least time between two frames drawn for progress reports alone. A worker
/// reporting a thousand times a second is drawn thirty times; a key or a result is
/// drawn at once.
pub const PROGRESS_FRAME: Duration = Duration::from_millis(33);

/// When the run loop draws, and how long it may sleep.
///
/// The loop sleeps until something arrives or a deadline passes, never on a fixed
/// tick: idle, it does not wake at all. The deadlines are the spinner's next frame
/// while one is on screen, a progress frame owed, and whatever the app says will
/// change on its own (a flash expiring).
#[derive(Debug, Default)]
pub struct Pacer {
    last_draw: Option<Instant>,
    /// A frame for progress reports, held back until [`PROGRESS_FRAME`] has passed.
    owed: bool,
    /// The spinner's next frame, while one is on screen.
    spin_due: Option<Instant>,
}

impl Pacer {
    /// Say whether a spinner is on screen. One that starts turns a frame later.
    pub fn spinning(&mut self, on: bool, now: Instant) {
        if on {
            self.spin_due.get_or_insert(now + SPINNER_FRAME);
        } else {
            self.spin_due = None;
        }
    }

    /// Whether the spinner's next frame is due; moves the deadline on when it is.
    pub fn turn_spinner(&mut self, now: Instant) -> bool {
        match self.spin_due {
            Some(due) if now >= due => {
                self.spin_due = Some(now + SPINNER_FRAME);
                true
            }
            _ => false,
        }
    }

    /// What a pass handled, and whether to draw for it now. A pass of progress
    /// reports alone, close behind the last frame, is owed a frame instead.
    pub fn handled(&mut self, updated: bool, progress_only: bool, now: Instant) -> bool {
        let progress_due = self
            .last_draw
            .is_none_or(|last| now >= last + PROGRESS_FRAME);
        if updated && !progress_only {
            return true;
        }
        if updated {
            self.owed = true;
        }
        self.owed && progress_due
    }

    /// A frame was drawn.
    pub fn drew(&mut self, now: Instant) {
        self.last_draw = Some(now);
        self.owed = false;
    }

    /// How long the loop may sleep: until the earliest deadline, or for as long as
    /// it takes when there is none.
    pub fn timeout(&self, app_deadline: Option<Instant>, now: Instant) -> Duration {
        let owed = self
            .owed
            .then(|| self.last_draw.map(|last| last + PROGRESS_FRAME))
            .flatten();
        [self.spin_due, owed, app_deadline]
            .into_iter()
            .flatten()
            .min()
            .map_or(Duration::MAX, |at| at.saturating_duration_since(now))
    }
}

/// Navigation keys a held run keeps every press of: the column cursor's (`h` `l`
/// `{` `}` and the arrows across, Shift for a page), which read nothing, and a find's
/// next and previous, each its own match.
fn replays_each_press(key: &KeyEvent) -> bool {
    matches!(
        key.code,
        KeyCode::Left
            | KeyCode::Right
            | KeyCode::Char('h')
            | KeyCode::Char('l')
            | KeyCode::Char('{')
            | KeyCode::Char('}')
            | KeyCode::Char('n')
            | KeyCode::Char('N')
    )
}

fn is_drag(mouse: &MouseEvent) -> bool {
    matches!(mouse.kind, crossterm::event::MouseEventKind::Drag(_))
}

/// Keys that move the view and are commonly held down. The column cursor's keys
/// (Left/Right/h/l, `{ }`) are included, so Enter behind them still inspects.
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
        | KeyCode::Char('{')
        | KeyCode::Char('}')
        | KeyCode::Char('G')
        // A find's next and previous move the cursor too.
        | KeyCode::Char('n')
        | KeyCode::Char('N') => true,
        KeyCode::Char('f') | KeyCode::Char('b') | KeyCode::Char('d') | KeyCode::Char('u') => ctrl,
        _ => false,
    }
}

#[cfg(test)]
mod tests;

//! The main loop, minus the terminal.
//!
//! `run()` sets up the terminal and hands [`EventPump::run`] a way to draw. Keys
//! arrive on the same channel as worker results ([`crate::app::terminal_input`]), so the
//! loop sleeps until either arrives or a deadline passes ([`Pacer`]). Keys typed
//! while busy are held in order and replayed one per iteration once idle, through
//! the path a fresh key takes; a replayed key's follow-ups drain before the next is
//! offered, so a queued Enter finishes its search first. Keys that arrive together
//! and act at once are drawn together ([`EventPump::burst_goes_on`]).

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::time::{Duration, Instant};

use color_eyre::Result;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent};

use crate::app::jobs::Hold;
use crate::app::keys::paste_keys::{self, PasteTarget};
use crate::app::pointer::Pointer;
use crate::{App, AppEvent};

/// Keys held while busy. Beyond this the newest is dropped and the user told: the
/// oldest may be the `/` that makes the rest a query rather than hotkeys.
pub const MAX_HELD_KEYS: usize = 32;

/// What a fresh key should do while the app cannot take it directly.
enum Act {
    /// Handle it now.
    Now,
    /// Hold it for replay once the app is idle.
    Hold,
    /// Hold this key instead: what the typed key means where it was typed.
    HoldAs(KeyEvent),
    /// Discard it: a bare Enter/Esc at a busy table confirms nothing.
    Drop,
    /// Handle it now and drop the `n`/`N` held behind it: one Esc stops every queued
    /// find.
    StopFind,
}

/// A key, mouse event or paste read from the terminal, kept in arrival order.
#[derive(Debug, Clone)]
enum Input {
    Key(KeyEvent),
    Mouse(MouseEvent),
    Paste(String),
}

/// Typed input held while the app was busy, replayed in order once it is idle.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Held {
    Key(KeyEvent),
    /// A paste, taken as one edit by whatever field types when it is replayed.
    Paste(String),
}

impl Held {
    fn key(&self) -> Option<&KeyEvent> {
        match self {
            Held::Key(key) => Some(key),
            Held::Paste(_) => None,
        }
    }

    fn is_navigation(&self) -> bool {
        self.key().is_some_and(is_navigation)
    }
}

/// What a pass over the channel found.
#[derive(Debug)]
pub enum Drained {
    /// Keep going. `updated`: something was handled and a frame is due.
    /// `progress_only`: all of it was progress reports ([`AppEvent::is_progress`]),
    /// whose frame may wait.
    Continue {
        updated: bool,
        progress_only: bool,
    },
    Exit,
    Crash(String),
    /// A path named at startup is not there.
    NotFound(std::path::PathBuf),
}

/// The screen the held keys were typed at. A change means their target is gone: a
/// modal that ended the work, the dataset left for home, or a statement's failure
/// under the query prompt.
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
    held: VecDeque<Held>,
    held_for: Screen,
    /// Continuations a handler returned, each holding the generation. Ahead of the
    /// channel: a follow-up is the rest of the event just handled. The hold covers the
    /// frame drawn before it runs, so a replayed key cannot find the generation free.
    next_up: VecDeque<(AppEvent, Hold)>,
    /// Events that arrived before the app existed (while `run` read the settings, then
    /// the startup open), handled first in order. Keys typed meanwhile are in
    /// [`Self::typed`].
    backlog: VecDeque<AppEvent>,
    /// Keys read and not yet offered, oldest first. Each waits for what arrived on the
    /// channel behind it, so a held-down `j` does not starve the load-ahead's answer.
    typed: VecDeque<Input>,
    /// Events handled since a key was last offered, bounded by [`RESULTS_PER_KEY`] so a
    /// fast worker cannot starve the keyboard.
    since_key: usize,
    /// How many keys at the front of [`Self::typed`] came from the backlog. Typed
    /// before anything was sent on the channel, they wait on none of it (a Ctrl+O
    /// typed during startup must beat the startup open).
    early: usize,
}

/// The most channel events handled while a typed key waits; then the key is offered.
const RESULTS_PER_KEY: usize = 64;

/// The most keys of a burst handled before a frame shows them ([`EventPump::burst_goes_on`]).
const KEYS_PER_FRAME: usize = 64;

/// The longest a burst's keys are handled before a frame shows them. Tests count
/// frames, and a loaded test machine must not split a burst.
const BURST_FRAME: Duration = if cfg!(test) {
    Duration::from_secs(10)
} else {
    Duration::from_millis(50)
};

/// The keys handled since the last frame, in one pass over the channel.
#[derive(Default)]
struct Burst {
    started: Option<Instant>,
    keys: usize,
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
            backlog: VecDeque::new(),
            typed: VecDeque::new(),
            since_key: 0,
            early: 0,
        }
    }

    /// Handle `events` before the channel: they arrived first, or are the startup open
    /// those keys were typed at. Keys among them are offered after the other events,
    /// ahead of the channel.
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
                AppEvent::Terminal(Event::Paste(text)) => {
                    self.typed.push_back(Input::Paste(text));
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

    /// The keys waiting for the app to go idle, oldest first (pastes left out).
    pub fn held_keys(&self) -> impl Iterator<Item = &KeyEvent> {
        self.held.iter().filter_map(Held::key)
    }

    /// A key from the terminal, classified by `classify`: handled now, held behind
    /// earlier keys, held as another key, or dropped. Returns whether the app changed.
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

    /// A mouse event, meaning what it lands on in the last frame ([`App::pointer`]).
    /// Never held: aimed at the screen now, it would land elsewhere later. Each acts
    /// where the key it stands for would act at once, and is dropped where that key
    /// would wait: the wheel as arrows, a click as ↓, a chip or menu line or tool as
    /// its key, a dragged width as `>`, a dropped header as `L`. Returns whether the app
    /// changed.
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
                        then.extend(crate::app::pointer::act_key(clicked.kind, back));
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
                // Only on the cell the cursor landed on: rows that moved since drawing were not
                // the ones clicked.
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

    /// A paste, taken as one edit by the field that takes typed text
    /// ([`App::paste_target`]); where nothing does, dropped, never read as keys. Typed
    /// where a key would wait, it waits in its place, and goes to the field focused
    /// when it is replayed. Returns whether the app changed.
    pub fn terminal_paste(&mut self, text: String) -> Result<bool> {
        self.discard_stale();
        // Held keys (a `:` typed while busy) may open the field it is meant for.
        if self.held.is_empty() && self.app.paste_target() == PasteTarget::Nowhere {
            return Ok(false);
        }
        // Classified as the text's first character, typed.
        let Some(first) = paste_keys::one_line(&text).chars().next() else {
            return Ok(false);
        };
        match self.classify(&KeyEvent::new(KeyCode::Char(first), KeyModifiers::NONE)) {
            Act::Now => {
                self.offer(AppEvent::Paste(text))?;
                Ok(true)
            }
            Act::Hold | Act::HoldAs(_) => {
                self.hold_input(Held::Paste(text));
                Ok(false)
            }
            Act::Drop | Act::StopFind => Ok(false),
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
        // Escapes jump the queue, busy or idle (Ctrl-Q/Ctrl-C quit, Ctrl-O home, a
        // confirmation answered). First, so a Ctrl-C during replay is not queued where a
        // modal could discard it.
        if self.app.hard_escape_while_busy(key) {
            if key.code == KeyCode::Esc && self.app.finding() {
                return Act::StopFind;
            }
            return Act::Now;
        }
        // The open menu's keys read nothing and act at once; a chosen line presses its key
        // as typed.
        if self.app.menu_takes(key) {
            return Act::Now;
        }
        let queued = !self.held.is_empty();
        // Idle with nothing ahead: handle now.
        if !self.app.is_busy() && !queued {
            return Act::Now;
        }
        // The loading screen has nothing to type into: allowed keys act, the rest drop
        // (one held stray would queue `q` behind it for the whole load).
        if self.app.is_busy() && self.app.awaiting_dataset() {
            if self.app.key_acts_while_busy(key) {
                return Act::Now;
            }
            return Act::Drop;
        }
        // Enter with nothing to drill into is Space, held as Space so a result drillable
        // by replay time is not drilled. Only while held keys move the cursor: after `/`
        // it is text's Enter.
        if self.app.is_busy()
            && key.code == KeyCode::Enter
            && self.app.in_normal_table_view()
            && self.app.enter_inspects()
            && self.held.iter().all(Held::is_navigation)
        {
            return Act::HoldAs(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        }
        // A sample draw holds only what needs every row: moving, finding and inspecting
        // the rows on hand act at once.
        if self.app.is_busy() && !queued && self.app.key_acts_while_sampling(key) {
            return Act::Now;
        }
        // Busy at the plain table with nothing queued: harmless view keys act; a bare Enter
        // or Esc confirms nothing and drops; everything else waits. Once anything is
        // queued, or in a text field or modal, every key waits to keep typed order.
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
        let Some(input) = self.held.pop_front() else {
            return Ok(false);
        };
        if self.held.is_empty() {
            self.app.set_input_dropped(false);
        }
        match input {
            Held::Key(key) => self.dispatch(key)?,
            Held::Paste(text) => self.offer(AppEvent::Paste(text))?,
        }
        Ok(true)
    }

    /// The next event: a continuation, then the backlog, then the channel. `Empty` once
    /// a typed key has waited long enough, or came from the backlog, so it is offered.
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
    /// it. The run loop's only wait. `Duration::MAX` waits indefinitely.
    pub fn wait_and_drain(&mut self, timeout: Duration) -> Result<Drained> {
        // A continuation is waiting: do not sit on it for the whole timeout.
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
        let mut burst = Burst::default();
        loop {
            match next {
                Ok((AppEvent::Exit, _)) => return Ok(Drained::Exit),
                Ok((AppEvent::Crash(msg), _)) => return Ok(Drained::Crash(msg)),
                // A path named at startup is not there: the session ends naming it. The look only
                // says so while current, so a user who moved on stays.
                Ok((AppEvent::NamedPathMissing(path), _)) => {
                    return Ok(Drained::NotFound(path));
                }
                // Offered once what arrived behind it is handled ([`Self::typed`]). A key pressed
                // for the user (Enter on a help line) is offered next as typed, so `classify`
                // treats it like the key itself.
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
                Ok((AppEvent::Terminal(Event::Paste(text)), _)) => {
                    if self.typed.is_empty() {
                        self.since_key = 0;
                    }
                    self.typed.push_back(Input::Paste(text));
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
                    let follow_up = match self.app.handle(event) {
                        Ok(follow_up) => follow_up,
                        Err(deferred) => {
                            self.hold(deferred);
                            None
                        }
                    };
                    self.discard_stale();
                    if let Some(follow_up) = follow_up {
                        // A follow-up defers work so the UI can show the current phase first (the `Do*`
                        // events rely on it). Break so a frame is drawn and keys are polled before it
                        // runs. Its hold is taken before this event's is released, so the generation is
                        // never free between phases.
                        self.queue_continuation(follow_up);
                        drop(continuation);
                        break;
                    }
                    // After the handler: whatever phase this event started holds the generation now.
                    // Then errands waiting on the hold get their turn.
                    if let Some(hold) = continuation.take() {
                        drop(hold);
                        self.app.let_waiting_errands_in();
                    }
                }
                Err(TryRecvError::Empty) => {
                    // The pointer was aimed at the frame on screen; after changes it waits for the
                    // frame showing them, requested now.
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
                    // A drag reports every cell crossed; only the latest position matters.
                    while let (Input::Mouse(now), Some(Input::Mouse(next))) =
                        (&input, self.typed.front())
                        && is_drag(now)
                        && is_drag(next)
                    {
                        input = Input::Mouse(*next);
                        self.typed.pop_front();
                        self.early = self.early.saturating_sub(1);
                    }
                    self.since_key = 0;
                    self.early = self.early.saturating_sub(1);
                    let acted = match input {
                        Input::Key(key) => self.terminal_key(key)?,
                        Input::Mouse(mouse) => self.terminal_mouse(mouse)?,
                        Input::Paste(text) => self.terminal_paste(text)?,
                    };
                    if acted {
                        updated = true;
                        progress_only = false;
                        if !self.burst_goes_on(&mut burst) {
                            break;
                        }
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

    /// Whether the keys typed behind one that just acted are handled before the frame
    /// showing it. A burst (key repeat, keys queued behind a slow frame) is drawn once:
    /// redrawing for every key only sends frames the screen replaces at once. A key
    /// that queued a follow-up gets its frame first (the follow-up shows its phase),
    /// as does one that made the app busy (its spinner) or held keys, and a burst is
    /// drawn every [`KEYS_PER_FRAME`] keys or [`BURST_FRAME`] so a held key shows
    /// motion. A click waits for its frame anyway (see `drain_from`).
    fn burst_goes_on(&self, burst: &mut Burst) -> bool {
        let started = *burst.started.get_or_insert_with(Instant::now);
        burst.keys += 1;
        self.next_up.is_empty()
            && self.held.is_empty()
            && !self.app.is_busy()
            && burst.keys < KEYS_PER_FRAME
            && started.elapsed() < BURST_FRAME
    }

    /// The run loop: draw the first frame, then replay one held key, handle what has
    /// arrived, sleep until something arrives or a deadline passes, and redraw, until
    /// exit. Everything comes through one channel, so nothing waits out a poll
    /// interval; a queued continuation runs right after the frame showing its phase.
    pub fn run(&mut self, mut draw: impl FnMut(&mut App) -> Result<()>) -> Result<Ended> {
        let mut pacer = Pacer::default();
        let mut first_rows = crate::loading::first_rows_trace::FirstRowsTrace::from_env();
        draw(&mut self.app)?;
        self.app.frame_painted();
        first_rows.painted(&self.app);
        pacer.drew(Instant::now());
        loop {
            let mut pass = Pass::default();
            // A replayed key's follow-up (a Search, an Export) is handled in this drain,
            // before anything typed since can overtake it.
            pass.updated = self.replay_one()?;
            pass.progress_only = !pass.updated;
            if let Some(end) = pass.add(self.drain()?) {
                return Ok(end);
            }
            if !pass.updated {
                let now = Instant::now();
                pacer.spinning(self.app.something_is_spinning(), self.app.is_busy(), now);
                let timeout = pacer.timeout(self.app.next_deadline(), now);
                if let Some(end) = pass.add(self.wait_and_drain(timeout)?) {
                    return Ok(end);
                }
            }
            let app = &mut self.app;
            let now = Instant::now();
            let mut redraw = pacer.handled(pass.updated, pass.progress_only, now);
            // The throbber turns while busy, or while a count or anything else with a spinner
            // is out.
            pacer.spinning(app.something_is_spinning(), app.is_busy(), now);
            if pacer.turn_spinner(now) {
                app.throbber_frame = app.throbber_frame.wrapping_add(1);
                redraw = true;
            }
            redraw |= app.tick_flash();
            redraw |= app.tick_follow_clock();
            redraw |= app.flash_background_panic();
            redraw |= app.flash_polars_warning();

            // `App::frame_work` is this and the frame below without the paint, for
            // harnesses: keep the two in step.
            app.request_what_the_frame_needs();

            if redraw {
                draw(app)?;
                // Read the rows the frame needed, and start a count waiting on their paint.
                app.frame_painted();
                first_rows.painted(app);
                pacer.drew(now);
                // Ask now for what the frame drew without knowing: there may be no next pass.
                app.request_what_the_frame_needs();
            }
        }
    }

    /// Hold a continuation, and the generation, until it is dispatched.
    fn queue_continuation(&mut self, follow_up: AppEvent) {
        let hold = self.app.hold_the_generation();
        self.next_up.push_back((follow_up, hold));
    }

    /// Offer one key to the app as the channel drain does, then reconcile the held keys
    /// with the screen it left.
    fn dispatch(&mut self, key: KeyEvent) -> Result<()> {
        self.offer(AppEvent::Key(key))
    }

    /// Offer typed input (a key or a paste) to the app, then reconcile the held keys
    /// with the screen it left.
    fn offer(&mut self, input: AppEvent) -> Result<()> {
        // The input may change the screen: a click waits for the frame that shows it.
        self.app.pointer.changed();
        let gen_before = self.app.screen_generation();
        match self.app.handle(input) {
            Ok(Some(follow_up)) => self.queue_continuation(follow_up),
            Ok(None) => {}
            // Only if the app went busy between check and call, which nothing on this thread
            // does; the key keeps its place either way.
            Err(deferred) => self.held.push_front(Held::Key(deferred)),
        }
        if self.app.screen_generation() != gen_before {
            // This input left the view (home, a declined download): held keys were for that
            // screen.
            self.held.clear();
            self.app.set_input_dropped(false);
        } else {
            // A modal this key opened (an overwrite prompt, an error) is one the held keys
            // answer, unlike one a background result brings: keep them and re-baseline so
            // `discard_stale` does not drop them.
            self.held_for = Self::screen_of(&self.app);
        }
        Ok(())
    }

    /// Hold a key. At the plain table, while every held key is navigation, repeats of
    /// one key coalesce into a press (each would chain a collect). Once `/` or any
    /// other key is held the run is text, so `/bookkeeper` keeps both `k`s. Column
    /// cursor keys and `n`/`N` are each kept: they read nothing or move one match each.
    /// At the cap the newest is dropped and the user told; never the oldest, which may
    /// be the `/`.
    fn hold(&mut self, key: KeyEvent) {
        if is_navigation(&key)
            && !replays_each_press(&key)
            && self.held.back().and_then(Held::key) == Some(&key)
            && self.app.in_normal_table_view()
            && self.held.iter().all(Held::is_navigation)
        {
            return;
        }
        self.hold_input(Held::Key(key));
    }

    /// Hold typed input behind what is held, up to [`MAX_HELD_KEYS`].
    fn hold_input(&mut self, input: Held) {
        if self.held.is_empty() {
            self.held_for = Self::screen_of(&self.app);
        }
        if self.held.len() >= MAX_HELD_KEYS {
            self.app.set_input_dropped(true);
            return;
        }
        self.held.push_back(input);
    }

    /// Drop the `n`/`N` held at the front among cursor moves; past any other key they
    /// are text or meant for what it opens.
    fn drop_held_finds(&mut self) {
        let run = self.held.iter().take_while(|k| k.is_navigation()).count();
        let rest = self.held.split_off(run);
        self.held.retain(|k| {
            !k.key()
                .is_some_and(|k| matches!(k.code, KeyCode::Char('n' | 'N')))
        });
        self.held.extend(rest);
        if self.held.is_empty() {
            self.app.set_input_dropped(false);
        }
    }

    /// Drop the held keys if their screen is gone: the view abandoned, or a modal they
    /// were not answers to. A modal a held key opened is re-baselined in `dispatch`, so
    /// this fires for changes the keys did not cause.
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

/// How often a spinner turns while the user waits: about 30 frames a second.
pub const SPINNER_FRAME: Duration = Duration::from_millis(33);

/// How often it turns for unwaited work (a count, a read-ahead): ten frames a
/// second, redrawing a third as often.
pub const SPINNER_IDLE_FRAME: Duration = Duration::from_millis(100);

/// The least time between frames drawn for progress reports alone; keys and
/// results draw at once.
pub const PROGRESS_FRAME: Duration = Duration::from_millis(33);

/// When the run loop draws and how long it may sleep: never on a fixed tick, only
/// until the spinner's next frame, an owed progress frame, or the app's own
/// deadline (a flash expiring).
#[derive(Debug, Default)]
pub struct Pacer {
    last_draw: Option<Instant>,
    /// A frame for progress reports, held back until [`PROGRESS_FRAME`] has passed.
    owed: bool,
    /// The spinner's next frame, while one is on screen.
    spin_due: Option<Instant>,
    /// How far apart its frames are: see [`SPINNER_IDLE_FRAME`].
    spin_every: Duration,
}

impl Pacer {
    /// Say whether a spinner is on screen and whether the user waits on it; a new one
    /// turns a frame later.
    pub fn spinning(&mut self, on: bool, waited_on: bool, now: Instant) {
        self.spin_every = if waited_on {
            SPINNER_FRAME
        } else {
            SPINNER_IDLE_FRAME
        };
        if on {
            let next = now + self.spin_every;
            self.spin_due = Some(self.spin_due.map_or(next, |due| due.min(next)));
        } else {
            self.spin_due = None;
        }
    }

    /// Whether the spinner's next frame is due; moves the deadline on when it is.
    pub fn turn_spinner(&mut self, now: Instant) -> bool {
        match self.spin_due {
            Some(due) if now >= due => {
                self.spin_due = Some(now + self.spin_every);
                true
            }
            _ => false,
        }
    }

    /// Whether to draw for a pass now. Progress-only passes close behind the last frame
    /// are owed one instead.
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

    /// How long the loop may sleep: until the earliest deadline, or indefinitely.
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

/// Navigation keys a held run keeps every press of: the column cursor's, which read
/// nothing, and a find's next and previous, each its own match.
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

/// Keys that move the view and are often held down; column cursor keys included,
/// so Enter behind them still inspects.
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

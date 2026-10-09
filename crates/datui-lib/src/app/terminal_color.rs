//! Asking the terminal for its background (OSC 11) so `theme.mode = "auto"` picks the
//! palette from the screen. The reply (`ESC ] 11 ; rgb:RRRR/GGGG/BBBB` then BEL or ST)
//! arrives on the input stream, which Crossterm reads ([`crate::app::terminal_input`]) as
//! Alt+`]` and typed characters, so while a question is out ([`armed`]) the reader takes
//! that run off the stream ([`ReplyScanner`]). Nothing waits: the first frame uses this
//! terminal's last answer (cached under [`terminal_key`]); a later answer switches if
//! different; silence lapses after [`ARMED_FOR`]. DEC mode 2031 is not used: Crossterm
//! 0.29 buffers such a `CSI ?` report forever, swallowing later keys; the question is
//! asked again on focus instead.

use std::io::{self, IsTerminal, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

use crate::config::ThemeMode;

/// The question: OSC 11 with `?`, ended by ST rather than BEL, so a terminal that
/// does not take OSC at all does not beep.
pub(crate) const QUERY: &[u8] = b"\x1b]11;?\x1b\\";

/// How long after asking any reply is taken off the input stream, an ESC read on its
/// own included. Past it, only a reply whose `ESC ]` arrived together is. Generous for
/// a slow SSH link; a terminal that answers does so in milliseconds.
pub(crate) const ARMED_FOR: Duration = Duration::from_secs(3);

/// How long the reader holds the start of what may be a reply for the rest of it.
/// A reply arrives in one write, so its keys are already queued when its first is
/// read; a real Alt+`]` is held this long at most.
pub(crate) const HOLD: Duration = Duration::from_millis(100);

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Milliseconds since [`epoch`] until which a reply is expected; 0 when none is.
static ARMED_UNTIL: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    u64::try_from(epoch().elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Whether a question is out and a reply may arrive.
pub(crate) fn armed() -> bool {
    now_ms() < ARMED_UNTIL.load(Ordering::SeqCst)
}

/// The reply arrived (or will not be read): keys are keys again.
pub(crate) fn disarm() {
    ARMED_UNTIL.store(0, Ordering::SeqCst);
}

/// Whether to ask: a Unix terminal on stdout. The Linux console prints unknown OSCs and
/// dumb terminals have none; Windows delivers replies as key events (an Esc among them),
/// so never there.
pub(crate) fn supported() -> bool {
    can_ask(
        cfg!(unix),
        io::stdout().is_terminal(),
        std::env::var("TERM").ok().as_deref(),
    )
}

fn can_ask(unix: bool, tty: bool, term: Option<&str>) -> bool {
    unix && tty && term.is_some_and(|term| !term.is_empty() && term != "linux" && term != "dumb")
}

/// Which terminal this is, as far as is cheap to tell: `TERM_PROGRAM` when set,
/// else `TERM`. Its last answer is remembered under this.
pub(crate) fn terminal_key() -> String {
    let var = |name| std::env::var(name).ok().filter(|v: &String| !v.is_empty());
    var("TERM_PROGRAM")
        .or_else(|| var("TERM"))
        .unwrap_or_default()
}

/// Ask the terminal for its background, unless a question is already out. Returns
/// whether it asked.
pub(crate) fn ask(out: &mut impl Write) -> bool {
    if armed() {
        return false;
    }
    if out.write_all(QUERY).and_then(|()| out.flush()).is_err() {
        return false;
    }
    let until = now_ms().saturating_add(u64::try_from(ARMED_FOR.as_millis()).unwrap_or(0));
    ARMED_UNTIL.store(until, Ordering::SeqCst);
    true
}

/// The color in an OSC 11 reply's body (`rgb:R/G/B` or `rgba:R/G/B/A`, one to four
/// hex digits a channel), each channel from 0 to 1.
pub fn parse_color(body: &str) -> Option<[f64; 3]> {
    let (channels, count) = match body.strip_prefix("rgb:") {
        Some(rest) => (rest, 3),
        None => (body.strip_prefix("rgba:")?, 4),
    };
    let parts = channels
        .split('/')
        .map(channel)
        .collect::<Option<Vec<f64>>>()?;
    match parts.as_slice() {
        [r, g, b, ..] if parts.len() == count => Some([*r, *g, *b]),
        _ => None,
    }
}

/// One channel: one to four hex digits, scaled to 0..=1 by its width.
fn channel(hex: &str) -> Option<f64> {
    if hex.is_empty() || hex.len() > 4 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let value = u32::from_str_radix(hex, 16).ok()?;
    let max = (1u32 << (4 * hex.len())) - 1;
    Some(f64::from(value) / f64::from(max))
}

/// Light or dark for a background color: light when black text would contrast with
/// it better than white, which is a relative luminance above about 0.179.
pub fn mode_for(rgb: [f64; 3]) -> ThemeMode {
    fn linear(c: f64) -> f64 {
        if c <= 0.04045 {
            c / 12.92
        } else {
            ((c + 0.055) / 1.055).powf(2.4)
        }
    }
    let luminance = 0.2126 * linear(rgb[0]) + 0.7152 * linear(rgb[1]) + 0.0722 * linear(rgb[2]);
    // Where (L + 0.05) / 0.05 equals 1.05 / (L + 0.05).
    if luminance > 0.179 {
        ThemeMode::Light
    } else {
        ThemeMode::Dark
    }
}

/// What the reader passes on: an event as read, or a reply taken off the stream
/// (`None` when its color could not be read).
#[derive(Debug, PartialEq)]
pub(crate) enum Scanned {
    Event(Event),
    Background(Option<ThemeMode>),
}

/// Takes an OSC 11 reply out of Crossterm's keys: Alt+`]`, the body's characters, then
/// Ctrl+G (or Alt+`\` for ST). An ESC split across reads arrives as Esc then characters,
/// so a lone Esc is held briefly while armed. Anything that stops looking like a reply
/// passes through in order.
#[derive(Debug, Default)]
pub(crate) struct ReplyScanner {
    held: Vec<Event>,
    /// The body after `ESC ]`, as far as it has come.
    text: String,
    /// The `ESC ]` (or Alt+`]`) has been seen.
    started: bool,
    /// An Esc inside the body: ST, if `\` follows.
    escaped: bool,
}

/// Longest body taken as a reply: `11;rgba:ffff/ffff/ffff/ffff` with room to spare.
const MAX_BODY: usize = 48;

impl ReplyScanner {
    /// Whether events are held waiting for the rest of a reply.
    pub(crate) fn holding(&self) -> bool {
        !self.held.is_empty()
    }

    /// Pass on whatever is held, as read.
    pub(crate) fn flush(&mut self, out: &mut Vec<Scanned>) {
        out.extend(self.held.drain(..).map(Scanned::Event));
        self.text.clear();
        self.started = false;
        self.escaped = false;
    }

    /// Take one event. `armed` says whether a reply is expected; when it is not, only a
    /// reply starting with Alt+`]` is looked for.
    pub(crate) fn feed(&mut self, event: Event, armed: bool, out: &mut Vec<Scanned>) {
        let key = match &event {
            Event::Key(key) if key.is_press() => *key,
            _ => {
                self.flush(out);
                out.push(Scanned::Event(event));
                return;
            }
        };
        if self.held.is_empty() {
            // A reply can come after the question lapsed (a slow link, tmux, mosh): one
            // whose `ESC ]` arrived together, as Alt+`]`, is still looked for.
            if (armed && (is_esc(&key) || is_open(&key, true))) || is_late_open(&key) {
                self.started = !is_esc(&key);
                self.held.push(event);
            } else {
                out.push(Scanned::Event(event));
            }
            return;
        }
        if !self.started {
            // An Esc is held: `]` makes it the reply's start.
            if is_open(&key, false) {
                self.started = true;
                self.held.push(event);
            } else {
                self.flush(out);
                self.feed(event, armed, out);
            }
            return;
        }
        if self.escaped {
            if key.code == KeyCode::Char('\\') && key.modifiers.is_empty() {
                self.finish(out, event);
            } else {
                self.flush(out);
                self.feed(event, armed, out);
            }
            return;
        }
        match key.code {
            KeyCode::Char('g') if key.modifiers == KeyModifiers::CONTROL => self.finish(out, event),
            KeyCode::Char('\\') if key.modifiers == KeyModifiers::ALT => self.finish(out, event),
            KeyCode::Esc if key.modifiers.is_empty() && self.text.starts_with("11;") => {
                self.escaped = true;
                self.held.push(event);
            }
            KeyCode::Char(c)
                if (key.modifiers - KeyModifiers::SHIFT).is_empty() && self.plausible_with(c) =>
            {
                self.text.push(c);
                self.held.push(event);
            }
            _ => {
                self.flush(out);
                self.feed(event, armed, out);
            }
        }
    }

    fn plausible_with(&self, c: char) -> bool {
        let len = self.text.len() + c.len_utf8();
        if len <= 3 {
            let mut text = self.text.clone();
            text.push(c);
            return "11;".starts_with(&text);
        }
        len <= MAX_BODY && (c.is_ascii_alphanumeric() || c == ':' || c == '/')
    }

    /// The terminator arrived: a reply if the body is one, otherwise everything is
    /// passed on.
    fn finish(&mut self, out: &mut Vec<Scanned>, terminator: Event) {
        match self.text.strip_prefix("11;") {
            Some(body) => {
                out.push(Scanned::Background(parse_color(body).map(mode_for)));
                self.held.clear();
                self.flush(out);
            }
            None => {
                self.flush(out);
                out.push(Scanned::Event(terminator));
            }
        }
    }
}

fn is_esc(key: &KeyEvent) -> bool {
    key.code == KeyCode::Esc && key.modifiers.is_empty()
}

/// Alt+`]`: a reply's `ESC ]` read together, looked for even when no question is
/// out. A typed Alt+`]` is held [`HOLD`] at most.
fn is_late_open(key: &KeyEvent) -> bool {
    key.code == KeyCode::Char(']') && key.modifiers == KeyModifiers::ALT
}

/// `]`: with Alt when `ESC ]` arrived together (or plain, when `alt_ok` allows it),
/// plain when the Esc came on its own.
fn is_open(key: &KeyEvent, alt_ok: bool) -> bool {
    key.code == KeyCode::Char(']')
        && (key.modifiers.is_empty() || (alt_ok && key.modifiers == KeyModifiers::ALT))
}

#[cfg(test)]
mod tests;

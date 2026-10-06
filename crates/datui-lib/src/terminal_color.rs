//! Asking the terminal for its background color, so `theme.mode = "auto"` can pick
//! the light or dark palette from what is actually on screen.
//!
//! The question is OSC 11 (`ESC ] 11 ; ? ST`); a terminal that knows it answers
//! `ESC ] 11 ; rgb:RRRR/GGGG/BBBB` followed by BEL or ST. The answer comes back on
//! the input stream, which Crossterm alone reads ([`crate::terminal_input`]).
//! Crossterm has no event for it: it reads `ESC ]` as Alt+`]` and the rest as typed
//! characters. So the reader watches for that run of keys while a question is out
//! ([`armed`]) and takes it off the stream ([`ReplyScanner`]) instead of letting it
//! reach the app as keystrokes.
//!
//! Nothing here waits on the terminal, startup included: the first frame is drawn
//! in the palette this terminal's last answer picked (kept in the cache under
//! [`terminal_key`]), and the answer, when it comes, switches palettes if it
//! differs. A terminal that does not answer sends nothing, and the question lapses
//! after [`ARMED_FOR`].
//!
//! DEC mode 2031 (the terminal reporting a scheme change as `CSI ? 997 ; n`) is not
//! enabled: Crossterm 0.29 takes any `CSI ?` sequence that ends in neither `u` nor
//! `c` as unfinished and keeps buffering, so one report would swallow every key
//! after it. The question is asked again when the terminal regains focus instead,
//! which is when a scheme changed elsewhere is first seen.

use std::io::{self, IsTerminal, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

use crate::config::ThemeMode;

/// The question: OSC 11 with `?`, ended by ST rather than BEL, so a terminal that
/// does not take OSC at all does not beep.
pub(crate) const QUERY: &[u8] = b"\x1b]11;?\x1b\\";

/// How long after asking a reply is still taken off the input stream. Past it, keys
/// that look like a reply are keys. Generous for a slow SSH link; a terminal that
/// answers does so in milliseconds.
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

/// Whether asking makes sense here: a Unix terminal on standard output. The Linux
/// console prints an OSC it does not know, and a dumb terminal has no OSC. On
/// Windows the console delivers a reply as key events, an Esc among them, which
/// would reach the app as a keypress, so the question is never asked there.
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

/// Takes an OSC 11 reply out of the keys Crossterm made of it.
///
/// Crossterm reads `ESC ] 11 ; rgb:… BEL` as Alt+`]`, the body's characters, then
/// Ctrl+G; with ST as the terminator, Alt+`\`. An ESC at the end of one read and the
/// rest in the next comes as Esc then plain characters, so while armed a lone Esc is
/// held a moment too. Anything that stops looking like a reply is passed on as it
/// was read, in order.
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

    /// Take one event. `armed` says whether a reply is expected; when it is not, a
    /// new reply is not looked for.
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
            if armed && (is_esc(&key) || is_open(&key, true)) {
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

/// `]`: with Alt when `ESC ]` arrived together (or plain, when `alt_ok` allows it),
/// plain when the Esc came on its own.
fn is_open(key: &KeyEvent, alt_ok: bool) -> bool {
    key.code == KeyCode::Char(']')
        && (key.modifiers.is_empty() || (alt_ok && key.modifiers == KeyModifiers::ALT))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyEventKind;

    /// The events Crossterm 0.29 makes of `bytes` read in one go: `ESC x` is Alt+x,
    /// a lone trailing ESC is Esc, 0x01-0x1A are Ctrl+letter, the rest characters
    /// (uppercase with Shift).
    fn crossterm_events(bytes: &[u8]) -> Vec<Event> {
        let plain = |c: char| {
            let mods = if c.is_uppercase() {
                KeyModifiers::SHIFT
            } else {
                KeyModifiers::NONE
            };
            KeyEvent::new(KeyCode::Char(c), mods)
        };
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            let key = if b == 0x1b {
                match bytes.get(i + 1) {
                    Some(&next) => {
                        i += 1;
                        let mut key = plain(next as char);
                        key.modifiers |= KeyModifiers::ALT;
                        key
                    }
                    None => KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
                }
            } else if (0x01..=0x1a).contains(&b) {
                KeyEvent::new(KeyCode::Char((b - 1 + b'a') as char), KeyModifiers::CONTROL)
            } else {
                plain(b as char)
            };
            out.push(Event::Key(key));
            i += 1;
        }
        out
    }

    fn scan(events: Vec<Event>, armed: bool) -> Vec<Scanned> {
        let mut scanner = ReplyScanner::default();
        let mut out = Vec::new();
        for event in events {
            scanner.feed(event, armed, &mut out);
        }
        scanner.flush(&mut out);
        out
    }

    fn keys(bytes: &[u8]) -> Vec<Scanned> {
        crossterm_events(bytes)
            .into_iter()
            .map(Scanned::Event)
            .collect()
    }

    #[test]
    fn colors_parse_at_every_digit_width() {
        assert_eq!(parse_color("rgb:ffff/ffff/ffff"), Some([1.0, 1.0, 1.0]));
        assert_eq!(parse_color("rgb:0000/0000/0000"), Some([0.0, 0.0, 0.0]));
        assert_eq!(parse_color("rgb:f/0/f"), Some([1.0, 0.0, 1.0]));
        assert_eq!(parse_color("rgb:80/80/80"), Some([128.0 / 255.0; 3]));
        assert_eq!(parse_color("rgb:800/800/800"), Some([2048.0 / 4095.0; 3]));
        assert_eq!(
            parse_color("rgb:FFFF/0/80"),
            Some([1.0, 0.0, 128.0 / 255.0])
        );
        assert_eq!(parse_color("rgba:ffff/ffff/ffff/ffff"), Some([1.0; 3]));
    }

    #[test]
    fn garbage_is_no_color() {
        for body in [
            "",
            "?",
            "rgb:",
            "rgb:ffff/ffff",
            "rgb:ffff/ffff/ffff/ffff",
            "rgb:fffff/0/0",
            "rgb:gg/00/00",
            "rgb://",
            "rgba:ffff/ffff/ffff",
            "#ffffff",
            "cmy:1/1/1",
        ] {
            assert_eq!(parse_color(body), None, "{body:?}");
        }
    }

    #[test]
    fn luminance_picks_the_mode() {
        let hex = |s: &str| parse_color(&format!("rgb:{s}")).unwrap();
        for light in ["ff/ff/ff", "fd/f6/e3", "fb/f1/c7", "ee/ee/ee", "80/80/80"] {
            assert_eq!(mode_for(hex(light)), ThemeMode::Light, "{light}");
        }
        for dark in ["00/00/00", "00/2b/36", "1a/1b/26", "28/28/28", "60/60/60"] {
            assert_eq!(mode_for(hex(dark)), ThemeMode::Dark, "{dark}");
        }
    }

    #[test]
    fn a_reply_ended_by_bel_or_st_is_taken_off_the_stream() {
        for reply in [
            &b"\x1b]11;rgb:ffff/ffff/ffff\x07"[..],
            b"\x1b]11;rgb:ffff/ffff/ffff\x1b\\",
            b"\x1b]11;rgb:FF/FF/FF\x07",
        ] {
            assert_eq!(
                scan(crossterm_events(reply), true),
                vec![Scanned::Background(Some(ThemeMode::Light))],
                "{reply:?}"
            );
        }
        assert_eq!(
            scan(crossterm_events(b"\x1b]11;rgb:1a1b/1c1c/2626\x07"), true),
            vec![Scanned::Background(Some(ThemeMode::Dark))]
        );
    }

    /// Keys typed before and after a reply pass through, in order; a reply whose
    /// color cannot be read is still not keys.
    #[test]
    fn keys_around_a_reply_pass_through() {
        let mut bytes = b"jk".to_vec();
        bytes.extend_from_slice(b"\x1b]11;rgb:0/0/0\x07");
        bytes.extend_from_slice(b"q");
        let mut want = keys(b"jk");
        want.push(Scanned::Background(Some(ThemeMode::Dark)));
        want.extend(keys(b"q"));
        assert_eq!(scan(crossterm_events(&bytes), true), want);

        assert_eq!(
            scan(crossterm_events(b"\x1b]11;cmy:0/0/0\x07"), true),
            vec![Scanned::Background(None)]
        );
    }

    /// An ESC read on its own, then the rest: Esc, `]`, the body, then Esc and `\`
    /// for ST.
    #[test]
    fn a_reply_split_after_its_esc_is_taken_too() {
        let mut events = vec![Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))];
        events.extend(
            crossterm_events(b"]11;rgb:ffff/ffff/ffff")
                .into_iter()
                .chain([
                    Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
                    Event::Key(KeyEvent::new(KeyCode::Char('\\'), KeyModifiers::NONE)),
                ]),
        );
        assert_eq!(
            scan(events, true),
            vec![Scanned::Background(Some(ThemeMode::Light))]
        );
    }

    /// Unarmed, nothing is taken: the reply's keys reach the app as read.
    #[test]
    fn nothing_is_taken_when_no_question_is_out() {
        let reply = b"\x1b]11;rgb:ffff/ffff/ffff\x07";
        assert_eq!(scan(crossterm_events(reply), false), keys(reply));
    }

    /// Armed, keys that only start like a reply are passed on unchanged, in order:
    /// `]` (sort), Esc, Alt+`]` then other text, a body that turns into garbage, and
    /// a body cut off without its terminator.
    #[test]
    fn near_misses_are_passed_on_as_typed() {
        for typed in [
            &b"]"[..],
            b"]j",
            b"\x1bj",
            b"\x1b]12;rgb:0/0/0\x07",
            b"\x1b]11;rgb:0/0/0 x\x07",
            b"\x1b]11;rgb:0/0",
            b"\x1b]1q",
        ] {
            assert_eq!(
                scan(crossterm_events(typed), true),
                keys(typed),
                "{typed:?}"
            );
        }
        // A lone Esc, as Crossterm reads it at the end of input.
        let esc = vec![Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))];
        assert_eq!(
            scan(esc.clone(), true),
            esc.into_iter().map(Scanned::Event).collect::<Vec<_>>()
        );
    }

    /// A non-key event or a release ends what was held: both pass through in order.
    #[test]
    fn other_events_flush_what_is_held() {
        let mut release = KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        let mut events = crossterm_events(b"\x1b]1");
        events.push(Event::Key(release));
        events.push(Event::Resize(80, 24));
        let want: Vec<_> = events.iter().cloned().map(Scanned::Event).collect();
        assert_eq!(scan(events, true), want);
    }

    /// Only a Unix terminal on standard output that takes OSC is asked: never when
    /// standard output is a pipe or a file.
    #[test]
    fn only_a_terminal_is_asked() {
        assert!(can_ask(true, true, Some("xterm-256color")));
        assert!(!can_ask(true, false, Some("xterm-256color")), "not a tty");
        assert!(!can_ask(false, true, Some("xterm-256color")), "Windows");
        for term in [None, Some(""), Some("linux"), Some("dumb")] {
            assert!(!can_ask(true, true, term), "{term:?}");
        }
    }

    #[test]
    fn the_question_ends_with_st() {
        assert_eq!(QUERY, b"\x1b]11;?\x1b\\");
    }
}

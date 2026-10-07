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

//! Terminal escape sequence injection.
//!
//! datui renders untrusted text by definition: cell values, column names and
//! filenames all come from whatever the user opened. If any of that reaches the
//! terminal without being neutralised, a hostile dataset stops being data and
//! starts being commands. Depending on the terminal that means a spoofed
//! interface, a clipboard write via OSC 52, a window title change, or worse.
//!
//! This is the classic vulnerability class for anything that displays text it
//! did not write, so it gets a test rather than an assumption.
//!
//! Everything asserted here goes through the real render path: load a file,
//! render the App into a ratatui Buffer, and inspect the symbols that would be
//! written to the terminal.

use datui::{App, AppEvent, OpenOptions};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

/// Sequences worth trying, and what each would do if it escaped.
///
/// The names matter more than the bytes: a future reader should be able to see
/// what capability each one is reaching for.
const PAYLOADS: &[(&str, &str)] = &[
    ("CSI clear screen", "\x1b[2J"),
    ("CSI cursor home", "\x1b[H"),
    ("CSI red text", "\x1b[31m"),
    ("OSC 52 clipboard write", "\x1b]52;c;aGVsbG8=\x07"),
    ("OSC 0 window title", "\x1b]0;pwned\x07"),
    (
        "OSC 8 hyperlink",
        "\x1b]8;;http://evil.example\x07link\x1b]8;;\x07",
    ),
    ("DCS passthrough", "\x1bPq\x1b\\"),
    ("bare ESC", "\x1b"),
    ("BEL", "\x07"),
    ("carriage return", "line1\rline2"),
    ("backspace overwrite", "safe\x08\x08\x08\x08evil"),
    ("C1 CSI single byte", "\u{009b}31m"),
];

fn pump_open_until_loaded(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        if let Some(ev) = next.take() {
            if matches!(ev, AppEvent::Crash(_)) {
                app.event(&ev);
                return;
            }
            next = app.event(&ev);
        } else {
            match rx.recv_timeout(std::time::Duration::from_millis(2000)) {
                Ok(ev) => next = Some(ev),
                Err(_) => return,
            }
        }
    }
}

/// Every symbol the buffer would emit, concatenated.
fn rendered_text(buf: &Buffer) -> String {
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// The characters that must never reach the terminal from untrusted text.
///
/// C0 controls other than tab, plus DEL and the C1 range. Tab is excluded
/// because it is layout, not control, and ratatui may legitimately place one.
fn offending_chars(s: &str) -> Vec<char> {
    s.chars()
        .filter(|c| {
            let n = *c as u32;
            (n < 0x20 && *c != '\t') || n == 0x7f || (0x80..=0x9f).contains(&n)
        })
        .collect()
}

fn write_csv(name: &str, contents: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("datui-escape-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write fixture");
    path
}

fn render_file(path: PathBuf, width: u16) -> String {
    common::isolate_cache();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    let area = Rect::new(0, 0, width, 40);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    rendered_text(&buf)
}

/// CSV-quote a field, so the parser hands the payload through as one value.
fn quoted(payload: &str) -> String {
    format!("\"{}\"", payload.replace('"', "\"\""))
}

#[test]
fn cell_values_cannot_emit_escape_sequences() {
    // One row per payload, one load. Also closer to a real hostile file, which
    // would not politely contain a single bad cell.
    let mut csv = String::from("id,note\n");
    for (i, (_, payload)) in PAYLOADS.iter().enumerate() {
        csv.push_str(&format!("{},{}\n", i, quoted(payload)));
    }
    let path = write_csv("cells.csv", &csv);

    let text = render_file(path, 120);
    let offenders = offending_chars(&text);
    assert!(
        offenders.is_empty(),
        "control chars {:?} reached the terminal from cell values; payloads were {:?}",
        offenders,
        PAYLOADS.iter().map(|(n, _)| *n).collect::<Vec<_>>()
    );
}

#[test]
fn column_names_cannot_emit_escape_sequences() {
    // Headers take a different path to the screen than cell values: they are
    // drawn as table headers and reused in modals and menus. A wide viewport so
    // that every column is actually on screen and therefore actually rendered.
    let headers: Vec<String> = PAYLOADS.iter().map(|(_, p)| quoted(p)).collect();
    let values: Vec<&str> = PAYLOADS.iter().map(|_| "x").collect();
    let csv = format!("{}\n{}\n", headers.join(","), values.join(","));
    let path = write_csv("headers.csv", &csv);

    let text = render_file(path, 400);
    let offenders = offending_chars(&text);
    assert!(
        offenders.is_empty(),
        "control chars {:?} reached the terminal from column names",
        offenders
    );
}

#[test]
fn filenames_cannot_emit_escape_sequences() {
    // The filename is shown in the header bar, and lands in the recent-files
    // list on the home screen. It is attacker-influenced whenever someone opens
    // a file they were sent.
    let path = write_csv("na\x1b[31mme.csv", "id,note\n1,x\n");

    let text = render_file(path, 120);
    let offenders = offending_chars(&text);
    assert!(
        offenders.is_empty(),
        "an escape sequence in a filename survived rendering as {:?}",
        offenders
    );
}

#[test]
fn error_messages_cannot_emit_escape_sequences() {
    // Error paths are where raw untrusted strings usually leak through, since
    // they tend to interpolate a filename or a parser message directly.
    let dir = std::env::temp_dir().join(format!("datui-escape-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let missing = dir.join("no\x1b]0;pwned\x07such.csv");

    let text = render_file(missing, 120);
    let offenders = offending_chars(&text);
    assert!(
        offenders.is_empty(),
        "an escape sequence in an error message survived rendering as {:?}",
        offenders
    );
}

#[test]
fn the_detector_would_catch_a_real_escape() {
    // Guards the guard. If offending_chars ever stopped matching, every test
    // above would pass vacuously.
    assert!(!offending_chars("\x1b[2J").is_empty());
    assert!(!offending_chars("\x07").is_empty());
    assert!(!offending_chars("\u{009b}").is_empty());
    assert!(offending_chars("ordinary text\tand a tab").is_empty());
}

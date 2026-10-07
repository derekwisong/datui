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

use super::*;

impl Help {
    fn open_for_test(&mut self, context: Context) {
        self.open(context, false);
    }
}

/// Under a text field, a plain character does not run: it would type. A chord
/// still does.
#[test]
fn a_text_field_keeps_its_characters() {
    let mut help = Help::default();
    help.open(Context::Find, true);
    press(&mut help, KeyCode::Char('/'));
    type_text(&mut help, "regex on");
    assert_eq!(
        press(&mut help, KeyCode::Enter),
        HelpKey::Press(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL))
    );
    help.open(Context::Export, true);
    let space = keys::screen(Context::Export).groups[0]
        .keys
        .iter()
        .find(|k| k.keys == "Space")
        .unwrap();
    assert_eq!(help.runnable(space), None);
    help.open(Context::Export, false);
    assert!(help.runnable(space).is_some());
}

fn press(help: &mut Help, code: KeyCode) -> HelpKey {
    help.key(&KeyEvent::new(code, KeyModifiers::NONE))
}

fn type_text(help: &mut Help, text: &str) {
    for c in text.chars() {
        press(help, KeyCode::Char(c));
    }
}

#[test]
fn every_screen_ends_with_the_keys_of_every_screen() {
    for screen in keys::SCREENS {
        let mut help = Help::default();
        help.open_for_test(screen.context);
        let blocks = help.blocks();
        assert_eq!(blocks.last().map(|b| b.name), Some(keys::GLOBAL.name));
        assert!(blocks.len() > 1, "{}", screen.title);
    }
}

#[test]
fn the_query_screen_carries_the_q_summary() {
    let mut help = Help::default();
    help.open_for_test(Context::Query);
    assert!(help.blocks().iter().any(|b| b.name == "q syntax"));
    help.open_for_test(Context::Table);
    assert!(!help.blocks().iter().any(|b| b.name == "q syntax"));
}

#[test]
fn slash_narrows_by_key_and_description() {
    let mut help = Help::default();
    help.open_for_test(Context::Table);
    let all = help.shown_keys().len();
    press(&mut help, KeyCode::Char('/'));
    type_text(&mut help, "value counts");
    let shown = help.shown_keys();
    assert_eq!(shown.len(), 1, "{shown:?}");
    assert_eq!(shown[0].keys, "F");
    // Backspace takes back a character.
    press(&mut help, KeyCode::Backspace);
    type_text(&mut help, "s");
    assert_eq!(help.filter, "value counts");
    // Esc clears the filter, then closes.
    assert_eq!(press(&mut help, KeyCode::Esc), HelpKey::Stay);
    assert_eq!(help.shown_keys().len(), all);
    assert!(help.is_open());
    assert_eq!(press(&mut help, KeyCode::Esc), HelpKey::Closed);
    assert!(!help.is_open());
}

#[test]
fn a_filter_that_matches_nothing_shows_nothing() {
    let mut help = Help::default();
    help.open_for_test(Context::Table);
    press(&mut help, KeyCode::Char('/'));
    type_text(&mut help, "zzzzzz");
    assert!(help.blocks().is_empty());
    assert_eq!(press(&mut help, KeyCode::Enter), HelpKey::Stay);
    assert!(help.is_open());
}

#[test]
fn enter_closes_and_presses_the_selected_key() {
    let mut help = Help::default();
    help.open_for_test(Context::Table);
    press(&mut help, KeyCode::Char('/'));
    type_text(&mut help, "value counts");
    let pressed = press(&mut help, KeyCode::Enter);
    assert_eq!(
        pressed,
        HelpKey::Press(KeyEvent::new(KeyCode::Char('F'), KeyModifiers::SHIFT))
    );
    assert!(!help.is_open());
}

#[test]
fn arrows_walk_the_keys_and_stop_at_the_ends() {
    let mut help = Help::default();
    help.open_for_test(Context::Query);
    let count = help.shown_keys().len();
    press(&mut help, KeyCode::Up);
    assert_eq!(help.selected, 0);
    press(&mut help, KeyCode::Char('j'));
    assert_eq!(help.selected, 1);
    press(&mut help, KeyCode::End);
    assert_eq!(help.selected, count - 1);
    press(&mut help, KeyCode::Down);
    assert_eq!(help.selected, count - 1);
    press(&mut help, KeyCode::Home);
    assert_eq!(help.selected, 0);
}

/// A line with nothing to press (typing, the mouse) keeps the help open.
#[test]
fn enter_on_a_line_with_no_key_stays() {
    let mut help = Help::default();
    help.open_for_test(Context::Query);
    assert_eq!(help.selected_key().map(|k| k.keys), Some("(digits)"));
    assert_eq!(press(&mut help, KeyCode::Enter), HelpKey::Stay);
    assert!(help.is_open());
}

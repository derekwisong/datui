use super::*;

fn key(code: KeyCode, modifiers: KeyModifiers) -> Input {
    Input::from(KeyEvent::new(code, modifiers))
}

#[test]
fn plain_characters_insert_themselves() {
    assert_eq!(
        action_for(key(KeyCode::Char('a'), KeyModifiers::NONE)),
        Action::Insert('a')
    );
    assert_eq!(
        action_for(key(KeyCode::Char('A'), KeyModifiers::SHIFT)),
        Action::Insert('A')
    );
}

#[test]
fn readline_bindings_match_their_arrow_key_equivalents() {
    let pairs = [
        (
            key(KeyCode::Char('a'), KeyModifiers::CONTROL),
            key(KeyCode::Home, KeyModifiers::NONE),
        ),
        (
            key(KeyCode::Char('e'), KeyModifiers::CONTROL),
            key(KeyCode::End, KeyModifiers::NONE),
        ),
        (
            key(KeyCode::Char('f'), KeyModifiers::CONTROL),
            key(KeyCode::Right, KeyModifiers::NONE),
        ),
        (
            key(KeyCode::Char('b'), KeyModifiers::CONTROL),
            key(KeyCode::Left, KeyModifiers::NONE),
        ),
        (
            key(KeyCode::Char('h'), KeyModifiers::CONTROL),
            key(KeyCode::Backspace, KeyModifiers::NONE),
        ),
        (
            key(KeyCode::Char('d'), KeyModifiers::CONTROL),
            key(KeyCode::Delete, KeyModifiers::NONE),
        ),
    ];
    for (emacs, arrow) in pairs {
        assert_eq!(
            action_for(emacs),
            action_for(arrow),
            "{emacs:?} vs {arrow:?}"
        );
    }
}

#[test]
fn ctrl_u_kills_to_line_start_and_ctrl_z_undoes() {
    assert_eq!(
        action_for(key(KeyCode::Char('u'), KeyModifiers::CONTROL)),
        Action::DeleteToLineStart
    );
    assert_eq!(
        action_for(key(KeyCode::Char('z'), KeyModifiers::CONTROL)),
        Action::Undo
    );
}

#[test]
fn ctrl_j_never_edits() {
    assert_eq!(
        action_for(key(KeyCode::Char('j'), KeyModifiers::CONTROL)),
        Action::Nop
    );
}

#[test]
fn shifted_movement_extends_the_selection() {
    assert_eq!(
        action_for(key(KeyCode::Right, KeyModifiers::SHIFT)),
        Action::Move(CursorMove::Forward, true)
    );
    assert_eq!(
        action_for(key(KeyCode::Right, KeyModifiers::NONE)),
        Action::Move(CursorMove::Forward, false)
    );
}

#[test]
fn ctrl_arrows_move_by_word() {
    assert_eq!(
        action_for(key(KeyCode::Right, KeyModifiers::CONTROL)),
        Action::Move(CursorMove::WordForward, false)
    );
    assert_eq!(
        action_for(key(KeyCode::Left, KeyModifiers::CONTROL)),
        Action::Move(CursorMove::WordBack, false)
    );
}

#[test]
fn unmapped_keys_do_nothing() {
    assert_eq!(
        action_for(key(KeyCode::Esc, KeyModifiers::NONE)),
        Action::Nop
    );
    assert_eq!(
        action_for(key(KeyCode::F(5), KeyModifiers::NONE)),
        Action::Nop
    );
    assert_eq!(
        action_for(key(KeyCode::BackTab, KeyModifiers::SHIFT)),
        Action::Nop
    );
}

#[test]
fn key_events_convert_with_their_modifiers() {
    let input = key(
        KeyCode::Char('k'),
        KeyModifiers::CONTROL | KeyModifiers::ALT,
    );
    assert_eq!(input.key, Key::Char('k'));
    assert!(input.ctrl);
    assert!(input.alt);
    assert!(!input.shift);
}

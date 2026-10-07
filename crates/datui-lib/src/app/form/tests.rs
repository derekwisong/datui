use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum F {
    Name,
    Notes,
    Format,
    Header,
    Column,
    Tags,
    Go,
}

struct T {
    focus: F,
    show_header: bool,
    edge: (bool, bool),
}

impl Form for T {
    type Field = F;
    fn fields(&self) -> Vec<(F, FieldKind)> {
        let mut fields = vec![
            (F::Name, FieldKind::Text),
            (F::Notes, FieldKind::MultilineText),
            (F::Format, FieldKind::Choice),
        ];
        if self.show_header {
            fields.push((F::Header, FieldKind::Checkbox));
        }
        fields.extend([
            (F::Column, FieldKind::Picker { multi: false }),
            (F::Tags, FieldKind::Picker { multi: true }),
            (F::Go, FieldKind::Button),
        ]);
        fields
    }
    fn focused(&self) -> F {
        self.focus
    }
    fn set_focused(&mut self, field: F) {
        self.focus = field;
    }
    fn text_edge(&self, _: F) -> (bool, bool) {
        self.edge
    }
}

fn form() -> T {
    T {
        focus: F::Name,
        show_header: true,
        edge: (true, true),
    }
}

fn press(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn ctrl(c: char) -> KeyEvent {
    KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
}

#[test]
fn tab_and_the_arrows_walk_every_field_and_wrap() {
    let mut f = form();
    let mut walked = vec![f.focus];
    for _ in 0..7 {
        assert_eq!(key(&mut f, &press(KeyCode::Down)), FormKey::Moved);
        walked.push(f.focus);
    }
    assert_eq!(
        walked,
        [
            F::Name,
            F::Notes,
            F::Format,
            F::Header,
            F::Column,
            F::Tags,
            F::Go,
            F::Name
        ]
    );
    key(&mut f, &press(KeyCode::Up));
    assert_eq!(f.focus, F::Go, "↑ from the first field wraps to the last");
    key(&mut f, &press(KeyCode::Tab));
    assert_eq!(f.focus, F::Name);
    key(&mut f, &press(KeyCode::BackTab));
    assert_eq!(f.focus, F::Go);
    key(&mut f, &KeyEvent::new(KeyCode::Tab, KeyModifiers::SHIFT));
    assert_eq!(
        f.focus,
        F::Tags,
        "Shift+Tab as Tab with Shift goes back too"
    );
}

#[test]
fn hidden_fields_are_skipped_and_a_hidden_focus_settles() {
    let mut f = form();
    f.show_header = false;
    f.focus = F::Format;
    key(&mut f, &press(KeyCode::Down));
    assert_eq!(f.focus, F::Column);
    f.focus = F::Header;
    f.settle_focus();
    assert_eq!(f.focus, F::Name);
    assert!(!f.focus(F::Header), "a hidden field cannot take focus");
    assert!(f.focus(F::Tags));
    assert_eq!(f.focus, F::Tags);
}

#[test]
fn the_arrows_across_step_choices_and_edit_text() {
    let mut f = form();
    assert_eq!(
        key(&mut f, &press(KeyCode::Left)),
        FormKey::Text(F::Name),
        "a text field keeps ← for its cursor"
    );
    f.focus = F::Format;
    assert_eq!(
        key(&mut f, &press(KeyCode::Left)),
        FormKey::Step(F::Format, -1)
    );
    assert_eq!(
        key(&mut f, &press(KeyCode::Right)),
        FormKey::Step(F::Format, 1)
    );
    f.focus = F::Column;
    assert_eq!(
        key(&mut f, &press(KeyCode::Right)),
        FormKey::Step(F::Column, 1)
    );
    f.focus = F::Tags;
    assert_eq!(key(&mut f, &press(KeyCode::Right)), FormKey::Other);
    f.focus = F::Header;
    assert_eq!(key(&mut f, &press(KeyCode::Right)), FormKey::Act(F::Header));
}

#[test]
fn space_acts_on_the_field() {
    let mut f = form();
    let space = press(KeyCode::Char(' '));
    assert_eq!(key(&mut f, &space), FormKey::Text(F::Name), "text types it");
    f.focus = F::Format;
    assert_eq!(key(&mut f, &space), FormKey::Step(F::Format, 1));
    for field in [F::Header, F::Column, F::Tags, F::Go] {
        f.focus = field;
        assert_eq!(key(&mut f, &space), FormKey::Act(field));
    }
}

#[test]
fn enter_submits_from_any_field_but_types_in_a_multiline_one() {
    let mut f = form();
    for field in [F::Name, F::Format, F::Header, F::Column, F::Tags, F::Go] {
        f.focus = field;
        assert_eq!(key(&mut f, &press(KeyCode::Enter)), FormKey::Submit);
    }
    f.focus = F::Notes;
    assert_eq!(key(&mut f, &press(KeyCode::Enter)), FormKey::Text(F::Notes));
    assert_eq!(key(&mut f, &ctrl('j')), FormKey::Submit);
    assert_eq!(
        key(
            &mut f,
            &KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL)
        ),
        FormKey::Submit
    );
}

#[test]
fn esc_cancels_from_any_field() {
    let mut f = form();
    for field in [F::Name, F::Notes, F::Format, F::Go] {
        f.focus = field;
        assert_eq!(key(&mut f, &press(KeyCode::Esc)), FormKey::Cancel);
    }
}

#[test]
fn history_is_ctrl_p_and_n_and_the_arrows_never_recall() {
    let mut f = form();
    assert_eq!(key(&mut f, &ctrl('p')), FormKey::Text(F::Name));
    assert_eq!(key(&mut f, &ctrl('n')), FormKey::Text(F::Name));
    assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Moved);
    assert_eq!(f.focus, F::Go);
}

#[test]
fn a_multiline_field_keeps_the_arrows_until_its_edge() {
    let mut f = form();
    f.focus = F::Notes;
    f.edge = (false, false);
    assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Text(F::Notes));
    assert_eq!(key(&mut f, &press(KeyCode::Down)), FormKey::Text(F::Notes));
    f.edge = (true, false);
    assert_eq!(key(&mut f, &press(KeyCode::Up)), FormKey::Moved);
    assert_eq!(f.focus, F::Name);
}

#[test]
fn vim_keys_move_only_where_nothing_types() {
    let mut f = form();
    assert_eq!(
        key(&mut f, &press(KeyCode::Char('j'))),
        FormKey::Text(F::Name)
    );
    f.focus = F::Format;
    assert_eq!(
        key(&mut f, &press(KeyCode::Char('l'))),
        FormKey::Step(F::Format, 1)
    );
    key(&mut f, &press(KeyCode::Char('j')));
    assert_eq!(f.focus, F::Header);
    key(&mut f, &press(KeyCode::Char('k')));
    assert_eq!(f.focus, F::Format);
    assert_eq!(key(&mut f, &press(KeyCode::Char('x'))), FormKey::Other);
}

#[test]
fn the_picker_narrows_moves_and_never_types_a_space() {
    let mut p = PickerState::new(vec!["alpha".into(), "beta".into(), "gamma".into()]);
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Down)),
        PickerKey::Handled
    );
    assert_eq!(p.selected_original(), Some(1));
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Char('g'))),
        PickerKey::Handled
    );
    assert_eq!(p.filter, "g");
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Char(' '))),
        PickerKey::Choose
    );
    assert_eq!(
        picker_key(&mut p, true, &press(KeyCode::Char(' '))),
        PickerKey::Toggle
    );
    assert_eq!(p.filter, "g", "a space never narrows");
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Enter)),
        PickerKey::Choose
    );
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Esc)),
        PickerKey::Close
    );
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::Tab)),
        PickerKey::ChooseAndMove(true)
    );
    assert_eq!(
        picker_key(&mut p, false, &press(KeyCode::BackTab)),
        PickerKey::ChooseAndMove(false)
    );
}

#[test]
fn steps_wrap_both_ways() {
    assert_eq!(step_index(0, 3, -1), 2);
    assert_eq!(step_index(2, 3, 1), 0);
    assert_eq!(step_value(&['a', 'b', 'c'], 'b', 1), 'c');
    assert_eq!(step_value(&['a', 'b', 'c'], 'z', 1), 'a');
}

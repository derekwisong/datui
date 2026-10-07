use super::*;

#[test]
fn a_chip_presses_the_key_it_names() {
    let key = |code, modifiers| Some(KeyEvent::new(code, modifiers));
    assert_eq!(chip_key("Enter"), key(KeyCode::Enter, KeyModifiers::NONE));
    assert_eq!(
        chip_key("^O"),
        key(KeyCode::Char('o'), KeyModifiers::CONTROL)
    );
    assert_eq!(chip_key("q"), key(KeyCode::Char('q'), KeyModifiers::NONE));
    assert_eq!(chip_key("Q"), key(KeyCode::Char('Q'), KeyModifiers::SHIFT));
    assert_eq!(chip_key("/"), key(KeyCode::Char('/'), KeyModifiers::NONE));
    assert_eq!(chip_key("F1"), key(KeyCode::F(1), KeyModifiers::NONE));
    // Two keys, or a glyph standing for a pair: nothing to press.
    assert_eq!(chip_key("↑↓"), None);
    assert_eq!(chip_key("h/l"), None);
    assert_eq!(chip_key("PgUp"), None);
}

#[test]
fn a_second_click_on_the_same_cell_soon_after_is_a_double_click() {
    let mut p = Pointing::default();
    let at = Position { x: 4, y: 7 };
    let t = Instant::now();
    assert!(!p.second_click(at, t));
    assert!(p.second_click(at, t + Duration::from_millis(200)));
    // A third starts over.
    assert!(!p.second_click(at, t + Duration::from_millis(300)));
    // Too late, or somewhere else.
    assert!(!p.second_click(at, t + Duration::from_secs(2)));
    assert!(!p.second_click(Position { x: 5, y: 7 }, t + Duration::from_secs(2)));
}

#[test]
fn presses_drags_releases_a_right_press_and_the_wheel_are_wanted() {
    let mouse = |kind| MouseEvent {
        kind,
        column: 0,
        row: 0,
        modifiers: KeyModifiers::NONE,
    };
    assert!(wanted(&mouse(MouseEventKind::Down(MouseButton::Left))));
    assert!(wanted(&mouse(MouseEventKind::ScrollUp)));
    assert!(wanted(&mouse(MouseEventKind::ScrollRight)));
    assert!(wanted(&mouse(MouseEventKind::Down(MouseButton::Right))));
    assert!(wanted(&mouse(MouseEventKind::Up(MouseButton::Left))));
    assert!(wanted(&mouse(MouseEventKind::Drag(MouseButton::Left))));
    assert!(!wanted(&mouse(MouseEventKind::Moved)));
    assert!(!wanted(&mouse(MouseEventKind::Down(MouseButton::Middle))));
    assert!(!wanted(&mouse(MouseEventKind::Drag(MouseButton::Right))));
}

/// With `display.mouse` off nothing asks the terminal for the mouse. On, the
/// request is presses, drags and the SGR encoding, never motion without a button.
/// The bytes are checked from the command itself: on Windows `execute!` takes the
/// console API and writes none.
#[test]
fn the_mouse_is_taken_only_when_asked_for() {
    use crossterm::Command;
    let mut out = Vec::new();
    capture(false, &mut out);
    assert!(out.is_empty(), "off: nothing is asked for");
    let mut ansi = String::new();
    EnableMouse.write_ansi(&mut ansi).unwrap();
    assert_eq!(ansi, "\x1b[?1000h\x1b[?1002h\x1b[?1006h");
    assert!(!ansi.contains("?1003h"), "no motion without a button");
    #[cfg(not(windows))]
    {
        capture(true, &mut out);
        assert_eq!(String::from_utf8(out).unwrap(), ansi, "on: it is asked for");
    }
}

#[test]
fn regions_are_recorded_only_while_a_frame_is_drawn() {
    let rect = Rect::new(1, 1, 4, 1);
    record(rect, Hit::Tool(0));
    let mut p = Pointing::default();
    p.forget_drawn();
    record(rect, Hit::Tool(1));
    record(Rect::new(0, 0, 0, 1), Hit::Tool(2));
    p.drawn();
    assert_eq!(p.hits, vec![(rect, Hit::Tool(1))]);
    record(rect, Hit::Tool(3));
    p.forget_drawn();
    p.drawn();
    assert!(p.hits.is_empty(), "a frame keeps only what it drew");
}

#[test]
fn the_topmost_region_takes_the_click_and_an_open_picker_takes_them_all() {
    let field = FieldId {
        form: "f",
        field: "Name".into(),
    };
    let mut p = Pointing {
        hits: vec![
            (Rect::new(0, 0, 10, 1), Hit::Field(field.clone())),
            (Rect::new(0, 0, 3, 1), Hit::Tool(2)),
        ],
        ..Pointing::default()
    };
    assert_eq!(p.hit_at(Position { x: 1, y: 0 }), Some(&Hit::Tool(2)));
    assert_eq!(
        p.hit_at(Position { x: 5, y: 0 }),
        Some(&Hit::Field(field.clone()))
    );
    assert_eq!(p.hit_at(Position { x: 5, y: 1 }), None);
    p.hits.push((Rect::new(0, 3, 10, 2), Hit::Picker));
    let item = Hit::PickerItem {
        visible: 1,
        selected: 0,
        multi: false,
    };
    p.hits.push((Rect::new(0, 4, 10, 1), item.clone()));
    assert_eq!(p.hit_at(Position { x: 5, y: 0 }), None, "the form waits");
    assert_eq!(p.hit_at(Position { x: 5, y: 4 }), Some(&item));
    assert!(p.over_picker(Position { x: 5, y: 3 }));
}

#[test]
fn under_a_dialog_only_what_it_drew_takes_clicks() {
    let p = Pointing {
        hits: vec![
            (Rect::new(0, 0, 40, 1), Hit::Tool(0)),
            (Rect::new(0, 5, 40, 1), Hit::Tool(1)),
            (Rect::new(10, 4, 20, 4), Hit::Modal),
            (Rect::new(10, 5, 20, 1), Hit::Tool(2)),
        ],
        ..Pointing::default()
    };
    assert_eq!(p.hit_at(Position { x: 2, y: 0 }), None, "outside: nothing");
    assert_eq!(p.hit_at(Position { x: 2, y: 5 }), None);
    assert_eq!(p.hit_at(Position { x: 12, y: 5 }), Some(&Hit::Tool(2)));
    assert_eq!(
        p.hit_at(Position { x: 12, y: 6 }),
        None,
        "inside, on nothing"
    );
}

#[test]
fn a_line_records_where_its_spans_landed() {
    use ratatui::text::{Line, Span};
    let line = Line::from(vec![
        Span::raw(" "),
        Span::raw("Pivot"),
        Span::raw(" | "),
        Span::raw("Melt"),
    ]);
    let mut p = Pointing::default();
    p.forget_drawn();
    record_spans(
        Rect::new(10, 2, 12, 1),
        &line,
        vec![(1, Hit::Tool(0)), (3, Hit::Tool(1))],
    );
    p.drawn();
    assert_eq!(
        p.hits,
        vec![
            (Rect::new(11, 2, 5, 1), Hit::Tool(0)),
            // Cut at the area's edge.
            (Rect::new(19, 2, 3, 1), Hit::Tool(1)),
        ]
    );
}

#[test]
fn a_click_on_a_value_steps_to_it_and_on_a_picker_line_chooses_it() {
    let right = press(KeyCode::Right);
    let left = press(KeyCode::Left);
    let option = |index, current| Hit::Option {
        field: None,
        index,
        current,
    };
    assert_eq!(
        click_hit(&option(2, 0), false),
        Pointer::Form {
            field: None,
            act: None,
            keys: vec![right, right]
        }
    );
    assert_eq!(
        click_hit(&option(0, 1), false),
        Pointer::Form {
            field: None,
            act: None,
            keys: vec![left]
        }
    );
    let item = |visible, selected, multi| Hit::PickerItem {
        visible,
        selected,
        multi,
    };
    assert_eq!(
        click_hit(&item(0, 2, false), false),
        Pointer::Keys(vec![
            press(KeyCode::Up),
            press(KeyCode::Up),
            press(KeyCode::Enter)
        ])
    );
    assert_eq!(
        click_hit(&item(1, 1, true), false),
        Pointer::Keys(vec![press(KeyCode::Char(' '))])
    );
}

#[test]
fn a_click_acts_on_a_field_as_space_and_a_right_click_steps_back() {
    let space = Some(press(KeyCode::Char(' ')));
    assert_eq!(act_key(FieldKind::Text, false), None);
    assert_eq!(act_key(FieldKind::MultilineText, true), None);
    assert_eq!(act_key(FieldKind::Checkbox, false), space);
    assert_eq!(
        act_key(FieldKind::Checkbox, true),
        None,
        "a right click focuses"
    );
    assert_eq!(act_key(FieldKind::Choice, false), space);
    assert_eq!(act_key(FieldKind::Choice, true), Some(press(KeyCode::Left)));
    assert_eq!(act_key(FieldKind::Picker { multi: true }, true), None);
    assert_eq!(act_key(FieldKind::Button, true), None);
    assert_eq!(act_key(FieldKind::Button, false), space);
}

#[test]
fn a_field_id_focuses_only_its_own_form() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum F {
        A,
        B,
    }
    struct One(F, bool);
    struct Two(F);
    impl Form for One {
        type Field = F;
        fn fields(&self) -> Vec<(F, FieldKind)> {
            let mut f = vec![(F::A, FieldKind::Text)];
            if self.1 {
                f.push((F::B, FieldKind::Checkbox));
            }
            f
        }
        fn focused(&self) -> F {
            self.0
        }
        fn set_focused(&mut self, field: F) {
            self.0 = field;
        }
    }
    impl Form for Two {
        type Field = F;
        fn fields(&self) -> Vec<(F, FieldKind)> {
            vec![(F::A, FieldKind::Text), (F::B, FieldKind::Choice)]
        }
        fn focused(&self) -> F {
            self.0
        }
        fn set_focused(&mut self, field: F) {
            self.0 = field;
        }
    }
    let id = FieldId::of::<One>(F::B);
    let mut two = Two(F::A);
    assert_eq!(id.focus_in(&mut two), None, "another form's field");
    assert_eq!(two.0, F::A);
    let mut hidden = One(F::A, false);
    assert_eq!(id.focus_in(&mut hidden), None, "a field not shown");
    let mut one = One(F::A, true);
    let clicked = |kind| Some(Clicked { kind, acts: true });
    assert_eq!(id.focus_in(&mut one), clicked(FieldKind::Checkbox));
    assert_eq!(one.0, F::B);
}

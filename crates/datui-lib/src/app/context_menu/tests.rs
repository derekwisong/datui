use super::*;
use crossterm::event::KeyModifiers;

/// Every line's key is a key of the table's in the key registry, so the menu
/// cannot offer a key the table does not take.
#[test]
fn every_line_presses_a_table_key_from_the_registry() {
    let table = datui_cli::keys::screen(datui_cli::keys::Context::Table);
    let table_keys: Vec<_> = table
        .groups
        .iter()
        .flat_map(|g| g.keys.iter())
        .flat_map(|k| datui_cli::keys::chords(k.keys))
        .collect();
    for item in ITEMS {
        assert!(item.action.is_none());
        let chord = datui_cli::keys::chord(item.key).expect("one key");
        assert!(
            table_keys.contains(&chord),
            "{} is not a table key in the registry",
            item.key
        );
    }
}

#[test]
fn the_arrows_move_round_and_enter_runs_the_line() {
    let mut menu = ContextMenu::with(Position { x: 0, y: 0 }, Vec::new());
    let press = |code| KeyEvent::new(code, KeyModifiers::NONE);
    assert_eq!(menu.key(&press(KeyCode::Up)), MenuKey::Moved);
    assert_eq!(menu.selected, ITEMS.len() - 1, "↑ from the top wraps");
    assert_eq!(
        menu.key(&press(KeyCode::Enter)),
        MenuKey::Run(press(KeyCode::Char(' ')))
    );
    menu.key(&press(KeyCode::Char('j')));
    assert_eq!(menu.selected, 0);
    assert_eq!(
        menu.key(&press(KeyCode::Enter)),
        MenuKey::Run(press(KeyCode::Char('+')))
    );
    assert_eq!(menu.key(&press(KeyCode::Esc)), MenuKey::Close);
    assert_eq!(menu.key(&press(KeyCode::Char('x'))), MenuKey::Other);
    let shifted = KeyEvent::new(KeyCode::Down, KeyModifiers::SHIFT);
    assert_eq!(menu.key(&shifted), MenuKey::Other, "Shift+↓ is the table's");
}

#[test]
fn the_menu_opens_at_the_point_and_stays_on_screen() {
    let screen = Rect::new(0, 0, 80, 24);
    let at = |x, y| ContextMenu::with(Position { x, y }, Vec::new()).area(screen);
    let menu = at(10, 5);
    assert_eq!((menu.x, menu.y), (11, 6));
    assert_eq!(menu.height, ITEMS.len() as u16 + 2);
    // At the right and bottom edges it moves left and up, whole.
    let corner = at(79, 23);
    assert_eq!(corner.right(), 80);
    assert_eq!(corner.bottom(), 24);
    assert_eq!(corner.width, menu.width);
    // A screen smaller than the menu: it fits as much as there is.
    let tiny = ContextMenu::with(Position { x: 3, y: 3 }, Vec::new()).area(Rect::new(0, 0, 12, 5));
    assert_eq!(tiny, Rect::new(0, 0, 12, 5));
}

#[test]
fn each_line_shows_its_key_and_the_cursor_line_carries_the_rail() {
    let ctx = RenderContext::for_test();
    let screen = Rect::new(0, 0, 60, 20);
    let mut buf = Buffer::empty(screen);
    let menu = ContextMenu {
        selected: 2,
        ..ContextMenu::with(Position { x: 2, y: 2 }, Vec::new())
    };
    menu.render(screen, &mut buf, &ctx);
    let area = menu.area(screen);
    let lines: Vec<String> = (area.y..area.bottom())
        .map(|y| {
            (area.x..area.right())
                .map(|x| buf[(x, y)].symbol())
                .collect()
        })
        .collect();
    let g = crate::glyphs::get();
    assert!(
        lines[1].contains("+      Filter to this value"),
        "{lines:?}"
    );
    assert!(lines[3].contains(&format!("{}F", g.rail)), "{lines:?}");
    assert!(lines[7].contains("Space  Inspect row"), "{lines:?}");
}

/// The column's lines have no key: choosing one does its action.
#[test]
fn a_column_line_does_its_action() {
    let mut menu = ContextMenu::with(Position { x: 0, y: 0 }, column_items(true));
    assert_eq!(menu.items.len(), ITEMS.len() + 2);
    menu.selected = ITEMS.len();
    let enter = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(menu.key(&enter), MenuKey::Do(MenuAction::ChangeType));
    assert_eq!(
        menu.chosen(ITEMS.len() + 1),
        Some(MenuKey::Do(MenuAction::CombineDatetime))
    );
    assert_eq!(column_items(false).len(), 1, "no datetime from a number");
}

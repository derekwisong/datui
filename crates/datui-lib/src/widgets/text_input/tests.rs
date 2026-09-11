//! Behaviour of the shared text field, in both of its modes.
//!
//! These cover the behaviour every call site in the app depends on: typing and
//! editing, Enter and Esc, history recall, cursor bookkeeping, and the fact
//! that what is rendered is always what `value()` reports.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier},
    widgets::Widget,
};

use super::*;
use crate::config::{Theme, ThemeConfig};

fn press(input: &mut TextInput, code: KeyCode) -> TextInputEvent {
    input.handle_key(&KeyEvent::new(code, KeyModifiers::NONE), None)
}

fn press_with(input: &mut TextInput, code: KeyCode, modifiers: KeyModifiers) -> TextInputEvent {
    input.handle_key(&KeyEvent::new(code, modifiers), None)
}

fn type_str(input: &mut TextInput, text: &str) {
    for c in text.chars() {
        press(input, KeyCode::Char(c));
    }
}

fn rendered(input: &TextInput, width: u16, height: u16) -> Vec<String> {
    let rect = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(rect);
    input.render(rect, &mut buf);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect()
}

#[test]
fn a_new_input_is_empty_and_single_line() {
    let input = TextInput::new();
    assert_eq!(input.value(), "");
    assert_eq!(input.cursor(), 0);
    assert_eq!(input.mode(), TextInputMode::SingleLine);
    assert!(input.is_empty());
    assert!(!input.is_focused());
}

#[test]
fn multiline_inputs_report_their_mode() {
    assert_eq!(TextInput::multiline().mode(), TextInputMode::MultiLine);
}

#[test]
fn typing_updates_the_value_and_cursor() {
    let mut input = TextInput::new();
    type_str(&mut input, "hello");
    assert_eq!(input.value(), "hello");
    assert_eq!(input.cursor(), 5);
}

#[test]
fn set_value_replaces_the_contents_and_parks_the_cursor_at_the_end() {
    let mut input = TextInput::new();
    input.set_value("select a");
    assert_eq!(input.value(), "select a");
    assert_eq!(input.cursor(), 8);
}

#[test]
fn set_value_is_what_gets_rendered() {
    // The value and the drawn text are the same thing, so restoring a saved
    // query puts it back on screen rather than leaving the last frame's text.
    let mut input = TextInput::new();
    type_str(&mut input, "typed");
    input.clear();
    input.set_value("restored query");
    assert_eq!(rendered(&input, 20, 1), vec!["restored query".to_string()]);
}

#[test]
fn a_single_line_input_never_holds_a_newline() {
    let mut input = TextInput::new();
    input.set_value("one\ntwo");
    assert_eq!(input.value(), "one two");
    assert_eq!(input.line_count(), 1);
}

#[test]
fn enter_submits_a_single_line_input() {
    let mut input = TextInput::new();
    type_str(&mut input, "abc");
    assert_eq!(press(&mut input, KeyCode::Enter), TextInputEvent::Submit);
    assert_eq!(input.value(), "abc");
}

#[test]
fn ctrl_m_submits_like_enter_instead_of_breaking_the_line() {
    let mut input = TextInput::new();
    type_str(&mut input, "abc");
    assert_eq!(
        press_with(&mut input, KeyCode::Char('m'), KeyModifiers::CONTROL),
        TextInputEvent::Submit
    );
    assert_eq!(input.value(), "abc");
}

#[test]
fn enter_inserts_a_newline_in_a_multiline_input() {
    let mut input = TextInput::multiline();
    type_str(&mut input, "one");
    assert_eq!(press(&mut input, KeyCode::Enter), TextInputEvent::None);
    type_str(&mut input, "two");
    assert_eq!(input.value(), "one\ntwo");
    assert_eq!(input.line_count(), 2);
    assert_eq!(input.cursor_line(), 1);
    assert_eq!(input.cursor_col(), 3);
}

#[test]
fn esc_cancels_in_both_modes() {
    for mut input in [TextInput::new(), TextInput::multiline()] {
        type_str(&mut input, "abc");
        assert_eq!(press(&mut input, KeyCode::Esc), TextInputEvent::Cancel);
        assert_eq!(input.value(), "abc");
    }
}

#[test]
fn editing_keys_work_the_same_in_both_modes() {
    for mut input in [TextInput::new(), TextInput::multiline()] {
        type_str(&mut input, "hello");
        press(&mut input, KeyCode::Backspace);
        assert_eq!(input.value(), "hell");
        press(&mut input, KeyCode::Home);
        assert_eq!(input.cursor(), 0);
        press(&mut input, KeyCode::Delete);
        assert_eq!(input.value(), "ell");
        press(&mut input, KeyCode::End);
        assert_eq!(input.cursor(), 3);
        press(&mut input, KeyCode::Left);
        assert_eq!(input.cursor(), 2);
        press(&mut input, KeyCode::Right);
        assert_eq!(input.cursor(), 3);
    }
}

#[test]
fn multibyte_values_are_measured_in_characters() {
    let mut input = TextInput::new();
    type_str(&mut input, "café🚀");
    assert_eq!(input.cursor(), 5);
    press(&mut input, KeyCode::Backspace);
    assert_eq!(input.value(), "café");
    assert_eq!(input.cursor(), 4);
}

#[test]
fn clear_empties_the_input() {
    let mut input = TextInput::new();
    type_str(&mut input, "abc");
    input.clear();
    assert_eq!(input.value(), "");
    assert_eq!(input.cursor(), 0);
    assert!(input.is_empty());
    assert_eq!(rendered(&input, 10, 1), vec![String::new()]);
}

#[test]
fn cursor_offsets_round_trip_through_lines_and_columns() {
    let mut input = TextInput::multiline();
    input.set_value("one\ntwo\nthree");
    assert_eq!(input.cursor(), 13);
    input.set_cursor(5);
    assert_eq!((input.cursor_line(), input.cursor_col()), (1, 1));
    assert_eq!(input.cursor(), 5);
    input.set_cursor(0);
    assert_eq!((input.cursor_line(), input.cursor_col()), (0, 0));
    input.set_cursor(999);
    assert_eq!((input.cursor_line(), input.cursor_col()), (2, 5));
}

#[test]
fn set_cursor_line_col_clamps_into_the_text() {
    let mut input = TextInput::multiline();
    input.set_value("one\ntwo");
    input.set_cursor_line_col(1, 99);
    assert_eq!((input.cursor_line(), input.cursor_col()), (1, 3));
    input.set_cursor_line_col(99, 0);
    assert_eq!(input.cursor_line(), 1);
}

#[test]
fn moving_by_lines_clamps_at_the_ends() {
    let mut input = TextInput::multiline();
    input.set_value("0\n1\n2\n3\n4\n5\n6\n7");
    input.set_cursor_line_col(7, 0);
    input.move_cursor_by_lines(-5);
    assert_eq!(input.cursor_line(), 2);
    input.move_cursor_by_lines(-5);
    assert_eq!(input.cursor_line(), 0);
    input.move_cursor_by_lines(5);
    assert_eq!(input.cursor_line(), 5);
    input.move_cursor_by_lines(5);
    assert_eq!(input.cursor_line(), 7);
}

#[test]
fn line_accessors_expose_the_wrapped_text() {
    let mut input = TextInput::multiline();
    input.set_value("one\ntwo");
    assert_eq!(input.line_count(), 2);
    assert_eq!(input.line_at(0), Some("one"));
    assert_eq!(input.line_at(5), None);
}

#[test]
fn submitting_records_the_value_in_the_history() {
    let dir = tempfile::tempdir().expect("temp dir");
    let cache = crate::cache::CacheManager::with_dir(dir.path().to_path_buf());
    let mut input = TextInput::new().with_history("test".to_string());

    input.set_value("select one");
    let event = input.handle_key(
        &KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        Some(&cache),
    );
    assert_eq!(event, TextInputEvent::Submit);
    assert_eq!(input.history_entries(), ["select one"]);

    let mut reopened = TextInput::new().with_history("test".to_string());
    reopened.load_history(&cache).expect("load history");
    reopened.handle_key(
        &KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
        Some(&cache),
    );
    assert_eq!(reopened.value(), "select one");
}

#[test]
fn history_navigation_replaces_the_value() {
    let mut input = TextInput::new().with_history("test".to_string());
    seed_history(&mut input, &["one", "two"]);

    input.set_value("draft");
    assert_eq!(
        press(&mut input, KeyCode::Up),
        TextInputEvent::HistoryChanged
    );
    assert_eq!(input.value(), "two");
    assert_eq!(input.cursor(), 3);
    press(&mut input, KeyCode::Up);
    assert_eq!(input.value(), "one");
    press(&mut input, KeyCode::Down);
    assert_eq!(input.value(), "two");
    press(&mut input, KeyCode::Down);
    assert_eq!(input.value(), "draft");
}

#[test]
fn recalled_history_is_what_gets_rendered() {
    let mut input = TextInput::new().with_history("test".to_string());
    seed_history(&mut input, &["select one"]);
    press(&mut input, KeyCode::Up);
    assert_eq!(rendered(&input, 16, 1), vec!["select one".to_string()]);
}

#[test]
fn multiline_recall_uses_ctrl_p_and_ctrl_n() {
    let mut input = TextInput::multiline().with_history("test".to_string());
    seed_history(&mut input, &["one", "two"]);

    input.set_value("draft");
    assert_eq!(
        press_with(&mut input, KeyCode::Char('p'), KeyModifiers::CONTROL),
        TextInputEvent::HistoryChanged
    );
    assert_eq!(input.value(), "two");
    press_with(&mut input, KeyCode::Char('n'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "draft");
}

#[test]
fn arrow_keys_move_between_lines_in_a_multiline_input_with_history() {
    let mut input = TextInput::multiline().with_history("test".to_string());
    seed_history(&mut input, &["recalled"]);
    input.set_value("one\ntwo");
    press(&mut input, KeyCode::Up);
    assert_eq!(input.value(), "one\ntwo");
    assert_eq!(input.cursor_line(), 0);
}

#[test]
fn typing_abandons_a_history_walk() {
    let mut input = TextInput::new().with_history("test".to_string());
    seed_history(&mut input, &["one", "two"]);
    input.set_value("draft");
    press(&mut input, KeyCode::Up);
    assert_eq!(input.value(), "two");
    type_str(&mut input, "!");
    assert_eq!(input.value(), "two!");
    // The walk was abandoned, so stepping forward has nothing to restore.
    press(&mut input, KeyCode::Down);
    assert_eq!(input.value(), "two!");
}

#[test]
fn arrow_keys_do_not_recall_without_a_history() {
    let mut input = TextInput::new();
    input.set_value("kept");
    assert_eq!(press(&mut input, KeyCode::Up), TextInputEvent::None);
    assert_eq!(input.value(), "kept");
}

#[test]
fn focus_controls_whether_a_cursor_is_drawn() {
    let mut input = TextInput::new();
    input.set_value("ab");
    input.set_cursor(0);

    let rect = Rect::new(0, 0, 2, 1);
    let mut buf = Buffer::empty(rect);
    (&input).render(rect, &mut buf);
    assert!(!buf[(0, 0)]
        .style()
        .add_modifier
        .contains(Modifier::REVERSED));

    input.set_focused(true);
    assert!(input.is_focused());
    let mut buf = Buffer::empty(rect);
    (&input).render(rect, &mut buf);
    assert!(buf[(0, 0)]
        .style()
        .add_modifier
        .contains(Modifier::REVERSED));
}

#[test]
fn a_theme_colours_the_text_and_the_cursor() {
    let theme = test_theme();
    let mut input = TextInput::new().with_theme(&theme);
    input.set_value("ab");
    input.set_cursor(0);
    input.set_focused(true);

    let rect = Rect::new(0, 0, 2, 1);
    let mut buf = Buffer::empty(rect);
    (&input).render(rect, &mut buf);
    assert_eq!(buf[(1, 0)].style().fg, Some(theme.get("text_primary")));
}

#[test]
fn an_explicit_cursor_colour_is_drawn_as_a_block() {
    let mut input = TextInput::new().with_text_color(Color::White);
    input.cursor_color = Some(Color::Cyan);
    input.apply_styles();
    input.set_value("ab");
    input.set_cursor(0);
    input.set_focused(true);

    let rect = Rect::new(0, 0, 2, 1);
    let mut buf = Buffer::empty(rect);
    (&input).render(rect, &mut buf);
    assert_eq!(buf[(0, 0)].style().bg, Some(Color::Cyan));
    assert_eq!(buf[(0, 0)].style().fg, Some(Color::Black));
}

#[test]
fn a_background_colour_fills_the_input_area() {
    let mut input = TextInput::new().with_background(Color::Blue);
    input.set_value("a");
    let rect = Rect::new(0, 0, 4, 1);
    let mut buf = Buffer::empty(rect);
    (&input).render(rect, &mut buf);
    for x in 0..4 {
        assert_eq!(buf[(x, 0)].style().bg, Some(Color::Blue));
    }
}

#[test]
fn long_values_scroll_horizontally_to_keep_the_cursor_in_view() {
    let mut input = TextInput::new();
    input.set_value("abcdefghij");
    assert_eq!(rendered(&input, 4, 1), vec!["hij".to_string()]);
    input.set_cursor(0);
    assert_eq!(rendered(&input, 4, 1), vec!["abcd".to_string()]);
}

#[test]
fn tall_values_scroll_vertically_to_keep_the_cursor_in_view() {
    let mut input = TextInput::multiline();
    input.set_value("l0\nl1\nl2\nl3");
    assert_eq!(
        rendered(&input, 4, 2),
        vec!["l2".to_string(), "l3".to_string()]
    );
    assert_eq!(input.scroll_offsets(), (2, 0));
    input.set_cursor_line_col(0, 0);
    assert_eq!(
        rendered(&input, 4, 2),
        vec!["l0".to_string(), "l1".to_string()]
    );
}

#[test]
fn readline_editing_shortcuts_are_available() {
    let mut input = TextInput::new();
    type_str(&mut input, "alpha beta");
    press_with(&mut input, KeyCode::Char('w'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "alpha ");
    press_with(&mut input, KeyCode::Char('a'), KeyModifiers::CONTROL);
    assert_eq!(input.cursor(), 0);
    press_with(&mut input, KeyCode::Char('k'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "");
    press_with(&mut input, KeyCode::Char('y'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "alpha ");
}

#[test]
fn undo_is_available_inside_the_field() {
    let mut input = TextInput::new();
    type_str(&mut input, "ab");
    press_with(&mut input, KeyCode::Char('u'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "a");
    press_with(&mut input, KeyCode::Char('r'), KeyModifiers::CONTROL);
    assert_eq!(input.value(), "ab");
}

#[test]
fn shift_arrow_selects_and_typing_replaces_the_selection() {
    let mut input = TextInput::new();
    type_str(&mut input, "hello");
    press_with(&mut input, KeyCode::Home, KeyModifiers::NONE);
    for _ in 0..5 {
        press_with(&mut input, KeyCode::Right, KeyModifiers::SHIFT);
    }
    type_str(&mut input, "x");
    assert_eq!(input.value(), "x");
}

#[test]
fn the_default_input_can_be_taken_and_rebuilt() {
    // `std::mem::take` on a field is how the chart modal re-themes its inputs.
    let mut input = TextInput::new();
    input.set_value("abc");
    let taken = std::mem::take(&mut input).with_theme(&test_theme());
    assert_eq!(taken.value(), "abc");
    assert_eq!(input.value(), "");
}

#[test]
fn a_zero_sized_area_renders_nothing() {
    let mut input = TextInput::new();
    input.set_value("abc");
    let mut buf = Buffer::empty(Rect::new(0, 0, 1, 1));
    (&input).render(Rect::new(0, 0, 0, 0), &mut buf);
    assert_eq!(buf[(0, 0)].symbol(), " ");
}

/// The default theme, as the app builds it from default configuration.
fn test_theme() -> Theme {
    Theme::from_config(&ThemeConfig::default()).expect("default theme")
}

/// Put entries into an input's history without going through the cache.
fn seed_history(input: &mut TextInput, entries: &[&str]) {
    for entry in entries {
        input.history.seed(entry.to_string());
    }
}

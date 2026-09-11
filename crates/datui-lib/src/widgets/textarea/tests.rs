//! Behavioural tests for the editor core.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::Widget,
};

use super::{CursorMove, Input, Key, TextArea};

fn press(area: &mut TextArea, code: KeyCode, modifiers: KeyModifiers) -> bool {
    area.input(KeyEvent::new(code, modifiers))
}

fn type_str(area: &mut TextArea, text: &str) {
    for c in text.chars() {
        area.insert_char(c);
    }
}

fn render_to_lines(area: &TextArea, width: u16, height: u16) -> Vec<String> {
    let rect = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(rect);
    area.render(rect, &mut buf);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

#[test]
fn new_editor_holds_one_empty_line() {
    let area = TextArea::new();
    assert_eq!(area.lines(), &[String::new()]);
    assert_eq!(area.cursor(), (0, 0));
    assert!(area.is_empty());
    assert_eq!(area.text(), "");
}

#[test]
fn from_text_splits_lines_and_parks_cursor_at_the_end() {
    let area = TextArea::from_text("one\ntwo");
    assert_eq!(area.lines(), &["one".to_string(), "two".to_string()]);
    assert_eq!(area.cursor(), (1, 3));
    assert_eq!(area.text(), "one\ntwo");
}

#[test]
fn crlf_is_normalised_on_load() {
    let area = TextArea::from_text("a\r\nb\r\n");
    assert_eq!(
        area.lines(),
        &["a".to_string(), "b".to_string(), String::new()]
    );
}

#[test]
fn typing_inserts_at_the_cursor() {
    let mut area = TextArea::new();
    type_str(&mut area, "hello");
    assert_eq!(area.text(), "hello");
    assert_eq!(area.cursor(), (0, 5));
    area.move_cursor(CursorMove::Head);
    type_str(&mut area, ">");
    assert_eq!(area.text(), ">hello");
    assert_eq!(area.cursor(), (0, 1));
}

#[test]
fn newline_splits_the_current_line() {
    let mut area = TextArea::from_text("abcd");
    area.set_cursor(0, 2);
    area.insert_newline();
    assert_eq!(area.lines(), &["ab".to_string(), "cd".to_string()]);
    assert_eq!(area.cursor(), (1, 0));
}

#[test]
fn backspace_joins_lines_at_column_zero() {
    let mut area = TextArea::from_text("ab\ncd");
    area.set_cursor(1, 0);
    assert!(area.delete_prev_char());
    assert_eq!(area.text(), "abcd");
    assert_eq!(area.cursor(), (0, 2));
}

#[test]
fn delete_joins_lines_at_end_of_line() {
    let mut area = TextArea::from_text("ab\ncd");
    area.set_cursor(0, 2);
    assert!(area.delete_next_char());
    assert_eq!(area.text(), "abcd");
    assert_eq!(area.cursor(), (0, 2));
}

#[test]
fn deleting_at_the_buffer_edges_is_a_no_op() {
    let mut area = TextArea::from_text("ab");
    area.set_cursor(0, 0);
    assert!(!area.delete_prev_char());
    area.set_cursor(0, 2);
    assert!(!area.delete_next_char());
    assert_eq!(area.text(), "ab");
}

#[test]
fn multibyte_text_is_edited_by_character_not_byte() {
    let mut area = TextArea::from_text("héllo🚀");
    assert_eq!(area.cursor(), (0, 6));
    assert!(area.delete_prev_char());
    assert_eq!(area.text(), "héllo");
    area.set_cursor(0, 1);
    assert!(area.delete_next_char());
    assert_eq!(area.text(), "hllo");
}

#[test]
fn cursor_moves_wrap_across_lines() {
    let mut area = TextArea::from_text("ab\ncd");
    area.set_cursor(0, 2);
    area.move_cursor(CursorMove::Forward);
    assert_eq!(area.cursor(), (1, 0));
    area.move_cursor(CursorMove::Back);
    assert_eq!(area.cursor(), (0, 2));
}

#[test]
fn vertical_moves_clamp_to_shorter_lines() {
    let mut area = TextArea::from_text("longer line\nab");
    area.set_cursor(0, 9);
    area.move_cursor(CursorMove::Down);
    assert_eq!(area.cursor(), (1, 2));
}

#[test]
fn head_end_top_and_bottom_moves() {
    let mut area = TextArea::from_text("one\ntwo three");
    area.set_cursor(1, 4);
    area.move_cursor(CursorMove::Head);
    assert_eq!(area.cursor(), (1, 0));
    area.move_cursor(CursorMove::End);
    assert_eq!(area.cursor(), (1, 9));
    area.move_cursor(CursorMove::Top);
    assert_eq!(area.cursor(), (0, 0));
    area.move_cursor(CursorMove::Bottom);
    assert_eq!(area.cursor(), (1, 9));
}

#[test]
fn word_moves_step_between_words() {
    let mut area = TextArea::from_text("alpha beta gamma");
    area.set_cursor(0, 0);
    area.move_cursor(CursorMove::WordForward);
    assert_eq!(area.cursor(), (0, 6));
    area.move_cursor(CursorMove::WordForward);
    assert_eq!(area.cursor(), (0, 11));
    area.move_cursor(CursorMove::WordBack);
    assert_eq!(area.cursor(), (0, 6));
}

#[test]
fn jump_clamps_into_the_buffer() {
    let mut area = TextArea::from_text("ab\ncd");
    area.move_cursor(CursorMove::Jump(99, 99));
    assert_eq!(area.cursor(), (1, 2));
}

#[test]
fn kill_to_end_of_line_then_join() {
    let mut area = TextArea::from_text("hello world\nnext");
    area.set_cursor(0, 5);
    assert!(area.delete_line_by_end());
    assert_eq!(area.lines()[0], "hello");
    assert_eq!(area.yanked_text(), " world");
    assert!(area.delete_line_by_end());
    assert_eq!(area.text(), "hellonext");
}

#[test]
fn kill_to_head_of_line_then_join() {
    let mut area = TextArea::from_text("one\ntwo");
    area.set_cursor(1, 3);
    assert!(area.delete_line_by_head());
    assert_eq!(area.text(), "one\n");
    assert!(area.delete_line_by_head());
    assert_eq!(area.text(), "one");
}

#[test]
fn word_deletion_removes_whole_words() {
    let mut area = TextArea::from_text("alpha beta");
    assert!(area.delete_prev_word());
    assert_eq!(area.text(), "alpha ");
    assert_eq!(area.yanked_text(), "beta");

    let mut area = TextArea::from_text("alpha beta");
    area.set_cursor(0, 0);
    assert!(area.delete_next_word());
    assert_eq!(area.text(), "beta");
}

#[test]
fn yank_round_trips_through_paste() {
    let mut area = TextArea::from_text("hello world");
    area.set_cursor(0, 6);
    area.delete_line_by_end();
    assert_eq!(area.text(), "hello ");
    area.set_cursor(0, 0);
    assert!(area.paste());
    assert_eq!(area.text(), "worldhello ");
}

#[test]
fn paste_with_an_empty_yank_buffer_does_nothing() {
    let mut area = TextArea::from_text("abc");
    assert!(!area.paste());
    assert_eq!(area.text(), "abc");
}

#[test]
fn selection_spans_the_moved_range() {
    let mut area = TextArea::from_text("hello");
    area.set_cursor(0, 1);
    area.move_cursor_selecting(CursorMove::End);
    assert_eq!(area.selection(), Some(((0, 1), (0, 5))));
    area.cancel_selection();
    assert_eq!(area.selection(), None);
}

#[test]
fn typing_replaces_the_selection() {
    let mut area = TextArea::from_text("hello");
    area.set_cursor(0, 0);
    area.move_cursor_selecting(CursorMove::End);
    area.insert_char('x');
    assert_eq!(area.text(), "x");
    assert_eq!(area.selection(), None);
}

#[test]
fn cut_and_copy_fill_the_yank_buffer() {
    let mut area = TextArea::from_text("hello");
    area.set_cursor(0, 0);
    area.move_cursor_selecting(CursorMove::Forward);
    area.move_cursor_selecting(CursorMove::Forward);
    assert!(area.copy());
    assert_eq!(area.yanked_text(), "he");
    assert_eq!(area.text(), "hello");

    area.set_cursor(0, 0);
    area.move_cursor_selecting(CursorMove::Forward);
    assert!(area.cut());
    assert_eq!(area.yanked_text(), "h");
    assert_eq!(area.text(), "ello");
}

#[test]
fn select_all_covers_every_line() {
    let mut area = TextArea::from_text("ab\ncd");
    area.select_all();
    assert_eq!(area.selection(), Some(((0, 0), (1, 2))));
    area.delete_selection();
    assert_eq!(area.text(), "");
}

#[test]
fn selection_across_lines_is_deleted_whole() {
    let mut area = TextArea::from_text("one\ntwo\nthree");
    area.set_cursor(0, 1);
    area.start_selection();
    area.move_cursor_selecting(CursorMove::Jump(2, 2));
    assert!(area.delete_selection());
    assert_eq!(area.text(), "oree");
}

#[test]
fn undo_and_redo_walk_the_edit_history() {
    let mut area = TextArea::new();
    type_str(&mut area, "abc");
    assert_eq!(area.text(), "abc");
    assert!(area.undo());
    assert_eq!(area.text(), "ab");
    assert!(area.undo());
    assert_eq!(area.text(), "a");
    assert!(area.redo());
    assert_eq!(area.text(), "ab");
    assert!(area.redo());
    assert_eq!(area.text(), "abc");
    assert!(!area.redo());
}

#[test]
fn undo_restores_multi_line_deletions() {
    let mut area = TextArea::from_text("one\ntwo\nthree");
    area.select_all();
    area.delete_selection();
    assert_eq!(area.text(), "");
    assert!(area.undo());
    assert_eq!(area.text(), "one\ntwo\nthree");
    assert_eq!(area.cursor(), (2, 5));
}

#[test]
fn undo_restores_the_cursor_position() {
    let mut area = TextArea::from_text("hello");
    area.set_cursor(0, 2);
    area.insert_char('X');
    assert_eq!(area.cursor(), (0, 3));
    area.undo();
    assert_eq!(area.text(), "hello");
    assert_eq!(area.cursor(), (0, 2));
}

#[test]
fn a_new_edit_discards_the_redo_stack() {
    let mut area = TextArea::new();
    type_str(&mut area, "ab");
    area.undo();
    area.insert_char('z');
    assert_eq!(area.text(), "az");
    assert!(!area.redo());
}

#[test]
fn set_text_discards_undo_history() {
    let mut area = TextArea::new();
    type_str(&mut area, "abc");
    area.set_text("fresh");
    assert!(!area.undo());
    assert_eq!(area.text(), "fresh");
}

#[test]
fn tab_inserts_spaces_to_the_next_stop() {
    let mut area = TextArea::new();
    area.set_tab_len(4);
    area.insert_tab();
    assert_eq!(area.text(), "    ");
    area.insert_char('x');
    area.insert_tab();
    assert_eq!(area.text(), "    x   ");
}

#[test]
fn key_input_drives_editing() {
    let mut area = TextArea::new();
    press(&mut area, KeyCode::Char('h'), KeyModifiers::NONE);
    press(&mut area, KeyCode::Char('i'), KeyModifiers::NONE);
    assert_eq!(area.text(), "hi");
    press(&mut area, KeyCode::Backspace, KeyModifiers::NONE);
    assert_eq!(area.text(), "h");
    press(&mut area, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(area.lines().len(), 2);
    press(&mut area, KeyCode::Char('u'), KeyModifiers::CONTROL);
    assert_eq!(area.lines().len(), 1);
}

#[test]
fn unhandled_keys_report_no_change() {
    let mut area = TextArea::from_text("abc");
    assert!(!press(&mut area, KeyCode::Esc, KeyModifiers::NONE));
    assert!(!press(&mut area, KeyCode::F(1), KeyModifiers::NONE));
    assert_eq!(area.text(), "abc");
}

#[test]
fn input_accepts_a_backend_independent_key() {
    let mut area = TextArea::new();
    area.input(Input::new(Key::Char('q')));
    assert_eq!(area.text(), "q");
}

#[test]
fn rendering_draws_the_buffer() {
    let area = TextArea::from_text("ab\ncd");
    let lines = render_to_lines(&area, 4, 2);
    assert_eq!(lines, vec!["ab  ".to_string(), "cd  ".to_string()]);
}

#[test]
fn rendering_scrolls_horizontally_to_follow_the_cursor() {
    let mut area = TextArea::from_text("abcdefghij");
    area.set_cursor(0, 10);
    let lines = render_to_lines(&area, 4, 1);
    // Cursor sits past 'j', so the last visible column is the cursor cell.
    assert_eq!(lines[0], "hij ");
    assert_eq!(area.scroll_offsets(), (0, 7));

    area.set_cursor(0, 0);
    let lines = render_to_lines(&area, 4, 1);
    assert_eq!(lines[0], "abcd");
    assert_eq!(area.scroll_offsets(), (0, 0));
}

#[test]
fn rendering_scrolls_vertically_to_follow_the_cursor() {
    let mut area = TextArea::from_text("l0\nl1\nl2\nl3\nl4");
    area.set_cursor(4, 0);
    let lines = render_to_lines(&area, 2, 2);
    assert_eq!(lines, vec!["l3".to_string(), "l4".to_string()]);
    assert_eq!(area.scroll_offsets(), (3, 0));

    area.set_cursor(0, 0);
    let lines = render_to_lines(&area, 2, 2);
    assert_eq!(lines, vec!["l0".to_string(), "l1".to_string()]);
}

#[test]
fn the_cursor_cell_is_styled_when_visible() {
    let mut area = TextArea::from_text("ab");
    area.set_cursor(0, 0);
    area.set_cursor_style(Style::default().bg(Color::Red));

    let rect = Rect::new(0, 0, 2, 1);
    let mut buf = Buffer::empty(rect);
    (&area).render(rect, &mut buf);
    assert_eq!(buf[(0, 0)].style().bg, Some(Color::Red));
    assert_eq!(buf[(1, 0)].style().bg, Some(Color::Reset));

    area.set_cursor_visible(false);
    let mut buf = Buffer::empty(rect);
    (&area).render(rect, &mut buf);
    assert_eq!(buf[(0, 0)].style().bg, Some(Color::Reset));
}

#[test]
fn the_cursor_is_drawn_past_the_end_of_a_line() {
    let mut area = TextArea::from_text("ab");
    area.set_cursor_style(Style::default().add_modifier(Modifier::REVERSED));
    let rect = Rect::new(0, 0, 4, 1);
    let mut buf = Buffer::empty(rect);
    (&area).render(rect, &mut buf);
    assert!(buf[(2, 0)]
        .style()
        .add_modifier
        .contains(Modifier::REVERSED));
}

#[test]
fn selected_cells_are_styled() {
    let mut area = TextArea::from_text("abcd");
    area.set_selection_style(Style::default().bg(Color::Blue));
    area.set_cursor_visible(false);
    area.set_cursor(0, 1);
    area.move_cursor_selecting(CursorMove::Forward);
    area.move_cursor_selecting(CursorMove::Forward);

    let rect = Rect::new(0, 0, 4, 1);
    let mut buf = Buffer::empty(rect);
    (&area).render(rect, &mut buf);
    assert_eq!(buf[(0, 0)].style().bg, Some(Color::Reset));
    assert_eq!(buf[(1, 0)].style().bg, Some(Color::Blue));
    assert_eq!(buf[(2, 0)].style().bg, Some(Color::Blue));
    assert_eq!(buf[(3, 0)].style().bg, Some(Color::Reset));
}

#[test]
fn the_base_style_fills_the_whole_area() {
    let mut area = TextArea::from_text("a");
    area.set_style(Style::default().bg(Color::Green));
    let rect = Rect::new(0, 0, 3, 2);
    let mut buf = Buffer::empty(rect);
    (&area).render(rect, &mut buf);
    for y in 0..2 {
        for x in 0..3 {
            assert_eq!(buf[(x, y)].style().bg, Some(Color::Green), "at {x},{y}");
        }
    }
}

#[test]
fn wide_characters_occupy_two_cells() {
    let area = TextArea::from_text("漢字");
    let lines = render_to_lines(&area, 6, 1);
    assert_eq!(lines[0], "漢 字   ");
    assert_eq!(area.display_col(0, 2), 4);
}

#[test]
fn tabs_render_as_spaces_to_the_next_stop() {
    let mut area = TextArea::new();
    area.set_tab_len(4);
    area.insert_char('a');
    area.insert_char('\t');
    area.insert_char('b');
    let lines = render_to_lines(&area, 6, 1);
    assert_eq!(lines[0], "a   b ");
    assert_eq!(area.display_col(0, 2), 4);
}

#[test]
fn rendering_into_a_zero_sized_area_is_safe() {
    let area = TextArea::from_text("abc");
    let rect = Rect::new(0, 0, 0, 0);
    let mut buf = Buffer::empty(Rect::new(0, 0, 1, 1));
    (&area).render(rect, &mut buf);
}

#[test]
fn page_moves_use_the_rendered_height() {
    let mut area = TextArea::from_text("0\n1\n2\n3\n4\n5\n6\n7\n8\n9");
    render_to_lines(&area, 4, 3);
    area.set_cursor(9, 0);
    area.move_cursor(CursorMove::UpBy(0));
    assert_eq!(area.cursor(), (6, 0));
    area.move_cursor(CursorMove::DownBy(0));
    assert_eq!(area.cursor(), (9, 0));
}

#[test]
fn explicit_page_sizes_clamp_at_the_buffer_edges() {
    let mut area = TextArea::from_text("0\n1\n2");
    area.set_cursor(0, 0);
    area.move_cursor(CursorMove::UpBy(5));
    assert_eq!(area.cursor(), (0, 0));
    area.move_cursor(CursorMove::DownBy(5));
    assert_eq!(area.cursor(), (2, 0));
}

#[test]
fn clear_empties_the_buffer() {
    let mut area = TextArea::from_text("a\nb");
    area.clear();
    assert!(area.is_empty());
    assert_eq!(area.cursor(), (0, 0));
    assert_eq!(area.line_count(), 1);
}

#[test]
fn line_accessors_report_the_buffer() {
    let area = TextArea::from_text("one\ntwo");
    assert_eq!(area.line_count(), 2);
    assert_eq!(area.line(1), Some("two"));
    assert_eq!(area.line(2), None);
}

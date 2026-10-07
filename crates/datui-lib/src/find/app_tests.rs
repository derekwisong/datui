use super::*;
use crate::table::DataTableState;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

fn key(app: &mut App, code: KeyCode) {
    key_with(app, code, KeyModifiers::NONE);
}

fn key_with(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, modifiers)));
    while let Some(event) = next {
        next = app.event(&event);
    }
}

fn type_text(app: &mut App, text: &str) {
    for c in text.chars() {
        key(app, KeyCode::Char(c));
    }
}

/// Handle what the workers send until nothing is owed.
fn settle(app: &mut App, rx: &Receiver<AppEvent>) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let event = match rx.try_recv() {
            Ok(event) => event,
            Err(_) if app.count_waits_for_a_frame() => AppEvent::FramePainted,
            Err(_) if !crate::tests::work_pending(app) => return,
            Err(_) => {
                assert!(Instant::now() < deadline, "the find never answered");
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(event) => event,
                    Err(_) => continue,
                }
            }
        };
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
    }
}

/// An app over `df` with its first buffer read, ten rows on screen.
fn app_over(df: DataFrame) -> (App, Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let mut state =
        DataTableState::from_lazyframe(df.lazy(), &crate::OpenOptions::default()).unwrap();
    state.visible_rows = 10;
    app.data_table_state = Some(state);
    app.spawn_async_collect("Loading");
    settle(&mut app, &rx);
    (app, rx)
}

fn haystack(rows: usize, needles: &[usize]) -> DataFrame {
    let values: Vec<String> = (0..rows)
        .map(|i| {
            if needles.contains(&i) {
                format!("needle {i}")
            } else {
                format!("hay {i}")
            }
        })
        .collect();
    df!("id" => (0..rows as i64).collect::<Vec<_>>(), "v" => values).unwrap()
}

fn cursor(app: &App) -> usize {
    app.data_table_state.as_ref().unwrap().cursor_row()
}

fn find(app: &mut App, rx: &Receiver<AppEvent>, pattern: &str) {
    key(app, KeyCode::Char('f'));
    assert_eq!(app.input_type, Some(InputType::Find));
    type_text(app, pattern);
    key(app, KeyCode::Enter);
    settle(app, rx);
}

/// Enter on an emptied field takes the find back (#644): no mark, no hit, and `n`
/// has nothing to repeat.
#[test]
fn an_emptied_find_clears_the_find() {
    let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
    find(&mut app, &rx, "needle");
    assert!(app.find_mark().is_some());

    key(&mut app, KeyCode::Char('f'));
    // The old pattern is selected, so Backspace empties the field.
    key(&mut app, KeyCode::Backspace);
    assert_eq!(app.find.input.value(), "");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(app.find_mark(), None);
    assert_eq!(app.find_hit(), None);

    let at = cursor(&app);
    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), at, "n does not move");
    assert_eq!(app.flash_message(), Some("Nothing to find yet: / finds"));
}

/// Esc at the table clears a find before it backs out of anything else (#644).
#[test]
fn esc_at_the_table_clears_the_find() {
    let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
    find(&mut app, &rx, "needle");
    assert!(app.find_mark().is_some());
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.find_mark(), None);
    assert_eq!(app.find_hit(), None);
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn f_then_n_and_capital_n_walk_the_matches_past_the_buffer() {
    let (mut app, rx) = app_over(haystack(300_000, &[5, 250_123]));
    let buffered_end = app.data_table_state.as_ref().unwrap().buffered_end();
    assert!(buffered_end < 250_123, "the far match is past the buffer");

    find(&mut app, &rx, "needle");
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(cursor(&app), 5);
    assert_eq!(app.find_hit(), Some((5, "v".to_string())));
    assert_eq!(
        app.find_mark().as_deref(),
        Some("find \"needle\" · match 1")
    );

    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), 250_123);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        (state.buffered_start()..state.buffered_end()).contains(&250_123),
        "the rows around the match were read"
    );
    assert_eq!(
        app.find_mark().as_deref(),
        Some("find \"needle\" · match 2")
    );

    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), 5);
    assert_eq!(app.flash_message(), Some("Wrapped to the top"));
    assert_eq!(
        app.find_mark().as_deref(),
        Some("find \"needle\" · match 1")
    );

    key(&mut app, KeyCode::Char('N'));
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), 250_123);
    assert_eq!(app.flash_message(), Some("Wrapped to the bottom"));
    assert_eq!(
        app.find_mark().as_deref(),
        Some("find \"needle\""),
        "round from the bottom, which match it is is not known"
    );
}

#[test]
fn the_view_is_searched_and_left_as_it_was() {
    let (mut app, rx) = app_over(haystack(1_000, &[3, 700]));
    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], false);
    settle(&mut app, &rx);
    let frame = app.data_table_state.as_ref().unwrap().len_generation();
    find(&mut app, &rx, "needle");
    let state = app.data_table_state.as_ref().unwrap();
    // Descending: row 299 of the view is id 700.
    assert_eq!(cursor(&app), 299);
    assert_eq!(state.len_generation(), frame, "the view is the same frame");
    assert_eq!(state.get_sort_columns(), ["id".to_string()]);
}

#[test]
fn esc_stops_a_find_and_the_cursor_stays() {
    let (mut app, rx) = app_over(haystack(300_000, &[250_123]));
    key(&mut app, KeyCode::Char('f'));
    type_text(&mut app, "needle");
    key(&mut app, KeyCode::Enter);
    assert!(app.finding() && app.is_busy());
    assert!(app.hard_escape_while_busy(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
    key(&mut app, KeyCode::Esc);
    assert!(!app.finding());
    assert!(!app.is_busy(), "the keys are the user's again");
    assert_eq!(app.flash_message(), Some(CANCELLED));
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), 0, "a cancelled find moves nothing");
    assert_eq!(app.find_hit(), None);
}

#[test]
fn ctrl_o_stops_a_find_on_the_way_home() {
    let (mut app, rx) = app_over(haystack(300_000, &[250_123]));
    find_started(&mut app);
    key_with(&mut app, KeyCode::Char('o'), KeyModifiers::CONTROL);
    assert!(!app.finding());
    assert_eq!(app.input_mode, InputMode::Home);
    settle(&mut app, &rx);
    assert_eq!(cursor(&app), 0);
}

fn find_started(app: &mut App) {
    key(app, KeyCode::Char('f'));
    type_text(app, "needle");
    key(app, KeyCode::Enter);
    assert!(app.finding());
}

#[test]
fn the_prompt_toggles_regex_and_column_and_says_why_a_regex_is_bad() {
    let (mut app, rx) = app_over(haystack(20, &[]));
    key(&mut app, KeyCode::Char('f'));
    key_with(&mut app, KeyCode::Char('r'), KeyModifiers::CONTROL);
    key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
    assert!(app.find.regex && app.find.in_column);
    assert_eq!(app.find.column.as_deref(), Some("id"));
    type_text(&mut app, "(1");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.input_type, Some(InputType::Find), "it stays open");
    assert!(
        app.find
            .error
            .as_deref()
            .unwrap()
            .starts_with("Not a regex")
    );
    key(&mut app, KeyCode::Backspace);
    key(&mut app, KeyCode::Backspace);
    assert!(app.find.error.is_none(), "an edit clears the reason");
    // Only in `id`: "hay 12" in `v` is not a match for `^12$`.
    type_text(&mut app, "^12$");
    key(&mut app, KeyCode::Enter);
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), Some((12, "id".to_string())));
    assert_eq!(
        app.find_mark().as_deref(),
        Some("find /^12$/ in id · match 1")
    );
}

/// Ctrl+L limits a find to the column cursor's column, and a match moves the
/// column cursor to the found cell's column.
#[test]
fn the_column_cursor_is_the_find_column_and_a_match_moves_it() {
    let df = df!(
            "id" => (0..20i64).collect::<Vec<_>>(),
            "v" => (0..20).map(|i| format!("hay {i}")).collect::<Vec<_>>(),
            "w" => (0..20).map(|i| if i == 7 { "needle".to_string() } else { format!("w {i}") }).collect::<Vec<_>>(),
        )
        .unwrap();
    let (mut app, rx) = app_over(df);
    let current = |app: &App| {
        app.data_table_state
            .as_ref()
            .unwrap()
            .current_column()
            .map(str::to_string)
    };
    key(&mut app, KeyCode::Char('l'));
    assert_eq!(current(&app).as_deref(), Some("v"));
    key(&mut app, KeyCode::Char('f'));
    key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
    assert_eq!(app.find.column.as_deref(), Some("v"));
    type_text(&mut app, "needle");
    key(&mut app, KeyCode::Enter);
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), None, "not in v");

    // Every column: the match is in `w`, and the column cursor goes there.
    key(&mut app, KeyCode::Char('f'));
    key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
    key(&mut app, KeyCode::Enter);
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), Some((7, "w".to_string())));
    assert_eq!(cursor(&app), 7);
    assert_eq!(current(&app).as_deref(), Some("w"));
    // The next limited find opens on it.
    key(&mut app, KeyCode::Char('f'));
    assert_eq!(app.find.column.as_deref(), Some("w"));
}

/// `n` and `N` go on from the cursor's cell: moved along the found row, the next
/// match is the one to the cursor's right, the previous the one to its left.
#[test]
fn n_and_capital_n_start_from_the_cursors_cell() {
    let cell = |hit: bool, i: usize| {
        if hit {
            "needle".to_string()
        } else {
            format!("hay {i}")
        }
    };
    let df = df!(
        "a" => (0..20).map(|i| cell(i == 2, i)).collect::<Vec<_>>(),
        "b" => (0..20).map(|i| cell(i == 2, i)).collect::<Vec<_>>(),
        "c" => (0..20).map(|i| cell(false, i)).collect::<Vec<_>>(),
        "d" => (0..20).map(|i| cell(i == 2 || i == 9, i)).collect::<Vec<_>>(),
    )
    .unwrap();
    let (mut app, rx) = app_over(df);
    find(&mut app, &rx, "needle");
    assert_eq!(app.find_hit(), Some((2, "a".to_string())));
    // To `c`, past the match in `b`.
    key(&mut app, KeyCode::Char('l'));
    key(&mut app, KeyCode::Char('l'));
    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), Some((2, "d".to_string())), "right of c");
    // Back to `c`: the previous match is `b`, left of it.
    key(&mut app, KeyCode::Char('h'));
    key(&mut app, KeyCode::Char('N'));
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), Some((2, "b".to_string())), "left of c");
    // Down the rows: from row 5, the next is row 9, not the rest of row 2.
    for _ in 0..3 {
        key(&mut app, KeyCode::Char('j'));
    }
    assert_eq!(cursor(&app), 5);
    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), Some((9, "d".to_string())));
}

#[test]
fn no_match_says_so_and_nothing_moves() {
    let (mut app, rx) = app_over(haystack(50, &[]));
    key(&mut app, KeyCode::Char('j'));
    find(&mut app, &rx, "needle");
    assert_eq!(cursor(&app), 1);
    assert_eq!(app.flash_message(), Some("No match for \"needle\""));
}

#[test]
fn n_before_any_find_says_how_to_start_one() {
    let (mut app, _rx) = app_over(haystack(5, &[]));
    key(&mut app, KeyCode::Char('N'));
    assert_eq!(app.flash_message(), Some("Nothing to find yet: / finds"));
    assert!(
        !app.data_table_state.as_ref().unwrap().row_numbers(),
        "N no longer toggles row numbers"
    );
}

#[test]
fn hash_toggles_row_numbers() {
    let (mut app, _rx) = app_over(haystack(5, &[]));
    key(&mut app, KeyCode::Char('#'));
    assert!(app.data_table_state.as_ref().unwrap().row_numbers());
    key(&mut app, KeyCode::Char('#'));
    assert!(!app.data_table_state.as_ref().unwrap().row_numbers());
}

/// `<` `>` `=` `w` at the table set the column cursor's width at once (#647).
#[test]
fn width_keys_set_the_column_cursors_width() {
    use crate::widgets::column_widths::{UNSEEN_WIDTH, WIDTH_STEP, WidthChoice};
    let (mut app, _rx) = app_over(haystack(5, &[]));
    let name = app
        .data_table_state
        .as_ref()
        .unwrap()
        .current_column()
        .unwrap()
        .to_string();
    let width = |app: &App| app.data_table_state.as_ref().unwrap().width_choice(&name);
    let start = app
        .data_table_state
        .as_ref()
        .unwrap()
        .on_screen_width(&name)
        .unwrap_or(UNSEEN_WIDTH);
    key(&mut app, KeyCode::Char('>'));
    assert_eq!(width(&app), WidthChoice::Manual(start + WIDTH_STEP));
    key(&mut app, KeyCode::Char('<'));
    assert_eq!(width(&app), WidthChoice::Manual(start));
    key(&mut app, KeyCode::Char('='));
    assert_eq!(width(&app), WidthChoice::Fit);
    key(&mut app, KeyCode::Char('w'));
    assert_eq!(width(&app), WidthChoice::Auto);
}

/// After the view changes under it, `n` starts from the cursor in the new view:
/// the cell it landed on belongs to a frame that is gone.
#[test]
fn n_after_the_view_changes_starts_from_the_cursor_in_the_new_view() {
    let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
    find(&mut app, &rx, "needle");
    assert_eq!(app.find_hit(), Some((5, "v".to_string())));
    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], false);
    settle(&mut app, &rx);
    assert_eq!(app.find_hit(), None, "the old cell is not this view's");
    assert_eq!(app.find_mark().as_deref(), Some("find \"needle\""));
    let from = cursor(&app);
    key(&mut app, KeyCode::Char('n'));
    settle(&mut app, &rx);
    // Descending: id 700 is view row 299, id 5 row 994.
    let expected = if from < 299 { 299 } else { 994 };
    assert_eq!(app.find_hit(), Some((expected, "v".to_string())));
    assert_eq!(cursor(&app), expected);
}

/// The found cell is drawn in the theme's find slot, on the cursor's row, and
/// nowhere once the cursor moves off it; with row numbers shown too.
#[test]
fn the_found_cell_is_highlighted_on_the_cursor_row() {
    for row_numbers in [false, true] {
        found_cell_is_highlighted(row_numbers);
    }
}

fn found_cell_is_highlighted(row_numbers: bool) {
    use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    let (mut app, rx) = app_over(haystack(30, &[4]));
    if row_numbers {
        key(&mut app, KeyCode::Char('#'));
    }
    find(&mut app, &rx, "needle 4");
    let style = app.theme.find_match_style();
    let draw = |app: &mut App| {
        let area = Rect::new(0, 0, 80, 24);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf
    };
    let buf = draw(&mut app);
    let marked: Vec<(u16, u16)> = buf
        .content()
        .iter()
        .enumerate()
        .filter(|(_, cell)| cell.bg == style.bg.unwrap())
        .map(|(i, _)| ((i % 80) as u16, (i / 80) as u16))
        .collect();
    assert!(!marked.is_empty(), "the cell is marked");
    let y = marked[0].1;
    let row: String = (0..80).map(|x| buf[(x, y)].symbol().to_string()).collect();
    assert!(row.contains("needle 4"), "{row}");
    let text: String = marked
        .iter()
        .map(|&(x, y)| buf[(x, y)].symbol().to_string())
        .collect();
    assert!(
        text.contains("needle 4"),
        "the mark is on the value: {text:?}"
    );

    // The found cell is the current cell, drawn as found rather than as the cell
    // cursor; the header keeps the cell cursor's mark.
    // The cell cursor's look on this terminal: its tint, or reversed where the
    // tint cannot show.
    let cell_cursor = app.theme.cell_cursor_style();
    let is_cell_cursor = |cell: &ratatui::buffer::Cell| match cell_cursor.bg {
        Some(bg) => cell.bg == bg,
        None => cell.modifier.contains(ratatui::style::Modifier::REVERSED),
    };
    assert!(
        marked.iter().all(|&(x, y)| !is_cell_cursor(&buf[(x, y)])),
        "one style on the found cell"
    );
    assert!(
        (0..80).any(|x| is_cell_cursor(&buf[(x, 0)])),
        "the header carries the cursor"
    );

    // The column cursor off it: the cell is plain, and the new current cell is the
    // cell cursor's.
    key(&mut app, KeyCode::Char('h'));
    let buf = draw(&mut app);
    assert!(
        !buf.content()
            .iter()
            .any(|cell| cell.bg == style.bg.unwrap()),
        "off its column, the cell is plain"
    );
    assert!((0..80).any(|x| is_cell_cursor(&buf[(x, y)])));
    key(&mut app, KeyCode::Char('l'));
    assert!(
        draw(&mut app)
            .content()
            .iter()
            .any(|cell| cell.bg == style.bg.unwrap()),
        "back on it, marked again"
    );

    key(&mut app, KeyCode::Char('j'));
    let buf = draw(&mut app);
    assert!(
        !buf.content()
            .iter()
            .any(|cell| cell.bg == style.bg.unwrap()),
        "off its row, the cell is plain"
    );
}

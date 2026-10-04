//! End-to-end coverage of every place the app puts a text field in front of the
//! user.
//!
//! The widget tests in `widgets::text_input` cover the field in isolation.
//! These drive the real `App` instead, one key event at a time, so that each
//! prior use of the old wrapped-textarea widget has a test that says what the
//! user sees: the command line (rows, SQL and q), the export path, the
//! chart axis filters, the sort and pivot filters, and the multi-line view
//! description.

use std::io::Write;
use std::sync::mpsc::{self, Receiver, Sender};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use crate::widgets::text_input::TextInput;
use crate::{App, AppEvent, InputMode, OpenOptions};

/// An app with a small CSV loaded, driven the way the real event loop drives it.
///
/// Keys are delivered one at a time, and every follow-up the app asks for is
/// run before the next key: chained events immediately, background results as
/// they arrive on the channel. Without that the app stays busy and drops keys,
/// which is exactly what a user would never see.
struct Harness {
    app: App,
    rx: Receiver<AppEvent>,
    _dir: tempfile::TempDir,
}

impl Harness {
    fn with_data() -> Self {
        Self::with_csv("name,age\nada,36\ngrace,45\nalan,41")
    }

    fn with_csv(contents: &str) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("people.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "{contents}").expect("write csv");
        drop(file);

        let (tx, rx): (Sender<AppEvent>, Receiver<AppEvent>) = mpsc::channel();
        let app = App::new(tx, crate::tests::test_runtime());
        let mut harness = Harness { app, rx, _dir: dir };
        harness.run(AppEvent::Open(vec![path], OpenOptions::default()));
        assert!(
            harness.app.data_table_state.is_some(),
            "the CSV should have loaded"
        );
        harness
    }

    /// Loaded, with `:` preferring q.
    fn q_style() -> Self {
        let mut h = Self::with_data();
        h.app.app_config.query.default_mode = crate::QueryMode::Q;
        h
    }

    /// Run `event` and everything that follows from it.
    fn run(&mut self, event: AppEvent) {
        self.run_until(Some(event), |_| false);
    }

    /// Run `event`, if any, and what follows from it until `stop` holds between two
    /// events or nothing is left to do. A result still out when it stops waits on the
    /// channel until the next run handles it.
    fn run_until(&mut self, event: Option<AppEvent>, stop: impl Fn(&App) -> bool) {
        let mut next = event;
        // Waits on the work, not on a quiet spell; the deadline is only a hang guard.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
        loop {
            if let Some(event) = next.take() {
                if let AppEvent::Crash(message) = &event {
                    panic!("the app crashed: {message}");
                }
                next = self.app.event(&event);
                continue;
            }
            if stop(&self.app) {
                break;
            }
            if let Ok(event) = self.rx.try_recv() {
                next = Some(event);
                continue;
            }
            // The run loop paints after every update, and a count waiting on that starts.
            if self.app.count_waits_for_a_frame() {
                next = Some(AppEvent::FramePainted);
                continue;
            }
            if crate::tests::work_pending(&self.app) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "background work never reported back"
                );
                next = self
                    .rx
                    .recv_timeout(std::time::Duration::from_millis(50))
                    .ok();
                continue;
            }
            break;
        }
    }

    fn press(&mut self, code: KeyCode) {
        self.press_with(code, KeyModifiers::NONE);
    }

    fn press_with(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        self.run(AppEvent::Key(KeyEvent::new(code, modifiers)));
    }

    fn type_str(&mut self, text: &str) {
        for c in text.chars() {
            self.press(KeyCode::Char(c));
        }
    }
}

/// The single visible line of a field, as the terminal would show it.
fn drawn(input: &TextInput, width: u16) -> String {
    let rect = Rect::new(0, 0, width, 1);
    let mut buf = Buffer::empty(rect);
    input.render(rect, &mut buf);
    (0..width)
        .map(|x| buf[(x, 0)].symbol().to_string())
        .collect::<String>()
        .trim_end()
        .to_string()
}

// ---------------------------------------------------------------------------
// The query bar
// ---------------------------------------------------------------------------

#[test]
fn typing_a_query_and_submitting_it_applies_the_query() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    assert_eq!(h.app.input_mode, InputMode::Editing);

    h.type_str("select name where age > 40");
    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    assert_eq!(drawn(&h.app.query_input, 40), "select name where age > 40");

    h.press(KeyCode::Enter);
    assert_eq!(h.app.input_mode, InputMode::Normal);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.get_active_query(), "select name where age > 40");
}

#[test]
fn reopening_the_query_bar_shows_the_query_that_is_running() {
    // Reopening has to put the live query back on screen, not the blank field
    // that Esc left behind: the user edits from where they were.
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    h.press(KeyCode::Esc);
    h.press(KeyCode::Char(':'));

    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    assert_eq!(drawn(&h.app.query_input, 40), "select name where age > 40");
    assert_eq!(
        h.app.query_input.cursor(),
        "select name where age > 40".chars().count()
    );
}

#[test]
fn esc_leaves_the_query_bar_without_running_anything() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Esc);

    assert_eq!(h.app.input_mode, InputMode::Normal);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.get_active_query(), "");
}

#[test]
fn editing_keys_work_in_the_query_bar() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("select nme");
    h.press(KeyCode::Backspace);
    h.press(KeyCode::Backspace);
    h.type_str("ame");
    assert_eq!(h.app.query_input.value(), "select name");

    h.press(KeyCode::Home);
    assert_eq!(h.app.query_input.cursor(), 0);
    h.press(KeyCode::Delete);
    assert_eq!(h.app.query_input.value(), "elect name");
    h.press(KeyCode::End);
    assert_eq!(h.app.query_input.cursor(), 10);
}

#[test]
fn the_query_bar_recalls_earlier_queries_with_the_arrow_keys() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    h.press(KeyCode::Char(':'));
    h.press(KeyCode::Up);
    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    assert_eq!(drawn(&h.app.query_input, 40), "select name where age > 40");
}

#[test]
fn the_command_line_goes_to_a_row_and_keeps_the_query() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    // Reopened, the line holds the query in effect, selected: digits replace it.
    query_screen(&mut h.app);
    h.press(KeyCode::Char(':'));
    assert_eq!(h.app.input_mode, InputMode::Editing);
    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    h.type_str("2");
    assert_eq!(h.app.query_input.value(), "2");
    assert_eq!(crate::render::input_strip::prefix(&h.app), "row:");
    h.press(KeyCode::Enter);
    assert_eq!(h.app.input_mode, InputMode::Normal);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.cursor_row(), 1, "row 2 of the view");
    assert_eq!(
        state.get_active_query(),
        "select name where age > 40",
        "a row number runs nothing"
    );
}

// ---------------------------------------------------------------------------
// Modal fields
// ---------------------------------------------------------------------------

/// Put the Sort tab's column filter under the cursor.
fn focus_column_filter(app: &mut App) {
    app.sort_filter_modal.active_tab = crate::sort_filter_modal::SortFilterTab::Columns;
    app.sort_filter_modal.focus = crate::sort_filter_modal::SortFilterField::Find;
}

#[test]
fn the_sort_and_filter_modal_filters_columns_as_you_type() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('s'));
    assert_eq!(h.app.input_mode, InputMode::SortFilter);
    focus_column_filter(&mut h.app);

    h.type_str("ag");
    assert_eq!(h.app.sort_filter_modal.sort.filter_input.value(), "ag");
    assert_eq!(drawn(&h.app.sort_filter_modal.sort.filter_input, 20), "ag");

    h.press(KeyCode::Backspace);
    assert_eq!(h.app.sort_filter_modal.sort.filter_input.value(), "a");
}

#[test]
fn the_view_description_holds_several_lines() {
    let mut h = Harness::with_data();

    // The save gate wants something to save; sort first.
    h.app
        .data_table_state
        .as_mut()
        .expect("data loaded")
        .sort_by(vec!["age".to_string()], vec![false]);
    h.press(KeyCode::Char('v'));
    h.press(KeyCode::Char('s'));
    // Move focus from the name field to the description.
    h.press(KeyCode::Tab);

    h.type_str("first");
    h.press(KeyCode::Enter);
    h.type_str("second");

    let description = &h.app.view_modal.description_input;
    assert_eq!(description.value(), "first\nsecond");
    assert_eq!(description.line_count(), 2);
    assert_eq!(description.cursor_line(), 1);
}

#[test]
fn the_view_description_pages_through_its_lines() {
    let mut h = Harness::with_data();

    // The save gate wants something to save; sort first.
    h.app
        .data_table_state
        .as_mut()
        .expect("data loaded")
        .sort_by(vec!["age".to_string()], vec![false]);
    h.press(KeyCode::Char('v'));
    h.press(KeyCode::Char('s'));
    h.press(KeyCode::Tab);

    for line in 0..8 {
        h.type_str(&format!("line{line}"));
        if line < 7 {
            h.press(KeyCode::Enter);
        }
    }
    assert_eq!(h.app.view_modal.description_input.cursor_line(), 7);

    h.press(KeyCode::PageUp);
    assert_eq!(h.app.view_modal.description_input.cursor_line(), 2);
    h.press(KeyCode::PageUp);
    assert_eq!(h.app.view_modal.description_input.cursor_line(), 0);
    h.press(KeyCode::PageDown);
    assert_eq!(h.app.view_modal.description_input.cursor_line(), 5);
}

#[test]
fn the_view_name_field_takes_text() {
    let mut h = Harness::with_data();

    // The save gate wants something to save; sort first.
    h.app
        .data_table_state
        .as_mut()
        .expect("data loaded")
        .sort_by(vec!["age".to_string()], vec![false]);
    h.press(KeyCode::Char('v'));
    h.press(KeyCode::Char('s'));
    // Creating from a loaded file suggests a name; typing replaces it.
    let suggested = h.app.view_modal.name_input.value().to_string();
    assert!(!suggested.is_empty(), "a name should be suggested");
    h.type_str("by age");

    assert_eq!(h.app.view_modal.name_input.value(), "by age");
    assert_eq!(drawn(&h.app.view_modal.name_input, 20), "by age");

    // Opened again, → keeps the suggestion and typing extends it.
    h.press(KeyCode::Esc);
    h.press(KeyCode::Char('s'));
    assert_eq!(h.app.view_modal.name_input.value(), suggested);
    h.press(KeyCode::Right);
    h.type_str(" v2");
    assert_eq!(
        h.app.view_modal.name_input.value(),
        format!("{suggested} v2")
    );
}

#[test]
fn the_melt_name_fields_replace_their_defaults_when_typed_over() {
    use crate::pivot_melt_modal::PivotMeltFocus;
    let mut h = Harness::with_data();
    let to_variable_row = |h: &mut Harness| {
        h.press(KeyCode::Char('p'));
        h.press(KeyCode::Right);
        while h.app.pivot_melt_modal.focus != PivotMeltFocus::MeltVariable {
            h.press(KeyCode::Tab);
        }
    };

    to_variable_row(&mut h);
    assert_eq!(
        h.app.pivot_melt_modal.melt_variable_input.value(),
        "variable"
    );
    h.type_str("field");
    h.press(KeyCode::Tab);
    h.type_str("reading");
    assert_eq!(h.app.pivot_melt_modal.melt_variable_input.value(), "field");
    assert_eq!(h.app.pivot_melt_modal.melt_value_input.value(), "reading");

    // Opened again, → keeps the default and typing extends it; Tab past a
    // default leaves it as it was.
    h.press(KeyCode::Esc);
    to_variable_row(&mut h);
    h.press(KeyCode::Right);
    h.type_str("_name");
    h.press(KeyCode::Tab);
    assert_eq!(
        h.app.pivot_melt_modal.melt_variable_input.value(),
        "variable_name"
    );
    assert_eq!(h.app.pivot_melt_modal.melt_value_input.value(), "value");
}

#[test]
fn unicode_survives_a_round_trip_through_a_field() {
    let mut h = Harness::q_style();

    h.press(KeyCode::Char(':'));
    h.type_str("where name == \"café\"");
    assert_eq!(h.app.query_input.value(), "where name == \"café\"");
    h.press(KeyCode::Backspace);
    assert_eq!(h.app.query_input.value(), "where name == \"café");
}

#[test]
fn every_field_starts_empty_and_unfocused() {
    // The modals build their fields fresh each time they open; a field that
    // kept the last value would leak one modal's text into the next.
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('s'));
    focus_column_filter(&mut h.app);
    h.type_str("ag");
    assert_eq!(h.app.sort_filter_modal.sort.filter_input.value(), "ag");
    h.press(KeyCode::Esc);
    h.press(KeyCode::Char('s'));

    assert_eq!(h.app.sort_filter_modal.sort.filter_input.value(), "");
}

#[test]
fn the_export_modal_takes_a_path() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('e'));
    assert_eq!(h.app.input_mode, InputMode::Export);

    // The modal suggests a filename; replace it with one of our own.
    let suggested = h.app.export_modal.path_input.value().to_string();
    for _ in 0..suggested.chars().count() {
        h.press(KeyCode::Backspace);
    }
    h.type_str("out.csv");

    assert_eq!(h.app.export_modal.path_input.value(), "out.csv");
    assert_eq!(drawn(&h.app.export_modal.path_input, 30), "out.csv");
}

#[test]
fn the_pivot_and_melt_modal_opens_a_picker_narrowed_as_you_type() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('p'));
    assert_eq!(h.app.input_mode, InputMode::PivotMelt);
    h.app.pivot_melt_modal.focus = crate::pivot_melt_modal::PivotMeltFocus::PivotIndex;

    // Space opens a picked row's Picker; typing narrows it.
    h.press(KeyCode::Char(' '));
    h.type_str("na");
    let picker = h.app.pivot_melt_modal.picker.as_ref().expect("picker open");
    assert_eq!(picker.filter, "na");
    assert!(picker.filtered().iter().all(|(_, c)| c.contains("na")));
}

/// Ctrl+T switches between SQL and q, the text going with it; the choice is
/// remembered the next time the line opens on no query.
#[test]
fn ctrl_t_switches_the_language_and_keeps_the_text() {
    let mut h = Harness::with_data();
    h.press(KeyCode::Char(':'));
    let first = h.app.query_mode;
    h.type_str("abc");
    h.press_with(KeyCode::Char('t'), KeyModifiers::CONTROL);
    let second = h.app.query_mode;
    if crate::QueryMode::available().len() > 1 {
        assert_ne!(second, first);
    }
    h.type_str("d");
    assert_eq!(
        h.app.query_prompt_text(),
        Some("abcd"),
        "the chord typed nothing"
    );
    h.press(KeyCode::Esc);
    h.press(KeyCode::Char(':'));
    assert_eq!(h.app.query_mode, second, "remembered");
}

/// The query in effect arrives selected; switched to the other language it stays
/// selected, so typing there states a new query rather than appending to the old.
#[cfg(feature = "sql")]
#[test]
fn ctrl_t_over_the_restored_query_keeps_it_selected() {
    let mut h = Harness::q_style();
    h.press(KeyCode::Char(':'));
    h.type_str("select name");
    h.press(KeyCode::Enter);
    h.press(KeyCode::Char(':'));
    h.press_with(KeyCode::Char('t'), KeyModifiers::CONTROL);
    assert_eq!(h.app.query_mode, crate::QueryMode::Sql);
    h.type_str("SELECT age FROM df");
    assert_eq!(h.app.query_prompt_text(), Some("SELECT age FROM df"));
}

/// Esc closes the prompt whole from every mode. The q path used to leave
/// the prompt's input type behind.
#[test]
fn esc_closes_the_query_prompt_from_every_mode() {
    let mut h = Harness::with_data();
    for &mode in crate::QueryMode::available() {
        h.press(KeyCode::Char(':'));
        while h.app.query_mode != mode {
            h.press_with(KeyCode::Char('t'), KeyModifiers::CONTROL);
        }
        h.press(KeyCode::Esc);
        assert_eq!(h.app.input_mode, InputMode::Normal, "{mode:?}");
        assert_eq!(h.app.input_type, None, "{mode:?}");
        assert_eq!(h.app.query_prompt_mode(), None, "{mode:?}");
    }
}

fn query_screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    buf.content().iter().map(|c| c.symbol()).collect()
}

/// As a find is typed, the cells it matches among the rows on hand light up and
/// are counted; Ctrl+G keeps only the rows that match, as a filter.
#[test]
fn a_find_lights_up_matches_and_keeps_them() {
    let mut h = Harness::with_data();
    query_screen(&mut h.app);
    h.press(KeyCode::Char('/'));
    h.type_str("al");
    assert_eq!(h.app.live_on_screen(), Some(1), "alan");
    let drawn = query_screen(&mut h.app);
    assert!(drawn.contains("1 on screen"), "{drawn}");
    h.press_with(KeyCode::Char('g'), KeyModifiers::CONTROL);
    assert_eq!(h.app.input_mode, InputMode::Normal);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.num_rows(), 1, "only alan's row has al");
    assert_eq!(state.view_filters()[0].describe(), "has \"al\"");
    let drawn = query_screen(&mut h.app);
    assert!(drawn.contains("has \"al\""), "the footer shows it: {drawn}");
}

/// A prompt on a terminal too short for it draws what fits and nothing past the
/// screen: the status line first.
#[test]
fn a_prompt_on_a_tiny_terminal_draws_what_fits() {
    let mut h = Harness::with_data();
    for open in ['/', ':'] {
        h.press(KeyCode::Char(open));
        h.type_str("al");
        for (width, height) in [(1, 1), (20, 1), (20, 2), (40, 3), (5, 4)] {
            let area = Rect::new(0, 0, width, height);
            let mut buf = Buffer::empty(area);
            h.app.render(area, &mut buf);
        }
        h.press(KeyCode::Esc);
    }
}

/// Kept over every column, a find searches the columns it searched: a hidden one
/// keeps no row.
#[test]
fn a_kept_find_searches_the_shown_columns() {
    let mut h = Harness::with_data();
    h.run(AppEvent::ColumnOrder(vec!["age".to_string()], 0));
    h.press(KeyCode::Char('/'));
    h.type_str("al");
    h.press_with(KeyCode::Char('g'), KeyModifiers::CONTROL);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.num_rows(), 0, "name is hidden, so alan is not kept");
}

/// Letters in order: `gce` finds grace.
#[test]
fn a_fuzzy_find_matches_letters_in_order() {
    let mut h = Harness::with_data();
    query_screen(&mut h.app);
    h.press(KeyCode::Char('/'));
    h.press_with(KeyCode::Char('t'), KeyModifiers::CONTROL);
    h.type_str("gce");
    assert_eq!(h.app.live_on_screen(), Some(1));
    h.press_with(KeyCode::Char('g'), KeyModifiers::CONTROL);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.num_rows(), 1);
}

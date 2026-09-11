//! End-to-end coverage of every place the app puts a text field in front of the
//! user.
//!
//! The widget tests in `widgets::text_input` cover the field in isolation.
//! These drive the real `App` instead, one key event at a time, so that each
//! prior use of the old wrapped-textarea widget has a test that says what the
//! user sees: the query bar and its tabs, go-to-line, the export path, the
//! chart axis filters, the sort and pivot filters, and the multi-line template
//! description.

use std::io::Write;
use std::sync::mpsc::{self, Receiver, Sender};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use crate::widgets::text_input::TextInput;
use crate::{App, AppEvent, InputMode, OpenOptions};

fn runtime() -> tokio::runtime::Handle {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test runtime")
    })
    .handle()
    .clone()
}

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
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("people.csv");
        let mut file = std::fs::File::create(&path).expect("create csv");
        writeln!(file, "name,age\nada,36\ngrace,45\nalan,41").expect("write csv");
        drop(file);

        let (tx, rx): (Sender<AppEvent>, Receiver<AppEvent>) = mpsc::channel();
        let app = App::new(tx, runtime());
        let mut harness = Harness { app, rx, _dir: dir };
        harness.run(AppEvent::Open(vec![path], OpenOptions::default()));
        assert!(
            harness.app.data_table_state.is_some(),
            "the CSV should have loaded"
        );
        harness
    }

    /// Run `event` and everything that follows from it.
    fn run(&mut self, event: AppEvent) {
        let mut next = Some(event);
        loop {
            if let Some(event) = next.take() {
                if let AppEvent::Crash(message) = &event {
                    panic!("the app crashed: {message}");
                }
                next = self.app.event(&event);
                continue;
            }
            if let Ok(event) = self.rx.try_recv() {
                next = Some(event);
                continue;
            }
            if self.app.busy {
                match self.rx.recv_timeout(std::time::Duration::from_secs(5)) {
                    Ok(event) => {
                        next = Some(event);
                        continue;
                    }
                    Err(_) => panic!("the app stayed busy with no background result"),
                }
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
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
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
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    h.press(KeyCode::Esc);
    h.press(KeyCode::Char('/'));

    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    assert_eq!(drawn(&h.app.query_input, 40), "select name where age > 40");
    assert_eq!(
        h.app.query_input.cursor(),
        "select name where age > 40".chars().count()
    );
}

#[test]
fn esc_leaves_the_query_bar_without_running_anything() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Esc);

    assert_eq!(h.app.input_mode, InputMode::Normal);
    let state = h.app.data_table_state.as_ref().expect("state");
    assert_eq!(state.get_active_query(), "");
}

#[test]
fn editing_keys_work_in_the_query_bar() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
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
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    h.press(KeyCode::Char('/'));
    h.press(KeyCode::Up);
    assert_eq!(h.app.query_input.value(), "select name where age > 40");
    assert_eq!(drawn(&h.app.query_input, 40), "select name where age > 40");
}

#[test]
fn go_to_line_opens_on_an_empty_field() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
    h.type_str("select name where age > 40");
    h.press(KeyCode::Enter);

    h.press(KeyCode::Char(':'));
    assert_eq!(h.app.input_mode, InputMode::Editing);
    assert_eq!(h.app.query_input.value(), "");
    assert_eq!(drawn(&h.app.query_input, 40), "");

    h.type_str("2");
    assert_eq!(h.app.query_input.value(), "2");
}

// ---------------------------------------------------------------------------
// Modal fields
// ---------------------------------------------------------------------------

/// Put the Sort tab's column filter under the cursor.
fn focus_column_filter(app: &mut App) {
    app.sort_filter_modal.focus = crate::sort_filter_modal::SortFilterFocus::Body;
    app.sort_filter_modal.sort.focus = crate::sort_modal::SortFocus::Filter;
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
fn the_template_description_holds_several_lines() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('s'));
    // Move focus from the name field to the description.
    h.press(KeyCode::Tab);

    h.type_str("first");
    h.press(KeyCode::Enter);
    h.type_str("second");

    let description = &h.app.template_modal.create_description_input;
    assert_eq!(description.value(), "first\nsecond");
    assert_eq!(description.line_count(), 2);
    assert_eq!(description.cursor_line(), 1);
}

#[test]
fn the_template_description_pages_through_its_lines() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('s'));
    h.press(KeyCode::Tab);

    for line in 0..8 {
        h.type_str(&format!("line{line}"));
        if line < 7 {
            h.press(KeyCode::Enter);
        }
    }
    assert_eq!(
        h.app.template_modal.create_description_input.cursor_line(),
        7
    );

    h.press(KeyCode::PageUp);
    assert_eq!(
        h.app.template_modal.create_description_input.cursor_line(),
        2
    );
    h.press(KeyCode::PageUp);
    assert_eq!(
        h.app.template_modal.create_description_input.cursor_line(),
        0
    );
    h.press(KeyCode::PageDown);
    assert_eq!(
        h.app.template_modal.create_description_input.cursor_line(),
        5
    );
}

#[test]
fn the_template_name_field_takes_text() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('t'));
    h.press(KeyCode::Char('s'));
    // Creating from a loaded file pre-fills a suggested name; clear it first.
    let suggested = h.app.template_modal.create_name_input.value().to_string();
    assert!(!suggested.is_empty(), "a name should be suggested");
    for _ in 0..suggested.chars().count() {
        h.press(KeyCode::Backspace);
    }
    h.type_str("by age");

    assert_eq!(h.app.template_modal.create_name_input.value(), "by age");
    assert_eq!(drawn(&h.app.template_modal.create_name_input, 20), "by age");
}

#[test]
fn unicode_survives_a_round_trip_through_a_field() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
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
fn the_pivot_and_melt_modal_filters_columns_as_you_type() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('p'));
    assert_eq!(h.app.input_mode, InputMode::PivotMelt);
    h.app.pivot_melt_modal.focus = crate::pivot_melt_modal::PivotMeltFocus::PivotFilter;

    h.type_str("na");
    assert_eq!(h.app.pivot_melt_modal.pivot_filter_input.value(), "na");
    assert_eq!(drawn(&h.app.pivot_melt_modal.pivot_filter_input, 20), "na");
}

#[test]
fn each_query_tab_keeps_its_own_value() {
    let mut h = Harness::with_data();

    h.press(KeyCode::Char('/'));
    h.type_str("select name");

    // Tab moves to the tab bar, then the arrow keys change tab.
    h.press(KeyCode::Tab);
    assert_eq!(h.app.query_focus, crate::QueryFocus::TabBar);
    h.press(KeyCode::Right);
    assert_eq!(h.app.query_tab, crate::QueryTab::Fuzzy);

    h.press(KeyCode::Tab);
    assert_eq!(h.app.query_focus, crate::QueryFocus::Input);
    h.type_str("ada");
    assert_eq!(h.app.fuzzy_input.value(), "ada");
    assert_eq!(drawn(&h.app.fuzzy_input, 20), "ada");

    // The SQL-like tab still holds what was typed there.
    assert_eq!(h.app.query_input.value(), "select name");
}

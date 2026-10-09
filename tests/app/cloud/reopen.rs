//! A bucket prefix whose files are replaced after it was opened: the read that meets
//! the gone file offers to reopen the dataset, and the reopen lists the prefix again
//! and keeps the place. Served by the in-process S3 stand-in (`common/fake_s3.rs`).

use crate::common::next_event;
use crate::fake_s3::FakeS3;
use crate::remote_quality::remote_events;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppConfig, AppEvent, Applied, OpenOptions};
use std::path::PathBuf;
use std::sync::mpsc;

/// Handle `first` and every event it leads to, until nothing more is owed.
fn settle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: Option<AppEvent>) {
    let mut next = first;
    loop {
        match next.take() {
            Some(event) => next = app.event(event),
            None => match next_event(app, rx) {
                Some(event) => next = Some(event),
                None => return,
            },
        }
    }
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// `s3://lake/events/` opened, its four files of 5,000 ids listed, with `config`.
fn open_events(s3: &FakeS3, config: AppConfig) -> (App, mpsc::Receiver<AppEvent>) {
    let config = AppConfig {
        cloud: s3.cloud_config(),
        ..config
    };
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, crate::common::test_runtime(), theme, config);
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Open(
            vec![PathBuf::from("s3://lake/events/")],
            OpenOptions::default(),
        )),
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|state| state.num_rows()),
        Some(20_000)
    );
    (app, rx)
}

/// The publisher rewrites one file under a new name, as NOAA does.
fn replace_a_file(s3: &FakeS3) {
    let part = remote_events().remove("events/part-1.parquet").unwrap();
    s3.remove("events/part-1.parquet");
    s3.put("events/part-1b.parquet", part);
}

fn rows(app: &App) -> usize {
    let state = app.data_table_state.as_ref().expect("a dataset");
    state.lf().clone().collect().unwrap().height()
}

/// Answer the question with Reopen and settle: the prefix is listed again.
fn reopen(app: &mut App, rx: &mpsc::Receiver<AppEvent>, s3: &FakeS3) {
    let asked = &app.confirmation_modal;
    assert!(asked.active, "the reopen is offered");
    assert!(
        asked.message.starts_with(
            "A file was removed or replaced after the dataset was opened: part-1.parquet."
        ),
        "{}",
        asked.message
    );
    assert_eq!((asked.yes_label, asked.no_label), ("Reopen", "Close"));
    let lists = s3.wire.count().lists;
    let first = press(app, KeyCode::Enter);
    settle(app, rx, first);
    assert!(s3.wire.count().lists > lists, "the prefix is listed again");
    assert_eq!(app.error_message(), None);
}

#[test]
fn a_sort_that_meets_a_replaced_file_reopens_with_the_query_and_sort() {
    let s3 = FakeS3::serve("lake", remote_events());
    let (mut app, rx) = open_events(&s3, AppConfig::default());
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Applied(Applied::QQuery(
            "select where id > 7000".to_string(),
        ))),
    );
    assert_eq!(app.error_message(), None);

    replace_a_file(&s3);
    settle(
        &mut app,
        &rx,
        Some(AppEvent::Applied(Applied::Sort(
            vec!["amount".to_string()],
            vec![false],
        ))),
    );
    reopen(&mut app, &rx, &s3);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where id > 7000");
    assert_eq!(state.get_sort_columns(), ["amount"]);
    assert_eq!(rows(&app), 12_999);
}

/// A query typed at the command line that meets the replaced file is what the reopen
/// tries again, not the view it would have replaced.
#[test]
fn a_query_that_met_a_replaced_file_runs_again_after_the_reopen() {
    let s3 = FakeS3::serve("lake", remote_events());
    let config: AppConfig = toml::from_str("[query]\ndefault_mode = \"q\"\n").unwrap();
    let (mut app, rx) = open_events(&s3, config);
    replace_a_file(&s3);

    press(&mut app, KeyCode::Char(':'));
    for c in "select where id > 7000".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    let first = press(&mut app, KeyCode::Enter);
    settle(&mut app, &rx, first);
    reopen(&mut app, &rx, &s3);
    assert_eq!(app.query_prompt_mode(), None, "the command line is closed");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where id > 7000");
    assert_eq!(rows(&app), 12_999);
}

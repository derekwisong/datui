//! The test harness's own waits: queued events, owed results, the hang guard.

use super::*;

/// The harness handles what is already queued before it calls the app settled.
#[test]
fn test_drain_events_handles_queued_events_first() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('o'),
        KeyModifiers::CONTROL,
    )))
    .unwrap();
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A quiet channel is not completion: the harness waits for the result work owes.
#[test]
fn test_drain_events_waits_for_owed_result() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let missing = common::fixture_dir().join("drain_waits_missing.csv");
    // The open is handed over at once; its scan answers from a worker.
    tx.send(AppEvent::Open(vec![missing], OpenOptions::default()))
        .unwrap();
    drain_events(&mut app, &rx);
    assert!(!app.is_busy(), "the worker's result was handled");
    assert!(app.error_message().is_some(), "and the failed open said so");
    assert!(!work_pending(&app));
}

/// A wait that runs out fails the test rather than falling through to the asserts.
#[test]
#[should_panic(expected = "background work never reported back")]
fn test_wait_past_its_guard_fails() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("nothing will answer", 0);
    common::next_event_within(&mut app, &rx, std::time::Duration::from_millis(1));
}

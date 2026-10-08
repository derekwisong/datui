//! Opening a CSV whose inferred types break further down, through a whole `App`.

/// Pump Open(path, opts) and subsequent events until no event is returned.
/// Returns true if a Crash event was seen.
fn pump_open_until_done(
    app: &mut crate::App,
    rx: &std::sync::mpsc::Receiver<crate::AppEvent>,
    path: std::path::PathBuf,
    opts: crate::OpenOptions,
) -> bool {
    use crate::AppEvent;
    let mut next: Option<AppEvent> = Some(AppEvent::Open(vec![path], opts));
    let mut saw_crash = false;
    // Done once nothing is chained, queued or still owed; the deadline is only a
    // hang guard.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        match next.take() {
            Some(ev) => {
                if matches!(ev, AppEvent::Crash(_)) {
                    saw_crash = true;
                    break;
                }
                next = app.event(ev);
            }
            _ => match rx.try_recv() {
                Ok(ev) => next = Some(ev),
                Err(_) if app.count_waits_for_a_frame() => next = Some(AppEvent::FramePainted),
                Err(_) if !crate::tests::work_pending(app) => break,
                Err(_) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "background work never reported back"
                    );
                    next = rx.recv_timeout(std::time::Duration::from_millis(50)).ok();
                }
            },
        }
    }
    saw_crash
}

/// CSV with 100 int-like rows then "N/A" then more ints. With infer_schema_length=100, Polars
/// infers Int from the first 100 rows; the parse error surfaces from the async collect as a
/// collect failure, which the app shows in the error modal rather than crashing.
#[test]
fn test_infer_schema_length_csv_short_inference_shows_error_modal() {
    use std::sync::mpsc;

    let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
    let opts = crate::OpenOptions {
        infer_schema_length: Some(100),
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = crate::App::new(tx, crate::tests::test_runtime());

    assert!(
        !pump_open_until_done(&mut app, &rx, path, opts),
        "load should not crash; parse failure should be surfaced via error modal"
    );
    // A page is read for its own rows: the error shows once the view reaches row 101.
    if let Some(state) = app.data_table_state.as_mut() {
        state.visible_rows = 40;
        state.scroll_to_row_centered(110);
    }
    app.spawn_async_collect(crate::App::LOADING_BUFFER);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    while crate::tests::work_pending(&app) && std::time::Instant::now() < deadline {
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(event);
            }
        }
    }
    assert!(
        app.error_modal.active,
        "error modal should be shown after parse failure"
    );
    assert!(!app.busy, "busy flag should be cleared after error");
}

#[test]
fn test_infer_schema_length_csv_succeeds_with_longer_inference() {
    use std::sync::mpsc;

    let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
    let opts = crate::OpenOptions {
        infer_schema_length: Some(101),
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = crate::App::new(tx, crate::tests::test_runtime());

    assert!(
        !pump_open_until_done(&mut app, &rx, path, opts),
        "load with infer_schema_length=101 should not crash"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().len(), 1);
    assert!(state.schema().contains("column"));
    assert_eq!(state.num_rows(), 201);
}

#[test]
fn test_infer_schema_length_csv_succeeds_with_default() {
    use std::sync::mpsc;

    let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
    let opts = crate::OpenOptions {
        infer_schema_length: Some(1000),
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = crate::App::new(tx, crate::tests::test_runtime());

    assert!(
        !pump_open_until_done(&mut app, &rx, path, opts),
        "load with infer_schema_length=1000 (default) should not crash"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().len(), 1);
    assert_eq!(state.num_rows(), 201);
}

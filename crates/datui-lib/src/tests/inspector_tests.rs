use crate::inspector_modal::FieldRead;
use crate::*;
use polars::prelude::{IntoLazy, df};

/// A three-row table with `secret` hidden, drawn once so its rows are on hand.
fn app() -> (App, std::sync::mpsc::Receiver<AppEvent>) {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = df!("a" => [1i64, 2, 3], "secret" => ["x", "y", "z"]).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.set_column_order(vec!["a".to_string()]);
    app.data_table_state = Some(state);
    draw(&mut app);
    (app, rx)
}

fn draw(app: &mut App) -> String {
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    buf.content().iter().map(|c| c.symbol()).collect()
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Enter on the hidden field: the answer the worker sends, still unhandled.
fn read_hidden(app: &mut App, rx: &std::sync::mpsc::Receiver<AppEvent>) -> AppEvent {
    press(app, KeyCode::Char(' '));
    assert_eq!(app.input_mode, InputMode::Inspect);
    press(app, KeyCode::End);
    assert!(app.inspector_modal.focused().unwrap().hidden);
    press(app, KeyCode::Enter);
    assert!(matches!(
        app.inspector_modal.read,
        Some(FieldRead::Reading { row: 0, .. })
    ));
    assert!(app.is_busy());
    loop {
        match rx.recv_timeout(std::time::Duration::from_secs(120)) {
            Ok(event @ AppEvent::JobEnded(ticket)) if ticket.kind() == JobKind::InspectRow => {
                return event;
            }
            Ok(_) => continue,
            Err(e) => panic!("no answer from the worker: {e}"),
        }
    }
}

#[test]
fn a_read_lands_for_the_row_it_was_asked_for() {
    let (mut app, rx) = app();
    let answer = read_hidden(&mut app, &rx);
    app.event(&answer);
    assert!(!app.is_busy());
    let frame = app.data_table_state.as_ref().unwrap().len_generation();
    let secret = app
        .inspector_modal
        .read_values(frame, 0)
        .and_then(|v| v.column("secret").ok()?.get(0).ok())
        .map(|v| v.str_value().into_owned());
    assert_eq!(secret.as_deref(), Some("x"));
}

/// The view was replaced while the read ran: its answer is dropped, never shown
/// as the new view's.
#[test]
fn a_read_from_a_stale_generation_is_dropped() {
    let (mut app, rx) = app();
    let answer = read_hidden(&mut app, &rx);
    app.jobs.advance();
    app.event(&answer);
    assert!(matches!(
        app.inspector_modal.read,
        Some(FieldRead::Reading { .. })
    ));
}

/// The cursor moved on: an answer for another row is not this row's.
#[test]
fn a_read_for_another_row_is_let_go() {
    let (mut app, rx) = app();
    let answer = read_hidden(&mut app, &rx);
    // Nobody waits on the read any more, so the cursor can move on before it lands.
    app.jobs.quiet(|job| matches!(job, Job::InspectRow { .. }));
    press(&mut app, KeyCode::Right);
    draw(&mut app);
    assert!(
        app.inspector_modal.read.is_none(),
        "the new row has read nothing"
    );
    app.event(&answer);
    assert!(app.inspector_modal.read.is_none());
}

/// A worker that dies says so in the pane, not as a spinner forever.
#[test]
fn a_failed_read_says_so_in_the_pane() {
    let (mut app, rx) = app();
    app.jobs.worker_dies =
        crate::tests::worker_dies_once(|job| matches!(job, Job::InspectRow { .. }));
    let answer = read_hidden(&mut app, &rx);
    app.event(&answer);
    assert!(!app.is_busy());
    assert!(matches!(
        app.inspector_modal.read,
        Some(FieldRead::Failed { .. })
    ));
    let screen = draw(&mut app);
    assert!(screen.contains("Could not read the field"));
    assert!(screen.contains("Retry"), "the footer says Enter retries");
}

/// #615: long JSON text is parsed on a worker; once the cursor moves to another
/// row, its answer opens nothing.
#[test]
fn a_json_parse_for_another_row_is_let_go() {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let long = format!("[{}0]", "0, ".repeat(40_000));
    let df = df!("j" => [long.clone(), long]).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.set_column_order(vec!["j".to_string()]);
    app.data_table_state = Some(state);
    draw(&mut app);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert!(app.inspector_modal.json_wait.is_some());
    assert!(app.is_busy());
    let answer = loop {
        match rx.recv_timeout(std::time::Duration::from_secs(120)) {
            Ok(event @ AppEvent::JobEnded(ticket)) if ticket.kind() == JobKind::InspectJson => {
                break event;
            }
            Ok(_) => continue,
            Err(e) => panic!("no answer from the worker: {e}"),
        }
    };
    app.jobs.quiet(|job| matches!(job, Job::InspectJson { .. }));
    press(&mut app, KeyCode::Right);
    draw(&mut app);
    assert!(app.inspector_modal.json_wait.is_none());
    app.event(&answer);
    assert!(app.inspector_modal.drill.is_none(), "nothing opened");

    // Asked again on this row, the answer opens the array.
    press(&mut app, KeyCode::Enter);
    let answer = loop {
        match rx.recv_timeout(std::time::Duration::from_secs(120)) {
            Ok(event @ AppEvent::JobEnded(ticket)) if ticket.kind() == JobKind::InspectJson => {
                break event;
            }
            Ok(_) => continue,
            Err(e) => panic!("no answer from the worker: {e}"),
        }
    };
    app.event(&answer);
    let drill = app.inspector_modal.drill.as_ref().expect("opened");
    assert_eq!(drill.row, 1);
    assert_eq!(drill.level().node.len(), 40_001);
}

/// #615: text too long to open as JSON, or that did not parse, is not offered
/// to open again; the whole of it is read in the value pane (#548).
#[test]
fn json_text_that_cannot_open_stops_offering_open() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let huge = format!(
        "[{}0]",
        "0,".repeat(inspector_drill::JSON_MAX_BYTES / 2 + 1)
    );
    let bad = format!("{{{}}}", "x".repeat(40 * 1024));
    let df = df!("huge" => [huge], "bad" => [bad]).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.set_column_order(vec!["huge".to_string(), "bad".to_string()]);
    app.data_table_state = Some(state);
    draw(&mut app);
    press(&mut app, KeyCode::Char(' '));
    let screen = draw(&mut app);
    assert!(
        !screen.contains("Open") && !screen.contains("More"),
        "{screen}"
    );
    press(&mut app, KeyCode::Enter);
    assert!(app.inspector_modal.drill.is_none());

    press(&mut app, KeyCode::Down);
    let screen = draw(&mut app);
    assert!(screen.contains("Open"), "{screen}");
    press(&mut app, KeyCode::Enter);
    assert!(
        app.flash_message()
            .is_some_and(|m| m.starts_with("Not JSON"))
    );
    let screen = draw(&mut app);
    assert!(!screen.contains("Open"), "{screen}");
    assert!(!screen.contains("json"), "no JSON view either: {screen}");
    assert!(app.inspector_modal.drill.is_none());
}

/// The footer names what Enter does on a field not read yet.
#[test]
fn enter_reads_an_unread_field_and_the_footer_says_so() {
    let (mut app, _rx) = app();
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::End);
    let screen = draw(&mut app);
    assert!(
        screen.contains("Read") && !screen.contains("More"),
        "{screen}"
    );
}

/// The fields listed are worked out once per change: a frame with nothing changed
/// reuses them; a change of order or of row works them out again.
#[test]
fn the_field_list_is_worked_out_once_per_change() {
    let (mut app, _rx) = app();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.input_mode, InputMode::Inspect);
    draw(&mut app);
    let builds = app.inspector_modal.list_builds;
    draw(&mut app);
    draw(&mut app);
    assert_eq!(
        app.inspector_modal.list_builds, builds,
        "frames reuse the list"
    );
    press(&mut app, KeyCode::Char('s'));
    draw(&mut app);
    assert_eq!(
        app.inspector_modal.list_builds,
        builds + 1,
        "the order changed"
    );
    draw(&mut app);
    assert_eq!(app.inspector_modal.list_builds, builds + 1);
    app.data_table_state
        .as_mut()
        .unwrap()
        .table_state
        .select(Some(1));
    draw(&mut app);
    assert_eq!(app.inspector_modal.list_builds, builds + 2, "another row");
}

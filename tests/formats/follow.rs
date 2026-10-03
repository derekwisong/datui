//! Following a file as it grows (`--follow`): rows appended while the table is up,
//! the cursor on the last row staying there and one scrolled up staying put, a partial
//! line held until it completes, a truncation read again from the start, standard
//! input arriving after the first rows, a pause and a resume, and the formats that
//! cannot be followed.

use super::*;
use datui::follow::Standing;
use std::io::Write as _;
use std::time::Instant;

fn app() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    (App::new(tx, common::test_runtime()), rx)
}

fn following() -> OpenOptions {
    OpenOptions {
        follow: true,
        ..Default::default()
    }
}

/// Draw a frame, as the run loop does after every event: it sizes the table.
fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    rendered_text(&buffer)
}

fn append(path: &Path, text: &str) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

/// Hand the app what arrives, asking the watcher to look each time, until `done`. The
/// writer has written before this is called; the wait is only for the watcher to see
/// it, never a sleep standing in for it.
#[track_caller]
fn until(app: &mut App, rx: &mpsc::Receiver<AppEvent>, done: impl Fn(&App) -> bool) {
    let caller = std::panic::Location::caller();
    let deadline = Instant::now() + common::HANG_GUARD;
    while !done(app) {
        assert!(
            Instant::now() < deadline,
            "the follow at {caller} never got there"
        );
        app.check_follow_now();
        // As the run loop does after every pass.
        app.request_what_the_frame_needs();
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
            drain_events(app, rx);
        }
    }
}

fn rows(app: &App) -> usize {
    app.data_table_state.as_ref().unwrap().num_rows()
}

fn shown(app: &App) -> usize {
    app.follow().map_or(0, |f| f.shown())
}

fn on_last_row(app: &App) -> bool {
    app.data_table_state.as_ref().unwrap().on_last_row()
}

fn visible(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .unwrap()
        .display_slice_df()
        .unwrap()
}

#[test]
fn appended_rows_show_and_the_last_row_sticks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.csv");
    std::fs::write(&path, "t,n\n1,10\n2,20\n3,30\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 3);
    assert!(screen(&mut app).contains("following"));

    // A follow starts on the last row, as tail -f does, and stays on it.
    append(&path, "4,40\n");
    until(&mut app, &rx, |app| shown(app) == 4 && app.follow_settled());
    assert!(on_last_row(&app));
    append(&path, "5,50\n");
    until(&mut app, &rx, |app| shown(app) == 5 && app.follow_settled());
    assert_eq!(rows(&app), 5);
    assert!(on_last_row(&app), "the cursor stuck to the bottom");
    let t = visible(&app).column("t").unwrap().i64().unwrap().to_vec();
    assert_eq!(t.last().copied().flatten(), Some(5), "{t:?}");

    // Scrolled up, it stays where it was put and the bar counts what came below.
    app.event(&key(KeyCode::Home));
    drain_events(&mut app, &rx);
    append(&path, "6,60\n7,70\n8,80\n");
    until(&mut app, &rx, |app| shown(app) == 8 && app.follow_settled());
    assert_eq!(rows(&app), 8);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.start_row() + state.table_state.selected().unwrap(), 0);
    assert_eq!(app.follow().unwrap().new_below(), 3);
    assert!(screen(&mut app).contains("3 new below"));

    // Esc stops following; the rows read stay.
    app.event(&key(KeyCode::Esc));
    assert!(app.follow().is_none());
    assert_eq!(rows(&app), 8);
    assert_eq!(app.flash_message(), Some("Stopped following"));
}

/// A filter, a query and a sort run over the rows that arrive.
#[test]
fn a_query_runs_over_the_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.csv");
    std::fs::write(&path, "level,n\ninfo,1\nerror,2\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    let mut next = Some(AppEvent::Search(
        "select where level = \"error\"".to_string(),
    ));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 1);
    append(&path, "error,3\ninfo,4\nerror,5\n");
    until(&mut app, &rx, |app| shown(app) == 5 && app.follow_settled());
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 3, "only the errors, new ones among them");
}

/// A line without its newline is not a row yet; once it has one, it is.
#[test]
fn a_partial_line_waits_for_its_newline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slow.csv");
    std::fs::write(&path, "t,word\n1,one\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    append(&path, "2,two\n3,thr");
    until(&mut app, &rx, |app| shown(app) == 2 && app.follow_settled());
    assert_eq!(rows(&app), 2, "the third line is not complete");
    append(&path, "ee\n");
    until(&mut app, &rx, |app| shown(app) == 3 && app.follow_settled());
    app.event(&key(KeyCode::End));
    drain_events(&mut app, &rx);
    let words = visible(&app).column("word").unwrap().str().unwrap().clone();
    assert_eq!(words.get(2), Some("three"), "read whole, not as it was");
}

/// A truncated file is read again from its start, and the bar says so.
#[test]
fn a_truncated_file_is_read_again_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotated.csv");
    std::fs::write(&path, "t,n\n1,10\n2,20\n3,30\n4,40\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    assert_eq!(rows(&app), 4);
    std::fs::write(&path, "t,n\n9,90\n").unwrap();
    until(&mut app, &rx, |app| shown(app) == 1 && app.follow_settled());
    assert_eq!(rows(&app), 1);
    assert!(
        app.flash_message()
            .is_some_and(|m| m.contains("from the start")),
        "{:?}",
        app.flash_message()
    );
    let t = visible(&app).column("t").unwrap().i64().unwrap().to_vec();
    assert_eq!(t, vec![Some(9)]);
}

/// Standard input goes on arriving after the first rows show, until it ends.
#[test]
fn standard_input_keeps_arriving() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"t,n\n1,10\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], following());
    screen(&mut app);
    assert!(rows(&app) >= 1);
    producer.write_all(b"2,20\n3,30\n").unwrap();
    until(&mut app, &rx, |app| shown(app) == 3 && app.follow_settled());
    assert_eq!(rows(&app), 3);
    assert!(on_last_row(&app), "a follow starts on the last row");
    drop(producer);
    until(&mut app, &rx, |app| {
        app.follow()
            .is_some_and(|f| *f.standing() == Standing::Ended)
    });
    assert_eq!(app.flash_message(), Some("Standard input ended"));
    assert_eq!(rows(&app), 3, "what arrived stays");
}

/// `t` pauses: rows are counted and wait; `t` again shows them.
#[test]
fn a_pause_holds_the_view_and_a_resume_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    std::fs::write(&path, "{\"id\": 1, \"msg\": \"a\"}\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    assert_eq!(rows(&app), 1);
    app.event(&key(KeyCode::Char('t')));
    assert_eq!(app.follow().unwrap().standing(), &Standing::Paused);
    append(
        &path,
        "{\"id\": 2, \"msg\": \"b\"}\n{\"id\": \"x\", \"msg\": \"c\"}\n",
    );
    until(&mut app, &rx, |app| app.follow().unwrap().waiting() == 2);
    assert_eq!(rows(&app), 1, "the view holds while paused");
    let bar = screen(&mut app);
    assert!(
        bar.contains("paused") && bar.contains("2 new rows"),
        "{bar}"
    );
    assert!(
        bar.contains("1 row does not fit"),
        "an id that is not a number: {bar}"
    );
    app.event(&key(KeyCode::Char('t')));
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 3);
    assert_eq!(app.follow().unwrap().standing(), &Standing::Following);
}

/// A file whose footer is written last cannot be read as it grows: refused, saying so.
#[test]
fn a_parquet_file_is_not_followed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.parquet");
    let mut df = df!("a" => [1i64, 2]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (mut app, rx) = app();
    let message = pump_open_until_error(&mut app, &rx, vec![path], following());
    assert!(
        message
            .as_deref()
            .is_some_and(|m| m.contains("can be followed")),
        "{message:?}"
    );
}

/// `t` on a file opened without `--follow` reads it again, following it.
#[test]
fn t_starts_following_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("later.csv");
    std::fs::write(&path, "a\n1\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    screen(&mut app);
    assert!(app.follow().is_none());
    let mut next = Some(key(KeyCode::Char('t')));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert!(app.follow().is_some());
    append(&path, "2\n");
    until(&mut app, &rx, |app| shown(app) == 2 && app.follow_settled());
    assert_eq!(rows(&app), 2);
}

/// Value Counts keep the rows they were read of, say how many arrived since, and `t`
/// counts them too.
#[test]
fn value_counts_keep_their_snapshot_until_t() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("levels.csv");
    std::fs::write(&path, "level\ninfo\nerror\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    app.event(&key(KeyCode::Char('F')));
    let counted = |app: &App| app.value_counts.current().map(|c| c.summary.rows);
    until(&mut app, &rx, |app| counted(app).is_some());
    assert_eq!(counted(&app), Some(2));
    append(&path, "warn\nerror\n");
    until(&mut app, &rx, |app| app.follow().unwrap().waiting() == 2);
    assert_eq!(counted(&app), Some(2), "the counts hold their rows");
    let bar = screen(&mut app);
    assert!(
        bar.contains("2 new rows") && bar.contains("Refresh"),
        "{bar}"
    );
    app.event(&key(KeyCode::Char('t')));
    until(&mut app, &rx, |app| counted(app) == Some(4));
    // Back at the table, it reads its rows again.
    app.event(&key(KeyCode::Esc));
    until(&mut app, &rx, |app| {
        app.follow_settled() && visible(app).height() == 4
    });
}

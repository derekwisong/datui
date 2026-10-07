//! Following a file as it grows (`--follow`): rows appended while the table is up,
//! the cursor on the last row staying there and one scrolled up staying put, a partial
//! line held until it completes, a truncation read again from the start, standard
//! input arriving after the first rows, a pause and a resume, and the formats that
//! cannot be followed.

use super::*;
use datui::loading::follow::Standing;
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
    common::buffer_text(&buffer)
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
                next = app.event(event);
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
    app.event(key(KeyCode::Home));
    drain_events(&mut app, &rx);
    append(&path, "6,60\n7,70\n8,80\n");
    until(&mut app, &rx, |app| shown(app) == 8 && app.follow_settled());
    assert_eq!(rows(&app), 8);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.start_row() + state.table_state.selected().unwrap(), 0);
    assert_eq!(app.follow().unwrap().new_below(), 3);
    assert!(screen(&mut app).contains("3 new below"));

    // Esc stops following; the rows read stay.
    app.event(key(KeyCode::Esc));
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
    let mut next = Some(AppEvent::QQuery(
        "select where level = \"error\"".to_string(),
    ));
    while let Some(event) = next {
        next = app.event(event);
    }
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 1);
    append(&path, "error,3\ninfo,4\nerror,5\n");
    until(&mut app, &rx, |app| shown(app) == 5 && app.follow_settled());
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 3, "only the errors, new ones among them");
}

/// A sidebar filter over a long followed file counts the rows that arrive on top of
/// what it counted, and its last page holds the last matches.
#[test]
fn a_filter_counts_and_reads_the_new_rows() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("long.csv");
    let lines = |range: std::ops::Range<i64>| -> String {
        range.map(|i| format!("{i},{}\n", i % 7)).collect()
    };
    std::fs::write(&path, format!("t,n\n{}", lines(0..20_000))).unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    app.event(AppEvent::Filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "n".into(),
        operator: FilterOperator::Eq,
        value: "3".into(),
        logical_op: LogicalOperator::And,
    }]));
    let matches = |n: i64| (0..n).filter(|i| i % 7 == 3).count();
    until(&mut app, &rx, |app| {
        let state = app.data_table_state.as_ref().unwrap();
        state.is_num_rows_valid() && state.num_rows() == matches(20_000)
    });
    for end in [20_050, 30_000] {
        let before = shown(&app) as i64;
        append(&path, &lines(before..end));
        until(&mut app, &rx, |app| {
            let state = app.data_table_state.as_ref().unwrap();
            shown(app) == end as usize && app.follow_settled() && state.is_num_rows_valid()
        });
        assert_eq!(rows(&app), matches(end));
        app.event(key(KeyCode::End));
        drain_events(&mut app, &rx);
        let page = visible(&app).column("t").unwrap().i64().unwrap().to_vec();
        let expected: Vec<_> = (0..end).filter(|i| i % 7 == 3).map(Some).collect();
        assert_eq!(page[..], expected[expected.len() - page.len()..]);
    }
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
    app.event(key(KeyCode::End));
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

/// A file put in place of the followed one, as big or bigger, is read from its start
/// too: its size alone would pass for a file that grew.
#[test]
fn a_replaced_file_of_the_same_or_larger_size_is_read_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rotated.csv");
    std::fs::write(&path, "t,n\n1,10\n2,20\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    assert_eq!(rows(&app), 2);
    let next = dir.path().join("next.csv");
    std::fs::write(&next, "t,n\n7,70\n8,80\n9,90\n").unwrap();
    std::fs::rename(&next, &path).unwrap();
    until(&mut app, &rx, |app| {
        app.flash_message()
            .is_some_and(|m| m.contains("from the start"))
            && app.follow_settled()
    });
    assert_eq!(rows(&app), 3);
    app.event(key(KeyCode::Home));
    drain_events(&mut app, &rx);
    let t = visible(&app).column("t").unwrap().i64().unwrap().to_vec();
    assert_eq!(t, vec![Some(7), Some(8), Some(9)]);
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
    // Rows that arrive just before the end are shown after it.
    producer.write_all(b"4,40\n5,50\n").unwrap();
    drop(producer);
    until(&mut app, &rx, |app| {
        app.follow()
            .is_some_and(|f| *f.standing() == Standing::Ended)
            && app.follow_settled()
    });
    assert_eq!(app.flash_message(), Some("Standard input ended"));
    assert_eq!(rows(&app), 5, "what arrived stays");
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
    app.event(key(KeyCode::Char('t')));
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
    app.event(key(KeyCode::Char('t')));
    drain_events(&mut app, &rx);
    assert_eq!(rows(&app), 3);
    assert_eq!(app.follow().unwrap().standing(), &Standing::Following);
}

/// Blank lines between NDJSON records are not rows: every record shows, the last
/// one included, at open and after an append (#672).
#[test]
fn blank_lines_in_ndjson_cost_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gaps.ndjson");
    std::fs::write(&path, "{\"id\": 1}\n\n{\"id\": 2}\n\n\n{\"id\": 3}\n").unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    let ids = |app: &App| -> Vec<i64> {
        let df = app
            .data_table_state
            .as_ref()
            .unwrap()
            .lf()
            .clone()
            .collect()
            .unwrap();
        df.column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect()
    };
    assert_eq!(ids(&app), [1, 2, 3]);
    append(&path, "\n{\"id\": 4}\n\n{\"id\": 5}\n");
    until(&mut app, &rx, |app| rows(app) == 5);
    assert_eq!(ids(&app), [1, 2, 3, 4, 5]);

    // A page past the first holds the records it should: Polars' in-memory engine,
    // sliced with an offset, counts the blank lines before it as rows.
    let more: String = (6..=8_000)
        .map(|i| format!("{{\"id\": {i}}}\n \n"))
        .collect();
    append(&path, &more);
    until(&mut app, &rx, |app| {
        shown(app) == 8_000 && app.follow_settled()
    });
    app.event(key(KeyCode::End));
    drain_events(&mut app, &rx);
    let page = visible(&app).column("id").unwrap().i64().unwrap().to_vec();
    assert_eq!(page.last().copied().flatten(), Some(8_000), "{page:?}");
    let first = page[0].unwrap();
    let expected: Vec<_> = (first..=8_000).map(Some).collect();
    assert_eq!(page, expected);
}

/// An Arrow IPC stream grows a record batch at a time, from a file or standard input;
/// a batch shows once its message is whole. An Arrow IPC file is refused.
#[test]
fn an_arrow_stream_is_followed_by_its_batches() {
    let frame = |from: i64, n: i64| {
        df!(
            "id" => (from..from + n).collect::<Vec<_>>(),
            "name" => (from..from + n).map(|i| format!("n{i}")).collect::<Vec<_>>(),
        )
        .unwrap()
    };
    let (schema, batches) = datui::loading::follow::stream_messages(&frame(0, 40), 5);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.arrows");
    std::fs::write(&path, [schema.clone(), batches[0].clone()].concat()).unwrap();
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], following());
    screen(&mut app);
    assert_eq!(rows(&app), 5);
    let cut = batches[1].len() / 2;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&batches[1][..cut]).unwrap();
    until(&mut app, &rx, |app| app.follow_settled());
    assert_eq!(rows(&app), 5, "half a batch waits");
    file.write_all(&batches[1][cut..]).unwrap();
    file.write_all(&batches[2..].concat()).unwrap();
    until(&mut app, &rx, |app| {
        shown(app) == 40 && app.follow_settled()
    });
    assert!(on_last_row(&app));
    let ids = visible(&app).column("id").unwrap().i64().unwrap().to_vec();
    assert_eq!(ids.last().copied().flatten(), Some(39), "{ids:?}");

    // Piped in.
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer
        .write_all(&[schema.clone(), batches[0].clone()].concat())
        .unwrap();
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], following());
    screen(&mut app);
    assert!(app.data_table_state.is_some(), "{:?}", app.error_message());
    producer.write_all(&batches[1..].concat()).unwrap();
    until(&mut app, &rx, |app| {
        shown(app) == 40 && app.follow_settled()
    });
    drop(producer);

    // An IPC file has its footer written last.
    let file = dir.path().join("done.arrow");
    let mut df = frame(0, 3);
    IpcWriter::new(File::create(&file).unwrap())
        .finish(&mut df)
        .unwrap();
    let (mut app, rx) = self::app();
    let message = pump_open_until_error(&mut app, &rx, vec![file], following());
    assert!(
        message
            .as_deref()
            .is_some_and(|m| m.contains("stream can be followed")),
        "{message:?}"
    );
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
        next = app.event(event);
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
    app.event(key(KeyCode::Char('F')));
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
    app.event(key(KeyCode::Char('t')));
    until(&mut app, &rx, |app| counted(app) == Some(4));
    // Back at the table, it reads its rows again.
    app.event(key(KeyCode::Esc));
    until(&mut app, &rx, |app| {
        app.follow_settled() && visible(app).height() == 4
    });
}

// `--tee FILE` (#604): standard input recorded to a file while it is viewed.

fn recording(file: &Path, follow: bool) -> OpenOptions {
    OpenOptions {
        follow,
        tee: Some(file.to_path_buf()),
        ..Default::default()
    }
}

fn spool(app: &App) -> std::sync::Arc<datui::loading::follow::Spool> {
    app.recording().expect("recording").clone()
}

/// A 16-bit stereo WAV header as a producer writing to a pipe writes it, its sizes
/// 0xFFFFFFFF, then `frames` frames.
fn streamed_wav(frames: u32) -> Vec<u8> {
    let mut out = b"RIFF".to_vec();
    out.extend(u32::MAX.to_le_bytes());
    out.extend(b"WAVEfmt ");
    out.extend(16u32.to_le_bytes());
    out.extend(1u16.to_le_bytes());
    out.extend(2u16.to_le_bytes());
    out.extend(48_000u32.to_le_bytes());
    out.extend((48_000u32 * 4).to_le_bytes());
    out.extend(4u16.to_le_bytes());
    out.extend(16u16.to_le_bytes());
    out.extend(b"data");
    out.extend(u32::MAX.to_le_bytes());
    for i in 0..frames {
        out.extend((i as i16).to_le_bytes());
        out.extend((i as i16).wrapping_neg().to_le_bytes());
    }
    out
}

/// What came in is what FILE holds, byte for byte, CSV followed and binary read once it
/// ended; the bar says saved.
#[test]
fn a_recording_is_the_bytes_as_they_came() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("run1.csv");
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"t,n\r\n1,10\r\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        recording(&file, true),
    );
    screen(&mut app);
    assert!(screen(&mut app).contains("rec "), "the bar says it records");
    producer.write_all(b"2,20\r\n3,3").unwrap();
    until(&mut app, &rx, |app| shown(app) == 2 && app.follow_settled());
    producer.write_all(b"0\r\n").unwrap();
    drop(producer);
    let spool = spool(&app);
    spool.wait();
    until(&mut app, &rx, |app| {
        app.follow()
            .is_some_and(|f| *f.standing() == Standing::Ended)
    });
    assert_eq!(
        std::fs::read(&file).unwrap(),
        b"t,n\r\n1,10\r\n2,20\r\n3,30\r\n"
    );
    assert_eq!(rows(&app), 3, "the last row is read before the end is");
    let bar = screen(&mut app);
    assert!(bar.contains("saved") && bar.contains("run1.csv"), "{bar}");

    // Binary, read once it has ended.
    let file = dir.path().join("t.parquet");
    let mut bytes = Vec::new();
    let mut df = df!("a" => [1i64, 2, 3]).unwrap();
    ParquetWriter::new(&mut bytes).finish(&mut df).unwrap();
    let (reader, mut producer) = std::io::pipe().unwrap();
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    let sent = bytes.clone();
    let writer = std::thread::spawn(move || producer.write_all(&sent).unwrap());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        recording(&file, false),
    );
    writer.join().unwrap();
    assert_eq!(std::fs::read(&file).unwrap(), bytes);
    assert_eq!(rows(&app), 3);
}

/// `--tee -` passes the stream on to standard output byte for byte while the table
/// reads it, and closes it when the stream ends; a reader downstream that goes away
/// stops the copy and says why. Without standard output to pass to, it is refused.
#[test]
fn tee_dash_passes_the_stream_on_to_standard_output() {
    let passing = OpenOptions {
        follow: true,
        tee: Some(PathBuf::from("-")),
        ..Default::default()
    };
    let (reader, mut producer) = std::io::pipe().unwrap();
    let (downstream, out) = std::io::pipe().unwrap();
    let passed = std::thread::spawn(move || {
        let mut got = Vec::new();
        let mut downstream = downstream;
        std::io::Read::read_to_end(&mut downstream, &mut got).unwrap();
        got
    });
    producer.write_all(b"t,n\n1,10\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    app.pass_stdout_to(out);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], passing.clone());
    assert!(screen(&mut app).contains("rec "), "the bar says it records");
    producer.write_all(b"2,20\n3,30\n").unwrap();
    until(&mut app, &rx, |app| shown(app) == 3 && app.follow_settled());
    drop(producer);
    spool(&app).wait();
    assert_eq!(
        passed.join().unwrap(),
        b"t,n\n1,10\n2,20\n3,30\n",
        "closed at the end"
    );
    until(&mut app, &rx, |app| {
        app.follow()
            .is_some_and(|f| *f.standing() == Standing::Ended)
    });
    let bar = screen(&mut app);
    assert!(bar.contains("sent") && !bar.contains("saved"), "{bar}");
    // As the run loop's tick notices it.
    app.tick_follow_clock();
    assert_eq!(app.flash_message(), Some("Standard input ended"));

    // Downstream stops reading.
    let (reader, mut producer) = std::io::pipe().unwrap();
    let (downstream, out) = std::io::pipe().unwrap();
    producer.write_all(b"t,n\n1,10\n").unwrap();
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    app.pass_stdout_to(out);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], passing.clone());
    drop(downstream);
    let _ = producer.write_all(b"2,20\n");
    spool(&app).wait();
    let ended = spool(&app).ended().flatten().unwrap_or_default();
    assert!(ended.contains("standard output"), "{ended}");

    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"t,n\n1,10\n").unwrap();
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    let message = pump_open_until_error(&mut app, &rx, vec![PathBuf::from("-")], passing);
    assert!(
        message
            .as_deref()
            .is_some_and(|m| m.contains("standard output")),
        "{message:?}"
    );
}

/// The copy is a thread of its own: megabytes go through while the app handles
/// nothing at all, so a slow draw never holds the producer up.
#[test]
fn a_slow_reader_does_not_stall_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("fast.csv");
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"i,s\n0,start\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        recording(&file, true),
    );
    let line = b"123456,abcdefghijklmnopqrstuvwxyz0123456789\n";
    let lines = 200_000;
    // Written in full before anything is handed to the app again.
    let writer = std::thread::spawn(move || {
        for _ in 0..lines {
            producer.write_all(line).unwrap();
        }
    });
    writer.join().unwrap();
    let spool = spool(&app);
    let deadline = Instant::now() + common::HANG_GUARD;
    let total = 12 + (line.len() * lines) as u64;
    while spool.bytes() < total {
        assert!(
            Instant::now() < deadline,
            "the copy stalled at {}",
            spool.bytes()
        );
        std::thread::yield_now();
    }
    assert_eq!(std::fs::metadata(&file).unwrap().len(), total);
    drop(rx);
}

/// Quitting while the producer sends asks: stop, and FILE ends there; keep, and it
/// goes on until the stream ends; Esc, and nothing happens.
#[test]
fn quitting_while_recording_asks_whether_to_keep_recording() {
    for keep in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("session.csv");
        let (reader, mut producer) = std::io::pipe().unwrap();
        producer.write_all(b"a\n1\n").unwrap();
        let (mut app, rx) = app();
        app.read_stdin_from(reader);
        pump_open_until_loaded(
            &mut app,
            &rx,
            vec![PathBuf::from("-")],
            recording(&file, true),
        );
        screen(&mut app);

        assert!(
            app.event(key(KeyCode::Char('Q'))).is_none(),
            "asked, not quit"
        );
        assert!(app.confirmation_modal.active);
        app.event(key(KeyCode::Esc));
        assert!(!app.confirmation_modal.active);
        assert!(spool(&app).live(), "Esc stays, recording");

        assert!(app.event(key(KeyCode::Char('Q'))).is_none());
        if keep {
            app.event(key(KeyCode::Right));
        }
        let out = app.event(key(KeyCode::Enter));
        assert!(matches!(out, Some(AppEvent::Exit)), "either way it quits");
        let after = app.recording_after_exit();
        if keep {
            let (tee, handle) = after.expect("kept recording");
            assert_eq!(tee.path, file);
            drop(app);
            producer.write_all(b"2\n3\n").unwrap();
            drop(producer);
            handle.spool().wait();
            assert_eq!(std::fs::read(&file).unwrap(), b"a\n1\n2\n3\n");
        } else {
            assert!(after.is_none());
            assert!(!spool(&app).live(), "stopped");
            // What the producer sends after the stop is not recorded.
            let _ = producer.write_all(b"2\n");
            drop(producer);
            assert_eq!(std::fs::read(&file).unwrap(), b"a\n1\n");
        }
    }
}

/// A WAV stream's header sizes, unknown to a producer writing to a pipe, are filled in
/// when the stream ends; `--tee-raw` leaves them.
#[test]
fn a_wav_header_is_fixed_at_the_end_of_the_stream() {
    for raw in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("take1.wav");
        let bytes = streamed_wav(1_000);
        let (reader, mut producer) = std::io::pipe().unwrap();
        let (mut app, rx) = app();
        app.read_stdin_from(reader);
        let sent = bytes.clone();
        let writer = std::thread::spawn(move || producer.write_all(&sent).unwrap());
        let options = OpenOptions {
            tee_raw: raw,
            ..recording(&file, false)
        };
        pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], options);
        writer.join().unwrap();
        let written = std::fs::read(&file).unwrap();
        assert_eq!(written.len(), bytes.len());
        let riff = u32::from_le_bytes(written[4..8].try_into().unwrap());
        let data = u32::from_le_bytes(written[40..44].try_into().unwrap());
        if raw {
            assert_eq!(written, bytes, "the bytes as they came");
        } else {
            assert_eq!(riff as usize, bytes.len() - 8);
            assert_eq!(data, 4_000);
            assert_eq!(&written[44..], &bytes[44..], "only the sizes change");
        }
        assert_eq!(rows(&app), 1_000);
    }
}

/// FILE is never replaced without `--force`.
#[test]
fn an_existing_file_is_not_replaced_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("keep.csv");
    std::fs::write(&file, "precious\n").unwrap();
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"a\n1\n").unwrap();
    drop(producer);
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    let message = pump_open_until_error(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        recording(&file, true),
    );
    assert!(
        message.as_deref().is_some_and(|m| m.contains("--force")),
        "{message:?}"
    );
    assert_eq!(std::fs::read(&file).unwrap(), b"precious\n");

    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"a\n1\n").unwrap();
    drop(producer);
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    let options = OpenOptions {
        force: true,
        ..recording(&file, false)
    };
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], options);
    assert_eq!(std::fs::read(&file).unwrap(), b"a\n1\n");
}

/// A write that fails (a full disk) stops the copy cleanly and says why, naming FILE.
#[cfg(target_os = "linux")]
#[test]
fn a_full_disk_stops_the_recording_and_says_why() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"a\n1\n").unwrap();
    drop(producer);
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    let options = OpenOptions {
        force: true,
        ..recording(Path::new("/dev/full"), false)
    };
    let message = pump_open_until_error(&mut app, &rx, vec![PathBuf::from("-")], options);
    assert!(
        message.as_deref().is_some_and(|m| m.contains("/dev/full")),
        "{message:?}"
    );
}

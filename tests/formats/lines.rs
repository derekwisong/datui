//! Text read as lines: a `.log` file, an unnamed text file, a pipe, a compressed log,
//! a directory of logs and a followed log each open as a `line` column, every line a
//! row, blank ones included, numbered by `#` as `less -N` numbers them; CSV opens as CSV
//! only on evidence, and bytes that are not text still open in the hex view.

use super::*;
use std::io::Write as _;
use std::time::Instant;

fn app() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    (App::new(tx, common::test_runtime()), rx)
}

fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

fn open(paths: Vec<PathBuf>, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (mut app, rx) = app();
    pump_open_until_loaded(&mut app, &rx, paths, options);
    drain_events(&mut app, &rx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    (app, rx)
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
}

fn lines(app: &App) -> Vec<String> {
    frame(app)
        .column("line")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.unwrap_or("<null>").to_string())
        .collect()
}

fn notes(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .unwrap()
        .notes()
        .into_iter()
        .map(|n| n.summary)
        .collect()
}

fn read_as_lines(app: &App) -> bool {
    notes(app).iter().any(|n| n.starts_with("read as lines"))
}

const LOG: &[u8] = b"started\r\n\r\nwarn: disk, 91% full\n\n\nstopped\n";
const LOG_LINES: [&str; 6] = ["started", "", "warn: disk, 91% full", "", "", "stopped"];

/// What `#` shows for the first `rows` rows on screen.
fn numbers(app: &App, rows: usize) -> Vec<usize> {
    let state = app.data_table_state.as_ref().unwrap();
    state.row_numbers_from(state.start_row(), rows)
}

#[test]
fn a_log_opens_as_its_lines_numbered_as_less_numbers_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "app.log", LOG);
    let (app, _rx) = open(vec![path], OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_column_order(), ["line"]);
    assert!(state.row_numbers(), "# is on for text");
    assert_eq!(numbers(&app, 6), [1, 2, 3, 4, 5, 6]);
    assert_eq!(lines(&app), LOG_LINES);
    assert!(read_as_lines(&app), "{:?}", notes(&app));
    assert!(
        !notes(&app).iter().any(|n| n.contains("--format csv")),
        "a name that says text is not a guess"
    );
}

#[test]
fn an_unnamed_text_file_is_lines_unless_it_is_csv() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "output", LOG);
    let (app, _rx) = open(vec![path], OpenOptions::default());
    assert_eq!(lines(&app), LOG_LINES);
    assert!(
        notes(&app).iter().any(|n| n.contains("--format csv")),
        "{:?}",
        notes(&app)
    );

    let path = write(dir.path(), "export", b"id,name\n1,a\n2,b\n");
    let (app, _rx) = open(vec![path], OpenOptions::default());
    assert_eq!(frame(&app).get_column_names(), ["id", "name"]);
}

#[test]
fn bytes_that_are_not_text_still_open_in_hex() {
    let dir = tempfile::tempdir().unwrap();
    let bytes: Vec<u8> = (0..2048u32).map(|i| (i * 31 % 256) as u8).collect();
    let path = write(dir.path(), "blob", &bytes);
    let (app, _rx) = open(vec![path], OpenOptions::default());
    assert_eq!(app.overlay, Overlay::Hex);
}

#[test]
fn a_pipe_of_text_is_lines_and_a_pipe_of_csv_is_csv() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(LOG).unwrap();
    drop(producer);
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    drain_events(&mut app, &rx);
    assert_eq!(lines(&app), LOG_LINES);
    assert!(read_as_lines(&app));

    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"host,status\na,up\nb,down\n").unwrap();
    drop(producer);
    let (mut app, rx) = self::app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    drain_events(&mut app, &rx);
    assert_eq!(frame(&app).get_column_names(), ["host", "status"]);
}

#[test]
fn a_compressed_log_is_decompressed_and_read_as_lines() {
    let dir = tempfile::tempdir().unwrap();
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), Default::default());
    gz.write_all(LOG).unwrap();
    let path = write(dir.path(), "app.log.gz", &gz.finish().unwrap());
    let (app, _rx) = open(vec![path.clone()], OpenOptions::default());
    assert_eq!(lines(&app), LOG_LINES);
    assert!(read_as_lines(&app));
    let (app, _rx) = open(
        vec![path],
        OpenOptions {
            decompress_in_memory: true,
            ..Default::default()
        },
    );
    assert_eq!(lines(&app), LOG_LINES);
}

#[test]
fn a_directory_of_logs_names_each_line_s_file() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.log", b"one\n\ntwo\n");
    write(dir.path(), "b.log", b"three\n");
    let (app, _rx) = open(vec![dir.path().to_path_buf()], OpenOptions::default());
    let df = frame(&app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_column_order(),
        ["file", "line"]
    );
    let files: Vec<Option<&str>> = df.column("file").unwrap().str().unwrap().iter().collect();
    assert_eq!(
        files,
        [Some("a.log"), Some("a.log"), Some("a.log"), Some("b.log")]
    );
    assert_eq!(lines(&app), ["one", "", "two", "three"]);
}

#[test]
fn a_readme_beside_data_is_not_the_table() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.csv", b"x\n1\n");
    write(dir.path(), "b.csv", b"x\n2\n");
    write(dir.path(), "README.txt", b"two csv files\n");
    let (app, _rx) = open(vec![dir.path().to_path_buf()], OpenOptions::default());
    assert_eq!(frame(&app).get_column_names(), ["x"]);
}

/// Find and the query work on `line` as on any column.
#[test]
fn a_query_filters_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "app.log", LOG);
    let (mut app, rx) = open(vec![path], OpenOptions::default());
    let mut next = Some(AppEvent::QQuery("select where line <> \"\"".to_string()));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert_eq!(lines(&app), ["started", "warn: disk, 91% full", "stopped"]);
}

/// `#` is each line's number in the file, and stays with it through a filter and a
/// sort; turned on for a CSV, it numbers the rows of the file as they were read.
#[test]
fn row_numbers_are_the_source_row_through_sort_and_filter() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "app.log", LOG);
    let (mut app, rx) = open(vec![path], OpenOptions::default());
    let run = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
        drain_events(app, &rx);
    };
    run(
        &mut app,
        AppEvent::Filter(vec![FilterStatement {
            columns: Vec::new(),
            column: "line".into(),
            operator: FilterOperator::NotEq,
            value: "".into(),
            logical_op: LogicalOperator::And,
        }]),
    );
    assert_eq!(lines(&app), ["started", "warn: disk, 91% full", "stopped"]);
    assert_eq!(numbers(&app, 3), [1, 3, 6]);
    run(&mut app, AppEvent::Sort(vec!["line".into()], vec![true]));
    assert_eq!(lines(&app), ["warn: disk, 91% full", "stopped", "started"]);
    assert_eq!(numbers(&app, 3), [3, 6, 1]);

    // A CSV opens without them; turned on over a sort, they are the file's rows.
    let path = write(dir.path(), "hosts.csv", b"host,up\nc,1\na,0\nb,1\n");
    let (mut app, rx) = open(vec![path], OpenOptions::default());
    assert!(!app.data_table_state.as_ref().unwrap().row_numbers());
    let run = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
        drain_events(app, &rx);
    };
    run(&mut app, AppEvent::Sort(vec!["host".into()], vec![false]));
    assert_eq!(
        numbers(&app, 3),
        [1, 2, 3],
        "the view's places while # is off"
    );
    run(
        &mut app,
        AppEvent::Key(KeyEvent::new(KeyCode::Char('#'), KeyModifiers::NONE)),
    );
    assert!(app.data_table_state.as_ref().unwrap().row_numbers());
    assert_eq!(numbers(&app, 3), [2, 3, 1]);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_column_order(), ["host", "up"]);
}

/// A followed log shows every line that arrives, blank ones too, and a line is read
/// whole once its newline lands.
#[test]
fn a_followed_log_shows_its_lines_as_they_arrive() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "live.log", b"first\n\n");
    let (mut app, rx) = open(
        vec![path.clone()],
        OpenOptions {
            follow: true,
            ..Default::default()
        },
    );
    assert_eq!(lines(&app), ["first", ""]);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(b"second, with a comma\n\nthi").unwrap();
    let shown = |app: &App| app.follow().map_or(0, |f| f.shown());
    let deadline = Instant::now() + common::HANG_GUARD;
    let pump_until = |app: &mut App, done: &dyn Fn(&App) -> bool| {
        while !done(app) {
            assert!(Instant::now() < deadline, "the follow never got there");
            app.check_follow_now();
            app.request_what_the_frame_needs();
            if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
                let mut next = Some(event);
                while let Some(event) = next {
                    next = app.event(&event);
                }
                drain_events(app, &rx);
            }
        }
    };
    pump_until(&mut app, &|app| shown(app) == 4 && app.follow_settled());
    assert_eq!(lines(&app), ["first", "", "second, with a comma", ""]);
    file.write_all(b"rd\n").unwrap();
    pump_until(&mut app, &|app| shown(app) == 5 && app.follow_settled());
    assert_eq!(
        lines(&app),
        ["first", "", "second, with a comma", "", "third"]
    );
}

/// Standard input followed is lines when it holds no table.
#[test]
fn a_followed_pipe_of_text_is_lines() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"boot\n\nready\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions {
            follow: true,
            ..Default::default()
        },
    );
    drain_events(&mut app, &rx);
    assert_eq!(lines(&app), ["boot", "", "ready"]);
    drop(producer);
}

/// Copy as Python splits the file at its newlines as datui does.
#[test]
fn copy_as_python_reads_the_lines_datui_shows() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "app.log", LOG);
    let (app, _rx) = open(vec![path], OpenOptions::default());
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    assert_eq!(rows, view_csv(&app), "{script}");
}

/// A log larger than what an open indexes shows its first rows, and its lines go on
/// being indexed behind them: the count, End and `#` then reach the last line.
#[test]
fn a_large_log_is_indexed_behind_its_first_rows() {
    let dir = tempfile::tempdir().unwrap();
    let lines = 1_500_000usize;
    let mut bytes = Vec::with_capacity(lines * 14);
    for i in 1..=lines {
        bytes.extend_from_slice(format!("line {i}\n").as_bytes());
    }
    assert!(bytes.len() > datui::lines::FIRST_BYTES);
    let path = write(dir.path(), "big.log", &bytes);
    let (mut app, rx) = app();
    // Up to the first rows, and End pressed at once: while lines are still being
    // indexed it waits for the last of them.
    let mut next = Some(AppEvent::Open(vec![path], OpenOptions::default()));
    while app.data_table_state.is_none() || app.is_busy() {
        match next.take() {
            Some(event) => next = app.event(&event),
            None => next = common::next_event(&app, &rx),
        }
    }
    let mut next = press(&mut app, KeyCode::End);
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.indexing().is_none());
    assert_eq!(state.num_rows_if_valid(), Some(lines));
    assert!(state.on_last_row(), "End reached the last line");
    assert_eq!(state.selected_display_row(), Some(lines));
    let numbers = state.row_numbers_from(state.start_row(), state.visible_rows.max(1));
    assert!(numbers.contains(&lines), "{numbers:?}");
}

/// A pipe shows its first rows while it is still sending, without `--follow`, and
/// reads on to its end: the rows so far are said to be a part, until it ends. The view
/// stays at the top, and what came in is spooled in the cache directory.
#[test]
fn a_pipe_shows_rows_as_they_arrive() {
    let (reader, mut producer) = std::io::pipe().unwrap();
    producer.write_all(b"boot\nready\n").unwrap();
    let (mut app, rx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("-")],
        OpenOptions::default(),
    );
    drain_events(&mut app, &rx);
    assert_eq!(lines(&app), ["boot", "ready"], "rows before the pipe ends");
    let follow = app.follow().expect("read on as it arrives");
    assert!(follow.is_pipe() && follow.live());
    let spooled = follow.path().to_path_buf();
    let cache = std::path::PathBuf::from(std::env::var_os("DATUI_CACHE_DIR").unwrap());
    assert!(
        spooled.starts_with(cache.join("spool")),
        "spooled in the cache: {}",
        spooled.display()
    );
    let text = screen(&mut app);
    assert!(text.contains("reading stdin"), "{text}");
    assert!(text.contains("/ 2+"), "the count is a part: {text}");

    producer.write_all(b"serving\n").unwrap();
    drop(producer);
    let deadline = Instant::now() + common::HANG_GUARD;
    while app.follow().is_some_and(|f| f.live() || f.shown() < 3) {
        assert!(Instant::now() < deadline, "the pipe never ended");
        app.check_follow_now();
        app.request_what_the_frame_needs();
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
            drain_events(&mut app, &rx);
        }
    }
    assert_eq!(lines(&app), ["boot", "ready", "serving"]);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.start_row(), 0, "the view stays at the top");
    let text = screen(&mut app);
    assert!(text.contains("/ 3") && !text.contains("/ 3+"), "{text}");
    assert!(!text.contains("reading stdin"), "{text}");
}

/// The screen as text.
fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 100, 20);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    common::buffer_text(&buffer)
}

/// A large log opened from its first rows.
fn large_log(dir: &Path, lines: usize) -> PathBuf {
    let mut bytes = Vec::with_capacity(lines * 14);
    for i in 1..=lines {
        bytes.extend_from_slice(format!("line {i}\n").as_bytes());
    }
    write(dir, "big.log", &bytes)
}

/// Up to the first rows of `path`, the rest of its lines maybe still being indexed.
fn open_first_rows(path: PathBuf) -> (App, mpsc::Receiver<AppEvent>) {
    let (mut app, rx) = app();
    let mut next = Some(AppEvent::Open(vec![path], OpenOptions::default()));
    while app.data_table_state.is_none() || app.is_busy() {
        match next.take() {
            Some(event) => next = app.event(&event),
            None => next = common::next_event(&app, &rx),
        }
    }
    (app, rx)
}

/// `:N` past the lines indexed so far waits for them and then goes there, rather than
/// stopping at the last line on hand.
#[test]
fn go_to_a_row_waits_for_the_lines_to_be_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx) = open_first_rows(large_log(dir.path(), 1_500_000));
    let _ = screen(&mut app);
    let mut next = app.event(&AppEvent::GoToLine(1_400_000));
    while let Some(event) = next {
        next = app.event(&event);
    }
    // While the lines are still coming, the footer's progress line says how far.
    if app.data_table_state.as_ref().unwrap().indexing().is_some() {
        let text = screen(&mut app);
        assert!(text.contains("lines ") && text.contains("read "), "{text}");
    }
    drain_events(&mut app, &rx);
    let _ = screen(&mut app);
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.selected_display_row(), Some(1_400_001));
    let numbers = state.row_numbers_from(state.start_row(), state.visible_rows.max(1));
    assert!(numbers.contains(&1_400_001), "{numbers:?}");
}

/// Home pauses the indexing, the table takes it up again; and the app gone, it stops
/// for good, so nothing holds the file.
#[test]
fn home_pauses_the_indexing_and_the_app_gone_stops_it() {
    let dir = tempfile::tempdir().unwrap();
    let lines = 1_500_000;
    let log = large_log(dir.path(), lines);
    let (mut app, rx) = open_first_rows(log.clone());
    let held = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lines_to_index()
        .cloned()
        .expect("opened from its first rows");
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);
    // Back at the table: a frame drawn takes the indexing up again.
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    while let Some(event) = next {
        next = app.event(&event);
    }
    assert!(app.at_table());
    let _ = screen(&mut app);
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(lines));
    assert!(held.whole() && !held.indexing());
    drop(app);

    // The same file again: Windows refuses to rewrite a file while it is mapped.
    let (mut app, _rx) = open_first_rows(log);
    let held = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lines_to_index()
        .cloned()
        .unwrap();
    app.enter_home();
    drop(app);
    assert!(!held.indexing(), "nothing waits on a file the app let go");
}

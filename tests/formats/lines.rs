//! Text read as lines: a `.log` file, an unnamed text file, a pipe, a compressed log,
//! a directory of logs and a followed log each open as `line_no` and `line`, every line
//! a row, blank ones included; CSV opens as CSV only on evidence, and bytes that are not
//! text still open in the hex view.

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
    notes(app).iter().any(|n| n.starts_with("Read as lines"))
}

const LOG: &[u8] = b"started\r\n\r\nwarn: disk, 91% full\n\n\nstopped\n";
const LOG_LINES: [&str; 6] = ["started", "", "warn: disk, 91% full", "", "", "stopped"];

#[test]
fn a_log_opens_as_its_lines_numbered_as_less_numbers_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "app.log", LOG);
    let (app, _rx) = open(vec![path], OpenOptions::default());
    let df = frame(&app);
    assert_eq!(df.get_column_names(), ["line_no", "line"]);
    let numbers: Vec<Option<u32>> = df.column("line_no").unwrap().u32().unwrap().to_vec();
    assert_eq!(numbers, (1..=6).map(Some).collect::<Vec<_>>());
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
    assert_eq!(app.input_mode, InputMode::Hex);
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
    assert_eq!(df.get_column_names(), ["file", "line_no", "line"]);
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
    let mut next = Some(AppEvent::Search(
        "select where line_no > 2, line <> \"\"".to_string(),
    ));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    assert_eq!(lines(&app), ["warn: disk, 91% full", "stopped"]);
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

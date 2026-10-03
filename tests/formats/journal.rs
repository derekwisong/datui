//! The systemd journal as `journalctl -o json` writes it, from a synthetic fixture of
//! twelve entries over two boots: `time`, `level` and readable messages derived, the
//! columns in reading order, a field first seen late, a pipe, `--follow` from a pipe,
//! and Copy as Python.

use super::*;
use std::io::Write as _;

const FIXTURE: &str = "tests/fixtures/journal.json";

fn app() -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    (App::new(tx.clone(), common::test_runtime()), rx, tx)
}

fn open(paths: Vec<PathBuf>, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (mut app, rx, tx) = app();
    pump_open_until_loaded(&mut app, &rx, paths, options);
    pump_until_idle(&mut app, &rx, &tx);
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

fn texts(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    df.column(name)
        .unwrap()
        .cast(&DataType::String)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_string))
        .collect()
}

fn piped(bytes: Vec<u8>, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (reader, mut producer) = std::io::pipe().unwrap();
    let writer = std::thread::spawn(move || {
        producer.write_all(&bytes).unwrap();
    });
    let (mut app, rx, tx) = app();
    app.read_stdin_from(reader);
    pump_open_until_loaded(&mut app, &rx, vec![PathBuf::from("-")], options);
    pump_until_idle(&mut app, &rx, &tx);
    writer.join().unwrap();
    (app, rx)
}

#[test]
fn a_journal_has_time_level_and_readable_messages_first() {
    let (app, _rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let state = app.data_table_state.as_ref().unwrap();
    let df = frame(&app);
    let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
    assert_eq!(
        names[..5],
        ["time", "level", "_SYSTEMD_UNIT", "_PID", "MESSAGE"]
    );
    // Every field stays, bookkeeping last.
    let tail: Vec<&str> = names[names.len() - 8..].to_vec();
    for kept in [
        "__CURSOR",
        "__REALTIME_TIMESTAMP",
        "__SEQNUM",
        "_BOOT_ID",
        "_MACHINE_ID",
    ] {
        assert!(tail.contains(&kept), "{kept} among {tail:?}");
    }
    assert!(names.contains(&"SYSLOG_IDENTIFIER") && names.contains(&"_HOSTNAME"));

    assert_eq!(
        df.column("time").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
    );
    let time = texts(&df, "time");
    assert!(
        time[0]
            .as_deref()
            .unwrap()
            .starts_with("2026-01-01 00:00:00"),
        "{time:?}"
    );
    assert!(matches!(
        df.column("level").unwrap().dtype(),
        DataType::Enum(..)
    ));
    let levels = texts(&df, "level");
    assert_eq!(levels[0].as_deref(), Some("info"));
    assert_eq!(levels[3].as_deref(), Some("warning"));
    assert_eq!(levels[9].as_deref(), Some("emerg"));

    // A message journalctl wrote as bytes reads as text, lossily.
    let messages = texts(&df, "MESSAGE");
    assert_eq!(
        messages[6].as_deref(),
        Some("status: \u{fffd}\x1b[1mok"),
        "{messages:?}"
    );
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes
            .iter()
            .any(|n| n.starts_with("1 message came as bytes")),
        "{notes:?}"
    );
}

/// `level` is ordered by severity: a filter keeps the entries at `err` or worse, and a
/// sort puts `emerg` first.
#[test]
fn level_filters_and_sorts_by_severity() {
    let (mut app, rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let mut next = Some(AppEvent::Search(
        "select level, MESSAGE where level <= \"err\"".to_string(),
    ));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    let df = frame(&app);
    assert_eq!(df.height(), 4, "emerg, alert, crit and err");
    let sorted = frame(&app).sort(["level"], Default::default()).unwrap();
    assert_eq!(texts(&sorted, "level")[0].as_deref(), Some("emerg"));
}

#[test]
fn journal_json_is_known_from_a_pipe() {
    let (app, _rx) = piped(std::fs::read(FIXTURE).unwrap(), OpenOptions::default());
    let df = frame(&app);
    assert_eq!(df.height(), 12);
    assert_eq!(df.get_column_names()[0].as_str(), "time");
}

/// A field first seen after a thousand entries is a column too.
#[test]
fn a_field_first_seen_late_is_a_column() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("late.jsonl");
    let mut text = String::new();
    for i in 0..1500u64 {
        let late = if i == 1499 {
            ",\"CODE_FILE\":\"late.c\""
        } else {
            ""
        };
        text.push_str(&format!(
            "{{\"__CURSOR\":\"s=1;i={i:x}\",\"__REALTIME_TIMESTAMP\":\"{}\",\"PRIORITY\":\"6\",\"MESSAGE\":\"m{i}\"{late}}}\n",
            1_767_225_600_000_000u64 + i
        ));
    }
    std::fs::write(&path, text).unwrap();
    let (app, _rx) = open(vec![path], OpenOptions::default());
    let df = frame(&app);
    let late = texts(&df, "CODE_FILE");
    assert_eq!(late[1499].as_deref(), Some("late.c"));
    assert_eq!(late.iter().filter(|v| v.is_some()).count(), 1);
}

/// `journalctl -o json -f | datui -f -`: the derived columns from the first entries
/// on, and the entries that arrive after.
#[test]
fn a_followed_journal_from_a_pipe_keeps_its_columns() {
    let lines: Vec<String> = std::fs::read_to_string(FIXTURE)
        .unwrap()
        .lines()
        .map(|l| format!("{l}\n"))
        .collect();
    let (reader, mut producer) = std::io::pipe().unwrap();
    for line in &lines[..6] {
        producer.write_all(line.as_bytes()).unwrap();
    }
    let (mut app, rx, _tx) = app();
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
    assert_eq!(frame(&app).get_column_names()[..2], ["time", "level"]);
    for line in &lines[6..] {
        producer.write_all(line.as_bytes()).unwrap();
    }
    drop(producer);
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    while !(app.follow().is_some_and(|f| f.shown() == 12) && app.follow_settled()) {
        assert!(
            std::time::Instant::now() < deadline,
            "the follow never got there"
        );
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
    let df = frame(&app);
    assert_eq!(df.height(), 12);
    assert_eq!(texts(&df, "level")[9].as_deref(), Some("emerg"));
    // A message as bytes arriving later is read as text, and fits.
    assert_eq!(
        texts(&df, "MESSAGE")[6].as_deref(),
        Some("status: \u{fffd}\x1b[1mok")
    );
    assert_eq!(app.follow().unwrap().misfits(), 0);
}

/// Copy as Python reads the journal and derives the same columns.
#[test]
fn copy_as_python_derives_the_journal_s_columns() {
    let (app, _rx) = open(vec![PathBuf::from(FIXTURE)], OpenOptions::default());
    let Some((rows, script)) = run_python_script(&app) else {
        eprintln!("skipped: no .venv to run the scripts with");
        return;
    };
    assert!(script.contains("infer_schema_length=None"), "{script}");
    assert_eq!(rows, view_csv(&app), "{script}");
}

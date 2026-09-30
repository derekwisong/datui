//! While the TUI owns the terminal, stderr goes to the log, and a panic still reaches
//! the terminal. Run in a child process, since it points this process's fd 2 away.
#![cfg(unix)]

use datui::logging::{LevelFilter, LogSettings, TuiSession};
use std::process::Command;

const CHILD: &str = "DATUI_STDERR_TEST_LOG";
const RESUME: &str = "DATUI_STDERR_TEST_RESUME";

/// The child's half: does nothing unless the parent test ran it.
#[test]
fn child_session() {
    let Ok(log) = std::env::var(CHILD) else {
        return;
    };
    datui::logging::init(&LogSettings {
        path: Some(log.into()),
        level: LevelFilter::Warn,
        unknown_level: None,
    });
    // No terminal here, so there is nothing to restore.
    let _session = TuiSession::begin(|| {});
    eprintln!("stray stderr line");
    let caught = std::thread::spawn(|| panic!("background boom")).join();
    assert!(caught.is_err(), "the worker's panic is caught");
    if std::env::var_os(RESUME).is_some() {
        // As a Polars worker's panic reaches the thread that asked: no hook runs.
        std::panic::resume_unwind(caught.unwrap_err());
    }
    panic!("foreground boom");
}

/// Run `child_session` in a child process: whether it succeeded, its stderr, its log.
fn run_child(resume: bool) -> (bool, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("datui.log");
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "child_session",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, &log);
    if resume {
        child.env(RESUME, "1");
    }
    let out = child.output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        std::fs::read_to_string(&log).unwrap(),
    )
}

#[test]
fn stderr_goes_to_the_log_and_a_panic_to_the_terminal() {
    let (succeeded, stderr, logged) = run_child(false);
    assert!(!succeeded, "the child panicked");
    assert!(stderr.contains("foreground boom"), "stderr: {stderr}");
    assert!(!stderr.contains("stray stderr line"), "stderr: {stderr}");
    assert!(!stderr.contains("background boom"), "stderr: {stderr}");
    assert!(logged.contains("stray stderr line"), "log: {logged}");
    assert!(logged.contains("background boom"), "log: {logged}");
    assert!(!logged.contains("foreground boom"), "log: {logged}");
}

/// A worker's panic carried onto the TUI thread skips the hook, so the session prints
/// it on the way out rather than leaving it only in the log.
#[test]
fn a_resumed_worker_panic_reaches_the_terminal() {
    let (succeeded, stderr, logged) = run_child(true);
    assert!(!succeeded, "the child panicked");
    assert!(stderr.contains("background boom"), "stderr: {stderr}");
    assert!(logged.contains("background boom"), "log: {logged}");
}

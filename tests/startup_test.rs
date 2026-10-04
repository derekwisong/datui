//! The binary in a pseudo-terminal: what reaches the screen at startup, and what
//! does not hold it up.
//!
//! Nothing here is timed. A terminal that never answers, a configuration file that
//! never finishes opening (a FIFO with no writer, which is what a stalled mount looks
//! like to `open`) and keys typed before the app exists all either reach the screen or
//! hang; the hang guard is the failure. Linux only, for `openpty` and `mkfifo`.
#![cfg(target_os = "linux")]

mod common;

use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Only a hang guard; nothing waits this long when the startup is right.
const HANG_GUARD: Duration = Duration::from_secs(60);

const PUSH: &[u8] = b"\x1b[>1u";
const POP: &[u8] = b"\x1b[<1u";
const QUERY: &[u8] = b"\x1b[?u";
const CTRL_Q: &[u8] = b"\x11";

/// One run of the binary on a pseudo-terminal nobody answers for: the test reads what
/// it draws and types for the user.
struct Session {
    child: Child,
    master: std::fs::File,
    out: Vec<u8>,
}

/// Isolated directories: config, cache and home.
struct Dirs {
    root: tempfile::TempDir,
}

impl Dirs {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        for dir in ["config/datui", "cache", "home", "data", "tmp"] {
            std::fs::create_dir_all(root.path().join(dir)).unwrap();
        }
        Self { root }
    }

    fn config_file(&self) -> PathBuf {
        self.root.path().join("config/datui/config.toml")
    }

    /// A three-row CSV whose first label is `ROWMARK`.
    fn csv(&self) -> PathBuf {
        let path = self.root.path().join("data/small.csv");
        std::fs::write(&path, "id,label\n0,ROWMARK\n1,second\n2,third\n").unwrap();
        path
    }

    /// The configuration file as a FIFO: opening it to read waits for a writer.
    fn stalled_config(&self) -> PathBuf {
        let path = self.config_file();
        let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: a valid NUL-terminated path; mkfifo reads nothing else.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        path
    }

    fn spawn(&self, args: &[&Path]) -> Session {
        self.spawn_with(args, None)
    }

    /// As [`Self::spawn`], with standard input a pipe the test writes and the
    /// pseudo-terminal the binary's controlling terminal, so keys still reach it through
    /// `/dev/tty`. Its temporary files go to `tmp`.
    fn spawn_piped(&self, args: &[&Path]) -> (Session, std::io::PipeWriter) {
        let (reader, writer) = std::io::pipe().unwrap();
        (self.spawn_with(args, Some(Stdio::from(reader))), writer)
    }

    /// Where a piped run spools standard input.
    fn tmp(&self) -> PathBuf {
        self.root.path().join("tmp")
    }

    /// `stdin`, when given, in place of the pseudo-terminal, which stays the
    /// controlling terminal.
    fn spawn_with(&self, args: &[&Path], stdin: Option<Stdio>) -> Session {
        use std::os::unix::process::CommandExt;
        let mut master = 0;
        let mut slave = 0;
        let size = libc::winsize {
            ws_row: 30,
            ws_col: 120,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: the out-parameters are valid; no name buffer or termios is passed.
        let opened = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        };
        assert_eq!(opened, 0, "openpty");
        // SAFETY: openpty returned these descriptors, and nothing else owns them.
        let (master, slave) =
            unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
        let root = self.root.path();
        let mut command = Command::new(env!("CARGO_BIN_EXE_datui"));
        command
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("TERM", "xterm-256color")
            .env("LANG", "C.UTF-8")
            .env("HOME", root.join("home"))
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_CACHE_HOME", root.join("cache"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env("DATUI_CACHE_DIR", root.join("cache/datui"))
            .env("TMPDIR", self.tmp())
            .current_dir(root.join("data"));
        match stdin {
            Some(stdin) => {
                command.stdin(stdin);
                // SAFETY: setsid and ioctl are async-signal-safe. A session of its own,
                // with the pseudo-terminal (its stdout) as the controlling terminal: what
                // `/dev/tty` opens when standard input is not a terminal.
                unsafe {
                    command.pre_exec(|| {
                        if libc::setsid() < 0 || libc::ioctl(1, libc::TIOCSCTTY, 0) < 0 {
                            return Err(std::io::Error::last_os_error());
                        }
                        Ok(())
                    });
                }
            }
            None => {
                command.stdin(Stdio::from(slave.try_clone().unwrap()));
            }
        }
        command
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave));
        let child = command.spawn().expect("the binary starts");
        Session {
            child,
            master: std::fs::File::from(master),
            out: Vec::new(),
        }
    }
}

impl Session {
    /// Read what the binary drew until `needle` is in it.
    #[track_caller]
    fn wait_for(&mut self, needle: &[u8]) {
        let deadline = Instant::now() + HANG_GUARD;
        while !contains(&self.out, needle) {
            assert!(
                Instant::now() < deadline,
                "never drew {:?}; drew {:?}",
                String::from_utf8_lossy(needle),
                String::from_utf8_lossy(&self.out)
            );
            if !self.read_some() && self.child.try_wait().unwrap().is_some() {
                panic!(
                    "exited before drawing {:?}: {:?}",
                    String::from_utf8_lossy(needle),
                    String::from_utf8_lossy(&self.out)
                );
            }
        }
    }

    /// Read what is there within a moment; false when nothing came.
    fn read_some(&mut self) -> bool {
        let mut poll = libc::pollfd {
            fd: self.master.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        if unsafe { libc::poll(&mut poll, 1, 50) } <= 0 {
            return false;
        }
        let mut buf = [0u8; 65536];
        match self.master.read(&mut buf) {
            Ok(n) if n > 0 => {
                self.out.extend_from_slice(&buf[..n]);
                true
            }
            // EIO once the binary has closed its side.
            _ => false,
        }
    }

    fn type_keys(&mut self, bytes: &[u8]) {
        self.master.write_all(bytes).unwrap();
    }

    /// Wait for the binary to exit, reading what it draws meanwhile.
    #[track_caller]
    fn wait_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + HANG_GUARD;
        loop {
            self.read_some();
            if let Some(status) = self.child.try_wait().unwrap() {
                while self.read_some() {}
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "never exited; drew {:?}",
                String::from_utf8_lossy(&self.out)
            );
        }
    }

    fn count(&self, needle: &[u8]) -> usize {
        self.out
            .windows(needle.len())
            .filter(|w| *w == needle)
            .count()
    }

    /// Wait until the screen, as a terminal would show it, holds `text` on one line.
    #[track_caller]
    fn wait_for_screen(&mut self, text: &str) {
        let deadline = Instant::now() + HANG_GUARD;
        while !screen(&self.out).iter().any(|row| row.contains(text)) {
            assert!(
                Instant::now() < deadline,
                "never showed {text:?}; the screen:\n{}",
                screen(&self.out).join("\n")
            );
            self.read_some();
        }
    }
}

/// The text a terminal would show for `out`, row by row: cursor positioning and
/// printing, which is all a frame is made of; colors and modes are skipped.
fn screen(out: &[u8]) -> Vec<String> {
    let (rows, cols) = (30, 120);
    let mut grid = vec![vec![' '; cols]; rows];
    let (mut row, mut col) = (0usize, 0usize);
    let text = String::from_utf8_lossy(out);
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() != Some(&'[') {
                chars.next();
                continue;
            }
            chars.next();
            let mut params = String::new();
            let mut last = '\0';
            for c in chars.by_ref() {
                if ('\x40'..='\x7e').contains(&c) {
                    last = c;
                    break;
                }
                params.push(c);
            }
            match last {
                'H' => {
                    let mut at = params.split(';').map(|n| n.parse::<usize>().unwrap_or(1));
                    row = at.next().unwrap_or(1).saturating_sub(1).min(rows - 1);
                    col = at.next().unwrap_or(1).saturating_sub(1);
                }
                'J' if params == "2" => grid = vec![vec![' '; cols]; rows],
                _ => {}
            }
        } else if !c.is_control() {
            if col < cols {
                grid[row][col] = c;
            }
            col += 1;
        }
    }
    grid.into_iter()
        .map(|r| r.into_iter().collect::<String>())
        .collect()
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A terminal that never answers anything costs nothing: the rows arrive with no
/// capability question asked, Ctrl+Enter's protocol is pushed once, and popped once
/// on the way out.
#[test]
fn rows_arrive_on_a_terminal_that_never_answers() {
    let dirs = Dirs::new();
    let csv = dirs.csv();
    let mut session = dirs.spawn(&[&csv]);
    session.wait_for(b"ROWMARK");
    assert!(!contains(&session.out, QUERY), "nothing was asked");
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
    assert_eq!(session.count(PUSH), 1, "pushed once");
    assert_eq!(session.count(POP), 1, "popped once");
    let push = session.out.windows(PUSH.len()).position(|w| w == PUSH);
    let pop = session.out.windows(POP.len()).position(|w| w == POP);
    assert!(push < pop, "pushed before it is popped");
}

/// Settings that never finish reading still give a screen, and Ctrl+Q leaves it with
/// the terminal handed back.
#[test]
fn a_stalled_config_shows_a_screen_that_ctrl_q_leaves() {
    let dirs = Dirs::new();
    dirs.stalled_config();
    let csv = dirs.csv();
    let mut session = dirs.spawn(&[&csv]);
    session.wait_for_screen("Reading settings...");
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
    assert_eq!(session.count(POP), 1, "the keyboard flags are popped");
}

/// Settings that arrive late are the ones used, and keys typed while they were read
/// reach the app, in order, once it exists.
#[test]
fn keys_typed_while_the_settings_are_read_reach_the_app() {
    let dirs = Dirs::new();
    let fifo = dirs.stalled_config();
    // No paths: the home screen, where typed letters go to its filter.
    let mut session = dirs.spawn(&[]);
    session.wait_for_screen("Reading settings...");
    session.type_keys(b"zqx");
    // Let the settings through: a configuration whose one setting shows on screen.
    let writer = std::thread::spawn(move || {
        let mut fifo = std::fs::OpenOptions::new().write(true).open(fifo).unwrap();
        fifo.write_all(b"[display]\nunicode = \"never\"\n").unwrap();
    });
    session.wait_for_screen("zqx");
    writer.join().unwrap();
    let drawn = screen(&session.out).join("\n");
    assert!(
        !drawn.contains('\u{2502}') && !drawn.contains('\u{2500}'),
        "the late settings' plain ASCII is the app's:\n{drawn}"
    );
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
}

/// A path that is not there ends the run with the file named and a failing status,
/// as it did when it was checked before the terminal was taken.
#[test]
fn a_missing_path_ends_the_run_with_its_name() {
    let dirs = Dirs::new();
    let missing = dirs.root.path().join("data/nope.csv");
    let mut session = dirs.spawn(&[&missing]);
    let status = session.wait_exit();
    assert_eq!(status.code(), Some(1));
    let out = String::from_utf8_lossy(&session.out);
    assert!(out.contains("File not found"), "{out}");
    assert!(out.contains("nope.csv"), "{out}");
}

/// With no terminal at all, an unusable configuration is still what is reported, not
/// the missing terminal: there is no screen for the read to hide behind.
#[test]
fn an_unusable_config_is_reported_without_a_terminal() {
    use std::os::unix::process::CommandExt;
    let dirs = Dirs::new();
    std::fs::write(dirs.config_file(), "[analysis]\nchart_rows = 0\n").unwrap();
    let csv = dirs.csv();
    let root = dirs.root.path();
    let mut command = Command::new(env!("CARGO_BIN_EXE_datui"));
    command
        .arg(&csv)
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("DATUI_CACHE_DIR", root.join("cache/datui"))
        .env("HOME", root.join("home"))
        .stdin(Stdio::null());
    // SAFETY: setsid is async-signal-safe; it leaves the child without a controlling
    // terminal, so /dev/tty cannot stand in for the missing one.
    unsafe {
        command.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    let out = command.output().expect("the binary runs");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("analysis.chart_rows"), "{stderr}");
}

/// A configuration the app cannot use ends the run with its error and a failing
/// status, as before.
#[test]
fn an_unusable_config_ends_the_run_with_its_error() {
    let dirs = Dirs::new();
    std::fs::write(dirs.config_file(), "[analysis]\nchart_rows = 0\n").unwrap();
    let csv = dirs.csv();
    let mut session = dirs.spawn(&[&csv]);
    let status = session.wait_exit();
    assert_eq!(status.code(), Some(1));
    let out = String::from_utf8_lossy(&session.out);
    assert!(out.contains("analysis.chart_rows"), "{out}");
    assert!(out.contains("Fix the configuration"), "{out}");
}

/// Each exit status `datui_cli::exit` documents (and the manpages list), one test
/// per status. Quitting is 0 (`rows_arrive_on_a_terminal_that_never_answers`), a
/// missing path 1 (`a_missing_path_ends_the_run_with_its_name`).
mod exit_status {
    use super::*;
    use datui_cli::exit;

    #[test]
    fn an_unknown_option_is_a_usage_error() {
        let out = Command::new(env!("CARGO_BIN_EXE_datui"))
            .arg("--no-such-option")
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs");
        assert_eq!(out.status.code(), Some(exit::USAGE));
        let out = Command::new(env!("CARGO_BIN_EXE_datui"))
            .args(["config", "nope"])
            .stdin(Stdio::null())
            .output()
            .expect("the binary runs");
        assert_eq!(out.status.code(), Some(exit::USAGE));
    }

    /// A signal ends the session as a quit does, the terminal handed back, with
    /// 128 + the signal's number.
    fn ended_by(signal: libc::c_int, status: i32) {
        let dirs = Dirs::new();
        let csv = dirs.csv();
        let mut session = dirs.spawn(&[&csv]);
        session.wait_for(b"ROWMARK");
        let pid = session.child.id() as libc::pid_t;
        // SAFETY: a plain kill of the child this test started and still holds.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
        assert_eq!(session.wait_exit().code(), Some(status));
        assert_eq!(session.count(POP), 1, "the terminal is handed back");
    }

    #[test]
    fn sigint_is_interrupted() {
        ended_by(libc::SIGINT, exit::INTERRUPTED);
    }

    #[test]
    fn sigterm_is_terminated() {
        ended_by(libc::SIGTERM, exit::TERMINATED);
    }

    #[test]
    fn sighup_is_hangup() {
        ended_by(libc::SIGHUP, exit::HANGUP);
    }
}

/// The files in `dir`.
fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

/// `bytes` gzipped.
fn gzipped(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

/// A one-row Parquet file whose label is `ROWMARK`.
fn parquet() -> Vec<u8> {
    use polars::prelude::*;
    let mut df = df!("id" => [0i64], "label" => ["ROWMARK"]).unwrap();
    let mut out = Vec::new();
    ParquetWriter::new(&mut out).finish(&mut df).unwrap();
    out
}

/// CSV, gzipped CSV, NDJSON and Parquet piped to `datui -` each open, their format
/// read from their first bytes, and a gzipped TSV as `--format tsv` says (#567); keys
/// reach the app through the terminal meanwhile, and the spooled file is gone once it
/// quits.
#[test]
fn data_piped_in_opens_in_its_format() {
    let csv = b"id,label\n0,ROWMARK\n1,second\n".to_vec();
    let tsv = b"id\tlabel\n0\tROWMARK\n1\tsecond\n".to_vec();
    let dash: &[&Path] = &[Path::new("-")];
    let as_tsv: &[&Path] = &[Path::new("--format"), Path::new("tsv"), Path::new("-")];
    let cases = [
        ("csv", dash, csv.clone()),
        ("gzip csv", dash, gzipped(&csv)),
        ("gzip tsv", as_tsv, gzipped(&tsv)),
        (
            "ndjson",
            dash,
            b"{\"id\": 0, \"label\": \"ROWMARK\"}\n{\"id\": 1, \"label\": \"x\"}\n".to_vec(),
        ),
        ("parquet", dash, parquet()),
    ];
    for (name, args, body) in cases {
        let dirs = Dirs::new();
        let (mut session, mut pipe) = dirs.spawn_piped(args);
        pipe.write_all(&body).unwrap();
        drop(pipe);
        session.wait_for_screen("ROWMARK");
        let drawn = screen(&session.out).join("\n");
        // Read as text, a JSON line or a binary file would show its quotes and braces.
        assert!(
            !drawn.contains('"') && !drawn.contains('{'),
            "{name}:\n{drawn}"
        );
        session.type_keys(CTRL_Q);
        assert!(session.wait_exit().success(), "{name}");
        assert_eq!(
            files_in(&dirs.tmp()),
            0,
            "{name}: the spooled file is removed"
        );
    }
}

/// An Arrow IPC stream piped in, as pyarrow writes one, is converted and opens; the
/// spooled file and the converted copy are both gone once it quits.
#[test]
fn an_arrow_stream_piped_in_opens() {
    common::ensure_sample_data();
    let body = std::fs::read("tests/sample-data/people_stream.arrow").unwrap();
    let dirs = Dirs::new();
    let (mut session, mut pipe) = dirs.spawn_piped(&[Path::new("-")]);
    pipe.write_all(&body).unwrap();
    drop(pipe);
    session.wait_for_screen("Lastname1");
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
    assert_eq!(files_in(&dirs.tmp()), 0, "both temp files are removed");
}

/// A padded logger export piped in, gzipped, reads with its dialect: the header from
/// the lines `--header-rows` names past the comments, the padding skipped.
#[test]
fn a_dialect_reaches_data_piped_in() {
    let log = b"#device_info, model=\"X\"\n#yyyy-mm-dd, degrees\n  Lcl Date,     Latitude\n          ,             \n2024-03-01,    40.100000\n#reset\n2024-03-02,    40.200000\n".to_vec();
    let args: &[&Path] = &[
        Path::new("--comment"),
        Path::new("#"),
        Path::new("--header-rows"),
        Path::new("3,2"),
        Path::new("--skip-initial-space"),
        Path::new("-"),
    ];
    let dirs = Dirs::new();
    let (mut session, mut pipe) = dirs.spawn_piped(args);
    pipe.write_all(&gzipped(&log)).unwrap();
    drop(pipe);
    session.wait_for_screen("Latitude degrees");
    session.wait_for_screen("/ 3");
    let drawn = screen(&session.out).join("\n");
    assert!(
        drawn.contains("Lcl Date yyyy-mm-dd") && drawn.contains("f64"),
        "{drawn}"
    );
    assert!(!drawn.contains("reset"), "{drawn}");
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
}

/// No path and data piped in reads it. A producer that is slow shows what has come
/// in so far, and Ctrl+O while it is read puts the read down and removes the partial
/// file, though the producer has not finished.
#[test]
fn a_slow_producer_shows_progress_and_ctrl_o_removes_the_partial_file() {
    const CTRL_O: &[u8] = b"\x0f";
    let dirs = Dirs::new();
    let (mut session, mut pipe) = dirs.spawn_piped(&[]);
    pipe.write_all(b"id,label\n0,ROWMARK\n").unwrap();
    session.wait_for_screen("Reading stdin");
    session.wait_for_screen("19 B");
    assert_eq!(files_in(&dirs.tmp()), 1, "the partial file is there");
    session.type_keys(CTRL_O);
    let deadline = Instant::now() + HANG_GUARD;
    while files_in(&dirs.tmp()) > 0 {
        assert!(
            Instant::now() < deadline,
            "the partial file was never removed"
        );
        session.read_some();
    }
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());
    drop(pipe);
}

/// `datui -` with nothing piped in says so, rather than reading the keyboard as data.
#[test]
fn a_dash_with_a_terminal_on_stdin_says_nothing_is_piped() {
    let dirs = Dirs::new();
    let mut session = dirs.spawn(&[Path::new("-")]);
    let status = session.wait_exit();
    assert_eq!(status.code(), Some(1));
    let out = String::from_utf8_lossy(&session.out);
    assert!(out.contains("Nothing is piped"), "{out}");
}

/// `/dev/null` on standard input, where a launcher points it, is nothing piped in:
/// no path opens the home screen, and `-` says nothing is piped.
#[test]
fn dev_null_on_stdin_is_nothing_piped_in() {
    let dirs = Dirs::new();
    let mut session = dirs.spawn_with(&[], Some(Stdio::null()));
    session.wait_for_screen("narrow and search");
    session.type_keys(CTRL_Q);
    assert!(session.wait_exit().success());

    let mut session = dirs.spawn_with(&[Path::new("-")], Some(Stdio::null()));
    assert_eq!(session.wait_exit().code(), Some(1));
    let out = String::from_utf8_lossy(&session.out);
    assert!(out.contains("Nothing is piped"), "{out}");
}

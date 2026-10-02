use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use datui::{App, AppEvent, OpenOptions};

#[allow(dead_code)]
static INIT: Once = Once::new();

/// How long a wait goes before it fails the test. Only a hang guard; nothing is timed.
#[allow(dead_code)]
pub const HANG_GUARD: Duration = Duration::from_secs(300);

/// Whether the app still owes a result: `busy`, the row count, or a footer pass still
/// reading the dataset's schema. What a test waits on rather than a quiet spell on the
/// channel, which on a loaded machine says nothing. Abandoned work is not waited on; a
/// cancelled analysis can run for minutes.
#[allow(dead_code)]
pub fn work_pending(app: &App) -> bool {
    app.is_busy() || app.row_count_pending() || footers_pending(app)
}

fn footers_pending(app: &App) -> bool {
    app.data_table_state
        .as_ref()
        .is_some_and(|state| state.footers_pending().is_some())
}

/// The next event: one already on the channel, or one background work still owes.
/// `None` once nothing is there and nothing is owed.
#[allow(dead_code)]
#[track_caller]
pub fn next_event(app: &App, rx: &Receiver<AppEvent>) -> Option<AppEvent> {
    next_event_within(app, rx, HANG_GUARD)
}

/// [`next_event`] with its own guard. Fails the test, naming the wait and what was
/// still owed, rather than returning: a wait that ended early would fall through to
/// asserts on the previous state.
#[allow(dead_code)]
#[track_caller]
pub fn next_event_within(app: &App, rx: &Receiver<AppEvent>, guard: Duration) -> Option<AppEvent> {
    let caller = std::panic::Location::caller();
    let deadline = Instant::now() + guard;
    loop {
        if let Ok(event) = rx.try_recv() {
            return Some(event);
        }
        // The run loop paints after every update; a count waiting for the rows to be
        // painted starts then.
        if app.count_waits_for_a_frame() {
            return Some(AppEvent::FramePainted);
        }
        if !work_pending(app) {
            return None;
        }
        assert!(
            Instant::now() < deadline,
            "background work never reported back to the wait at {caller} within {guard:?}: \
             busy={}, row_count_pending={}, footers_pending={}, input_mode={:?}, generation={}",
            app.is_busy(),
            app.row_count_pending(),
            footers_pending(app),
            app.input_mode,
            app.task_generation(),
        );
        if let Ok(event) = rx.recv_timeout(Duration::from_millis(50)) {
            return Some(event);
        }
    }
}

/// Handle events, and every event they chain to, until nothing is queued and no
/// background work is owed.
#[allow(dead_code)]
#[track_caller]
pub fn drain_events(app: &mut App, rx: &Receiver<AppEvent>) {
    while let Some(event) = next_event(app, rx) {
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
    }
}

/// Open `paths` and handle the load chain, background results included, until the
/// table, its row count and its footers are in. A crash is handled and ends the wait.
#[allow(dead_code)]
#[track_caller]
pub fn pump_open_until_loaded(
    app: &mut App,
    rx: &Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) {
    let mut next = Some(AppEvent::Open(paths, options));
    loop {
        match next.take() {
            Some(event @ AppEvent::Crash(_)) => {
                app.event(&event);
                return;
            }
            Some(event) => next = app.event(&event),
            None => match next_event(app, rx) {
                Some(event) => next = Some(event),
                None => return,
            },
        }
    }
}

/// Returns a tokio runtime handle for use in tests.
#[allow(dead_code)]
pub fn test_runtime() -> tokio::runtime::Handle {
    // Every test that builds an App comes through here, so this is the one place
    // that guarantees none of them writes to the developer's real cache.
    isolate_cache();
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test tokio runtime")
    })
    .handle()
    .clone()
}

/// A scratch directory named at random and kept for the life of the process.
///
/// A static is never dropped, so the directories are removed by an exit handler.
fn scratch_dir_until_exit(prefix: &str) -> PathBuf {
    static SCRATCH: std::sync::Mutex<Vec<tempfile::TempDir>> = std::sync::Mutex::new(Vec::new());
    unsafe extern "C" {
        fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
    }
    extern "C" fn remove_scratch_dirs() {
        if let Ok(mut held) = SCRATCH.lock() {
            held.clear();
        }
    }
    static REGISTER: Once = Once::new();
    // SAFETY: the C runtime's `atexit`, present on every platform std runs on; the
    // callback only drops the directories held above.
    REGISTER.call_once(|| unsafe {
        atexit(remove_scratch_dirs);
    });
    let dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("a scratch directory for the test process");
    let path = dir.path().to_path_buf();
    SCRATCH.lock().unwrap_or_else(|e| e.into_inner()).push(dir);
    path
}

/// Point the cache at a scratch directory for the whole test process.
///
/// Opening a dataset records it as recent, and tests open plenty. Without this a
/// test run writes its fixtures into the developer's own recent-files list — and
/// several tests running at once corrupt it, since they all rewrite the same file.
///
/// The variable is process-wide, so this is done once and as early as possible.
///
/// The directories are named at random, not by process id: ids are reused, and a run
/// that landed on a finished run's id inherited its recents and templates. They are
/// removed when the process exits.
#[allow(dead_code)]
pub fn isolate_cache() {
    static ISOLATE: Once = Once::new();
    ISOLATE.call_once(|| {
        let dir = scratch_dir_until_exit("datui-test-cache-");
        // The config directory holds templates, so a test App saving one without this
        // override writes it into the developer's own template list.
        let config_dir = scratch_dir_until_exit("datui-test-config-");
        // SAFETY: test-only. Tests run on parallel threads, so this can race another test
        // reading the environment; accepted in tests and never done outside them.
        unsafe { std::env::set_var("DATUI_CACHE_DIR", &dir) };
        unsafe { std::env::set_var("DATUI_CONFIG_DIR", &config_dir) };
    });
}

/// A fresh directory for one test's own fixtures, removed when the process exits.
///
/// `tests/sample-data` holds the generated fixtures and is shared by every test
/// process: worktrees can link one copy, and a full run can overlap a scoped one.
/// Polars memory-maps what it reads, so rewriting a file there can truncate one that
/// another process has mapped, which then dies with SIGBUS. Tests read from
/// `tests/sample-data` and write here.
#[allow(dead_code)]
pub fn fixture_dir() -> PathBuf {
    scratch_dir_until_exit("datui-test-fixture-")
}

/// The config TOML `layers` describe, lowest precedence first, as an import chain
/// stacks them.
#[allow(dead_code)]
pub fn layered_config(layers: &[&str]) -> datui::config::AppConfig {
    datui::config::AppConfig::from_layers(
        layers
            .iter()
            .map(|text| datui::config::ConfigLayer::parse(text).expect("test config layer parses")),
    )
    .expect("test config layers resolve")
}

/// Each bordered box drawn in `rows` (one string per screen row), as the row of
/// its bottom-left corner, in the active glyph set.
///
/// The ASCII set, which a locale without UTF-8 gets (Windows always), draws every
/// corner as `+`, so counting the corner glyph finds four per box there. A
/// bottom-left corner is the one with the left side above it and the bottom edge
/// after it.
#[allow(dead_code)]
pub fn frame_bottoms(rows: &[String]) -> Vec<usize> {
    let b = datui::glyphs::get().border;
    let grid: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.chars().map(String::from).collect())
        .collect();
    let at = |x: usize, y: usize| grid.get(y).and_then(|r| r.get(x)).map(String::as_str);
    // Where each corner has a glyph of its own, every one on screen is a box's,
    // whatever its shape: count them all, and every box opened must close.
    let corners = [b.top_left, b.top_right, b.bottom_left, b.bottom_right];
    if (1..4).all(|i| !corners[..i].contains(&corners[i])) {
        let opened: usize = rows.iter().map(|r| r.matches(b.top_left).count()).sum();
        let closed: Vec<usize> = rows
            .iter()
            .enumerate()
            .flat_map(|(y, r)| std::iter::repeat_n(y, r.matches(b.bottom_left).count()))
            .collect();
        assert_eq!(opened, closed.len(), "every frame closes: {rows:#?}");
        return closed;
    }
    let mut bottoms = Vec::new();
    for (y, row) in grid.iter().enumerate().skip(1) {
        for x in 0..row.len() {
            if at(x, y) == Some(b.bottom_left)
                && at(x, y - 1) == Some(b.vertical_left)
                && at(x + 1, y) == Some(b.horizontal_bottom)
                && (x == 0 || at(x - 1, y) != Some(b.horizontal_bottom))
            {
                bottoms.push(y);
            }
        }
    }
    bottoms
}

/// Ensures that sample data files are generated before tests run.
/// This function uses `std::sync::Once` to ensure it only runs once,
/// even if called from multiple tests.
#[allow(dead_code)]
pub fn ensure_sample_data() {
    INIT.call_once(|| {
        let sample_data_dir = Path::new("tests/sample-data");

        // Check if key files exist to determine if we need to generate data
        // We check for a few representative files that should always be generated
        let key_files = [
            "people.parquet",
            "sales.parquet",
            "large_dataset.parquet",
            "empty.parquet",
            "pivot_long.parquet",
            "melt_wide.parquet",
            "models/tiny.gguf",
        ];

        let needs_generation = !sample_data_dir.exists()
            || key_files
                .iter()
                .any(|file| !sample_data_dir.join(file).exists());

        if needs_generation {
            eprintln!("Sample data not found. Generating test data...");

            // Get the path to the Python script
            let script_path = Path::new("scripts/generate_sample_data.py");
            if !script_path.exists() {
                panic!(
                    "Sample data generation script not found at: {}. \
                    Please ensure you're running tests from the repository root.",
                    script_path.display()
                );
            }

            // Prefer the project virtualenv: the generator needs Polars and friends,
            // which a system Python almost never has. Falling straight through to
            // `python3` produces a bare ImportError that tells nobody what to do.
            let venv_python = if cfg!(windows) {
                Path::new(".venv/Scripts/python.exe")
            } else {
                Path::new(".venv/bin/python")
            };

            let python_cmd = if venv_python.exists() {
                venv_python.to_string_lossy().into_owned()
            } else if Command::new("python3").arg("--version").output().is_ok() {
                "python3".to_string()
            } else if Command::new("python").arg("--version").output().is_ok() {
                "python".to_string()
            } else {
                panic!(
                    "Python not found, and no project virtualenv at {}.\n\
                     Run ./scripts/dev/setup-test-data.sh to create one and generate \
                     the fixtures these tests read.",
                    venv_python.display()
                );
            };

            // Run the generation script
            let output = Command::new(python_cmd)
                .arg(script_path)
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to run sample data generation script: {}. \
                        Make sure Python is installed and the script is executable.",
                        e
                    );
                });

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let hint = if venv_python.exists() {
                    String::new()
                } else {
                    format!(
                        "\n\nNo virtualenv at {}. This usually means the generator's \
                         dependencies (Polars, NumPy, pyarrow, fastavro, openpyxl) are \
                         missing.\nRun ./scripts/dev/setup-test-data.sh to set it up.",
                        venv_python.display()
                    )
                };
                panic!(
                    "Sample data generation failed!\n\
                    Exit code: {:?}\n\
                    stdout:\n{}\n\
                    stderr:\n{}{}",
                    output.status.code(),
                    stdout,
                    stderr,
                    hint
                );
            }

            eprintln!("Sample data generation complete!");
        }
    });
}

use std::path::PathBuf;
use std::sync::Once;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use datui::{App, AppEvent, OpenOptions};

#[allow(dead_code)]
#[path = "../../crates/datui-lib/src/tests/shared.rs"]
mod shared;
#[allow(unused_imports)]
pub use shared::{buffer_lines, buffer_text, ensure_sample_data, sample_data_dir, test_runtime};

/// How long a wait goes before it fails the test. Only a hang guard; nothing is timed.
#[allow(dead_code)]
pub const HANG_GUARD: Duration = Duration::from_secs(300);

/// Whether the app still owes a result: `busy`, the row count, a footer pass still
/// reading the dataset's schema, or the Pivot & Melt preview. What a test waits on
/// rather than a quiet spell on the channel, which on a loaded machine says nothing.
/// Abandoned work is not waited on; a cancelled analysis can run for minutes.
#[allow(dead_code)]
pub fn work_pending(app: &App) -> bool {
    app.is_busy()
        || app.row_count_pending()
        || footers_pending(app)
        || app.reshape_preview_pending()
}

/// What the home screen still owes, for a wait that timed out: every worker flag a
/// settle could be waiting on, so the failure names the answer that never came.
#[allow(dead_code)]
pub fn home_pending(app: &App) -> String {
    let home = &app.home;
    let search = &home.search;
    format!(
        "browsing {:?}, filter {:?}, listing {}, sections waiting {}, awaiting {:?}, \
         search {{ running {}, done {}, scoring {}, root {:?}, epoch {}, indexed {}, limited {:?} }}, \
         returning {:?}, measuring {}, classifying {}, peeking {}, busy {}",
        home.browsing,
        home.filter,
        home.listing_in_flight,
        home.sections_waiting(),
        home.awaiting_listing(),
        search.running,
        search.done,
        search.scoring,
        search.root,
        search.epoch,
        search.indexed,
        search.limited,
        home.returning,
        home.measure_in_flight,
        home.classify_in_flight,
        home.peeking.len(),
        app.is_busy(),
    )
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
            next = app.event(event);
        }
    }
}

/// Read the rows the table's view needs, as the app does after a change: a job,
/// handled here with everything it chains to.
#[allow(dead_code)]
#[track_caller]
pub fn read_rows(app: &mut App, rx: &Receiver<AppEvent>) {
    app.spawn_async_collect(App::LOADING_BUFFER);
    drain_events(app, rx);
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
                app.event(event);
                return;
            }
            Some(event) => next = app.event(event),
            None => match next_event(app, rx) {
                Some(event) => next = Some(event),
                None => return,
            },
        }
    }
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
/// that landed on a finished run's id inherited its recents and views. They are
/// removed when the process exits.
#[allow(dead_code)]
pub fn isolate_cache() {
    static ISOLATE: Once = Once::new();
    ISOLATE.call_once(|| {
        let dir = scratch_dir_until_exit("datui-test-cache-");
        // The config directory holds views, so a test App saving one without this
        // override writes it into the developer's own view list.
        let config_dir = scratch_dir_until_exit("datui-test-config-");
        // SAFETY: test-only. Tests run on parallel threads, so this can race another test
        // reading the environment; accepted in tests and never done outside them.
        unsafe { std::env::set_var("DATUI_CACHE_DIR", &dir) };
        unsafe { std::env::set_var("DATUI_CONFIG_DIR", &config_dir) };
        // A developer's own format specs would change how a test's files open.
        unsafe { std::env::remove_var("DATUI_FORMATS_PATH") };
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

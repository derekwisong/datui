//! Allocation budgets for the home screen's per-key paths, counted on the thread that
//! handles keys: a change that multiplies the work of a keystroke fails here before
//! anyone feels it. Workers' reads are not counted; what a key costs the event loop is.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::{App, AppEvent};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::cell::Cell;
use std::sync::mpsc::Receiver;
use std::time::Instant;
use tempfile::TempDir;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNTED: Cell<u64> = const { Cell::new(0) };
}

/// Counts allocations made on a thread while it has asked to be counted.
struct ThreadCount;

impl ThreadCount {
    fn note() {
        // `try_with`: an allocation can come while the thread's locals are torn down.
        let _ = COUNTING.try_with(|on| {
            if on.get() {
                let _ = COUNTED.try_with(|n| n.set(n.get() + 1));
            }
        });
    }

    /// The allocations `f` makes on this thread.
    fn during(f: impl FnOnce()) -> u64 {
        let before = COUNTED.with(Cell::get);
        COUNTING.with(|on| on.set(true));
        f();
        COUNTING.with(|on| on.set(false));
        COUNTED.with(Cell::get) - before
    }
}

unsafe impl std::alloc::GlobalAlloc for ThreadCount {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::note();
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::note();
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new: usize) -> *mut u8 {
        Self::note();
        unsafe { std::alloc::System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCount = ThreadCount;

fn handle(app: &mut App, event: AppEvent) {
    let mut next = Some(event);
    while let Some(event) = next {
        next = app.event(event);
    }
}

/// What the home screen still has out: a listing, a search, a measuring or classifying
/// batch.
fn home_busy(app: &App) -> bool {
    let home = &app.home;
    home.listing_in_flight
        || home.search.running
        || home.measure_in_flight
        || home.classify_in_flight
}

/// Allocations on the loop's thread, apart: handling events and keys, and drawing frames.
#[derive(Debug, Default)]
struct Tally {
    handled: u64,
    frames: u64,
    drawn: u64,
}

impl Tally {
    fn handle(&mut self, app: &mut App, event: AppEvent) {
        self.handled += ThreadCount::during(|| handle(app, event));
    }

    fn draw(&mut self, app: &mut App, area: Rect) {
        self.frames += ThreadCount::during(|| {
            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);
            app.request_what_the_frame_needs();
        });
        self.drawn += 1;
    }
}

/// Run the loop as the app does at its busiest, a frame after every event, until the
/// home screen has nothing out and nothing is queued.
fn run_until_quiet(app: &mut App, rx: &Receiver<AppEvent>, area: Rect, tally: &mut Tally) {
    let deadline = Instant::now() + crate::common::HANG_GUARD;
    loop {
        tally.draw(app, area);
        if let Ok(event) = rx.try_recv() {
            tally.handle(app, event);
            continue;
        }
        if !home_busy(app) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the home screen never settled: {}",
            crate::common::home_pending(app)
        );
        if let Ok(event) = rx.recv_timeout(crate::common::FRAME_WAIT) {
            tally.handle(app, event);
        }
    }
}

/// Filter keystrokes over a directory of 2,000 files. The first reveals every row
/// (a filter lifts the directory's cut), and the rows near the screen are measured;
/// the rest narrow the list. Once 5.4 million allocations a key: every measurement
/// that landed folded every row again and scored the whole listing again (#813).
#[test]
fn a_filter_keystroke_over_thousands_of_files_stays_in_budget() {
    let tmp = TempDir::new().unwrap();
    let words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    for i in 0..2_000 {
        let name = format!("report_{i:05}_{}.csv", words[i % words.len()]);
        std::fs::write(tmp.path().join(name), b"a,b\n1,2\n").unwrap();
    }
    let _cwd = crate::in_cwd(tmp.path());

    let mut config = datui::config::AppConfig::default();
    config.home.desktop_recents = false;
    config.home.hide = vec!["examples".to_string()];
    config.cloud.discover = Some(datui::config::CloudDiscover::None);
    let (tx, rx) = std::sync::mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        crate::common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    let cache = TempDir::new().unwrap();
    app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
    let area = Rect::new(0, 0, 160, 50);
    app.enter_home();
    run_until_quiet(&mut app, &rx, area, &mut Tally::default());
    assert!(
        crate::visible_names(&app.home)
            .iter()
            .any(|n| n == "report_00001_bravo.csv"),
        "the directory is listed: {:?}",
        crate::visible_names(&app.home)
    );

    let mut per_key = Vec::new();
    for c in "rprt4".chars() {
        let measured = app.home.enriched.len();
        let mut tally = Tally::default();
        tally.handle(
            &mut app,
            AppEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)),
        );
        run_until_quiet(&mut app, &rx, area, &mut tally);
        per_key.push((c, app.home.enriched.len() - measured, tally));
    }
    assert_eq!(app.home.filter, "rprt4");

    // Measured: 5,000 allocations handling a key, 13 more for each measurement it
    // brings, 2,100 drawing a frame (the rows folded in, the list built, the screen
    // drawn), 7,000 to 10,500 for a key that measures nothing, frames and all. Before
    // #813: 2,100 a measurement, 25,000 a frame, 51,000 a key, and the first key measured
    // every row it revealed. The budgets are about twice what they are now.
    let (_, measured, _) = &per_key[0];
    assert!(
        (1..=150).contains(measured),
        "the first key measured the rows near the screen, not all 2,000: {per_key:?}"
    );
    for (c, measured, tally) in &per_key {
        assert!(
            tally.frames / tally.drawn <= 4_500,
            "typing {c:?}, {} frames made {} allocations: {per_key:?}",
            tally.drawn,
            tally.frames
        );
        assert!(
            tally.handled <= 10_000 + 30 * *measured as u64,
            "typing {c:?} made {} allocations handling it and {measured} measurements: \
             {per_key:?}",
            tally.handled
        );
        if *measured == 0 {
            let total = tally.handled + tally.frames;
            assert!(
                total <= 21_000,
                "typing {c:?} made {total} allocations: {per_key:?}"
            );
        }
    }
}

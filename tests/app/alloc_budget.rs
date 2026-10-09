//! Allocation budgets for keys the table repeats, and the home screen's filter and
//! measuring: a change that multiplies the work a key does fails here. Counted on the
//! test's own thread (the key and the frame it draws), so background jobs and other
//! tests do not count. Budgets are about twice what was measured.
//!
//! What they do not see: work moved off the UI thread (onto Polars' or rayon's pool)
//! is not counted, however much it allocates. They were measured on a debug build;
//! a release build allocates less, which only adds slack. A Polars upgrade that
//! changes what `Column::get` or `AnyValue` allocate can move the numbers, which the
//! headroom is for.

use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Passes every allocation to the system allocator, counting those of a thread
/// inside [`counted`].
struct ThreadCount;

thread_local! {
    /// Allocation calls and bytes on this thread since [`counted`] began, or `None`.
    static COUNTED: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

fn note(size: usize) {
    let _ = COUNTED.try_with(|counted| {
        if let Some((calls, bytes)) = counted.get() {
            counted.set(Some((calls + 1, bytes + size)));
        }
    });
}

unsafe impl GlobalAlloc for ThreadCount {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        note(new);
        unsafe { System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCount = ThreadCount;

/// Allocation calls and bytes `f` makes on this thread.
fn counted(f: impl FnOnce()) -> (usize, usize) {
    COUNTED.with(|c| c.set(Some((0, 0))));
    f();
    COUNTED.with(Cell::take).expect("counting")
}

const SCREEN: Rect = Rect::new(0, 0, 160, 50);

/// `df` written as Parquet and opened, drawn, with every answer the open owes in.
fn open_settled(df: &mut DataFrame, name: &str) -> (App, mpsc::Receiver<AppEvent>) {
    let path = common::fixture_dir().join(name);
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    // The first frame sets the rows on screen, which the buffer is then read for.
    for _ in 0..3 {
        app.render(SCREEN, &mut Buffer::empty(SCREEN));
        app.frame_painted();
        drain_events(&mut app, &rx);
    }
    (app, rx)
}

/// Allocation calls and bytes per press of `code`, each press drawn as the run loop
/// draws it. Answers the presses ask for are handled between presses, uncounted.
fn per_key(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    code: KeyCode,
    presses: usize,
) -> (usize, usize) {
    let mut buf = Buffer::empty(SCREEN);
    let (mut calls, mut bytes) = (0, 0);
    for _ in 0..presses {
        let (c, b) = counted(|| {
            press_key(app, code, KeyModifiers::NONE);
            buf.reset();
            app.render(SCREEN, &mut buf);
            app.frame_painted();
        });
        calls += c;
        bytes += b;
        drain_events(app, rx);
    }
    (calls / presses, bytes / presses)
}

fn cursor_row(app: &App) -> usize {
    app.data_table_state
        .as_ref()
        .and_then(|state| state.selected_display_row())
        .unwrap()
}

/// Text of `len` characters from `words`, a line break now and then; the same for
/// the same `seed`.
fn prose(words: &[&str], seed: usize, len: usize) -> String {
    let mut text = String::with_capacity(len * 3);
    let mut i = seed;
    let mut chars = 0;
    while chars < len {
        i = i
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let word = words[(i >> 33) % words.len()];
        text.push_str(word);
        text.push(if (i >> 20).is_multiple_of(29) {
            '\n'
        } else {
            ' '
        });
        chars += word.chars().count() + 1;
    }
    text
}

/// A news-like table: a headline, two bodies of 2 to 20 KB (one in CJK), and a page
/// of 1 MB in every 25th row.
fn news(rows: usize) -> DataFrame {
    let latin = [
        "market", "council", "report", "the", "of", "police", "said", "zebra",
    ];
    let cjk = ["東京", "市場", "報告", "警察", "。", "、"];
    let page = prose(&latin, 7, 1 << 20);
    df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "headline" => (0..rows).map(|r| prose(&latin, r, 60 + r % 60)).collect::<Vec<_>>(),
        "body" => (0..rows).map(|r| prose(&latin, r + 1, 2_000 + r * 97 % 18_000)).collect::<Vec<_>>(),
        "body_zh" => (0..rows).map(|r| prose(&cjk, r + 2, 700 + r * 31 % 6_000)).collect::<Vec<_>>(),
        "page" => (0..rows).map(|r| (r % 25 == 0).then(|| page.clone())).collect::<Vec<_>>(),
    )
    .unwrap()
}

/// A row down on a page of long text costs what its cells show: one new row of
/// cells, the rest moved, nothing the size of a value.
#[test]
fn row_down_over_long_text_stays_within_budget() {
    let (mut app, rx) = open_settled(&mut news(200), "alloc_budget_news.parquet");
    // Past the first page, so every press scrolls.
    per_key(&mut app, &rx, KeyCode::Char('j'), 60);
    let from = cursor_row(&app);
    let (calls, bytes) = per_key(&mut app, &rx, KeyCode::Char('j'), 30);
    assert_eq!(cursor_row(&app), from + 30);
    // Measured: 987 allocations and 97 KB a key; before cells were cut to the screen
    // and kept through a scroll, 1,537 and 1.2 MB.
    assert!(calls <= 2_000, "{calls} allocations per key");
    assert!(bytes <= 200_000, "{bytes} bytes per key");
}

/// A row down on a wide numeric page formats one row, not the page.
#[test]
fn row_down_over_numbers_stays_within_budget() {
    let rows = 400;
    let mut columns: Vec<Column> = (0..24)
        .map(|c| {
            let values: Vec<i64> = (0..rows)
                .map(|r| (r * 7919 + c * 104_729) % 2_000_003 - 1_000_000)
                .collect();
            Column::new(format!("int_{c}").into(), values)
        })
        .collect();
    columns.extend((0..12).map(|c| {
        let values: Vec<f64> = (0..rows)
            .map(|r| 1000.0 + ((r + c) as f64 * 0.37).sin() * 250.0)
            .collect();
        Column::new(format!("float_{c}").into(), values)
    }));
    let mut df = DataFrame::new(rows as usize, columns).unwrap();
    let (mut app, rx) = open_settled(&mut df, "alloc_budget_wide.parquet");
    per_key(&mut app, &rx, KeyCode::Char('j'), 60);
    let from = cursor_row(&app);
    let (calls, bytes) = per_key(&mut app, &rx, KeyCode::Char('j'), 30);
    assert_eq!(cursor_row(&app), from + 30);
    // Measured: 2,446 allocations and 336 KB a key, most of them ratatui's rows and
    // cells; before the cells were kept through a scroll, 7,965 and 566 KB.
    assert!(calls <= 5_000, "{calls} allocations per key");
    assert!(bytes <= 700_000, "{bytes} bytes per key");
}

/// What the home screen still has out: a listing, a search walk or its scoring, a
/// measuring or classifying batch, a cloud peek.
fn home_busy(app: &App) -> bool {
    let home = &app.home;
    home.listing_in_flight
        || home.search.running
        || home.search.scoring
        || home.measure_in_flight
        || !home.classifying.is_empty()
        || !home.peeking.is_empty()
}

/// Allocation calls on this thread, apart: handling events and keys, and drawing frames.
#[derive(Debug, Default)]
struct Tally {
    handled: usize,
    frames: usize,
    drawn: usize,
}

impl Tally {
    fn handle(&mut self, app: &mut App, event: AppEvent) {
        self.handled += counted(|| {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(event);
            }
        })
        .0;
    }

    fn draw(&mut self, app: &mut App) {
        self.frames += counted(|| {
            app.render(SCREEN, &mut Buffer::empty(SCREEN));
            app.request_what_the_frame_needs();
        })
        .0;
        self.drawn += 1;
    }
}

/// Run the loop as the app does at its busiest, a frame after every event, until the
/// home screen has nothing out and nothing is queued.
fn home_until_quiet(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tally: &mut Tally) {
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    loop {
        tally.draw(app);
        if let Ok(event) = rx.try_recv() {
            tally.handle(app, event);
            continue;
        }
        if !home_busy(app) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the home screen never settled: {}",
            common::home_pending(app)
        );
        if let Ok(event) = rx.recv_timeout(common::FRAME_WAIT) {
            tally.handle(app, event);
        }
    }
}

/// Browsing a directory of 1,500 files measures the rows on screen, and each
/// measurement costs its row, not the listing; then filter keystrokes narrow it.
/// Once 5.4 million allocations a key over 5,000 files: every measurement that landed
/// folded every row again and scored the whole listing again (#813). 1,500 files keep
/// the search's scoring inline (`SCORE_INLINE_MAX` is 2,000).
#[test]
fn home_filter_keys_and_measurements_stay_within_budget() {
    let dir = tempfile::TempDir::new().unwrap();
    let words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    for i in 0..1_500 {
        let name = format!("report_{i:05}_{}.csv", words[i % words.len()]);
        std::fs::write(dir.path().join(name), b"a,b\n1,2\n").unwrap();
    }
    let mut config = datui::config::AppConfig::default();
    config.home.desktop_recents = false;
    config.home.hide = vec!["examples".to_string()];
    config.cloud.discover = Some(datui::config::CloudDiscover::None);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    let cache = tempfile::TempDir::new().unwrap();
    app.use_cache(datui::CacheManager::with_dir(cache.path().to_path_buf()));
    app.home.browsing = Some(dir.path().to_path_buf());
    app.enter_home();
    let mut browsing = Tally::default();
    home_until_quiet(&mut app, &rx, &mut browsing);
    let measured = app.home.enriched.len();
    assert!(
        (1..200).contains(&measured),
        "the rows on screen measured, not the directory: {measured}"
    );

    let mut per_key = Vec::new();
    for c in "rprt4".chars() {
        let mut tally = Tally::default();
        tally.handle(
            &mut app,
            AppEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)),
        );
        home_until_quiet(&mut app, &rx, &mut tally);
        per_key.push((c, tally));
    }
    assert_eq!(app.home.filter, "rprt4");

    // Measured: 9 allocations handling a measurement, 1,700 drawing a frame while they
    // land (the rows folded in, the list built, the screen drawn); 2,900 to 7,900
    // handling a filter key and the search batches it brings, 2,100 to 2,300 a frame.
    // Before #813: 137 a measurement, 3,100 a frame, 14,700 to 42,100 a key. The
    // budgets are about twice what they are now.
    assert!(
        browsing.handled / measured <= 20,
        "{} allocations handling {measured} measurements",
        browsing.handled
    );
    for (what, tally) in std::iter::once(("browsing", &browsing))
        .chain(per_key.iter().map(|(_, tally)| ("a key", tally)))
    {
        assert!(
            tally.frames / tally.drawn <= 4_500,
            "{what}: {} frames made {} allocations: {per_key:?}",
            tally.drawn,
            tally.frames
        );
    }
    for (c, tally) in &per_key {
        assert!(
            tally.handled <= 16_000,
            "handling {c:?} made {} allocations: {per_key:?}",
            tally.handled
        );
    }
}

/// Allocation calls and bytes per press of `code`, each press drawn and its reads
/// asked for and handled as the run loop does them.
fn per_key_with_reads(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    code: KeyCode,
    presses: usize,
) -> (usize, usize) {
    let mut buf = Buffer::empty(SCREEN);
    let (calls, bytes) = counted(|| {
        for _ in 0..presses {
            press_key(app, code, KeyModifiers::NONE);
            buf.reset();
            app.render(SCREEN, &mut buf);
            app.frame_painted();
            app.request_what_the_frame_needs();
            drain_events(app, rx);
        }
    });
    (calls / presses, bytes / presses)
}

/// Page down through a CSV, reading ahead as it goes: what the UI thread does for a
/// key, its frame and the reads ahead it plans and installs.
#[test]
fn page_down_through_a_csv_stays_within_budget() {
    let path = common::fixture_dir().join("alloc_budget_pages.csv");
    let mut text = String::from("id,name,when,amount,flag\n");
    for i in 0..30_000 {
        text.push_str(&format!(
            "{i},name {i},2020-01-{:02},{}.25,{}\n",
            i % 28 + 1,
            i * 7 % 1_000,
            i % 2 == 0
        ));
    }
    std::fs::write(&path, text).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    // The first frame sets the rows on screen, which the buffer is then read for.
    for _ in 0..3 {
        app.render(SCREEN, &mut Buffer::empty(SCREEN));
        app.frame_painted();
        drain_events(&mut app, &rx);
    }
    per_key_with_reads(&mut app, &rx, KeyCode::PageDown, 20);
    let from = app.data_table_state.as_ref().unwrap().start_row();
    let (calls, bytes) = per_key_with_reads(&mut app, &rx, KeyCode::PageDown, 60);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.start_row() >= from + 60 * 40, "paged down");
    // Measured: 1,564 allocations and 104 KB a key, the reads ahead landing included.
    // The reads themselves run on workers: `table::fill_tests` bounds the rows they read.
    assert!(calls <= 3_500, "{calls} allocations per key");
    assert!(bytes <= 200_000, "{bytes} bytes per key");
}

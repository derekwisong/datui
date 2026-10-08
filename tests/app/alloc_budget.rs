//! Allocation budgets for keys the table repeats: a change that multiplies the work
//! a key does fails here. Counted on the test's own thread (the key and the frame it
//! draws), so background jobs and other tests do not count. Budgets are about twice
//! what was measured.

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

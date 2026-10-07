//! The table itself: keys, scrolling, wide tables, number formatting, the palette.

use super::*;

#[test]
fn test_app_creation() {
    let (tx, _) = mpsc::channel();
    let app = App::new(tx, common::test_runtime());
    assert!(app.at_table());
}

/// The table's letter keys are unmodified keys: Ctrl+E must not open Export
/// and Ctrl+R must not reverse. Paging (Ctrl+F/B/D/U) keeps its modifiers.
#[test]
fn modified_letters_are_not_table_feature_keys() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        !matches!(app.overlay, Overlay::Export { .. }),
        "Ctrl+E is not e"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('y'),
        KeyModifiers::CONTROL,
    )));
    assert_ne!(app.overlay, Overlay::Copy, "Ctrl+Y is not y");

    // And the plain letter still works. (Paging keeps Ctrl+F/B/D/U: those
    // four are the guard's explicit exceptions, matching their declared arms.)
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(app.overlay, Overlay::Export { .. }),
        "e still opens Export"
    );
}

/// Regression for commit 7b7bfe8: holding PageDown at the end of the data once
/// pushed `start_row` past `num_rows`, leaving the app `busy` because every spawn
/// no-op'd (buffer already valid after clamp) but the handler used to gate on
/// `needs && spawn`. Now `slide_table` clamps forward scroll, and the App
/// `handle_scroll` clears `busy` whether or not the spawn actually ran.
#[test]
fn test_scroll_past_end_does_not_hang_busy() {
    // Inline 200-row CSV so the test stays cheap and self-contained.
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("scroll_past_end_test.csv");
    let mut df = polars::df!(
        "id" => (0..200i64).collect::<Vec<_>>(),
        "value" => (0..200i64).map(|i| i * 10).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render once so visible_rows is set for real, then settle the post-render bounce.
    let settle = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>| {
        for _ in ticks() {
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);
            app.frame_painted();
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }
            while let Ok(ev) = rx.try_recv() {
                if let Some(next) = app.event(ev) {
                    let _ = tx.send(next);
                }
            }
            if !app.is_busy() && !needs {
                return;
            }
            common::wait_for_event(tx, rx);
        }
    };
    settle(&mut app, &rx, &tx);

    let total = app.data_table_state.as_ref().unwrap().num_rows();
    assert!(total > 0, "test data should have rows");

    // Jump to end via End key, then hammer PageDown a bunch — same sequence that
    // used to wedge the app. Each PageDown sets `busy=true` in the key handler;
    // the deferred scroll must clear it once the spawn no-ops past the bottom.
    if let Some(next) = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::End,
        KeyModifiers::NONE,
    ))) {
        let _ = tx.send(next);
    }
    settle(&mut app, &rx, &tx);

    for i in 0..15 {
        if let Some(next) = app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::PageDown,
            KeyModifiers::NONE,
        ))) {
            let _ = tx.send(next);
        }
        settle(&mut app, &rx, &tx);
        assert!(
            !app.is_busy(),
            "iteration {i}: PageDown past end must not leave busy stuck"
        );
    }
}

/// An object with nothing in it, whose name said it was data, is a write that stopped.
///
/// The note leads with it because it is the one skip that is a fault rather than a
/// tidy-up — and because nothing else can see it: a file that holds no bytes has no
/// footer to fail to read.
#[test]
fn test_a_write_that_stopped_is_said_to_have_stopped() {
    use datui::formats::schema_union::SkippedFiles;
    let note = datui::notes::from_dataset(
        &datui::formats::schema_union::union_file_schemas(
            &[],
            datui::formats::schema_union::SchemaOrigin::AllFooters(0),
        )
        .with_skipped(SkippedFiles {
            bookkeeping: 2,
            not_parquet: 1,
            empty: 1,
        }),
    );
    let said: Vec<&str> = note.iter().map(|n| n.summary.as_str()).collect();
    assert_eq!(
        said,
        ["skipped: 1 empty file, 1 file not Parquet, 2 writer bookkeeping files"],
        "the stopped write first, then the mistake, then the tidy-up"
    );
}

/// Every `DataTableState` must get its own `len_generation`. They used to all start at
/// zero, so an exact row count still running for the dataset you just closed matched
/// the one you just opened and set its row count to the wrong number.
#[test]
fn test_len_generations_are_unique_across_datasets() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );
    let first = app
        .data_table_state
        .as_ref()
        .expect("first dataset should load")
        .len_generation();

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/sales.parquet")],
        OpenOptions::default(),
    );
    let second = app
        .data_table_state
        .as_ref()
        .expect("second dataset should load")
        .len_generation();

    assert_ne!(
        first, second,
        "two datasets must not share a row-count generation"
    );
}

/// A read that widened a column's type says so, and one with no rule behind it does
/// not widen at all.
///
/// `to_supertypes` was added for exactly this case — one `N/A` makes `amount` a String
/// in one file and an Int64 in the next — and the note was written from the column
/// *names*, which agree. So the directory opened with `amount` silently text for every
/// row, where before it had failed loudly.
#[test]
fn test_widening_a_column_is_never_silent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let drifted = tmp.path().join("drifted");
    std::fs::create_dir_all(&drifted).unwrap();
    std::fs::write(drifted.join("a.csv"), "id,amount\n1,10\n").unwrap();
    std::fs::write(drifted.join("b.csv"), "id,amount\n2,N/A\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![drifted.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let state = app.data_table_state.as_ref().expect("it opens");
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|n| n.summary.contains("type differs across files")),
        "the widening is reported: {notes:?}"
    );
    assert!(
        !notes.iter().any(|n| n.summary.contains("same columns")),
        "and not as a disagreement about columns, which these files do not have: {notes:?}"
    );

    // The names agreeing is what made this invisible, so assert they do.
    assert_eq!(state.headers(), vec!["id", "amount"]);
}

/// Wide-table navigation across 300 columns (#462): `]` pages right through every
/// column with no gap and always moves; `[` pages back the same way; `}` fills the
/// last page and `{` returns; the bar names the cursor's column; `g` finds a column
/// by name. The column cursor (#574) rides along: on each page's first column, and
/// the view follows `h` `l` only at the edges.
#[test]
fn wide_table_pages_across_300_columns() {
    let size = (80, 24);
    let (mut app, _rx, _tx) = open_wide_table("wide_nav_300.parquet", 300, size);
    let first = columns_shown(&app).expect("300 columns do not fit");
    assert_eq!(first.first, 1);
    assert_eq!(first.total, 300);
    let screen = draw_sized(&mut app, size);
    let bar = screen.lines().last().unwrap();
    assert!(
        bar.contains("col 1/300"),
        "the bar names the cursor's column: {bar}"
    );

    // Right to the end, a page at a time: each page starts no later than the column
    // after the last one shown, so nothing is skipped, and always moves. The cursor
    // starts each page.
    let mut pages = vec![first];
    loop {
        page_and_draw(&mut app, KeyCode::Right, size);
        let now = columns_shown(&app).unwrap();
        let before = *pages.last().unwrap();
        if (now.first, now.last) == (before.first, before.last) {
            assert_eq!(now.cursor, 300, "] on the last page: its last column");
            break;
        }
        assert!(now.first > before.first, "{before:?} -> {now:?}");
        assert!(now.first <= before.last + 1, "a gap: {before:?} -> {now:?}");
        assert!(now.last - now.first >= 2, "an 80-wide page shows several");
        assert_eq!(now.cursor, now.first, "the cursor starts the page");
        pages.push(now);
    }
    let last = *pages.last().unwrap();
    assert_eq!(last.last, 300, "paging ends on the last column");
    assert!(pages.len() > 20, "{} pages", pages.len());
    let screen = draw_sized(&mut app, size);
    assert!(header_line(&screen).contains("code_299"), "{screen}");
    assert!(screen.lines().last().unwrap().contains("col 300/300"));

    // And back, through the same pages: `[` after `]` goes back to the page it left.
    let mut back = pages.clone();
    back.pop();
    while let Some(expected) = back.pop() {
        page_and_draw(&mut app, KeyCode::Left, size);
        assert_eq!(range_shown(&app), Some((expected.first, expected.last)));
        assert_eq!(cursor_at(&app), expected.first);
    }
    // Paged back from somewhere `]` did not go, `[` still leaves no gap. `h` walks
    // the cursor across the last page, and one more scrolls it a column.
    press_and_draw(&mut app, KeyCode::Char('}'), size);
    assert_eq!(cursor_at(&app), 300);
    for _ in last.first..=last.last {
        press_and_draw(&mut app, KeyCode::Char('h'), size);
    }
    assert_eq!(cursor_at(&app), last.first - 1);
    assert_eq!(
        range_shown(&app).unwrap().0,
        last.first - 1,
        "the view scrolls only at the edge"
    );
    loop {
        let before = columns_shown(&app).unwrap();
        page_and_draw(&mut app, KeyCode::Left, size);
        let now = columns_shown(&app).unwrap();
        if now.first == 1 {
            break;
        }
        assert!(now.first < before.first, "{before:?} -> {now:?}");
        assert!(now.last + 1 >= before.first, "a gap: {now:?} -> {before:?}");
    }

    press_and_draw(&mut app, KeyCode::Char('}'), size);
    assert_eq!(
        range_shown(&app),
        Some((last.first, last.last)),
        "}} lands on the last page"
    );
    assert_eq!(cursor_at(&app), 300);
    press_and_draw(&mut app, KeyCode::Char('{'), size);
    assert_eq!(columns_shown(&app).unwrap().first, 1);
    assert_eq!(cursor_at(&app), 1);

    // Shift+arrows page too; the plain arrows move the cursor one column, and the
    // view only when the cursor would leave it.
    press_key(&mut app, KeyCode::Right, KeyModifiers::SHIFT);
    draw_sized(&mut app, size);
    let paged = columns_shown(&app).unwrap();
    assert!(paged.first > 2, "{paged:?}");
    press_and_draw(&mut app, KeyCode::Char('l'), size);
    assert_eq!(columns_shown(&app).unwrap().first, paged.first);
    assert_eq!(cursor_at(&app), paged.first + 1);
    press_and_draw(&mut app, KeyCode::Left, size);
    press_and_draw(&mut app, KeyCode::Left, size);
    assert_eq!(cursor_at(&app), paged.first - 1);
    assert_eq!(
        columns_shown(&app).unwrap().first,
        paged.first - 1,
        "one column left past the edge scrolls one"
    );
    for _ in paged.first - 1..paged.last {
        press_and_draw(&mut app, KeyCode::Char('l'), size);
    }
    assert_eq!(cursor_at(&app), paged.last);
    assert_eq!(
        columns_shown(&app).unwrap().last,
        paged.last,
        "past the right edge: scrolled just enough to show it whole"
    );
    press_key(&mut app, KeyCode::Left, KeyModifiers::SHIFT);
    draw_sized(&mut app, size);
    press_key(&mut app, KeyCode::Left, KeyModifiers::SHIFT);
    draw_sized(&mut app, size);
    assert_eq!(columns_shown(&app).unwrap().first, 1);

    // g: a picker of the shown columns; typing narrows, Enter goes.
    let screen = press_and_draw(&mut app, KeyCode::Char('g'), size);
    assert_eq!(app.overlay, Overlay::GoToColumn);
    assert!(screen.contains("Go to Column"), "{screen}");
    let screen = type_and_draw(&mut app, "label_150", size);
    assert!(screen.contains("label_150"), "{screen}");
    let screen = press_and_draw(&mut app, KeyCode::Enter, size);
    assert!(app.at_table());
    assert_eq!(columns_shown(&app).unwrap().first, 151);
    assert_eq!(cursor_at(&app), 151, "g moves the cursor");
    assert!(header_line(&screen).contains("label_150"), "{screen}");
    // A column already whole on screen does not move the view.
    press_and_draw(&mut app, KeyCode::Char('g'), size);
    type_and_draw(&mut app, "id_152", size);
    press_and_draw(&mut app, KeyCode::Enter, size);
    assert_eq!(columns_shown(&app).unwrap().first, 151);
    assert_eq!(cursor_at(&app), 153);
    // A name nothing matches keeps the picker open; Esc leaves the view as it was.
    press_and_draw(&mut app, KeyCode::Char('g'), size);
    type_and_draw(&mut app, "zzz", size);
    let screen = press_and_draw(&mut app, KeyCode::Enter, size);
    assert_eq!(app.overlay, Overlay::GoToColumn);
    assert!(screen.contains("No column matches"), "{screen}");
    press_and_draw(&mut app, KeyCode::Esc, size);
    assert!(app.at_table());
    assert_eq!(columns_shown(&app).unwrap().first, 151);
    // The last column lands on a full last page rather than alone.
    press_and_draw(&mut app, KeyCode::Char('g'), size);
    type_and_draw(&mut app, "code_299", size);
    press_and_draw(&mut app, KeyCode::Enter, size);
    assert_eq!(range_shown(&app), Some((last.first, last.last)));
    assert_eq!(cursor_at(&app), 300);
}

/// Paging keeps frozen columns on screen, counts them first, and pages only the
/// columns that scroll; hidden columns are not counted and reordered ones page in
/// their new order.
#[test]
fn wide_table_pages_beside_frozen_and_hidden_columns() {
    let size = (80, 24);
    let (mut app, rx, tx) = open_wide_table("wide_nav_frozen.parquet", 120, size);
    let mut order = app.data_table_state.as_ref().unwrap().headers();
    // Hide two, move the last column to the front, then freeze two.
    order.retain(|c| c != "price_005" && c != "label_006");
    let moved = order.pop().unwrap();
    order.insert(0, moved.clone());
    run_and_settle(&mut app, AppEvent::ColumnOrder(order.clone(), 2), &rx, &tx);
    let screen = draw_sized(&mut app, size);
    assert!(
        header_line(&screen).starts_with(&format!(" {moved}")),
        "{screen}"
    );
    let start = columns_shown(&app).unwrap();
    assert_eq!(start.first, 3, "two frozen lead the count");
    assert_eq!(start.total, 118, "hidden columns are not counted");

    let screen = page_and_draw(&mut app, KeyCode::Right, size);
    let page = columns_shown(&app).unwrap();
    assert!(page.first > start.first);
    assert!(page.first <= start.last + 1);
    assert!(
        header_line(&screen).contains(&moved) && header_line(&screen).contains("id_000"),
        "the frozen columns stay: {screen}"
    );
    let screen = press_and_draw(&mut app, KeyCode::Char('}'), size);
    let last = columns_shown(&app).unwrap();
    assert_eq!(last.last, 118);
    assert!(header_line(&screen).contains("label_118"), "{screen}");
    assert!(header_line(&screen).contains(&moved), "{screen}");
    press_and_draw(&mut app, KeyCode::Char('{'), size);
    assert_eq!(range_shown(&app), Some((start.first, start.last)));
    assert_eq!(cursor_at(&app), 1, "{{ is the first column, a frozen one");

    // The picker lists the shown columns in order, never a hidden one.
    press_and_draw(&mut app, KeyCode::Char('g'), size);
    assert_eq!(app.pickers.go_to_column.items(), order.as_slice());
    // A frozen column is on screen already: choosing it moves nothing.
    type_and_draw(&mut app, "id_000", size);
    press_and_draw(&mut app, KeyCode::Enter, size);
    assert_eq!(range_shown(&app), Some((start.first, start.last)));
    assert_eq!(cursor_at(&app), 2);

    // Every column frozen: nothing scrolls, and the keys do nothing.
    run_and_settle(
        &mut app,
        AppEvent::ColumnOrder(order[..3].to_vec(), 3),
        &rx,
        &tx,
    );
    draw_sized(&mut app, size);
    for key in ['{', '}'] {
        press_and_draw(&mut app, KeyCode::Char(key), size);
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
    }
    for arrow in [KeyCode::Left, KeyCode::Right] {
        page_and_draw(&mut app, arrow, size);
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
    }
    // The cursor still walks them.
    press_and_draw(&mut app, KeyCode::Char('{'), size);
    let screen = press_and_draw(&mut app, KeyCode::Char('l'), size);
    assert_eq!(cursor_at(&app), 2);
    assert!(
        screen.lines().last().unwrap().contains("col 2/3"),
        "{screen}"
    );
}

/// `,` toggles digit grouping, which was `F`; `F` opens Value Counts.
#[test]
fn test_comma_toggles_digit_grouping() {
    let mut config = datui::AppConfig::default();
    config.display.number_format =
        datui::config::NumberFormatConfig::Preset("thousands".to_string());
    let path = common::fixture_dir().join("comma_grouping.csv");
    std::fs::write(&path, "n\n1234567\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 80, 24);
    assert!(painted(&mut app, &rx, &tx, area).contains("1,234,567"));
    press_and_send(&mut app, &tx, KeyCode::Char(','));
    let plain = painted(&mut app, &rx, &tx, area);
    assert!(
        plain.contains("1234567") && !plain.contains("1,234,567"),
        "{plain}"
    );
    press_and_send(&mut app, &tx, KeyCode::Char(','));
    assert!(painted(&mut app, &rx, &tx, area).contains("1,234,567"));

    counts_key(&mut app, &rx, &tx, KeyCode::Char('F'));
    assert_eq!(app.overlay, Overlay::ValueCounts);
    let screen = counts_screen(&mut app, 80, 24);
    assert!(
        screen.contains("1,234,567"),
        "counts follow the grouping: {screen}"
    );
}

/// `+` and `-` on a cell of every type keep or drop exactly the rows with its value:
/// the filter's text reads back to the very value, to the last fraction of a second,
/// in the column's zone, at the column's scale.
#[test]
fn plus_and_minus_keep_the_exact_value_of_every_type() {
    // Each column: the first row's value three times, two others, a null.
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    // 02:30:00.123456 on the night Paris falls back, the second time.
    let ambiguous = 1_729_992_600_123_456i64;
    let pattern = |a: i64, b: i64, c: i64| [Some(a), Some(b), Some(a), None, Some(c), Some(a)];
    let mut df = df!(
        "f" => &[Some(0.1f32), Some(0.2), Some(0.1), None, Some(0.3), Some(0.1)],
        "day" => &[Some(19723i32), Some(19724), Some(19723), None, Some(19725), Some(19723)],
        "ts" => &pattern(1_704_085_200_123_456, 1_704_085_200_123_457, 0),
        "tz" => &pattern(ambiguous, ambiguous - 3_600_000_000, 0),
        "clock" => &pattern(18_367_123_456_789, 18_367_123_456_788, 0),
        "dur" => &pattern(93_784_000_005, 93_784_000_006, -90_000_000),
        "dec" => &[Some("1.50"), Some("2.00"), Some("1.5"), None, Some("3.25"), Some("1.50")],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("day").cast(DataType::Date),
        col("ts").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
        col("tz").cast(DataType::Datetime(TimeUnit::Microseconds, paris)),
        col("clock").cast(DataType::Time),
        col("dur").cast(DataType::Duration(TimeUnit::Microseconds)),
        col("dec").cast(DataType::Decimal(10, 2)),
    ])
    .collect()
    .unwrap();
    let path = common::fixture_dir().join("quick_filter_types.parquet");
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    draw_sized(&mut app, (160, 24));

    for column in ["f", "day", "ts", "tz", "clock", "dur", "dec"] {
        for (pressed, rows) in [('+', 3), ('-', 2)] {
            app.data_table_state
                .as_mut()
                .unwrap()
                .set_current_column(column);
            run_and_settle(&mut app, key_event(pressed), &rx, &tx);
            draw_sized(&mut app, (160, 24));
            let (got, filters) = quick_view(&app);
            assert_eq!(got, rows, "{column} {pressed}: {filters:?}");
            run_and_settle(&mut app, key_event('R'), &rx, &tx);
            run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
            draw_sized(&mut app, (160, 24));
        }
    }
}

/// The first frame is not held for the terminal. With nothing remembered it is drawn
/// dark; an answer that differs, arriving after it, switches the palette and is
/// remembered, so the next start draws its first frame in that palette. An
/// explicit mode is drawn as set, whatever is remembered.
#[test]
fn test_first_frame_uses_the_terminals_last_answer() {
    use datui::cache::CacheManager;
    use datui::config::{AppConfig, ColorConfig, ConfigLayer, Theme, ThemeMode};
    let hex = |s: &str| datui::ColorParser::new().parse(s).expect("color parses");
    let dir = tempfile::tempdir().unwrap();
    let start = |text: &str, answered: Option<ThemeMode>| {
        let config = AppConfig::from_layers([ConfigLayer::parse(text).expect("layer parses")])
            .expect("resolves");
        let theme = Theme::from_config(&config.theme).expect("theme builds");
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new_with_config(tx, common::test_runtime(), theme, config);
        app.use_cache(CacheManager::with_dir(dir.path().to_path_buf()));
        app.settle_first_palette(answered);
        app
    };
    let header = |app: &App| app.theme().table_header_bg();
    let dark = hex(&ColorConfig::dark().table_header_bg);
    let light = hex(&ColorConfig::light().table_header_bg);

    // Answered dark at once, so `COLORFGBG` where the tests run does not matter.
    let auto = "[theme]\nmode = \"auto\"\n";
    let mut app = start(auto, Some(ThemeMode::Dark));
    assert_eq!(header(&app), dark, "nothing remembered: dark");
    app.event(AppEvent::TerminalBackground(ThemeMode::Light));
    assert_eq!(header(&app), light, "a late answer switches");

    let app = start(auto, None);
    assert_eq!(header(&app), light, "the last answer, before any new one");
    let mut app = start(auto, Some(ThemeMode::Dark));
    assert_eq!(header(&app), dark, "an answer already in wins");
    app.event(AppEvent::TerminalBackground(ThemeMode::Dark));
    assert_eq!(header(&start(auto, None)), dark, "remembered again");

    app.event(AppEvent::TerminalBackground(ThemeMode::Light));
    let pinned = start("[theme]\nmode = \"dark\"\n", None);
    assert_eq!(header(&pinned), dark, "an explicit mode ignores it");
}

/// The terminal's answer switches between the two named themes, a theme file
/// included, keeping `theme.colors` over each.
#[test]
fn test_terminal_background_switches_between_named_themes() {
    use datui::config::{AppConfig, ColorConfig, Theme, ThemeMode};
    let hex = |s: &str| datui::ColorParser::new().parse(s).expect("color parses");
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("themes")).unwrap();
    std::fs::write(
        dir.path().join("themes").join("my-dusk.toml"),
        "extends = \"night-market\"\naccent = \"#e0af68\"\ndimmed = \"#111111\"\n",
    )
    .unwrap();
    let root = dir.path().join("config.toml");
    std::fs::write(
        &root,
        "[theme]\ndark = \"my-dusk\"\n[theme.colors]\nfind_match = \"#ff9e64\"\n",
    )
    .unwrap();
    let config = AppConfig::load_from_file(&root).expect("config loads");
    assert!(config.theme.follow);
    let theme = Theme::from_config(&config.theme).expect("theme builds");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, common::test_runtime(), theme, config);

    for _ in 0..2 {
        app.event(AppEvent::TerminalBackground(ThemeMode::Light));
        let light = ColorConfig::light();
        assert_eq!(app.theme().accent(), hex(&light.accent));
        assert_eq!(app.theme().dimmed(), hex(&light.dimmed));
        assert_eq!(app.theme().find_match(), hex("#ff9e64"));

        app.event(AppEvent::TerminalBackground(ThemeMode::Dark));
        assert_eq!(app.theme().accent(), hex("#e0af68"));
        assert_eq!(app.theme().dimmed(), hex("#111111"));
        assert_eq!(
            app.theme().controls_bg(),
            hex(&ColorConfig::dark().controls_bg)
        );
        assert_eq!(app.theme().find_match(), hex("#ff9e64"));
    }
    assert_eq!(app.flash_message(), None);

    // A theme that cannot be used says so on the screen, where stderr is not seen.
    std::fs::write(&root, "[theme]\nmode = \"dark\"\ndark = \"nope\"\n").unwrap();
    let config = AppConfig::load_from_file(&root).expect("config loads");
    let theme = Theme::from_config(&config.theme).expect("theme builds");
    let (tx, _rx) = mpsc::channel();
    let app = App::new_with_config(tx, common::test_runtime(), theme, config);
    let said = app.flash_message().expect("a flash");
    assert!(
        said.contains("nope") && said.contains("night-market"),
        "{said}"
    );
}

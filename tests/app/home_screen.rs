//! The home screen as the App drives it: places, buckets, catalogs, going home and back.

use super::*;

/// The bar counts a listing as the loading screen does, with no percentage beside it.
#[test]
fn test_the_footer_counts_a_listing_without_a_percentage() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Reading schema", 40);
    let progress = app.footer_progress().clone();
    let listing = progress.listing();
    for _ in 0..1500 {
        listing.advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = common::buffer_lines(&buf);
    assert!(
        rows.iter().any(|r| r.contains("Listing files: 1,500")),
        "the body counts them:\n{}",
        rows.join("\n")
    );
    let bar = rows.last().expect("a footer");
    assert!(bar.contains("Listing files: 1,500"), "{bar:?}");
    assert!(
        !bar.contains('%'),
        "a listing has no fraction to show: {bar:?}"
    );
}

/// Abandoning drops the incoming dataset, not the one already on screen. Esc from
/// home has to put the user back where they were.
#[test]
fn test_escape_from_home_returns_to_the_dataset_that_was_open() {
    common::ensure_sample_data();
    let open_first = PathBuf::from("tests/sample-data/people.parquet");
    let abandoned = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(
        vec![open_first.clone()],
        OpenOptions::default(),
    ))
    .unwrap();

    // Render as we go: `visible_rows` is set by the render, and without it there is
    // no display slice to assert on later.
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
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
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    assert_eq!(app.open_path(), Some(open_first.as_path()));
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "first dataset should be displayable before we abandon anything"
    );

    // Start a second load and leave before it can install. Handled here, not sent: a
    // drain of the channel could take the scan's and the schema's answers too, and a
    // fast machine installed the second dataset before Ctrl+O (#522).
    assert!(
        app.event(AppEvent::Open(vec![abandoned], OpenOptions::default()))
            .is_none(),
        "the scan goes to a worker"
    );
    assert!(app.is_busy(), "the second open is on its way");
    app.event(ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);

    // Until the abandoned load has reported everything it was going to.
    for _tick in ticks() {
        let stepped = drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        if stepped == 0 && !app.background_work_in_flight() {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert!(
        app.at_table(),
        "Esc from home should return to the open dataset"
    );
    // Said on arrival, so a reflexive Esc too many does not leave the next keys acting
    // on a table nobody noticed coming back (#547 D14).
    assert_eq!(app.flash_message(), Some("Back to people.parquet"));
    assert_eq!(
        app.open_path(),
        Some(open_first.as_path()),
        "the abandoned load must not have replaced what was open"
    );
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "the dataset we returned to should still have its buffer"
    );
}

/// Going home clears the *load's* busy state, and leaves `task_generation` alone —
/// that counter also gates analysis and export results, which keep running. The
/// loading screen has nothing to type ahead into, so keys typed there are dropped
/// rather than held (a held stray key used to queue `q` behind it).
#[test]
fn test_entering_home_clears_load_state_but_not_task_generation() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    let mut pump = EventPump::new(app, tx, rx);
    // The open and what it sets going, but not the worker's answer: drained from the
    // channel, a fast read could have the table up before the keys below are typed.
    let mut next = Some(AppEvent::Open(vec![path], OpenOptions::default()));
    while let Some(event) = next {
        next = pump.app.event(event);
    }
    assert!(pump.app.is_busy(), "a load in flight should be busy");
    for code in [KeyCode::Char('j'), KeyCode::Enter] {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    }
    assert_eq!(
        pump.held_keys().count(),
        0,
        "the loading screen holds nothing: stray keys are dropped"
    );

    let generation_before = pump.app.task_generation();
    pump.terminal_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
        .unwrap();

    assert_eq!(pump.app.input_mode, InputMode::Home);
    assert!(
        !pump.app.is_busy(),
        "abandoning should clear the load's busy flag"
    );
    assert_eq!(
        pump.app.task_generation(),
        generation_before,
        "going home must not cancel an in-flight export or analysis"
    );
}

#[cfg(feature = "http")]
#[test]
fn test_declining_a_download_goes_home_instead_of_quitting() {
    let (mut app, _rx) = app_awaiting_open_confirmation();
    assert!(
        app.awaiting_open_confirmation(),
        "opening a remote URL should ask before downloading"
    );

    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "declining a download must not quit datui"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "declining a download should leave the user at home"
    );
    assert!(!app.awaiting_open_confirmation());
}

/// q pops the context: a dataset opened from the home screen returns there,
/// one launched straight from the command line quits as it always has. Q is
/// unconditional.
#[test]
fn q_pops_to_home_only_when_home_is_in_the_stack() {
    // Launched straight onto a file: q quits.
    let (mut app, _rx, _tx) = open_query_filter_fixture("q_direct.csv");
    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(out, Some(AppEvent::Exit)),
        "a direct launch keeps q as quit"
    );

    // Opened from the home screen: q returns there.
    let path = common::fixture_dir().join("q_from_home.csv");
    let (mut app, rx, _tx) = open_query_filter_fixture_at(&path, datui::AppConfig::default());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    // As `home_open_path` does before it emits the `Open`.
    app.input_mode = InputMode::Normal;

    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::NONE,
    )));
    assert!(out.is_none(), "q does not quit with home in the stack");
    assert_eq!(app.input_mode, InputMode::Home, "q pops to home");

    // Q stays unconditional, from the same stack.
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    app.input_mode = InputMode::Normal;
    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('Q'),
        KeyModifiers::SHIFT,
    )));
    assert!(matches!(out, Some(AppEvent::Exit)), "Q always quits");
}

/// PgUp/PgDn at home move a screenful, like the table, not a fixed ten rows.
#[test]
fn home_paging_moves_a_screenful() {
    let tmp = tempfile::TempDir::new().unwrap();
    for i in 0..80 {
        std::fs::write(tmp.path().join(format!("f{i:03}.csv")), b"a,b\n1,2\n").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();

    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        app.home.visible().len() >= 80 && app.home.view_height > 0
    });

    let page = app.home.view_height;
    assert!(page > 10, "the fixture should give more than the old ten");
    let before = app.home.selected;
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::PageDown,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.selected,
        (before + page).min(app.home.visible().len() - 1),
        "PgDn moves what one screen holds"
    );
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::PageUp,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.selected, before, "PgUp comes back the same amount");
}

/// Browsing a directory of more than 64 subdirectories: nothing is looked into while the
/// listing is built, so no row is labelled from where it sits, and the rows the frame
/// draws are looked into after it — on a worker, a screenful at a time.
///
/// The bug this covers is #270: the first 64 directories of a 6,241-partition share read
/// `multi`, and every identical one after them read `dir`.
#[test]
fn test_a_big_listing_is_labelled_from_the_viewport_not_from_directory_order() {
    let tmp = tempfile::TempDir::new().unwrap();
    for i in 0..200 {
        let partition = tmp.path().join(format!("d{i:03}")).join("year=2024");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("part.parquet"), b"").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();

    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);

    // The listing lands first, and the frame that draws it says of every row only
    // that nothing has looked into it.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        !app.home.visible().is_empty()
    });
    let unlooked_at = common::buffer_text(&buf);
    assert!(
        !unlooked_at.contains(" dir"),
        "no row should be called a plain directory before anything looked into one:\n\
         {unlooked_at}"
    );
    let unlooked_at_row = format!("d000/  {}", datui::glyphs::get().ellipsis);
    assert!(
        unlooked_at.contains(&unlooked_at_row),
        "an unlooked-at row should read `{unlooked_at_row}`:\n{unlooked_at}"
    );

    // Then the rows that frame drew are looked into, and say what they are. Asked of
    // the rows on screen rather than the selected one: the cursor starts on the row that
    // opens the whole directory, which is not one of the two hundred being looked into.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        app.home.visible().iter().any(|row| match row {
            datui::home::Row::Entry { entry, .. } => {
                entry.kind == datui::home::discover::EntryKind::Hive
            }
            _ => false,
        })
    });
    let looked_at = common::buffer_text(&buf);
    assert!(
        looked_at.contains("hive"),
        "the rows on screen should have been looked into:\n{looked_at}"
    );

    // And the bottom of the listing still has not been, which is the point: the
    // budget follows the viewport rather than directory order.
    let kinds: Vec<datui::home::discover::EntryKind> = app
        .home
        .visible()
        .iter()
        .filter_map(|row| match row {
            datui::home::Row::Entry { entry, .. } => Some(entry.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds.last(),
        Some(&datui::home::discover::EntryKind::Unknown),
        "the bottom of a two-hundred-row listing is nobody's viewport"
    );
}

/// Modals render over the home screen, but home used to consume every key, so one
/// raised while the user was at home could not be dismissed: Esc went to home_escape,
/// which at the time quit when nothing was loaded. The only way past an error was to
/// leave datui.
#[test]
fn test_error_modal_over_home_is_dismissable() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    // A load chosen here fails.
    let missing = common::fixture_dir().join("modal_over_home_missing.csv");
    assert!(
        pump_open_until_error(&mut app, &rx, vec![missing], OpenOptions::default()).is_some(),
        "the open fails"
    );
    assert!(app.modal_showing(), "and says so over the home screen");

    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc should dismiss the modal, not quit out from under it"
    );

    // With the modal gone, Esc is home's again. An empty home has nowhere left to
    // back out to, so it does nothing; Ctrl+C is what quits.
    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc at the top of the home screen must not quit"
    );
    assert_eq!(app.input_mode, InputMode::Home);
    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        matches!(out, Some(AppEvent::Exit)),
        "Ctrl+C quits from the home screen"
    );
}

/// Opening a second dataset from the home screen must not show the first one's rows
/// while the second is still loading. Between the keypress and the new dataset being
/// installed, the old table was still on screen — a page of one file's data under the
/// filename of another, for as long as the load took.
#[test]
fn test_opening_from_home_does_not_show_the_previous_dataset() {
    common::ensure_sample_data();
    let first = PathBuf::from("tests/sample-data/people.parquet");
    let second = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![first.clone()], OpenOptions::default()))
        .unwrap();

    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
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
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    assert!(
        common::buffer_text(&buf).contains("first_name"),
        "the first dataset should be on screen before we go home"
    );

    // Home, then open the second dataset the way a user does: through the path
    // prompt, so the real key path runs rather than a synthesised event.
    app.event(ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);
    app.event(key(KeyCode::Char('~')));
    for c in second.to_str().unwrap().chars() {
        app.event(key(KeyCode::Char(c)));
    }
    // Handled here, not sent, and the first frame drawn before the channel is read: a
    // drain could take every answer of the load, and the first frame would then show
    // it finished.
    let mut next = app.event(key(KeyCode::Enter));
    while let Some(event) = next {
        next = app.event(event);
    }
    // A worker looks at the typed path first; its answer opens it. Taken one event at a
    // time, so nothing of the load is handled before its first frame.
    while !app.at_table() {
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("the look answers");
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(event);
        }
    }
    assert!(
        app.at_table(),
        "opening from home should leave the home screen"
    );
    assert_ne!(app.open_path(), Some(second.as_path()), "still loading");

    // Every frame from here until the second dataset is installed.
    let mut frames = 0usize;
    for _tick in ticks() {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        frames += 1;
        let text = main_area_text(&buf, area);
        assert!(
            !text.contains("first_name") && !text.contains("job_title"),
            "frame {frames} showed the previous dataset while the next one was loading"
        );
        assert!(
            text.contains("large_dataset.parquet") || text.contains("dist_normal"),
            "frame {frames} named neither the dataset being loaded nor the one that arrived"
        );
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
        if app.open_path() == Some(second.as_path()) && !app.is_busy() && !needs {
            break;
        }
        drain_like_main_loop(&mut app, &tx, &rx);
        common::wait_for_event(&tx, &rx);
    }
    assert_eq!(
        app.open_path(),
        Some(second.as_path()),
        "the second dataset should have loaded"
    );
    assert!(frames > 1, "a frame drawn while loading, and one after");
}

/// A load chosen at home that fails is reported at home. It used to put the error over
/// the dataset open before, which is not where the user was when they chose.
#[test]
fn a_load_chosen_at_home_fails_at_home() {
    common::ensure_sample_data();
    let first = PathBuf::from("tests/sample-data/people.parquet");
    let dir = tempfile::tempdir().unwrap();
    let broken = dir.path().join("broken.parquet");
    std::fs::write(&broken, "not parquet").unwrap();

    let area = Rect::new(0, 0, 120, 40);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    // Its own recents: in the shared store, fifty opens elsewhere push these out.
    let cache = datui::CacheManager::with_dir(dir.path().join("cache"));
    app.use_cache(cache.clone());
    tx.send(AppEvent::Open(vec![first], OpenOptions::default()))
        .unwrap();
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if app.data_table_state.is_some() && !app.is_busy() {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    assert!(app.data_table_state.is_some());

    app.event(ctrl_o());
    let type_at_prompt = |app: &mut App, path: &std::path::Path| {
        app.event(key(KeyCode::Char('~')));
        for c in path.to_str().unwrap().chars() {
            app.event(key(KeyCode::Char(c)));
        }
        if let Some(next) = app.event(key(KeyCode::Enter)) {
            tx.send(next).unwrap();
        }
    };
    type_at_prompt(&mut app, &broken);
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if !app.is_busy() && app.input_mode == InputMode::Home {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    assert_eq!(app.input_mode, InputMode::Home);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    // The message from the app, the dialog from the screen: a long temp path (Windows')
    // wraps inside the name.
    let message = app.error_message().expect("the failure is said");
    assert!(message.contains("broken.parquet\": "), "{message}");
    let text = common::buffer_text(&buf);
    assert!(text.contains("Error") && text.contains("broken"), "{text}");

    // Nor is it a recent: recorded when a dataset installs, not when it is asked for.
    // The one that did load is, and recording is off-thread, so that is waited for.
    let recorded = |path: &std::path::Path| {
        let path = datui::canonical::canonicalize(path).unwrap();
        cache.load_recents().contains(&path)
    };
    app.settle_cache_writes();
    assert!(recorded(std::path::Path::new(
        "tests/sample-data/people.parquet"
    )));
    assert!(!recorded(&broken), "a file that failed is not a recent");

    // Dismissed, it is not said a second time beside the prompt: the dialog said it
    // (#547 D8).
    app.event(key(KeyCode::Enter));
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.status, None);

    // A file no reader takes is refused before anything is read.
    let model = dir.path().join("model.onnx");
    std::fs::write(&model, "onnx").unwrap();
    app.home.status = None;
    while rx.try_recv().is_ok() {}
    type_at_prompt(&mut app, &model);
    // The prompt lists the directory being typed meanwhile, and a worker looks at the
    // path; nothing is opened.
    for _tick in ticks() {
        let mut idle = true;
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(event, AppEvent::Open(..)), "nothing was opened");
            idle = false;
            if let Some(next) = app.event(event) {
                tx.send(next).unwrap();
            }
        }
        if idle && !app.is_busy() {
            break;
        }
        common::wait_for_event(&tx, &rx);
    }
    // A typed path has no row to dim, so the line says it.
    assert_eq!(
        app.home.status.as_deref(),
        Some(datui::home::discover::NO_READER)
    );
}

/// Esc at a bucket's top goes back to the home listing, not to a directory named `gs:`.
#[test]
fn test_escape_from_a_bucket_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(PathBuf::from("gs://bucket"));

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A remote location shows that it is being listed, not "No datasets here.", until its
/// listing arrives.
#[test]
fn test_remote_listing_shows_progress_until_it_arrives() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let dir = PathBuf::from("gs://bucket/demo");
    app.home.browsing = Some(dir.clone());

    let area = Rect::new(0, 0, 100, 20);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        common::buffer_text(&buf)
    };

    let waiting = screen(&mut app);
    assert!(waiting.contains("Listing gs://bucket/demo"), "{waiting}");
    assert!(!waiting.contains("No datasets here."), "{waiting}");

    app.home.probe_ready(dir, Vec::new(), false);
    let done = screen(&mut app);
    assert!(!done.contains("Listing gs://bucket/demo"), "{done}");
    assert_eq!(app.home.waiting_since, None);
}

/// Backspace still climbs above where a browse began, and Esc from there goes back to
/// the listing rather than on up the tree.
#[test]
fn test_escape_after_backspace_above_the_start_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    std::fs::create_dir_all(&start).unwrap();
    app.home.browsing = Some(start.clone());
    app.home.browse_start = Some(start);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Backspace,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing.as_deref(), Some(tmp.path()));
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
}

/// `→` goes inside a local directory that opens as one dataset, as it has always done in
/// a bucket.
///
/// The gate was `is_object_store_url`, so on a local hive tree or a local directory of
/// part files there was no way in at all: Enter opened the whole thing, `←`/`→` folded
/// the section, and the files inside were unreachable from the home screen. That is the
/// escape hatch for a directory classified wrongly, and locally there was none.
#[test]
fn test_right_goes_inside_a_local_multi_file_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("sales");
    std::fs::create_dir_all(&directory).unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(directory.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::MultiFile),
        "a directory of part files is offered as one dataset"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ browsed into the directory rather than folding the section"
    );
}

/// `Enter` on a place row browses the place: the way back to a directory found by
/// hand, now that a recent's directory is no longer a section of its own.
#[test]
fn test_enter_on_a_place_row_browses_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, _) = app_with_recents_in_two_places(&tmp, false);
    let here = recents[0].parent().unwrap().to_path_buf();

    // The bar says → goes inside, the same as on any directory.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the door: {bar:?}");

    app.event(key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
    assert_eq!(
        app.home.browse_start.as_deref(),
        Some(here.as_path()),
        "Esc comes back from here to the listing"
    );

    // → is the other door to the same place.
    app.event(key(KeyCode::Esc));
    assert_eq!(app.home.browsing, None);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("back at the listing");
    app.home.selected = row;
    app.event(key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
}

/// `Delete` on a place row forgets every recent under it and nothing else, after
/// asking. What is checked is the store, which is what the next launch reads.
#[test]
fn test_delete_on_a_place_row_forgets_exactly_its_recents_after_confirming() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, cache) = app_with_recents_in_two_places(&tmp, true);
    let holds =
        |cache: &datui::CacheManager, path: &Path| cache.load_recents().iter().any(|p| p == path);
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(key(KeyCode::Delete));
    let area = Rect::new(0, 0, 120, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(
        screen.contains("Forget 2 recently opened datasets under"),
        "asked first, and told how many: {screen:?}"
    );
    assert!(
        recents.iter().all(|p| holds(&cache, p)),
        "nothing is forgotten until the question is answered"
    );

    // Declined: the store is untouched, and a later confirmation is not armed.
    app.event(key(KeyCode::Esc));
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(key(KeyCode::Delete));
    app.event(key(KeyCode::Enter));
    assert!(
        !holds(&cache, &recents[0]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        !holds(&cache, &recents[1]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        holds(&cache, &recents[2]),
        "the other place's recent is left alone: {:?}",
        cache.load_recents()
    );
}

/// Ctrl+D adds the row under the cursor to catalog.toml, and pressed on a row from
/// catalog.toml forgets it there. A place row adds the directory. What is checked is
/// the file, which is what the next listing reads.
#[test]
fn test_ctrl_d_adds_to_the_catalog_and_forgets() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, _cache) = app_with_recents_in_two_places(&tmp, false);
    let config = tmp.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    app.use_catalog_dir(&config).unwrap();
    let here = recents[0].parent().unwrap().to_path_buf();
    let catalog = config.join("catalog.toml");
    let listed = |path: &Path| {
        datui::home::catalog::read(&catalog, "mine", datui::home::catalog::Origin::Mine)
            .unwrap()
            .is_some_and(|c| c.dataset_at(path).is_some())
    };

    // The cursor is on the place row for `here`.
    app.event(ctrl('d'));
    assert!(listed(&here), "{:?}", std::fs::read_to_string(&catalog));
    assert!(
        app.flash_message()
            .is_some_and(|s| s.starts_with("Added") && s.ends_with("My datasets")),
        "{:?}",
        app.flash_message()
    );
    assert!(
        std::fs::read_to_string(&catalog)
            .unwrap()
            .starts_with(datui::home::catalog::MINE_TEMPLATE),
        "a new catalog.toml says what it is"
    );

    app.event(ctrl('d'));
    assert!(!listed(&here), "a second press forgets it");
    assert!(app.flash_message().is_some_and(|s| s.starts_with("Forgot")));

    // A file row adds the file.
    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == recents[2]),
        )
        .expect("the recent in the other place is listed");
    app.home.selected = row;
    app.event(ctrl('d'));
    assert!(listed(&recents[2]), "a file row adds the file");

    // Delete on the same file under Recent forgets the recent, not the catalog entry.
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| {
            matches!(r, datui::home::Row::Entry { section, entry, .. }
                if entry.path == recents[2]
                    && app.home.sections[*section].title == datui::home::HomeState::RECENT_SECTION)
        })
        .expect("the recent is still listed");
    app.home.selected = row;
    app.event(key(KeyCode::Delete));
    assert!(listed(&recents[2]), "the catalog keeps it");
}

/// The places Ctrl+D kept in the cache before catalogs move into catalog.toml the
/// first time the home screen is listed, and the cache's list goes.
#[test]
fn test_remembered_places_move_into_catalog_toml() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = datui::canonical::canonicalize(tmp.path()).unwrap();
    let kept = root.join("kept");
    std::fs::create_dir_all(&kept).unwrap();
    let cache = datui::CacheManager::with_dir(root.join("cache"));
    cache
        .save_remembered_places(std::slice::from_ref(&kept))
        .unwrap();
    let config = root.join("config");
    std::fs::create_dir_all(&config).unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.use_cache(cache.clone());
    app.use_catalog_dir(&config).unwrap();
    app.enter_home();
    let catalog = datui::home::catalog::read(
        &config.join("catalog.toml"),
        "mine",
        datui::home::catalog::Origin::Mine,
    )
    .unwrap()
    .expect("catalog.toml written");
    assert!(catalog.dataset_at(&kept).is_some(), "{catalog:?}");
    assert!(cache.load_remembered_places().is_empty(), "moved once");
    assert!(
        app.home.catalogs.iter().any(|c| c.label == "My datasets"),
        "listed at once"
    );
}

/// `Enter` and `→` on the place of a recent opened over HTTP say why they do nothing,
/// rather than listing a URL and reporting it unreachable.
#[test]
fn test_the_place_of_an_http_recent_says_it_cannot_be_browsed() {
    common::isolate_cache();
    let url = PathBuf::from("https://example.com/data/y.csv");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(std::slice::from_ref(&url));
    let place = PathBuf::from("https://example.com/data");
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == place))
        .expect("the URL's prefix is its place");
    app.home.selected = row;

    // The bar does not offer the door.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(!bar.contains("Inside"), "{bar:?}");
    // And the details pane says why, before Enter is pressed.
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(screen.contains("none over HTTP"), "{screen}");

    app.event(key(KeyCode::Enter));
    assert_eq!(app.home.browsing, None);
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("HTTP")),
        "{:?}",
        app.home.status
    );
    // The line answers the last key: the next one takes it down.
    app.event(key(KeyCode::Right));
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.home.status, None, "gone at the next key");
}

/// With thirty list rows or more, a blank line precedes every section header but the
/// first; below that, none. The spacers are counted against the cap and the scroll,
/// so the selected row is always on screen.
#[test]
fn test_a_tall_list_spaces_its_sections_and_a_short_one_does_not() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let configured = tmp.path().join("configured");
    std::fs::write(
        {
            std::fs::create_dir_all(&configured).unwrap();
            configured.join("c.parquet")
        },
        b"x",
    )
    .unwrap();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let config = datui::AppConfig {
        read_catalogs: vec![
            datui::home::catalog::parse(
                &format!(
                    "[configured]\nname = \"Configured\"\npath = {:?}\n",
                    configured.to_string_lossy()
                ),
                "mine",
                datui::home::catalog::Origin::Mine,
                None,
            )
            .unwrap(),
        ],
        ..Default::default()
    };
    app.home.set_catalogs(datui::home::catalogs(&config));
    app.home.rebuild(&recents);

    let is_header = |line: &str| {
        line.contains("RECENT")
            || line.contains("current directory")
            || line.contains("catalog.toml")
    };
    // Tall: the wordmark and prompt take the top rows; the list below has room.
    let area = Rect::new(0, 0, 100, 50);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            rows[at - 1].trim().is_empty(),
            "a blank line precedes the header at {at}: {:?}",
            rows[at - 1]
        );
    }
    assert!(
        !rows[headers[0] - 1].trim().is_empty()
            || headers[0] == 0
            || rows[headers[0] - 1].contains("filter"),
        "no blank line before the first header"
    );
    let spacers = headers.len() - 1;
    let list_height = app.home.view_height + spacers;
    assert!(list_height >= 30, "{list_height}");
    assert_eq!(
        app.home.view_height,
        list_height - spacers,
        "the spacers come off the height the cap is a share of"
    );

    // The last row on screen, selected: drawn, spacers and all.
    let last = app.home.visible().len() - 1;
    app.home.selected = last;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    let name = match app.home.selected_row() {
        Some(datui::home::Row::Entry { entry, .. }) => entry.name.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        screen.contains(&name),
        "the selected row is on screen: {screen:?}"
    );

    // Short: dense.
    let area = Rect::new(0, 0, 100, 24);
    app.home.selected = 0;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            !rows[at - 1].trim().is_empty(),
            "no blank line before the header at {at} on a short screen"
        );
    }
}

/// The hint, and the descent, are only offered on a row that is a dataset directory.
/// `→` elsewhere goes on expanding the section, which on a visible row is already
/// expanded and so does nothing.
#[test]
fn test_right_does_not_browse_from_an_ordinary_row() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;

    // Wide on purpose: the bar is cut from the right, and this assertion is about
    // what the bar says, not about where the fitting loop stops.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "a file is not a directory to go inside: {bar:?}"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(tmp.path()),
        "→ on a file does not browse anywhere"
    );
}

/// What Enter asks of a path nothing has looked at: a worker looks, and its answer is
/// acted on.
fn looked_at_on_a_worker(app: &mut App, rx: &mpsc::Receiver<AppEvent>, follow: Option<AppEvent>) {
    let follow = follow.expect("a look at the path");
    assert!(
        matches!(follow, AppEvent::ClassifyThenOpen { .. }),
        "the key thread reads nothing"
    );
    common::handle_chain(app, follow);
    common::drain_events(app, rx);
}

/// A lake table typed at `~` is not opened as one table either.
///
/// `home_open_selected` learned to go inside one; the path input had no check at all, so
/// `~` and the table's path loaded every Parquet under the root as one table — the whole
/// of the silent wrong answer, reached one keystroke differently.
#[test]
fn test_a_lake_table_typed_as_a_path_is_gone_inside_not_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.path_input_active = true;
    app.home.path_input = table.to_string_lossy().into_owned();

    let follow = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    looked_at_on_a_worker(&mut app, &rx, follow);

    assert_eq!(app.input_mode, InputMode::Home, "nothing was opened");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the path went inside the table"
    );
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A directory of lake tables is not an empty home screen.
///
/// The guidance block is appended under the rows rather than shown instead of them, so a
/// warehouse of fifty `delta` rows printed "No datasets here." underneath them.
#[test]
fn test_a_directory_of_lake_tables_does_not_say_there_is_nothing_here() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for name in ["orders", "customers"] {
        let table = tmp.path().join(name);
        std::fs::create_dir_all(table.join("_delta_log")).unwrap();
        std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
        std::fs::write(table.join("part-0.parquet"), b"x").unwrap();
        std::fs::write(table.join("part-1.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = common::buffer_text(&buf);

    assert!(screen.contains("orders"), "the tables are listed: {screen}");
    assert!(
        !screen.contains("No datasets here."),
        "and the screen does not say there is nothing here: {screen}"
    );
}

/// `→` goes inside a lake table, as it does a hive or multi directory.
///
/// A cloud Delta root used to be labelled `multi`, where `→` descended; recognizing it
/// made `→` fold the section instead. Enter goes inside either way, so nothing was
/// unreachable, but the key that means "look inside this directory" stopped meaning it on
/// the one row where looking inside is all datui can do.
#[test]
fn test_right_goes_inside_a_lake_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Delta)
    );

    // Wide on purpose: this is about what the bar says, not where it is cut.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Inside"),
        "the key is offered here too: {bar:?}"
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "→ went inside the table rather than folding the section"
    );
}

/// An unexamined directory is classified before Enter opens it.
///
/// A cached kind this build will not take leaves the row `Unknown`, whose `is_dataset()`
/// is true — so Enter fell through to opening the path as one dataset. For a lake root
/// that is the whole of #237, restored from a cache written by an older datui.
#[test]
fn test_an_unexamined_lake_root_is_classified_before_it_is_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;

    // As a row restored from a cache this build will not take its kind from.
    for section in app.home.sections_mut().iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "orders") {
            entry.kind = datui::home::discover::EntryKind::Unknown;
        }
    }
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Unknown),
        "the row the cursor is on is the unexamined one"
    );

    let follow = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    looked_at_on_a_worker(&mut app, &rx, follow);

    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "nothing was opened as one table"
    );
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "it was looked at first, found to be a Delta root, and gone inside"
    );
}

/// `→` into a lake table says the same thing `Enter` does.
///
/// The footer advertises `→` on that row, and `home_browse_into` clears the status
/// line — so the door the bar points at was the one that arrived inside with no
/// explanation.
#[test]
fn test_right_into_a_lake_table_says_why() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(app.home.browsing.as_deref(), Some(table.as_path()));
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "→ says why it is showing files rather than a table: {status:?}"
    );
}

/// A Delta root on a mount that may not answer is looked at on a worker, and recognized.
///
/// `EntryKind::Unknown` — the only thing a remote row that has never been probed can be —
/// is offered as openable, so Enter read the whole root as one table. Classifying it
/// where the keys are read is the other half of the trap: `exists`, `is_dir` and a
/// `read_dir` on a hard-mounted share that has gone away is an uninterruptible freeze,
/// with Ctrl+C on the same thread.
#[test]
fn test_an_unexamined_remote_lake_root_is_classified_off_the_event_thread() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    // A share, as the mount table would have it — and the table in Recent, which is how
    // a row on one comes to be listed without anything having looked at it.
    app.home.network_check = |_| true;
    app.home.rebuild(std::slice::from_ref(&table));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Unknown),
        "nothing has looked at it, which is the whole point"
    );

    // The key itself decides nothing: it asks.
    let asked = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(asked, Some(AppEvent::ClassifyThenOpen { .. })),
        "Enter handed the look to a worker rather than doing it here"
    );
    let mut follow = asked;
    while let Some(event) = follow {
        follow = app.event(event);
    }
    assert!(app.is_busy(), "and says so while the worker is out");

    // The worker's answer comes back on the channel.
    let mut opened = false;
    while let Some(event) = next_event(&mut app, &rx) {
        if matches!(event, AppEvent::Open(..)) {
            opened = true;
        }
        let mut follow = app.event(event);
        while let Some(next) = follow {
            if matches!(next, AppEvent::Open(..)) {
                opened = true;
            }
            follow = app.event(next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert!(!opened, "it was never opened as one table");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the worker found a Delta root, and Enter went inside it"
    );
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// Opening a hive directory from the home screen still reads it as one dataset.
///
/// `home_open_path` used to work that out with a `stat`, which on a share that has gone
/// away is the freeze this whole path exists to avoid. It is told now, from the kind the
/// caller already has — so the thing to pin is that the answer did not change.
#[test]
fn test_a_hive_directory_from_home_still_opens_as_one_dataset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let hive = tmp.path().join("sales");
    for part in ["year=2024", "year=2025"] {
        std::fs::create_dir_all(hive.join(part)).unwrap();
        std::fs::write(hive.join(part).join("part-0.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Hive)
    );

    let opened = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    match opened {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![hive.clone()]);
            assert!(options.hive, "read as one partitioned dataset");
        }
        _ => panic!("Enter on a hive directory opens it"),
    }

    // And a single file is not. (The open above left the home screen.)
    app.enter_home();
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;
    match app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(_, options)) => assert!(!options.hive, "a file is not a hive tree"),
        _ => panic!("Enter on a file opens it"),
    }

    // And the case that proves the answer is told rather than stat'ed: a row whose kind
    // says hive but whose path no longer answers, which is how a dropped mount presents
    // itself. `is_dir()` is false there, so a stat would call it a single file.
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    let gone = PathBuf::from("/mnt/gone/sales");
    for section in app.home.sections_mut().iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "sales") {
            entry.path = gone.clone();
        }
    }
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == gone))
        .expect("the row is listed");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Hive),
        "the row still says hive"
    );
    match app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![gone]);
            assert!(
                options.hive,
                "told from the kind, not worked out with a stat that cannot reach it"
            );
        }
        other => panic!("Enter opens it: {}", other.is_some()),
    }
}

/// Two doors, on a directory datui does not recognize as anything.
///
/// A directory holding a CSV and a JSON is `mixed`: no label datui has says it is one
/// table, and before this the row could only be folded — `→` did nothing and `Enter`
/// tried to open it as a dataset and said it could not. A directory whose storage
/// convention datui does not know is exactly the directory a user most needs to get into,
/// so both doors are open on it now: `→` steps inside, and the first row in there reads
/// the whole of it.
#[test]
fn test_both_doors_are_open_on_a_directory_datui_cannot_name() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("exports");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(directory.join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "exports"))
        .expect("the directory is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.label().to_string()),
        Some("mixed".to_string()),
        "nothing datui knows calls this a dataset"
    );

    // The bar says the door is there, on a row no label offers as a dataset.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the key: {bar:?}");

    // One key in.
    app.event(key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ went inside a directory datui has no name for"
    );

    // And the first row in there is the other door. (The app rebuilds the listing on
    // the event this returns; here the same call does it on the spot.)
    app.home.rebuild(&[]);
    let names: Vec<String> = app
        .home
        .visible()
        .iter()
        .filter_map(|r| match r {
            datui::home::Row::Entry { entry, .. } | datui::home::Row::Door { entry, .. } => {
                Some(entry.name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        names.first().map(String::as_str),
        Some("exports (all files, mixed)"),
        "got {names:?}"
    );
}

/// The door into a lake table reads its files, and says they are not the table.
///
/// A lake table is not a directory of Parquet files however much it looks like one:
/// reading one as a union counts tombstoned rows, every rewritten version and both
/// sides of a compaction. Refusing it, though, left a directory the user could see and
/// could not read at all — and this row is the promise that no label locks you out.
/// So the read is labelled instead of refused: a note in the panel, a chip beside the
/// row count, and the row one level up still goes inside and says datui does not read
/// the table itself yet. All three, because each on its own is missable.
#[test]
fn test_the_door_into_a_lake_table_says_its_files_are_not_the_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let events = tmp.path().join("events");
    std::fs::create_dir_all(events.join("_delta_log")).unwrap();
    std::fs::write(events.join("_delta_log").join("00000000.json"), b"{}").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(events.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(events.clone());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Delta),
        "the listing under it is a Delta table"
    );

    // It opens, and the open carries what it is.
    let options = match app.event(key(KeyCode::Enter)) {
        Some(AppEvent::Open(_, options)) => options,
        _ => panic!("the door opens the directory it names, whatever the label says"),
    };
    assert_eq!(
        options.read_as_plain_files_of,
        Some("Delta"),
        "and the open says these are a Delta table's files, not the table"
    );

    // The note and the chip, from that one field. Both, because the note is a tab away
    // and the chip is in the corner: each on its own is missable.
    let notes = datui::notes::from_the_open(
        &[],
        options.read_as_plain_files_of,
        Default::default(),
        false,
    );
    assert_eq!(notes.len(), 1, "one note, about the read");
    assert!(
        notes[0].summary.contains("Delta") && notes[0].summary.contains("deleted rows"),
        "it names the format and what the count includes: {:?}",
        notes[0].summary
    );

    // And the row one level up still goes inside rather than reading it.
    let (tx, up_rx) = mpsc::channel();
    let mut up = App::new(tx, common::test_runtime());
    up.enter_home();
    up.home.browsing = Some(tmp.path().to_path_buf());
    up.home.rebuild(&[]);
    let row = up
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the directory is listed");
    up.home.selected = row;
    let follow = up.event(key(KeyCode::Enter));
    looked_at_on_a_worker(&mut up, &up_rx, follow);
    assert_eq!(up.home.browsing.as_deref(), Some(events.as_path()));
    let said = lake_heading(&mut up);
    assert!(
        said.contains("delta") && said.contains("not read"),
        "the row above is where datui says it does not read the table: {said:?}"
    );
}

/// `hive: true` is what puts the open on the local directory route at all: without it a
/// directory is `Unsupported file type`, and the whole of `directory_format`'s dispatch
/// is behind it. The door row builds its own open, so nothing else pins the flag.
#[test]
fn test_the_door_opens_a_directory_by_the_directory_route() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("a.csv"), b"x,y\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), b"x,y\n3,4\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;

    match app.event(key(KeyCode::Enter)) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![tmp.path().to_path_buf()]);
            assert!(
                options.hive,
                "without this the open is `Unsupported file type`"
            );
        }
        _ => panic!("Enter on the door should open the directory"),
    }
}

/// → goes inside a row nothing has looked into yet.
///
/// On a share that is most rows: `entry_for_path` calls a remote path with no data
/// extension `Unknown`, and a listing looks into nothing. Excluding `Unknown` from the
/// door would put the directories that cost most to reach back behind a classification —
/// the label deciding access again, one indirection along. A remote file with an odd
/// extension is browsed into and shows an empty listing, which `Esc` backs out of.
#[test]
fn test_right_goes_inside_a_row_nothing_has_looked_into() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("archive");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "archive"))
        .expect("the directory is listed");
    app.home.selected = row;
    // Deliberately not classified: this is what a listing hands over before anything
    // has looked into it, and what every row on a share looks like.
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::home::discover::EntryKind::Unknown),
    );

    app.event(key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ went inside without needing to know what it is first"
    );
}

/// Each route's directories are classified in that route's vocabulary.
///
/// The door used to ask the cloud classifier about a local directory, which got two
/// answers wrong in opposite directions. the listing drops dotted names, so `.hoodie`
/// never reached it and a local Hudi table came back `MultiFile` — the door then read
/// its tombstones, two keystrokes after the row above said datui does not read Hudi
/// tables yet. And the cloud Iceberg rule is the looser of the two on purpose, names
/// only, so a plain directory holding `data/` beside `metadata/` was refused as a lake
/// table it is not: the second door closing on a false verdict, which is the whole
/// thing phase 3 exists to stop.
#[test]
fn test_the_door_reads_a_local_directory_with_the_local_rules() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // A Hudi table: the marker is a dotted name.
    let trips = tmp.path().join("trips");
    std::fs::create_dir_all(trips.join(".hoodie")).unwrap();
    std::fs::write(trips.join(".hoodie").join("hoodie.properties"), b"x").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(trips.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    // And a plain directory that merely looks like an Iceberg table by its names.
    let project = tmp.path().join("project");
    std::fs::create_dir_all(project.join("data")).unwrap();
    std::fs::create_dir_all(project.join("metadata")).unwrap();
    std::fs::write(project.join("data").join("a.parquet"), b"x").unwrap();
    std::fs::write(project.join("metadata").join("notes.md"), b"x").unwrap();

    let door_kind = |dir: &std::path::Path| {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(dir.to_path_buf());
        app.home.rebuild(&[]);
        app.home
            .visible()
            .iter()
            .find_map(|r| match r {
                datui::home::Row::Door { entry, .. } => Some(entry.kind),
                _ => None,
            })
            .expect("the directory carries the row")
    };

    assert_eq!(
        door_kind(&trips),
        datui::home::discover::EntryKind::Hudi,
        "a dotted marker is not in the listing, so only the local rule can see it"
    );
    assert_eq!(
        door_kind(&project),
        datui::home::discover::EntryKind::Directory,
        "two directory names are not an Iceberg table: the local rule wants a \
         .metadata.json in one of them"
    );
}

/// A prefix in an object store is read with the reader its own listing calls for.
///
/// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of CSV
/// answered "Could not read from S3. Check credentials and URL" — a false statement
/// about a login that is fine. The listing has already counted what is there and it is
/// on screen, so picking the reader from it costs no request. What is left refused is
/// a prefix holding nothing datui has a multi-file reader for, and that refusal names
/// what is there rather than blaming the connection.
///
/// A fresh app per shape, because opening sets `busy` and the next key would be read
/// against a screen that is no longer the home screen.
#[cfg(feature = "cloud")]
#[test]
fn test_the_cloud_door_reads_a_prefix_with_the_reader_its_listing_calls_for() {
    use datui::home::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    // Press Enter on the door of a prefix holding these names, and say what happened.
    // A name with a dot in it stands for an object, the rest for sub-prefixes; a name
    // with an `=` in it is a partition, the way a listing hands one over.
    fn door(prefix: &str, names: &[&str]) -> (Option<OpenOptions>, String) {
        let place = PathBuf::from(prefix);
        let rows: Vec<Entry> = names
            .iter()
            .map(|name| {
                let mut entry = Entry::directory(&place.join(name));
                entry.name = (*name).to_string();
                if name.contains('.') {
                    entry.kind = EntryKind::File;
                    entry.size = Some(1_000);
                }
                entry
            })
            .collect();

        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.network_check = |_| true;
        app.home.probe_ready(place.clone(), rows, false);
        app.home.browsing = Some(place);
        app.home.rebuild(&[]);
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Door { .. }))
            .expect("the prefix carries the row");
        app.home.selected = row;
        let event = app.event(key(KeyCode::Enter));
        let options = match event {
            Some(AppEvent::Open(_, options)) => Some(options),
            _ => None,
        };
        (options, app.home.status.clone().unwrap_or_default())
    }

    // Data files, none of them Parquet: read as what they are.
    let (options, said) = door("s3://bucket/exports", &["a.csv", "b.csv", "c.csv"]);
    let options = options.expect("a prefix of CSV is a prefix datui can read");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert!(
        !said.to_lowercase().contains("credential"),
        "and nothing blames a login that is fine: {said:?}"
    );

    // Two formats, neither Parquet: the commonest is the reader, and the rest is said.
    // `label()` would call this `mixed`, a word rather than a count.
    let (options, _) = door("s3://bucket/pair", &["a.csv", "a2.csv", "b.json"]);
    let options = options.expect("a prefix of mostly CSV reads as CSV");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert_eq!(
        options.left_out,
        vec![(datui::FileFormat::Json, 1)],
        "and the dataset can say what it passed over"
    );

    // Nothing datui has a reader for. `holds.formats` is empty here, so a test written
    // over the formats alone let it through and the scan came back blaming the login.
    let (options, said) = door("s3://bucket/docs", &["README.md", "notes.pdf"]);
    assert!(options.is_none());
    assert!(said.contains("nothing datui can read"), "{said:?}");
    assert!(!said.to_lowercase().contains("credential"), "{said:?}");

    // Parquet opens by its own route, which is the only one with hive partitioning
    // behind it and the one every cloud dataset took before any of this. The listing
    // already calls this prefix a dataset, so no reader is named and the scan makes the
    // Parquet call it always made. Without this case, a change that named a reader for
    // everything would pass every other assertion here.
    let (options, _) = door("s3://bucket/parts", &["part-0.parquet", "part-1.parquet"]);
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a prefix of Parquet is what a cloud directory reads as"
    );

    // No data files at all: tried, because the files below may be Parquet and nothing
    // here has looked.
    let (options, _) = door("s3://bucket/warehouse", &["by_year", "by_station"]);
    assert!(
        options.is_some(),
        "nothing counted directly inside is not a reason to refuse"
    );

    // Including with unreadable files beside the sub-prefixes: a README at the top says
    // nothing about what is under `by_year/`.
    let (options, _) = door("s3://bucket/warehouse2", &["README.md", "by_year"]);
    assert!(
        options.is_some(),
        "a sub-prefix may hold Parquet, and nothing here has looked"
    );

    // And a hive root with one stray data file beside its partitions. `formats` holds
    // only the stray, so picking the reader from it would read the whole root as CSV —
    // a prefix the listing already calls a dataset keeps the route its label named.
    let (options, said) = door(
        "s3://bucket/events",
        &["date=2024-01-01", "date=2024-01-02", "manifest.csv"],
    );
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a hive root is read through its partitions, not as the stray beside them: {said:?}"
    );
}

/// The section's count is of what is listed, not the way out of the directory.
///
/// The door's kind is the directory's, so it counts as a dataset — and it is the same
/// dataset as the directory it opens, counted a second time.
#[test]
fn test_the_count_does_not_include_the_door() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for name in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        let file = std::fs::File::create(parts.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(parts);
    app.home.rebuild(&[]);
    app.home.classify_now(16);

    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = common::buffer_text(&buf);
    assert!(
        screen.contains("parts  3 "),
        "three files, and the door is not a fourth: {screen:?}"
    );
}

/// The footer says what Enter will really do, on a row of every shape.
///
/// `WhatEnter` is a prediction the renderer reads and `home_open_selected` is the thing
/// that decides, so the two can drift. This is what stops them: one row of each shape,
/// Enter pressed on it, and the prediction checked against what actually happened.
#[test]
fn test_the_bar_says_what_enter_will_really_do() {
    let tmp = tempfile::TempDir::new().unwrap();
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };

    // One table across two files; separate tables; a lake root; and a plain file.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let apart = tmp.path().join("apart");
    parquet(&apart, "by_block.parquet", table(&["block", "fee"]));
    parquet(&apart, "daily.parquet", table(&["day", "price"]));
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(delta.join("_delta_log").join("0.json"), "{}").unwrap();
    parquet(&delta, "part-0.parquet", table(&["id"]));
    parquet(tmp.path(), "loose.parquet", table(&["id"]));

    // Each row, classified the way the background pass would, then Enter pressed on it.
    for (name, expected) in [
        ("one", datui::WhatEnter::OpensDirectory),
        ("apart", datui::WhatEnter::GoesInside),
        ("delta", datui::WhatEnter::GoesInside),
        ("loose.parquet", datui::WhatEnter::OpensFile),
    ] {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(tmp.path().to_path_buf());
        app.home.rebuild(&[]);
        app.home.measure_now(16);
        app.home.classify_now(16);
        app.home.rebuild(&[]);

        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;

        let predicted = app.what_enter_does();
        assert_eq!(predicted, expected, "prediction for {name}");

        let was = app.home.browsing.clone();
        let opened = matches!(app.event(key(KeyCode::Enter)), Some(AppEvent::Open(..)));
        let went_inside = app.home.browsing != was;
        match expected {
            datui::WhatEnter::OpensDirectory | datui::WhatEnter::OpensFile => assert!(
                opened && !went_inside,
                "{name}: the bar promised an open and Enter did {opened}/{went_inside}"
            ),
            datui::WhatEnter::GoesInside => assert!(
                went_inside && !opened,
                "{name}: the bar promised to go inside and Enter did {opened}/{went_inside}"
            ),
            _ => unreachable!("no other shape is asserted here"),
        }
    }

    // The shapes that are not entries at all. Each does something different and each
    // said "Open" before, which is the wrong first impression on three more rows.
    let (tx, _rx) = mpsc::channel();
    let mut other = App::new(tx, common::test_runtime());
    other.enter_home();
    other.home.browsing = Some(tmp.path().to_path_buf());
    other.home.rebuild(&[]);
    let at = |app: &mut App, want: fn(&datui::home::Row) -> bool| {
        app.home.visible().iter().position(want)
    };
    if let Some(i) = at(&mut other, |r| matches!(r, datui::home::Row::Header { .. })) {
        other.home.selected = i;
        assert_eq!(
            other.what_enter_does(),
            datui::WhatEnter::FoldsSection,
            "Enter folds a section header; it does not open anything"
        );
    }

    // And the door, which reads whatever it is standing in.
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(apart.clone());
    app.home.rebuild(&[]);
    let door = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the door");
    app.home.selected = door;
    assert_eq!(app.what_enter_does(), datui::WhatEnter::OpensDirectory);
    assert!(matches!(
        app.event(key(KeyCode::Enter)),
        Some(AppEvent::Open(..))
    ));
}

/// A place row under `RECENT` gets the verb its key actually has.
///
/// Enter browses into the place, which is what → does on it too. The bar read
/// `Enter Open … → Inside`: the wrong verb, plus the two-chips-for-one-outcome the
/// labelling exists to remove. Neither the key-pumping test nor the bar's own tests
/// covered a place row, because both were written over entries.
#[test]
fn test_a_place_row_says_inside_and_says_it_once() {
    let tmp = tempfile::TempDir::new().unwrap();
    let held = tmp.path().join("exports");
    std::fs::create_dir_all(&held).unwrap();
    let file = held.join("sales.csv");
    std::fs::write(&file, "a,b\n1,2\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(std::slice::from_ref(&file));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { .. }))
        .expect("a recent under a place row");
    app.home.selected = row;

    assert_eq!(
        app.what_enter_does(),
        datui::WhatEnter::GoesInside,
        "Enter browses the place, which is what → does"
    );

    // And Enter really does browse, so the label is not a guess.
    app.event(key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(held.as_path()));
}

/// The pane does not point at a door that will not be there.
///
/// `whole_directory_row` gives no `(all files)` row to a directory with nothing in it,
/// nor to any directory while a filter is typed — and the pane said "the first row in
/// there reads the whole directory as one table" for every plain directory regardless.
#[test]
fn test_the_pane_only_promises_a_door_that_exists() {
    let tmp = tempfile::TempDir::new().unwrap();
    let empty = tmp.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let full = tmp.path().join("full");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("a.csv"), "a,b\n1,2\n").unwrap();
    std::fs::write(full.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let pane = |app: &mut App, name: &str| {
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;
        let area = ratatui::layout::Rect::new(0, 0, 120, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
        // The pane wraps and pads, so a sentence spans rows with a border and a run of
        // spaces in the middle. Flattened to single spaces so the text can be looked
        // for as it reads.
        let raw = common::buffer_lines(&buf).join(" ");
        // `|` is the border in the ASCII glyph set.
        raw.replace(['│', '|'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[]);
    app.home.measure_now(16);
    app.home.classify_now(16);
    app.home.rebuild(&[]);

    let shown = pane(&mut app, "full");
    assert!(
        shown.contains("first row opens all"),
        "a directory with something in it has the door to point at: {shown}"
    );
    assert!(
        !pane(&mut app, "empty").contains("first row opens all"),
        "an empty directory has none, so nothing points at one"
    );
}

/// A path named at startup that is not there ends the session, as it always has; the
/// check is a worker's, behind the first frame.
#[test]
fn test_a_missing_named_path_is_found_on_a_worker() {
    let tmp = tempfile::TempDir::new().unwrap();
    let missing = tmp.path().join("nope.csv");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    assert!(
        app.event(AppEvent::OpenNamed(
            vec![missing.clone()],
            OpenOptions::default()
        ))
        .is_none()
    );
    let mut found = None;
    while found.is_none() {
        let mut next = next_event(&mut app, &rx);
        while let Some(event) = next.take() {
            match event {
                AppEvent::NamedPathMissing(path) => found = Some(path),
                other => next = app.event(other),
            }
        }
    }
    assert_eq!(found, Some(missing));
    // A URL or a glob is the open's to judge.
    assert_eq!(
        App::missing_named_path(
            &[PathBuf::from("https://example.com/x.csv")],
            &Default::default()
        ),
        None
    );
    assert_eq!(
        App::missing_named_path(&[tmp.path().join("*.csv")], &Default::default()),
        None
    );
}

/// Going home while a directory is being looked at is not undone when the look lands.
///
/// The look can take seconds, and Ctrl+O works throughout — which is the point of
/// moving it off the startup thread. So the user can be somewhere else by the time it
/// answers, and the answer must not take them back.
#[test]
fn test_a_look_that_lands_after_the_user_left_is_dropped() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = tmp.path().join("one");
    std::fs::create_dir_all(&directory).unwrap();
    for name in ["a.csv", "b.csv"] {
        std::fs::write(directory.join(name), "id,ts\n1,2\n").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let event = App::route_named_paths(vec![directory.clone()], OpenOptions::default());
    app.event(event);

    // Ctrl+O while the look is out: the user is at the home screen now.
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    // The look lands. It must find nothing waiting for it.
    let mut landed = None;
    for _ in ticks() {
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50))
            && matches!(ev, AppEvent::JobEnded(t) if t.kind() == JobKind::LookAtDirectory)
        {
            landed = Some(app.event(ev));
            break;
        }
    }
    let landed = landed.expect("the look reports back");
    assert!(
        landed.is_none(),
        "the answer to a question the user walked away from does not open anything"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "and does not take them off the screen they chose"
    );
    assert!(
        app.home.browsing.is_none(),
        "nor browse them into the directory they left: {:?}",
        app.home.browsing
    );
}

/// Going home while the paths named at startup are looked at puts the open down: the
/// look's answer, landing after, opens nothing and does not take the user off the home
/// screen.
///
/// The look is the open's first phase. It used to be quieted rather than put down, so
/// its answer still carried the open on and pulled the user back from home to the file.
#[test]
fn test_going_home_during_the_look_at_named_paths_opens_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let file = tmp.path().join("named.csv");
    std::fs::write(&file, "a,b\n1,2\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Scanning input", 10);
    assert!(
        app.event(AppEvent::OpenNamed(vec![file], OpenOptions::default()))
            .is_none(),
        "the look goes to a worker"
    );
    app.event(ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);
    assert!(!app.is_busy(), "going home is immediate");

    let mut landed = false;
    for _ in ticks() {
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            landed |= matches!(ev, AppEvent::JobEnded(t) if t.kind() == JobKind::OpenNamed);
            let mut next = app.event(ev);
            while let Some(ev) = next {
                next = app.event(ev);
            }
            if landed {
                break;
            }
        }
    }
    assert!(landed, "the look reports back");
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Home, "the user stays home");
    assert!(app.data_table_state.is_none(), "and nothing was opened");
    assert!(app.error_message().is_none());
}

/// A dataset that is up owns the footer pass behind it: going home from it leaves the
/// pass running, and an open replacing it is what stops it.
///
/// The counter used to be the open's, cancelled whenever the user went home while that
/// open was still marked active — which it stayed after installing, so the first trip
/// home stopped the pass of the dataset left on screen.
#[test]
fn test_going_home_leaves_the_dataset_s_footer_pass_alone() {
    let tmp = tempfile::TempDir::new().unwrap();
    let file = tmp.path().join("people.csv");
    std::fs::write(&file, "name,age\nada,36\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![file.clone()], OpenOptions::default());
    assert!(app.data_table_state.is_some());
    let counter = app.footer_progress().clone();

    app.event(ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);
    assert!(
        !counter.is_cancelled(),
        "the dataset on screen keeps reading what it still has to"
    );

    pump_open_until_loaded(&mut app, &rx, vec![file], OpenOptions::default());
    assert!(
        counter.is_cancelled(),
        "the dataset that replaced it stopped it"
    );
    assert!(
        !std::sync::Arc::ptr_eq(&counter, app.footer_progress()),
        "and counts on a counter of its own"
    );
}

/// A setting that agrees with what the rule assumed is not a reason to stop asking it.
///
/// `has_header` and the skips reach `OpenOptions` from the config file as well as the
/// command line, so a guard over "did anyone set this" is true on every run for anyone
/// with `has_header = true` in `~/.config/datui/config.toml` — and every directory they
/// name is then forced down the one-table route, `datui .` included. The question is
/// whether the header is somewhere other than where the rule looked.
#[test]
fn test_a_setting_that_agrees_with_the_rule_changes_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    std::fs::write(apart.join("a.csv"), "id,ts\n1,2\n").unwrap();
    std::fs::write(apart.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let settle = |options: OpenOptions| {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = Some(AppEvent::OpenNamed(vec![apart.clone()], options));
        loop {
            if app.home.browsing.is_some() {
                return None;
            }
            match next.take() {
                Some(AppEvent::Open(paths, options)) => return Some((paths, options)),
                Some(ev) => next = app.event(ev),
                None => match next_event(&mut app, &rx) {
                    Some(ev) => next = Some(ev),
                    None => panic!("the chain stopped without settling"),
                },
            }
        }
    };

    // A header where one is expected, and a skip of nothing: the same thing the rule
    // assumed, so the directory of separate tables is still somewhere to look inside.
    for agrees in [
        OpenOptions {
            has_header: Some(true),
            ..OpenOptions::default()
        },
        OpenOptions {
            skip_rows: Some(0),
            skip_lines: Some(0),
            ..OpenOptions::default()
        },
    ] {
        assert!(
            settle(agrees).is_none(),
            "a setting the rule already assumed does not force the one-table route"
        );
    }

    // Moving the header does change it — not by overriding the rule, but because the
    // rule now reads the files the way the open will: headerless, every file's columns
    // are `column_1..N` and the narrower nests inside the wider.
    assert!(
        settle(OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        })
        .is_some(),
        "read as headerless, these files are one table"
    );
}

/// `?` before typing opens help, and the overlay owns the keys while it is up.
/// It used to be unreachable there (`?` typed into the filter) and, opened with
/// F1, unclosable: Esc went to the home screen underneath and backed out of
/// directories behind the overlay.
#[test]
fn home_help_opens_with_question_mark_and_esc_closes_it() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible(), "? on an empty filter opens help");
    assert!(app.home.filter.is_empty(), "? must not land in the filter");

    // Keys reach the overlay, not the list underneath.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible());

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible(), "Esc closes the overlay");
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "home is still up behind it"
    );
}

/// The wordmark yields on small terminals — short ones (it costs two dataset
/// rows) and narrow ones (its fourteen columns leave the path beside it all
/// ellipsis) — and the one-line title bar comes back.
#[test]
fn the_wordmark_yields_to_small_terminals() {
    let Some(wordmark) = datui::glyphs::get().wordmark else {
        return; // ASCII locale: there is no wordmark to yield.
    };
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();

    let drawn = |w: u16, h: u16, app: &mut App| -> String {
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };

    assert!(
        drawn(100, 50, &mut app).contains(wordmark[0]),
        "a big terminal gets the wordmark"
    );
    assert!(
        !drawn(100, 20, &mut app).contains(wordmark[0]),
        "a short terminal gets the rows back"
    );
    assert!(
        !drawn(36, 50, &mut app).contains(wordmark[0]),
        "a narrow terminal gives the path the columns"
    );
    assert!(
        drawn(36, 50, &mut app).contains("datui"),
        "the one-line title stands in"
    );
}

/// `-c home.wordmark=false` puts the one-line title bar where the wordmark would
/// fit; the default keeps the wordmark.
#[test]
fn home_wordmark_off_draws_the_title_bar() {
    use clap::Parser;
    use datui::config::{AppConfig, ConfigLayer};
    let Some(wordmark) = datui::glyphs::get().wordmark else {
        return; // ASCII locale: the title bar is all there is.
    };
    let drawn = |argv: &[&str]| -> String {
        let args = datui_cli::Args::try_parse_from(argv).expect("parses");
        let layer = ConfigLayer::from_overrides(&args.config).expect("-c parses");
        let config = AppConfig::from_layers([layer]).expect("config reads");
        let theme = datui::Theme::from_config(&config.theme).unwrap();
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new_with_config(tx, common::test_runtime(), theme, config);
        app.enter_home();
        let area = Rect::new(0, 0, 100, 50);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };

    // The title bar is the top row, padded by one column: ` datui ` then the path.
    let title_bar = |screen: &str| screen.chars().take(100).collect::<String>();
    let on = drawn(&["datui"]);
    assert!(on.contains(wordmark[0]), "the default draws the wordmark");
    assert!(!title_bar(&on).starts_with("  datui "), "and no title bar");

    let off = drawn(&["datui", "-c", "home.wordmark=false"]);
    assert!(!off.contains(wordmark[0]), "off, no wordmark");
    assert!(
        title_bar(&off).starts_with("  datui "),
        "the title bar stands in"
    );
}

/// The home screen lists a Hugging Face cache's splits inside it, above its files, and
/// a split's place (`hf_cache/test`) opens that split as `--table` would.
#[test]
fn a_hugging_face_cache_lists_its_splits_on_home() {
    common::ensure_sample_data();
    let cache = PathBuf::from("tests/sample-data/hf_cache");
    let mut home = datui::home::HomeState {
        browsing: Some(cache.clone()),
        ..datui::home::HomeState::default()
    };
    home.rebuild(&[]);
    let names: Vec<String> = home
        .visible()
        .iter()
        .filter_map(|r| match r {
            datui::home::Row::Entry { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(names[..3], ["train", "validation", "test"], "{names:?}");
    assert!(
        names.contains(&"people-test.arrow".to_string()),
        "{names:?}"
    );

    let scratch = tempfile::tempdir().unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![cache.join("test")], options),
    );
    assert_eq!(app.error_message(), None);
    let state = app.data_table_state.as_ref().expect("the test split opens");
    assert_eq!(state.other_tables(), ["train", "validation"]);
    // A directory has no footer of its own: no Arrow tab, once its facts are read.
    if let Some(next) = app.event(key(KeyCode::Char('i'))) {
        let _ = tx.send(next);
    }
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(
            app.file_facts(),
            Some(datui::widgets::info::FileFacts::Reading)
        )
    });
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(
        text.contains("Resources") && !text.contains("Arrow"),
        "{text}"
    );
    assert!(datui::home::discover::split_row(&cache.join("test")).is_some());
    assert!(datui::home::discover::split_row(&cache.join("dev")).is_none());
}

use crate::home_app::CLASSIFY_BATCH;
use crate::*;
use std::sync::mpsc;

/// A listing of `n` subdirectories nothing has looked into, under a directory that
/// does not exist — so a pass over them settles nothing and blocks on nothing.
fn unlooked_at(n: usize) -> home::Listing {
    let rows = (0..n)
        .map(|i| {
            let mut entry =
                discover::Entry::directory(&PathBuf::from(format!("/pretend/share/d{i:04}")));
            entry.kind = discover::EntryKind::Unknown;
            entry
        })
        .collect();
    home::Listing {
        sections: vec![home::Section::titled("SHARE", rows)],
        ..Default::default()
    }
}

/// Put the viewport where a frame of twenty rows would put it after moving down
/// to `selected` from the top, exactly as `render_list` does. The two move
/// together, so a test that set one and not the other would describe a screen
/// that cannot exist.
fn looking_at(app: &mut App, selected: usize) {
    app.home.selected = selected;
    app.home.view_height = 20;
    app.home.scroll = selected.saturating_sub(17);
}

/// Paging quickly must not leave a classification queued for every row it went
/// past. Only one pass is ever out, and the next one is chosen from the viewport
/// as it is when that one lands — so a page that crossed four hundred rows asks
/// about the forty it stopped on.
#[test]
fn only_one_classification_pass_is_out_at_a_time() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.apply_listing(unlooked_at(500));
    looking_at(&mut app, 18);

    app.request_home_classifications();
    assert!(
        app.home.classify_in_flight,
        "the first pass should have gone out"
    );

    // Paging while it is out. Nothing more is asked for meanwhile.
    for row in [117, 217, 317, 417] {
        looking_at(&mut app, row);
        app.request_home_classifications();
    }
    assert!(app.home.classify_in_flight, "and still only the one");

    // It lands, and what follows it is about where the viewport is now.
    app.event(&AppEvent::HomeClassified {
        measured: Vec::new(),
        done: true,
    });
    let next = app.home.unclassified_visible(CLASSIFY_BATCH);
    assert!(
        next.iter()
            .all(|e| e.name.trim_start_matches('d').parse::<usize>().unwrap() >= 300),
        "the next pass follows the viewport, not the rows paged over: {:?}",
        next.iter().map(|e| &e.name).collect::<Vec<_>>()
    );
}

/// A pass that answers must give the slot back, or the home screen stops
/// classifying anything for the rest of the session.
#[test]
fn an_answered_pass_frees_the_slot() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.classify_in_flight = true;

    app.event(&AppEvent::HomeClassified {
        measured: Vec::new(),
        done: true,
    });

    assert!(!app.home.classify_in_flight);
}

/// A pass that lands after the listing was rebuilt still labels its row. A probe
/// or a cloud peek landing rebuilds the listing, and a Recent section with a few
/// buckets in it lands several in a row: dropping the answer each time left a share
/// dataset unlabeled for half a minute.
#[test]
fn a_pass_that_outlives_its_listing_still_counts() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.apply_listing(unlooked_at(4));
    let path = PathBuf::from("/pretend/share/d0000");
    app.home.classify_in_flight = true;

    // What a rebuild does to the generation while the pass is out.
    app.home_app.generation = app.home_app.generation.wrapping_add(1);
    app.event(&AppEvent::HomeClassified {
        measured: vec![(
            path.clone(),
            home::Measured {
                kind: Some(discover::EntryKind::Hive),
                ..Default::default()
            },
        )],
        done: true,
    });

    let kind = app.home.visible().iter().find_map(|row| match row {
        home::Row::Entry { entry, .. } if entry.path == path => Some(entry.kind),
        _ => None,
    });
    assert_eq!(kind, Some(discover::EntryKind::Hive));
}

/// Space folds the header under the cursor, and never starts a filter: a filter of
/// one space is invisible at the prompt and searched below the working directory.
/// Once typing has started it types.
#[test]
fn space_folds_a_header_and_types_only_mid_filter() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.input_mode = InputMode::Home;
    app.home.apply_listing(unlooked_at(3));
    let space = || AppEvent::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    app.home.selected = 0;
    assert!(app.home.selection_is_header());

    app.event(&space());
    assert!(app.home.is_collapsed(0));
    app.event(&space());
    assert!(!app.home.is_collapsed(0));

    app.home.selected = 1;
    app.event(&space());
    assert_eq!(app.home.filter, "", "a space on a row is nothing");
    assert!(!app.home.is_collapsed(0));

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('d'),
        KeyModifiers::NONE,
    )));
    app.event(&space());
    assert_eq!(app.home.filter, "d ");
}

/// A row's label shows as soon as its answer lands, not when the batch it was in
/// finishes; the slot stays taken until then, so a second batch never overlaps it.
#[test]
fn a_label_lands_before_its_batch_is_done() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    app.home.apply_listing(unlooked_at(4));
    let path = PathBuf::from("/pretend/share/d0000");
    app.home.classify_in_flight = true;

    app.event(&AppEvent::HomeClassified {
        measured: vec![(
            path.clone(),
            home::Measured {
                kind: Some(discover::EntryKind::Hive),
                ..Default::default()
            },
        )],
        done: false,
    });

    let kind = app.home.visible().iter().find_map(|row| match row {
        home::Row::Entry { entry, .. } if entry.path == path => Some(entry.kind),
        _ => None,
    });
    assert_eq!(kind, Some(discover::EntryKind::Hive));
    assert!(app.home.classify_in_flight, "the batch is still out");
}

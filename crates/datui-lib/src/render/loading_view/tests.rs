use super::*;
use crate::tests::test_runtime;

#[test]
fn a_load_with_no_room_to_draw_is_skipped_rather_than_panicking() {
    // Terminals get resized to absurd sizes mid-load, and a load is exactly when
    // the user cannot press anything to recover.
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = crate::App::new(tx, test_runtime());
    app.set_loading_phase("Scanning input", 10);
    for (w, h) in [(1, 1), (4, 2), (11, 40), (200, 1)] {
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, &app, &RenderContext::for_test());
    }
}

/// While the footers are being read the screen counts them, and stops when they
/// land.
///
/// "Reading schema" is true of that wait but says nothing about its length; a
/// directory of thousands of files spends seconds there. A number that climbs is a
/// wait, and a number that stops is a problem — neither is legible without it.
#[test]
fn the_footer_count_replaces_the_phase_while_it_is_running() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = crate::App::new(tx, test_runtime());
    app.loading_for_tests(
        Some(std::path::PathBuf::from("/tmp/blocks")),
        2048,
        "Reading schema",
        40,
    );
    let area = Rect::new(0, 0, 60, 20);
    let painted = |app: &crate::App| {
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, app, &RenderContext::for_test());
        crate::tests::buffer_text(&buf)
    };

    assert!(
        painted(&app).contains("Reading schema"),
        "the phase, while nothing is being counted"
    );

    app.footer_progress().begin(6541);
    for _ in 0..1203 {
        app.footer_progress().advance();
    }
    // The count is taken once a frame rather than where it is shown; these tests
    // paint the body alone, so they do for themselves what a whole frame does
    // first. That the app does it is `test_the_footer_counts_the_footers_the
    // _loading_screen_does`, which renders the App and not this function.
    app.begin_frame();
    let text = painted(&app);
    assert!(
        text.contains("Reading footers: 1,203 of 6,541"),
        "the count, grouped so six thousand does not read as sixty: {text}"
    );
    assert!(
        !text.contains("Reading schema"),
        "and it replaces the phase rather than crowding in beside it: {text}"
    );

    // A new frame, because the count a frame shows is the one it started with: a
    // pass that lands halfway down the screen does not change what the bottom of
    // it says.
    app.footer_progress().done();
    app.begin_frame();
    assert!(
        painted(&app).contains("Reading schema"),
        "once they have landed there is no wait left to count"
    );
}

/// A listing counts the objects it has found, and gives way to the footers.
///
/// Listing a prefix of hundreds of thousands of objects is the longest wait of the
/// open, and it has no total, so the count is what says it is moving.
#[test]
fn the_listing_count_replaces_the_phase_while_it_is_running() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = crate::App::new(tx, test_runtime());
    app.loading_for_tests(
        Some(std::path::PathBuf::from("/tmp/by_station")),
        0,
        "Reading schema",
        40,
    );
    let area = Rect::new(0, 0, 60, 20);
    let painted = |app: &crate::App| {
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, app, &RenderContext::for_test());
        crate::tests::buffer_text(&buf)
    };

    let progress = app.footer_progress().clone();
    let listing = progress.listing();
    for _ in 0..412_000 {
        listing.advance();
    }
    app.begin_frame();
    let text = painted(&app);
    assert!(text.contains("Listing files: 412,000"), "{text}");
    assert!(!text.contains("Reading schema"), "{text}");

    // The two footers that follow the listing are counted as footers.
    drop(listing);
    app.footer_progress().begin(2);
    app.begin_frame();
    let text = painted(&app);
    assert!(text.contains("Reading footers: 0 of 2"), "{text}");
    assert!(!text.contains("Listing files"), "{text}");

    app.footer_progress().done();
    app.begin_frame();
    assert!(painted(&app).contains("Reading schema"));
}

/// The count is cut with a mark at a width it does not fit, not by the terminal.
///
/// "Reading schema" is fourteen characters and always fitted; "Reading footers:
/// 1,203 of 6,541" needs thirty-five. Cut by the terminal instead of by the panel it
/// ends mid-number — "of 6" where it means "of 6,541", a smaller figure than the one
/// it is counting towards, which is the one way this line could actively mislead.
#[test]
fn the_footer_count_is_cut_with_a_mark_rather_than_by_the_edge() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = crate::App::new(tx, test_runtime());
    app.loading_for_tests(
        Some(std::path::PathBuf::from("/tmp/blocks")),
        2048,
        "Reading schema",
        40,
    );
    app.footer_progress().begin(6541);
    for _ in 0..1203 {
        app.footer_progress().advance();
    }
    app.begin_frame();

    let ellipsis = crate::glyphs::get().ellipsis;
    // Every width the panel draws at, not a handful: the earlier list skipped the
    // band either side of where the count stops fitting, which is exactly where a
    // truncation is wrong if it is wrong anywhere.
    for width in 12u16..=60 {
        let area = Rect::new(0, 0, width, 20);
        let mut buf = Buffer::empty(area);
        render(area, &mut buf, &app, &RenderContext::for_test());
        // The panel is centred, so the phase is not always on row zero.
        let line = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .find(|row| row.contains("Read"))
            .map(|row| row.trim_end().to_string())
            .unwrap_or_else(|| panic!("no phase line at {width}"));
        // Every phase line ends with the mark by design, so a cut one and a whole
        // one look alike — which is the point: what must never happen is a cut
        // with *nothing* saying so, reading as a whole number smaller than the
        // real one. A number cut at some width is unavoidable; "of 6,5" is all
        // that fits in thirty-three columns.
        assert!(
            line.ends_with(ellipsis),
            "at {width} the line ends with nothing to say it may be cut: {line:?}"
        );
        // And where there is room for the whole count, it is not cut anyway. The
        // room needed is derived rather than counted out here because it is not a
        // constant: the ASCII glyph set spells the mark "..." rather than "…",
        // two columns more, so a number written in here would be right under one
        // locale and wrong under the other.
        let whole = format!("Reading footers: 1,203 of 6,541{ellipsis}")
            .chars()
            .count()
            + 3; // the spinner and its two spaces
        if width as usize >= whole {
            assert!(
                line.contains("1,203 of 6,541"),
                "at {width} the whole count fits: {line:?}"
            );
        }
    }
}

#[test]
fn the_phase_and_the_file_are_both_on_screen() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = crate::App::new(tx, test_runtime());
    app.loading_for_tests(
        Some(std::path::PathBuf::from("/tmp/quarterly.parquet")),
        2048,
        "Reading schema",
        40,
    );

    let area = Rect::new(0, 0, 60, 20);
    let mut buf = Buffer::empty(area);
    render(area, &mut buf, &app, &RenderContext::for_test());

    let text = crate::tests::buffer_text(&buf);
    assert!(text.contains("Reading schema"), "phase missing: {text:?}");
    assert!(text.contains("quarterly.parquet"), "file missing: {text:?}");
    assert!(text.contains("2.0 KiB"), "size missing: {text:?}");
}

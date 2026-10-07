use super::*;

#[test]
fn help_wrap_breaks_at_word_boundaries() {
    let wrapped = wrap_help_line("the quick brown fox jumps", 11);
    assert_eq!(wrapped, vec!["the quick", "brown fox", "jumps"]);
}

/// Width is characters, not bytes: a line of multibyte glyphs (arrows,
/// box drawing) must not fold early or split inside a character.
#[test]
fn help_wrap_measures_characters_not_bytes() {
    let wrapped = wrap_help_line("↑↓ / j/k: Navigate", 18);
    assert_eq!(wrapped, vec!["↑↓ / j/k: Navigate"]);
}

#[test]
fn an_overlong_word_is_split_rather_than_lost() {
    let wrapped = wrap_help_line("see /a/very/long/path/that/never/ends", 10);
    assert_eq!(wrapped.first().map(String::as_str), Some("see"));
    assert!(wrapped.iter().all(|line| line.chars().count() <= 10));
    assert_eq!(
        wrapped.join(""),
        "see/a/very/long/path/that/never/ends".replace(' ', "")
    );
}

/// A keyed row hangs under its description, a bullet past its dash, a
/// prose line at its indent; a column too narrow to hang in gives way to
/// the indent.
#[test]
fn help_wrap_hangs_under_the_text_it_continues() {
    let row = "  Enter:      Open a finding, then its rows";
    assert_eq!(
        wrap_help_line(row, 32),
        [
            "  Enter:      Open a finding,",
            "              then its rows"
        ]
    );
    assert_eq!(
        wrap_help_line("  A note is an observation, not a fault", 24),
        ["  A note is an", "  observation, not a", "  fault"]
    );
    assert_eq!(
        wrap_help_line("  - Empty select: select (all columns)", 24),
        ["  - Empty select: select", "    (all columns)"]
    );
    assert_eq!(
        wrap_help_line(row, 24),
        ["  Enter:      Open a", "  finding, then its rows"]
    );
}

/// Every row of a help screen as the overlay lays it out in `area`, read
/// off the buffer one scroll position at a time.
/// The width the help overlay wraps its text to in `area`.
fn grid(buf: &Buffer, area: Rect) -> Vec<String> {
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

/// A message taller than the capped frame scrolls: the first row is
/// there at the top, the last is reachable at the bottom, and the cut
/// row counts what is below instead of half-drawing it.
#[test]
fn a_long_error_scrolls_instead_of_hiding_its_tail() {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 60, 20);
    let long = (0..40)
        .map(|i| format!("diagnostic line {i}"))
        .collect::<Vec<_>>()
        .join("\n");

    let mut modal = crate::ErrorModal::new();
    modal.show(long.clone());
    let mut buf = Buffer::empty(area);
    render_error_modal(area, &mut buf, &mut modal, &ctx);
    let text = grid(&buf, area).join("\n");
    assert!(text.contains("diagnostic line 0"), "{text}");
    assert!(text.contains("more"), "the cut says what is below: {text}");

    // Over-scrolling clamps, and the tail becomes reachable.
    modal.scroll = usize::MAX;
    let mut buf = Buffer::empty(area);
    render_error_modal(area, &mut buf, &mut modal, &ctx);
    let text = grid(&buf, area).join("\n");
    assert!(
        text.contains("diagnostic line 39"),
        "the last line is reachable: {text}"
    );
}

/// One border, no bordered buttons: the frame's corners are the only ones.
#[test]
fn the_error_modal_is_one_surface_with_the_keys_in_the_footer() {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    let mut modal = crate::ErrorModal::new();
    modal.show("Select at least one index column.".to_string());
    render_error_modal(area, &mut buf, &mut modal, &ctx);
    let rows = grid(&buf, area);
    let frames = crate::glyphs::frame_corners(&rows).len();
    assert_eq!(frames, 1, "one frame, no inner boxes: {rows:#?}");
    let text = rows.join("\n");
    assert!(text.contains("Select at least one index column."));
    let footer = rows
        .iter()
        .find(|row| row.contains("Enter"))
        .expect("the footer");
    assert!(footer.contains("Close"), "{footer:?}");
    assert!(
        !footer.contains("OK") && !footer.contains("Esc"),
        "{footer:?}"
    );
}

/// The focused choice carries the rail; there is nothing to Tab onto.
#[test]
fn the_confirmation_modal_marks_the_choice_with_the_rail() {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    let mut modal = crate::ConfirmationModal::new();
    modal.show(
        "Overwrite out.csv?".to_string(),
        crate::app::feedback::Confirm::ClearRecents,
    );
    render_confirmation_modal(area, &mut buf, &mut modal, &ctx);
    let rows = grid(&buf, area);
    let frames = crate::glyphs::frame_corners(&rows).len();
    assert_eq!(frames, 1, "one frame, no button boxes: {rows:#?}");
    let text = rows.join("\n");
    assert!(text.contains("Overwrite out.csv?"));
    let choice_row = rows
        .iter()
        .find(|r| r.contains("Yes") && r.contains("No"))
        .expect("the Yes/No line is there");
    let rail = crate::glyphs::get().rail;
    assert!(
        choice_row.contains(&format!("{rail}Yes")),
        "the rail is on Yes by default: {choice_row:?}"
    );
    assert!(text.contains("Confirm") && text.contains("Cancel"));
    // The chosen label is in the accent, as focus is everywhere: never the
    // brighter one.
    let y = rows.iter().position(|r| r == choice_row).unwrap() as u16;
    let x = (0..area.width)
        .find(|&x| buf[(x, y)].symbol() == "Y")
        .unwrap();
    assert_eq!(buf[(x, y)].fg, ctx.accent);

    // Switching focus moves the rail, not the labels.
    modal.focus_yes = false;
    let mut buf2 = Buffer::empty(area);
    render_confirmation_modal(area, &mut buf2, &mut modal, &ctx);
    let rows2 = grid(&buf2, area);
    let choice_row2 = rows2
        .iter()
        .find(|r| r.contains("Yes") && r.contains("No"))
        .unwrap();
    assert!(
        choice_row2.contains(&format!("{rail}No")),
        "{choice_row2:?}"
    );
    // Compare columns, not byte offsets: the rail glyph is multi-byte.
    let col = |r: &str| r.replace(rail, " ").find("Yes");
    assert_eq!(
        col(choice_row),
        col(choice_row2),
        "labels hold still while the rail moves"
    );
}

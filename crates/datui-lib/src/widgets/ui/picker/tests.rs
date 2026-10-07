use super::*;

fn state() -> PickerState {
    PickerState::new(
        ["CSV", "Parquet", "JSON", "NDJSON", "Arrow", "Avro"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    )
}

#[test]
fn typing_narrows_and_backspace_widens() {
    let mut s = state();
    s.type_char('a');
    let names: Vec<&str> = s.filtered().iter().map(|(_, n)| *n).collect();
    assert_eq!(names, ["Arrow", "Avro", "Parquet"], "starts first");
    s.type_char('r');
    let names: Vec<&str> = s.filtered().iter().map(|(_, n)| *n).collect();
    assert_eq!(
        names,
        ["Arrow", "Parquet"],
        "matches anywhere, ignoring case"
    );
    s.backspace();
    s.backspace();
    assert_eq!(s.filtered().len(), 6);
}

#[test]
fn narrowing_keeps_the_cursor_on_its_item_when_it_survives() {
    let mut s = state();
    s.select_original(4); // Arrow
    s.type_char('r');
    assert_eq!(
        s.selected_original(),
        Some(4),
        "Arrow matches 'r' and keeps the cursor"
    );
    s.type_char('q');
    assert_eq!(
        s.selected_original(),
        Some(1),
        "'rq' filters Arrow away, so the cursor lands on the first match"
    );
}

#[test]
fn movement_walks_the_visible_items_and_wraps() {
    let mut s = state();
    s.type_char('a'); // Arrow, Avro, Parquet
    s.move_down();
    assert_eq!(s.selected_original(), Some(5));
    s.move_down();
    assert_eq!(s.selected_original(), Some(1));
    s.move_down();
    assert_eq!(s.selected_original(), Some(4), "wraps to the top");
    s.move_up();
    assert_eq!(s.selected_original(), Some(1), "and back around");
}

/// Typing a whole name lists it first and takes the cursor there, off an item
/// that only contains it: `hour` picks `hour`, not the `time_hour` it was on.
#[test]
fn a_name_typed_whole_ranks_first_and_takes_the_cursor() {
    let mut s = PickerState::new(
        ["time_hour", "dep_delay", "Hour", "hours"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    s.select_original(0);
    for c in "hour".chars() {
        s.type_char(c);
    }
    let names: Vec<&str> = s.filtered().iter().map(|(_, n)| *n).collect();
    assert_eq!(names, ["Hour", "hours", "time_hour"]);
    assert_eq!(s.selected_original(), Some(2));
    s.backspace();
    assert_eq!(
        s.selected_original(),
        Some(2),
        "kept while it still matches"
    );
}

#[test]
fn a_filter_that_admits_nothing_chooses_nothing_and_never_panics() {
    let mut s = state();
    for c in "zzz".chars() {
        s.type_char(c);
    }
    assert_eq!(s.selected_original(), None);
    s.move_down();
    s.move_up();
    assert_eq!(s.selected_original(), None);
}

fn render_rows(picker: &Picker, width: u16, height: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    picker.render(area, &mut buf, &ctx);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

#[test]
fn the_selection_carries_the_rail_and_only_the_selection() {
    let g = crate::glyphs::get();
    let picker = Picker::new(vec!["CSV", "Parquet", "JSON"], Some(1), true);
    let rows = render_rows(&picker, 20, 3);
    assert!(
        rows[1].starts_with(&format!("{}Parquet", g.rail)),
        "got {rows:?}"
    );
    assert!(rows[0].starts_with(" CSV"), "got {rows:?}");
    assert!(rows[2].starts_with(" JSON"), "got {rows:?}");
}

/// The rail leaves with focus — it is the form's one "you are here" —
/// but the chosen item never stops being visible: it keeps the accent
/// and a marker glyph, so it survives a terminal with no color at all.
#[test]
fn an_unfocused_selection_stays_visible_without_the_rail() {
    let g = crate::glyphs::get();
    let picker = Picker::new(vec!["CSV", "Parquet"], Some(0), false);
    let rows = render_rows(&picker, 20, 2);
    assert!(
        rows[0].starts_with(&format!("{}CSV", g.middot)),
        "the choice keeps a glyph without focus: {rows:?}"
    );
    assert!(
        !rows[0].starts_with(g.rail),
        "but never the rail, which means focus: {rows:?}"
    );

    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, 20, 2);
    let mut buf = Buffer::empty(area);
    picker.render(area, &mut buf, &ctx);
    assert_ne!(
        buf[(1, 0)].fg,
        buf[(1, 1)].fg,
        "the chosen item still reads apart from the rest"
    );
}

/// Items past the window are counted, not half-drawn.
#[test]
fn overflow_is_counted_on_the_last_row() {
    let picker = Picker::new(vec!["a", "b", "c", "d", "e"], Some(0), true);
    let rows = render_rows(&picker, 20, 3);
    assert!(rows[2].contains("3 more"), "got {rows:?}");
}

/// Marks turn the list into a toggle list: every item carries a checkbox,
/// on or off, so what is already chosen never has to be remembered.
#[test]
fn marks_draw_a_checkbox_on_every_item() {
    let g = crate::glyphs::get();
    let picker = Picker::new(vec!["dept", "region"], Some(0), true).marks(vec![true, false]);
    let rows = render_rows(&picker, 20, 2);
    assert!(
        rows[0].contains(&format!("{} dept", g.checkbox_on)),
        "got {rows:?}"
    );
    assert!(
        rows[1].contains(&format!("{} region", g.checkbox_off)),
        "got {rows:?}"
    );
}

#[test]
fn scrolling_keeps_the_selection_in_view() {
    let picker = Picker::new(vec!["a", "b", "c", "d", "e"], Some(4), true);
    let g = crate::glyphs::get();
    let rows = render_rows(&picker, 20, 3);
    assert!(
        rows[2].starts_with(&format!("{}e", g.rail)),
        "the selected last item is drawn, not the overflow count: {rows:?}"
    );
}
/// A chord is a chord: Ctrl+W edits the filter, and no modified
/// character ever lands in it as a letter.
#[test]
fn the_filter_keeps_readline_chords_out_of_the_text() {
    use crossterm::event::KeyModifiers;
    let mut p = PickerState::new(vec!["first_name".to_string(), "start date".to_string()]);
    for c in "start d".chars() {
        p.filter_key(c, KeyModifiers::NONE);
    }
    assert_eq!(p.filter, "start d");
    p.filter_key('w', KeyModifiers::CONTROL);
    assert_eq!(p.filter, "start ", "Ctrl+W drops the word, not types w");
    p.filter_key('u', KeyModifiers::CONTROL);
    assert_eq!(p.filter, "", "Ctrl+U clears, not types u");
    p.filter_key('x', KeyModifiers::ALT);
    assert_eq!(p.filter, "", "an Alt chord is not a letter");
}

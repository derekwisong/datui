use super::*;

fn text(values: u16) -> PageMeasure {
    PageMeasure {
        header: 4,
        type_label: 3,
        values,
        has_values: true,
        clips: true,
    }
}

fn number(values: u16) -> PageMeasure {
    PageMeasure {
        clips: false,
        ..text(values)
    }
}

#[test]
fn the_cap_is_two_fifths_of_the_terminal_within_bounds() {
    assert_eq!(text_cap(80), 32);
    assert_eq!(text_cap(120), 48);
    assert_eq!(text_cap(60), 24);
    assert_eq!(text_cap(20), 16);
    assert_eq!(text_cap(400), 64);
}

/// Text keeps the width of the first page it showed a value on; later pages
/// neither widen nor narrow it.
#[test]
fn text_keeps_its_first_page_width() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    assert_eq!(widths.width("d", &s, text(10), 32), 10);
    assert_eq!(widths.width("d", &s, text(200), 32), 10);
    assert_eq!(widths.width("d", &s, text(2), 32), 10);
}

/// A first page of nulls teaches nothing: the first page with a value does.
#[test]
fn a_page_of_nulls_does_not_settle_text() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    let nulls = PageMeasure {
        values: 1,
        has_values: false,
        ..text(1)
    };
    assert_eq!(widths.width("d", &s, nulls, 32), 4);
    assert_eq!(widths.width("d", &s, text(12), 32), 12);
    assert_eq!(widths.width("d", &s, text(20), 32), 12);
}

/// Long text and long headings are bounded by the cap.
#[test]
fn automatic_text_and_headings_stop_at_the_cap() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    assert_eq!(widths.width("d", &s, text(215), 32), 32);
    let long_heading = PageMeasure {
        header: 105,
        ..number(3)
    };
    assert_eq!(widths.width("n", &DataType::Int64, long_heading, 32), 32);
}

/// A number never draws narrower than its widest value seen, and does not shrink
/// back on a page of narrower ones.
#[test]
fn numbers_widen_and_stay_wide() {
    let mut widths = ColumnWidths::default();
    let i = DataType::Int64;
    assert_eq!(widths.width("n", &i, number(5), 32), 5);
    assert_eq!(widths.width("n", &i, number(7), 32), 7);
    assert_eq!(widths.width("n", &i, number(2), 32), 7);
}

/// A width set by hand is exact for text, and a floor for numbers.
#[test]
fn a_manual_width_is_exact_for_text_and_a_floor_for_numbers() {
    let mut widths = ColumnWidths::default();
    widths.set_choice("d", &DataType::String, WidthChoice::Manual(6));
    assert_eq!(widths.width("d", &DataType::String, text(30), 32), 6);
    widths.set_choice("n", &DataType::Int64, WidthChoice::Manual(6));
    assert_eq!(widths.width("n", &DataType::Int64, number(3), 32), 6);
    assert_eq!(widths.width("n", &DataType::Int64, number(9), 32), 9);
    widths.set_choice("d", &DataType::String, WidthChoice::Manual(1));
    assert_eq!(
        widths.choice("d", &DataType::String),
        WidthChoice::Manual(MIN_WIDTH)
    );
}

/// The same name with another type is another column: it starts afresh, and
/// the first comes back with its own width.
#[test]
fn a_column_is_its_name_and_type() {
    let mut widths = ColumnWidths::default();
    widths.set_choice("x", &DataType::Int64, WidthChoice::Manual(20));
    assert_eq!(widths.width("x", &DataType::String, text(5), 32), 5);
    assert_eq!(widths.choice("x", &DataType::String), WidthChoice::Auto);
    assert_eq!(widths.width("x", &DataType::Int64, number(3), 32), 20);
}

#[test]
fn fit_takes_the_page_and_automatic_returns_to_the_learned_width() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    assert_eq!(widths.width("d", &s, text(10), 32), 10);
    widths.set_choice("d", &s, WidthChoice::Fit);
    assert_eq!(widths.fits_pending(), vec![("d".to_string(), s.clone())]);
    widths.fit("d", &s, text(90), 32);
    assert!(widths.fits_pending().is_empty());
    assert_eq!(widths.choice("d", &s), WidthChoice::Manual(90));
    assert_eq!(widths.width("d", &s, text(3), 32), 90);
    widths.set_choice("d", &s, WidthChoice::Auto);
    assert_eq!(widths.width("d", &s, text(3), 32), 10);
}

/// A relearn waits for the new view's rows: the old ones drawn meanwhile teach
/// nothing that lasts. Then text and numbers start again, and manual widths stay.
#[test]
fn a_relearn_starts_again_from_the_next_rows() {
    let mut widths = ColumnWidths::default();
    let (s, i) = (DataType::String, DataType::Int64);
    assert_eq!(widths.width("d", &s, text(10), 32), 10);
    assert_eq!(widths.width("n", &i, number(9), 32), 9);
    widths.set_choice("m", &s, WidthChoice::Manual(7));
    widths.relearn();
    assert_eq!(widths.width("d", &s, text(20), 32), 10);
    widths.rows_arrived();
    assert_eq!(widths.width("d", &s, text(20), 32), 20);
    assert_eq!(widths.width("d", &s, text(30), 32), 20);
    assert_eq!(widths.width("n", &i, number(3), 32), 4);
    assert_eq!(widths.width("m", &s, text(30), 32), 7);
    // Only once: later rows are paging.
    widths.rows_arrived();
    assert_eq!(widths.width("d", &s, text(5), 32), 20);
}

/// A width drawn before a relearn is not one to plan a page with until the
/// column is drawn again; it is still the width the sidebar steps from.
#[test]
fn a_relearn_makes_drawn_widths_unknown_until_drawn_again() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    assert_eq!(widths.drawn("d", &s), None);
    assert_eq!(widths.width("d", &s, text(10), 32), 10);
    assert_eq!(widths.drawn("d", &s), Some(10));
    widths.relearn();
    widths.rows_arrived();
    assert_eq!(widths.drawn("d", &s), None);
    assert_eq!(widths.shown("d", &s), Some(10));
    assert_eq!(widths.width("d", &s, text(4), 32), 4);
    assert_eq!(widths.drawn("d", &s), Some(4));
}

/// A relearn whose view never showed is dropped.
#[test]
fn a_relearn_kept_back_changes_nothing() {
    let mut widths = ColumnWidths::default();
    let s = DataType::String;
    assert_eq!(widths.width("d", &s, text(10), 32), 10);
    widths.relearn();
    widths.keep_learned();
    widths.rows_arrived();
    assert_eq!(widths.width("d", &s, text(20), 32), 10);
}

#[test]
fn narrower_and_wider_step_from_what_is_drawn() {
    assert_eq!(
        WidthChoice::Auto.wider(Some(10)),
        WidthChoice::Manual(10 + WIDTH_STEP)
    );
    assert_eq!(
        WidthChoice::Auto.narrower(Some(10)),
        WidthChoice::Manual(10 - WIDTH_STEP)
    );
    assert_eq!(
        WidthChoice::Manual(MIN_WIDTH).narrower(None),
        WidthChoice::Manual(MIN_WIDTH)
    );
    assert_eq!(
        WidthChoice::Manual(MAX_WIDTH).wider(None),
        WidthChoice::Manual(MAX_WIDTH)
    );
    assert_eq!(
        WidthChoice::Fit.wider(None),
        WidthChoice::Manual(UNSEEN_WIDTH + WIDTH_STEP)
    );
}

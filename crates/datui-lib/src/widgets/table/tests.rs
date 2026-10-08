use super::*;
use crate::numfmt::NumberFormatSettings;
use crate::table::binary_stub;

thread_local! {
    /// Columns of cells formatted on this thread: a test's own thread draws.
    pub(crate) static FORMATTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Read the header (top) row of a rendered buffer as a string.
pub(crate) fn header_row_string(buf: &Buffer, area: Rect) -> String {
    (area.x..area.x + area.width)
        .map(|x| buf[(x, area.y)].symbol().to_string())
        .collect()
}

// Read an arbitrary row of a rendered buffer as a string (y = 0 is the header).
pub(crate) fn row_string(buf: &Buffer, area: Rect, y: u16) -> String {
    (area.x..area.x + area.width)
        .map(|x| buf[(x, area.y + y)].symbol().to_string())
        .collect()
}

fn table_with_format(preset: &str, align: bool) -> DataTable {
    DataTable::default().with_number_format(NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset(preset).unwrap(),
        enabled: true,
        exclude: Vec::new(),
        align_numeric_right: align,
    })
}

#[test]
fn grouping_is_off_by_default() {
    // Upgrading must not change how anything renders.
    let table = DataTable::default();
    let df = df!("pos" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("1234567"), "got: {row:?}");
    assert!(!row.contains("1,234,567"), "got: {row:?}");
}

#[test]
fn thousands_separators_are_applied_to_integer_columns() {
    let table = table_with_format("thousands", false);
    // Genomic coordinates, the case from issue #51.
    let df = df!("chromStart" => &[248956422i64, 3088269832]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).contains("248,956,422"));
    assert!(row_string(&buf, area, 2).contains("3,088,269,832"));
}

#[test]
fn column_width_accounts_for_separators() {
    // The separators widen the column; the heading must not be clipped and
    // the value must render in full.
    let table = table_with_format("thousands", false);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 12, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(shown, 1);
    assert!(row_string(&buf, area, 1).contains("1,234,567"));
}

#[test]
fn strings_are_untouched_and_integers_group_uniformly() {
    let table = table_with_format("thousands", false);
    let df = df!(
        "chrom" => &["chr1"],
        "n" => &[2024i32],
    )
    .unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("chr1"), "got: {row:?}");
    // No magnitude threshold: a column must not mix grouped and ungrouped
    // values, so four-digit numbers group like everything else.
    assert!(row.contains("2,024"), "got: {row:?}");
}

/// Excluding a column, by name or by pattern, is how identifier columns stay
/// plain: the replacement for a digit threshold.
#[test]
fn excluded_columns_are_not_grouped() {
    let row = |glob: &str, df: DataFrame| {
        let table = DataTable::default().with_number_format(NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
            enabled: true,
            exclude: vec![crate::numfmt::Glob::new(glob)],
            align_numeric_right: false,
        });
        let area = Rect::new(0, 0, 40, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false);
        row_string(&buf, area, 1)
    };
    let shown = row(
        "year",
        df!("year" => &[2024i32], "count" => &[2024i32]).unwrap(),
    );
    assert!(
        shown.contains("2024"),
        "excluded column stays plain: {shown:?}"
    );
    assert!(shown.contains("2,024"), "other column groups: {shown:?}");
    let shown = row(
        "*_id",
        df!("sample_id" => &[1234567i64], "count" => &[1234567i64]).unwrap(),
    );
    assert!(shown.contains("1234567"), "excluded column raw: {shown:?}");
    assert!(
        shown.contains("1,234,567"),
        "other column grouped: {shown:?}"
    );
}

#[test]
fn numeric_columns_and_their_headers_render_flush_right() {
    let table = table_with_format("none", true);
    // Header "value" is 5 wide; the values are shorter, so they must be
    // padded on the left, not the right.
    let df = df!("value" => &[7i64, 42]).unwrap();
    let area = Rect::new(0, 0, 5, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "    7");
    assert_eq!(row_string(&buf, area, 2), "   42");
    assert_eq!(header_row_string(&buf, area), "value");
}

#[test]
fn header_follows_its_column_alignment() {
    // A wide numeric column: the heading must sit flush right over the
    // digits rather than floating left.
    let table = table_with_format("none", true);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 7, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(header_row_string(&buf, area), "      n");
    assert_eq!(row_string(&buf, area, 1), "1234567");
}

#[test]
fn non_numeric_columns_stay_left_aligned() {
    let table = table_with_format("none", true);
    let df = df!("name" => &["ab"]).unwrap();
    let area = Rect::new(0, 0, 4, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "ab  ");
    assert_eq!(header_row_string(&buf, area), "name");
}

#[test]
fn alignment_can_be_turned_off() {
    let table = table_with_format("none", false);
    let df = df!("value" => &[7i64]).unwrap();
    let area = Rect::new(0, 0, 5, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "7    ");
}

#[test]
fn disabled_formatting_renders_raw_digits() {
    // What the `,` toggle does: same settings, enabled = false.
    let mut settings = NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
        enabled: true,
        exclude: Vec::new(),
        align_numeric_right: false,
    };
    settings.enabled = false;
    let table = DataTable::default().with_number_format(settings);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).contains("1234567"));
}

#[test]
fn binary_stub_columns_are_never_formatted_or_aligned() {
    // The stub is a placeholder, not data.
    let table = DataTable {
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        ..table_with_format("thousands", true)
    };
    let df = df!("blob" => &[binary_stub()]).unwrap();
    let area = Rect::new(0, 0, 10, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).starts_with(binary_stub()));
}

/// The buffer holds a binary column as stub text; the type row still says binary.
#[test]
fn a_binary_column_s_type_row_says_binary() {
    let table = DataTable {
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        dtype_row: true,
        ..table_with_format("thousands", true)
    };
    let df = df!("blob" => &[binary_stub()], "s" => &["x"]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let types = row_string(&buf, area, 1);
    assert!(types.contains("binary"), "{types:?}");
    assert_eq!(types.matches("str").count(), 1, "{types:?}");
}

/// A line break or a tab in a value is marked in the one-line cell; drawn as
/// is, ratatui drops it and `line1\nline2` reads `line1line2`.
#[test]
fn breaks_tabs_and_controls_are_marked_in_a_cell() {
    let table = DataTable::default();
    let df = df!("s" => ["line1\nline2", "tab\tseparated", "esc\u{1b}[0m"]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let g = table.glyphs;
    assert!(
        row_string(&buf, area, 1).starts_with(&format!("line1{}line2", g.newline_mark)),
        "{:?}",
        row_string(&buf, area, 1)
    );
    assert!(row_string(&buf, area, 2).starts_with(&format!("tab{}separated", g.tab_mark)));
    assert!(row_string(&buf, area, 3).starts_with(&format!("esc{}[0m", g.control_mark)));
}

/// A direction control in a value is marked too: drawn, a terminal that lays
/// out bidirectional text would reverse the rest of the row.
#[test]
fn direction_controls_are_marked_in_a_cell() {
    let table = DataTable::default();
    let df = df!("s" => ["a\u{202e}evil\u{202c}z"]).unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    table.render_dataframe(&df, area, &mut buf, &mut TableState::default(), false);
    let m = table.glyphs.control_mark;
    let row = row_string(&buf, area, 1);
    assert!(row.starts_with(&format!("a{m}evil{m}z")), "{row:?}");
    assert!(
        !buf.content()
            .iter()
            .any(|c| c.symbol().contains('\u{202e}'))
    );
}

/// A cell keeps and measures only what it can show of a huge value, past the
/// widest it can be drawn, and says it goes on.
#[test]
fn a_huge_value_is_measured_by_its_start() {
    let table = DataTable::default();
    let huge = "x".repeat(1 << 20);
    let df = df!("s" => [huge.as_str()]).unwrap();
    let mut scratch = String::new();
    let slice = table.slice_column(&df, 0, 1, &HashSet::new(), &mut scratch, None);
    let ellipsis = crate::glyphs::cell_width(table.glyphs.ellipsis);
    let widest = usize::from(crate::widgets::column_widths::MAX_WIDTH);
    assert_eq!(usize::from(slice.cells.value_width), widest + 1 + ellipsis);
}

#[test]
fn trailing_overflow_string_column_is_truncated_not_dropped() {
    // A string trailing column that doesn't fully fit should still be shown truncated
    // (filling the remaining width) rather than dropped entirely (leaving blank space).
    let table = DataTable::default();
    let df = df!(
        "a" => &[1i32, 2, 3],
        "wide_text" => &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
    )
    .unwrap();
    // "a" needs width 1; with padding that's used_width 2, leaving 6 for "wide_text",
    // whose full width (10) overflows -> it must be shown truncated to the remaining 6.
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 2,
        "the overflowing trailing string column should be kept (truncated)"
    );
    // Part of the heading should be visible so the user knows what the column is,
    // behind the clip marker (three cells in the ASCII set).
    assert!(
        header_row_string(&buf, area).contains("wid"),
        "truncated column heading should be visible: {:?}",
        header_row_string(&buf, area)
    );
}

#[test]
fn binary_stub_cells_are_styled_with_binary_color_and_italic() {
    // Binary columns render the `‹binary›` stub; those cells should be colored with
    // binary_col and italicized so they read as a placeholder, while ordinary columns
    // keep their normal (non-italic) styling.
    let table = DataTable {
        binary_col: Some(Color::DarkGray),
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        ..DataTable::default()
    };
    let df = df!(
        "a" => &[1i32, 2],
        "blob" => &[binary_stub(), binary_stub()],
    )
    .unwrap();
    let area = Rect::new(0, 0, 20, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);

    // A data row (y = 1; y = 0 is the header). The stub cells should be dark gray + italic.
    let stub_styled = (area.x..area.x + area.width).any(|x| {
        let cell = &buf[(x, 1)];
        cell.fg == Color::DarkGray && cell.modifier.contains(Modifier::ITALIC)
    });
    assert!(
        stub_styled,
        "binary stub cells should be colored with binary_col and italicized"
    );
    // The non-binary "a" column must not be italicized.
    let any_italic_non_darkgray = (area.x..area.x + area.width).any(|x| {
        let cell = &buf[(x, 1)];
        cell.modifier.contains(Modifier::ITALIC) && cell.fg != Color::DarkGray
    });
    assert!(
        !any_italic_non_darkgray,
        "only binary columns should be italicized"
    );
}

#[test]
fn trailing_overflow_numeric_column_is_dropped_not_truncated() {
    // A numeric column must NOT be shown truncated: a partial number reads as a wrong value.
    let table = DataTable::default();
    let df = df!(
        "a" => &[1i32, 2, 3],
        "wide_number" => &[111_111_111i64, 222_222_222, 333_333_333],
    )
    .unwrap();
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 1,
        "an overflowing numeric column should be dropped, not truncated"
    );
}

#[test]
fn trailing_overflow_binary_column_is_truncated() {
    // Binary columns render as text (e.g. b"...") and should be truncated like strings —
    // this is the EDGAR `txt_bytes` case where a wide Binary column was wrongly dropped.
    let table = DataTable::default();
    let a = Series::new("a".into(), &[1i32, 2, 3]);
    let bin = Series::new(
        "wide_bytes".into(),
        &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
    )
    .cast(&DataType::Binary)
    .unwrap();
    let df = DataFrame::new_infer_height(vec![a.into(), bin.into()]).unwrap();
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 2,
        "an overflowing binary column should be shown truncated"
    );
}

#[test]
fn tiny_remaining_width_drops_overflow_string_column() {
    // Even a string column should not render a useless 1-2 char sliver.
    let table = DataTable::default();
    let df = df!(
        "abcd" => &[1i32, 2, 3],
        "next" => &["yyyy", "yyyy", "yyyy"],
    )
    .unwrap();
    // "abcd" is 4 wide; used_width becomes 5, leaving only 1 (< MIN) for "next".
    let area = Rect::new(0, 0, 5, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(shown, 1, "a sub-minimal sliver should not be shown");
}

#[test]
fn sorted_column_header_carries_the_direction_mark() {
    // A sorted view must not look identical to an unsorted one: the sorted
    // column's header says so, and only that column's.
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(vec!["age".to_string()], vec![false]);
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}", g.sort_asc)),
        "the sorted column is marked: {header:?}"
    );
    assert!(
        !header.contains(&format!("name{}", g.sort_asc)),
        "the unsorted column is not: {header:?}"
    );
    assert!(
        !header.contains(g.sort_desc),
        "an ascending sort never shows the descending mark: {header:?}"
    );
}

#[test]
fn the_direction_mark_flips_with_the_sort() {
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(vec!["age".to_string()], vec![true]);
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}", g.sort_desc)),
        "a descending sort points down: {header:?}"
    );
    assert!(!header.contains(g.sort_asc), "and never up: {header:?}");
}

#[test]
fn every_column_of_a_multi_sort_is_marked() {
    // Marks only, no position numbers: the columns all run the same way.
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(
        vec!["name".to_string(), "age".to_string()],
        vec![false, false],
    );
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    for name in ["name", "age"] {
        assert!(
            header.contains(&format!("{name}{}", g.sort_asc)),
            "{name} carries the mark: {header:?}"
        );
    }
}

#[test]
fn the_sort_mark_composes_with_the_drift_mark() {
    // A column can be sorted and drifting at once; the header carries both
    // marks and the width arithmetic counts both, so nothing is clipped.
    let g = crate::glyphs::get();
    let table = DataTable::default()
        .with_sort(vec!["age".to_string()], vec![false])
        .with_drift(
            Vec::new(),
            Arc::new(vec![crate::formats::schema_union::DriftGroup {
                absent: vec!["age".into()],
                unread: Vec::new(),
            }]),
        );
    let df = df!("age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}{}", g.drift_mark, g.sort_asc)),
        "footnote first, direction after: {header:?}"
    );
    assert!(
        row_string(&buf, area, 1).contains("41"),
        "the widened header does not clip the value"
    );
}

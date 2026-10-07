//! Rows drawn through the widget from a state.

use super::*;

/// The ORDER BY of the SQL in effect is state, so it leaves its mark: the sort
/// mark on each column it orders by, as named in the result, until the sidebar
/// sorts or another query runs (#688, item 13).
#[cfg(feature = "sql")]
#[test]
fn a_sql_order_by_marks_the_header_until_the_sidebar_sorts() {
    let df = df!("k" => [1i64, 2, 3], "v" => [3i64, 2, 1]).unwrap();
    let marks = |sql: &str| {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        state.header_sort()
    };
    let owned = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    assert_eq!(
        marks("SELECT v, k FROM df ORDER BY k DESC"),
        (owned(&["k"]), vec![true])
    );
    assert_eq!(
        marks("SELECT * FROM df ORDER BY k DESC, v LIMIT 2"),
        (owned(&["k", "v"]), vec![true, false])
    );
    assert_eq!(
        marks("SELECT v AS w, k FROM df ORDER BY w"),
        (owned(&["w"]), vec![false]),
        "named as in the result"
    );
    assert_eq!(
        marks("SELECT k, SUM(v) AS s FROM df GROUP BY k ORDER BY s DESC"),
        (owned(&["s"]), vec![true])
    );
    // An expression, or a column the result leaves out, leaves no mark.
    assert_eq!(marks("SELECT * FROM df ORDER BY k + 1"), (vec![], vec![]));
    assert_eq!(marks("SELECT v FROM df ORDER BY k"), (vec![], vec![]));
    assert_eq!(marks("SELECT * FROM df"), (vec![], vec![]));

    // On the header, and the sidebar's sort replaces it.
    let mut state =
        DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
    state.sql_query("SELECT * FROM df ORDER BY k DESC".to_string());
    state.collect();
    let area = Rect::new(0, 0, 30, 6);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = row_string(&buf, area, 0);
    let g = crate::glyphs::get();
    assert!(header.contains(&format!("k{}", g.sort_desc)), "{header:?}");
    state.sort_by(vec!["v".to_string()], vec![false]);
    assert_eq!(state.header_sort(), (owned(&["v"]), vec![false]));
    // A new query names its own order, or none.
    state.sql_query("SELECT * FROM df".to_string());
    assert_eq!(state.header_sort(), (vec![], vec![]));
}

#[test]
fn byte_clamp_keeps_view_near_end_of_buffer() {
    // Regression: jumping to the END of a large dataset used to go blank because the
    // max_buffered_mb byte-clamp trimmed the buffer's HEAD (slice(0, max_rows)), discarding
    // exactly the tail rows the view needed. The clamp must keep the view in range.
    let lf = df!("a" => &["seed"]).unwrap().lazy();
    // 1 MB byte budget.
    let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
    state.num_rows = 1000;
    state.num_rows_valid = true;
    state.visible_rows = 40;
    state.start_row = 960; // jump-to-end position (num_rows - visible_rows)

    // Buffer spans [900, 1000); the view [960, 1000) sits at its tail. ~2 MB of data forces
    // a trim to roughly half the rows.
    let buffer_start = 900;
    let big: Vec<String> = (0..100).map(|_| "z".repeat(20_000)).collect();
    let df = df!("a" => big).unwrap();

    let fitted = state.fill_plan(buffer_start, 1000, 1000, true).fit(df);
    let (sliced, eff_start) = (fitted.df, fitted.start);
    let eff_end = eff_start + sliced.height();
    assert!(
        sliced.height() < 100,
        "expected a trim below the byte budget"
    );
    assert!(
        eff_start <= state.start_row,
        "view start {} fell before kept buffer start {}",
        state.start_row,
        eff_start
    );
    assert!(
        eff_end >= state.start_row + state.visible_rows,
        "view end {} fell after kept buffer end {}",
        state.start_row + state.visible_rows,
        eff_end
    );
    assert_eq!(
        sliced.height(),
        eff_end - eff_start,
        "df height must match range"
    );
}

#[test]
fn more_columns_indicator_appears_and_tracks_scroll() {
    // 4 columns that can't all fit: a right-edge marker should signal off-screen columns,
    // and after scrolling right a left-edge marker should appear too.
    let mut state =
        DataTableState::new(create_large_test_lf(), None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.collect();

    let area = Rect::new(0, 0, 6, 4); // narrow: not all 4 columns fit
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = header_row_string(&buf, area);
    // The arrows come from the glyph set for the locale, which is ASCII on Windows CI.
    let g = crate::glyphs::get();
    assert!(
        header.contains(g.arrow_right),
        "expected right indicator, header: {header:?}"
    );
    assert!(
        !header.contains(g.arrow_left),
        "should not show left indicator at offset 0: {header:?}"
    );

    // Scroll right: now columns exist both left and right of the viewport.
    state.scroll_right();
    let mut buf2 = Buffer::empty(area);
    DataTable::default().render(area, &mut buf2, &mut state);
    let header2 = header_row_string(&buf2, area);
    assert!(
        header2.contains(g.arrow_left),
        "expected left indicator after scroll: {header2:?}"
    );
}

#[test]
fn the_header_mark_follows_the_state_sort_and_its_reverse() {
    // Through the stateful render: the marks come from the state being drawn,
    // so applying a sort shows them and `reverse` flips them, with no caller
    // wiring in between.
    let g = crate::glyphs::get();
    let mut state = DataTableState::new(create_test_lf(), None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.sort(vec!["a".to_string()], true);

    let area = Rect::new(0, 0, 20, 5);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("a{}", g.sort_asc)),
        "sorted ascending: {header:?}"
    );

    state.reverse();
    let mut buf2 = Buffer::empty(area);
    DataTable::default().render(area, &mut buf2, &mut state);
    let header2 = header_row_string(&buf2, area);
    assert!(
        header2.contains(&format!("a{}", g.sort_desc)),
        "reversed: {header2:?}"
    );
    assert!(
        !header2.contains(g.sort_asc),
        "the old direction is gone: {header2:?}"
    );
}

/// A click finds the cell drawn under it, at 80×24 and on a wide screen: each
/// column where its heading is, frozen or scrolling, and each row where its cells
/// are, with row numbers on and the view scrolled down.
#[test]
fn a_click_finds_the_cell_drawn_under_it() {
    for (width, height) in [(80, 24), (200, 50)] {
        let n = 400;
        let mut columns = vec![Column::new("id".into(), (0..n).collect::<Vec<i64>>())];
        for c in 0..30 {
            columns.push(Column::new(
                format!("col_{c:02}").as_str().into(),
                (0..n).map(|i| format!("v{i}_{c}")).collect::<Vec<_>>(),
            ));
        }
        let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_locked_columns(1);
        state.toggle_row_numbers();
        let area = Rect::new(0, 0, width, height);
        let render = |state: &mut DataTableState| {
            let mut buf = Buffer::empty(area);
            DataTable::default().render(area, &mut buf, state);
            buf
        };
        // The first frame sets how many rows fit; the rows are read for it.
        render(&mut state);
        state.collect();
        for scrolled in [false, true] {
            if scrolled {
                state.page_down();
                state.collect();
            }
            let buf = render(&mut state);
            let drawn = state.drawn.clone().expect("the table was drawn");
            let header = row_string(&buf, area, 0);
            assert!(drawn.columns.len() > 3, "{width}x{height}: {header:?}");
            assert_eq!(drawn.columns[0].2, "id", "the frozen column first");
            for (_, _, name) in &drawn.columns {
                // In cells, not bytes: the frozen separator is a wide glyph.
                let at = header.find(name.as_str()).expect("heading drawn");
                let from = header[..at].chars().count() as u16;
                for x in [from, from + name.len() as u16 - 1] {
                    let hit = state.drawn_cell(x, 0).expect("on the table");
                    assert_eq!(hit.row, None, "the header is no row");
                    assert_eq!(hit.column.as_deref(), Some(name.as_str()), "at {x}");
                }
            }
            // The rail and the row numbers are no column, but are the row.
            let y = drawn.header + 5;
            assert_eq!(
                state.drawn_cell(0, y),
                Some(CellHit {
                    row: Some(5),
                    column: None
                })
            );
            // A cell: the row and column whose value is drawn there.
            let (from, to, name) = drawn.columns[2].clone();
            let c: usize = name["col_".len()..].parse().unwrap();
            let hit = state.drawn_cell(from, y).expect("a cell");
            let row = drawn.start_row + 5;
            let text: String = (from..to).map(|x| buf[(x, y)].symbol()).collect();
            assert_eq!(text.trim(), format!("v{row}_{c}"), "{width}x{height}");
            state.point_at(&hit);
            assert_eq!(state.table_state.selected(), Some(5));
            assert_eq!(state.current_column(), Some(name.as_str()));
            // Off the table's right or bottom edge is nothing.
            assert_eq!(state.drawn_cell(width, y), None);
            assert_eq!(state.drawn_cell(0, height), None);
        }
    }
}

/// The column cursor at 80×24: its header and cells take the column tint, the
/// current cell (the cursor's row and column) the cell tint, and the rest of the
/// current row keeps the row tint. Frozen columns take it the same way.
#[test]
fn the_column_cursor_tints_its_header_and_cells() {
    let (mut state, area) = cursor_fixture();
    let row_tint = Color::Rgb(0x28, 0x34, 0x57);
    let column_tint = Color::Rgb(0x29, 0x2e, 0x42);
    let cell_tint = Color::Rgb(0x3b, 0x42, 0x61);
    let table = || {
        DataTable {
            selection_style: Style::default().bg(row_tint),
            ..DataTable::default()
        }
        .with_cursor_styles(
            crate::config::column_cursor_style(Some(column_tint)),
            crate::config::cell_cursor_style(Some(cell_tint)),
        )
    };
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    assert_eq!(state.current_column(), Some("city"));
    let mut buf = Buffer::empty(area);
    table().render(area, &mut buf, &mut state);
    let city = column_span(&buf, area, "city");
    let name = column_span(&buf, area, "name");
    for x in city.clone() {
        assert_eq!(buf[(x, 0)].bg, cell_tint, "the header, at {x}");
        assert!(buf[(x, 0)].modifier.contains(Modifier::BOLD));
        assert_eq!(buf[(x, 1)].bg, cell_tint, "the current cell, at {x}");
        for y in 2..area.height {
            assert_eq!(buf[(x, y)].bg, column_tint, "the column, at {x},{y}");
        }
    }
    for x in name.clone() {
        assert_eq!(buf[(x, 1)].bg, row_tint, "the rest of the row");
        assert_ne!(buf[(x, 0)].bg, cell_tint, "another header");
        assert_ne!(buf[(x, 2)].bg, column_tint, "another column");
    }

    // Frozen: the same marks, left of the separator.
    state.move_cursor(CursorMove::First);
    assert_eq!(state.current_column(), Some("id"));
    let mut buf = Buffer::empty(area);
    table().render(area, &mut buf, &mut state);
    let id = column_span(&buf, area, "id");
    for x in id {
        assert_eq!(buf[(x, 0)].bg, cell_tint);
        assert_eq!(buf[(x, 1)].bg, cell_tint);
        assert_eq!(buf[(x, 5)].bg, column_tint);
    }
    for x in city {
        assert_ne!(buf[(x, 0)].bg, cell_tint, "city lets go of it");
    }
}

/// Where the tints would not show (16 colors, `NO_COLOR`), the header and the
/// current cell are reversed, so the cursor is still on screen, and nothing else is.
#[test]
fn the_column_cursor_shows_without_its_tints() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    for tint in [Color::Black, Color::White, Color::Reset] {
        let mut buf = Buffer::empty(area);
        DataTable::default()
            .with_cursor_styles(
                crate::config::column_cursor_style(Some(tint)),
                crate::config::cell_cursor_style(Some(tint)),
            )
            .render(area, &mut buf, &mut state);
        let name = column_span(&buf, area, "name");
        let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
        for x in name {
            assert!(reversed(x, 0), "{tint:?}: the header");
            assert!(reversed(x, 1), "{tint:?}: the current cell");
            assert!(!reversed(x, 2), "{tint:?}: not the rest of the column");
        }
        let city = column_span(&buf, area, "city");
        assert!(!reversed(city.start, 0) && !reversed(city.start, 1));
    }
}

/// Under a reversed row, the current cell is drawn upright, tinted or not, so it
/// stands out from the row; the header keeps its own mark.
#[test]
fn the_current_cell_stands_out_of_a_reversed_row() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    for tint in [Color::Rgb(0x3b, 0x42, 0x61), Color::Black, Color::Reset] {
        let mut buf = Buffer::empty(area);
        DataTable {
            selection_style: Style::default().add_modifier(Modifier::REVERSED),
            ..DataTable::default()
        }
        .with_cursor_styles(
            crate::config::column_cursor_style(Some(tint)),
            crate::config::cell_cursor_style(Some(tint)),
        )
        .render(area, &mut buf, &mut state);
        let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
        for x in column_span(&buf, area, "name") {
            assert!(!reversed(x, 1), "{tint:?}: the current cell is upright");
            assert!(buf[(x, 1)].modifier.contains(Modifier::BOLD));
        }
        let city = column_span(&buf, area, "city");
        assert!(reversed(city.start, 1), "{tint:?}: the rest of the row");
    }
}

/// The cursor follows its column by name when the columns are reordered or
/// frozen, and a column hidden from under it hands it to the one in its place.
#[test]
fn the_column_cursor_follows_its_column_by_name() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    assert_eq!(state.current_column(), Some("city"));
    let order = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    state.set_column_order(order(&["city", "id", "name", "amount"]));
    assert_eq!(state.current_column(), Some("city"));
    assert_eq!(state.current_column_index(), Some(0));
    state.set_locked_columns(0);
    state.set_column_order(order(&["id", "name", "city", "amount"]));
    state.set_locked_columns(3);
    assert_eq!(state.current_column(), Some("city"), "frozen now");
    // Hidden: the column now in its place takes it.
    state.set_locked_columns(0);
    state.set_column_order(order(&["id", "name", "amount"]));
    assert_eq!(state.current_column(), Some("amount"));
    // Hidden at the end: the last column.
    state.set_column_order(order(&["id", "name"]));
    assert_eq!(state.current_column(), Some("name"));
    state.set_column_order(Vec::new());
    assert_eq!(state.current_column(), None);
    DataTable::default().render(area, &mut Buffer::empty(area), &mut state);
}

/// `h` `l` cross from the frozen columns to the scrolling ones and back in the
/// shown order; the view moves only when the cursor would leave it, and a page
/// puts the cursor on the new page's first column.
#[test]
fn the_column_cursor_scrolls_only_at_the_edges() {
    let n = 5;
    let names: Vec<String> = (0..40).map(|i| format!("column_{i:02}")).collect();
    let columns: Vec<Column> = names
        .iter()
        .map(|name| Column::new(name.as_str().into(), (0..n).collect::<Vec<i64>>()))
        .collect();
    let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 5;
    state.set_locked_columns(2);
    let area = Rect::new(0, 0, 80, 8);
    let draw = |state: &mut DataTableState| {
        DataTable::default().render(area, &mut Buffer::empty(area), state);
        state.columns_on_screen().unwrap()
    };
    let start = draw(&mut state);
    assert_eq!((start.first, start.cursor), (3, 1));
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    let on = draw(&mut state);
    assert_eq!((on.first, on.cursor), (3, 3), "into the scrolling side");
    // Walk to the right edge: nothing scrolls until the cursor would leave (a
    // column cut at the edge counts as leaving), then just enough.
    let mut before = on;
    let past = loop {
        state.move_cursor(CursorMove::Right);
        let now = draw(&mut state);
        assert_eq!(now.cursor, before.cursor + 1);
        if now.first != before.first {
            break now;
        }
        before = now;
    };
    assert!(past.first > 3 && past.cursor <= past.last, "{past:?}");
    assert!(past.cursor >= start.last, "not before the edge: {past:?}");
    assert!(past.first <= before.last, "no column skipped: {past:?}");
    // Back to the left edge, and one more scrolls back a column.
    for _ in past.first..past.cursor {
        state.move_cursor(CursorMove::Left);
    }
    assert_eq!(draw(&mut state).first, past.first);
    state.move_cursor(CursorMove::Left);
    assert_eq!(draw(&mut state).first, past.first - 1);
    // Pages: the cursor starts the new page; on the last, it takes the last column.
    state.move_cursor(CursorMove::PageRight);
    let page = draw(&mut state);
    assert_eq!(page.cursor, page.first);
    state.move_cursor(CursorMove::Last);
    let last = draw(&mut state);
    assert_eq!((last.cursor, last.last), (40, 40));
    state.move_cursor(CursorMove::PageRight);
    assert_eq!(draw(&mut state).cursor, 40);
    // From a frozen column, the next is the first scrolling column: the view
    // goes back to it.
    state.go_to_column("column_01");
    assert_eq!(draw(&mut state).first, last.first, "frozen: on screen");
    state.move_cursor(CursorMove::Right);
    let back = draw(&mut state);
    assert_eq!((back.first, back.cursor), (3, 3));
    // `[` on the first page: its first column, then the first of all.
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::PageLeft);
    assert_eq!(draw(&mut state).cursor, 3);
    state.move_cursor(CursorMove::PageLeft);
    assert_eq!(draw(&mut state).cursor, 1);
    state.move_cursor(CursorMove::Left);
    assert_eq!(draw(&mut state).cursor, 1, "nothing left of the first");
}

/// Every cell right of the frozen separator sits one cell off it, as the cells
/// left of it do, and the gap takes its row's tint. A right-aligned number as
/// wide as its column, a negative one most often, used to touch the line:
/// `│-9.930889` (#386).
#[test]
fn the_frozen_separator_has_a_gap_on_both_sides() {
    let lf = df!(
        "carrier" => &["AA", "UA", "9E"],
        "delay" => &[-9.930889f64, 3.5, 12.25],
        "name" => &["American", "United", "Endeavor"],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.set_locked_columns(1);
    state.table_state.select(Some(0));

    let area = Rect::new(0, 0, 40, 6);
    let mut buf = Buffer::empty(area);
    let table = DataTable {
        header_bg: Color::Indexed(238),
        alternate_row_bg: Some(Color::Indexed(236)),
        selection_style: Style::default().bg(Color::Indexed(24)),
        ..DataTable::default()
    };
    table.render(area, &mut buf, &mut state);

    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    assert!(
        rows[1].contains(&format!("{} -9.930889", crate::glyphs::get().rule)),
        "{rows:#?}"
    );
    let rule = crate::glyphs::get().rule;
    let sep = (0..area.width)
        .find(|&x| buf[(x, 0)].symbol() == rule)
        .expect("a separator");
    for y in 0..area.height {
        let row = &rows[y as usize];
        assert_eq!(buf[(sep - 1, y)].symbol(), " ", "row {y}: {row:?}");
        assert_eq!(buf[(sep + 1, y)].symbol(), " ", "row {y}: {row:?}");
        assert_eq!(
            buf[(sep + 1, y)].bg,
            buf[(sep + 2, y)].bg,
            "row {y}'s gap takes the row's tint: {row:?}"
        );
    }
    // The header, the highlight and the stripe, not only unstyled rows.
    assert_eq!(buf[(sep + 1, 0)].bg, Color::Indexed(238));
    assert_eq!(buf[(sep + 1, 1)].bg, Color::Indexed(24));
    assert_eq!(buf[(sep + 1, 2)].bg, Color::Indexed(236));
}

/// The frozen separator runs down the header and the rows, and stops under the
/// last: a grouped view of seven rows had it running down the empty screen.
#[test]
fn the_frozen_separator_stops_at_the_last_row() {
    let lf = df!(
        "carrier" => &["AA", "UA", "9E"],
        "delay" => &[-9.9f64, 3.5, 12.25],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.set_locked_columns(1);
    state.table_state.select(Some(0));
    let area = Rect::new(0, 0, 30, 10);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let rule = crate::glyphs::get().rule;
    let sep = (0..area.width)
        .find(|&x| buf[(x, 0)].symbol() == rule)
        .expect("a separator");
    let ruled: Vec<u16> = (0..area.height)
        .filter(|&y| buf[(sep, y)].symbol() == rule)
        .collect();
    let last = ruled.last().copied().unwrap();
    assert_eq!(
        ruled,
        (0..=last).collect::<Vec<_>>(),
        "unbroken to the last row"
    );
    let header = DataTable::default().header_height();
    assert_eq!(last, header + 2, "under the third row, no further");
}

/// A frozen column whose type is wider than its name and values still gets its
/// whole width and the gap before the separator. The width pass left the type row
/// out, so `id` over `i64` ran onto the line (`i64│`) and a one-letter string
/// column did not fit at all.
#[test]
fn a_frozen_column_is_as_wide_as_its_type() {
    let lf = df!(
        "id" => &[1i64, 2],
        "k" => &["x", "y"],
        "v" => &[-3.5f64, 4.25],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 2;
    state.set_locked_columns(2);
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    DataTable {
        dtype_row: true,
        ..DataTable::default()
    }
    .render(area, &mut buf, &mut state);

    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    let rule = crate::glyphs::get().rule;
    assert!(rows[0].contains(&format!(" id k   {rule}")), "{rows:#?}");
    assert!(rows[1].contains(&format!("i64 str {rule}")), "{rows:#?}");
    assert!(rows[2].contains(&format!("  1 x   {rule}")), "{rows:#?}");
}

/// A list cell reads as it did when the buffer held lists as text, and the type
/// row names the list once the rows have landed, not `str`.
#[test]
fn a_list_column_draws_its_items_under_its_list_type() {
    let mut state = list_state();
    let area = Rect::new(0, 0, 80, 4);
    let mut buf = Buffer::empty(area);
    DataTable {
        dtype_row: true,
        ..DataTable::default()
    }
    .render(area, &mut buf, &mut state);
    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    assert!(rows[1].contains("list[str]"), "{rows:#?}");
    assert!(!rows[1].contains(" str "), "{rows:#?}");
    assert!(rows[2].contains("[a, b]"), "{rows:#?}");
    // The column's width cap cuts the rest; `exact` tests the whole preview.
    assert!(
        rows[3].contains("[t0, t1, t2, t3, t4, t5, t6, t7"),
        "{rows:#?}"
    );
}

/// A page over columns not drawn yet waits for the draw, which measures them from
/// the rows on hand and lands it: `Last` ends the page with the last column whole,
/// and the one before that would not have fitted. Nothing moves before then.
#[test]
fn a_page_over_columns_not_drawn_lands_at_the_draw() {
    let names: Vec<String> = (0..30).map(|i| format!("col{i:02}")).collect();
    let columns: Vec<Column> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let value = "x".repeat(3 + i % 5);
            Series::new(name.as_str().into(), vec![value; 3]).into()
        })
        .collect();
    let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.collect();
    let area = Rect::new(0, 0, 50, 4);
    let draw = |state: &mut DataTableState| {
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, state);
        row_string(&buf, area, 0)
    };
    draw(&mut state);
    assert!(state.drawn_width("col29").is_none(), "not drawn yet");

    state.scroll_columns(ColumnMove::Last);
    assert_eq!(state.termcol_index, 0, "nothing moves before the draw");
    let header = draw(&mut state);
    assert!(header.trim_end().ends_with("col29"), "{header}");
    let start = state.termcol_index;
    assert!(start > 0);
    // The column before the page would not have fitted beside it.
    let room = state.scroll_room.unwrap();
    let used: u16 = names[start - 1..]
        .iter()
        .map(|n| state.shown_width(n).unwrap() + room.padding)
        .sum::<u16>()
        - room.padding;
    assert!(used > room.width, "{used} in {}", room.width);

    // Back, which lands at the draw again; then forward over columns drawn now,
    // which needs none.
    state.scroll_columns(ColumnMove::PageLeft);
    draw(&mut state);
    let back = state.termcol_index;
    assert!(back < start);
    state.scroll_columns(ColumnMove::PageRight);
    assert_eq!(state.termcol_index, start);
    state.scroll_columns(ColumnMove::First);
    assert_eq!(state.termcol_index, 0);
}

/// The hidden-columns count goes in the blank run after the last column, or not
/// at all: at no width does it cover the type under a heading. The separator's gap
/// took the one cell of slack that used to keep `+2 >` clear of `str` at 60
/// columns, and the count then wrote over the `r`.
#[test]
fn the_hidden_count_never_covers_a_type() {
    let lf = df!(
        "k" => &["x"],
        "origin" => &["JFK"],
        "dest" => &["LAX"],
        "tail" => &["N1"],
        "name" => &["Endeavor"],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 1;
    state.set_locked_columns(1);
    let table = || DataTable {
        dtype_row: true,
        ..DataTable::default()
    };
    assert_eq!(table().header_height(), 2, "the count goes on the type row");
    for width in 8..=40 {
        let area = Rect::new(0, 0, width, 3);
        let mut buf = Buffer::empty(area);
        table().render(area, &mut buf, &mut state);
        let names = row_string(&buf, area, 0);
        let types = row_string(&buf, area, 1);
        // Every heading shown whole has its whole type under it; these are all
        // strings, so each starts where its name does.
        for name in ["origin", "dest", "tail", "name"] {
            if let Some(at) = names.find(&format!(" {name}")) {
                let x = names[..at].chars().count() + 1;
                let under: String = types.chars().skip(x).take(3).collect();
                assert_eq!(under, "str", "width {width}:\n{names}\n{types}");
            }
        }
    }
}

/// The gap is paid for in the width budget: at every width, the columns right of
/// the separator are whole or absent. Left out, the last column that fits exactly
/// comes out one cell short, and a number cut short reads as a different number.
/// The one exception is a first column with no room for its value at all, which
/// shows a preview behind the clip marker rather than nothing.
#[test]
fn the_separator_gap_never_cuts_a_number_short() {
    let values = ["-987", "654", "-32", "10"];
    let lf = df!(
        "k" => &["x"],
        "a" => &[-987i64],
        "b" => &[654i64],
        "c" => &[-32i64],
        "d" => &[10i64],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 1;
    state.set_locked_columns(1);
    let data_row = DataTable::default().header_height();
    for width in 8..=30 {
        let area = Rect::new(0, 0, width, data_row + 1);
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let row = row_string(&buf, area, data_row);
        let g = crate::glyphs::get();
        let (_, scrolled) = row.split_once(g.rule).expect("a separator");
        for (i, token) in scrolled.split_whitespace().enumerate() {
            let previewed = i == 0 && token.ends_with(g.ellipsis);
            assert!(
                values.contains(&token) || previewed,
                "width {width}: {token:?} is cut short in {row:?}"
            );
        }
    }
}

/// A heading wider than the table is clipped, marked, over values that fit whole:
/// it never leaves the table blank, at any width, in either glyph set, with or
/// without the type row. It used to drop the column, and with it everything
/// after, leaving the rail and an off-screen hint over nothing.
#[test]
fn a_long_header_never_blanks_the_table() {
    let name = format!("numeric_header_{}", "x".repeat(90));
    let df = DataFrame::new_infer_height(vec![
        Series::new(name.as_str().into(), &[1i64, 22, 333]).into(),
        Series::new("tail".into(), &["t1", "t2", "t3"]).into(),
    ])
    .unwrap();
    for g in glyph_sets() {
        for dtype_row in [false, true] {
            for width in [12u16, 20, 60, 80, 120] {
                let table = || DataTable {
                    glyphs: g,
                    dtype_row,
                    ..DataTable::default()
                };
                let header_h = usize::from(table().header_height());
                let mut state = state_of(&df, 3);
                let rows = draw(table(), &mut state, width, header_h as u16 + 3);
                let ctx = format!(
                    "{} glyphs, type row {dtype_row}, width {width}:\n{}",
                    set_name(g),
                    rows.join("\n")
                );
                assert!(rows[0].contains("num"), "{ctx}");
                for (i, value) in ["1", "22", "333"].iter().enumerate() {
                    assert!(
                        rows[header_h + i].split_whitespace().any(|t| t == *value),
                        "{value} is whole on its row: {ctx}"
                    );
                }
                if dtype_row && width >= 20 {
                    assert!(rows[1].contains("i64"), "{ctx}");
                }
                // Wider than any cap on automatic widths: always clipped, and from
                // 60 columns on, with the column after it beside it.
                assert!(rows[0].contains(g.ellipsis), "{ctx}");
                if width >= 60 {
                    assert!(rows[0].contains("tail"), "{ctx}");
                }
            }
        }
    }
}

/// A clipped heading gives way to its marks: the name is cut, never the sort
/// direction, which is state.
#[test]
fn a_clipped_heading_keeps_its_sort_mark() {
    let name = format!("numeric_header_{}", "x".repeat(90));
    let df = DataFrame::new_infer_height(vec![Series::new(name.as_str().into(), &[1i64]).into()])
        .unwrap();
    for g in glyph_sets() {
        let table = DataTable {
            glyphs: g,
            ..DataTable::default()
        }
        .with_sort(vec![name.clone()], vec![true]);
        let area = Rect::new(0, 0, 30, 2);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false);
        let header = header_row_string(&buf, area);
        assert!(
            header
                .trim_end()
                .ends_with(&format!("{}{}", g.ellipsis, g.sort_desc)),
            "{}: {header:?}",
            set_name(g)
        );
    }
}

/// A heading whose drawn width is not its string's width (`لا` draws two cells,
/// a halfwidth sound mark one) is drawn whole over right-aligned numbers, with its
/// sort mark after it. ratatui's own alignment pushed the last letter off the cell,
/// and the mark landed on it.
#[test]
fn a_heading_is_placed_by_the_cells_it_draws() {
    for name in ["الاسم", "ｶﾞｷﾞ"] {
        let df = DataFrame::new_infer_height(vec![
            Series::new(name.into(), &[1i64]).into(),
            Series::new("tail".into(), &["x"]).into(),
        ])
        .unwrap();
        let table = DataTable::default().with_sort(vec![name.to_string()], vec![false]);
        let mark = table.glyphs.sort_asc;
        let area = Rect::new(0, 0, 30, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false);
        let rows: Vec<String> = (0..3).map(|y| drawn_from(&buf, y, 0)).collect();
        assert!(rows[0].starts_with(&format!("{name}{mark}")), "{rows:#?}");
        let width = crate::glyphs::cell_width(name) + 1;
        assert!(
            rows[1].starts_with(&format!("{:>width$} x", "1")),
            "{rows:#?}"
        );
    }
}

/// A number too wide for the whole table shows its leading digits behind the clip
/// marker: something rather than nothing, and never its trailing digits passing
/// for a whole number, which is what ratatui's own cut of a right-aligned value
/// would show.
#[test]
fn a_number_wider_than_the_table_is_a_marked_preview() {
    let full = "-1234567890123456789";
    let df = df!("n" => &[-1234567890123456789i64]).unwrap();
    for g in glyph_sets() {
        for width in 3u16..=24 {
            let mut state = state_of(&df, 1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 2);
            let shown: String = rows[1].chars().skip(1).collect::<String>();
            let shown = shown.trim();
            let ctx = format!("{} glyphs, width {width}: {rows:?}", set_name(g));
            if usize::from(width) > full.len() {
                assert_eq!(shown, full, "{ctx}");
            } else if g.ellipsis.starts_with(shown) {
                // Room for no more than the marker.
                assert!(!shown.is_empty(), "{ctx}");
            } else {
                let kept = shown
                    .strip_suffix(g.ellipsis)
                    .unwrap_or_else(|| panic!("a clipped number carries the marker: {ctx}"));
                assert!(full.starts_with(kept), "{ctx}");
            }
        }
    }
}

/// Wide characters are measured in cells: a column of four CJK characters is
/// eight cells wide, and shows whole beside the columns after it while there is
/// room. Counted in characters, it was given four and clipped with space to spare.
#[test]
fn wide_characters_are_measured_in_cells() {
    let values = ["東京大阪", "京都横浜", "名古屋市"];
    let df = df!(
        "a" => &values,
        "b" => &[1i64, 2, 3],
        "tail" => &["x", "y", "z"],
    )
    .unwrap();
    for g in glyph_sets() {
        for width in [16u16, 30, 80] {
            let mut state = state_of(&df, 3);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 4);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert!(rows[0].contains("tail"), "{ctx}");
            for (i, value) in values.iter().enumerate() {
                assert!(rows[1 + i].contains(value), "{ctx}");
            }
        }
    }
}

/// A value cut where it meets the edge keeps whole graphemes and gains the clip
/// marker: no half of a wide character, no accent split from its letter, no
/// joined emoji broken apart, at any width, in either glyph set.
#[test]
fn a_clipped_cell_keeps_whole_graphemes() {
    let values = [
        "東京大阪名古屋横浜",
        "e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}",
        "👩\u{200d}👩\u{200d}👧👍🏽🇯🇵 and more",
        "plain text that runs on",
    ];
    let df = df!("id" => &[1i64, 2, 3, 4], "text" => &values).unwrap();
    for g in glyph_sets() {
        for width in 4u16..=30 {
            let mut state = state_of(&df, 4);
            let area = Rect::new(0, 0, width, 5);
            let mut buf = Buffer::empty(area);
            DataTable {
                glyphs: g,
                ..DataTable::default()
            }
            .render(area, &mut buf, &mut state);
            // The rail, `id` two cells wide, and one cell of padding.
            let text_x = 4;
            if text_x >= width {
                continue;
            }
            for (i, value) in values.iter().enumerate() {
                let y = 1 + i as u16;
                let shown = drawn_from(&buf, y, text_x);
                let shown = shown.trim_end();
                let ctx = format!("{} glyphs, width {width}, row {i}: {shown:?}", set_name(g));
                if shown.is_empty() || shown == *value {
                    continue;
                }
                let kept = shown
                    .strip_suffix(g.ellipsis)
                    .unwrap_or_else(|| panic!("a clipped value is marked: {ctx}"));
                let span = Span::raw(*value);
                let mut whole = String::new();
                for grapheme in span.styled_graphemes(Style::default()) {
                    if whole.len() >= kept.len() {
                        break;
                    }
                    whole.push_str(grapheme.symbol);
                }
                assert_eq!(whole, kept, "{ctx}");
                // And each cell holds a whole grapheme of the value or the marker.
                for x in text_x..width {
                    let symbol = buf[(x, y)].symbol();
                    assert!(
                        symbol == " "
                            || g.ellipsis.contains(symbol)
                            || span
                                .styled_graphemes(Style::default())
                                .any(|gr| gr.symbol == symbol),
                        "cell {x} holds {symbol:?}: {ctx}"
                    );
                }
            }
        }
    }
}

/// A frozen text column wider than the window is clipped, marked and still
/// frozen, with the column after it beside it. It used to vanish, leaving only
/// the column after it.
#[test]
fn a_frozen_long_text_is_clipped_not_dropped() {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let df = df!("url" => &[url.as_str(), "short"], "tail" => &[7i64, 8]).unwrap();
    for g in glyph_sets() {
        for width in [40u16, 60, 80, 120] {
            let mut state = state_of(&df, 2);
            state.set_locked_columns(1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 3);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            let (frozen, scrolled) = rows[0].split_once(g.rule).expect(&ctx);
            assert!(frozen.contains("url"), "{ctx}");
            assert!(scrolled.contains("tail"), "{ctx}");
            let (frozen, scrolled) = rows[1].split_once(g.rule).expect(&ctx);
            assert!(frozen.contains("https://exa"), "{ctx}");
            assert!(frozen.trim_end().ends_with(g.ellipsis), "{ctx}");
            assert!(scrolled.split_whitespace().any(|t| t == "7"), "{ctx}");
            assert_eq!(state.frozen_shown(), 1, "{ctx}");
        }
    }
}

/// The frozen columns are measured on the rows on screen, like the rest. They
/// were measured on the head of the buffer, which is not the page on screen once
/// the view has moved into it, so a longer value on screen was cut short.
#[test]
fn frozen_columns_are_measured_on_the_rows_on_screen() {
    let long = "a much longer frozen value";
    let names: Vec<String> = (0..400)
        .map(|i| {
            if i == 302 {
                long.to_string()
            } else {
                format!("n{i}")
            }
        })
        .collect();
    let df = df!("name" => names, "v" => (0..400i64).collect::<Vec<_>>()).unwrap();
    let mut state = state_of(&df, 5);
    state.set_locked_columns(1);
    state.scroll_to(300);
    state.collect();
    assert!(
        state.buffered_start_row < state.start_row,
        "the page is not the head of the buffer: {} vs {}",
        state.buffered_start_row,
        state.start_row
    );
    let rows = draw(DataTable::default(), &mut state, 80, 6);
    assert!(
        rows.iter().any(|row| row.contains(long)),
        "the value on screen is whole: {rows:#?}"
    );
}

/// Frozen columns are spaced like scrolling ones: the configured padding on both
/// sides of the separator, at every setting. The frozen side used a hardcoded
/// single space.
#[test]
fn frozen_and_scrolling_columns_share_the_padding() {
    let df = df!(
        "id" => &[1i64, 2],
        "k" => &["x", "y"],
        "v" => &[3i64, 4],
        "w" => &[5i64, 6],
    )
    .unwrap();
    for padding in [0u16, 1, 2, 3] {
        let mut state = state_of(&df, 2);
        state.set_locked_columns(2);
        let table = DataTable {
            table_cell_padding: padding,
            ..DataTable::default()
        };
        let rows = draw(table, &mut state, 40, 3);
        let gap = " ".repeat(usize::from(padding));
        let rule = crate::glyphs::get().rule;
        assert!(
            rows[0].starts_with(&format!(" id{gap}k {rule} v{gap}w")),
            "padding {padding}: {rows:#?}"
        );
        assert!(
            rows[1].contains(&format!(" 1{gap}x {rule} 3{gap}5")),
            "padding {padding}: {rows:#?}"
        );
    }
}

/// Frozen columns that cannot all fit beside a usable scrolling column: as many
/// as fit stay frozen, the broken rule says some had to scroll, those lead the
/// scrolling side, every column stays reachable, and the request stands, so a
/// wider window freezes them all again.
#[test]
fn a_frozen_prefix_too_wide_scrolls_until_there_is_room() {
    let df = phonetic_frame();
    for g in glyph_sets() {
        for row_numbers in [false, true] {
            let mut state = state_of(&df, 2);
            state.row_numbers = row_numbers;
            state.set_locked_columns(4);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table(), &mut state, 60, 3);
            let ctx = format!(
                "{} glyphs, row numbers {row_numbers}: {rows:#?}",
                set_name(g)
            );
            assert_eq!(state.locked_columns_count(), 4, "{ctx}");
            let shown = state.frozen_shown();
            assert!((1..4).contains(&shown), "{shown} frozen: {ctx}");
            let (frozen, scrolled) = rows[0].split_once(g.rule_broken).expect(&ctx);
            assert!(!rows[0].contains(g.rule), "{ctx}");
            assert!(frozen.contains(PHONETIC[0]), "{ctx}");
            assert!(
                scrolled.trim_start().starts_with(PHONETIC[shown]),
                "the first column left out leads the scrolling side: {ctx}"
            );

            let reached = names_reached(&mut state, g, 60);
            assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}: {ctx}");

            let rows = draw(table(), &mut state, 160, 3);
            assert_eq!(state.frozen_shown(), 4, "{rows:#?}");
            let (frozen, _) = rows[0].split_once(g.rule).expect("the plain rule");
            for name in &PHONETIC[..4] {
                assert!(frozen.contains(name), "{rows:#?}");
            }
        }
    }
}

/// A rollback puts back the frozen fit its columns were sliced for: a wider
/// layout since then must re-slice them, or the columns that had to scroll show
/// twice, frozen and scrolling.
#[test]
fn a_rollback_keeps_the_frozen_fit_its_columns_were_sliced_for() {
    let df = phonetic_frame();
    let mut state = state_of(&df, 2);
    state.set_locked_columns(4);
    draw(DataTable::default(), &mut state, 60, 3);
    assert!(state.frozen_shown() < 4);
    let saved = state.rollback_point();
    draw(DataTable::default(), &mut state, 200, 3);
    assert_eq!(state.frozen_shown(), 4);
    state.roll_back(saved);
    let rows = draw(DataTable::default(), &mut state, 200, 3);
    for name in PHONETIC {
        assert_eq!(rows[0].matches(name).count(), 1, "{name}: {rows:#?}");
    }
}

/// With every column frozen there is nothing to scroll, until the window is too
/// narrow for them all: then the ones that do not fit scroll, and each is still
/// reachable.
#[test]
fn with_every_column_frozen_each_is_still_reachable() {
    let df = phonetic_frame();
    for g in glyph_sets() {
        let mut state = state_of(&df, 2);
        state.set_locked_columns(PHONETIC.len());
        let table = || DataTable {
            glyphs: g,
            ..DataTable::default()
        };
        let rows = draw(table(), &mut state, 200, 3);
        assert_eq!(state.frozen_shown(), PHONETIC.len(), "{rows:#?}");
        assert!(rows[0].contains(g.rule), "{rows:#?}");
        assert!(!rows[0].contains(g.arrow_right), "{rows:#?}");

        let rows = draw(table(), &mut state, 60, 3);
        assert!(state.frozen_shown() < PHONETIC.len(), "{rows:#?}");
        assert!(rows[0].contains(g.rule_broken), "{rows:#?}");
        let reached = names_reached(&mut state, g, 60);
        assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}");
    }
}

/// The page from the issue's reproduction: one description runs to a long URL.
/// That page still shows its rows, with the description clipped and marked
/// rather than any column vanishing for it.
#[test]
fn a_page_with_one_long_value_is_not_blank() {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let n = 80usize;
    let df = df!(
        "id" => (0..n as i64).collect::<Vec<_>>(),
        "description" => (0..n)
            .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
            .collect::<Vec<_>>(),
        "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
        "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
    )
    .unwrap();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            state.page_down();
            state.collect();
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 21);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert!(rows[0].contains("id"), "{ctx}");
            assert!(rows[0].contains("desc"), "{ctx}");
            let long = rows
                .iter()
                .find(|row| row.contains("https://example.com/"))
                .expect(&ctx);
            assert!(long.contains(g.ellipsis), "{ctx}");
            for row in &rows[1..] {
                assert!(!row.trim().is_empty(), "{ctx}");
            }
        }
    }
}

/// Paging down past a page with one long value, and back, moves no column: the
/// heading row is the same on every page, at every width, in both glyph sets.
/// Measured per page, the long URL turned a six-column view into a two-column one.
/// On its page the URL is clipped and marked.
#[test]
fn widths_hold_still_across_pages() {
    let df = long_url_frame();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let first = draw(table(), &mut state, width, 21);
            let ctx = |rows: &[String]| format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            if width >= 80 {
                assert!(first[0].contains("timestamp"), "{}", ctx(&first));
            }
            let mut saw_url = false;
            for step in 0..6 {
                if step < 3 {
                    state.page_down();
                } else {
                    state.page_up();
                }
                state.collect();
                let rows = draw(table(), &mut state, width, 21);
                assert_eq!(rows[0], first[0], "page {step}: {}", ctx(&rows));
                if let Some(row) = rows.iter().find(|r| r.contains("https://")) {
                    saw_url = true;
                    let clipped = row.split_whitespace().nth(1).unwrap();
                    assert!(clipped.ends_with(g.ellipsis), "{}", ctx(&rows));
                }
            }
            assert!(saw_url, "the long URL's page was drawn");
        }
    }
}

/// A frozen column keeps its width across pages, so the number of columns that
/// stay frozen does not change with the page either.
#[test]
fn frozen_columns_hold_still_across_pages() {
    let df = long_url_frame();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            state.set_locked_columns(3);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let first = draw(table(), &mut state, width, 21);
            let frozen = state.frozen_shown();
            for _ in 0..3 {
                state.page_down();
                state.collect();
                let rows = draw(table(), &mut state, width, 21);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                assert_eq!(state.frozen_shown(), frozen, "{ctx}");
                assert_eq!(rows[0], first[0], "{ctx}");
            }
        }
    }
}

/// A number's column widens for a wider number on a later page, so it is never
/// cut, and does not narrow again on a page of narrower ones.
#[test]
fn a_number_column_widens_and_stays_wide() {
    let values: Vec<i64> = (0..60)
        .map(|i| if i == 30 { 123_456_789 } else { i })
        .collect();
    let df =
        df!("n" => values, "t" => (0..60).map(|i| format!("t{i}")).collect::<Vec<_>>()).unwrap();
    let mut state = state_of(&df, 20);
    draw(DataTable::default(), &mut state, 60, 21);
    assert_eq!(state.shown_width("n"), Some(2));
    state.page_down();
    state.collect();
    let rows = draw(DataTable::default(), &mut state, 60, 21);
    assert!(rows.iter().any(|r| r.contains("123456789")), "{rows:#?}");
    state.page_down();
    state.collect();
    draw(DataTable::default(), &mut state, 60, 21);
    assert_eq!(state.shown_width("n"), Some(9));
}

/// A struct is a preview like text: one long value on a later page is clipped
/// at the cap, and the columns after it stay on screen on the pages after.
#[test]
fn a_long_struct_value_does_not_widen_its_column_for_good() {
    let n = 60usize;
    let y: Vec<String> = (0..n)
        .map(|i| {
            if i == 30 {
                "a very long struct value ".repeat(6)
            } else {
                "short".to_string()
            }
        })
        .collect();
    let s = StructChunked::from_series(
        "s".into(),
        n,
        [
            Series::new("x".into(), (0..n as i64).collect::<Vec<_>>()),
            Series::new("y".into(), y),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), (0..n as i64).collect::<Vec<_>>()).into(),
        s.into(),
        Series::new("tail".into(), (0..n as i64).collect::<Vec<_>>()).into(),
    ])
    .unwrap();
    let mut state = state_of(&df, 20);
    for page in 0..3 {
        let rows = draw(DataTable::default(), &mut state, 80, 21);
        assert!(rows[0].contains("tail"), "page {page}: {rows:#?}");
        assert!(
            state.shown_width("s").is_some_and(|w| w <= 32),
            "page {page}: {rows:#?}"
        );
        if page == 1 {
            let long = rows.iter().find(|r| r.contains("{30,")).unwrap();
            assert!(long.contains(crate::glyphs::get().ellipsis), "{rows:#?}");
        }
        state.page_down();
        state.collect();
    }
}

/// Automatic text and headings stop at two fifths of the terminal, marked where
/// cut: 32 cells at 80 columns, 48 at 120.
#[test]
fn automatic_text_stops_at_the_cap_and_is_marked() {
    let long = "x".repeat(200);
    let df = df!("text" => &[long.as_str()], "tail" => &[1i64]).unwrap();
    for g in glyph_sets() {
        for (width, cap) in [(80u16, 32u16), (120, 48)] {
            let mut state = state_of(&df, 1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 2);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert_eq!(state.shown_width("text"), Some(cap), "{ctx}");
            let row: String = rows[1].chars().skip(1).collect();
            let value = row.split_whitespace().next().unwrap();
            assert!(value.ends_with(g.ellipsis), "{ctx}");
            assert_eq!(crate::glyphs::cell_width(value), usize::from(cap), "{ctx}");
            assert!(rows[0].contains("tail"), "{ctx}");
        }
    }
}

/// The last column drawn takes the room to the table's right edge: a long text on
/// the far right runs to the edge rather than stopping at the cap. A width set by
/// hand is drawn as set, and a number stays under its heading.
#[test]
fn the_last_column_runs_to_the_right_edge() {
    let long = "x".repeat(200);
    let df = df!("id" => &[1i64], "text" => &[long.as_str()]).unwrap();
    let ellipsis = crate::glyphs::get().ellipsis;
    let mut state = state_of(&df, 1);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    let value = rows[1].trim_end();
    assert!(value.ends_with(ellipsis), "{rows:#?}");
    assert_eq!(crate::glyphs::cell_width(value), 120, "{rows:#?}");
    assert_eq!(state.shown_width("text"), Some(48), "learned at the cap");
    assert!(state.on_screen_width("text").unwrap() > 48, "drawn past it");

    state.set_width_choices([("text".to_string(), WidthChoice::Manual(20))]);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    assert_eq!(state.shown_width("text"), Some(20), "{rows:#?}");
    assert!(
        crate::glyphs::cell_width(rows[1].trim_end()) < 40,
        "{rows:#?}"
    );

    let df = df!("text" => &["ab"], "n" => &[5i64]).unwrap();
    let mut state = state_of(&df, 1);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    assert!(
        crate::glyphs::cell_width(rows[1].trim_end()) < 20,
        "{rows:#?}"
    );
}

/// The cap follows the terminal, not the table: a sidebar narrowing the table
/// moves no column.
#[test]
fn a_sidebar_moves_no_column() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.page_down();
    state.collect();
    let table = || DataTable {
        screen_width: 120,
        ..DataTable::default()
    };
    let whole = draw(table(), &mut state, 120, 21);
    let widths: Vec<_> = ["id", "description", "amount"]
        .iter()
        .map(|c| state.shown_width(c))
        .collect();
    let beside = draw(table(), &mut state, 70, 21);
    for (c, before) in ["id", "description", "amount"].iter().zip(&widths) {
        assert_eq!(state.shown_width(c), *before, "{c}: {beside:#?}");
    }
    assert!(
        whole[0].starts_with(&beside[0][..40]),
        "{whole:#?} {beside:#?}"
    );
}

/// A width set by hand: exact for text, values clipped and marked; it survives
/// paging, scrolling, reordering, hiding and resizing; fit and reset change it.
#[test]
fn a_manual_width_survives_everything_but_fit_and_reset() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.set_width_choices([("description".to_string(), WidthChoice::Manual(6))]);
    let rows = draw(DataTable::default(), &mut state, 80, 21);
    assert_eq!(state.shown_width("description"), Some(6), "{rows:#?}");
    // Six cells, the ellipsis among them: `item …`, or `ite...` in ASCII.
    let ellipsis = crate::glyphs::get().ellipsis;
    let kept = 6 - crate::glyphs::display_width(ellipsis);
    let clipped = format!("{}{ellipsis}", &"item 10"[..kept]);
    assert!(rows.iter().any(|r| r.contains(&clipped)), "{rows:#?}");

    state.page_down();
    state.collect();
    state.scroll_right();
    draw(DataTable::default(), &mut state, 80, 21);
    state.scroll_left();
    // Moved to the end, then hidden and shown again.
    let mut order = state.headers();
    order.retain(|c| c != "description");
    order.push("description".to_string());
    state.set_column_order(order.clone());
    draw(DataTable::default(), &mut state, 200, 21);
    assert_eq!(state.shown_width("description"), Some(6));
    order.pop();
    state.set_column_order(order.clone());
    draw(DataTable::default(), &mut state, 40, 21);
    order.insert(1, "description".to_string());
    state.set_column_order(order);
    draw(DataTable::default(), &mut state, 120, 21);
    assert_eq!(state.width_choice("description"), WidthChoice::Manual(6));
    assert_eq!(state.shown_width("description"), Some(6));

    // Fit takes the rows on screen: page two holds the URL.
    state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
    draw(DataTable::default(), &mut state, 120, 21);
    let url_width = 20 + 13 * 15;
    assert_eq!(
        state.width_choice("description"),
        WidthChoice::Manual(url_width)
    );
    state.reset();
    assert_eq!(state.width_choice("description"), WidthChoice::Auto);
}

/// A column scrolled out of view is fitted to the rows on screen too.
#[test]
fn a_column_out_of_view_is_fitted_to_the_page() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.page_down();
    state.collect();
    for _ in 0..3 {
        state.scroll_right();
    }
    draw(DataTable::default(), &mut state, 80, 21);
    state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
    let rows = draw(DataTable::default(), &mut state, 80, 21);
    assert!(!rows[0].contains("description"), "{rows:#?}");
    assert_eq!(
        state.width_choice("description"),
        WidthChoice::Manual(20 + 13 * 15)
    );
}

/// A number column set narrower than its numbers still shows them whole.
#[test]
fn a_number_column_set_narrow_still_shows_whole_numbers() {
    let df = df!("n" => &[1_234_567i64, 2], "t" => &["a", "b"]).unwrap();
    let mut state = state_of(&df, 2);
    state.set_width_choices([("n".to_string(), WidthChoice::Manual(4))]);
    let rows = draw(DataTable::default(), &mut state, 40, 3);
    assert!(rows[1].contains("1234567"), "{rows:#?}");
}

/// A column ending at the table's edge with more after it leaves its heading's
/// last cell to the off-screen hint, so the hint covers neither a letter nor the
/// clip marker, and the values keep the full width: a number that fits is whole,
/// never a preview.
#[test]
fn the_offscreen_hint_takes_a_heading_cell_not_a_value_cell() {
    let df = df!(
        "aaaaaaaaaa" => &["aaaaaaaaaa"],
        "bbbbbbb" => &[1_234_567i64],
        "c" => &["x"],
    )
    .unwrap();
    for g in glyph_sets() {
        for dtype_row in [false, true] {
            let table = DataTable {
                glyphs: g,
                dtype_row,
                ..DataTable::default()
            };
            let header_h = usize::from(table.header_height());
            let mut state = state_of(&df, 1);
            // The rail, ten cells, a gap and seven: the second column ends at the edge.
            let rows = draw(table, &mut state, 19, header_h as u16 + 1);
            let ctx = format!("{} glyphs, type row {dtype_row}: {rows:#?}", set_name(g));
            assert!(rows[header_h].ends_with(" 1234567"), "{ctx}");
            let hint_row = &rows[header_h - 1];
            assert!(hint_row.ends_with(g.arrow_right), "{ctx}");
            let before = hint_row.strip_suffix(g.arrow_right).unwrap();
            if dtype_row {
                assert!(before.trim_end().ends_with("i64"), "{ctx}");
            } else {
                assert!(before.ends_with(g.ellipsis), "{ctx}");
            }
        }
    }
}

use crate::*;
use polars::prelude::IntoLazy;

fn rows_at(app: &mut App, width: u16, height: u16) -> Vec<String> {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..height)
        .map(|y| (0..width).map(|x| buf[(x, y)].symbol()).collect())
        .collect()
}

fn press(app: &mut App, code: KeyCode) {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
}

/// Twelve short fields and a long URL, the inspector open on the URL of row 2.
fn inspecting(width: u16, height: u16) -> Vec<String> {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let mut columns: Vec<polars::prelude::Column> = (0..12)
        .map(|i| polars::prelude::Column::new(format!("column_{i}").into(), ["v1", "v2"]))
        .collect();
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    columns.push(polars::prelude::Column::new(
        "note".into(),
        ["x".to_string(), url],
    ));
    let df = polars::prelude::DataFrame::new(2, columns).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    // Read the rows here, as the app's first collect would.
    state.set_column_order(state.headers());
    app.data_table_state = Some(state);
    rows_at(&mut app, width, height);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::End);
    rows_at(&mut app, width, height)
}

#[test]
fn one_frame_a_titled_list_the_value_and_the_way_out() {
    let g = crate::glyphs::get();
    for (width, height) in [(80, 24), (60, 20), (120, 30)] {
        let rows = inspecting(width, height);
        let text = rows.join("\n");
        assert!(rows[0].contains("Row 2"), "{width}x{height}:\n{text}");
        assert!(rows[1].contains("Fields"), "{width}x{height}:\n{text}");
        // One frame: no corner inside it.
        for row in &rows[1..rows.len() - 2] {
            let inside: String = row.chars().skip(1).take(row.chars().count() - 2).collect();
            assert!(
                !inside.contains(g.border.top_left) && !inside.contains(g.border.bottom_left),
                "{width}x{height}: a border inside the surface:\n{text}"
            );
        }
        // The focused field's rule, and its whole value wrapped under it.
        assert!(text.contains("note  str"), "{width}x{height}:\n{text}");
        assert!(
            text.contains("https://example.com/long-segment/"),
            "{width}x{height}:\n{text}"
        );
        let tail = rows
            .iter()
            .filter(|r| r.contains("long-segment") || r.contains("segment/"))
            .count();
        assert!(tail >= 3, "wrapped over lines: {width}x{height}:\n{text}");
        // The way out stays, on the footer.
        let footer = &rows[rows.len() - 3];
        assert!(
            footer.contains("Esc") && footer.contains("Close"),
            "{width}x{height}:\n{text}"
        );
        // The list scrolls to the focused field and the rule counts them all.
        assert!(rows[1].contains(" 13 "), "{width}x{height}:\n{text}");
    }
}

/// #548: the title counts the rows; the thirteen fields are all listed where
/// they fit beside the value (D11); where they do not, the value keeps the rows
/// its lines need, up to half, and the list the rest.
#[test]
fn the_list_takes_what_the_value_does_not_need() {
    for (width, height, whole) in [(80, 24, true), (120, 30, true), (60, 20, false)] {
        let rows = inspecting(width, height);
        let text = rows.join("\n");
        assert!(rows[0].contains("Row 2 of 2"), "{width}x{height}:\n{text}");
        let listed = rows
            .iter()
            .filter(|r| r.contains(" column_") && r.contains(" str "))
            .count();
        if whole {
            assert_eq!(listed, 12, "{width}x{height}:\n{text}");
            assert!(!text.contains(" more"), "{width}x{height}:\n{text}");
        } else {
            // The value's five rows whole, the list the rest (less the blank row
            // above the footer), its top counted.
            assert_eq!(listed, 6, "{width}x{height}:\n{text}");
            assert!(text.contains("6 above"), "{width}x{height}:\n{text}");
        }
        // The value keeps its lines under the list, and its rule follows the list.
        let rule = rows.iter().position(|r| r.contains("note  str")).unwrap();
        assert!(rows.len() - 3 - rule > 3, "{width}x{height}:\n{text}");
    }
}

/// #661: a resize while reading a value keeps the place in it, at every size
/// and across the switch between the stacked and the side-by-side layouts.
#[test]
fn a_resize_keeps_the_place_in_the_value() {
    use polars::prelude::{IntoLazy, df};
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let text: String = (1..=500).map(|i| format!("line {i}\n")).collect();
    let df = df!("id" => [1i64], "text" => [text]).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.set_column_order(state.headers());
    app.data_table_state = Some(state);
    rows_at(&mut app, 100, 30);
    // A frame after each key, as the event loop draws.
    for code in [KeyCode::Char(' '), KeyCode::Down, KeyCode::Tab]
        .into_iter()
        .chain([KeyCode::PageDown; 3])
    {
        press(&mut app, code);
        rows_at(&mut app, 100, 30);
    }
    let first_line = |rows: &[String]| {
        let text = rows.join("\n");
        // The position comes after the facts, which count `501 lines` too.
        let at = text.rfind(" lines ").expect(&text) + " lines ".len();
        text[at..]
            .split('-')
            .next()
            .unwrap()
            .parse::<usize>()
            .expect(&text)
    };
    let before = first_line(&rows_at(&mut app, 100, 30));
    assert!(before > 40, "{before}");
    for (width, height) in [(160, 40), (80, 24), (300, 80), (100, 30)] {
        let rows = rows_at(&mut app, width, height);
        assert_eq!(
            first_line(&rows),
            before,
            "{width}x{height}:\n{}",
            rows.join("\n")
        );
    }
}

/// An object's keys past the thousand measured for the name column are whole
/// when the page shows them, not cut to the width of `k999`.
#[test]
fn keys_past_the_measured_ones_are_not_cut() {
    use polars::prelude::{IntoLazy, df};
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let keys: Vec<String> = (0..2000).map(|i| format!("\"k{i}\": {i}")).collect();
    let df = df!("doc" => [format!("{{{}}}", keys.join(", "))]).unwrap();
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.set_column_order(vec!["doc".to_string()]);
    app.data_table_state = Some(state);
    rows_at(&mut app, 80, 24);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::End);
    let rows = rows_at(&mut app, 80, 24);
    let rail = crate::glyphs::get().rail;
    assert!(
        rows.iter().any(|r| r.contains(&format!("{rail}k1999 "))),
        "{}",
        rows.join("\n")
    );
}

/// #615: a row whose `order` is a struct holding a list of structs, drilled
/// into twice: the title is the breadcrumb, the list is a table of its fields,
/// and one frame holds it all, the way back on its footer and the footer.
#[test]
fn a_drill_titles_its_trail_and_tables_a_list_of_structs() {
    use polars::prelude::{DataFrame, IntoColumn, IntoSeries, NamedFrom, Series, df};
    let g = crate::glyphs::get();
    for (width, height) in [(80, 24), (60, 20), (120, 30)] {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let item = df!("sku" => ["A1", "B7", "C3"], "qty" => [2i64, 1, 5])
            .unwrap()
            .into_struct("".into())
            .into_series();
        let lines = Series::new("lines".into(), [item]);
        let order = DataFrame::new(
            1,
            vec![
                polars::prelude::Column::new("id".into(), [7i64]),
                lines.into_column(),
            ],
        )
        .unwrap()
        .into_struct("order".into())
        .into_series();
        let df = DataFrame::new(1, vec![order.into_column()]).unwrap();
        let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
        state.set_column_order(state.headers());
        app.data_table_state = Some(state);
        rows_at(&mut app, width, height);
        press(&mut app, KeyCode::Char(' '));
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Down);
        press(&mut app, KeyCode::Enter);
        let rows = rows_at(&mut app, width, height);
        let text = rows.join("\n");
        assert!(
            rows[0].contains(&format!("Row 1 of 1 {} order {} lines", g.trail, g.trail)),
            "{width}x{height}:\n{text}"
        );
        assert!(rows[1].contains("Items"), "{width}x{height}:\n{text}");
        assert!(
            rows[2].contains("sku") && rows[2].contains("qty"),
            "the table's header: {width}x{height}:\n{text}"
        );
        assert!(
            rows[3].contains(g.rail) && rows[3].contains("[0]") && rows[3].contains("A1"),
            "{width}x{height}:\n{text}"
        );
        for row in &rows[1..rows.len() - 2] {
            let inside: String = row.chars().skip(1).take(row.chars().count() - 2).collect();
            assert!(
                !inside.contains(g.border.top_left) && !inside.contains(g.border.bottom_left),
                "{width}x{height}: a border inside the surface:\n{text}"
            );
        }
        let footer = &rows[rows.len() - 3];
        assert!(
            footer.contains("Enter") && footer.contains("Open"),
            "{width}x{height}:\n{text}"
        );
        assert!(
            footer.contains("Esc") && footer.contains("Back"),
            "{width}x{height}:\n{text}"
        );
        // Esc climbs to the row, then closes.
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Esc);
        assert!(app.inspector_modal.drill.is_none());
        assert_eq!(app.input_mode, InputMode::Inspect);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.input_mode, InputMode::Normal);
    }
}

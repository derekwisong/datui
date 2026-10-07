//! The inspector and the info panel.

use super::*;

/// #615: in the row inspector, Enter drills into a struct, a list of structs (shown
/// as a table) and JSON held as text, long text parsed on a worker. The title is the
/// breadcrumb; `→` opens and `←`/Esc climb back to the row, where Esc closes. `y`
/// copies the focused item, a JSON object as indented JSON.
#[test]
fn test_inspector_drills_into_nested_values_and_json_text() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let dir = common::fixture_dir().join("inspector_nested_615");
    let address = df!("street" => ["1 Main St", "2 Elm St"], "city" => ["Boston", "Austin"])
        .unwrap()
        .into_struct("address".into())
        .into_series();
    let customer = DataFrame::new(
        2,
        vec![
            Column::new("name".into(), ["ann", "bob"]),
            address.into_column(),
        ],
    )
    .unwrap()
    .into_struct("customer".into())
    .into_series();
    let item = df!("sku" => ["A1", "B7"], "qty" => [2i64, 1])
        .unwrap()
        .into_struct("".into())
        .into_series();
    let items = Series::new("items".into(), [item.clone(), item]);
    // Over the size parsed on the key, so a worker parses it.
    let rows: Vec<String> = (0..20_000).map(|i| format!("{{\"id\": {i}}}")).collect();
    let big = format!("{{\"rows\": [{}]}}", rows.join(", "));
    assert!(big.len() > 64 * 1024);
    let df = DataFrame::new(
        2,
        vec![
            customer.into_column(),
            items.into_column(),
            Column::new("payload".into(), [big, "{\"a\": [1, 2]}".to_string()]),
            Column::new("bad".into(), ["{oops}", "{oops}"]),
        ],
    )
    .unwrap();
    write_parquet(&dir, "", df);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.join("data.parquet")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));
    let last_copy = || copies.lock().unwrap().last().unwrap().text.clone();
    let area = Rect::new(0, 0, 80, 24);
    let t = datui::glyphs::get().trail;
    let levels = |app: &App| -> Vec<String> {
        app.inspector_modal.drill.as_ref().map_or(Vec::new(), |d| {
            d.levels.iter().map(|l| l.label.clone()).collect()
        })
    };

    // The row, on its first field; Enter opens the struct.
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(app.overlay, Overlay::Inspect);
    let root = painted(&mut app, &rx, &tx, area);
    assert!(root.contains(" Enter  Open "), "{root}");
    press_and_send(&mut app, &tx, KeyCode::Enter);
    let screen = painted(&mut app, &rx, &tx, area);
    assert!(
        screen.contains(&format!("Row 1 of 2 {t} customer")),
        "{screen}"
    );
    assert!(screen.contains(" Esc  Back "), "{screen}");

    // `j` then `→` into the address; `y` copies the street.
    press_and_send(&mut app, &tx, KeyCode::Char('j'));
    press_and_send(&mut app, &tx, KeyCode::Right);
    let screen = painted(&mut app, &rx, &tx, area);
    assert!(
        screen.contains(&format!("Row 1 of 2 {t} customer {t} address")),
        "{screen}"
    );
    assert!(screen.contains("1 Main St"), "{screen}");
    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(last_copy(), "1 Main St");

    // Esc and ← climb a level each, back to the row.
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert_eq!(levels(&app), ["customer"]);
    press_and_send(&mut app, &tx, KeyCode::Left);
    assert!(levels(&app).is_empty());
    assert_eq!(app.overlay, Overlay::Inspect);

    // A list of structs is a table of its fields.
    press_and_send(&mut app, &tx, KeyCode::Char('j'));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    let screen = painted(&mut app, &rx, &tx, area);
    let header = screen
        .lines()
        .find(|l| l.contains("sku"))
        .unwrap_or_else(|| panic!("{screen}"));
    assert!(header.contains("qty"), "{screen}");
    assert!(
        screen
            .lines()
            .any(|l| l.contains("[1]") && l.contains("B7")),
        "{screen}"
    );
    press_and_send(&mut app, &tx, KeyCode::Esc);

    // Long JSON text is parsed on a worker, then drilled like a struct.
    press_and_send(&mut app, &tx, KeyCode::Char('j'));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert!(app.is_busy(), "the parse is a job the user waits on");
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(levels(&app), ["payload"]);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    press_and_send(&mut app, &tx, KeyCode::End);
    let screen = painted(&mut app, &rx, &tx, area);
    assert!(
        screen.contains(&format!("payload {t} rows")) && screen.contains("20,000"),
        "{screen}"
    );
    assert!(screen.contains("[19999]"), "{screen}");
    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(last_copy(), "{\n  \"id\": 19999\n}");

    // Text that is not JSON stays where it is and says why.
    press_and_send(&mut app, &tx, KeyCode::Esc);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    press_and_send(&mut app, &tx, KeyCode::Char('j'));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert!(levels(&app).is_empty());
    assert!(
        app.flash_message()
            .is_some_and(|m| m.starts_with("Not JSON")),
        "{:?}",
        app.flash_message()
    );

    // Esc at the row closes the inspector.
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());
}

/// #615: NDJSON objects load as structs and lists, and drill the same way.
#[test]
fn test_inspector_drills_into_ndjson_objects() {
    let dir = common::fixture_dir().join("inspector_ndjson_615");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.ndjson");
    std::fs::write(
        &path,
        "{\"id\": 1, \"user\": {\"name\": \"u1\", \"roles\": [\"admin\", \"dev\"]}}\n\
         {\"id\": 2, \"user\": {\"name\": \"u2\", \"roles\": []}}\n",
    )
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 80, 24);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Char('l'));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    press_and_send(&mut app, &tx, KeyCode::Char('j'));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    let t = datui::glyphs::get().trail;
    let screen = painted(&mut app, &rx, &tx, area);
    assert!(
        screen.contains(&format!("Row 1 of 2 {t} user {t} roles")),
        "{screen}"
    );
    assert!(
        screen
            .lines()
            .any(|l| l.contains("[1]") && l.contains("dev")),
        "{screen}"
    );
    for _ in 0..3 {
        press_and_send(&mut app, &tx, KeyCode::Esc);
    }
    assert!(app.at_table());
}

/// Space opens the inspector over the current row; ↑↓ walk the fields, ←→ the
/// rows with the table's cursor, and Esc leaves the table where the inspector
/// left it.
#[test]
fn test_inspector_opens_moves_between_rows_and_fields_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx, tx, _) = open_inspector_fixture(dir.path());

    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 1"), "{screen}");
    assert!(screen.contains("Fields"), "{screen}");
    for name in ["id", "description", "amount", "tags", "blob"] {
        assert!(screen.contains(name), "{name}: {screen}");
    }

    // The field list previews the break as a mark; the pane breaks the line.
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "description");
    let g = datui::glyphs::get();
    let screen = draw_inspector(&mut app);
    assert!(
        screen.contains(&format!("line1{}line2", g.newline_mark)),
        "{screen}"
    );
    let lines: Vec<&str> = screen.lines().collect();
    assert!(
        lines
            .iter()
            .any(|l| l.trim_start_matches(['│', '|', ' ']).starts_with("line2")),
        "line2 on a line of its own: {screen}"
    );

    // The amount: exact in the pane, and what the table rounds it to under it.
    press_key(&mut app, KeyCode::Char('j'), KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "amount");
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("1000000.125"), "{screen}");
    assert!(screen.contains("In the table:"), "{screen}");

    // The next rows, with the table's cursor; the field stays.
    press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 3"), "{screen}");
    assert!(screen.contains("NaN"), "{screen}");
    assert_eq!(inspected_field(&app), "amount");
    press_key(&mut app, KeyCode::Left, KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 2"), "{screen}");
    let side = format!("{}  -0.0 ", g.border.vertical_left);
    assert!(screen.lines().any(|l| l.contains(&side)), "{screen}");

    // Home and End reach the ends of the list.
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "blob");
    press_key(&mut app, KeyCode::Home, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "id");

    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.at_table());
    assert_ne!(app.overlay, Overlay::Inspect);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.start_row() + state.table_state.selected().unwrap(), 1);

    // Space opens it again, on the field it was on, and Space closes it too.
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "id");
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    assert!(app.at_table());
}

/// Text is exact in the pane and escaped on `e`: a break and a literal
/// backslash-n read apart, edge spaces and the empty string show, and a null is
/// not an empty string.
#[test]
fn test_inspector_shows_text_exactly_raw_and_escaped() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx, _) = open_inspector_fixture(dir.path());
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('e'), KeyModifiers::NONE);
    let rows = |app: &mut App| {
        let screen = draw_inspector(app);
        press_key(app, KeyCode::Right, KeyModifiers::NONE);
        screen
    };
    assert!(rows(&mut app).contains(r#""line1\nline2""#));
    assert!(rows(&mut app).contains(r#""tab\tseparated""#));
    assert!(rows(&mut app).contains(r#""literal \\n backslash""#));
    let padded = rows(&mut app);
    assert!(padded.contains(r#""  padded  ""#), "{padded}");
    assert!(padded.contains("2 leading spaces"), "{padded}");
    let empty = rows(&mut app);
    assert!(empty.contains(r#""""#), "{empty}");
    assert!(empty.contains("empty"), "{empty}");
    let null = draw_inspector(&mut app);
    let g = datui::glyphs::get();
    assert!(null.contains(&format!("{} null", g.null)), "{null}");
    assert!(
        !null.contains(r#""""#),
        "a null is not an empty string: {null}"
    );
}

/// `y` copies the focused field exactly: the stored float, not the table's
/// `1.0000e6`; a list as JSON; a null as nothing.
#[test]
fn test_inspector_copies_the_exact_value() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx, copies) = open_inspector_fixture(dir.path());
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "1000000.125");
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), r#"["t0","x"]"#);
    press_key(&mut app, KeyCode::Up, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "-0.0");
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Copied amount of row 2"), "{screen}");

    // The copy dialog's Cell scope is exact too.
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Up, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    copy_scope(&mut app, datui::copy_modal::CopyScope::Cell);
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    for c in "amount".chars() {
        press_key(&mut app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "1000000.125");
}

/// Binary and hidden columns are not in the table's rows: they show as not
/// read until Enter reads them for this row, in the background, and then show
/// and copy like any other field.
#[test]
fn test_inspector_reads_hidden_and_binary_fields_on_enter() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx, tx, copies) = open_inspector_fixture(dir.path());
    app.data_table_state
        .as_mut()
        .unwrap()
        .set_column_order(["id", "amount", "blob"].map(String::from).to_vec());
    pump_until_idle(&mut app, &rx, &tx);
    draw_inspector(&mut app);

    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    let names: Vec<String> = app
        .inspector_modal
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(names, ["id", "amount", "blob", "description", "tags"]);
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    for c in "desc".chars() {
        press_key(&mut app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "description");
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("not read"), "{screen}");
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert!(copies.lock().unwrap().is_empty(), "nothing to copy yet");

    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    assert!(app.is_busy(), "the read runs off the UI thread");
    pump_until_idle(&mut app, &rx, &tx);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("line2"), "{screen}");
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "line1\nline2");

    // The same read brought the bytes: a hex dump, copied as base64.
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert!(
        app.inspector_modal.filter.is_empty(),
        "Esc clears the find first"
    );
    assert_eq!(app.overlay, Overlay::Inspect);
    press_key(&mut app, KeyCode::Up, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "blob");
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("48 69 00"), "{screen}");
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "SGkA");

    // Another row has read nothing.
    press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    assert!(draw_inspector(&mut app).contains("not read"));

    // The read checks the row it found by the fields the table shows; a NaN
    // among them is still the same row.
    press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 3"), "{screen}");
    // One byte of UTF-8 reads as text; `e` shows it as hex.
    assert!(screen.contains("UTF-8 text"), "{screen}");
    press_key(&mut app, KeyCode::Char('e'), KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("00000000  62"), "{screen}");
}

/// The inspector shows the row the table shows, after a sort, and lists a
/// grouped row's lists whole; Enter still drills into the group once it closes.
#[test]
fn test_inspector_follows_the_view_and_leaves_enter_to_drill() {
    let (mut app, rx, tx) = open_query_filter_fixture("inspect_by.csv");
    press_key(&mut app, KeyCode::Char('r'), KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    draw_inspector(&mut app);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 1"), "{screen}");
    assert!(
        screen.contains("beta_99"),
        "the reversed view's first row: {screen}"
    );
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select name by c where a < 4".to_string());
    pump_until_idle(&mut app, &rx, &tx);
    draw_inspector(&mut app);
    pump_until_idle(&mut app, &rx, &tx);
    draw_inspector(&mut app);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "name");
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("2 items"), "{screen}");
    assert!(
        screen.lines().any(|l| l.contains("  \"")),
        "one item per line: {screen}"
    );
    // #548: on a group's row Enter drills from the inspector too, in one key.
    assert!(screen.contains("Enter  Rows"), "{screen}");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_ne!(app.overlay, Overlay::Inspect);
}

/// #548: a huge value is read a screen at a time. Tab focuses it and End
/// reaches its last line at once, with where it is on the rule; there is no
/// More to press, and `y` still copies all of it.
#[test]
fn test_inspector_reads_a_huge_value_to_its_end_in_two_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("huge.parquet");
    let huge = format!("{}THE END", "0123456789 ".repeat(200_000));
    let mut df = df!("id" => [1i64], "huge" => [huge.as_str()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    draw_inspector(&mut app);
    let copies = Copies::default();
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));

    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("2,200,007 chars"), "{screen}");
    assert!(!screen.contains("More"), "{screen}");
    assert!(screen.contains("Tab  Value"), "{screen}");

    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("THE END"), "{screen}");
    assert!(screen.contains("100%"), "{screen}");
    assert!(app.inspector_modal.reader.take_formatted() <= 16 * 1024);

    press_key(&mut app, KeyCode::Home, KeyModifiers::NONE);
    let screen = draw_inspector(&mut app);
    assert!(!screen.contains("THE END"), "{screen}");
    assert!(screen.contains(" 0%"), "{screen}");

    // Over a megabyte: copied off this thread, whole.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(copies.lock().unwrap().last().unwrap().len(), huge.len());
    // Esc gives the focus back to the list, then closes.
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect);
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert_ne!(app.overlay, Overlay::Inspect);
}

/// #548 M1, D11 and D5: a fourteen-field row is listed whole at 80×24 and on a
/// wide terminal, with nothing hidden under "more", and an empty string reads
/// `""` rather than blank. D6: `e` is offered on text, not on a number, and Find
/// keeps its chip.
#[test]
fn test_inspector_lists_a_short_row_whole_and_offers_only_keys_that_act() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx) = open_orders_fixture(dir.path());
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect);
    for (width, height) in [(80, 24), (200, 50)] {
        let rows = rows_at(&mut app, width, height);
        let text = rows.join("\n");
        assert!(rows[0].contains("Row 1 of 3"), "{width}x{height}:\n{text}");
        for name in ["id", "customer_name", "email", "point", "blob", "region"] {
            assert!(
                rows.iter().any(|r| r.contains(&format!(" {name} "))),
                "{name} at {width}x{height}:\n{text}"
            );
        }
        assert!(!text.contains(" more"), "{width}x{height}:\n{text}");
        let email = rows.iter().find(|r| r.contains(" email ")).unwrap();
        assert!(email.contains(r#""""#), "{width}x{height}: {email}");
        // On `id`, an integer: no escaped form, and Find is on the footer.
        let footer = &rows[height as usize - 3];
        assert!(footer.contains("/  Find"), "{width}x{height}: {footer}");
        assert!(!footer.contains("Escaped"), "{width}x{height}: {footer}");
    }
    // `e` on the number does nothing; on text it is offered and acts.
    press_key(&mut app, KeyCode::Char('e'), KeyModifiers::NONE);
    assert_eq!(app.inspector_modal.view, None, "a number has one view");
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "email");
    let rows = rows_at(&mut app, 80, 24);
    assert!(rows[21].contains("e  Escaped"), "{}", rows.join("\n"));
    press_key(&mut app, KeyCode::Char('e'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 80, 24);
    assert!(rows[21].contains("e  Raw"), "{}", rows.join("\n"));
}

/// #548 M1, D9: inside a drill-down the title names the row among the group's
/// rows and the group's key, which the takeover hides from the breadcrumb.
#[test]
fn test_inspector_title_names_the_group_inside_a_drill() {
    let (mut app, rx, tx) = open_query_filter_fixture("inspect_drill_title.csv");
    app.event(AppEvent::QQuery("select n: count a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, Rect::new(0, 0, 80, 24));
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    painted(&mut app, &rx, &tx, Rect::new(0, 0, 80, 24));
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect);
    let m = datui::glyphs::get().middot;
    let state = app.data_table_state.as_ref().unwrap();
    let key = state.drilled_group_key().unwrap().1[0].clone();
    let title = format!("Row 2 of {} {m} c={key}", state.num_rows());
    for (width, height) in [(80, 24), (200, 50)] {
        let rows = rows_at(&mut app, width, height);
        assert!(
            rows[0].contains(&title),
            "{title} at {width}x{height}: {}",
            rows[0]
        );
    }
}

/// #548 D3: a long value's rule says where the pane is in the whole value, not
/// in a chunk of it. A 1 MiB blob of noise is sized, read as hex, and its last
/// offset is two keys away.
#[test]
fn test_inspector_counts_the_lines_of_the_whole_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.parquet");
    // Noise, so the bytes are not text: a repeated letter would read as UTF-8.
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let blob: Vec<u8> = (0..1 << 20)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed >> 24) as u8
        })
        .collect();
    let mut df = df!("id" => [1i64], "blob" => [blob.as_slice()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    rows_at(&mut app, 80, 24);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    // Not read yet: Enter reads it, and there is nothing for `y` to copy.
    let footer = rows_at(&mut app, 80, 24)[21].clone();
    assert!(footer.contains("Enter  Read"), "{footer}");
    assert!(!footer.contains("Copy"), "{footer}");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let footer = rows_at(&mut app, 80, 24)[21].clone();
    assert!(footer.contains("y  Copy base64"), "{footer}");
    for (width, height) in [(200u16, 50u16), (80, 24)] {
        let text = rows_at(&mut app, width, height).join("\n");
        assert!(
            text.contains("1,048,576 bytes"),
            "{width}x{height}:\n{text}"
        );
        assert!(text.contains("of 0x100000"), "{width}x{height}:\n{text}");
        assert!(!text.contains("more lines"), "{width}x{height}:\n{text}");
        assert!(text.contains("Tab  Value"), "{width}x{height}:\n{text}");
    }
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    let text = rows_at(&mut app, 80, 24).join("\n");
    assert!(text.contains("000ffff0"), "the last row: {text}");
    assert!(text.contains("-0xfffff of 0x100000"), "{text}");
    assert!(text.contains("Home/End"), "the scroll keys: {text}");
}

/// #548: `c` puts the next row beside this one and marks and counts the fields
/// that differ; `f` then lists only those. `m` pins a row to compare others with.
#[test]
fn test_inspector_compares_rows_and_lists_only_the_differences() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx) = open_orders_fixture(dir.path());
    let g = datui::glyphs::get();
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('c'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 80, 24);
    let text = rows.join("\n");
    assert!(rows[0].contains("Row 1 of 3"), "{text}");
    assert!(rows[0].contains("compare with 2"), "{text}");
    // Every field but the binary one, which neither row has read, differs.
    assert!(rows[1].contains("13 differ"), "{text}");
    // The list's rows: the first is `id`, under the focus rail.
    let field_row = |name: &str| {
        rows[2..]
            .iter()
            .find(|r| {
                r.contains(&format!("{}{name} ", g.rail)) || r.contains(&format!("  {name} "))
            })
            .unwrap()
            .clone()
    };
    let id = field_row("id");
    assert!(id.contains(g.diff_mark), "{id}");
    let blob = field_row("blob");
    assert!(!blob.contains(g.diff_mark), "{blob}");

    press_key(&mut app, KeyCode::Char('f'), KeyModifiers::NONE);
    let text = rows_at(&mut app, 80, 24).join("\n");
    assert!(text.contains("differ only"), "{text}");
    assert!(!text.contains(" blob "), "{text}");
    assert_eq!(app.inspector_modal.visible.len(), 13);

    // Pinned, the row stays beside each row moved to.
    press_key(&mut app, KeyCode::Char('m'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 80, 24);
    assert!(rows[0].contains("Row 3 of 3"), "{}", rows.join("\n"));
    assert!(
        rows[0].contains("compare with pinned 1"),
        "{}",
        rows.join("\n")
    );

    // Compare off: `f` hides the nulls and empties, and says so.
    press_key(&mut app, KeyCode::Char('c'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('h'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('h'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('f'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 80, 24);
    let text = rows.join("\n");
    assert!(
        rows[1].contains("1 null") && rows[1].contains("1 empty"),
        "{text}"
    );
    assert!(rows[1].contains("nulls hidden"), "{text}");
    let wide = rows_at(&mut app, 200, 24).join("\n");
    assert!(wide.contains("Nulls: hidden"), "{wide}");
    press_key(&mut app, KeyCode::Char('f'), KeyModifiers::NONE);
    let wide = rows_at(&mut app, 200, 24).join("\n");
    assert!(wide.contains("Nulls: shown"), "{wide}");
    assert!(wide.contains(" email "), "{wide}");
    press_key(&mut app, KeyCode::Char('f'), KeyModifiers::NONE);
    assert!(!text.contains(" customer_name "), "{text}");
    assert!(!text.contains(" email "), "{text}");
    // `s` orders them by name.
    press_key(&mut app, KeyCode::Char('s'), KeyModifiers::NONE);
    rows_at(&mut app, 80, 24);
    let modal = &app.inspector_modal;
    let first = &modal.fields[modal.visible[0]].name;
    assert_eq!(first, "amount");
}

/// Esc backs out of Compare before it closes the inspector; Tab and Shift+Tab
/// cross to the value without moving a field; Tab in a level with nothing in it
/// stays on its list.
#[test]
fn test_inspector_esc_leaves_compare_first_and_tab_moves_nothing() {
    use datui::inspector_modal::Focus;

    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx, tx) = open_orders_fixture(dir.path());
    let none = KeyModifiers::NONE;
    press_key(&mut app, KeyCode::Char(' '), none);
    press_key(&mut app, KeyCode::Char('c'), none);
    assert!(app.inspector_modal.compare);
    let wide = rows_at(&mut app, 200, 24).join("\n");
    assert!(wide.contains("Esc  No compare"), "{wide}");
    press_key(&mut app, KeyCode::Esc, none);
    assert!(!app.inspector_modal.compare, "Esc leaves Compare");
    assert_eq!(app.overlay, Overlay::Inspect, "and only Compare");
    let wide = rows_at(&mut app, 200, 24).join("\n");
    assert!(wide.contains("Y  Copy row"), "{wide}");
    assert!(wide.contains("c  Compare"), "{wide}");
    assert!(wide.contains("Esc  Close"), "{wide}");

    // The panes are split by what they hold: crossing to the value moves no
    // field of the list.
    let line_of = |rows: &[String], name: &str| {
        rows.iter()
            .position(|row| row.contains(&format!(" {name} ")))
            .unwrap_or_else(|| panic!("{name}: {}", rows.join("\n")))
    };
    let before = rows_at(&mut app, 120, 40);
    for code in [KeyCode::Tab, KeyCode::BackTab] {
        press_key(&mut app, code, none);
        assert_eq!(app.inspector_modal.focus, Focus::Value, "{code:?}");
        let after = rows_at(&mut app, 120, 40);
        for name in ["customer_name", "region"] {
            assert_eq!(line_of(&before, name), line_of(&after, name), "{code:?}");
        }
        press_key(&mut app, KeyCode::Esc, none);
        assert_eq!(app.inspector_modal.focus, Focus::List);
    }

    // `{}` opens as JSON into a level with no items: no value to cross to.
    press_key(&mut app, KeyCode::Char('l'), none);
    pump_until_idle(&mut app, &rx, &tx);
    while inspected_field(&app) != "payload_json" {
        press_key(&mut app, KeyCode::Down, none);
    }
    press_key(&mut app, KeyCode::Enter, none);
    pump_until_idle(&mut app, &rx, &tx);
    rows_at(&mut app, 120, 40);
    assert!(
        app.inspector_modal.drill.is_some(),
        "Enter opens the object"
    );
    for code in [KeyCode::Tab, KeyCode::BackTab] {
        press_key(&mut app, code, none);
        assert_eq!(app.inspector_modal.focus, Focus::List, "{code:?}");
    }
}

/// #661: from 240 columns Compare shows the row before too: previous, this,
/// next, in row order and named over their columns; narrower, the next only.
#[test]
fn test_inspector_compares_three_rows_on_a_wide_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx) = open_orders_fixture(dir.path());
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('c'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 240, 50);
    let text = rows.join("\n");
    assert!(rows[0].contains("Row 2 of 3"), "{text}");
    assert!(rows[0].contains("compare with 1 and 3"), "{text}");
    let at = |row: &str, s: &str| row.find(s).unwrap_or_else(|| panic!("{s}:\n{text}"));
    let rule = &rows[1];
    assert!(at(rule, "Row 1") < at(rule, "Row 2") && at(rule, "Row 2") < at(rule, "Row 3"));
    let region = rows.iter().find(|r| r.contains(" region ")).unwrap();
    assert!(
        at(region, "north") < at(region, "south") && at(region, "south") < at(region, "east"),
        "{text}"
    );
    // Narrower, the next row only.
    let rows = rows_at(&mut app, 200, 50);
    let text = rows.join("\n");
    assert!(rows[0].contains("compare with 3"), "{text}");
    let region = rows.iter().find(|r| r.contains(" region ")).unwrap();
    assert!(!region.contains("north"), "{text}");
    // A pinned row is the one compared with, at any width.
    press_key(&mut app, KeyCode::Char('m'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('h'), KeyModifiers::NONE);
    let rows = rows_at(&mut app, 240, 50);
    assert!(
        rows[0].contains("compare with pinned 2"),
        "{}",
        rows.join("\n")
    );
}

/// #548: `Y` copies the whole row as one JSON object, exact, without leaving;
/// a field not read is left out and counted.
#[test]
fn test_inspector_copies_the_row_as_json() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, _rx, _tx, copies) = open_inspector_fixture(dir.path());
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('Y'), KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect, "the inspector stays open");
    let copied = copies.lock().unwrap().last().unwrap().clone();
    let json: serde_json::Value = serde_json::from_str(&copied).unwrap();
    assert_eq!(json["id"], 1);
    assert_eq!(json["description"], "line1\nline2");
    assert_eq!(json["tags"], serde_json::json!(["t0", "x"]));
    assert!(copied.contains("1000000.125"), "{copied}");
    assert!(json.get("blob").is_none(), "{copied}");
    let screen = draw_inspector(&mut app);
    assert!(
        screen.contains("Copied row 1: 4 fields, 1 not read"),
        "{screen}"
    );
}

/// #548: after Enter reads a field, each row moved to is read too while the
/// focus stays on it; an answer for a row already left is dropped.
#[test]
fn test_inspector_reads_follow_the_row_after_one_enter() {
    let dir = tempfile::tempdir().unwrap();
    let (mut app, rx, tx, _) = open_inspector_fixture(dir.path());
    let g = datui::glyphs::get();
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::End, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "blob");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let screen = draw_inspector(&mut app);
    assert!(
        screen.contains("48 69 00"),
        "row 1's bytes, as hex: {screen}"
    );

    // Two rows on before the first follow-up read lands: only the last is kept.
    press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
    draw_inspector(&mut app);
    app.request_what_the_frame_needs();
    press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
    draw_inspector(&mut app);
    app.request_what_the_frame_needs();
    // Nobody waits on a follow-up read: pump until it lands.
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(
            app.inspector_modal.read,
            Some(datui::inspector_modal::FieldRead::Reading { .. })
        )
    });
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("Row 3"), "{screen}");
    assert!(!screen.contains("not read"), "{screen}");
    let blob = screen
        .lines()
        .find(|l| l.contains(&format!("{}blob ", g.rail)))
        .unwrap();
    assert!(blob.contains("1 byte"), "row 3's one byte: {blob}");
    let frame = app.data_table_state.as_ref().unwrap().len_generation();
    let read = app
        .inspector_modal
        .read_values(frame, 2)
        .and_then(|v| v.column("blob").ok()?.get(0).ok().map(|v| v.to_string()));
    assert!(read.is_some(), "row 3 was read");
    assert!(app.inspector_modal.read_values(frame, 1).is_none());

    // Off the field, moving reads nothing.
    press_key(&mut app, KeyCode::Up, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('h'), KeyModifiers::NONE);
    draw_inspector(&mut app);
    app.request_what_the_frame_needs();
    assert!(app.inspector_modal.follow.is_none());
    assert!(app.inspector_modal.read.is_none());
}

/// #548: `/` in the value finds text in it; `n` goes round the places found.
#[test]
fn test_inspector_finds_text_inside_a_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.parquet");
    let log: String = (0..2_000)
        .map(|i| {
            if i % 500 == 7 {
                format!("line {i} ERROR disk full\n")
            } else {
                format!("line {i} ok\n")
            }
        })
        .collect();
    let mut df = df!("id" => [1i64], "log" => [log.as_str()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    // A frame between keys, as the event loop draws one.
    let key = |app: &mut App, code: KeyCode| {
        rows_at(app, 80, 24);
        press_key(app, code, KeyModifiers::NONE);
    };
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::End);
    key(&mut app, KeyCode::Tab);
    // At 80 columns the rule still says where the pane is, before the facts.
    let rule = rows_at(&mut app, 80, 24)
        .into_iter()
        .find(|r| r.contains("log  str"))
        .unwrap();
    assert!(rule.contains("lines 1-") && rule.contains("0%"), "{rule}");
    key(&mut app, KeyCode::Char('/'));
    for c in "error".chars() {
        key(&mut app, KeyCode::Char(c));
    }
    key(&mut app, KeyCode::Enter);
    let text = rows_at(&mut app, 80, 24).join("\n");
    assert!(text.contains("1 of 4"), "{text}");
    assert!(text.contains("line 7 ERROR"), "{text}");
    press_key(&mut app, KeyCode::Char('n'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('n'), KeyModifiers::NONE);
    let text = rows_at(&mut app, 80, 24).join("\n");
    assert!(text.contains("3 of 4"), "{text}");
    assert!(text.contains("line 1007 ERROR"), "{text}");
    assert!(text.contains("n/N  Next"), "{text}");
    press_key(&mut app, KeyCode::Char('N'), KeyModifiers::NONE);
    let text = rows_at(&mut app, 80, 24).join("\n");
    assert!(text.contains("line 507 ERROR"), "{text}");
    // Esc clears the find, then gives the focus back to the fields.
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.inspector_modal.value_find.is_none());
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Inspect);
}

/// #548: on a wide terminal the fields and the value sit side by side, and a
/// wide row's fields flow into columns: at least 44 of 214 at 200x50 and 150
/// at 300x80.
#[test]
fn test_inspector_lays_a_wide_row_out_side_by_side() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wide.parquet");
    let columns: Vec<Column> = (0..214)
        .map(|i| Column::new(format!("metric_{i:03}").into(), [i as i64, 0]))
        .collect();
    let mut df = DataFrame::new(2, columns).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    rows_at(&mut app, 200, 50);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    for (width, height, least) in [(200u16, 50u16, 44usize), (300, 80, 150)] {
        let rows = rows_at(&mut app, width, height);
        let text = rows.join("\n");
        let listed = text.matches("metric_").count();
        assert!(
            listed >= least,
            "{listed} listed at {width}x{height}:\n{text}"
        );
        // One rule line holds both panes' titles.
        assert!(
            rows[1].contains("Fields") && rows[1].contains("metric_000  i64"),
            "{width}x{height}:\n{text}"
        );
    }
    // PgDn pages the list.
    let before = app.inspector_modal.focused_position();
    press_key(&mut app, KeyCode::PageDown, KeyModifiers::NONE);
    assert!(app.inspector_modal.focused_position() > before + 40);
}

/// #661: from 240 columns a row with bytes gives its hex dump 32 bytes a row;
/// a resize while reading keeps the offset at the top, at any row length.
#[test]
fn test_inspector_widens_hex_rows_and_keeps_the_place_on_a_resize() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bytes.parquet");
    let bytes: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
    let mut df = df!("id" => [1i64], "blob" => [bytes.as_slice()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    rows_at(&mut app, 300, 80);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    assert_eq!(inspected_field(&app), "blob");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let text = rows_at(&mut app, 300, 80).join("\n");
    assert!(text.contains(" 00000020  "), "{text}");
    assert!(!text.contains(" 00000010  "), "{text}");
    let text = rows_at(&mut app, 200, 50).join("\n");
    assert!(
        text.contains(" 00000010  "),
        "16 a row at 200 columns:\n{text}"
    );

    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    rows_at(&mut app, 300, 80);
    press_key(&mut app, KeyCode::PageDown, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::PageDown, KeyModifiers::NONE);
    let top = |rows: &[String]| {
        let text = rows.join("\n");
        let at = text.find(" of 0x2000").expect(&text);
        let from = text[..at].rsplit(' ').next().unwrap();
        from.split('-').next().unwrap().to_string()
    };
    let before = top(&rows_at(&mut app, 300, 80));
    assert_ne!(before, "0x0");
    for (width, height) in [(200u16, 50u16), (80, 24), (300, 80)] {
        let rows = rows_at(&mut app, width, height);
        assert_eq!(top(&rows), before, "{width}x{height}:\n{}", rows.join("\n"));
    }
}

/// #661: gzip bytes are decompressed for their Text view by a worker, never
/// while the pane is built; bytes that hold no text lose the view and say so.
#[test]
fn test_inspector_decompresses_bytes_off_the_ui_thread() {
    use std::io::Write;
    let gzip = |bytes: &[u8]| {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    };
    let text = gzip(b"hello gzip");
    let noise = gzip(&[0u8, 1, 2, 0xff]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gzip.parquet");
    let mut df = df!(
        "id" => [1i64, 2],
        "blob" => [text.as_slice(), noise.as_slice()],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    draw_inspector(&mut app);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let pending = |app: &App| {
        matches!(
            app.inspector_modal.unpack,
            Some(datui::inspector_modal::Unpack::Pending { .. })
        )
    };

    let screen = draw_inspector(&mut app);
    assert!(screen.contains("gzip"), "{screen}");
    assert!(screen.contains("00000000"), "hex first: {screen}");
    for (row, shows) in [(1, "hello gzip"), (2, "not text")] {
        if row == 1 {
            press_key(&mut app, KeyCode::Char('e'), KeyModifiers::NONE);
        } else {
            // The next row keeps the Text view chosen for the field.
            press_key(&mut app, KeyCode::Char('l'), KeyModifiers::NONE);
            press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
            pump_until_idle(&mut app, &rx, &tx);
        }
        let screen = draw_inspector(&mut app);
        assert!(screen.contains("Decompressing..."), "row {row}: {screen}");
        assert!(app.inspector_modal.unpack.is_none(), "not in the frame");
        app.request_what_the_frame_needs();
        assert!(pending(&app));
        pump_until(&mut app, &rx, &tx, |app| !pending(app));
        let screen = draw_inspector(&mut app);
        assert!(screen.contains(shows), "row {row}: {screen}");
    }
    // The bytes that hold no text are back on their hex dump.
    let screen = draw_inspector(&mut app);
    assert!(screen.contains("00000000"), "{screen}");
}

/// A field past a megabyte is copied off the UI thread, whole.
#[test]
fn test_inspector_copies_a_large_field_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.parquet");
    let large = "abcdefgh".repeat(256 * 1024);
    let mut df = df!("large" => [large.as_str()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    draw_inspector(&mut app);
    let copies = Copies::default();
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));

    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert!(app.is_busy(), "written off the UI thread");
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(copies.lock().unwrap().last().unwrap().len(), large.len());
    assert!(draw_inspector(&mut app).contains("Copied large of row 1"));
}

/// `y` asks the destination first, as the copy dialog does: a field over the
/// terminal's cap is refused at once, never formatted on a worker, and the last
/// copy stays. One under the cap still goes.
#[test]
fn test_inspector_refuses_a_field_over_the_terminal_cap_before_formatting_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capped.parquet");
    let large = "abcdefgh".repeat(256 * 1024);
    let mut df = df!("id" => [1i64], "large" => [large.as_str()]).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    draw_inspector(&mut app);
    let copies = Copies::default();
    app.set_clipboard_destination(Box::new(CappedCopies(copies.clone(), 100 * 1024)));

    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert_eq!(copies.lock().unwrap().last().unwrap(), "1");

    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    assert!(!app.is_busy(), "refused before a worker formats it");
    let message = app.error_message().expect("refused out loud").to_string();
    assert!(
        message.contains("over 100 KB of base64") && message.contains("osc52_limit"),
        "{message}"
    );
    assert_eq!(copies.lock().unwrap().len(), 1, "the last copy stays");
}

/// The per-column keys act on the column cursor's column: the sidebar opens on it
/// (its Columns row, and a new filter's column), the inspector focuses its field, and
/// a cell copy takes it.
#[test]
fn test_the_column_cursor_drives_the_per_column_keys() {
    let (mut app, rx, tx) = open_csv_with("cursor_keys.csv", COUNTS_CSV, OpenOptions::default());
    painted(&mut app, &rx, &tx, Rect::new(0, 0, 80, 24));
    press_and_send(&mut app, &tx, KeyCode::Char('l'));

    press_and_send(&mut app, &tx, KeyCode::Char('s'));
    assert_eq!(app.overlay, Overlay::SortFilter);
    let sort = &app.sort_filter_modal.sort;
    let row = sort.table_state.selected().unwrap();
    assert_eq!(sort.filtered_columns()[row].1.name, "amount");
    press_and_send(&mut app, &tx, KeyCode::Up); // the tab bar
    press_and_send(&mut app, &tx, KeyCode::Up); // the last row: add filter
    press_and_send(&mut app, &tx, KeyCode::Char(' ')); // a new filter
    let filter = &app.sort_filter_modal.filter;
    let editor = filter.editor.as_ref().expect("the editor is open");
    let chosen = editor.column.selected_original().unwrap();
    assert_eq!(filter.available_columns[chosen], "amount");
    press_and_send(&mut app, &tx, KeyCode::Esc);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());

    press_and_send(&mut app, &tx, KeyCode::Char('l'));
    press_and_send(&mut app, &tx, KeyCode::Char(' '));
    assert_eq!(app.overlay, Overlay::Inspect);
    assert_eq!(app.inspector_modal.focused().unwrap().name, "id");
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());

    press_and_send(&mut app, &tx, KeyCode::Char('h'));
    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    assert_eq!(app.copy_modal.column.as_deref(), Some("amount"));
    press_and_send(&mut app, &tx, KeyCode::Esc);
}

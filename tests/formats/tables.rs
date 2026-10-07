//! `T` at the table: another table of the same file (a worksheet, a database table, a
//! format spec's record type) opened in place of the one on screen, and Enter on the
//! Excel tab's worksheets doing the same.

use super::*;
use datui::formats::{Registry, Spec};
use datui::widgets::info::InfoTab;

/// A copy of the sample `name` in a directory of the test's own.
fn copy_of(name: &str) -> (tempfile::TempDir, PathBuf) {
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let file = Path::new(name).file_name().unwrap();
    let to = dir.path().join(file);
    std::fs::copy(Path::new("tests/sample-data").join(name), &to).unwrap();
    (dir, to)
}

fn open(
    paths: Vec<PathBuf>,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, paths, options);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    (app, rx, tx)
}

/// The frame's lines.
fn lines(app: &mut App) -> Vec<String> {
    let area = Rect::new(0, 0, 120, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    common::buffer_lines(&buf)
}

/// The status footer: the frame's last line.
fn footer(app: &mut App) -> String {
    lines(app).pop().unwrap()
}

/// In the open table picker, the cursor onto `label`.
fn pick(app: &mut App, label: &str) {
    assert_eq!(app.input_mode, InputMode::PickTable);
    let at = app
        .pickers
        .table_picker
        .items()
        .iter()
        .position(|item| item == label)
        .unwrap_or_else(|| panic!("{label} in {:?}", app.pickers.table_picker.items()));
    while app.pickers.table_picker.selected_original() != Some(at) {
        let before = app.pickers.table_picker.selected_original();
        press(app, KeyCode::Down);
        if app.pickers.table_picker.selected_original() == before {
            // At the bottom: round from the top.
            for _ in 0..app.pickers.table_picker.items().len() {
                press(app, KeyCode::Up);
            }
        }
    }
}

/// Enter in the picker, and the open it asks for carried through.
fn open_picked(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    press_and_send(app, tx, KeyCode::Enter);
    pump_until(app, rx, tx, |app| !app.is_busy() && !work_pending(app));
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
}

#[test]
fn t_opens_another_worksheet_of_a_workbook() {
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, rx, tx) = open(vec![book.clone()], OpenOptions::default());
    assert_eq!(column_names(&app), ["id", "item"]);

    press(&mut app, KeyCode::Char('T'));
    let shown = lines(&mut app).join("\n");
    assert!(shown.contains("Table"), "{shown}");
    // Each sheet's range and size, the one open marked; the hidden one too.
    assert_eq!(
        app.pickers.table_picker.items(),
        ["Orders", "2023", "Lookup"]
    );
    assert!(shown.contains("A1:B4"), "{shown}");
    assert!(shown.contains("opened"), "{shown}");
    assert!(shown.contains("hidden"), "{shown}");
    assert_eq!(app.pickers.table_picker.selected_original(), Some(0));

    pick(&mut app, "2023");
    open_picked(&mut app, &rx, &tx);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(column_names(&app), ["month", "total", "note"]);
    assert_eq!(current_rows(&app), 12);
    assert_eq!(app.open_path(), Some(book.join("2023").as_path()));
    let line = footer(&mut app);
    assert!(
        line.contains("sheets.xlsx/2023") && !line.contains("2023/2023"),
        "the footer names the sheet once: {line}"
    );

    // From there, the picker marks the sheet now open.
    press(&mut app, KeyCode::Char('T'));
    assert_eq!(app.pickers.table_picker.selected_original(), Some(1));
}

#[test]
fn a_switch_leaves_the_query_filters_and_sort_behind() {
    use datui::filter_modal::FilterOperator;
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, rx, tx) = open(vec![book.join("2023")], OpenOptions::default());
    app.event(AppEvent::QQuery("select where month < 7".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Filter(vec![filter_stmt(
        "month",
        FilterOperator::Gt,
        "2",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Sort(vec!["total".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(current_rows(&app), 4);

    press(&mut app, KeyCode::Char('T'));
    pick(&mut app, "Orders");
    open_picked(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.get_active_query().is_empty(), "the query went");
    assert!(state.get_filters().is_empty(), "the filters went");
    assert!(state.get_sort_columns().is_empty(), "the sort went");
    assert_eq!(column_names(&app), ["id", "item"]);
    assert_eq!(current_rows(&app), 3);
    // Not the sheet's own footer key: nothing on the main screen says a query.
    assert!(!footer(&mut app).contains("query"));
}

#[test]
fn esc_closes_the_table_picker_and_keeps_the_table() {
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, _rx, _tx) = open(vec![book.clone()], OpenOptions::default());
    press(&mut app, KeyCode::Char('T'));
    pick(&mut app, "2023");
    assert!(press(&mut app, KeyCode::Esc).is_none(), "nothing is opened");
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.is_busy());
    assert_eq!(column_names(&app), ["id", "item"]);
    assert_eq!(app.open_path(), Some(book.as_path()));
    // Enter on the table already open opens nothing either.
    press(&mut app, KeyCode::Char('T'));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn the_footer_offers_t_only_for_a_file_of_several_tables() {
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, _rx, _tx) = open(vec![book], OpenOptions::default());
    assert!(app.offers_other_tables());
    let line = footer(&mut app);
    assert!(line.contains("T Table"), "{line}");

    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("plain.csv");
    std::fs::write(&csv, "a,b\n1,2\n3,4\n").unwrap();
    let (mut app, _rx, _tx) = open(vec![csv], OpenOptions::default());
    assert!(!app.offers_other_tables());
    let line = footer(&mut app);
    assert!(!line.contains("T Table"), "{line}");
    // The key says why it does nothing.
    press(&mut app, KeyCode::Char('T'));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(app.flash_message(), Some("Only one table here"));

    // A workbook of one sheet is one table too.
    let (_dir, one) = copy_of("people.xlsx");
    let (mut app, _rx, _tx) = open(vec![one], OpenOptions::default());
    assert!(!app.offers_other_tables());
    assert!(!footer(&mut app).contains("T Table"));
}

#[test]
fn enter_on_the_excel_tab_opens_the_worksheet_under_the_cursor() {
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, rx, tx) = open(vec![book.clone()], OpenOptions::default());
    press(&mut app, KeyCode::Char('i'));
    while app.info_modal.active_tab != InfoTab::Format {
        press(&mut app, KeyCode::Right);
    }
    let shown = lines(&mut app).join("\n");
    assert!(shown.contains("Enter  Open"), "{shown}");
    // The cursor starts on the sheet open; Enter there opens nothing.
    assert_eq!(app.info_modal.detail_selected, 0);
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.flash_message(), Some("Already open"));

    press(&mut app, KeyCode::Down);
    open_picked(&mut app, &rx, &tx);
    assert!(!app.info_modal.active, "the panel closes for the open");
    assert_eq!(column_names(&app), ["month", "total", "note"]);
    assert_eq!(app.open_path(), Some(book.join("2023").as_path()));
}

/// Enter means a tab's own thing: the column's type on Schema, the worksheet on Excel.
#[test]
fn enter_on_the_schema_tab_retypes_and_on_the_excel_tab_opens() {
    let (_dir, book) = copy_of("sheets.xlsx");
    let (mut app, _rx, _tx) = open(vec![book.clone()], OpenOptions::default());
    press(&mut app, KeyCode::Char('i'));
    while app.info_modal.active_tab != InfoTab::Schema {
        press(&mut app, KeyCode::Left);
    }
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.input_mode, InputMode::Retype);
    assert_eq!(app.open_path(), Some(book.as_path()), "no table opened");
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.input_mode, InputMode::Info, "back to the panel");

    while app.info_modal.active_tab != InfoTab::Format {
        press(&mut app, KeyCode::Right);
    }
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    assert!(app.column_forms.retype.is_none(), "no type picker");
    assert_ne!(app.input_mode, InputMode::Retype);
}

#[cfg(feature = "sqlite")]
#[test]
fn t_opens_another_table_of_a_database_and_leaves_the_query_behind() {
    use datui::filter_modal::FilterOperator;
    let (_dir, db) = copy_of("sqlite/shop.db");
    let (mut app, rx, tx) = open(vec![db.join("customers")], OpenOptions::default());
    assert!(app.offers_other_tables());

    // A query, a filter and a sort on the customers.
    app.event(AppEvent::QQuery("select where id < 4".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "q {:?}", app.error_message());
    app.event(AppEvent::Filter(vec![filter_stmt(
        "id",
        FilterOperator::Gt,
        "1",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "f {:?}", app.error_message());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.get_active_query().is_empty());
    assert_eq!(state.get_filters().len(), 1);
    app.event(AppEvent::Sort(vec!["name".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "s {:?}", app.error_message());

    press(&mut app, KeyCode::Char('T'));
    assert_eq!(
        app.input_mode,
        InputMode::PickTable,
        "{:?}",
        app.error_message()
    );
    // Its own tables and views, each with its kind and columns; not SQLite's own.
    let items = app.pickers.table_picker.items().to_vec();
    assert!(items.contains(&"orders".to_string()), "{items:?}");
    assert!(items.contains(&"big_orders".to_string()), "{items:?}");
    assert!(!items.iter().any(|i| i.starts_with("sqlite_")), "{items:?}");
    let shown = lines(&mut app).join("\n");
    assert!(shown.contains("5 columns"), "{shown}");
    let current = &items[app.pickers.table_picker.selected_original().unwrap()];
    assert_eq!(current, "customers");

    pick(&mut app, "orders");
    open_picked(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        column_names(&app),
        ["id", "customer_id", "amount", "note", "receipt"]
    );
    assert!(state.get_active_query().is_empty(), "the query went");
    assert!(state.get_filters().is_empty(), "the filters went");
    assert!(state.get_sort_columns().is_empty(), "the sort went");
    assert_eq!(app.open_path(), Some(db.join("orders").as_path()));
}

/// Length-prefixed messages, two record types picked by a one-byte type.
const TAPE: &str = r#"
name = "acme.tape"
match = { glob = ["*.tape"] }
endian = "be"

[records]
framing = "length_prefixed"
size = "len"
size_adjust = 2
type = "kind"
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]

[[variants]]
name = "quote"
when = "Q"
fields = [{ name = "bid", type = "u4" }, { name = "ask", type = "u4" }]

[[variants]]
name = "trade"
when = "T"
fields = [{ name = "price", type = "u4" }]
"#;

fn tape_bytes(pairs: u32) -> Vec<u8> {
    let message = |kind: u8, body: Vec<u8>| {
        let mut out = ((body.len() + 1) as u16).to_be_bytes().to_vec();
        out.push(kind);
        out.extend(body);
        out
    };
    let mut out = Vec::new();
    for i in 0..pairs {
        let mut quote = i.to_be_bytes().to_vec();
        quote.extend((i + 1).to_be_bytes());
        out.extend(message(b'Q', quote));
        out.extend(message(b'T', i.to_be_bytes().to_vec()));
    }
    out
}

#[test]
fn t_opens_a_record_type_of_a_spec_file_and_the_whole_file_again() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("day.tape");
    std::fs::write(&data, tape_bytes(50)).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.set_formats(Registry::of(vec![Spec::parse(TAPE, None).unwrap()]));
    pump_open_until_loaded(&mut app, &rx, vec![data.clone()], OpenOptions::default());
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    assert_eq!(current_rows(&app), 100);
    assert!(app.offers_other_tables());

    press(&mut app, KeyCode::Char('T'));
    assert_eq!(
        app.pickers.table_picker.items(),
        ["day.tape", "quote", "trade"]
    );
    assert_eq!(app.pickers.table_picker.selected_original(), Some(0));
    let shown = lines(&mut app).join("\n");
    assert!(shown.contains("every record type"), "{shown}");
    assert!(shown.contains("4 columns"), "{shown}");

    pick(&mut app, "trade");
    open_picked(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);
    assert_eq!(column_names(&app), ["len", "kind", "price"]);
    assert_eq!(app.open_path(), Some(data.join("trade").as_path()));

    // Back to every record.
    press(&mut app, KeyCode::Char('T'));
    assert_eq!(app.pickers.table_picker.selected_original(), Some(2));
    pick(&mut app, "day.tape");
    open_picked(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
}

#[test]
fn t_opens_another_split_of_a_hugging_face_cache() {
    common::ensure_sample_data();
    let cache = PathBuf::from("tests/sample-data/hf_cache");
    let scratch = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open(vec![cache.clone()], options);
    assert!(app.offers_other_tables());
    press(&mut app, KeyCode::Char('T'));
    assert_eq!(
        app.pickers.table_picker.items(),
        ["test", "train", "validation"]
    );
    assert_eq!(app.pickers.table_picker.selected_original(), Some(1));
    pick(&mut app, "test");
    open_picked(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.other_tables(), ["train", "validation"]);
    assert_eq!(app.open_path(), Some(cache.join("test").as_path()));
}

/// A q query on a SQLite table, then a sort: the sort ran into Polars' unreachable.
#[cfg(feature = "sqlite")]
#[test]
fn a_sort_after_a_query_on_a_sqlite_table() {
    let (_dir, db) = copy_of("sqlite/shop.db");
    let (mut app, rx, tx) = open(vec![db.join("customers")], OpenOptions::default());
    app.event(AppEvent::QQuery("select where id < 4".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.error_message().is_none(), "q {:?}", app.error_message());
    press_and_send(&mut app, &tx, KeyCode::Char('['));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.error_message().is_none(),
        "sort {:?}",
        app.error_message()
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_sort_columns(), ["id"]);
    assert_eq!(current_rows(&app), 3, "the query's rows, sorted");
}

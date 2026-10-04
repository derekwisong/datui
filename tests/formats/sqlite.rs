//! SQLite databases: a table opened by name or as the only one, and a database of
//! several listed on the home screen like a directory of tables.
//!
//! Fixtures are written by `scripts/generate_sample_data.py` under
//! `tests/sample-data/sqlite/`. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, drain_events, pump_open_until_loaded};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::discover::{self, EntryKind};
use datui::home::{HomeState, Row};
use datui::{App, AppEvent, InputMode, OpenOptions};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

fn sqlite() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/sqlite")
}

fn open_with(path: PathBuf, options: OpenOptions) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    (app, rx)
}

/// Temporary files go to a scratch directory of the test's own, to be counted.
fn scratch() -> (OpenOptions, PathBuf) {
    let dir = common::fixture_dir().join(format!(
        "sqlite-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    (
        OpenOptions {
            temp_dir: Some(dir.clone()),
            ..OpenOptions::default()
        },
        dir,
    )
}

fn table(name: &str) -> OpenOptions {
    let (options, _) = scratch();
    OpenOptions {
        table: Some(name.to_string()),
        ..options
    }
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .expect("a dataset is open")
        .lf()
        .clone()
        .collect()
        .expect("collect")
}

fn types(df: &DataFrame) -> Vec<(String, DataType)> {
    df.schema()
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect()
}

fn listed(home: &HomeState) -> Vec<String> {
    home.visible()
        .iter()
        .filter_map(|r| match r {
            Row::Entry { entry, .. } => Some(entry.name.clone()),
            _ => None,
        })
        .collect()
}

fn key(app: &mut App, rx: &mpsc::Receiver<AppEvent>, code: KeyCode, modifiers: KeyModifiers) {
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, modifiers)));
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(app, rx);
}

/// Handle events until the home screen lists the database's tables, as `home_test`'s
/// `settle` does. A listing superseded on the way clears the in-flight flag too, so the
/// rows are waited for as well.
fn settle_home(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        while let Ok(event) = rx.try_recv() {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
        let listed = app
            .home
            .sections
            .iter()
            .any(|s| s.rows.iter().any(|r| r.table.is_some()));
        if !app.home.listing_in_flight && listed {
            // A frame sets the list's height, which is what the visible rows are cut to.
            let area = ratatui::layout::Rect::new(0, 0, 100, 30);
            let mut buf = ratatui::buffer::Buffer::empty(area);
            ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the listing never came"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(20)) {
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }
}

fn files_in(dir: &Path) -> usize {
    std::fs::read_dir(dir).map_or(0, |d| d.count())
}

/// A database of one table opens it, typed by what its columns declare and hold.
#[test]
fn a_database_of_one_table_opens_it() {
    let (options, _) = scratch();
    let path = sqlite().join("one.sqlite");
    let (app, _rx) = open_with(path.clone(), options);
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 120);
    assert_eq!(
        types(&df),
        [
            ("station".to_string(), DataType::String),
            ("at".to_string(), DataType::String),
            ("celsius".to_string(), DataType::Float64),
            ("ok".to_string(), DataType::Int64),
        ]
    );
    assert_eq!(app.open_path(), Some(path.as_path()), "named by the file");
}

/// `--table` opens one table of several, named by its path inside the database; a
/// column of mixed types is text, and the Info panel says so and names the others.
#[test]
fn table_opens_one_of_several() {
    let db = sqlite().join("shop.db");
    let (app, _rx) = open_with(db.clone(), table("orders"));
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(df.height(), 200);
    assert_eq!(
        types(&df),
        [
            ("id".to_string(), DataType::Int64),
            ("customer_id".to_string(), DataType::Int64),
            ("amount".to_string(), DataType::Float64),
            ("note".to_string(), DataType::String),
            ("receipt".to_string(), DataType::Binary),
        ]
    );
    let note = df.column("note").unwrap().str().unwrap();
    assert_eq!(
        (
            note.get(0),
            note.get(1),
            note.get(2),
            note.get(3),
            note.get(4)
        ),
        (Some("gift"), Some("7"), Some("2.5"), Some("X'0001'"), None)
    );
    assert_eq!(app.open_path(), Some(db.join("orders").as_path()));
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.other_tables(), ["customers", "big_orders"]);
    let notes: Vec<String> = state.notes().into_iter().map(|n| n.summary).collect();
    assert!(
        notes.iter().any(|n| n.starts_with("note: mixed types")),
        "{notes:?}"
    );

    // A view, and SQLite's own tables, by name.
    let (app, _rx) = open_with(db.clone(), table("big_orders"));
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 200 - 26);
    let (app, _rx) = open_with(db, table("sqlite_master"));
    assert_eq!(app.error_message(), None);
    assert!(frame(&app).height() >= 5);
}

#[test]
fn a_table_that_is_not_there_names_those_that_are() {
    let (app, _rx) = open_with(sqlite().join("shop.db"), table("nope"));
    let message = app.error_message().expect("refused");
    assert!(
        message.contains("No table \"nope\"") && message.contains("customers, orders, big_orders"),
        "{message}"
    );
}

/// A table named by its path inside the database opens it, as a row of the database's
/// listing does.
#[test]
fn a_path_inside_a_database_opens_its_table() {
    let (options, _) = scratch();
    let path = sqlite().join("shop.db").join("customers");
    let (app, _rx) = open_with(path.clone(), options);
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 50);
    assert_eq!(app.open_path(), Some(path.as_path()));
}

/// A database of several tables, opened without `--table`, lands on the home screen
/// inside it: its tables are the rows and its own are hidden until Ctrl+A. Enter opens
/// one; q comes back to the list.
#[test]
fn a_database_of_several_tables_lands_on_its_tables() {
    let (options, _) = scratch();
    let db = sqlite().join("shop.db");
    let (mut app, rx) = open_with(db.clone(), options);
    settle_home(&mut app, &rx);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.browsing.as_deref(), Some(db.as_path()));
    assert_eq!(listed(&app.home), ["big_orders", "customers", "orders"]);
    assert!(
        app.home
            .visible()
            .iter()
            .any(|r| matches!(r, Row::Hidden { count: 2, .. })),
        "sqlite_sequence and sqlite_master stand behind one row"
    );

    key(&mut app, &rx, KeyCode::Char('a'), KeyModifiers::CONTROL);
    assert_eq!(
        listed(&app.home),
        [
            "big_orders",
            "customers",
            "orders",
            "sqlite_master",
            "sqlite_sequence"
        ]
    );

    // The cursor lands on the first table; Enter opens it.
    let at = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, Row::Entry { entry, .. } if entry.name == "orders"))
        .unwrap();
    app.home.select(at);
    key(&mut app, &rx, KeyCode::Enter, KeyModifiers::NONE);
    assert_eq!(app.error_message(), None);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(frame(&app).height(), 200);
    assert_eq!(app.open_path(), Some(db.join("orders").as_path()));

    key(&mut app, &rx, KeyCode::Char('q'), KeyModifiers::NONE);
    settle_home(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Home);
    assert_eq!(app.home.browsing.as_deref(), Some(db.as_path()));
    assert!(matches!(
        app.home.selected_row(),
        Some(Row::Entry { entry, .. }) if entry.name == "orders"
    ));
}

/// A database whose name says nothing is known by its first bytes.
#[test]
fn a_database_is_known_by_its_first_bytes() {
    let (options, dir) = scratch();
    let copy = dir.join("readings.bin");
    std::fs::copy(sqlite().join("one.sqlite"), &copy).unwrap();
    let (app, _rx) = open_with(copy, options);
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 120);
}

#[test]
fn a_db_file_that_is_not_sqlite_says_so() {
    let (options, dir) = scratch();
    let fake = dir.join("Thumbs.db");
    std::fs::write(&fake, b"\xd0\xcf\x11\xe0 not sqlite at all").unwrap();
    let (app, _rx) = open_with(fake, options);
    let message = app.error_message().expect("refused");
    assert!(message.contains("Not a SQLite database"), "{message}");
}

/// A table is read in place: nothing is written to the temp directory while it is open,
/// and the database is left exactly as it was, with nothing written beside it.
#[test]
fn reading_a_table_leaves_the_database_alone() {
    let (options, dir) = scratch();
    let db_dir = dir.join("db");
    std::fs::create_dir_all(&db_dir).unwrap();
    let db = db_dir.join("shop.db");
    std::fs::copy(sqlite().join("shop.db"), &db).unwrap();
    let before = std::fs::read(&db).unwrap();
    let (app, _rx) = open_with(
        db.clone(),
        OpenOptions {
            table: Some("orders".to_string()),
            ..options
        },
    );
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 200);
    assert_eq!(files_in(&dir), 1, "no copy of the table");
    drop(app);
    assert_eq!(files_in(&dir), 1, "only the database's directory is left");
    assert_eq!(files_in(&db_dir), 1, "nothing beside the database");
    assert_eq!(std::fs::read(&db).unwrap(), before);
}

/// The home screen counts a database's tables, lists them when it goes inside, and
/// calls a `.db` that is not SQLite a file it cannot open.
#[test]
fn the_home_screen_counts_and_lists_tables() {
    let db = sqlite().join("shop.db");
    let mut entry = discover::Entry::for_test(&db, "shop.db");
    discover::enrich(&mut entry);
    assert_eq!(entry.cost.tables, Some(3));
    assert_eq!(entry.label(), "3 tables");

    let mut one = discover::Entry::for_test(&sqlite().join("one.sqlite"), "one.sqlite");
    discover::enrich(&mut one);
    assert_eq!(one.label(), "1 table");
    assert_eq!(one.cols, Some(4));

    let (_, dir) = scratch();
    let fake = dir.join("cache.db");
    std::fs::write(&fake, b"not a database").unwrap();
    let mut other = discover::Entry::for_test(&fake, "cache.db");
    discover::enrich(&mut other);
    assert_eq!(other.kind, EntryKind::Other);

    let mut home = HomeState {
        browsing: Some(db.clone()),
        ..HomeState::default()
    };
    home.rebuild(&[], &[]);
    assert_eq!(listed(&home), ["big_orders", "customers", "orders"]);
    let orders = home
        .sections
        .iter()
        .flat_map(|s| s.rows.iter())
        .find(|r| r.name == "orders")
        .unwrap();
    assert_eq!(orders.path, db.join("orders"));
    assert_eq!(orders.cols, Some(5));
    let preview = discover::schema_preview(orders).unwrap();
    assert_eq!(preview[2], ("amount".to_string(), DataType::Float64));

    // A recent of a table is listed as the table it names.
    let recent = discover::table_row(&db.join("big_orders")).unwrap();
    assert_eq!(recent.table.as_ref().map(|t| t.kind.as_str()), Some("view"));
    assert_eq!(recent.path, db.join("big_orders"));
    assert!(discover::table_row(&db.join("nope")).is_none());
}

/// The sidebar's sort and filters on a table run in SQLite: the frame is the table's
/// scan with no sort or filter of Polars' own over it, and holds what Polars would give.
#[test]
fn the_sidebar_sorts_and_filters_in_sqlite() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let db = sqlite().join("shop.db");
    let (mut app, _rx) = open_with(db, table("orders"));
    assert_eq!(app.error_message(), None);
    let whole = frame(&app);
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![FilterStatement {
        column: "amount".to_string(),
        operator: FilterOperator::Gt,
        value: "40".to_string(),
        logical_op: LogicalOperator::And,
    }]);
    state.sort_by(vec!["amount".to_string()], vec![true]);
    let plan = state.lf().describe_plan().unwrap();
    assert!(
        !plan.contains("SORT") && !plan.contains("FILTER"),
        "nothing left to Polars:\n{plan}"
    );
    let expected = whole
        .lazy()
        .filter(col("amount").gt(lit(40.0)))
        .sort_by_exprs(
            [col("amount")],
            SortMultipleOptions::default()
                .with_order_descending(true)
                .with_nulls_last(true)
                .with_maintain_order(true),
        )
        .collect()
        .unwrap();
    assert!(expected.height() > 0);
    assert!(frame(&app).equals_missing(&expected));
}

/// Copy as Python reads the table on screen through `sqlite3`, the one table of a
/// database opened without `--table` included, and computes datui's rows.
#[test]
fn copy_as_python_reads_the_table_on_screen() {
    for (file, options) in [("one.sqlite", scratch().0), ("shop.db", table("orders"))] {
        let (mut app, rx) = open_with(sqlite().join(file), options);
        let query = match file {
            "one.sqlite" => "select station, celsius where celsius > 10.2",
            _ => "select id, amount where amount > 50",
        };
        app.data_table_state
            .as_mut()
            .unwrap()
            .query(query.to_string());
        drain_events(&mut app, &rx);
        let script = app.python_script(app.data_table_state.as_ref().unwrap());
        assert!(script.contains("import sqlite3\n"), "{script}");
        assert!(
            script.contains("pl.read_database(\"SELECT * FROM \\\""),
            "{script}"
        );
        let Some((rows, script)) = crate::run_python_script(&app) else {
            eprintln!("skipped: no .venv to run the scripts with");
            return;
        };
        assert_eq!(rows, crate::view_csv(&app), "{file}:\n{script}");
    }
}

/// An open table's SQLite tab: the database's pages and each table of its own, from the
/// schema the open read.
#[test]
fn the_sqlite_tab_lists_the_databases_tables() {
    let (app, _rx) = open_with(sqlite().join("shop.db"), table("orders"));
    let detail = app
        .data_table_state
        .as_ref()
        .and_then(|s| s.format_detail())
        .expect("the SQLite tab");
    assert_eq!(detail.tab, "SQLite");
    assert!(
        detail.lines[0].starts_with("Page size: "),
        "{:?}",
        detail.lines
    );
    let names: Vec<&str> = detail.list.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(names, ["customers", "orders", "big_orders"]);
}

/// A database at `dir/name` built by `sql`, through Python's `sqlite3`.
fn database(dir: &Path, name: &str, sql: &str) -> PathBuf {
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    let python = if Path::new(".venv/bin/python").exists() {
        ".venv/bin/python"
    } else {
        "python3"
    };
    let status = std::process::Command::new(python)
        .args([
            "-c",
            "import sqlite3, sys; c = sqlite3.connect(sys.argv[1]); c.executescript(sys.argv[2]); c.commit()",
        ])
        .arg(&path)
        .arg(sql)
        .status()
        .expect("python runs");
    assert!(status.success());
    path
}

/// A table of no rows shows its header and says it is empty, whether its columns
/// declare a type or not, and as one of several tables.
#[test]
fn an_empty_table_shows_its_header() {
    let (options, dir) = scratch();
    let untyped = database(&dir, "untyped.db", "CREATE TABLE t(a)");
    let lines = crate::empty_table_lines(untyped, options.clone());
    assert_eq!(&lines[..3], ["a", "str", "No rows"], "{lines:#?}");

    let typed = database(&dir, "typed.db", "CREATE TABLE t(a INTEGER, b TEXT)");
    let lines = crate::empty_table_lines(typed, options.clone());
    assert_eq!(&lines[..3], ["a  b", "i64  str", "No rows"], "{lines:#?}");

    let several = database(
        &dir,
        "several.db",
        "CREATE TABLE full(a INTEGER); INSERT INTO full VALUES (1), (2); CREATE TABLE empty(b)",
    );
    let lines = crate::empty_table_lines(
        several,
        OpenOptions {
            table: Some("empty".into()),
            ..options
        },
    );
    assert_eq!(&lines[..3], ["b", "str", "No rows"], "{lines:#?}");
}

/// An untyped column with rows reads as text, as the empty one does.
#[test]
fn an_untyped_column_with_rows_is_text() {
    let (options, dir) = scratch();
    let db = database(
        &dir,
        "rows.db",
        "CREATE TABLE t(a); INSERT INTO t VALUES ('x'), ('y')",
    );
    let (app, _rx) = open_with(db, options);
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(types(&df), [("a".to_string(), DataType::String)]);
    assert_eq!(df.height(), 2);
}

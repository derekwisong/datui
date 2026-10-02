use std::path::{Path, PathBuf};

use polars::prelude::*;
use rusqlite::Connection;

use super::*;
use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

/// A database at `dir/name` built by `sql`.
fn database(dir: &Path, name: &str, sql: &str) -> PathBuf {
    let path = dir.join(name);
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(sql).unwrap();
    path
}

fn table(path: &Path, name: &str) -> (Table, Vec<Table>) {
    let all = tables(path).unwrap();
    let one = all.iter().find(|t| t.name == name).unwrap().clone();
    (one, all)
}

fn open(path: &Path, name: &str) -> Opened {
    let (one, all) = table(path, name);
    open_table(path, path, &one, &all).unwrap()
}

/// Read all of `name` of `path` in place.
fn read_table(path: &Path, name: &str) -> (DataFrame, Opened) {
    let opened = open(path, name);
    let df = opened.lf.clone().collect().unwrap();
    (df, opened)
}

fn filter(
    column: &str,
    operator: FilterOperator,
    value: &str,
    logical: LogicalOperator,
) -> FilterStatement {
    FilterStatement {
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op: logical,
    }
}

fn temp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn declared_types_follow_sqlite_s_affinity_rules() {
    for (declared, affinity) in [
        ("INTEGER", Affinity::Integer),
        ("BIGINT", Affinity::Integer),
        ("POINT", Affinity::Integer),
        ("VARCHAR(10)", Affinity::Text),
        ("CHARINT", Affinity::Integer),
        ("clob", Affinity::Text),
        ("BLOB", Affinity::Blob),
        ("", Affinity::Blob),
        ("DOUBLE PRECISION", Affinity::Real),
        ("FLOATING POINT", Affinity::Integer),
        ("DECIMAL(10,5)", Affinity::Numeric),
        ("BOOLEAN", Affinity::Numeric),
        ("DATETIME", Affinity::Numeric),
    ] {
        assert_eq!(Affinity::of(declared), affinity, "{declared}");
    }
}

#[test]
fn the_magic_and_paths_inside_a_database() {
    let dir = temp();
    let db = database(dir.path(), "app.db", "CREATE TABLE t (a);");
    assert!(is_sqlite_file(&db));
    assert!(looks_like(b"SQLite format 3\0\x10\x00"));
    assert!(!looks_like(b"SQLite format 2\0"));
    assert_eq!(
        table_path(&db.join("users")),
        Some((db.clone(), "users".to_string()))
    );
    assert_eq!(table_path(&db), None, "the database itself is a file");
    assert_eq!(
        table_path(&db.join("a/b")),
        Some((db.clone(), "a/b".to_string())),
        "a name with a slash in it"
    );
    // Any name reads back whole from the path the home screen and recents give it, one
    // that looks absolute or has empty or dotted parts included.
    for name in [
        "users",
        "/etc",
        "a//b",
        "a/./b",
        "a/../b",
        "..",
        "with \"quotes\"",
    ] {
        let place = table_place(&db, name);
        assert!(place.starts_with(&db), "{name} stays inside the database");
        assert_eq!(
            table_path(&place),
            Some((db.clone(), name.to_string())),
            "{name}"
        );
    }
    assert_eq!(
        table_path(&db.join("users/")),
        Some((db.clone(), "users".to_string()))
    );
    let text = dir.path().join("notes.db");
    std::fs::write(&text, "not a database").unwrap();
    assert_eq!(table_path(&text.join("users")), None);
    assert_eq!(table_path(&dir.path().join("missing.db").join("t")), None);
}

#[test]
fn tables_are_listed_in_order_with_sqlite_s_own_marked() {
    let dir = temp();
    let db = database(
        dir.path(),
        "app.db",
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);
         INSERT INTO users (name) VALUES ('ada');
         CREATE TABLE orders (id INTEGER, amount REAL, note);
         CREATE VIEW big AS SELECT * FROM orders WHERE amount > 10;
         CREATE VIRTUAL TABLE docs USING fts5(body);
         ANALYZE;",
    );
    let all = tables(&db).unwrap();
    let own: Vec<(&str, &str)> = all
        .iter()
        .filter(|t| !t.internal)
        .map(|t| (t.name.as_str(), t.kind.as_str()))
        .collect();
    assert_eq!(
        own,
        [
            ("users", "table"),
            ("orders", "table"),
            ("big", "view"),
            ("docs", "virtual")
        ]
    );
    let internal: Vec<&str> = all
        .iter()
        .filter(|t| t.internal)
        .map(|t| t.name.as_str())
        .collect();
    assert!(internal.contains(&"sqlite_sequence"), "{internal:?}");
    assert!(internal.contains(&"sqlite_stat1"), "{internal:?}");
    assert!(internal.contains(&"docs_data"), "fts5 shadow: {internal:?}");
    assert_eq!(internal.last(), Some(&"sqlite_master"));
    let orders = all.iter().find(|t| t.name == "orders").unwrap();
    assert_eq!(
        orders.columns,
        [
            ("id".to_string(), "INTEGER".to_string()),
            ("amount".to_string(), "REAL".to_string()),
            ("note".to_string(), String::new()),
        ]
    );
}

#[test]
fn a_database_of_one_table_opens_it_and_several_are_listed() {
    let t = |name: &str, internal: bool| Table {
        name: name.to_string(),
        kind: "table".to_string(),
        internal,
        columns: Vec::new(),
    };
    let display = Path::new("app.db");
    let one = vec![t("users", false), t("sqlite_sequence", true)];
    assert_eq!(
        pick(one.clone(), None, display).unwrap(),
        Pick::One(t("users", false))
    );
    let several = vec![t("users", false), t("Orders", false)];
    assert!(matches!(
        pick(several.clone(), None, display).unwrap(),
        Pick::Several(_)
    ));
    assert_eq!(
        pick(several.clone(), Some("orders"), display).unwrap(),
        Pick::One(t("Orders", false)),
        "SQLite's own names are not case sensitive"
    );
    assert_eq!(
        pick(one.clone(), Some("sqlite_sequence"), display).unwrap(),
        Pick::One(t("sqlite_sequence", true)),
        "its own tables open by name"
    );
    let missing = pick(several, Some("nope"), display)
        .unwrap_err()
        .to_string();
    assert_eq!(
        missing,
        "No table \"nope\" in app.db. Its tables: users, Orders."
    );
    let empty = pick(vec![t("sqlite_master", true)], None, display)
        .unwrap_err()
        .to_string();
    assert!(empty.starts_with("app.db holds no tables"), "{empty}");
}

#[test]
fn columns_take_their_declared_type_and_mixed_ones_are_read_as_text() {
    let dir = temp();
    let db = database(
        dir.path(),
        "app.db",
        "CREATE TABLE t (i INTEGER, r REAL, s TEXT, b BLOB, n NUMERIC, u, mixed, empty, nada BLOB);
         INSERT INTO t VALUES (1, 1.5, 'a', x'0102', 3, 'x', 1, NULL, NULL);
         INSERT INTO t VALUES (NULL, 2, NULL, NULL, 4.5, NULL, 'two', NULL, NULL);
         INSERT INTO t VALUES (3, NULL, 'c', x'ff', NULL, 'z', 3.5, NULL, NULL);",
    );
    let (df, opened) = read_table(&db, "t");
    let types: Vec<(&str, DataType)> = df
        .schema()
        .iter()
        .map(|(n, t)| (n.as_str(), t.clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("i", DataType::Int64),
            ("r", DataType::Float64),
            ("s", DataType::String),
            ("b", DataType::Binary),
            ("n", DataType::Float64),
            ("u", DataType::String),
            ("mixed", DataType::String),
            ("empty", DataType::String),
            ("nada", DataType::Binary),
        ]
    );
    let mixed: Vec<Option<&str>> = df.column("mixed").unwrap().str().unwrap().iter().collect();
    assert_eq!(mixed, [Some("1"), Some("two"), Some("3.5")]);
    let r: Vec<Option<f64>> = df.column("r").unwrap().f64().unwrap().iter().collect();
    assert_eq!(r, [Some(1.5), Some(2.0), None]);
    let notes = opened.pushdown.notes();
    assert_eq!(notes.len(), 1);
    assert!(
        notes[0]
            .summary
            .starts_with("mixed holds values of several types"),
        "{}",
        notes[0].summary
    );
}

/// Columns are typed from their declarations and the first rows. A value further on
/// that does not fit a number column reads as null, and the census, a pass over the
/// whole table after the open, says how many; one in a text column reads as its text.
#[test]
fn a_value_the_first_rows_did_not_show_is_noted() {
    let dir = temp();
    let db = database(
        dir.path(),
        "late.db",
        "CREATE TABLE t (id INTEGER, v, b BLOB, s TEXT);
         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000)
         INSERT INTO t SELECT x, x, x'FF00', 'text' FROM c;
         UPDATE t SET v = 'late' WHERE id = 5000;
         UPDATE t SET v = 2.5 WHERE id = 4999;
         UPDATE t SET b = 'text' WHERE id = 5000;
         UPDATE t SET s = x'00FF' WHERE id = 5000;",
    );
    let opened = open(&db, "t");
    let df = opened.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 5_000);
    let v = df.column("v").unwrap().i64().unwrap();
    assert_eq!(v.get(0), Some(1));
    assert_eq!(v.get(4_998), None, "a real in a column of whole numbers");
    assert_eq!(v.get(4_999), None, "text in it");
    let b = df.column("b").unwrap().binary().unwrap();
    assert_eq!(b.get(0), Some(&b"\xFF\x00"[..]));
    assert_eq!(
        b.get(4_999),
        Some(&b"text"[..]),
        "text in a blob column is its bytes"
    );
    let s = df.column("s").unwrap().str().unwrap();
    assert_eq!(
        s.get(4_999),
        Some("X'00FF'"),
        "a blob in a text column is its literal"
    );
    // The open has started the census in the background; taken again here, it is done.
    census_for_tests(&opened);
    let notes: Vec<String> = opened
        .pushdown
        .notes()
        .into_iter()
        .map(|n| n.summary)
        .collect();
    assert_eq!(
        notes,
        [
            "s holds values of several types and is read as text",
            "v: 2 values not whole numbers, read as null",
            "b: 1 value not blobs, read as their bytes",
        ]
    );
    // Read through what the frame shows before the census and as stored after, the
    // rows are the same.
    let after = opened.lf.clone().collect().unwrap();
    assert!(after.equals_missing(&df));
}

#[test]
fn a_view_with_repeated_or_empty_names_gets_unique_ones() {
    let dir = temp();
    let db = database(
        dir.path(),
        "v.db",
        "CREATE TABLE a (id INTEGER, x TEXT);
         INSERT INTO a VALUES (1, 'one');
         CREATE VIEW twice AS SELECT a.id, b.id, a.x AS \"\" FROM a, a AS b;",
    );
    let (df, _) = read_table(&db, "twice");
    let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
    // SQLite names a repeated column itself; an empty name is given one.
    assert_eq!(names, ["id", "id:1", "column_3"]);
}

#[test]
fn an_empty_table_keeps_its_columns() {
    let dir = temp();
    let db = database(dir.path(), "e.db", "CREATE TABLE t (a INTEGER, b TEXT);");
    let (df, _) = read_table(&db, "t");
    assert_eq!(df.height(), 0);
    assert_eq!(df.schema().get("a"), Some(&DataType::Int64));
    assert_eq!(df.schema().get("b"), Some(&DataType::String));
}

/// Nothing is written beside or into the database: its bytes and its directory are as
/// they were, for a rollback-journal database and a WAL one.
#[test]
fn reading_writes_nothing() {
    let dir = temp();
    let db = database(
        dir.path(),
        "app.db",
        "CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);",
    );
    let listing = |dir: &Path| {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    let before = std::fs::read(&db).unwrap();
    let (df, opened) = read_table(&db, "t");
    assert_eq!(df.height(), 2);
    drop(opened);
    assert_eq!(std::fs::read(&db).unwrap(), before);
    assert_eq!(listing(dir.path()), ["app.db"]);

    let wal = database(
        dir.path(),
        "wal.db",
        "PRAGMA journal_mode = WAL; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);
         PRAGMA wal_checkpoint(TRUNCATE);",
    );
    let before = std::fs::read(&wal).unwrap();
    let (df, opened) = read_table(&wal, "t");
    assert_eq!(df.height(), 1);
    drop(opened);
    assert_eq!(std::fs::read(&wal).unwrap(), before);
    assert_eq!(
        listing(dir.path()),
        ["app.db", "wal.db"],
        "the WAL files go too"
    );
    assert!(
        open_for_tests(&db)
            .unwrap()
            .execute("INSERT INTO t VALUES (3)", [])
            .is_err(),
        "the connection refuses writes"
    );
}

/// A WAL database another program is writing is read through its WAL: rows committed
/// there and not yet copied into the database are read too.
#[test]
fn a_wal_database_in_use_is_read_through_its_wal() {
    let dir = temp();
    let path = dir.path().join("live.db");
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
             CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2), (3);",
        )
        .unwrap();
    assert!(dir.path().join("live.db-wal").exists());
    let (df, _) = read_table(&path, "t");
    assert_eq!(df.height(), 3);
    drop(writer);
}

/// A WAL database in a directory datui cannot write to is read as it stands.
#[cfg(unix)]
#[test]
fn a_wal_database_in_a_read_only_directory_still_opens() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp();
    let sub = dir.path().join("ro");
    std::fs::create_dir(&sub).unwrap();
    let db = database(
        &sub,
        "wal.db",
        "PRAGMA journal_mode = WAL; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);",
    );
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o555)).unwrap();
    // Root ignores permissions, and then the ordinary open works anyway.
    let (df, _) = read_table(&db, "t");
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(df.height(), 1);
}

/// A database left mid-write, its hot journal beside it, is refused: read as it stands
/// it would give rows of a transaction that never committed. Nothing is rolled back.
#[test]
fn a_database_left_mid_write_is_refused() {
    let dir = temp();
    let path = dir.path().join("live.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (a INTEGER);
         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000)
         INSERT INTO t SELECT x FROM c;
         PRAGMA cache_size = 1;
         BEGIN; UPDATE t SET a = a + 1000000;",
    )
    .unwrap();
    // Copies taken mid-transaction are what a crash leaves: pages of the change spilled
    // into the database, and the journal that undoes them, with no writer holding it.
    let crashed = dir.path().join("crashed.db");
    let journal = dir.path().join("crashed.db-journal");
    std::fs::copy(&path, &crashed).unwrap();
    std::fs::copy(dir.path().join("live.db-journal"), &journal).unwrap();
    drop(conn);
    let before = std::fs::read(&crashed).unwrap();
    let message = format!("{:#}", tables(&crashed).unwrap_err());
    assert!(message.contains("left mid-write"), "{message}");
    assert_eq!(std::fs::read(&crashed).unwrap(), before);
    assert!(journal.exists());
}

/// A `-wal` with no `-shm` in a directory datui cannot write to is refused rather than
/// read without the WAL, which would leave out what was committed there.
#[cfg(unix)]
#[test]
fn a_wal_that_cannot_be_read_is_not_ignored() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp();
    let path = dir.path().join("live.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
         CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);",
    )
    .unwrap();
    let sub = dir.path().join("ro");
    std::fs::create_dir(&sub).unwrap();
    let copy = sub.join("live.db");
    std::fs::copy(&path, &copy).unwrap();
    std::fs::copy(dir.path().join("live.db-wal"), sub.join("live.db-wal")).unwrap();
    drop(conn);
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o555)).unwrap();
    // Root writes anyway, and then the ordinary open reads the WAL.
    let writable = std::fs::File::create(sub.join("probe")).is_ok();
    let result = tables(&copy);
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    if !writable {
        let message = format!("{:#}", result.unwrap_err());
        assert!(message.contains("-wal file"), "{message}");
    }
}

#[test]
fn an_immutable_uri_escapes_what_sqlite_would_read_as_its_own() {
    let uri = immutable_uri_for_tests(Path::new("/data/a?b#c%d é.db"));
    assert_eq!(uri, "file:///data/a%3Fb%23c%25d %C3%A9.db?immutable=1");
}

#[test]
fn a_file_that_is_not_a_database_says_so() {
    let dir = temp();
    let text = dir.path().join("notes.db");
    std::fs::write(&text, "id,name\n1,ada\n".repeat(100)).unwrap();
    let message = format!("{:#}", tables(&text).unwrap_err());
    assert!(
        message.ends_with("notes.db is not a SQLite database."),
        "{message}"
    );
}

/// Damaged files give an error rather than a panic: a header and nothing after it, a
/// file cut short, and pages of noise behind a good header.
#[test]
fn a_damaged_database_is_an_error_not_a_crash() {
    let dir = temp();
    let db = database(
        dir.path(),
        "good.db",
        "CREATE TABLE t (a INTEGER, b TEXT);
         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000)
         INSERT INTO t SELECT x, printf('row %d', x) FROM c;",
    );
    let bytes = std::fs::read(&db).unwrap();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("header only", bytes[..100].to_vec()),
        ("cut short", bytes[..bytes.len() / 2].to_vec()),
        ("noise", {
            let mut noisy = bytes.clone();
            for (i, byte) in noisy.iter_mut().enumerate().skip(100) {
                *byte = (i * 7919 % 251) as u8;
            }
            noisy
        }),
        ("noise past the schema", {
            let mut noisy = bytes.clone();
            let page = 4096;
            for (i, byte) in noisy.iter_mut().enumerate().skip(page) {
                *byte = (i * 31 % 253) as u8;
            }
            noisy
        }),
    ];
    for (what, damaged) in cases {
        let path = dir.path().join(format!("{}.db", what.replace(' ', "_")));
        std::fs::write(&path, damaged).unwrap();
        let result = std::panic::catch_unwind(|| {
            let all = tables(&path)?;
            let Some(t) = all.iter().find(|t| t.name == "t") else {
                return Ok(0);
            };
            let opened = open_table(&path, &path, t, &all)?;
            Ok::<_, color_eyre::Report>(opened.lf.collect()?.height())
        });
        assert!(result.is_ok(), "{what}: panicked");
    }
}

/// A read stops when the dataset lets go of the table (Ctrl+O, quitting), even inside
/// a statement that has not returned a row: a view that never ends. Its open gives up
/// typing it after a moment, from what it has.
#[test]
fn a_read_stops_when_the_dataset_lets_go() {
    let dir = temp();
    let db = database(
        dir.path(),
        "forever.db",
        "CREATE VIEW forever AS
           WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c)
           SELECT max(x) AS m FROM c;",
    );
    let Opened { lf, hold, .. } = open(&db, "forever");
    let reader = std::thread::spawn(move || lf.collect());
    std::thread::sleep(std::time::Duration::from_millis(200));
    drop(hold);
    let result = reader.join().unwrap();
    let message = result.unwrap_err().to_string();
    assert!(message.contains("was stopped"), "{message}");
}

/// The home screen's preview of a view that never gives a row gives up.
#[test]
fn a_preview_of_a_view_that_never_ends_gives_up() {
    let dir = temp();
    let db = database(
        dir.path(),
        "forever.db",
        "CREATE VIEW forever AS
           WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c)
           SELECT max(x) FROM c;",
    );
    let (one, _) = table(&db, "forever");
    assert_eq!(schema_preview(&db, &one), None);
}

#[test]
fn the_preview_reads_the_columns_as_the_open_would() {
    let dir = temp();
    let db = database(
        dir.path(),
        "p.db",
        "CREATE TABLE t (i INTEGER, n NUMERIC, u);
         INSERT INTO t VALUES (1, 2, 'x');",
    );
    let (one, _) = table(&db, "t");
    let preview = schema_preview(&db, &one).unwrap();
    assert_eq!(
        preview,
        [
            ("i".to_string(), DataType::Int64),
            ("n".to_string(), DataType::Int64),
            ("u".to_string(), DataType::String),
        ]
    );
}

#[test]
fn the_other_tables_are_named_for_the_info_panel() {
    let dir = temp();
    let db = database(
        dir.path(),
        "o.db",
        "CREATE TABLE a (x); CREATE TABLE b (x); CREATE TABLE c (x);",
    );
    let (_, opened) = read_table(&db, "b");
    assert_eq!(opened.other_tables, ["a", "c"]);
}

/// Names from the file are quoted wherever they go into a statement: each of these
/// reads its own table, and none reads another or runs anything.
#[test]
fn hostile_table_names_read_their_own_table() {
    let dir = temp();
    let names = [
        "a\"b",
        "\"; DROP TABLE t; --",
        "x]y",
        "`tick`",
        "new\nline",
        "it's",
        "main.t",
    ];
    let conn = Connection::open(dir.path().join("odd.db")).unwrap();
    conn.execute_batch("CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('t');")
        .unwrap();
    for (i, name) in names.iter().enumerate() {
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        conn.execute_batch(&format!(
            "CREATE TABLE {quoted} (v TEXT); INSERT INTO {quoted} VALUES ('{i}');"
        ))
        .unwrap();
    }
    drop(conn);
    let db = dir.path().join("odd.db");
    for (i, name) in names.iter().enumerate() {
        let (df, _) = read_table(&db, name);
        let v: Vec<Option<&str>> = df.column("v").unwrap().str().unwrap().iter().collect();
        assert_eq!(v, [Some(i.to_string().as_str())], "{name:?}");
    }
    let (df, _) = read_table(&db, "t");
    assert_eq!(df.height(), 1, "t is still there");
    // A name with a NUL in it, which no statement can name, written into the schema:
    // SQLite finds the schema malformed, and says so.
    let nul = database(
        dir.path(),
        "nul.db",
        "CREATE TABLE nul (v TEXT);
         PRAGMA writable_schema = ON;
         UPDATE sqlite_schema SET name = 'n' || char(0) || 'ul' WHERE name = 'nul';
         PRAGMA writable_schema = OFF;",
    );
    assert!(tables(&nul).is_err());
}

/// A window is read straight from SQLite, deep or shallow, and agrees with the whole
/// table read in order; once the count is known, a window past the middle is read from
/// the end backward.
#[test]
fn windows_are_read_in_place() {
    let dir = temp();
    let db = database(
        dir.path(),
        "w.db",
        "CREATE TABLE t (id INTEGER, label TEXT);
         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10000)
         INSERT INTO t SELECT x * 3, 'row ' || x FROM c;
         DELETE FROM t WHERE id % 7 = 0;",
    );
    let (whole, opened) = read_table(&db, "t");
    let view = opened.pushdown.view(&[], &[], false).unwrap();
    let rows = whole.height();
    let window =
        |start: usize, len: usize| view.window.window(start, len).unwrap().collect().unwrap();
    let expected = |start: usize, len: usize| whole.slice(start as i64, len);
    // From the top, paging on, a jump, and before the count is known.
    for (start, len) in [(0, 50), (50, 50), (100, 50), (5_000, 20), (rows - 10, 50)] {
        assert!(
            window(start, len).equals_missing(&expected(start, len)),
            "{start}"
        );
    }
    assert_eq!((view.counter)().unwrap(), rows);
    // Past the middle with the count known: read backward from the end.
    for (start, len) in [(rows - 30, 30), (rows - 1, 5), (rows / 2 + 1, 40)] {
        assert!(
            window(start, len).equals_missing(&expected(start, len)),
            "{start}"
        );
    }
    assert_eq!(window(rows, 10).height(), 0, "past the end");
}

/// The sidebar's filters and sort run in SQLite and give the rows Polars gives for the
/// same filters and sort over the same data, windows and counts included; mixed
/// columns, nulls and ties included.
#[test]
fn filters_and_sorts_run_in_sqlite_as_polars_would() {
    let dir = temp();
    let db = database(
        dir.path(),
        "f.db",
        "CREATE TABLE t (n INTEGER, x REAL, s TEXT COLLATE NOCASE, u);
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 3000)
         INSERT INTO t SELECT
           CASE WHEN i % 11 = 0 THEN NULL ELSE i % 97 END,
           CASE WHEN i % 13 = 0 THEN NULL ELSE (i % 50) / 4.0 END,
           CASE i % 5 WHEN 0 THEN 'Apple' WHEN 1 THEN 'apple' WHEN 2 THEN 'Banana'
                      WHEN 3 THEN NULL ELSE 'cherry pie' END,
           CASE WHEN i % 2 = 0 THEN i ELSE 'v' || i END
         FROM c;
         UPDATE t SET n = 'not a number' WHERE rowid = 1500;",
    );
    let (whole, opened) = read_table(&db, "t");
    use FilterOperator::*;
    use LogicalOperator::*;
    type Case = (Vec<FilterStatement>, Vec<(String, bool)>);
    let cases: Vec<Case> = vec![
        (vec![filter("n", Gt, "50", And)], vec![]),
        (
            vec![filter("n", LtEq, "10", And), filter("s", Eq, "apple", Or)],
            vec![("x".to_string(), true)],
        ),
        (
            vec![
                filter("s", Contains, "pp", And),
                filter("x", GtEq, "5.5", And),
            ],
            vec![("s".to_string(), false), ("n".to_string(), true)],
        ),
        (
            vec![filter("s", NotContains, "a", And)],
            vec![("n".to_string(), false)],
        ),
        (
            vec![filter("u", Lt, "v2", And)],
            vec![("u".to_string(), true)],
        ),
        (vec![filter("s", NotEq, "Apple", And)], vec![]),
        (
            vec![],
            vec![("s".to_string(), true), ("x".to_string(), false)],
        ),
    ];
    for (filters, sort) in cases {
        let view = opened.pushdown.view(&filters, &sort, false).unwrap();
        // What Polars makes of the same, as the sidebar builds it.
        let mut predicate: Option<Expr> = None;
        for f in &filters {
            let value = match whole.schema().get(f.column.as_str()).unwrap() {
                DataType::Int64 => lit(f.value.parse::<i64>().unwrap()),
                DataType::Float64 => lit(f.value.parse::<f64>().unwrap()),
                _ => lit(f.value.clone()),
            };
            let c = col(f.column.as_str());
            let atom = match f.operator {
                Eq => c.eq(value),
                NotEq => c.neq(value),
                Gt => c.gt(value),
                Lt => c.lt(value),
                GtEq => c.gt_eq(value),
                LtEq => c.lt_eq(value),
                Contains => c.str().contains_literal(lit(f.value.clone())),
                NotContains => c.str().contains_literal(lit(f.value.clone())).not(),
            };
            predicate = Some(match (predicate, f.logical_op) {
                (None, _) => atom,
                (Some(p), And) => p.and(atom),
                (Some(p), Or) => p.or(atom),
            });
        }
        let mut lf = whole.clone().lazy();
        if let Some(p) = predicate {
            lf = lf.filter(p);
        }
        if !sort.is_empty() {
            lf = lf.sort_by_exprs(
                sort.iter()
                    .map(|(c, _)| col(c.as_str()))
                    .collect::<Vec<_>>(),
                SortMultipleOptions::default()
                    .with_order_descending_multi(sort.iter().map(|(_, d)| *d))
                    .with_nulls_last(true)
                    .with_maintain_order(true),
            );
        }
        let expected = lf.collect().unwrap();
        let got = view.lf.clone().collect().unwrap();
        assert!(
            got.equals_missing(&expected),
            "{filters:?} {sort:?}\n{got}\n{expected}"
        );
        assert_eq!((view.counter)().unwrap(), expected.height());
        let start = expected.height() / 3;
        let window = view.window.window(start, 25).unwrap().collect().unwrap();
        assert!(window.equals_missing(&expected.slice(start as i64, 25)));
        let start = expected.height().saturating_sub(7);
        let window = view.window.window(start, 25).unwrap().collect().unwrap();
        assert!(
            window.equals_missing(&expected.slice(start as i64, 25)),
            "the end"
        );
    }
    // The table's order backward.
    let back = opened.pushdown.view(&[], &[], true).unwrap();
    let got = back.lf.collect().unwrap();
    assert!(got.equals_missing(&whole.reverse()));
    // Polars would refuse these, and so are left to it.
    assert!(
        opened
            .pushdown
            .view(&[filter("n", Gt, "abc", And)], &[], false)
            .is_none()
    );
    assert!(
        opened
            .pushdown
            .view(&[filter("n", Contains, "1", And)], &[], false)
            .is_none()
    );
}

/// Text in a column that declares a number (a `DATETIME`, an `INT` holding words)
/// compares as text, as Polars compares it, though the value looks like a number.
#[test]
fn text_in_a_number_column_compares_as_text() {
    let dir = temp();
    let db = database(
        dir.path(),
        "d.db",
        "CREATE TABLE t (d DATETIME, w INT);
         INSERT INTO t VALUES ('2023-05-01', '-x'), ('2025-01-01', 'abc'), (NULL, NULL);",
    );
    let (whole, opened) = read_table(&db, "t");
    assert_eq!(whole.schema().get("d"), Some(&DataType::String));
    use FilterOperator::*;
    use LogicalOperator::*;
    let cases = [
        (filter("d", Lt, "2024", And), col("d").lt(lit("2024"))),
        (filter("d", Gt, "2024", And), col("d").gt(lit("2024"))),
        (filter("w", Gt, "10", And), col("w").gt(lit("10"))),
    ];
    // Read through what the frame shows, then as stored once the census finds the
    // columns clean.
    for census in [false, true] {
        if census {
            census_for_tests(&opened);
        }
        for (f, predicate) in &cases {
            let expected = whole
                .clone()
                .lazy()
                .filter(predicate.clone())
                .collect()
                .unwrap();
            let view = opened
                .pushdown
                .view(std::slice::from_ref(f), &[], false)
                .unwrap();
            let got = view.lf.collect().unwrap();
            assert!(got.equals_missing(&expected), "{f:?} {census}\n{got}");
            let pushed = opened
                .lf
                .clone()
                .filter(predicate.clone())
                .collect()
                .unwrap();
            assert!(pushed.equals_missing(&expected), "{predicate:?} {census}");
        }
    }
}

/// Once the census finds a column clean, a filter or sort on it reads it as stored, so
/// SQLite uses an index on it rather than sorting the table.
#[test]
fn an_index_serves_a_clean_column() {
    let dir = temp();
    let db = database(
        dir.path(),
        "i.db",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, price REAL, name TEXT);
         CREATE INDEX t_price ON t (price);
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 500)
         INSERT INTO t SELECT i, i * 1.5, 'n' || i FROM c;",
    );
    let opened = open(&db, "t");
    let filters = [filter(
        "price",
        FilterOperator::Gt,
        "100",
        LogicalOperator::And,
    )];
    let sort = [("price".to_string(), false)];
    let (_, before) = plan_for_tests(&opened, &filters, &sort);
    assert!(!before.contains("t_price"), "{before}");
    census_for_tests(&opened);
    let (sql, after) = plan_for_tests(&opened, &filters, &sort);
    assert!(after.contains("USING INDEX t_price"), "{sql}\n{after}");
    assert!(!after.contains("TEMP B-TREE"), "{after}");
}

/// A filter Polars pushes into the scan (a query's) runs in SQLite where it can, and
/// what it cannot say is applied by Polars all the same.
#[test]
fn a_predicate_polars_pushes_is_kept_whole() {
    let dir = temp();
    let db = database(
        dir.path(),
        "p.db",
        "CREATE TABLE t (a INTEGER, b TEXT);
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 1000)
         INSERT INTO t SELECT i, 'b' || (i % 10) FROM c;",
    );
    let (whole, opened) = read_table(&db, "t");
    let predicates = [
        col("a").gt(lit(990)),
        lit(5).gt_eq(col("a")),
        col("a").lt(lit(100)).and(col("b").eq(lit("b3"))),
        col("a")
            .gt(lit(995))
            .or(col("b").str().contains_literal(lit("b0"))),
        col("b")
            .str()
            .contains_literal(lit("7"))
            .and(col("a").lt(lit(50))),
    ];
    for predicate in predicates {
        let got = opened
            .lf
            .clone()
            .filter(predicate.clone())
            .collect()
            .unwrap();
        let expected = whole
            .clone()
            .lazy()
            .filter(predicate.clone())
            .collect()
            .unwrap();
        assert!(got.equals_missing(&expected), "{predicate:?}");
    }
    let head = opened
        .lf
        .clone()
        .filter(col("a").gt(lit(10)))
        .limit(3)
        .collect()
        .unwrap();
    assert_eq!(
        head.column("a").unwrap().i64().unwrap().to_vec(),
        [Some(11), Some(12), Some(13)]
    );
}

/// A table without rowids is paged by its primary key; a view, which has no order of
/// its own, by offset, and is not read backward; a column named `rowid` does not take
/// the place of the key.
#[test]
fn tables_without_rowids_views_and_a_column_named_rowid() {
    let dir = temp();
    let db = database(
        dir.path(),
        "k.db",
        "CREATE TABLE pk (a TEXT, b INTEGER, v, PRIMARY KEY (a, b)) WITHOUT ROWID;
         WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < 400)
         INSERT INTO pk SELECT 'k' || (i % 4), i, i * 2 FROM c;
         CREATE VIEW v AS SELECT b, v FROM pk WHERE b % 2 = 0;
         CREATE TABLE r (rowid TEXT, x INTEGER);
         INSERT INTO r VALUES ('z', 1), ('a', 2), ('m', 3);",
    );
    for name in ["pk", "v"] {
        let (whole, opened) = read_table(&db, name);
        let view = opened.pushdown.view(&[], &[], false).unwrap();
        for (start, len) in [(0, 10), (10, 10), (150, 30), (whole.height() - 3, 10)] {
            let got = view.window.window(start, len).unwrap().collect().unwrap();
            assert!(
                got.equals_missing(&whole.slice(start as i64, len)),
                "{name} {start}"
            );
        }
        assert_eq!(
            opened.pushdown.view(&[], &[], true).is_some(),
            name == "pk",
            "{name} backward"
        );
    }
    let (whole, _) = read_table(&db, "pk");
    let a = whole.column("a").unwrap().str().unwrap();
    assert_eq!(a.get(0), Some("k0"), "in key order");
    let (r, opened) = read_table(&db, "r");
    assert_eq!(r.column("rowid").unwrap().str().unwrap().get(0), Some("z"));
    let back = opened
        .pushdown
        .view(&[], &[], true)
        .unwrap()
        .lf
        .collect()
        .unwrap();
    assert_eq!(
        back.column("x").unwrap().i64().unwrap().to_vec(),
        [Some(3), Some(2), Some(1)]
    );
}

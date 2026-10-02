use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use polars::prelude::*;
use rusqlite::Connection;

use super::*;
use crate::OpenOptions;
use crate::unfinished::{Unfinished, Writer};

/// A database at `dir/name` built by `sql`.
fn database(dir: &Path, name: &str, sql: &str) -> PathBuf {
    let path = dir.join(name);
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(sql).unwrap();
    path
}

fn writer() -> (Writer, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    (Unfinished::default().writer(stop.clone()), stop)
}

fn table(path: &Path, name: &str) -> (Table, Vec<Table>) {
    let all = tables(path).unwrap();
    let one = all.iter().find(|t| t.name == name).unwrap().clone();
    (one, all)
}

/// Read `name` of `path` to a frame, with the temp files it scans in `dir`.
fn read_table(path: &Path, name: &str, dir: &Path) -> (DataFrame, crate::segments::Converted) {
    let (one, all) = table(path, name);
    let (writer, _) = writer();
    let options = OpenOptions {
        temp_dir: Some(dir.to_path_buf()),
        ..OpenOptions::default()
    };
    let read = AtomicU64::new(0);
    let converted = convert(path, path, &one, &all, &options, &writer, &read).unwrap();
    let df = converted.lf.clone().collect().unwrap();
    (df, converted)
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
    let (df, converted) = read_table(&db, "t", dir.path());
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
    assert_eq!(converted.notes.len(), 1);
    assert!(
        converted.notes[0]
            .summary
            .starts_with("mixed holds values of several types"),
        "{}",
        converted.notes[0].summary
    );
}

/// A column that turns out wider after its first batch was written is read as the
/// wider type throughout: the batches written before are cast to it.
#[test]
fn a_column_that_widens_late_is_wide_throughout() {
    let dir = temp();
    let db = database(
        dir.path(),
        "late.db",
        "CREATE TABLE t (id INTEGER, v, w);
         WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 70000)
         INSERT INTO t SELECT x, x, NULL FROM c;
         UPDATE t SET v = 'late' WHERE id = 70000;
         UPDATE t SET w = 2.5 WHERE id = 69999;",
    );
    let (df, converted) = read_table(&db, "t", dir.path());
    assert_eq!(df.height(), 70_000);
    assert!(converted.files.len() > 1, "more than one segment");
    let v = df.column("v").unwrap();
    assert_eq!(v.dtype(), &DataType::String);
    assert_eq!(v.str().unwrap().get(0), Some("1"));
    assert_eq!(v.str().unwrap().get(69_999), Some("late"));
    let w = df.column("w").unwrap();
    assert_eq!(w.dtype(), &DataType::Float64, "nulls first, then a float");
    assert_eq!(w.f64().unwrap().get(69_998), Some(2.5));
    assert_eq!(w.null_count(), 69_999);
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
    let (df, _) = read_table(&db, "twice", dir.path());
    let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
    // SQLite names a repeated column itself; an empty name is given one.
    assert_eq!(names, ["id", "id:1", "column_3"]);
}

#[test]
fn an_empty_table_keeps_its_columns() {
    let dir = temp();
    let db = database(dir.path(), "e.db", "CREATE TABLE t (a INTEGER, b TEXT);");
    let (df, _) = read_table(&db, "t", dir.path());
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
    let scratch = temp();
    let before = std::fs::read(&db).unwrap();
    let (df, converted) = read_table(&db, "t", scratch.path());
    assert_eq!(df.height(), 2);
    drop(converted);
    assert_eq!(std::fs::read(&db).unwrap(), before);
    assert_eq!(listing(dir.path()), ["app.db"]);

    let wal = database(
        dir.path(),
        "wal.db",
        "PRAGMA journal_mode = WAL; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);
         PRAGMA wal_checkpoint(TRUNCATE);",
    );
    let before = std::fs::read(&wal).unwrap();
    let (df, converted) = read_table(&wal, "t", scratch.path());
    assert_eq!(df.height(), 1);
    drop(converted);
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
    let scratch = temp();
    let (df, _) = read_table(&path, "t", scratch.path());
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
    let scratch = temp();
    let (df, _) = read_table(&db, "t", scratch.path());
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(df.height(), 1);
}

#[test]
fn an_immutable_uri_escapes_what_sqlite_would_read_as_its_own() {
    let uri = immutable_uri_for_tests(Path::new("/data/a?b#c%d é.db"));
    assert_eq!(uri, "file:/data/a%3Fb%23c%25d %C3%A9.db?immutable=1");
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
            let (writer, _) = writer();
            let options = OpenOptions {
                temp_dir: Some(dir.path().to_path_buf()),
                ..OpenOptions::default()
            };
            let converted = convert(&path, &path, t, &all, &options, &writer, &AtomicU64::new(0))?;
            Ok::<_, color_eyre::Report>(converted.lf.collect()?.height())
        });
        assert!(result.is_ok(), "{what}: panicked");
    }
}

/// A read stopped (Ctrl+O, quitting) gives up, even inside a statement that has not
/// returned a row: a view that never ends.
#[test]
fn a_stopped_read_gives_up() {
    let dir = temp();
    let db = database(
        dir.path(),
        "forever.db",
        "CREATE VIEW forever AS
           WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c)
           SELECT max(x) FROM c;",
    );
    let (one, all) = table(&db, "forever");
    let (writer, stop) = writer();
    let stopper = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        })
    };
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let result = convert(&db, &db, &one, &all, &options, &writer, &AtomicU64::new(0));
    stopper.join().unwrap();
    assert!(result.is_err());
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
    let (_, converted) = read_table(&db, "b", dir.path());
    assert_eq!(converted.other_tables, ["a", "c"]);
}

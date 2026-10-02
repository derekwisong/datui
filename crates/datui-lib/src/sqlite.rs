//! SQLite databases, read only.
//!
//! A database is a file of tables. One with a single table of its own opens it; one
//! with several is a place on the home screen whose rows are its tables, each named by
//! a path inside the file (`app.db/users`) that nothing on disk has. `--table` picks
//! one by name either way.
//!
//! A table is read once, start to end, into temporary Arrow IPC files a batch at a
//! time (see [`crate::segments`]), which the dataset scans lazily and holds: SQLite has
//! no reader Polars can push a query into, and a scan that re-read the table for every
//! page would hold the whole of it to sort or filter it.
//!
//! Nothing is written to the database. It is opened read only, with `query_only`,
//! defensive mode and an untrusted schema, and with extension loading left out of the
//! build. Reading a table runs no trigger. A database in WAL mode with a `-wal` file is
//! read through it, and SQLite creates the `-shm` index beside it if that is missing; one
//! with no `-wal` is read as it stands (`immutable=1`), writing nothing. A database that
//! cannot be read without writing beside it (a hot journal, or a `-wal` without its
//! `-shm` in a read-only directory) is refused rather than read wrong.

use std::path::{Path, PathBuf};

/// The first sixteen bytes of every SQLite 3 database.
pub const MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Whether `head`, the first bytes of a file, begins a SQLite database.
pub fn looks_like(head: &[u8]) -> bool {
    head.starts_with(MAGIC)
}

/// Whether the file at `path` is a SQLite database, by its first bytes.
pub fn is_sqlite_file(path: &Path) -> bool {
    use std::io::Read;
    let mut head = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut head))
        .is_ok()
        && looks_like(&head)
}

/// The path of `table` inside the database `db`, as the home screen lists it and recents
/// record it: `app.db/users`. The name is appended as it is, so a name that looks like a
/// path (`/etc`, `a/../b`) stays inside the database and reads back whole.
pub fn table_place(db: &Path, table: &str) -> PathBuf {
    let mut place = db.as_os_str().to_owned();
    place.push("/");
    place.push(table);
    place.into()
}

/// The database and the table a path inside a database names, the inverse of
/// [`table_place`]: `app.db/users` is the table `users` of `app.db`, and `app.db/a/b`
/// the table `a/b`. `None` for a path that is there, or that is inside no SQLite file.
pub fn table_path(path: &Path) -> Option<(PathBuf, String)> {
    if path.exists() {
        return None;
    }
    let db = path
        .ancestors()
        .skip(1)
        .take_while(|p| !p.as_os_str().is_empty())
        .find(|p| p.is_file())?;
    if !is_sqlite_file(db) {
        return None;
    }
    // What follows the database's name, less the one separator after it, as written:
    // a parent is a prefix of the path's own text.
    let rest = path.to_str()?.strip_prefix(db.to_str()?)?;
    let mut chars = rest.chars();
    chars.next().filter(|c| std::path::is_separator(*c))?;
    let table = chars.as_str().trim_end_matches(std::path::is_separator);
    (!table.is_empty()).then(|| (db.to_path_buf(), table.to_string()))
}

/// Whether a table is SQLite's own: the schema, `sqlite_sequence`, the statistics
/// tables, or the shadow tables a virtual table keeps its data in.
#[cfg(feature = "sqlite")]
fn is_internal(name: &str, kind: &str) -> bool {
    name.get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("sqlite_"))
        || kind == "shadow"
}

/// One table of a database, as its schema describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    /// `table`, `view`, `virtual` or `shadow`.
    pub kind: String,
    /// SQLite's own, hidden on the home screen until Ctrl+A.
    pub internal: bool,
    /// Each column's name and declared type (empty where none was declared).
    pub columns: Vec<(String, String)>,
}

/// How SQLite reads a declared type: the affinity rules of its documentation, in
/// order, so `VARCHAR(10)` is text and `POINT` (holding "INT") an integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    Integer,
    Text,
    /// Declared `BLOB`, or nothing at all: values are stored as they come.
    Blob,
    Real,
    Numeric,
}

impl Affinity {
    pub fn of(declared: &str) -> Self {
        let upper = declared.to_ascii_uppercase();
        if upper.contains("INT") {
            Self::Integer
        } else if ["CHAR", "CLOB", "TEXT"].iter().any(|t| upper.contains(t)) {
            Self::Text
        } else if upper.contains("BLOB") || upper.trim().is_empty() {
            Self::Blob
        } else if ["REAL", "FLOA", "DOUB"].iter().any(|t| upper.contains(t)) {
            Self::Real
        } else {
            Self::Numeric
        }
    }

    /// The type a column of this affinity is read as before any value says otherwise,
    /// and when every value is null: `None` for a column whose values decide.
    #[cfg(feature = "sqlite")]
    fn dtype(self, declared: &str) -> Option<polars::prelude::DataType> {
        use polars::prelude::DataType;
        match self {
            Self::Integer => Some(DataType::Int64),
            Self::Real => Some(DataType::Float64),
            Self::Text => Some(DataType::String),
            Self::Blob if !declared.trim().is_empty() => Some(DataType::Binary),
            Self::Blob | Self::Numeric => None,
        }
    }
}

/// What opening a database without `--table` finds: its one table of its own, or the
/// names of several, which the home screen lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    One(Table),
    Several(Vec<Table>),
}

/// The table `wanted` names among `tables`, or the database's one table of its own
/// when nothing is named. `display` names the database in errors.
pub fn pick(tables: Vec<Table>, wanted: Option<&str>, display: &Path) -> color_eyre::Result<Pick> {
    use color_eyre::eyre::eyre;
    let own: Vec<&Table> = tables.iter().filter(|t| !t.internal).collect();
    let names = || {
        const SHOWN: usize = 20;
        let mut names: Vec<&str> = own.iter().take(SHOWN).map(|t| t.name.as_str()).collect();
        let more = own.len().saturating_sub(SHOWN);
        let more = format!("and {more} more");
        if own.len() > SHOWN {
            names.push(&more);
        }
        names.join(", ")
    };
    if let Some(wanted) = wanted {
        // Exactly as written first; SQLite itself matches names without regard to case.
        let found = tables.iter().find(|t| t.name == wanted).or_else(|| {
            let mut alike = tables
                .iter()
                .filter(|t| t.name.eq_ignore_ascii_case(wanted));
            match (alike.next(), alike.next()) {
                (Some(one), None) => Some(one),
                _ => None,
            }
        });
        return match found {
            Some(table) => Ok(Pick::One(table.clone())),
            None if own.is_empty() => Err(eyre!(
                "No table {wanted:?} in {}, which holds no tables.",
                display.display()
            )),
            None => Err(eyre!(
                "No table {wanted:?} in {}. Its tables: {}.",
                display.display(),
                names()
            )),
        };
    }
    match own.as_slice() {
        [] => Err(eyre!(
            "{} holds no tables. --table sqlite_master shows its schema.",
            display.display()
        )),
        [one] => Ok(Pick::One((*one).clone())),
        _ => Ok(Pick::Several(tables)),
    }
}

#[cfg(feature = "sqlite")]
pub use read::*;

#[cfg(not(feature = "sqlite"))]
pub use unsupported::*;

#[cfg(not(feature = "sqlite"))]
mod unsupported {
    use std::path::Path;
    use std::sync::atomic::AtomicU64;

    use color_eyre::Result;
    use color_eyre::eyre::eyre;

    use super::Table;
    use crate::OpenOptions;
    use crate::segments::Converted;
    use crate::unfinished::Writer;

    fn refused() -> color_eyre::Report {
        eyre!(
            "This build of datui reads no SQLite databases: it was built without the sqlite feature."
        )
    }

    pub fn tables(_path: &Path) -> Result<Vec<Table>> {
        Err(refused())
    }

    pub(crate) fn estimate_rows(_path: &Path, _table: &Table) -> u64 {
        0
    }

    pub fn schema_preview(_path: &Path, _table: &Table) -> Option<crate::discover::SchemaPreview> {
        None
    }

    pub(crate) fn convert(
        _file: &Path,
        _display: &Path,
        _table: &Table,
        _others: &[Table],
        _options: &OpenOptions,
        _writer: &Writer,
        _read: &AtomicU64,
    ) -> Result<Converted> {
        Err(refused())
    }
}

#[cfg(feature = "sqlite")]
mod read {
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use color_eyre::Result;
    use color_eyre::eyre::eyre;
    use polars::prelude::*;
    use rusqlite::config::DbConfig;
    use rusqlite::types::ValueRef;
    use rusqlite::{Connection, OpenFlags};

    use super::{Affinity, Table, is_internal};
    use crate::OpenOptions;
    use crate::notes::Note;
    use crate::numfmt::group_chrome;
    use crate::segments::{Converted, Segments, blob_text};
    use crate::unfinished::Writer;

    /// How long a read waits on a writer's lock before it gives up.
    const BUSY: Duration = Duration::from_secs(2);
    /// Rows in a batch, at most.
    const BATCH_ROWS: usize = 1 << 16;
    /// Cells in a batch, at most, so a table of a thousand columns holds as much at once
    /// as one of sixty.
    const BATCH_CELLS: usize = 1 << 22;
    /// Text and blob bytes in a batch before it is written, however few its rows.
    const BATCH_BYTES: usize = 64 << 20;
    /// SQLite steps between looks at the stop flag, for a statement that works a long
    /// time before its first row (a view that sorts).
    const STEPS_PER_LOOK: i32 = 100_000;
    /// The longest a preview reads before it gives up.
    const PREVIEW_TIME: Duration = Duration::from_secs(2);
    /// Tables whose columns a listing reads; past this they are listed by name.
    const MAX_DESCRIBED: usize = 1000;

    /// Open the database at `path` read only, hardened against what is in it.
    ///
    /// A WAL database is read through its WAL when it has one, as another program may
    /// be writing it. One with no `-wal` beside it has nothing there to read, and is
    /// read as it stands (`immutable=1`): opened the ordinary way, SQLite would create
    /// the `-wal` and `-shm` files, and a read-only connection cannot remove them again.
    /// Immutable takes no lock, so a program that starts writing it mid-read can make
    /// the read fail or come out wrong; with no `-wal`, nothing was writing it a moment
    /// ago. Immutable is never used where a `-wal` or a hot `-journal` is beside the
    /// file, which it would ignore.
    fn open(path: &Path) -> Result<Connection> {
        let not_a_database = |e: rusqlite::Error| match e.sqlite_error_code() {
            Some(rusqlite::ErrorCode::NotADatabase) => {
                eyre!("{} is not a SQLite database.", path.display())
            }
            _ => color_eyre::Report::new(e),
        };
        if !(is_wal(path) && !beside(path, "-wal").exists()) {
            match plain(path) {
                Ok(conn) => return Ok(conn),
                // A journal left by a writer that stopped mid-write, which a reader may
                // not roll back: the file holds half a transaction, and read as it
                // stands it would give rows that were never committed together.
                Err(e) if cannot_open(&e) && beside(path, "-journal").exists() => {
                    return Err(eyre!(
                        "{} was left mid-write by a program that stopped: its -journal has to be rolled back first, which datui does not do. Opening it once with the sqlite3 tool rolls it back.",
                        path.display()
                    ));
                }
                // A WAL whose index (`-shm`) is missing and cannot be made here (a
                // read-only directory). Read without it, the database would lack what
                // was committed to the WAL.
                Err(e) if cannot_open(&e) && beside(path, "-wal").exists() => {
                    return Err(eyre!(
                        "{} has a -wal file that cannot be read from here without a -shm file beside it, and its directory is read only. Copy the database and its -wal to a writable directory.",
                        path.display()
                    ));
                }
                // A file in a directory datui cannot write to, with nothing beside it.
                Err(e) if cannot_open(&e) => {}
                Err(e) => return Err(not_a_database(e)),
            }
        }
        Connection::open_with_flags(immutable_uri(path), FLAGS | OpenFlags::SQLITE_OPEN_URI)
            .and_then(check)
            .map_err(not_a_database)
    }

    const FLAGS: OpenFlags =
        OpenFlags::SQLITE_OPEN_READ_ONLY.union(OpenFlags::SQLITE_OPEN_NO_MUTEX);

    fn plain(path: &Path) -> rusqlite::Result<Connection> {
        check(Connection::open_with_flags(path, FLAGS)?)
    }

    /// Whether the database's header says it is in WAL mode.
    fn is_wal(path: &Path) -> bool {
        use std::io::Read;
        let mut header = [0u8; 20];
        std::fs::File::open(path)
            .and_then(|mut f| f.read_exact(&mut header))
            .is_ok()
            && header[18] == 2
            && header[19] == 2
    }

    /// The file SQLite keeps beside `path` with `suffix`: `app.db-wal`.
    fn beside(path: &Path, suffix: &str) -> std::path::PathBuf {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        name.into()
    }

    fn cannot_open(e: &rusqlite::Error) -> bool {
        matches!(
            e.sqlite_error_code(),
            Some(rusqlite::ErrorCode::CannotOpen | rusqlite::ErrorCode::ReadOnly)
        )
    }

    /// Harden a connection and make SQLite read the file's header and schema now, so a
    /// file that is not a database says so here, and one that cannot be read the
    /// ordinary way is caught where there is another way to read it.
    fn check(conn: Connection) -> rusqlite::Result<Connection> {
        conn.busy_timeout(BUSY)?;
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)?;
        // Neither runs on a read; off all the same, as nothing here needs them.
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, false)?;
        conn.pragma_update(None, "query_only", true)?;
        conn.query_row("SELECT count(*) FROM main.sqlite_schema", [], |_| Ok(()))?;
        Ok(conn)
    }

    /// A `file:` URI for `path` that reads it as immutable: no lock, no WAL.
    fn immutable_uri(path: &Path) -> String {
        let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        let text = absolute.to_string_lossy().replace('\\', "/");
        // A Windows drive path is `file:/C:/...`.
        let mut uri = String::from(if text.starts_with('/') {
            "file:"
        } else {
            "file:/"
        });
        for byte in text.bytes() {
            match byte {
                b'?' | b'#' | b'%' | 0x80.. => uri.push_str(&format!("%{byte:02X}")),
                _ => uri.push(char::from(byte)),
            }
        }
        uri.push_str("?immutable=1");
        uri
    }

    /// A name as an SQL identifier.
    fn quoted(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// The tables and views of the database at `path`, SQLite's own included, in the
    /// order they were created; the schema table last.
    pub fn tables(path: &Path) -> Result<Vec<Table>> {
        let conn = open(path)?;
        // What kind each is, where SQLite says (shadow tables of a virtual table);
        // a schema SQLite cannot describe falls back to the schema table alone.
        let kinds: std::collections::HashMap<String, String> = conn
            .prepare("SELECT name, type FROM pragma_table_list WHERE schema = 'main'")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()
            })
            .unwrap_or_default();
        let mut stmt = conn.prepare(
            "SELECT name, type FROM main.sqlite_schema \
             WHERE type IN ('table', 'view') ORDER BY rowid",
        )?;
        let listed: Vec<(String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    text_of(row.get_ref(0)?).unwrap_or_default(),
                    text_of(row.get_ref(1)?).unwrap_or_default(),
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        let mut tables: Vec<Table> = listed
            .into_iter()
            .filter(|(name, _)| !name.is_empty())
            .map(|(name, kind)| {
                let kind = kinds.get(&name).cloned().unwrap_or(kind);
                Table {
                    internal: is_internal(&name, &kind),
                    name,
                    kind,
                    columns: Vec::new(),
                }
            })
            .collect();
        tables.push(Table {
            name: "sqlite_master".to_string(),
            kind: "table".to_string(),
            internal: true,
            columns: Vec::new(),
        });
        for table in tables.iter_mut().take(MAX_DESCRIBED) {
            table.columns = columns_of(&conn, &table.name);
        }
        Ok(tables)
    }

    /// A table's columns and declared types; empty where SQLite cannot say (a view of
    /// a table that is gone, a virtual table whose module this build lacks).
    fn columns_of(conn: &Connection, table: &str) -> Vec<(String, String)> {
        conn.prepare("SELECT name, type FROM pragma_table_info(?1, 'main')")
            .and_then(|mut stmt| {
                stmt.query_map([table], |row| {
                    Ok((
                        text_of(row.get_ref(0)?).unwrap_or_default(),
                        text_of(row.get_ref(1)?).unwrap_or_default(),
                    ))
                })?
                .collect::<rusqlite::Result<_>>()
            })
            .unwrap_or_default()
    }

    fn text_of(value: ValueRef<'_>) -> Option<String> {
        match value {
            ValueRef::Text(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            ValueRef::Integer(i) => Some(i.to_string()),
            _ => None,
        }
    }

    /// Roughly how many rows `table` holds, for the loading screen's bar: its largest
    /// rowid, which costs a step down one side of the tree. 0 where there is none (a
    /// view, a table without rowids).
    pub(crate) fn estimate_rows(path: &Path, table: &Table) -> u64 {
        let Ok(conn) = open(path) else {
            return 0;
        };
        let sql = format!("SELECT max(rowid) FROM main.{}", quoted(&table.name));
        conn.query_row(&sql, [], |row| row.get::<_, Option<i64>>(0))
            .ok()
            .flatten()
            .and_then(|n| u64::try_from(n).ok())
            .unwrap_or(0)
    }

    /// The columns `table` opens with, read from its first rows the way a read decides
    /// them, for the home screen's preview.
    pub fn schema_preview(path: &Path, table: &Table) -> Option<crate::discover::SchemaPreview> {
        const SAMPLE: usize = 100;
        let conn = open(path).ok()?;
        // A view can take long to give its first rows, or never end; the preview is
        // given up rather than left running on its worker.
        let began = std::time::Instant::now();
        conn.progress_handler(STEPS_PER_LOOK, Some(move || began.elapsed() > PREVIEW_TIME))
            .ok()?;
        let sql = format!("SELECT * FROM main.{} LIMIT {SAMPLE}", quoted(&table.name));
        let mut stmt = conn.prepare(&sql).ok()?;
        let (names, affinities) = described(&stmt, table);
        let mut batch = Batch::new(&affinities);
        let mut rows = stmt.query([]).ok()?;
        while let Some(row) = rows.next().ok()? {
            for (i, column) in batch.columns.iter_mut().enumerate() {
                // Only the type matters here: a text or blob of any length stands for
                // its kind empty, so a table of large blobs is not held to preview it.
                column.push(match row.get_ref(i).ok()? {
                    ValueRef::Text(_) => ValueRef::Text(b""),
                    ValueRef::Blob(_) => ValueRef::Blob(b""),
                    value => value,
                });
            }
        }
        Some(
            names
                .into_iter()
                .zip(&batch.columns)
                .map(|(name, column)| (name, column.dtype()))
                .collect(),
        )
    }

    /// The statement's column names, made unique and non-empty as Polars needs them,
    /// and each one's affinity from what `table` declares.
    fn described(
        stmt: &rusqlite::Statement<'_>,
        table: &Table,
    ) -> (Vec<String>, Vec<(Affinity, String)>) {
        let mut seen = std::collections::HashSet::new();
        let names = stmt
            .column_names()
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let base = if name.is_empty() {
                    format!("column_{}", i + 1)
                } else {
                    name.to_string()
                };
                let mut unique = base.clone();
                let mut n = 1;
                while !seen.insert(unique.clone()) {
                    unique = format!("{base}_{n}");
                    n += 1;
                }
                unique
            })
            .collect::<Vec<_>>();
        // Declared types by position, when the schema describes as many columns as the
        // statement reads; a virtual table's hidden columns are in neither.
        let declared: Vec<String> = if table.columns.len() == names.len() {
            table.columns.iter().map(|(_, t)| t.clone()).collect()
        } else {
            vec![String::new(); names.len()]
        };
        let affinities = declared
            .into_iter()
            .map(|d| (Affinity::of(&d), d))
            .collect();
        (names, affinities)
    }

    /// Read `table` of the database `file` (named `display` to the user) into temporary
    /// IPC files written through `writer`, counting its rows in `read`. `others` are the
    /// database's other tables, for the Info panel.
    pub(crate) fn convert(
        file: &Path,
        display: &Path,
        table: &Table,
        others: &[Table],
        options: &OpenOptions,
        writer: &Writer,
        read: &AtomicU64,
    ) -> Result<Converted> {
        let conn = open(file)?;
        let stop = writer.clone();
        conn.progress_handler(STEPS_PER_LOOK, Some(move || stop.stopped()))?;
        let sql = format!("SELECT * FROM main.{}", quoted(&table.name));
        let mut stmt = conn.prepare(&sql).map_err(|e| named(e.into(), display))?;
        let (names, affinities) = described(&stmt, table);
        let width = names.len().max(1);
        let batch_rows = (BATCH_CELLS / width).clamp(1, BATCH_ROWS);

        let mut segments = Segments::new(options, writer);
        let mut batch = Batch::new(&affinities);
        let mut rows = stmt.query([])?;
        let mut since_count = 0u64;
        loop {
            let row = match rows.next() {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(_) if writer.stopped() => return Err(eyre!("Reading was stopped.")),
                Err(e) => return Err(named(e.into(), display)),
            };
            for (i, column) in batch.columns.iter_mut().enumerate() {
                batch.bytes += column.push(row.get_ref(i)?);
            }
            batch.rows += 1;
            since_count += 1;
            if since_count == 1024 {
                read.fetch_add(since_count, Ordering::Relaxed);
                since_count = 0;
            }
            if batch.rows >= batch_rows || batch.bytes >= BATCH_BYTES {
                segments.write(&batch.take(&names)?)?;
            }
        }
        read.fetch_add(since_count, Ordering::Relaxed);
        // Written whatever its height: an empty table still has its columns.
        segments.write(&batch.take(&names)?)?;
        let mixed = batch.mixed(&names);
        let (lf, files) = segments.finish()?;
        // A column every value of which was null is read as its declared type.
        let typed: Vec<Expr> = names
            .iter()
            .zip(&batch.columns)
            .filter(|(_, column)| matches!(column, Column::Unset { .. }))
            .map(|(name, column)| col(name.as_str()).cast(column.dtype()))
            .collect();
        let lf = if typed.is_empty() {
            lf
        } else {
            lf.with_columns(typed)
        };
        Ok(Converted {
            lf,
            files,
            notes: notes(table, &mixed),
            other_tables: other_tables(table, others),
        })
    }

    /// An error from SQLite, said about the database by the name the user knows.
    fn named(e: color_eyre::Report, display: &Path) -> color_eyre::Report {
        e.wrap_err(format!("Could not read {}", display.display()))
    }

    fn notes(table: &Table, mixed: &[String]) -> Vec<Note> {
        let mut notes = Vec::new();
        if !mixed.is_empty() {
            notes.push(Note {
                summary: format!(
                    "{} {} values of several types and {} read as text",
                    mixed.join(", "),
                    if mixed.len() == 1 { "holds" } else { "hold" },
                    if mixed.len() == 1 { "is" } else { "are" },
                ),
                scope: format!("of the {} {}", table.kind, table.name),
                read_as_text: None,
                passed_over: None,
            });
        }
        notes
    }

    /// The database's other tables of its own, as `--table` names them.
    fn other_tables(table: &Table, others: &[Table]) -> Vec<String> {
        const SHOWN: usize = 12;
        let own: Vec<&str> = others
            .iter()
            .filter(|t| !t.internal && t.name != table.name)
            .map(|t| t.name.as_str())
            .collect();
        let mut shown: Vec<String> = own.iter().take(SHOWN).map(|s| s.to_string()).collect();
        if own.len() > SHOWN {
            shown.push(format!("{} more", group_chrome(own.len() - SHOWN)));
        }
        shown
    }

    /// The rows read since the last batch was written, a column at a time.
    struct Batch {
        columns: Vec<Column>,
        rows: usize,
        bytes: usize,
    }

    impl Batch {
        fn new(affinities: &[(Affinity, String)]) -> Self {
            Self {
                columns: affinities
                    .iter()
                    .map(|(affinity, declared)| Column::new(*affinity, declared))
                    .collect(),
                rows: 0,
                bytes: 0,
            }
        }

        /// The batch as a frame, leaving each column empty and of the type it has
        /// come to, which only ever widens.
        fn take(&mut self, names: &[String]) -> Result<DataFrame> {
            let height = self.rows;
            let columns: Vec<polars::prelude::Column> = names
                .iter()
                .zip(self.columns.iter_mut())
                .map(|(name, column)| column.take(name.as_str().into(), height).into())
                .collect();
            self.rows = 0;
            self.bytes = 0;
            Ok(DataFrame::new(height, columns)?)
        }

        /// The columns read as text because their values were of several types.
        fn mixed(&self, names: &[String]) -> Vec<String> {
            names
                .iter()
                .zip(&self.columns)
                .filter(|(_, column)| column.mixed())
                .map(|(name, _)| name.clone())
                .collect()
        }
    }

    /// One column of a batch. A value that does not fit widens it: integers to floats,
    /// anything to text. The lattice is Unset < Int < Float < Text and Unset < Bytes <
    /// Text, so a column's type never narrows from one batch to the next.
    enum Column {
        /// No value yet decides it: `nulls` nulls so far, and what the declaration says
        /// it is when none ever does.
        Unset {
            nulls: usize,
            fallback: DataType,
        },
        Int(Vec<Option<i64>>),
        Float(Vec<Option<f64>>),
        Text {
            values: Vec<Option<String>>,
            /// Widened to text from numbers or blobs, rather than declared or found so.
            mixed: bool,
        },
        Bytes(Vec<Option<Vec<u8>>>),
    }

    impl Column {
        fn new(affinity: Affinity, declared: &str) -> Self {
            match affinity.dtype(declared) {
                Some(DataType::Int64) => Self::Int(Vec::new()),
                Some(DataType::Float64) => Self::Float(Vec::new()),
                Some(DataType::Binary) => Self::Bytes(Vec::new()),
                Some(_) => Self::Text {
                    values: Vec::new(),
                    mixed: false,
                },
                // Undeclared and numeric columns take their values' type. All null,
                // a numeric one is a number and an undeclared one text.
                None => Self::Unset {
                    nulls: 0,
                    fallback: match affinity {
                        Affinity::Numeric => DataType::Float64,
                        _ => DataType::String,
                    },
                },
            }
        }

        fn mixed(&self) -> bool {
            matches!(self, Self::Text { mixed: true, .. })
        }

        fn dtype(&self) -> DataType {
            match self {
                Self::Unset { fallback, .. } => fallback.clone(),
                Self::Int(_) => DataType::Int64,
                Self::Float(_) => DataType::Float64,
                Self::Text { .. } => DataType::String,
                Self::Bytes(_) => DataType::Binary,
            }
        }

        /// Add `value`, widening the column if it does not fit; the text and blob
        /// bytes it adds.
        fn push(&mut self, value: ValueRef<'_>) -> usize {
            let bytes = match value {
                ValueRef::Text(b) | ValueRef::Blob(b) => b.len(),
                _ => 0,
            };
            if !self.fits(value) {
                self.widen(value);
            }
            match (self, value) {
                (Self::Unset { nulls, .. }, ValueRef::Null) => *nulls += 1,
                (Self::Int(v), ValueRef::Null) => v.push(None),
                (Self::Int(v), ValueRef::Integer(i)) => v.push(Some(i)),
                (Self::Float(v), ValueRef::Null) => v.push(None),
                (Self::Float(v), ValueRef::Integer(i)) => v.push(Some(i as f64)),
                (Self::Float(v), ValueRef::Real(f)) => v.push(Some(f)),
                (Self::Bytes(v), ValueRef::Null) => v.push(None),
                (Self::Bytes(v), ValueRef::Blob(b)) => v.push(Some(b.to_vec())),
                (Self::Text { values, mixed }, value) => {
                    *mixed |= matches!(
                        value,
                        ValueRef::Integer(_) | ValueRef::Real(_) | ValueRef::Blob(_)
                    );
                    values.push(as_text(value));
                }
                _ => unreachable!("widened to fit"),
            }
            bytes
        }

        fn fits(&self, value: ValueRef<'_>) -> bool {
            matches!(
                (self, value),
                (_, ValueRef::Null)
                    | (Self::Int(_), ValueRef::Integer(_))
                    | (Self::Float(_), ValueRef::Integer(_) | ValueRef::Real(_))
                    | (Self::Bytes(_), ValueRef::Blob(_))
                    | (Self::Text { .. }, _)
            )
        }

        /// Widen to the narrowest type that holds what the column holds and `value`.
        fn widen(&mut self, value: ValueRef<'_>) {
            let taken = std::mem::replace(
                self,
                Self::Unset {
                    nulls: 0,
                    fallback: DataType::String,
                },
            );
            *self = match (taken, value) {
                (Self::Unset { nulls, .. }, ValueRef::Integer(_)) => Self::Int(vec![None; nulls]),
                (Self::Unset { nulls, .. }, ValueRef::Real(_)) => Self::Float(vec![None; nulls]),
                (Self::Unset { nulls, .. }, ValueRef::Blob(_)) => Self::Bytes(vec![None; nulls]),
                (Self::Unset { nulls, .. }, _) => Self::Text {
                    values: vec![None; nulls],
                    mixed: false,
                },
                (Self::Int(v), ValueRef::Real(_)) => {
                    Self::Float(v.into_iter().map(|i| i.map(|i| i as f64)).collect())
                }
                (Self::Int(v), _) => Self::Text {
                    values: v.into_iter().map(|i| i.map(|i| i.to_string())).collect(),
                    mixed: true,
                },
                (Self::Float(v), _) => Self::Text {
                    values: v.into_iter().map(|f| f.map(real_text)).collect(),
                    mixed: true,
                },
                (Self::Bytes(v), _) => Self::Text {
                    values: v.into_iter().map(|b| b.map(|b| blob_text(&b))).collect(),
                    mixed: true,
                },
                (text @ Self::Text { .. }, _) => text,
            };
        }

        /// The values so far as a series of `height` rows, leaving the column empty and
        /// of the same type.
        fn take(&mut self, name: PlSmallStr, height: usize) -> Series {
            match self {
                Self::Unset { nulls, .. } => {
                    *nulls = 0;
                    Series::full_null(name, height, &DataType::Null)
                }
                Self::Int(v) => Series::new(name, std::mem::take(v)),
                Self::Float(v) => Series::new(name, std::mem::take(v)),
                Self::Text { values, .. } => Series::new(name, std::mem::take(values)),
                Self::Bytes(v) => {
                    let values = std::mem::take(v);
                    BinaryChunked::from_iter_options(name, values.into_iter()).into_series()
                }
            }
        }
    }

    /// Any value as text, as SQLite would cast it; a blob as its literal.
    fn as_text(value: ValueRef<'_>) -> Option<String> {
        match value {
            ValueRef::Null => None,
            ValueRef::Integer(i) => Some(i.to_string()),
            ValueRef::Real(f) => Some(real_text(f)),
            ValueRef::Text(b) => Some(String::from_utf8_lossy(b).into_owned()),
            ValueRef::Blob(b) => Some(blob_text(b)),
        }
    }

    /// A float as SQLite prints one: a whole number keeps its `.0`.
    fn real_text(f: f64) -> String {
        if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 {
            format!("{f:.1}")
        } else {
            f.to_string()
        }
    }

    #[cfg(test)]
    pub(super) fn open_for_tests(path: &Path) -> Result<Connection> {
        open(path)
    }

    #[cfg(test)]
    pub(super) fn immutable_uri_for_tests(path: &Path) -> String {
        immutable_uri(path)
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests;

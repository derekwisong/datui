//! SQLite databases, read only.
//!
//! A database is a file of tables. One with a single table of its own opens it; one
//! with several is a place on the home screen whose rows are its tables, each named by
//! a path inside the file (`app.db/users`) that nothing on disk has. `--table` picks
//! one by name either way.
//!
//! A table is read in place. The dataset's frame is a scan of it ([`open_table`]) whose
//! windows SQLite reads by the table's key, so the first rows show at once and the last
//! page costs what the first does; the sidebar's filters and sort run in SQLite as
//! `WHERE` and `ORDER BY` (see [`crate::pushdown`]), where its indexes serve them, and
//! the row count is SQLite's `count(*)`. What reads the whole view (analysis, a query,
//! an export) has it read a batch at a time from SQLite, and nothing is copied to disk.
//!
//! Nothing is written to the database. It is opened read only, with `query_only`,
//! defensive mode and an untrusted schema, and with extension loading left out of the
//! build. Reading a table runs no trigger. A database in WAL mode with a `-wal` file is
//! read through it, and SQLite creates the `-shm` index beside it if that is missing; one
//! with no `-wal` is read as it stands (`immutable=1`), writing nothing. A database that
//! cannot be read without writing beside it (a hot journal, or a `-wal` without its
//! `-shm` in a read-only directory) is refused rather than read wrong.

use std::path::Path;

/// What datui does with a SQLite database: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    python: Some(crate::python_script::Python {
        call: "pl.read_database",
        eager: true,
        glob_flag: false,
        arguments: Some(crate::python_script::sqlite_arguments),
    }),
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Magic,
        trusted: crate::readers::Trusted {
            tables: true,
            ..crate::readers::EVERYWHERE
        },
    }],
    tables: Some(tables),
    table_schema: Some(
        |file, name| match pick(tables(file).ok()?, name, file).ok()? {
            Pick::One(table) => schema_preview(file, &table),
            Pick::Several(_) => None,
        },
    ),
    bytes_decide: true,
    ..crate::readers::BASE
};

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

/// The path of a table inside a database, and the way back: see [`crate::members`].
pub use crate::members::{place as table_place, split as table_path};

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
    crate::members::pick(
        tables,
        wanted,
        display,
        " --table sqlite_master shows its schema.",
    )
}

#[cfg(feature = "sqlite")]
pub use read::*;

#[cfg(not(feature = "sqlite"))]
pub use unsupported::*;

/// A table opened in place, as the dataset takes it.
pub struct Opened {
    /// Every row of the table.
    pub lf: polars::prelude::LazyFrame,
    /// What runs the sidebar's filters and sort in SQLite.
    pub pushdown: std::sync::Arc<dyn crate::pushdown::Pushdown>,
    /// Stops the table's statements when the dataset lets go of it.
    pub hold: Hold,
    /// The database's other tables, for the Info panel.
    pub other_tables: Vec<String>,
}

/// Held by a dataset read in place: dropped, whatever SQLite is running for it stops.
pub struct Hold(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for Hold {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(not(feature = "sqlite"))]
mod unsupported {
    use std::path::Path;

    use color_eyre::Result;
    use color_eyre::eyre::eyre;

    use super::{Opened, Table};

    fn refused() -> color_eyre::Report {
        eyre!(
            "This build of datui reads no SQLite databases: it was built without the sqlite feature."
        )
    }

    pub fn tables(_path: &Path) -> Result<Vec<Table>> {
        Err(refused())
    }

    pub fn detail(_path: &Path, _tables: &[Table]) -> Option<crate::text_formats::Detail> {
        None
    }

    pub fn schema_preview(_path: &Path, _table: &Table) -> Option<crate::discover::SchemaPreview> {
        None
    }

    pub fn open_table(
        _file: &Path,
        _display: &Path,
        _table: &Table,
        _others: &[Table],
    ) -> Result<Opened> {
        Err(refused())
    }
}

#[cfg(feature = "sqlite")]
mod read {
    use std::collections::{BTreeMap, HashMap};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use color_eyre::Result;
    use polars::prelude::*;
    use rusqlite::config::DbConfig;
    use rusqlite::types::{Value, ValueRef};
    use rusqlite::{Connection, OpenFlags};

    use super::{Affinity, Hold, Opened, Table, is_internal};
    use crate::error_display::{FileError, file_message};
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use crate::notes::Note;
    use crate::numfmt::group_chrome;
    use crate::pushdown::{Counter, Pushdown, PushedView, Windowed};

    /// How long a read waits on a writer's lock before it gives up.
    const BUSY: Duration = Duration::from_secs(2);
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
                FileError::new(path, "not a SQLite database").into()
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
                    return Err(FileError::new(
                        path,
                        "the database was left mid-write by a program that stopped: its -journal has to be rolled back first, which datui does not do. Opening it once with the sqlite3 tool rolls it back.",
                    )
                    .into());
                }
                // A WAL whose index (`-shm`) is missing and cannot be made here (a
                // read-only directory). Read without it, the database would lack what
                // was committed to the WAL.
                Err(e) if cannot_open(&e) && beside(path, "-wal").exists() => {
                    return Err(FileError::new(
                        path,
                        "the database has a -wal file that cannot be read from here without a -shm file beside it, and its directory is read only. Copy the database and its -wal to a writable directory.",
                    )
                    .into());
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
        // An empty authority, so a path that starts `//` is not read as a host; a
        // Windows drive path is `file:///C:/...`.
        let mut uri = String::from(if text.starts_with('/') {
            "file://"
        } else {
            "file:///"
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

    /// The SQLite tab of the Info panel for the database at `path`, whose `tables` the
    /// open listed: the database's page size and versions, and each table of its own
    /// with its kind, columns and the rows `ANALYZE` stored for it. Nothing here reads a
    /// table: a count of every table's rows would be a pass over the whole database.
    pub fn detail(path: &Path, tables: &[Table]) -> Option<crate::text_formats::Detail> {
        use crate::model_files::MetaValue;
        use crate::text_formats::count;
        let conn = open(path).ok()?;
        let pragma = |name: &str| -> Option<i64> {
            conn.query_row(&format!("PRAGMA main.{name}"), [], |row| row.get(0))
                .ok()
        };
        let text = |name: &str| -> Option<String> {
            conn.query_row(&format!("PRAGMA main.{name}"), [], |row| row.get(0))
                .ok()
        };
        let middot = crate::glyphs::get().middot;
        let page_size = pragma("page_size").unwrap_or(0);
        let pages = pragma("page_count").unwrap_or(0);
        let mut lines = vec![format!(
            "Page size: {} {middot} {}",
            group_chrome(usize::try_from(page_size).unwrap_or(0)),
            count(u64::try_from(pages).unwrap_or(0), "page", "pages"),
        )];
        let mut versions = format!("Schema version: {}", pragma("schema_version").unwrap_or(0));
        if let Some(user) = pragma("user_version").filter(|v| *v != 0) {
            versions.push_str(&format!(" {middot} user version: {user}"));
        }
        if let Some(encoding) = text("encoding") {
            versions.push_str(&format!(" {middot} {encoding}"));
        }
        lines.push(versions);
        // `ANALYZE` stores a table's rows as the first number of its statistics; an
        // index's are the table's too.
        let mut analyzed: std::collections::HashMap<String, u64> = Default::default();
        let has_stats = tables.iter().any(|t| t.name == "sqlite_stat1");
        if has_stats
            && let Ok(mut stmt) = conn.prepare("SELECT tbl, stat FROM main.sqlite_stat1")
            && let Ok(rows) = stmt.query_map([], |row| {
                Ok((
                    text_of(row.get_ref(0)?).unwrap_or_default(),
                    text_of(row.get_ref(1)?).unwrap_or_default(),
                ))
            })
        {
            for (table, stat) in rows.flatten() {
                if let Some(n) = stat.split(' ').next().and_then(|n| n.parse::<u64>().ok()) {
                    let rows = analyzed.entry(table).or_default();
                    *rows = (*rows).max(n);
                }
            }
        }
        let own: Vec<&Table> = tables.iter().filter(|t| !t.internal).collect();
        let views = own.iter().filter(|t| t.kind == "view").count();
        let mut held = count((own.len() - views) as u64, "table", "tables");
        if views > 0 {
            held.push_str(&format!(
                " {middot} {}",
                count(views as u64, "view", "views")
            ));
        }
        lines.push(held);
        lines.push(if analyzed.is_empty() {
            "Rows: not stored; ANALYZE stores them".to_string()
        } else {
            "Rows: as ANALYZE last stored them".to_string()
        });
        let list = crate::text_formats::capped_list(
            own.iter().map(|t| {
                let mut said = vec![t.kind.clone()];
                if !t.columns.is_empty() {
                    said.push(count(t.columns.len() as u64, "column", "columns"));
                }
                if let Some(rows) = analyzed.get(&t.name) {
                    said.push(count(*rows, "row", "rows"));
                }
                (t.name.clone(), MetaValue::Text(said.join(", ")))
            }),
            own.len(),
        );
        Some(crate::text_formats::Detail {
            tab: crate::text_formats::tab(crate::FileFormat::Sqlite),
            lines,
            list_title: "Tables",
            list,
            ..Default::default()
        })
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

    /// Rows in a batch of a read of the whole table, at most.
    const BATCH_ROWS: usize = 1 << 16;
    /// Cells in a batch, at most, so a table of a thousand columns holds as much at once
    /// as one of sixty.
    const BATCH_CELLS: usize = 1 << 22;
    /// Text and blob bytes in a batch before it is handed on, however few its rows.
    const BATCH_BYTES: usize = 64 << 20;
    /// Rows the open reads to type the columns no declaration settles.
    const SAMPLE_ROWS: usize = 1000;
    /// The longest the open reads them for: a view may take long to give rows.
    const SAMPLE_TIME: Duration = Duration::from_secs(2);
    /// Places a view remembers in its order, to page on from rather than skip to.
    const CHECKPOINTS: usize = 4096;
    /// Views whose places and counts are remembered.
    const MEMO_VIEWS: usize = 16;
    /// Columns one pass of the census looks at; SQLite returns at most 2000 a row.
    const CENSUS_COLUMNS: usize = 1000;

    /// What a column is read as.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Kind {
        Int,
        Float,
        Text,
        Bytes,
    }

    impl Kind {
        fn dtype(self) -> DataType {
            match self {
                Self::Int => DataType::Int64,
                Self::Float => DataType::Float64,
                Self::Text => DataType::String,
                Self::Bytes => DataType::Binary,
            }
        }

        /// The storage classes a value read as this kind is held in, null included.
        fn classes(self) -> &'static str {
            match self {
                Self::Int => "'integer', 'null'",
                Self::Float => "'integer', 'real', 'null'",
                Self::Text => "'text', 'null'",
                Self::Bytes => "'blob', 'null'",
            }
        }

        /// `e` as the frame shows it, in SQL: what the frame holds is what a filter or a
        /// sort sees, so a statement over this agrees with Polars on every row. A number
        /// column's other values are null; a text column's numbers are SQLite's own text
        /// of them and its blobs their literal, `X'0A1B'`; a blob column's text its bytes.
        fn read(self, e: &str) -> String {
            match self {
                Self::Int => format!("CASE WHEN typeof({e}) = 'integer' THEN {e} END"),
                Self::Float => {
                    format!(
                        "CASE WHEN typeof({e}) IN ('integer', 'real') THEN CAST({e} AS REAL) END"
                    )
                }
                Self::Text => format!(
                    "CASE typeof({e}) WHEN 'blob' THEN 'X''' || hex({e}) || '''' ELSE CAST({e} AS TEXT) END"
                ),
                Self::Bytes => format!("CAST({e} AS BLOB)"),
            }
        }
    }

    /// The storage classes the open's sample found in a column.
    #[derive(Debug, Default, Clone, Copy)]
    struct Seen {
        int: bool,
        real: bool,
        text: bool,
        blob: bool,
    }

    impl Seen {
        fn add(&mut self, value: ValueRef<'_>) {
            match value {
                ValueRef::Null => {}
                ValueRef::Integer(_) => self.int = true,
                ValueRef::Real(_) => self.real = true,
                ValueRef::Text(_) => self.text = true,
                ValueRef::Blob(_) => self.blob = true,
            }
        }
    }

    /// The kind a column is read as, from what it declares and what the sample found,
    /// and whether it holds values of several types read as text.
    ///
    /// SQLite lets any column hold any value; its declared type only says what it
    /// prefers. A declaration is taken unless the sample contradicts it, and a column
    /// that declares nothing takes its values' type.
    fn decide(affinity: Affinity, declared: &str, seen: Seen) -> (Kind, bool) {
        let numbers = seen.int || seen.real;
        let several = [numbers, seen.text, seen.blob]
            .iter()
            .filter(|&&s| s)
            .count()
            > 1;
        match affinity {
            Affinity::Integer | Affinity::Real if seen.text || seen.blob => (Kind::Text, true),
            Affinity::Integer if seen.real => (Kind::Float, false),
            Affinity::Integer => (Kind::Int, false),
            Affinity::Real => (Kind::Float, false),
            Affinity::Text => (Kind::Text, seen.blob),
            Affinity::Blob if !declared.trim().is_empty() => {
                if numbers || seen.text {
                    (Kind::Text, true)
                } else {
                    (Kind::Bytes, false)
                }
            }
            _ if several => (Kind::Text, true),
            _ if seen.real => (Kind::Float, false),
            _ if seen.int => (Kind::Int, false),
            _ if seen.blob => (Kind::Bytes, false),
            _ if seen.text => (Kind::Text, false),
            // All null: a numeric column is a number, an undeclared one text.
            Affinity::Numeric => (Kind::Float, false),
            _ => (Kind::Text, false),
        }
    }

    /// The statement's column names, made unique and non-empty as Polars needs them,
    /// and each one's affinity and declared type from what `table` declares.
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

    /// A connection that gives up when `stop` is set or `deadline` passes.
    fn stoppable(
        conn: &Connection,
        stop: Arc<AtomicBool>,
        deadline: Option<Instant>,
    ) -> rusqlite::Result<()> {
        conn.progress_handler(
            STEPS_PER_LOOK,
            Some(move || {
                stop.load(Ordering::Relaxed) || deadline.is_some_and(|d| Instant::now() > d)
            }),
        )
    }

    /// The table's key, by which its rows are ordered and a page is found: its rowid,
    /// under a name no column of its own shadows; a table without rowids' primary key;
    /// nothing for a view, whose rows are in whatever order it gives them.
    fn key_of(conn: &Connection, table: &Table) -> Vec<String> {
        let quoted_table = quoted(&table.name);
        let shadowed = |alias: &str| {
            table
                .columns
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(alias))
        };
        if let Some(alias) = ["rowid", "_rowid_", "oid"]
            .into_iter()
            .find(|alias| !shadowed(alias))
            && conn
                .prepare(&format!("SELECT {alias} FROM main.{quoted_table} LIMIT 0"))
                .is_ok()
        {
            return vec![alias.to_string()];
        }
        if table.kind == "view" {
            return Vec::new();
        }
        conn.prepare("SELECT name FROM pragma_table_info(?1, 'main') WHERE pk > 0 ORDER BY pk")
            .and_then(|mut stmt| {
                stmt.query_map([&table.name], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map(|names| names.iter().map(|n| quoted(n)).collect())
            .unwrap_or_default()
    }

    /// A table as a source: read in place, a window or a view at a time.
    struct Source {
        file: PathBuf,
        display: PathBuf,
        /// `WITH s(k0, …, c0, …) AS (SELECT <key>, * FROM main."t")`: the table with
        /// its key and columns named by position, so no name from the file is written
        /// into a statement past this one place, where it is quoted.
        with: String,
        keys: usize,
        columns: Vec<SourceColumn>,
        schema: SchemaRef,
        census: OnceLock<Census>,
        memo: Mutex<HashMap<String, Memo>>,
        /// Set when the dataset lets go: whatever the source runs then stops.
        stop: Arc<AtomicBool>,
        table: Table,
    }

    struct SourceColumn {
        name: PlSmallStr,
        kind: Kind,
        /// The sample found values of several types, read as text.
        mixed: bool,
        /// Text in a column that declares a number: compared as stored, SQLite would
        /// make a number of a value that looks like one (`'2024'`), and text sorts
        /// after every number.
        numeric_text: bool,
    }

    /// What a pass over the whole table found: its rows, and each column's values that
    /// are not of its kind.
    struct Census {
        rows: usize,
        misfits: Vec<u64>,
    }

    /// What a view has learned about itself: its count, and places in its order.
    #[derive(Default)]
    struct Memo {
        rows: Option<usize>,
        /// Row `p` of the view is the first whose key is past these values.
        places: BTreeMap<usize, Vec<Value>>,
    }

    /// One condition of a view, on column `column`, against `value`.
    #[derive(Debug, Clone)]
    struct Atom {
        column: usize,
        operator: FilterOperator,
        value: Value,
    }

    /// The rows a view shows: the sidebar's filters, joined left to right as the
    /// sidebar joins them, and its sort.
    #[derive(Debug)]
    struct View {
        /// What it is, as the memo knows it.
        key: String,
        conditions: Vec<(LogicalOperator, Atom)>,
        /// Column and descending, nulls last, ties in the table's order.
        sort: Vec<(usize, bool)>,
        /// The table's order backward, when there is no sort.
        reversed: bool,
    }

    impl View {
        fn new(
            conditions: Vec<(LogicalOperator, Atom)>,
            sort: Vec<(usize, bool)>,
            reversed: bool,
        ) -> Self {
            Self {
                key: format!("{conditions:?} {sort:?} {reversed}"),
                conditions,
                sort,
                reversed,
            }
        }

        fn whole() -> Self {
            Self::new(Vec::new(), Vec::new(), false)
        }

        /// In the table's own order, forward or back.
        fn natural(&self) -> bool {
            self.sort.is_empty()
        }
    }

    /// A statement over a view.
    struct Query<'a> {
        columns: &'a [usize],
        /// A condition besides the view's, with its parameters: a predicate Polars
        /// pushed into the scan.
        also: Option<(String, Vec<Value>)>,
        /// The view's order backward, for a window nearer its end.
        backward: bool,
        /// Only rows past these key values, in the view's order.
        after: Option<Vec<Value>>,
        limit: Option<usize>,
        offset: usize,
        /// Read the key too, to remember where a page ended.
        with_keys: bool,
    }

    impl Source {
        fn open(file: &Path, display: &Path, table: &Table) -> Result<Self> {
            let conn = open(file)?;
            let stop = Arc::new(AtomicBool::new(false));
            stoppable(&conn, stop.clone(), Some(Instant::now() + SAMPLE_TIME))?;
            let key = key_of(&conn, table);
            let star = format!("SELECT * FROM main.{}", quoted(&table.name));
            let stmt = conn.prepare(&star).map_err(|e| named(e.into(), display))?;
            let (names, affinities) = described(&stmt, table);
            drop(stmt);
            let width = names.len();
            let mut with = String::from("WITH s(");
            let mut parts: Vec<String> = (0..key.len()).map(|i| format!("k{i}")).collect();
            parts.extend((0..width).map(|i| format!("c{i}")));
            with.push_str(&parts.join(", "));
            with.push_str(") AS (SELECT ");
            for k in &key {
                with.push_str(k);
                with.push_str(", ");
            }
            with.push_str(&format!("* FROM main.{})", quoted(&table.name)));

            // A sample of the first rows types the columns no declaration settles.
            let mut seen = vec![Seen::default(); width];
            let list = (0..width)
                .map(|i| format!("s.c{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            if width > 0 {
                let sql = format!("{with} SELECT {list} FROM s LIMIT {SAMPLE_ROWS}");
                let mut stmt = conn.prepare(&sql).map_err(|e| named(e.into(), display))?;
                let mut rows = stmt.query([]).map_err(|e| named(e.into(), display))?;
                // A sample cut short by its time limit types the columns from what
                // it read; anything else is an error.
                loop {
                    match rows.next() {
                        Ok(Some(row)) => {
                            for (i, seen) in seen.iter_mut().enumerate() {
                                seen.add(row.get_ref(i)?);
                            }
                        }
                        Ok(None) => break,
                        Err(e)
                            if e.sqlite_error_code()
                                == Some(rusqlite::ErrorCode::OperationInterrupted) =>
                        {
                            break;
                        }
                        Err(e) => return Err(named(e.into(), display)),
                    }
                }
            }
            let columns: Vec<SourceColumn> = names
                .into_iter()
                .zip(affinities)
                .zip(seen)
                .map(|((name, (affinity, declared)), seen)| {
                    let (kind, mixed) = decide(affinity, &declared, seen);
                    SourceColumn {
                        name: name.into(),
                        kind,
                        mixed,
                        numeric_text: kind == Kind::Text
                            && matches!(
                                affinity,
                                Affinity::Integer | Affinity::Real | Affinity::Numeric
                            ),
                    }
                })
                .collect();
            let schema = Arc::new(Schema::from_iter(
                columns
                    .iter()
                    .map(|c| Field::new(c.name.clone(), c.kind.dtype())),
            ));
            Ok(Self {
                file: file.to_path_buf(),
                display: display.to_path_buf(),
                with,
                keys: key.len(),
                columns,
                schema,
                census: OnceLock::new(),
                memo: Mutex::new(HashMap::new()),
                stop,
                table: table.clone(),
            })
        }

        /// A connection for one statement, stopped with the source.
        fn connect(&self) -> PolarsResult<Connection> {
            let conn = open(&self.file).map_err(|e| {
                polars_err!(ComputeError: "{}", crate::error_display::user_message_from_report(&e, Some(&self.display)))
            })?;
            stoppable(&conn, self.stop.clone(), None).map_err(|e| self.failed(e))?;
            Ok(conn)
        }

        fn failed(&self, e: rusqlite::Error) -> PolarsError {
            if self.stop.load(Ordering::Relaxed) {
                polars_err!(ComputeError: "{}", file_message(&self.display, "reading was stopped"))
            } else {
                polars_err!(ComputeError: "{}", file_message(&self.display, &e.to_string()))
            }
        }

        /// Whether every value of column `i` is of its kind, as the census found: then
        /// the column is read as it is stored, and SQLite can use an index on it.
        fn clean(&self, i: usize) -> bool {
            self.census.get().is_some_and(|c| c.misfits[i] == 0)
        }

        /// Column `i` as the frame shows it, in SQL.
        fn column_sql(&self, i: usize) -> String {
            let plain = format!("s.c{i}");
            if self.clean(i) {
                plain
            } else {
                self.columns[i].kind.read(&plain)
            }
        }

        /// Column `i` as a comparison with a value reads it: as [`Self::column_sql`],
        /// without the affinity that would turn a text value into a number first.
        fn compared_sql(&self, i: usize) -> String {
            let e = self.column_sql(i);
            if self.columns[i].numeric_text {
                format!("+{e}")
            } else {
                e
            }
        }

        fn index_of(&self, name: &str) -> Option<usize> {
            self.columns.iter().position(|c| c.name == name)
        }

        /// One sidebar filter as a condition SQLite runs, where it means what Polars
        /// would make of it; `None` where Polars would compare across types or refuse.
        fn atom(&self, filter: &FilterStatement) -> Option<Atom> {
            // A value SQLite holds that Polars reads as null is not NULL here.
            if !filter.operator.takes_value() {
                return None;
            }
            let column = self.index_of(&filter.column)?;
            let kind = self.columns[column].kind;
            let contains = matches!(
                filter.operator,
                FilterOperator::Contains | FilterOperator::NotContains
            );
            // The value parsed as the sidebar parses it for a column of this type.
            let value = match kind {
                Kind::Int if !contains => Value::Integer(filter.value.parse().ok()?),
                Kind::Float if !contains => Value::Real(filter.value.parse().ok()?),
                Kind::Text => Value::Text(filter.value.clone()),
                _ => return None,
            };
            Some(Atom {
                column,
                operator: filter.operator,
                value,
            })
        }

        fn atom_sql(&self, atom: &Atom, params: &mut Vec<Value>) -> String {
            let e = self.compared_sql(atom.column);
            // Text compares byte for byte, as Polars does, whatever the column declares.
            let collate = if self.columns[atom.column].kind == Kind::Text {
                " COLLATE BINARY"
            } else {
                ""
            };
            params.push(atom.value.clone());
            match atom.operator {
                FilterOperator::Eq => format!("{e} = ?{collate}"),
                FilterOperator::NotEq => format!("{e} <> ?{collate}"),
                FilterOperator::Gt => format!("{e} > ?{collate}"),
                FilterOperator::Lt => format!("{e} < ?{collate}"),
                FilterOperator::GtEq => format!("{e} >= ?{collate}"),
                FilterOperator::LtEq => format!("{e} <= ?{collate}"),
                FilterOperator::Contains => format!("instr({e}, ?) > 0"),
                FilterOperator::NotContains => format!("instr({e}, ?) = 0"),
                // Never an atom: see `atom`.
                FilterOperator::IsNull | FilterOperator::IsNotNull => {
                    unreachable!("a null test is not pushed down")
                }
            }
        }

        /// The view's conditions, joined left to right: `((a AND b) OR c)`.
        fn conditions_sql(&self, view: &View, params: &mut Vec<Value>) -> Option<String> {
            let mut joined: Option<String> = None;
            for (logical, atom) in &view.conditions {
                let sql = self.atom_sql(atom, params);
                joined = Some(match joined {
                    None => sql,
                    Some(before) => match logical {
                        LogicalOperator::And => format!("({before} AND {sql})"),
                        LogicalOperator::Or => format!("({before} OR {sql})"),
                    },
                });
            }
            joined
        }

        fn statement(&self, view: &View, query: &Query<'_>) -> (String, Vec<Value>) {
            let mut params = Vec::new();
            let mut select: Vec<String> =
                query.columns.iter().map(|&i| self.column_sql(i)).collect();
            if query.with_keys {
                select.extend((0..self.keys).map(|k| format!("s.k{k}")));
            }
            if select.is_empty() {
                select.push("NULL".to_string());
            }
            let mut sql = format!("{} SELECT {} FROM s", self.with, select.join(", "));
            let mut conditions: Vec<String> = Vec::new();
            if let Some(c) = self.conditions_sql(view, &mut params) {
                conditions.push(c);
            }
            if let Some((also, also_params)) = &query.also {
                conditions.push(also.clone());
                params.extend(also_params.iter().cloned());
            }
            if let Some(after) = &query.after {
                let keys: Vec<String> = (0..self.keys).map(|k| format!("s.k{k}")).collect();
                params.extend(after.iter().cloned());
                let marks = vec!["?"; after.len()];
                let past = if view.reversed { "<" } else { ">" };
                conditions.push(format!(
                    "({}) {past} ({})",
                    keys.join(", "),
                    marks.join(", ")
                ));
            }
            if !conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&conditions.join(" AND "));
            }
            let mut order: Vec<String> = view
                .sort
                .iter()
                .map(|&(i, descending)| {
                    let collate = if self.columns[i].kind == Kind::Text {
                        " COLLATE BINARY"
                    } else {
                        ""
                    };
                    // Nulls last, as the sidebar sorts; first when read backward.
                    let (direction, nulls) = match descending != query.backward {
                        true => ("DESC", if query.backward { "FIRST" } else { "LAST" }),
                        false => ("ASC", if query.backward { "FIRST" } else { "LAST" }),
                    };
                    format!("{}{collate} {direction} NULLS {nulls}", self.column_sql(i))
                })
                .collect();
            // Ties, and a view with no sort, in the table's order.
            let keys_backward = view.reversed != query.backward;
            order.extend(
                (0..self.keys)
                    .map(|k| format!("s.k{k} {}", if keys_backward { "DESC" } else { "ASC" })),
            );
            if !order.is_empty() {
                sql.push_str(" ORDER BY ");
                sql.push_str(&order.join(", "));
            }
            let limit = query.limit.map_or(-1, |n| n as i64);
            sql.push_str(&format!(" LIMIT {limit} OFFSET {}", query.offset));
            (sql, params)
        }

        /// Run `query`, handing `each` a frame of `query.columns` a batch at a time
        /// until it says stop; the key of the last row read, when asked for.
        fn run(
            &self,
            view: &View,
            query: &Query<'_>,
            mut each: impl FnMut(DataFrame) -> PolarsResult<bool>,
        ) -> PolarsResult<Option<Vec<Value>>> {
            let (sql, params) = self.statement(view, query);
            let conn = self.connect()?;
            let mut stmt = conn.prepare(&sql).map_err(|e| self.failed(e))?;
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params.iter()))
                .map_err(|e| self.failed(e))?;
            let width = query.columns.len().max(1);
            let batch_rows = (BATCH_CELLS / width).clamp(1, BATCH_ROWS);
            let mut batch = Batch::new(self, query.columns);
            let mut last_key = None;
            loop {
                let row = match rows.next() {
                    Ok(Some(row)) => row,
                    Ok(None) => break,
                    Err(e) => return Err(self.failed(e)),
                };
                for (i, column) in batch.columns.iter_mut().enumerate() {
                    batch.bytes += column.push(row.get_ref(i).map_err(|e| self.failed(e))?);
                }
                batch.rows += 1;
                if query.with_keys {
                    let first = query.columns.len();
                    last_key = Some(
                        (first..first + self.keys)
                            .map(|k| row.get::<_, Value>(k))
                            .collect::<rusqlite::Result<Vec<_>>>()
                            .map_err(|e| self.failed(e))?,
                    );
                }
                if (batch.rows >= batch_rows || batch.bytes >= BATCH_BYTES) && !each(batch.take()?)?
                {
                    return Ok(last_key);
                }
            }
            if batch.rows > 0 || query.columns.is_empty() {
                each(batch.take()?)?;
            }
            Ok(last_key)
        }

        /// How many rows the view holds, once known.
        fn known_rows(&self, view: &View) -> Option<usize> {
            if view.conditions.is_empty()
                && let Some(census) = self.census.get()
            {
                return Some(census.rows);
            }
            self.memo.lock().ok()?.get(&view.key)?.rows
        }

        fn remember(&self, view: &View, learn: impl FnOnce(&mut Memo)) {
            let Ok(mut memo) = self.memo.lock() else {
                return;
            };
            if !memo.contains_key(&view.key) && memo.len() >= MEMO_VIEWS {
                memo.clear();
            }
            learn(memo.entry(view.key.clone()).or_default());
        }

        /// Count the view's rows: SQLite's own `count(*)`, which for a whole table
        /// reads no row.
        fn count(&self, view: &View) -> PolarsResult<usize> {
            if let Some(rows) = self.known_rows(view) {
                return Ok(rows);
            }
            let mut params = Vec::new();
            let mut sql = format!("{} SELECT count(*) FROM s", self.with);
            if let Some(c) = self.conditions_sql(view, &mut params) {
                sql.push_str(" WHERE ");
                sql.push_str(&c);
            }
            let conn = self.connect()?;
            let rows: i64 = conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })
                .map_err(|e| self.failed(e))?;
            let rows = usize::try_from(rows).unwrap_or(0);
            self.remember(view, |memo| memo.rows = Some(rows));
            Ok(rows)
        }

        /// Rows `[start, start + len)` of the view, as `columns`.
        ///
        /// Paged by the table's key where the view is in its order: from the nearest
        /// place a page ended before, rather than skipping every row from the top. A
        /// window past the middle of a view whose count is known is read from the end,
        /// backward, so the last page costs what the first does.
        fn window(
            &self,
            view: &View,
            start: usize,
            len: usize,
            columns: &[usize],
        ) -> PolarsResult<DataFrame> {
            let empty = || self.empty(columns);
            let total = self.known_rows(view);
            if len == 0 || total.is_some_and(|t| start >= t) {
                return Ok(empty());
            }
            let mut frames = Vec::new();
            if self.keys > 0
                && let Some(total) = total
                && start > total / 2
            {
                let len = len.min(total - start);
                let query = Query {
                    columns,
                    also: None,
                    backward: true,
                    after: None,
                    limit: Some(len),
                    offset: total - start - len,
                    with_keys: false,
                };
                self.run(view, &query, |df| {
                    frames.push(df);
                    Ok(true)
                })?;
                let df = concat_frames(frames, empty())?;
                return Ok(df.reverse());
            }
            let mut offset = start;
            let mut after = None;
            let paged = self.keys > 0 && view.natural();
            if paged
                && let Ok(memo) = self.memo.lock()
                && let Some((place, key)) = memo
                    .get(&view.key)
                    .and_then(|m| m.places.range(..=start).next_back())
            {
                offset = start - place;
                after = Some(key.clone());
            }
            let query = Query {
                columns,
                also: None,
                backward: false,
                after,
                limit: Some(len),
                offset,
                with_keys: paged,
            };
            let last = self.run(view, &query, |df| {
                frames.push(df);
                Ok(true)
            })?;
            let df = concat_frames(frames, empty())?;
            let read = df.height();
            if let Some(key) = last {
                self.remember(view, |memo| {
                    if memo.places.len() >= CHECKPOINTS {
                        memo.places.clear();
                    }
                    memo.places.insert(start + read, key);
                });
            }
            // A short read that began inside the view ran off its end.
            if read < len && (start == 0 || read > 0) {
                self.remember(view, |memo| memo.rows = Some(start + read));
            }
            Ok(df)
        }

        /// Every row of the view as `columns`, the first `n_rows` where given, and only
        /// those `predicate` keeps. As much of the predicate as SQLite can run is run
        /// there, and all of it is applied again to each batch, so what SQLite cannot
        /// say is left to Polars rather than lost.
        fn whole(
            &self,
            view: &View,
            columns: &[usize],
            predicate: Option<&Expr>,
            n_rows: Option<usize>,
        ) -> PolarsResult<DataFrame> {
            let mut read: Vec<usize> = columns.to_vec();
            let also = predicate.and_then(|p| {
                let mut params = Vec::new();
                self.predicate_sql(p, &mut params).map(|sql| (sql, params))
            });
            if let Some(predicate) = predicate {
                for name in predicate.clone().meta().root_names() {
                    if let Some(i) = self.index_of(&name)
                        && !read.contains(&i)
                    {
                        read.push(i);
                    }
                }
            }
            let names: Vec<PlSmallStr> = columns
                .iter()
                .map(|&i| self.columns[i].name.clone())
                .collect();
            let mut frames = Vec::new();
            let mut kept = 0usize;
            let wanted = n_rows.unwrap_or(usize::MAX);
            let query = Query {
                columns: &read,
                also,
                backward: false,
                after: None,
                limit: predicate.is_none().then_some(n_rows).flatten(),
                offset: 0,
                with_keys: false,
            };
            self.run(view, &query, |df| {
                let df = match predicate {
                    Some(p) => df.lazy().filter(p.clone()).collect()?,
                    None => df,
                };
                let df = df.select(names.iter().cloned())?;
                kept += df.height();
                frames.push(df);
                Ok(kept < wanted)
            })?;
            let mut df = concat_frames(frames, self.empty(columns))?;
            if df.height() > wanted {
                df = df.head(Some(wanted));
            }
            Ok(df)
        }

        /// A frame of no rows of `columns`.
        fn empty(&self, columns: &[usize]) -> DataFrame {
            DataFrame::empty_with_schema(&Schema::from_iter(
                columns.iter().map(|&i| {
                    Field::new(self.columns[i].name.clone(), self.columns[i].kind.dtype())
                }),
            ))
        }
    }

    impl Source {
        /// As much of a predicate Polars pushed into the scan as SQLite can run, as a
        /// condition that keeps every row the predicate keeps (and maybe more, which the
        /// predicate then drops): comparisons of a column with a literal of its type,
        /// joined by and and or.
        fn predicate_sql(&self, e: &Expr, params: &mut Vec<Value>) -> Option<String> {
            let Expr::BinaryExpr { left, op, right } = e else {
                return None;
            };
            match op {
                Operator::And | Operator::LogicalAnd => {
                    let mut left_params = Vec::new();
                    let mut right_params = Vec::new();
                    let l = self.predicate_sql(left, &mut left_params);
                    let r = self.predicate_sql(right, &mut right_params);
                    // Either half alone keeps all the rows both keep.
                    match (l, r) {
                        (Some(l), Some(r)) => {
                            params.extend(left_params);
                            params.extend(right_params);
                            Some(format!("({l} AND {r})"))
                        }
                        (Some(l), None) => {
                            params.extend(left_params);
                            Some(l)
                        }
                        (None, Some(r)) => {
                            params.extend(right_params);
                            Some(r)
                        }
                        (None, None) => None,
                    }
                }
                Operator::Or | Operator::LogicalOr => {
                    let mut both = Vec::new();
                    let l = self.predicate_sql(left, &mut both)?;
                    let r = self.predicate_sql(right, &mut both)?;
                    params.extend(both);
                    Some(format!("({l} OR {r})"))
                }
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    // The column on the left, flipping the comparison if it is not.
                    let (name, value, op) = match (&**left, &**right) {
                        (Expr::Column(name), Expr::Literal(value)) => (name, value, *op),
                        (Expr::Literal(value), Expr::Column(name)) => (name, value, flipped(*op)),
                        _ => return None,
                    };
                    let i = self.index_of(name)?;
                    let kind = self.columns[i].kind;
                    let value = literal(value, kind)?;
                    let sign = match op {
                        Operator::Eq => "=",
                        Operator::NotEq => "<>",
                        Operator::Lt => "<",
                        Operator::LtEq => "<=",
                        Operator::Gt => ">",
                        _ => ">=",
                    };
                    let collate = if kind == Kind::Text {
                        " COLLATE BINARY"
                    } else {
                        ""
                    };
                    params.push(value);
                    Some(format!("{} {sign} ?{collate}", self.compared_sql(i)))
                }
                _ => None,
            }
        }

        /// Each column's values that are not of its kind, and the rows: one pass over
        /// the table, in the background, once. Until it is done, every column is read
        /// through what the frame shows (see [`Kind::read`]); after, a column found
        /// clean is read as stored, where an index on it can serve a filter or sort.
        fn take_census(&self) -> PolarsResult<()> {
            let conn = self.connect()?;
            let mut misfits = vec![0u64; self.columns.len()];
            let mut rows = 0usize;
            let chunks: Vec<Vec<usize>> = (0..self.columns.len().max(1))
                .collect::<Vec<_>>()
                .chunks(CENSUS_COLUMNS)
                .map(<[usize]>::to_vec)
                .collect();
            for chunk in chunks {
                let mut select = vec!["count(*)".to_string()];
                select.extend(chunk.iter().filter(|&&i| i < self.columns.len()).map(|&i| {
                    format!(
                        "total(typeof(s.c{i}) NOT IN ({}))",
                        self.columns[i].kind.classes()
                    )
                }));
                let sql = format!("{} SELECT {} FROM s", self.with, select.join(", "));
                conn.query_row(&sql, [], |row| {
                    rows = usize::try_from(row.get::<_, i64>(0)?).unwrap_or(0);
                    for (n, &i) in chunk.iter().enumerate() {
                        if i < misfits.len() {
                            misfits[i] = row.get::<_, f64>(n + 1)? as u64;
                        }
                    }
                    Ok(())
                })
                .map_err(|e| self.failed(e))?;
            }
            let _ = self.census.set(Census { rows, misfits });
            Ok(())
        }

        /// What reading the table found to say: columns of several types read as text,
        /// number columns with values that are not numbers (read as null), and blob
        /// columns with values that are not blobs (read as their bytes).
        fn notes(&self) -> Vec<Note> {
            let census = self.census.get();
            let misfit = |i: usize| census.map_or(0, |c| c.misfits[i]);
            let scope = format!("of the {} {}", self.table.kind, self.table.name);
            let note = |summary: String| Note {
                summary,
                scope: scope.clone(),
                read_as_text: None,
                passed_over: None,
            };
            let mut notes = Vec::new();
            let mixed: Vec<&str> = self
                .columns
                .iter()
                .enumerate()
                .filter(|(i, c)| c.kind == Kind::Text && (c.mixed || misfit(*i) > 0))
                .map(|(_, c)| c.name.as_str())
                .collect();
            if !mixed.is_empty() {
                notes.push(note(format!(
                    "{}: mixed types, read as text",
                    mixed.join(", ")
                )));
            }
            for (i, column) in self.columns.iter().enumerate() {
                let n = misfit(i);
                if n == 0 {
                    continue;
                }
                let values = format!(
                    "{} {}",
                    group_chrome(n as usize),
                    if n == 1 { "value" } else { "values" }
                );
                match column.kind {
                    Kind::Int => notes.push(note(format!(
                        "{}: {values} not whole numbers, read as null",
                        column.name
                    ))),
                    Kind::Float => notes.push(note(format!(
                        "{}: {values} not numbers, read as null",
                        column.name
                    ))),
                    Kind::Bytes => notes.push(note(format!(
                        "{}: {values} not blobs, read as their bytes",
                        column.name
                    ))),
                    Kind::Text => {}
                }
            }
            notes
        }
    }

    fn flipped(op: Operator) -> Operator {
        match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            other => other,
        }
    }

    /// A Polars literal as a value a column of `kind` compares with as Polars would.
    fn literal(value: &LiteralValue, kind: Kind) -> Option<Value> {
        // Scalars and the untyped literals Polars has not yet given a type.
        if !value.is_scalar() {
            return None;
        }
        let any = value.to_any_value()?.into_static();
        match (kind, any) {
            (Kind::Int | Kind::Float, AnyValue::Int8(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::Int16(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::Int32(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::Int64(v)) => Some(Value::Integer(v)),
            (Kind::Int | Kind::Float, AnyValue::UInt8(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::UInt16(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::UInt32(v)) => Some(Value::Integer(v.into())),
            (Kind::Int | Kind::Float, AnyValue::Float64(v)) if !v.is_nan() => Some(Value::Real(v)),
            (Kind::Int | Kind::Float, AnyValue::Float32(v)) if !v.is_nan() => {
                Some(Value::Real(v.into()))
            }
            (Kind::Text, AnyValue::String(s)) => Some(Value::Text(s.to_string())),
            (Kind::Text, AnyValue::StringOwned(s)) => Some(Value::Text(s.to_string())),
            _ => None,
        }
    }

    /// The frames of a read as one, or `empty` when there were none.
    fn concat_frames(frames: Vec<DataFrame>, empty: DataFrame) -> PolarsResult<DataFrame> {
        let mut frames = frames.into_iter();
        let Some(mut df) = frames.next() else {
            return Ok(empty);
        };
        for next in frames {
            df.vstack_mut_owned(next)?;
        }
        df.rechunk_mut();
        Ok(df)
    }

    /// The rows read since the last batch was handed on, a column at a time.
    struct Batch {
        columns: Vec<Buffer>,
        names: Vec<PlSmallStr>,
        rows: usize,
        bytes: usize,
    }

    enum Buffer {
        Int(Vec<Option<i64>>),
        Float(Vec<Option<f64>>),
        Text(Vec<Option<String>>),
        Bytes(Vec<Option<Vec<u8>>>),
    }

    impl Batch {
        fn new(source: &Source, columns: &[usize]) -> Self {
            Self {
                columns: columns
                    .iter()
                    .map(|&i| match source.columns[i].kind {
                        Kind::Int => Buffer::Int(Vec::new()),
                        Kind::Float => Buffer::Float(Vec::new()),
                        Kind::Text => Buffer::Text(Vec::new()),
                        Kind::Bytes => Buffer::Bytes(Vec::new()),
                    })
                    .collect(),
                names: columns
                    .iter()
                    .map(|&i| source.columns[i].name.clone())
                    .collect(),
                rows: 0,
                bytes: 0,
            }
        }

        fn take(&mut self) -> PolarsResult<DataFrame> {
            let height = self.rows;
            let columns: Vec<Column> = self
                .names
                .iter()
                .zip(self.columns.iter_mut())
                .map(|(name, buffer)| buffer.take(name.clone()).into())
                .collect();
            self.rows = 0;
            self.bytes = 0;
            DataFrame::new(height, columns)
        }
    }

    impl Buffer {
        /// Add `value`, already read as this column's kind by the statement; the text
        /// and blob bytes it adds.
        fn push(&mut self, value: ValueRef<'_>) -> usize {
            match (self, value) {
                (Self::Int(v), ValueRef::Integer(i)) => v.push(Some(i)),
                (Self::Int(v), _) => v.push(None),
                (Self::Float(v), ValueRef::Integer(i)) => v.push(Some(i as f64)),
                (Self::Float(v), ValueRef::Real(f)) => v.push(Some(f)),
                (Self::Float(v), _) => v.push(None),
                (Self::Text(v), ValueRef::Text(b)) => {
                    v.push(Some(String::from_utf8_lossy(b).into_owned()));
                    return b.len();
                }
                (Self::Text(v), ValueRef::Integer(i)) => v.push(Some(i.to_string())),
                (Self::Text(v), ValueRef::Real(f)) => v.push(Some(f.to_string())),
                (Self::Text(v), ValueRef::Blob(b)) => {
                    v.push(Some(blob_text(b)));
                    return 2 * b.len();
                }
                (Self::Text(v), ValueRef::Null) => v.push(None),
                (Self::Bytes(v), ValueRef::Blob(b) | ValueRef::Text(b)) => {
                    v.push(Some(b.to_vec()));
                    return b.len();
                }
                (Self::Bytes(v), _) => v.push(None),
            }
            0
        }

        fn take(&mut self, name: PlSmallStr) -> Series {
            match self {
                Self::Int(v) => Series::new(name, std::mem::take(v)),
                Self::Float(v) => Series::new(name, std::mem::take(v)),
                Self::Text(v) => Series::new(name, std::mem::take(v)),
                Self::Bytes(v) => {
                    BinaryChunked::from_iter_options(name, std::mem::take(v).into_iter())
                        .into_series()
                }
            }
        }
    }

    /// A blob as an SQL literal, `X'0A1B'`, as SQLite's own `hex` spells it.
    fn blob_text(b: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut text = String::with_capacity(3 + 2 * b.len());
        text.push_str("X'");
        for byte in b {
            let _ = write!(text, "{byte:02X}");
        }
        text.push('\'');
        text
    }

    /// The name a SQLite scan carries in a plan.
    pub const SCAN_NAME: &str = "SQLITE";

    /// A view as a Polars scan: all of it, or a window of it.
    struct Scan {
        source: Arc<Source>,
        view: Arc<View>,
        window: Option<(usize, usize)>,
    }

    impl Scan {
        fn into_lazy(self) -> PolarsResult<LazyFrame> {
            let schema = self.source.schema.clone();
            LazyFrame::anonymous_scan(
                Arc::new(self),
                ScanArgsAnonymous {
                    schema: Some(schema),
                    name: SCAN_NAME,
                    ..Default::default()
                },
            )
        }
    }

    impl AnonymousScan for Scan {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
            Ok(self.source.schema.clone())
        }

        fn allows_projection_pushdown(&self) -> bool {
            true
        }

        // A window's rows are counted before any filter on top of them, so a filter
        // stays above it; the whole view takes one.
        fn allows_predicate_pushdown(&self) -> bool {
            self.window.is_none()
        }

        fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
            let columns: Vec<usize> = match &args.with_columns {
                Some(names) => names
                    .iter()
                    .map(|name| {
                        self.source
                            .index_of(name)
                            .ok_or_else(|| polars_err!(ColumnNotFound: "{name}"))
                    })
                    .collect::<PolarsResult<_>>()?,
                None => (0..self.source.columns.len()).collect(),
            };
            match self.window {
                Some((start, len)) => {
                    let len = args.n_rows.map_or(len, |n| n.min(len));
                    self.source.window(&self.view, start, len, &columns)
                }
                None => {
                    self.source
                        .whole(&self.view, &columns, args.predicate.as_ref(), args.n_rows)
                }
            }
        }
    }

    /// The windows of a view.
    struct Windows {
        source: Arc<Source>,
        view: Arc<View>,
    }

    impl Windowed for Windows {
        fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
            Scan {
                source: self.source.clone(),
                view: self.view.clone(),
                window: Some((start, len)),
            }
            .into_lazy()
        }
    }

    /// A table the dataset runs its filters and sort in.
    struct InPlace(Arc<Source>);

    impl InPlace {
        fn pushed(&self, view: View) -> Option<PushedView> {
            let view = Arc::new(view);
            let source = self.0.clone();
            let lf = Scan {
                source: source.clone(),
                view: view.clone(),
                window: None,
            }
            .into_lazy()
            .ok()?;
            let counter: Counter = {
                let (source, view) = (source.clone(), view.clone());
                Arc::new(move || source.count(&view))
            };
            Some(PushedView {
                lf,
                window: Arc::new(Windows { source, view }),
                counter,
            })
        }
    }

    impl Pushdown for InPlace {
        fn view(
            &self,
            filters: &[FilterStatement],
            sort: &[(String, bool)],
            reversed: bool,
        ) -> Option<PushedView> {
            let source = &self.0;
            let conditions = filters
                .iter()
                .map(|f| Some((f.logical_op, source.atom(f)?)))
                .collect::<Option<Vec<_>>>()?;
            let sort = sort
                .iter()
                .map(|(name, descending)| Some((source.index_of(name)?, *descending)))
                .collect::<Option<Vec<_>>>()?;
            let reversed = reversed && sort.is_empty();
            // A view has no order of its own to run backward.
            if reversed && source.keys == 0 {
                return None;
            }
            self.pushed(View::new(conditions, sort, reversed))
        }

        fn notes(&self) -> Vec<Note> {
            self.0.notes()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Open `table` of the database `file` (named `display` to the user) in place: the
    /// frame over it, what the dataset runs its filters and sort through, and the hold
    /// that stops its statements when the dataset lets go. `others` are the database's
    /// other tables, for the Info panel.
    pub fn open_table(
        file: &Path,
        display: &Path,
        table: &Table,
        others: &[Table],
    ) -> Result<Opened> {
        open_table_with(file, display, table, others, true)
    }

    /// As [`open_table`], with or without the census behind it: a test that takes the
    /// census itself must not race one already running.
    pub(super) fn open_table_with(
        file: &Path,
        display: &Path,
        table: &Table,
        others: &[Table],
        census: bool,
    ) -> Result<Opened> {
        let source = Arc::new(Source::open(file, display, table)?);
        let hold = Hold(source.stop.clone());
        // The census, in the background: a pass over the table, stopped with it.
        if census {
            let source = source.clone();
            std::thread::Builder::new()
                .name("sqlite-census".to_string())
                .spawn(move || {
                    if let Err(e) = source.take_census() {
                        log::debug!(target: "datui", "sqlite census: {e}");
                    }
                })?;
        }
        let table_source = InPlace(source);
        let whole = table_source
            .pushed(View::whole())
            .ok_or_else(|| FileError::new(display, format!("could not read \"{}\"", table.name)))?;
        Ok(Opened {
            lf: whole.lf,
            pushdown: Arc::new(table_source),
            hold,
            other_tables: other_tables(table, others),
        })
    }

    /// The columns `table` opens with, read from its first rows the way the open
    /// decides them, for the home screen's preview. Gives up after a moment.
    pub fn schema_preview(path: &Path, table: &Table) -> Option<crate::discover::SchemaPreview> {
        const SAMPLE: usize = 100;
        let conn = open(path).ok()?;
        let stop = Arc::new(AtomicBool::new(false));
        stoppable(&conn, stop, Some(Instant::now() + PREVIEW_TIME)).ok()?;
        let sql = format!("SELECT * FROM main.{} LIMIT {SAMPLE}", quoted(&table.name));
        let mut stmt = conn.prepare(&sql).ok()?;
        let (names, affinities) = described(&stmt, table);
        let mut seen = vec![Seen::default(); names.len()];
        let mut rows = stmt.query([]).ok()?;
        while let Some(row) = rows.next().ok()? {
            for (i, seen) in seen.iter_mut().enumerate() {
                seen.add(row.get_ref(i).ok()?);
            }
        }
        Some(
            names
                .into_iter()
                .zip(affinities)
                .zip(seen)
                .map(|((name, (affinity, declared)), seen)| {
                    (name, decide(affinity, &declared, seen).0.dtype())
                })
                .collect(),
        )
    }

    /// An error from SQLite, said about the database by the name the user knows.
    fn named(e: color_eyre::Report, display: &Path) -> color_eyre::Report {
        crate::error_display::in_file(display, e)
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

    #[cfg(test)]
    pub(super) fn open_for_tests(path: &Path) -> Result<Connection> {
        open(path)
    }

    /// The source behind a table opened in place.
    #[cfg(test)]
    fn source_of(opened: &Opened) -> &Arc<Source> {
        &opened
            .pushdown
            .as_any()
            .downcast_ref::<InPlace>()
            .expect("a table in place")
            .0
    }

    /// Take the census now, as the background pass would.
    #[cfg(test)]
    pub(super) fn census_for_tests(opened: &Opened) {
        source_of(opened).take_census().unwrap();
    }

    /// The statement a window of the view with `filters` and `sort` runs, and SQLite's
    /// plan for it.
    #[cfg(test)]
    pub(super) fn plan_for_tests(
        opened: &Opened,
        filters: &[FilterStatement],
        sort: &[(String, bool)],
    ) -> (String, String) {
        let source = source_of(opened);
        let conditions = filters
            .iter()
            .map(|f| (f.logical_op, source.atom(f).unwrap()))
            .collect();
        let sort = sort
            .iter()
            .map(|(name, d)| (source.index_of(name).unwrap(), *d))
            .collect();
        let view = View::new(conditions, sort, false);
        let columns: Vec<usize> = (0..source.columns.len()).collect();
        let query = Query {
            columns: &columns,
            also: None,
            backward: false,
            after: None,
            limit: Some(10),
            offset: 0,
            with_keys: false,
        };
        let (sql, params) = source.statement(&view, &query);
        let conn = source.connect().unwrap();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let plan: Vec<String> = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        (sql, plan.join("\n"))
    }

    #[cfg(test)]
    pub(super) fn immutable_uri_for_tests(path: &Path) -> String {
        immutable_uri(path)
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests;

/// The scan of a SQLite database: the table `--table` names, or the database's only
/// table of its own, read in place; or none yet when it has several.
fn scan(input: crate::readers::ScanIn<'_>) -> color_eyre::Result<crate::scan::Scan> {
    let file = input.path();
    let tables = tables(file)?;
    match pick(tables.clone(), input.options.table.as_deref(), file)? {
        Pick::One(table) => {
            let opened = open_table(file, file, &table, &tables)?;
            let detail = detail(file, &tables).map(std::sync::Arc::new);
            // The only table, when none was named: what reading it again names.
            input.report.table = Some(table.name.clone());
            input.report.opened = Some(std::sync::Arc::new(crate::members::Opened {
                detail,
                ..Default::default()
            }));
            input.report.sqlite = Some(std::sync::Arc::new(crate::SqliteOpen {
                pushdown: opened.pushdown,
                hold: std::sync::Mutex::new(Some(opened.hold)),
                other_tables: opened.other_tables,
            }));
            Ok(opened.lf.into())
        }
        Pick::Several(tables) => Ok(crate::scan::Scan::Tables {
            file: file.to_path_buf(),
            tables: tables
                .into_iter()
                .filter(|t| !t.internal)
                .map(|t| t.name)
                .collect(),
            format: input.format,
        }),
    }
}

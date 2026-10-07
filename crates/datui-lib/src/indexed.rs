//! Tables of records an index found: the messages of one type in a flight log, the
//! frames of one CAN message in a candump.
//!
//! A log interleaves its message types, so where the rows of one type are is not
//! arithmetic. One pass over the file records where each record of each type starts
//! ([`Offsets`]); a table is then decoded from a map of the file at those offsets
//! through the decode core of the binary format specs ([`crate::fixed_records`]), only
//! the rows and columns a view reaches. The pass's result is kept ([`cached`]), so the
//! home screen can list a log's tables and each opens without reading the file again.
//! A log whose tables are its record types is a [`Log`].

use std::any::{Any, TypeId};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use polars::prelude::*;

use crate::fixed_records::{Bytes, ColumnLayout};
use crate::members::{Opened, Pick, Table};
use crate::readers::ScanIn;
use crate::scan::Scan;
use crate::text_formats::Detail;

/// Records one pass indexes, all types together. Four bytes a record for a file under
/// 4 GiB, eight past it: 256 MiB at most for a file under 4 GiB.
pub const MAX_RECORDS: usize = 64 << 20;

/// Where each record of one type starts, four bytes a record when the file allows.
#[derive(Debug, Clone)]
pub enum Offsets {
    Narrow(Vec<u32>),
    Wide(Vec<u64>),
}

impl Default for Offsets {
    fn default() -> Self {
        Self::Narrow(Vec::new())
    }
}

impl Offsets {
    /// An empty list for a file of `len` bytes.
    pub fn for_file(len: usize) -> Self {
        if u32::try_from(len).is_ok() {
            Self::Narrow(Vec::new())
        } else {
            Self::Wide(Vec::new())
        }
    }

    pub fn push(&mut self, at: usize) {
        match self {
            Self::Narrow(v) => v.push(at as u32),
            Self::Wide(v) => v.push(at as u64),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Narrow(v) => v.len(),
            Self::Wide(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, i: usize) -> usize {
        match self {
            Self::Narrow(v) => v[i] as usize,
            Self::Wide(v) => v[i] as usize,
        }
    }

    /// Add `other`'s offsets after these, widening these when `other` is wide.
    pub fn append(&mut self, other: &Offsets) {
        match (&mut *self, other) {
            (Self::Narrow(v), Self::Narrow(o)) => v.extend_from_slice(o),
            (Self::Wide(v), Self::Wide(o)) => v.extend_from_slice(o),
            (Self::Wide(v), Self::Narrow(o)) => v.extend(o.iter().map(|&at| u64::from(at))),
            (Self::Narrow(v), Self::Wide(o)) => {
                let mut wide: Vec<u64> = v.iter().map(|&at| u64::from(at)).collect();
                wide.extend_from_slice(o);
                *self = Self::Wide(wide);
            }
        }
    }

    /// Keep the first `len`.
    pub fn truncate(&mut self, len: usize) {
        match self {
            Self::Narrow(v) => v.truncate(len),
            Self::Wide(v) => v.truncate(len),
        }
    }

    /// Eight bytes an offset once a file of `len` bytes needs them.
    pub fn widen_for(&mut self, len: usize) {
        if let Self::Narrow(v) = self
            && u32::try_from(len).is_err()
        {
            *self = Self::Wide(v.iter().map(|&at| u64::from(at)).collect());
        }
    }

    pub fn shrink(&mut self) {
        match self {
            Self::Narrow(v) => v.shrink_to_fit(),
            Self::Wide(v) => v.shrink_to_fit(),
        }
    }
}

/// The records of one type: fixed columns at offsets into each record.
pub struct IndexedRecords {
    bytes: Arc<Bytes>,
    offsets: Arc<Offsets>,
    columns: Vec<ColumnLayout>,
    schema: SchemaRef,
}

impl IndexedRecords {
    /// The records starting at `offsets` in `bytes`, each read as `columns`, whose
    /// starts count from the record's. The index pass has checked every record holds
    /// every column.
    pub fn new(
        bytes: Arc<Bytes>,
        offsets: Arc<Offsets>,
        columns: Vec<ColumnLayout>,
    ) -> PolarsResult<Self> {
        let schema: Schema = columns
            .iter()
            .map(|c| Field::new(c.name.clone(), c.dtype()))
            .collect();
        polars_ensure!(
            schema.len() == columns.len(),
            Duplicate: "two columns have the same name"
        );
        Ok(Self {
            bytes,
            offsets,
            columns,
            schema: Arc::new(schema),
        })
    }

    pub fn rows(&self) -> usize {
        self.offsets.len().min(crate::row_index::MAX_ROWS)
    }

    pub fn lazy(self: &Arc<Self>) -> LazyFrame {
        crate::row_index::lazy(self)
    }

    fn starts(&self, rows: impl Iterator<Item = usize>) -> Vec<usize> {
        rows.map(|i| self.offsets.get(i)).collect()
    }

    /// Rows `[start, start + len)`, decoded now.
    pub fn collect_window(&self, start: usize, len: usize) -> PolarsResult<DataFrame> {
        let start = start.min(self.rows());
        let len = len.min(self.rows() - start);
        self.bytes.still_whole()?;
        let starts = self.starts(start..start + len);
        let columns = self
            .columns
            .iter()
            .map(|c| crate::fixed_records::decode_at(self.bytes.as_slice(), c, &starts))
            .collect::<PolarsResult<Vec<_>>>()?;
        DataFrame::new(len, columns)
    }
}

impl crate::row_index::RowSource for IndexedRecords {
    fn height(&self) -> usize {
        self.rows()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode(&self, column: usize, index: &IdxCa) -> PolarsResult<Column> {
        let rows = crate::row_index::checked(index, self.rows())?;
        self.bytes.still_whole()?;
        let starts = self.starts(rows.iter().map(|&r| r as usize));
        crate::fixed_records::decode_at(self.bytes.as_slice(), &self.columns[column], &starts)
    }
}

impl crate::pushdown::Windowed for IndexedRecords {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        Ok(self.collect_window(start, len)?.lazy())
    }
}

// --- What a pass found, kept -------------------------------------------------------

/// Indexes always kept: a few logs open in a session.
const KEPT: usize = 4;
/// More are kept while the files they index total this many bytes: an index's
/// size follows its file's, so a count alone let four huge logs hold gigabytes
/// while a fifth small one pushed out an index still listed on the home screen.
const KEPT_FILE_BYTES: u64 = 2 << 30;
/// And never more than this many.
const MOST_KEPT: usize = 64;

type Key = (PathBuf, u64, Option<std::time::SystemTime>, TypeId);

static KEPT_INDEXES: Mutex<Vec<(Key, Arc<dyn Any + Send + Sync>)>> = Mutex::new(Vec::new());

fn key<T: 'static>(path: &Path) -> Option<Key> {
    let meta = std::fs::metadata(path).ok()?;
    let path = crate::canonical::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    Some((path, meta.len(), meta.modified().ok(), TypeId::of::<T>()))
}

/// The index of `path` a pass already made, while the file is as it was.
pub fn peek<T: Any + Send + Sync>(path: &Path) -> Option<Arc<T>> {
    let key = key::<T>(path)?;
    let kept = KEPT_INDEXES.lock().unwrap_or_else(|e| e.into_inner());
    kept.iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, index)| index.clone().downcast::<T>().ok())
}

/// Let go of the index of `path` kept for type `T`.
pub fn forget<T: Any + Send + Sync>(path: &Path) {
    if let Some(key) = key::<T>(path) {
        let mut kept = KEPT_INDEXES.lock().unwrap_or_else(|e| e.into_inner());
        kept.retain(|(k, _)| *k != key);
    }
}

/// The index of `path`: the one kept, or `build`'s, which is then kept.
pub fn cached<T: Any + Send + Sync, E>(
    path: &Path,
    build: impl FnOnce() -> Result<T, E>,
) -> Result<Arc<T>, E> {
    if let Some(index) = peek::<T>(path) {
        return Ok(index);
    }
    let index = Arc::new(build()?);
    keep(path, index.clone());
    Ok(index)
}

/// Keep `index` as the one of `path`, in place of any kept before.
pub fn keep<T: Any + Send + Sync>(path: &Path, index: Arc<T>) {
    if let Some(key) = key::<T>(path) {
        let mut kept = KEPT_INDEXES.lock().unwrap_or_else(|e| e.into_inner());
        kept.retain(|(k, _)| *k != key);
        kept.push((key, index as Arc<dyn Any + Send + Sync>));
        // Oldest first, until what is left fits.
        let mut total: u64 = kept.iter().map(|((_, len, ..), _)| *len).sum();
        let mut excess = 0;
        while kept.len() - excess > KEPT
            && (total > KEPT_FILE_BYTES || kept.len() - excess > MOST_KEPT)
        {
            total -= kept[excess].0.1;
            excess += 1;
        }
        kept.drain(..excess);
    }
}

/// The bytes of the log at `path` and its index, made by `index` in one pass or kept
/// from one.
pub(crate) fn indexed<T: Any + Send + Sync>(
    path: &Path,
    index: impl FnOnce(&[u8]) -> Result<T, String>,
) -> color_eyre::Result<(Arc<Bytes>, Arc<T>)> {
    use crate::error_display::{FileError, in_file};
    let bytes = Arc::new(Bytes::map(path).map_err(|e| in_file(path, e.into()))?);
    let index = cached(path, || index(bytes.as_slice())).map_err(|e| FileError::new(path, e))?;
    Ok((bytes, index))
}

/// A log of record types one pass indexes, each type a table: a flight log. A new one
/// is this and a `READER` with [`scan`] and [`listed`].
pub trait Log: Any + Send + Sync + Sized {
    /// Said after "the file holds no tables." of a log with none.
    const EMPTY: &'static str;
    fn index(data: &[u8]) -> Result<Self, String>;
    /// Its tables, for the home screen and `--table`.
    fn tables(&self) -> Vec<Table>;
    /// What the Info panel's tab of it says.
    fn detail(&self) -> Detail;
    /// What the Notes tab says of the pass.
    fn notes(&self) -> Vec<String>;
    /// The table `name`, one of [`Self::tables`], read from `bytes`, filling what of
    /// `opened` it knows (its window, units).
    fn table(
        &self,
        bytes: Arc<Bytes>,
        name: &str,
        opened: &mut Opened,
    ) -> Result<LazyFrame, String>;
}

/// The log's tables as its indexing pass found them: listed once it has been opened,
/// and not read here, where the home screen waits.
pub(crate) fn listed<L: Log>(path: &Path) -> color_eyre::Result<Vec<Table>> {
    peek::<L>(path)
        .map(|log| log.tables())
        .ok_or_else(|| color_eyre::eyre::eyre!("Open the log to list its tables."))
}

/// The scan of a log: the table `--table` names, or its only one, decoded from the file
/// where it is shown; or none yet when it has several. The pass that indexes the log is
/// kept, so a table chosen from the list reads nothing again.
pub(crate) fn scan<L: Log>(input: ScanIn<'_>) -> color_eyre::Result<Scan> {
    let path = input.path().to_path_buf();
    let (bytes, log) = indexed(&path, L::index)?;
    let tables = log.tables();
    let picked = match crate::members::pick(
        tables.clone(),
        input.options.table.as_deref(),
        &path,
        L::EMPTY,
    )? {
        Pick::One(table) => table.name,
        Pick::Several(tables) => return Ok(crate::members::several(&input, tables)),
    };
    let mut opened = Opened::for_table(log.detail(), &tables, &picked, log.notes(), "the log");
    let lf = log
        .table(bytes, &picked, &mut opened)
        .map_err(|e| crate::error_display::FileError::new(&path, e))?;
    Ok(opened.scan(input, lf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed_records::Physical;

    #[test]
    fn records_at_offsets_decode_and_window() {
        // Records of (u16, i32) at 0 and 10, with bytes between them.
        let mut bytes = vec![0xEEu8; 20];
        for (at, a, b) in [(0usize, 1u16, -1i32), (10, 2, -2)] {
            bytes[at..at + 2].copy_from_slice(&a.to_le_bytes());
            bytes[at + 2..at + 6].copy_from_slice(&b.to_le_bytes());
        }
        let mut offsets = Offsets::for_file(bytes.len());
        for at in [0, 10] {
            offsets.push(at);
        }
        let records = Arc::new(
            IndexedRecords::new(
                Arc::new(Bytes::Owned(bytes)),
                Arc::new(offsets),
                vec![
                    ColumnLayout::new("a", 0, 0, Physical::Unsigned(2), 2),
                    ColumnLayout::new("b", 2, 0, Physical::Signed(4), 4),
                ],
            )
            .unwrap(),
        );
        let df = records.lazy().collect().unwrap();
        assert_eq!(
            df.column("b").unwrap().i32().unwrap().to_vec(),
            [Some(-1), Some(-2)]
        );
        let w = records.collect_window(1, 5).unwrap();
        assert_eq!(w.column("a").unwrap().u16().unwrap().to_vec(), [Some(2)]);
    }

    #[test]
    fn an_index_is_kept_until_the_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.bin");
        std::fs::write(&path, b"one").unwrap();
        let built = std::cell::Cell::new(0);
        let build = || -> Result<usize, ()> {
            built.set(built.get() + 1);
            Ok(7)
        };
        assert_eq!(*cached(&path, build).unwrap(), 7);
        assert_eq!(*cached(&path, build).unwrap(), 7);
        assert_eq!(built.get(), 1);
        std::fs::write(&path, b"longer").unwrap();
        assert!(peek::<usize>(&path).is_none());
    }
}

//! The home screen's `ROWS` preview: the first rows of the selected file, read the way
//! the open reads them and handed to the open as its first page (#547 M4).
//!
//! File reads are not spent twice. The worker runs the open's own scan and schema read
//! and fills the page the table would ask for first; the pane shows a few rows of it,
//! and Enter installs the dataset it built. The open then has nothing left to read
//! for that page, and no scan or schema read of its own.

use crate::FileFormat;
use crate::home::discover::{Entry, EntryKind};
use crate::table::DataTableState;
use polars::prelude::{DataFrame, DataType};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// Rows the pane shows.
pub const PREVIEW_ROWS: usize = 8;
/// Columns kept for the pane. More than a wide pane shows; the rest are counted.
const PREVIEW_COLUMNS: usize = 24;
/// Previews kept for the session, newest last.
const KEPT: usize = 32;
/// A cell longer than this is cut when it is kept: the pane never shows more.
const CELL_MAX: usize = 40;

/// What a file was when it was read: its size and modification time. A file that
/// changed since is read again, and its old page is never installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub len: Option<u64>,
    pub modified: Option<SystemTime>,
}

impl Stamp {
    /// From what the listing already knows of a row, so nothing is stat'ed to draw.
    pub fn of_entry(entry: &Entry) -> Self {
        Self {
            len: entry.size,
            modified: entry.modified,
        }
    }

    /// From the file as it is now.
    pub fn of_file(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        Some(Self {
            len: Some(meta.len()),
            modified: meta.modified().ok(),
        })
    }
}

/// The first rows of a file, as text, for the pane: the leading columns and how many
/// there are in all.
#[derive(Debug, Clone, PartialEq)]
pub struct PreviewRows {
    /// The leading columns, named and typed.
    pub columns: Vec<(String, DataType)>,
    /// Each row's cells, `None` for a null.
    pub rows: Vec<Vec<Option<String>>>,
    /// Columns in the dataset, the ones not kept included.
    pub total_columns: usize,
}

impl PreviewRows {
    /// The first [`PREVIEW_ROWS`] rows of `df`, its leading columns only.
    pub fn from_frame(df: &DataFrame) -> Self {
        let columns: Vec<_> = df
            .columns()
            .iter()
            .filter(|c| c.name().as_str() != crate::formats::schema_union::DRIFT_COLUMN)
            .collect();
        let total_columns = columns.len();
        let kept: Vec<_> = columns.into_iter().take(PREVIEW_COLUMNS).collect();
        let height = df.height().min(PREVIEW_ROWS);
        let rows = (0..height)
            .map(|i| {
                kept.iter()
                    .map(|c| match c.get(i) {
                        Ok(polars::prelude::AnyValue::Null) | Err(_) => None,
                        Ok(value) => {
                            let text = crate::exact::str_value(&value);
                            // One line: a newline inside a cell would break the row.
                            let line: String = text
                                .chars()
                                .map(|ch| if ch.is_control() { ' ' } else { ch })
                                .take(CELL_MAX)
                                .collect();
                            Some(line)
                        }
                    })
                    .collect()
            })
            .collect();
        Self {
            columns: kept
                .iter()
                .map(|c| (c.name().to_string(), c.dtype().clone()))
                .collect(),
            rows,
            total_columns,
        }
    }
}

/// A dataset built and its first page read, waiting for the open that will show it.
pub struct Prepared {
    pub(crate) state: Box<DataTableState>,
    pub(crate) options: crate::OpenOptions,
    pub(crate) debug_label: Option<String>,
    pub(crate) progress: Arc<crate::formats::schema_union::FooterProgress>,
}

/// A prepared dataset passed from the preview to the open that takes it, once.
pub type Handoff = Arc<Mutex<Option<Box<Prepared>>>>;

/// The previews read this session, and the one dataset waiting to be opened.
#[derive(Default)]
pub struct Previews {
    /// What was read for each file, at the stamp it was read at. `None` for a file
    /// that could not be previewed, so it is not asked about again.
    rows: HashMap<PathBuf, (Stamp, Option<Arc<PreviewRows>>)>,
    order: VecDeque<PathBuf>,
    /// The newest preview's dataset. Only one is kept: an open is of the row the
    /// cursor is on, and a dataset per row passed over would hold their pages.
    prepared: Option<(PathBuf, Stamp, Box<Prepared>)>,
    /// The file being read. One at a time: a cursor run down a directory reads the
    /// row it rests on, not every row it passed.
    pub(crate) inflight: Option<PathBuf>,
}

/// The reads of data the app has started, by kind: what a test counts to show a read
/// was not made twice.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReadCounts {
    /// Previews read for the home screen.
    pub previews: usize,
    /// Opens that scanned their paths.
    pub scans: usize,
    /// Pages of rows read for a dataset on screen.
    pub pages: usize,
}

impl Previews {
    /// What is known of `path` at `stamp`: `Some(None)` when it cannot be previewed.
    pub fn rows(&self, path: &Path, stamp: Stamp) -> Option<Option<Arc<PreviewRows>>> {
        self.rows
            .get(path)
            .filter(|(at, _)| *at == stamp)
            .map(|(_, rows)| rows.clone())
    }

    /// Whether `path` is being read now.
    pub fn reading(&self, path: &Path) -> bool {
        self.inflight.as_deref() == Some(path)
    }

    /// Keep what a worker read.
    /// `stamp` is the row's, which the rows are kept under; `read_at` the file's when
    /// it was read, which the dataset is installed under.
    pub(crate) fn landed(
        &mut self,
        path: PathBuf,
        stamp: Stamp,
        read_at: Stamp,
        rows: Option<Arc<PreviewRows>>,
        prepared: Option<Box<Prepared>>,
    ) {
        if self.inflight.as_ref() == Some(&path) {
            self.inflight = None;
        }
        if let Some(prepared) = prepared {
            self.prepared = Some((path.clone(), read_at, prepared));
        }
        self.order.retain(|p| p != &path);
        self.order.push_back(path.clone());
        self.rows.insert(path, (stamp, rows));
        while self.order.len() > KEPT {
            if let Some(old) = self.order.pop_front() {
                self.rows.remove(&old);
            }
        }
    }

    /// The dataset prepared for `path`, if the file is still what was read.
    pub(crate) fn take_prepared(&mut self, path: &Path) -> Option<Box<Prepared>> {
        if self.prepared.as_ref()?.0 != path {
            return None;
        }
        let now = Stamp::of_file(path);
        let (_, stamp, prepared) = self.prepared.take()?;
        (now == Some(stamp)).then_some(prepared)
    }

    /// Let go of the dataset waiting to be opened: the app is leaving home.
    pub(crate) fn drop_prepared(&mut self) {
        self.prepared = None;
    }
}

/// Whether a row's first rows are worth reading before it is opened: a local file of a
/// format whose first page is cheap, under `max_bytes` (Parquet: its average row
/// group). Nothing on a network share or in a store is read before it is opened.
pub fn previewable(entry: &Entry, max_bytes: u64) -> bool {
    if max_bytes == 0
        || entry.kind != EntryKind::File
        || entry.opens_whole_directory
        || entry.table.is_some()
        || entry.format_spec.is_some()
        || crate::home::is_remote_path(&entry.path)
    {
        return false;
    }
    let Some(size) = entry.size else {
        return false;
    };
    let preview =
        FileFormat::from_path(&entry.path).and_then(|f| crate::formats::readers::of(f).preview);
    match preview {
        // The first page is the first row group, whatever the file.
        Some(crate::formats::readers::Preview::RowGroup) => {
            let groups = entry.cost.row_groups.unwrap_or(1).max(1) as u64;
            size / groups <= max_bytes
        }
        Some(crate::formats::readers::Preview::Scan) => size <= max_bytes,
        None => false,
    }
}

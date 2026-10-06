//! Text formats read into a table of their own: VCD value change dumps, FIX logs and
//! SDF compound files, beside the GPS logs of [`crate::gps`].
//!
//! None can be scanned in place, so an open reads the file once, start to end, a piece
//! at a time, and writes its rows to temporary Arrow IPC segments (see
//! [`crate::segments`]) that the dataset scans lazily. Memory stays at one batch
//! however long the file. What a file says besides its rows (a VCD header, the FIX
//! dictionaries used, an SDF file's fields) is a [`Detail`] for the Info panel.

use std::io::Read;
use std::sync::Arc;

use color_eyre::Result;
use color_eyre::eyre::eyre;

use crate::model_files::MetaValue;
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::readers::{ConvertIn, ConvertOut};
use crate::segments::Converted;

/// How much of the file is read at a time.
const CHUNK: usize = 1 << 16;
/// The most rows of a [`Detail`] list; a file of more says how many are left out.
pub const MAX_DETAIL_ROWS: usize = 10_000;

/// What a file says besides its rows, for its tab of the Info panel: a VCD header, the
/// FIX dictionaries used, a model's totals and metadata, an audio file's format and
/// markers. Every format's tab is one of these, made by its reader when it opens.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Detail {
    /// The tab's name: `VCD`, `Model`, `Audio`, as [`tab`] gives it.
    pub tab: &'static str,
    /// The lines above the list.
    pub lines: Vec<String>,
    /// Lines after them in the warning color: what the file holds that is not shown.
    pub warnings: Vec<String>,
    /// The list's title: `Signals`, `Metadata`, `Tracks`.
    pub list_title: &'static str,
    /// The list, key and value.
    pub list: Vec<(String, MetaValue)>,
    /// Whether `i` opens on this tab rather than the schema: a table whose columns are
    /// the same for every file, where what is particular to it is here.
    pub first: bool,
    /// Whether the columns are datui's own rather than the file's, as the Schema tab
    /// says of where they came from.
    pub own_columns: bool,
    /// The keys of `list` that are tables of the file, which `T` at the table and
    /// Enter on the tab open: a workbook's worksheets, a database's tables.
    pub tables: Vec<String>,
    /// The one of `tables` on screen.
    pub table: Option<String>,
}

/// The name of `format`'s tab of the Info panel, as its descriptor says it: a reader
/// names its [`Detail`] by this, so the descriptor is the one place that says it.
pub const fn tab(format: crate::FileFormat) -> &'static str {
    match format.descriptor().summary {
        crate::Summary::Tab(tab) => tab,
        // A format said to have no tab has none to name; its title stands in.
        crate::Summary::None(_) => format.descriptor().title,
    }
}

/// Read the one file of `input` a piece at a time with `read`, a text format's reader,
/// through its compression: the conversion of a VCD dump, FIX log or SDF file.
pub(crate) fn read_one(
    input: &ConvertIn<'_>,
    read: impl FnOnce(&mut Pieces<'_>) -> Result<(Converted, Detail)>,
) -> ConvertOut {
    let [file] = input.files else {
        return Err(eyre!("Open {} files one at a time.", input.format.name()));
    };
    let writer = input.writer;
    let mut reader = crate::gps::open_reader(file, input.options, input.read)?;
    let mut pieces = |each: &mut dyn FnMut(&[u8]) -> Result<()>| -> Result<()> {
        let mut chunk = vec![0u8; CHUNK];
        loop {
            if writer.stopped() {
                return Err(eyre!("Reading was stopped."));
            }
            let n = match reader.read(&mut chunk) {
                Ok(0) => return Ok(()),
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            each(&chunk[..n])?;
        }
    };
    let (converted, detail) = read(&mut pieces)?;
    Ok((converted, Some(Arc::new(detail))))
}

/// Feeds a reader the file a piece at a time; returns when the file ends.
pub(crate) type Pieces<'a> = dyn FnMut(&mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> + 'a;

pub(crate) fn note(summary: String, scope: String) -> Note {
    Note {
        summary,
        scope,
        read_as_text: None,
        passed_over: None,
    }
}

/// `n` and its noun, singular or plural: `1 line`, `2 lines`.
pub(crate) fn count(n: u64, one: &str, many: &str) -> String {
    let n = usize::try_from(n).unwrap_or(usize::MAX);
    format!("{} {}", group_chrome(n), if n == 1 { one } else { many })
}

/// A list for a [`Detail`], at most [`MAX_DETAIL_ROWS`] long; one row more says how
/// many were left out.
pub(crate) fn capped_list(
    rows: impl Iterator<Item = (String, MetaValue)>,
    total: usize,
) -> Vec<(String, MetaValue)> {
    let mut list: Vec<_> = rows.take(MAX_DETAIL_ROWS).collect();
    if total > list.len() {
        let more = total - list.len();
        list.push((
            crate::glyphs::get().ellipsis.to_string(),
            MetaValue::Text(format!("{} more", group_chrome(more))),
        ));
    }
    list
}

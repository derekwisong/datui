//! Text formats read into a table of their own: VCD value change dumps, FIX logs, SDF
//! compound files and the GPS logs of [`crate::formats::gps`]; and [`Detail`], every format's
//! tab of the Info panel.
//!
//! None of those formats can be scanned in place, so an open reads the file once,
//! start to end, a piece at a time through its [`BatchReader`], and writes its rows to
//! temporary Arrow IPC segments (see `crate::formats::segments`) that the dataset scans
//! lazily. Memory stays at one batch however long the file.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use polars::prelude::{DataFrame, LazyFrame, PolarsResult};

use crate::download::TempDownload;
use crate::formats::model_files::MetaValue;
use crate::formats::readers::{ConvertIn, ConvertOut};
use crate::formats::segments::{Converted, Segments};
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::unfinished::Writer;
use crate::{CompressionFormat, OpenOptions};

/// How much of the file is read at a time.
const CHUNK: usize = 1 << 16;

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

/// A text format's reader: it takes the file a piece at a time and hands back its rows
/// a batch at a time, keeping no more of the file than the batch.
pub trait BatchReader {
    fn push(&mut self, piece: &[u8]) -> Result<()>;
    /// The rows ready, once a batch's worth is.
    fn take_batch(&mut self) -> PolarsResult<Option<DataFrame>>;
    /// The rows left at the end of the file.
    fn finish(&mut self) -> Result<DataFrame>;
}

/// A reader that counts the bytes read through it, for the loading screen.
struct Counted<'a, R> {
    inner: R,
    read: &'a AtomicU64,
}

impl<R: Read> Read for Counted<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// The file, decompressed as it is read when its name or `--compression` says so;
/// `read` counts the bytes of the file as stored.
pub(crate) fn open_reader<'a>(
    file: &Path,
    options: &OpenOptions,
    read: &'a AtomicU64,
) -> Result<Box<dyn Read + 'a>> {
    let f = BufReader::new(Counted {
        inner: File::open(file)?,
        read,
    });
    Ok(
        match options
            .compression
            .or_else(|| CompressionFormat::from_extension(file))
        {
            None => Box::new(f),
            Some(CompressionFormat::Gzip) => Box::new(flate2::read::MultiGzDecoder::new(f)),
            Some(CompressionFormat::Zstd) => Box::new(zstd::Decoder::new(f)?),
            Some(CompressionFormat::Bzip2) => Box::new(bzip2::read::BzDecoder::new(f)),
            Some(CompressionFormat::Xz) => Box::new(xz2::read::XzDecoder::new(f)),
        },
    )
}

/// Read `file` through `reader` a piece at a time, through its compression, into
/// temporary segments written through `writer`, counting the bytes read in `read`;
/// `each` sees every batch first. Gives the frame over the segments, and their files.
pub(crate) fn read_through<R: BatchReader>(
    file: &Path,
    options: &OpenOptions,
    writer: &Writer,
    read: &AtomicU64,
    reader: &mut R,
    mut each: impl FnMut(&DataFrame),
) -> Result<(LazyFrame, Vec<TempDownload>)> {
    let mut source = open_reader(file, options, read)?;
    let mut segments = Segments::new(options, writer);
    let mut chunk = vec![0u8; CHUNK];
    loop {
        if writer.stopped() {
            return Err(eyre!("Reading was stopped."));
        }
        let n = match source.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        };
        reader.push(&chunk[..n])?;
        if let Some(df) = reader.take_batch()? {
            each(&df);
            segments.write(&df)?;
        }
    }
    let last = reader.finish()?;
    each(&last);
    segments.write(&last)?;
    segments.finish()
}

/// The conversion of a format read one file at a time: the file read through
/// `reader`, then `finish` gives the frame, the notes and the Info panel's tab from the
/// frame over its segments and what the reader saw.
pub(crate) fn convert_with<R: BatchReader>(
    input: &ConvertIn<'_>,
    mut reader: R,
    finish: impl FnOnce(&R, LazyFrame) -> Result<(LazyFrame, Vec<Note>, Detail)>,
) -> ConvertOut {
    let [file] = input.files else {
        return Err(eyre!("Open {} files one at a time.", input.format.name()));
    };
    let (lf, files) = read_through(
        file,
        input.options,
        input.writer,
        input.read,
        &mut reader,
        |_| {},
    )?;
    let (lf, notes, detail) = finish(&reader, lf)?;
    let converted = Converted {
        lf,
        files,
        notes,
        other_tables: Vec::new(),
    };
    Ok((converted, Some(Arc::new(detail))))
}

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

/// A list for a [`Detail`], at most `limits.detail_rows` long; one row more says how
/// many were left out.
pub(crate) fn capped_list(
    rows: impl Iterator<Item = (String, MetaValue)>,
    total: usize,
) -> Vec<(String, MetaValue)> {
    let mut list: Vec<_> = rows.take(crate::limits::get().detail_rows).collect();
    if total > list.len() {
        let more = total - list.len();
        list.push((
            crate::glyphs::get().ellipsis.to_string(),
            MetaValue::Text(format!(
                "{} more {} limits.detail_rows raises it",
                group_chrome(more),
                crate::glyphs::get().middot
            )),
        ));
    }
    list
}

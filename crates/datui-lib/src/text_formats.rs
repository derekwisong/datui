//! Text formats read into a table of their own: VCD value change dumps, FIX logs and
//! SDF compound files, beside the GPS logs of [`crate::gps`].
//!
//! None can be scanned in place, so an open reads the file once, start to end, a piece
//! at a time, and writes its rows to temporary Arrow IPC segments (see
//! [`crate::segments`]) that the dataset scans lazily. Memory stays at one batch
//! however long the file. What a file says besides its rows (a VCD header, the FIX
//! dictionaries used, an SDF file's fields) is a [`Detail`] for the Info panel.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use color_eyre::Result;
use color_eyre::eyre::eyre;

use crate::model_files::MetaValue;
use crate::notes::Note;
use crate::numfmt::group_chrome;
use crate::segments::Converted;
use crate::unfinished::Writer;
use crate::{CompressionFormat, FileFormat, OpenOptions};

/// How much of the file is read at a time.
const CHUNK: usize = 1 << 16;
/// Bytes looked at to tell a file by its content.
pub const HEAD: usize = 4096;
/// The most rows of a [`Detail`] list; a file of more says how many are left out.
pub const MAX_DETAIL_ROWS: usize = 10_000;

/// What a text format's file says besides its rows, for its tab of the Info panel.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Detail {
    /// The tab's name: `VCD`, `FIX`, `SDF`.
    pub tab: &'static str,
    /// The lines above the list.
    pub lines: Vec<String>,
    /// The list's title: `Signals`, `Tags`, `Fields`.
    pub list_title: &'static str,
    /// The list, key and value.
    pub list: Vec<(String, MetaValue)>,
    /// Whether `i` opens on this tab rather than the schema: a table whose columns are
    /// the same for every file, where what is particular to it is here.
    pub first: bool,
}

/// Whether `format` is one of the text formats read here.
pub fn is_text_format(format: FileFormat) -> bool {
    matches!(format, FileFormat::Vcd | FileFormat::Fix | FileFormat::Sdf)
}

/// Whether `format` is read into a table of its own before it is scanned.
pub fn reads_into(format: FileFormat) -> bool {
    is_text_format(format) || crate::gps::is_gps(format)
}

/// The text format a name says, looking through a compression suffix:
/// `library.sdf.gz` is SDF.
pub fn format_by_name(path: &Path) -> Option<FileFormat> {
    FileFormat::from_path(path)
        .or_else(|| {
            CompressionFormat::from_extension(path)?;
            FileFormat::from_path(Path::new(path.file_stem()?))
        })
        .filter(|f| is_text_format(*f))
}

/// The text format the first bytes of a file say, if they say one.
pub fn sniff(head: &[u8]) -> Option<FileFormat> {
    if crate::fix::looks_like(head) {
        Some(FileFormat::Fix)
    } else if crate::vcd::looks_like(head) {
        Some(FileFormat::Vcd)
    } else if crate::sdf::looks_like(head) {
        Some(FileFormat::Sdf)
    } else {
        None
    }
}

/// The text format of the file at `path` by its first bytes, decompressed when its
/// name or `compression` says it is compressed. Only for a file whose name says no
/// format, so a file that opens as something today opens the same way.
pub fn sniff_path(path: &Path, compression: Option<CompressionFormat>) -> Option<FileFormat> {
    let compression = compression.or_else(|| CompressionFormat::from_extension(path));
    let head = match compression {
        Some(_) => crate::formats::head_of(path, compression, HEAD as u64)?,
        None => {
            let mut head = Vec::with_capacity(HEAD);
            File::open(path)
                .ok()?
                .take(HEAD as u64)
                .read_to_end(&mut head)
                .ok()?;
            head
        }
    };
    sniff(&head)
}

/// Read `files` (named `display` to the user) as `format` into temporary IPC files,
/// written through `writer`, counting the bytes read in `read`. A FIX log is read with
/// the dictionaries in `formats` and `--fix-dict`. GPS logs go to
/// [`crate::gps::convert`], several as one table, and have no [`Detail`]; the others
/// are one file.
pub(crate) fn convert(
    files: &[PathBuf],
    display: &Path,
    format: FileFormat,
    options: &OpenOptions,
    formats: &crate::formats::Registry,
    writer: &Writer,
    read: &AtomicU64,
) -> Result<(Converted, Option<Arc<Detail>>)> {
    if crate::gps::is_gps(format) {
        return Ok((
            crate::gps::convert(files, display, format, options, writer, read)?,
            None,
        ));
    }
    // A NumPy archive's compressed array, named by `--table` or found alone.
    if format == FileFormat::Numpy {
        let ([file], Some(name)) = (files, options.table.as_deref()) else {
            return Err(eyre!("Open one array of an archive at a time."));
        };
        let (held, lf, opened) = crate::numpy::convert(file, name, options, writer, read)?;
        return Ok((
            Converted {
                lf,
                files: vec![held],
                notes: opened.notes,
                other_tables: opened.other_tables,
            },
            opened.detail,
        ));
    }
    let [file] = files else {
        return Err(eyre!("Open {} files one at a time.", format.name()));
    };
    // Read before the file, so a dictionary that does not parse says so at once.
    let layers = match format {
        FileFormat::Fix => Some(crate::fix::layers(formats, options.fix_dict.as_deref())?),
        _ => None,
    };
    let mut reader = crate::gps::open_reader(file, options, read)?;
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
    let (converted, detail) = match format {
        FileFormat::Vcd => crate::vcd::convert(display, options, writer, &mut pieces)?,
        FileFormat::Fix => crate::fix::convert(
            display,
            options,
            layers.unwrap_or_default(),
            writer,
            &mut pieces,
        )?,
        FileFormat::Sdf => crate::sdf::convert(display, options, writer, &mut pieces)?,
        other => return Err(eyre!("{} is not a text format.", other.name())),
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_through_compression() {
        assert_eq!(
            format_by_name(Path::new("lib.sdf.gz")),
            Some(FileFormat::Sdf)
        );
        assert_eq!(format_by_name(Path::new("dump.vcd")), Some(FileFormat::Vcd));
        assert_eq!(format_by_name(Path::new("dump.csv")), None);
    }

    #[test]
    fn each_format_by_its_first_bytes() {
        assert_eq!(
            sniff(b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01\n"),
            Some(FileFormat::Fix)
        );
        assert_eq!(
            sniff(b"$timescale 1ns $end\n$scope module top $end\n"),
            Some(FileFormat::Vcd)
        );
        assert_eq!(
            sniff(b"aspirin\n  RDKit\n\n  0  0  0  0  0  0  0  0  0  0999 V2000\nM  END\n$$$$\n"),
            Some(FileFormat::Sdf)
        );
        assert_eq!(sniff(b"a,b\n1,2\n"), None);
    }

    #[test]
    fn a_compressed_file_by_what_it_holds() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.log.gz");
        let mut gz =
            flate2::write::GzEncoder::new(File::create(&path).unwrap(), Default::default());
        gz.write_all(b"20260101-00:00:00 : 8=FIX.4.2|9=5|35=0|10=000|\n")
            .unwrap();
        gz.finish().unwrap();
        assert_eq!(sniff_path(&path, None), Some(FileFormat::Fix));
    }
}

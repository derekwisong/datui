//! What a scan built for an open: a frame, or what the load turns into a file first.

use std::path::PathBuf;

use polars::prelude::LazyFrame;

use crate::{FileFormat, OpenOptions, cli};

/// What a scan built: the frame, or what the load has to turn into a file it can scan
/// first, since the copy has to outlive the scan.
pub(crate) enum Scan {
    Frame(Box<LazyFrame>),
    Decompress {
        file: PathBuf,
        format: FileFormat,
    },
    /// Arrow IPC streams, which the load converts to one IPC file before it scans.
    Streams(Vec<PathBuf>),
    /// A compressed file a format spec matched, to decompress and then read with it.
    DecompressSpec {
        file: PathBuf,
        choice: crate::formats::Choice,
    },
    /// GPS logs, or a VCD dump, FIX log or SDF file, read into files of their own
    /// before the frame over them is built (`Step::Convert`).
    ReadInto {
        files: Vec<PathBuf>,
        format: FileFormat,
    },
    /// A file of several tables (a SQLite database, a NumPy archive), named, and no
    /// `--table`: the home screen lists them.
    Tables {
        file: PathBuf,
        tables: Vec<String>,
        format: FileFormat,
    },
    /// A member compressed in an archive (a NumPy array), decompressed to a file before
    /// it is read (`Step::Convert`).
    Unpack {
        file: PathBuf,
        member: String,
        format: FileFormat,
    },
    /// A local file to show as bytes: no reader and no spec takes it, or `--hex` asked.
    Hex {
        file: PathBuf,
        asked: bool,
    },
}

impl Scan {
    /// The reader the scan chose: `found` (what the read reported, else what was
    /// asked for) for a frame, else the format of what is to be converted.
    pub(crate) fn format(&self, found: Option<FileFormat>) -> Option<FileFormat> {
        match self {
            Scan::Frame(_) => found,
            Scan::Decompress { format, .. } | Scan::ReadInto { format, .. } => Some(*format),
            Scan::Streams(_) => Some(FileFormat::Arrow),
            Scan::DecompressSpec { .. } => None,
            Scan::Tables { format, .. } => Some(*format),
            Scan::Unpack { format, .. } => Some(*format),
            Scan::Hex { .. } => None,
        }
    }

    /// How the open reads what this scan found, as [`FileFormat::read_mode`] says for
    /// its format and how it is stored. `format` is the reader the scan chose; a frame
    /// with none is a Parquet scan of a directory, a glob or a bucket.
    /// A frame of converted Arrow streams (`options.arrow_parts`) is the IPC file they
    /// were converted to, scanned again.
    pub(crate) fn read_mode(
        &self,
        format: Option<FileFormat>,
        spec: bool,
        options: &OpenOptions,
    ) -> Option<crate::ReadMode> {
        use crate::{ReadMode, Stored};
        let spec_read = |stored| cli::FormatChoice::Spec(String::new()).read_mode(stored);
        let converted = options.arrow_parts.as_ref().is_some_and(|parts| {
            parts
                .iter()
                .any(|part| matches!(part, crate::ipc_stream::Part::Converted { .. }))
        });
        match self {
            Scan::Frame(_) if spec => spec_read(Stored::Plain),
            Scan::Frame(_) if converted => FileFormat::Arrow.read_mode(Stored::Stream),
            Scan::Frame(_) => format.map_or(Some(ReadMode::Lazy), |f| f.read_mode(Stored::Plain)),
            Scan::Decompress { format, .. } => format.read_mode(Stored::Compressed {
                in_memory: options.decompress_in_memory,
            }),
            Scan::Streams(_) => FileFormat::Arrow.read_mode(Stored::Stream),
            Scan::ReadInto { format, .. } => format.read_mode(Stored::Plain),
            Scan::DecompressSpec { .. } => spec_read(Stored::Compressed { in_memory: false }),
            Scan::Tables { format, .. } => format.read_mode(Stored::Plain),
            Scan::Unpack { .. } => Some(crate::ReadMode::Decompressed),
            Scan::Hex { .. } => None,
        }
    }
}

impl From<LazyFrame> for Scan {
    fn from(lf: LazyFrame) -> Self {
        Scan::Frame(Box::new(lf))
    }
}

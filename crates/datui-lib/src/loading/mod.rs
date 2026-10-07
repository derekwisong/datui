//! Opening a dataset, from the request to its first rows, and its one owner.
//!
//! [`Loader`] holds the open in flight ([`Load`]): its origin, paths, phase, loading
//! screen text, and what it holds (stop flag, footer counter, download, converted IPC
//! file, the generation hold while a download is asked about). The app reports events
//! (a request, a worker's answer or failure, the user's download answer) and carries
//! out the [`Step`] it returns. Nothing else keeps the open's state.
//!
//! - **Identity.** Each load has a [`LoadId`], carried by its phases' jobs. An answer
//!   is taken only for the load in flight, in the phase that asked; others are dropped
//!   with their payload.
//! - **Retirement.** Abandoning, replacing or failing a load raises its stop flag (a
//!   download or conversion stops at its next chunk and removes its file; a footer
//!   pass stops reading), releases its files and its hold.
//! - **Handover.** The dataset is built holding the load's download or converted file
//!   and everything found ([`crate::table::OpenFacts`]), and takes the footer counter on
//!   install; what remains is the first-rows read ([`Phase::FirstRows`]), whose
//!   abandonment stops neither.
//!
//! Home's looks, analyses and charts are not loads.

pub(crate) mod counting;
pub(crate) mod first_rows_trace;
pub mod follow;
pub(crate) mod local_glob;
pub mod measurements;
pub(crate) mod open_options;
pub(crate) mod open_scan;
pub(crate) mod scan;
pub mod stdin;
pub mod tee;
pub(crate) mod unfinished;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use polars::prelude::LazyFrame;

use crate::cloud::download::TempDownload;
use crate::formats::schema_union::FooterProgress;
use crate::loading::unfinished::{Unfinished, Writer};
use crate::table::DataTableState;
use crate::{CompressionFormat, FileFormat, OpenOptions, cloud::source};

use crate::jobs::Hold;
#[cfg(any(feature = "http", feature = "cloud"))]
use crate::jobs::Jobs;

/// Names one open, from the moment it is asked for until it is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LoadId(u64);

impl LoadId {
    /// A load's name for a job a test starts by hand.
    #[cfg(test)]
    pub(crate) fn for_tests(n: u64) -> Self {
        Self(n)
    }
}

/// A remote file to download once the user agrees; its size is what the probe found.
#[cfg(any(feature = "http", feature = "cloud"))]
#[derive(Clone)]
pub(crate) enum PendingDownload {
    #[cfg(feature = "http")]
    Http {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    S3 {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    Gcs {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    Azure {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    /// Arrow in S3, GCS or Azure, one object or a prefix: `objects` are what the probe
    /// chose, `size` its streams' bytes (downloaded and converted); IPC files are scanned
    /// in place (`cloud_arrow`).
    #[cfg(feature = "cloud")]
    Arrow {
        url: String,
        objects: Vec<crate::cloud::cloud_arrow::Object>,
        size: Option<u64>,
        options: OpenOptions,
    },
}

#[cfg(any(feature = "http", feature = "cloud"))]
impl PendingDownload {
    /// The url, the size the probe found, and the open options — the same three
    /// fields whichever store this came from.
    pub(crate) fn parts(&self) -> (&str, Option<u64>, &OpenOptions) {
        match self {
            #[cfg(feature = "http")]
            PendingDownload::Http { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::S3 { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::Gcs { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::Azure { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::Arrow {
                url, size, options, ..
            } => (url, *size, options),
        }
    }

    /// How many Arrow streams the download is, and how many IPC files are read in
    /// place beside them, for the question about it.
    pub(crate) fn arrow_files(&self) -> Option<(usize, usize)> {
        match self {
            #[cfg(feature = "cloud")]
            PendingDownload::Arrow { objects, .. } => {
                let streams = objects.iter().filter(|o| o.stream).count();
                Some((streams, objects.len() - streams))
            }
            _ => None,
        }
    }

    /// The download once the user has agreed to it: it never asks again, so it has
    /// no limit to stop at.
    pub(crate) fn asked(mut self) -> Self {
        match &mut self {
            #[cfg(feature = "http")]
            PendingDownload::Http { options, .. } => options.download_unasked = None,
            #[cfg(feature = "cloud")]
            PendingDownload::S3 { options, .. }
            | PendingDownload::Gcs { options, .. }
            | PendingDownload::Azure { options, .. }
            | PendingDownload::Arrow { options, .. } => options.download_unasked = None,
        }
        self
    }

    /// Replace the placeholder size with what the probe actually found.
    pub(crate) fn with_size(mut self, found: Option<u64>) -> Self {
        match &mut self {
            #[cfg(feature = "http")]
            PendingDownload::Http { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::S3 { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::Gcs { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::Azure { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::Arrow { size, .. } => *size = found,
        }
        self
    }
}

/// Paths asked to be opened, and what the screen and the recents need of them. Built
/// once, when the open is asked for, and not changed after.
#[derive(Clone)]
pub(crate) struct OpenRequest {
    pub(crate) paths: Vec<PathBuf>,
    pub(crate) options: OpenOptions,
    /// The first path's size on disk, for the loading screen; 0 for a URL.
    pub(crate) size: u64,
    /// The path recorded as a recent if the dataset installs: the first, as named,
    /// unless it is a local path that is not there.
    pub(crate) recent: Option<PathBuf>,
    /// What the loading screen names in place of the first path: a table inside a
    /// database.
    pub(crate) shown: Option<PathBuf>,
    /// Ask before reading more than this many bytes whole into memory
    /// (`[read] memory_warning`); `None` never asks.
    pub(crate) warn_in_memory_above: Option<u64>,
}

impl OpenRequest {
    /// The request for `paths`, asking the filesystem for the first one's size and
    /// existence. Every open records a recent (command-line ones especially; URLs
    /// verbatim), under the name given, not a download's temp copy. A table inside a file
    /// (`app.db/users`) opens as the file with `--table` and is recorded as the table.
    pub(crate) fn named(
        mut paths: Vec<PathBuf>,
        mut options: OpenOptions,
        formats: &crate::formats::Registry,
    ) -> Self {
        let first = paths[0].clone();
        let piped = stdin::is_stdin(&first);
        let local = !piped && matches!(source::input_source(&first), source::InputSource::Local(_));
        let mut table = None;
        if local
            && paths.len() == 1
            && let Some((db, name)) = crate::formats::members::split(&first)
        {
            table = Some(first.clone());
            options.table = Some(name);
            paths = vec![db];
        } else if local
            && first.is_file()
            && let Some(name) = options.table.as_deref()
            && crate::formats::members::holder(&first).is_some()
        {
            table = Some(crate::formats::members::place(&first, name));
        } else if local
            && paths.len() == 1
            && options.table.is_none()
            && let Some((dir, split)) = crate::formats::hf_splits::split_place(&first)
        {
            // A split of a Hugging Face cache, as the home screen lists it.
            table = Some(first.clone());
            options.table = Some(split);
            paths = vec![dir];
        } else if local
            && first.is_dir()
            && let Some(split) = options.table.as_deref()
            && crate::formats::hf_splits::cache_splits(&first)
                .iter()
                .any(|s| s == split)
        {
            table = Some(first.join(split));
        } else if local
            && paths.len() == 1
            && options.table.is_none()
            && let Some((file, variant)) = crate::formats::members::split_variant(&first, formats)
        {
            // A variant of a file a spec reads as several, as the home screen lists it.
            table = Some(first.clone());
            options.table = Some(variant);
            paths = vec![file];
        } else if local
            && let Some(variant) = options.table.as_deref()
            && first.is_file()
            && formats.variants_of(&first).is_some()
        {
            table = Some(crate::formats::members::place(&first, variant));
        }
        let first = &paths[0];
        let size = if local {
            std::fs::metadata(first).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };
        // Standard input cannot be opened again from a list.
        let recent = table
            .clone()
            .or_else(|| (!piped && (!local || first.exists())).then(|| first.clone()));
        Self {
            paths,
            options,
            size,
            recent,
            shown: table,
            warn_in_memory_above: None,
        }
    }
}

/// Where an open stands. Each phase but the first rows is waiting on one worker, or on
/// the user.
pub(crate) enum Phase {
    /// Asked for, before its first step: the frame between a key and the open it asks
    /// for, or a startup open before its paths are looked at. Says what its caller says.
    Starting {
        label: String,
        percent: u16,
    },
    /// Whether the paths named on the command line are there, and which is a directory.
    LookingAtPaths,
    /// The spec a `--format` URL names, fetched before anything is read with it.
    ReadingSpec,
    /// What a directory named on the command line holds, before it is opened.
    LookingAtDirectory,
    /// A remote model's headers, read by range rather than downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    ReadingHeaders,
    /// The size of a remote file, to put the download to the user; `note` says why it
    /// is downloaded when it would not have been.
    #[cfg(any(feature = "http", feature = "cloud"))]
    CheckingSize {
        note: Option<&'static str>,
    },
    /// Waiting on the user to agree to read files whole into memory, past the size
    /// that asks first. Holds the generation meanwhile, once the app gives it a hold.
    ConfirmingRead {
        scan: Box<Scan>,
        _hold: Option<Hold>,
    },
    /// Waiting on the user to agree to the download. Holds the generation meanwhile:
    /// nothing is running, and the open is very much unfinished.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Confirming {
        pending: Box<PendingDownload>,
        note: Option<&'static str>,
        _hold: Hold,
    },
    #[cfg(any(feature = "http", feature = "cloud"))]
    Downloading,
    /// Standard input being read to a file; `read` counts its bytes.
    Spooling {
        read: Arc<AtomicU64>,
    },
    Decompressing,
    /// A compressed file a format spec reads, being decompressed to a copy.
    DecompressingRecords,
    /// The decompressed copy's records, read through the spec.
    ReadingRecords,
    /// Files being converted to ones the dataset scans, `read` of their `total` bytes:
    /// Arrow IPC streams to one IPC file, or GPS logs to their table.
    Converting {
        what: Conversion,
        read: Arc<AtomicU64>,
        total: u64,
    },
    /// A CSV read with its string columns parsed.
    ScanningStrings,
    /// A CSV whose footer rows are dropped (`--footer-rows`): the scan counts every
    /// row of the file first, which is the wait.
    CountingFooter,
    /// The scan; `downloaded` when it reads a download rather than what was named.
    Scanning {
        downloaded: bool,
    },
    /// The scan of text read as lines, which indexes them.
    ReadingLines,
    /// The schema, and whatever the dataset needs before its first rows.
    ReadingSchema,
    /// Installed: its first rows are being read. The dataset is the one on screen, and
    /// what the load held is its now.
    FirstRows,
}

impl Phase {
    /// What the loading screen and the footer call this phase, and the flat
    /// percentage the bar shows beside it (0 for none).
    pub(crate) fn label(&self) -> (&str, u16) {
        match self {
            Phase::Starting { label, percent } => (label, *percent),
            Phase::LookingAtPaths => ("Scanning input", 10),
            Phase::ReadingSpec => ("Reading spec", 5),
            Phase::LookingAtDirectory => (crate::App::LOOKING_AT_A_DIRECTORY, 5),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::ReadingHeaders => ("Reading headers", 20),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::CheckingSize { .. } | Phase::Confirming { .. } => ("Checking size", 0),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::Downloading => ("Downloading", 20),
            Phase::Spooling { .. } => ("Reading stdin", 5),
            Phase::ConfirmingRead { .. } => ("Scanning input", 0),
            Phase::Decompressing | Phase::DecompressingRecords => ("Decompressing", 30),
            Phase::ReadingRecords => ("Reading records", 35),
            Phase::Converting { what, read, total } => {
                let done = read.load(Ordering::Relaxed).min(*total);
                // Up to the scan or the schema read that follows.
                let share = (done * 20).checked_div(*total).unwrap_or(0);
                (what.label(), 10 + share as u16)
            }
            Phase::ScanningStrings => ("Scanning string columns", 55),
            Phase::CountingFooter => (COUNTING_FOOTER, 10),
            Phase::Scanning { downloaded: false } => ("Scanning input", 10),
            Phase::Scanning { downloaded: true } => ("Scanning", 30),
            Phase::ReadingLines => (READING_LINES, 10),
            Phase::ReadingSchema => ("Reading schema", 40),
            Phase::FirstRows => ("Loading buffer", 70),
        }
    }

    /// Whether the open waits on the user's answer to a question.
    fn asks(&self) -> bool {
        match self {
            Phase::ConfirmingRead { .. } => true,
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::Confirming { .. } => true,
            _ => false,
        }
    }

    /// Whether this phase's worker answers with the dataset itself.
    fn builds_the_dataset(&self) -> bool {
        match self {
            Phase::ReadingSchema | Phase::Decompressing => true,
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::ReadingHeaders => true,
            _ => false,
        }
    }

    /// Whether the open has not started any work of its own yet, so an open asked for
    /// now carries it on rather than replacing it: the `Open` a key or a look returns.
    fn starting(&self) -> bool {
        matches!(
            self,
            Phase::Starting { .. } | Phase::LookingAtPaths | Phase::LookingAtDirectory
        )
    }
}

/// A download a load fetched, and the URL it was fetched from: standard input's `-`
/// for what was piped in.
#[derive(Clone)]
struct Fetched {
    url: PathBuf,
    file: TempDownload,
    /// What the listing of a store's Arrow chose, read again with the file: each
    /// input's place, the split and the `--table` that chose it.
    arrow: Option<KeptArrow>,
}

#[derive(Clone)]
struct KeptArrow {
    parts: Arc<Vec<crate::formats::ipc_stream::Part>>,
    splits: Option<Arc<crate::formats::hf_splits::Splits>>,
    table: Option<String>,
}

impl Fetched {
    /// Whether an open with `options` reads what this download holds: not when it
    /// asks for another split.
    fn serves(&self, options: &OpenOptions) -> bool {
        self.arrow
            .as_ref()
            .is_none_or(|arrow| arrow.table == options.table)
    }
}

/// The open in flight.
pub(crate) struct Load {
    id: LoadId,
    /// Chosen on the home screen, which is where its failure is reported: the dataset
    /// left over from before is not what the user was looking at when they chose.
    from_home: bool,
    phase: Phase,
    /// What the screen names: the path as asked for (a URL, not the temporary file it
    /// landed in), and its size on disk.
    path: Option<PathBuf>,
    size: u64,
    /// The paths asked for, which `H` opens again; `None` for a frame handed over.
    paths: Option<Vec<PathBuf>>,
    recent: Option<PathBuf>,
    /// This load's footer counter, and through it its stop flag.
    progress: Arc<FooterProgress>,
    /// The stop flag again, for the workers that write files, and where they claim them.
    writer: Writer,
    download: Option<Fetched>,
    /// The IPC files its Arrow streams or GPS logs were converted to, which the
    /// dataset scans.
    converted: Vec<TempDownload>,
    /// See [`OpenRequest::warn_in_memory_above`].
    warn_in_memory_above: Option<u64>,
}

/// A scan held while the user is asked about it.
pub(crate) struct Scan {
    paths: Vec<PathBuf>,
    options: OpenOptions,
    display: Option<PathBuf>,
}

/// What a read whole into memory would take, put to the user before it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InMemory {
    /// The files' bytes on disk.
    pub(crate) bytes: u64,
    pub(crate) format: FileFormat,
    pub(crate) files: usize,
    /// The first file, as the loading screen names it.
    pub(crate) name: PathBuf,
}

impl Load {
    pub(crate) fn phase(&self) -> &Phase {
        &self.phase
    }

    /// The files the frame about to have its schema read is made from: the download,
    /// and what a conversion wrote.
    fn made(&self) -> Made {
        Made {
            download: self.download.as_ref().map(|fetched| fetched.file.clone()),
            converted: self.converted.clone(),
            ..Made::default()
        }
    }

    /// The path the screen names, if it names one.
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The size the screen shows beside the path: while standard input is read, what
    /// has come in so far.
    pub(crate) fn size(&self) -> u64 {
        match &self.phase {
            Phase::Spooling { read } => read.load(Ordering::Relaxed),
            _ => self.size,
        }
    }
}

/// What the app does next for the open.
pub(crate) enum Step {
    /// Nothing: the answer was for an open nobody is waiting on, or the phase waits.
    Nothing,
    /// The open cannot be read at all, and the session ends saying why.
    Crash(String),
    /// Read the headers of the remote model at `url` as `format`, by range; `writer`
    /// carries the load's stop flag.
    #[cfg(any(feature = "http", feature = "cloud"))]
    ReadHeaders {
        url: PathBuf,
        format: FileFormat,
        options: OpenOptions,
        writer: Writer,
    },
    /// Find the remote file's size.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Probe(PendingDownload),
    /// Ask the user whether to download it.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Ask(PendingDownload),
    /// Ask the user whether to read files this large whole into memory.
    AskRead(InMemory),
    /// Download it, writing through `writer`: the load's stop flag, and its claim on
    /// the file for quitting to find.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Download {
        pending: PendingDownload,
        writer: Writer,
    },
    /// Fetch the spec at `url`, asking `writer`'s stop flag before the request.
    FetchSpec {
        url: PathBuf,
        options: OpenOptions,
        writer: Writer,
    },
    /// Read standard input to a file through `writer`, counting its bytes in `read`.
    Spool {
        options: OpenOptions,
        writer: Writer,
        read: Arc<AtomicU64>,
    },
    /// Decompress the CSV in `file`, writing through `writer`; `path` names it on screen
    /// and in errors.
    Decompress {
        file: PathBuf,
        path: PathBuf,
        options: OpenOptions,
        writer: Writer,
        /// The download `file` is, given to the dataset built from it.
        download: Option<TempDownload>,
    },
    /// Decompress `file`, which `choice`'s spec reads, to a copy written through
    /// `writer`; `path` names it on screen and in errors.
    DecompressRecords {
        file: PathBuf,
        path: PathBuf,
        choice: crate::formats::Choice,
        options: OpenOptions,
        writer: Writer,
    },
    /// Read the records of `copy`, the decompressed file `path` names, with
    /// `choice`'s spec.
    ReadRecords {
        copy: PathBuf,
        path: PathBuf,
        choice: crate::formats::Choice,
        options: OpenOptions,
    },
    /// Convert `files` as `what` says into temp IPC files via `writer`, counting bytes read
    /// in `read`; `path` names them on screen and in errors.
    Convert {
        what: Conversion,
        files: Vec<PathBuf>,
        path: Option<PathBuf>,
        options: OpenOptions,
        writer: Writer,
        read: Arc<AtomicU64>,
    },
    /// Scan `paths`, saying `status` on the footer; `display` names the dataset when
    /// what is scanned is a download.
    Scan {
        paths: Vec<PathBuf>,
        options: OpenOptions,
        display: Option<PathBuf>,
        status: &'static str,
    },
    /// Read the scan's schema, reporting footers to `progress`.
    ReadSchema {
        lf: Box<LazyFrame>,
        path: Option<PathBuf>,
        options: OpenOptions,
        progress: Arc<FooterProgress>,
        /// What the open made the frame from, given to the dataset built from it.
        made: Made,
    },
    /// Install the dataset, then read its first rows.
    Install(Box<Loaded>),
    /// The open failed. Its load is retired.
    Failed(Failed),
    /// The open found a SQLite database of several tables and was asked for none: the
    /// home screen lists them. Its load is retired.
    Tables(Tables),
    /// The open found a local file no reader and no spec takes, or was asked for its
    /// bytes: the hex view shows it. Its load is retired.
    Hex(Hex),
}

/// A local file to show in the hex view.
#[derive(Debug)]
pub(crate) struct Hex {
    pub(crate) file: PathBuf,
    pub(crate) from_home: bool,
    /// Asked for (`--hex`) rather than fallen back to.
    pub(crate) asked: bool,
    /// `--hex-width`: the bytes a row holds.
    pub(crate) record_size: Option<usize>,
}

/// A file of several tables, to be listed on the home screen.
#[derive(Debug)]
pub(crate) struct Tables {
    /// The database file, as the user named it.
    pub(crate) database: PathBuf,
    pub(crate) from_home: bool,
}

/// What a conversion turns into files the dataset scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Conversion {
    /// Arrow IPC streams, into one IPC file; the IPC files among them stay put.
    Streams,
    /// GPS logs of this format, each into IPC files of its own, read as one table; or
    /// a VCD dump, FIX log or SDF file into its own.
    Text(FileFormat),
}

impl Conversion {
    /// What the loading screen calls it.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Conversion::Streams => "Converting Arrow stream",
            Conversion::Text(format) => format.conversion().label,
        }
    }

    /// What the footer says while it runs.
    pub(crate) fn status(self) -> &'static str {
        match self {
            Conversion::Streams => "Converting Arrow stream...",
            Conversion::Text(format) => format.conversion().status,
        }
    }
}

/// What a conversion wrote. Dropped unused, it removes its files.
pub(crate) enum Converted {
    /// The streams in one IPC file, and where each input's rows are: scanned next.
    Streams {
        file: TempDownload,
        parts: Vec<crate::formats::ipc_stream::Part>,
    },
    /// The logs' IPC files and the frame over them, which only needs its schema read,
    /// and what reading them noticed.
    Frame {
        files: Vec<TempDownload>,
        lf: Box<LazyFrame>,
        notes: Vec<crate::notes::Note>,
        other_tables: Vec<String>,
        /// What the file says besides its rows, for the Info panel.
        detail: Option<Arc<crate::formats::text_formats::Detail>>,
    },
}

/// The files an open made or fetched for the frame it scans, and what making them
/// found: handed to the dataset as it is built, which holds the files from then on.
#[derive(Default)]
pub(crate) struct Made {
    /// The download the frame reads.
    pub(crate) download: Option<TempDownload>,
    /// The files a conversion wrote.
    pub(crate) converted: Vec<TempDownload>,
    pub(crate) notes: Vec<crate::notes::Note>,
    pub(crate) other_tables: Vec<String>,
    pub(crate) detail: Option<Arc<crate::formats::text_formats::Detail>>,
}

/// A failed open: why, and whether it was chosen on the home screen.
#[derive(Debug)]
pub(crate) struct Failed {
    pub(crate) message: String,
    pub(crate) from_home: bool,
}

/// A dataset read and ready to install, with everything the load hands over to it.
pub(crate) struct Loaded {
    pub(crate) state: DataTableState,
    /// What names the dataset: the path or URL asked for.
    pub(crate) path: Option<PathBuf>,
    pub(crate) options: OpenOptions,
    pub(crate) debug_label: Option<String>,
    /// The paths asked for, which `H` opens again; `None` for a frame handed over.
    pub(crate) paths: Option<Vec<PathBuf>>,
    /// Recorded as a recent once installed.
    pub(crate) recent: Option<PathBuf>,
    pub(crate) from_home: bool,
    /// The footer counter the dataset's own pass reports to from now on.
    pub(crate) footers: Arc<FooterProgress>,
}

/// What a phase's worker found.
pub(crate) enum LoadAnswer {
    /// The scan's frame; `path` names the dataset.
    Scanned {
        lf: Box<LazyFrame>,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The scan found one compressed delimited file, `file`, to decompress first.
    Compressed {
        file: PathBuf,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The spec a `--format` URL names, and the open's options to carry it in.
    SpecFetched {
        spec: Arc<crate::formats::Spec>,
        options: OpenOptions,
    },
    /// The scan found a compressed file, `file`, that `choice`'s spec reads once it is
    /// decompressed.
    CompressedRecords {
        file: PathBuf,
        path: Option<PathBuf>,
        choice: crate::formats::Choice,
        options: OpenOptions,
    },
    /// The decompressed copy of a file a spec reads. Dropped unused, it removes itself.
    DecompressedRecords {
        copy: TempDownload,
        path: PathBuf,
        choice: crate::formats::Choice,
        options: OpenOptions,
    },
    /// The scan found `files`, `bytes` in all as stored, which have to be converted as
    /// `what` says before they can be read: Arrow IPC streams, or GPS logs.
    Convert {
        what: Conversion,
        files: Vec<PathBuf>,
        bytes: u64,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// What the conversion wrote. Dropped unused, it removes its files.
    Converted {
        converted: Converted,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The scan found a file of several tables (a SQLite database, a NumPy archive),
    /// `file`, its tables named `tables`, and no `--table` to say which.
    Tables {
        file: PathBuf,
        tables: Vec<String>,
        path: Option<PathBuf>,
    },
    /// The scan found a local file no reader and no spec takes, or `--hex` asked for
    /// its bytes.
    Hex {
        file: PathBuf,
        asked: bool,
        record_size: Option<usize>,
    },
    /// The dataset, its schema read.
    SchemaRead {
        state: Box<DataTableState>,
        path: Option<PathBuf>,
        options: OpenOptions,
        debug_label: Option<String>,
    },
    /// The remote model's server sends whole files, not ranges: it is downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    NoRanges { options: OpenOptions },
    /// The remote file's size.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Sized(PendingDownload),
    /// A download started without asking passed its limit and was stopped, its
    /// partial file removed: the user is asked before it is fetched whole.
    #[cfg(any(feature = "http", feature = "cloud"))]
    PastLimit(PendingDownload),
    /// The remote file, downloaded. Dropped unused, it removes the file.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Downloaded {
        download: TempDownload,
        options: OpenOptions,
    },
    /// Standard input, read to a file, and `options` with the format it holds.
    Spooled {
        download: TempDownload,
        options: OpenOptions,
    },
    /// Standard input, recorded to the file `--tee` named, which is read from here on
    /// as any file is.
    Recorded { file: PathBuf, options: OpenOptions },
}

/// A load put down before it finished: which, and what of it the app has to put down.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Retired {
    pub(crate) id: LoadId,
    /// It was asking the user about a download: the question goes with it.
    pub(crate) asking: bool,
}

/// The owner of the open in flight, and of the last download, kept to be read again.
#[derive(Default)]
pub(crate) struct Loader {
    next_id: u64,
    load: Option<Load>,
    /// The last remote file an installed open downloaded, kept so reopening the URL (`H`)
    /// reads it instead of downloading again; released when other data opens, removed
    /// once the scanning dataset also lets go. Stdin, read once, is kept the same way.
    kept: Option<Fetched>,
    /// The files every load's workers have written and not yet let go, for quitting to
    /// remove. See [`crate::loading::unfinished`].
    unfinished: Unfinished,
}

impl Loader {
    /// The files this loader's opens are writing, swept when the app exits.
    pub(crate) fn unfinished(&self) -> &Unfinished {
        &self.unfinished
    }

    /// The open in flight, if there is one.
    pub(crate) fn current(&self) -> Option<&Load> {
        self.load.as_ref()
    }

    pub(crate) fn id(&self) -> Option<LoadId> {
        self.load.as_ref().map(|load| load.id)
    }

    fn current_in(&self, id: LoadId, phase: impl Fn(&Phase) -> bool) -> bool {
        self.load
            .as_ref()
            .is_some_and(|load| load.id == id && phase(&load.phase))
    }

    /// Whether `id` is still looking at the paths named on the command line.
    pub(crate) fn looking_at_paths(&self, id: LoadId) -> bool {
        self.current_in(id, |phase| matches!(phase, Phase::LookingAtPaths))
    }

    /// Whether `id` is still looking at a directory named on the command line.
    pub(crate) fn looking_at_directory(&self, id: LoadId) -> bool {
        self.current_in(id, |phase| matches!(phase, Phase::LookingAtDirectory))
    }

    /// Whether an open is on its way and its dataset is not installed yet. Whatever
    /// table is up meanwhile belongs to the dataset being replaced.
    pub(crate) fn awaiting_dataset(&self) -> bool {
        self.load
            .as_ref()
            .is_some_and(|load| !matches!(load.phase, Phase::FirstRows))
    }

    /// Whether the user waits on the open (keys held): not while it asks about a download
    /// (the question takes the keys), nor once the dataset is up.
    pub(crate) fn waits(&self) -> bool {
        self.awaiting_dataset() && !self.asking()
    }

    /// Whether the open is waiting on the user to agree to a download.
    pub(crate) fn asking(&self) -> bool {
        self.load.as_ref().is_some_and(|load| load.phase.asks())
    }

    /// Give the question being asked the hold that keeps the generation while it waits.
    pub(crate) fn hold_while_asking(&mut self, hold: Hold) {
        if let Some(Load {
            phase: Phase::ConfirmingRead { _hold, .. },
            ..
        }) = self.load.as_mut()
        {
            *_hold = Some(hold);
        }
    }

    /// Why the download being asked about is a download, when it would not have been.
    #[cfg(any(feature = "http", feature = "cloud"))]
    pub(crate) fn download_note(&self) -> Option<&'static str> {
        match self.load.as_ref().map(|load| &load.phase) {
            Some(Phase::Confirming { note, .. }) => *note,
            _ => None,
        }
    }

    /// The footer counter of the open that has not installed yet: what the loading
    /// screen counts.
    pub(crate) fn progress(&self) -> Option<&Arc<FooterProgress>> {
        self.load
            .as_ref()
            .filter(|load| !matches!(load.phase, Phase::FirstRows))
            .map(|load| &load.progress)
    }

    /// Put down the open in flight, if there is one, and what it holds. Its stop flag is
    /// raised unless its dataset is installed, when the counter is the dataset's.
    pub(crate) fn retire(&mut self) -> Option<Retired> {
        let load = self.load.take()?;
        if !matches!(load.phase, Phase::FirstRows) {
            load.progress.cancel();
        }
        Some(Retired {
            id: load.id,
            asking: load.phase.asks(),
        })
    }

    /// Make way for an open being asked for: the load in flight is retired, unless it
    /// has not started work of its own yet, in which case the open carries it on.
    pub(crate) fn make_way(&mut self) -> Option<Retired> {
        if self.load.as_ref().is_some_and(|load| load.phase.starting()) {
            return None;
        }
        self.retire()
    }

    /// The load an open being asked for belongs to: the starting one, or a new one.
    fn start(&mut self, from_home: bool) -> &mut Load {
        // A caller that did not make way would leave a load doing work behind with its
        // stop flag down; retired here so its download and its footer pass stop.
        if self
            .load
            .as_ref()
            .is_some_and(|load| !load.phase.starting())
        {
            debug_assert!(false, "an open started without making way");
            self.retire();
        }
        if self.load.is_none() {
            self.next_id = self.next_id.wrapping_add(1);
            let progress = Arc::<FooterProgress>::default();
            self.load = Some(Load {
                id: LoadId(self.next_id),
                from_home,
                phase: Phase::Starting {
                    label: "Loading".to_string(),
                    percent: 0,
                },
                path: None,
                size: 0,
                paths: None,
                recent: None,
                writer: self.unfinished.writer(progress.cancel_flag()),
                progress,
                download: None,
                converted: Vec::new(),
                warn_in_memory_above: None,
            });
        }
        let load = self.load.as_mut().expect("started just above");
        load.from_home |= from_home;
        load
    }

    /// An open is on its way, asked for from the home screen when `from_home`: the
    /// screen is handed over to it now, saying `label`.
    pub(crate) fn announce(&mut self, from_home: bool, label: String, percent: u16) -> LoadId {
        let load = self.start(from_home);
        load.phase = Phase::Starting { label, percent };
        load.id
    }

    /// An open whose dataset is up and whose first rows are being read, with nothing
    /// running: for tests of what ends that wait.
    #[cfg(test)]
    pub(crate) fn first_rows_for_tests(&mut self) {
        self.retire();
        self.start(false).phase = Phase::FirstRows;
    }

    /// Give the starting load's path a size, as an open asking the filesystem does.
    #[cfg(test)]
    pub(crate) fn size_for_tests(&mut self, size: u64) {
        if let Some(load) = self.load.as_mut() {
            load.size = size;
        }
    }

    /// Put `path` on the loading screen, so a wait says what it is waiting for.
    pub(crate) fn name(&mut self, path: PathBuf) {
        if let Some(load) = self
            .load
            .as_mut()
            .filter(|load| !matches!(load.phase, Phase::FirstRows))
        {
            load.path = Some(stdin::named(&path));
        }
    }

    /// The paths named on the command line are looked at before they are opened.
    pub(crate) fn look_at_paths(&mut self) -> LoadId {
        let load = self.start(false);
        load.phase = Phase::LookingAtPaths;
        load.id
    }

    /// A directory named on the command line is looked at before it is opened.
    pub(crate) fn look_at_directory(&mut self, dir: PathBuf) -> LoadId {
        let load = self.start(false);
        load.phase = Phase::LookingAtDirectory;
        load.path = Some(dir);
        load.id
    }

    /// Open `request`: carry on the starting load, or begin one. The last download is
    /// let go unless this opens it again.
    pub(crate) fn open(&mut self, request: OpenRequest) -> Step {
        if !(request.paths.len() == 1
            && self
                .kept
                .as_ref()
                .is_some_and(|kept| kept.url == request.paths[0]))
        {
            self.kept = None;
        }
        let OpenRequest {
            paths,
            mut options,
            size,
            recent,
            shown,
            warn_in_memory_above,
        } = request;
        // What an earlier load found of its Arrow is not this one's to read.
        options.arrow_parts = None;
        let prepared = options
            .prepared
            .take()
            .and_then(|handoff| handoff.lock().ok()?.take());
        let load = self.start(false);
        load.path = Some(shown.unwrap_or_else(|| stdin::named(&paths[0])));
        load.size = size;
        load.recent = recent;
        load.paths = Some(paths.clone());
        load.warn_in_memory_above = warn_in_memory_above;
        match prepared {
            Some(prepared) => self.install_prepared(*prepared),
            None => self.first_step(paths, options),
        }
    }

    /// Install the dataset home's preview built: scan, schema and first page are read, so
    /// the open goes straight to its (on-hand) first rows.
    fn install_prepared(&mut self, prepared: crate::home::home_preview::Prepared) -> Step {
        let crate::home::home_preview::Prepared {
            state,
            options,
            debug_label,
            progress,
        } = prepared;
        let writer = self.unfinished.writer(progress.cancel_flag());
        let load = self.load.as_mut().expect("started by the open");
        load.phase = Phase::FirstRows;
        // The dataset was built reporting to the preview's counter, which is the one
        // its own footer pass goes on with.
        load.progress = progress;
        load.writer = writer;
        Step::Install(Box::new(Loaded {
            state: *state,
            path: load.path.clone(),
            options,
            debug_label,
            paths: load.paths.clone(),
            recent: load.recent.clone(),
            from_home: load.from_home,
            footers: load.progress.clone(),
        }))
    }

    /// Open a frame handed over (the Python binding's): only its schema is read.
    pub(crate) fn open_frame(&mut self, lf: LazyFrame, options: OpenOptions) -> Step {
        let load = self.start(false);
        load.path = None;
        load.size = 0;
        load.paths = None;
        load.recent = None;
        load.phase = Phase::ReadingSchema;
        Step::ReadSchema {
            lf: Box::new(lf),
            path: None,
            options,
            progress: load.progress.clone(),
            made: Made::default(),
        }
    }

    /// What the open of `paths` does first: read standard input, decompress, download,
    /// or scan.
    fn first_step(&mut self, paths: Vec<PathBuf>, options: OpenOptions) -> Step {
        let first = paths[0].clone();
        let src = source::input_source(&first);
        if paths.len() > 1 {
            let only_one = match &src {
                source::InputSource::S3(_) => {
                    Some("Only one S3 URL at a time. Open a single s3:// path.")
                }
                source::InputSource::Gcs(_) => {
                    Some("Only one GCS URL at a time. Open a single gs:// path.")
                }
                source::InputSource::Azure(_) => {
                    Some("Only one Azure URL at a time. Open a single abfss:// path.")
                }
                source::InputSource::Http(_) => {
                    Some("Only one HTTP/HTTPS URL at a time. Open a single URL.")
                }
                source::InputSource::Local(_) => None,
            };
            if let Some(message) = only_one {
                self.load = None;
                return Step::Crash(message.to_string());
            }
        }
        if let Some(message) = stdin::refuse(&paths, true) {
            self.load = None;
            return Step::Crash(message.to_string());
        }
        if options.follow
            && let Some(message) = crate::loading::follow::refuse_paths(&paths, &options)
        {
            self.load = None;
            return Step::Crash(message);
        }
        // A remote spec is fetched once, before any phase reads with it; the open
        // then starts again from here with it in hand.
        if let Some(url) = options
            .spec_file
            .clone()
            .filter(|file| options.spec_fetched.is_none() && source::is_remote_url(file))
        {
            let load = self.load.as_mut().expect("an open has a load");
            load.phase = Phase::ReadingSpec;
            return Step::FetchSpec {
                url,
                options,
                writer: load.writer.clone(),
            };
        }
        if stdin::is_stdin(&first) {
            // Read once: opened again (`H`), the copy on hand is read.
            if let Some(kept) = self.kept.clone().filter(|kept| {
                kept.url == first && kept.file.path().exists() && kept.serves(&options)
            }) {
                return self.read_download(kept, options);
            }
            let load = self.load.as_mut().expect("an open has a load");
            let read = Arc::<AtomicU64>::default();
            load.phase = Phase::Spooling { read: read.clone() };
            return Step::Spool {
                options,
                writer: load.writer.clone(),
                read,
            };
        }
        let load = self.load.as_mut().expect("an open has a load");
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(&first));
        let delimited = delimited_format(&first, &options);
        if matches!(src, source::InputSource::Local(_))
            && paths.len() == 1
            && compression.is_some()
            && let Some(format) = delimited
        {
            load.phase = Phase::Decompressing;
            return Step::Decompress {
                file: first.clone(),
                path: first,
                options: OpenOptions {
                    format: Some(format),
                    ..options
                },
                writer: load.writer.clone(),
                download: None,
            };
        }
        // Opened again, and downloaded already: read the copy on hand.
        #[cfg(any(feature = "http", feature = "cloud"))]
        if let Some(kept) = self
            .kept
            .clone()
            .filter(|kept| paths.len() == 1 && kept.file.path().exists() && kept.serves(&options))
        {
            return self.read_download(kept, options);
        }
        // A remote model's headers are all it needs: read by range, not downloaded.
        #[cfg(any(feature = "http", feature = "cloud"))]
        if paths.len() == 1
            && let Some(format) = crate::cloud::remote_model::model_format(&first, options.format)
        {
            let load = self.load.as_mut().expect("an open has a load");
            load.phase = Phase::ReadingHeaders;
            return Step::ReadHeaders {
                url: first,
                format,
                options,
                writer: load.writer.clone(),
            };
        }
        #[cfg(any(feature = "http", feature = "cloud"))]
        if let Some(pending) = remote_download(&src, &options) {
            let load = self.load.as_mut().expect("an open has a load");
            load.phase = Phase::CheckingSize { note: None };
            return Step::Probe(pending);
        }
        let load = self.load.as_mut().expect("an open has a load");
        if counts_footer(&paths, &options) {
            load.phase = Phase::CountingFooter;
            let display = load.path.clone().filter(|shown| *shown != paths[0]);
            return Step::Scan {
                paths,
                options,
                display,
                status: COUNTING_FOOTER_STATUS,
            };
        }
        if paths.len() == 1 && delimited.is_some() && options.parse_strings.is_some() {
            load.phase = Phase::ScanningStrings;
            return Step::Scan {
                paths,
                options,
                display: None,
                status: "Scanning string columns...",
            };
        }
        // A table inside a database goes by its path there, on screen and once open.
        let display = load.path.clone().filter(|shown| *shown != paths[0]);
        // A large file read whole is put to the user before the read starts.
        if let Some(limit) = load.warn_in_memory_above
            && let Some(read) = in_memory(&paths, &options)
            && read.bytes > limit
        {
            load.phase = Phase::ConfirmingRead {
                scan: Box::new(Scan {
                    paths,
                    options,
                    display,
                }),
                _hold: None,
            };
            return Step::AskRead(read);
        }
        if reads_lines(&paths, &options) {
            load.phase = Phase::ReadingLines;
            return Step::Scan {
                paths,
                options,
                display,
                status: READING_LINES_STATUS,
            };
        }
        load.phase = Phase::Scanning { downloaded: false };
        Step::Scan {
            paths,
            options,
            display,
            status: "Scanning input...",
        }
    }

    /// Read a download: decompress compressed CSV, TSV or PSV first, else scan it, named by
    /// the URL (or `stdin`), not the temp file.
    fn read_download(&mut self, fetched: Fetched, mut options: OpenOptions) -> Step {
        if let Some(arrow) = &fetched.arrow {
            options.format = Some(FileFormat::Arrow);
            options.hive = false;
            options.arrow_parts = Some(arrow.parts.clone());
            options.splits = arrow.splits.clone();
        }
        let load = self.load.as_mut().expect("a download read has a load");
        let file = fetched.file.path().to_path_buf();
        let url = stdin::named(&fetched.url);
        let download = fetched.file.clone();
        load.download = Some(fetched);
        // Compressed delimited text must be decompressed before scanning, as from disk.
        let compressed = options
            .compression
            .or_else(|| CompressionFormat::from_extension(&file))
            .is_some();
        if compressed && let Some(format) = delimited_format(&file, &options) {
            load.phase = Phase::Decompressing;
            return Step::Decompress {
                file,
                path: url,
                options: OpenOptions {
                    format: Some(format),
                    ..options
                },
                writer: load.writer.clone(),
                download: Some(download),
            };
        }
        let paths = vec![file];
        let status = if counts_footer(&paths, &options) {
            load.phase = Phase::CountingFooter;
            COUNTING_FOOTER_STATUS
        } else if reads_lines(&paths, &options) {
            load.phase = Phase::ReadingLines;
            READING_LINES_STATUS
        } else {
            load.phase = Phase::Scanning { downloaded: true };
            "Scanning..."
        };
        Step::Scan {
            paths,
            options,
            display: Some(url),
            status,
        }
    }

    /// A worker of `id` answered. Taken only while `id` is in flight and in the phase
    /// that asked; otherwise the answer, and whatever it carries, is dropped.
    pub(crate) fn answered(
        &mut self,
        id: LoadId,
        answer: LoadAnswer,
        #[cfg(any(feature = "http", feature = "cloud"))] jobs: &Jobs,
    ) -> Step {
        let Some(load) = self.load.as_mut().filter(|load| load.id == id) else {
            return Step::Nothing;
        };
        match (answer, &load.phase) {
            (
                LoadAnswer::Scanned { lf, path, options },
                Phase::Scanning { .. }
                | Phase::ScanningStrings
                | Phase::CountingFooter
                | Phase::ReadingRecords
                | Phase::ReadingLines,
            ) => {
                load.phase = Phase::ReadingSchema;
                Step::ReadSchema {
                    lf,
                    path,
                    options,
                    progress: load.progress.clone(),
                    made: load.made(),
                }
            }
            (
                LoadAnswer::Convert {
                    what,
                    files,
                    bytes,
                    path,
                    options,
                },
                Phase::Scanning { .. }
                | Phase::ScanningStrings
                | Phase::CountingFooter
                | Phase::ReadingLines,
            ) if load.converted.is_empty() => {
                let read = Arc::<AtomicU64>::default();
                load.phase = Phase::Converting {
                    what,
                    read: read.clone(),
                    total: bytes,
                };
                Step::Convert {
                    what,
                    files,
                    path,
                    options,
                    writer: load.writer.clone(),
                    read,
                }
            }
            (
                LoadAnswer::Converted {
                    converted: Converted::Streams { file, parts },
                    path,
                    options,
                },
                Phase::Converting { .. },
            ) => {
                let paths = vec![file.path().to_path_buf()];
                // A converted download or pipe spool is kept as its copy, to reread without
                // converting; the stream is released.
                if let Some(fetched) = load.download.as_mut() {
                    fetched.file = file.clone();
                }
                load.converted = vec![file];
                load.phase = Phase::Scanning { downloaded: true };
                Step::Scan {
                    paths,
                    options: OpenOptions {
                        format: Some(FileFormat::Arrow),
                        hive: false,
                        arrow_parts: Some(Arc::new(parts)),
                        ..options
                    },
                    display: path,
                    status: "Scanning...",
                }
            }
            (
                LoadAnswer::Converted {
                    converted:
                        Converted::Frame {
                            files,
                            lf,
                            notes,
                            other_tables,
                            detail,
                        },
                    path,
                    options,
                },
                Phase::Converting { .. },
            ) => {
                load.converted = files;
                load.phase = Phase::ReadingSchema;
                Step::ReadSchema {
                    lf,
                    path,
                    options,
                    progress: load.progress.clone(),
                    made: Made {
                        notes,
                        other_tables,
                        detail,
                        ..load.made()
                    },
                }
            }
            (
                LoadAnswer::Compressed {
                    file,
                    path,
                    options,
                },
                Phase::Scanning { .. }
                | Phase::ScanningStrings
                | Phase::CountingFooter
                | Phase::ReadingLines,
            ) => {
                load.phase = Phase::Decompressing;
                Step::Decompress {
                    path: path.unwrap_or_else(|| file.clone()),
                    file,
                    options,
                    writer: load.writer.clone(),
                    download: load.download.as_ref().map(|fetched| fetched.file.clone()),
                }
            }
            (LoadAnswer::SpecFetched { spec, options }, Phase::ReadingSpec) => {
                let paths = load.paths.clone().unwrap_or_default();
                if paths.is_empty() {
                    return Step::Nothing;
                }
                self.first_step(
                    paths,
                    OpenOptions {
                        spec_fetched: Some(spec),
                        ..options
                    },
                )
            }
            (
                LoadAnswer::CompressedRecords {
                    file,
                    path,
                    choice,
                    options,
                },
                Phase::Scanning { .. } | Phase::ScanningStrings | Phase::ReadingLines,
            ) => {
                load.phase = Phase::DecompressingRecords;
                Step::DecompressRecords {
                    path: path.unwrap_or_else(|| file.clone()),
                    file,
                    choice,
                    options,
                    writer: load.writer.clone(),
                }
            }
            (
                LoadAnswer::DecompressedRecords {
                    copy,
                    path,
                    choice,
                    options,
                },
                Phase::DecompressingRecords,
            ) => {
                // The load holds the copy until the dataset built from it does.
                let file = copy.path().to_path_buf();
                load.converted = vec![copy];
                load.phase = Phase::ReadingRecords;
                Step::ReadRecords {
                    copy: file,
                    path,
                    choice,
                    options,
                }
            }
            (
                LoadAnswer::Hex {
                    file,
                    asked,
                    record_size,
                },
                Phase::Scanning { .. }
                | Phase::ScanningStrings
                | Phase::CountingFooter
                | Phase::ReadingLines,
            ) => {
                let from_home = load.from_home;
                // A download is a temporary file the load owns; it has no bytes to show
                // once the load is put down.
                let fetched = load.download.is_some();
                let named = load.path.clone();
                self.retire();
                if fetched {
                    let message = match named {
                        Some(path) => crate::error_display::file_message(&path, crate::UNSUPPORTED),
                        None => crate::UNSUPPORTED.to_string(),
                    };
                    return Step::Failed(Failed { message, from_home });
                }
                Step::Hex(Hex {
                    file,
                    from_home,
                    asked,
                    record_size,
                })
            }
            (
                LoadAnswer::Tables { file, tables, path },
                Phase::Scanning { .. }
                | Phase::ScanningStrings
                | Phase::CountingFooter
                | Phase::ReadingLines,
            ) => {
                let from_home = load.from_home;
                let database = path.unwrap_or(file);
                // A download or standard input has no place on the home screen to list
                // the tables at: the table is named on the command line instead.
                let fetched = load.download.is_some();
                self.retire();
                if fetched {
                    let shown = tables.iter().take(20).cloned().collect::<Vec<_>>();
                    let more = tables.len().saturating_sub(shown.len());
                    let more = match more {
                        0 => String::new(),
                        n => format!(" and {n} more"),
                    };
                    return Step::Failed(Failed {
                        message: format!(
                            "{} holds {} tables: {}{more}. Open one with --table NAME.",
                            database.display(),
                            tables.len(),
                            shown.join(", ")
                        ),
                        from_home,
                    });
                }
                Step::Tables(Tables {
                    database,
                    from_home,
                })
            }
            (
                LoadAnswer::SchemaRead {
                    state,
                    path,
                    options,
                    debug_label,
                },
                phase,
            ) if phase.builds_the_dataset() => {
                load.phase = Phase::FirstRows;
                // The dataset was built holding its download (`Step::ReadSchema`); the
                // loader keeps it too, to be read again.
                if let Some(fetched) = load.download.take() {
                    self.kept = Some(fetched);
                }
                let state = *state;
                let load = self.load.as_ref().expect("installing its load");
                Step::Install(Box::new(Loaded {
                    state,
                    path,
                    options,
                    debug_label,
                    paths: load.paths.clone(),
                    recent: load.recent.clone(),
                    from_home: load.from_home,
                    footers: load.progress.clone(),
                }))
            }
            // Arrow in a store with no stream in it: nothing to download or ask about.
            #[cfg(feature = "cloud")]
            (
                LoadAnswer::Sized(PendingDownload::Arrow {
                    url,
                    objects,
                    options,
                    ..
                }),
                Phase::CheckingSize { .. },
            ) if crate::cloud::cloud_arrow::in_place(&objects).is_some() => {
                load.phase = Phase::Scanning { downloaded: false };
                let parts = crate::cloud::cloud_arrow::in_place(&objects).unwrap_or_default();
                Step::Scan {
                    paths: vec![PathBuf::from(url)],
                    options: OpenOptions {
                        format: Some(FileFormat::Arrow),
                        hive: false,
                        arrow_parts: Some(Arc::new(parts)),
                        ..options
                    },
                    display: None,
                    status: "Scanning...",
                }
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::NoRanges { options }, Phase::ReadingHeaders) => {
                let first = load.paths.as_ref().and_then(|paths| paths.first().cloned());
                match first
                    .and_then(|first| remote_download(&source::input_source(&first), &options))
                {
                    Some(pending) => {
                        load.phase = Phase::CheckingSize {
                            note: Some(NO_RANGES),
                        };
                        Step::Probe(pending)
                    }
                    None => self.failed(id, NO_RANGES),
                }
            }
            // A small file of the built-in catalog: its row said what it is and what it
            // weighs, so it is fetched without a question.
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::Sized(pending), Phase::CheckingSize { note: None })
                if pending
                    .parts()
                    .2
                    .download_unasked
                    .is_some_and(|unasked| unasked.covers(pending.parts().1)) =>
            {
                load.phase = Phase::Downloading;
                Step::Download {
                    pending,
                    writer: load.writer.clone(),
                }
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::Sized(pending), Phase::CheckingSize { note }) => {
                load.phase = Phase::Confirming {
                    pending: Box::new(pending.clone()),
                    note: *note,
                    _hold: jobs.hold(),
                };
                Step::Ask(pending)
            }
            // More arrived than the catalog said: asked once, as any large download is.
            // Its size is unknown now, whatever the server said.
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::PastLimit(pending), Phase::Downloading) => {
                let pending = pending.with_size(None);
                load.phase = Phase::Confirming {
                    pending: Box::new(pending.clone()),
                    note: Some(PAST_LIMIT),
                    _hold: jobs.hold(),
                };
                Step::Ask(pending)
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::Downloaded { download, options }, Phase::Downloading) => {
                let fetched = Fetched {
                    // The URL the open was asked for, which is what opening it again names.
                    url: load.path.clone().unwrap_or_default(),
                    file: download,
                    arrow: options.arrow_parts.clone().map(|parts| KeptArrow {
                        parts,
                        splits: options.splits.clone(),
                        table: options.table.clone(),
                    }),
                };
                self.read_download(fetched, options)
            }
            (LoadAnswer::Recorded { file, options }, Phase::Spooling { read }) => {
                load.size = read.load(Ordering::Relaxed);
                // The user's file, not a copy: `H` reads it again, and it is a recent.
                self.kept = None;
                load.path = Some(file.clone());
                load.paths = Some(vec![file.clone()]);
                load.recent = Some(file.clone());
                let paths = vec![file];
                let status = if counts_footer(&paths, &options) {
                    load.phase = Phase::CountingFooter;
                    COUNTING_FOOTER_STATUS
                } else {
                    load.phase = Phase::Scanning { downloaded: false };
                    "Scanning input..."
                };
                Step::Scan {
                    paths,
                    options,
                    display: None,
                    status,
                }
            }
            (LoadAnswer::Spooled { download, options }, Phase::Spooling { read }) => {
                load.size = read.load(Ordering::Relaxed);
                let fetched = Fetched {
                    url: PathBuf::from(stdin::PATH),
                    file: download,
                    arrow: None,
                };
                self.read_download(fetched, options)
            }
            _ => Step::Nothing,
        }
    }

    /// A worker of `id` failed. The open ends there, with its reason, unless it is no
    /// longer the one in flight or its dataset is already up.
    pub(crate) fn failed(&mut self, id: LoadId, message: &str) -> Step {
        let Some(load) = self.load.as_ref().filter(|load| load.id == id) else {
            return Step::Nothing;
        };
        if matches!(load.phase, Phase::FirstRows) {
            return Step::Nothing;
        }
        let from_home = load.from_home;
        // A download is read from a temp file the user never typed: the reason names
        // the URL they did, or `stdin`.
        let mut message = match &load.download {
            Some(fetched) => crate::error_display::named_by_source(
                message,
                fetched.file.path(),
                &stdin::named(&fetched.url),
            ),
            None => message.to_string(),
        };
        // So are the IPC files a conversion wrote.
        if let Some(path) = &load.path {
            for converted in &load.converted {
                message = crate::error_display::named_by_source(&message, converted.path(), path);
            }
        }
        self.retire();
        Step::Failed(Failed { message, from_home })
    }

    /// The user agreed to what the open asked: let go of the hold, and fetch the
    /// download or start the read.
    pub(crate) fn confirmed(&mut self) -> Step {
        let Some(load) = self.load.as_mut().filter(|load| load.phase.asks()) else {
            return Step::Nothing;
        };
        // The hold goes as the phase changes: the job the caller starts next holds the
        // generation before anything else can look at it.
        match std::mem::replace(&mut load.phase, Phase::Scanning { downloaded: false }) {
            Phase::ConfirmingRead { scan, .. } => {
                let Scan {
                    paths,
                    options,
                    display,
                } = *scan;
                Step::Scan {
                    paths,
                    options,
                    display,
                    status: "Scanning input...",
                }
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::Confirming { pending, .. } => {
                load.phase = Phase::Downloading;
                Step::Download {
                    pending: pending.asked(),
                    writer: load.writer.clone(),
                }
            }
            _ => unreachable!("a phase that asks"),
        }
    }

    /// The first rows of the installed dataset are on screen, or will not be read: the
    /// open is done.
    pub(crate) fn first_rows_settled(&mut self) {
        if self
            .load
            .as_ref()
            .is_some_and(|load| matches!(load.phase, Phase::FirstRows))
        {
            self.load = None;
        }
    }
}

impl Drop for Loader {
    /// A download still running stops at its next chunk and removes its partial file.
    fn drop(&mut self) {
        self.retire();
    }
}

/// What the loading screen and the footer say while text is read as lines.
const READING_LINES: &str = "Reading as lines";
const READING_LINES_STATUS: &str = "Reading as lines...";

/// Whether `paths` are read as lines per `--format` or their names, for the loading line
/// (bytes may still say otherwise, e.g. a candump `.log`).
fn reads_lines(paths: &[PathBuf], options: &OpenOptions) -> bool {
    options
        .format
        .or_else(|| match paths {
            [one] => FileFormat::from_path(one),
            _ => None,
        })
        .is_some_and(FileFormat::is_lines)
}

/// What reading `paths` would read whole into memory, by names and options only (not
/// bytes, on the event thread): local files of an in-memory format
/// ([`FileFormat::read_mode`]). `None` when none is.
pub(crate) fn in_memory(paths: &[PathBuf], options: &OpenOptions) -> Option<InMemory> {
    let mut found: Option<InMemory> = None;
    for path in paths {
        if !matches!(source::input_source(path), source::InputSource::Local(_)) {
            continue;
        }
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path));
        // By name, or by the first bytes the open will judge it by: journal JSON
        // piped to a file with no name, or in a `.json` one.
        let format = options.format.or_else(|| match compression {
            Some(_) => path
                .file_stem()
                .and_then(|stem| FileFormat::from_path(Path::new(stem))),
            None => match FileFormat::from_path(path) {
                Some(named) => Some(crate::formats::readers::refined(path, named).unwrap_or(named)),
                None => crate::formats::readers::sniff_open(path, None),
            },
        });
        let Some(format) = format else {
            continue;
        };
        let stored = match compression {
            Some(_) => crate::Stored::Compressed {
                in_memory: options.decompress_in_memory,
            },
            None => crate::Stored::Plain,
        };
        // A model file's table comes from its header, which is what a URL of one is
        // read in place for: small however large the file.
        if format.read_mode(stored) != Some(crate::ReadMode::InMemory)
            || format.http_file() == crate::RemoteRead::InPlace
        {
            continue;
        }
        let Some(bytes) = std::fs::metadata(path)
            .ok()
            .filter(|m| m.is_file())
            .map(|m| m.len())
        else {
            continue;
        };
        match &mut found {
            Some(read) => {
                read.bytes += bytes;
                read.files += 1;
            }
            None => {
                found = Some(InMemory {
                    bytes,
                    format,
                    files: 1,
                    name: path.clone(),
                })
            }
        }
    }
    found
}

/// What the loading screen says while a CSV is counted to drop its footer rows.
const COUNTING_FOOTER: &str = "Counting rows to skip the footer";
/// The footer's line for the same wait.
const COUNTING_FOOTER_STATUS: &str = "Counting rows to skip the footer...";

/// Whether the scan of `paths` counts every row first: delimited text whose footer
/// rows are dropped (`--footer-rows`).
fn counts_footer(paths: &[PathBuf], options: &OpenOptions) -> bool {
    options.skip_tail_rows.is_some_and(|n| n > 0)
        && paths.iter().all(|p| delimited_format(p, options).is_some())
}

/// The delimited format (CSV, TSV, PSV) or text `path` is read as, if any: `--format`,
/// else the extension past any compression suffix (`x.tsv.gz` is TSV).
pub(crate) fn delimited_format(path: &Path, options: &OpenOptions) -> Option<FileFormat> {
    let format = options.format.or_else(|| {
        FileFormat::from_path(path).or_else(|| {
            CompressionFormat::from_extension(path)?;
            FileFormat::from_path(Path::new(path.file_stem()?))
        })
    })?;
    format.decompressed_once().then_some(format)
}

/// Why a catalog file that started without a question asks partway.
#[cfg(any(feature = "http", feature = "cloud"))]
pub(crate) const PAST_LIMIT: &str =
    "This catalog file passed 50 MB, more than it may download without asking.";

/// Why a remote model is downloaded rather than read by its headers.
#[cfg(any(feature = "http", feature = "cloud"))]
pub(crate) const NO_RANGES: &str = "The server does not send byte ranges, so the model's header cannot be read without downloading the whole file.";

/// The download a remote source needs first, if any: always for HTTP, and for a store
/// object that cannot be scanned in place, or one a format spec reads.
#[cfg(any(feature = "http", feature = "cloud"))]
fn remote_download(src: &source::InputSource, options: &OpenOptions) -> Option<PendingDownload> {
    #[cfg(feature = "cloud")]
    let spec = options.spec_file.is_some() || options.spec_name.is_some();
    #[cfg(feature = "cloud")]
    let should_download =
        |url: &str| should_download(url) || (spec && !source::is_prefix_or_glob(url));
    #[cfg(feature = "cloud")]
    if !spec && let Some(arrow) = cloud_arrow(src, options) {
        return Some(arrow);
    }
    let options = options.clone();
    match src {
        #[cfg(feature = "http")]
        source::InputSource::Http(url) => Some(PendingDownload::Http {
            url: url.clone(),
            size: None,
            options,
        }),
        #[cfg(feature = "cloud")]
        source::InputSource::S3(url) => {
            let full = format!("s3://{url}");
            should_download(&full).then_some(PendingDownload::S3 {
                url: full,
                size: None,
                options,
            })
        }
        #[cfg(feature = "cloud")]
        source::InputSource::Gcs(url) => {
            let full = format!("gs://{url}");
            should_download(&full).then_some(PendingDownload::Gcs {
                url: full,
                size: None,
                options,
            })
        }
        #[cfg(feature = "cloud")]
        source::InputSource::Azure(url) => should_download(url).then(|| PendingDownload::Azure {
            url: url.clone(),
            size: None,
            options,
        }),
        _ => None,
    }
}

/// Arrow in a store (object or listed prefix, not a glob): the probe lists it, its
/// streams are downloaded, its IPC files scanned in place.
#[cfg(feature = "cloud")]
fn cloud_arrow(src: &source::InputSource, options: &OpenOptions) -> Option<PendingDownload> {
    let url = match src {
        source::InputSource::S3(url) => format!("s3://{url}"),
        source::InputSource::Gcs(url) => format!("gs://{url}"),
        source::InputSource::Azure(url) => url.clone(),
        _ => return None,
    };
    if url.contains('*') {
        return None;
    }
    let arrow = if url.ends_with('/') {
        options.format == Some(FileFormat::Arrow)
    } else {
        // Compressed, it is a download like any other compressed object.
        options.compression.is_none()
            && options
                .format
                .or_else(|| FileFormat::from_path(Path::new(&url)))
                == Some(FileFormat::Arrow)
    };
    arrow.then(|| PendingDownload::Arrow {
        url,
        objects: Vec::new(),
        size: None,
        options: options.clone(),
    })
}

#[cfg(feature = "cloud")]
fn should_download(url: &str) -> bool {
    let (_, ext) = source::url_path_extension(url);
    source::cloud_path_should_download(ext.as_deref(), source::is_prefix_or_glob(url))
}

#[cfg(test)]
mod tests;

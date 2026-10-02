//! Opening a dataset, from the request to its first rows, and its one owner.
//!
//! [`Loader`] holds the open in flight ([`Load`]): where it was asked from, the paths it
//! was asked for, the phase it is in, what the loading screen says about it, and what
//! it holds — its stop flag and footer counter, the download it fetched, the IPC file
//! its Arrow streams were converted to, and the hold on the generation while the user
//! is asked about a download. The app tells it what happened (an open asked for, a
//! phase's worker answering or failing, the user's answer to the download question) and
//! it says what to do next ([`Step`]). The app carries the step out, runs the workers
//! and installs the dataset. Nothing else keeps a copy of the open's state.
//!
//! - **Identity.** Every load has a [`LoadId`], and the jobs of its phases carry it. An
//!   answer is taken only while its load is the one in flight and in the phase that asked
//!   for it: one from an open that was abandoned or replaced installs nothing and changes
//!   no title, and its payload is dropped with it.
//! - **Retirement.** Abandoning, replacing or failing a load drops what it holds: its
//!   stop flag is raised, so a download or a conversion stops at its next chunk and
//!   removes its file and a footer pass stops issuing reads; its download and converted
//!   file are let go; its hold is released.
//! - **Handover.** The dataset is built holding the load's download or converted file,
//!   with everything else the open found ([`crate::widgets::datatable::OpenFacts`]), and
//!   on install takes the load's footer counter. From then on they are the dataset's:
//!   what is left of the load is the read of the first rows ([`Phase::FirstRows`]), and
//!   abandoning that stops neither.
//!
//! The home screen's looks at a path, analyses and charts are not loads, and are not here.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use polars::prelude::LazyFrame;

use crate::download::TempDownload;
use crate::schema_union::FooterProgress;
use crate::unfinished::{Unfinished, Writer};
use crate::widgets::datatable::DataTableState;
use crate::{CompressionFormat, FileFormat, OpenOptions, source, stdin};

#[cfg(any(feature = "http", feature = "cloud"))]
use crate::jobs::{Hold, Jobs};

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
        }
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
}

impl OpenRequest {
    /// The request for `paths`, asking the filesystem for the first one's size and
    /// whether it is there.
    ///
    /// Every open records a recent, not just those started from the home screen — most
    /// datasets are named on the command line, and those are exactly the ones worth
    /// getting back to. An object-store URL counts doubly: `s3://bucket/warehouse/events`
    /// is far more painful to retype than any local path, and it is recorded verbatim.
    /// Kept as named, since what is installed may be a download's temporary copy.
    ///
    /// A table inside a SQLite database, as the home screen lists one (`app.db/users`),
    /// is the database opened with `--table`, and is recorded as the table.
    pub(crate) fn named(mut paths: Vec<PathBuf>, mut options: OpenOptions) -> Self {
        let first = paths[0].clone();
        let piped = stdin::is_stdin(&first);
        let local = !piped && matches!(source::input_source(&first), source::InputSource::Local(_));
        let mut table = None;
        if local
            && paths.len() == 1
            && let Some((db, name)) = crate::sqlite::table_path(&first)
        {
            table = Some(first.clone());
            options.table = Some(name);
            paths = vec![db];
        } else if local
            && let Some(name) = options.table.as_deref()
            && first.is_file()
            && crate::sqlite::is_sqlite_file(&first)
        {
            table = Some(crate::sqlite::table_place(&first, name));
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
    /// A file being converted to one the dataset scans, `read` of its `total` bytes:
    /// Arrow IPC streams to one IPC file, or a GPS log to its table. `what` says which.
    Converting {
        what: &'static str,
        read: Arc<AtomicU64>,
        total: u64,
    },
    /// A CSV read with its string columns parsed.
    ScanningStrings,
    /// The scan; `downloaded` when it reads a download rather than what was named.
    Scanning {
        downloaded: bool,
    },
    /// The schema, and whatever the dataset needs before its first rows.
    ReadingSchema,
    /// Installed: its first rows are being read. The dataset is the one on screen, and
    /// what the load held is its now.
    FirstRows,
}

impl Phase {
    /// What the loading screen and the control bar call this phase, and the flat
    /// percentage the bar shows beside it (0 for none).
    pub(crate) fn label(&self) -> (&str, u16) {
        match self {
            Phase::Starting { label, percent } => (label, *percent),
            Phase::LookingAtPaths => ("Scanning input", 10),
            Phase::LookingAtDirectory => (crate::App::LOOKING_AT_A_DIRECTORY, 5),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::ReadingHeaders => ("Reading headers", 20),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::CheckingSize { .. } | Phase::Confirming { .. } => ("Checking size", 0),
            #[cfg(any(feature = "http", feature = "cloud"))]
            Phase::Downloading => ("Downloading", 20),
            Phase::Spooling { .. } => ("Reading stdin", 5),
            Phase::Decompressing => ("Decompressing", 30),
            Phase::Converting { what, read, total } => {
                let done = read.load(Ordering::Relaxed).min(*total);
                // Up to the scan of the converted file that follows.
                let share = (done * 20).checked_div(*total).unwrap_or(0);
                (what, 10 + share as u16)
            }
            Phase::ScanningStrings => ("Scanning string columns", 55),
            Phase::Scanning { downloaded: false } => ("Scanning input", 10),
            Phase::Scanning { downloaded: true } => ("Scanning", 30),
            Phase::ReadingSchema => ("Caching schema", 40),
            Phase::FirstRows => ("Loading buffer", 70),
        }
    }

    /// Whether this phase's worker answers with the dataset itself.
    fn builds_the_dataset(&self) -> bool {
        match self {
            Phase::ReadingSchema | Phase::Decompressing | Phase::Converting { .. } => true,
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
    /// The IPC file its Arrow streams were converted to, which the dataset scans.
    converted: Option<TempDownload>,
}

impl Load {
    pub(crate) fn phase(&self) -> &Phase {
        &self.phase
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
    /// Download it, writing through `writer`: the load's stop flag, and its claim on
    /// the file for quitting to find.
    #[cfg(any(feature = "http", feature = "cloud"))]
    Download {
        pending: PendingDownload,
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
    /// Convert the Arrow IPC streams `files` to one IPC file, writing through `writer`
    /// and counting the bytes read in `read`; `path` names them on screen and in errors.
    Convert {
        files: Vec<PathBuf>,
        path: Option<PathBuf>,
        options: OpenOptions,
        writer: Writer,
        read: Arc<AtomicU64>,
    },
    /// Read `file`, a GPS log, into a table of its own through `writer`, counting its
    /// progress in `read`, then its schema, reporting to `progress`; `path` names it on
    /// screen and in errors.
    ReadInto {
        file: PathBuf,
        path: PathBuf,
        options: OpenOptions,
        writer: Writer,
        read: Arc<AtomicU64>,
        progress: Arc<FooterProgress>,
        /// The download `file` is, given to the dataset built from it.
        download: Option<TempDownload>,
    },
    /// Scan `paths`, saying `status` on the control bar; `display` names the dataset when
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
        /// The download the scan reads, given to the dataset built from it.
        download: Option<TempDownload>,
    },
    /// Install the dataset, then read its first rows.
    Install(Box<Loaded>),
    /// The open failed. Its load is retired.
    Failed(Failed),
    /// The open found a SQLite database of several tables and was asked for none: the
    /// home screen lists them. Its load is retired.
    Tables(Tables),
}

/// A database of several tables, to be listed on the home screen.
#[derive(Debug)]
pub(crate) struct Tables {
    /// The database file, as the user named it.
    pub(crate) database: PathBuf,
    pub(crate) from_home: bool,
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
    /// The scan found Arrow IPC streams, `bytes` in all, which have to be converted
    /// before they can be scanned.
    Streams {
        files: Vec<PathBuf>,
        bytes: u64,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The streams, converted to one IPC file. Dropped unused, it removes the file.
    Converted {
        file: TempDownload,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The scan found a file to read into a table of its own first: a GPS log of
    /// `total` bytes (as stored).
    ReadInto {
        file: PathBuf,
        total: u64,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// The scan found a SQLite database, `file`, of several tables named `tables`, and
    /// no `--table` to say which.
    Tables {
        file: PathBuf,
        tables: Vec<String>,
        path: Option<PathBuf>,
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
    /// The last remote file downloaded by an open that installed, kept so opening the
    /// same URL again reads it rather than downloading it again: how `H` re-reads a
    /// download. Let go when different data is opened; the dataset scanning it holds
    /// the file too, so it is removed once both have let go. Standard input, read once,
    /// is kept the same way.
    kept: Option<Fetched>,
    /// The files every load's workers have written and not yet let go, for quitting to
    /// remove. See [`crate::unfinished`].
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

    /// Whether the user waits on the open: keys are held while it works. Not while it
    /// asks about a download, which the question's own keys answer, and not once the
    /// dataset is up, when the read of its first rows holds them.
    pub(crate) fn waits(&self) -> bool {
        self.awaiting_dataset() && !self.asking()
    }

    /// Whether the open is waiting on the user to agree to a download.
    pub(crate) fn asking(&self) -> bool {
        #[cfg(any(feature = "http", feature = "cloud"))]
        {
            self.load
                .as_ref()
                .is_some_and(|load| matches!(load.phase, Phase::Confirming { .. }))
        }
        #[cfg(not(any(feature = "http", feature = "cloud")))]
        {
            false
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
            asking: {
                #[cfg(any(feature = "http", feature = "cloud"))]
                {
                    matches!(load.phase, Phase::Confirming { .. })
                }
                #[cfg(not(any(feature = "http", feature = "cloud")))]
                {
                    false
                }
            },
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
                converted: None,
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
            options,
            size,
            recent,
            shown,
        } = request;
        let load = self.start(false);
        load.path = Some(shown.unwrap_or_else(|| stdin::named(&paths[0])));
        load.size = size;
        load.recent = recent;
        load.paths = Some(paths.clone());
        self.first_step(paths, options)
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
            download: None,
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
        if stdin::is_stdin(&first) {
            // Read once: opened again (`H`), the copy on hand is read.
            if let Some(kept) = self
                .kept
                .clone()
                .filter(|kept| kept.url == first && kept.file.path().exists())
            {
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
            .filter(|kept| paths.len() == 1 && kept.file.path().exists())
        {
            return self.read_download(kept, options);
        }
        // A remote model's headers are all it needs: read by range, not downloaded.
        #[cfg(any(feature = "http", feature = "cloud"))]
        if paths.len() == 1
            && let Some(format) = crate::remote_model::model_format(&first, options.format)
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
        if paths.len() == 1 && delimited.is_some() && options.parse_strings.is_some() {
            load.phase = Phase::ScanningStrings;
            return Step::Scan {
                paths,
                options,
                display: None,
                status: "Scanning string columns...",
            };
        }
        load.phase = Phase::Scanning { downloaded: false };
        // A table inside a database goes by its path there, on screen and once open.
        let display = load.path.clone().filter(|shown| *shown != paths[0]);
        Step::Scan {
            paths,
            options,
            display,
            status: "Scanning input...",
        }
    }

    /// Read a download: decompress it first if it is compressed CSV, TSV or PSV, else
    /// scan it.
    /// Either way the dataset is named by the URL, not the temporary file, and what
    /// was piped in by `stdin`.
    fn read_download(&mut self, fetched: Fetched, options: OpenOptions) -> Step {
        let load = self.load.as_mut().expect("a download read has a load");
        let file = fetched.file.path().to_path_buf();
        let url = stdin::named(&fetched.url);
        let download = fetched.file.clone();
        load.download = Some(fetched);
        // A compressed CSV, TSV or PSV has to be decompressed before it can be scanned,
        // as it is when opened from disk; scanning the download directly read `.gz` as
        // a format and refused it.
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
        load.phase = Phase::Scanning { downloaded: true };
        Step::Scan {
            paths: vec![file],
            options,
            display: Some(url),
            status: "Scanning...",
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
                Phase::Scanning { .. } | Phase::ScanningStrings,
            ) => {
                load.phase = Phase::ReadingSchema;
                // What the frame scans: the converted streams, else the download.
                let download = load
                    .converted
                    .clone()
                    .or_else(|| load.download.as_ref().map(|fetched| fetched.file.clone()));
                Step::ReadSchema {
                    lf,
                    path,
                    options,
                    progress: load.progress.clone(),
                    download,
                }
            }
            (
                LoadAnswer::Streams {
                    files,
                    bytes,
                    path,
                    options,
                },
                Phase::Scanning { .. },
            ) if load.converted.is_none() => {
                let read = Arc::<AtomicU64>::default();
                load.phase = Phase::Converting {
                    what: "Converting Arrow stream",
                    read: read.clone(),
                    total: bytes,
                };
                Step::Convert {
                    files,
                    path,
                    options,
                    writer: load.writer.clone(),
                    read,
                }
            }
            (
                LoadAnswer::Converted {
                    file,
                    path,
                    options,
                },
                Phase::Converting { .. },
            ) => {
                let paths = vec![file.path().to_path_buf()];
                // A download or a pipe's spool converted is kept as its copy, to be read
                // again without converting, and the stream is let go rather than held
                // beside the copy for the session.
                if let Some(fetched) = load.download.as_mut() {
                    fetched.file = file.clone();
                }
                load.converted = Some(file);
                load.phase = Phase::Scanning { downloaded: true };
                Step::Scan {
                    paths,
                    options: OpenOptions {
                        format: Some(FileFormat::Arrow),
                        hive: false,
                        ..options
                    },
                    display: path,
                    status: "Scanning...",
                }
            }
            (
                LoadAnswer::Compressed {
                    file,
                    path,
                    options,
                },
                Phase::Scanning { .. } | Phase::ScanningStrings,
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
            (
                LoadAnswer::ReadInto {
                    file,
                    total,
                    path,
                    options,
                },
                Phase::Scanning { .. } | Phase::ScanningStrings,
            ) => {
                let read = Arc::<AtomicU64>::default();
                load.phase = Phase::Converting {
                    what: reading(options.format),
                    read: read.clone(),
                    total,
                };
                Step::ReadInto {
                    path: path.unwrap_or_else(|| file.clone()),
                    file,
                    options,
                    writer: load.writer.clone(),
                    read,
                    progress: load.progress.clone(),
                    download: load.download.as_ref().map(|fetched| fetched.file.clone()),
                }
            }
            (
                LoadAnswer::Tables { file, tables, path },
                Phase::Scanning { .. } | Phase::ScanningStrings,
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
            #[cfg(any(feature = "http", feature = "cloud"))]
            (LoadAnswer::Sized(pending), Phase::CheckingSize { note }) => {
                load.phase = Phase::Confirming {
                    pending: Box::new(pending.clone()),
                    note: *note,
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
                };
                self.read_download(fetched, options)
            }
            (LoadAnswer::Spooled { download, options }, Phase::Spooling { read }) => {
                load.size = read.load(Ordering::Relaxed);
                let fetched = Fetched {
                    url: PathBuf::from(stdin::PATH),
                    file: download,
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
        // So is the IPC file streams were converted to.
        if let (Some(converted), Some(path)) = (&load.converted, &load.path) {
            message = crate::error_display::named_by_source(&message, converted.path(), path);
        }
        self.retire();
        Step::Failed(Failed { message, from_home })
    }

    /// The user agreed to the download: let go of the hold and fetch it.
    #[cfg(any(feature = "http", feature = "cloud"))]
    pub(crate) fn confirmed(&mut self) -> Step {
        let Some(load) = self
            .load
            .as_mut()
            .filter(|load| matches!(load.phase, Phase::Confirming { .. }))
        else {
            return Step::Nothing;
        };
        // The hold goes as the phase changes: the download job the caller starts next
        // holds the generation before anything else can look at it.
        let Phase::Confirming { pending, .. } =
            std::mem::replace(&mut load.phase, Phase::Downloading)
        else {
            unreachable!("matched just above");
        };
        Step::Download {
            pending: *pending,
            writer: load.writer.clone(),
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

/// What the loading screen says while a file is read into a table of its own.
fn reading(format: Option<FileFormat>) -> &'static str {
    match format {
        Some(FileFormat::Nmea | FileFormat::Gpx) => "Reading GPS log",
        _ => "Reading",
    }
}

/// The delimited format (CSV, TSV or PSV) `path` is read as, if it is one: `--format`
/// when given, else the extension, looking through a compression suffix
/// (`x.tsv.gz` is TSV).
pub(crate) fn delimited_format(path: &Path, options: &OpenOptions) -> Option<FileFormat> {
    let format = options.format.or_else(|| {
        FileFormat::from_path(path).or_else(|| {
            CompressionFormat::from_extension(path)?;
            FileFormat::from_path(Path::new(path.file_stem()?))
        })
    })?;
    format.separator().is_some().then_some(format)
}

/// Why a remote model is downloaded rather than read by its headers.
#[cfg(any(feature = "http", feature = "cloud"))]
pub(crate) const NO_RANGES: &str = "The server does not send byte ranges, so the model's header cannot be read without downloading the whole file.";

/// The download a remote source needs before it can be read, if it needs one: an HTTP
/// file always, and one object of a store that cannot be scanned in place.
#[cfg(any(feature = "http", feature = "cloud"))]
fn remote_download(src: &source::InputSource, options: &OpenOptions) -> Option<PendingDownload> {
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

#[cfg(feature = "cloud")]
fn should_download(url: &str) -> bool {
    let (_, ext) = source::url_path_extension(url);
    source::cloud_path_should_download(ext.as_deref(), source::is_prefix_or_glob(url))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::IntoLazy;

    fn frame() -> LazyFrame {
        polars::df!("a" => [1i32, 2, 3]).unwrap().lazy()
    }

    fn state() -> Box<DataTableState> {
        Box::new(DataTableState::from_lazyframe(frame(), &OpenOptions::default()).unwrap())
    }

    fn request(path: &str) -> OpenRequest {
        OpenRequest {
            paths: vec![PathBuf::from(path)],
            options: OpenOptions::default(),
            size: 7,
            recent: Some(PathBuf::from(path)),
            shown: None,
        }
    }

    #[cfg(any(feature = "http", feature = "cloud"))]
    fn jobs() -> Jobs {
        Jobs::new(std::sync::mpsc::channel().0)
    }

    /// `answered`, with the job owner the download question needs.
    fn answer(loader: &mut Loader, id: LoadId, answer: LoadAnswer) -> Step {
        loader.answered(
            id,
            answer,
            #[cfg(any(feature = "http", feature = "cloud"))]
            &jobs(),
        )
    }

    fn scanned(path: &str) -> LoadAnswer {
        LoadAnswer::Scanned {
            lf: Box::new(frame()),
            path: Some(PathBuf::from(path)),
            options: OpenOptions::default(),
        }
    }

    fn schema_read(path: &str) -> LoadAnswer {
        LoadAnswer::SchemaRead {
            state: state(),
            path: Some(PathBuf::from(path)),
            options: OpenOptions::default(),
            debug_label: None,
        }
    }

    /// A local file goes scan, schema, install, first rows, done; each step says what to
    /// run, and the screen says each phase in turn.
    #[test]
    fn a_local_open_runs_its_phases_in_order() {
        let mut loader = Loader::default();
        let step = loader.open(request("data.parquet"));
        let id = loader.id().expect("a load");
        assert!(matches!(
            step,
            Step::Scan { ref paths, display: None, status: "Scanning input...", .. }
                if paths == &[PathBuf::from("data.parquet")]
        ));
        let load = loader.current().unwrap();
        assert_eq!(load.phase().label(), ("Scanning input", 10));
        assert_eq!(load.path(), Some(Path::new("data.parquet")));
        assert_eq!(load.size(), 7);
        assert!(loader.awaiting_dataset() && loader.waits());

        let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("data.parquet"))
        else {
            panic!("the scan goes on to the schema");
        };
        assert!(Arc::ptr_eq(&progress, loader.progress().unwrap()));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Caching schema", 40)
        );

        let Step::Install(loaded) = answer(&mut loader, id, schema_read("data.parquet")) else {
            panic!("the schema goes on to the install");
        };
        assert_eq!(loaded.path.as_deref(), Some(Path::new("data.parquet")));
        assert_eq!(
            loaded.paths.as_deref(),
            Some(&[PathBuf::from("data.parquet")][..])
        );
        assert_eq!(loaded.recent.as_deref(), Some(Path::new("data.parquet")));
        assert!(!loaded.from_home);
        assert!(
            Arc::ptr_eq(&loaded.footers, &progress),
            "the dataset takes the load's footer counter"
        );
        assert!(!loader.awaiting_dataset(), "the dataset is up");
        assert!(!loader.waits(), "the read of its rows holds the keys");
        assert!(loader.progress().is_none(), "the counter is the dataset's");
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Loading buffer", 70)
        );

        loader.first_rows_settled();
        assert!(loader.current().is_none(), "the open is done");
    }

    /// Arrow IPC streams found by the scan are converted, counting their bytes, and the
    /// IPC file they become is scanned and held by the dataset, named by what was asked
    /// for; a failure names that too, and putting the load down lets the file go.
    #[test]
    fn streams_are_converted_then_scanned() {
        let dir = tempfile::tempdir().unwrap();
        let mut loader = Loader::default();
        let _ = loader.open(request("cache.arrow"));
        let id = loader.id().unwrap();
        let Step::Convert {
            files, path, read, ..
        } = answer(
            &mut loader,
            id,
            LoadAnswer::Streams {
                files: vec![PathBuf::from("cache.arrow")],
                bytes: 200,
                path: Some(PathBuf::from("cache.arrow")),
                options: OpenOptions::default(),
            },
        )
        else {
            panic!("the streams are converted");
        };
        assert_eq!(files, [PathBuf::from("cache.arrow")]);
        assert_eq!(path.as_deref(), Some(Path::new("cache.arrow")));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Converting Arrow stream", 10)
        );
        read.store(100, Ordering::Relaxed);
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Converting Arrow stream", 20),
            "the bar moves with the bytes read"
        );
        assert!(loader.waits());

        let converted =
            TempDownload::keep(TempDownload::create(Some(dir.path()), Some("arrow")).unwrap());
        let temp = converted.path().to_path_buf();
        let Step::Scan {
            paths,
            options,
            display,
            ..
        } = answer(
            &mut loader,
            id,
            LoadAnswer::Converted {
                file: converted,
                path: Some(PathBuf::from("cache.arrow")),
                options: OpenOptions::default(),
            },
        )
        else {
            panic!("the IPC file is scanned");
        };
        assert_eq!(paths, std::slice::from_ref(&temp));
        assert_eq!(options.format, Some(FileFormat::Arrow));
        assert_eq!(display.as_deref(), Some(Path::new("cache.arrow")));
        assert!(
            matches!(
                answer(
                    &mut loader,
                    id,
                    LoadAnswer::Streams {
                        files: vec![temp.clone()],
                        bytes: 1,
                        path: None,
                        options: OpenOptions::default(),
                    },
                ),
                Step::Nothing
            ),
            "converted once"
        );
        let Step::ReadSchema { download, .. } = answer(&mut loader, id, scanned("cache.arrow"))
        else {
            panic!("the schema is read");
        };
        assert_eq!(
            download.as_ref().map(|d| d.path().to_path_buf()),
            Some(temp.clone()),
            "the dataset holds the converted file"
        );
        drop(download);
        let Step::Failed(failed) = loader.failed(id, &format!("could not read {}", temp.display()))
        else {
            panic!("the open fails");
        };
        assert_eq!(failed.message, "could not read cache.arrow");
        assert!(!temp.exists(), "the retired load let the file go");
    }

    /// An answer from a load that is no longer in flight, or from a phase it has left,
    /// changes nothing: no install, no new phase, no title.
    #[test]
    fn an_old_load_s_answers_change_nothing() {
        let mut loader = Loader::default();
        let _ = loader.open(request("first.csv"));
        let first = loader.id().unwrap();
        assert!(loader.make_way().is_some(), "the first is doing work");
        let _ = loader.open(request("second.csv"));
        let second = loader.id().unwrap();
        assert_ne!(first, second);

        assert!(matches!(
            answer(&mut loader, first, scanned("first.csv")),
            Step::Nothing
        ));
        assert!(matches!(
            answer(&mut loader, first, schema_read("first.csv")),
            Step::Nothing
        ));
        assert!(matches!(loader.failed(first, "gone"), Step::Nothing));
        assert_eq!(loader.id(), Some(second));
        let load = loader.current().unwrap();
        assert_eq!(load.path(), Some(Path::new("second.csv")));
        assert_eq!(load.phase().label(), ("Scanning input", 10));

        // An answer for a phase the load is not in is dropped too: a schema before the
        // scan has answered.
        assert!(matches!(
            answer(&mut loader, second, schema_read("second.csv")),
            Step::Nothing
        ));
        assert!(loader.awaiting_dataset());
    }

    /// A conversion put down mid-way is told to stop through its writer, and its file,
    /// answered late or for a phase the load is not in, is dropped and so removed.
    #[test]
    fn a_converted_file_nobody_wants_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let converted = || {
            let file =
                TempDownload::keep(TempDownload::create(Some(dir.path()), Some("arrow")).unwrap());
            let path = file.path().to_path_buf();
            (
                LoadAnswer::Converted {
                    file,
                    path: Some(PathBuf::from("cache.arrow")),
                    options: OpenOptions::default(),
                },
                path,
            )
        };
        let mut loader = Loader::default();
        let _ = loader.open(request("cache.arrow"));
        let first = loader.id().unwrap();

        let (early, early_path) = converted();
        assert!(matches!(answer(&mut loader, first, early), Step::Nothing));
        assert!(!early_path.exists(), "not converting yet");

        let Step::Convert { writer, .. } = answer(
            &mut loader,
            first,
            LoadAnswer::Streams {
                files: vec![PathBuf::from("cache.arrow")],
                bytes: 1,
                path: Some(PathBuf::from("cache.arrow")),
                options: OpenOptions::default(),
            },
        ) else {
            panic!("the streams are converted");
        };
        assert!(!writer.stopped());
        assert!(loader.make_way().is_some(), "the conversion is work");
        assert!(writer.stopped(), "Ctrl+O or another open stops it");

        let _ = loader.open(request("other.csv"));
        let (late, late_path) = converted();
        assert!(matches!(answer(&mut loader, first, late), Step::Nothing));
        assert!(!late_path.exists(), "the replaced load's file");
        assert_eq!(
            loader.current().unwrap().path(),
            Some(Path::new("other.csv"))
        );
    }

    /// Opening something new replaces a load doing work, and stops it: its footer pass
    /// and its download read the stop flag.
    #[test]
    fn a_superseding_open_stops_the_one_it_replaces() {
        let mut loader = Loader::default();
        let _ = loader.open(request("big"));
        let id = loader.id().unwrap();
        let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("big")) else {
            panic!("reading the schema");
        };
        let retired = loader.make_way().expect("the load is replaced");
        assert!(!retired.asking);
        assert!(progress.is_cancelled(), "its pass stops issuing reads");
        assert!(loader.current().is_none());
    }

    /// The frame between a key and its open, and the look at named paths, are carried on
    /// by the open they lead to: one load, one identity, and the origin it was asked from.
    #[test]
    fn an_open_carries_on_the_load_that_announced_it() {
        let mut loader = Loader::default();
        let announced = loader.announce(true, "Scanning input".to_string(), 10);
        loader.name(PathBuf::from("chosen.csv"));
        assert_eq!(
            loader.current().unwrap().path(),
            Some(Path::new("chosen.csv"))
        );
        assert!(loader.waits(), "the keys wait from the key that asked");
        assert!(loader.make_way().is_none(), "nothing to replace yet");
        let _ = loader.open(request("chosen.csv"));
        assert_eq!(loader.id(), Some(announced));

        let Step::Install(loaded) = ({
            let _ = answer(&mut loader, announced, scanned("chosen.csv"));
            answer(&mut loader, announced, schema_read("chosen.csv"))
        }) else {
            panic!("installs");
        };
        assert!(loaded.from_home, "a failure would be reported at home");

        // A look at named paths, then the directory it found, then the open of it.
        let mut loader = Loader::default();
        let look = loader.look_at_paths();
        assert!(loader.looking_at_paths(look));
        assert!(loader.make_way().is_none());
        let directory = loader.look_at_directory(PathBuf::from("dir"));
        assert_eq!(directory, look);
        assert!(!loader.looking_at_paths(look), "that look is answered");
        assert!(loader.looking_at_directory(look));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            (crate::App::LOOKING_AT_A_DIRECTORY, 5)
        );
        let _ = loader.open(request("dir"));
        assert_eq!(loader.id(), Some(look));
        assert!(
            !loader.looking_at_directory(look),
            "nor is a late look taken"
        );
    }

    /// A worker's failure ends its load with the reason and where it was asked from, and
    /// retires it; one from a load already installed, or gone, is not the open's.
    #[test]
    fn a_failed_phase_retires_its_load() {
        let mut loader = Loader::default();
        let id = loader.announce(true, "Scanning input".to_string(), 10);
        let _ = loader.open(request("broken.parquet"));
        let Step::ReadSchema { progress, .. } = answer(&mut loader, id, scanned("broken.parquet"))
        else {
            panic!("reading the schema");
        };
        let Step::Failed(failed) = loader.failed(id, "not parquet") else {
            panic!("the open fails");
        };
        assert_eq!(failed.message, "not parquet");
        assert!(failed.from_home);
        assert!(loader.current().is_none());
        assert!(progress.is_cancelled());
        assert!(matches!(loader.failed(id, "again"), Step::Nothing));

        let id = {
            let _ = loader.open(request("good.parquet"));
            loader.id().unwrap()
        };
        let _ = answer(&mut loader, id, scanned("good.parquet"));
        let Step::Install(loaded) = answer(&mut loader, id, schema_read("good.parquet")) else {
            panic!("installs");
        };
        assert!(
            matches!(loader.failed(id, "the rows"), Step::Nothing),
            "the rows' failure is the table's"
        );
        assert!(!loaded.footers.is_cancelled());
        assert!(
            loader.retire().is_some(),
            "going home puts down the read of the first rows"
        );
        assert!(
            !loaded.footers.is_cancelled(),
            "but not the dataset's footer pass"
        );
    }

    /// Several URLs at once cannot be read, and say so.
    #[test]
    fn several_urls_end_the_session() {
        let mut loader = Loader::default();
        let step = loader.open(OpenRequest {
            paths: vec![PathBuf::from("s3://a/x.csv"), PathBuf::from("s3://a/y.csv")],
            options: OpenOptions::default(),
            size: 0,
            recent: None,
            shown: None,
        });
        assert!(matches!(step, Step::Crash(message) if message.contains("S3")));
        assert!(loader.current().is_none());
    }

    /// A compressed TSV or PSV is decompressed as a CSV is, told its format so it is
    /// split on its own separator (#567); the format comes from `--format`, else the
    /// name under the compression suffix.
    #[test]
    fn a_compressed_delimited_file_is_decompressed_as_its_format() {
        let opts = |format: Option<FileFormat>| OpenOptions {
            format,
            ..OpenOptions::default()
        };
        for (name, format, expected) in [
            ("x.tsv.gz", None, Some(FileFormat::Tsv)),
            ("x.PSV.xz", None, Some(FileFormat::Psv)),
            ("x.csv.zst", None, Some(FileFormat::Csv)),
            ("x.tsv", None, Some(FileFormat::Tsv)),
            ("x.gz", Some(FileFormat::Tsv), Some(FileFormat::Tsv)),
            ("x.csv.gz", Some(FileFormat::Psv), Some(FileFormat::Psv)),
            ("x.json.gz", None, None),
            ("x.gz", None, None),
            ("x.tsv.gz", Some(FileFormat::Parquet), None),
        ] {
            assert_eq!(
                delimited_format(Path::new(name), &opts(format)),
                expected,
                "{name} {format:?}"
            );
        }

        let mut loader = Loader::default();
        assert!(matches!(
            loader.open(request("logs.tsv.gz")),
            Step::Decompress { ref options, .. } if options.format == Some(FileFormat::Tsv)
        ));
    }

    /// A scan that finds one compressed delimited file, as a directory of one does, hands
    /// it to the decompress step under the name the open was asked for (#576); the
    /// answer of a load in another phase changes nothing.
    #[test]
    fn a_compressed_file_the_scan_found_is_decompressed() {
        let compressed = || LoadAnswer::Compressed {
            file: PathBuf::from("dir/data.tsv.gz"),
            path: Some(PathBuf::from("dir")),
            options: OpenOptions {
                format: Some(FileFormat::Tsv),
                ..OpenOptions::default()
            },
        };
        let mut loader = Loader::default();
        assert!(matches!(loader.open(request("dir")), Step::Scan { .. }));
        let id = loader.id().unwrap();
        assert!(matches!(
            answer(&mut loader, id, compressed()),
            Step::Decompress { ref file, ref path, ref options, download: None, .. }
                if file == Path::new("dir/data.tsv.gz")
                    && path == Path::new("dir")
                    && options.format == Some(FileFormat::Tsv)
        ));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Decompressing", 30)
        );
        assert!(matches!(
            answer(&mut loader, id, compressed()),
            Step::Nothing
        ));
        assert!(matches!(
            answer(&mut loader, id, schema_read("dir")),
            Step::Install(_)
        ));
    }

    /// A GPS log the scan found is converted under the name the open was asked for;
    /// a second answer for the phase it left changes nothing, the schema read installs,
    /// and putting the load down mid-conversion stops the writer, so its file goes.
    #[test]
    fn a_gps_log_the_scan_found_is_converted() {
        let convert = || LoadAnswer::ReadInto {
            file: PathBuf::from("drive.nmea"),
            total: 200,
            path: Some(PathBuf::from("drive.nmea")),
            options: OpenOptions {
                format: Some(FileFormat::Nmea),
                ..OpenOptions::default()
            },
        };
        let mut loader = Loader::default();
        let _ = loader.open(request("drive.nmea"));
        let id = loader.id().unwrap();
        let Step::ReadInto {
            file, writer, read, ..
        } = answer(&mut loader, id, convert())
        else {
            panic!("the log is converted");
        };
        assert_eq!(file, Path::new("drive.nmea"));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Reading GPS log", 10)
        );
        read.store(100, Ordering::Relaxed);
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Reading GPS log", 20),
            "the bar moves with the bytes read"
        );
        assert!(loader.waits());
        assert!(matches!(answer(&mut loader, id, convert()), Step::Nothing));
        assert!(!writer.stopped());
        loader.retire();
        assert!(
            writer.stopped(),
            "Ctrl+O or another open stops the conversion"
        );

        let mut loader = Loader::default();
        let _ = loader.open(request("drive.nmea"));
        let id = loader.id().unwrap();
        assert!(matches!(
            answer(&mut loader, id, convert()),
            Step::ReadInto { .. }
        ));
        assert!(matches!(
            answer(&mut loader, id, schema_read("drive.nmea")),
            Step::Install(_)
        ));
    }

    /// A database of several tables, opened without `--table`, puts the load down and
    /// lands on its tables, where it was asked from; a second answer changes nothing.
    /// Piped in, there is no place to list them, and the open fails naming them.
    #[test]
    fn a_database_of_several_tables_lands_on_them() {
        let tables = |file: &str| LoadAnswer::Tables {
            file: PathBuf::from(file),
            tables: vec!["users".to_string(), "orders".to_string()],
            path: Some(PathBuf::from(file)),
        };
        let mut loader = Loader::default();
        let _ = loader.open(request("shop.db"));
        let id = loader.id().unwrap();
        let Step::Tables(landed) = answer(&mut loader, id, tables("shop.db")) else {
            panic!("the tables are listed");
        };
        assert_eq!(landed.database, Path::new("shop.db"));
        assert!(!landed.from_home);
        assert!(loader.current().is_none(), "the load is put down");
        assert!(matches!(
            answer(&mut loader, id, tables("shop.db")),
            Step::Nothing
        ));

        let dir = tempfile::tempdir().unwrap();
        let mut loader = Loader::default();
        let Step::Spool { .. } = loader.open(OpenRequest::named(
            vec![PathBuf::from("-")],
            OpenOptions::default(),
        )) else {
            panic!("standard input is read first");
        };
        let id = loader.id().unwrap();
        let spool = TempDownload::keep(TempDownload::create(Some(dir.path()), None).unwrap());
        let options = OpenOptions {
            format: Some(FileFormat::Sqlite),
            ..Default::default()
        };
        let Step::Scan { paths, .. } = answer(
            &mut loader,
            id,
            LoadAnswer::Spooled {
                download: spool,
                options,
            },
        ) else {
            panic!("the spool is scanned");
        };
        let Step::Failed(failed) = answer(
            &mut loader,
            id,
            LoadAnswer::Tables {
                file: paths[0].clone(),
                tables: vec!["users".to_string(), "orders".to_string()],
                path: Some(PathBuf::from("stdin")),
            },
        ) else {
            panic!("piped in, the table has to be named");
        };
        assert_eq!(
            failed.message,
            "stdin holds 2 tables: users, orders. Open one with --table NAME."
        );
    }

    /// A table named by its path inside a database is the database opened with
    /// `--table`, recorded and shown as the table.
    #[test]
    fn a_path_inside_a_database_opens_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shop.db");
        let mut header = crate::sqlite::MAGIC.to_vec();
        header.resize(512, 0);
        std::fs::write(&db, header).unwrap();
        let request = OpenRequest::named(vec![db.join("orders")], OpenOptions::default());
        assert_eq!(request.paths, std::slice::from_ref(&db));
        assert_eq!(request.options.table.as_deref(), Some("orders"));
        assert_eq!(request.recent, Some(db.join("orders")));
        assert_eq!(request.shown, Some(db.join("orders")));

        let named = OpenRequest::named(
            vec![db.clone()],
            OpenOptions {
                table: Some("users".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(named.paths, std::slice::from_ref(&db));
        assert_eq!(
            named.recent,
            Some(db.join("users")),
            "--table is recorded too"
        );

        let plain = OpenRequest::named(vec![db.clone()], OpenOptions::default());
        assert_eq!(plain.recent, Some(db));
        assert_eq!(plain.shown, None);
    }

    /// A local compressed CSV is decompressed rather than scanned; a CSV read with its
    /// strings parsed scans saying so; a frame handed over goes straight to its schema
    /// and has no path to open again.
    #[test]
    fn the_first_step_follows_what_was_asked_for() {
        let mut loader = Loader::default();
        assert!(matches!(
            loader.open(request("logs.csv.gz")),
            Step::Decompress { ref file, ref path, .. }
                if file == Path::new("logs.csv.gz") && path == Path::new("logs.csv.gz")
        ));
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Decompressing", 30)
        );
        let id = loader.id().unwrap();
        assert!(matches!(
            answer(&mut loader, id, schema_read("logs.csv.gz")),
            Step::Install(_)
        ));

        let mut loader = Loader::default();
        let mut parsed = request("table.csv");
        parsed.options.parse_strings = Some(crate::ParseStringsTarget::All);
        assert!(matches!(
            loader.open(parsed),
            Step::Scan {
                status: "Scanning string columns...",
                ..
            }
        ));

        let mut loader = Loader::default();
        let Step::ReadSchema { path: None, .. } =
            loader.open_frame(frame(), OpenOptions::default())
        else {
            panic!("a frame's schema is read");
        };
        let id = loader.id().unwrap();
        let Step::Install(loaded) = answer(
            &mut loader,
            id,
            LoadAnswer::SchemaRead {
                state: state(),
                path: None,
                options: OpenOptions::default(),
                debug_label: None,
            },
        ) else {
            panic!("installs");
        };
        assert!(loaded.paths.is_none() && loaded.recent.is_none());
    }

    fn downloaded(dir: &Path, body: &str, extension: &str) -> TempDownload {
        use std::io::Write;
        let mut file = TempDownload::create(Some(dir), Some(extension)).unwrap();
        file.write_all(body.as_bytes()).unwrap();
        TempDownload::keep(file)
    }

    /// A remote model's headers are read by range, and that worker's dataset installs
    /// under the URL. A server without byte ranges sends it to the download question,
    /// which says why.
    #[cfg(feature = "http")]
    #[test]
    fn a_remote_model_reads_its_headers_or_falls_back_to_the_download() {
        let url = "https://example.com/m/model.safetensors";
        let jobs = jobs();
        let mut loader = Loader::default();
        let Step::ReadHeaders {
            url: read, format, ..
        } = loader.open(request(url))
        else {
            panic!("the headers are read, not downloaded");
        };
        assert_eq!(
            (read.as_path(), format),
            (Path::new(url), FileFormat::Safetensors)
        );
        let id = loader.id().unwrap();
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Reading headers", 20)
        );
        assert!(loader.waits());
        let Step::Install(loaded) = loader.answered(id, schema_read(url), &jobs) else {
            panic!("the headers are the dataset");
        };
        assert_eq!(loaded.recent.as_deref(), Some(Path::new(url)));

        let mut loader = Loader::default();
        let _ = loader.open(request(url));
        let id = loader.id().unwrap();
        let Step::Probe(pending) = loader.answered(
            id,
            LoadAnswer::NoRanges {
                options: OpenOptions::default(),
            },
            &jobs,
        ) else {
            panic!("no ranges: the file is sized for its download");
        };
        assert_eq!(pending.parts().0, url);
        assert_eq!(loader.download_note(), None, "not asked yet");
        assert!(matches!(
            loader.answered(id, LoadAnswer::Sized(pending), &jobs),
            Step::Ask(_)
        ));
        assert_eq!(loader.download_note(), Some(NO_RANGES));
        // An answer for a phase it has left changes nothing.
        assert!(matches!(
            loader.answered(id, schema_read(url), &jobs),
            Step::Nothing
        ));
    }

    /// A remote file is sized, put to the user, downloaded, then scanned under its URL;
    /// the installed dataset holds the file, and opening the URL again reads that copy
    /// rather than asking again.
    #[cfg(feature = "http")]
    #[test]
    fn a_remote_file_is_asked_about_then_downloaded_and_kept() {
        let url = "https://example.com/data.csv";
        let dir = tempfile::tempdir().unwrap();
        let jobs = jobs();
        let mut loader = Loader::default();
        let Step::Probe(pending) = loader.open(request(url)) else {
            panic!("the size is asked first");
        };
        let id = loader.id().unwrap();
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Checking size", 0)
        );

        let sized = pending.with_size(Some(42));
        assert!(matches!(
            loader.answered(id, LoadAnswer::Sized(sized), &jobs),
            Step::Ask(ref p) if p.parts().1 == Some(42)
        ));
        assert!(loader.asking() && loader.awaiting_dataset());
        assert!(!loader.waits(), "the question has the keys");
        assert!(jobs.would_strand(), "the generation is held while it asks");

        let Step::Download { .. } = loader.confirmed() else {
            panic!("agreed to, it downloads");
        };
        assert!(!jobs.would_strand(), "the hold goes with the question");
        assert!(matches!(loader.confirmed(), Step::Nothing), "asked once");
        assert_eq!(
            loader.current().unwrap().phase().label(),
            ("Downloading", 20)
        );

        let file = downloaded(dir.path(), "a\n1\n", "csv");
        let at = file.path().to_path_buf();
        let Step::Scan {
            paths,
            display,
            status,
            ..
        } = loader.answered(
            id,
            LoadAnswer::Downloaded {
                download: file,
                options: OpenOptions::default(),
            },
            &jobs,
        )
        else {
            panic!("the download is scanned");
        };
        assert_eq!(paths, vec![at.clone()]);
        assert_eq!(display.as_deref(), Some(Path::new(url)), "named by its URL");
        assert_eq!(status, "Scanning...");
        let Step::ReadSchema {
            download: Some(download),
            ..
        } = loader.answered(id, scanned(url), &jobs)
        else {
            panic!("the schema read is handed the download");
        };
        assert_eq!(download.path(), at, "the file the scan reads");
        // As the worker builds it: the dataset holds its file from the start.
        let state = DataTableState::from_lazyframe(frame(), &OpenOptions::default())
            .unwrap()
            .with_open(crate::widgets::datatable::OpenFacts {
                download: Some(download),
                ..Default::default()
            });
        let read = LoadAnswer::SchemaRead {
            state: Box::new(state),
            path: Some(PathBuf::from(url)),
            options: OpenOptions::default(),
            debug_label: None,
        };
        let Step::Install(loaded) = loader.answered(id, read, &jobs) else {
            panic!("installs");
        };
        assert!(
            loaded.state.scans_a_download(),
            "the dataset holds its file"
        );
        loader.first_rows_settled();
        drop(loaded);
        assert!(at.exists(), "and the loader keeps it to read again");

        let Step::Scan { paths, display, .. } = loader.open(request(url)) else {
            panic!("the copy on hand is scanned, with no question");
        };
        assert_eq!(paths, vec![at.clone()]);
        assert_eq!(display.as_deref(), Some(Path::new(url)));

        // Opening something else lets the copy go.
        assert!(loader.make_way().is_some());
        let _ = loader.open(request("local.csv"));
        assert!(!at.exists(), "nothing else held it");
    }

    /// The download handed to the schema read is claimed like any file the open wrote:
    /// quitting while that read is out removes it, though the worker still holds it.
    #[cfg(feature = "http")]
    #[test]
    fn quitting_mid_schema_read_sweeps_the_download_the_worker_holds() {
        use std::time::{Duration, Instant};
        let url = "https://example.com/data.csv";
        let dir = tempfile::tempdir().unwrap();
        let jobs = jobs();
        let mut loader = Loader::default();
        let Step::Probe(pending) = loader.open(request(url)) else {
            panic!("the size is asked first");
        };
        let id = loader.id().unwrap();
        let _ = loader.answered(id, LoadAnswer::Sized(pending.with_size(Some(4))), &jobs);
        let Step::Download { writer, .. } = loader.confirmed() else {
            panic!("agreed to, it downloads");
        };
        let file = crate::download::read_to_temp(
            Some(dir.path()),
            Some("csv"),
            || Ok((std::io::Cursor::new(b"a\n1\n".to_vec()), Some(4))),
            &writer,
        )
        .unwrap();
        let at = file.path().to_path_buf();
        let downloaded = LoadAnswer::Downloaded {
            download: file,
            options: OpenOptions::default(),
        };
        let _ = loader.answered(id, downloaded, &jobs);
        let Step::ReadSchema {
            download: Some(held),
            ..
        } = loader.answered(id, scanned(url), &jobs)
        else {
            panic!("the schema read is handed the download");
        };

        // Quitting drops the app, and the loader with it; the worker is still reading.
        let unfinished = loader.unfinished().clone();
        drop(loader);
        assert!(at.exists(), "the worker's copy keeps it");
        unfinished.sweep(Instant::now() + Duration::from_millis(50));
        assert!(!at.exists(), "the sweep removes it");
        // The worker ending later finds nothing to remove.
        drop(held);
    }

    /// Declining the download, a superseding open and a stale download each retire what
    /// the load held: the question's hold, and the file.
    #[cfg(feature = "http")]
    #[test]
    fn a_remote_open_put_down_leaves_nothing_behind() {
        let url = "https://example.com/data.csv";
        let dir = tempfile::tempdir().unwrap();
        let jobs = jobs();
        let mut loader = Loader::default();
        let Step::Probe(pending) = loader.open(request(url)) else {
            panic!("probe");
        };
        let id = loader.id().unwrap();
        let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
        let retired = loader.retire().expect("declined");
        assert!(retired.asking, "the question goes with it");
        assert!(!jobs.would_strand(), "and so does its hold");

        // A download answering for a load replaced meanwhile is dropped, and its file
        // with it.
        let Step::Probe(pending) = loader.open(request(url)) else {
            panic!("probe");
        };
        let old = loader.id().unwrap();
        let _ = loader.answered(old, LoadAnswer::Sized(pending), &jobs);
        let Step::Download { writer, .. } = loader.confirmed() else {
            panic!("download");
        };
        assert!(loader.make_way().is_some());
        assert!(writer.stopped(), "the download in flight is told to stop");
        let _ = loader.open(request("local.csv"));
        let file = downloaded(dir.path(), "a\n1\n", "csv");
        let at = file.path().to_path_buf();
        assert!(matches!(
            loader.answered(
                old,
                LoadAnswer::Downloaded {
                    download: file,
                    options: OpenOptions::default()
                },
                &jobs
            ),
            Step::Nothing
        ));
        assert!(!at.exists(), "the stale download's file went with it");

        // A download whose scan fails is let go, not kept.
        assert!(loader.make_way().is_some());
        let Step::Probe(pending) = loader.open(request(url)) else {
            panic!("probe");
        };
        let id = loader.id().unwrap();
        let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
        let _ = loader.confirmed();
        let file = downloaded(dir.path(), "a\n1\n", "csv");
        let at = file.path().to_path_buf();
        let _ = loader.answered(
            id,
            LoadAnswer::Downloaded {
                download: file,
                options: OpenOptions::default(),
            },
            &jobs,
        );
        let reason = format!(
            "Failed to load {}: bad csv\nIt stopped at {}.",
            at.display(),
            at.display()
        );
        let Step::Failed(failed) = loader.failed(id, &reason) else {
            panic!("the open fails");
        };
        assert_eq!(
            failed.message,
            format!("Failed to load {url}: bad csv\nIt stopped at {url}."),
            "named by the URL opened, never the temp file (#511)"
        );
        assert!(!at.exists(), "a failed open keeps no download");
    }

    /// A compressed CSV, TSV or PSV downloaded is decompressed under its URL as its
    /// format, as one opened from disk is (#567).
    #[cfg(feature = "http")]
    #[test]
    fn a_downloaded_compressed_csv_is_decompressed() {
        for (ext, format) in [
            ("csv.gz", FileFormat::Csv),
            ("tsv.zst", FileFormat::Tsv),
            ("psv.xz", FileFormat::Psv),
        ] {
            let url = format!("https://example.com/data.{ext}");
            let dir = tempfile::tempdir().unwrap();
            let jobs = jobs();
            let mut loader = Loader::default();
            let Step::Probe(pending) = loader.open(request(&url)) else {
                panic!("probe");
            };
            let id = loader.id().unwrap();
            let _ = loader.answered(id, LoadAnswer::Sized(pending), &jobs);
            let _ = loader.confirmed();
            let file = downloaded(dir.path(), "", ext);
            let step = loader.answered(
                id,
                LoadAnswer::Downloaded {
                    download: file,
                    options: OpenOptions::default(),
                },
                &jobs,
            );
            assert!(
                matches!(
                    step,
                    Step::Decompress { ref path, ref options, .. }
                        if path == Path::new(&url) && options.format == Some(format)
                ),
                "{ext}"
            );
        }
    }

    /// Standard input is read to a file first, its bytes counted on the loading
    /// screen, then scanned or decompressed as `stdin`; it is no recent, and opened
    /// again the copy on hand is read rather than the spent pipe.
    #[test]
    fn stdin_is_spooled_then_read_as_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let mut loader = Loader::default();
        let request = OpenRequest::named(vec![PathBuf::from("-")], OpenOptions::default());
        assert_eq!(request.recent, None, "not recorded in recents");
        let Step::Spool { writer, read, .. } = loader.open(request) else {
            panic!("standard input is read first");
        };
        let id = loader.id().unwrap();
        let load = loader.current().unwrap();
        assert_eq!(load.phase().label(), ("Reading stdin", 5));
        assert_eq!(load.path(), Some(Path::new("stdin")));
        read.store(1234, Ordering::Relaxed);
        assert_eq!(loader.current().unwrap().size(), 1234, "what has come in");
        assert!(loader.waits());

        let file = {
            use std::io::Write;
            let mut file = TempDownload::create(Some(dir.path()), None).unwrap();
            file.write_all(b"a\n1\n").unwrap();
            TempDownload::keep(file)
        };
        let at = file.path().to_path_buf();
        let options = OpenOptions {
            format: Some(FileFormat::Csv),
            ..Default::default()
        };
        let Step::Scan { paths, display, .. } = answer(
            &mut loader,
            id,
            LoadAnswer::Spooled {
                download: file,
                options: options.clone(),
            },
        ) else {
            panic!("the file is scanned");
        };
        assert_eq!(paths, vec![at.clone()]);
        assert_eq!(display.as_deref(), Some(Path::new("stdin")));
        let Step::ReadSchema {
            download: Some(_), ..
        } = answer(&mut loader, id, scanned("stdin"))
        else {
            panic!("the dataset holds the file");
        };
        let Step::Install(loaded) = answer(&mut loader, id, schema_read("stdin")) else {
            panic!("installs");
        };
        assert_eq!(loaded.recent, None);
        assert_eq!(loaded.paths.as_deref(), Some(&[PathBuf::from("-")][..]));
        loader.first_rows_settled();
        drop(writer);

        let Step::Scan { paths, display, .. } =
            loader.open(OpenRequest::named(vec![PathBuf::from("-")], options))
        else {
            panic!("the copy on hand is read again");
        };
        assert_eq!(paths, vec![at]);
        assert_eq!(display.as_deref(), Some(Path::new("stdin")));

        // Standard input with another path cannot be read.
        assert!(loader.make_way().is_some());
        let step = loader.open(OpenRequest::named(
            vec![PathBuf::from("-"), PathBuf::from("a.csv")],
            OpenOptions::default(),
        ));
        assert!(matches!(step, Step::Crash(_)));
    }

    /// A stream piped in is converted, and the copy, not the spool, is what is kept:
    /// the spool goes once converted, and opened again the copy is scanned as it is.
    #[test]
    fn a_converted_pipe_keeps_the_copy_not_the_stream() {
        let dir = tempfile::tempdir().unwrap();
        let mut loader = Loader::default();
        let options = OpenOptions {
            format: Some(FileFormat::Arrow),
            ..Default::default()
        };
        let Step::Spool { .. } = loader.open(OpenRequest::named(
            vec![PathBuf::from("-")],
            OpenOptions::default(),
        )) else {
            panic!("standard input is read first");
        };
        let id = loader.id().unwrap();
        let spool = downloaded(dir.path(), "stream", "tmp");
        let spooled = spool.path().to_path_buf();
        let Step::Scan { paths, .. } = answer(
            &mut loader,
            id,
            LoadAnswer::Spooled {
                download: spool,
                options: options.clone(),
            },
        ) else {
            panic!("the spool is scanned");
        };
        let Step::Convert { .. } = answer(
            &mut loader,
            id,
            LoadAnswer::Streams {
                files: paths,
                bytes: 6,
                path: Some(PathBuf::from("stdin")),
                options: options.clone(),
            },
        ) else {
            panic!("the stream is converted");
        };
        let copy = downloaded(dir.path(), "ipc file", "arrow");
        let converted = copy.path().to_path_buf();
        let Step::Scan { paths, display, .. } = answer(
            &mut loader,
            id,
            LoadAnswer::Converted {
                file: copy,
                path: Some(PathBuf::from("stdin")),
                options: options.clone(),
            },
        ) else {
            panic!("the copy is scanned");
        };
        assert_eq!(paths, std::slice::from_ref(&converted));
        assert_eq!(display.as_deref(), Some(Path::new("stdin")));
        assert!(!spooled.exists(), "the spool goes once converted");

        let Step::ReadSchema {
            download: Some(_), ..
        } = answer(&mut loader, id, scanned("stdin"))
        else {
            panic!("the dataset holds the copy");
        };
        let Step::Install(loaded) = answer(&mut loader, id, schema_read("stdin")) else {
            panic!("installs");
        };
        loader.first_rows_settled();
        drop(loaded);
        assert!(converted.exists(), "kept to read again");
        let Step::Scan { paths, .. } =
            loader.open(OpenRequest::named(vec![PathBuf::from("-")], options))
        else {
            panic!("the copy on hand is read again");
        };
        assert_eq!(paths, [converted]);
    }

    /// A compressed CSV piped in is decompressed as `stdin`; putting the read down
    /// stops its writer, which removes the partial file.
    #[test]
    fn piped_compressed_csv_is_decompressed_and_a_stop_reaches_the_spool() {
        let dir = tempfile::tempdir().unwrap();
        let mut loader = Loader::default();
        let Step::Spool { writer, .. } = loader.open(OpenRequest::named(
            vec![PathBuf::from("-")],
            OpenOptions::default(),
        )) else {
            panic!("spool");
        };
        assert!(loader.make_way().is_some(), "Ctrl+O puts it down");
        assert!(writer.stopped(), "and the spool stops at its next chunk");

        let Step::Spool { .. } = loader.open(OpenRequest::named(
            vec![PathBuf::from("-")],
            OpenOptions::default(),
        )) else {
            panic!("spool");
        };
        let id = loader.id().unwrap();
        let file = TempDownload::keep(TempDownload::create(Some(dir.path()), None).unwrap());
        let step = answer(
            &mut loader,
            id,
            LoadAnswer::Spooled {
                download: file,
                options: OpenOptions {
                    format: Some(FileFormat::Csv),
                    compression: Some(CompressionFormat::Gzip),
                    ..Default::default()
                },
            },
        );
        assert!(matches!(
            step,
            Step::Decompress { ref path, download: Some(_), .. } if path == Path::new("stdin")
        ));
    }
}
